"""Graph actions: pydantic-graph graphs as adapters of the action port, checkpointed (ADR-0009).

A :class:`GraphAction` drives a ``GraphBuilder`` graph step by step. At each step boundary
where it is safe to, it saves the graph's state and the tasks that run next to the run, so a
retry, or another executor after a crash, resumes after the last saved step.

It is safe to checkpoint when nothing runs in parallel: one task comes next, and it is not
between a fork and its join, and the next node's input type is known. Inside a fork, a crash
resumes from the checkpoint before the fork, so the branches that had finished run again: the
same at-least-once guarantee runs have everywhere.

A step's input type is its ``StepContext`` annotation. Decisions and forks run no code of their
own, so their input is whatever the edges into them carry: when every such edge comes, with no
transform, from a step with a return annotation, the graph's start, or a decision whose type is
known, and they all carry the same type, that is the node's input type.
"""

import inspect
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import KW_ONLY, dataclass, field
from typing import Any, TypeVar, get_args, get_origin, get_type_hints

from pydantic import JsonValue, PydanticSchemaGenerationError, TypeAdapter, ValidationError
from pydantic_core import PydanticSerializationError
from pydantic_graph import (
    Decision,
    EndMarker,
    EndNode,
    Fork,
    Graph,
    GraphRun,
    GraphTask,
    GraphTaskRequest,
    StartNode,
    Step,
    StepContext,
)

# pydantic-graph 2.51 does not re-export these from its top level, so they come from its modules,
# here and nowhere else in reflexr, so that if they move only these lines change: the ids in a
# task's fork stack, which a checkpoint saves, and the markers on an edge's path, which inferring
# a decision's or a fork's input type reads.
from pydantic_graph.id_types import ForkStackItem, NodeID, NodeRunID
from pydantic_graph.paths import DestinationMarker, TransformMarker

from reflexr.telemetry import attributes as a
from reflexr.workspace import Reaction

CHECKPOINT_VERSION = 1
"""The version of the checkpoint format, stored in each checkpoint."""

_STACK: TypeAdapter[tuple[ForkStackItem, ...]] = TypeAdapter(tuple[ForkStackItem, ...])

type _Batch = Sequence[GraphTask] | EndMarker[Any]
"""What a step boundary yields: the tasks that run next, or the graph's output."""


@dataclass
class GraphAction[D, S, I, O]:
    """Run a pydantic-graph graph in response to a firing, checkpointed after its steps.

    The graph's deps are the :class:`~reflexr.workspace.Reaction`, so steps can read the
    firing and emit events. Its output is the run's output.

    A checkpoint saves the next node's inputs as that node's input type. A step's is its
    ``StepContext`` annotation, and the end node's the graph's output type. A decision's or a
    fork's is inferred from the edges into it: the return type of the steps they come from, the
    graph's input type from its start, or the type of a decision before it. It stays unknown
    when an edge has a transform, or comes from a step with no return annotation (or ``Any``),
    a join or a fork, or when the edges carry different types. A boundary before a node whose
    input type is unknown is not saved, and neither is one before a decision whose inputs do
    not read back as the class they had, since a decision routes by class.

    Args:
        graph: A graph built with ``GraphBuilder``, whose deps type is ``Reaction[D]``.
        name: The name rules refer to the action by; defaults to the graph's name.
        state: Builds the graph's initial state from the reaction; defaults to the state
            type's constructor with no arguments.
        inputs: Builds the graph's inputs from the reaction; defaults to None.
        input_types: Input types by node id, for the nodes whose type reflexr cannot infer,
            such as a decision after a transform. An explicit type always wins over an
            annotation or an inferred type, and decisions after the node infer from it.
    """

    graph: Graph[S, Reaction[D], I, O]
    _: KW_ONLY
    name: str = ""
    state: Callable[[Reaction[D]], S] | None = None
    inputs: Callable[[Reaction[D]], I] | None = None
    input_types: Mapping[str, Any] = field(default_factory=dict[str, Any])
    _types: dict[str, TypeAdapter[Any]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.name = self.name or self.graph.name or ""
        if not self.name:
            raise ValueError("name the action, or the graph")
        self._types = _input_adapters(self.graph, self.input_types)

    async def __call__(self, reaction: Reaction[D]) -> JsonValue:
        """Run the graph for one attempt, resuming from the run's checkpoint if it has one."""
        graph = self.graph
        saved = self._restorable(reaction.run.checkpoint)
        if saved is not None and "output" in saved:
            return saved["output"]  # the graph finished; only recording the run did not
        state_adapter = TypeAdapter(graph.state_type)
        input_adapter = TypeAdapter(graph.input_type)
        if saved is None:
            state = self.state(reaction) if self.state else graph.state_type()
            # Without an inputs builder the graph gets None, which its input type must accept.
            inputs = self.inputs(reaction) if self.inputs else input_adapter.validate_python(None)
            pending: EndMarker[O] | Sequence[GraphTaskRequest] | None = None
        else:
            state = state_adapter.validate_python(saved["state"])
            inputs = input_adapter.validate_python(saved["inputs"])
            pending = self._requests(saved, reaction.run.attempts)
        tracer = reaction.workspace.telemetry.tracer
        async with graph.iter(state=state, deps=reaction, inputs=inputs, infer_name=False) as run:
            if pending is None:
                pending = await anext(run)  # runs only the start node
                await self._checkpoint(reaction, run, inputs, pending, "__start__")
            while not isinstance(pending, EndMarker):
                ran = ", ".join(str(task.node_id) for task in pending)
                with tracer.start_as_current_span(
                    f"execute_step {ran}", attributes={a.RUN_ID: reaction.run.id}
                ):
                    pending = await run.next(pending)
                await self._checkpoint(reaction, run, inputs, pending, ran)
        return TypeAdapter(graph.output_type).dump_python(pending.value, mode="json")

    def _restorable(self, checkpoint: JsonValue) -> dict[str, Any] | None:
        """Return the run's checkpoint if this graph wrote it, or None to start over.

        A checkpoint from another version of the format, or from a graph whose nodes have
        changed since, or whose next node's input type is no longer known, cannot be resumed;
        the run starts over, as any retry may.
        """
        if not isinstance(checkpoint, dict):
            return None
        saved: dict[str, Any] = checkpoint
        if saved.get("v") != CHECKPOINT_VERSION:
            return None
        if saved.get("nodes") != sorted(self.graph.nodes):
            return None
        # A node's inferred type depends on the edges into it, not only on the nodes, so a
        # checkpoint's next node may have no type now (an edge into it gained a transform).
        frontier: list[dict[str, Any]] = saved.get("frontier", [])
        if any(task["node_id"] not in self._types for task in frontier):
            return None
        return saved

    def _requests(self, saved: dict[str, Any], attempt: int) -> list[GraphTaskRequest]:
        requests: list[GraphTaskRequest] = []
        for task in saved["frontier"]:
            node_id: str = task["node_id"]
            stack = tuple(
                # Restored runs number their node runs afresh, so prefix the saved ids.
                ForkStackItem(
                    item.fork_id, NodeRunID(f"a{attempt}:{item.node_run_id}"), item.thread_index
                )
                for item in _STACK.validate_python(task["fork_stack"])
            )
            inputs = self._types[node_id].validate_python(task["inputs"])
            requests.append(GraphTaskRequest(NodeID(node_id), inputs, stack))
        return requests

    async def _checkpoint(
        self,
        reaction: Reaction[D],
        run: GraphRun[S, Reaction[D], O],
        inputs: I,
        batch: _Batch,
        ran: str,
    ) -> None:
        saved = self._snapshot(run, inputs, batch)
        if saved is not None:
            await reaction.checkpoint(ran, saved)

    def _snapshot(self, run: GraphRun[S, Reaction[D], O], inputs: I, batch: _Batch) -> JsonValue:
        """Return a checkpoint of the run at this boundary, or None if it is not safe to save."""
        graph = self.graph
        saved: dict[str, Any] = {
            "v": CHECKPOINT_VERSION,
            "nodes": sorted(graph.nodes),
            "inputs": TypeAdapter(graph.input_type).dump_python(inputs, mode="json"),
            "state": TypeAdapter(graph.state_type).dump_python(run.state, mode="json"),
        }
        if isinstance(batch, EndMarker):
            saved["output"] = TypeAdapter(graph.output_type).dump_python(batch.value, mode="json")
            return saved
        if not _quiescent(graph, batch) or batch[0].node_id not in self._types:
            return None
        [task] = batch
        if isinstance(task.inputs, Iterator):
            return None  # writing a one-shot iterator down would use it up before the node runs
        adapter = self._types[task.node_id]
        try:
            dumped = adapter.dump_python(task.inputs, mode="json", warnings="error")
        except PydanticSerializationError:
            return None  # inputs its type cannot write down: resume from an earlier step
        if isinstance(graph.nodes[task.node_id], Decision) and not _reads_back(
            adapter, dumped, task.inputs
        ):
            return None  # a decision routes by class, so a resumed one must get the same class
        saved["frontier"] = [
            {
                "node_id": task.node_id,
                "inputs": dumped,
                "fork_stack": _STACK.dump_python(task.fork_stack, mode="json"),
            }
        ]
        return saved


def _input_adapters(
    graph: Graph[Any, Any, Any, Any], overrides: Mapping[str, Any]
) -> dict[str, TypeAdapter[Any]]:
    """Map each node whose input type is known to an adapter for its inputs.

    An explicit type wins. Otherwise a step's type is its annotation and the end node's the
    graph's output type, and decisions' and forks' types are inferred from the edges into them.
    An explicit type pydantic cannot handle raises; an inferred one is dropped, so the graph
    runs as it would without it, with no checkpoint before that node.
    """
    declared: dict[str, Any] = {}
    for node_id, node in graph.nodes.items():
        if isinstance(node, Step):
            found = _step_input_type(node)
            if found is not None:
                declared[node_id] = found
        elif isinstance(node, EndNode):
            declared[node_id] = graph.output_type
    declared.update(overrides)
    adapters = {node_id: TypeAdapter(kind) for node_id, kind in declared.items()}
    for node_id, kind in _inferred_types(graph, declared).items():
        with suppress(PydanticSchemaGenerationError):
            adapters[node_id] = TypeAdapter(kind)
    return adapters


def _step_input_type(step: Step[Any, Any, Any, Any]) -> Any:
    """The ``InputT`` of a step's ``StepContext[StateT, DepsT, InputT]`` parameter, if typed."""
    call = step.call
    context = next(iter(inspect.signature(call).parameters), "")
    hint = get_type_hints(call).get(context)
    if get_origin(hint) is not StepContext:
        return None
    return get_args(hint)[2]


def _step_output_type(step: Step[Any, Any, Any, Any]) -> Any:
    """A step's return annotation, or None if it has none or it says nothing (Any, a TypeVar).

    Read like its input annotation, without ``Annotated`` metadata: the graph never validated
    the value against constraints, so a resumed run must not either.
    """
    hint = get_type_hints(step.call).get("return")
    return None if hint is Any or isinstance(hint, TypeVar) else hint


def _inferred_types(
    graph: Graph[Any, Any, Any, Any], declared: Mapping[str, Any]
) -> dict[str, Any]:
    """Infer the input types of the decisions and forks ``declared`` does not type.

    Each takes the one type every edge into it carries. Decisions can lead to decisions, so
    this repeats until a pass learns nothing more.
    """
    into = _edges_into(graph)
    pending = {
        node_id
        for node_id, node in graph.nodes.items()
        if isinstance(node, Decision | Fork) and node_id not in declared
    }
    inferred: dict[str, Any] = {}
    while True:
        known = {**declared, **inferred}
        found = {
            node_id: kind
            for node_id in pending
            if (kind := _carried(graph, into.get(node_id, []), known)) is not None
        }
        if not found:
            return inferred
        inferred |= found
        pending -= found.keys()


def _edges_into(graph: Graph[Any, Any, Any, Any]) -> dict[NodeID, list[tuple[NodeID, bool]]]:
    """Each node's incoming edges: the node each comes from, and whether it has a transform.

    The graph's paths end at one destination each, forks having been split out of them when
    it was built; a decision's branches are paths from the decision.
    """
    paths = [
        (source, path) for source, outgoing in graph.edges_by_source.items() for path in outgoing
    ] + [
        (node.id, branch.path)
        for node in graph.nodes.values()
        if isinstance(node, Decision)
        for branch in node.branches
    ]
    into: dict[NodeID, list[tuple[NodeID, bool]]] = {}
    for source, path in paths:
        transformed = any(isinstance(item, TransformMarker) for item in path.items)
        for item in path.items:
            if isinstance(item, DestinationMarker):
                into.setdefault(item.destination_id, []).append((source, transformed))
    return into


def _carried(
    graph: Graph[Any, Any, Any, Any],
    edges: Sequence[tuple[NodeID, bool]],
    known: Mapping[str, Any],
) -> Any:
    """The type every one of these edges carries, or None if one is unknown or they differ."""
    kinds = [
        None if transformed else _output_type(graph, source, known) for source, transformed in edges
    ]
    first = kinds[0] if kinds else None
    if first is None or any(kind != first for kind in kinds):
        return None
    return first


def _output_type(graph: Graph[Any, Any, Any, Any], source: NodeID, known: Mapping[str, Any]) -> Any:
    """The type a node passes along its edges, or None if it is not known."""
    node = graph.nodes[source]
    if isinstance(node, StartNode):
        return graph.input_type
    if isinstance(node, Decision):
        return known.get(source)  # a decision passes its input on
    if isinstance(node, Step):
        return _step_output_type(node)
    return None  # a join's reduced value, or a fork's items


def _reads_back(adapter: TypeAdapter[Any], dumped: Any, inputs: object) -> bool:
    """Whether saved inputs validate back into the class they had."""
    try:
        return type(adapter.validate_python(dumped)) is type(inputs)
    except ValidationError:
        return False


def _quiescent(graph: Graph[Any, Any, Any, Any], batch: Sequence[GraphTask]) -> bool:
    """Whether nothing but the one next task is in flight, and no join holds partial results."""
    if len(batch) != 1:
        return False
    [task] = batch
    forks = [f for f in task.fork_stack if isinstance(graph.nodes.get(f.fork_id), Fork)]
    if not forks:
        return True
    joined: set[str] = set()
    for parent in graph.parent_forks.values():
        joined |= {parent.fork_id, *parent.intermediate_nodes}
    if any(f.fork_id not in joined for f in forks):
        return False  # a fork without a join: its branches never meet again
    return not any(
        task.node_id == join_id or task.node_id in parent.intermediate_nodes
        for join_id, parent in graph.parent_forks.items()
    )
