"""Graph actions: pydantic-graph graphs as adapters of the action port, checkpointed (ADR-0043).

A :class:`GraphAction` drives a ``GraphBuilder`` graph step by step. At each step boundary
where it is safe to, it saves the graph's state and the tasks that run next to the run, so a
retry, or another executor after a crash, resumes after the last saved step.

It is safe to checkpoint when nothing runs in parallel: one task comes next, and it is not
between a fork and its join, and the next node's input type is known. Inside a fork, a crash
resumes from the checkpoint before the fork, so the branches that had finished run again: the
same at-least-once guarantee runs have everywhere.

A step's input type is its ``StepContext`` annotation. A fork runs no code of its own, so its
input is whatever the edges into it carry: when every such edge comes, with no transform, from a
step with a return annotation or the graph's start, and they all carry the same type, that is
the fork's input type. The boundary before a decision is never saved: a decision runs no code
either, so the boundary after it saves the same progress.

A checkpoint is saved only if what it restores reads back as it was: the state, the graph's
inputs and the next node's inputs. A checkpoint that cannot be resumed, because the graph or
its types changed since, is ignored and the run starts over; the attempt's span says why.
"""

import inspect
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import KW_ONLY, dataclass, field
from typing import Any, TypeVar, get_args, get_origin, get_type_hints

from opentelemetry import trace
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
# a fork's input type reads.
from pydantic_graph.id_types import ForkStackItem, NodeID, NodeRunID
from pydantic_graph.paths import DestinationMarker, TransformMarker

from reflexr.telemetry import attributes as a
from reflexr.workspace import Reaction

CHECKPOINT_VERSION = 1
"""The version of the checkpoint format, stored in each checkpoint."""

_STACK: TypeAdapter[tuple[ForkStackItem, ...]] = TypeAdapter(tuple[ForkStackItem, ...])

type _Batch = Sequence[GraphTask] | EndMarker[Any]
"""What a step boundary yields: the tasks that run next, or the graph's output."""

DISCARDED = "reflexr.checkpoint.discarded"
"""The event on an attempt's span when the run's checkpoint cannot be resumed."""


class _Unwritable(Exception):
    """A value its type would not read back as the same value, so it is not saved."""


@dataclass
class GraphAction[D, S, I, O]:
    """Run a pydantic-graph graph in response to a firing, checkpointed after its steps.

    The graph's deps are the :class:`~reflexr.workspace.Reaction`, so steps can read the
    firing and emit events. Its output is the run's output.

    A checkpoint saves the next node's inputs as that node's input type. A step's is its
    ``StepContext`` annotation, and the end node's the graph's output type. A fork's is inferred
    from the edges into it: the return type of the steps they come from, or the graph's input
    type from its start. It stays unknown when an edge has a transform, or comes from a step with
    no return annotation (or ``Any``), a decision, a join or a fork, or when the edges carry
    different types. A boundary before a node whose input type is unknown is not saved, and
    neither is one before a decision, which runs no code: the boundary after it saves the same.

    Nor is a boundary whose state, graph inputs or next inputs would not read back equal to
    what they were, such as a model given to a stream step or a ``BaseNode``, whose input types
    say ``Any``.

    Args:
        graph: A graph built with ``GraphBuilder``, whose deps type is ``Reaction[D]``.
        name: The name rules refer to the action by; defaults to the graph's name.
        state: Builds the graph's initial state from the reaction; defaults to the state
            type's constructor with no arguments.
        inputs: Builds the graph's inputs from the reaction; defaults to None.
        input_types: Input types by node id, for the steps and forks whose type reflexr
            cannot read or infer, such as a stream step, whose input type reads as ``Any``, or a
            fork after a transform. An explicit type wins over an annotation or an inferred one.

    Raises:
        ValueError: If the action has no name, or ``input_types`` names a node that is not a
            step or a fork.
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
        resumed = None if saved is None else self._resume(saved, reaction.run.attempts)
        if resumed is None:
            state = self.state(reaction) if self.state else graph.state_type()
            # Without an inputs builder the graph gets None, which its input type must accept.
            inputs = (
                self.inputs(reaction)
                if self.inputs
                else TypeAdapter(graph.input_type).validate_python(None)
            )
            pending: EndMarker[O] | Sequence[GraphTaskRequest] | None = None
        else:
            state, inputs, pending = resumed
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
        the run starts over, as any retry may, and the attempt's span says why.
        """
        if checkpoint is None:
            return None  # nothing saved yet
        if not isinstance(checkpoint, dict) or checkpoint.get("v") != CHECKPOINT_VERSION:
            return _discarded("format")
        saved: dict[str, Any] = checkpoint
        if saved.get("nodes") != sorted(self.graph.nodes):
            return _discarded("graph_changed")
        # A fork's inferred type depends on the edges into it, not only on the nodes, so a
        # checkpoint's next node may have no type now (an edge into it gained a transform).
        frontier: list[dict[str, Any]] = saved.get("frontier", [])
        if any(task["node_id"] not in self._types for task in frontier):
            return _discarded("untyped")
        return saved

    def _resume(
        self, saved: dict[str, Any], attempt: int
    ) -> tuple[S, I, list[GraphTaskRequest]] | None:
        """Read a checkpoint back, or return None to start over if it no longer validates.

        Every checkpoint saved now reads back, so one that does not was saved before a type it
        holds changed, or by a build that did not check.
        """
        graph = self.graph
        try:
            state = TypeAdapter(graph.state_type).validate_python(saved["state"])
            inputs = TypeAdapter(graph.input_type).validate_python(saved["inputs"])
            requests = self._requests(saved, attempt)
        except ValidationError as error:
            return _discarded("invalid", error)
        return state, inputs, requests

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
        header: dict[str, Any] = {"v": CHECKPOINT_VERSION, "nodes": sorted(graph.nodes)}
        if isinstance(batch, EndMarker):
            return {
                **header,
                "inputs": TypeAdapter(graph.input_type).dump_python(inputs, mode="json"),
                "state": TypeAdapter(graph.state_type).dump_python(run.state, mode="json"),
                "output": TypeAdapter(graph.output_type).dump_python(batch.value, mode="json"),
            }
        if not _quiescent(graph, batch) or batch[0].node_id not in self._types:
            return None
        [task] = batch
        if isinstance(task.inputs, Iterator):
            return None  # writing a one-shot iterator down would use it up before the node runs
        # What cannot be written down, or would not read back as it was, is not saved: the run
        # resumes from an earlier step instead.
        try:
            return {
                **header,
                "inputs": _write(TypeAdapter(graph.input_type), inputs),
                "state": _write(TypeAdapter(graph.state_type), run.state),
                "frontier": [
                    {
                        "node_id": task.node_id,
                        "inputs": _write(self._types[task.node_id], task.inputs),
                        "fork_stack": _STACK.dump_python(task.fork_stack, mode="json"),
                    }
                ],
            }
        except _Unwritable:
            return None


def _input_adapters(
    graph: Graph[Any, Any, Any, Any], overrides: Mapping[str, Any]
) -> dict[str, TypeAdapter[Any]]:
    """Map each node whose input type is known to an adapter for its inputs.

    An explicit type wins. Otherwise a step's type is its annotation, the end node's the graph's
    output type, and a fork's the one type the edges into it carry. A decision has none, so the
    boundary before it is never saved. An explicit type pydantic cannot handle raises; an
    inferred one is dropped, so the graph runs as it would without it, with no checkpoint before
    that fork.

    Raises:
        ValueError: If ``overrides`` names a node that is not a step or a fork, such as a
            decision, before which nothing is saved.
    """
    wrong = sorted(n for n in overrides if not isinstance(graph.nodes.get(NodeID(n)), Step | Fork))
    if wrong:
        raise ValueError(f"input_types names nodes that are not steps or forks: {', '.join(wrong)}")
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
    into = _edges_into(graph)
    for node_id, node in graph.nodes.items():
        if isinstance(node, Fork) and node_id not in declared:
            kind = _carried(graph, into.get(node_id, []))
            if kind is not None:
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


def _carried(graph: Graph[Any, Any, Any, Any], edges: Sequence[tuple[NodeID, bool]]) -> Any:
    """The type every one of these edges carries, or None if one is unknown or they differ."""
    kinds = [None if transformed else _output_type(graph, source) for source, transformed in edges]
    first = kinds[0] if kinds else None
    if first is None or any(kind != first for kind in kinds):
        return None
    return first


def _output_type(graph: Graph[Any, Any, Any, Any], source: NodeID) -> Any:
    """The type a node passes along its edges, or None if it is not known."""
    node = graph.nodes[source]
    if isinstance(node, StartNode):
        return graph.input_type
    if isinstance(node, Step):
        return _step_output_type(node)
    return None  # a decision's input, a join's reduced value, or a fork's items


def _write(adapter: TypeAdapter[Any], value: object) -> JsonValue:
    """Write a value down as JSON, if it reads back equal to it.

    Raises:
        _Unwritable: If it does not: a model saved through a type that says ``Any`` reads back
            as a dict.
    """
    try:
        dumped = adapter.dump_python(value, mode="json", warnings="error")
        back = adapter.validate_python(dumped)
    except (PydanticSerializationError, ValidationError) as error:
        raise _Unwritable from error
    if back != value:
        raise _Unwritable
    return dumped


def _discarded(reason: str, error: ValidationError | None = None) -> None:
    """Say on the attempt's span why the run's checkpoint is not resumed, and resume nothing.

    The reason is ``format``, ``graph_changed``, ``untyped`` or ``invalid``. What did not
    validate is described by location and message, without the values, which may be private.
    """
    attributes = {a.CHECKPOINT_REASON: reason}
    if error is not None:
        problems = [
            f"{'.'.join(map(str, e['loc'])) or 'the value'}: {e['msg']}"
            for e in error.errors(include_input=False)
        ]
        attributes[a.CHECKPOINT_ERROR] = f"{error.title}: {'; '.join(problems)}"
    trace.get_current_span().add_event(DISCARDED, attributes)


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
