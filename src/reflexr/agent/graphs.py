"""Graph actions: pydantic-graph graphs as adapters of the action port, checkpointed (ADR-0009).

A :class:`GraphAction` drives a ``GraphBuilder`` graph step by step. At each step boundary
where it is safe to, it saves the graph's state and the tasks that run next to the run, so a
retry, or another executor after a crash, resumes after the last saved step.

It is safe to checkpoint when nothing runs in parallel: one task comes next, and it is not
between a fork and its join, and the next node's input type is known. Inside a fork, a crash
resumes from the checkpoint before the fork, so the branches that had finished run again: the
same at-least-once guarantee runs have everywhere. Decisions, forks and joins run no code of
their own, so the boundaries before them are not saved.
"""

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import KW_ONLY, dataclass, field
from typing import Any, get_args, get_origin, get_type_hints

from pydantic import JsonValue, TypeAdapter
from pydantic_core import PydanticSerializationError
from pydantic_graph import (
    EndMarker,
    EndNode,
    Fork,
    Graph,
    GraphRun,
    GraphTask,
    GraphTaskRequest,
    Step,
    StepContext,
)
from pydantic_graph.id_types import ForkStackItem, NodeID, NodeRunID

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

    Args:
        graph: A graph built with ``GraphBuilder``, whose deps type is ``Reaction[D]``.
        name: The name rules refer to the action by; defaults to the graph's name.
        state: Builds the graph's initial state from the reaction; defaults to the state
            type's constructor with no arguments.
        inputs: Builds the graph's inputs from the reaction; defaults to None.
        input_types: The input type of nodes reflexr cannot infer, by node id, such as forks
            and decisions. Steps' types come from their ``StepContext`` annotation. A boundary
            before a node whose input type is unknown is not saved.
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
        changed since, cannot be resumed; the run starts over, as any retry may.
        """
        if not isinstance(checkpoint, dict):
            return None
        if checkpoint.get("v") != CHECKPOINT_VERSION:
            return None
        if checkpoint.get("nodes") != sorted(self.graph.nodes):
            return None
        return checkpoint

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
        try:
            dumped = self._types[task.node_id].dump_python(
                task.inputs, mode="json", warnings="error"
            )
        except PydanticSerializationError:
            return None  # inputs its type cannot write down: resume from an earlier step
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
    """Map each node whose input type is known to an adapter for its inputs."""
    types: dict[str, Any] = {}
    for node_id, node in graph.nodes.items():
        if isinstance(node, Step):
            found = _step_input_type(node)
            if found is not None:
                types[node_id] = found
        elif isinstance(node, EndNode):
            types[node_id] = graph.output_type
    types.update(overrides)
    return {node_id: TypeAdapter(kind) for node_id, kind in types.items()}


def _step_input_type(step: Step[Any, Any, Any, Any]) -> Any:
    """The ``InputT`` of a step's ``StepContext[StateT, DepsT, InputT]`` parameter, if typed."""
    call = step.call
    context = next(iter(inspect.signature(call).parameters), "")
    hint = get_type_hints(call).get(context)
    if get_origin(hint) is not StepContext:
        return None
    return get_args(hint)[2]


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
