"""The feedback mirror: a subscriber on a workspace's log that sends feedback to a score sink.

Each ``feedback_given`` becomes one score per scored field, attached to the trace it is about:

- a run: the trace of the run's latest attempt
- a firing: the trace of the evaluation that recorded it, from its ``rule_fired`` envelope
- a causal chain, or anything without a trace: the session, which is the chain

Score ids are derived from the envelope's id and the field, so mirroring the same log again
replaces scores instead of duplicating them: the mirror can always start over from the
beginning.
"""

import uuid
from typing import assert_never

from reflexr.core import (
    ChainTarget,
    Envelope,
    Feedback,
    FeedbackGiven,
    FiringTarget,
    RuleFired,
    RunTarget,
    TraceId,
    feedback_types,
)
from reflexr.scores.mapping import score_configs, score_values
from reflexr.scores.ports import Score, ScoreConfigStore, ScoreSink
from reflexr.telemetry import parse_traceparent
from reflexr.workspace import Workspace

_SCORE_IDS = uuid.UUID("5b8f2a1e-7c3d-4e9a-b6f0-2d1c8e4a7b93")
"""The namespace of score ids."""

FIRING_SEARCH = 1000
"""How far after the envelope a rule fired at the mirror looks for its ``rule_fired``."""


class FeedbackMirror:
    """Sends a workspace's feedback to a score sink.

    Args:
        workspace: The workspace to follow; any actor's handle will do, since it only reads.
        sink: Where scores go.
    """

    def __init__(self, workspace: Workspace, sink: ScoreSink) -> None:
        self._workspace = workspace
        self._sink = sink

    async def follow(self, *, after_seq: int = 0) -> None:
        """Mirror feedback after ``after_seq``, then each new piece as it is given, until cancelled.

        Run it as a task for as long as the workspace should be mirrored.
        """
        async for envelope in self._workspace.subscribe(after_seq=after_seq):
            await self.mirror(envelope)

    async def mirror(self, envelope: Envelope) -> list[Score]:
        """Send the scores of one envelope, and return them; other events send nothing."""
        scores = await self.scores(envelope)
        for score in scores:
            await self._sink.send(score)
        return scores

    async def scores(self, envelope: Envelope) -> list[Score]:
        """Return the scores of a ``feedback_given`` envelope.

        There are none for other events, or for a feedback type this process does not
        register.
        """
        event = envelope.event
        if not isinstance(event, FeedbackGiven):
            return []
        feedback_type = feedback_types().get(event.feedback_type)
        if feedback_type is None:
            return []
        trace_id = await self._trace(event)
        metadata = {
            "tenant_id": self._workspace.tenant_id,
            "workspace_id": self._workspace.workspace_id,
            "feedback_type": event.feedback_type,
            "target": event.target.kind,
            "actor": envelope.actor.participant,
            "actor_kind": envelope.actor.kind,
            "seq": str(envelope.seq),
        }
        return [
            Score(
                id=str(uuid.uuid5(_SCORE_IDS, f"{envelope.id}/{config.name}")),
                name=config.name,
                value=value,
                data_type=config.data_type,
                trace_id=trace_id,
                # Feedback joins the chain of what it is about, so the envelope's chain is it.
                session_id=None if trace_id else envelope.correlation_id,
                timestamp=envelope.ts,
                metadata=metadata,
            )
            for config, value in score_values(feedback_type, event.value)
        ]

    async def _trace(self, event: FeedbackGiven) -> TraceId | None:
        """The trace a piece of feedback is about, if it is about one."""
        target = event.target
        match target:
            case ChainTarget():
                return None
            case RunTarget():
                # The run exists: it was checked when the feedback was given, and runs are kept.
                run = await self._workspace.run(target.run_id)
                return run.trace_ids[-1] if run.trace_ids else None
            case FiringTarget():
                return await self._evaluation_trace(target.firing_id)
            case _:
                assert_never(target)

    async def _evaluation_trace(self, firing_id: str) -> TraceId | None:
        run = await self._workspace.run(firing_id)  # a firing's run has its id
        for envelope in await self._workspace.read(after_seq=run.fired_seq, limit=FIRING_SEARCH):
            fact = envelope.event
            if isinstance(fact, RuleFired) and fact.firing_id == firing_id:
                context = parse_traceparent(envelope.traceparent)
                return f"{context.trace_id:032x}" if context else None
        return None


async def sync_score_configs(
    store: ScoreConfigStore, types: list[type[Feedback]] | None = None
) -> list[str]:
    """Create the score configs of feedback types that a store does not have yet.

    Args:
        store: Where the configs live.
        types: The feedback types; every registered type by default.

    Returns:
        The names of the configs created.
    """
    existing = set(await store.names())
    created: list[str] = []
    for feedback_type in types if types is not None else list(feedback_types().values()):
        for config in score_configs(feedback_type):
            if config.name not in existing:
                await store.create(config)
                existing.add(config.name)
                created.append(config.name)
    return created
