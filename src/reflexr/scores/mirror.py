"""The feedback mirror: a subscriber on a workspace's log that sends feedback to a score sink.

Each ``reflexr:feedback_given`` becomes one score per scored field, attached to the trace it is
about:

- a run: the trace of the run's latest attempt
- a firing: the trace of the evaluation that recorded it, from its ``reflexr:rule_fired`` envelope
- a causal chain, or anything without a trace: the session, which is the chain

Score ids are derived from the envelope's id and the field, so mirroring the same log again
replaces scores instead of duplicating them: the mirror can always start over from the
beginning. It need not, though: it saves a cursor in the workspace as it goes, and a restarted
mirror carries on after it (ADR-0040). The scores are evalr's ``Score``: they have no
evaluator, they say where the feedback came from in their ``source``, and they name no span,
since feedback judges a trace or a session, never one of its spans.
"""

import uuid
from typing import assert_never

import evalr.core
from evalr.core import Score, ScoreConfigStore, ScoreSink

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
from reflexr.telemetry import parse_traceparent, untraced
from reflexr.workspace import Workspace

_SCORE_IDS = uuid.UUID("5b8f2a1e-7c3d-4e9a-b6f0-2d1c8e4a7b93")
"""The namespace of score ids."""

FIRING_SEARCH = 1000
"""How far after the envelope a rule fired at the mirror looks for its ``reflexr:rule_fired``."""

SAVE_EVERY = 500
"""How many envelopes without feedback a mirror follows before it saves its cursor anyway, so a
restarted mirror reads at most this many again."""


class FeedbackMirror:
    """Records a workspace's feedback in a score sink.

    Args:
        workspace: The workspace to follow; any actor's handle will do, since it only reads the
            log and saves its cursor.
        sink: Where scores go.
        cursor: The name of the cursor the mirror saves in the workspace, such as ``"langfuse"``,
            so that it carries on where it was when it starts again. Give each mirror of a
            workspace its own: two sharing a name would skip feedback after a restart. A new name
            mirrors everything again, and keeps doing so across restarts. ``None`` keeps no
            cursor: the mirror follows from the start every time.
    """

    def __init__(self, workspace: Workspace, sink: ScoreSink, *, cursor: str | None) -> None:
        self._workspace = workspace
        self._sink = sink
        self._cursor = cursor

    async def follow(self, *, after_seq: int | None = None) -> None:
        """Mirror feedback after ``after_seq``, then each new piece as it is given, until cancelled.

        Run it as a task for as long as the workspace should be mirrored. By default it
        carries on after its cursor, or starts at the beginning of the log if it has none.

        Mirroring is at least once. The mirror saves its cursor once it has recorded a piece of
        feedback's scores, and after every 500 other envelopes. A mirror stopped
        between recording scores and saving records them again when it restarts, and the sink
        replaces them, since their ids are the same. A cursor only moves forward, so following
        from an earlier ``after_seq`` mirrors feedback again but leaves the cursor where it is
        until the mirror passes it; a restart then carries on after the cursor.

        The mirror's reads, scores and cursor are untraced, so an idle mirror, which polls its
        workspace's log, makes no traces.
        """
        name = self._cursor
        with untraced():
            start = after_seq
            if start is None:
                start = 0 if name is None else await self._workspace.cursor(name)
            saved = start
            async for envelope in self._workspace.subscribe(after_seq=start):
                recorded = await self.mirror(envelope)
                if name is not None and (recorded or envelope.seq - saved >= SAVE_EVERY):
                    await self._workspace.save_cursor(name, envelope.seq)
                    saved = envelope.seq

    async def mirror(self, envelope: Envelope) -> list[Score]:
        """Record the scores of one envelope, and return them; other events record nothing."""
        scores = await self.scores(envelope)
        if scores:
            await self._sink.record(scores)
        return scores

    async def scores(self, envelope: Envelope) -> list[Score]:
        """Return the scores of a ``reflexr:feedback_given`` envelope.

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
        source = {
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
                source=source,
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
        The names of the configs created. A config the store has by name is left as it is.
    """
    chosen = types if types is not None else list(feedback_types().values())
    configs = [config for feedback_type in chosen for config in score_configs(feedback_type)]
    return await evalr.core.sync_score_configs(store, configs)
