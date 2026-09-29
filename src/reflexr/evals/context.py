"""What feedback, or an evaluator, is about: a run, a firing or a chain, with its events."""

from dataclasses import dataclass

from reflexr.core import Envelope, Run
from reflexr.workspace import Workspace


@dataclass(frozen=True)
class RunRecord:
    """A run with the events around it: what made its rule fire, and what it emitted.

    Attributes:
        run: The run.
        matched: The envelopes that made its rule fire, in order.
        emitted: The events the run published, in order.
    """

    run: Run
    matched: tuple[Envelope, ...]
    emitted: tuple[Envelope, ...]


async def run_record(workspace: Workspace, run_id: str) -> RunRecord:
    """Load a run and the events around it.

    Raises:
        NotFound: If there is no such run.
    """
    run = await workspace.run(run_id)
    log = await workspace.read()
    by_seq = {envelope.seq: envelope for envelope in log}
    matched = tuple(by_seq[seq] for seq in run.matched if seq in by_seq)
    emitted = tuple(
        envelope
        for envelope in log
        if envelope.causation is not None
        and envelope.causation.run_id == run.id
        and envelope.actor.kind != "system"
    )
    return RunRecord(run=run, matched=matched, emitted=emitted)


async def chain_events(workspace: Workspace, correlation_id: str) -> tuple[Envelope, ...]:
    """Return every envelope of a causal chain, in order."""
    return tuple(e for e in await workspace.read() if e.correlation_id == correlation_id)
