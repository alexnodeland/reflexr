"""What feedback, or an evaluator, is about: a run, a firing or a chain, with its events."""

from collections.abc import Sequence
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


class LogIndex:
    """A workspace's log, read once and indexed for the records of many runs and chains."""

    def __init__(self, log: Sequence[Envelope]) -> None:
        self._by_seq = {envelope.seq: envelope for envelope in log}
        self._emitted: dict[str, list[Envelope]] = {}
        self._chains: dict[str, list[Envelope]] = {}
        for envelope in log:
            self._chains.setdefault(envelope.correlation_id, []).append(envelope)
            if envelope.causation is not None and envelope.actor.kind != "system":
                self._emitted.setdefault(envelope.causation.run_id, []).append(envelope)

    def run_record(self, run: Run) -> RunRecord:
        """Return a run with the envelopes that made its rule fire and those it emitted."""
        matched = tuple(self._by_seq[seq] for seq in run.matched if seq in self._by_seq)
        return RunRecord(run=run, matched=matched, emitted=tuple(self._emitted.get(run.id, ())))

    def chain(self, correlation_id: str) -> tuple[Envelope, ...]:
        """Return every envelope of a causal chain, in order."""
        return tuple(self._chains.get(correlation_id, ()))


async def run_record(workspace: Workspace, run_id: str) -> RunRecord:
    """Load a run and the events around it.

    Raises:
        NotFound: If there is no such run.
    """
    run = await workspace.run(run_id)
    return LogIndex(await workspace.read()).run_record(run)


async def chain_events(workspace: Workspace, correlation_id: str) -> tuple[Envelope, ...]:
    """Return every envelope of a causal chain, in order."""
    return LogIndex(await workspace.read()).chain(correlation_id)
