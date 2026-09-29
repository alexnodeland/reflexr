"""Identifier types and factories.

Identifiers are plain strings; the aliases below document which kind of thing a string
identifies. Generated identifiers carry a short type prefix, such as ``evt_3f9c2a1b7d4e5f60``,
so they read well in logs and on the wire, but any string the host application chooses works.

Firing ids are derived rather than random, so evaluating the same log twice produces the same
ids, and a firing's id can serve as its run's idempotency key.
"""

import hashlib
import secrets

type TenantId = str
"""Identifies a tenant: the top-level isolation boundary."""

type WorkspaceId = str
"""Identifies a workspace within a tenant. A workspace has one event log."""

type EventId = str
"""Identifies an event within a workspace. Publishing an id that already exists is a no-op."""

type RuleName = str
"""Identifies a rule. Rules are named by their authors, not generated."""

type ScopeKey = str
"""Identifies one scope of a rule: the canonical JSON of its scope fields' values."""

type FiringId = str
"""Identifies a firing. It is also the id of the run the firing starts."""

type RunId = str
"""Identifies a run: the execution of one firing's action."""

type TraceId = str
"""A W3C trace id, as 32 lowercase hex digits: the trace of one run attempt."""


def new_id(prefix: str) -> str:
    """Return a new random identifier with the given prefix.

    Args:
        prefix: A short type tag, such as ``"evt"``.

    Returns:
        An identifier such as ``evt_3f9c2a1b7d4e5f60``.
    """
    return f"{prefix}_{secrets.token_hex(8)}"


def new_event_id() -> EventId:
    """Return a new event identifier."""
    return new_id("evt")


def firing_id(rule: RuleName, generation: int, scope: ScopeKey, seq: int) -> FiringId:
    """Return the id of the firing of ``rule`` for ``scope`` at ``seq``.

    Args:
        rule: The rule that fired.
        generation: The rule's generation, which changes when its state is reset.
        scope: The scope that fired.
        seq: The envelope at which it fired.

    Returns:
        A deterministic identifier such as ``fir_0b6a3c9e1f2d4a58``.
    """
    digest = hashlib.sha256(f"{rule}\x00{generation}\x00{scope}\x00{seq}".encode()).hexdigest()
    return f"fir_{digest[:16]}"


def derived_event_id(run: RunId, index: int) -> EventId:
    """Return the id of the ``index``-th event a run emits in an attempt.

    Every attempt of a run derives the same ids in the same order, so a retried run that
    emits again publishes duplicates, which the log ignores.
    """
    digest = hashlib.sha256(f"{run}\x00{index}".encode()).hexdigest()
    return f"evt_{digest[:16]}"
