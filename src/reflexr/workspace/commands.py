"""One handler for the protocol's commands, which every surface shares (ADR-0044).

REST, the WebSocket and MCP turn their requests into :data:`~reflexr.core.protocol.Command`
models and hand them here, so a command behaves the same whichever way it arrives.
"""

from typing import assert_never

from reflexr.core import load_feedback
from reflexr.core.protocol import (
    ArchiveRule,
    CancelRun,
    Command,
    GiveFeedback,
    InstallRule,
    Outcome,
    Publish,
    PublishedOutcome,
    RecordedOutcome,
    ReplayRule,
    RetryRun,
    RuleOutcome,
    RuleVersionOutcome,
    RunOutcome,
    SkipRun,
    UpdateRule,
)
from reflexr.workspace.workspace import RuleVersion, Workspace


async def execute(workspace: Workspace, command: Command) -> Outcome:
    """Carry out a command on a workspace, as the handle's actor.

    Raises:
        Rejection: If the command cannot be carried out.
    """
    match command:
        case Publish(event=event, id=event_id, correlation_id=chain):
            published = await workspace.publish(event, id=event_id, correlation_id=chain)
            envelope = published.envelope
            return PublishedOutcome(seq=envelope.seq, id=envelope.id, duplicate=published.duplicate)
        case GiveFeedback(feedback_type=kind, target=target, value=value):
            feedback = load_feedback(kind, target, value)
            envelope = await workspace.give_feedback(feedback, on=target)
            return RecordedOutcome(seq=envelope.seq, id=envelope.id)
        case RetryRun(run_id=run_id):
            return RunOutcome(run=await workspace.retry_run(run_id))
        case SkipRun(run_id=run_id, reason=reason):
            return RunOutcome(run=await workspace.skip_run(run_id, reason=reason))
        case CancelRun(run_id=run_id, reason=reason):
            return RunOutcome(run=await workspace.cancel_run(run_id, reason=reason))
        case ReplayRule(rule=rule, from_seq=from_seq, mode=mode):
            progress = await workspace.replay_rule(rule, from_seq=from_seq, mode=mode)
            return RuleOutcome(rule=rule, progress=progress)
        case InstallRule(rule=rule, provenance=provenance):
            return _versioned(await workspace.install_rule(rule, provenance=provenance))
        case UpdateRule(rule=rule, expected_version=expected, provenance=provenance):
            updated = await workspace.update_rule(
                rule, expected_version=expected, provenance=provenance
            )
            return _versioned(updated)
        case ArchiveRule(rule=rule, expected_version=expected, reason=reason):
            archived = await workspace.archive_rule(rule, expected_version=expected, reason=reason)
            return _versioned(archived)
        case _:
            assert_never(command)


def _versioned(changed: RuleVersion) -> RuleVersionOutcome:
    stored = changed.stored
    return RuleVersionOutcome(
        rule=stored.rule.name, version=stored.version, seq=changed.seq, duplicate=changed.duplicate
    )
