"""The workspace protocol's frames, commands and resume rule."""

import pytest
from pydantic import TypeAdapter, ValidationError

from reflexr.core import (
    PROTOCOL,
    ArchiveRule,
    ClientFrame,
    CommandFrame,
    CommandResult,
    GiveFeedback,
    Hello,
    InstallRule,
    Publish,
    PublishedOutcome,
    ReplayRule,
    Rule,
    RuleVersionOutcome,
    RunTarget,
    ServerFrame,
    UnknownEvent,
    UnsupportedProtocol,
    UpdateRule,
    ValidationFailed,
    Welcome,
    on,
    resume,
    run,
)
from tests.event_types import ServiceError

CLIENT: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)
SERVER: TypeAdapter[ServerFrame] = TypeAdapter(ServerFrame)

deploys = Rule(name="chat:deploys", when=on("oncall:deploy.completed"), then=run("notify"))


def test_client_frames_round_trip_by_their_type() -> None:
    frames: list[ClientFrame] = [
        Hello(protocol=PROTOCOL, resume_after_seq=4, types=("app:service.error",)),
        Hello(protocol=PROTOCOL, from_head=True),
        CommandFrame(command_id="c1", command=Publish(event=ServiceError(service="auth"), id="e1")),
        CommandFrame(
            command_id="c2",
            command=GiveFeedback(
                feedback_type="quality", target=RunTarget(run_id="fir_1"), value={"score": 3}
            ),
        ),
        CommandFrame(command_id="c3", command=ReplayRule(rule="app:triage", mode="refire")),
        CommandFrame(
            command_id="c4",
            command=InstallRule(rule=deploys, provenance={"artifact": "art_rule_7"}),
        ),
        CommandFrame(command_id="c5", command=UpdateRule(rule=deploys, expected_version=1)),
        CommandFrame(command_id="c6", command=ArchiveRule(rule="chat:deploys", reason="done")),
    ]
    for frame in frames:
        assert CLIENT.validate_json(CLIENT.dump_json(frame)) == frame


def test_the_rfcs_install_frame_is_a_command() -> None:
    frame = CLIENT.validate_python(
        {
            "type": "command",
            "command_id": "install-prp_12",
            "command": {
                "type": "install_rule",
                "rule": {
                    "name": "chat:prod-deploy-failures",
                    "when": {
                        "filter": {"kind": "on", "types": ["oncall:deploy.completed"]},
                        "throttle": {"at_most": 1, "per": "PT15M"},
                    },
                    "then": {"action": "notify", "params": {"thread_id": "thr_4"}},
                    "ordering": "none",
                    "timeout": "PT1M",
                },
                "provenance": {"source": "artifactr", "proposal": "prp_12"},
            },
        }
    )
    assert isinstance(frame, CommandFrame)
    assert isinstance(frame.command, InstallRule)
    assert frame.command.rule.then.params == {"thread_id": "thr_4"}


def test_commands_are_strict_and_events_of_unknown_types_are_kept() -> None:
    with pytest.raises(ValidationError):
        CLIENT.validate_python({"type": "command", "command_id": "c", "command": {"type": "nope"}})
    with pytest.raises(ValidationError):
        CLIENT.validate_python(
            {"type": "command", "command_id": "", "command": {"type": "retry_run", "run_id": "r"}}
        )
    frame = CLIENT.validate_python(
        {
            "type": "command",
            "command_id": "c",
            "command": {"type": "publish", "event": {"type": "pager.sent"}},
        }
    )
    assert isinstance(frame, CommandFrame)
    assert isinstance(frame.command, Publish)
    assert isinstance(frame.command.event, UnknownEvent)
    with pytest.raises(ValidationError, match="an event needs a string 'type'"):
        Publish.model_validate({"event": {"service": "auth"}})
    with pytest.raises(
        ValidationError, match=r"invalid app:service\.error event: service: Field required"
    ):
        Publish.model_validate({"event": {"type": "app:service.error"}})


def test_server_frames_round_trip_by_their_type() -> None:
    frames: list[ServerFrame] = [
        Welcome(workspace_id="prod", head_seq=7, reset=True),
        CommandResult(command_id="c1", ok=True, outcome=PublishedOutcome(seq=8, id="e1")),
        CommandResult(command_id="c2", ok=False, rejection={"type": "not_found", "message": "x"}),
        CommandResult(
            command_id="c3",
            ok=True,
            outcome=RuleVersionOutcome(rule="chat:deploys", version=2, seq=812, duplicate=True),
        ),
    ]
    for frame in frames:
        assert SERVER.validate_json(SERVER.dump_json(frame)) == frame


def test_resuming_replays_after_the_clients_seq_or_resets() -> None:
    assert resume(Hello(protocol=PROTOCOL, resume_after_seq=3), head_seq=5).replay_after == 3
    ahead = resume(Hello(protocol=PROTOCOL, resume_after_seq=9), head_seq=5)
    assert (ahead.replay_after, ahead.reset) == (0, True)
    with pytest.raises(UnsupportedProtocol, match=r"this server speaks reflexr\.v1"):
        resume(Hello(protocol="reflexr.v0"), head_seq=5)
    assert RunTarget(run_id="r").kind == "run"


def test_a_client_can_start_at_the_head_and_replay_nothing() -> None:
    plan = resume(Hello(protocol=PROTOCOL, from_head=True), head_seq=5)
    assert (plan.replay_after, plan.reset) == (5, False)
    assert resume(Hello(protocol=PROTOCOL, from_head=True), head_seq=0).replay_after == 0
    with pytest.raises(ValidationFailed, match="resume_after_seq must be 0"):
        resume(Hello(protocol=PROTOCOL, resume_after_seq=3, from_head=True), head_seq=5)
    with pytest.raises(UnsupportedProtocol):
        resume(Hello(protocol="reflexr.v0", from_head=True), head_seq=5)
