"""The terminal client: requests and rendering, then whole commands against a running server."""

import argparse
import asyncio
import io
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from rich.console import Console

from conftest import FakeClock, Script, eventually, publish, triage
from oncall import cli
from oncall.app import Oncall
from oncall.cli import (
    command_of,
    error_text,
    event_of,
    follow,
    hello,
    parser,
    render,
    render_result,
    render_run,
)


def terminal() -> tuple[Console, io.StringIO]:
    out = io.StringIO()
    return Console(file=out, width=200, color_system=None, highlight=False), out


def plain(markup: str | None) -> str:
    console, out = terminal()
    console.print(markup)
    return out.getvalue().rstrip("\n")


def args(*argv: str) -> argparse.Namespace:
    return parser().parse_args(list(argv))


# ─── requests ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("argv", "event"),
    [
        (
            ["alert", "api", "--severity", "8", "--message", "5xx above 5%"],
            {
                "type": "oncall:alert.fired",
                "service": "api",
                "severity": 8,
                "message": "5xx above 5%",
            },
        ),
        (
            ["deploy", "api", "--version", "v42"],
            {"type": "oncall:deploy.completed", "service": "api", "version": "v42"},
        ),
        (["heartbeat", "db"], {"type": "oncall:service.heartbeat", "service": "db"}),
    ],
)
def test_publishing_commands_build_events(argv: list[str], event: dict[str, Any]) -> None:
    assert event_of(args(*argv)) == event


@pytest.mark.parametrize(
    ("argv", "command"),
    [
        (["retry", "fir_1"], {"type": "retry_run", "run_id": "fir_1"}),
        (["skip", "fir_1"], {"type": "skip_run", "run_id": "fir_1", "reason": None}),
        (
            ["cancel", "fir_1", "--reason", "handled"],
            {"type": "cancel_run", "run_id": "fir_1", "reason": "handled"},
        ),
    ],
)
def test_run_commands_build_command_frames(argv: list[str], command: dict[str, Any]) -> None:
    frame = command_of(args(*argv))
    assert (frame["type"], frame["command"]) == ("command", command)
    assert frame["command_id"].startswith("cmd_")


@pytest.mark.parametrize(
    ("given", "error"),
    [("11", "a severity is from 1 to 10"), ("0", "a severity is from 1 to 10"), ("high", "high")],
)
def test_severity_is_one_to_ten(given: str, error: str, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        args("alert", "api", "--severity", given, "--message", "x")
    [complaint] = [line for line in capsys.readouterr().err.splitlines() if "error:" in line]
    assert "argument --severity: " in complaint
    assert error in complaint


def test_hello_resumes_after_a_seq() -> None:
    assert hello(7) == {"type": "hello", "protocol": "reflexr.v1", "resume_after_seq": 7}


# ─── rendering ────────────────────────────────────────────────────────────────

ALICE = {"kind": "user", "id": "alice", "name": "alice"}
TRIAGE = {"kind": "agent", "rule": "oncall:triage", "run_id": "fir_1", "name": "triage"}
REACTOR = {"kind": "system", "name": "reactor"}
MCP = {"kind": "external_agent", "client_id": "mcp"}


def event(position: int, actor: dict[str, Any], /, **fields: Any) -> dict[str, Any]:
    """An event frame: an envelope at ``position`` in the log, with the event's fields."""
    ts = "2026-09-28T09:00:05Z"
    return {"type": "event", "seq": position, "ts": ts, "actor": actor, "event": fields}


FRAMES: list[dict[str, Any]] = [
    {"type": "welcome", "workspace_id": "prod", "head_seq": 2, "reset": False},
    event(1, ALICE, type="oncall:service.heartbeat", service="api"),
    event(2, ALICE, type="oncall:deploy.completed", service="api", version="v2"),
    {"type": "replay_complete", "up_to_seq": 2},
    event(3, ALICE, type="oncall:alert.fired", service="api", severity=4, message="slow [p99]"),
    event(4, ALICE, type="oncall:alert.fired", service="api", severity=8, message="5xx"),
    event(
        5,
        REACTOR,
        type="reflexr:rule_fired",
        rule="oncall:triage",
        scope={"service": "api"},
        scope_key='["api"]',
        firing_id="fir_1",
        matched=[4, 5, 6],
    ),
    event(6, REACTOR, type="reflexr:run_started", run_id="fir_1", rule="oncall:triage", attempt=1),
    event(7, TRIAGE, type="oncall:incident.opened", service="api", severity=8, summary="v2 fails"),
    event(
        8, REACTOR, type="reflexr:run_succeeded", run_id="fir_1", rule="oncall:triage", output={}
    ),
    event(
        9,
        REACTOR,
        type="reflexr:run_progressed",
        run_id="fir_2",
        rule="oncall:runbook",
        step="diagnose",
    ),
    event(
        10,
        REACTOR,
        type="reflexr:run_retrying",
        run_id="fir_2",
        rule="oncall:runbook",
        attempt=1,
        error="api is still\n  unhealthy",  # one line, however many the error has
    ),
    event(11, MCP, type="oncall:incident.resolved", service="api", resolution="rolled back to v1"),
    event(
        12,
        REACTOR,
        type="reflexr:rule_errored",
        rule="oncall:triage",
        seq=3,
        error="no field 'service'",
    ),
    event(
        13, ALICE, type="reflexr:rule_reset", rule="oncall:triage", generation=2, reason="replayed"
    ),
    event(
        14,
        REACTOR,
        type="reflexr:run_dead_lettered",
        run_id="fir_3",
        rule="oncall:page",
        attempts=5,
        error="x",
    ),
    event(
        15,
        ALICE,
        type="reflexr:run_skipped",
        run_id="fir_3",
        rule="oncall:page",
        reason="done by hand",
    ),
    event(16, ALICE, type="reflexr:run_cancelled", run_id="fir_4", rule="oncall:page"),
    event(17, ALICE, type="reflexr:run_requeued", run_id="fir_3", rule="oncall:page"),
    event(
        18, {"kind": "system", "name": "scheduler"}, type="reflexr:tick", schedule="heartbeat-check"
    ),
    event(19, ALICE, type="reflexr:feedback_given", feedback_type="useful", value={"ok": True}),
    event(20, {"kind": "source"}, type="custom", data=1),
    {"type": "command_result", "command_id": "cmd_1", "ok": True, "outcome": {}},
    {"type": "command_result", "command_id": "cmd_2", "ok": False, "rejection": {"message": "no"}},
    {"type": "error", "message": "not a command frame"},
]


def test_frames_render_one_line_per_event() -> None:
    lines = [plain(line) for frame in FRAMES if (line := render(frame)) is not None]
    assert lines == [
        "Watching prod, 2 events so far.",
        "   1 09:00:05 heartbeat api",
        "   2 09:00:05 deploy api v2 · alice",
        "── live after seq 2 ──",
        "   3 09:00:05 alert api sev 4: slow [p99] · alice",
        "   4 09:00:05 alert api sev 8: 5xx · alice",
        "   5 09:00:05 rule oncall:triage fired for service=api on seq 4, 5, 6 · reactor",
        "   6 09:00:05 run fir_1 of oncall:triage started, attempt 1 · reactor",
        "   7 09:00:05 INCIDENT OPENED api sev 8: v2 fails · triage",
        "   8 09:00:05 run fir_1 of oncall:triage succeeded · reactor",
        "   9 09:00:05 run fir_2 finished step diagnose · reactor",
        "  10 09:00:05 run fir_2 failed attempt 1, will retry: api is still unhealthy · reactor",
        "  11 09:00:05 INCIDENT RESOLVED api: rolled back to v1 · mcp",
        "  12 09:00:05 rule oncall:triage could not evaluate seq 3: no field 'service' · reactor",
        "  13 09:00:05 rule oncall:triage reset (replayed), generation 2 · alice",
        "  14 09:00:05 run fir_3 gave up after 5 attempts: x · reactor",
        "  15 09:00:05 run fir_3 skipped: done by hand · alice",
        "  16 09:00:05 run fir_4 cancelled · alice",
        "  17 09:00:05 run fir_3 requeued · alice",
        "  18 09:00:05 tick heartbeat-check",
        "  19 09:00:05 reflexr:feedback_given "
        '{"feedback_type": "useful", "value": {"ok": true}} · alice',
        '  20 09:00:05 custom {"data": 1} · source',
        "✗ no",
        "✗ not a command frame",
    ]


def test_incidents_rules_and_runs_stand_out() -> None:
    marked = {f["event"]["type"]: str(render(f)) for f in FRAMES if f["type"] == "event"}
    assert "[bold red]INCIDENT OPENED api" in marked["oncall:incident.opened"]
    assert "[bold green]INCIDENT RESOLVED api" in marked["oncall:incident.resolved"]
    assert "[bold magenta]rule oncall:triage fired" in marked["reflexr:rule_fired"]
    assert "[bold cyan]run fir_1 of oncall:triage succeeded" in marked["reflexr:run_succeeded"]
    assert "[dim]heartbeat api" in marked["oncall:service.heartbeat"]
    mild, severe = (str(render(frame)) for frame in FRAMES[4:6])
    assert "[yellow]alert api sev 4" in mild
    assert "[bold yellow]alert api sev 8" in severe


def test_runs_and_results_render_as_lines() -> None:
    run: dict[str, Any] = {
        "id": "fir_1",
        "rule": "oncall:runbook",
        "scope": {"service": "api"},
        "status": "retrying",
        "attempts": 2,
        "step": "roll_back",
        "error": "RuntimeError: api is\nstill unhealthy",
    }
    assert plain(render_run(run)) == (
        "fir_1  oncall:runbook  service=api      retrying, attempt 2, after step roll_back: "
        "RuntimeError: api is still unhealthy"
    )
    quiet = {**run, "scope": {}, "status": "pending", "attempts": 0, "step": None, "error": None}
    assert plain(render_run(quiet)) == "fir_1  oncall:runbook  the workspace    pending"
    recovered = {**run, "status": "succeeded"}  # a succeeded run keeps its last error
    assert (
        plain(render_run(recovered))
        == "fir_1  oncall:runbook  service=api      succeeded, attempt 2"
    )
    done = {"ok": True, "outcome": {"type": "run", "run": {**run, "status": "skipped"}}}
    assert render_result(done) == "run fir_1 is skipped"
    refused = {"ok": False, "rejection": {"type": "not_found", "message": "no run [x]"}}
    assert plain(render_result(refused)) == "✗ no run [x]"


@pytest.mark.parametrize(
    ("body", "text"),
    [
        ({"detail": {"type": "not_found", "message": "no event type 'x'"}}, "no event type 'x'"),
        ({"detail": [{"loc": ["body"], "msg": "Field required"}]}, "Field required"),
        ({"detail": "Not Found"}, "Not Found"),
        (["odd"], "['odd']"),
    ],
)
def test_error_bodies_become_messages(body: Any, text: str) -> None:
    assert error_text(body) == text


class Replay:
    """A connection that sends some frames, then ends."""

    def __init__(self, *frames: dict[str, Any]) -> None:
        self.sent: list[str] = []
        self._frames = [json.dumps(frame) for frame in frames]

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def __aiter__(self) -> AsyncIterator[str]:
        for frame in self._frames:
            yield frame


async def test_following_ends_with_the_stream() -> None:
    console, out = terminal()
    socket = Replay(FRAMES[0], FRAMES[3], FRAMES[-3])  # the last renders nothing
    assert not await follow(socket, console, after=3, until="oncall:incident.resolved")
    assert socket.sent == ['{"type": "hello", "protocol": "reflexr.v1", "resume_after_seq": 3}']
    assert out.getvalue().splitlines() == [
        "Watching prod, 2 events so far.",
        "── live after seq 2 ──",
        "The server closed the stream.",
    ]


# ─── commands against a running server ───────────────────────────────────────


async def oncall_cli(server: str, *argv: str) -> tuple[int, str]:
    """Run one ``oncall`` command against the server, returning its status and output."""
    console, out = terminal()
    status = await cli.run(args("--url", server, "--user", "alice", *argv), console)
    return status, out.getvalue()


async def test_watching_an_incident_from_alert_to_resolution(
    server: str, oncall: Oncall, script: Script, clock: FakeClock
) -> None:
    script.steps += triage(summary="api v2 fails")
    console, out = terminal()
    watching = asyncio.create_task(
        cli.run(args("--url", server, "watch", "--until", "oncall:incident.resolved"), console)
    )
    await eventually(lambda: "── live after seq 0 ──" in out.getvalue())

    for version in ("v1", "v2"):
        assert (await oncall_cli(server, "deploy", "api", "--version", version))[0] == 0
    for n in range(3):
        status, said = await oncall_cli(
            server, "alert", "api", "--severity", "9", "--message", "5xx"
        )
        assert (status, said) == (0, f"Published oncall:alert.fired at seq {n + 3}.\n")
    await oncall.reactor.settle()

    assert await asyncio.wait_for(watching, 5) == 0
    transcript = out.getvalue()
    for expected in (
        "   3 09:00:00 alert api sev 9: 5xx · alice",
        "rule oncall:triage fired for service=api on seq 3, 4, 5 · reactor",
        "INCIDENT OPENED api sev 8: api v2 fails · triage",
        "rule oncall:runbook fired for service=api",
        "finished step roll_back",
        "INCIDENT RESOLVED api: api v2 was deployed just before the incident; rolled api back",
    ):
        assert expected in transcript
    assert transcript.rstrip().splitlines()[-1].endswith("· runbook")

    status, listed = await oncall_cli(server, "runs")
    assert status == 0
    runbook, triaged = listed.splitlines()
    assert "oncall:runbook  service=api      succeeded" in runbook
    assert "triage   service=api      succeeded" in triaged
    assert await oncall_cli(server, "runs", "--rule", "oncall:silence") == (0, "No runs.\n")


async def test_operating_a_stuck_run(server: str, oncall: Oncall) -> None:
    oncall.deps.deployer.unhealthy["api"] = 5  # the runbook's verification keeps failing
    for version in ("v1", "v2"):
        await oncall_cli(server, "deploy", "api", "--version", version)
    async with httpx.AsyncClient(base_url=server) as http:
        await publish(
            http, type="oncall:incident.opened", service="api", severity=9, summary="down"
        )
    await oncall.reactor.settle()
    status, listed = await oncall_cli(server, "runs", "--status", "retrying")
    assert status == 0
    run_id = listed.split()[0]
    assert "after step roll_back: RuntimeError: api is still unhealthy" in listed

    assert await oncall_cli(server, "cancel", run_id) == (0, f"run {run_id} is cancelled\n")
    assert await oncall_cli(server, "retry", run_id) == (0, f"run {run_id} is pending\n")
    assert await oncall_cli(server, "skip", run_id, "--reason", "fixed by hand") == (
        0,
        f"run {run_id} is skipped\n",
    )
    status, said = await oncall_cli(server, "cancel", run_id)
    assert (status, said.startswith("✗ ")) == (1, True)
    status, said = await oncall_cli(server, "retry", "fir_nope")
    assert (status, said) == (1, "✗ run fir_nope does not exist\n")


async def test_a_server_that_refuses_is_reported(server: str) -> None:
    status, said = await oncall_cli(f"{server}/nothing", "heartbeat", "api")
    assert (status, said) == (1, "✗ 404: Not Found\n")


def test_main_exits_with_the_status_of_its_command(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []

    async def run(parsed: argparse.Namespace, console: Console) -> int:
        ran.append(parsed.command)
        return 1

    monkeypatch.setattr(cli, "run", run)
    with pytest.raises(SystemExit) as exited:
        cli.main(["heartbeat", "api"])
    assert (exited.value.code, ran) == (1, ["heartbeat"])


def test_main_stops_quietly_when_interrupted(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run(parsed: argparse.Namespace, console: Console) -> int:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run", run)
    with pytest.raises(SystemExit) as exited:
        cli.main(["watch"])
    assert exited.value.code == 130


@pytest.mark.parametrize("command", [["heartbeat", "api"], ["watch"]])
def test_main_explains_a_connection_failure(command: list[str]) -> None:
    with pytest.raises(SystemExit, match=r"oncall: cannot talk to http://127\.0\.0\.1:9: "):
        cli.main(["--url", "http://127.0.0.1:9", *command])
