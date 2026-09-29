"""A terminal client for oncall: publish events, watch the log live, and operate runs.

::

    oncall watch                                   # follow the log, one line per event
    oncall alert api --severity 8 --message "5xx rate above 5%"
    oncall deploy api --version v42
    oncall heartbeat api
    oncall runs                                    # the runs, newest first
    oncall retry RUN_ID                            # or skip, or cancel

Publishing and runs use REST; ``watch`` speaks the WebSocket stream protocol: send ``hello``,
render the replay up to ``replay_complete``, then render events as they are appended. Building
requests and rendering frames are plain functions over JSON, so a client in any language follows
the same shape.
"""

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Protocol

import httpx
from rich.console import Console
from rich.markup import escape
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException
from websockets.typing import Subprotocol

from reflexr import new_id
from reflexr.core import PROTOCOL

type Frame = dict[str, Any]
"""A protocol frame, event or run as JSON."""

STATUSES = ("pending", "running", "retrying", "succeeded", "dead", "cancelled", "skipped")


def severity(text: str) -> int:
    """Parse an alert's severity: a whole number from 1 to 10."""
    value = int(text)
    if not 1 <= value <= 10:
        raise argparse.ArgumentTypeError("a severity is from 1 to 10")
    return value


def parser() -> argparse.ArgumentParser:
    """The command line: shared connection options, then one subcommand."""
    main = argparse.ArgumentParser(prog="oncall", description="Talk to an oncall server.")
    main.add_argument("--url", default="http://127.0.0.1:8000", help="the server's base URL")
    main.add_argument("--workspace", default="prod", help="the workspace (default: prod)")
    main.add_argument("--user", default="guest", help="who you are (demo authentication)")
    commands = main.add_subparsers(dest="command", required=True, metavar="COMMAND")

    alert = commands.add_parser("alert", help="publish alert.fired, as monitoring would")
    alert.add_argument("service")
    alert.add_argument("--severity", type=severity, required=True, metavar="1-10")
    alert.add_argument("--message", required=True)
    deploy = commands.add_parser("deploy", help="publish deploy.completed, as CI would")
    deploy.add_argument("service")
    deploy.add_argument("--version", required=True)
    heartbeat = commands.add_parser("heartbeat", help="publish service.heartbeat")
    heartbeat.add_argument("service")

    watch = commands.add_parser("watch", help="follow the log live, one line per event")
    watch.add_argument("--after", type=int, default=0, metavar="SEQ", help="replay after SEQ")
    watch.add_argument("--until", metavar="TYPE", help="stop after an event of this type")

    runs = commands.add_parser("runs", help="list runs, newest first")
    runs.add_argument("--rule")
    runs.add_argument("--status", choices=STATUSES)
    runs.add_argument("--limit", type=int, default=20)
    commands.add_parser("retry", help="make a run runnable now").add_argument("run_id")
    for name, text in (("skip", "give up on a waiting run"), ("cancel", "cancel a run")):
        operate = commands.add_parser(name, help=text)
        operate.add_argument("run_id")
        operate.add_argument("--reason")
    return main


# ─── requests ────────────────────────────────────────────────────────────────


def event_of(args: argparse.Namespace) -> Frame:
    """The event ``alert``, ``deploy`` or ``heartbeat`` publishes."""
    match args.command:
        case "alert":
            return {
                "type": "alert.fired",
                "service": args.service,
                "severity": args.severity,
                "message": args.message,
            }
        case "deploy":
            return {"type": "deploy.completed", "service": args.service, "version": args.version}
        case _:
            return {"type": "service.heartbeat", "service": args.service}


def command_of(args: argparse.Namespace) -> Frame:
    """The command frame ``retry``, ``skip`` or ``cancel`` sends."""
    command: Frame = {"type": f"{args.command}_run", "run_id": args.run_id}
    if args.command != "retry":
        command["reason"] = args.reason
    return {"type": "command", "command_id": new_id("cmd"), "command": command}


def hello(after_seq: int) -> Frame:
    """The first frame on the stream: replay everything after ``after_seq``, then go live."""
    return {"type": "hello", "protocol": PROTOCOL, "resume_after_seq": after_seq}


# ─── rendering ───────────────────────────────────────────────────────────────


def render(frame: Frame) -> str | None:
    """Render a server frame as one line of rich markup, or None for frames not shown."""
    match frame["type"]:
        case "welcome":
            return f"[dim]Watching {frame['workspace_id']}, {frame['head_seq']} events so far.[/]"
        case "replay_complete":
            return f"[dim]── live after seq {frame['up_to_seq']} ──[/]"
        case "event":
            return render_event(frame)
        case "command_result" if not frame["ok"]:
            return f"[red]✗ {escape(frame['rejection']['message'])}[/]"
        case "error":
            return f"[red]✗ {escape(frame['message'])}[/]"
        case _:
            return None


def render_event(envelope: Frame) -> str:
    """Render an envelope: its seq and time, then what happened, styled by what it is.

    Incidents are the loudest, then rule firings and runs; routine traffic is dim.
    """
    at = datetime.fromisoformat(envelope["ts"]).strftime("%H:%M:%S")
    style, text = describe(envelope["event"])
    by = f" [dim]· {escape(actor_name(envelope['actor']))}[/]" if style != "dim" else ""
    return f"[dim]{envelope['seq']:>4} {at}[/] [{style}]{one_line(text)}[/]{by}"


def one_line(text: str) -> str:
    """Collapse the line breaks in text, such as a multi-line error, so it takes one line."""
    return " ".join(text.split())


def describe(event: Frame) -> tuple[str, str]:
    """A style and a few words of markup for an event."""
    match event:
        case {"type": "incident.opened", "service": service, "severity": severity}:
            return (
                "bold red",
                f"INCIDENT OPENED {service} sev {severity}: {escape(event['summary'])}",
            )
        case {"type": "incident.resolved", "service": service, "resolution": resolution}:
            return "bold green", f"INCIDENT RESOLVED {service}: {escape(resolution)}"
        case {"type": "alert.fired", "service": service, "severity": severity}:
            style = "bold yellow" if severity >= 7 else "yellow"
            return style, f"alert {service} sev {severity}: {escape(event['message'])}"
        case {"type": "deploy.completed", "service": service, "version": version}:
            return "blue", f"deploy {service} {escape(version)}"
        case {"type": "service.heartbeat", "service": service}:
            return "dim", f"heartbeat {service}"
        case {"type": "rule_fired", "rule": rule, "scope": scope, "matched": matched}:
            seqs = ", ".join(str(seq) for seq in matched)
            return "bold magenta", f"rule {rule} fired for {scope_text(scope)} on seq {seqs}"
        case {"type": "rule_errored", "rule": rule, "seq": seq, "error": error}:
            return "red", f"rule {rule} could not evaluate seq {seq}: {escape(error)}"
        case {"type": "rule_reset", "rule": rule, "reason": reason, "generation": generation}:
            return "magenta", f"rule {rule} reset ({reason}), generation {generation}"
        case {"type": "run_started", "run_id": run, "rule": rule, "attempt": attempt}:
            return "cyan", f"run {run} of {rule} started, attempt {attempt}"
        case {"type": "run_progressed", "run_id": run, "step": step}:
            return "cyan", f"run {run} finished step {step}"
        case {"type": "run_retrying", "run_id": run, "attempt": attempt, "error": error}:
            return "yellow", f"run {run} failed attempt {attempt}, will retry: {escape(error)}"
        case {"type": "run_succeeded", "run_id": run, "rule": rule}:
            return "bold cyan", f"run {run} of {rule} succeeded"
        case {"type": "run_dead_lettered", "run_id": run, "attempts": attempts, "error": error}:
            return "bold red", f"run {run} gave up after {attempts} attempts: {escape(error)}"
        case {"type": "run_cancelled" | "run_skipped" as kind, "run_id": run}:
            reason = event.get("reason")
            ending = f": {escape(reason)}" if reason else ""
            return "yellow", f"run {run} {kind.removeprefix('run_')}{ending}"
        case {"type": "run_requeued", "run_id": run}:
            return "cyan", f"run {run} requeued"
        case {"type": "tick", "schedule": schedule}:
            return "dim", f"tick {schedule}"
        case _:
            fields = {key: value for key, value in event.items() if key != "type"}
            return "default", f"{event['type']} {escape(json.dumps(fields, sort_keys=True))}"


def actor_name(actor: Frame) -> str:
    """Who published an event: a person, an agent or graph by its action, or a system."""
    return str(actor.get("name") or actor.get("id") or actor.get("client_id") or actor["kind"])


def scope_text(scope: Frame) -> str:
    """A scope as ``service=api``, or ``the workspace`` for a rule without one."""
    return ", ".join(f"{key}={value}" for key, value in scope.items()) or "the workspace"


def render_run(run: Frame) -> str:
    """Render a run as one line: its id, rule, scope and status, and where and why it is stuck."""
    done = run["status"] == "succeeded"
    style = {"succeeded": "green", "dead": "bold red", "retrying": "yellow"}.get(run["status"])
    status = f"[{style}]{run['status']}[/]" if style else run["status"]
    line = f"{run['id']}  {run['rule']:<8} {scope_text(run['scope']):<16} {status}"
    line += f", attempt {run['attempts']}" if run["attempts"] > 1 else ""
    line += f", after step {run['step']}" if run.get("step") and not done else ""
    error = f": {escape(one_line(run['error']))}" if run.get("error") and not done else ""
    return line + error


def render_result(result: Frame) -> str:
    """Render a run command's result: the run's new status, or why it was refused."""
    if not result["ok"]:
        return f"[red]✗ {escape(result['rejection']['message'])}[/]"
    run = result["outcome"]["run"]
    return f"run {run['id']} is {run['status']}"


def error_text(body: Any) -> str:
    """The message in an HTTP error's JSON body, as FastAPI and reflexr write them."""
    match body:
        case {"detail": {"message": str(message)}}:
            return message
        case {"detail": [{"msg": str(message)}, *_]}:
            return message
        case {"detail": detail}:
            return str(detail)
        case _:
            return str(body)


# ─── talking to the server ───────────────────────────────────────────────────


class Refused(Exception):
    """The server answered a request with an error."""


class Socket(Protocol):
    """The part of a WebSocket connection ``follow`` uses."""

    async def send(self, message: str) -> None:
        """Send a text frame."""
        ...

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        """Receive frames until the connection closes."""
        ...


async def follow(socket: Socket, console: Console, *, after: int, until: str | None) -> bool:
    """Say hello, then render frames until an event of type ``until`` or the end of the stream.

    Returns:
        Whether an event of type ``until`` arrived.
    """
    await socket.send(json.dumps(hello(after)))
    async for raw in socket:
        frame: Frame = json.loads(raw)
        line = render(frame)
        if line is not None:
            console.print(line, highlight=False)
        if frame["type"] == "event" and frame["event"]["type"] == until:
            return True
    console.print("[dim]The server closed the stream.[/]")
    return False


async def run(args: argparse.Namespace, console: Console) -> int:
    """Carry out one command against the server.

    Returns:
        The exit status: 0, or 1 if the server refused or the stream ended early.
    """
    base = f"{args.url.rstrip('/')}/v1/workspaces/{args.workspace}"
    headers = {"x-user": args.user}
    if args.command == "watch":
        async with connect(
            base.replace("http", "ws", 1) + "/stream",
            subprotocols=[Subprotocol(PROTOCOL)],
            additional_headers=headers,
        ) as socket:
            found = await follow(socket, console, after=args.after, until=args.until)
        return 0 if found or args.until is None else 1
    async with httpx.AsyncClient(base_url=base, headers=headers) as http:
        try:
            return await _request(http, args, console)
        except Refused as refused:
            console.print(f"[red]✗ {escape(str(refused))}[/]")
            return 1


async def _request(http: httpx.AsyncClient, args: argparse.Namespace, console: Console) -> int:
    match args.command:
        case "alert" | "deploy" | "heartbeat":
            event = event_of(args)
            [published] = _ok(await http.post("/events", json={"event": event}))
            console.print(f"Published {event['type']} at seq {published['seq']}.")
        case "runs":
            params = {"rule": args.rule, "status": args.status, "limit": args.limit}
            chosen = {key: value for key, value in params.items() if value is not None}
            runs = _ok(await http.get("/runs", params=chosen))
            for found in runs:
                console.print(render_run(found), highlight=False)
            if not runs:
                console.print("[dim]No runs.[/]")
        case _:
            # A refused command still answers with a command result, whatever its status.
            result = (await http.post("/commands", json=command_of(args))).json()
            console.print(render_result(result), highlight=False)
            return 0 if result["ok"] else 1
    return 0


def _ok(response: httpx.Response) -> Any:
    if response.is_error:
        raise Refused(f"{response.status_code}: {error_text(response.json())}")
    return response.json()


def main(argv: list[str] | None = None) -> None:
    """Run the terminal client."""
    args = parser().parse_args(argv)
    try:
        status = asyncio.run(run(args, Console()))
    except (OSError, httpx.TransportError, WebSocketException) as error:
        sys.exit(f"oncall: cannot talk to {args.url}: {error}")
    except KeyboardInterrupt:
        status = 130  # interrupted: the shell's convention for Ctrl-C
    sys.exit(status)
