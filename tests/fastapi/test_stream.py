"""The workspace protocol over WebSocket."""

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketTestSession
from starlette.websockets import WebSocketDisconnect

from tests.fastapi.conftest import build

STREAM = "/v1/workspaces/prod/stream"
ERROR = {"type": "app:service.error", "service": "auth"}
DEPLOY = {"type": "app:deploy.finished", "service": "auth"}


@pytest.fixture
def client() -> Iterator[TestClient]:
    app, _, _ = build()
    with TestClient(app) as client:
        yield client


def hello(**options: Any) -> dict[str, Any]:
    return {"type": "hello", "protocol": "reflexr.v1", **options}


def command(command_id: str, **body: Any) -> dict[str, Any]:
    return {"type": "command", "command_id": command_id, "command": body}


def publish(client: TestClient, event: dict[str, Any]) -> None:
    client.post("/v1/workspaces/prod/events", json={"event": event})


def until(ws: WebSocketTestSession, frame_type: str, limit: int = 50) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for _ in range(limit):
        frames.append(ws.receive_json())
        if frames[-1]["type"] == frame_type:
            return frames
    raise AssertionError(f"no {frame_type} frame in {frames}")


def test_hello_welcome_replay_then_live(client: TestClient) -> None:
    publish(client, ERROR)
    publish(client, DEPLOY)
    with client.websocket_connect(STREAM, subprotocols=["reflexr.v1"]) as ws:
        assert ws.accepted_subprotocol == "reflexr.v1"
        ws.send_json(hello(types=["app:service.error"]))
        welcome = ws.receive_json()
        assert (welcome["type"], welcome["head_seq"], welcome["reset"]) == ("welcome", 2, False)
        replayed = until(ws, "replay_complete")
        assert [f.get("seq") for f in replayed] == [1, None], "the deploy is filtered out"
        assert replayed[-1] == {"type": "replay_complete", "up_to_seq": 2}
        publish(client, DEPLOY)  # not received
        publish(client, ERROR)
        live = ws.receive_json()
        assert (live["type"], live["seq"], live["event"]["type"]) == (
            "event",
            4,
            "app:service.error",
        )


def test_an_up_to_date_client_gets_replay_complete_at_once(client: TestClient) -> None:
    publish(client, ERROR)
    with client.websocket_connect(STREAM) as ws:
        assert ws.accepted_subprotocol is None
        ws.send_json(hello(resume_after_seq=1))
        assert ws.receive_json()["type"] == "welcome"
        assert ws.receive_json() == {"type": "replay_complete", "up_to_seq": 1}


def test_a_client_from_the_head_replays_nothing_and_follows_live(client: TestClient) -> None:
    publish(client, ERROR)
    publish(client, DEPLOY)
    with client.websocket_connect(STREAM) as ws:
        ws.send_json(hello(from_head=True, types=["app:service.error"]))
        welcome = ws.receive_json()
        assert (welcome["head_seq"], welcome["reset"]) == (2, False)
        assert ws.receive_json() == {"type": "replay_complete", "up_to_seq": 2}
        publish(client, DEPLOY)  # not received
        publish(client, ERROR)
        live = ws.receive_json()
        assert (live["type"], live["seq"]) == ("event", 4)


def test_a_client_from_the_head_cannot_also_resume(client: TestClient) -> None:
    publish(client, ERROR)
    with client.websocket_connect(STREAM) as ws:
        ws.send_json(hello(from_head=True, resume_after_seq=1))
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
    assert (closed.value.code, closed.value.reason) == (
        4400,
        "from_head replays nothing, so resume_after_seq must be 0",
    )


def test_a_client_ahead_of_the_log_is_reset(client: TestClient) -> None:
    with client.websocket_connect(STREAM) as ws:
        ws.send_json(hello(resume_after_seq=99))
        assert ws.receive_json()["reset"] is True


@pytest.mark.parametrize(
    ("path", "headers", "code"),
    [(STREAM, {"x-token": "bad"}, 4401), ("/v1/workspaces/secret/stream", {}, 4403)],
)
def test_refused_connections_are_closed_with_a_reason(
    client: TestClient, path: str, headers: dict[str, str], code: int
) -> None:
    with client.websocket_connect(path, headers=headers) as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
        assert closed.value.code == code


@pytest.mark.parametrize(
    "first",
    [
        {"type": "command"},
        hello() | {"protocol": "reflexr.v0"},
        hello() | {"types": ["rule_fired"]},  # a type needs its namespace: reflexr:rule_fired
    ],
)
def test_a_bad_first_frame_closes_the_connection(client: TestClient, first: dict[str, Any]) -> None:
    with client.websocket_connect(STREAM) as ws:
        ws.send_json(first)
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
    assert closed.value.code == 4400


def test_hello_must_arrive_in_time() -> None:
    app, _, _ = build(hello_timeout=0.05)
    with TestClient(app) as client, client.websocket_connect(STREAM) as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
        assert closed.value.code == 4408


def test_commands_invalid_frames_and_rejections(client: TestClient) -> None:
    with client.websocket_connect(STREAM) as ws:
        ws.send_json(hello())
        until(ws, "replay_complete")
        ws.send_json(command("c1", type="publish", event=ERROR, id="e1"))
        frames = [ws.receive_json(), ws.receive_json()]
        [result] = [f for f in frames if f["type"] == "command_result"]
        assert (result["command_id"], result["ok"], result["outcome"]["seq"]) == ("c1", True, 1)
        ws.send_json({"type": "nonsense"})
        error = ws.receive_json()
        assert (error["type"], error["message"].startswith("not a command frame: ")) == (
            "error",
            True,
        )
        ws.send_json(command("c2", type="skip_run", run_id="nope"))
        rejected = ws.receive_json()
        assert (rejected["ok"], rejected["rejection"]["type"]) == (False, "not_found")


def test_a_command_that_crashes_reports_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def explode(*args: Any) -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr("reflexr.fastapi.router.execute", explode)
    app, _, _ = build()
    with TestClient(app) as client, client.websocket_connect(STREAM) as ws:
        ws.send_json(hello())
        until(ws, "replay_complete")
        ws.send_json(command("c1", type="publish", event=ERROR))
        assert ws.receive_json() == {"type": "error", "message": "command c1 failed on the server"}


def test_a_client_that_cannot_keep_up_is_disconnected() -> None:
    app, _, _ = build(outbox_size=1)
    with TestClient(app) as client:
        for _ in range(30):
            publish(client, ERROR)
        with client.websocket_connect(STREAM) as ws:
            ws.send_json(hello())
            with pytest.raises(WebSocketDisconnect) as closed:
                drain(ws)
            assert closed.value.code == 4429


def drain(ws: WebSocketTestSession) -> None:
    for _ in range(100):
        ws.receive_json()
