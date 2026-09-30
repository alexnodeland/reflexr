"""A scripted model, a clock tests move by hand, and the oncall app in process or on a port.

No test calls a model API: the triage agent's model is a :class:`Script` that answers each
request with its next step. No test waits for the reactor either: the app is built with
``serve=False`` and tests drive it with ``reactor.settle()``, except the one test of ``serve``.
"""

import asyncio
import socket
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import uvicorn
from pydantic_ai import ModelMessage, ModelRequest, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from oncall.app import Oncall

BASE = "/v1/workspaces/prod"
START = datetime(2026, 9, 28, 9, 0, tzinfo=UTC)


def call(tool: str, **args: Any) -> ModelResponse:
    """A step that calls one tool."""
    return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=args)])


def triage(service: str = "api", severity: int = 8, summary: str = "api is failing") -> list[Any]:
    """The steps of a triage that opens an incident, then answers with its verdict."""
    incident = {"service": service, "severity": severity, "summary": summary}
    return [
        call("read_events", types=["oncall:deploy.completed"]),
        call("emit_event", type="oncall:incident.opened", fields=incident),
        call("final_result", severity=severity, summary=summary),
    ]


class Script:
    """A model that answers each request with the next step, recording what it was sent."""

    def __init__(self, *steps: ModelResponse) -> None:
        self.steps = list(steps)
        self.requests: list[list[ModelMessage]] = []
        self.tools: list[list[str]] = []

    @property
    def model(self) -> FunctionModel:
        return FunctionModel(self._respond)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.requests.append(list(messages))
        self.tools.append([tool.name for tool in info.function_tools])
        return self.steps.pop(0)

    def sent(self, request: int) -> list[str]:
        """The prompts and tool results sent in a request."""
        last = self.requests[request][-1]
        assert isinstance(last, ModelRequest)
        return [str(getattr(part, "content", "")) for part in last.parts]


class FakeClock:
    """A clock tests move by hand."""

    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


async def eventually(check: Callable[[], object], seconds: float = 5.0) -> None:
    """Wait, polling, until ``check()`` is truthy."""
    for _ in range(int(seconds * 100)):
        if check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timed out")


@pytest.fixture
def script() -> Script:
    return Script()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def oncall(script: Script, clock: FakeClock) -> Oncall:
    return Oncall(model=script.model, clock=clock)


@pytest.fixture
async def client(oncall: Oncall) -> AsyncIterator[httpx.AsyncClient]:
    """The app in process, as alice. The reactor runs only when a test settles it."""
    transport = httpx.ASGITransport(app=oncall.app(serve=False))
    async with httpx.AsyncClient(
        transport=transport, base_url="http://oncall", headers={"x-user": "alice"}
    ) as client:
        yield client


@pytest.fixture
async def server(oncall: Oncall) -> AsyncIterator[str]:
    """Serve the app on a free port in this event loop, and return its URL."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    config = uvicorn.Config(oncall.app(serve=False), ws="websockets-sansio", log_level="warning")
    served = uvicorn.Server(config)
    serving = asyncio.create_task(served.serve(sockets=[listener]))
    await eventually(lambda: served.started)
    yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    served.should_exit = True
    await serving


async def publish(client: httpx.AsyncClient, **event: Any) -> dict[str, Any]:
    """Publish one event over REST and return its outcome."""
    response = await client.post(f"{BASE}/events", json={"event": event})
    assert response.status_code == 200, response.text
    [outcome] = response.json()
    return outcome


async def log(client: httpx.AsyncClient, *types: str) -> list[dict[str, Any]]:
    """The envelopes in the log, optionally of some types."""
    response = await client.get(f"{BASE}/events", params={"type": list(types)} if types else {})
    return response.json()
