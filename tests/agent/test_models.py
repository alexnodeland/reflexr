"""function_model: one plain function answers a model's plain and streamed requests."""

from collections.abc import AsyncIterable
from typing import Any

import pytest
from pydantic_ai import (
    Agent,
    AgentStreamEvent,
    BinaryContent,
    FilePart,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    PartStartEvent,
    RunContext,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo

from reflexr.agent import function_model


def think_then_say(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(parts=[ThinkingPart("Short."), TextPart("On it.")])


async def call_then_answer(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    last = messages[-1]
    assert isinstance(last, ModelRequest)
    returned = [p.content for p in last.parts if isinstance(p, ToolReturnPart)]
    if returned:
        return ModelResponse(parts=[TextPart(f"It is {returned[0]}.")])
    return ModelResponse(parts=[ToolCallPart("double", {"x": 2}, tool_call_id="c1")])


class Events:
    """An event stream handler that keeps what it was given."""

    def __init__(self) -> None:
        self.events: list[AgentStreamEvent] = []

    async def __call__(self, ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]) -> None:
        async for event in stream:
            self.events.append(event)


async def test_plain_and_streamed_requests_get_the_same_response() -> None:
    agent = Agent(function_model(think_then_say))
    assert (await agent.run("Go")).output == "On it."
    handler = Events()
    streamed = await agent.run("Go", event_stream_handler=handler)
    assert streamed.output == "On it."
    started = [e.part for e in handler.events if isinstance(e, PartStartEvent)]
    assert started == [ThinkingPart("Short."), TextPart("On it.")], "each part, in order"


async def test_an_async_function_calls_tools_in_a_streamed_run() -> None:
    agent = Agent(function_model(call_then_answer))

    @agent.tool_plain
    def double(x: int) -> int:
        """Double a number."""
        return x * 2

    result = await agent.run("What is 2 doubled?", event_stream_handler=Events())
    assert result.output == "It is 4."


async def test_a_part_the_stream_cannot_carry_fails_the_request() -> None:
    def draw(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[FilePart(BinaryContent(b"\x89PNG", media_type="image/png"))])

    agent = Agent(function_model(draw))
    with pytest.raises(ValueError, match="cannot stream a file part"):
        await agent.run("Draw", event_stream_handler=Events())
