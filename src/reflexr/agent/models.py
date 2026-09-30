"""A model from one plain function, for tests and offline runs.

An agent's requests are streamed whenever a capability wraps the run's event stream, as
:class:`~reflexr.litellm.LiteLLMGateway` does, and pydantic-ai's ``FunctionModel`` answers a
streamed request only with a stream function of its own. :func:`function_model` derives that
stream from the function's response.
"""

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable

from pydantic_ai import ModelMessage, ModelResponse, TextPart, ThinkingPart, ToolCallPart
from pydantic_ai.models.function import (
    AgentInfo,
    DeltaThinkingCalls,
    DeltaThinkingPart,
    DeltaToolCall,
    DeltaToolCalls,
    FunctionModel,
)

type Respond = Callable[[list[ModelMessage], AgentInfo], ModelResponse | Awaitable[ModelResponse]]
"""Answers a model request: pydantic-ai's ``FunctionDef``, sync or async."""


def function_model(respond: Respond) -> FunctionModel:
    """Return a pydantic-ai ``FunctionModel`` that answers every request with ``respond``.

    A plain request gets the response as it is. A streamed request, such as each request of an
    agent the ``LiteLLMGateway`` capability wraps, gets it streamed: each text part as one text
    delta, each thinking part as one thinking delta, and each tool call whole. Script a model
    for a test with it, rather than writing a stream function beside the function::

        def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("On it.")])


        with agent.override(model=function_model(respond)):
            ...

    Args:
        respond: Answers each request from the messages so far, as ``FunctionModel``'s
            function does; it may be async.

    Raises:
        ValueError: When a streamed response has a part other than text, thinking or a tool
            call, which the stream cannot carry.
    """

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[str | DeltaToolCalls | DeltaThinkingCalls]:
        answered = respond(messages, info)
        response = await answered if inspect.isawaitable(answered) else answered
        for index, part in enumerate(response.parts):
            match part:
                case TextPart():
                    yield part.content
                case ThinkingPart():
                    thought = DeltaThinkingPart(content=part.content, signature=part.signature)
                    yield {index: thought}
                case ToolCallPart():
                    args = part.args_as_json_str()
                    call = DeltaToolCall(part.tool_name, args, tool_call_id=part.tool_call_id)
                    yield {index: call}
                case _:
                    raise ValueError(f"function_model cannot stream a {part.part_kind} part")

    return FunctionModel(respond, stream_function=stream)
