"""A scripted model for agent tests: no test calls a model API."""

from collections.abc import Callable
from typing import Any

from pydantic_ai import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

Step = ModelResponse | Callable[[list[ModelMessage]], ModelResponse]


def say(text: str) -> ModelResponse:
    return ModelResponse(parts=[TextPart(text)])


def call(tool: str, call_id: str = "call_1", **args: Any) -> ModelResponse:
    return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=args, tool_call_id=call_id)])


class Script:
    """A model that answers each request with the next scripted step, recording requests."""

    def __init__(self, *steps: Step) -> None:
        self.steps = list(steps)
        self.requests: list[list[ModelMessage]] = []
        self.tools: list[list[str]] = []

    @property
    def model(self) -> FunctionModel:
        return FunctionModel(self._respond)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.requests.append(list(messages))
        self.tools.append([tool.name for tool in info.function_tools])
        step = self.steps.pop(0)
        return step(messages) if callable(step) else step

    def instructions(self, request: int) -> str:
        last = self.requests[request][-1]
        assert isinstance(last, ModelRequest)
        return last.instructions or ""

    def sent(self, request: int) -> list[str]:
        """The user prompts, tool results and retry prompts sent in a request."""
        last = self.requests[request][-1]
        assert isinstance(last, ModelRequest)
        return [str(getattr(part, "content", "")) for part in last.parts]
