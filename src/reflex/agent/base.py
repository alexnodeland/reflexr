"""Agent protocol and base implementation.

Agents are the core processing units that react to events.
They use PydanticAI for LLM integration with type-safe tools.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logfire
from pydantic import BaseModel
from pydantic_ai import Agent as PydanticAgent

from reflex.config import settings

if TYPE_CHECKING:
    from reflex.core.context import AgentContext

# Type variable for structured output
T = TypeVar("T", bound=BaseModel)


class Agent(ABC):
    """Protocol for event-handling agents.

    Agents are triggered by events and execute business logic.
    They receive an AgentContext with the event and dependencies.
    """

    @abstractmethod
    async def run(self, ctx: AgentContext) -> None:
        """Execute the agent logic.

        Args:
            ctx: The execution context with event and dependencies
        """
        ...


class BaseAgent(Agent, Generic[T]):
    """Base agent implementation using PydanticAI.

    Provides LLM integration with:
    - Automatic tool registration
    - Structured output parsing
    - Full observability via Logfire

    Subclasses should:
    1. Define output_type class variable for structured responses
    2. Override get_system_prompt() for agent personality
    3. Override process_result() for post-processing
    4. Use @tool decorator to register tools

    Example:
        class SummaryOutput(BaseModel):
            summary: str
            keywords: list[str]

        class SummarizerAgent(BaseAgent[SummaryOutput]):
            output_type = SummaryOutput

            def get_system_prompt(self) -> str:
                return "You are a text summarizer."

            async def process_result(
                self, result: SummaryOutput, ctx: AgentContext
            ) -> None:
                await ctx.publish(SummaryEvent(
                    source="agent:summarizer",
                    summary=result.summary,
                    **ctx.derive_event(),
                ))
    """

    output_type: type[T] | None = None
    model: str = settings.default_model

    def __init__(self) -> None:
        """Initialize the agent with PydanticAI."""
        self._agent: PydanticAgent[Any, T] | None = None
        self._tools: list[Any] = []

    def _get_agent(self) -> PydanticAgent[Any, T]:
        """Get or create the PydanticAI agent instance."""
        if self._agent is None:
            agent: PydanticAgent[Any, T] = PydanticAgent(
                self.model,
                result_type=self.output_type or str,  # type: ignore[arg-type]
                system_prompt=self.get_system_prompt(),
            )
            # Register any tools
            for tool in self._tools:
                agent.tool(tool)
            self._agent = agent
        return self._agent

    def get_system_prompt(self) -> str:
        """Get the system prompt for the LLM.

        Override this to customize agent behavior.

        Returns:
            The system prompt string
        """
        return "You are a helpful assistant."

    def get_user_prompt(self, ctx: AgentContext) -> str:
        """Get the user prompt from the event context.

        Override this to customize how events are converted to prompts.

        Args:
            ctx: The agent context with the triggering event

        Returns:
            The user prompt string
        """
        # Default: use event content if available, otherwise serialize
        event = ctx.event
        content = getattr(event, "content", None)
        if content is not None:
            return str(content)
        return event.model_dump_json()

    async def process_result(self, result: T, ctx: AgentContext) -> None:
        """Process the LLM result.

        Override this to handle the structured output, e.g., publish
        derived events or update state.

        Args:
            result: The parsed LLM output
            ctx: The agent context
        """
        pass

    async def run(self, ctx: AgentContext) -> None:
        """Execute the agent.

        1. Build prompt from context
        2. Call LLM via PydanticAI
        3. Process structured result
        """
        agent_name = self.__class__.__name__
        with logfire.span(
            "agent.run",
            agent=agent_name,
            event_id=ctx.event.id,
            event_type=ctx.event.type,
        ):
            try:
                agent = self._get_agent()
                user_prompt = self.get_user_prompt(ctx)

                logfire.info(
                    "Agent executing",
                    agent=agent_name,
                    prompt_length=len(user_prompt),
                )

                result = await agent.run(user_prompt)
                result_data = result.data  # type: ignore[attr-defined]

                logfire.info(
                    "Agent completed",
                    agent=agent_name,
                    result_type=type(result_data).__name__,  # type: ignore[arg-type]
                )

                await self.process_result(result_data, ctx)  # type: ignore[arg-type]

            except Exception as e:
                logfire.error(
                    "Agent failed",
                    agent=agent_name,
                    error=str(e),
                )
                raise


class SimpleAgent(Agent):
    """Simple agent without LLM integration.

    Use this for agents that don't need LLM capabilities,
    just event processing logic.

    Can be used in two ways:

    1. Functional style (recommended for simple cases):
        async def process(ctx: AgentContext) -> dict:
            return {"processed": ctx.event.id}

        agent = SimpleAgent(process)

    2. Subclass style (for more complex agents):
        class LoggingAgent(SimpleAgent):
            async def handle(self, ctx: AgentContext) -> Any:
                print(f"Received event: {ctx.event.id}")
    """

    _handler: Callable[[AgentContext], Awaitable[Any]] | None = None

    def __init__(
        self,
        handler: Callable[[AgentContext], Awaitable[Any]] | None = None,
    ) -> None:
        """Initialize the agent.

        Args:
            handler: Optional async function to handle events.
                     If not provided, override the handle() method instead.
        """
        self._handler = handler

    async def run(self, ctx: AgentContext) -> None:
        """Execute the agent by calling handle()."""
        agent_name = self._handler.__name__ if self._handler else self.__class__.__name__
        with logfire.span(
            "agent.run",
            agent=agent_name,
            event_id=ctx.event.id,
            event_type=ctx.event.type,
        ):
            await self.handle(ctx)

    async def handle(self, ctx: AgentContext) -> Any:
        """Handle the event.

        Override this with your event processing logic,
        or pass a handler function to __init__.

        Args:
            ctx: The agent context with event and dependencies

        Returns:
            Any result from processing
        """
        if self._handler is not None:
            return await self._handler(ctx)
        raise NotImplementedError("Either pass a handler to __init__ or override handle()")
