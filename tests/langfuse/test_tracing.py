"""Whole traces in Langfuse, and each run's session, user, trace name, tags and metadata."""

from opentelemetry.sdk.trace import ReadableSpan

from reflexr import ExternalAgentActor, Rule, UserActor, on, run
from reflexr.langfuse import MAX_ATTRIBUTE, langfuse_run, run_attributes, should_export_span
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces
from tests.event_types import Deploy
from tests.langfuse.conftest import Backend

rule = Rule(name="deploys", when=on(Deploy), then=run("note"))
seen: list[Reaction[None]] = []


async def note(reaction: Reaction[None]) -> None:
    seen.append(reaction)


def test_whole_traces_are_exported(backend: Backend) -> None:
    for scope in (
        "reflexr",
        "opentelemetry.instrumentation.sqlalchemy",
        "opentelemetry.instrumentation.fastapi",
        "mcp-python-sdk",
        "pydantic-ai",
        "some.library",
    ):
        with backend.tracer_provider.get_tracer(scope).start_as_current_span(scope):
            pass
    with backend.tracer_provider.get_tracer("other.llm").start_as_current_span("chat") as span:
        span.set_attribute("gen_ai.operation.name", "chat")
    assert backend.exported() == [
        "reflexr",
        "opentelemetry.instrumentation.sqlalchemy",
        "opentelemetry.instrumentation.fastapi",
        "mcp-python-sdk",
        "pydantic-ai",
        "chat",
    ]


def test_a_span_without_a_scope_follows_langfuses_default() -> None:
    assert not should_export_span(ReadableSpan(name="anonymous"))


async def test_a_run_carries_its_session_user_name_tags_and_metadata(backend: Backend) -> None:
    workspaces = Workspaces(
        InMemoryStorage(), rules=[rule], tracer_provider=backend.tracer_provider
    )
    ws = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    deploy = (await ws.publish(Deploy(service="auth"))).envelope
    await Reactor(workspaces, actions={"note": note}, run_context=langfuse_run).settle()
    [done] = await ws.runs()
    backend.exported()
    spans = {span.name: dict(span.attributes or {}) for span in backend.spans.get_finished_spans()}
    attempt = spans["invoke_workflow deploys"]
    assert attempt["session.id"] == deploy.id
    assert attempt["user.id"] == "ada"
    assert attempt["langfuse.trace.name"] == "deploys"
    assert attempt["langfuse.trace.tags"] == ("tenant:acme", "workspace:prod", "rule:deploys")
    assert attempt["langfuse.trace.metadata.run_id"] == done.id
    assert attempt["langfuse.trace.metadata.attempt"] == "1"


async def test_attributes_are_ascii_and_within_langfuses_limits() -> None:
    client = ExternalAgentActor(client_id="claude-code")
    workspaces = Workspaces(InMemoryStorage(), rules=[rule])
    ws = await workspaces.open("acme", "prod", actor=client)
    await ws.publish(Deploy(service="auth"), id="evt_é" + "x" * 300)
    seen.clear()
    await Reactor(workspaces, actions={"note": note}).settle()
    [reaction] = seen
    attributes = run_attributes(reaction)
    assert attributes["session_id"] == ("evt_?" + "x" * 300)[:MAX_ATTRIBUTE]
    assert attributes["user_id"] is None, "only people are users"
    assert attributes["tags"] == ["tenant:acme", "workspace:prod", "rule:deploys"]
