"""Agent actions: pydantic-ai agents responding to firings, with the EventContext capability."""

from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.capabilities import Instrumentation
from pydantic_ai.models.instrumented import InstrumentationSettings
from pydantic_ai.usage import UsageLimits

from reflexr import AgentActor, Event, F, Rule, SourceActor, by, on, run
from reflexr.agent import AgentAction, EventContext, firing_text
from reflexr.telemetry import attributes as a
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspace, Workspaces
from tests.agent.conftest import Script, call, say
from tests.event_types import Deploy, ServiceError

triage_rule = Rule(
    name="triage",
    description="Errors after a deploy.",
    when=on(ServiceError),
    scope=by(F.service),
    then=run("triage"),
)


class Escalated(Event, name="incident.escalated"):
    service: str
    severity: int


class Verdict(BaseModel):
    severity: int
    summary: str


async def setup(
    action: AgentAction[None, Any], tracer_provider: TracerProvider | None = None
) -> tuple[Workspace, Reactor[None]]:
    workspaces = Workspaces(InMemoryStorage(), rules=[triage_rule], tracer_provider=tracer_provider)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))
    await workspace.publish(Deploy(service="auth"))
    await workspace.publish(ServiceError(service="auth", severity=8, message="token check"))
    return workspace, Reactor(workspaces, actions={"triage": action})


def agent(script: Script, *capabilities: Any, output_type: Any = str) -> Agent[Reaction[None], Any]:
    return Agent(
        script.model,
        deps_type=Reaction[None],
        output_type=output_type,
        capabilities=list(capabilities),
        name="triage",
    )


async def test_an_agent_reads_the_log_emits_events_and_answers() -> None:
    script = Script(
        call("read_events", types=["deploy.finished"]),
        call(
            "emit_event",
            "call_2",
            type="incident.escalated",
            fields={"service": "auth", "severity": 8},
        ),
        say("Escalated: the deploy broke token checks."),
    )
    action = AgentAction(agent(script, EventContext(emit=[Escalated])))
    workspace, reactor = await setup(action)
    await reactor.settle()
    [done] = await workspace.runs(rule="triage")
    assert (done.status, done.output) == ("succeeded", "Escalated: the deploy broke token checks.")
    [prompt] = script.sent(0)
    assert prompt.startswith('Rule "triage" fired for {"service": "auth"}. This is attempt 1.')
    assert "Errors after a deploy." in prompt
    assert '<event seq="2" type="service.error"' in prompt
    assert "emit_event: incident.escalated" in script.instructions(0)
    assert script.tools[0] == ["read_events", "emit_event"]
    [deploys] = script.sent(1)
    assert '<event seq="1" type="deploy.finished"' in deploys
    assert script.sent(2) == ["Published incident.escalated at seq 5."]
    escalation = (await workspace.read())[4]
    assert escalation.event == Escalated(service="auth", severity=8)
    assert escalation.actor == AgentActor(rule="triage", run_id=done.id, name="triage")
    assert (escalation.causation, escalation.correlation_id) == (
        done.causation,
        done.correlation_id,
    )


async def test_emitting_is_limited_to_allowed_and_valid_events() -> None:
    script = Script(
        call("emit_event", type="deploy.finished", fields={"service": "auth"}),
        call("emit_event", "call_2", type="incident.escalated", fields={"service": "auth"}),
        say("Could not escalate."),
    )
    workspace, reactor = await setup(AgentAction(agent(script, EventContext(emit=[Escalated]))))
    await reactor.settle()
    [refused] = script.sent(1)
    assert "You may not emit 'deploy.finished'; allowed: incident.escalated." in refused
    [invalid] = script.sent(2)
    assert "The fields do not match incident.escalated" in invalid
    assert not [e for e in await workspace.read() if isinstance(e.event, Escalated)]


async def test_without_emit_types_an_agent_can_only_read() -> None:
    script = Script(call("read_events", after_seq=99), say("Nothing new."))
    _, reactor = await setup(AgentAction(agent(script, EventContext())))
    await reactor.settle()
    assert script.tools[0] == ["read_events"]
    assert script.sent(1) == ["No events."]
    assert "emit_event" not in script.instructions(0)


async def test_an_agent_reads_the_log_from_its_end_and_backwards() -> None:
    script = Script(
        call("read_events", last=1, types=["deploy.finished", "service.error"]),
        call("read_events", "call_2", last=5, before_seq=2),
        call("read_events", "call_3", limit=1, last=1),
        say("Read back to the deploy."),
    )
    _, reactor = await setup(AgentAction(agent(script, EventContext(read_limit=1))))
    await reactor.settle()
    [latest] = script.sent(1)
    assert latest.startswith('<event seq="2" type="service.error"')
    [earlier] = script.sent(2)
    assert earlier.startswith('<event seq="1" type="deploy.finished"'), "capped at read_limit"
    [refused] = script.sent(3)
    assert "give limit or last, not both" in refused


async def test_structured_outputs_and_custom_prompts() -> None:
    script = Script(call("final_result", severity=8, summary="token checks fail"))
    action = AgentAction(
        agent(script, output_type=Verdict),
        name="triage",
        prompt=lambda reaction: f"Triage {reaction.scope['service']}.",
        usage_limits=UsageLimits(request_limit=3),
    )
    workspace, reactor = await setup(action)
    await reactor.settle()
    [done] = await workspace.runs(rule="triage")
    assert done.output == {"severity": 8, "summary": "token checks fail"}
    assert script.sent(0) == ["Triage auth."]
    fixed = Script(say("ok"))
    workspace, reactor = await setup(AgentAction(agent(fixed), prompt="Look into it."))
    await reactor.settle()
    assert fixed.sent(0) == ["Look into it."]


def test_an_action_needs_a_name() -> None:
    unnamed = Agent(Script().model, deps_type=Reaction[None])
    with pytest.raises(ValueError, match="name the action, or the agent"):
        AgentAction(unnamed)
    assert AgentAction(unnamed, name="triage").name == "triage"
    assert run(AgentAction(agent(Script()))).action == "triage"


async def test_the_agent_span_joins_the_chain_with_reflexr_attribution() -> None:
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    instrumented = Instrumentation(settings=InstrumentationSettings(tracer_provider=provider))
    script = Script(say("done"))
    workspace, reactor = await setup(
        AgentAction(agent(script, EventContext(), instrumented)), tracer_provider=provider
    )
    await reactor.settle()
    [done] = await workspace.runs(rule="triage")
    [agent_span] = [s for s in spans.get_finished_spans() if s.name.startswith("invoke_agent")]
    attributes = dict(agent_span.attributes or {})
    assert attributes[a.CONVERSATION_ID] == done.correlation_id
    assert (attributes[a.RULE], attributes[a.RUN_ID], attributes[a.ATTEMPT]) == (
        "triage",
        done.id,
        1,
    )
    assert attributes[a.TENANT_ID] == "acme"
    [workflow] = [s for s in spans.get_finished_spans() if s.name == "invoke_workflow triage"]
    parent = agent_span.parent
    assert parent is not None
    context = workflow.get_span_context()
    assert context is not None
    assert parent.span_id == context.span_id  # the agent runs inside the run's attempt
    provider.shutdown()


async def test_firings_render_without_a_scope_or_description() -> None:
    plain = Rule(name="deploys", when=on(Deploy), then=run("note"))
    captured: list[str] = []

    async def note(reaction: Reaction[None]) -> None:
        captured.append(firing_text(reaction, max_event_chars=10))

    workspaces = Workspaces(InMemoryStorage(), rules=[plain])
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    await workspace.publish(Deploy(service="authentication-service"))
    await Reactor(workspaces, actions={"note": note}).settle()
    [text] = captured
    assert text.startswith('Rule "deploys" fired. This is attempt 1.\nThe events')
    assert '{"service…</event>' in text
