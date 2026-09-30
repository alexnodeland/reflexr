"""The feedback mirror records through evalr's Langfuse score sink."""

from evalr.langfuse import LangfuseScoreSink
from opentelemetry.sdk.trace import TracerProvider

from reflexr import Rule, SourceActor, UserActor, on, run
from reflexr.core import RunTarget
from reflexr.scores import FeedbackMirror
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces
from tests.event_types import Deploy
from tests.langfuse.conftest import Backend
from tests.scores.kinds import Helpfulness


async def note(reaction: Reaction[None]) -> None:
    return None


async def test_feedback_reaches_langfuse_on_its_trace(backend: Backend) -> None:
    rule = Rule(name="deploys", when=on(Deploy), then=run("note"))
    workspaces = Workspaces(InMemoryStorage(), rules=[rule], tracer_provider=TracerProvider())
    ws = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    await ws.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions={"note": note}).settle()
    [done] = await ws.runs()
    person = ws.as_actor(UserActor(id="ada"))
    await person.give_feedback(Helpfulness(rating=5), on=RunTarget(run_id=done.id))
    sink = LangfuseScoreSink(backend.client)
    mirror = FeedbackMirror(ws, sink, cursor="langfuse")
    [envelope] = await ws.read(last=1)
    [sent] = await mirror.mirror(envelope)
    await sink.flush()
    body = backend.api.scores[sent.id]
    assert (body["name"], body["value"], body["traceId"]) == (
        "helpfulness.rating",
        5.0,
        done.trace_ids[-1],
    )
    assert "observationId" not in body, "the mirror names no span"
    assert body["metadata"]["actor"] == "user:ada"
