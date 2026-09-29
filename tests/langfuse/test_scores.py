"""Langfuse behind evalr's score ports, checked by evalr's contract suites."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
from evalr.contracts import check_score_config_store, check_score_sink
from evalr.core import Score, ScoreConfig
from opentelemetry.sdk.trace import TracerProvider

from reflexr import Rule, SourceActor, UserActor, on, run
from reflexr.core import RunTarget
from reflexr.langfuse import LangfuseScoreConfigs, LangfuseScores
from reflexr.scores import FeedbackMirror, sync_score_configs
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces
from tests.event_types import Deploy
from tests.langfuse.conftest import Backend, FakeLangfuseApi
from tests.scores.kinds import Accuracy, Helpfulness

TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
NOW = datetime(2026, 9, 28, tzinfo=UTC)


def score(id: str, value: bool | float | str, data_type: Any, **target: Any) -> Score:
    return Score(
        id=id,
        name=f"helpfulness.{id}",
        value=value,
        data_type=data_type,
        trace_id=target.get("trace_id"),
        session_id=target.get("session_id"),
        timestamp=NOW,
        source={"tenant_id": "acme"},
    )


def received(api: FakeLangfuseApi) -> list[Score]:
    """The scores the fake Langfuse holds, read back as evalr's scores."""
    held: list[Score] = []
    for body in api.scores.values():
        value = body["value"]
        held.append(
            Score(
                id=body["id"],
                name=body["name"],
                value=value == 1 if body["dataType"] == "BOOLEAN" else value,
                data_type=body["dataType"],
                trace_id=body.get("traceId"),
                session_id=body.get("sessionId"),
            )
        )
    return held


async def test_the_score_sink_contract(backend: Backend) -> None:
    async def recorded() -> Sequence[Score]:
        backend.client.flush()
        return received(backend.api)

    await check_score_sink(LangfuseScores(backend.client), recorded)


async def test_the_score_config_store_contract(backend: Backend) -> None:
    await check_score_config_store(LangfuseScoreConfigs(backend.client))


async def test_langfuse_scores_carry_their_type_time_and_metadata(backend: Backend) -> None:
    await LangfuseScores(backend.client).record(
        [
            score("rating", 4.0, "NUMERIC", trace_id=TRACE),
            score("useful", True, "BOOLEAN", session_id="thr_1"),
        ]
    )
    backend.client.flush()
    rating, useful = backend.api.scores["rating"], backend.api.scores["useful"]
    assert (rating["dataType"], rating["metadata"]) == ("NUMERIC", {"tenant_id": "acme"})
    assert (useful["dataType"], useful["value"], useful["sessionId"]) == ("BOOLEAN", 1, "thr_1")
    assert "traceId" not in useful
    assert backend.api.times["rating"] == NOW


async def test_a_value_not_of_its_type_queues_nothing(backend: Backend) -> None:
    with pytest.raises(ValueError, match="a NUMERIC score cannot be 'four'"):
        await LangfuseScores(backend.client).record(
            [score("tone", "casual", "CATEGORICAL"), score("rating", "four", "NUMERIC")]
        )
    backend.client.flush()
    assert backend.api.scores == {}


async def test_score_configs_are_synced_once_past_those_langfuse_has() -> None:
    backend = Backend(FakeLangfuseApi("helpfulness.rating", "other.a", "other.b"))
    store = LangfuseScoreConfigs(backend.client)
    created = await sync_score_configs(store, [Helpfulness, Accuracy])
    assert created == [
        "helpfulness.reason",
        "accuracy.correct",
        "accuracy.verdict",
        "accuracy.tone",
        "accuracy.confidence",
    ]
    assert len(await store.names()) == 8
    assert await sync_score_configs(store, [Helpfulness, Accuracy]) == []
    backend.client.shutdown()


async def test_langfuse_score_configs_describe_each_field(api: FakeLangfuseApi) -> None:
    backend = Backend(api)
    configs = LangfuseScoreConfigs(backend.client)
    await sync_score_configs(configs, [Accuracy])
    created = {config["name"]: config for config in api.configs}
    assert created["accuracy.verdict"]["categories"] == [
        {"label": "right", "value": 0},
        {"label": "wrong", "value": 1},
    ]
    assert (
        created["accuracy.confidence"]["minValue"],
        created["accuracy.confidence"]["maxValue"],
    ) == (0, 1)
    assert "categories" not in created["accuracy.correct"]
    too_long = ScoreConfig(
        name="a_rather_long_feedback_type.a_long_field",
        type_name="a_rather_long_feedback_type",
        field="a_long_field",
        data_type="TEXT",
        description="Explained.",
    )
    with pytest.raises(ValueError, match="shorten"):
        await configs.create(too_long)
    await configs.create(
        ScoreConfig(name="t.f", type_name="t", field="f", data_type="TEXT", description="Why")
    )
    assert api.configs[-1]["description"] == "Why"
    backend.client.shutdown()


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
    mirror = FeedbackMirror(ws, LangfuseScores(backend.client))
    [envelope] = await ws.read(after_seq=await ws.head_seq() - 1)
    [sent] = await mirror.mirror(envelope)
    backend.client.flush()
    body = backend.api.scores[sent.id]
    assert (body["name"], body["value"], body["traceId"]) == (
        "helpfulness.rating",
        5.0,
        done.trace_ids[-1],
    )
    assert body["metadata"]["actor"] == "user:ada"
