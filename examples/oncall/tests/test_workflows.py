"""The workflows one at a time: paging a silent service, and the runbook's paths and retries."""

from datetime import timedelta

import httpx
import pytest

from conftest import BASE, START, FakeClock, log, publish
from oncall.app import Oncall
from oncall.services import Page, Pager, Rollback

# ─── silence: absence, with ticks keeping time ───────────────────────────────


async def test_a_service_that_goes_quiet_is_paged_once(
    client: httpx.AsyncClient, oncall: Oncall, clock: FakeClock
) -> None:
    pager = oncall.deps.pager
    await publish(client, type="service.heartbeat", service="api")
    await publish(client, type="service.heartbeat", service="db")
    await oncall.reactor.settle()  # the heartbeat check's timetable starts now
    clock.advance(90)
    await publish(client, type="service.heartbeat", service="db")
    await oncall.reactor.settle()  # a tick at 90s: api has been quiet for less than 2 minutes
    assert pager.pages == []

    clock.advance(60)
    settled = await oncall.reactor.settle()  # a tick at 150s: api has been quiet too long
    assert (settled.ticks, settled.firings) == (1, 1)
    [page] = pager.pages
    assert page == Page("api", "no heartbeat from api for 2 minutes")
    [alert] = await log(client, "alert.fired")
    assert alert["event"] == {
        "type": "alert.fired",
        "service": "api",
        "severity": 8,
        "message": "no heartbeat from api for 2 minutes",
    }
    assert (alert["actor"]["kind"], alert["actor"]["rule"]) == ("agent", "silence")
    [run] = (await client.get(f"{BASE}/runs")).json()
    assert (run["rule"], run["scope"], run["output"]) == (
        "silence",
        {"service": "api"},
        {"paged": "api"},
    )

    clock.advance(300)
    await oncall.reactor.settle()
    assert [p.service for p in pager.pages] == ["api", "db"], "api is paged once per silence"
    await publish(client, type="service.heartbeat", service="api")  # api is back: it rearms
    clock.advance(150)
    await oncall.reactor.settle()
    assert [p.service for p in pager.pages] == ["api", "db", "api"]


async def test_the_pager_deduplicates_by_key() -> None:
    pager = Pager()
    first = await pager.page("api", "down", key="run_1")
    assert await pager.page("api", "still down", key="run_1") is first
    assert pager.pages == [Page("api", "down")]


# ─── the runbook ─────────────────────────────────────────────────────────────


async def open_incident(client: httpx.AsyncClient, service: str = "api") -> None:
    """Open an incident by hand, as a person (or an MCP client) can: the runbook runs for it."""
    incident = {"service": service, "severity": 7, "summary": "checkout is slow"}
    await publish(client, type="incident.opened", **incident)


async def test_a_failed_verification_resumes_without_rolling_back_again(
    client: httpx.AsyncClient, oncall: Oncall, clock: FakeClock
) -> None:
    deployer = oncall.deps.deployer
    deployer.unhealthy["api"] = 1  # the first health check fails
    for version in ("v1", "v2"):
        await publish(client, type="deploy.completed", service="api", version=version)
    await open_incident(client)
    await oncall.reactor.settle()

    [waiting] = (await client.get(f"{BASE}/runs")).json()
    assert (waiting["status"], waiting["step"], waiting["error"]) == (
        "retrying",
        "roll_back",
        "RuntimeError: api is still unhealthy",
    )
    assert waiting["checkpoint"]["state"] == {
        "notes": ["api v2 was deployed just before the incident", "rolled api back to v1"]
    }
    assert await log(client, "incident.resolved") == []

    clock.advance(10)  # the runbook rule's backoff
    await oncall.reactor.settle()
    [done] = (await client.get(f"{BASE}/runs")).json()
    assert (done["status"], done["attempts"]) == ("succeeded", 2)
    assert deployer.rollbacks == [Rollback("api", "v1")], "the retry resumed at verify"
    steps = [e["event"]["step"] for e in await log(client, "run_progressed")]
    assert steps == ["__start__", "diagnose", "decide", "roll_back", "verify", "resolve", "__end__"]
    [resolved] = await log(client, "incident.resolved")
    assert resolved["event"]["resolution"].endswith("rolled api back to v1; api is healthy")


NOT_DEPLOYED = "api was not deployed in the 30 minutes before it"


@pytest.mark.parametrize(
    ("deploys", "reason"),
    [
        ([], NOT_DEPLOYED),
        ([("api", "v1", 7200), ("api", "v2", 3600)], NOT_DEPLOYED),
        (
            [("api", "v1", 60)],
            "api v1 was deployed just before the incident, with no earlier version",
        ),
        ([("db", "v1", 7200), ("db", "v2", 60)], NOT_DEPLOYED),
    ],
    ids=["no deploys", "old deploys", "a first deploy", "another service's deploys"],
)
async def test_without_a_version_to_roll_back_to_the_runbook_pages(
    client: httpx.AsyncClient,
    oncall: Oncall,
    clock: FakeClock,
    deploys: list[tuple[str, str, int]],
    reason: str,
) -> None:
    incident_at = START + timedelta(hours=2)
    for service, version, seconds_before in deploys:  # oldest first
        clock.now = incident_at - timedelta(seconds=seconds_before)
        await publish(client, type="deploy.completed", service=service, version=version)
    clock.now = incident_at
    await open_incident(client)
    await oncall.reactor.settle()

    assert oncall.deps.deployer.rollbacks == []
    [page] = oncall.deps.pager.pages
    assert page == Page("api", reason)
    [resolved] = await log(client, "incident.resolved")
    assert resolved["event"]["resolution"] == (
        f"{reason}; paged the person on call about api; api is healthy"
    )
