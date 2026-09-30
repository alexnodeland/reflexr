"""End-to-end measures of workflows, from the log."""

from datetime import timedelta

from reflexr import F, RetryPolicy, Rule, SourceActor, UserActor, by, on, run
from reflexr.evals import RuleOutcomes, rule_outcomes, time_to_resolution
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces
from tests.clock import FakeClock
from tests.event_types import Deploy, ServiceError

page = Rule(
    name="app:page",
    when=on(ServiceError),
    scope=by(F.service),
    then=run("page"),
    retry=RetryPolicy(max_attempts=2, backoff=timedelta(seconds=1)),
)


async def paging(reaction: Reaction[None]) -> None:
    if reaction.scope["service"] != "auth":
        raise RuntimeError("the pager is down")
    await reaction.emit(Deploy(service="auth", version="fixed"))


async def test_rule_outcomes_and_time_to_resolution() -> None:
    clock = FakeClock()
    workspaces = Workspaces(InMemoryStorage(clock=clock), rules=[page], clock=clock)
    monitor = await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))
    for service in ("auth", "billing", "search"):
        await monitor.publish(ServiceError(service=service))
    reactor = Reactor(workspaces, actions={"page": paging})
    await reactor.settle()
    clock.advance(1)
    await reactor.settle()  # billing and search are dead-lettered after their retry
    operator = monitor.as_actor(UserActor(id="ada"))
    [search] = await monitor.runs(scope_key='["search"]')
    await operator.skip_run(search.id)
    [billing] = await monitor.runs(scope_key='["billing"]')
    await operator.retry_run(billing.id)
    assert await rule_outcomes(monitor) == {
        "app:page": RuleOutcomes(
            rule="app:page", runs=3, succeeded=1, dead_lettered=2, retried=2, intervened=2
        )
    }
    outcomes = (await rule_outcomes(monitor))["app:page"]
    assert (outcomes.dead_letter_rate, outcomes.retry_rate, outcomes.intervention_rate) == (
        2 / 3,
        2 / 3,
        2 / 3,
    )
    empty = RuleOutcomes(rule="app:quiet")
    assert (empty.dead_letter_rate, empty.retry_rate, empty.intervention_rate) == (0, 0, 0)
    resolved = await time_to_resolution(
        monitor, resolves=lambda envelope: envelope.event_type == "app:deploy.finished"
    )
    [auth_chain] = [e.correlation_id for e in await monitor.read(limit=1)]
    assert resolved == {auth_chain: timedelta(0)}
