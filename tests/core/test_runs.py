"""The run lifecycle and which runs may start."""

from datetime import UTC, datetime, timedelta

import pytest

from reflexr.core import (
    Firing,
    InvalidState,
    RetryPolicy,
    Rule,
    Run,
    RunDeadLettered,
    RunRetrying,
    cancel,
    checkpoint,
    create_run,
    fail,
    on,
    retry,
    run,
    runnable,
    skip,
    start,
    succeed,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
SECOND = timedelta(seconds=1)


def rule(**changes: object) -> Rule:
    fields: dict[str, object] = {
        "name": "r",
        "when": on("service.error"),
        "then": run("act"),
        "retry": RetryPolicy(max_attempts=2, backoff=SECOND),
    }
    return Rule.model_validate({**fields, **changes})


def fired(seq: int = 1) -> Run:
    firing = Firing(
        id=f"fir_{seq}",
        rule="r",
        scope_key="[]",
        scope={},
        seq=seq,
        at=NOW,
        matched=(seq,),
        depth=1,
    )
    return create_run(firing, now=NOW)


def test_a_run_succeeds() -> None:
    created = fired()
    assert (created.status, created.attempts, created.depth) == ("pending", 0, 1)
    running, started = start(created, now=NOW)
    assert (running.status, running.attempts, started.attempt) == ("running", 1, 1)
    saved, progressed = checkpoint(running, now=NOW, step="diagnose", state={"n": 1})
    assert (saved.step, saved.checkpoint, progressed.step) == ("diagnose", {"n": 1}, "diagnose")
    done, succeeded = succeed(saved, now=NOW + SECOND, output={"ok": True})
    assert (done.status, done.output, succeeded.output) == ("succeeded", {"ok": True}, {"ok": True})
    assert done.updated_at == NOW + SECOND


def test_a_failing_run_retries_then_is_dead_lettered() -> None:
    running, _ = start(fired(), now=NOW)
    retrying, event = fail(running, rule(), now=NOW, error="boom")
    assert (retrying.status, retrying.next_attempt_at) == ("retrying", NOW + SECOND)
    assert isinstance(event, RunRetrying)
    assert (event.attempt, event.error) == (1, "boom")
    again, _ = start(retrying, now=NOW + SECOND)
    dead, dead_event = fail(again, rule(), now=NOW + SECOND, error="boom again")
    assert isinstance(dead_event, RunDeadLettered)
    assert (dead.status, dead.error, dead_event.attempts) == ("dead", "boom again", 2)


def test_transitions_are_refused_from_the_wrong_status() -> None:
    created = fired()
    with pytest.raises(InvalidState, match="cannot succeed run fir_1, which is pending"):
        succeed(created, now=NOW)
    with pytest.raises(InvalidState):
        fail(created, rule(), now=NOW, error="x")
    with pytest.raises(InvalidState):
        checkpoint(created, now=NOW, step="s", state=None)
    done, _ = succeed(start(created, now=NOW)[0], now=NOW)
    for transition in (start, cancel, skip, retry):
        with pytest.raises(InvalidState):
            transition(done, now=NOW)


def test_cancel_skip_and_retry() -> None:
    cancelled, event = cancel(fired(), now=NOW, reason="no longer needed")
    assert (cancelled.status, event.reason) == ("cancelled", "no longer needed")
    skipped, skip_event = skip(fired(), now=NOW)
    assert (skipped.status, skip_event.reason) == ("skipped", None)
    running, _ = start(fired(), now=NOW)
    dead, _ = fail(
        start(fail(running, rule(), now=NOW, error="x")[0], now=NOW)[0], rule(), now=NOW, error="x"
    )
    revived = retry(dead, now=NOW + SECOND)
    assert (revived.status, revived.attempts, revived.next_attempt_at) == (
        "pending",
        0,
        NOW + SECOND,
    )
    waiting, _ = fail(start(fired(), now=NOW)[0], rule(), now=NOW, error="x")
    assert retry(waiting, now=NOW).attempts == 1


def test_runs_of_a_scope_start_in_order() -> None:
    first, second = fired(1), fired(2)
    assert runnable([first, second], rule(), now=NOW) == [first]
    running = start(first, now=NOW)[0]
    assert runnable([running, second], rule(), now=NOW) == []
    retrying = fail(running, rule(), now=NOW, error="x")[0]
    assert runnable([retrying, second], rule(), now=NOW) == []
    assert runnable([retrying, second], rule(), now=NOW + SECOND) == [retrying]
    done = succeed(start(retrying, now=NOW)[0], now=NOW)[0]
    assert runnable([done, second], rule(), now=NOW) == [second]
    assert runnable([], rule(), now=NOW) == []


def test_dead_letters_continue_or_block() -> None:
    running = start(fired(1), now=NOW)[0]
    dead = fail(
        start(fail(running, rule(), now=NOW, error="x")[0], now=NOW)[0], rule(), now=NOW, error="x"
    )[0]
    second = fired(2)
    assert runnable([dead, second], rule(), now=NOW) == [second]
    assert runnable([dead, second], rule(on_dead_letter="block"), now=NOW) == []
    skipped = skip(dead, now=NOW)[0]
    assert runnable([skipped, second], rule(on_dead_letter="block"), now=NOW) == [second]


def test_without_ordering_every_due_run_starts() -> None:
    first, second = fired(1), fired(2)
    running = start(first, now=NOW)[0]
    assert runnable([running, second], rule(ordering="none"), now=NOW) == [second]
