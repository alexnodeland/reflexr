"""Builders shared by the workspace tests."""

from reflexr.core import Firing, Run, SourceActor, create_run
from reflexr.workspace import Entry
from tests.event_types import ServiceError
from tests.workspace.conftest import START


def entry(event_id: str, service: str = "auth", *, chain: str | None = None) -> Entry:
    return Entry(
        id=event_id,
        actor=SourceActor(name="monitor"),
        event=ServiceError(service=service),
        correlation_id=chain,
    )


def fired(run_id: str, *, rule: str = "triage", scope: str = "auth", seq: int = 1) -> Run:
    firing = Firing(
        id=run_id,
        rule=rule,
        scope_key=f'["{scope}"]',
        scope={"service": scope},
        seq=seq,
        at=START,
        matched=(seq,),
        correlation_id=f"evt_{seq}",
    )
    return create_run(firing, now=START)
