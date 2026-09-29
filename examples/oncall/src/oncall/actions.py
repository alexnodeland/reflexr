"""The actions the rules run: an agent, a graph and a function, all over one ``Reaction``."""

from pydantic_ai.models import Model

from oncall.events import AlertFired
from oncall.rules import SILENCE
from oncall.runbook import runbook_action
from oncall.services import OncallDeps
from oncall.triage import triage_action
from reflexr.workspace import Action, Reaction


async def page(reaction: Reaction[OncallDeps]) -> dict[str, str]:
    """Page the person on call about a silent service, and raise an alert about it.

    The alert counts towards the service's triage like any other. Its id is derived from the
    run, and the page is deduplicated by the run's id, so a retried run does neither twice.
    """
    service = str(reaction.scope["service"])
    reason = f"no heartbeat from {service} for {SILENCE.total_seconds() / 60:g} minutes"
    await reaction.deps.pager.page(service, reason, key=reaction.run_id)
    await reaction.emit(AlertFired(service=service, severity=8, message=reason))
    return {"paged": service}


def build_actions(model: Model | str | None = None) -> dict[str, Action[OncallDeps]]:
    """Every action, by the name rules refer to it by.

    Args:
        model: The triage agent's model; see :func:`oncall.triage.build_triage_agent`.
    """
    return {"triage": triage_action(model), "runbook": runbook_action(), "page": page}
