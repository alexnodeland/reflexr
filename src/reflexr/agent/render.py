"""How a firing is described to a model."""

import json
from typing import Any

from reflexr.core import Envelope
from reflexr.workspace import Reaction


def firing_text(reaction: Reaction[Any], *, max_event_chars: int = 2000) -> str:
    """Describe the firing an action is responding to: the rule, the scope and the events."""
    run = reaction.run
    lines = [
        f'Rule "{reaction.rule.name}" fired'
        + (f" for {json.dumps(run.scope, sort_keys=True)}" if run.scope else "")
        + f". This is attempt {run.attempts}.",
    ]
    if reaction.rule.description:
        lines.append(f"The rule: {reaction.rule.description}")
    lines.append("The events that made it fire, oldest first:")
    lines.extend(envelope_text(e, max_chars=max_event_chars) for e in reaction.events)
    return "\n".join(lines)


def envelope_text(envelope: Envelope, *, max_chars: int = 2000) -> str:
    """Render one envelope as a line a model can read."""
    data = json.dumps(envelope.data, sort_keys=True, default=str)
    if len(data) > max_chars:
        data = data[: max_chars - 1] + "…"
    return (
        f'<event seq="{envelope.seq}" type="{envelope.event_type}" '
        f'at="{envelope.ts.isoformat()}" by="{envelope.actor.kind}">{data}</event>'
    )
