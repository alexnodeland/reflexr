"""The names of the attributes on reflexr's spans and metrics.

They live in one module because the OpenTelemetry GenAI conventions are still in development:
if a name changes, it changes here. reflexr's own names match artifactr's, with ``reflexr.`` in
place of ``artifactr.``.
"""

# ─── attribution ─────────────────────────────────────────────────────────────

TENANT_ID = "reflexr.tenant.id"
WORKSPACE_ID = "reflexr.workspace.id"
ACTOR_KIND = "reflexr.actor.kind"
USER_ID = "user.id"
"""Set when a person published or acted."""

# ─── events ──────────────────────────────────────────────────────────────────

EVENT_TYPE = "reflexr.event.type"
EVENT_ID = "reflexr.event.id"
EVENT_SEQ = "reflexr.event.seq"
EVENT_COUNT = "reflexr.event.count"
DUPLICATE = "reflexr.event.duplicate"
"""Whether a publish found its event id already in the log and appended nothing."""

DEPTH = "reflexr.causation.depth"

# ─── rules and runs ──────────────────────────────────────────────────────────

RULE = "reflexr.rule"
SCOPE = "reflexr.scope"
RUN_ID = "reflexr.run.id"
RUN_STATUS = "reflexr.run.status"
RUN_REASON = "reflexr.run.reason"
"""A stable code for why a run attempt failed; bounded, since reasons are codes."""
ATTEMPT = "reflexr.attempt"
CHECKPOINT_REASON = "reflexr.checkpoint.reason"
"""Why a run's checkpoint was not resumed: a stable code."""
CHECKPOINT_ERROR = "reflexr.checkpoint.error"
"""What in a run's checkpoint did not validate, without the values."""
SCHEDULE = "reflexr.schedule"
EVALUATED_RULES = "reflexr.evaluation.rules"
"""The rules that evaluated new envelopes in an evaluation pass."""

FIRING_COUNT = "reflexr.evaluation.firings"

CLOSE_CODE = "reflexr.stream.close_code"
"""How a WebSocket connection ended."""

# ─── feedback ────────────────────────────────────────────────────────────────

FEEDBACK_TYPE = "reflexr.feedback.type"
FEEDBACK_TARGET = "reflexr.feedback.target"

# ─── sessions and GenAI ──────────────────────────────────────────────────────

SESSION_ID = "session.id"
"""The causal chain's correlation id. Langfuse groups a session's traces by it."""

CONVERSATION_ID = "gen_ai.conversation.id"
"""The same value under the GenAI name, which pydantic-ai sets on agent spans."""

OPERATION_NAME = "gen_ai.operation.name"
WORKFLOW_NAME = "gen_ai.workflow.name"
