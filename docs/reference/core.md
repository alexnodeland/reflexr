# reflexr.core

::: reflexr.core
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## Events

Event types, their namespaces and registries, and the envelope each stored event travels in. See [Events and envelopes](../guides/events.md).

::: reflexr.core.Event

::: reflexr.core.UnknownEvent

::: reflexr.core.AnyEvent

::: reflexr.core.Envelope

::: reflexr.core.Causation

::: reflexr.core.EventRegistry

::: reflexr.core.DEFAULT_REGISTRY

::: reflexr.core.EventName

::: reflexr.core.check_event_name

::: reflexr.core.load_event

::: reflexr.core.dump_event

::: reflexr.core.type_of

## reflexr's own events

The facts reflexr appends about rules, runs, feedback and schedules. See [reflexr's own events](../guides/events.md#reflexrs-own-events).

::: reflexr.core.SystemEvent

::: reflexr.core.SYSTEM_EVENTS

::: reflexr.core.about_rule

::: reflexr.core.RuleFired

::: reflexr.core.RuleErrored

::: reflexr.core.RuleReset

::: reflexr.core.RunStarted

::: reflexr.core.RunProgressed

::: reflexr.core.RunRetrying

::: reflexr.core.RunSucceeded

::: reflexr.core.RunDeadLettered

::: reflexr.core.RunCancelled

::: reflexr.core.RunRequeued

::: reflexr.core.RunSkipped

::: reflexr.core.FeedbackGiven

::: reflexr.core.Tick

## Actors

Who published an event or performed a command. See [Actors](../guides/events.md#actors).

::: reflexr.core.Actor

::: reflexr.core.UserActor

::: reflexr.core.AgentActor

::: reflexr.core.ExternalAgentActor

::: reflexr.core.SystemActor

::: reflexr.core.SourceActor

::: reflexr.core.EvaluatorActor

## Rules

A rule's condition, scope, action and policies. See [Rules](../guides/rules.md).

::: reflexr.core.Rule

::: reflexr.core.Scope

::: reflexr.core.by

::: reflexr.core.scope_key

::: reflexr.core.ActionRef

::: reflexr.core.run

::: reflexr.core.Named

::: reflexr.core.RetryPolicy

## Conditions

The builder, and the stages it produces. See [Conditions](../guides/rules.md#conditions).

::: reflexr.core.on

::: reflexr.core.sequence

::: reflexr.core.F

::: reflexr.core.field

::: reflexr.core.FieldRef

::: reflexr.core.Condition

::: reflexr.core.Filter

::: reflexr.core.OnFilter

::: reflexr.core.WhereFilter

::: reflexr.core.Op

::: reflexr.core.AllFilter

::: reflexr.core.AnyFilter

::: reflexr.core.NotFilter

::: reflexr.core.PredicateFilter

::: reflexr.core.Dedupe

::: reflexr.core.Pattern

::: reflexr.core.EachPattern

::: reflexr.core.CountPattern

::: reflexr.core.SequencePattern

::: reflexr.core.AbsencePattern

::: reflexr.core.Throttle

## Evaluation

The host contract: what to load, and how a batch of envelopes is decided ([ADR-0017](../adr/0017-the-cores-evaluation-contract.md)). The reactor uses these; call them directly only when you write a host of your own, or to test a rule without storage.

::: reflexr.core.needs

::: reflexr.core.evaluate

::: reflexr.core.begin

::: reflexr.core.reset

::: reflexr.core.matches

::: reflexr.core.Predicates

::: reflexr.core.NO_PREDICATES

::: reflexr.core.NotLoaded

## State

What a rule remembers between batches, and what evaluating a batch decides.

::: reflexr.core.RuleProgress

::: reflexr.core.ScopeState

::: reflexr.core.DedupeState

::: reflexr.core.PatternState

::: reflexr.core.EachState

::: reflexr.core.CountState

::: reflexr.core.SequenceState

::: reflexr.core.AbsenceState

::: reflexr.core.ThrottleState

::: reflexr.core.Match

::: reflexr.core.Firing

::: reflexr.core.EvaluationError

::: reflexr.core.Fact

::: reflexr.core.Evaluation

## Runs

A firing's run and its lifecycle, as pure transitions. See [The reactor](../guides/reactor.md).

::: reflexr.core.Run

::: reflexr.core.RunStatus

::: reflexr.core.FINISHED

::: reflexr.core.WAITING

::: reflexr.core.HOLDING

::: reflexr.core.create_run

::: reflexr.core.start

::: reflexr.core.succeed

::: reflexr.core.fail

::: reflexr.core.checkpoint

::: reflexr.core.cancel

::: reflexr.core.skip

::: reflexr.core.retry

::: reflexr.core.runnable

::: reflexr.core.holds

## Feedback

Typed feedback on runs, firings and causal chains. See [Feedback and evaluation](../guides/evaluation.md).

::: reflexr.core.Feedback

::: reflexr.core.FeedbackTarget

::: reflexr.core.RunTarget

::: reflexr.core.FiringTarget

::: reflexr.core.ChainTarget

::: reflexr.core.TargetKind

::: reflexr.core.TARGET_KINDS

::: reflexr.core.feedback_types

::: reflexr.core.load_feedback

## Rejections

The ways a command can fail, and the error for an invalid rule. See [Rejections](../guides/workspaces.md#rejections).

::: reflexr.core.Rejection

::: reflexr.core.NotFound

::: reflexr.core.InvalidState

::: reflexr.core.ValidationFailed

::: reflexr.core.Forbidden

::: reflexr.core.DepthExceeded

::: reflexr.core.UnsupportedProtocol

::: reflexr.core.InvalidRule

## Protocol

The stream protocol's commands, outcomes, frames and resume rule. See the [stream protocol](../protocol.md).

::: reflexr.core.PROTOCOL

::: reflexr.core.Command

::: reflexr.core.Publish

::: reflexr.core.GiveFeedback

::: reflexr.core.RetryRun

::: reflexr.core.SkipRun

::: reflexr.core.CancelRun

::: reflexr.core.ReplayRule

::: reflexr.core.Outcome

::: reflexr.core.PublishedOutcome

::: reflexr.core.RecordedOutcome

::: reflexr.core.RunOutcome

::: reflexr.core.RuleOutcome

::: reflexr.core.Hello

::: reflexr.core.Welcome

::: reflexr.core.EventFrame

::: reflexr.core.ReplayComplete

::: reflexr.core.CommandFrame

::: reflexr.core.CommandResult

::: reflexr.core.ErrorFrame

::: reflexr.core.ClientFrame

::: reflexr.core.ServerFrame

::: reflexr.core.resume

::: reflexr.core.ResumePlan

## Identifiers

Identifiers are plain strings. These aliases say what a string identifies, and the factories generate or derive ids with a short type prefix.

::: reflexr.core.TenantId

::: reflexr.core.WorkspaceId

::: reflexr.core.EventId

::: reflexr.core.RuleName

::: reflexr.core.ScopeKey

::: reflexr.core.FiringId

::: reflexr.core.RunId

::: reflexr.core.TraceId

::: reflexr.core.new_id

::: reflexr.core.new_event_id

::: reflexr.core.firing_id

::: reflexr.core.derived_event_id
