"""evalr for reflexr: the ``[evals]`` extra (RFC-0002, ADR-0020).

evalr owns evaluation: evaluators, datasets, optimizers and experiments behind ports
(ADR-0025). This package adapts reflexr to them:

- :class:`LogFeedbackSource` is evalr's ``FeedbackSource`` over a workspace's log: people's
  typed feedback becomes examples, with inputs built from the run, firing or chain it is about.
- :class:`EvaluatorAction` runs an evalr evaluator as a rule's action, recording its verdicts as
  feedback from an :class:`~reflexr.core.EvaluatorActor`, so evaluators run online, sampled and
  throttled like any rule.
- :func:`rule_outcomes` and :func:`time_to_resolution` measure workflows end to end from the
  log: dead-letter, retry and intervention rates per rule, and time to resolution per chain.
"""

from reflexr.evals.action import Build, EvaluatorAction
from reflexr.evals.context import RunRecord, chain_events, run_record
from reflexr.evals.measures import OPERATORS, RuleOutcomes, rule_outcomes, time_to_resolution
from reflexr.evals.source import BuildInput, FeedbackContext, LogFeedbackSource

__all__ = [
    "OPERATORS",
    "Build",
    "BuildInput",
    "EvaluatorAction",
    "FeedbackContext",
    "LogFeedbackSource",
    "RuleOutcomes",
    "RunRecord",
    "chain_events",
    "rule_outcomes",
    "run_record",
    "time_to_resolution",
]
