"""Minimal type stubs for the parts of opentelemetry-processor-baggage artifactr uses."""

from collections.abc import Callable, Sequence

from opentelemetry.sdk.trace import SpanProcessor

type BaggageKeyPredicate = Callable[[str], bool]

class BaggageSpanProcessor(SpanProcessor):
    def __init__(
        self, baggage_key_predicate: BaggageKeyPredicate | Sequence[BaggageKeyPredicate]
    ) -> None: ...
