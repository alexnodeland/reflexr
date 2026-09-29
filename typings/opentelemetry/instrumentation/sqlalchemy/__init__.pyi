"""Minimal type stubs for the parts of opentelemetry-instrumentation-sqlalchemy artifactr uses."""

from collections.abc import Collection
from typing import Any

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

class SQLAlchemyInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]: ...
    def _instrument(self, **kwargs: Any) -> Any: ...
    def _uninstrument(self, **kwargs: Any) -> None: ...
