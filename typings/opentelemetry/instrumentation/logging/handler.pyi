"""Minimal type stubs for the parts of opentelemetry-instrumentation-logging artifactr uses."""

import logging

from opentelemetry._logs import LoggerProvider

class LoggingHandler(logging.Handler):
    def __init__(
        self,
        level: int = ...,
        logger_provider: LoggerProvider | None = None,
        log_code_attributes: bool = False,
    ) -> None: ...
