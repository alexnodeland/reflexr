"""A real Langfuse client against an in-process Langfuse API: no network."""

import json
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from langfuse import Langfuse
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from reflexr.langfuse import langfuse_client


class FakeLangfuseApi:
    """The part of Langfuse's HTTP API a client sends scores to, in memory."""

    def __init__(self) -> None:
        self.scores: dict[str, dict[str, Any]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/public/ingestion":
            for event in json.loads(request.content)["batch"]:
                if event["type"] == "score-create":
                    self.scores[event["body"]["id"]] = event["body"]
            return httpx.Response(207, json={"successes": [], "errors": []})
        return httpx.Response(404)


class Backend:
    """A Langfuse client, the API it talks to, and the spans it exports."""

    def __init__(self, api: FakeLangfuseApi, **options: Any) -> None:
        self.api = api
        self.spans = InMemorySpanExporter()
        self.tracer_provider = TracerProvider()
        self.client: Langfuse = langfuse_client(
            tracer_provider=self.tracer_provider,
            public_key=f"pk-lf-{uuid.uuid4()}",  # Langfuse shares resources by public key
            secret_key="sk-lf-test",
            base_url="http://langfuse.test",
            httpx_client=httpx.Client(transport=httpx.MockTransport(api.handle)),
            span_exporter=self.spans,
            **options,
        )

    def exported(self) -> list[str]:
        self.tracer_provider.force_flush()
        self.client.flush()
        return [span.name for span in self.spans.get_finished_spans()]


@pytest.fixture
def api() -> FakeLangfuseApi:
    return FakeLangfuseApi()


@pytest.fixture
def backend(api: FakeLangfuseApi) -> Iterator[Backend]:
    backend = Backend(api)
    yield backend
    backend.client.shutdown()
    backend.tracer_provider.shutdown()
