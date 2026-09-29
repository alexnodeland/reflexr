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
    """The parts of Langfuse's HTTP API the adapters use, in memory."""

    def __init__(self, *configs: str, page_size: int = 2) -> None:
        self.configs: list[dict[str, Any]] = [{"name": name} for name in configs]
        self.scores: dict[str, dict[str, Any]] = {}
        self.page_size = page_size

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/public/ingestion":
            for event in json.loads(request.content)["batch"]:
                if event["type"] == "score-create":
                    self.scores[event["body"]["id"]] = event["body"]
            return httpx.Response(207, json={"successes": [], "errors": []})
        if path == "/api/public/score-configs" and request.method == "POST":
            self.configs.append(json.loads(request.content))
            return httpx.Response(200, json=self._config(self.configs[-1]))
        if path == "/api/public/score-configs":
            page = int(request.url.params["page"])
            chunk = self.configs[(page - 1) * self.page_size : page * self.page_size]
            pages = -(-len(self.configs) // self.page_size)
            meta = {"page": page, "limit": 2, "totalItems": len(self.configs), "totalPages": pages}
            return httpx.Response(
                200, json={"data": [self._config(c) for c in chunk], "meta": meta}
            )
        return httpx.Response(404)

    @staticmethod
    def _config(config: dict[str, Any]) -> dict[str, Any]:
        return {
            "dataType": "NUMERIC",
            **config,
            "id": config["name"],
            "createdAt": "2026-09-28T00:00:00Z",
            "updatedAt": "2026-09-28T00:00:00Z",
            "projectId": "project",
            "isArchived": False,
        }


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
