#!/usr/bin/env bash
# Runs on the host before the dev container is created (ADR-0037). When stackr's stack is
# running, its network (`stackr`) exists: the dev container then joins it, and sends telemetry
# to its Collector. Otherwise the dev container runs with the contributor stack alone.
set -euo pipefail

cd "$(dirname "$0")"
if docker network inspect stackr >/dev/null 2>&1; then
  cat >stackr.generated.yaml <<'YAML'
# Written by initialize.sh: stackr's stack is running.
services:
  dev:
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4318
      LANGFUSE_BASE_URL: http://langfuse-web:3000
    networks:
      - default
      - stackr
networks:
  stackr:
    external: true
YAML
else
  cat >stackr.generated.yaml <<'YAML'
# Written by initialize.sh: stackr's stack is not running.
services: {}
YAML
fi
