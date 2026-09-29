# oncall, reflexr's reference app, as the contributor Compose file's `app` profile runs it.
# Build it from the repository root: `docker compose --profile app build`.

FROM ghcr.io/astral-sh/uv:0.12-python3.12-trixie-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# uv fetches evalr, which the [langfuse] extra needs for the score mapping and ports, from GitHub
# until it is published (ADR-0020, ADR-0025).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN uv sync --locked --no-dev --package oncall

ENV PATH="/app/.venv/bin:$PATH" ONCALL_HOST=0.0.0.0 ONCALL_PORT=8000
EXPOSE 8000
CMD ["oncall-serve"]
