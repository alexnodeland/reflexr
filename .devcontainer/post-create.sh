#!/usr/bin/env bash
set -euo pipefail

# Same as `make install`, without assuming make is present in the base image.
uv sync --all-groups --all-extras --all-packages
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
