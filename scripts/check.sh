#!/usr/bin/env bash
set -euo pipefail

uv run pytest tests
uv run ruff format .
uv run ruff check .
