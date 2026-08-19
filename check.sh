#!/usr/bin/env bash
set -euo pipefail

echo "==> ruff check"
uv run ruff check .

echo "==> ruff format"
uv run ruff format --check .

echo "==> mypy"
uv run mypy services packages

echo "==> all checks passed"
