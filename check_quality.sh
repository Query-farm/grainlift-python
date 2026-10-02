#!/usr/bin/env bash
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
# Keep pydoclint outside the runtime environment: its parser dependency conflicts
# with VGI-RPC's docstring-parser distribution.
set -euo pipefail
cd "$(dirname "$0")"
uv sync --locked --extra granian --extra storage
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src tests
quality_python=$(uv run python -c 'import sys; print(sys.executable)')
uvx --python "$quality_python" --from pydoclint==0.9.1 pydoclint \
  --config pyproject.toml src tests
