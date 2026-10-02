# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`grainlift` (PyPI distribution `grainlift`, GitHub repo `Query-farm/grainlift-python`)
is a Python toolkit for building ADBC services. Clients load the native Grainlift
ADBC driver (`Query-farm/grainlift`, Rust); a Python worker supplies query behavior
and lazy Arrow batches over VGI-RPC (`vgi-rpc[http]`). The toolkit handles
authentication, ownership, quotas, Arrow transport and cleanup; it does not emulate
database semantics. It speaks Grainlift protocol **0.4.0**
(`src/grainlift/protocol.py`), which must match the native client version.

## Commands

    uv sync --locked --extra granian --extra storage   # set up the environment (Python 3.13+)
    ./check_quality.sh                 # ruff check, ruff format --check, strict mypy, pydoclint
    uv run --no-sync pytest            # full test suite
    uv build                           # sdist + wheel via hatchling

`tests/test_native_hosting.py` skips unless `GRAINLIFT_NATIVE_DRIVER` points at a
compiled `libadbc_driver_grainlift` shared library (CI builds it from a pinned
`Query-farm/grainlift` commit).

## Layout

- `src/grainlift/api.py` — author-facing API: `Worker`, `Connection`, `Statement`,
  `QueryResult`, `ResultProducer`, `PartitionedResult`, `AdbcError`, `Limits`.
- `server.py` — `Service`, sessions, authenticated WSGI app, `serve()` (Waitress).
- `tcp.py` — `TcpServer`/`TLSConfig`: bounded TCP/mTLS hosting.
- `hosting.py` — optional `serve_granian` (needs the `granian` extra).
- `storage.py` — `ExternalStorageConfig`: S3-compatible bucket for large requests and
  results (VGI-RPC external locations, SigV4 presigned URLs); needs the `storage` extra.
- `isolation.py` — `IsolatedWorker`, process-per-connection execution.
- `protocol.py`, `requests.py`, `wire.py`, `options.py`, `tokens.py`, `binding.py`
  — typed wire contract, request records, option codecs, signed partition claims,
  upload staging.
- `cli.py` — development CLI: `run()` for a worker's own console script, or
  `python -m grainlift.cli serve module:Factory`. The package installs no
  console script of its own, to avoid colliding with other `grainlift` commands.
- `docs/API.md`, `docs/HOSTING.md` — API contract and hosting guide.

Public exports live in `src/grainlift/__init__.py`; keep `__all__` in sync.

## Conventions

- Every source file starts with the `# Copyright (c) 2026 Query Farm LLC` and
  `# SPDX-License-Identifier: Apache-2.0` header, then a one-line module docstring.
- Google-style docstrings (ruff `D` rules + pydoclint). mypy runs in strict mode over
  `src` and `tests`. Line length is 120.
- pydoclint runs through `uvx` in `check_quality.sh`, never in the project
  environment: its docstring parser conflicts with VGI-RPC's.
- The README is rendered on PyPI, so links to repo files must be absolute
  `https://github.com/Query-farm/grainlift-python/blob/main/...` URLs.
- GitHub Actions are pinned to full commit SHAs with a version comment. When
  choosing a version, sort tags with `sort -V` (lexical sort puts v1.9 after v1.14).

## Releasing

Publishing is automated by `.github/workflows/publish.yml` using PyPI Trusted
Publishing (environment `pypi`, no API token):

1. Bump `version` in `pyproject.toml` and run `uv lock`.
2. Commit and push to `main`.
3. `git tag vX.Y.Z && git push origin vX.Y.Z`.

The workflow fails if the tag does not equal `v` + the project version, then runs
the quality checks, builds, runs `twine check --strict`, tests the installed wheel
and uploads. PyPI versions can never be reused.
