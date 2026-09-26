# grainlift-python

Build ADBC services in Python. Applications load the existing native Grainlift
ADBC driver; your worker supplies query behavior and lazy Arrow batches over
VGI-RPC. No downstream ADBC driver is required.

This is an initial, HTTP-only implementation of Grainlift protocol 0.2.0,
not a complete ADBC driver SDK. It supports authenticated connections, statements,
SQL execution, optional schema inference, and pull-based results. Transactions,
prepared statements, binding, metadata discovery, ingestion, partitioned results,
and Substrait return ADBC NOT_IMPLEMENTED. Cancellation is opt-in, as described below.
Use autocommit=True with the Python ADBC driver manager.

## Development

Python 3.13+ and [uv](https://docs.astral.sh/uv/) are required. Clone this repository
and run:

    uv sync --locked
    ./check_quality.sh
    uv run --no-sync pytest

The lockfile and source override pin public VGI-RPC revision
`cf0564f0ea9780ffbbff18995b3d027d3aa41d75`, which includes the explicit unary
RecordBatch return-schema support and raw wire-error message fix.
PyPI `vgi-rpc==0.47.1` alone is insufficient. Public repository availability is
separate from package-index publication; publish a new VGI-RPC version and update
the dependency floor before releasing this package to an index.

The CI workflow checks Linux/macOS with Python 3.13/3.14, runs Ruff, formatting,
strict mypy and isolated pydoclint, then installs the built wheel and runs its
tests. A configured workflow is not evidence that every matrix job has passed;
consult the repository's Actions results for the revision being deployed.

See [grainlift-hello-world-python](https://github.com/Query-farm/grainlift-hello-world-python)
for a complete worker and ordinary ADBC client.
The author API is Worker.connect(principal) -> Connection, and
Connection.execute(sql) -> QueryResult(schema, batch_iterator).
Override Connection.execute_schema(sql) for schema inference.
Iterators should expose close() to release resources. Worker code must generate
bounded batches and cooperate with cleanup. Do not materialize a whole result.

## Lifecycle and resource contract

Each service owns its sessions in one process. Route all calls, including
continuations, to that process. Restart invalidates every handle. There is no
transparent multi-replica behavior. Calls on one session serialize under its own
lock; independent sessions and connection factories can run concurrently. Worker
factories must therefore be thread-safe. Opening connections reserve session quota
before invoking the factory. The reaper skips busy sessions.

Session lock waits default to five seconds. In-process callbacks must still be
bounded and cooperative: Python threads cannot safely preempt arbitrary calls.
`Service.close()` rejects new work and requests cancellation of busy connections.
It waits up to `Limits.shutdown_seconds` for busy session locks, then reports ADBC
TIMEOUT if callbacks remain. Their eventual return triggers cleanup. This wait
budget does not preempt in-process cancellation or cleanup hooks.

Defaults: 64 sessions, 32 statements per session, one result per statement,
1 MiB of referenced Arrow buffers per batch, 1 MiB schema descriptor,
2 MiB HTTP request body, 64 KiB SQL, and 300 seconds idle lifetime.
HTTP responses have a 2 MiB transport budget, including protocol overhead.
Oversized data is rejected, not silently buffered or externalized.
Transport/schema overhead can therefore reject a batch below its buffer limit.

Results retain at most one batch for replay. Reading the immediately previous
sequence repeats that batch; skipped/older sequences fail. EOF closes the iterator.
Explicit result release, statement close/reuse, connection close, stream cancel,
idle expiry, and Service.close() release resources. A broken HTTP connection
does not necessarily mean the logical cursor is abandoned: idle expiry cleans up
clients that disappear. Use Service as a context manager.

## Security

Every request and continuation is authenticated. Opaque session handles are
bound to a configured principal; child handles are scoped to that session.
Targets are server-selected and caller database/connection options are rejected.
Only enabling autocommit is accepted as a connection mutation.

The CLI binds to loopback and requires a bearer token. For deployment, host
Service.app(tokens={token: principal}) behind HTTPS and enforce process affinity.
Do not expose plain HTTP with bearer tokens on an untrusted network.
TCP, mTLS, and Iroh serving are not implemented or validated here.

The WSGI wrapper filters VGI-RPC transport logs during Grainlift requests because
diagnostics can contain SQL, credentials, Arrow values, and raw exceptions. It
preserves logger levels, handlers, propagation, and unrelated applications' logs.
It installs filters on loaded VGI-RPC loggers at app creation and request entry;
re-audit this boundary when adding transports or upgrading VGI-RPC. Grainlift's
own access event contains only status and duration. AdbcError preserves client-facing status,
SQLSTATE, vendor code and binary details; unexpected worker errors become a generic
INTERNAL error. Do not include secrets in client-facing AdbcError messages.
Worker-authored logging is the worker author's responsibility.

## Optional process isolation

```python
from grainlift import IsolatedWorker, Limits, Service

worker = IsolatedWorker(
    "my_worker:MyWorker",  # Importable Worker subclass, constructed in the child.
    worker_options={"example_setting": "value"},
    timeout_seconds=5,
    startup_timeout_seconds=10,
    max_message_bytes=2 * 1024 * 1024,
    max_results=32,
)
service = Service(worker, limits=Limits(sessions=16))
app = service.app(tokens={"replace-with-a-secret": "configured-principal"})
```

Each connection owns a spawned process and two anonymous pipes. Worker options
must be JSON-compatible. Results use capped JSON/Arrow IPC messages; SQL executes
once and each fetch pulls one batch. Schema and transport overhead count toward
the IPC cap. Overflow returns INVALID_DATA and releases the affected result.
The child also caps live results independently of the service's statement quota.
Use an importable module and guard executable entry points with
`if __name__ == "__main__":` for multiprocessing spawn.

Opening, execution, fetching, schema inference, result release, and closing have
finite deadlines. Timeout terminates the child and returns TIMEOUT; a crash
returns IO. Termination waits up to 0.7 additional seconds for the child to exit.
Cancellation also terminates the child. All existing statements/results on that
connection become unusable; the client must close it and reconnect explicitly.
No transaction recovery or rollback behavior is emulated. Per-operation deadlines
do not constitute a single aggregate service-shutdown deadline.

In-process workers may override `Connection.cancel()` with a thread-safe,
nonblocking hook. Otherwise cancellation returns NOT_IMPLEMENTED. Connection
cancellation authenticates the session owner; statement cancellation additionally
requires an operation currently active on that statement. Closing an HTTP result
stream releases that cursor and is separate from ADBC cancellation.

Isolation contains callback hangs and crashes; it is not a sandbox for untrusted
code. Session quotas bound child count and IPC limits bound transferred data.
Apply OS/container memory and CPU quotas to bound allocations inside a worker.
Workers must not spawn unmanaged descendants. Child stdout/stderr are discarded;
worker-managed logging destinations remain the author's responsibility.

## Validation and native-client limitation

Tests cover the actual native Grainlift C ABI through adbc-driver-manager,
including multiple batches, empty results with schemas, schema inference,
authentication, early release, errors, and handle cleanup. Toolkit tests exercise
principal isolation (including HTTP continuations), immediate replay, idle expiry,
stream cancellation, invalid schemas, and limits below, at, and above boundaries.

The wire preserves vendor_code, but the current Rust adbc_ffi dependency in
Grainlift overwrites the C error's vendor-code slot with the ADBC 1.1 private-data
sentinel. Consequently adbc-driver-manager 1.12.0 exposes vendor_code=None in these
tests, even when the worker sent a code. Status, SQLSTATE, and binary details
survive. This is an existing native-client limitation, not a claim of complete
end-to-end error fidelity.
