# grainlift-python

Build ADBC services in Python. Applications load the existing native Grainlift
ADBC driver; your worker supplies query behavior and lazy Arrow batches over
VGI-RPC. No downstream ADBC driver is required.

The toolkit exposes the Grainlift protocol 0.4.0 ADBC operation surface over HTTP:
transactions, statements, preparation, typed options, parameter batches and
streams, updates and ingestion, metadata, partitioned results, and Substrait
plans. Your backend implements each capability through `Connection` and
`Statement` hooks. The toolkit manages authentication, ownership, quotas, Arrow
transport, and cleanup; it does not emulate database semantics. Unimplemented
backend hooks return ADBC `NOT_IMPLEMENTED`.

Existing `Connection.execute(sql)` workers remain supported through a statement
adapter. Those workers retain their original query-only capabilities and should
use `autocommit=True` with the Python ADBC driver manager.

## Development

Python 3.13+ and [uv](https://docs.astral.sh/uv/) are required. Clone this repository
and run:

    uv sync --locked
    ./check_quality.sh
    uv run --no-sync pytest

The toolkit uses published `vgi-rpc[http]>=0.47.1`; the lockfile pins its index
release and dependencies. No modified VGI runtime is required. Unary service
methods return frozen typed response dataclasses, which VGI serializes through
its standard binary result envelope. Opening connections, setting options, and
filtered metadata discovery use named request dataclasses with native Arrow
fields. Option values use a nested typed record, and partition descriptors use
an Arrow binary list with signed typed claims. Parameter uploads use a
fixed one-row envelope so empty and zero-column Arrow data remain unambiguous.
Protocol 0.4.0 requires a matching Grainlift client; earlier wire clients must be
upgraded together with the service. See [the API guide](docs/API.md) for details.

The CI workflow checks Linux/macOS with Python 3.13/3.14, runs Ruff, formatting,
strict mypy and isolated pydoclint, then installs the built wheel and runs its
tests. A configured workflow is not evidence that every matrix job has passed;
consult the repository's Actions results for the revision being deployed.

See [grainlift-hello-world-python](https://github.com/Query-farm/grainlift-hello-world-python)
for a complete worker and ordinary ADBC client.

## Authoring statements

Override `Connection.new_statement()` to create independent backend statements.
This small example implements one query; add only capabilities the backend can
perform correctly. See [the API contract](docs/API.md) for every hook.

```python
import pyarrow as pa
from grainlift import AdbcError, Connection, QueryResult, Statement, Worker

SCHEMA = pa.schema([("answer", pa.int64())])

class AnswerStatement(Statement):
    def __init__(self) -> None:
        self.sql: str | None = None

    def set_sql_query(self, sql: str) -> None:
        self.sql = sql

    def execute(self) -> QueryResult:
        if self.sql != "SELECT 42":
            raise AdbcError("Expected SELECT 42", "invalid_arguments", sqlstate="42000")
        batch = pa.record_batch([[42]], schema=SCHEMA)
        return QueryResult(SCHEMA, iter([batch]))

class AnswerConnection(Connection):
    def new_statement(self) -> Statement:
        return AnswerStatement()

class AnswerWorker(Worker):
    def connect(self, principal: str) -> Connection:
        return AnswerConnection()
```

For a database-backed service, retain the real backend statement, delegate
`prepare`, `bind`, `bind_stream`, `execute_update`, and other supported hooks,
and close that backend object in `Statement.close()`. Implement transactions in
the connection's option/commit/rollback hooks. Ingestion uses typed statement
options such as `adbc.ingest.target_table` and `adbc.ingest.mode`, parameter
binding, and `execute_update()`; the backend performs the actual ingestion.
Substrait plans are opaque bytes passed to the backend, not translated into SQL.

Return `QueryResult(schema, batch_iterator)` for queries and metadata. Iterators
should expose `close()` to release resources. Generate bounded batches lazily,
and preserve the same schema, including metadata, throughout each result.

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

Defaults: 64 sessions, 32 statements and 32 result handles per session, one query
result per statement, 1 MiB of referenced Arrow buffers per batch, 1 MiB schema
descriptor, 2 MiB HTTP request body, 64 KiB SQL, and 300 seconds idle lifetime.
Metadata and partition readers consume the same result-handle quota. Typed option
responses and serialized partition responses, including their schema, have the
1 MiB response budget. Partition execution returns at most 1,024 descriptors.
HTTP responses have a 2 MiB transport budget, including protocol overhead.
Oversized results are rejected. Parameter uploads use the bounded spool described
below instead of collecting all parameter batches in memory.
Transport/schema overhead can therefore reject a batch below its buffer limit.

Results retain at most one batch for replay. Reading the immediately previous
sequence repeats that batch; skipped/older sequences fail. EOF closes the iterator.
Explicit result release, statement close/reuse, connection close, stream cancel,
idle expiry, and Service.close() release resources. A broken HTTP connection
does not necessarily mean the logical cursor is abandoned: idle expiry cleans up
clients that disappear. Use Service as a context manager.

Parameter binding uses an anonymous Arrow IPC spool with a 64 MiB cumulative
`Limits.bind_bytes` budget, including schema, dictionaries, and stream framing.
Each batch also obeys `Limits.batch_bytes`. `bind` accepts one batch;
`bind_stream` accepts a sequence, including an empty stream with a known schema.
An explicit finish turn distinguishes successful end-of-input from disconnect.
The backend receives parameters only after that finish is validated.

Upload turns carry sequence numbers; only an identical immediate replay is
acknowledged again. A statement can own one completed binding and one pending
replacement, each separately capped. Failure, cancellation, or idle expiry of
the pending upload leaves the previous completed binding intact. Execution and
preparation reject unfinished uploads. Backend parameter readers remain valid
until successful replacement, query/plan replacement, or statement close.
Statement close invokes backend cleanup before closing its retained reader.

## Security

Every request and continuation is authenticated. Opaque session handles are
bound to a configured principal; child handles are scoped to that session.
Targets are server-selected. `Service(database_options=..., connection_options=...)`
defines authoritative settings: callers may neither supply those keys in either
opening option scope nor mutate them afterward. Other caller options are decoded
strictly and delegated to the worker. Option values are `str`, `bytes`, signed
64-bit `int`, or IEEE 754 `float` (including NaN and infinities); booleans and duplicate keys are rejected.

`Worker.open_connection(principal, database_options, connection_options)` is the
database-factory hook. Its default rejects database options, calls the existing
`connect(principal)`, and applies connection options with cleanup on failure.
Override it for an actual configured database factory. `get_option` results are
client-visible: never expose credentials through a backend getter.

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

Partition descriptors are signed wrappers bound to the service instance, target,
and authenticated principal. The same principal can read one through another
connection until its `Limits.idle_seconds` expiry. Tampering, another principal,
another service instance, and expired descriptors are rejected before backend
execution. A restart invalidates wrappers. The toolkit creates no durable
partition store and makes no cross-replica or transaction-recovery guarantee.

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
    max_statements=32,
    max_bind_bytes=64 * 1024 * 1024,
)
service = Service(worker, limits=Limits(sessions=16))
app = service.app(tokens={"replace-with-a-secret": "configured-principal"})
```

Each connection owns a spawned process and two anonymous pipes. Worker options
must be JSON-compatible. Results use capped JSON/Arrow IPC messages; SQL executes
once and each fetch pulls one batch. Schema and transport overhead count toward
the IPC cap. Overflow returns INVALID_DATA and releases the affected result.
The child independently caps live statements, results, and parameter spools.
The full statement/connection capability surface is forwarded to its backend;
the selected worker still determines which capabilities are implemented.
Use an importable module and guard executable entry points with
`if __name__ == "__main__":` for multiprocessing spawn.

Opening, statement/connection operations, upload turns, fetching, and cleanup have
finite deadlines. Timeout terminates the child and returns TIMEOUT; a crash
returns IO. Termination waits up to 0.7 additional seconds for the child to exit.
Cancellation also terminates the child. All existing statements/results on that
connection become unusable; the client must close it and reconnect explicitly.
No transaction recovery or rollback behavior is emulated. Per-operation deadlines
do not constitute a single aggregate service-shutdown deadline.

In-process workers may override `Connection.cancel()` and `Statement.cancel()`
with thread-safe, nonblocking hooks. Otherwise cancellation returns
NOT_IMPLEMENTED. Legacy statements delegate cancellation to their connection.
Connection
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
Focused feature tests cover typed options, authoritative configuration,
metadata filters and quotas, statement hooks, transactions, binding replay and
spool cleanup, and signed partition ownership. Isolated-worker and native-driver
fixtures exercise backend delegation separately; see [the API coverage map](docs/API.md).

The wire preserves vendor_code, but the current Rust adbc_ffi dependency in
Grainlift overwrites the C error's vendor-code slot with the ADBC 1.1 private-data
sentinel. Consequently adbc-driver-manager 1.12.0 exposes vendor_code=None in these
tests, even when the worker sent a code. Status, SQLSTATE, and binary details
survive. This is an existing native-client limitation, not a claim of complete
end-to-end error fidelity.
