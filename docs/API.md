<!-- Copyright (c) 2026 Query Farm LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python worker API and operation coverage

The public client interface remains ADBC. These hooks implement the server side
of the existing Grainlift wire contract; applications do not need a separate
Python query API. The toolkit transports and bounds each operation. A worker
implements the actual database behavior and raises `AdbcError` when a capability
is unavailable or a backend operation fails.

## Capability map

| ADBC operation | Backend hook | Toolkit behavior |
| --- | --- | --- |
| Database/connection opening | `Worker.open_connection(principal, database_options, connection_options)` | Authenticate, reserve session quota, enforce configured option keys, and clean up failed opening |
| Statement creation | `Connection.new_statement() -> Statement` | Own the statement under its connection and principal |
| SQL and Substrait | `Statement.set_sql_query(sql)`, `set_substrait_plan(payload)` | Bound input size and retire replaced result/binding state |
| Preparation | `Statement.prepare()`, `get_parameter_schema()` | Reject unfinished uploads; preserve backend parameter schema |
| Parameter binding | `Statement.bind(batch)`, `bind_stream(reader)` | Validate schema/sequence, spool bounded Arrow IPC, require explicit finish, and own retained readers |
| Query results | `Statement.execute() -> QueryResult` | Allocate a bounded result handle and pull one batch per fetch with immediate replay |
| Updates and ingestion | `Statement.execute_update() -> int | None` | Preserve the affected-row count; ingestion is configured through statement options and bindings |
| Result schema | `Statement.execute_schema() -> pa.Schema` | Return a bounded Arrow schema without forcing query execution |
| Typed options | `Connection`/`Statement`.`set_option(key, value)` and `get_option(key, value_type)` | Strict string/bytes/int/double wire codec; enforce authoritative configured keys |
| Transactions | `Connection.commit()`, `rollback()` and the autocommit option | Delegate real transaction semantics without implicit SQL or rollback emulation |
| Driver/vendor info | `Connection.get_info(codes)` | Validate unsigned 32-bit codes and allocate a quota-controlled metadata result |
| Object discovery | `Connection.get_objects(depth, catalog, db_schema, table_name, table_types, column_name)` | Preserve null/empty/pattern filters and the requested hierarchy depth |
| Table metadata | `Connection.get_table_schema(catalog, db_schema, table_name)`, `get_table_types()` | Preserve schema responses or bounded discovery cursors |
| Statistics | `Connection.get_statistic_names()`, `get_statistics(catalog, db_schema, table_name, approximate)` | Preserve requested approximation semantics and backend Arrow metadata schemas |
| Partition execution | `Statement.execute_partitions() -> PartitionedResult` | Bound count/serialized bytes and sign descriptors for their owner and expiry |
| Partition reading | `Connection.read_partition(descriptor) -> QueryResult` | Verify the exported wrapper, pass raw backend bytes, and allocate a result cursor |
| Cancellation | `Connection.cancel()`, `Statement.cancel()` | Authenticate independently of the execution lock; isolated cancellation terminates the connection's worker |
| Cleanup | `Statement.close()`, `Connection.close()`, result iterator `close()` | Release result cursors, backend statements, retained bindings, and connections |

`Connection.new_statement()` defaults to an adapter over legacy
`Connection.execute(sql)` and `execute_schema(sql)`. The adapter does not invent
preparation, update, ingestion, metadata, or transaction support. Base capability
hooks return `NOT_IMPLEMENTED`. The legacy connection supports only enabling and
reading autocommit; a transactional backend overrides those option hooks.

## Values and ownership

`QueryResult` holds a stable `pa.Schema`, a lazy `Iterator[pa.RecordBatch]`, and
optional `rows_affected`. Result iterators must release backend cursor resources
when closed. Every emitted batch must match the declared schema, including
metadata. Metadata hooks return the standard ADBC Arrow schemas supplied by the
backend; the toolkit does not synthesize database discovery answers.

`PartitionedResult` holds a `pa.Schema`, `list[bytes]` of backend descriptors,
and `rows_affected` (`-1` when unknown). The native wire encodes exported opaque
bytes as JSON byte arrays. Toolkit wrappers expire after `Limits.idle_seconds`,
are bound to one service instance/target/principal, and may be read from another
connection belonging to that principal. Backend partition lifetime may be
shorter. A wrapper does not guarantee a backend snapshot survives transaction or
connection closure.

`OptionValue` is `str | bytes | int | float`. Wire tags are `string`, `bytes`
(base64), `int` (signed 64-bit), and `double` (finite). Explicit get-option type
mismatches are errors, not coercions. Opening option lists reject duplicate
keys and malformed JSON; configured server keys cannot be shadowed across the
database and connection scopes. The service copies configuration mappings so
later mutation of the caller's mapping does not change authority.

Opening callbacks and independent sessions may run concurrently. Calls on one
session serialize; cancellation hooks are the exception and must be thread-safe
and nonblocking. Statement cancellation requires an active operation on that
statement. In process isolation, cancellation, timeout, or a crash invalidates
the entire backend connection and all child handles; clients must reconnect.

## Binding and ingestion

The transport uploads parameters one batch at a time to an anonymous temporary
Arrow IPC spool. `Limits.bind_bytes` includes the schema, dictionary messages,
batches, and end-of-stream framing; `Limits.batch_bytes` independently bounds
each batch. `bind` requires exactly one batch. `bind_stream` permits an empty
stream with a known schema. The explicit end-of-input marker is separate from
an empty data batch and from a disconnected client.

Only the immediately preceding identical upload turn can be replayed. A new
replacement upload has its own opaque handle and does not invalidate a completed
binding until the replacement backend callback succeeds. Each statement can
therefore retain at most two separately bounded spools. An unfinished upload
blocks execution/preparation, expires when idle, and is released on cancellation
or close. Query/plan replacement closes old bindings. Backend statement cleanup
runs before closing its retained completed reader.

The toolkit does not create tables or convert parameters into insert SQL.
Implement ingestion by handling `adbc.ingest.*` options in the backend statement,
accepting a batch/stream, then performing ingestion in `execute_update()`.
Similarly, Substrait support means the worker receives the exact plan bytes;
only the chosen engine determines which plans it supports.

## Limits and validation

| Resource | Service default |
| --- | ---: |
| Sessions, including opening factories | 64 |
| Statements per session | 32 |
| Live query/metadata/partition result handles per session | 32 |
| Referenced Arrow buffers per batch / schema descriptor bytes | 1 MiB each |
| HTTP request body / Substrait input / typed option input JSON | 2 MiB each |
| Encoded typed option output / partition schema plus descriptor JSON | 1 MiB each |
| SQL UTF-8 bytes | 64 KiB |
| Complete Arrow IPC parameter spool | 64 MiB |
| Partition descriptors per execution | 1,024 |
| Idle resource lifetime / partition wrapper expiry | 300 seconds |
| Session lock wait / shutdown lock-wait budget | 5 seconds each |

The optional `IsolatedWorker` additionally bounds each pipe message, child
statement/result counts, and child parameter spools. Defaults are 2 MiB per
message, 32 child statements, 32 child results, and 64 MiB per binding spool.
These transport and handle limits cannot prevent arbitrary allocations inside
trusted worker code; apply OS/container memory and CPU limits.

Focused regression coverage lives in `tests/test_unary_features.py`,
`tests/test_binding.py`, `tests/test_binding_service.py`, and
`tests/test_isolation_features.py`, with additional ownership, HTTP, lifecycle,
and process-failure tests in the original toolkit suite. The Grainlift native
regression fixture exercises the same hooks through the ordinary ADBC driver,
including real SQLite transactions/prepared parameters/ingestion and explicit
metadata, partition, and Substrait test capabilities. Consult executed test
reports for validation counts; a hook's presence does not imply every backend
implements it.
