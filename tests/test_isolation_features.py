# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Spawned-process coverage for statement, binding, transaction and metadata hooks."""

from __future__ import annotations

import json
import multiprocessing
import os
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from grainlift import (
    AdbcError,
    Connection,
    IsolatedWorker,
    OptionValue,
    PartitionedResult,
    QueryResult,
    Statement,
    Worker,
)
from grainlift.isolation import _arrow_bytes, _ChildState, _ProcessConnection

SCHEMA = pa.schema([("n", pa.int64())])


def result(values: list[int]) -> QueryResult:
    """Create one small result batch whose schema is stable."""
    return QueryResult(SCHEMA, iter([pa.record_batch([values], schema=SCHEMA)]))


class FaultingResultIterator(Iterator[pa.RecordBatch]):
    """Expose a primary result failure followed by a distinct cleanup failure."""

    def __init__(self, connection: FeatureConnection, mode: str) -> None:
        """Retain the connection for observable cleanup accounting."""
        self.connection = connection
        self.mode = mode

    def __next__(self) -> pa.RecordBatch:
        """Raise a structured backend error or return an invalid result batch."""
        if self.mode == "fetch_schema":
            return pa.record_batch([[1]], names=["wrong"])
        if self.mode == "fetch_size":
            return pa.record_batch([list(range(1024))], schema=SCHEMA)
        raise AdbcError(
            "Primary fetch failure", "io", sqlstate="HY001", vendor_code=73, details={"binary": b"\x00\xff"}
        )

    def close(self) -> None:
        """Record cleanup before raising a secondary error."""
        self.connection.options["result_closes"] = int(self.connection.options.get("result_closes", 0)) + 1
        raise RuntimeError("Secondary cleanup failure")


class FeatureStatement(Statement):
    """Implement all statement hooks with observable ownership and deterministic faults."""

    def __init__(self, connection: FeatureConnection) -> None:
        """Retain the owner and initialize statement-local state."""
        self.connection = connection
        self.sql = "ok"
        self.options: dict[str, OptionValue] = {}
        self.reader: pa.RecordBatchReader | None = None
        self.batch: pa.RecordBatch | None = None

    def set_sql_query(self, sql: str) -> None:
        """Replace SQL and discard previous bound input."""
        self.sql = sql
        self.reader = None
        self.batch = None

    def set_substrait_plan(self, payload: bytes) -> None:
        """Preserve the plan bytes for typed-option inspection."""
        self.options["plan"] = payload

    def prepare(self) -> None:
        """Mark the statement prepared or block for the cancellation test."""
        self.connection.event("prepare")
        if self.sql == "hang":
            time.sleep(30)
        if self.sql == "crash":
            os._exit(7)
        self.options["prepared"] = 1

    def bind(self, batch: pa.RecordBatch) -> None:
        """Retain a single Arrow parameter batch."""
        self.batch = batch
        self.reader = None

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Retain the lazy spool reader or reject the binding without consuming it."""
        if self.sql == "reject_bind":
            raise AdbcError("Binding rejected", "invalid_arguments")
        self.reader = reader
        self.batch = None

    def execute(self) -> QueryResult:
        """Return bound input lazily or one default batch."""
        if self.sql.startswith("fault_"):
            mode = self.sql.removeprefix("fault_")
            schema = SCHEMA.with_metadata({b"large": b"x" * 4096}) if mode == "schema_size" else SCHEMA
            return QueryResult(
                schema, FaultingResultIterator(self.connection, mode), 2**63 if mode == "row_count" else 0
            )
        if self.reader is not None:
            return QueryResult(self.reader.schema, iter(self.reader))
        if self.batch is not None:
            return QueryResult(self.batch.schema, iter([self.batch]))
        return result([42])

    def execute_update(self) -> int | None:
        """Count input rows, modeling a backend ingestion operation."""
        if self.sql == "unknown_update":
            return None
        if self.reader is not None:
            return sum(batch.num_rows for batch in self.reader)
        return self.batch.num_rows if self.batch is not None else 7

    def execute_schema(self) -> pa.Schema:
        """Expose the fixture result schema."""
        return SCHEMA

    def get_parameter_schema(self) -> pa.Schema:
        """Expose the fixture parameter schema."""
        return SCHEMA

    def execute_partitions(self) -> PartitionedResult:
        """Return binary descriptors, including empty and non-UTF8 values."""
        if self.sql == "large_partitions":
            return PartitionedResult(SCHEMA, [b"x" * 4096])
        if self.sql == "many_partitions":
            return PartitionedResult(SCHEMA, [b""] * 1025)
        return PartitionedResult(SCHEMA, [b"\x00\xff", b""], 2)

    def set_option(self, key: str, value: OptionValue) -> None:
        """Set a statement-local typed option."""
        self.options[key] = value

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Return a statement-local typed option."""
        return self.options[key]

    def close(self) -> None:
        """Record close while bound readers remain usable for backend cleanup."""
        self.connection.options["closed_statements"] = int(self.connection.options.get("closed_statements", 0)) + 1
        if self.reader is not None:
            # Closing an exhausted reader is safe and exercises close ordering.
            self.reader.close()
        self.connection.event("statement_closed")


class FeatureConnection(Connection):
    """Echo hook arguments using Arrow batches and typed options."""

    def __init__(self, directory: str = "") -> None:
        """Initialize independent state for every child connection."""
        self.options: dict[str, OptionValue] = {}
        self.directory = directory

    def event(self, name: str) -> None:
        """Record a fixed lifecycle label without application values."""
        if self.directory:
            with (Path(self.directory) / name).open("a") as output:
                output.write("event\n")

    def new_statement(self) -> Statement:
        """Allocate a fully featured statement."""
        return FeatureStatement(self)

    def set_option(self, key: str, value: OptionValue) -> None:
        """Retain a typed option independently of all statements."""
        self.options[key] = value

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Return a typed option for a roundtrip assertion."""
        return self.options[key]

    def commit(self) -> None:
        """Record the backend commit hook."""
        self.options["transaction"] = "committed"

    def rollback(self) -> None:
        """Record the backend rollback hook."""
        self.options["transaction"] = "rolled_back"

    def get_info(self, codes: list[int] | None) -> QueryResult:
        """Return requested metadata codes as a lazy result."""
        return result(codes or [99])

    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult:
        """Record every catalog filter and return the requested depth."""
        self.options["objects"] = json.dumps([depth, catalog, db_schema, table_name, table_types, column_name])
        return result([depth])

    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema:
        """Record qualified table identifiers and return the schema."""
        self.options["table"] = json.dumps([catalog, db_schema, table_name])
        return SCHEMA

    def get_table_types(self) -> QueryResult:
        """Return a distinguishable lazy table-type fixture result."""
        return result([2])

    def get_statistic_names(self) -> QueryResult:
        """Return a distinguishable lazy statistic-name fixture result."""
        return result([3])

    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult:
        """Record statistics filters and approximation preference."""
        self.options["statistics"] = json.dumps([catalog, db_schema, table_name, approximate])
        return result([4])

    def read_partition(self, partition: bytes) -> QueryResult:
        """Echo opaque partition bytes as row values."""
        return result(list(partition))

    def close(self) -> None:
        """Record connection close after child statements have closed."""
        self.event("connection_closed")


class FeatureWorker(Worker):
    """Create the importable fixture in a real spawned child process."""

    def __init__(self, directory: str = "") -> None:
        """Retain an optional test-owned lifecycle directory."""
        self.directory = directory

    def open_connection(
        self, principal: str, database_options: Mapping[str, OptionValue], connection_options: Mapping[str, OptionValue]
    ) -> Connection:
        """Preserve initial typed options and the authenticated principal."""
        connection = FeatureConnection(self.directory)
        connection.options.update(database_options)
        connection.options.update(connection_options)
        connection.options["principal"] = principal
        return connection


def isolated(**options: object) -> _ProcessConnection:
    """Open a real child with small per-message limits for focused tests."""
    worker = IsolatedWorker("test_isolation_features:FeatureWorker", max_message_bytes=4096, **options)  # type: ignore[arg-type]
    return worker.connect("alice")


def values(query: QueryResult) -> list[int]:
    """Drain and close a small fixture result."""
    try:
        return [int(cast(int, item)) for batch in query.batches for item in batch.column(0).to_pylist()]
    finally:
        query.close()


def test_statement_typed_options_prepare_bind_and_partitions() -> None:
    """Exercise every statement hook across the actual child-process boundary."""
    connection = isolated()
    statement = connection.new_statement()
    try:
        statement.set_sql_query("bound")
        statement.prepare()
        assert statement.get_option("prepared", "int") == 1
        assert statement.execute_schema().equals(SCHEMA)
        assert statement.get_parameter_schema().equals(SCHEMA)
        statement.set_substrait_plan(b"\x00\xff")
        assert statement.get_option("plan", "bytes") == b"\x00\xff"
        options: list[tuple[str, OptionValue]] = [
            ("string", "text"),
            ("bytes", b"\x00\xff"),
            ("int", -(2**63)),
            ("double", 1.25),
        ]
        for kind, value in options:
            statement.set_option(kind, value)
            assert statement.get_option(kind, kind) == value
        statement.set_option("adbc.ingest.target_table", "items")
        statement.bind(pa.record_batch([[1, 2]], schema=SCHEMA))
        assert values(statement.execute()) == [1, 2]
        assert statement.execute_update() == 2
        partitions = statement.execute_partitions()
        assert partitions.schema.equals(SCHEMA)
        assert partitions.partitions == [b"\x00\xff", b""]
        assert partitions.rows_affected == 2
        assert values(connection.read_partition(partitions.partitions[0])) == [0, 255]
        statement.set_sql_query("unknown_update")
        assert statement.execute_update() is None
    finally:
        statement.close()
        connection.close()


def test_initial_options_transactions_and_metadata() -> None:
    """Preserve initial options and delegate all connection capabilities."""
    worker = IsolatedWorker("test_isolation_features:FeatureWorker")
    connection = worker.open_connection("alice", {"destination": b"\x00\xff"}, {"timeout": 9})
    try:
        assert connection.get_option("destination", "bytes") == b"\x00\xff"
        assert connection.get_option("timeout", "int") == 9
        assert connection.get_option("principal", "string") == "alice"
        options: list[tuple[str, OptionValue]] = [
            ("string", "text"),
            ("bytes", b"\x00\xff"),
            ("int", 2**63 - 1),
            ("double", -1.25),
        ]
        for kind, value in options:
            connection.set_option(kind, value)
            assert connection.get_option(kind, kind) == value
        connection.commit()
        assert connection.get_option("transaction", "string") == "committed"
        connection.rollback()
        assert connection.get_option("transaction", "string") == "rolled_back"
        assert values(connection.get_info([1, 2])) == [1, 2]
        assert values(connection.get_info(None)) == [99]
        assert values(connection.get_objects(2, "c", None, "t", ["TABLE"], "col")) == [2]
        assert json.loads(str(connection.get_option("objects", "string"))) == [2, "c", None, "t", ["TABLE"], "col"]
        assert connection.get_table_schema(None, "public", "t").equals(SCHEMA)
        assert json.loads(str(connection.get_option("table", "string"))) == [None, "public", "t"]
        assert values(connection.get_table_types()) == [2]
        assert values(connection.get_statistic_names()) == [3]
        assert values(connection.get_statistics(None, "s", "t", True)) == [4]
        assert json.loads(str(connection.get_option("statistics", "string"))) == [None, "s", "t", True]
    finally:
        connection.close()


def test_stream_binding_is_lazy_in_child_and_released_on_replace() -> None:
    """Spool multiple messages and let execution consume the child reader lazily."""
    connection = isolated(max_bind_bytes=65536)
    statement = connection.new_statement()
    try:
        batches = (pa.record_batch([[index] * 128], schema=SCHEMA) for index in range(20))
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, batches))
        assert values(statement.execute()) == [index for index in range(20) for _ in range(128)]
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, [pa.record_batch([[8, 9]], schema=SCHEMA)]))
        assert statement.execute_update() == 2
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, []))
        assert statement.execute_update() == 0
        statement.set_sql_query("ok")
        assert values(statement.execute()) == [42]
    finally:
        statement.close()
        assert connection.get_option("closed_statements", "int") == 1
        connection.close()


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_stream_binding_aggregate_boundary(headroom: int) -> None:
    """Enforce the full serialized stream budget immediately below, at, and above."""
    batch = pa.record_batch([[1, 2]], schema=SCHEMA)
    size = len(_arrow_bytes(SCHEMA, batch, 4096))
    connection = isolated(max_bind_bytes=size + headroom)
    statement = connection.new_statement()
    try:
        reader = pa.RecordBatchReader.from_batches(SCHEMA, [batch])
        if headroom < 0:
            with pytest.raises(AdbcError, match="byte limit"):
                statement.bind_stream(reader)
        else:
            statement.bind_stream(reader)
            assert statement.execute_update() == 2
    finally:
        statement.close()
        connection.close()


@pytest.mark.parametrize("failure", ["oversize", "source", "backend"])
def test_failed_bind_stream_cleans_upload_and_allows_recovery(failure: str) -> None:
    """Abort uploads after serialization, source, or backend errors without leaking handles."""
    connection = isolated(max_bind_bytes=65536)
    statement = connection.new_statement()
    if failure == "backend":
        statement.set_sql_query("reject_bind")

    def batches() -> Iterator[pa.RecordBatch]:
        yield pa.record_batch([[1]], schema=SCHEMA)
        if failure == "source":
            raise ValueError("fixture source failed")
        yield pa.record_batch([[2] * (10000 if failure == "oversize" else 1)], schema=SCHEMA)

    try:
        with pytest.raises((AdbcError, ValueError)):
            statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, batches()))
        statement.set_sql_query("ok")
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, [pa.record_batch([[3]], schema=SCHEMA)]))
        assert statement.execute_update() == 1
    finally:
        statement.close()
        connection.close()


def test_statement_and_result_quotas_and_ownership() -> None:
    """Bound independent statement handles and close their results on reuse or release."""
    connection = isolated(max_statements=2, max_results=1)
    first = connection.new_statement()
    second = connection.new_statement()
    try:
        with pytest.raises(AdbcError, match="statement limit"):
            connection.new_statement()
        old = first.execute()
        replacement = first.execute()
        with pytest.raises(AdbcError, match="unavailable"):
            next(old.batches)
        with pytest.raises(AdbcError, match="result limit"):
            second.execute()
        first.close()
        with pytest.raises(AdbcError, match="unavailable"):
            next(replacement.batches)
        third = connection.new_statement()
        third.close()
        assert values(second.execute()) == [42]
    finally:
        first.close()
        second.close()
        connection.close()


@pytest.mark.parametrize("query", ["large_partitions", "many_partitions"])
def test_partition_descriptor_limits_are_recoverable(query: str) -> None:
    """Reject descriptors beyond count or message limits without losing the statement."""
    connection = isolated()
    statement = connection.new_statement()
    try:
        statement.set_sql_query(query)
        with pytest.raises(AdbcError):
            statement.execute_partitions()
        statement.set_sql_query("ok")
        assert statement.execute_partitions().rows_affected == 2
    finally:
        statement.close()
        connection.close()


@pytest.mark.parametrize("operation", ["timeout", "cancel", "crash"])
def test_prepare_timeout_cancellation_and_crash_cleanup(tmp_path: Path, operation: str) -> None:
    """Apply hard process termination to the new statement callbacks and handles."""
    connection = isolated(
        timeout_seconds=0.5 if operation == "timeout" else 5, worker_options={"directory": str(tmp_path)}
    )
    statement = connection.new_statement()
    statement.set_sql_query("crash" if operation == "crash" else "hang")
    try:
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(statement.prepare)
            deadline = time.monotonic() + 2
            while not (tmp_path / "prepare").exists():
                assert time.monotonic() < deadline
                time.sleep(0.005)
            if operation == "cancel":
                statement.cancel()
            with pytest.raises(AdbcError) as error:
                pending.result(timeout=3)
            assert error.value.status == {"timeout": "timeout", "cancel": "cancelled", "crash": "io"}[operation]
        assert connection._process_closed
        assert connection._send.closed and connection._receive.closed
    finally:
        statement.close()
        connection.close()


def test_shutdown_and_peer_disconnect_close_statements_and_spools(tmp_path: Path) -> None:
    """Close bound child statements on normal shutdown and loss of the parent pipe."""
    before = {child.pid for child in multiprocessing.active_children()}
    for disconnect in (False, True):
        directory = tmp_path / str(disconnect)
        directory.mkdir()
        connection = isolated(worker_options={"directory": str(directory)})
        statement = connection.new_statement()
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, [pa.record_batch([[1]], schema=SCHEMA)]))
        if disconnect:
            connection._send.close()
            deadline = time.monotonic() + 3
            while not (directory / "connection_closed").exists():
                assert time.monotonic() < deadline
                time.sleep(0.005)
            connection._stop("io")
            connection._receive.close()
        else:
            connection.close()
        assert (directory / "statement_closed").exists()
        assert (directory / "connection_closed").exists()
        assert connection._process_closed
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_child_spool_ownership_on_error_rebind_and_close() -> None:
    """Inspect anonymous spool ownership directly to detect descriptor leaks."""
    child = _ChildState(FeatureConnection(), 4096, 2, 2, 65536)
    header, _ = child.dispatch({"op": "new_statement"}, b"")
    sid = header["statement_id"]
    schema = _arrow_bytes(SCHEMA, None, 4096)
    batch = _arrow_bytes(SCHEMA, pa.record_batch([[1]], schema=SCHEMA), 4096)
    try:
        child.dispatch({"op": "bind_begin", "statement_id": sid}, schema)
        first = child.uploads[sid]
        child.dispatch({"op": "bind_batch", "statement_id": sid}, batch)
        child.dispatch({"op": "bind_finish", "statement_id": sid}, b"")
        assert child.bindings[sid] is first
        assert not first.closed
        child.dispatch({"op": "bind_begin", "statement_id": sid}, schema)
        failed = child.uploads[sid]
        with pytest.raises(pa.ArrowException):
            child.dispatch({"op": "bind_batch", "statement_id": sid}, b"invalid")
        assert failed.closed and not first.closed
        child.dispatch({"op": "statement_sql", "statement_id": sid, "sql": "ok"}, b"")
        assert first.closed and not child.bindings and not child.uploads
        statement = cast(FeatureStatement, child.statements[sid])
        assert statement.reader is None
    finally:
        child.close()


@pytest.mark.parametrize("name", ["max_statements", "max_bind_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_isolation_feature_limits_reject_invalid_configuration(name: str, value: object) -> None:
    """Require positive integer quotas before any child process can start."""
    with pytest.raises(ValueError):
        IsolatedWorker("test_isolation_features:FeatureWorker", **{name: value})  # type: ignore[arg-type]


def test_large_initial_options_rejected_before_process_creation() -> None:
    """Check binary initial-option bounds before spawning or encoding large values."""
    before = {child.pid for child in multiprocessing.active_children()}
    worker = IsolatedWorker("test_isolation_features:FeatureWorker", max_message_bytes=4096)
    with pytest.raises(AdbcError, match="exceeds message limit"):
        worker.open_connection("alice", {"oversized": b"x" * 4097}, {})
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_statement_close_does_not_cancel_another_handle() -> None:
    """Reject cancellation on a closed statement while preserving its live connection."""
    connection = isolated()
    first = connection.new_statement()
    second = connection.new_statement()
    try:
        first.close()
        with pytest.raises(AdbcError) as error:
            first.cancel()
        assert error.value.status == "invalid_state"
        assert values(second.execute()) == [42]
    finally:
        second.close()
        connection.close()


def test_bind_stream_total_deadline_cleans_partial_spool() -> None:
    """Apply a total upload deadline across multiple individually quick IPC calls."""
    connection = isolated(timeout_seconds=0.1)
    statement = connection.new_statement()

    def batches() -> Iterator[pa.RecordBatch]:
        yield pa.record_batch([[1]], schema=SCHEMA)
        time.sleep(0.12)
        yield pa.record_batch([[2]], schema=SCHEMA)

    try:
        with pytest.raises(AdbcError) as error:
            statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, batches()))
        assert error.value.status == "timeout"
        # Only the local upload budget expired; no callback was active when it
        # expired. The aborted anonymous spool must not poison this connection.
        statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, []))
        assert statement.execute_update() == 0
    finally:
        statement.close()
        connection.close()


def test_metadata_results_share_the_configured_cursor_quota() -> None:
    """Bound metadata cursors together with query results and release their slots."""
    connection = isolated(max_results=1)
    try:
        first = connection.get_info(None)
        with pytest.raises(AdbcError, match="result limit"):
            connection.get_table_types()
        first.close()
        assert values(connection.get_statistic_names()) == [3]
    finally:
        connection.close()


@pytest.mark.parametrize("operation", ["prepare", "execute", "update", "schema", "partitions"])
def test_child_rejects_execution_during_pending_bind(operation: str) -> None:
    """Apply complete-upload checks even if private IPC bypasses the HTTP frontend."""
    child = _ChildState(FeatureConnection(), 4096, 2, 2, 65536)
    header, _ = child.dispatch({"op": "new_statement"}, b"")
    sid = header["statement_id"]
    try:
        child.dispatch({"op": "bind_begin", "statement_id": sid}, _arrow_bytes(SCHEMA, None, 4096))
        with pytest.raises(AdbcError) as error:
            child.dispatch({"op": f"statement_{operation}", "statement_id": sid}, b"")
        assert error.value.status == "invalid_state"
        assert not child.uploads[sid].closed
    finally:
        upload = child.uploads[sid]
        child.close()
        assert upload.closed


@pytest.mark.parametrize("operation", ["prepare", "execute_update", "execute_partitions"])
def test_statement_state_changes_release_previous_child_result(operation: str) -> None:
    """Close old result cursors before preparing or executing a different operation."""
    connection = isolated(max_results=1)
    statement = connection.new_statement()
    try:
        previous = statement.execute()
        getattr(statement, operation)()
        with pytest.raises(AdbcError, match="unavailable"):
            next(previous.batches)
        assert values(statement.execute()) == [42]
    finally:
        statement.close()
        connection.close()


def test_failed_child_rebind_preserves_previous_reader_until_success() -> None:
    """Retain old bound input through rejected replacement, then release it exactly on success."""
    child = _ChildState(FeatureConnection(), 4096, 2, 2, 65536)
    header, _ = child.dispatch({"op": "new_statement"}, b"")
    sid = header["statement_id"]
    schema = _arrow_bytes(SCHEMA, None, 4096)
    batch = _arrow_bytes(SCHEMA, pa.record_batch([[1]], schema=SCHEMA), 4096)

    def stage() -> None:
        child.dispatch({"op": "bind_begin", "statement_id": sid}, schema)
        child.dispatch({"op": "bind_batch", "statement_id": sid}, batch)

    try:
        stage()
        child.dispatch({"op": "bind_finish", "statement_id": sid}, b"")
        previous = child.bindings[sid]
        backend = cast(FeatureStatement, child.statements[sid])
        backend.sql = "reject_bind"
        stage()
        rejected = child.uploads[sid]
        with pytest.raises(AdbcError, match="Binding rejected"):
            child.dispatch({"op": "bind_finish", "statement_id": sid}, b"")
        assert rejected.closed
        assert not previous.closed
        assert previous.reader is not None
        assert previous.reader.read_next_batch().column(0).to_pylist() == [1]
        backend.sql = "ok"
        stage()
        child.dispatch({"op": "bind_finish", "statement_id": sid}, b"")
        assert previous.closed
        assert not child.bindings[sid].closed
    finally:
        child.close()


@pytest.mark.parametrize("mode", ["fetch_error", "fetch_schema", "fetch_size", "schema_size", "row_count"])
def test_result_cleanup_failure_preserves_primary_error(mode: str) -> None:
    """Preserve primary error metadata and release cursor slots when backend cleanup raises."""
    connection = isolated(max_results=1)
    statement = connection.new_statement()
    try:
        statement.set_sql_query(f"fault_{mode}")
        with pytest.raises(AdbcError) as error:
            query = statement.execute()
            next(query.batches)
        assert error.value.status == ("io" if mode == "fetch_error" else "invalid_data")
        if mode == "fetch_error":
            assert json.loads(str(error.value)) == json.loads(
                str(
                    AdbcError(
                        "Primary fetch failure", "io", sqlstate="HY001", vendor_code=73, details={"binary": b"\x00\xff"}
                    )
                )
            )
        assert connection.get_option("result_closes", "int") == 1
        # A separate statement verifies that releasing the failing statement's
        # result, rather than resetting that statement, freed the sole slot.
        other = connection.new_statement()
        try:
            assert values(other.execute()) == [42]
        finally:
            other.close()
        statement.close()
        assert connection.get_option("result_closes", "int") == 1
    finally:
        statement.close()
        connection.close()
