# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Real spawned worker processes, deadlines, cancellation, and bounded IPC."""

from __future__ import annotations

import multiprocessing
import os
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import Connection as PipeConnection
from typing import cast

import pyarrow as pa
import pytest

from grainlift import AdbcError, Connection, IsolatedWorker, QueryResult, Worker
from grainlift.isolation import _BoundedBuffer, _decode, _encode, _ProcessConnection

SCHEMA = pa.schema([("n", pa.int64())])


class ProcessTestWorker(Worker):
    """Inject startup and shutdown faults in child processes."""

    def __init__(self, block_open: bool = False, block_close: bool = False) -> None:
        """Initialize configured state and resource ownership."""
        self.block_open = block_open
        self.block_close = block_close

    def connect(self, principal: str) -> ProcessTestConnection:
        """Open a connection bound to the authenticated principal."""
        if principal == "denied":
            raise AdbcError("Rejected", "unauthorized", sqlstate="28000")
        if self.block_open:
            time.sleep(30)
        return ProcessTestConnection(self.block_close)


class ProcessTestConnection(Connection):
    """Provide queries that trigger process failure paths."""

    def __init__(self, block_close: bool) -> None:
        """Initialize configured state and resource ownership."""
        self.block_close = block_close

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return its schema and lazy batch iterator."""
        if sql == "hang":
            time.sleep(30)
        if sql == "exit":
            os._exit(7)
        if sql == "error":
            raise AdbcError(
                "test",
                "invalid_data",
                sqlstate="22000",
                vendor_code=42,
                details={"raw": b"\x00\xff"},
            )

        def batches() -> Iterator[pa.RecordBatch]:
            if sql == "fetch_hang":
                time.sleep(30)
            size = 10000 if sql == "oversized" else 3
            yield pa.record_batch([list(range(size))], schema=SCHEMA)

        return QueryResult(SCHEMA, batches(), rows_affected=cast(int, float("nan")) if sql == "bad_count" else None)

    def execute_schema(self, sql: str) -> pa.Schema:
        """Infer the result schema without opening a cursor."""
        return SCHEMA

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        if self.block_close:
            time.sleep(30)


def isolated(
    *,
    max_results: int = 32,
    max_message_bytes: int = 2 * 1024 * 1024,
    worker_options: Mapping[str, object] | None = None,
) -> IsolatedWorker:
    """Configure a spawned fixture worker with short deadlines."""
    return IsolatedWorker(
        "test_isolation:ProcessTestWorker",
        timeout_seconds=0.4,
        startup_timeout_seconds=10,
        max_results=max_results,
        max_message_bytes=max_message_bytes,
        worker_options=worker_options,
    )


def test_process_roundtrip_and_cleanup() -> None:
    """Verify process roundtrip and cleanup."""
    connection = isolated().connect("alice")
    try:
        query = connection.execute("ok")
        assert query.schema.equals(SCHEMA)
        assert next(query.batches).column(0).to_pylist() == [0, 1, 2]
        with pytest.raises(StopIteration):
            next(query.batches)
        query.close()
        assert connection.execute_schema("ok").equals(SCHEMA)
    finally:
        connection.close()
    assert connection._process_closed
    assert connection._send.closed
    assert connection._receive.closed


@pytest.mark.parametrize("operation", ["hang", "fetch_hang", "exit"])
def test_timeout_or_crash_invalidates_connection(operation: str) -> None:
    """Verify timeout or crash invalidates connection."""
    connection = isolated().connect("alice")
    started = time.monotonic()
    try:
        with pytest.raises(AdbcError) as exc:
            query = connection.execute(operation)
            next(query.batches)
        assert exc.value.status == ("io" if operation == "exit" else "timeout")
        assert time.monotonic() - started < 3
        assert connection._process_closed
        with pytest.raises(AdbcError):
            connection.execute("ok")
    finally:
        connection.close()


def test_cancellation_terminates_process() -> None:
    """Verify cancellation terminates process."""
    connection = isolated().connect("alice")
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(connection.execute, "hang")
        # Wait for the I/O lock, rather than assuming a scheduler order.
        deadline = time.monotonic() + 1
        while not connection._io_lock.locked():
            assert time.monotonic() < deadline
            time.sleep(0.001)
        connection.cancel()
        with pytest.raises(AdbcError) as exc:
            pending.result(timeout=2)
        assert exc.value.status == "cancelled"
    connection.close()
    assert connection._process_closed


def test_hung_startup_has_deadline() -> None:
    """Verify hung startup has deadline."""
    worker = IsolatedWorker(
        "test_isolation:ProcessTestWorker",
        startup_timeout_seconds=0.5,
        worker_options={"block_open": True},
    )
    started = time.monotonic()
    with pytest.raises(AdbcError) as exc:
        worker.connect("alice")
    assert exc.value.status == "timeout"
    assert time.monotonic() - started < 3


def test_hung_close_is_terminated() -> None:
    """Verify hung close is terminated."""
    connection = isolated(worker_options={"block_close": True}).connect("alice")
    with pytest.raises(AdbcError) as exc:
        connection.close()
    assert exc.value.status == "timeout"
    assert connection._process_closed


def test_oversized_ipc_batch_is_rejected_and_recoverable() -> None:
    """Verify oversized ipc batch is rejected and recoverable."""
    connection = isolated(max_message_bytes=4096).connect("alice")
    try:
        query = connection.execute("oversized")
        with pytest.raises(AdbcError) as exc:
            next(query.batches)
        assert exc.value.status == "invalid_data"
        query.close()
        recovered = connection.execute("ok")
        assert next(recovered.batches).num_rows == 3
        recovered.close()
    finally:
        connection.close()


def test_error_details_survive_process_boundary() -> None:
    """Verify error details survive process boundary."""
    import json

    connection = isolated().connect("alice")
    try:
        with pytest.raises(AdbcError) as exc:
            connection.execute("error")
        wire = json.loads(str(exc.value))
        assert wire["vendor_code"] == 42
        assert wire["sqlstate"] == [50, 50, 48, 48, 48]
        assert wire["details"] == [["raw", "AP8="]]
    finally:
        connection.close()


@pytest.mark.parametrize("size", [4089, 4090, 4091])
def test_message_boundary(size: int) -> None:
    # Four framing bytes and the two-byte JSON object consume six bytes.
    """Verify message boundary."""
    if size > 4090:
        with pytest.raises(AdbcError, match="exceeds limit"):
            _encode({}, b"x" * size, 4096)
    else:
        encoded = _encode({}, b"x" * size, 4096)
        assert _decode(encoded) == ({}, b"x" * size)


@pytest.mark.parametrize("size", [4095, 4096, 4097])
def test_arrow_output_boundary(size: int) -> None:
    """Verify arrow output boundary."""
    out = _BoundedBuffer(4096)
    if size > 4096:
        with pytest.raises(AdbcError, match="exceeds limit"):
            out.write(b"x" * size)
        assert out.tell() == 0
    else:
        assert out.write(b"x" * size) == size


def test_result_quota_and_failed_execute_release_slot() -> None:
    """Verify result quota and failed execute release slot."""
    connection = isolated(max_results=2).connect("alice")
    try:
        with pytest.raises(AdbcError, match="affected row"):
            connection.execute("bad_count")
        first = connection.execute("ok")
        second = connection.execute("ok")
        with pytest.raises(AdbcError, match="result limit"):
            connection.execute("ok")
        first.close()
        recovered = connection.execute("ok")
        recovered.close()
        second.close()
    finally:
        connection.close()


def test_startup_preserves_adbc_error() -> None:
    """Verify startup preserves adbc error."""
    with pytest.raises(AdbcError) as exc:
        isolated().connect("denied")
    assert exc.value.status == "unauthorized"


def test_rejected_startup_closes_every_pipe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Close parent pipe ends even when the child returns a structured startup error."""
    context = multiprocessing.get_context("spawn")
    create_pipe = context.Pipe
    endpoints: list[PipeConnection] = []

    def pipe(duplex: bool = True) -> tuple[PipeConnection, PipeConnection]:
        ends = create_pipe(duplex=duplex)
        endpoints.extend(ends)
        return ends

    monkeypatch.setattr(context, "Pipe", pipe)
    with pytest.raises(AdbcError) as exc:
        isolated().connect("denied")
    assert exc.value.status == "unauthorized"
    assert len(endpoints) == 4
    assert all(endpoint.closed for endpoint in endpoints)


def test_service_shutdown_terminates_busy_child() -> None:
    """Verify service shutdown terminates busy child."""
    from test_service import open_session

    from grainlift import Service

    service = Service(isolated())
    with ThreadPoolExecutor(1) as pool:
        sid, ctx = open_session(service)
        statement = service.new_statement(sid, ctx).statement_id
        service.set_sql_query(sid, statement, "hang", ctx)
        connection = cast(_ProcessConnection, service._sessions[sid].connection)
        pending = pool.submit(service.execute, sid, statement, ctx)
        try:
            deadline = time.monotonic() + 1
            while not connection._io_lock.locked():
                assert time.monotonic() < deadline
                time.sleep(0.001)
            service.close()
            with pytest.raises(AdbcError) as exc:
                pending.result(timeout=2)
            assert exc.value.status == "cancelled"
            assert not service._sessions
            assert connection._process_closed
        finally:
            service.close()


def test_repeated_processes_leave_no_active_children() -> None:
    """Verify repeated processes leave no active children."""
    import multiprocessing

    before = {child.pid for child in multiprocessing.active_children()}
    for _ in range(10):
        connection = isolated().connect("alice")
        query = connection.execute("ok")
        next(query.batches)
        query.close()
        connection.close()
        assert connection._process_closed
        assert connection._send.closed and connection._receive.closed
    assert {child.pid for child in multiprocessing.active_children()} == before
