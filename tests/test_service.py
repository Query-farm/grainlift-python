# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Session ownership, lazy results, quotas, and lifecycle regressions."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import cast

import pyarrow as pa
import pytest
from vgi_rpc import AuthContext, CallContext

from grainlift import AdbcError, Connection, Limits, QueryResult, Service, Worker
from grainlift.protocol import ResultCursor

SCHEMA = pa.schema([("n", pa.int64())])


class Reader:
    """Track lazy reads and cursor cleanup."""

    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        """Initialize configured state and resource ownership."""
        self.batches = iter(batches)
        self.reads = 0
        self.closed = False

    def __next__(self) -> pa.RecordBatch:
        """Read the next batch or signal exhaustion."""
        self.reads += 1
        return next(self.batches)

    def __iter__(self) -> Reader:
        """Return this lazy batch iterator."""
        return self

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        self.closed = True


class TestConnection(Connection):
    """Provide deterministic batches and failure injection."""

    __test__ = False

    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        """Initialize configured state and resource ownership."""
        self.batches = batches
        self.readers: list[Reader] = []
        self.closed = False

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return its schema and lazy batch iterator."""
        if sql == "crash":
            raise RuntimeError("SECRET raw backend error")
        reader = Reader(self.batches)
        self.readers.append(reader)
        return QueryResult(SCHEMA, reader)

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        self.closed = True


class TestWorker(Worker):
    """Create instrumented test connections."""

    __test__ = False

    def __init__(self, batches: list[pa.RecordBatch] | None = None) -> None:
        """Initialize configured state and resource ownership."""
        self.batches = (
            batches
            if batches is not None
            else [
                pa.record_batch([[1, 2]], schema=SCHEMA),
                pa.record_batch([[3]], schema=SCHEMA),
            ]
        )
        self.connections: list[TestConnection] = []

    def connect(self, principal: str) -> TestConnection:
        """Open a connection bound to the authenticated principal."""
        connection = TestConnection(self.batches)
        self.connections.append(connection)
        return connection


def context(service: Service, principal: str = "alice") -> CallContext:
    """Create a call context with the selected authenticated principal."""
    return CallContext(
        AuthContext(domain="test", authenticated=bool(principal), principal=principal),
        lambda message: None,
        implementation=service,
    )


def value(batch: pa.RecordBatch, name: str) -> str:
    """Extract a string handle from a single-row protocol response."""
    return cast(str, batch.column(name)[0].as_py())


def open_session(service: Service, principal: str = "alice") -> tuple[str, CallContext]:
    """Open a test session and return its authenticated context."""
    ctx = context(service, principal)
    sid = value(service.open_connection("default", "[]", "[]", ctx), "session_id")
    return sid, ctx


def execute(service: Service, sid: str, ctx: CallContext) -> tuple[str, str]:
    """Execute SQL and return its schema and lazy batch iterator."""
    stmt = value(service.new_statement(sid, ctx), "statement_id")
    service.set_sql_query(sid, stmt, "query", ctx)
    rid = value(service.execute(sid, stmt, ctx), "result_id")
    return stmt, rid


@pytest.fixture
def service() -> Iterator[Service]:
    """Yield a service with deterministic test batches."""
    with Service(TestWorker()) as service:
        yield service


def test_lazy_fetch_replay_and_eof(service: Service) -> None:
    """Verify lazy fetch replay and eof."""
    sid, ctx = open_session(service)
    _, rid = execute(service, sid, ctx)
    reader = cast(TestWorker, service.worker).connections[0].readers[0]
    assert reader.reads == 0
    first = service.next_batch(sid, rid, 0, ctx)
    assert reader.reads == 1
    assert service.next_batch(sid, rid, 0, ctx) is first
    assert reader.reads == 1
    with pytest.raises(AdbcError, match="sequence"):
        service.next_batch(sid, rid, 2, ctx)
    second = service.next_batch(sid, rid, 1, ctx)
    assert second is not None and second.num_rows == 1
    with pytest.raises(AdbcError, match="sequence"):
        service.next_batch(sid, rid, 0, ctx)
    assert service.next_batch(sid, rid, 2, ctx) is None
    assert reader.closed
    assert service.next_batch(sid, rid, 2, ctx) is None


@pytest.mark.parametrize("operation", ["result", "statement", "connection", "reuse", "cancel", "shutdown"])
def test_early_cleanup(service: Service, operation: str) -> None:
    """Verify early cleanup."""
    sid, ctx = open_session(service)
    stmt, rid = execute(service, sid, ctx)
    reader = cast(TestWorker, service.worker).connections[0].readers[0]
    service.next_batch(sid, rid, 0, ctx)
    if operation == "result":
        service.close_result(sid, rid, ctx)
    elif operation == "statement":
        service.close_statement(sid, stmt, ctx)
    elif operation == "connection":
        service.close_connection(sid, ctx)
    elif operation == "reuse":
        service.execute(sid, stmt, ctx)
    elif operation == "cancel":
        ResultCursor(sid, rid, 1).on_cancel(ctx)
    else:
        service.close()
    assert reader.closed


def test_principal_and_child_handle_isolation(service: Service) -> None:
    """Verify principal and child handle isolation."""
    sid, alice = open_session(service)
    stmt, rid = execute(service, sid, alice)
    other_sid, bob = open_session(service, "bob")
    actions: list[Callable[[], object]] = [
        lambda: service.close_connection(sid, bob),
        lambda: service.next_batch(sid, rid, 0, bob),
        lambda: service.close_statement(sid, stmt, bob),
        lambda: service.next_batch(other_sid, rid, 0, bob),
        lambda: service.close_statement(other_sid, stmt, bob),
    ]
    for action in actions:
        with pytest.raises(AdbcError, match="unavailable"):
            action()
    first = service.next_batch(sid, rid, 0, alice)
    assert first is not None and first.num_rows == 2
    with pytest.raises(AdbcError, match="Authentication"):
        open_session(service, "")


@pytest.mark.parametrize("count", [1, 2, 3])
def test_session_limit(count: int) -> None:
    """Verify session limit."""
    with Service(TestWorker(), limits=Limits(sessions=2)) as service:
        for _ in range(min(count, 2)):
            open_session(service)
        if count == 3:
            with pytest.raises(AdbcError, match="Session limit"):
                open_session(service)


@pytest.mark.parametrize("count", [1, 2, 3])
def test_statement_limit(count: int) -> None:
    """Verify statement limit."""
    with Service(TestWorker(), limits=Limits(statements_per_session=2)) as service:
        sid, ctx = open_session(service)
        for _ in range(min(count, 2)):
            service.new_statement(sid, ctx)
        if count == 3:
            with pytest.raises(AdbcError, match="Statement limit"):
                service.new_statement(sid, ctx)


@pytest.mark.parametrize("size", [15, 16, 17])
def test_batch_limit(size: int) -> None:
    # Keep the schema limit independent of this small batch boundary.
    """Verify batch limit."""
    data_schema = pa.schema([("x", pa.binary(1))])
    data = pa.record_batch([[b"x"] * size], schema=data_schema)
    with Service(TestWorker([data])) as service:
        sid, ctx = open_session(service)
        _, rid = execute(service, sid, ctx)
        result = service._sessions[sid].results[rid]
        result.query.schema = data_schema
        service.limits = Limits(batch_bytes=16)
        if size > 16:
            with pytest.raises(AdbcError, match="batch exceeds"):
                service.next_batch(sid, rid, 0, ctx)
            assert result.closed
        else:
            first = service.next_batch(sid, rid, 0, ctx)
            assert first is not None and first.num_rows == size


@pytest.mark.parametrize("size", [15, 16, 17])
def test_sql_limit(size: int) -> None:
    """Verify sql limit."""
    with Service(TestWorker(), limits=Limits(sql_bytes=16)) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        if size > 16:
            with pytest.raises(AdbcError, match="SQL exceeds"):
                service.set_sql_query(sid, stmt, "x" * size, ctx)
        else:
            service.set_sql_query(sid, stmt, "x" * size, ctx)


def test_idle_cleanup_without_further_requests() -> None:
    """Verify idle cleanup without further requests."""
    worker = TestWorker()
    with Service(worker, limits=Limits(idle_seconds=0.05)) as service:
        sid, ctx = open_session(service)
        execute(service, sid, ctx)
        deadline = time.monotonic() + 2
        while not worker.connections[0].closed and time.monotonic() < deadline:
            time.sleep(0.01)
        assert worker.connections[0].closed
        assert worker.connections[0].readers[0].closed
        assert not service._sessions


def test_schema_mismatch_closes_reader() -> None:
    """Verify schema mismatch closes reader."""
    with Service(TestWorker([pa.record_batch([["bad"]], names=["x"])])) as service:
        sid, ctx = open_session(service)
        _, rid = execute(service, sid, ctx)
        with pytest.raises(AdbcError, match="schema changed"):
            service.next_batch(sid, rid, 0, ctx)
        assert cast(TestWorker, service.worker).connections[0].readers[0].closed


def test_unexpected_error_sanitized(service: Service) -> None:
    """Verify unexpected error sanitized."""
    sid, ctx = open_session(service)
    stmt = value(service.new_statement(sid, ctx), "statement_id")
    service.set_sql_query(sid, stmt, "crash", ctx)
    with pytest.raises(AdbcError) as exc:
        service.execute(sid, stmt, ctx)
    assert exc.value.status == "internal"
    assert "SECRET" not in str(exc.value)
    assert exc.value.__suppress_context__


def test_reject_options_and_unsupported_methods(service: Service) -> None:
    """Verify reject options and unsupported methods."""
    ctx = context(service)
    with pytest.raises(AdbcError, match="Caller-supplied"):
        service.open_connection("default", '[{"key":"uri","type":"string","value":"secret"}]', "[]", ctx)
    sid, ctx = open_session(service)
    with pytest.raises(AdbcError) as exc:
        service.commit(session_id=sid, ctx=ctx)
    assert exc.value.status == "not_implemented"


def test_error_details() -> None:
    """Verify error details."""
    error = AdbcError("safe", "invalid_data", sqlstate="22000", vendor_code=42, details={"x": b"\x00\xff"})
    wire = json.loads(str(error))
    assert wire["sqlstate"] == [50, 50, 48, 48, 48]
    assert wire["vendor_code"] == 42
    assert wire["details"] == [["x", "AP8="]]
