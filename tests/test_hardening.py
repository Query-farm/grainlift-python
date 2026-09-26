# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Validation, independent sessions, quotas, and cancellation without sockets."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from test_service import TestConnection, TestWorker, context, execute, open_session
from vgi_rpc import CallContext

from grainlift import AdbcError, Limits, QueryResult, Service
from grainlift.protocol import SetConnectionOptionRequest
from grainlift.server import _Session


@pytest.mark.parametrize("field", ["sessions", "statements_per_session", "batch_bytes", "request_bytes", "sql_bytes"])
@pytest.mark.parametrize("bad", [0, -1, True, 1.1, float("nan"), float("inf"), "2", None])
def test_integer_limits(field: str, bad: Any) -> None:
    """Verify integer limits."""
    with pytest.raises(ValueError, match=field):
        Limits(**{field: bad})


@pytest.mark.parametrize("field", ["idle_seconds", "lock_timeout_seconds", "shutdown_seconds"])
@pytest.mark.parametrize("bad", [0, -1, True, float("nan"), float("inf"), "2", None])
def test_duration_limits(field: str, bad: Any) -> None:
    """Verify duration limits."""
    with pytest.raises(ValueError, match=field):
        Limits(**{field: bad})


@pytest.mark.parametrize("value", [None, "private malformed input", 1])
def test_malformed_option_has_client_error(value: Any) -> None:
    """Reject an incorrectly typed option without echoing private input."""
    with Service(TestWorker()) as service:
        sid, ctx = open_session(service)
        with pytest.raises(AdbcError) as exc:
            service.set_connection_option(
                SetConnectionOptionRequest(session_id=sid, key="adbc.connection.autocommit", value=value), ctx
            )
        assert exc.value.status == "invalid_arguments"
        assert "private malformed" not in str(exc.value)


class BlockingWorker(TestWorker):
    """Coordinate blocked callbacks with deterministic events."""

    def __init__(self, *, cancellable: bool = False, block_open: bool = False) -> None:
        """Initialize configured state and resource ownership."""
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancellable = cancellable
        self.block_open = block_open

    def connect(self, principal: str) -> TestConnection:
        """Open a connection bound to the authenticated principal."""
        if self.block_open:
            self.started.set()
            assert self.release.wait(5)
        worker = self

        class BlockingConnection(TestConnection):
            """Block execution until released or cancelled."""

            def execute(self, sql: str) -> QueryResult:
                """Execute SQL and return its schema and lazy batch iterator."""
                if sql == "block":
                    worker.started.set()
                    assert worker.release.wait(5)
                    if worker.cancellable:
                        raise AdbcError("Cancelled test operation", "cancelled")
                return super().execute(sql)

            def cancel(self) -> None:
                """Request cancellation of the active backend operation."""
                if not worker.cancellable:
                    return super().cancel()
                worker.release.set()

        connection = BlockingConnection(self.batches)
        self.connections.append(connection)
        return connection


def blocking_statement(service: Service, sid: str, ctx: CallContext) -> str:
    """Create a statement that waits for the release event."""
    statement = service.new_statement(sid, ctx).statement_id
    service.set_sql_query(sid, statement, "block", ctx)
    return statement


def test_independent_session_progress_and_reaping() -> None:
    """Verify independent session progress and reaping."""
    worker = BlockingWorker()
    with Service(worker, limits=Limits(idle_seconds=0.1)) as service, ThreadPoolExecutor(2) as pool:
        sid, ctx = open_session(service)
        statement = blocking_statement(service, sid, ctx)
        pending = pool.submit(service.execute, sid, statement, ctx)
        try:
            assert worker.started.wait(1)
            other, other_ctx = open_session(service, "bob")
            execute(service, other, other_ctx)
            assert not pending.done()
            deadline = time.monotonic() + 2
            while not worker.connections[1].closed and time.monotonic() < deadline:
                time.sleep(0.01)
            assert worker.connections[1].closed
            assert not worker.connections[0].closed
        finally:
            worker.release.set()
            pending.result(timeout=2)


def test_opening_reserves_quota() -> None:
    """Verify opening reserves quota."""
    worker = BlockingWorker(block_open=True)
    with Service(worker, limits=Limits(sessions=1)) as service, ThreadPoolExecutor(1) as pool:
        pending = pool.submit(open_session, service)
        try:
            assert worker.started.wait(1)
            with pytest.raises(AdbcError, match="Session limit"):
                open_session(service)
        finally:
            worker.release.set()
            pending.result(timeout=2)


def test_busy_session_has_bounded_lock_wait() -> None:
    """Verify busy session has bounded lock wait."""
    worker = BlockingWorker()
    with (
        Service(worker, limits=Limits(lock_timeout_seconds=0.02)) as service,
        ThreadPoolExecutor(1) as pool,
    ):
        sid, ctx = open_session(service)
        statement = blocking_statement(service, sid, ctx)
        pending = pool.submit(service.execute, sid, statement, ctx)
        try:
            assert worker.started.wait(1)
            with pytest.raises(AdbcError) as exc:
                service.new_statement(sid, ctx)
            assert exc.value.status == "timeout"
        finally:
            worker.release.set()
            pending.result(timeout=2)


@pytest.mark.parametrize("operation", ["connection", "statement"])
def test_cancellation_reaches_active_callback(operation: str) -> None:
    """Verify cancellation reaches active callback."""
    worker = BlockingWorker(cancellable=True)
    with Service(worker) as service, ThreadPoolExecutor(1) as pool:
        sid, ctx = open_session(service)
        statement = blocking_statement(service, sid, ctx)
        pending = pool.submit(service.execute, sid, statement, ctx)
        try:
            assert worker.started.wait(1)
            with pytest.raises(AdbcError, match="unavailable"):
                service.cancel_connection(sid, context(service, "bob"))
            assert not worker.release.is_set()
            if operation == "connection":
                service.cancel_connection(sid, ctx)
            else:
                service.cancel_statement(sid, statement, ctx)
            with pytest.raises(AdbcError) as exc:
                pending.result(timeout=2)
            assert exc.value.status == "cancelled"
        finally:
            worker.release.set()


def test_shutdown_during_open_releases_late_connection() -> None:
    """Verify shutdown during open releases late connection."""
    worker = BlockingWorker(block_open=True)
    service = Service(worker, limits=Limits(shutdown_seconds=0.05))
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(open_session, service)
        try:
            assert worker.started.wait(1)
            with pytest.raises(AdbcError, match="Shutdown"):
                service.close()
        finally:
            worker.release.set()
        with pytest.raises(AdbcError, match="closed"):
            pending.result(timeout=2)
    assert worker.connections[0].closed
    assert service._opening == 0
    assert not service._sessions
    service.close()


def test_shutdown_busy_inprocess_reports_incomplete_then_cleans_up() -> None:
    """Verify shutdown busy inprocess reports incomplete then cleans up."""
    worker = BlockingWorker()
    service = Service(worker, limits=Limits(shutdown_seconds=0.02))
    with ThreadPoolExecutor(1) as pool:
        sid, ctx = open_session(service)
        statement = blocking_statement(service, sid, ctx)
        pending = pool.submit(service.execute, sid, statement, ctx)
        try:
            assert worker.started.wait(1)
            with pytest.raises(AdbcError, match="Shutdown"):
                service.close()
        finally:
            worker.release.set()
        pending.result(timeout=2)
    assert worker.connections[0].closed
    assert not service._sessions
    service.close()


def test_cancellation_rechecks_session_after_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a cancellation that resolved its handle before concurrent connection closure."""
    worker = BlockingWorker(cancellable=True)
    looked_up = threading.Event()
    resume = threading.Event()
    with Service(worker) as service, ThreadPoolExecutor(1, thread_name_prefix="cancel-race") as pool:
        sid, ctx = open_session(service)
        original = service._session

        def lookup(session_id: str, call: CallContext) -> _Session:
            session = original(session_id, call)
            if threading.current_thread().name.startswith("cancel-race") and not looked_up.is_set():
                looked_up.set()
                assert resume.wait(5)
            return session

        monkeypatch.setattr(service, "_session", lookup)
        pending = pool.submit(service.cancel_connection, sid, ctx)
        try:
            assert looked_up.wait(2)
            service.close_connection(sid, ctx)
        finally:
            resume.set()
        with pytest.raises(AdbcError) as error:
            pending.result(timeout=2)
        assert error.value.status == "not_found"
        assert not worker.release.is_set()
