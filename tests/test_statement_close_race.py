# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Cancellation cannot enter backend statements after their teardown has begun."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_service import open_session, value

from grainlift import AdbcError, Connection, Service, Statement, Worker


class ClosingStatement(Statement):
    """Expose the backend-close interval without relying on scheduling delays."""

    def __init__(self) -> None:
        """Initialize synchronization and cancellation observations."""
        self.closing = threading.Event()
        self.release = threading.Event()
        self.cancel_calls = 0
        self.closed = False

    def close(self) -> None:
        """Block inside backend teardown until the competing request completes."""
        self.closing.set()
        assert self.release.wait(timeout=5)
        self.closed = True

    def cancel(self) -> None:
        """Record entry to an operation unsafe once backend teardown has started."""
        self.cancel_calls += 1


class ClosingConnection(Connection):
    """Return the one observed statement for this lifecycle test."""

    def __init__(self, statement: ClosingStatement) -> None:
        """Retain the test-owned statement."""
        self.statement = statement

    def new_statement(self) -> Statement:
        """Return the backend fixture without creating additional resources."""
        return self.statement


class ClosingWorker(Worker):
    """Create an independent fixture connection with a blocking close callback."""

    def __init__(self, statement: ClosingStatement) -> None:
        """Retain the statement observed by the test thread."""
        self.statement = statement

    def connect(self, principal: str) -> Connection:
        """Open a connection owned by the authenticated test principal."""
        return ClosingConnection(self.statement)


def test_cancel_cannot_enter_statement_during_backend_close() -> None:
    """Make a closing handle unavailable before calling any backend cleanup."""
    backend = ClosingStatement()
    with Service(ClosingWorker(backend)) as service, ThreadPoolExecutor(1) as pool:
        session_id, context = open_session(service)
        statement_id = value(service.new_statement(session_id, context), "statement_id")
        closing = pool.submit(service.close_statement, session_id, statement_id, context)
        try:
            assert backend.closing.wait(timeout=2)
            with pytest.raises(AdbcError) as error:
                service.cancel_statement(session_id, statement_id, context)
            assert error.value.status == "not_found"
            assert backend.cancel_calls == 0
        finally:
            backend.release.set()
        closing.result(timeout=2)
        assert backend.closed
        assert statement_id not in service._sessions[session_id].statements
