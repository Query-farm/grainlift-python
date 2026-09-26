# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Bind upload ownership and reader lifetime across service state transitions."""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pytest
from test_service import context, open_session, value
from vgi_rpc import CallContext

from grainlift import AdbcError, Connection, QueryResult, Service, Statement, Worker
from grainlift.protocol import schema_ipc

SCHEMA = pa.schema([("value", pa.int64())])
DATA = pa.record_batch([[1, 2, 3]], schema=SCHEMA)
EMPTY = pa.record_batch([[]], schema=SCHEMA)


class RetainingStatement(Statement):
    """Retain a bound reader until execution, replacement or backend cleanup."""

    def __init__(self) -> None:
        """Initialize binding and cleanup observations."""
        self.reader: pa.RecordBatchReader | None = None
        self.reject = False
        self.close_rows = 0

    def set_sql_query(self, sql: str) -> None:
        """Drop the old backend binding on query replacement."""
        self.reader = None

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Retain input, or reject it before replacing the old reader."""
        if self.reject:
            raise AdbcError("Rejected binding", "invalid_data")
        self.reader = reader

    def execute(self) -> QueryResult:
        """Read parameters only when execution consumes them."""
        assert self.reader is not None
        return QueryResult(SCHEMA, iter(self.reader))

    def close(self) -> None:
        """Consume any remaining bound input before toolkit cleanup closes its file."""
        if self.reader is not None:
            self.close_rows = sum(batch.num_rows for batch in self.reader)
            self.reader = None


class RetainingConnection(Connection):
    """Create independent resource-owning statements."""

    def new_statement(self) -> RetainingStatement:
        """Construct an independent statement with deferred parameter consumption."""
        return RetainingStatement()


class RetainingWorker(Worker):
    """Construct connections for binding lifecycle tests."""

    def connect(self, principal: str) -> RetainingConnection:
        """Return a connection owned by the supplied principal."""
        return RetainingConnection()


@dataclass
class Case:
    """One independent ADBC session and statement under test."""

    service: Service
    sid: str
    stmt: str
    ctx: CallContext

    def upload(self, *, finish: bool) -> str:
        """Start and populate an upload, optionally committing its binding."""
        upload = self.service.bind_stream(self.sid, self.stmt, schema_ipc(SCHEMA), self.ctx).state.upload_id
        self.service.push_binding(self.sid, self.stmt, upload, 0, DATA, False, self.ctx)
        if finish:
            self.service.push_binding(self.sid, self.stmt, upload, 1, EMPTY, True, self.ctx)
        return upload

    def rows(self) -> list[int]:
        """Consume the configured statement result through the service pull path."""
        rid = value(self.service.execute(self.sid, self.stmt, self.ctx), "result_id")
        batch = self.service.next_batch(self.sid, rid, 0, self.ctx)
        assert batch is not None
        return cast(list[int], batch.column(0).to_pylist())


@pytest.fixture
def case() -> Iterator[Case]:
    """Close every test's statement, reader, spool, connection and reaper."""
    with Service(RetainingWorker()) as service:
        sid, ctx = open_session(service)
        yield Case(service, sid, value(service.new_statement(sid, ctx), "statement_id"), ctx)


def test_cancelled_rebind_preserves_prior_reader(case: Case) -> None:
    """Cancellation of new input does not close an already bound backend reader."""
    original = case.upload(finish=True)
    pending = case.upload(finish=False)
    case.service.cancel_binding(case.sid, case.stmt, pending, case.ctx)
    case.service.cancel_binding(case.sid, case.stmt, original, case.ctx)
    assert case.rows() == [1, 2, 3]


def test_rejected_rebind_preserves_prior_reader(case: Case) -> None:
    """A failed backend bind does not invalidate its previously accepted input."""
    case.upload(finish=True)
    wrapper = case.service._sessions[case.sid].statements[case.stmt]
    assert isinstance(wrapper.backend, RetainingStatement)
    wrapper.backend.reject = True
    pending = case.upload(finish=False)
    upload = wrapper.upload
    assert upload is not None
    with pytest.raises(AdbcError, match="Rejected binding"):
        case.service.push_binding(case.sid, case.stmt, pending, 1, EMPTY, True, case.ctx)
    assert upload.closed
    assert case.rows() == [1, 2, 3]


def test_successful_rebind_closes_only_replaced_reader(case: Case) -> None:
    """Replacement keeps at most the active binding and one unfinished upload."""
    case.upload(finish=True)
    wrapper = case.service._sessions[case.sid].statements[case.stmt]
    previous = wrapper.binding
    assert previous is not None
    case.upload(finish=True)
    assert previous.closed
    assert wrapper.binding is not None and not wrapper.binding.closed
    assert wrapper.upload is None
    assert case.rows() == [1, 2, 3]


@pytest.mark.parametrize("method", ["execute", "execute_update", "execute_schema", "execute_partitions", "prepare"])
def test_incomplete_input_cannot_execute(case: Case, method: str) -> None:
    """Reject execution until the producer explicitly finishes uploading parameters."""
    case.upload(finish=False)
    with pytest.raises(AdbcError, match="has not finished"):
        getattr(case.service, method)(case.sid, case.stmt, case.ctx)


def test_foreign_principal_cannot_advance_or_cancel_upload(case: Case) -> None:
    """Session ownership applies to all upload continuations and cancellation."""
    pending = case.upload(finish=False)
    foreign = context(case.service, "bob")
    with pytest.raises(AdbcError, match="unavailable"):
        case.service.push_binding(case.sid, case.stmt, pending, 1, EMPTY, True, foreign)
    with pytest.raises(AdbcError, match="unavailable"):
        case.service.cancel_binding(case.sid, case.stmt, pending, foreign)
    case.service.push_binding(case.sid, case.stmt, pending, 1, EMPTY, True, case.ctx)
    assert case.rows() == [1, 2, 3]


def test_upload_handle_cannot_move_between_statements(case: Case) -> None:
    """An upload is bound to its statement in addition to its authenticated session."""
    pending = case.upload(finish=False)
    other = value(case.service.new_statement(case.sid, case.ctx), "statement_id")
    with pytest.raises(AdbcError, match="unavailable"):
        case.service.push_binding(case.sid, other, pending, 1, EMPTY, True, case.ctx)
    case.service.push_binding(case.sid, case.stmt, pending, 1, EMPTY, True, case.ctx)


def test_invalid_upload_turn_closes_pending_file(case: Case) -> None:
    """Protocol errors release anonymous staging without touching an earlier binding."""
    case.upload(finish=True)
    pending = case.upload(finish=False)
    upload = case.service._sessions[case.sid].statements[case.stmt].upload
    assert upload is not None
    with pytest.raises(AdbcError, match="sequence"):
        case.service.push_binding(case.sid, case.stmt, pending, 8, EMPTY, True, case.ctx)
    assert upload._file.file.closed
    assert case.rows() == [1, 2, 3]


def test_idle_reaping_closes_unfinished_input_only(case: Case) -> None:
    """Abandoned HTTP upload requests expire even if other session activity continues."""
    case.upload(finish=True)
    case.upload(finish=False)
    wrapper = case.service._sessions[case.sid].statements[case.stmt]
    upload = wrapper.upload
    assert upload is not None
    upload.touched -= case.service.limits.idle_seconds + 1
    case.service._reap()
    assert upload.closed
    assert wrapper.upload is None
    assert case.rows() == [1, 2, 3]


def test_statement_cleanup_keeps_reader_live_through_backend_close(case: Case) -> None:
    """Let the backend release owned input before the toolkit closes its storage."""
    case.upload(finish=True)
    case.upload(finish=False)
    wrapper = case.service._sessions[case.sid].statements[case.stmt]
    active, pending = wrapper.binding, wrapper.upload
    assert active is not None and pending is not None
    assert isinstance(wrapper.backend, RetainingStatement)
    case.service.close_statement(case.sid, case.stmt, case.ctx)
    assert wrapper.backend.close_rows == 3
    assert active.closed and pending.closed
