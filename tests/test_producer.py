# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""ResultProducer results: state carried in continuation tokens instead of server memory."""

from dataclasses import dataclass

import pyarrow as pa
import pytest
from test_service import context, open_session
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient

from grainlift import AdbcError, Connection, Limits, QueryResult, ResultProducer, Service, Worker
from grainlift.protocol import Grainlift, OpenConnectionRequest

SCHEMA = pa.schema([("n", pa.int64()), ("total", pa.int64())])


@dataclass
class RunningTotal(ResultProducer):
    """Emit ``per_batch`` numbers per call with a running total that spans batches.

    Attributes:
        stop: Exclusive upper bound.
        per_batch: Rows per emitted batch.
        next: Next number to emit.
        total: Sum of every number emitted so far.
    """

    stop: int
    per_batch: int = 2
    next: int = 0
    total: int = 0

    def produce(self) -> pa.RecordBatch | None:
        """Emit the next rows, or None when exhausted."""
        if self.next >= self.stop:
            return None
        numbers = list(range(self.next, min(self.next + self.per_batch, self.stop)))
        totals = []
        for number in numbers:
            self.total += number
            totals.append(self.total)
        self.next += len(numbers)
        return pa.RecordBatch.from_pydict({"n": numbers, "total": totals}, schema=SCHEMA)


@dataclass
class Padded(ResultProducer):
    """Carry an oversized state field.

    Attributes:
        padding: Bytes that inflate the serialized state.
    """

    padding: bytes = b""

    def produce(self) -> pa.RecordBatch | None:
        """Never produce rows."""
        return None


class ProducerConnection(Connection):
    """Answer every query with a running total whose size is the SQL text's integer value."""

    def execute(self, sql: str) -> QueryResult:
        """Return a producer-backed result."""
        if sql == "padded":
            return QueryResult.from_producer(SCHEMA, Padded(b"x" * 1024))
        return QueryResult.from_producer(SCHEMA, RunningTotal(int(sql)))


class ProducerWorker(Worker):
    """Open producer connections."""

    def connect(self, principal: str) -> Connection:
        """Open a connection bound to the authenticated principal."""
        return ProducerConnection()


def execute(service: Service, sql: str) -> tuple[str, str, bytes]:
    """Open a session, execute SQL, and return the handles and initial token state."""
    sid, ctx = open_session(service)
    stmt = service.new_statement(sid, ctx).statement_id
    service.set_sql_query(sid, stmt, sql, ctx)
    rid = service.execute(sid, stmt, ctx).result_id
    state = service.read_result(sid, rid, 0, ctx).state.producer
    assert state is not None
    return sid, rid, state


def test_http_resumes_producer_from_continuation_tokens() -> None:
    """Batches arrive through tokens; the service retains neither iterator progress nor batches."""
    with Service(ProducerWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            sid = rpc.open_connection(
                request=OpenConnectionRequest(target="default", database_options=[], connection_options=[])
            ).session_id
            stmt = rpc.new_statement(session_id=sid).statement_id
            rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="5")
            rid = rpc.execute(session_id=sid, statement_id=stmt).result_id
            with rpc.read_result(session_id=sid, result_id=rid, sequence=0) as stream:
                batches = [item.batch.to_pydict() for item in stream]
            assert batches == [
                {"n": [0, 1], "total": [0, 1]},
                {"n": [2, 3], "total": [3, 6]},
                {"n": [4], "total": [10]},
            ]
            result = service._sessions[sid].results[rid]
            assert result.last is None and result.finished and result.sequence == 3
            rpc.close_connection(session_id=sid)


def test_replay_reproduces_previous_batch_from_token_state() -> None:
    """Retrying the previous sequence recomputes its batch; older sequences fail."""
    with Service(ProducerWorker()) as service:
        sid, rid, initial = execute(service, "5")
        ctx = context(service)
        first, second_state = service.next_produced_batch(sid, rid, 0, initial, ctx)
        replayed, replayed_state = service.next_produced_batch(sid, rid, 0, initial, ctx)
        assert first is not None and replayed is not None and first.equals(replayed)
        assert replayed_state == second_state
        second, third_state = service.next_produced_batch(sid, rid, 1, second_state, ctx)
        assert second is not None and second.column("total").to_pylist() == [3, 6]
        with pytest.raises(AdbcError, match="Invalid result sequence"):
            service.next_produced_batch(sid, rid, 0, initial, ctx)
        with pytest.raises(AdbcError, match="continuation tokens"):
            service.read_result(sid, rid, 1, ctx)


def test_in_memory_iteration_matches_token_resumption() -> None:
    """Isolation and TCP read producers through the ordinary batches iterator."""
    result = QueryResult.from_producer(SCHEMA, RunningTotal(5))
    assert [batch.column("total").to_pylist() for batch in result.batches] == [[0, 1], [3, 6], [10]]


def test_producer_state_limit() -> None:
    """Oversized producer state is rejected before any token is issued."""
    with Service(ProducerWorker(), limits=Limits(producer_state_bytes=512)) as service:
        sid, ctx = open_session(service)
        stmt = service.new_statement(sid, ctx).statement_id
        service.set_sql_query(sid, stmt, "padded", ctx)
        with pytest.raises(AdbcError, match="producer state exceeds"):
            service.execute(sid, stmt, ctx)


def test_unknown_producer_state_is_invalid_data() -> None:
    """Only registered producer types decode."""
    with pytest.raises(AdbcError, match="Unknown result producer"):
        ResultProducer.decode(b"os:system\0")
    with pytest.raises(AdbcError, match="Unknown result producer"):
        ResultProducer.decode(b"no separator")


def test_rpc_error_for_unknown_result() -> None:
    """A producer fetch for a released result reports not_found."""
    with Service(ProducerWorker()) as service:
        sid, rid, initial = execute(service, "1")
        ctx = context(service)
        service.close_result(sid, rid, ctx)
        with pytest.raises(AdbcError, match="unavailable"):
            service.next_produced_batch(sid, rid, 0, initial, ctx)
