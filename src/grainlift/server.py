# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Bounded, process-local Grainlift sessions and authenticated WSGI application."""

from __future__ import annotations

import hashlib
import hmac
import inspect
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from functools import wraps
from typing import Concatenate, ParamSpec, TypeVar

import falcon
import pyarrow as pa
from vgi_rpc import AuthContext, CallContext, RpcServer, Stream
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.utils import ArrowSerializableDataclass, serialize_record_batch_bytes

from . import protocol as p
from .api import AdbcError, Connection, Limits, OptionValue, QueryResult, ResultProducer, Statement, Worker
from .binding import BindUpload, decode_batch, decode_schema
from .credentials import TokenStore
from .options import WireOptionValue, configured_options, option_mapping, validate_key
from .requests import Request
from .storage import ExternalStorageConfig
from .telemetry import PrivateApplication
from .tokens import PartitionClaims, seal_partition, unseal_partition

P = ParamSpec("P")
T = TypeVar("T")


def guarded[**P, T](method: Callable[Concatenate[Service, P], T]) -> Callable[Concatenate[Service, P], T]:
    """Serialize only one session; never hold the registry lock across a worker callback."""
    signature = inspect.signature(method)

    @wraps(method)
    def call(self: Service, /, *args: P.args, **kwargs: P.kwargs) -> T:
        bound = signature.bind(self, *args, **kwargs)
        request = bound.arguments.get("request")
        if request is not None:
            self._request(request)
        sid = getattr(request, "session_id", bound.arguments.get("session_id"))
        session = None
        with self._lock:
            if self._closed:
                raise AdbcError("Service is closed", "invalid_state")
        if sid is not None:
            session = self._session(sid, bound.arguments["ctx"])
            if not session.lock.acquire(timeout=self.limits.lock_timeout_seconds):
                raise AdbcError("Session is busy", "timeout")
        try:
            if session is not None:
                assert sid is not None
                self._session(sid, bound.arguments["ctx"])
                with session.cancel_lock:
                    statement_id = getattr(request, "statement_id", bound.arguments.get("statement_id"))
                    if statement_id is None and "result_id" in bound.arguments:
                        statement_id = next(
                            (
                                key
                                for key, stmt in session.statements.items()
                                if stmt.result_id == bound.arguments["result_id"]
                            ),
                            None,
                        )
                    session.active_statement = statement_id
            return method(self, *args, **kwargs)
        except AdbcError:
            raise
        except Exception:  # noqa: BLE001 -- sanitize untrusted worker exceptions
            raise AdbcError("Worker operation failed", "internal") from None
        finally:
            if session is not None:
                with session.cancel_lock:
                    session.active_statement = None
                session.touched = time.monotonic()
                try:
                    if self._closed:
                        self._close_session(sid, session)
                finally:
                    session.lock.release()

    return call


@dataclass
class _Result:
    query: QueryResult
    producer: bytes | None = None
    sequence: int = 0
    last: pa.RecordBatch | None = None
    finished: bool = False
    closed: bool = False
    touched: float = field(default_factory=time.monotonic)

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        if not self.closed:
            self.closed = True
            with suppress(Exception):  # Continue cleanup without logging worker secrets.
                self.query.close()
        self.last = None


@dataclass
class _Statement:
    backend: Statement
    result_id: str | None = None
    binding: BindUpload | None = None
    binding_id: str | None = None
    upload: BindUpload | None = None
    upload_id: str | None = None

    def close(self) -> None:
        """Release the backend before its retained parameter reader."""
        with suppress(Exception):
            self.backend.close()
        if self.upload is not None:
            with suppress(Exception):
                self.upload.close()
            self.upload = None
        self.upload_id = None
        if self.binding is not None:
            with suppress(Exception):
                self.binding.close()
            self.binding = None
        self.binding_id = None


@dataclass
class _Session:
    principal: str
    connection: Connection
    touched: float = field(default_factory=time.monotonic)
    statements: dict[str, _Statement] = field(default_factory=dict)
    results: dict[str, _Result] = field(default_factory=dict)
    lock: threading.RLock = field(default_factory=threading.RLock)
    cancel_lock: threading.Lock = field(default_factory=threading.Lock)
    active_statement: str | None = None
    closing: bool = False
    closed: bool = False

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        if self.closed:
            return
        with self.cancel_lock:
            self.closing = True
        for result in self.results.values():
            result.close()
        self.results.clear()
        for statement in self.statements.values():
            statement.close()
        self.statements.clear()
        with suppress(Exception):  # Continue cleanup without logging worker secrets.
            self.connection.close()
        self.closed = True


class Service:
    """Own handles for one worker. Use as a context manager or call close()."""

    def __init__(
        self,
        worker: Worker,
        *,
        limits: Limits | None = None,
        database_options: Mapping[str, OptionValue] | None = None,
        connection_options: Mapping[str, OptionValue] | None = None,
    ) -> None:
        """Start the handle registry and idle-resource reaper.

        Args:
            worker: Factory for independent principal-bound connections.
            limits: Service quotas and lifecycle deadlines; defaults to Limits().
            database_options: Authoritative database settings callers may not override.
            connection_options: Authoritative connection settings callers may not override.
        """
        self.worker = worker
        self.limits = limits or Limits()
        # Upload cap of external storage (``app(external_storage=...)``), through
        # which a bind frame may arrive instead of in one request.
        self._external_upload_bytes = 0
        self._database_options = configured_options(database_options, self.limits.request_bytes)
        self._connection_options = configured_options(connection_options, self.limits.request_bytes)
        self._partition_key = secrets.token_bytes(32)
        self._sessions: dict[str, _Session] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._opening = 0
        self._stop = threading.Event()
        self._reaper = threading.Thread(target=self._reap_loop, daemon=True, name="grainlift-reaper")
        self._reaper.start()

    def __enter__(self) -> Service:
        """Return this service for managed cleanup."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close owned sessions when leaving the context."""
        self.close()

    def _reap_loop(self) -> None:
        while not self._stop.wait(min(1.0, self.limits.idle_seconds / 2)):
            self._reap()

    def _reap(self) -> None:
        now = time.monotonic()
        with self._lock:
            sessions = list(self._sessions.items())
        for sid, session in sessions:
            if not session.lock.acquire(blocking=False):
                continue
            try:
                if now - session.touched >= self.limits.idle_seconds:
                    self._close_session(sid, session)
                else:
                    for statement in session.statements.values():
                        if statement.upload is not None and now - statement.upload.touched >= self.limits.idle_seconds:
                            self._discard_upload(statement)
                    for rid, result in list(session.results.items()):
                        if now - result.touched >= self.limits.idle_seconds:
                            session.results.pop(rid).close()
            finally:
                session.lock.release()

    def _close_session(self, sid: str, session: _Session) -> None:
        # Keep the quota reservation until potentially blocking cleanup finishes.
        session.close()
        with self._lock:
            if self._sessions.get(sid) is session:
                del self._sessions[sid]

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        self._stop.set()
        with self._lock:
            self._closed = True
            sessions = list(self._sessions.items())
        deadline = time.monotonic() + self.limits.shutdown_seconds
        for sid, session in sessions:
            if not session.lock.acquire(blocking=False):
                with suppress(Exception):  # Continue shutdown without logging worker secrets.
                    session.connection.cancel()
                if not session.lock.acquire(timeout=max(0, deadline - time.monotonic())):
                    # The operation's finally block closes it when it returns.
                    continue
            try:
                self._close_session(sid, session)
            finally:
                session.lock.release()
        self._reaper.join(timeout=max(0, deadline - time.monotonic()))
        with self._lock:
            if self._sessions or self._opening:
                raise AdbcError("Shutdown is waiting for worker callbacks", "timeout")

    @staticmethod
    def _principal(ctx: CallContext) -> str:
        if not ctx.auth.authenticated or not ctx.auth.principal:
            raise AdbcError("Authentication required", "unauthenticated")
        return ctx.auth.principal

    def _session(self, sid: str, ctx: CallContext) -> _Session:
        principal = self._principal(ctx)
        with self._lock:
            session = self._sessions.get(sid)
            if session is None or session.principal != principal or session.closing:
                raise AdbcError("Session is unavailable", "not_found")
            session.touched = time.monotonic()
            return session

    def _statement(self, sid: str, statement_id: str, ctx: CallContext) -> tuple[_Session, _Statement]:
        session = self._session(sid, ctx)
        statement = session.statements.get(statement_id)
        if statement is None:
            raise AdbcError("Statement is unavailable", "not_found")
        return session, statement

    @staticmethod
    def _discard_result(session: _Session, statement: _Statement) -> None:
        result = session.results.pop(statement.result_id, None) if statement.result_id is not None else None
        statement.result_id = None
        if result is not None:
            result.close()

    @staticmethod
    def _discard_upload(statement: _Statement) -> None:
        if statement.upload is not None:
            statement.upload.close()
            statement.upload = None
        statement.upload_id = None

    @staticmethod
    def _discard_binding(statement: _Statement) -> None:
        Service._discard_upload(statement)
        if statement.binding is not None:
            statement.binding.close()
            statement.binding = None
        statement.binding_id = None

    @staticmethod
    def _require_complete_binding(statement: _Statement) -> None:
        if statement.upload is not None:
            raise AdbcError("Bind upload has not finished", "invalid_state")

    def _ensure_result_slot(self, session: _Session) -> None:
        if len(session.results) >= self.limits.results_per_session:
            raise AdbcError("Result limit reached", "invalid_state")

    def _encoded_schema(self, schema: pa.Schema) -> bytes:
        encoded = p.schema_ipc(schema)
        if len(encoded) > self.limits.batch_bytes:
            raise AdbcError("Schema exceeds configured limit", "invalid_data")
        return encoded

    def _response[R: ArrowSerializableDataclass](self, response: R) -> R:
        # Measure the same nested IPC bytes and outer result envelope that VGI
        # serializes, before registering any resources owned by the response.
        encoded = response.serialize_to_bytes()
        if len(encoded) > self.limits.batch_bytes:
            raise AdbcError("Response exceeds configured limit", "invalid_data")
        envelope = pa.RecordBatch.from_pydict({"result": [encoded]}, schema=p.UNARY_OUTPUT)
        if len(serialize_record_batch_bytes(envelope)) > self.limits.batch_bytes:
            raise AdbcError("Response exceeds configured limit", "invalid_data")
        return response

    def _schema_response(self, schema: pa.Schema) -> p.SchemaResponse:
        return self._response(p.SchemaResponse(schema_ipc=self._encoded_schema(schema)))

    def _request(self, request: Request) -> None:
        # Transport already enforces the HTTP body budget. Also enforce it for
        # direct service calls before handle lookup, locking, or backend mutation.
        if not isinstance(request, Request):
            raise AdbcError("Expected a typed request", "invalid_arguments")
        request.__post_init__()
        encoded = request.serialize_to_bytes()
        if len(encoded) > self.limits.request_bytes:
            raise AdbcError("Request exceeds configured limit", "invalid_arguments")
        envelope = pa.RecordBatch.from_pydict({"request": [encoded]}, schema=p.REQUEST_INPUT)
        if len(serialize_record_batch_bytes(envelope)) > self.limits.request_bytes:
            raise AdbcError("Request exceeds configured limit", "invalid_arguments")

    @staticmethod
    def _row_count(value: int | None) -> int | None:
        if value is not None and (type(value) is not int or not -1 <= value < 2**63):
            raise AdbcError("Invalid affected row count", "invalid_data")
        return value

    def _encoded_producer(self, producer: ResultProducer) -> bytes:
        encoded = producer.encode()
        if len(encoded) > self.limits.producer_state_bytes:
            raise AdbcError("Result producer state exceeds configured limit", "invalid_data")
        return encoded

    def _register_result(self, session: _Session, query: QueryResult) -> p.ExecuteResponse:
        result = _Result(query)
        try:
            if query.producer is not None:
                result.producer = self._encoded_producer(query.producer)
            self._ensure_result_slot(session)
            encoded = self._encoded_schema(query.schema)
            rid = secrets.token_urlsafe(24)
            response = self._response(
                p.ExecuteResponse(result_id=rid, rows_affected=self._row_count(query.rows_affected), schema_ipc=encoded)
            )
        except Exception:
            result.close()
            raise
        session.results[rid] = result
        return response

    @guarded
    def open_connection(self, request: p.OpenConnectionRequest, ctx: CallContext) -> p.SessionResponse:
        """Authenticate and allocate a connection within the service quota."""
        principal = self._principal(ctx)
        if request.target != self.worker.target:
            raise AdbcError("Target is unavailable", "not_found")
        database_options = option_mapping(request.database_options)
        connection_options = option_mapping(request.connection_options)
        configured_keys = self._database_options.keys() | self._connection_options.keys()
        if database_options.keys() & configured_keys:
            raise AdbcError("Database option is configured by the server", "unauthorized")
        if connection_options.keys() & configured_keys:
            raise AdbcError("Connection option is configured by the server", "unauthorized")
        database_options.update(self._database_options)
        connection_options.update(self._connection_options)
        with self._lock:
            if self._closed:
                raise AdbcError("Service is closed", "invalid_state")
            if len(self._sessions) + self._opening >= self.limits.sessions:
                raise AdbcError("Session limit reached", "invalid_state")
            self._opening += 1
        connection = None
        inserted = False
        try:
            connection = self.worker.open_connection(principal, database_options, connection_options)
            sid = secrets.token_urlsafe(24)
            response = self._response(p.SessionResponse(session_id=sid))
            with self._lock:
                if self._closed:
                    raise AdbcError("Service is closed", "invalid_state")
                self._sessions[sid] = _Session(principal, connection)
                inserted = True
            return response
        finally:
            try:
                if connection is not None and not inserted:
                    connection.close()
            finally:
                with self._lock:
                    self._opening -= 1

    @guarded
    def close_connection(self, session_id: str, ctx: CallContext) -> p.OkResponse:
        """Close a connection and all child handles."""
        session = self._session(session_id, ctx)
        self._close_session(session_id, session)
        return self._response(p.OkResponse(ok=True))

    @guarded
    def new_statement(self, session_id: str, ctx: CallContext) -> p.StatementResponse:
        """Allocate an empty statement within the session quota."""
        session = self._session(session_id, ctx)
        if len(session.statements) >= self.limits.statements_per_session:
            raise AdbcError("Statement limit reached", "invalid_state")
        statement_id = secrets.token_urlsafe(24)
        response = self._response(p.StatementResponse(session_id=session_id, statement_id=statement_id))
        session.statements[statement_id] = _Statement(session.connection.new_statement())
        return response

    @guarded
    def close_statement(self, session_id: str, statement_id: str, ctx: CallContext) -> p.OkResponse:
        """Close a statement and its active result."""
        session, statement = self._statement(session_id, statement_id, ctx)
        # Cancellation does not take the session lock. Remove the handle under
        # its lock before backend teardown so it cannot enter a closing object.
        with session.cancel_lock:
            del session.statements[statement_id]
        self._discard_result(session, statement)
        statement.close()
        return self._response(p.OkResponse(ok=True))

    @guarded
    def set_sql_query(self, session_id: str, statement_id: str, sql: str, ctx: CallContext) -> p.OkResponse:
        """Replace statement SQL after validating its encoded size."""
        session, statement = self._statement(session_id, statement_id, ctx)
        if len(sql.encode("utf-8")) > self.limits.sql_bytes:
            raise AdbcError("SQL exceeds configured limit", "invalid_arguments")
        self._discard_result(session, statement)
        statement.backend.set_sql_query(sql)
        self._discard_binding(statement)
        return self._response(p.OkResponse(ok=True))

    @guarded
    def execute(self, session_id: str, statement_id: str, ctx: CallContext) -> p.ExecuteResponse:
        """Execute SQL and return its schema and lazy batch iterator."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._require_complete_binding(statement)
        self._discard_result(session, statement)
        self._ensure_result_slot(session)
        response = self._register_result(session, statement.backend.execute())
        statement.result_id = response.result_id
        return response

    @guarded
    def execute_schema(self, session_id: str, statement_id: str, ctx: CallContext) -> p.SchemaResponse:
        """Infer the result schema without opening a cursor."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._require_complete_binding(statement)
        self._discard_result(session, statement)
        return self._schema_response(statement.backend.execute_schema())

    @guarded
    def read_result(self, session_id: str, result_id: str, sequence: int, ctx: CallContext) -> Stream[p.ResultCursor]:
        """Open a pull stream at the requested result sequence."""
        session = self._session(session_id, ctx)
        result = session.results.get(result_id)
        if result is None:
            raise AdbcError("Result is unavailable", "not_found")
        if result.producer is not None and sequence != 0:
            raise AdbcError("Producer results resume from continuation tokens", "invalid_arguments")
        return Stream(
            output_schema=result.query.schema,
            state=p.ResultCursor(session_id, result_id, sequence, result.producer),
        )

    @guarded
    def next_batch(self, session_id: str, result_id: str, sequence: int, ctx: CallContext) -> pa.RecordBatch | None:
        """Fetch one batch, replay the previous batch, or report exhaustion."""
        session = self._session(session_id, ctx)
        result = session.results.get(result_id)
        if result is None:
            raise AdbcError("Result is unavailable", "not_found")
        result.touched = time.monotonic()
        if sequence == result.sequence - 1 and result.last is not None:
            return result.last
        if sequence != result.sequence:
            raise AdbcError("Invalid result sequence", "invalid_arguments")
        if result.finished:
            return None
        try:
            batch = next(result.query.batches, None)
            if batch is None:
                result.finished = True
                result.close()
                return None
            if not batch.schema.equals(result.query.schema, check_metadata=True):
                raise AdbcError("Result schema changed", "invalid_data")
            if batch.get_total_buffer_size() > self.limits.batch_bytes:
                raise AdbcError("Result batch exceeds configured limit", "invalid_data")
            result.sequence += 1
            result.last = batch
            return batch
        except Exception:
            session.results.pop(result_id).close()
            raise

    @guarded
    def next_produced_batch(
        self, session_id: str, result_id: str, sequence: int, state: bytes, ctx: CallContext
    ) -> tuple[pa.RecordBatch | None, bytes]:
        """Resume a producer from token state, fetch one batch, and return the advanced state.

        Replaying the previous sequence re-produces its batch from the token's
        state instead of retaining the batch in memory.

        Args:
            session_id: Owning session handle.
            result_id: Result handle within the session.
            sequence: Batch index the token's state produces next.
            state: Encoded producer from the continuation token.
            ctx: Authenticated call context.

        Returns:
            The batch, or None at end of result, and the state for the following token.
        """
        session = self._session(session_id, ctx)
        result = session.results.get(result_id)
        if result is None or result.producer is None:
            raise AdbcError("Result is unavailable", "not_found")
        result.touched = time.monotonic()
        if sequence not in (result.sequence, result.sequence - 1):
            raise AdbcError("Invalid result sequence", "invalid_arguments")
        if result.finished and sequence == result.sequence:
            return None, state
        try:
            producer = ResultProducer.decode(state)
            batch = producer.produce()
            if batch is None:
                result.finished = True
                return None, state
            if not batch.schema.equals(result.query.schema, check_metadata=True):
                raise AdbcError("Result schema changed", "invalid_data")
            if batch.get_total_buffer_size() > self.limits.batch_bytes:
                raise AdbcError("Result batch exceeds configured limit", "invalid_data")
            advanced = self._encoded_producer(producer)
        except Exception:
            session.results.pop(result_id).close()
            raise
        result.sequence = max(result.sequence, sequence + 1)
        return batch, advanced

    @guarded
    def close_result(self, session_id: str, result_id: str, ctx: CallContext) -> p.OkResponse:
        """Release a result cursor and retained replay batch."""
        session = self._session(session_id, ctx)
        result = session.results.pop(result_id, None)
        if result:
            result.close()
        return self._response(p.OkResponse(ok=True))

    @guarded
    def set_connection_option(self, request: p.SetConnectionOptionRequest, ctx: CallContext) -> p.OkResponse:
        """Set connection option when supported."""
        session = self._session(request.session_id, ctx)
        key = self._option_key(request.key)
        if key in self._database_options or key in self._connection_options:
            raise AdbcError("Connection option is configured by the server", "unauthorized")
        session.connection.set_option(key, request.value.to_value())
        return self._response(p.OkResponse(ok=True))

    def _option_key(self, key: str) -> str:
        validate_key(key)
        if len(key.encode()) > self.limits.request_bytes:
            raise AdbcError("Option key exceeds configured limit", "invalid_arguments")
        return key

    def _option_response(self, value: OptionValue, value_type: str) -> p.ValueResponse:
        if isinstance(value, str | bytes) and len(value) > self.limits.batch_bytes:
            raise AdbcError("Option value exceeds configured limit", "invalid_data")
        return self._response(p.ValueResponse(value=WireOptionValue.from_value(value, value_type)))

    @staticmethod
    def _value_type(value_type: str) -> str:
        if value_type not in {"string", "bytes", "int", "double"}:
            raise AdbcError("Invalid option type", "invalid_arguments")
        return value_type

    def cancel_connection(self, session_id: str, ctx: CallContext) -> p.OkResponse:
        """Call a nonblocking backend cancellation hook without the session lock."""
        session = self._session(session_id, ctx)
        with session.cancel_lock:
            self._session(session_id, ctx)
            try:
                session.connection.cancel()
            except AdbcError:
                raise
            except Exception:  # noqa: BLE001 -- sanitize downstream cancellation errors
                raise AdbcError("Cancellation failed", "internal") from None
        return self._response(p.OkResponse(ok=True))

    def cancel_statement(self, session_id: str, statement_id: str, ctx: CallContext) -> p.OkResponse:
        """Cancel only an active operation on the selected statement."""
        session = self._session(session_id, ctx)
        with session.cancel_lock:
            self._session(session_id, ctx)
            if statement_id not in session.statements:
                raise AdbcError("Statement is unavailable", "not_found")
            if session.active_statement != statement_id:
                raise AdbcError("No cancellable operation on this statement", "not_implemented")
            try:
                session.statements[statement_id].backend.cancel()
            except AdbcError:
                raise
            except Exception:  # noqa: BLE001 -- sanitize downstream cancellation errors
                raise AdbcError("Cancellation failed", "internal") from None
        return self._response(p.OkResponse(ok=True))

    @guarded
    def get_connection_option(self, session_id: str, key: str, value_type: str, ctx: CallContext) -> p.ValueResponse:
        """Get connection option when supported."""
        session = self._session(session_id, ctx)
        value_type = self._value_type(value_type)
        return self._option_response(session.connection.get_option(self._option_key(key), value_type), value_type)

    @guarded
    def set_statement_option(
        self,
        request: p.SetStatementOptionRequest,
        ctx: CallContext,
    ) -> p.OkResponse:
        """Set a typed backend statement option, including ingestion configuration."""
        _, statement = self._statement(request.session_id, request.statement_id, ctx)
        key = self._option_key(request.key)
        if key in self._database_options or key in self._connection_options:
            raise AdbcError("Statement option is configured by the server", "unauthorized")
        statement.backend.set_option(key, request.value.to_value())
        return self._response(p.OkResponse(ok=True))

    @guarded
    def get_statement_option(
        self,
        session_id: str,
        statement_id: str,
        key: str,
        value_type: str,
        ctx: CallContext,
    ) -> p.ValueResponse:
        """Read a backend statement option in its requested representation."""
        _, statement = self._statement(session_id, statement_id, ctx)
        value_type = self._value_type(value_type)
        return self._option_response(statement.backend.get_option(self._option_key(key), value_type), value_type)

    @guarded
    def commit(self, session_id: str, ctx: CallContext) -> p.OkResponse:
        """Commit the backend transaction while retaining connection ownership."""
        self._session(session_id, ctx).connection.commit()
        return self._response(p.OkResponse(ok=True))

    @guarded
    def rollback(self, session_id: str, ctx: CallContext) -> p.OkResponse:
        """Roll back the backend transaction without emulating database behavior."""
        self._session(session_id, ctx).connection.rollback()
        return self._response(p.OkResponse(ok=True))

    @guarded
    def prepare(self, session_id: str, statement_id: str, ctx: CallContext) -> p.OkResponse:
        """Prepare a statement through the backend capability hook."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._require_complete_binding(statement)
        self._discard_result(session, statement)
        statement.backend.prepare()
        return self._response(p.OkResponse(ok=True))

    @guarded
    def set_substrait_plan(self, session_id: str, statement_id: str, payload: bytes, ctx: CallContext) -> p.OkResponse:
        """Install a bounded serialized Substrait plan on the backend statement."""
        session, statement = self._statement(session_id, statement_id, ctx)
        if len(payload) > self.limits.request_bytes:
            raise AdbcError("Substrait plan exceeds configured limit", "invalid_arguments")
        self._discard_result(session, statement)
        statement.backend.set_substrait_plan(payload)
        self._discard_binding(statement)
        return self._response(p.OkResponse(ok=True))

    @guarded
    def get_parameter_schema(self, session_id: str, statement_id: str, ctx: CallContext) -> p.SchemaResponse:
        """Return the backend's prepared parameter schema within the schema budget."""
        _, statement = self._statement(session_id, statement_id, ctx)
        return self._schema_response(statement.backend.get_parameter_schema())

    @guarded
    def execute_update(self, session_id: str, statement_id: str, ctx: CallContext) -> p.UpdateResponse:
        """Execute an update or ingestion statement and retain its affected-row count."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._require_complete_binding(statement)
        self._discard_result(session, statement)
        return self._response(p.UpdateResponse(rows_affected=self._row_count(statement.backend.execute_update())))

    def _partition_owner(self, principal: str) -> str:
        return hmac.new(
            self._partition_key, (self.worker.target + "\0" + principal).encode(), hashlib.sha256
        ).hexdigest()

    def _seal_partition(self, descriptor: bytes, principal: str) -> bytes:
        if not isinstance(descriptor, bytes) or len(descriptor) > self.limits.batch_bytes:
            raise AdbcError("Partition descriptor exceeds configured limit", "invalid_data")
        claims = PartitionClaims(
            version=1,
            expires_at_ms=int((time.time() + self.limits.idle_seconds) * 1000),
            owner=self._partition_owner(principal),
            descriptor=descriptor,
        )
        return seal_partition(claims, self._partition_key, self.limits.request_bytes)

    def _unseal_partition(self, payload: bytes, principal: str) -> bytes:
        claims = unseal_partition(payload, self._partition_key, self.limits.request_bytes)
        if claims.expires_at_ms <= int(time.time() * 1000) or not hmac.compare_digest(
            claims.owner, self._partition_owner(principal)
        ):
            raise AdbcError("Partition descriptor is unavailable", "not_found")
        return claims.descriptor

    @guarded
    def execute_partitions(self, session_id: str, statement_id: str, ctx: CallContext) -> p.PartitionsResponse:
        """Export bounded, expiring partition descriptors bound to the authenticated principal."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._require_complete_binding(statement)
        self._discard_result(session, statement)
        result = statement.backend.execute_partitions()
        schema = self._encoded_schema(result.schema)
        count = self._row_count(result.rows_affected)
        if count is None or len(result.partitions) > self.limits.partitions_per_result:
            raise AdbcError("Invalid partitioned result", "invalid_data")
        descriptors: list[bytes] = []
        size = len(schema)
        for descriptor in result.partitions:
            sealed = self._seal_partition(descriptor, session.principal)
            if len(sealed) > self.limits.request_bytes:
                raise AdbcError("Partition descriptor exceeds configured limit", "invalid_data")
            size += len(sealed)
            if size > self.limits.batch_bytes:
                raise AdbcError("Partitioned result exceeds configured limit", "invalid_data")
            descriptors.append(sealed)
        return self._response(p.PartitionsResponse(rows_affected=count, schema_ipc=schema, partitions=descriptors))

    @guarded
    def read_partition(self, session_id: str, payload: bytes, ctx: CallContext) -> p.ExecuteResponse:
        """Read a validated partition through a fresh, independently bounded result handle."""
        session = self._session(session_id, ctx)
        descriptor = self._unseal_partition(payload, session.principal)
        self._ensure_result_slot(session)
        return self._register_result(session, session.connection.read_partition(descriptor))

    @guarded
    def get_info(self, request: p.GetInfoRequest, ctx: CallContext) -> p.ExecuteResponse:
        """Read requested unsigned ADBC information codes as a bounded Arrow result."""
        session = self._session(request.session_id, ctx)
        self._ensure_result_slot(session)
        return self._register_result(session, session.connection.get_info(request.codes))

    @guarded
    def get_objects(self, request: p.GetObjectsRequest, ctx: CallContext) -> p.ExecuteResponse:
        """Read the backend's hierarchical object metadata and preserve its filters."""
        session = self._session(request.session_id, ctx)
        self._ensure_result_slot(session)
        return self._register_result(
            session,
            session.connection.get_objects(
                request.depth,
                request.catalog,
                request.db_schema,
                request.table_name,
                request.table_types,
                request.column_name,
            ),
        )

    @guarded
    def get_table_schema(self, request: p.GetTableSchemaRequest, ctx: CallContext) -> p.SchemaResponse:
        """Read a table schema after validating the metadata filter arguments."""
        session = self._session(request.session_id, ctx)
        return self._schema_response(
            session.connection.get_table_schema(request.catalog, request.db_schema, request.table_name)
        )

    @guarded
    def get_table_types(self, session_id: str, ctx: CallContext) -> p.ExecuteResponse:
        """Return a bounded table-type discovery cursor."""
        session = self._session(session_id, ctx)
        self._ensure_result_slot(session)
        return self._register_result(session, session.connection.get_table_types())

    @guarded
    def get_statistic_names(self, session_id: str, ctx: CallContext) -> p.ExecuteResponse:
        """Return a bounded statistic-name discovery cursor."""
        session = self._session(session_id, ctx)
        self._ensure_result_slot(session)
        return self._register_result(session, session.connection.get_statistic_names())

    @guarded
    def get_statistics(self, request: p.GetStatisticsRequest, ctx: CallContext) -> p.ExecuteResponse:
        """Read backend statistics with exact or approximate semantics preserved."""
        session = self._session(request.session_id, ctx)
        self._ensure_result_slot(session)
        return self._register_result(
            session,
            session.connection.get_statistics(
                request.catalog, request.db_schema, request.table_name, request.approximate
            ),
        )

    def _start_binding(
        self, session_id: str, statement_id: str, schema_ipc: bytes, stream: bool, ctx: CallContext
    ) -> Stream[p.BindCursor]:
        session, statement = self._statement(session_id, statement_id, ctx)
        schema = decode_schema(schema_ipc, self.limits.batch_bytes)
        self._discard_result(session, statement)
        self._discard_upload(statement)
        upload_id = secrets.token_urlsafe(24)
        statement.upload = BindUpload(
            schema, limit=self.limits.bind_bytes, batch_limit=self.limits.batch_bytes, stream=stream
        )
        statement.upload_id = upload_id
        return Stream(
            output_schema=p.OK,
            input_schema=p.BIND_INPUT,
            state=p.BindCursor(session_id, statement_id, upload_id),
        )

    @guarded
    def bind(self, session_id: str, statement_id: str, schema_ipc: bytes, ctx: CallContext) -> Stream[p.BindCursor]:
        """Begin a bounded single-batch Arrow binding exchange."""
        return self._start_binding(session_id, statement_id, schema_ipc, False, ctx)

    @guarded
    def bind_stream(
        self, session_id: str, statement_id: str, schema_ipc: bytes, ctx: CallContext
    ) -> Stream[p.BindCursor]:
        """Begin a bounded Arrow stream exchange backed by an anonymous temporary file."""
        return self._start_binding(session_id, statement_id, schema_ipc, True, ctx)

    def _bind_frame_bytes(self) -> int:
        """Largest bind frame: one request, or an upload through external storage.

        Returns:
            The byte limit.
        """
        return max(self.limits.request_bytes, self._external_upload_bytes)

    @guarded
    def push_binding_frame(
        self,
        session_id: str,
        statement_id: str,
        upload_id: str,
        sequence: int,
        frame: pa.RecordBatch,
        ctx: CallContext,
    ) -> None:
        """Decode a fixed one-row binding envelope before staging parameter data."""
        _, statement = self._statement(session_id, statement_id, ctx)
        pending = upload_id == statement.upload_id
        upload = statement.upload if pending else statement.binding if upload_id == statement.binding_id else None
        if upload is None:
            raise AdbcError("Bind upload is unavailable", "not_found")
        try:
            if (
                not frame.schema.equals(p.BIND_INPUT, check_metadata=True)
                or frame.num_rows != 1
                or any(column.null_count for column in frame.columns)
                or frame.get_total_buffer_size() > self._bind_frame_bytes()
            ):
                raise AdbcError("Invalid bind envelope", "invalid_arguments")
            payload = frame.column("batch_ipc")[0].as_py()
            finish = frame.column("finish")[0].as_py()
            if finish:
                if payload:
                    raise AdbcError("Bind finish payload must be empty", "invalid_arguments")
                batch = pa.RecordBatch.from_pylist([], schema=upload.schema)
            else:
                batch = decode_batch(payload, self._bind_frame_bytes())
            self.push_binding(session_id, statement_id, upload_id, sequence, batch, finish, ctx)
        except Exception:
            if pending:
                self._discard_upload(statement)
            raise

    @guarded
    def push_binding(
        self,
        session_id: str,
        statement_id: str,
        upload_id: str,
        sequence: int,
        batch: pa.RecordBatch,
        finish: bool,
        ctx: CallContext,
    ) -> None:
        """Stage one turn and bind only after an explicit, successfully completed finish."""
        _, statement = self._statement(session_id, statement_id, ctx)
        pending = upload_id == statement.upload_id
        upload = statement.upload if pending else statement.binding if upload_id == statement.binding_id else None
        if upload is None:
            raise AdbcError("Bind upload is unavailable", "not_found")
        try:
            if not upload.accept(batch, sequence, finish=finish):
                return
            assert upload.reader is not None
            if upload.stream:
                statement.backend.bind_stream(upload.reader)
            else:
                statement.backend.bind(next(upload.reader))
            # The backend has replaced its prior binding. Only now release its
            # old reader; a failed or cancelled upload must leave it usable.
            previous = statement.binding
            statement.binding, statement.binding_id = upload, upload_id
            statement.upload, statement.upload_id = None, None
            if previous is not None:
                previous.close()
        except Exception:
            if pending:
                self._discard_upload(statement)
            raise

    @guarded
    def cancel_binding(self, session_id: str, statement_id: str, upload_id: str, ctx: CallContext) -> None:
        """Release pending input; stream teardown after a final acknowledgement preserves binding."""
        _, statement = self._statement(session_id, statement_id, ctx)
        if statement.upload_id == upload_id:
            self._discard_upload(statement)

    def app(
        self,
        *,
        tokens: dict[str, str] | TokenStore | None = None,
        anonymous_principal: str | None = None,
        external_storage: ExternalStorageConfig | None = None,
    ) -> PrivateApplication:
        """Create the WSGI app, authenticating bearer tokens and optionally anonymous requests.

        Anonymous access is for services that are safe to expose without
        credentials, such as read-only data. Requests without an Authorization
        header act as ``anonymous_principal``; all anonymous clients share that
        principal, so the worker should grant it only public, read-only
        capabilities. A request that presents a bearer token which does not
        match is rejected, never downgraded to anonymous.

        Args:
            tokens: Bearer secrets mapped to principals; optional when anonymous access is enabled.
            anonymous_principal: Principal for requests without credentials; None requires a token.
            external_storage: Bucket for requests over the request limit and large result batches.

        Returns:
            The WSGI application.
        """
        credentials = access_credentials(tokens, anonymous_principal)
        external, upload_urls = external_storage.server_config() if external_storage is not None else (None, None)
        if external_storage is not None:
            self._external_upload_bytes = external_storage.max_upload_bytes

        def authenticate(req: falcon.Request) -> AuthContext:
            supplied = req.get_header("Authorization")
            if supplied is None and anonymous_principal is not None:
                # A distinct domain keeps anonymous continuation tokens separate from token principals.
                return AuthContext(domain="grainlift.anonymous", authenticated=True, principal=anonymous_principal)
            principal = credentials.authenticate(supplied or "") if credentials is not None else None
            if principal is not None and principal != anonymous_principal:
                return AuthContext(domain="grainlift", authenticated=True, principal=principal)
            raise ValueError("Authentication required")

        app = make_wsgi_app(
            RpcServer(p.Grainlift, self, external_location=external),
            token_key=secrets.token_bytes(32),
            authenticate=authenticate,
            max_request_bytes=self.limits.request_bytes,
            max_response_bytes=self.limits.batch_bytes + 1024 * 1024,
            token_ttl=max(1, int(self.limits.idle_seconds)),
            enable_landing_page=False,
            enable_describe_page=False,
            upload_url_provider=upload_urls,
            max_upload_bytes=external_storage.max_upload_bytes if external_storage is not None else None,
        )
        return PrivateApplication(app)


def access_credentials(
    tokens: dict[str, str] | TokenStore | None, anonymous_principal: str | None
) -> TokenStore | None:
    """Validate an HTTP access configuration.

    Args:
        tokens: Bearer secrets mapped to principals, if any.
        anonymous_principal: Principal for requests without credentials, if enabled.

    Returns:
        The token store, or None for anonymous-only access.
    """
    if tokens is None and anonymous_principal is None:
        raise ValueError("Configure bearer tokens, anonymous access, or both")
    if anonymous_principal is not None and (
        not isinstance(anonymous_principal, str) or not anonymous_principal or len(anonymous_principal) > 1024
    ):
        raise ValueError("Invalid anonymous principal")
    credentials = tokens if tokens is None or isinstance(tokens, TokenStore) else TokenStore(tokens)
    if credentials is not None and anonymous_principal in credentials.principals():
        raise ValueError("The anonymous principal must differ from every token principal")
    return credentials


def serve(
    worker: Worker,
    *,
    token: str | None = None,
    port: int = 8080,
    limits: Limits | None = None,
    anonymous_principal: str | None = None,
    external_storage: ExternalStorageConfig | None = None,
) -> None:
    """Serve on loopback. Deployment behind TLS requires a process-affine WSGI host.

    Args:
        worker: Worker to serve.
        token: Bearer token for the ``developer`` principal; optional with anonymous access.
        port: Loopback port.
        limits: ADBC quotas and cleanup deadlines.
        anonymous_principal: Principal for requests without credentials; None requires the token.
        external_storage: Bucket for requests over the request limit and large result batches.
    """
    import waitress

    with Service(worker, limits=limits) as service:
        app = service.app(
            tokens={token: "developer"} if token else None,
            anonymous_principal=anonymous_principal,
            external_storage=external_storage,
        )
        print(f"Grainlift target {worker.target!r} listening on http://127.0.0.1:{port}", flush=True)
        # Waitress rejects Content-Length >= its cap; the SDK rejects > its cap.
        # Translate the host boundary while retaining the exact application limit.
        waitress.serve(app, host="127.0.0.1", port=port, max_request_body_size=service.limits.request_bytes + 1)
