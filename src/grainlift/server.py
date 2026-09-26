# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Bounded, process-local Grainlift sessions and HTTP serving."""

from __future__ import annotations

import inspect
import json
import secrets
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from functools import wraps
from typing import Concatenate, ParamSpec, TypeVar

import falcon
import pyarrow as pa
from vgi_rpc import AuthContext, CallContext, RpcServer, Stream
from vgi_rpc.http import make_wsgi_app

from . import protocol as p
from .api import AdbcError, Connection, Limits, QueryResult, Worker
from .credentials import TokenStore
from .telemetry import PrivateApplication

P = ParamSpec("P")
T = TypeVar("T")


def guarded[**P, T](method: Callable[Concatenate[Service, P], T]) -> Callable[Concatenate[Service, P], T]:
    """Serialize only one session; never hold the registry lock across a worker callback."""
    signature = inspect.signature(method)

    @wraps(method)
    def call(self: Service, /, *args: P.args, **kwargs: P.kwargs) -> T:
        bound = signature.bind(self, *args, **kwargs)
        sid = bound.arguments.get("session_id")
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
                    statement_id = bound.arguments.get("statement_id")
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
    sql: str | None = None
    result_id: str | None = None


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
        self.statements.clear()
        with suppress(Exception):  # Continue cleanup without logging worker secrets.
            self.connection.close()
        self.closed = True


class Service:
    """Own handles for one worker. Use as a context manager or call close()."""

    def __init__(self, worker: Worker, *, limits: Limits | None = None) -> None:
        """Start the handle registry and idle-resource reaper.

        Args:
            worker: Factory for independent principal-bound connections.
            limits: Service quotas and lifecycle deadlines; defaults to Limits().
        """
        self.worker = worker
        self.limits = limits or Limits()
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

    @guarded
    def open_connection(
        self, target: str, database_options_json: str, connection_options_json: str, ctx: CallContext
    ) -> pa.RecordBatch:
        """Authenticate and allocate a connection within the service quota."""
        principal = self._principal(ctx)
        if target != self.worker.target:
            raise AdbcError("Target is unavailable", "not_found")
        for encoded in (database_options_json, connection_options_json):
            try:
                options = json.loads(encoded)
            except (ValueError, TypeError):
                raise AdbcError("Invalid options", "invalid_arguments") from None
            if options != []:
                raise AdbcError("Caller-supplied options are not supported", "not_implemented")
        with self._lock:
            if self._closed:
                raise AdbcError("Service is closed", "invalid_state")
            if len(self._sessions) + self._opening >= self.limits.sessions:
                raise AdbcError("Session limit reached", "invalid_state")
            self._opening += 1
        connection = None
        inserted = False
        try:
            connection = self.worker.connect(principal)
            sid = secrets.token_urlsafe(24)
            response = p.batch(p.SESSION, session_id=sid)
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
    def close_connection(self, session_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Close a connection and all child handles."""
        session = self._session(session_id, ctx)
        self._close_session(session_id, session)
        return p.batch(p.OK, ok=True)

    @guarded
    def new_statement(self, session_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Allocate an empty statement within the session quota."""
        session = self._session(session_id, ctx)
        if len(session.statements) >= self.limits.statements_per_session:
            raise AdbcError("Statement limit reached", "invalid_state")
        statement_id = secrets.token_urlsafe(24)
        session.statements[statement_id] = _Statement()
        return p.batch(p.STATEMENT, session_id=session_id, statement_id=statement_id)

    @guarded
    def close_statement(self, session_id: str, statement_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Close a statement and its active result."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._discard_result(session, statement)
        del session.statements[statement_id]
        return p.batch(p.OK, ok=True)

    @guarded
    def set_sql_query(self, session_id: str, statement_id: str, sql: str, ctx: CallContext) -> pa.RecordBatch:
        """Replace statement SQL after validating its encoded size."""
        session, statement = self._statement(session_id, statement_id, ctx)
        if len(sql.encode("utf-8")) > self.limits.sql_bytes:
            raise AdbcError("SQL exceeds configured limit", "invalid_arguments")
        self._discard_result(session, statement)
        statement.sql = sql
        return p.batch(p.OK, ok=True)

    @guarded
    def execute(self, session_id: str, statement_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Execute SQL and return its schema and lazy batch iterator."""
        session, statement = self._statement(session_id, statement_id, ctx)
        self._discard_result(session, statement)
        if statement.sql is None:
            raise AdbcError("Set a query before execution", "invalid_state")
        query = session.connection.execute(statement.sql)
        result = _Result(query)
        try:
            encoded = p.schema_ipc(query.schema)
            if len(encoded) > self.limits.batch_bytes:
                raise AdbcError("Schema exceeds configured limit", "invalid_data")
            rid = secrets.token_urlsafe(24)
            response = p.batch(p.EXECUTE, result_id=rid, rows_affected=query.rows_affected, schema_ipc=encoded)
        except Exception:
            result.close()
            raise
        session.results[rid] = result
        statement.result_id = rid
        return response

    @guarded
    def execute_schema(self, session_id: str, statement_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Infer the result schema without opening a cursor."""
        session, statement = self._statement(session_id, statement_id, ctx)
        if statement.sql is None:
            raise AdbcError("Set a query before schema inference", "invalid_state")
        encoded = p.schema_ipc(session.connection.execute_schema(statement.sql))
        if len(encoded) > self.limits.batch_bytes:
            raise AdbcError("Schema exceeds configured limit", "invalid_data")
        return p.batch(p.SCHEMA, schema_ipc=encoded)

    @guarded
    def read_result(self, session_id: str, result_id: str, sequence: int, ctx: CallContext) -> Stream[p.ResultCursor]:
        """Open a pull stream at the requested result sequence."""
        session = self._session(session_id, ctx)
        result = session.results.get(result_id)
        if result is None:
            raise AdbcError("Result is unavailable", "not_found")
        return Stream(output_schema=result.query.schema, state=p.ResultCursor(session_id, result_id, sequence))

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
    def close_result(self, session_id: str, result_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Release a result cursor and retained replay batch."""
        session = self._session(session_id, ctx)
        result = session.results.pop(result_id, None)
        if result:
            result.close()
        return p.batch(p.OK, ok=True)

    @guarded
    def set_connection_option(self, session_id: str, key: str, value_json: str, ctx: CallContext) -> pa.RecordBatch:
        """Set connection option when supported."""
        self._session(session_id, ctx)
        try:
            value = json.loads(value_json)
        except (ValueError, TypeError):
            raise AdbcError("Invalid option value", "invalid_arguments") from None
        if key == "adbc.connection.autocommit" and value == {
            "type": "string",
            "value": "true",
        }:
            return p.batch(p.OK, ok=True)
        raise AdbcError("Connection option is not supported", "not_implemented")

    def cancel_connection(self, session_id: str, ctx: CallContext) -> pa.RecordBatch:
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
        return p.batch(p.OK, ok=True)

    def cancel_statement(self, session_id: str, statement_id: str, ctx: CallContext) -> pa.RecordBatch:
        """Cancel only an active operation on the selected statement."""
        session = self._session(session_id, ctx)
        with session.cancel_lock:
            self._session(session_id, ctx)
            if statement_id not in session.statements:
                raise AdbcError("Statement is unavailable", "not_found")
            if session.active_statement != statement_id:
                raise AdbcError("No cancellable operation on this statement", "not_implemented")
            try:
                session.connection.cancel()
            except AdbcError:
                raise
            except Exception:  # noqa: BLE001 -- sanitize downstream cancellation errors
                raise AdbcError("Cancellation failed", "internal") from None
        return p.batch(p.OK, ok=True)

    @guarded
    def get_connection_option(self, session_id: str, key: str, value_type: str, ctx: CallContext) -> pa.RecordBatch:
        """Get connection option when supported."""
        self._session(session_id, ctx)
        if key == "adbc.connection.autocommit" and value_type == "string":
            return p.batch(p.VALUE, value_json=json.dumps({"type": "string", "value": "true"}))
        raise AdbcError("Connection option is not supported", "not_implemented")

    def __getattr__(self, name: str) -> Callable[..., Stream[p.ResultCursor]]:
        """Resolve unsupported protocol methods to authenticated ADBC errors."""
        if name.startswith("_") or not callable(getattr(p.Grainlift, name, None)):
            raise AttributeError(name)

        def unsupported(
            session_id: str,
            statement_id: str | None = None,
            schema_ipc: bytes | None = None,
            args_json: str | None = None,
            key: str | None = None,
            payload: bytes | None = None,
            value_type: str | None = None,
            value_json: str | None = None,
            *,
            ctx: CallContext,
        ) -> Stream[p.ResultCursor]:
            with self._lock:
                session = self._session(session_id, ctx)
                if statement_id is not None and statement_id not in session.statements:
                    raise AdbcError("Statement is unavailable", "not_found")
                raise AdbcError(f"{name} is not implemented", "not_implemented")

        return unsupported

    def app(self, *, tokens: dict[str, str] | TokenStore) -> PrivateApplication:
        """Create authenticated WSGI app; token values map to configured principals."""
        credentials = tokens if isinstance(tokens, TokenStore) else TokenStore(tokens)

        def authenticate(req: falcon.Request) -> AuthContext:
            supplied = req.get_header("Authorization") or ""
            principal = credentials.authenticate(supplied)
            if principal is not None:
                return AuthContext(domain="grainlift", authenticated=True, principal=principal)
            raise ValueError("Authentication required")

        app = make_wsgi_app(
            RpcServer(p.Grainlift, self),
            token_key=secrets.token_bytes(32),
            authenticate=authenticate,
            max_request_bytes=self.limits.request_bytes,
            max_response_bytes=self.limits.batch_bytes + 1024 * 1024,
            token_ttl=max(1, int(self.limits.idle_seconds)),
            enable_landing_page=False,
            enable_describe_page=False,
        )
        return PrivateApplication(app)


def serve(worker: Worker, *, token: str, port: int = 8080, limits: Limits | None = None) -> None:
    """Serve on loopback. Deployment behind TLS requires a process-affine WSGI host."""
    import waitress

    with Service(worker, limits=limits) as service:
        app = service.app(tokens={token: "developer"})
        print(f"Grainlift target {worker.target!r} listening on http://127.0.0.1:{port}", flush=True)
        # Waitress rejects Content-Length >= its cap; the SDK rejects > its cap.
        # Translate the host boundary while retaining the exact application limit.
        waitress.serve(app, host="127.0.0.1", port=port, max_request_body_size=service.limits.request_bytes + 1)
