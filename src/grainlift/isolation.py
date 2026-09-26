# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Optional process-per-connection execution with finite callback deadlines.

This isolates failures, not untrusted code. Worker factories are trusted,
importable module:attribute names. Killing a worker invalidates its entire
connection, statements and results; reconnection is always explicit.
"""

from __future__ import annotations

import base64
import contextlib
import importlib
import io
import json
import math
import multiprocessing
import os
import struct
import threading
import time
from collections.abc import Buffer, Iterator, Mapping
from multiprocessing.connection import Connection as PipeConnection
from multiprocessing.connection import wait
from typing import Any

import pyarrow as pa

from .api import AdbcError, Connection, OptionValue, PartitionedResult, QueryResult, Statement, Worker
from .binding import BindUpload


class _BoundedBuffer(io.BytesIO):
    def __init__(self, limit: int) -> None:
        """Initialize configured state and resource ownership."""
        super().__init__()
        self.limit = limit

    def write(self, value: Buffer) -> int:
        if self.tell() + memoryview(value).nbytes > self.limit:
            raise AdbcError("Isolated Arrow response exceeds limit", "invalid_data")
        return super().write(value)


def _arrow_bytes(schema: pa.Schema, batch: pa.RecordBatch | None, limit: int) -> bytes:
    out = _BoundedBuffer(limit)
    with pa.ipc.new_stream(out, schema) as writer:
        if batch is not None:
            if batch.get_total_buffer_size() > limit:
                raise AdbcError("Isolated batch exceeds limit", "invalid_data")
            writer.write_batch(batch)
    return out.getvalue()


def _encode(header: Mapping[str, Any], payload: bytes, limit: int) -> bytes:
    metadata = json.dumps(header, allow_nan=False).encode()
    if 4 + len(metadata) + len(payload) > limit:
        raise AdbcError("Isolated message exceeds limit", "invalid_data")
    return struct.pack("<I", len(metadata)) + metadata + payload


def _decode(data: bytes) -> tuple[dict[str, Any], bytes]:
    if len(data) < 4:
        raise AdbcError("Invalid isolated response", "invalid_data")
    length = struct.unpack("<I", data[:4])[0]
    if length > len(data) - 4:
        raise AdbcError("Invalid isolated response", "invalid_data")
    header = json.loads(data[4 : 4 + length])
    if not isinstance(header, dict):
        raise AdbcError("Invalid isolated response", "invalid_data")
    return header, data[4 + length :]


def _send_error(pipe: PipeConnection, error: AdbcError, limit: int) -> None:
    try:
        packet = _encode({"error": json.loads(str(error))}, b"", limit)
    except (AdbcError, ValueError):
        packet = _encode(
            {"error": json.loads(str(AdbcError("Isolated error exceeds limit", "invalid_data")))},
            b"",
            limit,
        )
    pipe.send_bytes(packet)


def _option(value: OptionValue, limit: int) -> dict[str, Any]:
    if isinstance(value, (bytes, str)) and len(value) > limit:
        raise AdbcError("Isolated option exceeds message limit", "invalid_data")
    if isinstance(value, bytes):
        return {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if type(value) is int and -(2**63) <= value < 2**63:
        return {"type": "int", "value": value}
    if type(value) is float:
        # Private pipe JSON carries exact IEEE 754 bits, including NaN payloads
        # and signed zero, without introducing nonstandard JSON numeric values.
        return {"type": "double_bits", "value": struct.pack(">d", value).hex()}
    raise AdbcError("Invalid isolated option value", "invalid_arguments")


def _option_value(value: Mapping[str, Any]) -> OptionValue:
    kind, raw = value["type"], value["value"]
    if kind == "bytes":
        return base64.b64decode(raw, validate=True)
    if kind == "string" and isinstance(raw, str):
        return raw
    if kind == "int" and type(raw) is int and -(2**63) <= raw < 2**63:
        return raw
    if kind == "double_bits" and isinstance(raw, str) and len(raw) == 16:
        try:
            result: float = struct.unpack(">d", bytes.fromhex(raw))[0]
            return result
        except (ValueError, struct.error):
            pass
    raise AdbcError("Invalid isolated option value", "invalid_data")


def _row_count(value: int | None) -> None:
    if value is not None and (type(value) is not int or not -1 <= value < 2**63):
        raise AdbcError("Invalid affected row count", "invalid_data")


class _ChildState:
    def __init__(
        self, connection: Connection, limit: int, max_results: int, max_statements: int, bind_limit: int
    ) -> None:
        self.connection = connection
        self.limit = limit
        self.max_results = max_results
        self.max_statements = max_statements
        self.bind_limit = bind_limit
        self.results: dict[str, QueryResult] = {}
        self.owners: dict[str, str] = {}
        self.statements: dict[str, Statement] = {}
        self.bindings: dict[str, BindUpload] = {}
        self.uploads: dict[str, BindUpload] = {}
        self.sequence = 0

    def _identifier(self) -> str:
        self.sequence += 1
        return str(self.sequence)

    def _result(self, query: QueryResult, owner: str = "") -> tuple[dict[str, Any], bytes]:
        try:
            _row_count(query.rows_affected)
            payload = _arrow_bytes(query.schema, None, self.limit - 1024)
            rid = self._identifier()
            header = {"result_id": rid, "rows_affected": query.rows_affected}
            _encode(header, payload, self.limit)
        except Exception:
            with contextlib.suppress(Exception):
                query.close()
            raise
        self.results[rid] = query
        self.owners[rid] = owner
        return header, payload

    def _release(self, rid: str) -> None:
        self.owners.pop(rid, None)
        query = self.results.pop(rid, None)
        if query is not None:
            query.close()

    def _release_statement(self, sid: str) -> None:
        self._discard_results(sid)
        statement = self.statements.pop(sid, None)
        try:
            if statement is not None:
                statement.close()
        finally:
            self._clear_bindings(sid)

    def _discard_results(self, sid: str) -> None:
        for rid, owner in list(self.owners.items()):
            if owner == sid:
                with contextlib.suppress(Exception):
                    self._release(rid)

    def _clear_bindings(self, sid: str) -> None:
        for registry in (self.bindings, self.uploads):
            binding = registry.pop(sid, None)
            if binding is not None:
                binding.close()

    def close(self) -> None:
        for sid in list(self.statements):
            with contextlib.suppress(Exception):
                self._release_statement(sid)
        for rid in list(self.results):
            with contextlib.suppress(Exception):
                self._release(rid)

    def dispatch(self, request: Mapping[str, Any], incoming: bytes) -> tuple[dict[str, Any], bytes]:
        operation = request["op"]
        sid = request.get("statement_id", "")
        query_operations = {
            "execute",
            "get_info",
            "get_objects",
            "get_table_types",
            "get_statistic_names",
            "get_statistics",
            "read_partition",
        }
        if operation in query_operations and len(self.results) >= self.max_results:
            raise AdbcError("Isolated result limit reached", "invalid_state")
        if operation == "execute":
            return self._result(self.connection.execute(request["sql"]))
        if operation == "schema":
            return {}, _arrow_bytes(self.connection.execute_schema(request["sql"]), None, self.limit - 1024)
        if operation == "fetch":
            rid = request["result_id"]
            query = self.results.get(rid)
            if query is None:
                raise AdbcError("Isolated result is unavailable", "not_found")
            try:
                batch = next(query.batches, None)
                if batch is None:
                    self._release(rid)
                    return {"eof": True}, b""
                if not batch.schema.equals(query.schema, check_metadata=True):
                    raise AdbcError("Isolated result schema changed", "invalid_data")
                return {}, _arrow_bytes(query.schema, batch, self.limit - 1024)
            except Exception:
                with contextlib.suppress(Exception):
                    self._release(rid)
                raise
        if operation == "release":
            self._release(request["result_id"])
        elif operation == "new_statement":
            if len(self.statements) >= self.max_statements:
                raise AdbcError("Isolated statement limit reached", "invalid_state")
            statement = self.connection.new_statement()
            sid = self._identifier()
            self.statements[sid] = statement
            return {"statement_id": sid}, b""
        elif operation == "close_statement":
            self._release_statement(sid)
        elif operation.startswith("statement_") or operation.startswith("bind_"):
            return self._statement(request, incoming)
        elif operation == "get_option":
            return {
                "option": _option(self.connection.get_option(request["key"], request["value_type"]), self.limit)
            }, b""
        elif operation == "set_option":
            self.connection.set_option(request["key"], _option_value(request["option"]))
        elif operation == "commit":
            self.connection.commit()
        elif operation == "rollback":
            self.connection.rollback()
        elif operation == "get_info":
            return self._result(self.connection.get_info(request["codes"]))
        elif operation == "get_objects":
            return self._result(self.connection.get_objects(**request["arguments"]))
        elif operation == "get_table_schema":
            schema = self.connection.get_table_schema(**request["arguments"])
            return {}, _arrow_bytes(schema, None, self.limit - 1024)
        elif operation == "get_table_types":
            return self._result(self.connection.get_table_types())
        elif operation == "get_statistic_names":
            return self._result(self.connection.get_statistic_names())
        elif operation == "get_statistics":
            return self._result(self.connection.get_statistics(**request["arguments"]))
        elif operation == "read_partition":
            return self._result(self.connection.read_partition(incoming))
        else:
            raise AdbcError("Unknown isolated operation", "invalid_arguments")
        return {"ok": True}, b""

    def _statement(self, request: Mapping[str, Any], incoming: bytes) -> tuple[dict[str, Any], bytes]:
        sid = request["statement_id"]
        statement = self.statements.get(sid)
        if statement is None:
            raise AdbcError("Isolated statement is unavailable", "not_found")
        operation = request["op"]
        if sid in self.uploads and operation in {
            "statement_prepare",
            "statement_execute",
            "statement_update",
            "statement_schema",
            "statement_partitions",
        }:
            raise AdbcError("Isolated binding upload has not finished", "invalid_state")
        if operation == "statement_sql":
            self._discard_results(sid)
            statement.set_sql_query(request["sql"])
            self._clear_bindings(sid)
        elif operation == "statement_substrait":
            self._discard_results(sid)
            statement.set_substrait_plan(incoming)
            self._clear_bindings(sid)
        elif operation == "statement_prepare":
            self._discard_results(sid)
            statement.prepare()
        elif operation == "statement_set_option":
            statement.set_option(request["key"], _option_value(request["option"]))
        elif operation == "statement_get_option":
            return {"option": _option(statement.get_option(request["key"], request["value_type"]), self.limit)}, b""
        elif operation == "statement_execute":
            self._discard_results(sid)
            if len(self.results) >= self.max_results:
                raise AdbcError("Isolated result limit reached", "invalid_state")
            return self._result(statement.execute(), sid)
        elif operation == "statement_update":
            self._discard_results(sid)
            count = statement.execute_update()
            _row_count(count)
            return {"rows_affected": count}, b""
        elif operation in ("statement_schema", "statement_parameter_schema"):
            schema = statement.execute_schema() if operation == "statement_schema" else statement.get_parameter_schema()
            return {}, _arrow_bytes(schema, None, self.limit - 1024)
        elif operation == "statement_partitions":
            self._discard_results(sid)
            result = statement.execute_partitions()
            if result.rows_affected is None:
                raise AdbcError("Invalid partition affected row count", "invalid_data")
            _row_count(result.rows_affected)
            if len(result.partitions) > 1024 or any(not isinstance(part, bytes) for part in result.partitions):
                raise AdbcError("Invalid or excessive isolated partitions", "invalid_data")
            schema_bytes = _arrow_bytes(result.schema, None, self.limit - 1024)
            lengths = [len(part) for part in result.partitions]
            if sum(lengths) + len(schema_bytes) > self.limit - 1024:
                raise AdbcError("Isolated partitions exceed limit", "invalid_data")
            return {
                "schema_bytes": len(schema_bytes),
                "partition_bytes": lengths,
                "rows_affected": result.rows_affected,
            }, schema_bytes + b"".join(result.partitions)
        elif operation == "statement_bind":
            self._discard_results(sid)
            with pa.ipc.open_stream(incoming) as reader:
                statement.bind(reader.read_next_batch())
            self._clear_bindings(sid)
        elif operation == "bind_begin":
            self._discard_results(sid)
            with pa.ipc.open_stream(incoming) as reader:
                schema = reader.schema
            previous = self.uploads.pop(sid, None)
            if previous is not None:
                previous.close()
            self.uploads[sid] = BindUpload(schema, limit=self.bind_limit, batch_limit=self.limit - 1024, stream=True)
        elif operation == "bind_abort":
            abandoned = self.uploads.pop(sid, None)
            if abandoned is not None:
                abandoned.close()
        elif operation in ("bind_batch", "bind_finish"):
            upload = self.uploads.get(sid)
            if upload is None:
                raise AdbcError("Isolated binding upload is unavailable", "invalid_state")
            try:
                if operation == "bind_batch":
                    with pa.ipc.open_stream(incoming) as reader:
                        if not reader.schema.equals(upload.schema, check_metadata=True):
                            raise AdbcError("Isolated binding schema changed", "invalid_data")
                        upload.accept(reader.read_next_batch(), upload.sequence, finish=False)
                else:
                    upload.accept(pa.RecordBatch.from_pylist([], schema=upload.schema), upload.sequence, finish=True)
                    assert upload.reader is not None
                    self._discard_results(sid)
                    statement.bind_stream(upload.reader)
                    self.uploads.pop(sid)
                    previous = self.bindings.get(sid)
                    self.bindings[sid] = upload
                    if previous is not None:
                        previous.close()
            except Exception:
                self.uploads.pop(sid, None)
                upload.close()
                raise
        else:
            raise AdbcError("Unknown isolated statement operation", "invalid_arguments")
        return {"ok": True}, b""


def _watch_owner() -> None:
    parent = multiprocessing.parent_process()
    if parent is not None:
        wait([parent.sentinel])
        # The session owner is gone; no callback or IPC reply can be useful.
        # Exit without running potentially stuck backend cleanup hooks.
        os._exit(1)


def _child_main(
    incoming: PipeConnection,
    outgoing: PipeConnection,
    factory: str,
    options_json: str,
    principal: str,
    limit: int,
    max_results: int,
    max_statements: int,
    bind_limit: int,
    connection_options_json: str,
) -> None:
    threading.Thread(target=_watch_owner, name="grainlift-owner-watch", daemon=True).start()
    # Worker-authored stdout/stderr must not accidentally escape into service logs.
    with open(os.devnull, "wb") as sink:
        os.dup2(sink.fileno(), 1)
        os.dup2(sink.fileno(), 2)
    connection: Connection | None = None
    state: _ChildState | None = None
    try:
        module, name = factory.split(":", 1)
        worker_type = getattr(importlib.import_module(module), name)
        worker: Worker = worker_type(**json.loads(options_json))
        configured = json.loads(connection_options_json)
        connection = worker.open_connection(
            principal,
            {key: _option_value(value) for key, value in configured["database"].items()},
            {key: _option_value(value) for key, value in configured["connection"].items()},
        )
        state = _ChildState(connection, limit, max_results, max_statements, bind_limit)
        outgoing.send_bytes(_encode({"ready": True}, b"", limit))
        while True:
            request, incoming_payload = _decode(incoming.recv_bytes(maxlength=limit))
            try:
                if request["op"] == "close":
                    break
                header, payload = state.dispatch(request, incoming_payload)
                outgoing.send_bytes(_encode(header, payload, limit))
            except AdbcError as error:
                _send_error(outgoing, error, limit)
            except Exception:  # noqa: BLE001 -- never expose arbitrary worker errors
                _send_error(outgoing, AdbcError("Isolated worker operation failed", "internal"), limit)
    except (EOFError, BrokenPipeError, OSError):
        pass
    except AdbcError as error:
        with contextlib.suppress(BrokenPipeError, OSError):
            _send_error(outgoing, error, limit)
    except Exception:  # noqa: BLE001 -- never expose arbitrary worker errors
        with contextlib.suppress(BrokenPipeError, OSError):
            _send_error(outgoing, AdbcError("Isolated worker startup failed", "internal"), limit)
    finally:
        try:
            if state is not None:
                state.close()
            if connection is not None:
                connection.close()
            # For a graceful close, cleanup finishes before acknowledgment.
            outgoing.send_bytes(_encode({"closed": True}, b"", limit))
        except Exception:  # noqa: BLE001, S110 -- exit cleanup without logging secrets
            pass
        finally:
            incoming.close()
            outgoing.close()


class _RemoteIterator(Iterator[pa.RecordBatch]):
    def __init__(self, connection: _ProcessConnection, rid: str) -> None:
        """Initialize configured state and resource ownership."""
        self.connection = connection
        self.rid = rid
        self.closed = False

    def __iter__(self) -> _RemoteIterator:
        """Return this lazy batch iterator."""
        return self

    def __next__(self) -> pa.RecordBatch:
        """Read the next batch or signal exhaustion."""
        if self.closed:
            raise StopIteration
        header, payload = self.connection._rpc({"op": "fetch", "result_id": self.rid})
        if header.get("eof"):
            self.closed = True
            raise StopIteration
        return pa.ipc.open_stream(payload).read_next_batch()

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        if not self.closed:
            self.closed = True
            if self.connection._failure is None:
                self.connection._rpc({"op": "release", "result_id": self.rid})


class _ProcessConnection(Connection):
    def __init__(
        self,
        factory: str,
        options_json: str,
        principal: str,
        timeout: float,
        startup_timeout: float,
        limit: int,
        max_results: int,
        max_statements: int = 32,
        bind_limit: int = 64 * 1024 * 1024,
        connection_options_json: str = '{"database":{},"connection":{}}',
    ) -> None:
        """Initialize configured state and resource ownership."""
        self.timeout = timeout
        self.limit = limit
        self.bind_limit = bind_limit
        self._io_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._failure: str | None = None
        self._process_closed = False
        context = multiprocessing.get_context("spawn")
        child_read, self._send = context.Pipe(duplex=False)
        self._receive, child_write = context.Pipe(duplex=False)
        self._process = context.Process(
            target=_child_main,
            args=(
                child_read,
                child_write,
                factory,
                options_json,
                principal,
                limit,
                max_results,
                max_statements,
                bind_limit,
                connection_options_json,
            ),
            daemon=True,
        )
        try:
            self._process.start()
        except Exception:  # noqa: BLE001 -- sanitize process startup errors
            child_read.close()
            child_write.close()
            self._send.close()
            self._receive.close()
            raise AdbcError("Cannot start isolated worker", "internal") from None
        child_read.close()
        child_write.close()
        try:
            self._exchange(None, startup_timeout)
        except Exception:
            self._stop("invalid_state")
            self._send.close()
            self._receive.close()
            raise

    def _stop(self, status: str) -> None:
        with self._state_lock:
            if self._failure is not None:
                return
            self._failure = status
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=0.2)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(timeout=0.5)
            if not self._process.is_alive():
                self._process.close()
                self._process_closed = True
            # Do not close pipe objects underneath a recv/send in another thread.
            # Terminating the child closes its pipe ends and wakes that thread.

    def _exchange(
        self, request: Mapping[str, Any] | None, timeout: float, outgoing: bytes = b""
    ) -> tuple[dict[str, Any], bytes]:
        if self._failure is not None:
            raise AdbcError("Isolated connection is unavailable", self._failure)
        packet = None if request is None else _encode(request, outgoing, self.limit)
        timer = threading.Timer(timeout, self._stop, args=("timeout",))
        timer.daemon = True
        timer.start()
        try:
            if packet is not None:
                self._send.send_bytes(packet)
            data = self._receive.recv_bytes(maxlength=self.limit)
            timer.cancel()
            timer.join()
            if self._failure is not None:
                raise AdbcError("Isolated operation terminated", self._failure)
            header, payload = _decode(data)
            if "error" in header:
                error = header["error"]
                raise AdbcError(
                    error["message"],
                    error["status"],
                    sqlstate=bytes(value % 256 for value in error["sqlstate"]).decode("ascii"),
                    vendor_code=error["vendor_code"],
                    details={key: base64.b64decode(value) for key, value in error["details"]},
                )
            return header, payload
        except (EOFError, OSError):
            self._stop("io")
            raise AdbcError("Isolated worker stopped", self._failure or "io") from None
        finally:
            timer.cancel()
            timer.join()
            if self._failure is not None:
                self._send.close()
                self._receive.close()

    def _rpc(
        self, request: Mapping[str, Any], payload: bytes = b"", *, timeout: float | None = None
    ) -> tuple[dict[str, Any], bytes]:
        wait = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + wait
        if wait <= 0 or not self._io_lock.acquire(timeout=wait):
            raise AdbcError("Isolated connection is busy", "timeout")
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AdbcError("Isolated operation deadline expired", "timeout")
            return self._exchange(request, remaining, payload)
        finally:
            self._io_lock.release()

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return its schema and lazy batch iterator."""
        return self._query({"op": "execute", "sql": sql})

    def _query(self, request: Mapping[str, Any], incoming: bytes = b"") -> QueryResult:
        header, payload = self._rpc(request, incoming)
        return QueryResult(
            pa.ipc.open_stream(payload).schema,
            _RemoteIterator(self, header["result_id"]),
            header["rows_affected"],
        )

    def execute_schema(self, sql: str) -> pa.Schema:
        """Infer the result schema without opening a cursor."""
        _, payload = self._rpc({"op": "schema", "sql": sql})
        return pa.ipc.open_stream(payload).schema

    def new_statement(self) -> Statement:
        """Create a child-owned statement within the configured handle quota."""
        header, _ = self._rpc({"op": "new_statement"})
        return _ProcessStatement(self, header["statement_id"])

    def set_option(self, key: str, value: OptionValue) -> None:
        """Set a typed connection option in the worker process."""
        self._rpc({"op": "set_option", "key": key, "option": _option(value, self.limit)})

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Read a typed connection option without losing byte values."""
        header, _ = self._rpc({"op": "get_option", "key": key, "value_type": value_type})
        return _option_value(header["option"])

    def commit(self) -> None:
        """Commit the child connection's active transaction."""
        self._rpc({"op": "commit"})

    def rollback(self) -> None:
        """Roll back the child connection's active transaction."""
        self._rpc({"op": "rollback"})

    def get_info(self, codes: list[int] | None) -> QueryResult:
        """Return lazy metadata batches from the child connection."""
        return self._query({"op": "get_info", "codes": codes})

    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult:
        """Return lazy catalog metadata with the requested filters."""
        return self._query(
            {
                "op": "get_objects",
                "arguments": {
                    "depth": depth,
                    "catalog": catalog,
                    "db_schema": db_schema,
                    "table_name": table_name,
                    "table_types": table_types,
                    "column_name": column_name,
                },
            }
        )

    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema:
        """Return one table's schema from the child connection."""
        _, payload = self._rpc(
            {
                "op": "get_table_schema",
                "arguments": {
                    "catalog": catalog,
                    "db_schema": db_schema,
                    "table_name": table_name,
                },
            }
        )
        return pa.ipc.open_stream(payload).schema

    def get_table_types(self) -> QueryResult:
        """Return lazy supported-table-type metadata."""
        return self._query({"op": "get_table_types"})

    def get_statistic_names(self) -> QueryResult:
        """Return lazy supported-statistic-name metadata."""
        return self._query({"op": "get_statistic_names"})

    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult:
        """Return lazy statistics using the caller's approximation preference."""
        return self._query(
            {
                "op": "get_statistics",
                "arguments": {
                    "catalog": catalog,
                    "db_schema": db_schema,
                    "table_name": table_name,
                    "approximate": approximate,
                },
            }
        )

    def read_partition(self, partition: bytes) -> QueryResult:
        """Read one opaque partition through bounded IPC and lazy batch pulls."""
        return self._query({"op": "read_partition"}, partition)

    def cancel(self) -> None:
        """Invalidate this connection and terminate its worker process."""
        self._stop("cancelled")
        if self._io_lock.acquire(blocking=False):
            try:
                self._send.close()
                self._receive.close()
            finally:
                self._io_lock.release()

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        try:
            if self._failure is None:
                self._rpc({"op": "close"})
        finally:
            self._stop("invalid_state")
            if self._io_lock.acquire(blocking=False):
                try:
                    self._send.close()
                    self._receive.close()
                finally:
                    self._io_lock.release()


class _ProcessStatement(Statement):
    def __init__(self, connection: _ProcessConnection, sid: str) -> None:
        self.connection = connection
        self.sid = sid
        self.closed = False

    def _request(self, operation: str, **values: Any) -> dict[str, Any]:
        if self.closed:
            raise AdbcError("Isolated statement is closed", "invalid_state")
        return {"op": operation, "statement_id": self.sid, **values}

    def set_sql_query(self, sql: str) -> None:
        """Replace the worker statement's SQL text."""
        self.connection._rpc(self._request("statement_sql", sql=sql))

    def set_substrait_plan(self, payload: bytes) -> None:
        """Pass a bounded Substrait plan to the worker statement."""
        self.connection._rpc(self._request("statement_substrait"), payload)

    def prepare(self) -> None:
        """Prepare the worker statement without executing it."""
        self.connection._rpc(self._request("statement_prepare"))

    def bind(self, batch: pa.RecordBatch) -> None:
        """Transfer one parameter batch within the isolated message limit."""
        payload = _arrow_bytes(batch.schema, batch, self.connection.limit - 1024)
        self.connection._rpc(self._request("statement_bind"), payload)

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Spool a reader into bounded anonymous child storage and transfer ownership.

        Each batch must fit one IPC message; the complete stream must fit
        max_bind_bytes. The input reader closes on both success and error.
        Child files and readers close on replacement, statement close, error,
        or process termination. The upload has one total callback deadline.

        Args:
            reader: Trusted local Arrow reader whose batches are consumed once.
        """
        deadline = time.monotonic() + self.connection.timeout
        started = False
        try:
            payload = _arrow_bytes(reader.schema, None, self.connection.limit - 1024)
            self.connection._rpc(self._request("bind_begin"), payload, timeout=deadline - time.monotonic())
            started = True
            for batch in reader:
                payload = _arrow_bytes(batch.schema, batch, self.connection.limit - 1024)
                self.connection._rpc(self._request("bind_batch"), payload, timeout=deadline - time.monotonic())
            self.connection._rpc(self._request("bind_finish"), timeout=deadline - time.monotonic())
        except Exception:
            if started and self.connection._failure is None:
                with contextlib.suppress(Exception):
                    self.connection._rpc(self._request("bind_abort"))
            raise
        finally:
            reader.close()

    def execute(self) -> QueryResult:
        """Execute this statement and retain its lazy result in the child."""
        return self.connection._query(self._request("statement_execute"))

    def execute_update(self) -> int | None:
        """Execute an update and preserve an unknown affected-row count."""
        header, _ = self.connection._rpc(self._request("statement_update"))
        value: int | None = header["rows_affected"]
        return value

    def execute_schema(self) -> pa.Schema:
        """Infer this statement's result schema without opening a cursor."""
        _, payload = self.connection._rpc(self._request("statement_schema"))
        return pa.ipc.open_stream(payload).schema

    def get_parameter_schema(self) -> pa.Schema:
        """Return the worker statement's prepared-parameter schema."""
        _, payload = self.connection._rpc(self._request("statement_parameter_schema"))
        return pa.ipc.open_stream(payload).schema

    def execute_partitions(self) -> PartitionedResult:
        """Return up to 1024 partition descriptors within one bounded message."""
        header, payload = self.connection._rpc(self._request("statement_partitions"))
        end = header["schema_bytes"]
        schema = pa.ipc.open_stream(payload[:end]).schema
        partitions = []
        for size in header["partition_bytes"]:
            partitions.append(payload[end : end + size])
            end += size
        return PartitionedResult(schema, partitions, header["rows_affected"])

    def set_option(self, key: str, value: OptionValue) -> None:
        """Set a typed worker statement option, including ingestion settings."""
        self.connection._rpc(
            self._request("statement_set_option", key=key, option=_option(value, self.connection.limit))
        )

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Read a typed worker statement option."""
        header, _ = self.connection._rpc(self._request("statement_get_option", key=key, value_type=value_type))
        return _option_value(header["option"])

    def cancel(self) -> None:
        """Terminate the owning process; every handle on this connection becomes invalid."""
        if self.closed:
            raise AdbcError("Isolated statement is closed", "invalid_state")
        self.connection.cancel()

    def close(self) -> None:
        """Release child results, the backend statement, and its parameter spool."""
        if not self.closed:
            try:
                if self.connection._failure is None:
                    self.connection._rpc(self._request("close_statement"))
            finally:
                self.closed = True


class IsolatedWorker(Worker):
    """Run a trusted worker factory in a separate process for each connection.

    The factory is an importable module:attribute, called with JSON-compatible
    worker_options. Both opening and callbacks have finite deadlines. A deadline
    or cancellation kills that connection's process; no state is reconstructed.
    Pipes carry capped JSON/Arrow messages, not pickled result data. Configure
    service session quotas to bound process count and OS/container limits to
    bound arbitrary worker allocations before a batch reaches the IPC boundary.
    Each statement may retain one bounded parameter spool and one replacement
    upload. Child disk use is therefore at most twice max_statements times
    max_bind_bytes, excluding allocations and files created by the backend.
    Partitioned execution has a hard limit of 1024 opaque descriptors and the
    same complete-message byte cap as every other IPC response.
    """

    def __init__(
        self,
        factory: str,
        *,
        target: str = "default",
        timeout_seconds: float = 5,
        startup_timeout_seconds: float = 10,
        max_message_bytes: int = 2 * 1024 * 1024,
        max_results: int = 32,
        max_statements: int = 32,
        max_bind_bytes: int = 64 * 1024 * 1024,
        worker_options: Mapping[str, object] | None = None,
    ) -> None:
        """Configure a factory and finite process/IPC boundaries.

        Args:
            factory: Importable module:attribute naming a trusted worker factory.
            target: Public Grainlift target name served by this factory.
            timeout_seconds: Deadline for each execution, fetch, or cleanup call.
            startup_timeout_seconds: Deadline to import and connect a child worker.
            max_message_bytes: Maximum complete JSON/Arrow pipe message size.
            max_results: Maximum live result cursors retained in each child.
            max_statements: Maximum live backend statements in each child.
            max_bind_bytes: Maximum Arrow stream bytes in each anonymous parameter spool.
            worker_options: JSON-compatible keyword arguments for the factory.
        """
        for value in (timeout_seconds, startup_timeout_seconds):
            if type(value) not in (int, float) or value <= 0 or not math.isfinite(value):
                raise ValueError("Isolated deadlines must be finite and positive")
        if type(max_message_bytes) is not int or max_message_bytes < 4096:
            raise ValueError("Isolated message limit must be an integer >= 4096")
        if type(max_results) is not int or max_results < 1:
            raise ValueError("Isolated result limit must be a positive integer")
        if type(max_statements) is not int or max_statements < 1:
            raise ValueError("Isolated statement limit must be a positive integer")
        if type(max_bind_bytes) is not int or max_bind_bytes < 1:
            raise ValueError("Isolated binding limit must be a positive integer")
        if ":" not in factory or not all(factory.split(":", 1)):
            raise ValueError("Worker factory must be module:attribute")
        self.factory = factory
        self.target = target
        self.timeout = timeout_seconds
        self.startup_timeout = startup_timeout_seconds
        self.limit = max_message_bytes
        self.max_results = max_results
        self.max_statements = max_statements
        self.max_bind_bytes = max_bind_bytes
        self.options_json = json.dumps(worker_options or {}, allow_nan=False)
        if len(self.options_json.encode()) > max_message_bytes:
            raise ValueError("Worker options exceed message limit")

    def connect(self, principal: str) -> _ProcessConnection:
        """Open a connection bound to the authenticated principal."""
        return self.open_connection(principal, {}, {})

    def open_connection(
        self, principal: str, database_options: Mapping[str, OptionValue], connection_options: Mapping[str, OptionValue]
    ) -> _ProcessConnection:
        """Initialize a child with bounded typed database and connection options."""
        configured = {
            "database": {key: _option(value, self.limit) for key, value in database_options.items()},
            "connection": {key: _option(value, self.limit) for key, value in connection_options.items()},
        }
        _encode(configured, b"", self.limit)
        return _ProcessConnection(
            self.factory,
            self.options_json,
            principal,
            self.timeout,
            self.startup_timeout,
            self.limit,
            self.max_results,
            self.max_statements,
            self.max_bind_bytes,
            json.dumps(configured),
        )
