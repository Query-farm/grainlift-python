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
from collections.abc import Buffer, Iterator, Mapping
from multiprocessing.connection import Connection as PipeConnection
from typing import Any

import pyarrow as pa

from .api import AdbcError, Connection, QueryResult, Worker


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


def _child_main(
    incoming: PipeConnection,
    outgoing: PipeConnection,
    factory: str,
    options_json: str,
    principal: str,
    limit: int,
    max_results: int,
) -> None:
    # Worker-authored stdout/stderr must not accidentally escape into service logs.
    with open(os.devnull, "wb") as sink:
        os.dup2(sink.fileno(), 1)
        os.dup2(sink.fileno(), 2)
    connection: Connection | None = None
    results: dict[str, QueryResult] = {}
    sequence = 0
    try:
        module, name = factory.split(":", 1)
        worker_type = getattr(importlib.import_module(module), name)
        worker: Worker = worker_type(**json.loads(options_json))
        connection = worker.connect(principal)
        outgoing.send_bytes(_encode({"ready": True}, b"", limit))
        while True:
            request, _ = _decode(incoming.recv_bytes(maxlength=limit))
            try:
                operation = request["op"]
                payload = b""
                header: dict[str, Any] = {"ok": True}
                if operation == "execute":
                    if len(results) >= max_results:
                        raise AdbcError("Isolated result limit reached", "invalid_state")
                    query = connection.execute(request["sql"])
                    try:
                        if query.rows_affected is not None and (
                            type(query.rows_affected) is not int or not -(2**63) <= query.rows_affected < 2**63
                        ):
                            raise AdbcError("Invalid affected row count", "invalid_data")
                        payload = _arrow_bytes(query.schema, None, limit - 1024)
                        sequence += 1
                        rid = str(sequence)
                        header = {"result_id": rid, "rows_affected": query.rows_affected}
                        # Validate the complete response before retaining its cursor.
                        _encode(header, payload, limit)
                    except Exception:
                        query.close()
                        raise
                    results[rid] = query
                elif operation == "schema":
                    payload = _arrow_bytes(connection.execute_schema(request["sql"]), None, limit - 1024)
                elif operation == "fetch":
                    query = results[request["result_id"]]
                    try:
                        batch = next(query.batches, None)
                        if batch is None:
                            results.pop(request["result_id"]).close()
                            header = {"eof": True}
                        else:
                            if not batch.schema.equals(query.schema, check_metadata=True):
                                raise AdbcError("Isolated result schema changed", "invalid_data")
                            payload = _arrow_bytes(query.schema, batch, limit - 1024)
                    except Exception:
                        failed = results.pop(request["result_id"], None)
                        if failed is not None:
                            failed.close()
                        raise
                elif operation == "release":
                    released = results.pop(request["result_id"], None)
                    if released is not None:
                        released.close()
                elif operation == "close":
                    break
                else:
                    raise AdbcError("Unknown isolated operation", "invalid_arguments")
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
            for query in results.values():
                with contextlib.suppress(Exception):  # Continue cleanup without logging worker secrets.
                    query.close()
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
    ) -> None:
        """Initialize configured state and resource ownership."""
        self.timeout = timeout
        self.limit = limit
        self._io_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._failure: str | None = None
        self._process_closed = False
        context = multiprocessing.get_context("spawn")
        child_read, self._send = context.Pipe(duplex=False)
        self._receive, child_write = context.Pipe(duplex=False)
        self._process = context.Process(
            target=_child_main,
            args=(child_read, child_write, factory, options_json, principal, limit, max_results),
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

    def _exchange(self, request: Mapping[str, Any] | None, timeout: float) -> tuple[dict[str, Any], bytes]:
        if self._failure is not None:
            raise AdbcError("Isolated connection is unavailable", self._failure)
        packet = None if request is None else _encode(request, b"", self.limit)
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

    def _rpc(self, request: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
        if not self._io_lock.acquire(timeout=self.timeout):
            raise AdbcError("Isolated connection is busy", "timeout")
        try:
            return self._exchange(request, self.timeout)
        finally:
            self._io_lock.release()

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return its schema and lazy batch iterator."""
        header, payload = self._rpc({"op": "execute", "sql": sql})
        return QueryResult(
            pa.ipc.open_stream(payload).schema,
            _RemoteIterator(self, header["result_id"]),
            header["rows_affected"],
        )

    def execute_schema(self, sql: str) -> pa.Schema:
        """Infer the result schema without opening a cursor."""
        _, payload = self._rpc({"op": "schema", "sql": sql})
        return pa.ipc.open_stream(payload).schema

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


class IsolatedWorker(Worker):
    """Run a trusted worker factory in a separate process for each connection.

    The factory is an importable module:attribute, called with JSON-compatible
    worker_options. Both opening and callbacks have finite deadlines. A deadline
    or cancellation kills that connection's process; no state is reconstructed.
    Pipes carry capped JSON/Arrow messages, not pickled result data. Configure
    service session quotas to bound process count and OS/container limits to
    bound arbitrary worker allocations before a batch reaches the IPC boundary.
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
            worker_options: JSON-compatible keyword arguments for the factory.
        """
        for value in (timeout_seconds, startup_timeout_seconds):
            if type(value) not in (int, float) or value <= 0 or not math.isfinite(value):
                raise ValueError("Isolated deadlines must be finite and positive")
        if type(max_message_bytes) is not int or max_message_bytes < 4096:
            raise ValueError("Isolated message limit must be an integer >= 4096")
        if type(max_results) is not int or max_results < 1:
            raise ValueError("Isolated result limit must be a positive integer")
        if ":" not in factory or not all(factory.split(":", 1)):
            raise ValueError("Worker factory must be module:attribute")
        self.factory = factory
        self.target = target
        self.timeout = timeout_seconds
        self.startup_timeout = startup_timeout_seconds
        self.limit = max_message_bytes
        self.max_results = max_results
        self.options_json = json.dumps(worker_options or {}, allow_nan=False)
        if len(self.options_json.encode()) > max_message_bytes:
            raise ValueError("Worker options exceed message limit")

    def connect(self, principal: str) -> _ProcessConnection:
        """Open a connection bound to the authenticated principal."""
        return _ProcessConnection(
            self.factory,
            self.options_json,
            principal,
            self.timeout,
            self.startup_timeout,
            self.limit,
            self.max_results,
        )
