# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""WSGI portability and supervised Granian lifecycle regressions."""

import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from http.client import HTTPConnection
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from wsgiref.types import StartResponse

import httpx2
import pyarrow as pa
import pytest
from test_service import TestConnection, TestWorker
from test_tcp import eventually, open_connection
from vgi_rpc.http import http_connect

from grainlift import QueryResult
from grainlift._wsgi import PrimedResponse
from grainlift.protocol import Grainlift


@pytest.mark.parametrize("consume", [False, True])
def test_priming_bounds_prefetch_and_preserves_context(consume: bool) -> None:
    """Preserve headers, bytes and cleanup even if another thread closes early."""
    private = ContextVar("test_private", default=False)
    pulled: list[int] = []
    closed: list[bool] = []

    def app(environ: dict[str, Any], start: StartResponse) -> Iterator[bytes]:
        token = private.set(True)
        try:
            start("401 Unauthorized", [("Content-Type", "text/plain")])
            for index in range(3):
                assert private.get()
                pulled.append(index)
                yield bytes([index])
        finally:
            private.reset(token)
            closed.append(True)

    start = Mock()
    response = PrimedResponse(app, {}, start)
    start.assert_called_once_with("401 Unauthorized", [("Content-Type", "text/plain")])
    assert pulled == [0]
    assert not private.get()
    with ThreadPoolExecutor(1) as pool:
        if consume:
            assert pool.submit(list, response).result() == [b"\x00", b"\x01", b"\x02"]
        pool.submit(response.close).result()
    response.close()
    assert closed == [True]
    assert pulled == ([0, 1, 2] if consume else [0])
    assert not private.get()


def test_priming_failure_closes_response() -> None:
    """A failure before the first chunk must close the original iterable."""
    source = Mock()
    source.__iter__ = Mock(return_value=source)
    source.__next__ = Mock(side_effect=RuntimeError("failed first chunk"))
    with pytest.raises(RuntimeError, match="failed first chunk"):
        PrimedResponse(Mock(return_value=source), {}, Mock())
    source.close.assert_called_once()


def test_wsgi_disconnect_serializes_with_an_active_iteration() -> None:
    """A host closing on another thread must not re-enter the response's context."""
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()

    def app(environ: dict[str, Any], start: StartResponse) -> Iterator[bytes]:
        try:
            start("200 OK", [])
            yield b"first"
            entered.set()
            assert release.wait(2)
            yield b"second"
        finally:
            closed.set()

    response = PrimedResponse(app, {}, Mock())
    assert next(response) == b"first"
    with ThreadPoolExecutor(2) as pool:
        advancing = pool.submit(next, response)
        assert entered.wait(1)
        closing = pool.submit(response.close)
        try:
            with pytest.raises(TimeoutError):
                closing.result(timeout=0.02)
        finally:
            release.set()
        assert advancing.result(timeout=1) == b"second"
        closing.result(timeout=1)
    assert closed.is_set()


class HostingConnection(TestConnection):
    """Record child ownership and cleanup and inject cooperative or stuck work."""

    def __init__(self, batches: list[pa.RecordBatch], marker: Path) -> None:
        """Retain the fixture's finite batches and marker directory."""
        super().__init__(batches)
        self.marker = marker
        (marker / "pid").write_text(str(os.getpid()))

    def execute(self, sql: str) -> QueryResult:
        """Run a bounded query or inject a stalled callback for shutdown tests."""
        if sql in ("pause", "hang"):
            (self.marker / "started").touch()
            time.sleep(0.3 if sql == "pause" else 30)
        return super().execute(sql)

    def close(self) -> None:
        """Record backend cleanup from the owning serving process."""
        (self.marker / "closed").touch()
        super().close()


class HostingWorker(TestWorker):
    """Importable child-side factory for the public Granian entry point."""

    def __init__(self, marker: str) -> None:
        """Retain a test-owned directory, not a live service object."""
        super().__init__()
        self.marker = Path(marker)

    def connect(self, principal: str) -> HostingConnection:
        """Create a connection inside the serving child."""
        return HostingConnection(self.batches, self.marker)


@pytest.fixture
def granian_host(tmp_path: Path) -> Iterator[tuple[subprocess.Popen[str], int, Path]]:
    """Start the public host with a bounded supervisor and dispose all processes."""
    pytest.importorskip("granian")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    program = """
import sys
from grainlift import Limits, serve_granian
if __name__ == '__main__':
    serve_granian('test_hosting:HostingWorker', tokens={'test-token': 'alice'},
        worker_options={'marker': sys.argv[1]}, port=int(sys.argv[2]), threads=2,
        backpressure=4, shutdown_seconds=3, limits=Limits(shutdown_seconds=1))
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parent)}
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(tmp_path), str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while True:
            if process.poll() is not None:
                pytest.fail("Host exited before readiness: " + str(process.communicate()))
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                assert time.monotonic() < deadline
                time.sleep(0.05)
        yield process, port, tmp_path
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            output = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
            pytest.fail("Granian supervisor exceeded shutdown deadline")
        assert "SECRET" not in "".join(output)


def test_granian_authentication_body_limits_and_session_cleanup(
    granian_host: tuple[subprocess.Popen[str], int, Path],
) -> None:
    """Use a real Granian listener with ordinary SDK auth, quotas and cleanup."""
    process, port, marker = granian_host
    client = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        client.request("POST", "/", body=b"invalid")
        response = client.getresponse()
        assert response.status == 401
        response.read()
        headers = {"Authorization": "Bearer test-token", "Content-Type": "application/vnd.apache.arrow.stream"}
        for size in (2 * 1024 * 1024 - 1, 2 * 1024 * 1024, 2 * 1024 * 1024 + 1):
            client.request("POST", "/org.queryfarm.Grainlift.v1/open_connection", body=b"x" * size, headers=headers)
            response = client.getresponse()
            assert response.status == (413 if size > 2 * 1024 * 1024 else 400)
            response.read()
    finally:
        client.close()
    with (
        httpx2.Client(base_url=f"http://127.0.0.1:{port}", headers={"Authorization": "Bearer test-token"}) as http,
        http_connect(Grainlift, client=http) as rpc,  # type: ignore[type-abstract]
    ):
        sid = open_connection(rpc)
        statement = rpc.new_statement(session_id=sid).statement_id
        rpc.set_sql_query(session_id=sid, statement_id=statement, sql="SECRET query")
        result = rpc.execute(session_id=sid, statement_id=statement).result_id
        with rpc.read_result(session_id=sid, result_id=result, sequence=0) as stream:
            assert sum(item.batch.num_rows for item in stream) == 3
        # Abandon the logical session; host shutdown must close it.
    assert int((marker / "pid").read_text()) != process.pid
    process.terminate()
    assert process.wait(timeout=8) == 0
    assert (marker / "closed").exists()


@pytest.mark.parametrize("query", ["pause", "hang"])
def test_granian_shutdown_during_backend_callback(
    granian_host: tuple[subprocess.Popen[str], int, Path],
    query: str,
) -> None:
    """Drain cooperative callbacks and bound shutdown even when a callback never returns."""
    process, port, marker = granian_host

    def execute() -> bool:
        try:
            with (
                httpx2.Client(
                    base_url=f"http://127.0.0.1:{port}", timeout=7, headers={"Authorization": "Bearer test-token"}
                ) as http,
                http_connect(Grainlift, client=http) as rpc,  # type: ignore[type-abstract]
            ):
                sid = open_connection(rpc)
                statement = rpc.new_statement(session_id=sid).statement_id
                rpc.set_sql_query(session_id=sid, statement_id=statement, sql=query)
                rpc.execute(session_id=sid, statement_id=statement)
                return True
        except Exception:
            return False

    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(execute)
        eventually(lambda: (marker / "started").exists())
        started = time.monotonic()
        process.terminate()
        process.wait(timeout=8)
        assert time.monotonic() - started < 7
        if query == "pause":
            assert future.result(timeout=2)
            assert (marker / "closed").exists()
        else:
            assert not future.result(timeout=2)
    serving_pid = int((marker / "pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(serving_pid, 0)


def test_granian_shutdown_with_slow_request_body(granian_host: tuple[subprocess.Popen[str], int, Path]) -> None:
    """The supervisor deadline bounds shutdown while a client stalls mid-body."""
    process, port, _ = granian_host
    with socket.create_connection(("127.0.0.1", port), timeout=5) as peer:
        peer.sendall(
            b"POST /org.queryfarm.Grainlift.v1/open_connection HTTP/1.1\r\nHost: localhost\r\n"
            b"Authorization: Bearer test-token\r\nContent-Length: 1024\r\n\r\nx"
        )
        process.terminate()
        process.wait(timeout=8)
