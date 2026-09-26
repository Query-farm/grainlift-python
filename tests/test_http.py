# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Exercise the actual HTTP app's auth and message-size boundaries."""

import logging
from io import BytesIO
from wsgiref.types import WSGIApplication

import falcon.testing
import pyarrow as pa
import pytest
import waitress
from test_service import TestConnection, TestWorker, value
from vgi_rpc import AnnotatedBatch, RpcError
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._wire import _write_request
from waitress.adjustments import Adjustments
from waitress.parser import HTTPRequestParser

from grainlift import Limits, Service, serve
from grainlift.protocol import Grainlift, schema_ipc


def test_continuation_checks_principal() -> None:
    """Verify continuation checks principal."""
    with Service(TestWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"alice-token": "alice", "bob-token": "bob"}),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer alice-token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            sid = value(
                rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]"),
                "session_id",
            )
            stmt = value(rpc.new_statement(session_id=sid), "statement_id")
            rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="query")
            rid = value(rpc.execute(session_id=sid, statement_id=stmt), "result_id")
            stream = rpc.read_result(session_id=sid, result_id=rid, sequence=0)
            batches = iter(stream)
            assert next(batches).batch.num_rows == 2
            client._default_headers["Authorization"] = "Bearer bob-token"
            with pytest.raises(RpcError, match="signature verification failed"):
                next(batches)
            client._default_headers["Authorization"] = "Bearer alice-token"
            rpc.close_connection(session_id=sid)


def test_unsupported_bind_has_adbc_error() -> None:
    """Verify unsupported bind has adbc error."""
    with Service(TestWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            sid = value(
                rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]"),
                "session_id",
            )
            stmt = value(rpc.new_statement(session_id=sid), "statement_id")
            with (
                pytest.raises(RpcError, match="not_implemented"),
                rpc.bind(
                    session_id=sid,
                    statement_id=stmt,
                    schema_ipc=schema_ipc(pa.schema([])),
                ) as stream,
            ):
                empty = pa.RecordBatch.from_pydict({}, schema=pa.schema([]))
                stream.exchange(AnnotatedBatch(empty))
                stream.exchange(AnnotatedBatch(empty, pa.KeyValueMetadata({b"GRAINLIFT:bind_finish": b"1"})))


def test_transport_logs_are_suppressed(caplog: pytest.LogCaptureFixture) -> None:
    """Verify transport logs are suppressed."""

    class LoggingWorker(TestWorker):
        """Emit sensitive diagnostics during connection creation."""

        def connect(self, principal: str) -> TestConnection:
            """Open a connection bound to the authenticated principal."""
            logging.getLogger("vgi_rpc.rpc").error("SECRET downstream exception")
            logging.getLogger("vgi_rpc.created_during_request").error("SECRET dynamically created logger")
            return super().connect(principal)

    logger = logging.getLogger("vgi_rpc.rpc")
    before = (logger.disabled, logger.level, logger.propagate, list(logger.handlers))
    caplog.set_level(logging.DEBUG)
    with Service(LoggingWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer token"},
        )
        assert (logger.disabled, logger.level, logger.propagate, list(logger.handlers)) == before
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]")
        assert "SECRET" not in caplog.text
        logger.error("unrelated application remains observable")
        assert "unrelated application remains observable" in caplog.text
        assert any(record.name == "grainlift.access" for record in caplog.records)


def test_isolated_worker_through_wsgi() -> None:
    """Verify isolated worker through wsgi."""
    from grainlift import IsolatedWorker

    worker = IsolatedWorker("test_isolation:ProcessTestWorker", startup_timeout_seconds=10)
    with Service(worker) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            sid = value(
                rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]"),
                "session_id",
            )
            stmt = value(rpc.new_statement(session_id=sid), "statement_id")
            rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="ok")
            rid = value(rpc.execute(session_id=sid, statement_id=stmt), "result_id")
            with rpc.read_result(session_id=sid, result_id=rid, sequence=0) as stream:
                assert [item.batch.column(0).to_pylist() for item in stream] == [[0, 1, 2]]
            rpc.cancel_connection(session_id=sid)
            with pytest.raises(RpcError, match="cancelled"):
                rpc.execute(session_id=sid, statement_id=stmt)
            rpc.close_connection(session_id=sid)
            assert not service._sessions


def test_private_handler_filters_refresh_between_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Filter newly attached transport handlers even when their ancestor logger was already filtered."""
    records: list[logging.LogRecord] = []

    class RecordingHandler(logging.Handler):
        """Record messages that reach a custom transport logging destination."""

        def emit(self, record: logging.LogRecord) -> None:
            """Retain a record for the privacy assertion."""
            records.append(record)

    class LoggingWorker(TestWorker):
        """Emit through a child logger created during request handling."""

        def connect(self, principal: str) -> TestConnection:
            """Open a connection after emitting a transport diagnostic."""
            logging.getLogger("vgi_rpc.refresh_parent.new_child").error("SECRET")
            return super().connect(principal)

    parent = logging.getLogger("vgi_rpc.refresh_parent")
    monkeypatch.setattr(parent, "propagate", False)
    with Service(LoggingWorker()) as service:
        app = service.app(tokens={"token": "alice"})
        monkeypatch.setattr(parent, "handlers", [RecordingHandler()])
        client = _SyncTestClient(
            app,  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={"Authorization": "Bearer token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # Reflect the protocol class.
            rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]")
        assert not records
        logging.getLogger("vgi_rpc.refresh_parent.new_child").error("public")
        assert [record.getMessage() for record in records] == ["public"]


@pytest.mark.parametrize("headroom", [1, 0, -1])
def test_http_request_limit(headroom: int) -> None:
    """Verify http request limit."""
    buf = BytesIO()
    _write_request(
        buf,
        "open_connection",
        rpc_methods(Grainlift)["open_connection"].params_schema,
        {"target": "default", "database_options_json": "[]", "connection_options_json": "[]"},
        protocol=Grainlift.protocol_name,
        protocol_version=Grainlift.protocol_version,
    )
    body = buf.getvalue()
    with Service(TestWorker(), limits=Limits(request_bytes=len(body) + headroom)) as service:
        client = falcon.testing.TestClient(service.app(tokens={"token": "alice"}))
        response = client.simulate_post(
            "/org.queryfarm.Grainlift.v1/open_connection",
            body=body,
            headers={
                "Authorization": "Bearer token",
                "Content-Type": "application/vnd.apache.arrow.stream",
            },
        )
        assert response.status_code == (413 if headroom < 0 else 200)
        assert len(service._sessions) == (0 if headroom < 0 else 1)


@pytest.mark.parametrize("headroom", [1, 0, -1])
def test_serve_preserves_inclusive_content_length_limit(monkeypatch: pytest.MonkeyPatch, headroom: int) -> None:
    """Exercise the real host parser and WSGI app at both sides of the configured body limit."""
    buf = BytesIO()
    _write_request(
        buf,
        "open_connection",
        rpc_methods(Grainlift)["open_connection"].params_schema,
        {"target": "default", "database_options_json": "[]", "connection_options_json": "[]"},
        protocol=Grainlift.protocol_name,
        protocol_version=Grainlift.protocol_version,
    )
    body = buf.getvalue()
    statuses: list[int] = []

    def host(app: WSGIApplication, *, host: str, port: int, max_request_body_size: int) -> None:
        assert host == "127.0.0.1"
        assert port == 8080
        parser = HTTPRequestParser(Adjustments(max_request_body_size=max_request_body_size))
        incoming = (
            f"POST /org.queryfarm.Grainlift.v1/open_connection HTTP/1.1\r\n"
            f"Host: localhost\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        while incoming and not parser.completed:
            consumed = parser.received(incoming)
            assert consumed > 0
            incoming = incoming[consumed:]
        try:
            if parser.error is not None:
                statuses.append(parser.error.code)
            else:
                response = falcon.testing.TestClient(app).simulate_post(
                    "/org.queryfarm.Grainlift.v1/open_connection",
                    body=parser.get_body_stream().read(),
                    headers={
                        "Authorization": "Bearer token",
                        "Content-Type": "application/vnd.apache.arrow.stream",
                    },
                )
                statuses.append(response.status_code)
        finally:
            parser.close()

    monkeypatch.setattr(waitress, "serve", host)
    serve(TestWorker(), token="token", limits=Limits(request_bytes=len(body) + headroom))
    assert statuses == [413 if headroom < 0 else 200]
