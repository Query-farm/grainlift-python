# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Large requests and results through S3-compatible object storage."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from socketserver import ThreadingMixIn
from typing import Any, cast
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import pyarrow as pa
import pytest

from grainlift import Connection, ExternalStorageConfig, Limits, QueryResult, Service, Statement, Worker
from grainlift.cli import main
from grainlift.storage import Presigner

AWS_EXAMPLE = Presigner(
    "https://s3.amazonaws.com",
    "examplebucket",
    "us-east-1",
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    virtual_hosted_style=True,
)


def test_presign_matches_the_aws_documentation_example() -> None:
    """Sign the presigned GET from AWS's Signature Version 4 query-string documentation."""
    url = AWS_EXAMPLE.presign("GET", "test.txt", datetime(2013, 5, 24, tzinfo=UTC), 86400)
    assert url == (
        "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"
        "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    )


def test_path_style_urls_and_the_bucket_validator() -> None:
    """Name the bucket in the path and accept only that bucket's objects."""
    signer = Presigner("https://s3.amazonaws.com", "examplebucket", "us-east-1", "AK", "secret-canary")
    url = signer.presign("PUT", "grainlift/a b.arrow", datetime.now(UTC), 60)
    assert url.startswith("https://s3.amazonaws.com/examplebucket/grainlift/a%20b.arrow?")
    validate = signer.validator()
    validate("https://s3.amazonaws.com/examplebucket/grainlift/x.arrow?sig=1")
    for bad in (
        "https://s3.amazonaws.com/otherbucket/x.arrow",
        "http://s3.amazonaws.com/examplebucket/x.arrow",
        "https://169.254.169.254/examplebucket/x",
    ):
        with pytest.raises(ValueError):
            validate(bad)
    assert "secret-canary" not in repr(signer)


def test_config_validation_credentials_and_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject invalid fields, read credentials from AWS variables and never show the secret."""
    config = ExternalStorageConfig(
        endpoint="https://acct.r2.cloudflarestorage.com", bucket="b", access_key_id="AK", secret_access_key="canary"
    )
    assert (config.region, config.url_ttl_seconds, config.threshold_bytes) == ("auto", 900, 1024 * 1024)
    assert config.credentials() == ("AK", "canary")
    assert "canary" not in repr(config)
    valid: dict[str, Any] = {"endpoint": "https://e", "bucket": "b"}
    invalid: list[dict[str, Any]] = [
        {"endpoint": "ftp://x"},
        {"endpoint": "https://x?q=1"},
        {"bucket": " "},
        {"url_ttl_seconds": 0},
        {"url_ttl_seconds": 604801},
        {"threshold_bytes": 0},
        {"max_upload_bytes": -1},
    ]
    for changes in invalid:
        with pytest.raises(ValueError):
            ExternalStorageConfig(**(valid | changes))
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    unconfigured = ExternalStorageConfig(endpoint="https://e", bucket="b")
    with pytest.raises(ValueError, match="credentials"):
        unconfigured.credentials()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-ak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
    assert unconfigured.credentials() == ("env-ak", "env-sk")


def test_cli_requires_endpoint_and_bucket_together() -> None:
    """Refuse half an external storage configuration."""
    with pytest.raises(SystemExit):
        main(["serve", "test_service:TestWorker", "--storage-bucket", "b"])
    with pytest.raises(SystemExit):
        main(
            [
                "serve",
                "test_service:TestWorker",
                "--host",
                "mtls",
                "--storage-endpoint",
                "https://e",
                "--storage-bucket",
                "b",
            ]
        )


FIELDS: list[pa.Field[Any]] = [pa.field("i", pa.int64()), pa.field("v", pa.string())]
SCHEMA = pa.schema(FIELDS)


class StoringStatement(Statement):
    """Stores bound rows on ``store``; ``SELECT`` returns them in batches of whole rows."""

    def __init__(self, rows: list[pa.RecordBatch]) -> None:
        """Share the worker's row store."""
        self.rows = rows
        self.sql = ""
        self.bound: list[pa.RecordBatch] = []

    def set_sql_query(self, sql: str) -> None:
        """Remember the query."""
        self.sql = sql

    def bind(self, batch: pa.RecordBatch) -> None:
        """Retain one parameter batch."""
        self.bound = [batch]

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Retain every parameter batch."""
        self.bound = list(reader)

    def execute(self) -> QueryResult:
        """Store bound rows, or return the stored rows."""
        if self.sql == "store":
            self.rows.extend(self.bound)
            stored = sum(batch.num_rows for batch in self.bound)
            self.bound = []
            return QueryResult(SCHEMA, iter(()), rows_affected=stored)
        return QueryResult(SCHEMA, iter([pa.Table.from_batches(self.rows, SCHEMA).combine_chunks().to_batches()[0]]))

    def execute_update(self) -> int | None:
        """Store bound rows."""
        return self.execute().rows_affected


class StoringConnection(Connection):
    """Hands out statements over one row store."""

    def __init__(self, rows: list[pa.RecordBatch]) -> None:
        """Share the worker's row store."""
        self.rows = rows

    def new_statement(self) -> Statement:
        """Create a statement."""
        return StoringStatement(self.rows)


class StoringWorker(Worker):
    """Keeps rows across connections."""

    def __init__(self) -> None:
        """Start empty."""
        self.rows: list[pa.RecordBatch] = []

    def connect(self, principal: str) -> Connection:
        """Open a connection."""
        return StoringConnection(self.rows)


class ThreadingServer(ThreadingMixIn, WSGIServer):
    """A WSGI server that stops cleanly from another thread."""

    daemon_threads = True


class QuietHandler(WSGIRequestHandler):
    """Suppress access logging."""

    def log_message(self, format: str, *args: Any) -> None:
        """Discard the log line."""


@pytest.fixture
def s3() -> Iterator[tuple[str, Any]]:
    """Run a local S3 emulator with an empty bucket."""
    pytest.importorskip("moto")
    import boto3  # type: ignore[import-untyped]
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    client = boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    client.create_bucket(Bucket="grainlift-test")
    try:
        yield endpoint, client
    finally:
        server.stop()


def test_native_driver_uploads_requests_and_fetches_results_through_storage(s3: tuple[str, Any]) -> None:
    """Send rows larger than the request limit via upload URLs and read a large result back from the bucket."""
    driver = os.environ.get("GRAINLIFT_NATIVE_DRIVER")
    if not driver:
        pytest.skip("Set GRAINLIFT_NATIVE_DRIVER to the compiled Grainlift shared library")
    import adbc_driver_manager.dbapi as adbc

    endpoint, client = s3
    storage = ExternalStorageConfig(
        endpoint=endpoint,
        bucket="grainlift-test",
        region="us-east-1",
        prefix="grainlift/",
        access_key_id="test",
        secret_access_key="test",
    )
    limits = Limits(request_bytes=1024 * 1024, batch_bytes=32 * 1024 * 1024)
    worker = StoringWorker()
    with Service(worker, limits=limits) as service:
        app = service.app(tokens={"token": "alice"}, external_storage=storage)
        host = make_server("127.0.0.1", 0, app, server_class=ThreadingServer, handler_class=QuietHandler)
        thread = threading.Thread(target=host.serve_forever, daemon=True)
        thread.start()
        try:
            options = {
                "grainlift.uri": f"http://127.0.0.1:{host.server_port}",
                "grainlift.target": "default",
                "grainlift.auth.bearer_token": "token",
            }
            rows = pa.record_batch(
                [pa.array([0, 1, 2]), pa.array([chr(65 + i) * 3_000_000 for i in range(3)])], schema=SCHEMA
            )
            with (
                adbc.connect(
                    driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
                ) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("store", rows)
                # Consume each result. This host runs in the client's process, and
                # replacing an unconsumed result here waits ~30 s (the driver's
                # request timeout) before its close reaches the host; with the host
                # in another process the close is immediate.
                cursor.fetch_arrow_table()
                assert sum(batch.num_rows for batch in worker.rows) == 3
                cursor.execute("SELECT")
                table = cursor.fetch_arrow_table()
            assert table.column("i").to_pylist() == [0, 1, 2]
            values = cast(list[str], table.column("v").to_pylist())
            assert [(len(v), v[0], v[-1]) for v in values] == [(3_000_000, c, c) for c in "ABC"]
        finally:
            host.shutdown()
            host.server_close()
            thread.join(timeout=5)
    listing: dict[str, Any] = client.list_objects_v2(Bucket="grainlift-test")
    keys = [str(item["Key"]) for item in listing.get("Contents", [])]
    # At least one client upload (each 3 MB row exceeds the 1 MiB request limit) and one stored result.
    assert len(keys) >= 2
    assert all(key.startswith("grainlift/") and key.endswith(".arrow") for key in keys)


@contextmanager
def _native_host(limits: Limits, worker: Worker) -> Iterator[dict[str, str]]:
    """Serve ``worker`` over HTTP without object storage; yield native driver options."""
    with Service(worker, limits=limits) as service:
        app = service.app(tokens={"token": "alice"})
        host = make_server("127.0.0.1", 0, app, server_class=ThreadingServer, handler_class=QuietHandler)
        thread = threading.Thread(target=host.serve_forever, daemon=True)
        thread.start()
        try:
            yield {
                "grainlift.uri": f"http://127.0.0.1:{host.server_port}",
                "grainlift.target": "default",
                "grainlift.auth.bearer_token": "token",
            }
        finally:
            host.shutdown()
            host.server_close()
            thread.join(timeout=5)


def _native_driver() -> str:
    driver = os.environ.get("GRAINLIFT_NATIVE_DRIVER")
    if not driver:
        pytest.skip("Set GRAINLIFT_NATIVE_DRIVER to the compiled Grainlift shared library")
    return driver


def test_native_binds_are_limited_by_the_request_not_batch_bytes() -> None:
    """A bound row that fits a request is accepted even when it exceeds ``batch_bytes``."""
    driver = _native_driver()
    import adbc_driver_manager.dbapi as adbc

    worker = StoringWorker()
    # batch_bytes (1 MiB) is smaller than the 3 MB row; the 4 MiB request carries it.
    limits = Limits(request_bytes=4 * 1024 * 1024, batch_bytes=1024 * 1024)
    with (
        _native_host(limits, worker) as options,
        adbc.connect(driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("store", pa.record_batch([pa.array([7]), pa.array(["x" * 3_000_000])], schema=SCHEMA))
        cursor.fetch_arrow_table()
        # A service can return what it accepted, though the batch exceeds batch_bytes.
        cursor.execute("SELECT")
        table = cursor.fetch_arrow_table()
    assert [(row["i"], len(row["v"])) for batch in worker.rows for row in batch.to_pylist()] == [(7, 3_000_000)]
    values = cast(list[str], table.column("v").to_pylist())
    assert table.column("i").to_pylist() == [7]
    assert [len(v) for v in values] == [3_000_000]


def test_native_bind_streams_split_to_a_small_request_limit() -> None:
    """Many small rows over a 1 MiB request limit are split across bind turns."""
    driver = _native_driver()
    import adbc_driver_manager.dbapi as adbc

    worker = StoringWorker()
    rows = pa.record_batch(
        [pa.array(range(48)), pa.array([chr(65 + i % 26) * 65_536 for i in range(48)])], schema=SCHEMA
    )
    with (
        _native_host(Limits(request_bytes=1024 * 1024), worker) as options,
        adbc.connect(driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True) as conn,
        conn.cursor() as cursor,
    ):
        cursor.adbc_statement.set_sql_query("store")
        cursor.adbc_statement.bind_stream(pa.RecordBatchReader.from_batches(SCHEMA, [rows]))
        cursor.adbc_statement.execute_update()
    stored = pa.Table.from_batches(worker.rows, SCHEMA)
    assert len(worker.rows) > 1
    assert stored.column("i").to_pylist() == list(range(48))
    assert stored.column("v").to_pylist() == rows.column("v").to_pylist()


def test_native_row_larger_than_the_request_is_refused_without_storage() -> None:
    """Without object storage, a row larger than a request fails with a clear message."""
    driver = _native_driver()
    import adbc_driver_manager.dbapi as adbc

    worker = StoringWorker()
    with (
        _native_host(Limits(request_bytes=1024 * 1024), worker) as options,
        adbc.connect(driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True) as conn,
        conn.cursor() as cursor,
        pytest.raises(adbc.Error, match="bytes per request"),
    ):
        cursor.execute("store", pa.record_batch([pa.array([1]), pa.array(["x" * 2_000_000])], schema=SCHEMA))
    assert worker.rows == []
