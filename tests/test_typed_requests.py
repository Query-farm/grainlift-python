# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Typed request wire schemas, strict decoding, filters, and session ownership."""

from io import BytesIO
from typing import Any

import falcon.testing
import pyarrow as pa
import pytest
from test_service import context, open_session
from test_unary_features import FeatureStatement, FeatureWorker
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._wire import _write_request
from vgi_rpc.utils import ArrowSerializableDataclass

from grainlift import AdbcError, Service
from grainlift.options import NamedOption, WireOptionValue
from grainlift.protocol import (
    GetInfoRequest,
    GetObjectsRequest,
    GetStatisticsRequest,
    GetTableSchemaRequest,
    Grainlift,
    OpenConnectionRequest,
    SetConnectionOptionRequest,
    SetStatementOptionRequest,
)

OPTION_FIELDS: list[pa.Field[Any]] = [
    pa.field("kind", pa.string(), nullable=False),
    pa.field("string_value", pa.string()),
    pa.field("bytes_value", pa.binary()),
    pa.field("int_value", pa.int64()),
    pa.field("double_value", pa.float64()),
]
NAMED_OPTION = pa.struct(
    [pa.field("key", pa.string(), nullable=False), pa.field("value", pa.struct(OPTION_FIELDS), nullable=False)]
)
SESSION = pa.field("session_id", pa.string(), nullable=False)


def _ipc(batch: pa.RecordBatch, *, count: int = 1) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        for _ in range(count):
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


@pytest.mark.parametrize(
    ("record", "fields"),
    [
        (
            OpenConnectionRequest(target="default", database_options=[], connection_options=[]),
            [
                pa.field("target", pa.string(), nullable=False),
                pa.field("database_options", pa.list_(NAMED_OPTION), nullable=False),
                pa.field("connection_options", pa.list_(NAMED_OPTION), nullable=False),
            ],
        ),
        (
            SetConnectionOptionRequest(session_id="session", key="x", value=WireOptionValue(kind="int", int_value=1)),
            [
                SESSION,
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.struct(OPTION_FIELDS), nullable=False),
            ],
        ),
        (
            SetStatementOptionRequest(
                session_id="session", statement_id="stmt", key="x", value=WireOptionValue(kind="bytes", bytes_value=b"")
            ),
            [
                SESSION,
                pa.field("statement_id", pa.string(), nullable=False),
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.struct(OPTION_FIELDS), nullable=False),
            ],
        ),
        (GetInfoRequest(session_id="session", codes=None), [SESSION, pa.field("codes", pa.list_(pa.int64()))]),
        (
            GetObjectsRequest(
                session_id="session",
                depth=0,
                catalog=None,
                db_schema=None,
                table_name=None,
                table_types=None,
                column_name=None,
            ),
            [
                SESSION,
                pa.field("depth", pa.int64(), nullable=False),
                pa.field("catalog", pa.string()),
                pa.field("db_schema", pa.string()),
                pa.field("table_name", pa.string()),
                pa.field("table_types", pa.list_(pa.string())),
                pa.field("column_name", pa.string()),
            ],
        ),
        (
            GetTableSchemaRequest(session_id="session", catalog=None, db_schema=None, table_name="t"),
            [
                SESSION,
                pa.field("catalog", pa.string()),
                pa.field("db_schema", pa.string()),
                pa.field("table_name", pa.string(), nullable=False),
            ],
        ),
        (
            GetStatisticsRequest(session_id="session", catalog=None, db_schema=None, table_name=None, approximate=True),
            [
                SESSION,
                pa.field("catalog", pa.string()),
                pa.field("db_schema", pa.string()),
                pa.field("table_name", pa.string()),
                pa.field("approximate", pa.bool_(), nullable=False),
            ],
        ),
    ],
)
def test_request_schema_oracle_and_roundtrip(record: ArrowSerializableDataclass, fields: list[pa.Field[Any]]) -> None:
    """Pin every named field, nested option representation, and Arrow nullability."""
    assert record.ARROW_SCHEMA.equals(pa.schema(fields), check_metadata=True)
    assert type(record).deserialize_from_bytes(record.serialize_to_bytes()) == record


@pytest.mark.parametrize(
    "method",
    [
        "open_connection",
        "set_connection_option",
        "set_statement_option",
        "get_info",
        "get_objects",
        "get_table_schema",
        "get_statistics",
    ],
)
def test_complex_methods_have_only_standard_named_request_parameter(method: str) -> None:
    """Prevent old JSON arguments or proxy-specific serialization from reappearing."""
    expected = pa.schema([pa.field("request", pa.binary(), nullable=False)])
    assert rpc_methods(Grainlift)[method].params_schema.equals(expected, check_metadata=True)


@pytest.mark.parametrize(
    "malformation",
    [
        "empty",
        "truncated",
        "extra_batch",
        "trailing",
        "missing_eos",
        "zero_rows",
        "two_rows",
        "missing_field",
        "extra_field",
        "wrong_type",
        "nullable_handle",
        "null_handle",
        "null_code",
    ],
)
def test_strict_request_rejects_malformed_ipc(malformation: str) -> None:
    """Reject malformed framing, schema drift, null handles, and null list children."""
    schema = pa.schema([SESSION, pa.field("codes", pa.list_(pa.int64()))])
    data: dict[str, Any] = {"session_id": ["session"], "codes": [[0, 2**32 - 1]]}
    if malformation == "zero_rows":
        data = {"session_id": [], "codes": []}
    elif malformation == "two_rows":
        data = {"session_id": ["a", "b"], "codes": [None, []]}
    elif malformation == "missing_field":
        schema = pa.schema([SESSION])
        data = {"session_id": ["session"]}
    elif malformation == "extra_field":
        schema = schema.append(pa.field("extra", pa.int64()))
        data["extra"] = [1]
    elif malformation == "wrong_type":
        schema = pa.schema([SESSION, pa.field("codes", pa.list_(pa.string()))])
        data["codes"] = [["0"]]
    elif malformation == "nullable_handle":
        schema = pa.schema([pa.field("session_id", pa.string()), schema.field("codes")])
    elif malformation == "null_handle":
        data["session_id"] = [None]
    elif malformation == "null_code":
        data["codes"] = [[None]]
    payload = _ipc(pa.RecordBatch.from_pydict(data, schema=schema), count=2 if malformation == "extra_batch" else 1)
    if malformation == "empty":
        payload = b""
    elif malformation == "truncated":
        payload = payload[:32]
    elif malformation == "trailing":
        payload += b"trailing"
    elif malformation == "missing_eos":
        payload = payload[:-8]
    with pytest.raises((AdbcError, ValueError, pa.ArrowException)):
        GetInfoRequest.deserialize_from_bytes(payload)


@pytest.mark.parametrize("codes", [None, [], [0], [2**32 - 1]])
def test_information_codes_preserve_null_empty_and_u32_boundaries(codes: list[int] | None) -> None:
    """Forward null and empty lists distinctly, including unsigned 32-bit extrema."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        service.get_info(GetInfoRequest(session_id=sid, codes=codes), ctx)
        assert worker.connections[0].calls == [("info", codes)]


@pytest.mark.parametrize("types", [None, [], [""], ["TABLE", "VIEW"]])
def test_object_filters_preserve_null_empty_and_pattern_strings(types: list[str] | None) -> None:
    """Retain filters without replacing empty strings or lists with nulls."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        service.get_objects(
            GetObjectsRequest(
                session_id=sid, depth=3, catalog="", db_schema=None, table_name="_%", table_types=types, column_name=""
            ),
            ctx,
        )
        assert worker.connections[0].calls == [("objects", (3, "", None, "_%", types, ""))]


def test_duplicate_named_options_rejected_before_opening_backend() -> None:
    """Reject duplicate keys rather than silently overwriting initialization options."""
    worker = FeatureWorker()
    with Service(worker) as service, pytest.raises(AdbcError) as error:
        service.open_connection(
            OpenConnectionRequest(
                target="default",
                database_options=[],
                connection_options=[
                    NamedOption(key="x", value=WireOptionValue(kind="int", int_value=1)),
                    NamedOption(key="x", value=WireOptionValue(kind="int", int_value=2)),
                ],
            ),
            context(service),
        )
    assert error.value.status == "invalid_arguments" and not worker.connections


def test_session_ownership_applies_to_named_requests() -> None:
    """Extract handles from typed requests before dispatching any metadata or option callback."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, owner = open_session(service)
        stmt = service.new_statement(sid, owner).statement_id
        foreign = context(service, "bob")
        requests = [
            (
                "set_connection_option",
                SetConnectionOptionRequest(session_id=sid, key="x", value=WireOptionValue(kind="int", int_value=1)),
            ),
            (
                "set_statement_option",
                SetStatementOptionRequest(
                    session_id=sid, statement_id=stmt, key="x", value=WireOptionValue(kind="int", int_value=1)
                ),
            ),
            ("get_info", GetInfoRequest(session_id=sid, codes=None)),
            (
                "get_objects",
                GetObjectsRequest(
                    session_id=sid,
                    depth=0,
                    catalog=None,
                    db_schema=None,
                    table_name=None,
                    table_types=None,
                    column_name=None,
                ),
            ),
            ("get_table_schema", GetTableSchemaRequest(session_id=sid, catalog=None, db_schema=None, table_name="t")),
            (
                "get_statistics",
                GetStatisticsRequest(session_id=sid, catalog=None, db_schema=None, table_name=None, approximate=False),
            ),
        ]
        for method, request in requests:
            with pytest.raises(AdbcError) as error:
                getattr(service, method)(request, foreign)
            assert error.value.status == "not_found"
        assert not worker.connections[0].calls and not worker.connections[0].options
        assert not worker.connections[0].statements[0].options


def test_malformed_opening_ipc_over_http_does_not_allocate_connection() -> None:
    """Exercise the real transport boundary rather than only calling a decoder directly."""
    worker = FeatureWorker()
    body = BytesIO()
    _write_request(
        body,
        "open_connection",
        rpc_methods(Grainlift)["open_connection"].params_schema,
        {"request": b"private malformed request"},
        protocol=Grainlift.protocol_name,
        protocol_version=Grainlift.protocol_version,
    )
    with Service(worker) as service:
        client = falcon.testing.TestClient(service.app(tokens={"token": "alice"}))
        reply = client.simulate_post(
            "/org.queryfarm.Grainlift.v1/open_connection",
            body=body.getvalue(),
            headers={"Authorization": "Bearer token", "Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert not worker.connections and not service._sessions
        assert b"private malformed request" not in reply.content


@pytest.mark.parametrize("fails", [False, True])
def test_execute_schema_invalidates_prior_active_result(monkeypatch: pytest.MonkeyPatch, fails: bool) -> None:
    """Release the previous cursor before successful or failed schema execution."""

    def fail_schema(self: FeatureStatement) -> pa.Schema:
        raise AdbcError("Schema inference unavailable", "not_implemented")

    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = service.new_statement(sid, ctx).statement_id
        result = service.execute(sid, stmt, ctx)
        wrapper = service._sessions[sid].results[result.result_id]
        if fails:
            monkeypatch.setattr(FeatureStatement, "execute_schema", fail_schema)
            with pytest.raises(AdbcError, match="Schema inference unavailable"):
                service.execute_schema(sid, stmt, ctx)
        else:
            service.execute_schema(sid, stmt, ctx)
        assert wrapper.closed and result.result_id not in service._sessions[sid].results
        with pytest.raises(AdbcError) as error:
            service.next_batch(sid, result.result_id, 0, ctx)
        assert error.value.status == "not_found"
