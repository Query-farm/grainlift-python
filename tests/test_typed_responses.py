# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Nominal response types, stock VGI interoperability, and serialized size limits."""

from dataclasses import FrozenInstanceError, replace
from typing import Any

import pyarrow as pa
import pytest
from test_service import TestWorker, context, open_session
from test_unary_features import FeatureWorker
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.utils import ArrowSerializableDataclass, serialize_record_batch_bytes

from grainlift import AdbcError, Service
from grainlift.options import WireOptionValue
from grainlift.protocol import (
    UNARY_OUTPUT,
    ExecuteResponse,
    Grainlift,
    OkResponse,
    PartitionsResponse,
    SchemaResponse,
    SessionResponse,
    StatementResponse,
    UpdateResponse,
    ValueResponse,
)


@pytest.mark.parametrize(
    "response",
    [
        OkResponse(ok=True),
        SessionResponse(session_id="session"),
        StatementResponse(session_id="session", statement_id="statement"),
        ExecuteResponse(result_id="result", rows_affected=None, schema_ipc=b"schema"),
        SchemaResponse(schema_ipc=b"schema"),
        ValueResponse(value=WireOptionValue(kind="bytes", bytes_value=b"\x00\xff")),
        UpdateResponse(rows_affected=None),
        PartitionsResponse(rows_affected=-1, schema_ipc=b"schema", partitions=[b"", b"\x00\xff"]),
    ],
)
def test_nominal_response_roundtrip(response: ArrowSerializableDataclass) -> None:
    """Serialize every nominal response through the published VGI dataclass codec."""
    assert not isinstance(response, pa.RecordBatch)
    encoded = response.serialize_to_bytes()
    with pa.ipc.open_stream(encoded) as reader:
        assert reader.schema == response.ARROW_SCHEMA
        batches = list(reader)
    assert len(batches) == 1 and batches[0].num_rows == 1
    assert type(response).deserialize_from_bytes(encoded) == response
    field = response.ARROW_SCHEMA.names[0]
    with pytest.raises(FrozenInstanceError):
        setattr(response, field, None)


@pytest.mark.parametrize(
    "arguments",
    [
        {"kind": "missing"},
        {"kind": "int"},
        {"kind": "int", "int_value": True},
        {"kind": "int", "int_value": 2**63},
        {"kind": "int", "int_value": -(2**63) - 1},
        {"kind": "double", "double_value": float("nan")},
        {"kind": "double", "double_value": float("inf")},
        {"kind": "double", "double_value": 1},
        {"kind": "string", "bytes_value": b""},
        {"kind": "bytes", "bytes_value": b"", "string_value": ""},
    ],
)
def test_wire_option_rejects_invalid_discriminator_payload(arguments: dict[str, Any]) -> None:
    """Reject ambiguous fields and numeric coercions before Arrow serialization."""
    with pytest.raises(AdbcError) as error:
        WireOptionValue(**arguments)
    assert error.value.status == "invalid_data"


@pytest.mark.parametrize("value", ["", b"\x00\xff", -(2**63), 2**63 - 1, 0.0, -1.5])
def test_wire_option_native_types_survive_nested_roundtrip(value: str | bytes | int | float) -> None:
    """Preserve binary values and signed numeric boundaries through the nested struct."""
    kind = {str: "string", bytes: "bytes", int: "int", float: "double"}[type(value)]
    response = ValueResponse(value=WireOptionValue.from_value(value, kind))
    decoded = ValueResponse.deserialize_from_bytes(response.serialize_to_bytes()).value.to_value()
    assert decoded == value and type(decoded) is type(value)


def test_stock_vgi_unary_methods_have_standard_binary_envelope() -> None:
    """Derive unary response envelopes from nominal types without fixed-schema runtime patches."""
    methods = rpc_methods(Grainlift)
    for name, method in methods.items():
        if name not in {"read_result", "bind", "bind_stream"}:
            assert method.result_schema.equals(UNARY_OUTPUT, check_metadata=True)


def test_stock_http_returns_nominal_objects() -> None:
    """Use the stock HTTP client to recover actual response and nested option classes."""
    with Service(FeatureWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}),  # type: ignore[arg-type]  # WSGI wrapper is callable.
            default_headers={"Authorization": "Bearer token"},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # Reflect the protocol.
            session = rpc.open_connection(target="default", database_options_json="[]", connection_options_json="[]")
            assert type(session) is SessionResponse
            statement = rpc.new_statement(session_id=session.session_id)
            assert type(statement) is StatementResponse
            sid, stmt = session.session_id, statement.statement_id
            assert rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="query") == OkResponse(ok=True)
            assert type(rpc.execute_schema(session_id=sid, statement_id=stmt)) is SchemaResponse
            result = rpc.execute(session_id=sid, statement_id=stmt)
            assert type(result) is ExecuteResponse and result.rows_affected is None
            assert rpc.close_result(session_id=sid, result_id=result.result_id).ok
            assert type(rpc.execute_update(session_id=sid, statement_id=stmt)) is UpdateResponse
            assert type(rpc.execute_partitions(session_id=sid, statement_id=stmt)) is PartitionsResponse
            rpc.set_connection_option(session_id=sid, key="binary", value_json='{"type":"bytes","value":"AP8="}')
            option = rpc.get_connection_option(session_id=sid, key="binary", value_type="bytes")
            assert type(option) is ValueResponse and type(option.value) is WireOptionValue
            assert option.value.bytes_value == b"\x00\xff"
            assert rpc.close_connection(session_id=sid).ok


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_execute_response_size_boundary_closes_unregistered_cursor(headroom: int) -> None:
    """Include nested and outer IPC overhead and release failed result registration."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        reference = service.get_info(sid, "{}", ctx)
        envelope = pa.RecordBatch.from_pydict({"result": [reference.serialize_to_bytes()]}, schema=UNARY_OUTPUT)
        budget = len(serialize_record_batch_bytes(envelope))
        service.close_result(sid, reference.result_id, ctx)
        service.limits = replace(service.limits, batch_bytes=budget + headroom)
        if headroom < 0:
            with pytest.raises(AdbcError, match="exceeds"):
                service.get_info(sid, "{}", ctx)
            assert not service._sessions[sid].results
            assert worker.connections[0].readers[-1].closed
        else:
            response = service.get_info(sid, "{}", ctx)
            assert response.result_id in service._sessions[sid].results


def test_oversized_session_response_closes_connection_before_registration() -> None:
    """Do not leak a backend when even its session handle exceeds the wire budget."""
    worker = TestWorker()
    with Service(worker) as service:
        service.limits = replace(service.limits, batch_bytes=1)
        with pytest.raises(AdbcError, match="exceeds"):
            service.open_connection("default", "[]", "[]", context(service))
        assert not service._sessions and worker.connections[0].closed


def test_oversized_statement_response_does_not_allocate_backend() -> None:
    """Validate a statement response before calling the backend allocation hook."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        service.limits = replace(service.limits, batch_bytes=1)
        with pytest.raises(AdbcError, match="exceeds"):
            service.new_statement(sid, ctx)
        assert not service._sessions[sid].statements and not worker.connections[0].statements
