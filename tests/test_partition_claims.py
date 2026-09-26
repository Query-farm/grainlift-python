# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Opaque partition tokens preserve typed claims, bounds, and signature-first validation."""

from __future__ import annotations

import hmac
import time
from typing import Any

import pyarrow as pa
import pytest
from test_service import open_session
from test_unary_features import FeatureWorker

from grainlift import AdbcError, Limits, Service
from grainlift.tokens import PartitionClaims, seal_partition, unseal_partition

KEY = b"test signing key"
FIELDS: list[pa.Field[Any]] = [
    pa.field("version", pa.int64(), nullable=False),
    pa.field("expires_at_ms", pa.int64(), nullable=False),
    pa.field("owner", pa.string(), nullable=False),
    pa.field("descriptor", pa.binary(), nullable=False),
]
SCHEMA = pa.schema(FIELDS)


def _claims() -> PartitionClaims:
    return PartitionClaims(version=1, expires_at_ms=123456, owner="a" * 64, descriptor=b"\x00\xff")


def test_partition_claims_schema_and_opaque_envelope_oracle() -> None:
    """Check the binary token framing and fields independently of the sealing implementation."""
    claims = _claims()
    assert claims.ARROW_SCHEMA.equals(SCHEMA, check_metadata=True)
    token = seal_partition(claims, KEY, 4096)
    assert token[:4] == b"GLP2"
    signature, body = token[4:36], token[36:]
    assert hmac.compare_digest(signature, hmac.digest(KEY, body, "sha256"))
    with pa.ipc.open_stream(body) as reader:
        assert reader.schema.equals(SCHEMA, check_metadata=True)
        batches = list(reader)
    assert len(batches) == 1 and batches[0].to_pylist() == [
        {"version": 1, "expires_at_ms": 123456, "owner": "a" * 64, "descriptor": b"\x00\xff"}
    ]
    assert unseal_partition(token, KEY, 4096) == claims


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_partition_claims_complete_token_boundary(headroom: int) -> None:
    """Include the prefix, signature, schema, and IPC framing in the inclusive budget."""
    claims = _claims()
    size = 36 + len(claims.serialize_to_bytes())
    if headroom < 0:
        with pytest.raises(AdbcError, match="exceeds"):
            seal_partition(claims, KEY, size + headroom)
    else:
        assert len(seal_partition(claims, KEY, size + headroom)) == size
    token = seal_partition(claims, KEY, size)
    if headroom < 0:
        with pytest.raises(AdbcError, match="exceeds"):
            unseal_partition(token, KEY, size + headroom)
    else:
        assert unseal_partition(token, KEY, size + headroom) == claims


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", 0),
        ("version", 2),
        ("version", True),
        ("expires_at_ms", -1),
        ("expires_at_ms", 2**63),
        ("expires_at_ms", True),
        ("owner", "g" * 64),
        ("owner", "A" * 64),
        ("owner", "a" * 63),
        ("descriptor", "text"),
    ],
)
def test_invalid_partition_claim_values(field: str, value: Any) -> None:
    """Reject unsupported claims versions and ambiguous value representations."""
    values: dict[str, Any] = {"version": 1, "expires_at_ms": 123456, "owner": "a" * 64, "descriptor": b"\x00\xff"}
    values[field] = value
    with pytest.raises(AdbcError) as error:
        PartitionClaims(**values)
    assert error.value.status == "invalid_data"


@pytest.mark.parametrize("change", ["version", "null", "schema", "two_batches", "trailing", "malformed"])
def test_authenticated_malformed_claims_are_unavailable(change: str) -> None:
    """Even correctly signed tokens must match the exact supported typed claim contract."""
    values: dict[str, Any] = {
        "version": [1],
        "expires_at_ms": [123456],
        "owner": ["a" * 64],
        "descriptor": [b"\x00\xff"],
    }
    schema = SCHEMA
    if change == "version":
        values["version"] = [2]
    elif change == "null":
        values["descriptor"] = [None]
    elif change == "schema":
        schema = SCHEMA.set(0, pa.field("version", pa.int32(), nullable=False))
    batch = pa.RecordBatch.from_pydict(values, schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
        if change == "two_batches":
            writer.write_batch(batch)
    body = sink.getvalue().to_pybytes()
    if change == "trailing":
        body += b"extra"
    elif change == "malformed":
        body = b"private malformed bytes"
    token = b"GLP2" + hmac.digest(KEY, body, "sha256") + body
    with pytest.raises(AdbcError) as error:
        unseal_partition(token, KEY, 4096)
    assert error.value.status == "not_found" and "private malformed" not in str(error.value)


def test_bad_signature_is_rejected_before_claim_decode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Authenticate the complete body before invoking the Arrow decoder."""

    def forbidden_decode(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Invalid signature reached the IPC decoder")

    monkeypatch.setattr("grainlift.tokens.decode_batch", forbidden_decode)
    with pytest.raises(AdbcError) as error:
        unseal_partition(b"GLP2" + b"\x00" * 32 + b"private malformed bytes", KEY, 4096)
    assert error.value.status == "not_found"


@pytest.mark.parametrize("milliseconds", [-1, 0, 1])
def test_partition_expiry_boundary(monkeypatch: pytest.MonkeyPatch, milliseconds: int) -> None:
    """A partition remains usable just before its deadline and expires at that deadline."""
    monkeypatch.setattr(time, "time", lambda: 1000.0)
    worker = FeatureWorker()
    with Service(worker, limits=Limits(idle_seconds=1)) as service:
        sid, ctx = open_session(service)
        stmt = service.new_statement(sid, ctx).statement_id
        token = service.execute_partitions(sid, stmt, ctx).partitions[0]
        monkeypatch.setattr(time, "time", lambda: (1001000 + milliseconds) / 1000)
        if milliseconds < 0:
            service.read_partition(sid, token, ctx)
            assert worker.connections[0].calls == [("partition", b"partition-one")]
        else:
            with pytest.raises(AdbcError) as error:
                service.read_partition(sid, token, ctx)
            assert error.value.status == "not_found" and not worker.connections[0].calls


def test_partition_owner_includes_configured_target() -> None:
    """Changing a target cannot reuse an old partition under the same key and principal."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = service.new_statement(sid, ctx).statement_id
        token = service.execute_partitions(sid, stmt, ctx).partitions[0]
        worker.target = "another-target"
        with pytest.raises(AdbcError) as error:
            service.read_partition(sid, token, ctx)
        assert error.value.status == "not_found" and not worker.connections[0].calls
