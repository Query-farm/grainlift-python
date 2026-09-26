# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Validate raw nested Arrow payloads independently of transport compression."""

import pyarrow as pa
import pytest

from grainlift import AdbcError
from grainlift.binding import decode_batch


def _encode(batch: pa.RecordBatch, count: int = 1) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema, options=pa.ipc.IpcWriteOptions(compression=None)) as writer:
        for _ in range(count):
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


@pytest.mark.parametrize(
    "batch",
    [
        pa.record_batch([[1, None, 3]], names=["value"]),
        pa.record_batch([pa.array([], type=pa.int64())], names=["value"]),
        pa.record_batch([pa.array(["a", "b", "a"]).dictionary_encode()], names=["value"]),
        pa.record_batch([[1, 2, 3]], names=["ignored"]).select([]),
        pa.record_batch([], schema=pa.schema([])),
    ],
)
def test_raw_ipc_roundtrip(batch: pa.RecordBatch) -> None:
    """Preserve dictionaries, nulls, empty dimensions, and schema metadata."""
    batch = batch.replace_schema_metadata({b"application": b"binding-test"})
    payload = _encode(batch)
    decoded = decode_batch(payload, len(payload))
    assert decoded.equals(batch, check_metadata=True)
    assert decoded.num_rows == batch.num_rows


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_raw_payload_byte_boundary(delta: int) -> None:
    """Enforce the inclusive payload limit before opening the Arrow stream."""
    batch = pa.record_batch([[1, 2, 3]], names=["value"])
    payload = _encode(batch)
    if delta < 0:
        with pytest.raises(AdbcError, match="exceeds configured limit"):
            decode_batch(payload, len(payload) + delta)
    else:
        assert decode_batch(payload, len(payload) + delta).equals(batch)


@pytest.mark.parametrize("case", ["empty", "invalid", "truncated", "no_eos", "trailing", "zero", "multiple"])
def test_malformed_nested_stream(case: str) -> None:
    """Reject malformed framing and any turn that does not contain exactly one batch."""
    batch = pa.record_batch([[1, 2, 3]], names=["value"])
    payload = _encode(batch)
    malformed = {
        "empty": b"",
        "invalid": b"invalid Arrow IPC",
        "truncated": payload[: len(payload) // 2],
        "no_eos": payload[:-8],
        "trailing": payload + b"trailing",
        "zero": _encode(batch, 0),
        "multiple": _encode(batch, 2),
    }[case]
    with pytest.raises(AdbcError) as error:
        decode_batch(malformed, 16384)
    assert error.value.status == "invalid_data"
