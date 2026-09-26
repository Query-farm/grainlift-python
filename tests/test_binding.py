# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Prove Arrow upload limits, replay behavior, and spool ownership."""

import pyarrow as pa
import pytest

from grainlift import AdbcError
from grainlift.binding import BindUpload, decode_schema
from grainlift.protocol import schema_ipc

SCHEMA = pa.schema([("value", pa.int64())])
DATA = pa.record_batch([[1, 2, 3]], schema=SCHEMA)
EMPTY = pa.record_batch([[]], schema=SCHEMA)


def _size() -> int:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, SCHEMA) as writer:
        writer.write_batch(DATA)
    return sink.getvalue().size


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_upload_cumulative_boundary(delta: int) -> None:
    """Count schema and IPC footer as well as buffers at the exact disk budget."""
    upload = BindUpload(SCHEMA, limit=_size() + delta, batch_limit=1024, stream=True)
    try:
        if delta < 0:
            with pytest.raises(AdbcError, match="byte limit"):
                upload.accept(DATA, 0, finish=False)
                upload.accept(EMPTY, 1, finish=True)
            assert upload.closed
            assert upload._file.file.closed
        else:
            assert not upload.accept(DATA, 0, finish=False)
            assert upload.accept(EMPTY, 1, finish=True)
            assert upload.reader is not None
            assert upload.reader.read_all().column(0).to_pylist() == [1, 2, 3]
    finally:
        upload.close()
    assert upload._file.file.closed


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_upload_batch_boundary(delta: int) -> None:
    """Apply the per-batch memory budget independently of the disk budget."""
    upload = BindUpload(SCHEMA, limit=4096, batch_limit=DATA.get_total_buffer_size() + delta, stream=True)
    try:
        if delta < 0:
            with pytest.raises(AdbcError, match="batch exceeds"):
                upload.accept(DATA, 0, finish=False)
        else:
            upload.accept(DATA, 0, finish=False)
            assert upload.accept(EMPTY, 1, finish=True)
    finally:
        upload.close()


def test_upload_replay_is_exactly_once() -> None:
    """Retry data and finish acknowledgements without duplicate input or rebind."""
    upload = BindUpload(SCHEMA, limit=4096, batch_limit=1024, stream=True)
    try:
        upload.accept(DATA, 0, finish=False)
        assert not upload.accept(DATA, 0, finish=False)
        assert upload.accept(EMPTY, 1, finish=True)
        assert not upload.accept(EMPTY, 1, finish=True)
        assert upload.reader is not None
        assert upload.reader.read_all().num_rows == DATA.num_rows
        with pytest.raises(AdbcError, match="sequence"):
            upload.accept(DATA, 2, finish=False)
    finally:
        upload.close()


def test_changed_replay_and_skipped_sequence_are_rejected() -> None:
    """A signed continuation cannot rewrite or skip a staged upload turn."""
    upload = BindUpload(SCHEMA, limit=4096, batch_limit=1024, stream=True)
    try:
        upload.accept(DATA, 0, finish=False)
        changed = pa.record_batch([[9, 8, 7]], schema=SCHEMA)
        with pytest.raises(AdbcError, match="sequence"):
            upload.accept(changed, 0, finish=False)
        with pytest.raises(AdbcError, match="sequence"):
            upload.accept(DATA, 2, finish=False)
        assert upload.accept(EMPTY, 1, finish=True)
    finally:
        upload.close()


def test_empty_stream_and_single_empty_batch_are_distinct() -> None:
    """BindStream permits no batches; Bind requires one even when it has no rows."""
    for stream in (True, False):
        upload = BindUpload(SCHEMA, limit=4096, batch_limit=1024, stream=stream)
        try:
            if not stream:
                upload.accept(EMPTY, 0, finish=False)
            assert upload.accept(EMPTY, int(not stream), finish=True)
            assert upload.reader is not None
            assert upload.reader.schema == SCHEMA
            assert len(list(upload.reader)) == int(not stream)
        finally:
            upload.close()


def test_single_bind_rejects_multiple_batches() -> None:
    """Do not silently truncate a malformed multi-batch Bind request."""
    upload = BindUpload(SCHEMA, limit=4096, batch_limit=1024, stream=False)
    upload.accept(DATA, 0, finish=False)
    with pytest.raises(AdbcError, match="only one"):
        upload.accept(DATA, 1, finish=False)
    assert upload.closed


def test_finish_requires_zero_rows_and_closes_on_failure() -> None:
    """Never treat a data-bearing final marker as a completed upload."""
    upload = BindUpload(SCHEMA, limit=4096, batch_limit=1024, stream=True)
    with pytest.raises(AdbcError, match="empty batch"):
        upload.accept(DATA, 0, finish=True)
    assert upload.closed


def test_dictionary_replay_fingerprint_includes_dictionary_values() -> None:
    """Equal indices with changed dictionary values are different input turns."""
    first = pa.record_batch([pa.array(["alpha", "beta"]).dictionary_encode()], names=["value"])
    second = pa.record_batch([pa.array(["gamma", "delta"]).dictionary_encode()], names=["value"])
    upload = BindUpload(first.schema, limit=4096, batch_limit=1024, stream=True)
    try:
        upload.accept(first, 0, finish=False)
        with pytest.raises(AdbcError, match="sequence"):
            upload.accept(second, 0, finish=False)
    finally:
        upload.close()


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_schema_descriptor_boundary(delta: int) -> None:
    """Validate the native protocol's raw schema representation and inclusive cap."""
    descriptor = schema_ipc(SCHEMA)
    if delta < 0:
        with pytest.raises(AdbcError, match="schema exceeds"):
            decode_schema(descriptor, len(descriptor) + delta)
    else:
        assert decode_schema(descriptor, len(descriptor) + delta) == SCHEMA


@pytest.mark.parametrize("payload", [b"", b"not-arrow", b"\x00" * 32])
def test_invalid_schema_is_sanitized(payload: bytes) -> None:
    """Malformed Arrow schemas produce an ADBC error without reflecting the input."""
    with pytest.raises(AdbcError):
        decode_schema(payload, 4096)
