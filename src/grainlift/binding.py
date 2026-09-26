# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Bounded anonymous-file staging for Arrow binding and ingestion uploads."""

from __future__ import annotations

import hashlib
import io
import struct
import tempfile
import time
from collections.abc import Buffer
from contextlib import suppress

import pyarrow as pa

from .api import AdbcError


def decode_schema(payload: bytes, limit: int) -> pa.Schema:
    """Decode Grainlift's unframed FlatBuffer schema within its byte budget.

    Args:
        payload: Raw Arrow IPC schema message metadata.
        limit: Inclusive maximum metadata size.

    Returns:
        The decoded Arrow schema.
    """
    if not payload or len(payload) > limit:
        raise AdbcError("Bind schema exceeds configured limit or is empty", "invalid_arguments")
    padding = b"\x00" * (-len(payload) % 8)
    framed = b"\xff" * 4 + struct.pack("<I", len(payload) + len(padding)) + payload + padding
    try:
        return pa.ipc.read_schema(pa.py_buffer(framed))
    except (pa.ArrowException, OSError, ValueError):
        raise AdbcError("Invalid bind schema", "invalid_arguments") from None


def decode_batch(payload: bytes, limit: int) -> pa.RecordBatch:
    """Decode exactly one bounded, uncompressed IPC parameter batch.

    Compression belongs to the transport. The nested payload is an ordinary
    uncompressed Arrow IPC stream and is decoded using PyArrow's public API.

    Args:
        payload: A complete IPC stream with schema, optional dictionaries, one batch and EOS.
        limit: Inclusive encoded-payload and decoded-buffer byte ceiling.

    Returns:
        The decoded parameter batch with its original schema and row count.
    """
    if not payload or len(payload) > limit:
        raise AdbcError("Bind payload exceeds configured limit or is empty", "invalid_data")
    try:
        source = pa.BufferReader(payload)
        first = pa.ipc.read_message(source)
        if first.type != "schema":
            raise ValueError("Missing IPC schema")
        batches = 0
        while True:
            before = source.tell()
            try:
                message = pa.ipc.read_message(source)
            except EOFError:
                if source.tell() != len(payload) or payload[before:] != b"\xff\xff\xff\xff\x00\x00\x00\x00":
                    raise ValueError("Missing IPC end marker or trailing bytes") from None
                break
            if message.type == "record batch":
                batches += 1
                if batches != 1:
                    raise ValueError("Multiple parameter batches in one turn")
            elif message.type != "dictionary" or batches:
                raise ValueError("Unexpected IPC message")
        if batches != 1:
            raise ValueError("Missing parameter batch")
        with pa.ipc.open_stream(payload) as reader:
            result = reader.read_next_batch()
            result.validate(full=True)
            if result.get_total_buffer_size() > limit:
                raise ValueError("Decoded parameter buffers exceed configured limit")
            return result
    except (pa.ArrowException, OSError, ValueError, EOFError, struct.error, OverflowError):
        raise AdbcError("Invalid or oversized bind IPC payload", "invalid_data") from None


class _BoundedFile(io.RawIOBase):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self.file = tempfile.TemporaryFile(mode="w+b")  # noqa: SIM115 -- owned until upload/statement cleanup.
        self.limit = limit
        self.written = 0

    def writable(self) -> bool:
        return True

    def write(self, value: Buffer) -> int:
        size = memoryview(value).nbytes
        if self.written + size > self.limit:
            raise AdbcError("Bind upload exceeds configured byte limit", "invalid_data")
        written = self.file.write(memoryview(value))
        self.written += written
        return written

    def tell(self) -> int:
        return self.file.tell()

    def close(self) -> None:
        self.file.close()
        super().close()


class _DigestSink(io.RawIOBase):
    def __init__(self) -> None:
        super().__init__()
        self.digest = hashlib.sha256()

    def writable(self) -> bool:
        return True

    def write(self, value: Buffer) -> int:
        view = memoryview(value)
        self.digest.update(view)
        return view.nbytes


class BindUpload:
    """Stage one upload with one-turn replay detection and explicit ownership.

    The cumulative budget includes the schema, dictionaries, batches and IPC
    footer. No batch list is retained. The returned reader stays owned by this
    object until replacement, statement/session close or expiry. A finished
    upload retains only its reader and last digest for an identical final retry.
    """

    def __init__(self, schema: pa.Schema, *, limit: int, batch_limit: int, stream: bool) -> None:
        """Create a capped temporary Arrow stream.

        Args:
            schema: Exact schema every input batch must match.
            limit: Inclusive cumulative serialized upload budget.
            batch_limit: Inclusive per-batch referenced-buffer budget.
            stream: Whether zero or multiple input batches are permitted.
        """
        self.schema = schema
        self.batch_limit = batch_limit
        self.stream = stream
        self.sequence = 0
        self.touched = time.monotonic()
        self.finished = False
        self.closed = False
        self.reader: pa.RecordBatchReader | None = None
        self._last: bytes | None = None
        self._batches = 0
        self._file = _BoundedFile(limit)
        try:
            self._writer = pa.ipc.new_stream(self._file, schema)
        except Exception:
            self._file.close()
            raise

    def accept(self, batch: pa.RecordBatch, sequence: int, *, finish: bool) -> bool:
        """Stage a turn once, rejecting changed replays and sequence gaps.

        Args:
            batch: Input batch matching the negotiated schema.
            sequence: Zero-based upload turn number.
            finish: Explicit end-of-input marker; requires a zero-row batch.

        Returns:
            True for a newly finished upload, False for a data turn or retry.
        """
        if self.closed:
            raise AdbcError("Bind upload is unavailable", "not_found")
        if not batch.schema.equals(self.schema, check_metadata=True):
            raise AdbcError("Bind schema changed", "invalid_data")
        if batch.get_total_buffer_size() > self.batch_limit:
            raise AdbcError("Bind batch exceeds configured limit", "invalid_data")
        sink = _DigestSink()
        with pa.ipc.new_stream(sink, self.schema) as writer:
            writer.write_batch(batch)
        sink.digest.update(bytes([finish]))
        fingerprint = sink.digest.digest()
        if sequence == self.sequence - 1 and fingerprint == self._last:
            self.touched = time.monotonic()
            return False
        if sequence != self.sequence or self.finished:
            raise AdbcError("Invalid bind upload sequence", "invalid_arguments")
        try:
            if finish:
                if batch.num_rows != 0:
                    raise AdbcError("Bind finish requires an empty batch", "invalid_arguments")
                if not self.stream and self._batches != 1:
                    raise AdbcError("Bind requires exactly one batch", "invalid_arguments")
                self._writer.close()
                self._file.file.seek(0)
                self.reader = pa.ipc.open_stream(self._file.file)
                self.finished = True
            else:
                if not self.stream and self._batches:
                    raise AdbcError("Bind accepts only one batch", "invalid_arguments")
                self._writer.write_batch(batch)
                self._batches += 1
        except Exception:
            self.close()
            raise
        self._last = fingerprint
        self.sequence += 1
        self.touched = time.monotonic()
        return finish

    def close(self) -> None:
        """Close the reader and anonymous spool on every lifecycle exit."""
        if not self.closed:
            self.closed = True
            if self.reader is not None:
                self.reader.close()
            with suppress(Exception):
                self._writer.close()
            self._file.close()
