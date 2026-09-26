# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Strict, bounded control records using the published VGI Arrow codec."""

from __future__ import annotations

from typing import Any, Self

import pyarrow as pa
from vgi_rpc.utils import ArrowSerializableDataclass, IpcValidation

from .api import AdbcError
from .binding import decode_batch

MAX_CONTROL_BYTES = 256 * 1024 * 1024


def _validate_array(array: pa.Array[Any], nullable: bool) -> None:
    if not nullable and array.null_count:
        raise AdbcError("Null in required control field", "invalid_arguments")
    if isinstance(array, pa.StructArray):
        for index, field in enumerate(array.type):
            _validate_array(array.field(index), field.nullable)
    elif isinstance(array, pa.ListArray):
        # VGI declares list children nullable; Grainlift list[T] never permits None items.
        _validate_array(array.values, False)


class ControlRecord(ArrowSerializableDataclass):
    """Require the exact versioned schema and one complete uncompressed IPC batch.

    Transport decoding owns compression. The hard ceiling here is additional to
    the service's smaller configured request or token limit. Adding or removing
    fields requires a compatible protocol negotiation, never silent coercion.
    """

    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Read a complete bounded control payload with mandatory full validation."""
        return cls.deserialize_from_batch(decode_batch(data, MAX_CONTROL_BYTES), ipc_validation=ipc_validation)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Reject extra fields, missing fields, null list items and incorrect physical types."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=True):
            raise AdbcError("Unexpected control record schema or row count", "invalid_arguments")
        batch.validate(full=True)
        for field, array in zip(batch.schema, batch.columns, strict=True):
            _validate_array(array, field.nullable)
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)
