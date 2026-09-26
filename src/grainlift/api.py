# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Small author-facing API; wire handles are managed by the toolkit."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import pyarrow as pa


class AdbcError(Exception):
    """An ADBC error with optional SQLSTATE, vendor code, and binary details.

    Messages go to the requesting client. Never put credentials in them.
    """

    def __init__(
        self,
        message: str,
        status: str = "unknown",
        *,
        sqlstate: str = "00000",
        vendor_code: int = 0,
        details: Mapping[str, bytes] | None = None,
    ) -> None:
        """Encode a structured error for the requesting ADBC client.

        Args:
            message: Client-visible message; must not contain credentials.
            status: Lowercase ADBC status name, such as invalid_data.
            sqlstate: Exactly five ASCII characters.
            vendor_code: Downstream driver's numeric error code.
            details: Additional named binary error fields.
        """
        if len(sqlstate) != 5 or not sqlstate.isascii():
            raise ValueError("SQLSTATE must contain five ASCII characters")
        self.status = status
        self.error_kind = f"adbc.{status}"
        super().__init__(
            json.dumps(
                {
                    "status": status,
                    "message": message,
                    "vendor_code": vendor_code,
                    "sqlstate": list(sqlstate.encode("ascii")),
                    "details": [(k, base64.b64encode(v).decode("ascii")) for k, v in (details or {}).items()],
                }
            )
        )


@dataclass(frozen=True)
class Limits:
    """Finite service quotas; batch bytes include all referenced Arrow buffers.

    Attributes:
        sessions: Maximum live and currently opening connections.
        statements_per_session: Maximum statement handles per connection.
        batch_bytes: Maximum batch buffer bytes and schema descriptor bytes.
        request_bytes: Maximum HTTP request body bytes.
        sql_bytes: Maximum UTF-8 encoded SQL bytes per statement.
        idle_seconds: Idle lifetime of connections and result cursors.
        lock_timeout_seconds: Maximum wait to acquire a busy session lock.
        shutdown_seconds: Total wait budget for busy session locks at shutdown.
    """

    sessions: int = 64
    statements_per_session: int = 32
    batch_bytes: int = 1024 * 1024
    request_bytes: int = 2 * 1024 * 1024
    sql_bytes: int = 64 * 1024
    idle_seconds: float = 300
    lock_timeout_seconds: float = 5
    shutdown_seconds: float = 5

    def __post_init__(self) -> None:
        """Reject nonfinite durations and invalid integer quotas."""
        for name in (
            "sessions",
            "statements_per_session",
            "batch_bytes",
            "request_bytes",
            "sql_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("idle_seconds", "lock_timeout_seconds", "shutdown_seconds"):
            value = getattr(self, name)
            if type(value) not in (int, float) or value <= 0 or (isinstance(value, float) and not math.isfinite(value)):
                raise ValueError(f"{name} must be a finite positive number")


@dataclass
class QueryResult:
    """Known schema and lazy batches whose iterator owns cursor resources.

    Attributes:
        schema: Stable schema shared by every batch in the result.
        batches: Lazy iterator with an optional close() cleanup method.
        rows_affected: Affected-row count, or None when unknown.
    """

    schema: pa.Schema
    batches: Iterator[pa.RecordBatch]
    rows_affected: int | None = None

    def close(self) -> None:
        """Call the batch iterator's close method when available."""
        close = getattr(self.batches, "close", None)
        if close is not None:
            close()


class Connection:
    """Override execute; optionally provide schema inference and cleanup."""

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return its schema and lazy batch iterator."""
        raise AdbcError("Query execution is not implemented", "not_implemented")

    def execute_schema(self, sql: str) -> pa.Schema:
        """Infer the result schema without opening a cursor."""
        raise AdbcError("Schema inference is not implemented", "not_implemented")

    def close(self) -> None:
        """Release owned resources; repeated calls are safe."""
        pass

    def cancel(self) -> None:
        """Request cancellation concurrently; implementations must be thread-safe and nonblocking."""
        raise AdbcError("Downstream cancellation is not implemented", "not_implemented")


class Worker:
    """One configured target. Each connection belongs to an authenticated principal."""

    target = "default"

    def connect(self, principal: str) -> Connection:
        """Open a connection bound to the authenticated principal."""
        raise AdbcError("Connection creation is not implemented", "not_implemented")
