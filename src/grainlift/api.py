# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Small author-facing API; wire handles are managed by the toolkit."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass

import pyarrow as pa

type OptionValue = str | bytes | int | float


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
        results_per_session: Maximum live result handles, including metadata cursors.
        partitions_per_result: Maximum descriptors returned by partitioned execution.
        bind_bytes: Maximum cumulative serialized Arrow bytes in one parameter upload.
        batch_bytes: Maximum batch buffer bytes and schema descriptor bytes.
        request_bytes: Maximum HTTP request body bytes.
        sql_bytes: Maximum UTF-8 encoded SQL bytes per statement.
        idle_seconds: Idle lifetime of connections and result cursors.
        lock_timeout_seconds: Maximum wait to acquire a busy session lock.
        shutdown_seconds: Total wait budget for busy session locks at shutdown.
    """

    sessions: int = 64
    statements_per_session: int = 32
    results_per_session: int = 32
    partitions_per_result: int = 1024
    bind_bytes: int = 64 * 1024 * 1024
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
            "results_per_session",
            "partitions_per_result",
            "bind_bytes",
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


@dataclass
class PartitionedResult:
    """Schema and opaque downstream descriptors from partitioned execution.

    Attributes:
        schema: Schema shared by all partition readers.
        partitions: Backend descriptors; the service authenticates exported wrappers.
        rows_affected: Affected-row count, or -1 when unknown.
    """

    schema: pa.Schema
    partitions: list[bytes]
    rows_affected: int = -1


class Statement:
    """Backend statement with optional ADBC capabilities and explicit resource ownership."""

    def set_sql_query(self, sql: str) -> None:
        """Replace SQL text; implementations should invalidate prior prepared state."""
        raise AdbcError("SQL statements are not implemented", "not_implemented")

    def set_substrait_plan(self, payload: bytes) -> None:
        """Replace the statement with a serialized Substrait plan."""
        raise AdbcError("Substrait plans are not implemented", "not_implemented")

    def prepare(self) -> None:
        """Prepare the configured query without executing it."""
        raise AdbcError("Prepared statements are not implemented", "not_implemented")

    def bind(self, batch: pa.RecordBatch) -> None:
        """Bind one parameter batch, retaining it until execution or replacement."""
        raise AdbcError("Parameter binding is not implemented", "not_implemented")

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Bind a parameter reader valid until query replacement or statement close."""
        raise AdbcError("Parameter stream binding is not implemented", "not_implemented")

    def execute(self) -> QueryResult:
        """Execute the configured statement and return a lazy result cursor."""
        raise AdbcError("Query execution is not implemented", "not_implemented")

    def execute_update(self) -> int | None:
        """Execute without a result cursor and return the affected-row count if known."""
        raise AdbcError("Update execution is not implemented", "not_implemented")

    def execute_schema(self) -> pa.Schema:
        """Infer the result schema without executing the statement."""
        raise AdbcError("Schema inference is not implemented", "not_implemented")

    def get_parameter_schema(self) -> pa.Schema:
        """Return the prepared statement's parameter schema."""
        raise AdbcError("Parameter schema is not implemented", "not_implemented")

    def execute_partitions(self) -> PartitionedResult:
        """Execute and return backend partition descriptors instead of row batches."""
        raise AdbcError("Partitioned execution is not implemented", "not_implemented")

    def set_option(self, key: str, value: OptionValue) -> None:
        """Set a typed statement option, including backend-supported ingestion settings."""
        raise AdbcError("Statement option is not supported", "not_implemented")

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Read an option in the requested string, bytes, int, or double representation."""
        raise AdbcError("Statement option is not supported", "not_implemented")

    def cancel(self) -> None:
        """Request cancellation concurrently; implementations must be thread-safe and nonblocking."""
        raise AdbcError("Statement cancellation is not implemented", "not_implemented")

    def close(self) -> None:
        """Release backend resources before any toolkit-owned parameter reader is closed."""


class Connection:
    """Backend connection; override capabilities supported by the actual data source."""

    def new_statement(self) -> Statement:
        """Create an independent statement; adapt legacy execute(sql) workers by default."""
        return _LegacyStatement(self)

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

    def set_option(self, key: str, value: OptionValue) -> None:
        """Set a backend option; the default connection supports only enabling autocommit."""
        if key == "adbc.connection.autocommit" and type(value) is str and value == "true":
            return
        raise AdbcError("Connection option is not supported", "not_implemented")

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Read a backend option; legacy query workers report autocommit enabled."""
        if key == "adbc.connection.autocommit" and value_type == "string":
            return "true"
        raise AdbcError("Connection option is not supported", "not_implemented")

    def commit(self) -> None:
        """Commit the active backend transaction without emulating transaction semantics."""
        raise AdbcError("Transactions are not implemented", "not_implemented")

    def rollback(self) -> None:
        """Roll back the active backend transaction."""
        raise AdbcError("Transactions are not implemented", "not_implemented")

    def get_info(self, codes: list[int] | None) -> QueryResult:
        """Return driver or vendor information using the ADBC GetInfo Arrow schema."""
        raise AdbcError("Driver information is not implemented", "not_implemented")

    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult:
        """Return the requested ADBC object hierarchy with backend filtering semantics."""
        raise AdbcError("Object discovery is not implemented", "not_implemented")

    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema:
        """Return the schema of a backend table."""
        raise AdbcError("Table schemas are not implemented", "not_implemented")

    def get_table_types(self) -> QueryResult:
        """Return the supported table types using the ADBC metadata schema."""
        raise AdbcError("Table type discovery is not implemented", "not_implemented")

    def get_statistic_names(self) -> QueryResult:
        """Return names and keys of supported backend statistics."""
        raise AdbcError("Statistic name discovery is not implemented", "not_implemented")

    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult:
        """Return statistics using the ADBC statistics Arrow schema."""
        raise AdbcError("Statistics are not implemented", "not_implemented")

    def read_partition(self, descriptor: bytes) -> QueryResult:
        """Read a backend partition after the service verifies the exported descriptor."""
        raise AdbcError("Partition reading is not implemented", "not_implemented")


class _LegacyStatement(Statement):
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.sql: str | None = None

    def set_sql_query(self, sql: str) -> None:
        self.sql = sql

    def _query(self) -> str:
        if self.sql is None:
            raise AdbcError("Set a query before execution", "invalid_state")
        return self.sql

    def execute(self) -> QueryResult:
        return self.connection.execute(self._query())

    def execute_schema(self) -> pa.Schema:
        return self.connection.execute_schema(self._query())

    def cancel(self) -> None:
        self.connection.cancel()

    def close(self) -> None:
        self.sql = None


class Worker:
    """One configured target. Each connection belongs to an authenticated principal."""

    target = "default"

    def connect(self, principal: str) -> Connection:
        """Open a connection bound to the authenticated principal."""
        raise AdbcError("Connection creation is not implemented", "not_implemented")

    def open_connection(
        self,
        principal: str,
        database_options: Mapping[str, OptionValue],
        connection_options: Mapping[str, OptionValue],
    ) -> Connection:
        """Open a configured connection, applying options or failing with cleanup.

        Args:
            principal: Authenticated owner of the resulting connection.
            database_options: Server-approved database factory options.
            connection_options: Server-approved initial connection settings.

        Returns:
            A connection ready for statement creation.
        """
        if database_options:
            raise AdbcError("Caller-supplied database options are not supported", "not_implemented")
        connection = self.connect(principal)
        try:
            for key, value in connection_options.items():
                connection.set_option(key, value)
        except Exception:
            with suppress(Exception):
                connection.close()
            raise
        return connection
