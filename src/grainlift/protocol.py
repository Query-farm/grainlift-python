# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Grainlift 0.3.0 typed responses carried by standard VGI-RPC serialization."""

from dataclasses import dataclass
from typing import ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import AnnotatedBatch, CallContext, ExchangeState, OutputCollector, ProducerState, Stream
from vgi_rpc.utils import ArrowSerializableDataclass

from .options import WireOptionValue


def schema(*fields: tuple[str, pa.DataType, bool]) -> pa.Schema:
    """Build a wire schema from name, type, and nullability triples."""
    return pa.schema([pa.field(*field) for field in fields])


OK = schema(("ok", pa.bool_(), False))
BIND_INPUT = schema(("batch_ipc", pa.binary(), False), ("finish", pa.bool_(), False))
UNARY_OUTPUT = schema(("result", pa.binary(), False))


@dataclass(frozen=True, kw_only=True)
class OkResponse(ArrowSerializableDataclass):
    """Acknowledge a successful operation.

    Attributes:
        ok: Whether the operation succeeded.
    """

    ok: bool


@dataclass(frozen=True, kw_only=True)
class SessionResponse(ArrowSerializableDataclass):
    """Return a principal-owned connection handle.

    Attributes:
        session_id: Opaque connection identifier.
    """

    session_id: str


@dataclass(frozen=True, kw_only=True)
class StatementResponse(ArrowSerializableDataclass):
    """Return a statement handle scoped to its connection.

    Attributes:
        session_id: Owning connection identifier.
        statement_id: Opaque statement identifier.
    """

    session_id: str
    statement_id: str


@dataclass(frozen=True, kw_only=True)
class ExecuteResponse(ArrowSerializableDataclass):
    """Describe a lazy result without consuming its Arrow batches.

    Attributes:
        result_id: Opaque cursor identifier.
        rows_affected: Backend row count, or None if unknown.
        schema_ipc: Unframed Arrow FlatBuffer schema message.
    """

    result_id: str
    rows_affected: int | None
    schema_ipc: bytes


@dataclass(frozen=True, kw_only=True)
class SchemaResponse(ArrowSerializableDataclass):
    """Return a schema without creating a result cursor.

    Attributes:
        schema_ipc: Unframed Arrow FlatBuffer schema message.
    """

    schema_ipc: bytes


@dataclass(frozen=True, kw_only=True)
class ValueResponse(ArrowSerializableDataclass):
    """Return an option as a typed nested Arrow record.

    Attributes:
        value: Validated option discriminator and payload.
    """

    value: WireOptionValue


@dataclass(frozen=True, kw_only=True)
class UpdateResponse(ArrowSerializableDataclass):
    """Report affected rows without allocating a cursor.

    Attributes:
        rows_affected: Backend row count, or None if unknown.
    """

    rows_affected: int | None


@dataclass(frozen=True, kw_only=True)
class PartitionsResponse(ArrowSerializableDataclass):
    """Return bounded partition descriptors as native binary values.

    Attributes:
        rows_affected: Backend row count, or -1 if unknown.
        schema_ipc: Unframed Arrow FlatBuffer schema message.
        partitions: Signed descriptors scoped to the service and principal.
    """

    rows_affected: int
    schema_ipc: bytes
    partitions: list[bytes]


def schema_ipc(value: pa.Schema) -> bytes:
    # Rust expects the FlatBuffer Message, without IPC framing or padding.
    """Serialize a FlatBuffer schema message without IPC framing."""
    return pa.ipc.read_message(value.serialize()).metadata.to_pybytes()


@dataclass
class ResultCursor(ProducerState):
    """Serializable handle cursor; the actual iterator remains in the service."""

    session_id: str
    result_id: str
    sequence: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one batch or finish the pull stream at end of results."""
        batch = ctx.implementation.next_batch(self.session_id, self.result_id, self.sequence, ctx)
        if batch is None:
            out.finish()
        else:
            out.emit(batch)
            self.sequence += 1

    def on_cancel(self, ctx: CallContext) -> None:
        """Release the server cursor when the pull stream is cancelled."""
        ctx.implementation.close_result(self.session_id, self.result_id, ctx)


@dataclass
class BindCursor(ExchangeState):
    """Signed upload cursor; bounded Arrow data remains in the owning service."""

    session_id: str
    statement_id: str
    upload_id: str
    sequence: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Stage one batch or finish input and acknowledge only completed work."""
        ctx.implementation.push_binding_frame(
            self.session_id, self.statement_id, self.upload_id, self.sequence, input.batch, ctx
        )
        out.emit(pa.RecordBatch.from_pydict({"ok": [True]}, schema=OK))
        self.sequence += 1

    def on_cancel(self, ctx: CallContext) -> None:
        """Discard incomplete input without unbinding a successfully finished upload."""
        ctx.implementation.cancel_binding(self.session_id, self.statement_id, self.upload_id, ctx)


class Grainlift(Protocol):
    """Describe the public Grainlift ADBC wire methods."""

    protocol_name: ClassVar[str] = "org.queryfarm.Grainlift.v1"
    protocol_version: ClassVar[str] = "0.3.0"

    def open_connection(self, target: str, database_options_json: str, connection_options_json: str) -> SessionResponse:
        """Authenticate and allocate a connection within the service quota."""
        ...

    def close_connection(self, session_id: str) -> OkResponse:
        """Close a connection and all child handles."""
        ...

    def new_statement(self, session_id: str) -> StatementResponse:
        """Allocate an empty statement within the session quota."""
        ...

    def close_statement(self, session_id: str, statement_id: str) -> OkResponse:
        """Close a statement and its active result."""
        ...

    def set_sql_query(self, session_id: str, statement_id: str, sql: str) -> OkResponse:
        """Replace statement SQL after validating its encoded size."""
        ...

    def execute(self, session_id: str, statement_id: str) -> ExecuteResponse:
        """Execute SQL and return its schema and lazy batch iterator."""
        ...

    def execute_schema(self, session_id: str, statement_id: str) -> SchemaResponse:
        """Infer the result schema without opening a cursor."""
        ...

    def close_result(self, session_id: str, result_id: str) -> OkResponse:
        """Release a result cursor and retained replay batch."""
        ...

    def read_result(self, session_id: str, result_id: str, sequence: int) -> Stream[ResultCursor]:
        """Open a pull stream at the requested result sequence."""
        ...

    def set_connection_option(self, session_id: str, key: str, value_json: str) -> OkResponse:
        """Set connection option through the ADBC wire protocol."""
        ...

    def get_connection_option(self, session_id: str, key: str, value_type: str) -> ValueResponse:
        """Get connection option through the ADBC wire protocol."""
        ...

    def commit(self, session_id: str) -> OkResponse:
        """Commit through the ADBC wire protocol."""
        ...

    def rollback(self, session_id: str) -> OkResponse:
        """Rollback through the ADBC wire protocol."""
        ...

    def cancel_connection(self, session_id: str) -> OkResponse:
        """Cancel connection through the ADBC wire protocol."""
        ...

    def cancel_statement(self, session_id: str, statement_id: str) -> OkResponse:
        """Cancel statement through the ADBC wire protocol."""
        ...

    def prepare(self, session_id: str, statement_id: str) -> OkResponse:
        """Prepare through the ADBC wire protocol."""
        ...

    def execute_update(self, session_id: str, statement_id: str) -> UpdateResponse:
        """Execute update through the ADBC wire protocol."""
        ...

    def execute_partitions(self, session_id: str, statement_id: str) -> PartitionsResponse:
        """Execute partitions through the ADBC wire protocol."""
        ...

    def get_parameter_schema(self, session_id: str, statement_id: str) -> SchemaResponse:
        """Get parameter schema through the ADBC wire protocol."""
        ...

    def set_substrait_plan(self, session_id: str, statement_id: str, payload: bytes) -> OkResponse:
        """Set substrait plan through the ADBC wire protocol."""
        ...

    def set_statement_option(self, session_id: str, statement_id: str, key: str, value_json: str) -> OkResponse:
        """Set statement option through the ADBC wire protocol."""
        ...

    def get_statement_option(self, session_id: str, statement_id: str, key: str, value_type: str) -> ValueResponse:
        """Get statement option through the ADBC wire protocol."""
        ...

    def bind(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[BindCursor]:
        """Bind through the ADBC wire protocol."""
        ...

    def bind_stream(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[BindCursor]:
        """Bind stream through the ADBC wire protocol."""
        ...

    def get_info(self, session_id: str, args_json: str) -> ExecuteResponse:
        """Get info through the ADBC wire protocol."""
        ...

    def get_objects(self, session_id: str, args_json: str) -> ExecuteResponse:
        """Get objects through the ADBC wire protocol."""
        ...

    def get_table_schema(self, session_id: str, args_json: str) -> SchemaResponse:
        """Get table schema through the ADBC wire protocol."""
        ...

    def get_table_types(self, session_id: str) -> ExecuteResponse:
        """Get table types through the ADBC wire protocol."""
        ...

    def get_statistic_names(self, session_id: str) -> ExecuteResponse:
        """Get statistic names through the ADBC wire protocol."""
        ...

    def get_statistics(self, session_id: str, args_json: str) -> ExecuteResponse:
        """Get statistics through the ADBC wire protocol."""
        ...

    def read_partition(self, session_id: str, payload: bytes) -> ExecuteResponse:
        """Read partition through the ADBC wire protocol."""
        ...
