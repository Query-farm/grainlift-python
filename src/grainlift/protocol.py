# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Grainlift 0.2.0 wire schemas. Keep aligned with grainlift-protocol."""

from dataclasses import dataclass
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import AnnotatedBatch, CallContext, ExchangeState, OutputCollector, ProducerState, Stream


def schema(*fields: tuple[str, pa.DataType, bool]) -> pa.Schema:
    """Build a wire schema from name, type, and nullability triples."""
    return pa.schema([pa.field(*field) for field in fields])


OK = schema(("ok", pa.bool_(), False))
SESSION = schema(("session_id", pa.string(), False))
STATEMENT = schema(("session_id", pa.string(), False), ("statement_id", pa.string(), False))
EXECUTE = schema(
    ("result_id", pa.string(), False),
    ("rows_affected", pa.int64(), True),
    ("schema_ipc", pa.binary(), False),
)
SCHEMA = schema(("schema_ipc", pa.binary(), False))
VALUE = schema(("value_json", pa.string(), False))
UPDATE = schema(("rows_affected", pa.int64(), True))
PARTITIONS = schema(
    ("rows_affected", pa.int64(), False),
    ("schema_ipc", pa.binary(), False),
    ("partitions_json", pa.string(), False),
)

Ok = Annotated[pa.RecordBatch, OK]
Session = Annotated[pa.RecordBatch, SESSION]
Statement = Annotated[pa.RecordBatch, STATEMENT]
Execute = Annotated[pa.RecordBatch, EXECUTE]
Schema = Annotated[pa.RecordBatch, SCHEMA]
Value = Annotated[pa.RecordBatch, VALUE]
Update = Annotated[pa.RecordBatch, UPDATE]
Partitions = Annotated[pa.RecordBatch, PARTITIONS]


def batch(output: pa.Schema, **values: object) -> pa.RecordBatch:
    """Build a single-row wire response with an explicit schema."""
    return pa.RecordBatch.from_pydict({key: [value] for key, value in values.items()}, schema=output)


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
        finish = input.custom_metadata is not None and b"GRAINLIFT:bind_finish" in input.custom_metadata
        ctx.implementation.push_binding(
            self.session_id, self.statement_id, self.upload_id, self.sequence, input.batch, finish, ctx
        )
        out.emit(batch(OK, ok=True))
        self.sequence += 1

    def on_cancel(self, ctx: CallContext) -> None:
        """Discard incomplete input without unbinding a successfully finished upload."""
        ctx.implementation.cancel_binding(self.session_id, self.statement_id, self.upload_id, ctx)


class Grainlift(Protocol):
    """Describe the public Grainlift ADBC wire methods."""

    protocol_name: ClassVar[str] = "org.queryfarm.Grainlift.v1"
    protocol_version: ClassVar[str] = "0.2.0"

    def open_connection(self, target: str, database_options_json: str, connection_options_json: str) -> Session:
        """Authenticate and allocate a connection within the service quota."""
        ...

    def close_connection(self, session_id: str) -> Ok:
        """Close a connection and all child handles."""
        ...

    def new_statement(self, session_id: str) -> Statement:
        """Allocate an empty statement within the session quota."""
        ...

    def close_statement(self, session_id: str, statement_id: str) -> Ok:
        """Close a statement and its active result."""
        ...

    def set_sql_query(self, session_id: str, statement_id: str, sql: str) -> Ok:
        """Replace statement SQL after validating its encoded size."""
        ...

    def execute(self, session_id: str, statement_id: str) -> Execute:
        """Execute SQL and return its schema and lazy batch iterator."""
        ...

    def execute_schema(self, session_id: str, statement_id: str) -> Schema:
        """Infer the result schema without opening a cursor."""
        ...

    def close_result(self, session_id: str, result_id: str) -> Ok:
        """Release a result cursor and retained replay batch."""
        ...

    def read_result(self, session_id: str, result_id: str, sequence: int) -> Stream[ResultCursor]:
        """Open a pull stream at the requested result sequence."""
        ...

    def set_connection_option(self, session_id: str, key: str, value_json: str) -> Ok:
        """Set connection option through the ADBC wire protocol."""
        ...

    def get_connection_option(self, session_id: str, key: str, value_type: str) -> Value:
        """Get connection option through the ADBC wire protocol."""
        ...

    def commit(self, session_id: str) -> Ok:
        """Commit through the ADBC wire protocol."""
        ...

    def rollback(self, session_id: str) -> Ok:
        """Rollback through the ADBC wire protocol."""
        ...

    def cancel_connection(self, session_id: str) -> Ok:
        """Cancel connection through the ADBC wire protocol."""
        ...

    def cancel_statement(self, session_id: str, statement_id: str) -> Ok:
        """Cancel statement through the ADBC wire protocol."""
        ...

    def prepare(self, session_id: str, statement_id: str) -> Ok:
        """Prepare through the ADBC wire protocol."""
        ...

    def execute_update(self, session_id: str, statement_id: str) -> Update:
        """Execute update through the ADBC wire protocol."""
        ...

    def execute_partitions(self, session_id: str, statement_id: str) -> Partitions:
        """Execute partitions through the ADBC wire protocol."""
        ...

    def get_parameter_schema(self, session_id: str, statement_id: str) -> Schema:
        """Get parameter schema through the ADBC wire protocol."""
        ...

    def set_substrait_plan(self, session_id: str, statement_id: str, payload: bytes) -> Ok:
        """Set substrait plan through the ADBC wire protocol."""
        ...

    def set_statement_option(self, session_id: str, statement_id: str, key: str, value_json: str) -> Ok:
        """Set statement option through the ADBC wire protocol."""
        ...

    def get_statement_option(self, session_id: str, statement_id: str, key: str, value_type: str) -> Value:
        """Get statement option through the ADBC wire protocol."""
        ...

    def bind(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[BindCursor]:
        """Bind through the ADBC wire protocol."""
        ...

    def bind_stream(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[BindCursor]:
        """Bind stream through the ADBC wire protocol."""
        ...

    def get_info(self, session_id: str, args_json: str) -> Execute:
        """Get info through the ADBC wire protocol."""
        ...

    def get_objects(self, session_id: str, args_json: str) -> Execute:
        """Get objects through the ADBC wire protocol."""
        ...

    def get_table_schema(self, session_id: str, args_json: str) -> Schema:
        """Get table schema through the ADBC wire protocol."""
        ...

    def get_table_types(self, session_id: str) -> Execute:
        """Get table types through the ADBC wire protocol."""
        ...

    def get_statistic_names(self, session_id: str) -> Execute:
        """Get statistic names through the ADBC wire protocol."""
        ...

    def get_statistics(self, session_id: str, args_json: str) -> Execute:
        """Get statistics through the ADBC wire protocol."""
        ...

    def read_partition(self, session_id: str, payload: bytes) -> Execute:
        """Read partition through the ADBC wire protocol."""
        ...
