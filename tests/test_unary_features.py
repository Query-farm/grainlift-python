# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Backend capability delegation, typed options, quotas, and partition ownership."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pyarrow as pa
import pytest
from test_service import Reader, context, open_session, value

from grainlift import (
    AdbcError,
    Connection,
    Limits,
    OptionValue,
    PartitionedResult,
    QueryResult,
    Service,
    Statement,
    Worker,
)
from grainlift.options import decode_options, decode_value, encode_value

SCHEMA = pa.schema([("value", pa.int64())])


class FeatureStatement(Statement):
    """Record capability calls without emulating any database transaction behavior."""

    def __init__(self) -> None:
        """Create independent statement state."""
        self.calls: list[tuple[str, object]] = []
        self.options: dict[str, OptionValue] = {}
        self.partitions = [b"partition-one", b"partition-two"]
        self.rows_affected: int | None = 7
        self.closed = False

    def set_sql_query(self, sql: str) -> None:
        """Retain SQL configuration."""
        self.calls.append(("sql", sql))

    def set_substrait_plan(self, payload: bytes) -> None:
        """Retain a serialized Substrait plan."""
        self.calls.append(("substrait", payload))

    def prepare(self) -> None:
        """Record explicit preparation."""
        self.calls.append(("prepare", None))

    def get_parameter_schema(self) -> pa.Schema:
        """Return a stable parameter schema."""
        return SCHEMA

    def execute_schema(self) -> pa.Schema:
        """Return the stable result schema."""
        return SCHEMA

    def execute(self) -> QueryResult:
        """Return a lazy single-batch result."""
        return QueryResult(SCHEMA, Reader([pa.record_batch([[7]], schema=SCHEMA)]))

    def execute_update(self) -> int | None:
        """Return the configured backend update count."""
        self.calls.append(("update", None))
        return self.rows_affected

    def execute_partitions(self) -> PartitionedResult:
        """Return raw downstream descriptors for service authentication."""
        return PartitionedResult(SCHEMA, self.partitions, rows_affected=7)

    def set_option(self, key: str, value: OptionValue) -> None:
        """Retain typed options without converting their representation."""
        self.options[key] = value

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Return the configured backend option."""
        return self.options[key]

    def cancel(self) -> None:
        """Record statement-specific cancellation."""
        self.calls.append(("cancel", None))

    def close(self) -> None:
        """Mark backend statement resources released."""
        self.closed = True


class FeatureConnection(Connection):
    """Record metadata filters, transaction hooks, and cursor ownership."""

    def __init__(self) -> None:
        """Create independent connection state."""
        self.options: dict[str, OptionValue] = {}
        self.statements: list[FeatureStatement] = []
        self.calls: list[tuple[str, object]] = []
        self.readers: list[Reader] = []
        self.closed = False

    def new_statement(self) -> FeatureStatement:
        """Allocate an independent backend statement."""
        statement = FeatureStatement()
        self.statements.append(statement)
        return statement

    def _metadata(self, method: str, arguments: object = None) -> QueryResult:
        self.calls.append((method, arguments))
        reader = Reader([pa.record_batch([[1]], schema=SCHEMA)])
        self.readers.append(reader)
        return QueryResult(SCHEMA, reader)

    def get_info(self, codes: list[int] | None) -> QueryResult:
        """Retain the exact requested information codes."""
        return self._metadata("info", codes)

    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult:
        """Retain every object-discovery filter without narrowing semantics."""
        return self._metadata("objects", (depth, catalog, db_schema, table_name, table_types, column_name))

    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema:
        """Retain table coordinates and return its schema."""
        self.calls.append(("table_schema", (catalog, db_schema, table_name)))
        return SCHEMA

    def get_table_types(self) -> QueryResult:
        """Return a metadata cursor."""
        return self._metadata("types")

    def get_statistic_names(self) -> QueryResult:
        """Return a statistic-name cursor."""
        return self._metadata("statistic_names")

    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult:
        """Retain exact-versus-approximate statistics semantics."""
        return self._metadata("statistics", (catalog, db_schema, table_name, approximate))

    def read_partition(self, descriptor: bytes) -> QueryResult:
        """Accept only a service-authenticated raw backend descriptor."""
        return self._metadata("partition", descriptor)

    def set_option(self, key: str, value: OptionValue) -> None:
        """Retain a typed option or inject a cleanup-triggering failure."""
        if key == "fail":
            raise AdbcError("Rejected option", "invalid_arguments", sqlstate="HY024")
        self.options[key] = value

    def get_option(self, key: str, value_type: str) -> OptionValue:
        """Return the configured option without implicit conversions."""
        return self.options[key]

    def commit(self) -> None:
        """Record the backend commit hook."""
        self.calls.append(("commit", None))

    def rollback(self) -> None:
        """Record the backend rollback hook."""
        self.calls.append(("rollback", None))

    def close(self) -> None:
        """Mark connection resources released."""
        self.closed = True


class FeatureWorker(Worker):
    """Create inspectable capability-bearing connections."""

    def __init__(self) -> None:
        """Retain connections for lifecycle assertions."""
        self.connections: list[FeatureConnection] = []

    def connect(self, principal: str) -> FeatureConnection:
        """Allocate a fresh backend connection."""
        connection = FeatureConnection()
        self.connections.append(connection)
        return connection


@pytest.mark.parametrize("option", ["text", b"\x00\xff", -(2**63), 2**63 - 1, 1.25])
def test_typed_connection_and_statement_option_roundtrip(option: OptionValue) -> None:
    """Preserve all four ADBC option representations through their exact wire codec."""
    encoded = json.dumps(encode_value(option))
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        service.set_connection_option(sid, "setting", encoded, ctx)
        service.set_statement_option(sid, stmt, "setting", encoded, ctx)
        kind = str(encode_value(option)["type"])
        for response in (
            service.get_connection_option(sid, "setting", kind, ctx),
            service.get_statement_option(sid, stmt, "setting", kind, ctx),
        ):
            actual = decode_value(json.loads(value(response, "value_json")))
            assert actual == option and type(actual) is type(option)


@pytest.mark.parametrize(
    "encoded",
    [
        "{}",
        "null",
        '[{"key":"x","type":"int","value":true}]',
        '[{"key":"x","type":"int","value":9223372036854775808}]',
        '[{"key":"x","type":"double","value":NaN}]',
        '[{"key":"x","type":"bytes","value":"%%%"}]',
        '[{"key":"x","type":"string","value":null}]',
        '[{"key":"x","type":"string","value":"x","extra":1}]',
        '[{"key":"x","key":"y","type":"int","value":1}]',
        '[{"key":"x","type":"int","value":1},{"key":"x","type":"int","value":2}]',
    ],
)
def test_invalid_option_wire_rejected(encoded: str) -> None:
    """Reject malformed or ambiguous typed options before a backend callback."""
    with pytest.raises(AdbcError) as exc:
        decode_options(encoded, 4096)
    assert exc.value.status == "invalid_arguments"


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_option_input_size_boundary(headroom: int) -> None:
    """Apply an inclusive byte budget to named option JSON."""
    encoded = '[{"key":"x","type":"bytes","value":"AP8="}]'
    if headroom < 0:
        with pytest.raises(AdbcError, match="exceeds"):
            decode_options(encoded, len(encoded) + headroom)
    else:
        assert decode_options(encoded, len(encoded) + headroom) == {"x": b"\0\xff"}


def test_default_open_connection_closes_after_failed_option() -> None:
    """Release a newly opened backend when applying a caller option fails."""
    worker = FeatureWorker()
    with Service(worker) as service:
        with pytest.raises(AdbcError, match="HY024|Rejected option"):
            service.open_connection("default", "[]", '[{"key":"fail","type":"int","value":1}]', context(service))
        assert worker.connections[0].closed
        assert service._opening == 0 and not service._sessions


def test_server_options_authoritative_across_open_and_mutation() -> None:
    """Reject injected destinations and credentials in either caller option scope."""

    class ConfiguredWorker(FeatureWorker):
        """Capture authoritative database options without logging values."""

        def open_connection(
            self,
            principal: str,
            database_options: Mapping[str, OptionValue],
            connection_options: Mapping[str, OptionValue],
        ) -> Connection:
            """Assert server-selected database settings and apply connection settings."""
            assert database_options == {"uri": "configured-target"}
            return super().open_connection(principal, {}, connection_options)

    worker = ConfiguredWorker()
    with Service(
        worker, database_options={"uri": "configured-target"}, connection_options={"role": "fixed"}
    ) as service:
        ctx = context(service)
        for key in ("uri", "role"):
            supplied = json.dumps([{"key": key, "type": "string", "value": "injected"}])
            for database, connection in ((supplied, "[]"), ("[]", supplied)):
                with pytest.raises(AdbcError) as exc:
                    service.open_connection("default", database, connection, ctx)
                assert exc.value.status == "unauthorized"
        assert not worker.connections
        sid, ctx = open_session(service)
        assert worker.connections[0].options == {"role": "fixed"}
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        for key in ("uri", "role"):
            with pytest.raises(AdbcError) as exc:
                service.set_connection_option(sid, key, '{"type":"string","value":"injected"}', ctx)
            assert exc.value.status == "unauthorized"
            with pytest.raises(AdbcError) as exc:
                service.set_statement_option(sid, stmt, key, '{"type":"string","value":"injected"}', ctx)
            assert exc.value.status == "unauthorized"
        assert not worker.connections[0].statements[0].options
        visible = service.get_connection_option(sid, "role", "string", ctx)
        assert decode_value(json.loads(value(visible, "value_json"))) == "fixed"


def test_statement_and_transaction_hooks_reach_backend() -> None:
    """Delegate preparation, schema, plan, update, and transaction operations."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        service.set_sql_query(sid, stmt, "query", ctx)
        service.prepare(sid, stmt, ctx)
        assert service.get_parameter_schema(sid, stmt, ctx).num_rows == 1
        assert service.execute_schema(sid, stmt, ctx).num_rows == 1
        assert service.execute_update(sid, stmt, ctx).column("rows_affected")[0].as_py() == 7
        service.set_substrait_plan(sid, stmt, b"plan", ctx)
        service.commit(sid, ctx)
        service.rollback(sid, ctx)
        backend = worker.connections[0]
        assert backend.calls == [("commit", None), ("rollback", None)]
        assert backend.statements[0].calls == [
            ("sql", "query"),
            ("prepare", None),
            ("update", None),
            ("substrait", b"plan"),
        ]
        service.close_statement(sid, stmt, ctx)
        assert backend.statements[0].closed


def test_metadata_filters_and_result_quota() -> None:
    """Preserve discovery filters and reserve metadata cursors within the result quota."""
    worker = FeatureWorker()
    with Service(worker, limits=Limits(results_per_session=2)) as service:
        sid, ctx = open_session(service)
        first = service.get_info(sid, '{"codes":[0,4294967295]}', ctx)
        service.get_objects(
            sid, '{"depth":0,"catalog":"","db_schema":null,"table_name":"x%","table_type":[],"column_name":"_%"}', ctx
        )
        backend = worker.connections[0]
        assert backend.calls == [("info", [0, 4294967295]), ("objects", (0, "", None, "x%", [], "_%"))]
        with pytest.raises(AdbcError, match="Result limit"):
            service.get_table_types(sid, ctx)
        assert len(backend.readers) == 2
        service.close_result(sid, value(first, "result_id"), ctx)
        assert backend.readers[0].closed
        service.get_statistic_names(sid, ctx)
        service.close_connection(sid, ctx)
        assert all(reader.closed for reader in backend.readers)


@pytest.mark.parametrize("args", ['{"codes":[true]}', '{"codes":[-1]}', '{"codes":[4294967296]}', '{"unknown":1}'])
def test_invalid_metadata_arguments_do_not_reach_worker(args: str) -> None:
    """Reject invalid information codes and unknown argument names before execution."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        with pytest.raises(AdbcError) as exc:
            service.get_info(sid, args, ctx)
        assert exc.value.status == "invalid_arguments"
        assert not worker.connections[0].calls


def test_partition_descriptors_require_owner_instance_and_live_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permit same-principal reconnects while rejecting transfer, tampering, expiry, and another service."""
    worker = FeatureWorker()
    with Service(worker) as service, Service(worker) as other_service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        response = service.execute_partitions(sid, stmt, ctx)
        descriptor = bytes(json.loads(value(response, "partitions_json"))[0])
        service.close_connection(sid, ctx)
        fresh, fresh_ctx = open_session(service)
        service.read_partition(fresh, descriptor, fresh_ctx)
        assert worker.connections[-1].calls == [("partition", b"partition-one")]
        bob, bob_ctx = open_session(service, "bob")
        other, other_ctx = open_session(other_service)
        for implementation, session_id, payload, call in (
            (service, bob, descriptor, bob_ctx),
            (service, fresh, descriptor[:-1] + b"x", fresh_ctx),
            (other_service, other, descriptor, other_ctx),
        ):
            with pytest.raises(AdbcError) as exc:
                implementation.read_partition(session_id, payload, call)
            assert exc.value.status == "not_found"
        expired = time.time() + 301
        monkeypatch.setattr(time, "time", lambda: expired)
        with pytest.raises(AdbcError, match="unavailable"):
            service.read_partition(fresh, descriptor, fresh_ctx)


@pytest.mark.parametrize("count", [1, 2, 3])
def test_partition_count_boundary(count: int) -> None:
    """Bound descriptor cardinality before creating exported partition wrappers."""
    worker = FeatureWorker()
    with Service(worker, limits=Limits(partitions_per_result=2)) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        worker.connections[0].statements[0].partitions = [b"x"] * count
        if count > 2:
            with pytest.raises(AdbcError, match="partitioned result"):
                service.execute_partitions(sid, stmt, ctx)
        else:
            response = service.execute_partitions(sid, stmt, ctx)
            assert len(json.loads(value(response, "partitions_json"))) == count


@pytest.mark.parametrize("bad", [True, 2**63, float("nan")])
def test_invalid_backend_update_counts_rejected(bad: Any) -> None:
    """Reject invalid affected-row counts without allowing Arrow coercions."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        worker.connections[0].statements[0].rows_affected = bad
        with pytest.raises(AdbcError) as exc:
            service.execute_update(sid, stmt, ctx)
        assert exc.value.status == "invalid_data"


@pytest.mark.parametrize("headroom", [-1, 0, 1])
@pytest.mark.parametrize("option", [b"x" * 8, "é🌾"])
def test_typed_option_output_boundary(headroom: int, option: OptionValue) -> None:
    """Bound the encoded option response, including its JSON and base64 overhead."""
    budget = len(json.dumps(encode_value(option)).encode()) + headroom
    kind = str(encode_value(option)["type"])
    worker = FeatureWorker()
    with Service(worker, limits=Limits(batch_bytes=budget)) as service:
        sid, ctx = open_session(service)
        worker.connections[0].options["binary"] = option
        if headroom < 0:
            with pytest.raises(AdbcError, match="exceeds"):
                service.get_connection_option(sid, "binary", kind, ctx)
        else:
            response = service.get_connection_option(sid, "binary", kind, ctx)
            assert decode_value(json.loads(value(response, "value_json"))) == option


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_unicode_option_input_uses_utf8_byte_limit(headroom: int) -> None:
    """Count encoded UTF-8 bytes instead of Unicode characters before backend delegation."""
    encoded = json.dumps({"type": "string", "value": "é🌾"}, ensure_ascii=False)
    worker = FeatureWorker()
    with Service(worker, limits=Limits(request_bytes=len(encoded.encode()) + headroom)) as service:
        sid, ctx = open_session(service)
        if headroom < 0:
            with pytest.raises(AdbcError, match="exceeds"):
                service.set_connection_option(sid, "text", encoded, ctx)
            assert not worker.connections[0].options
        else:
            service.set_connection_option(sid, "text", encoded, ctx)
            assert worker.connections[0].options == {"text": "é🌾"}


@pytest.mark.parametrize("size", [15, 16, 17])
def test_substrait_input_boundary(size: int) -> None:
    """Reject over-budget plans before invoking the backend setter."""
    worker = FeatureWorker()
    with Service(worker, limits=Limits(request_bytes=16)) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        if size > 16:
            with pytest.raises(AdbcError, match="exceeds"):
                service.set_substrait_plan(sid, stmt, b"x" * size, ctx)
            assert not worker.connections[0].statements[0].calls
        else:
            service.set_substrait_plan(sid, stmt, b"x" * size, ctx)
            assert worker.connections[0].statements[0].calls == [("substrait", b"x" * size)]


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_partition_serialized_output_boundary(monkeypatch: pytest.MonkeyPatch, headroom: int) -> None:
    """Include schema and signed descriptor JSON in the partition response budget."""
    monkeypatch.setattr(time, "time", lambda: 1000.0)
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        stmt = value(service.new_statement(sid, ctx), "statement_id")
        original = service.execute_partitions(sid, stmt, ctx)
        size = len(value(original, "partitions_json")) + len(original.column("schema_ipc")[0].as_py())
        service.limits = replace(service.limits, batch_bytes=size + headroom)
        if headroom < 0:
            with pytest.raises(AdbcError, match="exceeds"):
                service.execute_partitions(sid, stmt, ctx)
        else:
            response = service.execute_partitions(sid, stmt, ctx)
            assert response.equals(original)


def test_statistics_and_table_discovery_preserve_arguments() -> None:
    """Forward nullable table coordinates and both approximation modes without coercion."""
    worker = FeatureWorker()
    with Service(worker) as service:
        sid, ctx = open_session(service)
        service.get_table_schema(sid, '{"catalog":"","db_schema":null,"table_name":"target"}', ctx)
        service.get_table_types(sid, ctx)
        for approximate in (True, False):
            service.get_statistics(
                sid,
                json.dumps({"catalog": "c%", "db_schema": "s_", "table_name": None, "approximate": approximate}),
                ctx,
            )
        assert worker.connections[0].calls == [
            ("table_schema", ("", None, "target")),
            ("types", None),
            ("statistics", ("c%", "s_", None, True)),
            ("statistics", ("c%", "s_", None, False)),
        ]


@pytest.mark.parametrize("field", ["results_per_session", "partitions_per_result", "bind_bytes"])
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, float("inf"), None])
def test_new_resource_limits_reject_invalid_values(field: str, bad: Any) -> None:
    """Keep result, partition, and cumulative upload bounds positive integral quotas."""
    with pytest.raises(ValueError, match=field):
        Limits(**{field: bad})
