# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Nominal Grainlift request records with ADBC argument semantics."""

from dataclasses import dataclass, field

from .api import AdbcError
from .options import NamedOption, WireOptionValue, option_mapping, validate_key
from .wire import ControlRecord


def _text(value: str | None, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or "\0" in value:
        raise AdbcError("Invalid text argument", "invalid_arguments")


def _handle(value: str) -> None:
    _text(value)
    if not value:
        raise AdbcError("Empty handle", "invalid_arguments")


class Request(ControlRecord):
    """Base for named control arguments validated before session mutation."""

    def __post_init__(self) -> None:
        """Validate semantic constraints in each concrete request."""


@dataclass(frozen=True, kw_only=True)
class OpenConnectionRequest(Request):
    """Open one connection to a server-authorized target.

    Attributes:
        target: Server-configured target name.
        database_options: Driver-defined database options; duplicate keys are invalid.
        connection_options: Driver-defined connection options; duplicate keys are invalid.
    """

    target: str
    database_options: list[NamedOption] = field(default_factory=list)
    connection_options: list[NamedOption] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Check target and both typed option lists."""
        _handle(self.target)
        option_mapping(self.database_options)
        option_mapping(self.connection_options)


@dataclass(frozen=True, kw_only=True)
class SetConnectionOptionRequest(Request):
    """Set a connection option without losing its ADBC value type.

    Attributes:
        session_id: Principal-owned connection handle.
        key: Driver-defined option name.
        value: Exactly one typed option value.
    """

    session_id: str
    key: str
    value: WireOptionValue

    def __post_init__(self) -> None:
        """Check handle, key and discriminated value."""
        _handle(self.session_id)
        validate_key(self.key)
        NamedOption(key=self.key, value=self.value)


@dataclass(frozen=True, kw_only=True)
class SetStatementOptionRequest(Request):
    """Set a statement option, including standard ADBC ingestion options.

    Attributes:
        session_id: Principal-owned connection handle.
        statement_id: Statement owned by that connection.
        key: Driver-defined option name.
        value: Exactly one typed option value.
    """

    session_id: str
    statement_id: str
    key: str
    value: WireOptionValue

    def __post_init__(self) -> None:
        """Check both handles, the key and discriminated value."""
        _handle(self.session_id)
        _handle(self.statement_id)
        NamedOption(key=self.key, value=self.value)


@dataclass(frozen=True, kw_only=True)
class GetInfoRequest(Request):
    """Request standard or vendor-specific ADBC information codes.

    Attributes:
        session_id: Principal-owned connection handle.
        codes: Unsigned 32-bit codes carried as int64; None requests all supported codes.
    """

    session_id: str
    codes: list[int] | None = None

    def __post_init__(self) -> None:
        """Preserve null versus empty lists and reject out-of-range codes."""
        _handle(self.session_id)
        if self.codes is not None and (
            not isinstance(self.codes, list)
            or any(type(code) is not int or not 0 <= code < 2**32 for code in self.codes)
        ):
            raise AdbcError("Invalid information codes", "invalid_arguments")


@dataclass(frozen=True, kw_only=True)
class GetObjectsRequest(Request):
    """Request hierarchical metadata with ADBC filter semantics.

    Attributes:
        session_id: Principal-owned connection handle.
        depth: ADBC depth: 0 all, 1 catalogs, 2 schemas, 3 tables.
        catalog: Catalog pattern; None means no filter, empty means unnamed catalog.
        db_schema: Schema pattern; None means no filter, empty means unnamed schema.
        table_name: Table name search pattern; None means no filter.
        table_types: Accepted table types; None means any type, empty means no types.
        column_name: Column name search pattern; None means no filter.
    """

    session_id: str
    depth: int
    catalog: str | None = None
    db_schema: str | None = None
    table_name: str | None = None
    table_types: list[str] | None = None
    column_name: str | None = None

    def __post_init__(self) -> None:
        """Validate depth and filter types without normalizing empty values."""
        _handle(self.session_id)
        if type(self.depth) is not int or self.depth not in {0, 1, 2, 3}:
            raise AdbcError("Invalid object depth", "invalid_arguments")
        for value in (self.catalog, self.db_schema, self.table_name, self.column_name):
            _text(value, nullable=True)
        if self.table_types is not None:
            if not isinstance(self.table_types, list):
                raise AdbcError("Invalid table type filter", "invalid_arguments")
            for value in self.table_types:
                _text(value)


@dataclass(frozen=True, kw_only=True)
class GetTableSchemaRequest(Request):
    """Identify one table by exact names, not search patterns.

    Attributes:
        session_id: Principal-owned connection handle.
        catalog: Exact catalog name, or None when not applicable.
        db_schema: Exact schema name, or None when not applicable.
        table_name: Required exact table name.
    """

    session_id: str
    catalog: str | None = None
    db_schema: str | None = None
    table_name: str

    def __post_init__(self) -> None:
        """Validate exact identifiers while preserving empty names."""
        _handle(self.session_id)
        _text(self.catalog, nullable=True)
        _text(self.db_schema, nullable=True)
        _text(self.table_name)


@dataclass(frozen=True, kw_only=True)
class GetStatisticsRequest(Request):
    """Request ADBC statistics without emulating unavailable backend results.

    Attributes:
        session_id: Principal-owned connection handle.
        catalog: Catalog search pattern, or None for no filter.
        db_schema: Schema search pattern, or None for no filter.
        table_name: Table search pattern, or None for no filter.
        approximate: Whether approximate or cached values are permitted.
    """

    session_id: str
    catalog: str | None = None
    db_schema: str | None = None
    table_name: str | None = None
    approximate: bool

    def __post_init__(self) -> None:
        """Validate filters and require an actual boolean approximation flag."""
        _handle(self.session_id)
        for value in (self.catalog, self.db_schema, self.table_name):
            _text(value, nullable=True)
        if type(self.approximate) is not bool:
            raise AdbcError("Invalid approximation flag", "invalid_arguments")
