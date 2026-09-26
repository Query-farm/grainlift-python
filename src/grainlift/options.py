# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Typed ADBC option records and bounded server configuration codecs."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass

from vgi_rpc.utils import ArrowSerializableDataclass

from .api import AdbcError, OptionValue


@dataclass(frozen=True, kw_only=True)
class WireOptionValue(ArrowSerializableDataclass):
    """Represent one typed ADBC option without JSON or base64 encoding.

    Attributes:
        kind: One of string, bytes, int, or double.
        string_value: String payload, set only for the string kind.
        bytes_value: Binary payload, set only for the bytes kind.
        int_value: Signed 64-bit payload, set only for the int kind.
        double_value: IEEE 754 float64 payload, set only for the double kind.
    """

    kind: str
    string_value: str | None = None
    bytes_value: bytes | None = None
    int_value: int | None = None
    double_value: float | None = None

    def __post_init__(self) -> None:
        """Require exactly the payload field selected by the discriminator."""
        fields = {
            "string": self.string_value,
            "bytes": self.bytes_value,
            "int": self.int_value,
            "double": self.double_value,
        }
        selected = fields.get(self.kind)
        if selected is None or any(value is not None for kind, value in fields.items() if kind != self.kind):
            raise AdbcError("Invalid typed option payload", "invalid_data")
        _validate_value(selected, self.kind)

    @classmethod
    def from_value(cls, value: OptionValue, value_type: str) -> WireOptionValue:
        """Validate a backend option and select its native Arrow payload field."""
        _validate_value(value, value_type)
        return cls(
            kind=value_type,
            string_value=value if isinstance(value, str) else None,
            bytes_value=value if isinstance(value, bytes) else None,
            int_value=value if type(value) is int else None,
            double_value=value if type(value) is float else None,
        )

    def to_value(self) -> OptionValue:
        """Return the validated native Python option value."""
        for value in (self.string_value, self.bytes_value, self.int_value, self.double_value):
            if value is not None:
                return value
        raise AdbcError("Invalid typed option payload", "invalid_data")


@dataclass(frozen=True, kw_only=True)
class NamedOption(ArrowSerializableDataclass):
    """Associate an extensible driver option key with exactly one typed value.

    Attributes:
        key: Nonempty driver-defined option name without NUL characters.
        value: Binary, string, signed integer or double option value.
    """

    key: str
    value: WireOptionValue

    def __post_init__(self) -> None:
        """Validate option names and payload types before dispatch."""
        validate_key(self.key)
        if not isinstance(self.value, WireOptionValue):
            raise AdbcError("Invalid named option value", "invalid_arguments")
        self.value.__post_init__()


def option_mapping(options: list[NamedOption]) -> dict[str, OptionValue]:
    """Validate typed option lists without dropping duplicates or coercing values."""
    if not isinstance(options, list):
        raise AdbcError("Options must be a typed list", "invalid_arguments")
    result: dict[str, OptionValue] = {}
    for option in options:
        if not isinstance(option, NamedOption):
            raise AdbcError("Invalid named option", "invalid_arguments")
        option.__post_init__()
        if option.key in result:
            raise AdbcError("Duplicate option key", "invalid_arguments")
        result[option.key] = option.value.to_value()
    return result


def validate_key(key: object) -> str:
    """Require a nonempty option key without embedded NUL characters."""
    if not isinstance(key, str) or not key or "\0" in key:
        raise AdbcError("Invalid option key", "invalid_arguments")
    return key


def _validate_value(value: OptionValue, value_type: str | None = None) -> str:
    kinds = {str: "string", bytes: "bytes", int: "int", float: "double"}
    kind = kinds.get(type(value))
    if kind is None or (value_type is not None and kind != value_type):
        raise AdbcError("Backend returned an incompatible option type", "invalid_data")
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise AdbcError("Backend integer option is out of range", "invalid_data")
    return kind


def encode_value(value: OptionValue, value_type: str | None = None) -> dict[str, object]:
    """Measure server configuration using its stable local JSON representation."""
    kind = _validate_value(value, value_type)
    return {"type": kind, "value": base64.b64encode(value).decode("ascii") if isinstance(value, bytes) else value}


def configured_options(options: Mapping[str, OptionValue] | None, limit: int) -> dict[str, OptionValue]:
    """Copy and validate authoritative server options before accepting connections."""
    result = dict(options or {})
    encoded = json.dumps([{"key": validate_key(key), **encode_value(value)} for key, value in result.items()])
    if len(encoded.encode()) > limit:
        raise ValueError("Configured options exceed request limit")
    return result
