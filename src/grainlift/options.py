# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Strict bounded JSON codecs for Grainlift's four ADBC option types."""

from __future__ import annotations

import base64
import binascii
import json
import math
from collections.abc import Mapping
from typing import Any

from .api import AdbcError, OptionValue


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def decode_json(encoded: str, limit: int) -> Any:
    """Parse bounded input JSON and reject duplicate object fields or nonfinite numbers."""
    if len(encoded.encode("utf-8")) > limit:
        raise AdbcError("JSON input exceeds configured limit", "invalid_arguments")
    try:
        return json.loads(encoded, object_pairs_hook=_object, parse_constant=_reject_constant)
    except (ValueError, TypeError, RecursionError):
        raise AdbcError("Invalid JSON arguments", "invalid_arguments") from None


def _reject_constant(value: str) -> None:
    raise ValueError("Nonfinite JSON number")


def validate_key(key: object) -> str:
    """Require a nonempty option key without embedded NUL characters."""
    if not isinstance(key, str) or not key or "\0" in key:
        raise AdbcError("Invalid option key", "invalid_arguments")
    return key


def decode_value(value: object) -> OptionValue:
    """Decode a strict tagged option value without implicit boolean or numeric coercion."""
    if not isinstance(value, dict) or set(value) != {"type", "value"}:
        raise AdbcError("Invalid option value", "invalid_arguments")
    kind, raw = value["type"], value["value"]
    if kind == "string" and type(raw) is str:
        return raw
    if kind == "int" and type(raw) is int and -(2**63) <= raw < 2**63:
        return raw
    if kind == "double" and type(raw) in (int, float):
        try:
            number = float(raw)
            if math.isfinite(number):
                return number
        except OverflowError:
            pass
    if kind == "bytes" and type(raw) is str:
        try:
            return base64.b64decode(raw, validate=True)
        except (ValueError, binascii.Error):
            pass
    raise AdbcError("Invalid option type or value", "invalid_arguments")


def encode_value(value: OptionValue, value_type: str | None = None) -> dict[str, object]:
    """Encode a backend option while enforcing exact type and finite numeric ranges."""
    kinds = {str: "string", bytes: "bytes", int: "int", float: "double"}
    kind = kinds.get(type(value))
    if kind is None or (value_type is not None and kind != value_type):
        raise AdbcError("Backend returned an incompatible option type", "invalid_data")
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise AdbcError("Backend integer option is out of range", "invalid_data")
    if type(value) is float and not math.isfinite(value):
        raise AdbcError("Backend double option is not finite", "invalid_data")
    return {"type": kind, "value": base64.b64encode(value).decode("ascii") if isinstance(value, bytes) else value}


def decode_options(encoded: str, limit: int) -> dict[str, OptionValue]:
    """Decode the wire list of named options, rejecting duplicate option keys."""
    raw = decode_json(encoded, limit)
    if not isinstance(raw, list):
        raise AdbcError("Options must be a list", "invalid_arguments")
    result: dict[str, OptionValue] = {}
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"key", "type", "value"}:
            raise AdbcError("Invalid named option", "invalid_arguments")
        key = validate_key(item["key"])
        if key in result:
            raise AdbcError("Duplicate option key", "invalid_arguments")
        result[key] = decode_value({"type": item["type"], "value": item["value"]})
    return result


def configured_options(options: Mapping[str, OptionValue] | None, limit: int) -> dict[str, OptionValue]:
    """Copy and validate authoritative server options before accepting connections."""
    result = dict(options or {})
    encoded = json.dumps([{"key": validate_key(key), **encode_value(value)} for key, value in result.items()])
    if len(encoded.encode()) > limit:
        raise ValueError("Configured options exceed request limit")
    return result
