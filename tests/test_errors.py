# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""ADBC error fields match their C ABI widths and preserve opaque detail bytes."""

import base64
import json
from typing import Any

import pytest

from grainlift import AdbcError


@pytest.mark.parametrize("vendor_code", [-(2**31), -1, 0, 2**31 - 1])
def test_vendor_code_signed_int32_boundaries(vendor_code: int) -> None:
    """Preserve the entire ADBC vendor-code range without narrowing or coercion."""
    wire = json.loads(str(AdbcError("safe", vendor_code=vendor_code)))
    assert wire["vendor_code"] == vendor_code


@pytest.mark.parametrize(
    "arguments",
    [
        {"vendor_code": -(2**31) - 1},
        {"vendor_code": 2**31},
        {"vendor_code": True},
        {"vendor_code": 1.0},
        {"status": "wat"},
        {"status": "ok"},
        {"status": "INVALID_DATA"},
        {"sqlstate": ""},
        {"sqlstate": "000000"},
        {"sqlstate": "é0000"},
        {"sqlstate": b"00000"},
        {"details": {"x": "text"}},
        {"details": {"x": bytearray(b"x")}},
        {"details": {1: b"x"}},
    ],
)
def test_invalid_adbc_error_fields_rejected(arguments: dict[str, Any]) -> None:
    """Fail locally before emitting error fields a native ADBC client cannot represent."""
    with pytest.raises(ValueError):
        AdbcError("safe", **arguments)


def test_binary_details_and_sqlstate_preserved() -> None:
    """Carry empty and arbitrary byte details together with the original SQLSTATE."""
    details = {"binary": bytes(range(256)), "empty": b""}
    wire = json.loads(str(AdbcError("safe", "integrity", sqlstate="23000", details=details)))
    decoded = {key: base64.b64decode(value, validate=True) for key, value in wire["details"]}
    assert decoded == details
    assert wire["sqlstate"] == list(b"23000") and wire["status"] == "integrity"


@pytest.mark.parametrize("message", [None, b"bytes", 1])
def test_error_message_requires_string(message: Any) -> None:
    """Prevent accidental stringification of unvalidated downstream objects."""
    with pytest.raises(ValueError, match="message"):
        AdbcError(message)
