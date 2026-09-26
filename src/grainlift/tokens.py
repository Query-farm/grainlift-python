# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Versioned, authenticated partition claims hidden behind opaque ADBC tokens."""

import hmac
from dataclasses import dataclass

from .api import AdbcError
from .binding import decode_batch
from .wire import ControlRecord


@dataclass(frozen=True, kw_only=True)
class PartitionClaims(ControlRecord):
    """Define the signed content of a Grainlift partition token.

    Attributes:
        version: Claims schema version; exactly 1 in the GLP2 envelope.
        expires_at_ms: Unix epoch milliseconds at or after which the token is expired.
        owner: HMAC-derived identity binding the target and authenticated principal.
        descriptor: Opaque downstream ADBC partition descriptor, preserved byte-for-byte.
    """

    version: int
    expires_at_ms: int
    owner: str
    descriptor: bytes

    def __post_init__(self) -> None:
        """Require the supported version and bounded, unambiguous claim values."""
        if (
            type(self.version) is not int
            or self.version != 1
            or type(self.expires_at_ms) is not int
            or not 0 <= self.expires_at_ms < 2**63
            or not isinstance(self.owner, str)
            or len(self.owner) != 64
            or any(char not in "0123456789abcdef" for char in self.owner)
            or not isinstance(self.descriptor, bytes)
        ):
            raise AdbcError("Invalid partition claims", "invalid_data")


def seal_partition(claims: PartitionClaims, key: bytes, limit: int) -> bytes:
    """Encode and authenticate one typed record within the complete token budget."""
    claims.__post_init__()
    body = claims.serialize_to_bytes()
    if len(body) + 36 > limit:
        raise AdbcError("Partition descriptor exceeds configured limit", "invalid_data")
    return b"GLP2" + hmac.digest(key, body, "sha256") + body


def unseal_partition(payload: bytes, key: bytes, limit: int) -> PartitionClaims:
    """Check envelope size and signature before decoding any internal claim fields."""
    if len(payload) > limit:
        raise AdbcError("Partition descriptor exceeds configured limit", "invalid_arguments")
    if len(payload) < 36 or payload[:4] != b"GLP2":
        raise AdbcError("Partition descriptor is unavailable", "not_found")
    signature, body = payload[4:36], payload[36:]
    if not hmac.compare_digest(signature, hmac.digest(key, body, "sha256")):
        raise AdbcError("Partition descriptor is unavailable", "not_found")
    try:
        return PartitionClaims.deserialize_from_batch(decode_batch(body, limit))
    except (AdbcError, ValueError, TypeError, OverflowError):
        raise AdbcError("Partition descriptor is unavailable", "not_found") from None
