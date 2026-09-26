# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Atomic replacement of bearer credentials without rebuilding the WSGI app."""

import hmac
import threading
from collections.abc import Mapping


class TokenStore:
    """Keep a replaceable token-to-principal mapping in one server process.

    Rotation affects every subsequent request, including stream continuations.
    Keep the same principal when rotating credentials for an existing session.
    Callers must load secrets from their trusted configuration source; this
    class does not watch files, expose an administrative HTTP API, or log values.
    """

    def __init__(self, tokens: Mapping[str, str]) -> None:
        """Validate and copy the initial nonempty credential mapping.

        Args:
            tokens: Bearer secrets mapped to authenticated principal names.
        """
        self._lock = threading.Lock()
        self._tokens: tuple[tuple[bytes, str], ...] = ()
        self.replace(tokens)

    def replace(self, tokens: Mapping[str, str]) -> None:
        """Atomically replace all credentials, leaving the previous set on error.

        Use an overlap set containing old and new credentials before removing
        the old credential once clients have switched. An already authenticated
        request may finish after removal; subsequent requests use the new set.

        Args:
            tokens: Complete replacement mapping of nonempty secrets/principals.
        """
        if not tokens or len(tokens) > 4096:
            raise ValueError("Configure between one and 4096 bearer credentials")
        updated: list[tuple[bytes, str]] = []
        for token, principal in tokens.items():
            if (
                not isinstance(token, str)
                or not isinstance(principal, str)
                or not token
                or not principal
                or len(token.encode()) > 4096
                or len(principal.encode()) > 1024
                or any(character.isspace() or ord(character) < 33 or ord(character) > 126 for character in token)
            ):
                raise ValueError("Invalid bearer credential configuration")
            updated.append((f"Bearer {token}".encode(), principal))
        with self._lock:
            self._tokens = tuple(updated)

    def authenticate(self, authorization: str) -> str | None:
        """Return the configured principal for a matching authorization header.

        Args:
            authorization: Complete HTTP Authorization value.

        Returns:
            Principal for a configured credential, otherwise None.
        """
        if len(authorization) > 4103:
            return None
        supplied = authorization.encode()
        with self._lock:
            snapshot = self._tokens
        matched = None
        for expected, principal in snapshot:
            if hmac.compare_digest(supplied, expected):
                matched = principal
        return matched
