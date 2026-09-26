# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Credential rotation preserves principal identity and in-flight continuations."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from grainlift.credentials import TokenStore


def test_rotation_overlap_and_revocation() -> None:
    """Accept the overlap set and immediately reject removed credentials."""
    store = TokenStore({"old": "alice"})
    assert store.authenticate("Bearer old") == "alice"
    store.replace({"old": "alice", "new": "alice"})
    assert store.authenticate("Bearer old") == store.authenticate("Bearer new") == "alice"
    store.replace({"new": "alice"})
    assert store.authenticate("Bearer old") is None
    assert store.authenticate("Bearer new") == "alice"
    assert store.authenticate("Bearer unknown") is None


@pytest.mark.parametrize("size", [4095, 4096, 4097])
def test_token_length_boundary(size: int) -> None:
    """Accept secrets through the exact documented byte limit."""
    if size > 4096:
        with pytest.raises(ValueError):
            TokenStore({"x" * size: "alice"})
    else:
        store = TokenStore({"x" * size: "alice"})
        assert store.authenticate("Bearer " + "x" * size) == "alice"


@pytest.mark.parametrize("count", [4095, 4096, 4097])
def test_credential_count_boundary(count: int) -> None:
    """Reject credential sets above the bounded authentication work limit."""
    tokens = {f"token-{index}": "alice" for index in range(count)}
    if count > 4096:
        with pytest.raises(ValueError):
            TokenStore(tokens)
    else:
        assert TokenStore(tokens).authenticate(f"Bearer token-{count - 1}") == "alice"


@pytest.mark.parametrize("bad", [{}, {"": "alice"}, {"secret": ""}, {"secret\n": "alice"}, {"☃": "alice"}])
def test_invalid_replacement_preserves_previous_credentials(bad: dict[str, str]) -> None:
    """Validate the entire new configuration before replacing a live mapping."""
    store = TokenStore({"old": "alice"})
    with pytest.raises(ValueError):
        store.replace(bad)
    assert store.authenticate("Bearer old") == "alice"


def test_concurrent_replacement_has_no_partial_snapshot() -> None:
    """Readers always see one complete configured identity during rotations."""
    store = TokenStore({"stable": "alice", "old": "alice"})

    def read() -> None:
        for _ in range(1000):
            assert store.authenticate("Bearer stable") == "alice"

    with ThreadPoolExecutor(4) as pool:
        readers = [pool.submit(read) for _ in range(4)]
        for index in range(1000):
            store.replace({"stable": "alice", f"rotating-{index}": "alice"})
        for reader in readers:
            reader.result(timeout=5)
