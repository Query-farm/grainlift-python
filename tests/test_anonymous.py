# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Opt-in anonymous HTTP access for services that are safe to expose without credentials."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from test_service import TestWorker
from vgi_rpc import RpcError
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient

from grainlift import Limits, Service, TokenStore, serve
from grainlift.hosting import _load, serve_granian
from grainlift.protocol import Grainlift, OpenConnectionRequest
from grainlift.telemetry import PrivateApplication


@contextmanager
def connect(app: PrivateApplication, token: str | None = None) -> Iterator[Any]:
    """Connect a VGI client to the app, sending a bearer token only when given one."""
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    client = _SyncTestClient(app, default_headers=headers)  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
    with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
        yield rpc


def query(rpc: Any) -> tuple[str, list[list[int]]]:
    """Open a session, run the test query, and read every batch through continuation tokens."""
    sid = rpc.open_connection(
        request=OpenConnectionRequest(target="default", database_options=[], connection_options=[])
    ).session_id
    stmt = rpc.new_statement(session_id=sid).statement_id
    rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="query")
    rid = rpc.execute(session_id=sid, statement_id=stmt).result_id
    with rpc.read_result(session_id=sid, result_id=rid, sequence=0) as stream:
        return sid, [item.batch.column(0).to_pylist() for item in stream]


def test_anonymous_only_service_needs_no_credentials() -> None:
    """Without any tokens configured, a client without credentials queries through continuations."""
    with Service(TestWorker()) as service:
        with connect(service.app(anonymous_principal="public")) as rpc:
            sid, batches = query(rpc)
            assert batches == [[1, 2], [3]]
            assert service._sessions[sid].principal == "public"
            rpc.close_connection(session_id=sid)
        assert not service._sessions


def test_token_only_service_still_rejects_missing_credentials() -> None:
    """Anonymous access is opt-in; the default remains token-required."""
    with Service(TestWorker()) as service:
        with connect(service.app(tokens={"token": "alice"})) as rpc, pytest.raises(RpcError):
            query(rpc)
        assert not service._sessions


def test_wrong_token_is_rejected_not_downgraded_to_anonymous() -> None:
    """Presenting an invalid credential fails even when anonymous access is enabled."""
    with Service(TestWorker()) as service:
        app = service.app(tokens={"token": "alice"}, anonymous_principal="public")
        for bad in ("wrong", ""):
            with connect(app, bad) as rpc, pytest.raises(RpcError):
                query(rpc)
        assert not service._sessions


def test_anonymous_and_token_principals_are_isolated() -> None:
    """Anonymous clients cannot use a token principal's session, and vice versa."""
    with Service(TestWorker()) as service:
        app = service.app(tokens={"token": "alice"}, anonymous_principal="public")
        with connect(app, "token") as alice, connect(app) as anonymous:
            alice_sid, alice_batches = query(alice)
            anonymous_sid, anonymous_batches = query(anonymous)
            assert alice_batches == anonymous_batches == [[1, 2], [3]]
            assert service._sessions[alice_sid].principal == "alice"
            assert service._sessions[anonymous_sid].principal == "public"
            with pytest.raises(RpcError, match="not_found"):
                anonymous.new_statement(session_id=alice_sid)
            with pytest.raises(RpcError, match="not_found"):
                alice.new_statement(session_id=anonymous_sid)


def test_anonymous_continuation_cannot_be_resumed_with_a_token() -> None:
    """Continuation tokens minted for anonymous clients stay bound to anonymous requests."""
    with Service(TestWorker()) as service:
        client = _SyncTestClient(
            service.app(tokens={"token": "alice"}, anonymous_principal="public"),  # type: ignore[arg-type]  # VGI test helper accepts WSGI wrappers at runtime.
            default_headers={},
        )
        with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]  # VGI reflects the protocol class.
            sid = rpc.open_connection(
                request=OpenConnectionRequest(target="default", database_options=[], connection_options=[])
            ).session_id
            stmt = rpc.new_statement(session_id=sid).statement_id
            rpc.set_sql_query(session_id=sid, statement_id=stmt, sql="query")
            rid = rpc.execute(session_id=sid, statement_id=stmt).result_id
            batches = iter(rpc.read_result(session_id=sid, result_id=rid, sequence=0))
            assert next(batches).batch.num_rows == 2
            client._default_headers["Authorization"] = "Bearer token"
            with pytest.raises(RpcError):
                next(batches)


@pytest.mark.parametrize(
    ("tokens", "anonymous", "message"),
    [
        (None, None, "Configure bearer tokens, anonymous access, or both"),
        (None, "", "Invalid anonymous principal"),
        ({"token": "public"}, "public", "must differ"),
    ],
)
def test_access_configuration_is_validated(tokens: dict[str, str] | None, anonymous: str | None, message: str) -> None:
    """Reject configurations that would admit nobody or merge anonymous and token principals."""
    with Service(TestWorker()) as service, pytest.raises(ValueError, match=message):
        service.app(tokens=tokens, anonymous_principal=anonymous)


def test_rotation_cannot_grant_a_token_the_anonymous_principal() -> None:
    """A rotated credential mapped to the anonymous principal name is refused at authentication."""
    store = TokenStore({"token": "alice"})
    with Service(TestWorker()) as service:
        app = service.app(tokens=store, anonymous_principal="public")
        store.replace({"token": "public"})
        with connect(app, "token") as rpc, pytest.raises(RpcError):
            query(rpc)
        assert not service._sessions


def test_serve_allows_anonymous_without_a_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Waitress development host serves anonymous clients when no token is configured."""
    hosted: dict[str, Any] = {}
    monkeypatch.setattr("waitress.serve", lambda app, **options: hosted.update(app=app, **options))
    serve(TestWorker(), anonymous_principal="public", port=9002)
    assert hosted["port"] == 9002
    # serve() closed its service on return, so the app now refuses new work.
    with connect(hosted["app"]) as rpc, pytest.raises(RpcError, match="Service is closed"):
        query(rpc)
    with pytest.raises(ValueError, match="Configure bearer tokens"):
        serve(TestWorker(), port=9002)


def test_granian_loader_serves_anonymous_clients() -> None:
    """The Granian child builds an anonymous app, and the parent validates access first."""
    app = _load("test_service:TestWorker", {}, None, Limits(), "public")
    with connect(app) as rpc:
        assert query(rpc)[1] == [[1, 2], [3]]
    with pytest.raises(ValueError, match="Configure bearer tokens"):
        serve_granian("test_service:TestWorker")
