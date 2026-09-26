# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Real sockets, verified certificates, ownership, bounds and host shutdown."""

from __future__ import annotations

import datetime
import logging
import socket
import ssl
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from test_service import TestConnection, TestWorker
from vgi_rpc import RpcError
from vgi_rpc.rpc import tcp_connect

from grainlift import AdbcError, Limits, QueryResult, Service, TcpLimits, TcpServer, TLSConfig
from grainlift.protocol import Grainlift, OpenConnectionRequest
from grainlift.tcp import _Reader, _Writer


@pytest.fixture(scope="module")
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create disposable certificate chains without external commands."""
    directory = tmp_path_factory.mktemp("tls")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test CA")])
    now = datetime.datetime.now(datetime.UTC)

    def builder(subject: x509.Name, public: rsa.RSAPublicKey) -> x509.CertificateBuilder:
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(name)
            .public_key(public)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(public), False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), False)
        )

    ca = (
        builder(name, key.public_key())
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
        .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), True)
    )
    (directory / "ca.pem").write_bytes(ca.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
    for identity in ("server", "client", "other"):
        leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, identity)])
        san: x509.GeneralName = (
            x509.DNSName("localhost")
            if identity == "server"
            else x509.UniformResourceIdentifier(f"spiffe://test/{identity}")
        )
        cert = (
            builder(subject, leaf_key.public_key())
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.KeyUsage(True, False, True, False, False, False, False, False, False), True)
            .add_extension(x509.SubjectAlternativeName([san]), False)
            .add_extension(
                x509.ExtendedKeyUsage(
                    [ExtendedKeyUsageOID.SERVER_AUTH if identity == "server" else ExtendedKeyUsageOID.CLIENT_AUTH]
                ),
                False,
            )
        )
        (directory / f"{identity}.pem").write_bytes(
            cert.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)
        )
        private = directory / f"{identity}-key.pem"
        private.write_bytes(
            leaf_key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        private.chmod(0o600)
    return directory


def tls_config(directory: Path, *, other: bool = False) -> TLSConfig:
    """Authorize one or two certificate URI identities."""
    principals = {"spiffe://test/client": "alice"}
    if other:
        principals["spiffe://test/other"] = "bob"
    return TLSConfig(directory / "server.pem", directory / "server-key.pem", directory / "ca.pem", principals)


def client_context(directory: Path, identity: str | None = "client") -> ssl.SSLContext:
    """Require the test server chain and optionally present a client certificate."""
    context = ssl.create_default_context(cafile=directory / "ca.pem")
    if identity:
        context.load_cert_chain(directory / f"{identity}.pem", directory / f"{identity}-key.pem")
    return context


def eventually(predicate: Callable[[], bool]) -> None:
    """Wait briefly for asynchronous socket cleanup without unbounded hangs."""
    deadline = time.monotonic() + 3
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def open_connection(rpc: Grainlift) -> str:
    """Open one ordinary typed Grainlift session."""
    return rpc.open_connection(
        request=OpenConnectionRequest(target="default", database_options=[], connection_options=[])
    ).session_id


@pytest.mark.parametrize("secure", [False, True])
def test_queries_errors_reuse_and_private_logs(
    certificates: Path, secure: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Keep repeated streams on one socket and suppress only this service's VGI logs."""
    caplog.set_level(logging.DEBUG, logger="vgi_rpc")
    worker = TestWorker()
    with TcpServer(
        Service(worker), tls=tls_config(certificates) if secure else None, local_principal=None if secure else "alice"
    ) as server:
        with tcp_connect(
            Grainlift,  # type: ignore[type-abstract]  # VGI protocol reflection.
            *server.address,
            tls_context=client_context(certificates) if secure else None,
            server_hostname="localhost",
        ) as rpc:
            sid = open_connection(rpc)
            statement = rpc.new_statement(session_id=sid).statement_id
            for _ in range(4):
                rpc.set_sql_query(session_id=sid, statement_id=statement, sql="SECRET query")
                result = rpc.execute(session_id=sid, statement_id=statement).result_id
                with rpc.read_result(session_id=sid, result_id=result, sequence=0) as stream:
                    assert sum(item.batch.num_rows for item in stream) == 3
            rpc.set_sql_query(session_id=sid, statement_id=statement, sql="crash")
            with pytest.raises(RpcError, match="internal"):
                rpc.execute(session_id=sid, statement_id=statement)
            rpc.close_connection(session_id=sid)
        eventually(lambda: server.statistics["active"] == 0)
        assert server.statistics["opened"] == 1
    # Client-side logs are outside the host's private context; inspect host threads.
    assert not any(
        "SECRET" in record.getMessage() for record in caplog.records if record.threadName == "grainlift-tcp-connection"
    )
    assert all(connection.closed for connection in worker.connections)
    logger = logging.getLogger("vgi_rpc")
    logger.error("unrelated logging retained")
    assert "unrelated logging retained" in caplog.text


def test_verified_identity_cannot_access_another_session(certificates: Path) -> None:
    """Bind session ownership to the authorized certificate identity."""
    with (
        TcpServer(Service(TestWorker()), tls=tls_config(certificates, other=True)) as server,
        tcp_connect(
            Grainlift,  # type: ignore[type-abstract]  # VGI protocol reflection.
            *server.address,
            tls_context=client_context(certificates),
            server_hostname="localhost",
        ) as alice,
        tcp_connect(
            Grainlift,  # type: ignore[type-abstract]  # VGI protocol reflection.
            *server.address,
            tls_context=client_context(certificates, "other"),
            server_hostname="localhost",
        ) as bob,
    ):
        sid = open_connection(alice)
        with pytest.raises(RpcError, match="not_found"):
            bob.new_statement(session_id=sid)
        alice.close_connection(session_id=sid)


@pytest.mark.parametrize(
    "identity,hostname", [(None, "localhost"), ("other", "localhost"), ("client", "wrong.invalid")]
)
def test_certificate_rejection(certificates: Path, identity: str | None, hostname: str) -> None:
    """Reject absent certificates, unauthorized identities and wrong server names."""
    with TcpServer(Service(TestWorker()), tls=tls_config(certificates)) as server:
        with (
            pytest.raises((OSError, RpcError, ValueError)),
            tcp_connect(
                Grainlift,  # type: ignore[type-abstract]  # VGI protocol reflection.
                *server.address,
                tls_context=client_context(certificates, identity),
                server_hostname=hostname,
            ) as rpc,
        ):
            open_connection(rpc)
        eventually(lambda: server.statistics["active"] == 0)
        assert not server.service._sessions


@pytest.mark.parametrize("size", [15, 16, 17])
def test_reader_bounds_before_allocation(size: int) -> None:
    """Accept the exact read and cumulative boundary and reject before consuming input."""
    left, right = socket.socketpair()
    try:
        right.sendall(b"x" * 17)
        reader = _Reader(left, TcpLimits(read_bytes=16, input_bytes=16))
        if size > 16:
            with pytest.raises(ValueError, match="budget"):
                reader.read(size)
            assert reader.remaining == 16
        else:
            assert reader.read(size) == b"x" * size
            assert reader.read(16 - size) == b"x" * (16 - size)
            with pytest.raises(ValueError, match="budget"):
                reader.read(1)
        with pytest.raises(ValueError, match="budget"):
            reader.read()
    finally:
        left.close()
        right.close()


def test_read_deadline_is_absolute() -> None:
    """Trickling bytes cannot reset the deadline for one Arrow read."""
    left, right = socket.socketpair()
    stop = threading.Event()

    def trickle() -> None:
        while not stop.wait(0.02):
            try:
                right.sendall(b"x")
            except OSError:
                return

    thread = threading.Thread(target=trickle)
    thread.start()
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            _Reader(left, TcpLimits(io_seconds=0.1)).read(100)
        assert time.monotonic() - started < 1
    finally:
        stop.set()
        left.close()
        right.close()
        thread.join(1)


def test_admission_and_shutdown_interrupt_pending_handshakes(certificates: Path) -> None:
    """Count pending TLS peers against admission and close them without polling."""
    service = Service(TestWorker())
    server = TcpServer(
        service, tls=tls_config(certificates), limits=TcpLimits(connections=2, drain_seconds=0.01, handshake_seconds=30)
    )
    server.start()
    peers = [socket.create_connection(server.address, timeout=1) for _ in range(2)]
    try:
        eventually(lambda: server.statistics["active"] == 2)
        with socket.create_connection(server.address, timeout=1) as excess:
            assert excess.recv(1) == b""
        assert server.statistics["rejected"] == 1
        started = time.monotonic()
        server.close()
        assert time.monotonic() - started < 2
        assert server.statistics["active"] == 0
        assert server.statistics["opened"] == server.statistics["completed"] == 2
        assert service._closed and not service._reaper.is_alive()
        server.close()
    finally:
        for peer in peers:
            peer.close()
        server.close()


def test_idle_expiry_disconnect_and_listener_recovery() -> None:
    """Expire an idle peer and its abandoned session while keeping the listener usable."""
    service = Service(TestWorker(), limits=Limits(idle_seconds=0.1))
    with TcpServer(service, local_principal="local", limits=TcpLimits(io_seconds=0.15)) as server:
        with tcp_connect(Grainlift, *server.address) as rpc:  # type: ignore[type-abstract]
            open_connection(rpc)
            eventually(lambda: server.statistics["active"] == 0)
            eventually(lambda: not service._sessions)
        with tcp_connect(Grainlift, *server.address) as rpc:  # type: ignore[type-abstract]
            rpc.close_connection(session_id=open_connection(rpc))


def test_plaintext_requires_explicit_trusted_loopback_identity() -> None:
    """Reject plaintext exposure and accidental anonymous local access."""
    with Service(TestWorker()) as service:
        with pytest.raises(ValueError):
            TcpServer(service)
        with pytest.raises(ValueError):
            TcpServer(service, host="0.0.0.0", local_principal="local")


def test_slow_consumer_hits_write_deadline() -> None:
    """A peer that never reads cannot keep the transport writer blocked indefinitely."""
    left, right = socket.socketpair()
    try:
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        with pytest.raises(TimeoutError):
            _Writer(left, 0.1).write(b"x" * 1024 * 1024)
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("shutdown", [False, True])
def test_active_cancellation_and_shutdown(shutdown: bool) -> None:
    """Keep cancellation independent of the busy transport and clean up active work."""
    entered, cancelled = threading.Event(), threading.Event()

    class BlockingConnection(TestConnection):
        def execute(self, sql: str) -> QueryResult:
            entered.set()
            assert cancelled.wait(3)
            raise AdbcError("Cancelled", "cancelled")

        def cancel(self) -> None:
            cancelled.set()

    class BlockingWorker(TestWorker):
        def connect(self, principal: str) -> TestConnection:
            connection = BlockingConnection(self.batches)
            self.connections.append(connection)
            return connection

    worker = BlockingWorker()
    with (
        TcpServer(Service(worker), local_principal="local", limits=TcpLimits(drain_seconds=0.01)) as server,
        tcp_connect(Grainlift, *server.address) as rpc,  # type: ignore[type-abstract]
        ThreadPoolExecutor(1) as pool,
    ):
        sid = open_connection(rpc)
        statement = rpc.new_statement(session_id=sid).statement_id
        rpc.set_sql_query(session_id=sid, statement_id=statement, sql="wait")
        future = pool.submit(rpc.execute, session_id=sid, statement_id=statement)
        assert entered.wait(2)
        if shutdown:
            server.close()
        else:
            with tcp_connect(Grainlift, *server.address) as cancellation:  # type: ignore[type-abstract]
                cancellation.cancel_connection(session_id=sid)
        with pytest.raises((RpcError, OSError, ValueError)):
            future.result(timeout=2)
    assert cancelled.is_set()
    assert all(connection.closed for connection in worker.connections)
    assert not server.service._sessions


@pytest.mark.parametrize(
    "field",
    [
        "connections",
        "backlog",
        "read_bytes",
        "input_bytes",
        "handshake_seconds",
        "io_seconds",
        "drain_seconds",
        "join_seconds",
    ],
)
@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf")])
def test_invalid_transport_limits(field: str, value: float) -> None:
    """Reject nonsensical or unbounded listener configurations."""
    with pytest.raises(ValueError):
        TcpLimits(**{field: value})  # type: ignore[arg-type]  # Deliberately invalid configuration.
