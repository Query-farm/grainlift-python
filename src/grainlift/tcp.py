# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Bounded TCP/mTLS hosting with explicit admission, draining and shutdown."""

from __future__ import annotations

import ipaddress
import math
import selectors
import socket
import ssl
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from io import IOBase, RawIOBase
from pathlib import Path
from typing import Any, Self, cast

from vgi_rpc import AuthContext, RpcServer
from vgi_rpc.rpc import TcpTransport

from .api import AdbcError
from .protocol import Grainlift
from .server import Service
from .telemetry import private_transport


@dataclass(frozen=True)
class TcpLimits:
    """Bound listener resources independently of ADBC handle quotas.

    Attributes:
        connections: Maximum admitted sockets, including pending TLS handshakes.
        backlog: Kernel listen backlog; excess admitted sockets are rejected.
        handshake_seconds: Absolute TLS handshake deadline.
        io_seconds: Absolute deadline for each transport read or write, including idle waits.
        drain_seconds: Time for existing connections to finish before forced disconnect.
        join_seconds: Time to wait for transport threads after disconnect and service cleanup.
        read_bytes: Maximum individual Arrow read, checked before allocation.
        input_bytes: Cumulative serialized input budget per connection.
    """

    connections: int = 128
    backlog: int = 128
    handshake_seconds: float = 5
    io_seconds: float = 30
    drain_seconds: float = 1
    join_seconds: float = 5
    read_bytes: int = 2 * 1024 * 1024
    input_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        """Reject invalid limits before allocating listener resources."""
        for name in ("connections", "backlog", "read_bytes", "input_bytes"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError("TCP resource limits must be positive integers")
        for name in ("handshake_seconds", "io_seconds", "drain_seconds", "join_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError("TCP deadlines must be finite and positive")


@dataclass(frozen=True)
class TLSConfig:
    """Require verified client certificates and exact URI-SAN authorization.

    Attributes:
        certificate: Server certificate chain in PEM format.
        private_key: Server private key in PEM format.
        client_ca: Trust roots used to verify client certificate chains.
        principals: Exact certificate URI SANs mapped to application principals.
            A peer must have exactly one URI SAN. No subject/CN fallback is used.
    """

    certificate: str | Path
    private_key: str | Path
    client_ca: str | Path
    principals: Mapping[str, str]


class _Reader(RawIOBase):
    def __init__(self, sock: socket.socket, limits: TcpLimits) -> None:
        self.sock = sock
        self.limits = limits
        self.remaining = limits.input_bytes

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size < 0 or size > self.limits.read_bytes or size > self.remaining:
            raise ValueError("TCP input budget exceeded")
        deadline = time.monotonic() + self.limits.io_seconds
        data = bytearray(size)
        count = 0
        while count < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("TCP read deadline exceeded")
            self.sock.settimeout(remaining)
            received = self.sock.recv_into(memoryview(data)[count:])
            if received == 0:
                break
            count += received
            self.remaining -= received
        return bytes(memoryview(data)[:count])


class _Writer(RawIOBase):
    def __init__(self, sock: socket.socket, timeout: float) -> None:
        self.sock = sock
        self.timeout = timeout

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        # sendall uses one absolute timeout for the complete write.
        self.sock.settimeout(self.timeout)
        self.sock.sendall(data)
        return len(data)


class _Transport(TcpTransport):
    def __init__(self, sock: socket.socket, limits: TcpLimits) -> None:
        self.socket = sock
        self.input = _Reader(sock, limits)
        self.output = _Writer(sock, limits.io_seconds)

    @property
    def reader(self) -> IOBase:
        return self.input

    @property
    def writer(self) -> IOBase:
        return self.output

    def close(self) -> None:
        self.input.close()
        self.output.close()
        self.socket.close()


class TcpServer:
    """Own a service and bounded listener; use as a context manager or call close().

    Closing stops admission, drains briefly, interrupts socket I/O, closes the
    service and joins handlers. In-process backend callbacks must be cooperative;
    use IsolatedWorker for enforced backend deadlines. This object cannot restart.
    """

    def __init__(
        self,
        service: Service,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        tls: TLSConfig | None = None,
        local_principal: str | None = None,
        limits: TcpLimits | None = None,
    ) -> None:
        """Validate configuration without starting threads or opening sockets.

        Args:
            service: Owned service; close() also closes its sessions and workers.
            host: Literal IPv4 or IPv6 bind address; plaintext requires loopback.
            port: TCP port, or zero for an ephemeral port reported by address.
            tls: Verified mTLS configuration for network access.
            local_principal: Explicit trusted local identity for plaintext loopback only.
            limits: Listener bounds, distinct from the service's ADBC quotas.
        """
        address = ipaddress.ip_address(host)
        if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
            raise ValueError("Invalid TCP port")
        if tls is None and (
            not address.is_loopback
            or not isinstance(local_principal, str)
            or not local_principal
            or len(local_principal) > 1024
        ):
            raise ValueError("Plain TCP requires loopback and an explicit local principal")
        if tls is not None and local_principal is not None:
            raise ValueError("TLS identity cannot be replaced by a local principal")
        self.service = service
        self.limits = limits or TcpLimits()
        self._principals = dict(tls.principals) if tls else {}
        if tls and (
            not self._principals
            or len(self._principals) > 4096
            or any(
                not isinstance(uri, str)
                or not uri
                or len(uri) > 2048
                or not isinstance(principal, str)
                or not principal
                or len(principal) > 1024
                for uri, principal in self._principals.items()
            )
        ):
            raise ValueError("Invalid certificate principal mapping")
        self._context = None
        if tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_cert_chain(tls.certificate, tls.private_key)
            context.load_verify_locations(tls.client_ca)
            self._context = context
        self._local_principal = local_principal
        self._bind = (str(address), port)
        self._family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
        self._condition = threading.Condition()
        self._close_lock = threading.Lock()
        self._workers: dict[threading.Thread, socket.socket] = {}
        self._listener: socket.socket | None = None
        self._wake: tuple[socket.socket, socket.socket] | None = None
        self._thread: threading.Thread | None = None
        self._closing = False
        self._opened = 0
        self._completed = 0
        self._rejected = 0
        with private_transport():
            self._rpc = RpcServer(Grainlift, service)

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound address after start()."""
        with self._condition:
            if self._listener is None:
                raise RuntimeError("TCP listener is not running")
            bound = self._listener.getsockname()
            return str(bound[0]), int(bound[1])

    @property
    def statistics(self) -> dict[str, int]:
        """Return anonymous transport counters, including handshakes in active."""
        with self._condition:
            return {
                "active": len(self._workers),
                "opened": self._opened,
                "completed": self._completed,
                "rejected": self._rejected,
            }

    def start(self) -> Self:
        """Bind synchronously, then start an event-driven accept thread.

        Returns:
            This running server.
        """
        with self._condition:
            if self._thread is not None or self._closing:
                raise RuntimeError("TCP server cannot be started twice")
            listener = socket.socket(self._family, socket.SOCK_STREAM)
            try:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(self._bind)
                listener.listen(self.limits.backlog)
                listener.setblocking(False)
                self._wake = socket.socketpair()
                self._listener = listener
                self._thread = threading.Thread(target=self._accept, name="grainlift-tcp-listener", daemon=True)
                self._thread.start()
            except BaseException:
                listener.close()
                self._listener = None
                if self._wake:
                    for sock in self._wake:
                        sock.close()
                    self._wake = None
                raise
        return self

    def __enter__(self) -> Self:
        """Start the owned listener.

        Returns:
            This running server.
        """
        return self.start()

    def __exit__(self, *_: object) -> None:
        """Drain and close the listener and owned service."""
        self.close()

    def _accept(self) -> None:
        assert self._listener is not None and self._wake is not None
        with selectors.DefaultSelector() as selector:
            selector.register(self._listener, selectors.EVENT_READ)
            selector.register(self._wake[0], selectors.EVENT_READ)
            while True:
                for key, _ in selector.select():
                    if key.fileobj is self._wake[0]:
                        return
                    with self._condition:
                        if self._closing:
                            return
                        try:
                            sock, _ = self._listener.accept()
                        except BlockingIOError:
                            continue
                        if len(self._workers) >= self.limits.connections:
                            self._rejected += 1
                            sock.close()
                            continue
                        worker = threading.Thread(
                            target=self._serve, args=(sock,), daemon=True, name="grainlift-tcp-connection"
                        )
                        self._workers[worker] = sock
                        self._opened += 1
                        try:
                            worker.start()
                        except RuntimeError:
                            self._workers.pop(worker)
                            self._completed += 1
                            sock.close()

    def _serve(self, sock: socket.socket) -> None:
        transport = None
        try:
            with private_transport():
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                principal = self._local_principal
                if self._context:
                    with self._condition:
                        if self._closing:
                            return
                        sock = self._context.wrap_socket(sock, server_side=True, do_handshake_on_connect=False)
                        self._workers[threading.current_thread()] = sock
                    sock.settimeout(self.limits.handshake_seconds)
                    sock.do_handshake()
                    cert = cast("dict[str, Any]", sock.getpeercert())
                    uris = [value for kind, value in (cert or {}).get("subjectAltName", ()) if kind == "URI"]
                    principal = self._principals.get(uris[0]) if len(uris) == 1 else None
                    if principal is None:
                        with self._condition:
                            self._rejected += 1
                        return
                assert principal is not None
                transport = _Transport(sock, self.limits)
                self._rpc.serve(
                    transport, auth=AuthContext(domain="grainlift", authenticated=True, principal=principal)
                )
        except Exception:
            # No protocol arguments, certificates or backend exceptions in logs.
            pass
        finally:
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            if transport:
                with suppress(OSError):
                    transport.close()
            sock.close()
            with self._condition:
                self._workers.pop(threading.current_thread(), None)
                self._completed += 1
                self._condition.notify_all()

    def close(self) -> None:
        """Stop admission, drain, disconnect peers, close the service and join handlers.

        A timeout is reported if backend callbacks remain; another close() can
        finish cleanup after they return. No user callback is run under the
        listener's state lock.
        """
        with self._close_lock:
            self._close()

    def _close(self) -> None:
        with self._condition:
            self._closing = True
            if self._wake:
                with suppress(OSError):
                    self._wake[1].send(b"x")
        if self._thread:
            self._thread.join(self.limits.join_seconds)
            if self._thread.is_alive():
                raise AdbcError("TCP listener did not stop", "timeout")
        with self._condition:
            if self._listener:
                self._listener.close()
                self._listener = None
            if self._wake:
                for sock in self._wake:
                    sock.close()
                self._wake = None
            workers = list(self._workers)
            self._condition.wait_for(lambda: not self._workers, timeout=self.limits.drain_seconds)
            for sock in self._workers.values():
                with suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
        try:
            self.service.close()
        finally:
            deadline = time.monotonic() + self.limits.join_seconds
            for worker in workers:
                worker.join(timeout=max(0, deadline - time.monotonic()))
            finished = not any(worker.is_alive() for worker in workers)
        if not finished:
            raise AdbcError("TCP shutdown is waiting for worker callbacks", "timeout")
