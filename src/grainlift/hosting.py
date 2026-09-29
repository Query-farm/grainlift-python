# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Optional Granian hosting with one process owning all ADBC sessions."""

from __future__ import annotations

import importlib
import logging
from functools import partial
from multiprocessing.util import Finalize
from typing import Any

from .api import Limits, Worker
from .server import Service, access_credentials
from .telemetry import PrivateApplication


def _load(
    factory: str,
    options: dict[str, Any],
    tokens: dict[str, str] | None,
    limits: Limits,
    anonymous_principal: str | None = None,
) -> PrivateApplication:
    module, _, name = factory.partition(":")
    worker = getattr(importlib.import_module(module), name)(**options)
    if not isinstance(worker, Worker):
        raise TypeError("The configured factory must return a Grainlift Worker")
    service = Service(worker, limits=limits)

    def finish() -> None:
        try:
            service.close()
        except Exception:
            # Cleanup hooks can fail or overrun; expose no backend exception text.
            logging.getLogger("grainlift.host").error("Grainlift worker cleanup did not complete")

    try:
        application = service.app(tokens=tokens, anonymous_principal=anonymous_principal)
        # Spawned workers bypass atexit; multiprocessing finalizers still run on
        # orderly exit. The supervisor forcibly terminates a worker past its deadline.
        Finalize(None, finish, exitpriority=10)
        return application
    except BaseException:
        finish()
        raise


def serve_granian(
    factory: str,
    *,
    tokens: dict[str, str] | None = None,
    worker_options: dict[str, Any] | None = None,
    port: int = 8080,
    threads: int = 8,
    backpressure: int = 64,
    shutdown_seconds: int = 15,
    limits: Limits | None = None,
    anonymous_principal: str | None = None,
) -> None:
    """Serve authenticated loopback HTTP with Granian's process supervisor.

    Call from the main thread under an ``if __name__ == '__main__'`` guard.
    SIGTERM/SIGINT stop admission and drain the serving process; the supervisor
    terminates it after shutdown_seconds. Put a TLS proxy with finite body and
    slow-client deadlines in front of this loopback listener for remote access.
    One process owns sessions; process restart invalidates existing handles.

    Args:
        factory: Importable ``module:factory`` returning a Worker inside the child.
        tokens: Bearer credentials mapped to configured application principals.
        worker_options: Picklable keyword arguments for the worker factory.
        port: Explicit loopback port, from one through 65535.
        threads: Maximum blocking request threads in the serving process.
        backpressure: Maximum concurrently admitted requests; overload waits upstream.
        shutdown_seconds: Supervisor grace period before forced process termination.
        limits: ADBC quotas and backend cleanup deadlines.
        anonymous_principal: Principal for requests without credentials; None requires a token.
    """
    if not factory.partition(":")[0] or not factory.partition(":")[2]:
        raise ValueError("Worker factory must use module:factory syntax")
    for value in (port, threads, backpressure, shutdown_seconds):
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError("Granian host limits must be positive integers")
    if port > 65535:
        raise ValueError("Invalid HTTP port")
    access_credentials(tokens, anonymous_principal)
    service_limits = limits or Limits()
    if shutdown_seconds <= service_limits.shutdown_seconds:
        raise ValueError("Supervisor deadline must exceed service shutdown deadline")
    try:
        from granian import Granian
        from granian.constants import HTTPModes, Interfaces
        from granian.http import HTTP1Settings
    except ImportError:
        raise ImportError("Install grainlift[granian] to use serve_granian") from None

    server = Granian(
        "grainlift.hosting",
        address="127.0.0.1",
        port=port,
        interface=Interfaces.WSGI,
        workers=1,
        blocking_threads=threads,
        runtime_threads=1,
        backpressure=backpressure,
        http=HTTPModes.http1,
        http1_settings=HTTP1Settings(header_read_timeout=10000, max_buffer_size=1024 * 1024),
        log_enabled=False,
        log_access=False,
        workers_kill_timeout=shutdown_seconds,
        respawn_failed_workers=False,
    )
    server.serve(
        target_loader=partial(
            _load,
            factory,
            dict(worker_options or {}),
            dict(tokens) if tokens is not None else None,
            service_limits,
            anonymous_principal,
        ),
        wrap_loader=False,
    )
