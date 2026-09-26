# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Scope sensitive transport diagnostics to the Grainlift request context."""

import logging
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from contextvars import ContextVar
from typing import Any
from wsgiref.types import StartResponse, WSGIApplication

_private_request = ContextVar("grainlift_private_request", default=False)
_install_lock = threading.Lock()


class _TransportFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (_private_request.get() and (record.name == "vgi_rpc" or record.name.startswith("vgi_rpc.")))


_transport_filter = _TransportFilter()


def _install_filters() -> None:
    # HTTP modules are loaded by make_wsgi_app before this executes. Recheck at
    # each request for modules/loggers installed between construction and serving.
    with _install_lock:
        # Root-handler filters also cover transport loggers first created during
        # a request. Ancestor logger filters do not run during propagation.
        for handler in logging.getLogger().handlers:
            if _transport_filter not in handler.filters:
                handler.addFilter(_transport_filter)
        for name, logger in list(logging.Logger.manager.loggerDict.items()):
            if (name == "vgi_rpc" or name.startswith("vgi_rpc.")) and isinstance(logger, logging.Logger):
                if _transport_filter not in logger.filters:
                    logger.addFilter(_transport_filter)
                for handler in logger.handlers:
                    if _transport_filter not in handler.filters:
                        handler.addFilter(_transport_filter)


class PrivateApplication:
    """Keep VGI diagnostics private during this application's WSGI requests.

    Does not replace handlers or change levels, propagation or disabled flags. Other
    applications retain their normal logging. Grainlift emits only an HTTP
    status and duration, without paths, principals, arguments or exceptions.
    """

    def __init__(self, app: WSGIApplication) -> None:
        """Wrap a WSGI application and install request-scoped transport filters."""
        self.app = app
        _install_filters()

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterator[bytes]:
        """Serve a request within a private transport logging context."""
        _install_filters()
        token = _private_request.set(True)
        started = time.monotonic()
        status_code = 500
        response: Iterable[bytes] | None = None

        def start(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Callable[[bytes], object]:
            nonlocal status_code
            status_code = int(status.split(" ", 1)[0])
            return start_response(status, headers, exc_info)

        try:
            response = self.app(environ, start)
            yield from response
        finally:
            try:
                if response is not None:
                    close = getattr(response, "close", None)
                    if close is not None:
                        close()
            finally:
                _private_request.reset(token)
                logging.getLogger("grainlift.access").info(
                    "Grainlift request completed",
                    extra={
                        "http_status": status_code,
                        "duration_ms": (time.monotonic() - started) * 1000,
                    },
                )
