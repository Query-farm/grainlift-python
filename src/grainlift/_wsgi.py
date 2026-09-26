# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Expose lazy WSGI headers while retaining one bounded chunk and its context."""

from collections.abc import Iterable, Iterator
from contextvars import copy_context
from threading import RLock
from typing import Any, Self
from wsgiref.types import StartResponse, WSGIApplication


class PrimedResponse:
    """Keep one prefetched chunk and its request context across host threads."""

    def __init__(self, app: WSGIApplication, environ: dict[str, Any], start_response: StartResponse) -> None:
        """Start a response before returning it to the host.

        Args:
            app: Bounded WSGI application.
            environ: Request environment.
            start_response: Host status and headers callback.
        """
        self._lock = RLock()
        self._context = copy_context()
        self._response: Iterable[bytes] = self._context.run(app, environ, start_response)
        self._closed = False
        self._first: bytes | None = None
        try:
            self._iterator: Iterator[bytes] = self._context.run(iter, self._response)
            self._first = self._context.run(next, self._iterator, None)
        except BaseException:
            self.close()
            raise

    def __iter__(self) -> Self:
        """Return this iterator.

        Returns:
            This response iterator.
        """
        return self

    def __next__(self) -> bytes:
        """Return the saved chunk or advance once in the original context.

        Returns:
            One unchanged response chunk.
        """
        with self._lock:
            return self._next()

    def _next(self) -> bytes:
        if self._closed:
            raise StopIteration
        if self._first is not None:
            chunk, self._first = self._first, None
            return chunk
        try:
            return self._context.run(next, self._iterator)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release the underlying response once, including an unconsumed chunk."""
        with self._lock:
            self._close()

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            self._first = None
            if close := getattr(self._response, "close", None):
                self._context.run(close)


def prime(app: WSGIApplication, environ: dict[str, Any], start_response: StartResponse) -> PrimedResponse:
    """Expose response headers before Granian captures them.

    Args:
        app: Bounded WSGI application.
        environ: Request environment.
        start_response: Host status and headers callback.

    Returns:
        A bounded, context-preserving response iterator.
    """
    return PrimedResponse(app, environ, start_response)
