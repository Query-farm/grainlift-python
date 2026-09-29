# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Development hosting for a worker: ``run()`` for your own command, or ``python -m grainlift.cli serve``.

The toolkit installs no console script of its own. Give each worker its own
command by calling [`run`][grainlift.cli.run] from a ``[project.scripts]`` entry.
"""

from __future__ import annotations

import argparse
import importlib
import os
import secrets
import signal
import sys
import threading
from collections.abc import Sequence

from .api import Worker
from .hosting import serve_granian
from .server import Service, serve
from .tcp import TcpServer, TLSConfig

TOKEN_VARIABLE = "GRAINLIFT_TOKEN"
ANONYMOUS_PRINCIPAL = "anonymous"


def load_worker(factory: str) -> Worker:
    """Import ``module:factory`` and call it to create a worker.

    Args:
        factory: Importable ``module:attribute``; the attribute is a Worker class or zero-argument factory.

    Returns:
        The created worker.
    """
    module, _, name = factory.partition(":")
    if not module or not name:
        raise ValueError("Worker factory must use module:factory syntax")
    worker = getattr(importlib.import_module(module), name)()
    if not isinstance(worker, Worker):
        raise TypeError("The configured factory must return a Grainlift Worker")
    return worker


def _parser(description: str | None, *, with_factory: bool, auth: str = "token") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    if with_factory:
        parser.add_argument("factory", help="worker to serve, as module:factory")
    parser.add_argument(
        "--host",
        choices=("waitress", "granian", "mtls"),
        default="waitress",
        help="HTTP with Waitress (default), HTTP with Granian, or verified TCP/mTLS",
    )
    parser.add_argument("--port", type=int, default=8080, help="loopback port (default: 8080)")
    parser.add_argument(
        "--auth",
        choices=("token", "anonymous"),
        default=auth,
        help=f"HTTP access: require a bearer token, or also allow clients without one (default: {auth})",
    )
    tls = parser.add_argument_group("mTLS (--host mtls)")
    tls.add_argument("--tls-cert", help="server certificate chain (PEM)")
    tls.add_argument("--tls-key", help="server private key (PEM)")
    tls.add_argument("--client-ca", help="CA that issues client certificates (PEM)")
    tls.add_argument("--client-uri", help="authorized client certificate URI SAN")
    return parser


def _http_access(auth: str) -> tuple[dict[str, str] | None, str | None]:
    token = os.environ.get(TOKEN_VARIABLE)
    if auth == "anonymous":
        print(f"Anonymous access enabled: clients connect without a token as {ANONYMOUS_PRINCIPAL!r}", flush=True)
        return ({token: "developer"} if token else None), ANONYMOUS_PRINCIPAL
    return {_development_token(token): "developer"}, None


def _development_token(token: str | None) -> str:
    if token:
        return token
    token = secrets.token_urlsafe(24)
    print(f"{TOKEN_VARIABLE} is not set; generated a token for this run:", file=sys.stderr)
    print(f"    export {TOKEN_VARIABLE}={token}", file=sys.stderr, flush=True)
    return token


def _serve_mtls(parser: argparse.ArgumentParser, args: argparse.Namespace, factory: str) -> None:
    if not all((args.tls_cert, args.tls_key, args.client_ca, args.client_uri)):
        parser.error("--host mtls requires --tls-cert, --tls-key, --client-ca and --client-uri")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    tls = TLSConfig(args.tls_cert, args.tls_key, args.client_ca, {args.client_uri: "developer"})
    with TcpServer(Service(load_worker(factory)), port=args.port, tls=tls):
        print(f"Grainlift listening on tls+tcp://127.0.0.1:{args.port}", flush=True)
        stop.wait()


def run(
    factory: str, argv: Sequence[str] | None = None, *, description: str | None = None, auth: str = "token"
) -> None:
    """Serve one worker on loopback for development, choosing the host from the command line.

    HTTP hosts authenticate with the bearer token in ``GRAINLIFT_TOKEN``; when it
    is unset, a random token is generated and printed for the client to export.
    With ``--auth anonymous``, clients may also connect without a token; use it
    only for workers that are safe to expose publicly, such as read-only data.
    The mTLS host authorizes one client certificate URI instead. Production
    deployments should configure ``Service``, ``TcpServer`` or ``serve_granian``
    directly with their own credentials and limits.

    Args:
        factory: Importable ``module:factory`` returning a Worker. Granian imports it in its serving process.
        argv: Command-line arguments; defaults to ``sys.argv[1:]``.
        description: Help text shown by ``--help``.
        auth: Default for ``--auth``: ``token`` or ``anonymous``.
    """
    if auth not in ("token", "anonymous"):
        raise ValueError("auth must be 'token' or 'anonymous'")
    parser = _parser(description, with_factory=False, auth=auth)
    _dispatch(parser, parser.parse_args(argv), factory)


def _dispatch(parser: argparse.ArgumentParser, args: argparse.Namespace, factory: str) -> None:
    if args.host == "mtls":
        _serve_mtls(parser, args, factory)
    elif args.host == "granian":
        tokens, anonymous = _http_access(args.auth)
        print(f"Grainlift listening on http://127.0.0.1:{args.port}", flush=True)
        serve_granian(factory, tokens=tokens, anonymous_principal=anonymous, port=args.port)
    else:
        tokens, anonymous = _http_access(args.auth)
        token = next(iter(tokens)) if tokens else None
        serve(load_worker(factory), token=token, anonymous_principal=anonymous, port=args.port)


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for ``python -m grainlift.cli serve module:factory``.

    Args:
        argv: Command-line arguments; defaults to ``sys.argv[1:]``.
    """
    parser = argparse.ArgumentParser(prog="python -m grainlift.cli", description="Grainlift worker tools")
    commands = parser.add_subparsers(dest="command", required=True)
    serve_parser = commands.add_parser(
        "serve",
        parents=[_parser(None, with_factory=True)],
        add_help=False,
        help="serve a worker on loopback for development",
    )
    args = parser.parse_args(argv)
    _dispatch(serve_parser, args, args.factory)


if __name__ == "__main__":
    main()
