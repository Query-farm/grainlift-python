# Supported Python hosts

Grainlift keeps ADBC sessions, transactions, statements and result cursors in
one process. Reconnect explicitly after process restart; old handles cannot
migrate to another worker or replica. Configure replica affinity externally.
All hosts use the same typed Grainlift service, ownership checks, backend hooks,
error mapping and handle quotas. Neither host changes the protocol or requires
a patched VGI-RPC runtime.

## TCP and mutual TLS

`TcpServer(service, *, host="127.0.0.1", port=0, tls=None,
local_principal=None, limits=None)` owns the supplied `Service`. `start()` binds
synchronously and returns the server; `address` exposes its actual address.
Context entry starts it and context exit calls `close()`. Instances cannot be
restarted; create a new service/server after shutdown.

```python
import signal
import threading

from grainlift import Service, TcpLimits, TcpServer, TLSConfig
from my_worker import MyWorker

if __name__ == "__main__":
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    tls = TLSConfig(
        "server.pem", "server-key.pem", "client-roots.pem",
        {"spiffe://example.org/client": "analytics"},
    )
    with TcpServer(
        Service(MyWorker()), host="0.0.0.0", port=8443, tls=tls,
        limits=TcpLimits(connections=256),
    ):
        stop.wait()
```

The native ADBC client uses `tls+tcp://hostname:8443` and its existing
`grainlift.tls.ca`, `grainlift.tls.cert`, `grainlift.tls.key` and
`grainlift.tls.server_name` options. Certificate/hostname validation stays enabled.

The server verifies each client's certificate chain against only the configured
client roots and requires exactly one URI SAN matching the copied `principals`
mapping. The mapped value becomes the ADBC session principal. Unknown identities,
missing certificates and multiple URI SANs are rejected. No header, subject CN,
SQL option or client-supplied principal can override this mapping. URI identities
may be SPIFFE IDs; this is exact verified URI-SAN authorization, not a SPIFFE
federation or workload-API implementation. TLS 1.2 is the minimum version.

Plain TCP is restricted to a literal loopback address and requires an explicit
`local_principal`. This trusts local processes; it does not authenticate their
individual identities. Non-loopback TCP always requires mTLS. Hostnames are not
accepted as bind addresses, avoiding ambiguous DNS-based exposure.

The TLS context and identity map are snapshots. To rotate trust, keys or identity
authorization, start a new server and drain the old one. Already connected TLS
peers retain their negotiated identity until disconnected. There is no implicit
certificate-revocation lookup or file watcher.

### Resource limits

`TcpLimits` defaults are independent of `Service.limits`:

| Setting | Default | Scope |
| --- | ---: | --- |
| `connections` | 128 | Accepted sockets, including pending TLS handshakes |
| `backlog` | 128 | Kernel listen backlog |
| `handshake_seconds` | 5 | Complete TLS handshake |
| `io_seconds` | 30 | Each complete read/write, including waiting on an idle socket |
| `drain_seconds` | 1 | Grace before forcing accepted sockets to disconnect |
| `join_seconds` | 5 | Joining transport handlers after service cleanup |
| `read_bytes` | 2 MiB | Individual serialized Arrow read |
| `input_bytes` | 256 MiB | Cumulative serialized input per connection |

The listener blocks on socket readiness and an explicit shutdown wakeup; it does
not poll an accept timer. Excess accepted sockets close immediately instead of
forming an unbounded Python work queue. Each admitted socket has one handler.
Reads reject invalid sizes before allocating in the Python reader and count
every consumed byte. Slow trickling cannot reset the deadline for a read;
stalled writes also have a complete-write deadline. Read and input limits are
serialized-byte bounds, not a hard process-memory sandbox for Arrow decoding.
Service limits still validate decoded requests, schemas and batches. Operate
within process/container memory limits and authorize trusted client workloads.

A native ADBC connection normally needs a control socket plus one reusable result
socket. Overlapping readers, parameter uploads and cancellation can need more.
Provision admission headroom accordingly; saturated admission can reject a new
cancellation connection. Idle native result sockets are replaced through a
read-only probe without replaying queries. Control/session recovery after a
disconnect remains explicit. The cumulative input budget can end a long-lived
connection; choose a budget suitable for your parameter-upload workload and
rotate ADBC connections deliberately.

### Shutdown and cancellation

`close()` stops admission, waits up to `drain_seconds` for peers to finish,
interrupts remaining socket I/O, closes the owned service and joins handlers.
The service rejects new operations and requests cancellation of active backend
connections. A timeout reports callbacks that have not returned; calling close
again can complete cleanup later. No listener lock spans a backend callback.
Transport shutdown does not promise transaction commit or query replay.

Logical sessions are shared by control/result sockets. A disconnected socket
does not revoke every handle in its session: result cancellation, explicit ADBC
release and the service idle reaper perform their normal cleanup. ADBC statement
and connection cancellation remain separate operations, using backend support.

In-process callback and cleanup code must be cooperative. Python cannot safely
kill arbitrary callback threads, so service deadlines do not preempt a stuck
in-process cleanup hook. Use `IsolatedWorker` for backend call deadlines and an
external process supervisor for an overall termination deadline. Isolated
children watch their owning process's exit sentinel and terminate if that owner
dies, including while waiting in a backend call. Isolation remains a reliability
boundary for trusted workers, not an untrusted-code sandbox.

`statistics` returns a copied mapping of active, opened, completed and rejected
transport counts. It contains no SQL, credentials or principals. VGI transport
diagnostics are suppressed only in this host's execution context; unrelated
applications retain their logging configuration.

## Granian HTTP

Install `grainlift-python[granian]`, then call the public entry point from a main
guard. The factory must be an importable `module:attribute` returning a Worker.
It runs in the serving child, never in the supervisor. Worker options must be
picklable; do not pass a live Service or database connection across processes.

```python
import os

from grainlift import serve_granian

if __name__ == "__main__":
    serve_granian(
        "my_worker:MyWorker",
        tokens={os.environ["GRAINLIFT_TOKEN"]: "analytics"},
        port=8080,
        threads=8,
        backpressure=64,
        shutdown_seconds=15,
    )
```

This binds authenticated HTTP to `127.0.0.1` only. For remote access, put a TLS
proxy in front with finite body-read and response-write deadlines and process
affinity. The SDK request/body and result limits remain enabled. Granian's header
deadline is ten seconds, header buffer is 1 MiB, and HTTP/1 is selected. `threads`
bounds blocking handlers and `backpressure` bounds concurrent admitted requests.
A slow body or reader still needs the front proxy's per-request deadline.

The supervisor runs exactly one serving process and does not automatically
replace failed workers. SIGTERM/SIGINT initiates draining; a worker that exceeds
`shutdown_seconds` is forcibly terminated. The value must exceed
`Limits.shutdown_seconds`. Normal exit runs Service cleanup; forced termination
can skip in-process backend cleanup hooks. The supervisor deadline is the final
process bound, not a guarantee that an uncooperative callback finishes correctly.

`Service.app()` now sets WSGI headers before returning, prefetching at most one
existing bounded response chunk. Iteration and cleanup retain the original
request context even when Granian changes threads. Early close, first-chunk
failure and normal iteration release the underlying response. This fixes the
eager-header compatibility issue without buffering a whole query result.

The Waitress `serve()` convenience entry point is retained for compatibility;
it does not automatically switch to Granian. Optional hosting dependencies and
deployment choices are explicit. SDK CI exercises Granian on Linux/macOS with
Python 3.13/3.14; consult actual CI results for the revision being deployed.
