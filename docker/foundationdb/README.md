# FoundationDB 7.3.69 experimental fixture

This fixture provides a worktree-scoped FoundationDB client and single-server
cluster for NovaRocks experimental state-store leaf-provider tests. The server image and official native
client are pinned to FoundationDB 7.3.69. The Rust binding selects API version
730 separately at compile time. The dedicated live-fixture check runs only on
Linux x86_64. macOS results are developer auxiliary evidence and do not replace
that check. Starting the server requires Docker 29 or newer with
Docker Compose.

Supported client platforms:

- macOS arm64 developer machines:
  `FoundationDB-7.3.69_arm64.pkg`, SHA-256
  `6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e`.
- Linux x86_64 experimental CI:
  `foundationdb-clients_7.3.69-1_amd64.deb`, SHA-256
  `ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf`.

The scripts reject other native-client platforms and reject assets whose hash
does not match the pinned value. Runtime files and downloaded binaries stay
under the ignored `runtime/` directory.

The release matrix is intentionally duplicated here for operator visibility:
server and client 7.3.69, Rust API 730, macOS arm64 asset SHA-256
`6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e`,
and Linux x86_64 asset SHA-256
`ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf`.
The executable fixture scripts remain the source of truth used by CI. The
workflow invokes `docker/foundationdb/up.sh`, whose exact self-check verifies
the pinned version, API, and platform-specific asset SHA. The experimental
check then consumes the generated environment and validates its existence, Linux
x86_64 platform, and required client artifacts; it does not duplicate those
pinned constants.

Prepare the client and generated environment without starting Docker:

```bash
docker/foundationdb/up.sh --prepare-only
source docker/foundationdb/runtime/current/env.sh
```

The generated environment exports the cluster file, keyspace UUID, client
library, `fdbcli`, runtime library path, Docker Compose project, and runtime
paths. `FDB_CLIENT_LIB_PATH` and `NOVA_FDB_CLIENT_LIBRARY_DIR` both name the
client library directory required by `foundationdb-sys`; the concrete library
file is exported separately as `NOVA_FDB_CLIENT_LIBRARY_FILE`. The environment
and keyspace UUID are derived from the canonical worktree path, so separate
worktrees do not share a Docker project or logical keyspace.

Start the pinned Linux amd64 server and wait for `fdbcli status` readiness:

```bash
docker/foundationdb/up.sh
docker/foundationdb/status.sh
```

On Linux x86_64, run the optional experimental check after the fixture is ready:

```bash
tools/ci/foundationdb-provider.sh
```

The check verifies the SPI boundary, the live single-runtime conformance, and
two independent helper processes. The helpers are separate FDB clients, not
NovaRocks FEs, and provide experimental cross-process provider evidence only.
Remote provider format design, multi-FE protocol, and failover behavior remain
future work.

`status.sh` prints the cluster-file path, but never prints its contents. The
host cluster file points at the worktree-specific published port. Compose uses
that absolute worktree runtime path as a read-only bind mount at
`/var/fdb/fdb.cluster`; both the container's `FDB_CLUSTER_FILE` and explicit
`fdbserver --cluster-file` argument select the mounted file. This keeps the
server and host `fdbcli` on the same connection string even though the fixture
overrides the stock image entrypoint to publish a host-reachable address.

Treat the cluster file, TLS private key, TLS password, and deployment
credentials as secrets. Do not print their contents or copy them into test
output. NovaRocks structured logs expose lifecycle state (including retryable
`shutdown_deferred`), the maximum and selected API versions, client readiness,
and a one-way keyspace hash. Commit-state native error warnings additionally
expose `transaction_id` as a canonical UUID, `phase`, `native_error_code`, and
`category`. They do not log cluster-file contents, credential values,
certificate/private-key contents, logical keys or values, secrets, or the raw
keyspace UUID.

Remove only this worktree's generated runtime while leaving Docker untouched:

```bash
docker/foundationdb/down.sh
```

Explicitly stop only this worktree's Compose project before removing its
runtime:

```bash
docker/foundationdb/down.sh --docker
```

Shutdown order is strict: stop accepting new state-store work, finish or cancel
callers, drop all read/write transactions and store handles, call
`StateStoreRuntime::shutdown()` until it succeeds, and only then stop the
fixture or FDB client environment. A shutdown deadline leaves the
runtime owner intact so the caller can release remaining handles and retry; it
is not permission to tear down the native client underneath live handles.

This fixture is not a supported NovaRocks deployment path or runtime downloader.
Any future remote-provider deployment design must separately define client
installation, network/host/TLS access control, and operational ownership.
