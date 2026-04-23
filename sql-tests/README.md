# SQL Tests

`sql-tests/` is the standalone SQL regression suite for NovaRocks.

## Hard Prerequisite

Managed-lake SQL tests require a reachable MinIO-compatible object store at `http://127.0.0.1:9000`.

Default credentials (matching the standalone-server defaults):

- access key: `admin`
- secret key: `admin123`
- bucket: `novarocks`

If MinIO is not running, the default standalone sql-tests flow fails fast before any suite starts:

```
MinIO at http://127.0.0.1:9000 is unreachable.
hint: start it with:
  mkdir -p ~/minio-data && minio server ~/minio-data --console-address :9001 &
```

Example local startup:

```bash
mkdir -p ~/minio-data
minio server ~/minio-data --console-address :9001 &
```

## Default Standalone Flow

Start the standalone server on `9030`:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

Then run a suite:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite filter --mode verify
```

The runner now prefers `tests/sql-test-runner/conf/standalone_managed_lake.conf`
by default (managed-lake). If you need the legacy StarRocks-style connection
config, pass `--config tests/sql-test-runner/conf/sr.conf`.

## Explicit Managed-Lake Config

If you want a persistent standalone metadata DB / warehouse root, start the
server with:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config \
  tests/sql-test-runner/conf/standalone_managed_lake.toml
```
