# NovaRocks

NovaRocks is a Rust-native analytical query engine that started as a
StarRocks BE-compatible runtime and has evolved into a system that can also run
independently without StarRocks FE.

The project currently has two first-class execution modes:

1. **StarRocks-compatible backend mode**
   - StarRocks FE keeps producing plans and talking through FE-compatible
     heartbeat, backend thrift, and brpc/internal-service protocols.
   - A C++ shim handles brpc compatibility; Rust owns plan lowering, execution,
     exchange, connectors, and result handling.

2. **Standalone SQL engine mode**
   - NovaRocks can parse and execute SQL without StarRocks FE.
   - `standalone-server` exposes a MySQL-compatible endpoint for SQL clients and
     SQL regression tests.
   - The standalone engine has its own in-process catalog, Iceberg catalog
     registry, managed-lake metadata store, and connector-backed DDL/DML flows.

NovaRocks is still experimental and is not production-ready. It is useful for
learning StarRocks-style execution internals, iterating on connector and
Iceberg semantics, testing managed-lake behavior, and running local SQL
experiments on macOS/Linux without maintaining a full StarRocks FE/BE cluster.

## Current Scope

Implemented or actively exercised areas include:

- FE-compatible BE runtime entrypoints:
  - heartbeat thrift service
  - backend thrift service
  - brpc/internal-service gateway through the C++ shim
  - gRPC exchange service
- Rust execution stack:
  - thrift-plan lowering
  - Arrow `RecordBatch` / `Chunk` processing
  - pipeline drivers, dependencies, and scheduling
  - exchange, result buffering, runtime filters, spill, and cache plumbing
- Standalone SQL stack:
  - StarRocks-oriented SQL parsing and analysis
  - SQL planner/codegen into NovaRocks execution plans
  - MySQL-compatible standalone server
  - SQL test runner integration
- Catalog and connector work:
  - Iceberg catalogs: memory, Hadoop/filesystem, and REST
  - Iceberg SELECT, INSERT, DELETE, UPDATE/MERGE-related mutation flows, schema
    changes, refs, and compaction experiments
  - managed-lake DDL/DML, SQLite metadata, object-store-backed storage, and
    materialized-view lifecycle work

Known limits:

- This repository is still research/experimental code.
- Most code has been AI-assisted and has not gone through production-grade
  validation.
- Share-nothing mode is not supported; share-data style storage is the main
  target.
- Some Iceberg/managed-lake features are phase-based and may have narrow
  contract support rather than full StarRocks parity.

## Architecture

### StarRocks-Compatible Mode

```text
StarRocks FE
  |- HeartbeatService (Thrift) -------> Rust service/heartbeat_service
  |- BackendService (Thrift) ---------> Rust service/backend_service
  `- PInternalService (brpc) ---------> C++ shim
                                          |
                                          `- FFI
                                               v
                                      Rust internal_service
                                               v
                                          lower/**
                                               v
                                      exec/pipeline/**
                                               v
                         result_buffer / exchange / connectors
```

### Standalone Mode

```text
SQL client / mysql CLI / SQL test runner
  `- MySQL-compatible protocol
       v
  src/server/mod.rs
       v
  src/engine/**
       v
  src/sql/parser + analyzer + optimizer + codegen
       v
  exec/pipeline + runtime
       v
  connector backends
     |- local catalog / Parquet
     |- Iceberg catalog registry
     `- managed-lake metadata + object store
```

## Design Principles

- **Two mode boundaries are explicit.** FE-compatible mode follows
  FE-provided thrift metadata and protocol contracts. Standalone mode owns SQL
  parsing, catalog resolution, planning, and session context.
- **Arrow-first execution.** NovaRocks uses Arrow `RecordBatch` wrapped as
  `Chunk` as the in-memory batch format.
- **Protocol and execution stay separated.** The C++ shim is only the brpc
  compatibility gateway; execution semantics belong to Rust.
- **Fail fast on unsupported semantics.** Ambiguous or unsupported plan/SQL
  behavior should return explicit errors instead of silently falling back.
- **Connector-backed storage semantics.** Standalone DDL/DML routes through
  catalog/table-source/table-sink/MV backends instead of hard-coding storage
  behavior into the SQL server.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- C/C++ build toolchain
- `cmake` 3.20+; 3.27+ recommended

Minimum toolchain versions:

- `rustc` / `cargo`: 1.92.0
- Linux `gcc` / `g++`: 12+

### Linux

Environment variables:

- `STARROCKS_GCC_HOME`: GCC toolchain root containing `bin/gcc` and `bin/g++`
- `STARROCKS_THIRDPARTY`: thirdparty root

Recommended environment:

- StarRocks official Docker image, where both variables are preconfigured.
- For non-official Docker or bare metal, configure both variables manually and
  build NovaRocks thirdparty with `./thirdparty/build-thirdparty.sh`.

### macOS

Environment variables:

- `STARROCKS_THIRDPARTY`: thirdparty root

Prepare thirdparty by following the StarRocks macOS guide:

- <https://github.com/StarRocks/starrocks/blob/main/docs/en/developers/mac-compile-run-test.md>

## Build

```bash
# debug mode (default)
./build.sh
./build.sh --debug

# release mode
./build.sh --release
```

Build artifacts:

- debug: `./target/debug/novarocks`
- release: `./target/release/novarocks`

Packaging is disabled by default. Use `--package` when a StarRocks-style
runtime output is needed:

```bash
./build.sh --release --package
```

Default package output:

```text
./output/novarocks
```

Release mode uses `RUSTFLAGS="-C target-cpu=native"` by default when
`RUSTFLAGS` is not already set. Override it with:

```bash
NOVAROCKS_RELEASE_RUSTFLAGS="-C target-cpu=native -C debuginfo=1" ./build.sh --release
```

## Configuration

NovaRocks loads config in this order:

1. `--config <path>`
2. `NOVAROCKS_CONFIG=<path>`
3. `./novarocks.toml`

Useful files:

- `novarocks.toml`: local runtime config
- `novarocks.toml.example`: extended documented template

Standalone mode is configured through `[standalone_server]`:

```toml
[metadata]
provider = "sqlite"
path = "meta/standalone.sqlite"

[standalone_server]
mysql_port = 9030
user = "root"
warehouse_uri = "s3://novarocks/standalone"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true
```

`[metadata].path` stores standalone catalog/managed-lake metadata in SQLite.
`warehouse_uri` plus `[standalone_server.object_store]` enables managed-lake
storage.

## Run

### StarRocks-Compatible Backend Mode

CLI usage:

```bash
novarocks [run|start|stop|restart] [--config <path>]
```

Control script:

```bash
# foreground
./bin/novarocksctl start

# daemon mode
./bin/novarocksctl start --daemon

# stop daemon
./bin/novarocksctl stop

# restart daemon
./bin/novarocksctl restart
```

Built binary:

```bash
./target/debug/novarocks run --config ./novarocks.toml
./target/release/novarocks run --config ./novarocks.toml
```

### Standalone MySQL-Compatible Server

Run a local standalone SQL server without StarRocks FE:

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --port 9030
```

Or use a config file:

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config ./novarocks.toml
```

Connect with a MySQL client:

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

The standalone server supports session context such as `USE <db>`,
`SET catalog = <catalog>`, `SET query_timeout = N`, and
`SET group_concat_max_len = N`.

## Local Iceberg REST + MinIO + Spark Environment

The `docker/iceberg-rest/` directory contains the shared local Iceberg REST
Catalog, MinIO, and Spark environment used by both local development and CI.
Bring it up, then discover the active worktree ports and generated configs
from the fixed entry:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

Codex environment setup uses `docker/iceberg-rest/up.sh --prepare-only`
instead. That only writes `runtime/current/env.sh` and related config files;
it does not start Docker.

Useful generated values:

- `NOVA_ENV_COMPOSE_PROJECT`
- `NOVA_ENV_MYSQL_PORT`
- `NOVAROCKS_ICEBERG_REST_URI`
- `NOVAROCKS_ICEBERG_REST_WAREHOUSE`
- `AWS_S3_ENDPOINT`
- `NOVAROCKS_STANDALONE_CONFIG`
- `NOVAROCKS_SQL_TEST_CONFIG`
- `NOVAROCKS_ICE_REST_CATALOG_SQL`
- `NOVAROCKS_SPARK_DEFAULTS`
- `NOVAROCKS_SPARK_V3_SMOKE_SQL`
- `NOVAROCKS_SPARK_SQL`

Start standalone-server with the generated object-store config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Generate an Iceberg format-v3 table through Spark using the same REST Catalog
and MinIO object store:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

Spark uses the Docker-network endpoints `http://rest:8181` and
`http://minio:9000`; NovaRocks uses the host endpoints exported in `env.sh`.
The Docker services are shared across worktrees by default and use the
service-default host ports configured in `docker/iceberg-rest/shared.env`;
the NovaRocks standalone-server port is allocated per worktree.

Run the cross-engine Iceberg compatibility SQL suite:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

See [`docker/iceberg-rest/README.md`](docker/iceberg-rest/README.md) for the
full guide, including required Docker images and a CI integration example.

## SQL Regression Tests

The SQL test runner expects a MySQL-compatible NovaRocks standalone server.

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --mode <verify|record|diff> \
  --query-timeout 60 \
  -j 4
```

When using the generated local environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --mode verify
```

Common suites include `ssb`, `tpc-h`, `tpc-ds`, `cte`, `join`, `filter`,
`sort`, `iceberg`, and `iceberg-rest`.

## Development Workflow

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
./build.sh
cargo test
```

For focused standalone validation:

```bash
cargo test --test standalone_cli
cargo test --test standalone_mysql_server
```

## License

Apache License 2.0. See `LICENSE.txt`.
