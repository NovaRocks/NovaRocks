<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# NovaRocks

NovaRocks is a Rust-native analytical query engine. Its production runtime is
the native NovaRocks FE/BE role model; one binary starts a `fe`, `be`, or
`all-in-one` role through the `standalone` command.

- `fe` owns the MySQL SQL entrypoint, planning, and distributed coordination.
- `be` owns local fragment execution and the native gRPC boundary.
- `all-in-one` is a test and local-development convenience. It keeps the FE/BE
  application boundary rather than adding a direct-call shortcut.

StarRocks is supported only as a read-only external Connector. RPC reads can
serve every StarRocks topology; direct reads permanently require shared-data.
NovaRocks is not a StarRocks BE-compatible server and does not own a native
internal StarRocks table type.

NovaRocks is still experimental and is not production-ready. It is useful for
iterating on distributed execution, connector, and Iceberg semantics and for
running local SQL experiments on macOS/Linux.

## Current Scope

Implemented or actively exercised areas include:

- Native distributed runtime entrypoints:
  - native FE SQL/coordinator services
  - native BE gRPC fragment and exchange services
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
  - Iceberg-backed materialized-view lifecycle work
  - StarRocks external Connector with RPC remote reads for all topologies and
    shared-data-only direct reads

Known limits:

- This repository is still research/experimental code.
- Most code has been AI-assisted and has not gone through production-grade
  validation.
- Share-nothing mode is not supported; share-data style storage is the main
  target.
- Some Iceberg features are phase-based and may have narrow
  contract support rather than full StarRocks parity.

## Architecture

### Native FE/BE Roles

```text
SQL client / mysql CLI
  `- MySQL-compatible protocol
       v
  NovaRocks role=fe
       v
  frontend coordinator + SQL compiler
       v
  native gRPC fragment submission
       v
  NovaRocks role=be
       v
  Arrow execution pipeline + external connectors
```

## Design Principles

- **Native role boundaries are explicit.** FE owns SQL admission and global
  coordination; BE owns local execution. `all-in-one` preserves those owners.
- **Arrow-first execution.** NovaRocks uses Arrow `RecordBatch` wrapped as
  `Chunk` as the in-memory batch format.
- **Protocol and execution stay separated.** Native gRPC is the FE/BE process
  boundary; execution semantics remain in Rust application owners.
- **Fail fast on unsupported semantics.** Ambiguous or unsupported plan/SQL
  behavior should return explicit errors instead of silently falling back.
- **Connector-backed storage semantics.** Native DDL/DML routes through
  external catalog/provider and Iceberg write contracts instead of hard-coding storage
  behavior into the SQL server.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- A platform C toolchain required by Rust dependencies
- `rustc` / `cargo` 1.92.0 or newer

## Build

```bash
# debug mode (default)
cargo build -p novarocks-server

# release mode
cargo build --release -p novarocks-server
```

Build artifacts:

- debug: `./target/debug/novarocks`
- release: `./target/release/novarocks`


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

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true
```

`[metadata].path` stores native control metadata in SQLite. Persistent user
tables belong to explicitly created external Iceberg catalogs;
`[connector.object_store]` supplies process-local object-store credentials for
connector execution and does not create a native internal table store.

## Run

### Native Roles

The only server command is `standalone`:

```bash
novarocks standalone --role fe --config ./novarocks.toml
novarocks standalone --role be --config ./be.toml
novarocks standalone --role all-in-one --config ./novarocks.toml
```

`--help` and `-h` show the command contract. Historical daemon commands
(`run`, `start`, `stop`, and `restart`) are not supported.

Native role examples:

```bash
# One process that composes a real FE and BE through the gRPC boundary.
cargo run -p novarocks-server -- standalone --role all-in-one --config ./novarocks.toml

# Split FE/BE deployment.
cargo run -p novarocks-server -- standalone --role be --config ./be.toml
cargo run -p novarocks-server -- standalone --role fe --config ./fe.toml
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

Start standalone with the generated object-store config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"
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
the NovaRocks standalone port is allocated per worktree.

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

### Native development

The root workspace builds the native FE/BE runtime and external Connector
crates without a StarRocks server toolchain.

```bash
cargo fmt --all
cargo clippy --all-targets
cargo build
cargo test
```

Use targeted package tests while iterating on a native crate:

```bash
cargo test -p novarocks
cargo test -p novarocks-backend
cargo test -p novarocks-server
```

For focused native validation:

```bash
cargo test --test standalone_cli
cargo test --test standalone_mysql_server
```

## License

Apache License 2.0. See `LICENSE.txt`.
