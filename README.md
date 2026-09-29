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

The binary's sealed active Connector providers are Iceberg and Paimon. Iceberg
supports the existing read/write surface; Paimon currently supports bounded
snapshot reads for append-only and `deduplicate` primary-key tables. The
StarRocks connector source is retained only as retired reference code: it has no
active read capability, and Server rejects legacy `[connector.starrocks]`
configuration.

NovaRocks is still experimental and is not production-ready. It is useful for
iterating on distributed execution, connector, Iceberg, and Paimon semantics and for
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
  - exchange, result buffering, runtime filters, and cache plumbing
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
  - Paimon Filesystem Catalog reads for Parquet append-only and `deduplicate`
    primary-key tables over shared S3-compatible storage

Known limits:

- This repository is still research/experimental code.
- Most code has been AI-assisted and has not gone through production-grade
  validation.
- Share-nothing mode is not supported; share-data style storage is the main
  target.
- Some Iceberg features are phase-based and may have narrow contract support
  rather than full upstream feature parity.

## Architecture

Application ownership, dependency direction, logical-execution lifecycle and
the separation between responsibility, output, and resource release are
described in the [application domains guide](docs/guides/architecture/application-domains.md).

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

Deployable role configuration is explicit. Use the two templates as a pair:

- `novarocks-fe.toml.example`: FE MySQL, coordinator-report gRPC, management
  HTTP, an explicit catalog source, and optional local Accelerator carrier.
- `novarocks-catalogs.toml.example`: the immutable, explicit empty catalog
  snapshot paired with the static-file FE template.
- `novarocks-be.toml.example`: BE Native gRPC, management HTTP, and local
  connector execution binding.

Both role files must contain `[cluster].role`; the command line asserts that role
and never changes it. A frontend selects exactly one `[catalog_source]` authority.
This outline uses `dynamic-state-store` because it permits SQL catalog mutation;
the checked-in FE template instead uses its paired static snapshot. `[state_store]`
never selects the source mode implicitly and never owns backend membership.

```toml
[state_store]
provider = "sqlite"
path = "meta/frontend-state.sqlite"
cluster_id = "local-cluster"

[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[server]
grpc_port = 9080
http_port = 8040

[cluster]
role = "fe"

[catalog_source]
mode = "dynamic-state-store"

[standalone_server]
mysql_port = 9030
user = "root"

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
enable_path_style_access = true
```

`[state_store]` is the frontend-owned durable control-plane store, not a backend
membership registry. Every BE self-registers to the FE native endpoint and is
eligible only after its authenticated announce and FE-pull heartbeat agree on its
process identity. Persistent user tables belong to explicitly created external
Iceberg or Paimon catalogs; only Iceberg currently supplies native write
capabilities.
`[connector.object_store]` supplies process-local object-store credentials for
connector execution and does not create a native internal table store.

SQLite is the only production StateStore provider. Its file uses schema v3; an
older file is rejected rather than migrated or reset, and its bytes are left
untouched. MySQL and FoundationDB implementations remain experimental leaf
crates and are not Server configuration options. A configured SQLite failure
blocks FE startup.

`[state_store.limits]` bounds what the provider enforces, including
`transaction_timeout_ms` for one physical transaction.
`[application.state_store_policy]` is separate and holds what the application
decides: `max_attempts` and `operation_timeout_ms` for one logical operation.
Both groups may tighten the built-in defaults and never relax them, and neither
timeout constrains the other, because they bound different things. Unknown keys
fail startup, so a configuration carrying the retired `history_retention`,
`runner_max_attempts` or `transaction_deadline_ms` names is rejected rather than
silently ignored.
Secret-bearing scalars may be literal for local development or an exact
`${ENV:VAR}` reference. References are resolved once by Server startup; missing,
empty, malformed, and non-UTF-8 values fail startup without exposing the value.

`[native_trust]` is mandatory in every deployable FE/BE configuration. Its
shared secret authenticates every Native RPC in all three directions (FE→BE,
BE→BE, BE→FE); it is not SQL authorization, backend membership, or a message
MAC. Omitting `[native_trust.transport]` deliberately selects authenticated
plaintext h2c for a trusted network. Select `automatic` or `pem` to add the
fixed TLS 1.3/h2 layer; JWT authentication remains mandatory in either TLS
mode. See the [Native trust deployment guide](docs/guides/deployment/native-trust.md)
for exact TLS configuration, rotation, and threat boundaries. MySQL and
management HTTP are outside this NWT-3 configuration surface.

## Run

### Native Roles

The only server command is `standalone`:

```bash
novarocks standalone --role fe --config ./novarocks-fe.toml
novarocks standalone --role be --config ./novarocks-be.toml
novarocks standalone --role all-in-one --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml
```

`--help` and `-h` show the command contract. Historical daemon commands
(`run`, `start`, `stop`, and `restart`) are not supported.

Native role examples:

```bash
# One process that supervises the normal FE and BE role runners.
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml

# Split FE/BE deployment.
cargo run -p novarocks-server -- standalone --role be --config ./novarocks-be.toml
cargo run -p novarocks-server -- standalone --role fe --config ./novarocks-fe.toml
```

Connect with a MySQL client:

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

The standalone server supports session context such as `USE <db>`,
`SET catalog = <catalog>`, `SET query_timeout = N`, and
`SET group_concat_max_len = N`.

## Local Iceberg REST + MinIO + Spark Environment

`docker/iceberg-rest/` 为开发与 CI 提供版本化 fixture。相同输入复用一个对象存储/catalog 实例，输入变化允许新旧版本并存；对象存储可供多个 catalog 使用。owner 预留实际服务端口，不使用固定 host 端口或固定共享 Compose 项目。

先启动/附着，再一次性解析 publication 并读取生成配置：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

`runtime/current` 只是定位器，入口中的 `published` 是绑定与全部配置的唯一原子提交点。`NOVA_ENV_REST_ENV_FILE`、FE/BE 配置、runner 配置及 Spark defaults 都指向这次 publication；`NOVA_ENV_RUNTIME_DIR` 是 SQLite 等稳定运行数据目录。消费者不能据此拼接 `env.sh`，也不能混用两次 publication 的变量。

Codex setup 的 `up.sh --prepare-only` 不调用 Docker；有保存的 ready 记录时恢复该入口，否则发布没有端点/镜像的 unbound。该 ready 状态不是健康证明，运行前仍需正常 up。输入须由 `docker/fixture-inputs/provision.sh` 显式供给；verify/测试不下载、pull 或构建。BOM 缺失/无效返回 75，CI 报 BLOCKED；owner 端口与身份失败报 VERIFY FAILED。

常用输出是 `NOVA_ENV_OBJECT_STORE_RUNTIME`、`NOVA_ENV_CATALOG_RUNTIME`、`NOVA_ENV_COMPOSE_PROJECT`、`AWS_S3_ENDPOINT`、`NOVAROCKS_ICEBERG_REST_URI`、`NOVAROCKS_FE_CONFIG`、`NOVAROCKS_BE_CONFIG`、`NOVAROCKS_SQL_TEST_CONFIG` 和 `NOVAROCKS_SPARK_DEFAULTS`。Spark 容器使用生成的网络端点，NovaRocks 使用生成的 host 端点。

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"

NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_REST_ENV_FILE" \
  docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"

cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify --cluster-mode cross-process --cluster-size 3
```

显式 worktree 解绑用 `down.sh --runtime-only`；加 `--purge` 才清理全部历史数据位置，全部成功后才释放引用。共享服务 stop/delete 按精确 runtime ID 管理；force catalog 删除仅条件解绑目标，保留数据引用。对象存储任何删除都受引用保护，HMS 等外部连接须先退出。首次新对象存储会重建 benchmark READY 数据。

旧 `nr-iceberg-rest` / `nr-iceberg-hive` 及其卷保留，不自动迁移或删除；用户确认旧消费者退出和数据保存后另行安排退役。详见 [fixture 操作说明](docker/iceberg-rest/README.md) 与 [ADR-0165](docs/adr/ADR-0165-versioned-fixture-runtime-ownership.md)。

## SQL Regression Tests

The SQL test runner expects a MySQL-compatible NovaRocks standalone server.

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --suite <suite> \
  --mode <verify|record|diff> \
  --query-timeout 60 \
  -j 4
```

When using the generated local environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --mode verify
```

Common suites include `ssb`, `tpc-h`, `tpc-ds`, `cte`, `join`, `filter`,
`sort`, `iceberg`, and `iceberg-rest`.

## Development Workflow

### Native development

The root workspace builds the native FE/BE runtime and the active Iceberg and
Paimon Connector crates without a StarRocks server toolchain. The retired
StarRocks connector crate is not part of the Server provider manifest.

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo build --workspace
cargo test --workspace
```

`--workspace` matters: the root manifest sets `default-members =
["novarocks-server"]`, so without it these commands cover only the server
package and every other member's tests are skipped.

Use targeted package tests while iterating on a native crate:

```bash
cargo test -p novarocks
cargo test -p novarocks-native-adapter
cargo test -p novarocks-server
```

Tests are layered by owner. Pick the layer that owns what you changed:

| Layer | Entry point |
| --- | --- |
| Owner-local component tests | `cargo test -p <crate>` |
| SQL / result / plan-shape contracts | `novarocks-sql-test` runner, see `tests/sql/README.md` |
| Real 1FE+3BE lifecycle, faults, recovery | `cargo run -p novarocks-system-test-runner -- --list` |
| Server binary composition, readiness, signal | `cargo test -p novarocks-server --test server_binary_smoke` |
| Process, log and TCP-port mechanics | `cargo test -p novarocks-test-support` |

## License

Apache License 2.0. See `LICENSE.txt`.
