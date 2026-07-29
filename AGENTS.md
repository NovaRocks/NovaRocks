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

# NovaRocks - AI Agents Guide

This document is a quick operational index for agents working on NovaRocks.
It is designed to help you quickly:
- locate the right code paths
- understand the current execution architecture
- implement changes without semantic drift

This guide focuses on high-frequency implementation details and modification entry points.

---

## 1. Project Overview

NovaRocks is a **Rust-based, cloud-native, compute-storage decoupling friendly**
analytical query engine.

It now has two first-class modes:

- **StarRocks-compatible backend mode**
  - FE-compatible protocol behavior with zero FE awareness changes.
  - C++ Shim handles brpc access and protocol bridging.
  - Rust handles thrift plan lowering, pipeline execution, exchange, and connectors.

- **Standalone SQL engine mode**
  - Runs without StarRocks FE.
  - Provides a MySQL-compatible local server through `standalone`.
  - Owns SQL parsing, analysis, planning, codegen, catalog state, Iceberg catalog
    dispatch, managed-lake metadata, and SQL test execution.

Columnar processing is centered on Arrow `RecordBatch` / `Chunk` in both modes.

---

## 2. Architecture Overview (Current Code)

### 2.1 StarRocks-Compatible Backend Mode

```text
StarRocks FE
  |- HeartbeatService (Thrift) -------> Rust: src/service/heartbeat_service.rs
  |- BackendService (Thrift, be_port) -> Rust: src/service/backend_service.rs
  `- PInternalService (brpc) ---------> C++ Shim: src/shim/brpc_server.cpp
                                          |
                                          `- FFI (C ABI, compat.h)
                                               v
                                          Rust FFI: src/service/engine_ffi.rs
                                               v
                                          Query Execute: src/service/internal_service.rs
                                               v
                                          Lowering: src/lower/**
                                               v
                                          Pipeline: src/exec/pipeline/**
                                               v
                      +---------- ResultBuffer: src/runtime/result_buffer.rs
                      `---------- Exchange (gRPC): src/runtime/exchange.rs + src/service/grpc_*.rs
```

### 2.2 Standalone SQL Engine Mode

```text
SQL client / SQL test runner
  `- MySQL protocol -----------> Rust: src/server/mod.rs
                                      |
                                      v
                            Standalone Engine: src/engine/**
                                      |
                                      v
                     SQL Parser / Analyzer / Optimizer / Codegen:
                         src/sql/parser/**
                         src/sql/analyzer/**
                         src/sql/optimizer/**
                         src/sql/codegen/**
                                      |
                                      v
                            Pipeline / Runtime / Connectors
                                      |
                 +--------------------+--------------------+
                 |                    |                    |
          Local Parquet        Iceberg Catalogs        Managed Lake
        in-memory catalog    memory/hadoop/rest/hive   SQLite + object store
```

---

## 3. Non-Negotiable Rules (High Priority)

1. **Strictly follow FE-provided plan and type metadata**
   No fallback behavior, no guessed defaults, no implicit type downgrade.

2. **Fail fast on unsupported or ambiguous semantics**
   Return explicit errors in parsing/lowering stages instead of "best effort" execution.

3. **Keep protocol and execution responsibilities separated**
   C++ Shim is the protocol gateway; execution semantics belong to Rust.

4. **Keep FE-compatible and standalone responsibilities explicit**
   FE-compatible paths follow FE-provided thrift metadata. Standalone paths own
   SQL parsing, catalog resolution, and session context. Do not mix assumptions
   between the two modes without checking the active entrypoint.

5. **Distributed deployment is the source of truth; standalone is test-only**
   The real, user-facing deployment is NovaRocks distributed (1 NovaRocks FE +
   N BE; CI baseline 1FE+3BE). The single-process all-in-one form ("standalone")
   is only a testing convenience. Do NOT model for, or add special-case branches
   for, the single-process form. Cluster-topology quantities (broadcast fanout,
   backend count, per-node budgets) must be read dynamically from the live BE
   registry, never hardcoded or defaulted to "single-process = 1 node". Tests
   must never pass only in standalone while failing under 1FE+3BE.

6. **Frontend owns durable backend membership**
   In `role=fe`, StateStore is required and `ClusterBackendService` is the sole
   durable membership owner. `[cluster].backends` are additive seeds; SQL
   `ADD BACKEND` / `DROP BACKEND` persist across FE restart. Heartbeat, live
   state, generation, and fragment activity are runtime observations, not a
   second durable catalog. Core consumes `BackendTopologyPort` only: do not add
   a metadata bridge, global registry, in-memory fallback, or direct-call path.
   `role=all-in-one` keeps loopback membership transient, while `role=be` does
   not create frontend membership or StateStore services.

7. **Language policy**
   - User interaction and design docs: Chinese
   - Code comments, logs, error messages, commit messages: English

---

## 4. Key Code Index (Validated Against Current Repository)

### 4.1 Entrypoints and Services

- `src/main.rs`
  Process entry. Dispatches FE-compatible modes (`run`, `start`, `stop`,
  `restart`) and the `standalone` mode.

- `src/server/mod.rs`
  MySQL-compatible standalone server, session context, SQL batch splitting,
  query timeout, and embedded statement routing.

- `src/service/internal_service.rs`
  FE-compatible query execution entrypoints: `submit_exec_plan_fragment`,
  `submit_exec_batch_plan_fragments`, `cancel`.

- `src/service/engine_ffi.rs`
  C ABI exports: submit/fetch/cancel and lake publish/abort.

- `src/service/compat.rs`
  Rust-to-C++ shim bootstrap bridge.

- `src/service/backend_service.rs`
  StarRocks BE thrift service (`be_port`).

- `src/service/heartbeat_service.rs`
  FE heartbeat service (`heartbeat_port`).

- `src/service/grpc_server.rs`
  gRPC server for exchange, runtime filters, lookup, and related internal RPCs.

- `src/service/grpc_client.rs`
  gRPC client for exchange and runtime filter transmission.

### 4.2 Standalone SQL Engine

- `src/engine/mod.rs`
  `StandaloneNovaRocks`, `StandaloneSession`, standalone state, query execution,
  catalog registration, execution-plan selection, and managed-lake open
  reconciliation.

- `src/engine/statement.rs`
  Standalone DDL/DML dispatch: database/catalog/table DDL, INSERT, DELETE,
  UPDATE/MERGE-related mutation routing, TRUNCATE, Iceberg schema/ref changes,
  ADD FILES, equality deletes, and OPTIMIZE commands.

- `src/engine/query_prep.rs`
  Standalone query registration and table-reference preparation, especially for
  Iceberg and three-part names.

- `src/engine/iceberg_writer.rs`
  Standalone Iceberg INSERT INTO / INSERT OVERWRITE write path.

- `src/engine/delete_flow.rs`, `src/engine/mutation_flow.rs`
  Standalone Iceberg DELETE, UPDATE, and MERGE-related mutation flows.

- `src/engine/mv_flow.rs`
  Standalone materialized-view refresh boundary.

- `src/sql/parser/**`
  StarRocks-oriented SQL parser extensions for catalogs, tables, materialized
  views, Iceberg refs, drops, and dialect behavior.

- `src/sql/analyzer/**`
  Standalone SQL analysis and name/expression resolution.

- `src/sql/optimizer/**`
  Standalone logical/physical optimization rules and cost/statistics helpers.

- `src/sql/codegen/**`
  Standalone SQL to execution-plan codegen.

### 4.3 FE Plan Lowering

- `src/lower/fragment.rs`
  Fragment-level execution preparation, runtime state assembly, lowering, and pipeline executor invocation.

- `src/lower/node/mod.rs`
  `TPlanNode` lowering dispatch by node type.

- `src/lower/expr/mod.rs`
  `TExpr` lowering entry and expression submodules.

- `src/lower/layout.rs`
  Tuple/slot layout inference and reordering.

- `src/lower/type_lowering.rs`
  Thrift type to execution-layer type mapping.

### 4.4 Execution Plan and Operators

- `src/exec/node/mod.rs`
  `ExecNode`, `ExecNodeKind`, `ExecPlan` definitions.

- `src/exec/expr/mod.rs`
  `ExprArena` and `ExprNode` execution-layer structures.

- `src/exec/operators/mod.rs`
  Operator factory registration; concrete operators are under `src/exec/operators/**`.

- `src/exec/chunk/mod.rs`
  `Chunk` (Arrow `RecordBatch` wrapper) and slot metadata mapping.

### 4.5 Pipeline Execution Framework

- `src/exec/pipeline/builder.rs`
  Builds pipeline graph from `ExecPlan`.

- `src/exec/pipeline/executor.rs`
  Top-level pipeline execution entry.

- `src/exec/pipeline/driver.rs`
  Driver execution logic.

- `src/exec/pipeline/global_driver_executor.rs`
  Global driver scheduling executor.

- `src/exec/pipeline/dependency.rs`
  Operator dependency management.

- `src/exec/pipeline/schedule/*`
  Scheduling and observable event mechanisms.

### 4.6 Exchange and Runtime

- `src/runtime/exchange.rs`
  Exchange receiver registry, chunk encode/decode, sender completion tracking.

- `src/runtime/exchange_scan.rs`
  `ScanOp` implementation for `EXCHANGE_NODE`.

- `src/service/exchange_sender.rs`
  Outbound queue, backpressure, and async send coordination.

- `src/runtime/result_buffer.rs`
  Query result buffering and fetch behavior.

- `src/runtime/query_context.rs`
  Query-level context, cancellation, and lifecycle management.

- `src/runtime/runtime_state.rs`
  Runtime state for cache, spill, runtime filters, and execution context.

### 4.7 Connectors / Catalog Backends / Formats / Filesystem

- `src/connector/mod.rs`
  `ConnectorRegistry`, scan connector registry, standalone catalog/table
  source and sink backends, and MV backend registration.

- `src/connector/jdbc.rs`
  JDBC/MySQL scan connector.

- `src/connector/hdfs.rs`
  HDFS/Iceberg/Parquet-style scan connector.

- `src/connector/starrocks.rs`
  StarRocks connector.

- `src/connector/iceberg/**`
  Iceberg scan, metadata, catalog registry, memory/Hadoop/REST catalog support,
  data/delete writers, commit actions, refs, compaction, and schema/default
  value helpers.

- `src/connector/starrocks/managed/**`
  Standalone managed-lake catalog backend, SQLite metadata store, DDL/DML,
  transaction lifecycle, erase worker, and materialized-view management.

- `src/formats/**`
  Parquet/ORC/StarRocks format readers.

- `src/fs/**`
  Local and opendal/OSS filesystem abstractions.

### 4.8 C++ Shim

- `src/shim/brpc_server.cpp`
  brpc service entry (protocol gateway).

- `src/shim/compat.cpp`
  C++ compat layer implementation.

- `src/shim/compat.h`
  Rust/C++ FFI ABI contract.

---

## 5. Core Execution Flows

### 5.1 FE-Compatible Query Path

1. FE sends requests to C++ Shim through brpc.
2. C++ forwards thrift binary attachments into Rust FFI (`engine_ffi.rs`).
3. `internal_service.rs` deserializes payloads and organizes fragment execution.
4. `lower/` transforms thrift plan/expr into `ExecPlan` + `ExprArena`.
5. `exec/pipeline/executor.rs` builds and schedules pipeline drivers.
6. Results are written into `result_buffer` or sent downstream via exchange sink.
7. FE fetches results through the fetch path (FFI fetch -> `result_buffer`).

### 5.2 Standalone SQL Path

1. A MySQL client or SQL test runner connects to `standalone`
   (`src/server/mod.rs`).
2. The standalone server resolves session state (`USE`, `SET catalog`,
   timeouts, current database) and forwards supported statements to
   `StandaloneSession::execute_in_context`.
3. `src/sql/parser/**` and `src/engine/statement.rs` route DDL/DML statements
   to local catalog, Iceberg catalog, or managed-lake backends.
4. SELECT/EXPLAIN statements go through standalone analyzer, optimizer, and
   codegen under `src/sql/**`.
5. Generated execution plans run through the shared pipeline/runtime stack.
6. Results return as `QueryResult`, then `src/server/encoding.rs` converts Arrow
   values to MySQL wire values.

### 5.3 Standalone Catalog and Storage Path

1. Local Parquet tables are registered in the standalone in-memory catalog.
2. Iceberg catalogs are registered in `IcebergCatalogRegistry`; supported
   catalog types are `memory`, `hadoop`, `rest`, and `hive`.
3. Managed-lake tables use SQLite metadata (`metadata_db_path`) plus object
   store configuration (`warehouse_uri` and `[standalone_server.object_store]`).
4. Standalone DDL/DML uses connector backends (`CatalogBackend`, `TableSource`,
   `TableSink`, `MvBackend`) to keep SQL dispatch separate from storage
   implementation.

### 5.4 Exchange Path

1. Sender-side operators encode chunks and send through `exchange_sender -> grpc_client`.
2. Receiver side (`grpc_server.exchange`) decodes payloads and pushes into `runtime/exchange`.
3. `ExchangeScanOp` blocks until all senders reach EOS.
4. On cancellation, `exchange::cancel_*` clears exchange keys and wakes blocked waiters.

---

## 6. Core Data Structures (Current Implementation)

- `Chunk`: `src/exec/chunk/mod.rs`
  Arrow `RecordBatch` wrapper with `slot_id -> column_index` mapping and memory accounting.

- `ExecPlan` / `ExecNode` / `ExecNodeKind`: `src/exec/node/mod.rs`
  Lowered execution plan tree.

- `ExprArena` / `ExprNode`: `src/exec/expr/mod.rs`
  Arena-based expression graph model.

- `Layout`: `src/lower/layout.rs`
  Tuple/slot layout metadata.

- `RuntimeState`: `src/runtime/runtime_state.rs`
  Runtime context for cache, spill, and runtime filter behavior.

- `ExchangeKey`: `src/runtime/exchange.rs`
  Exchange routing key (`fragment_instance_id + node_id`).

- `StandaloneNovaRocks` / `StandaloneSession` / `StandaloneState`: `src/engine/mod.rs`
  Standalone SQL engine state, catalog registries, managed-lake metadata handle,
  connector registry, and session execution surface.

- `QueryResult` / `QueryResultColumn`: `src/runtime/query_result.rs`
  Generic result type used by standalone SQL execution and MySQL response
  encoding.

---

## 7. Configuration and Runtime

### 7.1 Config File

- Default config file: `./novarocks.toml`
- Environment override: `NOVAROCKS_CONFIG=/path/to/file.toml`
- CLI override: `--config <path>`

### 7.2 Common Config Sections

- `[server]`
  `host`, `priority_networks`, `heartbeat_port`, `be_port`, `brpc_port`,
  `http_port`, `starlet_port`

- `[runtime]`
  `exchange_wait_ms`, `exchange_io_threads`, `exchange_io_max_inflight_bytes`,
  `pipeline_scan_thread_pool_thread_num`, `pipeline_exec_thread_pool_thread_num`, `cache.*`

- `[iceberg]`
  Embedded-JVM toggle for Iceberg metadata-table and remote metadata planning.

- `[standalone_server]`
  `mysql_port`, `user`, `metadata_db_path`, `warehouse_uri`,
  and `mv_default_storage_engine`.

- `[standalone_server.object_store]`
  Object-store endpoint and credentials for managed-lake standalone storage.

- `[debug]`
  `exec_node_output`, `exec_batch_plan_json`

- `[spill]`
  Spill enablement, directories, block size, and compression strategy

### 7.3 Local Test Environment (Iceberg REST + MinIO + Spark)

The canonical local test fixture lives at `docker/iceberg-rest/` and is also
the CI fixture for the `iceberg`, `iceberg-compatibility`, and `iceberg-rest`
SQL suites. The Codex workspace manifest at
`.codex/environments/environment.toml` points setup at this directory.

The Docker side is shared across worktrees by default. Codex environment setup
only runs `docker/iceberg-rest/up.sh --prepare-only`, which generates this
worktree's runtime entry and does not start Docker. When Docker-backed tests
are actually needed, `docker/iceberg-rest/up.sh` starts or reuses one shared
Docker Compose project configured by
`docker/iceberg-rest/shared.env`; the default shared service ports are MinIO
`9000`, MinIO console `9001`, Iceberg REST `8181`, and Spark UI `4040`. Each
worktree still gets its own generated runtime entry and a separate NovaRocks
standalone port.

Do not guess the NovaRocks server port. Always discover the active worktree
environment from the fixed generated entry:

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

Important generated locations:

- `docker/iceberg-rest/runtime/current/env.sh`
  Shell exports for the active worktree. Prefer this for commands.
- `docker/iceberg-rest/runtime/current/manifest.json`
  Machine-readable endpoints, ports, Docker Compose project, warehouses, and config paths.
- `docker/iceberg-rest/runtime/current/README.md`
  Human-readable summary of the active worktree environment.

Important environment variables after sourcing `env.sh`:

- `NOVA_ENV_SHARED_DOCKER`, `NOVA_ENV_COMPOSE_PROJECT`, `NOVA_ENV_CONFIG_FILE`
- `NOVA_ENV_MINIO_PORT`, `NOVA_ENV_REST_PORT`, `NOVA_ENV_MYSQL_PORT`
- `NOVA_ENV_SPARK_UI_PORT`
- `AWS_S3_ENDPOINT`, `AWS_S3_ACCESS_KEY_ID`, `AWS_S3_SECRET_ACCESS_KEY`
- `NOVAROCKS_ICEBERG_REST_URI`
- `NOVAROCKS_ICEBERG_REST_WAREHOUSE`
- `NOVAROCKS_STANDALONE_CONFIG`
- `NOVAROCKS_SQL_TEST_CONFIG`
- `NOVAROCKS_ICE_REST_CATALOG_SQL`
- `NOVAROCKS_SPARK_DEFAULTS`
- `NOVAROCKS_SPARK_V3_SMOKE_SQL`
- `NOVAROCKS_SPARK_SQL`

If the fixed entry is missing, initialize or inspect the environment with:

```bash
docker/iceberg-rest/up.sh --prepare-only
docker/iceberg-rest/status.sh
```

Start standalone against the generated config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"
```

When backgrounding the server (e.g. inside an automated test driver), wait
for the readiness marker before issuing the first query — probing the mysql
port alone cannot distinguish a freshly-bound server from a leftover
process that already owned the port:

```bash
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
# Wait up to 60 s for the server to bind. If bind fails the line never
# appears, the process exits with code 1, and `wait` surfaces the failure.
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }
```

The marker line is emitted on stdout immediately after a successful bind:
`NOVAROCKS_READY mysql_port=23223 pid=<pid>`. Any orchestration that
backgrounds the server **must** gate its first connection on this line.

Run SQL tests with the generated runner config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Run cross-engine Iceberg compatibility tests where Spark writes through REST
Catalog + MinIO and NovaRocks reads the table:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Run NovaRocks-only Iceberg REST end-to-end smoke (no Spark, NovaRocks both
writes and reads):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Generate an Iceberg format-v3 table through Spark against the same REST Catalog
and MinIO services:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

Inside the Docker network, Spark must use `http://rest:8181` for REST Catalog
and `http://minio:9000` for object storage. NovaRocks should use the host
endpoints from `env.sh`. Do not mix container endpoints into NovaRocks catalog
SQL.

Workspace cleanup uses `docker/iceberg-rest/down.sh --runtime-only --purge`,
which removes only this worktree's generated runtime entry. It deliberately
leaves the shared Docker services running because other worktrees may be using
them. Use `docker/iceberg-rest/down.sh --docker` only when you explicitly want
to stop the shared Docker project, and `--docker --volumes` only when you also
want to delete its shared MinIO volume.

---

## 8. Development and Testing Standards

### 8.1 Language Standard

- User communication and design docs: Chinese
- Code comments/logs/errors/commit messages: English

### 8.2 Build Mode

Three profiles trade compile time against query speed (numbers are from a 10-core
machine — treat as relative, not absolute):

- **Debug build (`cargo build`, profile `dev`)**: opt-level 0. Fastest incremental
  rebuild (~18s) but slow query execution (~5-10x slower than release).
  **Use when**: checking correctness or running targeted queries where runtime does
  not matter.
- **Balanced build (`cargo build --profile dev-opt`, artifacts in `target/dev-opt/`)**:
  opt-level 1, `debug = 1`, `codegen-units = 256`, `incremental = true`, `lto = false`.
  Incremental rebuild ~32s (vs ~6 min for release on the same one-line lib edit) while
  query execution matches release. On engine-CPU-bound SQL suites (many small queries)
  it runs ~1.9x faster than debug; on object-store-I/O-bound bulk scans (SSB/TPC-H over
  MinIO) the profile barely matters. First (cold) build is ~2x debug because all
  dependencies are optimized too — a one-time cost.
  **Use when**: iterating on the SQL/test loop and you want fast rebuilds *and*
  near-release query speed. Default for running suites during development.
- **Release build (`cargo build --release`)**: opt-level 3 + thin LTO + `codegen-units = 1`,
  `incremental` off. Fastest execution, but incremental rebuilds are punishing
  (~6 min for a one-line lib change), so it is unusable for iteration.
  **Use when**: measuring query latency/throughput or running benchmarks.

**Rule of thumb**: `dev` for pure correctness iteration; `dev-opt` for the dev/test
loop when query speed matters (fast rebuilds + release-class runtime); `--release`
only for performance measurement and benchmarks.

### 8.3 Code Quality

- `cargo fmt`
- `cargo clippy`
- `cargo build`
- `cargo test`

### 8.4 SQL Regression Tests

Unified runner under `sql-tests`. It requires a running NovaRocks
MySQL-compatible standalone server. Do not assume a fixed port in Codex
workspaces; source `docker/iceberg-rest/runtime/current/env.sh` when that
entry exists.

**Start standalone (no external FE needed):**

```bash
# Debug: fast compile, slow query (for fix verification)
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --port 9030

# Release: slow compile, fast query (for suite testing)
NO_PROXY=127.0.0.1,localhost cargo run --release -p novarocks-server -- standalone --port 9030
```

When the local test environment is active:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"
```

When starting a server manually inside a Codex worktree, prefer the generated
config or `--port "$NOVA_ENV_MYSQL_PORT"` instead of hard-coding `9030`,
because another worktree may already be using the default MySQL port.

**Run test suites:**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> --mode <verify|record|diff> [--query-timeout 60] [-j 4]
```

With a generated runner config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Available suites: `ssb`, `tpc-h`, `tpc-ds`, `cte`, `join`, `filter`, `sort`, etc.
The `starrocks-compat` suite is special: the runner deploys its own FE + BE
cluster and needs extra environment — see 8.5 before running it.

**Run specific cases:**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --only q10,q35,q69 --mode verify
```

### 8.5 StarRocks-Compat Suite (compat CI: runner-managed FE + 3 compat BE)

The `starrocks-compat` suite is the compat CI gate. Unlike every other
suite, it does NOT connect to a standalone server you started. The runner
itself deploys an ephemeral cluster per run: one isolated StarRocks FE
(symlinked from a prebuilt FE home, with a re-rendered `fe.conf` and dynamic
ports) plus three compat-mode NovaRocks BEs (from a validated compat
artifact), registers the BEs, waits for topology, runs the cases, and tears
everything down. **Do not start FE/BE manually for this suite**, and do not
confuse this flow with the long-lived manual deployment used for FE Java
development — the two share the FE home directory but nothing else.

**Prerequisites (all hard preflight failures if missing):**

1. **Built StarRocks FE home** at `$HOME/starrocks-on-novarocks/fe`
   (override with `STARROCKS_FE_HOME`). It must contain `bin/start_fe.sh`
   and the full `lib/*.jar` set from a StarRocks `./build.sh --fe` build.
2. **Shared-data keys in the FE home's `conf/fe.conf`.** The runner renders
   its own isolated `fe.conf` (ports, meta dir, `run_mode = shared_data`)
   but copies ONLY the builtin-storage-volume keys from the base file and
   preflights them. The base `fe.conf` must therefore contain a valid block
   such as:

   ```properties
   enable_load_volume_from_conf = true
   cloud_native_storage_type = S3
   aws_s3_path = <bucket>/starrocks-compat
   aws_s3_endpoint = http://127.0.0.1:9000   # shared MinIO; see env.sh AWS_S3_ENDPOINT
   aws_s3_access_key = <from env.sh AWS_S3_ACCESS_KEY_ID>
   aws_s3_secret_key = <from env.sh AWS_S3_SECRET_ACCESS_KEY>
   ```

   Endpoint and credentials must match the shared MinIO from
   `docker/iceberg-rest/runtime/current/env.sh`.
3. **Shared MinIO running** (`docker/iceberg-rest/up.sh`).
4. **Java 17+** on PATH (or `JAVA_HOME`).
5. **>= 5 GB free disk** on the volume holding the repo — the FE refuses to
   start when the meta dir has less (`MetaHelper.checkMetaDir`).
6. **Compat artifact + thirdparty toolchain.** BEs run a compat-feature
   binary described by a manifest. Building it (explicitly or via the
   runner's auto-build fallback) needs the StarRocks thirdparty environment
   exported — non-interactive shells do not source `~/.zshrc`:

   ```bash
   export STARROCKS_THIRDPARTY=$HOME/project/starrocks/thirdparty
   export STARROCKS_THIRDPARTY_INSTALLED=$HOME/project/starrocks/thirdparty/installed
   export STARROCKS_HOME=$HOME/project/starrocks
   export STARROCKS_GCC_HOME=/usr/bin
   ```

**Environment contract consumed by the runner:**

| Variable | Meaning |
|----------|---------|
| `STARROCKS_FE_HOME` | FE home override (default `$HOME/starrocks-on-novarocks/fe`) |
| `NOVAROCKS_COMPAT_ARTIFACT_MANIFEST` | Path to a prebuilt compat artifact manifest; unset = runner builds one itself (needs thirdparty env) |
| `NOVAROCKS_COMPAT_ARTIFACT_PROFILE` | Cargo profile the manifest must declare (e.g. `dev`) |
| `NOVAROCKS_BIN` | Default (non-compat) binary; must be a DIFFERENT binary than the compat artifact (SHA-256 checked) |
| `NO_PROXY=127.0.0.1,localhost` | Required, as for every suite touching local services |

The manifest also pins `git_head`: it must equal the current repo HEAD, so
rebuild the artifact after every commit/rebase (a dirty working tree is
fine — only HEAD is compared).

**Canonical commands:**

```bash
# 1. Build the compat artifact (after exporting the thirdparty env above)
tools/ci/build-compat-artifact.sh --profile dev --output-dir /tmp/nr-compat-artifact

# 2. Run the suite
source docker/iceberg-rest/runtime/current/env.sh
env NO_PROXY=127.0.0.1,localhost \
  NOVAROCKS_BIN="$(pwd)/target/debug/novarocks" \
  NOVAROCKS_COMPAT_ARTIFACT_MANIFEST=/tmp/nr-compat-artifact/manifest.txt \
  NOVAROCKS_COMPAT_ARTIFACT_PROFILE=dev \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite starrocks-compat --mode verify -j 1
```

**Debugging failures:**

- Per-run state lives under `.sql-test-runner-runtime/starrocks-compat-*/`:
  `fe/log/fe.log` (FE), `be-N.log` (captured BE stdout/stderr), and
  `be-N/log/novarocks.INFO|WARNING|ERROR` (BE tracing files). The whole
  directory is deleted during cleanup, so capture what you need while the
  run is still failing, or instrument locally.
- `@be_log_contains` / `@be_log_count_at_least` directives read ONLY the
  captured stdout/stderr (`be-N.log`), never the rotating tracing files.
  Test-evidence markers in BE code must be emitted with `eprintln!`
  (see `compat_scan`, `compat_ingress`, `compat_fragment_sink`).
- On macOS, a BE that registers fine but then drops every connection with
  zero log output usually died from a signal; check the crash reports in
  `~/Library/Logs/DiagnosticReports/novarocks-compat-*.ips` (the
  `faultingThread` stack is in the JSON body).

---

## 9. Suggested Starting Points for Typical Changes

- **Plan lowering changes**: start with `src/lower/node/mod.rs` and the relevant node submodules.
- **Standalone SQL/parser/planner changes**: start with `src/sql/parser/**`,
  `src/sql/analyzer/**`, `src/sql/optimizer/**`, `src/sql/codegen/**`, and
  `src/engine/mod.rs`.
- **Standalone MySQL protocol behavior**: inspect `src/server/mod.rs` and
  `src/server/encoding.rs`.
- **Standalone DDL/DML behavior**: inspect `src/engine/statement.rs` first,
  then the specific flow file (`insert_flow`, `delete_flow`, `mutation_flow`,
  `iceberg_writer`, or `mv_flow`).
- **Execution semantics/operator behavior**: inspect `src/exec/node/*` and `src/exec/operators/*`.
- **Scheduling/parallelism**: inspect `src/exec/pipeline/*`.
- **Exchange behavior**: inspect `src/runtime/exchange.rs`, `src/runtime/exchange_scan.rs`, `src/service/grpc_*.rs`.
- **Connector behavior**: inspect `src/connector/*`, `src/connector/iceberg/**`,
  `src/connector/starrocks/managed/**`, and `src/formats/*`.
- **FE/BE interface behavior**: inspect `src/service/internal_service.rs`, `src/service/backend_service.rs`, `src/service/engine_ffi.rs`, `src/shim/compat.h`.
- **Optimizer observability / plan-shape regression**: see
  `src/sql/explain.rs` for the EXPLAIN formatter (Normal/Verbose/Costs/
  Analyze). `EXPLAIN ANALYZE` returns a query-level Planning/Execution/
  Rows header above the Verbose plan; per-operator runtime stats are a
  follow-up. Verbose/Costs/Analyze append a stable `stats={rows=N}`
  trailer to each physical node. `SET disable_optimizer_rules = 'RuleA,RuleB'`
  (alias `cbo_disabled_rules`) bisects optimizer rules at session level;
  see `src/sql/optimizer/options.rs`. Use the `sql-tests/optimizer/` suite
  for plan-golden cases, and `-- @explain_contains=<substr>` /
  `-- @normalize_explain_timing` in any sql-test case to assert plan-shape
  facts alongside the result golden.
- **Aggregate pushdown rule (OPT-1)**: see
  `src/sql/optimizer/rbo/rules/aggregate_pushdown/`. Pushes
  `LogicalAggregate` past inner/outer joins toward leaves when NDV
  bucketing predicts a real row-count reduction. White-list functions
  are SUM/MIN/MAX/COUNT(col). Disable via
  `SET disable_optimizer_rules = 'AggregatePushdown'`. Plan-shape
  cases live under `sql-tests/optimizer/aggregate_pushdown_*.sql`. The
  idempotency guard is `AggregateNode::already_pushed` — other rules
  must preserve the flag when cloning.

---

## 10. StarRocks Reference Code Location

For StarRocks side-by-side reference implementation, use: `~/project/starrocks`

---

## 11. Architecture Decision Records (ADR)

Durable design decisions, philosophies, and their honestly-recorded trade-offs live in
`docs/adr/` (index and authoring rules: `docs/adr/README.md`).
Before changing architecture-level behavior, check the index for the affected domain.
Any PR that embodies a new design decision or accepts a compromise must add or
supersede an ADR — use the `/adr` skill (`skills/adr/`, exposed to Claude Code via the
`.claude/skills/adr` symlink), which embeds the template, numbering, supersede, and
collision-renumbering rules.
