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

Its production runtime is the native NovaRocks FE/BE role model:

- **Native FE/BE roles**
  - `role=fe` owns the MySQL SQL entrypoint, statement admission, planning, and
    distributed coordination.
  - `role=be` owns local fragment execution and the native gRPC boundary.
  - `role=all-in-one` preserves the FE/BE application boundary for local
    testing; it is not a separate production topology.

- **StarRocks external Connector**
  - StarRocks is read-only external data, never an inbound server protocol.
  - RPC reads support every topology; direct reads permanently require
    shared-data.

Columnar processing is centered on Arrow `RecordBatch` / `Chunk`.

---

## 2. Architecture Overview (Current Code)

### 2.1 Native FE/BE Roles

```text
SQL client
  `- MySQL protocol -----------> NovaRocks role=fe
                                      |
                                      v
                            Frontend SQL/coordinator application
                                      |
                                      v
                            Native gRPC fragment submission
                                      |
                                      v
                            NovaRocks role=be
                                      |
                            Pipeline / Runtime / Connectors
```

---

## 3. Non-Negotiable Rules (High Priority)

1. **Strictly follow native frozen contracts and type metadata**
   No fallback behavior, no guessed defaults, no implicit type downgrade.

2. **Fail fast on unsupported or ambiguous semantics**
   Return explicit errors in parsing/lowering stages instead of "best effort" execution.

3. **Keep protocol and execution responsibilities separated**
   Native gRPC is the FE/BE process boundary; execution semantics belong to
   their Rust application owners.

4. **Keep native role and Connector responsibilities explicit**
   FE owns SQL admission and global coordination; BE owns local execution.
   Connectors own external-system facts. Do not mix those owners through
   process-global state, direct calls, or transport fallback.

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

- `novarocks-server/src/main.rs`
  Native top-level command dispatch. The only server command is `standalone`;
  it chooses `fe`, `be`, or `all-in-one` before startup side effects.

- `novarocks/frontend/src/**`
  FE application composition, SQL admission, connector control host, and
  distributed query coordination.

- `novarocks/backend/src/**`
  BE application composition, native gRPC services, query lifecycle registry,
  and connector execution host.

- `novarocks/core/src/**`
  Carrier-neutral execution, query lifecycle contracts, and native runtime
  kernels shared by FE and BE application owners.

### 4.2 Standalone SQL Engine

- `src/engine/mod.rs`
  `StandaloneNovaRocks`, `StandaloneSession`, standalone state, query execution,
  external catalog registration, and execution-plan selection.

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

### 4.7 Connectors / Catalog Backends / Filesystem

- `novarocks/connector/starrocks/**`
  Read-only StarRocks external Connector. It owns RPC remote reads for every
  topology and shared-data-only direct reads; it does not depend on Core,
  Frontend, Backend, Compat, or an Iceberg provider.

- `novarocks/connector/iceberg/**`
  Iceberg control/execution contracts, catalog integrations, and storage facts.

- `novarocks/fs/**`
  Connector-neutral authorized object-store access.

---

## 5. Core Execution Flows

### 5.1 Native SQL and Connector Path

1. A MySQL client connects to a `role=fe` process through the native SQL
   entrypoint.
2. The frontend owns session admission, catalog resolution, planning, and
   coordinator lifecycle assembly.
3. Persistent tables belong to external providers. Iceberg owns its catalog and
   mutation truth; StarRocks is a read-only external Connector.
4. The frontend freezes native fragment and Connector facts, then sends them to
   one or more `role=be` processes through native gRPC.
5. BE hosts bind installed Connector execution instances and run Arrow batches
   through the pipeline/runtime stack.

### 5.2 Exchange Path

1. Sender-side operators encode chunks and send through `exchange_sender -> grpc_client`.
2. Receiver side (`grpc_server.exchange`) decodes payloads and pushes into `runtime/exchange`.
3. `ExchangeScanOp` blocks until all senders reach EOS.
4. On cancellation, `exchange::cancel_*` clears exchange keys and wakes blocked waiters.

### 5.3 Native Distributed Query Lifecycle Path

1. `novarocks/frontend/src/coordinator/execution.rs` freezes a
   `QueryExecutionId` and participant manifests from one live backend snapshot.
2. `novarocks/frontend/src/coordinator/query_lifecycle/` concurrently sends
   `InitQuery`, attaches every `QueryControlStream`, and waits for every
   `ControlReady`.
3. Only the resulting control-ready execution exposes native fragment
   submission; each `SubmitFragmentRequest` carries the same execution id.
4. `novarocks/backend/src/query_lifecycle/registry.rs` owns the BE-local wire
   lifecycle entry, exact fragment admission, heartbeat fail-close, bounded
   tombstones, and the single termination latch.
5. Runtime-filter state is installed as an Init contribution. Client
   cancellation remains `KILL QUERY` through the frontend query-control owner
   and is delivered as lifecycle Abort.
6. QLC-2 still submits and immediately starts fragments one by one after the
   barrier. Atomic Stage/Start is a later lifecycle phase, so this path must not
   be described as global atomic startup.

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
  Standalone SQL engine state, external catalog registries, connector registry,
  and session execution surface.

- `QueryResult` / `QueryResultColumn`: `src/runtime/query_result.rs`
  Generic result type used by standalone SQL execution and MySQL response
  encoding.

- `QueryExecutionId` / `ParticipantManifest`:
  `novarocks/core/src/query_execution/lifecycle/`
  Immutable native query-attempt identity, participant contract, digest, and
  wire codec shared across the process boundary.

- `QueryLifecycleRegistry`:
  `novarocks/backend/src/query_lifecycle/registry.rs`
  BE-owned lifecycle state, fragment admission, heartbeat/pre-start timeout,
  termination, and bounded tombstone registry.

---

## 7. Configuration and Runtime

### 7.1 Config File

- Default config file: `./novarocks.toml`
- Environment override: `NOVAROCKS_CONFIG=/path/to/file.toml`
- CLI override: `--config <path>`

### 7.2 Common Config Sections

- `[server]`
  `host`, `priority_networks`, `http_port`, `grpc_port`, and native advertise
  identity.

- `[runtime]`
  `exchange_wait_ms`, `exchange_io_threads`, `exchange_io_max_inflight_bytes`,
  `pipeline_scan_thread_pool_thread_num`, `pipeline_exec_thread_pool_thread_num`, `cache.*`

- `[iceberg]`
  Embedded-JVM toggle for Iceberg metadata-table and remote metadata planning.

- `[standalone_server]`
  `mysql_port`, `user`, MV scheduler settings, and Iceberg maintenance settings.

- `[connector.object_store]`
  Process-local object-store credentials for connector execution; this does
  not define a native internal table store.

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
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --role all-in-one

# Release: slow compile, fast query (for suite testing)
NO_PROXY=127.0.0.1,localhost cargo run --release -p novarocks-server -- standalone --role all-in-one
```

When the local test environment is active:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"
```

When starting a server manually inside a Codex worktree, prefer the generated
config so its configured MySQL port cannot collide with another worktree.

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

Available suites: `ssb`, `tpc-h`, `tpc-ds`, `cte`, `join`, `filter`, `sort`, and
native distributed suites. Cluster mode and backend count come from runner CLI;
no suite owns an alternate server runtime.

**Run specific cases:**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --only q10,q35,q69 --mode verify
```

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
- **Native distributed query lifecycle**: inspect
  `novarocks/frontend/src/coordinator/execution.rs`,
  `novarocks/frontend/src/coordinator/query_lifecycle/**`,
  `novarocks/backend/src/query_lifecycle/**`,
  `novarocks/core/src/query_execution/lifecycle/**`, and
  `novarocks/core/src/query_execution/fragment_transport.rs`. Preserve the
  Init + ControlReady production barrier; do not add a lifecycle shim,
  standalone direct-call path, or no-runtime-filter retry inside an attempt.
- **Connector behavior**: inspect `novarocks/connector/**` and
  `novarocks/fs/**`. StarRocks is a read-only external Connector, not a native
  internal-table catalog or a server protocol.
- **FE/BE interface behavior**: inspect `novarocks/frontend/src/**`,
  `novarocks/backend/src/**`, and the neutral contracts under `novarocks/spi/**`.
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
supersede an ADR — use the `/adr` skill (`.agents/skills/adr/`, exposed directly
to Codex and to Claude Code via the `.claude/skills/adr` symlink), which embeds the
template, numbering, supersede, and collision-renumbering rules.

---

## 12. Project Development Workflow

For NovaRocks feature, architecture, roadmap, and refactor work, resolve the
project documentation root from memory. Use the newest directly applicable,
existing path; when memory has no usable project documentation path, use
`<repo-root>/docs/workflow`.

Use the generic skills-only plugin under `.agents/skills/dev-workflow/`:

- `dev-workflow`: identify and route the current stage;
- `dev-workflow-discuss-design`: settle the problem and major design decisions;
- `dev-workflow-write-spec`: write the accepted design into project docs;
- `dev-workflow-plan`: plan with Codex Plan mode and persist the
  approved plan;
- `dev-workflow-execute`: create a goal and execute continuously through
  verification;
- `dev-workflow-finish`: publish and archive only when authorized.

Codex discovers the project-local skill sources directly under `.agents/skills/`.

Do not skip the accepted-design and approved-plan gates. Sub-agents are allowed
in every stage when they provide useful parallel investigation, independent
verification, isolated implementation, or risk review. The main agent retains
ownership and verifies their results.

The bundle's
`.agents/skills/dev-workflow/skills/dev-workflow/references/workflow-contract.md` is the
only development-workflow contract. Do not depend on an external workflow
document. In Plan mode, produce a task DAG with hard dependencies, parallel
waves, non-overlapping file ownership, sub-agent scheduling labels, independent
validation, integration gates, and local commit checkpoints.

Once execution starts, routine implementation difficulties are not reasons to
stop; pause only for the major decision conditions defined by
`dev-workflow-execute`. On a task-local development branch, checkpoint commits
are allowed after a coherent plan section completes or before risky changes.
The execute stage must never push or open a PR; those actions require explicit
authorization in `dev-workflow-finish`.
