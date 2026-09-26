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
  - `all-in-one` is a server-owned local supervisor that reads one normal FE
    config and one normal BE config. It preserves the FE/BE application
    boundary and is not an application role or separate production topology.

- **Sealed external Connector providers**
  - The active Server provider manifest contains exactly Iceberg and Paimon.
  - Paimon is read-only for append-only and `deduplicate` primary-key snapshot
    reads. Iceberg retains its existing read/write capabilities.
  - StarRocks is retired: its source remains for reference, but it has no active
    read capability and `[connector.starrocks]` is rejected at configuration
    parsing.

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

5. **Distributed deployment is the source of truth; all-in-one is test-only**
   The real, user-facing deployment is NovaRocks distributed (1 NovaRocks FE +
   N BE; CI baseline 1FE+3BE). The single-process all-in-one form is only a
   testing convenience. It starts normal FE and BE role runners with a pair of
   deployable configs. Do NOT model for, or add special-case branches for, the
   single-process form. Cluster-topology quantities (broadcast fanout,
   backend count, per-node budgets) must be read dynamically from the live BE
   registry, never hardcoded or defaulted to "single-process = 1 node". Tests
   must never pass only in standalone while failing under 1FE+3BE.

6. **External orchestration owns backend desired membership**
   In `role=fe`, `ClusterBackendService` is an in-memory, rebuildable runtime
   projection. A BE mints one UUIDv7 process identity per start and can enter
   new-query scheduling only after its authenticated announce and the FE-pull
   exact heartbeat agree on the immutable descriptor. StateStore is never a
   backend-membership source; `[cluster].backends` and SQL `ADD BACKEND` /
   `DROP BACKEND` are removed. Heartbeat/announce loss affects future admission,
   not an existing attempt; only lifecycle/control/transport evidence or exact
   process replacement decides that attempt's fate. Core consumes the read-only
   `BackendTopologyPort` only: do not add a metadata bridge, global registry,
   in-memory fallback, or direct-call production path. `role=be` never creates
   frontend membership or StateStore services.

7. **Language policy**
   - User interaction and design docs: Chinese
   - Code comments, logs, error messages, commit messages: English

---

## 4. Key Code Index (Validated Against Current Repository)

### 4.1 Entrypoints and Services

- `novarocks-server/src/main.rs`
  Native top-level command dispatch. The only server command is `standalone`;
  it chooses `fe`, `be`, or `all-in-one` before startup side effects.

- `novarocks/frontend-application/src/**`
  FE application composition, SQL admission, catalog projection, native query
  execution adapter and distributed task coordination. Query-local semantic
  policy remains in `novarocks/query-application/src/**`; long-lived Catalog
  generation ownership remains in `novarocks/catalog-application/src/**`.
  `state_family/**` is the closed manifest of every frontend-local state
  family: it owns each family's classification (`ExternalProjection` /
  `ProcessRuntime` / `Accelerator`) and is the single definition point for
  persistent key prefixes and record versions, so a runtime family
  structurally cannot own durable state (ADR-0114).

- `novarocks/worker/src/**` and `novarocks/native-adapter/src/**`
  Worker owns BE-local Task/context admission, lifecycle, leases, convergence
  and connector execution policy. Native Adapter owns generated stubs/codec,
  listener composition, data-plane handlers and role-local wire projection.
  Keep generated DTO decode at the adapter boundary; do not move it into
  Worker or recreate a Backend facade.

- `novarocks/execution/src/**` and `novarocks/execution-contract/src/**`
  Carrier-neutral execution, query lifecycle contracts, and native runtime
  kernels shared by FE and BE application owners. `execution-contract` owns the
  identities and contracts both roles must agree on; `execution` owns the
  runtime that runs under them.

### 4.2 SQL Frontend: Parse, Plan, Admit

- `novarocks/parser/src/**`
  Hand-written lexer, parser and AST, including the StarRocks-compatible
  surface for catalogs, tables, materialized views, Iceberg refs and drops.

- `novarocks/sql/src/{analysis,analyzer,semantic}/**`
  Name and expression resolution, type rules and semantic validation.

- `novarocks/sql/src/optimizer/**`
  Logical/physical optimization: `rewrite/rules/**` and `cascades_rules/**` for
  rules, `cost.rs` and `estimate/**` for cost and statistics, `options.rs` for
  session-level rule disabling.

- `novarocks/sql/src/planner/**`
  Builds logical plans, materializes optimizer output into planner physical IR,
  and plans distributed fragment topology (`logical/`, `physical/`,
  `distributed/`, `pipeline/`, `runtime_filter/`).

- `novarocks/query-application/src/sql.rs`, `novarocks/query-application/src/sql/**`
  Query-application ownership of parser-admitted statement shape: statement
  admission and DDL/DML routing.

- `novarocks/frontend-application/src/query_execution/dml/**`
  DML flows — `insert.rs`, `delete/**`, `mutation.rs` / `mutation_flow.rs`,
  `truncate.rs`, `add_files.rs`, `ctas.rs` / `iceberg_ctas.rs`,
  `iceberg_writer.rs` (INSERT INTO / INSERT OVERWRITE).

- `novarocks/frontend-application/src/catalog_application/statement.rs`
  Catalog, database and table DDL against the projected catalog.

- `novarocks/mysql-adapter/src/**`
  MySQL protocol: `listener.rs` (connection serving and drain),
  `result_encoding.rs` and `row_encoding.rs` (result-set and row encoding),
  `error_mapping/**`.

### 4.3 Native Plan Wire and BE-Side Fragment Decode

- `novarocks/plan-codec/src/**`
  Deterministic planner-IR-to-protobuf encoding for the native FE/BE boundary;
  `native_type.rs` and `physical_expr.rs` encode the frozen type and expression vocabulary.

- `novarocks/native-adapter/src/fragment_plan_node.rs`
  Native node lowering during BE preparation, dispatched by node type.

- `novarocks/native-adapter/src/fragment_plan_decode/**`
  Per-node decode (filter, project, sort, topn, joins, table function,
  table write, unpivot, change-event expand).

- `novarocks/native-adapter/src/fragment_expression/**`
  Expression decode by expression kind.

- `novarocks/native-adapter/src/{fragment_layout.rs,descriptor_snapshot.rs}`
  Output-layout and exchange-input contract projections; tuple/slot descriptors.

- `novarocks/native-adapter/src/{fragment_submission.rs,fragment_instance.rs,fragment_plan_decode_submission.rs}`
  Fragment envelope and per-task assignment projection. Accepted preparation
  separates the decoded construction plan into pure `LocalProgram` and exact
  task-owned `LocalRuntimeBindings` before pipeline preparation.

Keep generated DTO decode at this boundary; do not move it into Worker or
Execution, and do not recreate a Backend facade around it.

### 4.4 Execution Plan and Operators

- `novarocks/local-program/src/**`
  Pure task-independent node, expression, layout, sink and binding-requirement
  facts. `LocalProgram` owns no live scan, writer, connector or async capability.

- `novarocks/execution/src/exec/node/lowering.rs`
  Separates a construction `ExecPlan` into `LocalProgram` and per-task
  `LocalRuntimeBindings`; the bindings retain exact scan/writer/finisher owners.

- `novarocks/execution/src/runtime/fragment/submission.rs`
  One task submission owns its program and runtime bindings, with exact
  requirement/binding validation before drivers are prepared.

- `novarocks/execution/src/exec/node/mod.rs`
  `ExecNode`, `ExecNodeKind`, `ExecPlan` construction types.

- `novarocks/execution/src/exec/expr/mod.rs`
  `ExprArena` and `ExprNode` execution-layer structures.

- `novarocks/execution/src/exec/operators/mod.rs`
  Operator factory registration; concrete operators are under
  `novarocks/execution/src/exec/operators/**`.

- `novarocks/execution/src/exec/chunk/{mod.rs,chunk_impl.rs}`
  `Chunk` (Arrow `RecordBatch` wrapper) and slot metadata mapping.

### 4.5 Pipeline Execution Framework

- `novarocks/execution/src/exec/pipeline/builder.rs`
  Construction-plan helpers; `builder/local.rs` builds the production pipeline
  directly from `LocalProgram` and exact `LocalRuntimeBindings`.

- `novarocks/execution/src/exec/pipeline/executor.rs`
  Top-level pipeline execution entry, including
  `prepare_report_neutral_local_program_pipeline_execution` for one pure program
  and its task-owned capabilities.

- `novarocks/execution/src/exec/pipeline/driver.rs`
  Driver execution logic.

- `novarocks/execution/src/exec/pipeline/global_driver_executor.rs`
  Global driver scheduling executor.

- `novarocks/execution/src/exec/pipeline/dependency.rs`
  Operator dependency management.

- `novarocks/execution/src/exec/pipeline/schedule/**`
  Scheduling and observable event mechanisms.

### 4.6 Exchange and Runtime

- `novarocks/execution/src/runtime/exchange.rs`
  Exchange receiver registry, chunk encode/decode, sender completion tracking.

- `novarocks/execution/src/runtime/fragment/io/**`
  Fragment I/O edges: `exchange_edge.rs` (the Installed/normal-close gated open
  barrier), `exchange_queue.rs` (outbound queue and backpressure),
  `exchange_receiver.rs` (application-hosted ingress for one receiver),
  `result.rs`, `scan.rs`, `commit.rs`.

- `novarocks/execution/src/exec/operators/exchange_source.rs`
  Source operator for an exchange node: reconstructs chunks and tracks sender
  completion.

- `novarocks/native-adapter/src/{exchange_data_plane.rs,exchange_transmitter.rs,fragment_exchange_receiver.rs}`
  Native exchange wire projection, route settlement and transmission.

- `novarocks/worker/src/result_buffer.rs`
  BE-local result buffering, retained budget and fetch behavior.

- `novarocks/worker/src/query_context.rs`
  BE-local query context manager, cleanup leases and resource snapshots.

- `novarocks/execution/src/runtime/runtime_state.rs`
  Runtime state for cache, spill, runtime filters, and execution context.

### 4.7 Connectors / Catalog Backends / Filesystem

- `novarocks/types/src/{naming,schema}.rs`
  Neutral catalog naming and schema vocabulary. It contains no catalog runtime
  or provider authority.

- `novarocks/sql/src/catalog/memory.rs`
  SQL-owned local `PlannerMemoryCatalog`; it stores private planner facts and
  materializes only SQL catalog-visible values.

- `novarocks/frontend-application/src/catalog_application/query_catalog/**`
  Frontend-owned query catalog registry, schema cache, and service composition.
  Connector admission and local-catalog snapshots remain application-owned.

- `novarocks/connector/starrocks/**`
  Retired reference implementation. It is not registered by the Server
  provider manifest and provides no active product read capability.

- `novarocks/connector/iceberg/**`
  Iceberg control/execution contracts, catalog integrations, and storage facts.

- `novarocks/connector/paimon/**`
  Read-only Paimon Filesystem Catalog, private wire codec, snapshot planning,
  and append-only / `deduplicate` primary-key table reads.

- `novarocks/fs/**`
  Connector-neutral authorized object-store access. Its process-local
  `StorageAuthority` acquires and renews vended material for the consumer that
  signs the object request; attempt access still verifies exact scope. See
  ADR-0151 before changing credential ownership or renewal.

---

## 5. Core Execution Flows

### 5.1 Native SQL and Connector Path

1. A MySQL client connects to a `role=fe` process through the native SQL
   entrypoint.
2. The frontend owns session admission, catalog resolution, planning, and
   coordinator lifecycle assembly.
3. Persistent tables belong to external providers. Iceberg owns its catalog and
   mutation truth; Paimon owns its external snapshots and exposes only its
   supported read capability.
4. The frontend freezes native fragment and Connector facts, then sends them to
   one or more `role=be` processes through native gRPC.
5. BE hosts bind installed Connector execution instances and run Arrow batches
   through the pipeline/runtime stack.

### 5.2 Exchange Path

1. Sender-side operators encode chunks into
   `novarocks/execution/src/runtime/fragment/io/exchange_queue.rs`, which owns the
   outbound queue and backpressure, and transmit through
   `novarocks/native-adapter/src/exchange_transmitter.rs`.
2. An exchange edge starts Closed and opens only after every frozen destination
   has an Installed fact or has normally withdrawn its exact input demand
   (`novarocks/execution/src/runtime/fragment/io/exchange_edge.rs`), so no frame
   can precede the receiver that counts it.
3. Receiver side (`novarocks/native-adapter/src/exchange_data_plane.rs`) projects
   the wire payload and pushes into
   `novarocks/execution/src/runtime/fragment/io/exchange_receiver.rs` and the
   registry in `novarocks/execution/src/runtime/exchange.rs`.
4. The exchange source operator
   (`novarocks/execution/src/exec/operators/exchange_source.rs`) blocks until all
   senders reach EOS.
5. On cancellation, `exchange::cancel_fragment` / `cancel_exchange_key` clear
   exchange keys and wake blocked waiters.

### 5.3 Native Distributed Task Execution Path

1. `novarocks/frontend-application/src/query_execution/native_execution_adapter.rs` freezes a
   `QueryExecutionId` and, from one live backend snapshot, a task graph:
   `novarocks/frontend-application/src/task_execution/graph.rs` mints a `TaskIdentity`
   ({QueryExecutionId, StageId, TaskId, BackendProcessId}) per placement and a
   `QueryContextRef` ({QueryExecutionId, FrontendProcessId, BackendProcessId})
   per participating backend. A context exists only where the scheduler placed
   a task, so the number of participants is a property of the plan and the
   splits, never of the cluster size.
2. `execute_round_on_task_protocol` establishes every context and creates every
   task through a bounded, fair dispatcher
   (`novarocks/frontend-application/src/task_execution/{round,dispatch,context_owner}.rs`).
   Each operation gets one verdict per domain -- Apply / Idempotent / Older /
   Conflict -- and a domain never rolls back, which is why replaying the exact
   request is the prescribed recovery for an unknown outcome and a conflicting
   answer is fatal. `Accepted` proves Worker ownership before expensive
   preparation; `Installed` separately proves that local runtime installation
   completed. FE retains a per-backend deployment window W across Accepted or
   unknown RPC outcomes until Installed, terminal evidence or permanent
   stand-down. W must not exceed that exact backend descriptor's advertised
   per-context preparation positions P.
3. Push exchange edges start Closed and open once every frozen destination has
   an Installed fact or has normally withdrawn its exact input demand
   (`novarocks/execution/src/runtime/fragment/io/exchange_edge.rs`), so no
   frame can precede the receiver that counts it.
4. `novarocks/worker/src/task_registry.rs` owns the BE-local
   context and task state, exact admission, lease renewal and expiry, bounded
   tombstones, and one termination latch per task. The latch is first-wins with
   a single exception: a derived cause (`Aborted(PeerTaskFailed)`) is a
   placeholder that an originating cause (`Failed(TaskFailure)`) replaces
   exactly once. Accepted jobs enter a bounded fair FIFO; count, byte and
   per-context position charges remain until the preparation job actually exits,
   including after Installed or terminal publication. See
   `novarocks/worker/src/{task_registry.rs,task_registry_config.rs}`.
   Native status observation uses covered subscriptions with a mandatory
   nonzero generation (`novarocks/native-adapter/src/task_protocol.rs`);
   cursor-only generation-zero requests are refused.
5. Runtime-filter contributions and an operator's own counters ride
   `ReleaseQueryContextAck`: release is the backend's own statement that every
   local task is a terminal record, and the Frontend Application drives it for every
   intent. The task protocol mints no participant proof or attestation -- a
   task's terminal *is* its own status.
6. Client cancellation is `KILL QUERY` through the Query Application query-control
   owner, delivered as `AbortQueryContext`. The Frontend Application withholds the
   interrupt until its coordinator worker unwinds, so the next statement on
   that connection cannot race the statement generation.

## 6. Core Data Structures (Current Implementation)

- `Chunk`: `novarocks/execution/src/exec/chunk/chunk_impl.rs`
  Arrow `RecordBatch` wrapper with `slot_id -> column_index` mapping and memory accounting.

- `ExecPlan` / `ExecNode` / `ExecNodeKind`: `novarocks/execution/src/exec/node/mod.rs`
  Temporary construction plan tree; production driver preparation consumes
  `LocalProgram` plus per-task `LocalRuntimeBindings`.

- `ExprArena` / `ExprNode`: `novarocks/execution/src/exec/expr/mod.rs`
  Arena-based expression graph model.

- `RuntimeState`: `novarocks/execution/src/runtime/runtime_state.rs`
  Runtime context for cache, spill, and runtime filter behavior.

- `ExchangeKey`: `novarocks/execution/src/runtime/exchange.rs`
  Exchange routing key (`finst_id_hi` + `finst_id_lo` + `node_id`).

- `QueryResult`: `novarocks/query-application/src/api/result.rs`
  Query-application result type consumed by the MySQL adapter's result encoding.

- `QueryExecutionId`: `novarocks/types/src/identity.rs`
  Immutable native query-attempt identity, shared across the process boundary.

- `TaskIdentity` / `QueryContextRef`:
  `novarocks/execution-contract/src/identity.rs`
  The indivisible {QueryExecutionId, StageId, TaskId, BackendProcessId} a task
  is addressed by, and the {QueryExecutionId, FrontendProcessId,
  BackendProcessId} a backend holds on an attempt's behalf. Any component
  mismatching is fatal; neither is ever partially matched.

- `TaskExecutionRegistry`:
  `novarocks/worker/src/task_registry.rs`
  BE-owned context and task state, exact admission, per-domain progression,
  lease renewal and expiry, bounded tombstones, and one termination latch per
  task.

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

- `[native_trust]`
  Required in each deployable FE/BE config: a common `deployment_id` and
  Server-resolved `shared_secret`. Every Native RPC requires its deployment
  JWT. An absent `[native_trust.transport]` deliberately means authenticated
  plaintext h2c; `automatic` and `pem` add optional TLS 1.3/h2 without
  removing JWT. This does not protect MySQL or management HTTP. Read
  `docs/guides/deployment/native-trust.md` and ADR-0110 before changing this
  boundary; NWT-4 protocol slimming remains separate follow-up work.

- `[runtime]`
  `exchange_wait_ms`, `exchange_io_threads`, `exchange_io_max_inflight_bytes`,
  `pipeline_scan_thread_pool_thread_num`, `pipeline_exec_thread_pool_thread_num`, `cache.*`

- `[iceberg]`
  Embedded-JVM toggle for Iceberg metadata-table and remote metadata planning.

- `[standalone_server]`
  `mysql_port`, `user`, MV scheduler settings, and Iceberg maintenance settings.

- `[connector.object_store]`
  Process-local object-store credentials for Iceberg and Paimon connector
  execution; this does not define a native internal table store.

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
- `NOVAROCKS_FE_CONFIG`, `NOVAROCKS_BE_CONFIG`
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
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When backgrounding the server (e.g. inside an automated test driver), wait
for the readiness marker before issuing the first query — probing the mysql
port alone cannot distinguish a freshly-bound server from a leftover
process that already owned the port:

```bash
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG" >"$LOG" 2>&1 &
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
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Run cross-engine Iceberg compatibility tests where Spark writes through REST
Catalog + MinIO and NovaRocks reads the table:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Run NovaRocks-only Iceberg REST end-to-end smoke (no Spark, NovaRocks both
writes and reads):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
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
which removes only this worktree's generated runtime entry and private object
prefixes. It deliberately preserves `s3://novarocks/shared/benchmarks/` and
leaves the shared Docker services running because other worktrees may be using
them. Standard SSB/TPC-H/TPC-DS data is an immutable READY-published fixture:
the runner resolves it before suite hooks, and only the bootstrap's exact-key
Docker lease may build it. Use `docker/iceberg-rest/down.sh --docker` only when
you explicitly want to stop the shared Docker project. `--docker --volumes`
always rejects the canonical project; it requires an exact task-created project
and volume confirmation for an isolated reset.

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

The unified runner is under `tests/sql/runner`, with correctness suites under `tests/sql/correctness`
and benchmark workloads under `tests/sql/benchmarks`.
It requires a running NovaRocks
MySQL-compatible standalone server. Do not assume a fixed port in Codex
workspaces; source `docker/iceberg-rest/runtime/current/env.sh` when that
entry exists.

**Start standalone (no external FE needed):**

The two role configs are yours to create: copy `novarocks-fe.toml.example` and
`novarocks-be.toml.example`, or use the generated pair described below.

```bash
# Debug: fast compile, slow query (for fix verification)
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml

# Release: slow compile, fast query (for suite testing)
NO_PROXY=127.0.0.1,localhost cargo run --release -p novarocks-server -- standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml
```

When the local test environment is active:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When starting a server manually inside a Codex worktree, prefer the generated
config so its configured MySQL port cannot collide with another worktree.

**Run test suites:**

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --suite <suite> --mode <verify|record|diff> [--query-timeout 60] [-j 4]
```

With a generated runner config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Suites are discovered from `tests/sql/correctness/`; ask the runner for the
current list with `--list-suites`. `tests/sql/correctness/README.md` carries the
suite map — which engine area each suite covers, its typical change entry, and
whether it needs the REST Catalog fixture or a cross-process topology — and is
the reference for choosing suites. `ssb`, `tpc-h` and `tpc-ds` are benchmark
workloads under `tests/sql/benchmarks/` and belong to the benchmark runner, not
to correctness CI. Cluster mode and backend count come from runner CLI; no suite
owns an alternate server runtime.

**Run specific cases:**

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --suite join --only join_cross_join_small,join_array_type --mode verify
```

### 8.5 Test Scope Selection

Run the tests a change can actually break. Targeted verification is the default;
a full run is a deliberate step with a stated reason, not a safety reflex. Full
runs belong at milestone convergence, at the final verification before claiming
completion, and where the blast radius genuinely is the whole repository.

**Rust: choose `-p` from the crate you edited.** Every first-party crate lives
in the single root workspace, so `cargo test -p <crate>` always works; the
packages under `vendor/` own separate workspaces and locks and are out of scope
unless you edited them.

| Change lands in | Run at least |
|---|---|
| `novarocks/sql/**` (parser, analyzer, optimizer, planner, function registry) | `-p novarocks-sql` |
| `novarocks/execution/**` (operators, expressions, pipeline, exchange, runtime filter) | `-p novarocks-execution` |
| `novarocks/frontend-application/**`, `novarocks/query-application/**`, `novarocks/catalog-application/**` | that crate, plus `-p novarocks-worker -p novarocks-native-adapter` when the change crosses the FE/BE boundary |
| `novarocks/worker/**`, `novarocks/native-adapter/**` | `-p novarocks-worker -p novarocks-native-adapter` |
| `novarocks/connector/<name>/**` | that connector crate, plus `-p novarocks-fs` when authorized object-store access changes |
| `novarocks/state-store/**` | the touched backend crate plus `-p novarocks-state-store-api` |
| `novarocks/types/**`, `novarocks/spi/**`, `novarocks/execution-contract/**`, `novarocks/*-codec/**`, `novarocks/proto-models/**` | the whole workspace: a shared vocabulary or wire format has no bounded blast radius |

**SQL: choose suites from the suite map** in
`tests/sql/correctness/README.md`, and narrow further with `--only <case>` when
a single case covers the change. A change to array function declarations needs
`complex-type` and `function`, not the corpus; outer-join nullability needs
`join`; CUBE needs `aggregate`.

**What a full run costs** (measured 2026-09-15; re-check rather than quote if it
matters): `cargo test --workspace` builds and runs about 119 test binaries
across 50 packages and roughly ten thousand cases, minutes per round even when
little changed. `--suite all` selects 33 suites / 776 cases; one runner
invocation owns one server lifecycle, so running suites one at a time pays a
server start per suite and a full pass costs tens of minutes.

**A saturated machine produces false failures.** These are load-sensitive, not
regressions:

- `novarocks-fs`: `provider_pool_evicts_vended_provider_at_credential_expiration`
  (`novarocks/fs/src/access.rs`) turns on a credential-expiration time constant.
- `novarocks-test-support`: the `managed_process` family
  (`tests/test-support/src/managed_process.rs`) turns on process readiness and
  signal timing.

The criterion is the combination — a time constant in the test, concurrent load
while it ran, and a clean pass when run alone — not the crate name. Neither is
in `tools/ci/baselines/known-failures.toml`, which covers accepted SQL failures
only.

For SQL suites, read the first failure rather than the failure count: one
failing statement can leave the shared server in a state where later cases fail
for reasons of their own, so the count overstates the problem. Re-run a
suspected case against a clean server before attributing it to the change.

## 9. Suggested Starting Points for Typical Changes

- **SQL / parser / planner changes**: start with `novarocks/parser/src/**`,
  `novarocks/sql/src/{analysis,analyzer,semantic}/**`,
  `novarocks/sql/src/optimizer/**` and `novarocks/sql/src/planner/**`.
- **Fragment decode changes**: start with
  `novarocks/native-adapter/src/fragment_plan_node.rs` and the relevant
  `fragment_plan_decode/**` or `fragment_expression/**` submodule; program/binding
  separation is in `fragment_plan_decode_submission.rs` and Execution
  `exec/node/lowering.rs`. The wire vocabulary lives in `novarocks/plan-codec/src/**`.
- **MySQL protocol behavior**: inspect `novarocks/mysql-adapter/src/listener.rs`,
  `result_encoding.rs` and `row_encoding.rs`.
- **DDL/DML behavior**: inspect `novarocks/query-application/src/sql/**` for
  admission and routing, then the specific flow under
  `novarocks/frontend-application/src/query_execution/dml/**` (`insert.rs`,
  `delete/**`, `mutation_flow.rs`, `truncate.rs`, `add_files.rs`,
  `iceberg_writer.rs`); catalog/table DDL is in
  `novarocks/frontend-application/src/catalog_application/statement.rs`.
- **Execution semantics/operator behavior**: inspect
  `novarocks/execution/src/exec/node/**` and
  `novarocks/execution/src/exec/operators/**`.
- **Scheduling/parallelism**: inspect `novarocks/execution/src/exec/pipeline/**`.
- **Exchange behavior**: inspect `novarocks/execution/src/runtime/exchange.rs`,
  `novarocks/execution/src/runtime/fragment/io/**`,
  `novarocks/execution/src/exec/operators/exchange_source.rs`, and
  `novarocks/native-adapter/src/{exchange_data_plane,exchange_transmitter,fragment_exchange_receiver}.rs`.
- **Materialized views**: inspect `novarocks/mv-application/src/**` and
  `novarocks/frontend-application/src/mv/**`; MV query rewrite is an optimizer
  concern under `novarocks/sql/src/optimizer/**`.
- **Native distributed task execution**: inspect
  `novarocks/frontend-application/src/query_execution/native_execution_adapter.rs`,
  `novarocks/frontend-application/src/task_execution/**`,
  `novarocks/worker/src/{task_registry,ingress,convergence}.rs`,
  `novarocks/native-adapter/src/{backend_application,task_protocol_ingress}.rs`, and
  `novarocks/execution/src/task_execution/**`. Preserve the Installed/normal-close
  gated edge-open barrier and the per-domain Apply / Idempotent / Older /
  Conflict verdict; do not add a protocol shim, a standalone direct-call path,
  or a no-runtime-filter retry inside an attempt. See ADR-0146.
- **Connector behavior**: inspect `novarocks/connector/**` and
  `novarocks/fs/**`. The active sealed providers are Iceberg and Paimon;
  StarRocks is retired and must not be restored through a local-binding config
  or runtime fallback.
- **FE/BE interface behavior**: inspect `novarocks/frontend-application/src/**`,
  `novarocks/query-application/src/**`, `novarocks/worker/src/**`,
  `novarocks/native-adapter/src/**`, and the neutral contracts under `novarocks/spi/**`
  (Connector) and `novarocks/state-store/api/**` (StateStore). Those two
  provider domains are separate compilation units: the StateStore contract does
  not depend on Arrow or on the Connector contract, and its shared test
  mechanics live in `novarocks/state-store/testkit/**`, which no production
  crate may depend on. See `docs/guides/development/state-store-boundary.md`.
- **Optimizer observability / plan-shape regression**: see
  `novarocks/sql/src/explain/**` for the EXPLAIN formatter (Normal/Verbose/Costs/
  Analyze). `EXPLAIN ANALYZE` returns a query-level Planning/Execution/
  Rows header above the Verbose plan; per-operator runtime stats are a
  follow-up. Verbose/Costs/Analyze append a stable `stats={rows=N}`
  trailer to each physical node. `SET disable_optimizer_rules = 'RuleA,RuleB'`
  (alias `cbo_disabled_rules`) bisects optimizer rules at session level;
  see `novarocks/sql/src/optimizer/options.rs`. Use the `tests/sql/correctness/optimizer/` suite
  for plan-golden cases, and `-- @explain_contains=<substr>` /
  `-- @normalize_explain_timing` in any sql-test case to assert plan-shape
  facts alongside the result golden.
- **Aggregate pushdown rule (OPT-1)**: see
  `novarocks/sql/src/optimizer/rewrite/rules/aggregate_pushdown/`. Pushes
  `LogicalAggregate` past inner/outer joins toward leaves when NDV
  bucketing predicts a real row-count reduction. White-list functions
  are SUM/MIN/MAX/COUNT(col). Disable via
  `SET disable_optimizer_rules = 'AggregatePushdown'`. Plan-shape
  cases live under `tests/sql/correctness/optimizer/sql/aggregate_pushdown_*.sql`.
  The idempotency guard is `LogicalAggregateNode::already_pushed`
  (`novarocks/sql/src/planner/logical/node.rs`) — other rules must preserve the
  flag when cloning.

---

## 10. StarRocks Reference Code Location

For StarRocks side-by-side reference implementation, use: `~/project/starrocks`

---

## 11. Architecture Decision Records (ADR)

Durable design decisions, philosophies, and their honestly-recorded trade-offs live in
`docs/adr/` (index and authoring rules: `docs/adr/README.md`).
Before changing architecture-level behavior, check the index for the affected domain.
Any PR that embodies a new design decision or accepts a compromise must add or
supersede an ADR — use `$ops-capture` from the `workbench` plugin
(`.agents/skills/workbench/`, exposed directly to Codex and to Claude Code via the
`.claude/skills` → `.agents/skills` symlink). Its contract embeds the template,
numbering, supersede, and collision-renumbering rules. `docs/adr/README.md` remains
authoritative for this repository: where it and the skill contract disagree, the
README wins.

Before writing one, apply the scope test: an ADR records a **long-lived design in
the code**, not one fix's ruling. If the entry would lose its value once the
incident that prompted it is forgotten, it belongs in the knowledge base as a
case, not here. Case narrative — field quotes, logs, the investigation path,
wrong turns, which fix was chosen — stays out of the ADR body; the ADR reaches it
through `provenance` and `related` only.

---

## 12. Project Development Workflow and Knowledge Base

For NovaRocks feature, architecture, roadmap, and refactor work, resolve the
project documentation root from memory. Use the newest directly applicable,
existing path; when memory has no usable project documentation path, use
`<repo-root>/docs/workbench`.

The documentation root holds two side-by-side halves: `workflow/` for staged
development artifacts (spec, plan, umbrella, archive) and `ops/` for the durable
engineering knowledge base (scenarios, cases, and — for projects without an
in-repo ADR directory — ADRs). Resolve the root itself, never either half; if a
memory entry points straight at `.../workflow`, take its parent.

Use the generic skills-only plugin under `.agents/skills/workbench/`:

- `workbench`: identify and route the current stage or knowledge operation;
- `dev-workflow-explain-technical-concept`: explain technical concepts and
  current mechanisms in Chinese from first principles, with one concrete
  running example and explicit concept/design/implementation/impact layers;
- `dev-workflow-discuss-design`: settle the problem and major design decisions;
- `dev-workflow-write-spec`: write the accepted design into project docs;
- `dev-workflow-plan`: write and iterate the plan directly in project docs,
  then mark the persisted version approved after explicit user approval;
- `dev-workflow-execute`: create a goal and execute continuously through
  verification;
- `dev-workflow-finish`: publish and archive only when authorized;
- `ops-capture`: record a reproducible scenario, a first-hand debugging case, or
  an ADR, with its symptoms written both the way the field describes them and the
  way they appear in logs; for an ADR it first applies a scope test that keeps
  case narrative out of the record and makes the ruling produce named, reusable
  rules;
- `ops-lookup`: check our own records for whether a symptom has been hit before,
  and hand off to external-source search when it has not.

Codex discovers the project-local skill sources directly under `.agents/skills/`.

Knowledge operations are a side route available from every stage. `ops-lookup` is
read-only; `ops-capture` writes only knowledge entries. Neither changes spec, plan,
goal, or publication state.

Technical explanation is a read-only side route available from every stage. It
does not accept a design, approve a plan, authorize implementation, or modify
workflow state. Deliver the requested explanation before proposing a spec,
plan, code change, commit, push, or PR.

Do not skip the accepted-design and approved-plan gates. Sub-agents are allowed
in every stage when they provide useful parallel investigation, independent
verification, isolated implementation, or risk review. The main agent retains
ownership and verifies their results.

The bundle carries two contracts under
`.agents/skills/workbench/skills/workbench/references/`: `workflow-contract.md` is
the only development-workflow contract, and `ops-contract.md` is the only
knowledge-base contract. Do not depend on an external workflow document. The plan stage runs in the current editable mode and persists its
draft immediately; do not require Codex Plan mode. Produce a task DAG with hard
dependencies, parallel waves, non-overlapping file ownership, sub-agent
scheduling labels, independent validation, integration gates, and local commit
checkpoints. Explicit user approval promotes the persisted plan from `draft` to
`approved` before execution.

Plan and execute both resolve test scope through section 8.5: a plan task names
the tests its own change can break, and execution runs those, not the whole
repository. A full run needs one of the reasons section 8.5 lists.

Once execution starts, routine implementation difficulties are not reasons to
stop; pause only for the major decision conditions defined by
`dev-workflow-execute`. On a task-local development branch, checkpoint commits
are allowed after a coherent plan section completes or before risky changes.
The execute stage must never push or open a PR; those actions require explicit
authorization in `dev-workflow-finish`.
