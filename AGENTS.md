# NovaRocks - AI Agents Guide

This document is a quick operational index for agents working on NovaRocks.  
It is designed to help you quickly:
- locate the right code paths
- understand the current execution architecture
- implement changes without semantic drift

This guide focuses on high-frequency implementation details and modification entry points.

---

## 1. Project Overview

NovaRocks is a **Rust-based, cloud-native, compute-storage decoupling friendly** implementation of a StarRocks compute node.

Current shape:
- FE-compatible protocol behavior (zero FE awareness changes)
- C++ Shim handles brpc access and protocol bridging
- Rust handles plan lowering, pipeline execution, exchange, and connectors
- Columnar processing is centered on Arrow `RecordBatch` / `Chunk`

---

## 2. Architecture Overview (Current Code)

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

---

## 3. Non-Negotiable Rules (High Priority)

1. **Strictly follow FE-provided plan and type metadata**  
   No fallback behavior, no guessed defaults, no implicit type downgrade.

2. **Fail fast on unsupported or ambiguous semantics**  
   Return explicit errors in parsing/lowering stages instead of "best effort" execution.

3. **Keep protocol and execution responsibilities separated**  
   C++ Shim is the protocol gateway; execution semantics belong to Rust.

4. **Language policy**  
   - User interaction and design docs: Chinese  
   - Code comments, logs, error messages, commit messages: English

---

## 4. Key Code Index (Validated Against Current Repository)

### 4.1 Entrypoints and Services

- `src/main.rs`  
  Process entry; starts heartbeat, backend thrift, C++ compat, and gRPC server.

- `src/service/internal_service.rs`  
  Query execution entrypoints: `submit_exec_plan_fragment`, `submit_exec_batch_plan_fragments`, `cancel`.

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

### 4.2 Plan Lowering

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

### 4.3 Execution Plan and Operators

- `src/exec/node/mod.rs`  
  `ExecNode`, `ExecNodeKind`, `ExecPlan` definitions.

- `src/exec/expr/mod.rs`  
  `ExprArena` and `ExprNode` execution-layer structures.

- `src/exec/operators/mod.rs`  
  Operator factory registration; concrete operators are under `src/exec/operators/**`.

- `src/exec/chunk/mod.rs`  
  `Chunk` (Arrow `RecordBatch` wrapper) and slot metadata mapping.

### 4.4 Pipeline Execution Framework

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

### 4.5 Exchange and Runtime

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

### 4.6 Connectors / Formats / Filesystem

- `src/connector/mod.rs`  
  `ConnectorRegistry` and connector abstractions.

- `src/connector/jdbc.rs`  
  JDBC/MySQL scan connector.

- `src/connector/hdfs.rs`  
  HDFS/Iceberg/Parquet-style scan connector.

- `src/connector/starrocks.rs`  
  StarRocks connector.

- `src/connector/lake_data.rs`  
  Lake-data related write and transaction interfaces.

- `src/formats/**`  
  Parquet/ORC/StarRocks format readers.

- `src/fs/**`  
  Local and opendal/OSS filesystem abstractions.

### 4.7 C++ Shim

- `src/shim/brpc_server.cpp`
  brpc service entry (protocol gateway).

- `src/shim/compat.cpp`
  C++ compat layer implementation.

- `src/shim/compat.h`
  Rust/C++ FFI ABI contract.

---

## 5. Core Execution Flows

### 5.1 Main Query Path

1. FE sends requests to C++ Shim through brpc.  
2. C++ forwards thrift binary attachments into Rust FFI (`engine_ffi.rs`).  
3. `internal_service.rs` deserializes payloads and organizes fragment execution.  
4. `lower/` transforms thrift plan/expr into `ExecPlan` + `ExprArena`.  
5. `exec/pipeline/executor.rs` builds and schedules pipeline drivers.  
6. Results are written into `result_buffer` or sent downstream via exchange sink.  
7. FE fetches results through the fetch path (FFI fetch -> `result_buffer`).

### 5.2 Exchange Path

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

---

## 7. Configuration and Runtime

### 7.1 Config File

- Default config file: `./novarocks.toml`
- Environment override: `NOVAROCKS_CONFIG=/path/to/file.toml`
- CLI override: `--config <path>`

### 7.2 Common Config Sections

- `[server]`  
  `host`, `heartbeat_port`, `be_port`, `brpc_port`, `http_port`

- `[runtime]`  
  `exchange_wait_ms`, `exchange_io_threads`, `exchange_io_max_inflight_bytes`,  
  `pipeline_scan_thread_pool_thread_num`, `pipeline_exec_thread_pool_thread_num`, `cache.*`

- `[debug]`  
  `exec_node_output`, `exec_batch_plan_json`

- `[spill]`  
  Spill enablement, directories, block size, and compression strategy

---

## 8. Development and Testing Standards

### 8.1 Language Standard

- User communication and design docs: Chinese
- Code comments/logs/errors/commit messages: English

### 8.2 Code Quality

- `cargo fmt`
- `cargo clippy`
- `cargo build`
- `cargo test`

### 8.3 SQL Regression Tests

Unified runner under `sql-tests`:

```bash
cargo run --manifest-path sql-tests/Cargo.toml --bin sql-tests -- --suite <suite> --mode <verify|record|diff>
```

Example:

```bash
cargo run --manifest-path sql-tests/Cargo.toml --bin sql-tests -- --suite write-path --mode verify
```

---

## 9. Suggested Starting Points for Typical Changes

- **Plan lowering changes**: start with `src/lower/node/mod.rs` and the relevant node submodules.
- **Execution semantics/operator behavior**: inspect `src/exec/node/*` and `src/exec/operators/*`.
- **Scheduling/parallelism**: inspect `src/exec/pipeline/*`.
- **Exchange behavior**: inspect `src/runtime/exchange.rs`, `src/runtime/exchange_scan.rs`, `src/service/grpc_*.rs`.
- **Connector behavior**: inspect `src/connector/*` and `src/formats/*`.
- **FE/BE interface behavior**: inspect `src/service/internal_service.rs`, `src/service/backend_service.rs`, `src/service/engine_ffi.rs`, `src/shim/compat.h`.

---

## 10. StarRocks Reference Code Location

For StarRocks side-by-side reference implementation, use: `~/worktree/starrocks/main`
