# Standalone Multi-Fragment Exchange Hang

**Date**: 2026-04-10
**Status**: Root Cause Confirmed — Fixes In Progress
**Severity**: High - blocks multi-fragment queries in standalone mode (TPC-DS q13, q18, q69, q77, etc.)

## Problem

Multi-fragment queries executed via the standalone coordinator hang indefinitely. Background fragment pipelines never complete, and exchange data never reaches consuming fragments.

## Root Cause Analysis

Investigation revealed **three independent issues** that compound to cause the hang. The original hypothesis ("shared tokio runtime deadlock") was incorrect — the gRPC server creates its own dedicated runtime (4 worker threads at `grpc_server.rs:574`), completely separate from `data_runtime()`.

### Issue 1 (Primary): Runtime filter probe specs on exchange source nodes

`push_down_local_runtime_filters()` (`exec/node/mod.rs`) and `lower/node/mod.rs` lowering both attach `RuntimeFilterProbeSpec` to exchange source nodes. This creates a `RuntimeFilterProbe` dependency in the exchange source operator's `precondition_dependency()` check (`exec/operators/exchange_source.rs:437`).

In multi-fragment coordinated execution, the RF producer is in a different fragment. The exchange source's RF dependency calls `dependency_or_timeout()` which blocks the driver until either:
- The RF arrives from the other fragment (via gRPC transmit_runtime_filter), or
- The timeout expires (typically 1 second)

**Impact**: Each exchange source with pushed RF specs blocks for up to 1 second before consuming data. In queries with many exchange sources and RFs, this compounds into multi-second delays during which producers are already running.

Combined with Issue 2, the timing creates conditions where background fragments finish and exit before exchange sources start consuming, leading to the hang.

### Issue 2 (Contributing): MultiCastDataStreamSinkOperator EOS flush race

`MultiCastDataStreamSinkOperator::set_finishing()` (`exec/operators/multi_cast_data_stream_sink.rs:283-299`) sets `self.finished = true` **immediately** after calling inner sinks' `set_finishing()`, without waiting for the EOS gRPC sends to complete.

Compare with `DataStreamSinkOperator::set_finishing()` which calls `maybe_mark_finished()` — this checks `send_tracker.is_idle()` and only sets `finished` when all inflight sends complete. Additionally, `DataStreamSinkOperator::need_input()` returns `false` during finishing until sends complete, causing the driver to block on `OutputFull` with a `sink_observable` wakeup.

The MultiCast operator skips all of this. The driver sees `is_finished() = true` immediately, completes the pipeline, and the fragment thread exits. EOS tasks are still queued in the io_executor and will be sent eventually, but there's a window where the receiver hasn't seen EOS yet.

### Issue 3 (Correctness): Coordinator uses MultiCast for single-consumer stream producers

The standalone coordinator wrapped every stream producer fragment's sink in `TMultiCastDataStreamSink`, even when there's only one consumer. StarRocks FE uses plain `TDataStreamSink` for single-consumer fragments.

This unnecessarily triggers the MultiCast EOS flush race (Issue 2) and adds complexity.

### Issue 4 (Correctness): per_fragment_exch_num_senders counting bug

The old code used `BTreeMap::insert(exchange_node_id, 1)` which always sets the sender count to 1, regardless of how many edges point to the same exchange node. This could cause incorrect `expected_senders` in edge cases with multiple producers targeting the same exchange node.

### How the issues compound (hang mechanism)

1. Background fragments start executing pipeline → produce data → exchange sink submits gRPC sends to io_executor
2. Pipeline finishes → MultiCast `set_finishing` → inner sinks submit EOS → **MultiCast immediately sets finished** → driver finishes → fragment thread returns
3. EOS tasks still in io_executor queue, being processed asynchronously
4. Root fragment starts consuming → exchange source checks `precondition_dependency()` → **RF probe blocks for 1+ seconds**
5. During the RF timeout window, EOS gRPC sends are completing in io_executor
6. When the RF timeout expires, exchange source starts consuming — at this point data may already be in the buffer
7. For queries with many fragments and RFs, the timing windows overlap such that some exchange sources never see their EOS (races with fragment cleanup, exchange cancel, or other timing effects)

### Why FE+BE mode works

In FE+BE (compat) mode:
- FE handles RF push-down differently (doesn't push to exchange nodes at the BE level)
- `run_send_task` uses `internal_rpc_client::send_chunks()` through C++ brpc — completely separate I/O path
- FE uses `LocalExchange` to split MultiCast into independent consumer pipelines with proper `pending_finish()` semantics

### Key code paths (verified)

| Path | File:Line | Role |
|------|-----------|------|
| Exchange send queue | `exchange_sender.rs:360` | `io_executor().submit(run_send_task)` |
| gRPC client send | `grpc_client.rs:103` | `data_block_on(async { grpc call })` |
| gRPC server exchange | `grpc_server.rs:78` | `tokio::spawn` on server's own runtime |
| Server chunk handler | `grpc_server.rs:112` | `spawn_blocking(handle_transmit_chunk)` |
| Push chunks to buffer | `exchange.rs:333` | Mutex + condvar notify (no runtime deps) |
| RF probe check | `exchange_source.rs:437` | `dependency_or_timeout()` with 1s fallback |
| Local RF dep check | `exchange_source.rs:231` | No timeout, blocks until ready |
| MultiCast finishing | `multi_cast_data_stream_sink.rs:297` | `self.finished = true` (immediate) |
| DataStreamSink finishing | `data_stream_sink.rs:1741` | `maybe_mark_finished()` waits for idle |

### Runtime architecture (corrected)

```
data_runtime()          gRPC server runtime         io_executor (threadpool)
  │ (shared)              │ (independent)              │
  │ N worker threads      │ 4 worker threads           │ M threads
  │                       │                            │
  │ ← HTTP/2 conn task    │ ← tonic server             │ ← run_send_task()
  │ ← opendal reads       │ ← exchange handler         │    └── data_block_on()
  │ ← RF transmissions    │ ← spawn_blocking(decode)   │         └── gRPC call
  │                       │                            │
  └── NO circular dep ────┘── separate I/O ────────────┘
```

## Fixes Applied (this branch)

| Fix | File | Status |
|-----|------|--------|
| Skip RF push-down to exchange source nodes | `src/exec/node/mod.rs` | ✅ Done |
| Skip RF attach to exchange node during lowering | `src/lower/node/mod.rs` | ✅ Done |
| Use plain `TDataStreamSink` for single-consumer stream producers | `src/standalone/coordinator.rs` | ✅ Done |
| Set `exec_params.destinations` for DataStreamSink path | `src/standalone/coordinator.rs` | ✅ Done |
| Fix `per_fragment_exch_num_senders` counting | `src/standalone/coordinator.rs` | ✅ Done |
| Remove debug eprintln statements | `src/standalone/coordinator.rs` | ✅ Done |

## Remaining Work

### Option A: Separate tokio runtime for exchange sender (deferred)

The original proposal to create a dedicated `exchange_sender_runtime()` is **no longer the primary fix**. The gRPC server already has its own runtime, and the `data_runtime()` contention hypothesis was not confirmed. However, it may still be valuable as a defense-in-depth measure for high-concurrency scenarios with heavy scan I/O. Deferred to a follow-up.

### MultiCast `pending_finish()` override (deferred)

Adding a proper `pending_finish()` to `MultiCastDataStreamSinkOperator` that waits for inner sinks' send trackers to be idle would be the correct long-term fix for Issue 2. Deferred because the coordinator now uses plain `TDataStreamSink` which already has the correct finishing behavior.

## Reproduction

```bash
# Standalone mode - previously hung on TPC-DS with Iceberg data
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030
# In another terminal:
mysql -P9030 -u root iceberg_cat.tpcds -e "$(cat sql-tests/tpc-ds/sql/q13.sql)"
```

## Diagnostic methodology

1. Added `[diag:]` eprintln logs to: `data_block_on`, `get_or_create_channel`, `send_chunks`, gRPC server exchange handler
2. Confirmed exchange gRPC path works end-to-end (ENTER → channel → RPC → server RECV → ack → EXIT)
3. Stashed working tree fixes, tested original code with simple CTE query → passes (issue requires complex TPC-DS + Iceberg conditions)
4. Traced `MultiCastDataStreamSinkOperator::set_finishing` → immediately sets `finished=true` without waiting for EOS delivery
5. Traced `DataStreamSinkOperator::need_input` → blocks driver until `send_tracker.is_idle()` during finishing
6. Confirmed exchange source `precondition_dependency()` checks RF probes with timeout, but adds latency
