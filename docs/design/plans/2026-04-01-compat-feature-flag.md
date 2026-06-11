# `compat` Feature Flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a `compat` Cargo feature flag so that `cargo build` (default) produces a pure-Rust standalone binary without STARROCKS_THIRDPARTY, while `cargo build --features compat` links the full C++ brpc shim for StarRocks BE compatibility.

**Architecture:** The `compat` feature gates three concerns: (1) C++ shim compilation and external library linkage in `build.rs`, (2) Rust modules that contain FFI declarations (`compat`, `engine_ffi`, `internal_rpc_client`), and (3) call sites that dispatch between brpc (compat) and gRPC (standalone) paths. The Rust thrift codegen and tonic gRPC codegen are always built — they are used by both modes.

**Tech Stack:** Cargo features, `#[cfg(feature = "compat")]`, conditional build.rs logic

---

## File Map

| File | Change | Responsibility |
|------|--------|----------------|
| `Cargo.toml` | Modify | Add `compat` feature |
| `src/build.rs` | Modify | Gate C++ compilation & linking behind `compat` |
| `src/service/mod.rs` | Modify | Gate module declarations |
| `src/service/exchange_sender.rs` | Modify | Replace runtime flag with compile-time cfg |
| `src/runtime/result_buffer.rs` | Modify | Gate FFI call behind `compat` |
| `src/exec/operators/fetch_processor.rs` | Modify | Re-route proto type imports, gate brpc lookup |
| `src/service/internal_rpc.rs` | Modify | Gate test-only brpc references |
| `src/runtime/runtime_filter_worker.rs` | Modify | Gate brpc runtime filter send |
| `src/main.rs` | Modify | Gate BE-mode compat startup |
| `src/standalone/server.rs` | Modify | Remove runtime `enable_grpc_exchange()` call |

---

### Task 1: Add `compat` feature to Cargo.toml

**Files:**
- Modify: `Cargo.toml:82-84`

- [ ] **Step 1: Add the feature flag**

In `Cargo.toml`, add `compat` to the `[features]` section:

```toml
[features]
ssb = []
compat = []
embedded-jvm = ["dep:jni"]
```

- [ ] **Step 2: Verify default build still works**

Run: `cargo check 2>&1 | tail -3`
Expected: Compiles (will fail at link because build.rs still unconditionally links C++ — that's Task 2).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "feat: add compat feature flag for C++ shim compilation"
```

---

### Task 2: Gate C++ compilation in build.rs

**Files:**
- Modify: `src/build.rs:269-591`

The build.rs currently:
1. Always resolves STARROCKS_THIRDPARTY (line 325, panics if missing)
2. Always runs C++ thrift codegen (lines 352-366)
3. Always runs C++ protobuf codegen (lines 410-436)
4. Always compiles C++ shim via cc::Build (lines 438-493)
5. Always links brpc/thrift/glog/etc (lines 496-567)

When `compat` is disabled, we skip steps 1-5 but KEEP:
- Rust thrift codegen (lines 368-408) — uses `thrift` tool from PATH
- Rust gRPC codegen via tonic_build (lines 569-591) — uses vendored protoc

- [ ] **Step 1: Add compat feature detection function**

After the existing `embedded_jvm_feature_enabled()` function (line 243), add:

```rust
fn compat_feature_enabled() -> bool {
    env::var_os("CARGO_FEATURE_COMPAT").is_some()
}
```

Also add to the rerun-if-env-changed section (after line 310):

```rust
println!("cargo:rerun-if-env-changed=CARGO_FEATURE_COMPAT");
```

- [ ] **Step 2: Gate thirdparty resolution**

Replace lines 325-341 (the `resolve_thirdparty_root` block and tp_bin/tp_include/tp_lib/tp_lib64 bindings) with:

```rust
let compat = compat_feature_enabled();

// Thirdparty is only required for compat (C++ shim) builds.
let (tp_bin, tp_include, tp_lib, tp_lib64) = if compat {
    let project_thirdparty_root = resolve_thirdparty_root(&manifest_dir).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        eprintln!("  To build default thirdparty, run: ./thirdparty/build-thirdparty.sh");
        panic!("thirdparty root not found");
    });
    let project_thirdparty = resolve_thirdparty_install_root(&project_thirdparty_root)
        .unwrap_or_else(|e| {
            eprintln!("Error: {e}");
            eprintln!("  To build default thirdparty, run: ./thirdparty/build-thirdparty.sh");
            panic!("thirdparty artifacts not found");
        });
    (
        project_thirdparty.join("bin"),
        project_thirdparty.join("include"),
        project_thirdparty.join("lib"),
        project_thirdparty.join("lib64"),
    )
} else {
    // Dummy paths — not used when compat is disabled.
    let dummy = std::path::PathBuf::from("/dev/null");
    (dummy.clone(), dummy.clone(), dummy.clone(), dummy)
};
```

- [ ] **Step 3: Gate C++ thrift codegen**

Wrap the C++ thrift generation loop (lines 352-366) with:

```rust
if compat {
    let thrift_cmd = find_tool("thrift", &tp_bin);
    for thrift_file in [
        "idl/thrift/HeartbeatService.thrift",
        "idl/thrift/InternalService.thrift",
    ] {
        // ... existing codegen code ...
    }
}
```

- [ ] **Step 4: Gate Rust thrift codegen to use PATH thrift when no thirdparty**

The Rust thrift codegen (lines 368-408) needs to find `thrift` in PATH when compat is disabled. Currently it uses `find_tool("thrift", &tp_bin)`. When compat is off, `tp_bin` is a dummy. Change lines 368 onward to:

```rust
// Rust thrift codegen is always needed (shared types for both modes).
// When compat is disabled, thrift must be in PATH.
let thrift_rs_cmd = if compat {
    find_tool("thrift", &tp_bin)
} else {
    find_tool("thrift", &std::path::PathBuf::from("/dev/null"))
};
for thrift_file in [
    "idl/thrift/InternalService.thrift",
    // ... same list as before ...
] {
    let thrift_rs_status = std::process::Command::new(&thrift_rs_cmd)
        // ... rest unchanged ...
```

- [ ] **Step 5: Gate C++ protobuf codegen**

Wrap lines 410-436 (protobuf C++ generation) with:

```rust
if compat {
    let protoc_cmd = find_tool("protoc", &tp_bin);
    // ... existing protoc code ...
}
```

- [ ] **Step 6: Gate C++ compilation (cc::Build) and library linkage**

Wrap lines 438-567 (cc::Build through all `println!("cargo:rustc-link-*")` directives) with:

```rust
if compat {
    let mut build = cc::Build::new();
    // ... existing cc::Build code (lines 438-493) ...

    // ... existing link-search and link-lib directives (lines 495-567) ...
}
```

- [ ] **Step 7: Verify compat=off builds without thirdparty**

Unset STARROCKS_THIRDPARTY and build:

Run: `unset STARROCKS_THIRDPARTY && cargo build 2>&1 | tail -3`
Expected: Build succeeds (no C++ linking needed)

- [ ] **Step 8: Verify compat=on still works**

Run: `cargo build --features compat 2>&1 | tail -3`
Expected: Build succeeds (C++ shim compiled and linked)

- [ ] **Step 9: Commit**

```bash
git add src/build.rs
git commit -m "feat(build): gate C++ shim compilation behind compat feature"
```

---

### Task 3: Gate FFI modules in service/mod.rs

**Files:**
- Modify: `src/service/mod.rs:17-30`

- [ ] **Step 1: Add cfg gates to compat-only modules**

```rust
pub mod backend_service;
#[cfg(feature = "compat")]
pub mod compat;
pub mod disk_report;
#[cfg(feature = "compat")]
pub mod engine_ffi;
pub mod exchange_sender;
pub(crate) mod exec_state_reporter;
pub mod fe_report;
pub mod frontend_rpc;
pub mod grpc_client;
pub mod grpc_proto;
pub mod grpc_server;
pub mod heartbeat_service;
pub(crate) mod internal_rpc;
#[cfg(feature = "compat")]
pub(crate) mod internal_rpc_client;
pub mod internal_service;
pub mod load_tracking_http;
pub mod report_worker;
pub mod stream_load;
pub mod stream_load_http;
```

- [ ] **Step 2: Verify build**

Run: `cargo check 2>&1 | grep "error\[" | head -10`
Expected: Errors in files that reference `internal_rpc_client`, `compat`, `engine_ffi` without compat feature — these are fixed in subsequent tasks.

- [ ] **Step 3: Commit**

```bash
git add src/service/mod.rs
git commit -m "feat(service): gate compat/engine_ffi/internal_rpc_client modules behind compat feature"
```

---

### Task 4: Fix exchange_sender.rs — compile-time brpc vs gRPC

**Files:**
- Modify: `src/service/exchange_sender.rs`

Currently has a runtime `USE_GRPC_EXCHANGE` AtomicBool. Replace with compile-time dispatch.

- [ ] **Step 1: Remove runtime flag and replace send_runtime_filter**

Remove `USE_GRPC_EXCHANGE`, `enable_grpc_exchange()`. Replace `run_send_task` and `send_runtime_filter`:

Remove these lines (near top of file):
```rust
// DELETE these:
use std::sync::atomic::AtomicBool;
static USE_GRPC_EXCHANGE: AtomicBool = AtomicBool::new(false);
pub fn enable_grpc_exchange() { ... }
```

Replace `send_runtime_filter`:
```rust
#[cfg(feature = "compat")]
pub fn send_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: crate::service::internal_rpc_client::proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    crate::service::internal_rpc_client::transmit_runtime_filter(dest_host, dest_port, params)
}

#[cfg(not(feature = "compat"))]
pub fn send_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: crate::service::grpc_client::proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    crate::service::grpc_client::transmit_runtime_filter(dest_host, dest_port, params)
}
```

- [ ] **Step 2: Replace run_send_task dispatch**

In `run_send_task`, replace the `if USE_GRPC_EXCHANGE.load(...)` block:

```rust
fn run_send_task(task: ExchangeSendTask, inflight: Arc<AtomicUsize>, reserve_bytes: usize) {
    let send_start = Instant::now();

    #[cfg(feature = "compat")]
    let result = crate::service::internal_rpc_client::send_chunks(
        &task.dest_host,
        task.dest_port,
        task.finst_id,
        task.node_id,
        task.sender_id,
        task.be_number,
        task.eos,
        task.sequence,
        task.payload,
    );

    #[cfg(not(feature = "compat"))]
    let result = crate::service::grpc_client::send_chunks(
        &task.dest_host,
        task.dest_port,
        task.finst_id,
        task.node_id,
        task.sender_id,
        task.be_number,
        task.eos,
        task.sequence,
        task.payload,
    );

    // ... rest of run_send_task unchanged ...
```

- [ ] **Step 3: Clean up imports**

Remove the `use crate::service::internal_rpc_client;` import line. The fully qualified paths in the cfg blocks handle it.

Also remove `AtomicBool` from the `use std::sync::atomic::{...}` import if no longer needed.

- [ ] **Step 4: Verify build**

Run: `cargo check 2>&1 | grep "error\[" | head -5`
Expected: Fewer errors (exchange_sender now compiles without compat).

- [ ] **Step 5: Commit**

```bash
git add src/service/exchange_sender.rs
git commit -m "feat(exchange): compile-time brpc vs gRPC dispatch via compat feature"
```

---

### Task 5: Fix result_buffer.rs — gate FFI behind compat

**Files:**
- Modify: `src/runtime/result_buffer.rs:146-159`

- [ ] **Step 1: Gate the C++ FFI call**

Replace the existing `#[cfg(not(test))]` guard to also require `compat`:

```rust
#[cfg(all(feature = "compat", not(test)))]
unsafe extern "C" {
    fn novarocks_compat_notify_fetch_ready(finst_id_hi: i64, finst_id_lo: i64);
}

fn notify_fetch_ready(finst_id: UniqueId) {
    #[cfg(all(feature = "compat", not(test)))]
    unsafe {
        novarocks_compat_notify_fetch_ready(finst_id.hi, finst_id.lo);
    }

    #[cfg(not(all(feature = "compat", not(test))))]
    let _ = finst_id;
}
```

- [ ] **Step 2: Commit**

```bash
git add src/runtime/result_buffer.rs
git commit -m "feat(runtime): gate notify_fetch_ready FFI behind compat feature"
```

---

### Task 6: Fix fetch_processor.rs — re-route proto imports and gate brpc lookup

**Files:**
- Modify: `src/exec/operators/fetch_processor.rs`

This file uses `internal_rpc_client::proto::starrocks::*` for proto types and `internal_rpc_client::lookup` for the brpc call.

- [ ] **Step 1: Replace proto type import**

Change:
```rust
use crate::service::internal_rpc_client;
```
to:
```rust
use crate::service::grpc_proto as internal_proto;
#[cfg(feature = "compat")]
use crate::service::internal_rpc_client;
```

Then replace all `internal_rpc_client::proto::starrocks::` with `internal_proto::starrocks::` throughout the file (both in main code and test module).

- [ ] **Step 2: Gate the brpc lookup call**

The `lookup` call at line 285 needs a compat gate. When compat is off, return an error (lookup is a distributed-only operation):

```rust
#[cfg(feature = "compat")]
let resp = internal_rpc_client::lookup(
    &node_info.host,
    node_info.async_internal_port as u16,
    req,
)?;

#[cfg(not(feature = "compat"))]
let resp: crate::service::grpc_proto::starrocks::PLookUpResponse = {
    return Err("lookup not supported without compat feature".to_string());
};
```

- [ ] **Step 3: Gate test module brpc references**

In the `#[cfg(test)]` module, gate the test-hook imports behind compat:

```rust
#[cfg(feature = "compat")]
use crate::service::internal_rpc_client;
```

And wrap the test functions that use `internal_rpc_client::set_lookup_hook` with `#[cfg(feature = "compat")]`.

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/fetch_processor.rs
git commit -m "feat(fetch): gate brpc lookup behind compat, use grpc_proto for types"
```

---

### Task 7: Fix internal_rpc.rs — gate test brpc references

**Files:**
- Modify: `src/service/internal_rpc.rs`

Lines 351, 520-521, 555, 599 reference `internal_rpc_client` in test code.

- [ ] **Step 1: Gate the test imports and calls**

The `internal_rpc_client` references are all in `#[cfg(test)]` blocks. Gate them:

```rust
#[cfg(all(test, feature = "compat"))]
use crate::service::internal_rpc_client;
```

And wrap individual test functions that call `internal_rpc_client::*` with `#[cfg(feature = "compat")]`.

- [ ] **Step 2: Commit**

```bash
git add src/service/internal_rpc.rs
git commit -m "feat(internal_rpc): gate brpc test hooks behind compat feature"
```

---

### Task 8: Fix main.rs — gate BE-mode compat startup

**Files:**
- Modify: `src/main.rs:721-751`

- [ ] **Step 1: Gate the C++ shim startup block**

Wrap the compat startup (lines 721-732) and shutdown (line 751) with `#[cfg(feature = "compat")]`:

```rust
// Start C++ brpc service (for query execution)
#[cfg(feature = "compat")]
{
    let compat_cfg = novarocks::service::compat::CompatConfig {
        host: server.host.as_str(),
        heartbeat_port: server.heartbeat_port,
        brpc_port: server.brpc_port,
        internal_service_query_rpc_thread_num:
            novarocks::common::config::internal_service_query_rpc_thread_num()
                .min(u32::MAX as usize) as u32,
        debug_exec_batch_plan_json: cfg.debug.exec_batch_plan_json,
        log_level: log_level_num,
    };
    novarocks::service::compat::start(&compat_cfg).expect("start compat");
}
```

Similarly gate the shutdown call:

```rust
#[cfg(feature = "compat")]
novarocks::service::compat::stop();
```

- [ ] **Step 2: Adjust the startup log message**

The println at line 734 references `compat_cfg.host` — when compat is off, use the raw config values directly. Use `server.host`, `server.heartbeat_port`, `server.brpc_port` instead of `compat_cfg.*`.

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat(main): gate C++ brpc startup behind compat feature"
```

---

### Task 9: Clean up standalone/server.rs — remove runtime flag

**Files:**
- Modify: `src/standalone/server.rs`

- [ ] **Step 1: Remove enable_grpc_exchange call**

The call `crate::service::exchange_sender::enable_grpc_exchange()` is no longer needed — exchange routing is now compile-time. Remove it.

- [ ] **Step 2: Commit**

```bash
git add src/standalone/server.rs
git commit -m "refactor(standalone): remove runtime enable_grpc_exchange, now compile-time"
```

---

### Task 10: Final verification

- [ ] **Step 1: Verify standalone build (no compat, no thirdparty)**

```bash
unset STARROCKS_THIRDPARTY
cargo build 2>&1 | tail -3
```
Expected: Build succeeds.

- [ ] **Step 2: Run tests without compat**

```bash
cargo test 2>&1 | tail -5
```
Expected: Tests pass (some compat-gated tests will be skipped).

- [ ] **Step 3: Verify compat build (with thirdparty)**

```bash
cargo build --features compat 2>&1 | tail -3
```
Expected: Build succeeds with C++ shim linked.

- [ ] **Step 4: Run tests with compat**

```bash
cargo test --features compat 2>&1 | tail -5
```
Expected: All tests pass including brpc-dependent tests.

- [ ] **Step 5: End-to-end standalone test**

```bash
cargo build && ./target/debug/novarocks standalone-server &
sleep 5
mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1"
pkill -f "novarocks standalone"
```
Expected: Query returns 1.

- [ ] **Step 6: Commit any remaining fixes**

```bash
git add -A
git commit -m "feat: complete compat feature flag — standalone builds without thirdparty"
```
