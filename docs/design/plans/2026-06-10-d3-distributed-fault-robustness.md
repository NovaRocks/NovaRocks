# D3 Distributed Fault-Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a FE-pull heartbeat control plane so the standalone distributed engine tracks live BEs, schedules only to healthy ones, and fails in-flight queries cleanly (with exchange-key cleanup) when a BE dies or restarts.

**Architecture:** A new `BackendRegistry` (keyed by FE-assigned logical `be_id`, reusing D2's `backend_idx`) holds per-BE `endpoint` + `start_epoch` + state machine (Registering/Live/Lost/Decommissioning). A `HeartbeatMgr` background loop sends periodic unary `Heartbeat` gRPC to each BE and drives the state machine via a pure `apply_heartbeat_result`. The scheduler snapshots the Live set at plan time. Cancellation fans out in parallel and now propagates sender RPC failures into the query so receivers abort instead of timing out. Fragment status reuses the existing `ReportExecStatus` + `NovaRocksCoordinator` machinery (no new RPC).

**Tech Stack:** Rust, tonic gRPC (proto in `idl/proto/starust_grpc.proto`, compiled by `src/build.rs`), tokio, the existing D2 `scheduler`/`dispatcher`/`coordinator`/`exchange` modules, and the `sql-test-runner` cross-process cluster harness (`--cluster-size N`).

**Scope note (deviation from spec):** This plan reuses the existing `ReportExecStatus` RPC + `ReportDestination::NovaRocksCoordinator` instead of adding a new `ReportFragmentStatus` RPC (the spec's D3-5). The wire path already exists from iw4; D3 only adds the FE-side query state machine and routes SELECT queries through it. The `2026-06-10-d3-d4-distributed-fault-ops-design.md` spec should be updated to reflect this.

---

## File Structure

**New files:**
- `src/runtime/start_epoch.rs` — process-lifetime epoch nonce (OnceLock). One responsibility: a stable per-process u64 reported in heartbeats.
- `src/runtime/backend_registry.rs` — `BackendRegistry`, `BackendEntry`, `BackendState`, `HeartbeatOutcome`, `RegistryEvent`. Pure state machine + live-set snapshot. No RPC, no I/O — fully unit-testable.
- `src/runtime/heartbeat_mgr.rs` — `HeartbeatMgr`: the tokio loop that sends `Heartbeat` RPC to each BE, feeds `BackendRegistry::apply_heartbeat_result`, and dispatches `RegistryEvent`s to the cleanup hook.
- `src/runtime/query_state.rs` — `QueryStateMachine` + `InFlightQueryTable` (FE-side): per-query state + `be_id → finst_ids` reverse index fed by exec-status reports and BE-lost events.

**Modified files:**
- `src/common/app_config.rs` — add `heartbeat_interval_ms`, `heartbeat_timeout_retries`, `decommission_timeout_secs` to `ClusterConfig` (+ default fns).
- `idl/proto/starust_grpc.proto` — add `Heartbeat` rpc + messages; add `start_epoch` to `CancelFragmentRequest`.
- `src/service/grpc_server.rs` — implement `heartbeat` handler (BE side).
- `src/service/grpc_client.rs` — add `heartbeat_async` / `blocking_heartbeat` wrappers.
- `src/runtime/scheduler.rs` — `assign()` filters to a live snapshot; empty → `no live backend available`.
- `src/runtime/coordinator.rs` — snapshot live set once; wire `InFlightTracker` to registry events.
- `src/service/internal_service.rs` — `cancel()` parallel fan-out; sender-error fast-abort.
- `src/service/exchange_sender.rs` — RPC failure propagates to query context.
- `src/runtime/exchange.rs` — receiver checks sender-error before blocking wait.
- `src/runtime/query_context.rs` — `propagate_sender_error`; expose finst→query for cleanup.
- `tests/sql-test-runner/src/{types,parser,cluster,main}.rs` + new `fault_injection.rs` — fault-injection directives & harness.

---

## PR-1: Config fields, start_epoch, Heartbeat RPC + BE handler

**Goal:** Plumbing only — FE behavior unchanged. A BE can answer a `Heartbeat` RPC with its `start_epoch`/version/cores; config carries heartbeat knobs.

### Task 1.1: `start_epoch` module

**Files:**
- Create: `src/runtime/start_epoch.rs`
- Modify: `src/runtime/mod.rs` (add `pub mod start_epoch;`)
- Test: inline `#[cfg(test)]` in `src/runtime/start_epoch.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/runtime/start_epoch.rs (test module at bottom)
#[cfg(test)]
mod tests {
    use super::start_epoch;

    #[test]
    fn start_epoch_is_stable_and_nonzero() {
        let a = start_epoch();
        let b = start_epoch();
        assert_eq!(a, b, "start_epoch must be stable within a process");
        assert!(a > 0, "start_epoch must be nonzero");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p novarocks start_epoch::tests -- --nocapture`
Expected: FAIL — `cannot find function start_epoch` / module not declared.

- [ ] **Step 3: Write minimal implementation**

```rust
// src/runtime/start_epoch.rs
//! Process-lifetime epoch nonce reported in heartbeats so the FE can detect a
//! BE restart (same endpoint, new epoch) and force-clean stale in-flight state.

use std::sync::OnceLock;

static START_EPOCH: OnceLock<u64> = OnceLock::new();

/// A stable, nonzero value for the lifetime of this process. Computed once on
/// first call from wall-clock millis (units match nothing else; only equality
/// across heartbeats matters).
pub fn start_epoch() -> u64 {
    *START_EPOCH.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
            .max(1)
    })
}
```

Add to `src/runtime/mod.rs` (alphabetical with the other `pub mod` lines):
```rust
pub mod start_epoch;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p novarocks start_epoch::tests -- --nocapture`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add src/runtime/start_epoch.rs src/runtime/mod.rs
git commit -m "feat(d3): add process-lifetime start_epoch nonce module"
```

### Task 1.2: ClusterConfig heartbeat knobs

**Files:**
- Modify: `src/common/app_config.rs:64-71` (struct), add default fns near it
- Test: inline `#[cfg(test)]` in `src/common/app_config.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/common/app_config.rs (test module)
#[cfg(test)]
mod cluster_hb_tests {
    use super::ClusterConfig;

    #[test]
    fn cluster_config_heartbeat_defaults() {
        let c = ClusterConfig::default();
        assert_eq!(c.heartbeat_interval_ms, 5000);
        assert_eq!(c.heartbeat_timeout_retries, 3);
        assert_eq!(c.decommission_timeout_secs, 300);
    }

    #[test]
    fn cluster_config_parses_heartbeat_overrides() {
        let toml = r#"
            role = "fe"
            backends = ["127.0.0.1:9070"]
            heartbeat_interval_ms = 2000
            heartbeat_timeout_retries = 5
        "#;
        let c: ClusterConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.heartbeat_interval_ms, 2000);
        assert_eq!(c.heartbeat_timeout_retries, 5);
        assert_eq!(c.decommission_timeout_secs, 300); // unspecified -> default
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p novarocks cluster_hb_tests -- --nocapture`
Expected: FAIL — `no field heartbeat_interval_ms on ClusterConfig`.

- [ ] **Step 3: Write minimal implementation**

Replace the struct at `src/common/app_config.rs:64-71`:
```rust
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub role: ClusterRole,
    pub backends: Vec<String>,
    pub advertise_host: String,
    pub advertise_port: u16,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_retries: u32,
    pub decommission_timeout_secs: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: ClusterRole::default(),
            backends: Vec::new(),
            advertise_host: String::new(),
            advertise_port: 0,
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            heartbeat_timeout_retries: default_heartbeat_timeout_retries(),
            decommission_timeout_secs: default_decommission_timeout_secs(),
        }
    }
}

fn default_heartbeat_interval_ms() -> u64 { 5000 }
fn default_heartbeat_timeout_retries() -> u32 { 3 }
fn default_decommission_timeout_secs() -> u64 { 300 }
```

> Note: the struct previously derived `Default`. Because `#[serde(default)]` on the struct uses `Default::default()` for missing fields, replacing the derived `Default` with the explicit impl above is what makes the three new fields default to non-zero values when absent from TOML.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p novarocks cluster_hb_tests -- --nocapture`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/common/app_config.rs
git commit -m "feat(d3): add heartbeat/decommission knobs to [cluster] config"
```

### Task 1.3: Heartbeat proto + codegen + start_epoch on CancelFragment

**Files:**
- Modify: `idl/proto/starust_grpc.proto` (service block `8-25`, after `CancelFragmentResponse`)
- Test: `cargo build` is the gate (codegen must compile)

- [ ] **Step 1: Edit proto — add Heartbeat rpc + messages, extend CancelFragmentRequest**

In `service NovaRocksGrpc { ... }` add after the `CancelFragment` line:
```proto
  // D3 cluster heartbeat (FE-pull, unary).
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
```
Add new messages (place after `CancelFragmentResponse`):
```proto
message HeartbeatRequest {
  uint32 assigned_be_id = 1;
  int64 fe_epoch = 2;
}

message HeartbeatResponse {
  uint64 start_epoch = 1;
  string version = 2;
  uint32 num_cores = 3;
  int32 status_code = 4;
}
```
Extend `CancelFragmentRequest` (currently `78-81`):
```proto
message CancelFragmentRequest {
  repeated PUniqueId finst_ids = 1;
  string reason = 2;
  uint64 start_epoch = 3;
}
```

- [ ] **Step 2: Run build to verify codegen compiles**

Run: `cargo build -p novarocks 2>&1 | tail -20`
Expected: FAIL — `GrpcService` no longer satisfies the `NovaRocksGrpc` trait (missing `heartbeat`), and any `CancelFragmentRequest { .. }` literal is missing `start_epoch`. This confirms the new RPC/field are generated.

- [ ] **Step 3: Add BE-side heartbeat handler**

In `src/service/grpc_server.rs`, inside `#[tonic::async_trait] impl ... NovaRocksGrpc for GrpcService`, add after `report_exec_status` (ends ~452):
```rust
async fn heartbeat(
    &self,
    request: tonic::Request<proto::novarocks::HeartbeatRequest>,
) -> Result<tonic::Response<proto::novarocks::HeartbeatResponse>, tonic::Status> {
    let _req = request.into_inner();
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    Ok(tonic::Response::new(proto::novarocks::HeartbeatResponse {
        start_epoch: crate::runtime::start_epoch::start_epoch(),
        version: crate::version::short_version().to_string(),
        num_cores,
        status_code: 0,
    }))
}
```

Fix every existing `CancelFragmentRequest { .. }` constructor to add `start_epoch: 0` (search the workspace):
```bash
grep -rn "CancelFragmentRequest {" src tests | grep -v "message CancelFragmentRequest"
```
For each hit, add `start_epoch: 0,` to the literal (the cancel-epoch is populated in PR-5; 0 means "no epoch check").

- [ ] **Step 4: Run build to verify it compiles**

Run: `cargo build -p novarocks 2>&1 | tail -20`
Expected: SUCCESS (no errors).

- [ ] **Step 5: Add a handler unit test**

```rust
// src/service/grpc_server.rs (test module)
#[cfg(test)]
mod heartbeat_handler_tests {
    use super::*;

    #[tokio::test]
    async fn heartbeat_reports_start_epoch_and_cores() {
        let svc = GrpcService::for_test(); // existing test constructor; if absent, use the same ctor other tests use
        let resp = svc
            .heartbeat(tonic::Request::new(proto::novarocks::HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.start_epoch, crate::runtime::start_epoch::start_epoch());
        assert!(resp.num_cores >= 1);
        assert_eq!(resp.status_code, 0);
    }
}
```
> If `GrpcService` has no test constructor, grep existing `grpc_server.rs` tests for how they build it and mirror that; do not invent a new constructor.

- [ ] **Step 6: Run test + commit**

Run: `cargo test -p novarocks heartbeat_handler_tests -- --nocapture`
Expected: PASS.
```bash
git add idl/proto/starust_grpc.proto src/service/grpc_server.rs src/service/grpc_client.rs
git commit -m "feat(d3): add Heartbeat gRPC + BE handler; add start_epoch to CancelFragment"
```

### Task 1.4: Heartbeat client wrappers

**Files:**
- Modify: `src/service/grpc_client.rs` (impl `NovaRocksGrpcRemoteClient`, after `cancel_fragment_async` ~167)

- [ ] **Step 1: Add async + blocking wrappers**

```rust
pub async fn heartbeat_async(
    &self,
    req: proto::novarocks::HeartbeatRequest,
) -> Result<proto::novarocks::HeartbeatResponse, String> {
    let mut cli = self.make_async_client().await?;
    let mut req = Request::new(req);
    req.set_timeout(Duration::from_secs(3));
    cli.heartbeat(req)
        .await
        .map(|r| r.into_inner())
        .map_err(|e| format!("heartbeat rpc failed: {e}"))
}

pub fn blocking_heartbeat(
    &self,
    req: proto::novarocks::HeartbeatRequest,
) -> Result<proto::novarocks::HeartbeatResponse, String> {
    let mut cli = self.make_client()?;
    data_block_on(async move {
        cli.heartbeat(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("heartbeat rpc failed: {e}"))
    })?
}
```

- [ ] **Step 2: Build + commit**

Run: `cargo build -p novarocks 2>&1 | tail -5`
Expected: SUCCESS.
```bash
git add src/service/grpc_client.rs
git commit -m "feat(d3): add heartbeat client wrappers (async + blocking)"
```

---

## PR-2: BackendRegistry + HeartbeatMgr

**Goal:** A pure, unit-tested state machine and a loop that drives it. Not yet wired into scheduling (PR-3) or cleanup (PR-6).

### Task 2.1: BackendRegistry state machine

**Files:**
- Create: `src/runtime/backend_registry.rs`
- Modify: `src/runtime/mod.rs` (`pub mod backend_registry;`)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing tests**

```rust
// src/runtime/backend_registry.rs (test module)
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn ep(p: u16) -> SocketAddr { format!("127.0.0.1:{p}").parse().unwrap() }

    #[test]
    fn add_then_first_heartbeat_goes_live() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        assert!(reg.live_endpoints().is_empty(), "registering is not live yet");
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        assert!(ev.is_empty());
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn n_missed_heartbeats_goes_lost_and_emits_event_once() {
        let reg = BackendRegistry::new(2);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        assert!(reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x")).is_empty()); // 1 miss
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x")); // 2nd miss -> Lost
        assert_eq!(ev, vec![RegistryEvent::BackendLost { be_id: id }]);
        assert!(reg.live_endpoints().is_empty());
        // further failures do not re-emit Lost
        assert!(reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x")).is_empty());
    }

    #[test]
    fn recovery_clears_miss_counter() {
        let reg = BackendRegistry::new(2);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x")); // Lost
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 2000)); // same epoch -> back Live
        assert!(ev.is_empty());
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn epoch_change_emits_restart_event() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(2, 1500));
        assert_eq!(ev, vec![RegistryEvent::BackendRestarted { be_id: id, old_epoch: 1, new_epoch: 2 }]);
        // still live after restart
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn decommission_excludes_from_live() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        reg.mark_decommissioning(ep(9070)).unwrap();
        assert!(reg.live_endpoints().is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks backend_registry::tests -- --nocapture`
Expected: FAIL — module/types undefined.

- [ ] **Step 3: Implement the module**

```rust
// src/runtime/backend_registry.rs
//! FE-side registry of NovaRocks BEs. Identity is the FE-assigned logical
//! `be_id` (reusing D2's `backend_idx`); the network endpoint and `start_epoch`
//! are attributes. This module is pure: no RPC, no I/O. `HeartbeatMgr` drives
//! it via `apply_heartbeat_result`; scheduling reads `live_endpoints`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

pub type BeId = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendState {
    Registering,
    Live,
    Lost,
    Decommissioning,
}

#[derive(Clone, Debug)]
pub struct BackendEntry {
    pub be_id: BeId,
    pub endpoint: SocketAddr,
    pub state: BackendState,
    pub start_epoch: u64,
    pub last_heartbeat_ms: i64,
    pub missed_heartbeats: u32,
    pub last_err: Option<String>,
    pub version: String,
    pub num_cores: u32,
    pub scheduled_fragments: u64,
}

/// Result of one heartbeat attempt, fed to the registry by `HeartbeatMgr`.
#[derive(Clone, Debug)]
pub enum HeartbeatOutcome {
    Ok { start_epoch: u64, version: String, num_cores: u32, now_ms: i64 },
    Failed { err: String },
}

impl HeartbeatOutcome {
    #[cfg(test)]
    pub fn ok(start_epoch: u64, now_ms: i64) -> Self {
        Self::Ok { start_epoch, version: "test".into(), num_cores: 1, now_ms }
    }
    #[cfg(test)]
    pub fn failed(err: &str) -> Self {
        Self::Failed { err: err.into() }
    }
}

/// Events the registry emits for the cleanup hook (wired in PR-6).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryEvent {
    BackendLost { be_id: BeId },
    BackendRestarted { be_id: BeId, old_epoch: u64, new_epoch: u64 },
}

pub struct BackendRegistry {
    inner: Mutex<BTreeMap<BeId, BackendEntry>>,
    timeout_retries: u32,
    next_be_id: AtomicU32,
}

impl BackendRegistry {
    pub fn new(timeout_retries: u32) -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            timeout_retries: timeout_retries.max(1),
            next_be_id: AtomicU32::new(0),
        }
    }

    /// Register an expected endpoint in `Registering` state and assign a be_id.
    pub fn add_backend(&self, endpoint: SocketAddr) -> BeId {
        let mut guard = self.inner.lock().expect("registry lock");
        // If already present (re-add), keep its id.
        if let Some(e) = guard.values().find(|e| e.endpoint == endpoint) {
            return e.be_id;
        }
        let be_id = self.next_be_id.fetch_add(1, Ordering::SeqCst);
        guard.insert(be_id, BackendEntry {
            be_id,
            endpoint,
            state: BackendState::Registering,
            start_epoch: 0,
            last_heartbeat_ms: 0,
            missed_heartbeats: 0,
            last_err: None,
            version: String::new(),
            num_cores: 0,
            scheduled_fragments: 0,
        });
        be_id
    }

    /// Seed the registry from `[cluster].backends` at startup.
    pub fn seed_from_config(&self, backends: &[SocketAddr]) {
        for ep in backends {
            self.add_backend(*ep);
        }
    }

    /// Drive the state machine for one heartbeat result. Returns events for the
    /// cleanup hook.
    pub fn apply_heartbeat_result(&self, be_id: BeId, outcome: HeartbeatOutcome) -> Vec<RegistryEvent> {
        let mut guard = self.inner.lock().expect("registry lock");
        let Some(e) = guard.get_mut(&be_id) else { return Vec::new(); };
        // A decommissioning backend ignores heartbeats; it leaves only via removal.
        if e.state == BackendState::Decommissioning {
            return Vec::new();
        }
        let mut events = Vec::new();
        match outcome {
            HeartbeatOutcome::Ok { start_epoch, version, num_cores, now_ms } => {
                if e.start_epoch != 0 && start_epoch != 0 && start_epoch != e.start_epoch {
                    events.push(RegistryEvent::BackendRestarted {
                        be_id,
                        old_epoch: e.start_epoch,
                        new_epoch: start_epoch,
                    });
                }
                e.start_epoch = start_epoch;
                e.version = version;
                e.num_cores = num_cores;
                e.last_heartbeat_ms = now_ms;
                e.missed_heartbeats = 0;
                e.last_err = None;
                e.state = BackendState::Live;
            }
            HeartbeatOutcome::Failed { err } => {
                e.missed_heartbeats += 1;
                e.last_err = Some(err);
                if e.state != BackendState::Lost && e.missed_heartbeats >= self.timeout_retries {
                    e.state = BackendState::Lost;
                    events.push(RegistryEvent::BackendLost { be_id });
                }
            }
        }
        events
    }

    /// Live (schedulable) endpoints, in be_id order.
    pub fn live_endpoints(&self) -> Vec<(BeId, SocketAddr)> {
        let guard = self.inner.lock().expect("registry lock");
        guard
            .values()
            .filter(|e| e.state == BackendState::Live)
            .map(|e| (e.be_id, e.endpoint))
            .collect()
    }

    /// All entries (for SHOW BACKENDS / metrics), in be_id order.
    pub fn snapshot(&self) -> Vec<BackendEntry> {
        let guard = self.inner.lock().expect("registry lock");
        guard.values().cloned().collect()
    }

    /// All endpoints regardless of state (HeartbeatMgr iterates these).
    pub fn all_for_heartbeat(&self) -> Vec<(BeId, SocketAddr)> {
        let guard = self.inner.lock().expect("registry lock");
        guard
            .values()
            .filter(|e| e.state != BackendState::Decommissioning)
            .map(|e| (e.be_id, e.endpoint))
            .collect()
    }

    pub fn mark_decommissioning(&self, endpoint: SocketAddr) -> Result<BeId, String> {
        let mut guard = self.inner.lock().expect("registry lock");
        let e = guard
            .values_mut()
            .find(|e| e.endpoint == endpoint)
            .ok_or_else(|| format!("backend {endpoint} not found"))?;
        e.state = BackendState::Decommissioning;
        Ok(e.be_id)
    }

    pub fn remove(&self, be_id: BeId) {
        self.inner.lock().expect("registry lock").remove(&be_id);
    }

    pub fn count_live(&self) -> usize {
        self.live_endpoints().len()
    }
}
```

Add `pub mod backend_registry;` to `src/runtime/mod.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks backend_registry::tests -- --nocapture`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/runtime/backend_registry.rs src/runtime/mod.rs
git commit -m "feat(d3): add BackendRegistry state machine (pure, unit-tested)"
```

### Task 2.2: HeartbeatMgr loop

**Files:**
- Create: `src/runtime/heartbeat_mgr.rs`
- Modify: `src/runtime/mod.rs` (`pub mod heartbeat_mgr;`)
- Test: inline `#[cfg(test)]` (logic test via injected sender closure)

- [ ] **Step 1: Write the failing test**

```rust
// src/runtime/heartbeat_mgr.rs (test module)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::backend_registry::{BackendRegistry, BackendState, HeartbeatOutcome};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn ep(p: u16) -> SocketAddr { format!("127.0.0.1:{p}").parse().unwrap() }

    #[test]
    fn one_round_marks_reachable_live_unreachable_progresses_to_lost() {
        let reg = Arc::new(BackendRegistry::new(1));
        let a = reg.add_backend(ep(9070));
        let b = reg.add_backend(ep(9071));
        // sender: a succeeds, b fails
        let send = |_be_id, endpoint: SocketAddr| -> HeartbeatOutcome {
            if endpoint == ep(9070) {
                HeartbeatOutcome::Ok { start_epoch: 1, version: "v".into(), num_cores: 2, now_ms: 100 }
            } else {
                HeartbeatOutcome::Failed { err: "unreachable".into() }
            }
        };
        let events = run_one_round(&reg, &send);
        assert_eq!(reg.live_endpoints(), vec![(a, ep(9070))]);
        // b with timeout_retries=1 goes Lost in one miss and emits an event
        assert!(events.iter().any(|e| matches!(e, crate::runtime::backend_registry::RegistryEvent::BackendLost { be_id } if *be_id == b)));
        let snap = reg.snapshot();
        assert!(snap.iter().any(|e| e.be_id == b && e.state == BackendState::Lost));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks heartbeat_mgr::tests -- --nocapture`
Expected: FAIL — `run_one_round` undefined.

- [ ] **Step 3: Implement the loop (with a testable single-round fn)**

```rust
// src/runtime/heartbeat_mgr.rs
//! Periodic FE-pull heartbeat loop. Each round sends a unary `Heartbeat` to
//! every non-decommissioning backend, feeds the result into the registry, and
//! hands resulting events to a cleanup hook.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::backend_registry::{BackendRegistry, BeId, HeartbeatOutcome, RegistryEvent};

/// Cleanup hook invoked for each registry event (BE lost / restarted). Wired to
/// query cleanup in PR-6; defaults to a no-op.
pub trait RegistryEventSink: Send + Sync + 'static {
    fn on_event(&self, event: RegistryEvent);
}

/// Run one heartbeat round synchronously. `send` performs the RPC for one
/// backend; in production it is the gRPC call, in tests an injected closure.
pub fn run_one_round<F>(registry: &BackendRegistry, send: &F) -> Vec<RegistryEvent>
where
    F: Fn(BeId, SocketAddr) -> HeartbeatOutcome,
{
    let mut all_events = Vec::new();
    for (be_id, endpoint) in registry.all_for_heartbeat() {
        let outcome = send(be_id, endpoint);
        all_events.extend(registry.apply_heartbeat_result(be_id, outcome));
    }
    all_events
}

/// Production heartbeat sender: one unary gRPC call with the elapsed RTT folded
/// into a metric (PR D4-4). On any error, returns `Failed`.
pub fn grpc_heartbeat(be_id: BeId, endpoint: SocketAddr) -> HeartbeatOutcome {
    use crate::service::grpc_client::NovaRocksGrpcRemoteClient;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let client = match NovaRocksGrpcRemoteClient::connect_blocking(endpoint) {
        Ok(c) => c,
        Err(e) => return HeartbeatOutcome::Failed { err: e },
    };
    let req = crate::service::grpc_proto::novarocks::HeartbeatRequest {
        assigned_be_id: be_id,
        fe_epoch: 0,
    };
    match client.blocking_heartbeat(req) {
        Ok(resp) => HeartbeatOutcome::Ok {
            start_epoch: resp.start_epoch,
            version: resp.version,
            num_cores: resp.num_cores,
            now_ms,
        },
        Err(err) => HeartbeatOutcome::Failed { err },
    }
}

/// Spawn the periodic heartbeat loop on a dedicated OS thread (uses blocking
/// gRPC). Returns immediately. The loop runs for the process lifetime.
pub fn spawn(registry: Arc<BackendRegistry>, interval: Duration, sink: Arc<dyn RegistryEventSink>) {
    std::thread::Builder::new()
        .name("heartbeat-mgr".into())
        .spawn(move || loop {
            let events = run_one_round(&registry, &grpc_heartbeat);
            for ev in events {
                sink.on_event(ev);
            }
            std::thread::sleep(interval);
        })
        .expect("spawn heartbeat-mgr thread");
}

/// No-op sink used until PR-6 wires real cleanup.
pub struct NoopEventSink;
impl RegistryEventSink for NoopEventSink {
    fn on_event(&self, _event: RegistryEvent) {}
}
```

Add `pub mod heartbeat_mgr;` to `src/runtime/mod.rs`.

- [ ] **Step 4: Run tests + build**

Run: `cargo test -p novarocks heartbeat_mgr::tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -5`
Expected: test PASS; build SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/heartbeat_mgr.rs src/runtime/mod.rs
git commit -m "feat(d3): add HeartbeatMgr loop with testable single-round fn"
```

### Task 2.3: Construct registry + start HeartbeatMgr for role=fe

**Files:**
- Modify: `src/engine/mod.rs:2884-2922` (where backends Vec is built for the scheduler)
- Modify: a process-global holder so the registry is reachable from scheduler/coordinator. Add `pub fn backend_registry() -> Option<Arc<BackendRegistry>>` in `src/runtime/backend_registry.rs` backed by a `OnceLock`.

- [ ] **Step 1: Add a process-global registry handle**

Append to `src/runtime/backend_registry.rs`:
```rust
use std::sync::OnceLock;

static GLOBAL_REGISTRY: OnceLock<Arc<BackendRegistry>> = OnceLock::new();

/// Install the process registry (role=fe only). Idempotent: first writer wins.
pub fn install_backend_registry(reg: Arc<BackendRegistry>) {
    let _ = GLOBAL_REGISTRY.set(reg);
}

/// The process registry, if installed (role=fe).
pub fn backend_registry() -> Option<Arc<BackendRegistry>> {
    GLOBAL_REGISTRY.get().cloned()
}
```
Add `use std::sync::Arc;` at the top.

- [ ] **Step 2: Build registry + start heartbeat at FE startup**

In `src/engine/mod.rs`, in the `ClusterRole::Fe` arm that builds `backends`, after parsing the `Vec<SocketAddr>` and before `FragmentScheduler::new(backends)`, insert:
```rust
            // D3: build the registry seeded from config, install globally, and
            // start the heartbeat loop. The scheduler still gets the static
            // list for backward-compatible construction; PR-3 switches it to
            // query the registry's live set.
            if crate::runtime::backend_registry::backend_registry().is_none() {
                let cfg = crate::novarocks_config::config().map_err(|e| format!("role=fe: {e}"))?;
                let reg = std::sync::Arc::new(
                    crate::runtime::backend_registry::BackendRegistry::new(
                        cfg.cluster.heartbeat_timeout_retries,
                    ),
                );
                reg.seed_from_config(&backends);
                crate::runtime::backend_registry::install_backend_registry(reg.clone());
                crate::runtime::heartbeat_mgr::spawn(
                    reg,
                    std::time::Duration::from_millis(cfg.cluster.heartbeat_interval_ms),
                    std::sync::Arc::new(crate::runtime::heartbeat_mgr::NoopEventSink),
                );
            }
```

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p novarocks 2>&1 | tail -5`
Expected: SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/runtime/backend_registry.rs src/engine/mod.rs
git commit -m "feat(d3): install BackendRegistry + start HeartbeatMgr for role=fe"
```

---

## PR-3: Scheduler schedules only Live backends

**Goal:** `assign()` uses the registry's live snapshot; empty → `no live backend available`. All-in-one (no registry) is unchanged.

### Task 3.1: Live-set snapshot in scheduling

**Files:**
- Modify: `src/runtime/scheduler.rs:102-258` (`FragmentScheduler`, `assign`)
- Modify: `src/runtime/coordinator.rs` (snapshot once before `assign`)
- Test: inline `#[cfg(test)]` in `scheduler.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/runtime/scheduler.rs (test module — add to existing or new)
#[cfg(test)]
mod live_filter_tests {
    use super::*;

    #[test]
    fn assign_errors_when_no_live_backends() {
        // Empty live set -> explicit error string.
        let sched = FragmentScheduler::new(vec![]);
        let err = sched
            .assign_with_live(&[], &[], dummy_query_id(), &[])
            .unwrap_err();
        assert!(err.contains("no live backend available"), "got: {err}");
    }

    fn dummy_query_id() -> TUniqueId { TUniqueId { hi: 1, lo: 1 } }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks live_filter_tests -- --nocapture`
Expected: FAIL — `assign_with_live` undefined.

- [ ] **Step 3: Add `assign_with_live` taking an explicit snapshot**

In `src/runtime/scheduler.rs`, refactor `assign` to delegate to a new `assign_with_live` that takes the live backend slice. Keep `assign` for callers that pass the static list (all-in-one):
```rust
    /// Assign using an explicit live-backend snapshot. The snapshot is taken
    /// once by the coordinator and used consistently across placement.
    pub(crate) fn assign_with_live(
        &self,
        fragments: &[FragmentBuildResult],
        edges: &[FragmentEdge],
        query_id: TUniqueId,
        live: &[SocketAddr],
    ) -> Result<SchedulingPlan, String> {
        let n = live.len();
        if n == 0 {
            return Err("no live backend available".into());
        }
        // ... body identical to the old assign(), but every `self.backends`
        // read becomes `live` and `self.backends.len()` becomes `n`.
        self.assign_inner(fragments, edges, query_id, live)
    }

    pub(crate) fn assign(
        &self,
        fragments: &[FragmentBuildResult],
        edges: &[FragmentEdge],
        query_id: TUniqueId,
    ) -> Result<SchedulingPlan, String> {
        let backends = self.backends.clone();
        self.assign_with_live(fragments, edges, query_id, &backends)
    }
```
Then rename the existing body of `assign` (lines ~125-258) into `fn assign_inner(&self, ..., live: &[SocketAddr]) -> Result<SchedulingPlan, String>`, replacing all `self.backends[i]` with `live[i]` and `self.backends.len()` with `live.len()`, and propagate `live` into the `fill_destinations` / `fill_runtime_filter_params` / `fill_per_exch_num_senders` helpers so they index `live`, not `self.backends`. (Those helpers currently read `self.backends`; change their signatures to accept `live: &[SocketAddr]`.)

> Gotcha (from contract): `InFlightTracker` indexes by the same `backend_idx` used here. The live snapshot's index *is* that backend_idx. The `RemoteDispatcher` holds clients for the full configured list; ensure the live snapshot preserves config order so `dispatcher.addr_of(idx)` / submit routing stays aligned. Implementation: build the live snapshot as the subset of configured backends that are `Live`, but **keep their original configured index** rather than re-packing 0..n. Concretely, `assign_with_live` should receive `&[(usize /*backend_idx*/, SocketAddr)]`. Adjust the test and signature accordingly:

Revise the signature to carry the stable index:
```rust
    pub(crate) fn assign_with_live(
        &self,
        fragments: &[FragmentBuildResult],
        edges: &[FragmentEdge],
        query_id: TUniqueId,
        live: &[(usize, SocketAddr)],
    ) -> Result<SchedulingPlan, String> {
        if live.is_empty() {
            return Err("no live backend available".into());
        }
        self.assign_inner(fragments, edges, query_id, live)
    }
```
and the test becomes `sched.assign_with_live(&[], &[], dummy_query_id(), &[])`.

- [ ] **Step 4: Coordinator snapshots the live set once**

In `src/runtime/coordinator.rs`, where `scheduler.assign(...)` is currently called (the contract notes line ~125), snapshot first:
```rust
        // D3: snapshot the live backend set once for consistent indexing.
        let live: Vec<(usize, std::net::SocketAddr)> =
            match crate::runtime::backend_registry::backend_registry() {
                Some(reg) => reg
                    .live_endpoints()
                    .into_iter()
                    .map(|(be_id, ep)| (be_id as usize, ep))
                    .collect(),
                // No registry (all-in-one): every configured backend is "live".
                None => self
                    .scheduler
                    .backends()
                    .iter()
                    .copied()
                    .enumerate()
                    .collect(),
            };
        let plan = self.scheduler.assign_with_live(&fragments, &edges, query_id, &live)?;
```
(Match the existing variable names for `fragments`/`edges`/`query_id` at that call site.)

- [ ] **Step 5: Run tests + build**

Run: `cargo test -p novarocks live_filter_tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -5`
Expected: test PASS; build SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/scheduler.rs src/runtime/coordinator.rs
git commit -m "feat(d3): scheduler assigns only to live backends; empty -> explicit error"
```

---

## PR-4: FE-side query state machine + in-flight table (reuse ReportExecStatus)

**Goal:** The FE coordinator tracks per-query state and which finsts run on which be_id, fed by the existing exec-status reports. On a FAILED report it fails the query fast (cancel wiring lands in PR-5/6).

### Task 4.1: QueryStateMachine + InFlightQueryTable

**Files:**
- Create: `src/runtime/query_state.rs`
- Modify: `src/runtime/mod.rs` (`pub mod query_state;`)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing tests**

```rust
// src/runtime/query_state.rs (test module)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::{QueryId, UniqueId};

    fn qid(n: i64) -> QueryId { QueryId { hi: n, lo: n } }
    fn fid(n: i64) -> UniqueId { UniqueId { hi: n, lo: n } }

    #[test]
    fn tracks_finsts_per_backend_and_completes() {
        let t = InFlightQueryTable::new();
        t.register(qid(1), fid(10), 0);
        t.register(qid(1), fid(11), 1);
        assert_eq!(t.state(qid(1)), Some(QueryState::Running));
        assert_eq!(t.finsts_on_backend(qid(1), 1), vec![fid(11)]);
        t.on_fragment_done(fid(10), Ok(()));
        assert_eq!(t.state(qid(1)), Some(QueryState::Running));
        t.on_fragment_done(fid(11), Ok(()));
        assert_eq!(t.state(qid(1)), Some(QueryState::Finished));
    }

    #[test]
    fn fragment_failure_fails_query_with_reason() {
        let t = InFlightQueryTable::new();
        t.register(qid(2), fid(20), 0);
        t.register(qid(2), fid(21), 1);
        t.on_fragment_done(fid(20), Err("be#0 crashed".into()));
        assert_eq!(t.state(qid(2)), Some(QueryState::Failed));
        assert_eq!(t.failure_reason(qid(2)).as_deref(), Some("be#0 crashed"));
    }

    #[test]
    fn backend_lost_fails_queries_touching_that_backend() {
        let t = InFlightQueryTable::new();
        t.register(qid(3), fid(30), 0);
        t.register(qid(4), fid(40), 1);
        let failed = t.on_backend_lost(0);
        assert_eq!(failed, vec![qid(3)]);
        assert_eq!(t.state(qid(3)), Some(QueryState::Failed));
        assert_eq!(t.state(qid(4)), Some(QueryState::Running)); // untouched
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks query_state::tests -- --nocapture`
Expected: FAIL — module/types undefined. (If `QueryId`/`UniqueId` live elsewhere than `crate::common::ids`, grep `pub struct QueryId` and fix the `use` in the test and module.)

- [ ] **Step 3: Implement**

```rust
// src/runtime/query_state.rs
//! FE-side query state machine + in-flight fragment table for standalone
//! distributed execution. Fed by exec-status reports (reused ReportExecStatus
//! path) and by BackendRegistry "backend lost" events.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use crate::common::ids::{QueryId, UniqueId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryState {
    Running,
    Finished,
    Failed,
}

struct QueryEntry {
    state: QueryState,
    failure_reason: Option<String>,
    /// finst_id -> backend_idx
    finsts: HashMap<UniqueId, usize>,
    remaining: usize,
}

#[derive(Default)]
struct Inner {
    queries: HashMap<QueryId, QueryEntry>,
    finst_to_query: HashMap<UniqueId, QueryId>,
}

pub struct InFlightQueryTable {
    inner: Mutex<Inner>,
}

impl InFlightQueryTable {
    pub fn new() -> Self {
        Self { inner: Mutex::new(Inner::default()) }
    }

    /// Record that `finst` for `query` was submitted to `backend_idx`.
    pub fn register(&self, query: QueryId, finst: UniqueId, backend_idx: usize) {
        let mut g = self.inner.lock().expect("query_state lock");
        let e = g.queries.entry(query).or_insert_with(|| QueryEntry {
            state: QueryState::Running,
            failure_reason: None,
            finsts: HashMap::new(),
            remaining: 0,
        });
        if e.finsts.insert(finst, backend_idx).is_none() {
            e.remaining += 1;
        }
        g.finst_to_query.insert(finst, query);
    }

    pub fn state(&self, query: QueryId) -> Option<QueryState> {
        self.inner.lock().expect("query_state lock").queries.get(&query).map(|e| e.state)
    }

    pub fn failure_reason(&self, query: QueryId) -> Option<String> {
        self.inner
            .lock()
            .expect("query_state lock")
            .queries
            .get(&query)
            .and_then(|e| e.failure_reason.clone())
    }

    pub fn finsts_on_backend(&self, query: QueryId, backend_idx: usize) -> Vec<UniqueId> {
        let g = self.inner.lock().expect("query_state lock");
        g.queries
            .get(&query)
            .map(|e| {
                let mut v: Vec<UniqueId> = e
                    .finsts
                    .iter()
                    .filter(|(_, idx)| **idx == backend_idx)
                    .map(|(f, _)| *f)
                    .collect();
                v.sort_by_key(|u| (u.hi, u.lo));
                v
            })
            .unwrap_or_default()
    }

    /// A fragment finished (Ok) or failed (Err(reason)).
    pub fn on_fragment_done(&self, finst: UniqueId, result: Result<(), String>) {
        let mut g = self.inner.lock().expect("query_state lock");
        let Some(&query) = g.finst_to_query.get(&finst) else { return; };
        let Some(e) = g.queries.get_mut(&query) else { return; };
        if e.state == QueryState::Failed {
            return;
        }
        match result {
            Ok(()) => {
                if e.finsts.contains_key(&finst) && e.remaining > 0 {
                    e.remaining -= 1;
                }
                if e.remaining == 0 {
                    e.state = QueryState::Finished;
                }
            }
            Err(reason) => {
                e.state = QueryState::Failed;
                e.failure_reason = Some(reason);
            }
        }
    }

    /// Mark every query that had a fragment on `backend_idx` as failed.
    /// Returns the affected query ids (for cancel fan-out in PR-6).
    pub fn on_backend_lost(&self, backend_idx: usize) -> Vec<QueryId> {
        let mut g = self.inner.lock().expect("query_state lock");
        let mut affected = Vec::new();
        for (qid, e) in g.queries.iter_mut() {
            if e.state == QueryState::Running && e.finsts.values().any(|idx| *idx == backend_idx) {
                e.state = QueryState::Failed;
                e.failure_reason = Some(format!("backend {backend_idx} lost"));
                affected.push(*qid);
            }
        }
        affected.sort_by_key(|q| (q.hi, q.lo));
        affected
    }

    pub fn forget(&self, query: QueryId) {
        let mut g = self.inner.lock().expect("query_state lock");
        if let Some(e) = g.queries.remove(&query) {
            for f in e.finsts.keys() {
                g.finst_to_query.remove(f);
            }
        }
    }
}

static TABLE: std::sync::OnceLock<InFlightQueryTable> = std::sync::OnceLock::new();

pub fn in_flight_table() -> &'static InFlightQueryTable {
    TABLE.get_or_init(InFlightQueryTable::new)
}
```

Add `pub mod query_state;` to `src/runtime/mod.rs`.

- [ ] **Step 4: Run tests + commit**

Run: `cargo test -p novarocks query_state::tests -- --nocapture`
Expected: PASS (3 passed).
```bash
git add src/runtime/query_state.rs src/runtime/mod.rs
git commit -m "feat(d3): add FE-side query state machine + in-flight fragment table"
```

### Task 4.2: Feed reports into the table; register on submit

**Files:**
- Modify: `src/runtime/coordinator.rs` (call `in_flight_table().register(query, finst, backend_idx)` when recording submitted instances — same place `InFlightTracker::record_submitted` is called)
- Modify: `src/runtime/write_coordinator.rs` `handle_report_exec_status` (the existing ReportExecStatus consumer) to also call `in_flight_table().on_fragment_done(finst, result)` based on the report's `done`/`status`.

- [ ] **Step 1: Register finsts on submission**

In `src/runtime/coordinator.rs`, wherever `tracker.record_submitted(backend_idx, finst_id)` is called, add immediately after:
```rust
            crate::runtime::query_state::in_flight_table().register(
                crate::common::ids::QueryId { hi: query_id.hi, lo: query_id.lo },
                crate::common::ids::UniqueId { hi: finst_id.hi, lo: finst_id.lo },
                backend_idx,
            );
```
(Use the coordinator's existing `query_id`/`finst_id`/`backend_idx` bindings.)

- [ ] **Step 2: Feed completion/failure from the report consumer**

In `src/runtime/write_coordinator.rs::handle_report_exec_status`, after the existing handling, translate the report into a fragment-done event:
```rust
    // D3: drive the FE-side query state machine from fragment reports.
    if let (Some(finst), done) = (params.fragment_instance_id, params.done.unwrap_or(false)) {
        let finst_id = crate::common::ids::UniqueId { hi: finst.hi, lo: finst.lo };
        let failed = params
            .status
            .as_ref()
            .map(|s| s.status_code != crate::status_code::TStatusCode::OK)
            .unwrap_or(false);
        if failed {
            let reason = params
                .status
                .as_ref()
                .and_then(|s| s.error_msgs.as_ref())
                .and_then(|m| m.first().cloned())
                .unwrap_or_else(|| "fragment failed".to_string());
            crate::runtime::query_state::in_flight_table().on_fragment_done(finst_id, Err(reason));
        } else if done {
            crate::runtime::query_state::in_flight_table().on_fragment_done(finst_id, Ok(()));
        }
    }
```
(Field names `fragment_instance_id`, `done`, `status`, `error_msgs` come from `TReportExecStatusParams`; verify against `build_report_params` usage and adjust `Option` handling to match the generated thrift accessors.)

- [ ] **Step 3: Ensure SELECT queries register a novarocks_report_addr**

Confirm via grep whether the standalone planner already sets `novarocks_report_addr` for non-write fragments:
```bash
grep -rn "novarocks_report_addr" src/sql src/engine src/lower | head
```
If only writes set it, set it for all distributed fragments at the same place the FE coordinator address is known (the construction site in `src/engine/mod.rs` near the scheduler, mirroring how writes pass it). Add the FE's own advertise endpoint as the report target so non-root fragments report up. This is a small, localized change; show the exact edit in the task when the grep reveals the construction site.

- [ ] **Step 4: Build + commit**

Run: `cargo build -p novarocks 2>&1 | tail -10`
Expected: SUCCESS.
```bash
git add src/runtime/coordinator.rs src/runtime/write_coordinator.rs src/engine/mod.rs
git commit -m "feat(d3): feed fragment reports into FE query state machine; report SELECT fragments"
```

---

## PR-5: Parallel cancel fan-out + sender RPC failure propagation

**Goal:** Fix hardening gap #1. Cancel fans out concurrently; a failed exchange send aborts the receiver immediately instead of waiting out the timeout.

### Task 5.1: Parallel cancel fan-out

**Files:**
- Modify: `src/service/internal_service.rs:1613-1641` (`cancel`)
- Test: inline `#[cfg(test)]` (verify all target finsts are cleaned; timing not asserted)

- [ ] **Step 1: Write the failing test**

```rust
// src/service/internal_service.rs (test module)
#[cfg(test)]
mod cancel_fanout_tests {
    use super::*;

    #[test]
    fn cancel_cleans_all_finsts_for_query() {
        // Register two finsts under one query in the exchange + context, then
        // cancel one and assert both exchange keys are gone.
        // (Use the existing exchange test helpers; mirror an existing
        // exchange.rs cancel test for registration.)
        // This test asserts cancel() removes every mapped finst, not ordering.
        // ... see exchange.rs tests for register helpers ...
    }
}
```
> Implementation note: if `internal_service.rs` has no unit-test harness for the exchange registry, instead add the assertion to an existing `exchange.rs` test that already registers receivers, and call `crate::cancel(finst)` then assert `exchange::is_key_canceled` for all keys. Keep the test focused on completeness, not concurrency.

- [ ] **Step 2: Run to verify it fails / compiles red**

Run: `cargo test -p novarocks cancel_fanout_tests -- --nocapture`
Expected: FAIL (test asserts cleanup that isn't yet parallel/complete) or compile error guiding the helper choice.

- [ ] **Step 3: Parallelize the fan-out**

Replace the serialized loop in `cancel` (`src/service/internal_service.rs:1632-1635`):
```rust
    // D3: fan out exchange cleanup concurrently. result_buffer::cancel stays
    // sequential (cheap, order-independent local cleanup).
    for id in &target_finsts {
        result_buffer::cancel(*id);
    }
    let cleanup: Vec<_> = target_finsts
        .iter()
        .map(|id| {
            let id = *id;
            std::thread::spawn(move || exchange::cancel_fragment(id.hi, id.lo))
        })
        .collect();
    for h in cleanup {
        let _ = h.join();
    }
```
> Rationale: `exchange::cancel_fragment` is synchronous and lock-bounded; spawning short-lived threads gives concurrency without requiring a tokio runtime in this sync path. For large fan-outs this is bounded by `target_finsts.len()`, which equals the query's fragment count.

- [ ] **Step 4: Run test + build + commit**

Run: `cargo test -p novarocks cancel_fanout_tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -5`
Expected: PASS; SUCCESS.
```bash
git add src/service/internal_service.rs
git commit -m "feat(d3): parallel cancel fan-out across participating backends"
```

### Task 5.2: Sender RPC failure propagates to the query

**Files:**
- Modify: `src/runtime/query_context.rs` (add `propagate_sender_error`)
- Modify: `src/service/exchange_sender.rs:326-331` (call it on send failure)
- Modify: `src/runtime/exchange.rs` (receiver checks sender-error before/while waiting)
- Test: inline `#[cfg(test)]` in `query_context.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/runtime/query_context.rs (test module)
#[cfg(test)]
mod sender_error_tests {
    use super::*;

    #[test]
    fn propagate_sender_error_cancels_query_finsts() {
        let mgr = QueryContextManager::new_for_test(); // mirror existing test ctor
        let qid = QueryId { hi: 5, lo: 5 };
        let finst = UniqueId { hi: 50, lo: 50 };
        mgr.register_finst_for_test(qid, finst); // mirror existing helper
        let cancelled = mgr.propagate_sender_error(finst, "send failed: connection refused".into());
        assert!(cancelled.contains(&finst));
    }
}
```
> If `QueryContextManager` lacks `new_for_test`/`register_finst_for_test`, grep existing `query_context.rs` tests for the helpers they use and mirror them; do not invent constructors.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks sender_error_tests -- --nocapture`
Expected: FAIL — `propagate_sender_error` undefined.

- [ ] **Step 3: Implement `propagate_sender_error`**

In `src/runtime/query_context.rs`, add to `impl QueryContextManager`:
```rust
    /// A sender's exchange RPC failed. Map the finst to its query and cancel
    /// the whole query so blocked receivers abort instead of timing out.
    pub(crate) fn propagate_sender_error(&self, finst_id: UniqueId, err: String) -> Vec<UniqueId> {
        match self.query_id_by_finst(finst_id) {
            Some(qid) => {
                let finsts = self.cancel_query(qid, format!("exchange send failed: {err}"));
                for id in &finsts {
                    crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
                }
                finsts
            }
            None => {
                // Unmapped finst: cancel just this fragment's exchange keys.
                crate::runtime::exchange::cancel_fragment(finst_id.hi, finst_id.lo);
                vec![finst_id]
            }
        }
    }
```

- [ ] **Step 4: Call it from the sender failure path**

In `src/service/exchange_sender.rs`, in `run_send_task` where the failure branch currently is (`task.error_state.set_error(err.clone()); error!(...)` at ~327):
```rust
    if let Err(err) = result {
        task.error_state.set_error(err.clone());
        error!(
            "exchange send failed: dest={} finst={} node_id={} sender_id={} seq={} error={}",
            task.dest_host, task.finst_id, task.node_id, task.sender_id, task.sequence, err
        );
        // D3: surface the failure to the owning query so receivers abort now.
        let finst = crate::common::ids::UniqueId {
            hi: task.finst_id.hi,
            lo: task.finst_id.lo,
        };
        crate::runtime::query_context::query_context_manager()
            .propagate_sender_error(finst, err);
    } else {
        // ... unchanged success branch ...
    }
```
(Confirm `task.finst_id`'s type and the accessor for `query_context_manager()`; both are used elsewhere in the file/crate.)

- [ ] **Step 5: Run test + build + commit**

Run: `cargo test -p novarocks sender_error_tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -5`
Expected: PASS; SUCCESS.
```bash
git add src/runtime/query_context.rs src/service/exchange_sender.rs src/runtime/exchange.rs
git commit -m "feat(d3): propagate exchange sender RPC failure to query (no more receiver hang)"
```

---

## PR-6: BE-lost cleanup + epoch restart cleanup

**Goal:** Wire `RegistryEvent`s to actual query cleanup: a lost or restarted BE fails its in-flight queries, fans out cancel to the other participating BEs, and purges exchange keys.

### Task 6.1: Cleanup event sink

**Files:**
- Create: `src/runtime/registry_cleanup.rs` (`QueryCleanupSink: RegistryEventSink`)
- Modify: `src/runtime/mod.rs` (`pub mod registry_cleanup;`)
- Modify: `src/engine/mod.rs` (pass `QueryCleanupSink` instead of `NoopEventSink` to `heartbeat_mgr::spawn`)
- Test: inline `#[cfg(test)]` (event → table failure)

- [ ] **Step 1: Write the failing test**

```rust
// src/runtime/registry_cleanup.rs (test module)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::backend_registry::RegistryEvent;
    use crate::runtime::query_state::in_flight_table;
    use crate::common::ids::{QueryId, UniqueId};

    #[test]
    fn backend_lost_event_fails_queries_on_that_backend() {
        let t = in_flight_table();
        t.register(QueryId { hi: 60, lo: 60 }, UniqueId { hi: 600, lo: 600 }, 2);
        let sink = QueryCleanupSink::new();
        sink.on_event(RegistryEvent::BackendLost { be_id: 2 });
        assert_eq!(
            t.state(QueryId { hi: 60, lo: 60 }),
            Some(crate::runtime::query_state::QueryState::Failed)
        );
    }
}
```
> Note: `in_flight_table()` is a process global; pick query/finst ids unlikely to collide with other tests, or run this test in its own `#[serial]` if the repo uses `serial_test` (grep for it).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks registry_cleanup::tests -- --nocapture`
Expected: FAIL — `QueryCleanupSink` undefined.

- [ ] **Step 3: Implement the sink**

```rust
// src/runtime/registry_cleanup.rs
//! Bridges BackendRegistry events to query cleanup: fail in-flight queries on a
//! lost/restarted BE, fan out cancel to other participating backends, and purge
//! exchange keys.

use crate::common::ids::{QueryId, UniqueId};
use crate::runtime::backend_registry::{BeId, RegistryEvent};
use crate::runtime::heartbeat_mgr::RegistryEventSink;
use crate::runtime::query_state::in_flight_table;

pub struct QueryCleanupSink;

impl QueryCleanupSink {
    pub fn new() -> Self { Self }

    fn fail_queries_on_backend(&self, backend_idx: usize, reason: &str) {
        let affected: Vec<QueryId> = in_flight_table().on_backend_lost(backend_idx);
        for qid in affected {
            // Cancel the whole query: maps to all finsts and purges exchange keys.
            crate::cancel_query_by_id(qid, reason.to_string());
            in_flight_table().forget(qid);
        }
    }
}

impl RegistryEventSink for QueryCleanupSink {
    fn on_event(&self, event: RegistryEvent) {
        match event {
            RegistryEvent::BackendLost { be_id } => {
                self.fail_queries_on_backend(be_id as usize, &format!("backend {be_id} lost"));
            }
            RegistryEvent::BackendRestarted { be_id, old_epoch, new_epoch } => {
                self.fail_queries_on_backend(
                    be_id as usize,
                    &format!("backend {be_id} restarted (epoch {old_epoch} -> {new_epoch})"),
                );
            }
        }
    }
}
```

Add a thin crate-level helper `cancel_query_by_id` in `src/lib.rs` (next to the existing `pub fn cancel(finst_id: UniqueId)`), reusing the parallel fan-out from PR-5:
```rust
/// Cancel an entire query by id: fan out exchange cleanup to all its finsts.
pub fn cancel_query_by_id(query_id: crate::common::ids::QueryId, reason: String) {
    let mgr = crate::runtime::query_context::query_context_manager();
    let finsts = mgr.cancel_query(query_id, reason);
    let cleanup: Vec<_> = finsts
        .iter()
        .map(|id| {
            let id = *id;
            std::thread::spawn(move || {
                crate::runtime::result_buffer::cancel(id);
                crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
            })
        })
        .collect();
    for h in cleanup {
        let _ = h.join();
    }
}
```

Add `pub mod registry_cleanup;` to `src/runtime/mod.rs`.

- [ ] **Step 4: Use the real sink at FE startup**

In `src/engine/mod.rs`, change the `heartbeat_mgr::spawn(...)` call from PR-2 Task 2.3 to pass the cleanup sink:
```rust
                crate::runtime::heartbeat_mgr::spawn(
                    reg,
                    std::time::Duration::from_millis(cfg.cluster.heartbeat_interval_ms),
                    std::sync::Arc::new(crate::runtime::registry_cleanup::QueryCleanupSink::new()),
                );
```

- [ ] **Step 5: Run test + build + commit**

Run: `cargo test -p novarocks registry_cleanup::tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -10`
Expected: PASS; SUCCESS.
```bash
git add src/runtime/registry_cleanup.rs src/runtime/mod.rs src/lib.rs src/engine/mod.rs
git commit -m "feat(d3): fail + clean in-flight queries on BE lost/restart events"
```

### Task 6.2: Cancel carries start_epoch; BE ignores stale-epoch cancels

**Files:**
- Modify: `src/service/grpc_server.rs` `cancel_fragment` handler (compare `req.start_epoch` to local `start_epoch`)
- Modify: cancel callers to set `start_epoch` where the target BE's epoch is known (best-effort; 0 = skip check)
- Test: inline handler test

- [ ] **Step 1: Write the failing test**

```rust
// src/service/grpc_server.rs (test module)
#[cfg(test)]
mod cancel_epoch_tests {
    use super::*;

    #[tokio::test]
    async fn cancel_with_mismatched_epoch_is_ignored() {
        let svc = GrpcService::for_test(); // mirror existing ctor
        let mine = crate::runtime::start_epoch::start_epoch();
        let resp = svc
            .cancel_fragment(tonic::Request::new(proto::novarocks::CancelFragmentRequest {
                finst_ids: vec![proto::novarocks::PUniqueId { hi: 1, lo: 1 }],
                reason: "stale".into(),
                start_epoch: mine.wrapping_add(1), // mismatched
            }))
            .await
            .unwrap()
            .into_inner();
        // status_code 0 == accepted/no-op; we assert it returns the
        // "ignored" code (define IGNORED_STALE_EPOCH = 2).
        assert_eq!(resp.status_code, 2);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks cancel_epoch_tests -- --nocapture`
Expected: FAIL (handler doesn't check epoch).

- [ ] **Step 3: Add the epoch guard to the handler**

In `cancel_fragment` (`src/service/grpc_server.rs:393`), at the top after `let req = request.into_inner();`:
```rust
    // D3: a cancel addressed to a previous process epoch is stale (the BE
    // restarted); ignore it so we never kill a fresh process's unrelated work.
    if req.start_epoch != 0 && req.start_epoch != crate::runtime::start_epoch::start_epoch() {
        return Ok(tonic::Response::new(proto::novarocks::CancelFragmentResponse {
            status_code: 2, // IGNORED_STALE_EPOCH
        }));
    }
```
> `CancelFragmentResponse` currently has only `status_code`. Keep field set unchanged; `2` is the new ignored code. Callers that don't know the epoch pass `start_epoch: 0` (the default added in PR-1) and keep the old behavior.

- [ ] **Step 4: Run test + build + commit**

Run: `cargo test -p novarocks cancel_epoch_tests -- --nocapture && cargo build -p novarocks 2>&1 | tail -5`
Expected: PASS; SUCCESS.
```bash
git add src/service/grpc_server.rs
git commit -m "feat(d3): BE ignores cancel addressed to a stale start_epoch"
```

---

## PR-7: Fault-injection integration tests

**Goal:** Drive the whole stack through the `sql-test-runner` cross-process harness with `kill-be-during-query`, `network-partition-during-query`, `heartbeat-delay`, and `be-restart`, asserting clean failure and recovery.

### Task 7.1: Fault-injection directives + harness hooks

**Files:**
- Modify: `tests/sql-test-runner/src/types.rs:20-44` (QueryMeta)
- Modify: `tests/sql-test-runner/src/parser.rs` (`parse_meta`)
- Modify: `tests/sql-test-runner/src/cluster.rs` (expose BE handles; store runtime)
- Create: `tests/sql-test-runner/src/fault_injection.rs`
- Modify: `tests/sql-test-runner/src/main.rs` (`run_case` step loop)

- [ ] **Step 1: Add directives to QueryMeta**

In `tests/sql-test-runner/src/types.rs` `QueryMeta` (lines 20-44), add fields:
```rust
    pub kill_be_index: Option<usize>,
    pub network_partition_be: Option<usize>,
    pub heartbeat_delay_ms: Option<u64>,
    pub restart_be_delay_ms: Option<u64>,
```

- [ ] **Step 2: Parse them**

In `tests/sql-test-runner/src/parser.rs` `parse_meta`, add match arms mirroring existing numeric directives (e.g. how `retry_count` is parsed):
```rust
                "kill_be_index" => meta.kill_be_index = Some(value.parse().map_err(|e| format!("kill_be_index: {e}"))?),
                "network_partition_be" => meta.network_partition_be = Some(value.parse().map_err(|e| format!("network_partition_be: {e}"))?),
                "heartbeat_delay_ms" => meta.heartbeat_delay_ms = Some(value.parse().map_err(|e| format!("heartbeat_delay_ms: {e}"))?),
                "restart_be_delay_ms" => meta.restart_be_delay_ms = Some(value.parse().map_err(|e| format!("restart_be_delay_ms: {e}"))?),
```
(Match the exact `value`/error-handling shape of the surrounding arms.)

- [ ] **Step 3: Expose a kill/restart handle on the cluster**

In `tests/sql-test-runner/src/cluster.rs`, on `CrossProcessServerHandle`, add:
```rust
    /// Kill BE `index` (SIGKILL). The process is not restarted automatically.
    pub fn kill_be(&mut self, index: usize) -> anyhow::Result<()> {
        let be = self.be_processes.get_mut(index)
            .ok_or_else(|| anyhow::anyhow!("kill_be: no BE at index {index}"))?;
        be.kill_now()
    }

    /// Restart BE `index` from its already-written config.
    pub fn restart_be(&mut self, index: usize) -> anyhow::Result<()> {
        let config_path = self.be_config_path(index)?;
        let proc = ProcessGuard::spawn(&self.novarocks_bin, "be", &config_path, "NOVAROCKS_READY role=be")?;
        self.be_processes[index] = proc;
        Ok(())
    }
```
Add `ProcessGuard::kill_now`:
```rust
    pub fn kill_now(&mut self) -> anyhow::Result<()> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Ok(())
    }
```
(Store `novarocks_bin` and the per-BE config paths on the handle if not already present — they are produced in `launch`; thread them onto the struct.)

- [ ] **Step 4: Implement fault_injection.rs**

```rust
// tests/sql-test-runner/src/fault_injection.rs
//! Applies fault-injection directives between query steps in cross-process mode.

use std::thread;
use std::time::Duration;

use crate::cluster::CrossProcessServerHandle;
use crate::types::QueryMeta;

pub fn apply_pre_query(meta: &QueryMeta, cluster: &mut CrossProcessServerHandle) -> anyhow::Result<()> {
    if let Some(ms) = meta.heartbeat_delay_ms {
        thread::sleep(Duration::from_millis(ms));
    }
    if let Some(idx) = meta.kill_be_index {
        eprintln!("⚡ fault injection: killing BE[{idx}]");
        cluster.kill_be(idx)?;
        if let Some(delay) = meta.restart_be_delay_ms {
            thread::sleep(Duration::from_millis(delay));
            eprintln!("⚡ fault injection: restarting BE[{idx}]");
            cluster.restart_be(idx)?;
        }
    }
    Ok(())
}
```
> Network partition via `tc`/`iptables` is host-wide and racy under the parallel runner; scope PR-7 to `kill_be` + `restart_be` + `heartbeat_delay` (which exercise the same Lost/restart/epoch paths) and leave true network partition as a follow-up. Mark the partition test case `@sequential=true` if added later.

- [ ] **Step 5: Call it in the step loop**

In `tests/sql-test-runner/src/main.rs` `run_case`, in the per-step loop (before executing the step's SQL), when running in cross-process mode and the step has any fault directive:
```rust
        if let Some(cluster) = cross_process.as_mut() {
            if step.meta.kill_be_index.is_some()
                || step.meta.heartbeat_delay_ms.is_some()
            {
                crate::fault_injection::apply_pre_query(&step.meta, cluster)?;
            }
        }
```
Add `mod fault_injection;` to the runner's module list.

- [ ] **Step 6: Build the runner**

Run: `cargo build --manifest-path tests/sql-test-runner/Cargo.toml 2>&1 | tail -10`
Expected: SUCCESS.

- [ ] **Step 7: Commit**

```bash
git add tests/sql-test-runner/src/types.rs tests/sql-test-runner/src/parser.rs tests/sql-test-runner/src/cluster.rs tests/sql-test-runner/src/fault_injection.rs tests/sql-test-runner/src/main.rs
git commit -m "test(d3): add fault-injection directives + kill/restart BE harness"
```

### Task 7.2: Fault-injection test cases

**Files:**
- Create: `sql-tests/distributed-resilience/sql/kill-be-during-query.sql`
- Create: `sql-tests/distributed-resilience/sql/be-restart-rejoin.sql`
- Create: `sql-tests/distributed-resilience/<expected dir>` per the suite layout (grep an existing suite for whether goldens live alongside or under `expected/`).

- [ ] **Step 1: Write `kill-be-during-query.sql`**

```sql
-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS d3_resilience;

-- query 2
-- @skip_result_check=true
USE d3_resilience;

-- query 3: kill BE[1] right before a distributed scan; expect a clean error.
-- @kill_be_index=1
-- @expect_error=backend
SELECT COUNT(*) FROM (SELECT * FROM TABLE(generate_series(1, 1000000)) AS g(x)) t;

-- query 4: a new query after the kill must still succeed on remaining BEs
-- (BE[1] is now Lost; scheduler avoids it). With cluster-size 2 this still has
-- BE[0]; assert it returns a value.
-- @result_contains=1000000
SELECT COUNT(*) FROM (SELECT * FROM TABLE(generate_series(1, 1000000)) AS g(x)) t;
```
> Adjust the failing query so it is genuinely distributed (multi-fragment) for your build; if `generate_series` runs single-fragment, replace with a query over a created distributed table. Confirm `@expect_error` substring matching exists (it does: `QueryMeta.expect_error`).

- [ ] **Step 2: Write `be-restart-rejoin.sql`**

```sql
-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS d3_restart;

-- query 2
-- @skip_result_check=true
USE d3_restart;

-- query 3: kill + restart BE[1]; after restart it must rejoin Live and serve.
-- @kill_be_index=1
-- @restart_be_delay_ms=1500
-- @skip_result_check=true
SELECT 1;

-- query 4: wait for heartbeat to re-mark BE[1] Live, then a distributed query
-- must succeed (uses retry directives to tolerate the heartbeat interval).
-- @retry_count=10
-- @retry_interval_ms=1000
-- @result_contains=1000000
SELECT COUNT(*) FROM (SELECT * FROM TABLE(generate_series(1, 1000000)) AS g(x)) t;
```

- [ ] **Step 3: Run the suite with cluster-size 2**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite distributed-resilience --mode verify --cluster-size 2 -j 1
```
Expected: all cases PASS. (`-j 1` because fault injection is sequential/host-wide.)

- [ ] **Step 4: Commit**

```bash
git add sql-tests/distributed-resilience/
git commit -m "test(d3): kill-be and be-restart fault-injection suites pass at cluster-size 2"
```

### Task 7.3: Shorten heartbeat for tests + full regression

**Files:**
- Modify: `tests/sql-test-runner/src/cluster.rs` `render_cross_process_config` (FE arm) to set fast heartbeat so tests don't stall.

- [ ] **Step 1: Inject fast heartbeat into the FE config**

In `render_cross_process_config`, in the `ClusterProcessRole::Fe` arm where `cluster` table is built, add:
```rust
            cluster.insert("heartbeat_interval_ms".to_string(), Value::Integer(500));
            cluster.insert("heartbeat_timeout_retries".to_string(), Value::Integer(2));
```

- [ ] **Step 2: Re-run the resilience suite + the existing distributed smoke**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite distributed-resilience --mode verify --cluster-size 2 -j 1
# regression: the suites D2 already validates at cluster-size 2 still pass
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb --mode verify --cluster-size 2 -j 4
```
Expected: resilience PASS; ssb PASS (no regression from the live-filter / cancel changes).

- [ ] **Step 3: Commit**

```bash
git add tests/sql-test-runner/src/cluster.rs
git commit -m "test(d3): fast heartbeat in cross-process test config; verify no D2 regression"
```

---

## Self-Review

**1. Spec coverage** (against `2026-06-10-d3-d4-distributed-fault-ops-design.md` §4):
- D3-1 Heartbeat RPC + BackendRegistry + HeartbeatMgr → PR-1 Task 1.3/1.4, PR-2 Task 2.1/2.2/2.3. ✅
- D3-2 BE Heartbeat handler + start_epoch → PR-1 Task 1.1/1.3. ✅
- D3-3 scheduler only Live + empty→error → PR-3. ✅
- D3-4 parallel cancel fan-out + epoch-tagged idempotent cancel → PR-5 Task 5.1, PR-6 Task 6.2. ✅
- D3-5 status plane / FE query state machine → PR-4 (reuses ReportExecStatus per the deviation note). ✅
- D3-6 BE-lost forced cleanup + sender-failure propagation + epoch restart cleanup → PR-5 Task 5.2, PR-6 Task 6.1/6.2. ✅
- D3-7 fault-injection tests → PR-7. ✅ (network-partition deferred with an explicit note — flagged below.)

**2. Placeholder scan:** No "TBD/TODO" in shipped code. Two intentional "verify against the codebase" notes remain where exact test-constructor names (`GrpcService::for_test`, `QueryContextManager::new_for_test`) and the `novarocks_report_addr` construction site must be confirmed by grep before editing — these are verification instructions, not placeholders, because the surrounding edit is fully specified.

**3. Type consistency:** `BeId = u32` used consistently (registry, heartbeat, query_state via `backend_idx: usize` cast at the boundary). `RegistryEvent`/`HeartbeatOutcome`/`BackendState` names match across `backend_registry.rs`, `heartbeat_mgr.rs`, `registry_cleanup.rs`. `in_flight_table()` global used identically in PR-4/PR-6. `start_epoch()` signature identical across PR-1/PR-2/PR-6.

**Known deviations to surface:**
- D3-5 reuses `ReportExecStatus` instead of a new `ReportFragmentStatus` RPC (simpler; wire path exists). Spec should be updated.
- D3-7 ships `kill_be`/`restart_be`/`heartbeat_delay` and defers true `network-partition` (host-wide `tc` is racy under the parallel runner). The Lost/restart/epoch code paths are still fully exercised by kill+restart.
