# D2 多 BE 并行执行 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 NovaRocks standalone 从 D1 的 1FE+1BE 升级到 1FE+N BE 真正分布式执行，1FE+2BE 跑通 SSB 全套和 TPC-H Q5/Q9 byte-identical（vs all-in-one），1FE+3BE SSB 抽样通过。

**Architecture:** 新建 `FragmentScheduler` 模块决定每个 (fragment, instance) 落到哪个 BE；`FragmentDispatcher` trait 加 `backend_idx` 首参；`RemoteDispatcher` 持 `Vec<NovaRocksGrpcRemoteClient>` 按 idx 路由；scheduler 按 StarRocks 同款"instance 跟随上游 partition type"规则推断 instance count，集中填 destinations / RF prober_params / per_exch_num_senders；scan splits 通过 connector-first 已有的 `ConnectorScanPlanner.to_thrift_scan` 接口按 round-robin 切给 N 个 instance；数据流和 wire protocol 跟 StarRocks BE byte-identical（D1 决策延续）。

**Tech Stack:** Rust, tonic gRPC，复用现有 thrift `TExecPlanFragmentParams` / `TPlanFragmentDestination` / `TRuntimeFilterProberParams` / `TScanRangeParams`，复用 D1 的 `FragmentDispatcher` trait 和 `InProcessDispatcher`/`RemoteDispatcher`，复用 connector-first 的 `ConnectorScanPlanner` + `PlannedConnectorScan`。

**Spec:** [docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md](../specs/2026-05-28-distributed-multi-be-execution-design.md)

**Roadmap 任务 brief:** [NovaRocks TODO/distributed-multi-be-execution.md](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-multi-be-execution.md)

**前置 commit**：D1（`fa835350`）+ connector-first 阶段 1+2+3（`14bdefdb`）+ Iceberg `to_thrift_scan` node+ranges 迁移（`f6fcfcb2` / #205，已覆盖原 PR-0 范围并 super-set）已合入 main。

---

## Prerequisites

### 通用准备

```bash
# 当前在 D2 工作分支，HEAD 应已 rebase 到 f6fcfcb2 之上
git log -1 --oneline origin/main
# 期望：f6fcfcb2 docs(iceberg): spec + plan for to_thrift_scan node+ranges migration (#205)

cargo build
```

启动 Iceberg + MinIO 本地 fixture（PR-5 acceptance 用，PR-1 ~ PR-4 不需要）：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

辅助变量：

```bash
SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
```

### 命名约定 / 类型契约（贯穿所有 PR）

| 名称 | 类型 | 出现在 PR | 备注 |
|---|---|---|---|
| `FragmentScheduler` | struct | PR-3 | 持 `Vec<SocketAddr>` |
| `FragmentInstancePlacement` | struct | PR-3 | 单个 instance 的 placement |
| `SchedulingPlan` | struct | PR-3 | scheduler.assign 的输出 |
| `InFlightTracker` | struct | PR-4 | 按 backend_idx 分组的 in-flight finst_id |
| `backend_idx` | usize | 全部 | 0-based index into `[cluster].backends` |
| `assign` / `fill_destinations` / `fill_runtime_filter_params` / `fill_per_exch_num_senders` | 方法 | PR-3 | scheduler 的 4 个 stage |

---

## PR 概览

每个 PR 独立 review、独立可回滚，本身是有意义的"工作软件"。

| PR | 主题 | 输入 | 输出 | 验收 |
|---|---|---|---|---|
| ~~PR-0~~ | ~~Iceberg `to_thrift_scan` 填实~~ | — | **已由 main #205 (`f6fcfcb2`) 完成，且 super-set 了本计划的范围** —— 详见下方 PR-0 段落 | 已在 main 上验证 |
| PR-1 | Config 放宽 `backends.len() >= 1` | main (post #205) | TOML 接受多 backend；启动期 dial 全部 BE | 单测 + all-in-one 不回归 |
| PR-2 | Dispatcher trait + RemoteDispatcher 多 backend | PR-1 | trait 加 backend_idx；RemoteDispatcher 持 Vec<client>；删 exchange_addr | 单测 + D1 cross-process smoke 不回归 |
| PR-3 | FragmentScheduler 模块 | PR-2 | scheduler 4 个 fill_* 函数 + Placement 数据类型 | 单测全通 |
| PR-4 | Coordinator 集成 + be_number + InFlightTracker | PR-3 | coordinator 走 scheduler；data_stream_sink 不再硬编码；多 BE 实际跑通 | tests/cluster_mvp_d2 smoke 通过 |
| PR-5 | sql-test-runner cluster-size N + acceptance | PR-4 | SSB / TPC-H Q5/Q9 / iceberg-rest 在 1FE+2BE 下 byte-identical | D2 主验收门槛通过 |

---

# PR-0: Iceberg `ConnectorScanPlanner::to_thrift_scan` 填实 — **已被 main 覆盖（OBSOLETE）**

> **状态**：原计划 PR-0 的全部工作已在 main `f6fcfcb2`（PR #205 "to_thrift_scan node+ranges migration"）中完成，且**做得比本计划更多**。本 PR 跳过，直接从 PR-1 开始。
>
> **main 已具备的内容（验证锚点）**：
> - [src/connector/iceberg/scan_planner.rs:142-156](../../../src/connector/iceberg/scan_planner.rs) — `IcebergConnectorScanPlanner::to_thrift_scan` 已实现
> - [src/connector/iceberg/scan_planner.rs:238](../../../src/connector/iceberg/scan_planner.rs) — `build_hdfs_scan_range_params_for_file` 已从 nodes.rs 迁入
> - [src/sql/codegen/nodes.rs:80](../../../src/sql/codegen/nodes.rs) + [:605](../../../src/sql/codegen/nodes.rs) — Iceberg codegen 全部走 `planner.to_thrift_scan`
> - 测试：`to_thrift_scan_returns_hdfs_scan_node_and_scan_ranges` 已在 scan_planner.rs 中
>
> **与原 PR-0 设计的偏差（重要）**：原计划期望 Iceberg `to_thrift_scan` 返回 `ThriftScanPlan { node: None, scan_ranges }`，让 codegen 继续负责构造 `TPlanNode`。但 PR #205 把 **node 构造也搬进了 `to_thrift_scan`**，于是返回的是 `ThriftScanPlan { node: Some(THdfsScanNode), scan_ranges }`。这影响 PR-3 任务 3.4 的 scan splits 切分策略（详见任务 3.4 内的"⚠️ 设计调整"段落）。
---

# PR-1: `ClusterConfig.backends.len() >= 1` 放宽

**范围**：D1 强制 `backends.len() == 1`，D2 改为 `>= 1`。校验放宽 + 启动期 dial 全部 BE。

**输入**：D2 工作分支 rebase 到 `origin/main`（HEAD ≥ `f6fcfcb2`）。原计划 PR-0 已由 #205 完成，直接从这里起跑。

**输出**：
- `[cluster].backends = ["a:1", "b:2", "c:3"]` 解析通过
- `role = "fe"` 时启动期主动 dial 每个 BE，任一失败立即报错
- 重复 backend 地址报错
- `role = "all-in-one"` / `role = "be"` 仍然必须 `backends.is_empty()`

**验证**：
```bash
cargo build && cargo test --lib --package novarocks -- common::app_config::cluster
# D1 单机模式不回归
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
sleep 2 && mysql -h 127.0.0.1 -P 9030 -e 'SELECT 1' && kill %1
```

**回滚**：`git revert <pr-1-commit>`。

---

### 任务 1.1：写 `len() >= 1` 解析的失败测试

**Files:**
- Modify: `src/common/app_config.rs`（测试模块）

- [ ] **Step 1.1.1：加测试**

```rust
#[test]
fn test_cluster_role_fe_with_three_backends_passes() {
    let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.2:9070", "10.0.0.3:9070"]
"#;
    let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse fe with 3 backends");
    cfg.cluster.validate().expect("3 backends should pass D2 validate");
}

#[test]
fn test_cluster_role_fe_rejects_duplicate_backends() {
    let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.1:9070"]
"#;
    let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
    let err = cfg.cluster.validate().expect_err("duplicate backends should fail");
    assert!(err.contains("duplicate") || err.contains("10.0.0.1:9070"));
}

#[test]
fn test_cluster_role_fe_rejects_malformed_backend() {
    let toml = r#"
[cluster]
role = "fe"
backends = ["not-a-socket-addr"]
"#;
    let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
    let err = cfg.cluster.validate().expect_err("malformed addr should fail");
    assert!(err.contains("not-a-socket-addr") || err.contains("invalid"));
}

#[test]
fn test_cluster_role_fe_empty_backends_still_rejected() {
    let toml = r#"
[cluster]
role = "fe"
backends = []
"#;
    let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
    let err = cfg.cluster.validate().expect_err("empty backends still rejected");
    assert!(err.contains("at least one") || err.contains("backends"));
}
```

注意：把 D1 的 `test_cluster_role_fe_requires_exactly_one_backend_v1` 测试**删除**或改成"至少一个"的版本，因为 D2 改了规则。

- [ ] **Step 1.1.2：运行测试确认 3-backends 案例 fail**

```bash
cargo test --lib --package novarocks -- common::app_config::tests::test_cluster_role_fe_with_three_backends_passes
```

期望：fail（D1 的 `len != 1` 检查会拒绝 3 backends）。

---

### 任务 1.2：放宽 `validate()`

**Files:**
- Modify: `src/common/app_config.rs::ClusterConfig::validate`

- [ ] **Step 1.2.1：替换 validate 内容**

定位 `impl ClusterConfig` 中 `validate` 方法的 `ClusterRole::Fe` 分支，从：

```rust
ClusterRole::Fe => {
    if self.backends.len() != 1 {
        return Err(format!("D1 v1 only supports exactly one backend, got {}", self.backends.len()));
    }
}
```

改为：

```rust
ClusterRole::Fe => {
    if self.backends.is_empty() {
        return Err("role=fe requires at least one backend in [cluster].backends".into());
    }
    // 校验每个 backend 字符串是合法 SocketAddr
    for b in &self.backends {
        b.parse::<std::net::SocketAddr>()
            .map_err(|e| format!("invalid backend addr '{}': {}", b, e))?;
    }
    // 校验无重复
    let mut seen = std::collections::HashSet::new();
    for b in &self.backends {
        if !seen.insert(b.clone()) {
            return Err(format!("duplicate backend in [cluster].backends: {}", b));
        }
    }
}
```

`ClusterRole::Be` 和 `ClusterRole::AllInOne` 分支保持 D1 现有的"backends 必须为空"逻辑。

- [ ] **Step 1.2.2：运行测试确认全通**

```bash
cargo test --lib --package novarocks -- common::app_config
```

期望：全部 cluster 相关测试通过。

---

### 任务 1.3：启动期 dial 全部 BE

**Files:**
- Modify: `src/main.rs`（FE 启动期 dial 路径）

- [ ] **Step 1.3.1：定位 D1 的 FE 启动期 dial 逻辑**

```bash
grep -n "dial\|connect_blocking\|TcpStream::connect" src/main.rs | head -10
```

D1 在 FE role 启动时对**单个** backend 做 dial probe。D2 改为遍历全部。

- [ ] **Step 1.3.2：先写失败测试**

在 `src/main.rs` 的测试模块加：

```rust
#[test]
fn test_fe_startup_dials_all_backends() {
    use std::net::TcpListener;
    // 在两个本地端口起 listener 模拟 2 BE
    let l1 = TcpListener::bind("127.0.0.1:0").unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").unwrap();
    let p1 = l1.local_addr().unwrap().port();
    let p2 = l2.local_addr().unwrap().port();
    drop(l1);
    drop(l2);
    // 端口立即释放后 dial 必然 connection refused（fail-fast 触发）
    let backends = vec![
        format!("127.0.0.1:{}", p1),
        format!("127.0.0.1:{}", p2),
    ];
    let err = crate::probe_all_backends(&backends).expect_err("all backends down should fail");
    assert!(err.contains("127.0.0.1") && (err.contains(&p1.to_string()) || err.contains(&p2.to_string())));
}

#[test]
fn test_fe_startup_reports_first_unreachable_backend() {
    let live = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let live_port = live.local_addr().unwrap().port();
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);
    let backends = vec![
        format!("127.0.0.1:{}", live_port),
        format!("127.0.0.1:{}", dead_port),
    ];
    let err = crate::probe_all_backends(&backends).expect_err("one down should fail");
    assert!(err.contains(&dead_port.to_string()),
        "error message must mention which backend failed");
    drop(live);  // 防止 listener leak 影响其他测试
}
```

- [ ] **Step 1.3.3：运行测试确认 fail**

```bash
cargo test --lib --package novarocks -- main::test_fe_startup_dials_all_backends
```

期望：fail（`probe_all_backends` 函数不存在或只 dial 一个）。

- [ ] **Step 1.3.4：实现 `probe_all_backends`**

在 `src/main.rs` 加：

```rust
/// FE role startup: actively dial each backend's gRPC address.
/// Returns Err on the first unreachable backend, with the address in the
/// error message for fast diagnosis.
pub(crate) fn probe_all_backends(backends: &[String]) -> Result<(), String> {
    use std::net::TcpStream;
    use std::time::Duration;
    for addr_str in backends {
        let addr: std::net::SocketAddr = addr_str.parse()
            .map_err(|e| format!("invalid backend addr '{}': {}", addr_str, e))?;
        TcpStream::connect_timeout(&addr, Duration::from_secs(3))
            .map_err(|e| format!("failed to dial backend {} ({}): {}",
                                 backends.iter().position(|b| b == addr_str).unwrap_or(0),
                                 addr_str, e))?;
    }
    Ok(())
}
```

注：原 D1 的单 backend dial 调用点应该改为调 `probe_all_backends(&cfg.cluster.backends)`。

- [ ] **Step 1.3.5：找到 D1 调用点并替换**

```bash
grep -rn "backends\[0\]\|backends\.first" src/main.rs src/server/mod.rs
```

把所有"只 dial 第一个 backend"的地方改成 `probe_all_backends(&backends)`。

- [ ] **Step 1.3.6：跑测试确认通过**

```bash
cargo test --lib --package novarocks -- main::test_fe_startup
```

期望：通过。

---

### 任务 1.4：跑 all-in-one 回归

- [ ] **Step 1.4.1：默认启动确认**

```bash
cargo build
LOG=/tmp/d2-pr1-smoke.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server --port 9030 >"$LOG" 2>&1 &
SRV=$!
for i in $(seq 1 30); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  sleep 1
done
mysql -h 127.0.0.1 -P 9030 -e 'SELECT 1'
kill $SRV
```

期望：返回 `1`。

- [ ] **Step 1.4.2：跑 SSB 回归**

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
$SQLT --suite ssb --mode verify
```

期望：通过。

---

### 任务 1.5：PR-1 commit

- [ ] **Step 1.5.1：commit**

```bash
git add src/common/app_config.rs src/main.rs
git commit -m "$(cat <<'EOF'
feat(cluster): relax backends.len() == 1 to >= 1 (D2 PR-1)

- ClusterConfig::validate accepts >= 1 backend for role=fe
- Reject empty backends, malformed SocketAddr, duplicate entries
- Startup probe_all_backends dials every backend with 3s timeout;
  error message identifies the failing backend index + address
- role=be / role=all-in-one validation unchanged

Refs: docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md
EOF
)"
```

---

# PR-2: `FragmentDispatcher` Trait + `RemoteDispatcher` 多 backend

**范围**：trait 加 `backend_idx` 首参；删 `exchange_addr()`；加 `backend_count()`。`RemoteDispatcher` 改持 `Vec<NovaRocksGrpcRemoteClient>`。`InProcessDispatcher` 在 backend_idx != 0 时 Err。

**输入**：PR-1 已合并。

**输出**：
- `src/runtime/dispatcher.rs` trait + InProcess + Remote 都改造完
- 所有 D1 调用点临时传 `backend_idx = 0`（PR-3/4 才真正使用 idx）
- D1 cross-process smoke 不回归

**验证**：
```bash
cargo test --test cluster_mvp
cargo test --lib --package novarocks -- runtime::dispatcher
```

**回滚**：`git revert <pr-2-commit>`，PR-3 ~ PR-5 全部依赖此 PR，回滚需配套撤回它们。

---

### 任务 2.1：trait 签名改造

**Files:**
- Modify: `src/runtime/dispatcher.rs`（trait 定义）

- [ ] **Step 2.1.1：定位现有 trait**

```bash
grep -n "pub trait FragmentDispatcher\|fn submit_fragment\|fn fetch_result\|fn cancel_fragments\|fn exchange_addr" src/runtime/dispatcher.rs
```

- [ ] **Step 2.1.2：写新签名 + backend_count 测试**

在 `src/runtime/dispatcher.rs` 测试模块加：

```rust
#[test]
fn in_process_dispatcher_rejects_nonzero_backend_idx() {
    let d = InProcessDispatcher::new("127.0.0.1", 9070);
    let params = crate::runtime::exec_params::test_helpers::empty_fragment_params(
        crate::internal_service::TUniqueId { hi: 0, lo: 1 },
    );
    let err = d.submit_fragment(1, params).expect_err("InProc only supports idx=0");
    assert!(err.contains("backend_idx") || err.contains("InProcess"));
}

#[test]
fn in_process_dispatcher_backend_count_is_one() {
    let d = InProcessDispatcher::new("127.0.0.1", 9070);
    assert_eq!(d.backend_count(), 1);
}
```

- [ ] **Step 2.1.3：改 trait 定义**

替换 trait：

```rust
pub trait FragmentDispatcher: Send + Sync + 'static {
    fn submit_fragment(
        &self,
        backend_idx: usize,
        params: TExecPlanFragmentParams,
    ) -> Result<(), String>;

    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String>;

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[TUniqueId]);

    fn backend_count(&self) -> usize;
}
```

**注意**：删除原 trait 上的 `fn exchange_addr() -> std::net::SocketAddr`。

---

### 任务 2.2：InProcessDispatcher 改造

**Files:**
- Modify: `src/runtime/dispatcher.rs::InProcessDispatcher`

- [ ] **Step 2.2.1：替换 submit_fragment**

```rust
impl FragmentDispatcher for InProcessDispatcher {
    fn submit_fragment(&self, backend_idx: usize, params: TExecPlanFragmentParams)
        -> Result<(), String>
    {
        if backend_idx != 0 {
            return Err(format!(
                "InProcessDispatcher only supports backend_idx=0, got {}",
                backend_idx
            ));
        }
        // 原 D1 路径完全保留
        let fragment = params.fragment.ok_or("missing fragment")?;
        let desc_tbl = params.desc_tbl;
        let exec_params = params.params.ok_or("missing exec_params")?;
        let query_options = params.query_options;
        let pipeline_dop = compute_pipeline_dop();
        std::thread::spawn(move || {
            let _ = crate::lower::fragment::execute_fragment(
                &fragment, desc_tbl.as_ref(), Some(&exec_params),
                query_options.as_ref(), None, pipeline_dop,
                None, None, None, None, None, None, None,
            );
        });
        Ok(())
    }

    fn fetch_result(&self, backend_idx: usize, finst_id: TUniqueId, max_wait_ms: i64)
        -> Result<FetchOutcome, String>
    {
        debug_assert_eq!(backend_idx, 0, "InProcessDispatcher only supports backend_idx=0");
        // 原 D1 路径完全保留
        let r = crate::runtime::result_buffer::wait_fetch(finst_id, max_wait_ms);
        Ok(map_try_fetch_to_outcome(r))
    }

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[TUniqueId]) {
        debug_assert_eq!(backend_idx, 0, "InProcessDispatcher only supports backend_idx=0");
        for id in finst_ids {
            crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
            crate::runtime::result_buffer::cancel(*id);
        }
    }

    fn backend_count(&self) -> usize { 1 }
}
```

- [ ] **Step 2.2.2：跑 InProcessDispatcher 测试确认通过**

```bash
cargo test --lib --package novarocks -- runtime::dispatcher::in_process_dispatcher
```

期望：所有 InProc 测试通过（含 PR-2 新增的 reject_nonzero / backend_count 测试）。

---

### 任务 2.3：RemoteDispatcher 改 Vec<client>

**Files:**
- Modify: `src/runtime/dispatcher.rs::RemoteDispatcher`

- [ ] **Step 2.3.1：先写 multi-backend 单测**

```rust
#[test]
fn remote_dispatcher_holds_multiple_clients() {
    // 起两个 mock gRPC server（复用 D1 spawn_mock_server helper）
    let (addr1, _h1) = spawn_mock_server();
    let (addr2, _h2) = spawn_mock_server();
    let dispatcher = RemoteDispatcher::new(&[addr1, addr2]).expect("connect");
    assert_eq!(dispatcher.backend_count(), 2);
}

#[test]
fn remote_dispatcher_routes_submit_by_backend_idx() {
    // 起两个 mock，分别返回不同的 message
    let (addr1, _h1) = spawn_mock_server_with_response("mock-be-0");
    let (addr2, _h2) = spawn_mock_server_with_response("mock-be-1");
    let dispatcher = RemoteDispatcher::new(&[addr1, addr2]).expect("connect");
    let params = crate::runtime::exec_params::test_helpers::empty_fragment_params(
        crate::internal_service::TUniqueId { hi: 0, lo: 1 }
    );
    // submit 到 backend_idx=1，验证去了 addr2 不是 addr1
    // 通过 mock 收到的 thrift bytes / 调用计数验证
    dispatcher.submit_fragment(1, params).expect("submit to idx=1");
    // 假设 mock 暴露 last_submit_count() 之类
    // 略：具体 mock helpers 在 PR-2 task 内现写
}

#[test]
fn remote_dispatcher_returns_err_on_out_of_range_idx() {
    let (addr, _h) = spawn_mock_server();
    let dispatcher = RemoteDispatcher::new(&[addr]).expect("connect");
    let params = crate::runtime::exec_params::test_helpers::empty_fragment_params(
        crate::internal_service::TUniqueId { hi: 0, lo: 1 }
    );
    let err = dispatcher.submit_fragment(5, params).expect_err("idx out of range");
    assert!(err.contains("backend_idx") && err.contains("5"));
}
```

- [ ] **Step 2.3.2：替换 RemoteDispatcher struct + impl**

```rust
pub struct RemoteDispatcher {
    clients: Vec<crate::service::grpc_client::NovaRocksGrpcRemoteClient>,
    addrs: Vec<std::net::SocketAddr>,
}

impl RemoteDispatcher {
    pub fn new(backends: &[std::net::SocketAddr]) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("RemoteDispatcher requires at least one backend".into());
        }
        let mut clients = Vec::with_capacity(backends.len());
        for addr in backends {
            let c = crate::service::grpc_client::NovaRocksGrpcRemoteClient::connect_blocking(*addr)
                .map_err(|e| format!("connect to {}: {}", addr, e))?;
            clients.push(c);
        }
        Ok(Self {
            clients,
            addrs: backends.to_vec(),
        })
    }

    fn check_idx(&self, backend_idx: usize) -> Result<(), String> {
        if backend_idx >= self.clients.len() {
            return Err(format!(
                "backend_idx {} out of range (have {} backends)",
                backend_idx, self.clients.len()
            ));
        }
        Ok(())
    }

    pub fn addr_of(&self, backend_idx: usize) -> Option<std::net::SocketAddr> {
        self.addrs.get(backend_idx).copied()
    }
}

impl FragmentDispatcher for RemoteDispatcher {
    fn submit_fragment(&self, backend_idx: usize, params: TExecPlanFragmentParams)
        -> Result<(), String>
    {
        self.check_idx(backend_idx)?;
        let bytes = serialize_thrift_exec_plan_fragment_params(&params)?;
        let req = crate::service::SubmitFragmentRequest {
            exec_plan_fragment_params_thrift: bytes,
        };
        let resp = self.clients[backend_idx].blocking_submit_fragment(req)
            .map_err(|e| format!("submit_fragment to BE[{}] ({}): {}",
                                 backend_idx, self.addrs[backend_idx], e))?;
        if resp.status_code != 0 {
            return Err(format!(
                "submit_fragment to BE[{}] ({}): status={} message={}",
                backend_idx, self.addrs[backend_idx], resp.status_code, resp.message
            ));
        }
        Ok(())
    }

    fn fetch_result(&self, backend_idx: usize, finst_id: TUniqueId, max_wait_ms: i64)
        -> Result<FetchOutcome, String>
    {
        self.check_idx(backend_idx)?;
        let req = crate::service::FetchResultRequest {
            finst_id: Some(crate::service::PUniqueId { hi: finst_id.hi, lo: finst_id.lo }),
            max_wait_ms,
        };
        let resp = self.clients[backend_idx].blocking_fetch_result(req)
            .map_err(|e| format!("fetch_result from BE[{}] ({}): {}",
                                 backend_idx, self.addrs[backend_idx], e))?;
        Ok(map_fetch_response_to_outcome(resp))
    }

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[TUniqueId]) {
        if self.check_idx(backend_idx).is_err() {
            return;  // 越界静默失败（best-effort）
        }
        let req = crate::service::CancelFragmentRequest {
            finst_ids: finst_ids.iter().map(|id| crate::service::PUniqueId {
                hi: id.hi, lo: id.lo
            }).collect(),
            reason: "fe-initiated".to_string(),
        };
        let _ = self.clients[backend_idx].blocking_cancel_fragment(req);
    }

    fn backend_count(&self) -> usize { self.clients.len() }
}
```

- [ ] **Step 2.3.3：删除 trait 和 RemoteDispatcher 上的旧 `exchange_addr` 方法**

```bash
grep -n "fn exchange_addr\|exchange_addr()" src/runtime/dispatcher.rs
```

把所有 `exchange_addr` 相关代码删除。

---

### 任务 2.4：D1 调用点临时迁移

**Files:**
- Modify: `src/runtime/coordinator.rs`（D1 调 dispatcher.submit/fetch/cancel 的位置）
- Modify: `src/engine/mod.rs`（dispatcher_for_role）

- [ ] **Step 2.4.1：grep 所有 dispatcher 调用点**

```bash
grep -rn "\.submit_fragment(\|\.fetch_result(\|\.cancel_fragments(\|\.exchange_addr(" src/
```

- [ ] **Step 2.4.2：所有调用点临时传 `backend_idx = 0`**

D1 调用点一律改为：

```rust
// 旧
self.dispatcher.submit_fragment(params)?;
// 新（PR-2 临时）
self.dispatcher.submit_fragment(0, params)?;

// 旧
self.dispatcher.fetch_result(finst_id, wait_ms)
// 新
self.dispatcher.fetch_result(0, finst_id, wait_ms)

// 旧
self.dispatcher.cancel_fragments(&ids);
// 新
self.dispatcher.cancel_fragments(0, &ids);
```

**注意**：PR-4 才会让 coordinator 真正按 scheduler 的 placement 传 backend_idx。PR-2 只让代码能编译 + D1 行为不变。

- [ ] **Step 2.4.3：定位并替换 `exchange_addr` 调用点**

```bash
grep -rn "exchange_addr" src/
```

通常会在 coordinator.rs:154 附近，把 D1 用 `dispatcher.exchange_addr()` 填 destinations 的逻辑改为：

```rust
// 旧
let brpc_addr = self.dispatcher.exchange_addr();
// 新（PR-2 临时；PR-4 scheduler 会取代）
let brpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9070));
// 上面只是 PR-2 让代码能编译的桩；PR-3/4 引入 scheduler 后这里完全删掉
```

或者更稳：把这段逻辑暂时 `#[cfg(debug_assertions)] todo!("PR-4 replaces this with scheduler placements");`，让 D1 不再走这条路径但 release build 不带 placeholder。

更安全的方式：保留 D1 行为——把"在 coordinator 里填 destinations"那块代码用一个临时 helper 包起来，临时返回固定的本机 addr。

- [ ] **Step 2.4.4：`dispatcher_for_role` 改造（多 backend 构造）**

`src/engine/mod.rs:2717-2750` 的 D1 实现：

```rust
ClusterRole::Fe => {
    let n = cfg.cluster.backends.len();
    if n != 1 { return Err(format!("expected exactly one backend, got {}", n)); }
    let backend: std::net::SocketAddr = cfg.cluster.backends[0].parse()?;
    Ok(Arc::new(RemoteDispatcher::new(backend)))
}
```

改为：

```rust
ClusterRole::Fe => {
    if cfg.cluster.backends.is_empty() {
        return Err("role=fe requires non-empty cluster.backends".into());
    }
    let addrs: Vec<std::net::SocketAddr> = cfg.cluster.backends.iter()
        .map(|s| s.parse::<std::net::SocketAddr>())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("backend addr parse: {}", e))?;
    Ok(Arc::new(RemoteDispatcher::new(&addrs)?))
}
```

`RemoteDispatcher::new` 现在签名变了（从 `(SocketAddr)` 到 `(&[SocketAddr])`）。

- [ ] **Step 2.4.5：cargo build 修编译错**

```bash
cargo build 2>&1 | tee /tmp/d2-pr2-build.log | tail -30
```

依据错误逐个修。预期错误：
- `dispatcher.exchange_addr()` 不存在 → 删调用或用临时桩
- `RemoteDispatcher::new(addr)` 签名变了 → 改为 `new(&[addr])`
- `dispatcher.submit_fragment(params)` 签名变了 → 改为 `submit_fragment(0, params)`

---

### 任务 2.5：跑 D1 cross-process smoke 回归

- [ ] **Step 2.5.1：跑 D1 集成测试**

```bash
cargo test --test cluster_mvp
```

期望：D1 所有 6 个集成测试通过（cross_process_remote_dispatcher_smoke / submit_half_failure_cancels_submitted / mysql_disconnect_triggers_cancel / query_timeout_triggers_cancel / be_kill9_during_query_fails_cleanly / reserved_port_blocks_rebinding_until_release）。

- [ ] **Step 2.5.2：跑 dispatcher 单测**

```bash
cargo test --lib --package novarocks -- runtime::dispatcher
```

期望：通过（含 PR-2 新增的 multi-backend 测试）。

---

### 任务 2.6：PR-2 commit

- [ ] **Step 2.6.1：commit**

```bash
git add src/runtime/dispatcher.rs src/runtime/coordinator.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
refactor(runtime): FragmentDispatcher trait adds backend_idx; RemoteDispatcher holds Vec<client> (D2 PR-2)

- trait submit_fragment/fetch_result/cancel_fragments now take backend_idx
  as first parameter; add backend_count() query method; remove exchange_addr().
- InProcessDispatcher rejects backend_idx != 0 (debug_assert in hot paths,
  Err in submit_fragment).
- RemoteDispatcher holds Vec<NovaRocksGrpcRemoteClient> and Vec<SocketAddr>;
  routes by backend_idx; error messages include BE[idx] (addr).
- Existing D1 call sites temporarily pass backend_idx=0; PR-4 wires scheduler
  output for real backend_idx selection.
- D1 cluster_mvp integration tests still pass.

Refs: docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md
EOF
)"
```

---

# PR-3: `FragmentScheduler` 模块

**范围**：新建 `src/runtime/scheduler.rs`，含 `FragmentScheduler` + `FragmentInstancePlacement` + `SchedulingPlan` + `assign` / `fill_destinations` / `fill_runtime_filter_params` / `fill_per_exch_num_senders` 4 个 stage。

> **⚠️ 真实类型更正（执行时以此为准，下方任务里的代码草图基于一个不准确的心智模型，已被以下事实取代）：**
> - `FragmentBuildResult` / `FragmentEdge` / `FragmentId` 在 **`src/sql/codegen/mod.rs`**（不是 `coordinator.rs`），均为 `pub(crate)`，**不 derive Clone/Default**。`FragmentId = u32`。
> - `FragmentBuildResult` 字段：`fragment_id`、`plan: plan_nodes::TPlan`（**非 Option**，flat `.nodes: Vec<TPlanNode>`）、`exec_params: TPlanFragmentExecParams`（含 `per_node_scan_ranges: BTreeMap<TPlanNodeId, Vec<TScanRangeParams>>`）、`desc_tbl`、`output_sink`、`output_columns`、`cte_id`、`cte_exchange_nodes`、`query_global_dicts*`。**不含** `ResolvedTable`/`PlannedConnectorScan`。
> - `FragmentEdge` **没有** `partition_type` 字段；用 `edge.output_partition.type_`（`TDataPartition.type_: TPartitionType`）。还有 `edge_kind: FragmentEdgeKind { Stream, CteMulticast{cte_id} }`。
> - scan 节点类型：**没有 `ICEBERG_SCAN_NODE`**；用 `FILE_SCAN_NODE | HDFS_SCAN_NODE | LAKE_SCAN_NODE`（`ICEBERG_DELTA_SCAN_NODE=1000` 是 IVM delta，scan 切分不涉及）。
> - `TPlanFragmentDestination` / `TRuntimeFilterProberParams` **不 derive Default**（字段须显式构造）；后者两个字段都是 `Option<_>`。
> - runtime filter plan 真实类型 `RuntimeFilterPlanResult { all_filters: HashMap<i32,TRuntimeFilterDescription>, build_side_filters: HashMap<FragmentId,Vec<i32>>, probe_side_filters: HashMap<FragmentId,Vec<(i32 filter_id, i32 scan_node_id)>> }`。
>
> **⚠️ scan 切分定案：方案 C（partition 已建好的 ranges）。** scheduler **不** 重新调 `to_thrift_scan`、**不** 伪造 `ThriftScanContext`、**不** 给 `FragmentBuildResult` 加 plumbing 字段。改为把 codegen 已构造正确的 `fr.exec_params.per_node_scan_ranges[node]` 里的 `Vec<TScanRangeParams>` 按 `i % count` round-robin 切给 N 个 instance（无重无漏；单 instance 时全部进唯一 instance，与 all-in-one 完全一致）。因此 `assign` **不收** `ConnectorRegistry` 参数。理由：scan ranges 的正确构造依赖 `min_max_predicates`/`change_op_slot`/`cloud_properties`，这些只有 codegen 阶段有；scheduler 重算会丢失它们产出错误 ranges。任务 3.4 的"重调 to_thrift_scan"代码作废，以方案 C 取代。

**输入**：PR-2 已合并。

**输出**：
- 完整 scheduler 模块 + 单元测试
- 没人调它（PR-4 才接进 coordinator）

**验证**：
```bash
cargo test --lib --package novarocks -- runtime::scheduler
```

**回滚**：`git revert <pr-3-commit>`，没下游依赖直接撤。

---

### 任务 3.1：scheduler.rs 骨架 + 类型定义

**Files:**
- Create: `src/runtime/scheduler.rs`
- Modify: `src/runtime/mod.rs`（pub mod scheduler）

- [ ] **Step 3.1.1：新建文件骨架**

```rust
//! Fragment scheduler.
//!
//! Decides which BE each (fragment, instance) runs on, splits scan ranges,
//! and fills destinations / runtime_filter_prober_params / per_exch_num_senders.
//! Scheduler is a pure decision layer; submit is done by coordinator
//! using the produced SchedulingPlan.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use crate::internal_service::{TExecPlanFragmentParams, TPlanFragmentDestination, TUniqueId};
use crate::planner::TPlanFragment;
use crate::runtime::coordinator::{FragmentBuildResult, FragmentEdge, FragmentId};

#[derive(Clone, Debug)]
pub struct FragmentInstancePlacement {
    pub fragment_id: FragmentId,
    pub instance_index: usize,
    pub finst_id: TUniqueId,
    pub backend_idx: usize,
    pub scan_ranges: BTreeMap<i32 /*plan_node_id*/, Vec<crate::internal_service::TScanRangeParams>>,
    pub destinations: Vec<TPlanFragmentDestination>,
    pub runtime_filter_prober_params:
        BTreeMap<i32 /*filter_id*/, Vec<crate::internal_service::TRuntimeFilterProberParams>>,
    pub per_exch_num_senders: BTreeMap<i32 /*exchange_node_id*/, i32>,
}

#[derive(Debug)]
pub struct SchedulingPlan {
    pub by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub root_finst_id: TUniqueId,
    pub root_backend_idx: usize,
}

pub struct FragmentScheduler {
    backends: Vec<SocketAddr>,
}

impl FragmentScheduler {
    pub fn new(backends: Vec<SocketAddr>) -> Self {
        Self { backends }
    }

    pub fn backends(&self) -> &[SocketAddr] {
        &self.backends
    }
}
```

- [ ] **Step 3.1.2：注册模块**

`src/runtime/mod.rs` 加：

```rust
pub mod scheduler;
```

- [ ] **Step 3.1.3：cargo build 确认编译**

```bash
cargo build
```

期望：通过（仅 dead_code warn）。

---

### 任务 3.2：`assign` instance_count 规则

**Files:**
- Modify: `src/runtime/scheduler.rs`

- [ ] **Step 3.2.1：写 instance_count 测试**

```rust
#[cfg(test)]
mod assign_instance_count_tests {
    use super::*;

    fn fake_scan_fragment(fid: FragmentId) -> FragmentBuildResult {
        // 构造一个最小的 fragment：plan tree 含一个 TPlanNodeType::HDFS_SCAN_NODE
        // 略，具体在 test_helpers 模块构造
        unimplemented!()
    }

    fn fake_non_scan_fragment(fid: FragmentId) -> FragmentBuildResult {
        // plan tree 全是非 scan 节点（Aggregate / Project）
        unimplemented!()
    }

    fn fake_edge(src: FragmentId, tgt: FragmentId, p: crate::partitions::TPartitionType) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: src,
            target_fragment_id: tgt,
            target_exchange_node_id: 100,
            partition_type: p,
            ..Default::default()
        }
    }

    #[test]
    fn scan_fragment_gets_n_instances() {
        let scheduler = FragmentScheduler::new(vec![
            "127.0.0.1:9070".parse().unwrap(),
            "127.0.0.1:9071".parse().unwrap(),
            "127.0.0.1:9072".parse().unwrap(),
        ]);
        let fragments = vec![fake_scan_fragment(0)];
        let edges = vec![];
        let plan = scheduler.assign(
            &fragments, &edges,
            TUniqueId { hi: 0, lo: 0 },
            mock_connector_registry(),
        ).expect("ok");
        assert_eq!(plan.by_fragment[&0].len(), 3);  // N = 3 backends
    }

    #[test]
    fn hash_consumer_inherits_upstream_n() {
        let scheduler = FragmentScheduler::new(vec![
            "127.0.0.1:9070".parse().unwrap(),
            "127.0.0.1:9071".parse().unwrap(),
        ]);
        // F0 (scan) → HASH → F1 (consumer)
        let fragments = vec![
            fake_scan_fragment(0),
            fake_non_scan_fragment(1),
        ];
        let edges = vec![fake_edge(0, 1, crate::partitions::TPartitionType::HashPartitioned)];
        let plan = scheduler.assign(&fragments, &edges, TUniqueId { hi: 0, lo: 0 }, mock_connector_registry())
            .expect("ok");
        assert_eq!(plan.by_fragment[&0].len(), 2);  // scan: N
        assert_eq!(plan.by_fragment[&1].len(), 2);  // HASH consumer: 跟随上游 N
    }

    #[test]
    fn unpartitioned_gather_is_one_instance() {
        let scheduler = FragmentScheduler::new(vec![
            "127.0.0.1:9070".parse().unwrap(),
            "127.0.0.1:9071".parse().unwrap(),
        ]);
        // F0 (scan) → UNPARTITIONED → F1 (root gather)
        let fragments = vec![
            fake_scan_fragment(0),
            fake_non_scan_fragment(1),  // 假设 fid=1 是 root
        ];
        let edges = vec![fake_edge(0, 1, crate::partitions::TPartitionType::Unpartitioned)];
        let plan = scheduler.assign(&fragments, &edges, TUniqueId { hi: 0, lo: 0 }, mock_connector_registry())
            .expect("ok");
        assert_eq!(plan.by_fragment[&0].len(), 2);  // scan: N
        assert_eq!(plan.by_fragment[&1].len(), 1);  // gather: 1
    }

    #[test]
    fn root_fragment_is_always_one_instance() {
        // 即使 root 有 HASH 输入，根 fragment 仍强制 1
        // 因为根 fragment 含 ResultSink，FE 只 fetch 一个 finst_id
        // 这是 spec § 9.3 的硬规则
    }
}
```

注：`mock_connector_registry()` / `fake_scan_fragment` / `fake_non_scan_fragment` 等 helpers 需要现写在测试模块里——构造最小可识别 scan 节点的 plan tree。具体构造请参考现有的 [`coordinator.rs::coord_tests`](src/runtime/coordinator.rs) 中的 `tiny_two_fragment_build_result` 写法。

- [ ] **Step 3.2.2：运行测试确认 fail**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::assign_instance_count_tests
```

期望：fail（`assign` 函数不存在）。

- [ ] **Step 3.2.3：实现 `assign` 的 instance_count 部分**

```rust
impl FragmentScheduler {
    pub fn assign(
        &self,
        fragments: &[FragmentBuildResult],
        edges: &[FragmentEdge],
        query_id: TUniqueId,
        connectors: &crate::connector::ConnectorRegistry,
    ) -> Result<SchedulingPlan, String> {
        let n = self.backends.len();
        if n == 0 {
            return Err("scheduler has no backends".into());
        }

        // Step 1: 拓扑排序（自底向上）
        let topo = topological_sort_bottom_up(fragments, edges)?;
        let root_fragment_id = identify_root_fragment(fragments, edges)?;

        // Step 2: 推断每个 fragment 的 instance_count
        let mut instance_count: BTreeMap<FragmentId, usize> = BTreeMap::new();
        for fid in &topo {
            let fr = fragments.iter().find(|f| f.fragment_id == *fid)
                .ok_or_else(|| format!("fragment {} not in build result", fid))?;
            let scan_plan_nodes = find_scan_plan_nodes(&fr.fragment);
            let is_scan = !scan_plan_nodes.is_empty();

            let count = if is_scan {
                n
            } else {
                let incoming = edges.iter().filter(|e| e.target_fragment_id == *fid);
                let mut max_parallel = 1usize;
                for edge in incoming {
                    use crate::partitions::TPartitionType::*;
                    let upstream_n = *instance_count.get(&edge.source_fragment_id).unwrap_or(&1);
                    match edge.partition_type {
                        HashPartitioned | BucketShuffleHashPartitioned => {
                            max_parallel = max_parallel.max(upstream_n);
                        }
                        _ => {}
                    }
                }
                max_parallel
            };
            instance_count.insert(*fid, count);
        }

        // 根 fragment 强制 1 instance
        instance_count.insert(root_fragment_id, 1);

        // Step 3 - Step 5 在后续任务实现（assign 函数返回 SchedulingPlan 之前需要做完）
        todo!("subsequent tasks 3.3 / 3.4 / 3.5 / 3.6 实现 backend_idx + scan_ranges + destinations + RF + per_exch_num_senders")
    }
}

fn topological_sort_bottom_up(
    fragments: &[FragmentBuildResult],
    edges: &[FragmentEdge],
) -> Result<Vec<FragmentId>, String> {
    // Kahn's algorithm: source → target，scan 节点先（无入边），root 最后
    let mut in_degree: BTreeMap<FragmentId, usize> = BTreeMap::new();
    for fr in fragments {
        in_degree.insert(fr.fragment_id, 0);
    }
    for edge in edges {
        *in_degree.get_mut(&edge.target_fragment_id).unwrap() += 1;
    }
    let mut ready: Vec<FragmentId> = in_degree.iter()
        .filter(|(_, d)| **d == 0).map(|(id, _)| *id).collect();
    let mut topo = Vec::new();
    while let Some(fid) = ready.pop() {
        topo.push(fid);
        for edge in edges.iter().filter(|e| e.source_fragment_id == fid) {
            let d = in_degree.get_mut(&edge.target_fragment_id).unwrap();
            *d -= 1;
            if *d == 0 { ready.push(edge.target_fragment_id); }
        }
    }
    if topo.len() != fragments.len() {
        return Err("fragment graph has a cycle".into());
    }
    Ok(topo)
}

fn identify_root_fragment(
    fragments: &[FragmentBuildResult],
    edges: &[FragmentEdge],
) -> Result<FragmentId, String> {
    // 根 fragment = 没有出边的 fragment
    let has_outgoing: BTreeSet<FragmentId> = edges.iter().map(|e| e.source_fragment_id).collect();
    let mut roots: Vec<FragmentId> = fragments.iter()
        .filter(|f| !has_outgoing.contains(&f.fragment_id))
        .map(|f| f.fragment_id)
        .collect();
    if roots.len() != 1 {
        return Err(format!("expected exactly one root fragment, found {}", roots.len()));
    }
    Ok(roots.pop().unwrap())
}

fn find_scan_plan_nodes(fragment: &TPlanFragment) -> Vec<i32 /*plan_node_id*/> {
    let plan = match fragment.plan.as_ref() {
        Some(p) => p,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    walk_plan_tree(plan, &mut |node| {
        use crate::planner::TPlanNodeType::*;
        if matches!(node.node_type, HDFS_SCAN_NODE | FILE_SCAN_NODE | LAKE_SCAN_NODE | ICEBERG_SCAN_NODE) {
            out.push(node.node_id);
        }
    });
    out
}

fn walk_plan_tree(plan: &crate::planner::TPlan, f: &mut impl FnMut(&crate::planner::TPlanNode)) {
    for node in &plan.nodes {
        f(node);
    }
    // 假设 TPlan 内 nodes 已经是 flat 列表；如果是树形递归，需要 visit children
}
```

注：`identify_root_fragment` / `find_scan_plan_nodes` / `walk_plan_tree` 这几个 helper 的具体写法依赖现有 TPlanFragment 数据结构；任务 3.2.3 执行时要先 `grep -n "struct TPlanFragment\|pub plan" src/` 看实际字段，按需调整。

- [ ] **Step 3.2.4：跑测试确认 instance_count 部分通过**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::assign_instance_count_tests
```

期望：所有 instance_count 测试通过；其他 stage 的测试因 `todo!()` 仍 panic（OK，后续任务实现）。

---

### 任务 3.3：`assign` backend_idx + finst_id 分配

**Files:**
- Modify: `src/runtime/scheduler.rs`

- [ ] **Step 3.3.1：写 backend_idx 分配测试**

```rust
#[test]
fn multi_instance_fragment_backend_idx_equals_instance_index() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
        "127.0.0.1:9072".parse().unwrap(),
    ]);
    let fragments = vec![fake_scan_fragment(0)];
    let edges = vec![];
    let plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0, lo: 5 }, mock_connector_registry()).expect("ok");
    let instances = &plan.by_fragment[&0];
    assert_eq!(instances.len(), 3);
    for (i, inst) in instances.iter().enumerate() {
        assert_eq!(inst.instance_index, i);
        assert_eq!(inst.backend_idx, i);
    }
}

#[test]
fn single_instance_fragment_lands_on_query_id_hash() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
        "127.0.0.1:9072".parse().unwrap(),
    ]);
    // query_id.lo = 7，N = 3，hash = 7 % 3 = 1
    let fragments = vec![fake_non_scan_fragment(0)];  // 单 instance root fragment
    let edges = vec![];
    let plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0, lo: 7 }, mock_connector_registry()).expect("ok");
    assert_eq!(plan.root_backend_idx, 1);
    assert_eq!(plan.by_fragment[&0][0].backend_idx, 1);
}

#[test]
fn finst_id_encodes_fragment_id_and_instance_index() {
    let scheduler = FragmentScheduler::new(vec!["127.0.0.1:9070".parse().unwrap()]);
    let fragments = vec![fake_non_scan_fragment(3)];  // fragment_id = 3
    let edges = vec![];
    let plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0xabc, lo: 0 }, mock_connector_registry()).expect("ok");
    let inst = &plan.by_fragment[&3][0];
    assert_eq!(inst.finst_id.hi, 0xabc);
    // lo = (fragment_id << 16) | instance_index = (3 << 16) | 0 = 0x30000
    assert_eq!(inst.finst_id.lo, 0x30000);
}
```

- [ ] **Step 3.3.2：实现 backend_idx + finst_id 分配段**

把 `assign` 函数体内 `todo!()` 替换为：

```rust
let root_backend_idx = (query_id.lo as usize) % n;
let mut by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>> = BTreeMap::new();
let mut root_finst_id = None;

for fr in fragments {
    let count = instance_count[&fr.fragment_id];
    let mut instances = Vec::with_capacity(count);
    for instance_index in 0..count {
        let backend_idx = if count == 1 { root_backend_idx } else { instance_index };
        let finst_id = TUniqueId {
            hi: query_id.hi,
            lo: ((fr.fragment_id as i64) << 16) | (instance_index as i64),
        };
        if fr.fragment_id == root_fragment_id {
            root_finst_id = Some(finst_id);
        }
        instances.push(FragmentInstancePlacement {
            fragment_id: fr.fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            runtime_filter_prober_params: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
        });
    }
    by_fragment.insert(fr.fragment_id, instances);
}

let plan = SchedulingPlan {
    by_fragment,
    root_finst_id: root_finst_id.ok_or("root fragment not found")?,
    root_backend_idx,
};
// scan_ranges / destinations / RF / per_exch_num_senders 在后续任务填
Ok(plan)
```

- [ ] **Step 3.3.3：跑 backend_idx 测试**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::assign_instance_count_tests
```

期望：backend_idx 相关 3 个测试通过。

---

### 任务 3.4：`assign` scan splits round-robin 切分

**Files:**
- Modify: `src/runtime/scheduler.rs`

> **⚠️ 设计调整（vs. 原计划）**：原 PR-0 期望 Iceberg `to_thrift_scan` 只产 `scan_ranges`（`node: None`），node 由 codegen 单独构造。但 main #205 把 node 构造也搬进了 `to_thrift_scan`，每次调 `to_thrift_scan` 都会顺带生成一份 `TPlanNode`。本任务需明确选一种做法：
>
> - **方案 A**：scheduler 每个 instance 各自调一次 `to_thrift_scan(scan, splits_for_instance_i, ctx)` —— 取其 `scan_ranges` 给该 instance；`node` 在所有 instance 之间是恒等的（同 scan 同 conjuncts），用任一份即可，多余的丢弃。简单但每 instance 多算一次 node。
> - **方案 B（推荐）**：codegen 阶段（`nodes.rs` 走 `planner.to_thrift_scan` 拿 `THdfsScanNode` 那里）保留单次 node 构造；scheduler **绕开 `to_thrift_scan` 重新调用**，从 `PlannedConnectorScan.splits` 按 `i % N` 切分后再调一个专门的轻量入口（如新增 trait 方法 `planner.splits_to_scan_ranges(scan, splits, ctx)`）；或临时复用 `to_thrift_scan` 但显式忽略其 `node` 字段并加注释说明。
>
> **执行前**：在 scheduler.rs 里贴一行 `// D2 scan split policy: <A or B> — chosen because <reason>`，让 review 看到决策痕迹。下方测试与代码片段先按方案 A 写（最小变更）。若选 B，需要在 `ConnectorScanPlanner` trait 加新方法，并补一个 PR-3 子任务。

- [ ] **Step 3.4.1：写 scan splits 切分测试**

```rust
#[test]
fn scan_splits_round_robin_seven_files_three_instances() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
        "127.0.0.1:9072".parse().unwrap(),
    ]);
    // 7 files, N=3：file 0,3,6 → instance 0；file 1,4 → instance 1；file 2,5 → instance 2
    let fragments = vec![fake_scan_fragment_with_n_files(0, /*scan_node_id*/ 10, 7)];
    let edges = vec![];
    let plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0, lo: 0 }, mock_connector_registry_with_iceberg_planner()).expect("ok");

    let instances = &plan.by_fragment[&0];
    assert_eq!(instances.len(), 3);
    assert_eq!(instances[0].scan_ranges[&10].len(), 3);  // file 0, 3, 6
    assert_eq!(instances[1].scan_ranges[&10].len(), 2);  // file 1, 4
    assert_eq!(instances[2].scan_ranges[&10].len(), 2);  // file 2, 5
    let total: usize = instances.iter().map(|i| i.scan_ranges[&10].len()).sum();
    assert_eq!(total, 7);
}

#[test]
fn scan_splits_no_overlap_no_loss() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
    ]);
    let fragments = vec![fake_scan_fragment_with_n_files(0, 10, 6)];
    let edges = vec![];
    let plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0, lo: 0 }, mock_connector_registry_with_iceberg_planner()).expect("ok");

    let all_files: Vec<String> = plan.by_fragment[&0].iter()
        .flat_map(|inst| inst.scan_ranges[&10].iter().map(|sr| extract_file_path(sr)))
        .collect();
    let unique: std::collections::HashSet<_> = all_files.iter().collect();
    assert_eq!(all_files.len(), 6);
    assert_eq!(unique.len(), 6);  // 无重无漏
}
```

- [ ] **Step 3.4.2：实现 scan splits 切分段**

在 `assign` 函数体内（backend_idx 分配之后），加入：

```rust
// Step 4: 给 scan fragment 每个 instance 切片 splits + 调 connector.to_thrift_scan
for (fragment_id, instances) in plan.by_fragment.iter_mut() {
    let fr = fragments.iter().find(|f| f.fragment_id == *fragment_id).unwrap();
    let scan_nodes_with_planned = collect_planned_scan_nodes(fr);
    for (plan_node_id, planned) in &scan_nodes_with_planned {
        let count = instances.len();
        let planner = connectors.scan_planner(&planned.scan.connector_id())
            .ok_or_else(|| format!("connector {} not registered",
                                    planned.scan.connector_id()))?;
        for (idx, placement) in instances.iter_mut().enumerate() {
            let instance_splits: Vec<crate::connector::Split> = planned.splits.iter().enumerate()
                .filter(|(i, _)| i % count == idx)
                .map(|(_, s)| s.clone())
                .collect();
            let thrift_ctx = crate::connector::ThriftScanContext::for_codegen();
            let thrift = planner.to_thrift_scan(&planned.scan, &instance_splits, thrift_ctx)
                .map_err(|e| format!("to_thrift_scan for fragment {} node {}: {}",
                                     fragment_id, plan_node_id, e))?;
            placement.scan_ranges.insert(*plan_node_id, thrift.scan_ranges);
        }
    }
}
```

`collect_planned_scan_nodes(fr)` 是辅助函数，从 `FragmentBuildResult` 中拿到 `Vec<(plan_node_id, &PlannedConnectorScan)>`。具体实现要看 `FragmentBuildResult` 的字段——目前 `PlannedConnectorScan` 挂在 `ResolvedTable.planned_scan`，需要从 `FragmentBuildResult` 反查所有 scan plan node 对应的 ResolvedTable。

**plumbing 前置**：执行此任务前需要在 `FragmentBuildResult` 上加一个字段 `scan_resolved_tables: Vec<(i32 /*plan_node_id*/, ResolvedTable)>` 或等价，由 fragment_builder 在构造阶段填写。这是 spec §9.5 提到的 plumbing 前提。

- [ ] **Step 3.4.3：跑 scan splits 测试**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::assign_instance_count_tests
```

期望：scan splits 测试通过。

---

### 任务 3.5：`fill_destinations` 实现

**Files:**
- Modify: `src/runtime/scheduler.rs`

- [ ] **Step 3.5.1：写测试**

```rust
#[test]
fn fill_destinations_upstream_each_instance_gets_all_downstream() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
    ]);
    // F0 (scan, 2 instances) → HASH → F1 (consumer, 2 instances)
    let fragments = vec![
        fake_scan_fragment(0),
        fake_non_scan_fragment_consuming_hash(1, 0),
    ];
    let edges = vec![fake_edge(0, 1, crate::partitions::TPartitionType::HashPartitioned)];
    let mut plan = scheduler.assign(&fragments, &edges,
        TUniqueId { hi: 0, lo: 0 }, mock_connector_registry()).expect("ok");
    scheduler.fill_destinations(&mut plan, &edges);

    // F0 每个 instance 的 destinations 应该是 F1 全部 2 个 instance
    for src_inst in &plan.by_fragment[&0] {
        assert_eq!(src_inst.destinations.len(), 2);
        let dest_finst_ids: Vec<TUniqueId> = src_inst.destinations.iter()
            .map(|d| d.fragment_instance_id).collect();
        let expected: Vec<TUniqueId> = plan.by_fragment[&1].iter()
            .map(|i| i.finst_id).collect();
        assert_eq!(dest_finst_ids, expected);
    }
}

#[test]
fn fill_destinations_includes_brpc_server_addr() {
    let scheduler = FragmentScheduler::new(vec![
        "10.0.0.1:9070".parse().unwrap(),
        "10.0.0.2:9070".parse().unwrap(),
    ]);
    let fragments = vec![fake_scan_fragment(0), fake_non_scan_fragment_consuming_hash(1, 0)];
    let edges = vec![fake_edge(0, 1, crate::partitions::TPartitionType::HashPartitioned)];
    let mut plan = scheduler.assign(&fragments, &edges, TUniqueId { hi: 0, lo: 0 },
        mock_connector_registry()).expect("ok");
    scheduler.fill_destinations(&mut plan, &edges);
    let dest = &plan.by_fragment[&0][0].destinations[0];
    let addr = dest.brpc_server.as_ref().expect("brpc_server set");
    assert_eq!(addr.hostname, "10.0.0.1");
    assert_eq!(addr.port, 9070);
}
```

- [ ] **Step 3.5.2：实现 fill_destinations**

```rust
impl FragmentScheduler {
    pub fn fill_destinations(&self, plan: &mut SchedulingPlan, edges: &[FragmentEdge]) {
        for edge in edges {
            let target_instances = plan.by_fragment.get(&edge.target_fragment_id)
                .cloned().unwrap_or_default();
            let dest_list: Vec<TPlanFragmentDestination> = target_instances.iter().map(|t| {
                TPlanFragmentDestination {
                    fragment_instance_id: t.finst_id,
                    brpc_server: Some(crate::types::TNetworkAddress {
                        hostname: self.backends[t.backend_idx].ip().to_string(),
                        port: self.backends[t.backend_idx].port() as i32,
                    }),
                    ..Default::default()
                }
            }).collect();
            if let Some(source_instances) = plan.by_fragment.get_mut(&edge.source_fragment_id) {
                for src in source_instances.iter_mut() {
                    src.destinations.extend(dest_list.clone());
                }
            }
        }
    }
}
```

- [ ] **Step 3.5.3：跑测试**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::fill_destinations
```

期望：2 个 fill_destinations 测试通过。

---

### 任务 3.6：`fill_runtime_filter_params` 实现

**Files:**
- Modify: `src/runtime/scheduler.rs`

- [ ] **Step 3.6.1：写测试**

```rust
#[test]
fn fill_rf_build_prober_params_includes_all_probe_instances() {
    let scheduler = FragmentScheduler::new(vec![
        "10.0.0.1:9070".parse().unwrap(),
        "10.0.0.2:9070".parse().unwrap(),
    ]);
    // F_build = 0 (单 instance), F_probe = 1 (2 instance)
    let fragments = vec![
        fake_non_scan_fragment(0),
        fake_scan_fragment(1),
    ];
    let edges = vec![];
    let rf_plan = RfPlanForTest {
        filters: vec![
            RfDescriptorForTest {
                filter_id: 42,
                build_fragment_id: 0,
                probe_fragment_id: 1,
            },
        ],
    };
    let mut plan = scheduler.assign(&fragments, &edges, TUniqueId { hi: 0, lo: 0 },
        mock_connector_registry()).expect("ok");
    scheduler.fill_runtime_filter_params(&mut plan, &rf_plan);

    let build_inst = &plan.by_fragment[&0][0];
    let provers = &build_inst.runtime_filter_prober_params[&42];
    assert_eq!(provers.len(), 2);  // 2 probe instances
    // verify each prober has the right addr
    assert_eq!(provers[0].fragment_instance_address.as_ref().unwrap().hostname, "10.0.0.1");
    assert_eq!(provers[1].fragment_instance_address.as_ref().unwrap().hostname, "10.0.0.2");
}
```

- [ ] **Step 3.6.2：实现**

```rust
pub fn fill_runtime_filter_params(
    &self,
    plan: &mut SchedulingPlan,
    rf_plan: &impl RfPlanLike,
) {
    for rf in rf_plan.filters() {
        let probe_instances = plan.by_fragment.get(&rf.probe_fragment_id())
            .cloned().unwrap_or_default();
        let prober_list: Vec<crate::internal_service::TRuntimeFilterProberParams> = probe_instances.iter()
            .map(|p| crate::internal_service::TRuntimeFilterProberParams {
                fragment_instance_id: p.finst_id,
                fragment_instance_address: Some(crate::types::TNetworkAddress {
                    hostname: self.backends[p.backend_idx].ip().to_string(),
                    port: self.backends[p.backend_idx].port() as i32,
                }),
                ..Default::default()
            }).collect();
        if let Some(build_instances) = plan.by_fragment.get_mut(&rf.build_fragment_id()) {
            for b in build_instances.iter_mut() {
                b.runtime_filter_prober_params
                    .entry(rf.filter_id())
                    .or_insert(prober_list.clone());
            }
        }
    }
}
```

`RfPlanLike` 是一个 trait 让测试和真实的 `RfPlan` 都能传——实际签名也可以直接接 `&RfPlan`，但 D2 spec 强调 scheduler 是 pure decision，trait 边界更清晰。简化版可以直接 `&RfPlan`。

- [ ] **Step 3.6.3：跑测试**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::fill_rf
```

期望：通过。

---

### 任务 3.7：`fill_per_exch_num_senders` 实现

**Files:**
- Modify: `src/runtime/scheduler.rs`

- [ ] **Step 3.7.1：写测试**

```rust
#[test]
fn fill_per_exch_num_senders_accumulates_upstream_count() {
    let scheduler = FragmentScheduler::new(vec![
        "127.0.0.1:9070".parse().unwrap(),
        "127.0.0.1:9071".parse().unwrap(),
    ]);
    // F0 (scan, 2) + F2 (scan, 2) → F1 (consumer, 2 via 2 edges through 2 exchange nodes)
    let fragments = vec![
        fake_scan_fragment(0),
        fake_non_scan_fragment_consuming_hash(1, 0),
        fake_scan_fragment(2),
    ];
    let edges = vec![
        FragmentEdge {
            source_fragment_id: 0, target_fragment_id: 1,
            target_exchange_node_id: 100,
            partition_type: crate::partitions::TPartitionType::HashPartitioned,
            ..Default::default()
        },
        FragmentEdge {
            source_fragment_id: 2, target_fragment_id: 1,
            target_exchange_node_id: 200,  // 不同 exchange node
            partition_type: crate::partitions::TPartitionType::HashPartitioned,
            ..Default::default()
        },
    ];
    let mut plan = scheduler.assign(&fragments, &edges, TUniqueId { hi: 0, lo: 0 },
        mock_connector_registry()).expect("ok");
    scheduler.fill_per_exch_num_senders(&mut plan, &edges);

    let f1_inst = &plan.by_fragment[&1][0];
    assert_eq!(f1_inst.per_exch_num_senders.get(&100), Some(&2));  // F0 has 2 instances
    assert_eq!(f1_inst.per_exch_num_senders.get(&200), Some(&2));  // F2 has 2 instances
}
```

- [ ] **Step 3.7.2：实现**

```rust
pub fn fill_per_exch_num_senders(
    &self,
    plan: &mut SchedulingPlan,
    edges: &[FragmentEdge],
) {
    for edge in edges {
        let upstream_n = plan.by_fragment.get(&edge.source_fragment_id)
            .map(|v| v.len()).unwrap_or(0);
        if let Some(target_instances) = plan.by_fragment.get_mut(&edge.target_fragment_id) {
            for t in target_instances.iter_mut() {
                *t.per_exch_num_senders.entry(edge.target_exchange_node_id).or_insert(0)
                    += upstream_n as i32;
            }
        }
    }
}
```

- [ ] **Step 3.7.3：跑测试**

```bash
cargo test --lib --package novarocks -- runtime::scheduler::fill_per_exch
```

期望：通过。

---

### 任务 3.8：跑全部 scheduler 单测

- [ ] **Step 3.8.1**

```bash
cargo test --lib --package novarocks -- runtime::scheduler
```

期望：全部通过。

---

### 任务 3.9：PR-3 commit

- [ ] **Step 3.9.1：commit**

```bash
git add src/runtime/scheduler.rs src/runtime/mod.rs
git commit -m "$(cat <<'EOF'
feat(runtime): add FragmentScheduler module (D2 PR-3)

New src/runtime/scheduler.rs module decides (fragment, instance) → backend
mapping for multi-BE execution. Four stages:

- assign(): compute instance_count per fragment using StarRocks-style
  "instance follows upstream" rule (scan=N, HASH/BUCKET_SHUFFLE consumer=N,
  UNPARTITIONED gather / root=1); allocate backend_idx (multi-instance:
  instance_index; single-instance: backends[query_id.lo % N]); compute
  finst_id = (query_id.hi, (fragment_id << 16) | instance_index); call
  ConnectorScanPlanner.to_thrift_scan once per instance with round-robin
  scan range slice.
- fill_destinations(): for each edge, fill source fragment's
  destinations with target fragment's all instances (finst_id + brpc_server).
- fill_runtime_filter_params(): for each RF, fill build fragment's
  prober_params with probe fragment's all instance addresses.
- fill_per_exch_num_senders(): accumulate upstream instance counts per
  target exchange node.

Coordinator does not yet call this; PR-4 wires it in.

Refs: docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md
EOF
)"
```

---

# PR-4: Coordinator 集成 + `be_number` + `InFlightTracker`

**范围**：让 coordinator 在 submit 前调 scheduler；`InFlightTracker` 按 BE 分组；`data_stream_sink.rs` 不再硬编码 `be_number = 0`；exec_params 把 placement 字段塞进 `TExecPlanFragmentParams`。

**输入**：PR-3 已合并。

**输出**：
- coordinator 走 scheduler.assign → fill_destinations → fill_runtime_filter_params → fill_per_exch_num_senders
- InFlightTracker 按 BE 分组维护
- data_stream_sink 从 exec_params.backend_num 读 be_number
- 1FE+2BE 跨进程 smoke 跑通

**验证**：
```bash
cargo test --test cluster_mvp  # D1 不回归
cargo test --test cluster_mvp_d2  # D2 新增 smoke
```

**回滚**：`git revert <pr-4-commit>`。

---

### 任务 4.1：`InFlightTracker` 数据类型

**Files:**
- Modify: `src/runtime/coordinator.rs`

- [ ] **Step 4.1.1：写测试**

在 `src/runtime/coordinator.rs` 测试模块加：

```rust
#[test]
fn in_flight_tracker_groups_by_backend() {
    let mut tracker = InFlightTracker::default();
    tracker.record_submitted(0, TUniqueId { hi: 0, lo: 1 });
    tracker.record_submitted(0, TUniqueId { hi: 0, lo: 2 });
    tracker.record_submitted(1, TUniqueId { hi: 0, lo: 3 });
    assert_eq!(tracker.by_backend.get(&0).map(|v| v.len()), Some(2));
    assert_eq!(tracker.by_backend.get(&1).map(|v| v.len()), Some(1));
}

#[test]
fn in_flight_tracker_cancel_all_calls_each_backend() {
    let mut tracker = InFlightTracker::default();
    tracker.record_submitted(0, TUniqueId { hi: 0, lo: 1 });
    tracker.record_submitted(1, TUniqueId { hi: 0, lo: 2 });

    let mock = Arc::new(CancelTrackingDispatcher::default());
    tracker.cancel_all(&*mock);
    assert!(mock.was_called_with(0));
    assert!(mock.was_called_with(1));
}
```

`CancelTrackingDispatcher` 是测试用 mock，记录 `cancel_fragments` 调用次数 + 参数。

- [ ] **Step 4.1.2：实现 InFlightTracker**

```rust
#[derive(Default, Debug)]
pub(crate) struct InFlightTracker {
    pub(crate) by_backend: BTreeMap<usize, Vec<TUniqueId>>,
}

impl InFlightTracker {
    pub(crate) fn record_submitted(&mut self, backend_idx: usize, finst_id: TUniqueId) {
        self.by_backend.entry(backend_idx).or_default().push(finst_id);
    }
    pub(crate) fn cancel_all(&self, dispatcher: &dyn crate::runtime::dispatcher::FragmentDispatcher) {
        for (idx, ids) in &self.by_backend {
            dispatcher.cancel_fragments(*idx, ids);
        }
    }
}
```

- [ ] **Step 4.1.3：跑测试**

```bash
cargo test --lib --package novarocks -- runtime::coordinator::in_flight_tracker
```

期望：通过。

---

### 任务 4.2：`data_stream_sink` 改读 backend_num

**Files:**
- Modify: `src/exec/operators/data_stream_sink.rs:973`

- [ ] **Step 4.2.1：定位硬编码**

```bash
sed -n '970,980p' src/exec/operators/data_stream_sink.rs
```

确认当前是 `let be_number = 0i32;`。

- [ ] **Step 4.2.2：写测试**

```rust
#[test]
fn data_stream_sink_reads_be_number_from_exec_params() {
    let mut exec_params = TPlanFragmentExecParams::default();
    exec_params.backend_num = Some(2);
    // 构造 DataStreamSink 与 exec_params，pump 一个 chunk
    // verify operator 内 be_number 字段 == 2
    // 略：mock chunk 路径
}
```

- [ ] **Step 4.2.3：改实现**

```rust
let be_number = self.exec_params.backend_num.unwrap_or(0);
```

- [ ] **Step 4.2.4：grep 其他地方有没有也假设 `be_number == 0`**

```bash
grep -rn "be_number" src/ --include="*.rs"
```

把每个 hit 都过一遍 —— 没有"假设值为 0"的逻辑。

- [ ] **Step 4.2.5：跑测试**

```bash
cargo test --lib --package novarocks -- data_stream_sink
```

---

### 任务 4.3：exec_params builder 加 placement 字段

**Files:**
- Modify: `src/runtime/exec_params.rs::build_exec_plan_fragment_params`

- [ ] **Step 4.3.1：扩展 builder 签名**

当前签名（D1）：

```rust
pub(crate) fn build_exec_plan_fragment_params(
    fr: &FragmentBuildResult,
    thrift_fragment: planner::TPlanFragment,
    exec_params: internal_service::TPlanFragmentExecParams,
    query_options: Option<internal_service::TQueryOptions>,
    pipeline_dop: i32,
) -> internal_service::TExecPlanFragmentParams
```

D2 加入 placement 参数：

```rust
pub(crate) fn build_exec_plan_fragment_params(
    fr: &FragmentBuildResult,
    placement: &crate::runtime::scheduler::FragmentInstancePlacement,
    thrift_fragment: planner::TPlanFragment,
    query_options: Option<internal_service::TQueryOptions>,
    pipeline_dop: i32,
) -> internal_service::TExecPlanFragmentParams
```

内部把 placement 的字段塞到 exec_params：

```rust
let mut exec_params = fr.exec_params.clone();
exec_params.fragment_instance_id = placement.finst_id;
exec_params.destinations = Some(placement.destinations.clone());
exec_params.per_exch_num_senders = placement.per_exch_num_senders.clone();
exec_params.runtime_filter_params = build_runtime_filter_params_from_placement(placement);
exec_params.backend_num = Some(placement.backend_idx as i32);
exec_params.per_node_scan_ranges = placement.scan_ranges.clone().into_thrift_map();
TExecPlanFragmentParams {
    protocol_version: 0,
    fragment: Some(thrift_fragment),
    desc_tbl: Some(fr.desc_tbl.clone()),
    params: Some(exec_params),
    query_options,
    ..Default::default()
}
```

- [ ] **Step 4.3.2：cargo build 修签名变化处**

D1 调用点（应该在 coordinator.rs 内）签名换了，要补 placement 参数。临时全部传 placement 占位（PR-4 后面会真正传 scheduler 的输出）。

---

### 任务 4.4：coordinator 集成 scheduler

**Files:**
- Modify: `src/runtime/coordinator.rs::ExecutionCoordinator::execute`

- [ ] **Step 4.4.1：改 ExecutionCoordinator new 签名加 scheduler**

```rust
pub(crate) fn new(
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn FragmentDispatcher>,
    scheduler: Arc<crate::runtime::scheduler::FragmentScheduler>,
    query_options: Option<TQueryOptions>,
) -> Self
```

- [ ] **Step 4.4.2：改 engine/mod.rs 构造点传 scheduler**

```rust
let backends = match cluster_role {
    ClusterRole::Fe => {
        cfg.cluster.backends.iter()
            .map(|s| s.parse::<SocketAddr>())
            .collect::<Result<Vec<_>, _>>()?
    }
    ClusterRole::AllInOne => {
        vec![format!("127.0.0.1:{}", exchange_port).parse().unwrap()]
    }
    ClusterRole::Be => unreachable!(),
};
let scheduler = Arc::new(FragmentScheduler::new(backends));
crate::runtime::coordinator::ExecutionCoordinator::new(
    *build_result,
    dispatcher,
    scheduler,
    query_opts,
).execute()
```

- [ ] **Step 4.4.3：改 execute() 主体走 scheduler**

D1 中的 submit loop：

```rust
for fr in fragment_results {
    let params = build_exec_plan_fragment_params(...);
    self.dispatcher.submit_fragment(0, params)?;
    submitted_ids.push(...);
}
```

D2 改为：

```rust
let query_id = TUniqueId { hi: self.query_id_hi, lo: self.query_id_lo };
let connectors = crate::connector::registry();
let mut plan = self.scheduler.assign(
    &self.build_result.fragments,
    &self.build_result.edges,
    query_id,
    connectors,
)?;
self.scheduler.fill_destinations(&mut plan, &self.build_result.edges);
self.scheduler.fill_runtime_filter_params(&mut plan, &self.build_result.rf_plan);
self.scheduler.fill_per_exch_num_senders(&mut plan, &self.build_result.edges);

let mut tracker = InFlightTracker::default();
for (_, instances) in &plan.by_fragment {
    for placement in instances {
        let fr = self.build_result.fragments.iter()
            .find(|f| f.fragment_id == placement.fragment_id).unwrap();
        let params = build_exec_plan_fragment_params(
            fr,
            placement,
            fr.fragment.clone(),
            self.query_options.clone(),
            compute_pipeline_dop(),
        );
        if let Err(e) = self.dispatcher.submit_fragment(placement.backend_idx, params) {
            tracker.cancel_all(&*self.dispatcher);
            return Err(format!(
                "submit fragment {} instance {} to BE[{}]: {}",
                placement.fragment_id, placement.instance_index,
                placement.backend_idx, e
            ));
        }
        tracker.record_submitted(placement.backend_idx, placement.finst_id);
    }
}

// fetch loop：从 root_backend_idx 拉
loop {
    if self.cancel_signal.load(std::sync::atomic::Ordering::Relaxed) {
        tracker.cancel_all(&*self.dispatcher);
        return Err("client disconnected".into());
    }
    let remaining = self.deadline.checked_duration_since(std::time::Instant::now())
        .unwrap_or_default();
    if remaining.is_zero() {
        tracker.cancel_all(&*self.dispatcher);
        return Err("query timeout".into());
    }
    let wait_ms = std::cmp::min(300, remaining.as_millis() as i64);
    match self.dispatcher.fetch_result(plan.root_backend_idx, plan.root_finst_id, wait_ms) {
        Ok(FetchOutcome::Ready(c)) => chunks.push(c),
        Ok(FetchOutcome::NotReady) => continue,
        Ok(FetchOutcome::Eof) => break,
        Ok(FetchOutcome::Err(msg)) => {
            tracker.cancel_all(&*self.dispatcher);
            return Err(msg);
        }
        Err(rpc_err) => {
            tracker.cancel_all(&*self.dispatcher);
            return Err(format!("fetch_result from BE[{}]: {}",
                               plan.root_backend_idx, rpc_err));
        }
    }
}
```

- [ ] **Step 4.4.4：cargo build 修编译错**

```bash
cargo build 2>&1 | tee /tmp/d2-pr4-build.log | tail -30
```

逐个修。

- [ ] **Step 4.4.5：跑 D1 cluster_mvp 回归**

```bash
cargo test --test cluster_mvp
```

期望：D1 所有 6 个测试通过。

---

### 任务 4.5：1FE+2BE smoke

**Files:**
- Create: `tests/cluster_mvp_d2.rs`

- [ ] **Step 4.5.1：扩展 cluster_mvp_tests::spawn_fe 支持多 backend**

`tests/cluster_mvp/src/lib.rs` 中 `spawn_fe` 当前签名：

```rust
pub fn spawn_fe(mysql_port: u16, be_addr: &str) -> FeProcess
```

改为：

```rust
pub fn spawn_fe_multi(mysql_port: u16, be_addrs: &[&str]) -> FeProcess
```

`write_fe_config` 把 `backends = ["..."]` 改为接受多条目。

- [ ] **Step 4.5.2：写 cross_process_two_be_smoke 测试**

```rust
// tests/cluster_mvp_d2.rs
use cluster_mvp_tests::*;

#[test]
fn cross_process_two_be_select_1() {
    let be0 = spawn_be(19070);
    let be1 = spawn_be(19071);
    let fe = spawn_fe_multi(19030, &["127.0.0.1:19070", "127.0.0.1:19071"]);

    let url = format!("mysql://root@127.0.0.1:{}/", fe.mysql_port);
    let pool = mysql::Pool::new(url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let rows: Vec<i64> = conn.query("SELECT 1").unwrap();
    assert_eq!(rows, vec![1]);

    drop(fe); drop(be1); drop(be0);
}

#[test]
fn cross_process_two_be_count_star_iceberg() {
    // 起 docker iceberg-rest fixture，准备 SSB lineorder table（小规模）
    // 通过 2 BE 跑 SELECT COUNT(*) FROM lineorder
    // verify 结果与单 BE 一致
    // 这个测试需要 docker fixture，可以打 #[ignore] 让 CI 选择性跑
}
```

- [ ] **Step 4.5.3：cargo build 确认 cluster_mvp_d2 编译**

```bash
cargo build --test cluster_mvp_d2
```

- [ ] **Step 4.5.4：跑 smoke**

```bash
cargo test --test cluster_mvp_d2 cross_process_two_be_select_1
```

期望：通过。

---

### 任务 4.6：PR-4 commit

- [ ] **Step 4.6.1：commit**

```bash
git add src/runtime/coordinator.rs src/runtime/exec_params.rs src/exec/operators/data_stream_sink.rs src/engine/mod.rs tests/cluster_mvp/ tests/cluster_mvp_d2.rs
git commit -m "$(cat <<'EOF'
feat(runtime): wire FragmentScheduler into coordinator (D2 PR-4)

- ExecutionCoordinator now takes Arc<FragmentScheduler> and calls
  assign + fill_destinations + fill_runtime_filter_params +
  fill_per_exch_num_senders before submit loop.
- InFlightTracker groups submitted finst_id by backend_idx;
  cancel_all fans out to each BE.
- build_exec_plan_fragment_params threads placement fields
  (destinations / per_node_scan_ranges / backend_num / RF prober_params)
  into TExecPlanFragmentParams.
- data_stream_sink reads be_number from exec_params.backend_num instead
  of hardcoded 0.
- engine/mod.rs constructs scheduler from cfg.cluster.backends for FE role,
  from local exchange_port for AllInOne.
- tests/cluster_mvp_d2.rs cross_process_two_be_select_1 smoke passes.

Refs: docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md
EOF
)"
```

---

# PR-5: sql-test-runner `--cluster-size N` + Acceptance Suites

**范围**：sql-test-runner 扩展 `--cluster-size N` 参数；D2 主验收套件（SSB / TPC-H Q5/Q9 / iceberg-rest）在 1FE+2BE 模式 byte-identical 通过；SSB 在 1FE+3BE 抽样验证。

**输入**：PR-4 已合并。

**输出**：
- sql-test-runner 接受 `--cluster-size N`（N >= 1）
- SSB / TPC-H Q5/Q9 / iceberg-rest 在 1FE+2BE 下 byte-identical 通过
- SSB 在 1FE+3BE 抽样通过
- 现有 all-in-one suite 不回归
- D2 验收门槛全部满足

**验证**：D2 spec § 13 验收标准全部勾选。

**回滚**：`git revert <pr-5-commit>`，runner 回到 D1 的 `--cluster-size 1` only。

---

### 任务 5.1：sql-test-runner `--cluster-size` 参数

**Files:**
- Modify: `tests/sql-test-runner/src/main.rs`、`tests/sql-test-runner/src/cluster.rs`

- [ ] **Step 5.1.1：定位现有 cluster-mode 参数**

```bash
grep -n "cluster.mode\|ClusterMode\|cross.process" tests/sql-test-runner/src/main.rs
grep -n "spawn_be\|spawn_fe" tests/sql-test-runner/src/cluster.rs
```

D1 应该已经有 `--cluster-mode cross-process`（默认 cluster-size=1）。

- [ ] **Step 5.1.2：加 `--cluster-size N` 参数**

在 CLI 解析处加：

```rust
#[arg(long, default_value_t = 1)]
cluster_size: usize,
```

校验：`cluster_size >= 1`；`cluster-mode=all-in-one` 时 `cluster_size` 必须 == 1（否则报错）。

- [ ] **Step 5.1.3：扩展 spawn_cluster 起 N BE**

`tests/sql-test-runner/src/cluster.rs` 当前 `spawn_cross_process_pair` 起 1FE+1BE。改为 `spawn_cross_process_cluster(n: usize)`：

```rust
pub fn spawn_cross_process_cluster(n: usize) -> ClusterHandle {
    let mut be_processes = Vec::with_capacity(n);
    let mut be_addrs = Vec::with_capacity(n);
    for i in 0..n {
        let port = pick_free_port();
        be_processes.push(spawn_be(port));
        be_addrs.push(format!("127.0.0.1:{}", port));
    }
    let fe_port = pick_free_port();
    let be_addr_strs: Vec<&str> = be_addrs.iter().map(|s| s.as_str()).collect();
    let fe = spawn_fe_multi(fe_port, &be_addr_strs);
    ClusterHandle { fe, be_processes }
}
```

- [ ] **Step 5.1.4：写 CLI 解析单测**

```rust
#[test]
fn cli_cluster_size_defaults_to_one() {
    let args = vec!["sql-tests".to_string()];
    let cli = parse_cli(&args).expect("parse");
    assert_eq!(cli.cluster_size, 1);
}

#[test]
fn cli_cluster_size_2() {
    let args = vec!["sql-tests".to_string(), "--cluster-size".to_string(), "2".to_string()];
    let cli = parse_cli(&args).expect("parse");
    assert_eq!(cli.cluster_size, 2);
}

#[test]
fn cli_cluster_size_zero_rejected() {
    let args = vec!["sql-tests".to_string(), "--cluster-size".to_string(), "0".to_string()];
    parse_cli(&args).expect_err("0 should fail");
}

#[test]
fn cli_all_in_one_with_cluster_size_2_rejected() {
    let args = vec!["sql-tests".to_string(),
                    "--cluster-mode".to_string(), "all-in-one".to_string(),
                    "--cluster-size".to_string(), "2".to_string()];
    parse_cli(&args).expect_err("all-in-one + cluster-size>1 should fail");
}
```

- [ ] **Step 5.1.5：跑测试**

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests cli_cluster_size
```

期望：通过。

---

### 任务 5.2：SSB 1FE+2BE byte-identical 验收

- [ ] **Step 5.2.1：先跑 all-in-one baseline 锁定基线（如果还没 recorded）**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
$SQLT --suite ssb --mode record
```

- [ ] **Step 5.2.2：跑 1FE+2BE 模式**

```bash
$SQLT --suite ssb --mode verify --cluster-mode cross-process --cluster-size 2
```

期望：13 个 SSB 查询全部 byte-identical 通过。

如有差异：
- `--mode diff` 看具体哪里不同
- 检查 chunk 顺序是否因为 HASH shuffle 在 2 BE 间合并后不稳定（缺 ORDER BY 的查询补 ORDER BY；或在 sql-tests 中加 `-- @normalize_unordered`）

---

### 任务 5.3：TPC-H Q5/Q9 1FE+2BE 验收

- [ ] **Step 5.3.1**

```bash
$SQLT --suite tpc-h --mode verify --only q5,q9 --cluster-mode cross-process --cluster-size 2
```

期望：Q5（multi-join + HASH shuffle 关键）和 Q9（multi-join + group by）byte-identical 通过。

- [ ] **Step 5.3.2（补强）：Q1 / Q12 也跑一遍**

```bash
$SQLT --suite tpc-h --mode verify --only q1,q12 --cluster-mode cross-process --cluster-size 2
```

---

### 任务 5.4：iceberg-rest 1FE+2BE smoke

- [ ] **Step 5.4.1**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify --cluster-mode cross-process --cluster-size 2
```

期望：通过。

---

### 任务 5.5：SSB 1FE+3BE 抽样

- [ ] **Step 5.5.1**

```bash
$SQLT --suite ssb --mode verify --cluster-mode cross-process --cluster-size 3
```

期望：通过（与 cluster-size=2 一致）。

---

### 任务 5.6：all-in-one 模式不回归（最终 gate）

- [ ] **Step 5.6.1：跑全套 sql-test 在 all-in-one 模式**

```bash
$SQLT --suite ssb --mode verify
$SQLT --suite tpc-h --mode verify --only q1,q5,q9,q12
$SQLT --suite cte --mode verify
$SQLT --suite iceberg --mode verify
$SQLT --suite iceberg-rest --mode verify
$SQLT --suite iceberg-compatibility --mode verify
```

期望：全部通过；不允许出现 D1 之前能过 D2 之后失败的 case。

---

### 任务 5.7：BE 崩溃测试（多 BE 版）

**Files:**
- Modify: `tests/cluster_mvp_d2.rs`

- [ ] **Step 5.7.1：写 BE kill 测试**

```rust
#[test]
fn cross_process_two_be_be_kill_fails_cleanly() {
    let mut be0 = spawn_be(19070);
    let be1 = spawn_be(19071);
    let fe = spawn_fe_multi(19030, &["127.0.0.1:19070", "127.0.0.1:19071"]);

    let url = format!("mysql://root@127.0.0.1:{}/", fe.mysql_port);
    let pool = mysql::Pool::new(url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // 起一个 SQL，立刻 kill be0
    let kill_handle = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let _ = be0.child.kill();
    });

    let result: Result<Vec<i64>, _> = conn.query("SELECT count(*) FROM big_iceberg_table");
    kill_handle.join().unwrap();

    assert!(result.is_err(), "query should fail when a BE dies");
    // FE 仍活着
    assert!(fe.is_alive());

    drop(fe); drop(be1);
}
```

- [ ] **Step 5.7.2：跑**

```bash
cargo test --test cluster_mvp_d2 cross_process_two_be_be_kill_fails_cleanly
```

期望：通过。

---

### 任务 5.8：PR-5 commit

- [ ] **Step 5.8.1：commit**

```bash
git add tests/sql-test-runner/ tests/cluster_mvp_d2.rs tests/cluster_mvp/
git commit -m "$(cat <<'EOF'
feat(distributed): sql-test-runner --cluster-size N + D2 acceptance (D2 PR-5)

Adds --cluster-size N to sql-test-runner; runner spawns N BE + 1 FE for
each test. SSB full suite, TPC-H Q1/Q5/Q9/Q12, and iceberg-rest pass
byte-identically against all-in-one baseline at cluster-size=2; SSB
sampled at cluster-size=3. Cross-BE BE-kill test confirms FE process
does not crash when a BE dies mid-query.

This completes D2's main acceptance gate per
docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md.

Refs: docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md
EOF
)"
```

---

## D2 验收 Checklist

完成所有 6 个 PR 后，确认 spec 第 13 节验收标准全部满足：

- [ ] 1FE + 2BE 同机跨进程跑通 SSB 全套 byte-identical
- [ ] 1FE + 2BE TPC-H Q5/Q9 byte-identical（跨 BE HASH join 关键 case）
- [ ] 1FE + 2BE iceberg-rest smoke 通过
- [ ] 1FE + 3BE SSB 抽样通过
- [ ] 现有 all-in-one suite 不回归
- [ ] BE 崩溃测试通过（FE 进程不挂）
- [ ] HASH shuffle 跨 BE 数据正确性测试通过
- [ ] 跨 BE RF 数据正确性测试通过
- [ ] scan ranges 在 BE 间近似均衡（差距 ≤ 1 文件）
- [ ] `data_stream_sink.rs` 硬编码 `be_number = 0` 删除
- [ ] coordinator 错误信息含 `BE[idx] (addr:port)`
- [ ] D2 PR-0：Iceberg `ConnectorScanPlanner::to_thrift_scan` 已填实

到此 D2 完成。下一步进入 [D3: 动态注册 + 故障健壮性](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-dynamic-registry-and-resilience.md)。

---

## 风险与开放问题（执行时关注）

1. **任务 0.2** Iceberg byte-identical 测试可能因为 `build_hdfs_scan_range_params_for_file` 是 private 难以测试；如有需要在 nodes.rs 暴露 `pub(crate) fn xxx_for_test`。
2. **任务 3.4 (scan_resolved_tables plumbing)**：`FragmentBuildResult` 当前可能没有"plan_node_id → ResolvedTable"映射。PR-3 实施时第一步是 grep + 决定如何把 ResolvedTable 引用传到 FragmentBuildResult 上。
3. **任务 3.2 (find_scan_plan_nodes / walk_plan_tree)**：依赖 TPlanFragment 内部结构（plan 是 flat 列表还是树？）。实施前 grep 确认。
4. **任务 4.4 (cancel_signal)**：D1 已经有 `Arc<AtomicBool>` 信号机制（[src/runtime/query_cancel.rs:1-37](../../src/runtime/query_cancel.rs:1)）。PR-4 直接复用，不引入新机制。
5. **任务 5.2 (byte-identical with HASH shuffle)**：如果 2BE 模式下输出顺序与 all-in-one 不一致，先 root cause；不要随便加 normalize（可能掩盖真实分桶错误）。
6. **任务 4.5 (cluster_mvp_d2 smoke)**：spawn_be 默认起在 starlet_port；多 BE 时端口冲突要确保 each BE 用不同端口。`tests/cluster_mvp/src/lib.rs` 已经提供端口管理 helper（`pick_free_port`），直接复用。
