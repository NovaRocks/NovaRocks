# D2：多 BE 并行执行 — 设计

日期：2026-05-28
状态：已确认待评审
对应 roadmap 任务：[`distributed-multi-be-execution`](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-multi-be-execution.md)（Standalone Distributed Execution Roadmap 第 D2 项）

依赖：
- D1（PR #201 / commit `fa835350`）— 跨进程 MVP
- Connector-first 阶段 1+2+3（PR #202 / commit `14bdefdb`）— scan planning 抽象 + StarRocks/Iceberg 迁移

---

## 1. 概述

把 NovaRocks standalone 模式从 D1 的 **1 FE + 1 BE** 升级到 **1 FE + N BE** 真正分布式执行：FE 端 fragment scheduler 决定每个 (fragment, instance) 落到哪个 BE，scan ranges 在 N 个 BE 之间按文件粒度切分，HASH / BUCKET_SHUFFLE shuffle 在 N 个 BE 间正确分桶，跨 BE runtime filter 传播到全部 probe instance。目标 1FE+2BE / 1FE+3BE 同机跑通 SSB / TPC-H Q5/Q9 / iceberg-rest，与 all-in-one 输出 byte-identical。

## 2. 目标与非目标

### 2.1 目标

1. `[cluster].backends` 放宽至 N >= 1 条目，启动期 fail-fast dial 所有 BE。
2. 新建 `FragmentScheduler` 模块（`src/runtime/scheduler.rs`），独立于 coordinator + dispatcher。
3. `FragmentDispatcher` trait 加 `backend_idx` 首参；删 `exchange_addr()`；加 `backend_count()`。
4. `RemoteDispatcher` 持 `Vec<NovaRocksGrpcRemoteClient>`，按 backend_idx 路由。
5. Instance count 规则：scan fragment = N；HASH / BUCKET_SHUFFLE 消费者 = 跟上游同 N；UNPARTITIONED gather / 根 fragment = 1。
6. 根 fragment 放置：`backends[query_id % N]`，FE 从该 BE fetch 结果。
7. Scan ranges 按文件粒度 round-robin 切给 N instances（v1 不看文件 bytes）。
8. Scheduler 集中填 `destinations` + `runtime_filter_prober_params` + `per_exch_num_senders`。
9. `data_stream_sink.rs` 删除硬编码 `be_number = 0`，改读 `exec_params.backend_num`。
10. 跨 BE exchange / RF 复用 D1 已有 BE 侧 gRPC 协议；FE 端只填正确的 destinations / prober addrs。
11. 错误处理：`InFlightTracker` 按 BE 分组跟踪；fan-out cancel；错误信息含 `BE[idx] (addr:port)`。
12. 1FE+2BE 同机跑通 SSB 全套 + TPC-H Q5/Q9 + iceberg-rest，与 all-in-one byte-identical。

### 2.2 非目标

- 不做心跳 / 动态注册 BE（D3）。
- 不做 `ADD/DROP/SHOW BACKEND` SQL（D4）。
- 不做 CI 接入（D5）。
- 不做按 bytes 均衡 scan 切分（D2.1 sub-task）；v1 只 round-robin 文件计数。
- 不做并行 cancel fan-out（D2.1 sub-task）；v1 串行调 cancel。
- 不做跨机器部署（v1 只验证同机多端口）。
- 不做 fragment 故障 retry / 重调度（fail-fast 一律）。
- 不动 BE 侧 gRPC server / exchange_sender / runtime/exchange.rs 协议层代码。
- 不动 connector-first 已经定义的 `ConnectorScanPlanner` trait 签名。

### 2.3 D2 PR-0 前置（与 connector-first 阶段 3 后置打通）

Iceberg `ConnectorScanPlanner::to_thrift_scan` 当前是 stub（[src/connector/iceberg/scan_planner.rs:131-143](../../src/connector/iceberg/scan_planner.rs:131) 返回 "codegen still produces HDFS scan ranges via build_hdfs_scan_range_params_for_file"）。D2 PR-0 必须把它填实（参考 StarRocks 实现 [scan_planner.rs:206-230](../../src/connector/starrocks/table/scan_planner.rs:206)），否则 Iceberg 多 instance 路径要么绕过 connector API、要么放弃 Iceberg 多 BE。

## 3. 现状与问题（D1 + connector-first 之后）

[D1 spec](2026-05-27-distributed-cross-process-mvp-design.md) 已经把跨进程框架建好：FragmentDispatcher / InProcessDispatcher / RemoteDispatcher / coordinator submit + fetch / cancel + timeout 错误链路。

但 D1 实现里：
- `RemoteDispatcher` 只持单个 `SocketAddr`，`exchange_addr()` 返回单值（[src/runtime/dispatcher.rs:489-511](../../src/runtime/dispatcher.rs:489)）
- coordinator instance_map 是 `FragmentId → (hi, lo)` 一对一（[src/runtime/coordinator.rs:78-85](../../src/runtime/coordinator.rs:78)），无 per-instance 概念
- `data_stream_sink.rs:973` 硬编码 `let be_number = 0i32`
- `[cluster].backends` 校验 `len() == 1`（D1 锁死）

Connector-first（PR #202）落地了：
- `ConnectorScanPlanner` trait（[src/connector/scan_planning.rs:147-164](../../src/connector/scan_planning.rs:147)）+ `begin_scan` / `plan_splits` / `to_thrift_scan`
- `PlannedConnectorScan { scan, splits }`（[src/sql/codegen/resolve.rs:11-14](../../src/sql/codegen/resolve.rs:11)）
- StarRocks `to_thrift_scan` 已实现；Iceberg 仍 stub（见 §2.3）
- codegen 阶段全量 splits 灌给单 instance（[src/sql/codegen/nodes.rs:644](../../src/sql/codegen/nodes.rs:644)）

D2 不重复 connector-first 已做的事，专注 multi-instance scheduling + cross-BE wiring。

## 4. 核心设计决策

### 4.1 新建独立 `FragmentScheduler` 模块

`src/runtime/scheduler.rs` 新文件。Coordinator 在 build fragments 后、submit loop 前调 `scheduler.assign() + fill_destinations() + fill_runtime_filter_params() + fill_per_exch_num_senders()` 拿到 `SchedulingPlan`。Scheduler **决策不执行**：输出全部 placements 后退场，submit 由 coordinator 按 placements 调 dispatcher。

理由：D3（动态注册）/ D4（SQL 运维命令）/ D5（多 BE CI）都能针对该模块独立写单测；调度策略升级不污染 coordinator。

### 4.2 Instance count 跟随上游

参考 StarRocks `ExecutionDAG.finalizeDAG` + `LocalFragmentAssignmentStrategy`：

- scan fragment（plan 含 scan node）→ N instances
- 非 scan fragment 看入边 partition_type：
  - 任一入边是 `HashPartitioned` / `BucketShuffleHashPartitioned` → 跟随上游 instance 数（典型 = N）
  - 所有入边是 `Unpartitioned` / gather → 1 instance
- 根 fragment 强制 1 instance（接收 gather 做 ResultSink）

理由：StarRocks 长期验证的方案。HASH shuffle 在 N 个 BE 间真实兑现并行度，不是只在 scan 阶段并行。

### 4.3 根 fragment 放置 `backends[query_id % N]`

不是 BE[0] 固定。负载分散；FE 通过 query_id 可预测 root 落点。代价：query trace 时需 log query_id → backend_idx 映射。

### 4.4 复用 connector-first 接口，不引入新 trait

D2 scheduler 通过 `ConnectorRegistry.scan_planner(name)` 拿到 planner，对每个 instance 调 `planner.to_thrift_scan(scan, instance_splits, ctx)` 生成 per-instance scan_ranges。**不**引入 `ScanRangeResolver` 等新 trait。

### 4.5 Wire protocol 保持 byte-identical（D1 决策延续）

NovaRocks-FE → NovaRocks-BE 的 thrift payload 不变。D2 改的只是 FE 端如何**填写**这些字段（`destinations` 真填 N 个 addrs、`backend_num` 真有 idx、`per_node_scan_ranges` 真有切片）。BE 侧 scan operator / exchange / RF 通路完全不动。

### 4.6 Fail-fast 启动期校验

`role=fe` 启动时主动 dial 每个 BE 的 starlet_port + gRPC handshake；任一失败立即报错。D3 心跳到位后可放宽。

## 5. 架构与角色分工

### 5.1 1FE + 3BE 数据流（含 HASH shuffle 例）

```
MySQL client
  │
  ▼
FE
  ├─ 解析 + 优化 + FragmentBuilder（含 connector-first plan_splits 阶段，给 ResolvedTable 装入 PlannedConnectorScan）
  └─ scheduler.assign + fill_destinations + fill_runtime_filter_params + fill_per_exch_num_senders
        │
        ├─ scan fragment：3 instance（per BE）；每个 instance 拿 round-robin 切片 splits
        ├─ HASH 消费者 fragment：3 instance（跟随上游）
        ├─ 根 fragment：1 instance on backends[query_id % 3]
        ├─ destinations 二次填回：上游每个 instance 都拿到下游全部 3 instances 的 (finst_id, brpc_server)
        └─ RF prober_params：build instance 拿到全部 probe instance addrs

coordinator.submit_and_fetch_loop
  │ for each placement: dispatcher.submit_fragment(backend_idx, params)
  │ tracker.record_submitted(backend_idx, finst_id)
  ▼
RemoteDispatcher { clients[3] }
  │ submit_fragment / fetch_result / cancel_fragments by backend_idx
  ▼
BE[0] / BE[1] / BE[2]
  │ 各自 execute_fragment(per-instance params + per-instance scan_ranges)
  │ BE 之间通过现有 gRPC Exchange / TransmitRuntimeFilter 通信（D1 已就位）
  ▼
根 fragment 在 backends[query_id % 3] 上的 ResultBuffer
  │
FE
  │ dispatcher.fetch_result(root_backend_idx, root_finst_id, max_wait_ms)
  ▼
MySQL handler → MySQL client
```

### 5.2 三个进程角色（继承 D1，行为不变）

| 角色 | 端口集 | D2 变化 |
|---|---|---|
| `all-in-one` | MySQL (9030) + gRPC (9070) | 仍走 InProcessDispatcher；行为不变 |
| `fe` | MySQL (9030) | D2 dial 全部 N 个 BE；scheduler / dispatcher 多 backend |
| `be` | gRPC (9070) | 不变（BE 不感知自己 backend_idx） |

## 6. 配置 Schema 变更

### 6.1 TOML `[cluster]` 节

```toml
[cluster]
role = "fe"
# D1: 必须恰好 1 条
# D2: >= 1 条
backends = [
  "be1.internal:9070",
  "be2.internal:9070",
  "be3.internal:9070",
]
advertise_host = ""
advertise_port = 0
```

### 6.2 `ClusterConfig::validate()` 改动

```rust
match self.role {
    ClusterRole::Fe => {
        if self.backends.is_empty() {
            return Err("role=fe requires at least one backend".into());
        }
        for b in &self.backends {
            b.parse::<SocketAddr>().map_err(|e| format!("invalid '{}': {}", b, e))?;
        }
        let mut seen = HashSet::new();
        for b in &self.backends {
            if !seen.insert(b.clone()) {
                return Err(format!("duplicate backend: {}", b));
            }
        }
    }
    ClusterRole::Be | ClusterRole::AllInOne => {
        if !self.backends.is_empty() {
            return Err(format!("role={:?} must not configure backends", self.role));
        }
    }
}
```

### 6.3 启动期 fail-fast

| 校验 | 失败行为 |
|---|---|
| `backends.is_empty()` (fe) | startup error |
| backend 字符串非合法 SocketAddr | startup error，指出哪一条 |
| backends 列表重复 | startup error |
| 任一 backend handshake fail | startup error，列出失败的 BE |

### 6.4 BE 索引

`backend_idx` = backend 在 `[cluster].backends` 列表的 0-based 下标。BE 进程不知道自己的 idx；idx 是 FE 端定义。

## 7. gRPC 协议（不动）

D1 已建的三个 RPC `SubmitFragment` / `FetchResult` / `CancelFragment` 在 D2 完全复用。proto 不改、wire payload 不改、BE handler 不改。

跨 BE Exchange / TransmitRuntimeFilter 通路也在 D1 之前就有（FE-compat 路径用）；D2 不改协议，只让 FE 端把 destinations / prober_addrs 填正确。

## 8. `FragmentDispatcher` Trait 与 `RemoteDispatcher` 改造

### 8.1 Trait 变更

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

D1 → D2 变化：
- ❌ 删 `exchange_addr() -> SocketAddr`
- ✏️ 三个动作方法加 `backend_idx: usize` 首参
- ➕ 新增 `backend_count() -> usize`

### 8.2 InProcessDispatcher

`backend_idx` 必须 == 0；否则 Err。`backend_count()` 返回 1。其他逻辑同 D1。

### 8.3 RemoteDispatcher

```rust
pub struct RemoteDispatcher {
    clients: Vec<NovaRocksGrpcRemoteClient>,
    addrs: Vec<SocketAddr>,
}

impl RemoteDispatcher {
    pub fn new(backends: &[SocketAddr]) -> Result<Self, String> {
        let mut clients = Vec::with_capacity(backends.len());
        for addr in backends {
            let c = NovaRocksGrpcRemoteClient::connect_blocking(*addr)
                .map_err(|e| format!("connect to {}: {}", addr, e))?;
            clients.push(c);
        }
        Ok(Self { clients, addrs: backends.to_vec() })
    }
}
```

submit / fetch / cancel 内部按 backend_idx 索引 `self.clients[idx]`；越界返回 Err；错误信息含 `BE[idx] (addr)`。持久 client 连接，不每次 RPC 重连。

### 8.4 `dispatcher_for_role` 改造

[src/engine/mod.rs:2717-2750](../../src/engine/mod.rs:2717) 当前 D1 实现：

```rust
ClusterRole::Fe => {
    let n = cfg.cluster.backends.len();
    if n != 1 { return Err(format!("expected exactly one backend, got {}", n)); }
    let backend = cfg.cluster.backends[0].parse()?;
    Ok(Arc::new(RemoteDispatcher::new(backend)))
}
```

D2 改为：

```rust
ClusterRole::Fe => {
    if cfg.cluster.backends.is_empty() {
        return Err("role=fe requires non-empty cluster.backends".into());
    }
    let addrs: Vec<SocketAddr> = cfg.cluster.backends.iter()
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("backend addr parse: {}", e))?;
    Ok(Arc::new(RemoteDispatcher::new(&addrs)?))
}
```

## 9. `FragmentScheduler` 模块

### 9.1 数据类型

```rust
// src/runtime/scheduler.rs

#[derive(Clone, Debug)]
pub struct FragmentInstancePlacement {
    pub fragment_id: FragmentId,
    pub instance_index: usize,
    pub finst_id: TUniqueId,
    pub backend_idx: usize,
    pub scan_ranges: BTreeMap<i32 /*plan_node_id*/, Vec<TScanRangeParams>>,
    pub destinations: Vec<TPlanFragmentDestination>,
    pub runtime_filter_prober_params: BTreeMap<i32 /*filter_id*/, Vec<TRuntimeFilterProberParams>>,
    pub per_exch_num_senders: BTreeMap<i32 /*exchange_node_id*/, i32>,
}

pub struct SchedulingPlan {
    pub by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub root_finst_id: TUniqueId,
    pub root_backend_idx: usize,
}

pub struct FragmentScheduler {
    backends: Vec<SocketAddr>,
}
```

### 9.2 API

```rust
impl FragmentScheduler {
    pub fn new(backends: Vec<SocketAddr>) -> Self;

    /// Stage 1: 决定每个 fragment 的 instance count + backend_idx + scan_ranges。
    /// 通过 ConnectorRegistry 调 to_thrift_scan 把 splits 转 thrift scan ranges。
    pub fn assign(
        &self,
        fragments: &[FragmentBuildResult],
        edges: &[FragmentEdge],
        query_id: TUniqueId,
        connectors: &ConnectorRegistry,
    ) -> Result<SchedulingPlan, String>;

    /// Stage 2: 把上游 fragment 的 destinations 填成下游全部 instance 的 (finst_id, brpc_server)。
    pub fn fill_destinations(&self, plan: &mut SchedulingPlan, edges: &[FragmentEdge]);

    /// Stage 3: 给 RF build 端 instance 填 probe 端全部 instance addrs。
    pub fn fill_runtime_filter_params(
        &self,
        plan: &mut SchedulingPlan,
        rf_plan: &RfPlan,
    );

    /// Stage 4: 给每个 fragment instance 算 per_exch_num_senders = sum of upstream instance counts。
    pub fn fill_per_exch_num_senders(
        &self,
        plan: &mut SchedulingPlan,
        edges: &[FragmentEdge],
    );
}
```

### 9.3 Instance count 算法（自底向上拓扑序）

```rust
let mut instance_count: BTreeMap<FragmentId, usize> = BTreeMap::new();
let topo = topological_sort_bottom_up(fragments, edges);

for fid in &topo {
    let fr = fragments.iter().find(|f| f.fragment_id == *fid).unwrap();
    let is_scan = !find_scan_plan_nodes(&fr.fragment).is_empty();

    let count = if is_scan {
        n  // = backends.len()
    } else {
        let incoming = edges.iter().filter(|e| e.target_fragment_id == *fid);
        let mut max_parallel = 1usize;
        for edge in incoming {
            use TPartitionType::*;
            match edge.partition_type {
                HashPartitioned | BucketShuffleHashPartitioned => {
                    max_parallel = max_parallel.max(instance_count[&edge.source_fragment_id]);
                }
                _ => {}
            }
        }
        max_parallel
    };
    instance_count.insert(*fid, count);
}

// 根 fragment 强制 1
instance_count.insert(root_fragment_id, 1);
```

### 9.4 backend_idx + finst_id 分配

```rust
let root_backend_idx = (query_id.lo as usize) % n;

for fr in fragments {
    let count = instance_count[&fr.fragment_id];
    let mut instances = Vec::with_capacity(count);
    for instance_index in 0..count {
        let backend_idx = if count == 1 {
            root_backend_idx
        } else {
            instance_index
        };
        let finst_id = TUniqueId {
            hi: query_id.hi,
            // 低 16 位 instance_index，剩下给 fragment_id；保证 query 内唯一
            lo: ((fr.fragment_id as i64) << 16) | (instance_index as i64),
        };
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
```

### 9.5 Scan splits 按 round-robin 切到 instance

`PlannedConnectorScan { scan, splits }` 实际挂在 [`ResolvedTable.planned_scan`](../../src/sql/codegen/resolve.rs:21)，不直接在 `FragmentBuildResult` 上。Scheduler 需要从 fragment 内的 scan plan node 反查到对应的 `ResolvedTable`：

```rust
for (fragment_id, instances) in by_fragment.iter_mut() {
    let fr = fragments.iter().find(|f| f.fragment_id == *fragment_id).unwrap();
    let scan_plan_nodes = find_scan_plan_nodes(&fr.fragment);  // Vec<(plan_node_id, &ResolvedTable)>

    for (plan_node_id, resolved_table) in scan_plan_nodes {
        let Some(planned) = &resolved_table.planned_scan else { continue; };
        let count = instances.len();
        let planner = connectors.scan_planner(&planned.scan.connector_id)?;

        for (idx, placement) in instances.iter_mut().enumerate() {
            // Round-robin file count: splits[idx], splits[idx+count], splits[idx+2*count], ...
            let instance_splits: Vec<Split> = planned.splits.iter().enumerate()
                .filter(|(i, _)| i % count == idx)
                .map(|(_, s)| s.clone())
                .collect();

            // 调 connector to_thrift_scan 转 thrift
            let ctx = ThriftScanContext { /* ... */ };
            let thrift = planner.to_thrift_scan(&planned.scan, &instance_splits, ctx)?;

            placement.scan_ranges.insert(plan_node_id, thrift.scan_ranges);
        }
    }
}
```

**plumbing 前提**：`FragmentBuildResult` 当前可能不直接持有 `ResolvedTable` 的引用——D2 实现时需要把 `Vec<(plan_node_id, ResolvedTable)>` 或等价的 `scan_plan_nodes_with_planned_scan` 字段挂到 `FragmentBuildResult` 上（由 fragment_builder 在构造阶段填写）。具体 plumbing 方式在 writing-plans 阶段细化。

### 9.6 Destinations 填回

```rust
pub fn fill_destinations(&self, plan: &mut SchedulingPlan, edges: &[FragmentEdge]) {
    for edge in edges {
        let target_instances = plan.by_fragment.get(&edge.target_fragment_id)
            .cloned().unwrap_or_default();
        let dest_list: Vec<TPlanFragmentDestination> = target_instances.iter().map(|t| {
            TPlanFragmentDestination {
                fragment_instance_id: t.finst_id,
                brpc_server: socket_addr_to_thrift(self.backends[t.backend_idx]),
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
```

### 9.7 Runtime Filter prober_params

```rust
pub fn fill_runtime_filter_params(&self, plan: &mut SchedulingPlan, rf_plan: &RfPlan) {
    for rf in &rf_plan.filters {
        let probe_instances = plan.by_fragment.get(&rf.probe_fragment_id)
            .cloned().unwrap_or_default();
        let prober_list: Vec<TRuntimeFilterProberParams> = probe_instances.iter().map(|p| {
            TRuntimeFilterProberParams {
                fragment_instance_id: p.finst_id,
                fragment_instance_address: socket_addr_to_thrift(self.backends[p.backend_idx]),
                ..Default::default()
            }
        }).collect();
        if let Some(build_instances) = plan.by_fragment.get_mut(&rf.build_fragment_id) {
            for b in build_instances.iter_mut() {
                b.runtime_filter_prober_params
                    .entry(rf.filter_id)
                    .or_insert(prober_list.clone());
            }
        }
    }
}
```

### 9.8 per_exch_num_senders

```rust
pub fn fill_per_exch_num_senders(&self, plan: &mut SchedulingPlan, edges: &[FragmentEdge]) {
    for edge in edges {
        let upstream_n = plan.by_fragment.get(&edge.source_fragment_id)
            .map(|v| v.len()).unwrap_or(0);
        if let Some(target_instances) = plan.by_fragment.get_mut(&edge.target_fragment_id) {
            for t in target_instances.iter_mut() {
                *t.per_exch_num_senders
                    .entry(edge.target_exchange_node_id)
                    .or_insert(0) += upstream_n as i32;
            }
        }
    }
}
```

## 10. 跨 BE Exchange + Runtime Filter

### 10.1 Exchange 路径（D1 已就位）

```
BE_A 上 DataStreamSink (sender)
  │ HASH(key) % dest_count → dest_idx
  │ chunk → exchange_sender 队列
  ▼
NovaRocksGrpcRemoteClient::Exchange RPC
  │
  ▼
BE_B 上 NovaRocksGrpc::exchange() handler
  │ 解码 Arrow chunk → 投递 BE_B 的 exchange registry
  ▼
BE_B 上 ExchangeScanOp 拿到 chunk
```

D2 不动这条路径。只让 scheduler 填的 `destinations` 真正含 N 个 BE 地址，HASH 分桶自然走对。

### 10.2 `be_number` 改动

[src/exec/operators/data_stream_sink.rs:973](../../src/exec/operators/data_stream_sink.rs:973) 当前：

```rust
let be_number = 0i32;
```

D2 改为：

```rust
let be_number = self.exec_params.backend_num.unwrap_or(0);
```

`backend_num` 在 coordinator submit 时填进 `TPlanFragmentExecParams.backend_num`，值 = `placement.backend_idx as i32`。

**PR 自检**：grep `backend_num` 全仓所有使用点；任何地方假设 `backend_num == 0` 都要 review。

### 10.3 Runtime Filter 跨 BE

`TRuntimeFilterParams.id_to_prober_params: BTreeMap<i32, Vec<TRuntimeFilterProberParams>>` 协议层已支持多 prober（D1 时只填一条）。

D2 中 scheduler.fill_runtime_filter_params 填全部 probe instance addrs。build BE 算出 RF 后，通过现有 [exchange_sender::transmit_runtime_filter](../../src/service/exchange_sender.rs) 遍历 prober_list 调 `transmit_runtime_filter(addr, params)`。

N=3 时 RF 流量 = N²（每个 build send N 次），v1 接受这个开销。

## 11. 错误处理（D1 扩展为多 BE）

### 11.1 InFlightTracker

```rust
#[derive(Default)]
struct InFlightTracker {
    by_backend: BTreeMap<usize, Vec<TUniqueId>>,
}

impl InFlightTracker {
    fn record_submitted(&mut self, backend_idx: usize, finst_id: TUniqueId) {
        self.by_backend.entry(backend_idx).or_default().push(finst_id);
    }
    fn cancel_all(&self, dispatcher: &dyn FragmentDispatcher) {
        for (idx, ids) in &self.by_backend {
            dispatcher.cancel_fragments(*idx, ids);
        }
    }
}
```

### 11.2 Coordinator 错误骨架（D2 修订版）

```rust
fn execute(self) -> Result<QueryResult, String> {
    let plan = self.scheduler.assign(...)?;
    self.scheduler.fill_destinations(&mut plan, ...);
    self.scheduler.fill_runtime_filter_params(&mut plan, ...);
    self.scheduler.fill_per_exch_num_senders(&mut plan, ...);

    let deadline = compute_deadline(&self.query_options);
    let mut tracker = InFlightTracker::default();

    for (_, instances) in &plan.by_fragment {
        for placement in instances {
            let params = build_exec_plan_fragment_params(placement, ...);
            if let Err(e) = self.dispatcher.submit_fragment(placement.backend_idx, params) {
                tracker.cancel_all(&*self.dispatcher);
                return Err(format!(
                    "submit fragment {} instance {} to BE[{}] ({}): {}",
                    placement.fragment_id, placement.instance_index,
                    placement.backend_idx, self.backends[placement.backend_idx], e
                ));
            }
            tracker.record_submitted(placement.backend_idx, placement.finst_id);
        }
    }

    let mut chunks = Vec::new();
    loop {
        if self.cancel_signal.load(Ordering::Relaxed) {
            tracker.cancel_all(&*self.dispatcher);
            return Err("client disconnected".into());
        }
        let remaining = deadline.checked_duration_since(Instant::now()).unwrap_or_default();
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
                return Err(format!(
                    "fetch_result from BE[{}] ({}): {}",
                    plan.root_backend_idx, self.backends[plan.root_backend_idx], rpc_err
                ));
            }
        }
    }
    Ok(QueryResult::from_chunks(chunks))
}
```

### 11.3 错误源 / 路径

| 错误源 | FE 行为 |
|---|---|
| submit RPC 失败 | tracker.cancel_all → 返回错给 MySQL |
| submit business 失败（status_code != 0） | 同上 |
| FetchResult ERROR | 同上 |
| FetchResult RPC 失败（根 BE 崩溃 / 网络断） | 同上 |
| MySQL client 断连（AtomicBool 信号） | 同上 |
| query_timeout | wallclock deadline → 同上 |
| 非根 BE 崩溃 | 间接：根 BE 上 fragment 等 exchange wait timeout → FetchResult ERROR → 同上（D3 心跳到位前 acceptable） |

### 11.4 Cancel 行为约定

- `cancel_fragments(idx, ids)` 是 idempotent + best-effort
- `cancel_all` 串行调每个 BE 的 cancel（v1 不并行，D2.1 优化）
- 单个 BE cancel 失败（RPC error）不阻塞其他 BE
- 错误信息含 `BE[idx] (addr:port)`，纳入 D2 PR 自检 checklist

## 12. 测试计划

### 12.1 单元测试

| 模块 | 测试 | 位置 |
|---|---|---|
| ClusterConfig::validate (D2) | len >= 1 / 空报错 / 重复报错 | `src/common/app_config.rs` |
| scheduler.assign instance_count | scan=N / HASH=N / gather=1 / 根=1 | `src/runtime/scheduler.rs` |
| scheduler.assign backend_idx | multi 走 instance_index；single 走 query_id%N | `src/runtime/scheduler.rs` |
| Round-robin scan range 切分 | 7 文件切 3 instance 各拿 3/2/2，无重无漏 | `src/runtime/scheduler.rs` |
| fill_destinations | 上游每 instance 拿到下游全 N 个 dest | `src/runtime/scheduler.rs` |
| fill_runtime_filter_params | build prober_params 含 probe 全部 addrs | `src/runtime/scheduler.rs` |
| fill_per_exch_num_senders | N 上游 → 下游计数 = N | `src/runtime/scheduler.rs` |
| RemoteDispatcher new(multi) | N backends connect；一个 fail 整体 Err | `src/runtime/dispatcher.rs` |
| RemoteDispatcher backend_idx 路由 | 调 clients[idx]；越界 Err | `src/runtime/dispatcher.rs` |
| data_stream_sink be_number | 从 exec_params.backend_num 读 | `src/exec/operators/data_stream_sink.rs` |
| InFlightTracker.cancel_all | 全 BE 调用；单 BE fail 不阻塞 | `src/runtime/coordinator.rs` |

### 12.2 调度集成测试（mock dispatcher）

新建 `tests/d2_scheduler_integration.rs`：

| 测试 | 验证 |
|---|---|
| single_scan_single_agg_two_backends | 2 BE 提交 2 scan instance + 1 agg instance |
| hash_shuffle_two_backends | 上游 2 instance destinations 各 2 条 |
| runtime_filter_cross_backends | build prober_params 含 2 addrs |
| submit_half_failure_three_backends | BE[1] 注入 fail，cancel 只调 BE[0] |
| non_root_backend_fetch_error | fetch 始终从 root BE 拉 |

### 12.3 跨进程多 BE 集成测试

扩展 `tests/cluster_mvp.rs`，新增 `tests/cluster_mvp_d2.rs`：

- `cross_process_two_be_smoke` — SSB Q1 byte-identical
- `one_be_kill9_during_query_two_be_fails_cleanly` — BE 崩溃 FE 不挂
- `hash_shuffle_data_correctness_two_be` — HASH agg 2BE byte-identical
- `scan_range_balance_across_backends` — 10 文件分到 2 BE 各 5（±1）
- `runtime_filter_cross_be_correctness` — RF join 2BE 与 1BE byte-identical

### 12.4 SQL 套件验收

`tests/sql-test-runner` 扩展 `--cluster-mode cross-process --cluster-size N`：

```bash
$SQLT --suite ssb --mode verify --cluster-mode cross-process --cluster-size 2
$SQLT --suite tpc-h --mode verify --only q1,q5,q9,q12 --cluster-mode cross-process --cluster-size 2
$SQLT --suite iceberg-rest --mode verify --cluster-mode cross-process --cluster-size 2
$SQLT --suite ssb --mode verify --cluster-mode cross-process --cluster-size 3  # 抽样
```

| 套件 | 模式 | 标准 |
|---|---|---|
| SSB 13 个查询 | cross-process, cluster-size=2 | 与 all-in-one byte-identical |
| TPC-H Q5/Q9 | cross-process, cluster-size=2 | byte-identical（跨 BE HASH join 关键） |
| TPC-DS Q4/Q67 | cross-process, cluster-size=2 | byte-identical |
| 现有 sql-test suite | all-in-one（回归 gate） | 不允许新 fail |
| iceberg-rest | cross-process, cluster-size=2 | 通过 |
| SSB | cross-process, cluster-size=3 | 抽样通过 |

### 12.5 性能 sanity check（非验收门槛）

```bash
time mysql --port=$ALL_IN_ONE_PORT -e "$(cat sql-tests/tpc-h/sql/q5.sql)"
time mysql --port=$FE_PORT_2BE -e "$(cat sql-tests/tpc-h/sql/q5.sql)"
```

期望：2 BE 不会比 all-in-one 快 2x（RPC 开销），但**应该比 1FE+1BE 快**。性能基准 D2.1 严格化。

### 12.6 非目标

- 心跳 / 动态拉起停掉 BE（D3）
- ADD/DROP BACKEND SQL（D4）
- CI 接入（D5）
- 跨机器部署（v1 只同机多端口）

## 13. 验收标准

- 1FE + 2BE 同机跨进程跑通 SSB 全套 byte-identical
- 1FE + 2BE TPC-H Q5/Q9 byte-identical（跨 BE HASH join 关键 case）
- 1FE + 2BE iceberg-rest smoke 通过
- 1FE + 3BE SSB 抽样通过
- 现有 all-in-one suite 不回归
- BE 崩溃测试通过（FE 进程不挂）
- HASH shuffle 跨 BE 数据正确性测试通过
- 跨 BE RF 数据正确性测试通过
- scan ranges 在 BE 间近似均衡（差距 ≤ 1 文件）
- `data_stream_sink.rs` 硬编码 `be_number = 0` 删除
- coordinator 错误信息含 `BE[idx] (addr:port)`
- D2 PR-0：Iceberg `ConnectorScanPlanner::to_thrift_scan` 已填实

## 14. 风险与备选

### 14.1 风险

- **Iceberg `to_thrift_scan` 必须先填实**（PR-0）：拖住 D2 多 instance Iceberg 路径。建议参考 StarRocks 实现快速跟进。
- **HASH shuffle 跨 BE 错分桶**：sender_id / be_number / dest_id 三层身份在 D1 单 BE 复用同一 RuntimeState；跨 BE 后必须显式通过 ExecParams 正确分配。需要 SQL test 覆盖 HASH 边界 case（NULL key / 空表 / 单值 / 不均衡 key）。
- **非根 BE 崩溃感知延迟**：依赖 exchange wait timeout（几秒到几十秒），D3 心跳到位前 acceptable，但 v1 测试时要意识到这个延迟。
- **scan range 切分不均**：v1 round-robin 文件计数，文件大小差异大时 BE 负载差异大。已规划 D2.1 sub-task 按 bytes 均衡。
- **per_exch_num_senders 错算**：D1 是单 BE 单 instance，规则简单；D2 多 instance 后规则改为 `+= upstream_count`，容易写错。必须有 unit test 覆盖。
- **all-in-one 模式不能回归**：所有改动需要在 InProcessDispatcher 走 backend_idx=0 路径仍跑通现有 all-in-one suite。

### 14.2 备选与已拒绝方案

- **scan=N，其他=1**（D2 brief 原版策略）：已拒绝——HASH shuffle 后聚合只在 1 BE 执行，N 个 BE 并行只兑现在 scan 阶段；浪费跨 BE 设施。改采 StarRocks 同款"instance 跟随上游"。
- **scheduler 内置 fan-out cancel（trait 内 fan-out）**：已拒绝——cancel 路径需要 `finst_id → backend_idx` 映射，这个信息只在 coordinator 内有；dispatcher 不该知道 placement 细节。
- **每次 RPC 创建新 client（D1 行为延续）**：已拒绝——N=3 时连接开销 ×3；D2 改成持久 client 数组。
- **BE 自己知道自己的 backend_idx**：已拒绝——BE 应无状态，可横向加（D3）；FE 单方面定义索引；BE 重启换端口由 D3 身份 token 解决。
- **D2 自己做 scan operator 重构 + scan ranges 上移**：已拒绝——connector-first 阶段 2+3 已经做完。D2 直接复用 PlannedConnectorScan + ConnectorScanPlanner，不重新设计。
- **D2 v1 跳过 scan 切分只做 multi-BE shuffle**：已拒绝——SSB / TPC-H 验收无法证明"多 BE 真正并行"；scan 切分是 D2 必经。

## 15. 代码引用索引

### D1 已就位的（D2 复用）

- 三个 gRPC RPC：[src/service/grpc_server.rs:236-367](../../src/service/grpc_server.rs:236)
- gRPC client：[src/service/grpc_client.rs](../../src/service/grpc_client.rs)
- BE exchange / RF：[src/runtime/exchange.rs](../../src/runtime/exchange.rs)、[src/service/exchange_sender.rs](../../src/service/exchange_sender.rs)
- coordinator：[src/runtime/coordinator.rs](../../src/runtime/coordinator.rs)
- dispatcher trait（D2 修改）：[src/runtime/dispatcher.rs](../../src/runtime/dispatcher.rs)
- exec_params 构造：[src/runtime/exec_params.rs](../../src/runtime/exec_params.rs)
- cluster config / CLI：[src/common/app_config.rs](../../src/common/app_config.rs)、[src/main.rs](../../src/main.rs)
- cluster_mvp 测试基建：[tests/cluster_mvp.rs](../../tests/cluster_mvp.rs)

### Connector-first 已就位的（D2 复用）

- `ConnectorScanPlanner` trait：[src/connector/scan_planning.rs:147-164](../../src/connector/scan_planning.rs:147)
- `PlannedConnectorScan`：[src/sql/codegen/resolve.rs:11-14](../../src/sql/codegen/resolve.rs:11)
- `ConnectorRegistry.scan_planner`：[src/connector/mod.rs:250-255](../../src/connector/mod.rs:250)
- StarRocks `to_thrift_scan`：[src/connector/starrocks/table/scan_planner.rs:206-230](../../src/connector/starrocks/table/scan_planner.rs:206)
- Iceberg scan planner（`to_thrift_scan` 仍是 stub，D2 PR-0 填实）：[src/connector/iceberg/scan_planner.rs](../../src/connector/iceberg/scan_planner.rs)
- codegen 接入：[src/sql/codegen/fragment_builder.rs](../../src/sql/codegen/fragment_builder.rs)、[src/sql/codegen/nodes.rs](../../src/sql/codegen/nodes.rs)

### D2 新建文件

- `src/runtime/scheduler.rs`（trait `FragmentScheduler` + `FragmentInstancePlacement` + `SchedulingPlan` + 4 个 fill_* 函数）
- `tests/d2_scheduler_integration.rs`（mock dispatcher 单元）
- `tests/cluster_mvp_d2.rs`（跨进程 1FE+2BE）

### D2 修改文件

- `src/common/app_config.rs`：放宽 ClusterConfig::validate
- `src/engine/mod.rs`：dispatcher_for_role 支持多 backend
- `src/runtime/dispatcher.rs`：trait 加 backend_idx；RemoteDispatcher 多 backend
- `src/runtime/coordinator.rs`：接入 scheduler；InFlightTracker
- `src/runtime/exec_params.rs`：把 placement 字段塞进 TExecPlanFragmentParams
- `src/exec/operators/data_stream_sink.rs:973`：be_number 从 exec_params.backend_num 读
- `src/connector/iceberg/scan_planner.rs`：填实 to_thrift_scan（PR-0）
- `tests/sql-test-runner/src/cluster.rs`、`tests/sql-test-runner/src/main.rs`：扩展 --cluster-size N
