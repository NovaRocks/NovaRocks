# D3 + D4 设计：动态注册 + 故障健壮性 + 集群运维面 + 监控

- 日期：2026-06-10
- 阶段：Standalone Distributed Execution / 健壮性 + 运维产品面
- 依赖：D2（多 BE 并行执行，已合并，PR #209）
- 范围：本 spec 合并覆盖 roadmap 的 D3（动态注册 + 故障健壮性）与 D4（集群运维面 + 监控），共享同一核心数据结构 `BackendRegistry`。
- 实现节奏：单一实现计划，连续约 12 个 PR；D3 控制面在前，D4 运维面在后。

---

## 1. 背景与问题

D2 已经让 standalone 模式能以 `1 FE + N BE`（N ≥ 1）在单机多端口下做分布式执行：`FragmentScheduler` 决定每个 (fragment, instance) 落哪个 BE，`RemoteDispatcher` 按 `backend_idx` 路由提交，`InFlightTracker` 按 backend 分组以便取消。BE 列表来自静态 TOML `[cluster].backends`，启动期 `probe_all_backends()` 以 3s TCP 超时拨测可达性。

但 D2 没有「BE 是否在线」的概念，也没有把内核能力暴露成可运维产品：

- 没有心跳/健康追踪：BE 启动晚了或中途挂了，FE 仍会调度到不可用目标。
- cancel 串行、且 sender RPC 失败只 `log + 设本地 error_state`、不跨节点传播——远端失联只能靠 receiver 超时被动发现（硬伤）。
- 没有 `ADD/DROP/SHOW BACKEND` SQL、没有 decommission、没有任何 Prometheus 指标。
- `[cluster].advertise_host / advertise_port` 字段已声明但**未接 TOML、未使用**。

D3 补齐控制面（健康追踪 + 干净故障路径），D4 把内核能力暴露成运维产品面（SQL 管理 + 监控）。

---

## 2. 已确认的关键设计决策（含理由）

| # | 决策 | 选择 | 理由 |
|---|---|---|---|
| D-1 | 文档/实现边界 | **D3+D4 合并一份 spec，连续实现** | 共享 `BackendRegistry` 控制面，一次设计到位 |
| D-2 | 心跳/注册方向 | **FE-pull（StarRocks 模型），周期 unary** | 与 D2 既有 FE→BE 单向一致；StarRocks `HeartbeatMgr`/`SystemInfoService`/`BackendHbResponse` 可直接移植；BE 无需知道 FE 地址；启动顺序无关自然满足；现有 `StarletHeartbeat` 即 unary 先例。**取消** roadmap 原 `RegisterBackend` RPC——「注册 = 首次心跳成功」 |
| D-3 | BE 身份 | **逻辑 be_id（复用 D2 `backend_idx`）+ host:port endpoint 属性 + start_epoch** | 身份与网络地址解耦，单机伪分布式（同机多进程不同端口）成为一等公民；epoch 处理同端口重启检测；无需 id 分配握手（FE 分配，经 exec_params 下发，复用 D2 `be_number`） |
| D-4 | BE 列表持久化 | **sqlite 持久化（`metadata_db_path`），TOML 仅 bootstrap seed，SQL 为权威源** | sqlite 已在用（managed-lake），边际成本低；`ADD/DROP BACKEND` 跨 FE 重启生效；be_id 跨重启稳定 |
| D-5 | 心跳传输 | **周期 unary（5s，FE 线程池并发）** | 由 D-2 派生；streaming 在此场景只增不减复杂度 |
| D-6 | decommission 长尾 | **timeout 强移 + `DROP ... FORCE` 双保险** | 纯 timeout 会被卡死查询拖住；纯 FORCE 不够优雅 |
| D-7 | advertise 端口语义 | **= 本节点 NovaRocksGrpc 端口（FE 拨它发 Heartbeat/SubmitFragment/Exchange，当前 `starlet_port`）** | 单一控制方向，单一 advertise 端点 |
| D-8 | 调度并发安全 | **调度照常下发 + RPC 失败由 query 层处理**（roadmap 倾向） | D3-5/6 补齐失败传播后即安全；比「等下一轮心跳」更可预测 |

---

## 3. 架构骨架

### 3.1 四个面（核心心智模型）

D2 只有「② fragment 控制面 + ④ exchange 数据面」。D3/D4 增加「① 集群心跳面 + ③ fragment 状态面」。**集群健康（周期 5s）与 query 内 fragment 状态（按事件）是两条独立通道**——这是整个设计的分水岭。

| 面 | 方向 | 频率 | 归属 | 现状 |
|---|---|---|---|---|
| ① 集群心跳面 | FE→BE pull unary | 周期 5s | D3 新建 | 无（FE-compat 那条是反向） |
| ② fragment 控制面 | FE→BE | 每 query | D2 有，D3 扩 | SubmitFragment/Cancel 已存在；cancel 串行、失败不传播 |
| ③ fragment 状态面 | BE→FE-coordinator | 每 fragment 事件 | D3 新建 | 无（standalone 下 coordinator 只等 root fetch） |
| ④ exchange 数据面 | BE↔BE | 数据流 | D2 有，D3 补清理 | sender RPC 失败只 log、不传播 cancel（硬伤） |

### 3.2 拓扑

```text
            standalone role=fe (coordinator)
   ┌─────────────────────────────────────────────────────┐
   │  BackendRegistry  ──persist──▶  sqlite(metadata_db)   │
   │  HeartbeatMgr (线程池, 5s)                            │
   │  QueryStateMachine + InFlightTracker(按 be_id 分组)   │
   │  axum http_port: /metrics (novarocks_*)               │
   └───┬───────────────┬───────────────┬──────────────────┘
   ①心跳│         ②Submit/Cancel│      ③ReportFragmentStatus│(BE→FE)
       ▼               ▼               ▲
   ┌───────────┐   ┌───────────┐   ┌───────────┐
   │ BE#0 :9061│   │ BE#1 :9062│   │ BE#2 :9063│   ← 单机伪分布式=改端口
   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
         └────④ exchange shuffle (BE↔BE) ┘
```

### 3.3 BackendRegistry 数据模型

```rust
// src/runtime/backend_registry.rs（新建）
type BeId = u32; // 复用 D2 backend_idx 语义；FE 分配；持久化

struct BackendEntry {
    be_id: BeId,
    endpoint: SocketAddr,   // advertise host:port（FE 拨它）；单机=同 host 不同端口
    state: BackendState,
    start_epoch: u64,       // BE 每次心跳上报的进程启动 nonce；变了 = 重启
    last_heartbeat_ms: i64,
    missed_heartbeats: u32,
    last_err: Option<String>,
    // D4 监控/SHOW 用
    version: String,
    num_cores: u32,
    scheduled_fragments: u64, // 累计调度数
}

enum BackendState { Registering, Live, Lost, Decommissioning }

struct BackendRegistry {
    inner: Mutex<BTreeMap<BeId, BackendEntry>>,
    store: SqliteBackendStore,    // D4 接入；D3 阶段可先 in-memory
    next_be_id: AtomicU32,
}
```

### 3.4 状态机

```text
            首次心跳成功                      DROP BACKEND
  Registering ──────────▶ Live ───────────────────────────▶ Decommissioning
      ▲                   │  ▲                                    │
      │ ADD BACKEND       │  │ 心跳恢复                            │ in-flight 跑完
      │ (sqlite+seed)     │  │                                    │ / 超时强移 / FORCE
      │           N 次漏跳 │  │                                    ▼
      └─── (新进程注册) ── Lost ─────────────────────────────▶  removed
                          ・epoch 变化 ⇒ 判定重启 ⇒ 强制 fail 该 BE in-flight query + 清 exchange key
```

- **调度过滤**：scheduler 只选 `Live`；候选集空 ⇒ 新 query 显式失败 `no live backend available`。`Decommissioning` 排除出新 query 但跑完在途。
- **失联路径**：漏跳 N 次（默认 3 × 5s）→ `Lost` → 对每条「在该 be_id 上有 fragment」的在途 query：fail query、**并行**广播 CancelFragment 给其余参与 BE、强制 purge exchange key。
- **重启检测**：be_id 相同但 start_epoch 变 ⇒ 判定重启 ⇒ 强制清旧 epoch 残留，然后转 `Live`。

### 3.5 挂进现有 D2 代码的位置（最小侵入）

- `BackendRegistry` / `HeartbeatMgr` 新建于 `src/runtime/`（与 `scheduler.rs`/`dispatcher.rs`/`coordinator.rs` 同层）。
- `scheduler.rs` 的 `backends: Vec<SocketAddr>` 改为规划时向 registry 快照 `Live` 集合（决策层与 registry 解耦）。
- `coordinator.rs` 的 `InFlightTracker`（已按 backend 分组）接 registry 失联事件 + 承载 ③ 的 query 状态机。
- 心跳/cancel/status 走新 gRPC 方法，加在 `idl/proto/starust_grpc.proto`（已有 `StarletHeartbeat` unary 先例）。

---

## 4. D3：故障健壮性

> FE-pull 调整：roadmap 原 `RegisterBackend` RPC 取消；BE 不需要知道 FE 地址。

### D3-1 · Heartbeat RPC + BackendRegistry + HeartbeatMgr（FE 侧）

新增 unary gRPC（`starust_grpc.proto`）：
```proto
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
// Req:  { fe_epoch, assigned_be_id }
// Resp: { start_epoch, be_port, brpc_port, exchange_port, version, num_cores, status }
```
- `BackendRegistry`：启动时从 sqlite 加载 + 与 TOML `[cluster].backends` 合并去重（D4 接入持久化；D3 阶段可先 TOML-only 内存态）。
- `HeartbeatMgr`：tokio 周期任务（5s），对每个 entry 并行发 Heartbeat（有界并发，仿 StarRocks `HeartbeatMgr`），按响应驱动状态机：成功 → `Registering/Lost → Live`、重置 `missed_heartbeats`、更新 `last_heartbeat_ms`/version/cores；失败 → `missed_heartbeats++`，达阈值 → `Lost`（发失联事件）。

### D3-2 · BE 侧 Heartbeat handler + start_epoch

- BE 在 `grpc_server.rs` 实现 Heartbeat handler，回 `start_epoch`（进程启动时生成一次的 nonce，存 `OnceLock`）+ 端口/版本/核数。
- **就绪门自动成立**：BE 没起来不会应答心跳 ⇒ FE 不标 Live ⇒ 不调度。无需 roadmap 原「注册后才接 SubmitFragment」的显式门。

### D3-3 · scheduler 只调度 Live

- `scheduler.rs` 改为规划时向 registry 快照 `Live` 集合（be_id + endpoint）。
- 候选集空 ⇒ 新 query 显式失败 `no live backend available`。`Decommissioning` 排除。
- 并发安全（决策 D-8）：快照后某 BE 恰好失联，SubmitFragment RPC 失败 → 触发 query fail + 清理（D3-5/6 保证安全）。

### D3-4 · 跨 BE cancel 并行广播（修硬伤 #1）

- `cancel_query` 从串行改为并行 fan-out CancelFragment 给所有参与 BE（`join_all`）。参与 BE 来自 `InFlightTracker`（已按 backend 分组）。
- Cancel 带 `start_epoch` 且幂等：重启后的新 BE 忽略旧 epoch 的 cancel，避免误杀新进程无关 query。

### D3-5 · ReportFragmentStatus + FE query 状态机（状态面）

新增 unary（BE→FE-coordinator）：
```proto
rpc ReportFragmentStatus(FragmentStatusReport) returns (StatusAck);
// { finst_id, be_id, start_epoch, state: RUNNING|FINISHED|FAILED|CANCELLED, error? }
```
- 复用现有上报基建：`exec_state_reporter.rs` 双队列 + `fe_report.rs` REPORT_REGISTRY 本就是 BE→coordinator 上报，这里把目标从 StarRocks FE 换成 NovaRocks FE coordinator。
- FE 侧：每 query 一个状态机 `INIT→RUNNING→{FINISHED|FAILED|CANCELLED}→CLEANED` + in-flight 表（`be_id→finst_ids`、`finst_id→status`），挂进 `query_context.rs`。
- 价值：非 root fragment 在 BE#2 崩了，无须等 root 的 exchange 读超时——任一 fragment FAILED 上报 ⇒ coordinator 快速失败并带真实错误 ⇒ 触发 D3-4 广播 cancel。

### D3-6 · BE 失联检测 + 强制清理（修硬伤 #2）

- 漏跳 N 次 ⇒ `Lost` ⇒ registry 发「backend lost」事件。
- 事件处理：对 in-flight 表里「在该 be_id 上有 fragment」的每条 query → fail query → 并行 cancel 其余 BE → **强制 purge 该 query 的 exchange key**。
- 直接补硬伤：`exchange_sender.rs` 中 RPC 失败处从「只 log + 设本地 error_state」改为**上抛到 query-level error**，receiver 不再干等满超时。
- epoch 变化（重启）走同一条强制清理路径，清旧 epoch 残留。

### D3-7 · 故障注入测试

扩展 D2 已有的 sql-test-runner `--cluster-size N` 基建：

| 场景 | 注入 | 验收 |
|---|---|---|
| `kill-be-during-query` | SELECT 中途 `kill -9` 一个 BE | query 干净失败、错误明确；新 query 不再调度到它 |
| `network-partition-during-query` | 阻断心跳+exchange | 转 `Lost`、query 失败、无 exchange key 泄漏 |
| `heartbeat-delay` | 心跳延迟但在容忍内 | 不误判 Lost（防 false-positive） |
| `be-restart` | kill 后原端口重启 | epoch 变化被识别 ⇒ 旧残留清掉 ⇒ BE 重新 Live 被新 query 调度 |

### D3 验收标准

- 启动顺序无关：FE 先起 / BE 先起都能正确建立 cluster。
- BE 中途 `kill -9`：在途 query 干净失败、错误明确；新 query 不再调度到该 BE。
- BE 恢复后能重新 Live 并被新 query 调度。
- `kill-be-during-query` 与 `network-partition-during-query` 两场景在 runner 通过。
- 调度候选集为空时新 query 显式失败，错误含 `no live backend available`。

---

## 5. D4：集群运维面 + 监控

### D4-1 · parser/analyzer：ADD/DROP/SHOW BACKEND

- custom dialect 层（仿 `materialized_view.rs` 的 `looks_like_*` / `parse_*`），新建 `src/sql/parser/dialect/backend.rs`。
- AST 加 `AddBackendStmt{addr}` / `DropBackendStmt{addr, force}` / `ShowBackendsStmt` 到 `src/sql/parser/ast/mod.rs`。
- 语法：`ADD BACKEND 'host:port'` / `DROP BACKEND 'host:port' [FORCE]` / `SHOW BACKENDS`。

### D4-2 · dispatch：路由到 BackendRegistry 写路径

- `dispatch_statement`（`src/engine/mod.rs`）加 3 个 match arm → registry 写路径，结果走 QueryResult（仿 `list_mvs`）。
- ADD BACKEND（pull 语义）：endpoint 入期望集 → 分配 be_id → 写 sqlite → HeartbeatMgr 开始拨它 → 首次心跳成功转 `Live`。不等 BE 主动 push。
- 角色守卫：ADD/DROP 仅 `role=fe` 合法；`role=be` 无 MySQL server 无法下发；FE-compat（brpc/thrift）路径若触达则报错「backend management is owned by StarRocks FE」。`SHOW BACKENDS` 在 `fe` + `all-in-one`（显示 1 个隐式本地 BE）均可。
- 双写权威源：启动时 sqlite ∪ TOML `[cluster].backends` 合并去重，SQL 注册为权威源，TOML 仅 bootstrap seed。

### D4-3 · decommission 状态机（timeout + FORCE 双保险）

- `DROP BACKEND` → `Decommissioning`：scheduler 跳过、不接新 query；在途 fragment 跑完后正式从 registry + sqlite 移除。
- `[cluster].decommission_timeout_secs`（默认 300s）：超时强制移除，残留在途 query 失败。
- `DROP BACKEND 'addr' FORCE`：立即移除，在途 query 立即失败。

### D4-4 · Prometheus /metrics（novarocks_*）

- 加 `prometheus` crate（Cargo.toml 目前无），全局 Registry；在 axum `build_novarocks_http_app`（`grpc_server.rs`）挂 `.route("/metrics", get(handler))`，复用 `http_port`。

| 指标 | 类型 | 来源 |
|---|---|---|
| `novarocks_fragment_scheduled_total`（调度 QPS） | counter | 复用 `SUBMIT_FRAGMENT_CALLS` atomic |
| `novarocks_exchange_shuffle_bytes_total` | counter | `exchange_sender.rs` `ExchangeSendTracker` 加累计计数 |
| `novarocks_heartbeat_rtt_seconds` | histogram | HeartbeatMgr 调用前后 `Instant`（FE 侧测） |
| `novarocks_live_backends` | gauge | registry `Live` 计数 |
| `novarocks_backends{state=...}` | gauge | registry 各状态计数（附带） |
| `novarocks_fragment_exec_duration_seconds` | histogram | fragment 完成时长分位（附带，喂自 profile） |

- 前 4 个为验收必需核心指标；指标名稳定性纳入 PR 自检 checklist。

### D4-5 · advertise 校验

- 给 `app_config.rs` 的 `[cluster].advertise_host/advertise_port` 补 serde（目前声明了未接 TOML）。
- 语义（决策 D-7）：advertise endpoint = 本节点对外可达的 NovaRocksGrpc 端口（FE 拨 BE 发 Heartbeat/SubmitFragment/Exchange，当前 `starlet_port`）；FE 的 `[cluster].backends` 条目即各 BE 的 advertise endpoint。
- fallback：`advertise_host` 空 → 复用 `network.rs` `choose_advertise_host` 走 priority_networks CIDR 解析；`advertise_port` 空 → 取该角色 gRPC 端口。
- 启动自拨校验：节点 bind 后主动 dial 自己的 advertise endpoint，失败 fail-fast，错误含 `failed to reach advertised endpoint`（复用 `probe_*` / `wait_for_tcp_ready`）。

### D4-6 · 集成测试

- 3 节点（单机伪分布式，be_id 0/1/2 不同端口）：`ADD BACKEND`（新进程上线→Live→被新 query 调度）/ `DROP BACKEND`（在途不被强杀、跑完移除）/ `DROP ... FORCE` / scrape `/metrics` 断言 4 核心指标。
- 复用 sql-test-runner `--cluster-size N`；`SHOW BACKENDS` 输出与 registry 实时一致。

**SHOW BACKENDS 列**：`BackendId | Host | GrpcPort | State | Alive | LastHeartbeatMs | StartEpoch | Version | NumCores | ScheduledFragments | ErrMsg`

### D4 验收标准

- 3 机部署上能用 SQL 加/减节点；新调度自动感知 `Live` 变化。
- `DROP BACKEND` 后在途 query 不被强杀、新 query 不再调度；在途跑完后正式移除。
- 4 核心指标（调度 QPS / shuffle 字节 / 心跳 RTT / live BE 数）在 `/metrics` 可见。
- advertise 配错（错 IP / 防火墙挡端口）启动失败，错误含 `failed to reach advertised endpoint`。
- `SHOW BACKENDS` 输出全字段，state 与 registry 实时一致。

---

## 6. 配置 schema 变更

```toml
[cluster]
role = "fe"                         # 已有
backends = ["127.0.0.1:9061", ...]  # 已有；bootstrap seed
advertise_host = ""                 # 接通：空则 priority_networks 解析
advertise_port = 0                  # 接通：0 则取角色 gRPC 端口
heartbeat_interval_ms = 5000        # 新增
heartbeat_timeout_retries = 3       # 新增；漏跳 N 次判 Lost
decommission_timeout_secs = 300     # 新增
```

---

## 7. gRPC / proto 新增（`idl/proto/starust_grpc.proto`，编译走 `src/build.rs` tonic_build）

- `Heartbeat(HeartbeatRequest) -> HeartbeatResponse`（FE→BE，unary）
- `ReportFragmentStatus(FragmentStatusReport) -> StatusAck`（BE→FE-coordinator，unary）
- `CancelFragment` 扩展：请求体加 `start_epoch`（幂等 + 防误杀重启进程）

服务端实现在 `grpc_server.rs`，客户端 blocking/async wrapper 在 `grpc_client.rs`，沿用现有 channel 缓存与超时配置。

---

## 8. 实现顺序（约 12 PR，连续）

**Phase A — D3 控制面**
1. PR-1 plumbing：`[cluster].advertise_*` serde + `start_epoch` + `Heartbeat` proto + BE 侧 Heartbeat handler（FE 行为不变，可独立测）
2. PR-2 `BackendRegistry` + `HeartbeatMgr`（FE pull 循环 + 状态机，TOML seed，内存态）
3. PR-3 scheduler 接 `Live` 集合 + 空候选集 fail
4. PR-4 `ReportFragmentStatus` proto + BE→FE 上报 + FE query 状态机 + in-flight 表
5. PR-5 cancel 并行 fan-out + sender-RPC-失败上抛 query-level（硬伤 #1）
6. PR-6 BE-lost 事件 → 强制 fail query + purge exchange key + epoch 重启清理（硬伤 #2）
7. PR-7 故障注入测试（kill-be / partition / heartbeat-delay / be-restart）

**Phase B — D4 运维面**
8. PR-8 parser ADD/DROP/SHOW BACKEND + dispatch + 角色守卫 + sqlite 持久化（合并 TOML seed）
9. PR-9 decommission 状态机 + timeout + FORCE
10. PR-10 Prometheus `/metrics` + 4 核心指标
11. PR-11 advertise 自拨校验 + priority_networks fallback
12. PR-12 3 节点集成测试（ADD/DROP/metrics scrape）

---

## 9. 非目标

- 不做 fragment 故障 retry / 重调度（v1 fail-fast）。
- 不做高级 failure detector（phi accrual 等），N 次心跳超时即可。
- 不做 replica / data layout（standalone 下 Iceberg / 本地 parquet 不依赖 BE 副本）。
- 不做权限 / RBAC、不做高可用多 FE 选主、不做 Grafana dashboard 模板、不做 BE-level 资源配额/隔离。
- 不做 BE 重启后历史 query 状态保留（重启即新 epoch）。
- 不做 D2.1 的 scan split 按字节均衡（独立子任务）。

---

## 10. 风险点

- **心跳与调度并发安全**：采用「调度照常下发 + RPC 失败由 query 层处理」（D-8）。依赖 D3-5/6 的失败传播；PR 顺序保证 PR-5/6 在 scheduler 接 Live（PR-3）之后落地前，scheduler 的 RPC 失败路径要先有最小 fail 处理。
- **fragment instance 残留**：BE 崩溃后必须强制 purge exchange key（D3-6）；epoch 是关键抓手，否则同 ID 复用冲突。
- **be_id 持久化与 TOML 合并**：SQL 为权威源；合并去重逻辑要覆盖「TOML 有、sqlite 已 DROP」的条目不应复活——以 sqlite 的删除标记为准。
- **心跳风暴**：N 个 BE × 5s 对 FE 影响小，但 `HeartbeatMgr` 用有界并发线程池，避免 N 大时打满。
- **decommission 长尾**：timeout 强移 + FORCE 双保险（D-6）。
- **指标命名空间**：`novarocks_*`，指标名稳定性纳入 PR 自检。
- **FE-compat 模式区分**：`ADD/DROP BACKEND` 在 FE-compat 模式报错，不可直接写本地注册表。

---

## 11. 参考实现（StarRocks，`~/project/starrocks`）

- `fe/fe-core/.../system/SystemInfoService.java`——backend registry（`idToBackendRef`）
- `fe/fe-core/.../system/HeartbeatMgr.java`——FE 主动心跳线程池
- `fe/fe-core/.../system/Backend.java` / `ComputeNode.java`——BE 状态机模型
- `fe/fe-core/.../system/BackendHbResponse.java`——心跳响应体
