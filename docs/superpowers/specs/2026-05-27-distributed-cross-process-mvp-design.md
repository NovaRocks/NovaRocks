# D1：跨进程执行最小闭环 — 设计

日期：2026-05-27
状态：已确认待评审
对应 roadmap 任务：[`distributed-cross-process-mvp`](../../../../Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-cross-process-mvp.md)（Standalone Distributed Execution Roadmap 第 D1 项）

---

## 1. 概述

把 NovaRocks 的 `standalone-server` 从单进程一体架构拆出 `fe` / `be` 两个进程角色，coordinator 不再用 `std::thread::spawn` 跑非根 fragment、不再用 foreground 跑根 fragment；统一改造为通过 NovaRocks 原生 gRPC RPC 把每个 fragment 提交到目标 BE。目标拓扑是同机 **1 FE + 1 BE**，跑通现有 SQL 套件，输出与默认 `all-in-one` 模式 byte-identical。

这是 Standalone Distributed Execution Roadmap 的奠基性一步：D2-D5 的多 BE 调度 / 心跳 / 运维 / CI 全部建立在 D1 打开的跨进程边界之上。

## 2. 目标与非目标

### 2.1 目标

1. 拆出 `--role fe | be | all-in-one` 三个进程角色，`all-in-one` 默认值，行为不变。
2. 新增三个 NovaRocks 原生 gRPC RPC：`SubmitFragment` / `FetchResult` / `CancelFragment`，加到现有 `NovaRocksGrpc` service。
3. wire-level payload 与 StarRocks BE 协议保持 byte-identical：复用 `TExecPlanFragmentParams` thrift 类型，gRPC 仅作为 transport。
4. 引入 `FragmentDispatcher` trait，`InProcessDispatcher` 和 `RemoteDispatcher` 两个实现，coordinator 不感知 transport 选择。
5. coordinator 内部清理：根 fragment 不再有 foreground 特殊路径，所有 fragment 统一走 submit + fetch。
6. 1 FE + 1 BE 同机跨进程跑通 SSB / TPC-H Q1/Q5/Q9/Q12 / iceberg-rest smoke，与 all-in-one byte-identical。
7. 错误路径明确：submit 半失败 / BE 崩溃 / MySQL 断连 / query timeout 都干净失败、不挂死。

### 2.2 非目标

- 不做多 BE 调度（D2 范围）。
- 不做 BE 心跳 / 动态注册（D3 范围）。
- 不做 `ADD/DROP/SHOW BACKEND` SQL（D4 范围）。
- 不做 CI 接入（D5 范围）。
- 不动 FE-compat 模式（C++ shim + thrift FFI 的 `run/start/stop/restart` 路径）。
- 不引入 protobuf 化的 plan/expr/exec_params 定义；wire 层完全沿用现有 thrift IDL。
- 不引入 fragment 故障 retry / 重调度。FE 失联 / BE 崩溃一律 fail-fast。
- 不优化 fragment-level batched submit；`submit_exec_batch_plan_fragments` 不在 D1 dispatcher 接口暴露，D2 再加。
- 不做跨进程性能 benchmark 作为验收门槛；只在 D1 完成时做一次 sanity check。

## 3. 现状与问题

`src/runtime/coordinator.rs` 当前有两条独立路径：

- **非根 fragment**（[coordinator.rs:460](../../src/runtime/coordinator.rs)）：`std::thread::spawn(execute_fragment)` 进程内起线程。
- **根 fragment**（[coordinator.rs:488-565](../../src/runtime/coordinator.rs)）：foreground 直接调 `execute_plan_with_pipeline`，**不**经过 `execute_fragment`。

`src/engine/mod.rs:2615` 把字符串 `"127.0.0.1"` 硬编码传给 `ExecutionCoordinator::new` 作为 exchange host。整个 standalone 进程没有"跨进程"边界概念。

`src/service/internal_service.rs:1352` 已经有 `submit_exec_plan_fragment(thrift_bytes: &[u8])` 入口（FE-compat 模式的 thrift FFI 调用点），它内部解码 `TExecPlanFragmentParams` 然后调 `execute_fragment` —— 这是 D1 可以直接复用的入口。

`src/service/grpc_server.rs:65` 已经有 tonic `NovaRocksGrpc` service，目前暴露 `exchange` / `exchange_unary` / `transmit_runtime_filter` / `lookup` 四个 RPC，端口由 `[server].starlet_port`（默认 9070）配置。新增 RPC 应加到同一 service。

`src/runtime/result_buffer.rs` 当前同步阻塞 fetch（Condvar 唤醒），keyed on `fragment_instance_id`，BE 侧不动。

## 4. 核心设计决策

### 4.1 根 fragment 跑在 BE，FE 没有 executor

NovaRocks-FE 进程只跑 MySQL server + 优化器 + coordinator + 出站 gRPC client。**不**保留 executor 能力。BE 跑所有 fragment 含根。根 fragment 的 ResultSink 写 BE 本地 ResultBuffer。FE 通过 `FetchResult` RPC 拉 chunk。

理由：

- FE/BE 职责清晰，FE 是纯调度器；为将来 FE 多副本（无状态化）铺路。
- ResultBuffer 不需要做远程访问；BE 侧实现完全不动。
- 跨进程 RPC 三件套（Submit / Fetch / Cancel）刚好对应 StarRocks BE 的三个核心入口，wire 层完整对齐。

### 4.2 Wire protocol 与 StarRocks BE byte-identical

NovaRocks FE → NovaRocks BE 的 wire 在 thrift 类型层面与 StarRocks FE → StarRocks BE 完全一致；只是 transport 从 brpc 换成 gRPC。BE 侧 gRPC handler 极薄：

```rust
async fn submit_fragment(&self, req: Request<SubmitFragmentRequest>)
    -> Result<Response<SubmitFragmentResponse>, Status>
{
    let bytes = req.into_inner().exec_plan_fragment_params_thrift;
    match crate::submit_exec_plan_fragment(&bytes) {
        Ok(()) => Ok(Response::new(SubmitFragmentResponse { status_code: 0, message: "".into() })),
        Err(e) => Ok(Response::new(SubmitFragmentResponse { status_code: 1, message: e })),
    }
}
```

`crate::submit_exec_plan_fragment` 是现有 FFI 入口，零业务逻辑改动。

### 4.3 `FragmentDispatcher` trait 隔离 transport

Coordinator 只与 trait 打交道；`InProcessDispatcher` 用 `std::thread::spawn` + 直接 typed-args 调 `execute_fragment` 避免 thrift serde 开销；`RemoteDispatcher` 用 thrift 序列化 + tonic blocking client。两个实现的语义对外等价（包括 cancel 的 idempotency）。

### 4.4 Coordinator 内部清理：根 / 非根统一

把 `coordinator.rs` 现有"根 foreground" + "非根 thread::spawn"两条路径合并到一条：所有 fragment 通过 `dispatcher.submit_fragment` 提交，coordinator 通过 `dispatcher.fetch_result` 拉根 fragment 的结果。

新增辅助函数 `build_exec_plan_fragment_params` 把根 / CTE / Stream-source 三种 fragment 的 `TExecPlanFragmentParams` 凑装逻辑合并到一处，消除分支。

### 4.5 启动期 fail-fast 校验

`role = "fe"` 时 FE 在启动期主动 dial 每个 BE 的 starlet_port，gRPC handshake 失败立即报错。理由：分布式系统排错没什么比"启动就报错"更直接；运行时再连接失败的诊断成本高。

### 4.6 query_timeout 由 FE 强制

`TQueryOptions.query_timeout` 在 FE 侧的 fetch loop 用 wallclock 跟踪。FE 触发 timeout 后发 `CancelFragment` 给 BE。BE 自带的 fragment-level timeout（如果有）保留作为 last-resort safety net，D1 不修改。

## 5. 架构与角色分工

### 5.1 三个进程角色

| 角色 | 端口集 | 运行什么 | 不运行什么 |
|---|---|---|---|
| `all-in-one`（默认） | MySQL (9030) + gRPC (9070) | MySQL server + 优化器 + coordinator(InProcessDispatcher) + executor + ResultBuffer | — |
| `fe` | MySQL (9030) | MySQL server + 优化器 + coordinator(RemoteDispatcher) + 出站 gRPC client | gRPC server 端口、executor、本地 ResultBuffer |
| `be` | gRPC (9070) | gRPC server（SubmitFragment / FetchResult / CancelFragment + 现有 exchange / RF / lookup）+ executor + ResultBuffer | MySQL 端口 |

### 5.2 1 FE + 1 BE 数据流

```
MySQL client → FE (MySQL server)
                │ SQL 解析 + 优化 + PlanFragmentBuilder
                ▼
              FE coordinator (RemoteDispatcher)
                │ for each fragment in fragments:
                │     submit_fragment(target=唯一BE, params=TExecPlanFragmentParams)
                ▼
              gRPC SubmitFragment(thrift_bytes) ─→ BE
                                                   │ decode thrift → execute_fragment
                                                   │ 全部 fragment 在 BE 进程内（含根）
                                                   │ fragment 之间走 in-process exchange
                                                   │ 根 fragment ResultSink → 本地 ResultBuffer
              FE coordinator
                │ loop:
                │   FetchResult(root_finst_id, max_wait_ms=300) → chunk / EOF / err
                ▼
              MySQL handler → encode → MySQL client
```

D1 拓扑下**没有跨进程 exchange**：所有 fragment 在同一 BE 进程，exchange 仍走 in-process registry。跨进程 exchange 是 D2 才打开的。

### 5.3 关键改造点

1. `coordinator.rs:488-565` 根 fragment foreground 路径删除，统一走 dispatcher。
2. `engine/mod.rs:2615` 硬编码 `"127.0.0.1"` 删除；`ExecutionCoordinator::new` 不再需要 exchange_host/port 参数。
3. 新增 `src/runtime/dispatcher.rs`：trait + 两个实现。
4. 新增 BE 侧 gRPC handler（三个方法，极薄）加到 `src/service/grpc_server.rs`。
5. 新增 `[cluster]` 配置节解析（`src/common/app_config.rs`）。
6. 新增 `--role` CLI 选项与进程启动分派（`src/main.rs`）。

## 6. 配置 Schema 与 CLI

### 6.1 TOML `[cluster]` 节

```toml
[cluster]
# 必填，三选一。默认 all-in-one。
role = "all-in-one"

# 仅 role = "fe" 时使用。v1 必须恰好一个条目。
backends = ["127.0.0.1:9070"]

# 仅 role = "be" 时使用。BE 对外宣告的地址。
# 留空时从 [server].host + [server].starlet_port 推导。
# v1 不直接使用（D2 才填进 destinations），但字段先就位。
advertise_host = ""
advertise_port = 0
```

### 6.2 CLI

```bash
# 默认（行为不变）
novarocks standalone-server --config /etc/novarocks/novarocks.toml

# D1 新增 --role 覆盖
novarocks standalone-server --role fe --config /etc/novarocks/fe.toml
novarocks standalone-server --role be --config /etc/novarocks/be.toml
novarocks standalone-server --role all-in-one --config /etc/novarocks/novarocks.toml
```

`--role` 优先级高于 TOML 中的 `[cluster].role`。`--port` 行为不变。

### 6.3 启动期校验（fail-fast）

| 校验 | 失败行为 |
|---|---|
| `role` 字段值合法 | startup error，列出三个合法值 |
| `role = "fe"` 且 `backends.len() != 1`（v1） | startup error："D1 v1 only supports exactly one backend, got N" |
| `role = "fe"` 时主动 dial 每个 BE 的 starlet_port + gRPC handshake | startup error，包含目标 BE 地址 |
| `role = "be"` 时确认 starlet_port 真的被绑定 | startup error |
| `role = "be"` 同时存在 `--port`（MySQL 端口） | warn 并忽略 |

## 7. gRPC 服务契约

新增到现有 `NovaRocksGrpc` service（同一 tonic service、同一 starlet_port）。

### 7.1 Proto 草案

`src/service/proto/novarocks_backend.proto`（与现有 exchange proto 同包）：

```proto
service NovaRocksBackend {
  // 已有
  rpc Exchange(stream PTransmitChunkParams) returns (stream PTransmitChunkResult);
  rpc ExchangeUnary(PTransmitChunkParams) returns (PTransmitChunkResult);
  rpc TransmitRuntimeFilter(PTransmitRuntimeFilterParams) returns (PTransmitRuntimeFilterResult);
  rpc Lookup(PLookupRequest) returns (PLookupResponse);

  // D1 新增
  rpc SubmitFragment(SubmitFragmentRequest) returns (SubmitFragmentResponse);
  rpc FetchResult(FetchResultRequest) returns (FetchResultResponse);
  rpc CancelFragment(CancelFragmentRequest) returns (CancelFragmentResponse);
}

message SubmitFragmentRequest {
  // thrift::serialize(TExecPlanFragmentParams)，
  // 与现有 FFI 入口 submit_exec_plan_fragment(thrift_bytes) byte-identical。
  bytes exec_plan_fragment_params_thrift = 1;
}

message SubmitFragmentResponse {
  // 0 = 业务成功；非 0 = 业务失败（thrift decode 错 / pipeline 启动错等）。
  // 注意：业务成功只代表 fragment 已被 BE 接受并开始执行，不代表执行成功。
  int32 status_code = 1;
  string message = 2;
}

message FetchResultRequest {
  TUniqueId finst_id = 1;
  // long-poll 上限；0 表示立即返回。
  int64 max_wait_ms = 2;
}

message FetchResultResponse {
  enum Status {
    READY = 0;     // chunk_arrow_ipc 有效
    NOT_READY = 1; // 未等到 chunk，FE 可继续 poll
    EOF = 2;       // fragment 已完成且队列耗尽
    ERROR = 3;     // fragment 执行出错，message 含原因
  }
  Status status = 1;
  bytes chunk_arrow_ipc = 2; // 仅 READY；Arrow IPC 编码（与现有 exchange wire 一致）
  string message = 3;        // 仅 ERROR
}

message CancelFragmentRequest {
  repeated TUniqueId finst_ids = 1;
  string reason = 2;
}

message CancelFragmentResponse {
  int32 status_code = 1; // 总是 0；cancel 是 best-effort
}

message TUniqueId {
  int64 hi = 1;
  int64 lo = 2;
}
```

### 7.2 错误语义约定

| 场景 | gRPC `Status` | proto `status_code` |
|---|---|---|
| RPC 成功投递且业务成功 | `Code::Ok` | 0 |
| RPC 成功投递但业务失败 | `Code::Ok` | 非 0 + message |
| 网络错误 / BE 进程崩溃 / RPC 超时 | `Code::Unavailable` / `Code::Cancelled` | —— |
| BE 处理过载（连接队列满） | `Code::ResourceExhausted` | —— |
| 客户端主动 cancel | `Code::Cancelled` | —— |

FE 收到非 OK 状态：把对应 query 标记失败 → cancel 该 query 所有已 submit fragments → 返回错误给 MySQL。

### 7.3 BE 侧 handler

`src/service/grpc_server.rs` 中 `NovaRocksGrpc` impl 新增三个方法。每个方法都是极薄的包装：

- `submit_fragment` 透传字节到 `crate::submit_exec_plan_fragment`
- `fetch_result` 调用新增的 `result_buffer::try_fetch_for_rpc`（薄包装现有 `try_fetch`，把三态映射到 proto enum 加上 EOF）
- `cancel_fragment` 遍历 finst_ids 调 `exchange::cancel_fragment` + `result_buffer::cancel`

### 7.4 FE 侧 client

`RemoteDispatcher` 持有一个 tonic channel 指向唯一 BE。HTTP/2 keepalive 5s（与现有 exchange channel 一致）。所有 RPC 用 blocking client 调用（保持 coordinator 同步代码路径）。

## 8. `FragmentDispatcher` Trait 与 Coordinator 改造

### 8.1 Trait 定义

`src/runtime/dispatcher.rs`（新文件）：

```rust
pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Submit a single fragment instance. Returns once accepted, not after completion.
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String>;

    /// Pull a chunk for the given root fragment instance.
    /// Blocks up to max_wait_ms (0 = non-blocking).
    fn fetch_result(&self, finst_id: TUniqueId, max_wait_ms: i64)
        -> Result<FetchOutcome, String>;

    /// Cancel a set of fragment instances. Best-effort, idempotent.
    fn cancel_fragments(&self, finst_ids: &[TUniqueId]);
}

pub enum FetchOutcome {
    Ready(arrow::RecordBatch),
    NotReady,
    Eof,
    Err(String),
}
```

### 8.2 `InProcessDispatcher`

```rust
pub struct InProcessDispatcher;

impl FragmentDispatcher for InProcessDispatcher {
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
        // 拆 typed args 避免 in-process serde 开销
        let TExecPlanFragmentParams {
            protocol_version: _,
            fragment, desc_tbl, params: exec_params, query_options, ..
        } = params;
        // pipeline_dop 与 coordinator.rs:447-449 现有逻辑一致：
        //   std::thread::available_parallelism().map(|p| p.get().min(4)).unwrap_or(4) as i32
        let pipeline_dop = compute_pipeline_dop();
        std::thread::spawn(move || {
            execute_fragment(&fragment, desc_tbl.as_ref(), Some(&exec_params),
                             query_options.as_ref(),
                             /* session_time_zone */ None,
                             pipeline_dop,
                             /* group_execution_scan_dop */ None,
                             /* db_name */ None,
                             /* profiler */ None,
                             /* last_query_id */ None,
                             /* fe_addr */ None,
                             /* backend_num */ None,
                             /* mem_tracker */ None)
        });
        Ok(())
    }

    fn fetch_result(&self, finst_id, max_wait_ms) -> Result<FetchOutcome, String> {
        let r = result_buffer::try_fetch(finst_id, max_wait_ms);
        Ok(map_try_fetch_to_outcome(r))
    }

    fn cancel_fragments(&self, finst_ids: &[TUniqueId]) {
        for id in finst_ids {
            exchange::cancel_fragment(id.hi, id.lo);
            result_buffer::cancel(*id);
        }
    }
}
```

### 8.3 `RemoteDispatcher`

```rust
pub struct RemoteDispatcher {
    backend: SocketAddr,
    client: NovaRocksBackendGrpcClient,
}

impl FragmentDispatcher for RemoteDispatcher {
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
        let bytes = thrift::serialize(&params)?;
        self.client.blocking_submit_fragment(SubmitFragmentRequest {
            exec_plan_fragment_params_thrift: bytes,
        }).map_err(|e| format!("submit_fragment to {}: {}", self.backend, e))?;
        Ok(())
    }

    fn fetch_result(&self, finst_id, max_wait_ms) -> Result<FetchOutcome, String> {
        let resp = self.client.blocking_fetch_result(FetchResultRequest { finst_id, max_wait_ms })
            .map_err(|e| format!("fetch_result: {}", e))?;
        Ok(map_proto_to_outcome(resp))
    }

    fn cancel_fragments(&self, finst_ids: &[TUniqueId]) {
        let _ = self.client.blocking_cancel_fragment(CancelFragmentRequest {
            finst_ids: finst_ids.to_vec(),
            reason: "fe-initiated".into(),
        });
    }
}
```

### 8.4 Coordinator 签名与 execute 改造

```rust
// 现状
pub(crate) fn new(
    build_result: MultiFragmentBuildResult,
    exchange_host: String,
    exchange_port: u16,
    query_options: Option<TQueryOptions>,
) -> Self

// D1 之后
pub(crate) fn new(
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn FragmentDispatcher>,
    query_options: Option<TQueryOptions>,
) -> Self
```

`engine/mod.rs:2615` 处构造 dispatcher：

```rust
let dispatcher: Arc<dyn FragmentDispatcher> = match cluster_role {
    ClusterRole::AllInOne => Arc::new(InProcessDispatcher),
    ClusterRole::Fe => Arc::new(RemoteDispatcher::new(backends[0])?),
    ClusterRole::Be => unreachable!("BE role should not enter the coordinator path"),
};
```

新增 `build_exec_plan_fragment_params`，落在新模块 `src/runtime/exec_params.rs`（与 dispatcher 同层）：合并现有 根 / CTE / Stream-source 三种 fragment 的 `TExecPlanFragmentParams` 凑装逻辑到一处，消除分支。这是 D1 内部 cleanup 的核心。从 `coordinator.rs` 独立出来的理由是这段逻辑既要被 coordinator 调用，也可能被未来 D2 的 fragment-to-BE 调度器扩展。

`execute` 改造：所有 fragment 通过 dispatcher 提交，根 fragment 不再 foreground 执行；详见第 9 节。

## 9. 错误处理、Cancel、Timeout

### 9.1 Coordinator 错误处理骨架

```rust
fn execute(self) -> Result<QueryResult, String> {
    let deadline = compute_deadline(&self.query_options);
    let mut submitted_ids: Vec<TUniqueId> = Vec::new();
    let mut root_finst_id = None;

    // 阶段 1：submit 全部 fragment
    for fr in fragments {
        let params = build_exec_plan_fragment_params(&fr);
        let finst_id = params.params.fragment_instance_id;
        if fr.fragment_id == root_fragment_id {
            root_finst_id = Some(finst_id);
        }
        if let Err(e) = self.dispatcher.submit_fragment(params) {
            self.dispatcher.cancel_fragments(&submitted_ids);
            return Err(format!("submit fragment failed: {}", e));
        }
        submitted_ids.push(finst_id);
    }
    let root = root_finst_id.unwrap();

    // 阶段 2：循环 fetch 根 fragment 结果
    let mut chunks = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.dispatcher.cancel_fragments(&submitted_ids);
            return Err("query timeout".into());
        }
        let wait_ms = std::cmp::min(300, remaining.as_millis() as i64);
        match self.dispatcher.fetch_result(root, wait_ms) {
            Ok(FetchOutcome::Ready(c)) => chunks.push(c),
            Ok(FetchOutcome::NotReady) => continue,
            Ok(FetchOutcome::Eof) => break,
            Ok(FetchOutcome::Err(msg)) => {
                self.dispatcher.cancel_fragments(&submitted_ids);
                return Err(msg);
            }
            Err(rpc_err) => {
                self.dispatcher.cancel_fragments(&submitted_ids);
                return Err(format!("fetch_result rpc failed: {}", rpc_err));
            }
        }
    }
    Ok(QueryResult::from_chunks(chunks))
}
```

### 9.2 错误源分类

| 类型 | 触发条件 | FE 行为 |
|---|---|---|
| Submit RPC 失败 | tonic 错（`Unavailable` / `Cancelled` / `ResourceExhausted`） | cancel 已 submit；返回错给 MySQL |
| Submit business 失败 | `SubmitFragmentResponse.status_code != 0` | 同上 |
| `FetchResult` 返回 `ERROR` | BE 端 fragment 失败 | 同上 |
| `FetchResult` RPC 失败 | 网络断 / BE 崩溃 | 同上，额外日志 |
| MySQL client 断连 | FE handler 感知 | 主动 `CancelFragment`；FE cleanup |
| Query timeout | wallclock 超过 deadline | 同上，MySQL 返回 timeout |

### 9.3 Cancel 语义保证

- `CancelFragment` RPC 是 idempotent + best-effort：BE 对已完成 fragment 返回 OK 不报错。
- FE 一次 `cancel_fragments(ids)` 可以传整个 query 的所有 fragment instances；BE 内部并行调 `exchange::cancel_fragment` + `result_buffer::cancel`。
- 复用 [`exchange::cancel_fragment`](../../src/runtime/exchange.rs) 与 [`result_buffer::cancel`](../../src/runtime/result_buffer.rs)，不引入新机制。
- FE 不等 `CancelFragment` 响应——发出去就返回错误给 MySQL，避免 BE 慢响应阻塞错误传播。

### 9.4 Submit 半失败清理

5 个 fragment，第 3 个 submit RPC 失败时：

- 第 1、2 个已在 BE 上启动，占资源、可能阻塞在 exchange 等 senders；
- FE 必须 `CancelFragment(ids=[1,2])`，**不**能只取消第 3 个之后；
- 第 3 个本身没 submit 成功，无需 cancel；
- 实现：`submitted_ids: Vec` 仅追加成功 submit 的 ID，失败时 `cancel_fragments(&submitted_ids)`。

### 9.5 Timeout 强制点

- `query_timeout` 来自 `TQueryOptions`（FE 设置）。
- FE wallclock 跟踪：fetch loop 每次循环检查 deadline。
- BE 自带的 fragment-level timeout 保留作为 last-resort safety net，D1 不修改。

### 9.6 MySQL 断连感知

- FE MySQL handler 在每次 fetch loop 间隔检查 session 是否还活着。
- 断连后跳出 fetch loop → 走 cancel 路径。
- 实现：`Arc<AtomicBool>` 在 MySQL handler 和 coordinator 间共享。若现有 server 没有这个机制，作为 D1 sub-task 落实。

### 9.7 all-in-one 模式

InProcessDispatcher 的 cancel 路径直接调本地 `exchange::cancel_fragment` + `result_buffer::cancel`，与 BE-only 模式 BE-side cancel 路径完全相同。Coordinator 错误处理代码路径 100% 共享，只是 dispatcher 实现不同。

## 10. 测试计划

### 10.1 单元测试

| 模块 | 测试内容 | 位置 |
|---|---|---|
| `[cluster]` 配置解析 | role 三选一 / backends 数量 / fail-fast | `src/common/app_config.rs` 测试模块 |
| `--role` CLI 解析 | flag 覆盖 TOML / 组合 `--port` / 未知 role 拒绝 | `src/main.rs` 测试模块 |
| `build_exec_plan_fragment_params` | 根 / CTE / Stream-source 三种 fragment 关键字段正确 | `src/runtime/coordinator.rs` 测试模块 |
| `InProcessDispatcher` | submit / fetch / cancel 与改造前等价 | `src/runtime/dispatcher.rs` 新测试 |
| `RemoteDispatcher` | tonic mock server 验证 RPC 编解码 / 错误码 / cancel | `src/runtime/dispatcher.rs` 新测试 |
| BE 侧 gRPC handler | 透传到现有 FFI 入口；三态映射 | `src/service/grpc_server.rs` 测试模块 |

### 10.2 本地 1FE+1BE 集成测试

新增 `tests/cluster_mvp/` 测试 crate，包含：

- `spawn_be_process` / `spawn_fe_process` / `wait_for_ready` 帮助函数
- 至少一个 `SELECT 1` 跨进程跑通的 smoke
- 至少一个 SSB Q1 跨进程 vs all-in-one byte-identical 比对

readiness 标记沿用 `NOVAROCKS_READY mysql_port=... pid=...`（[CLAUDE.md 现有约定](../../CLAUDE.md)）。BE 角色启动后也发 `NOVAROCKS_READY role=be starlet_port=... pid=...`。

### 10.3 SQL 套件回归（D1 主验收）

`tests/sql-test-runner` 增加 `--cluster-mode` 选项：

```bash
cargo run --bin sql-tests -- --suite ssb --mode verify
cargo run --bin sql-tests -- --suite ssb --mode verify --cluster-mode cross-process
```

cross-process 模式内部拉起 `--role be` 子进程 + `--role fe` 子进程，把 SQL 发到 FE MySQL 端口，结果与 all-in-one baseline byte-identical 比对。

**必须通过的套件**：

| 套件 | 模式 | 标准 |
|---|---|---|
| SSB 13 个查询 | cross-process | 与 all-in-one byte-identical |
| TPC-H Q1 / Q5 / Q9 / Q12 | cross-process | 同上 |
| 现有所有 sql-test suite | all-in-one（回归） | 全部通过，不允许新 fail |
| `iceberg-rest` smoke | cross-process | 通过（验证 Iceberg scan 在跨进程下工作） |

### 10.4 错误场景测试（`tests/cluster_mvp/`）

| 场景 | 期望行为 |
|---|---|
| FE 配置错（backends 空 / 写错地址） | FE 启动失败，错误含 "backends" / 目标地址 |
| BE 启动晚于 FE | FE 启动期 dial 失败，错误含目标地址 |
| Query 执行中 BE 进程 `kill -9` | FE 拿到 RPC error → 干净失败；FE 进程不挂 |
| Query 执行中 MySQL client 断连 | FE 检测到断连 → `CancelFragment` → BE log 显示 cancel |
| `query_timeout` 超时 | FE 超过 deadline → cancel + timeout error |
| Submit 第二个 fragment 失败（fault injection） | 已 submit 的第一个被 cancel；错误信息含两条信息 |

### 10.5 性能 sanity check（不作验收门槛）

同机跑 SSB Q1，all-in-one vs cross-process 延迟比对，期望差距 < 20%。差距 > 50% 时把"FetchResult unary 批次大小 / poll 频率调优"列入独立优化项。

### 10.6 非目标

- 多 BE 测试（D2）
- 心跳 / 动态拉起停掉 BE（D3）
- `ADD/DROP BACKEND` SQL（D4）
- CI 集成（D5）
- 性能 benchmark 作为门槛

## 11. 验收标准

- 1 FE + 1 BE 同机跨进程跑通 SSB 全套，输出与 all-in-one byte-identical。
- TPC-H Q1 / Q5 / Q9 / Q12 在 cross-process 模式下输出与 all-in-one byte-identical。
- 现有所有 sql-test suite 在 all-in-one 模式下不回归。
- `iceberg-rest` smoke 在 cross-process 模式下通过。
- `--role fe` 不监听 starlet_port；`--role be` 不监听 MySQL 端口。
- BE 进程 `kill -9` / SubmitFragment RPC 失败 / FetchResult RPC 失败 时 FE 上的 query 干净失败、错误信息明确，FE 进程不挂。
- query_timeout 触发后 FE 真的发了 `CancelFragment`，BE log 显示 cancel 被处理。
- coordinator.rs 中 488-565 行根 fragment foreground 路径删除，所有 fragment 走统一 dispatcher 路径。
- `engine/mod.rs:2615` 硬编码 `"127.0.0.1"` 删除。

## 12. 风险与备选

### 12.1 风险点

- **all-in-one 性能回归**：拆 dispatcher 这一层间接可能在极短查询上引入 measurable overhead。第 10.5 节 sanity check 必须跑；若回归 > 5%，把 InProcessDispatcher 改成内联（compile-time 分派）的优化项。
- **根 fragment foreground 删除后的回归**：现有 488-565 行那条路径有一些 query_options / desc_tbl 处理细节，重写时容易丢字段。单元测试 `build_exec_plan_fragment_params` 必须覆盖所有现有字段。
- **submit 半失败的 race**：FE 调 `CancelFragment` 时第 1 个 fragment 可能刚好自己结束、第 2 个还在跑；cancel 必须 idempotent（在 spec 第 9.3 节已保证）。
- **MySQL session 断连感知**：现有 server 是否已经能让外部代码 (coordinator) 感知断连？如果没有，D1 sub-task 范围会扩大。需要在 PR 1（CLI/config）之后立刻确认。
- **PlanFragmentParams 字段语义跨进程一致性**：`destinations` / `fe_addr` / `backend_num` 在 in-process 时是默认值，跨进程时 FE 必须显式填。`build_exec_plan_fragment_params` 必须做对。

### 12.2 备选与已拒绝方案

- **根 fragment 跑在 FE**（备选）：FE 保留 executor，BE leaf 反向发到 FE 的 ExchangeReceiver。已**拒绝**——理由是 FE/BE 职责模糊、ResultBuffer 跨进程不需要、未来 FE 多副本路径变难。
- **全 fragment 跑在 BE 但根 ResultSink 换成 ExchangeSender，FE 起一个轻量"接收 fragment"**（备选）：完整对称但多一个 fragment、调优化器/splitter 写一个特殊小节点。已**拒绝**——D1 时机不对，复杂度过高。
- **payload 用 proto 字段化（一步到位）**：已**拒绝**——TPlanFragment 嵌套几十种 thrift 类型，一步到位代价大，与 StarRocks 协议双轨维护增加心智负担。

## 13. 代码引用索引

- 当前 in-process coordinator：[`src/runtime/coordinator.rs`](../../src/runtime/coordinator.rs)
- 现有 FFI 入口（D1 复用）：[`src/service/internal_service.rs::submit_exec_plan_fragment`](../../src/service/internal_service.rs)
- FFI thin C wrapper（参考）：[`src/service/engine_ffi.rs`](../../src/service/engine_ffi.rs)
- 现有 tonic gRPC server：[`src/service/grpc_server.rs`](../../src/service/grpc_server.rs)
- 现有 exchange registry：[`src/runtime/exchange.rs`](../../src/runtime/exchange.rs)
- 现有 ResultBuffer：[`src/runtime/result_buffer.rs`](../../src/runtime/result_buffer.rs)
- Fragment 执行入口：[`src/lower/fragment.rs::execute_fragment`](../../src/lower/fragment.rs)
- 现有 thrift 类型 `TExecPlanFragmentParams`：`idl/thrift/InternalService.thrift`
- 配置入口：[`src/common/app_config.rs`](../../src/common/app_config.rs)
- 网络解析：[`src/common/network.rs`](../../src/common/network.rs)
- 进程入口：[`src/main.rs`](../../src/main.rs)
- standalone server entry：[`src/server/mod.rs`](../../src/server/mod.rs)
- 测试基础设施：`tests/sql-test-runner/`、`docker/iceberg-rest/`
