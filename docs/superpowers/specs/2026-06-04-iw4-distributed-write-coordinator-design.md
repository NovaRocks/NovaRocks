# IW-4：Distributed Write Coordinator 设计

日期：2026-06-04
状态：已确认，待用户评审
对应 roadmap 任务：`IW-4 Distributed Write Coordinator`

依赖：
- IW-3 / SP2+SP3（PR #242）已将 Iceberg writer 写入路径收敛到 `sink_io`，并继续通过 `runtime::sink_commit` 暂存 staged file metadata。
- D1/D2 standalone distributed execution 已提供 `NovaRocksGrpc`、`FragmentDispatcher`、multi-BE scheduling、remote submit/fetch/cancel 基础。

---

## 1. 概述

IW-4 为 distributed write 增加 coordinator-owned 的写入协调能力：coordinator 必须知道一个写查询期望哪些 writer 完成，收集所有 writer 的最终执行状态和 staged file metadata，并在任一 writer 失败时 fail query、fan-out cancel 其它 fragment。

核心协议原则是与 FE-compatible 保持同一语义：writer 完成信息不设计成 standalone 专用的 writer-report 协议，而是复用 StarRocks/NovaRocks 已有的 `TReportExecStatusParams` 语义。FE-compatible 模式继续把同一类 report 通过 FE report worker 发给 StarRocks FE；NovaRocks standalone distributed 模式通过 `NovaRocksGrpc` 把同一 payload 发给 NovaRocks coordinator。两条路径的区别只在 transport，不在 report 字段解释。

IW-4 v1 只生成统一的 `WriteCommitInput` / `WriteAbortInput`，不执行具体 Iceberg/Hive/managed-lake metadata commit 或 cleanup 策略。后续 IW-5/IW-7/IW-8 消费这些输入。

## 2. 目标与非目标

### 2.1 目标

1. 新增 coordinator-side write state machine，跟踪 expected writers 和 writer lifecycle。
2. 复用 `TReportExecStatusParams` 作为统一 fragment exec status report payload。
3. 在 `NovaRocksGrpc` 增加 standalone coordinator 接收 report 的 RPC。
4. 抽出可复用 report builder，让 FE-compatible adapter 和 standalone gRPC adapter 共用同一份 `sink_commit` 汇总逻辑。
5. 从 final OK report 中收集 `sink_commit_infos`、tablet commit/fail infos、load counters、loaded rows/bytes 等现有字段。
6. 任一 final error report 触发 write failed，并通过现有 `dispatcher.cancel_fragments` 取消其它 fragment。
7. duplicate final report 幂等；内容冲突 fail fast。
8. 1FE+2BE standalone distributed write 能收集两个 BE 的 staged file metadata。
9. coordinator 日志能观察 writer Pending/Running/Finished/Failed/Canceled lifecycle。

### 2.2 非目标

- 不实现具体 Iceberg metadata commit 策略。
- 不实现 table-format-specific staging cleanup executor。
- 不实现 BE crash reschedule。
- 不实现 exactly-once retry。
- 不改变 StarRocks FE 的 `reportExecStatus` thrift schema。
- 不把 standalone write report 设计成独立于 FE-compatible 的新语义。

## 3. 现状

### 3.1 FE-compatible report 路径

当前 NovaRocks 已经有 FE-compatible report 路径：

- `src/service/fe_report.rs` 维护 fragment report registry。
- `fe_report::register_instance` 在 fragment 执行前注册 `query_id`、`fragment_instance_id`、`backend_num`、coordinator address、profile 配置。
- fragment 运行中可通过 `fe_report::report_exec_state` 发送 `done=false` 的周期 report。
- fragment 结束时 `fe_report::report_fragment_done` 构造 final `TReportExecStatusParams`，通过 `exec_state_reporter` 发给 FE。
- `build_report_params` 会从 `runtime::sink_commit` 汇总 `TSinkCommitInfo`、tablet commit/fail infos、load counters、loaded rows/bytes。

`TReportExecStatusParams` 本身已经包含 IW-4 需要的关键字段：

- `query_id`
- `backend_num`
- `fragment_instance_id`
- `status`
- `done`
- `commitInfos`
- `failInfos`
- `sink_commit_infos`
- `load_counters`
- loaded rows/bytes/filter rows 等 load status 字段

### 3.2 Standalone distributed 执行路径

Standalone distributed execution 已有：

- `NovaRocksGrpc` 上的 `SubmitFragment` / `FetchResult` / `CancelFragment`。
- `FragmentDispatcher` 抽象 in-process 和 remote BE。
- `RemoteDispatcher` 按 backend_idx 调用 remote BE。
- `ExecutionCoordinator` 负责 submit fragments、fetch root result、timeout/cancel。

缺口是：standalone coordinator 当前只 fetch root result，不接收非 root writer 的 final exec status；`runtime::sink_commit` 仍是 fragment-local side channel，不能作为 distributed write 的最终裁决依据。

## 4. 核心设计决策

### 4.1 统一 report payload，transport adapter 分流

IW-4 不新增 `ReportWriterStatus` 之类 standalone-only RPC。统一事件来源是 `TReportExecStatusParams`。

FE-compatible：

```text
BE fragment finish
  -> build TReportExecStatusParams
  -> FE report worker
  -> StarRocks FE reportExecStatus / batchReportExecStatus
```

Standalone distributed：

```text
BE fragment finish
  -> build TReportExecStatusParams
  -> NovaRocksGrpc ReportExecStatus / BatchReportExecStatus
  -> NovaRocks coordinator WriteCoordinator
```

这样 `sink_commit_infos`、load counters、tablet commit/fail infos 的构造和解释只有一份。

### 4.2 gRPC 继续使用 thrift-binary payload

`NovaRocksGrpc` 现有 `SubmitFragment` / `FetchResult` 已经采用 gRPC transport + thrift-binary payload。IW-4 report RPC 也沿用这个模式：

```proto
rpc ReportExecStatus(ReportExecStatusRequest) returns (ReportExecStatusResponse);
rpc BatchReportExecStatus(BatchReportExecStatusRequest) returns (BatchReportExecStatusResponse);

message ReportExecStatusRequest {
  bytes report_exec_status_params_thrift = 1;
}

message ReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
}

message BatchReportExecStatusRequest {
  repeated bytes report_exec_status_params_thrift = 1;
}

message BatchReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
}
```

不在 protobuf 中镜像 `TReportExecStatusParams` 字段，避免两套 schema 漂移。

### 4.3 WriteCoordinator 只消费 transport-neutral event

gRPC server 负责把 thrift bytes 反序列化为 `TReportExecStatusParams`，然后转换为内部事件：

```text
FragmentExecStatusReport {
  query_id,
  backend_num,
  fragment_instance_id,
  status,
  done,
  sink_commit_infos,
  tablet_commit_infos,
  tablet_fail_infos,
  load_counters,
  loaded_rows,
  loaded_bytes,
  filtered_rows,
}
```

`WriteCoordinator` 不知道 report 来自 FE-compatible transport 还是 NovaRocksGrpc transport。它只按事件更新 writer state。

### 4.4 v1 幂等键不承诺 retry 语义

IW-4 roadmap 提到 `AttemptId`。v1 内部模型和日志可以预留 attempt 概念，但 wire contract 不引入新的 attempt 字段，因为当前没有 BE crash retry/reschedule。v1 幂等键使用：

```text
(query_id, fragment_instance_id, backend_num)
```

后续真正引入 retry/reschedule 时，再把 attempt identity 纳入提交参数和 report payload。

## 5. 数据模型

### 5.1 Write identity

`WriteId` 在 v1 中可直接使用 `query_id`。如果一个 query 未来包含多个独立 write sink，再扩展为 `(query_id, sink_id)`。IW-4 v1 不提前设计多 sink wire 字段。

### 5.2 Writer identity

```text
WriterKey {
  query_id: TUniqueId,
  fragment_instance_id: TUniqueId,
  backend_num: i32,
}
```

`writer_id` 是 coordinator 内部派生出的稳定序号，用于日志、状态表和 `WriteCommitInput` 排序。它不作为 v1 wire identity。

### 5.3 Writer state

```text
Pending
Running {
  last_report_time,
  load_counters,
}
Finished {
  sink_commit_infos,
  tablet_commit_infos,
  tablet_fail_infos,
  load_counters,
  loaded_rows,
  loaded_bytes,
  filtered_rows,
}
Failed {
  error,
}
Canceled {
  reason,
}
```

状态解释：

- `Pending`：coordinator 已注册 expected writer，但尚未收到 report。
- `Running`：收到 `done=false` report，可更新 profile/load counters，但不能视为完成。
- `Finished`：收到 `done=true` 且 `status=OK`，冻结 commit metadata。
- `Failed`：收到非 OK final report。
- `Canceled`：coordinator 因其它 writer 失败或 query cancel 主动取消该 writer。

### 5.4 Commit/abort 输出

`WriteCommitInput`：

```text
WriteCommitInput {
  write_id,
  writers: [
    {
      writer_id,
      writer_key,
      sink_commit_infos,
      tablet_commit_infos,
      load_counters,
      loaded_rows,
      loaded_bytes,
      filtered_rows,
    }
  ]
}
```

`WriteAbortInput`：

```text
WriteAbortInput {
  write_id,
  reason,
  completed_writer_outputs,
  incomplete_writers,
}
```

IW-4 只产出这些结构，不执行 table-format-specific commit/abort。

## 6. Data Flow

### 6.1 Query start

1. `ExecutionCoordinator` 完成 scheduling 后识别 write query。
2. Coordinator 根据 fragment placements 注册 expected writers。
3. 对每个 writer 初始化 `Pending` 状态，并记录 backend_idx、fragment_instance_id、query_id。
4. Fragment submit 参数继续使用 `TExecPlanFragmentParams` 作为主体；report destination 由已有 coord address 或 standalone gRPC coordinator address 决定。

### 6.2 BE execution

1. BE 执行 fragment。
2. Sink 写 staged files。
3. Sink 继续通过 `RuntimeState::add_sink_commit_info` / `add_sink_load_stats` 写入 `runtime::sink_commit`。
4. fragment finish 时统一 report builder 从 `sink_commit` 读取本 fragment 的 file metadata 和 load stats。

### 6.3 Report dispatch

统一 builder 产出 `TReportExecStatusParams` 后进入 adapter：

- FE-compatible adapter：交给 `exec_state_reporter`，走 FE thrift RPC。
- Standalone adapter：序列化成 thrift binary，调用 coordinator 的 `NovaRocksGrpc::ReportExecStatus`。

final report 保持 priority/retry 语义，不能被静默丢弃。

### 6.4 Coordinator report handling

1. gRPC handler 反序列化 report。
2. handler 查找 query 对应的 `WriteCoordinator`。
3. `WriteCoordinator` 验证 writer key 是否在 expected set。
4. 按 `done` 和 `status` 更新 writer state。
5. 所有 expected writers 都 `Finished` 后生成 `WriteCommitInput`。
6. 任一 writer `Failed` 后生成 `WriteAbortInput`，并触发 cancel fan-out。

## 7. Error Handling

### 7.1 Writer failure

收到 `done=true` 且 `status != OK`：

1. 对应 writer 进入 `Failed`。
2. write state 进入 failed，记录第一个错误。
3. `ExecutionCoordinator` 通过现有 `dispatcher.cancel_fragments` 取消其它未完成 fragments。
4. 后续 OK report 只记录为 late report，不改变最终 failed 结果。

### 7.2 Duplicate report

同一个 writer key 重复收到 final report：

- 内容相同：幂等忽略。
- OK/ERROR 冲突：fail fast。
- OK 但 `sink_commit_infos` 或关键 load stats 不一致：fail fast。

这能暴露 fragment 重入、重复 close、report 重试 payload 不稳定等协议错误。

### 7.3 Missing writer

query 完成前如果 expected writer 仍停留在 `Pending` / `Running` / `Canceled` 且没有 final OK report，coordinator 不能生成 `WriteCommitInput`。最终错误必须说明 missing writer key。

### 7.4 Unknown writer

收到未注册 writer key 的 report：

- 如果 query 已完成并清理，可返回明确错误或 query-gone 状态。
- 如果 query 仍存在，视为协议错误并 fail query，避免收集到非本 query 的 staged files。

### 7.5 Report RPC failure

Standalone final report RPC 使用与 FE-compatible final report 类似的 priority/retry 策略。超过 retry 后不能静默成功；本地 fragment 应将 report failure 作为 query failure 暴露。

## 8. Code Organization

### 8.1 Report builder 抽取

把现有 `fe_report::build_report_params` 中与 FE transport 无关的部分抽到共享模块，例如：

```text
src/service/exec_status_report.rs
```

该模块负责：

- 从 report instance snapshot 和 `finst_id` 构造 `TReportExecStatusParams`。
- 从 `runtime::sink_commit` 读取 `sink_commit_infos`、tablet commit/fail infos、load counters。
- 保持 FE-compatible 现有字段填充行为不变。

`fe_report.rs` 只保留 registry 和 FE adapter。Standalone gRPC adapter 复用同一 builder。

### 8.2 Standalone report adapter

新增 standalone report sender，挂在服务层或 runtime 层都可以，但职责必须单一：

- 输入 `TReportExecStatusParams`。
- thrift-binary serialize。
- 调 `NovaRocksGrpcRemoteClient::blocking_report_exec_status` 或 async 等价方法。
- final report 使用 priority/retry 语义。

### 8.3 Coordinator state

新增 runtime 模块，例如：

```text
src/runtime/write_coordinator.rs
```

职责：

- 注册 expected writers。
- 接收 `FragmentExecStatusReport`。
- 维护 writer state。
- 生成 `WriteCommitInput` / `WriteAbortInput`。
- 暴露 query failed/cancel signal 给 `ExecutionCoordinator`。

`ExecutionCoordinator` 仍负责 fragment submit/fetch/cancel 编排，`WriteCoordinator` 不直接发 RPC。

### 8.4 gRPC server/client

修改：

- `idl/proto/starust_grpc.proto`
- `src/service/grpc_server.rs`
- `src/service/grpc_client.rs`

gRPC handler 只做薄包装：decode thrift payload、调用 coordinator report registry、返回 status_code/message。不要在 handler 内执行 commit 或 cleanup。

## 9. Testing

### 9.1 Protocol tests

- `ReportExecStatus` thrift decode error 返回 nonzero status/message。
- OK final report 能被 coordinator registry 接收。
- Error final report 能触发 failed state。
- `BatchReportExecStatus` 中任一 payload 错误时返回明确错误。
- client 侧 nonzero response 转成 `Err`，不吞 final report failure。

### 9.2 Shared report builder tests

- builder 从 `runtime::sink_commit` 填充 `sink_commit_infos`。
- builder 保持 tablet commit/fail infos 行为。
- builder 保持 load counters keys：`dpp.norm.ALL`、`dpp.abnorm.ALL`、`loaded.bytes`。
- FE-compatible adapter 和 standalone adapter 使用同一 builder 产物。

### 9.3 WriteCoordinator unit tests

- all expected writers final OK 后生成 `WriteCommitInput`。
- writer failed 后 write failed，并暴露 cancel fan-out 需求。
- duplicate same final report 幂等。
- duplicate conflicting final report fail fast。
- missing writer 不能 commit。
- unknown writer report fail fast。
- canceled writer 的 late OK report 不改变 failed 结果。

### 9.4 Integration tests

- all-in-one 路径验证 local report 仍能驱动 coordinator。
- 1FE+2BE standalone cluster 验证两个 BE 的 staged file metadata 都被 coordinator 收集。
- fault injection：一个 writer 返回 error，另一个 writer 被 cancel，最终 query failed 且不生成 `WriteCommitInput`。
- FE-compatible existing behavior regression：`sink_commit_infos` 仍通过 `TReportExecStatusParams` 发给 FE。

## 10. Observability

Coordinator 日志必须包含：

- write_id / query_id
- writer_id
- fragment_instance_id
- backend_num
- state transition
- final report summary：commit_info count、loaded rows、loaded bytes
- failure reason
- cancel fan-out target count
- duplicate report handling result

日志内容使用英文，便于 CI 和运维脚本匹配。

## 11. Acceptance Criteria

1. 1FE+2BE standalone distributed write 能收集两个 BE 的 staged file metadata。
2. 任一 writer failure 会 fail query，并 cancel 其它未完成 fragments。
3. duplicate report 幂等。
4. conflicting duplicate report fail fast。
5. missing writer 不能生成 `WriteCommitInput`。
6. coordinator logs 能看见 writer lifecycle。
7. FE-compatible 和 standalone distributed 使用同一 `TReportExecStatusParams` payload 和同一 report builder。
8. transport 差异被限制在 adapter：FE-compatible 走 FE thrift/brpc-compatible path，standalone 走 `NovaRocksGrpc`。

## 12. Risks

1. **Report destination 配置混乱**：BE 必须清楚当前 fragment 的 report 应发给 StarRocks FE 还是 NovaRocks coordinator。解决：adapter selection 在 fragment runtime 初始化时显式决定，并在日志中打印 mode/destination。
2. **Final report 重试导致重复处理**：解决：coordinator 状态机按 writer key 幂等。
3. **Thrift payload schema 演进**：解决：gRPC 只承载 thrift bytes，不复制字段；schema 继续由 thrift 定义维护。
4. **Query cleanup 早于 late report**：解决：report registry 对 query-gone 返回明确状态，BE final report worker 能区分 query-gone 与 transport failure。
5. **Commit strategy 被提前耦合**：解决：IW-4 只生成 commit/abort input，不调用 table-format commit。

## 13. Implementation Boundary

IW-4 v1 的最小可交付范围是：

- proto RPC 加入 `NovaRocksGrpc`。
- shared report builder 抽取完成。
- standalone report adapter 能把 final `TReportExecStatusParams` 发回 coordinator。
- coordinator-side `WriteCoordinator` 能消费 report 并生成 commit/abort input。
- `ExecutionCoordinator` 能在 writer failure 时触发现有 cancel fan-out。
- 单元测试和 1FE+2BE 集成测试覆盖 roadmap 验收。

后续 implementation plan 应按 TDD 拆分，不在 spec 阶段直接实现。
