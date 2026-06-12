# P8 Engine Error and CI Observability Design

日期：2026-06-12
状态：Draft，等待评审
来源：`docs/design/2026-06-12-distributed-execution-target-architecture.md` 的 P8
交付边界：一个大 PR 一次性完成完整 P8

## 1. 背景

P8 的目标是把分布式执行长期架构里的 fail-fast 站点收敛到一套 typed error 分类，并让
CI 和 EXPLAIN 能直接观测这些分类。当前代码里已经出现了几条彼此独立的错误词表或准词表：

- StarRocks 兼容协议的 `TStatusCode`。
- `grpc_server.rs` 里的 `REPORT_EXEC_STATUS_*` i32。
- metadata provider 的 `MetaErrorKind`。
- P6/P7 新增的字符串前缀式 stable code，例如 `IcebergWriteDescriptorMismatch`。
- Iceberg commit service 的 typed `CommitServiceError`。

这些机制各自合理，但跨到 engine/server/CI 边界时会重新变成字符串。尤其是
`grpc_server.rs` 当前用 `message.contains("write coordinator not found for query")`
判断 query-gone，这正是 P8 要删除的 regex-on-text 模式。

本设计选择一个大 PR 完成完整 P8。PR 内部仍按可验证顺序组织提交：先建权威词表和机器可读通道，
再接入 fail-fast 点、SQL runner、CI baseline、EXPLAIN boundary schema。

## 2. 目标

1. 新增 `EngineErrorCode` / `EngineError`，作为 NovaRocks 内部唯一语义错误词表。
2. 删除 P8 相关边界的字符串分类，尤其是 report-status 的 `contains()` 分类。
3. 让 gRPC report-status、standalone MySQL、SQL runner 和 CI summary 都能读取稳定 error code。
4. 增加 committed known-failures baseline，并按 `PASS` / `KNOWN_FAIL` / `NEW_FAIL` /
   `UNEXPECTED_PASS` 分类。
5. `EXPLAIN VERBOSE` 输出 distributed fragment/exchange/root 边界 schema，且 schema 工件来自
   lowering/exec 层，不从 optimizer 层重新推导。
6. 把 P6/P7 已有局部 code、P1 `type_relation::TypeMismatch`、Iceberg commit typed error 接入
   `EngineError`。

## 3. 非目标

- 不把全仓库所有 `Result<_, String>` 一次性迁移成 `EngineError`。
- 不把普通 parser/analyzer 用户错误强制改成 typed engine error。
- 不新增 `FrontendService.thrift` 字段，不把 `EngineErrorCode` 泄漏进 FE thrift status。
- 不恢复原 s8 的 `FragmentBoundaryContract`、`BoundaryCapabilities` 或 `transport_schema`。
- 不在 `EXPLAIN` 普通模式输出 schema dump。
- 不把 `logs/ci-full/` 作为 committed baseline；该目录继续作为本地运行产物。

## 4. 关键决策

### 4.1 词表关系

`EngineErrorCode` 是 NovaRocks 内部语义权威。现有词表的职责如下：

- `TStatusCode` 只作为 StarRocks/FE/MySQL 兼容 protocol envelope。
- `REPORT_EXEC_STATUS_*` 只作为 report-status RPC 的 transport/business envelope：
  `OK`、`ERROR`、`QUERY_GONE`。具体语义从 `EngineErrorCode` 来。
- `MetaErrorKind` 保持 metadata provider 的 domain-local error。进入 engine/server/CI 边界时映射到
  `EngineErrorCode`。
- `CommitServiceError` 保持 Iceberg commit lifecycle 的 domain-local typed error。进入 engine/write
  transaction 边界时包装成 `EngineError`。

禁止新增第四套长期语义词表。禁止 `From<EngineError> for String`，因为它会丢失 `code()`。

### 4.2 Error code 命名

第一批 `EngineErrorCode`：

| code | 来源/触发 |
|---|---|
| `TypeMismatch` | P1 `type_relation::TypeMismatch` |
| `TypeDeterminismViolation` | P2 决定性 guard |
| `ExchangeDescriptorMismatch` | P3 exchange descriptor 不一致 |
| `AggregateStateLayoutMismatch` | P5 aggregate state layout 不一致 |
| `IcebergWriteDescriptorMismatch` | P6 partition/write descriptor decode 或校验失败 |
| `UnsupportedDistributedDmlShape` | P7 DML shape 无法完整分布式表达 |
| `DistributedWriteOutputMismatch` | P7 writer output 不完整或互相矛盾 |
| `WriteCoordinatorGone` | writer report 到达时 coordinator/query 已不存在 |
| `CommitKnownUncommitted` | Iceberg commit typed definite failure |
| `CommitUnknown` | Iceberg commit unknown，需人工恢复判断 |
| `ProtocolDecodeError` | gRPC/thrift/proto payload decode 失败 |
| `InternalInvariantViolation` | 封闭 internal invariant 子类，不允许 free-form `Internal(String)` |

`InternalInvariantViolation` 必须携带静态子 code 或封闭 enum reason。它不是字符串逃生舱。

## 5. 架构

新增 `src/common/engine_error.rs`：

```rust
pub enum EngineErrorCode { ... }

pub enum EngineErrorDetail {
    TypeMismatch(crate::exec::chunk::type_relation::TypeMismatch),
    IcebergWriteDescriptor(crate::connector::iceberg::write_descriptor::IcebergWriteDescriptorError),
    CommitService(crate::connector::iceberg::commit::CommitServiceError),
    WriteCoordinatorGone { query_id: crate::types::TUniqueId },
    UnsupportedDistributedDmlShape { operation: &'static str, reason: &'static str },
    DistributedWriteOutputMismatch { operation: &'static str, reason: &'static str },
    InternalInvariantViolation { code: InternalInvariantCode },
    Message { static_code: EngineErrorCode, message: String },
}

pub struct EngineError {
    code: EngineErrorCode,
    detail: EngineErrorDetail,
}
```

边界转换必须显式命名：

- `EngineError::code() -> EngineErrorCode`
- `EngineError::to_user_message() -> String`
- `EngineError::to_log_fields() -> EngineErrorLogFields`
- `EngineError::to_tstatus_code() -> TStatusCode`
- `EngineError::to_mysql_error_kind() -> ErrorKind`
- `EngineError::to_report_status_code() -> i32`

允许 `Display for EngineError`，但 display 只用于人读消息，不能作为分类来源。所有分类必须使用
`code()`。

`EngineErrorDetail::Message { static_code, message }` 只用于已有稳定 `EngineErrorCode` 的边界包装，
例如 protocol decode 失败的可读 detail；它不能作为任意 internal string 的逃生舱。

## 6. 接入点

### 6.1 P6/P7 Iceberg write descriptor

`src/connector/iceberg/write_descriptor.rs` 已有 `IcebergWriteDescriptorError::code()`。P8 将它改为
可被 `EngineErrorDetail::IcebergWriteDescriptor` 包装，并让 collector/data_writer 边界保留 typed error
直到 engine boundary。

保留用户消息中的 readable detail，但 SQL runner 和 CI 读取 `IcebergWriteDescriptorMismatch` code。

### 6.2 Write coordinator report-status

`src/runtime/write_coordinator.rs::handle_report_exec_status` 和
`src/service/grpc_server.rs::handle_standalone_report_exec_status` 返回 `Result<_, EngineError>`。

当 coordinator 不存在且 report 携带 write metadata 时，返回：

```rust
EngineError::write_coordinator_gone(query_id)
```

`grpc_server.rs` 不再解析错误 message：

```rust
fn report_exec_status_error_code(err: &EngineError) -> i32 {
    err.to_report_status_code()
}
```

### 6.3 Type relation

`src/exec/chunk/type_relation.rs::TypeMismatch` 作为 `EngineErrorDetail::TypeMismatch` 的载荷，不重建
另一套 type mismatch 结构。`EngineErrorCode::TypeMismatch` 是 P8 对 P1 的统一出口。

### 6.4 Iceberg commit service

`CommitServiceError::KnownUncommitted` 映射为 `CommitKnownUncommitted`。
`CommitServiceError::Unknown` 映射为 `CommitUnknown`。

`run_iceberg_commit` 的 legacy string wrapper 可以保留给尚未迁移的调用方，但 P8 覆盖的 engine/write
transaction 路径必须消费 `run_iceberg_commit_typed` 或在边界立即包装成 `EngineError`，不能靠
`"iceberg commit unknown ("` 字符串分类。

### 6.5 Standalone server

standalone MySQL wire 仍返回 errno + message。P8 只改变消息格式和内部分类来源：

```text
ERROR 1105 (HY000): [IcebergWriteDescriptorMismatch] missing partition descriptor
```

`classify_query_error` 可继续服务普通 string errors。若输入是 `EngineError`，则使用
`to_mysql_error_kind()`，避免对 typed error 再做 lowercase contains。

## 7. Protocol

只改 `idl/proto/starust_grpc.proto`：

```proto
message ReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
  string error_code = 3;
}

message BatchReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
  string error_code = 3;
}
```

不改 `FrontendService.thrift`。FE-compatible thrift status 继续只拿 `TStatusCode` envelope 和 readable
message。

生成的 Rust proto 类型更新后，`standalone_exec_state_reporter.rs` 对 `REPORT_EXEC_STATUS_QUERY_GONE`
继续视为 terminal success，但测试要断言 `error_code == "WriteCoordinatorGone"`。

## 8. SQL runner

`tests/sql-test-runner` 增加可选 meta：

```sql
-- @expect_error_code=IcebergWriteDescriptorMismatch
```

语义：

- query 必须失败。
- runner 从错误文本中解析 bracket code：`[CodeName]`。
- 解析出的 code 必须与 `expect_error_code` 相等。
- 旧 `-- @expect_error=` 保留，继续做普通 substring 匹配。

`QueryExecution` 不需要承载失败，因为失败当前走 `(ok, execution, err_msg)` 返回。runner 增加一个
`extract_engine_error_code(err_msg: &str) -> Option<String>` helper，并在 verify/record/diff 三种模式都
使用同一逻辑。

SQL runner 日志在失败行输出 machine-readable 字段：

```text
engine_error_code=IcebergWriteDescriptorMismatch
```

CI runner 读取这个字段做 baseline 分类。

## 9. CI runner

### 9.1 Tier

`tools/ci/local-full-ci.sh` 增加：

```bash
--tier smoke|targeted|full
--from <run-dir>
```

语义：

- `smoke`：最小 SQL suite 和 Rust gate，用于快速验证 error plumbing。
- `targeted`：P8 相关 targeted suite，例如 `optimizer`、`iceberg-rest`、`aggregate`、`runtime-filter`。
- `full`：稳定 suite 清单或显式 `--all-discovered`。
- `--from` 只引用当前本地 run-dir 的已有日志做 summary/reclassification，不跨机器、不假设
  `logs/ci-full` 可提交。

现有 `--suite`、`--all-discovered` 保留。若同时传 `--tier` 和 `--suite`，显式 suite 优先，但 summary
仍记录 tier。

### 9.2 Known failures baseline

新增提交入仓文件：

```text
tools/ci/baselines/known-failures.toml
```

示例：

```toml
[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q93"
step = 1
error_code = "QueryTimeout"
reason = "Known distributed scheduling backlog, not covered by P8 type spine"
expires = "2026-07-15"
```

字段：

- `tier`: `smoke` / `targeted` / `full`
- `suite`: SQL suite name
- `case`: case id 或 file stem
- `step`: 可选 query step
- `error_code`: stable engine error code；非 engine timeout 可使用 runner-defined code
- `reason`: 必填，面向 reviewer
- `expires`: 可选，到期后 CI 报 `EXPIRED_KNOWN_FAIL`

CI summary 分类：

| 分类 | 含义 | 退出码 |
|---|---|---|
| `PASS` | case 通过，baseline 无期望失败 | 0 |
| `KNOWN_FAIL` | case 失败且匹配 baseline | 0 |
| `NEW_FAIL` | case 失败但无 baseline | 1 |
| `UNEXPECTED_PASS` | baseline 期望失败但实际通过 | 1 |
| `EXPIRED_KNOWN_FAIL` | baseline 命中但 expires 到期 | 1 |

known failure 不隐藏，summary 必须列出 suite/case/error_code/reason。

## 10. EXPLAIN boundary schema

只暴露 distributed boundary，不输出全 operator schema。

新增 lowering/exec 层工件：

```rust
pub enum BoundaryKind {
    ExchangeSender,
    ExchangeReceiver,
    RemoteRoot,
    ResultRoot,
}

pub struct BoundarySchemaColumn {
    pub slot_id: i32,
    pub name: String,
    pub arrow_type: DataType,
    pub logical_type: Option<TTypeDesc>,
    pub nullable: bool,
}

pub struct BoundarySchemaReport {
    pub fragment_id: Option<i32>,
    pub node_id: i32,
    pub boundary_kind: BoundaryKind,
    pub columns: Vec<BoundarySchemaColumn>,
}
```

工件来源：

- lowering/exec 层在构造 fragment/exchange/root 输出时记录 schema report。
- optimizer `src/sql/explain.rs` 只消费 report 并格式化，不从 `analysis::OutputColumn.data_type`
  重新推导。

`EXPLAIN VERBOSE` 增加：

```text
Boundary Schemas:
  Fragment 2 EXCHANGE_SEND node=17:
    slot=4 name=sum_amt arrow=Decimal128(38,2) logical=DECIMAL128(38,2) nullable=true
```

普通 `EXPLAIN` 不变。`EXPLAIN ANALYZE` 可复用 verbose schema 段，但不要求 per-operator runtime stats。

## 11. 测试计划

### 11.1 Rust unit tests

- `EngineErrorCode` display/parse round-trip。
- `EngineError` 到 MySQL/TStatus/gRPC envelope 的映射。
- `WriteCoordinatorGone` 分类不依赖 message substring。
- `IcebergWriteDescriptorError` 包装后 code 为 `IcebergWriteDescriptorMismatch`。
- `type_relation::TypeMismatch` 包装后 code 为 `TypeMismatch`。
- `CommitServiceError::KnownUncommitted` / `Unknown` 包装后分别映射到
  `CommitKnownUncommitted` / `CommitUnknown`。

### 11.2 gRPC tests

- 单条 report-status query-gone 返回 `status_code=REPORT_EXEC_STATUS_QUERY_GONE` 且
  `error_code=WriteCoordinatorGone`。
- batch report-status 同样带 error code。
- bad thrift 返回 envelope error，且 `error_code=ProtocolDecodeError`。

### 11.3 SQL runner tests

- `-- @expect_error_code=...` 成功匹配。
- `-- @expect_error_code=...` 在 query 成功时失败。
- `-- @expect_error_code=...` 在 code 不匹配时失败。
- 旧 `-- @expect_error=...` 不回归。

### 11.4 CI runner tests

- baseline 命中输出 `KNOWN_FAIL`。
- baseline 未命中输出 `NEW_FAIL`。
- baseline 期望失败但 case 通过输出 `UNEXPECTED_PASS`。
- expired baseline 输出 `EXPIRED_KNOWN_FAIL`。
- `--tier` 选择正确 suite 集合。
- `--from` 对已有 run-dir 做 reclassification，不要求重跑 SQL。

### 11.5 SQL golden

- 一个 `EXPLAIN VERBOSE` case 覆盖 boundary schema 输出。
- 一个 P8 error-code case 覆盖 `@expect_error_code`。
- 不重录全量 explain golden。

## 12. 实施顺序

虽然是一个大 PR，实施仍按以下顺序：

1. 新增 `EngineErrorCode` / `EngineError` 和单元测试。
2. 增加 proto `error_code` 字段并更新 gRPC tests。
3. 改 report-status query-gone 为 typed error，删除 `message.contains()` 分类。
4. 接入 P6/P7 write descriptor、distributed DML shape/output mismatch、commit service、type mismatch。
5. standalone server 输出 bracket error code，SQL runner 增加 `@expect_error_code`。
6. CI runner 增加 tier、baseline、summary 分类、`--from` reclassification。
7. lowering/exec 增加 `BoundarySchemaReport`，`EXPLAIN VERBOSE` 消费并输出。
8. 增加 focused SQL golden 和 targeted verification。

每一步都必须保留旧用户消息的可读性，但机器分类只能读 `EngineErrorCode`。

## 13. 风险和约束

- 一个大 PR 会跨 proto/server/runner/CI/explain，review 成本高。通过 commit 分层和 focused tests 降低风险。
- `Result<_, String>` 面很大，不能把普通用户错误全卷入 P8；只迁移 P8 支柱相关 fail-fast 点。
- MySQL wire 没有原生 engine error code 字段，只能用 bracket prefix。runner 对 bracket prefix 的解析是
  protocol boundary 约定，不是内部分类依据。
- baseline 可能掩盖真实回归，所以 `KNOWN_FAIL` 必须显式列出，`UNEXPECTED_PASS` 必须失败，过期 baseline
  必须失败。
- EXPLAIN schema 工件必须来自 lowering/exec 层。若某条 boundary 当前拿不到 `TTypeDesc`，实现期应输出
  `logical=<none>` 并补 task，而不是从 optimizer 猜。

## 14. 验收标准

- P8 相关 fail-fast 点都有 stable `EngineErrorCode`。
- report-status 不再通过错误字符串判断 `QUERY_GONE`。
- gRPC response 包含 `error_code`，且 FE thrift 不新增字段。
- SQL runner 支持 `@expect_error_code`，旧 `@expect_error` 仍可用。
- `tools/ci/baselines/known-failures.toml` 被提交，`local-full-ci.sh` 能按 baseline 分类。
- `EXPLAIN VERBOSE` 显示 distributed boundary schema，普通 `EXPLAIN` 不受影响。
- targeted unit/gRPC/runner/CI/SQL golden 全部通过。
