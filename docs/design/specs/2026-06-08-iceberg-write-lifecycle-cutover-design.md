# Iceberg 写入生命周期 Cutover 设计

## 目标

收尾 Iceberg distributed write lifecycle 这条线:让
writer/coordinator/commit/lifecycle 共享链路成为用户级 Iceberg 写入的**默认且唯一**
路径。

设计一次到位(本文覆盖 append、overwrite、RowDelta-family DML 全部路径),
但**落地可以拆成多个 PR**。"无回退"按 **per-flow** 粒度执行:不提供 session/config
开关回旧路径;每个 flow 的旧默认入口在切换它的同一 PR 内删除或降级为内部 helper。

范围:

- `INSERT INTO` append 默认走 distributed writer lifecycle。
- `INSERT OVERWRITE` 和 `INSERT OVERWRITE PARTITIONS` 默认走同一套 lifecycle,并保留 overwrite 特有校验。
- 当前支持的 RowDelta-family DML(`DELETE`、`UPDATE`、`MERGE`,含 DV 和 COW update),
  以及 `ADD EQUALITY DELETE`,默认走同一套 lifecycle。
- 不新增 session 或 config fallback 回旧同步 writer 路径。
- 不启动 partition MV、后台恢复调度器、新 optimizer rule 或无关 maintenance 工作。

**前置条件(最关键)**:今天用户 SQL 写入走的是全保真的 `inject_written_file` 路径;
distributed coordinator 路径的文件元数据通道(`TSinkCommitInfo` → `convert()`)是有损的。
在切换任何默认路径之前,必须先把该通道补到无损(见
"写入元数据通道 (Phase 0)")。这是整条 cutover 的真正阻塞点,也是 IW-4 coordinator
至今对写入 dormant 的根因。

预期结果:IW-6 correctness 语义和第一批用户可见 IW-7/IW-8/IW-9 写入 cutover 作为一条主线
落地,而不是在多个路径里重复做表面接入。

## 当前上下文

`origin/main` 已具备 typed Iceberg commit service facade(`run_iceberg_commit_typed`
+ `CommitServiceError`)和 shared Iceberg operation lifecycle repository。MV refresh 已通过
`StoredMvRefresh.operation_id` 和 `mv.refresh/0002.avsc` 接入 shared lifecycle。

当前分支已加入第一层 writer lifecycle adapter:

- `src/runtime/write_operation_lifecycle.rs` 将 `WriteCommitInput` / `WriteAbortInput`
  转成 operation request 和 pre-commit abort fact(metadata-agnostic)。
- `src/engine/write_operation_lifecycle.rs` 在 engine boundary 持久化 writer operation,
  但要求调用方显式提供 `WriteOperationContext`。
- `src/service/grpc_server.rs` 的 report-status 测试使用 `write_registry_test_guard()`,
  避免污染全局 write registry。

尚未闭合的是生产路径 cutover:

- `ExecutionCoordinator::execute()`(`src/runtime/coordinator.rs`)当前只 log
  `WriteCommitInput`,返回 `QueryResult` 时丢弃 writer outcome,没有 `CoordinatedQueryResult`。
- 同步 Iceberg SQL 写入路径仍直接构造 data/delete files、用 `inject_written_file` 喂
  collector、调用 `run_iceberg_commit`,并在同一函数内做 cache invalidation 和
  dictionary stale marking。
- IW-4 distributed write coordinator 目前只服务 SELECT,对写入是 dormant。
- **writer→commit 元数据通道有损**:distributed 路径经
  `src/connector/iceberg/commit/collector.rs` 的 `convert()` 从 `TSinkCommitInfo`
  重建 `WrittenFile`,丢失列统计、v3 row-lineage `first_row_id`、`equality_ids`、
  `key_metadata`、Puffin/NDV sketch(详见 Phase 0)。

## 已确认决策

1. 使用单一 engine-owned transaction runner,不在每个 SQL flow 内各自接 lifecycle。
2. 当前支持的用户级 Iceberg SQL 写入路径全部默认切换。
3. 不提供 fallback 开关回旧同步 writer。
4. runtime execution state 不依赖 metadata state。
5. `WriteCommitInput` 本身不足以决定 commit;engine 必须显式提供 target、commit strategy、
   base snapshot guard 和 validation policy。
6. **切任何默认路径前,writer→commit 元数据通道必须无损。机制为扩展现有 `TIcebergDataFile`
   thrift(加 optional 字段)+ 接通 `convert()` 读 `column_stats`,sketch 走 writer report
   的 out-of-band 字段;以 parity 测试作为 gate。**
7. **设计一次到位,落地可跨多个 PR;无回退按 per-flow 执行,旧默认入口在切换它的同一 PR 内
   删除或降级为内部 helper。**
8. **用户写入操作走完整 operation 状态机,成功终态为 `Finalized`;`Committed` 是
   "metadata 已提交、finalization 未完成"的中间态。**
9. **empty-input 以"先建 record 覆盖 writing 阶段恢复、确认零产出后再决定"为准(见数据流)。**

## 架构

新增 engine-owned `IcebergWriteTransactionRunner`,作为所有需要 file writer output、
metadata commit、lifecycle persistence、cache invalidation 的 Iceberg SQL 写入的默认边界。

runner 流程(对应 operation 状态机):

1. (可选)静态可知空输入短路(见 Empty Input)。
2. 创建 `Preparing` 状态的 Iceberg operation record。
3. 将 source query 或 mutation plan 降成 coordinated writer plan;operation 推进到 `Writing`。
4. 执行 coordinated plan,经**无损通道**收集 writer outcome(`WriteCommitInput` /
   `WriteAbortInput`)。如需区分"plan 完成、等待最终 writer report",可用 `Collecting`。
5. 零产出处理(见 Empty Input)。
6. 将 `WriteCommitInput` 与 spec 提供的 commit 上下文转成 commit collector input;
   operation 推进到 `Committing`。
7. 调用 `run_iceberg_commit_typed`;成功记 `Committed`(snapshot id + manifest paths)。
8. operation 推进到 `Finalizing`,执行 post-commit finalization(cache invalidation、
   dictionary stale marking);成功推进到 `Finalized`。
9. 失败按"错误与恢复语义"记录对应终态。

`Aborting` / `Aborted` 保留给异步恢复或显式 abort 流程,不在同步用户写入主路径使用。

SQL-specific flow 不直接拥有 lifecycle。它们只构造 `IcebergWriteTransactionSpec`,表达:

- target catalog / namespace / table / ref
- operation kind
- commit op kind
- base snapshot id 和 base sequence guard
- validation policy
- sink mode
- source query 或 mutation plan
- snapshot properties
- post-commit finalization policy

runtime 保持 metadata-agnostic。`src/runtime/coordinator.rs` 只暴露 writer outcome,
不创建 operation record、不读取 Iceberg catalog、不调用 commit service。

## 组件边界

### 写入元数据通道(thrift / sink / convert)— Phase 0

这是 cutover 的前置基础设施,先于任何路由切换落地。

- `idl/thrift/Types.thrift`:给 `TIcebergDataFile` 加 optional 字段
  `first_row_id`(i64)、`equality_ids`(list<i32>)、`key_metadata`(binary),使用新增
  tag(建议 11/12/13)。新增 optional 字段对 FE-compat 路径向后兼容;在 standalone 模式下
  这些字段是 NovaRocks 内部 writer→coordinator 上报,不跨 FE。
- sink 侧(`src/connector/iceberg/data_writer.rs`、`src/connector/iceberg/sink.rs`):
  从 iceberg-rust `DataFile` 填充上述新字段(它们当前已填充 `column_stats`)。
- `src/connector/iceberg/commit/collector.rs::convert()`:解析 `column_stats` →
  bounds/counts/column_sizes;读取新 optional 字段;移除对 `EQUALITY_DELETES` 的硬报错,
  改为用 `equality_ids` 正确重建 equality-delete `WrittenFile`。
- sketch / NDV:在 writer report(`report_exec_status` 参数)上增加 out-of-band
  sketch-set 承载,runner 在 commit 前 `inject_sketch_set` 到 collector。
- partition_values:保留 `parse_partition_path` 重建,以 parity 测试覆盖 transform / binary
  分区;若 parity 不过,则补结构化 partition 承载。

### `src/engine/write_transaction.rs`

新增 runner 和 write spec 模块。

核心类型:

- `IcebergWriteTransactionSpec`
- `IcebergWriteSource`
- `IcebergWriteCommitPolicy`
- `IcebergWriteValidationPolicy`
- `IcebergWriteTransactionRunner`
- `IcebergWriteTransactionOutcome`

职责:

- 解析 target table 和 commit service 所需 base metadata。
- 构造 `WriteOperationContext`。
- 创建和推进 operation record。
- 调用 coordinated execution 和 typed commit service。
- 持久化 commit / failure fact。
- 执行 post-commit finalization。

依赖边界:可以依赖 `StandaloneState`、Iceberg catalog/table handle、operation repository
和 typed commit service。

### `src/runtime/coordinator.rs`

拆分 coordinator 返回结构:

- `execute()` 保留兼容 wrapper,仍返回 `QueryResult`。
- `execute_with_write_outcome()` 返回 `CoordinatedQueryResult { query_result, write_commit, write_abort }`。

`WriteAbortInput` 必须能暴露给 engine,用于 writer registration 之后发生的失败路径
(含 submit/fetch 失败)。coordinator 仍负责取消已提交 fragment 和收集 writer final report,
但不持久化 lifecycle fact。

### `src/runtime/write_operation_lifecycle.rs`

保留已有 adapter,并只扩展 runtime writer output 相关能力:

- staged artifact extraction
- 从 `WriteCommitInput` 创建 operation request
- 从 `WriteAbortInput` 创建 pre-commit abort fact
- 将 writer sink commit info(经无损通道)转成 commit collector 所需 written-file metadata

该模块不能依赖 `StandaloneState` 或 Iceberg catalog registry。

### `src/engine/write_operation_lifecycle.rs`

继续作为 writer operation fact 的小型持久化桥。新 transaction runner 应调用它,
而不是在 runner 内重复写 repository transaction 逻辑。

### `src/connector/iceberg/operation_lifecycle.rs`

继续负责 typed commit service result 到 operation fact 的映射:

- committed outcome -> `Committed`
- known-uncommitted -> `FailedKnownUncommitted`
- unknown -> `CommitUnknown`
- finalize failure -> `FinalizeFailedKnownCommitted`

如果 runner 需要 operation-id-aware wrapper,应加在这里或
`src/engine/write_operation_lifecycle.rs`,不能散落在 SQL flow 中手写 fact merge。

### 现有 SQL 写入 flow

这些模块转为 spec builder 和 SQL-specific validation owner:

- `src/engine/iceberg_writer.rs`
- `src/engine/delete_flow.rs`
- `src/engine/mutation_flow.rs`
- `src/engine/equality_delete_flow.rs`

它们保留 SQL-specific validation 和 planning,但最终 writer、metadata commit、lifecycle、
finalization 都通过 `IcebergWriteTransactionRunner`。

UPDATE / MERGE 的 match-query 执行、行身份提取(`__nr_file` / `__nr_pos` / `__nr_row_id`)、
COW/MOR 选择、replacement 或 delete-file plan 构造留在 SQL flow;只有 writer/commit/
lifecycle/finalization 委托给 runner。这部分 SQL 逻辑无法干净退化成纯 spec builder,
是分阶段排序中风险最高的 flow。

**注**:`equality_delete_flow` 当前只做 cache invalidation,漏了 `mark_target_stale()`
(其他三个 flow 都有)。切到 runner 的统一 finalization 后应一并修正这个潜在 bug。

旧同步 writer implementation 能删则删;删不掉的函数只作为内部 helper,不能作为可切换执行
路径继续存在。

## 数据流

### Append

`INSERT INTO iceberg_table SELECT ...` 构造 append transaction spec:

- operation kind: `InsertAppend`
- commit op kind: `FastAppend`
- source: 产出 target-aligned rows 的 query
- sink mode: data files
- validation: append-compatible schema 和 ref 检查

runner 执行 coordinated writer plan,并通过 typed commit service commit 收集到的 data files。

### Overwrite

`INSERT OVERWRITE` 构造 overwrite transaction spec:

- operation kind: `InsertOverwrite`
- commit op kind: `Overwrite`
- validation: 保留当前 overwrite 限制,包括 variant、partition spec 等已有校验

empty input 不是 no-op。它仍必须 commit overwrite 语义并清空目标范围。

`INSERT OVERWRITE PARTITIONS` 使用同一 runner,但 commit op kind 为 `OverwritePartitions`,
并使用 partitioned-table validation policy。

### RowDelta / DML

DELETE、UPDATE、MERGE 继续拥有 DML-specific planning:

- touched file discovery
- MOR/COW mode selection
- row-lineage 和 DV validation
- replacement 或 delete-file plan construction

它们输出 `IcebergWriteTransactionSpec`,并选择正确 commit op kind:

- `RowDelta`
- `RowDeltaDv`
- `CowUpdate`

runner 负责 writer execution、commit 和 lifecycle persistence。

### Equality Delete

`ADD EQUALITY DELETE` 构造 equality-delete spec:

- content: EqualityDeletes,携带 `equality_ids`
- 经无损通道(Phase 0 后 `convert()` 不再对 `EQUALITY_DELETES` 报错)与 runner commit

### Empty Input

empty 检测分两类:

- **静态可知空**(空 `VALUES`、静态可判定为空的 source):在创建 record 前短路。
  append / RowDelta 直接返回 OK,不创建 operation record;overwrite 仍创建并 commit。
- **query-sourced**(只有执行后才知道是空):runner 已创建 `Preparing` record
  (覆盖 writing 阶段崩溃恢复)。执行后若 writer outcome 为零产出且零 staged artifact:
  - append / RowDelta:删除该 `Preparing` record 并返回 OK。**安全性**:零 staged artifact
    意味着没有 orphan 文件,删除 record 不丢任何恢复信息,也避免 operation repository 噪声。
  - overwrite:仍 commit overwrite,清空目标范围(empty overwrite 不是 no-op)。

这样既避免 append/mutation no-op 噪声,又保留 overwrite 语义,同时不牺牲 writing 阶段的
崩溃可恢复性。

## 操作状态机

用户写入驱动现有 `IcebergOperationState`:

```
Preparing -> Writing -> (Collecting) -> Committing -> Committed -> Finalizing -> Finalized
```

失败终态:`FailedKnownUncommitted` / `CommitUnknown` / `FinalizeFailedKnownCommitted`。
`Aborting` / `Aborted` 保留给异步恢复或显式 abort,不在同步用户写入主路径出现。该状态机与
MV refresh 共用(MV 的 finalization 更重)。`Committed` 与 `Finalized` 必须区分:
finalize 失败发生在两者之间,记 `FinalizeFailedKnownCommitted`。

## 写入元数据通道 (Phase 0)

**问题**:collector 有两条喂入路径。现状 SQL 写入用 `inject_written_file`(全保真
`WrittenFile`:per-column bounds、counts、结构化 partition_values、`key_metadata`、
`equality_ids`、`first_row_id`,外加 `inject_sketch_set` 的 Puffin/NDV sketch)。cutover 要切到的
distributed 路径,唯一文件元数据通道是 writer 上报的 `TSinkCommitInfo`,经 `convert()`
重建 `WrittenFile`,而 `convert()` 有损。

损耗分四类(性质不同,修复成本不同):

- **(a) 已在线上却被丢弃**:`column_stats`(bounds / counts / column_sizes)writer 已填充到
  `TIcebergDataFile.column_stats`,但 `convert()` 未读取,全置 `Default`。接通 `convert()`
  即可,同时修正这个现存 bug。
- **(b) thrift 缺字段**:`first_row_id`(v3 row-lineage)、`equality_ids`、`key_metadata`
  在 `TIcebergDataFile` 结构里不存在。扩展 thrift(已确认决策 6)。
- **(c) out-of-band**:Puffin / NDV sketch 在 `TSinkCommitInfo` 里没有承载位置。report
  增加 sketch-set 承载。
- **(d) 近似重建**:partition_values 经 `parse_partition_path` 反解。parity 测试覆盖,
  必要时补结构化承载。

下游影响(对应各 flow):

- (a) 不修 → append/overwrite 切过去丢 manifest 列统计,读侧 range 裁剪退化(静默回归)。
- (b) `first_row_id` 不修 → v3 MOR UPDATE row-lineage 回归(正确性问题)。
- (b) `equality_ids` 不修 → equality-delete 在 `convert()` 硬报错。
- (c) 不修 → NDV sketch 丢失。

**Gate**:parity 测试——构造代表性 `WrittenFile`(含 bounds、lineage、`equality_ids`、
各类 partition),sink → `TSinkCommitInfo` → `convert()` round-trip,逐字段断言与 inject 路径
一致。**任一 flow 只有在其所需字段通过 parity 后,才允许切到 coordinator 路径。** parity
未通过的字段,要么补齐通道,要么让对应 cutover checkpoint 失败,而不是保留旧 default path。

## 错误与恢复语义

Iceberg operation record 是用户可见写入事实的 source of truth。Writer coordinator state
只是内部 collection state,不能替代 operation state。

### Pre-write failure

runner 创建 operation 后、提交 writer fragment 前失败时,记录 `FailedKnownUncommitted`,
staged artifacts 为空,`next_action=None`。

### Writer failure / timeout / client disconnect

writer fragments 已提交但 metadata commit 之前失败时,获取或构造 `WriteAbortInput`,
记录 `FailedKnownUncommitted`。

如果存在 staged artifacts,`next_action=RetryAbort`;否则 `next_action=None`。

### Commit-ready failure

已有 `WriteCommitInput` 但 writer metadata 不完整或不一致,导致无法构造 commit input 时,
记录 `FailedKnownUncommitted`。这是 pre-commit known-uncommitted failure。

### Commit success

`run_iceberg_commit_typed` 成功时,记录 `Committed`,包含 snapshot id 和 written manifest
paths,随后进入 `Finalizing`。cache invalidation 和 dictionary stale marking 只在 `Committed`
fact 持久化后执行;finalization 成功推进到 `Finalized`。

### Known-uncommitted commit failure

`CommitServiceError::KnownUncommitted` 记录 `FailedKnownUncommitted`、cleanup outcome、
failure kind 和 next action。

cleanup error 不改变主事实,只影响 diagnostic 和是否继续保留 `RetryAbort`。

### Commit unknown

`CommitServiceError::Unknown` 记录 `CommitUnknown` 和 recovery evidence。runner 不能清理
staged files,也不能 invalidate table cache。用户可见错误必须包含 operation id、state、
failure kind 和 `ManualInspect` next action。

### Post-commit finalize failure

`Committed` 之后(`Finalizing` 阶段)的 finalization failure 进入
`FinalizeFailedKnownCommitted`。用户可见错误必须说明 metadata commit 已 known committed,
避免用户盲目重试写入。

普通 INSERT / OVERWRITE / RowDelta 的 finalization 主要是 cache 和 dictionary maintenance。
MV refresh 仍是 domain-specific finalization 更重的路径。

## 无回退与分阶段落地

不新增 session variable 或 config 返回旧同步 writer 路径。无回退按 **per-flow** 执行:
每个 flow 的旧默认入口在切换它的同一 PR 内删除或降级为内部 helper,避免后续贡献者重新绕回
旧路径。

设计一次到位(本文),落地建议拆分如下(顺序体现依赖与风险递增):

- **PR-0(Phase 0 通道)**:扩展 thrift、sink 填充新字段、`convert()` 接通 `column_stats`
  与新字段、sketch out-of-band、parity 测试。无用户可见路由变化。顺带修 `convert()` 列统计
  bug。
- **PR-1(runner + coordinator 管道)**:runner / spec 类型 + fake writer/commit 单测;
  coordinator `execute_with_write_outcome` + `CoordinatedQueryResult`,并把 `WriteAbortInput`
  暴露给 engine。无路由变化。
- **PR-2(append + overwrite)**:切 `INSERT INTO` / `INSERT OVERWRITE` /
  `OVERWRITE PARTITIONS`,删旧默认入口,补 SQL 覆盖 + 真实 operation-record 断言。
- **PR-3(DELETE + equality-delete)**:切 `DELETE`(RowDelta / DV)与 `ADD EQUALITY DELETE`,
  删旧默认入口;顺带修 equality-delete dictionary stale。
- **PR-4(UPDATE + MERGE)**:切 COW/MOR update 与 MERGE,删旧默认入口。最重的一段,
  match-query 与行身份逻辑留在 SQL 层。

每个 cutover PR 以其 flow 在 PR-0 的 parity 测试通过为前提。落地必须依赖 deterministic
tests 捕获主要回归,而不是依赖开关规避风险。

## 测试策略

### Parity Tests(Phase 0 gate)

`sink → TSinkCommitInfo → convert()` 逐字段 round-trip 断言,覆盖 append / overwrite /
RowDelta / DV / COW / equality-delete 所需字段,以及 identity / transform / binary partition。
parity 通过是对应 flow 允许 cutover 的硬前提。

### Unit Tests

覆盖:

- runner operation creation 和 state transition(含 `Committed -> Finalizing -> Finalized`)
- writer commit -> staged artifact / commit request conversion
- writer abort -> known-uncommitted fact conversion
- commit success -> committed fact
- known-uncommitted -> cleanup outcome / retry action
- commit unknown -> recovery evidence / no cleanup
- finalize failure -> known-committed failure
- empty append / empty overwrite / empty RowDelta policy(含 query-sourced 删除
  `Preparing` record 的路径)

### Runtime Harness Tests

使用 fake 或 in-process coordinated writer outcome 验证:

- successful append 产生 committed -> finalized operation
- writer timeout 记录带 abort evidence 的 known-uncommitted
- writer final-report mismatch 记录 known-uncommitted
- commit unknown 记录 recovery evidence 且不清理 staged files

这些测试必须 deterministic。不要在 SQL test 中依赖真实 timing 或 flaky network failure。

### SQL Tests

新增或更新 Iceberg SQL tests:

- `INSERT INTO ... SELECT`
- `INSERT OVERWRITE ... SELECT`
- `INSERT OVERWRITE PARTITIONS ... SELECT`
- 当前支持的 DELETE RowDelta / DV 输出路径
- `ADD EQUALITY DELETE`
- 当前支持的 UPDATE / MERGE RowDelta 或 COW update 路径

SQL tests 验证表内容。**至少一个真实 SQL test 必须断言用户写入产生了带 snapshot id 的
`Committed` / `Finalized` operation record**——这是"新路径确实被走到"的端到端证据,
不能只靠 harness fake。若当前 test runner 暂不支持稳定读取 metadata repository,则该断言
放到直接驱动真实 engine routing(非 fake)的单测,并明确这是 cutover 验收项;diagnostic 的
其余覆盖可放到 deterministic harness。

### Regression Tests

保持现有 focused tests 绿色:

- `write_commit`
- `writer_abort`
- `staged_artifacts`
- `writer_operation`
- `write_coordinator`
- `report_exec_status`
- commit service typed error tests
- 被 shared operation helper 触及的 MV lifecycle tests

## 风险与缓解

### 元数据通道有损(最高优先,已升级为 Phase 0)

不再是风险注脚:列统计是现存 bug、row-lineage / equality 是协议缺口、sketch 缺通道。
缓解=PR-0 先做无损 + parity gate;未通过 parity 的 flow 不许切。runner 仍必须 fail fast,
缺字段时记 `FailedKnownUncommitted`,不能猜。

### PR 面较大

设计一次到位但 per-flow 拆 PR、按风险递增排序;shared runner 保持小,每个 SQL flow 只做
spec builder,而不是第二套 lifecycle implementation。

### Commit unknown 误分类

commit service 已提供 typed unknown error。runner 必须消费 `CommitServiceError::Unknown`
enum,不能解析 legacy string。

### Commit 后 cache invalidation 失败

metadata commit 成功后的 cache invalidation 失败不能表现成写入失败。它要记录
`FinalizeFailedKnownCommitted`。

### 旧路径回流

因为没有 fallback,实现应 per-flow 删除或降级旧入口。测试应断言用户 SQL path 调用
transaction runner,而不是旧同步 writer function。

## 实现计划提示

后续 implementation plan 按上面 PR-0..PR-4 拆 checkpoint:

1. PR-0:扩展 `TIcebergDataFile` thrift、sink 填充、`convert()` 接通 `column_stats` +
   新字段 + 移除 equality-delete 硬报错、sketch out-of-band、parity 测试。
2. PR-1:定义 runner / spec types,用 fake writer 和 fake commit service outcome 测
   state transition;扩展 coordinator return shape(`CoordinatedQueryResult` +
   `execute_with_write_outcome`)暴露 writer outcome 与 abort input,同时保留兼容 wrapper。
3. PR-2:cut over append、overwrite、overwrite partitions,删旧默认入口,补 SQL 覆盖 +
   真实 operation-record 断言。
4. PR-3:cut over DELETE(RowDelta / DV)与 ADD EQUALITY DELETE,删旧默认入口,
   修 equality-delete dictionary stale。
5. PR-4:cut over UPDATE / MERGE(COW / MOR),删旧默认入口。
6. 每个 cutover PR 跑 focused unit tests、Iceberg SQL tests 和 lifecycle regression tests,
   并以对应 flow 的 parity 测试通过为前提。

plan 不能引入 fallback switch。如果某条路径缺少安全 cutover 所需 writer metadata,正确动作
是先在 PR-0 补齐通道并让 parity 通过,而不是保留旧 default path 或在 runner 内猜测。
