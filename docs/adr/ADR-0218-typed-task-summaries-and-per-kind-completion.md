---
id: ADR-0218
title: "Typed summaries freeze authors, contributor units and exact per-kind completion"
domain: [pipeline-execution, distributed-query-lifecycle, frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-11-06
related:
  - "ADR-0206-frozen-write-placement-and-task-completion-assembly.md"
  - "ADR-0208-logical-write-counts-and-strict-stream-delivery.md"
  - "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
provenance:
  - "discussion: 2026-11-06 closed typed summaries, author-declared contribution units and exact per-kind completion"
code-anchors:
  - "novarocks/result-contract/src/summary.rs (SummaryKind, SummaryField, SummaryValue, DeleteVerificationCounts, PerKind)"
  - "novarocks/physical-plan/src/write_execution.rs (SummaryColumns, SummaryAuthor, SummaryOutput, SummaryCut)"
  - "novarocks/physical-plan/src/validation/summary.rs (validate_summary_output, validate_summary_cut, validate_fragment_summary_inputs)"
  - "novarocks/local-program/src/sink.rs (StaticSummaryBranch, StaticSummaryAuthor)"
  - "novarocks/execution/src/exec/operators/logical_change_summary.rs (SummaryAuthorAssemblyObligation, SummaryAuthorObligation, SummaryUnitObligation, SummarySourceFactory, SummaryCoverage)"
  - "novarocks/execution/src/runtime/task/sink.rs (materialize_compiled_sink_for_pipeline)"
  - "novarocks/execution/src/exec/pipeline/executor.rs (prepare_compiled_program_with_sink_materializer)"
  - "novarocks/execution/src/exec/operators/table_finish/logical_input.rs (LogicalFinish)"
  - "novarocks/spi/src/connector/write_stack/relation.rs (RootRowKind, RootWriteResultRowShape, RootWriteResultMembershipValidator)"
  - "novarocks/native-adapter/src/root_write_commit_codec.rs (WriteCommitRecordView, WriteCommitTotals)"
  - "novarocks/frontend-application/src/query_execution/write_result.rs (RootWriteDecodeContract, RootWriteResultDecoder, DecodedPreparedWriteSet)"
  - "novarocks/frontend-application/src/task_execution/protocol_facts.rs (FrozenAttemptProtocol)"
  - "novarocks/frontend-application/src/task_execution/completion.rs (WriteCompletionTracker)"
---

## 问题

分布式写入怎样让不同作者产生的完成事实按种类准确归并，同时保留各自的输入耗尽、来源覆盖、资源保管与外部发布边界？

## 背景与执行事实

summary 是一个 Task 在特定种类上的完成事实。每种具有封闭的有序字段、线路类型、零值和合并规则。writer 的物理产物、逻辑变化计数和删除核验回答不同问题；相同 Task 可以同时产生多个种类，但一种完成不能补足另一种缺失。

本条补充 [ADR-0206](ADR-0206-frozen-write-placement-and-task-completion-assembly.md) 与 [ADR-0208](ADR-0208-logical-write-counts-and-strict-stream-delivery.md)：既有写计数继续按 Router 的实际 driver 贡献，本条把独立输出、完成单位和来源覆盖扩展为按种类的合同。两条旧 ADR 的 Provider 路由、严格消费、真实退出与 FE publication 分工保持有效；[ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 的同一逻辑执行与原始 custody 继续适用。

| 种类 | 有序的非空线路列 | 合并与结果用途 |
|---|---|---|
| `WriteCounts` | inserted、replaced、deleted：三个 Int64 | 逐项受检相加，三项总和受 Int64 结果域约束；`LogicalChanges` 计数模式使用其受影响行数 |
| `DeleteVerification` | requested、matched、insufficient_keys：三个 Int64；insufficient_fingerprint：FixedSizeBinary(16) | 三个计数各自受检相加，各有 Int64 结果域；指纹按小端 u128 做模 2^128 相加；提供准确核验值，不解释业务决定 |

| owner | 保管的原事实 | 接受边界 |
|---|---|---|
| `SummaryKind` / `SummaryValue` | 封闭种类、字段身份、零值与同种类合并 | 不接受异种类算术，不携带覆盖自证 |
| Fragment 与 `StaticSummaryBranch` | 每种至多一个输出、准确作者、字段、分支与布局 | 作者为 Router sink，或声明对应作者身份的本 fragment 节点；种类和字段不从 sink 形状猜测 |
| `SummaryAuthorAssemblyObligation` | 作者领取与冻结单位数的准备义务 | `seal` 受检；无 Drop 检查，不以未消费义务覆盖原构造失败 |
| `TaskSummaryState` / `SummaryUnitObligation` | 原 Task 内的单位领取、值、正常耗尽及失败 | 单位完整输入严格 EOS 后完成一次；未完成即 Drop 会记录失败并唤醒原 Source |
| `SourceBoundSummaryInput` / `SummaryCoverage` | 接收队列提供的准确来源身份、每源一次与受检合并 | 不接受载荷自选来源；重复、外来与缺失均拒绝 |
| `LogicalFinish` / Root codec | writer 结果、各种类输入与原核心续体；类型化 Root 记录 | 所有声明种类独立覆盖完整后才能 finalize；codec 先预检再发行，FE 按声明核对是否必须出现 |
| `FrozenAttemptProtocol` / `WriteCompletionTracker` | 同一个 attempt 的 writer、按种类来源与 Root Task 完成义务 | Task 终态与完整 Root 数据、End 分别核对；完成事实不授予 publication 权限 |

现有生产作者是 Router 的 `WriteCounts`。所有现有物理节点与程序节点的 `summary_authorship` 均返回 None，因此节点作者计划明确被拒绝。删除核验的执行与解码合同已经具名，生产节点、其完整输入耗尽证明及业务消费者仍须在实际接入时完成；测试作者不能证明该生产路径已可用。本条不记录任何测试通过或完整分布式验收结论。

## 考虑过的选项

**每个种类分别挂在各自的 sink 或节点上：设计否决。** 它保留调用处的局部便利，但会产生不同的输出声明、编解码、来源与完成规则；节点作者还被迫取得不属于它的 sink 身份。采用 fragment 级声明和显式作者引用。

**开放的列加规约器框架：待评估；当前采用封闭种类。** 它允许调用方组合不同归约算法，但字段形状不能证明含义，还会把溢出与线路类型变成开放组合。当前使用封闭种类，增加种类时审阅完整语义并更新 exact compatibility 合同。若出现真实的新规约需求，再评估开放框架。

**一律按作者所在 pipeline 的 DOP 创建单位：设计否决。** 多输入作者可能跨多组 driver，只有作者知道何时完整耗尽；任一 pipeline 的宽度不能代表全部义务。作者在任何 driver 创建之前声明正的单位数，Router 的单位数仍取实际 root DOP。

**从 Task 终态、profile 或已收到的报告推导完整性：设计否决。** 缺失报告会让缺失来源从集合中消失；终态也不能证明 FE 已收到完整 Root 数据。来源集合在执行前冻结，逐源覆盖与 Task 终态分别证明。

**把核验值封装成不透明 Root body：设计否决。** 它绕开 Root 类型化列的可见合同，使 Arrow 形状和互斥性只能事后猜测。采用独立记录种类及四个可空的类型化列，其他记录在这四列保持 NULL。

**分层归约或跨 attempt 的持久 summary：待评估。** 前者可能降低 Root 汇聚成本，后者可能支持失败接续，但都需要额外身份、覆盖和持久恢复合同；当前只保留单个 attempt 内的原始归约责任。

## 裁决

1. **封闭种类规则。** 种类及其字段顺序、非空线路类型、零值、负数拒绝和合并算法由中立 `result-contract` 唯一定义。不同种类不能合并；核验指纹只作证据，不参与准确相等、覆盖或发布资格判定。

2. **独立声明与准确作者规则。** summary 属于 fragment 的独立输出，不属于 Provider route 的 target ordinal。每种至多一项，所有字段与准确作者都在纯计划中冻结。Router 只作者 `WriteCounts`；节点作者须由节点种类声明，并核对字段确由该节点输出。边与 cut 保持同一种类、准确字段、MustDrain、非分区 Stream 和单份所有权。纯编译不取得 Task、connector 或当前 topology。

3. **作者单位冻结规则。** 在任何 driver 创建之前，每个作者领取一次 `SummaryAuthorObligation` 并声明正的单位数。Router 以实际 root DOP 为单位数，每个 driver 领取一个；其他作者按完整输入结构声明，不能从 profile、Task 终态或收到的载荷推导。准备装配的 `seal` 必须确认每个声明作者都交出了原状态。

4. **严格耗尽与末尾领取检查规则。** 每个单位只领取一次，只在自身完整输入严格 EOS 后完成一次。零行输入也要沿正常耗尽完成零值；不能把未收到当作零。未完成即丢弃的单位把 `MissingContributor` 记入同一 Task 状态，禁止发行 summary。所有计算 pipeline 先实例化 driver，随后在图末尾创建独立 summary Source；Source 创建时检查全部单位已领取，运行时等待全部正常完成才恰好发一行。全部 driver 加入原 Task 的同一 DriverBatch、取消与真实退出 owner。

5. **逐种类准确覆盖规则。** 每种输入使用冻结的完整 `ExchangeSource` 集合，身份从原接收队列取得，每源恰好一行。值的合并与覆盖分别保管；外来、重复或缺失来源、形状错误、负数及计数溢出均拒绝。其他种类完整不能掩盖本种类缺失，失败携带种类，原来源身份继续由原 receiver 与 coverage 保管，不保证错误文本包含具体来源。

6. **公平完成规则。** `LogicalFinish` 按 writer、声明的各种类输入、原 aggregate/unpivot 核心续体轮转。每轮先给予子项实际工作机会，再计父级协调；不能先等齐 summary 再消费 writer partial。writer 与全部声明种类均 End、覆盖核对完成且核心可收尾时，才能 finalize；同一 receiver 不再建立竞争消费的第二个普通 ExchangeSource。

7. **Root 记录与 FE 声明规则。** 行数记录名为 `RowCount` / ROW_COUNT，线路值仍为 1，完整 Root 恰好一条；它表达写入行数，不代表所有 summary 种类。声明删除核验时另发一条 DELETE_VERIFICATION；Root 四个核验列在该记录非空、在其他记录为 NULL。PWC 的核验载荷固定为三个 i64LE 加 16 字节指纹，负数或重复在发行 header 前拒绝。codec 只保证至多一条；FE 按冻结声明拒绝未声明、重复与缺失，交出原核验值，不代替领域作发布或退役决定。schema 与线路变化遵循原 exact compatibility island，无旧格式读取回退。

8. **同 attempt 与原 owner 规则。** FE 从同一冻结 placement 分别保留 writer 与每个种类的来源 Task 集合；每个集合均属于同一个准确 Task graph。完整 Root 数据与 End、writer/summary/root Task 成功、实际退出分别保留，不能跨 attempt 拼接完成事实。状态与覆盖使用原 Task tracker，schema、结果、Transport 与真实退出继续由各自的原 owner 保管；声明身份、Source 关闭或 Task 终态均不授予容量、外部提交或清理权限。

## 接受的妥协（诚实记录）

封闭种类让每个字段的语义可检查，但增加一种完成事实就要同时扩展静态计划、线路、局部程序、运行时、Root 与 FE，且触及 exact compatibility 身份。这是为单一语义作者接受的协调成本；没有把“可解码”当作“可混合执行”。

每种保留独立 Source、输入、单位状态和来源覆盖，空间随实际单位与 Task 来源数增长。固定三个或四个载荷字段不提供总 metadata 或 RSS 上界；真实容量拒绝沿原查询资源 owner 处理，不另设种类专用配额。来源索引的排序仍是不可抢占的库工作，公平轮转也不提供任意 kernel 的抢占保证。

Root 结果追加四列使普通记录携带四个 NULL，仍由一个 Root 汇聚完成事实。选择类型可见与原发布边界，接受额外的少量记录宽度；没有据此证明最优吞吐、Root failover 或跨重启恢复。

节点作者的校验接缝和纯执行测试作者，不提供任何生产核验能力。节点实际结构、单位冻结点、所有输入 EOS 证明与业务消费者必须在接入时各自取证。开发验证、原生分布式准入与完整测试范围独立于本条架构裁决记录。

## 何时重新评估

- 新增真实 summary 消费者，且其规约无法由封闭种类清晰表达时，重新评估开放规约框架；保留字段语义、错误算法、完整来源与 exact compatibility 的单一作者。
- 出现具有多组 driver 的生产节点作者时，审阅其贡献单位如何覆盖全部输入、取消与真实退出；不得只新增 `summary_authorship` 返回值就声称完成接线。
- 测得单位状态、来源索引排序或单 Root 汇聚成为实际瓶颈时，评估可续体索引、分层归约或新的根结构，保留每种的准确覆盖与同一 publication owner。
- 需要跨 attempt 或跨进程故障恢复时，独立设计持久身份、请求、结果与残留责任；不得把旧 summary 缓存或新一轮的部分报告拼成原 attempt 的成功。
