---
id: ADR-0206
title: "Frozen write placement, actual drivers and exact Task completion compose before publication"
domain: [pipeline-execution, distributed-query-lifecycle, frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-10-28
provenance:
  - "discussion: 2026-10-23 accepted independent write placement, logical summary and strict delivery contracts"
  - "implementation: frozen write placement and exact Task completion assembly"
code-anchors:
  - "novarocks/physical-plan/src/write_execution.rs (StagePlacementPolicy, WriteRouteRequirements, LogicalSummaryColumns)"
  - "novarocks/query-application/src/coordination/schedule.rs (build_attempt_schedule)"
  - "novarocks/frontend-application/src/query_execution/artifact/task_manifest_binding.rs (TaskManifestBinding::summary_sources, TaskManifestBinding::writer_cohorts)"
  - "novarocks/frontend-application/src/task_execution/manifest_round.rs (ManifestAssembledRound::run)"
  - "novarocks/frontend-application/src/task_execution/protocol_facts.rs (FrozenAttemptProtocol, WriteProtocolTracker)"
  - "novarocks/frontend-application/src/query_execution/internal_consumer/write.rs (wait_write, WriteComputationOutput)"
  - "novarocks/native-adapter/src/compiled_writer_binding.rs (bind_compiled_writers)"
  - "novarocks/native-adapter/src/backend_task_execution/execution_host.rs (validate_native_strict_inputs, NativeTaskExecutionHost::retire_receiver_normally)"
  - "novarocks/execution/src/exec/pipeline/executor.rs (prepare_compiled_program_with_sink_materializer)"
  - "novarocks/execution/src/runtime/fragment/sink.rs (materialize_compiled_sink_for_pipeline)"
  - "novarocks/execution/src/exec/pipeline/builder/compiled_writer.rs (build_logical_finish)"
  - "novarocks/frontend-application/src/query/statement_publication.rs (StatementPublication)"
  - "novarocks/frontend-application/src/query/write_action.rs (selected_command_completion)"
  - "novarocks/frontend-application/src/query/mutation_action.rs (MutationAffectedRows)"
  - "novarocks/frontend-application/src/query_execution/write_session.rs (ConnectorWriteSession::freeze_received_row_count)"
  - "novarocks/query-application/src/session_control/statement_action.rs (StatementCommandCompletion, StatementActionReady::transfer_protocol_in)"
  - "novarocks/query-application/src/protocol_delivery.rs (GovernedCompletionStatementResult)"
  - "novarocks/mysql-adapter/src/terminal.rs (write_success, ok_payload)"
---

## 问题

分布式 change-stream 写入怎样把独立的源与 writer 放置、Task 内实际并行和逻辑 summary 装配成完整计算，同时让外部发布与真实收敛各有唯一 owner？

## 背景与执行事实

同一逻辑变化可以进入多个 Provider route。每个 route 的共置要求、实际实例集合和 driver 数量各有自己的事实；逻辑受影响行数既不等于物理写行数，也不等于某个固定 target 子集之和。纯计划只有要求和语义，不能取得 live sender、writer 或 Task-local accumulator。

[ADR-0208](ADR-0208-logical-write-counts-and-strict-stream-delivery.md) 固定了计数、严格消费和公平双输入的算子核契约；其正文保留当时装配尚未完成的阶段事实。本条补充当前装配规则，不替代该核，也不改写历史正文。[ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 的统一逻辑执行、原始 custody、计算就绪与独立 publication 继续适用。本条记录已组装的责任，不能据此推断完整 Native 分布式验收、性能收益或容量上界已经验证。

| 实体 | 唯一拥有的事实 | 不提供的证明 |
|---|---|---|
| Provider 的 row-mutation preparation / write session | 准确代次、identity、opaque route、写配方、共置与布局要求、外部效果 | 不选择本次 BE 或替 FE 定义 Task 集合 |
| SQL 完成的 PhysicalPlan | 一次物化的 selected action、effect、route/summary 端口、类型、分区和 stage placement policy | 不持有 Task runtime、session、凭据或当前 backend registry |
| Query Application 的 schedule 与 FE manifest | 一次 live backend snapshot 上实际选择并冻结的 Task、端点、边、writer 与 summary-source 集合 | 不根据报告缩小参与集合，不制造消费或提交事实 |
| Native Task binding 与 Execution | 原 descriptor 的准确能力、实际 computing drivers、同 batch 的唯一 Task summary 和 source-bound 输入 | 不取得外部 catalog mutation 权限 |
| Root finish 与 FE write consumer | 完整 Root 结果与 End、准确逻辑计数和产物，独立的必要 Task 完成义务 | 根 seal 不等于全部义务完成，ComputationReady 不等于已发布 |
| 原 Session / StatementPublication / Provider | 同一份 accepted prepared set、凭据、lease、publication 请求和精确结果 | Task 终态、timeout 或观察者退出不替代真实外部效果与清理事实 |

`build_attempt_schedule` 只解释完成图已经声明的 placement；`TaskManifestBinding` 从该冻结放置投影出每个原 Task。`prepare_compiled_program_with_sink_materializer` 在图建立后交出实际 root DOP；`build_logical_finish` 只构造一个拥有双输入 continuation 的 source。它们分别回答不同问题，不能合成一个随处可变的共享上下文。

## 考虑过的选项

**只凭根结果 End 或根成功就交给 publication：设计否决。** 该边界早、实现简单，但不能证明另一必要 source/writer 的独立终态，也不能代替完整 Root 数据的领域校验。反过来，仅等 Task 状态也不能证明 FE 已收到完整产物。

**从已报告的 writer/summary 或统一 metrics 反推参与者：设计否决。** 它省去显式集合，但缺失报告会使缺失的参与者从问题定义中消失，个数相同也不能证明准确来源。诊断与完成必须分别有自己的输入。

**在编译器内建 live accumulator，或先按请求 DOP 建共享状态再补实际宽度：设计否决。** 这种方式少一个装配切点，却让纯编译取得运行权限，或让不同 driver 几何共享另一套初始化/结束生命周期。纯 program 只能声明要求；实际图的 owner 才能绑定贡献者。

**完成图声明、一次放置冻结、准确 Task 绑定、同 batch 计算和领域就绪后 publication：采纳。** 静态与运行事实分别保管，通过准确身份交接；需要多一类 summary 输出和显式集合，但每个事实只有一个生产者与接受者。

**远端信用、不可变重放和跨进程可靠产物清单：待评估。** 它们能扩大容量控制或故障恢复能力，却需要各自的传输与持久化证明。此处继续消费现有单次发送与原结果运输，不能为了证明严格交付而隐含新增第二套协议。

## 裁决

1. **静态要求与实际放置分离规则。** Provider 共置、布局偏好、stage 实例放置和实例内 driver 分区分别表达。Singleton、ScanWork、Spread 与 FollowSources 只由原 schedule 对同一次 retained backend snapshot 解释，按准确 source 引用冻结；源、数据 writer 和删除 writer 可有不同宽度。零输入仍保留必要的结束参与者。没有 key 不自动变成 Singleton，资源不足不以暗降宽度恢复正确性。

2. **准确角色与原来源规则。** Provider route 与 LogicalSummary 是独立输出角色，summary 不占 writer target ordinal。FE 的 sink targets、Native outbound cuts、receiver node、partitioning、delivery 与 source set 必须来自同一完成图/descriptor；分支顺序只用于原声明的机械投影，不授予按位置猜身份的权力。Task identity 包含完整 attempt 与 backend process；同一 Task 可以承担多个角色，各角色义务分别保留。

3. **实际几何装配规则。** 先用纯 LocalProgram 与本 Task 能力建立 computing graph，再用唯一实际 root pipeline 的正 DOP 绑定 accumulator。每个实际 driver 领取一个准确 contributor，全部成功耗尽后，本 Task 的独立 summary pipeline 发出恰好一份 I/U/D；零输入也发三个零。所有 computing 和 summary drivers 由同一个 Prepared batch、取消和真实退出 owner 管理，不等 writer finalization、Task terminal 或 FE publication 才推进 summary。

4. **两层分派规则。** 同一个 canonical typed key 使用独立、冻结的 InstanceV1 与 DriverV1 hash 域，不能混入本地 Task/driver identity。条件断言只对同一 selected action 导出的 TRUE applies 判重和分派，FALSE/NULL 走原 bypass；已选中的 matched key 缺失是合同错误。断言、expand、route 和 summary 不重算 WHEN。普通写、普通全行 assertion 与 Global assertion 保持各自的显式 scope。

5. **一个 summary 输入 owner 规则。** logical finish 将 writer 子图接入原受结构水位控制的 UnionAll，直接绑定同一 receiver 的 SourceBoundSummaryInput；不能再建一个普通 ExchangeSource 竞争这个 summary receiver。finish 轮转服务 writer partial、summary 与原 aggregate/unpivot continuation；最终授权才等待两类输入与准确覆盖齐备。逻辑 I/U/D、writer 物理接收数、提交片段和统计产物分别校验，不能互相推导。

6. **严格成功与推进规则。** MustDrain 输入的准确 Consumed(final sequence) 由实际消费者覆盖数据和 End 后产生；宿主在正常成功的终态发布前核对，正常退休复用同一事实。真实取消/失败保留原结论并走原中止，不伪造 Consumed；取消终态本身不提供完整消费或提交证明。正常 drain 先记请求；任何冻结 strict 端点尚未终态时，原 Native run 继续接收状态和推进 Create、Open、split/update 与 pumps，不能仅凭根 seal 交出这个驱动者。全部 strict 尾部结束后才开始原正常关闭。普通 DemandDriven 读没有因此取得全参与者完成屏障。

7. **提议、结算与退休规则。** 失败先交原 TaskConclusionPort，交接返回不提供 verdict；真实停止后登记原 settled continuation，实际退休时重读唯一 owner 的当前结论。根 seal、summary End、Task terminal 和 actual exit 各守原事实，settled 之后的合法取消仍生效。不在执行侧加第二个结论 latch，清队列或等待结束也不充当真实请求退出。

8. **领域就绪与发布分离规则。** 原 FE write consumer 完整解释 Root 数据和 End，等待 manifest 冻结的 writer、summary source 与 root 完成义务，并保留 stream failure、原 Task cause、取消与 deadline 否决；普通写的空 summary-source 集不增加义务。随后将同一个 prepared set 交入同一个 Session union，Ready 仍保留原 credentials、lease 与代次。只有原 FE publication owner 能调用 Provider mutation；KnownCommitted、KnownUncommitted、Unknown 与后续清理不因晚到错误重新解释，也不由 ComputationReady 推断。

9. **交付、容量与兼容分离规则。** ACK 只证明接收；sender 正常收束观察自己的发送响应与真实退出，不增加远端 Consumed 等待。现有本地结构水位、查询状态收费和结果预算继续由原 owner 执行，严格消费不宣称远端接收有界。新增成功/装配语义进入原 Native exact compatibility island，能解码而不能执行同一完成语义的进程不能混用。

10. **业务计数与协议完成规则。** 成功命令通过显式中立完成值保留领域选定的 affected rows，不能把完成简化为无载荷 OK。普通写和 MOR 复用同 Session 已完整接收、只冻结一次的 Root 逻辑计数；COW 保留原实际 selection 的动作数，重写保留行不能覆盖它。CTAS 只在最终 CREATE 和必要业务步骤成功后暴露原写数，签名 NoOp 与控制命令明确给零。Unknown、未提交、必要步骤失败或缺少原计数不能退成 OK(0)。协议 owner 只按标准无符号整数编码该事实，保留原 visibility、取消、实际 flush 和退休责任；一个标量不授予新 publication、资源或协议权限。

## 接受的妥协（诚实记录）

每个 source Task 多一个 summary 输出，每个实际 computing driver 保留一次贡献结束状态；FE 和 finish 保留准确冻结集合与覆盖，规模分别随 Task/driver 数增长。它消除了逐行计数账本，但不会令跨源 metadata 为常数。

单个 Root BE 仍归约结果和产物，它是实际内存与汇聚吞吐的集中点，attempt 内没有 failover。选择它是为了复用已有数据流、bounded root 结果和 FE publication 边界，不是据此宣称最优吞吐。source/writer 的独立宽度与每 driver writer 也不提供性能收益的自动保证。

本地结构水位允许单个合法 chunk 越过水位，不是硬 RSS 上界。准确 keyed 状态沿普通查询治理，尚不提供 spill；资源不足明确失败并保留原清理责任，不把合法大输入误记为数据损坏。

远端接收仍按现有协议接收后应答；unknown transport 使本 attempt 失败，不自动续传或重新 match。准确 Consumed 的建立没有带来远端信用、可靠重放、跨 FE 重启产物清单或安全扫描回收证明。远端观察预算结束也不能放弃仍未实际退出的本地 holder。

## 何时重新评估

- 引入远端信用或不可变重放时，由原有序流/transport owner 统一接管序号、重复裁决与累计确认，保留 Consumed 覆盖最终 End 的含义，不能平行新增完成 authority。
- 若需要跨 FE/BE 故障可靠接收全部 staged 产物或精确回收缺失清单，独立评估持久清单、ACK 和外部 fencing；不能用更长 timeout 或年龄窗替代证明。
- source 数量或单 Root 汇聚成为实际资源/延迟瓶颈时，评估可续体索引、分层 finish 或新根结构，保留准确覆盖、语义计数和原 publication 权限。
- 需要新的 Provider 共置/布局或 effect 时，先扩展冻结契约及 typed equality/hash 版本，再改变执行；不能以 target ordinal、显示文本或现有分支数量猜新事实。
- 需要在当前资源范围外处理更大的 keyed/selection 工作集时，按原查询预算与真实保管边界评估分区或 spill，不能靠隐式降 DOP、重 match 或恢复旧兼容输出绕过。
