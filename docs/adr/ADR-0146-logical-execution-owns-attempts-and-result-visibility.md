---
id: ADR-0146
title: "Logical execution owns attempts, result visibility and residual convergence"
domain: [distributed-query-lifecycle, distributed-execution]
status: active
supersedes: [ADR-0135]
superseded-by: null
date: 2026-09-11
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-11 logical execution recovery, delivery and worker convergence"
code-anchors:
  - "novarocks/query-application/src/coordination/actor.rs (logical execution actor)"
  - "novarocks/query-application/src/coordination/supervisor.rs (LogicalExecutionSupervisor)"
  - "novarocks/query-application/src/coordination/runtime_registry.rs (LogicalExecutionRuntimeRegistry)"
  - "novarocks/query-application/src/coordination/replacement.rs (replacement qualification)"
  - "novarocks/query-application/src/api/result.rs (bounded result delivery)"
  - "novarocks/frontend/src/task_execution/manifest_round.rs (ManifestAssembledRound)"
  - "novarocks/frontend/src/query_execution/native_execution_adapter.rs (Native logical execution adapter)"
  - "novarocks/backend/src/task_execution/registry.rs (TaskExecutionRegistry)"
---

## 问题

一次面向用户的逻辑执行、它的多个物理 attempts、已经可见的结果以及失败 attempt 的残余责任，应由谁统一裁决，才能在 Worker 失败时安全恢复而不重复结果、不复活旧 Task、也不把资源收敛误当成业务结论？

## 背景与执行事实

一次逻辑执行固定用户请求、输入绑定、输出契约和恢复政策；一个 attempt 则固定 `QueryExecutionId`、参与 Worker、Task/Context identity、placement、运行能力与结果序列。Worker 或 transport 失败可能结束一个 attempt，却不必结束逻辑执行。相反，一旦结果已经对客户端可见，即使输入 snapshot 不变，整体重启也可能重复已经交付的行，因此“是否可替换 attempt”必须与“是否开始交付”在同一串行裁决点决定。

Query Application 为每个逻辑执行运行一个 async actor，并由进程级 `LogicalExecutionSupervisor` 与 registry 持有 actor、attempt owner、结果出口和 join/retirement 责任。actor 独占以下可变状态：当前获准推进的 attempt、旧 residual attempts、恢复预算、结果 visibility、业务结论、取消和资源 obligation。Native adapter 只能把冻结 manifest 实例化为 dormant/active attempt owner，不能改变恢复政策或绕过 registry。

Task 仍是一次 attempt 内可独立创建、推进与终结的单位。ADR-0135 中以下协议规则继续成立，并由本 ADR 重述为逻辑执行的下层不变量：

- `TaskIdentity` 是 `{QueryExecutionId, StageId, TaskId, BackendProcessId}` 的不可分割整体；共享 Context 用 `{QueryExecutionId, FrontendProcessId, BackendProcessId}`。任何部分不等都拒绝，endpoint 不替代 process identity。
- Create/Update/Abort/Release 等 typed operation 各自携带 identity、deadline 和独立 operation id；每个 domain 只单调推进，accepted/idempotent/older 与 conflict/rejected 不折叠。receipt 只陈述被接受的事实，不成为第二条 TaskStatus authority。
- `TaskStatus` 是 Task 生命周期和终态的唯一发布者；状态订阅按准确 cursor/version 前进。`TaskInfo` 只做有界诊断，transport keepalive、heartbeat 或 Backend membership 不证明 coordinator/Task 存活。
- Query execution lease 与状态观察分离；只有准确 Context owner 续租。租约失效、状态流错误、控制 receipt 和精确 process replacement 按各自契约产生事实，互不伪造。
- push exchange edge 初始关闭，只有所有冻结 destination 的 Create ACK 都证明 receiver/capability 已安装后才开放。入站 frame 在 Arrow decode 和 receiver allocation 前按完整 Task/edge/sender/process identity 准入。
- split、Runtime Filter、credential、edge 与 Context 各自按独立 token/watermark 推进；同 token 的 replay 幂等，冲突显式失败。消费者已正常结束后的迟到投递是 no-op settlement；发送方在自己已经封口后继续产生内容仍是错误。
- end-of-stream 代表一个 sender 的完整 driver 集合；持有未交付行的 driver 不得让另一个 driver 提前封口。正常下游结束可以关闭边，不能写成上游 failure。
- 显式 `ReleaseQueryContext` 才闭合 Context；“当前看到的 Task 全部终态”不能推导未来没有合法 Create。retirement fence/tombstone 至少覆盖合法请求 horizon，过期请求不得复活历史 identity。
- descriptor 冻结完整协议事实；plan body 可以由 codec 私有 handle 持有其唯一 wire 表示，但 FE/BE application owner 不直接解释 generated message。不会为形式纯洁再建立一套完整 plan IR。

结果是独立的数据面。root Fetch 使用准确 attempt、连续 sequence 与 ACK watermark；Backend 在准确 ACK 前保留 payload。FE 在 Fetch、decode、queued delivery 和 protocol write 之间转移同一份 byte credit，不能出现未计账空档。schema 只交付一次；batch/EOF 使用 move-only delivery 与 `Completed`、`Failed`、`Dropped` receipt。只有协议 adapter 确认完整接受后才能释放对应信用和推进可见状态，失败流不能生成成功 EOF。

旧 attempt 的业务失败、物理停止、输出撤销与资源释放是不同事实。只读恢复的 correctness 来自 identity isolation、旧 eligibility 撤销和新 attempt 的实际准入，而不是要求失联 Worker 发出不可能到达的停止 ACK。旧 residual 在后台继续持有 last-known/current-unknown 计账，直至收到 Worker 实际停止且 Context 已围栏，或观察到同 endpoint 的精确 process replacement；`Unobservable` 本身不代表停止。

## 考虑过的选项

**选项一：每个业务调用者自行循环创建和重试 attempts。** SQL、MV、Statistics 和 Maintenance 可以分别选择策略，但它们会复制拓扑等待、失败分类、退避、结果边界和 residual 回收；同一 Task 协议会得到多套上层状态机。这是**设计否决**。

**选项二：协调器只拥有一个 attempt，任何失败都返回业务层。** 它简单且不会重复结果，但失联 Worker、瞬时 transport 和可替换 placement 都无法在保持同一语义描述时恢复；业务层又必须理解底层 Task identity 才能重试。这是**成本否决**：对有外部效果或无法隔离输出的执行仍然是正确策略，但不能作为所有执行的唯一模型。

**选项三：逻辑执行 actor 统一拥有 attempts、visibility 和 residual。** 业务选择一个封闭恢复策略；actor 在策略内串行裁决 attempt replacement 与首次交付，Supervisor/Registry 保留进程级 owner，Worker/Native 只报告自己能证明的事实。采纳。

**选项四：为了透明恢复，先完整物化所有结果。** 这样可把整个 result 当成一次原子发布，但会增加首行延迟、存储和 I/O，并使短查询承担不必要成本。这是**成本否决**；需要 fault-tolerant exchange 或可恢复结果缓存时可以作为新的 execution mode 重新评估。

**选项五：新 attempt 必须等待所有旧 Worker 证明实际停止。** 它能减少同时占用资源，却使永久失联的 Worker 永远阻塞恢复；process loss 正是恢复要处理的主要场景。identity fence 与容量计账已经保护正确性和资源政策，因此把停止证明设为只读恢复硬门是**设计否决**。有外部效果的执行仍可要求更强产物隔离或停止证明。

## 裁决

1. **Logical-owner rule。** 逻辑执行拥有其全部 attempts、恢复预算、结果 visibility、业务结论和 residual obligations。业务调用者只选择封闭恢复模式，不填写开放式恢复语言。
2. **Single-progressing-attempt rule。** 同一逻辑执行任一时刻最多一个 attempt 获准继续调度、产生可交付输出和接受新控制更新。旧 attempt 可以物理残留，但其 eligibility、路由和交付能力已经撤销。
3. **Pre-visibility recovery rule。** 首期恢复模式只有 `NoRecovery` 与 `RestartAttemptBeforeVisibility`。只有无外部效果、描述可重放、结果尚未可见、identity/data-plane 隔离成立、可达旧 Context 已封闭新增工作且新 Worker 完成实际容量准入时，才能激活 successor。
4. **Visibility-seal rule。** 首个 schema/batch 的协议接受与 replacement 决定在同一 actor 中线性化。任何 partial protocol write 后都不可透明整体重启；固定 snapshot 和 RPC 幂等不能消除重复行。
5. **Attempt-isolation rule。** Task、Context、Exchange、Fetch、Runtime Filter、credential 和 split 的每条路径都按准确 attempt/process identity 准入。retirement 后旧授权不可因 tombstone 淘汰而复活。
6. **Residual-convergence rule。** 旧 attempt 的业务结论不释放 residual responsibility。`Unobservable` 和观察错误只表示缺少正面事实，继续按 last-known/current-unknown 计账；只有 Worker 停止且 Context 围栏，或精确 process replacement，才能闭合对应 residual。
7. **Result-credit rule。** Fetch 前预留信用；encoded、in-flight、decoded、queued 和 protocol-writing bytes 只做所有权转移，不重复计账也不出现空档。每个 batch/EOF 的 receipt 在完整接受、显式失败或 Drop 中恰好终结一次。
8. **Task-authority rule。** Task protocol继续遵守本 ADR 背景列出的 exact identity、per-domain monotonic verdict、single TaskStatus authority、edge-open barrier、lease separation、watermark、EOS 和 explicit release 规则。上层恢复不得产生第二套 Task lifecycle truth。
9. **Event-driven rule。** actor、Supervisor、result pump 和 Task supervision 通过事件、watch、bounded mailbox、deadline index 与 async I/O 推进；等待查询、等待容量或等待 Task 不得各自永久占有 OS thread。
10. **Owner-retirement rule。** Registry 先原子安装 actor owner、attempt permit、output transfer 和 join responsibility，再暴露执行 handle。shutdown 关闭新 reservation，等待 actor join 与 exact retirement receipt；deadline/cancel 不 detach owner，不以 abort 伪造远端收敛。

本 ADR 完整 supersede ADR-0135。ADR-0135 对单 attempt Task protocol 的有效裁决已在背景和第 8 条中重述；被替代的是“query attempt 即最高层执行 owner”以及其旧的 Backend-loss/结果交付边界。更早由 ADR-0135 supersede 的 ADR-0008 和旧 participant-role ADR 仍通过该历史链保留。

## 接受的妥协（诚实记录）

直接流式交付降低首行延迟并保持有界内存，但一旦有字节对客户端可见，就放弃透明整体恢复。客户端可能拿到 partial result 后收到错误；协议 adapter 必须忠实结束失败流，不能用成功 EOF 掩盖它。

只读 successor 可以在失联旧 Worker 尚未停止时启动，因此同一逻辑执行可能短期占用新旧两份物理资源。旧占用只能用 last-known/current-unknown 保守计账，不能形成严格的远端容量保留。我们接受吞吐下降或准入等待，以避免把永久失联变成永久不可恢复。

事件驱动模型仍为每个逻辑执行保留一个 async actor 和有界状态；它消除的是每查询/每 Task 专用 OS thread，不是零调度成本。Mailbox、result bytes、residual count 和 deadline entries 都必须有进程级上限。

Task descriptor 继续通过 codec 私有 handle 持有 generated plan wire，而不是新增完全中立的第四套 plan IR。这个边界不够形式纯粹，但避免多套 plan 表示长期并存；application owner 不得因此直接读取或构造 wire message。

当前恢复只在同一 FE 进程内生效，也只覆盖无外部效果且可重放的执行。有提交、发布或不可撤销外部效果的工作默认 `NoRecovery`；本 ADR 没有解决多 FE takeover、持久 execution journal 或 exactly-once result replay。

## 何时重新评估

1. 需要在结果可见后继续透明恢复时，必须引入可证明的 result/exchange materialization、去重 token 或客户端 resume protocol，并重新定义 visibility seal 和存储成本。
2. 写入、发布或维护需要 attempt replacement 时，先定义 effect artifact isolation、commit truth 和业务授权；不能复用只读模式直接开放。
3. 多 FE、FE 重启恢复或协调服务迁移成为需求时，需要持久化 logical execution、attempt ledger、delivery watermark 和 owner fence；当前进程内 registry 不足。
4. residual unknown 在真实故障中持续占满资源预算时，评估 Worker process epoch、外部进程终止证明或更强容量衰减策略；不得把超时本身伪装成 stopped。
5. 直接流在短查询之外造成不可接受的 partial-result 失败率时，评估按查询类别选择 materialized execution mode，而不是让全部查询无条件落盘。
6. Task protocol的 identity、verdict、edge、watermark 或显式 release 规则需要改变时，必须在新的 ADR 中完整 supersede 本篇相关规则，避免逻辑恢复与单 attempt authority 分叉。
