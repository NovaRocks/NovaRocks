---
id: ADR-0230
title: "Frontend attempt decisions use the shared runtime and original transport custody"
domain: [distributed-query-lifecycle, frontend-architecture, memory-governance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0198
    scope: "The choice to keep an asynchronous imperative shell as the Native FE attempt decision owner. The logical execution owner, business readiness, publication, recovery and actual convergence rules remain active."
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
date: 2026-11-11
related:
  - "ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md"
  - "ADR-0211-deterministic-control-runtime-and-task-ownership.md"
provenance:
  - "TST-G9b2 approved FE attempt core design, revision 11"
  - "TST-G9b2 revision 11 erratum, admission per physical send generation"
code-anchors:
  - "novarocks/attempt-protocol/src/core.rs (attempt decision owner)"
  - "novarocks/frontend-application/src/control_runtime/actor.rs (FrontendActor runtime adapter)"
  - "novarocks/frontend-application/src/query_execution/native_execution_adapter.rs (original logical drive and attempt installation)"
  - "novarocks/frontend-application/src/task_execution/host/transport/effect_source.rs (attempt effects and retirement)"
  - "novarocks/control-runtime-core/src/capacity.rs (keyed quota and exact refusal target)"
  - "novarocks/control-runtime/src/capacity_notification.rs (capacity registrants and release notifications)"
  - "novarocks/frontend-application/src/native/transport_capacity.rs (process and target transport windows)"
  - "novarocks/frontend-application/src/task_execution/host/observations.rs (ordered observations and root control)"
---

## 问题

一次 Native attempt 的任务、参与者、发送与根终态相互约束。多个异步输入同时到达时，谁决定应用顺序，谁实际持有传输与资源，逻辑执行又凭什么确认原 attempt 已退出？

## 裁决

1. **一次 attempt 只有一个协议决定者。** `novarocks-attempt-protocol` 根据冻结的拓扑、显式时间和类型化输入推进纯状态，发出 effect、控制命令和观察投影。共用控制运行时提供有限步骤、调度、因果输入、实际 Work 保管和退出。Frontend 宿主持有 I/O、codec、Native 回执、载荷与失败原件。宿主的唤醒只安排后续访问，原回执与退出各自通过其作者入账。
2. **逻辑执行与 Native attempt 保留原身份。** Query Application 继续持有逻辑执行、预算、客户端或领域就绪和重试裁决。桥沿原 activation 与原 drive 传送 Establish、根控制、结果与真实退出。Worker 继续决定 BE 本地的 Accepted、Installed、Task 与 Participation 事实。FE 只在准确 Root Installed 后开启根读取。协议终态与真实退出分别结算，后者仍等原工作、输入和宿主 Scope 实际退休。
3. **观察与根控制共享接收顺序。** 覆盖流和根控制共用一个有界入口，根控制有原预留位置。核心以已接收的前缀裁决，未就绪的控制可以继续接收促成结算的证据。有效活性仅由同一订阅代的新 Bookmark 或首个 CatchUpComplete 提供。判断失联使用同一个取样时点的经过时间、实际背压暂停和入口内证据。延迟或旧代确认只结算其原请求。
4. **进程级与键控配额在同一控制账本申领。** 键控维度族为每键配置同一组上限，活跃键的占用全部归零时退出账本。每次申领同时核对进程维度与至多一个键，按进程优先报告准确不足项。拒绝与等待登记同一事务完成，原释放在锁外唤醒对应登记。唤醒后必须以原请求重新申领。以上扩展继续遵守 ADR-0211 的原 Scope、Work 与报告规则。
5. **传输窗口和联合准入分别由原作者发放。** FE 控制账本以进程和准确 `BackendProcessId` 约束物理传输的条数、保留字节、队列字节及编码，普通工作留出控制推进所需份额。Workload Control 执行排队输入和实际派发的联合准入，并负责责任报价与真实归还。宿主在物理报价受阻时归还临时位置，沿原公平顺序重新观察。它在实际 I/O 与 captures 退出后归还对应的队列与编码预留，再登记该次输入退出队列。回执和原载荷继续由其实际 holder 保管。
6. **一次物理发送对应一个准入代次。** 同一逻辑操作的重放继续使用原标识、冻结请求和原载荷，每个物理代次重新申请一次 `AdmitQueuedInput`。冻结后由首份授予派生的保留钉保管原载荷，后续代次只报价自己的头部。旧代次的结果未知不改变已排队的新代次。任一代次的确定回执结清操作并撤回尚未开始的新代次。一个代次的确定未发送只结清该代；任何已开始发送保留自己的实际回执、退出和未知结果。

## 接受的取舍

核心只拥有有界的决定和事实引用。实际工作需要经过宿主、原报告与原输入回到核心，多了一次因果交接，也使测试能分别验证决定、实际开始和真实退出。进程关闭继续保管退出未证实的原义务。跨代重放、保留钉和控制预留提高了保管结构的复杂度；它们保留既有 Worker/WC 契约，并使未开始的重放可撤回。组件、原生场景和全工作区验收分别以各自程序及源码回执取证。
