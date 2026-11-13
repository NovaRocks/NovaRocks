---
id: ADR-0111
title: "Frontend maintenance and statistics jobs are process runtime; GC observation is an accelerator"
domain: [table-maintenance, provider-spi]
status: active
supersedes: [ADR-0065, ADR-0067, ADR-0084]
superseded-by: null
partially-superseded-by:
  - id: ADR-0231
    scope: "Maintenance/statistics job queues, workers, histories and observer-only cancellation in decisions 1 and 3; crash-only publication, activity gates and GC Accelerator remain"
date: 2026-08-26
provenance:
  - "discussion: 2026-08-26 frontend runtime-state owner cut"
code-anchors:
  - "novarocks/table-maintenance/src/gc_observation.rs (GcOwnedRefObservationAccelerator)"
  - "novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs (DirectRewriteAction)"
  - "novarocks/frontend-application/src/statistics/attempt_executor/statement.rs (StatementStatisticsAction)"
---

## 问题

Frontend 的 maintenance 与 statistics job 能否把进程内 attempt、worker lease、retry、recovery 和历史
operation 记录继续作为 StateStore durable authority；若不能，什么状态可以为了 owned-ref GC 的安全年龄窗
保留跨 restart 的时间证据？

## 背景与执行事实

表维护与统计的外部业务真相已经由 Connector 和 Catalog publication frontier 拥有。一次 current attempt
可以通过 exact metadata observation 捕获目标、在 provider 侧准备并执行工作，并以
`KnownUncommitted`、`KnownCommitted` 或 `CommitUnknown` 取得 publication outcome。Frontend job、worker
lease、queue、checkpoint、retry deadline、operation payload 和 terminal history 只描述该进程如何驱动这一次
attempt；它们不是 lake 上可见的业务事实。

把这些运行账持久化会制造第二条 effect-capable authority：新进程能够 claim、takeover、reconcile 或解释旧
attempt 的未知响应。即使它不重放普通工作，historical maintenance recovery 也会让新 generation 依据旧
evidence继续 mutation。该路径与 ADR-0110 的 crash-only publication contract 不兼容：`CommitUnknown` 的
正确后果是停止 mutation，而不是为旧 attempt 寻找自动出口。

owned-ref GC 的 first-observation 不同。它只记当前 provider 已证明的 exact
`(table UUID, ref name, head snapshot, provenance version, provenance digest)` 第一次被观察的时间；丢失它
只会延迟删除，不会改变 lake 可见状态。它既不拥有 ref，也不拥有 snapshot publication 或 delete decision。
因此它可以作为可 wipe、可重建、fail-closed 的 Accelerator 保留，但不能与 job ledger 混为同一 StateStore
owner。

## 考虑过的选项

1. **保留 durable job/lease/recovery，并补强 fence。** 这保留跨 restart 的 SHOW、cancel 和进度，但仍让
   StateStore record 决定新进程是否可以继续 effect-capable attempt；更强的 lease 不会把运行账变成业务真相。
2. **把所有 maintenance/statistics 状态都改成 ProcessRuntime，并连同 GC observation 删除。** 这彻底消除
   durable runtime，但 owned-ref 删除失去跨 restart 的完整安全年龄证明，只能依赖内存时间或提前删除风险。
3. **maintenance/statistics job 为 ProcessRuntime；GC first-observation 单独为 Accelerator**（采用）。
   普通 job 重启即丢弃，fresh intent 只从 Catalog current state 规划；GC record 只保留安全时间证据，并在
   clone、format unknown、corruption 或 exact tuple变化时 reset。
4. **把 durable history 移到外部 managed service 后继续驱动恢复。** 这只是把第二 authority 换了载体；外部
   history 可用于诊断和告警，不能反向授权 retry、takeover、reconcile 或 publication。

## 裁决

选择选项 3。

1. maintenance v1-v4 job/operation/payload/attempt/state/transaction/index 与 statistics job/worker lease/
   retry/cursor 都是 `ProcessRuntime`。当前 Frontend 可以保留有界 queue、active job、recent terminal
   observation、cancel source 与 worker；这些对象不得有 persistent codec、StateStore repository、startup
   decoder、clone policy、takeover API 或跨 restart identity。
2. maintenance 在一个 FE 内以 process-local per-table activity gate 避免 family 间并发消费失效 base facts。
   它不是跨 FE fence；两个 FE 或外部 writer 的正确性只由 provider 在 Catalog frontier 的 exact
   base-state/OCC 决定。
3. observer cancellation、client disconnect 或 wait deadline 只 detach observer。显式 cancel、Frontend shutdown
   或 attempt deadline 阻止尚未 dispatch 的工作；已经可能 dispatch 的工作必须以实际三态 outcome 分类，不得
   伪称 rollback。
4. `CommitUnknown` 后停止一切自动 mutation：不得 retry、abort、cleanup、roll-forward、exact reconcile 或
   historical recovery。Connector 保留 current-attempt prepare/execute/publish 和 Catalog-owned outcome；不再
   暴露仅为旧 job recovery 服务的 maintenance/statistics reconcile 或 historical maintenance capability。
5. GC first-observation 是唯一此处保留的 `Accelerator`。它只在可识别 current format 且 exact tuple再次匹配、
   elapsed time严格超过安全年龄窗时成熟。缺失、tuple变化、corruption、unknown version、clock/store错误与 clone
   都必须 fail closed；可安全的退化只有延迟删除。clone target 在启用前必须 wipe 该 family，不能继承 source
   maturity。

ADR-0110 继续拥有 publication outcome、target OCC 与安全年龄窗的上位契约。ADR-0085 继续拥有
physical object capture/rebind；statistics 仍在每个 current attempt 使用它，但不再把 target binding 写成
可恢复 job record。

## 接受的妥协（诚实记录）

重启后用户失去旧 maintenance/statistics job 的 SHOW、cancel、进度与 retry 能力；长任务崩溃后必须从 Catalog
current state 重新规划，可能重复计算甚至放弃一次已未知的外部请求。选择这一成本不是因为 ProcessRuntime 更易
实现，而是因为保留 durable runtime 会继续授予新进程解释并推进旧 effect-capable attempt 的权力。

同一个 table 上的多个 FE 可能重复执行 maintenance 或 ANALYZE，造成计算浪费。我们接受这个可用性/成本代价，
因为把 StateStore lease 重新包装为共享 scheduler authority 会重新引入被裁决移除的正确性路径；未来的 designation、
sharding 和成本控制必须建立在不改变 Catalog OCC authority 的独立机制上。

GC reset 和 clone wipe 会额外保留垃圾至少一个完整安全年龄窗。我们接受存储成本，而不是让未知或跨 deployment
的时间证据提前授权 destructive delete。

## 何时重新评估

- 如果产品需要跨 FE/跨 restart 的 job history、cancel 或进度，必须先定义一个只读 observability contract，
  并证明它不能反向驱动 publication、retry、takeover 或 recovery。
- 如果 Catalog/provider 不能把 current attempt 的外部结果可靠地表达为三态 outcome，不能以 durable reconcile
  绕过；应先扩展 ADR-0110 下的 typed publication contract。
- 如果多 FE 的重复工作成为可量化成本瓶颈，应提出独立的 designation/sharding 设计，并证明它不是 publication
  correctness authority。
- 如果 GC record 的安全年龄需要跨 deployment 复制，必须先定义显式 clone protocol 与 source identity；在此之前
  clone wipe 继续是唯一安全策略。
