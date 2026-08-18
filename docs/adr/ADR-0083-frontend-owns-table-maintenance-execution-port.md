---
id: ADR-0083
title: "Frontend owns the table maintenance execution port"
domain: [table-maintenance]
status: active
supersedes: [ADR-0009]
superseded-by: null
date: 2026-08-17
provenance:
  - "discussion: 2026-08-17 table-maintenance query-assembly owner cut"
code-anchors:
  - "novarocks/frontend/src/table_maintenance/mod.rs (FrontendTableMaintenanceService)"
---

## 问题

当表维护执行会冻结 native fragment、query execution context 与 connector-write registration 时，
`TableMaintenanceEngine` / `TableMaintenanceService` 能否继续作为聚合 core 暴露给 Frontend 的 port，
同时又让 query assembly 完整归 Frontend？

## 背景与执行事实

ADR-0009 正确地把表维护的 durable application lifecycle 归给 Frontend，但保留了 Core 定义的
`TableMaintenanceEngine` / `TableMaintenanceService`。后续分布式 rewrite 路径使这个 port 不再是中立
connector 事实：`prepare_distributed_rewrite_cohort` 返回的 cohort 持有 native fragment encoding input、
query execution service、admitted execution context 与 sealed connector-write registration。

这些对象都是 Frontend query assembly 的真实产物。若 trait 留在 Core，Core 必须命名 Frontend assembly
类型；若把 cohort 重新包装成 Core 值或 factory，则会引入仅为跨 crate 转发而存在的 ABI / facade，重新制造
双 owner 或隐藏 bridge。

## 考虑过的选项

1. 保留 Core port，并把 cohort 改为中立 Core carrier。
2. 保留 Core port，并提供 Frontend factory 或 callback 来组装 cohort。
3. 将 table-maintenance execution port 及其实现与 query assembly 一并归 Frontend；Core 仅保留中立 DTO 与 connector facts。

## 裁决

选择第三项：

- Frontend 同时拥有表维护 application lifecycle **和** execution port，包括 `TableMaintenanceEngine`、
  `TableMaintenanceService`、foreground / background engine、attempt、frozen rewrite cohort 与维护 command；
- 这些实现与 `MaintenanceExecutionKernel`、native fragment assembly 和 concrete connector sealing binding 一起
  原子迁入 Frontend；不保留 Core facade、re-export 或 factory bridge；
- Core 只保留 `MaintenanceTarget` 与 connector external-system facts；其余维护请求、结果和 attempt 值均属于
  Frontend execution port，并且不得让 Core 命名 Frontend query assembly 类型；
- Frontend MV background runtime 直接消费 Frontend maintenance port，而不是经 Core 回调。

这保留了 ADR-0009 关于唯一 durable owner、StateStore 作为唯一 durable truth，以及 connector 仍拥有外部
catalog / snapshot / file / commit 事实的结论；被取代的仅是“Core 持有 execution port”的实现边界。

## 接受的妥协（诚实记录）

Frontend 将直接持有更多 connector-facing typed DTO，并在模块内组合维护 engine；这比一个位于 Core 的窄 trait
少了一层显式依赖反转。接受这个成本不是因为 Frontend 更适合保存 connector truth，而是因为该 trait 的返回值
已经是 Frontend assembly artifact，继续把类型名放在 Core 只能用 facade 掩盖真实 owner。Connector 仍保留事实与
动作能力，Frontend 不获得 provider 的全局状态或 object-store 句柄。

这次切换与 query assembly 同一原子迁移，改动面较大；为避免双权威，不能分阶段保留两个 engine 实现。验证必须
覆盖 foreground SQL、durable optimize worker、recovery 与 distributed rewrite 的 native submission，而不能只
依赖单进程 mock。

## 何时重新评估

- 表维护需要由独立进程或远程服务执行，且必须以稳定 wire 而非进程内 typed 调用表达；
- 出现第二个独立 domain 真实需要同一套 maintenance execution lifecycle，足以证明共享 contract 而非
  Frontend-local owner 的价值；
- connector external action 需要新的跨进程 idempotency / backpressure contract，且现有 typed DTO 不能安全表达。
