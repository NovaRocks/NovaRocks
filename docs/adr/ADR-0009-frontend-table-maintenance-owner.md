---
id: ADR-0009
title: "Frontend owns table maintenance application lifecycle"
domain: [table-maintenance]
status: superseded
supersedes: []
superseded-by: ADR-0083
date: 2026-07-28
provenance:
  - "discussion: 2026-07-28 table maintenance ownership extraction"
code-anchors:
  - "novarocks/frontend/src/table_maintenance/mod.rs (FrontendTableMaintenanceService)"
---

## 问题

Iceberg 表维护的 SQL 路由、异步 optimize job、后台 worker 与关闭顺序应由哪个组件拥有，core、frontend 与 connector 之间又应以什么边界协作？

## 背景与执行事实

表维护同时包含三类不同性质的事实：

- SQL 语句识别、用户与自动调用的策略差异、job 入队、worker 启停和 host 关闭顺序属于 application/lifecycle 编排；
- 表名解析后的 catalog、live snapshot、文件集合、Iceberg commit、abort cleanup 与 cache invalidation 属于执行引擎和外部系统事实；
- optimize job 的状态转换必须跨 FE 重启持久化，不能依赖进程内队列，也不能让同一 job 同时存在两套 durable truth。

Frontend 已经是 standalone application host，拥有 StateStore 与 service 生命周期，因此它应拥有表维护 application/lifecycle。Core 仍然掌握执行引擎能力与 connector 入口，但只通过 `TableMaintenanceEngine` 向 frontend 提供窄能力；frontend 通过 `TableMaintenanceService` 被 core host 显式注入。Frontend 不获得 `StandaloneState`、connector registry、Iceberg catalog handle 或 object-store handle。

这对 contract 是一个 consumer-owned、一对一 domain port，不是可枚举或可替换 provider 的 SPI。它不进入统一 provider SPI，也不允许通过 service locator 在运行时查找实现。

## 考虑过的选项

1. **Frontend 拥有 application/lifecycle，core 暴露窄 engine port，connector 保留外部事实。** SQL route、job repository、worker 与 host shutdown 都归 frontend；typed request/outcome 穿过 port；Iceberg catalog、snapshot、file、commit 与 cache truth 留在 core/connector。
2. **只把现有 wrapper 搬到 frontend。** 改动小，但 parser、job repository、worker 或 lifecycle 仍留在 core，会留下两个 application owner，依赖方向和关闭顺序都没有真正收敛。
3. **把 connector executor 整体搬进 frontend。** 表面上 owner 集中，但会迫使 frontend 依赖 Iceberg catalog、object store、query execution 和 connector 私有类型，反而扩大边界并复制 external-system truth。
4. **抽象为通用 workflow engine。** 可以统一建模 job，但当前只有表维护的具体 lifecycle，通用状态机、插件注册与调度语义没有第二个已证实的消费者；现在引入会把领域语义稀释成过早抽象。

## 裁决

选择第一项：

- Frontend 是 table-maintenance application/lifecycle 的唯一 owner，负责 SQL dispatch、typed validation、optimize job repository、worker、startup reconcile 与 shutdown；
- Core 只公开 typed `TableMaintenanceEngine` / `TableMaintenanceService` dependency-inversion contract，以及每个 action 独立字段的 request/outcome DTO；
- Connector 继续拥有 external system truth，包括 live Iceberg metadata、file rewrite、commit、cleanup 与 cache invalidation；
- StateStore 是 optimize job 的唯一 durable store；不保留 MetaStore dual-write、同步 bridge 或内存 durable fallback；
- host 使用显式构造注入，不增加 SPI 注册、service locator 或动态 provider 发现。

首阶段只承诺单 FE claim/reconcile：启动时处理遗留 running job，再继续 pending job。它不声称 active-active FE 安全，也不包含 lease、attempt ownership、fence、takeover 或 destructive-action guard。多 FE 协调必须作为后继架构决策，在最终 owner 和 durable record 上增加这些语义，而不能把它们提前塞进本次抽取。

## 接受的妥协（诚实记录）

选择一对一 domain port 会在 core 与 frontend 之间增加一组 DTO 映射，并使同步 trait 方法承担 frontend 到 engine 的显式转发；这比直接传递 `StandaloneState` 或 connector handle 多一些样板代码。接受该成本不是因为 DTO 转换本身更优雅，而是因为它把 application owner、external truth 和依赖方向固定成可审查边界，并避免未来继续从 frontend 穿透 core 私有状态。

本阶段保留单 FE 恢复语义，意味着 FE 进程并发运行或 ownership 模糊时没有 lease/fence 保护。这样选择是为了忠实完成 owner 迁移并控制一次变更的风险，不代表单 FE 模型足以支撑高可用部署；在后继多 FE 工作完成前，部署和运维必须遵守这一边界。

StateStore 成为唯一 durable store 后，未配置 StateStore 的环境不能提交或查询 optimize job，但仍可执行无需 job lifecycle 的直接 maintenance action。这会使部分轻量测试配置需要显式增加 SQLite StateStore；该配置成本优于悄悄使用内存队列或维持双写。

## 何时重新评估

- 需要两个或更多 FE 同时 claim maintenance job，或需要 FE failure 后安全 takeover 时；
- 外部 action 具备不可安全重放的破坏性候选集，需要 attempt ownership、lease、fence 与 authoritative reconcile 时；
- 出现第二个具有相同 durable lifecycle、失败恢复和调度语义的 application domain，足以用真实重复证明通用 workflow abstraction 的价值时；
- connector truth 必须跨进程远程执行，当前同步 engine port 不再能表达 transport、idempotency 或 backpressure 契约时；
- StateStore 的事务或可用性指标无法满足 optimize job 的 durable lifecycle 要求，需要重新选择唯一 durable provider 时。
