---
id: ADR-0125
title: "Query-leased catalog runtime and provider binding evidence"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: [ADR-0120]
superseded-by: null
date: 2026-08-29
provenance:
  - "discussion: 2026-08-28 query-leased catalog runtime lifecycle"
  - "implementation: local NID-1 commits pending publication"
code-anchors:
  - "novarocks/spi/src/connector/provider_binding.rs (ConnectorProviderBindingKey)"
  - "novarocks/backend/src/task_execution/context_host.rs (NativeQueryContextHost::install_catalogs)"
---

## 问题

如何让 Backend 只按查询租约中的精确 CatalogHandle 选择 connector runtime，同时保留 Frontend 与 provider 对 opaque effect artifact 所需的私有版本证据，而不把后者误当作跨进程 execution identity？

## 背景与执行事实

Backend 的 `CatalogManager` 保存由 Init 中 `CatalogSet` 安装的 runtime，并由 `QueryLifecycleRegistry::catalog_read_execution_for_query` 以 query lease 和完整 `CatalogHandle` 打开 read capability。typed scan、writer 和 terminal report 都只携带或验证这个 handle；writer protocol v2 已保留旧的 instance/incarnation 字段号与名称，防止旧 carrier 回归。

Frontend 的 effect lifecycle 另有 `ConnectorControlRuntimeId`，它是本进程可重建的 owner，绝不进入 Backend wire。provider 仍需用 instance 加 epoch 验证自己签发的 preparation、rewrite、mutation 和 durable artifact；该事实被命名为 `ConnectorProviderBindingKey` 与 `ProviderBindingEpoch`，只保留在 provider-private capability 和 FE effect validation 内。

## 考虑过的选项

1. 保留旧 `ConnectorExecutionBindingKey`，并说明它现在不再用于 Backend。改动最小，但名称继续暗示它是跨进程 runtime authority，未来很容易重新接入 fragment carrier。
2. 用 `CatalogHandle` 同时代表 BE runtime、FE effect owner 和 provider artifact epoch。wire 看似统一，但会把 FE ProcessRuntime 身份和 provider 私有 proof 暴露给 Backend，且无法表达同 catalog 的本地 control replacement。
3. 让 `CatalogHandle` 成为唯一 BE capability key，显式保留 `ConnectorControlRuntimeId` 与 provider-private binding epoch 两个不相互替代的本地事实。

## 裁决

采用选项 3，并 supersede ADR-0120 的 legacy execution-host restart reconciliation。删除 Ensure/Retire、execution declaration、generic connector read carrier、execution host 及其 generation replacement state。Backend 新查询只能在 lifecycle 准入后，以完整 query-leased `CatalogHandle` 从 `CatalogManager` 获取 runtime；旧 handle 由 query lease drain 后通过 complete reachable snapshot 回收。

`ConnectorProviderBindingKey` 不是 BE runtime key，也不编码进 native fragment 或 terminal report。它只为 provider-private opaque artifact 与 FE effect validation 保留固定字节布局；`ConnectorControlRuntimeId` 仍是 FE 本地 effect owner。三者不得互相转换或作 fallback。

## 接受的妥协（诚实记录）

同一个 catalog 同时有三类 identity，阅读代码时不能只看名称推断作用域，必须按 carrier 和 owner 判断。我们接受这一显式分层，因为把它们压成一个 key 会重新引入旧桥的 correctness authority；代价是 provider effect API 仍需要保存私有 epoch，不能完全由 `CatalogHandle` 取代。

本次不保证 Frontend 重启后继续未完成的 write、mutation 或 maintenance operation。它们仍由各自的 durable outcome/reconcile contract 决定；不能把 Backend 无 query lease 的 catalog eviction 扩展为 effect takeover。

## 何时重新评估

- 多 Frontend 或跨 Frontend failover 需要 durable effect fencing 时，单独设计 `ConnectorControlRuntimeId` 的 authority 与 handoff，不向 Backend 传递它。
- provider-private artifact 需要跨版本兼容或外部审计时，定义 provider-specific revision，而不是恢复 generic execution identity。
- CatalogManager 的 handle drain 指标显示高 churn 或 retained handle 明显占用资源时，评估更细的容量预算和 eviction observability；仍不得按 catalog name/version 建无界 metrics label。
