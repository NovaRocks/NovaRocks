---
id: ADR-0022
title: "Connector statistics capability"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-07-31
provenance:
  - "discussion: unified statistics architecture"
code-anchors:
  - "novarocks/spi/src/connector/statistics.rs (FE-only statistics contract and lease)"
  - "novarocks/frontend/src/connector/control_host.rs (generation-fenced statistics lease)"
---

## 问题

如何让 optimizer read 与 ANALYZE collection/publish 使用同一个 Connector-native statistics authority，同时不把该控制能力泄漏给 BE？

## 背景与执行事实

Connector control binding 已将 FE metadata、planning、distribution 与可选 catalog mutation 组合为 generation-fenced capability；BE 只接收有限的 execution declaration。statistics 的 data version 不能复用 `ConnectorTableMetadata::version`，因为 provider 可将后者用于 schema-level identity。外部 artifact publish 也会出现已提交、未提交和未知提交三态。

## 考虑过的选项

1. 让 Core 直接持有 provider client 和 statistics cache。实现短，但破坏 FE control ownership，且无法在 connector generation 退役时安全围栏。
2. 在 BE execution binding 增加 statistics provider。能贴近 scan，但会把 catalog artifact 和 publication capability 扩散到数据面。
3. 在 FE control binding 增加可选、provider-neutral statistics capability，并由独立 lease 保持 generation 存活。

## 裁决

采用选项 3。`ConnectorStatistics` 是 FE-only optional capability：所有 binding 都可没有它；reader 支持 immutable evidence read，collection half 可选地准备、publish、reconcile ANALYZE evidence。`ConnectorStatisticsLease` 同时校验 descriptor/incarnation 与 request/evidence owner，host 在 planning、mutation、write、statistics 四类 lease 都释放前不得退休 generation。BE `ConnectorExecutionBinding` 保持不变。

data-version、evidence-revision、provider plan/result/receipt 均为有界 opaque bytes；metric selection 使用 typed stable fields。statistics publish 复用 ADR-0017 的 `ExternalMutationOutcome` 和 `ExternalMutationEvidence`，unknown 结果必须 authoritative reconcile，不能 blind replay。

## 接受的妥协（诚实记录）

SPI 暂不规定 Core 如何将 collection plan 变为 distributed fragments，也不规定 provider 的 Puffin/manifest layout；这些属于 consumer/provider implementation。collection plan/result 只提供有界 provider-neutral handoff，避免在 SPI 固化 Iceberg 或 runtime object。

## 何时重新评估

- statistics collection 需要跨 provider 的 wire codec 或 BE-side bounded declaration；
- data-version token 的 64 KiB 上限不足以承载一个真实、可验证的 provider identity；
- 新 provider 不能以 manifest/artifact 或 visible-row scan 表达 collection 与 reconcile。
