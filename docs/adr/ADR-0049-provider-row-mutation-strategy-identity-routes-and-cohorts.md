---
id: ADR-0049
title: "Provider owns row-mutation strategy, identity, routes and cohorts"
domain: [provider-spi, frontend-dml]
status: active
supersedes: [ADR-0045]
superseded-by: null
date: 2026-08-10
provenance:
  - "discussion: 2026-08-09 provider-owned row-mutation boundary"
  - "implementation: provider-owned row-mutation planning and activation"
code-anchors:
  - "novarocks/spi/src/connector/row_mutation.rs (ConnectorRowMutationPreparation)"
  - "novarocks/connector/iceberg/src/commit/row_mutation_preparation.rs (prepare_row_mutation)"
---

## 问题

行级 DELETE、UPDATE 与 MERGE 如何在保持 Frontend 单一 durable operation 和 aggregate commit 的同时，不让 SQL、Core 或 Frontend 推断表格式的 identity、物理写入策略、路由及 cohort？

## 背景与执行事实

Iceberg 的 position delete、deletion vector、merge-on-read、copy-on-write 与 equality delete 取决于 provider 所拥有的 snapshot、ref、format version、existing deletes、文件布局和 property。旧 change-stream topology 将 `DeleteDv`、`ReuseData` 与 `FreshData` 作为跨 owner 的物理事实，迫使 SQL 选择 Iceberg 策略，也使 generic mutation flow 读取 `_file`、`_pos` 等 provider identity。

Frontend 已由 ADR-0033 固定 statement admission、durable intent 与单一 terminal lifecycle；ADR-0023 固定 operation/cohort/execution/writer 的 aggregate external commit；ADR-0048 固定 exact write lease 和 Provider-signed write authority。这些边界仍成立。

## 考虑过的选项

1. 保留 SQL 的物理 branch enum，让 provider 只在 sink 时翻译。改动较小，但 SQL 和 wire 继续承认 Iceberg 策略，新增 provider 无法替换这些事实。
2. 新增独立 row-DML capability。它能局部隐藏 Iceberg payload，却会复制 write lease、activation 和 terminal ownership，形成第二套 DML 生命周期。
3. 在既有 `ConnectorWriteControl` 的 exact lease 上，由 provider 签发 row-mutation preparation、opaque routes 和 sealed cohorts；SQL 只产生逻辑 effect 与 token-bound values。

## 裁决

采用选项 3。Provider 拥有 row identity、match contract、strategy、route id、cohort、old/new-file ownership、commit/abort/reconcile evidence；这些事实仅以 sealed SPI payload 与 opaque id 离开 provider。SQL 仅产生 `Delete`、`Replace`、`Insert` logical effect 和 admission 绑定的 before/after/input projection；Core 仅执行 token、digest、budget 与 route/effect contract。

direct strategy 可将同一个 `Replace` fanout 到多个接受该 effect 的 opaque routes。COW 的 bounded matched selection 在同一 exact lease 下交给 provider，provider 完整验证 contexts 后一次注册 operation service。Frontend 在 match 或 staging 前持久化同一个 operation intent，保留一个 terminal decision、aggregate commit 和至多一个 snapshot。

## 接受的妥协（诚实记录）

SPI 现在显式表达 row-mutation strategy、selection 和 cohort recipe，接口比旧 change-stream layout 更大。它是为了将真实 provider facts 从 generic 层移走，而不是承诺所有 Connector 都实现所有策略。COW 选择暂存为受 row/byte budget 限制的 `Vec<RecordBatch>`，不引入新的 spill 协议；超限显式失败。

## 何时重新评估

当多个非 Iceberg provider 需要共享 COW selection transport、selection 超出有界内存仍须正确执行，或恢复要求重新 match、重新选择 strategy 或取得 current generation 时，重新讨论 SPI 和 durable evidence 形状。
