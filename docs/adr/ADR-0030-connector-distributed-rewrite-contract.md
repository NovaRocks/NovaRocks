---
id: ADR-0030
title: "Connector distributed rewrite contract"
domain: [provider-spi, table-maintenance]
status: superseded
supersedes: []
superseded-by: ADR-0133
partially-superseded-by:
  - id: ADR-0235
    scope: "The inherited fixed 16,384 writers and 64 MiB aggregate staging budgets. ADR-0133 remains the superseding contract"
    link: "ADR-0235-iceberg-write-layout-and-data-plane-capacity.md"
date: 2026-08-02
provenance:
  - "discussion: 2026-08-02 connector distributed rewrite"
code-anchors:
  - "novarocks/spi/src/connector/control.rs (ConnectorControlBinding)"
---

## 问题

需要 BE Arrow staging 的 table-format rewrite，如何同时保留 FE 的 durable recovery、exact connector generation 与
Iceberg 的一次 snapshot 原子提交，而不把 catalog commit 放回 BE 或 generic core？

## 背景与执行事实

`REWRITE DATA FILES` 和 `REWRITE POSITION DELETE FILES` 都需要读取已有文件、stage replacement files，再提交外部
catalog。C1 已提供 operation/cohort/execution/writer 分层、fragment-local report 与 FE aggregate commit；E1 已提供
exact-generation durable maintenance operation。两者单独使用都不足以冻结一组跨 BE staging 的 rewrite inputs 并在崩溃后
安全处理 staged files 和 commit response loss。

## 考虑过的选项

1. 保留 core concrete rewrite action。实现短，但无 durable fence、会让 provider retry 自己重新规划，并继续绕过 C1。
2. 为 maintenance 建新的 BE wire 和 collector。能独立建模，但重复 C1 carrier，也会扩大 FE/BE protocol surface。
3. 以 provider-frozen groups 映射 C1 cohorts，并由 FE durable operation 驱动同一 write control 的 aggregate commit。

## 裁决

采用独立 FE-only distributed-rewrite capability，与 `ConnectorWriteControl`、metadata 和 execution distribution 从同一
exact generation 原子取得为 composite maintenance lease。

- provider 在任何 staging 前冻结全部 file groups 并写 immutable, content-addressed artifact；公共 SPI 和 StateStore
  仅保留 bounded handle/digest。
- 一个 group 对应一个 C1 cohort；所有 cohort 一次注册、排序并 seal。首版串行 staging，所有 accepted reports 只做
  一次 aggregate commit，恰好写一个 Iceberg snapshot。
- operation restart 在 staging 期只 abort 已记录 artifacts，在 commit-pending 期只 reconcile marker；找不到 exact
  generation 保持 `Unresolved`。
- BE 只读取 provider-private splits、stage Arrow batches 和报告 opaque staged reports；catalog commit、abort/reconcile
  与 cache finalization 均由 FE/control provider 执行。

## 接受的妥协（诚实记录）

首版把 group staging 固定为串行，并继承 C1 的 4096 cohort、16384 writer 和 64 MiB aggregate 上限。这会限制极大表的
maintenance 吞吐，也可能让超预算任务被拒绝；选择它是为了让 cancellation、artifact cleanup 和 single-snapshot
failure boundary 可证明，而不是因为串行执行本身更高效。

provider artifact 会在 terminal 后 best-effort 删除，残留交给后续 orphan cleanup。我们接受短期的私有控制对象，避免在
commit unknown 时删除仍可能已经成为 snapshot 一部分的 replacement file。

## 何时重新评估

- 真实 rewrite workload 无法在 C1 固定 budget 内完成，需要 provider-owned durable staged-manifest reference。
- 产品要求 partial-progress snapshots、跨 group output sharing 或自动 rebase。
- multi-FE takeover 能够安全接管 `Unresolved` 的 exact-generation operation。
- Connector write contract 的 execution/abort/reconcile ownership 发生根本变化。
