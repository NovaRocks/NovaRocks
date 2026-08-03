---
id: ADR-0039
title: "Immutable scan-unit domain facts"
domain: [provider-spi, runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "discussion: 2026-08-03 immutable scan-unit physical facts"
code-anchors:
  - "novarocks/spi/src/connector/domain_facts.rs (ConnectorScanUnitDomainFacts)"
  - "novarocks/spi/src/connector/execution.rs (ConnectorPreparedScanUnitDescriptor)"
---

## 问题

已冻结的 Connector local scan unit 如何向后续执行侧提供可安全使用的物理值域证据，同时不把 Runtime Filter 生命周期、文件格式或 Provider 策略重新耦合进 SPI？

## 背景与执行事实

cluster split 负责 Frontend 的有界工作包与授权 membership；Backend prepare 会把它认证并 materialize 为不可伪造的 local scan unit。此前 unit 只携带 reader payload 与 cost，导致执行侧既无法证明一个值域属于哪个 physical leaf，也无法区分 Provider无能力、footer缺统计、类型不支持和资源超限。

Parquet footer 可以在已授权文件上提供 row-group 的 min/max/null count，但 Iceberg field mapping、delete/DV语义属于 Provider；StarRocks当前没有由 Frontend 冻结的 rowset/segment statistics contract。将这些细节放进通用 wire 或在 reader open 时查询 latest metadata 都会破坏 membership 和 retry 语义。

## 考虑过的选项

1. 在 Runtime Filter Provider callback 中按当前filter需要读取 metadata。拒绝：facts随subscription变化，Provider获得了 RF lifecycle，且同一unit不再有单一线性化点。
2. 将 file、row group 或 statistics 加入通用 native protobuf。拒绝：protocol 固化 Provider物理语义，无法让不同 Connector 独立演进。
3. 在 `prepare_split` 将 bounded、immutable、provider-neutral `Range + Null` facts 与 unit payload/cost一起 seal，Core只搬运，Execution以后再决定是否使用。采用。

## 裁决

SPI定义唯一中立 scalar vocabulary及 `Available|Missing` scan-unit facts。可用事实包含 exact physical row count、稳定table field ordinal、typed range/all-null、null count以及 Exact 或 Conservative evidence；事实只通过 sealed unit 的只读访问器暴露。typed missing 是合法 fail-open evidence，corruption、身份不一致和不合法bounds仍是 prepare error。

facts 不进入既有 membership digest，也没有独立 version、callback、refresh 或同unit refinement。新 split 可以独立产生新 unit facts；已经发布的 unit 不可变。SPI不依赖文件格式、Provider或 Execution；文件系统只暴露原始 footer statistics，Iceberg Provider拥有 schema/field mapping与删除语义，StarRocks在缺少 pinned truth 时显式missing。

Core 可以把 sealed unit带到 reader-open 前并上报 availability evidence，但不比较值域、不读取 RF snapshot、不返回 prune decision。实际的 pre-open evaluation、effect 与 fail-open policy由后续 Execution owner负责。

## 接受的妥协（诚实记录）

第一版只支持 Range + Null 和少量无损 scalar 类型，因此 decimal、floating point、集合、prefix、compound domain及更丰富的格式统计都会是 missing。选择这个较窄的契约不是因为它能表达所有优化，而是为了先证明 facts、membership、reader 与跨进程 retry 的同源关系；在没有该关系前扩大代数只会把不安全的近似隐藏在类型中。

prepare 会额外读取已授权 Parquet footer，并为合法但超过facts预算的值域放弃为 typed missing。这牺牲一部分潜在剪枝机会，换取固定资源上界、明确可观测的降级和不改变查询结果的 fail-open 行为。

## 何时重新评估

- 已有真实 workload 证明 Range + Null 不足，且需要的额外 Domain 能在不注入 RF identity 的前提下保持 bounded；
- 有证据表明同一 sealed unit 的单调 facts refinement 值得承担明确的version、replay与effect attribution；
- StarRocks 或其他 Provider提供由 Frontend冻结的 rowset/segment/delete-vector membership与稳定statistics contract；
- footer acquisition 成本成为可测量瓶颈，需要重新设计按需采集与预算，而不是回退为 reader-time latest metadata 查询。
