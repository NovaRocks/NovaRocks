---
id: ADR-0259
title: "Trino aggregates freeze state and published semantics"
domain: [sql-compiler, map-semantics]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0265
    scope: "Items 4 and 5 on published result semantics, legacy interpretation layouts and contract rebinding after an engine change."
    link: "ADR-0265-mv-publishes-bound-source-and-storage-facts.md"
date: 2026-11-23
provenance:
  - "TRC-6d4 Spec v5 and Plan v2, approved Obsidian snapshot 98286ab2126bd4cc819c167e78ef4969cd4c3be1"
code-anchors:
  - "novarocks/functions/src/aggregate_call.rs (PersistentAggregateContract)"
  - "novarocks/sql/src/agg_mergeability.rs"
  - "novarocks/mv-application/src/persistence/aggregate_bindings.rs (validate_published_aggregate_contracts)"
---

## 问题

聚合重载的更新、状态交换和结果发布怎样保留同一调用事实？升级后，拥有相同列和物理布局的旧物化结果怎样退出改写和增量维护？

## 裁决

1. **从原重载取得结果及状态。** SQL 绑定公开调用及其逻辑实参，Functions 签发实际结果类型、状态格式和阶段资格。极值从已定型值域和方向选择比较器。浮点有限值及无穷在两个方向都优于 NaN，正负零比较相等。样本方差和标准差沿样本重载执行。旧公开名称和专有字符串聚合会话选项随调用切换退役。
2. **按原调用的行域更新。** 普通聚合 FILTER 依 [ADR-0253](ADR-0253-aggregate-filters-select-original-rows-per-call.md) 选择当前调用的原始行。无序且无 DISTINCT 的单参 `approx_distinct` 可在 Partial 更新并交换已选状态。Final 核对原生产调用和状态格式再合并。LISTAGG 单阶段保留完整组内排序，DISTINCT 比较全逻辑实参元组。超过前缀字节预算时按原选项拒绝或截断完整值。
3. **各自决定空状态与键身份。** `array_agg` 保留已选的 NULL 元素，无选中行时发射 NULL。`map_agg` 跳过 NULL 键并保留值的 NULL，无有效键时发射 NULL。更新与合并使用相同键等价，允许按接收次序保留一个合法的重复键代表。标量 MAP 构造、普通查找和 MAP 键域继续沿 [ADR-0183](ADR-0183-map-keys-are-non-null-and-unique.md) 与 [ADR-0255](ADR-0255-collection-calls-and-relational-sequence.md)。
4. **由当前发布证明资格。** keyed 解释修订 2 保留旧可解码布局，并在聚合条目追加已选函数、重载、结果语义与状态格式。查询改写和增量准备共用实际发布的校验。受影响的旧发布缺少合同或合同不匹配时，改写回到基表计划，增量在读取状态前报告需全量重建。显式直读仍呈现所读版本的已发布结果。
5. **由原发布作者完成重建。** FULL 从源重新计算，通过同一物化发布流程附着重新绑定的解释和输入版本。只有实际发布成功后才可用于后续改写或增量准备。仍缺少状态作者的形态保留原具名拒绝。普通 HLL 交换状态的可用性与 7B4 的持久状态资格分别验证。

## 取舍与定位

- 以函数名、结果类型与状态列布局推断旧发布语义，会把旧浮点 `max(1.0, NaN)` 的 NaN 当成新语义的 1.0。聚合解释记录实际绑定，因而允许共用物理布局并拒绝不匹配的发布。
- 物理计划包从修订 40 升至 41。部署采用现有 FE/BE exact-island 准入。诊断跨版本问题时，先查两端构建兼容身份，再检查实际发布引用的解释及收到的拒绝。按原管理接续流程执行 FULL，并在新快照上核对显式读取、EXPLAIN 与下一次刷新。见[物化视图管理](../guides/iceberg-v3/materialized-views.md)。
- 当前双参 `approx_distinct` 误差控制、弃用 `approx_percentile` accuracy，以及未安装的窗口重载由 B9 承接。持久状态的支持范围受实际安装的作者约束。
