---
id: ADR-0252
title: "Aggregate filters select original rows per call"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-22
provenance:
  - "discussion: 2026-11-22 ordinary aggregate FILTER row domain and state handoff"
code-anchors:
  - "novarocks/sql/src/planner/logical/build/aggregate.rs (project_aggregate_inputs)"
  - "novarocks/physical-plan/src/plan.rs (AggregateCall)"
  - "novarocks/execution/src/exec/operators/compiled_aggregate.rs (AggregateFilterScratch)"
---

## 问题

同一组内的普通聚合如何使用各自的筛选条件，同时保留分组集的原行求值、空状态和分布式合并？

## 背景与执行事实

普通聚合的分组键和值通道具有不同生命周期。Repeat 把原行复制到多个分组集，省略的分组键在独立列中置 NULL。部分聚合先接收原输入行，然后发射中间状态。后继聚合通过状态值和序列找到该产出。

Trino 484 在分组集展开前投影聚合输入，逐调用用布尔 mask 选择更新行。选中行上的 NULL 继续交给原聚合重载处理。函数实参和排序键的必要求值可能产生数据错误，谓词为 FALSE 的行也处于原输入求值域。

## 考虑过的选项

1. **全局行筛选。设计否决。** 在聚合前筛掉不满足条件的原行会改变可见组和兄弟调用的输入。
2. **改写为条件实参。设计否决。** 用 NULL 表示未选行会失去选中 NULL 与未选行的区别，也会改变原实参求值。
3. **逐调用选择并更新组内状态。采用。** 聚合算子在完整分组映射上生成选中原行及紧凑组号。已有更新接口消费两者，排序键和值沿原批寻址。
4. **带 FILTER 的 DISTINCT 按组分片后完整聚合。待评估。** 此放置可减少网络 Gather 压力，依赖完整组键和分区间输出唯一性的独立证明。

## 裁决

1. **原行求值。** SQL 给每个已绑定调用保留原发生处。在聚合输入上投影存在的布尔谓词。有 Repeat 时，计算型值实参与排序键也在展开前物化。确定性复用要求同一行域、完全相同的输入和已证实的求值资格。
2. **更新根。** 冻结包的普通聚合 FILTER 是端口 0 的直接 BOOLEAN 值根，需求为 TruthOnly。根独立于实参和排序通道。组在全部原行上分配，Single 和 Partial 只更新谓词为 TRUE 的原行。空选择保留已初始化的状态。
3. **合并状态。** Intermediate 和 Final 读取对应产出状态并核对序列及状态值。GroupedTopN 状态消费同样使用已证实的中间产出。重载、状态布局、DISTINCT、排序和 NULL 元素的行为由其原作者持有。
4. **改写资格。** 共享 DISTINCT 键改写需证明各调用的键存在性和兄弟调用状态。移动或替换聚合需证明输入行域、行重数、必要求值和状态语义。Iceberg IVM 的定型刷新合同拒绝带逐调用 FILTER 的定义。

## 接受的妥协（诚实记录）

分组集在展开和聚合之间保管额外的投影值。当前带 FILTER 的 DISTINCT 走已有 Single 分布属性，可能将同一节点的全部原行聚到一个 BE。

## 何时重新评估

提出按完整组键分片的 Single 放置时，证明每个组只在一个分片发射，并用真实多 BE 布局验证。重新启用筛选后的共享键去重时，验证每个调用从同一键的真、假和 NULL 行得出的存在性以及兄弟状态的原行域。

[ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) · [ADR-0238](ADR-0238-self-contained-expression-units-and-explicit-environments.md) · [ADR-0249](ADR-0249-trino-public-functions-and-internal-aggregate-identities.md)
