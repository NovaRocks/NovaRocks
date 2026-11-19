---
id: ADR-0236
title: "Installed optimization domains and one partition rank retention author"
domain: [sql-compiler, runtime-filter, pipeline-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-17
provenance:
  - "discussion: 2026-11-17 partition rank retention and installed optimization domains"
code-anchors:
  - "novarocks/physical-plan/src/lib.rs (grouped_state_topn_key_supported)"
  - "novarocks/local-program/src/runtime_filter.rs (is_filter_key_type)"
  - "novarocks/sql/src/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs (sort_matches_window_signature)"
  - "novarocks/execution/src/exec/operators/compiled_sort.rs (CompiledSortProcessor::retain)"
  - "novarocks/execution/src/exec/operators/sort/creation.rs (SortCreation::encode_keys)"
---

## 问题

可选优化如何同时满足规划资格、准备侧安装能力和排名语义，使优化不会把原本可执行的查询改成能力拒绝，也不会在不同执行阶段产生不同的排名边界？

## 背景与执行事实

SQL 拥有改写和放置政策，LocalCompiler 消费冻结的物理事实，Execution 执行本实例已绑定的程序。三者不能根据名字重建另一层的语义。准备侧仍须校验实际包，规划侧的保守选择不替代契约校验；唯一静态执行路径沿 ADR-0185。

| 机制 | 规划与准备共同消费的事实 | 执行作者 |
|---|---|---|
| GroupedStates TopN | `grouped_state_topn_key_supported` 组合比较器与 NativeExchangeV1 分区键域，覆盖完整分组键 | 原分组状态与 TopN 装配 |
| membership RF | canonical membership schema 的 `is_filter_key_type` | 原 membership evaluator 与订阅 |
| ordered RF | 完整声明的 ordered key 域及扫描输出的 flat payload 域 | 原 ordered scan |
| partition TopN | 分区键、组内排序键、排名种类与非零 K | compiled Sort 的 `retain` |

`LocalSortMode` 将完整排序、分区完整排序、行数 TopN 和分区 TopN 封闭为不同形态。行数 TopN 是无分区 RowNumber 的快速路径，按 `offset + limit` 保留，最终输出再跳过 offset；分区 TopN 按分区键、组内键及稳定到达序排序。Rank 保留名次不大于 K 的整个边界同级组，DenseRank 保留前 K 个同级组，RowNumber 保留前 K 行。没有组内键时，同分区全部为同级。分区及同级判等复用窗口原有的规范化键编码，不另定浮点或 NULL 语义。

CompleteOnce membership 的定点等待降级，在 LocalProgram 中用 live Batch 表示：完整快照到达前的批原样通过，从之后的批边界起过滤；它不同于 MonotonicUpdates 的逐次更新。此映射延续 ADR-0003 的静态定点政策，不授予真正 membership MonotonicUpdates 或其它粒度的执行能力。

这些是已裁决的设计规则。源码接线、原生结果、真实退出和最终验收状态须按各自实际证据核对，ADR 不充当通过证明。

## 考虑过的选项

1. **规划、降级、执行各自维护类型白名单。设计否决。** 局部修改方便，但同一优化的资格会漂移，规划可选优化后才在准备时失败。共有的纯判定作者应表达已安装能力；各层保留自己的责任与校验。
2. **遇到能力拒绝后在准备侧删除优化或重建替代计划。设计否决。** 这会越过冻结的物理事实与静态证明边界。能否不规划可选优化由 SQL 决定；已经封存的包必须准确接受或准确拒绝。
3. **为行数裁剪、分区裁剪及将来的局部阶段各写排名循环。设计否决。** 多个作者会独立解释同级边界、NULL 和到达序。排序后按名次保留必须共用一个作者；窗口继续计算结果列，不能替代排序中已经声明的限额。
4. **在 Sort 作者中归一化保留规则，复用现有排序与构造路径。采用。** 行数 TopN 保留原部分排序快路径，分区模式完整排序后按名次选择，增量裁剪与最终输出调用同一例程。输入键只在到达时求值一次。
5. **为分区排名增加 hash 定位或专用堆。待评估。** 它们可能降低重排成本，但还需要新的状态及生命周期证明；当前不引入。即使以后采用，也必须在同一排序作者内落实名次保留契约。

交换之前增加局部排名阶段已作为后继最终形态选定，当前实现不包含该阶段。启用前须明确其分布、排名种类与全局限额关系；它仍由同一排序作者执行，不能增加第二个名次保留算法。

## 裁决

1. **安装域规则。** 可选优化只在当前实际安装域内规划。规划与准备调用同一纯判定函数；域外跳过该可选优化，准备侧保留原契约校验，不吞掉资源、控制或非法输入错误。
2. **唯一名次保留规则。** 任何阶段的分区排名限额，都由排序作者中的同一个名次保留例程执行，包括将来增加的局部阶段。增量裁剪与最终输出共用该例程；分区及同级判等与窗口同源，稳定到达序跨裁剪保留。不能把普通完整排序冒称已经执行限额。
3. **改写前提规则。** 依赖键等式的改写必须自己核对等式。排名下推核对 Sort 的分区标记、完整分区前缀与窗口签名相同，组内键表达式、方向、NULL 放置与签名完全一致；多余排序键也不满足前提。去重签名与改写比较共用签名作者，不能把证明负担移给下游跨节点猜测。
4. **降级激活规则。** 已证明的 CompleteOnce membership Batch 降级只改变等待与后续批过滤时机，保持原 binding、channel、键、NULL 与 reduction；订阅激活不一致仍由原执行契约拒绝。

## 接受的妥协（诚实记录）

- 分区裁剪要完整重排当前保留段与新增段，较专用增量结构可能多做工作；本裁决优先收敛语义与构造作者，没有性能优越性结论。
- Rank 和 DenseRank 的边界同级组可以很大，K 不是保留行数或内存上界。裁剪阈值随实际保留行数增长；资源拒绝仍由实际构造 owner 决定，沿 ADR-0162 保持内存内计算。
- 安装域外不使用可选优化，可能失去剪枝收益。扩大安装域必须同时扩大实际准备与执行能力，不能靠放宽规划谓词制造支持。
- 完整快照晚到时，已通过的批不会补过滤；join 本体继续保证结果正确。共享键编码保留现有浮点位与 NULL 判等，未在本裁决中修改 SQL 全局比较语义。

## 何时重新评估

- 真实负载证明重复完整重排是主要开销时，评估专用增量数据结构；增加局部阶段时，先证明其分布与全局排名关系，并继续复用唯一名次保留作者。
- 任一比较器、分区键、membership schema 或扫描载荷支持域扩大时，同步检查纯谓词、准备校验与原生回归。
- 窗口的浮点、NULL 或同级语义调整时，同步迁移共享作者及独立排名预期，不能仅改裁剪侧。
- reader 级过滤或新的 RF lifecycle/粒度具备实际执行能力时，重新评估降级激活映射和可选放置范围，保留独立订阅与批边界证明。
