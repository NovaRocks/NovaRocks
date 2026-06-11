# OQ-15 · TopN and Sort Compactness Design

- 日期: 2026-06-10
- 对应 roadmap: `OQ-15 · TopN and sort compactness`
- 依赖: `OQ-8 · Distribution-aware physical search`
- 状态: Spec - pending implementation plan
- 范围: standalone SQL optimizer/codegen; FE-compatible lowering/runtime 非目标
- 参考基线: `logs/plan-quality/20260609-fe-nr-plan-diff`

## 1. 一句话

清理 standalone optimizer 中不必要的两阶段 TopN、冗余 Sort enforcement 和可证明等价的
TopN/Sort/Limit 形态, 让 `ORDER BY` / `LIMIT` 查询生成更紧凑的物理 plan, 同时保持
null ordering、direction、offset window 和 distributed merge 语义不变。

本设计采用激进但可证明的替换策略: 只要结构信息和确定性 logical properties 能证明等价,
就允许直接合并或消除上层 TopN/Sort; 不能证明时不生成候选。统计信息、NDV、row count
和 cost 只能用于等价候选之间的选择, 不能作为语义等价证明。

## 2. Brainstorming 决策

| 决策点 | 结论 |
|---|---|
| 总体范围 | 做完整 TopN/Sort compactness 设计, 覆盖 merge、Sort satisfaction、TopN pushdown、exchange/topn 关系 |
| 验收基线 | 使用当前 worktree 的 `logs/plan-quality/20260609-fe-nr-plan-diff` |
| pushdown 范围 | 一阶段设计覆盖 scan/project/join/aggregate/set-op, 不只做 child-side pruning |
| 替换策略 | 可证明等价时允许直接消掉或合并上层 TopN/Sort |
| 证明来源 | 语法结构 + 确定性 logical properties, 不使用估算统计证明等价 |
| 实现边界 | 只改 standalone SQL optimizer/codegen; StarRocks FE plan 只作参考 |
| 等价类策略 | 复用现有 `EquivalenceClasses`/`unique_columns`, 新增 TopN/Sort 专用 proof helper, 不重建等价类系统 |

## 3. 当前 NovaRocks 基线

当前实现已有以下基础:

- `SortLimitToTopN` 已将 `LogicalLimit(LogicalSort(x))` 转成 `LogicalTopN(x)` 候选。
- `SplitTopN` 已生成 `Final(split) -> Partial` 的两阶段 TopN 候选。
- `PhysicalTopN` 的 property derivation 能区分 `Partial` / `Final` distribution。
- `OrderingSpec::satisfies` 支持精确 sort key 前缀匹配。
- Search 已能在 provided property 不满足 required property 时插入 `PhysicalSort` / `PhysicalDistribution`
  enforcer。
- `LogicalProperties` 已包含 `equivalence_classes` 和 `unique_columns`。
- `EquivalenceClasses` 能从 filter equality、inner join equality 和 physical hash join equality 派生,
  并已被 `InnerJoinEquivalencePredicateRule` 用于 literal predicate propagation。

主要缺口:

- 当前 TopN split 是候选生成, 但缺少对最终可见 plan 形态的 compactness 约束; `tpc-ds/q41`
  和 `tpc-ds/q72` 的 NR plan 出现连续两个 `TOP-N`, FE plan 是一个 `TOP-N` 加 merging exchange。
- `OrderingSpec::satisfies` 不使用等价类或 Project alias remap, 只能做原始 `ColumnId` 精确匹配。
- `LogicalProject` 对等价类只是继承并裁剪, 不能表达 `ColumnRef -> ColumnRef` alias/remap。
- TopN/Sort 的等价判断分散在 rule、property、codegen 中, 没有单一 proof helper。
- Join/Aggregate/SetOp 下的 TopN pushdown 没有明确 guard, 不能安全地做激进替换。
- `unique_columns` 目前主要来自 aggregate group key, 不是完整 catalog constraint framework, 只能作为有限证明来源。

## 4. 设计目标

1. 合并连续同序 TopN, 减少 `tpc-ds/q41/q72` 这类可见双 `TOP-N` plan。
2. 当 child 已满足 ordering requirement 时避免插入冗余 Sort。
3. 当 TopN/Sort/Limit window 能证明等价时, 允许直接消除或合并上层节点。
4. 在 Scan/Project/Join/Aggregate/SetOp 上生成可证明等价的 TopN pushdown 候选。
5. 所有 aggressive rewrite 都 fail closed: 不能证明等价就不生成候选。
6. 保持 `asc`、`nulls_first`、offset、limit、NULLS ordering 和 tie-breaker 语义不变。
7. 保持 distributed TopN final/partial 和 merging exchange 的执行语义不变。
8. 新增 optimizer plan golden 和 SQL verify, 锁住 compactness 与语义边界。

## 5. 非目标

- 不改变 FE-compatible thrift plan lowering、BE protocol path 或 runtime operator 语义。
- 不为单个 TPC query 手工删除 TopN/Sort。
- 不使用估算 row count、NDV、confidence 或 cost 证明 TopN 替换等价。
- 不实现完整 catalog unique/foreign-key constraint framework。
- 不把 StarRocks FE 的所有 TopN/Sort rule 逐条移植。
- 不在没有 order requirement 的位置保留虚假 ordering。
- 不让 hash distribution 被误当作 ordering。

## 6. 架构

OQ-15 不新增 LogicalPlan-level TopN。入口仍沿用当前路径:

```text
LogicalPlan Sort + Limit
  -> Memo LogicalSort / LogicalLimit
  -> SortLimitToTopN
  -> LogicalTopN
  -> TopN compactness transformation rules
  -> implementation/search
  -> PhysicalTopN / PhysicalSort
  -> fragment_builder
```

新增能力分三层:

1. **Canonicalization 层**
   - 归一化等价 TopN/Sort/Limit 形态。
   - 合并连续 TopN。
   - 消除 `TopN(Sort(x))` 中同序冗余 Sort。
   - 收敛 `Final(split) -> Partial` 的可见 plan 形态。

2. **Equivalence Pushdown 层**
   - 在 Cascades transformation rules 中生成等价候选。
   - 支持 Scan pushdown 以及穿过 Project、Join、Aggregate、SetOp 的 TopN pushdown。
   - 每条 rule 必须通过 proof helper 证明等价。

3. **Property / Enforcer 层**
   - Ordering/distribution satisfaction 是最终防线。
   - Sort enforcer 只在 chosen child output ordering 不满足 required ordering 时插入。
   - TopN output ordering 只在 sort item 能稳定映射到 `ColumnId` 时声明。
   - `PhysicalTopN(Final, split)` 与 merging exchange/codegen 的关系单独守住。

## 7. TopN/Sort Proof Helper

新增一个内部 helper, 建议放在 `src/sql/optimizer/topn_proof.rs` 或
`src/sql/optimizer/property/topn.rs` 一类小模块。它只回答语义证明问题, 不计算 cost。

职责:

- 判断两个 sort key list 是否等价:
  - `asc` 必须一致;
  - `nulls_first` 必须一致;
  - key 表达式必须可映射到稳定 `ColumnId`;
  - 可以使用 `EquivalenceClasses` 判断 `ColumnId` 等价;
  - 可以使用 Project 的 `ColumnRef -> ColumnRef` remap;
  - 不接受函数、cast、复杂表达式的猜测等价。
- 判断 TopN window 是否可合并:
  - `limit` 必须有限;
  - offset/limit 合成不能溢出;
  - inner window 必须覆盖 outer window 需要的全量行区间;
  - 无 limit 或 offset-only 不参与 aggressive merge。
- 判断 operator boundary 是否允许 pushdown:
  - Scan 是否声明了能保证全局 ordered top-k 或 limit 的能力;
  - Project remap 是否纯 ColumnRef;
  - Join side、join type、multiplicity proof 是否满足;
  - Aggregate 输出与 group key/aggregate output 是否能确定映射;
  - SetOp branch output ordering key 是否一致。

现有 `EquivalenceClasses` 不重建, 但本任务需要补 TopN/Sort 消费侧能力。当前等价类基础设施主要用于
谓词传播, 对 Project alias、ordering satisfaction 和 TopN merge 还不够成熟; helper 是这次的扩展点。

## 8. Transformation Rules

### 8.1 `MergeConsecutiveTopN`

目标形态:

```text
TopN(outer_order, outer_limit, outer_offset)
  TopN(inner_order, inner_limit, inner_offset)
    child
```

合并条件:

- outer/inner order 等价;
- direction/null ordering 完全一致;
- inner window 覆盖 outer window;
- `limit + offset` 使用饱和/checked 计算, 不能溢出;
- partial/final phase 必须兼容, 不能把 distributed partial 误当作 global final。

输出:

- 若 outer window 已完全由 inner 覆盖, 可消 inner 或合成单个 TopN。
- 若 outer 更严格, 保留更严格 window。
- 若 phase/split 语义不兼容, 不生成候选。

### 8.2 `RemoveRedundantSortUnderTopN`

目标形态:

```text
TopN(order)
  Sort(order)
    child
```

条件:

- Sort order 满足 TopN order;
- Sort 不是 analytic precursor sort, 或 analytic partition requirement 已由 TopN 语义证明不需要;
- Sort 不携带其它执行语义。

输出:

```text
TopN(order)
  child
```

### 8.3 `MergeSplitTopNShape`

目标:

- 识别 `Final(split) -> Partial` 的合法 distributed TopN。
- 在 extract/codegen 中尽量输出一个可见 `TOP-N`/merging exchange 形态, 避免 `q41/q72`
  这种连续 `TOP-N` plan。

约束:

- 不破坏 partial 预裁剪和 final global merge 的执行语义。
- child 必须是 expected partial shape; 不满足时保持现有 fail-fast。
- 如果 codegen 仍需要 partial SORT_NODE 作为 exchange 输入, EXPLAIN 层应避免把它误渲染成冗余 global TopN。

### 8.4 `PushTopNIntoScan`

Scan pushdown 分成两个层级:

1. **Pruning hint**: scan 可以接收 `limit` / `topn` 上界用于减少读取, 但 final TopN 必须保留。
2. **Equivalent replacement**: 只有 scan backend 明确声明能返回全局 ordered top-k, 且 sort key/null ordering/direction
   与 TopN 完全一致时, 才允许消除 final TopN。

默认规则:

- local parquet、Iceberg data-file scan、普通 object-store scan 不默认声明全局 ordering。
- 没有 `ORDER BY` 的 plain `LIMIT` 可以作为 row-count upper-bound hint 下传, 但不能依赖 scan 输出顺序消除全局 Limit。
- connector 能力必须是显式 capability, 不能通过统计或文件布局猜测。
- sort key 是复杂表达式时不下推到 scan, 除非 backend 明确支持同一表达式语义。

### 8.5 `PushTopNThroughProject`

条件:

- TopN sort keys 都能通过 Project 做 `ColumnRef -> ColumnRef` 反向映射。
- Project 不丢失 TopN 输出需要的列。
- Project item 中涉及 sort key 的表达式不能是函数、cast、binary op 或 nullable-changing rewrite。

输出:

```text
Project
  TopN(remapped_order)
    child
```

若 proof helper 能证明 Project 上下 TopN 等价, 可以消除上层 TopN; 否则保留 final TopN。

### 8.6 `PushTopNThroughJoin`

默认第一版只允许 inner join。条件:

- TopN keys 完全来自可推的一侧。
- 另一侧不会改变该侧 key 的 multiplicity, 或可由确定性 logical properties 证明 multiplicity 不影响 TopN 结果。
- 可使用 inner join 等价类做 join key 替换。
- 不使用 row count、NDV 或 cost 证明 multiplicity。

默认不处理:

- outer join;
- semi/anti join;
- null-aware join;
- sort key 跨两侧;
- sort key 是 join 后表达式。

输出可以是:

```text
TopN
  Join(left, right)
```

到:

```text
Join(TopN(left), right)
```

只有 proof helper 证明 global TopN 被 child TopN 完全覆盖时, 才允许消除上层 TopN。

### 8.7 `PushTopNThroughAggregate`

条件:

- TopN keys 是 aggregate 输出列。
- 可证明 key 与 group key 或 aggregate 输出之间存在确定性映射。
- 不把 `ORDER BY sum(v) LIMIT` 这类 aggregate-function order 推到 child, 因为 child row order
  不能证明 group result order。
- group-by-only 场景可使用 group key 唯一性证明。

默认策略:

- 允许生成 child pruning 候选。
- 只有确定性证明 final TopN 等价时才消除 final。

### 8.8 `PushTopNThroughSetOp`

`UNION ALL`:

- 可生成 branch TopN pruning 候选。
- branch order key 必须能映射到统一 output key。
- final TopN 通常保留。
- 只有每个 branch TopN window 已证明覆盖 global window 时, 才允许合并或消除 final。

`UNION DISTINCT` / `INTERSECT` / `EXCEPT`:

- 第一版不做 aggressive TopN 消除。
- 后续若实现, 必须先证明 dedup/intersection/subtraction 不改变 TopN window。

## 9. Property 和 Enforcer 调整

`OrderingSpec::satisfies` 保持前缀语义:

- provided ordering 必须至少覆盖 required ordering 前缀;
- direction 和 null ordering 必须精确一致;
- hash distribution 不提供 ordering;
- `Gather` 本身不提供 ordering;
- ordering equivalence 只能通过 proof helper 使用 `EquivalenceClasses`/Project remap 判断。

TopN output property:

- `Final` 仍输出 `Gather`。
- `Partial` 不声明 global ordering。
- sort keys 无法稳定映射到 `ColumnId` 时输出 `OrderingSpec::Any`。
- split final 的 child requirement 不能要求 child 已 global ordered; final merge 负责 global order。

Sort enforcer:

- 只在 chosen child output ordering 不满足 required ordering 时插入。
- enforcer Sort 必须保留 plain ORDER BY 语义, 不带 analytic partition tag。

## 10. Codegen / EXPLAIN 边界

Codegen 目标不是改变执行语义, 而是让 chosen compact physical plan 映射成更紧凑的 visible plan:

- single-stage TopN 继续生成限额 SORT_NODE。
- `Final(split)` 继续消费 partial sorted streams 并执行 global merge。
- 如果 partial SORT_NODE 是 distributed TopN 的必要局部预裁剪, EXPLAIN 不应把它误算成冗余 global TopN。
- 若 `Final(split)` child 不是 partial root, 保持现有显式错误。

EXPLAIN/plan golden 的稳定信号:

- `TOP-N (limit=` 数量下降;
- redundant `SORT BY` 不新增;
- `HASH/BROADCAST/GATHER EXCHANGE` 不因 TopN merge 多插一层;
- disable rule 后能看到未 compact 的 fallback shape。

## 11. Error Handling

- 所有 aggressive rules fail closed: helper 返回 false 就不生成候选。
- 不支持的表达式、复杂 sort key、非 ColumnRef remap、复杂 offset window、非 inner join、非 `UNION ALL`
  set-op 默认返回空候选。
- 规则返回空候选不是错误。
- 真正的错误只保留给 existing invariants, 例如 chosen `Final(split)` 的 child 不是 expected partial shape。
- 不为追求 compactness 降级或猜测 SQL ordering 语义。

## 12. 测试设计

### 12.1 Rust unit tests

新增或扩展 unit tests:

1. proof helper 判断同序 TopN keys 等价。
2. proof helper 拒绝 direction/null ordering 不同的 keys。
3. proof helper 使用 `EquivalenceClasses` 判断等价 ColumnId。
4. Project `ColumnRef -> ColumnRef` remap 成功, 表达式 remap 失败。
5. TopN window 合成覆盖 `limit`、`offset`、overflow false case。
6. `MergeConsecutiveTopN` 合并合法连续 TopN。
7. `RemoveRedundantSortUnderTopN` 消除同序 Sort。
8. Join pushdown 拒绝 outer/semi/anti/null-aware join。
9. Scan replacement 只有 backend 声明 ordered top-k capability 时成功。
10. Aggregate pushdown 拒绝 `ORDER BY sum(v)` child pushdown。
11. SetOp pushdown 只允许 `UNION ALL` pruning 候选。

### 12.2 Optimizer SQL golden

新增 `sql-tests/optimizer/sql/topn_compactness_*.sql`:

- `topn_compactness_merge.sql`
  - consecutive TopN merge;
  - `-- @explain_contains=TOP-N`;
  - 避免出现连续两个同序 `TOP-N`。
- `topn_compactness_sort_elision.sql`
  - `TopN(Sort(x))` 消 Sort。
- `topn_compactness_project.sql`
  - alias remap 成功 case;
  - expression remap guard case。
- `topn_compactness_scan.sql`
  - scan pruning hint 保留 final TopN;
  - 无 ordered top-k capability 时不消 final。
- `topn_compactness_join.sql`
  - inner join safe pushdown;
  - outer/semi/anti guard case。
- `topn_compactness_aggregate.sql`
  - group-key safe case;
  - aggregate function order unsafe case。
- `topn_compactness_setop.sql`
  - `UNION ALL` branch pruning + final TopN;
  - distinct/intersect/except guard case。
- `topn_compactness_disabled.sql`
  - `SET disable_optimizer_rules = 'MergeConsecutiveTopN,RemoveRedundantSortUnderTopN,PushTopNIntoScan,PushTopNThroughProject,PushTopNThroughJoin,PushTopNThroughAggregate,PushTopNThroughSetOp'`
    验证回退。

### 12.3 Existing SQL verify

必须覆盖:

- `sql-tests/sort/sql/topn_order_limit.sql`
- `sql-tests/sort/sql/topn_null_order_limit_offset.sql`
- `sql-tests/optimizer/sql/window_ordering_reuses_child_sort.sql`
- 新增 optimizer suite cases

如果改动触达 codegen/explain, 还需要跑包含 `ORDER BY` / `LIMIT` 的 `tpc-h`、`tpc-ds` 局部 cases。

### 12.4 Plan-quality regression

基线使用:

- `logs/plan-quality/20260609-fe-nr-plan-diff/fe/tpc-ds__q41.out`
- `logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q41.out`
- `logs/plan-quality/20260609-fe-nr-plan-diff/fe/tpc-ds__q72.out`
- `logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q72.out`
- `logs/plan-quality/20260609-fe-nr-plan-diff/fe/tpc-h__q22.out`
- `logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-h__q22.out`

验收目标:

- `tpc-ds/q41`、`tpc-ds/q72` 的连续 `TOP-N` 收敛。
- `tpc-h/q22` 不新增 Sort/Gather。
- TopN/Sort count 向 FE plan 收敛, 但不要求完全同构。

## 13. Rollout

### Batch 1: compactness core

- 新增 proof helper 基础能力。
- 实现 consecutive TopN merge。
- 实现 redundant Sort under TopN elimination。
- 收敛 `q41/q72` 的双 TopN golden。
- 跑 unit tests + optimizer TopN compactness golden + sort TopN verify。

### Batch 2: pushdown candidates

- Project pushdown。
- Scan pruning hint 与 ordered top-k capability guard。
- Join pushdown conservative inner-join-only guard。
- Aggregate pushdown guard。
- `UNION ALL` branch TopN pruning。
- 增加 false-case golden, 确保 unsupported cases 不生成候选。

### Batch 3: property/enforcer/codegen polish

- 将 proof helper 接入 ordering satisfaction。
- 校准 Sort enforcer 和 TopN output property。
- 收敛 split TopN visible shape。
- 补 EXPLAIN/diagnostic signal。
- 重新跑 plan-quality diff, 记录 TopN/Sort count 变化。

每个 batch 都必须保持可编译、SQL verify 通过、rule 可通过 `disable_optimizer_rules` 回退。

## 14. Success Criteria

- `tpc-ds/q41` 和 `tpc-ds/q72` 不再出现同序连续 global `TOP-N`。
- `tpc-h/q22` 不因为 OQ-15 新增 Sort/Gather。
- 新增 TopN compactness optimizer golden 覆盖 merge、Sort elision、Scan/Project/Join/Aggregate/SetOp guard。
- `topn_null_order_limit_offset.sql` 等 order-sensitive case 结果不变。
- 所有 aggressive rewrites 都有 false-case 单测或 SQL golden。
- disable rule path 可以回退到未 compact 的候选形态。
