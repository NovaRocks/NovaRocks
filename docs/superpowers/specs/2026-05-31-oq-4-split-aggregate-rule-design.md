# OQ-4: SplitAggregateRule 两阶段聚合 — 设计

Date: 2026-05-31
Tasks:
- OQ-4 in [Optimizer Plan Quality Roadmap](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/NovaRocks%20Roadmap.md#optimizer-plan-quality-roadmap)
Predecessor: OQ-3 / OQ-3.1 predicate-aware cardinality（PR #218，当前 `origin/main`）
Successor: OQ-5 runtime filter optimizer wiring
Scope: 普通非 DISTINCT 聚合的 SplitAgg 规则化、CBO 选择、plan golden 与 StarRocks 对齐验收

---

## 1. 目标

把 NovaRocks standalone optimizer 的普通聚合从“实现规则里临时生成 Local/Global 物理链”提升为显式的 Cascades transformation：`SplitAggregateRule` 在 memo explore 阶段为符合条件的 `LogicalAggregate` 生成 `Global -> Local -> child` 逻辑备选，后续 implementation rule 只负责将各 stage 降到 `PhysicalHashAggregate`。

完成时：

- 普通非 DISTINCT 聚合（包含有 `GROUP BY` 与 scalar aggregate）可产生 StarRocks 风格两阶段 plan：`LOCAL update -> exchange/enforcer -> GLOBAL merge/finalize`。
- 原始 Single 聚合仍作为 memo 备选存在，由 CBO search 基于 OQ-3 的 cardinality 与 exchange cost 选择 Single 或 Split。
- `EXPLAIN VERBOSE/COSTS` 可稳定看到 `HASH AGGREGATE (LOCAL)`、`HASH EXCHANGE`（`ShuffleAgg` 或 `Gather`）、`HASH AGGREGATE (GLOBAL)`。
- `disable_optimizer_rules = 'SplitAggregateRule'` 可关闭该 transformation 并回退 Single。
- optimizer golden 与标杆 plan 对比锁住 OQ-4 的 plan shape，避免后续 OQ-5/OQ-6 回退。

---

## 2. 当前状态与问题

当前 `origin/main` 已包含 OQ-3/OQ-3.1，并且已经有一部分 SplitAgg 基础设施：

- `AggMode::{Single, Local, Global, DistinctGlobal, DistinctLocal}` 已存在。
- `PhysicalHashAggregateOp` 已有 `mode` 与 `is_merge`，codegen 能在 Global 阶段编译 merge aggregate call。
- `derive/hash_aggregate.rs` 已能让 Global 要求 `DistributionSpec::shuffle_agg(...)`，scalar Global 要求 `Gather`。
- `AggToHashAgg` 当前会直接为非 DISTINCT 且有 `GROUP BY` 的 `LogicalAggregate` 生成两个 physical alternatives：Single 与 Local->Global。

主要问题：

1. **Split 逻辑藏在 implementation rule。** `AggToHashAgg` 同时负责 logical-to-physical lowering 与 split 枚举，边界不清晰，也难以单独 disable / 测试 / 对齐 StarRocks `SplitAggregateRule`。
2. **scalar agg 被跳过。** Roadmap 标杆 `join_one_key` q22 是 scalar `count(...)`，当前 `op.group_by.is_empty()` 直接返回 Single，无法完整完成 OQ-4。
3. **cost gate 不透明。** Local/Global 与 Single 的竞争只依赖现有 cost 常数，缺少针对 SplitAgg 的验收与回归用例。
4. **golden 不足。** 现有 optimizer golden 覆盖 aggregate pushdown，但没有专门锁住普通 SplitAgg 的 grouped/scalar plan shape、disable 回退和 StarRocks 对齐差异。

---

## 3. 非目标

- 不在本次扩展 DISTINCT 聚合的语义。现有 `SplitDistinctAgg` 保留；本任务只确保普通 SplitAgg 不破坏 distinct fallback / 既有 distinct 多阶段路径。
- 不实现 ordered aggregate 的新多阶段语义。带 `ORDER BY` 的 aggregate 若 merge/update 输入保序语义不明确，应保守留 Single 或走既有安全路径。
- 不重写执行层 aggregate operator。OQ-4 只改 optimizer rule、properties/cost、codegen stage 映射的必要边界。
- 不改变 FE-compatible thrift plan lowering；本任务只服务 standalone SQL optimizer。

---

## 4. 架构设计

### 4.1 引入逻辑 aggregate stage

给 `LogicalAggregateOp` 增加逻辑 stage 信息，表达该 logical aggregate 在多阶段聚合链中的角色：

```rust
pub(crate) enum AggStage {
    Single,
    Local,
    Global,
}

pub(crate) struct LogicalAggregateOp {
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    pub stage: AggStage,
    pub is_merge: Vec<bool>,
    pub is_split: bool,
}
```

设计要点：

- `convert::logical_plan_to_memo` 创建的原始 aggregate 为 `stage = Single`、`is_merge = false...`、`is_split = false`。
- `is_split` 防止 `SplitAggregateRule` 对自己的 Global/Local 输出再次拆分。
- `is_merge` 在 logical 层记录 Global 阶段每个 aggregate 是否读取 Local intermediate state；implementation lowering 原样传给 `PhysicalHashAggregateOp`。
- 若实现时希望避免引入新 enum，也可以复用现有 `AggMode`；但推荐新增 `AggStage`，避免把物理执行决策名字泄漏到 logical operator。

### 4.2 新增 `SplitAggregateRule`

新增 `src/sql/optimizer/cascades_rules/split_aggregate.rs`，注册到 `all_transformation_rules()`，rule name 固定为 `SplitAggregateRule`。

匹配条件：

- operator 是 `LogicalAggregate`。
- `stage == Single && !is_split`。
- aggregate calls 非空或 group-by-only 均可；但所有 aggregate call 必须是非 DISTINCT。
- ordered aggregate 默认不拆，除非确认对应 merge function 保留语义。
- 函数名不在 sketch/bitmap 等当前 merge 语义敏感名单中；这些函数保守走 Single。

输出：

- 保留原始 Single logical expression 作为当前 group 的既有表达式。
- 增加一个新的 logical expression：`LogicalAggregate(Global)`，其 child 是一个新 memo group，包含 `LogicalAggregate(Local)`。

Grouped aggregate：

```text
LogicalAggregate(Global, group_by = refs_to_local_group_key_outputs, is_merge = true...)
  LogicalAggregate(Local, group_by = original_group_by, is_merge = false...)
    child
```

Scalar aggregate：

```text
LogicalAggregate(Global, group_by = [], is_merge = true...)
  LogicalAggregate(Local, group_by = [], is_merge = false...)
    child
```

Global group key 引用必须读取 Local 输出列，而不是重新读取原始 child 表达式。可复用/搬移当前 `aggregate_group_key_output_ref(...)` 逻辑，保证 derived group-by expr 的 `ColumnId` 与 Local 输出一致，避免 distribution property 和 codegen scope 脱节。

### 4.3 简化 `AggToHashAgg`

`AggToHashAgg` 改为纯 lowering：

- `LogicalAggregate(stage = Single)` -> `PhysicalHashAggregate(mode = Single)`。
- `LogicalAggregate(stage = Local)` -> `PhysicalHashAggregate(mode = Local)`。
- `LogicalAggregate(stage = Global)` -> `PhysicalHashAggregate(mode = Global)`。

它不再创建 Local child group，也不再枚举 Split 备选。这样：

- split 是否启用由 `SplitAggregateRule` 控制。
- `disable_optimizer_rules = 'SplitAggregateRule'` 可以稳定回退 Single。
- implementation phase 的职责与其它 logical-to-physical rule 保持一致。

### 4.4 Distribution 与 fragment 边界

沿用现有 property 设计：

- Local 输出按 group key 派生 `DistributionSpec::shuffle_agg(group_keys)`；**Local scalar aggregate 不能宣称 `Gather`**，应输出 `Any`/child-preserving distribution，让上层 Global 的 `Gather` requirement 触发 enforcer。Single/Global scalar 的最终输出才是 `Gather`。
- Global 对 child 要求：
  - grouped：`DistributionSpec::shuffle_agg(global_group_by_column_ids)`。
  - scalar：`Gather`。
- search 自动插入 `PhysicalDistribution` enforcer；fragment builder 在 distribution node 处切 fragment。

root-level `Gather` elision 不应吞掉 scalar split 中 Local->Global 之间的 Gather，因为该 Gather 是 Global child requirement，不是 root wrapper。当前 `derive/hash_aggregate.rs` 对空 group key 统一返回 `Gather`，实现时需要按 stage 拆开，避免 Local scalar 被误判为已全局汇总。

### 4.5 Cost gate

SplitAgg 不做硬编码强制选中。CBO search 同时拥有 Single 与 Split alternatives：

- Single cost：读取 child 后单阶段聚合。
- Split cost：Local cost + enforcer/exchange cost + Global cost。
- OQ-3 的 post-filter / post-join cardinality 作为 child stats 输入，影响 Split 是否值得。

实现时需要校准 `compute_cost(PhysicalHashAggregate)` 的系数与 exchange cost，至少满足：

- 大输入、低 group NDV：Split 优于 Single。
- 小输入或高 group NDV：Single 可以胜出。
- scalar aggregate 在大输入上应倾向 Local->Gather->Global，以对齐 StarRocks partial update + merge finalize。

若当前 cost model 缺少 exchange cost 或始终偏向某一边，本任务应补 targeted unit tests 并调整常数；不要用“所有普通聚合都强制 split”的方式掩盖 cost 缺口。

---

## 5. StarRocks 参考对齐

PR 描述与实现注释应引用：

- `~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/SplitAggregateRule.java`
  - `createNormalAgg(...)`
  - `getIntermediateType(...)`
  - distinct suitability guard（本任务只引用非目标边界）
- `~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/SplitTwoPhaseAggRule.java`
  - `check(...)`: global aggregate、未 split、可生成 multi-stage aggregate
  - `transform(...)`: Local + Global 两阶段结构

NovaRocks 与 StarRocks 的明确差异：

- NovaRocks 本任务用 Cascades transformation + CBO alternatives 表达 Split，而不是 StarRocks 完全同构的 Java operator builder。
- DISTINCT 多阶段已有独立历史实现，本任务不把它并入普通 `SplitAggregateRule`。
- session variable 不完整复刻 StarRocks；NovaRocks 使用现有 `disable_optimizer_rules` 作为开关。

---

## 6. 测试设计

### 6.1 Unit tests

新增或调整 optimizer unit tests：

1. `SplitAggregateRule` 对 grouped aggregate 生成 `Global -> Local` logical alternative。
2. `SplitAggregateRule` 对 scalar aggregate 生成 `Global -> Local` logical alternative。
3. rule 不匹配 `stage != Single` 或 `is_split = true` 的 aggregate，避免循环拆分。
4. DISTINCT / ordered / sensitive aggregate 不被普通 SplitAggregateRule 拆分。
5. `AggToHashAgg` 对 `Single/Local/Global` 只做对应 physical lowering，不再自行创建 split chain。
6. cost/search 测试覆盖大输入低 NDV 选择 Split、小输入或高 NDV 可选择 Single。

### 6.2 SQL golden

在 `sql-tests/optimizer/` 增加 golden：

- `split_aggregate_grouped.sql`
  - `SELECT k, SUM(v) FROM ... GROUP BY k`
  - `-- @explain_contains=HASH AGGREGATE (LOCAL`
  - `-- @explain_contains=HASH EXCHANGE`
  - `-- @explain_contains=source ShuffleAgg`
  - `-- @explain_contains=HASH AGGREGATE (GLOBAL`
- `split_aggregate_scalar.sql`
  - `SELECT COUNT(*), SUM(v) FROM ...`
  - `-- @explain_contains=HASH AGGREGATE (LOCAL`
  - `-- @explain_contains=HASH AGGREGATE (GLOBAL`
  - exchange/gather shape 以实际 EXPLAIN 输出锁定。
- `split_aggregate_disabled.sql`
  - baseline 有 Local/Global。
  - `SET disable_optimizer_rules = 'SplitAggregateRule'` 后只剩 Single。
- negative case：
  - DISTINCT 或 ordered aggregate 不被普通 SplitAggregateRule 误拆。

现有 `aggregate_pushdown_*` golden 需要复核：AggregatePushdown 会在 join 下方插 partial aggregate；OQ-4 不应把 `already_pushed` 相关语义弄乱，也不应反复 split 自己的输出。

### 6.3 标杆验收

按 Roadmap PR checklist 跑并记录：

1. `join_one_key` q22：scalar `count(...)` 应出现 partial update + merge finalize 对应结构（fragment 数可与 StarRocks 不同）。
2. `join_linear_chained` q31：确认 SplitAgg 不破坏 OQ-1/OQ-2/OQ-3 的 column pruning、NULL filter、cardinality。
3. 一个简单 INNER `count(*)`：验证 scalar split 与 StarRocks plan shape 收敛。

每条都保存 NovaRocks 与 StarRocks `EXPLAIN VERBOSE` / `EXPLAIN COSTS` 关键片段到 PR 描述。

---

## 7. 风险与应对

- **LogicalAggregateOp 字段扩展影响面较大。** `convert`、stats/logical props、explain、tests、Debug equality 都可能受影响。应先做 mechanical compile fix，再跑 optimizer unit tests。
- **Local output column metadata 与 intermediate type 不完全一致。** 当前 physical codegen 已在 Local 阶段用 aggregate intermediate type 建 slot；logical stats/display 仍可能看到 final output type。实现时若发现不一致影响 Global compile，应为 Local 生成明确 intermediate output metadata，不能靠 silent fallback。
- **scalar split 的 Gather 可能被错误 elide。** 需要 SQL golden 锁住 Local 与 Global 之间确实有 required distribution/enforcer。
- **cost 可能总是选 Single 或总是选 Split。** 必须用 targeted cost/search tests 校准；不要为了 golden 临时强制。
- **与 SplitDistinctAgg 规则交互。** 普通 SplitAggregateRule 要明确跳过 DISTINCT，避免和现有 distinct implementation rule 竞争出不一致的 physical chain。
- **AggregatePushdown 已有 `already_pushed` 语义。** OQ-4 的 logical stage/is_split 必须与该标记兼容，避免被 pushdown collector 当成新候选反复处理。

---

## 8. 大致实现顺序（粗略；详细分解交给 writing-plans）

1. 增加 `AggStage` / `LogicalAggregateOp` 字段，并让 convert、stats、logical props、Debug/unit tests 编译通过。
2. 新增 `SplitAggregateRule` transformation，先覆盖 grouped + scalar 普通聚合，带循环 guard 与 negative guards。
3. 把 `AggToHashAgg` 简化为 stage-to-physical lowering，移除内部 Local group 构造。
4. 校准 derive/cost/search：确保 grouped 使用 `ShuffleAgg`，scalar 使用 `Gather`，CBO 能在 Single/Split 间选择。
5. 更新 `is_known_rule_name` 覆盖与 disable rule 行为测试。
6. 增加 optimizer unit tests 与 SQL golden。
7. 跑 Roadmap 三条标杆 plan diff，更新 PR 描述与 Roadmap 进度。

---

## 9. Definition of Done

- `SplitAggregateRule` 是独立 transformation rule，并可被 `disable_optimizer_rules` 单独关闭。
- 普通 grouped 与 scalar aggregate 都能产生 Local->Global logical/physical split 备选。
- `AggToHashAgg` 不再承担 split 枚举职责。
- CBO search 基于成本选择 Single 或 Split；大输入低 NDV / scalar count 的标杆能选 Split。
- optimizer golden 覆盖 grouped split、scalar split、disable fallback、distinct/ordered negative。
- `join_one_key` q22、`join_linear_chained` q31、简单 INNER `count(*)` 的 NovaRocks plan 在 SplitAgg 关注维度上与 StarRocks 收敛。
