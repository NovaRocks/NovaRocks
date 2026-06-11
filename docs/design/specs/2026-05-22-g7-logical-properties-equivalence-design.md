# G7 — LogicalProperties 等价类与唯一列

**Date**: 2026-05-22
**Roadmap ref**: [SQL Layer Roadmap §2.4](2026-05-20-sql-layer-roadmap-design.md#24-g7--logicalproperty-equivalence-classes--unique-columns-p1) (G7)
**Status**: Spec — pending implementation plan
**Scope**: `src/sql/optimizer/` 的 logical property 推导与一个窄作用域 inner join 等价谓词消费者。不修改 FE-compatible lowering、fragment codegen、execution runtime、connector 或 storage 语义。

---

## §0 Goal

G7 第一版补齐 NovaRocks SQL optimizer 的 logical property 基座，并用一个最小消费者证明它不是死字段：

1. 在 `LogicalProperties` 中加入基于 `ColumnId` 的等价类与唯一列信息。
2. 从 `Filter` / `Inner Join` 等 logical operator 中保守推导等价类。
3. 新增一个只匹配 `INNER JOIN` 的 transformation rule，消费等价类做一跳 literal equality 派生：

```sql
l.k = r.k AND l.k = 10  =>  r.k = 10
r.k = l.k AND r.k = 10  =>  l.k = 10
```

这次 PR 的成功标准是：G7 属性结构落地、inner join 消费者产生可见计划变化、验证覆盖 ColumnId/alias 场景，并且不把 G2/G4/G5 或 OPT 系列工作混进来。

---

## §1 Non-goals

本阶段明确不做：

- OUTER / SEMI / ANTI join 的等价谓词派生。
- DISTINCT 消除、unique column 消费、join cardinality 基于唯一键的进一步收窄。
- 多跳复杂 rewrite，例如 `a = b AND b = c AND a = 10` 全量展开到每一列。
- RBO 字符串列名路径增强。G7 消费者必须基于 `ColumnId` 和 `LogicalProperties`。
- `HashDistribution` source type，即 G4。
- `LogicalPlan` 与 `Operator::Logical*Op` 合并，即 G2。
- Pattern-based rule framework，即 G5。

---

## §2 Current Baseline

当前 main 已有 G1 / G3：

- `ColumnId` 与 `ColumnRefFactory` 位于 `src/sql/column_id.rs`。
- `LogicalProperties` 位于 `src/sql/optimizer/memo.rs`，当前只有：

```rust
pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) row_count: f64,
}
```

- logical property 推导入口在 `src/sql/optimizer/stats.rs::derive_group_statistics`，它先 derives statistics，再调用 `derive_output_columns`。
- G3 已把 physical output / required properties 移到 `src/sql/optimizer/derive/` visitor；G7 不应回退到 `search.rs` 内部硬编码。
- transformation rule 当前通过 `Rule::apply(&MExpr, &mut Memo) -> Vec<NewExpr>` 运行，看不到 parent filter，只能消费当前 expression 与 child group properties。

这些约束决定 G7 第一版消费者应放在 CBO transformation 层，而不是 RBO 字符串列名 rewrite；同时，测试 SQL 需要选用能稳定进入 join condition 或 child filter 的形态。

---

## §3 Data Model

### §3.1 ColumnIdSet

新增 `ColumnIdSet`，建议放在 `src/sql/optimizer/property.rs`：

```rust
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ColumnIdSet {
    columns: Vec<ColumnId>,
}
```

约束：

- 内部 `columns` 始终排序、去重。
- `ColumnId::UNSET` 不允许进入集合。
- 提供 `new`, `single`, `from_iter`, `contains`, `iter`, `is_empty`, `union`, `intersects`, `is_subset` 等小型 API。
- 不暴露可变 `Vec`，避免调用方破坏排序去重 invariant。

使用排序 `Vec` 而不是裸 `HashSet`，原因是 EXPLAIN、debug 和 unit test 需要稳定顺序。

### §3.2 EquivalenceClasses

等价类需要支持合并闭包，建议引入一个薄封装：

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EquivalenceClasses {
    classes: Vec<ColumnIdSet>,
}
```

核心操作：

- `merge_pair(a, b)`：把两个 `ColumnId` 放进同一个等价类；若分别位于两个 class，则合并 class。
- `extend_from(other)`：合并 child classes。
- `class_containing(id)`：返回包含某列的 class。
- `normalize()`：按最小 `ColumnId` 排序 class，保证稳定输出。

第一版只需要 union-find 语义，不需要复杂图结构。

### §3.3 LogicalProperties

扩展为：

```rust
pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) row_count: f64,
    pub(crate) equivalence_classes: EquivalenceClasses,
    pub(crate) unique_columns: Vec<ColumnIdSet>,
}
```

`unique_columns` 使用 `Vec<ColumnIdSet>`，因为唯一约束可能是组合列。第一版只推导并保存，不作为优化消费者使用。

---

## §4 Logical Property Derivation

推导原则是宁可漏推，不可错推。所有跨作用域、NULL 保留或表达式派生不清楚的场景都保守清空或只继承本侧信息。

### §4.1 Scan

- `output_columns`：沿用 scan columns。
- `equivalence_classes`：空。
- `unique_columns`：不从 `TableDef` 猜测。当前 `TableDef` 没有通用 primary/unique key 字段，MV primary key 也不是普通表全局契约。

### §4.2 Filter

- 继承 child `equivalence_classes` 与 `unique_columns`。
- 从 top-level `AND` 中提取 `ColumnRef = ColumnRef` 与 `ColumnRef <=> ColumnRef`，调用 `merge_pair`。
- `ColumnRef = Literal` 不进入等价类；它作为消费者可读取的 constant binding。
- 非 equality、表达式 equality、含 `ColumnId::UNSET` 的 equality 全部跳过。

### §4.3 Inner Join

- 合并左右 child 的 `equivalence_classes`。
- 从 join condition 的 top-level `AND` 中提取左右 `ColumnRef = ColumnRef` / null-safe equality，合并到同一等价类。
- `unique_columns` 保守合并可确定仍唯一的 child unique keys；第一版不消费，若实现上存在疑义，可先只保留输出侧可见且不跨侧的 child unique keys。

### §4.4 Non-inner Join

非 inner join 不产生跨侧 equivalence：

- LEFT / RIGHT / FULL OUTER 的 NULL 产生侧不能与另一侧建立普通等价闭包。
- SEMI / ANTI 输出列范围与过滤语义不同，第一版不消费。
- 可以继承 surviving side 内部的 child equivalence，但不得从 join condition 合并左右 ColumnId。

### §4.5 Project / SubqueryAlias

- pass-through `ColumnRef` 保持原 `ColumnId`，可继承仍完全位于输出列集合中的等价类。
- 表达式派生列不自动与输入列等价，即使表达式文本看起来相同。
- `SubqueryAlias` 只改变 display boundary，不 mint 新 id，因此可按输出 `ColumnId` 过滤继承。

### §4.6 Aggregate / Distinct

- group-by 输出列组成一个 `unique_columns` key。
- scalar aggregate 的输出唯一性可以后续用专门 marker 表达，第一版不需要消费，不强行建模。
- `SELECT DISTINCT` 当前通过 aggregate 形态表达，沿用 aggregate 的 group-by unique key。
- aggregate 不继承 child equivalence，除非 group-by output 明确复用了原 `ColumnId` 且能证明所有列仍在输出中；第一版可保守只推 unique key。

### §4.7 Set Ops / Window / Repeat / TableFunction / CTE

- `Window`：pass-through base columns 可保守继承输出可见的 equivalence；window result 列不参与。
- `Union` / `Intersect` / `Except`：跨输入 column id 不同，第一版不推 equivalence。
- `Repeat`：ROLLUP/CUBE 会注入 NULL pattern，第一版清空或仅继承非 rollup key 的安全子集；若实现成本高，直接清空。
- `TableFunction`：保守继承 child 内部 equivalence；函数输出列不参与。
- `CTEProduce` / `CTEConsume` / `CTEAnchor`：只在 ColumnId 保持一致且输出可见时继承；否则清空，避免跨引用误合并。

---

## §5 Inner Join Consumer

新增 transformation rule，例如：

```rust
pub(crate) struct InnerJoinEquivalencePredicateRule;
```

注册位置：`src/sql/optimizer/rules/mod.rs::all_transformation_rules()` 中 join commutativity / associativity 之后、implementation 之前。

### §5.1 Match Conditions

规则只匹配：

- `Operator::LogicalJoin(j)`
- `j.join_type == JoinKind::Inner`
- `expr.children.len() == 2`
- 两个 child group 都有 `logical_props`
- join condition 或 child filter 中存在可读取的 literal equality

### §5.2 Predicate Sources

第一版读取两类来源：

1. join condition 内的 top-level conjuncts：

```sql
l.k = r.k AND l.k = 10
```

2. child 已经是 `LogicalFilter(l.k = 10)` 的形态，且 filter child 的输出列可判断为同一侧。

因为 `Rule::apply` 看不到 parent，不能直接读取 `Filter(Join)` 的 parent predicate。依赖现有 RBO 先把安全 predicate 合并进 join condition 或下推到 child。SQL golden 要选择能稳定触发这两类来源的查询。

### §5.3 Rewrite

对每个 join 等价列对 `(left_id, right_id)`：

- 如果已知 `left_id = literal`，生成 `right_id = literal`。
- 如果已知 `right_id = literal`，生成 `left_id = literal`。
- 新谓词必须只引用目标 child 输出列。
- 若目标 child 已经有语义相同 literal equality，不重复生成。
- 新谓词插入为目标 child 上方的 `LogicalFilter` group，而不是直接改 scan predicate。
- 一次 rule firing 只做一跳派生，避免 fixed-point 展开失控。

生成的新 join expression 保持原 join condition 不变，只替换对应 child group：

```text
Join(left, right)
  => Join(Filter(left, derived_left_pred), right)
  => Join(left, Filter(right, derived_right_pred))
```

如果左右都需要派生，可创建两个 filtered child group 并返回一个同时替换左右 child 的 `NewExpr`。也可以返回左右两个 alternatives；推荐同时替换，便于 SQL golden 稳定观察。

### §5.4 Dedup and Loop Guard

必须避免固定点循环：

- 生成前检查目标 child filter / scan predicates / join condition 是否已有同一 `ColumnId = same literal`。
- 派生谓词只由另一侧已有 literal equality 产生，不再读取本轮新生成的谓词进行二次扩展。
- 对同一 `(target_column_id, literal)` 只生成一次。

---

## §6 Testing

### §6.1 Unit Tests

新增或扩展 Rust unit tests：

- `ColumnIdSet` 排序、去重、拒绝 `ColumnId::UNSET`。
- `EquivalenceClasses::merge_pair` 能把 `c1=c2`、`c2=c3` 合成同一类。
- `Filter` logical property 从 `c1 = c2` 推出等价类。
- `Inner Join` 从左右 equi condition 推出跨侧等价类。
- LEFT / RIGHT / FULL / SEMI / ANTI join 不产生跨侧等价类。
- consumer rule 从 `l.k = r.k AND l.k = literal` 生成 opposite-side filter。
- consumer rule 对已有目标谓词不重复生成。
- alias 场景使用同一 `ColumnId`，不靠字符串列名匹配。

### §6.2 SQL Golden

新增 optimizer suite cases，例如：

```text
sql-tests/optimizer/sql/g7_equivalence_inner_join.sql
sql-tests/optimizer/sql/g7_equivalence_alias.sql
sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql
```

使用 `EXPLAIN VERBOSE` 与 `-- @explain_contains` / negative contains：

- `INNER JOIN` 上 `l.k = r.k AND l.k = 10` 能看到右侧 filter 或 scan predicate 包含 `r.k = 10`。
- 反向 `r.k = 10` 能补左侧。
- `LEFT JOIN` 类似条件不补 nullable side。
- alias / self-join-like 场景验证 ColumnId 路径，不因同名列误推。

SQL golden 不依赖完整计划文本，只断言关键 predicate 和 join/filter shape。

### §6.3 Build Gates

计划阶段和实现阶段的验证门槛：

```bash
cargo fmt --all -- --check
cargo test g7 -- --nocapture
cargo test logical_properties -- --nocapture
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --mode verify --only g7_equivalence_inner_join,g7_equivalence_alias,g7_equivalence_outer_join_guard
cargo build
```

具体 test filter 名称可在实现计划中按实际 unit test 命名调整，但必须保留 focused Rust tests + optimizer SQL golden + build gate。

---

## §7 Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| outer/semi/anti join 上错误派生谓词改变 NULL 保留语义 | high | 第一版 rule 只匹配 `JoinKind::Inner`；unit + SQL negative cases 覆盖 outer guard。 |
| transformation rule 看不到 parent filter，导致预期 SQL 不触发 | medium | 设计明确只消费 join condition 与 child filter；golden 选择稳定触发形态。 |
| 等价类推导把表达式派生列误当 pass-through | high | 只有 `ExprKind::ColumnRef` 且 ColumnId 非 UNSET 才参与 equivalence；表达式列不自动等价。 |
| fixed-point 循环重复插入同一谓词 | medium | 以 `(ColumnId, LiteralValue)` 做 dedup，生成前检查已有 predicate。 |
| `unique_columns` 建模过度但无消费者 | low | 第一版只保存保守信息；不以 unique columns 驱动 rewrite。 |
| RBO 字符串列名规则与 G7 ColumnId rule 行为不一致 | medium | G7 消费者只使用 Memo logical props；不增强 RBO 字符串路径。 |

---

## §8 Implementation Boundaries

建议拆分为三个实现任务：

1. **Data model + property derivation**
   - `ColumnIdSet`
   - `EquivalenceClasses`
   - `LogicalProperties` 字段扩展
   - `stats.rs::derive_group_statistics` 调用新的 logical property deriver

2. **Inner join consumer rule**
   - 新 transformation rule
   - literal equality collector
   - child filter group construction
   - dedup guard

3. **Regression tests**
   - property/rule unit tests
   - optimizer SQL golden
   - focused Cargo + SQL verification

如果实现中发现 transformation rule 需要 parent context 才能覆盖原定 SQL 形态，不扩大 scope 到 RBO 重写；先调整 golden 到 child filter / join condition 稳定来源，并把 parent-aware rewrite 留作 G7 follow-up 或 G5 pattern framework 之后处理。

---

## §9 PR Checklist

- 新增 optimizer property 数据结构全部基于 `ColumnId`，不新增字符串列身份判断。
- 所有等价类输出顺序稳定。
- 非 inner join 不产生跨侧 equivalence。
- 第一版消费者只生成 literal equality，一跳、可 dedup。
- SQL golden 能看到真实计划变化。
- `disable_optimizer_rules = 'InnerJoinEquivalencePredicateRule'` 能关闭新消费者；若 rule 名称不同，SQL/文档使用实际名称。
- PR description 明确说明 `unique_columns` 目前只落结构和保守推导，不消费。
