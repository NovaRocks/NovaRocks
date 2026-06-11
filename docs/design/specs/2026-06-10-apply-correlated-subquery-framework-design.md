# Apply / CorrelatedSubquery 体系设计（standalone optimizer）

- 日期：2026-06-10
- 状态：设计稿，待确认后进入 implementation plan
- 关联 roadmap：Optimizer Plan Quality Roadmap · OQ-13（Subquery decorrelation and analytic rewrite parity）
- StarRocks 参考基线：`~/project/starrocks`，HEAD `84bb94c5`（2026-06-09，upstream main 的 Apply / subquery rewrite 机制）

---

## 1. 背景与目标

当前 NovaRocks standalone 路径没有 LogicalApply / PhysicalApply。子查询在 **analyzer 阶段**被
`src/sql/analyzer/subquery_rewrite.rs` 立即改写成 JOIN / aggregate / semi-anti join，planner 与
optimizer 只能看到普通 `LogicalJoin` / `LogicalAggregate`。「这是一个 correlated scalar subquery /
EXISTS / IN / NOT IN」的原始语义在进入 optimizer 之前就丢失了。

这带来两类问题：

1. **架构问题**：OQ-13 要做的 analytic/window parity（scalar correlated aggregate → window、
   top/rank per group → window）需要在 optimizer 里识别「correlated scalar aggregate subquery」
   这一语义单元。在 join-heavy plan 上反推意图既脆弱又会误伤用户手写的 join。
2. **语义缺陷**：analyzer 即时 rewrite 为了「一步到位」省掉了若干 SQL 标准语义 guard（详见 §2.3），
   其中 scalar subquery 多行不报错是正确性问题。

本设计的目标：

- analyzer 只负责**语义绑定与子查询元信息收集**；
- planner 把子查询表示为 **`LogicalPlan::Apply`** 节点；
- optimizer 在专用 rewrite stage 完成 **decorrelation 与形态选择**（join / aggregate / window）；
- 完整保留 SQL 语义：scalar 多行错误、empty input、NULL correlation key、NOT IN null-aware；
- 为 OQ-13 的 `ApplyToWindow`（WinMagic）提供直接挂载点；
- 不为任何单个 TPC 查询硬编码 rewrite。

---

## 2. 现状：analyzer 即时 rewrite 链路

### 2.1 链路与数据结构

1. `src/sql/analyzer/resolve_expr.rs` 在表达式分析中遇到子查询时生成
   `ExprKind::SubqueryPlaceholder { id, kind, data_type }`（`src/sql/analysis/mod.rs:392`），并把
   `SubqueryInfo { id, kind, subquery: Box<sqlparser::ast::Query>, data_type, in_expr }`
   （`analysis/mod.rs:425`）压入 `AnalyzerContext::collected_subqueries`。
   `SubqueryKind` 仅三种：`Scalar` / `Exists { negated }` / `InSubquery { negated }`
   （`analysis/mod.rs:413`）；ANY / SOME / ALL 在 `resolve_expr.rs:713` 的 catch-all 中直接报错。
2. `analyze_select` 末尾（`src/sql/analyzer/mod.rs:557`）调用 `rewrite_subqueries`
   （`subquery_rewrite.rs:63`），**在 `ResolvedSelect` 层**把每个子查询改写为 join：

   | 形态 | uncorrelated | correlated |
   |---|---|---|
   | EXISTS | LEFT OUTER JOIN ON true + 子查询改 `SELECT 1 LIMIT 1`，placeholder → `IS NOT NULL` | LEFT SEMI JOIN，子查询 WHERE 整体提入 ON |
   | NOT EXISTS | 同上，placeholder → `IS NULL` | LEFT ANTI JOIN |
   | IN | LEFT SEMI JOIN on `Eq`；OR / projection 位置走 LEFT OUTER + DISTINCT + indicator 列 | LEFT SEMI JOIN，eq + 子查询 WHERE 提入 ON |
   | NOT IN | LEFT ANTI；任一侧 nullable → `NullAwareLeftAnti`；OR / projection 走 indicator + CASE | 同 nullable 规则；nullable residual 包 `coalesce(..., false)` |
   | Scalar | CROSS JOIN + ColumnRef 替换 | LEFT OUTER JOIN；correlation key 追加进子查询 GROUP BY |

3. correlation 在 rewrite 时**重新检测**（`CorrelationPred`，`subquery_rewrite.rs:2142`）：
   只识别二元比较（`Eq/Ne/Lt/Le/Gt/Ge`）中一侧 outer-only 的形态；EXISTS/IN 另有
   `expr_references_outer_scope` 兜底，scalar 没有兜底。
4. planner / optimizer 之后只见普通 join 树；codegen 对残留 placeholder 报错
   （`expr_compiler.rs:807`：`unexpected SubqueryPlaceholder (id={id}) in expression compilation;
   subquery rewriting may have failed`；`id_binding_verifier.rs:406` 二次兜底）。

### 2.2 架构问题

- **语义过早消解**。optimizer 无法区分「correlated scalar aggregate 改写出的 LEFT OUTER JOIN +
  GROUP BY」与用户手写的同形 join，OQ-13 的 window rewrite 没有可靠匹配入口。
- **rewrite 工作在 AST/ResolvedSelect 层**。`SubqueryInfo.subquery` 是裸 sqlparser AST，rewrite
  过程要反复 re-analyze（`infer_scalar_subquery_data_type` 还要 snapshot/restore
  `collected_subqueries`，`resolve_expr.rs:802-824` 注释明确记录了这个 drain 危险）；改写靠字符串
  qualifier、`format!("{:?}")` 结构判等（`exprs_structurally_equal`，`subquery_rewrite.rs:3609`），
  无法利用 ColumnId 体系。
- **无规则化、无开关、无观测**。3700+ 行手写改写逻辑没有 rule 粒度的 disable、trace、plan golden
  入口；`disable_optimizer_rules` 对它完全无效。
- **位置覆盖靠枚举分支**。WHERE / HAVING / projection / JOIN-ON 各一套手写路径（JOIN-ON 路径
  700+ 行且注释已过期），GROUP BY / ORDER BY 位置的 placeholder 直接泄漏到 codegen 报错。

### 2.3 现有语义缺陷清单（设计动机的一部分）

| # | 缺陷 | 位置 | 后果 |
|---|---|---|---|
| D1 | scalar subquery 完全没有「至多 1 行」guard：uncorrelated 是裸 CROSS JOIN；correlated 靠 GROUP BY correlation key 隐式塌缩，且不校验投影是聚合 | `subquery_rewrite.rs:1670`、`:1977-2070` | 多行时静默复制 outer 行（正确性错误） |
| D2 | NOT IN 在 OR / projection 位置的 indicator 形态不处理 build 侧 NULL | `subquery_rewrite.rs:1364-1390` | 应为 UNKNOWN 的行被判 TRUE（静默错误） |
| D3 | JOIN-ON 位置的 NOT IN indicator 形态既不处理 probe NULL 也不处理 build NULL | `subquery_rewrite.rs:592-607`、`:307-322` | 同上 |
| D4 | correlated EXISTS 出现在 SELECT list 时 placeholder 不被替换 | `rewrite_exists` 只删 filter/having | codegen 报 placeholder 错误（报错但信息误导） |
| D5 | GROUP BY / ORDER BY 中的子查询 placeholder 不被改写 | `rewrite_subqueries` 不遍历这两处 | codegen 报 placeholder 错误 |
| D6 | 非等值 correlated scalar（如 `inner.k < outer.k`）被接受，GROUP BY 该 key 后 LEFT OUTER JOIN 可匹配多 key | `extract_correlation_predicates` 接受 6 种比较符 | outer 行静默重复 |
| D7 | scalar 子查询的 correlation 检测无兜底：检测不到的 outer 引用按 uncorrelated 处理 | `subquery_rewrite.rs:1546+` | codegen 列解析错误（报错但信息误导） |

D1/D6 是新框架第一阶段直接修复项；D2/D3 在 value-form 迁移阶段修复；D4/D5/D7 至少升级为
analyzer 阶段的明确报错。

---

## 3. StarRocks 参考架构（调研摘要）

> 详细出处：`fe/fe-core/src/main/java/com/starrocks/sql/optimizer/`，行号基于 `84bb94c5`。

### 3.1 LogicalApplyOperator 字段

```java
ScalarOperator subqueryOperator;       // Apply 由哪个表达式构造（InPredicate/Exists/inner 输出列）
ColumnRefOperator output;              // 代表子查询取值的新列
List<ColumnRefOperator> correlationColumnRefs;   // 子查询内部引用的 outer 列
ScalarOperator correlationConjuncts;   // push-down 阶段从 inner Filter 提上来的相关谓词
boolean needCheckMaxRows;              // scalar 子查询是否仍需「至多 1 行」检查
boolean useSemiAnti;                   // 是否可以改写成 semi/anti join（WHERE 顶层 AND 位置）
boolean needOutputRightChildColumns;   // 仅 lateral join 用
ColumnRefSet unCorrelationSubqueryPredicateColumns;  // 同谓词中的 outer 兄弟列（驱动左下推）
// 基类 Operator.predicate 复用为 inner 提上来的 uncorrelated residual
```

kind 由 `subqueryOperator` 的类型判定（`isQuantified` = In/MultiIn、`isExistential` = Exists、
其余 `isScalar`）。`useSemiAnti` 只在 WHERE/HAVING/JOIN-ON 顶层 AND conjunct 为 true；OR / NOT /
任何嵌套表达式位置经 `Context.clone` 降为 false（`SqlToScalarOperatorTranslator.java:252-289`）。

### 3.2 规则管线（`QueryOptimizer.java:543-550`，在谓词下推与 CBO 之前）

```java
rewriteIterative(PUSH_DOWN_SUBQUERY_RULES);            // MergeApplyWithTableFunction,
                                                       // PushDownApplyLeftProjectRule, PushDownApplyLeftRule
rewriteIterative(SUBQUERY_EXTRACT_CORRELATION_PREDICATE_RULES);
                                                       // PushDownApplyProject/Filter/AggFilter/AggProjectFilter
rewriteIterative(SUBQUERY_REWRITE_TO_WINDOW_RULES);    // ScalarApply2AnalyticRule (WinMagic)
rewriteOnce(ExtractRangePredicateFromScalarApplyRule); // 分区裁剪辅助
rewriteIterative(SUBQUERY_REWRITE_TO_JOIN_RULES);      // Quantified/Existential/ScalarApply2JoinRule,
                                                       // Existential/QuantifiedApply2OuterJoinRule
rewriteOnce(ApplyExceptionRule);                       // 残留 Apply → "Not support the subquery!"
```

要点：

- **`PushDownApplyFilterRule` 是 decorrelation 的轴心**：把 inner Filter 的 conjunct 按是否引用
  `correlationColumnRefs` 拆成 `correlationConjuncts` 与 residual `predicate`，之后
  `containsCorrelationSubquery` 变 false，to-window / to-join 规则才允许匹配。
- **`PushDownApplyAggFilterRule`** 把 correlated scalar aggregate 归一化成「按 correlation key 的
  vector agg」，同时 `setNeedCheckMaxRows(false)`（group-by key 保证每 key ≤ 1 行）。
- **scalar 多行 guard 双形态**（`ScalarApply2JoinRule`）：uncorrelated →
  `LogicalAssertOneRowOperator(LE 1)`；correlated 非 agg 形态 → `GROUP BY corr key` +
  `count(1) as countRows, any_value(expr)` + 投影 `assert_true(countRows IS NULL OR countRows <= 1,
  'correlate scalar subquery result must 1 row')`。
- **`ScalarApplyNormalizeCountRule`**（ScalarApply2JoinRule 的 predecessor）：把被引用的
  `count(...)` 输出包成 `ifnull(count, 0)`，修正 decorrelation 后「无行 → NULL」与原语义
  「count 空输入 → 0」的偏差。
- **NOT IN 永远产 `NULL_AWARE_LEFT_ANTI_JOIN`**（`QuantifiedApply2JoinRule`；代码内 TODO 提到
  可在 null-rejecting 时降级 plain anti，但未做）。
- **`ScalarApply2AnalyticRule`（WinMagic，OQ-13 核心参考）**：匹配
  `Filter → Project → Apply(LEAF, Agg)`，条件（全部满足才改写）：
  1. 聚合函数 ∈ {count, sum, avg, min, max} 且非 DISTINCT，子查询内仅一个 Agg；
  2. 子查询与 outer block 内都无 limit，算子白名单 {Scan, Join(仅 cross), Filter, Project, Agg}；
  3. outer 表集合 = 子查询表集合 + 恰好 1 张表（按 catalog Table id 同一性，拒绝自连接/重复表）；
  4. 谓词同一性：每个 correlation conjunct 在 outer conjuncts 中有结构相同的孪生；
     子查询 Filter 的其余 conjunct 与 outer 剩余 conjunct 一一对应。
  输出：丢弃整个子查询子树，改写为
  `Project → Filter(子查询比较谓词, agg 替换为 window 列) → Window(AGG OVER (PARTITION BY 相关键))
  → Filter(其余 outer 谓词) → outer 子树`。无独立 session 开关，仅 `cbo_disabled_rules` 通用开关；
  to-window 排在 to-join 之前，无 cost 比较，命中即赢。
- **StarRocks 也没有 PhysicalApply**：所有 Apply 必须在 rewrite 阶段消除，唯一例外 lateral table
  function 由 `MergeApplyWithTableFunction` 吸收。这直接佐证本设计第一阶段「无 PhysicalApply、
  decorrelate-or-fail」的决策。

### 3.3 翻译期硬错误（值得对齐的措辞）

`"Scalar subquery should output one column"`、
`"Unsupported correlated in predicate subquery with grouping or aggregation"`、
`"Not support Non-EQ correlated predicate in correlated subquery"`、
`"Not support without correlated predicate in correlated subquery"`、
`"Not support the subquery!"`（ApplyExceptionRule）等，见调研报告 §9。

---

## 4. 方案对比

### 方案 A：保留 analyzer rewrite，在 optimizer 反推子查询意图（否决）

在 join-heavy plan 上模式匹配「LEFT OUTER JOIN + GROUP BY correlation key」反推 scalar
subquery。问题：与用户手写 join 不可区分（误改写风险）；NOT IN / EXISTS 的 indicator 形态更难
反推；每条 OQ-13 规则都要重复一套脆弱的形态识别；§2.3 的语义缺陷一个都修不掉。

### 方案 B：引入 LogicalApply，optimizer 统一 decorrelation（**推荐**）

对齐 StarRocks：analyzer 绑定语义并收集元信息，planner 产 `LogicalPlan::Apply`，optimizer 在
**rewrite pipeline 最前面的专用 stage** 内完成 push-down 归一化 → to-window → to-join →
ApplyException。Apply 的生命周期被限制在这一个 stage 内：后续所有 rewrite stage、memo/cascades、
codegen 永远见不到 Apply（§5.5）。改造面可控（§5.6），OQ-13 获得一等入口，D1/D6 顺带修复。

### 方案 C：同时引入 PhysicalApply（逐行执行的 correlated nested loop）（第一阶段不做）

能兜住任意无法 decorrelate 的形态，但执行语义是 O(outer rows) 次子查询执行，需要全新 operator、
取消/内存语义，且 StarRocks 至今没有它也覆盖了生产负载。保留为长期可选扩展：若未来出现真实
需求（如任意非等值 correlation），在 Apply 框架上加 implementation rule 即可，不影响本设计。

**决策：方案 B。第一阶段所有 Apply 必须在 SubqueryRewrite stage 内被消除，否则显式报错（§6.4）。**

---

## 5. 新 logical 表达设计

### 5.1 `LogicalPlan::Apply` 节点

```rust
// src/sql/planner/plan.rs

/// What the subquery expression looks like to its enclosing clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

pub(crate) struct ApplyNode {
    /// Outer plan (child 0).
    pub left: Box<LogicalPlan>,
    /// Subquery plan (child 1). May reference outer columns by ColumnId
    /// while the Apply is alive (see §5.5 invariants).
    pub right: Box<LogicalPlan>,
    pub kind: ApplyKind,
    /// The expression the Apply was built from, written over the inner
    /// plan's output columns: `lhs IN (inner_col)` / `EXISTS(inner_col)` /
    /// bare `ColumnRef(inner_col)` for scalar.
    pub subquery_expr: TypedExpr,
    /// Fresh column standing in for the subquery's value in outer expressions.
    pub output_column: OutputColumn,
    /// Outer-side columns referenced inside the subquery.
    pub correlation_column_ids: Vec<ColumnId>,
    /// Correlated conjuncts hoisted out of the inner plan by
    /// PushDownApplyFilter (empty at construction).
    pub correlation_conjuncts: Vec<TypedExpr>,
    /// Uncorrelated residual predicate hoisted out of the inner plan.
    pub residual_predicate: Option<TypedExpr>,
    /// Scalar only: subquery must still be runtime-checked to yield <= 1 row.
    pub need_check_max_rows: bool,
    /// True iff the subquery sits as a top-level AND conjunct of
    /// WHERE / HAVING / JOIN-ON, so it may collapse into a semi/anti join.
    pub use_semi_anti: bool,
    /// For uncorrelated scalar subqueries used inside a predicate: the outer
    /// sibling columns of that predicate (drives PushDownApplyLeft*).
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
```

与 StarRocks 的差异及理由：

- **`kind` 用显式枚举**而非 `subqueryOperator instanceof ...`：Rust 侧 `TypedExpr` 没有谓词类型
  层级，显式 kind 更直接；`subquery_expr` 仍然保留完整表达式供 to-join 规则展开。
- **不设 `needOutputRightChildColumns`**：NovaRocks 的 lateral/UNNEST 已有
  `TableFunctionNode` 专用路径，不经 Apply。
- **不设 MultiIn**：multi-column IN 第一阶段继续走 legacy（§7 路由表）；后续扩展时给
  `ApplyKind::In` 增加多列形态而不是新 kind。
- **correlation 用 `ColumnId` 集合**：NovaRocks 的 `ColumnId` 全局唯一（`ColumnRefFactory` 单一
  铸造），inner plan 中的 outer 引用天然可以表达为普通 `ColumnRef`，无需 StarRocks 式
  ExpressionMapping 桥接。

### 5.2 语义保留矩阵

| 语义 | 机制 | 状态 |
|---|---|---|
| scalar 多行错误（uncorrelated） | 新 `LogicalPlan::AssertOneRow` → codegen 发 `ASSERT_NUM_ROWS_NODE`（LE 1, 带 `subquery_string`） | exec 算子 `assert_num_rows_processor.rs` + lowering `lower/node/assert.rs` 已存在且 FE-compat 验证过，仅缺 standalone codegen 发射（`fragment_builder` 一个 visit arm） |
| scalar 多行错误（correlated，子查询非 scalar-agg 形态） | `GROUP BY corr key` + `count(1)`/`any_value(expr)` + 投影 `assert_true(count_rows IS NULL OR count_rows <= 1, '...')` | `assert_true`（`exec/expr/function/conditional`）、`any_value`（`exec/expr/agg/functions/any_value.rs`）均已存在 |
| scalar 多行（correlated，scalar-agg 形态） | `PushDownApplyAggFilter` 归一化后置 `need_check_max_rows = false`，免 guard | 规则行为，无新执行件 |
| empty input → NULL | LEFT OUTER JOIN 补 NULL 行；聚合算子 finalize 已实现「无 GROUP BY 空输入 → 1 行默认值；有 GROUP BY 空输入 → 0 行」（`exec/operators/aggregate/mod.rs:640-682`） | 已存在 |
| count 空输入 → 0 | `NormalizeCountScalarApply` 规则：被引用的 `count` 输出包 `ifnull(count, 0)`（对应 StarRocks `ScalarApplyNormalizeCountRule`） | `ifnull` 已存在 |
| NOT IN NULL 语义 | `QuantifiedApplyToJoin` 产 `JoinKind::NullAwareLeftAnti`；保留现有「两侧均不可空时降级 plain `LeftAnti`」优化（NovaRocks 现状即如此，比 StarRocks 的 always-NAAJ 更优） | hash join / NL join 的 NAAJ 执行已存在 |
| NOT IN value-form（OR / projection 位置）的 build 侧 NULL | 第一阶段不迁移（继续 legacy，缺陷 D2 记录在案）；迁移阶段采用 StarRocks `QuantifiedApply2OuterJoinRule` 的 CTE + count/distinct 双分支 + CASE 形态 | 后续阶段 |
| 非等值 correlation | apply 模式下显式报错 `non-EQ correlated predicate in correlated subquery is not supported`（对齐 StarRocks），替代 legacy 的静默重复（D6） | 行为收紧，需在迁移说明中标注 |

### 5.3 analyzer 职责变化

analyzer **不再做结构改写**，只做绑定与收集：

1. `resolve_expr.rs` 仍生成 placeholder，但 `SubqueryInfo` 升级为携带**已分析产物**：

   ```rust
   pub(crate) struct SubqueryInfo {
       pub id: usize,
       pub kind: SubqueryKind,
       /// Analyzed inner query (replaces the raw sqlparser AST).
       pub resolved: ResolvedQuery,
       /// Outer columns referenced inside the subquery, detected during
       /// the merged-scope analysis (replaces rewrite-time re-detection).
       pub correlation_column_ids: Vec<ColumnId>,
       /// Clause context: top-level AND conjunct of WHERE/HAVING/JOIN-ON.
       pub use_semi_anti: bool,
       /// For IN: analyzed LHS expression(s).
       pub in_exprs: Vec<TypedExpr>,
       pub data_type: DataType,
   }
   ```

   关键变化：子查询在收集时即用 merged scope（现有 `analyze_query_in_scope_with_inner` 机制）
   分析一次到位，inner plan 中的 outer 引用直接落成带 outer `ColumnId` 的 `ColumnRef`。现状的
   「trial-analyze 推类型 + rewrite 时再 analyze」双重分析及其 snapshot/restore 危险随之消失。
2. `use_semi_anti` 在表达式下降时维护（对应 StarRocks `Context.clone` 规则）：进入 OR / NOT /
   任意非顶层 AND 的嵌套位置即置 false；WHERE / HAVING / JOIN-ON 顶层 AND conjunct 保持 true。
3. 前置校验保留在 analyzer（JSON/BITMAP/HLL 操作数、IN 列数不匹配、scalar 多列输出等现有错误，
   外加 D5 的修复：GROUP BY / ORDER BY 位置的子查询在 analyzer 显式报
   `subquery is not supported in GROUP BY / ORDER BY`，而不是泄漏到 codegen）。
4. **路由**（迁移期）：`analyze_select` 末尾按 session 变量与形态白名单决定每个
   `SubqueryInfo` 走新链路（保留给 planner）还是旧链路（调用现有 `rewrite_subqueries`），见 §7。

### 5.4 planner 构造 Apply

`plan_select_scoped` 在构建 Filter / Having / Project 之前消费留存的 `SubqueryInfo`：

1. 递归 `plan_query(resolved_inner)` 得到 inner `LogicalPlan`；
2. 按 StarRocks 堆叠规则构造 left-deep Apply 链：当前累计 plan 作 `left`，inner plan 作
   `right`；同一谓词中的多个子查询依次堆叠
   `Apply2(Apply1(outer, sub1), sub2)`；
3. 表达式中的 placeholder 替换为 `output_column` 的 `ColumnRef`；
4. `use_semi_anti && matches!(kind, Exists | In)` 的 conjunct 从 Filter 谓词中**整体删除**
   （语义由 Apply 自身承载，对应 StarRocks `SubqueryUtils.rewriteScalarOperator`）；scalar 比较
   （如 `v > <placeholder>`）保留在 Filter 中引用 `output_column`。

`plan_output_columns` 对 Apply 的输出 schema：`left 输出列 ∪ {output_column}`。

### 5.5 Apply 的生命周期约束（核心架构决策）

**Apply 只活在 rewrite pipeline 的第一个 stage（`SubqueryRewrite`）内部，绝不进入 memo。**

- `rewrite/registry.rs` 在 `PredicatePushdownPreJoin` 之前插入 `SubqueryRewrite` stage。这一排序
  与 StarRocks 一致，并且是 `ApplyToWindow` 的匹配前提：谓词尚未下推时，comma join 仍是
  `Filter(CrossJoin(...))` 形态，WinMagic 的「只允许 cross join + 谓词一一对应」条件才可判定。
- stage 末尾的 `ApplyException` 规则保证离开该 stage 时树中无 Apply（§6.4）。因此：
  - 后续 rewrite stage（predicate pushdown / join reorder / aggregate pushdown / column
    pruning / dict rewrite）**无需理解 Apply**，相应 exhaustive match 一律写防御 arm；
  - `convert.rs::logical_plan_to_memo` 对 Apply 与 IMV marker 同等处理
    （`panic!("apply operator leaked into memo conversion")` —— 不可达防御，用户可读错误已由
    `ApplyException` 在更早处给出）；
  - **不需要 `Operator::LogicalApply` / `PhysicalApply` memo 变体**，stats / cost / implement /
    physical explain / fragment builder 全部不感知 Apply。
- **跨树引用不变量**：Apply 存活期间，`right` 子树允许引用 `correlation_column_ids` 中的 outer
  列（`ColumnId` 全局唯一使之天然可表达；`id_binding_verifier` 在 codegen 阶段运行，届时 Apply
  已消除，不会误报）。SubqueryRewrite stage 内部的规则负责维护「右子树的 outer 引用 ⊆
  `correlation_column_ids`」；decorrelation 完成的判定与 StarRocks
  `containsCorrelationSubquery` 等价——右子树谓词不再引用 correlation 列。

### 5.6 新增节点的机械改造清单

新增 `LogicalPlan::Apply` 与 `LogicalPlan::AssertOneRow` 两个变体。基于调研确认的全部 exhaustive
match 位点（编译期强制项标 [E]）：

| 位点 | Apply 的处理 | AssertOneRow 的处理 |
|---|---|---|
| `plan.rs` 变体 + 节点结构体 [E] | 完整定义 | `{ input, subquery_text: String }` |
| `planner/mod.rs::plan_output_columns` [E] | left ∪ output_column | 透传 |
| `rewrite/tree.rs::rewrite_children` + trip-wire test [E] | 递归两子 | 递归单子 |
| `rewrite/required_columns.rs` tag + column pruning | 保守：不 prune（Apply 短命） | 透传 |
| `cte_rewrite.rs` 三处 visit [E] | 递归 | 递归 |
| `rewrite/rules/utils.rs` 输出列收集 ×3 [E] | 同 plan_output_columns | 透传 |
| `join_reorder/cardinality.rs`、`predicate_pushdown/join_pushdown.rs::subtree_has_predicate_key` 等 walker [E] | 防御 arm（运行不到：stage 在其之前） | 正常 arm |
| IMV walkers（`rewrite/imv/*` 9 处）[E] | fail-fast arm（IMV 查询不应含 Apply） | fail-fast arm |
| `engine/mod.rs::collect_scan_stats`、`mv/iceberg_refresh.rs` [E] | 递归 | 递归 |
| `explain.rs::format_node`（logical formatter） | `APPLY (kind, correlation: [...])` —— 调试与 analyzer/planner unit test 用 | `ASSERT ONE ROW` |
| `convert.rs::logical_plan_to_memo` [E] | panic 防御（同 IMV marker） | 正常转换 → `Operator::LogicalAssertOneRow` |
| `Operator` 枚举 + `is_logical` + stats + cost + implement + derive + physical explain + fragment_builder + id_binding_verifier | **不需要**（Apply 不进 memo） | 各加一个 arm；fragment_builder 发 `ASSERT_NUM_ROWS_NODE`（lowering / exec 已存在） |

工作量评估：Apply 侧 ~20 个 match arm，其中多数是三行防御/递归；AssertOneRow 需要走完整
logical→physical→codegen 链，但执行层零新代码。trip-wire test
（`tree.rs:566` 与 `optimizer` 现有约定）保证不漏。

### 5.7 EXPLAIN 与观测

- `SubqueryRewrite` stage 自动获得现有 `RewriteTrace` 事件（RuleMatched / RuleChanged /
  RuleRejected / RuleFailed）。
- 每条规则按现有约定实现 `LogicalRewriteRule::name()`，自动进入
  `is_known_rule_name` / `disable_optimizer_rules` 单一命名空间。
- plan golden 通过 `sql-tests/optimizer/` 的 `@explain_contains` 断言物理形态（`ANALYTIC`、
  `NULL AWARE LEFT ANTI`、`ASSERT NUM ROWS`、join 计数），无需新增 EXPLAIN 能力。

---

## 6. SubqueryRewrite 规则体系

### 6.1 stage 与规则排序

`rewrite/registry.rs::query_rewrite_pipeline` 头部新增：

```text
Stage "SubqueryRewrite"  (RewritePhase::StructuralRewrite, 在 PredicatePushdownPreJoin 之前)
  1. PushDownApplyLeftProject     # 仅 uncorrelated scalar：Apply(Project(A), sub) → Project(Apply(A, sub))
  2. PushDownApplyLeft            # 仅 uncorrelated scalar：下推到产生 outer 兄弟列的那一侧
  3. PushDownApplyProject         # Apply(L, Project(R)) → 内联投影进 subquery_expr
  4. PushDownApplyFilter          # 拆 correlation_conjuncts / residual_predicate（decorrelation 轴心）
  5. PushDownApplyAggProjectFilter# Agg(Project(Filter)) → Agg(Filter(Project)) 换序
  6. PushDownApplyAggFilter       # correlated scalar agg → vector agg by corr keys; 清 need_check_max_rows
  7. ApplyToWindow                # WinMagic（M2 落地，OQ-13 核心）
  8. NormalizeCountScalarApply    # 被引用 count → ifnull(count, 0)
  9. ScalarApplyToJoin            # → CROSS JOIN + AssertOneRow / LEFT OUTER JOIN (+ assert_true 投影)
 10. ExistentialApplyToJoin       # use_semi_anti: → LEFT SEMI / LEFT ANTI
 11. QuantifiedApplyToJoin        # use_semi_anti: → LEFT SEMI / NullAwareLeftAnti|LeftAnti
 12. ExistentialApplyToOuterJoin  # value-form（M4）
 13. QuantifiedApplyToOuterJoin   # value-form，CTE 双分支 + CASE（M4）
 14. ApplyException               # 兜底：残留 Apply → 硬错误
```

规则名即 `disable_optimizer_rules` 可用名。迭代语义沿用现有 `RewritePipeline`
（`max_iterations` 内 fixpoint）；规则间顺序依赖（如 4 必须先于 7/9-13）通过
「to-window / to-join 规则的 matches 要求右子树已无 correlation 引用」表达，与 StarRocks 的
`containsCorrelationSubquery` gate 同构，不依赖列表顺序的脆弱约定。

### 6.2 关键规则输出形态

**ScalarApplyToJoin**（对应 StarRocks `ScalarApply2JoinRule` 三分支）：

```text
uncorrelated:
  Project(left cols, output_column)
    └─ CROSS JOIN
         ├─ left
         └─ AssertOneRow(subquery_text)      # 子查询可证 ≤1 行时省略（全局 agg 无 group key 等）
              └─ right

correlated + need_check_max_rows（非 agg 形态）:        # 全部 correlation conjunct 必须 EQ
  Project(left cols, output_column := any_value_col,
          __assert := assert_true(count_rows IS NULL OR count_rows <= 1,
                                  'correlated scalar subquery result must be at most 1 row'))
    └─ LEFT OUTER JOIN ON 去相关 EQ 键
         ├─ left
         └─ Aggregate(GROUP BY inner corr keys,
                      count_rows := count(1), any_value_col := any_value(subquery_expr))
              └─ [Filter(residual_predicate)] → right

correlated + !need_check_max_rows（agg 形态，q2/q17）:
  Project(left cols, output_column)
    └─ LEFT OUTER JOIN ON (correlation_conjuncts AND residual)
         ├─ left
         └─ right（已被 PushDownApplyAggFilter 归一化为 vector agg）
```

**ApplyToWindow**（WinMagic，匹配条件完整移植 §3.2 的 1-4 条，按 NovaRocks planner 输出形态
调整 pattern；表同一性用 `(catalog, database, table)` 全限定名 + 别名去重判定，自连接/重复表
拒绝）：

```text
Filter(... lhs op <output_column> ...) over Apply(outer, Agg(...))
  ⇒
Project
  └─ Filter(子查询比较谓词，output_column 替换为 window 输出列)
       └─ Window(AGG(args 重定位到 outer 列) OVER (PARTITION BY 相关键 outer 侧))
            └─ Filter(其余 outer 谓词)
                 └─ outer 子树          # 子查询子树整体丢弃
```

window 不改变行数、PARTITION BY 列的 distribution property 可被 OQ-8 利用——这正是 OQ-13
期望的 stats/property 行为；`LogicalWindow` 的 stats derive（行数透传 + window 列 unknown）
已存在。

**ExistentialApplyToJoin / QuantifiedApplyToJoin**：输出形态与现有 legacy 改写结果**有意保持
同构**（LEFT SEMI / LEFT ANTI / NullAwareLeftAnti，join 条件保持裸 `Eq` conjunct 以便 cascades
提取 hash key——legacy 演进中已有「IS-NULL-OR 包裹导致退化 NestLoopJoin 超时」的教训，必须
沿用裸 Eq 约定）。迁移这两类的收益是架构统一与 D4 类缺陷收口，不是 plan 形态变化。

### 6.3 与现有优化器的衔接

- SubqueryRewrite 产出的 join / aggregate / window 是普通节点，自然进入后续 predicate
  pushdown、join reorder（semi/anti 仍按现状不参与重排）、aggregate pushdown、column pruning、
  CBO 流程，无需任何特例。
- `AssertOneRow` 对 join reorder 等价于不可重排的一元屏障（语义上必须在子查询输出聚合点上方、
  CROSS JOIN 下方），cardinality 估计取 `min(child_rows, 1)`。

### 6.4 失败路径与错误信息（第一阶段无 PhysicalApply 的代价）

失败分三层，全部英文：

1. **analyzer 路由层**（首选拦截点）：apply 模式白名单外的形态直接走 legacy（迁移期）或报
   analyzer 错误（终态），用户永远不会因「框架覆盖不全」损失现有能力。
2. **规则前置条件**（对齐 StarRocks 措辞）：
   - `Err("non-EQ correlated predicate in correlated subquery is not supported")`
   - `Err("correlated subquery without correlation predicate is not supported")`
   - `Err("correlated IN subquery with grouping or aggregation is not supported")`
   这些是硬 `Err`（`LogicalRewriteRule::apply` 返回 `Err` 在 CollectDiagnostics 与 FailFast 两种
   policy 下均 fatal——现有框架语义，正合需求）。
3. **ApplyException 兜底**：

   ```text
   Err("subquery decorrelation failed: a residual Apply node (kind={kind:?}, correlated={bool})
        survived the SubqueryRewrite stage; this subquery shape is not yet supported.
        Workaround: SET subquery_unnest_mode = 'legacy'")
   ```

   迁移完成、legacy 删除后，workaround 提示一并移除。

运行期语义错误（非失败路径，是修复后的正确行为）：

- `assert_num_rows failed: ...`（带 `subquery '<sql>'` 前缀，现有 operator 消息格式）；
- `assert_true` 触发 `correlated scalar subquery result must be at most 1 row`。

---

## 7. 迁移路线

### 7.1 session 开关与路由

新增 session 变量 `subquery_unnest_mode`（`SET subquery_unnest_mode = '...'`）：

| 值 | 行为 |
|---|---|
| `legacy` | 全部走现有 analyzer rewrite（M0-M1 默认） |
| `apply` | 白名单形态走 Apply 链路，其余自动走 legacy（M2 起默认） |
| `apply_strict` | 白名单形态走 Apply；白名单外报错而非回退（CI / 调试用，保证覆盖面可见） |

规则粒度的开关复用 `disable_optimizer_rules`（如 `SET disable_optimizer_rules =
'ApplyToWindow'` 验证 to-join fallback 形态）。

按「NovaRocks 没有历史用户，不写兼容性代码」的项目原则：**legacy 路径是迁移期工具，不是长期
双轨**。每个 kind 在 apply 链路稳定（对应 suite 全绿 + 一个开发周期）后，立即删除
`subquery_rewrite.rs` 中对应分支；全部迁移完成后删除该文件与 `subquery_unnest_mode` 变量。

### 7.2 形态路由表（哪些先走 Apply）

| 形态 | 阶段 | 理由 |
|---|---|---|
| scalar（correlated + uncorrelated，WHERE/HAVING/SELECT-list） | **M1** | OQ-13 核心；D1/D6 正确性修复在此；改造收益最大 |
| correlated scalar agg → window | **M2**（ApplyToWindow） | 依赖 M1 的 Apply 形态与 push-down 归一化 |
| EXISTS / NOT EXISTS（WHERE/HAVING 顶层 AND） | **M3** | 输出形态与 legacy 同构，迁移以架构统一为目的，风险低但收益也低，故置后 |
| IN / NOT IN（WHERE/HAVING 顶层 AND，单列） | **M3** | 同上；NAAJ 语义已被 legacy 正确覆盖，重点是不回归 |
| EXISTS / IN value-form（OR、projection 位置） | **M4** | 需要 CTE 双分支 + CASE 形态（修复 D2）；依赖 to-join 规则成熟 |
| JOIN-ON 位置子查询 | **M4** | legacy 路径最脆弱（D3），但用户面小 |
| multi-column IN、GROUP BY/ORDER BY 位置（D5 报错收口）、correlated EXISTS in SELECT（D4） | **M4+** | 按需推进；D4/D5 的报错收口可以提前单独落 |

### 7.3 里程碑

**M0 — 基建（不改变任何行为）**
`ApplyKind` / `ApplyNode` / `AssertOneRowNode` + §5.6 全部 match arm + `SubqueryRewrite` 空
stage + `ApplyException` + `subquery_unnest_mode`（默认 `legacy`）+ logical EXPLAIN 的
`APPLY` / `ASSERT ONE ROW` 格式。验收：全部现有 suite 不变；trip-wire test 通过。

**M1 — scalar 链路 + 多行 guard**
analyzer `SubqueryInfo` 升级与路由、planner Apply 构造（scalar）、PushDownApplyProject/Filter/
AggFilter/AggProjectFilter、NormalizeCountScalarApply、ScalarApplyToJoin、AssertOneRow 的
codegen 发射。默认仍 `legacy`；CI 增加 `apply_strict` 模式跑 scalar 相关 case。
验收：apply 模式下 scalar 子查询结果与 legacy 一致（多行场景除外——legacy 静默错，apply 报
错，**这是有意的语义修复**，在 PR 描述与 golden 中显式标注）；tpc-h q2/q17 在 apply 模式结果
正确且 plan 不差于 legacy。

**M2 — ApplyToWindow（OQ-13 主交付）**
WinMagic 规则 + plan golden + 默认切 `apply`（scalar kind）。
验收：q17 EXPLAIN 出现 `ANALYTIC`（或在条件不满足时保持紧凑 agg+join，join 数不回退）；q2 join
数明显下降或出现 window；`disable_optimizer_rules='ApplyToWindow'` 时回到 M1 形态。

**M3 — EXISTS / IN to-join 迁移**
Existential/QuantifiedApplyToJoin；join/runtime-filter/tpc 套件在 apply 模式全绿后把 EXISTS/IN
加入 `apply` 模式的形态白名单（模式默认值在 M2 已是 `apply`，此处扩大的是白名单覆盖面）；删除
legacy 对应分支。验收重点：NAAJ 全矩阵（`join_null_aware_anti` 等）不回归。

**M4 — value-form、JOIN-ON、收尾**
OuterJoin 形态规则（修 D2/D3）、JOIN-ON 子查询、D4/D5 报错收口；删除 `subquery_rewrite.rs` 与
`subquery_unnest_mode`。

每个里程碑独立可合并、独立可回退（session 变量 + rule disable 双保险）。

---

## 8. 测试策略

### 8.1 analyzer / planner unit tests

- 沿用 `src/sql/analyzer/mod.rs` 现有测试风格：apply 模式下断言 `SubqueryInfo` 携带的
  correlation 列、kind、`use_semi_anti`（WHERE 顶层 AND = true；OR / NOT / projection = false）；
  legacy 模式现有测试不动，作为迁移期回归锚。
- planner 测试：Apply 链 left-deep 堆叠顺序、placeholder 替换为 `output_column`、semi-anti
  conjunct 从 Filter 中删除、`plan_output_columns` 正确。

### 8.2 optimizer plan golden（`sql-tests/optimizer/subquery_*`）

现状盘点：optimizer suite 今天**没有任何** EXISTS / IN / scalar decorrelation 的 plan-golden；
NAAJ 形态也没有 `@explain_contains` 断言。新增 case 族（每条规则至少一正一反）：

- `subquery_scalar_uncorrelated_assert_rows`：锁 `ASSERT NUM ROWS`；
- `subquery_scalar_correlated_agg_join`：锁 LEFT OUTER JOIN + vector agg（M1 形态）；
- `subquery_scalar_correlated_nonagg_guard`：锁 `assert_true` / count+any_value 投影；
- `subquery_scalar_to_window`：WinMagic 命中，锁 `ANALYTIC` + `@explain_not_contains` 多余
  join；同 case 第二步 `SET disable_optimizer_rules='ApplyToWindow'` 锁 fallback 形态；
- `subquery_scalar_to_window_rejected_*`：自连接 / 表集不匹配 / 谓词不一致 / 带 limit 等
  逐条否定条件，锁**不**出现 `ANALYTIC`（防误改写——这是「不为 TPC 硬编码」的反向保险）；
- `subquery_not_in_null_aware_shape`：锁 `NULL AWARE LEFT ANTI`（补上现状缺失的形态断言）；
- `subquery_exists_semi_shape` / `subquery_not_exists_anti_shape`。

记录方式：`--mode record --record-from target`（NovaRocks-only golden 的既有约定）。

### 8.3 SQL correctness 与 NULL-sensitive cases

- 复用并扩展 join suite 既有家族（`join_not_in_with_null`、`join_not_in_correlated_conjunct_
  null_aware`、`join_null_aware_anti`、`join_exists_subquery_semantics` 等）——M3 切默认前在
  `apply` 模式整体重跑，结果 golden 不得变化。
- 新增 correctness case：
  - **scalar 多行**：uncorrelated 与 correlated 非 agg 形态各一，`@expect_error` 断言
    assert 消息（这两个 case 只能在 apply 模式下落 golden——legacy 模式行为是静默复制行，
    无法用 `@expect_error` 表达）；
  - **empty group**：correlated scalar agg 在无匹配组时输出 NULL；`count` 经
    NormalizeCount 后输出 0（两者分别断言）；
  - **NULL correlation key**：probe 侧 NULL key 行在 scalar / EXISTS / IN 三种形态下的输出；
  - **NOT IN NULL 全矩阵**：build NULL / probe NULL / 两侧 NULL / 空子查询 ×（correlated |
    uncorrelated），与 legacy 输出逐行一致。

### 8.4 双模式回归与 CI

- M1-M3 期间 CI 对受影响 suite 各跑一遍 `apply_strict`（前置 `SET subquery_unnest_mode`，
  复用 runner 的 init/step 机制），与默认模式 golden 共用——保证两条链路同结果；
- 切默认后保留一个里程碑周期的 legacy 回归窗口，然后随 legacy 代码一起删除。

### 8.5 TPC 验收（OQ-13 对接）

- **tpc-h q2 / q17 是 decorrelation + window rewrite 的主验收**：两者都是 correlated scalar
  aggregate（q2 = `min`，q17 = `avg`），当前结果正确（在 stable CI 中）但形态 join-heavy。
  验收 = §7.3 M2 标准 + 与 `logs/plan-quality/20260603-fe-nr-plan-diff/` FE 基线对照 join 数与
  window 出现。
- **tpc-ds q47 / q49 / q57 的定位澄清**（调研修正）：这三条查询的 SQL **本身直接书写 window
  函数**（q47/q57 是 `rank()` + `avg(...) over` 自连接，q49 是三通道 `rank()` union），不含
  correlated subquery，且当前已在 stable CI 通过。它们属于 OQ-13 范围里「window/set-op 周边
  plan 形态 parity」与 OQ-14 set-op 的交界，**不是 Apply 框架的 decorrelation 验收对象**。对
  本设计它们的角色是回归保险：Apply 框架与 ApplyToWindow 落地后三者 plan/结果不得回退。
  top/rank-per-group 的 window 化 rewrite（OQ-13 另一子项）不依赖 Apply 入口，但复用本设计
  建立的 SubqueryRewrite stage、plan golden 与 rule disable 基建。
- 其余覆盖面：tpc-h q4/q21/q22（EXISTS/NOT EXISTS）、q16/q18/q20（IN/NOT IN）、tpc-ds
  q1/q6/q30/q32/q81/q92（correlated scalar）在 M3 切默认时作为整体回归面。

---

## 9. 风险与未决问题

1. **ApplyToWindow 条件极严**（表同一性、谓词同一性），通用查询命中率低。可接受：未命中时
   fallback 是 M1 的紧凑 agg+join——与现状形态相同，没有回退风险；命中收益（消除整棵子查询
   子树的重复扫描）正是 q2/q17 的差距来源。**不放宽条件去凑覆盖率**（放宽 = 误改写风险）。
2. **q2/q17 当前结果正确**，本次迁移在这两条查询上是纯 plan 形态收益。真正的正确性收益在
   D1/D6（多行 guard）——它会把既有「静默错」变成报错，属于行为收紧；需在 M1 的 PR 描述中
   显式公告。
3. **Iceberg snapshot 同一性**：WinMagic 的表同一性按全限定名判定时，同名表在同一查询内的两次
   scan 必须解析到同一 snapshot。standalone 查询准备阶段（`query_prep.rs`）目前即按语句级
   pin snapshot，设计上把这一不变量写进 ApplyToWindow 的前置断言（不满足则拒绝改写）。
4. **EXISTS/IN 迁移的性能回归**：to-join 输出必须保持裸 `Eq` join 条件（hash key 可提取），
   M3 验收以 join/runtime-filter suite 与 `join_large_in_predicate` 的执行不回退为门槛。
5. **`use_semi_anti` 的 clause 追踪**需要 resolve_expr 在表达式下降中维护上下文标志，是
   analyzer 改造里最容易出错的点；用 §8.1 的 unit test 矩阵（AND/OR/NOT/嵌套/HAVING/ON）锁。
6. **未决：HAVING 中子查询的 correlation 进 aggregate 之上**。legacy 通过把 HAVING 谓词留在
   aggregate 之上的 Filter 解决；Apply 链路同样把 Apply 插在 aggregate 之上即可，但 correlated
   HAVING 子查询引用聚合结果列的形态需要在 M1 中先用 `apply_strict` 暴露面，再决定支持或显式
   报错。
7. **未决：`AssertOneRow` 之上的 limit 交互**。`AssertOneRow` 不可与 Limit 交换（`LIMIT 1` 会
   掩盖多行错误）；M0 在节点注释与 rewrite walker 中固定该约束，暂不需要规则强制（没有规则
   会做这种交换），留意后续 TopN 类规则。

---

## 10. 附录：legacy 缺陷的处置一览

| 缺陷（§2.3） | 处置 |
|---|---|
| D1 scalar 无多行 guard | M1 修复（AssertOneRow / assert_true 形态） |
| D2 NOT IN value-form build NULL | M4（CTE 双分支 + CASE 形态）；迁移前保持 legacy 行为并在本文档记录 |
| D3 JOIN-ON NOT IN indicator NULL | M4 |
| D4 correlated EXISTS in SELECT list 泄漏 placeholder | M4 value-form 覆盖；或提前在 analyzer 显式报错 |
| D5 GROUP BY / ORDER BY 子查询泄漏 placeholder | 可独立提前：analyzer 显式报错（一行检查 + 测试） |
| D6 非等值 correlated scalar 静默重复 | M1 起 apply 模式显式报 non-EQ 错误；legacy 行为随分支删除而消失 |
| D7 scalar correlation 检测无兜底 | 新链路在 merged-scope 分析时即收集 outer 引用，兜底天然存在；检测不到的形态归为 analyzer 错误 |
