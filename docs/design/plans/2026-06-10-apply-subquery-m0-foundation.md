# Apply / CorrelatedSubquery M0 基建 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地设计文档 `docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md` 的 M0 里程碑：`LogicalPlan::Apply` / `LogicalPlan::AssertOneRow` 两个新节点的全部类型系统接线、`SubqueryRewrite` 空 stage + `ApplyException` 兜底、`subquery_unnest_mode` session 变量。**零行为变化**——M0 结束时没有任何生产路径构造 Apply 或 AssertOneRow。

**Architecture:** Apply 只活在 rewrite pipeline 第一个 stage 内、绝不进 memo（convert 处 panic 防御 + optimize() 不可禁用 backstop）；AssertOneRow 走完整 logical→physical→codegen 链（执行层 `assert_num_rows_processor` 已存在，只缺 standalone codegen 发射）。

**Tech Stack:** Rust；现有 rewrite 框架（`LogicalRewriteRule` / `RewritePipeline`）；Cascades memo；thrift `ASSERT_NUM_ROWS_NODE`。

**关键执行约束：**

1. **Task 1–8 是一个编译原子单元。** 给两个 exhaustive enum 加变体会同时打破 ~20 个文件的编译；Task 1 改完后 `cargo build` 必然失败，逐 Task 修复，直到 Task 8 末尾第一次回绿。**Task 8 之前不要 commit。** 编译器就是这一阶段的"failing test"。
2. 所有行号是写作时的参考位置（"around line N"），以符号名定位为准。
3. 新增 walker arm 的统一模式：**Apply = 二元节点（递归 left + right）；AssertOneRow = 一元透传节点（递归 input）**。唯二特殊点：`plan_output_columns`（Apply 输出 = left ∪ output_column）和 `convert.rs`（Apply 必须 panic 防御）。如果编译器报出本计划未列出的 match 位点（抽取可能有遗漏），按这个模式补 arm 即可。
4. 提交信息英文，不加 Co-Authored-By trailer。

---

### Task 1: plan.rs — 新节点类型与 LogicalPlan 变体

**Files:**
- Modify: `src/sql/planner/plan.rs`

- [ ] **Step 1: 在 `AggregateStateMergeNode`（around line 96–104）之后新增三个类型定义**

```rust
/// What the subquery expression looks like to its enclosing clause.
/// M1 consumes the non-Scalar variants; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

/// Subquery glue node: left child = outer plan, right child = subquery plan.
/// Built by the planner from analyzer-collected subquery metadata (M1);
/// rewritten into join / aggregate / window shapes by the optimizer's
/// SubqueryRewrite stage. Must never survive past that stage — the
/// ApplyException rule and the optimize() backstop enforce this, and
/// memo conversion panics on a leaked Apply as defence in depth.
/// Field semantics mirror StarRocks LogicalApplyOperator; see the design doc
/// docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §5.1.
/// M1 consumes the remaining fields; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ApplyNode {
    pub left: Box<LogicalPlan>,
    /// Subquery plan. May reference outer columns from
    /// `correlation_column_ids` while the Apply is alive.
    pub right: Box<LogicalPlan>,
    pub kind: ApplyKind,
    /// The expression the Apply was built from, written over the inner plan's
    /// output columns (`lhs IN (inner_col)`, `EXISTS(inner_col)`, or a bare
    /// `ColumnRef(inner_col)` for scalar subqueries).
    pub subquery_expr: TypedExpr,
    /// Fresh column standing in for the subquery's value in outer expressions.
    pub output_column: OutputColumn,
    /// Outer-side columns referenced inside the subquery.
    pub correlation_column_ids: Vec<ColumnId>,
    /// Correlated conjuncts hoisted out of the inner plan by the
    /// SubqueryRewrite push-down rules (empty at construction).
    pub correlation_conjuncts: Vec<TypedExpr>,
    /// Uncorrelated residual predicate hoisted out of the inner plan.
    pub residual_predicate: Option<TypedExpr>,
    /// Scalar only: the subquery must still be runtime-checked to <= 1 row.
    pub need_check_max_rows: bool,
    /// True iff the subquery sits as a top-level AND conjunct of
    /// WHERE / HAVING / JOIN-ON, so it may collapse into a semi/anti join.
    pub use_semi_anti: bool,
    /// For uncorrelated scalar subqueries used inside a predicate: the outer
    /// sibling columns of that predicate (drives left-side Apply push-down).
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Runtime guard asserting its input yields at most one row (SQL scalar
/// subquery cardinality rule). Lowered to thrift ASSERT_NUM_ROWS_NODE; the
/// exec operator and FE-compat lowering already exist. Must not be reordered
/// with Limit (a LIMIT above would mask the multi-row error).
/// M1 produces this node from ScalarApplyToJoin; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct AssertOneRowNode {
    pub input: Box<LogicalPlan>,
    /// Original subquery text used in the runtime error message.
    pub subquery_text: String,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
```

- [ ] **Step 2: 在 `LogicalPlan` enum（around line 19–66）的 `AggregateStateMerge(AggregateStateMergeNode),` 之后、`ImvDelta` 之前插入两个变体**

```rust
    /// Subquery glue node (outer ⋈ subquery). Eliminated by the
    /// SubqueryRewrite stage; see ApplyNode.
    Apply(ApplyNode),
    /// At-most-one-row runtime guard for scalar subqueries.
    AssertOneRow(AssertOneRowNode),
```

- [ ] **Step 3: 确认编译失败（预期内）**

Run: `cargo build 2>&1 | grep -c "non-exhaustive\|patterns.*not covered" || true`
Expected: 多个 `E0004` non-exhaustive 错误，涉及 planner/mod.rs、tree.rs、cte_rewrite.rs、convert.rs 等。这是后续 Task 的工作清单。**不要 commit。**

---

### Task 2: planner/mod.rs — 输出 schema 与测试 helper

**Files:**
- Modify: `src/sql/planner/mod.rs`

- [ ] **Step 1: `plan_output_columns`（around line 510–560）在 `AggregateStateMerge` arm 之后加两个 arm**

```rust
        LogicalPlan::Apply(node) => {
            let mut columns = plan_output_columns(&node.left)?;
            columns.push(node.output_column.clone());
            Ok(columns)
        }
        LogicalPlan::AssertOneRow(node) => plan_output_columns(&node.input),
```

- [ ] **Step 2: 四个测试 helper 的 `visit` match 各加两个 arm**

位置（`#[cfg(test)]` 模块内）：`first_aggregate_calls`（around 3375）、`first_repeat_node`（around 3435）、`first_window_exprs`（around 3518）、`first_window_output_columns`（around 3553）。每处在 `AggregateStateMerge` arm 之后加：

```rust
                LogicalPlan::Apply(node) => visit(&node.left).or_else(|| visit(&node.right)),
                LogicalPlan::AssertOneRow(node) => visit(&node.input),
```

- [ ] **Step 3: `plan_scoped_query` 的 set-op patch match（around line 65–82）有 `_ => {}` wildcard，无需改动——确认即可**

---

### Task 3: rewrite/tree.rs — 通用 walker 与 trip-wire

**Files:**
- Modify: `src/sql/optimizer/rewrite/tree.rs`

- [ ] **Step 1: 文件头部 `use crate::sql::planner::plan::{...}` import 列表中加入 `ApplyNode, AssertOneRowNode`**

- [ ] **Step 2: `rewrite_children`（around line 82）在 `AggregateStateMerge` arm 之后加两个 arm**

```rust
        LogicalPlan::Apply(node) => {
            let (left, left_changed) = rewrite_with_rule(*node.left, rule, ctx)?;
            let (right, right_changed) = rewrite_with_rule(*node.right, rule, ctx)?;
            Ok((
                LogicalPlan::Apply(ApplyNode {
                    left: Box::new(left),
                    right: Box::new(right),
                    ..node
                }),
                left_changed || right_changed,
            ))
        }
        LogicalPlan::AssertOneRow(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::AssertOneRow(AssertOneRowNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
```

- [ ] **Step 3: trip-wire test `assert_variant_handled`（around line 476–501）的 or-列表追加**

```rust
                | LogicalPlan::Apply(_)
                | LogicalPlan::AssertOneRow(_)
```

---

### Task 4: rewrite 框架长尾 walker

统一模式：Apply 二元递归 / AssertOneRow 一元递归；重建型函数用 `..node` functional update（tree.rs 同款，部分移动后 FRU 合法）。

**Files:**
- Modify: `src/sql/optimizer/cte_rewrite.rs`
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/{collector.rs,rewriter.rs,rule.rs}`

- [ ] **Step 1: `cte_rewrite.rs` 三处（import 加 `ApplyNode, AssertOneRowNode`）**

`visit`（around line 12）：

```rust
            LogicalPlan::Apply(node) => {
                visit(&node.left, ctx);
                visit(&node.right, ctx);
            }
            LogicalPlan::AssertOneRow(node) => visit(&node.input, ctx),
```

`inline_single_use_ctes`（around line 70）：

```rust
        LogicalPlan::Apply(node) => Ok(LogicalPlan::Apply(ApplyNode {
            left: Box::new(inline_single_use_ctes(*node.left, ctx)?),
            right: Box::new(inline_single_use_ctes(*node.right, ctx)?),
            ..node
        })),
        LogicalPlan::AssertOneRow(node) => Ok(LogicalPlan::AssertOneRow(AssertOneRowNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            ..node
        })),
```

`replace_cte_consume`（around line 225）：

```rust
        LogicalPlan::Apply(node) => Ok(LogicalPlan::Apply(ApplyNode {
            left: Box::new(replace_cte_consume(*node.left, cte_id, replacement)?),
            right: Box::new(replace_cte_consume(*node.right, cte_id, replacement)?),
            ..node
        })),
        LogicalPlan::AssertOneRow(node) => Ok(LogicalPlan::AssertOneRow(AssertOneRowNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            ..node
        })),
```

- [ ] **Step 2: `required_columns.rs` 四处**

`tag_required_columns` dispatch（around line 40）加两个 arm（与既有 arm 同款签名，传入 `parent_needed`）：

```rust
        LogicalPlan::Apply(_) => tag_apply(plan, parent_needed),
        LogicalPlan::AssertOneRow(_) => tag_assert_one_row(plan, parent_needed),
```

文件内新增两个函数（放在其他 `tag_*` 函数附近）：

```rust
/// Apply is eliminated by the SubqueryRewrite stage, which runs before column
/// pruning, so pruning never sees it in production plans. Tag conservatively:
/// require everything below, prune nothing.
fn tag_apply(plan: LogicalPlan, _parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    match plan {
        LogicalPlan::Apply(mut node) => {
            node.required_output_columns = None;
            node.left = Box::new(tag_required_columns(*node.left, None));
            node.right = Box::new(tag_required_columns(*node.right, None));
            LogicalPlan::Apply(node)
        }
        other => other,
    }
}

/// Conservative: no pruning through AssertOneRow in M0. Tighten when M1
/// starts producing this node in real plans.
fn tag_assert_one_row(
    plan: LogicalPlan,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::AssertOneRow(mut node) => {
            node.required_output_columns = None;
            node.input = Box::new(tag_required_columns(*node.input, None));
            LogicalPlan::AssertOneRow(node)
        }
        other => other,
    }
}
```

`collect_cte_consumer_needs`（around line 501）：

```rust
        LogicalPlan::Apply(n) => {
            collect_cte_consumer_needs(&n.left, target_id, acc);
            collect_cte_consumer_needs(&n.right, target_id, acc);
        }
        LogicalPlan::AssertOneRow(n) => collect_cte_consumer_needs(&n.input, target_id, acc),
```

`walk_consume_position_map`（around line 570）：

```rust
        LogicalPlan::Apply(n) => {
            walk_consume_position_map(&n.left, target_id, map);
            walk_consume_position_map(&n.right, target_id, map);
        }
        LogicalPlan::AssertOneRow(n) => walk_consume_position_map(&n.input, target_id, map),
```

`subtree_untagged`（around line 650，模式同 Join 只看 left）：

```rust
        LogicalPlan::Apply(n) => subtree_untagged(&n.left),
        LogicalPlan::AssertOneRow(n) => subtree_untagged(&n.input),
```

- [ ] **Step 3: `utils.rs` 三处**

`collect_output_columns`（around line 160）：

```rust
        LogicalPlan::Apply(a) => {
            let mut out = collect_output_columns(&a.left);
            out.insert(a.output_column.name.to_lowercase());
            out
        }
        LogicalPlan::AssertOneRow(a) => collect_output_columns(&a.input),
```

`collect_output_ids_ordered`（around line 374）：

```rust
        LogicalPlan::Apply(a) => {
            let mut ids = collect_output_ids_ordered(&a.left);
            ids.push(a.output_column.column_id);
            ids
        }
        LogicalPlan::AssertOneRow(a) => collect_output_ids_ordered(&a.input),
```

`collect_qualified_output_columns_inner`（around line 694）：

```rust
        // Apply's synthesized output column is unqualified; only the left
        // side contributes qualified refs.
        LogicalPlan::Apply(a) => collect_qualified_output_columns_inner(&a.left, out),
        LogicalPlan::AssertOneRow(a) => collect_qualified_output_columns_inner(&a.input, out),
```

- [ ] **Step 4: `cardinality.rs` `estimate_statistics`（around line 21）**

```rust
        // Apply is row-preserving on the outer side for the estimator's
        // purposes; join reorder never reorders through it anyway.
        LogicalPlan::Apply(n) => estimate_statistics(&n.left, table_stats),
        LogicalPlan::AssertOneRow(n) => {
            let child = estimate_statistics(&n.input, table_stats);
            Statistics {
                output_row_count: child.output_row_count.min(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: HashMap::new(),
            }
        }
```

- [ ] **Step 5: `join_pushdown.rs` `subtree_has_predicate_key`（around line 458）**

```rust
        LogicalPlan::Apply(apply) => {
            subtree_has_predicate_key(&apply.left, key)
                || subtree_has_predicate_key(&apply.right, key)
        }
        LogicalPlan::AssertOneRow(assert) => subtree_has_predicate_key(&assert.input, key),
```

- [ ] **Step 6: low_cardinality_dict 五处**

`collector.rs::collect_blocklist`（around line 65）：

```rust
        LogicalPlan::Apply(node) => {
            collect_blocklist(&node.left, out);
            collect_blocklist(&node.right, out);
        }
        LogicalPlan::AssertOneRow(node) => collect_blocklist(&node.input, out),
```

`collector.rs::walk`（around line 202）：

```rust
        LogicalPlan::Apply(node) => {
            walk(&node.left, provider, blocklist, dict_ctx)?;
            walk(&node.right, provider, blocklist, dict_ctx)?;
        }
        LogicalPlan::AssertOneRow(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
```

`rewriter.rs::rewrite_node`（around line 122–140）：把 `| LogicalPlan::Apply(_)` 和 `| LogicalPlan::AssertOneRow(_)` 加入既有 decode-boundary or-组（`Intersect | Except | Window | ... | AggregateStateMerge | ... | CTEProduce => decode_boundary(plan, ctx)`）。

`rewriter.rs` 局部 `plan_output_columns`（around line 1265）：

```rust
        LogicalPlan::Apply(node) => {
            let mut out = plan_output_columns(&node.left);
            out.push(node.output_column.clone());
            out
        }
        LogicalPlan::AssertOneRow(node) => plan_output_columns(&node.input),
```

`rule.rs::contains_scan`（around line 44）：

```rust
        LogicalPlan::Apply(node) => contains_scan(&node.left) || contains_scan(&node.right),
        LogicalPlan::AssertOneRow(node) => contains_scan(&node.input),
```

---

### Task 5: IMV walkers 与 engine 层

dossier 确认以下 IMV 文件的 match 是 exhaustive（其余 IMV 文件带 `_ =>` wildcard，无需改）。

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs`
- Modify: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
- Modify: `src/sql/optimizer/rewrite/imv/apply_key.rs`
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs`
- Modify: `src/sql/optimizer/rewrite/imv/union_delta.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: `marker.rs` 两处**

`plan_contains_imv_marker`（around line 182）：

```rust
        LogicalPlan::Apply(n) => {
            plan_contains_imv_marker(&n.left) || plan_contains_imv_marker(&n.right)
        }
        LogicalPlan::AssertOneRow(n) => plan_contains_imv_marker(&n.input),
```

`collect_into`（around line 224）：

```rust
        LogicalPlan::Apply(n) => {
            collect_into(&n.left, found);
            collect_into(&n.right, found);
        }
        LogicalPlan::AssertOneRow(n) => collect_into(&n.input, found),
```

- [ ] **Step 2: `aggregate_rewrite.rs` 两处**

嵌套 `visit`（around line 673）：

```rust
            LogicalPlan::Apply(node) => {
                visit(&node.left, found)?;
                visit(&node.right, found)
            }
            LogicalPlan::AssertOneRow(node) => visit(&node.input, found),
```

`thread_delta_action_column`（around line 732）：

```rust
        LogicalPlan::Apply(mut node) => {
            node.left = Box::new(thread_delta_action_column(*node.left, action_column)?);
            node.right = Box::new(thread_delta_action_column(*node.right, action_column)?);
            LogicalPlan::Apply(node)
        }
        LogicalPlan::AssertOneRow(mut node) => {
            node.input = Box::new(thread_delta_action_column(*node.input, action_column)?);
            LogicalPlan::AssertOneRow(node)
        }
```

- [ ] **Step 3: 三个 `plan_kind`（`apply_key.rs` around 184、`join_delta.rs` around 208、`union_delta.rs` around 378）各加**

```rust
        LogicalPlan::Apply(_) => "Apply",
        LogicalPlan::AssertOneRow(_) => "AssertOneRow",
```

- [ ] **Step 4: `join_delta.rs` 局部 `plan_output_columns`（around line 262）**

```rust
        LogicalPlan::Apply(apply) => {
            let mut out = plan_output_columns(&apply.left)?;
            out.push(apply.output_column.clone());
            out
        }
        LogicalPlan::AssertOneRow(assert) => plan_output_columns(&assert.input)?,
```

- [ ] **Step 5: `engine/mod.rs::collect_scan_stats`（around line 3041）**

```rust
        LogicalPlan::Apply(n) => {
            collect_scan_stats(&n.left, out);
            collect_scan_stats(&n.right, out);
        }
        LogicalPlan::AssertOneRow(n) => collect_scan_stats(&n.input, out),
```

- [ ] **Step 6: `iceberg_refresh.rs::logical_plan_contains_aggregate_state_merge`（around line 9402）**

```rust
        LogicalPlan::Apply(n) => {
            logical_plan_contains_aggregate_state_merge(&n.left)
                || logical_plan_contains_aggregate_state_merge(&n.right)
        }
        LogicalPlan::AssertOneRow(n) => logical_plan_contains_aggregate_state_merge(&n.input),
```

---

### Task 6: memo 层 — operator / convert / stats / cost / logical_props

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/logical_props.rs`

- [ ] **Step 1: `operator.rs` — 新 op 结构体（放在 `LogicalLimitOp`/`PhysicalLimitOp` 附近）**

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalAssertOneRowOp {
    /// Original subquery text used in the runtime error message.
    pub subquery_text: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalAssertOneRowOp {
    pub subquery_text: String,
}
```

`Operator` enum（around line 473–522）：logical 区在 `LogicalAggregateStateMerge(AggregateStateMergeOp),` 后加 `LogicalAssertOneRow(LogicalAssertOneRowOp),`；physical 区在 `PhysicalAggregateStateMerge(AggregateStateMergeOp),` 后加 `PhysicalAssertOneRow(PhysicalAssertOneRowOp),`。

`is_logical()`（around line 524）的 or-列表追加 `| Operator::LogicalAssertOneRow(_)`。

- [ ] **Step 2: `convert.rs` — import 列表加 `LogicalAssertOneRowOp`，在 `AggregateStateMerge` arm 之后、Imv panic arm 之前加两个 arm**

```rust
        LogicalPlan::AssertOneRow(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalAssertOneRow(LogicalAssertOneRowOp {
                subquery_text: node.subquery_text.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }
        LogicalPlan::Apply(_) => {
            // Defence in depth only: the ApplyException rule and the
            // optimize() backstop both fire before memo conversion.
            panic!("apply operator must be eliminated by the SubqueryRewrite stage before memo conversion");
        }
```

- [ ] **Step 3: `stats.rs` — `derive_statistics` match 末尾（`PhysicalAggregateStateMerge` arm 之后）加**

```rust
        Operator::LogicalAssertOneRow(_) | Operator::PhysicalAssertOneRow(_) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            Statistics {
                output_row_count: child_stats.output_row_count.min(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }
```

`derive_output_columns`：logical passthrough or-组（around line 1346，含 `LogicalLimit`）追加 `| Operator::LogicalAssertOneRow(_)`；physical passthrough or-组（around line 1421，含 `PhysicalLimit`）追加 `| Operator::PhysicalAssertOneRow(_)`。

- [ ] **Step 4: `cost.rs` — logical 0.0 or-组（around line 344–364）追加 `| Operator::LogicalAssertOneRow(_)`；在 `Operator::PhysicalLimit(_) => 0.01,` 之后加**

```rust
        Operator::PhysicalAssertOneRow(_) => 0.01,
```

- [ ] **Step 5: `logical_props.rs` — `inherit_from_child` or-组（around line 1071–1085，含 `LogicalLimit`/`PhysicalLimit`）追加**

```rust
        | Operator::LogicalAssertOneRow(_)
        | Operator::PhysicalAssertOneRow(_)
```

---

### Task 7: physical 链 — derive / implementation rule

**Files:**
- Create: `src/sql/optimizer/derive/assert_one_row.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [ ] **Step 1: 新文件 `src/sql/optimizer/derive/assert_one_row.rs`**

```rust
//! AssertOneRow — runtime guard that its input yields at most one row.
//!
//! The row count must be observed globally, so the child is required to be
//! gathered to a single instance before the assert fires (same correctness
//! argument as a global LIMIT). Output mirrors the child's output; ordering
//! requirements pass through.

use crate::sql::optimizer::operator::PhysicalAssertOneRowOp;
use crate::sql::optimizer::property::{DistributionSpec, PhysicalPropertySet};

use super::passthrough::passthrough_output;
use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalAssertOneRowOp {
    fn derive_output(&self, children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        passthrough_output(children_outputs)
    }
}

impl DeriveRequired for PhysicalAssertOneRowOp {
    fn derive_required(
        &self,
        parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: parent_required.ordering.clone(),
        }]
    }
}
```

- [ ] **Step 2: `derive/mod.rs` — 文件末尾模块列表加 `pub(crate) mod assert_one_row;`；`derive_output` dispatch（around line 67）和 `derive_required` dispatch（around line 119）各加一个 arm（放在 `PhysicalLimit` arm 旁）**

```rust
        Operator::PhysicalAssertOneRow(o) => o.derive_output(children_outputs),
```

```rust
        Operator::PhysicalAssertOneRow(o) => o.derive_required(parent_required, num_children),
```

- [ ] **Step 3: `implement.rs` — import 列表（`use super::operator::{...}`）补 `LogicalAssertOneRowOp` 所需符号（该文件已 import `Operator` 与各 op 结构体；加 `PhysicalAssertOneRowOp`），在 `LimitToPhysical` 之后新增规则**

```rust
// ---------------------------------------------------------------------------
// AssertOneRowToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct AssertOneRowToPhysical;

impl Rule for AssertOneRowToPhysical {
    fn name(&self) -> &str {
        "AssertOneRowToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAssertOneRow(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAssertOneRow(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalAssertOneRow(PhysicalAssertOneRowOp {
                subquery_text: op.subquery_text.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}
```

- [ ] **Step 4: `cascades_rules/mod.rs` `all_implementation_rules()`（around line 14–41）在 `Box::new(implement::LimitToPhysical),` 之后加**

```rust
        Box::new(implement::AssertOneRowToPhysical),
```

---

### Task 8: explain / codegen / verifier — 编译回绿点

**Files:**
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/id_binding_verifier.rs`

- [ ] **Step 1: `explain.rs` `format_node`（logical formatter，around line 119）加两个 arm；顶部从 planner::plan 的 import 补 `ApplyKind`**

```rust
        LogicalPlan::Apply(node) => {
            let kind = match node.kind {
                ApplyKind::Scalar => "SCALAR",
                ApplyKind::Exists { negated: false } => "EXISTS",
                ApplyKind::Exists { negated: true } => "NOT EXISTS",
                ApplyKind::In { negated: false } => "IN",
                ApplyKind::In { negated: true } => "NOT IN",
            };
            out.push(format!(
                "{pad}APPLY ({kind}, correlated={}, use_semi_anti={})",
                !node.correlation_column_ids.is_empty(),
                node.use_semi_anti
            ));
            format_node(&node.left, level, indent + 1, out);
            format_node(&node.right, level, indent + 1, out);
        }
        LogicalPlan::AssertOneRow(node) => {
            out.push(format!("{pad}ASSERT ONE ROW"));
            format_node(&node.input, level, indent + 1, out);
        }
```

- [ ] **Step 2: `explain.rs` `format_physical_node`（around line 401+）在 `PhysicalLimit` arm 之后加**

```rust
        Operator::PhysicalAssertOneRow(_) => {
            out.push(format!(
                "{pad}ASSERT NUM ROWS (<= 1){costs_suffix}{stats_suffix}"
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
```

- [ ] **Step 3: `fragment_builder.rs` — visit dispatcher（around line 1145）在 `PhysicalLimit` arm 之后加 dispatch；operator import 列表补 `PhysicalAssertOneRowOp`**

```rust
            Operator::PhysicalAssertOneRow(op) => self.visit_assert_one_row(op, node),
```

新增 visit 方法（放在 `visit_limit` 附近；模式照 `visit_repeat` 尾部：新建 TPlanNode、pre-order 拼接、透传 child 的 scope/tuple_ids）：

```rust
    fn visit_assert_one_row(
        &mut self,
        op: &PhysicalAssertOneRowOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "PhysicalAssertOneRow expected exactly 1 child, got {}",
                node.children.len()
            ));
        }
        let child = self.visit(&node.children[0])?;
        let node_id = self.alloc_node();

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = child.tuple_ids.clone();
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.assert_num_rows_node = Some(plan_nodes::TAssertNumRowsNode {
            desired_num_rows: Some(1),
            subquery_string: Some(op.subquery_text.clone()),
            assertion: Some(plan_nodes::TAssertion::LE),
        });

        // Pre-order: assert node first, then child nodes.
        let mut out_nodes = vec![plan_node];
        out_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes: out_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: child.ordering,
        })
    }
```

- [ ] **Step 4: `id_binding_verifier.rs` — pass-through arm（around line 608）扩展**

把

```rust
        Operator::PhysicalLimit(_) | Operator::PhysicalDistribution(_) => {
```

改为

```rust
        Operator::PhysicalLimit(_)
        | Operator::PhysicalDistribution(_)
        | Operator::PhysicalAssertOneRow(_) => {
```

- [ ] **Step 5: 第一次全量编译回绿**

Run: `cargo build`
Expected: 成功。如果还有 non-exhaustive 报错（抽取遗漏的 match 位点），按本计划开头的统一模式补 arm（Apply 二元递归 / AssertOneRow 一元递归；`has_x`/`is_x` 类谓词函数对 Apply 递归两侧、对 AssertOneRow 递归 input）。

- [ ] **Step 6: 跑现有测试确认零回归**

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 全部通过（现有测试不感知新变体）。

- [ ] **Step 7: Commit（Task 1–8 的原子提交）**

```bash
git add -A
git commit -m "feat(optimizer): add LogicalApply and AssertOneRow plan nodes (M0 scaffolding)

LogicalPlan::Apply carries subquery kind, correlation columns, semi-anti
eligibility, and the max-rows guard flag (mirrors StarRocks
LogicalApplyOperator). It lives only inside the upcoming SubqueryRewrite
stage and never enters the memo (conversion panics as defence in depth).
AssertOneRow gets a full logical->physical->codegen chain emitting the
existing thrift ASSERT_NUM_ROWS_NODE (assertion LE 1); the exec operator
and lowering already existed via the FE-compat path. No production path
constructs either node yet — zero behavior change."
```

---

### Task 9: 新节点行为单测

**Files:**
- Modify: `src/sql/planner/mod.rs`（tests 模块）
- Modify: `src/sql/optimizer/rewrite/tree.rs`（tests 模块）
- Modify: `src/sql/optimizer/mod.rs`（tests 模块）
- Modify: `src/sql/explain.rs`（tests 模块，around line 1251）
- Modify: `src/sql/codegen/fragment_builder.rs`（tests 模块）

- [ ] **Step 1: planner 输出 schema 测试（planner/mod.rs tests 模块）**

```rust
    #[test]
    fn apply_output_columns_extend_left_with_output_column() {
        use std::collections::HashSet;

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{ApplyKind, ApplyNode, ValuesNode};

        let left_col = OutputColumn {
            column_id: ColumnId(11),
            name: "l1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let out_col = OutputColumn {
            column_id: ColumnId(12),
            name: "__sq_1".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        };
        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![left_col.clone()],
                required_output_columns: None,
            })),
            right: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(12),
                    qualifier: None,
                    column: "__sq_1".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: out_col.clone(),
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let columns = plan_output_columns(&plan).expect("apply output columns");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column_id, left_col.column_id);
        assert_eq!(columns[1].column_id, out_col.column_id);
    }

    #[test]
    fn assert_one_row_output_columns_pass_through() {
        use arrow::datatypes::DataType;

        use crate::sql::analysis::OutputColumn;
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{AssertOneRowNode, ValuesNode};

        let col = OutputColumn {
            column_id: ColumnId(21),
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let plan = LogicalPlan::AssertOneRow(AssertOneRowNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![col.clone()],
                required_output_columns: None,
            })),
            subquery_text: "select 1".to_string(),
            required_output_columns: None,
        });

        let columns = plan_output_columns(&plan).expect("assert output columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column_id, col.column_id);
    }
```

- [ ] **Step 2: 运行，确认失败原因只可能是断言（类型已在 Task 1–2 落地，应直接通过）**

Run: `cargo test --lib apply_output_columns_extend_left_with_output_column assert_one_row_output_columns_pass_through 2>&1 | tail -3`
Expected: 2 passed。

- [ ] **Step 3: tree.rs 遍历测试（tests 模块，复用现有 `RenameScanRule` 与 helper）**

```rust
    #[test]
    fn bottom_up_rewrite_rebuilds_apply_children() {
        use std::collections::HashSet;

        use crate::sql::planner::plan::{ApplyKind, ApplyNode};

        let outer = project_over_scan("outer");
        let LogicalPlan::Project(outer_project) = outer else {
            panic!("helper returns project");
        };
        let inner = project_over_scan("before");
        let LogicalPlan::Project(inner_project) = inner else {
            panic!("helper returns project");
        };

        let plan = LogicalPlan::Apply(ApplyNode {
            left: outer_project.input,
            right: inner_project.input,
            kind: ApplyKind::Scalar,
            subquery_expr: column_ref(ColumnId(7), "sq"),
            output_column: output_column("sq"),
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let LogicalPlan::Apply(apply) = rewritten else {
            panic!("expected apply root");
        };
        let LogicalPlan::Scan(right_scan) = *apply.right else {
            panic!("expected scan on apply right side");
        };
        assert_eq!(right_scan.table.name, "after");
    }
```

- [ ] **Step 4: optimize() 端到端 AssertOneRow 测试（optimizer/mod.rs tests 模块，仿 `optimize_accepts_migrated_query_rewrite_pipeline`）**

```rust
    #[test]
    fn optimize_implements_assert_one_row() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{AssertOneRowNode, LogicalPlan, ValuesNode};

        let plan = LogicalPlan::AssertOneRow(AssertOneRowNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            subquery_text: "select 1".to_string(),
            required_output_columns: None,
        });
        let factory = ColumnRefFactory::new();
        let physical =
            optimize(plan, &HashMap::new(), factory, None).expect("optimize assert one row");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalAssertOneRow"));
    }
```

- [ ] **Step 5: explain 格式测试（explain.rs tests 模块）**

```rust
    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        use std::collections::HashSet;

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{
            ApplyKind, ApplyNode, AssertOneRowNode, LogicalPlan, ValuesNode,
        };

        let values = || {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        };
        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(values()),
            right: Box::new(LogicalPlan::AssertOneRow(AssertOneRowNode {
                input: Box::new(values()),
                subquery_text: "select 1".to_string(),
                required_output_columns: None,
            })),
            kind: ApplyKind::Exists { negated: true },
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                is_internal: true,
            },
            correlation_column_ids: vec![ColumnId(1)],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: true,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let mut out = Vec::new();
        super::format_node(&plan, super::ExplainLevel::Normal, 0, &mut out);
        assert!(
            out.iter()
                .any(|line| line.contains("APPLY (NOT EXISTS, correlated=true, use_semi_anti=true)")),
            "missing APPLY line: {out:?}"
        );
        assert!(
            out.iter().any(|line| line.contains("ASSERT ONE ROW")),
            "missing ASSERT ONE ROW line: {out:?}"
        );
    }
```

- [ ] **Step 6: fragment_builder thrift 发射测试（fragment_builder.rs tests 模块，仿 `build_generate_series_emits_table_function_without_scan_source`，复用 `DummyCatalog`）**

```rust
    #[test]
    fn assert_one_row_emits_assert_num_rows_node() {
        let child = PhysicalPlanNode {
            op: Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: "generate_series".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
                name: "generate_series".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        let output_columns = child.output_columns.clone();
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalAssertOneRow(
                crate::sql::optimizer::operator::PhysicalAssertOneRowOp {
                    subquery_text: "select 1".to_string(),
                },
            ),
            children: vec![child],
            stats: Statistics::default(),
            output_columns,
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build = PlanFragmentBuilder::build(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        let assert_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE)
            .expect("assert num rows node");
        let payload = assert_node
            .assert_num_rows_node
            .as_ref()
            .expect("assert payload");
        assert_eq!(payload.desired_num_rows, Some(1));
        assert_eq!(payload.assertion, Some(plan_nodes::TAssertion::LE));
        assert_eq!(payload.subquery_string.as_deref(), Some("select 1"));
    }
```

- [ ] **Step 7: 运行全部新测试**

Run: `cargo test --lib apply_ assert_one_row bottom_up_rewrite_rebuilds_apply logical_explain_formats optimize_implements_assert 2>&1 | tail -5`
Expected: 全部通过。若 `Statistics::default()` 不存在 `Default` 实现，改用现有测试中的字面量构造（`Statistics { output_row_count: 0.0, column_statistics: HashMap::new(), ..Default::default() }` 的写法在 fragment_builder 现有测试中已出现，照抄即可）。

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "test(optimizer): cover Apply/AssertOneRow node plumbing

Output-schema derivation, rewrite-tree traversal, end-to-end
optimize() implementation of AssertOneRow, logical EXPLAIN formatting,
and ASSERT_NUM_ROWS_NODE thrift emission."
```

---

### Task 10: SubqueryRewrite stage + ApplyException + optimize() backstop

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`
- Create: `src/sql/optimizer/rewrite/rules/subquery/apply_exception.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: 先写失败测试（subquery/mod.rs 的 tests 随模块一起创建，见 Step 2/3；registry 的两个现有测试先改成期望新 stage/rule——它们此刻会失败）**

`registry.rs` 测试 `query_pipeline_uses_expected_stage_order_and_rules`：`stage_names()` 期望列表头部插入 `"SubqueryRewrite"`；排序后的 `rule_names` 期望列表中按字母序插入 `"ApplyException"`（位于 `"AggregatePushdown"` 与 `"DeriveJoinNotNullPredicate"` 之间）。测试 `rewrite_registry_recognizes_migrated_query_rules` 追加：

```rust
        assert!(is_known_rewrite_rule_name("ApplyException"));
```

Run: `cargo test --lib query_pipeline_uses_expected_stage_order 2>&1 | tail -3`
Expected: FAIL（stage 尚未注册）。

- [ ] **Step 2: 新文件 `src/sql/optimizer/rewrite/rules/subquery/apply_exception.rs`**

```rust
//! Terminal guard of the SubqueryRewrite stage: any Apply node still present
//! after the decorrelation rules means the subquery shape is unsupported.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{ApplyNode, LogicalPlan};

pub(crate) struct ApplyException;

impl LogicalRewriteRule for ApplyException {
    fn name(&self) -> &'static str {
        "ApplyException"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match &plan {
            LogicalPlan::Apply(node) => Err(apply_exception_message(node)),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

pub(super) fn apply_exception_message(node: &ApplyNode) -> String {
    format!(
        "subquery decorrelation failed: a residual Apply node (kind={:?}, correlated={}) \
         survived the SubqueryRewrite stage; this subquery shape is not yet supported. \
         Workaround: SET subquery_unnest_mode = 'legacy'",
        node.kind,
        !node.correlation_column_ids.is_empty()
    )
}
```

- [ ] **Step 3: 新文件 `src/sql/optimizer/rewrite/rules/subquery/mod.rs`**

```rust
//! SubqueryRewrite stage rules.
//!
//! M0 ships only the ApplyException terminal guard. The decorrelation rules
//! (push-down normalization, ApplyToWindow, *ApplyToJoin) land with M1+; see
//! docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §6.

mod apply_exception;

pub(crate) use apply_exception::ApplyException;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(ApplyException)]
}

/// Non-disableable backstop used by `optimize()`: returns the ApplyException
/// error if any Apply survived the rewrite pipeline (possible when the user
/// disabled the ApplyException rule via `disable_optimizer_rules`); a leaked
/// Apply must surface as a user-readable error, never as the memo-conversion
/// panic.
pub(crate) fn find_residual_apply(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Apply(node) => Some(apply_exception::apply_exception_message(node)),
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => None,
        LogicalPlan::Filter(n) => find_residual_apply(&n.input),
        LogicalPlan::Project(n) => find_residual_apply(&n.input),
        LogicalPlan::Aggregate(n) => find_residual_apply(&n.input),
        LogicalPlan::Join(n) => {
            find_residual_apply(&n.left).or_else(|| find_residual_apply(&n.right))
        }
        LogicalPlan::Sort(n) => find_residual_apply(&n.input),
        LogicalPlan::Limit(n) => find_residual_apply(&n.input),
        LogicalPlan::Union(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Intersect(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Except(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::TableFunction(n) => find_residual_apply(&n.input),
        LogicalPlan::Window(n) => find_residual_apply(&n.input),
        LogicalPlan::Repeat(n) => find_residual_apply(&n.input),
        LogicalPlan::CTEAnchor(n) => {
            find_residual_apply(&n.produce).or_else(|| find_residual_apply(&n.consumer))
        }
        LogicalPlan::CTEProduce(n) => find_residual_apply(&n.input),
        LogicalPlan::Decode(n) => find_residual_apply(&n.input),
        LogicalPlan::AggregateStateMerge(n) => {
            find_residual_apply(&n.old_input).or_else(|| find_residual_apply(&n.delta_input))
        }
        LogicalPlan::AssertOneRow(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvDelta(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvVersion(n) => find_residual_apply(&n.input),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{ApplyKind, ApplyNode, ValuesNode};

    fn apply_over_values() -> LogicalPlan {
        let values = || {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        };
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(values()),
            right: Box::new(values()),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    #[test]
    fn apply_exception_fails_residual_apply() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let err = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect_err("residual apply must fail the pipeline");
        assert!(
            err.contains("subquery decorrelation failed"),
            "unexpected error: {err}"
        );
        assert!(err.contains("kind=Scalar"), "unexpected error: {err}");
    }

    #[test]
    fn disabled_apply_exception_is_caught_by_backstop() {
        let mut ctx = RewriteContext::for_query(vec!["ApplyException".to_string()]);
        let rewritten = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect("pipeline passes with the rule disabled");
        let message = find_residual_apply(&rewritten).expect("backstop must detect the apply");
        assert!(message.contains("subquery decorrelation failed"));
    }

    #[test]
    fn find_residual_apply_ignores_plain_plans() {
        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        });
        assert!(find_residual_apply(&plan).is_none());
    }
}
```

- [ ] **Step 4: `rules/mod.rs` — 模块声明区（`pub(crate) mod predicate_pushdown;` 之后、`pub(crate) mod ukfk;` 之前，按字母序）加**

```rust
pub(crate) mod subquery;
```

- [ ] **Step 5: `registry.rs` — `query_rewrite_pipeline` 的 `from_stages(vec![...])` 头部（`PredicatePushdownPreJoin` stage 之前）插入**

```rust
        RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            rules::subquery::subquery_rewrite_rules(),
        ),
```

- [ ] **Step 6: `optimize()`（optimizer/mod.rs）在 `let rewritten = ...query_rewrite_pipeline(table_stats).rewrite(plan, &mut rewrite_ctx)?;` 之后插入 backstop**

```rust
    // Non-disableable backstop: Apply must not survive the SubqueryRewrite
    // stage. The ApplyException rule reports this with rule attribution, but
    // a user-disabled rule must not let an Apply leak into memo conversion
    // (which panics by contract).
    if let Some(message) = rewrite::rules::subquery::find_residual_apply(&rewritten) {
        return Err(message);
    }
```

- [ ] **Step 7: optimize() 层 backstop 集成测试（optimizer/mod.rs tests 模块）**

```rust
    #[test]
    fn optimize_rejects_residual_apply_when_rule_disabled() {
        use std::collections::{HashMap, HashSet};

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::{ColumnId, ColumnRefFactory};
        use crate::sql::planner::plan::{ApplyKind, ApplyNode, LogicalPlan, ValuesNode};

        let values = || {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        };
        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(values()),
            right: Box::new(values()),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let settings = crate::sql::optimizer::options::SessionOptimizerSettings {
            disabled_rules: vec!["ApplyException".to_string()],
            ..Default::default()
        };
        let err = crate::sql::optimizer::options::with_session_optimizer_settings(settings, || {
            optimize(plan, &HashMap::new(), ColumnRefFactory::new(), None)
        })
        .expect_err("backstop must reject the residual apply");
        assert!(
            err.contains("subquery decorrelation failed"),
            "unexpected error: {err}"
        );
    }
```

- [ ] **Step 8: 运行全部相关测试**

Run: `cargo test --lib subquery apply_exception query_pipeline_uses_expected_stage_order rewrite_registry_recognizes optimize_rejects_residual 2>&1 | tail -5`
Expected: 全部通过（含 Step 1 改过的两个 registry 测试）。

- [ ] **Step 9: 确认零行为变化——全量 lib 测试**

Run: `cargo test --lib 2>&1 | tail -3`
Expected: 全部通过。

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(optimizer): add SubqueryRewrite stage with ApplyException guard

New first stage of the query rewrite pipeline. M0 registers only the
ApplyException terminal rule (any residual Apply fails with an explicit
unsupported-shape error); decorrelation rules land with M1. optimize()
additionally runs a non-disableable residual-Apply backstop so that
disabling the rule via disable_optimizer_rules degrades to the same
error instead of the memo-conversion panic."
```

---

### Task 11: `subquery_unnest_mode` session 变量

**Files:**
- Modify: `src/sql/optimizer/options.rs`
- Modify: `src/server/mod.rs`

- [ ] **Step 1: 先写失败测试（options.rs tests 模块）**

```rust
    #[test]
    fn subquery_unnest_mode_parses_known_values() {
        assert_eq!(
            SubqueryUnnestMode::parse("legacy"),
            Some(SubqueryUnnestMode::Legacy)
        );
        assert_eq!(
            SubqueryUnnestMode::parse("APPLY"),
            Some(SubqueryUnnestMode::Apply)
        );
        assert_eq!(
            SubqueryUnnestMode::parse("apply_strict"),
            Some(SubqueryUnnestMode::ApplyStrict)
        );
        assert_eq!(SubqueryUnnestMode::parse("bogus"), None);
    }

    #[test]
    fn subquery_unnest_mode_defaults_to_legacy() {
        assert_eq!(
            SessionOptimizerSettings::default().subquery_unnest_mode,
            SubqueryUnnestMode::Legacy
        );
    }
```

Run: `cargo test --lib subquery_unnest_mode 2>&1 | tail -3`
Expected: FAIL（类型不存在，编译错误）。

- [ ] **Step 2: options.rs — 类型与字段**

在 `SessionOptimizerSettings` 之前加：

```rust
/// How subqueries are unnested. `Legacy` keeps the analyzer-time rewrite;
/// `Apply` routes white-listed shapes through LogicalApply + the
/// SubqueryRewrite stage; `ApplyStrict` errors instead of falling back for
/// unsupported shapes (CI / debugging aid). M0 only plumbs the setting —
/// analyzer routing consumes it starting with M1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SubqueryUnnestMode {
    #[default]
    Legacy,
    Apply,
    ApplyStrict,
}

impl SubqueryUnnestMode {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "legacy" => Some(Self::Legacy),
            "apply" => Some(Self::Apply),
            "apply_strict" => Some(Self::ApplyStrict),
            _ => None,
        }
    }
}
```

`SessionOptimizerSettings` 加字段（在 `disabled_rules` 之后）：

```rust
    /// Subquery unnesting routing mode. Read by M1 analyzer routing.
    #[allow(dead_code)]
    pub subquery_unnest_mode: SubqueryUnnestMode,
```

Run: `cargo test --lib subquery_unnest_mode 2>&1 | tail -3`
Expected: 2 passed。

- [ ] **Step 3: server/mod.rs — SET 处理（紧跟 `disable_optimizer_rules`/`cbo_disabled_rules` 的 for 循环块之后）**

```rust
    if let Some(values) = parse_set_string_csv(trimmed, "subquery_unnest_mode") {
        let mode = match values.as_slice() {
            [value] => crate::sql::optimizer::options::SubqueryUnnestMode::parse(value),
            _ => None,
        };
        let Some(mode) = mode else {
            return Err((
                ErrorKind::ER_WRONG_VALUE,
                format!(
                    "invalid subquery_unnest_mode value {values:?}; \
                     expected 'legacy', 'apply', or 'apply_strict'"
                ),
            ));
        };
        shim.optimizer_settings.subquery_unnest_mode = mode;
        return Ok(StatementResult::Ok);
    }
```

注意：与 `disable_optimizer_rules` 同款单引号取值约定（`SET subquery_unnest_mode = 'apply'`）；与既有变量不同，**非法取值显式报错**而不是静默吞掉——这是设计文档 §7.1 的要求（模式值必须有效）。

- [ ] **Step 4: server 解析测试（server/mod.rs tests 模块，放在 `parse_set_string_csv_*` 测试附近）**

```rust
    #[test]
    fn parse_set_string_csv_matches_subquery_unnest_mode() {
        assert_eq!(
            parse_set_string_csv(
                "SET subquery_unnest_mode = 'apply'",
                "subquery_unnest_mode"
            ),
            Some(vec!["apply".to_string()]),
        );
        assert_eq!(
            parse_set_string_csv(
                "SET subquery_unnest_mode = 'apply_strict'",
                "subquery_unnest_mode"
            ),
            Some(vec!["apply_strict".to_string()]),
        );
    }
```

- [ ] **Step 5: 运行**

Run: `cargo test --lib subquery_unnest 2>&1 | tail -3`
Expected: 全部通过。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(server): add subquery_unnest_mode session variable

SET subquery_unnest_mode = 'legacy' | 'apply' | 'apply_strict'
(single-quoted, same convention as disable_optimizer_rules). Invalid
values are rejected explicitly. Default is legacy; M0 only plumbs the
setting — analyzer routing consumes it starting with M1."
```

---

### Task 12: 最终验证

- [ ] **Step 1: 格式与静态检查**

```bash
cargo fmt
cargo clippy 2>&1 | tail -5
```

Expected: fmt 无 diff 或仅本次新代码的格式化；clippy 无新增 warning（若 clippy 对新代码报 warning，修掉而不是 allow——`#[allow(dead_code)]` 仅限计划里明确标注的 M1 留口）。

- [ ] **Step 2: 全量构建 + 测试**

```bash
cargo build
cargo test 2>&1 | tail -5
```

Expected: 全部通过。

- [ ] **Step 3: 行为不变抽查（可选，需要本地测试环境）**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo build --profile dev-opt
# 启动 standalone-server 后：
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify -j 1
```

Expected: optimizer suite 全绿、无 golden 变化（M0 零行为变化）。

- [ ] **Step 4: 若 Step 1 的 fmt 产生了 diff，追加 fixup commit**

```bash
git add -A
git diff --cached --quiet || git commit -m "style: cargo fmt for M0 apply scaffolding"
```

---

## 验收清单（对照设计文档 §7.3 M0）

- [x 计划覆盖] `ApplyKind` / `ApplyNode` / `AssertOneRowNode` + 两个 `LogicalPlan` 变体（Task 1）
- [x 计划覆盖] §5.6 全部 match arm：planner（Task 2）、tree walker + trip-wire（Task 3）、rewrite 长尾（Task 4）、IMV/engine（Task 5）、memo/stats/cost/props（Task 6）、derive/implement（Task 7）、explain/codegen/verifier（Task 8）
- [x 计划覆盖] `SubqueryRewrite` 空 stage + `ApplyException` + 不可禁用 backstop（Task 10）
- [x 计划覆盖] `subquery_unnest_mode`（默认 `legacy`）（Task 11）
- [x 计划覆盖] logical EXPLAIN 的 `APPLY` / `ASSERT ONE ROW` 格式（Task 8 + Task 9 测试）
- [x 计划覆盖] 验收：现有 suite 不变、trip-wire 通过（Task 8 Step 6、Task 12）

**M0 不做**（M1 范围，另出 plan）：analyzer `SubqueryInfo` 升级与路由、planner Apply 构造、PushDownApply* / NormalizeCount / ScalarApplyToJoin 规则、AssertOneRow 从 SQL 路径的真实产生、`apply_strict` CI 接入。
