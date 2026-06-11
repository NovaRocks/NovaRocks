# OQ-2 Join key 自动 NULL filter 推导 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增逻辑改写规则 `DeriveJoinNotNullPredicate`,对 Inner/LeftSemi/RightSemi join 在安全侧的等值 key 列上自动推导 `IS NOT NULL` 并经现有 pushdown 下沉到 scan,过滤永不匹配的 NULL 行。

**Architecture:** 在 `query_rewrite_pipeline` 的 `PredicatePushdownPostJoin` stage 追加一条 `PlanRewriteRule`。规则 match 裸 `Join`,按 join 类型表决定安全侧,从 ON 顶层 AND 链抽 equi-key 操作数,克隆操作数构造 `IS NOT NULL`,wrap 到对应 child 上;同 stage 的 `PushDownPredicate*` 在同一 fixed-point loop 内把它推到 scan。幂等靠「沿 child 谓词脊检测该列是否已保证非空」实现,不给 `JoinNode` 加字段。

**Tech Stack:** Rust;`src/sql/optimizer/rewrite/`(rule trait `PlanRewriteRule`、pipeline、`utils.rs`);golden 测试 `sql-tests/optimizer/`(EXPLAIN VERBOSE + `-- @explain_contains` + record/verify)。

**Spec:** `docs/design/specs/2026-05-30-oq-2-join-not-null-derivation-design.md`

**设计偏差说明(已在实现前确认):** spec §4.1 原写「抽取并泛化 ukfk helper、适配 ukfk 调用点」。本计划改为**只向 `utils.rs` 新增**一套 lenient、operand-returning 的 equi-key helper(`join_equi_keys`),**不改动 `ukfk.rs`** 的 strict、name-based `join_equality_pairs`(它服务 FK 全等匹配,语义不同)。理由:OQ-1(#208)刚带 documented regression 落地,零 ukfk churn 风险最低;spec 的真实意图(共享 helper 落在 utils、规则文件不重写遍历)已满足。

---

## File Structure

- **Modify** `src/sql/optimizer/rewrite/rules/utils.rs` — 新增 lenient equi-key 抽取:`JoinSide`、`unwrap_column_ref`、`classify_operand`、`JoinEquiKey`、`join_equi_keys`、`collect_join_equi_keys`,以及单元测试。
- **Create** `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs` — `DeriveJoinNotNullPredicate` 规则:`safe_sides` 表、四条 gate、克隆操作数构造 `IS NOT NULL`、谓词脊幂等、`wrap_not_null`,以及单元测试。
- **Modify** `src/sql/optimizer/rewrite/rules/mod.rs` — 声明新模块;把规则加入 `all_query_rewrite_rules`(test-only inventory);更新 `registry_contains_expected_rules` 测试。
- **Modify** `src/sql/optimizer/rewrite/registry.rs` — 把规则追加到 `PredicatePushdownPostJoin` stage;更新 `query_pipeline_contains_migrated_query_rules` 测试。
- **Create** `sql-tests/optimizer/sql/derive_join_not_null_inner.sql` / `_leftsemi.sql` / `_anti.sql` / `_disabled.sql` + 对应 `sql-tests/optimizer/result/` golden(record 生成)。

---

## Task 1: lenient equi-key helper(`utils.rs`)

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`(在文件末尾的 `#[cfg(test)] mod column_id_helper_tests` 之前插入实现;测试加到该模块内)

- [ ] **Step 1: 写失败测试**

在 `src/sql/optimizer/rewrite/rules/utils.rs` 的 `mod column_id_helper_tests` 内追加(该模块已 `use super::*;` 并已有 `make_output_column`、`ColumnId`、`DataType`、`ScanSource`、`TableDef`、`ColumnDef`):

```rust
// ---------------------------------------------------------------------
// join_equi_keys tests
// ---------------------------------------------------------------------

fn nullable_scan(alias: &str, table: &str, cols: &[(&str, u32)]) -> LogicalPlan {
    let column_defs = cols
        .iter()
        .map(|(name, _)| ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
            write_default: None,
            logical_type: None,
        })
        .collect();
    let output = cols
        .iter()
        .map(|(name, id)| OutputColumn {
            column_id: ColumnId::new_for_test(*id),
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
            is_internal: false,
        })
        .collect();
    LogicalPlan::Scan(ScanNode {
        database: "default".to_string(),
        table: TableDef {
            name: table.to_string(),
            columns: column_defs,
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
        },
        alias: Some(alias.to_string()),
        columns: output,
        predicates: vec![],
        required_columns: None,
        dict_columns: vec![],
        required_output_columns: None,
    })
}

fn qcol(qualifier: &str, name: &str, id: u32) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(id),
            qualifier: Some(qualifier.to_string()),
            column: name.to_string(),
        },
        data_type: DataType::Int32,
        nullable: true,
    }
}

fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: crate::sql::analysis::BinOp::Eq,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: true,
    }
}

fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: crate::sql::analysis::BinOp::And,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: true,
    }
}

fn two_table_join(condition: Option<TypedExpr>) -> JoinNode {
    JoinNode {
        left: Box::new(nullable_scan("l", "tl", &[("a", 1), ("a2", 3)])),
        right: Box::new(nullable_scan("r", "tr", &[("b", 2), ("b2", 4)])),
        join_type: crate::sql::analysis::JoinKind::Inner,
        condition,
        required_output_columns: None,
    }
}

#[test]
fn join_equi_keys_extracts_single_pair_oriented_left_right() {
    let join = two_table_join(Some(eq_expr(qcol("l", "a", 1), qcol("r", "b", 2))));
    let keys = join_equi_keys(&join);
    assert_eq!(keys.len(), 1);
    // left operand belongs to join.left, right operand to join.right.
    assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
    assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { column, .. } if column == "b"));
}

#[test]
fn join_equi_keys_orients_reversed_pair() {
    // r.b = l.a  -> still left=a, right=b
    let join = two_table_join(Some(eq_expr(qcol("r", "b", 2), qcol("l", "a", 1))));
    let keys = join_equi_keys(&join);
    assert_eq!(keys.len(), 1);
    assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
    assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { column, .. } if column == "b"));
}

#[test]
fn join_equi_keys_collects_each_and_conjunct() {
    let join = two_table_join(Some(and_expr(
        eq_expr(qcol("l", "a", 1), qcol("r", "b", 2)),
        eq_expr(qcol("l", "a2", 3), qcol("r", "b2", 4)),
    )));
    let keys = join_equi_keys(&join);
    assert_eq!(keys.len(), 2);
}

#[test]
fn join_equi_keys_skips_non_equi_and_missing_condition() {
    assert!(join_equi_keys(&two_table_join(None)).is_empty());
    let gt = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(qcol("l", "a", 1)),
            op: crate::sql::analysis::BinOp::Gt,
            right: Box::new(qcol("r", "b", 2)),
        },
        data_type: DataType::Boolean,
        nullable: true,
    };
    assert!(join_equi_keys(&two_table_join(Some(gt))).is_empty());
}

#[test]
fn join_equi_keys_disambiguates_self_join_by_qualifier() {
    // q22 shape: same column name on both sides, distinct aliases.
    let join = JoinNode {
        left: Box::new(nullable_scan("a", "t", &[("k", 1)])),
        right: Box::new(nullable_scan("b", "t", &[("k", 2)])),
        join_type: crate::sql::analysis::JoinKind::LeftSemi,
        condition: Some(eq_expr(qcol("a", "k", 1), qcol("b", "k", 2))),
        required_output_columns: None,
    };
    let keys = join_equi_keys(&join);
    assert_eq!(keys.len(), 1);
    assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "a"));
    assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "b"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p novarocks join_equi_keys 2>&1 | tail -20`
Expected: 编译失败 / `cannot find function join_equi_keys`(实现尚未存在)。

- [ ] **Step 3: 实现 helper**

在 `src/sql/optimizer/rewrite/rules/utils.rs` 中,`#[cfg(test)]` 模块之前插入(文件顶部已 `use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};`、`use std::collections::HashSet;`、`use crate::sql::planner::plan::*;`,`QualifiedRef` 与 `collect_qualified_output_columns` 已定义于本文件):

```rust
/// One equi-join key pair, with operands oriented so `left` comes from the
/// join's left child and `right` from the right child. Operands are the
/// unwrapped inner `ColumnRef` (Cast/Nested peeled).
pub(crate) struct JoinEquiKey {
    pub(crate) left: TypedExpr,
    pub(crate) right: TypedExpr,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

/// Peel `Cast` / `Nested` wrappers and return the inner `ColumnRef` expr, if any.
fn unwrap_column_ref(expr: &TypedExpr) -> Option<&TypedExpr> {
    match &expr.kind {
        ExprKind::ColumnRef { .. } => Some(expr),
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => unwrap_column_ref(expr),
        _ => None,
    }
}

/// Classify a join-condition operand as left/right child column, returning the
/// unwrapped inner `ColumnRef` clone. `None` if it is not a column unambiguously
/// owned by exactly one side (constants, expressions, ambiguous self-join refs).
fn classify_operand(
    expr: &TypedExpr,
    left_cols: &HashSet<QualifiedRef>,
    right_cols: &HashSet<QualifiedRef>,
) -> Option<(JoinSide, TypedExpr)> {
    let inner = unwrap_column_ref(expr)?;
    let ExprKind::ColumnRef {
        qualifier, column, ..
    } = &inner.kind
    else {
        return None;
    };
    let key = (
        qualifier.as_ref().map(|q| q.to_lowercase()),
        column.to_lowercase(),
    );
    match (left_cols.contains(&key), right_cols.contains(&key)) {
        (true, false) => Some((JoinSide::Left, inner.clone())),
        (false, true) => Some((JoinSide::Right, inner.clone())),
        _ => None,
    }
}

/// Extract equi-join key pairs from a join's ON condition (lenient: walks the
/// top-level AND chain and keeps every `col = col` conjunct it can orient,
/// ignoring other conjuncts). Returns empty when there is no usable equi key.
pub(crate) fn join_equi_keys(join: &JoinNode) -> Vec<JoinEquiKey> {
    let Some(condition) = join.condition.as_ref() else {
        return Vec::new();
    };
    let left_cols = collect_qualified_output_columns(&join.left);
    let right_cols = collect_qualified_output_columns(&join.right);
    let mut keys = Vec::new();
    collect_join_equi_keys(condition, &left_cols, &right_cols, &mut keys);
    keys
}

fn collect_join_equi_keys(
    expr: &TypedExpr,
    left_cols: &HashSet<QualifiedRef>,
    right_cols: &HashSet<QualifiedRef>,
    keys: &mut Vec<JoinEquiKey>,
) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_equi_keys(left, left_cols, right_cols, keys);
            collect_join_equi_keys(right, left_cols, right_cols, keys);
        }
        // Only strict `Eq`. `EqForNull` (<=>) is null-safe (NULL <=> NULL is
        // true), so deriving IS NOT NULL on its operands would change results;
        // it is intentionally excluded (matches StarRocks `isEqual()`).
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (
            classify_operand(left, left_cols, right_cols),
            classify_operand(right, left_cols, right_cols),
        ) {
            (Some((JoinSide::Left, le)), Some((JoinSide::Right, re)))
            | (Some((JoinSide::Right, re)), Some((JoinSide::Left, le))) => {
                keys.push(JoinEquiKey { left: le, right: re });
            }
            _ => {}
        },
        _ => {}
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p novarocks join_equi_keys 2>&1 | tail -20`
Expected: 4 个 `join_equi_keys_*` 测试 PASS。

- [ ] **Step 5: fmt + 提交**

```bash
cargo fmt
git add src/sql/optimizer/rewrite/rules/utils.rs
git commit -m "feat(optimizer): add lenient equi-join-key extraction helper (OQ-2)

join_equi_keys() walks a join ON condition's top-level AND chain and returns
each col=col conjunct as a left/right-oriented operand pair (Cast/Nested
peeled). Distinct from ukfk's strict name-based join_equality_pairs: lenient
(keeps usable equi keys, ignores other conjuncts) and operand-returning, for
the OQ-2 NULL-filter derivation rule.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `DeriveJoinNotNullPredicate` 规则

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs`

- [ ] **Step 1: 写规则骨架 + 失败测试**

创建 `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs`,先写实现骨架(`apply` 永远 `None`)与完整测试,让测试先失败:

```rust
//! OQ-2: derive `IS NOT NULL` predicates on equi-join keys for null-rejecting
//! join types (Inner / LeftSemi / RightSemi), mirroring StarRocks
//! `JoinPredicatePushdown.deriveIsNotNullPredicate`. The derived `Filter` is
//! pushed to the scan by the existing PredicatePushdownPostJoin pushdown rules
//! running in the same fixed-point loop.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{combine_and, join_equi_keys, split_and};
use crate::sql::planner::plan::*;

pub(crate) struct DeriveJoinNotNullPredicate;

/// `(derive_left, derive_right)`: which sides' equi-keys may receive IS NOT NULL.
/// Mirrors StarRocks: inner -> both; left-semi -> right; right-semi -> left.
/// Anti / null-aware-anti / outer / cross -> neither (see spec §3.1).
fn safe_sides(join_type: JoinKind) -> (bool, bool) {
    match join_type {
        JoinKind::Inner => (true, true),
        JoinKind::LeftSemi => (false, true),
        JoinKind::RightSemi => (true, false),
        JoinKind::LeftAnti
        | JoinKind::RightAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::LeftOuter
        | JoinKind::RightOuter
        | JoinKind::FullOuter
        | JoinKind::Cross => (false, false),
    }
}

impl PlanRewriteRule for DeriveJoinNotNullPredicate {
    fn name(&self) -> &'static str {
        "DeriveJoinNotNullPredicate"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Join(_))
    }

    fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};

    fn scan(alias: &str, table: &str, cols: &[(&str, u32, bool)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: table.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _, nullable)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: *nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
            },
            alias: Some(alias.to_string()),
            columns: cols
                .iter()
                .map(|(name, id, nullable)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: *nullable,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col(qualifier: &str, name: &str, id: u32, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn join(jt: JoinKind, left: LogicalPlan, right: LogicalPlan, cond: Option<TypedExpr>) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: jt,
            condition: cond,
            required_output_columns: None,
        })
    }

    /// (left_child_is_filter, right_child_is_filter) for the rule's output.
    fn side_filters(out: Option<LogicalPlan>) -> (bool, bool) {
        match out {
            None => (false, false),
            Some(LogicalPlan::Join(j)) => (
                matches!(*j.left, LogicalPlan::Filter(_)),
                matches!(*j.right, LogicalPlan::Filter(_)),
            ),
            Some(_) => panic!("rule must return a Join"),
        }
    }

    /// Count IS NOT NULL conjuncts in a Filter's predicate (0 if not a Filter).
    fn not_null_count(plan: &LogicalPlan) -> usize {
        let LogicalPlan::Filter(f) = plan else { return 0 };
        split_and(f.predicate.clone())
            .iter()
            .filter(|e| matches!(&e.kind, ExprKind::IsNull { negated: true, .. }))
            .count()
    }

    fn inner_eq_join(left_nullable: bool, right_nullable: bool) -> LogicalPlan {
        join(
            JoinKind::Inner,
            scan("l", "tl", &[("a", 1, left_nullable)]),
            scan("r", "tr", &[("b", 2, right_nullable)]),
            Some(eq(col("l", "a", 1, left_nullable), col("r", "b", 2, right_nullable))),
        )
    }

    #[test]
    fn join_type_safety_table() {
        let cases = [
            (JoinKind::Inner, true, true),
            (JoinKind::LeftSemi, false, true),
            (JoinKind::RightSemi, true, false),
            (JoinKind::LeftAnti, false, false),
            (JoinKind::RightAnti, false, false),
            (JoinKind::NullAwareLeftAnti, false, false),
            (JoinKind::LeftOuter, false, false),
            (JoinKind::RightOuter, false, false),
            (JoinKind::FullOuter, false, false),
            (JoinKind::Cross, false, false),
        ];
        for (jt, exp_l, exp_r) in cases {
            let cond = if matches!(jt, JoinKind::Cross) {
                None
            } else {
                Some(eq(col("l", "a", 1, true), col("r", "b", 2, true)))
            };
            let plan = join(jt, scan("l", "tl", &[("a", 1, true)]), scan("r", "tr", &[("b", 2, true)]), cond);
            assert_eq!(side_filters(DeriveJoinNotNullPredicate.apply(plan)), (exp_l, exp_r), "join type {jt:?}");
        }
    }

    #[test]
    fn non_nullable_keys_are_skipped() {
        assert_eq!(side_filters(DeriveJoinNotNullPredicate.apply(inner_eq_join(false, false))), (false, false));
        // Only the nullable side gets a filter.
        assert_eq!(side_filters(DeriveJoinNotNullPredicate.apply(inner_eq_join(true, false))), (true, false));
    }

    #[test]
    fn composite_inner_key_derives_all_columns_on_each_side() {
        let plan = join(
            JoinKind::Inner,
            scan("l", "tl", &[("a", 1, true), ("a2", 3, true)]),
            scan("r", "tr", &[("b", 2, true), ("b2", 4, true)]),
            Some(and(
                eq(col("l", "a", 1, true), col("r", "b", 2, true)),
                eq(col("l", "a2", 3, true), col("r", "b2", 4, true)),
            )),
        );
        let Some(LogicalPlan::Join(j)) = DeriveJoinNotNullPredicate.apply(plan) else {
            panic!("expected join");
        };
        assert_eq!(not_null_count(&j.left), 2);
        assert_eq!(not_null_count(&j.right), 2);
    }

    #[test]
    fn idempotent_second_apply_is_noop() {
        let once = DeriveJoinNotNullPredicate.apply(inner_eq_join(true, true)).expect("first applies");
        // Second application over the already-derived plan must not change it.
        assert!(DeriveJoinNotNullPredicate.apply(once).is_none());
    }

    #[test]
    fn non_equi_and_missing_condition_skipped() {
        assert!(DeriveJoinNotNullPredicate
            .apply(join(JoinKind::Inner, scan("l", "tl", &[("a", 1, true)]), scan("r", "tr", &[("b", 2, true)]), None))
            .is_none());
        let gt = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col("l", "a", 1, true)),
                op: BinOp::Gt,
                right: Box::new(col("r", "b", 2, true)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        };
        assert!(DeriveJoinNotNullPredicate
            .apply(join(JoinKind::Inner, scan("l", "tl", &[("a", 1, true)]), scan("r", "tr", &[("b", 2, true)]), Some(gt)))
            .is_none());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p novarocks derive_join_not_null 2>&1 | tail -25`
Expected: `join_type_safety_table` 等多个测试 FAIL(骨架 `apply` 恒返回 `None`,Inner/Semi 期望有 filter)。

- [ ] **Step 3: 实现 `apply` + 辅助函数**

把 Step 1 中 `impl PlanRewriteRule for DeriveJoinNotNullPredicate` 的 `apply` 替换为完整实现,并在 `impl` 块之后(测试模块之前)加入辅助函数:

```rust
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        let (derive_left, derive_right) = safe_sides(join.join_type);
        if !derive_left && !derive_right {
            return None;
        }
        let keys = join_equi_keys(&join);
        if keys.is_empty() {
            return None;
        }

        let left_preds = if derive_left {
            eligible_not_null(&join.left, keys.iter().map(|k| &k.left))
        } else {
            Vec::new()
        };
        let right_preds = if derive_right {
            eligible_not_null(&join.right, keys.iter().map(|k| &k.right))
        } else {
            Vec::new()
        };
        if left_preds.is_empty() && right_preds.is_empty() {
            return None;
        }

        let JoinNode {
            left,
            right,
            join_type,
            condition,
            required_output_columns,
        } = join;
        Some(LogicalPlan::Join(JoinNode {
            left: Box::new(wrap_not_null(*left, left_preds)),
            right: Box::new(wrap_not_null(*right, right_preds)),
            join_type,
            condition,
            required_output_columns,
        }))
    }
```

辅助函数(放在 `impl` 块外、`#[cfg(test)]` 前):

```rust
/// For each candidate key operand (a ColumnRef from `child`), build the
/// `IS NOT NULL` predicates to add: keep only operands that are (a) nullable and
/// (b) not already guaranteed non-null by `child`'s predicate spine. Dedupe by
/// column identity within the side.
fn eligible_not_null<'a>(
    child: &LogicalPlan,
    operands: impl Iterator<Item = &'a TypedExpr>,
) -> Vec<TypedExpr> {
    let (guaranteed_ids, guaranteed_names) = spine_not_null(child);
    let mut seen_ids: HashSet<ColumnId> = HashSet::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut preds = Vec::new();
    for operand in operands {
        if !operand.nullable {
            continue;
        }
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &operand.kind
        else {
            continue;
        };
        let name = column.to_lowercase();
        if (*column_id != ColumnId::UNSET && guaranteed_ids.contains(column_id))
            || guaranteed_names.contains(&name)
        {
            continue; // already guaranteed non-null -> idempotency
        }
        let fresh = if *column_id != ColumnId::UNSET {
            seen_ids.insert(*column_id)
        } else {
            seen_names.insert(name.clone())
        };
        if fresh {
            preds.push(is_not_null(operand.clone()));
        }
    }
    preds
}

fn is_not_null(operand: TypedExpr) -> TypedExpr {
    TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::IsNull {
            expr: Box::new(operand),
            negated: true,
        },
    }
}

fn wrap_not_null(child: LogicalPlan, preds: Vec<TypedExpr>) -> LogicalPlan {
    if preds.is_empty() {
        return child;
    }
    LogicalPlan::Filter(FilterNode {
        input: Box::new(child),
        predicate: combine_and(preds),
        required_output_columns: None,
    })
}

/// Walk `plan`'s predicate spine (passthrough single-input nodes down to the
/// root scan) collecting column identities already guaranteed non-null by an
/// `IS NOT NULL` conjunct. Used for idempotency / redundant-filter avoidance.
fn spine_not_null(plan: &LogicalPlan) -> (HashSet<ColumnId>, HashSet<String>) {
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    spine_not_null_inner(plan, &mut ids, &mut names);
    (ids, names)
}

fn spine_not_null_inner(
    plan: &LogicalPlan,
    ids: &mut HashSet<ColumnId>,
    names: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::Filter(f) => {
            for conj in split_and(f.predicate.clone()) {
                record_not_null(&conj, ids, names);
            }
            spine_not_null_inner(&f.input, ids, names);
        }
        LogicalPlan::Scan(s) => {
            for p in &s.predicates {
                record_not_null(p, ids, names);
            }
        }
        LogicalPlan::Project(p) => spine_not_null_inner(&p.input, ids, names),
        LogicalPlan::SubqueryAlias(s) => spine_not_null_inner(&s.input, ids, names),
        LogicalPlan::Sort(s) => spine_not_null_inner(&s.input, ids, names),
        LogicalPlan::Limit(l) => spine_not_null_inner(&l.input, ids, names),
        _ => {}
    }
}

fn record_not_null(expr: &TypedExpr, ids: &mut HashSet<ColumnId>, names: &mut HashSet<String>) {
    if let ExprKind::IsNull {
        expr: inner,
        negated: true,
    } = &expr.kind
    {
        if let ExprKind::ColumnRef {
            column_id, column, ..
        } = &inner.kind
        {
            if *column_id != ColumnId::UNSET {
                ids.insert(*column_id);
            }
            names.insert(column.to_lowercase());
        }
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p novarocks derive_join_not_null 2>&1 | tail -25`
Expected: 全部 5 个测试 PASS。

- [ ] **Step 5: clippy + fmt + 提交**

Run: `cargo clippy -p novarocks 2>&1 | tail -15`(无新 warning)

```bash
cargo fmt
git add src/sql/optimizer/rewrite/rules/derive_join_not_null.rs
git commit -m "feat(optimizer): DeriveJoinNotNullPredicate rule (OQ-2)

Derives IS NOT NULL on equi-join keys for Inner (both sides), LeftSemi (right)
and RightSemi (left), mirroring StarRocks deriveIsNotNullPredicate. Anti /
null-aware-anti / outer / cross derive nothing. Per-column gates: nullable, not
already guaranteed non-null along the child predicate spine (idempotent). The
wrapped Filter is pushed to scan by the existing PostJoin pushdown rules.

Not yet registered in the pipeline (next task).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: 注册进 pipeline + 更新 registry 测试

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [ ] **Step 1: 声明模块并加入 inventory**

在 `src/sql/optimizer/rewrite/rules/mod.rs` 的模块声明区(`pub(crate) mod column_pruning;` 一组)按字母序加入:

```rust
pub(crate) mod derive_join_not_null;
```

在同文件 `all_query_rewrite_rules`(test-only inventory)的 `all.extend(...)` 序列末尾加入一行,使其与生产 pipeline 内容一致:

```rust
    all.push(Box::new(derive_join_not_null::DeriveJoinNotNullPredicate));
```

- [ ] **Step 2: 更新 `registry_contains_expected_rules` 测试**

在 `src/sql/optimizer/rewrite/rules/mod.rs` 的该测试中:把 `assert_eq!(rules.len(), 28);` 改为 `29`,并把注释末尾 `= 28` 改为 `= 29`;在排序名单里(`"AggregatePushdown",` 之后、`"EliminateUniqueAggregate",` 之前)插入一行:

```rust
                "DeriveJoinNotNullPredicate",
```

- [ ] **Step 3: 把规则追加到 PostJoin stage**

在 `src/sql/optimizer/rewrite/registry.rs::query_rewrite_pipeline` 中,把 `PredicatePushdownPostJoin` stage 的构造替换为(其余 stage 不变):

```rust
        RewriteStage::new(
            "PredicatePushdownPostJoin",
            RewritePhase::StructuralRewrite,
            {
                let mut rules = rules::predicate_pushdown_rules();
                rules.push(Box::new(
                    rules::derive_join_not_null::DeriveJoinNotNullPredicate,
                ));
                rules
            },
        ),
```

- [ ] **Step 4: 更新 `query_pipeline_contains_migrated_query_rules` 测试**

在 `src/sql/optimizer/rewrite/registry.rs` 的该测试的排序名单里(`"AggregatePushdown",` 之后)插入一行:

```rust
                "DeriveJoinNotNullPredicate",
```

> 该名单是 `query_rewrite_pipeline` 全部 stage 的规则名(含重复)排序后的结果;`DeriveJoinNotNullPredicate` 只在 PostJoin 出现一次,故只加一行。

- [ ] **Step 5: 跑相关测试 + disable 可见性**

Run: `cargo test -p novarocks -- rewrite::rules::tests::registry_contains_expected_rules rewrite::registry::tests::query_pipeline_contains_migrated_query_rules 2>&1 | tail -25`
Expected: 两个测试 PASS。

补一个 disable 校验测试,确认规则名进入 `is_known_rewrite_rule_name`。在 `src/sql/optimizer/rewrite/registry.rs` 的 `mod tests` 内 `rewrite_registry_recognizes_migrated_query_rules` 测试中追加一行断言:

```rust
        assert!(is_known_rewrite_rule_name("DeriveJoinNotNullPredicate"));
```

Run: `cargo test -p novarocks rewrite_registry_recognizes_migrated_query_rules 2>&1 | tail -10`
Expected: PASS。

- [ ] **Step 6: 全量 build + 提交**

Run: `cargo build 2>&1 | tail -15`
Expected: 编译通过。

```bash
cargo fmt
git add src/sql/optimizer/rewrite/rules/mod.rs src/sql/optimizer/rewrite/registry.rs
git commit -m "feat(optimizer): register DeriveJoinNotNullPredicate in PostJoin stage (OQ-2)

Append the rule to the PredicatePushdownPostJoin stage so the derived
IS NOT NULL filters are pushed to scans in the same fixed-point loop. Update
both registry rule-name tests and the disable-recognition test.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Golden plan 测试(record)

**Files:**
- Create: `sql-tests/optimizer/sql/derive_join_not_null_inner.sql`
- Create: `sql-tests/optimizer/sql/derive_join_not_null_leftsemi.sql`
- Create: `sql-tests/optimizer/sql/derive_join_not_null_anti.sql`
- Create: `sql-tests/optimizer/sql/derive_join_not_null_disabled.sql`
- Create (record 生成): 对应 `sql-tests/optimizer/result/*.result`

前置:standalone-server 已起(见 Task 5 Step 1 的启动方式),并 `source docker/iceberg-rest/runtime/current/env.sh`。

- [ ] **Step 1: 写四个 golden .sql**

`derive_join_not_null_inner.sql`:

```sql
-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- INNER JOIN on nullable keys derives IS NOT NULL on BOTH scan sides.
DROP TABLE IF EXISTS ${case_db}.t_dnn_l;
DROP TABLE IF EXISTS ${case_db}.t_dnn_r;
CREATE TABLE ${case_db}.t_dnn_l (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_r (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_l
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_r
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_l l
INNER JOIN ${case_db}.t_dnn_r r ON l.k = r.k;
```

`derive_join_not_null_leftsemi.sql`:

```sql
-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- LEFT SEMI JOIN on nullable keys derives IS NOT NULL on the RIGHT (build)
-- side only; the left (probe) side is unchanged (StarRocks-faithful).
DROP TABLE IF EXISTS ${case_db}.t_dnn_sl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_sr;
CREATE TABLE ${case_db}.t_dnn_sl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_sr (k INT);
INSERT INTO ${case_db}.t_dnn_sl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_sr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(generate_series(1, 2000));
EXPLAIN VERBOSE
SELECT l.v
FROM ${case_db}.t_dnn_sl l
LEFT SEMI JOIN ${case_db}.t_dnn_sr r ON l.k = r.k;
```

`derive_join_not_null_anti.sql`(负向:不应出现 IS NOT NULL):

```sql
-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- LEFT ANTI JOIN must NOT derive any IS NOT NULL (left NULL keys are emitted).
-- The recorded golden is the regression guard for absence.
DROP TABLE IF EXISTS ${case_db}.t_dnn_al;
DROP TABLE IF EXISTS ${case_db}.t_dnn_ar;
CREATE TABLE ${case_db}.t_dnn_al (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_ar (k INT);
INSERT INTO ${case_db}.t_dnn_al
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_ar
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(generate_series(1, 2000));
EXPLAIN VERBOSE
SELECT l.v
FROM ${case_db}.t_dnn_al l
LEFT ANTI JOIN ${case_db}.t_dnn_ar r ON l.k = r.k;
```

`derive_join_not_null_disabled.sql`:

```sql
-- @tags=optimizer,derive_join_not_null,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='DeriveJoinNotNullPredicate' suppresses the
-- derivation. The two EXPLAIN VERBOSE outputs around the SET must differ:
-- first has IS NOT NULL on the scans, second does not.
DROP TABLE IF EXISTS ${case_db}.t_dnn_dl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_dr;
CREATE TABLE ${case_db}.t_dnn_dl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_dr (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_dl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_dr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));

EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = 'DeriveJoinNotNullPredicate';

EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = '';
```

- [ ] **Step 2: record golden**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode record \
  --only derive_join_not_null_inner,derive_join_not_null_leftsemi,derive_join_not_null_anti,derive_join_not_null_disabled
```
Expected: 4 个 case recorded,生成 `sql-tests/optimizer/result/derive_join_not_null_*.result`。

- [ ] **Step 3: 人工核对 golden 内容**

Read `sql-tests/optimizer/result/derive_join_not_null_inner.result`:确认两个 scan 都带 `IS NOT NULL`(记下 EXPLAIN 渲染的确切子串,例如 `k IS NOT NULL`)。
Read `sql-tests/optimizer/result/derive_join_not_null_leftsemi.result`:确认**只有 build/右**侧 scan 带 `IS NOT NULL`,probe/左侧没有。
Read `sql-tests/optimizer/result/derive_join_not_null_anti.result`:确认**没有** `IS NOT NULL`。
Read `sql-tests/optimizer/result/derive_join_not_null_disabled.result`:确认第一段 EXPLAIN 有 `IS NOT NULL`、第二段没有。

若 leftsemi/inner 的 `IS NOT NULL` 没有落到 scan(只停在 SubqueryAlias 之上),记录现象到 PR 描述(spec §5 caveat),但只要谓词出现在 build 输入之前即算达成核心目标。

- [ ] **Step 4: 给正向 case 补 `@explain_contains`**

用 Step 3 观察到的确切子串,在 `derive_join_not_null_inner.sql` 与 `derive_join_not_null_leftsemi.sql` 的头部(`-- @tags=...` 之后)各加一行,锁定 plan 形状。示例(以实际渲染为准替换 `<observed substring>`):

```sql
-- @explain_contains=<observed substring>
```

- [ ] **Step 5: verify 复跑确认稳定**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify \
  --only derive_join_not_null_inner,derive_join_not_null_leftsemi,derive_join_not_null_anti,derive_join_not_null_disabled
```
Expected: 4/4 PASS(`@explain_contains` 命中,golden 一致)。

- [ ] **Step 6: 提交**

```bash
git add sql-tests/optimizer/sql/derive_join_not_null_*.sql sql-tests/optimizer/result/derive_join_not_null_*.result
git commit -m "test(optimizer): golden plan cases for OQ-2 NULL filter derivation

inner (both sides), left-semi (right only), anti (negative: none), and a
disable case. Positive cases pin the IS NOT NULL via @explain_contains.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 验证、基线与 FE plan diff

**Files:** 无代码改动(更新 roadmap 进度文本除外)。

- [ ] **Step 1: 起 standalone-server(release,用于 suite)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo build --release
LOG=/tmp/novarocks-oq2.log
NO_PROXY=127.0.0.1,localhost target/release/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 120); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died:"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```
Expected: 日志出现 `NOVAROCKS_READY mysql_port=... pid=...`。

- [ ] **Step 2: 建立改动前基线(切回基线 commit 跑一次)**

> 因 OQ-1(#208)自述带 regression,先记录“引入 OQ-2 之前”的四套结果,作为对比锚点。

```bash
git stash -u 2>/dev/null; BASE=$(git rev-parse HEAD)
# Baseline = the commit just before OQ-2's first CODE commit (Task 1's utils
# commit), found by message so it is robust to the number of commits.
FIRST_CODE=$(git log --reverse --format='%H %s' | grep -m1 'add lenient equi-join-key extraction helper' | cut -d' ' -f1)
BASELINE=$(git rev-parse "${FIRST_CODE}^")
git checkout "$BASELINE"
# 重启 server(同 Step 1),然后:
for s in join cte aggregate filter; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite $s --mode verify -j 1 \
    2>&1 | tee /tmp/oq2-base-$s.log | tail -3
done
git checkout "$BASE"
```
Expected: 记录每套 pass/fail 数与 join suite wall_time 到 `/tmp/oq2-base-*.log`(基线锚点)。

- [ ] **Step 3: 跑改动后的四套回归**

重启 server(Step 1),然后:
```bash
for s in join cte aggregate filter; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite $s --mode verify -j 1 \
    2>&1 | tee /tmp/oq2-after-$s.log | tail -3
done
```
Expected: 相对 Step 2 基线,cte/aggregate/filter **无新增 fail**;join suite **无新增 fail**,wall_time 较基线下降(roadmap 目标 -10%~-20%)。逐一 diff `/tmp/oq2-base-$s.log` 与 `/tmp/oq2-after-$s.log`。任何新增 fail 必须用 systematic-debugging 定位后再继续。

- [ ] **Step 4: optimizer suite 全量**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify -j 1 2>&1 | tail -5
```
Expected: 全绿(含新增 4 个 case)。

- [ ] **Step 5: FE plan diff(三条标杆)**

按 `~/.claude/skills/starrocks-fe-on-novarocks` skill 起 StarRocks FE(9030)。对 `join_one_key` q22、`join_linear_chained` q31、一条简单 `INNER ... count(*)`,分别在 NovaRocks(`$NOVA_ENV_MYSQL_PORT`)与 FE(9030)跑 `EXPLAIN`(及 `EXPLAIN COSTS`),对比 `IS NOT NULL` 落点:
- Inner:两侧 key scan 都应带 `IS NOT NULL`;
- LeftSemi:仅 build 侧;
- 与 StarRocks 在「`IS NOT NULL` 出现的侧」上一致。
把 plan diff quote 进 PR 描述(roadmap PR checklist #3)。

> 提醒:NovaRocks 端口用 `$NOVA_ENV_MYSQL_PORT`,绝不写死 9030;9030 是对比用的 StarRocks FE。

- [ ] **Step 6: 更新 roadmap 进度**

在 `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md` 的「Optimizer Plan Quality Roadmap → 进度」section,把 OQ-2 标记为完成并记录 join suite wall_time(基线 → 改动后)。同时(可选)在仓库内记一笔到对应任务文档。

- [ ] **Step 7: 收尾**

```bash
kill "$SRV_PID" 2>/dev/null
git add -A && git status   # 确认只剩 roadmap/笔记类改动
git commit -m "docs(roadmap): mark OQ-2 done; record join suite wall_time delta

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## 验证命令汇总

- 单元测试:`cargo test -p novarocks join_equi_keys derive_join_not_null 2>&1 | tail -25`
- registry 测试:`cargo test -p novarocks registry_contains_expected_rules query_pipeline_contains_migrated_query_rules 2>&1 | tail -25`
- fmt/clippy/build:`cargo fmt && cargo clippy -p novarocks 2>&1 | tail -15 && cargo build 2>&1 | tail -5`
- golden:`... --suite optimizer --mode verify --only derive_join_not_null_inner,derive_join_not_null_leftsemi,derive_join_not_null_anti,derive_join_not_null_disabled`
- 回归:`... --suite join|cte|aggregate|filter --mode verify -j 1`
