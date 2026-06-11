# OQ-9 Residual Predicate Placement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build generalized residual predicate factoring and placement so complex TPC-DS predicates, especially `q85`, are placed and derived early enough to run successfully in dev profile.

**Architecture:** Keep OQ-9 in the existing logical rewrite pipeline. Refactor join predicate pushdown into focused predicate group, classification, derivation, and move-around modules under `predicate_pushdown`, then run pushdown before join reorder, after join reorder, after move-around, and before aggregate pushdown.

**Tech Stack:** Rust, NovaRocks logical optimizer, Arrow `DataType`, `sql-tests` optimizer and TPC-DS suites, standalone-server dev profile.

---

## File Structure

- Create `src/sql/optimizer/rewrite/rules/predicate_pushdown/predicate_group.rs`
  - Owns `PredicateGroup`, canonical predicate keys, top-level AND splitting, OR branch helpers, deterministic-expression detection, expression combination, and deduplication.
- Create `src/sql/optimizer/rewrite/rules/predicate_pushdown/classifier.rs`
  - Classifies predicate groups against join child output `ColumnId` sets with conservative name-based fallback.
- Create `src/sql/optimizer/rewrite/rules/predicate_pushdown/deriver.rs`
  - Derives equality, range, IN-list, OR side-filter, and range-envelope predicates for inner/cross joins.
- Create `src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs`
  - Rebuilds join trees from classified groups and derived groups. This becomes the shared implementation used by `Filter(Join)` and `Join(condition)`.
- Create `src/sql/optimizer/rewrite/rules/predicate_pushdown/move_around.rs`
  - Adds a standalone rewrite rule `JoinPredicateMoveAround` that derives opposite-side predicates from join conditions plus child predicate domains.
- Modify `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs`
  - Shrink to thin `PushDownPredicateJoin` rule wrapper.
- Modify `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`
  - Export new modules and include `JoinPredicateMoveAround` in a dedicated rule vector.
- Modify `src/sql/optimizer/rewrite/registry.rs`
  - Insert `PredicateMoveAround` and `PredicatePushdownAfterMoveAround` stages. Update registry tests.
- Add SQL golden cases under `sql-tests/optimizer/sql/` and `sql-tests/optimizer/result/`.
  - `residual_or_side_filter_inner.sql`
  - `residual_range_envelope_inner.sql`
  - `residual_outer_join_guard.sql`
  - `residual_move_around_disabled.sql`

## Task 1: Predicate Group Model

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/predicate_pushdown/predicate_group.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Add failing unit tests for grouping, keys, OR branches, and non-determinism**

Add this test module to the new file first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    #[test]
    fn top_level_and_is_split_but_or_stays_atomic() {
        let expr = bool_expr(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            BinOp::And,
            bool_expr(
                bool_expr(col("b", 2), BinOp::Eq, int_lit(2)),
                BinOp::Or,
                bool_expr(col("b", 2), BinOp::Eq, int_lit(3)),
            ),
        );

        let groups = PredicateGroup::from_predicate(expr, PredicateOrigin::Filter);

        assert_eq!(groups.len(), 2);
        assert!(groups[0].referenced_ids.contains(&ColumnId::new_for_test(1)));
        assert!(groups[1].referenced_ids.contains(&ColumnId::new_for_test(2)));
        assert!(matches!(groups[1].expr.kind, ExprKind::BinaryOp { op: BinOp::Or, .. }));
    }

    #[test]
    fn dedupe_keeps_first_group_for_same_canonical_key() {
        let first = PredicateGroup::new(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            PredicateOrigin::Filter,
            PredicateDerivedKind::None,
        );
        let second = PredicateGroup::new(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            PredicateOrigin::Derived,
            PredicateDerivedKind::Equivalence,
        );

        let deduped = dedupe_groups(vec![first.clone(), second]);

        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].origin, first.origin);
    }

    #[test]
    fn split_or_refs_flattens_nested_or() {
        let expr = bool_expr(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            BinOp::Or,
            bool_expr(
                bool_expr(col("a", 1), BinOp::Eq, int_lit(2)),
                BinOp::Or,
                bool_expr(col("a", 1), BinOp::Eq, int_lit(3)),
            ),
        );

        assert_eq!(split_or_refs(&expr).len(), 3);
    }

    #[test]
    fn non_deterministic_function_is_detected() {
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".to_string(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        };

        assert!(contains_non_deterministic_function(&expr));
    }
}
```

- [ ] **Step 2: Run tests and verify the new module is missing**

Run:

```bash
cargo test --lib predicate_group -- --nocapture
```

Expected: FAIL with an unresolved module or missing symbol error before implementation is added.

- [ ] **Step 3: Implement the predicate group module**

Add this implementation above the test module:

```rust
use std::collections::{BTreeSet, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PredicateKey(String);

impl PredicateKey {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateOrigin {
    Filter,
    JoinCondition,
    Derived,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateDerivedKind {
    None,
    Equivalence,
    Range,
    RangeEnvelope,
    OrSideFilter,
    NotNull,
}

#[derive(Clone, Debug)]
pub(crate) struct PredicateGroup {
    pub(crate) expr: TypedExpr,
    pub(crate) referenced_ids: BTreeSet<ColumnId>,
    pub(crate) key: PredicateKey,
    pub(crate) origin: PredicateOrigin,
    pub(crate) derived: PredicateDerivedKind,
    pub(crate) deterministic: bool,
}

impl PredicateGroup {
    pub(crate) fn new(
        expr: TypedExpr,
        origin: PredicateOrigin,
        derived: PredicateDerivedKind,
    ) -> Self {
        let referenced_ids = collect_column_id_refs(&expr).into_iter().collect();
        let key = predicate_key(&expr);
        let deterministic = !contains_non_deterministic_function(&expr);
        Self {
            expr,
            referenced_ids,
            key,
            origin,
            derived,
            deterministic,
        }
    }

    pub(crate) fn from_predicate(expr: TypedExpr, origin: PredicateOrigin) -> Vec<Self> {
        split_and(expr)
            .into_iter()
            .map(|expr| Self::new(expr, origin, PredicateDerivedKind::None))
            .collect()
    }
}

pub(crate) fn predicate_key(expr: &TypedExpr) -> PredicateKey {
    PredicateKey(format!("{:?}", expr.kind))
}

pub(crate) fn dedupe_groups(groups: Vec<PredicateGroup>) -> Vec<PredicateGroup> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for group in groups {
        if seen.insert(group.key.clone()) {
            out.push(group);
        }
    }
    out
}

pub(crate) fn exprs_from_groups(groups: Vec<PredicateGroup>) -> Vec<TypedExpr> {
    groups.into_iter().map(|group| group.expr).collect()
}

pub(crate) fn combine_or(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: DataType::Boolean,
            nullable: left.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(result),
            },
        };
    }
    result
}

pub(crate) fn split_or_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut out = split_or_refs(left);
            out.extend(split_or_refs(right));
            out
        }
        ExprKind::Nested(inner) => split_or_refs(inner),
        _ => vec![expr],
    }
}

pub(crate) fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut out = split_and_refs(left);
            out.extend(split_and_refs(right));
            out
        }
        ExprKind::Nested(inner) => split_and_refs(inner),
        _ => vec![expr],
    }
}

pub(crate) fn contains_non_deterministic_function(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            matches!(
                lower.as_str(),
                "rand" | "random" | "uuid" | "now" | "current_timestamp" | "current_date" | "current_time"
            ) || args.iter().any(contains_non_deterministic_function)
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter().any(contains_non_deterministic_function)
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(&item.expr))
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_non_deterministic_function)
                || partition_by.iter().any(contains_non_deterministic_function)
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(&item.expr))
        }
        ExprKind::BinaryOp { left, right, .. } => {
            contains_non_deterministic_function(left) || contains_non_deterministic_function(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => contains_non_deterministic_function(expr),
        ExprKind::InList { expr, list, .. } => {
            contains_non_deterministic_function(expr)
                || list.iter().any(contains_non_deterministic_function)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            contains_non_deterministic_function(expr)
                || contains_non_deterministic_function(low)
                || contains_non_deterministic_function(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_non_deterministic_function(expr)
                || contains_non_deterministic_function(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_non_deterministic_function(expr))
                || when_then.iter().any(|(when, then)| {
                    contains_non_deterministic_function(when)
                        || contains_non_deterministic_function(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_non_deterministic_function(expr))
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            contains_non_deterministic_function(body)
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}
```

Modify `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`:

```rust
pub(crate) mod predicate_group;
```

- [ ] **Step 4: Run predicate group tests**

Run:

```bash
cargo test --lib predicate_group -- --nocapture
```

Expected: PASS for the four new tests.

- [ ] **Step 5: Commit Task 1**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/predicate_group.rs
git commit -m "feat: add predicate group model"
```

## Task 2: Predicate Classifier

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/predicate_pushdown/classifier.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Add failing classifier tests**

Add these tests to `classifier.rs` with the implementation imports absent at first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(name.chars().next().unwrap().to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
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

    fn group(expr: TypedExpr) -> PredicateGroup {
        PredicateGroup::new(expr, PredicateOrigin::Filter, PredicateDerivedKind::None)
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId::new_for_test).collect()
    }

    #[test]
    fn inner_join_classifies_left_right_and_join_groups_by_column_id() {
        let placement = classify_predicate_groups(
            JoinKind::Inner,
            &ids(&[1]),
            &ids(&[2]),
            vec![
                group(eq(col("a", 1), int_lit(10))),
                group(eq(col("b", 2), int_lit(20))),
                group(eq(col("a", 1), col("b", 2))),
            ],
        );

        assert_eq!(placement.left_pushdown.len(), 1);
        assert_eq!(placement.right_pushdown.len(), 1);
        assert_eq!(placement.join_residual.len(), 1);
        assert!(placement.remain_above_join.is_empty());
    }

    #[test]
    fn left_outer_keeps_right_filter_above_join() {
        let placement = classify_predicate_groups(
            JoinKind::LeftOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(eq(col("b", 2), int_lit(20)))],
        );

        assert!(placement.right_pushdown.is_empty());
        assert_eq!(placement.remain_above_join.len(), 1);
    }

    #[test]
    fn full_outer_keeps_single_side_filters_above_join() {
        let placement = classify_predicate_groups(
            JoinKind::FullOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(eq(col("a", 1), int_lit(10))), group(eq(col("b", 2), int_lit(20)))],
        );

        assert_eq!(placement.remain_above_join.len(), 2);
    }
}
```

- [ ] **Step 2: Run classifier tests and verify missing implementation**

Run:

```bash
cargo test --lib classifier -- --nocapture
```

Expected: FAIL with unresolved `classify_predicate_groups` or module export.

- [ ] **Step 3: Implement classifier**

Add this implementation above the tests:

```rust
use std::collections::HashSet;

use crate::sql::analysis::JoinKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::PredicateGroup;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClassifiedPredicates {
    pub(crate) left_pushdown: Vec<PredicateGroup>,
    pub(crate) right_pushdown: Vec<PredicateGroup>,
    pub(crate) join_residual: Vec<PredicateGroup>,
    pub(crate) remain_above_join: Vec<PredicateGroup>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SideTarget {
    Left,
    Right,
    Both,
    Neither,
    Outside,
}

pub(crate) fn classify_predicate_groups(
    join_type: JoinKind,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    groups: Vec<PredicateGroup>,
) -> ClassifiedPredicates {
    let mut out = ClassifiedPredicates::default();
    for group in groups {
        match classify_group(&group, left_ids, right_ids) {
            SideTarget::Left if may_push_left(join_type) => out.left_pushdown.push(group),
            SideTarget::Left => out.remain_above_join.push(group),
            SideTarget::Right if may_push_right(join_type) => out.right_pushdown.push(group),
            SideTarget::Right => out.remain_above_join.push(group),
            SideTarget::Both if may_place_cross_side_in_join(join_type) => {
                out.join_residual.push(group)
            }
            SideTarget::Both => out.remain_above_join.push(group),
            SideTarget::Neither if may_push_left(join_type) => out.left_pushdown.push(group),
            SideTarget::Neither => out.remain_above_join.push(group),
            SideTarget::Outside => out.remain_above_join.push(group),
        }
    }
    out
}

fn classify_group(
    group: &PredicateGroup,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> SideTarget {
    if group.referenced_ids.is_empty() {
        return SideTarget::Neither;
    }
    let mut left = false;
    let mut right = false;
    for id in &group.referenced_ids {
        match (left_ids.contains(id), right_ids.contains(id)) {
            (true, false) => left = true,
            (false, true) => right = true,
            (true, true) => return SideTarget::Outside,
            (false, false) => return SideTarget::Outside,
        }
    }
    match (left, right) {
        (true, false) => SideTarget::Left,
        (false, true) => SideTarget::Right,
        (true, true) => SideTarget::Both,
        (false, false) => SideTarget::Neither,
    }
}

fn may_push_left(join_type: JoinKind) -> bool {
    matches!(
        join_type,
        JoinKind::Inner
            | JoinKind::Cross
            | JoinKind::LeftOuter
            | JoinKind::LeftSemi
            | JoinKind::LeftAnti
    )
}

fn may_push_right(join_type: JoinKind) -> bool {
    matches!(
        join_type,
        JoinKind::Inner
            | JoinKind::Cross
            | JoinKind::RightOuter
            | JoinKind::RightSemi
            | JoinKind::RightAnti
    )
}

fn may_place_cross_side_in_join(join_type: JoinKind) -> bool {
    matches!(join_type, JoinKind::Inner | JoinKind::Cross)
}
```

Modify `predicate_pushdown/mod.rs`:

```rust
pub(crate) mod classifier;
```

- [ ] **Step 4: Run classifier tests**

Run:

```bash
cargo test --lib classifier -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/classifier.rs
git commit -m "feat: classify join predicate groups"
```

## Task 3: Shared Join Predicate Pushdown Core

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Add failing tests for `Filter(Join)` parity and join-condition pushdown**

Create tests in `join_pushdown.rs` that mirror existing `push_to_join.rs` helpers but use `ColumnId`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn scan(alias: &str, cols: &[(&str, u32)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: alias.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: true,
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
                .map(|(name, id)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
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

    fn join(join_type: JoinKind, condition: Option<TypedExpr>) -> JoinNode {
        JoinNode {
            left: Box::new(scan("l", &[("a", 1), ("v", 3)])),
            right: Box::new(scan("r", &[("b", 2), ("w", 4)])),
            join_type,
            condition,
            required_output_columns: None,
        }
    }

    #[test]
    fn filter_join_pushes_left_right_and_keeps_cross_side_residual() {
        let predicate = crate::sql::optimizer::rewrite::rules::utils::combine_and(vec![
            eq(col("l", "a", 1), int_lit(10)),
            eq(col("r", "b", 2), int_lit(20)),
            eq(col("l", "v", 3), col("r", "w", 4)),
        ]);

        let (plan, changed) = push_filter_predicates_through_join(predicate, join(JoinKind::Inner, None));

        assert!(changed);
        let LogicalPlan::Join(join) = plan else {
            panic!("expected bare Join");
        };
        assert!(matches!(*join.left, LogicalPlan::Filter(_)));
        assert!(matches!(*join.right, LogicalPlan::Filter(_)));
        assert!(join.condition.is_some());
    }

    #[test]
    fn join_condition_pushes_single_side_terms_below_inner_join() {
        let condition = crate::sql::optimizer::rewrite::rules::utils::combine_and(vec![
            eq(col("l", "a", 1), int_lit(10)),
            eq(col("l", "v", 3), col("r", "w", 4)),
        ]);

        let plan = push_join_condition_predicates(join(JoinKind::Inner, Some(condition)))
            .expect("join condition should be rewritten");

        let LogicalPlan::Join(join) = plan else {
            panic!("expected Join");
        };
        assert!(matches!(*join.left, LogicalPlan::Filter(_)));
        let condition = join.condition.expect("join condition");
        let rendered = format!("{:?}", condition.kind);
        assert!(rendered.contains("\"v\""));
        assert!(rendered.contains("\"w\""));
        assert!(!rendered.contains("Int(10)"));
    }

    #[test]
    fn cross_join_with_residual_condition_upgrades_to_inner() {
        let condition = eq(col("l", "v", 3), col("r", "w", 4));

        let plan = push_join_condition_predicates(join(JoinKind::Cross, Some(condition)))
            .expect("cross join should be upgraded");

        let LogicalPlan::Join(join) = plan else {
            panic!("expected Join");
        };
        assert_eq!(join.join_type, JoinKind::Inner);
    }
}
```

- [ ] **Step 2: Run tests and verify unresolved symbols**

Run:

```bash
cargo test --lib join_pushdown -- --nocapture
```

Expected: FAIL with unresolved `push_filter_predicates_through_join` or `push_join_condition_predicates`.

- [ ] **Step 3: Implement shared join pushdown functions**

Add these public entrypoints and rebuild helpers to `join_pushdown.rs`:

```rust
use crate::sql::analysis::{JoinKind, TypedExpr};
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::classifier::{
    classify_predicate_groups, ClassifiedPredicates,
};
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    dedupe_groups, exprs_from_groups, PredicateGroup, PredicateOrigin,
};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_output_ids, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::planner::plan::*;

pub(crate) fn push_filter_predicates_through_join(
    predicate: TypedExpr,
    join: JoinNode,
) -> (LogicalPlan, bool) {
    let groups = PredicateGroup::from_predicate(predicate, PredicateOrigin::Filter);
    rebuild_join_with_groups(join, groups, true)
}

pub(crate) fn push_join_condition_predicates(join: JoinNode) -> Option<LogicalPlan> {
    let condition = join.condition.clone()?;
    let groups = PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition);
    let (plan, changed) = rebuild_join_with_groups(
        JoinNode {
            condition: None,
            ..join
        },
        groups,
        false,
    );
    changed.then_some(plan)
}

fn rebuild_join_with_groups(
    join: JoinNode,
    groups: Vec<PredicateGroup>,
    keep_remaining_above: bool,
) -> (LogicalPlan, bool) {
    let left_ids = collect_output_ids(&join.left);
    let right_ids = collect_output_ids(&join.right);
    let groups = dedupe_groups(groups);
    let ClassifiedPredicates {
        left_pushdown,
        right_pushdown,
        join_residual,
        remain_above_join,
    } = classify_predicate_groups(join.join_type, &left_ids, &right_ids, groups);

    let changed = !left_pushdown.is_empty()
        || !right_pushdown.is_empty()
        || !join_residual.is_empty()
        || (!remain_above_join.is_empty() && !keep_remaining_above);

    let JoinNode {
        left,
        right,
        join_type,
        condition,
        required_output_columns,
    } = join;

    let new_left = wrap_child_filter(*left, left_pushdown);
    let new_right = wrap_child_filter(*right, right_pushdown);
    let new_condition = merge_conditions(condition, exprs_from_groups(join_residual));
    let new_join_type = if join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join_type
    };

    let new_join = LogicalPlan::Join(JoinNode {
        left: Box::new(new_left),
        right: Box::new(new_right),
        join_type: new_join_type,
        condition: new_condition,
        required_output_columns,
    });

    let plan = if keep_remaining_above {
        wrap_remaining_filter(new_join, exprs_from_groups(remain_above_join))
    } else if remain_above_join.is_empty() {
        new_join
    } else {
        let condition_plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(new_join),
            predicate: combine_and(exprs_from_groups(remain_above_join)),
            required_output_columns: None,
        });
        condition_plan
    };

    (plan, changed)
}

fn wrap_child_filter(child: LogicalPlan, groups: Vec<PredicateGroup>) -> LogicalPlan {
    if groups.is_empty() {
        child
    } else {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(child),
            predicate: combine_and(exprs_from_groups(groups)),
            required_output_columns: None,
        })
    }
}

fn merge_conditions(existing: Option<TypedExpr>, new_preds: Vec<TypedExpr>) -> Option<TypedExpr> {
    let mut all = Vec::new();
    if let Some(condition) = existing {
        all.extend(split_and(condition));
    }
    all.extend(new_preds);
    let groups = dedupe_groups(
        all.into_iter()
            .map(|expr| PredicateGroup::new(
                expr,
                PredicateOrigin::JoinCondition,
                crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::PredicateDerivedKind::None,
            ))
            .collect(),
    );
    let exprs = exprs_from_groups(groups);
    if exprs.is_empty() {
        None
    } else {
        Some(combine_and(exprs))
    }
}
```

Modify `push_to_join.rs` so it only wraps these functions:

```rust
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::join_pushdown::{
    push_filter_predicates_through_join, push_join_condition_predicates,
};
use crate::sql::planner::plan::*;

pub(crate) struct PushDownPredicateJoin;

impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Join(_))
        ) || matches!(plan, LogicalPlan::Join(join) if join.condition.is_some())
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let LogicalPlan::Join(join) = *filter.input else {
                    return None;
                };
                let (rewritten, changed) =
                    push_filter_predicates_through_join(filter.predicate, join);
                changed.then_some(rewritten)
            }
            LogicalPlan::Join(join) => push_join_condition_predicates(join),
            _ => None,
        }
    }
}
```

Modify `predicate_pushdown/mod.rs`:

```rust
pub(crate) mod join_pushdown;
```

- [ ] **Step 4: Run focused and existing pushdown tests**

Run:

```bash
cargo test --lib join_pushdown -- --nocapture
cargo test --lib push_to_join -- --nocapture
```

Expected: PASS. If existing `push_to_join` tests assert now-removed private helpers, move those test cases into `join_pushdown.rs` and keep their assertions unchanged.

- [ ] **Step 5: Commit Task 3**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs
git commit -m "feat: share join predicate pushdown core"
```

## Task 4: Equality, Range, and OR Side-Filter Deriver

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/predicate_pushdown/deriver.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Add failing deriver tests**

Add these tests to `deriver.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn group(expr: TypedExpr) -> PredicateGroup {
        PredicateGroup::new(expr, PredicateOrigin::Filter, PredicateDerivedKind::None)
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId::new_for_test).collect()
    }

    #[test]
    fn derives_equality_across_join_key() {
        let join_eq = group(bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)));
        let left_filter = group(bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)));

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[join_eq], &[left_filter]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn derives_or_side_filter_from_branch_equalities() {
        let or_pred = bool_expr(
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(1)),
            ),
            BinOp::Or,
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(2)),
            ),
        );

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("InList") || rendered.contains("Or"));
        assert!(rendered.contains("Int(1)"));
        assert!(rendered.contains("Int(2)"));
    }

    #[test]
    fn derives_range_envelope_from_or_branches() {
        let or_pred = bool_expr(
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(int_lit(100)),
                    high: Box::new(int_lit(150)),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(int_lit(50)),
                    high: Box::new(int_lit(200)),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        );

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[3]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"price\""));
        assert!(rendered.contains("Int(50)"));
        assert!(rendered.contains("Int(200)"));
    }
}
```

- [ ] **Step 2: Run deriver tests and verify missing implementation**

Run:

```bash
cargo test --lib deriver -- --nocapture
```

Expected: FAIL with unresolved `derive_inner_join_predicates`.

- [ ] **Step 3: Implement deriver API**

Implement this public API:

```rust
pub(crate) fn derive_inner_join_predicates(
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    join_groups: &[PredicateGroup],
    filter_groups: &[PredicateGroup],
) -> Vec<PredicateGroup>
```

Implementation requirements:

- Return an empty vector if any source group has `deterministic == false`.
- Extract column-column equality pairs from both `join_groups` and OR branches in `filter_groups`.
- Extract simple column-literal constraints:
  - `column = literal`
  - `column IN (literal, ...)`
  - `column >= literal`, `column > literal`, `column <= literal`, `column < literal`
  - `column BETWEEN literal AND literal` with `negated == false`
- For equality derivation, clone the literal constraint and replace the source column with the equal target column.
- For OR side-filter derivation, every OR branch must yield at least one constraint for the same target side. If all branch constraints are equalities on the same target column, emit `ExprKind::InList`. If every branch has a numeric range on the same target column, emit a `Between` envelope with the min low and max high literal.
- Mark derived groups with `PredicateOrigin::Derived` and one of:
  - `PredicateDerivedKind::Equivalence`
  - `PredicateDerivedKind::Range`
  - `PredicateDerivedKind::RangeEnvelope`
  - `PredicateDerivedKind::OrSideFilter`
- Use `predicate_group::dedupe_groups` before returning.

Use helper signatures with these exact names:

```rust
fn extract_column_pair_equality(expr: &TypedExpr) -> Option<(TypedExpr, TypedExpr)>;
fn extract_column_literal_constraint(expr: &TypedExpr) -> Option<ColumnConstraint>;
fn substitute_constraint_column(
    constraint: &ColumnConstraint,
    target_column: &TypedExpr,
    derived: PredicateDerivedKind,
) -> Option<PredicateGroup>;
fn derive_or_branch_side_filters(
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    group: &PredicateGroup,
) -> Vec<PredicateGroup>;
```

Represent branch constraints with:

```rust
#[derive(Clone, Debug)]
struct ColumnConstraint {
    column: TypedExpr,
    kind: ConstraintKind,
}

#[derive(Clone, Debug)]
enum ConstraintKind {
    Eq(TypedExpr),
    InList(Vec<TypedExpr>),
    Lower { op: BinOp, value: TypedExpr },
    Upper { op: BinOp, value: TypedExpr },
    Between { low: TypedExpr, high: TypedExpr },
}
```

Numeric envelope only needs to support `LiteralValue::Int`, `LiteralValue::LargeInt`, `LiteralValue::Float`, and `LiteralValue::Decimal`. If a branch uses unsupported literal kinds, skip envelope derivation for that OR group and keep the original OR residual.

Modify `predicate_pushdown/mod.rs`:

```rust
pub(crate) mod deriver;
```

- [ ] **Step 4: Wire deriver into `join_pushdown.rs` for inner/cross joins**

In `rebuild_join_with_groups`, before classification:

```rust
let mut all_groups = groups;
if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
    let join_groups: Vec<_> = join
        .condition
        .clone()
        .map(|condition| PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition))
        .unwrap_or_default();
    let derived = crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates(
        &left_ids,
        &right_ids,
        &join_groups,
        &all_groups,
    );
    all_groups.extend(join_groups);
    all_groups.extend(derived);
}
let groups = dedupe_groups(all_groups);
```

Keep the old `join.condition` merge path for non-inner joins and ensure existing join condition conjuncts are not dropped.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test --lib deriver -- --nocapture
cargo test --lib join_pushdown -- --nocapture
cargo test --lib push_to_join -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/deriver.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/join_pushdown.rs
git commit -m "feat: derive safe join side predicates"
```

## Task 5: Join Predicate Move-Around Rule

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/predicate_pushdown/move_around.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Add failing move-around tests**

Add these tests to `move_around.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
    use arrow::datatypes::DataType;

    fn scan(alias: &str, cols: &[(&str, u32)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: alias.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: true,
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
                .map(|(name, id)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
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

    #[test]
    fn derives_opposite_side_filter_from_child_filter_and_join_equality() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(scan("r", &[("b", 2)])),
            join_type: JoinKind::Inner,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        let out = JoinPredicateMoveAround.apply(plan).expect("move-around should derive right filter");
        let LogicalPlan::Join(join) = out else {
            panic!("expected Join");
        };
        assert!(matches!(*join.right, LogicalPlan::Filter(_)));
    }

    #[test]
    fn skips_left_outer_nullable_side_derivation() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(scan("r", &[("b", 2)])),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }
}
```

- [ ] **Step 2: Run move-around tests and verify missing rule**

Run:

```bash
cargo test --lib move_around -- --nocapture
```

Expected: FAIL with unresolved `JoinPredicateMoveAround`.

- [ ] **Step 3: Implement `JoinPredicateMoveAround`**

Implement this rule:

```rust
use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    exprs_from_groups, PredicateGroup, PredicateOrigin,
};
use crate::sql::optimizer::rewrite::rules::utils::{collect_output_ids, combine_and, split_and};
use crate::sql::planner::plan::*;

pub(crate) struct JoinPredicateMoveAround;

impl PlanRewriteRule for JoinPredicateMoveAround {
    fn name(&self) -> &'static str {
        "JoinPredicateMoveAround"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Join(join) if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) && join.condition.is_some())
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        let left_ids = collect_output_ids(&join.left);
        let right_ids = collect_output_ids(&join.right);
        let join_groups = join
            .condition
            .clone()
            .map(|condition| PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition))
            .unwrap_or_default();
        let child_groups = child_predicate_groups(&join.left)
            .into_iter()
            .chain(child_predicate_groups(&join.right))
            .collect::<Vec<_>>();
        let derived = derive_inner_join_predicates(&left_ids, &right_ids, &join_groups, &child_groups);
        if derived.is_empty() {
            return None;
        }
        let (left_derived, right_derived): (Vec<_>, Vec<_>) = derived.into_iter().partition(|group| {
            !group.referenced_ids.is_empty()
                && group.referenced_ids.iter().all(|id| left_ids.contains(id))
        });
        if left_derived.is_empty() && right_derived.is_empty() {
            return None;
        }
        Some(LogicalPlan::Join(JoinNode {
            left: Box::new(wrap_filter(*join.left, left_derived)),
            right: Box::new(wrap_filter(*join.right, right_derived)),
            join_type: join.join_type,
            condition: join.condition,
            required_output_columns: join.required_output_columns,
        }))
    }
}

fn child_predicate_groups(plan: &LogicalPlan) -> Vec<PredicateGroup> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut groups = PredicateGroup::from_predicate(filter.predicate.clone(), PredicateOrigin::Filter);
            groups.extend(child_predicate_groups(&filter.input));
            groups
        }
        LogicalPlan::Scan(scan) => scan
            .predicates
            .iter()
            .cloned()
            .flat_map(|predicate| PredicateGroup::from_predicate(predicate, PredicateOrigin::Filter))
            .collect(),
        LogicalPlan::Project(project) => child_predicate_groups(&project.input),
        LogicalPlan::Sort(sort) => child_predicate_groups(&sort.input),
        LogicalPlan::Limit(limit) => child_predicate_groups(&limit.input),
        _ => Vec::new(),
    }
}

fn wrap_filter(child: LogicalPlan, groups: Vec<PredicateGroup>) -> LogicalPlan {
    if groups.is_empty() {
        child
    } else {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(child),
            predicate: combine_and(exprs_from_groups(groups)),
            required_output_columns: None,
        })
    }
}
```

Guard requirements:

- The rule applies only to `JoinKind::Inner | JoinKind::Cross` in OQ-9.
- The rule must be idempotent by relying on `derive_inner_join_predicates` dedupe and by checking existing child predicate spines before wrapping new filters. Use `predicate_group::predicate_key` to compare existing and derived predicates.

Modify `predicate_pushdown/mod.rs`:

```rust
pub(crate) mod move_around;

pub(crate) fn predicate_move_around_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(move_around::JoinPredicateMoveAround)]
}
```

- [ ] **Step 4: Run move-around tests**

Run:

```bash
cargo test --lib move_around -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 5**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs \
  src/sql/optimizer/rewrite/rules/predicate_pushdown/move_around.rs
git commit -m "feat: add join predicate move-around"
```

## Task 6: Rewrite Pipeline Integration

**Files:**
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`

- [ ] **Step 1: Add failing registry expectations**

Update `query_pipeline_contains_migrated_query_rules` in `registry.rs` so expected rule names include:

```rust
"JoinPredicateMoveAround",
"PushDownPredicateAggregate",
"PushDownPredicateJoin",
"PushDownPredicateProject",
"PushDownPredicateScan",
"PushSemiAntiRightOnlyCondition",
```

The predicate pushdown rule names should now appear three times because there are pre-join, post-join, and after-move-around stages.

Update `rewrite_registry_recognizes_migrated_query_rules`:

```rust
assert!(is_known_rewrite_rule_name("JoinPredicateMoveAround"));
```

Update `rules/mod.rs` `registry_contains_expected_rules` count and expected names to include `JoinPredicateMoveAround`.

- [ ] **Step 2: Run registry tests and verify mismatch**

Run:

```bash
cargo test --lib rewrite::registry -- --nocapture
cargo test --lib rewrite::rules -- --nocapture
```

Expected: FAIL because the new stage is not registered yet.

- [ ] **Step 3: Add move-around and after-move-around stages**

Change `query_rewrite_pipeline` to this stage order:

```rust
RewriteStage::new(
    "PredicatePushdownPreJoin",
    RewritePhase::StructuralRewrite,
    rules::predicate_pushdown_rules(),
),
RewriteStage::new(
    "JoinReorder",
    RewritePhase::StructuralRewrite,
    rules::join_reorder_rules(table_stats),
),
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
RewriteStage::new(
    "PredicateMoveAround",
    RewritePhase::StructuralRewrite,
    rules::predicate_move_around_rules(),
),
RewriteStage::new(
    "PredicatePushdownAfterMoveAround",
    RewritePhase::StructuralRewrite,
    rules::predicate_pushdown_rules(),
),
RewriteStage::new(
    "AggregatePushdown",
    RewritePhase::StructuralRewrite,
    rules::aggregate_pushdown::aggregate_pushdown_rules(table_stats),
),
```

Add this function to `rules/mod.rs`:

```rust
pub(crate) fn predicate_move_around_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    predicate_pushdown::predicate_move_around_rules()
}
```

Update `all_query_rewrite_rules` to include `predicate_move_around_rules()` and adjust its count from 28 to 29.

- [ ] **Step 4: Run registry tests**

Run:

```bash
cargo test --lib rewrite::registry -- --nocapture
cargo test --lib rewrite::rules -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 6**

```bash
git add src/sql/optimizer/rewrite/registry.rs src/sql/optimizer/rewrite/rules/mod.rs
git commit -m "feat: run predicate move-around in optimizer pipeline"
```

## Task 7: Optimizer SQL Goldens

**Files:**
- Create: `sql-tests/optimizer/sql/residual_or_side_filter_inner.sql`
- Create: `sql-tests/optimizer/sql/residual_range_envelope_inner.sql`
- Create: `sql-tests/optimizer/sql/residual_outer_join_guard.sql`
- Create: `sql-tests/optimizer/sql/residual_move_around_disabled.sql`
- Create or record matching files under `sql-tests/optimizer/result/`

- [ ] **Step 1: Add SQL cases**

Create `residual_or_side_filter_inner.sql`:

```sql
-- @tags=optimizer,oq9,residual_predicate
DROP TABLE IF EXISTS ${case_db}.oq9_cd1;
DROP TABLE IF EXISTS ${case_db}.oq9_cd2;
CREATE TABLE ${case_db}.oq9_cd1 (id INT, ms VARCHAR, edu VARCHAR);
CREATE TABLE ${case_db}.oq9_cd2 (id INT, ms VARCHAR, edu VARCHAR);
INSERT INTO ${case_db}.oq9_cd1 VALUES (1, 'M', 'Primary'), (2, 'S', 'College'), (3, 'D', 'Other');
INSERT INTO ${case_db}.oq9_cd2 VALUES (10, 'M', 'Primary'), (20, 'S', 'College'), (30, 'W', 'Other');
-- @explain_contains=oq9_cd2
-- @explain_contains=IN
-- @explain_contains=M
-- @explain_contains=S
EXPLAIN VERBOSE
SELECT *
FROM ${case_db}.oq9_cd1 cd1
JOIN ${case_db}.oq9_cd2 cd2
  ON cd1.ms = cd2.ms AND cd1.edu = cd2.edu
WHERE (cd1.ms = 'M' AND cd1.edu = 'Primary')
   OR (cd1.ms = 'S' AND cd1.edu = 'College');
```

Create `residual_range_envelope_inner.sql`:

```sql
-- @tags=optimizer,oq9,residual_predicate
DROP TABLE IF EXISTS ${case_db}.oq9_sales;
CREATE TABLE ${case_db}.oq9_sales (id INT, price INT, profit INT);
INSERT INTO ${case_db}.oq9_sales
    SELECT generate_series, generate_series % 300, generate_series % 100
    FROM TABLE(generate_series(1, 1000));
-- @explain_contains=price BETWEEN 50 AND 200
-- @explain_contains=profit BETWEEN 5 AND 80
EXPLAIN VERBOSE
SELECT *
FROM ${case_db}.oq9_sales s
WHERE (s.price BETWEEN 100 AND 150 AND s.profit BETWEEN 5 AND 10)
   OR (s.price BETWEEN 50 AND 200 AND s.profit BETWEEN 60 AND 80);
```

Create `residual_outer_join_guard.sql`:

```sql
-- @tags=optimizer,oq9,residual_predicate,outer
DROP TABLE IF EXISTS ${case_db}.oq9_outer_l;
DROP TABLE IF EXISTS ${case_db}.oq9_outer_r;
CREATE TABLE ${case_db}.oq9_outer_l (k INT, v INT);
CREATE TABLE ${case_db}.oq9_outer_r (k INT, v INT);
INSERT INTO ${case_db}.oq9_outer_l VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.oq9_outer_r VALUES (1, 100);
-- @explain_contains=LEFT OUTER
-- @explain_contains=oq9_outer_r
-- @explain_not_contains=oq9_outer_r[k, v] predicates:
EXPLAIN VERBOSE
SELECT *
FROM ${case_db}.oq9_outer_l l
LEFT OUTER JOIN ${case_db}.oq9_outer_r r ON l.k = r.k
WHERE r.v = 100;
```

Create `residual_move_around_disabled.sql`:

```sql
-- @tags=optimizer,oq9,residual_predicate,session_rule_disable
DROP TABLE IF EXISTS ${case_db}.oq9_ma_l;
DROP TABLE IF EXISTS ${case_db}.oq9_ma_r;
CREATE TABLE ${case_db}.oq9_ma_l (k INT, v INT);
CREATE TABLE ${case_db}.oq9_ma_r (k INT, v INT);
INSERT INTO ${case_db}.oq9_ma_l SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq9_ma_r SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));

-- @explain_contains=oq9_ma_r
-- @explain_contains=k = 10
EXPLAIN VERBOSE
SELECT *
FROM ${case_db}.oq9_ma_l l
JOIN ${case_db}.oq9_ma_r r ON l.k = r.k
WHERE l.k = 10;

SET disable_optimizer_rules = 'JoinPredicateMoveAround';

-- @explain_not_contains=oq9_ma_r[k, v] predicates: (k = 10)
EXPLAIN VERBOSE
SELECT *
FROM ${case_db}.oq9_ma_l l
JOIN ${case_db}.oq9_ma_r r ON l.k = r.k
WHERE l.k = 10;

SET disable_optimizer_rules = '';
```

- [ ] **Step 2: Run optimizer suite in record mode for new cases**

Start standalone-server in another terminal:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Wait for:

```text
NOVAROCKS_READY mysql_port=... pid=...
```

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only residual_or_side_filter_inner,residual_range_envelope_inner,residual_outer_join_guard,residual_move_around_disabled \
  --mode record -j 1
```

Expected: PASS and result files created.

- [ ] **Step 3: Verify optimizer suite for new cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only residual_or_side_filter_inner,residual_range_envelope_inner,residual_outer_join_guard,residual_move_around_disabled \
  --mode verify -j 1
```

Expected: PASS.

- [ ] **Step 4: Commit Task 7**

```bash
git add sql-tests/optimizer/sql/residual_or_side_filter_inner.sql \
  sql-tests/optimizer/sql/residual_range_envelope_inner.sql \
  sql-tests/optimizer/sql/residual_outer_join_guard.sql \
  sql-tests/optimizer/sql/residual_move_around_disabled.sql \
  sql-tests/optimizer/result/residual_or_side_filter_inner.result \
  sql-tests/optimizer/result/residual_range_envelope_inner.result \
  sql-tests/optimizer/result/residual_outer_join_guard.result \
  sql-tests/optimizer/result/residual_move_around_disabled.result
git commit -m "test: add OQ-9 residual predicate optimizer cases"
```

## Task 8: q85 Plan Inspection and Targeted Corrections

**Files:**
- Modify only files from Tasks 1-6 if q85 plan inspection shows a missing general rule.

- [ ] **Step 1: Build dev server and inspect q85 plan**

Start standalone-server:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In a second terminal, capture q85 explain:

```bash
source docker/iceberg-rest/runtime/current/env.sh
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --batch --raw --skip-column-names \
  -e "EXPLAIN VERBOSE $(cat sql-tests/tpc-ds/sql/q85.sql)" \
  > /tmp/oq9_q85_explain_after.tsv
```

Expected plan facts:

```bash
rg -n "cd2|cd_marital_status|cd_education_status|ws_sales_price|ws_net_profit|ca_gmt_offset|ca_state|BETWEEN| IN |other:" /tmp/oq9_q85_explain_after.tsv
```

Expected: output shows derived filters for `cd2`, `web_sales` range envelope, and no repeated demographics residual text.

- [ ] **Step 2: Add a focused unit test before each correction**

If q85 inspection shows a missing general rule, add a unit test to the smallest module:

- Missing column-side classification: `classifier.rs`
- Missing equality or range derivation: `deriver.rs`
- Missing join condition placement: `join_pushdown.rs`
- Missing post-reorder propagation: `move_around.rs`

Run the focused test and confirm FAIL before editing implementation:

```bash
cargo test --lib deriver -- --nocapture
```

Expected: FAIL for the newly added case only.

- [ ] **Step 3: Implement the correction in the same module**

Keep each correction constrained:

- Do not add q85 table names or q85 column names to Rust code.
- Keep original OR residual in join condition or above the lowest legal join.
- Add derived predicates only when every OR branch proves a same-side necessary condition.
- Do not widen outer/semi/anti join pushdown guards.

Run:

```bash
cargo test --lib predicate_group -- --nocapture
cargo test --lib classifier -- --nocapture
cargo test --lib deriver -- --nocapture
cargo test --lib join_pushdown -- --nocapture
cargo test --lib move_around -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit each general correction**

Use the module name in the commit message:

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown
git commit -m "fix: refine OQ-9 predicate derivation"
```

## Task 9: Full Verification

**Files:**
- No planned source edits.

- [ ] **Step 1: Run Rust focused tests**

Run:

```bash
cargo test --lib predicate_group -- --nocapture
cargo test --lib classifier -- --nocapture
cargo test --lib deriver -- --nocapture
cargo test --lib join_pushdown -- --nocapture
cargo test --lib move_around -- --nocapture
cargo test --lib rewrite::registry -- --nocapture
cargo test --lib rewrite::rules -- --nocapture
```

Expected: PASS for all focused tests.

- [ ] **Step 2: Run full optimizer SQL suite**

With standalone-server already running:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify -j 1
```

Expected: PASS.

- [ ] **Step 3: Run q85 dev acceptance**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q85 --mode verify --query-timeout 180 -j 1
```

Expected: PASS. The previous baseline failure was an `EXCHANGE_SOURCE` timeout after 132.80s; this command must complete without that timeout.

- [ ] **Step 4: Run focused TPC-DS sanity**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q48,q64,q85 --mode verify --query-timeout 180 -j 1
```

Expected: PASS for all three queries.

- [ ] **Step 5: Format and compile**

Run:

```bash
cargo fmt
cargo build
```

Expected: both commands PASS.

- [ ] **Step 6: Commit verification-only adjustments**

If `cargo fmt` changed files:

```bash
git add src sql-tests
git commit -m "style: format OQ-9 predicate placement changes"
```

If `cargo fmt` did not change files, leave the tree unchanged.

## Completion Checklist

- [ ] `PushDownPredicateJoin` handles both `Filter(Join)` and `Join(condition)`.
- [ ] OR groups remain atomic unless a necessary side-filter or range envelope is derived.
- [ ] Derived predicates are canonical-deduped.
- [ ] `JoinPredicateMoveAround` is disableable via `SET disable_optimizer_rules = 'JoinPredicateMoveAround'`.
- [ ] Outer/semi/anti guards remain conservative.
- [ ] Optimizer SQL cases verify.
- [ ] `tpc-ds/q85` verifies in dev profile with `--query-timeout 180`.
- [ ] No Rust code contains q85-specific table or column name special cases.
