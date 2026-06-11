# G7 Logical Properties Equivalence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add G7 logical equivalence properties to NovaRocks optimizer and prove them with a narrow inner-join literal predicate consumer.

**Architecture:** Add stable `ColumnId` set and equivalence-class data structures in the optimizer property layer, extend `LogicalProperties`, then derive those properties bottom-up in the existing statistics pass. Add one CBO transformation rule that only matches `INNER JOIN` and inserts child-side filters for one-hop literal equality propagation.

**Tech Stack:** Rust, Cargo unit tests, NovaRocks SQL test runner, optimizer `EXPLAIN VERBOSE` golden directives.

---

## File Structure

- Create: `src/sql/optimizer/logical_props.rs`
  - Owns logical-property derivation helpers, expression equality collectors, output-column filtering, and literal-equality collection shared by the consumer rule.
- Create: `src/sql/optimizer/rules/equivalence_predicate.rs`
  - Owns `InnerJoinEquivalencePredicateRule`, child filter group creation, and duplicate predicate guards.
- Modify: `src/sql/optimizer/property.rs`
  - Adds `ColumnIdSet` and `EquivalenceClasses`.
- Modify: `src/sql/optimizer/memo.rs`
  - Extends `LogicalProperties` with `equivalence_classes` and `unique_columns`, plus a constructor.
- Modify: `src/sql/optimizer/mod.rs`
  - Exposes `logical_props`.
- Modify: `src/sql/optimizer/stats.rs`
  - Replaces direct `LogicalProperties { output_columns, row_count }` construction with the logical-property deriver.
- Modify: `src/sql/optimizer/rules/mod.rs`
  - Registers `equivalence_predicate` and `InnerJoinEquivalencePredicateRule`.
- Create: `sql-tests/optimizer/sql/g7_equivalence_inner_join.sql`
  - Verifies inner join opposite-side predicate propagation.
- Create: `sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql`
  - Verifies non-inner joins do not propagate.

---

### Task 1: Add Stable ColumnId Sets and Equivalence Classes

**Files:**
- Modify: `src/sql/optimizer/property.rs`

- [ ] **Step 1: Write failing property-layer unit tests**

Add this test module content to the existing `#[cfg(test)] mod tests` in `src/sql/optimizer/property.rs`:

```rust
    #[test]
    fn column_id_set_sorts_dedups_and_drops_unset() {
        let set = ColumnIdSet::from_columns([
            ColumnId(3),
            ColumnId::UNSET,
            ColumnId(1),
            ColumnId(3),
            ColumnId(2),
        ]);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![ColumnId(1), ColumnId(2), ColumnId(3)]);
        assert!(set.contains(ColumnId(2)));
        assert!(!set.contains(ColumnId::UNSET));
    }

    #[test]
    fn column_id_set_union_keeps_stable_order() {
        let left = ColumnIdSet::from_columns([ColumnId(3), ColumnId(1)]);
        let right = ColumnIdSet::from_columns([ColumnId(2), ColumnId(3)]);
        assert_eq!(
            left.union(&right).iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)]
        );
    }

    #[test]
    fn equivalence_classes_merge_transitively() {
        let mut classes = EquivalenceClasses::default();
        classes.merge_pair(ColumnId(1), ColumnId(2));
        classes.merge_pair(ColumnId(2), ColumnId(3));
        let class = classes.class_containing(ColumnId(1)).expect("class for c1");
        assert_eq!(class.iter().collect::<Vec<_>>(), vec![ColumnId(1), ColumnId(2), ColumnId(3)]);
        assert_eq!(classes.classes().len(), 1);
    }

    #[test]
    fn equivalence_classes_extend_merges_overlapping_classes() {
        let mut left = EquivalenceClasses::default();
        left.merge_pair(ColumnId(1), ColumnId(2));
        let mut right = EquivalenceClasses::default();
        right.merge_pair(ColumnId(2), ColumnId(4));
        left.extend_from(&right);
        let class = left.class_containing(ColumnId(4)).expect("class for c4");
        assert_eq!(class.iter().collect::<Vec<_>>(), vec![ColumnId(1), ColumnId(2), ColumnId(4)]);
    }
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test column_id_set_ equivalence_classes_ -- --nocapture
```

Expected: compilation fails because `ColumnIdSet` and `EquivalenceClasses` are not defined.

- [ ] **Step 3: Implement `ColumnIdSet` and `EquivalenceClasses`**

Add this code near the top of `src/sql/optimizer/property.rs`, after the `use crate::sql::column_id::ColumnId;` line and before `PhysicalPropertySet`:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct ColumnIdSet {
    columns: Vec<ColumnId>,
}

impl ColumnIdSet {
    pub(crate) fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub(crate) fn single(column: ColumnId) -> Self {
        Self::from_columns([column])
    }

    pub(crate) fn from_columns<I>(columns: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut columns: Vec<ColumnId> = columns
            .into_iter()
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        columns.sort_unstable();
        columns.dedup();
        Self { columns }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn contains(&self, column: ColumnId) -> bool {
        self.columns.binary_search(&column).is_ok()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.columns.iter().copied()
    }

    pub(crate) fn min_column(&self) -> Option<ColumnId> {
        self.columns.first().copied()
    }

    pub(crate) fn union(&self, other: &Self) -> Self {
        Self::from_columns(self.iter().chain(other.iter()))
    }

    pub(crate) fn is_subset(&self, other: &Self) -> bool {
        self.iter().all(|id| other.contains(id))
    }

    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.iter().any(|id| other.contains(id))
    }
}

impl FromIterator<ColumnId> for ColumnIdSet {
    fn from_iter<T: IntoIterator<Item = ColumnId>>(iter: T) -> Self {
        Self::from_columns(iter)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EquivalenceClasses {
    classes: Vec<ColumnIdSet>,
}

impl EquivalenceClasses {
    pub(crate) fn classes(&self) -> &[ColumnIdSet] {
        &self.classes
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.classes.is_empty()
    }

    pub(crate) fn merge_pair(&mut self, left: ColumnId, right: ColumnId) {
        if left == ColumnId::UNSET || right == ColumnId::UNSET || left == right {
            return;
        }

        let mut matched = Vec::new();
        for (idx, class) in self.classes.iter().enumerate() {
            if class.contains(left) || class.contains(right) {
                matched.push(idx);
            }
        }

        match matched.as_slice() {
            [] => self.classes.push(ColumnIdSet::from_columns([left, right])),
            [idx] => {
                let merged = self.classes[*idx].union(&ColumnIdSet::from_columns([left, right]));
                self.classes[*idx] = merged;
            }
            _ => {
                let mut merged = ColumnIdSet::from_columns([left, right]);
                for idx in matched.iter().rev() {
                    let class = self.classes.remove(*idx);
                    merged = merged.union(&class);
                }
                self.classes.push(merged);
                self.normalize();
            }
        }
    }

    pub(crate) fn extend_from(&mut self, other: &Self) {
        for class in other.classes() {
            let ids: Vec<ColumnId> = class.iter().collect();
            if let Some((&first, rest)) = ids.split_first() {
                for id in rest {
                    self.merge_pair(first, *id);
                }
            }
        }
        self.normalize();
    }

    pub(crate) fn class_containing(&self, column: ColumnId) -> Option<&ColumnIdSet> {
        self.classes.iter().find(|class| class.contains(column))
    }

    pub(crate) fn retain_subset_of(&mut self, output_columns: &ColumnIdSet) {
        self.classes = self
            .classes
            .iter()
            .map(|class| ColumnIdSet::from_columns(class.iter().filter(|id| output_columns.contains(*id))))
            .filter(|class| class.len() >= 2)
            .collect();
        self.normalize();
    }

    pub(crate) fn normalize(&mut self) {
        self.classes.sort_by_key(|class| class.min_column().unwrap_or(ColumnId::UNSET));
        self.classes.dedup();
    }
}
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
cargo test column_id_set_ equivalence_classes_ -- --nocapture
```

Expected: all four tests pass.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add src/sql/optimizer/property.rs
git commit -m "feat(optimizer): add ColumnId equivalence sets"
```

---

### Task 2: Extend LogicalProperties and Derive G7 Properties

**Files:**
- Create: `src/sql/optimizer/logical_props.rs`
- Modify: `src/sql/optimizer/memo.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/stats.rs`

- [ ] **Step 1: Write failing derivation tests**

Create `src/sql/optimizer/logical_props.rs` with the module header and tests first:

```rust
//! Logical-property derivation for optimizer Memo groups.

use super::memo::{GroupId, LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use super::property::ColumnIdSet;
use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use arrow::datatypes::DataType;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::LiteralValue;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::operator::{LogicalFilterOp, LogicalJoinOp, LogicalScanOp};
    use std::path::PathBuf;

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
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
            nullable: false,
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
            nullable: false,
        }
    }

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
            }),
            children: Vec::new(),
        })
    }

    #[test]
    fn collect_column_equalities_reads_top_level_and() {
        let predicate = and(eq(col(1, "a"), col(2, "b")), eq(col(1, "a"), lit(10)));
        assert_eq!(collect_column_equalities(&predicate), vec![(ColumnId(1), ColumnId(2))]);
        assert_eq!(collect_literal_equalities(&predicate).len(), 1);
    }

    #[test]
    fn filter_derivation_merges_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp {
                predicate: eq(col(1, "a"), col(2, "b")),
            }),
            children: vec![child],
        });
        let props = derive_for_group(&memo, filter, vec![output(1, "a"), output(2, "b")], 50.0);
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .expect("filter equivalence class");
        assert_eq!(class.iter().collect::<Vec<_>>(), vec![ColumnId(1), ColumnId(2)]);
    }

    #[test]
    fn inner_join_derivation_merges_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props = Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq(col(1, "lk"), col(2, "rk"))),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(&memo, join, vec![output(1, "lk"), output(2, "rk")], 10.0);
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(2))
            .expect("join equivalence class");
        assert_eq!(class.iter().collect::<Vec<_>>(), vec![ColumnId(1), ColumnId(2)]);
    }

    #[test]
    fn left_join_derivation_does_not_merge_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props = Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(eq(col(1, "lk"), col(2, "rk"))),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(&memo, join, vec![output(1, "lk"), output(2, "rk")], 10.0);
        assert!(props.equivalence_classes.class_containing(ColumnId(1)).is_none());
        assert!(props.equivalence_classes.class_containing(ColumnId(2)).is_none());
    }
}
```

- [ ] **Step 2: Wire the module and run tests to verify failure**

Add this line to `src/sql/optimizer/mod.rs`:

```rust
pub(crate) mod logical_props;
```

Run:

```bash
cargo test logical_props -- --nocapture
```

Expected: compilation fails because `LogicalProperties::new`, `derive_for_group`, `collect_column_equalities`, and `collect_literal_equalities` are not implemented.

- [ ] **Step 3: Extend `LogicalProperties`**

Modify `src/sql/optimizer/memo.rs` imports:

```rust
use super::operator::Operator;
use super::property::{ColumnIdSet, EquivalenceClasses};
use crate::sql::analysis::OutputColumn;
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnRefFactory;
```

Replace the `LogicalProperties` struct with:

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) row_count: f64,
    pub(crate) equivalence_classes: EquivalenceClasses,
    pub(crate) unique_columns: Vec<ColumnIdSet>,
}

impl LogicalProperties {
    pub(crate) fn new(output_columns: Vec<OutputColumn>, row_count: f64) -> Self {
        Self {
            output_columns,
            row_count,
            equivalence_classes: EquivalenceClasses::default(),
            unique_columns: Vec::new(),
        }
    }
}
```

- [ ] **Step 4: Implement logical property derivation helpers**

Append this implementation above the `#[cfg(test)] mod tests` block in `src/sql/optimizer/logical_props.rs`:

```rust
pub(crate) fn derive_for_group(
    memo: &Memo,
    group_idx: GroupId,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
) -> LogicalProperties {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());
    let Some(expr) = expr else {
        return LogicalProperties::new(output_columns, row_count);
    };
    derive_for_expr(expr, memo, output_columns, row_count)
}

pub(crate) fn derive_for_expr(
    expr: &MExpr,
    memo: &Memo,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
) -> LogicalProperties {
    let output_ids = output_id_set(&output_columns);
    let mut props = LogicalProperties::new(output_columns, row_count);

    match &expr.op {
        Operator::LogicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_column_equalities(&filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::PhysicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_column_equalities(&filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::LogicalJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                if let Some(condition) = &join.condition {
                    for (left, right) in collect_column_equalities(condition) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::PhysicalHashJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                for eq in &join.eq_conditions {
                    if let (Some(left), Some(right)) =
                        (column_id_from_expr(&eq.left), column_id_from_expr(&eq.right))
                    {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
                if let Some(condition) = &join.other_condition {
                    for (left, right) in collect_column_equalities(condition) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::LogicalAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::PhysicalHashAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::LogicalProject(_)
        | Operator::PhysicalProject(_)
        | Operator::LogicalSubqueryAlias(_)
        | Operator::PhysicalSubqueryAlias(_)
        | Operator::LogicalSort(_)
        | Operator::PhysicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::PhysicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::PhysicalTopN(_)
        | Operator::LogicalWindow(_)
        | Operator::PhysicalWindow(_)
        | Operator::LogicalTableFunction(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalDistribution(_) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
        }
        _ => {}
    }

    props.equivalence_classes.retain_subset_of(&output_ids);
    props.unique_columns
        .retain(|key| !key.is_empty() && key.is_subset(&output_ids));
    props
}

fn inherit_from_child(
    memo: &Memo,
    expr: &MExpr,
    child_slot: usize,
    output_ids: &ColumnIdSet,
    props: &mut LogicalProperties,
) {
    let Some(child_group_id) = expr.children.get(child_slot).copied() else {
        return;
    };
    let Some(child_props) = memo.groups[child_group_id].logical_props.as_ref() else {
        return;
    };
    props
        .equivalence_classes
        .extend_from(&child_props.equivalence_classes);
    for key in &child_props.unique_columns {
        if key.is_subset(output_ids) {
            props.unique_columns.push(key.clone());
        }
    }
    props.equivalence_classes.retain_subset_of(output_ids);
}

fn output_id_set(output_columns: &[OutputColumn]) -> ColumnIdSet {
    ColumnIdSet::from_columns(output_columns.iter().map(|column| column.column_id))
}

pub(crate) fn column_id_from_expr(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        ExprKind::Nested(inner) => column_id_from_expr(inner),
        _ => None,
    }
}

pub(crate) fn collect_column_equalities(expr: &TypedExpr) -> Vec<(ColumnId, ColumnId)> {
    let mut out = Vec::new();
    collect_column_equalities_inner(expr, &mut out);
    out
}

fn collect_column_equalities_inner(expr: &TypedExpr, out: &mut Vec<(ColumnId, ColumnId)>) {
    match &expr.kind {
        ExprKind::Nested(inner) => collect_column_equalities_inner(inner, out),
        ExprKind::BinaryOp { left, op: BinOp::And, right } => {
            collect_column_equalities_inner(left, out);
            collect_column_equalities_inner(right, out);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            if let (Some(left_id), Some(right_id)) =
                (column_id_from_expr(left), column_id_from_expr(right))
            {
                out.push((left_id, right_id));
            }
        }
        _ => {}
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LiteralEquality {
    pub(crate) column_id: ColumnId,
    pub(crate) literal: TypedExpr,
}

pub(crate) fn collect_literal_equalities(expr: &TypedExpr) -> Vec<LiteralEquality> {
    let mut out = Vec::new();
    collect_literal_equalities_inner(expr, &mut out);
    out
}

fn collect_literal_equalities_inner(expr: &TypedExpr, out: &mut Vec<LiteralEquality>) {
    match &expr.kind {
        ExprKind::Nested(inner) => collect_literal_equalities_inner(inner, out),
        ExprKind::BinaryOp { left, op: BinOp::And, right } => {
            collect_literal_equalities_inner(left, out);
            collect_literal_equalities_inner(right, out);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            match (&left.kind, &right.kind) {
                (ExprKind::ColumnRef { column_id, .. }, ExprKind::Literal(_))
                    if *column_id != ColumnId::UNSET =>
                {
                    out.push(LiteralEquality {
                        column_id: *column_id,
                        literal: right.as_ref().clone(),
                    });
                }
                (ExprKind::Literal(_), ExprKind::ColumnRef { column_id, .. })
                    if *column_id != ColumnId::UNSET =>
                {
                    out.push(LiteralEquality {
                        column_id: *column_id,
                        literal: left.as_ref().clone(),
                    });
                }
                _ => {}
            }
        }
        _ => {}
    }
}

pub(crate) fn make_column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

pub(crate) fn make_eq_literal_predicate(column: &OutputColumn, literal: TypedExpr) -> TypedExpr {
    TypedExpr {
        nullable: column.nullable || literal.nullable,
        data_type: DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(make_column_ref_expr(column)),
            op: BinOp::Eq,
            right: Box::new(literal),
        },
    }
}

pub(crate) fn combine_with_and(mut predicates: Vec<TypedExpr>) -> Option<TypedExpr> {
    let first = predicates.drain(..1).next()?;
    Some(predicates.into_iter().fold(first, |left, right| TypedExpr {
        nullable: left.nullable || right.nullable,
        data_type: DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::And,
            right: Box::new(right),
        },
    }))
}
```

- [ ] **Step 5: Use the deriver from `stats.rs`**

In `src/sql/optimizer/stats.rs`, replace:

```rust
        memo.groups[group_idx].logical_props = Some(LogicalProperties {
            output_columns,
            row_count: stats.output_row_count,
        });
```

with:

```rust
        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_group(
            memo,
            group_idx,
            output_columns,
            stats.output_row_count,
        ));
```

Then remove the now-unused `LogicalProperties` import from the top of `stats.rs` if the compiler reports it.

- [ ] **Step 6: Fix direct `LogicalProperties` construction sites**

Run:

```bash
rg -n "LogicalProperties \\{" src/sql/optimizer
```

For each result, replace direct struct construction with `LogicalProperties::new(output_columns, row_count)` and then assign any required extra fields explicitly. The current known site is in `src/sql/optimizer/rules/implement.rs` tests; use this shape:

```rust
memo.groups[gid].logical_props = Some(LogicalProperties::new(
    vec![OutputColumn {
        column_id: ColumnId(1),
        name: "c1".to_string(),
        data_type: DataType::Int64,
        nullable: false,
    }],
    10.0,
));
```

- [ ] **Step 7: Run derivation tests**

Run:

```bash
cargo test logical_props -- --nocapture
```

Expected: all tests in `src/sql/optimizer/logical_props.rs` pass.

- [ ] **Step 8: Commit Task 2**

Run:

```bash
git add src/sql/optimizer/logical_props.rs src/sql/optimizer/memo.rs src/sql/optimizer/mod.rs src/sql/optimizer/stats.rs src/sql/optimizer/rules/implement.rs
git commit -m "feat(optimizer): derive logical equivalence properties"
```

---

### Task 3: Add the Inner Join Literal Predicate Consumer

**Files:**
- Create: `src/sql/optimizer/rules/equivalence_predicate.rs`
- Modify: `src/sql/optimizer/rules/mod.rs`

- [ ] **Step 1: Write failing rule tests**

Create `src/sql/optimizer/rules/equivalence_predicate.rs` with tests first:

```rust
//! Inner join equivalence predicate propagation.

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::logical_props::{
    collect_column_equalities, collect_literal_equalities, combine_with_and,
    make_eq_literal_predicate,
};
use crate::sql::optimizer::memo::{GroupId, LogicalProperties, MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalFilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use arrow::datatypes::DataType;
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::operator::LogicalScanOp;
    use std::path::PathBuf;

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
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
            nullable: false,
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
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
            }),
            children: Vec::new(),
        });
        memo.groups[group].logical_props = Some(LogicalProperties::new(vec![output(id, name)], 10.0));
        group
    }

    #[test]
    fn propagates_literal_from_left_to_right() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10)))),
            }),
            children: vec![left, right],
        };

        let out = InnerJoinEquivalencePredicateRule.apply(&join, &mut memo);
        assert_eq!(out.len(), 1);
        let new_right = out[0].children[1];
        let filter = memo.groups[new_right]
            .logical_exprs
            .iter()
            .find_map(|expr| match &expr.op {
                Operator::LogicalFilter(filter) => Some(filter),
                _ => None,
            })
            .expect("right child filter");
        assert_eq!(collect_literal_equalities(&filter.predicate).len(), 1);
    }

    #[test]
    fn does_not_fire_for_left_outer_join() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10)))),
            }),
            children: vec![left, right],
        };
        assert!(!InnerJoinEquivalencePredicateRule.matches(&join.op));
        assert!(InnerJoinEquivalencePredicateRule.apply(&join, &mut memo).is_empty());
    }

    #[test]
    fn does_not_duplicate_existing_target_predicate() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right_scan = scan_group(&mut memo, 2, "rk");
        let right_filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp {
                predicate: eq(col(2, "rk"), lit(10)),
            }),
            children: vec![right_scan],
        });
        memo.groups[right_filter].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 5.0));
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10)))),
            }),
            children: vec![left, right_filter],
        };
        assert!(InnerJoinEquivalencePredicateRule.apply(&join, &mut memo).is_empty());
    }
}
```

- [ ] **Step 2: Register the module and run tests to verify failure**

In `src/sql/optimizer/rules/mod.rs`, add:

```rust
pub(crate) mod equivalence_predicate;
```

Run:

```bash
cargo test equivalence_predicate -- --nocapture
```

Expected: compilation fails because `InnerJoinEquivalencePredicateRule` is not implemented.

- [ ] **Step 3: Implement the rule**

Insert this implementation above the `#[cfg(test)] mod tests` block in `src/sql/optimizer/rules/equivalence_predicate.rs`:

```rust
pub(crate) struct InnerJoinEquivalencePredicateRule;

impl Rule for InnerJoinEquivalencePredicateRule {
    fn name(&self) -> &str {
        "InnerJoinEquivalencePredicateRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                ..
            })
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(join) = &expr.op else {
            return Vec::new();
        };
        if join.join_type != JoinKind::Inner || expr.children.len() != 2 {
            return Vec::new();
        }

        let left_group = expr.children[0];
        let right_group = expr.children[1];
        let Some(left_props) = memo.groups[left_group].logical_props.clone() else {
            return Vec::new();
        };
        let Some(right_props) = memo.groups[right_group].logical_props.clone() else {
            return Vec::new();
        };

        let left_columns = columns_by_id(&left_props.output_columns);
        let right_columns = columns_by_id(&right_props.output_columns);
        let mut literal_by_column = literal_equalities_from_join(join)
            .into_iter()
            .chain(literal_equalities_from_group(memo, left_group))
            .chain(literal_equalities_from_group(memo, right_group))
            .collect::<HashMap<ColumnId, TypedExpr>>();
        expand_literals_with_equivalence(&left_props, &mut literal_by_column);
        expand_literals_with_equivalence(&right_props, &mut literal_by_column);

        let mut left_new = Vec::new();
        let mut right_new = Vec::new();
        for (raw_left, raw_right) in join_column_pairs(join) {
            let Some((left_id, right_id)) =
                orient_pair(raw_left, raw_right, &left_columns, &right_columns)
            else {
                continue;
            };

            if let Some(literal) = literal_by_column.get(&left_id).cloned() {
                if !has_literal_equality(memo, right_group, right_id, &literal) {
                    if let Some(column) = right_columns.get(&right_id) {
                        right_new.push(make_eq_literal_predicate(column, literal));
                    }
                }
            }
            if let Some(literal) = literal_by_column.get(&right_id).cloned() {
                if !has_literal_equality(memo, left_group, left_id, &literal) {
                    if let Some(column) = left_columns.get(&left_id) {
                        left_new.push(make_eq_literal_predicate(column, literal));
                    }
                }
            }
        }

        if left_new.is_empty() && right_new.is_empty() {
            return Vec::new();
        }

        let new_left = if left_new.is_empty() {
            left_group
        } else {
            add_filter_group(memo, left_group, left_new)
        };
        let new_right = if right_new.is_empty() {
            right_group
        } else {
            add_filter_group(memo, right_group, right_new)
        };

        vec![NewExpr {
            op: Operator::LogicalJoin(join.clone()),
            children: vec![new_left, new_right],
        }]
    }
}

fn columns_by_id(columns: &[OutputColumn]) -> HashMap<ColumnId, OutputColumn> {
    columns
        .iter()
        .filter(|column| column.column_id != ColumnId::UNSET)
        .map(|column| (column.column_id, column.clone()))
        .collect()
}

fn expand_literals_with_equivalence(
    props: &LogicalProperties,
    literal_by_column: &mut HashMap<ColumnId, TypedExpr>,
) {
    let mut additions = Vec::new();
    for class in props.equivalence_classes.classes() {
        let literal = class
            .iter()
            .find_map(|column_id| literal_by_column.get(&column_id).cloned());
        if let Some(literal) = literal {
            for column_id in class.iter() {
                additions.push((column_id, literal.clone()));
            }
        }
    }
    for (column_id, literal) in additions {
        literal_by_column.entry(column_id).or_insert(literal);
    }
}

fn join_column_pairs(join: &LogicalJoinOp) -> Vec<(ColumnId, ColumnId)> {
    join.condition
        .as_ref()
        .map(collect_column_equalities)
        .unwrap_or_default()
}

fn literal_equalities_from_join(join: &LogicalJoinOp) -> Vec<(ColumnId, TypedExpr)> {
    join.condition
        .as_ref()
        .map(|condition| {
            collect_literal_equalities(condition)
                .into_iter()
                .map(|eq| (eq.column_id, eq.literal))
                .collect()
        })
        .unwrap_or_default()
}

fn literal_equalities_from_group(memo: &Memo, group_id: GroupId) -> Vec<(ColumnId, TypedExpr)> {
    let mut out = Vec::new();
    let Some(group) = memo.groups.get(group_id) else {
        return out;
    };
    for expr in &group.logical_exprs {
        match &expr.op {
            Operator::LogicalFilter(filter) => {
                out.extend(
                    collect_literal_equalities(&filter.predicate)
                        .into_iter()
                        .map(|eq| (eq.column_id, eq.literal)),
                );
            }
            Operator::LogicalScan(scan) => {
                for predicate in &scan.predicates {
                    out.extend(
                        collect_literal_equalities(predicate)
                            .into_iter()
                            .map(|eq| (eq.column_id, eq.literal)),
                    );
                }
            }
            _ => {}
        }
    }
    out
}

fn orient_pair(
    left: ColumnId,
    right: ColumnId,
    left_columns: &HashMap<ColumnId, OutputColumn>,
    right_columns: &HashMap<ColumnId, OutputColumn>,
) -> Option<(ColumnId, ColumnId)> {
    if left_columns.contains_key(&left) && right_columns.contains_key(&right) {
        Some((left, right))
    } else if left_columns.contains_key(&right) && right_columns.contains_key(&left) {
        Some((right, left))
    } else {
        None
    }
}

fn has_literal_equality(
    memo: &Memo,
    group_id: GroupId,
    column_id: ColumnId,
    literal: &TypedExpr,
) -> bool {
    literal_equalities_from_group(memo, group_id)
        .into_iter()
        .any(|(existing_column, existing_literal)| {
            existing_column == column_id && literal_signature(&existing_literal) == literal_signature(literal)
        })
}

fn literal_signature(expr: &TypedExpr) -> String {
    format!("{:?}:{:?}", expr.data_type, expr.kind)
}

fn add_filter_group(memo: &mut Memo, child_group: GroupId, predicates: Vec<TypedExpr>) -> GroupId {
    let predicate = combine_with_and(predicates).expect("filter group needs at least one predicate");
    let filter_expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalFilter(LogicalFilterOp { predicate }),
        children: vec![child_group],
    };
    let new_group = memo.new_group(filter_expr);
    if let Some(child_props) = memo.groups[child_group].logical_props.as_ref() {
        let row_count = (child_props.row_count * 0.1).max(1.0);
        let output_columns = child_props.output_columns.clone();
        let props = crate::sql::optimizer::logical_props::derive_for_group(
            memo,
            new_group,
            output_columns,
            row_count,
        );
        memo.groups[new_group].logical_props = Some(props);
    }
    new_group
}
```

- [ ] **Step 4: Register the transformation rule**

In `src/sql/optimizer/rules/mod.rs`, add the rule after `JoinAssociativity`:

```rust
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(equivalence_predicate::InnerJoinEquivalencePredicateRule),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_top_n::SplitTopN),
    ]
}
```

- [ ] **Step 5: Run rule tests**

Run:

```bash
cargo test equivalence_predicate -- --nocapture
```

Expected: all rule tests pass.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add src/sql/optimizer/rules/equivalence_predicate.rs src/sql/optimizer/rules/mod.rs
git commit -m "feat(optimizer): propagate inner join equivalence predicates"
```

---

### Task 4: Add Optimizer SQL Golden Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/g7_equivalence_inner_join.sql`
- Create: `sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql`

- [ ] **Step 1: Add inner join SQL regression**

Create `sql-tests/optimizer/sql/g7_equivalence_inner_join.sql`:

```sql
-- @explain_contains=PhysicalFilter
-- @explain_contains=rk = 10
CREATE TABLE g7_l (
    lk BIGINT,
    payload BIGINT
);

CREATE TABLE g7_r (
    rk BIGINT,
    payload BIGINT
);

EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM g7_l l
JOIN g7_r r ON l.lk = r.rk AND l.lk = 10;

-- @explain_contains=PhysicalFilter
-- @explain_contains=lk = 20
EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM g7_l l
JOIN g7_r r ON l.lk = r.rk AND r.rk = 20;

DROP TABLE g7_l;
DROP TABLE g7_r;
```

- [ ] **Step 2: Add non-inner join guard SQL regression**

Create `sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql`:

```sql
-- @explain_contains=LEFT
-- @explain_not_contains=rk = 10
CREATE TABLE g7_outer_l (
    lk BIGINT,
    payload BIGINT
);

CREATE TABLE g7_outer_r (
    rk BIGINT,
    payload BIGINT
);

EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM g7_outer_l l
LEFT JOIN g7_outer_r r ON l.lk = r.rk AND l.lk = 10;

DROP TABLE g7_outer_l;
DROP TABLE g7_outer_r;
```

- [ ] **Step 3: Run SQL tests and inspect actual plan text**

Run with the local generated config when available:

```bash
if [ -f docker/iceberg-rest/runtime/current/env.sh ]; then
  source docker/iceberg-rest/runtime/current/env.sh
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --mode verify --only g7_equivalence_inner_join,g7_equivalence_outer_join_guard
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --mode verify --only g7_equivalence_inner_join,g7_equivalence_outer_join_guard
fi
```

Expected: the new SQL files are discovered. The first run may fail because the exact `EXPLAIN` substrings differ from the initial guesses.

- [ ] **Step 4: Adjust only brittle `@explain_contains` strings**

If Step 3 fails only because the explain text uses different casing or a qualified display name, update the directive lines to the exact substrings shown in the runner output. Keep these semantic assertions:

```sql
-- inner join file must assert the opposite-side literal predicate appears
-- outer join file must assert the nullable-side propagated predicate does not appear
```

Do not weaken the test to only check that the query compiles.

- [ ] **Step 5: Re-run SQL tests**

Run the same command from Step 3.

Expected: `g7_equivalence_inner_join` and `g7_equivalence_outer_join_guard` pass in verify mode.

- [ ] **Step 6: Commit Task 4**

Run:

```bash
git add sql-tests/optimizer/sql/g7_equivalence_inner_join.sql sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql
git commit -m "test(optimizer): cover G7 equivalence predicate propagation"
```

---

### Task 5: Final Verification and Cleanup

**Files:**
- Modify only files already touched by Tasks 1-4 if verification exposes compile or test issues.

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt --all -- --check
```

Expected: exit code 0. If it fails, run `cargo fmt --all`, inspect `git diff`, then re-run the check.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test column_id_set_ equivalence_classes_ logical_props equivalence_predicate -- --nocapture
```

Expected: all focused tests pass.

- [ ] **Step 3: Run focused SQL tests**

Run:

```bash
if [ -f docker/iceberg-rest/runtime/current/env.sh ]; then
  source docker/iceberg-rest/runtime/current/env.sh
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --mode verify --only g7_equivalence_inner_join,g7_equivalence_outer_join_guard
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --mode verify --only g7_equivalence_inner_join,g7_equivalence_outer_join_guard
fi
```

Expected: both G7 SQL cases pass in verify mode.

- [ ] **Step 4: Run build**

Run:

```bash
cargo build
```

Expected: build completes with exit code 0.

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git status --short
git diff --check
git diff --stat HEAD~4..HEAD
```

Expected: no whitespace errors; diff contains only optimizer property/rule files and optimizer SQL tests from this plan.

- [ ] **Step 6: Commit verification fixes if any were needed**

If formatter or verification required code changes after Task 4, commit them:

```bash
git add src/sql/optimizer/property.rs src/sql/optimizer/memo.rs src/sql/optimizer/mod.rs src/sql/optimizer/stats.rs src/sql/optimizer/logical_props.rs src/sql/optimizer/rules/mod.rs src/sql/optimizer/rules/equivalence_predicate.rs sql-tests/optimizer/sql/g7_equivalence_inner_join.sql sql-tests/optimizer/sql/g7_equivalence_outer_join_guard.sql
git commit -m "fix(optimizer): stabilize G7 equivalence verification"
```

If no changes were needed, do not create an empty commit.
