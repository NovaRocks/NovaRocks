# OQ-15 TopN Sort Compactness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make standalone TopN/Sort plans more compact by proving and applying safe TopN merge, Sort elision, scan/project/join/aggregate/set-op pushdown candidates, and split TopN visible-shape cleanup.

**Architecture:** Keep TopN in the current Cascades/Memo optimizer instead of adding a LogicalPlan-level TopN node. Add one focused proof helper that answers semantic-equivalence questions from structure, `EquivalenceClasses`, `unique_columns`, and pure Project remaps; then add small Cascades transformation rules that fail closed when proof is unavailable. Search remains responsible for choosing among proven-equivalent candidates, while property derivation and codegen keep distributed TopN semantics intact.

**Tech Stack:** Rust crate `novarocks`, Cascades optimizer under `src/sql/optimizer`, fragment/explain codegen under `src/sql/codegen` and `src/sql/explain.rs`, SQL golden tests under `sql-tests/optimizer` and `sql-tests/sort`.

**Spec:** `docs/design/specs/2026-06-10-oq-15-topn-sort-compactness-design.md`

---

## Scope Check

The approved spec is broad, but it is one coherent optimizer workstream: all tasks consume the same TopN/Sort proof layer and all outputs are testable through optimizer EXPLAIN goldens. This plan implements it in three independently committable batches:

1. Proof helper + compactness core.
2. Pushdown candidates with conservative guards.
3. Property/codegen polish and plan-quality validation.

If a task becomes too large during execution, split it at a commit boundary after the failing tests are in place and passing for the completed subset.

## File Structure

Create:
- `src/sql/optimizer/topn_proof.rs`
  - Owns sort-key equivalence, TopN window math, pure Project remaps, and scan capability decisions.
- `src/sql/optimizer/cascades_rules/topn_compactness.rs`
  - Owns `MergeConsecutiveTopN`, `RemoveRedundantSortUnderTopN`, `PushTopNIntoScan`, `PushTopNThroughProject`, `PushTopNThroughJoin`, `PushTopNThroughAggregate`, and `PushTopNThroughSetOp`.
- `sql-tests/optimizer/sql/topn_compactness_merge.sql`
  - EXPLAIN golden for consecutive TopN merge and rule disable.
- `sql-tests/optimizer/sql/topn_compactness_sort_elision.sql`
  - EXPLAIN golden for `TopN(Sort(child))` Sort removal.
- `sql-tests/optimizer/sql/topn_compactness_scan_project.sql`
  - EXPLAIN golden for scan guard and Project alias remap.
- `sql-tests/optimizer/sql/topn_compactness_join_aggregate_setop.sql`
  - EXPLAIN golden for join/aggregate/set-op pushdown guards.

Modify:
- `src/sql/optimizer/mod.rs`
  - Export the new `topn_proof` module.
- `src/sql/optimizer/cascades_rules/mod.rs`
  - Register new TopN compactness transformation rules.
- `src/sql/optimizer/property.rs`
  - Add regression tests that preserve the existing strict `satisfies` contract while equivalence-aware checks stay in `topn_proof`.
- `src/sql/optimizer/derive/top_n.rs`
  - Keep TopN output properties conservative and add regression tests for non-column sort keys.
- `src/sql/optimizer/extract.rs`
  - Avoid wrapping or rendering redundant Sort enforcers after proof-based ordering satisfaction is available.
- `src/sql/codegen/fragment_builder.rs`
  - Keep `Final(split)` TopN lowering strict and validate split visible shape against generated fragment plans.
- `src/sql/explain.rs`
  - Preserve stable TopN/SORT labels while avoiding duplicate global TopN diagnostics.

Common commands:
- Format: `cargo fmt`
- Focused Rust tests: `cargo test --lib topn_proof -- --nocapture`
- Focused optimizer rule tests: `cargo test --lib topn_compactness -- --nocapture`
- Build: `cargo build`
- SQL runner after starting standalone server from the generated environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Run optimizer SQL cases:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only topn_compactness_merge,topn_compactness_sort_elision,topn_compactness_scan_project,topn_compactness_join_aggregate_setop \
  --mode verify
```

Commit message language is English.

---

## Batch 1 - Proof Helper And Compactness Core

### Task 1: Add TopN Proof Helper Foundation

**Files:**
- Create: `src/sql/optimizer/topn_proof.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [x] **Step 1: Add the module export**

In `src/sql/optimizer/mod.rs`, add this line near the other optimizer modules:

```rust
pub(crate) mod topn_proof;
```

- [x] **Step 2: Write failing proof helper tests**

Create `src/sql/optimizer/topn_proof.rs` with this test-first skeleton:

```rust
use crate::sql::analysis::{ExprKind, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::property::{EquivalenceClasses, SortKey, typed_expr_to_column_id};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopNWindow {
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl TopNWindow {
    pub(crate) fn from_limit_offset(limit: Option<i64>, offset: Option<i64>) -> Option<Self> {
        let limit = limit?;
        if limit < 0 {
            return None;
        }
        let offset = offset.unwrap_or(0);
        if offset < 0 {
            return None;
        }
        Some(Self { limit, offset })
    }

    pub(crate) fn end_exclusive(self) -> Option<i64> {
        self.offset.checked_add(self.limit)
    }

    pub(crate) fn covers(self, needed: Self) -> bool {
        let Some(self_end) = self.end_exclusive() else {
            return false;
        };
        let Some(needed_end) = needed.end_exclusive() else {
            return false;
        };
        self.offset <= needed.offset && self_end >= needed_end
    }
}

pub(crate) fn sort_items_to_keys(items: &[SortItem]) -> Option<Vec<SortKey>> {
    items
        .iter()
        .map(|item| {
            typed_expr_to_column_id(&item.expr).map(|column| SortKey {
                column,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(crate) fn sort_keys_equivalent(
    left: &[SortKey],
    right: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).all(|(l, r)| {
        l.asc == r.asc
            && l.nulls_first == r.nulls_first
            && columns_equivalent(l.column, r.column, equivalences)
    })
}

pub(crate) fn ordering_covers(
    provided: &[SortKey],
    required: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    provided.len() >= required.len()
        && provided
            .iter()
            .take(required.len())
            .zip(required)
            .all(|(p, r)| {
                p.asc == r.asc
                    && p.nulls_first == r.nulls_first
                    && columns_equivalent(p.column, r.column, equivalences)
            })
}

pub(crate) fn columns_equivalent(
    left: ColumnId,
    right: ColumnId,
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left == right {
        return true;
    }
    equivalences
        .and_then(|classes| classes.class_containing(left))
        .map(|class| class.contains(right))
        .unwrap_or(false)
}

pub(crate) fn pure_project_column_remap(items: &[ProjectItem]) -> Vec<(ColumnId, ColumnId)> {
    items
        .iter()
        .filter_map(|item| {
            let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                return None;
            };
            if *column_id == ColumnId::UNSET || item.output_column_id == ColumnId::UNSET {
                return None;
            }
            Some((item.output_column_id, *column_id))
        })
        .collect()
}

pub(crate) fn remap_sort_items_through_project(
    items: &[SortItem],
    project_items: &[ProjectItem],
) -> Option<Vec<SortItem>> {
    let remap = pure_project_column_remap(project_items);
    items
        .iter()
        .map(|item| {
            let output_col = typed_expr_to_column_id(&item.expr)?;
            let input_col = remap
                .iter()
                .find_map(|(out, input)| (*out == output_col).then_some(*input))?;
            let mut remapped = item.clone();
            remapped.expr = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: input_col,
                    qualifier: None,
                    column: format!("{}", input_col),
                },
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            };
            Some(remapped)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{LiteralValue, ProjectItem};
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn sort_item(id: u32, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr: col(id, &format!("c{id}")),
            asc,
            nulls_first,
        }
    }

    #[test]
    fn topn_window_requires_finite_non_negative_limit_and_offset() {
        assert_eq!(
            TopNWindow::from_limit_offset(Some(10), Some(2)),
            Some(TopNWindow { limit: 10, offset: 2 })
        );
        assert_eq!(TopNWindow::from_limit_offset(None, Some(2)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(-1), Some(0)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(1), Some(-1)), None);
    }

    #[test]
    fn topn_window_covers_required_range() {
        let inner = TopNWindow { limit: 20, offset: 0 };
        let outer = TopNWindow { limit: 5, offset: 10 };
        assert!(inner.covers(outer));
        assert!(!outer.covers(inner));
    }

    #[test]
    fn sort_keys_use_equivalence_classes() {
        let mut eq = EquivalenceClasses::default();
        eq.merge_pair(ColumnId(1), ColumnId(2));
        let left = sort_items_to_keys(&[sort_item(1, true, false)]).unwrap();
        let right = sort_items_to_keys(&[sort_item(2, true, false)]).unwrap();
        assert!(sort_keys_equivalent(&left, &right, Some(&eq)));
    }

    #[test]
    fn sort_keys_reject_direction_or_null_order_mismatch() {
        let asc = sort_items_to_keys(&[sort_item(1, true, false)]).unwrap();
        let desc = sort_items_to_keys(&[sort_item(1, false, false)]).unwrap();
        let nulls_first = sort_items_to_keys(&[sort_item(1, true, true)]).unwrap();
        assert!(!sort_keys_equivalent(&asc, &desc, None));
        assert!(!sort_keys_equivalent(&asc, &nulls_first, None));
    }

    #[test]
    fn project_remap_accepts_column_refs_only() {
        let project_items = vec![
            ProjectItem {
                expr: col(1, "a"),
                output_name: "x".to_string(),
                output_column_id: ColumnId(10),
            },
            ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(7)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "lit".to_string(),
                output_column_id: ColumnId(11),
            },
        ];

        assert_eq!(pure_project_column_remap(&project_items), vec![(ColumnId(10), ColumnId(1))]);
        assert!(remap_sort_items_through_project(&[sort_item(10, true, false)], &project_items).is_some());
        assert!(remap_sort_items_through_project(&[sort_item(11, true, false)], &project_items).is_none());
    }
}
```

- [x] **Step 3: Run proof helper tests**

Run:

```bash
cargo test --lib topn_proof -- --nocapture
```

Expected: PASS.

- [x] **Step 4: Format and commit**

```bash
cargo fmt
git add src/sql/optimizer/mod.rs src/sql/optimizer/topn_proof.rs
git commit -m "feat(optimizer): add TopN proof helper"
```

### Task 2: Add MergeConsecutiveTopN Rule

**Files:**
- Create: `src/sql/optimizer/cascades_rules/topn_compactness.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [x] **Step 1: Write failing rule tests**

Create `src/sql/optimizer/cascades_rules/topn_compactness.rs` with this initial module:

```rust
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::topn_proof::{TopNWindow, sort_items_to_keys, sort_keys_equivalent};

pub(crate) struct MergeConsecutiveTopN;

impl Rule for MergeConsecutiveTopN {
    fn name(&self) -> &str {
        "MergeConsecutiveTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        merge_consecutive_topn(expr, memo)
    }
}

fn merge_consecutive_topn(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(outer) = &expr.op else {
        return Vec::new();
    };
    if expr.children.len() != 1 {
        return Vec::new();
    }
    let Some(outer_window) = TopNWindow::from_limit_offset(outer.limit, outer.offset) else {
        return Vec::new();
    };
    let Some(outer_keys) = sort_items_to_keys(&outer.items) else {
        return Vec::new();
    };
    let child_group_id = expr.children[0];
    let Some(child_group) = memo.groups.get(child_group_id) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for child_expr in &child_group.logical_exprs {
        let Operator::LogicalTopN(inner) = &child_expr.op else {
            continue;
        };
        if child_expr.children.len() != 1 {
            continue;
        }
        if !topn_phase_can_merge(outer, inner) {
            continue;
        }
        let Some(inner_window) = TopNWindow::from_limit_offset(inner.limit, inner.offset) else {
            continue;
        };
        if !inner_window.covers(outer_window) {
            continue;
        }
        let Some(inner_keys) = sort_items_to_keys(&inner.items) else {
            continue;
        };
        let equivalences = child_group
            .logical_props
            .as_ref()
            .map(|props| &props.equivalence_classes);
        if !sort_keys_equivalent(&outer_keys, &inner_keys, equivalences) {
            continue;
        }
        out.push(NewExpr {
            op: Operator::LogicalTopN(outer.clone()),
            children: child_expr.children.clone(),
        });
    }
    out
}

fn topn_phase_can_merge(outer: &LogicalTopNOp, inner: &LogicalTopNOp) -> bool {
    matches!(
        (outer.phase, inner.phase, outer.is_split, inner.is_split),
        (TopNPhase::Final, TopNPhase::Final, false, false)
            | (TopNPhase::Final, TopNPhase::Partial, true, false)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};
    use arrow::datatypes::DataType;

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn sort_item(id: u32) -> SortItem {
        SortItem {
            expr: col(id),
            asc: true,
            nulls_first: false,
        }
    }

    fn values_group(memo: &mut Memo) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        })
    }

    fn topn_group(memo: &mut Memo, child: usize, limit: i64, offset: i64) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![sort_item(1)],
                limit: Some(limit),
                offset: Some(offset),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![child],
        })
    }

    #[test]
    fn merges_consecutive_topn_when_inner_window_covers_outer() {
        let mut memo = Memo::new();
        let base = values_group(&mut memo);
        let inner = topn_group(&mut memo, base, 20, 0);
        let outer = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![sort_item(1)],
                limit: Some(5),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![inner],
        };

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].children, vec![base]);
    }

    #[test]
    fn does_not_merge_when_inner_window_is_too_small() {
        let mut memo = Memo::new();
        let base = values_group(&mut memo);
        let inner = topn_group(&mut memo, base, 10, 0);
        let outer = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![sort_item(1)],
                limit: Some(5),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![inner],
        };

        assert!(MergeConsecutiveTopN.apply(&outer, &mut memo).is_empty());
    }
}
```

- [x] **Step 2: Register the module and rule**

In `src/sql/optimizer/cascades_rules/mod.rs`, add:

```rust
pub(crate) mod topn_compactness;
```

In `all_transformation_rules()`, add `MergeConsecutiveTopN` after `SplitTopN`:

```rust
        Box::new(split_top_n::SplitTopN),
        Box::new(topn_compactness::MergeConsecutiveTopN),
```

- [x] **Step 3: Run rule tests**

Run:

```bash
cargo test --lib topn_compactness::tests::merges_consecutive_topn_when_inner_window_covers_outer -- --nocapture
cargo test --lib topn_compactness::tests::does_not_merge_when_inner_window_is_too_small -- --nocapture
```

Expected: PASS.

- [x] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): merge consecutive TopN alternatives"
```

### Task 3: Add Redundant Sort Under TopN Elision

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [x] **Step 1: Add failing tests for Sort elision**

Append this rule implementation shell and tests to `topn_compactness.rs`:

```rust
pub(crate) struct RemoveRedundantSortUnderTopN;

impl Rule for RemoveRedundantSortUnderTopN {
    fn name(&self) -> &str {
        "RemoveRedundantSortUnderTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        remove_redundant_sort_under_topn(expr, memo)
    }
}

fn remove_redundant_sort_under_topn(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return Vec::new();
    };
    if expr.children.len() != 1 {
        return Vec::new();
    }
    let Some(topn_keys) = sort_items_to_keys(&topn.items) else {
        return Vec::new();
    };
    let Some(child_group) = memo.groups.get(expr.children[0]) else {
        return Vec::new();
    };
    let equivalences = child_group
        .logical_props
        .as_ref()
        .map(|props| &props.equivalence_classes);
    let mut out = Vec::new();
    for child_expr in &child_group.logical_exprs {
        let Operator::LogicalSort(sort) = &child_expr.op else {
            continue;
        };
        if !sort.analytic_partition_exprs.is_empty() || child_expr.children.len() != 1 {
            continue;
        }
        let Some(sort_keys) = sort_items_to_keys(&sort.items) else {
            continue;
        };
        if crate::sql::optimizer::topn_proof::ordering_covers(&sort_keys, &topn_keys, equivalences)
        {
            out.push(NewExpr {
                op: Operator::LogicalTopN(topn.clone()),
                children: child_expr.children.clone(),
            });
        }
    }
    out
}
```

Add this test to the existing `tests` module:

```rust
#[test]
fn removes_plain_sort_under_matching_topn() {
    use crate::sql::optimizer::operator::LogicalSortOp;

    let mut memo = Memo::new();
    let base = values_group(&mut memo);
    let sort_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalSort(LogicalSortOp {
            items: vec![sort_item(1)],
            analytic_partition_exprs: vec![],
        }),
        children: vec![base],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![sort_item(1)],
            limit: Some(10),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![sort_group],
    };

    let out = RemoveRedundantSortUnderTopN.apply(&topn, &mut memo);

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].children, vec![base]);
}
```

- [x] **Step 2: Register the rule**

In `all_transformation_rules()` add:

```rust
        Box::new(topn_compactness::RemoveRedundantSortUnderTopN),
```

immediately after `MergeConsecutiveTopN`.

- [x] **Step 3: Run focused tests**

Run:

```bash
cargo test --lib topn_compactness::tests::removes_plain_sort_under_matching_topn -- --nocapture
```

Expected: PASS.

- [x] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): remove redundant sort under TopN"
```

### Task 4: Add Compactness SQL Goldens For Merge And Sort Elision

**Files:**
- Create: `sql-tests/optimizer/sql/topn_compactness_merge.sql`
- Create: `sql-tests/optimizer/sql/topn_compactness_sort_elision.sql`

- [x] **Step 1: Create merge golden**

Create `sql-tests/optimizer/sql/topn_compactness_merge.sql`:

```sql
-- @tags=optimizer,topn,compactness
-- @skip_result_check=true
-- @explain_contains=TOP-N
-- @explain_contains=stats={rows=
EXPLAIN VERBOSE
SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

SET disable_optimizer_rules = 'MergeConsecutiveTopN';

-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

SET disable_optimizer_rules = '';
```

- [x] **Step 2: Create sort elision golden**

Create `sql-tests/optimizer/sql/topn_compactness_sort_elision.sql`:

```sql
-- @tags=optimizer,topn,sort,compactness
-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT id, score
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 30 AS score
    ) t
    ORDER BY score DESC, id ASC
) sorted_t
ORDER BY score DESC, id ASC
LIMIT 2;

SET disable_optimizer_rules = 'RemoveRedundantSortUnderTopN';

-- @skip_result_check=true
-- @explain_contains=SORT
EXPLAIN VERBOSE
SELECT id, score
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 30 AS score
    ) t
    ORDER BY score DESC, id ASC
) sorted_t
ORDER BY score DESC, id ASC
LIMIT 2;

SET disable_optimizer_rules = '';
```

- [x] **Step 3: Run optimizer cases**

Start standalone server from the generated environment, then run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only topn_compactness_merge,topn_compactness_sort_elision \
  --mode verify
```

Expected: PASS. If output goldens are missing because these are new cases, run the same command with `--mode record`, inspect the generated result files for compactness, then rerun `--mode verify`.

- [x] **Step 4: Commit**

```bash
git add sql-tests/optimizer/sql/topn_compactness_merge.sql sql-tests/optimizer/sql/topn_compactness_sort_elision.sql sql-tests/optimizer/result/topn_compactness_merge.result sql-tests/optimizer/result/topn_compactness_sort_elision.result
git commit -m "test(optimizer): add TopN compactness goldens"
```

## Batch 2 - Pushdown Candidates

### Task 5: Add Project TopN Pushdown

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs`

- [x] **Step 1: Add Project pushdown rule**

Append this rule:

```rust
pub(crate) struct PushTopNThroughProject;

impl Rule for PushTopNThroughProject {
    fn name(&self) -> &str {
        "PushTopNThroughProject"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_through_project(expr, memo)
    }
}

fn push_topn_through_project(expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return Vec::new();
    };
    if expr.children.len() != 1 {
        return Vec::new();
    }
    let Some(project_group) = memo.groups.get(expr.children[0]).cloned() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for project_expr in project_group.logical_exprs {
        let Operator::LogicalProject(project) = &project_expr.op else {
            continue;
        };
        if project_expr.children.len() != 1 {
            continue;
        }
        let Some(remapped_items) =
            crate::sql::optimizer::topn_proof::remap_sort_items_through_project(
                &topn.items,
                &project.items,
            )
        else {
            continue;
        };
        let pushed_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: remapped_items,
                limit: topn.limit,
                offset: topn.offset,
                phase: topn.phase,
                is_split: topn.is_split,
            }),
            children: project_expr.children.clone(),
        });
        out.push(NewExpr {
            op: Operator::LogicalProject(project.clone()),
            children: vec![pushed_group],
        });
    }
    out
}
```

- [x] **Step 2: Add positive Project pushdown test**

Add this test to the existing `tests` module:

```rust
#[test]
fn project_pushdown_remaps_column_ref_sort_keys() {
    use crate::sql::analysis::ProjectItem;
    use crate::sql::optimizer::operator::LogicalProjectOp;

    let mut memo = Memo::new();
    let base = values_group(&mut memo);
    let project_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalProject(LogicalProjectOp {
            items: vec![
                ProjectItem {
                    expr: col(1),
                    output_name: "alias_id".to_string(),
                    output_column_id: ColumnId(10),
                },
                ProjectItem {
                    expr: col(2),
                    output_name: "alias_score".to_string(),
                    output_column_id: ColumnId(20),
                },
            ],
            output_qualifier: None,
        }),
        children: vec![base],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![SortItem {
                expr: col(20),
                asc: false,
                nulls_first: false,
            }],
            limit: Some(10),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![project_group],
    };

    let out = PushTopNThroughProject.apply(&topn, &mut memo);

    assert_eq!(out.len(), 1);
    assert!(matches!(out[0].op, Operator::LogicalProject(_)));
    let pushed_topn_group = out[0].children[0];
    let pushed_expr = memo.groups[pushed_topn_group]
        .logical_exprs
        .first()
        .expect("pushed group should contain TopN");
    let Operator::LogicalTopN(pushed_topn) = &pushed_expr.op else {
        panic!("expected pushed LogicalTopN, got {:?}", pushed_expr.op);
    };
    let pushed_key = crate::sql::optimizer::property::typed_expr_to_column_id(
        &pushed_topn.items[0].expr,
    );
    assert_eq!(pushed_key, Some(ColumnId(2)));
    assert_eq!(pushed_expr.children, vec![base]);
}
```

- [x] **Step 3: Register Project rule**

In `src/sql/optimizer/cascades_rules/mod.rs`, add:

```rust
        Box::new(topn_compactness::PushTopNThroughProject),
```

after the Sort elision rule.

- [x] **Step 4: Run focused tests**

Run:

```bash
cargo test --lib topn_compactness -- --nocapture
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): push TopN through pure projects"
```

### Task 6: Add Scan Pushdown Guard Scaffolding

**Files:**
- Modify: `src/sql/optimizer/topn_proof.rs`
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [x] **Step 1: Add scan capability helper**

Add to `topn_proof.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanTopNCapability {
    NoOrdering,
    OrderedTopK,
}

pub(crate) fn default_scan_topn_capability() -> ScanTopNCapability {
    ScanTopNCapability::NoOrdering
}
```

Add this test:

```rust
#[test]
fn default_scan_capability_does_not_claim_ordered_topk() {
    assert_eq!(default_scan_topn_capability(), ScanTopNCapability::NoOrdering);
}
```

- [x] **Step 2: Add PushTopNIntoScan rule that fails closed**

Add to `topn_compactness.rs`:

```rust
pub(crate) struct PushTopNIntoScan;

impl Rule for PushTopNIntoScan {
    fn name(&self) -> &str {
        "PushTopNIntoScan"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(topn) = &expr.op else {
            return Vec::new();
        };
        if TopNWindow::from_limit_offset(topn.limit, topn.offset).is_none() {
            return Vec::new();
        }
        if expr.children.len() != 1 {
            return Vec::new();
        }
        let Some(child_group) = memo.groups.get(expr.children[0]) else {
            return Vec::new();
        };
        let has_scan = child_group
            .logical_exprs
            .iter()
            .any(|child| matches!(child.op, Operator::LogicalScan(_)));
        if !has_scan {
            return Vec::new();
        }
        match crate::sql::optimizer::topn_proof::default_scan_topn_capability() {
            crate::sql::optimizer::topn_proof::ScanTopNCapability::NoOrdering => Vec::new(),
            crate::sql::optimizer::topn_proof::ScanTopNCapability::OrderedTopK => {
                vec![NewExpr {
                    op: expr.op.clone(),
                    children: expr.children.clone(),
                }]
            }
        }
    }
}
```

This intentionally produces no new candidate with current scan backends. It locks in the fail-closed capability boundary so a future backend can opt in explicitly.

- [x] **Step 3: Register Scan rule**

In `all_transformation_rules()` add:

```rust
        Box::new(topn_compactness::PushTopNIntoScan),
```

before Project pushdown.

- [x] **Step 4: Run tests**

Run:

```bash
cargo test --lib default_scan_capability_does_not_claim_ordered_topk
cargo test --lib topn_compactness -- --nocapture
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/topn_proof.rs src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): add scan TopN capability guard"
```

### Task 7: Add Join, Aggregate, And SetOp Pushdown Guards

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [x] **Step 1: Add guarded rule structs**

Add these rule structs with fail-closed implementations first:

```rust
macro_rules! guarded_topn_rule {
    ($name:ident) => {
        pub(crate) struct $name;

        impl Rule for $name {
            fn name(&self) -> &str {
                stringify!($name)
            }

            fn rule_type(&self) -> RuleType {
                RuleType::Transformation
            }

            fn matches(&self, op: &Operator) -> bool {
                matches!(op, Operator::LogicalTopN(_))
            }

            fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
                match stringify!($name) {
                    "PushTopNThroughJoin" => push_topn_through_join(expr, memo),
                    "PushTopNThroughAggregate" => push_topn_through_aggregate(expr, memo),
                    "PushTopNThroughSetOp" => push_topn_through_setop(expr, memo),
                    _ => Vec::new(),
                }
            }
        }
    };
}

guarded_topn_rule!(PushTopNThroughJoin);
guarded_topn_rule!(PushTopNThroughAggregate);
guarded_topn_rule!(PushTopNThroughSetOp);

fn push_topn_through_join(_expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
    Vec::new()
}

fn push_topn_through_aggregate(_expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
    Vec::new()
}

fn push_topn_through_setop(_expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
    Vec::new()
}
```

This creates stable rule names and disable hooks before adding positive cases.

- [x] **Step 2: Add false-case tests**

Add these tests to the existing `tests` module in `topn_compactness.rs`:

```rust
#[test]
fn join_pushdown_fails_closed_for_inner_join_without_multiplicity_proof() {
    use crate::sql::analysis::JoinKind;
    use crate::sql::optimizer::operator::LogicalJoinOp;

    let mut memo = Memo::new();
    let left = values_group(&mut memo);
    let right = values_group(&mut memo);
    let join_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalJoin(LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: None,
        }),
        children: vec![left, right],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![sort_item(1)],
            limit: Some(10),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![join_group],
    };

    assert!(PushTopNThroughJoin.apply(&topn, &mut memo).is_empty());
}

#[test]
fn aggregate_pushdown_fails_closed_for_aggregate_function_order() {
    use crate::sql::analysis::OutputColumn;
    use crate::sql::optimizer::operator::LogicalAggregateOp;

    let mut memo = Memo::new();
    let input = values_group(&mut memo);
    let aggregate_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalAggregate(LogicalAggregateOp::single(
            vec![],
            vec![],
            vec![OutputColumn {
                column_id: ColumnId(10),
                name: "total_score".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        )),
        children: vec![input],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![sort_item(10)],
            limit: Some(1),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![aggregate_group],
    };

    assert!(PushTopNThroughAggregate.apply(&topn, &mut memo).is_empty());
}

#[test]
fn setop_pushdown_fails_closed_for_union_distinct() {
    use crate::sql::optimizer::operator::LogicalUnionOp;

    let mut memo = Memo::new();
    let left = values_group(&mut memo);
    let right = values_group(&mut memo);
    let union_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalUnion(LogicalUnionOp {
            all: false,
            output_columns: vec![],
        }),
        children: vec![left, right],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![sort_item(1)],
            limit: Some(10),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![union_group],
    };

    assert!(PushTopNThroughSetOp.apply(&topn, &mut memo).is_empty());
}
```

- [x] **Step 3: Register guarded rules**

In `all_transformation_rules()` add:

```rust
        Box::new(topn_compactness::PushTopNThroughJoin),
        Box::new(topn_compactness::PushTopNThroughAggregate),
        Box::new(topn_compactness::PushTopNThroughSetOp),
```

- [x] **Step 4: Run false-case tests**

Run:

```bash
cargo test --lib topn_compactness -- --nocapture
```

Expected: PASS and no rule generates unsafe candidates.

- [x] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): add guarded TopN pushdown rules"
```

### Task 8: Add Positive UNION ALL Branch TopN Pruning Candidate

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs`

- [x] **Step 1: Implement UNION ALL branch candidate**

Replace `push_topn_through_setop` with:

```rust
fn push_topn_through_setop(expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return Vec::new();
    };
    if TopNWindow::from_limit_offset(topn.limit, topn.offset).is_none() || expr.children.len() != 1 {
        return Vec::new();
    }
    let Some(child_group) = memo.groups.get(expr.children[0]).cloned() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for child_expr in child_group.logical_exprs {
        let Operator::LogicalUnion(union) = &child_expr.op else {
            continue;
        };
        if !union.all || child_expr.children.is_empty() {
            continue;
        }
        let mut pushed_children = Vec::with_capacity(child_expr.children.len());
        for branch in &child_expr.children {
            let pushed = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalTopN(topn.clone()),
                children: vec![*branch],
            });
            pushed_children.push(pushed);
        }
        let union_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(union.clone()),
            children: pushed_children,
        });
        out.push(NewExpr {
            op: Operator::LogicalTopN(topn.clone()),
            children: vec![union_group],
        });
    }
    out
}
```

This is pruning-only: the final TopN is intentionally retained.

- [x] **Step 2: Add positive UNION ALL test**

Add this test to the existing `tests` module:

```rust
#[test]
fn setop_pushdown_adds_branch_topn_for_union_all_and_keeps_final_topn() {
    use crate::sql::optimizer::operator::LogicalUnionOp;

    let mut memo = Memo::new();
    let left = values_group(&mut memo);
    let right = values_group(&mut memo);
    let union_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalUnion(LogicalUnionOp {
            all: true,
            output_columns: vec![],
        }),
        children: vec![left, right],
    });
    let topn = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalTopN(LogicalTopNOp {
            items: vec![sort_item(1)],
            limit: Some(10),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![union_group],
    };

    let out = PushTopNThroughSetOp.apply(&topn, &mut memo);

    assert_eq!(out.len(), 1);
    assert!(matches!(out[0].op, Operator::LogicalTopN(_)));
    let pushed_union_group = out[0].children[0];
    let pushed_union = memo.groups[pushed_union_group]
        .logical_exprs
        .first()
        .expect("pushed union group should contain one logical expr");
    let Operator::LogicalUnion(union) = &pushed_union.op else {
        panic!("expected LogicalUnion under final TopN, got {:?}", pushed_union.op);
    };
    assert!(union.all);
    assert_eq!(pushed_union.children.len(), 2);
    for branch_group in &pushed_union.children {
        let branch_expr = memo.groups[*branch_group]
            .logical_exprs
            .first()
            .expect("branch group should contain pushed TopN");
        assert!(matches!(branch_expr.op, Operator::LogicalTopN(_)));
    }
}
```

- [x] **Step 3: Run tests**

Run:

```bash
cargo test --lib topn_compactness -- --nocapture
```

Expected: PASS.

- [x] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/cascades_rules/topn_compactness.rs
git commit -m "feat(optimizer): add UNION ALL TopN branch pruning"
```

### Task 9: Add Pushdown SQL Goldens

**Files:**
- Create: `sql-tests/optimizer/sql/topn_compactness_scan_project.sql`
- Create: `sql-tests/optimizer/sql/topn_compactness_join_aggregate_setop.sql`

- [x] **Step 1: Create scan/project SQL case**

Create `sql-tests/optimizer/sql/topn_compactness_scan_project.sql`:

```sql
-- @tags=optimizer,topn,compactness,project
DROP TABLE IF EXISTS ${case_db}.topn_compactness_project_t;
CREATE TABLE ${case_db}.topn_compactness_project_t (
  id INT,
  score INT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.topn_compactness_project_t VALUES
  (1, 10), (2, 20), (3, 15);

-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT alias_id, alias_score
FROM (
  SELECT id AS alias_id, score AS alias_score
  FROM ${case_db}.topn_compactness_project_t
) p
ORDER BY alias_score DESC, alias_id ASC
LIMIT 2;

SET disable_optimizer_rules = 'PushTopNIntoScan,PushTopNThroughProject';

-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT alias_id, alias_score
FROM (
  SELECT id AS alias_id, score AS alias_score
  FROM ${case_db}.topn_compactness_project_t
) p
ORDER BY alias_score DESC, alias_id ASC
LIMIT 2;

SET disable_optimizer_rules = '';
```

- [x] **Step 2: Create join/aggregate/set-op SQL case**

Create `sql-tests/optimizer/sql/topn_compactness_join_aggregate_setop.sql`:

```sql
-- @tags=optimizer,topn,compactness,join,aggregate,setop
-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT *
FROM (
  SELECT 1 AS id, 10 AS score
  UNION ALL SELECT 2 AS id, 20 AS score
  UNION ALL SELECT 3 AS id, 15 AS score
) u
ORDER BY score DESC, id ASC
LIMIT 2;

-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT id, SUM(score) AS total_score
FROM (
  SELECT 1 AS id, 10 AS score
  UNION ALL SELECT 1 AS id, 20 AS score
  UNION ALL SELECT 2 AS id, 15 AS score
) a
GROUP BY id
ORDER BY total_score DESC
LIMIT 1;

-- @skip_result_check=true
-- @explain_contains=HASH JOIN
-- @explain_contains=TOP-N
EXPLAIN VERBOSE
SELECT l.id, l.score, r.tag
FROM (
  SELECT 1 AS id, 10 AS score
  UNION ALL SELECT 2 AS id, 20 AS score
) l
JOIN (
  SELECT 1 AS id, 'a' AS tag
  UNION ALL SELECT 2 AS id, 'b' AS tag
) r
ON l.id = r.id
ORDER BY l.score DESC
LIMIT 1;
```

- [x] **Step 3: Record and verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only topn_compactness_scan_project,topn_compactness_join_aggregate_setop \
  --mode record

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only topn_compactness_scan_project,topn_compactness_join_aggregate_setop \
  --mode verify
```

Expected: verify PASS.

- [x] **Step 4: Commit**

```bash
git add sql-tests/optimizer/sql/topn_compactness_scan_project.sql sql-tests/optimizer/sql/topn_compactness_join_aggregate_setop.sql sql-tests/optimizer/result/topn_compactness_scan_project.result sql-tests/optimizer/result/topn_compactness_join_aggregate_setop.result
git commit -m "test(optimizer): add TopN pushdown guard goldens"
```

## Batch 3 - Property, Codegen, And Validation

### Task 10: Keep TopN Output Properties Conservative

**Files:**
- Modify: `src/sql/optimizer/derive/top_n.rs`
- Modify: `src/sql/optimizer/property.rs`

- [x] **Step 1: Add regression test for non-column TopN key**

In `derive/top_n.rs` tests, add:

```rust
#[test]
fn top_n_non_column_sort_key_does_not_claim_ordering() {
    use crate::sql::analysis::{ExprKind, LiteralValue, SortItem, TypedExpr};
    use arrow::datatypes::DataType;

    let op = PhysicalTopNOp {
        items: vec![SortItem {
            expr: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(1)),
                data_type: DataType::Int64,
                nullable: false,
            },
            asc: true,
            nulls_first: false,
        }],
        limit: Some(10),
        offset: Some(0),
        phase: TopNPhase::Final,
        is_split: false,
    };

    let out = op.derive_output(&[]);

    assert_eq!(out.ordering, OrderingSpec::Any);
    assert_eq!(out.distribution, DistributionSpec::Gather);
}
```

- [x] **Step 2: Run test**

Run:

```bash
cargo test --lib top_n_non_column_sort_key_does_not_claim_ordering
```

Expected: PASS with current or minimally adjusted implementation.

- [x] **Step 3: Add ordering proof helper test in property module**

In `property.rs` tests, add:

```rust
#[test]
fn ordering_prefix_stays_strict_without_equivalence_helper() {
    let provided = OrderingSpec::Required(vec![SortKey {
        column: ColumnId(1),
        asc: true,
        nulls_first: false,
    }]);
    let required = OrderingSpec::Required(vec![SortKey {
        column: ColumnId(2),
        asc: true,
        nulls_first: false,
    }]);

    assert!(!provided.satisfies(&required));
}
```

This preserves the existing strict `satisfies` contract; equivalence-aware decisions stay in `topn_proof`.

- [x] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/derive/top_n.rs src/sql/optimizer/property.rs
git commit -m "test(optimizer): lock conservative TopN ordering properties"
```

### Task 11: Validate Split TopN Visible Shape Against Plan-Diff Baseline

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/explain.rs`
- Modify: `sql-tests/optimizer/sql/topn_compactness_merge.sql`

- [x] **Step 1: Inspect current q41/q72 shapes**

Run:

```bash
rg -n "TOP-N|SORT|MERGING-EXCHANGE|EXCHANGE" \
  logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q41.out \
  logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q72.out
```

Expected current baseline includes consecutive `TOP-N` lines in NR q41/q72.

- [x] **Step 2: Add a Rust regression only if physical extraction still emits duplicate global TopN**

If the earlier rule changes still leave a duplicate global `PhysicalTopN` in extracted plans, add a focused unit test near existing TopN/fragment builder tests in `fragment_builder.rs`. The test should build `PhysicalTopN(Final, is_split=true)` over `PhysicalTopN(Partial)` and assert the generated root plan does not contain two global TopN labels. Use existing helper patterns around TopN tests in `fragment_builder.rs`; do not rewrite fragment builder setup.

- [x] **Step 3: Make the minimal codegen/explain adjustment**

If the failure is only explain labeling, adjust `src/sql/explain.rs` to distinguish local partial pre-sort from global `TOP-N`. If the failure is physical tree shape, adjust extraction/rule generation first instead of hiding it in EXPLAIN.

- [x] **Step 4: Run focused checks**

Run:

```bash
cargo test --lib fragment_builder -- --nocapture
cargo test --lib explain -- --nocapture
```

Expected: PASS.

- [x] **Step 5: Commit if code changed**

If this task changed code, commit:

```bash
git add src/sql/codegen/fragment_builder.rs src/sql/explain.rs sql-tests/optimizer/sql/topn_compactness_merge.sql sql-tests/optimizer/result/topn_compactness_merge.result
git commit -m "fix(optimizer): compact split TopN visible shape"
```

If inspection proves no code change is needed, leave no commit for this task and record the evidence in the final validation summary.

### Task 12: Full Verification And Plan-Quality Check

**Files:**
- No source edits expected.
- The external roadmap note is not modified by this plan.

- [x] **Step 1: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [x] **Step 2: Run Rust focused tests**

Run:

```bash
cargo test --lib topn_proof -- --nocapture
cargo test --lib topn_compactness -- --nocapture
cargo test --lib top_n_non_column_sort_key_does_not_claim_ordering
```

Expected: PASS.

- [x] **Step 3: Build**

Run:

```bash
cargo build
```

Expected: PASS.

- [x] **Step 4: Run order-sensitive SQL verify**

Run with standalone server running:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite sort --only topn_order_limit,topn_null_order_limit_offset \
  --mode verify
```

Expected: PASS.

- [x] **Step 5: Run optimizer TopN compactness verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only topn_compactness_merge,topn_compactness_sort_elision,topn_compactness_scan_project,topn_compactness_join_aggregate_setop,window_ordering_reuses_child_sort \
  --mode verify
```

Expected: PASS.

- [x] **Step 6: Compare plan-quality baseline snippets**

Run:

```bash
rg -n "TOP-N|SORT BY|GATHER EXCHANGE" \
  logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q41.out \
  logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-ds__q72.out \
  logs/plan-quality/20260609-fe-nr-plan-diff/nr/tpc-h__q22.out
```

Expected after regenerating plan-quality outputs in a follow-up validation run:
- q41/q72 no longer show consecutive equivalent global `TOP-N`.
- q22 does not gain extra Sort/Gather.

- [x] **Step 7: Final commit for validation artifacts if any were recorded**

If any result files changed during verification:

```bash
git add sql-tests/optimizer/result sql-tests/sort/result
git commit -m "test(optimizer): refresh TopN compactness results"
```

If no files changed, do not create an empty commit.
