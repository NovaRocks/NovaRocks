# Arc A TypedExpr Final Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make optimizer-internal production logic stop inspecting analyzer/planner `TypedExpr` / `LogicalPlanNode`, leaving only explicit bridge and outbound codegen/planner boundaries.

**Architecture:** Keep `ScalarArena` / `ScalarId` as the optimizer expression authority. First add scalar-native traversal/construction helpers and an audit gate, then migrate remaining rules from narrow to broad: `ukfk`, variant-path, predicate pushdown, aggregate/LC/ranking-window, CBO implementation/split rules, MV rewrite, subquery, and IMV. `scalar.rs`, `scalar_bridge.rs`, `convert.rs`, and planner/codegen callers remain explicit boundaries unless the final strict-zero task moves a whole subsystem out of `src/sql/optimizer`.

**Tech Stack:** Rust, `ScalarArena`, `ScalarNode`, `OptExpr`, Cascades memo, `cargo test --lib`, optimizer SQL golden tests.

---

## Current State

Already completed on `claude/arc-a-typedexpr-finish`:

- `derive_join_not_null.rs`: production path is ScalarId-native.
- `required_columns.rs`: production required-column collection is ScalarId-native.
- `topn_proof.rs` and `cascades_rules/topn_compactness.rs`: TopN proof/remap logic is ScalarId-native.
- A3/#340 is merged into `origin/main`; optimizer entry already takes `OptExpr + ScalarArena`.

Remaining production clusters, excluding test modules and comments:

- Explicit boundaries: `scalar.rs`, `scalar_bridge.rs`, `convert.rs`, `property.rs`.
- CBO implementation/split rules: `cascades_rules/implement.rs`, `split_aggregate.rs`, `split_distinct_agg.rs`.
- MV rewrite: `cascades_rules/mv_rewrite/*`.
- RBO rewrite: `aggregate_pushdown/*`, `low_cardinality_dict/*`, `predicate_pushdown/*`, `ranking_window_predicate_pushdown/rule.rs`, `ukfk.rs`, `variant_path_pushdown/rule.rs`, `rewrite/rules/utils.rs`.
- Legacy bridges/subsystems: `rewrite/rules/subquery/*`, `rewrite/imv/*`.

Non-goals for this cleanup:

- Do not remove `scalar.rs::materialize` or `scalar_bridge.rs`; they are bridge APIs.
- Do not remove codegen/planner outbound materialization.
- Do not change SQL semantics or optimizer rule enablement behavior.

## File Structure

Create:

- `tools/dev/audit_optimizer_typedexpr.py`
  - One repeatable audit command for production-code residuals.

Modify:

- `src/sql/optimizer/mod.rs`
  - Add the new scalar utility module.
- `src/sql/optimizer/scalar_expr.rs`
  - New scalar-native traversal/construction utilities used by rewrite/CBO rules.
- `src/sql/optimizer/rewrite/rules/ukfk.rs`
  - First narrow rule migration with direct tests.
- `src/sql/optimizer/rewrite/rules/variant_path_pushdown/rule.rs`
  - Replace materialize/mutate/reintern pattern with scalar rewrite.
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/*`
  - Replace `TypedExpr` predicate grouping, splitting, deriving, and pushdown with `ScalarId`.
- `src/sql/optimizer/rewrite/rules/aggregate_pushdown/*`
  - Replace typed expression collectors and expression identity with `ScalarId`.
- `src/sql/optimizer/rewrite/rules/low_cardinality_dict/*`
  - Replace typed walkers and project/sort rewrites with scalar walkers.
- `src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs`
  - Replace typed test/production expression checks with scalar helpers.
- `src/sql/optimizer/cascades_rules/implement.rs`
  - Extract hash-join equi/residual predicates from ScalarId directly.
- `src/sql/optimizer/cascades_rules/split_aggregate.rs`
- `src/sql/optimizer/cascades_rules/split_distinct_agg.rs`
  - Replace typed group-by/aggregate structural checks with ScalarId-based checks.
- `src/sql/optimizer/cascades_rules/mv_rewrite/*`
  - Add scalar descriptor/normalization path and keep typed descriptor only as bridge fallback until deleted.
- `src/sql/optimizer/rewrite/rules/subquery/*`
  - Convert subquery rules from local `LogicalPlanNode` bridge to `OptExpr` rules.
- `src/sql/optimizer/rewrite/imv/*`
  - Convert IMV rules to `OptExpr` or move the whole LogicalPlanNode-shaped IMV rewrite outside `src/sql/optimizer`.

## Task 1: Add Repeatable Audit Gate

**Files:**
- Create: `tools/dev/audit_optimizer_typedexpr.py`

- [ ] **Step 1: Create the audit script**

```python
#!/usr/bin/env python3
from pathlib import Path
import argparse
import re
import sys

DEFAULT_ALLOW = {
    "src/sql/optimizer/scalar.rs",
    "src/sql/optimizer/scalar_bridge.rs",
    "src/sql/optimizer/convert.rs",
}

PATTERN = re.compile(r"\\b(TypedExpr|LogicalPlanNode|ProjectItem|SortItem|materialize)\\b")

def production_hits(path: Path):
    in_test = False
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        if "#[cfg(test)]" in line:
            in_test = True
        if in_test:
            continue
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("//!") or stripped.startswith("///"):
            continue
        if PATTERN.search(line):
            yield lineno, line.rstrip()

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--allow", action="append", default=[])
    args = parser.parse_args()
    allowed = set(DEFAULT_ALLOW)
    allowed.update(args.allow)
    failed = False
    for path in sorted(Path("src/sql/optimizer").rglob("*.rs")):
        rel = path.as_posix()
        hits = list(production_hits(path))
        if not hits:
            continue
        if rel in allowed:
            continue
        failed = True
        print(rel)
        for lineno, line in hits:
            print(f"  {lineno}: {line}")
    return 1 if failed and args.strict else 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the audit in report mode**

Run:

```bash
python3 tools/dev/audit_optimizer_typedexpr.py
```

Expected: prints the current residual list and exits `0`.

- [ ] **Step 3: Run strict mode to verify it fails before migration**

Run:

```bash
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: exits `1` and lists non-allowlisted optimizer files.

- [ ] **Step 4: Commit**

```bash
git add tools/dev/audit_optimizer_typedexpr.py
git commit -m "chore(optimizer): add TypedExpr residual audit"
```

## Task 2: Add Scalar-Native Expression Utilities

**Files:**
- Create: `src/sql/optimizer/scalar_expr.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Test: `src/sql/optimizer/scalar_expr.rs`

- [ ] **Step 1: Add module export**

In `src/sql/optimizer/mod.rs`, add:

```rust
pub(crate) mod scalar_expr;
```

- [ ] **Step 2: Add utility module**

Create `src/sql/optimizer/scalar_expr.rs`:

```rust
use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::{
    HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey,
};

pub(crate) fn column_id(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) if *id != ColumnId::UNSET => Some(*id),
        _ => None,
    }
}

pub(crate) fn collect_column_ids_strict(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<HashSet<ColumnId>> {
    let mut out = HashSet::new();
    collect_column_ids_strict_inner(arena, expr, &mut out)?;
    Some(out)
}

fn collect_column_ids_strict_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<ColumnId>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) => {
            if *id == ColumnId::UNSET {
                return None;
            }
            out.insert(*id);
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_ids_strict_inner(arena, *left, out)?;
            collect_column_ids_strict_inner(arena, *right, out)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_column_ids_strict_inner(arena, *child, out)?,
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_column_ids_strict_inner(arena, *body, out)?;
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            for item in list {
                collect_column_ids_strict_inner(arena, *item, out)?;
            }
        }
        ScalarNode::Between { child, low, high, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *low, out)?;
            collect_column_ids_strict_inner(arena, *high, out)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *pattern, out)?;
        }
        ScalarNode::Case { operand, when_then, else_expr } => {
            if let Some(operand) = operand {
                collect_column_ids_strict_inner(arena, *operand, out)?;
            }
            for (when, then) in when_then {
                collect_column_ids_strict_inner(arena, *when, out)?;
                collect_column_ids_strict_inner(arena, *then, out)?;
            }
            if let Some(else_expr) = else_expr {
                collect_column_ids_strict_inner(arena, *else_expr, out)?;
            }
        }
        ScalarNode::WindowCall { args, partition_by, order_by, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for expr in partition_by {
                collect_column_ids_strict_inner(arena, *expr, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
    }
    Some(())
}

pub(crate) fn split_conjuncts(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp { op: BinOp::And, left, right } => {
            split_conjuncts(arena, *left, out);
            split_conjuncts(arena, *right, out);
        }
        _ => out.push(expr),
    }
}

pub(crate) fn combine_conjuncts(arena: &mut ScalarArena, mut exprs: Vec<ScalarId>) -> Option<ScalarId> {
    let mut result = exprs.pop()?;
    while let Some(next) = exprs.pop() {
        result = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: next,
                right: result,
            },
            DataType::Boolean,
            false,
        );
    }
    Some(result)
}

pub(crate) fn bool_literal(arena: &mut ScalarArena, value: bool) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        DataType::Boolean,
        false,
    )
}

pub(crate) fn int_literal(arena: &mut ScalarArena, value: i64) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        DataType::Int64,
        false,
    )
}

pub(crate) fn is_literal_count_arg(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(_)))
            | ScalarNode::Literal(HashableLiteral(LiteralValue::Null))
    )
}

pub(crate) fn contains_aggregate(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::AggregateCall { .. } => true,
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_aggregate(arena, *left) || contains_aggregate(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_aggregate(arena, *child),
        ScalarNode::FunctionCall { args, .. } => args.iter().any(|arg| contains_aggregate(arena, *arg)),
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_aggregate(arena, *body)
        }
        ScalarNode::InList { child, list, .. } => {
            contains_aggregate(arena, *child) || list.iter().any(|item| contains_aggregate(arena, *item))
        }
        ScalarNode::Between { child, low, high, .. } => {
            contains_aggregate(arena, *child)
                || contains_aggregate(arena, *low)
                || contains_aggregate(arena, *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_aggregate(arena, *child) || contains_aggregate(arena, *pattern)
        }
        ScalarNode::Case { operand, when_then, else_expr } => {
            operand.is_some_and(|expr| contains_aggregate(arena, expr))
                || when_then.iter().any(|(when, then)| {
                    contains_aggregate(arena, *when) || contains_aggregate(arena, *then)
                })
                || else_expr.is_some_and(|expr| contains_aggregate(arena, expr))
        }
        ScalarNode::WindowCall { args, partition_by, order_by, .. } => {
            args.iter().any(|arg| contains_aggregate(arena, *arg))
                || partition_by.iter().any(|expr| contains_aggregate(arena, *expr))
                || order_by.iter().any(|item| contains_aggregate(arena, item.expr))
        }
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_) => false,
    }
}

pub(crate) fn sort_key_column_id(arena: &ScalarArena, key: &SortKey) -> Option<ColumnId> {
    column_id(arena, key.expr)
}
```

- [ ] **Step 3: Add targeted unit tests**

Add at the bottom of `scalar_expr.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use crate::sql::analysis::BinOp;

    fn col(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(ScalarNode::ColumnRef(ColumnId(id)), DataType::Int64, true)
    }

    #[test]
    fn strict_column_collection_rejects_unset_column_ref() {
        let mut arena = ScalarArena::new();
        let expr = arena.intern(ScalarNode::ColumnRef(ColumnId::UNSET), DataType::Int64, true);
        assert_eq!(collect_column_ids_strict(&arena, expr), None);
    }

    #[test]
    fn split_and_combine_conjuncts_round_trip_column_refs() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let and = arena.intern(
            ScalarNode::BinaryOp { op: BinOp::And, left: a, right: b },
            DataType::Boolean,
            false,
        );
        let mut parts = Vec::new();
        split_conjuncts(&arena, and, &mut parts);
        assert_eq!(parts.len(), 2);
        let rebuilt = combine_conjuncts(&mut arena, parts).unwrap();
        assert!(matches!(arena.node(rebuilt), ScalarNode::BinaryOp { op: BinOp::And, .. }));
    }
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test --lib scalar_expr
```

Expected: all `scalar_expr` tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/mod.rs src/sql/optimizer/scalar_expr.rs
git commit -m "refactor(optimizer): add scalar expression utilities"
```

## Task 3: Migrate `ukfk.rs` with Direct Tests

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/ukfk.rs`

- [ ] **Step 1: Add failing tests for scalar-native helper behavior**

Add `#[cfg(test)] mod tests` in `ukfk.rs` covering:

```rust
#[test]
fn project_referenced_side_rejects_cross_side_scalar_refs() { /* build ScalarArena + left/right OptExpr outputs */ }

#[test]
fn join_equality_pairs_accepts_nested_or_cast_column_refs() { /* condition: left.a = CAST(right.b AS BIGINT) */ }

#[test]
fn eliminable_count_accepts_literal_count_args_without_materializing() { /* count(1) and count(NULL) */ }
```

Expected first run:

```bash
cargo test --lib ukfk
```

Expected: tests compile and fail because helpers still materialize or helper signatures have not changed.

- [ ] **Step 2: Replace project-side reference collection**

Replace:

```rust
let item_expr = scalar::materialize(arena, item.expr);
let ids = match collect_column_id_refs_strict(&item_expr) {
```

with:

```rust
let ids = match crate::sql::optimizer::scalar_expr::collect_column_ids_strict(arena, item.expr) {
```

- [ ] **Step 3: Replace join equality extraction**

Change `collect_join_equality_pairs` and `classify_column_ref` to take `ScalarId` plus `&ScalarArena`:

```rust
fn collect_join_equality_pairs(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    pairs: &mut Vec<(String, String)>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op: BinOp::And, right } => {
            collect_join_equality_pairs(arena, *left, left_ids, right_ids, pairs)?;
            collect_join_equality_pairs(arena, *right, left_ids, right_ids, pairs)
        }
        ScalarNode::BinaryOp { left, op: BinOp::Eq, right } => {
            let left_ref = classify_column_ref(arena, *left, left_ids, right_ids)?;
            let right_ref = classify_column_ref(arena, *right, left_ids, right_ids)?;
            match (left_ref, right_ref) {
                ((Side::Left, left_col), (Side::Right, right_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                ((Side::Right, right_col), (Side::Left, left_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                _ => None,
            }
        }
        _ => None,
    }
}
```

`classify_column_ref` should unwrap `ScalarNode::ColumnRef`, `ScalarNode::Cast`, and `ScalarNode::Nested`. Column names must come from `ScanOp.columns` or `ScalarArena` display metadata; if display metadata is unavailable, return `None`, not a guessed name.

- [ ] **Step 4: Replace group-by and count checks**

Use `scalar_expr::column_id`, `ScanOp.columns`, and `scalar_expr::is_literal_count_arg`.

- [ ] **Step 5: Keep aggregate project rewrite as a second sub-step**

For `rewrite_eliminated_aggregate_project_item`, replace materialize/reintern with direct scalar rewrite:

```rust
fn rewrite_eliminated_aggregate_expr(arena: &mut ScalarArena, expr: ScalarId) -> Option<ScalarId> {
    match arena.node(expr).clone() {
        ScalarNode::AggregateCall { name, distinct, order_by, .. }
            if name.eq_ignore_ascii_case("count") && !distinct && order_by.is_empty() =>
        {
            Some(crate::sql::optimizer::scalar_expr::int_literal(arena, 1))
        }
        _ if !crate::sql::optimizer::scalar_expr::contains_aggregate(arena, expr) => Some(expr),
        _ => None,
    }
}
```

- [ ] **Step 6: Verify**

Run:

```bash
cargo fmt -- --check
cargo test --lib ukfk
cargo build --lib
```

Expected: `ukfk` tests pass and build exits `0`.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/ukfk.rs
git commit -m "refactor(optimizer): keep UKFK rewrites ScalarId-native"
```

## Task 4: Migrate Variant Path Pushdown

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/variant_path_pushdown/rule.rs`

- [ ] **Step 1: Add tests before rewriting**

Add tests for:

- filter predicate variant request becomes scan `variant_columns` plus a slot ref;
- project expression variant request becomes scan `variant_columns`;
- unsupported/non-variant expression stays unchanged;
- repeated same variant path reuses one generated variant column.

Run:

```bash
cargo test --lib variant_path_pushdown
```

Expected before migration: current behavior passes; after migration it must remain identical.

- [ ] **Step 2: Add scalar rewrite helpers inside the file**

Use this shape:

```rust
fn rewrite_variant_request_scalar(
    arena: &mut ScalarArena,
    expr: ScalarId,
    bindings: &mut VariantBindings,
) -> Result<Option<ScalarId>, String> {
    match arena.node(expr).clone() {
        ScalarNode::FunctionCall { name, args, distinct } => {
            if let Some(request) = variant_request_scalar(arena, &name, &args)? {
                return Ok(Some(bindings.column_ref_for(arena, request)?));
            }
            let mut changed = false;
            let mut new_args = Vec::with_capacity(args.len());
            for arg in args {
                match rewrite_variant_request_scalar(arena, arg, bindings)? {
                    Some(new_arg) => {
                        changed = true;
                        new_args.push(new_arg);
                    }
                    None => new_args.push(arg),
                }
            }
            if changed {
                return Ok(Some(arena.intern(
                    ScalarNode::FunctionCall { name, args: new_args, distinct },
                    arena.data_type(expr).clone(),
                    arena.nullable(expr),
                )));
            }
            Ok(None)
        }
        _ => rewrite_scalar_children(arena, expr, bindings),
    }
}
```

- [ ] **Step 3: Replace all production `scalar::materialize` calls**

Targets include current materialize calls around filter/project/sort/aggregate/window expressions. The replacement must return `ScalarId` directly and only create `TypedExpr` in tests or bridge code.

- [ ] **Step 4: Verify**

```bash
cargo fmt -- --check
cargo test --lib variant_path_pushdown
cargo build --lib
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: audit still fails, but `variant_path_pushdown/rule.rs` is no longer listed.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/variant_path_pushdown/rule.rs
git commit -m "refactor(optimizer): keep variant path pushdown ScalarId-native"
```

## Task 5: Migrate Predicate Pushdown Core

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/{predicate_group.rs,deriver.rs,push_to_join.rs,join_pushdown.rs,move_around.rs,push_through_project.rs,push_to_scan.rs,push_to_aggregate.rs,semi_anti_condition.rs}`
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`

- [ ] **Step 1: Convert `PredicateGroup` to ScalarId**

Change predicate storage from `TypedExpr` to `ScalarId`:

```rust
pub(crate) struct PredicateGroup {
    pub(crate) predicates: Vec<ScalarId>,
    pub(crate) origin: PredicateOrigin,
    pub(crate) derived: PredicateDerivedKind,
}
```

- [ ] **Step 2: Replace split/combine helpers**

Replace typed `split_and`, `combine_and`, `combine_or` production uses with scalar equivalents. Keep typed helpers only if they are still required by bridge files and document them as boundary-only.

- [ ] **Step 3: Migrate join classification**

Every predicate side classification must use `scalar_expr::collect_column_ids_strict(arena, id)` and output sets from `OptExpr`.

- [ ] **Step 4: Migrate project remap**

Use scalar substitution:

```rust
fn remap_predicate_through_project(
    arena: &mut ScalarArena,
    predicate: ScalarId,
    project_items: &[ScalarProjectItem],
) -> Option<ScalarId>
```

It must replace only column refs that match project output ids and must return `None` if a required project item is computed and cannot be pushed.

- [ ] **Step 5: Verify**

Run focused tests:

```bash
cargo test --lib predicate_pushdown
cargo test --lib push_to_join
cargo test --lib push_through_project
cargo build --lib
```

Expected: all existing predicate pushdown tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/predicate_pushdown src/sql/optimizer/rewrite/rules/utils.rs
git commit -m "refactor(optimizer): keep predicate pushdown ScalarId-native"
```

## Task 6: Migrate Aggregate Pushdown, Low-Cardinality Dict, Ranking Window

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/*`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/*`
- Modify: `src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs`

- [ ] **Step 1: Aggregate pushdown**

Replace typed collectors with scalar collectors:

```rust
fn aggregate_args_column_ids(arena: &ScalarArena, aggregate: &ScalarAggregateSpec) -> Option<HashSet<ColumnId>>
```

Run:

```bash
cargo test --lib aggregate_pushdown
```

- [ ] **Step 2: Low-cardinality dict**

Replace typed rewrites with scalar rewrites that preserve `ScalarId` metadata and display metadata. Verify:

```bash
cargo test --lib low_cardinality_dict
```

- [ ] **Step 3: Ranking window**

Use scalar column-id and window sort-key helpers. Verify:

```bash
cargo test --lib ranking_window_predicate_pushdown
```

- [ ] **Step 4: Build and commit**

```bash
cargo build --lib
git add src/sql/optimizer/rewrite/rules/aggregate_pushdown \
        src/sql/optimizer/rewrite/rules/low_cardinality_dict \
        src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs
git commit -m "refactor(optimizer): finish scalar-native structural rewrites"
```

## Task 7: Migrate CBO Implementation and Aggregate Split Rules

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/cascades_rules/split_aggregate.rs`
- Modify: `src/sql/optimizer/cascades_rules/split_distinct_agg.rs`

- [ ] **Step 1: `implement.rs` hash-join extraction**

Replace `TypedHashJoinEqCondition` with scalar form:

```rust
struct ScalarHashJoinEqCondition {
    left: ScalarId,
    right: ScalarId,
    null_safe_equal: bool,
}
```

All residual split/combine logic should use scalar conjunct helpers.

- [ ] **Step 2: Split aggregate**

Use `ScalarId` structural identity for group-by and aggregate arguments. If two expressions are structurally identical, they already intern to the same `ScalarId`.

- [ ] **Step 3: Split distinct aggregate**

Replace `typed_exprs_structurally_equal` with `ScalarId` equality, and create phase output refs with `ScalarNode::ColumnRef`.

- [ ] **Step 4: Verify**

```bash
cargo test --lib implement
cargo test --lib split_aggregate
cargo test --lib split_distinct_agg
cargo build --lib
```

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/implement.rs \
        src/sql/optimizer/cascades_rules/split_aggregate.rs \
        src/sql/optimizer/cascades_rules/split_distinct_agg.rs
git commit -m "refactor(optimizer): keep CBO implementation rules ScalarId-native"
```

## Task 8: Migrate MV Rewrite Descriptors

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/{aggregate_rollup,column_mapping,descriptor,predicate_split,rule}.rs`

- [ ] **Step 1: Add scalar descriptor type**

In `descriptor.rs`, add:

```rust
pub(crate) enum ScalarSpjgOutputExpr {
    Dimension(ScalarId),
    Aggregate(ScalarAggregateSpec),
}

pub(crate) struct ScalarSpjgDescriptor {
    pub group_by: Vec<ScalarId>,
    pub predicates: Vec<ScalarId>,
    pub outputs: Vec<ScalarSpjgOutputExpr>,
}
```

- [ ] **Step 2: Move normalization to ScalarId**

`column_mapping.rs` should normalize directly from `ScalarNode`. The normalized form must include function/operator names, literal values, sort-key flags, data type, and nullability where the existing typed path used them.

- [ ] **Step 3: Replace rule materialization**

In `rule.rs`, remove `materialize_exprs`, `materialize_aggregate_calls`, and `ProjectItem` construction from production paths. Construct `ScalarProjectItem`, `ScalarAggregateSpec`, and `ScalarNode::ColumnRef` directly.

- [ ] **Step 4: Verify**

```bash
cargo test --lib mv_rewrite
cargo build --lib
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: strict audit still fails if subquery/IMV/predicate tasks remain, but `cascades_rules/mv_rewrite/*` is no longer listed.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/mv_rewrite
git commit -m "refactor(optimizer): keep MV rewrite descriptors ScalarId-native"
```

## Task 9: Retire Subquery Local LogicalPlan Bridge

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/*`

- [ ] **Step 1: Keep `bridge.rs` as the only initial allowlisted subquery file**

Add an explicit allow entry for `src/sql/optimizer/rewrite/rules/subquery/bridge.rs` in the audit script while migrating the other files. Do not allowlist the whole subquery directory.

- [ ] **Step 2: Convert leaf rules first**

Migrate in this order:

1. `existential_apply_to_join.rs`
2. `quantified_apply_to_join.rs`
3. `push_down_apply_filter.rs`
4. `push_down_apply_agg_filter.rs`
5. `scalar_apply_to_join.rs`
6. `apply_to_window.rs`
7. helper modules

Each converted rule must implement directly over `OptExpr` and `ScalarId`, with no `opt_expr_to_plan` call.

- [ ] **Step 3: Delete or shrink `bridge.rs`**

When no rule imports `subquery::bridge::opt_expr_to_plan`, delete the reverse materialization path. Keep only `plan_to_opt_expr` if a parser/planner boundary still needs it; otherwise delete the file.

- [ ] **Step 4: Verify**

```bash
cargo test --lib subquery
cargo test --lib sql::optimizer
cargo build --lib
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: subquery files are not listed except a deliberately allowlisted bridge, if one still exists.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/subquery tools/dev/audit_optimizer_typedexpr.py
git commit -m "refactor(optimizer): retire subquery TypedExpr bridge"
```

## Task 10: Retire or Relocate IMV LogicalPlanNode Rewrite

**Files:**
- Modify or move: `src/sql/optimizer/rewrite/imv/*`

- [ ] **Step 1: Choose one boundary**

Use exactly one of these approaches:

- Preferred: convert IMV rewrite modules to `OptExpr` and `ScalarId`.
- Alternative: move IMV `LogicalPlanNode` rewrite modules out of `src/sql/optimizer` into a planner-stage namespace, for example `src/sql/planner/imv_rewrite/*`.

Do not leave LogicalPlanNode-shaped IMV production code in `src/sql/optimizer` if strict TypedExpr-out-of-optimizer is the goal.

- [ ] **Step 2: Convert validation helpers**

Replace signatures like:

```rust
fn validate(plan: &LogicalPlanNode) -> Result<(), String>
```

with:

```rust
fn validate(plan: &OptExpr, arena: &ScalarArena) -> Result<(), String>
```

- [ ] **Step 3: Convert ProjectItem construction**

Create `ScalarProjectItem` directly:

```rust
ScalarProjectItem {
    expr: arena.intern(ScalarNode::ColumnRef(column_id), data_type, nullable),
    output_name,
    output_column_id,
    expr_display: None,
}
```

- [ ] **Step 4: Verify**

```bash
cargo test --lib imv
cargo test --lib mv
cargo build --lib
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: IMV files are no longer listed in optimizer audit.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv src/sql/planner
git commit -m "refactor(optimizer): remove IMV LogicalPlanNode dependency"
```

## Task 11: Final Audit, SQL Golden, and PR

**Files:**
- Modify: `tools/dev/audit_optimizer_typedexpr.py`
- Modify: docs or PR body only if results need recording.

- [ ] **Step 1: Shrink allowlist**

The final allowlist should contain only:

```python
DEFAULT_ALLOW = {
    "src/sql/optimizer/scalar.rs",
    "src/sql/optimizer/scalar_bridge.rs",
    "src/sql/optimizer/convert.rs",
    "src/sql/optimizer/property.rs",
}
```

If `convert.rs` or `property.rs` is still listed, add comments in the script explaining their exact bridge role. Do not allowlist rule directories.

- [ ] **Step 2: Run Rust verification**

```bash
cargo fmt -- --check
git diff --check
cargo build --lib
cargo test --lib
python3 tools/dev/audit_optimizer_typedexpr.py --strict
```

Expected: all commands exit `0`.

- [ ] **Step 3: Run optimizer SQL golden**

Use the generated runtime config when available:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: optimizer suite passes.

- [ ] **Step 4: Run TPC-DS smoke or full suite according to time budget**

Minimum:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q10,q35,q69 --mode verify --query-timeout 120
```

Full acceptance:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --mode verify --query-timeout 120 -j 1
```

- [ ] **Step 5: Final commit**

```bash
git add tools/dev/audit_optimizer_typedexpr.py docs/superpowers/plans/2026-06-20-arc-a-typedexpr-final-cleanup.md
git commit -m "test(optimizer): enforce TypedExpr cleanup audit"
```

- [ ] **Step 6: PR update**

Push current branch and open/update PR with:

- summary of migrated clusters;
- final allowlist;
- Rust verification output;
- SQL golden output;
- remaining deliberately-out-of-scope boundaries, if any.

## Execution Order

Recommended order:

1. Task 1 audit gate.
2. Task 2 scalar utilities.
3. Task 3 `ukfk.rs`.
4. Task 4 variant path pushdown.
5. Task 5 predicate pushdown.
6. Task 6 aggregate/LC/ranking-window RBO cleanup.
7. Task 7 CBO implementation/split rules.
8. Task 8 MV rewrite.
9. Task 9 subquery bridge retirement.
10. Task 10 IMV migration or relocation.
11. Task 11 final audit and SQL verification.

Commit after every task. Do not batch subquery/IMV with unrelated rules.

## Self-Review

**Spec coverage:** This covers the current residual list from `src/sql/optimizer` production code: narrow rules, CBO rules, MV rewrite, subquery bridge, IMV LogicalPlanNode modules, and explicit bridge allowlist.

**Placeholder scan:** No task says "TBD" or "write tests" without concrete test names and commands. Large subsystem tasks provide exact migration order and required replacement signatures.

**Type consistency:** New helpers use `ScalarArena`, `ScalarId`, `ScalarNode`, and existing `ScalarProjectItem` / `ScalarAggregateSpec` types. Final audit allowlist keeps only bridge modules.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-20-arc-a-typedexpr-final-cleanup.md`.

Two execution options:

1. Subagent-Driven (recommended) - dispatch a fresh subagent per task, review between tasks, fast iteration.
2. Inline Execution - execute tasks in this session using executing-plans, batch execution with checkpoints.
