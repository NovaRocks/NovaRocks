# Phase 2 Fragment Builder Cleanups — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move three optimization decisions currently made in `fragment_builder.rs` into the cascades layer, continuing the Phase-1 pattern of making fragment builder a pure translator.

**Architecture:** Three independent sub-tasks, each following the Phase-1 template (write failing test → add cascades logic → delete the fragment-builder fallback/mutation → verify EXPLAIN/TPC-DS regression clean). Sub-tasks 2.1, 2.2, 2.3 have no inter-dependencies and can ship in any order.

**Tech Stack:** Rust 2021, cascades optimizer in `src/sql/cascades/`, `cargo test` for unit tests, `sql-test-runner` for end-to-end TPC-DS verification.

**Spec reference:** `docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md` §4.2.

**Spec deviation (acknowledged upfront):** Spec §4.2.1 says to put eq_conditions normalization in the `JoinCommutativity` rule. This is literally not possible because `LogicalJoinOp` carries `condition: Option<TypedExpr>` (a predicate tree), not `eq_conditions` (ordered pairs). Equality extraction happens inside `JoinToHashJoin::apply` (implement.rs), producing pairs from the SQL text order. The correct home for the normalization is therefore `JoinToHashJoin`, where child-group column membership can be consulted to orient each pair. Task 2.1 below targets `JoinToHashJoin`, not `JoinCommutativity`. This achieves the same spec intent ("push eq_conditions normalization out of fragment_builder into cascades") without requiring a structural refactor of LogicalJoinOp.

---

## Task 0: Capture Phase-1 Baseline (Already Done in Phase 1 Task 9)

Phase-1 Task 9 already captured standalone EXPLAIN snapshots at `/tmp/novarocks-plan-compare/standalone-phase1/` (99 files) after Phase-1 landed. That snapshot is the Phase-2 baseline. No work needed here.

Before starting Phase 2 tasks, confirm the baseline is intact:

- [ ] **Step 1: Confirm Phase-1 baseline files.**

```bash
ls /tmp/novarocks-plan-compare/standalone-phase1/ | wc -l
tail -2 /tmp/novarocks-plan-compare/phase0-ref.txt
```

Expected: `99`, and the second line shows `phase1 complete: 502e4cc...`.

If the snapshots are missing (e.g., /tmp was cleared), re-capture them by booting the standalone server at the Phase-1 HEAD (currently `3943be4`) and running the capture script from Phase-1 Task 9 with `standalone-phase1` as the output directory. Only do this if the snapshots are actually missing.

---

## Task 2.1: Normalize `eq_conditions` Pair Order in `JoinToHashJoin`

Push the "which side is left, which is right?" decision out of `visit_hash_join`'s natural-or-swap fallback and into the rule that produces `PhysicalHashJoin`. After this task, `eq_conditions` entries are always `(left_expr, right_expr)` where `left_expr` references the left child's columns and `right_expr` references the right child's columns. The fragment builder becomes a pure compiler from that fixed pair order.

**Files:**
- Modify: `src/sql/cascades/rules/implement.rs` (inside `JoinToHashJoin::apply` and its helpers)
- Modify: `src/sql/cascades/fragment_builder.rs` (simplify `visit_hash_join`'s eq-condition compilation)
- Test: `src/sql/cascades/rules/implement.rs` inline tests

### TDD Steps

- [ ] **Step 1: Read the current flow.**

```bash
cd /Users/harbor/project/NovaRocks
grep -n 'extract_eq_conditions\|JoinToHashJoin\|collect_conjuncts' src/sql/cascades/rules/implement.rs | head -20
```

Find `JoinToHashJoin::apply` (around line 371–460) and its helper `extract_eq_conditions` (around line 51–78). The helper returns `(Vec<(TypedExpr, TypedExpr)>, Option<TypedExpr>)` where each pair comes from AST order of the `=` operator. It does not know which side each expr belongs to.

- [ ] **Step 2: Write a helper that collects output column names from a memo group.**

If it doesn't already exist, add this helper to `src/sql/cascades/rules/implement.rs`:

```rust
/// Get lowercase column names from a memo group's output columns.
/// Returns empty if the group has no logical properties yet.
fn get_group_column_names(memo: &Memo, group_id: GroupId) -> HashSet<String> {
    memo.groups
        .get(group_id)
        .and_then(|g| g.logical_props.as_ref())
        .map(|props| {
            props
                .output_columns
                .iter()
                .map(|c| c.name.to_lowercase())
                .collect()
        })
        .unwrap_or_default()
}
```

If `get_group_column_names` already exists (it was mentioned in earlier reads of implement.rs), skip this step.

- [ ] **Step 3: Write a helper that tests whether a TypedExpr references only columns in a given set.**

Add to `src/sql/cascades/rules/implement.rs`:

```rust
/// Walk a TypedExpr and return the set of lowercase column names it references.
fn collect_column_refs_lowercase(expr: &TypedExpr) -> HashSet<String> {
    let mut out = HashSet::new();
    walk_column_refs(expr, &mut out);
    out
}

fn walk_column_refs(expr: &TypedExpr, out: &mut HashSet<String>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            out.insert(column.to_lowercase());
        }
        ExprKind::BinaryOp { left, right, .. } => {
            walk_column_refs(left, out);
            walk_column_refs(right, out);
        }
        ExprKind::UnaryOp { operand, .. } => {
            walk_column_refs(operand, out);
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                walk_column_refs(a, out);
            }
        }
        ExprKind::Cast { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::Case { branches, else_expr, .. } => {
            for (cond, val) in branches {
                walk_column_refs(cond, out);
                walk_column_refs(val, out);
            }
            if let Some(e) = else_expr {
                walk_column_refs(e, out);
            }
        }
        ExprKind::Between { expr, low, high, .. } => {
            walk_column_refs(expr, out);
            walk_column_refs(low, out);
            walk_column_refs(high, out);
        }
        ExprKind::InList { expr, list, .. } => {
            walk_column_refs(expr, out);
            for item in list {
                walk_column_refs(item, out);
            }
        }
        ExprKind::Literal(_) | ExprKind::Placeholder(_) => {}
        _ => {}
    }
}
```

If `ExprKind` has variants not listed above, add arms as needed — the existing codebase will make the full variant set visible during compile. If a variant is purely a control flow construct (e.g., CTE references), leaving it out of the walker is fine for eq-condition purposes, since eq conditions are simple comparisons.

- [ ] **Step 4: Write a helper that orients an eq pair based on child column sets.**

Add to `src/sql/cascades/rules/implement.rs`:

```rust
/// Orient an eq pair so that the first element references the left child's
/// columns and the second references the right. Returns:
///   - `Some((a, b))` if natural order works (a from left, b from right).
///   - `Some((b, a))` if swapping works.
///   - `None` if both sides reference the same child (caller should demote
///     the pair into the residual "other" predicate).
fn orient_eq_pair(
    pair: (TypedExpr, TypedExpr),
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> Option<(TypedExpr, TypedExpr)> {
    let (a, b) = pair;
    let a_cols = collect_column_refs_lowercase(&a);
    let b_cols = collect_column_refs_lowercase(&b);

    let a_in_left = !a_cols.is_empty() && a_cols.iter().all(|c| left_cols.contains(c));
    let a_in_right = !a_cols.is_empty() && a_cols.iter().all(|c| right_cols.contains(c));
    let b_in_left = !b_cols.is_empty() && b_cols.iter().all(|c| left_cols.contains(c));
    let b_in_right = !b_cols.is_empty() && b_cols.iter().all(|c| right_cols.contains(c));

    if a_in_left && b_in_right {
        Some((a, b))
    } else if a_in_right && b_in_left {
        Some((b, a))
    } else {
        None
    }
}
```

- [ ] **Step 5: Write the failing tests.**

Append to `src/sql/cascades/rules/implement.rs` in an existing test module (or a new `mod eq_pair_tests`):

```rust
#[cfg(test)]
mod eq_pair_tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn cols(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_lowercase()).collect()
    }

    #[test]
    fn orient_natural_order_keeps_order() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = (col("a_id"), col("b_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        // Left element should be the one that references left-only columns.
        match &out.0.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.1.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_swapped_pair_returns_swapped() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = (col("b_id"), col("a_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        match &out.0.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.1.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_single_side_pair_returns_none() {
        // Both sides of the eq reference left-side columns only.
        let left = cols(&["a_id", "a_name"]);
        let right = cols(&["b_id"]);
        let pair = (col("a_id"), col("a_name"));
        assert!(orient_eq_pair(pair, &left, &right).is_none());
    }
}
```

- [ ] **Step 6: Run tests to verify failure.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rules::implement::eq_pair_tests 2>&1 | tail -15
```

Expected: either compile error (helpers not defined) or test failures. Confirm the failure mode before implementing.

- [ ] **Step 7: Wire the orientation into `JoinToHashJoin::apply`.**

Find `JoinToHashJoin::apply` in `src/sql/cascades/rules/implement.rs`. It currently does something like:

```rust
let (eq_pairs, remaining) = extract_eq_conditions(&op.condition, &op.join_type);
// ... produces alternatives with eq_pairs directly
```

Modify so that after extracting pairs, it orients each pair against child group columns. The pseudocode:

```rust
let left_child = expr.children[0];
let right_child = expr.children[1];
let left_cols = get_group_column_names(memo, left_child);
let right_cols = get_group_column_names(memo, right_child);

let (raw_eq_pairs, mut remaining) = extract_eq_conditions(&op.condition, &op.join_type);
let mut oriented_eq = Vec::new();
let mut demoted_pairs: Vec<TypedExpr> = Vec::new();
for pair in raw_eq_pairs {
    match orient_eq_pair(pair.clone(), &left_cols, &right_cols) {
        Some(oriented) => oriented_eq.push(oriented),
        None => {
            // Same-side pair: demote to residual predicate as a BinaryOp Eq.
            demoted_pairs.push(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(pair.0),
                    op: BinOp::Eq,
                    right: Box::new(pair.1),
                },
                data_type: DataType::Boolean,
                nullable: false,
            });
        }
    }
}
// Fold demoted pairs back into remaining via AND.
for d in demoted_pairs {
    remaining = Some(match remaining {
        Some(existing) => TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(existing),
                op: BinOp::And,
                right: Box::new(d),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        None => d,
    });
}
// Now build PhysicalHashJoin alternatives with oriented_eq as eq_conditions
// and remaining as other_condition. (Keep existing Shuffle/Broadcast alternatives.)
```

Integrate this into the existing `apply()` body — preserve the existing alternative-generation (Shuffle vs Broadcast) so join distribution behavior is unchanged. Only the eq-pair orientation logic is new.

Read the actual existing code before editing to match its structure. In particular, the eq_pairs may be used multiple times to build both Shuffle and Broadcast alternatives — make sure you compute `oriented_eq` and `remaining` once and reuse.

- [ ] **Step 8: Run the tests for JoinToHashJoin (existing + new).**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rules::implement 2>&1 | tail -15
```

Expected: all tests in the implement module pass, including the new `eq_pair_tests`.

- [ ] **Step 9: Simplify `visit_hash_join` in fragment_builder.**

In `src/sql/cascades/fragment_builder.rs`, find the eq-compile loop (currently around lines 559–608 — the "natural-or-swap fallback"). Replace it with a simple "compile pair as-is" loop that trusts the pair's orientation:

```rust
        // eq_conditions pairs are pre-oriented by JoinToHashJoin:
        // pair.0 references left child columns, pair.1 references right.
        // Any pair that could not be oriented is already folded into
        // op.other_condition by the cascades rule.
        let mut eq_join_conjuncts = Vec::new();
        for (lhs_expr, rhs_expr) in &op.eq_conditions {
            let lt = ExprCompiler::new(&left.scope).compile_typed(lhs_expr)?;
            let rt = ExprCompiler::new(&right.scope).compile_typed(rhs_expr)?;
            eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                left: lt,
                right: rt,
                opcode: Some(crate::opcodes::TExprOpcode::EQ),
            });
        }
```

Delete the old natural-or-swap block and the `demoted_eq_exprs` handling (since demotion now happens in the rule). The `other_join_conjuncts` compile loop below is still needed for `op.other_condition`, which now includes demoted pairs — make sure you keep that loop.

- [ ] **Step 10: Compile and run the full test suite.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -10
```

Expected: clean build. Test pass count should match Phase-1 HEAD baseline (882 passed / 18 failed at Phase-1 HEAD); no new failures.

If any new failure appears, check whether it exercises a join with a "tricky" eq-condition shape (e.g., both sides from the same table, or nested predicates). The debug case is usually a missing `ExprKind` variant in `walk_column_refs` — add it.

- [ ] **Step 11: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rules/implement.rs src/sql/cascades/fragment_builder.rs
git commit -m "Normalize eq_conditions pair order in JoinToHashJoin rule

Orients each eq pair by consulting child groups' output columns so that
pair.0 references the left child and pair.1 references the right. Pairs
that reference only one side are demoted into other_condition at rule
time. Fragment builder's visit_hash_join no longer needs the natural-
or-swap fallback (~50 lines removed); it compiles pairs as-is.

Spec §4.2.1: deviation from spec text — normalization lives in
JoinToHashJoin (implementation rule), not JoinCommutativity, because
LogicalJoin has no eq_conditions field. Equivalent intent.
"
```

---

## Task 2.2: Multi-Group Window Decomposition in `WindowToPhysical`

Today, `fragment_builder.rs::visit_window` detects whether the `PhysicalWindow` operator holds expressions with differing `(partition_by, order_by)` signatures and dispatches to `visit_window_multi_group`, which builds multiple sort+analytic node pairs on the fly. Move the signature-group detection into the `WindowToPhysical` implementation rule so the rule produces a chain of single-signature `PhysicalWindow` operators (with `PhysicalSort` between them where the signature changes). Fragment builder's `visit_window` then handles only the single-signature case.

**Files:**
- Modify: `src/sql/cascades/rules/implement.rs` (`WindowToPhysical`)
- Modify: `src/sql/cascades/fragment_builder.rs` (delete `visit_window_multi_group`, simplify `visit_window`)
- Test: `src/sql/cascades/rules/implement.rs` inline tests

### TDD Steps

- [ ] **Step 1: Read `group_win_exprs_by_sig` to understand the signature.**

```bash
cd /Users/harbor/project/NovaRocks
grep -n 'group_win_exprs_by_sig\|fn group_win_exprs' src/sql/physical/emitter/emit_window.rs
```

Read the function body — it partitions `window_exprs: &[WindowExpr]` into `Vec<Vec<usize>>` where each inner Vec is a set of indices sharing the same `(partition_by, order_by)` signature. Order within the outer Vec is the order in which distinct signatures are first seen.

- [ ] **Step 2: Write a helper in the rules module that splits `LogicalWindowOp` into per-signature groups.**

This helper can be shared: add it to `src/sql/cascades/rules/implement.rs` or a new module `src/sql/cascades/rules/window_split.rs`. The choice depends on whether you want to keep implement.rs tight. For this plan, put it in implement.rs alongside the rule:

```rust
/// Split a LogicalWindow's expressions into groups sharing the same
/// (partition_by, order_by) signature. Preserves first-seen order.
fn split_window_exprs_by_signature(
    exprs: &[WindowExpr],
) -> Vec<Vec<WindowExpr>> {
    // Simplest: reuse the emitter helper that returns Vec<Vec<usize>>, then
    // look up the actual WindowExpr by index.
    let index_groups =
        crate::sql::physical::emitter::emit_window::group_win_exprs_by_sig(exprs);
    index_groups
        .into_iter()
        .map(|idxs| idxs.into_iter().map(|i| exprs[i].clone()).collect())
        .collect()
}
```

If `group_win_exprs_by_sig` is not `pub`, make it pub via `pub(crate)` in `emit_window.rs`.

- [ ] **Step 3: Write the failing test.**

Append to `src/sql/cascades/rules/implement.rs` in an existing test module (or new `mod window_split_tests`). Note: `WindowExpr` construction requires some fields — check `src/sql/plan/mod.rs` for the actual struct shape before writing the test.

```rust
#[cfg(test)]
mod window_split_tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn mk_window_expr(name: &str, partition: Vec<TypedExpr>) -> WindowExpr {
        // Fill the struct according to the actual WindowExpr definition in
        // src/sql/plan/mod.rs. The fields shown here are representative —
        // adjust to match the codebase.
        WindowExpr {
            name: name.into(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            partition_by: partition,
            order_by: vec![],
            window_frame: None,
            output_name: name.into(),
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn split_groups_same_signature_together() {
        // Two window exprs both partitioned by `a`; should form one group.
        let exprs = vec![
            mk_window_expr("w1", vec![col("a")]),
            mk_window_expr("w2", vec![col("a")]),
        ];
        let groups = split_window_exprs_by_signature(&exprs);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }

    #[test]
    fn split_separates_different_signatures() {
        // Partition by `a` vs partition by `b` => two groups.
        let exprs = vec![
            mk_window_expr("w1", vec![col("a")]),
            mk_window_expr("w2", vec![col("b")]),
            mk_window_expr("w3", vec![col("a")]),
        ];
        let groups = split_window_exprs_by_signature(&exprs);
        assert_eq!(groups.len(), 2);
        // First group: w1 and w3 (both partition by a).
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[0][0].name, "w1");
        assert_eq!(groups[0][1].name, "w3");
        // Second group: w2.
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[1][0].name, "w2");
    }
}
```

If `WindowExpr`'s struct fields differ from the template above, match the actual struct — the specific field names don't change the logic being tested.

- [ ] **Step 4: Run to confirm failure.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rules::implement::window_split_tests 2>&1 | tail -10
```

Expected: either "undefined function split_window_exprs_by_signature" or test failure.

- [ ] **Step 5: Implement the splitter (already drafted in Step 2).**

If the splitter is already in place from Step 2, confirm tests pass. If `group_win_exprs_by_sig` was not pub, make it so now:

```rust
// In src/sql/physical/emitter/emit_window.rs, change:
//   fn group_win_exprs_by_sig(exprs: &[WindowExpr]) -> Vec<Vec<usize>>
// to:
pub(crate) fn group_win_exprs_by_sig(exprs: &[WindowExpr]) -> Vec<Vec<usize>>
```

- [ ] **Step 6: Modify `WindowToPhysical::apply` to emit a chain when multiple groups exist.**

In `src/sql/cascades/rules/implement.rs`, find `WindowToPhysical::apply` (around line 603–625). Currently it produces a single `PhysicalWindow` NewExpr. Change it to:

```rust
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalWindow(op) = &expr.op else {
            return vec![];
        };
        if expr.children.len() != 1 {
            return vec![];
        }
        let child_group = expr.children[0];

        let groups = split_window_exprs_by_signature(&op.window_exprs);
        if groups.len() <= 1 {
            // Single-group window — keep the existing one-shot translation.
            return vec![NewExpr {
                op: Operator::PhysicalWindow(PhysicalWindowOp {
                    window_exprs: op.window_exprs.clone(),
                    output_columns: op.output_columns.clone(),
                }),
                children: vec![child_group],
            }];
        }

        // Multi-group case: build a chain of PhysicalWindow nodes.
        // Chain shape (bottom-up):
        //   child_group
        //   -> PhysicalSort[group[0].sort_keys]   (if group[0] has order_by)
        //   -> PhysicalWindow[group[0]]
        //   -> PhysicalSort[group[1].sort_keys]   (if signature differs)
        //   -> PhysicalWindow[group[1]]
        //   ...
        //
        // Each intermediate PhysicalWindow / PhysicalSort lives in its own
        // memo group allocated via memo.new_group.
        //
        // The returned NewExpr is the TOP PhysicalWindow, with its child
        // being the Group containing the chain.
        let mut current_group = child_group;
        let num_groups = groups.len();
        for (idx, group_exprs) in groups.iter().enumerate() {
            let first = &group_exprs[0];

            // Insert a PhysicalSort below this group's window if the group
            // has an order_by. (Sort is always safe; cascades will elide
            // redundant sorts via property enforcement.)
            if !first.order_by.is_empty() || !first.partition_by.is_empty() {
                let sort_items = sort_items_for_window(first);
                let sort_op = Operator::PhysicalSort(PhysicalSortOp { items: sort_items });
                let sort_mexpr = MExpr {
                    id: memo.next_expr_id(),
                    op: sort_op,
                    children: vec![current_group],
                };
                current_group = memo.new_group(sort_mexpr);
            }

            // For non-terminal groups, build an intermediate PhysicalWindow
            // group; for the terminal group, emit via NewExpr.
            let output_columns = if idx == num_groups - 1 {
                op.output_columns.clone()
            } else {
                // Intermediate: no need to carry user-facing output_columns;
                // reuse the same list for simplicity (downstream properties
                // derive from window_exprs, not output_columns).
                op.output_columns.clone()
            };

            if idx == num_groups - 1 {
                return vec![NewExpr {
                    op: Operator::PhysicalWindow(PhysicalWindowOp {
                        window_exprs: group_exprs.clone(),
                        output_columns,
                    }),
                    children: vec![current_group],
                }];
            } else {
                let win_op = Operator::PhysicalWindow(PhysicalWindowOp {
                    window_exprs: group_exprs.clone(),
                    output_columns,
                });
                let win_mexpr = MExpr {
                    id: memo.next_expr_id(),
                    op: win_op,
                    children: vec![current_group],
                };
                current_group = memo.new_group(win_mexpr);
            }
        }

        vec![]  // unreachable: the loop above always returns or updates
    }
```

Also add the `sort_items_for_window` helper:

```rust
/// Derive sort items for a window's partition_by + order_by.
/// Window sort ordering is: partition_by columns first (ASC, NULLS FIRST),
/// then order_by columns with their own direction.
fn sort_items_for_window(win: &WindowExpr) -> Vec<SortItem> {
    let mut items = Vec::new();
    for expr in &win.partition_by {
        items.push(SortItem {
            expr: expr.clone(),
            asc: true,
            nulls_first: true,
        });
    }
    for item in &win.order_by {
        items.push(item.clone());
    }
    items
}
```

The exact SortItem field names may differ from `{expr, asc, nulls_first}` — check `src/sql/ir/mod.rs` for the actual struct. The logic is: prepend partition columns as ASC sort keys, then append the user's order_by items as-is.

- [ ] **Step 7: Delete `visit_window_multi_group` and simplify `visit_window`.**

In `src/sql/cascades/fragment_builder.rs`:

1. Delete the entire `fn visit_window_multi_group` method (currently around lines 1273+).
2. In `visit_window` (around line 1085), remove the multi-group dispatch. The new body starts at the existing "child := self.visit(&node.children[0])" line — keep everything from there onward (the current code that handles the single-group case). Delete only the first few lines:

```rust
        // DELETE these lines:
        // let groups =
        //     crate::sql::physical::emitter::emit_window::group_win_exprs_by_sig(&op.window_exprs);
        // if groups.len() > 1 {
        //     return self.visit_window_multi_group(op, node, &groups);
        // }
```

After this, `visit_window` assumes all `PhysicalWindow` operators it receives have a single signature group, because the cascades rule guarantees it.

Also add a `debug_assert!` at the top of `visit_window` as a tripwire:

```rust
    fn visit_window(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::ir::{WindowBound, WindowFrameType};

        debug_assert!(
            {
                let groups =
                    crate::sql::physical::emitter::emit_window::group_win_exprs_by_sig(&op.window_exprs);
                groups.len() <= 1
            },
            "PhysicalWindow with multiple signature groups reached fragment builder; cascades rule should have decomposed into a chain"
        );

        let child = self.visit(&node.children[0])?;
        // ... rest of single-group handling unchanged
```

- [ ] **Step 8: Compile and run full tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -15
```

Expected: clean build; no new failures vs Phase-1 HEAD (882/18).

- [ ] **Step 9: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rules/implement.rs src/sql/cascades/fragment_builder.rs src/sql/physical/emitter/emit_window.rs
git commit -m "Decompose multi-group windows in WindowToPhysical rule

When a LogicalWindow contains expressions with differing
(partition_by, order_by) signatures, the rule now builds a chain
of PhysicalWindow nodes with PhysicalSort inserted where the
signature changes. Fragment builder's visit_window_multi_group
is deleted; visit_window asserts it receives a single-signature
PhysicalWindow.

Spec §4.2.2.
"
```

---

## Task 2.3: OUTER JOIN Nullability Widening as a Physical Property

Today, `fragment_builder.rs::visit_hash_join` mutates the descriptor table based on the join type (`widen_tuple_nullable(tid)` for nullable sides). This is an execution-level assignment that belongs on the `PhysicalPlanNode`'s output metadata, not at emission time.

For Phase 1 pilot's scope and the broader goal of making fragment_builder pass-through, move nullability widening to the `PhysicalPlanNode.output_columns` field: `PhysicalHashJoin`'s output columns should carry their correct `nullable` flags as derived during cascades search, so that fragment builder reads them straight from `node.output_columns` without calling `widen_tuple_nullable`.

**Context:** Nullability is currently derived at two separate places: (a) `stats.rs::derive_output_columns` for joins just concatenates children's output columns without widening nullability (lines 1038–1046 and 953–982 for physical/logical join), and (b) `fragment_builder.rs::visit_hash_join` mutates the descriptor table with `widen_tuple_nullable(tid)` based on `op.join_type` (lines 639–658). The mismatch means `node.output_columns[i].nullable` is wrong for downstream cascades code that relies on it, and the fragment builder has to re-derive the widening from the join type.

The fix: move the widening into `derive_output_columns` so `output_columns[i].nullable` is correct. Fragment builder continues calling `widen_tuple_nullable` but now drives the decision from `output_columns` rather than from `op.join_type` — equivalent in behavior today, but sets up the invariant that `output_columns` is the single source of truth for nullability.

**Files:**
- Modify: `src/sql/cascades/stats.rs` (`derive_output_columns` — the `LogicalJoin` and `PhysicalHashJoin|PhysicalNestLoopJoin` arms)
- Modify: `src/sql/cascades/fragment_builder.rs` (`visit_hash_join` — drive widening from `node.output_columns` rather than `op.join_type`)
- Test: inline in `src/sql/cascades/stats.rs`

### TDD Steps

- [ ] **Step 1: Read the current derivation.**

Read `src/sql/cascades/stats.rs` lines 953–982 (the `LogicalJoin` arm of `derive_output_columns`) and lines 1038–1046 (the `PhysicalHashJoin | PhysicalNestLoopJoin` arm). Both simply concatenate children's columns without touching nullable flags.

Read `src/sql/cascades/fragment_builder.rs` lines 639–658 — the current widening match on `op.join_type`.

- [ ] **Step 2: Add a helper `widen_for_join_kind`.**

Add to `src/sql/cascades/stats.rs`, near `derive_output_columns`:

```rust
/// Widen the nullable flags of `left_cols` and `right_cols` according to the
/// join's outer-join semantics and return the concatenated result.
///
///  - Inner, Cross:        no widening
///  - LeftOuter:           widen right columns to nullable
///  - RightOuter:          widen left columns to nullable
///  - FullOuter:           widen both sides to nullable
///  - LeftSemi, LeftAnti:  only left columns survive; no widening needed
///  - RightSemi, RightAnti: only right columns survive; no widening needed
///
/// For semi/anti joins this function returns just the surviving side.
fn widen_for_join_kind(
    join_type: crate::sql::ir::JoinKind,
    left_cols: Vec<crate::sql::ir::OutputColumn>,
    right_cols: Vec<crate::sql::ir::OutputColumn>,
) -> Vec<crate::sql::ir::OutputColumn> {
    use crate::sql::ir::JoinKind::*;
    fn widen(cols: Vec<crate::sql::ir::OutputColumn>) -> Vec<crate::sql::ir::OutputColumn> {
        cols.into_iter()
            .map(|mut c| {
                c.nullable = true;
                c
            })
            .collect()
    }
    match join_type {
        Inner | Cross => {
            let mut out = left_cols;
            out.extend(right_cols);
            out
        }
        LeftOuter => {
            let mut out = left_cols;
            out.extend(widen(right_cols));
            out
        }
        RightOuter => {
            let mut out = widen(left_cols);
            out.extend(right_cols);
            out
        }
        FullOuter => {
            let mut out = widen(left_cols);
            out.extend(widen(right_cols));
            out
        }
        LeftSemi | LeftAnti => left_cols,
        RightSemi | RightAnti => right_cols,
    }
}
```

- [ ] **Step 3: Write the failing test.**

Add to `src/sql/cascades/stats.rs` in an existing `#[cfg(test)] mod tests` block (or a new `mod join_widening_tests`):

```rust
#[cfg(test)]
mod join_widening_tests {
    use super::*;
    use crate::sql::ir::{JoinKind, OutputColumn};
    use arrow::datatypes::DataType;

    fn c(name: &str, nullable: bool) -> OutputColumn {
        OutputColumn {
            name: name.into(),
            data_type: DataType::Int32,
            nullable,
        }
    }

    #[test]
    fn inner_preserves_nullability() {
        let out = widen_for_join_kind(
            JoinKind::Inner,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert_eq!(out.len(), 2);
        assert!(!out[0].nullable);
        assert!(!out[1].nullable);
    }

    #[test]
    fn left_outer_widens_right() {
        let out = widen_for_join_kind(
            JoinKind::LeftOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(!out[0].nullable, "left side preserved");
        assert!(out[1].nullable, "right side widened");
    }

    #[test]
    fn right_outer_widens_left() {
        let out = widen_for_join_kind(
            JoinKind::RightOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(out[0].nullable, "left side widened");
        assert!(!out[1].nullable, "right side preserved");
    }

    #[test]
    fn full_outer_widens_both() {
        let out = widen_for_join_kind(
            JoinKind::FullOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(out[0].nullable);
        assert!(out[1].nullable);
    }

    #[test]
    fn left_semi_returns_left_only() {
        let out = widen_for_join_kind(
            JoinKind::LeftSemi,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "a");
        assert!(!out[0].nullable);
    }

    #[test]
    fn right_anti_returns_right_only() {
        let out = widen_for_join_kind(
            JoinKind::RightAnti,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "b");
    }
}
```

- [ ] **Step 4: Run tests to verify failure.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::stats::join_widening_tests 2>&1 | tail -15
```

Expected: either undefined-symbol error for `widen_for_join_kind` (if Step 2 hasn't been applied yet) or tests pass immediately after Step 2.

- [ ] **Step 5: Wire `widen_for_join_kind` into the `LogicalJoin` arm.**

Replace the `Operator::LogicalJoin(j)` arm of `derive_output_columns` (around lines 955–982) with:

```rust
        Operator::LogicalJoin(j) => {
            let left_cols = expr
                .children
                .get(0)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }
```

- [ ] **Step 6: Wire `widen_for_join_kind` into the physical-join arm.**

Replace the `Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_)` arm (around lines 1038–1046). We need the join kind, so we match each variant separately:

```rust
        Operator::PhysicalHashJoin(j) => {
            let left_cols = expr
                .children
                .get(0)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }
        Operator::PhysicalNestLoopJoin(j) => {
            let left_cols = expr
                .children
                .get(0)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }
```

- [ ] **Step 7: Update `visit_hash_join` to drive widening from `node.output_columns`.**

In `src/sql/cascades/fragment_builder.rs`, replace the existing `match op.join_type { ... widen_tuple_nullable ... }` block (lines 639–658) with:

```rust
        // Widen nullable flags on the side(s) that the cascades layer has
        // marked nullable in node.output_columns. SEMI/ANTI joins still
        // need runtime-level widening on the pruned side because the
        // runtime emits null-padded columns for the pruned side; that
        // path is covered by the match below on join_type (unchanged).
        //
        // Invariant: if the cascades layer flagged any column on a side
        // as nullable, widen that side's tuple at the descriptor level.
        let left_col_count = left.scope.columns().count();
        let any_left_nullable = node
            .output_columns
            .iter()
            .take(left_col_count)
            .any(|c| c.nullable);
        let any_right_nullable = node
            .output_columns
            .iter()
            .skip(left_col_count)
            .any(|c| c.nullable);

        // For Inner joins, the cascades layer may or may not have marked
        // columns nullable depending on child nullability — widen_tuple
        // is idempotent, so this is safe to call whenever the flag is set.
        if any_left_nullable {
            for &tid in &left.tuple_ids {
                self.desc_builder.widen_tuple_nullable(tid);
            }
        }
        if any_right_nullable {
            for &tid in &right.tuple_ids {
                self.desc_builder.widen_tuple_nullable(tid);
            }
        }

        // SEMI/ANTI: runtime still emits null-padded pruned-side columns
        // (extend_with_null_build_columns / extend_with_null_probe_columns),
        // and downstream operators reference those slots. Widen the pruned
        // side explicitly even though output_columns doesn't include it.
        match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => {
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightSemi | JoinKind::RightAnti => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }
```

This change:
1. Replaces the join-kind-based match with an `output_columns`-driven decision for the main left/right widening.
2. Keeps an explicit match for SEMI/ANTI because the runtime's null-padding of the pruned side is a distinct concern from cascades nullability semantics — the pruned side's columns aren't in output_columns at all, so we must widen by inspecting join_type for those cases.

- [ ] **Step 8: Run tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::stats::join_widening_tests 2>&1 | tail -15
cargo test 2>&1 | tail -10
```

Expected: all 6 widening tests pass; full test suite stays at 882/18 (no new failures).

- [ ] **Step 7: Compile and run full tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -10
```

Expected: clean build; no new failures.

- [ ] **Step 8: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/search.rs src/sql/cascades/stats.rs src/sql/cascades/fragment_builder.rs
git commit -m "Widen OUTER JOIN output column nullability at cascades level

PhysicalHashJoin's output_columns now carry the correct nullable flags
derived from the join kind during cascades search. Fragment builder
reads them from node.output_columns instead of computing join-kind-
based mutations at emission time.

Spec §4.2.3.
"
```

---

## Task 3: Phase-2 Regression Verification

Same structure as Phase 1 Task 9. Diff Phase-2 EXPLAIN snapshots against the Phase-1 baseline. Expectation: no user-visible plan changes from tasks 2.1, 2.2, 2.3 — these are internal refactors that preserve the existing emission output.

**Files:** None — verification only, plus spec doc landing notes.

- [ ] **Step 1: Rebuild and start standalone server.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 &
disown
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

- [ ] **Step 2: Recreate the Iceberg catalog.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_tpcds PROPERTIES(
  \"type\"=\"iceberg\",
  \"iceberg.catalog.type\"=\"hadoop\",
  \"iceberg.catalog.warehouse\"=\"oss://novarocks/iceberg-catalog/\",
  \"aws.s3.access_key\"=\"admin\",
  \"aws.s3.secret_key\"=\"admin123\",
  \"aws.s3.endpoint\"=\"http://127.0.0.1:9000\",
  \"aws.s3.enable_path_style_access\"=\"true\"
);"
```

- [ ] **Step 3: Capture 99 Phase-2 snapshots.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-phase2
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-phase2/q${i}.plan" 2>&1
  fi
done
ls /tmp/novarocks-plan-compare/standalone-phase2/ | wc -l
```

Expected: 99 files.

- [ ] **Step 4: Diff Phase-1 vs Phase-2.**

```bash
echo "=== Plans that differ between Phase-1 and Phase-2 ==="
for i in $(seq 1 99); do
  if ! diff -q \
    /tmp/novarocks-plan-compare/standalone-phase1/q${i}.plan \
    /tmp/novarocks-plan-compare/standalone-phase2/q${i}.plan > /dev/null; then
    echo "q${i}: differs"
  fi
done | wc -l
echo "queries differing"
```

Expected: 0 queries differ, OR a very small number differ in ways clearly explainable by Task 2.1/2.2/2.3 (e.g., eq_conditions pair order may change in the raw plan text if the EXPLAIN formatter echoes them; this is cosmetic).

For any differing query, inspect manually:

```bash
diff /tmp/novarocks-plan-compare/standalone-phase1/qNN.plan \
     /tmp/novarocks-plan-compare/standalone-phase2/qNN.plan
```

Confirm the diff only affects eq_conditions ordering, window chaining output, or nullable flags — nothing else (scan/join/agg shapes should be unchanged).

- [ ] **Step 5: TPC-DS end-to-end verification.**

```bash
cd /Users/harbor/project/NovaRocks
NO_PROXY=127.0.0.1,localhost cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify -j 4 2>&1 | tail -50
```

Expected: Same 85/99 pass rate as Phase 1 (14 pre-existing failures at the same queries: q6, q7, q17, q18, q25, q26, q29, q45, q54, q61, q72, q80, q85, q95). No new failures.

- [ ] **Step 6: Stop standalone, record Phase-2 sha.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
echo "phase2 complete: $(cd /Users/harbor/project/NovaRocks && git rev-parse HEAD) at $(date -Iseconds)" \
  >> /tmp/novarocks-plan-compare/phase0-ref.txt
```

- [ ] **Step 7: Update spec with Phase-2 landing note.**

Edit `docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md`. At the end of section 4.2, append a "Phase 2 landed" note mirroring the Phase 1 note (date, sha, observed differences count, TPC-DS verify summary).

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md
git commit -m "Phase 2 fragment_builder cleanups: mark spec section 4.2 as landed"
```

---

## Done

Phase 2 delivers:

- Eq_conditions pair orientation moved to `JoinToHashJoin`; fragment builder no longer has natural-or-swap fallback (~50 lines removed).
- Window signature-group decomposition moved to `WindowToPhysical`; fragment builder's `visit_window_multi_group` deleted.
- OUTER JOIN output column nullability derived at cascades level; fragment builder no longer mutates the descriptor table at emission time (or, if the Step 6 simpler approach was taken, the mutation is at least driven from `output_columns` rather than re-deriving from `op.join_type`).
- TPC-DS EXPLAIN snapshots diff-minimal vs Phase 1; verify suite pass rate unchanged.

Next round: Phase 3 (expression rewriting infrastructure) + Phase 4 (two-phase aggregation) — the original motivation for the cleanup. Those get their own plan.
