# Cascades CTE Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align non-recursive CTE handling with the StarRocks Cascades structure by routing all CTE queries through a single `CTEAnchor / CTEProduce / CTEConsume` logical tree, inlining single-use CTEs and reusing multi-use CTEs via multicast/exchange.

**Architecture:** The analyzer stops pre-inlining CTEs and registers every non-recursive `WITH` definition in declaration order. The planner emits a single `LogicalPlan` tree built from nested `CTEAnchor` nodes. Before Memo conversion, a lightweight Cascades CTE rewrite collects consume counts and inlines only `consume_count <= 1`; the remaining multi-use CTE structure reaches the physical plan, where `PlanFragmentBuilder` emits multicast producer fragments and exchange consumers.

**Tech Stack:** Rust, Arrow, existing Thrift plan/data sink types, existing Cascades Memo/extract pipeline, standalone embedded engine tests

**Design Spec:** `docs/superpowers/specs/2026-04-03-cascades-cte-alignment-design.md`

---

## File Structure

### New Files

| File | Responsibility |
|------|----------------|
| `src/sql/cascades/cte_rewrite.rs` | `CTEContext`, `collect_cte_counts()`, `inline_single_use_ctes()`; owns the pre-Memo CTE decision pass |

### Modified Files

| File | Change |
|------|--------|
| `src/sql/cte.rs` | Change `CTERegistry` semantics from “shared CTEs only” to “all non-recursive CTE definitions in scope” |
| `src/sql/analyzer/mod.rs` | Remove ref-count-based CTE splitting, register every non-recursive CTE, add analyzer tests |
| `src/sql/analyzer/resolve_from.rs` | Emit `Relation::CTEConsume` for every CTE reference; reject forward references |
| `src/sql/ir/mod.rs` | Update `Relation::CTEConsume` comments to reflect unified CTE structure |
| `src/sql/plan/mod.rs` | Add `CTEAnchor` and `CTEProduce` plan nodes; retire `QueryPlan` from the main path |
| `src/sql/planner/mod.rs` | Change `plan_query()` to return a single anchor-wrapped `LogicalPlan`; add planner tests |
| `src/sql/explain.rs` | Format logical and physical `CTE ANCHOR / PRODUCE / CONSUME` nodes |
| `src/sql/cascades/mod.rs` | Run pre-Memo CTE rewrite before logical-to-Memo conversion |
| `src/sql/cascades/operator.rs` | Add logical and physical `CTEAnchor` operator structs and enum variants |
| `src/sql/cascades/convert.rs` | Convert `CTEAnchor / Produce / Consume` to Memo groups; add convert tests |
| `src/sql/cascades/rules/implement.rs` | Implement `LogicalCTEAnchor -> PhysicalCTEAnchor` |
| `src/sql/cascades/stats.rs` | Derive stats and output columns for `CTEAnchor` |
| `src/sql/cascades/search.rs` | Treat `PhysicalCTEAnchor` as pass-through for required/output properties |
| `src/sql/cascades/cost.rs` | Give `PhysicalCTEAnchor` zero local cost |
| `src/sql/cascades/extract.rs` | Extract `PhysicalCTEAnchor` into `PhysicalPlanNode` |
| `src/sql/cascades/fragment_builder.rs` | Add `visit_cte_anchor()` and align `visit_cte_produce/consume()` with StarRocks responsibilities |
| `src/sql/optimizer/mod.rs` | Add compile-safe `CTEAnchor / CTEProduce` handling in legacy helper matches |
| `src/sql/optimizer/cardinality.rs` | Add compile-safe `CTEAnchor / CTEProduce` handling |
| `src/sql/optimizer/column_pruning.rs` | Add compile-safe `CTEAnchor / CTEProduce` handling |
| `src/sql/optimizer/expr_utils.rs` | Add compile-safe `CTEAnchor / CTEProduce` handling |
| `src/sql/optimizer/join_reorder.rs` | Add compile-safe `CTEAnchor / CTEProduce` handling |
| `src/sql/physical/emitter/mod.rs` | Reject or pass through new logical CTE nodes so the legacy emitter still compiles while it is no longer on the main path |
| `src/standalone/engine.rs` | Remove CTE/non-CTE branching; run all non-recursive queries through planner -> cascades -> fragment builder; add embedded CTE tests |

---

### Task 1: Analyzer Registers All Non-Recursive CTEs

**Files:**
- Modify: `src/sql/cte.rs`
- Modify: `src/sql/analyzer/mod.rs`
- Modify: `src/sql/analyzer/resolve_from.rs`
- Modify: `src/sql/ir/mod.rs`
- Test: `src/sql/analyzer/mod.rs`

- [ ] **Step 1: Add failing analyzer tests for single-use registration and forward-reference rejection**

Append these tests to the existing `#[cfg(test)] mod tests` in `src/sql/analyzer/mod.rs`:

```rust
    fn parse_and_analyze_with_registry(
        sql: &str,
    ) -> Result<(ResolvedQuery, crate::sql::cte::CTERegistry), String> {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| e.to_string())?;
        let stmt = ast.pop().ok_or_else(|| "expected a statement".to_string())?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected a query".into()),
        };
        analyze(&query, &TestCatalog, "default")
    }

    #[test]
    fn test_single_use_cte_is_still_registered() {
        let sql = "WITH order_totals AS (SELECT o_orderkey AS ok FROM orders) \
                   SELECT ok FROM order_totals";
        let (_resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].name, "order_totals");
    }

    #[test]
    fn test_forward_cte_reference_is_rejected() {
        let sql = "WITH b AS (SELECT * FROM a), a AS (SELECT o_orderkey FROM orders) \
                   SELECT * FROM b";
        let err = parse_and_analyze_with_registry(sql).expect_err("forward reference must fail");
        assert!(
            err.contains("forward CTE reference is not supported"),
            "err={err}"
        );
    }
```

- [ ] **Step 2: Run the targeted analyzer tests and verify they fail for the current behavior**

Run:

```bash
cargo test test_single_use_cte_is_still_registered --lib
cargo test test_forward_cte_reference_is_rejected --lib
```

Expected:
- `test_single_use_cte_is_still_registered` fails because the registry is empty for single-use CTEs
- `test_forward_cte_reference_is_rejected` fails because the current code still allows the later CTE to be seen

- [ ] **Step 3: Rework `CTERegistry` to describe all non-recursive definitions, not only shared CTEs**

Replace the top-level comments in `src/sql/cte.rs` with:

```rust
//! CTE (Common Table Expression) metadata types.
//!
//! The registry stores all non-recursive CTE definitions visible to the
//! current query scope. Inline vs reuse is decided later by Cascades.
```

Keep the type layout the same, but change the struct comments to:

```rust
/// Registry of all non-recursive CTEs produced by the analyzer.
/// The planner turns these definitions into `CTEProduce` / `CTEAnchor`
/// structure; Cascades decides later whether to inline or reuse them.
#[derive(Clone, Debug, Default)]
pub(crate) struct CTERegistry {
    pub entries: Vec<CTEEntry>,
    next_id: CteId,
}

/// A single analyzed CTE definition in the current query scope.
#[derive(Clone, Debug)]
pub(crate) struct CTEEntry {
    pub id: CteId,
    pub name: String,
    pub resolved_query: ResolvedQuery,
    pub output_columns: Vec<OutputColumn>,
}
```

- [ ] **Step 4: Remove ref-count-based CTE splitting from `AnalyzerContext` and register CTEs in declaration order**

In `src/sql/analyzer/mod.rs`, replace the CTE-specific fields in `AnalyzerContext`:

```rust
    /// Already-visible CTE definitions from outer scopes or earlier entries in
    /// the same WITH clause, keyed by lowercase name.
    pub(super) ctes: std::collections::HashMap<String, (sqlast::Query, Vec<String>)>,
    /// Names declared by the current WITH clause but not yet visible because
    /// their definitions have not been analyzed.
    pub(super) pending_ctes: std::collections::HashSet<String>,
    /// Subqueries collected during expression analysis.
    pub(super) collected_subqueries: std::cell::RefCell<Vec<SubqueryInfo>>,
    /// Accumulated CTE registry for the current query scope.
    pub(super) cte_registry: std::cell::RefCell<crate::sql::cte::CTERegistry>,
```

Then replace the current “Two-pass CTE analysis” block in `analyze_query()` with this declaration-order loop:

```rust
            let mut pending_ctes: std::collections::HashSet<String> = with_clause
                .cte_tables
                .iter()
                .map(|cte| cte.alias.name.value.to_lowercase())
                .collect();

            let mut child_ctx = super::AnalyzerContext {
                catalog: self.catalog,
                current_database: self.current_database,
                ctes: self.ctes.clone(),
                pending_ctes: pending_ctes.clone(),
                next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
                collected_subqueries: std::cell::RefCell::new(Vec::new()),
                cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
            };

            for cte in &with_clause.cte_tables {
                let name = cte.alias.name.value.to_lowercase();
                pending_ctes.remove(&name);
                child_ctx.pending_ctes = pending_ctes.clone();

                let col_aliases: Vec<String> = cte
                    .alias
                    .columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect();

                let mut resolved_cte = child_ctx.analyze_query(&cte.query)?;
                if !col_aliases.is_empty() {
                    for (idx, alias_name) in col_aliases.iter().enumerate() {
                        if let Some(col) = resolved_cte.output_columns.get_mut(idx) {
                            col.name = alias_name.clone();
                        }
                    }
                }

                let output_columns = resolved_cte.output_columns.clone();
                child_ctx
                    .cte_registry
                    .borrow_mut()
                    .register(name.clone(), resolved_cte, output_columns);

                child_ctx
                    .ctes
                    .insert(name, (*cte.query.clone(), col_aliases));
            }
```

- [ ] **Step 5: Always emit `Relation::CTEConsume` for CTE references and reject forward references**

In `src/sql/analyzer/resolve_from.rs`, replace the current shared/single-ref split with:

```rust
                if parts.len() == 1 {
                    let registry = self.cte_registry.borrow();
                    if let Some(entry) = registry.entries.iter().find(|entry| entry.name == tbl_lower) {
                        let alias_name = alias
                            .as_ref()
                            .map(|a| a.name.value.clone())
                            .unwrap_or_else(|| tbl.clone());
                        let output_columns = entry.output_columns.clone();
                        let mut scope = AnalyzerScope::new();
                        for col in &output_columns {
                            scope.add_column(
                                Some(&alias_name),
                                &col.name,
                                col.data_type.clone(),
                                col.nullable,
                            );
                        }
                        return Ok((
                            Relation::CTEConsume {
                                cte_id: entry.id,
                                alias: alias_name,
                                output_columns,
                            },
                            scope,
                        ));
                    }
                }

                if parts.len() == 1 && self.pending_ctes.contains(&tbl_lower) {
                    return Err(format!(
                        "forward CTE reference is not supported: {tbl_lower}"
                    ));
                }
```

Also update the `Relation::CTEConsume` comment in `src/sql/ir/mod.rs` to:

```rust
    /// Reference to an analyzed non-recursive CTE definition.
    /// Inline vs reuse is decided later by Cascades.
```

- [ ] **Step 6: Run the analyzer tests again and verify they pass**

Run:

```bash
cargo test test_single_use_cte_is_still_registered --lib
cargo test test_forward_cte_reference_is_rejected --lib
cargo test test_cte_with_alias --lib
cargo test test_cte_with_comma_join_and_correlated_subquery --lib
```

Expected:
- all four tests pass
- no new analyzer regressions appear in the selected CTE tests

- [ ] **Step 7: Commit the analyzer-only change**

```bash
git add src/sql/cte.rs src/sql/analyzer/mod.rs src/sql/analyzer/resolve_from.rs src/sql/ir/mod.rs
git commit -m "refactor(sql): analyze all non-recursive CTE definitions"
```

---

### Task 2: Planner Emits Anchor-Wrapped Single-Tree CTE Plans

**Files:**
- Modify: `src/sql/plan/mod.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/cardinality.rs`
- Modify: `src/sql/optimizer/column_pruning.rs`
- Modify: `src/sql/optimizer/expr_utils.rs`
- Modify: `src/sql/optimizer/join_reorder.rs`
- Modify: `src/sql/physical/emitter/mod.rs`
- Modify: `src/standalone/engine.rs`
- Test: `src/sql/planner/mod.rs`

- [ ] **Step 1: Add failing planner tests for single CTE and two-CTE anchor chains**

Add a new `#[cfg(test)] mod tests` block at the bottom of `src/sql/planner/mod.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::tests::TestCatalog;

    fn parse_analyze_and_plan(sql: &str) -> Result<LogicalPlan, String> {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| e.to_string())?;
        let stmt = ast.pop().ok_or_else(|| "expected a statement".to_string())?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected query".into()),
        };
        let (resolved, cte_registry) =
            crate::sql::analyzer::analyze(&query, &TestCatalog, "default")?;
        plan_query(resolved, cte_registry)
    }

    #[test]
    fn test_plan_query_wraps_single_cte_in_anchor() {
        let plan = parse_analyze_and_plan(
            "WITH t AS (SELECT o_orderkey AS ok FROM orders) SELECT ok FROM t",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 0);
                assert!(matches!(*anchor.produce, LogicalPlan::CTEProduce(_)));
            }
            other => panic!("expected CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_query_builds_nested_anchor_chain() {
        let plan = parse_analyze_and_plan(
            "WITH a AS (SELECT o_orderkey AS ok FROM orders), \
                  b AS (SELECT ok FROM a) \
             SELECT ok FROM b",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(anchor_a) => match *anchor_a.consumer {
                LogicalPlan::CTEAnchor(anchor_b) => {
                    assert_eq!(anchor_a.cte_id, 0);
                    assert_eq!(anchor_b.cte_id, 1);
                }
                other => panic!("expected nested CTEAnchor, got {other:?}"),
            },
            other => panic!("expected outer CTEAnchor, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the planner tests and verify they fail because `plan_query()` still returns `QueryPlan`**

Run:

```bash
cargo test test_plan_query_wraps_single_cte_in_anchor --lib
cargo test test_plan_query_builds_nested_anchor_chain --lib
```

Expected:
- compilation fails or tests fail because `plan_query()` does not yet return `LogicalPlan`

- [ ] **Step 3: Add `CTEAnchor` and `CTEProduce` nodes to `LogicalPlan`, but keep `QueryPlan` temporarily for the legacy execution branch**

In `src/sql/plan/mod.rs`, replace the CTE section with:

```rust
    /// Defines the scope of one CTE. The left child is the producer subtree;
    /// the right child is the query subtree that may consume it.
    CTEAnchor(CTEAnchorNode),
    /// Produces the analyzed CTE definition.
    CTEProduce(CTEProduceNode),
    /// Reference to a CTE definition. Leaf node.
    CTEConsume(CTEConsumeNode),
}

#[derive(Clone, Debug)]
pub(crate) struct CTEAnchorNode {
    pub cte_id: crate::sql::cte::CteId,
    pub produce: Box<LogicalPlan>,
    pub consumer: Box<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProduceNode {
    pub cte_id: crate::sql::cte::CteId,
    pub input: Box<LogicalPlan>,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeNode {
    pub cte_id: crate::sql::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}
```

Keep `QueryPlan` and `CTEProducePlan` in `src/sql/plan/mod.rs` for now; `execute_query()` still needs them until Task 5 removes the old branch.

- [ ] **Step 4: Change `plan_query()` to return a single anchor-wrapped `LogicalPlan`, and move the old behavior into a temporary legacy helper**

In `src/sql/planner/mod.rs`, replace the current `plan_query()` implementation with:

```rust
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<LogicalPlan, String> {
    let mut root = plan(resolved)?;

    for entry in cte_registry.entries.into_iter().rev() {
        let produce_input = plan(entry.resolved_query)?;
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: entry.id,
            input: Box::new(produce_input),
            output_columns: entry.output_columns,
        });
        root = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: entry.id,
            produce: Box::new(produce),
            consumer: Box::new(root),
        });
    }

    Ok(root)
}

pub(crate) fn plan_query_legacy(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<QueryPlan, String> {
    let output_columns = resolved.output_columns.clone();
    let main_plan = plan(resolved)?;

    let cte_plans = cte_registry
        .entries
        .into_iter()
        .map(|entry| {
            let cte_plan = plan(entry.resolved_query)?;
            Ok(CTEProducePlan {
                cte_id: entry.id,
                plan: cte_plan,
                output_columns: entry.output_columns,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(QueryPlan {
        cte_plans,
        main_plan,
        output_columns,
    })
}
```

Keep `plan(resolved)` as the non-`WITH` helper that plans a single `ResolvedQuery`.

- [ ] **Step 5: Add logical explain formatting, compile-safe helper matches, and temporary engine compatibility**

In `src/sql/explain.rs`, add these logical formatting arms inside `format_node()`:

```rust
        LogicalPlan::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node(&node.produce, level, indent + 1, out);
            format_node(&node.consumer, level, indent + 1, out);
        }
        LogicalPlan::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node(&node.input, level, indent + 1, out);
        }
```

Then add compile-safe passthrough arms in the legacy helpers:

`src/sql/optimizer/mod.rs`
```rust
        LogicalPlan::CTEAnchor(n) => LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: n.cte_id,
            produce: Box::new(f(*n.produce)),
            consumer: Box::new(f(*n.consumer)),
        }),
        LogicalPlan::CTEProduce(n) => LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: n.cte_id,
            input: Box::new(f(*n.input)),
            output_columns: n.output_columns,
        }),
```

`src/sql/optimizer/cardinality.rs`
```rust
        LogicalPlan::CTEAnchor(node) => estimate(&node.consumer, table_stats),
        LogicalPlan::CTEProduce(node) => estimate(&node.input, table_stats),
```

`src/sql/optimizer/join_reorder.rs`
```rust
        LogicalPlan::CTEAnchor(_) | LogicalPlan::CTEProduce(_) | LogicalPlan::CTEConsume(_) => plan,
```

`src/sql/physical/emitter/mod.rs`
```rust
            LogicalPlan::CTEAnchor(_) | LogicalPlan::CTEProduce(_) => Err(
                "legacy emitter does not handle Cascades CTE structure".to_string()
            ),
```

In `src/standalone/engine.rs`, switch the CTE `EXPLAIN` branch to the new logical tree, but keep execution on the temporary legacy helper until Task 5:

```rust
    } else {
        let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;
        let all_stats = build_table_stats_from_plan(&logical);

        if matches!(level, ExplainLevel::Costs) {
            for (table, stats) in &all_stats {
                lines.push(format!(
                    "  Statistics: {table} row_count={}",
                    stats.row_count
                ));
            }
        }
        lines.extend(crate::sql::explain::explain_plan(&logical, level));
    }
```

and in `execute_query()` keep the old branch compiling with:

```rust
        let query_plan =
            crate::sql::planner::plan_query_legacy(resolved, cte_registry)?;
```

Update any remaining `match plan { ... }` compile failures with the same pass-through rule:
- `CTEAnchor` recurses to both children or to `consumer`, depending on the helper’s job
- `CTEProduce` recurses to `input`

- [ ] **Step 6: Run the planner tests again and verify they pass**

Run:

```bash
cargo test test_plan_query_wraps_single_cte_in_anchor --lib
cargo test test_plan_query_builds_nested_anchor_chain --lib
cargo build
```

Expected:
- both planner tests pass
- `cargo build` still succeeds after the new logical variants are added

- [ ] **Step 7: Commit the planner structure change**

```bash
git add src/sql/plan/mod.rs src/sql/planner/mod.rs src/sql/explain.rs src/sql/optimizer/mod.rs src/sql/optimizer/cardinality.rs src/sql/optimizer/column_pruning.rs src/sql/optimizer/expr_utils.rs src/sql/optimizer/join_reorder.rs src/sql/physical/emitter/mod.rs src/standalone/engine.rs
git commit -m "refactor(sql): plan CTEs as anchor-wrapped logical trees"
```

---

### Task 3: Teach Cascades About `CTEAnchor`

**Files:**
- Modify: `src/sql/cascades/operator.rs`
- Modify: `src/sql/cascades/convert.rs`
- Modify: `src/sql/cascades/rules/implement.rs`
- Modify: `src/sql/cascades/rules/mod.rs`
- Modify: `src/sql/cascades/stats.rs`
- Modify: `src/sql/cascades/search.rs`
- Modify: `src/sql/cascades/cost.rs`
- Modify: `src/sql/cascades/extract.rs`
- Test: `src/sql/cascades/convert.rs`

- [ ] **Step 1: Add a failing Memo conversion test for `CTEAnchor`**

Append this test to the existing `#[cfg(test)] mod tests` in `src/sql/cascades/convert.rs`:

```rust
    #[test]
    fn test_cte_anchor_to_memo() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: dummy_table_def(),
            alias: None,
            columns: dummy_output_columns(),
            predicates: vec![],
            required_columns: None,
        });

        let produce = LogicalPlan::CTEProduce(crate::sql::plan::CTEProduceNode {
            cte_id: 7,
            input: Box::new(scan.clone()),
            output_columns: dummy_output_columns(),
        });

        let consume = LogicalPlan::CTEConsume(crate::sql::plan::CTEConsumeNode {
            cte_id: 7,
            alias: "t".to_string(),
            output_columns: dummy_output_columns(),
        });

        let anchor = LogicalPlan::CTEAnchor(crate::sql::plan::CTEAnchorNode {
            cte_id: 7,
            produce: Box::new(produce),
            consumer: Box::new(consume),
        });

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&anchor, &mut memo);

        assert_eq!(gid, 2);
        assert!(matches!(
            memo.groups[2].logical_exprs[0].op,
            Operator::LogicalCTEAnchor(_)
        ));
        assert_eq!(memo.groups[2].logical_exprs[0].children, vec![1, 0]);
    }
```

- [ ] **Step 2: Run the convert test and verify it fails because `LogicalCTEAnchor` does not exist**

Run: `cargo test test_cte_anchor_to_memo --lib`

Expected:
- compilation fails because `LogicalCTEAnchor` / `PhysicalCTEAnchor` are not defined

- [ ] **Step 3: Add logical and physical `CTEAnchor` operators**

In `src/sql/cascades/operator.rs`, add:

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEAnchorOp {
    pub cte_id: CteId,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalCTEAnchorOp {
    pub cte_id: CteId,
}
```

Insert the enum variants:

```rust
    LogicalCTEAnchor(LogicalCTEAnchorOp),
    LogicalCTEProduce(LogicalCTEProduceOp),
    LogicalCTEConsume(LogicalCTEConsumeOp),
```

and:

```rust
    PhysicalCTEAnchor(PhysicalCTEAnchorOp),
    PhysicalCTEProduce(PhysicalCTEProduceOp),
    PhysicalCTEConsume(PhysicalCTEConsumeOp),
```

- [ ] **Step 4: Support `CTEAnchor` in convert, implement, stats, search, cost, and extract**

Add these exact blocks:

`src/sql/cascades/convert.rs`
```rust
        LogicalPlan::CTEAnchor(node) => {
            let produce = logical_plan_to_memo(&node.produce, memo);
            let consumer = logical_plan_to_memo(&node.consumer, memo);
            let expr = MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalCTEAnchor(LogicalCTEAnchorOp {
                    cte_id: node.cte_id,
                }),
                children: vec![produce, consumer],
            };
            memo.new_group(expr)
        }
```

`src/sql/cascades/rules/implement.rs`
```rust
pub(crate) struct CTEAnchorToPhysical;

impl Rule for CTEAnchorToPhysical {
    fn name(&self) -> &str { "CTEAnchorToPhysical" }
    fn rule_type(&self) -> RuleType { RuleType::Implementation }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEAnchor(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEAnchor(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: op.cte_id }),
            children: expr.children.clone(),
        }]
    }
}
```

Register it in `src/sql/cascades/rules/mod.rs` before `CTEProduceToPhysical`.

`src/sql/cascades/stats.rs`
```rust
        Operator::LogicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),
        Operator::PhysicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),
```

and in the output-column helper:

```rust
        Operator::LogicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::PhysicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
```

`src/sql/cascades/search.rs`
```rust
        Operator::PhysicalCTEAnchor(_) => derive_child_properties(memo, expr, required, 1),
```

and in output-property derivation:

```rust
        Operator::PhysicalCTEAnchor(_) => child_output_property(ctx, memo, expr, 1, required)?,
```

`src/sql/cascades/cost.rs`
```rust
        | Operator::LogicalCTEAnchor(_)
        | Operator::PhysicalCTEAnchor(_) => 0.0,
```

`src/sql/cascades/extract.rs`
```rust
        Operator::PhysicalCTEAnchor(op) => PhysicalPlanNode {
            op: Operator::PhysicalCTEAnchor(op.clone()),
            children: vec![
                extract_group_child(memo, expr, 0, winners, required)?,
                extract_group_child(memo, expr, 1, winners, required)?,
            ],
            stats: best_stats.clone(),
            output_columns: best_output_columns.clone(),
        },
```

- [ ] **Step 5: Run the CTEAnchor convert test and a full build**

Run:
- `cargo test test_cte_anchor_to_memo --lib`
- `cargo build`

Expected:
- the convert test passes
- `cargo build` succeeds with the new operator wired through Cascades

- [ ] **Step 6: Commit the Cascades structural support**

```bash
git add src/sql/cascades/operator.rs src/sql/cascades/convert.rs src/sql/cascades/rules/implement.rs src/sql/cascades/rules/mod.rs src/sql/cascades/stats.rs src/sql/cascades/search.rs src/sql/cascades/cost.rs src/sql/cascades/extract.rs
git commit -m "feat(cascades): add CTEAnchor logical and physical operators"
```

---

### Task 4: Inline Single-Use CTEs Before Memo Conversion

**Files:**
- Create: `src/sql/cascades/cte_rewrite.rs`
- Modify: `src/sql/cascades/mod.rs`
- Test: `src/sql/cascades/cte_rewrite.rs`

- [ ] **Step 1: Create failing tests for `collect_cte_counts()` and `inline_single_use_ctes()`**

Create `src/sql/cascades/cte_rewrite.rs` with this test-first scaffold:

```rust
use crate::sql::cte::CteId;
use crate::sql::plan::{CTEAnchorNode, CTEConsumeNode, CTEProduceNode, LogicalPlan, ScanNode};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub(crate) struct CTEContext {
    pub produces: HashSet<CteId>,
    pub consume_count: HashMap<CteId, usize>,
}

pub(crate) fn collect_cte_counts(_plan: &LogicalPlan) -> CTEContext {
    panic!("collect_cte_counts is not implemented in this test scaffold")
}

pub(crate) fn inline_single_use_ctes(_plan: LogicalPlan, _ctx: &CTEContext) -> LogicalPlan {
    panic!("inline_single_use_ctes is not implemented in this test scaffold")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::OutputColumn;
    use arrow::datatypes::DataType;
    use std::path::PathBuf;

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t1".to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                }],
                storage: TableStorage::LocalParquetFile {
                    path: PathBuf::from("/tmp/t1.parquet"),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        })
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 1,
                alias: "t".to_string(),
                output_columns: output_columns(),
            })),
        });

        let ctx = collect_cte_counts(&plan);
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 1,
                alias: "t".to_string(),
                output_columns: output_columns(),
            })),
        });

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx);
        assert!(matches!(rewritten, LogicalPlan::Scan(_)));
    }
}
```

- [ ] **Step 2: Run the new tests and confirm the scaffold panics before implementation**

Run:

```bash
cargo test test_collect_cte_counts_counts_consumes --lib
cargo test test_inline_single_use_cte_removes_anchor --lib
```

Expected:
- both tests fail with the explicit scaffold panic messages

- [ ] **Step 3: Implement the counting pass and the single-use rewrite**

Replace the stubs in `src/sql/cascades/cte_rewrite.rs` with:

```rust
pub(crate) fn collect_cte_counts(plan: &LogicalPlan) -> CTEContext {
    fn visit(plan: &LogicalPlan, ctx: &mut CTEContext) {
        match plan {
            LogicalPlan::CTEAnchor(node) => {
                ctx.produces.insert(node.cte_id);
                visit(&node.produce, ctx);
                visit(&node.consumer, ctx);
            }
            LogicalPlan::CTEProduce(node) => visit(&node.input, ctx),
            LogicalPlan::CTEConsume(node) => {
                *ctx.consume_count.entry(node.cte_id).or_insert(0) += 1;
            }
            LogicalPlan::Filter(node) => visit(&node.input, ctx),
            LogicalPlan::Project(node) => visit(&node.input, ctx),
            LogicalPlan::Aggregate(node) => visit(&node.input, ctx),
            LogicalPlan::Sort(node) => visit(&node.input, ctx),
            LogicalPlan::Limit(node) => visit(&node.input, ctx),
            LogicalPlan::Window(node) => visit(&node.input, ctx),
            LogicalPlan::SubqueryAlias(node) => visit(&node.input, ctx),
            LogicalPlan::Repeat(node) => visit(&node.input, ctx),
            LogicalPlan::Join(node) => {
                visit(&node.left, ctx);
                visit(&node.right, ctx);
            }
            LogicalPlan::Union(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::Intersect(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::Except(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {}
        }
    }

    let mut ctx = CTEContext::default();
    visit(plan, &mut ctx);
    ctx
}

pub(crate) fn inline_single_use_ctes(plan: LogicalPlan, ctx: &CTEContext) -> LogicalPlan {
    match plan {
        LogicalPlan::CTEAnchor(node)
            if ctx.consume_count.get(&node.cte_id).copied().unwrap_or(0) <= 1 =>
        {
            let produce_input = match *node.produce {
                LogicalPlan::CTEProduce(produce) => *produce.input,
                other => other,
            };
            let consumer = replace_cte_consume(*node.consumer, node.cte_id, &produce_input);
            inline_single_use_ctes(consumer, ctx)
        }
        LogicalPlan::CTEAnchor(node) => LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: node.cte_id,
            produce: Box::new(inline_single_use_ctes(*node.produce, ctx)),
            consumer: Box::new(inline_single_use_ctes(*node.consumer, ctx)),
        }),
        LogicalPlan::CTEProduce(node) => LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            output_columns: node.output_columns,
        }),
        LogicalPlan::Filter(node) => LogicalPlan::Filter(crate::sql::plan::FilterNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            predicate: node.predicate,
        }),
        LogicalPlan::Project(node) => LogicalPlan::Project(crate::sql::plan::ProjectNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            items: node.items,
        }),
        LogicalPlan::Aggregate(node) => LogicalPlan::Aggregate(crate::sql::plan::AggregateNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Sort(node) => LogicalPlan::Sort(crate::sql::plan::SortNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            items: node.items,
        }),
        LogicalPlan::Limit(node) => LogicalPlan::Limit(crate::sql::plan::LimitNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            limit: node.limit,
            offset: node.offset,
        }),
        LogicalPlan::Window(node) => LogicalPlan::Window(crate::sql::plan::WindowNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
        }),
        LogicalPlan::SubqueryAlias(node) => {
            LogicalPlan::SubqueryAlias(crate::sql::plan::SubqueryAliasNode {
                input: Box::new(inline_single_use_ctes(*node.input, ctx)),
                alias: node.alias,
                output_columns: node.output_columns,
            })
        }
        LogicalPlan::Repeat(node) => LogicalPlan::Repeat(crate::sql::plan::RepeatPlanNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_fn_args: node.grouping_fn_args,
        }),
        LogicalPlan::Join(node) => LogicalPlan::Join(crate::sql::plan::JoinNode {
            left: Box::new(inline_single_use_ctes(*node.left, ctx)),
            right: Box::new(inline_single_use_ctes(*node.right, ctx)),
            join_type: node.join_type,
            condition: node.condition,
        }),
        LogicalPlan::Union(node) => LogicalPlan::Union(crate::sql::plan::UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(crate::sql::plan::IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(crate::sql::plan::ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
        }),
        LogicalPlan::CTEConsume(node) => LogicalPlan::CTEConsume(node),
        LogicalPlan::Scan(node) => LogicalPlan::Scan(node),
        LogicalPlan::Values(node) => LogicalPlan::Values(node),
        LogicalPlan::GenerateSeries(node) => LogicalPlan::GenerateSeries(node),
    }
}

fn replace_cte_consume(plan: LogicalPlan, cte_id: CteId, replacement: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::CTEConsume(node) if node.cte_id == cte_id => replacement.clone(),
        LogicalPlan::Filter(node) => LogicalPlan::Filter(crate::sql::plan::FilterNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            predicate: node.predicate,
        }),
        LogicalPlan::Project(node) => LogicalPlan::Project(crate::sql::plan::ProjectNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            items: node.items,
        }),
        LogicalPlan::Aggregate(node) => LogicalPlan::Aggregate(crate::sql::plan::AggregateNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Sort(node) => LogicalPlan::Sort(crate::sql::plan::SortNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            items: node.items,
        }),
        LogicalPlan::Limit(node) => LogicalPlan::Limit(crate::sql::plan::LimitNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            limit: node.limit,
            offset: node.offset,
        }),
        LogicalPlan::Window(node) => LogicalPlan::Window(crate::sql::plan::WindowNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
        }),
        LogicalPlan::SubqueryAlias(node) => {
            LogicalPlan::SubqueryAlias(crate::sql::plan::SubqueryAliasNode {
                input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
                alias: node.alias,
                output_columns: node.output_columns,
            })
        }
        LogicalPlan::Repeat(node) => LogicalPlan::Repeat(crate::sql::plan::RepeatPlanNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_fn_args: node.grouping_fn_args,
        }),
        LogicalPlan::Join(node) => LogicalPlan::Join(crate::sql::plan::JoinNode {
            left: Box::new(replace_cte_consume(*node.left, cte_id, replacement)),
            right: Box::new(replace_cte_consume(*node.right, cte_id, replacement)),
            join_type: node.join_type,
            condition: node.condition,
        }),
        LogicalPlan::Union(node) => LogicalPlan::Union(crate::sql::plan::UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(crate::sql::plan::IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(crate::sql::plan::ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
        }),
        other => other,
    }
}
```

- [ ] **Step 4: Hook the rewrite into `cascades::optimize()` before Memo conversion**

In `src/sql/cascades/mod.rs`, add the module and call:

```rust
pub(crate) mod cte_rewrite;
```

Then update `optimize()`:

```rust
    let rewritten = rewriter::rewrite(plan, table_stats);
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);

    let mut memo = Memo::new();
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);
```

- [ ] **Step 5: Run the rewrite tests and a focused build**

Run:

```bash
cargo test test_collect_cte_counts_counts_consumes --lib
cargo test test_inline_single_use_cte_removes_anchor --lib
cargo build
```

Expected:
- both rewrite tests pass
- `cargo build` succeeds with the new pre-Memo rewrite wired in

- [ ] **Step 6: Commit the CTE rewrite pass**

```bash
git add src/sql/cascades/cte_rewrite.rs src/sql/cascades/mod.rs
git commit -m "feat(cascades): inline single-use CTEs before memo conversion"
```

---

### Task 5: Build Multi-Use CTE Fragments and Route All Queries Through the New Path

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/standalone/engine.rs`
- Test: `src/standalone/engine.rs`

- [ ] **Step 1: Add failing embedded-engine tests for single-use inline and multi-use reuse**

Append these tests to the existing `mod tests` in `src/standalone/engine.rs`:

```rust
    #[test]
    fn embedded_query_executes_single_use_cte_through_cascades() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine.register_parquet_table("tbl", parquet.path()).expect("register table");

        let session = engine.session();
        let result = session
            .query("WITH t AS (SELECT id, name FROM tbl WHERE id >= 2) SELECT name FROM t ORDER BY 1")
            .expect("execute query");

        assert_eq!(result.row_count(), 2);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "name");
    }

    #[test]
    fn embedded_query_executes_multi_use_cte_through_multicast_reuse() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine.register_parquet_table("tbl", parquet.path()).expect("register table");

        let session = engine.session();
        let result = session
            .query("WITH t AS (SELECT id FROM tbl) \
                    SELECT a.id FROM t a JOIN t b ON a.id = b.id ORDER BY 1")
            .expect("execute query");

        assert_eq!(result.row_count(), 3);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "id");
    }
```

- [ ] **Step 2: Run the new embedded tests and capture the current failure on the existing CTE execution branch**

Run:

```bash
cargo test embedded_query_executes_single_use_cte_through_cascades --lib
cargo test embedded_query_executes_multi_use_cte_through_multicast_reuse --lib
```

Expected:
- one or both tests fail because `execute_query()` still sends CTE queries down the old path

- [ ] **Step 3: Add `visit_cte_anchor()` and align `visit_cte_produce()` / `visit_cte_consume()`**

In `src/sql/cascades/fragment_builder.rs`, extend the dispatcher:

```rust
            Operator::PhysicalCTEAnchor(op) => self.visit_cte_anchor(op, node),
            Operator::PhysicalCTEProduce(op) => self.visit_cte_produce(op, node),
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op),
```

Add the new visitor:

```rust
    fn visit_cte_anchor(
        &mut self,
        _op: &crate::sql::cascades::operator::PhysicalCTEAnchorOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let _ = self.visit(&node.children[0])?;
        self.visit(&node.children[1])
    }
```

Update `visit_cte_produce()` so the completed fragment keeps `output_columns` and does not synthesize an exchange node:

```rust
        let cte_fragment = FragmentBuildResult {
            fragment_id: cte_fragment_id,
            plan: plan_nodes::TPlan::new(child.plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
            output_sink: build_noop_sink(),
            output_columns: op
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: Some(op.cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let idx = self.completed_fragments.len();
        self.completed_fragments.push(cte_fragment);
        self.cte_fragments.insert(op.cte_id, idx);

        Ok(VisitResult {
            plan_nodes: Vec::new(),
            scope: child.scope,
            tuple_ids: child.tuple_ids,
        })
```

Leave `visit_cte_consume()` as the exchange-only consumer, but keep the `cte_exchange_nodes.push((op.cte_id, exchange_node_id));` call exactly where it is.

- [ ] **Step 4: Unify `explain_query()` and `execute_query()` to always use `plan_query() -> cascades::optimize()`**

In `src/standalone/engine.rs`, replace the CTE/non-CTE branch in `explain_query()` with:

```rust
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;
    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::cascades::optimize(logical, &table_stats)?;

    if matches!(level, ExplainLevel::Costs) {
        for (table, stats) in &table_stats {
            lines.push(format!("  Statistics: {table} row_count={}", stats.row_count));
        }
    }
    lines.extend(explain_physical_plan(&physical, level));
```

Replace the top of `execute_query()` with:

```rust
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;

    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::cascades::optimize(logical, &table_stats)?;
    let build_result = crate::sql::cascades::fragment_builder::PlanFragmentBuilder::build(
        &physical,
        catalog,
        current_database,
    )?;
```

Delete the old `if cte_registry.entries.is_empty() { ... } else { ... }` branch entirely.

Also extend `build_table_stats_from_plan()` in the same file with:

```rust
        LogicalPlan::CTEAnchor(node) => {
            stats.extend(build_table_stats_from_plan(&node.produce));
            stats.extend(build_table_stats_from_plan(&node.consumer));
        }
        LogicalPlan::CTEProduce(node) => {
            stats.extend(build_table_stats_from_plan(&node.input));
        }
```

- [ ] **Step 5: Make physical explain show `CTE ANCHOR`**

In `src/sql/explain.rs`, add:

```rust
        Operator::PhysicalCTEAnchor(op) => {
            out.push(format!("{pad}CTE ANCHOR (cte_id={}){costs_suffix}", op.cte_id));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
```

Keep the existing `PhysicalCTEProduce` and `PhysicalCTEConsume` blocks unchanged.

- [ ] **Step 6: Run the embedded CTE tests and a focused build**

Run:

```bash
cargo test embedded_query_executes_single_use_cte_through_cascades --lib
cargo test embedded_query_executes_multi_use_cte_through_multicast_reuse --lib
cargo build
```

Expected:
- both embedded CTE tests pass
- `cargo build` succeeds with the unified CTE execution path

- [ ] **Step 7: Commit the unified execution path**

```bash
git add src/sql/cascades/fragment_builder.rs src/sql/explain.rs src/standalone/engine.rs
git commit -m "feat(cascades): route CTE queries through the unified fragment builder path"
```

---

### Task 6: Remove the Old CTE Main-Path Dependency and Run Verification

**Files:**
- Modify: `src/standalone/engine.rs`
- Test: `src/standalone/engine.rs`

- [ ] **Step 1: Make the old CTE path explicitly dead on the main query path**

In `src/standalone/engine.rs`, delete the old imports/usages of:

```rust
crate::sql::optimizer::optimize_query_plan
crate::sql::fragment::plan_fragments
crate::sql::physical::emit_multi_fragment
```

Then add this regression comment above the unified path:

```rust
    // All non-recursive queries, including CTE queries, now flow through
    // planner -> cascades -> fragment builder. The legacy QueryPlan/FragmentPlan
    // path remains only for compile-time compatibility and should not be used here.
```

- [ ] **Step 2: Add one end-to-end regression test that covers physical explain for a reused CTE**

Append this test to `src/standalone/engine.rs`:

```rust
    #[test]
    fn embedded_query_explain_for_multi_use_cte_shows_physical_cte_nodes() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine.register_parquet_table("tbl", parquet.path()).expect("register table");

        let session = engine.session();
        let explain = session
            .query("EXPLAIN WITH t AS (SELECT id FROM tbl) SELECT a.id FROM t a JOIN t b ON a.id = b.id")
            .expect("execute explain");

        assert!(explain.row_count() > 0);
        let text = explain
            .chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk.batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("string array");
                (0..arr.len()).map(|idx| arr.value(idx).to_string()).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("CTE ANCHOR"), "text={text}");
        assert!(text.contains("CTE PRODUCE"), "text={text}");
        assert!(text.contains("CTE CONSUME"), "text={text}");
    }
```

- [ ] **Step 3: Run the focused regression suite**

Run:

```bash
cargo test test_single_use_cte_is_still_registered --lib
cargo test test_forward_cte_reference_is_rejected --lib
cargo test test_plan_query_wraps_single_cte_in_anchor --lib
cargo test test_plan_query_builds_nested_anchor_chain --lib
cargo test test_cte_anchor_to_memo --lib
cargo test test_collect_cte_counts_counts_consumes --lib
cargo test test_inline_single_use_cte_removes_anchor --lib
cargo test embedded_query_executes_single_use_cte_through_cascades --lib
cargo test embedded_query_executes_multi_use_cte_through_multicast_reuse --lib
cargo test embedded_query_explain_for_multi_use_cte_shows_physical_cte_nodes --lib
```

Expected:
- all targeted analyzer / planner / cascades / engine tests pass

- [ ] **Step 4: Run the core code-quality commands**

Run:
- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build`

Expected:
- all commands pass cleanly

- [ ] **Step 5: Run one SQL regression suite that exercises CTEs**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpch --mode verify
```

Expected:
- the suite completes without new CTE regressions
- if any existing unrelated failures remain, record them separately before merging

- [ ] **Step 6: Commit the cleanup and verification pass**

```bash
git add src/standalone/engine.rs
git commit -m "chore(cascades): remove old CTE main-path dependency and verify alignment"
```

---

## Self-Review Checklist

### Spec Coverage

- Analyzer now registers all non-recursive CTE definitions: Task 1
- Planner emits `CTEAnchor / CTEProduce / CTEConsume` single tree: Task 2
- Cascades supports `CTEAnchor`: Task 3
- Inline single-use / reuse multi-use: Task 4
- FragmentBuilder emits multicast producer and exchange consumers: Task 5
- Explain distinguishes inline vs reuse and main path no longer branches on CTE: Tasks 5-6

### Placeholder Scan

- No placeholder markers remain
- Every code-modifying step includes an explicit code block
- Every run step includes an exact command and expected output

### Type Consistency

- `CTEAnchorNode`, `CTEProduceNode`, and `CTEConsumeNode` are introduced in Task 2 and reused consistently in Tasks 3-5
- `CTEContext`, `collect_cte_counts()`, and `inline_single_use_ctes()` are introduced in Task 4 and reused consistently in later tasks
- `PhysicalCTEAnchor` is introduced in Task 3 before `visit_cte_anchor()` uses it in Task 5

Plan complete and saved to `docs/superpowers/plans/2026-04-03-cascades-cte-alignment.md`. Two execution options:

1. Subagent-Driven (recommended) - I dispatch a fresh subagent per task, review between tasks, fast iteration

2. Inline Execution - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
