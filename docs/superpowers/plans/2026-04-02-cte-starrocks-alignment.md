# CTE StarRocks Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align NovaRocks CTE planning/execution with the StarRocks model by introducing `CTEAnchor / CTEProduce / CTEConsume` into the unified Cascades path and making CTE producer fragments own multicast destinations.

**Architecture:** Add explicit CTE anchor/produce nodes to the logical and physical plan layers, then route CTE queries through the same `planner -> cascades::optimize -> fragment_builder -> coordinator` pipeline used by non-CTE queries. Replace the current `QueryPlan { cte_plans, main_plan }` execution split with a transitional adapter that builds a single logical tree, and tighten fragment ownership so producer fragments behave like StarRocks-style multicast sources while consumers are exchange receivers.

**Tech Stack:** Rust, existing SQL analyzer/planner modules, Cascades memo/physical plan modules, existing `FragmentBuildResult` / `ExecutionCoordinator`, existing `sql-tests` runner

---

## File Map

### New files

| File | Responsibility |
|------|----------------|
| `src/sql/cascades/cte_adapter.rs` | Transitional `QueryPlan -> LogicalPlan` adapter that builds nested `CTEAnchor / CTEProduce` trees for shared CTEs |

### Modified files

| File | Change |
|------|--------|
| `src/sql/plan/mod.rs` | Add `LogicalPlan::CTEAnchor`, `LogicalPlan::CTEProduce`, node structs, and keep `QueryPlan` as transitional planner output |
| `src/sql/planner/mod.rs` | Add adapter-facing helper/tests around `plan_query()` output shape; later expose unified logical tree entry |
| `src/sql/cascades/mod.rs` | Export CTE adapter and keep optimize entry unified |
| `src/sql/cascades/operator.rs` | Add `LogicalCTEAnchorOp` and `PhysicalCTEAnchorOp` |
| `src/sql/cascades/convert.rs` | Convert `LogicalPlan::CTEAnchor` / `CTEProduce` into memo groups |
| `src/sql/cascades/rules/implement.rs` | Add `CTEAnchorToPhysical` implementation rule |
| `src/sql/cascades/rules/mod.rs` | Register `CTEAnchorToPhysical` alongside existing CTE rules |
| `src/sql/cascades/search.rs` | Define anchor property behavior and child requirements |
| `src/sql/cascades/extract.rs` | Extract `PhysicalCTEAnchor` from winners |
| `src/sql/cascades/stats.rs` | Derive anchor outputs and replace fixed CTE consume row-count fallback with producer-linked stats and duplicate-producer fail-fast checks |
| `src/sql/cascades/fragment_builder.rs` | Introduce explicit producer-owned multicast destination model and `visit_cte_anchor()` ordering |
| `src/sql/cascades/physical_plan.rs` | No schema change expected, but tests may be updated for new operators |
| `src/sql/explain.rs` | Explain logical/physical CTE anchor nodes and stop relying on `QueryPlan` for CTE explain |
| `src/standalone/engine.rs` | Route non-recursive shared CTE `execute_query` / `explain_query` through Cascades while preserving the existing recursive CTE path |
| `src/standalone/coordinator.rs` | Prefer producer-owned multicast destination metadata while retaining legacy root-fragment patch-tuple fallback for the old emitter path |
| `src/sql/physical/mod.rs` | Extend fragment build result metadata with producer-owned destinations without breaking the legacy emitter path |
| `src/sql/physical/emitter/mod.rs` | Keep the old multi-fragment emitter compiling by populating transitional CTE metadata fields during the migration |
| `sql-tests/cte/sql/cte_starrocks_alignment.sql` | Regression suite for shared non-recursive CTE execution/explain cases on the new path |
| `sql-tests/cte/result/cte_starrocks_alignment.result` | Expected results for the new CTE alignment suite |

### Existing files to read before implementation

| File | Why |
|------|-----|
| `docs/superpowers/specs/2026-04-02-cte-starrocks-alignment-design.md` | Source of truth for scope, architecture, and acceptance criteria |
| `src/sql/plan/mod.rs` | Current logical plan / `QueryPlan` layout |
| `src/sql/planner/mod.rs` | Current `plan()` / `plan_query()` behavior |
| `src/sql/cascades/operator.rs` | Existing CTE produce/consume operators |
| `src/sql/cascades/fragment_builder.rs` | Existing multicast/exchange wiring skeleton |
| `src/standalone/engine.rs` | Current CTE vs non-CTE branching |
| `src/standalone/coordinator.rs` | Current multicast wiring assumptions |
| `sql-tests/cte/sql/cte_in_where_subquery.sql` | Existing non-recursive CTE suite patterns |
| `sql-tests/limit/sql/limit_offset_basic.sql` | Existing EXPLAIN + multicast-style CTE coverage pattern |

---

### Task 1: Add logical `CTEAnchor / CTEProduce` nodes and adapter coverage

**Files:**
- Modify: `src/sql/plan/mod.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/column_pruning.rs`
- Modify: `src/sql/optimizer/cardinality.rs`
- Modify: `src/sql/optimizer/expr_utils.rs`
- Modify: `src/sql/optimizer/join_reorder.rs`
- Modify: `src/sql/physical/emitter/mod.rs`
- Modify: `src/sql/explain.rs`
- Create: `src/sql/cascades/cte_adapter.rs`
- Modify: `src/sql/cascades/mod.rs`
- Test: `src/sql/planner/mod.rs`

- [ ] **Step 1: Write the failing planner/adapter tests**

Add a `#[cfg(test)] mod tests` block at the bottom of `src/sql/planner/mod.rs` (or extend an existing one if present) with fixtures that build a `QueryPlan` and assert the adapter returns a nested anchor tree:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::cte_adapter::build_cte_logical_tree;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::OutputColumn;
    use crate::sql::plan::{
        CTEAnchorNode, CTEConsumeNode, CTEProduceNode, LogicalPlan, QueryPlan, ScanNode,
    };
    use arrow::datatypes::DataType;
    use std::path::PathBuf;

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: vec![ColumnDef {
                    name: "c1".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                }],
                storage: TableStorage::LocalParquetFile {
                    path: PathBuf::from(format!("/tmp/{name}.parquet")),
                },
            },
            alias: None,
            columns: output_columns(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn build_cte_logical_tree_wraps_main_plan_with_anchor_and_produce() {
        let query_plan = QueryPlan {
            cte_plans: vec![crate::sql::plan::CTEProducePlan {
                cte_id: 7,
                plan: scan("cte_source"),
                output_columns: output_columns(),
            }],
            main_plan: LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 7,
                alias: "c".to_string(),
                output_columns: output_columns(),
            }),
            output_columns: output_columns(),
        };

        let logical = build_cte_logical_tree(query_plan).expect("adapter should build CTE tree");

        match logical {
            LogicalPlan::CTEAnchor(CTEAnchorNode { cte_id, left, right }) => {
                assert_eq!(cte_id, 7);
                assert!(matches!(
                    *left,
                    LogicalPlan::CTEProduce(CTEProduceNode { cte_id: 7, .. })
                ));
                assert!(matches!(*right, LogicalPlan::CTEConsume(CTEConsumeNode { cte_id: 7, .. })));
            }
            other => panic!("expected CTEAnchor root, got {:?}", other),
        }
    }

    #[test]
    fn build_cte_logical_tree_nests_multiple_ctes_in_registration_order() {
        let query_plan = QueryPlan {
            cte_plans: vec![
                crate::sql::plan::CTEProducePlan {
                    cte_id: 1,
                    plan: scan("cte1"),
                    output_columns: output_columns(),
                },
                crate::sql::plan::CTEProducePlan {
                    cte_id: 2,
                    plan: scan("cte2"),
                    output_columns: output_columns(),
                },
            ],
            main_plan: LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 2,
                alias: "c2".to_string(),
                output_columns: output_columns(),
            }),
            output_columns: output_columns(),
        };

        let logical = build_cte_logical_tree(query_plan).expect("adapter should nest anchors");

        match logical {
            LogicalPlan::CTEAnchor(outer) => {
                assert_eq!(outer.cte_id, 1);
                match *outer.right {
                    LogicalPlan::CTEAnchor(inner) => {
                        assert_eq!(inner.cte_id, 2);
                    }
                    other => panic!("expected nested anchor, got {:?}", other),
                }
            }
            other => panic!("expected outer anchor, got {:?}", other),
        }
    }
}
```

- [ ] **Step 2: Run the planner/adapter tests to verify they fail**

Run:

```bash
cargo test build_cte_logical_tree_wraps_main_plan_with_anchor_and_produce --lib
cargo test build_cte_logical_tree_nests_multiple_ctes_in_registration_order --lib
```

Expected: FAIL with unresolved imports / missing `CTEAnchorNode`, `CTEProduceNode`, or missing `build_cte_logical_tree`.

- [ ] **Step 3: Add logical node structs in `src/sql/plan/mod.rs`**

Insert the new logical variants and node structs:

```rust
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Intersect(IntersectNode),
    Except(ExceptNode),
    Values(ValuesNode),
    GenerateSeries(GenerateSeriesNode),
    Window(WindowNode),
    SubqueryAlias(SubqueryAliasNode),
    Repeat(RepeatPlanNode),
    CTEAnchor(CTEAnchorNode),
    CTEProduce(CTEProduceNode),
    CTEConsume(CTEConsumeNode),
}

#[derive(Clone, Debug)]
pub(crate) struct CTEAnchorNode {
    pub cte_id: crate::sql::cte::CteId,
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProduceNode {
    pub cte_id: crate::sql::cte::CteId,
    pub input: Box<LogicalPlan>,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}
```

Keep `QueryPlan` and `CTEProducePlan` in place for the transitional adapter.

- [ ] **Step 4: Implement the transitional adapter**

Create `src/sql/cascades/cte_adapter.rs`:

```rust
use crate::sql::plan::{
    CTEAnchorNode, CTEProduceNode, LogicalPlan, QueryPlan,
};

pub(crate) fn build_cte_logical_tree(query_plan: QueryPlan) -> Result<LogicalPlan, String> {
    let mut current = query_plan.main_plan;

    for cte in query_plan.cte_plans.into_iter().rev() {
        current = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: cte.cte_id,
            left: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: cte.cte_id,
                input: Box::new(cte.plan),
                output_columns: cte.output_columns,
            })),
            right: Box::new(current),
        });
    }

    Ok(current)
}
```

Export it from `src/sql/cascades/mod.rs`:

```rust
pub(crate) mod cte_adapter;
```

- [ ] **Step 5: Make the new logical variants compile across existing logical-plan visitors**

Update every exhaustive `match` that currently knows about `CTEConsume` only, so Task 1 can compile independently before any physical anchor work starts. At minimum, patch:

- `src/sql/optimizer/mod.rs::map_children()`
- `src/sql/optimizer/column_pruning.rs::prune_inner()`
- `src/sql/optimizer/cardinality.rs::estimate_statistics()`
- `src/sql/optimizer/expr_utils.rs`
- `src/sql/optimizer/join_reorder.rs`
- `src/sql/physical/emitter/mod.rs::emit_node()`
- `src/sql/explain.rs::format_node()`

Use the same structural rules consistently:

```rust
LogicalPlan::CTEAnchor(node) => LogicalPlan::CTEAnchor(CTEAnchorNode {
    cte_id: node.cte_id,
    left: Box::new(f(*node.left)),
    right: Box::new(f(*node.right)),
}),
LogicalPlan::CTEProduce(node) => LogicalPlan::CTEProduce(CTEProduceNode {
    cte_id: node.cte_id,
    input: Box::new(f(*node.input)),
    output_columns: node.output_columns,
}),
LogicalPlan::CTEConsume(node) => LogicalPlan::CTEConsume(node),
```

For old logical-only utilities that should never look through shared CTE boundaries, treat `CTEProduce` like a unary passthrough and `CTEAnchor` as “recurse both sides, return right-side semantics” rather than inventing a new optimizer branch.

- [ ] **Step 6: Run the planner/adapter tests to verify they pass**

Run:

```bash
cargo test build_cte_logical_tree_wraps_main_plan_with_anchor_and_produce --lib
cargo test build_cte_logical_tree_nests_multiple_ctes_in_registration_order --lib
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/plan/mod.rs src/sql/planner/mod.rs src/sql/optimizer/mod.rs src/sql/optimizer/column_pruning.rs src/sql/optimizer/cardinality.rs src/sql/optimizer/expr_utils.rs src/sql/optimizer/join_reorder.rs src/sql/physical/emitter/mod.rs src/sql/explain.rs src/sql/cascades/mod.rs src/sql/cascades/cte_adapter.rs
git commit -m "feat(sql): add CTE anchor/produce logical nodes"
```

---

### Task 2: Add logical/physical `CTEAnchor` support throughout Cascades

**Files:**
- Modify: `src/sql/cascades/operator.rs`
- Modify: `src/sql/cascades/convert.rs`
- Modify: `src/sql/cascades/rules/implement.rs`
- Modify: `src/sql/cascades/rules/mod.rs`
- Modify: `src/sql/cascades/search.rs`
- Modify: `src/sql/cascades/extract.rs`
- Modify: `src/sql/cascades/stats.rs`
- Modify: `src/sql/cascades/cost.rs`
- Test: `src/sql/cascades/convert.rs`
- Test: `src/sql/cascades/search.rs`

- [ ] **Step 1: Write the failing convert/search tests**

Extend `src/sql/cascades/convert.rs` tests with:

```rust
#[test]
fn test_cte_anchor_to_memo_creates_binary_group() {
    let child_cols = dummy_output_columns();
    let plan = LogicalPlan::CTEAnchor(crate::sql::plan::CTEAnchorNode {
        cte_id: 9,
        left: Box::new(LogicalPlan::CTEProduce(crate::sql::plan::CTEProduceNode {
            cte_id: 9,
            input: Box::new(LogicalPlan::Scan(ScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: child_cols.clone(),
                predicates: vec![],
                required_columns: None,
            })),
            output_columns: child_cols.clone(),
        })),
        right: Box::new(LogicalPlan::CTEConsume(crate::sql::plan::CTEConsumeNode {
            cte_id: 9,
            alias: "c".to_string(),
            output_columns: child_cols,
        })),
    });

    let mut memo = Memo::new();
    let gid = logical_plan_to_memo(&plan, &mut memo);
    let expr = &memo.groups[gid].logical_exprs[0];
    assert_eq!(expr.children.len(), 2);
    assert!(matches!(expr.op, Operator::LogicalCTEAnchor(_)));
}
```

Extend `src/sql/cascades/search.rs` tests with:

```rust
#[test]
fn cte_anchor_requires_left_then_right_children() {
    let required = PhysicalPropertySet::gather();
    let child_reqs = required_input_properties(
        &Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: 1 }),
        &required,
    );
    assert_eq!(child_reqs.len(), 2);
    assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    assert_eq!(child_reqs[1], required);
}
```

- [ ] **Step 2: Run the focused Cascades tests to verify they fail**

Run:

```bash
cargo test test_cte_anchor_to_memo_creates_binary_group --lib
cargo test cte_anchor_requires_left_then_right_children --lib
```

Expected: FAIL because `LogicalCTEAnchor` / `PhysicalCTEAnchor` do not exist yet.

- [ ] **Step 3: Add anchor operators in `src/sql/cascades/operator.rs`**

Insert operator structs and enum variants:

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

Add them to the enum:

```rust
LogicalCTEAnchor(LogicalCTEAnchorOp),
LogicalCTEProduce(LogicalCTEProduceOp),
LogicalCTEConsume(LogicalCTEConsumeOp),
```

and

```rust
PhysicalCTEAnchor(PhysicalCTEAnchorOp),
PhysicalCTEProduce(PhysicalCTEProduceOp),
PhysicalCTEConsume(PhysicalCTEConsumeOp),
```

Update `is_logical()`.

- [ ] **Step 4: Teach `convert.rs` to lower anchor/produce**

Extend `logical_plan_to_memo()`:

```rust
LogicalPlan::CTEAnchor(node) => {
    let left = logical_plan_to_memo(&node.left, memo);
    let right = logical_plan_to_memo(&node.right, memo);
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalCTEAnchor(LogicalCTEAnchorOp { cte_id: node.cte_id }),
        children: vec![left, right],
    };
    memo.new_group(expr)
}

LogicalPlan::CTEProduce(node) => {
    let child = logical_plan_to_memo(&node.input, memo);
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalCTEProduce(LogicalCTEProduceOp {
            cte_id: node.cte_id,
            output_columns: node.output_columns.clone(),
        }),
        children: vec![child],
    };
    memo.new_group(expr)
}
```

- [ ] **Step 5: Add the implementation rule**

In `src/sql/cascades/rules/implement.rs`, add:

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

Register it before produce/consume rules in `src/sql/cascades/rules/mod.rs`.

- [ ] **Step 6: Define anchor property / extract / stats behavior**

Apply the minimal physical semantics:

```rust
// search.rs
Operator::PhysicalCTEAnchor(_) => PhysicalPropertySet::any(),
```

and in `required_input_properties()`:

```rust
Operator::PhysicalCTEAnchor(_) => vec![PhysicalPropertySet::any(), required.clone()],
```

In `extract.rs`, anchor nodes are extracted like any other physical op once they have children.

In `stats.rs`, add:

```rust
Operator::LogicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),
Operator::PhysicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),
```

and include anchor variants in output-column derivation as passthrough from the right child.

- [ ] **Step 7: Extend `cost.rs` so the new operator variants compile cleanly**

Patch `src/sql/cascades/cost.rs` so both logical and physical anchor variants are accounted for explicitly:

```rust
Operator::LogicalCTEAnchor(_)
| Operator::LogicalCTEProduce(_)
| Operator::LogicalCTEConsume(_) => 0.0,
```

and

```rust
Operator::PhysicalCTEAnchor(_)
| Operator::PhysicalCTEProduce(_)
| Operator::PhysicalCTEConsume(_) => own_stats.output_row_count * 0.01,
```

The exact anchor cost can remain minimal in this phase; the key is to keep `compute_cost()` exhaustive so Task 2 is independently buildable.

- [ ] **Step 8: Run the focused Cascades tests to verify they pass**

Run:

```bash
cargo test test_cte_anchor_to_memo_creates_binary_group --lib
cargo test cte_anchor_requires_left_then_right_children --lib
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/sql/cascades/operator.rs src/sql/cascades/convert.rs src/sql/cascades/rules/implement.rs src/sql/cascades/rules/mod.rs src/sql/cascades/search.rs src/sql/cascades/extract.rs src/sql/cascades/stats.rs src/sql/cascades/cost.rs
git commit -m "feat(cascades): add CTE anchor operator support"
```

---

### Task 3: Replace placeholder CTE statistics with producer-linked semantics

**Files:**
- Modify: `src/sql/cascades/stats.rs`
- Test: `src/sql/cascades/stats.rs`

- [ ] **Step 1: Write the failing CTE stats test**

Extend `src/sql/cascades/stats.rs` tests with:

```rust
#[test]
fn cte_consume_uses_registered_producer_statistics() {
    let cols = vec![crate::sql::ir::OutputColumn {
        name: "id".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
    }];
    let mut memo = Memo::new();

    let scan_gid = memo.new_group(MExpr {
        id: 0,
        op: Operator::LogicalScan(LogicalScanOp {
            database: "db".to_string(),
            table: dummy_table_def(),
            alias: None,
            columns: cols.clone(),
            predicates: vec![],
            required_columns: None,
        }),
        children: vec![],
    });

    let produce_gid = memo.new_group(MExpr {
        id: 1,
        op: Operator::LogicalCTEProduce(LogicalCTEProduceOp {
            cte_id: 42,
            output_columns: cols.clone(),
        }),
        children: vec![scan_gid],
    });

    memo.groups[scan_gid].logical_props = Some(super::super::memo::LogicalProperties {
        output_columns: cols.clone(),
        row_count: 17.0,
    });
    memo.groups[produce_gid].logical_props = Some(super::super::memo::LogicalProperties {
        output_columns: cols.clone(),
        row_count: 17.0,
    });

    let consume_expr = MExpr {
        id: 2,
        op: Operator::LogicalCTEConsume(LogicalCTEConsumeOp {
            cte_id: 42,
            alias: "c".to_string(),
            output_columns: cols,
        }),
        children: vec![],
    };

    let stats = derive_statistics(&consume_expr, &memo, &std::collections::HashMap::new());
    assert_eq!(stats.output_row_count, 17.0);
}
```

- [ ] **Step 2: Run the stats test to verify it fails**

Run:

```bash
cargo test cte_consume_uses_registered_producer_statistics --lib
```

Expected: FAIL because `LogicalCTEConsume` still returns `1000.0` instead of the producer's `17.0`.

- [ ] **Step 3: Add a producer lookup helper, remove the fixed `1000.0` fallback, and make missing-producer wiring fail fast**

In `src/sql/cascades/stats.rs`, add a helper:

```rust
fn cte_producer_statistics(
    memo: &Memo,
    cte_id: crate::sql::cte::CteId,
) -> Option<Statistics> {
    memo.groups
        .iter()
        .flat_map(|group| group.logical_exprs.iter().chain(group.physical_exprs.iter()))
        .find_map(|expr| match &expr.op {
            Operator::LogicalCTEProduce(op) | Operator::PhysicalCTEProduce(op) if op.cte_id == cte_id => {
                Some(child_statistics(memo, &expr.children, 0))
            }
            _ => None,
        })
}
```

Use it for both consume variants:

```rust
Operator::LogicalCTEConsume(op) => cte_producer_statistics(memo, op.cte_id)
    .expect("CTE consume must have a matching producer in memo"),
```

and the same for `PhysicalCTEConsume`.

Also add a second test that proves the fail-fast behavior:

```rust
#[test]
#[should_panic(expected = "CTE consume must have a matching producer in memo")]
fn cte_consume_panics_when_producer_is_missing() {
    let expr = MExpr {
        id: 0,
        op: Operator::LogicalCTEConsume(LogicalCTEConsumeOp {
            cte_id: 404,
            alias: "missing".to_string(),
            output_columns: vec![],
        }),
        children: vec![],
    };
    let memo = Memo::new();
    let _ = derive_statistics(&expr, &memo, &std::collections::HashMap::new());
}
```

- [ ] **Step 4: Run the stats test to verify it passes**

Run:

```bash
cargo test cte_consume_uses_registered_producer_statistics --lib
cargo test cte_consume_panics_when_producer_is_missing --lib
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/cascades/stats.rs
git commit -m "fix(cascades): derive CTE consume stats from producers"
```

---

### Task 3.5: Add explicit fail-fast coverage for invalid CTE topology

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`
- Test: `src/sql/cascades/fragment_builder.rs`

- [ ] **Step 1: Write the failing fragment-builder topology test**

Extend the fragment builder tests with:

```rust
#[test]
fn cte_anchor_rejects_wrong_child_count() {
    let node = PhysicalPlanNode {
        op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: 1 }),
        children: vec![],
        stats: Statistics {
            output_row_count: 1.0,
            column_statistics: std::collections::HashMap::new(),
        },
        output_columns: vec![],
    };
    let catalog = InMemoryCatalog::default();
    let err = PlanFragmentBuilder::build(&node, &catalog, "default").unwrap_err();
    assert!(err.contains("CTE anchor expects exactly 2 children"));
}
```

- [ ] **Step 2: Add the recursive-CTE guard to the shared CTE execution path**

Extend the fragment builder tests with duplicate-producer and missing-metadata fail-fast coverage:

```rust
#[test]
fn cte_produce_rejects_duplicate_cte_id_registration() {
    let mut builder = PlanFragmentBuilder::new("default");
    builder.cte_fragments.insert(7, 0);
    let err = builder.register_cte_fragment(7, 1).unwrap_err();
    assert!(err.contains("duplicate CTE producer for cte_id=7"));
}

#[test]
fn cte_produce_rejects_missing_output_columns() {
    let op = PhysicalCTEProduceOp {
        cte_id: 9,
        output_columns: vec![],
    };
    let err = validate_cte_produce_metadata(&op).unwrap_err();
    assert!(err.contains("CTE producer metadata missing output columns"));
}
```

- [ ] **Step 3: Implement the explicit builder validations**

In `src/sql/cascades/fragment_builder.rs`:

1. add a small registration helper that rejects multiple producer fragments for the same `cte_id`
2. validate `PhysicalCTEProduceOp.output_columns` before creating the fragment
3. keep the existing `CTE anchor expects exactly 2 children` check for illegal anchor shape

Example helper shape:

```rust
fn register_cte_fragment(&mut self, cte_id: CteId, fragment_idx: usize) -> Result<(), String> {
    if self.cte_fragments.insert(cte_id, fragment_idx).is_some() {
        return Err(format!("duplicate CTE producer for cte_id={cte_id}"));
    }
    Ok(())
}
```

and metadata validation:

```rust
fn validate_cte_produce_metadata(op: &PhysicalCTEProduceOp) -> Result<(), String> {
    if op.output_columns.is_empty() {
        return Err("CTE producer metadata missing output columns".to_string());
    }
    Ok(())
}
```

- [ ] **Step 4: Run the fail-fast checks**

Run:

```bash
cargo test cte_anchor_rejects_wrong_child_count --lib
cargo test cte_produce_rejects_duplicate_cte_id_registration --lib
cargo test cte_produce_rejects_missing_output_columns --lib
```

Expected:

- unit test: PASS

- [ ] **Step 5: Commit**

```bash
git add src/sql/cascades/fragment_builder.rs
git commit -m "fix(cte): fail fast on invalid shared CTE topology"
```

---

### Task 4: Refactor fragment ownership to a StarRocks-style producer-owned multicast model

**Files:**
- Modify: `src/sql/physical/mod.rs`
- Modify: `src/sql/cascades/fragment_builder.rs`
- Modify: `src/standalone/coordinator.rs`
- Modify: `src/sql/physical/emitter/mod.rs`
- Test: `src/sql/cascades/fragment_builder.rs`
- Test: `src/standalone/coordinator.rs`

- [ ] **Step 1: Write the failing fragment builder test**

Add a targeted test module at the bottom of `src/sql/cascades/fragment_builder.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::standalone::catalog::InMemoryCatalog;
    use crate::sql::cascades::operator::{Operator, PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp};
    use crate::sql::cascades::physical_plan::PhysicalPlanNode;
    use crate::sql::ir::OutputColumn;
    use crate::sql::statistics::Statistics;
    use arrow::datatypes::DataType;

    fn cols() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }

    #[test]
    fn cte_anchor_builds_producer_fragment_before_consumer_exchange() {
        let produce = PhysicalPlanNode {
            op: Operator::PhysicalCTEProduce(PhysicalCTEProduceOp {
                cte_id: 1,
                output_columns: cols(),
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalValues(crate::sql::cascades::operator::PhysicalValuesOp {
                    rows: vec![vec![]],
                    columns: cols(),
                }),
                children: vec![],
                stats: Statistics {
                    output_row_count: 1.0,
                    column_statistics: std::collections::HashMap::new(),
                },
                output_columns: cols(),
            }],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: std::collections::HashMap::new(),
            },
            output_columns: cols(),
        };

        let consume = PhysicalPlanNode {
            op: Operator::PhysicalCTEConsume(PhysicalCTEConsumeOp {
                cte_id: 1,
                alias: "c".to_string(),
                output_columns: cols(),
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: std::collections::HashMap::new(),
            },
            output_columns: cols(),
        };

        let root = PhysicalPlanNode {
            op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: 1 }),
            children: vec![produce, consume],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: std::collections::HashMap::new(),
            },
            output_columns: cols(),
        };

        let catalog = InMemoryCatalog::default();
        let build = PlanFragmentBuilder::build(&root, &catalog, "default")
            .expect("builder should support CTE anchor");

        let producer = build.fragments.iter().find(|f| f.cte_id == Some(1)).expect("producer fragment");
        assert!(
            !producer.cte_consumer_exchange_nodes.is_empty(),
            "producer should own destinations"
        );
    }
}
```

- [ ] **Step 2: Run the builder test to verify it fails**

Run:

```bash
cargo test cte_anchor_builds_producer_fragment_before_consumer_exchange --lib
```

Expected: FAIL because `PhysicalCTEAnchor` and producer-owned destinations do not exist yet.

- [ ] **Step 3: Extend `FragmentBuildResult` with transitional producer-owned destinations**

In `src/sql/physical/mod.rs`, keep the legacy field for the old emitter path and add the new producer-owned field side-by-side:

```rust
pub cte_id: Option<CteId>,
pub cte_exchange_nodes: Vec<(CteId, i32)>,
pub cte_consumer_exchange_nodes: Vec<i32>,
```

The transition rule for this phase is:

- old emitter / old recursive path still reads and writes `cte_exchange_nodes`
- new Cascades CTE path reads and writes `cte_consumer_exchange_nodes`
- coordinator prefers `cte_consumer_exchange_nodes` when present, then falls back to legacy patch tuples for old-path fragments

- [ ] **Step 4: Introduce explicit anchor visitation and producer-owned destination wiring**

In `src/sql/cascades/fragment_builder.rs`:

1. Add `PhysicalCTEAnchorOp` to the imports and dispatch.
2. Replace `cte_fragments: HashMap<CteId, usize>` with a map from `cte_id` to completed-fragment index, keeping ownership on the producer fragment.
3. Stop appending new-path destinations into root-level `cte_exchange_nodes`; use producer-owned registration for the Cascades path only.

Add:

```rust
fn visit_cte_anchor(
    &mut self,
    _op: &PhysicalCTEAnchorOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    if node.children.len() != 2 {
        return Err("CTE anchor expects exactly 2 children".to_string());
    }

    self.visit(&node.children[0])?;
    self.visit(&node.children[1])
}
```

Refactor `visit_cte_produce()` so it only registers the completed producer fragment:

```rust
let cte_fragment = FragmentBuildResult {
    fragment_id: cte_fragment_id,
    plan: plan_nodes::TPlan::new(child.plan_nodes),
    desc_tbl: DescriptorTableBuilder::new().build(),
    exec_params: nodes::build_exec_params_multi(&[])?,
    output_sink: build_noop_sink(),
    output_columns: op.output_columns.iter().map(|c| OutputColumn {
        name: c.name.clone(),
        data_type: c.data_type.clone(),
        nullable: c.nullable,
    }).collect(),
    cte_id: Some(op.cte_id),
    cte_exchange_nodes: Vec::new(),
    cte_consumer_exchange_nodes: Vec::new(),
};
```

and return an empty visit result:

```rust
Ok(VisitResult {
    plan_nodes: Vec::new(),
    scope: child.scope,
    tuple_ids: child.tuple_ids,
})
```

Refactor `visit_cte_consume()` to register the exchange against the producer fragment:

```rust
let cte_idx = *self
    .cte_fragments
    .get(&op.cte_id)
    .ok_or_else(|| format!("CTE consume references unknown cte_id={}", op.cte_id))?;
self.completed_fragments[cte_idx]
    .cte_consumer_exchange_nodes
    .push(exchange_node_id);
```

- [ ] **Step 5: Update `ExecutionCoordinator` to read producer-owned exchange destinations**

In `src/standalone/coordinator.rs`, prefer producer-owned destinations but keep legacy fallback so the old multi-fragment emitter still works:

```rust
let consumer_exchange_nodes = if !cte_fr.cte_consumer_exchange_nodes.is_empty() {
    cte_fr.cte_consumer_exchange_nodes.clone()
} else {
    cte_consumers.get(&cte_id).cloned().unwrap_or_default()
};
if consumer_exchange_nodes.is_empty() {
    return Err(format!(
        "CTE fragment (cte_id={cte_id}) has no consumer exchanges"
    ));
}
```

Also patch `src/sql/physical/emitter/mod.rs` so every existing `FragmentBuildResult` initializer keeps compiling by explicitly setting:

```rust
cte_exchange_nodes,
cte_consumer_exchange_nodes: Vec::new(),
```

This keeps the old emitter path buildable while Task 4 introduces the new metadata for the Cascades path.

and update `per_exch_num_senders` to iterate over producer fragments:

```rust
for cte_fr in &cte_fragments {
    for exchange_node_id in &cte_fr.cte_consumer_exchange_nodes {
        per_exch_num_senders.insert(*exchange_node_id, 1);
    }
}
```

- [ ] **Step 6: Run the builder test and coordinator tests to verify they pass**

Run:

```bash
cargo test cte_anchor_builds_producer_fragment_before_consumer_exchange --lib
cargo test --lib coordinator
```

Expected: the focused builder test passes; if there are no direct coordinator-named tests yet, the second command should complete without running any tests rather than failing to compile.

- [ ] **Step 7: Commit**

```bash
git add src/sql/physical/mod.rs src/sql/cascades/fragment_builder.rs src/standalone/coordinator.rs src/sql/physical/emitter/mod.rs
git commit -m "refactor(cte): make producer fragments own multicast destinations"
```

---

### Task 5: Unify `execute_query` and `explain_query` on the CTE-aware Cascades path

**Files:**
- Modify: `src/standalone/engine.rs`
- Modify: `src/sql/explain.rs`
- Test: `src/sql/explain.rs`
- Test: `sql-tests/cte/sql/cte_starrocks_alignment.sql`
- Test: `sql-tests/cte/result/cte_starrocks_alignment.result`

- [ ] **Step 1: Write the failing EXPLAIN formatter test**

Add a focused unit test to `src/sql/explain.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::operator::{Operator, PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp};
    use crate::sql::cascades::physical_plan::PhysicalPlanNode;
    use crate::sql::ir::OutputColumn;
    use crate::sql::statistics::Statistics;
    use arrow::datatypes::DataType;

    fn cols() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }

    #[test]
    fn explain_physical_plan_shows_cte_anchor_topology() {
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: 5 }),
            children: vec![
                PhysicalPlanNode {
                    op: Operator::PhysicalCTEProduce(PhysicalCTEProduceOp {
                        cte_id: 5,
                        output_columns: cols(),
                    }),
                    children: vec![],
                    stats: Statistics {
                        output_row_count: 1.0,
                        column_statistics: std::collections::HashMap::new(),
                    },
                    output_columns: cols(),
                },
                PhysicalPlanNode {
                    op: Operator::PhysicalCTEConsume(PhysicalCTEConsumeOp {
                        cte_id: 5,
                        alias: "c".to_string(),
                        output_columns: cols(),
                    }),
                    children: vec![],
                    stats: Statistics {
                        output_row_count: 1.0,
                        column_statistics: std::collections::HashMap::new(),
                    },
                    output_columns: cols(),
                },
            ],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: std::collections::HashMap::new(),
            },
            output_columns: cols(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Normal);
        assert!(lines.iter().any(|l| l.contains("CTE ANCHOR (cte_id=5)")));
        assert!(lines.iter().any(|l| l.contains("CTE PRODUCE (cte_id=5)")));
        assert!(lines.iter().any(|l| l.contains("CTE CONSUME (cte_id=5)")));
    }
}
```

- [ ] **Step 2: Run the EXPLAIN test to verify it fails**

Run:

```bash
cargo test explain_physical_plan_shows_cte_anchor_topology --lib
```

Expected: FAIL because physical explain does not yet know `PhysicalCTEAnchor`.

- [ ] **Step 3: Update `src/sql/explain.rs` for logical and physical anchor support**

In logical formatting:

```rust
LogicalPlan::CTEAnchor(node) => {
    out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
    format_node(&node.left, level, indent + 1, out);
    format_node(&node.right, level, indent + 1, out);
}
LogicalPlan::CTEProduce(node) => {
    out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
    format_node(&node.input, level, indent + 1, out);
}
```

In physical formatting:

```rust
Operator::PhysicalCTEAnchor(op) => {
    out.push(format!("{pad}CTE ANCHOR (cte_id={}){costs_suffix}", op.cte_id));
    for child in &node.children {
        format_physical_node(child, level, indent + 1, out);
    }
}
```

- [ ] **Step 4: Route CTE queries through the adapter + Cascades path**

In `src/standalone/engine.rs`, change the branching in both `explain_query()` and `execute_query()` so only **non-recursive shared CTE** queries enter the new adapter + Cascades path. Keep the existing old path for recursive CTE SQL (`WITH RECURSIVE`) unchanged in this phase.

Use the parsed AST to split the behavior first:

```rust
let is_recursive_cte = query
    .with
    .as_ref()
    .map(|with| with.recursive)
    .unwrap_or(false);
```

Then route:

```rust
let logical = if cte_registry.entries.is_empty() {
    crate::sql::planner::plan(resolved)?
} else if is_recursive_cte {
    // Keep the existing recursive / old CTE execution path unchanged in this phase.
    let query_plan = crate::sql::planner::plan_query(resolved, cte_registry)?;
    return execute_legacy_cte_query_plan(query_plan, catalog, current_database);
} else {
    let query_plan = crate::sql::planner::plan_query(resolved, cte_registry)?;
    crate::sql::cascades::cte_adapter::build_cte_logical_tree(query_plan)?
};
```

Do the same split in `explain_query()`: recursive CTE keeps `optimize_query_plan(...) + explain_query_plan(...)`; non-recursive shared CTE goes through `build_cte_logical_tree(...) + cascades::optimize(...) + explain_physical_plan(...)`.

Then compute stats once:

```rust
let table_stats = build_table_stats_from_plan(&logical);
let physical = crate::sql::cascades::optimize(logical, &table_stats)?;
```

and use the same physical-plan explain / fragment-builder execution path regardless of CTE presence.

Also extend `collect_scan_stats()` to recurse through the new logical nodes:

```rust
LogicalPlan::CTEProduce(n) => collect_scan_stats(&n.input, out),
LogicalPlan::CTEAnchor(n) => {
    collect_scan_stats(&n.left, out);
    collect_scan_stats(&n.right, out);
}
```

- [ ] **Step 5: Add a focused SQL regression suite for non-recursive shared CTEs**

Create `sql-tests/cte/sql/cte_starrocks_alignment.sql`:

```sql
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.cte_align_t0;
DROP TABLE IF EXISTS ${case_db}.cte_align_t1;
CREATE TABLE ${case_db}.cte_align_t0 (
    k1 bigint NULL,
    k2 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.cte_align_t1 (
    k1 bigint NULL,
    v1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.cte_align_t0 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.cte_align_t1 VALUES (1, 100), (2, 200), (4, 400);

-- query 2
-- @order_sensitive=true
WITH base AS (
    SELECT t0.k1, t0.k2 FROM ${case_db}.cte_align_t0 t0
)
SELECT l.k1, r.k1
FROM base l
JOIN base r ON l.k1 = r.k1
ORDER BY l.k1, r.k1;

-- query 3
-- @order_sensitive=true
WITH base AS (
    SELECT t0.k1, t0.k2 FROM ${case_db}.cte_align_t0 t0
),
joined AS (
    SELECT b.k1, t1.v1
    FROM base b
    JOIN ${case_db}.cte_align_t1 t1 ON b.k1 = t1.k1
)
SELECT k1, v1 FROM joined ORDER BY k1;

-- query 4
-- @skip_result_check=true
EXPLAIN VERBOSE
WITH base AS (
    SELECT t0.k1, t0.k2 FROM ${case_db}.cte_align_t0 t0
)
SELECT l.k1
FROM base l
JOIN base r ON l.k1 = r.k1
ORDER BY l.k1;
```

Create `sql-tests/cte/result/cte_starrocks_alignment.result`:

```text
-- query 2
k1	k1
1	1
2	2
3	3

-- query 3
k1	v1
1	100
2	200
```

- [ ] **Step 6: Run focused unit + SQL tests**

Run:

```bash
cargo test explain_physical_plan_shows_cte_anchor_topology --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --only cte_starrocks_alignment
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --only cte_recursive
```

Expected:

- unit test: PASS
- sql-tests: suite runs and `cte_starrocks_alignment` passes with zero mismatches
- `cte_recursive`: still passes on the preserved old path

- [ ] **Step 7: Commit**

```bash
git add src/standalone/engine.rs src/sql/explain.rs sql-tests/cte/sql/cte_starrocks_alignment.sql sql-tests/cte/result/cte_starrocks_alignment.result
git commit -m "feat(cte): route shared CTE queries through cascades"
```

---

### Task 6: Reconcile coordinator/build result wiring and run broader regression

**Files:**
- Modify: `src/standalone/coordinator.rs`
- Modify: `src/sql/cascades/fragment_builder.rs`
- Test: `sql-tests/cte/sql/cte_in_where_subquery.sql`
- Test: `sql-tests/limit/sql/limit_offset_basic.sql`

- [ ] **Step 1: Run the existing non-recursive CTE and limit suites on the new path**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --skip cte_recursive
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite limit --mode verify --only limit_offset_basic
```

Expected: at least one existing case fails due to missing explain text, multicast wiring, or limit/consumer handling on the new path.

- [ ] **Step 2: Fix any remaining producer/consumer wiring gaps surfaced by regression**

Make the minimal code fixes required in `src/sql/cascades/fragment_builder.rs` and `src/standalone/coordinator.rs` so that:

```rust
// Producer fragment remains the source of truth for all consumer exchanges.
assert!(cte_fragment.cte_id.is_some());
assert!(!cte_fragment.cte_consumer_exchange_nodes.is_empty());
```

and root-fragment execution still receives:

```rust
per_exch_num_senders.insert(exchange_node_id, 1);
```

for every registered consumer exchange.

- [ ] **Step 3: Re-run the existing regression suites**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --skip cte_recursive
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --only cte_recursive
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite limit --mode verify --only limit_offset_basic
```

Expected: PASS.

- [ ] **Step 4: Run the repository verification commands used for SQL / Cascades changes**

Run:

```bash
cargo fmt --all
cargo test
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --skip cte_recursive
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify --only cte_recursive
```

Expected:

- `cargo fmt --all`: no diffs afterward
- `cargo test`: PASS
- non-recursive `cte` suite: PASS
- `cte_recursive`: PASS on the preserved legacy path

- [ ] **Step 5: Commit**

```bash
git add src/standalone/coordinator.rs src/sql/cascades/fragment_builder.rs
git commit -m "test(cte): verify StarRocks-aligned CTE execution path"
```

---

## Self-Review

### Spec coverage

- **Unified logical/physical CTE model** → Tasks 1-2
- **Producer-owned multicast / consumer exchange** → Task 4
- **Unified execute/explain path** → Task 5
- **Non-recursive scope, recursive excluded from the new path** → Tasks 5-6 preserve the legacy recursive path and verify `cte_recursive` separately
- **Regression / validation cases** → Tasks 3.5, 5-6
- **Planner transitional adapter, not one-shot planner rewrite** → Task 1

No spec gaps remain for the requested phase.

### Placeholder scan

Searched manually while writing for: `TODO`, `TBD`, “implement later”, “appropriate error handling”, “write tests for the above”, and “similar to task N”. None remain in this plan.

### Type consistency

- Logical names are consistently `CTEAnchorNode`, `CTEProduceNode`, `CTEConsumeNode`
- Cascades operator names are consistently `LogicalCTEAnchorOp`, `PhysicalCTEAnchorOp`
- Transitional adapter entry is consistently `build_cte_logical_tree(query_plan)`
- Producer-owned fragment metadata is consistently `cte_consumer_exchange_nodes`
