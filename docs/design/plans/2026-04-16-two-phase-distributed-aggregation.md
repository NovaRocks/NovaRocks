# Two-Phase Distributed Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit Local+Global two-phase aggregation alternatives in the Cascades optimizer so that GROUP BY queries pre-aggregate before shuffling, matching StarRocks FE plan quality.

**Architecture:** The `AggToHashAgg` implementation rule produces a two-phase alternative (Local agg → hash exchange → Global agg) alongside the existing single-phase option. The fragment builder handles mode-specific Thrift generation: `need_finalize=false` for Local, `is_merge_agg=true` for Global. No expression rewriting at the optimizer level — positional slot mapping in the fragment builder.

**Tech Stack:** Rust, Cascades optimizer, Thrift plan nodes, StarRocks execution layer.

**Spec:** `docs/design/specs/2026-04-16-two-phase-distributed-aggregation-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/sql/cascades/rules/implement.rs` | Modify (lines 631–665) | Emit Local+Global alternative in `AggToHashAgg.apply()` |
| `src/sql/physical/nodes.rs` | Modify (lines 253–295) | Add `need_finalize` parameter to `build_aggregation_node` |
| `src/sql/physical/expr_compiler.rs` | Modify (add method after line 173) | Add `compile_merge_aggregate_call` method |
| `src/sql/cascades/fragment_builder.rs` | Modify (lines 787–881) | Mode-aware aggregate compilation |
| `src/sql/physical/emitter/emit_aggregate.rs` | Modify (line 84) | Update `build_aggregation_node` call site |
| `src/sql/physical/emitter/emit_set_op.rs` | Modify (line 107) | Update `build_aggregation_node` call site |

---

### Task 1: Add `need_finalize` parameter to `build_aggregation_node`

**Files:**
- Modify: `src/sql/physical/nodes.rs:253-295`
- Modify: `src/sql/physical/emitter/emit_aggregate.rs:84`
- Modify: `src/sql/physical/emitter/emit_set_op.rs:107`
- Modify: `src/sql/cascades/fragment_builder.rs:863` (first call site)
- Modify: `src/sql/cascades/fragment_builder.rs:2123` (second call site)

- [ ] **Step 1: Add `need_finalize` parameter to `build_aggregation_node`**

In `src/sql/physical/nodes.rs`, change the function signature and use the parameter:

```rust
pub(crate) fn build_aggregation_node(
    node_id: i32,
    output_tuple_id: i32,
    intermediate_tuple_id: i32,
    grouping_exprs: Vec<exprs::TExpr>,
    aggregate_functions: Vec<exprs::TExpr>,
    need_finalize: bool,
) -> plan_nodes::TPlanNode {
```

And change the hardcoded field:

```rust
        need_finalize,
```

(was `need_finalize: true,`)

- [ ] **Step 2: Update all existing call sites to pass `true`**

There are 4 call sites. Each currently passes 5 args; add `true` as the 6th:

`src/sql/physical/emitter/emit_aggregate.rs:84`:
```rust
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            true,
        );
```

`src/sql/physical/emitter/emit_set_op.rs:107`:
```rust
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![], // no aggregate functions — pure DISTINCT
            true,
        );
```

`src/sql/cascades/fragment_builder.rs:863`:
```rust
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            true,
        );
```

`src/sql/cascades/fragment_builder.rs:2123`:
```rust
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![],
            true,
        );
```

- [ ] **Step 3: Verify build passes**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add src/sql/physical/nodes.rs src/sql/physical/emitter/emit_aggregate.rs \
        src/sql/physical/emitter/emit_set_op.rs src/sql/cascades/fragment_builder.rs
git commit -m "refactor: add need_finalize parameter to build_aggregation_node"
```

---

### Task 2: Add `compile_merge_aggregate_call` to ExprCompiler

**Files:**
- Modify: `src/sql/physical/expr_compiler.rs` (add method after `compile_aggregate_call_typed`)

- [ ] **Step 1: Add the merge compilation method**

Insert after `compile_aggregate_call_typed` (after line 173) in `src/sql/physical/expr_compiler.rs`:

```rust
    /// Compile a merge-phase aggregate call for two-phase distributed aggregation.
    ///
    /// Instead of compiling the original args, this generates a single SlotRef
    /// child pointing to the intermediate column from the Local phase's output.
    /// The root node has `is_merge_agg: true` so the execution layer calls
    /// merge+finalize instead of update+serialize.
    pub fn compile_merge_aggregate_call(
        &mut self,
        agg_call: &AggregateCall,
        input_slot_id: i32,
        input_tuple_id: i32,
        input_type: &DataType,
    ) -> Result<exprs::TExpr, String> {
        self.nodes.clear();

        let is_distinct = agg_call.distinct;
        let effective_name = if is_distinct {
            match agg_call.name.as_str() {
                "count" => "multi_distinct_count".to_string(),
                "sum" => "multi_distinct_sum".to_string(),
                _ => agg_call.name.clone(),
            }
        } else {
            agg_call.name.clone()
        };

        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        // Single child: SlotRef to the intermediate column from Local phase.
        let input_type_desc = arrow_type_to_type_desc(input_type)?;
        self.nodes
            .push(slot_ref_node(input_slot_id, input_tuple_id, input_type_desc));

        let arg_types = if agg_call.args.is_empty() {
            // count(*): no original args, but merge needs the intermediate type
            vec![input_type.clone()]
        } else {
            agg_call.args.iter().map(|a| a.data_type.clone()).collect()
        };

        let return_type = agg_call.result_type.clone();
        let type_desc = arrow_type_to_type_desc(&return_type)?;

        let (_, intermediate_type) =
            infer_agg_function_types(&effective_name, &arg_types, is_distinct)?;
        let intermediate_type_desc = match &intermediate_type {
            Some(it) => arrow_type_to_type_desc(it)?,
            None => types::TTypeDesc { types: None },
        };

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(|t| arrow_type_to_type_desc(t))
            .collect::<Result<Vec<_>, _>>()?;

        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: type_desc.clone(),
            num_children: 1, // single SlotRef child
            agg_expr: Some(exprs::TAggregateExpr {
                is_merge_agg: true,
            }),
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: effective_name,
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: fn_arg_types,
                ret_type: type_desc,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: Some(types::TAggregateFunction {
                    intermediate_type: intermediate_type_desc,
                    update_fn_symbol: None,
                    init_fn_symbol: None,
                    serialize_fn_symbol: None,
                    merge_fn_symbol: None,
                    finalize_fn_symbol: None,
                    get_value_fn_symbol: None,
                    remove_fn_symbol: None,
                    is_analytic_only_fn: None,
                    symbol: None,
                    is_asc_order: None,
                    nulls_first: None,
                    is_distinct: if is_distinct { Some(true) } else { None },
                }),
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        };
        self.last_type = return_type;
        self.last_nullable = true;
        Ok(exprs::TExpr::new(std::mem::take(&mut self.nodes)))
    }
```

- [ ] **Step 2: Verify build passes**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors (the new method exists but is not yet called — may get a dead_code warning, which is fine).

- [ ] **Step 3: Commit**

```bash
git add src/sql/physical/expr_compiler.rs
git commit -m "feat: add compile_merge_aggregate_call for two-phase agg"
```

---

### Task 3: Emit two-phase alternative in `AggToHashAgg`

**Files:**
- Modify: `src/sql/cascades/rules/implement.rs:631-665`

- [ ] **Step 1: Add unit test for two-phase alternative generation**

Append this test module at the end of `src/sql/cascades/rules/implement.rs` (after the existing test modules):

```rust
#[cfg(test)]
mod two_phase_agg_tests {
    use super::*;
    use crate::sql::cascades::memo::{MExpr, Memo};
    use crate::sql::cascades::operator::{LogicalAggregateOp, LogicalValuesOp};
    use crate::sql::ir::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::plan::AggregateCall;
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

    #[test]
    fn agg_to_hash_agg_produces_single_and_two_phase() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("city")],
                aggregates: vec![AggregateCall {
                    name: "sum".into(),
                    args: vec![col("amount")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                }],
                output_columns: vec![
                    OutputColumn {
                        name: "city".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "sum(amount)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule.apply(&expr, &mut memo);

        // Should produce 2 alternatives: Single and Global.
        assert_eq!(out.len(), 2, "expected Single + Global alternatives");

        // Alternative 1: Single
        match &out[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Single));
                assert_eq!(p.group_by.len(), 1);
                assert_eq!(p.aggregates.len(), 1);
            }
            other => panic!("expected PhysicalHashAggregate(Single), got {:?}", other),
        }
        assert_eq!(out[0].children, vec![child_group]);

        // Alternative 2: Global (child is a new group containing Local)
        match &out[1].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Global));
                assert_eq!(p.group_by.len(), 1);
                assert_eq!(p.aggregates.len(), 1);
            }
            other => panic!("expected PhysicalHashAggregate(Global), got {:?}", other),
        }
        let local_group_id = out[1].children[0];
        assert_ne!(local_group_id, child_group, "Global's child should be a new group");

        // The new group should contain a Local physical expr
        let local_group = &memo.groups[local_group_id];
        assert_eq!(local_group.physical_exprs.len(), 1);
        match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Local));
                assert_eq!(p.group_by.len(), 1);
                assert_eq!(p.aggregates.len(), 1);
            }
            other => panic!("expected PhysicalHashAggregate(Local), got {:?}", other),
        }
        // Local's child should be the original scan group
        assert_eq!(local_group.physical_exprs[0].children, vec![child_group]);
    }

    #[test]
    fn agg_to_hash_agg_skips_two_phase_for_distinct() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("city")],
                aggregates: vec![AggregateCall {
                    name: "count".into(),
                    args: vec![col("id")],
                    distinct: true,
                    result_type: DataType::Int64,
                    order_by: vec![],
                }],
                output_columns: vec![
                    OutputColumn {
                        name: "city".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "count(distinct id)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule.apply(&expr, &mut memo);

        // Only Single — no two-phase for DISTINCT.
        assert_eq!(out.len(), 1, "DISTINCT agg should only produce Single");
        match &out[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Single));
            }
            other => panic!("expected PhysicalHashAggregate(Single), got {:?}", other),
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test two_phase_agg_tests --lib -- --nocapture 2>&1 | tail -20`
Expected: FAIL — `agg_to_hash_agg_produces_single_and_two_phase` fails because `out.len()` is 1 (only Single is emitted).

- [ ] **Step 3: Implement two-phase alternative in `AggToHashAgg.apply()`**

Replace the body of `AggToHashAgg.apply()` (lines 643–664) with:

```rust
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(op) = &expr.op else {
            return vec![];
        };

        // Alternative 1: Single-phase aggregation (always applicable).
        let single = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        };

        // Two-phase Local+Global: skip when any aggregate is DISTINCT or
        // when there are no group-by keys (scalar agg — deferred).
        let has_distinct = op.aggregates.iter().any(|a| a.distinct);
        if has_distinct || op.group_by.is_empty() {
            return vec![single];
        }

        // Alternative 2: Local pre-agg → (hash exchange inserted by enforcer) → Global merge.
        let local_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Local,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        };
        let local_group_id = memo.new_group(local_mexpr);

        let global = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Global,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: vec![local_group_id],
        };

        vec![single, global]
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test two_phase_agg_tests --lib -- --nocapture 2>&1 | tail -20`
Expected: both tests PASS.

- [ ] **Step 5: Run full build**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/sql/cascades/rules/implement.rs
git commit -m "feat: emit two-phase Local+Global alternative in AggToHashAgg"
```

---

### Task 4: Mode-aware fragment builder

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs:787-881` (`visit_hash_aggregate`)

This is the core change. The fragment builder must:
- **Local mode**: set `need_finalize=false`, compile aggregate functions normally
- **Global mode**: set `need_finalize=true`, compile aggregate functions as merge expressions using child scope's intermediate slots

- [ ] **Step 1: Add `AggMode` import**

In `src/sql/cascades/fragment_builder.rs`, add `AggMode` to the import on line 20:

```rust
use crate::sql::cascades::operator::{
    AggMode, PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp,
    PhysicalDistributionOp, PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp,
    PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalIntersectOp, PhysicalLimitOp,
    PhysicalNestLoopJoinOp, PhysicalProjectOp, PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp,
    PhysicalSubqueryAliasOp, PhysicalTopNOp, PhysicalUnionOp, PhysicalValuesOp, PhysicalWindowOp,
};
```

- [ ] **Step 2: Rewrite `visit_hash_aggregate` to be mode-aware**

Replace `visit_hash_aggregate` (lines 787–881) with:

```rust
    fn visit_hash_aggregate(
        &mut self,
        op: &PhysicalHashAggregateOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions (same for all modes — the child scope
        // has the correct columns for both scan-level and Local-output contexts).
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                &name,
                &data_type,
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                nullable,
            };
            agg_scope.add_column(None, name, binding.clone());
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = gb_expr.kind
            {
                agg_scope.add_qualified_alias(q.clone(), column.clone(), binding);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions — mode-dependent.
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();
        let is_global = matches!(op.mode, AggMode::Global);

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if is_global {
                // Global (merge) phase: the child scope contains the Local's
                // output.  Each intermediate aggregate column sits at position
                // group_by.len() + idx in the child scope's ordered columns.
                let child_columns: Vec<_> = child.scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                // Single or Local: compile against child scope normally.
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_aggregate_call_typed(agg_call)?
            };

            let data_type = agg_call.result_type.clone();
            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let slot_id = self.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            self.desc_builder
                .add_slot(slot_id, agg_tuple_id, &name, &data_type, nullable, col_pos);
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    nullable,
                },
            );
            aggregate_functions.push(texpr);
        }

        let need_finalize = !matches!(op.mode, AggMode::Local);

        self.desc_builder.add_tuple(agg_tuple_id);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            need_finalize,
        );

        // Pre-order: agg first, then child nodes
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }
```

- [ ] **Step 3: Verify build passes**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 4: Run unit tests**

Run: `cargo test --lib 2>&1 | tail -10`
Expected: all tests pass (including the two-phase tests from Task 3).

- [ ] **Step 5: Commit**

```bash
git add src/sql/cascades/fragment_builder.rs
git commit -m "feat: mode-aware aggregate compilation in fragment builder"
```

---

### Task 5: End-to-end EXPLAIN verification

Verify that two-phase aggregation appears in EXPLAIN output for GROUP BY queries.

- [ ] **Step 1: Build the standalone server (debug)**

Run: `cargo build 2>&1 | tail -5`
Expected: build succeeds.

- [ ] **Step 2: Start standalone server**

Run in background:
```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

Wait for startup (look for the heartbeat or listening log).

- [ ] **Step 3: Verify two-phase in EXPLAIN for a GROUP BY query**

Connect with mysql client and run EXPLAIN on a simple aggregate:

```sql
EXPLAIN SELECT d_year, SUM(ss_sales_price)
FROM tpcds_iceberg.tpcds.store_sales
JOIN tpcds_iceberg.tpcds.date_dim ON ss_sold_date_sk = d_date_sk
GROUP BY d_year;
```

Expected: the plan should show:
- `HASH AGGREGATE (GLOBAL, group by: [d_year])` near the top
- `HASH EXCHANGE (hash: ...)` in between
- `HASH AGGREGATE (LOCAL, group by: [d_year])` below that

If the optimizer chose Single instead (cost-based decision), run a query with higher selectivity:

```sql
EXPLAIN SELECT i_category, COUNT(*), SUM(ss_sales_price)
FROM tpcds_iceberg.tpcds.store_sales
JOIN tpcds_iceberg.tpcds.item ON ss_item_sk = i_item_sk
GROUP BY i_category;
```

- [ ] **Step 4: Verify DISTINCT stays single-phase**

```sql
EXPLAIN SELECT d_year, COUNT(DISTINCT ss_customer_sk)
FROM tpcds_iceberg.tpcds.store_sales
JOIN tpcds_iceberg.tpcds.date_dim ON ss_sold_date_sk = d_date_sk
GROUP BY d_year;
```

Expected: `HASH AGGREGATE (SINGLE, ...)` — no LOCAL/GLOBAL.

- [ ] **Step 5: Run SQL test suites to verify correctness**

Run SSB suite first (fastest):
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb --mode verify --query-timeout 60
```
Expected: 13/13 pass.

Then TPC-H:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-h --mode verify --query-timeout 60
```
Expected: 22/22 pass.

Then TPC-DS:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify --query-timeout 120
```
Expected: 99/99 pass.

- [ ] **Step 6: If any suite fails, debug and fix**

If a query fails:
1. Run `EXPLAIN` on the failing query to check the plan shape
2. If the failure is in two-phase mode, check that the Global merge produces correct results
3. Common issues: intermediate type mismatch, merge function not handling the intermediate state correctly
4. Fix and re-run the failing suite

- [ ] **Step 7: Commit any fixes from Step 6**

If fixes were needed:
```bash
git add -A
git commit -m "fix: address two-phase aggregation correctness issues from test suites"
```
