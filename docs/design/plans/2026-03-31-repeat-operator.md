# Repeat Operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ROLLUP's UNION ALL expansion with a Repeat operator that executes the base join once and replicates rows, reducing q18 peak memory from 5.8 GB to ~2 GB.

**Architecture:** Four layers changed bottom-up: (1) IR adds RepeatInfo, (2) Analyzer computes repeat metadata instead of expanding UNION ALL, (3) Planner inserts Repeat node between join and aggregate, (4) Emitter generates TRepeatNode Thrift structure consumed by the existing executor. The existing lowering (`lower/node/repeat.rs`) and pipeline executor (`RepeatOperator`) are reused unchanged.

**Tech Stack:** Rust, existing Thrift plan_nodes types (TPlanNode.repeat_node field, TPlanNodeType::REPEAT_NODE)

---

### Task 1: Add RepeatInfo to IR and RepeatNode to LogicalPlan

This task adds the data structures without changing behavior.

**Files:**
- Modify: `src/sql/ir/mod.rs`
- Modify: `src/sql/plan/mod.rs`
- Modify: `src/sql/optimizer/mod.rs` (map_children)
- Modify: `src/sql/optimizer/cardinality.rs` (estimate for Repeat)
- Modify: `src/sql/optimizer/cost.rs` (cost for Repeat)

- [ ] **Step 1: Add RepeatInfo to `src/sql/ir/mod.rs`**

Add after the `ResolvedSelect` struct:

```rust
/// Metadata for ROLLUP/CUBE/GROUPING SETS repeat execution.
#[derive(Clone, Debug)]
pub(crate) struct RepeatInfo {
    /// For each repeat level, the column names that are NON-null.
    /// Level 0 has all keys, last level has none.
    pub repeat_column_ref_list: Vec<Vec<String>>,
    /// Grouping ID bitmap for each level. Bit=1 means column is NULLed.
    pub grouping_ids: Vec<u64>,
    /// All rollup column names.
    pub all_rollup_columns: Vec<String>,
    /// GROUPING() function calls: (output_name, arg_column_names).
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}
```

Add `repeat` field to `ResolvedSelect`:

```rust
pub struct ResolvedSelect {
    pub from: Option<Relation>,
    pub filter: Option<TypedExpr>,
    pub group_by: Vec<TypedExpr>,
    pub having: Option<TypedExpr>,
    pub projection: Vec<ProjectItem>,
    pub has_aggregation: bool,
    pub distinct: bool,
    pub repeat: Option<RepeatInfo>,  // NEW
}
```

Update all construction sites of `ResolvedSelect` to add `repeat: None` (search: `ResolvedSelect {` in `mod.rs` and `subquery_rewrite.rs`).

- [ ] **Step 2: Add RepeatNode to `src/sql/plan/mod.rs`**

Add the struct:

```rust
/// Repeat node for ROLLUP/CUBE/GROUPING SETS.
/// Replicates each input row N times with different null patterns.
#[derive(Clone, Debug)]
pub(crate) struct RepeatPlanNode {
    pub input: Box<LogicalPlan>,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}
```

Add variant to LogicalPlan:

```rust
pub(crate) enum LogicalPlan {
    // ... existing variants ...
    Repeat(RepeatPlanNode),
}
```

- [ ] **Step 3: Update `map_children` in `src/sql/optimizer/mod.rs`**

Add handling for Repeat in the match:

```rust
LogicalPlan::Repeat(n) => LogicalPlan::Repeat(RepeatPlanNode {
    input: Box::new(f(*n.input)),
    ..n
}),
```

- [ ] **Step 4: Add Repeat to cardinality.rs and cost.rs**

In `cardinality.rs`, add a match arm for `LogicalPlan::Repeat`:
```rust
LogicalPlan::Repeat(r) => {
    let input = estimate_statistics(&r.input, table_stats);
    let repeat_times = r.repeat_column_ref_list.len() as f64;
    Statistics {
        output_row_count: input.output_row_count * repeat_times,
        column_statistics: input.column_statistics,
    }
}
```

In `cost.rs`, add:
```rust
LogicalPlan::Repeat(_) => {
    // Streaming operator, minimal cost
    CostEstimate {
        cpu_cost: own_stats.compute_size(),
        memory_cost: 0.0,
        network_cost: 0.0,
    }
}
```

- [ ] **Step 5: Build and test**

Run: `cargo build && cargo test -p novarocks`

Expected: Clean build. All existing tests pass (Repeat is just a new variant, not used yet).

- [ ] **Step 6: Commit**

```bash
git add src/sql/ir/mod.rs src/sql/plan/mod.rs src/sql/optimizer/mod.rs src/sql/optimizer/cardinality.rs src/sql/optimizer/cost.rs src/sql/analyzer/subquery_rewrite.rs
git commit -m "feat: add RepeatInfo and RepeatPlanNode data structures for ROLLUP"
```

---

### Task 2: Analyzer — resolve_rollup replaces expand_rollup

Replace the UNION ALL expansion with repeat metadata computation.

**Files:**
- Modify: `src/sql/analyzer/mod.rs`

- [ ] **Step 1: Add `resolve_rollup` method**

Add a new method to `AnalyzerContext` that:

1. Takes the same inputs as `expand_rollup`: `&sqlast::Select` and `&[Vec<sqlast::Expr>]`
2. Computes the repeat metadata:
   - For ROLLUP(a, b, c): creates levels [(a,b,c), (a,b), (a), ()]
   - `repeat_column_ref_list`: for each level, the list of column names that are active (NON-null)
   - `grouping_ids`: for each level, a u64 bitmap where bit i = 1 if column i is NULLed
   - `all_rollup_columns`: flat list of all rollup column names
3. Detects GROUPING() function calls in the SELECT projection and records them in `grouping_fn_args`
4. Analyzes the SELECT ONCE with all GROUP BY keys active (level 0)
5. Sets `repeat: Some(RepeatInfo { ... })` on the `ResolvedSelect`
6. Returns `(QueryBody::Select(sel), cols)` — a single SELECT, not SetOperation

Key difference from `expand_rollup`: the analyzer runs ONCE and produces ONE ResolvedSelect with RepeatInfo attached, instead of N+1 ResolvedSelects combined by UNION ALL.

For GROUPING() calls in the projection: the analyzer must recognize `grouping(col)` as a function that returns Int64. It should NOT be resolved as a scalar function — it should be recorded in RepeatInfo and replaced with a placeholder column reference that the emitter maps to a virtual slot.

- [ ] **Step 2: Replace the call site**

In `analyze_set_expr`, change:

```rust
if let Some(rollup_exprs) = self.extract_rollup_from_group_by(s) {
    return self.expand_rollup(s, &rollup_exprs);
}
```

to:

```rust
if let Some(rollup_exprs) = self.extract_rollup_from_group_by(s) {
    return self.resolve_rollup(s, &rollup_exprs);
}
```

Keep `expand_rollup` and the GROUPING-related helper functions (`replace_grouping_calls`, `replace_grouping_in_window`) temporarily — they can be removed once tests confirm the new path works.

- [ ] **Step 3: Build and test**

Run: `cargo build && cargo test -p novarocks`

Some existing ROLLUP tests may need updating since the output structure changes from UNION ALL (SetOperation) to single Select with RepeatInfo.

- [ ] **Step 4: Commit**

```bash
git add src/sql/analyzer/mod.rs
git commit -m "feat: resolve_rollup computes RepeatInfo instead of UNION ALL expansion"
```

---

### Task 3: Planner — generate Repeat + Aggregate

**Files:**
- Modify: `src/sql/planner/mod.rs`

- [ ] **Step 1: Handle repeat in plan_select**

In `plan_select`, after building the base plan (FROM + WHERE + predicate pushdown), check for `select.repeat`:

```rust
fn plan_select(mut select: ResolvedSelect) -> Result<LogicalPlan, String> {
    // 1. FROM → base plan
    let mut current = match select.from { ... };

    // 2. WHERE → Filter
    if let Some(predicate) = select.filter { ... }

    // 3. If ROLLUP: insert Repeat node between join and aggregate
    if let Some(repeat_info) = select.repeat.take() {
        current = LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(current),
            repeat_column_ref_list: repeat_info.repeat_column_ref_list,
            grouping_ids: repeat_info.grouping_ids,
            all_rollup_columns: repeat_info.all_rollup_columns,
            grouping_fn_args: repeat_info.grouping_fn_args,
        });
    }

    // 4. GROUP BY / aggregation → Aggregate (existing code)
    // The GROUP BY keys now include all rollup columns.
    // The Aggregate groups by ALL keys — the Repeat's null patterns
    // make rows with NULLed columns group together.
    if select.has_aggregation || !select.group_by.is_empty() { ... }

    // ... rest unchanged
}
```

The Repeat node sits ABOVE the join/filter and BELOW the aggregate. The aggregate groups by all original GROUP BY keys — the null patterns from Repeat cause the desired rollup grouping.

- [ ] **Step 2: Build and test**

Run: `cargo build && cargo test -p novarocks`

Expected: The planner produces `Aggregate → Repeat → Join/Filter/Scan` for ROLLUP queries.

- [ ] **Step 3: Commit**

```bash
git add src/sql/planner/mod.rs
git commit -m "feat: planner generates Repeat node for ROLLUP queries"
```

---

### Task 4: Emitter — generate TRepeatNode

**Files:**
- Create: `src/sql/physical/emitter/emit_repeat.rs`
- Modify: `src/sql/physical/emitter/mod.rs`

- [ ] **Step 1: Create `emit_repeat.rs`**

The emitter takes the `RepeatPlanNode` and produces a Thrift `TPlanNode` with `repeat_node` set.

```rust
use std::collections::BTreeSet;
use crate::plan_nodes;
use crate::sql::plan::RepeatPlanNode;
use crate::sql::physical::resolve::ColumnBinding;
use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_repeat(&mut self, node: RepeatPlanNode) -> Result<EmitResult, String> {
        let child = self.emit_node(*node.input)?;

        let repeat_node_id = self.alloc_node();
        let output_tuple_id = self.alloc_tuple();

        // 1. Pass through all child columns to the output tuple
        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        let mut output_scope = crate::sql::physical::resolve::ExprScope::new();
        let mut child_slot_ids: Vec<i32> = Vec::new();

        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id, output_tuple_id, name,
                &binding.data_type, binding.nullable,
                idx as i32,
            );
            output_scope.add_column(None, name.clone(), ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: binding.data_type.clone(),
                nullable: true, // rollup can null any column
            });
            child_slot_ids.push(slot_id);
        }

        // 2. Add virtual slots for grouping_id and GROUPING() functions
        let num_virtual = 1 + node.grouping_fn_args.len(); // grouping_id + each GROUPING() call
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);
        let grouping_id_slot = self.alloc_slot();
        let grouping_id_col_pos = child_cols.len() as i32;
        self.desc_builder.add_slot(
            grouping_id_slot, output_tuple_id, "__grouping_id",
            &arrow::datatypes::DataType::Int64, false,
            grouping_id_col_pos,
        );
        virtual_slot_ids.push(grouping_id_slot);

        for (fn_idx, (fn_name, _)) in node.grouping_fn_args.iter().enumerate() {
            let slot = self.alloc_slot();
            self.desc_builder.add_slot(
                slot, output_tuple_id, fn_name,
                &arrow::datatypes::DataType::Int64, false,
                grouping_id_col_pos + 1 + fn_idx as i32,
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(output_tuple_id);

        // 3. Build slot_id_set_list: for each level, which child slots are NON-null
        let all_rollup_slot_ids: BTreeSet<i32> = node.all_rollup_columns.iter().filter_map(|col| {
            child_cols.iter().zip(child_slot_ids.iter()).find_map(|((name, _), sid)| {
                if name.to_lowercase() == col.to_lowercase() { Some(*sid) } else { None }
            })
        }).collect();

        let slot_id_set_list: Vec<BTreeSet<i32>> = node.repeat_column_ref_list.iter().map(|non_null_cols| {
            non_null_cols.iter().filter_map(|col| {
                child_cols.iter().zip(child_slot_ids.iter()).find_map(|((name, _), sid)| {
                    if name.to_lowercase() == col.to_lowercase() { Some(*sid) } else { None }
                })
            }).collect()
        }).collect();

        // 4. Build grouping_list: [grouping_ids, grouping_fn_1_values, ...]
        let repeat_times = node.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);
        // First row: grouping_id values
        grouping_list.push(node.grouping_ids.iter().map(|g| *g as i64).collect());
        // Additional rows: per-GROUPING() function values
        for (_fn_name, fn_args) in &node.grouping_fn_args {
            let mut values = Vec::with_capacity(repeat_times);
            for (level, non_null_cols) in node.repeat_column_ref_list.iter().enumerate() {
                let _ = level;
                let mut bits: u64 = 0;
                for (bit_pos, arg_col) in fn_args.iter().enumerate() {
                    let is_null = !non_null_cols.iter().any(|c| c.to_lowercase() == arg_col.to_lowercase());
                    if is_null { bits |= 1 << bit_pos; }
                }
                values.push(bits as i64);
            }
            grouping_list.push(values);
        }

        let repeat_id_list: Vec<i64> = node.grouping_ids.iter().map(|g| *g as i64).collect();

        // 5. Build TPlanNode
        let mut plan_node = super::super::nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
        })
    }
}
```

Note: The `TRepeatNode` struct fields use the types from the generated Thrift code. Check the actual field types (they may be `Vec<BTreeSet<i32>>` or `Vec<HashSet<i32>>` — adapt to match).

- [ ] **Step 2: Register in `emit_node` dispatch**

In `src/sql/physical/emitter/mod.rs`, add the module declaration and match arm:

```rust
mod emit_repeat;
```

In `emit_node`:
```rust
LogicalPlan::Repeat(node) => self.emit_repeat(node),
```

- [ ] **Step 3: Build and test**

Run: `cargo build`

Resolve any type mismatches between the emitter code and the actual Thrift-generated `TRepeatNode` struct.

Then: `cargo test -p novarocks`

- [ ] **Step 4: Commit**

```bash
git add src/sql/physical/emitter/emit_repeat.rs src/sql/physical/emitter/mod.rs
git commit -m "feat: emitter generates TRepeatNode for ROLLUP queries"
```

---

### Task 5: Integration testing and q18 verification

**Files:** None (testing only)

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p novarocks`

Fix any regressions.

- [ ] **Step 2: Build and restart standalone-server**

```bash
cargo build
pkill -f "novarocks standalone-server" 2>/dev/null; sleep 1
nohup ./target/debug/novarocks standalone-server >/dev/null 2>/dev/null &
sleep 4
```

Create catalog and test basic ROLLUP:

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS tpcds_cat PROPERTIES (...);
SET CATALOG tpcds_cat; USE tpcds;
SELECT s_state, grouping(s_state) as g, count(*) as cnt
FROM store_sales, store WHERE ss_store_sk = s_store_sk
GROUP BY ROLLUP(s_state) ORDER BY s_state LIMIT 5;
```

Expected: Correct results with grouping(s_state) = 0 for grouped rows, 1 for the total row.

- [ ] **Step 3: Verify q27, q36 (previously passing ROLLUP queries)**

Run q27 and q36 to verify they still produce correct results.

- [ ] **Step 4: Test q18 memory on release build**

```bash
cargo build --release
```

Run q18 with memory monitoring (30-second timeout). Expected: peak RSS ≤ 2.5 GB (down from 5.8 GB).

- [ ] **Step 5: Run TPC-DS q1-50 regression**

Run the full q1-50 suite on release build with 30s timeout and crash recovery. Verify no regressions from the 37-pass baseline.

- [ ] **Step 6: Clean up old code**

Remove the old `expand_rollup` function and the `replace_grouping_calls` / `replace_grouping_in_window` helper functions that are no longer needed. Also remove the `build_cast_texpr` UNION ALL null-type fix from `emit_set_op.rs` if it was only needed for ROLLUP.

Commit:
```bash
git add -A
git commit -m "chore: remove old UNION ALL ROLLUP expansion code"
```
