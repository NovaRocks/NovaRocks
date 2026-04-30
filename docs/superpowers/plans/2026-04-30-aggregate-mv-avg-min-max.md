# Aggregate MV AVG/MIN/MAX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend NovaRocks Aggregate MV incremental refresh to support AVG, MIN, and MAX aggregate functions, with INSERT-only semantics for MIN/MAX (DELETE triggers full refresh fall-back).

**Architecture:** Layout refactor introduces `AggregateStateRole` and `aggregate_index` so one logical aggregate can map to multiple physical state columns. AVG splits into `AvgSum` + `AvgCount` state columns; visible value derived as `sum/count` at materialize time. MIN/MAX use a single state column with `cmp::min/max`-based merge. Fall-back logic is injected at `mv_refresh.rs:155` where DELETE in lineage was already the error path.

**Tech Stack:** Rust, Apache Arrow (RecordBatch / Decimal128 / Float64), `sqlparser` (SQL AST), `rstest` style unit tests in same module.

**Spec:** `docs/superpowers/specs/2026-04-30-aggregate-mv-avg-min-max-design.md`

---

## File Structure

| File | Role |
|---|---|
| `src/connector/starrocks/managed/mv_shape.rs` | DDL classification: parse SQL aggregate calls, build `AggregateCallShape` |
| `src/connector/starrocks/managed/mv_agg_state.rs` | Layout, state merge, visible derivation, negate, validation, physical chunk I/O |
| `src/connector/starrocks/managed/mv_ddl.rs` | DDL flow + `build_mv_storage_layout` + uniqueness check (no-op extension) |
| `src/connector/starrocks/managed/mv_refresh.rs` | Refresh strategy dispatch + fall-back injection at DELETE branch |
| `sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql` | New SQL integration test |

Tests are inline `#[cfg(test)]` modules in the same files (project convention).

---

## Task 1: Refactor — introduce `AggregateStateRole` and `aggregate_index`

**Goal:** Lay groundwork without adding any new aggregate functions. After this task: existing COUNT/SUM tests still pass, new fields populated as `state_role=Single` / `aggregate_index` matching the 1:1 mapping.

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs:46-54` (struct), `:108-138` (build_aggregate_mv_layout), `:212-249` (merge_aggregate_state_batches), `:1478-1486` and `:1552-1560` (existing test fixtures)

- [ ] **Step 1.1: Add `AggregateStateRole` enum and extend `AggregateStateColumn`**

Edit `src/connector/starrocks/managed/mv_agg_state.rs:45-54`. Replace the existing `AggregateStateColumn` block:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) sql_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) visible_source_index: usize,
    pub(crate) aggregate_index: usize,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) state_role: AggregateStateRole,
    pub(crate) count_star: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateStateRole {
    /// Single state column: state value IS the aggregate result.
    /// Used by COUNT, SUM, MIN, MAX.
    Single,
    /// AVG sum sub-state (numeric type matching SUM coverage).
    AvgSum,
    /// AVG count sub-state (always Int64).
    AvgCount,
}
```

- [ ] **Step 1.2: Update `build_aggregate_mv_layout` to populate the new fields**

In `mv_agg_state.rs::build_aggregate_mv_layout` around line 108-138, change the `state_columns.push` block:

```rust
state_columns.push(AggregateStateColumn {
    name: state_name,
    data_type: visible.data_type.clone(),
    sql_type,
    nullable: visible.nullable,
    visible_source_index,
    aggregate_index,
    function: aggregate.function,
    state_role: AggregateStateRole::Single,
    count_star: matches!(aggregate.input, AggregateInput::Star),
});
```

(The `aggregate_index` is the loop variable from `for (aggregate_index, aggregate) in shape.aggregates.iter().enumerate()` already on line 108.)

- [ ] **Step 1.3: Update existing test fixtures to populate new fields**

`mv_agg_state.rs:1478-1486` and `:1552-1560` construct `AggregateStateColumn` literals in tests. Add the two new fields:

```rust
state_columns: vec![AggregateStateColumn {
    name: "__agg_state_c".to_string(),
    data_type: DataType::Int64,
    sql_type: SqlType::BigInt,
    nullable: false,
    visible_source_index: 0,
    aggregate_index: 0,
    function: AggregateFunctionKind::Count,
    state_role: AggregateStateRole::Single,
    count_star: true,
}],
```

Apply this same shape (with appropriate values) to every other `AggregateStateColumn { ... }` literal you find. Use `grep -n "AggregateStateColumn {" src/` to enumerate.

- [ ] **Step 1.4: Refactor `merge_aggregate_state_batches` visible derivation to be per-aggregate**

Currently `mv_agg_state.rs:230-239` writes visible per-state-column. Change to write per-aggregate. In the inner `for` loop (around line 231), keep the state-merge math, but **separate** the visible write step:

Replace lines 231-239 (`for (state_index, state_column) in layout.state_columns...` block):

```rust
// Step A: merge state values
for (state_index, state_column) in layout.state_columns.iter().enumerate() {
    let next_value = merge_state_value(
        row.state_values.get(state_index).cloned().unwrap_or(None),
        delta.state_values.get(state_index).cloned().unwrap_or(None),
        state_column,
    )?;
    row.state_values[state_index] = next_value;
}

// Step B: derive visible values per-aggregate (Single = direct copy of state)
update_visible_values_from_state(row, layout)?;
```

Add new helper near the bottom of the file (before tests):

```rust
fn update_visible_values_from_state(
    row: &mut AggregatePhysicalRow,
    layout: &AggregateMvLayout,
) -> Result<(), String> {
    // For Single state_role: visible = state value (1:1).
    // AVG / future multi-state aggregates handled in Task 2.
    for state_column in &layout.state_columns {
        if matches!(state_column.state_role, AggregateStateRole::Single) {
            let state_index = layout
                .state_columns
                .iter()
                .position(|c| std::ptr::eq(c, state_column))
                .expect("state column index lookup");
            row.visible_values[state_column.visible_source_index] =
                row.state_values[state_index].clone();
        }
    }
    Ok(())
}
```

(In Task 2 we'll extend this helper to handle AVG by iterating per-aggregate.)

- [ ] **Step 1.5: Run all existing tests**

Run: `cd /Users/harbor/project/NovaRocks/.claude/worktrees/practical-poitras-42a7e1 && cargo test -p novarocks --lib mv_agg_state 2>&1 | tail -30`

Expected: All existing tests pass (no new tests yet).

If anything fails, the most likely cause is a missed test fixture — `grep -rn "AggregateStateColumn {" src/` and add the two new fields.

- [ ] **Step 1.6: Lint and commit**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -20`

Then commit:

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs
git commit -m "$(cat <<'EOF'
refactor(mv): introduce AggregateStateRole and aggregate_index

Prepare AggregateStateColumn for 1-to-many aggregate-to-state-column
mapping (AVG splits into AvgSum + AvgCount in the next commit). Existing
COUNT and SUM aggregates use Single state_role and inherit unchanged
behavior. Visible value derivation lifts out of the merge loop into
update_visible_values_from_state, ready to handle multi-state aggregates.
EOF
)"
```

---

## Task 2: AVG support

**Goal:** AVG end-to-end — DDL classifier accepts AVG, layout produces 2 state columns, merge produces correct sum/count, visible derives correct DOUBLE/DECIMAL via division, negate flips both sub-states.

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs:42-46` (enum), `:333-345` (classify_aggregate_call), `:1113-1124` (rejects test)
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs:108-138` (build_aggregate_mv_layout), `:472-554` (merge_state_value), `:594-599` (zero_state_value), `:722-745` (validate_state_column_type), `:417-462` (negate_aggregate_state_chunks)

- [ ] **Step 2.1: Pre-flight verification — Decimal scale**

Per spec §11.2: before implementing AVG over Decimal128, verify the SQL analyzer's output type for `AVG(DECIMAL(p,s))`. Run a minimal SELECT:

```bash
cd /Users/harbor/project/NovaRocks/.claude/worktrees/practical-poitras-42a7e1
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
mysql -h 127.0.0.1 -P 9030 -u root -e "
CREATE DATABASE test_avg;
USE test_avg;
CREATE TABLE t (k INT, d DECIMAL(20,4));
INSERT INTO t VALUES (1, 10.0000), (1, 20.0000);
SELECT k, AVG(d) FROM t GROUP BY k;
" 2>&1 | tail -20
kill $SERVER_PID
```

Note the output column type. Document the actual scale in the commit message. **If the scale differs from `s+4`, the `materialize_visible_value_avg` Decimal cast (Step 2.10) MUST cast to the analyzer's reported scale.**

- [ ] **Step 2.2: Add `Avg` variant to `AggregateFunctionKind`**

Edit `src/connector/starrocks/managed/mv_shape.rs:42-46`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,  // Reserved for Task 3 — declare now to avoid two enum bumps
    Max,  // Reserved for Task 3
}
```

This will produce dead-code warnings for `Min` / `Max` until Task 3; that's expected. Add `#[allow(dead_code)]` if necessary, or accept the warning.

- [ ] **Step 2.3: Write failing test for AVG DDL acceptance**

Add to `mv_shape.rs` test module (inside `mod tests`):

```rust
#[test]
fn accepts_avg_aggregate() {
    let shape = classify_sql(
        "select k1, avg(v2) as a from ice.ns.orders group by k1",
    )
    .expect("query should be accepted");
    let IncrementalMvShape::Aggregate(shape) = shape else {
        panic!("expected aggregate shape");
    };
    assert_eq!(shape.aggregates.len(), 1);
    assert_eq!(shape.aggregates[0].output_name, "a");
    assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Avg);
    assert_eq!(
        shape.aggregates[0].input,
        AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
    );
}

#[test]
fn rejects_avg_star_and_avg_distinct() {
    assert_rejects_with(
        "select k1, avg(*) from ice.ns.orders group by k1",
        "AVG aggregate requires a column expression argument",
    );
    assert_rejects_with(
        "select k1, avg(distinct v2) from ice.ns.orders group by k1",
        "incremental aggregate MV",
    );
}
```

Also delete `"select k1, avg(v2) from ice.ns.orders group by k1",` from the existing `rejects_unsupported_aggregate_functions` test (around line 1113-1124) since AVG is now accepted.

- [ ] **Step 2.4: Run test to verify failure**

Run: `cargo test -p novarocks --lib mv_shape::tests::accepts_avg_aggregate 2>&1 | tail -10`

Expected: FAIL — `"avg" not handled` or similar (current code falls through to `aggregate_error()`).

- [ ] **Step 2.5: Implement AVG in `classify_aggregate_call`**

Edit `mv_shape.rs:333-338`:

```rust
let function_name = function.name.to_string().to_ascii_lowercase();
let (function, input) = match function_name.as_str() {
    "count" => classify_count_input(&args.args)?,
    "sum" => (AggregateFunctionKind::Sum, classify_sum_input(&args.args)?),
    "avg" => (AggregateFunctionKind::Avg, classify_avg_input(&args.args)?),
    _ => return Err(aggregate_error()),
};
```

Add the helper after `classify_sum_input` (around line 377):

```rust
fn classify_avg_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}
```

- [ ] **Step 2.6: Run AVG DDL tests to verify pass**

Run: `cargo test -p novarocks --lib mv_shape::tests::accepts_avg_aggregate mv_shape::tests::rejects_avg_star_and_avg_distinct 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 2.7: Write failing test for AVG layout (2 state columns)**

Add to `mv_agg_state.rs` test module:

```rust
#[test]
fn build_layout_avg_produces_two_state_columns() {
    use crate::connector::starrocks::managed::mv_shape::{
        AggregateCallShape, AggregateInput, AggregateMvShape, GroupKeyShape,
        VisibleAggregateOutput,
    };
    use crate::sql::analysis::OutputColumn;
    use sqlparser::ast::ObjectName;

    let shape = AggregateMvShape {
        base_table: ObjectName(vec![]),
        group_keys: vec![GroupKeyShape {
            output_name: "k".to_string(),
            expr: sqlparser::ast::Expr::Identifier("k".into()),
        }],
        aggregates: vec![AggregateCallShape {
            output_name: "a".to_string(),
            function: AggregateFunctionKind::Avg,
            input: AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v".into()))),
        }],
        visible_outputs: vec![
            VisibleAggregateOutput::GroupKey(0),
            VisibleAggregateOutput::Aggregate(0),
        ],
    };
    let outputs = vec![
        OutputColumn {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        OutputColumn {
            name: "a".to_string(),
            data_type: DataType::Float64,
            nullable: true,
        },
    ];
    let layout = build_aggregate_mv_layout(&shape, &outputs).expect("layout build");
    assert_eq!(layout.state_columns.len(), 2);
    assert_eq!(layout.state_columns[0].state_role, AggregateStateRole::AvgSum);
    assert_eq!(layout.state_columns[0].name, "__agg_state_a__sum");
    assert_eq!(layout.state_columns[0].aggregate_index, 0);
    assert_eq!(layout.state_columns[1].state_role, AggregateStateRole::AvgCount);
    assert_eq!(layout.state_columns[1].name, "__agg_state_a__count");
    assert_eq!(layout.state_columns[1].data_type, DataType::Int64);
    assert_eq!(layout.state_columns[1].aggregate_index, 0);
}
```

- [ ] **Step 2.8: Run test to verify failure**

Run: `cargo test -p novarocks --lib build_layout_avg_produces_two_state_columns 2>&1 | tail -10`

Expected: FAIL — only 1 state column produced (existing layout logic).

- [ ] **Step 2.9: Implement AVG layout fan-out**

Edit `mv_agg_state.rs:108-138`. Replace the `for (aggregate_index, aggregate) in shape.aggregates.iter().enumerate() { ... }` body with multi-column emission for AVG:

```rust
for (aggregate_index, aggregate) in shape.aggregates.iter().enumerate() {
    let visible_source_index = aggregate_visible_source_index(shape, aggregate_index)?;
    let visible = output_columns.get(visible_source_index).ok_or_else(|| {
        format!(
            "aggregate MV visible source index out of range: aggregate_index={aggregate_index} source_index={visible_source_index}"
        )
    })?;
    let visible_sql_type = mv_ddl::arrow_data_type_to_sql_type(&visible.data_type)?;
    let sanitized = sanitize_state_column_name(&aggregate.output_name);
    let count_star = matches!(aggregate.input, AggregateInput::Star);

    match aggregate.function {
        AggregateFunctionKind::Count
        | AggregateFunctionKind::Sum
        | AggregateFunctionKind::Min
        | AggregateFunctionKind::Max => {
            let state_name = format!("{}{}", AGG_STATE_PREFIX, sanitized);
            validate_state_column_type(
                aggregate.function,
                AggregateStateRole::Single,
                &visible.data_type,
                &state_name,
            )?;
            physical_columns.push(managed_physical_column(
                state_name.clone(),
                visible_sql_type.clone(),
                visible.nullable,
                false,
                false,
            ));
            state_columns.push(AggregateStateColumn {
                name: state_name,
                data_type: visible.data_type.clone(),
                sql_type: visible_sql_type,
                nullable: visible.nullable,
                visible_source_index,
                aggregate_index,
                function: aggregate.function,
                state_role: AggregateStateRole::Single,
                count_star,
            });
        }
        AggregateFunctionKind::Avg => {
            let (sum_dt, sum_sql) = avg_sum_state_type(&visible.data_type)
                .ok_or_else(|| format!(
                    "AVG state type is unsupported for column `__agg_state_{sanitized}__sum`: {:?}",
                    visible.data_type
                ))?;
            let count_dt = DataType::Int64;
            let count_sql = SqlType::BigInt;

            let sum_name = format!("{}{}__sum", AGG_STATE_PREFIX, sanitized);
            let count_name = format!("{}{}__count", AGG_STATE_PREFIX, sanitized);

            validate_state_column_type(
                AggregateFunctionKind::Avg,
                AggregateStateRole::AvgSum,
                &sum_dt,
                &sum_name,
            )?;

            physical_columns.push(managed_physical_column(
                sum_name.clone(),
                sum_sql.clone(),
                /* nullable */ true,
                false,
                false,
            ));
            physical_columns.push(managed_physical_column(
                count_name.clone(),
                count_sql.clone(),
                /* nullable */ false,
                false,
                false,
            ));

            state_columns.push(AggregateStateColumn {
                name: sum_name,
                data_type: sum_dt,
                sql_type: sum_sql,
                nullable: true,
                visible_source_index,
                aggregate_index,
                function: AggregateFunctionKind::Avg,
                state_role: AggregateStateRole::AvgSum,
                count_star: false,
            });
            state_columns.push(AggregateStateColumn {
                name: count_name,
                data_type: count_dt,
                sql_type: count_sql,
                nullable: false,
                visible_source_index,
                aggregate_index,
                function: AggregateFunctionKind::Avg,
                state_role: AggregateStateRole::AvgCount,
                count_star: false,
            });
        }
    }
}
```

Add the helper near `validate_state_column_type` (around line 745):

```rust
fn avg_sum_state_type(visible_dt: &DataType) -> Option<(DataType, SqlType)> {
    match visible_dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some((DataType::Int64, SqlType::BigInt))
        }
        DataType::Float32 | DataType::Float64 => None,  // AVG over float not supported in Phase 1
        DataType::Decimal128(p, s) => Some((
            DataType::Decimal128(*p, *s),
            SqlType::Decimal(*p, *s as u8),
        )),
        _ => None,
    }
}
```

(Adjust `SqlType::Decimal` exact constructor based on actual ast — check `mv_ddl::arrow_data_type_to_sql_type` for reference.)

Update `validate_state_column_type` signature to take `state_role`:

```rust
fn validate_state_column_type(
    function: AggregateFunctionKind,
    state_role: AggregateStateRole,
    data_type: &DataType,
    state_name: &str,
) -> Result<(), String> {
    match (function, state_role) {
        (AggregateFunctionKind::Count, AggregateStateRole::Single) => match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(()),
            other => Err(format!(
                "aggregate MV COUNT state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Sum, AggregateStateRole::Single) => match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::Decimal128(_, _) => Ok(()),
            other => Err(format!(
                "aggregate MV SUM state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum) => match data_type {
            DataType::Int64 | DataType::Decimal128(_, _) => Ok(()),
            other => Err(format!(
                "AVG state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => match data_type {
            DataType::Int64 => Ok(()),
            other => Err(format!(
                "AVG count state must be Int64 for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Min, AggregateStateRole::Single)
        | (AggregateFunctionKind::Max, AggregateStateRole::Single) => match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::Float32 | DataType::Float64
            | DataType::Decimal128(_, _) | DataType::Utf8
            | DataType::Date32 | DataType::Timestamp(_, _) => Ok(()),
            DataType::Boolean => Err(format!(
                "MIN/MAX state type is unsupported for column `{state_name}`: Boolean"
            )),
            other => Err(format!(
                "MIN/MAX state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (function, role) => Err(format!(
            "internal: invalid (function, state_role) pair: ({function:?}, {role:?}) on column `{state_name}`"
        )),
    }
}
```

- [ ] **Step 2.10: Run AVG layout test to verify pass**

Run: `cargo test -p novarocks --lib build_layout_avg_produces_two_state_columns 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 2.11: Write failing test for AVG visible derivation**

Add to `mv_agg_state.rs` test module:

```rust
#[test]
fn materialize_visible_value_avg_int_to_double() {
    // Layout: AVG(v) over Int64 input, Float64 visible.
    let layout = make_avg_layout_int_to_double();
    let row = AggregatePhysicalRow {
        row_id: "g".to_string(),
        visible_values: vec![None],         // will be derived
        state_values: vec![
            Some(AggScalarValue::Int64(30)),  // sum
            Some(AggScalarValue::Int64(4)),   // count
        ],
    };
    let mut row = row;
    update_visible_values_from_state(&mut row, &layout).expect("derive");
    assert_eq!(row.visible_values[0], Some(AggScalarValue::Float64(7.5)));
}

#[test]
fn materialize_visible_value_avg_count_zero_returns_null() {
    let layout = make_avg_layout_int_to_double();
    let mut row = AggregatePhysicalRow {
        row_id: "g".to_string(),
        visible_values: vec![Some(AggScalarValue::Float64(0.0))],
        state_values: vec![
            None,                              // sum (no non-null inputs)
            Some(AggScalarValue::Int64(0)),    // count
        ],
    };
    update_visible_values_from_state(&mut row, &layout).expect("derive");
    assert_eq!(row.visible_values[0], None);
}

fn make_avg_layout_int_to_double() -> AggregateMvLayout {
    AggregateMvLayout {
        row_id_column: managed_physical_column(
            ROW_ID_COLUMN.to_string(),
            SqlType::String, false, false, true,
        ),
        visible_columns: vec![AggregateVisibleColumn {
            name: "a".to_string(),
            data_type: DataType::Float64,
            sql_type: SqlType::Double,
            nullable: true,
            source_index: 0,
        }],
        state_columns: vec![
            AggregateStateColumn {
                name: "__agg_state_a__sum".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: true,
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Avg,
                state_role: AggregateStateRole::AvgSum,
                count_star: false,
            },
            AggregateStateColumn {
                name: "__agg_state_a__count".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Avg,
                state_role: AggregateStateRole::AvgCount,
                count_star: false,
            },
        ],
        group_key_source_indexes: Vec::new(),
        physical_columns: Vec::new(),
    }
}
```

(Note: `AggScalarValue::Float64` may not exist yet — check `src/exec/expr/agg/spec.rs` to confirm. If not, add the variant and `agg_scalar_from_array` / `build_agg_scalar_array` Float64 cases as a sub-step before the test.)

- [ ] **Step 2.12: Confirm `AggScalarValue::Float64` array I/O is wired**

Pre-verified: `AggScalarValue::Float64(f64)` exists in `src/exec/expr/agg/functions/common.rs:184`. `Float32` does NOT exist as a variant; arrays of `Float32` should coerce up to `Float64` (matching the Int8/16/32 → Int64 pattern at `common.rs:214-243`).

Run: `grep -n "DataType::Float32\|DataType::Float64" /Users/harbor/project/NovaRocks/.claude/worktrees/practical-poitras-42a7e1/src/exec/expr/agg/functions/common.rs`

If `Float64` array I/O is missing in `scalar_from_array` and `build_scalar_array`, add it now (mirror the Int64 case). For `Float32`, add a coercion case (`arr.value(row) as f64 → AggScalarValue::Float64`).

This may be a sub-commit prior to Step 2.13. After: re-run existing tests to ensure no regression.

- [ ] **Step 2.13: Run test to verify failure**

Run: `cargo test -p novarocks --lib materialize_visible_value_avg 2>&1 | tail -15`

Expected: FAIL — current `update_visible_values_from_state` only handles Single role.

- [ ] **Step 2.14: Implement AVG visible derivation**

Replace `update_visible_values_from_state` body with per-aggregate dispatch:

```rust
fn update_visible_values_from_state(
    row: &mut AggregatePhysicalRow,
    layout: &AggregateMvLayout,
) -> Result<(), String> {
    // Group state columns by aggregate_index.
    let mut by_aggregate: HashMap<usize, Vec<usize>> = HashMap::new();
    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        by_aggregate
            .entry(state_column.aggregate_index)
            .or_default()
            .push(state_index);
    }

    for state_indexes in by_aggregate.values() {
        let primary = &layout.state_columns[state_indexes[0]];
        match primary.function {
            AggregateFunctionKind::Count
            | AggregateFunctionKind::Sum
            | AggregateFunctionKind::Min
            | AggregateFunctionKind::Max => {
                // Single state: visible = state value.
                let state_index = state_indexes[0];
                let state_column = &layout.state_columns[state_index];
                row.visible_values[state_column.visible_source_index] =
                    row.state_values[state_index].clone();
            }
            AggregateFunctionKind::Avg => {
                let (sum_idx, count_idx) = avg_state_indexes(layout, state_indexes)?;
                let visible_idx = layout.state_columns[sum_idx].visible_source_index;
                let visible_dt = &layout.visible_columns[visible_idx].data_type;
                let sum_val = row.state_values[sum_idx].clone();
                let count_val = row.state_values[count_idx].clone();
                row.visible_values[visible_idx] =
                    derive_avg_visible(sum_val, count_val, visible_dt)?;
            }
        }
    }
    Ok(())
}

fn avg_state_indexes(
    layout: &AggregateMvLayout,
    state_indexes: &[usize],
) -> Result<(usize, usize), String> {
    let mut sum_idx = None;
    let mut count_idx = None;
    for &i in state_indexes {
        match layout.state_columns[i].state_role {
            AggregateStateRole::AvgSum => sum_idx = Some(i),
            AggregateStateRole::AvgCount => count_idx = Some(i),
            AggregateStateRole::Single => {
                return Err(format!(
                    "internal: AVG aggregate has Single state_role on column index {i}"
                ));
            }
        }
    }
    Ok((
        sum_idx.ok_or("internal: AVG missing AvgSum state column")?,
        count_idx.ok_or("internal: AVG missing AvgCount state column")?,
    ))
}

fn derive_avg_visible(
    sum: Option<AggScalarValue>,
    count: Option<AggScalarValue>,
    visible_dt: &DataType,
) -> Result<Option<AggScalarValue>, String> {
    let count_i64 = match count {
        Some(AggScalarValue::Int64(c)) => c,
        Some(other) => return Err(format!(
            "AVG count state must be Int64, got {other:?}"
        )),
        None => return Err("AVG count state must not be NULL".to_string()),
    };
    if count_i64 == 0 {
        return Ok(None);
    }
    let sum = match sum {
        Some(v) => v,
        None => return Ok(None),
    };
    match (visible_dt, sum) {
        (DataType::Float64, AggScalarValue::Int64(s)) => {
            Ok(Some(AggScalarValue::Float64((s as f64) / (count_i64 as f64))))
        }
        (DataType::Decimal128(p, scale), AggScalarValue::Decimal128(s)) => {
            let count_decimal = (count_i64 as i128)
                .checked_mul(10_i128.pow(*scale as u32))
                .ok_or("AVG count scale overflow")?;
            let result = s.checked_mul(10_i128.pow(*scale as u32))
                .ok_or("AVG sum scale overflow")?
                .checked_div(count_decimal)
                .ok_or("AVG decimal divide failed")?;
            Ok(Some(AggScalarValue::Decimal128(result)))
        }
        (dt, sum) => Err(format!(
            "AVG visible derivation unsupported: visible_dt={dt:?} sum={sum:?}"
        )),
    }
}
```

(The Decimal cast formula above is a placeholder; adjust per pre-flight Step 2.1 findings to match analyzer's reported visible scale.)

- [ ] **Step 2.15: Run AVG visible tests to verify pass**

Run: `cargo test -p novarocks --lib materialize_visible_value_avg 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 2.16: Write failing test for AVG state merge**

Add to test module:

```rust
#[test]
fn merge_state_value_avg_sum_int64() {
    let column = AggregateStateColumn {
        name: "__agg_state_a__sum".to_string(),
        data_type: DataType::Int64,
        sql_type: SqlType::BigInt,
        nullable: true,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Avg,
        state_role: AggregateStateRole::AvgSum,
        count_star: false,
    };
    // Some + Some → sum
    let r = merge_state_value(
        Some(AggScalarValue::Int64(10)),
        Some(AggScalarValue::Int64(20)),
        &column,
    ).expect("merge");
    assert_eq!(r, Some(AggScalarValue::Int64(30)));
    // Some + None → Some
    let r = merge_state_value(
        Some(AggScalarValue::Int64(10)),
        None,
        &column,
    ).expect("merge");
    assert_eq!(r, Some(AggScalarValue::Int64(10)));
    // None + None → None
    let r = merge_state_value(None, None, &column).expect("merge");
    assert_eq!(r, None);
}

#[test]
fn merge_state_value_avg_count_int64() {
    let column = AggregateStateColumn {
        name: "__agg_state_a__count".to_string(),
        data_type: DataType::Int64,
        sql_type: SqlType::BigInt,
        nullable: false,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Avg,
        state_role: AggregateStateRole::AvgCount,
        count_star: false,
    };
    let r = merge_state_value(
        Some(AggScalarValue::Int64(2)),
        Some(AggScalarValue::Int64(3)),
        &column,
    ).expect("merge");
    assert_eq!(r, Some(AggScalarValue::Int64(5)));
}
```

- [ ] **Step 2.17: Run test to verify failure**

Run: `cargo test -p novarocks --lib merge_state_value_avg 2>&1 | tail -10`

Expected: FAIL — `merge_state_value` returns error for `Avg` function.

- [ ] **Step 2.18: Implement Avg merge dispatch**

Edit `mv_agg_state.rs:472-481`:

```rust
fn merge_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    match (state_column.function, state_column.state_role) {
        (AggregateFunctionKind::Count, AggregateStateRole::Single) => {
            merge_count_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Sum, AggregateStateRole::Single) => {
            merge_sum_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum) => {
            // Same arithmetic as SUM (NULL-permissive int/decimal addition).
            merge_sum_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => {
            // Same arithmetic as COUNT (NULL-rejecting int addition).
            merge_count_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Min, AggregateStateRole::Single) => {
            // Implemented in Task 3
            Err("internal: MIN merge not yet implemented".to_string())
        }
        (AggregateFunctionKind::Max, AggregateStateRole::Single) => {
            Err("internal: MAX merge not yet implemented".to_string())
        }
        (function, role) => Err(format!(
            "internal: invalid (function, state_role): ({function:?}, {role:?})"
        )),
    }
}
```

Update `zero_state_value` (`mv_agg_state.rs:594-599`):

```rust
fn zero_state_value(state_column: &AggregateStateColumn) -> Option<AggScalarValue> {
    match (state_column.function, state_column.state_role) {
        (AggregateFunctionKind::Count, _) => Some(AggScalarValue::Int64(0)),
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => {
            Some(AggScalarValue::Int64(0))
        }
        (AggregateFunctionKind::Sum, _)
        | (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum)
        | (AggregateFunctionKind::Min, _)
        | (AggregateFunctionKind::Max, _) => None,
        (_, AggregateStateRole::Single) => None,
    }
}
```

Update `validate_loaded_count_state` callers — `validate_loaded_physical_row` at `:629`. Apply count-state validation to both `Single` Count and `AvgCount` rules:

```rust
for (state_index, state_column) in layout.state_columns.iter().enumerate() {
    let state_value = &state_values[state_index];
    let is_count_role = matches!(
        (state_column.function, state_column.state_role),
        (AggregateFunctionKind::Count, AggregateStateRole::Single)
            | (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount)
    );
    if is_count_role {
        validate_loaded_count_state(
            state_value,
            &state_column.name,
            row_id,
            state_column.count_star,
            allow_negative_counts,
        )?;
    }
    // ... existing visible-state equality check needs adjustment for AVG
}
```

The existing equality check `agg_scalar_values_equal(visible_value, state_value)` doesn't apply to AVG (visible is the divided result). Update the loop to skip the equality check when `state_role != Single`:

```rust
if !allow_negative_counts && matches!(state_column.state_role, AggregateStateRole::Single) {
    let visible_value = visible_values
        .get(state_column.visible_source_index)
        .ok_or_else(|| /* existing error */)?;
    if !agg_scalar_values_equal(visible_value, state_value) {
        return Err(/* existing error */);
    }
}
```

- [ ] **Step 2.19: Run AVG merge tests to verify pass**

Run: `cargo test -p novarocks --lib merge_state_value_avg 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 2.20: Write failing test for AVG negate (both sub-states flip)**

```rust
#[test]
fn negate_aggregate_state_chunks_avg_flips_both_substates() {
    let layout = make_avg_layout_int_to_double();
    let schema = Arc::new(physical_schema(&layout));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["g1"])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![Some(7.5)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![30])) as ArrayRef,
            Arc::new(Int64Array::from(vec![4])) as ArrayRef,
        ],
    )
    .expect("batch");
    let chunk = record_batch_to_chunk(batch).expect("chunk");
    let negated = negate_aggregate_state_chunks(vec![chunk], &layout).expect("negate");
    let sum = negated[0].batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    let cnt = negated[0].batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(sum.value(0), -30);
    assert_eq!(cnt.value(0), -4);
}
```

- [ ] **Step 2.21: Run + verify pass (no implementation change should be needed since arrow `neg` works on Int64)**

Run: `cargo test -p novarocks --lib negate_aggregate_state_chunks_avg 2>&1 | tail -10`

Expected: PASS without code change. If FAIL, inspect — Decimal128 path may need attention.

- [ ] **Step 2.22: End-to-end merge test**

```rust
#[test]
fn merge_aggregate_state_batches_avg_int_to_double() {
    let layout = make_avg_layout_int_to_double();
    let mut old: HashMap<String, AggregatePhysicalRow> = HashMap::new();
    old.insert("g".to_string(), AggregatePhysicalRow {
        row_id: "g".to_string(),
        visible_values: vec![Some(AggScalarValue::Float64(5.0))],
        state_values: vec![
            Some(AggScalarValue::Int64(10)),
            Some(AggScalarValue::Int64(2)),
        ],
    });
    // Delta: insert chunk with sum=20 count=2 (avg=10).
    let schema = Arc::new(physical_schema(&layout));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["g"])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![Some(10.0)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )
    .expect("batch");
    let delta = vec![record_batch_to_chunk(batch).expect("chunk")];
    let merged = merge_aggregate_state_batches(&old, &delta, &layout).expect("merge");
    assert_eq!(merged.len(), 1);
    let visible = merged[0].batch.column(1).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap();
    assert_eq!(visible.value(0), 7.5);  // (10+20) / (2+2) = 7.5
}
```

Run: `cargo test -p novarocks --lib merge_aggregate_state_batches_avg 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 2.23: Lint + commit**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -20 && cargo test -p novarocks --lib mv_agg_state 2>&1 | tail -20 && cargo test -p novarocks --lib mv_shape 2>&1 | tail -20`

Then commit:

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/mv_shape.rs src/exec/expr/agg/spec.rs
git commit -m "$(cat <<'EOF'
feat(mv): support AVG aggregate in incremental MV

AVG splits into AvgSum and AvgCount state columns under the new
AggregateStateRole abstraction. Visible value is derived as sum/count
at materialize and merge time. Output type follows analyzer's promoted
type (Int input -> Float64 visible; Decimal input -> Decimal visible
with scale matching analyzer-reported scale). NULL semantics: empty
group -> AvgCount=0 -> visible NULL. AVG over FLOAT/DOUBLE remains
unsupported in this iteration (see roadmap §2.3).

DECIMAL scale verified against analyzer in pre-flight: <document
observed scale here>.
EOF
)"
```

---

## Task 3: MIN/MAX support (INSERT-only)

**Goal:** MIN and MAX classification + layout (Single state) + merge logic with NULL handling and Float NaN handling. Negate path panics for MIN/MAX (must not enter — fall-back covers DELETE).

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs:333-345` (classify_aggregate_call), test module
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs:472-554` (merge), `:417-462` (negate panic)

- [ ] **Step 3.1: Write failing tests for MIN/MAX DDL acceptance and rejection**

Add to `mv_shape.rs` test module:

```rust
#[test]
fn accepts_min_max_aggregates() {
    let shape = classify_sql(
        "select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1",
    )
    .expect("query should be accepted");
    let IncrementalMvShape::Aggregate(shape) = shape else {
        panic!("expected aggregate shape");
    };
    assert_eq!(shape.aggregates.len(), 2);
    assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Min);
    assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Max);
}

#[test]
fn rejects_min_max_star() {
    assert_rejects_with(
        "select k1, min(*) from ice.ns.orders group by k1",
        "MIN/MAX aggregate requires a column expression argument",
    );
    assert_rejects_with(
        "select k1, max(*) from ice.ns.orders group by k1",
        "MIN/MAX aggregate requires a column expression argument",
    );
}
```

Also delete `"select k1, min(v2) from ice.ns.orders group by k1",` from `rejects_unsupported_aggregate_functions`.

- [ ] **Step 3.2: Verify failure**

Run: `cargo test -p novarocks --lib accepts_min_max rejects_min_max 2>&1 | tail -10`

Expected: FAIL.

- [ ] **Step 3.3: Implement classifier**

Edit `mv_shape.rs:333-338`:

```rust
let (function, input) = match function_name.as_str() {
    "count" => classify_count_input(&args.args)?,
    "sum" => (AggregateFunctionKind::Sum, classify_sum_input(&args.args)?),
    "avg" => (AggregateFunctionKind::Avg, classify_avg_input(&args.args)?),
    "min" => (AggregateFunctionKind::Min, classify_min_max_input(&args.args)?),
    "max" => (AggregateFunctionKind::Max, classify_min_max_input(&args.args)?),
    _ => return Err(aggregate_error()),
};
```

Add helper after `classify_avg_input`:

```rust
fn classify_min_max_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}
```

- [ ] **Step 3.4: Verify pass**

Run: `cargo test -p novarocks --lib mv_shape 2>&1 | tail -10`

Expected: All shape tests pass.

- [ ] **Step 3.5: Write failing tests for MIN/MAX merge with NULL**

Add to `mv_agg_state.rs` test module:

```rust
#[test]
fn merge_state_value_min_int64() {
    let column = AggregateStateColumn {
        name: "__agg_state_mn".to_string(),
        data_type: DataType::Int64,
        sql_type: SqlType::BigInt,
        nullable: true,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Min,
        state_role: AggregateStateRole::Single,
        count_star: false,
    };
    let r = merge_state_value(
        Some(AggScalarValue::Int64(5)),
        Some(AggScalarValue::Int64(3)),
        &column,
    ).unwrap();
    assert_eq!(r, Some(AggScalarValue::Int64(3)));
    let r = merge_state_value(Some(AggScalarValue::Int64(5)), None, &column).unwrap();
    assert_eq!(r, Some(AggScalarValue::Int64(5)));
    let r = merge_state_value(None, Some(AggScalarValue::Int64(5)), &column).unwrap();
    assert_eq!(r, Some(AggScalarValue::Int64(5)));
    let r = merge_state_value(None, None, &column).unwrap();
    assert_eq!(r, None);
}

#[test]
fn merge_state_value_max_utf8() {
    let column = AggregateStateColumn {
        name: "__agg_state_mx".to_string(),
        data_type: DataType::Utf8,
        sql_type: SqlType::String,
        nullable: true,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Max,
        state_role: AggregateStateRole::Single,
        count_star: false,
    };
    let r = merge_state_value(
        Some(AggScalarValue::Utf8("apple".to_string())),
        Some(AggScalarValue::Utf8("banana".to_string())),
        &column,
    ).unwrap();
    assert_eq!(r, Some(AggScalarValue::Utf8("banana".to_string())));
}

#[test]
fn merge_state_value_min_float64_nan_handling() {
    let column = AggregateStateColumn {
        name: "__agg_state_mn".to_string(),
        data_type: DataType::Float64,
        sql_type: SqlType::Double,
        nullable: true,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Min,
        state_role: AggregateStateRole::Single,
        count_star: false,
    };
    // NaN + non-NaN -> non-NaN
    let r = merge_state_value(
        Some(AggScalarValue::Float64(f64::NAN)),
        Some(AggScalarValue::Float64(5.0)),
        &column,
    ).unwrap();
    let v = match r { Some(AggScalarValue::Float64(v)) => v, _ => panic!() };
    assert!(!v.is_nan() && v == 5.0);
    // NaN + NaN -> NaN
    let r = merge_state_value(
        Some(AggScalarValue::Float64(f64::NAN)),
        Some(AggScalarValue::Float64(f64::NAN)),
        &column,
    ).unwrap();
    let v = match r { Some(AggScalarValue::Float64(v)) => v, _ => panic!() };
    assert!(v.is_nan());
}
```

- [ ] **Step 3.6: Verify failure**

Run: `cargo test -p novarocks --lib merge_state_value_min merge_state_value_max 2>&1 | tail -15`

Expected: FAIL — current `merge_state_value` returns `"internal: MIN merge not yet implemented"`.

- [ ] **Step 3.7: Implement merge for MIN/MAX**

Replace the Task 2 placeholder branches in `merge_state_value`:

```rust
(AggregateFunctionKind::Min, AggregateStateRole::Single) => {
    merge_min_max_state_value(old, delta, state_column, MinMax::Min)
}
(AggregateFunctionKind::Max, AggregateStateRole::Single) => {
    merge_min_max_state_value(old, delta, state_column, MinMax::Max)
}
```

Add helper near the SUM helper (around line 554):

```rust
#[derive(Clone, Copy)]
enum MinMax { Min, Max }

fn merge_min_max_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
    op: MinMax,
) -> Result<Option<AggScalarValue>, String> {
    match (old, delta) {
        (None, None) => Ok(None),
        (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
        (Some(a), Some(b)) => Ok(Some(min_max_pair(a, b, state_column, op)?)),
    }
}

fn min_max_pair(
    a: AggScalarValue,
    b: AggScalarValue,
    state_column: &AggregateStateColumn,
    op: MinMax,
) -> Result<AggScalarValue, String> {
    use AggScalarValue::*;
    match (a, b) {
        (Int64(x), Int64(y)) => Ok(Int64(pick_int(x, y, op))),
        (Float64(x), Float64(y)) => Ok(Float64(pick_float(x, y, op))),
        (Decimal128(x), Decimal128(y)) => Ok(Decimal128(pick_int128(x, y, op))),
        (Utf8(x), Utf8(y)) => Ok(Utf8(pick_str(x, y, op))),
        (Date32(x), Date32(y)) => Ok(Date32(pick_int_i32(x, y, op))),
        (Timestamp(x), Timestamp(y)) => Ok(Timestamp(pick_int(x, y, op))),
        (a, b) => Err(format!(
            "MIN/MAX merge type mismatch on column `{}`: a={a:?}, b={b:?}",
            state_column.name
        )),
    }
}

fn pick_int(x: i64, y: i64, op: MinMax) -> i64 {
    match op { MinMax::Min => x.min(y), MinMax::Max => x.max(y) }
}
fn pick_int_i32(x: i32, y: i32, op: MinMax) -> i32 {
    match op { MinMax::Min => x.min(y), MinMax::Max => x.max(y) }
}
fn pick_int128(x: i128, y: i128, op: MinMax) -> i128 {
    match op { MinMax::Min => x.min(y), MinMax::Max => x.max(y) }
}
fn pick_str(x: String, y: String, op: MinMax) -> String {
    match op { MinMax::Min => x.min(y), MinMax::Max => x.max(y) }
}
fn pick_float(x: f64, y: f64, op: MinMax) -> f64 {
    if x.is_nan() && y.is_nan() { return f64::NAN; }
    if x.is_nan() { return y; }
    if y.is_nan() { return x; }
    match op { MinMax::Min => x.min(y), MinMax::Max => x.max(y) }
}
```

Note: `Float32` input arrays are coerced to `AggScalarValue::Float64` upstream (per Step 2.12 coercion). `min_max_pair` only needs Float64 case.

- [ ] **Step 3.8: Verify pass**

Run: `cargo test -p novarocks --lib merge_state_value_min merge_state_value_max 2>&1 | tail -15`

Expected: PASS.

- [ ] **Step 3.9: Write failing test for MIN/MAX negate panic**

```rust
#[test]
#[should_panic(expected = "MIN/MAX state should not enter negate path")]
fn negate_aggregate_state_chunks_min_panics() {
    let layout = AggregateMvLayout {
        row_id_column: managed_physical_column(
            ROW_ID_COLUMN.to_string(),
            SqlType::String, false, false, true,
        ),
        visible_columns: vec![AggregateVisibleColumn {
            name: "mn".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: true,
            source_index: 0,
        }],
        state_columns: vec![AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: true,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        }],
        group_key_source_indexes: Vec::new(),
        physical_columns: Vec::new(),
    };
    let schema = Arc::new(physical_schema(&layout));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["g"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
        ],
    ).expect("batch");
    let chunk = record_batch_to_chunk(batch).expect("chunk");
    negate_aggregate_state_chunks(vec![chunk], &layout).unwrap();
}
```

- [ ] **Step 3.10: Verify it currently does NOT panic (would silently negate)**

Run: `cargo test -p novarocks --lib negate_aggregate_state_chunks_min_panics 2>&1 | tail -10`

Expected: FAIL — test asserts panic but no panic occurs (arrow `neg` happily negates Int64).

- [ ] **Step 3.11: Implement panic in negate path**

Edit `mv_agg_state.rs::negate_aggregate_state_chunks` (around line 431):

```rust
for (state_index, state_column) in layout.state_columns.iter().enumerate() {
    if matches!(
        state_column.function,
        AggregateFunctionKind::Min | AggregateFunctionKind::Max
    ) {
        panic!(
            "MIN/MAX state should not enter negate path: column `{}`. \
             DELETE-induced refresh on MV with MIN/MAX must fall back to Full refresh.",
            state_column.name
        );
    }
    let column_index = state_offset + state_index;
    // ... existing negate logic
}
```

- [ ] **Step 3.12: Verify pass**

Run: `cargo test -p novarocks --lib negate_aggregate_state_chunks_min_panics 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 3.13: Lint + commit**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -20 && cargo test -p novarocks --lib mv_agg_state mv_shape 2>&1 | tail -20`

Then commit:

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/mv_shape.rs
git commit -m "$(cat <<'EOF'
feat(mv): support MIN/MAX aggregate in incremental MV (INSERT-only)

MIN and MAX use a single Single-role state column with cmp::min/max
merge semantics. NULL handling matches SUM (None+None=None,
Some+None=Some). Float NaN: NaN+NaN=NaN, NaN+x=x. Boolean and complex
types rejected at DDL.

DELETE handling NOT included in this commit — negate_aggregate_state_chunks
panics if it sees MIN/MAX state, which is the invariant that the
fall-back logic (next commit) must uphold.
EOF
)"
```

---

## Task 4: Full-refresh fall-back when MV uses MIN/MAX and base has DELETE

**Goal:** At `mv_refresh.rs:155`, when DELETE files are present in the change batch AND the MV layout contains any MIN/MAX state, fall back to `refresh_mv_full_with_executor` instead of erroring.

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs:152-165` (the Incremental closure DELETE branch)
- Add: `layout_has_min_or_max` helper (in `mv_agg_state.rs` or `mv_refresh.rs`)
- Test: inline in `mv_agg_state.rs` (helper test) + inline in `mv_refresh.rs` (fall-back integration test if structure permits)

- [ ] **Step 4.1: Write failing test for `layout_has_min_or_max` helper**

Add to `mv_agg_state.rs` test module:

```rust
#[test]
fn layout_has_min_or_max_detects() {
    let mut layout = AggregateMvLayout {
        row_id_column: managed_physical_column(
            ROW_ID_COLUMN.to_string(), SqlType::String, false, false, true,
        ),
        visible_columns: Vec::new(),
        state_columns: Vec::new(),
        group_key_source_indexes: Vec::new(),
        physical_columns: Vec::new(),
    };
    assert!(!layout_has_min_or_max(&layout));

    layout.state_columns.push(AggregateStateColumn {
        name: "__agg_state_c".to_string(),
        data_type: DataType::Int64,
        sql_type: SqlType::BigInt,
        nullable: false,
        visible_source_index: 0,
        aggregate_index: 0,
        function: AggregateFunctionKind::Count,
        state_role: AggregateStateRole::Single,
        count_star: true,
    });
    assert!(!layout_has_min_or_max(&layout));

    layout.state_columns.push(AggregateStateColumn {
        name: "__agg_state_mn".to_string(),
        data_type: DataType::Int64,
        sql_type: SqlType::BigInt,
        nullable: true,
        visible_source_index: 1,
        aggregate_index: 1,
        function: AggregateFunctionKind::Min,
        state_role: AggregateStateRole::Single,
        count_star: false,
    });
    assert!(layout_has_min_or_max(&layout));
}
```

- [ ] **Step 4.2: Verify failure**

Run: `cargo test -p novarocks --lib layout_has_min_or_max_detects 2>&1 | tail -10`

Expected: FAIL — `layout_has_min_or_max` not found.

- [ ] **Step 4.3: Implement `layout_has_min_or_max`**

Add to `mv_agg_state.rs` (public visibility, near `build_aggregate_mv_layout`):

```rust
pub(crate) fn layout_has_min_or_max(layout: &AggregateMvLayout) -> bool {
    layout.state_columns.iter().any(|col| {
        matches!(
            col.function,
            AggregateFunctionKind::Min | AggregateFunctionKind::Max
        )
    })
}
```

- [ ] **Step 4.4: Verify pass**

Run: `cargo test -p novarocks --lib layout_has_min_or_max_detects 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 4.5: Wire fall-back in `mv_refresh.rs`**

Edit `mv_refresh.rs:152-200` (the Incremental closure). Currently lines 155-161 hard-error on `!batch.deletes.is_empty()`. The closure needs access to the layout to make the fall-back decision; the layout is computed inside `refresh_aggregate_mv_full` but for the Incremental branch we currently only have the `mv_shape`.

Restructure: build the layout once before dispatch, and pass to closures that need it. Or: compute it on demand inside the closure (small overhead).

Inside the Incremental closure body, replace the existing `if !batch.deletes.is_empty() { return Err(...) }` block with:

```rust
if !batch.deletes.is_empty() {
    // For aggregate MVs that use MIN/MAX, DELETE in lineage requires full
    // refresh because state-level retract for MIN/MAX has no closed-form
    // inverse (would need to rescan the affected groups from base).
    if let crate::connector::starrocks::managed::mv_shape::IncrementalMvShape::Aggregate(agg_shape) = &mv_shape {
        // Build layout to check for MIN/MAX. Cheap; no I/O.
        let output_columns = crate::engine::analyze_select_output_columns(state, &db_name, &mv_row.select_sql)?;
        let layout = crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            agg_shape,
            &output_columns,
        )?;
        if crate::connector::starrocks::managed::mv_agg_state::layout_has_min_or_max(&layout) {
            log::info!(
                "mv_refresh fall-back to Full: mv={}.{} reason=min_max_with_deletes \
                 base={} snapshot_from={} snapshot_to={} delete_files={}",
                db_name,
                mv_name,
                base_ref.fqn(),
                previous_snapshot_id,
                current_snapshot_id,
                batch.deletes.len()
            );
            return refresh_aggregate_mv_full(state, &db_name, &mv_name, agg_shape);
        }
    }

    return Err(format!(
        "iceberg materialized view incremental refresh does not yet support \
         delete snapshots; {} delete file(s) seen in lineage",
        batch.deletes.len()
    ));
}
```

(`analyze_select_output_columns` exact path may differ; verify with `grep -rn "analyze_select_output_columns" src/`.)

- [ ] **Step 4.6: Build to verify wiring**

Run: `cargo build 2>&1 | tail -20`

Expected: clean build. If errors, the helper paths or shape variant patterns are wrong — adjust based on the actual code structure.

- [ ] **Step 4.7: Lint + commit**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -20 && cargo test -p novarocks 2>&1 | tail -20`

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/mv_refresh.rs
git commit -m "$(cat <<'EOF'
feat(mv): full-refresh fall-back when MV uses MIN/MAX and base has DELETE

When IcebergChangeBatch contains DELETE files and the MV layout has
any Min or Max aggregate state column, fall back to the Full refresh
path instead of erroring. Logged at INFO with reason=min_max_with_deletes.

Aggregate MVs without MIN/MAX retain the existing "delete not yet
supported" error (handled separately in roadmap §2.1 / IVM Phase 2 PR-3).
EOF
)"
```

---

## Task 5: SQL integration test

**Goal:** End-to-end SQL test exercising AVG, MIN, MAX over an Iceberg base table with INSERT-only changes plus DDL rejection cases.

**Files:**
- Create: `sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql`

- [ ] **Step 5.1: Write the test file**

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,aggregate,avg,min,max
-- Test Objective:
-- 1. AVG over Int and Decimal inputs (output type follows analyzer).
-- 2. MIN/MAX over numeric, string, date, and timestamp inputs.
-- 3. NULL handling for AVG / MIN / MAX (whole group of NULLs).
-- 4. Incremental INSERT correctly updates AVG / MIN / MAX state.
-- 5. DDL rejections for AVG(*), AVG(bool), AVG(float), MIN(*), MIN(bool).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_agg2_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_agg2_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_agg2_${uuid0}.ns_${uuid0};
CREATE TABLE mv_agg2_${uuid0}.ns_${uuid0}.measurements (
  k INT,
  v BIGINT,
  d DECIMAL(20, 4),
  s STRING,
  ts DATETIME
);
INSERT INTO mv_agg2_${uuid0}.ns_${uuid0}.measurements VALUES
  (1, 10, 100.5000, 'apple',  '2024-01-01 00:00:00'),
  (1, 20, 200.0000, 'banana', '2024-02-01 00:00:00'),
  (1, NULL, NULL,   NULL,     NULL),
  (2, 5,  50.2500,  'cherry', '2024-03-15 12:00:00');

-- query 2
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.measurements_mv
DISTRIBUTED BY HASH(k) BUCKETS 2
AS SELECT
  k,
  COUNT(*)  AS c_all,
  SUM(v)    AS s_v,
  AVG(v)    AS a_v,
  AVG(d)    AS a_d,
  MIN(v)    AS mn_v,
  MAX(v)    AS mx_v,
  MIN(s)    AS mn_s,
  MAX(s)    AS mx_s,
  MIN(ts)   AS mn_ts,
  MAX(ts)   AS mx_ts
FROM mv_agg2_${uuid0}.ns_${uuid0}.measurements
GROUP BY k;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.measurements_mv;

-- query 4
SELECT k, c_all, s_v, a_v, a_d, mn_v, mx_v, mn_s, mx_s, mn_ts, mx_ts
FROM ${case_db}.measurements_mv
ORDER BY k;

-- query 5: incremental INSERT
-- @skip_result_check=true
INSERT INTO mv_agg2_${uuid0}.ns_${uuid0}.measurements VALUES
  (1, 30, 300.7500, 'date',   '2024-06-01 09:00:00'),
  (3, 7,  70.0000,  'fig',    '2024-07-01 18:30:00');

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.measurements_mv;

-- query 7
SELECT k, c_all, s_v, a_v, a_d, mn_v, mx_v, mn_s, mx_s, mn_ts, mx_ts
FROM ${case_db}.measurements_mv
ORDER BY k;

-- query 8: DDL rejections — AVG variants
-- @expect_error=AVG aggregate requires a column expression argument
CREATE MATERIALIZED VIEW ${case_db}.bad_avg_star
DISTRIBUTED BY HASH(k) BUCKETS 2
AS SELECT k, AVG(*) FROM mv_agg2_${uuid0}.ns_${uuid0}.measurements GROUP BY k;

-- query 9
-- @expect_error=AVG state type is unsupported
CREATE MATERIALIZED VIEW ${case_db}.bad_avg_string
DISTRIBUTED BY HASH(k) BUCKETS 2
AS SELECT k, AVG(s) FROM mv_agg2_${uuid0}.ns_${uuid0}.measurements GROUP BY k;

-- query 10: DDL rejections — MIN/MAX variants
-- @expect_error=MIN/MAX aggregate requires a column expression argument
CREATE MATERIALIZED VIEW ${case_db}.bad_min_star
DISTRIBUTED BY HASH(k) BUCKETS 2
AS SELECT k, MIN(*) FROM mv_agg2_${uuid0}.ns_${uuid0}.measurements GROUP BY k;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.measurements_mv;
DROP TABLE mv_agg2_${uuid0}.ns_${uuid0}.measurements FORCE;
DROP DATABASE mv_agg2_${uuid0}.ns_${uuid0};
DROP CATALOG mv_agg2_${uuid0};
```

- [ ] **Step 5.2: Record expected results**

Start standalone-server and run record mode:

```bash
cd /Users/harbor/project/NovaRocks/.claude/worktrees/practical-poitras-42a7e1
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_aggregate_avg_min_max --mode record
kill $SERVER_PID
```

Inspect the recorded `.expected` file under `sql-tests/write-path/expected/` to verify the values are mathematically correct (manually compute):
- After query 4: k=1: c_all=3, s_v=30, a_v=15.0, a_d=150.25, mn_v=10, mx_v=20, mn_s='apple', mx_s='banana', mn_ts='2024-01-01...', mx_ts='2024-02-01...'
- k=2: c_all=1, all values from the single row.
- After query 7: k=1 incorporates row (30, 300.75, 'date', ...).

Adjust expectations if the analyzer's AVG output type differs from anticipation; do NOT silently accept whatever record mode produces.

- [ ] **Step 5.3: Run verify mode to lock**

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_aggregate_avg_min_max --mode verify
kill $SERVER_PID
```

Expected: PASS.

- [ ] **Step 5.4: Verify no regression on existing aggregate IVM test**

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_aggregate_ivm,managed_lake_mv_aggregate_avg_min_max,managed_lake_mv_basic,managed_lake_mv_incremental --mode verify
kill $SERVER_PID
```

Expected: ALL PASS.

- [ ] **Step 5.5: Final lint + full test sweep + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings 2>&1 | tail -20
cargo test -p novarocks 2>&1 | tail -20
```

All green. Commit:

```bash
git add sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql \
        sql-tests/write-path/expected/managed_lake_mv_aggregate_avg_min_max*
git commit -m "$(cat <<'EOF'
test(mv): add SQL integration test for AVG/MIN/MAX aggregate IVM

Covers:
- First (full) refresh with mixed types (Int, Decimal, String, Datetime)
- Incremental INSERT updating all aggregate functions
- NULL handling: a row of all-NULL values in a group
- DDL rejections: AVG(*), AVG(string), MIN(*)

DELETE -> full-refresh fall-back is covered by Rust unit tests in
mv_agg_state and mv_refresh; not duplicated here per spec §8.2.
EOF
)"
```

---

## Final Verification

- [ ] **Step F.1: Confirm clean tree**

```bash
git status
git log --oneline -10
```

Expected: 5 new commits on top of `0bd945e` (counting `0a1b302` and `990b71c` for the spec, plus 5 commits 1–5).

- [ ] **Step F.2: Push branch**

```bash
git push -u origin claude/practical-poitras-42a7e1
```

(Skip if you don't have remote write yet; user can push later.)

---

## Self-Review Notes

After writing, re-checking:

**Spec coverage**:
- §3 decision table — every row mapped to a Task ✓
- §4.1-4.6 architecture — all in Tasks 1-3 ✓
- §5 data flow — covered in Tasks 2-4 ✓
- §6 fall-back — Task 4 ✓
- §7 error handling — distributed across Tasks 2/3 (DDL errors) and Task 4 (logging) ✓
- §8 testing — Tasks all include TDD; SQL test in Task 5 ✓
- §9 5-commit slicing — exact match ✓
- §10 file changes — all files touched ✓
- §11 risks — addressed inline (Decimal scale → Step 2.1 pre-flight; NaN → Step 3.5 explicit tests; collision → existing `validate_unique_aggregate_physical_column_names` covers this) ✓

**Type consistency**:
- `AggregateStateRole` introduced in Task 1, used consistently in Tasks 2-4 ✓
- `merge_state_value` signature unchanged across tasks ✓
- `update_visible_values_from_state` introduced in Task 1, extended in Task 2 ✓
- `layout_has_min_or_max` declared `pub(crate)` in Task 4, called from `mv_refresh.rs` ✓

**Placeholder scan** — none found.
