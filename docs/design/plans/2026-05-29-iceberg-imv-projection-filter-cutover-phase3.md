# Iceberg IMV Projection/Filter Cutover (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch single-table projection/filter Iceberg MV incremental refresh from the hand-written `__nr_ivm_delta` SQL path to consuming `run_imv_rewrite` output.

**Architecture:** Hook `run_imv_rewrite` into `execute_query_with_options` (after `plan_query`, before `optimize`) via a new optional `mv_refresh_ctx` parameter. Make `IcebergDeltaScanOperator` project its internal column superset to the codegen scan tuple by name (R1), so the delta scan can emit only `{data, _row_id, __change_op}`. Add rewrite rules that inject `_row_id` and the apply key, generalize action-column propagation to any internal column, then delete the legacy AST-mutate PF path.

**Tech Stack:** Rust, Arrow `RecordBatch`/`Chunk`, NovaRocks IMV rewrite pipeline (`src/sql/optimizer/rewrite/imv/`), Iceberg connector.

**Spec:** `docs/design/specs/2026-05-29-iceberg-imv-projection-filter-cutover-phase3-design.md`

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `src/exec/operators/iceberg_delta_scan.rs` | Delta scan operator | Modify: R1 name-based projection to tuple; drop rigid 5-col split |
| `src/sql/optimizer/rewrite/imv/row_id_column.rs` | `_row_id` internal-column descriptor + `InjectRowIdRule` | Create |
| `src/sql/optimizer/rewrite/imv/action_propagation.rs` | Internal-column propagation | Modify: generalize `PropagateActionColumnRule` to all `is_internal` columns |
| `src/sql/optimizer/rewrite/imv/apply_key.rs` | Root apply-key project rule | Create |
| `src/sql/optimizer/rewrite/imv/action_column.rs` | Validation | Modify: add V6 (apply key) + V7 (`_row_id`) |
| `src/sql/optimizer/rewrite/imv/pipeline.rs` | Pipeline stage registration | Modify: register `InjectRowIdRule` + `InjectApplyKeyProjectRule` |
| `src/sql/optimizer/rewrite/imv/mod.rs` | Module declarations | Modify: add `row_id_column`, `apply_key` |
| `src/engine/mod.rs` | `execute_query_with_options` | Modify: add `mv_refresh_ctx` param + rewrite hook |
| `src/engine/mv/iceberg_refresh.rs` | PF incremental refresh | Modify: cutover; delete legacy PF helpers |
| `src/sql/codegen/fragment_builder.rs` | Codegen | Modify: remove `reject_internal_action_column` tripwire |
| `sql-tests/optimizer/imv_projection_filter_cutover_*.sql` | Plan-shape golden | Create |

---

## Task 1: R1 — IcebergDeltaScanOperator projects to tuple by name

**Files:**
- Modify: `src/exec/operators/iceberg_delta_scan.rs`
- Test: `src/exec/operators/iceberg_delta_scan.rs` (inline `#[cfg(test)]`)

Today the operator assumes the codegen tuple's last 5 slots are exactly
`[_file, _pos, _row_id, _last_updated_sequence_number, __change_op]`
(`data_slot_count = slots.len() - 5` at `:246`, positional lineage passthrough at
`:367`). R1 keeps the scanner producing the full superset internally but projects
the final batch to the tuple by name, dropping virtual columns the tuple omits.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module. This test builds a fake superset batch
`[k, _file, _pos, _row_id, _last_updated_sequence_number, __change_op]` and a
target chunk schema with only `[k, _row_id, __change_op]`, then asserts the
projection keeps exactly those three in tuple order.

```rust
#[test]
fn project_superset_to_tuple_keeps_only_requested_virtual_columns() {
    use arrow::array::{Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    // Internal superset the operator produces (data + 4 lineage + change_op).
    let superset = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, false),
            Field::new("__change_op", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["f.parquet"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int8Array::from(vec![1])),
        ],
    )
    .unwrap();

    // Tuple wants only data + _row_id + __change_op (PF shape).
    let tuple_names = vec!["k", "_row_id", "__change_op"];
    let out = project_superset_to_tuple(&superset, &tuple_names).unwrap();

    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.schema().field(0).name(), "k");
    assert_eq!(out.schema().field(1).name(), "_row_id");
    assert_eq!(out.schema().field(2).name(), "__change_op");
}

#[test]
fn project_superset_to_tuple_is_identity_on_full_five_column_tail() {
    use arrow::array::{Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let superset = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, false),
            Field::new("__change_op", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["f.parquet"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int8Array::from(vec![1])),
        ],
    )
    .unwrap();

    // Legacy aggregate/join tuples still request all 5 trailing virtual columns.
    let tuple_names = vec![
        "k",
        "_file",
        "_pos",
        "_row_id",
        "_last_updated_sequence_number",
        "__change_op",
    ];
    let out = project_superset_to_tuple(&superset, &tuple_names).unwrap();
    assert_eq!(out.num_columns(), 6);
    for (i, name) in tuple_names.iter().enumerate() {
        assert_eq!(&out.schema().field(i).name().as_str(), name);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib project_superset_to_tuple -- --nocapture`
Expected: FAIL — `project_superset_to_tuple` is not defined.

- [ ] **Step 3: Implement `project_superset_to_tuple`**

Add this free function near `project_scanner_batch_to_contract`. It selects
columns from the superset by name, in the order the tuple lists them.

```rust
/// Project the operator's internal superset batch onto the codegen scan tuple
/// by column name, in tuple order. The superset always carries the full data
/// columns plus the four v3 lineage columns plus `__change_op`; the tuple may
/// request only a subset (e.g. projection/filter MVs want only `_row_id` and
/// `__change_op`). Columns the tuple does not name are dropped here, at the
/// operator boundary — this is the only place the rigid trailing-column count
/// used to be assumed. `_pos` is required upstream to derive `_row_id`
/// (`first_row_id + _pos`, see `synthesize_row_id`), so it stays in the
/// superset; it is simply not re-emitted unless the tuple asks for it.
fn project_superset_to_tuple(
    superset: &RecordBatch,
    tuple_names: &[&str],
) -> Result<RecordBatch, String> {
    let schema = superset.schema();
    let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(tuple_names.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(tuple_names.len());
    for name in tuple_names {
        let idx = schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                format!(
                    "iceberg delta-scan: codegen tuple requests column `{name}` \
                     not present in scanner output (have: {:?})",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                )
            })?;
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(superset.column(idx).clone());
    }
    let out_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new_with_options(
        out_schema,
        columns,
        &arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(superset.num_rows())),
    )
    .map_err(|e| format!("project iceberg delta-scan superset to tuple: {e}"))
}
```

- [ ] **Step 4: Run the new function's tests to verify they pass**

Run: `cargo test --lib project_superset_to_tuple -- --nocapture`
Expected: PASS (both tests).

- [ ] **Step 5: Wire the projection into `pull_chunk` and remove the rigid split**

In `pull_chunk` (`:156`), after `inject_change_op_column`, project to the tuple
before constructing the `Chunk`. Replace:

```rust
                    let realigned = project_scanner_batch_to_contract(&batch, &projection)?;
                    let tagged = inject_change_op_column(realigned, op)?;
                    let chunk = Chunk::try_new_with_chunk_schema(
                        tagged,
                        self.node.output_chunk_schema.clone(),
                    )?;
                    return Ok(Some(chunk));
```

with:

```rust
                    let realigned = project_scanner_batch_to_contract(&batch, &projection)?;
                    let tagged = inject_change_op_column(realigned, op)?;
                    // R1: the codegen scan tuple is the authoritative output spec.
                    // Project the internal superset onto it by name, dropping any
                    // virtual columns the tuple does not request.
                    let tuple_names: Vec<&str> = self
                        .node
                        .output_chunk_schema
                        .slots()
                        .iter()
                        .map(|s| s.name())
                        .collect();
                    let projected = project_superset_to_tuple(&tagged, &tuple_names)?;
                    let chunk = Chunk::try_new_with_chunk_schema(
                        projected,
                        self.node.output_chunk_schema.clone(),
                    )?;
                    return Ok(Some(chunk));
```

- [ ] **Step 6: Generalize `build_data_column_projection_plan` to classify by virtual-column name**

The data-column projection must no longer assume "last 5 are virtual". Replace
the `data_slot_count = slots.len() - 5` logic so data slots are those whose names
are NOT in the virtual-column set. Replace the body of
`build_data_column_projection_plan` (`:229`) up to the `targets` loop:

```rust
fn build_data_column_projection_plan(
    node: &IcebergDeltaScanNode,
) -> Result<IcebergDataColumnProjection, String> {
    use crate::exec::row_position::{
        is_change_op, is_iceberg_file_path, is_iceberg_last_updated_sequence_number,
        is_iceberg_row_id, is_iceberg_row_pos,
    };
    // A slot is a virtual column iff its name is one of the known v3 lineage /
    // change-op names. Everything else is a data column requiring field-id
    // projection. This replaces the former rigid "trailing 5" assumption.
    let is_virtual = |name: &str| {
        is_iceberg_file_path(name)
            || is_iceberg_row_pos(name)
            || is_iceberg_row_id(name)
            || is_iceberg_last_updated_sequence_number(name)
            || is_change_op(name)
    };
    let slots = node.output_chunk_schema.slots();
    let current_schema = node.iceberg_runtime.base_table.metadata().current_schema();
    let mut targets = Vec::new();
    for slot in slots.iter().filter(|s| !is_virtual(s.name())) {
        let name = slot.name().to_string();
        let nested = current_schema
            .field_by_name(slot.name())
            .or_else(|| current_schema.field_by_name_case_insensitive(slot.name()))
            .ok_or_else(|| {
                format!(
                    "iceberg delta-scan codegen tuple references column `{}` that is not in \
                     the current iceberg schema (schema_id={})",
                    slot.name(),
                    current_schema.schema_id(),
                )
            })?;
        targets.push(IcebergDataColumnTarget {
            name,
            field_id: nested.id,
            expected_data_type: slot.data_type().clone(),
            nullable: slot.nullable(),
        });
    }
    Ok(IcebergDataColumnProjection { targets })
}
```

Note: `project_scanner_batch_to_contract` still produces `[<data by field-id>,
<4 lineage from scanner>]`; `inject_change_op_column` appends `__change_op`; the
new `project_superset_to_tuple` (Step 5) then selects the tuple's columns by name.
The `ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT = 4` constant stays (it
describes the scanner's physical tail). Delete
`ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT = 5` and its uses.

- [ ] **Step 7: Update the lockstep comment in the connector backend**

In `src/connector/iceberg/catalog/backend.rs`, find the comment block near
`build_iceberg_table_def_for_delta_scan` that says the trailing virtual-column
count is kept in lockstep with the operator constant. Replace the lockstep claim
with: the operator now projects its superset onto the codegen tuple by name
(`iceberg_delta_scan.rs::project_superset_to_tuple`), so the TableDef's virtual
column exposure and the operator's output are decoupled.

- [ ] **Step 8: Run the operator tests + the iceberg-ivm suite is deferred to Task 11**

Run: `cargo test --lib iceberg_delta_scan`
Expected: PASS. (End-to-end suite verification happens in Task 11 after cutover.)

- [ ] **Step 9: Commit**

```bash
git add src/exec/operators/iceberg_delta_scan.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(iceberg-imv): R1 — delta scan projects superset to tuple by name"
```

---

## Task 2: `_row_id` internal-column descriptor + `InjectRowIdRule`

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/row_id_column.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`
- Test: inline in `row_id_column.rs`

Mirrors `ImvActionColumn` / `InjectActionColumnRule`. Injects `_row_id`
(`Int64`, non-null, `is_internal`) into `IcebergDeltaTable`-bound scans so the
apply-key project (Task 4) can reference it.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{LogicalPlan, ScanNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta_scan() -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        }
    }

    #[test]
    fn row_id_output_column_shape() {
        let col = ImvRowIdColumn::output_column(ColumnId(7));
        assert_eq!(col.name, "_row_id");
        assert_eq!(col.data_type, DataType::Int64);
        assert!(!col.nullable);
        assert!(col.is_internal);
    }

    #[test]
    fn inject_row_id_on_delta_scan() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::Scan(delta_scan());
        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Scan(scan)) =
            rule.apply(plan, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        assert!(scan.columns.iter().any(ImvRowIdColumn::matches));
    }

    #[test]
    fn inject_row_id_is_idempotent() {
        let rule = InjectRowIdRule;
        let ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns.push(ImvRowIdColumn::output_column(ColumnId(9)));
        assert!(!rule.matches(&LogicalPlan::Scan(scan), &ctx));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib row_id_column`
Expected: FAIL — module/`ImvRowIdColumn`/`InjectRowIdRule` not defined.

- [ ] **Step 3: Implement the descriptor + rule**

Create `src/sql/optimizer/rewrite/imv/row_id_column.rs`:

```rust
//! IMV `_row_id` internal column descriptor + injection rule.
//!
//! `_row_id` is the Iceberg v3 row-lineage identity. The IMV apply key
//! (`__nova_base_row_id`) is derived from it. Phase 3 injects it (internal,
//! Int64, non-null) on Delta-bound scans so the root apply-key project can
//! reference it. It is never exposed to user-visible output.

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct ImvRowIdColumn;

impl ImvRowIdColumn {
    pub(crate) const NAME: &'static str = crate::exec::row_position::ICEBERG_ROW_ID_COL;

    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        }
    }

    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }
}

/// Registered into the `imv-action-propagation` stage alongside
/// `InjectActionColumnRule`. Adds the `_row_id` internal column to
/// Delta-bound scans (idempotent).
pub(crate) struct InjectRowIdRule;

impl LogicalRewriteRule for InjectRowIdRule {
    fn name(&self) -> &'static str {
        "InjectRowId"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvRowIdColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut scan) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "InjectRowId requires ImvExtension in RewriteContext".to_string())?;
        let column_id = ext.allocate_column_id();
        scan.columns.push(ImvRowIdColumn::output_column(column_id));
        Ok(RewriteResult::Changed(LogicalPlan::Scan(scan)))
    }
}
```

(Test module from Step 1 appended below this.)

- [ ] **Step 4: Declare the module**

In `src/sql/optimizer/rewrite/imv/mod.rs`, add (alphabetical with siblings):

```rust
pub(crate) mod row_id_column;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib row_id_column`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/row_id_column.rs src/sql/optimizer/rewrite/imv/mod.rs
git commit -m "feat(iceberg-imv): add _row_id internal column descriptor + InjectRowIdRule"
```

---

## Task 3: Generalize `PropagateActionColumnRule` to all internal columns

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_propagation.rs`
- Test: inline (existing PR #206 tests must stay green; add one new test)

`_row_id` (Task 2) is injected at the leaf scan and must ride through the
Project/Filter chain to the root, exactly as `__change_op` does. Generalize the
Project propagation to carry **every** `is_internal` child-output column that the
Project does not already expose. Existing PR #206 tests use scans carrying only
`__change_op`, so they remain green (the rule still propagates only the internal
columns that are present). The Aggregate/Join/Union fail-fast arms are unchanged.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module. A delta scan carrying both `__change_op` and `_row_id`
(both internal), under a Project that selects only `k`, must end up with all three.

```rust
#[test]
fn propagate_carries_all_internal_columns_through_project() {
    use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;

    let rule = PropagateActionColumnRule;
    let mut ctx = build_ctx();
    let mut scan = delta_scan_with_action(ColumnId(100));
    scan.columns.push(ImvRowIdColumn::output_column(ColumnId(101)));
    let plan = project_over(LogicalPlan::Scan(scan), ColumnId(1));
    assert!(rule.matches(&plan, &ctx));
    let RewriteResult::Changed(LogicalPlan::Project(project)) =
        rule.apply(plan, &mut ctx).expect("apply")
    else {
        panic!("expected Changed(Project)");
    };
    // k + __change_op + _row_id
    assert_eq!(project.items.len(), 3);
    assert!(project.items.iter().any(|i| i.output_name == "__change_op"));
    assert!(project.items.iter().any(|i| i.output_name == "_row_id"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib propagate_carries_all_internal_columns_through_project`
Expected: FAIL — only `__change_op` is propagated; `_row_id` missing; len == 2.

- [ ] **Step 3: Generalize the propagation helpers + rule**

Add a generic "internal columns of a child output" helper and rewrite the
Project arm of `PropagateActionColumnRule::apply` to append all missing internal
columns. Add near the existing helpers:

```rust
/// Collect every internal (`is_internal`) output column exposed by the first
/// descendant Scan, threaded up through Filter/Project. Used by the generalized
/// propagation rule to carry `__change_op`, `_row_id`, and any future internal
/// column through the unary chain.
pub(crate) fn descendant_internal_columns(plan: &LogicalPlan) -> Vec<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => {
            scan.columns.iter().filter(|c| c.is_internal).cloned().collect()
        }
        LogicalPlan::Filter(node) => descendant_internal_columns(&node.input),
        LogicalPlan::Project(node) => descendant_internal_columns(&node.input),
        _ => Vec::new(),
    }
}
```

Replace `output_has_action_column` usage inside the rule's `matches` so the
Project arm fires while ANY internal child column is missing from the Project.
Change the `LogicalPlan::Project` arm of `matches`:

```rust
            LogicalPlan::Project(p) => {
                let internal = descendant_internal_columns(&p.input);
                !internal.is_empty()
                    && internal.iter().any(|c| {
                        !p.items
                            .iter()
                            .any(|item| item.output_name.eq_ignore_ascii_case(&c.name))
                    })
            }
```

Replace the `LogicalPlan::Project(mut p)` arm of `apply`:

```rust
            LogicalPlan::Project(mut p) => {
                let internal = descendant_internal_columns(&p.input);
                for col in internal {
                    let already = p
                        .items
                        .iter()
                        .any(|item| item.output_name.eq_ignore_ascii_case(&col.name));
                    if already {
                        continue;
                    }
                    p.items.push(ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: col.column_id,
                                qualifier: None,
                                column: col.name.clone(),
                            },
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                        },
                        output_name: col.name.clone(),
                    });
                }
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
```

Leave the `subtree_has_action_column`-based arms for Aggregate/Join/Union (they
fail-fast) and the `first_delta_base_fqn` diagnostic unchanged. Keep the existing
`__change_op`-specific helpers (`output_has_action_column`, `find_action_column`,
`subtree_has_action_column`) — they are still used by `action_column.rs`
validation.

Update the module doc comment (top of file) to note Project now carries all
internal columns, not just `__change_op`.

- [ ] **Step 4: Run all action_propagation tests**

Run: `cargo test --lib action_propagation`
Expected: PASS — the new test plus all PR #206 tests (which use action-only
scans and still see exactly `__change_op` propagated).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_propagation.rs
git commit -m "feat(iceberg-imv): propagate all internal columns through Project (generalize __change_op path)"
```

---

## Task 4: Root apply-key project rule

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/apply_key.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`
- Test: inline

Wraps the plan root in a `Project` that appends
`__nova_base_row_id` (= `ColumnRef(_row_id)`, internal, non-null). Fires once at
the root, only when a delta subtree carrying `_row_id` exists and the apply key
is not already present.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{LogicalPlan, ProjectNode, ScanNode};
    use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(200)),
        });
        ctx
    }

    fn delta_scan_with_row_id(row_id: ColumnId) -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                ImvRowIdColumn::output_column(row_id),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        }
    }

    fn project_root(scan: ScanNode, row_id: ColumnId) -> LogicalPlan {
        // Project carrying user col k + propagated _row_id (as Task 3 would leave it).
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
            items: vec![
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(1),
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: row_id,
                            qualifier: None,
                            column: "_row_id".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "_row_id".to_string(),
                },
            ],
        })
    }

    #[test]
    fn wraps_root_with_apply_key_project() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Project(root)) =
            rule.apply(plan, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        assert!(root
            .items
            .iter()
            .any(|i| i.output_name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)));
    }

    #[test]
    fn idempotent_when_apply_key_present() {
        let rule = InjectApplyKeyProjectRule::new();
        let ctx = build_ctx();
        let mut plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        if let LogicalPlan::Project(p) = &mut plan {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(101),
                        qualifier: None,
                        column: "_row_id".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
            });
        }
        assert!(!rule.matches(&plan, &ctx));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib apply_key`
Expected: FAIL — module/`InjectApplyKeyProjectRule` not defined.

- [ ] **Step 3: Implement the rule**

Create `src/sql/optimizer/rewrite/imv/apply_key.rs`:

```rust
//! IMV apply-key projection rule.
//!
//! Wraps the rewrite plan root in a `Project` that appends the apply-key column
//! `__nova_base_row_id` derived from the internal `_row_id` column. The merge
//! sink reads this column by name to locate target rows for DELETE. Fires once
//! at the root; idempotent.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_propagation::descendant_internal_columns;
use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode};

pub(crate) struct InjectApplyKeyProjectRule {
    fired: AtomicBool,
}

impl InjectApplyKeyProjectRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

/// Find the propagated `_row_id` column's id/name from the plan's effective
/// output, walking Project items first then descendant scans.
fn root_row_id_ref(plan: &LogicalPlan) -> Option<(crate::sql::column_id::ColumnId, String)> {
    if let LogicalPlan::Project(p) = plan {
        if let Some(item) = p
            .items
            .iter()
            .find(|i| i.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        {
            if let ExprKind::ColumnRef { column_id, column, .. } = &item.expr.kind {
                return Some((*column_id, column.clone()));
            }
        }
    }
    descendant_internal_columns(plan)
        .into_iter()
        .find(|c| ImvRowIdColumn::matches(c))
        .map(|c| (c.column_id, c.name))
}

fn output_has_apply_key(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project(p) => p
            .items
            .iter()
            .any(|i| i.output_name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)),
        _ => false,
    }
}

impl LogicalRewriteRule for InjectApplyKeyProjectRule {
    fn name(&self) -> &'static str {
        "InjectApplyKeyProject"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        if self.fired.load(Ordering::SeqCst) {
            return false;
        }
        root_row_id_ref(plan).is_some() && !output_has_apply_key(plan)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        let Some((row_id_col, row_id_name)) = root_row_id_ref(&plan) else {
            return Ok(RewriteResult::Unchanged);
        };
        // If the root is already a Project, append; otherwise wrap it.
        let apply_item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: row_id_col,
                    qualifier: None,
                    column: row_id_name,
                },
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
            output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        };
        match plan {
            LogicalPlan::Project(mut p) => {
                p.items.push(apply_item);
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            other => {
                // Wrap: re-expose nothing but the apply key would be wrong; instead
                // re-project the child's columns is out of scope here because a
                // non-Project root means the MV had no top projection. Phase 3
                // only supports projection/filter MVs, which always plan a root
                // Project, so wrap defensively and let validation (V6) catch any
                // shape that loses visible output.
                Ok(RewriteResult::Changed(LogicalPlan::Project(ProjectNode {
                    input: Box::new(other),
                    items: vec![apply_item],
                })))
            }
        }
    }
}
```

Note: the non-Project wrap arm is defensive only. Projection/filter MVs always
plan a root `Project`; if a future shape reaches the wrap arm it will lose
visible output and V6/V4 validation will reject it with a clear error. Add a doc
note rather than silently handling unknown shapes.

- [ ] **Step 4: Declare the module**

In `src/sql/optimizer/rewrite/imv/mod.rs`, add:

```rust
pub(crate) mod apply_key;
```

Also make `descendant_internal_columns` (Task 3) `pub(crate)` so `apply_key.rs`
can import it (already declared `pub(crate)` in Task 3 Step 3).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib apply_key`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/apply_key.rs src/sql/optimizer/rewrite/imv/mod.rs
git commit -m "feat(iceberg-imv): add root apply-key project rule (__nova_base_row_id = _row_id)"
```

---

## Task 5: Register the new rules in the IMV pipeline

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Test: existing pipeline tests + a new stage-content assertion

`InjectRowIdRule` joins the `imv-action-propagation` stage (alongside
`InjectActionColumn` and `PropagateActionColumn`). `InjectApplyKeyProjectRule`
runs after action propagation and before validation — add it to a new
`imv-apply-key` stage placed between `imv-action-propagation` and
`imv-marker-cleanup`.

- [ ] **Step 1: Write the failing test**

Add to `pipeline.rs` tests (or wherever `build_imv_pipeline` stage names are
asserted). If no such test exists, add:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_has_apply_key_stage_after_action_propagation() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        let ap = names.iter().position(|n| n == "imv-action-propagation").unwrap();
        let ak = names.iter().position(|n| n == "imv-apply-key").unwrap();
        let val = names.iter().position(|n| n == "imv-validation").unwrap();
        assert!(ap < ak && ak < val, "stage order: {names:?}");
    }
}
```

(If `RewritePipeline` exposes a different accessor than `stage_names()`, use the
existing one — check `src/sql/optimizer/rewrite/pipeline.rs`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib pipeline_has_apply_key_stage_after_action_propagation`
Expected: FAIL — `imv-apply-key` stage not present.

- [ ] **Step 3: Register the rules**

In `build_imv_pipeline`, add `InjectRowIdRule` to the `imv-action-propagation`
stage's rule vec, and insert a new `imv-apply-key` stage after it. Add imports:

```rust
use crate::sql::optimizer::rewrite::imv::apply_key::InjectApplyKeyProjectRule;
use crate::sql::optimizer::rewrite::imv::row_id_column::InjectRowIdRule;
```

Change the `imv-action-propagation` stage:

```rust
        RewriteStage::new(
            "imv-action-propagation",
            RewritePhase::SemanticRewrite,
            vec![
                Box::new(InjectActionColumnRule) as Box<dyn LogicalRewriteRule>,
                Box::new(InjectRowIdRule),
                Box::new(PropagateActionColumnRule),
            ],
        ),
        RewriteStage::new(
            "imv-apply-key",
            RewritePhase::SemanticRewrite,
            vec![Box::new(InjectApplyKeyProjectRule::new()) as Box<dyn LogicalRewriteRule>],
        ),
```

(Insert the `imv-apply-key` stage immediately before the existing
`imv-marker-cleanup` stage.)

- [ ] **Step 4: Run pipeline + imv tests to verify they pass**

Run: `cargo test --lib imv`
Expected: PASS — new stage assertion passes; existing IMV unit/E2E tests still
pass (the entrypoint E2E tests now produce `_row_id` + apply key; if any E2E test
asserts exact output-column sets, update it to include `_row_id` and
`__nova_base_row_id` as internal columns).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/pipeline.rs
git commit -m "feat(iceberg-imv): register InjectRowId + apply-key stage in IMV pipeline"
```

---

## Task 6: Validation V6 (apply key) + V7 (`_row_id`)

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_column.rs`
- Test: inline

Extend the validator: V6 — when a delta subtree exists, the root output must
carry `__nova_base_row_id` (the apply key); V7 — the delta scan subtree must
expose `_row_id` so the apply-key projection can reference it.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn validation_rejects_missing_apply_key_above_delta() {
        // Delta scan with action + row_id, root Project that drops the apply key.
        use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns.push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
            items: vec![
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(1),
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(100),
                            qualifier: None,
                            column: "__change_op".to_string(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: "__change_op".to_string(),
                },
            ],
        });
        let err = validate(&project).expect_err("missing apply key must fail");
        assert!(err.contains("apply key"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_delta_scan_missing_row_id() {
        // Delta scan with action column but NO _row_id.
        let plan = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let err = validate(&plan).expect_err("missing _row_id must fail");
        assert!(err.contains("_row_id"), "got: {err}");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib validation_rejects_missing_apply_key_above_delta validation_rejects_delta_scan_missing_row_id`
Expected: FAIL — validator does not yet check apply key / `_row_id`.

- [ ] **Step 3: Add V6 + V7 to `validate` / `validate_scan`**

In `validate_scan`, for the `IcebergDeltaTable` arm, after the existing action
column checks, add a `_row_id` presence check (V7):

```rust
        ScanSource::IcebergDeltaTable { .. } => {
            // (existing action-column V1/V2/V5 checks unchanged) ...
            // V7: delta scan must expose the internal _row_id column for the
            // apply-key projection to reference.
            let has_row_id = scan.columns.iter().any(
                crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn::matches,
            );
            if !has_row_id {
                return Err(format!("Delta-bound scan {fqn} missing internal _row_id column"));
            }
            // ... then return Ok(()) as before
            Ok(())
        }
```

(Integrate into the existing `match action_columns.as_slice()` block: keep the
`[]` / `[col]` / `_` arms; add the `_row_id` check inside the `[col]` success
path before `Ok(())`.)

In `validate`, after `validate_node(plan)?` and the `has_visible_output` check,
add V6:

```rust
    // V6: if a delta subtree exists, the root output must carry the apply key.
    if subtree_has_delta(plan) && !output_has_apply_key(plan) {
        let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
        return Err(format!(
            "root output above delta-bound scan {fqn} missing apply key {}",
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN
        ));
    }
```

Add the helper:

```rust
fn output_has_apply_key(plan: &LogicalPlan) -> bool {
    use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
    match plan {
        LogicalPlan::Project(node) => node
            .items
            .iter()
            .any(|i| i.output_name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)),
        LogicalPlan::Filter(node) => output_has_apply_key(&node.input),
        _ => false,
    }
}
```

Also update `has_visible_output`'s Project arm so the apply key and `_row_id`
internal names are not mistaken for visible output (they must not count as
user-visible):

```rust
        LogicalPlan::Project(node) => node.items.iter().any(|item| {
            !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && !item.output_name.eq_ignore_ascii_case(
                    crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn::NAME,
                )
                && !item.output_name.eq_ignore_ascii_case(
                    crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
                )
        }),
```

- [ ] **Step 4: Run validation tests to verify they pass**

Run: `cargo test --lib action_column`
Expected: PASS — V6/V7 tests pass; existing V1–V5 tests still pass. (Existing
well-formed-delta tests now need a `_row_id` column on the delta scan to satisfy
V7 — update `delta_scan_with` test helper to also push
`ImvRowIdColumn::output_column(ColumnId(102))`, OR add `_row_id` in each affected
test. Prefer adding it in the `validation_passes_on_well_formed_delta_scan` test
and any other test that expects `validate` to succeed.)

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_column.rs
git commit -m "feat(iceberg-imv): validate apply key (V6) + _row_id presence (V7)"
```

---

## Task 7: Add `mv_refresh_ctx` parameter to `execute_query_with_options`

**Files:**
- Modify: `src/engine/mod.rs:2685` (`execute_query_with_options`)
- Modify callers: `src/engine/mod.rs:2664`, `src/engine/mv/iceberg_refresh.rs:2720`, `:5882`, `:6627`, `:7262`
- Test: relies on existing tests (no behavior change — all callers pass `None`)

Thread an optional `&IcebergMvRefreshContext` through. When `Some`, run
`run_imv_rewrite` between `plan_query` and `optimize`. All callers pass `None` in
this task; the PF cutover (Task 9) flips its caller to `Some`.

- [ ] **Step 1: Add the parameter and the rewrite hook**

Modify the signature and body of `execute_query_with_options`:

```rust
pub(crate) fn execute_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let mut logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;

    // IMV refresh cutover: when a refresh context is supplied, rewrite the
    // logical plan into a delta/version plan before optimization. Plain queries
    // pass None and skip this entirely.
    if let Some(mv_ctx) = mv_refresh_ctx {
        let disabled_rules = crate::sql::optimizer::options::current_session_optimizer_settings()
            .disabled_rules
            .clone();
        let outcome = crate::sql::optimizer::rewrite::imv::entrypoint::run_imv_rewrite(
            crate::sql::optimizer::rewrite::imv::entrypoint::ImvRewriteInput {
                plan: logical,
                mv_ctx: std::sync::Arc::clone(&mv_ctx.rewrite),
                disabled_rules,
                deadline: None,
                next_column_id: factory.peek_next_id(),
            },
        )
        .map_err(|e| format!("imv rewrite: {e}"))?;
        logical = outcome.plan;
    }

    let table_stats = build_table_stats_from_plan(&logical);
    let mut physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)?;
    // ... (rest unchanged: force_single_fragment, fragment_builder, execute) ...
```

- [ ] **Step 2: Update all five callers to pass `None`**

At each call site, add a trailing `None` argument:
- `src/engine/mod.rs:2664` (inside `execute_query`) → add `None,`
- `src/engine/mv/iceberg_refresh.rs:2720`, `:5882`, `:6627`, `:7262` → add `None,`

(The `:7262` site is the PF incremental path; it stays `None` in this task and is
flipped in Task 9.)

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build --lib`
Expected: clean (no behavior change; `mv_refresh_ctx` is always `None`).

- [ ] **Step 4: Run the lib test suite to verify no regression**

Run: `cargo test --lib`
Expected: PASS — identical behavior; rewrite hook is dormant.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mod.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat(iceberg-imv): thread mv_refresh_ctx into execute_query_with_options (dormant)"
```

---

## Task 8: Remove the codegen action-column tripwire

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs` (`:145` helper, `:497` call, `:6323` test module)
- Test: the cutover (Task 9) exercises the removed guard end-to-end

The Phase 2 tripwire (`reject_internal_action_column`) rejected any internal
action column reaching codegen. Phase 3 consumes it, so the guard must go.

- [ ] **Step 1: Remove the call in `visit_scan`**

Delete the line at `:497`:

```rust
        reject_internal_action_column(&op.columns, &op.database, &op.table.name)?;
```

- [ ] **Step 2: Remove the helper function**

Delete `reject_internal_action_column` (`:145`–`:165`) and its doc comment.

- [ ] **Step 3: Remove the guard test module**

Delete the `mod action_column_guard_tests` block at `:6323` (the test asserting
the guard fires).

- [ ] **Step 4: Build + test to verify nothing else referenced it**

Run: `cargo build --lib && cargo test --lib fragment_builder`
Expected: clean build; PASS. (If `CHANGE_OP_COLUMN` import in fragment_builder
becomes unused, remove it.)

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "feat(iceberg-imv): remove Phase 2 codegen action-column tripwire"
```

---

## Task 9: Cut over `incremental_refresh_iceberg_mv` to the rewrite path

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (`incremental_refresh_iceberg_mv` steps 4–8, `:6896`–~`:7270`; PF `try_run_imv_rewrite_pipeline` call at `:1415`)
- Test: `iceberg-ivm` suite (Task 11)

Replace the one-shot `InMemoryCatalog` + AST-mutate + `__change_op` injection with
a normal base-table registration and a single
`execute_query_with_options(..., mv_refresh_ctx: Some(&ctx))` call.

- [ ] **Step 1: Determine the base-registration call (read-only investigation)**

Read `run_mv_full_select_result` (`:4219`) and `build_join_branch_catalog`
(used by the join rebuild path) to see how a normal Iceberg base table is made
visible to the analyzer (via `query_prep::refresh_external_tables_for_query` +
`state.catalog` snapshot, or a dedicated catalog builder). The new PF path must
register `base_ref` as a **normal** Iceberg table (so the analyzer plans
`LogicalPlan::Scan { source: IcebergDataFiles, table: <IcebergTableInfo matching
ctx.rewrite.base_refs> }`). Confirm the produced `IcebergTableInfo`
catalog/namespace/table match `ctx.rewrite.base_refs` so `BindIcebergScanRule`'s
`find_base_ref` succeeds.

- [ ] **Step 2: Rewrite steps 4–8 of `incremental_refresh_iceberg_mv`**

Replace the block that currently (a) builds the one-shot `InMemoryCatalog` via
`build_iceberg_table_def_for_delta_scan` (`:7054`), (b) parses + `iceberg_mv_physical_select_sql`
(`:7096`), (c) `mutate_query_for_ivm_delta_scan` (`:7134`), (d) `append_change_op_to_projection`
(`:7188`), with:

```rust
    // Parse the stored MV SELECT verbatim. The IMV rewrite (run inside
    // execute_query_with_options via mv_refresh_ctx) binds the base scan to the
    // delta snapshot window and injects the internal action / row-id / apply-key
    // columns — no AST mutation here.
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(&mv_definition.select_sql)
        .map_err(|err| handle_iceberg_mv_commit_error(
            state, target, target_entry, &staging_branch, refresh_id, err))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|err| handle_iceberg_mv_commit_error(
            state, target, target_entry, &staging_branch, refresh_id,
            format!("sql parser error: {err}")))?;
    let sqlparser::ast::Statement::Query(query_box) = statement else {
        return Err(handle_iceberg_mv_commit_error(
            state, target, target_entry, &staging_branch, refresh_id,
            "REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string()));
    };
    let query = *query_box;

    // Register the base table as a normal Iceberg table so the analyzer plans a
    // normal IcebergScan; the IMV rewrite rebinds it to the delta window.
    // (Use the registration mechanism identified in Step 1.)
    let catalog = build_pf_refresh_catalog(state, base_ref)?;  // <-- defined in Step 3

    // (pre-load locator inputs — UNCHANGED from current step 6)
    let locator_state = if has_delete_changes { /* ...unchanged... */ } else { None };

    // (build merge sink — UNCHANGED from current step 7)
    let op_kind = if has_delete_changes { CommitOpKind::RowDeltaDv } else { CommitOpKind::FastAppend };
    let collector = new_iceberg_mv_commit_collector(&target_table, &ident, &staging_branch, op_kind);
    let merge_sink_plan = crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkPlan {
        target_table: target_table.clone(),
        collector: Arc::clone(&collector),
        locator_state,
        apply_key_column: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Int64,
    };
    let merge_sink =
        crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkFactory::new(merge_sink_plan);

    // Execute through the rewrite path.
    {
        let connectors_snapshot = state.connectors.read()
            .expect("standalone connector registry read lock").clone();
        let catalogs_guard = state.iceberg_catalogs.read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if let Err(err) = crate::engine::execute_query_with_options(
            &query,
            &catalog,
            &connectors_snapshot,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(merge_sink)),
            Some(&*catalogs_guard),
            Some(&ctx),                 // <-- mv_refresh_ctx
        ) {
            drop(catalogs_guard);
            return Err(handle_iceberg_mv_commit_error(
                state, target, target_entry, &staging_branch, refresh_id, err));
        }
        drop(catalogs_guard);
    }
```

(Keep the post-execution row-count / empty-delta short-circuit and commit logic
that follows unchanged.)

- [ ] **Step 3: Add the `build_pf_refresh_catalog` helper**

Based on Step 1's findings, add a small helper that registers `base_ref` as a
normal Iceberg table in an `InMemoryCatalog`. If `run_mv_full_select_result`
shows the standalone path uses `state.catalog` + `query_prep::refresh_external_tables_for_query`,
prefer reusing that path (snapshot `state.catalog`, ensure the base is
registered). Implement the minimal version that produces a catalog the analyzer
can resolve `base_ref` against as `ScanSource::IcebergDataFiles`. Document the
chosen mechanism in a comment.

- [ ] **Step 4: Remove the PF `try_run_imv_rewrite_pipeline` call**

Delete the call at `:1415` (in `refresh_iceberg_mv`, the PF entry). Leave the
three aggregate/join calls (`:1703`, `:2453`, `:5490`) and the
`try_run_imv_rewrite_pipeline` function itself intact.

- [ ] **Step 5: Build**

Run: `cargo build --lib`
Expected: clean. (`mutate_query_for_ivm_delta_scan` / `append_change_op_to_projection`
are now unused — they are deleted in Task 10; a dead-code warning here is
expected and resolved by Task 10.)

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(iceberg-imv): cut over single-table PF incremental refresh to rewrite path"
```

---

## Task 10: Delete legacy PF-only helpers

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (delete `mutate_query_for_ivm_delta_scan` `:4966` + its 7 tests `:8136`–`:8232`; delete `append_change_op_to_projection` `:5008`)

These are now unreferenced (PF was their only caller). Keep
`iceberg_mv_physical_select_sql` (first-refresh/rebuild use it),
`build_iceberg_table_def_for_delta_scan` (other paths), `__nr_ivm_delta` TVF,
and the aggregate/join delta helpers.

- [ ] **Step 1: Verify no remaining references**

Run:
```bash
grep -n 'mutate_query_for_ivm_delta_scan\|append_change_op_to_projection' src/engine/mv/iceberg_refresh.rs
```
Expected: only the definitions and their `#[cfg(test)]` tests (no production
callers — Task 9 removed `:7134` and `:7188`).

- [ ] **Step 2: Delete the functions and their tests**

Delete `fn mutate_query_for_ivm_delta_scan` and its 7 unit tests
(`mutate_query_for_ivm_delta_scan_*`), and `fn append_change_op_to_projection`.

- [ ] **Step 3: Build to confirm nothing else referenced them**

Run: `cargo build --lib`
Expected: clean — no dead-code warning for these two functions.

- [ ] **Step 4: Confirm the reserved-name check still exists at CREATE-MV time**

Read the CREATE MATERIALIZED VIEW path (`create_iceberg_mv`, `:71`) to confirm a
user column named `__nova_base_row_id` is rejected at create time (the PF path
lost `iceberg_mv_physical_select_sql`'s reserved-name check at `:7096`). If
CREATE does NOT already reject it, add the check there:

```rust
// In create_iceberg_mv, before persisting the MV definition: reject any
// user-visible output column whose name collides with the reserved apply key.
if mv_output_columns.iter().any(|c| {
    c.eq_ignore_ascii_case(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN)
}) {
    return Err(format!(
        "materialized view output column name {} is reserved for the internal apply key",
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN
    ));
}
```

(Use the actual variable holding the resolved output column names in
`create_iceberg_mv`.)

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor(iceberg-imv): delete legacy PF AST-mutate helpers post-cutover"
```

---

## Task 11: EXPLAIN plan-shape golden + iceberg-ivm suite verification

**Files:**
- Create: `sql-tests/optimizer/imv_projection_filter_cutover_basic.sql`
- Test: `iceberg-ivm` suite + `cargo test --lib`

- [ ] **Step 1: Inspect an existing optimizer golden case for format**

Read one `sql-tests/optimizer/*.sql` golden (e.g. an `aggregate_pushdown_*.sql`)
to confirm the `-- @explain_contains=` directive syntax and how a case sets up a
table.

- [ ] **Step 2: Verify the lib test suite is green end-to-end**

Run: `cargo test --lib`
Expected: PASS — all IMV unit/E2E tests, operator tests, validation tests.

- [ ] **Step 3: Run the iceberg-ivm SQL suite (hard gate)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start standalone-server per CLAUDE.md readiness-gated recipe, then:
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
```
Expected: 61/61 pass. (This proves R1 is a no-op on aggregate/join legacy paths
and the PF cutover is behavior-preserving end-to-end.)

- [ ] **Step 4: Add the plan-shape golden case**

Create `sql-tests/optimizer/imv_projection_filter_cutover_basic.sql`. The case
must set up a single-table projection/filter Iceberg MV, trigger an incremental
refresh, and EXPLAIN the refresh plan. Assert (adapt directive syntax to Step 1):

```sql
-- A single-table projection/filter MV refresh plan must:
--   * NOT contain the legacy __nr_ivm_delta table function
--   * contain an Iceberg delta scan bound to a snapshot window
--   * keep __change_op / _row_id / __nova_base_row_id internal (not user-visible)
-- @explain_contains=IcebergDeltaScan
-- @explain_not_contains=__nr_ivm_delta
<EXPLAIN of the incremental refresh plan for the PF MV>
```

If the suite runner has no `@explain_not_contains`, assert the positive
`IcebergDeltaScan` / bound-window substring and document the negative as a code
comment. Use the iceberg-rest fixture per CLAUDE.md §7.3.

- [ ] **Step 5: Run the new golden case**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only imv_projection_filter_cutover_basic --mode verify
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sql-tests/optimizer/imv_projection_filter_cutover_basic.sql
git commit -m "test(iceberg-imv): plan-shape golden for PF cutover + verify iceberg-ivm 61/61"
```

---

## Self-Review Notes

- **Spec coverage:** §4.A → Task 7; §4.B (R1) → Task 1; §4.C (InjectRowId,
  generalize propagate, InjectApplyKeyProject, register, V6/V7) → Tasks 2–6;
  §4.D (refresh simplify, tripwire, legacy delete) → Tasks 8–10; §7 testing →
  Task 11; §3.5/§4.B v3 `_pos`→`_row_id` guard → Task 1 Step 3/6 comments.
- **Ordering safety:** Tasks 1–8 are dormant w.r.t. execution (the PF
  `try_run_imv_rewrite_pipeline` still swallows until Task 9 flips the caller).
  Tripwire removal (Task 8) precedes cutover (Task 9) so the executed plan's
  internal columns are not rejected at codegen. Legacy deletion (Task 10) follows
  cutover so the dead-code transition is clean.
- **Type consistency:** `ImvRowIdColumn` (Task 2) is referenced by Tasks 3/4/6
  with the same `NAME`/`matches`/`output_column` API. `descendant_internal_columns`
  is defined `pub(crate)` in Task 3 and imported by Task 4. `mv_refresh_ctx`
  param (Task 7) is `Option<&IcebergMvRefreshContext>` and consumed via
  `mv_ctx.rewrite` (Arc<IcebergMvRewriteContext>), matching `run_imv_rewrite`'s
  `ImvRewriteInput.mv_ctx`.
- **Integration risk:** Task 9 Step 1/3 (`build_pf_refresh_catalog`) is the one
  genuine investigation point — the exact base-registration call must be read
  from `run_mv_full_select_result` rather than guessed. Flagged as a read-only
  step.
