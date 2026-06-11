# Iceberg IMV Branch UNION Aggregate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement end-to-end incremental refresh for B-family Iceberg IMVs whose top-level `UNION ALL` branches are aggregate queries.

**Architecture:** Upgrade the currently recognized `UnsupportedBranchUnionAggregate` contract into an executable `BranchUnionAggregate` strategy. Preserve branch independence by threading `BranchScope` into aggregate target-state reads, rewriting `Delta(UnionAll(Aggregate branches))` into branch-scoped aggregate-state merges, and locating target rows by `(__branch_id__, __row_id__)` instead of a single group row id.

**Tech Stack:** Rust; NovaRocks standalone SQL analyzer/planner; IMV rewrite pipeline under `src/sql/optimizer/rewrite/imv/`; Iceberg MV refresh code under `src/engine/mv/`; SQL fixtures under `sql-tests/iceberg-ivm/`.

---

## File Structure

- `src/engine/mv/refresh_contract.rs`
  - Rename B-family strategy to executable `BranchUnionAggregate`.
  - Add a branch-scoped aggregate apply-key constructor.
  - Keep contract derivation over analyzed query structure.
- `src/engine/mv/refresh_driver.rs`
  - No core behavior change expected; B-family uses existing `AllBasesRequired` decision policy.
- `src/sql/catalog.rs`
  - Add `BranchScope`.
  - Extend `IcebergMvTargetStateRowFilter::DeltaInputRowIds` with optional branch scope.
- `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
  - Extract `build_aggregate_state_merge`.
  - Build `Scan -> Filter(__branch_id__ = i) -> Project(physical columns)` for branch-scoped old target-state input.
- `src/sql/optimizer/rewrite/imv/target_state.rs`
  - Keep `row_filter` as the source metadata carrier.
  - Tests should assert branch scope survives scan-source construction.
- `src/sql/optimizer/rewrite/imv/branch_union.rs`
  - New B-family rewrite rule.
- `src/sql/optimizer/rewrite/imv/mod.rs`
  - Register the new module.
- `src/sql/optimizer/rewrite/imv/pipeline.rs`
  - Add `imv-branch-union` structural stage.
- `src/sql/optimizer/rewrite/imv/action_propagation.rs`
  - Add `is_supported_branch_union`.
  - Avoid fail-fast on rewritten B-family unions.
- `src/sql/optimizer/rewrite/imv/action_column.rs`
  - Allow rewritten B-family unions during validation.
- `src/sql/codegen/nodes.rs`
  - Include branch-scope column in projected target-state scan columns.
- `src/sql/codegen/fragment_builder.rs`
  - Update tests and refresh-codegen fixtures for the extended row-filter type.
- `src/engine/mv/refresh_context.rs`
  - Validate branch-scope metadata against persisted `BranchUnionContract`.
  - Expose target-state scans that can read `__branch_id__`.
- `src/engine/mv/iceberg_target_apply.rs`
  - Add branch-scoped aggregate apply-key request and locator.
- `src/engine/mv/iceberg_refresh.rs`
  - Remove CREATE/REFRESH fail-fast guards for B-family aggregate union.
  - Route B-family through aggregate refresh path using `BranchUnionAggregate`.
  - Un-ignore and extend B-family tests.
- `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql`
  - New SQL fixture for same group key across branches.
- `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql`
  - New SQL fixture for empty branch behavior.
- `sql-tests/iceberg-ivm/result/*.result`
  - Recorded expected results for the new fixtures.

---

## Task 0: Normalize Rebased Test Fixtures

**Files:**
- Modify: `src/engine/mv/refresh_contract.rs`
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Confirm the current compile failure without the fixture fields**

Run:

```bash
cargo test --lib b_family -- --nocapture
```

Expected before this task's implementation on a clean `origin/main`: FAIL with missing fields:

```text
missing field `serialized_metadata_rows` in initializer of `sql::catalog::IcebergTableInfo`
missing field `row_count_confidence` in initializer of `optimizer::statistics::Statistics`
```

- [ ] **Step 2: Add `serialized_metadata_rows` to the refresh-contract test helper**

In `src/engine/mv/refresh_contract.rs`, update the test helper `iceberg_table_info`:

```rust
    fn iceberg_table_info(database: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{database}/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }
```

- [ ] **Step 3: Add row-count confidence to runtime-filter test fixtures**

In `src/sql/optimizer/runtime_filter_pass.rs`, update both `Statistics` literals in `test_support::shuffle_join_with_probe_exchange`:

```rust
            stats: Statistics {
                output_row_count: 1_000_000.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
            },
```

and:

```rust
            stats: Statistics {
                output_row_count: 100.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
            },
```

- [ ] **Step 4: Verify the targeted baseline tests pass**

Run:

```bash
cargo test --lib b_family -- --nocapture
```

Expected: PASS. The output includes both named B-family tests as passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/refresh_contract.rs src/sql/optimizer/runtime_filter_pass.rs
git commit -m "test: update fixtures for metadata rows and stats confidence"
```

---

## Task 1: Make B-Family Refresh Contract Executable

**Files:**
- Modify: `src/engine/mv/refresh_contract.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Write the failing contract tests**

In `src/engine/mv/refresh_contract.rs`, rename the existing B-family test to assert an executable strategy. Replace the final assertion in `recognizes_b_family_but_keeps_it_unsupported` with:

```rust
        assert_eq!(
            contract.strategy,
            RefreshStrategy::BranchUnionAggregate
        );
        assert_eq!(contract.base_refs.len(), 2);
        assert_eq!(contract.branch.expect("branch contract").branch_count, 2);
        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::branch_union_aggregate_group_row()
        );
```

Expected before implementation: compile failure because `BranchUnionAggregate` and `branch_union_aggregate_group_row` do not exist.

- [ ] **Step 2: Run the failing contract test**

Run:

```bash
cargo test --lib refresh_contract::tests::recognizes_b_family_but_keeps_it_unsupported -- --nocapture
```

Expected: FAIL at compile time with missing variant or missing constructor.

- [ ] **Step 3: Replace the strategy variant**

In `src/engine/mv/refresh_contract.rs`, change `RefreshStrategy` to:

```rust
pub(crate) enum RefreshStrategy {
    ProjectionFilter,
    JoinProjectionFilter,
    UnionProjectionFilter,
    SingleAggregate,
    FanInAggregate,
    JoinAggregate,
    BranchUnionAggregate,
}
```

Run this search and replace all remaining strategy references:

```bash
rg -n "UnsupportedBranchUnionAggregate" src/engine/mv src/sql docs/design/specs/2026-06-04-iceberg-imv-branch-union-aggregate-design.md
```

Only the historical spec may still mention `UnsupportedBranchUnionAggregate` as old state. Rust code must have no matches.

- [ ] **Step 4: Add branch-scoped aggregate apply-key contract**

In `impl ApplyKeyContract`, add:

```rust
    pub(crate) fn branch_union_aggregate_group_row() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::Aggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }
```

The `value_type` remains `Utf8` because the inner aggregate row id is `__row_id__`. Branch identity is matched by the locator, not encoded into the row-id string.

- [ ] **Step 5: Update B-family contract construction**

In `DerivedStructure::into_contract`, update the B-family arm:

```rust
            Self::BranchUnionAggregate {
                branch_count,
                group_key_count,
                aggregate_count,
            } => ImvRefreshContract {
                strategy: RefreshStrategy::BranchUnionAggregate,
                base_refs,
                apply_key: ApplyKeyContract::branch_union_aggregate_group_row(),
                aggregate: Some(AggregateRefreshContract {
                    group_key_count,
                    aggregate_count,
                }),
                join: None,
                branch: Some(BranchRefreshContract { branch_count }),
            },
```

- [ ] **Step 6: Remove CREATE-time unsupported guard**

In `src/engine/mv/iceberg_refresh.rs`, remove this guard from `create_iceberg_mv`:

```rust
    if refresh_contract.strategy
        == crate::engine::mv::refresh_contract::RefreshStrategy::UnsupportedBranchUnionAggregate
    {
        return Err(
            "Iceberg MV UNION ALL of aggregate branches is recognized but refresh execution is not supported in this build"
                .to_string(),
        );
    }
```

Do not add a replacement guard. B-family CREATE must be allowed once the contract is executable.

- [ ] **Step 7: Replace refresh guards with temporary executable-path errors**

In `refresh_iceberg_mv_with_planned_partitions` and `plan_iceberg_mv_refresh`, replace `UnsupportedBranchUnionAggregate` match arms with `BranchUnionAggregate` arms that still fail with a clear execution message until later tasks route the execution:

```rust
        crate::engine::mv::refresh_contract::RefreshStrategy::BranchUnionAggregate => {
            return Err(
                "top-level aggregate UNION ALL refresh execution is not wired yet".to_string(),
            );
        }
```

and:

```rust
        RefreshStrategy::BranchUnionAggregate => {
            return Err(RefreshError::user(
                "top-level aggregate UNION ALL MV refresh execution is not wired yet",
            ));
        }
```

These arms are removed in Task 6.

- [ ] **Step 8: Update the CREATE-level test**

In `src/engine/mv/iceberg_refresh.rs`, replace `create_b_family_union_aggregate_reports_refresh_contract_unsupported` with:

```rust
    #[test]
    fn create_b_family_union_aggregate_persists_branch_contract() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "t1");
        create_aggregate_fact_table(&env.state, "ice", "sales", "t2");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_agg
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c FROM ice.sales.t1 GROUP BY region
                UNION ALL
                SELECT region, count(*) AS c FROM ice.sales.t2 GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create B-family UNION ALL aggregate MV");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_union_agg")
            .expect("stored MV definition");
        let contract = mv.schema_contract.expect("schema contract");
        assert!(contract.aggregate.is_some());
        let branch = contract.branch.expect("branch contract");
        assert_eq!(branch.branch_count, 2);
        assert_eq!(
            branch.inner_apply_key_source,
            crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId
        );
    }
```

- [ ] **Step 9: Verify contract and CREATE tests**

Run:

```bash
cargo test --lib refresh_contract::tests::recognizes_b_family_but_keeps_it_unsupported create_b_family_union_aggregate_persists_branch_contract -- --nocapture
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/engine/mv/refresh_contract.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat(imv): make branch union aggregate refresh contract executable"
```

---

## Task 2: Add Branch Scope to Target-State Aggregate Merge

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/sql/optimizer/rewrite/imv/target_state.rs`
- Modify: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/engine/mv/refresh_context.rs`

- [ ] **Step 1: Add the failing branch-scope type test**

In `src/sql/catalog.rs` tests, add:

```rust
    #[test]
    fn target_state_row_filter_carries_branch_scope() {
        let filter = IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 2,
            }),
        };

        let IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            branch_scope: Some(scope),
            ..
        } = filter
        else {
            panic!("expected branch scope");
        };
        assert_eq!(scope.branch_id_column_name, "__branch_id__");
        assert_eq!(scope.branch_id, 2);
    }
```

Expected before implementation: compile failure because `BranchScope` and `branch_scope` are missing.

- [ ] **Step 2: Run the failing branch-scope type test**

Run:

```bash
cargo test --lib catalog::tests::target_state_row_filter_carries_branch_scope -- --nocapture
```

Expected: FAIL at compile time.

- [ ] **Step 3: Extend the row-filter type**

In `src/sql/catalog.rs`, replace the row-filter enum with:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchScope {
    pub(crate) branch_id_column_name: String,
    pub(crate) branch_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvTargetStateRowFilter {
    DeltaInputRowIds {
        row_id_column_name: String,
        branch_scope: Option<BranchScope>,
    },
}
```

Update `constraint_summary`:

```rust
        let row_filter = match &self.row_filter {
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: None,
            } => format!("row_filter=delta_input_row_ids({row_id_column_name})"),
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: Some(scope),
            } => format!(
                "row_filter=delta_input_row_ids({row_id_column_name}, {}={})",
                scope.branch_id_column_name,
                scope.branch_id
            ),
        };
```

- [ ] **Step 4: Update all existing row-filter construction sites**

Run:

```bash
rg -n "DeltaInputRowIds \\{" src
```

Every existing construction that is not B-family branch-scoped must become:

```rust
crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
    row_id_column_name: "__row_id__".to_string(),
    branch_scope: None,
}
```

- [ ] **Step 5: Add failing aggregate builder branch-scope test**

In `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`, add:

```rust
    #[test]
    fn build_aggregate_state_merge_threads_branch_scope() {
        let ctx = build_ctx();
        let ext = ctx.extension::<ImvExtension>().expect("extension").clone();
        let LogicalPlan::Aggregate(aggregate) = aggregate_over(leaf_scan()) else {
            panic!("expected aggregate");
        };

        let merge = build_aggregate_state_merge(
            aggregate,
            None,
            Some(crate::sql::catalog::BranchScope {
                branch_id_column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                    .to_string(),
                branch_id: 1,
            }),
            &ext,
        )
        .expect("branch-scoped merge builds");

        let LogicalPlan::AggregateStateMerge(node) = merge else {
            panic!("expected AggregateStateMerge");
        };
        let LogicalPlan::Project(project) = node.old_input.as_ref() else {
            panic!("expected old input Project dropping branch id");
        };
        let LogicalPlan::Filter(filter) = project.input.as_ref() else {
            panic!("expected branch filter under old-input Project");
        };
        let LogicalPlan::Scan(old_scan) = filter.input.as_ref() else {
            panic!("expected target-state scan under branch filter");
        };
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert!(matches!(
            &target_state.row_filter,
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                branch_scope: Some(scope),
                ..
            } if scope.branch_id == 1
        ));
        assert!(project
            .items
            .iter()
            .all(|item| !item.output_name.eq_ignore_ascii_case("__branch_id__")));
    }
```

Expected before implementation: compile failure because `build_aggregate_state_merge` does not exist.

- [ ] **Step 6: Run the failing aggregate builder test**

Run:

```bash
cargo test --lib aggregate_rewrite::tests::build_aggregate_state_merge_threads_branch_scope -- --nocapture
```

Expected: FAIL at compile time.

- [ ] **Step 7: Extract `build_aggregate_state_merge`**

In `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`, add imports:

```rust
use crate::sql::analysis::{BinOp, ProjectItem};
use crate::sql::planner::plan::FilterNode;
```

Change `RewriteAggregateStateRule::apply` so after extracting `aggregate` it gets the extension and calls:

```rust
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| {
                "RewriteAggregateState requires ImvExtension in RewriteContext".to_string()
            })?
            .clone();
        let merge = build_aggregate_state_merge(aggregate, delta.action_column, None, &ext)?;
        Ok(RewriteResult::Changed(merge))
```

Add this function containing the moved body:

```rust
pub(crate) fn build_aggregate_state_merge(
    aggregate: AggregateNode,
    action_column: Option<ColumnId>,
    branch_scope: Option<crate::sql::catalog::BranchScope>,
    ext: &ImvExtension,
) -> Result<LogicalPlan, String> {
    if aggregate.group_by.is_empty() {
        return Err(
            "Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string(),
        );
    }
    if aggregate.aggregates.iter().any(|call| call.distinct) {
        return Err(
            "Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string(),
        );
    }

    let (aggregate_shape, aggregate_layout) = ext.mv_ctx.aggregate_shape_and_layout_for_execution()?;
    let group_key_names = group_key_names(&aggregate)?;
    let aggregate_state_names = aggregate_state_names(ext, &aggregate, &aggregate_layout)?;
    let row_id_column_name = aggregate_row_id_column_name(ext)?;
    let target_columns = target_columns(ext)?;
    let target = &ext.mv_ctx.target;
    let aggregate_contract = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate state contract".to_string()
        })?;
    let physical_column_names = aggregate_layout
        .physical_columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let partition_constraint = if is_unpartitioned_target_contract(&ext.mv_ctx.schema_contract) {
        IcebergMvTargetStatePartitionConstraint::Unpartitioned
    } else {
        IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired
    };

    let old_source = build_target_state_scan_source(
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        ext.mv_ctx.target_table_uuid.clone(),
        ext.mv_ctx.target_snapshot_id,
        aggregate_contract.state_layout_version,
        target_columns.clone(),
        group_key_names.clone(),
        aggregate_state_names.clone(),
        physical_column_names,
        row_id_column_name.clone(),
        IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: row_id_column_name.clone(),
            branch_scope: branch_scope.clone(),
        },
        partition_constraint,
    );
    let old_scan = target_state_old_scan(target, target_columns, &aggregate_state_names, &row_id_column_name, old_source, ext);
    let old_input = branch_scoped_old_input(old_scan, branch_scope, &aggregate_layout, ext)?;

    let action_column = action_column.unwrap_or_else(|| ext.allocate_column_id());
    let output_columns = aggregate.output_columns.clone();
    let signed_aggregate = signed_aggregate(
        aggregate,
        action_column,
        ext,
        &aggregate_shape,
        &aggregate_layout,
    )?;

    Ok(LogicalPlan::AggregateStateMerge(AggregateStateMergeNode {
        old_input: Box::new(old_input),
        delta_input: Box::new(signed_aggregate),
        group_key_names,
        aggregate_state_names,
        change_op_column: ImvActionColumn::NAME.to_string(),
        output_columns,
    }))
}
```

The helper `target_state_old_scan` should contain the existing target-state `LogicalPlan::Scan` construction from the old `apply` body and return a `LogicalPlan`.

- [ ] **Step 8: Add branch old-input helpers**

In `aggregate_rewrite.rs`, add:

```rust
fn branch_scoped_old_input(
    old_scan: LogicalPlan,
    branch_scope: Option<crate::sql::catalog::BranchScope>,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    ext: &ImvExtension,
) -> Result<LogicalPlan, String> {
    let Some(scope) = branch_scope else {
        return Ok(old_scan);
    };
    let filtered = LogicalPlan::Filter(FilterNode {
        input: Box::new(old_scan),
        predicate: branch_scope_predicate(&scope, ext),
        required_output_columns: None,
    });
    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(filtered),
        items: aggregate_physical_passthrough_items(layout, ext)?,
        output_qualifier: None,
        required_output_columns: None,
    }))
}

fn branch_scope_predicate(
    scope: &crate::sql::catalog::BranchScope,
    ext: &ImvExtension,
) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ext.allocate_column_id(),
                    qualifier: None,
                    column: scope.branch_id_column_name.clone(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(scope.branch_id as i64)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn aggregate_physical_passthrough_items(
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    ext: &ImvExtension,
) -> Result<Vec<ProjectItem>, String> {
    layout
        .physical_columns
        .iter()
        .map(|physical| {
            let column = &physical.column;
            let column_id = ext.allocate_column_id();
            Ok(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
                output_name: column.name.clone(),
                output_column_id: column_id,
            })
        })
        .collect()
}
```

If column pruning aligns by name rather than id on this path, these allocated ids are safe because the Project defines a fresh output schema.

- [ ] **Step 9: Include branch id in target-state projected columns**

In `src/sql/codegen/nodes.rs`, update `projected_target_state_column_names`:

```rust
    if let crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &scan.row_filter
        && !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&scope.branch_id_column_name))
    {
        names.push(scope.branch_id_column_name.clone());
    }
```

- [ ] **Step 10: Validate branch scope in refresh context**

In `src/engine/mv/refresh_context.rs`, update the row-filter match:

```rust
        match &scan.row_filter {
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } if row_id_column_name.eq_ignore_ascii_case(&scan.row_id_column_name) => {
                validate_target_state_branch_scope(scan, branch_scope.as_ref(), &self.rewrite.schema_contract)?;
            }
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                ..
            } => {
                return Err(format!(
                    "Iceberg target-state scan {} row filter column mismatch: filter={} scan={}",
                    scan.fqn(),
                    row_id_column_name,
                    scan.row_id_column_name
                ));
            }
        }
```

Add:

```rust
fn validate_target_state_branch_scope(
    scan: &IcebergMvTargetStateScan,
    scope: Option<&crate::sql::catalog::BranchScope>,
    contract: &MvSchemaContract,
) -> Result<(), String> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let branch = contract.branch.as_ref().ok_or_else(|| {
        format!(
            "Iceberg target-state scan {} has branch scope but schema contract has no branch contract",
            scan.fqn()
        )
    })?;
    if !scope
        .branch_id_column_name
        .eq_ignore_ascii_case(&branch.branch_id_column.column_name)
    {
        return Err(format!(
            "Iceberg target-state scan {} branch column mismatch: scope={} contract={}",
            scan.fqn(),
            scope.branch_id_column_name,
            branch.branch_id_column.column_name
        ));
    }
    if scope.branch_id < 0 || scope.branch_id as u32 >= branch.branch_count {
        return Err(format!(
            "Iceberg target-state scan {} branch id {} out of range 0..{}",
            scan.fqn(),
            scope.branch_id,
            branch.branch_count
        ));
    }
    Ok(())
}
```

- [ ] **Step 11: Update codegen and refresh-context tests for `branch_scope: None`**

Run:

```bash
rg -n "DeltaInputRowIds \\{" src/sql/codegen src/engine/mv/refresh_context.rs
```

For each test literal, add:

```rust
branch_scope: None,
```

Add one codegen nodes test assertion in `src/sql/codegen/nodes.rs` that branch scope projects `__branch_id__`:

```rust
    #[test]
    fn projected_target_state_columns_include_branch_scope_column() {
        let mut scan = test_target_state_scan();
        scan.row_filter = crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(crate::sql::catalog::BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 1,
            }),
        };
        let projected = projected_target_state_column_names(&scan);
        assert!(projected.iter().any(|name| name == "__branch_id__"));
    }
```

If `test_target_state_scan` does not exist, create it by extracting the existing `IcebergMvTargetStateScan` literal from the same test module into a helper function.

- [ ] **Step 12: Verify branch-scope tests**

Run:

```bash
cargo test --lib catalog::tests::target_state_row_filter_carries_branch_scope aggregate_rewrite::tests::build_aggregate_state_merge_threads_branch_scope target_state::tests nodes::tests::projected_target_state_columns_include_branch_scope_column -- --nocapture
```

Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add src/sql/catalog.rs src/sql/optimizer/rewrite/imv/target_state.rs src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs src/sql/codegen/nodes.rs src/sql/codegen/fragment_builder.rs src/engine/mv/refresh_context.rs
git commit -m "feat(imv): add branch scope to aggregate target-state reads"
```

---

## Task 3: Add B-Family Branch UNION Rewrite

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/branch_union.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`

- [ ] **Step 1: Write the failing rewrite test**

Create `src/sql/optimizer/rewrite/imv/branch_union.rs` with the test module first. Use helpers copied from `union_delta.rs` and `aggregate_rewrite.rs` so the file is self-contained. The first test should be:

```rust
    #[test]
    fn rewrites_top_union_of_aggregates_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![aggregate_over(scan("t1", 1)), aggregate_over(scan("t2", 10))],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };

        assert_eq!(union.inputs.len(), 2);
        for (idx, branch) in union.inputs.iter().enumerate() {
            let LogicalPlan::Project(project) = branch else {
                panic!("expected Project branch");
            };
            let branch_item = project
                .items
                .iter()
                .find(|item| item.output_name.eq_ignore_ascii_case("__branch_id__"))
                .expect("branch id item");
            assert!(matches!(
                &branch_item.expr.kind,
                ExprKind::Literal(LiteralValue::Int(value)) if *value == idx as i64
            ));
            assert!(matches!(
                project.input.as_ref(),
                LogicalPlan::AggregateStateMerge(_)
            ));
        }
    }
```

Expected before implementation: compile failure because `RewriteBranchUnionRule` is missing.

- [ ] **Step 2: Run the failing rewrite test**

Run:

```bash
cargo test --lib branch_union::tests::rewrites_top_union_of_aggregates_into_branch_scoped_merges -- --nocapture
```

Expected: FAIL at compile time.

- [ ] **Step 3: Implement `RewriteBranchUnionRule`**

In `branch_union.rs`, implement:

```rust
use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::aggregate_rewrite::build_aggregate_state_merge;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::join_delta::plan_output_columns;
use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode, UnionNode};

pub(crate) struct RewriteBranchUnionRule;

impl LogicalRewriteRule for RewriteBranchUnionRule {
    fn name(&self) -> &'static str {
        "RewriteBranchUnion"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root
                    && matches!(
                        delta.input.as_ref(),
                        LogicalPlan::Union(union)
                            if union.all && !plan_contains_imv_marker(delta.input.as_ref())
                    )
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        if !delta.is_root {
            return Ok(RewriteResult::Unchanged);
        }
        let LogicalPlan::Union(union) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };
        if !union.all {
            return Err("Iceberg IMV branch UNION rewrite supports UNION ALL only".to_string());
        }
        if union.inputs.len() < 2 {
            return Err(
                "Iceberg IMV branch UNION rewrite requires at least two aggregate branches"
                    .to_string(),
            );
        }

        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "RewriteBranchUnion requires ImvExtension in RewriteContext".to_string())?
            .clone();
        let UnionNode {
            inputs,
            all: _,
            output_columns,
            required_output_columns,
        } = union;
        let branch_id_column = ext.allocate_column_id();
        let mut rewritten_inputs = Vec::with_capacity(inputs.len());
        for (idx, branch) in inputs.into_iter().enumerate() {
            let branch_id = i32::try_from(idx)
                .map_err(|_| "Iceberg IMV branch UNION branch index overflow".to_string())?;
            let LogicalPlan::Aggregate(aggregate) = branch else {
                return Err(format!(
                    "Iceberg IMV branch UNION rewrite supports only aggregate branches, got {}",
                    plan_kind(&branch)
                ));
            };
            let merge = build_aggregate_state_merge(
                aggregate,
                delta.action_column,
                Some(crate::sql::catalog::BranchScope {
                    branch_id_column_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
                    branch_id,
                }),
                &ext,
            )?;
            rewritten_inputs.push(append_branch_id_project(merge, branch_id, branch_id_column)?);
        }

        Ok(RewriteResult::Changed(LogicalPlan::Union(UnionNode {
            inputs: rewritten_inputs,
            all: true,
            output_columns: branch_union_output_columns(output_columns, branch_id_column),
            required_output_columns,
        })))
    }
}
```

Add helpers:

```rust
fn append_branch_id_project(
    input: LogicalPlan,
    branch_id: i32,
    branch_id_column: crate::sql::column_id::ColumnId,
) -> Result<LogicalPlan, String> {
    let mut items = plan_output_columns(&input)?
        .into_iter()
        .map(|column| ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            output_name: column.name,
            output_column_id: column.column_id,
        })
        .collect::<Vec<_>>();
    items.push(ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(branch_id as i64)),
            data_type: DataType::Int32,
            nullable: false,
        },
        output_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        output_column_id: branch_id_column,
    });
    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        items,
        output_qualifier: None,
        required_output_columns: None,
    }))
}

fn branch_union_output_columns(
    mut output_columns: Vec<OutputColumn>,
    branch_id_column: crate::sql::column_id::ColumnId,
) -> Vec<OutputColumn> {
    output_columns.push(OutputColumn {
        column_id: branch_id_column,
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: true,
    });
    output_columns
}

fn plan_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Project(_) => "Project",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Union(_) => "Union",
        _ => "Other",
    }
}
```

- [ ] **Step 4: Register the module and pipeline stage**

In `src/sql/optimizer/rewrite/imv/mod.rs`, add:

```rust
pub(crate) mod branch_union;
```

In `src/sql/optimizer/rewrite/imv/pipeline.rs`, add the import:

```rust
use crate::sql::optimizer::rewrite::imv::branch_union::RewriteBranchUnionRule;
```

Insert this stage immediately after `imv-delta-marker`:

```rust
        RewriteStage::new(
            "imv-branch-union",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteBranchUnionRule) as Box<dyn LogicalRewriteRule>],
        ),
```

Add a pipeline order assertion:

```rust
        let branch_union = names
            .iter()
            .position(|n| *n == "imv-branch-union")
            .expect("branch union stage must exist");
        assert!(branch_union < pushdown, "stage order: {names:?}");
```

- [ ] **Step 5: Add rejection tests**

In `branch_union.rs`, add tests:

```rust
    #[test]
    fn rejects_non_aggregate_branch() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![aggregate_over(scan("t1", 1)), scan("t2", 10)],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        let err = rule.apply(plan, &mut ctx).expect_err("scan branch must fail");
        assert!(
            err.contains("supports only aggregate branches"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn does_not_match_marked_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                    input: Box::new(aggregate_over(scan("t1", 1))),
                    is_root: false,
                    action_column: None,
                }),
                aggregate_over(scan("t2", 10)),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        assert!(!rule.matches(&plan, &ctx));
    }
```

- [ ] **Step 6: Verify rewrite tests**

Run:

```bash
cargo test --lib branch_union::tests pipeline::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/branch_union.rs src/sql/optimizer/rewrite/imv/mod.rs src/sql/optimizer/rewrite/imv/pipeline.rs
git commit -m "feat(imv): rewrite branch union aggregates with branch identity"
```

---

## Task 4: Allow Rewritten Branch UNION Through Action Validation

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_propagation.rs`
- Modify: `src/sql/optimizer/rewrite/imv/action_column.rs`

- [ ] **Step 1: Add failing predicate tests**

In `src/sql/optimizer/rewrite/imv/action_propagation.rs`, add:

```rust
    #[test]
    fn supported_branch_union_is_not_rejected_by_propagation() {
        let rule = PropagateActionColumnRule;
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let plan = branch_union_with_aggregate_state_merge();

        assert!(!rule.matches(&plan, &ctx));
        let LogicalPlan::Union(union) = &plan else {
            panic!("expected union");
        };
        assert!(is_supported_branch_union(union));
    }
```

Add a local helper in the test module:

```rust
    fn branch_union_with_aggregate_state_merge() -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![
                project_with_branch_id(aggregate_state_merge_stub(), 0),
                project_with_branch_id(aggregate_state_merge_stub(), 1),
            ],
            all: true,
            output_columns: vec![
                output_column(1, "region", DataType::Utf8, false, false),
                output_column(2, "s", DataType::Int64, true, false),
                output_column(100, "__branch_id__", DataType::Int32, false, true),
            ],
            required_output_columns: None,
        })
    }
```

Expected before implementation: FAIL because `is_supported_branch_union` does not exist and the union may match the rejection arm.

- [ ] **Step 2: Add failing action-column validation test**

In `src/sql/optimizer/rewrite/imv/action_column.rs`, add:

```rust
    #[test]
    fn validation_accepts_rewritten_branch_union() {
        let plan = branch_union_with_aggregate_state_merge();
        validate(&plan).expect("rewritten branch union should validate");
    }
```

Use test helpers matching the action-propagation branch-union helper. If `validate` is private to the module, the test can call it directly because it is in the same module.

- [ ] **Step 3: Run the failing tests**

Run:

```bash
cargo test --lib action_propagation::tests::supported_branch_union_is_not_rejected_by_propagation action_column::tests::validation_accepts_rewritten_branch_union -- --nocapture
```

Expected: FAIL at compile time or validation rejection.

- [ ] **Step 4: Add `is_supported_branch_union`**

In `action_propagation.rs`, add:

```rust
pub(crate) fn is_supported_branch_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    node.all
        && !node.inputs.is_empty()
        && node.output_columns.iter().any(|column| {
            column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)
                && column.is_internal
        })
        && node.inputs.iter().all(is_supported_branch_union_project)
}

fn is_supported_branch_union_project(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Project(project) = plan else {
        return false;
    };
    let has_branch_id = project.items.iter().any(|item| {
        item.output_name
            .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)
            && matches!(item.expr.kind, ExprKind::Literal(LiteralValue::Int(_)))
    });
    has_branch_id && matches!(project.input.as_ref(), LogicalPlan::AggregateStateMerge(_))
}
```

Add `LiteralValue` to the imports:

```rust
use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
```

- [ ] **Step 5: Update propagation union rejection**

In `PropagateActionColumnRule::matches`, update the `LogicalPlan::Union` arm:

```rust
                    u.inputs.iter().any(subtree_has_action_column)
                        && !is_supported_join_delta_union(u)
                        && !is_supported_fan_in_delta_union(u)
                        && !is_supported_branch_union(u)
```

- [ ] **Step 6: Update action-column validation**

In `src/sql/optimizer/rewrite/imv/action_column.rs`, import the predicate:

```rust
use crate::sql::optimizer::rewrite::imv::action_propagation::{
    first_delta_base_fqn, is_supported_branch_union, is_supported_fan_in_delta_union,
};
```

Add a validation arm before the generic union rejection:

```rust
        LogicalPlan::Union(node) if is_supported_branch_union(node) => {
            for input in &node.inputs {
                validate_node(input)?;
            }
            Ok(())
        }
```

Update the rejection guard:

```rust
                && !is_supported_join_delta_union(node)
                && !is_supported_fan_in_delta_union(node)
                && !is_supported_branch_union(node) =>
```

- [ ] **Step 7: Verify action tests**

Run:

```bash
cargo test --lib action_propagation::tests action_column::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_propagation.rs src/sql/optimizer/rewrite/imv/action_column.rs
git commit -m "feat(imv): accept rewritten branch union in action validation"
```

---

## Task 5: Add Branch-Scoped Aggregate Target Locator

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`
- Modify: `src/engine/mv/refresh_contract.rs`

- [ ] **Step 1: Add failing locator predicate test**

In `src/engine/mv/iceberg_target_apply.rs`, add:

```rust
    #[test]
    fn branch_scoped_string_key_matches_only_same_branch() {
        let requested = requested_apply_key_values(ApplyKeyRequest::BranchUtf8(&[
            BranchStringApplyKey {
                branch_id: 1,
                key: "k1".to_string(),
            },
        ]));

        assert!(!branch_scoped_string_apply_key_matches(
            0,
            "k1",
            &requested
        ));
        assert!(branch_scoped_string_apply_key_matches(1, "k1", &requested));
        assert!(!branch_scoped_string_apply_key_matches(
            1,
            "k2",
            &requested
        ));
    }
```

Expected before implementation: compile failure because `BranchUtf8`, `BranchStringApplyKey`, and `branch_scoped_string_apply_key_matches` do not exist.

- [ ] **Step 2: Run the failing locator test**

Run:

```bash
cargo test --lib iceberg_target_apply::tests::branch_scoped_string_key_matches_only_same_branch -- --nocapture
```

Expected: FAIL at compile time.

- [ ] **Step 3: Add branch string apply-key types**

In `iceberg_target_apply.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BranchStringApplyKey {
    pub branch_id: i32,
    pub key: String,
}
```

Extend `ApplyKeyRequest`:

```rust
    BranchUtf8(&'a [BranchStringApplyKey]),
```

Extend `ApplyKeyValue`:

```rust
    BranchUtf8(BranchStringApplyKey),
```

Update `Display`:

```rust
            Self::BranchUtf8(value) => {
                write!(f, "branch {} apply key {}", value.branch_id, value.key)
            }
```

Update `requested_apply_key_values`:

```rust
        ApplyKeyRequest::BranchUtf8(keys) => keys
            .iter()
            .cloned()
            .map(ApplyKeyValue::BranchUtf8)
            .collect::<std::collections::HashSet<_>>(),
```

- [ ] **Step 4: Add branch string matching helper**

In `iceberg_target_apply.rs`, add:

```rust
fn branch_scoped_string_apply_key_matches(
    row_branch_id: i32,
    row_key: &str,
    requested: &std::collections::HashSet<ApplyKeyValue>,
) -> bool {
    requested.contains(&ApplyKeyValue::BranchUtf8(BranchStringApplyKey {
        branch_id: row_branch_id,
        key: row_key.to_string(),
    }))
}
```

- [ ] **Step 5: Add async branch-scoped aggregate locator**

Add:

```rust
pub(crate) async fn locate_target_rows_by_branch_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[BranchStringApplyKey],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::BranchUtf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}
```

Update `ApplyKeyRequest::is_empty`:

```rust
            Self::BranchUtf8(keys) => keys.is_empty(),
```

Update `locate_target_rows_by_apply_key_impl`:

```rust
    let request_is_i64 = matches!(requested_keys, ApplyKeyRequest::Int64(_));
    let request_is_branch = matches!(
        requested_keys,
        ApplyKeyRequest::BranchInt64(_) | ApplyKeyRequest::BranchUtf8(_)
    );
    let mut select_columns = vec!["_file".to_string(), "_pos".to_string()];
    if request_is_branch {
        select_columns.push(ICEBERG_MV_BRANCH_ID_COLUMN.to_string());
    }
    select_columns.push(apply_key_column.to_string());
```

Change the processing dispatch:

```rust
        match requested_keys {
            ApplyKeyRequest::BranchInt64(_) => process_branch_i64_apply_key_locator_batch(
                &batch,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?,
            ApplyKeyRequest::BranchUtf8(_) => process_branch_utf8_apply_key_locator_batch(
                &batch,
                apply_key_column,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?,
            ApplyKeyRequest::Int64(_) | ApplyKeyRequest::Utf8(_) => process_apply_key_locator_batch(
                &batch,
                apply_key_column,
                request_is_i64,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?,
        }
```

Rename the existing `process_branch_apply_key_locator_batch` to `process_branch_i64_apply_key_locator_batch`. Add a sibling `process_branch_utf8_apply_key_locator_batch` that casts the apply-key column to `Utf8` and records `ApplyKeyValue::BranchUtf8`.

- [ ] **Step 6: Add merge-sink apply key value type**

In `src/engine/mv/iceberg_merge_sink.rs`, extend `ApplyKeyValueType`:

```rust
    BranchUtf8,
```

Update code paths that branch on `ApplyKeyValueType` so `BranchUtf8` extracts `__branch_id__` plus a Utf8 apply-key column. Use the existing `BranchInt64` branch as the template, replacing the key cast with Utf8.

- [ ] **Step 7: Wire contract constructor to `BranchUtf8`**

In `ApplyKeyContract::branch_union_aggregate_group_row`, change:

```rust
value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::BranchUtf8,
```

The column name remains `ICEBERG_MV_GROUP_APPLY_KEY_COLUMN`.

- [ ] **Step 8: Verify locator tests**

Run:

```bash
cargo test --lib iceberg_target_apply::tests::branch_scoped_string_key_matches_only_same_branch iceberg_target_apply::tests::requested_apply_key_values -- --nocapture
```

Expected: PASS. If there is no existing `requested_apply_key_values` test name, run:

```bash
cargo test --lib iceberg_target_apply::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/engine/mv/iceberg_target_apply.rs src/engine/mv/iceberg_merge_sink.rs src/engine/mv/refresh_contract.rs
git commit -m "feat(imv): locate branch aggregate rows by branch and group key"
```

---

## Task 6: Route B-Family Refresh Execution

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Un-ignore the existing B-family acceptance test**

In `src/engine/mv/iceberg_refresh.rs`, remove:

```rust
    #[ignore = "pending IMV-v2 unified engine (RFC 2026-06-03); B-family execution not wired"]
```

from `union_of_aggregates_keeps_same_group_key_independent_across_branches`.

- [ ] **Step 2: Run the failing acceptance test**

Run:

```bash
cargo test --lib union_of_aggregates_keeps_same_group_key_independent_across_branches -- --nocapture
```

Expected before implementation: FAIL with the temporary refresh execution message from Task 1.

- [ ] **Step 3: Add B-family shape extraction helper**

In `iceberg_refresh.rs`, add:

```rust
fn branch_union_aggregate_shape_for_refresh(
    shape: &IncrementalMvShape,
) -> Result<(&UnionAllMvShape, AggregateMvShape), String> {
    let IncrementalMvShape::UnionAll(union_shape) = shape else {
        return Err("B-family aggregate UNION refresh requires UNION ALL shape".to_string());
    };
    if union_shape.branch_kind != UnionBranchKind::Aggregate {
        return Err("B-family aggregate UNION refresh requires aggregate branches".to_string());
    }
    let first = first_union_aggregate_branch(union_shape)?;
    Ok((union_shape, first.clone()))
}
```

- [ ] **Step 4: Route `BranchUnionAggregate` in refresh dispatcher**

In `refresh_iceberg_mv_with_planned_partitions`, replace the temporary `BranchUnionAggregate` error with:

```rust
        crate::engine::mv::refresh_contract::RefreshStrategy::BranchUnionAggregate => {
            let shape = classify_incremental_mv_query(&canonical_select_query)?;
            let (union_shape, aggregate_shape) = branch_union_aggregate_shape_for_refresh(&shape)?;
            return refresh_branch_union_aggregate_iceberg_mv(
                state,
                &target,
                &target_entry,
                &iceberg_catalog,
                &target_loaded.table,
                expected_main_snapshot_id_from_table(&target_loaded.table),
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                schema_contract,
                union_shape,
                &aggregate_shape,
                refresh_contract.apply_key,
                &planned_affected_partitions,
            );
        }
```

This is parallel to `refresh_fan_in_aggregate_iceberg_mv`, but validates union branch metadata.

- [ ] **Step 5: Implement `refresh_branch_union_aggregate_iceberg_mv`**

Add the function near `refresh_fan_in_aggregate_iceberg_mv`:

```rust
#[allow(clippy::too_many_arguments)]
fn refresh_branch_union_aggregate_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    union_shape: &UnionAllMvShape,
    aggregate_shape: &AggregateMvShape,
    apply_key: ApplyKeyContract,
    planned_affected_partitions: &crate::engine::mv::partition::AffectedMvPartitions,
) -> Result<StatementResult, String> {
    validate_branch_union_aggregate_base_refs(union_shape, base_refs)?;
    validate_branch_union_contract(schema_contract, union_shape)?;

    let mut pre_pin_current_snapshots = BTreeMap::new();
    for base_ref in base_refs {
        let loaded = load_current_iceberg_base_table(state, base_ref)?;
        validate_aggregate_schema_contract_for_base(
            schema_contract,
            base_ref,
            &loaded.table,
            target_table,
        )?;
        pre_pin_current_snapshots.insert(
            base_ref.fqn(),
            loaded.table.metadata().current_snapshot().map(|s| s.snapshot_id()),
        );
    }

    let refresh_label = format!(
        "iceberg branch UNION aggregate MV {}.{}.{}",
        target.catalog, target.namespace, target.table
    );
    let pre_pin_statuses = base_refs
        .iter()
        .map(|base_ref| {
            base_snapshot_status_for_refresh(
                base_ref,
                mv_definition.last_refresh_snapshots.get(&base_ref.fqn()).copied(),
                pre_pin_current_snapshots.get(&base_ref.fqn()).copied().flatten(),
            )
        })
        .collect::<Vec<_>>();
    match decide_refresh(BaseSnapshotPolicy::AllBasesRequired, &pre_pin_statuses, &refresh_label) {
        RefreshDecision::SkipEmpty => return Ok(StatementResult::Ok),
        RefreshDecision::FailFast { reason } => return Err(reason),
        RefreshDecision::FirstRefresh | RefreshDecision::MetadataOnly | RefreshDecision::Incremental => {}
    }

    let pin = crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin::capture(
        state, base_refs,
    )?;
    validate_refresh_pin_table_uuids(mv_definition, &pin, base_refs)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let ctx = {
        let iceberg_catalog_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        IcebergMvRefreshContext::new_with_affected_partitions(
            target.clone(),
            mv_definition.mv_id,
            current_catalog,
            current_database,
            Arc::new(mv_definition.clone()),
            Arc::new(canonical_select_query),
            Arc::from(base_refs.to_vec()),
            Arc::new(pin.clone()),
            &iceberg_catalog_guard,
            Arc::new(target_entry.clone()),
            iceberg_catalog.clone(),
            target_table.clone(),
            planned_affected_partitions.clone(),
        )?
    };
    let refresh_decision = decide_refresh(
        BaseSnapshotPolicy::AllBasesRequired,
        &base_refs
            .iter()
            .map(|base_ref| {
                base_snapshot_status_for_refresh(
                    base_ref,
                    mv_definition.last_refresh_snapshots.get(&base_ref.fqn()).copied(),
                    pin.get(base_ref),
                )
            })
            .collect::<Vec<_>>(),
        &refresh_label,
    );

    IcebergMvRefreshLifecycle::run(
        refresh_decision,
        || {
            let staging_branch = format!(
                "__nova_mv_refresh_{}_{}",
                mv_definition.mv_id,
                uuid::Uuid::new_v4().simple()
            );
            let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                state,
                target,
                mv_definition.mv_id,
                expected_main_snapshot_id,
                pin.to_snapshot_map(),
                &staging_branch,
            )?;
            first_refresh_iceberg_branch_union_aggregate_mv(
                state,
                &ctx,
                &staging_branch,
                refresh_id,
                union_shape,
            )
        },
        || {
            finalize_iceberg_mv_metadata_only_refresh(
                state,
                target,
                mv_definition,
                pin.to_snapshot_map(),
                pin.to_table_uuid_map(),
            )
        },
        || {
            let base_changes = branch_union_base_changes(base_refs, mv_definition, &pin)?;
            incremental_refresh_iceberg_mv_with_changes(
                state,
                &ctx,
                base_changes,
                None,
                RewriteMergeRefreshOptions { apply_key },
            )
        },
    )
}
```

If the exact `incremental_refresh_iceberg_mv_with_changes` signature differs after earlier tasks, use the current signature and pass the same options object used by fan-in aggregate, with `apply_key` set to `BranchUtf8`.

- [ ] **Step 6: Add branch validation helpers**

Add:

```rust
fn validate_branch_union_contract(
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    union_shape: &UnionAllMvShape,
) -> Result<(), String> {
    let branch = schema_contract.branch.as_ref().ok_or_else(|| {
        "B-family aggregate UNION refresh requires branch schema contract".to_string()
    })?;
    if branch.branch_count != union_shape.branches.len() as u32 {
        return Err(format!(
            "B-family aggregate UNION branch count mismatch: contract={} shape={}",
            branch.branch_count,
            union_shape.branches.len()
        ));
    }
    if branch.inner_apply_key_source
        != crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId
    {
        return Err(
            "B-family aggregate UNION refresh requires GroupRowId branch apply key".to_string(),
        );
    }
    Ok(())
}

fn validate_branch_union_aggregate_base_refs(
    union_shape: &UnionAllMvShape,
    base_refs: &[IcebergTableRef],
) -> Result<(), String> {
    if union_shape.branches.len() != base_refs.len() {
        return Err(format!(
            "B-family aggregate UNION branch count {} does not match base refs {}",
            union_shape.branches.len(),
            base_refs.len()
        ));
    }
    Ok(())
}
```

- [ ] **Step 7: Implement branch-aware first refresh SQL**

Add `first_refresh_iceberg_branch_union_aggregate_mv` near first-refresh helpers:

```rust
fn first_refresh_iceberg_branch_union_aggregate_mv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
    staging_branch: &str,
    refresh_id: i64,
    union_shape: &UnionAllMvShape,
) -> Result<StatementResult, String> {
    let physical_sql = iceberg_branch_union_aggregate_first_refresh_sql(
        &ctx.rewrite.mv_definition.select_sql,
        union_shape,
    )?;
    first_refresh_iceberg_mv_with_physical_sql(
        state,
        ctx,
        staging_branch,
        refresh_id,
        &physical_sql,
    )
}
```

Implement `iceberg_branch_union_aggregate_first_refresh_sql` by parsing the stored query, flattening the top-level `UNION ALL`, and adding `__branch_id__` to each branch projection:

```rust
fn iceberg_branch_union_aggregate_first_refresh_sql(
    select_sql: &str,
    union_shape: &UnionAllMvShape,
) -> Result<String, String> {
    let mut stmt = crate::sql::parser::parse_sql_raw(select_sql)
        .map_err(|e| format!("parse B-family aggregate UNION first-refresh SQL: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("B-family aggregate UNION first refresh expects SELECT query".to_string());
    };
    let mut branches = Vec::new();
    flatten_union_all_mut(query.body.as_mut(), &mut branches)?;
    if branches.len() != union_shape.branches.len() {
        return Err(format!(
            "B-family aggregate UNION first-refresh branch count mismatch: sql={} shape={}",
            branches.len(),
            union_shape.branches.len()
        ));
    }
    for (idx, branch) in branches.into_iter().enumerate() {
        append_branch_id_to_select_branch(branch, idx)?;
    }
    Ok(stmt.to_string())
}
```

The helper `append_branch_id_to_select_branch` should require a `SetExpr::Select` branch and push:

```rust
sqlparser::ast::SelectItem::ExprWithAlias {
    expr: sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(idx.to_string(), false)),
    alias: sqlparser::ast::Ident::new(ICEBERG_MV_BRANCH_ID_COLUMN),
}
```

Use the sqlparser `Value` variant names used elsewhere in this repository. If this exact constructor does not compile, inspect `sqlparser::ast::Value` and use the numeric literal variant for the pinned version.

- [ ] **Step 8: Update plan refresh path**

In `plan_iceberg_mv_refresh`, replace the temporary `BranchUnionAggregate` error with a planned refresh mode path parallel to fan-in aggregate:

```rust
        RefreshStrategy::BranchUnionAggregate => {
            let (union_shape, aggregate_shape) =
                branch_union_aggregate_shape_for_refresh(&shape).map_err(RefreshError::user)?;
            validate_branch_union_contract(schema_contract, union_shape)
                .map_err(RefreshError::user)?;
            validate_branch_union_aggregate_base_refs(union_shape, base_refs)
                .map_err(RefreshError::user)?;
            return plan_branch_union_aggregate_mv_refresh(
                state,
                &iceberg_target,
                &target_loaded.table,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                base_refs,
                union_shape,
                &aggregate_shape,
            );
        }
```

Implement `plan_branch_union_aggregate_mv_refresh` using the same mode computation as fan-in aggregate and the same `RefreshPlan` fields that `plan_iceberg_aggregate_mv_refresh` returns. The planned path does not execute the rewrite; it must surface the expected refresh mode and base snapshots.

- [ ] **Step 9: Strengthen rewrite outcome validation**

In `validate_aggregate_refresh_rewrite_outcome`, accept B-family only when both rules changed:

```rust
    if strategy == RefreshStrategy::BranchUnionAggregate
        && !rewrite_outcome_rule_changed(outcome, "RewriteBranchUnion")
    {
        return Err(format!(
            "iceberg {label} MV {} incremental refresh rewrite did not apply RewriteBranchUnion",
            target_fqn
        ));
    }
```

Keep the existing `RewriteAggregateState` requirement.

- [ ] **Step 10: Verify acceptance test**

Run:

```bash
cargo test --lib union_of_aggregates_keeps_same_group_key_independent_across_branches -- --nocapture
```

Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(imv): execute branch union aggregate refresh"
```

---

## Task 7: Add SQL Fixtures for B-Family UNION Aggregate

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_basic.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_branch_empty.result`

- [ ] **Step 1: Create the basic SQL fixture**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql`:

```sql
-- @tags=mv,iceberg,ivm,union_all,aggregate,branch
-- @require=iceberg
-- @explain_contains=RewriteBranchUnion
-- @explain_contains=AggregateStateMerge

DROP MATERIALIZED VIEW IF EXISTS mv_union_agg_basic;
DROP TABLE IF EXISTS ice.sales.bu_t1;
DROP TABLE IF EXISTS ice.sales.bu_t2;

CREATE TABLE ice.sales.bu_t1 (
  id BIGINT,
  region STRING,
  amount BIGINT
) PROPERTIES ("format-version"="3", "write.row-lineage"="true");

CREATE TABLE ice.sales.bu_t2 (
  id BIGINT,
  region STRING,
  amount BIGINT
) PROPERTIES ("format-version"="3", "write.row-lineage"="true");

INSERT INTO ice.sales.bu_t1 VALUES
  (1, 'k1', 10),
  (2, 'k2', 5);

INSERT INTO ice.sales.bu_t2 VALUES
  (3, 'k1', 100),
  (4, 'k3', 7);

CREATE MATERIALIZED VIEW mv_union_agg_basic
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES('storage_engine'='iceberg')
AS
SELECT region, count(*) AS c, sum(amount) AS s
FROM ice.sales.bu_t1
GROUP BY region
UNION ALL
SELECT region, count(*) AS c, sum(amount) AS s
FROM ice.sales.bu_t2
GROUP BY region;

REFRESH MATERIALIZED VIEW mv_union_agg_basic;

SELECT region, c, s FROM mv_union_agg_basic ORDER BY region, s;

DELETE FROM ice.sales.bu_t2 WHERE region = 'k1';
REFRESH MATERIALIZED VIEW mv_union_agg_basic;

SELECT region, c, s FROM mv_union_agg_basic ORDER BY region, s;

INSERT INTO ice.sales.bu_t1 VALUES (5, 'k1', 50);
REFRESH MATERIALIZED VIEW mv_union_agg_basic;

SELECT region, c, s FROM mv_union_agg_basic ORDER BY region, s;
```

- [ ] **Step 2: Create the branch-empty SQL fixture**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql`:

```sql
-- @tags=mv,iceberg,ivm,union_all,aggregate,branch
-- @require=iceberg

DROP MATERIALIZED VIEW IF EXISTS mv_union_agg_empty;
DROP TABLE IF EXISTS ice.sales.bu_empty_t1;
DROP TABLE IF EXISTS ice.sales.bu_empty_t2;

CREATE TABLE ice.sales.bu_empty_t1 (
  id BIGINT,
  region STRING,
  amount BIGINT
) PROPERTIES ("format-version"="3", "write.row-lineage"="true");

CREATE TABLE ice.sales.bu_empty_t2 (
  id BIGINT,
  region STRING,
  amount BIGINT
) PROPERTIES ("format-version"="3", "write.row-lineage"="true");

INSERT INTO ice.sales.bu_empty_t1 VALUES
  (1, 'solo', 9);

CREATE MATERIALIZED VIEW mv_union_agg_empty
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES('storage_engine'='iceberg')
AS
SELECT region, count(*) AS c, sum(amount) AS s
FROM ice.sales.bu_empty_t1
GROUP BY region
UNION ALL
SELECT region, count(*) AS c, sum(amount) AS s
FROM ice.sales.bu_empty_t2
GROUP BY region;

REFRESH MATERIALIZED VIEW mv_union_agg_empty;

SELECT region, c, s FROM mv_union_agg_empty ORDER BY region, s;

INSERT INTO ice.sales.bu_empty_t2 VALUES
  (2, 'solo', 11),
  (3, 'east', 3);
REFRESH MATERIALIZED VIEW mv_union_agg_empty;

SELECT region, c, s FROM mv_union_agg_empty ORDER BY region, s;

DELETE FROM ice.sales.bu_empty_t2 WHERE region = 'solo';
REFRESH MATERIALIZED VIEW mv_union_agg_empty;

SELECT region, c, s FROM mv_union_agg_empty ORDER BY region, s;
```

- [ ] **Step 3: Record the two fixtures**

Start the local Iceberg test environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Record:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_union_of_aggregates_basic,iceberg_ivm_union_of_aggregates_branch_empty \
  --mode record
```

Expected: result files are created under `sql-tests/iceberg-ivm/result/`.

- [ ] **Step 4: Inspect the recorded results**

Open the result files and verify these result sets exist:

`iceberg_ivm_union_of_aggregates_basic.result` first refresh:

```text
k1	1	10
k1	1	100
k2	1	5
k3	1	7
```

after deleting branch `t2.k1`:

```text
k1	1	10
k2	1	5
k3	1	7
```

after inserting into branch `t1.k1`:

```text
k1	2	60
k2	1	5
k3	1	7
```

`iceberg_ivm_union_of_aggregates_branch_empty.result` first refresh:

```text
solo	1	9
```

after inserting into the formerly empty branch:

```text
east	1	3
solo	1	9
solo	1	11
```

after deleting branch `t2.solo`:

```text
east	1	3
solo	1	9
```

- [ ] **Step 5: Verify fixtures**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_union_of_aggregates_basic,iceberg_ivm_union_of_aggregates_branch_empty \
  --mode verify
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_basic.result \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_branch_empty.result
git commit -m "test(imv): add branch union aggregate SQL fixtures"
```

---

## Task 8: Final Verification and Cleanup

**Files:**
- Modify only files touched by earlier tasks if verification exposes a defect.

- [ ] **Step 1: Run focused Rust tests**

Run:

```bash
cargo test --lib \
  refresh_contract::tests \
  aggregate_rewrite::tests \
  branch_union::tests \
  action_propagation::tests \
  action_column::tests \
  iceberg_target_apply::tests \
  union_of_aggregates_keeps_same_group_key_independent_across_branches \
  -- --nocapture
```

Expected: PASS.

- [ ] **Step 2: Run library build**

Run:

```bash
cargo build --lib
```

Expected: PASS.

- [ ] **Step 3: Run SQL fixture verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_union_of_aggregates_basic,iceberg_ivm_union_of_aggregates_branch_empty \
  --mode verify
```

Expected: PASS.

- [ ] **Step 4: Run formatting**

Run:

```bash
cargo fmt
```

Expected: no formatting failures.

- [ ] **Step 5: Run diff check**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 6: Confirm no unsupported B-family strings remain in Rust code**

Run:

```bash
rg -n "UnsupportedBranchUnionAggregate|recognized but refresh execution is not supported|not wired yet" src
```

Expected: no output.

- [ ] **Step 7: Commit verification-only cleanup if needed**

If any verification fix was required, commit it:

```bash
git add -A
git commit -m "fix(imv): finalize branch union aggregate refresh"
```

If no files changed, do not create an empty commit.
