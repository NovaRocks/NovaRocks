# Connector Metadata P2 Scan Binding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 standalone Iceberg scan-binding 在 codegen 阶段现场读取当前 snapshot 并生成 scan ranges，同时让 query-prep 注册路径只注册 schema-only `TableDef`。

**Architecture:** P2 仍不切 analyzer 到 `CatalogMgr`，也不删除 `register_external_tables_for_query`，所以并发 drop/register bug 仍留到 P3。P2 只移动 Iceberg files 的生命周期：`IcebergTableSource` 在注册时不展开 files，`IcebergConnectorScanPlanner` 在 codegen `plan_splits` 时通过 `IcebergCatalogRegistry` 读取当前 snapshot。显式 snapshot / MV refresh scan sources 继续使用 already-materialized files，不被改成 current snapshot。

**Tech Stack:** Rust, existing `CatalogBackend` / `TableSource` / `ConnectorScanPlanner`, `IcebergCatalogRegistry`, `ScanSource::IcebergDataFiles`, existing thrift scan-range contract (`TPlanFragmentExecParams.per_node_scan_ranges`).

---

## Non-Goals

- 不把 analyzer 改到 `CatalogMgr`。
- 不删除 `register_external_tables_for_query`、`drop_registered_external_table` 或全局 `InMemoryCatalog` 外表注册。
- 不改 `src/lower/**`，不把 Iceberg catalog 依赖下沉到 BE lower。
- 不改变 `IcebergVersionTable` / `IcebergMvTargetState` 等显式 snapshot refresh-only scan 的语义。
- 不实现 P3 的 schema-id remote probe 或 cache invalidation wiring。

## File Structure

- Modify `src/connector/backend.rs`
  - Add a schema-only `TableSource` entry point with default fallback.
- Modify `src/connector/iceberg/catalog/backend.rs`
  - Override schema-only table-def construction for Iceberg.
  - Extract reusable row-lineage metadata column helper.
  - Expose data-file conversion for the scan planner.
- Modify `src/connector/iceberg/scan_planner.rs`
  - Make the Iceberg planner optionally registry-backed.
  - Add a current-snapshot split source distinct from explicit files.
- Modify `src/connector/mod.rs`
  - Register the standalone Iceberg scan planner with the shared `IcebergCatalogRegistry`.
- Modify `src/sql/codegen/fragment_builder.rs`
  - Build ordinary Iceberg scans with current-snapshot handles.
  - Keep explicit-file scan sources explicit for refresh-only paths.
- Modify `src/engine/query_prep.rs`
  - Register Iceberg external tables with schema-only table defs.
- Test additions live in the same modules as the changed code.

---

## Task 1: Add schema-only table definition path

**Files:**
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: Write failing tests**

In `src/connector/iceberg/catalog/backend.rs`, add tests near existing row-lineage table-def tests:

```rust
#[test]
fn schema_only_v3_row_lineage_table_def_keeps_metadata_columns_without_files() {
    let table_def = build_iceberg_schema_table_def(
        &test_entry(),
        "ice",
        "db",
        "t",
        v3_row_lineage_loaded_table(),
    )
    .expect("schema-only table def");

    let names = table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec!["_file", "_pos", "_row_id", "_last_updated_sequence_number"]
    );
    let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
        panic!("expected iceberg data-file scan source");
    };
    assert!(
        files.is_empty(),
        "schema-only registration must not carry scan-binding files"
    );
}

#[test]
fn schema_only_v2_table_def_hides_row_lineage_metadata_columns() {
    let table_def = build_iceberg_schema_table_def(
        &test_entry(),
        "ice",
        "db",
        "t",
        loaded_table(),
    )
    .expect("schema-only table def");

    assert!(table_def.iceberg_row_lineage_metadata_columns.is_empty());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib connector::iceberg::catalog::backend::tests::schema_only_ 2>&1 | tail -30
```

Expected: compile failure because `build_iceberg_schema_table_def` is not defined.

- [ ] **Step 3: Add `TableSource::build_schema_table_def`**

In `src/connector/backend.rs`, extend `TableSource`:

```rust
pub(crate) trait TableSource: Send + Sync {
    fn name(&self) -> &'static str;

    /// Build a `TableDef` suitable for registration in the in-memory logical
    /// catalog. Different backends pick different `ScanSource` variants
    /// (IcebergDataFiles / IcebergMetadataTable / IcebergDeltaTable).
    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String>;

    /// Build a schema-only `TableDef` for catalog registration. The default
    /// preserves existing connector behavior. Iceberg overrides this to avoid
    /// expanding snapshot data files during query-prep registration.
    fn build_schema_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        self.build_table_def(table)
    }

    /// Phase-1 entry point for time-travel-aware table-def construction.
    /// Default impl ignores the snapshot pin and delegates to `build_table_def`,
    /// which is correct for connectors that do not have time-travel semantics.
    fn build_table_def_at(
        &self,
        table: &ResolvedTable,
        _snapshot_id: Option<i64>,
    ) -> Result<TableDef, String> {
        self.build_table_def(table)
    }
}
```

- [ ] **Step 4: Add schema-only Iceberg helper**

In `src/connector/iceberg/catalog/backend.rs`, add this helper above `build_iceberg_table_def_with_data_files`:

```rust
fn build_iceberg_schema_table_def(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files_impl(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        Vec::new(),
        IcebergTableDefMode::SchemaOnly,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcebergTableDefMode {
    ScanBinding,
    SchemaOnly,
}
```

Rename the existing `build_iceberg_table_def_with_data_files` body into an impl helper:

```rust
fn build_iceberg_table_def_with_data_files(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files_impl(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
        IcebergTableDefMode::ScanBinding,
    )
}
```

Then make the old body the new `build_iceberg_table_def_with_data_files_impl(...)`.

- [ ] **Step 5: Extract row-lineage metadata columns**

In `src/connector/iceberg/catalog/backend.rs`, add:

```rust
fn iceberg_row_lineage_metadata_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "_file".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: "_pos".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: "_row_id".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: "_last_updated_sequence_number".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ]
}
```

Inside `build_iceberg_table_def_with_data_files_impl`, replace the row-lineage metadata-column decision with:

```rust
let iceberg_row_lineage_metadata_columns = match mode {
    IcebergTableDefMode::SchemaOnly => {
        if row_lineage_enabled(loaded.table.metadata()) {
            iceberg_row_lineage_metadata_columns()
        } else {
            vec![]
        }
    }
    IcebergTableDefMode::ScanBinding => {
        if has_data_files && is_v3_row_lineage(loaded.table.metadata()) && all_files_have_first_row_id
        {
            iceberg_row_lineage_metadata_columns()
        } else {
            if has_data_files
                && is_v3_row_lineage(loaded.table.metadata())
                && !all_files_have_first_row_id
            {
                tracing::warn!(
                    table = %format!("{}.{}", namespace, table_name),
                    "iceberg table declares write.row-lineage=true but at least one data file lacks \
                     first_row_id; row-lineage metadata columns (_row_id, _last_updated_sequence_number, \
                     _file, _pos) are hidden; downstream features depending on row lineage \
                     (e.g. IVM apply-key) will not see correct data for those rows"
                );
            }
            vec![]
        }
    }
};
```

- [ ] **Step 6: Override Iceberg schema-only table source**

In `impl TableSource for IcebergTableSource`, add:

```rust
fn build_schema_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
    let guard = self.registry.read().expect("iceberg catalog read lock");
    let entry = guard.get(&table.catalog)?;
    let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
    build_iceberg_schema_table_def(
        &entry,
        &table.catalog,
        &table.namespace,
        &table.table,
        loaded,
    )
}
```

- [ ] **Step 7: Run targeted tests**

Run:

```bash
cargo test --lib connector::iceberg::catalog::backend::tests::schema_only_ 2>&1 | tail -30
cargo test --lib connector::iceberg::catalog::backend::tests::empty_v3_row_lineage_table_def_hides_metadata_columns 2>&1 | tail -20
cargo test --lib connector::iceberg::catalog::backend::tests::non_empty_v3_row_lineage_table_def_keeps_metadata_columns 2>&1 | tail -20
```

Expected: schema-only tests pass; existing empty/non-empty scan-binding tests keep their old behavior.

- [ ] **Step 8: Commit**

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(connector): add schema-only table source path"
```

---

## Task 2: Make Iceberg scan planner support current-snapshot split planning

**Files:**
- Modify: `src/connector/iceberg/scan_planner.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: Write failing explicit/current split-source tests**

In `src/connector/iceberg/scan_planner.rs` tests, add a unit test for the handle constructors:

```rust
#[test]
fn current_snapshot_table_handle_does_not_embed_files() {
    let table_info = test_iceberg_table_info();
    let handle = IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
        "ice",
        "db",
        "t",
        table_info,
        vec!["id".to_string()],
    );
    let inner = handle
        .downcast_ref::<IcebergTableHandle>()
        .expect("iceberg table handle");

    assert!(matches!(inner.split_source, IcebergSplitSource::CurrentSnapshot));
}

#[test]
fn explicit_file_table_handle_preserves_files() {
    let file = test_data_file("s3://bucket/old.parquet");
    let handle = IcebergConnectorScanPlanner::table_handle_from_source(
        "ice",
        "db",
        "t",
        Some(7),
        test_iceberg_table_info(),
        vec![file.clone()],
        vec!["id".to_string()],
    );
    let inner = handle
        .downcast_ref::<IcebergTableHandle>()
        .expect("iceberg table handle");

    let IcebergSplitSource::ExplicitFiles(files) = &inner.split_source else {
        panic!("expected explicit files");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, file.path);
}
```

Use local test helpers in the same test module:

```rust
fn test_iceberg_table_info() -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "t".to_string(),
        table_uuid: None,
        current_snapshot_id: Some(7),
        schema_id: 0,
        location: "s3://bucket/t".to_string(),
        schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
        serialized_metadata: None,
    }
}

fn test_data_file(path: &str) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: path.to_string(),
        size: 1,
        row_count: Some(1),
        column_stats: None,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        ivm_change_op: None,
        delete_files: vec![],
        manifest_path: None,
        partition_values: vec![],
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner::tests::current_snapshot_table_handle_does_not_embed_files 2>&1 | tail -30
```

Expected: compile failure because `table_handle_for_current_snapshot` / `IcebergSplitSource` do not exist.

- [ ] **Step 3: Add split-source enum and registry-backed planner**

In `src/connector/iceberg/scan_planner.rs`, update imports:

```rust
use std::sync::{Arc, RwLock};

use crate::connector::iceberg::catalog::registry::IcebergCatalogRegistry;
```

Add:

```rust
#[derive(Clone, Debug)]
pub(crate) enum IcebergSplitSource {
    CurrentSnapshot,
    ExplicitFiles(Vec<IcebergDataFileInfo>),
}
```

Change `IcebergTableHandle`:

```rust
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) split_source: IcebergSplitSource,
    pub(crate) column_names: Vec<String>,
}
```

Change `IcebergConnectorScanPlanner`:

```rust
#[derive(Debug, Default)]
pub(crate) struct IcebergConnectorScanPlanner {
    registry: Option<Arc<RwLock<IcebergCatalogRegistry>>>,
}

impl IcebergConnectorScanPlanner {
    pub(crate) fn new() -> Self {
        Self { registry: None }
    }

    pub(crate) fn with_catalog_registry(
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Self {
        Self {
            registry: Some(registry),
        }
    }
```

Update `table_handle_from_source` to set explicit files:

```rust
split_source: IcebergSplitSource::ExplicitFiles(files),
```

Add current-snapshot constructor:

```rust
pub(crate) fn table_handle_for_current_snapshot(
    catalog: &str,
    namespace: &str,
    table: &str,
    table_info: IcebergTableInfo,
    column_names: Vec<String>,
) -> TableHandle {
    TableHandle::new(
        CONNECTOR_ID,
        IcebergTableHandle {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            snapshot_id: None,
            table_info,
            split_source: IcebergSplitSource::CurrentSnapshot,
            column_names,
        },
    )
}
```

- [ ] **Step 4: Expose data-file conversion for planner**

In `src/connector/iceberg/catalog/backend.rs`, change:

```rust
fn data_file_with_stats_to_iceberg_data_file_info(
```

to:

```rust
pub(crate) fn data_file_with_stats_to_iceberg_data_file_info(
```

- [ ] **Step 5: Plan splits from current snapshot when requested**

In `src/connector/iceberg/scan_planner.rs`, add helper methods:

```rust
fn plan_files_for_scan(&self, table: &IcebergTableHandle) -> Result<Vec<IcebergDataFileInfo>, String> {
    match &table.split_source {
        IcebergSplitSource::ExplicitFiles(files) => Ok(files.clone()),
        IcebergSplitSource::CurrentSnapshot => self.plan_current_snapshot_files(table),
    }
}

fn plan_current_snapshot_files(
    &self,
    table: &IcebergTableHandle,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    let registry = self.registry.as_ref().ok_or_else(|| {
        format!(
            "Iceberg current-snapshot scan {}.{}.{} requires a catalog registry",
            table.catalog, table.namespace, table.table
        )
    })?;
    let entry = {
        let guard = registry
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        guard.get(&table.catalog)?
    };
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &table.namespace,
        &table.table,
    )?;
    let Some(snapshot_id) = loaded.table.metadata().current_snapshot_id() else {
        return Ok(vec![]);
    };
    let data_files = if let Some(cached) =
        entry.cached_data_files(&table.namespace, &table.table, Some(snapshot_id))?
    {
        cached
    } else {
        let extracted =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                &loaded.table,
                snapshot_id,
            )?;
        entry.cache_data_files(
            &table.namespace,
            &table.table,
            Some(snapshot_id),
            extracted.clone(),
        )?;
        extracted
    };
    Ok(data_files
        .into_iter()
        .map(crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
        .collect())
}
```

Update `plan_splits`:

```rust
fn plan_splits(
    &self,
    scan: &ScanHandle,
    _ctx: SplitPlanningContext,
) -> Result<Vec<Split>, String> {
    let scan = iceberg_scan_handle(scan)?;
    Ok(self
        .plan_files_for_scan(&scan.table)?
        .into_iter()
        .map(|file| {
            Split::new(
                CONNECTOR_ID,
                IcebergSplit { data_file: file },
            )
        })
        .collect())
}
```

- [ ] **Step 6: Run targeted scan-planner tests**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner::tests::current_snapshot_table_handle_does_not_embed_files 2>&1 | tail -20
cargo test --lib connector::iceberg::scan_planner::tests::explicit_file_table_handle_preserves_files 2>&1 | tail -20
cargo test --lib connector::iceberg::scan_planner::tests 2>&1 | tail -25
```

Expected: all targeted scan-planner tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/scan_planner.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(iceberg): plan current snapshot splits in scan planner"
```

---

## Task 3: Register stateful Iceberg scan planner in standalone connector registry

**Files:**
- Modify: `src/connector/mod.rs`

- [ ] **Step 1: Write failing registry test**

In `src/connector/mod.rs` inside `scan_planning_registry_tests`, add:

```rust
#[test]
fn default_connectors_register_stateful_iceberg_scan_planner() {
    let state = Arc::new(crate::engine::StandaloneState::default());
    super::register_standalone_backends(&state);
    let connectors = state
        .connectors
        .read()
        .expect("connector registry read lock");
    let planner = connectors.scan_planner("iceberg").expect("iceberg planner");
    let handle = crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
        "missing_catalog",
        "db",
        "t",
        crate::sql::catalog::IcebergTableInfo {
            catalog: "missing_catalog".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: "s3://bucket/t".to_string(),
            schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        },
        vec!["id".to_string()],
    );
    let scan = planner
        .begin_scan(
            handle,
            crate::connector::scan_planning::BeginScanContext::default(),
        )
        .expect("begin scan");
    let err = planner
        .plan_splits(
            &scan,
            crate::connector::scan_planning::SplitPlanningContext::default(),
        )
        .expect_err("stateful planner should consult registry");
    assert!(err.contains("unknown catalog"), "{err}");
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test --lib connector::tests::default_connectors_register_stateful_iceberg_scan_planner 2>&1 | tail -40
```

Expected: failure because default registry still registers a stateless `IcebergConnectorScanPlanner::new()`.

- [ ] **Step 3: Wire registry into default connector registration**

In `src/connector/mod.rs`, replace:

```rust
connectors.register_scan_planner(Arc::new(iceberg::IcebergConnectorScanPlanner::new()));
```

with:

```rust
connectors.register_scan_planner(Arc::new(
    iceberg::IcebergConnectorScanPlanner::with_catalog_registry(Arc::clone(&iceberg_catalogs)),
));
```

Keep tests that instantiate `IcebergConnectorScanPlanner::new()` for pure explicit-file unit tests unchanged.

- [ ] **Step 4: Run targeted test**

Run:

```bash
cargo test --lib connector::tests::default_connectors_register_stateful_iceberg_scan_planner 2>&1 | tail -30
```

Expected: test passes.

- [ ] **Step 5: Commit**

```bash
git add src/connector/mod.rs
git commit -m "feat(connector): register stateful Iceberg scan planner"
```

---

## Task 4: Switch ordinary Iceberg codegen to current-snapshot handles

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Write failing codegen test**

In `src/sql/codegen/fragment_builder.rs` tests, add a counting planner that rejects embedded files for ordinary Iceberg scans:

```rust
#[derive(Debug)]
struct CurrentSnapshotAssertingIcebergPlanner {
    counts: std::sync::Arc<ScanPlannerCallCounts>,
}

impl crate::connector::scan_planning::ConnectorScanPlanner
    for CurrentSnapshotAssertingIcebergPlanner
{
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn begin_scan(
        &self,
        table: crate::connector::scan_planning::TableHandle,
        _ctx: crate::connector::scan_planning::BeginScanContext,
    ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
        self.counts
            .begin_scan
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let inner = table
            .downcast_ref::<crate::connector::iceberg::IcebergTableHandle>()
            .ok_or_else(|| "expected IcebergTableHandle".to_string())?
            .clone();
        assert!(
            matches!(
                inner.split_source,
                crate::connector::iceberg::IcebergSplitSource::CurrentSnapshot
            ),
            "ordinary Iceberg scans must not embed registered files"
        );
        Ok(crate::connector::scan_planning::ScanHandle::new(
            "iceberg",
            crate::connector::iceberg::IcebergScanHandle { table: inner },
        ))
    }

    fn plan_splits(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        _ctx: crate::connector::scan_planning::SplitPlanningContext,
    ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
        self.counts
            .plan_splits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let scan = crate::connector::iceberg::iceberg_scan_handle(scan)?;
        Ok(vec![crate::connector::scan_planning::Split::new(
            "iceberg",
            crate::connector::iceberg::IcebergSplit {
                data_file: iceberg_i32_file("s3://bucket/current.parquet", 1, 1),
            },
        )])
    }

    fn to_thrift_scan(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        splits: &[crate::connector::scan_planning::Split],
        ctx: crate::connector::scan_planning::ThriftScanContext,
    ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
        self.counts
            .to_thrift_scan
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.counts
            .thrift_contexts
            .lock()
            .expect("thrift contexts")
            .push(ctx.clone());
        crate::connector::iceberg::IcebergConnectorScanPlanner::new()
            .to_thrift_scan(scan, splits, ctx)
    }
}
```

Then add:

```rust
#[test]
fn visit_scan_uses_current_snapshot_handle_for_ordinary_iceberg_scan() {
    let mut plan = iceberg_scan_plan();
    if let Operator::PhysicalScan(scan) = &mut plan.op {
        let ScanSource::IcebergDataFiles { files, .. } = &mut scan.table.source else {
            panic!("expected iceberg source");
        };
        files.push(iceberg_i32_file("s3://bucket/stale-registered.parquet", 1, 1));
    }
    let catalog = DummyCatalog;
    let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
    let planner = std::sync::Arc::new(CurrentSnapshotAssertingIcebergPlanner {
        counts: counts.clone(),
    });
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(planner);

    PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
        .expect("build Iceberg fragment");

    assert_eq!(
        counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_uses_current_snapshot_handle_for_ordinary_iceberg_scan 2>&1 | tail -40
```

Expected: failure because current `visit_scan` passes `table_handle_from_source(... files.clone() ...)`.

- [ ] **Step 3: Change ordinary Iceberg scan handle construction**

In `PlanFragmentBuilder::visit_scan`, replace the Iceberg branch:

```rust
let table_handle =
    crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
        &iceberg_table.catalog,
        &iceberg_table.namespace,
        &iceberg_table.table,
        iceberg_table.current_snapshot_id,
        iceberg_table.clone(),
        files.clone(),
        column_names,
    );
```

with:

```rust
let table_handle =
    crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
        &iceberg_table.catalog,
        &iceberg_table.namespace,
        &iceberg_table.table,
        iceberg_table.clone(),
        column_names,
    );
```

Remove `files` from the match binding if it becomes unused:

```rust
crate::sql::catalog::ScanSource::IcebergDataFiles {
    table: iceberg_table,
    ..
} => {
```

Do not change `build_iceberg_scan_ranges_from_source` in `src/sql/codegen/nodes.rs`; that helper handles explicit refresh-only scan sources and must continue using `table_handle_from_source`.

- [ ] **Step 4: Run targeted codegen tests**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_uses_current_snapshot_handle_for_ordinary_iceberg_scan 2>&1 | tail -30
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg 2>&1 | tail -30
cargo test --lib sql::codegen::fragment_builder::tests::scan_dict_column_on_iceberg_scan_is_supported 2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "feat(codegen): bind Iceberg scans to current snapshot"
```

---

## Task 5: Register external Iceberg tables as schema-only

**Files:**
- Modify: `src/engine/query_prep.rs`

- [ ] **Step 1: Write failing regression assertion**

In `src/engine/query_prep.rs` tests, add a small helper test for the registration call site by introducing a private wrapper function above `register_external_table_by_name`:

```rust
fn build_registration_table_def(
    source: &dyn crate::connector::backend::TableSource,
    resolved: &crate::connector::backend::ResolvedTable,
) -> Result<TableDef, String> {
    source.build_schema_table_def(resolved)
}
```

Then add this test:

```rust
#[test]
fn registration_table_def_uses_schema_only_table_source() {
    struct SchemaOnlySource;
    impl crate::connector::backend::TableSource for SchemaOnlySource {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn build_table_def(
            &self,
            _table: &crate::connector::backend::ResolvedTable,
        ) -> Result<TableDef, String> {
            Err("scan-binding path must not be used for registration".to_string())
        }

        fn build_schema_table_def(
            &self,
            table: &crate::connector::backend::ResolvedTable,
        ) -> Result<TableDef, String> {
            Ok(TableDef {
                name: table.table.clone(),
                columns: table.columns.clone(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: test_iceberg_table_info(),
                    files: vec![],
                    cloud_properties: Default::default(),
                },
            })
        }
    }

    let resolved = crate::connector::backend::ResolvedTable {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "t".to_string(),
        columns: vec![],
    };
    let table_def = build_registration_table_def(&SchemaOnlySource, &resolved)
        .expect("schema-only registration");

    let ScanSource::IcebergDataFiles { files, .. } = table_def.source else {
        panic!("expected iceberg source");
    };
    assert!(files.is_empty());
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test --lib engine::query_prep::tests::registration_table_def_uses_schema_only_table_source 2>&1 | tail -40
```

Expected: failure until the helper exists and uses `build_schema_table_def`.

- [ ] **Step 3: Use schema-only registration helper in query-prep**

In `register_external_table_by_name`, replace:

```rust
let table_def = source.build_table_def(&resolved)?;
```

with:

```rust
let table_def = build_registration_table_def(source.as_ref(), &resolved)?;
```

In `register_external_tables_for_query_impl`, replace:

```rust
let table_def = source.build_table_def(&resolved)?;
```

with:

```rust
let table_def = build_registration_table_def(source.as_ref(), &resolved)?;
```

Leave `rewrite_time_travel_refs` / `build_table_def_at(... Some(snapshot_id))` paths unchanged because those are explicit snapshot synthetic tables.

- [ ] **Step 4: Run targeted query-prep tests**

Run:

```bash
cargo test --lib engine::query_prep::tests::registration_table_def_uses_schema_only_table_source 2>&1 | tail -30
cargo test --lib engine::query_prep::tests 2>&1 | tail -30
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/query_prep.rs
git commit -m "feat(query-prep): register Iceberg tables schema-only"
```

---

## Task 6: Verification and P2 guardrails

**Files:** no required source changes unless verification exposes issues.

- [ ] **Step 1: Run codegen and connector targeted tests**

Run:

```bash
cargo test --lib connector::iceberg::catalog::backend::tests::schema_only_ 2>&1 | tail -30
cargo test --lib connector::iceberg::scan_planner::tests 2>&1 | tail -30
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_uses_current_snapshot_handle_for_ordinary_iceberg_scan 2>&1 | tail -30
cargo test --lib engine::query_prep::tests::registration_table_def_uses_schema_only_table_source 2>&1 | tail -30
```

Expected: all targeted tests pass.

- [ ] **Step 2: Run module-level tests that cover scan range generation**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg 2>&1 | tail -30
cargo test --lib sql::codegen::fragment_builder::tests::iceberg_fragment_exec_params 2>&1 | tail -30
cargo test --lib sql::codegen::nodes::tests:: 2>&1 | tail -30
```

Expected: tests pass. If a test name filter matches zero tests, replace it with the exact nearby test name shown by `cargo test --lib <module>::tests -- --list`.

- [ ] **Step 3: Build and clippy**

Run:

```bash
cargo build 2>&1 | tail -15
cargo clippy --lib > /tmp/novarocks-connector-metadata-p2-clippy.log 2>&1
rc=$?
grep -E "connector::iceberg|query_prep|fragment_builder|warning: |error: " /tmp/novarocks-connector-metadata-p2-clippy.log | head -40
exit $rc
```

Expected: build succeeds; clippy exits 0. Existing repo warnings are acceptable, but no new warnings in files touched by this P2 should remain.

- [ ] **Step 4: Verify architectural boundaries**

Run:

```bash
rg -n "IcebergCatalogRegistry|iceberg_catalogs|catalog_mgr" src/lower || true
git diff --stat origin/main...HEAD -- src/lower src/service src/sql/analyzer
```

Expected: no new lower/service/analyzer changes. `src/lower` should not gain catalog dependencies.

- [ ] **Step 5: Optional SQL smoke when local runtime is available**

If `docker/iceberg-rest/runtime/current/env.sh` exists, run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: suite passes. If Docker/runtime is unavailable, state that this smoke was not run.

- [ ] **Step 6: Final commit if verification required fixes**

Only if Step 1-4 required code changes:

```bash
git add <fixed-files>
git commit -m "chore(connector-metadata): verify P2 scan binding"
```

---

## Acceptance Criteria

1. Ordinary standalone Iceberg scans no longer use `ScanSource::IcebergDataFiles.files` from the registered `TableDef`; they use `IcebergConnectorScanPlanner::table_handle_for_current_snapshot`.
2. `register_external_table_by_name` and `register_external_tables_for_query_impl` call the schema-only table-source path, so query-prep registration does not expand current snapshot data files.
3. Explicit-file scan sources for MV refresh / version / target-state paths still use `table_handle_from_source` and keep their pinned snapshot semantics.
4. No changes under `src/lower/**`; BE lower still consumes thrift scan ranges only.
5. Targeted unit tests pass, `cargo build` passes, and clippy has no new warnings in touched P2 files.

## Risks and Follow-Ups

- P2 still keeps global drop/register in query-prep, so the original cross-session unknown-table race is not fixed until P3.
- Schema-only registration may expose row-lineage metadata columns based on table metadata rather than data-file inspection. If a table declares row-lineage but current files lack `first_row_id`, scan-binding must fail fast when those metadata columns are actually required.
- P3 should replace `TableSource::build_schema_table_def` usage with `CatalogMgr` / `IcebergCatalog` and remove the global in-memory Iceberg registration path.
