# Iceberg Codegen via Connector `begin_scan`/`plan_splits` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let standalone Iceberg `ScanSource::IcebergDataFiles` codegen call `ConnectorScanPlanner::begin_scan` / `plan_splits` from a registered Iceberg planner to obtain the `ScanHandle` and per-file splits, instead of reading `op.table.source.IcebergDataFiles.files` directly inside `build_exec_params_multi`.

**Architecture:** Introduce `IcebergTableHandle` / `IcebergScanHandle` / `IcebergSplit` opaque types in `src/connector/iceberg/scan_planner.rs` and a stateless `IcebergConnectorScanPlanner`. Register the planner alongside the StarRocks one in `register_standalone_backends`. `visit_scan` calls the Iceberg planner for `ScanSource::IcebergDataFiles` to populate `ResolvedTable.planned_scan`. `build_exec_params_multi` reads files from `planned_scan.splits` (downcast `Split` → `IcebergSplit`) instead of from `ScanSource.files`. min-max file pruning, `change_op_slot`, and per-query state stay on the codegen side. `cloud_properties` continues to be read from `ScanSource::IcebergDataFiles.cloud_properties` by `build_hdfs_scan_node` (per-scan-node metadata, not per-split). `to_thrift_scan` is implemented as a stub that returns an error so the trait contract holds without migrating HDFS scan-range generation. `IcebergMetadataTable` and `IcebergDeltaTable` keep their existing codegen paths and do not yet flow through the connector.

**Tech Stack:** Rust, existing `ConnectorRegistry` / `ConnectorScanPlanner`, existing standalone codegen (`PlanFragmentBuilder`, `build_exec_params_multi`), `cargo test --lib`.

---

## Scope Check

The parent spec (`docs/superpowers/specs/2026-05-28-connector-first-standalone-scan-design.md`) Stage 3 covers full Iceberg migration. This plan completes only the connector-adapter bootstrap for `IcebergDataFiles`:

- New connector planner types and a stateless `IcebergConnectorScanPlanner`.
- Codegen path for `IcebergDataFiles` reads splits from the planner.
- `to_thrift_scan` returns a stub error (HDFS scan-range generation stays in `nodes.rs`).

Out of scope (later slices):

- Migrating `IcebergMetadataTable` and `IcebergDeltaTable` through the connector.
- Moving HDFS scan-range generation (`build_hdfs_scan_range_params_for_file`, `build_hdfs_scan_node`) into the connector's `to_thrift_scan`.
- Deleting `ScanSource::IcebergDataFiles.files` field (still read by optimizer / planner / query_prep / dictionary / explain — Stage 4 / Stage 5 work).
- Moving Iceberg file enumeration out of query_prep into the connector.

## File Structure

- Create: `src/connector/iceberg/scan_planner.rs`
  - Defines `IcebergTableHandle`, `IcebergScanHandle`, `IcebergSplit`, `IcebergConnectorScanPlanner`.
  - Implements `ConnectorTableHandle` / `ConnectorScanHandle` / `ConnectorSplit` / `ConnectorScanPlanner` traits.
  - Implements `begin_scan` and `plan_splits`; `to_thrift_scan` returns an error.
  - Holds `iceberg_scan_handle(scan: &ScanHandle) -> Result<&IcebergScanHandle, String>` and `iceberg_split(split: &Split) -> Result<&IcebergSplit, String>` downcast helpers.
  - Includes a focused unit test (`downcasts_iceberg_scan_and_split`).

- Modify: `src/connector/iceberg/mod.rs`
  - Adds `pub(crate) mod scan_planner;`.
  - Re-exports `IcebergConnectorScanPlanner`, `IcebergScanHandle`, `IcebergSplit`, `IcebergTableHandle` at crate-level (matching the `starrocks/table/mod.rs` re-export pattern).

- Modify: `src/connector/mod.rs`
  - In `register_standalone_backends`, registers `IcebergConnectorScanPlanner::new()` next to the StarRocks scan planner.
  - Adds a registration test in `scan_planning_registry_tests`.

- Modify: `src/sql/codegen/fragment_builder.rs`
  - In `visit_scan`, replaces the `_ => None` arm of `let planned_scan = match &op.table.source { ... }` with a real Iceberg branch that calls `self.connectors.scan_planner("iceberg")?.begin_scan/plan_splits` for `ScanSource::IcebergDataFiles`. `IcebergMetadataTable` and `IcebergDeltaTable` continue to set `planned_scan = None`.
  - In the test module, adds `mock_iceberg_registry()` helper and a `CountingIcebergScanPlanner` for the end-to-end guard test.

- Modify: `src/sql/codegen/nodes.rs`
  - In `build_exec_params_multi`, changes the `ScanSource::IcebergDataFiles { files, .. }` arm to read files from `planned_scan.splits` (downcast to `IcebergSplit`) instead of `source.files`.
  - The min-max pruning loop and `change_op_slot` handling stay unchanged.

- Modify: `src/engine/mod.rs`
  - Extends `mock_starrocks_registry_for_engine_test` to also register an `IcebergConnectorScanPlanner` so engine tests that happen to involve Iceberg scans still build cleanly after the visit_scan switch.

## Task 1: Add Iceberg connector scan-planner types

**Files:**
- Create: `src/connector/iceberg/scan_planner.rs`
- Modify: `src/connector/iceberg/mod.rs`

- [ ] **Step 1: Write the focused downcast test**

Create `src/connector/iceberg/scan_planner.rs` with the initial test module and minimal imports:

```rust
use std::any::Any;

use crate::connector::scan_planning::{
    ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle, ScanHandle, Split,
};
use crate::sql::catalog::{IcebergDataFileInfo, IcebergTableInfo};

const CONNECTOR_ID: &str = "iceberg";

#[derive(Clone, Debug)]
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) files: Vec<IcebergDataFileInfo>,
}

impl ConnectorTableHandle for IcebergTableHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergScanHandle {
    pub(crate) table: IcebergTableHandle,
}

impl ConnectorScanHandle for IcebergScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergSplit {
    pub(crate) data_file: IcebergDataFileInfo,
}

impl ConnectorSplit for IcebergSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn iceberg_scan_handle(scan: &ScanHandle) -> Result<&IcebergScanHandle, String> {
    scan.downcast_ref::<IcebergScanHandle>()
        .ok_or_else(|| "expected IcebergScanHandle for iceberg scan".to_string())
}

pub(crate) fn iceberg_split(split: &Split) -> Result<&IcebergSplit, String> {
    split
        .downcast_ref::<IcebergSplit>()
        .ok_or_else(|| "expected IcebergSplit for iceberg split".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::scan_planning::{validate_split_connectors, ScanHandle, Split};
    use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};

    fn dummy_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    fn dummy_iceberg_file() -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: "s3://bucket/data/file.parquet".to_string(),
            size: 1024,
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

    #[test]
    fn downcasts_iceberg_scan_and_split() {
        let table = IcebergTableHandle {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(42),
            table_info: dummy_iceberg_table_info(),
            files: vec![dummy_iceberg_file()],
        };
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle {
                table: table.clone(),
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            IcebergSplit {
                data_file: dummy_iceberg_file(),
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(iceberg_scan_handle(&scan).expect("scan").table.table, "orders");
        assert_eq!(
            iceberg_split(&splits[0]).expect("split").data_file.path,
            "s3://bucket/data/file.parquet"
        );
    }
}
```

- [ ] **Step 2: Run the focused test and verify it fails to compile**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner::tests::downcasts_iceberg_scan_and_split
```

Expected: compile failure because `src/connector/iceberg/mod.rs` does not yet expose `scan_planner`.

- [ ] **Step 3: Expose the module and add the planner stub**

In `src/connector/iceberg/mod.rs`, add the module declaration with the other `pub mod` lines:

```rust
pub(crate) mod scan_planner;
```

Then append the `IcebergConnectorScanPlanner` implementation to `src/connector/iceberg/scan_planner.rs` (after the downcast helpers, before the `#[cfg(test)] mod tests`):

```rust
use crate::connector::scan_planning::{
    validate_split_connectors, BeginScanContext, ConnectorScanPlanner, SplitPlanningContext,
    TableHandle, ThriftScanContext, ThriftScanPlan,
};

#[derive(Debug, Default)]
pub(crate) struct IcebergConnectorScanPlanner;

impl IcebergConnectorScanPlanner {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn table_handle_from_source(
        catalog: &str,
        namespace: &str,
        table: &str,
        snapshot_id: Option<i64>,
        table_info: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            IcebergTableHandle {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                snapshot_id,
                table_info,
                files,
            },
        )
    }
}

impl ConnectorScanPlanner for IcebergConnectorScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(
        &self,
        table: TableHandle,
        _ctx: BeginScanContext,
    ) -> Result<ScanHandle, String> {
        let inner = table
            .downcast_ref::<IcebergTableHandle>()
            .ok_or_else(|| "expected IcebergTableHandle for iceberg scan".to_string())?
            .clone();
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle { table: inner },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = iceberg_scan_handle(scan)?;
        Ok(scan
            .table
            .files
            .iter()
            .map(|file| {
                Split::new(
                    CONNECTOR_ID,
                    IcebergSplit {
                        data_file: file.clone(),
                    },
                )
            })
            .collect())
    }

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        _ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String> {
        validate_split_connectors(scan, splits)?;
        Err(
            "IcebergConnectorScanPlanner::to_thrift_scan is not yet implemented; \
             codegen still produces HDFS scan ranges via build_hdfs_scan_range_params_for_file"
                .to_string(),
        )
    }
}
```

- [ ] **Step 4: Run the focused test and verify it passes**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner::tests::downcasts_iceberg_scan_and_split
```

Expected: one passing test.

- [ ] **Step 5: Re-export the public types**

In `src/connector/iceberg/mod.rs`, near the existing `pub use` block (around the `metadata::*` / `schema::*` re-exports), add:

```rust
pub(crate) use scan_planner::{
    IcebergConnectorScanPlanner, IcebergScanHandle, IcebergSplit, IcebergTableHandle,
};
```

- [ ] **Step 6: Build the whole crate**

Run:

```bash
cargo build
```

Expected: clean build (warnings about unused `IcebergConnectorScanPlanner` / `IcebergScanHandle` / `IcebergSplit` are acceptable — Tasks 2-4 consume them).

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/scan_planner.rs src/connector/iceberg/mod.rs
git commit -m "feat(iceberg): add connector scan planner"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 2: Register the Iceberg scan planner in standalone backends

**Files:**
- Modify: `src/connector/mod.rs`

- [ ] **Step 1: Register the planner**

In `src/connector/mod.rs::register_standalone_backends` (~line 258), locate the StarRocks scan-planner registration (look for `connectors.register_scan_planner(Arc::new(starrocks::table::StarRocksTableScanPlanner::new(...)));`) and add an Iceberg registration adjacent to it. After the existing StarRocks registration block, add:

```rust
connectors.register_scan_planner(Arc::new(
    iceberg::IcebergConnectorScanPlanner::new(),
));
```

- [ ] **Step 2: Add a registration test**

In `src/connector/mod.rs`, locate the `#[cfg(test)] mod scan_planning_registry_tests` block (~line 130 area). Add this test inside it:

```rust
#[test]
fn default_registry_does_not_register_standalone_iceberg_scan_planner() {
    let registry = ConnectorRegistry::default();

    let err = registry
        .scan_planner("iceberg")
        .expect_err("standalone planners are registered with state, not Default");

    assert_eq!(err, "unknown scan planner: iceberg");
}
```

This documents the same boundary as the existing StarRocks test: state-bound planners (or registered via `register_standalone_backends`) are not exposed by `ConnectorRegistry::default()`.

- [ ] **Step 3: Run the registry tests**

```bash
cargo test --lib connector::scan_planning_registry_tests
```

Expected: all registry tests pass (the existing two StarRocks tests plus the new iceberg test plus any others — confirm zero failures).

- [ ] **Step 4: Commit**

```bash
git add src/connector/mod.rs
git commit -m "feat(connector): register standalone Iceberg scan planner"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 3: Add `mock_iceberg_registry` test helper and update existing mixed/iceberg test sites

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/engine/mod.rs`

Task 4 will make `visit_scan` invoke the Iceberg planner whenever a scan has `ScanSource::IcebergDataFiles`. Before that, prepare the test fixtures so the registry has the iceberg planner registered everywhere it is needed.

- [ ] **Step 1: Add `mock_iceberg_registry()` helper in `fragment_builder.rs::tests`**

In `src/sql/codegen/fragment_builder.rs`, locate the existing `mock_starrocks_registry(...)` helper (in `#[cfg(test)] mod tests`, near line 4106 area). Add this new helper right after it:

```rust
fn mock_iceberg_registry() -> crate::connector::ConnectorRegistry {
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(std::sync::Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    registry
}
```

Also add a helper that combines both for the MixedCatalog test:

```rust
fn mock_starrocks_and_iceberg_registry(
    layout: &crate::sql::catalog::PhysicalTableLayout,
) -> crate::connector::ConnectorRegistry {
    let mut registry = mock_starrocks_registry(layout);
    registry.register_scan_planner(std::sync::Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    registry
}
```

- [ ] **Step 2: Update tests that already use `mock_starrocks_registry` for a mixed StarRocks+Iceberg scan**

The mixed-join test `mixed_starrocks_and_iceberg_scan_table_ids_do_not_collide` (or similar — find it by `grep -n "MixedCatalog" src/sql/codegen/fragment_builder.rs`) currently uses `mock_starrocks_registry(&layout)`. Switch it to use the combined helper:

```rust
let registry = mock_starrocks_and_iceberg_registry(&layout);
```

This is the only test currently mixing the two; other StarRocks tests continue to use `mock_starrocks_registry(&layout)`.

- [ ] **Step 3: Extend `mock_starrocks_registry_for_engine_test` in `engine/mod.rs`**

In `src/engine/mod.rs` (within the `#[cfg(test)] mod tests` block), locate `mock_starrocks_registry_for_engine_test` and have it also register an `IcebergConnectorScanPlanner` so engine-level tests that build fragments through `build_fragments_for_query` keep working after the visit_scan switch in Task 4:

Change the body from:

```rust
fn mock_starrocks_registry_for_engine_test(...) -> crate::connector::ConnectorRegistry {
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(std::sync::Arc::new(...));
    registry
}
```

to:

```rust
fn mock_starrocks_registry_for_engine_test(...) -> crate::connector::ConnectorRegistry {
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(std::sync::Arc::new(...));
    registry.register_scan_planner(std::sync::Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    registry
}
```

(Keep the existing StarRocks registration body; only add the Iceberg registration line.)

- [ ] **Step 4: Run the fragment_builder tests**

```bash
cargo test --lib sql::codegen::fragment_builder::tests
```

Expected: all tests still pass (Task 4 has not yet wired visit_scan; this task only adds plumbing).

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs src/engine/mod.rs
git commit -m "test(iceberg): add mock_iceberg_registry helper for codegen tests"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 4: Switch `visit_scan` and `build_exec_params_multi` to the Iceberg connector (atomic)

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`

This task is atomic: both files must change together. `visit_scan` starts populating `ResolvedTable.planned_scan` for `IcebergDataFiles` scans, and `build_exec_params_multi` switches to reading files from `planned_scan.splits` instead of `source.files`. The two halves only make sense together.

- [ ] **Step 1: Extend the `planned_scan` match in `visit_scan`**

In `src/sql/codegen/fragment_builder.rs::visit_scan` (~line 488), the current `planned_scan` block is:

```rust
let planned_scan = match &op.table.source {
    crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
        let planner = self.connectors.scan_planner("starrocks")?;
        ...
        Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
    }
    _ => None,
};
```

Add an Iceberg arm before the `_` catch-all. The full new block reads:

```rust
let planned_scan = match &op.table.source {
    crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
        let planner = self.connectors.scan_planner("starrocks")?;
        let table_handle =
            crate::connector::starrocks::table::StarRocksTableScanPlanner::table_handle_from_source(
                &op.database,
                &op.table.name,
                *db_id,
                *table_id,
            );
        let scan = planner.begin_scan(
            table_handle,
            crate::connector::scan_planning::BeginScanContext::default(),
        )?;
        let splits = planner.plan_splits(
            &scan,
            crate::connector::scan_planning::SplitPlanningContext::default(),
        )?;
        Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
    }
    crate::sql::catalog::ScanSource::IcebergDataFiles {
        table: iceberg_table,
        files,
        ..
    } => {
        let planner = self.connectors.scan_planner("iceberg")?;
        let table_handle =
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &iceberg_table.catalog,
                &iceberg_table.namespace,
                &iceberg_table.table,
                iceberg_table.current_snapshot_id,
                iceberg_table.clone(),
                files.clone(),
            );
        let scan = planner.begin_scan(
            table_handle,
            crate::connector::scan_planning::BeginScanContext::default(),
        )?;
        let splits = planner.plan_splits(
            &scan,
            crate::connector::scan_planning::SplitPlanningContext::default(),
        )?;
        Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
    }
    _ => None,
};
```

The `_ => None` branch still covers `IcebergMetadataTable` and `IcebergDeltaTable` (they retain the existing placeholder-range codegen path).

- [ ] **Step 2: Switch the Iceberg arm in `build_exec_params_multi`**

In `src/sql/codegen/nodes.rs::build_exec_params_multi` (~line 581), the current Iceberg arm is:

```rust
ScanSource::IcebergDataFiles { files, .. } => {
    let file_predicates = scan_file_min_max_predicates(planned);
    let change_op_slot = planned_change_op_slot(planned);
    let mut ranges = Vec::new();
    for file in files
        .iter()
        .filter(|f| file_may_satisfy_min_max(f, &file_predicates))
    {
        ranges.extend(build_hdfs_scan_range_params_for_file(file, change_op_slot)?);
    }
    ranges
}
```

Change to:

```rust
ScanSource::IcebergDataFiles { .. } => {
    let file_predicates = scan_file_min_max_predicates(planned);
    let change_op_slot = planned_change_op_slot(planned);
    let planned_scan = resolved.planned_scan.as_ref().ok_or_else(|| {
        format!(
            "Iceberg scan {}.{} reached scan-range builder without planned connector scan",
            resolved.database, resolved.table.name
        )
    })?;
    let mut ranges = Vec::new();
    for split in &planned_scan.splits {
        let iceberg_split =
            crate::connector::iceberg::scan_planner::iceberg_split(split)?;
        let file = &iceberg_split.data_file;
        if !file_may_satisfy_min_max(file, &file_predicates) {
            continue;
        }
        ranges.extend(build_hdfs_scan_range_params_for_file(file, change_op_slot)?);
    }
    ranges
}
```

The remaining arms (`IcebergMetadataTable`, `IcebergDeltaTable`, the `StarRocks { .. } => unreachable!(...)` guard) are unchanged.

- [ ] **Step 3: Build**

```bash
cargo build
```

Expected: build succeeds. Warnings about `iceberg_table_info` reading `files` for `IcebergDataFiles` are acceptable if any appear; the helper is still used by `IcebergMetadataTable` / `IcebergDeltaTable` arms.

- [ ] **Step 4: Run the codegen unit tests**

```bash
cargo test --lib sql::codegen::nodes::tests sql::codegen::fragment_builder::tests
```

Expected: all tests pass. The `physical_change_op_column_does_not_emit_extended_columns` and `metadata_change_op_column_emits_extended_columns` tests in `nodes.rs` exercise `build_exec_params_multi` for IcebergDataFiles directly with hand-built `PlannedScanTable` fixtures — these will need `resolved.planned_scan` populated, because the new Iceberg arm reads from it.

If those nodes.rs tests fail with "Iceberg scan ... without planned connector scan", update each test's `PlannedScanTable.resolved` to set `planned_scan` to a `Some(PlannedConnectorScan { scan, splits })` constructed from the test fixture's `files` (use `IcebergConnectorScanPlanner::new()` directly to produce the splits; the planner is stateless so no registry is needed for these unit tests).

Concretely, in each test that currently does:

```rust
resolved: ResolvedTable {
    database: ...,
    table: TableDef { ... source: ScanSource::IcebergDataFiles { files: vec![...], ... } ... },
    planned_scan: None,
    alias: None,
},
```

change to:

```rust
let iceberg_files = vec![...];  // same list as in TableDef.source
let iceberg_table_info = test_iceberg_table_info();
let planner = crate::connector::iceberg::IcebergConnectorScanPlanner::new();
let table_handle = crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
    &iceberg_table_info.catalog,
    &iceberg_table_info.namespace,
    &iceberg_table_info.table,
    iceberg_table_info.current_snapshot_id,
    iceberg_table_info.clone(),
    iceberg_files.clone(),
);
let scan = planner.begin_scan(
    table_handle,
    crate::connector::scan_planning::BeginScanContext::default(),
).expect("begin_scan");
let splits = planner.plan_splits(
    &scan,
    crate::connector::scan_planning::SplitPlanningContext::default(),
).expect("plan_splits");
let planned = PlannedScanTable {
    scan_node_id: ...,
    resolved: ResolvedTable {
        database: ...,
        table: TableDef { ... source: ScanSource::IcebergDataFiles { files: iceberg_files, table: iceberg_table_info, cloud_properties: BTreeMap::new() } ... },
        planned_scan: Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits }),
        alias: None,
    },
    ...
};
```

This keeps the existing test behavior (same files, same assertions) while satisfying the new codegen contract.

- [ ] **Step 5: Run the full lib tests**

```bash
cargo test --lib
```

Expected: all tests pass. If a real production path (engine integration test, dictionary test, etc.) fails because it has an Iceberg scan and was not previously registering the planner, the fix is to extend `mock_starrocks_registry_for_engine_test` (Task 3 already did this) or to grant the affected test access to a registry with Iceberg registered.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs src/sql/codegen/nodes.rs
git commit -m "refactor(codegen): visit_scan obtains Iceberg files via connector registry"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 5: End-to-end guard that Iceberg `begin_scan` and `plan_splits` are invoked

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

Add a regression test (counterpart to `visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks`) that uses a counting wrapper around `IcebergConnectorScanPlanner` to assert `begin_scan` and `plan_splits` are each invoked once for an Iceberg scan.

- [ ] **Step 1: Add `CountingIcebergScanPlanner` wrapper**

In `src/sql/codegen/fragment_builder.rs::tests`, locate the existing `CountingScanPlanner` (the StarRocks counter; near the `ScanPlannerCallCounts` struct). Add a parallel Iceberg wrapper next to it:

```rust
#[derive(Debug)]
struct CountingIcebergScanPlanner {
    inner: crate::connector::iceberg::IcebergConnectorScanPlanner,
    counts: std::sync::Arc<ScanPlannerCallCounts>,
}

impl crate::connector::scan_planning::ConnectorScanPlanner for CountingIcebergScanPlanner {
    fn name(&self) -> &'static str {
        crate::connector::scan_planning::ConnectorScanPlanner::name(&self.inner)
    }

    fn begin_scan(
        &self,
        table: crate::connector::scan_planning::TableHandle,
        ctx: crate::connector::scan_planning::BeginScanContext,
    ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
        self.counts
            .begin_scan
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.begin_scan(table, ctx)
    }

    fn plan_splits(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        ctx: crate::connector::scan_planning::SplitPlanningContext,
    ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
        self.counts
            .plan_splits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.plan_splits(scan, ctx)
    }

    fn to_thrift_scan(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        splits: &[crate::connector::scan_planning::Split],
        ctx: crate::connector::scan_planning::ThriftScanContext,
    ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
        self.inner.to_thrift_scan(scan, splits, ctx)
    }
}
```

`ScanPlannerCallCounts` is the struct introduced by the previous slice's Task 6; reuse it.

- [ ] **Step 2: Add the test**

Place this test next to `visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks`:

```rust
#[test]
fn visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg() {
    let plan = iceberg_scan_plan();
    let catalog = DummyCatalog;

    let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
    let planner = std::sync::Arc::new(CountingIcebergScanPlanner {
        inner: crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
        counts: counts.clone(),
    });
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(planner);

    let _ = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
        .expect("build Iceberg fragment");

    assert_eq!(
        counts
            .begin_scan
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "begin_scan must be invoked exactly once for the Iceberg scan"
    );
    assert_eq!(
        counts
            .plan_splits
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "plan_splits must be invoked exactly once for the Iceberg scan"
    );
}
```

`iceberg_scan_plan()` is the existing helper at ~line 4341 (returns a `PhysicalPlanNode` over an `IcebergDataFiles` source with `files: vec![]`). The test asserts call counts; the empty `files` list is fine because we only care that the methods are invoked.

- [ ] **Step 3: Run the new test**

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg
```

Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "test(iceberg): assert visit_scan invokes connector begin_scan/plan_splits"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 6: Validation pass

**Files:**
- No source edits expected unless validation surfaces a real bug.

- [ ] **Step 1: Formatting**

```bash
cargo fmt --check
```

Expected: no diffs. If diffs appear, run `cargo fmt`, inspect with `git diff`, and only commit changes that touch files modified by this slice. Out-of-scope drift must be reverted with `git checkout --` before committing.

- [ ] **Step 2: Build**

```bash
cargo build
```

Expected: clean build. New warnings are acceptable; report them.

- [ ] **Step 3: Run focused connector and codegen tests**

```bash
cargo test --lib connector::iceberg::scan_planner connector::scan_planning_registry_tests sql::codegen::fragment_builder::tests sql::codegen::nodes::tests
```

Expected: all pass.

- [ ] **Step 4: Run dictionary lock-free regression**

```bash
cargo test --lib engine::dictionary::tests::dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node
```

Expected: passes.

- [ ] **Step 5: Run full lib tests**

```bash
cargo test --lib
```

Expected: all pass. If an unrelated pre-existing failure appears, record the failure name and rerun the focused tests from Steps 3-4. Do NOT attempt to fix unrelated failures in this task.

- [ ] **Step 6: Optional commit for validation-only fixes**

If validation required any small fixes (cargo fmt diff in-scope, a missed test fixture update, etc.):

```bash
git add <changed-files>
git commit -m "fix(iceberg): address connector scan planner validation"
```

If no fixes were needed, do not create an empty commit.

## Follow-on Plans

After this plan lands:

1. Migrate `IcebergMetadataTable` and `IcebergDeltaTable` through the connector (separate slice — those scan-source variants do not carry "files" and need different handle/split semantics).
2. Move `build_hdfs_scan_range_params_for_file` + `build_hdfs_scan_node` into `IcebergConnectorScanPlanner::to_thrift_scan` (requires extending `ThriftScanContext` to carry per-query state: `min_max_conjuncts`, `change_op_slot`, `iceberg_metadata_pseudo_column_slots`).
3. Stage 5 cleanup: delete `ScanSource::IcebergDataFiles.files` (after optimizer / planner / dictionary / explain stop reading it via Stage 4 capabilities work).
