# Connector-first StarRocks Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the connector-first scan-planning API and route standalone StarRocks scan-range generation through StarRocks connector-owned handles and splits.

**Architecture:** This is the first executable slice of `docs/superpowers/specs/2026-05-28-connector-first-standalone-scan-design.md`. It adds opaque connector scan handles/splits, implements a StarRocks connector adapter, and changes standalone codegen to build StarRocks `TInternalScanRange` values from connector splits. It intentionally keeps `PhysicalTableLayout` and `CatalogProvider::get_physical_layout` as the bootstrap source for this slice; subsequent plans will move StarRocks split planning earlier and remove the side-map after the adapter boundary is proven.

**Tech Stack:** Rust, existing `ConnectorRegistry`, existing standalone SQL planner/optimizer/codegen, existing StarRocks-compatible thrift `TLakeScanNode` / `TScanRangeParams`, `cargo test --lib`.

---

## Scope Check

The full spec spans several independent migrations: core connector API, StarRocks scan migration, Iceberg scan migration, optimizer capability routing, and old abstraction cleanup. This plan covers the first working slice:

- Create connector-first scan-planning API.
- Implement StarRocks table scan handles and splits.
- Route StarRocks standalone scan-range codegen through the adapter.
- Preserve FE-compatible thrift lowering unchanged.
- Preserve Iceberg and old side-map behavior until follow-up plans.

This keeps the branch reviewable and gives later plans a concrete adapter boundary to build on.

## File Structure

- Create: `src/connector/scan_planning.rs`
  - Defines `ConnectorId`, opaque `TableHandle` / `ScanHandle` / `Split`, `ConnectorScanPlanner`, `ThriftScanPlan`, and contexts.
  - Holds generic validation such as connector-id matching.
- Modify: `src/connector/mod.rs`
  - Exposes `scan_planning`.
  - Adds `scan_planners` registration and lookup to `ConnectorRegistry`.
  - Registers the StarRocks table scan planner in `register_standalone_backends`.
- Create: `src/connector/starrocks/table/scan_planner.rs`
  - Defines `StarRocksTableHandle`, `StarRocksScanHandle`, `StarRocksSplit`.
  - Implements StarRocks connector-owned conversion from `PhysicalTableLayout` to splits.
  - Implements StarRocks `to_thrift_scan`.
- Modify: `src/connector/starrocks/table/mod.rs`
  - Exposes the new scan planner module and re-exports its public crate-level types.
- Modify: `src/sql/codegen/resolve.rs`
  - Adds optional connector-planned scan state to `ResolvedTable` while preserving the existing `physical_layout` field for the bootstrap slice.
- Modify: `src/sql/codegen/fragment_builder.rs`
  - Builds StarRocks planned scan state through the StarRocks connector adapter during `visit_scan`.
  - Passes planned state into scan-range generation.
- Modify: `src/sql/codegen/nodes.rs`
  - Uses StarRocks connector `to_thrift_scan` for StarRocks ranges instead of directly iterating `PhysicalTableLayout`.
  - Keeps Iceberg paths unchanged.

## Task 1: Add connector scan-planning core types

**Files:**
- Create: `src/connector/scan_planning.rs`
- Modify: `src/connector/mod.rs`

- [ ] **Step 1: Write the connector-id mismatch test**

Create `src/connector/scan_planning.rs` with the initial test module and minimal imports:

```rust
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::{internal_service, plan_nodes};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct DummyScanHandle;
    impl ConnectorScanHandle for DummyScanHandle {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct DummySplit;
    impl ConnectorSplit for DummySplit {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn validate_splits_rejects_mismatched_connector_ids() {
        let scan = ScanHandle::new("starrocks", DummyScanHandle);
        let splits = vec![Split::new("iceberg", DummySplit)];

        let err = validate_split_connectors(&scan, &splits)
            .expect_err("mismatched split connector must fail");

        assert!(
            err.contains("split connector mismatch"),
            "unexpected error: {err}"
        );
    }
}
```

- [ ] **Step 2: Run the focused test and verify it fails to compile**

Run:

```bash
cargo test --lib connector::scan_planning::tests::validate_splits_rejects_mismatched_connector_ids
```

Expected: compile failure mentioning undefined `ConnectorScanHandle`, `ScanHandle`, `Split`, `ConnectorSplit`, or `validate_split_connectors`.

- [ ] **Step 3: Implement the core opaque handle types**

In `src/connector/scan_planning.rs`, add this code above the test module:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConnectorId(String);

impl ConnectorId {
    pub(crate) fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for ConnectorId {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

pub(crate) trait ConnectorTableHandle: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub(crate) trait ConnectorScanHandle: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub(crate) trait ConnectorSplit: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
pub(crate) struct TableHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorTableHandle>,
}

impl TableHandle {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorTableHandle + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorTableHandle + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScanHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorScanHandle>,
}

impl ScanHandle {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorScanHandle + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorScanHandle + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Split {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorSplit>,
}

impl Split {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorSplit + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorSplit + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

pub(crate) fn validate_split_connectors(
    scan: &ScanHandle,
    splits: &[Split],
) -> Result<(), String> {
    for split in splits {
        if split.connector_id() != scan.connector_id() {
            return Err(format!(
                "split connector mismatch: scan connector={} split connector={}",
                scan.connector_id().as_str(),
                split.connector_id().as_str()
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Add scan-planning contexts and planner trait**

Append these definitions after `validate_split_connectors`:

```rust
#[derive(Clone, Debug, Default)]
pub(crate) struct BeginScanContext;

#[derive(Clone, Debug, Default)]
pub(crate) struct SplitPlanningContext;

#[derive(Clone, Debug)]
pub(crate) struct ThriftScanContext {
    pub(crate) database: String,
    pub(crate) table: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ThriftScanPlan {
    pub(crate) node: Option<plan_nodes::TPlanNode>,
    pub(crate) scan_ranges: Vec<internal_service::TScanRangeParams>,
}

pub(crate) trait ConnectorScanPlanner: Send + Sync {
    fn name(&self) -> &'static str;

    fn begin_scan(
        &self,
        table: TableHandle,
        ctx: BeginScanContext,
    ) -> Result<ScanHandle, String>;

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String>;

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String>;
}
```

- [ ] **Step 5: Expose the new module**

In `src/connector/mod.rs`, add near the existing module declarations:

```rust
pub(crate) mod scan_planning;
```

- [ ] **Step 6: Run the focused test and verify it passes**

Run:

```bash
cargo test --lib connector::scan_planning::tests::validate_splits_rejects_mismatched_connector_ids
```

Expected: one passing test.

- [ ] **Step 7: Commit core scan-planning types**

```bash
git add src/connector/scan_planning.rs src/connector/mod.rs
git commit -m "feat(connector): add scan planning handle types"
```

## Task 2: Register connector scan planners

**Files:**
- Modify: `src/connector/mod.rs`

- [ ] **Step 1: Write registry tests**

Add this test module to `src/connector/mod.rs` near `backend_test`:

```rust
#[cfg(test)]
mod scan_planning_registry_tests {
    use std::sync::Arc;

    use super::scan_planning::{
        BeginScanContext, ConnectorScanPlanner, ScanHandle, Split, SplitPlanningContext,
        TableHandle, ThriftScanContext, ThriftScanPlan,
    };
    use super::ConnectorRegistry;

    #[derive(Debug)]
    struct NoopPlanner;

    impl ConnectorScanPlanner for NoopPlanner {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn begin_scan(
            &self,
            _table: TableHandle,
            _ctx: BeginScanContext,
        ) -> Result<ScanHandle, String> {
            Err("not used".to_string())
        }

        fn plan_splits(
            &self,
            _scan: &ScanHandle,
            _ctx: SplitPlanningContext,
        ) -> Result<Vec<Split>, String> {
            Err("not used".to_string())
        }

        fn to_thrift_scan(
            &self,
            _scan: &ScanHandle,
            _splits: &[Split],
            _ctx: ThriftScanContext,
        ) -> Result<ThriftScanPlan, String> {
            Err("not used".to_string())
        }
    }

    #[test]
    fn connector_registry_returns_registered_scan_planner() {
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(NoopPlanner));

        let planner = registry.scan_planner("noop").expect("registered planner");

        assert_eq!(planner.name(), "noop");
    }

    #[test]
    fn connector_registry_reports_unknown_scan_planner() {
        let registry = ConnectorRegistry::new();

        let err = registry
            .scan_planner("missing")
            .expect_err("unknown planner should fail");

        assert_eq!(err, "unknown scan planner: missing");
    }
}
```

- [ ] **Step 2: Run tests and verify they fail to compile**

Run:

```bash
cargo test --lib connector::scan_planning_registry_tests
```

Expected: compile failure because `ConnectorRegistry` has no `scan_planners`, `register_scan_planner`, or `scan_planner`.

- [ ] **Step 3: Add scan planner storage to `ConnectorRegistry`**

In `src/connector/mod.rs`, import the trait:

```rust
use scan_planning::ConnectorScanPlanner;
```

Add a field to `ConnectorRegistry`:

```rust
scan_planners: HashMap<&'static str, Arc<dyn ConnectorScanPlanner>>,
```

Initialize it in `ConnectorRegistry::new()`:

```rust
scan_planners: HashMap::new(),
```

Add methods next to the existing registry methods:

```rust
pub(crate) fn register_scan_planner(&mut self, planner: Arc<dyn ConnectorScanPlanner>) {
    self.scan_planners.insert(planner.name(), planner);
}

pub(crate) fn scan_planner(
    &self,
    name: &str,
) -> Result<Arc<dyn ConnectorScanPlanner>, String> {
    self.scan_planners
        .get(name)
        .cloned()
        .ok_or_else(|| format!("unknown scan planner: {name}"))
}
```

Update the `Debug` impl to list scan planners:

```rust
let mut scan_planners: Vec<_> = self.scan_planners.keys().copied().collect();
scan_planners.sort();
```

and include `.field("scan_planners", &scan_planners)` in the debug struct.

- [ ] **Step 4: Run registry tests**

Run:

```bash
cargo test --lib connector::scan_planning_registry_tests
```

Expected: both tests pass.

- [ ] **Step 5: Commit registry support**

```bash
git add src/connector/mod.rs
git commit -m "feat(connector): register scan planners"
```

## Task 3: Implement StarRocks connector scan planner

**Files:**
- Create: `src/connector/starrocks/table/scan_planner.rs`
- Modify: `src/connector/starrocks/table/mod.rs`
- Modify: `src/connector/starrocks/table/catalog.rs`

- [ ] **Step 1: Make layout construction callable by the StarRocks planner**

In `src/connector/starrocks/table/catalog.rs`, change:

```rust
fn starrocks_table_physical_layout(
```

to:

```rust
pub(crate) fn starrocks_table_physical_layout(
```

This keeps the existing layout construction logic as the bootstrap source for StarRocks splits in this plan.

- [ ] **Step 2: Add StarRocks planner tests**

Create `src/connector/starrocks/table/scan_planner.rs` with this initial test-focused content:

```rust
use std::any::Any;

use crate::connector::scan_planning::{ConnectorScanHandle, ConnectorSplit, ScanHandle, Split};

const CONNECTOR_ID: &str = "starrocks";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksTableHandle {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksSplit {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

impl ConnectorSplit for StarRocksSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksScanHandle {
    pub(crate) table: StarRocksTableHandle,
    pub(crate) schema_id: i64,
}

impl ConnectorScanHandle for StarRocksScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn starrocks_scan_handle(scan: &ScanHandle) -> Result<&StarRocksScanHandle, String> {
    scan.downcast_ref::<StarRocksScanHandle>()
        .ok_or_else(|| "expected StarRocksScanHandle for starrocks scan".to_string())
}

pub(crate) fn starrocks_split(split: &Split) -> Result<&StarRocksSplit, String> {
    split
        .downcast_ref::<StarRocksSplit>()
        .ok_or_else(|| "expected StarRocksSplit for starrocks split".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::scan_planning::{validate_split_connectors, ScanHandle, Split};

    #[test]
    fn downcasts_starrocks_scan_and_split() {
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(starrocks_scan_handle(&scan).expect("scan").schema_id, 30);
        assert_eq!(starrocks_split(&splits[0]).expect("split").tablet_id, 300);
    }
}
```

- [ ] **Step 3: Expose the module**

In `src/connector/starrocks/table/mod.rs`, add:

```rust
pub(crate) mod scan_planner;
```

and re-export the public crate-level types:

```rust
pub(crate) use scan_planner::{
    StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle, StarRocksTableScanPlanner,
};
```

The re-export of `StarRocksTableScanPlanner` will fail until the next step defines it.

- [ ] **Step 4: Run the focused test and verify it fails for the missing planner type**

Run:

```bash
cargo test --lib connector::starrocks::table::scan_planner::tests::downcasts_starrocks_scan_and_split
```

Expected: compile failure mentioning `StarRocksTableScanPlanner` is missing.

- [ ] **Step 5: Implement `StarRocksTableScanPlanner` and thrift conversion**

Append this implementation to `src/connector/starrocks/table/scan_planner.rs`:

```rust
use std::sync::{Arc, Weak};

use crate::connector::scan_planning::{
    validate_split_connectors, BeginScanContext, ConnectorScanPlanner, SplitPlanningContext,
    TableHandle, ThriftScanContext, ThriftScanPlan,
};
use crate::engine::StandaloneState;
use crate::{internal_service, plan_nodes};

pub(crate) struct StarRocksTableScanPlanner {
    state: Weak<StandaloneState>,
}

impl StarRocksTableScanPlanner {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }

    pub(crate) fn table_handle_from_source(
        database: &str,
        table: &str,
        db_id: i64,
        table_id: i64,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            StarRocksTableHandle {
                database: database.to_string(),
                table: table.to_string(),
                db_id,
                table_id,
            },
        )
    }

    fn build_internal_scan_range_params(
        database: &str,
        table: &str,
        schema_id: i64,
        split: &StarRocksSplit,
    ) -> internal_service::TScanRangeParams {
        let internal_scan_range = plan_nodes::TInternalScanRange::new(
            vec![],
            schema_id.to_string(),
            split.version.to_string(),
            split.version.to_string(),
            split.tablet_id,
            database.to_string(),
            None::<Vec<plan_nodes::TKeyRange>>,
            None::<String>,
            Some(table.to_string()),
            Some(split.partition_id),
            None::<i64>,
            Some(true),
            None::<i32>,
            Some(false),
            Some(false),
            None::<i64>,
        );

        internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                Some(internal_scan_range),
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                None::<plan_nodes::THdfsScanRange>,
                None::<plan_nodes::TBinlogScanRange>,
                None::<plan_nodes::TBenchmarkScanRange>,
            ),
            None::<i32>,
            Some(false),
            Some(false),
        )
    }
}

impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(
        &self,
        table: TableHandle,
        _ctx: BeginScanContext,
    ) -> Result<ScanHandle, String> {
        let table = table
            .downcast_ref::<StarRocksTableHandle>()
            .ok_or_else(|| "expected StarRocksTableHandle for starrocks scan".to_string())?
            .clone();
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&table.database, &table.table)?;
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table,
                schema_id: runtime.table.current_schema_id,
            },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = starrocks_scan_handle(scan)?;
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&scan.table.database, &scan.table.table)?;
        let layout = super::catalog::starrocks_table_physical_layout(runtime)?;
        Ok(layout
            .tablets
            .into_iter()
            .map(|tablet| {
                Split::new(
                    CONNECTOR_ID,
                    StarRocksSplit {
                        tablet_id: tablet.tablet_id,
                        partition_id: tablet.partition_id,
                        version: tablet.version,
                    },
                )
            })
            .collect())
    }

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String> {
        validate_split_connectors(scan, splits)?;
        let scan = starrocks_scan_handle(scan)?;
        let scan_ranges = splits
            .iter()
            .map(|split| {
                let split = starrocks_split(split)?;
                Ok(Self::build_internal_scan_range_params(
                    &ctx.database,
                    &ctx.table,
                    scan.schema_id,
                    split,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ThriftScanPlan {
            node: None,
            scan_ranges,
        })
    }
}
```

- [ ] **Step 6: Run focused StarRocks planner test**

Run:

```bash
cargo test --lib connector::starrocks::table::scan_planner::tests::downcasts_starrocks_scan_and_split
```

Expected: test passes.

- [ ] **Step 7: Commit StarRocks planner types**

```bash
git add src/connector/starrocks/table/scan_planner.rs src/connector/starrocks/table/mod.rs src/connector/starrocks/table/catalog.rs
git commit -m "feat(starrocks): add connector scan planner"
```

## Task 4: Register the StarRocks scan planner in standalone backends

**Files:**
- Modify: `src/connector/mod.rs`

- [ ] **Step 1: Register the planner**

In `register_standalone_backends`, after registering the StarRocks table source/sink, add:

```rust
connectors.register_scan_planner(Arc::new(
    starrocks::table::StarRocksTableScanPlanner::new(state),
));
```

- [ ] **Step 2: Add a registration test**

In `src/connector/mod.rs`, add this test to `scan_planning_registry_tests`:

```rust
#[test]
fn default_registry_does_not_register_standalone_scan_planners() {
    let registry = ConnectorRegistry::default();

    let err = registry
        .scan_planner("starrocks")
        .expect_err("standalone planners are registered with state, not Default");

    assert_eq!(err, "unknown scan planner: starrocks");
}
```

This documents that `ConnectorRegistry::default()` is runtime scan-connector setup only; standalone planners are state-bound and registered by `register_standalone_backends`.

- [ ] **Step 3: Run registry tests**

Run:

```bash
cargo test --lib connector::scan_planning_registry_tests
```

Expected: all registry tests pass.

- [ ] **Step 4: Commit standalone planner registration**

```bash
git add src/connector/mod.rs
git commit -m "feat(connector): register standalone StarRocks scan planner"
```

## Task 5: Thread planned StarRocks scan state through codegen

**Files:**
- Modify: `src/sql/codegen/resolve.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add planned scan state to `ResolvedTable`**

In `src/sql/codegen/resolve.rs`, add the import:

```rust
use crate::connector::scan_planning::{ScanHandle, Split};
```

Add the struct:

```rust
#[derive(Clone, Debug)]
pub(crate) struct PlannedConnectorScan {
    pub(crate) scan: ScanHandle,
    pub(crate) splits: Vec<Split>,
}
```

Add a field to `ResolvedTable`:

```rust
pub planned_scan: Option<PlannedConnectorScan>,
```

Keep `physical_layout` for Iceberg/StarRocks bridge compatibility during this plan.

- [ ] **Step 2: Compile to enumerate missing initializers**

Run:

```bash
cargo build 2>&1 | rg "missing field `planned_scan`|ResolvedTable"
```

Expected: compile errors for `ResolvedTable` initializers that need `planned_scan: None`.

- [ ] **Step 3: Update non-StarRocks `ResolvedTable` initializers**

For every `ResolvedTable { ... }` initializer outside the StarRocks scan path, add:

```rust
planned_scan: None,
```

Known files from current tree:

- `src/sql/codegen/fragment_builder.rs`
- `src/sql/codegen/nodes.rs`

- [ ] **Step 4: Add helper to plan StarRocks scans in fragment builder**

In `src/sql/codegen/fragment_builder.rs`, add imports:

```rust
use crate::connector::scan_planning::{
    BeginScanContext, SplitPlanningContext, TableHandle,
};
use crate::connector::starrocks::table::StarRocksTableScanPlanner;
```

Add this helper near `iceberg_table_info`:

```rust
fn plan_starrocks_connector_scan(
    database: &str,
    table: &crate::sql::catalog::TableDef,
    physical_layout: &crate::sql::catalog::PhysicalTableLayout,
) -> Result<crate::sql::codegen::resolve::PlannedConnectorScan, String> {
    let crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } = &table.source else {
        return Err(format!(
            "expected StarRocks ScanSource while planning connector scan for {}.{}",
            database, table.name
        ));
    };
    if *db_id != physical_layout.db_id || *table_id != physical_layout.table_id {
        return Err(format!(
            "StarRocks scan identity mismatch for {}.{}: source=(db_id={}, table_id={}) layout=(db_id={}, table_id={})",
            database,
            table.name,
            db_id,
            table_id,
            physical_layout.db_id,
            physical_layout.table_id
        ));
    }

    let table_handle = StarRocksTableScanPlanner::table_handle_from_source(
        database,
        &table.name,
        *db_id,
        *table_id,
    );
    let scan = crate::connector::scan_planning::ScanHandle::new(
        "starrocks",
        crate::connector::starrocks::table::StarRocksScanHandle {
            table: table_handle
                .downcast_ref::<crate::connector::starrocks::table::StarRocksTableHandle>()
                .expect("table_handle_from_source returns StarRocksTableHandle")
                .clone(),
            schema_id: physical_layout.schema_id,
        },
    );
    let splits = physical_layout
        .tablets
        .iter()
        .map(|tablet| {
            crate::connector::scan_planning::Split::new(
                "starrocks",
                crate::connector::starrocks::table::StarRocksSplit {
                    tablet_id: tablet.tablet_id,
                    partition_id: tablet.partition_id,
                    version: tablet.version,
                },
            )
        })
        .collect();

    Ok(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
}
```

This helper is a bootstrap adapter. It uses the existing query-snapshot `PhysicalTableLayout` to create connector-owned StarRocks scan state without introducing a new state lock in codegen.

- [ ] **Step 5: Attach planned scan state in `visit_scan`**

In `visit_scan`, after `physical_layout` is loaded and before constructing `ResolvedTable`, compute:

```rust
let planned_scan = match (&op.table.source, physical_layout.as_ref()) {
    (crate::sql::catalog::ScanSource::StarRocks { .. }, Some(layout)) => {
        Some(plan_starrocks_connector_scan(&op.database, &op.table, layout)?)
    }
    _ => None,
};
```

When building `ResolvedTable`, set:

```rust
planned_scan,
```

- [ ] **Step 6: Run compile check**

Run:

```bash
cargo build
```

Expected: build succeeds or only fails in scan-range generation where `planned_scan` is not yet used.

- [ ] **Step 7: Commit planned scan threading**

```bash
git add src/sql/codegen/resolve.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/nodes.rs
git commit -m "refactor(codegen): thread planned connector scans"
```

## Task 6: Generate StarRocks scan ranges through the connector adapter

**Files:**
- Modify: `src/sql/codegen/nodes.rs`

- [ ] **Step 1: Add a failing unit test for StarRocks connector range generation**

In the existing `#[cfg(test)] mod tests` in `src/sql/codegen/nodes.rs`, add a test that creates a `ResolvedTable` with `planned_scan` and no `physical_layout`:

```rust
#[test]
fn starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout() {
    use crate::connector::scan_planning::{ScanHandle, Split};
    use crate::connector::starrocks::table::{
        StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::resolve::{PlannedConnectorScan, ResolvedTable};
    use arrow::datatypes::DataType;

    let table = TableDef {
        name: "orders".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }],
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks {
            db_id: 10,
            table_id: 20,
        },
    };
    let planned_scan = PlannedConnectorScan {
        scan: ScanHandle::new(
            "starrocks",
            StarRocksScanHandle {
                table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
            },
        ),
        splits: vec![Split::new(
            "starrocks",
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )],
    };
    let resolved = ResolvedTable {
        database: "default".to_string(),
        table,
        physical_layout: None,
        planned_scan: Some(planned_scan),
        alias: None,
    };

    let ranges = super::build_starrocks_scan_ranges_from_planned_scan(&resolved)
        .expect("planned scan ranges");

    assert_eq!(ranges.len(), 1);
    let internal = ranges[0]
        .scan_range
        .as_ref()
        .and_then(|range| range.internal_scan_range.as_ref())
        .expect("internal scan range");
    assert_eq!(internal.tablet_id, 300);
    assert_eq!(internal.partition_id, Some(100));
    assert_eq!(internal.version.as_deref(), Some("7"));
    assert_eq!(internal.schema_hash.as_deref(), Some("30"));
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test --lib sql::codegen::nodes::tests::starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout
```

Expected: compile failure for missing `build_starrocks_scan_ranges_from_planned_scan`.

- [ ] **Step 3: Implement planned StarRocks range generation**

In `src/sql/codegen/nodes.rs`, add imports:

```rust
use crate::connector::scan_planning::{
    ConnectorScanPlanner, ThriftScanContext,
};
use crate::connector::starrocks::table::StarRocksTableScanPlanner;
```

Add this helper near `build_internal_scan_range_params`:

```rust
pub(crate) fn build_starrocks_scan_ranges_from_planned_scan(
    resolved: &ResolvedTable,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    let planned = resolved
        .planned_scan
        .as_ref()
        .ok_or_else(|| {
            format!(
                "StarRocks table {}.{} reached scan-range builder without planned connector scan",
                resolved.database, resolved.table.name
            )
        })?;
    let planner = StarRocksTableScanPlanner::stateless_for_codegen();
    let thrift = planner.to_thrift_scan(
        &planned.scan,
        &planned.splits,
        ThriftScanContext {
            database: resolved.database.clone(),
            table: resolved.table.name.clone(),
        },
    )?;
    Ok(thrift.scan_ranges)
}
```

Add this associated constructor to `StarRocksTableScanPlanner` in `scan_planner.rs`:

```rust
pub(crate) fn stateless_for_codegen() -> Self {
    Self { state: Weak::new() }
}
```

`to_thrift_scan` does not need state, so this keeps codegen independent from `StandaloneState`.

- [ ] **Step 4: Switch StarRocks branch in scan-range builder**

In `build_exec_params_multi` or the local helper that currently checks `resolved.physical_layout`, change the StarRocks branch so it prefers planned connector scan:

```rust
if matches!(
    resolved.table.source,
    crate::sql::catalog::ScanSource::StarRocks { .. }
) {
    let ranges = build_starrocks_scan_ranges_from_planned_scan(resolved)?;
    if ranges.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no selected tablet splits",
            resolved.database, resolved.table.name
        ));
    }
    ranges
} else if let Some(layout) = resolved.physical_layout.as_ref() {
    ...
}
```

Keep the old `physical_layout` path temporarily for non-StarRocks callers until follow-up cleanup removes it.

- [ ] **Step 5: Run focused test**

Run:

```bash
cargo test --lib sql::codegen::nodes::tests::starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout
```

Expected: test passes.

- [ ] **Step 6: Run existing StarRocks codegen dictionary tests**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::scan_emits_single_slot_per_dict_column sql::codegen::fragment_builder::tests::starrocks_scan_requires_lake_dict_mapping
```

Expected: both tests pass. If the exact test names differ after rebasing, run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::scan_emits_single_slot_per_dict_column
cargo test --lib sql::codegen::fragment_builder::tests::starrocks_scan_requires_lake_dict_mapping
```

- [ ] **Step 7: Commit StarRocks range generation switch**

```bash
git add src/sql/codegen/nodes.rs src/connector/starrocks/table/scan_planner.rs
git commit -m "refactor(codegen): build StarRocks scan ranges from connector splits"
```

## Task 7: Add an end-to-end guard for StarRocks planned scan without side-map codegen

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add a test that fails if StarRocks codegen ignores planned scans**

In `src/sql/codegen/fragment_builder.rs`, add a test beside existing StarRocks scan tests:

```rust
#[test]
fn starrocks_fragment_exec_params_are_generated_from_planned_connector_scan() {
    let layout = starrocks_layout();
    let plan = starrocks_scan_plan();
    let catalog = StarRocksCatalog { layout };

    let build = PlanFragmentBuilder::build(&plan, &catalog, "default")
        .expect("build StarRocks fragment");
    let root = build
        .fragment_results
        .iter()
        .find(|fragment| fragment.fragment_id == build.root_fragment_id)
        .expect("root fragment");
    let exec_params = &root.exec_params;
    let per_node = exec_params
        .per_node_scan_ranges
        .as_ref()
        .expect("scan ranges");
    let ranges = per_node
        .values()
        .next()
        .expect("one scan node should have ranges");

    assert_eq!(ranges.len(), 2);
    let tablet_ids = ranges
        .iter()
        .map(|range| {
            range
                .scan_range
                .as_ref()
                .and_then(|scan_range| scan_range.internal_scan_range.as_ref())
                .map(|internal| internal.tablet_id)
                .expect("internal scan range")
        })
        .collect::<Vec<_>>();
    assert_eq!(tablet_ids, vec![101, 102]);
}
```

Use the existing `starrocks_layout()` helper values. If that helper uses different tablet ids, update the final assertion to the exact helper values.

- [ ] **Step 2: Run the test**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::starrocks_fragment_exec_params_are_generated_from_planned_connector_scan
```

Expected: test passes once Tasks 5 and 6 are complete.

- [ ] **Step 3: Commit end-to-end guard**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "test(codegen): cover StarRocks connector-planned scan ranges"
```

## Task 8: Validation pass

**Files:**
- No source edits expected unless validation exposes a bug.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: no formatting diffs. If it fails, run `cargo fmt`, inspect the diff, and commit formatting with the relevant task commit if the formatter only touched files changed by this plan.

- [ ] **Step 2: Run build**

Run:

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 3: Run focused unit tests**

Run:

```bash
cargo test --lib connector::scan_planning connector::scan_planning_registry_tests connector::starrocks::table::scan_planner
```

Expected: all focused connector tests pass.

- [ ] **Step 4: Run StarRocks codegen tests**

Run:

```bash
cargo test --lib sql::codegen::nodes::tests::starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout sql::codegen::fragment_builder::tests::starrocks_fragment_exec_params_are_generated_from_planned_connector_scan
```

Expected: both tests pass.

- [ ] **Step 5: Run dictionary lock-free regression**

Run:

```bash
cargo test --lib engine::dictionary::tests::dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node
```

Expected: test passes.

- [ ] **Step 6: Run full lib tests**

Run:

```bash
cargo test --lib
```

Expected: all lib tests pass. If an unrelated pre-existing failure appears, record the exact failure and rerun the focused tests from Steps 3-5.

- [ ] **Step 7: Commit validation-only fixes if needed**

If validation required small fixes, commit only those fixes:

```bash
git add <changed-files>
git commit -m "fix(connector): address StarRocks scan planning validation"
```

If validation required no fixes, do not create an empty commit.

## Follow-on Plans

After this plan lands, write separate implementation plans for:

1. Moving StarRocks split planning earlier so it no longer bootstraps from `PhysicalTableLayout`.
2. Removing `InMemoryCatalog.physical_layouts` and `CatalogProvider::get_physical_layout`.
3. Migrating Iceberg from `ScanSource::IcebergDataFiles` to connector-owned file splits.
4. Routing optimizer projection/predicate/statistics/dictionary/explain through connector capabilities.
