# Connector Metadata P3/P4 CatalogMgr Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut standalone Iceberg schema resolution over to `CatalogMgr`/`SchemaCache`, remove the normal query-time global Iceberg table drop/register path, and clean up the remaining registry boundary debt from P1/P2.

**Architecture:** P3 wires `CatalogMgr` into the analyzer through a catalog-aware `CatalogProvider` adapter while keeping scan-binding in codegen. Ordinary Iceberg SELECT analysis reads schema through `CatalogMgr` and never mutates the global `InMemoryCatalog`; time-travel and refresh-only synthetic sources remain explicit, plan-local exceptions. P4 then removes the obsolete query-prep registration surface, registers/deregisters Iceberg catalogs through `CatalogMgr`, wires cache invalidation on write/DDL paths, and deletes dead code.

**Tech Stack:** Rust, existing `CatalogProvider`, `CatalogMgr`, `SchemaCache`, `CatalogBackend`/`TableSource`, `IcebergCatalogRegistry`, standalone SQL analyzer/planner/codegen.

---

## Non-Goals

- Do not change `src/lower/**`; FE-compatible BE lowering still consumes thrift plan/ranges only.
- Do not move Iceberg data-file planning back into analyzer or `CatalogMgr`.
- Do not remove synthetic time-travel table registration in this PR; `FOR VERSION AS OF` still needs a local synthetic `TableDef` until the AST/IR can carry versioned scan handles directly.
- Do not redesign MV refresh scan sources; `IcebergVersionTable`, `IcebergDeltaTable`, and `IcebergMvTargetState` stay explicit-file or refresh-context driven.

## File Structure

- Modify `src/sql/catalog.rs`
  - Add catalog-aware lookup methods and `TableLookupMode`.
- Modify `src/sql/analyzer/resolve_from.rs`
  - Preserve catalog overrides from 3-part names and request special lookup mode for Iceberg metadata tables.
- Modify `src/sql/analyzer/mod.rs`
  - Accept `&dyn CatalogProvider` cleanly after the trait grows catalog-aware methods.
- Modify `src/engine/catalog_mgr/metadata.rs`
  - Add `TableMetadata::to_table_def`.
- Modify `src/engine/catalog_mgr/catalog.rs`
  - Add optional `invalidate_table`.
- Modify `src/engine/catalog_mgr/iceberg.rs`
  - Use schema-only table defs, real schema-id probes, and cache invalidation.
- Modify `src/connector/backend.rs`
  - Add `CatalogBackend::current_schema_id` default hook.
- Modify `src/connector/iceberg/catalog/backend.rs`
  - Implement `current_schema_id` for Iceberg.
- Create `src/engine/catalog_mgr/provider.rs`
  - Implement `CatalogMgrProvider` as the analyzer-facing `CatalogProvider`.
- Modify `src/engine/catalog_mgr/mod.rs`
  - Export provider, cloneable manager, unregister/invalidate helpers.
- Modify `src/engine/mod.rs`
  - Wire `StandaloneState.catalog` as `Arc<RwLock<InMemoryCatalog>>`, add `catalog_mgr`, route SELECT/EXPLAIN analysis through `CatalogMgrProvider`, and drop normal Iceberg pre-registration.
- Modify `src/connector/mod.rs`
  - Register the internal catalog and Iceberg `CatalogMgr` entries alongside connector backends.
- Modify write/DDL paths that mutate Iceberg tables:
  - `src/engine/iceberg_writer.rs`
  - `src/engine/mutation_flow.rs`
  - `src/engine/delete_flow.rs`
  - `src/engine/equality_delete_flow.rs`
  - `src/engine/iceberg_truncate.rs`
  - `src/engine/statement.rs`
  - `src/engine/iceberg_ctas.rs`
  - `src/connector/iceberg/catalog/schema_update.rs`
- Modify `src/engine/query_prep.rs`
  - Delete normal external-table registration helpers; keep only synthetic table builders/time-travel helpers.

---

## Task 1: Make Analyzer Catalog Lookup Catalog-Aware

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/sql/analyzer/mod.rs`
- Modify: `src/sql/analyzer/resolve_from.rs`

- [ ] **Step 1: Write failing analyzer tests**

In `src/sql/analyzer/mod.rs` tests, add a catalog-aware test catalog and tests near the existing metadata-table tests:

```rust
struct CatalogAwareTestCatalog;

impl crate::sql::catalog::CatalogProvider for CatalogAwareTestCatalog {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get_table_in_catalog(None, database, table)
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        let catalog_name = catalog.unwrap_or("default_catalog");
        Ok(TableDef {
            name: format!("{catalog_name}_{database}_{table}"),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: if catalog == Some("ice") { 100 } else { 1 },
                table_id: 2,
            },
        })
    }
}

#[test]
fn analyzer_passes_three_part_catalog_to_catalog_provider() {
    let stmt = crate::sql::parser::parse_sql_raw("SELECT id FROM ice.db.orders")
        .expect("parse");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query");
    };

    let (resolved, _, _) =
        analyze(&query, &CatalogAwareTestCatalog, "default").expect("analyze");

    let QueryBody::Select(select) = resolved.body else {
        panic!("expected select");
    };
    let Some(Relation::Scan(scan)) = select.from else {
        panic!("expected scan");
    };
    assert_eq!(scan.database, "db");
    assert_eq!(scan.table.name, "ice_db_orders");
}

#[test]
fn analyzer_uses_metadata_lookup_mode_for_partitions_table() {
    struct MetadataModeCatalog(std::cell::Cell<bool>);
    impl crate::sql::catalog::CatalogProvider for MetadataModeCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            self.get_table_with_mode(None, database, table, TableLookupMode::SchemaOnly)
        }

        fn get_table_with_mode(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
            mode: TableLookupMode,
        ) -> Result<TableDef, String> {
            self.0.set(matches!(
                mode,
                TableLookupMode::IcebergMetadata {
                    metadata_table_type: IcebergMetadataTableType::Partitions,
                }
            ));
            CatalogAwareTestCatalog.get_table_in_catalog(catalog, database, table)
        }
    }

    let catalog = MetadataModeCatalog(std::cell::Cell::new(false));
    let stmt = crate::sql::parser::parse_sql_raw(
        "SELECT record_count FROM ice.db.orders$partitions",
    )
    .expect("parse");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query");
    };

    let _ = analyze(&query, &catalog, "default").expect("analyze");
    assert!(catalog.0.get(), "partitions metadata lookup mode was not requested");
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib sql::analyzer::tests::analyzer_passes_three_part_catalog_to_catalog_provider sql::analyzer::tests::analyzer_uses_metadata_lookup_mode_for_partitions_table
```

Expected: compile failure because `TableLookupMode` and catalog-aware trait methods do not exist.

- [ ] **Step 3: Extend `CatalogProvider`**

In `src/sql/catalog.rs`, add below `TableDef`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableLookupMode {
    SchemaOnly,
    IcebergMetadata {
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    },
    ExplainStats,
}
```

Then extend `CatalogProvider`:

```rust
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        let _ = catalog;
        self.get_table(database, table)
    }

    fn get_table_with_mode(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        mode: TableLookupMode,
    ) -> Result<TableDef, String> {
        let _ = mode;
        self.get_table_in_catalog(catalog, database, table)
    }

    // existing default methods stay below
}
```

- [ ] **Step 4: Change analyzer entry to use trait object cleanly**

In `src/sql/analyzer/mod.rs`, change the public analyzer signature:

```rust
pub(crate) fn analyze(
    query: &sqlast::Query,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<
    (
        ResolvedQuery,
        crate::sql::analysis::cte::CTERegistry,
        crate::sql::column_id::ColumnRefFactory,
    ),
    String,
> {
```

Keep `AnalyzerContext.catalog` as:

```rust
pub(super) catalog: &'a dyn CatalogProvider,
```

- [ ] **Step 5: Preserve catalog override in `resolve_from`**

In `src/sql/analyzer/resolve_from.rs`, replace both metadata and base-table lookup extraction blocks with catalog-aware forms.

For metadata table lookup:

```rust
let (catalog_override, db_lower, tbl_lower) = match base_parts.as_slice() {
    [tbl] => (None, self.current_database.to_lowercase(), tbl.to_lowercase()),
    [db, tbl] => (None, db.to_lowercase(), tbl.to_lowercase()),
    [cat, db, tbl] => (Some(cat.as_str()), db.to_lowercase(), tbl.to_lowercase()),
    _ => {
        return Err(format!(
            "iceberg metadata table requires <tbl> | <db>.<tbl> | <cat>.<db>.<tbl>, got: {parts:?}"
        ));
    }
};

let table_def = self.catalog.get_table_with_mode(
    catalog_override,
    &db_lower,
    &tbl_lower,
    crate::sql::catalog::TableLookupMode::IcebergMetadata {
        metadata_table_type: metadata_ty.clone(),
    },
)?;
```

For ordinary base table lookup:

```rust
let (catalog_override, db, tbl) = match parts.len() {
    1 => (None, self.current_database.to_string(), parts[0].clone()),
    2 => (None, parts[0].clone(), parts[1].clone()),
    3 => (Some(parts[0].as_str()), parts[1].clone(), parts[2].clone()),
    _ => return Err(format!("unsupported table name: {name}")),
};
let db_lower = db.to_lowercase();
let tbl_lower = tbl.to_lowercase();
let table_def = self
    .catalog
    .get_table_in_catalog(catalog_override, &db_lower, &tbl_lower)?;
```

- [ ] **Step 6: Run targeted analyzer tests**

Run:

```bash
cargo test --lib sql::analyzer::tests::analyzer_passes_three_part_catalog_to_catalog_provider sql::analyzer::tests::analyzer_uses_metadata_lookup_mode_for_partitions_table
cargo test --lib sql::analyzer::tests::analyzer_resolves_t_dollar_snapshots_to_metadata_scan
```

Expected: all targeted analyzer tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/catalog.rs src/sql/analyzer/mod.rs src/sql/analyzer/resolve_from.rs
git commit -m "feat(analyzer): preserve catalog-aware table lookup"
```

---

## Task 2: Add TableMetadata Conversion and CatalogMgrProvider

**Files:**
- Modify: `src/engine/catalog_mgr/metadata.rs`
- Create: `src/engine/catalog_mgr/provider.rs`
- Modify: `src/engine/catalog_mgr/mod.rs`

- [ ] **Step 1: Write failing metadata conversion tests**

In `src/engine/catalog_mgr/metadata.rs` tests, add:

```rust
#[test]
fn table_metadata_to_table_def_rebuilds_schema_only_iceberg_source() {
    let id = TableIdentity::new("ice", "ns", "orders");
    let meta = TableMetadata {
        identity: id,
        columns: vec![col("id")],
        iceberg_row_lineage_columns: vec![col("_row_id")],
        binding: TableBinding::Iceberg {
            info: iceberg_info(),
        },
    };

    let table_def = meta.to_table_def();

    assert_eq!(table_def.name, "orders");
    assert_eq!(table_def.columns.len(), 1);
    assert_eq!(table_def.iceberg_row_lineage_metadata_columns.len(), 1);
    let ScanSource::IcebergDataFiles { files, binding, .. } = table_def.source else {
        panic!("expected iceberg source");
    };
    assert!(files.is_empty());
    assert_eq!(binding, IcebergDataFileBinding::CurrentSnapshot);
}
```

- [ ] **Step 2: Implement `TableMetadata::to_table_def`**

In `src/engine/catalog_mgr/metadata.rs`, add:

```rust
impl TableMetadata {
    pub(crate) fn to_table_def(&self) -> TableDef {
        let source = match &self.binding {
            TableBinding::Internal { db_id, table_id } => ScanSource::StarRocks {
                db_id: *db_id,
                table_id: *table_id,
            },
            TableBinding::Iceberg { info } => ScanSource::IcebergDataFiles {
                table: info.clone(),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        TableDef {
            name: self.identity.table.clone(),
            columns: self.columns.clone(),
            iceberg_row_lineage_metadata_columns: self.iceberg_row_lineage_columns.clone(),
            source,
        }
    }
}
```

- [ ] **Step 3: Write failing provider tests**

Create `src/engine/catalog_mgr/provider.rs` with test skeleton:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef, TableLookupMode,
    };
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    struct FixedIceCatalog;
    impl Catalog for FixedIceCatalog {
        fn name(&self) -> &str {
            "ice"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            Ok(TableMetadata {
                identity: TableIdentity::new("ice", namespace, table),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Iceberg {
                    info: iceberg_info(),
                },
            })
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 3,
            location: "s3://warehouse/db/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    #[test]
    fn provider_resolves_current_catalog_without_mutating_local_catalog() {
        let local = InMemoryCatalog::default();
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(FixedIceCatalog));
        let connectors = crate::connector::ConnectorRegistry::default();
        let provider =
            CatalogMgrProvider::new(Some("ice"), &local, &mgr, &connectors, TableLookupMode::SchemaOnly);

        let table = provider.get_table("db", "orders").expect("resolve");

        assert_eq!(table.name, "orders");
        assert!(matches!(table.source, ScanSource::IcebergDataFiles { .. }));
        assert!(local.get("db", "orders").is_err());
    }
}
```

Expected: compile failure because `CatalogMgrProvider` does not exist and `iceberg_info` is private.

- [ ] **Step 4: Implement `CatalogMgrProvider`**

In `src/engine/catalog_mgr/provider.rs`, add:

```rust
//! Analyzer-facing adapter over CatalogMgr plus the local InMemoryCatalog.

use crate::connector::ConnectorRegistry;
use crate::engine::catalog::InMemoryCatalog;
use crate::engine::catalog_mgr::CatalogMgr;
use crate::sql::catalog::{CatalogProvider, TableDef, TableLookupMode};

pub(crate) struct CatalogMgrProvider<'a> {
    current_catalog: Option<&'a str>,
    local: &'a InMemoryCatalog,
    catalog_mgr: &'a CatalogMgr,
    connectors: &'a ConnectorRegistry,
    default_mode: TableLookupMode,
}

impl<'a> CatalogMgrProvider<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        local: &'a InMemoryCatalog,
        catalog_mgr: &'a CatalogMgr,
        connectors: &'a ConnectorRegistry,
        default_mode: TableLookupMode,
    ) -> Self {
        Self {
            current_catalog,
            local,
            catalog_mgr,
            connectors,
            default_mode,
        }
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn iceberg_table_def(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        mode: &TableLookupMode,
    ) -> Result<TableDef, String> {
        match mode {
            TableLookupMode::SchemaOnly => self
                .catalog_mgr
                .resolve(catalog, database, table)
                .map(|metadata| metadata.to_table_def()),
            TableLookupMode::ExplainStats
            | TableLookupMode::IcebergMetadata {
                metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType::Partitions,
            } => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_table_def(&resolved)
            }
            TableLookupMode::IcebergMetadata { .. } => self
                .catalog_mgr
                .resolve(catalog, database, table)
                .map(|metadata| metadata.to_table_def()),
        }
    }
}

impl CatalogProvider for CatalogMgrProvider<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get_table_with_mode(None, database, table, self.default_mode.clone())
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        self.get_table_with_mode(catalog, database, table, self.default_mode.clone())
    }

    fn get_table_with_mode(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        mode: TableLookupMode,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self.local.get_table(database, table),
            Some(catalog) => self.iceberg_table_def(catalog, database, table, &mode),
        }
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<crate::sql::catalog::LegacyRangePartition>, String> {
        self.local.get_legacy_range_partition(database, table, partition)
    }

    fn get_physical_layout(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<crate::sql::catalog::PhysicalTableLayout>, String> {
        self.local.get_physical_layout(database, table)
    }
}
```

- [ ] **Step 5: Export provider and make `CatalogMgr` cloneable**

In `src/engine/catalog_mgr/mod.rs`:

```rust
pub(crate) mod provider;

#[derive(Clone, Default)]
pub(crate) struct CatalogMgr {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogMgr {
    pub(crate) fn unregister(&mut self, name: &str) {
        self.catalogs.remove(&name.to_ascii_lowercase());
    }

    pub(crate) fn invalidate_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(), String> {
        self.get_catalog(catalog)?.invalidate_table(namespace, table);
        Ok(())
    }
}
```

Keep `register` normalized:

```rust
pub(crate) fn register(&mut self, catalog: Arc<dyn Catalog>) {
    self.catalogs
        .insert(catalog.name().to_ascii_lowercase(), catalog);
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib engine::catalog_mgr::metadata::tests::table_metadata_to_table_def_rebuilds_schema_only_iceberg_source
cargo test --lib engine::catalog_mgr::provider::tests
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/catalog_mgr/metadata.rs src/engine/catalog_mgr/provider.rs src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add analyzer provider"
```

---

## Task 3: Wire Real Schema-ID Probe and Catalog Invalidations

**Files:**
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/engine/catalog_mgr/catalog.rs`
- Modify: `src/engine/catalog_mgr/iceberg.rs`

- [ ] **Step 1: Write failing schema-id cache test**

In `src/engine/catalog_mgr/iceberg.rs` tests, extend `MockBackend` with a schema id counter and add:

```rust
#[test]
fn iceberg_catalog_rebuilds_when_remote_schema_id_changes() {
    let loads = Arc::new(AtomicUsize::new(0));
    let schema_id = Arc::new(AtomicUsize::new(1));
    let cat = IcebergCatalog::new(
        "ice",
        Arc::new(MockBackend {
            loads: Arc::clone(&loads),
            schema_id: Arc::clone(&schema_id),
        }),
        Arc::new(MockSource),
    );

    let first = cat.get_table_metadata("ns", "t").expect("first");
    let second = cat.get_table_metadata("ns", "t").expect("cached");
    assert_eq!(first.columns.len(), second.columns.len());
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    schema_id.store(2, Ordering::SeqCst);
    let _ = cat.get_table_metadata("ns", "t").expect("rebuild");
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}
```

Expected: compile failure because `CatalogBackend::current_schema_id` is missing.

- [ ] **Step 2: Add schema-id hook to `CatalogBackend`**

In `src/connector/backend.rs`:

```rust
fn current_schema_id(
    &self,
    _catalog: &str,
    _namespace: &str,
    _table: &str,
) -> Result<Option<i32>, String> {
    Ok(None)
}
```

- [ ] **Step 3: Implement Iceberg schema-id hook**

In `impl CatalogBackend for IcebergCatalogBackend`:

```rust
fn current_schema_id(
    &self,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<Option<i32>, String> {
    let loaded = reg_load_table(&self.entry(catalog)?, namespace, table)?;
    Ok(Some(loaded.table.metadata().current_schema_id()))
}
```

- [ ] **Step 4: Add invalidation hook to `Catalog`**

In `src/engine/catalog_mgr/catalog.rs`:

```rust
fn invalidate_table(&self, _namespace: &str, _table: &str) {}
```

In `src/engine/catalog_mgr/iceberg.rs`:

```rust
fn invalidate_table(&self, namespace: &str, table: &str) {
    self.invalidate(namespace, table);
}
```

- [ ] **Step 5: Use schema-only table defs and schema-id validation in `IcebergCatalog`**

In `src/engine/catalog_mgr/iceberg.rs`, replace `get_table_metadata` body:

```rust
fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<TableMetadata, String> {
    let id = TableIdentity::new(&self.name, namespace, table);
    let current_schema_id = self.backend.current_schema_id(&self.name, namespace, table)?;
    self.cache.get_or_build_validated(&id, current_schema_id, || {
        let resolved = self.backend.load_table(&self.name, namespace, table)?;
        let td = self.source.build_schema_table_def(&resolved)?;
        TableMetadata::from_table_def(id.clone(), &td)
    })
}
```

- [ ] **Step 6: Run targeted tests**

Run:

```bash
cargo test --lib engine::catalog_mgr::iceberg::tests
cargo test --lib connector::iceberg::catalog::backend::tests::schema_only_
```

Expected: all targeted tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs src/engine/catalog_mgr/catalog.rs src/engine/catalog_mgr/iceberg.rs
git commit -m "feat(catalog-mgr): validate Iceberg schema cache by schema id"
```

---

## Task 4: Wire CatalogMgr Into StandaloneState and Catalog Lifecycle

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/connector/mod.rs`
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Write failing lifecycle tests**

In `src/connector/mod.rs` tests, add:

```rust
#[test]
fn standalone_backends_register_internal_catalog_mgr_entry() {
    let state = Arc::new(crate::engine::StandaloneState::default());
    register_standalone_backends(&state);

    let mgr = state.catalog_mgr.read().expect("catalog mgr");
    assert!(mgr.get_catalog("default_catalog").is_ok());
}
```

In `src/engine/mod.rs` tests, add:

```rust
#[test]
fn create_catalog_registers_catalog_mgr_entry() {
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open");
    engine
        .execute("CREATE CATALOG ice PROPERTIES('type'='memory')")
        .expect("create catalog");

    let mgr = engine.inner.catalog_mgr.read().expect("catalog mgr");
    assert!(mgr.get_catalog("ice").is_ok());
}
```

Expected: compile failure because `StandaloneState.catalog_mgr` does not exist.

- [ ] **Step 2: Change `StandaloneState.catalog` to shared Arc and add `catalog_mgr`**

In `src/engine/mod.rs`:

```rust
pub(crate) struct StandaloneState {
    pub(crate) catalog: Arc<RwLock<InMemoryCatalog>>,
    pub(crate) catalog_mgr: RwLock<catalog_mgr::CatalogMgr>,
    pub(crate) iceberg_catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
}
```

In `Default`:

```rust
let catalog = Arc::new(RwLock::new(InMemoryCatalog::default()));
let mut catalog_mgr = catalog_mgr::CatalogMgr::new();
catalog_mgr.register(Arc::new(catalog_mgr::internal::InternalCatalog::new(
    "default_catalog",
    Arc::clone(&catalog),
)));
Self {
    catalog,
    catalog_mgr: RwLock::new(catalog_mgr),
    iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
}
```

Keep the rest of the `StandaloneState` fields exactly as they are in the current file.

In `open_body`, build `catalog` and `catalog_mgr` the same way before constructing `StandaloneState`.

- [ ] **Step 3: Update struct literals**

For every test literal found by:

```bash
rg -n "StandaloneState \\{|catalog: RwLock::new" src tests
```

replace:

```rust
catalog: RwLock::new(catalog),
```

with:

```rust
catalog: Arc::new(RwLock::new(catalog)),
```

When a literal uses `..StandaloneState::default()`, prefer:

```rust
let mut state = StandaloneState::default();
state.catalog = Arc::new(RwLock::new(catalog));
let state = Arc::new(state);
```

so the default `catalog_mgr` is not silently left pointing at the old catalog. If that pattern is awkward, rebuild `catalog_mgr` with the helper from Step 4.

- [ ] **Step 4: Add CatalogMgr registration helpers**

In `src/connector/mod.rs`:

```rust
pub(crate) fn register_default_catalog_mgr_entries(state: &Arc<crate::engine::StandaloneState>) {
    let mut mgr = state.catalog_mgr.write().expect("catalog mgr write lock");
    mgr.register(Arc::new(crate::engine::catalog_mgr::internal::InternalCatalog::new(
        "default_catalog",
        Arc::clone(&state.catalog),
    )));
}

pub(crate) fn register_iceberg_catalog_mgr_entry(
    state: &Arc<crate::engine::StandaloneState>,
    catalog_name: &str,
) -> Result<(), String> {
    let connectors = state
        .connectors
        .read()
        .expect("connector registry read lock");
    let backend = connectors.catalog_backend("iceberg")?;
    let source = connectors.table_source("iceberg")?;
    drop(connectors);
    state
        .catalog_mgr
        .write()
        .expect("catalog mgr write lock")
        .register(Arc::new(crate::engine::catalog_mgr::iceberg::IcebergCatalog::new(
            catalog_name,
            backend,
            source,
        )));
    Ok(())
}
```

Call `register_default_catalog_mgr_entries(state);` from `register_standalone_backends`.

- [ ] **Step 5: Register restored and newly created Iceberg catalogs**

In `restore_iceberg_catalogs`, after each successful `guard.create_catalog` call:

```rust
crate::connector::register_iceberg_catalog_mgr_entry(state, &catalog.catalog)?;
```

In `handle_create_catalog`, after the successful `guard.create_catalog` call and before returning:

```rust
crate::connector::register_iceberg_catalog_mgr_entry(&self.inner, &stmt.name)?;
```

In `execute_drop_catalog_statement`, after dropping from `iceberg_catalogs` and deleting metadata:

```rust
state
    .catalog_mgr
    .write()
    .expect("catalog mgr write lock")
    .unregister(&normalize_identifier(catalog_name)?);
```

- [ ] **Step 6: Run lifecycle tests**

Run:

```bash
cargo test --lib connector::tests::standalone_backends_register_internal_catalog_mgr_entry
cargo test --lib engine::tests::create_catalog_registers_catalog_mgr_entry
cargo test --lib engine::catalog_mgr::
```

Expected: all targeted tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs src/connector/mod.rs src/engine/statement.rs
git commit -m "feat(catalog-mgr): wire standalone catalog registry"
```

---

## Task 5: Cut SELECT/EXPLAIN Analysis Over to CatalogMgrProvider

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/query_prep.rs`

- [ ] **Step 1: Write failing no-global-registration test**

In `src/engine/query_prep.rs` tests, add a regression test that proves provider-based analysis no longer requires mutating `state.catalog`:

```rust
#[test]
fn catalog_mgr_provider_analyzes_iceberg_query_without_global_registration() {
    let state = state_with_per_table_binding_source();
    {
        let mut mgr = state.catalog_mgr.write().expect("catalog mgr");
        let connectors = state.connectors.read().expect("connectors");
        mgr.register(std::sync::Arc::new(
            crate::engine::catalog_mgr::iceberg::IcebergCatalog::new(
                "ice",
                connectors.catalog_backend("iceberg").expect("backend"),
                connectors.table_source("iceberg").expect("source"),
            ),
        ));
    }
    let local = state.catalog.read().expect("catalog").clone();
    let connectors = state.connectors.read().expect("connectors").clone();
    let mgr = state.catalog_mgr.read().expect("catalog mgr").clone();
    let provider = crate::engine::catalog_mgr::provider::CatalogMgrProvider::new(
        Some("ice"),
        &local,
        &mgr,
        &connectors,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let query = parse_query_for_table_names("SELECT * FROM parted");

    let _ = crate::sql::analyzer::analyze(&query, &provider, "db").expect("analyze");

    assert!(
        local.get("db", "parted").is_err(),
        "Iceberg analysis must not require global InMemoryCatalog registration"
    );
}
```

Expected: fail before Task 5 because engine paths still rely on global registration, or compile failure until Task 4 helper state is updated.

- [ ] **Step 2: Add provider construction helpers**

In `src/engine/mod.rs`, add:

```rust
fn catalog_mgr_snapshot(state: &Arc<StandaloneState>) -> catalog_mgr::CatalogMgr {
    state.catalog_mgr.read().expect("catalog mgr read lock").clone()
}

fn build_analyzer_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog: &'a InMemoryCatalog,
    catalog_mgr: &'a catalog_mgr::CatalogMgr,
    connectors: &'a crate::connector::ConnectorRegistry,
    mode: crate::sql::catalog::TableLookupMode,
) -> catalog_mgr::provider::CatalogMgrProvider<'a> {
    catalog_mgr::provider::CatalogMgrProvider::new(
        current_catalog,
        catalog,
        catalog_mgr,
        connectors,
        mode,
    )
}
```

- [ ] **Step 3: Stop pre-registering ordinary Iceberg SELECT refs**

In the `sqlast::Statement::Query` branch of `StandaloneSession::execute_in_context`:

- Keep view expansion, virtual table rewrite, and `rewrite_time_travel_refs`.
- Remove calls to `register_iceberg_tables_for_query` for ordinary non-time-travel refs.
- Remove `extract_three_part_table_refs`/`strip_catalog_from_three_part_names` for ordinary base tables.
- Keep time-travel synthetic registration because `rewrite_time_travel_refs` still materializes `<table>__at_<snapshot_id>`.

Use this execution shape for the final query:

```rust
let catalog_snapshot = self
    .inner
    .catalog
    .read()
    .expect("standalone catalog read lock")
    .clone();
let connectors_snapshot = self
    .inner
    .connectors
    .read()
    .expect("standalone connector registry read lock")
    .clone();
let catalog_mgr_snapshot = catalog_mgr_snapshot(&self.inner);
let analyzer_provider = build_analyzer_provider(
    current_catalog,
    &catalog_snapshot,
    &catalog_mgr_snapshot,
    &connectors_snapshot,
    crate::sql::catalog::TableLookupMode::SchemaOnly,
);
self::statistics::observe_query(&self.inner, query, current_database)?;
let result = execute_query_with_catalog_provider(
    query,
    &analyzer_provider,
    &catalog_snapshot,
    &connectors_snapshot,
    current_database,
    self.inner.exchange_port,
    query_opts.clone(),
)?;
```

- [ ] **Step 4: Split execute query analyzer provider from codegen catalog**

Add a new function near `execute_query`:

```rust
pub(crate) fn execute_query_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator(
        query,
        analyzer_catalog,
        codegen_catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        None,
        None,
        None,
        None,
    )
}
```

Then change `execute_query_with_options_and_imv_validator` signature to take both:

```rust
pub(crate) fn execute_query_with_options_and_imv_validator(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_with_mv_refresh_ctx(
            &physical,
            codegen_catalog,
            connectors,
            mv_refresh_ctx,
        )?;
```

Keep legacy `execute_query` by passing `catalog` as both analyzer and codegen catalog.

- [ ] **Step 5: Cut EXPLAIN over to provider mode**

In `prepare_explain_query`, remove global Iceberg registration and three-part stripping. Only view/time-travel rewrites remain.

Change `explain_query` and `explain_analyze_query` signatures to accept `analyzer_catalog: &dyn CatalogProvider` plus `codegen_catalog: &InMemoryCatalog` when they execute. Use `TableLookupMode::ExplainStats` for the planning provider so row-count/min-max stats from P2 remain available:

```rust
let analyzer_provider = build_analyzer_provider(
    current_catalog,
    &catalog_snapshot,
    &catalog_mgr_snapshot,
    &connectors_snapshot,
    crate::sql::catalog::TableLookupMode::ExplainStats,
);
```

- [ ] **Step 6: Run targeted tests**

Run:

```bash
cargo test --lib engine::query_prep::tests::catalog_mgr_provider_analyzes_iceberg_query_without_global_registration
cargo test --lib engine::query_prep::tests::partition_metadata_scan_binding_is_per_table
cargo test --lib sql::analyzer::tests::analyzer_resolves_t_dollar_snapshots_to_metadata_scan
cargo test --lib sql::codegen::fragment_builder::tests::explicit_iceberg_data_file_binding_uses_explicit_files
```

Expected: all targeted tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs src/engine/query_prep.rs
git commit -m "feat(catalog-mgr): resolve standalone Iceberg queries without global registration"
```

---

## Task 6: Wire Write/DDL Invalidations and Remove Query-Prep Dead Code

**Files:**
- Modify: `src/engine/query_prep.rs`
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/mutation_flow.rs`
- Modify: `src/engine/delete_flow.rs`
- Modify: `src/engine/equality_delete_flow.rs`
- Modify: `src/engine/iceberg_truncate.rs`
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/iceberg_ctas.rs`
- Modify: `src/connector/iceberg/catalog/schema_update.rs`
- Modify any compile-discovered call sites.

- [ ] **Step 1: Add cache invalidation helper**

In `src/engine/query_prep.rs`, before deleting old helpers, add a focused helper:

```rust
pub(crate) fn invalidate_catalog_mgr_table(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    state
        .catalog_mgr
        .read()
        .expect("catalog mgr read lock")
        .invalidate_table(catalog, namespace, table)
}
```

- [ ] **Step 2: Replace write-path local registration refreshes**

For each Iceberg write/DDL path that currently calls `refresh_external_tables_for_query`, `register_external_table_by_name`, or `drop_registered_external_table` only to refresh Iceberg schema, replace the schema refresh with:

```rust
crate::engine::query_prep::invalidate_catalog_mgr_table(
    state,
    &target.catalog,
    &target.namespace,
    &target.table,
)?;
```

Keep `drop_registered_external_table` only when the target is known to be a synthetic local table or StarRocks-managed local table cleanup.

- [ ] **Step 3: Delete normal query-prep registration API**

Remove these from `src/engine/query_prep.rs` after call sites are gone:

```rust
register_external_tables_for_query
register_external_tables_for_query_with_scan_bindings
refresh_external_tables_for_query
register_external_tables_for_query_impl
build_registration_table_def
build_query_registration_table_def
query_table_names
partition_metadata_scan_binding_targets
```

Keep:

```rust
rewrite_time_travel_refs
build_iceberg_table_def_for_delta_scan
build_iceberg_table_def_with_files
drop_registered_external_table
```

because they still support synthetic version/delta/MV paths or local-table cleanup.

- [ ] **Step 4: Remove now-unused query-ref imports**

In `src/engine/mod.rs`, remove:

```rust
use crate::sql::parser::query_refs::{
    extract_three_part_table_refs, strip_catalog_from_three_part_names,
};
```

unless another non-registration path still uses them.

- [ ] **Step 5: Run compile to find remaining call sites**

Run:

```bash
cargo test --lib engine::query_prep::tests
```

Expected: compile failures only for stale references to deleted helpers. Fix each stale reference by either using `CatalogMgrProvider` or `invalidate_catalog_mgr_table`.

- [ ] **Step 6: Run static dead-code search**

Run:

```bash
rg -n "register_external_tables_for_query|register_external_tables_for_query_with_scan_bindings|refresh_external_tables_for_query|partition_metadata_scan_binding_targets|query_requires_partition_metadata_files" src
```

Expected: no matches except deleted-test history if a test was intentionally removed.

- [ ] **Step 7: Run targeted tests**

Run:

```bash
cargo test --lib engine::query_prep::tests
cargo test --lib engine::catalog_mgr::
cargo test --lib sql::analyzer::tests::analyzer_passes_three_part_catalog_to_catalog_provider sql::analyzer::tests::analyzer_uses_metadata_lookup_mode_for_partitions_table
cargo test --lib sql::codegen::fragment_builder::tests::explicit_iceberg_data_file_binding_uses_explicit_files
```

Expected: all targeted tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/engine/query_prep.rs src/engine/iceberg_writer.rs src/engine/mutation_flow.rs src/engine/delete_flow.rs src/engine/equality_delete_flow.rs src/engine/iceberg_truncate.rs src/engine/statement.rs src/engine/iceberg_ctas.rs src/connector/iceberg/catalog/schema_update.rs src/engine/mod.rs
git commit -m "chore(catalog-mgr): remove legacy Iceberg query registration"
```

---

## Task 7: End-to-End Verification and PR Cleanup

**Files:**
- Modify only if verification finds defects.

- [ ] **Step 1: Format and whitespace**

Run:

```bash
cargo fmt --check
git diff --check
```

Expected: both pass. If `cargo fmt --check` fails, run `cargo fmt`, inspect the diff, and commit formatting with the affected task if not already committed.

- [ ] **Step 2: Run catalog/query targeted unit tests**

Run:

```bash
cargo test --lib engine::catalog_mgr::
cargo test --lib engine::query_prep::tests
cargo test --lib sql::analyzer::tests::analyzer_resolves_t_dollar_snapshots_to_metadata_scan
cargo test --lib sql::codegen::fragment_builder::tests::explicit_iceberg_data_file_binding_uses_explicit_files
cargo test --lib connector::iceberg::catalog::backend::tests::schema_only_
cargo test --lib connector::iceberg::scan_planner::tests
```

Expected: all pass.

- [ ] **Step 3: Run build and clippy**

Run:

```bash
cargo build
cargo clippy --lib
```

Expected: build passes; clippy has no new warnings in touched P3/P4 files.

- [ ] **Step 4: Start the generated Iceberg REST runtime and standalone server**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-p3-p4-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > "$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    tail -40 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG"
```

Expected: server prints `NOVAROCKS_READY mysql_port=...` for the generated worktree port.

- [ ] **Step 5: Run the P3 concurrency regression gate**

Prepare the SQL test runner and bootstrap SSB data once against the running server:

```bash
cargo build --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests
SQL_TEST_RUNNER=tests/sql-test-runner/target/debug/sql-tests

"$SQL_TEST_RUNNER" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --only q2.1 \
  --mode verify \
  --query-timeout 120 \
  -j 1
```

Run the original SSB q2.1 reproduction shape without allowing concurrent benchmark bootstrap:

```bash
for worker in $(seq 1 8); do
  (
    for iter in $(seq 1 10); do
      "$SQL_TEST_RUNNER" \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite ssb \
        --only q2.1 \
        --mode verify \
        --no-auto-bootstrap-benchmark-data \
        --query-timeout 120 \
        -j 1
    done
  ) >"/tmp/novarocks-q21-worker-${worker}.log" 2>&1 &
done
wait
```

Expected: all 8 workers complete 10/10 iterations with 0 `unknown table` failures.

- [ ] **Step 6: Run the SSB parallel suite gate**

Run:

```bash
"$SQL_TEST_RUNNER" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify \
  --no-auto-bootstrap-benchmark-data \
  --query-timeout 120 \
  -j 8
```

Expected: `ssb` passes under parallel case execution.

- [ ] **Step 7: Run schema freshness gates**

Run the in-process schema evolution case:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_schema_evolution \
  --mode verify \
  --query-timeout 120 \
  -j 1
```

Run the Spark-written external schema evolution case:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility \
  --only spark_rest_minio_v3_schema_evolution \
  --mode verify \
  --query-timeout 180 \
  -j 1
```

Expected: the local `ALTER`/write path sees the new schema after invalidation, and Spark schema changes are visible through `schema_id` validation.

- [ ] **Step 8: Run the full spec SQL regression gate**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --mode verify \
  --query-timeout 120 \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --mode verify \
  --query-timeout 120 \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility \
  --mode verify \
  --query-timeout 180 \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite join \
  --mode verify \
  --query-timeout 60 \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-h \
  --mode verify \
  --query-timeout 120 \
  -j 1
```

Expected: all required spec suites pass with no regressions.

- [ ] **Step 9: Stop the standalone server**

Run:

```bash
kill -INT "$SRV_PID"
```

Expected: the standalone server exits cleanly.

- [ ] **Step 10: Final search for forbidden boundary regressions**

Run:

```bash
rg -n "catalog_mgr" src/lower
rg -n "register_external_tables_for_query|refresh_external_tables_for_query" src
```

Expected:
- First command returns no matches.
- Second command returns no matches after Task 6 cleanup.

- [ ] **Step 11: Final commit if verification required fixes**

Only if Step 1-10 required fixes, stage the files changed by those fixes explicitly. For example, if the fix touches the analyzer provider and engine wiring:

```bash
git add src/engine/catalog_mgr/provider.rs src/engine/mod.rs
git commit -m "chore(catalog-mgr): verify P3 P4 cutover"
```

---

## Acceptance Criteria

1. Ordinary standalone Iceberg SELECT/EXPLAIN analysis resolves base tables through `CatalogMgrProvider` and `CatalogMgr`, not by mutating `StandaloneState.catalog`.
2. Current-catalog 1/2-part names and explicit 3-part names both preserve catalog identity during analyzer lookup.
3. `$partitions` metadata scans still get explicit scan-binding file metadata; other metadata tables use schema-level table metadata.
4. `SchemaCache` validates Iceberg entries against a real remote `current_schema_id` probe and invalidates on Iceberg write/DDL paths.
5. `CREATE CATALOG`, metadata restore, and `DROP CATALOG` update `CatalogMgr`.
6. `src/lower/**` has no `catalog_mgr` dependency.
7. Normal query-prep external-table drop/register helpers are removed or reduced to synthetic time-travel/local cleanup only.
8. Targeted unit tests, `cargo build`, and `cargo clippy --lib` pass.
9. P3 concurrency regression gate passes with 8 workers x 10 q2.1 iterations and 0 failures.
10. `ssb` passes under `-j 8` parallel verify.
11. Schema freshness passes for both in-process Iceberg REST schema evolution and Spark-written `iceberg-compatibility` schema evolution.
12. Full spec SQL regression gate passes for `iceberg`, `iceberg-rest`, `iceberg-compatibility`, `join`, and `tpc-h`.

## Risks and Mitigations

- **Risk:** Holding `catalog_mgr` locks across query execution can block DDL.
  **Mitigation:** clone `CatalogMgr` before planning; its `Arc<dyn Catalog>` entries are cheap to clone and avoid holding the registry lock through execution.
- **Risk:** EXPLAIN loses row-count/min-max observability after schema-only cutover.
  **Mitigation:** use `TableLookupMode::ExplainStats` so the provider materializes scan-binding metadata for planning stats while runtime codegen still binds current snapshot in the scan planner.
- **Risk:** `$partitions` loses partition rows if schema-only metadata is used.
  **Mitigation:** analyzer requests `TableLookupMode::IcebergMetadata { Partitions }`; provider builds an explicit-file `TableDef` through the Iceberg `TableSource`.
- **Risk:** Some write path still relies on global `InMemoryCatalog` Iceberg registration.
  **Mitigation:** Task 6 deletes the registration API and uses compile errors plus `rg` to force every caller onto `CatalogMgr` invalidation or an explicit synthetic-table exception.
