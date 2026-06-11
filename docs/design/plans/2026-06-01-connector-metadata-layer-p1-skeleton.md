# Connector Metadata 层 P1（骨架类型 + 抽象）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 connector metadata 层的骨架——`TableMetadata`/`TableBinding`/`Catalog` trait/`CatalogMgr`/`SchemaCache`/`InternalCatalog`/`IcebergCatalog`，纯新增、不接线、全部单元可测。

**Architecture:** 在 `src/engine/catalog_mgr/` 下新建一个独立模块。它定义 schema 层元数据类型与 catalog 抽象，但**不修改** analyzer / codegen / query_prep 的任何现有调用（接线在后续 P2/P3）。`IcebergCatalog` 复用现有 `CatalogBackend` + `TableSource` 抽象组装 `TableMetadata`，单测用 mock 实现，不依赖真实 Iceberg。

**Tech Stack:** Rust，`std::sync::RwLock`，现有 `src/sql/catalog.rs` 类型（`TableDef`/`ScanSource`/`IcebergTableInfo`）、`src/connector/backend.rs`（`CatalogBackend`/`TableSource`/`ResolvedTable`）。

**设计说明（相对 spec 的细化）：**
- `TableBinding::Internal` 只含 `{ db_id, table_id }`（不含 `schema_id`）——`schema_id` 在 StarRocks 表里属于 `PhysicalTableLayout`（scan-binding 层），不在 `TableDef` 内；本地/StarRocks 表的 schema 新鲜度由本进程 DDL 的 invalidate 保证，不需要 `schema_id` 校验。`schema_id` 校验只对 Iceberg（防外部改 schema）有意义。
- P1 的 `SchemaCache::get_or_build_validated` 已实现 `schema_id` 校验逻辑（由调用方传入 `current_schema_id`）；但 `IcebergCatalog` 在 P1 暂传 `None`（不做远程 probe），真实 probe 在 P3 接线时补上。
- `TableMetadata::from_table_def` 只处理 `ScanSource::StarRocks` 和 `ScanSource::IcebergDataFiles`（catalog base table 的两种形态）。合成变体（`IcebergMetadataTable`/`IcebergDeltaTable`/`IcebergVersionTable`）是 analyzer/optimizer 的 plan-time 产物、不来自 catalog，遇到时 `fail fast` 返回错误。

**文件结构：**
- Create `src/engine/catalog_mgr/mod.rs` — 模块声明 + `CatalogMgr`
- Create `src/engine/catalog_mgr/metadata.rs` — `TableIdentity` / `TableBinding` / `TableMetadata` + `from_table_def`
- Create `src/engine/catalog_mgr/catalog.rs` — `trait Catalog`
- Create `src/engine/catalog_mgr/schema_cache.rs` — `SchemaCache`
- Create `src/engine/catalog_mgr/internal.rs` — `InternalCatalog`
- Create `src/engine/catalog_mgr/iceberg.rs` — `IcebergCatalog`
- Modify `src/engine/mod.rs` — 增加 `pub(crate) mod catalog_mgr;`

---

## Task 1: metadata 类型 + `from_table_def` 转换

**Files:**
- Create: `src/engine/catalog_mgr/metadata.rs`

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/metadata.rs` 末尾写：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use arrow::datatypes::DataType;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "t".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 3,
            location: "s3://w/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    #[test]
    fn from_table_def_maps_starrocks_binding() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![col("a")],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 10, table_id: 20 },
        };
        let id = TableIdentity::new("default_catalog", "db", "t");
        let meta = TableMetadata::from_table_def(id.clone(), &td).expect("convert");
        assert_eq!(meta.identity, id);
        assert_eq!(meta.columns.len(), 1);
        assert_eq!(meta.binding, TableBinding::Internal { db_id: 10, table_id: 20 });
    }

    #[test]
    fn from_table_def_maps_iceberg_binding_and_drops_files() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![col("a"), col("b")],
            iceberg_row_lineage_metadata_columns: vec![col("_row_id")],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(),
                files: vec![], // files 应被丢弃，不进 TableMetadata
                cloud_properties: Default::default(),
            },
        };
        let id = TableIdentity::new("ice", "ns", "t");
        let meta = TableMetadata::from_table_def(id.clone(), &td).expect("convert");
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.iceberg_row_lineage_columns.len(), 1);
        match meta.binding {
            TableBinding::Iceberg { info } => {
                assert_eq!(info.schema_id, 3);
                assert_eq!(info.table, "t");
            }
            other => panic!("expected Iceberg binding, got {other:?}"),
        }
    }

    #[test]
    fn from_table_def_rejects_synthetic_source() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergVersionTable {
                table: iceberg_info(),
                snapshot_id: 7,
            },
        };
        let id = TableIdentity::new("ice", "ns", "t");
        let err = TableMetadata::from_table_def(id, &td).expect_err("must reject synthetic");
        assert!(err.contains("synthetic"), "got: {err}");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::metadata::tests 2>&1 | tail -20`
Expected: 编译失败（`TableIdentity` / `TableMetadata` / `TableBinding` 未定义，模块未声明）。

- [ ] **Step 3: 写最小实现**

在 `src/engine/catalog_mgr/metadata.rs` 顶部写：

```rust
//! Schema-level table metadata for the connector metadata layer.
//!
//! `TableMetadata` is what the analyzer needs to resolve a table: identity +
//! columns + a backend `TableBinding` that says *where* scan-binding will be
//! resolved later (in codegen). It deliberately carries NO scan-binding data
//! (no Iceberg data files, no StarRocks tablets, no snapshot) so it is stable
//! and safe to cache.

use crate::sql::catalog::{ColumnDef, IcebergTableInfo, ScanSource, TableDef};

/// Fully-qualified table identity. Used as the schema-cache key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TableIdentity {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl TableIdentity {
    pub(crate) fn new(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }
}

/// Backend-specific locator for scan-binding. Carries identity only, never data.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TableBinding {
    /// Local / StarRocks table. Tablets live in `InMemoryCatalog`
    /// (`PhysicalTableLayout`); resolved at plan time, not here.
    Internal { db_id: i64, table_id: i64 },
    /// Iceberg table. `info` carries identity + schema; the current snapshot's
    /// data files are resolved at codegen time, never stored here.
    Iceberg { info: IcebergTableInfo },
}

/// Schema-level metadata returned by `Catalog::get_table_metadata`. Cacheable.
#[derive(Clone, Debug)]
pub(crate) struct TableMetadata {
    pub identity: TableIdentity,
    pub columns: Vec<ColumnDef>,
    pub iceberg_row_lineage_columns: Vec<ColumnDef>,
    pub binding: TableBinding,
}

impl TableMetadata {
    /// Build schema-level metadata from a legacy `TableDef`, dropping any
    /// scan-binding data (Iceberg files). Only catalog base-table sources are
    /// accepted; synthetic plan-time sources are rejected (fail fast).
    pub(crate) fn from_table_def(
        identity: TableIdentity,
        td: &TableDef,
    ) -> Result<Self, String> {
        let binding = match &td.source {
            ScanSource::StarRocks { db_id, table_id } => TableBinding::Internal {
                db_id: *db_id,
                table_id: *table_id,
            },
            ScanSource::IcebergDataFiles { table, .. } => TableBinding::Iceberg {
                info: table.clone(),
            },
            ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergVersionTable { .. } => {
                return Err(format!(
                    "synthetic plan-time scan source is not a catalog base table: {}.{}.{}",
                    identity.catalog, identity.namespace, identity.table
                ));
            }
        };
        Ok(Self {
            identity,
            columns: td.columns.clone(),
            iceberg_row_lineage_columns: td.iceberg_row_lineage_metadata_columns.clone(),
            binding,
        })
    }
}
```

- [ ] **Step 4: 声明模块（使测试可编译）**

`src/engine/catalog_mgr/mod.rs`（新建，本 task 先放最小内容）：

```rust
//! Connector metadata layer (FE-side). See
//! docs/design/specs/2026-06-01-connector-metadata-layer-design.md

pub(crate) mod metadata;
```

`src/engine/mod.rs`：在现有 `pub(crate) mod catalog;`（第 37 行附近）之后加一行：

```rust
pub(crate) mod catalog_mgr;
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::metadata::tests 2>&1 | tail -20`
Expected: 3 个测试 PASS。

- [ ] **Step 6: commit**

```bash
git add src/engine/catalog_mgr/mod.rs src/engine/catalog_mgr/metadata.rs src/engine/mod.rs
git commit -m "feat(catalog-mgr): add TableMetadata/TableIdentity/TableBinding types"
```

---

## Task 2: `trait Catalog`

**Files:**
- Create: `src/engine/catalog_mgr/catalog.rs`
- Modify: `src/engine/catalog_mgr/mod.rs`

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/catalog.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};

    struct FixedCatalog;

    impl Catalog for FixedCatalog {
        fn name(&self) -> &str {
            "fixed"
        }
        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            if table == "missing" {
                return Err(format!("unknown table: {table}"));
            }
            Ok(TableMetadata {
                identity: TableIdentity::new("fixed", namespace, table),
                columns: vec![],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Internal { db_id: 1, table_id: 2 },
            })
        }
    }

    #[test]
    fn catalog_trait_object_resolves_table() {
        let cat: Box<dyn Catalog> = Box::new(FixedCatalog);
        assert_eq!(cat.name(), "fixed");
        let meta = cat.get_table_metadata("ns", "t").expect("resolve");
        assert_eq!(meta.identity.table, "t");
        assert!(cat.get_table_metadata("ns", "missing").is_err());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::catalog::tests 2>&1 | tail -20`
Expected: 编译失败（`Catalog` 未定义）。

- [ ] **Step 3: 写最小实现**

`src/engine/catalog_mgr/catalog.rs` 顶部：

```rust
//! The `Catalog` trait: one named catalog's schema-resolution interface.
//! Implemented by `InternalCatalog` (local/StarRocks) and `IcebergCatalog`.

use crate::engine::catalog_mgr::metadata::TableMetadata;

pub(crate) trait Catalog: Send + Sync {
    /// The catalog's registered name (e.g. "default_catalog", "iceberg_cat_x").
    fn name(&self) -> &str;

    /// Resolve schema-level metadata for `namespace.table`. Returns an error
    /// when the table does not exist or cannot be resolved.
    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<TableMetadata, String>;
}
```

`src/engine/catalog_mgr/mod.rs` 增加：

```rust
pub(crate) mod catalog;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::catalog::tests 2>&1 | tail -20`
Expected: 1 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add src/engine/catalog_mgr/catalog.rs src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add Catalog trait"
```

---

## Task 3: `CatalogMgr`（named catalog 注册表）

**Files:**
- Modify: `src/engine/catalog_mgr/mod.rs`

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/mod.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use std::sync::Arc;

    struct OneTableCatalog {
        name: String,
    }

    impl Catalog for OneTableCatalog {
        fn name(&self) -> &str {
            &self.name
        }
        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            Ok(TableMetadata {
                identity: TableIdentity::new(&self.name, namespace, table),
                columns: vec![],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Internal { db_id: 1, table_id: 1 },
            })
        }
    }

    #[test]
    fn mgr_registers_and_resolves() {
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(OneTableCatalog { name: "ice".to_string() }));

        let meta = mgr.resolve("ice", "ns", "t").expect("resolve");
        assert_eq!(meta.identity.catalog, "ice");
        assert_eq!(meta.identity.table, "t");
    }

    #[test]
    fn mgr_unknown_catalog_errors() {
        let mgr = CatalogMgr::new();
        let err = mgr.resolve("nope", "ns", "t").expect_err("unknown catalog");
        assert!(err.contains("unknown catalog"), "got: {err}");
    }

    #[test]
    fn mgr_get_catalog_returns_handle() {
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(OneTableCatalog { name: "ice".to_string() }));
        let cat = mgr.get_catalog("ice").expect("get");
        assert_eq!(cat.name(), "ice");
        assert!(mgr.get_catalog("missing").is_err());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::tests 2>&1 | tail -20`
Expected: 编译失败（`CatalogMgr` 未定义）。

- [ ] **Step 3: 写最小实现**

在 `src/engine/catalog_mgr/mod.rs` 的 `mod` 声明之后、`#[cfg(test)]` 之前插入：

```rust
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::TableMetadata;

/// Registry of named catalogs (FE-side). Replaces the scattered resolution
/// across `InMemoryCatalog` / `IcebergCatalogRegistry` / `StarRocksTableCatalog`
/// with a single catalog-aware entry point.
#[derive(Default)]
pub(crate) struct CatalogMgr {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogMgr {
    pub(crate) fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
        }
    }

    /// Register (or overwrite) a named catalog. Keyed by `catalog.name()`.
    pub(crate) fn register(&mut self, catalog: Arc<dyn Catalog>) {
        self.catalogs.insert(catalog.name().to_string(), catalog);
    }

    /// Look up a named catalog handle.
    pub(crate) fn get_catalog(&self, name: &str) -> Result<Arc<dyn Catalog>, String> {
        self.catalogs
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown catalog: {name}"))
    }

    /// Resolve schema-level metadata for `catalog.namespace.table`.
    pub(crate) fn resolve(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<TableMetadata, String> {
        self.get_catalog(catalog)?
            .get_table_metadata(namespace, table)
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::tests 2>&1 | tail -20`
Expected: 3 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add CatalogMgr named-catalog registry"
```

---

## Task 4: `SchemaCache`

**Files:**
- Create: `src/engine/catalog_mgr/schema_cache.rs`
- Modify: `src/engine/catalog_mgr/mod.rs`

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/schema_cache.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn meta(id: &TableIdentity, ncols: usize) -> TableMetadata {
        TableMetadata {
            identity: id.clone(),
            columns: Vec::with_capacity(ncols),
            iceberg_row_lineage_columns: vec![],
            binding: TableBinding::Internal { db_id: 1, table_id: 1 },
        }
    }

    #[test]
    fn builds_on_miss_and_caches_on_hit() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);

        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache.get_or_build_validated(&id, Some(1), build).expect("build");
        let _ = cache.get_or_build_validated(&id, Some(1), build).expect("hit");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "second call must hit cache");
    }

    #[test]
    fn rebuilds_when_schema_id_changes() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache.get_or_build_validated(&id, Some(1), build).expect("build v1");
        let _ = cache.get_or_build_validated(&id, Some(2), build).expect("rebuild v2");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "schema_id change must rebuild");
    }

    #[test]
    fn invalidate_forces_rebuild() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache.get_or_build_validated(&id, Some(1), build).expect("build");
        cache.invalidate(&id);
        let _ = cache.get_or_build_validated(&id, Some(1), build).expect("rebuild");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn none_schema_id_always_hits_after_build() {
        // P1 IcebergCatalog passes None (no schema_id probe yet): once built,
        // subsequent None lookups must hit the cache.
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };
        let _ = cache.get_or_build_validated(&id, None, build).expect("build");
        let _ = cache.get_or_build_validated(&id, None, build).expect("hit");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_get_or_build_is_consistent() {
        let cache = Arc::new(SchemaCache::new());
        let id = TableIdentity::new("c", "ns", "t");
        let mut handles = vec![];
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                cache
                    .get_or_build_validated(&id, Some(1), || Ok(meta(&id, 0)))
                    .expect("build")
                    .identity
                    .table
                    .clone()
            }));
        }
        for h in handles {
            assert_eq!(h.join().expect("join"), "t");
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::schema_cache::tests 2>&1 | tail -20`
Expected: 编译失败（`SchemaCache` 未定义）。

- [ ] **Step 3: 写最小实现**

`src/engine/catalog_mgr/schema_cache.rs` 顶部：

```rust
//! Schema cache for the connector metadata layer.
//!
//! Read-mostly cache of `TableMetadata` keyed by `TableIdentity`. Crucially it
//! NEVER drops entries as part of a refresh — staleness is handled by rebuild
//! (on `schema_id` mismatch) or explicit `invalidate`. This eliminates the
//! "table temporarily absent" window that the old per-query drop+reload had.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};

struct CachedEntry {
    /// schema id the cached metadata was built for. `None` means "not tracked"
    /// (the builder didn't probe a schema id); such entries hit on any `None`
    /// lookup and are refreshed only via `invalidate`.
    schema_id: Option<i32>,
    metadata: TableMetadata,
}

#[derive(Default)]
pub(crate) struct SchemaCache {
    entries: RwLock<HashMap<TableIdentity, CachedEntry>>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return cached metadata if present and its `schema_id` matches
    /// `current_schema_id`; otherwise build via `builder`, cache, and return.
    ///
    /// The builder runs WITHOUT holding the cache lock, so a slow remote build
    /// never blocks other tables. Two threads racing the same key may both
    /// build (builds are idempotent); the last write wins. There is never a
    /// window where the key is absent for an unrelated reader.
    pub(crate) fn get_or_build_validated<F>(
        &self,
        id: &TableIdentity,
        current_schema_id: Option<i32>,
        builder: F,
    ) -> Result<TableMetadata, String>
    where
        F: FnOnce() -> Result<TableMetadata, String>,
    {
        // Fast path: read lock, check hit + schema_id match.
        {
            let guard = self.entries.read().expect("schema cache read lock");
            if let Some(entry) = guard.get(id) {
                if entry.schema_id == current_schema_id {
                    return Ok(entry.metadata.clone());
                }
            }
        }
        // Miss or stale: build without holding the lock.
        let metadata = builder()?;
        let mut guard = self.entries.write().expect("schema cache write lock");
        guard.insert(
            id.clone(),
            CachedEntry {
                schema_id: current_schema_id,
                metadata: metadata.clone(),
            },
        );
        Ok(metadata)
    }

    /// Drop the cached entry for one table (e.g. after a local write/DDL).
    pub(crate) fn invalidate(&self, id: &TableIdentity) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .remove(id);
    }

    /// Drop all cached entries.
    pub(crate) fn invalidate_all(&self) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .clear();
    }
}
```

`src/engine/catalog_mgr/mod.rs` 增加：

```rust
pub(crate) mod schema_cache;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::schema_cache::tests 2>&1 | tail -20`
Expected: 5 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add src/engine/catalog_mgr/schema_cache.rs src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add read-mostly SchemaCache with schema_id validation"
```

---

## Task 5: `InternalCatalog`（包 `InMemoryCatalog`）

**Files:**
- Create: `src/engine/catalog_mgr/internal.rs`
- Modify: `src/engine/catalog_mgr/mod.rs`

注：`InMemoryCatalog` 字段私有，但 `register` / `get` 是 `pub(crate)`，且 `InternalCatalog` 在同一 crate 的 `engine` 模块内，可直接调用。测试通过 `InMemoryCatalog::register` 注入一张 StarRocks 表。

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/internal.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::TableBinding;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use std::sync::{Arc, RwLock};

    fn starrocks_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 5, table_id: 6 },
        }
    }

    #[test]
    fn resolves_registered_internal_table() {
        let mut inner = InMemoryCatalog::default();
        inner.create_database("db").expect("create db");
        inner.register("db", starrocks_table_def()).expect("register");

        let cat = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(inner)));
        let meta = cat.get_table_metadata("db", "t").expect("resolve");

        assert_eq!(meta.identity.catalog, "default_catalog");
        assert_eq!(meta.columns.len(), 1);
        assert_eq!(meta.binding, TableBinding::Internal { db_id: 5, table_id: 6 });
    }

    #[test]
    fn missing_table_errors() {
        let inner = InMemoryCatalog::default();
        let cat = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(inner)));
        assert!(cat.get_table_metadata("db", "nope").is_err());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::internal::tests 2>&1 | tail -20`
Expected: 编译失败（`InternalCatalog` 未定义）。

- [ ] **Step 3: 写最小实现**

`src/engine/catalog_mgr/internal.rs` 顶部：

```rust
//! `InternalCatalog`: a `Catalog` over the existing `InMemoryCatalog`, serving
//! local / StarRocks tables (registered at CREATE time, stable schema). Shares
//! the same `InMemoryCatalog` instance as the rest of the engine via `Arc`.

use std::sync::{Arc, RwLock};

use crate::engine::catalog::InMemoryCatalog;
use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};

pub(crate) struct InternalCatalog {
    name: String,
    inner: Arc<RwLock<InMemoryCatalog>>,
}

impl InternalCatalog {
    pub(crate) fn new(name: &str, inner: Arc<RwLock<InMemoryCatalog>>) -> Self {
        Self {
            name: name.to_string(),
            inner,
        }
    }
}

impl Catalog for InternalCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<TableMetadata, String> {
        let td = self
            .inner
            .read()
            .expect("internal catalog read lock")
            .get(namespace, table)?;
        let identity = TableIdentity::new(&self.name, namespace, table);
        TableMetadata::from_table_def(identity, &td)
    }
}
```

注：`InMemoryCatalog::get(database, table)`（`src/engine/catalog.rs:172`）是 `pub(crate)`，返回 `Result<TableDef, String>`，对缺失表返回 `unknown table: ...`，满足第二个测试。

`src/engine/catalog_mgr/mod.rs` 增加：

```rust
pub(crate) mod internal;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::internal::tests 2>&1 | tail -20`
Expected: 2 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add src/engine/catalog_mgr/internal.rs src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add InternalCatalog over InMemoryCatalog"
```

---

## Task 6: `IcebergCatalog`（复用 `CatalogBackend` + `TableSource`，mock 单测）

**Files:**
- Create: `src/engine/catalog_mgr/iceberg.rs`
- Modify: `src/engine/catalog_mgr/mod.rs`

`IcebergCatalog` 持有 `catalog_backend` + `table_source` + `SchemaCache`。`get_table_metadata` = `cache.get_or_build_validated(id, None, || backend.load_table(...) → source.build_table_def(...) → from_table_def)`。P1 传 `current_schema_id = None`（不做远程 probe，留 P3）。单测用 mock `CatalogBackend`/`TableSource`，不依赖真实 Iceberg。

- [ ] **Step 1: 写失败测试**

在 `src/engine/catalog_mgr/iceberg.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::backend::{CatalogBackend, CreateTableRequest, ResolvedTable, TableSource};
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::TableBinding;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    use arrow::datatypes::DataType;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct MockBackend {
        loads: Arc<AtomicUsize>,
    }
    impl CatalogBackend for MockBackend {
        fn name(&self) -> &'static str {
            "iceberg"
        }
        fn namespace_exists(&self, _c: &str, _n: &str) -> Result<bool, String> {
            unimplemented!()
        }
        fn create_namespace(&self, _c: &str, _n: &str) -> Result<(), String> {
            unimplemented!()
        }
        fn drop_namespace(&self, _c: &str, _n: &str, _f: bool) -> Result<(), String> {
            unimplemented!()
        }
        fn create_table(&self, _req: CreateTableRequest) -> Result<(), String> {
            unimplemented!()
        }
        fn table_exists(&self, _c: &str, _n: &str, _t: &str) -> Result<bool, String> {
            unimplemented!()
        }
        fn alter_iceberg_partition_spec(
            &self,
            _c: &str,
            _n: &str,
            _t: &str,
            _s: AlterIcebergPartitionSpecStmt,
        ) -> Result<(), String> {
            unimplemented!()
        }
        fn drop_table(&self, _c: &str, _n: &str, _t: &str, _e: bool) -> Result<(), String> {
            unimplemented!()
        }
        fn load_table(
            &self,
            catalog: &str,
            namespace: &str,
            table: &str,
        ) -> Result<ResolvedTable, String> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(ResolvedTable {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                columns: vec![ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
            })
        }
    }

    struct MockSource;
    impl TableSource for MockSource {
        fn name(&self) -> &'static str {
            "iceberg"
        }
        fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            Ok(TableDef {
                name: table.table.clone(),
                columns: table.columns.clone(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: table.catalog.clone(),
                        namespace: table.namespace.clone(),
                        table: table.table.clone(),
                        table_uuid: None,
                        current_snapshot_id: Some(1),
                        schema_id: 0,
                        location: "s3://w/t".to_string(),
                        schema: IcebergSchemaDef { fields: vec![] },
                        serialized_metadata: None,
                    },
                    files: vec![],
                    cloud_properties: Default::default(),
                },
            })
        }
    }

    #[test]
    fn resolves_iceberg_table_and_caches() {
        let loads = Arc::new(AtomicUsize::new(0));
        let cat = IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend { loads: Arc::clone(&loads) }),
            Arc::new(MockSource),
        );

        let meta = cat.get_table_metadata("ns", "t").expect("resolve");
        assert_eq!(meta.identity.catalog, "ice");
        assert_eq!(meta.columns.len(), 1);
        assert!(matches!(meta.binding, TableBinding::Iceberg { .. }));

        // Second resolve must hit the cache (no extra backend load).
        let _ = cat.get_table_metadata("ns", "t").expect("hit");
        assert_eq!(loads.load(Ordering::SeqCst), 1, "second resolve must hit cache");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib catalog_mgr::iceberg::tests 2>&1 | tail -20`
Expected: 编译失败（`IcebergCatalog` 未定义）。

- [ ] **Step 3: 写最小实现**

`src/engine/catalog_mgr/iceberg.rs` 顶部：

```rust
//! `IcebergCatalog`: a `Catalog` over an Iceberg backend. Resolves schema-level
//! metadata via the existing `CatalogBackend` + `TableSource` abstractions and
//! caches it in a `SchemaCache`. Scan-binding (data files) is NOT resolved here
//! — it happens at codegen time (P2). P1 passes `current_schema_id = None`
//! (no remote schema probe yet); the probe is wired in P3.

use std::sync::Arc;

use crate::connector::backend::{CatalogBackend, TableSource};
use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};
use crate::engine::catalog_mgr::schema_cache::SchemaCache;

pub(crate) struct IcebergCatalog {
    name: String,
    backend: Arc<dyn CatalogBackend>,
    source: Arc<dyn TableSource>,
    cache: SchemaCache,
}

impl IcebergCatalog {
    pub(crate) fn new(
        name: &str,
        backend: Arc<dyn CatalogBackend>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        Self {
            name: name.to_string(),
            backend,
            source,
            cache: SchemaCache::new(),
        }
    }

    /// Drop the cached schema for one table (used by local write/DDL paths in
    /// later phases).
    pub(crate) fn invalidate(&self, namespace: &str, table: &str) {
        let id = TableIdentity::new(&self.name, namespace, table);
        self.cache.invalidate(&id);
    }
}

impl Catalog for IcebergCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<TableMetadata, String> {
        let id = TableIdentity::new(&self.name, namespace, table);
        // P1: current_schema_id = None (no remote probe yet; wired in P3).
        self.cache.get_or_build_validated(&id, None, || {
            let resolved = self.backend.load_table(&self.name, namespace, table)?;
            let td = self.source.build_table_def(&resolved)?;
            TableMetadata::from_table_def(id.clone(), &td)
        })
    }
}
```

`src/engine/catalog_mgr/mod.rs` 增加：

```rust
pub(crate) mod iceberg;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib catalog_mgr::iceberg::tests 2>&1 | tail -20`
Expected: 1 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add src/engine/catalog_mgr/iceberg.rs src/engine/catalog_mgr/mod.rs
git commit -m "feat(catalog-mgr): add IcebergCatalog over CatalogBackend+TableSource with schema cache"
```

---

## Task 7: 全模块编译 + clippy + 整模块测试

**Files:** 无（验证 + 收尾）

- [ ] **Step 1: 整模块测试**

Run: `cargo test --lib catalog_mgr:: 2>&1 | tail -25`
Expected: 全部 PASS（Task 1–6 累计：metadata 3 + catalog 1 + mgr 3 + schema_cache 5 + internal 2 + iceberg 1）。

- [ ] **Step 2: 编译整个 crate（确认无 warning 阻断、未破坏现有代码）**

Run: `cargo build 2>&1 | tail -15`
Expected: 编译成功。`catalog_mgr` 为纯新增模块，不应触及任何现有代码行为。

- [ ] **Step 3: clippy**

Run: `cargo clippy --lib 2>&1 | grep -E "catalog_mgr|warning: |error: " | head -30`
Expected: `catalog_mgr` 模块无 clippy 警告。若有，按提示修正（常见：`#[derive(Default)]` 已加、`to_string` vs `clone` 等）。

- [ ] **Step 4: 确认未接线（grep 验证现有路径未改）**

Run: `git diff --stat HEAD~6 -- src/sql/analyzer src/sql/codegen src/engine/query_prep.rs src/engine/mod.rs`
Expected: 仅 `src/engine/mod.rs` 有改动（且只是新增一行 `pub(crate) mod catalog_mgr;`）；analyzer / codegen / query_prep **零改动**。这验证 P1 是纯骨架、未接线。

- [ ] **Step 5: 最终 commit（若 clippy 有修正）**

```bash
git add -A
git commit -m "chore(catalog-mgr): clippy clean + verify P1 skeleton is unwired"
```

---

## 验收标准（P1）

1. `cargo test --lib catalog_mgr::` 全绿（15 个测试）。
2. `cargo build` 成功，`cargo clippy --lib` 对 `catalog_mgr` 无警告。
3. analyzer / codegen / query_prep 零改动（仅 `src/engine/mod.rs` 加一行模块声明）——即骨架未接线，现有行为完全不变。
4. 新模块边界纪律：`catalog_mgr` 不被 `src/lower/**` 引用（BE 侧零依赖）。可用 `grep -rn "catalog_mgr" src/lower/` 验证为空。

## 后续阶段（不在本计划内）

- **P2**：codegen `visit_scan` 改为现场 `ScanPlanner` 解析当前 snapshot（不再读预存 `ScanSource.files`）。
- **P3**：analyzer 切到 `CatalogMgr.resolve` + `SchemaCache`；为 `IcebergCatalog` 接入真实 `schema_id` probe；移除 `register_external_tables_for_query`（消除并发崩溃）；写入路径接 `invalidate`。
- **P4**：收敛注册表到 `CatalogMgr`；还 lower 层 catalog 旁路的边界债；删死代码。
