# Iceberg Hadoop-Style Catalog & Direct S3 Scan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace MemoryCatalog + local-parquet materialization with hadoop-style S3 auto-discovery and direct S3 scan, aligned with StarRocks FE architecture.

**Architecture:** `IcebergCatalogEntry` stores S3 connection config + OpenDAL operator. Tables are discovered lazily from S3 warehouse directory structure (`{warehouse}/{namespace}/{table}/metadata/*.metadata.json`). The execution engine reads parquet files directly from S3 via `TCloudConfiguration` on `THdfsScanNode`, eliminating the download-to-local-parquet step.

**Tech Stack:** Iceberg Rust SDK (`Table::builder()`), OpenDAL S3 operator, existing `THdfsScanRange` + `TCloudConfiguration`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/sql/catalog.rs` | Modify | Add `S3ParquetFiles` variant to `TableStorage` |
| `src/standalone/iceberg.rs` | Major rewrite | Remove `MemoryCatalog`, implement S3-based discovery and `load_table` via `Table::builder()` |
| `src/sql/physical/nodes.rs` | Modify | Handle `S3ParquetFiles` in `build_exec_params`, populate `TCloudConfiguration` |
| `src/standalone/engine.rs` | Modify | Remove `materialize_iceberg_tables_for_query`, replace with direct S3 table registration |
| `src/sql/dialect/create_catalog.rs` | Modify | Support `IF NOT EXISTS` |
| `src/standalone/iceberg_add_files.rs` | Modify | Adapt to new `IcebergCatalogEntry` (no `MemoryCatalog`) |

---

### Task 1: Extend `TableStorage` with S3 variant

**Files:**
- Modify: `src/sql/catalog.rs`

- [ ] **Step 1: Add `S3ParquetFiles` variant**

```rust
// src/sql/catalog.rs
use std::path::PathBuf;

use arrow::datatypes::DataType;

use crate::fs::object_store::ObjectStoreConfig;

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
}

#[derive(Clone, Debug)]
pub enum TableStorage {
    LocalParquetFile { path: PathBuf },
    S3ParquetFiles {
        files: Vec<S3FileInfo>,
        cloud_properties: std::collections::BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub storage: TableStorage,
}

/// Catalog abstraction for SQL analysis.
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;
}
```

Note: Remove `PartialEq` derive from `TableStorage` and `TableDef` since `BTreeMap` fields make it unnecessary and it was only used in optimizer tests — those tests should use `LocalParquetFile`.

- [ ] **Step 2: Fix optimizer test compilation**

In `src/sql/optimizer/mod.rs` tests, the `TableStorage::LocalParquetFile` variant is used and `PartialEq` was derived. Remove the `PartialEq` assertion or implement it manually for `LocalParquetFile` only. The simplest fix: change the `PartialEq` to only exist on the fields that need it, or just remove the `assert_eq!` on `TableDef` in tests and assert on individual fields.

- [ ] **Step 3: Verify compilation**

Run: `cargo build --lib 2>&1 | tail -5`

Expected: Build succeeds (possibly with warnings). Fix any compilation errors from the `TableStorage` change in `nodes.rs` — these will be addressed in Task 3.

---

### Task 2: Rewrite `IcebergCatalogEntry` — remove MemoryCatalog, add S3 discovery

**Files:**
- Modify: `src/standalone/iceberg.rs`

This is the largest task. The new `IcebergCatalogEntry` drops `MemoryCatalog` entirely and implements hadoop-style discovery.

- [ ] **Step 1: Replace `IcebergCatalogEntry` struct**

Remove the `catalog: Arc<MemoryCatalog>` field. Replace with:

```rust
#[derive(Clone, Debug)]
pub(crate) struct IcebergCatalogEntry {
    pub(crate) name: String,
    pub(crate) warehouse_uri: String,
    pub(crate) properties: Vec<(String, String)>,
    /// S3 connection config (None for local filesystem warehouses).
    s3_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    /// Local filesystem warehouse path (for local catalogs).
    pub(crate) warehouse_path: PathBuf,
    /// Cache of loaded tables: (namespace, table_name) -> IcebergLoadedTable
    table_cache: Arc<std::sync::RwLock<HashMap<(String, String), IcebergLoadedTable>>>,
}
```

- [ ] **Step 2: Rewrite `build_memory_catalog` → `build_catalog_entry`**

Remove all `MemoryCatalog` / `MemoryCatalogBuilder` usage. The new function:
1. Parses properties (keep existing validation)
2. Accepts both `hadoop` and `memory` as `iceberg.catalog.type` (or ignores the field)
3. For S3 warehouses: builds `ObjectStoreConfig` from properties
4. For local warehouses: keeps `warehouse_path`
5. Returns `IcebergCatalogEntry` with empty `table_cache`
6. Remove `auto_discover_s3_tables` and `auto_discover_local_tables` calls — discovery is now lazy

- [ ] **Step 3: Rewrite `load_table` — direct S3 metadata read**

The new `load_table` function:
1. Check `table_cache` — return if hit
2. Find latest metadata JSON:
   - S3: use OpenDAL to list `{warehouse}/{ns}/{table}/metadata/*.metadata.json`, pick last sorted
   - Local: use `std::fs::read_dir` (keep existing `latest_table_metadata_location` logic)
3. Read metadata JSON content (via OpenDAL for S3 or `std::fs::read` for local)
4. Deserialize to `iceberg::spec::TableMetadata` via `serde_json`
5. Build `iceberg::table::Table` via `Table::builder().file_io(...).metadata(...).identifier(...).build()`
6. For `FileIO`: use the S3StorageFactory to create a `FileIO` that can read S3 paths
7. Extract columns, logical types, etc. (reuse existing code)
8. Also extract data file info from the Iceberg scan (file paths + sizes) for `S3ParquetFiles`
9. Cache and return

For the `FileIO` construction, use the existing `S3StorageFactory` or build a `FileIO` from the Iceberg SDK that understands S3 paths.

- [ ] **Step 4: Add helper to extract data file info from Iceberg Table**

```rust
/// Extract parquet data file paths and sizes from an Iceberg table's current snapshot.
pub(crate) fn extract_data_files(
    table: &iceberg::table::Table,
) -> Result<Vec<(String, i64)>, String> {
    block_on_iceberg(async {
        let scan = table.scan().build()
            .map_err(|e| format!("build scan: {e}"))?;
        let tasks: Vec<_> = scan.plan_files().await
            .map_err(|e| format!("plan files: {e}"))?
            .try_collect()
            .await
            .map_err(|e| format!("collect tasks: {e}"))?;
        let mut files = Vec::new();
        for task in &tasks {
            files.push((
                task.data_file().file_path().to_string(),
                task.data_file().file_size_in_bytes(),
            ));
        }
        Ok(files)
    })
    .map_err(|e| format!("extract data files runtime: {e}"))?
}
```

- [ ] **Step 5: Remove MemoryCatalog-dependent functions**

Remove or simplify:
- `create_namespace` — no longer needed for query path (keep for CREATE DATABASE DDL)
- `namespace_exists` — can be implemented by listing S3 dirs
- `register_existing_table` — replaced by `load_table` auto-discovery
- `auto_discover_s3_tables` — removed, discovery is lazy
- `auto_discover_local_tables` — removed, discovery is lazy
- `find_latest_s3_metadata` — refactor into `load_table`

Keep for DDL operations:
- `create_table` — needs rewrite to not use MemoryCatalog. For S3 warehouses, write metadata JSON directly.
- `insert_rows` — needs Iceberg Table object (obtained via `load_table`)

- [ ] **Step 6: Verify compilation**

Run: `cargo build --lib 2>&1 | tail -10`

Fix any compilation errors. The `engine.rs` and `iceberg_add_files.rs` references to `entry.catalog` will break — those are fixed in Tasks 4 and 5.

---

### Task 3: Handle `S3ParquetFiles` in physical plan emission

**Files:**
- Modify: `src/sql/physical/nodes.rs`

- [ ] **Step 1: Update `build_exec_params` for S3**

In `build_scan_node` (line ~41), populate `TCloudConfiguration` when table storage is S3:

```rust
// In build_scan_node, replace None::<TCloudConfiguration> with:
let cloud_config = match &resolved.table.storage {
    TableStorage::S3ParquetFiles { cloud_properties, .. } => {
        Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        ))
    }
    _ => None,
};
// Use cloud_config in the THdfsScanNode constructor
```

- [ ] **Step 2: Update `build_exec_params_multi` for S3**

In `build_exec_params_multi` (line ~404), handle `S3ParquetFiles`:

```rust
for (scan_node_id, resolved) in scan_tables {
    let scan_node_id = *scan_node_id;
    match &resolved.table.storage {
        TableStorage::LocalParquetFile { path } => {
            // existing logic: std::fs::metadata, local path
        }
        TableStorage::S3ParquetFiles { files, .. } => {
            // Create one scan range per S3 file
            for file_info in files {
                let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
                    None::<String>,
                    Some(0_i64),
                    Some(file_info.size),
                    None::<i64>,
                    Some(file_info.size),
                    Some(descriptors::THdfsFileFormat::PARQUET),
                    None::<descriptors::TTextFileDesc>,
                    Some(file_info.path.clone()),
                    // ... remaining None fields same as existing
                );
                // Add to per_node_scan_ranges
            }
        }
    }
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build --lib 2>&1 | tail -5`

---

### Task 4: Update `engine.rs` — remove materialization, use direct S3 scan

**Files:**
- Modify: `src/standalone/engine.rs`

- [ ] **Step 1: Replace `materialize_iceberg_tables_for_query` with `register_iceberg_tables_for_query`**

The new function does NOT download data. Instead:
1. Extract table names from query AST (keep existing `extract_table_names_from_query`)
2. For each table not yet in local catalog:
   a. Call `iceberg::load_table(entry, namespace, table_name)` — this lazily discovers from S3
   b. Call `iceberg::extract_data_files(&loaded.table)` — get S3 file paths and sizes
   c. Build `TableDef` with `TableStorage::S3ParquetFiles` using catalog's cloud properties
   d. Register in `InMemoryCatalog`

```rust
fn register_iceberg_tables_for_query(
    &self,
    catalog_name: &str,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    let table_names = extract_table_names_from_query(query);
    if table_names.is_empty() {
        return Ok(());
    }
    let iceberg_guard = self.inner.iceberg_catalogs.read().expect("lock");
    let entry = match iceberg_guard.get(catalog_name) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for table_name in &table_names {
        {
            let local = self.inner.catalog.read().expect("lock");
            if local.get(current_database, table_name).is_ok() {
                continue;
            }
        }
        let loaded = super::iceberg::load_table(&entry, current_database, table_name)?;
        let data_files = super::iceberg::extract_data_files(&loaded.table)?;
        let cloud_properties = entry.cloud_properties_map();

        let table_def = crate::sql::catalog::TableDef {
            name: table_name.clone(),
            columns: loaded.columns,
            storage: crate::sql::catalog::TableStorage::S3ParquetFiles {
                files: data_files.into_iter().map(|(path, size)| {
                    crate::sql::catalog::S3FileInfo { path, size }
                }).collect(),
                cloud_properties,
            },
        };
        let mut guard = self.inner.catalog.write().expect("lock");
        if guard.get(current_database, table_name).is_err() {
            guard.create_database(current_database).ok();
            guard.register(current_database, table_def)
                .map_err(|e| format!("register table: {e}"))?;
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Update query execution path**

In `execute_in_context`, replace the call to `materialize_iceberg_tables_for_query` with `register_iceberg_tables_for_query`.

- [ ] **Step 3: Remove dead code**

Remove `load_full_iceberg_batch`, `write_parquet_to_path` (from engine.rs), `normalize_iceberg_source_batch`, `concat_or_empty_batches`, `apply_iceberg_table_semantics_if_needed`, and any other functions only used by the materialization path.

- [ ] **Step 4: Verify compilation**

Run: `cargo build --lib 2>&1 | tail -10`

---

### Task 5: Fix `CREATE CATALOG IF NOT EXISTS`

**Files:**
- Modify: `src/sql/dialect/create_catalog.rs`

- [ ] **Step 1: Add IF NOT EXISTS parsing**

After consuming `CATALOG` keyword, check for `IF NOT EXISTS`:

```rust
// After parser.next_token(); // consume CATALOG
// Add:
let _if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
```

- [ ] **Step 2: Handle idempotency in `IcebergCatalogRegistry::create_catalog`**

In `src/standalone/iceberg.rs`, change `create_catalog` to return Ok if catalog already exists:

```rust
pub(crate) fn create_catalog(
    &mut self,
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<(), String> {
    let key = normalize_identifier(catalog_name)?;
    if self.catalogs.contains_key(&key) {
        return Ok(()); // idempotent
    }
    let entry = build_catalog_entry(catalog_name, properties)?;
    self.catalogs.insert(key, entry);
    Ok(())
}
```

- [ ] **Step 3: Verify with test**

Run: `cargo test --lib standalone 2>&1 | tail -20`

---

### Task 6: Adapt `iceberg_add_files.rs` to new catalog entry

**Files:**
- Modify: `src/standalone/iceberg_add_files.rs`

- [ ] **Step 1: Update `add_files` to use new catalog entry**

The `add_files` function calls `load_table(entry, ...)` which now uses the new S3-based discovery instead of MemoryCatalog. Since `load_table` no longer returns a table registered in MemoryCatalog, and `Transaction::commit` previously committed to MemoryCatalog, we need to handle this differently.

For ADD FILES in the new architecture: since there's no MemoryCatalog to commit to, the transaction should commit metadata directly to the S3 warehouse (which the `Table::builder()` constructed table's `FileIO` already points to).

Check if `Transaction::commit` requires a `Catalog` reference. If yes, we may need to keep a lightweight catalog proxy or use a different commit path.

If `Transaction::commit` can work without a catalog (direct metadata write), just update the call. Otherwise, create a minimal shim.

- [ ] **Step 2: Verify compilation**

Run: `cargo build --lib 2>&1 | tail -5`

---

### Task 7: Build, run SSB suite, fix issues

**Files:**
- All modified files

- [ ] **Step 1: Full build**

Run: `cargo build --release 2>&1 | tail -10`

Fix any remaining compilation errors.

- [ ] **Step 2: Run unit tests**

Run: `cargo test --lib standalone 2>&1 | tail -30`

Fix any test failures.

- [ ] **Step 3: Start standalone server and setup SSB data**

```bash
# Kill any existing server
pkill -f "novarocks standalone-server" || true

# Start server
./target/release/novarocks standalone-server --port 19030 &
sleep 2

# Create catalog (SSB data already in MinIO from earlier ADD FILES)
mysql -h 127.0.0.1 -P 19030 -u root -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS ssb_cat PROPERTIES (
    'type'='iceberg',
    'iceberg.catalog.type'='hadoop',
    'iceberg.catalog.warehouse'='oss://novarocks/iceberg-catalog/',
    'aws.s3.access_key'='admin',
    'aws.s3.secret_key'='admin123',
    'aws.s3.endpoint'='http://127.0.0.1:9000',
    'aws.s3.enable_path_style_access'='true'
);
"
```

No CREATE TABLE or ADD FILES needed — tables should be auto-discovered lazily.

- [ ] **Step 4: Run SSB Q1.1 (simplest query)**

```bash
mysql -h 127.0.0.1 -P 19030 -u root -e "
SET CATALOG ssb_cat;
USE ssb;
select sum(lo_revenue) as revenue
from lineorder join dates on lo_orderdate = d_datekey
where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;
"
```

Expected: `218453880421`

- [ ] **Step 5: Run all 13 SSB queries**

```bash
PASS=0; FAIL=0
for q in sql-tests/ssb/sql/q*.sql; do
  name=$(basename "$q" .sql)
  expected="sql-tests/ssb/result/${name}.result"
  sql=$(grep -v '^--' "$q")
  actual=$(mysql -h 127.0.0.1 -P 19030 -u root -N -B -e "
SET CATALOG ssb_cat; USE ssb; ${sql}" 2>&1)
  header=$(head -1 "$expected")
  if printf '%s\n%s\n' "$header" "$actual" | diff - "$expected" > /dev/null 2>&1; then
    echo "PASS: $name"; PASS=$((PASS+1))
  else
    echo "FAIL: $name"; FAIL=$((FAIL+1))
  fi
done
echo "Results: ${PASS} passed, ${FAIL} failed"
```

Expected: 13 passed, 0 failed

- [ ] **Step 6: Fix any failures**

Debug and fix issues found in step 5.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: replace MemoryCatalog with hadoop-style S3 discovery and direct S3 scan

- Remove MemoryCatalog from IcebergCatalogEntry
- Implement lazy table discovery from S3 warehouse directory structure
- Add S3ParquetFiles storage variant for direct S3 scan
- Populate TCloudConfiguration so execution engine reads S3 directly
- Eliminate download-to-local-parquet materialization step
- Support CREATE CATALOG IF NOT EXISTS
- All 13 SSB queries pass"
```
