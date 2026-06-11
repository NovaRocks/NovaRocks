# MV Dependency Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit MV dependency graph metadata so `REFRESH MATERIALIZED VIEW target_mv` refreshes upstream MV dependencies first and refuses unsafe drops or dependency cycles.

**Architecture:** Store dependency edges in `MvMetaRepository` as `meta_records` with downstream and upstream lookup keys. Keep backend implementations responsible for one MV at a time; put graph resolution, cycle checks, topological ordering, and multi-MV refresh orchestration above them in `src/engine/mv/dependency.rs` and `src/engine/mv_flow.rs`.

**Tech Stack:** Rust, serde JSON metadata payloads, NovaRocks `MetaStoreProvider`, existing `MvBackend` lifecycle, SQL golden runner under `sql-tests/iceberg-ivm`.

---

## File Structure

- Modify `src/meta/repository/mv.rs`
  - Add dependency record types, lookup keys, write/read/delete APIs, reverse-dependency guard helpers.
- Modify `tests/meta_repository.rs`
  - Add repository-level tests for dependency indexing, replacement, deletion, and reverse guards.
- Create `src/engine/mv/dependency.rs`
  - Resolve MV/base dependencies from analyzed MV refs.
  - Convert MV definitions and table targets into dependency object refs.
  - Build upstream MV refresh order with cycle-safe graph traversal.
- Modify `src/engine/mv/mod.rs`
  - Export the new `dependency` module.
- Modify `src/engine/mv/lifecycle.rs`
  - Add `dependencies` to `MvListRow`.
- Modify `src/engine/mv_flow.rs`
  - Load upstream refresh steps before the requested target.
  - Execute a precomputed step list through the existing single-MV lifecycle.
- Modify `src/connector/starrocks/managed/mv_ddl.rs`
  - Use shared dependency resolution during managed-lake MV create.
  - Persist dependency edges in the create transaction.
  - Include `Dependencies` in `SHOW MATERIALIZED VIEWS`.
  - Guard managed-lake MV drop with reverse-dependency checks.
- Modify `src/engine/mv/iceberg_refresh.rs`
  - Use shared dependency resolution during Iceberg MV create.
  - Persist dependency edges in the create transaction.
  - Guard Iceberg MV drop with reverse-dependency checks.
- Modify `src/engine/mv/iceberg_backend.rs`
  - No semantic change expected; verify list/drop/create signatures still compile.
- Modify `src/connector/starrocks/managed/backend.rs`
  - No semantic change expected; verify list/drop/create signatures still compile.
- Modify `src/engine/statement.rs`
  - Guard `DROP TABLE` for Iceberg and managed-lake tables before backend drop.
- Add SQL cases under `sql-tests/iceberg-ivm/sql/` and expected output under `sql-tests/iceberg-ivm/result/`.

---

### Task 1: Add Repository Dependency Records and Index APIs

**Files:**
- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Write failing repository index test**

Append this test to `tests/meta_repository.rs` near other MV repository tests:

```rust
#[test]
fn mv_repository_stores_dependency_indexes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let downstream_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let mv = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "select id from ice.sales.orders".to_string(),
                base_table_refs: vec!["ice.sales.orders".to_string()],
                primary_key_columns: vec![],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("sales".to_string()),
                target_table: Some("orders_mv".to_string()),
                schema_contract: None,
                partition_spec: None,
                created_at_ms: 100,
            },
        )?;
        txn.commit()?;
        mv.mv_id
    };

    let table_ref = MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: "sales".to_string(),
        name: "orders".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    };
    let upstream_mv_ref = MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: "sales".to_string(),
        name: "regional_mv".to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    };

    {
        let mut txn = provider.begin_write("replace mv dependencies")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            downstream_id,
            vec![
                CreateMvDependencyRequest {
                    upstream: table_ref.clone(),
                    created_at_ms: 101,
                },
                CreateMvDependencyRequest {
                    upstream: upstream_mv_ref.clone(),
                    created_at_ms: 102,
                },
            ],
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let by_downstream = repository.list_dependencies_by_downstream(read.as_ref(), downstream_id)?;
    assert_eq!(
        by_downstream
            .iter()
            .map(|dep| dep.upstream.display_name())
            .collect::<Vec<_>>(),
        vec!["ice.sales.orders", "mv:ice.sales.regional_mv"]
    );

    let reverse = repository.list_downstream_dependencies(read.as_ref(), &upstream_mv_ref)?;
    assert_eq!(reverse.len(), 1);
    assert_eq!(reverse[0].downstream_mv_id, downstream_id);
    assert_eq!(reverse[0].upstream, upstream_mv_ref);

    Ok(())
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_stores_dependency_indexes -- --nocapture
```

Expected: FAIL with unresolved imports or missing types such as
`MvDependencyObjectRef`, `CreateMvDependencyRequest`, or
`replace_dependencies_for_mv`.

- [ ] **Step 3: Add dependency public types**

In `src/meta/repository/mv.rs`, extend the imports:

```rust
use std::collections::{BTreeMap, BTreeSet};
```

Add these constants near the existing MV repository constants:

```rust
const MV_DEPENDENCY_KIND: &str = "mv.dependency";
const MV_DEPENDENCY_SCHEMA_VERSION: i32 = 1;
```

Add these types after `CreateMvDefinitionRequest`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyObjectType {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyStorageEngine {
    ManagedLake,
    Iceberg,
    ExternalTable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MvDependencyObjectRef {
    pub catalog: Option<String>,
    pub database_or_namespace: String,
    pub name: String,
    pub object_type: MvDependencyObjectType,
    pub storage_engine: MvDependencyStorageEngine,
}

impl MvDependencyObjectRef {
    pub fn display_name(&self) -> String {
        let object = match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database_or_namespace, self.name),
            None => format!("{}.{}", self.database_or_namespace, self.name),
        };
        match self.object_type {
            MvDependencyObjectType::Table => object,
            MvDependencyObjectType::MaterializedView => format!("mv:{object}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDependency {
    pub downstream_mv_id: i64,
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDependencyRequest {
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: i64,
}
```

- [ ] **Step 4: Add key helpers and encode helpers**

Append these helper functions near the existing `key_by_target` and
`key_refresh` helpers in `src/meta/repository/mv.rs`:

```rust
fn dependency_object_key(object: &MvDependencyObjectRef) -> RepositoryResult<String> {
    let catalog = object
        .catalog
        .as_deref()
        .map(normalize_lookup_name)
        .unwrap_or_else(|| "_".to_string());
    let object_type = match object.object_type {
        MvDependencyObjectType::Table => "table",
        MvDependencyObjectType::MaterializedView => "mv",
    };
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::ManagedLake => "managed_lake",
        MvDependencyStorageEngine::Iceberg => "iceberg",
        MvDependencyStorageEngine::ExternalTable => "external_table",
    };
    Ok(format!(
        "{storage_engine}|{object_type}|{}|{}|{}",
        catalog,
        normalize_lookup_name(&object.database_or_namespace),
        normalize_lookup_name(&object.name)
    ))
}

fn key_dependency_by_downstream(
    downstream_mv_id: i64,
    upstream: &MvDependencyObjectRef,
) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-downstream".to_string(),
            downstream_mv_id.to_string(),
            dependency_object_key(upstream)?,
        ],
    )?)
}

fn key_prefix_dependency_by_downstream(
    downstream_mv_id: i64,
) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-downstream".to_string(),
            downstream_mv_id.to_string(),
        ],
    )?)
}

fn key_dependency_by_upstream(
    upstream: &MvDependencyObjectRef,
    downstream_mv_id: i64,
) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-upstream".to_string(),
            dependency_object_key(upstream)?,
            downstream_mv_id.to_string(),
        ],
    )?)
}

fn key_prefix_dependency_by_upstream(
    upstream: &MvDependencyObjectRef,
) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-upstream".to_string(),
            dependency_object_key(upstream)?,
        ],
    )?)
}

fn decode_dependency_record(record: MetaRecord) -> RepositoryResult<StoredMvDependency> {
    decode_record_payload(&record, MV_DEPENDENCY_KIND, MV_DEPENDENCY_SCHEMA_VERSION)
}

fn put_dependency_indexes(
    txn: &mut dyn MetaWriteTxn,
    dependency: &StoredMvDependency,
) -> RepositoryResult<()> {
    let payload = encode_json_payload(MV_DEPENDENCY_SCHEMA_VERSION, dependency)?;
    txn.put(MetaRecordPut::new(
        key_dependency_by_downstream(dependency.downstream_mv_id, &dependency.upstream)?,
        record_kind(MV_DEPENDENCY_KIND)?,
        ExpectedRevision::Any,
        payload.clone(),
    ))?;
    txn.put(MetaRecordPut::new(
        key_dependency_by_upstream(&dependency.upstream, dependency.downstream_mv_id)?,
        record_kind(MV_DEPENDENCY_KIND)?,
        ExpectedRevision::Any,
        payload,
    ))?;
    Ok(())
}
```

- [ ] **Step 5: Add repository index methods**

Inside `impl MvMetaRepository`, add:

```rust
    pub fn replace_dependencies_for_mv(
        &self,
        txn: &mut dyn MetaWriteTxn,
        downstream_mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        self.delete_dependencies_for_mv(txn, downstream_mv_id)?;

        let mut seen = BTreeSet::new();
        let mut stored = Vec::new();
        for req in dependencies {
            let key = dependency_object_key(&req.upstream)?;
            if !seen.insert(key) {
                continue;
            }
            let dependency = StoredMvDependency {
                downstream_mv_id,
                upstream: req.upstream,
                created_at_ms: req.created_at_ms,
            };
            put_dependency_indexes(txn, &dependency)?;
            stored.push(dependency);
        }
        Ok(stored)
    }

    pub fn list_dependencies_by_downstream(
        &self,
        txn: &dyn MetaReadTxn,
        downstream_mv_id: i64,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        let mut dependencies = txn
            .scan(&key_prefix_dependency_by_downstream(downstream_mv_id)?, None)?
            .into_iter()
            .map(decode_dependency_record)
            .collect::<RepositoryResult<Vec<_>>>()?;
        dependencies.sort_by(|left, right| left.upstream.cmp(&right.upstream));
        Ok(dependencies)
    }

    pub fn list_downstream_dependencies(
        &self,
        txn: &dyn MetaReadTxn,
        upstream: &MvDependencyObjectRef,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        let mut dependencies = txn
            .scan(&key_prefix_dependency_by_upstream(upstream)?, None)?
            .into_iter()
            .map(decode_dependency_record)
            .collect::<RepositoryResult<Vec<_>>>()?;
        dependencies.sort_by_key(|dep| dep.downstream_mv_id);
        Ok(dependencies)
    }
```

- [ ] **Step 6: Run test and verify it passes**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_stores_dependency_indexes -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat(ivm): add MV dependency repository indexes"
```

---

### Task 2: Add Repository Replacement, Deletion, and Reverse Guard Semantics

**Files:**
- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Write failing replacement and guard tests**

Append these tests to `tests/meta_repository.rs`:

```rust
fn iceberg_mv_ref(namespace: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

#[test]
fn mv_repository_replaces_dependencies_and_clears_reverse_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let downstream_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let mv = repository.create_definition(txn.as_mut(), sample_mv_definition_request("select 1"))?;
        txn.commit()?;
        mv.mv_id
    };
    let old_ref = iceberg_mv_ref("sales", "old_mv");
    let new_ref = iceberg_mv_ref("sales", "new_mv");

    {
        let mut txn = provider.begin_write("seed mv dependency")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            downstream_id,
            vec![CreateMvDependencyRequest {
                upstream: old_ref.clone(),
                created_at_ms: 10,
            }],
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("replace mv dependency")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            downstream_id,
            vec![CreateMvDependencyRequest {
                upstream: new_ref.clone(),
                created_at_ms: 11,
            }],
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(repository.list_downstream_dependencies(read.as_ref(), &old_ref)?.is_empty());
    assert_eq!(
        repository
            .list_downstream_dependencies(read.as_ref(), &new_ref)?
            .iter()
            .map(|dep| dep.downstream_mv_id)
            .collect::<Vec<_>>(),
        vec![downstream_id]
    );
    Ok(())
}

#[test]
fn mv_repository_reports_downstream_dependents_for_drop_guard()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let downstream_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let mv = repository.create_definition(txn.as_mut(), sample_mv_definition_request("select 1"))?;
        txn.commit()?;
        mv.mv_id
    };
    let upstream = iceberg_mv_ref("sales", "upstream_mv");

    {
        let mut txn = provider.begin_write("seed dependency")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            downstream_id,
            vec![CreateMvDependencyRequest {
                upstream: upstream.clone(),
                created_at_ms: 12,
            }],
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .ensure_no_downstream_dependencies(read.as_ref(), &upstream)
        .expect_err("upstream should be protected");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    assert!(
        err.to_string()
            .contains("mv:ice.sales.upstream_mv has downstream materialized views")
    );
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_replaces_dependencies_and_clears_reverse_indexes mv_repository_reports_downstream_dependents_for_drop_guard -- --nocapture
```

Expected: FAIL because `delete_dependencies_for_mv` and
`ensure_no_downstream_dependencies` are missing.

- [ ] **Step 3: Add deletion and guard methods**

Inside `impl MvMetaRepository`, add:

```rust
    pub fn delete_dependencies_for_mv(
        &self,
        txn: &mut dyn MetaWriteTxn,
        downstream_mv_id: i64,
    ) -> RepositoryResult<()> {
        let existing = self.list_dependencies_by_downstream(txn, downstream_mv_id)?;
        for dependency in existing {
            txn.delete(
                &key_dependency_by_downstream(dependency.downstream_mv_id, &dependency.upstream)?,
                ExpectedRevision::Any,
            )?;
            txn.delete(
                &key_dependency_by_upstream(&dependency.upstream, dependency.downstream_mv_id)?,
                ExpectedRevision::Any,
            )?;
        }
        Ok(())
    }

    pub fn ensure_no_downstream_dependencies(
        &self,
        txn: &dyn MetaReadTxn,
        upstream: &MvDependencyObjectRef,
    ) -> RepositoryResult<()> {
        let downstream = self.list_downstream_dependencies(txn, upstream)?;
        if downstream.is_empty() {
            return Ok(());
        }
        let mut ids = downstream
            .iter()
            .map(|dep| dep.downstream_mv_id.to_string())
            .collect::<Vec<_>>();
        ids.sort();
        Err(RepositoryError::conflict(format!(
            "{} has downstream materialized views: {}",
            upstream.display_name(),
            ids.join(", ")
        )))
    }
```

- [ ] **Step 4: Update `drop_by_id` and `drop_by_target` to delete dependency edges**

In `drop_by_target`, before deleting the definition record, call:

```rust
        self.delete_dependencies_for_mv(txn, lookup.mv_id)?;
```

In `drop_by_id`, before deleting the definition record, call:

```rust
        self.delete_dependencies_for_mv(txn, mv_id)?;
```

Do not add reverse guard checks inside these repository drop methods. The user
facing layer must call `ensure_no_downstream_dependencies` before deciding the
drop is allowed, because it can produce object names rather than only IDs.

- [ ] **Step 5: Run tests and verify they pass**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_replaces_dependencies_and_clears_reverse_indexes mv_repository_reports_downstream_dependents_for_drop_guard -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat(ivm): maintain MV dependency reverse indexes"
```

---

### Task 3: Add Engine MV Dependency Resolution Module

**Files:**
- Create: `src/engine/mv/dependency.rs`
- Modify: `src/engine/mv/mod.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Write failing unit tests for dependency object conversion**

Create `src/engine/mv/dependency.rs` with tests first:

```rust
use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_ddl::ResolvedTableRef;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::{
    CreateMvDependencyRequest, MvDependencyObjectRef, MvDependencyObjectType,
    MvDependencyStorageEngine, StoredMvDefinition,
};

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<IcebergTableRef>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn iceberg_table_dependency_ref(base: &IcebergTableRef) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(base.catalog.clone()),
        database_or_namespace: base.namespace.clone(),
        name: base.table.clone(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn iceberg_mv_dependency_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_mv_dependency_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::ManagedLake,
    }
}

pub(crate) fn stored_definition_dependency_ref(
    definition: &StoredMvDefinition,
    managed_name: Option<(&str, &str)>,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        let catalog = definition
            .target_catalog
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target catalog".to_string())?;
        let namespace = definition
            .target_namespace
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target namespace".to_string())?;
        let table = definition
            .target_table
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target table".to_string())?;
        return Ok(iceberg_mv_dependency_ref(catalog, namespace, table));
    }
    let (database, table) = managed_name.ok_or_else(|| {
        "managed-lake MV definition requires database/table name for dependency ref".to_string()
    })?;
    Ok(managed_mv_dependency_ref(database, table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_ref_display_distinguishes_table_and_mv() {
        let table = iceberg_table_dependency_ref(&IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        });
        let mv = iceberg_mv_dependency_ref("ice", "sales", "orders_mv");

        assert_eq!(table.display_name(), "ice.sales.orders");
        assert_eq!(mv.display_name(), "mv:ice.sales.orders_mv");
    }
}
```

- [ ] **Step 2: Export module and run test**

Add to `src/engine/mv/mod.rs`:

```rust
pub(crate) mod dependency;
```

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency::tests::dependency_ref_display_distinguishes_table_and_mv -- --nocapture
```

Expected: PASS after adding the module and imports.

- [ ] **Step 3: Add dependency resolution function**

In `src/engine/mv/dependency.rs`, add:

```rust
pub(crate) fn resolve_create_mv_dependencies(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view dependency resolution requires metadata provider".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency metadata read transaction failed: {e}"))?;

    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let base = IcebergTableRef {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some()
                {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::ManagedLake { database, table } => {
                let managed = state
                    .managed_lake
                    .read()
                    .expect("standalone managed lake read lock");
                let runtime = managed.table(database, table).map_err(|err| {
                    format!("resolve managed-lake MV dependency {database}.{table} failed: {err}")
                })?;
                if runtime.table.kind
                    != crate::connector::starrocks::managed::model::ManagedTableKind::MaterializedView
                {
                    return Err(format!(
                        "materialized view base tables must be Iceberg tables or materialized views; found managed lake table `{database}.{table}`"
                    ));
                }
                return Err(format!(
                    "managed-lake MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                ));
            }
        }
    }
    if base_refs.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(ResolvedCreateMvDependencies {
        base_refs,
        dependencies,
    })
}
```

This deliberately recognizes managed-lake MVs but still rejects them as
incremental bases because the existing IVM refresh path expects Iceberg
`base_table_refs`. Iceberg-backed MV-on-MV is the executable B2 slice.

- [ ] **Step 4: Keep old base-ref helper available during transition**

Do not delete `mv_ddl::extract_base_table_refs` in this task. Existing tests
still cover it, and later tasks will replace create-path calls with the shared
dependency resolver.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::extract_base_table_refs_returns_iceberg_fqns -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/dependency.rs src/engine/mv/mod.rs
git commit -m "feat(ivm): add MV dependency resolver"
```

---

### Task 4: Persist Dependencies During CREATE MATERIALIZED VIEW

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Write failing repository drop cleanup test**

Append to `tests/meta_repository.rs`:

```rust
#[test]
fn mv_repository_drop_definition_removes_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let upstream = iceberg_mv_ref("sales", "upstream_mv");
    let downstream_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let mv = repository.create_definition(txn.as_mut(), sample_mv_definition_request("select 1"))?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            mv.mv_id,
            vec![CreateMvDependencyRequest {
                upstream: upstream.clone(),
                created_at_ms: 42,
            }],
        )?;
        txn.commit()?;
        mv.mv_id
    };

    {
        let mut txn = provider.begin_write("drop mv definition")?;
        assert!(repository.drop_by_id(txn.as_mut(), downstream_id)?);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(
        repository
            .list_downstream_dependencies(read.as_ref(), &upstream)?
            .is_empty()
    );
    Ok(())
}
```

- [ ] **Step 2: Run test and verify it passes after Task 2 cleanup**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_drop_definition_removes_dependency_edges -- --nocapture
```

Expected: PASS. If it fails, fix `drop_by_id` and `drop_by_target` to call
`delete_dependencies_for_mv` before deleting the definition record.

- [ ] **Step 3: Wire managed-lake create path**

In `src/connector/starrocks/managed/mv_ddl.rs`, replace this code in
`create_mv`:

```rust
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
```

with:

```rust
    let created_at_ms = now_ms();
    let resolved_dependencies = crate::engine::mv::dependency::resolve_create_mv_dependencies(
        state,
        &analysis.resolved_refs,
        created_at_ms,
    )?;
    let base_refs = resolved_dependencies.base_refs;
```

Later in the same function, in the `CreateMvDefinitionRequest`, replace:

```rust
                created_at_ms: now_ms(),
```

with:

```rust
                created_at_ms,
```

Immediately after `create_definition_with_id` or `create_definition` returns the
stored MV definition, add this before committing the metadata transaction:

```rust
    state
        .mv_repo
        .replace_dependencies_for_mv(
            txn.as_mut(),
            mv_definition.mv_id,
            resolved_dependencies.dependencies,
        )
        .map_err(|e| format!("persist materialized view dependencies failed: {e}"))?;
```

Use the actual local variable name returned by the current create-definition
call. If the current code ignores the returned definition, bind it:

```rust
    let mv_definition = state
        .mv_repo
        .create_definition_with_id(...)
        .map_err(|e| format!("persist materialized view definition failed: {e}"))?;
```

- [ ] **Step 4: Wire Iceberg-backed create path**

In `src/engine/mv/iceberg_refresh.rs`, replace:

```rust
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
```

with:

```rust
    let created_at_ms = now_ms();
    let resolved_dependencies = crate::engine::mv::dependency::resolve_create_mv_dependencies(
        state,
        &analysis.resolved_refs,
        created_at_ms,
    )?;
    let base_refs = resolved_dependencies.base_refs;
```

In the metadata transaction, replace the local `created_at_ms = now_ms();` with
the value already captured above. Bind the returned definition:

```rust
        let mv_definition = state
            .mv_repo
            .create_definition(
                txn.as_mut(),
                CreateMvDefinitionRequest {
                    select_sql: canonical_select_query.to_string(),
                    base_table_refs: base_refs.iter().map(IcebergTableRef::fqn).collect(),
                    primary_key_columns: primary_key_columns.clone(),
                    storage_engine: ManagedMvStorageEngine::Iceberg.as_sql_str().to_string(),
                    target_catalog: Some(target.catalog.clone()),
                    target_namespace: Some(target.namespace.clone()),
                    target_table: Some(target.table.clone()),
                    schema_contract: Some(schema_contract.clone()),
                    partition_spec: schema_contract.target.partition.clone(),
                    created_at_ms,
                },
            )
            .map_err(|e| format!("create iceberg MV repository metadata failed: {e}"))?;
        state
            .mv_repo
            .replace_dependencies_for_mv(
                txn.as_mut(),
                mv_definition.mv_id,
                resolved_dependencies.dependencies,
            )
            .map_err(|e| format!("create iceberg MV dependency metadata failed: {e}"))?;
```

- [ ] **Step 5: Run create-path unit tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::iceberg_refresh::tests -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::create_mv_shape_accepts_projection_filter -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/mv_ddl.rs src/engine/mv/iceberg_refresh.rs src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat(ivm): persist MV dependencies on create"
```

---

### Task 5: Add Cycle Detection Before CREATE Commit

**Files:**
- Modify: `src/engine/mv/dependency.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add failing pure graph tests**

Append to `src/engine/mv/dependency.rs` tests:

```rust
#[test]
fn dependency_cycle_detector_rejects_new_back_edge() {
    let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
    let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
    let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
    let existing = vec![
        (mv_a.clone(), vec![mv_b.clone()]),
        (mv_b.clone(), vec![mv_c.clone()]),
    ];

    let err = validate_no_cycle_for_edges(&mv_c, &[mv_a.clone()], &existing)
        .expect_err("c -> a should form a cycle");
    assert_eq!(
        err,
        "dependency cycle detected: mv:ice.sales.mv_c -> mv:ice.sales.mv_a -> mv:ice.sales.mv_b -> mv:ice.sales.mv_c"
    );
}

#[test]
fn dependency_cycle_detector_accepts_dag() {
    let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
    let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
    let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
    let existing = vec![(mv_b.clone(), vec![mv_a.clone()])];

    validate_no_cycle_for_edges(&mv_c, &[mv_b], &existing).expect("dag should be accepted");
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency::tests::dependency_cycle_detector -- --nocapture
```

Expected: FAIL because `validate_no_cycle_for_edges` is missing.

- [ ] **Step 3: Implement pure cycle helper**

Add to `src/engine/mv/dependency.rs`:

```rust
pub(crate) fn validate_no_cycle_for_edges(
    new_target: &MvDependencyObjectRef,
    new_upstreams: &[MvDependencyObjectRef],
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut graph: std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> =
        std::collections::BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }
    graph.insert(new_target.clone(), new_upstreams.to_vec());

    fn visit(
        graph: &std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        node: &MvDependencyObjectRef,
        target: &MvDependencyObjectRef,
        path: &mut Vec<MvDependencyObjectRef>,
    ) -> Option<Vec<MvDependencyObjectRef>> {
        if path.contains(node) {
            return None;
        }
        path.push(node.clone());
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if &upstream == target {
                let mut cycle = path.clone();
                cycle.push(upstream);
                return Some(cycle);
            }
            if upstream.object_type == MvDependencyObjectType::MaterializedView
                && let Some(cycle) = visit(graph, &upstream, target, path)
            {
                return Some(cycle);
            }
        }
        path.pop();
        None
    }

    if let Some(cycle) = visit(graph, new_target, new_target, &mut Vec::new()) {
        let display = cycle
            .iter()
            .map(MvDependencyObjectRef::display_name)
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(format!("dependency cycle detected: {display}"));
    }
    Ok(())
}
```

- [ ] **Step 4: Add repository-backed cycle validation**

Add to `src/engine/mv/dependency.rs`:

```rust
pub(crate) fn validate_no_create_cycle(
    state: &Arc<StandaloneState>,
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for dependency cycle check failed: {e}"))?;
    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref(&definition, None)?;
        let dependencies = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for cycle check failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, dependencies));
    }
    let new_upstreams = new_dependencies
        .iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream.clone())
        .collect::<Vec<_>>();
    validate_no_cycle_for_edges(new_target, &new_upstreams, &edges)
}
```

Before `validate_no_create_cycle`, add this helper and use it instead of calling
`stored_definition_dependency_ref(&definition, None)` directly:

```rust
fn stored_definition_dependency_ref_from_state(
    state: &Arc<StandaloneState>,
    definition: &StoredMvDefinition,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return stored_definition_dependency_ref(definition, None);
    }
    let managed = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let table = managed
        .snapshot
        .tables
        .iter()
        .find(|table| table.table_id == definition.mv_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime table metadata",
                definition.mv_id
            )
        })?;
    let database = managed
        .snapshot
        .databases
        .iter()
        .find(|database| database.db_id == table.db_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime database metadata",
                definition.mv_id
            )
        })?;
    stored_definition_dependency_ref(definition, Some((&database.name, &table.name)))
}
```

Then replace both occurrences of:

```rust
        let target = stored_definition_dependency_ref(&definition, None)?;
```

with:

```rust
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
```

- [ ] **Step 5: Call cycle validation from both create paths**

For Iceberg create, build the new target ref:

```rust
    let dependency_target = crate::engine::mv::dependency::iceberg_mv_dependency_ref(
        &target.catalog,
        &target.namespace,
        &target.table,
    );
    crate::engine::mv::dependency::validate_no_create_cycle(
        state,
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| {
        format!(
            "cannot create materialized view {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;
```

For managed-lake create, after resolving `(db_name, mv_name)`:

```rust
    let dependency_target =
        crate::engine::mv::dependency::managed_mv_dependency_ref(&db_name, &mv_name);
    crate::engine::mv::dependency::validate_no_create_cycle(
        state,
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| format!("cannot create materialized view {db_name}.{mv_name}: {e}"))?;
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency -- --nocapture
cargo test -p novarocks --lib engine::mv::iceberg_refresh::tests::create_iceberg_mv -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/dependency.rs src/connector/starrocks/managed/mv_ddl.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat(ivm): reject MV dependency cycles"
```

---

### Task 6: Orchestrate Upstream Pull Refresh in mv_flow

**Files:**
- Modify: `src/engine/mv/dependency.rs`
- Modify: `src/engine/mv_flow.rs`

- [ ] **Step 1: Add failing topological order tests**

Append to `src/engine/mv/dependency.rs` tests:

```rust
#[test]
fn topological_upstream_order_runs_deepest_first() {
    let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
    let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
    let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
    let edges = vec![
        (mv_b.clone(), vec![mv_a.clone()]),
        (mv_c.clone(), vec![mv_b.clone()]),
    ];

    let order = topological_upstream_order_for_edges(&mv_c, &edges).expect("order");
    assert_eq!(order, vec![mv_a, mv_b, mv_c]);
}

#[test]
fn topological_upstream_order_deduplicates_shared_dependencies() {
    let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
    let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
    let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
    let mv_d = iceberg_mv_dependency_ref("ice", "sales", "mv_d");
    let edges = vec![
        (mv_b.clone(), vec![mv_a.clone()]),
        (mv_c.clone(), vec![mv_a.clone()]),
        (mv_d.clone(), vec![mv_b.clone(), mv_c.clone()]),
    ];

    let order = topological_upstream_order_for_edges(&mv_d, &edges).expect("order");
    assert_eq!(order, vec![mv_a, mv_b, mv_c, mv_d]);
}
```

- [ ] **Step 2: Implement pure topological helper**

Add:

```rust
pub(crate) fn topological_upstream_order_for_edges(
    target: &MvDependencyObjectRef,
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<Vec<MvDependencyObjectRef>, String> {
    let mut graph: std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> =
        std::collections::BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }

    let mut permanent = std::collections::BTreeSet::new();
    let mut temporary = std::collections::BTreeSet::new();
    let mut ordered = Vec::new();

    fn visit(
        node: &MvDependencyObjectRef,
        graph: &std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        permanent: &mut std::collections::BTreeSet<MvDependencyObjectRef>,
        temporary: &mut std::collections::BTreeSet<MvDependencyObjectRef>,
        ordered: &mut Vec<MvDependencyObjectRef>,
    ) -> Result<(), String> {
        if permanent.contains(node) {
            return Ok(());
        }
        if !temporary.insert(node.clone()) {
            return Err(format!("dependency cycle detected while planning refresh at {}", node.display_name()));
        }
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if upstream.object_type == MvDependencyObjectType::MaterializedView {
                visit(&upstream, graph, permanent, temporary, ordered)?;
            }
        }
        temporary.remove(node);
        permanent.insert(node.clone());
        ordered.push(node.clone());
        Ok(())
    }

    visit(target, &graph, &mut permanent, &mut temporary, &mut ordered)?;
    Ok(ordered)
}
```

- [ ] **Step 3: Add refresh step model**

In `src/engine/mv/dependency.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvRefreshDependencyStep {
    pub(crate) object: MvDependencyObjectRef,
    pub(crate) target: crate::engine::mv::lifecycle::MvTarget,
    pub(crate) storage_engine: crate::engine::mv::lifecycle::MvStorageEngine,
}
```

Add conversion helpers:

```rust
pub(crate) fn refresh_step_for_dependency_object(
    object: &MvDependencyObjectRef,
) -> Result<MvRefreshDependencyStep, String> {
    if object.object_type != MvDependencyObjectType::MaterializedView {
        return Err(format!("refresh dependency object is not a materialized view: {}", object.display_name()));
    }
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::ManagedLake => crate::engine::mv::lifecycle::MvStorageEngine::ManagedLake,
        MvDependencyStorageEngine::Iceberg => crate::engine::mv::lifecycle::MvStorageEngine::Iceberg,
        MvDependencyStorageEngine::ExternalTable => {
            return Err(format!("external table cannot be refreshed as materialized view: {}", object.display_name()));
        }
    };
    Ok(MvRefreshDependencyStep {
        object: object.clone(),
        target: crate::engine::mv::lifecycle::MvTarget {
            catalog: object.catalog.clone(),
            database: object.database_or_namespace.clone(),
            name: object.name.clone(),
        },
        storage_engine,
    })
}
```

- [ ] **Step 4: Add repository-backed refresh plan builder**

Add a function that loads all MV definitions, converts them to dependency
objects, loads each downstream's MV upstream dependencies, and calls
`topological_upstream_order_for_edges`:

```rust
pub(crate) fn build_upstream_refresh_steps(
    state: &Arc<StandaloneState>,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![refresh_step_for_dependency_object(requested)?]);
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency refresh graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for refresh graph failed: {e}"))?;

    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref(&definition, None)?;
        let upstream_mvs = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for refresh graph failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, upstream_mvs));
    }

    topological_upstream_order_for_edges(requested, &edges)?
        .iter()
        .map(refresh_step_for_dependency_object)
        .collect()
}
```

Use `stored_definition_dependency_ref_from_state(state, &definition)?` in this
function as well, so managed-lake MV definitions are either converted with their
runtime database/table names or fail with the explicit missing-metadata message
defined above.

- [ ] **Step 5: Refactor `mv_flow::refresh_mv` to execute steps**

In `src/engine/mv_flow.rs`, after resolving the requested target and engine,
build the requested dependency object:

```rust
    let requested_object = match engine {
        MvStorageEngine::Iceberg => crate::engine::mv::dependency::iceberg_mv_dependency_ref(
            target.catalog.as_deref().ok_or_else(|| {
                "iceberg MV refresh target missing catalog".to_string()
            })?,
            &target.database,
            &target.name,
        ),
        MvStorageEngine::ManagedLake => {
            crate::engine::mv::dependency::managed_mv_dependency_ref(&target.database, &target.name)
        }
    };
    let steps =
        crate::engine::mv::dependency::build_upstream_refresh_steps(state, &requested_object)?;
```

Replace the single `run_refresh_lifecycle` call with:

```rust
    for step in steps {
        let backend = backend_by_engine(state, step.storage_engine)?;
        let statement = RefreshMaterializedViewStmt {
            name: crate::sql::parser::ast::ObjectName {
                parts: match step.target.catalog.as_deref() {
                    Some(_) => vec![step.target.database.clone(), step.target.name.clone()],
                    None => vec![step.target.name.clone()],
                },
            },
            full: stmt.full,
        };
        let req = RefreshRequest {
            target: step.target.clone(),
            current_catalog: step.target.catalog.clone(),
            current_database: step.target.database.clone(),
            statement,
        };
        if let Err(err) = run_refresh_lifecycle(backend, req) {
            if step.object != requested_object {
                return Err(format!(
                    "cannot refresh materialized view {}: upstream materialized view {} failed: {err}",
                    requested_object.display_name().trim_start_matches("mv:"),
                    step.object.display_name().trim_start_matches("mv:")
                ));
            }
            return Err(err);
        }
    }
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency -- --nocapture
cargo test -p novarocks --lib engine::mv_flow::lifecycle_tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/dependency.rs src/engine/mv_flow.rs
git commit -m "feat(ivm): refresh upstream MV dependencies first"
```

---

### Task 7: Add DROP Guards for MV and Base Tables

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mv/dependency.rs`

- [ ] **Step 1: Add helper for drop guard object refs**

In `src/engine/mv/dependency.rs`, add:

```rust
pub(crate) fn iceberg_table_object_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_table_object_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::ManagedLake,
    }
}

pub(crate) fn ensure_no_downstream_dependencies(
    state: &Arc<StandaloneState>,
    upstream: &MvDependencyObjectRef,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop guard read failed: {e}"))?;
    state
        .mv_repo
        .ensure_no_downstream_dependencies(read.as_ref(), upstream)
        .map_err(|e| e.to_string())
}
```

- [ ] **Step 2: Guard Iceberg MV drop**

In `src/engine/mv/iceberg_refresh.rs`, inside `preflight_iceberg_mv_drop` after
confirming the definition has no refresh in progress, add:

```rust
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(
        state,
        &crate::engine::mv::dependency::iceberg_mv_dependency_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        ),
    )?;
```

- [ ] **Step 3: Guard managed-lake MV drop**

In `src/connector/starrocks/managed/mv_ddl.rs`, inside `drop_mv` after checking
`runtime.table.kind == ManagedTableKind::MaterializedView`, add:

```rust
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(
        state,
        &crate::engine::mv::dependency::managed_mv_dependency_ref(&db_name, &mv_name),
    )?;
```

- [ ] **Step 4: Guard `DROP TABLE`**

In `src/engine/statement.rs`, before `backend.drop_table(...)`, add:

```rust
    let dependency_ref = if target.backend_name == "iceberg" {
        crate::engine::mv::dependency::iceberg_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    } else {
        crate::engine::mv::dependency::managed_table_object_ref(&target.namespace, &target.table)
    };
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(state, &dependency_ref)?;
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository_reports_downstream_dependents_for_drop_guard -- --nocapture
cargo test -p novarocks --lib engine::mv::iceberg_refresh::tests::drop_iceberg_mv -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::drop_managed_mv -- --nocapture
```

Expected: PASS for matching local test names.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/dependency.rs src/engine/mv/iceberg_refresh.rs src/connector/starrocks/managed/mv_ddl.rs src/engine/statement.rs
git commit -m "feat(ivm): protect MV dependencies on drop"
```

---

### Task 8: Expose Dependencies in SHOW MATERIALIZED VIEWS

**Files:**
- Modify: `src/engine/mv/lifecycle.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: existing SHOW MV tests and result files touched by column shape

- [ ] **Step 1: Add field to list row**

In `src/engine/mv/lifecycle.rs`, change `MvListRow` to include:

```rust
    pub dependencies: String,
```

- [ ] **Step 2: Populate dependencies in list rows**

In `src/connector/starrocks/managed/mv_ddl.rs`, add a helper:

```rust
fn dependency_display_for_mv(
    state: &Arc<StandaloneState>,
    mv_id: i64,
) -> Result<String, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(String::new());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency display read failed: {e}"))?;
    let dependencies = state
        .mv_repo
        .list_dependencies_by_downstream(read.as_ref(), mv_id)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}
```

In every `MvListRow` construction, add:

```rust
                dependencies: dependency_display_for_mv(state, mv.mv_id)?,
```

- [ ] **Step 3: Add column to result builder**

In `build_mv_rows_result`, add one `QueryResultColumn`:

```rust
        QueryResultColumn {
            name: "Dependencies".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
```

Add one Arrow field:

```rust
        Field::new("Dependencies", DataType::Utf8, false),
```

Add one array:

```rust
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.dependencies.clone()))
                .collect::<Vec<_>>(),
        )),
```

- [ ] **Step 4: Fix struct literals in tests**

Find missing `dependencies` fields:

```bash
rg -n "MvListRow \\{" src tests
```

For test rows without dependencies, add:

```rust
dependencies: String::new(),
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::show_materialized_views -- --nocapture
cargo test -p novarocks --lib engine::mv_flow::lifecycle_tests -- --nocapture
```

Expected: PASS after expected column assertions are updated.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/lifecycle.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat(ivm): show MV dependencies"
```

---

### Task 9: Add SQL Golden Coverage for Iceberg MV-on-MV

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_mv_dependency_graph.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_mv_dependency_graph.result`

- [ ] **Step 1: Add SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_mv_dependency_graph.sql`:

```sql
-- @uuid
CREATE DATABASE IF NOT EXISTS ${iceberg_catalog}.dep_${uuid0};
USE ${iceberg_catalog}.dep_${uuid0};

CREATE TABLE orders_${uuid0} (
  id BIGINT,
  region STRING,
  amount BIGINT
) PROPERTIES (
  'format-version' = '3',
  'write.row-lineage' = 'true'
);

INSERT INTO orders_${uuid0} VALUES
  (1, 'east', 10),
  (2, 'west', 20);

CREATE MATERIALIZED VIEW mv_orders_${uuid0}
PROPERTIES('storage_engine'='iceberg')
AS SELECT id, region, amount FROM orders_${uuid0};

REFRESH MATERIALIZED VIEW mv_orders_${uuid0};

CREATE MATERIALIZED VIEW mv_region_${uuid0}
PROPERTIES('storage_engine'='iceberg')
AS SELECT region, SUM(amount) AS total_amount, COUNT(*) AS row_count
FROM mv_orders_${uuid0}
GROUP BY region;

SHOW MATERIALIZED VIEWS FROM dep_${uuid0};

REFRESH MATERIALIZED VIEW mv_region_${uuid0};

SELECT region, total_amount, row_count FROM mv_region_${uuid0} ORDER BY region;

INSERT INTO orders_${uuid0} VALUES
  (3, 'east', 7);

REFRESH MATERIALIZED VIEW mv_region_${uuid0};

SELECT region, total_amount, row_count FROM mv_region_${uuid0} ORDER BY region;

DROP MATERIALIZED VIEW mv_orders_${uuid0};

DROP MATERIALIZED VIEW mv_region_${uuid0};
DROP MATERIALIZED VIEW mv_orders_${uuid0};
DROP TABLE orders_${uuid0};
DROP DATABASE dep_${uuid0};
```

- [ ] **Step 2: Record expected result**

Start the generated Iceberg REST environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/novarocks-mv-dependency.log 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' /tmp/novarocks-mv-dependency.log; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    tail -80 /tmp/novarocks-mv-dependency.log
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' /tmp/novarocks-mv-dependency.log
```

Record:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_mv_dependency_graph \
  --mode record
```

Expected: record succeeds and writes
`sql-tests/iceberg-ivm/result/iceberg_ivm_mv_dependency_graph.result`.

- [ ] **Step 3: Verify the recorded case**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_mv_dependency_graph \
  --mode verify
```

Expected: PASS.

- [ ] **Step 4: Stop local server**

```bash
kill "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
```

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_mv_dependency_graph.sql sql-tests/iceberg-ivm/result/iceberg_ivm_mv_dependency_graph.result
git commit -m "test(ivm): cover MV dependency graph refresh"
```

---

### Task 10: Final Validation and Cleanup

**Files:**
- Review all files modified in Tasks 1-9.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: command exits 0.

- [ ] **Step 2: Run repository tests**

Run:

```bash
cargo test -p novarocks --test meta_repository mv_repository -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run MV dependency unit tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::dependency -- --nocapture
cargo test -p novarocks --lib engine::mv_flow::lifecycle_tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run create/drop focused tests**

Run:

```bash
cargo test -p novarocks --lib engine::mv::iceberg_refresh::tests -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run SQL golden verification**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_mv_dependency_graph \
  --mode verify
```

Expected: PASS.

- [ ] **Step 6: Check final diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: only intentional files are modified, or the tree is clean if all
tasks were committed.

- [ ] **Step 7: Confirm there are no remaining uncommitted changes**

```bash
git status --short
```

Expected: no output.
