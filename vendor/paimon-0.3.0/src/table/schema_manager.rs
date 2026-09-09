// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Schema manager for reading versioned table schemas.
//!
//! Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)

use crate::io::{FileIO, ReadControl, ReadReservation};
use crate::spec::{DataField, DataType, TableSchema};
use futures::future::try_join_all;
use opendal::raw::get_basename;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const SCHEMA_DIR: &str = "schema";
const SCHEMA_PREFIX: &str = "schema-";
const SCHEMA_RETAINED_BASE_BYTES: u64 = 4 * 1024;

#[derive(Debug)]
struct CachedTableSchema {
    schema: Arc<TableSchema>,
}

/// Manager for versioned table schema files.
///
/// Each table stores schema versions as JSON files under `{table_path}/schema/schema-{id}`.
/// When a schema evolution occurs (e.g. ADD COLUMN, ALTER COLUMN TYPE), a new schema file
/// is written with an incremented ID. Data files record which schema they were written with
/// via `DataFileMeta.schema_id`.
///
/// The schema cache is shared across clones via `Arc`, so multiple readers
/// (e.g. parallel split streams) benefit from a single cache.
///
/// Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
#[derive(Debug, Clone)]
pub struct SchemaManager {
    file_io: FileIO,
    table_path: String,
    /// Shared cache of loaded schemas by ID.
    cache: Arc<Mutex<HashMap<i64, CachedTableSchema>>>,
}

impl SchemaManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Path to the schema directory (e.g. `{table_path}/schema`).
    fn schema_directory(&self) -> String {
        format!("{}/{}", self.table_path.trim_end_matches('/'), SCHEMA_DIR)
    }

    /// Create a SchemaManager for a branch of this table.
    pub fn with_branch(&self, branch_name: &str) -> Self {
        let branch_path = format!(
            "{}/branch/branch-{}",
            self.table_path.trim_end_matches('/'),
            branch_name
        );
        Self::new(self.file_io.clone(), branch_path)
    }

    /// Path to a specific schema file (e.g. `{table_path}/schema/schema-0`).
    pub fn schema_path(&self, schema_id: i64) -> String {
        format!("{}/{}{}", self.schema_directory(), SCHEMA_PREFIX, schema_id)
    }

    /// List all schema ids sorted ascending. Returns an empty vector if the
    /// schema directory is missing or empty.
    ///
    /// Mirrors Java [SchemaManager.listAllIds()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn list_all_ids(&self) -> crate::Result<Vec<i64>> {
        let statuses = self
            .file_io
            .list_status_retained(&self.schema_directory())
            .await?;
        let mut ids: Vec<i64> = statuses.map(|statuses| {
            statuses
                .into_iter()
                .filter(|s| !s.is_dir)
                .filter_map(|s| {
                    get_basename(s.path.as_str())
                        .strip_prefix(SCHEMA_PREFIX)?
                        .parse::<i64>()
                        .ok()
                })
                .collect()
        });
        ids.sort_unstable();
        Ok(ids)
    }

    /// List all schemas sorted by id ascending.
    ///
    /// Mirrors Java [SchemaManager.listAll()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn list_all(&self) -> crate::Result<Vec<Arc<TableSchema>>> {
        let ids = self.list_all_ids().await?;
        try_join_all(ids.into_iter().map(|id| self.schema(id))).await
    }

    /// Return the schema with the highest id, or `None` when no schema files
    /// exist under the schema directory.
    ///
    /// Mirrors Java [SchemaManager.latest()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn latest(&self) -> crate::Result<Option<Arc<TableSchema>>> {
        let ids = self.list_all_ids().await?;
        match ids.last() {
            Some(&max_id) => Ok(Some(self.schema(max_id).await?)),
            None => Ok(None),
        }
    }

    /// Load a schema by ID. Returns cached version if available.
    ///
    /// The cache is shared across all clones of this `SchemaManager`, so loading
    /// a schema in one stream makes it available to all other streams reading
    /// from the same table.
    ///
    /// Reference: [SchemaManager.schema(long)](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
    pub async fn schema(&self, schema_id: i64) -> crate::Result<Arc<TableSchema>> {
        // Fast path: check cache under a short lock.
        {
            let cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.get(&schema_id) {
                return Ok(cached.schema.clone());
            }
        }

        // Cache miss — load from file (no lock held during I/O).
        let path = self.schema_path(schema_id);
        let input = self.file_io.new_input(&path)?;
        let bytes = input.read().await?;
        let schema: TableSchema =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to parse schema file: {path}"),
                source: Some(Box::new(e)),
            })?;
        let retained_bytes = estimate_cached_schema_bytes(&schema, bytes.len())?;

        // Check and install while holding the one cache lock. Concurrent misses
        // may perform duplicate I/O and parsing, but only the winner reserves
        // retained bytes and inserts; every caller returns that same entry.
        let mut cache = self.cache.lock().unwrap();
        if let Some(cached) = cache.get(&schema_id) {
            return Ok(cached.schema.clone());
        }
        let control = self.file_io.read_control();
        let reservation = reserve_cached_schema(control.as_ref(), retained_bytes)?;
        cache
            .try_reserve(1)
            .map_err(|error| crate::Error::DataInvalid {
                message: "Schema cache cannot reserve an entry".to_string(),
                source: Some(Box::new(error)),
            })?;
        let mut schema = schema;
        schema.attach_read_reservation(reservation);
        let schema = Arc::new(schema);
        if let Some(control) = control.as_ref() {
            control.checkpoint()?;
        }
        cache.insert(
            schema_id,
            CachedTableSchema {
                schema: schema.clone(),
            },
        );

        Ok(schema)
    }
}

fn reserve_cached_schema(
    control: Option<&Arc<dyn ReadControl>>,
    bytes: u64,
) -> crate::Result<Option<Box<dyn ReadReservation>>> {
    control
        .map(|control| {
            control.checkpoint()?;
            control.try_reserve(bytes.max(1))
        })
        .transpose()
}

fn estimate_cached_schema_bytes(schema: &TableSchema, json_bytes: usize) -> crate::Result<u64> {
    let mut structural = usize_to_u64(size_of::<CachedTableSchema>() + size_of::<TableSchema>())?;
    add_field_slice_bytes(&mut structural, schema.fields())?;
    add_string_slice_bytes(&mut structural, schema.partition_keys())?;
    add_string_slice_bytes(&mut structural, schema.primary_keys())?;
    structural = checked_add(
        structural,
        checked_mul(
            usize_to_u64(schema.options().len())?,
            usize_to_u64(size_of::<(String, String)>() * 2)?,
        )?,
    )?;
    for (key, value) in schema.options() {
        structural = checked_add(structural, usize_to_u64(key.len())?)?;
        structural = checked_add(structural, usize_to_u64(value.len())?)?;
    }
    if let Some(comment) = schema.comment() {
        structural = checked_add(structural, usize_to_u64(comment.len())?)?;
    }

    // Double the walked heap estimate for Vec/HashMap capacity and allocator
    // slack. The JSON-derived floor covers small but syntactically dense
    // schemas whose node overhead dominates their string payload.
    let structural = checked_mul(structural, 2)?;
    let json_floor = checked_add(
        checked_mul(usize_to_u64(json_bytes)?, 8)?,
        SCHEMA_RETAINED_BASE_BYTES,
    )?;
    Ok(structural.max(json_floor).max(1))
}

fn add_field_slice_bytes(total: &mut u64, fields: &[DataField]) -> crate::Result<()> {
    *total = checked_add(
        *total,
        checked_mul(
            usize_to_u64(fields.len())?,
            usize_to_u64(size_of::<DataField>())?,
        )?,
    )?;
    for field in fields {
        *total = checked_add(*total, usize_to_u64(field.name().len())?)?;
        if let Some(description) = field.description() {
            *total = checked_add(*total, usize_to_u64(description.len())?)?;
        }
        add_data_type_heap_bytes(total, field.data_type())?;
    }
    Ok(())
}

fn add_data_type_heap_bytes(total: &mut u64, data_type: &DataType) -> crate::Result<()> {
    match data_type {
        DataType::Array(array) => {
            *total = checked_add(*total, usize_to_u64(size_of::<DataType>())?)?;
            add_data_type_heap_bytes(total, array.element_type())
        }
        DataType::Map(map) => {
            *total = checked_add(
                *total,
                checked_mul(usize_to_u64(size_of::<DataType>())?, 2)?,
            )?;
            add_data_type_heap_bytes(total, map.key_type())?;
            add_data_type_heap_bytes(total, map.value_type())
        }
        DataType::Multiset(multiset) => {
            *total = checked_add(*total, usize_to_u64(size_of::<DataType>())?)?;
            add_data_type_heap_bytes(total, multiset.element_type())
        }
        DataType::Row(row) => add_field_slice_bytes(total, row.fields()),
        DataType::Vector(vector) => {
            *total = checked_add(*total, usize_to_u64(size_of::<DataType>())?)?;
            add_data_type_heap_bytes(total, vector.element_type())
        }
        _ => Ok(()),
    }
}

fn add_string_slice_bytes(total: &mut u64, values: &[String]) -> crate::Result<()> {
    *total = checked_add(
        *total,
        checked_mul(
            usize_to_u64(values.len())?,
            usize_to_u64(size_of::<String>())?,
        )?,
    )?;
    for value in values {
        *total = checked_add(*total, usize_to_u64(value.len())?)?;
    }
    Ok(())
}

fn usize_to_u64(value: usize) -> crate::Result<u64> {
    u64::try_from(value).map_err(|_| schema_size_overflow())
}

fn checked_add(left: u64, right: u64) -> crate::Result<u64> {
    left.checked_add(right).ok_or_else(schema_size_overflow)
}

fn checked_mul(left: u64, right: u64) -> crate::Result<u64> {
    left.checked_mul(right).ok_or_else(schema_size_overflow)
}

fn schema_size_overflow() -> crate::Error {
    crate::Error::DataInvalid {
        message: "Schema cache retained-size estimate overflow".to_string(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIOBuilder, FileStatus, ReadOnlyFileIO};
    use crate::spec::Schema;
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_schema_file(file_io: &FileIO, dir: &str, id: i64) {
        let schema = Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(id, &schema);
        let json = serde_json::to_vec(&table_schema).unwrap();
        let path = format!("{dir}/{SCHEMA_PREFIX}{id}");
        let out = file_io.new_output(&path).unwrap();
        out.write(Bytes::from(json)).await.unwrap();
    }

    fn schema_bytes(id: i64) -> (TableSchema, Bytes) {
        let schema = Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(id, &schema);
        let bytes = Bytes::from(serde_json::to_vec(&table_schema).unwrap());
        (table_schema, bytes)
    }

    #[derive(Debug)]
    struct SchemaReadBackend {
        files: HashMap<String, Bytes>,
    }

    #[async_trait::async_trait]
    impl ReadOnlyFileIO for SchemaReadBackend {
        async fn stat(&self, path: &str) -> crate::Result<FileStatus> {
            let bytes = self
                .files
                .get(path)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("missing test schema: {path}"),
                    source: None,
                })?;
            Ok(FileStatus {
                size: u64::try_from(bytes.len()).unwrap(),
                is_dir: false,
                path: path.to_string(),
                last_modified: None,
            })
        }

        async fn exists(&self, path: &str) -> crate::Result<bool> {
            Ok(self.files.contains_key(path))
        }

        async fn read(&self, path: &str, range: Range<u64>) -> crate::Result<Bytes> {
            tokio::task::yield_now().await;
            let bytes = self
                .files
                .get(path)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("missing test schema: {path}"),
                    source: None,
                })?;
            let start = usize::try_from(range.start).unwrap();
            let end = usize::try_from(range.end).unwrap();
            Ok(bytes.slice(start..end))
        }

        async fn list(
            &self,
            _path: &str,
            _recursive: bool,
        ) -> crate::Result<crate::io::FileStatusStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[derive(Debug)]
    struct BudgetControl {
        retained: Arc<AtomicU64>,
        limit: u64,
        successful_requests: Mutex<Vec<u64>>,
    }

    impl BudgetControl {
        fn new(limit: u64) -> Self {
            Self {
                retained: Arc::new(AtomicU64::new(0)),
                limit,
                successful_requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl ReadControl for BudgetControl {
        fn check_active(&self) -> crate::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> crate::Result<()> {
            Ok(())
        }

        fn try_reserve(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>> {
            self
                .retained
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.limit)
                })
                .map_err(|current| crate::Error::DataInvalid {
                    message: format!(
                        "test schema budget exceeded: current={current}, requested={bytes}, limit={}",
                        self.limit
                    ),
                    source: None,
                })?;
            self.successful_requests.lock().unwrap().push(bytes);
            Ok(Box::new(BudgetReservation {
                bytes,
                retained: self.retained.clone(),
            }))
        }
    }

    #[derive(Debug)]
    struct BudgetReservation {
        bytes: u64,
        retained: Arc<AtomicU64>,
    }

    impl ReadReservation for BudgetReservation {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
            self
        }
    }

    impl Drop for BudgetReservation {
        fn drop(&mut self) {
            let previous = self.retained.fetch_sub(self.bytes, Ordering::SeqCst);
            assert!(previous >= self.bytes);
        }
    }

    fn controlled_schema_manager(
        table_path: &str,
        files: impl IntoIterator<Item = (i64, Bytes)>,
        limit: u64,
    ) -> (SchemaManager, Arc<BudgetControl>) {
        let files = files
            .into_iter()
            .map(|(id, bytes)| {
                (
                    format!("{table_path}/{SCHEMA_DIR}/{SCHEMA_PREFIX}{id}"),
                    bytes,
                )
            })
            .collect();
        let control = Arc::new(BudgetControl::new(limit));
        let read_control: Arc<dyn ReadControl> = control.clone();
        let file_io = FileIO::from_read_only(Arc::new(SchemaReadBackend { files }), read_control);
        (SchemaManager::new(file_io, table_path.to_string()), control)
    }

    #[tokio::test]
    async fn list_all_ids_returns_empty_for_missing_directory() {
        let file_io = memory_file_io();
        let sm = SchemaManager::new(file_io, "memory:/list_missing".to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_all_ids_returns_empty_for_empty_directory() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_empty";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_all_ids_sorts_ascending() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_sorted";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [3, 0, 2, 1] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn list_all_ids_ignores_unrelated_files() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_filter";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_file(&file_io, &dir, 0).await;
        // `schema-foo` starts with the prefix but is not an i64.
        let junk = file_io
            .new_output(&format!("{dir}/{SCHEMA_PREFIX}foo"))
            .unwrap();
        junk.write(Bytes::from("{}")).await.unwrap();
        // A completely unrelated file.
        let other = file_io.new_output(&format!("{dir}/README")).unwrap();
        other.write(Bytes::from("hi")).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn list_all_loads_schemas_in_order() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_all_load";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [0, 2, 1] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let schemas = sm.list_all().await.unwrap();
        let ids: Vec<i64> = schemas.iter().map(|s| s.id()).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn latest_returns_none_when_no_schemas() {
        let file_io = memory_file_io();
        let sm = SchemaManager::new(file_io, "memory:/latest_none".to_string());
        assert!(sm.latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn latest_returns_max_id_schema() {
        let file_io = memory_file_io();
        let table_path = "memory:/latest_max";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [0, 5, 2] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let latest = sm.latest().await.unwrap().expect("latest");
        assert_eq!(latest.id(), 5);
    }

    #[tokio::test]
    async fn controlled_schema_versions_accumulate_until_shared_cache_drop() {
        let table_path = "memory:/controlled-schema-accumulate";
        let (schema0, bytes0) = schema_bytes(0);
        let (schema1, bytes1) = schema_bytes(1);
        let retained0 = estimate_cached_schema_bytes(&schema0, bytes0.len()).unwrap();
        let retained1 = estimate_cached_schema_bytes(&schema1, bytes1.len()).unwrap();
        let transient = u64::try_from(bytes0.len().max(bytes1.len())).unwrap();
        let limit = retained0 + retained1 + transient;
        let (manager, control) =
            controlled_schema_manager(table_path, [(0, bytes0), (1, bytes1)], limit);
        let clone = manager.clone();

        let loaded0 = manager.schema(0).await.unwrap();
        assert_eq!(loaded0.id(), 0);
        assert_eq!(control.retained.load(Ordering::SeqCst), retained0);
        let loaded1 = manager.schema(1).await.unwrap();
        assert_eq!(loaded1.id(), 1);
        assert_eq!(loaded0.retained_read_bytes(), retained0);
        assert_eq!(loaded1.retained_read_bytes(), retained1);
        assert_eq!(
            control.retained.load(Ordering::SeqCst),
            retained0 + retained1
        );

        let owned_clone = loaded0.as_ref().clone();
        drop((manager, clone));
        assert_eq!(
            control.retained.load(Ordering::SeqCst),
            retained0 + retained1
        );
        drop(loaded1);
        assert_eq!(control.retained.load(Ordering::SeqCst), retained0);
        drop(loaded0);
        assert_eq!(control.retained.load(Ordering::SeqCst), retained0);
        drop(owned_clone);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn returned_schema_keeps_lease_after_temporary_manager_cache_drops() {
        let table_path = "memory:/controlled-schema-returned-owner";
        let (schema, bytes) = schema_bytes(3);
        let retained = estimate_cached_schema_bytes(&schema, bytes.len()).unwrap();
        let (manager, control) = controlled_schema_manager(table_path, [(3, bytes)], u64::MAX);

        let returned = manager.schema(3).await.unwrap();
        drop(manager);
        assert_eq!(returned.retained_read_bytes(), retained);
        assert_eq!(control.retained.load(Ordering::SeqCst), retained);

        drop(returned);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn second_schema_version_is_refused_without_leaking_or_evicting_first() {
        let table_path = "memory:/controlled-schema-budget";
        let (schema0, bytes0) = schema_bytes(0);
        let (schema1, bytes1) = schema_bytes(1);
        let retained0 = estimate_cached_schema_bytes(&schema0, bytes0.len()).unwrap();
        let retained1 = estimate_cached_schema_bytes(&schema1, bytes1.len()).unwrap();
        let second_input = u64::try_from(bytes1.len()).unwrap();
        let limit = retained0 + second_input + retained1 - 1;
        let (manager, control) =
            controlled_schema_manager(table_path, [(0, bytes0), (1, bytes1)], limit);

        let loaded0 = manager.schema(0).await.unwrap();
        let error = manager.schema(1).await.unwrap_err();
        assert!(error.to_string().contains("test schema budget exceeded"));
        assert_eq!(control.retained.load(Ordering::SeqCst), retained0);
        assert!(Arc::ptr_eq(&loaded0, &manager.schema(0).await.unwrap()));

        drop(loaded0);
        drop(manager);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_schema_miss_inserts_and_charges_one_cache_entry() {
        let table_path = "memory:/controlled-schema-concurrent";
        let (schema, bytes) = schema_bytes(7);
        let retained = estimate_cached_schema_bytes(&schema, bytes.len()).unwrap();
        let (manager, control) = controlled_schema_manager(table_path, [(7, bytes)], u64::MAX);
        let clone = manager.clone();

        let (left, right) = tokio::join!(manager.schema(7), clone.schema(7));
        let left = left.unwrap();
        let right = right.unwrap();

        assert!(Arc::ptr_eq(&left, &right));
        assert_eq!(control.retained.load(Ordering::SeqCst), retained);
        assert_eq!(
            control
                .successful_requests
                .lock()
                .unwrap()
                .iter()
                .filter(|&&bytes| bytes == retained)
                .count(),
            1
        );

        drop((left, right, manager, clone));
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }
}
