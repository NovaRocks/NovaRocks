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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::connector::iceberg::catalog::load_table;
use crate::engine::StandaloneState;
use crate::engine::mv_flow::execute_query_for_mv_refresh_with_catalog;
use crate::exec::chunk::Chunk;
use crate::runtime::query_result::{QueryResult, record_batch_to_chunk};
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn run_mv_full_select_chunks_with_catalog(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    database: &str,
    select_sql: &str,
) -> Result<Vec<Chunk>, String> {
    let result =
        execute_query_for_mv_refresh_with_catalog(state, current_catalog, database, select_sql)?;
    query_result_to_chunks(result)
}

pub(crate) fn query_result_to_chunks(result: QueryResult) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| record_batch_to_chunk(chunk.batch))
        .collect()
}

pub(crate) fn load_current_iceberg_base_table(
    state: &Arc<StandaloneState>,
    table_ref: &TableIdentity,
) -> Result<crate::connector::iceberg::catalog::IcebergLoadedTable, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&table_ref.catalog)?
    };
    entry.invalidate_table_cache(&table_ref.namespace, &table_ref.table);
    load_table(&entry, &table_ref.namespace, &table_ref.table)
}

pub(crate) fn single_snapshot_map(
    table_ref: &TableIdentity,
    snapshot_id: i64,
) -> BTreeMap<String, i64> {
    let mut snapshots = BTreeMap::new();
    snapshots.insert(table_ref.fqn(), snapshot_id);
    snapshots
}

pub(crate) fn single_table_uuid_map(
    table_ref: &TableIdentity,
    table_uuid: &str,
) -> BTreeMap<String, String> {
    let mut uuids = BTreeMap::new();
    uuids.insert(table_ref.fqn(), table_uuid.to_string());
    uuids
}

pub(crate) fn acquire_mv_refresh_lock() -> Result<MutexGuard<'static, ()>, String> {
    static MV_REFRESH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    lock_mv_refresh_mutex(MV_REFRESH_LOCK.get_or_init(|| Mutex::new(())))
}

fn lock_mv_refresh_mutex(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, String> {
    lock.lock()
        .map_err(|_| "materialized view refresh lock poisoned".to_string())
}

pub(crate) fn parse_iceberg_table_refs(refs: &[String]) -> Result<Vec<TableIdentity>, String> {
    refs.iter()
        .map(|fqn| {
            let parts = fqn.split('.').collect::<Vec<_>>();
            let [catalog, namespace, table] = parts.as_slice() else {
                return Err(format!(
                    "materialized view base table reference must be catalog.namespace.table, got `{fqn}`"
                ));
            };
            Ok(TableIdentity {
                catalog: novarocks_catalog::identifier::normalize_identifier(catalog)?,
                namespace: novarocks_catalog::identifier::normalize_identifier(namespace)?,
                table: novarocks_catalog::identifier::normalize_identifier(table)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    #[test]
    fn lock_mv_refresh_mutex_reports_poisoned_lock() {
        let lock: &'static Mutex<()> = Box::leak(Box::new(Mutex::new(())));
        static PANIC_HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _hook_guard = PANIC_HOOK_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("panic hook lock");
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poison_result = std::panic::catch_unwind(|| {
            let _guard = lock.lock().expect("lock");
            panic!("poison test lock");
        });
        std::panic::set_hook(old_hook);
        assert!(poison_result.is_err());

        let err = super::lock_mv_refresh_mutex(lock).expect_err("poisoned lock should fail");
        assert!(err.contains("poisoned"), "err={err}");
    }
}
