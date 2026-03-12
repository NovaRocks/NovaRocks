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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::common::ids::SlotId;
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig, StarletShardInfo};
use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
use crate::types;

#[derive(Clone)]
pub(crate) struct TabletRuntimeEntry {
    pub(crate) root_path: String,
    pub(crate) schema: TabletSchemaPb,
    pub(crate) s3_config: Option<S3StoreConfig>,
}

static TABLET_RUNTIME_REGISTRY: OnceLock<Mutex<HashMap<i64, TabletRuntimeEntry>>> = OnceLock::new();
static TXN_LOG_APPEND_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn tablet_runtime_registry() -> &'static Mutex<HashMap<i64, TabletRuntimeEntry>> {
    TABLET_RUNTIME_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn txn_log_append_locks() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    TXN_LOG_APPEND_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn with_txn_log_append_lock<T>(
    tablet_id: i64,
    txn_id: i64,
    f: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for txn append lock: {}",
            tablet_id
        ));
    }
    if txn_id <= 0 {
        return Err(format!("invalid txn_id for txn append lock: {}", txn_id));
    }

    let key = format!("{tablet_id}:{txn_id}");
    let lock = {
        let mut guard = txn_log_append_locks()
            .lock()
            .map_err(|_| "lock txn append lock map failed".to_string())?;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _lock_guard = lock
        .lock()
        .map_err(|_| "lock txn append entry failed".to_string())?;
    f()
}

#[derive(Clone, Debug)]
pub(crate) enum PartialUpdateWriteMode {
    Unknown,
    Row,
    ColumnUpsert,
    Auto,
    ColumnUpdate,
}

impl PartialUpdateWriteMode {
    pub(crate) fn from_thrift(mode: Option<types::TPartialUpdateMode>) -> Self {
        match mode {
            Some(types::TPartialUpdateMode::ROW_MODE) => Self::Row,
            Some(types::TPartialUpdateMode::COLUMN_UPSERT_MODE) => Self::ColumnUpsert,
            Some(types::TPartialUpdateMode::AUTO_MODE) => Self::Auto,
            Some(types::TPartialUpdateMode::COLUMN_UPDATE_MODE) => Self::ColumnUpdate,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AutoIncrementWritePolicy {
    pub(crate) null_expr_in_auto_increment: bool,
    pub(crate) miss_auto_increment_column: bool,
    pub(crate) auto_increment_column_idx: Option<usize>,
    pub(crate) auto_increment_column_name: Option<String>,
    pub(crate) auto_increment_in_sort_key: bool,
    pub(crate) fe_addr: Option<types::TNetworkAddress>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartialUpdateWritePolicy {
    pub(crate) mode: PartialUpdateWriteMode,
    pub(crate) merge_condition: Option<String>,
    pub(crate) column_to_expr_value: HashMap<String, String>,
    pub(crate) schema_slot_bindings: Vec<Option<SlotId>>,
    pub(crate) auto_increment: AutoIncrementWritePolicy,
}

impl PartialUpdateWritePolicy {
    pub(crate) fn expr_default_value_for(&self, column_name: &str) -> Option<&str> {
        let key = column_name.trim().to_ascii_lowercase();
        self.column_to_expr_value.get(&key).map(String::as_str)
    }
}

impl Default for PartialUpdateWritePolicy {
    fn default() -> Self {
        Self {
            mode: PartialUpdateWriteMode::Unknown,
            merge_condition: None,
            column_to_expr_value: HashMap::new(),
            schema_slot_bindings: Vec::new(),
            auto_increment: AutoIncrementWritePolicy::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TabletWriteContext {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) tablet_id: i64,
    pub(crate) tablet_root_path: String,
    pub(crate) tablet_schema: TabletSchemaPb,
    pub(crate) s3_config: Option<S3StoreConfig>,
    pub(crate) partial_update: PartialUpdateWritePolicy,
}

fn normalize_s3_config(cfg: &S3StoreConfig) -> Result<S3StoreConfig, String> {
    let endpoint = cfg.endpoint.trim();
    if endpoint.is_empty() {
        return Err("tablet runtime S3 endpoint is empty".to_string());
    }
    let bucket = cfg.bucket.trim();
    if bucket.is_empty() {
        return Err("tablet runtime S3 bucket is empty".to_string());
    }
    let access_key_id = cfg.access_key_id.trim();
    if access_key_id.is_empty() {
        return Err("tablet runtime S3 access_key_id is empty".to_string());
    }
    let access_key_secret = cfg.access_key_secret.trim();
    if access_key_secret.is_empty() {
        return Err("tablet runtime S3 access_key_secret is empty".to_string());
    }
    Ok(S3StoreConfig {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        root: cfg.root.trim_matches('/').to_string(),
        access_key_id: access_key_id.to_string(),
        access_key_secret: access_key_secret.to_string(),
        region: cfg
            .region
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string()),
        enable_path_style_access: cfg.enable_path_style_access,
    })
}

fn sync_runtime_to_shard_registry(tablet_id: i64, entry: &TabletRuntimeEntry) {
    let _ = starlet_shard_registry::upsert_many_infos(vec![(
        tablet_id,
        StarletShardInfo {
            full_path: entry.root_path.clone(),
            s3: entry.s3_config.clone(),
        },
    )]);
}

pub(crate) fn cache_tablet_runtime(
    tablet_id: i64,
    entry: TabletRuntimeEntry,
) -> Result<TabletRuntimeEntry, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for runtime registry: {}",
            tablet_id
        ));
    }
    {
        let mut guard = tablet_runtime_registry()
            .lock()
            .map_err(|_| "lock tablet runtime registry failed".to_string())?;
        guard.insert(tablet_id, entry.clone());
    }
    sync_runtime_to_shard_registry(tablet_id, &entry);
    Ok(entry)
}

pub(crate) fn register_tablet_runtime(ctx: &TabletWriteContext) -> Result<(), String> {
    if ctx.tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for runtime registry: {}",
            ctx.tablet_id
        ));
    }
    let root_path = ctx
        .tablet_root_path
        .trim()
        .trim_end_matches('/')
        .to_string();
    if root_path.is_empty() {
        return Err("tablet_root_path is empty for runtime registry".to_string());
    }
    let scheme = classify_scan_paths([root_path.as_str()])?;
    let s3_config = match scheme {
        ScanPathScheme::Local => {
            if ctx.s3_config.is_some() {
                return Err(format!(
                    "tablet runtime for local path must not include S3 config: tablet_id={} path={}",
                    ctx.tablet_id, root_path
                ));
            }
            None
        }
        ScanPathScheme::Oss => {
            let raw = ctx.s3_config.as_ref().ok_or_else(|| {
                format!(
                    "missing S3 config for object-store tablet runtime: tablet_id={} path={}",
                    ctx.tablet_id, root_path
                )
            })?;
            Some(normalize_s3_config(raw)?)
        }
        ScanPathScheme::Hdfs => {
            return Err(format!(
                "tablet runtime does not support hdfs path yet: tablet_id={} path={}",
                ctx.tablet_id, root_path
            ));
        }
    };
    let entry = TabletRuntimeEntry {
        root_path: root_path.clone(),
        schema: ctx.tablet_schema.clone(),
        s3_config,
    };
    cache_tablet_runtime(ctx.tablet_id, entry)?;
    Ok(())
}

pub(crate) fn get_tablet_runtime(tablet_id: i64) -> Result<TabletRuntimeEntry, String> {
    if tablet_id <= 0 {
        return Err(format!("invalid tablet_id for runtime lookup: {tablet_id}"));
    }

    if let Some(found) = tablet_runtime_registry()
        .lock()
        .map_err(|_| "lock tablet runtime registry failed".to_string())?
        .get(&tablet_id)
        .cloned()
    {
        sync_runtime_to_shard_registry(tablet_id, &found);
        return Ok(found);
    }
    Err(format!(
        "tablet runtime is not registered for tablet_id={tablet_id}"
    ))
}

pub(crate) fn update_tablet_runtime_schema(
    tablet_id: i64,
    schema: &TabletSchemaPb,
) -> Result<(), String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for runtime schema update: {tablet_id}"
        ));
    }

    let updated = {
        let mut guard = tablet_runtime_registry()
            .lock()
            .map_err(|_| "lock tablet runtime registry failed".to_string())?;
        let entry = guard
            .get_mut(&tablet_id)
            .ok_or_else(|| format!("tablet runtime is not registered for tablet_id={tablet_id}"))?;
        entry.schema = schema.clone();
        entry.clone()
    };
    sync_runtime_to_shard_registry(tablet_id, &updated);
    Ok(())
}

pub(crate) fn remove_tablet_runtime(tablet_id: i64) -> Result<(), String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for runtime removal: {tablet_id}"
        ));
    }

    if let Ok(mut guard) = tablet_runtime_registry().lock() {
        guard.remove(&tablet_id);
    }
    let _ = starlet_shard_registry::remove_many([tablet_id]);
    Ok(())
}

#[cfg(test)]
pub(crate) fn clear_tablet_runtime_cache_for_test() {
    if let Ok(mut guard) = tablet_runtime_registry().lock() {
        guard.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TabletWriteContext, clear_tablet_runtime_cache_for_test, get_tablet_runtime,
        register_tablet_runtime,
    };
    use crate::runtime::starlet_shard_registry;
    use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;

    fn local_context(tablet_id: i64, root_path: String) -> TabletWriteContext {
        TabletWriteContext {
            db_id: 1,
            table_id: 2,
            tablet_id,
            tablet_root_path: root_path,
            tablet_schema: TabletSchemaPb::default(),
            s3_config: None,
            partial_update: Default::default(),
        }
    }

    #[test]
    fn runtime_lookup_fails_after_cache_clear_without_persistence() {
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
        let tablet_id = 79_001;
        let root = std::env::temp_dir()
            .join("novarocks_lake_context_test")
            .join(tablet_id.to_string())
            .to_string_lossy()
            .to_string();
        let ctx = local_context(tablet_id, root);
        register_tablet_runtime(&ctx).expect("register runtime");

        let found = get_tablet_runtime(tablet_id).expect("runtime should exist before clear");
        assert_eq!(
            found.root_path,
            ctx.tablet_root_path.trim_end_matches('/').to_string()
        );

        clear_tablet_runtime_cache_for_test();
        let err = match get_tablet_runtime(tablet_id) {
            Ok(_) => panic!("lookup should fail after cache clear"),
            Err(err) => err,
        };
        assert!(
            err.contains("is not registered"),
            "unexpected error after cache clear: {err}"
        );
        starlet_shard_registry::clear_for_test();
    }
}
