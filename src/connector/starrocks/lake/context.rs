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
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;

use crate::common::ids::SlotId;
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig, StarletShardInfo};
use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
use crate::types;

const TABLET_RUNTIME_PERSIST_DIR: &str = "starust_lake_tablet_runtime";

#[derive(Clone)]
pub(crate) struct TabletRuntimeEntry {
    pub(crate) root_path: String,
    pub(crate) schema: TabletSchemaPb,
    pub(crate) s3_config: Option<S3StoreConfig>,
}

#[derive(Clone, PartialEq, Message)]
struct TabletRuntimeS3ConfigPb {
    #[prost(string, tag = "1")]
    endpoint: String,
    #[prost(string, tag = "2")]
    bucket: String,
    #[prost(string, tag = "3")]
    root: String,
    #[prost(string, tag = "4")]
    access_key_id: String,
    #[prost(string, tag = "5")]
    access_key_secret: String,
    #[prost(string, optional, tag = "6")]
    region: Option<String>,
    #[prost(bool, optional, tag = "7")]
    enable_path_style_access: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct TabletRuntimePersistPb {
    #[prost(string, tag = "1")]
    root_path: String,
    #[prost(message, optional, tag = "2")]
    schema: Option<TabletSchemaPb>,
    #[prost(message, optional, tag = "3")]
    s3_config: Option<TabletRuntimeS3ConfigPb>,
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
    };
    let entry = TabletRuntimeEntry {
        root_path: root_path.clone(),
        schema: ctx.tablet_schema.clone(),
        s3_config,
    };
    {
        let mut guard = tablet_runtime_registry()
            .lock()
            .map_err(|_| "lock tablet runtime registry failed".to_string())?;
        guard.insert(ctx.tablet_id, entry.clone());
    }
    persist_tablet_runtime(ctx.tablet_id, &entry)?;
    sync_runtime_to_shard_registry(ctx.tablet_id, &entry);
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

    let persisted = load_persisted_tablet_runtime(tablet_id)?.ok_or_else(|| {
        format!("tablet runtime is not registered or persisted for tablet_id={tablet_id}")
    })?;
    let mut guard = tablet_runtime_registry()
        .lock()
        .map_err(|_| "lock tablet runtime registry failed".to_string())?;
    guard.insert(tablet_id, persisted.clone());
    sync_runtime_to_shard_registry(tablet_id, &persisted);
    Ok(persisted)
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

    let file = tablet_runtime_persist_file(tablet_id)?;
    if file.exists() {
        fs::remove_file(&file).map_err(|e| {
            format!(
                "remove persisted tablet runtime failed: file={}, error={}",
                file.display(),
                e
            )
        })?;
    }
    Ok(())
}

fn tablet_runtime_persist_file(tablet_id: i64) -> Result<PathBuf, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for runtime persistence file: {tablet_id}"
        ));
    }
    Ok(std::env::temp_dir()
        .join(TABLET_RUNTIME_PERSIST_DIR)
        .join(format!("{tablet_id}.pb")))
}

fn persist_tablet_runtime(tablet_id: i64, entry: &TabletRuntimeEntry) -> Result<(), String> {
    let file = tablet_runtime_persist_file(tablet_id)?;
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("create tablet runtime persist dir failed: {}", e))?;
    }
    let bytes = TabletRuntimePersistPb {
        root_path: entry.root_path.clone(),
        schema: Some(entry.schema.clone()),
        s3_config: entry.s3_config.as_ref().map(|cfg| TabletRuntimeS3ConfigPb {
            endpoint: cfg.endpoint.clone(),
            bucket: cfg.bucket.clone(),
            root: cfg.root.clone(),
            access_key_id: cfg.access_key_id.clone(),
            access_key_secret: cfg.access_key_secret.clone(),
            region: cfg.region.clone(),
            enable_path_style_access: cfg.enable_path_style_access,
        }),
    }
    .encode_to_vec();
    fs::write(&file, bytes).map_err(|e| {
        format!(
            "persist tablet runtime failed: file={}, error={}",
            file.display(),
            e
        )
    })
}

fn load_persisted_tablet_runtime(tablet_id: i64) -> Result<Option<TabletRuntimeEntry>, String> {
    let file = tablet_runtime_persist_file(tablet_id)?;
    if !file.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&file).map_err(|e| {
        format!(
            "read persisted tablet runtime failed: file={}, error={}",
            file.display(),
            e
        )
    })?;
    let persisted = TabletRuntimePersistPb::decode(bytes.as_slice()).map_err(|e| {
        format!(
            "decode persisted tablet runtime failed: file={}, error={}",
            file.display(),
            e
        )
    })?;
    if persisted.root_path.trim().is_empty() {
        return Err(format!(
            "persisted tablet runtime has empty root_path: file={}",
            file.display()
        ));
    }
    let schema = persisted.schema.ok_or_else(|| {
        format!(
            "persisted tablet runtime missing schema: file={}",
            file.display()
        )
    })?;
    let root_path = persisted.root_path.trim_end_matches('/').to_string();
    let s3_config = match persisted.s3_config {
        Some(cfg) => Some(normalize_s3_config(&S3StoreConfig {
            endpoint: cfg.endpoint,
            bucket: cfg.bucket,
            root: cfg.root,
            access_key_id: cfg.access_key_id,
            access_key_secret: cfg.access_key_secret,
            region: cfg.region,
            enable_path_style_access: cfg.enable_path_style_access,
        })?),
        None => None,
    };
    let scheme = classify_scan_paths([root_path.as_str()])?;
    match scheme {
        ScanPathScheme::Local => {
            if s3_config.is_some() {
                return Err(format!(
                    "persisted tablet runtime for local path must not contain S3 config: file={}",
                    file.display()
                ));
            }
        }
        ScanPathScheme::Oss => {
            if s3_config.is_none() {
                return Err(format!(
                    "persisted tablet runtime for object-store path is missing S3 config: file={}",
                    file.display()
                ));
            }
        }
    }
    Ok(Some(TabletRuntimeEntry {
        root_path,
        schema,
        s3_config,
    }))
}

#[cfg(test)]
pub(crate) fn clear_tablet_runtime_cache_for_test() {
    if let Ok(mut guard) = tablet_runtime_registry().lock() {
        guard.clear();
    }
}

#[cfg(test)]
pub(crate) fn clear_persisted_tablet_runtime_for_test(tablet_id: i64) -> Result<(), String> {
    let file = tablet_runtime_persist_file(tablet_id)?;
    if file.exists() {
        fs::remove_file(&file).map_err(|e| {
            format!(
                "remove persisted tablet runtime failed: file={}, error={}",
                file.display(),
                e
            )
        })?;
    }
    Ok(())
}
