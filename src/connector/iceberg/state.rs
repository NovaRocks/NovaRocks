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
use std::sync::{Mutex, OnceLock};

use crate::{descriptors, types};

static ICEBERG_TABLE_LOCATIONS: OnceLock<Mutex<HashMap<types::TTableId, String>>> = OnceLock::new();
static ICEBERG_OBJECT_STORE_CONFIGS: OnceLock<
    Mutex<HashMap<String, crate::fs::object_store::ObjectStoreConfig>>,
> = OnceLock::new();

fn iceberg_table_locations() -> &'static Mutex<HashMap<types::TTableId, String>> {
    ICEBERG_TABLE_LOCATIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn iceberg_object_store_configs()
-> &'static Mutex<HashMap<String, crate::fs::object_store::ObjectStoreConfig>> {
    ICEBERG_OBJECT_STORE_CONFIGS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn path_bucket(path: &str) -> Option<String> {
    let trimmed = path.trim();
    for scheme in ["oss://", "s3://"] {
        if let Some(rest) = trimmed.strip_prefix(scheme) {
            let bucket = rest.split('/').next()?.trim();
            if !bucket.is_empty() {
                return Some(bucket.to_string());
            }
        }
    }
    None
}

pub(crate) fn cache_iceberg_table_locations(desc_tbl: Option<&descriptors::TDescriptorTable>) {
    let Some(desc_tbl) = desc_tbl else { return };
    let Some(tables) = desc_tbl.table_descriptors.as_ref() else {
        return;
    };
    let mut guard = iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock");
    for t in tables {
        let Some(iceberg) = t.iceberg_table.as_ref() else {
            continue;
        };
        let Some(location) = iceberg.location.as_ref().filter(|s| !s.is_empty()) else {
            continue;
        };
        guard.insert(t.id, location.clone());
    }
}

pub(crate) fn lookup_iceberg_table_location(table_id: types::TTableId) -> Option<String> {
    iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock")
        .get(&table_id)
        .cloned()
}

pub(crate) fn snapshot_iceberg_table_locations() -> HashMap<types::TTableId, String> {
    iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock")
        .clone()
}

pub(crate) fn cache_iceberg_object_store_config_for_paths<'a, I>(
    cfg: &crate::fs::object_store::ObjectStoreConfig,
    paths: I,
) where
    I: IntoIterator<Item = &'a str>,
{
    let mut buckets = Vec::new();
    if !cfg.bucket.trim().is_empty() {
        buckets.push(cfg.bucket.trim().to_string());
    }
    for path in paths {
        if let Some(bucket) = path_bucket(path) {
            buckets.push(bucket);
        }
    }
    if buckets.is_empty() {
        return;
    }
    let mut guard = iceberg_object_store_configs()
        .lock()
        .expect("iceberg_object_store_configs lock");
    for bucket in buckets {
        let mut cached = cfg.clone();
        cached.bucket = bucket.clone();
        guard.insert(bucket, cached);
    }
}

pub(crate) fn lookup_iceberg_object_store_config_for_path(
    path: &str,
) -> Option<crate::fs::object_store::ObjectStoreConfig> {
    let bucket = path_bucket(path)?;
    iceberg_object_store_configs()
        .lock()
        .expect("iceberg_object_store_configs lock")
        .get(&bucket)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::{
        cache_iceberg_object_store_config_for_paths, lookup_iceberg_object_store_config_for_path,
    };

    fn sample_cfg() -> crate::fs::object_store::ObjectStoreConfig {
        crate::fs::object_store::ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: String::new(),
            root: String::new(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[test]
    fn object_store_cache_resolves_bucket_from_path() {
        let cfg = sample_cfg();
        cache_iceberg_object_store_config_for_paths(
            &cfg,
            ["oss://demo-bucket/iceberg/metadata/v1.metadata.json"],
        );
        let cached = lookup_iceberg_object_store_config_for_path(
            "oss://demo-bucket/iceberg/metadata/snap-1.avro",
        )
        .expect("cached config");
        assert_eq!(cached.bucket, "demo-bucket");
        assert_eq!(cached.endpoint, "http://127.0.0.1:9000");
    }
}
