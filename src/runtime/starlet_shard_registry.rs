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
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct S3StoreConfig {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) root: String,
    pub(crate) access_key_id: String,
    pub(crate) access_key_secret: String,
    pub(crate) region: Option<String>,
    pub(crate) enable_path_style_access: Option<bool>,
}

impl S3StoreConfig {
    pub(crate) fn to_object_store_config(&self) -> crate::fs::object_store::ObjectStoreConfig {
        let mut cfg = crate::fs::object_store::ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: None,
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        crate::fs::object_store::apply_object_store_runtime_defaults(&mut cfg);
        cfg
    }

    pub(crate) fn to_aws_s3_properties(&self) -> BTreeMap<String, String> {
        let mut props = BTreeMap::new();
        props.insert("aws.s3.endpoint".to_string(), self.endpoint.clone());
        props.insert("aws.s3.accessKeyId".to_string(), self.access_key_id.clone());
        props.insert(
            "aws.s3.accessKeySecret".to_string(),
            self.access_key_secret.clone(),
        );
        if let Some(region) = self.region.as_ref().filter(|v| !v.trim().is_empty()) {
            props.insert("aws.s3.region".to_string(), region.clone());
        }
        if let Some(path_style) = self.enable_path_style_access {
            props.insert(
                "aws.s3.enable_path_style_access".to_string(),
                if path_style { "true" } else { "false" }.to_string(),
            );
        }
        props
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarletShardInfo {
    pub(crate) full_path: String,
    pub(crate) s3: Option<S3StoreConfig>,
}

static SHARD_INFOS: OnceLock<Mutex<HashMap<i64, StarletShardInfo>>> = OnceLock::new();

fn shard_infos() -> &'static Mutex<HashMap<i64, StarletShardInfo>> {
    SHARD_INFOS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_full_path(path: &str) -> Option<String> {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn path_covers(prefix: &str, target: &str) -> bool {
    if target == prefix {
        return true;
    }
    let Some(rest) = target.strip_prefix(prefix) else {
        return false;
    };
    rest.starts_with('/')
}

fn parse_bucket_from_object_store_path(path: &str) -> Option<&str> {
    let trimmed = path.trim();
    let rest = trimmed
        .strip_prefix("s3://")
        .or_else(|| trimmed.strip_prefix("oss://"))?;
    let bucket = rest.split('/').next()?.trim();
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

fn read_nonempty_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn infer_s3_config_from_env(path: &str) -> Option<S3StoreConfig> {
    let bucket = parse_bucket_from_object_store_path(path)?.to_string();
    let access_key_id =
        read_nonempty_env(&["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "MINIO_ROOT_USER"])?;
    let access_key_secret = read_nonempty_env(&[
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "MINIO_ROOT_PASSWORD",
    ])?;
    let endpoint = read_nonempty_env(&[
        "AWS_ENDPOINT_URL_S3",
        "S3_ENDPOINT",
        "OSS_ENDPOINT",
        "MINIO_ENDPOINT",
    ])
    .or_else(|| {
        if std::env::var("MINIO_ROOT_USER").is_ok() && std::env::var("MINIO_ROOT_PASSWORD").is_ok()
        {
            Some("http://localhost:9000".to_string())
        } else {
            None
        }
    })?;
    let region = read_nonempty_env(&["AWS_REGION", "AWS_DEFAULT_REGION"]);
    Some(S3StoreConfig {
        endpoint,
        bucket,
        root: String::new(),
        access_key_id,
        access_key_secret,
        region,
        enable_path_style_access: Some(true),
    })
}

pub(crate) fn upsert_many(entries: impl IntoIterator<Item = (i64, String)>) -> usize {
    let mapped = entries.into_iter().map(|(tablet_id, full_path)| {
        (
            tablet_id,
            StarletShardInfo {
                full_path,
                s3: None,
            },
        )
    });
    upsert_many_infos(mapped)
}

pub(crate) fn upsert_many_infos(
    entries: impl IntoIterator<Item = (i64, StarletShardInfo)>,
) -> usize {
    let Ok(mut guard) = shard_infos().lock() else {
        return 0;
    };
    let mut count = 0usize;
    for (tablet_id, info) in entries {
        if tablet_id <= 0 {
            continue;
        }
        let Some(full_path) = normalize_full_path(&info.full_path) else {
            continue;
        };
        let preserved_s3 = guard.get(&tablet_id).and_then(|old| old.s3.clone());
        guard.insert(
            tablet_id,
            StarletShardInfo {
                full_path,
                s3: info.s3.or(preserved_s3),
            },
        );
        count += 1;
    }
    count
}

pub(crate) fn remove_many(tablet_ids: impl IntoIterator<Item = i64>) -> usize {
    let Ok(mut guard) = shard_infos().lock() else {
        return 0;
    };
    let mut count = 0usize;
    for tablet_id in tablet_ids {
        if guard.remove(&tablet_id).is_some() {
            count += 1;
        }
    }
    count
}

pub(crate) fn select_paths(tablet_ids: &[i64]) -> HashMap<i64, String> {
    let infos = select_infos(tablet_ids);
    let mut selected = HashMap::with_capacity(infos.len());
    for (tablet_id, info) in infos {
        selected.insert(tablet_id, info.full_path);
    }
    selected
}

pub(crate) fn select_infos(tablet_ids: &[i64]) -> HashMap<i64, StarletShardInfo> {
    let Ok(guard) = shard_infos().lock() else {
        return HashMap::new();
    };
    let mut selected = HashMap::with_capacity(tablet_ids.len());
    for tablet_id in tablet_ids {
        if let Some(info) = guard.get(tablet_id) {
            selected.insert(*tablet_id, info.clone());
        }
    }
    selected
}

pub(crate) fn find_s3_config_for_path(path: &str) -> Option<S3StoreConfig> {
    let target = normalize_full_path(path)?;
    let guard = shard_infos().lock().ok()?;
    let mut best: Option<(usize, S3StoreConfig)> = None;
    for info in guard.values() {
        let s3 = match info.s3.as_ref() {
            Some(v) => v,
            None => continue,
        };
        if !path_covers(&info.full_path, &target) {
            continue;
        }
        let score = info.full_path.len();
        match &best {
            Some((best_score, _)) if *best_score >= score => {}
            _ => best = Some((score, s3.clone())),
        }
    }
    best.map(|(_, cfg)| cfg)
}

/// Look up the OSS credentials for a native lake tablet path from the shard registry and
/// return an [`ObjectStoreConfig`] ready for use with
/// [`resolve_oss_operator_and_path_with_config`].
///
/// This is the entry point for the native lake write/read paths.  Iceberg external tables
/// must not call this — they receive credentials from `THdfsScanNode.cloud_configuration`.
pub(crate) fn oss_config_for_path(
    path: &str,
) -> Result<crate::fs::object_store::ObjectStoreConfig, String> {
    find_s3_config_for_path(path)
        .map(|cfg| cfg.to_object_store_config())
        .ok_or_else(|| {
            format!(
                "missing shard registry config for path={path}; \
                expected AddShard or persisted tablet runtime to provide S3 credentials"
            )
        })
}

pub(crate) fn infer_s3_config_for_path(path: &str) -> Option<S3StoreConfig> {
    if let Some(cfg) = find_s3_config_for_path(path) {
        return Some(cfg);
    }

    let target_bucket = parse_bucket_from_object_store_path(path);
    let guard = shard_infos().lock().ok()?;
    let mut unique_cfg: Option<S3StoreConfig> = None;
    let mut has_conflict = false;
    for info in guard.values() {
        let Some(cfg) = info.s3.as_ref() else {
            continue;
        };
        if target_bucket.is_some_and(|bucket| cfg.bucket == bucket) {
            return Some(cfg.clone());
        }
        match unique_cfg.as_ref() {
            None => unique_cfg = Some(cfg.clone()),
            Some(existing) if existing == cfg => {}
            Some(_) => has_conflict = true,
        }
    }
    if !has_conflict {
        if let Some(cfg) = unique_cfg {
            return Some(cfg);
        }
    }
    infer_s3_config_from_env(path)
}

#[cfg(test)]
pub(crate) fn clear_for_test() {
    if let Ok(mut guard) = shard_infos().lock() {
        guard.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        S3StoreConfig, StarletShardInfo, clear_for_test, find_s3_config_for_path,
        infer_s3_config_for_path, upsert_many, upsert_many_infos,
    };

    fn sample_s3_config() -> S3StoreConfig {
        S3StoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "bucket".to_string(),
            root: "lake/root".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: Some("us-east-1".to_string()),
            enable_path_style_access: Some(true),
        }
    }

    #[test]
    fn path_only_upsert_preserves_existing_s3_config() {
        clear_for_test();
        let inserted = upsert_many_infos(vec![(
            1001,
            StarletShardInfo {
                full_path: "s3://bucket/lake/root/tablet-1001".to_string(),
                s3: Some(sample_s3_config()),
            },
        )]);
        assert_eq!(inserted, 1);

        let updated = upsert_many(vec![(
            1001,
            "s3://bucket/lake/root/tablet-1001".to_string(),
        )]);
        assert_eq!(updated, 1);
        let cfg = find_s3_config_for_path("s3://bucket/lake/root/tablet-1001/data/a.parquet")
            .expect("s3 config should still exist");
        assert_eq!(cfg.access_key_id, "ak");
    }

    #[test]
    fn find_s3_config_prefers_longest_path_prefix() {
        clear_for_test();
        let _ = upsert_many_infos(vec![
            (
                2001,
                StarletShardInfo {
                    full_path: "s3://bucket/root".to_string(),
                    s3: Some(S3StoreConfig {
                        endpoint: "http://127.0.0.1:9000".to_string(),
                        bucket: "bucket".to_string(),
                        root: "root".to_string(),
                        access_key_id: "ak_root".to_string(),
                        access_key_secret: "sk_root".to_string(),
                        region: None,
                        enable_path_style_access: Some(true),
                    }),
                },
            ),
            (
                2002,
                StarletShardInfo {
                    full_path: "s3://bucket/root/db1/t1".to_string(),
                    s3: Some(S3StoreConfig {
                        endpoint: "http://127.0.0.1:9000".to_string(),
                        bucket: "bucket".to_string(),
                        root: "root/db1/t1".to_string(),
                        access_key_id: "ak_table".to_string(),
                        access_key_secret: "sk_table".to_string(),
                        region: None,
                        enable_path_style_access: Some(true),
                    }),
                },
            ),
        ]);
        let cfg = find_s3_config_for_path("s3://bucket/root/db1/t1/tablet-1/meta/0001.meta")
            .expect("find config for nested path");
        assert_eq!(cfg.access_key_id, "ak_table");
    }

    #[test]
    fn infer_s3_config_uses_bucket_when_path_prefix_does_not_match() {
        clear_for_test();
        let bucket = "bucket-infer-3001";
        let _ = upsert_many_infos(vec![(
            3001,
            StarletShardInfo {
                full_path: format!("s3://{bucket}/root/db1/t1/p1"),
                s3: Some(S3StoreConfig {
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    bucket: bucket.to_string(),
                    root: "root/db1/t1/p1".to_string(),
                    access_key_id: "ak".to_string(),
                    access_key_secret: "sk".to_string(),
                    region: Some("us-east-1".to_string()),
                    enable_path_style_access: Some(true),
                }),
            },
        )]);
        let cfg = infer_s3_config_for_path(&format!("s3://{bucket}/root/db10001/30806/30808"))
            .expect("infer config by bucket");
        assert_eq!(cfg.bucket, bucket);
        assert_eq!(cfg.access_key_id, "ak");
    }
}
