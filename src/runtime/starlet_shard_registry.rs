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

/// Cluster-level S3 credential profile for native lake tablets.
///
/// This type intentionally does NOT carry any per-tablet/per-path state
/// (e.g. tablet root, partition prefix). A tablet's location lives in
/// [`StarletShardInfo::full_path`]; the OpenDAL operator is built with an
/// empty `root` and `normalize_oss_path` resolves the bucket-relative key
/// from the full_path being accessed. Mixing path state into this struct
/// (as earlier revisions did via `path_prefix`) causes `full_path` and
/// the OpenDAL `root` to drift apart and resolve to the wrong object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct S3StoreConfig {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
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
            // OpenDAL builder.root is intentionally empty: keys are
            // computed as bucket-relative from the full_path being
            // accessed, so the operator can be safely shared across all
            // tablets in the same bucket.
            root: String::new(),
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

#[derive(Default)]
struct ShardRegistry {
    active: HashMap<i64, StarletShardInfo>,
}

static SHARD_INFOS: OnceLock<Mutex<ShardRegistry>> = OnceLock::new();

fn shard_registry() -> &'static Mutex<ShardRegistry> {
    SHARD_INFOS.get_or_init(|| Mutex::new(ShardRegistry::default()))
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
    let Ok(mut guard) = shard_registry().lock() else {
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
        let preserved_s3 = guard.active.get(&tablet_id).and_then(|old| old.s3.clone());
        guard.active.insert(
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
    let Ok(mut guard) = shard_registry().lock() else {
        return 0;
    };
    let mut count = 0usize;
    for tablet_id in tablet_ids {
        if guard.active.remove(&tablet_id).is_some() {
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
    let Ok(guard) = shard_registry().lock() else {
        return HashMap::new();
    };
    let mut selected = HashMap::with_capacity(tablet_ids.len());
    for tablet_id in tablet_ids {
        if let Some(info) = guard.active.get(tablet_id) {
            selected.insert(*tablet_id, info.clone());
        }
    }
    selected
}

pub(crate) fn select_tablet_ids_by_path(path: &str) -> Vec<i64> {
    let Some(target) = normalize_full_path(path) else {
        return Vec::new();
    };
    let Ok(guard) = shard_registry().lock() else {
        return Vec::new();
    };
    let mut tablet_ids = guard
        .active
        .iter()
        .filter_map(|(tablet_id, info)| (info.full_path == target).then_some(*tablet_id))
        .collect::<Vec<_>>();
    tablet_ids.sort_unstable();
    tablet_ids
}

/// Pick the cluster-level S3 credentials for `path` by scanning the registry
/// for tablets whose `full_path` is a prefix of `path`.
///
/// `S3StoreConfig` no longer carries a `root` field, so the only reason to
/// score by `full_path` length is to break ties when several covering
/// tablets disagree on cluster-level fields — that is a real configuration
/// error which we surface as `None` so the caller fails fast rather than
/// guessing. If all covering tablets agree we return that single config.
pub(crate) fn find_s3_config_for_path(path: &str) -> Option<S3StoreConfig> {
    let target = normalize_full_path(path)?;
    let guard = shard_registry().lock().ok()?;
    let mut candidate: Option<S3StoreConfig> = None;
    for info in guard.active.values() {
        let s3 = match info.s3.as_ref() {
            Some(v) => v,
            None => continue,
        };
        if !path_covers(&info.full_path, &target) {
            continue;
        }
        match candidate.as_ref() {
            None => candidate = Some(s3.clone()),
            Some(existing) if existing == s3 => {}
            Some(_) => return None,
        }
    }
    candidate
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
                expected AddShard/runtime registry/StarManager retrieval to provide S3 credentials"
            )
        })
}

/// Pick the cluster-level S3 credentials for `path` when no tablet's
/// `full_path` covers it (e.g. directory probes, metadata scans, sibling
/// partition paths produced by FE planning).
///
/// Falls back to any registry entry whose bucket matches the requested
/// path's bucket — but only if every same-bucket entry agrees on the full
/// cluster profile. If they disagree we return `None` so the caller fails
/// fast instead of inheriting another tablet's path state. Unlike older
/// revisions there is **no** "unique cluster profile when bucket is
/// unknown" fallback: a path without an `s3://`/`oss://` scheme means the
/// caller is asking the wrong question, not that we should guess.
pub(crate) fn infer_s3_config_for_path(path: &str) -> Option<S3StoreConfig> {
    if let Some(cfg) = find_s3_config_for_path(path) {
        return Some(cfg);
    }

    let target_bucket = parse_bucket_from_object_store_path(path)?;
    let guard = shard_registry().lock().ok()?;
    let mut candidate: Option<S3StoreConfig> = None;
    for info in guard.active.values() {
        let Some(cfg) = info.s3.as_ref() else {
            continue;
        };
        if cfg.bucket != target_bucket {
            continue;
        }
        match candidate.as_ref() {
            None => candidate = Some(cfg.clone()),
            Some(existing) if existing == cfg => {}
            Some(_) => return None,
        }
    }
    candidate
}

#[cfg(test)]
pub(crate) fn clear_for_test() {
    if let Ok(mut guard) = shard_registry().lock() {
        guard.active.clear();
    }
}

#[cfg(test)]
pub(crate) fn lock_for_test() -> std::sync::MutexGuard<'static, ()> {
    static TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{
        S3StoreConfig, StarletShardInfo, clear_for_test, find_s3_config_for_path,
        infer_s3_config_for_path, lock_for_test, remove_many, select_infos,
        select_tablet_ids_by_path, upsert_many, upsert_many_infos,
    };

    fn sample_s3_config() -> S3StoreConfig {
        S3StoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "bucket".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: Some("us-east-1".to_string()),
            enable_path_style_access: Some(true),
        }
    }

    #[test]
    fn path_only_upsert_preserves_existing_s3_config() {
        let _guard = lock_for_test();
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
    fn find_s3_config_returns_single_cluster_profile_when_covering_tablets_agree() {
        let _guard = lock_for_test();
        clear_for_test();
        let _ = upsert_many_infos(vec![
            (
                2001,
                StarletShardInfo {
                    full_path: "s3://bucket/root".to_string(),
                    s3: Some(sample_s3_config()),
                },
            ),
            (
                2002,
                StarletShardInfo {
                    full_path: "s3://bucket/root/db1/t1".to_string(),
                    s3: Some(sample_s3_config()),
                },
            ),
        ]);
        let cfg = find_s3_config_for_path("s3://bucket/root/db1/t1/tablet-1/meta/0001.meta")
            .expect("find config for nested path");
        assert_eq!(cfg.bucket, "bucket");
        assert_eq!(cfg.access_key_id, "ak");
    }

    #[test]
    fn find_s3_config_returns_none_when_covering_tablets_disagree_on_cluster_profile() {
        let _guard = lock_for_test();
        clear_for_test();
        let mut conflicting = sample_s3_config();
        conflicting.access_key_id = "ak_other".to_string();
        let _ = upsert_many_infos(vec![
            (
                2101,
                StarletShardInfo {
                    full_path: "s3://bucket/root".to_string(),
                    s3: Some(sample_s3_config()),
                },
            ),
            (
                2102,
                StarletShardInfo {
                    full_path: "s3://bucket/root/db1/t1".to_string(),
                    s3: Some(conflicting),
                },
            ),
        ]);
        assert!(
            find_s3_config_for_path("s3://bucket/root/db1/t1/tablet-1/meta/0001.meta").is_none(),
            "conflicting cluster profiles must surface as None instead of a guess"
        );
    }

    #[test]
    fn infer_s3_config_uses_bucket_when_path_prefix_does_not_match() {
        let _guard = lock_for_test();
        clear_for_test();
        let bucket = "bucket-infer-3001";
        let _ = upsert_many_infos(vec![(
            3001,
            StarletShardInfo {
                full_path: format!("s3://{bucket}/root/db1/t1/p1"),
                s3: Some(S3StoreConfig {
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    bucket: bucket.to_string(),
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

    #[test]
    fn infer_s3_config_returns_none_when_bucket_unknown() {
        let _guard = lock_for_test();
        clear_for_test();
        let _ = upsert_many_infos(vec![(
            3101,
            StarletShardInfo {
                full_path: "s3://known-bucket/root/tablet-3101".to_string(),
                s3: Some(sample_s3_config()),
            },
        )]);
        assert!(
            infer_s3_config_for_path("s3://other-bucket/root/foo").is_none(),
            "unknown bucket must never inherit another bucket's credentials"
        );
    }

    #[test]
    fn infer_s3_config_returns_none_when_same_bucket_profiles_disagree() {
        let _guard = lock_for_test();
        clear_for_test();
        let bucket = "shared-bucket";
        let mut conflicting = sample_s3_config();
        conflicting.bucket = bucket.to_string();
        let mut base = sample_s3_config();
        base.bucket = bucket.to_string();
        conflicting.access_key_id = "ak_other".to_string();
        let _ = upsert_many_infos(vec![
            (
                3201,
                StarletShardInfo {
                    full_path: format!("s3://{bucket}/p1"),
                    s3: Some(base),
                },
            ),
            (
                3202,
                StarletShardInfo {
                    full_path: format!("s3://{bucket}/p2"),
                    s3: Some(conflicting),
                },
            ),
        ]);
        assert!(
            infer_s3_config_for_path(&format!("s3://{bucket}/p3/foo")).is_none(),
            "ambiguous cluster profiles for the same bucket must fail fast"
        );
    }

    #[test]
    fn s3_store_config_to_object_store_config_has_empty_root() {
        let cfg = sample_s3_config();
        let object_cfg = cfg.to_object_store_config();
        assert_eq!(
            object_cfg.root, "",
            "S3StoreConfig is cluster-level only; root must always be empty"
        );
        assert_eq!(object_cfg.bucket, "bucket");
    }

    #[test]
    fn full_path_resolution_does_not_strip_anything_beyond_bucket() {
        // Regression: previously the registry could attach one tablet's
        // root onto an unrelated full_path, causing OpenDAL key resolution
        // to silently strip a wrong prefix. The S3StoreConfig profile no
        // longer carries path state, so any tablet path is normalized to a
        // bucket-relative key without depending on cached root state.
        let s3 = sample_s3_config();
        let cfg = s3.to_object_store_config();
        let key = crate::fs::object_store::normalize_oss_path(
            "s3://bucket/brand/new/root/tablet-42/data/seg_0.parquet",
            &cfg.bucket,
            &cfg.root,
        )
        .expect("normalize bucket-relative key");
        assert_eq!(key, "brand/new/root/tablet-42/data/seg_0.parquet");
    }

    #[test]
    fn upsert_preserves_cluster_profile_through_full_path_changes() {
        // Same tablet, two completely different full_paths, both inside
        // the same cluster: the preserved S3 config must keep its cluster
        // identity but never carry the old path back onto the new one.
        let _guard = lock_for_test();
        clear_for_test();
        let initial_path = "s3://bucket/old/root/tablet-9001".to_string();
        let upserted = upsert_many_infos(vec![(
            9001,
            StarletShardInfo {
                full_path: initial_path.clone(),
                s3: Some(sample_s3_config()),
            },
        )]);
        assert_eq!(upserted, 1);

        let new_path = "s3://bucket/totally/different/place/tablet-9001".to_string();
        assert_eq!(upsert_many(vec![(9001, new_path.clone())]), 1);

        let info = select_infos(&[9001])
            .remove(&9001)
            .expect("shard should remain registered after path update");
        assert_eq!(info.full_path, new_path);
        let preserved = info.s3.expect("cluster S3 profile should be preserved");
        assert_eq!(preserved, sample_s3_config());
        let cfg = preserved.to_object_store_config();
        assert_eq!(cfg.root, "");
        let key = crate::fs::object_store::normalize_oss_path(
            &format!("{new_path}/meta/0001.meta"),
            &cfg.bucket,
            &cfg.root,
        )
        .expect("normalize key");
        assert_eq!(key, "totally/different/place/tablet-9001/meta/0001.meta");
    }

    #[test]
    fn oss_config_for_path_fails_fast_when_no_matching_tablet_or_bucket() {
        let _guard = lock_for_test();
        clear_for_test();
        let err = super::oss_config_for_path("s3://unknown-bucket/foo/bar")
            .expect_err("scan/sink path without a known cluster profile must fail fast");
        assert!(err.contains("missing shard registry config"), "err={err}");
    }

    #[test]
    fn select_tablet_ids_by_path_returns_sorted_siblings() {
        let _guard = lock_for_test();
        clear_for_test();
        let shared_path = "s3://bucket/root/db1/p1".to_string();
        upsert_many_infos(vec![
            (
                11,
                StarletShardInfo {
                    full_path: shared_path.clone(),
                    s3: Some(sample_s3_config()),
                },
            ),
            (
                7,
                StarletShardInfo {
                    full_path: shared_path.clone(),
                    s3: Some(sample_s3_config()),
                },
            ),
            (
                19,
                StarletShardInfo {
                    full_path: "s3://bucket/root/db1/p2".to_string(),
                    s3: Some(sample_s3_config()),
                },
            ),
        ]);

        let tablet_ids = select_tablet_ids_by_path(&shared_path);
        assert_eq!(tablet_ids, vec![7, 11]);
    }

    #[test]
    fn remove_many_drops_active_shard_info_immediately() {
        let _guard = lock_for_test();
        clear_for_test();
        let _ = upsert_many_infos(vec![(
            4001,
            StarletShardInfo {
                full_path: "s3://bucket/removed/path".to_string(),
                s3: Some(sample_s3_config()),
            },
        )]);
        assert_eq!(remove_many([4001]), 1);
        assert!(select_infos(&[4001]).is_empty());
    }
}
