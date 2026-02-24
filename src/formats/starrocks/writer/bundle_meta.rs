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

use futures::TryStreamExt;
use prost::Message;

use crate::formats::starrocks::writer::io::read_bytes_if_exists;
use crate::formats::starrocks::writer::io::write_bytes;
use crate::formats::starrocks::writer::layout::{
    BUNDLE_TABLET_ID, META_DIR, bundle_meta_file_path, join_tablet_path,
};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::service::grpc_client::proto::starrocks::{
    BundleTabletMetadataPb, PagePointerPb, RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb,
};

const BUNDLE_METADATA_FOOTER_SIZE: usize = 8;

static BUNDLE_META_WRITE_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn bundle_meta_write_locks() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    BUNDLE_META_WRITE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn with_bundle_meta_write_lock<T>(
    tablet_root_path: &str,
    version: i64,
    f: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if version <= 0 {
        return Err(format!(
            "invalid version for bundle metadata write lock: {}",
            version
        ));
    }
    let root = tablet_root_path.trim().trim_end_matches('/');
    if root.is_empty() {
        return Err("invalid tablet_root_path for bundle metadata write lock".to_string());
    }

    let key = format!("{root}:{version}");
    let lock = {
        let mut guard = bundle_meta_write_locks()
            .lock()
            .map_err(|_| "lock bundle meta write lock map failed".to_string())?;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _lock_guard = lock
        .lock()
        .map_err(|_| "lock bundle meta write entry failed".to_string())?;
    f()
}

pub fn load_tablet_metadata_at_version(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
) -> Result<Option<TabletMetadataPb>, String> {
    if version <= 0 {
        return Ok(Some(empty_tablet_metadata(tablet_id)));
    }
    let path = bundle_meta_file_path(tablet_root_path, version)?;
    let maybe_bytes = read_bytes_if_exists(&path)?;
    let Some(bytes) = maybe_bytes else {
        return Ok(None);
    };
    match decode_tablet_metadata_from_bundle_bytes(&bytes, tablet_id, version) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if is_missing_tablet_page_in_bundle_error(&err) => {
            match load_tablet_metadata_from_previous_versions(tablet_root_path, tablet_id, version)?
            {
                Some(mut recovered) => {
                    recovered.version = Some(version);
                    Ok(Some(recovered))
                }
                None => Err(format!(
                    "bundle metadata does not contain tablet page: tablet_id={}, path={}",
                    tablet_id, path
                )),
            }
        }
        Err(err) => Err(err),
    }
}
pub fn write_bundle_meta_file(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    schema: &TabletSchemaPb,
    tablet_meta: &TabletMetadataPb,
) -> Result<(), String> {
    with_bundle_meta_write_lock(tablet_root_path, version, || {
        let schema_id = schema
            .id
            .filter(|v| *v > 0)
            .ok_or_else(|| "bundle schema id is missing".to_string())?;

        let meta_path = bundle_meta_file_path(tablet_root_path, version)?;
        let mut tablet_metas = HashMap::new();
        let mut tablet_to_schema = HashMap::new();
        let mut schemas = HashMap::new();

        let mut merge_bundle_bytes = |bytes: &[u8],
                                      source: &str,
                                      bump_to_version: bool|
         -> Result<(), String> {
            let (existing_bundle, footer_offset) = decode_bundle_metadata_from_bytes(bytes)?;
            tablet_to_schema.extend(existing_bundle.tablet_to_schema);
            schemas.extend(existing_bundle.schemas);
            for (existing_tablet_id, page) in existing_bundle.tablet_meta_pages {
                let start = page.offset as usize;
                let end = start.saturating_add(page.size as usize);
                if end > footer_offset {
                    return Err(format!(
                        "existing bundle tablet page out of range: source={} tablet_id={} offset={} size={} file_size={}",
                        source, existing_tablet_id, page.offset, page.size, footer_offset
                    ));
                }
                let mut meta = TabletMetadataPb::decode(&bytes[start..end]).map_err(|e| {
                    format!(
                        "decode existing bundle tablet metadata failed: source={} tablet_id={} error={}",
                        source, existing_tablet_id, e
                    )
                })?;
                if bump_to_version {
                    meta.version = Some(version);
                }
                tablet_metas.insert(existing_tablet_id, meta);
            }
            Ok(())
        };

        // For shared partition-root layout, each new bundle version must remain readable by
        // all tablets in the partition. Seed from previous version first, then overlay current.
        if version > 1 {
            let previous_meta_path = bundle_meta_file_path(tablet_root_path, version - 1)?;
            if let Some(previous_bytes) = read_bytes_if_exists(&previous_meta_path)? {
                merge_bundle_bytes(&previous_bytes, &previous_meta_path, true)?;
            }
        }

        if let Some(existing_bytes) = read_bytes_if_exists(&meta_path)? {
            merge_bundle_bytes(&existing_bytes, &meta_path, false)?;
        }

        tablet_metas.insert(tablet_id, tablet_meta.clone());
        tablet_to_schema.insert(tablet_id, schema_id);
        schemas.insert(schema_id, schema.clone());

        let mut ordered_tablet_ids = tablet_metas.keys().copied().collect::<Vec<_>>();
        ordered_tablet_ids.sort_unstable();

        let mut tablet_meta_pages = HashMap::with_capacity(ordered_tablet_ids.len());
        let mut file_bytes = Vec::new();
        for tid in ordered_tablet_ids {
            let meta = tablet_metas
                .remove(&tid)
                .ok_or_else(|| format!("bundle metadata map missing tablet_id={tid}"))?;
            let tablet_meta_bytes = meta.encode_to_vec();
            let offset = file_bytes.len() as u64;
            file_bytes.extend_from_slice(&tablet_meta_bytes);
            tablet_meta_pages.insert(
                tid,
                PagePointerPb {
                    offset,
                    size: tablet_meta_bytes.len() as u32,
                },
            );
        }

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema,
            schemas,
            tablet_meta_pages,
        };
        let bundle_bytes = bundle.encode_to_vec();
        let bundle_size = bundle_bytes.len() as u64;

        file_bytes.extend_from_slice(&bundle_bytes);
        file_bytes.extend_from_slice(&bundle_size.to_le_bytes());
        write_bytes(&meta_path, file_bytes)
    })
}

#[allow(dead_code)]
pub fn load_latest_tablet_metadata(
    tablet_root_path: &str,
    tablet_id: i64,
) -> Result<(i64, TabletMetadataPb), String> {
    let latest_version = discover_latest_bundle_version(tablet_root_path)?;
    let Some(latest_version) = latest_version else {
        return Ok((0, empty_tablet_metadata(tablet_id)));
    };
    if latest_version <= 0 {
        return Ok((0, empty_tablet_metadata(tablet_id)));
    }

    let path = bundle_meta_file_path(tablet_root_path, latest_version)?;
    let maybe_bytes = read_bytes_if_exists(&path)?;
    let Some(bytes) = maybe_bytes else {
        return Ok((0, empty_tablet_metadata(tablet_id)));
    };
    if let Ok(meta) = decode_tablet_metadata_from_bundle_bytes(&bytes, tablet_id, latest_version) {
        return Ok((latest_version, meta));
    }
    Ok((0, empty_tablet_metadata(tablet_id)))
}

#[allow(dead_code)]
pub fn discover_latest_bundle_version(tablet_root_path: &str) -> Result<Option<i64>, String> {
    let meta_root = join_tablet_path(tablet_root_path, META_DIR)?;
    let scheme = classify_scan_paths([meta_root.as_str()])?;
    match scheme {
        ScanPathScheme::Local => discover_latest_bundle_version_local(&meta_root),
        ScanPathScheme::Oss => discover_latest_bundle_version_oss(&meta_root),
    }
}

#[allow(dead_code)]
pub fn discover_latest_bundle_version_local(meta_root: &str) -> Result<Option<i64>, String> {
    let dir = PathBuf::from(meta_root);
    if !dir.exists() {
        return Ok(None);
    }
    if !dir.is_dir() {
        return Err(format!("meta path is not a directory: {}", meta_root));
    }

    let mut latest: Option<i64> = None;
    let entries = fs::read_dir(&dir).map_err(|e| {
        format!(
            "read local meta directory failed: path={}, error={}",
            meta_root, e
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "iterate local meta directory failed: path={}, error={}",
                meta_root, e
            )
        })?;
        let Some(name) = entry.file_name().to_str().map(|v| v.to_string()) else {
            continue;
        };
        if let Some(version) = parse_bundle_version_from_meta_file_name(&name) {
            latest = match latest {
                Some(prev) => Some(prev.max(version)),
                None => Some(version),
            };
        }
    }
    Ok(latest)
}

#[allow(dead_code)]
pub fn discover_latest_bundle_version_oss(meta_root: &str) -> Result<Option<i64>, String> {
    let (op, rel_root) = crate::fs::oss::resolve_oss_operator_and_path(meta_root)?;
    let list_prefix = if rel_root.is_empty() {
        String::new()
    } else if rel_root.ends_with('/') {
        rel_root
    } else {
        format!("{}/", rel_root)
    };
    crate::fs::oss::oss_block_on(async move {
        let mut latest: Option<i64> = None;
        let mut lister = op
            .lister_with(&list_prefix)
            .recursive(false)
            .await
            .map_err(|e| {
                format!(
                    "list oss meta directory failed: path={}, error={}",
                    meta_root, e
                )
            })?;
        while let Some(entry) = lister.try_next().await.map_err(|e| {
            format!(
                "iterate oss meta directory failed: path={}, error={}",
                meta_root, e
            )
        })? {
            let path = entry.path();
            let name = path.rsplit('/').next().unwrap_or(path);
            if let Some(version) = parse_bundle_version_from_meta_file_name(name) {
                latest = match latest {
                    Some(prev) => Some(prev.max(version)),
                    None => Some(version),
                };
            }
        }
        Ok(latest)
    })?
}

#[allow(dead_code)]
pub fn parse_bundle_version_from_meta_file_name(name: &str) -> Option<i64> {
    let trimmed = name.trim();
    if trimmed.is_empty() || !trimmed.to_ascii_lowercase().ends_with(".meta") {
        return None;
    }
    let stem = &trimmed[..trimmed.len().saturating_sub(5)];
    let mut parts = stem.split('_');
    let tablet_hex = parts.next()?;
    let version_hex = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if tablet_hex.len() != 16 || version_hex.len() != 16 {
        return None;
    }
    let tablet_u64 = u64::from_str_radix(tablet_hex, 16).ok()?;
    if tablet_u64 != BUNDLE_TABLET_ID as u64 {
        return None;
    }
    let version_u64 = u64::from_str_radix(version_hex, 16).ok()?;
    if version_u64 == 0 || version_u64 > i64::MAX as u64 {
        return None;
    }
    Some(version_u64 as i64)
}

pub fn empty_tablet_metadata(tablet_id: i64) -> TabletMetadataPb {
    TabletMetadataPb {
        id: Some(tablet_id),
        version: Some(0),
        schema: None,
        rowsets: Vec::new(),
        next_rowset_id: Some(1),
        cumulative_point: Some(0),
        delvec_meta: None,
        compaction_inputs: Vec::new(),
        prev_garbage_version: None,
        orphan_files: Vec::new(),
        enable_persistent_index: None,
        persistent_index_type: None,
        commit_time: None,
        source_schema: None,
        sstable_meta: None,
        dcg_meta: None,
        historical_schemas: HashMap::new(),
        rowset_to_schema: HashMap::new(),
        gtid: Some(0),
        compaction_strategy: None,
        flat_json_config: None,
    }
}

pub fn decode_bundle_metadata_from_bytes(
    bytes: &[u8],
) -> Result<(BundleTabletMetadataPb, usize), String> {
    if bytes.len() < BUNDLE_METADATA_FOOTER_SIZE {
        return Err("invalid bundle meta file: too small".to_string());
    }
    let footer_offset = bytes.len() - BUNDLE_METADATA_FOOTER_SIZE;
    let bundle_size = u64::from_le_bytes(
        bytes[footer_offset..]
            .try_into()
            .map_err(|_| "decode bundle footer failed".to_string())?,
    ) as usize;
    if bundle_size == 0 || bundle_size > footer_offset {
        return Err(format!(
            "invalid bundle meta footer: file_size={} bundle_size={}",
            bytes.len(),
            bundle_size
        ));
    }
    let bundle_offset = footer_offset - bundle_size;
    let bundle = BundleTabletMetadataPb::decode(&bytes[bundle_offset..footer_offset])
        .map_err(|e| format!("decode bundle metadata failed: {}", e))?;
    Ok((bundle, footer_offset))
}

pub fn decode_tablet_metadata_from_bundle_bytes(
    bytes: &[u8],
    tablet_id: i64,
    expected_version: i64,
) -> Result<TabletMetadataPb, String> {
    let (bundle, footer_offset) = decode_bundle_metadata_from_bytes(bytes)?;

    let page = bundle.tablet_meta_pages.get(&tablet_id).ok_or_else(|| {
        format!(
            "bundle metadata missing tablet page for tablet_id={}",
            tablet_id
        )
    })?;
    let start = page.offset as usize;
    let end = start.saturating_add(page.size as usize);
    if end > footer_offset {
        return Err(format!(
            "tablet page out of range: offset={} size={} file_size={}",
            start, page.size, footer_offset
        ));
    }
    let meta = TabletMetadataPb::decode(&bytes[start..end])
        .map_err(|e| format!("decode tablet metadata failed: {}", e))?;
    if meta.id != Some(tablet_id) {
        return Err(format!(
            "tablet metadata id mismatch: expected={} actual={:?}",
            tablet_id, meta.id
        ));
    }
    if meta.version != Some(expected_version) {
        return Err(format!(
            "tablet metadata version mismatch: expected={} actual={:?}",
            expected_version, meta.version
        ));
    }
    Ok(meta)
}

fn load_tablet_metadata_from_previous_versions(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
) -> Result<Option<TabletMetadataPb>, String> {
    if version <= 1 {
        return Ok(None);
    }
    for prev_version in (1..version).rev() {
        let prev_path = bundle_meta_file_path(tablet_root_path, prev_version)?;
        let Some(prev_bytes) = read_bytes_if_exists(&prev_path)? else {
            continue;
        };
        match decode_tablet_metadata_from_bundle_bytes(&prev_bytes, tablet_id, prev_version) {
            Ok(metadata) => return Ok(Some(metadata)),
            Err(err) if is_missing_tablet_page_in_bundle_error(&err) => continue,
            Err(err) => {
                return Err(format!(
                    "decode tablet metadata from fallback version failed: tablet_id={} target_version={} fallback_version={} path={} error={}",
                    tablet_id, version, prev_version, prev_path, err
                ));
            }
        }
    }
    Ok(None)
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
}

pub fn next_rowset_id(rowsets: &[RowsetMetadataPb]) -> u32 {
    rowsets
        .iter()
        .filter_map(|r| r.id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}
