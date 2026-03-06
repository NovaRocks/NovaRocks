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
    BUNDLE_TABLET_ID, INITIAL_VERSION, META_DIR, bundle_meta_file_path, initial_meta_file_path,
    join_tablet_path, standalone_meta_file_path, tablet_meta_rel_path,
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

pub struct BundleMetaWriteEntry<'a> {
    pub tablet_id: i64,
    pub schema: &'a TabletSchemaPb,
    pub tablet_meta: &'a TabletMetadataPb,
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
    if let Some(metadata) =
        try_load_standalone_tablet_metadata(tablet_root_path, tablet_id, version)?
    {
        return Ok(Some(metadata));
    }
    if version == INITIAL_VERSION {
        return try_load_initial_tablet_metadata(tablet_root_path, tablet_id);
    }

    let path = bundle_meta_file_path(tablet_root_path, version)?;
    let maybe_bytes = read_bytes_if_exists(&path)?;
    let Some(bytes) = maybe_bytes else {
        return Ok(None);
    };
    decode_tablet_metadata_from_bundle_bytes(&bytes, tablet_id, version)
        .map(Some)
        .map_err(|err| {
            format!(
                "decode bundle metadata failed: path={}, error={}",
                path, err
            )
        })
}

pub fn write_standalone_meta_file(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    tablet_meta: &TabletMetadataPb,
) -> Result<(), String> {
    let meta_path = standalone_meta_file_path(tablet_root_path, tablet_id, version)?;
    write_bytes(&meta_path, tablet_meta.encode_to_vec())
}

pub fn write_initial_meta_file(
    tablet_root_path: &str,
    tablet_meta: &TabletMetadataPb,
) -> Result<(), String> {
    let meta_path = initial_meta_file_path(tablet_root_path)?;
    write_bytes(&meta_path, tablet_meta.encode_to_vec())
}

fn metadata_path_candidates(
    tablet_root_path: &str,
    tablet_id: i64,
    metadata_file_tablet_id: i64,
    version: i64,
) -> Result<Vec<String>, String> {
    let rel = tablet_meta_rel_path(metadata_file_tablet_id, version)?;
    let mut paths = vec![join_tablet_path(tablet_root_path, &rel)?];
    paths.push(join_tablet_path(
        tablet_root_path,
        &format!("{tablet_id}/{rel}"),
    )?);
    paths.dedup();
    Ok(paths)
}

fn try_load_standalone_tablet_metadata(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
) -> Result<Option<TabletMetadataPb>, String> {
    for path in metadata_path_candidates(tablet_root_path, tablet_id, tablet_id, version)? {
        let Some(bytes) = read_bytes_if_exists(&path)? else {
            continue;
        };
        let metadata = decode_standalone_metadata_from_bytes(&bytes, tablet_id, version, &path)?;
        return Ok(Some(metadata));
    }
    Ok(None)
}

fn try_load_initial_tablet_metadata(
    tablet_root_path: &str,
    tablet_id: i64,
) -> Result<Option<TabletMetadataPb>, String> {
    for path in metadata_path_candidates(
        tablet_root_path,
        tablet_id,
        BUNDLE_TABLET_ID,
        INITIAL_VERSION,
    )? {
        let Some(bytes) = read_bytes_if_exists(&path)? else {
            continue;
        };
        match decode_initial_metadata_from_bytes(&bytes, tablet_id, &path) {
            Ok(metadata) => return Ok(Some(metadata)),
            Err(raw_err) => {
                // Allow old NovaRocks bundle-v1 test data to remain readable while
                // preferring the StarRocks initial raw-metadata layout.
                match decode_tablet_metadata_from_bundle_bytes(&bytes, tablet_id, INITIAL_VERSION) {
                    Ok(metadata) => return Ok(Some(metadata)),
                    Err(bundle_err) => {
                        return Err(format!(
                            "decode initial metadata failed: path={}, raw_error={}, bundle_error={}",
                            path, raw_err, bundle_err
                        ));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn decode_standalone_metadata_from_bytes(
    bytes: &[u8],
    tablet_id: i64,
    expected_version: i64,
    path: &str,
) -> Result<TabletMetadataPb, String> {
    let metadata = TabletMetadataPb::decode(bytes).map_err(|e| {
        format!(
            "decode standalone tablet metadata failed: path={}, error={}",
            path, e
        )
    })?;
    validate_standalone_metadata_identity(&metadata, tablet_id, expected_version, path)?;
    Ok(metadata)
}

fn decode_initial_metadata_from_bytes(
    bytes: &[u8],
    tablet_id: i64,
    path: &str,
) -> Result<TabletMetadataPb, String> {
    let mut metadata = TabletMetadataPb::decode(bytes).map_err(|e| {
        format!(
            "decode initial tablet metadata failed: path={}, error={}",
            path, e
        )
    })?;
    if metadata.version != Some(INITIAL_VERSION) {
        return Err(format!(
            "initial tablet metadata version mismatch: expected={} actual={:?} path={}",
            INITIAL_VERSION, metadata.version, path
        ));
    }
    metadata.id = Some(tablet_id);
    Ok(metadata)
}

fn validate_standalone_metadata_identity(
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    expected_version: i64,
    path: &str,
) -> Result<(), String> {
    if metadata.id != Some(tablet_id) {
        return Err(format!(
            "tablet metadata id mismatch: expected={} actual={:?} path={}",
            tablet_id, metadata.id, path
        ));
    }
    if metadata.version != Some(expected_version) {
        return Err(format!(
            "tablet metadata version mismatch: expected={} actual={:?} path={}",
            expected_version, metadata.version, path
        ));
    }
    Ok(())
}
pub fn write_bundle_meta_file(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    schema: &TabletSchemaPb,
    tablet_meta: &TabletMetadataPb,
) -> Result<(), String> {
    write_bundle_meta_file_batch(
        tablet_root_path,
        version,
        &[BundleMetaWriteEntry {
            tablet_id,
            schema,
            tablet_meta,
        }],
    )
}

pub fn write_bundle_meta_file_batch(
    tablet_root_path: &str,
    version: i64,
    entries: &[BundleMetaWriteEntry<'_>],
) -> Result<(), String> {
    if entries.is_empty() {
        return Ok(());
    }
    with_bundle_meta_write_lock(tablet_root_path, version, || {
        let (mut tablet_metas, mut tablet_to_schema, mut schemas) =
            load_bundle_meta_write_state(tablet_root_path, version)?;
        for entry in entries {
            let schema_id = entry
                .schema
                .id
                .filter(|v| *v > 0)
                .ok_or_else(|| "bundle schema id is missing".to_string())?;
            tablet_metas.insert(entry.tablet_id, entry.tablet_meta.clone());
            tablet_to_schema.insert(entry.tablet_id, schema_id);
            schemas.insert(schema_id, entry.schema.clone());
        }
        let file_bytes = encode_bundle_meta_file_bytes(tablet_metas, tablet_to_schema, schemas)?;
        let meta_path = bundle_meta_file_path(tablet_root_path, version)?;
        write_bytes(&meta_path, file_bytes)
    })
}

type BundleWriteState = (
    HashMap<i64, TabletMetadataPb>,
    HashMap<i64, i64>,
    HashMap<i64, TabletSchemaPb>,
);

fn load_bundle_meta_write_state(
    tablet_root_path: &str,
    version: i64,
) -> Result<BundleWriteState, String> {
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
            if version - 1 == INITIAL_VERSION
                && is_initial_raw_metadata_bytes(&previous_bytes).unwrap_or(false)
            {
                // StarRocks version=1 initial metadata is a raw TabletMetadataPB, not a bundle.
                // The first bundle version must be built from current entries instead of merging
                // the raw initial metadata file.
            } else {
                merge_bundle_bytes(&previous_bytes, &previous_meta_path, true)?;
            }
        }
    }

    if let Some(existing_bytes) = read_bytes_if_exists(&meta_path)? {
        merge_bundle_bytes(&existing_bytes, &meta_path, false)?;
    }
    Ok((tablet_metas, tablet_to_schema, schemas))
}

fn is_initial_raw_metadata_bytes(bytes: &[u8]) -> Result<bool, String> {
    match TabletMetadataPb::decode(bytes) {
        Ok(metadata) => Ok(metadata.version == Some(INITIAL_VERSION)),
        Err(_) => Ok(false),
    }
}

fn encode_bundle_meta_file_bytes(
    mut tablet_metas: HashMap<i64, TabletMetadataPb>,
    tablet_to_schema: HashMap<i64, i64>,
    schemas: HashMap<i64, TabletSchemaPb>,
) -> Result<Vec<u8>, String> {
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
    Ok(file_bytes)
}

#[allow(dead_code)]
pub fn load_latest_tablet_metadata(
    tablet_root_path: &str,
    tablet_id: i64,
) -> Result<(i64, TabletMetadataPb), String> {
    let latest_version = discover_latest_tablet_metadata_version(tablet_root_path, tablet_id)?;
    let Some(latest_version) = latest_version else {
        return Ok((0, empty_tablet_metadata(tablet_id)));
    };
    if latest_version <= 0 {
        return Ok((0, empty_tablet_metadata(tablet_id)));
    }

    if let Some(meta) =
        load_tablet_metadata_at_version(tablet_root_path, tablet_id, latest_version)?
    {
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
        ScanPathScheme::Hdfs => Err(format!(
            "discover latest bundle version does not support hdfs path yet: {}",
            meta_root
        )),
    }
}

#[allow(dead_code)]
pub fn discover_latest_tablet_metadata_version(
    tablet_root_path: &str,
    tablet_id: i64,
) -> Result<Option<i64>, String> {
    let mut latest = discover_latest_metadata_version_in_dir(
        &join_tablet_path(tablet_root_path, META_DIR)?,
        tablet_id,
    )?;
    let tablet_scoped = discover_latest_metadata_version_in_dir(
        &join_tablet_path(tablet_root_path, &format!("{tablet_id}/{META_DIR}"))?,
        tablet_id,
    )?;
    if let Some(tablet_scoped) = tablet_scoped {
        latest = Some(
            latest
                .map(|v| v.max(tablet_scoped))
                .unwrap_or(tablet_scoped),
        );
    }
    Ok(latest)
}

fn discover_latest_metadata_version_in_dir(
    meta_root: &str,
    tablet_id: i64,
) -> Result<Option<i64>, String> {
    let scheme = classify_scan_paths([meta_root])?;
    match scheme {
        ScanPathScheme::Local => discover_latest_metadata_version_local(meta_root, tablet_id),
        ScanPathScheme::Oss => discover_latest_metadata_version_oss(meta_root, tablet_id),
        ScanPathScheme::Hdfs => Err(format!(
            "discover latest metadata version does not support hdfs path yet: {}",
            meta_root
        )),
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

fn discover_latest_metadata_version_local(
    meta_root: &str,
    tablet_id: i64,
) -> Result<Option<i64>, String> {
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
        if let Some((file_tablet_id, version)) = parse_meta_file_name(&name)
            && (file_tablet_id == tablet_id || file_tablet_id == BUNDLE_TABLET_ID)
        {
            latest = Some(latest.map(|prev| prev.max(version)).unwrap_or(version));
        }
    }
    Ok(latest)
}

#[allow(dead_code)]
pub fn discover_latest_bundle_version_oss(meta_root: &str) -> Result<Option<i64>, String> {
    let cfg = crate::runtime::starlet_shard_registry::oss_config_for_path(meta_root)?;
    let (op, rel_root) =
        crate::fs::oss::resolve_oss_operator_and_path_with_config(meta_root, &cfg)?;
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

fn discover_latest_metadata_version_oss(
    meta_root: &str,
    tablet_id: i64,
) -> Result<Option<i64>, String> {
    let cfg = crate::runtime::starlet_shard_registry::oss_config_for_path(meta_root)?;
    let (op, rel_root) =
        crate::fs::oss::resolve_oss_operator_and_path_with_config(meta_root, &cfg)?;
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
            let meta = entry.metadata();
            if meta.is_dir() {
                continue;
            }
            let Some(name) = entry.name().rsplit('/').next() else {
                continue;
            };
            if let Some((file_tablet_id, version)) = parse_meta_file_name(name)
                && (file_tablet_id == tablet_id || file_tablet_id == BUNDLE_TABLET_ID)
            {
                latest = Some(latest.map(|prev| prev.max(version)).unwrap_or(version));
            }
        }
        Ok(latest)
    })?
}

#[allow(dead_code)]
pub fn parse_bundle_version_from_meta_file_name(name: &str) -> Option<i64> {
    let (tablet_id, version) = parse_meta_file_name(name)?;
    (tablet_id == BUNDLE_TABLET_ID && version != INITIAL_VERSION).then_some(version)
}

fn parse_meta_file_name(name: &str) -> Option<(i64, i64)> {
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
    let version_u64 = u64::from_str_radix(version_hex, 16).ok()?;
    if version_u64 == 0 || tablet_u64 > i64::MAX as u64 || version_u64 > i64::MAX as u64 {
        return None;
    }
    Some((tablet_u64 as i64, version_u64 as i64))
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

pub fn next_rowset_id(rowsets: &[RowsetMetadataPb]) -> u32 {
    rowsets
        .iter()
        .filter_map(|r| r.id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::grpc_client::proto::starrocks::{KeysType, TabletSchemaPb};
    use tempfile::TempDir;

    #[test]
    fn parse_bundle_version_ignores_initial_metadata_file() {
        assert_eq!(
            parse_bundle_version_from_meta_file_name("0000000000000000_0000000000000001.meta"),
            None
        );
        assert_eq!(
            parse_bundle_version_from_meta_file_name("0000000000000000_0000000000000002.meta"),
            Some(2)
        );
    }

    #[test]
    fn load_tablet_metadata_at_version_reads_initial_raw_metadata() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let owner_tablet_id = 30001_i64;
        let requested_tablet_id = 30005_i64;
        let metadata = TabletMetadataPb {
            id: Some(owner_tablet_id),
            version: Some(INITIAL_VERSION),
            schema: Some(TabletSchemaPb {
                keys_type: Some(KeysType::DupKeys as i32),
                id: Some(301_i64),
                ..Default::default()
            }),
            next_rowset_id: Some(1),
            ..Default::default()
        };
        write_initial_meta_file(
            temp_dir.path().to_str().expect("temp path to str"),
            &metadata,
        )
        .expect("write initial metadata");

        let loaded = load_tablet_metadata_at_version(
            temp_dir.path().to_str().expect("temp path to str"),
            requested_tablet_id,
            INITIAL_VERSION,
        )
        .expect("load metadata")
        .expect("metadata exists");
        assert_eq!(loaded.id, Some(requested_tablet_id));
        assert_eq!(loaded.version, Some(INITIAL_VERSION));
        assert_eq!(loaded.schema.as_ref().and_then(|v| v.id), Some(301_i64));
    }

    #[test]
    fn load_tablet_metadata_at_version_does_not_fallback_to_previous_bundle_versions() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let tablet_id = 30011_i64;
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            id: Some(302_i64),
            ..Default::default()
        };
        let metadata_v1 = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(INITIAL_VERSION),
            schema: Some(schema.clone()),
            ..Default::default()
        };
        write_initial_meta_file(
            temp_dir.path().to_str().expect("temp path to str"),
            &metadata_v1,
        )
        .expect("write initial metadata");

        let loaded = load_tablet_metadata_at_version(
            temp_dir.path().to_str().expect("temp path to str"),
            tablet_id,
            2,
        )
        .expect("load metadata");
        assert!(
            loaded.is_none(),
            "version 2 must not fall back to version 1"
        );
    }
}
