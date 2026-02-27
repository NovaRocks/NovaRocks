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
//! StarRocks tablet metadata loader for native scan.
//!
//! This module resolves tablet snapshot metadata, validates layout assumptions,
//! and loads segment footers for bundle-backed segment files.
//!
//! Current limitations:
//! - Segment reads rely on local filesystem or S3-compatible object storage.

use std::collections::BTreeMap;

use opendal::{ErrorKind, Operator};
use prost::Message;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::formats::starrocks::cache::{segment_footer_cache_get, segment_footer_cache_put};
use crate::formats::starrocks::segment::{StarRocksSegmentFooter, decode_segment_footer};
use crate::runtime::global_async_runtime::data_runtime;
use crate::service::grpc_client::proto::starrocks::{
    BundleTabletMetadataPb, DelvecPagePb, KeysType, PagePointerPb, TabletMetadataPb, TabletSchemaPb,
};

const METADATA_DIR: &str = "meta";
const DATA_DIR: &str = "data";
const BUNDLE_METADATA_FOOTER_SIZE: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
/// One segment entry resolved from tablet rowsets.
pub struct StarRocksSegmentFile {
    pub name: String,
    pub relative_path: String,
    pub path: String,
    pub rowset_version: i64,
    pub segment_id: Option<u32>,
    pub bundle_file_offset: Option<i64>,
    pub segment_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// One delvec page pointer resolved from tablet metadata.
pub struct StarRocksDelvecPageRaw {
    pub version: i64,
    pub offset: u64,
    pub size: u64,
    pub crc32c: Option<u32>,
    pub crc32c_gen_version: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Minimal primary-key delvec metadata required by native reader.
pub struct StarRocksDelvecMetaRaw {
    pub version_to_file_rel_path: BTreeMap<i64, String>,
    pub segment_delvec_pages: BTreeMap<u32, StarRocksDelvecPageRaw>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal IN predicate shape extracted from StarRocks metadata.
pub struct StarRocksInPredicateRaw {
    pub column_name: String,
    pub is_not_in: bool,
    pub values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal binary predicate shape extracted from StarRocks metadata.
pub struct StarRocksBinaryPredicateRaw {
    pub column_name: String,
    pub op: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal is-null predicate shape extracted from StarRocks metadata.
pub struct StarRocksIsNullPredicateRaw {
    pub column_name: String,
    pub is_not_null: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// One delete-predicate group extracted from rowset metadata.
pub struct StarRocksDeletePredicateRaw {
    pub version: i64,
    pub sub_predicates: Vec<String>,
    pub in_predicates: Vec<StarRocksInPredicateRaw>,
    pub binary_predicates: Vec<StarRocksBinaryPredicateRaw>,
    pub is_null_predicates: Vec<StarRocksIsNullPredicateRaw>,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Minimal metadata snapshot required for native read planning.
pub struct StarRocksTabletSnapshot {
    pub tablet_id: i64,
    pub version: i64,
    pub metadata_path: String,
    pub tablet_schema: TabletSchemaPb,
    pub total_num_rows: u64,
    pub rowset_count: usize,
    pub segment_files: Vec<StarRocksSegmentFile>,
    pub delete_predicates: Vec<StarRocksDeletePredicateRaw>,
    pub delvec_meta: StarRocksDelvecMetaRaw,
}

/// Load tablet metadata and build a minimal snapshot.
/// It supports both standalone metadata and bundle metadata layouts.
pub fn load_tablet_snapshot(
    tablet_id: i64,
    version: i64,
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksTabletSnapshot, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for metadata loader: {tablet_id}"
        ));
    }
    if version <= 0 {
        return Err(format!(
            "invalid tablet version for metadata loader: {version}"
        ));
    }

    let root = TabletRoot::parse(tablet_root_path)?;
    let op = build_operator(&root, object_store_profile)?;
    let rt = data_runtime()?;
    load_tablet_snapshot_from_root(tablet_id, version, &root, rt.as_ref(), &op)
}

fn load_tablet_snapshot_from_root(
    tablet_id: i64,
    version: i64,
    root: &TabletRoot,
    rt: &tokio::runtime::Runtime,
    op: &Operator,
) -> Result<StarRocksTabletSnapshot, String> {
    let mut fallback_error: Option<String> = None;
    for candidate_version in (1..=version).rev() {
        match load_tablet_snapshot_at_version(tablet_id, candidate_version, root, rt, op) {
            Ok(mut snapshot) => {
                // Keep the caller-visible version stable even when metadata is carried over
                // from an earlier version due missing page/file in a newer bundle.
                snapshot.version = version;
                return Ok(snapshot);
            }
            Err(err) => {
                if fallback_error.is_none() {
                    fallback_error = Some(err.clone());
                }
                if candidate_version == 1 || !should_try_previous_metadata_version(&err) {
                    return Err(err);
                }
            }
        }
    }
    Err(fallback_error.unwrap_or_else(|| {
        format!(
            "load tablet snapshot failed with unknown error: tablet_id={}, version={}",
            tablet_id, version
        )
    }))
}

fn load_tablet_snapshot_at_version(
    tablet_id: i64,
    version: i64,
    root: &TabletRoot,
    rt: &tokio::runtime::Runtime,
    op: &Operator,
) -> Result<StarRocksTabletSnapshot, String> {
    let standalone_metadata_rel = metadata_rel_path(tablet_id, version)?;
    for candidate_rel in metadata_rel_path_candidates(tablet_id, &standalone_metadata_rel) {
        if object_exists(rt, op, &candidate_rel)? {
            let metadata_bytes = read_all_bytes(rt, op, &candidate_rel)?;
            let metadata_path = root.join_relative_path(&candidate_rel);
            return parse_standalone_snapshot(
                tablet_id,
                version,
                root,
                &metadata_path,
                &metadata_bytes,
            );
        }
    }

    let bundle_metadata_rel = metadata_rel_path(0, version)?;
    for candidate_rel in metadata_rel_path_candidates(tablet_id, &bundle_metadata_rel) {
        if object_exists(rt, op, &candidate_rel)? {
            let metadata_bytes = read_all_bytes(rt, op, &candidate_rel)?;
            let metadata_path = root.join_relative_path(&candidate_rel);
            return parse_bundle_snapshot(
                tablet_id,
                version,
                root,
                &metadata_path,
                &metadata_bytes,
            );
        }
    }
    Err(format!("metadata file not found: {}", bundle_metadata_rel))
}

fn should_try_previous_metadata_version(error: &str) -> bool {
    error.contains("metadata file not found:")
        || error.contains("bundle metadata does not contain tablet page:")
}

/// Load and validate segment footers from a metadata snapshot.
/// Supports both bundle-backed ranges and standalone segment files.
pub fn load_bundle_segment_footers(
    snapshot: &StarRocksTabletSnapshot,
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<Vec<StarRocksSegmentFooter>, String> {
    if let Some(footers) =
        segment_footer_cache_get(tablet_root_path, snapshot.tablet_id, snapshot.version)
    {
        return Ok(footers);
    }

    let root = TabletRoot::parse(tablet_root_path)?;
    let op = build_operator(&root, object_store_profile)?;
    let rt = data_runtime()?;

    let mut footers = Vec::with_capacity(snapshot.segment_files.len());
    for segment in &snapshot.segment_files {
        let segment_bytes = if let Some(bundle_offset) = segment.bundle_file_offset {
            let segment_size = segment.segment_size.ok_or_else(|| {
                format!(
                    "missing segment_size for bundle segment in metadata: path={}",
                    segment.path
                )
            })?;
            let start = u64::try_from(bundle_offset).map_err(|_| {
                format!(
                    "invalid bundle file offset for segment: path={}, offset={}",
                    segment.path, bundle_offset
                )
            })?;
            let end = start.checked_add(segment_size).ok_or_else(|| {
                format!(
                    "segment range overflow: path={}, offset={}, segment_size={}",
                    segment.path, bundle_offset, segment_size
                )
            })?;
            read_range_bytes(rt.as_ref(), &op, &segment.relative_path, start, end)?
        } else {
            read_all_bytes(rt.as_ref(), &op, &segment.relative_path)?
        };
        footers.push(decode_segment_footer(&segment.path, &segment_bytes)?);
    }

    segment_footer_cache_put(
        tablet_root_path,
        snapshot.tablet_id,
        snapshot.version,
        footers.clone(),
    );

    Ok(footers)
}

fn parse_standalone_snapshot(
    tablet_id: i64,
    version: i64,
    root: &TabletRoot,
    metadata_path: &str,
    metadata_bytes: &[u8],
) -> Result<StarRocksTabletSnapshot, String> {
    let metadata = TabletMetadataPb::decode(metadata_bytes).map_err(|e| {
        format!(
            "decode standalone TabletMetadataPB failed: path={}, error={}",
            metadata_path, e
        )
    })?;
    ensure_tablet_identity(&metadata, tablet_id, version, metadata_path)?;
    let tablet_schema = metadata.schema.clone().ok_or_else(|| {
        format!(
            "tablet schema missing in standalone metadata: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let schema_id = tablet_schema.id.unwrap_or(-1);
    ensure_supported_keys_type(
        &tablet_schema,
        tablet_id,
        schema_id,
        "tablet",
        metadata_path,
    )?;
    for (rowset_schema_id, rowset_schema) in &metadata.historical_schemas {
        ensure_supported_keys_type(
            rowset_schema,
            tablet_id,
            *rowset_schema_id,
            "rowset",
            metadata_path,
        )?;
    }
    let (segment_files, delete_predicates) = collect_segment_files(root, &metadata)?;
    let total_num_rows = collect_total_num_rows(&metadata, tablet_id, metadata_path)?;
    let delvec_meta = collect_delvec_meta(&metadata)?;

    Ok(StarRocksTabletSnapshot {
        tablet_id,
        version,
        metadata_path: metadata_path.to_string(),
        tablet_schema,
        total_num_rows,
        rowset_count: metadata.rowsets.len(),
        segment_files,
        delete_predicates,
        delvec_meta,
    })
}

fn parse_bundle_snapshot(
    tablet_id: i64,
    version: i64,
    root: &TabletRoot,
    metadata_path: &str,
    metadata_bytes: &[u8],
) -> Result<StarRocksTabletSnapshot, String> {
    let (bundle, _bundle_meta_size) = decode_bundle_metadata(metadata_path, metadata_bytes)?;
    let page = bundle.tablet_meta_pages.get(&tablet_id).ok_or_else(|| {
        format!(
            "bundle metadata does not contain tablet page: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let mut metadata = decode_tablet_metadata_page(metadata_path, metadata_bytes, page)?;
    ensure_tablet_identity(&metadata, tablet_id, version, metadata_path)?;
    hydrate_schema_from_bundle(tablet_id, &bundle, &mut metadata, metadata_path)?;
    let tablet_schema = metadata.schema.clone().ok_or_else(|| {
        format!(
            "tablet schema missing after bundle schema hydration: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let (segment_files, delete_predicates) = collect_segment_files(root, &metadata)?;
    let total_num_rows = collect_total_num_rows(&metadata, tablet_id, metadata_path)?;
    let delvec_meta = collect_delvec_meta(&metadata)?;

    Ok(StarRocksTabletSnapshot {
        tablet_id,
        version,
        metadata_path: metadata_path.to_string(),
        tablet_schema,
        total_num_rows,
        rowset_count: metadata.rowsets.len(),
        segment_files,
        delete_predicates,
        delvec_meta,
    })
}

fn decode_bundle_metadata(
    metadata_path: &str,
    bytes: &[u8],
) -> Result<(BundleTabletMetadataPb, usize), String> {
    if bytes.len() < BUNDLE_METADATA_FOOTER_SIZE {
        return Err(format!(
            "invalid bundle metadata file: {} (file too small, size={})",
            metadata_path,
            bytes.len()
        ));
    }

    let footer_offset = bytes.len() - BUNDLE_METADATA_FOOTER_SIZE;
    let bundle_meta_size = u64::from_le_bytes(
        bytes[footer_offset..]
            .try_into()
            .map_err(|_| "decode bundle footer failed".to_string())?,
    ) as usize;
    if bundle_meta_size == 0 || bundle_meta_size > footer_offset {
        return Err(format!(
            "invalid bundle metadata footer: path={}, file_size={}, bundle_meta_size={}",
            metadata_path,
            bytes.len(),
            bundle_meta_size
        ));
    }

    let bundle_offset = footer_offset - bundle_meta_size;
    let bundle =
        BundleTabletMetadataPb::decode(&bytes[bundle_offset..footer_offset]).map_err(|e| {
            format!(
                "decode BundleTabletMetadataPB failed: path={}, error={}",
                metadata_path, e
            )
        })?;
    Ok((bundle, bundle_meta_size))
}

fn decode_tablet_metadata_page(
    metadata_path: &str,
    file_bytes: &[u8],
    page: &PagePointerPb,
) -> Result<TabletMetadataPb, String> {
    let offset = usize::try_from(page.offset).map_err(|_| {
        format!(
            "invalid tablet metadata page offset: path={}, offset={}",
            metadata_path, page.offset
        )
    })?;
    let size = usize::try_from(page.size).map_err(|_| {
        format!(
            "invalid tablet metadata page size: path={}, size={}",
            metadata_path, page.size
        )
    })?;
    let end = offset.saturating_add(size);
    if offset > file_bytes.len() || end > file_bytes.len() {
        return Err(format!(
            "tablet metadata page out of range: path={}, offset={}, size={}, file_size={}",
            metadata_path,
            offset,
            size,
            file_bytes.len()
        ));
    }

    TabletMetadataPb::decode(&file_bytes[offset..end]).map_err(|e| {
        format!(
            "decode TabletMetadataPB failed: path={}, offset={}, size={}, error={}",
            metadata_path, offset, size, e
        )
    })
}

fn ensure_tablet_identity(
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    version: i64,
    metadata_path: &str,
) -> Result<(), String> {
    if metadata.id != Some(tablet_id) {
        return Err(format!(
            "tablet id mismatch in metadata page: expected={}, actual={:?}, path={}",
            tablet_id, metadata.id, metadata_path
        ));
    }
    if metadata.version != Some(version) {
        return Err(format!(
            "tablet version mismatch in metadata page: expected={}, actual={:?}, path={}",
            version, metadata.version, metadata_path
        ));
    }
    Ok(())
}

fn hydrate_schema_from_bundle(
    tablet_id: i64,
    bundle: &BundleTabletMetadataPb,
    metadata: &mut TabletMetadataPb,
    metadata_path: &str,
) -> Result<(), String> {
    let schema_id = bundle.tablet_to_schema.get(&tablet_id).ok_or_else(|| {
        format!(
            "tablet schema id missing in bundle metadata: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let schema = bundle.schemas.get(schema_id).ok_or_else(|| {
        format!(
            "tablet schema not found in bundle metadata: tablet_id={}, schema_id={}, path={}",
            tablet_id, schema_id, metadata_path
        )
    })?;
    ensure_supported_keys_type(schema, tablet_id, *schema_id, "tablet", metadata_path)?;

    metadata.schema = Some(schema.clone());
    metadata
        .historical_schemas
        .insert(*schema_id, schema.clone());

    for rowset_schema_id in metadata.rowset_to_schema.values() {
        let rowset_schema = bundle.schemas.get(rowset_schema_id).ok_or_else(|| {
            format!(
                "rowset schema not found in bundle metadata: tablet_id={}, schema_id={}, path={}",
                tablet_id, rowset_schema_id, metadata_path
            )
        })?;
        ensure_supported_keys_type(
            rowset_schema,
            tablet_id,
            *rowset_schema_id,
            "rowset",
            metadata_path,
        )?;
        metadata
            .historical_schemas
            .insert(*rowset_schema_id, rowset_schema.clone());
    }
    Ok(())
}

fn ensure_supported_keys_type(
    schema: &TabletSchemaPb,
    tablet_id: i64,
    schema_id: i64,
    schema_kind: &str,
    metadata_path: &str,
) -> Result<(), String> {
    let raw_keys_type = schema.keys_type.ok_or_else(|| {
        format!(
            "missing keys_type in tablet schema: tablet_id={}, schema_id={}, schema_kind={}, path={}",
            tablet_id, schema_id, schema_kind, metadata_path
        )
    })?;
    let keys_type = KeysType::try_from(raw_keys_type).map_err(|_| {
        format!(
            "unknown keys_type in tablet schema: tablet_id={}, schema_id={}, schema_kind={}, keys_type={}, path={}",
            tablet_id, schema_id, schema_kind, raw_keys_type, metadata_path
        )
    })?;
    if !matches!(
        keys_type,
        KeysType::DupKeys | KeysType::AggKeys | KeysType::UniqueKeys | KeysType::PrimaryKeys
    ) {
        return Err(format!(
            "unsupported keys_type for rust native starrocks reader: tablet_id={}, schema_id={}, schema_kind={}, keys_type={}, supported=[DUP_KEYS,AGG_KEYS,UNIQUE_KEYS,PRIMARY_KEYS], path={}",
            tablet_id,
            schema_id,
            schema_kind,
            keys_type.as_str_name(),
            metadata_path
        ));
    }
    Ok(())
}

fn collect_total_num_rows(
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    metadata_path: &str,
) -> Result<u64, String> {
    let mut total_rows = 0_u64;
    for (idx, rowset) in metadata.rowsets.iter().enumerate() {
        let num_rows = rowset.num_rows.ok_or_else(|| {
            format!(
                "rowset num_rows is missing in tablet metadata: tablet_id={}, rowset_index={}, path={}",
                tablet_id, idx, metadata_path
            )
        })?;
        if num_rows < 0 {
            return Err(format!(
                "rowset num_rows is negative in tablet metadata: tablet_id={}, rowset_index={}, num_rows={}, path={}",
                tablet_id, idx, num_rows, metadata_path
            ));
        }
        total_rows = total_rows
            .checked_add(num_rows as u64)
            .ok_or_else(|| {
                format!(
                    "rowset num_rows overflow in tablet metadata: tablet_id={}, rowset_index={}, path={}",
                    tablet_id, idx, metadata_path
                )
            })?;
    }
    Ok(total_rows)
}

fn collect_segment_files(
    root: &TabletRoot,
    metadata: &TabletMetadataPb,
) -> Result<(Vec<StarRocksSegmentFile>, Vec<StarRocksDeletePredicateRaw>), String> {
    let mut files = Vec::new();
    let mut delete_predicates = Vec::new();
    for (rowset_index, rowset) in metadata.rowsets.iter().enumerate() {
        // Keep lake-tablet semantics in StarRocks BE:
        // - delete predicate version key is rowset index, not delete_predicate.version.
        // - rowset scan applies delete predicates with version >= current rowset index.
        let rowset_version = i64::try_from(rowset_index).map_err(|_| {
            format!(
                "rowset index overflow while deriving delete visibility version: rowset_id={:?}, rowset_index={}",
                rowset.id, rowset_index
            )
        })?;
        if rowset_version < 0 {
            return Err(format!(
                "invalid rowset version in tablet metadata: rowset_id={:?}, version={}",
                rowset.id, rowset_version
            ));
        }

        if let Some(delete_predicate) = rowset.delete_predicate.as_ref() {
            delete_predicates.push(StarRocksDeletePredicateRaw {
                version: rowset_version,
                // Lake reader in StarRocks BE does not use sub_predicates.
                sub_predicates: Vec::new(),
                in_predicates: delete_predicate
                    .in_predicates
                    .iter()
                    .map(|p| {
                        let column_name = p
                            .column_name
                            .as_deref()
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "delete in predicate column_name is missing: rowset_id={:?}, version={}",
                                    rowset.id, rowset_version
                                )
                            })?;
                        Ok(StarRocksInPredicateRaw {
                            column_name: column_name.to_string(),
                            is_not_in: p.is_not_in.unwrap_or(false),
                            values: p.values.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                binary_predicates: delete_predicate
                    .binary_predicates
                    .iter()
                    .map(|p| {
                        let column_name = p
                            .column_name
                            .as_deref()
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "delete binary predicate column_name is missing: rowset_id={:?}, version={}",
                                    rowset.id, rowset_version
                                )
                            })?;
                        let op = p.op.as_deref().map(str::trim).filter(|v| !v.is_empty()).ok_or_else(
                            || {
                                format!(
                                    "delete binary predicate op is missing: rowset_id={:?}, version={}, column_name={}",
                                    rowset.id, rowset_version, column_name
                                )
                            },
                        )?;
                        let value = p
                            .value
                            .as_deref()
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "delete binary predicate value is missing: rowset_id={:?}, version={}, column_name={}, op={}",
                                    rowset.id, rowset_version, column_name, op
                                )
                            })?;
                        Ok(StarRocksBinaryPredicateRaw {
                            column_name: column_name.to_string(),
                            op: op.to_string(),
                            value: value.to_string(),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                is_null_predicates: delete_predicate
                    .is_null_predicates
                    .iter()
                    .map(|p| {
                        let column_name = p
                            .column_name
                            .as_deref()
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "delete is-null predicate column_name is missing: rowset_id={:?}, version={}",
                                    rowset.id, rowset_version
                                )
                            })?;
                        Ok(StarRocksIsNullPredicateRaw {
                            column_name: column_name.to_string(),
                            is_not_null: p.is_not_null.unwrap_or(false),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            });
        }

        if !rowset.segment_size.is_empty() && rowset.segment_size.len() != rowset.segments.len() {
            return Err(format!(
                "invalid rowset segment_size: segments={}, segment_size={}",
                rowset.segments.len(),
                rowset.segment_size.len()
            ));
        }
        if !rowset.bundle_file_offsets.is_empty()
            && rowset.bundle_file_offsets.len() != rowset.segments.len()
        {
            return Err(format!(
                "invalid rowset bundle_file_offsets: segments={}, bundle_file_offsets={}",
                rowset.segments.len(),
                rowset.bundle_file_offsets.len()
            ));
        }
        if !rowset.bundle_file_offsets.is_empty()
            && rowset.segment_size.len() != rowset.segments.len()
        {
            return Err(format!(
                "bundle rowset missing segment_size: segments={}, segment_size={}",
                rowset.segments.len(),
                rowset.segment_size.len()
            ));
        }
        for (index, raw_name) in rowset.segments.iter().enumerate() {
            let name = raw_name.trim().trim_start_matches('/').to_string();
            if name.is_empty() {
                return Err("empty segment file name in rowset metadata".to_string());
            }
            let segment_id = match rowset.id {
                Some(rowset_id) => {
                    let ordinal = u32::try_from(index).map_err(|_| {
                        format!(
                            "segment index overflow while deriving segment_id: rowset_id={}, index={}",
                            rowset_id, index
                        )
                    })?;
                    Some(rowset_id.checked_add(ordinal).ok_or_else(|| {
                        format!(
                            "segment_id overflow while deriving rowset_id+segment_index: rowset_id={}, index={}",
                            rowset_id, index
                        )
                    })?)
                }
                None => None,
            };
            let bundle_file_offset = rowset.bundle_file_offsets.get(index).copied();
            if let Some(offset) = bundle_file_offset {
                if offset < 0 {
                    return Err(format!(
                        "invalid negative bundle_file_offset in rowset metadata: segment={}, offset={}",
                        name, offset
                    ));
                }
            }
            let segment_size = rowset
                .segment_size
                .get(index)
                .copied()
                .filter(|size| *size > 0);
            if bundle_file_offset.is_some() && segment_size.is_none() {
                return Err(format!(
                    "missing segment_size for bundle segment in rowset metadata: segment={}",
                    name
                ));
            }
            let rel_path = format!("{DATA_DIR}/{name}");
            files.push(StarRocksSegmentFile {
                name,
                relative_path: rel_path.clone(),
                path: root.join_relative_path(&rel_path),
                rowset_version,
                segment_id,
                bundle_file_offset,
                segment_size,
            });
        }
    }
    Ok((files, delete_predicates))
}

fn collect_delvec_meta(metadata: &TabletMetadataPb) -> Result<StarRocksDelvecMetaRaw, String> {
    let mut out = StarRocksDelvecMetaRaw::default();
    let Some(raw) = metadata.delvec_meta.as_ref() else {
        return Ok(out);
    };

    for (version, file) in &raw.version_to_file {
        if *version < 0 {
            return Err(format!(
                "invalid delvec file version in metadata: {version}"
            ));
        }
        let name = file
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("delvec file name is missing for version {}", version))?;
        let rel = format!("{DATA_DIR}/{}", name.trim_start_matches('/'));
        if out
            .version_to_file_rel_path
            .insert(*version, rel.clone())
            .is_some()
        {
            return Err(format!(
                "duplicated delvec file version in metadata: version={}, path={}",
                version, rel
            ));
        }
    }

    for (segment_id, page) in &raw.delvecs {
        let parsed = parse_delvec_page(*segment_id, page)?;
        if out
            .segment_delvec_pages
            .insert(*segment_id, parsed.clone())
            .is_some()
        {
            return Err(format!(
                "duplicated delvec page for segment_id in metadata: segment_id={}",
                segment_id
            ));
        }
    }

    Ok(out)
}

fn parse_delvec_page(
    segment_id: u32,
    page: &DelvecPagePb,
) -> Result<StarRocksDelvecPageRaw, String> {
    let version = page
        .version
        .ok_or_else(|| format!("missing delvec page version for segment_id {}", segment_id))?;
    if version < 0 {
        return Err(format!(
            "invalid delvec page version for segment_id {}: {}",
            segment_id, version
        ));
    }
    let offset = page
        .offset
        .ok_or_else(|| format!("missing delvec page offset for segment_id {}", segment_id))?;
    let size = page
        .size
        .ok_or_else(|| format!("missing delvec page size for segment_id {}", segment_id))?;
    Ok(StarRocksDelvecPageRaw {
        version,
        offset,
        size,
        crc32c: page.crc32c,
        crc32c_gen_version: page.crc32c_gen_version,
    })
}

fn metadata_rel_path(tablet_id: i64, version: i64) -> Result<String, String> {
    if tablet_id < 0 {
        return Err(format!(
            "invalid tablet id for metadata file name: {tablet_id}"
        ));
    }
    if version <= 0 {
        return Err(format!(
            "invalid tablet version for metadata file name: {version}"
        ));
    }
    let tablet_id_u64 = u64::try_from(tablet_id)
        .map_err(|_| format!("convert tablet_id to u64 failed: {tablet_id}"))?;
    let version_u64 =
        u64::try_from(version).map_err(|_| format!("convert version to u64 failed: {version}"))?;
    Ok(format!(
        "{METADATA_DIR}/{tablet_id_u64:016X}_{version_u64:016X}.meta"
    ))
}

fn metadata_rel_path_candidates(tablet_id: i64, rel_path: &str) -> Vec<String> {
    let normalized = rel_path.trim_start_matches('/').to_string();
    if normalized.is_empty() {
        return vec![normalized];
    }
    let tablet_prefix = format!("{tablet_id}/");
    if normalized.starts_with(&tablet_prefix) {
        vec![normalized]
    } else {
        vec![normalized.clone(), format!("{tablet_id}/{normalized}")]
    }
}

fn object_exists(rt: &tokio::runtime::Runtime, op: &Operator, path: &str) -> Result<bool, String> {
    const MAX_STAT_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_STAT_ATTEMPTS {
        match rt.block_on(op.stat(path)) {
            Ok(_) => return Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) if e.is_temporary() && attempt < MAX_STAT_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => return Err(format!("stat object failed: path={}, error={}", path, e)),
        }
    }
    Err(format!("stat object failed after retries: path={}", path))
}

fn read_all_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
) -> Result<Vec<u8>, String> {
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read(path)) {
            Ok(v) => return Ok(v.to_vec()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("metadata file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read metadata file failed: path={}, error={}",
                    path, e
                ));
            }
        }
    }
    Err(format!(
        "read metadata file failed after retries: path={}",
        path
    ))
}

fn read_range_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end <= start {
        return Err(format!(
            "invalid read range for segment file: path={}, start={}, end={}",
            path, start, end
        ));
    }
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read_with(path).range(start..end).into_future()) {
            Ok(v) => return Ok(v.to_vec()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("segment file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read segment file range failed: path={}, range={}..{}, error={}",
                    path, start, end, e
                ));
            }
        }
    }
    Err(format!(
        "read segment file range failed after retries: path={}, range={}..{}",
        path, start, end
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Parsed tablet root location for metadata/segment object access.
enum TabletRoot {
    Local {
        root_dir: String,
    },
    ObjectStore {
        scheme: String,
        bucket: String,
        root_path: String,
    },
}

impl TabletRoot {
    fn parse(tablet_root_path: &str) -> Result<Self, String> {
        let raw = tablet_root_path.trim().trim_end_matches('/');
        if raw.is_empty() {
            return Err("tablet_root_path is empty".to_string());
        }

        if let Some(rest) = raw
            .strip_prefix("s3://")
            .or_else(|| raw.strip_prefix("oss://"))
        {
            let scheme = if raw.starts_with("s3://") {
                "s3".to_string()
            } else {
                "oss".to_string()
            };
            let (bucket, root_path) = split_bucket_and_root(rest)?;
            return Ok(Self::ObjectStore {
                scheme,
                bucket,
                root_path,
            });
        }

        if raw.contains("://") {
            return Err(format!(
                "unsupported tablet_root_path scheme for native metadata loader: {raw}"
            ));
        }
        Ok(Self::Local {
            root_dir: raw.to_string(),
        })
    }

    fn join_relative_path(&self, rel_path: &str) -> String {
        let rel = rel_path.trim_start_matches('/');
        match self {
            Self::Local { root_dir } => {
                if rel.is_empty() {
                    root_dir.clone()
                } else {
                    format!("{}/{}", root_dir.trim_end_matches('/'), rel)
                }
            }
            Self::ObjectStore {
                scheme,
                bucket,
                root_path,
            } => {
                let root = root_path.trim_matches('/');
                if root.is_empty() {
                    format!("{scheme}://{bucket}/{rel}")
                } else {
                    format!("{scheme}://{bucket}/{root}/{rel}")
                }
            }
        }
    }
}

fn split_bucket_and_root(value: &str) -> Result<(String, String), String> {
    let trimmed = value.trim_matches('/');
    let (bucket, root_path) = match trimmed.split_once('/') {
        Some((bucket, path)) => (bucket, path),
        None => (trimmed, ""),
    };
    if bucket.trim().is_empty() {
        return Err(format!("missing bucket in tablet_root_path: {value}"));
    }
    Ok((bucket.to_string(), root_path.trim_matches('/').to_string()))
}

fn build_operator(
    root: &TabletRoot,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<Operator, String> {
    match root {
        TabletRoot::Local { root_dir } => {
            let builder = opendal::services::Fs::default().root(root_dir);
            let operator_builder = Operator::new(builder)
                .map_err(|e| format!("init local metadata operator failed: {}", e))?;
            Ok(operator_builder.finish())
        }
        TabletRoot::ObjectStore {
            scheme: _,
            bucket,
            root_path,
        } => {
            let profile = object_store_profile.ok_or_else(|| {
                format!(
                    "missing object store profile for metadata loader: path={}://{}/{}",
                    "s3", bucket, root_path
                )
            })?;
            build_s3_operator(bucket, root_path, profile)
        }
    }
}

fn build_s3_operator(
    bucket: &str,
    root_path: &str,
    object_store_profile: &ObjectStoreProfile,
) -> Result<Operator, String> {
    let cfg = object_store_profile.to_object_store_config(bucket, root_path);
    crate::fs::object_store::build_oss_operator(&cfg)
        .map_err(|e| format!("init object store metadata operator failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::grpc_client::proto::starrocks::{
        BinaryPredicatePb, DeletePredicatePb, InPredicatePb, IsNullPredicatePb, KeysType,
        RowsetMetadataPb, TabletSchemaPb,
    };
    use prost::Message;
    use tempfile::TempDir;

    #[test]
    fn load_bundle_metadata_snapshot_from_local_file() {
        let tablet_id = 10001_i64;
        let version = 42_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("meta")).expect("create meta dir");

        let tablet_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                id: Some(7),
                segments: vec!["a.dat".to_string(), "b.dat".to_string()],
                num_rows: Some(11),
                segment_size: vec![64, 128],
                bundle_file_offsets: vec![10, 20],
                ..Default::default()
            }],
            rowset_to_schema: [(1_u32, 7_i64)].into_iter().collect(),
            ..Default::default()
        };
        let tablet_metadata_bytes = tablet_metadata.encode_to_vec();

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(tablet_id, 7_i64)].into_iter().collect(),
            schemas: [(
                7_i64,
                TabletSchemaPb {
                    keys_type: Some(KeysType::DupKeys as i32),
                    id: Some(7_i64),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            tablet_meta_pages: [(
                tablet_id,
                PagePointerPb {
                    offset: 0_u64,
                    size: u32::try_from(tablet_metadata_bytes.len())
                        .expect("metadata bytes fit u32"),
                },
            )]
            .into_iter()
            .collect(),
        };
        let bundle_bytes = bundle.encode_to_vec();
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&tablet_metadata_bytes);
        file_bytes.extend_from_slice(&bundle_bytes);
        file_bytes.extend_from_slice(
            &u64::try_from(bundle_bytes.len())
                .expect("bundle bytes fit u64")
                .to_le_bytes(),
        );

        let bundle_metadata_file = temp_dir
            .path()
            .join(metadata_rel_path(0, version).expect("bundle metadata rel path"));
        std::fs::write(&bundle_metadata_file, file_bytes).expect("write bundle metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load bundle snapshot");

        assert_eq!(snapshot.tablet_id, tablet_id);
        assert_eq!(snapshot.version, version);
        assert_eq!(snapshot.total_num_rows, 11);
        assert_eq!(snapshot.rowset_count, 1);
        assert_eq!(snapshot.segment_files.len(), 2);
        assert_eq!(snapshot.segment_files[0].name, "a.dat");
        assert_eq!(snapshot.segment_files[0].relative_path, "data/a.dat");
        assert_eq!(snapshot.segment_files[0].segment_id, Some(7));
        assert_eq!(snapshot.segment_files[0].bundle_file_offset, Some(10));
        assert_eq!(snapshot.segment_files[0].segment_size, Some(64));
        assert_eq!(snapshot.segment_files[1].segment_id, Some(8));
        assert!(
            snapshot.segment_files[0].path.ends_with("/data/a.dat"),
            "path={}",
            snapshot.segment_files[0].path
        );
    }

    #[test]
    fn load_standalone_metadata_snapshot() {
        let tablet_id = 10002_i64;
        let version = 43_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("meta")).expect("create meta dir");
        let standalone_rel = metadata_rel_path(tablet_id, version).expect("standalone rel path");
        let standalone_file = temp_dir.path().join(&standalone_rel);
        std::fs::create_dir_all(standalone_file.parent().expect("standalone parent exists"))
            .expect("create standalone parent");
        let standalone_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                segments: vec!["seg0.dat".to_string()],
                num_rows: Some(7),
                ..Default::default()
            }],
            schema: Some(TabletSchemaPb {
                keys_type: Some(KeysType::DupKeys as i32),
                id: Some(9_i64),
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(&standalone_file, standalone_metadata.encode_to_vec())
            .expect("write standalone metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load standalone snapshot");
        assert_eq!(snapshot.tablet_id, tablet_id);
        assert_eq!(snapshot.version, version);
        assert_eq!(snapshot.total_num_rows, 7);
        assert_eq!(snapshot.rowset_count, 1);
        assert_eq!(snapshot.segment_files.len(), 1);
        assert_eq!(snapshot.segment_files[0].relative_path, "data/seg0.dat");
        assert_eq!(snapshot.segment_files[0].bundle_file_offset, None);
        assert_eq!(snapshot.segment_files[0].segment_size, None);
    }

    #[test]
    fn fallback_to_tablet_subdir_standalone_metadata_snapshot() {
        let tablet_id = 10020_i64;
        let version = 63_i64;
        let temp_dir = TempDir::new().expect("create temp dir");

        let tablet_root = temp_dir.path().join(tablet_id.to_string());
        let standalone_rel = metadata_rel_path(tablet_id, version).expect("standalone rel path");
        let standalone_file = tablet_root.join(&standalone_rel);
        std::fs::create_dir_all(standalone_file.parent().expect("standalone parent exists"))
            .expect("create standalone parent");
        let standalone_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            schema: Some(TabletSchemaPb {
                keys_type: Some(KeysType::DupKeys as i32),
                id: Some(20_i64),
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(&standalone_file, standalone_metadata.encode_to_vec())
            .expect("write standalone metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load snapshot through tablet path fallback");
        assert_eq!(snapshot.tablet_id, tablet_id);
        assert_eq!(snapshot.version, version);
        assert_eq!(snapshot.total_num_rows, 0);
        assert!(
            snapshot.metadata_path.ends_with(&format!(
                "{tablet_id}/meta/{tablet_id:016X}_{version:016X}.meta"
            )),
            "path={}",
            snapshot.metadata_path
        );
    }

    #[test]
    fn fallback_to_tablet_subdir_bundle_metadata_snapshot() {
        let tablet_id = 10021_i64;
        let version = 64_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        let tablet_root = temp_dir.path().join(tablet_id.to_string());
        std::fs::create_dir_all(tablet_root.join("meta")).expect("create tablet meta dir");

        let tablet_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                segments: vec!["seg.dat".to_string()],
                num_rows: Some(5),
                segment_size: vec![10],
                bundle_file_offsets: vec![0],
                ..Default::default()
            }],
            rowset_to_schema: [(1_u32, 21_i64)].into_iter().collect(),
            ..Default::default()
        };
        let tablet_metadata_bytes = tablet_metadata.encode_to_vec();

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(tablet_id, 21_i64)].into_iter().collect(),
            schemas: [(
                21_i64,
                TabletSchemaPb {
                    keys_type: Some(KeysType::DupKeys as i32),
                    id: Some(21_i64),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            tablet_meta_pages: [(
                tablet_id,
                PagePointerPb {
                    offset: 0_u64,
                    size: u32::try_from(tablet_metadata_bytes.len())
                        .expect("metadata bytes fit u32"),
                },
            )]
            .into_iter()
            .collect(),
        };
        let bundle_bytes = bundle.encode_to_vec();
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&tablet_metadata_bytes);
        file_bytes.extend_from_slice(&bundle_bytes);
        file_bytes.extend_from_slice(
            &u64::try_from(bundle_bytes.len())
                .expect("bundle bytes fit u64")
                .to_le_bytes(),
        );

        let bundle_metadata_file =
            tablet_root.join(metadata_rel_path(0, version).expect("bundle metadata rel path"));
        std::fs::create_dir_all(bundle_metadata_file.parent().expect("bundle parent exists"))
            .expect("create bundle parent");
        std::fs::write(&bundle_metadata_file, file_bytes).expect("write bundle metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load bundle snapshot through tablet path fallback");
        assert_eq!(snapshot.tablet_id, tablet_id);
        assert_eq!(snapshot.version, version);
        assert_eq!(snapshot.total_num_rows, 5);
        assert_eq!(snapshot.segment_files.len(), 1);
        assert!(
            snapshot.metadata_path.ends_with(&format!(
                "{tablet_id}/meta/{:016X}_{version:016X}.meta",
                0_u64
            )),
            "path={}",
            snapshot.metadata_path
        );
    }

    #[test]
    fn load_bundle_segment_footers_from_local_shared_file() {
        let tablet_id = 10003_i64;
        let version = 44_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("meta")).expect("create meta dir");
        std::fs::create_dir_all(temp_dir.path().join("data")).expect("create data dir");

        let segment_0 = build_test_segment_bytes(1, 3, 8);
        let segment_1 = build_test_segment_bytes(2, 7, 16);
        let segment_0_size = u64::try_from(segment_0.len()).expect("segment0 size fit u64");
        let segment_1_size = u64::try_from(segment_1.len()).expect("segment1 size fit u64");
        let segment_1_offset = i64::try_from(segment_0.len()).expect("segment1 offset fit i64");
        let mut shared_file = Vec::new();
        shared_file.extend_from_slice(&segment_0);
        shared_file.extend_from_slice(&segment_1);
        std::fs::write(temp_dir.path().join("data/shared.dat"), shared_file)
            .expect("write shared segment file");

        let tablet_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                segments: vec!["shared.dat".to_string(), "shared.dat".to_string()],
                num_rows: Some(10),
                segment_size: vec![segment_0_size, segment_1_size],
                bundle_file_offsets: vec![0, segment_1_offset],
                ..Default::default()
            }],
            rowset_to_schema: [(1_u32, 7_i64)].into_iter().collect(),
            ..Default::default()
        };
        let tablet_metadata_bytes = tablet_metadata.encode_to_vec();

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(tablet_id, 7_i64)].into_iter().collect(),
            schemas: [(
                7_i64,
                TabletSchemaPb {
                    keys_type: Some(KeysType::DupKeys as i32),
                    id: Some(7_i64),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            tablet_meta_pages: [(
                tablet_id,
                PagePointerPb {
                    offset: 0_u64,
                    size: u32::try_from(tablet_metadata_bytes.len())
                        .expect("metadata bytes fit u32"),
                },
            )]
            .into_iter()
            .collect(),
        };
        let bundle_bytes = bundle.encode_to_vec();
        let mut metadata_file_bytes = Vec::new();
        metadata_file_bytes.extend_from_slice(&tablet_metadata_bytes);
        metadata_file_bytes.extend_from_slice(&bundle_bytes);
        metadata_file_bytes.extend_from_slice(
            &u64::try_from(bundle_bytes.len())
                .expect("bundle bytes fit u64")
                .to_le_bytes(),
        );

        let bundle_metadata_file = temp_dir
            .path()
            .join(metadata_rel_path(0, version).expect("bundle metadata rel path"));
        std::fs::write(&bundle_metadata_file, metadata_file_bytes).expect("write bundle metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load bundle snapshot");
        let footers = load_bundle_segment_footers(
            &snapshot,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("load segment footers");

        assert_eq!(footers.len(), 2);
        assert_eq!(footers[0].version, 1);
        assert_eq!(footers[0].num_rows, Some(3));
        assert_eq!(footers[1].version, 2);
        assert_eq!(footers[1].num_rows, Some(7));
    }

    #[test]
    fn accept_unique_table_model() {
        let tablet_id = 10004_i64;
        let version = 45_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("meta")).expect("create meta dir");

        let tablet_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                segments: vec!["a.dat".to_string()],
                num_rows: Some(3),
                segment_size: vec![64],
                bundle_file_offsets: vec![0],
                ..Default::default()
            }],
            rowset_to_schema: [(1_u32, 9_i64)].into_iter().collect(),
            ..Default::default()
        };
        let tablet_metadata_bytes = tablet_metadata.encode_to_vec();

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(tablet_id, 9_i64)].into_iter().collect(),
            schemas: [(
                9_i64,
                TabletSchemaPb {
                    keys_type: Some(KeysType::UniqueKeys as i32),
                    id: Some(9_i64),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            tablet_meta_pages: [(
                tablet_id,
                PagePointerPb {
                    offset: 0_u64,
                    size: u32::try_from(tablet_metadata_bytes.len())
                        .expect("metadata bytes fit u32"),
                },
            )]
            .into_iter()
            .collect(),
        };
        let bundle_bytes = bundle.encode_to_vec();
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&tablet_metadata_bytes);
        file_bytes.extend_from_slice(&bundle_bytes);
        file_bytes.extend_from_slice(
            &u64::try_from(bundle_bytes.len())
                .expect("bundle bytes fit u64")
                .to_le_bytes(),
        );

        let bundle_metadata_file = temp_dir
            .path()
            .join(metadata_rel_path(0, version).expect("bundle metadata rel path"));
        std::fs::write(&bundle_metadata_file, file_bytes).expect("write bundle metadata");

        let snapshot = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("unique model should be accepted");
        assert_eq!(snapshot.tablet_id, tablet_id);
        assert_eq!(snapshot.version, version);
    }

    #[test]
    fn reject_missing_keys_type_in_schema() {
        let tablet_id = 10005_i64;
        let version = 46_i64;
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("meta")).expect("create meta dir");

        let tablet_metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            rowsets: vec![RowsetMetadataPb {
                segments: vec!["a.dat".to_string()],
                num_rows: Some(3),
                segment_size: vec![64],
                bundle_file_offsets: vec![0],
                ..Default::default()
            }],
            rowset_to_schema: [(1_u32, 10_i64)].into_iter().collect(),
            ..Default::default()
        };
        let tablet_metadata_bytes = tablet_metadata.encode_to_vec();

        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(tablet_id, 10_i64)].into_iter().collect(),
            schemas: [(
                10_i64,
                TabletSchemaPb {
                    id: Some(10_i64),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            tablet_meta_pages: [(
                tablet_id,
                PagePointerPb {
                    offset: 0_u64,
                    size: u32::try_from(tablet_metadata_bytes.len())
                        .expect("metadata bytes fit u32"),
                },
            )]
            .into_iter()
            .collect(),
        };
        let bundle_bytes = bundle.encode_to_vec();
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&tablet_metadata_bytes);
        file_bytes.extend_from_slice(&bundle_bytes);
        file_bytes.extend_from_slice(
            &u64::try_from(bundle_bytes.len())
                .expect("bundle bytes fit u64")
                .to_le_bytes(),
        );

        let bundle_metadata_file = temp_dir
            .path()
            .join(metadata_rel_path(0, version).expect("bundle metadata rel path"));
        std::fs::write(&bundle_metadata_file, file_bytes).expect("write bundle metadata");

        let err = load_tablet_snapshot(
            tablet_id,
            version,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect_err("schema without keys_type should be rejected");
        assert!(err.contains("missing keys_type"), "err={}", err);
    }

    #[test]
    fn collect_segment_files_uses_rowset_index_for_visibility_version() {
        let root = TabletRoot::parse("/tmp/starrocks_tablet").expect("parse local root");
        let metadata = TabletMetadataPb {
            rowsets: vec![
                RowsetMetadataPb {
                    id: Some(1),
                    segments: vec!["r0.dat".to_string()],
                    segment_size: vec![10],
                    bundle_file_offsets: vec![0],
                    version: Some(1234),
                    ..Default::default()
                },
                RowsetMetadataPb {
                    id: Some(2),
                    segments: vec!["r1.dat".to_string()],
                    segment_size: vec![10],
                    bundle_file_offsets: vec![10],
                    version: None,
                    delete_predicate: Some(DeletePredicatePb {
                        // Keep lake semantic: ignore delete_predicate.version, use rowset index.
                        version: -1,
                        sub_predicates: vec!["c1=1".to_string()],
                        in_predicates: vec![InPredicatePb {
                            column_name: Some("c2".to_string()),
                            is_not_in: Some(false),
                            values: vec!["10".to_string(), "20".to_string()],
                        }],
                        binary_predicates: vec![BinaryPredicatePb {
                            column_name: Some("c3".to_string()),
                            op: Some("=".to_string()),
                            value: Some("abc".to_string()),
                        }],
                        is_null_predicates: vec![IsNullPredicatePb {
                            column_name: Some("c4".to_string()),
                            is_not_null: Some(true),
                        }],
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let (segments, delete_predicates) =
            collect_segment_files(&root, &metadata).expect("collect segment files");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].rowset_version, 0);
        assert_eq!(segments[0].segment_id, Some(1));
        assert_eq!(segments[1].rowset_version, 1);
        assert_eq!(segments[1].segment_id, Some(2));
        assert_eq!(delete_predicates.len(), 1);
        assert_eq!(delete_predicates[0].version, 1);
        assert!(delete_predicates[0].sub_predicates.is_empty());
        assert_eq!(delete_predicates[0].in_predicates.len(), 1);
        assert_eq!(delete_predicates[0].binary_predicates.len(), 1);
        assert_eq!(delete_predicates[0].is_null_predicates.len(), 1);
    }

    #[test]
    fn collect_delvec_meta_extracts_version_file_and_segment_page_mapping() {
        let metadata = TabletMetadataPb {
            delvec_meta: Some(
                crate::service::grpc_client::proto::starrocks::DelvecMetadataPb {
                    version_to_file: [(
                        8_i64,
                        crate::service::grpc_client::proto::starrocks::FileMetaPb {
                            name: Some("delvec_8".to_string()),
                            ..Default::default()
                        },
                    )]
                    .into_iter()
                    .collect(),
                    delvecs: [(
                        12_u32,
                        crate::service::grpc_client::proto::starrocks::DelvecPagePb {
                            version: Some(8),
                            offset: Some(100),
                            size: Some(64),
                            crc32c: Some(1234),
                            crc32c_gen_version: Some(8),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            ),
            ..Default::default()
        };

        let delvec_meta = collect_delvec_meta(&metadata).expect("collect delvec meta");
        assert_eq!(
            delvec_meta
                .version_to_file_rel_path
                .get(&8)
                .map(String::as_str),
            Some("data/delvec_8")
        );
        let page = delvec_meta
            .segment_delvec_pages
            .get(&12)
            .expect("segment delvec page");
        assert_eq!(page.version, 8);
        assert_eq!(page.offset, 100);
        assert_eq!(page.size, 64);
        assert_eq!(page.crc32c, Some(1234));
        assert_eq!(page.crc32c_gen_version, Some(8));
    }

    #[test]
    fn reject_delete_predicate_with_missing_column_name() {
        let root = TabletRoot::parse("/tmp/starrocks_tablet").expect("parse local root");
        let metadata = TabletMetadataPb {
            rowsets: vec![RowsetMetadataPb {
                id: Some(3),
                segments: vec!["r0.dat".to_string()],
                segment_size: vec![10],
                bundle_file_offsets: vec![0],
                delete_predicate: Some(DeletePredicatePb {
                    version: 1,
                    in_predicates: vec![InPredicatePb {
                        column_name: None,
                        is_not_in: Some(false),
                        values: vec!["1".to_string()],
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = collect_segment_files(&root, &metadata)
            .expect_err("missing delete predicate column name should fail");
        assert!(err.contains("column_name is missing"), "err={err}");
    }

    #[derive(Clone, PartialEq, Message)]
    struct TestSegmentFooterPb {
        #[prost(uint32, optional, tag = "1")]
        version: Option<u32>,
        #[prost(uint32, optional, tag = "3")]
        num_rows: Option<u32>,
    }

    fn build_test_segment_bytes(version: u32, num_rows: u32, payload_len: usize) -> Vec<u8> {
        let footer = TestSegmentFooterPb {
            version: Some(version),
            num_rows: Some(num_rows),
        };
        let footer_bytes = footer.encode_to_vec();
        let footer_len = u32::try_from(footer_bytes.len()).expect("footer bytes fit u32");
        let checksum = crc32c::crc32c(&footer_bytes);

        let mut bytes = vec![0_u8; payload_len];
        bytes.extend_from_slice(&footer_bytes);
        bytes.extend_from_slice(&footer_len.to_le_bytes());
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(b"D0R1");
        bytes
    }
}
