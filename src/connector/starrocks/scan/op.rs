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
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;

use crate::common::ids::SlotId;
use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::object_store_profile::ObjectStoreProfile;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::metrics;
use crate::novarocks_logging::{info, warn};
use crate::runtime::profile::RuntimeProfile;
use crate::runtime::starlet_shard_registry;
use crate::types;

use super::reader::StarRocksNativeReader;

pub type QueryGlobalDictEncodeMap = HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>;

#[derive(Clone, Debug)]
pub struct StarRocksScanRange {
    pub tablet_id: i64,
    pub partition_id: Option<i64>,
    pub version: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct LakeScanSchemaMeta {
    pub db_id: i64,
    pub table_id: i64,
    pub schema_id: i64,
    pub fe_addr: Option<types::TNetworkAddress>,
    pub query_id: Option<types::TUniqueId>,
}

#[derive(Clone, Debug)]
pub struct StarRocksScanConfig {
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub properties: BTreeMap<String, String>,
    pub ranges: Vec<StarRocksScanRange>,
    pub has_more: bool,
    pub required_chunk_schema: ChunkSchemaRef,
    pub output_chunk_schema: ChunkSchemaRef,
    pub query_global_dicts: QueryGlobalDictEncodeMap,
    pub limit: Option<usize>,
    pub batch_size: Option<i32>,
    pub query_timeout: Option<i32>,
    pub mem_limit: Option<i64>,
    pub profile_label: Option<String>,
    pub min_max_predicates: Vec<MinMaxPredicate>,
    pub lake_schema_meta: Option<LakeScanSchemaMeta>,
    /// Maps TopN runtime filter_id → scan column name.
    /// Populated during lowering so that execute_iter() can convert
    /// `RuntimeMinMaxFilter` instances into `MinMaxPredicate` values
    /// for storage-level segment pruning.
    pub topn_filter_column_map: HashMap<i32, String>,
}

#[derive(Clone, Debug)]
struct StarRocksExecutionContext {
    partition_storage_paths: HashMap<i64, String>,
    object_store_profile: Option<ObjectStoreProfile>,
}

impl StarRocksExecutionContext {
    fn from_scan_config(cfg: &StarRocksScanConfig) -> Result<Self, String> {
        let partition_storage_paths = resolve_partition_storage_paths(cfg)?;
        let object_store_profile =
            resolve_object_store_profile(&cfg.properties, partition_storage_paths.values())?;
        Ok(Self {
            partition_storage_paths,
            object_store_profile,
        })
    }
}

#[derive(Clone, Debug)]
pub struct StarRocksScanOp {
    cfg: StarRocksScanConfig,
}

impl StarRocksScanOp {
    pub fn new(cfg: StarRocksScanConfig) -> Self {
        Self { cfg }
    }
}

impl ScanOp for StarRocksScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let ScanMorsel::StarRocksRange { index, .. } = morsel else {
            return Err("starrocks scan received unexpected morsel".to_string());
        };

        let range = self
            .cfg
            .ranges
            .get(index)
            .cloned()
            .ok_or_else(|| format!("starrocks scan range index out of bounds: {index}"))?;
        let mut cfg = self.cfg.clone();
        cfg.ranges = vec![range];
        cfg.limit = None;

        // Apply MinMax runtime filters from TopN as storage-level predicates.
        if let Some(rf_ctx) = runtime_filters {
            let mm_filters = rf_ctx.min_max_filters();
            for (filter_id, filter) in mm_filters {
                if let Some(column_name) = cfg.topn_filter_column_map.get(&filter_id) {
                    match filter.to_min_max_predicates(column_name) {
                        Ok(preds) => cfg.min_max_predicates.extend(preds),
                        Err(e) => {
                            warn!(
                                "failed to convert runtime min/max filter {} to predicates: {}",
                                filter_id, e
                            );
                        }
                    }
                }
            }
        }

        let ctx = StarRocksExecutionContext::from_scan_config(&cfg)?;

        if let Some(profile) = profile.as_ref() {
            profile.add_info_string("DataSourceType", "StarRocks");
            profile.add_info_string("RangeCount", format!("{}", cfg.ranges.len()));
            profile.add_info_string("ReaderBackend", "rust_native");
        }

        let iter = StarRocksScanIter::new(cfg, ctx, profile);
        Ok(Box::new(iter))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let morsels = self
            .cfg
            .ranges
            .iter()
            .enumerate()
            .map(|(index, range)| ScanMorsel::StarRocksRange {
                index,
                tablet_id: range.tablet_id,
            })
            .collect();
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn profile_name(&self) -> Option<String> {
        let label = self.cfg.profile_label.as_deref()?;
        if let Some(id) = label
            .strip_prefix("starrocks_scan_node_id=")
            .and_then(|s| s.parse::<i32>().ok())
        {
            return Some(format!("STARROCKS_SCAN (id={id})"));
        }
        if let Some(id) = label
            .strip_prefix("lake_scan_node_id=")
            .and_then(|s| s.parse::<i32>().ok())
        {
            return Some(format!("LAKE_SCAN (id={id})"));
        }
        Some("STARROCKS_SCAN".to_string())
    }
}

struct StarRocksScanIter {
    cfg: StarRocksScanConfig,
    ctx: StarRocksExecutionContext,
    profile: Option<RuntimeProfile>,
    range_idx: usize,
    scanner: Option<StarRocksScanner>,
    finished: bool,
    total_rows: usize,
}

impl StarRocksScanIter {
    fn new(
        cfg: StarRocksScanConfig,
        ctx: StarRocksExecutionContext,
        profile: Option<RuntimeProfile>,
    ) -> Self {
        Self {
            cfg,
            ctx,
            profile,
            range_idx: 0,
            scanner: None,
            finished: false,
            total_rows: 0,
        }
    }

    fn close_scanner(&mut self) {
        if let Some(scanner) = self.scanner.as_mut() {
            if let Err(e) = scanner.close() {
                warn!("failed to close starrocks scanner: {}", e);
            }
        }
        self.scanner = None;
    }

    fn open_next_scanner(&mut self) -> Result<(), String> {
        self.close_scanner();
        if self.range_idx >= self.cfg.ranges.len() {
            return Err("no more starrocks scan ranges".to_string());
        }
        let range = self.cfg.ranges[self.range_idx].clone();
        let open_start = Instant::now();
        let scanner = StarRocksScanner::open(&self.cfg, &self.ctx, range)?;
        self.scanner = Some(scanner);
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add(
                "ScannerOpenTime",
                metrics::TUnit::TIME_NS,
                open_start.elapsed().as_nanos() as i64,
            );
        }
        Ok(())
    }

    fn remaining_limit(&self) -> Option<usize> {
        self.cfg
            .limit
            .map(|limit| limit.saturating_sub(self.total_rows))
    }
}

impl Iterator for StarRocksScanIter {
    type Item = Result<Chunk, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Some(remaining) = self.remaining_limit() {
            if remaining == 0 {
                self.finished = true;
                self.close_scanner();
                return None;
            }
        }

        loop {
            if self.scanner.is_none() {
                if self.range_idx >= self.cfg.ranges.len() {
                    self.finished = true;
                    return None;
                }
                if let Err(e) = self.open_next_scanner() {
                    self.finished = true;
                    return Some(Err(e));
                }
            }

            let Some(scanner) = self.scanner.as_mut() else {
                self.finished = true;
                return None;
            };

            let batch_start = Instant::now();
            match scanner.get_next() {
                Ok(ScanBatch::Eos) => {
                    self.close_scanner();
                    self.range_idx += 1;
                    continue;
                }
                Ok(ScanBatch::Chunk(chunk, rows, bytes)) => {
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add("ExternalRowsRead", metrics::TUnit::UNIT, rows as i64);
                        profile.counter_add(
                            "ExternalBytesRead",
                            metrics::TUnit::BYTES,
                            bytes as i64,
                        );
                        profile.counter_add(
                            "ScannerGetNextTime",
                            metrics::TUnit::TIME_NS,
                            batch_start.elapsed().as_nanos() as i64,
                        );
                    }

                    let mut out_chunk = chunk;
                    let chunk_rows = rows;
                    let remaining = self.remaining_limit();
                    if let Some(remaining) = remaining {
                        if chunk_rows > remaining {
                            out_chunk = out_chunk.slice(0, remaining);
                            self.total_rows = self.total_rows.saturating_add(remaining);
                            self.finished = true;
                            self.close_scanner();
                            return Some(Ok(out_chunk));
                        }
                    }

                    self.total_rows = self.total_rows.saturating_add(chunk_rows);
                    return Some(Ok(out_chunk));
                }
                Err(e) => {
                    self.close_scanner();
                    return Some(Err(e));
                }
            }
        }
    }
}

enum ScanBatch {
    Eos,
    Chunk(Chunk, usize, usize),
}

struct StarRocksScanner {
    reader: StarRocksNativeReader,
    output_chunk_schema: ChunkSchemaRef,
}

impl StarRocksScanner {
    fn open(
        cfg: &StarRocksScanConfig,
        ctx: &StarRocksExecutionContext,
        range: StarRocksScanRange,
    ) -> Result<Self, String> {
        let output_slot_meta = cfg
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| (slot.name().to_string(), slot.slot_id()))
            .collect::<Vec<_>>();
        info!(
            "StarRocksScanner::open tablet_id={} output_slot_ids={:?} output_slot_meta={:?} query_global_dict_slots={:?}",
            range.tablet_id,
            cfg.output_chunk_schema.slot_ids(),
            output_slot_meta,
            cfg.query_global_dicts.keys().collect::<Vec<_>>()
        );
        let partition_id = range.partition_id.ok_or_else(|| {
            format!(
                "missing partition_id in starrocks scan range for tablet_id {}",
                range.tablet_id
            )
        })?;
        let storage_path = ctx
            .partition_storage_paths
            .get(&partition_id)
            .ok_or_else(|| {
                format!(
                    "missing partition_storage_path for partition_id {} (tablet_id={}) in partition_storage_paths",
                    partition_id, range.tablet_id
                )
            })?
            .clone();

        let version = range
            .version
            .ok_or_else(|| format!("missing tablet version for tablet_id {}", range.tablet_id))?;
        eprintln!(
            "[DEBUG] StarRocksScanner::open tablet_id={} partition_id={} version={} storage_path={}",
            range.tablet_id, partition_id, version, storage_path
        );

        let reader = StarRocksNativeReader::open(
            range.tablet_id,
            &storage_path,
            version,
            cfg.required_chunk_schema.clone(),
            cfg.output_chunk_schema.clone(),
            cfg.query_global_dicts.clone(),
            cfg.min_max_predicates.clone(),
            ctx.object_store_profile.as_ref(),
            cfg.lake_schema_meta.as_ref(),
        )?;

        Ok(Self {
            reader,
            output_chunk_schema: cfg.output_chunk_schema.clone(),
        })
    }

    fn get_next(&mut self) -> Result<ScanBatch, String> {
        let output_schema = self.output_chunk_schema.arrow_schema_ref();
        let batch = self.reader.get_next(&output_schema)?;
        match batch {
            None => Ok(ScanBatch::Eos),
            Some(batch) => {
                let rows = batch.num_rows();
                let chunk =
                    Chunk::try_new_with_chunk_schema(batch, self.output_chunk_schema.clone())?;
                let bytes = chunk.logical_bytes();
                Ok(ScanBatch::Chunk(chunk, rows, bytes))
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.reader.close()
    }
}

pub(crate) fn build_native_object_store_profile_from_properties(
    props: &BTreeMap<String, String>,
) -> Result<Option<ObjectStoreProfile>, String> {
    ObjectStoreProfile::from_properties_optional(props)
}

fn resolve_partition_storage_paths(
    cfg: &StarRocksScanConfig,
) -> Result<HashMap<i64, String>, String> {
    if let Some(paths) = parse_partition_storage_paths_optional(&cfg.properties)? {
        return Ok(paths);
    }
    Err(
        "starrocks direct read requires FE to provide partition_storage_paths in execution properties"
            .to_string(),
    )
}

fn parse_partition_storage_paths_optional(
    props: &BTreeMap<String, String>,
) -> Result<Option<HashMap<i64, String>>, String> {
    let Some(raw) = props.get("partition_storage_paths") else {
        return Ok(None);
    };

    let value: Value = serde_json::from_str(raw)
        .map_err(|e| format!("parse partition_storage_paths json failed: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "partition_storage_paths must be a JSON object".to_string())?;

    let mut out = HashMap::with_capacity(obj.len());
    for (key, value) in obj {
        let partition_id = key
            .parse::<i64>()
            .map_err(|_| format!("invalid partition_id in partition_storage_paths: {key}"))?;
        let path = value
            .as_str()
            .ok_or_else(|| format!("partition_storage_paths entry for {key} is not a string"))?;
        if path.is_empty() {
            return Err(format!("partition_storage_paths entry for {key} is empty"));
        }
        out.insert(partition_id, path.to_string());
    }

    Ok(Some(out))
}

fn resolve_object_store_profile<'a>(
    props: &BTreeMap<String, String>,
    storage_paths: impl Iterator<Item = &'a String>,
) -> Result<Option<ObjectStoreProfile>, String> {
    if let Some(profile) = ObjectStoreProfile::from_properties_optional(props)? {
        eprintln!(
            "[DEBUG] starrocks direct read explicit object store profile endpoint={}",
            profile.endpoint
        );
        info!(
            "starrocks direct read uses explicit object store profile endpoint={}",
            profile.endpoint
        );
        return Ok(Some(profile));
    }

    let paths = storage_paths.cloned().collect::<Vec<_>>();
    if paths.is_empty() {
        return Ok(None);
    }
    match classify_scan_paths(paths.iter().map(|path| path.as_str()))? {
        ScanPathScheme::Local => Ok(None),
        ScanPathScheme::Hdfs => Err(
            "starrocks direct read does not support hdfs tablet paths without explicit cloud configuration"
                .to_string(),
        ),
        ScanPathScheme::Oss => {
            let mut selected: Option<crate::runtime::starlet_shard_registry::S3StoreConfig> = None;
            for path in &paths {
                let s3 = starlet_shard_registry::infer_s3_config_for_path(path).ok_or_else(|| {
                    format!(
                        "missing object store config for direct-read path={path}; provide aws.s3.* \
                         properties or ensure shard/env credentials can be inferred"
                    )
                })?;
                match selected.as_ref() {
                    None => selected = Some(s3),
                    Some(prev) if prev == &s3 => {}
                    Some(prev) => {
                        return Err(format!(
                            "inconsistent inferred object store configs across starrocks direct-read paths: \
                             current_bucket={} current_endpoint={} previous_bucket={} previous_endpoint={}",
                            s3.bucket, s3.endpoint, prev.bucket, prev.endpoint
                        ));
                    }
                }
            }
            selected
                .as_ref()
                .map(|config| {
                    eprintln!(
                        "[DEBUG] starrocks direct read inferred object store config bucket={} endpoint={} root={}",
                        config.bucket, config.endpoint, config.root
                    );
                    info!(
                        "starrocks direct read inferred object store config bucket={} endpoint={} root={}",
                        config.bucket, config.endpoint, config.root
                    );
                    ObjectStoreProfile::from_s3_store_config(config)
                })
                .transpose()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn mock_scan_config(profile_label: Option<String>) -> StarRocksScanConfig {
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("c1", DataType::Int64, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        StarRocksScanConfig {
            db_name: None,
            table_name: None,
            properties: BTreeMap::new(),
            ranges: Vec::new(),
            has_more: false,
            required_chunk_schema: chunk_schema.clone(),
            output_chunk_schema: chunk_schema,
            query_global_dicts: HashMap::new(),
            limit: None,
            batch_size: None,
            query_timeout: None,
            mem_limit: None,
            profile_label,
            min_max_predicates: Vec::new(),
            lake_schema_meta: None,
            topn_filter_column_map: HashMap::new(),
        }
    }

    #[test]
    fn build_object_store_profile_keeps_fe_path_style_setting() {
        let mut props = BTreeMap::new();
        props.insert(
            "aws.s3.endpoint".to_string(),
            "http://oss-cn-zhangjiakou.aliyuncs.com".to_string(),
        );
        props.insert("aws.s3.accessKeyId".to_string(), "ak".to_string());
        props.insert("aws.s3.accessKeySecret".to_string(), "sk".to_string());
        props.insert(
            "aws.s3.enable_path_style_access".to_string(),
            "true".to_string(),
        );

        let profile = build_native_object_store_profile_from_properties(&props)
            .expect("build object store profile")
            .expect("profile should exist");
        assert_eq!(
            profile.enable_path_style_access,
            Some(true),
            "path style should follow FE aws.s3.enable_path_style_access"
        );
    }

    #[test]
    fn reject_unsupported_object_storage_prefix() {
        let mut props = BTreeMap::new();
        props.insert("fs.s3a.access.key".to_string(), "ak".to_string());
        let err = build_native_object_store_profile_from_properties(&props)
            .expect_err("unsupported key should fail");
        assert!(
            err.contains("unsupported object storage properties"),
            "err={err}"
        );
    }

    #[test]
    fn profile_name_reports_lake_scan_id() {
        let op = StarRocksScanOp::new(mock_scan_config(Some("lake_scan_node_id=9".to_string())));
        assert_eq!(op.profile_name().as_deref(), Some("LAKE_SCAN (id=9)"));
    }

    #[test]
    fn profile_name_reports_starrocks_scan_id() {
        let op = StarRocksScanOp::new(mock_scan_config(Some(
            "starrocks_scan_node_id=8".to_string(),
        )));
        assert_eq!(op.profile_name().as_deref(), Some("STARROCKS_SCAN (id=8)"));
    }

    #[test]
    fn resolve_partition_storage_paths_requires_fe_execution_properties() {
        let cfg = mock_scan_config(None);
        let err = resolve_partition_storage_paths(&cfg)
            .expect_err("missing partition_storage_paths should fail");
        assert!(
            err.contains("partition_storage_paths"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn resolve_partition_storage_paths_reads_json_map() {
        let mut cfg = mock_scan_config(None);
        cfg.properties.insert(
            "partition_storage_paths".to_string(),
            "{\"17\":\"s3://bucket/root/table/17\"}".to_string(),
        );

        let partition_storage_paths =
            resolve_partition_storage_paths(&cfg).expect("partition_storage_paths should parse");
        assert_eq!(
            partition_storage_paths.get(&17).map(String::as_str),
            Some("s3://bucket/root/table/17")
        );
    }
}
