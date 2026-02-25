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

use arrow::datatypes::SchemaRef;
use serde_json::Value;

use crate::common::ids::SlotId;
use crate::connector::starrocks::object_store_profile::ObjectStoreProfile;
use crate::exec::chunk::Chunk;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use crate::formats::parquet::MinMaxPredicate;
use crate::metrics;
use crate::novarocks_logging::{info, warn};
use crate::runtime::profile::RuntimeProfile;
use crate::types;

use super::reader::StarRocksNativeReader;

pub type QueryGlobalDictEncodeMap = HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>;

#[derive(Clone, Debug)]
pub struct StarRocksScanRange {
    pub tablet_id: i64,
    pub version: Option<i64>,
    pub schema_hash: Option<i32>,
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub hosts: Vec<types::TNetworkAddress>,
    pub fill_data_cache: bool,
    pub skip_page_cache: bool,
    pub skip_disk_cache: bool,
}

#[derive(Clone, Debug)]
pub struct StarRocksScanConfig {
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub opaqued_query_plan: String,
    pub properties: BTreeMap<String, String>,
    pub ranges: Vec<StarRocksScanRange>,
    pub has_more: bool,
    pub required_schema: SchemaRef,
    pub schema: SchemaRef,
    pub slot_ids: Vec<SlotId>,
    pub query_global_dicts: QueryGlobalDictEncodeMap,
    pub limit: Option<usize>,
    pub batch_size: Option<i32>,
    pub query_timeout: Option<i32>,
    pub mem_limit: Option<i64>,
    pub profile_label: Option<String>,
    pub min_max_predicates: Vec<MinMaxPredicate>,
}

#[derive(Clone, Debug)]
struct StarRocksExecutionContext {
    fetch_mode: Option<String>,
    tablet_root_paths: HashMap<i64, String>,
    object_store_profile: ObjectStoreProfile,
}

impl StarRocksExecutionContext {
    fn from_properties(props: &BTreeMap<String, String>) -> Result<Self, String> {
        let fetch_mode = props.get("fetch_mode").cloned().filter(|v| !v.is_empty());
        let tablet_root_paths = parse_tablet_root_paths(props)?;
        let object_store_profile = ObjectStoreProfile::from_properties_required(props)?;
        Ok(Self {
            fetch_mode,
            tablet_root_paths,
            object_store_profile,
        })
    }

    fn validate_fetch_mode(&self) -> Result<(), String> {
        if let Some(mode) = self.fetch_mode.as_deref() {
            let normalized = mode.trim_matches(|c: char| c.is_whitespace() || c == '\0');
            if normalized != "object_store" {
                return Err(format!(
                    "unsupported starrocks fetch_mode: {normalized} (only object_store is supported)"
                ));
            }
        } else {
            warn!("starrocks scan missing fetch_mode, assuming object_store");
        }
        Ok(())
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
        _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let ScanMorsel::StarRocksRange { index } = morsel else {
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

        let ctx = StarRocksExecutionContext::from_properties(&cfg.properties)?;
        ctx.validate_fetch_mode()?;

        if let Some(profile) = profile.as_ref() {
            profile.add_info_string("DataSourceType", "StarRocks");
            profile.add_info_string("RangeCount", format!("{}", cfg.ranges.len()));
            profile.add_info_string("ReaderBackend", "rust_native");
        }

        let iter = StarRocksScanIter::new(cfg, ctx, profile);
        Ok(Box::new(iter))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let morsels = (0..self.cfg.ranges.len())
            .map(|index| ScanMorsel::StarRocksRange { index })
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
    output_schema: SchemaRef,
}

impl StarRocksScanner {
    fn open(
        cfg: &StarRocksScanConfig,
        ctx: &StarRocksExecutionContext,
        range: StarRocksScanRange,
    ) -> Result<Self, String> {
        let output_slot_meta = cfg
            .schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().to_string(),
                    f.metadata()
                        .get(crate::exec::chunk::FIELD_META_SLOT_ID)
                        .cloned(),
                )
            })
            .collect::<Vec<_>>();
        info!(
            "StarRocksScanner::open tablet_id={} cfg.slot_ids={:?} output_slot_meta={:?} query_global_dict_slots={:?}",
            range.tablet_id,
            cfg.slot_ids,
            output_slot_meta,
            cfg.query_global_dicts.keys().collect::<Vec<_>>()
        );
        let tablet_root_path = ctx
            .tablet_root_paths
            .get(&range.tablet_id)
            .ok_or_else(|| {
                format!(
                    "missing tablet_root_path for tablet_id {} in tablet_root_paths",
                    range.tablet_id
                )
            })?
            .clone();

        let version = range
            .version
            .ok_or_else(|| format!("missing tablet version for tablet_id {}", range.tablet_id))?;

        let options = build_native_options(ctx);
        let reader = StarRocksNativeReader::open(
            range.tablet_id,
            &tablet_root_path,
            version,
            cfg.required_schema.clone(),
            cfg.schema.clone(),
            cfg.query_global_dicts.clone(),
            cfg.min_max_predicates.clone(),
            &options,
        )?;

        Ok(Self {
            reader,
            output_schema: cfg.schema.clone(),
        })
    }

    fn get_next(&mut self) -> Result<ScanBatch, String> {
        let batch = self.reader.get_next(&self.output_schema)?;
        match batch {
            None => Ok(ScanBatch::Eos),
            Some(batch) => {
                let rows = batch.num_rows();
                let chunk = Chunk::try_new(batch)?;
                let bytes = chunk.logical_bytes();
                Ok(ScanBatch::Chunk(chunk, rows, bytes))
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.reader.close()
    }
}

fn build_native_options(ctx: &StarRocksExecutionContext) -> ObjectStoreProfile {
    ctx.object_store_profile.clone()
}

pub(crate) fn build_native_object_store_profile_from_properties(
    props: &BTreeMap<String, String>,
) -> Result<Option<ObjectStoreProfile>, String> {
    ObjectStoreProfile::from_properties_optional(props)
}

fn parse_tablet_root_paths(
    props: &BTreeMap<String, String>,
) -> Result<HashMap<i64, String>, String> {
    let raw = props.get("tablet_root_paths").ok_or_else(|| {
        "tablet_root_paths not found in properties for object_store mode".to_string()
    })?;

    let value: Value = serde_json::from_str(raw)
        .map_err(|e| format!("parse tablet_root_paths json failed: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "tablet_root_paths must be a JSON object".to_string())?;

    let mut out = HashMap::with_capacity(obj.len());
    for (key, value) in obj {
        let tablet_id = key
            .parse::<i64>()
            .map_err(|_| format!("invalid tablet_id in tablet_root_paths: {key}"))?;
        let path = value
            .as_str()
            .ok_or_else(|| format!("tablet_root_paths entry for {key} is not a string"))?;
        if path.is_empty() {
            return Err(format!("tablet_root_paths entry for {key} is empty"));
        }
        out.insert(tablet_id, path.to_string());
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn mock_scan_config(profile_label: Option<String>) -> StarRocksScanConfig {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, true)]));
        StarRocksScanConfig {
            db_name: None,
            table_name: None,
            opaqued_query_plan: String::new(),
            properties: BTreeMap::new(),
            ranges: Vec::new(),
            has_more: false,
            required_schema: schema.clone(),
            schema,
            slot_ids: Vec::new(),
            query_global_dicts: HashMap::new(),
            limit: None,
            batch_size: None,
            query_timeout: None,
            mem_limit: None,
            profile_label,
            min_max_predicates: Vec::new(),
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
}
