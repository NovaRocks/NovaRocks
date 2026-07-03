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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use crate::common::ids::SlotId;
use crate::common::runtime_scan_predicate::{
    RuntimeScanPredicateBindings, RuntimeScanPredicateCounters, RuntimeScanPredicateOptions,
    runtime_filters_to_scan_predicates,
};
use crate::connector::iceberg::delete_file::{IcebergDeleteFileSpec, IcebergFileContent};
use crate::connector::iceberg::file_pruning::{
    IcebergFilePruningCounters, iceberg_range_may_satisfy_scan_predicates,
};
use crate::connector::iceberg::position_delete::load_position_deletes;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{
    HdfsScanFileFormat, IncrementalScanRange, RuntimeFilterContext, ScanMorsel, ScanMorsels,
    ScanOp, ScanRuntimeFilterDecision,
};
use crate::formats::{FileFormatConfig, build_format_iter};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::runtime::profile::RuntimeProfile;
use crate::runtime::runtime_filter_hub::AcquiredRuntimeFilters;

fn delete_files_have_position_deletes(delete_files: &[IcebergDeleteFileSpec]) -> bool {
    delete_files
        .iter()
        .any(|file| file.file_content == IcebergFileContent::PositionDeletes)
}

fn apply_parquet_pruning_gate_for_delete_files(
    parquet_cfg: &mut crate::formats::parquet::ParquetScanConfig,
    delete_files: &[IcebergDeleteFileSpec],
) {
    if delete_files_have_position_deletes(delete_files) {
        parquet_cfg.enable_page_index = false;
        parquet_cfg.min_max_predicates.clear();
        parquet_cfg.runtime_min_max_filter_columns.clear();
        parquet_cfg.variant_path_predicates.clear();
    }
}

#[derive(Clone, Debug, Default)]
pub struct HdfsIcebergRuntimePruningConfig {
    pub slot_to_column: HashMap<SlotId, String>,
    pub min_max_filter_columns: HashMap<i32, String>,
    pub discrete_set_max_values: usize,
}

#[derive(Clone, Debug)]
pub struct HdfsScanConfig {
    pub ranges: Vec<FileScanRange>,
    /// Original range count from FE `per_node_scan_ranges` before any local coalescing.
    /// This is useful for profiling/debugging when multiple splits point to the same file.
    pub original_range_count: usize,
    pub has_more: bool,
    pub limit: Option<usize>,
    pub profile_label: Option<String>,
    pub format: Option<FileFormatConfig>,
    /// OSS credentials supplied by FE via `THdfsScanNode.cloud_configuration`.
    /// Used as a fallback when the shard registry has no entry for the scanned path
    /// (typical for Iceberg external tables whose files are not tracked as lake tablets).
    pub object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    /// Cached Iceberg table locations keyed by `table_id`, used to resolve incremental
    /// scan ranges that only carry `relative_path`.
    pub iceberg_table_locations: HashMap<i64, String>,
    /// Per-slot global dictionary encode maps (string bytes -> dict id) for
    /// dict-encoded output columns. Empty for all non-dict scans. Injected into
    /// the parquet format config in `execute_iter`; the reader reads the dict
    /// column as Utf8 and encodes the strings to ids.
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
    pub iceberg_runtime_pruning: Option<HdfsIcebergRuntimePruningConfig>,
}

#[derive(Clone, Debug)]
pub struct HdfsScanOp {
    cfg: HdfsScanConfig,
    row_position_scan: bool,
    next_scan_range_id: Arc<AtomicI32>,
    iceberg_runtime_pruning_counters: Arc<HdfsIcebergRuntimePruningCounters>,
    iceberg_runtime_pruning_profile_flushed: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct HdfsIcebergRuntimePruningCounters {
    files_total: AtomicU64,
    files_selected: AtomicU64,
    files_pruned: AtomicU64,
    predicates: AtomicU64,
    unsupported: AtomicU64,
    unavailable: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HdfsIcebergRuntimePruningCounterSnapshot {
    pub(crate) files_total: u64,
    pub(crate) files_selected: u64,
    pub(crate) files_pruned: u64,
    pub(crate) predicates: u64,
    pub(crate) unsupported: u64,
    pub(crate) unavailable: u64,
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn atomic_add_saturating(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(delta))
    });
}

impl HdfsIcebergRuntimePruningCounters {
    fn record_runtime_predicates(
        &self,
        predicates: usize,
        predicate_counters: &RuntimeScanPredicateCounters,
    ) {
        atomic_add_saturating(&self.predicates, predicates as u64);
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(predicate_counters.unsupported),
        );
    }

    fn record_file_counters(&self, file_counters: &IcebergFilePruningCounters) {
        atomic_add_saturating(
            &self.files_total,
            u128_to_u64_saturating(file_counters.files_total),
        );
        atomic_add_saturating(
            &self.files_selected,
            u128_to_u64_saturating(file_counters.files_selected),
        );
        atomic_add_saturating(
            &self.files_pruned,
            u128_to_u64_saturating(file_counters.files_pruned),
        );
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(file_counters.unsupported),
        );
    }

    fn record_missing_metadata(&self, ranges: usize) {
        let ranges = ranges as u64;
        atomic_add_saturating(&self.files_total, ranges);
        atomic_add_saturating(&self.files_selected, ranges);
        atomic_add_saturating(&self.unsupported, ranges);
    }

    fn record_unavailable(&self) {
        atomic_add_saturating(&self.unavailable, 1);
    }

    fn snapshot(&self) -> HdfsIcebergRuntimePruningCounterSnapshot {
        HdfsIcebergRuntimePruningCounterSnapshot {
            files_total: self.files_total.load(Ordering::Acquire),
            files_selected: self.files_selected.load(Ordering::Acquire),
            files_pruned: self.files_pruned.load(Ordering::Acquire),
            predicates: self.predicates.load(Ordering::Acquire),
            unsupported: self.unsupported.load(Ordering::Acquire),
            unavailable: self.unavailable.load(Ordering::Acquire),
        }
    }
}

impl HdfsScanOp {
    pub fn new(cfg: HdfsScanConfig) -> Self {
        let row_position_scan = cfg
            .ranges
            .iter()
            .any(|r| r.scan_range_id >= 0 || r.first_row_id.is_some());
        let next_scan_range_id = cfg
            .ranges
            .iter()
            .filter_map(|r| (r.scan_range_id >= 0).then_some(r.scan_range_id))
            .max()
            .map(|v| v.saturating_add(1))
            .unwrap_or(0);
        Self {
            cfg,
            row_position_scan,
            next_scan_range_id: Arc::new(AtomicI32::new(next_scan_range_id)),
            iceberg_runtime_pruning_counters: Arc::new(HdfsIcebergRuntimePruningCounters::default()),
            iceberg_runtime_pruning_profile_flushed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn expected_hdfs_file_format(&self) -> Option<HdfsScanFileFormat> {
        match self.cfg.format.as_ref() {
            Some(FileFormatConfig::Parquet(_)) => Some(HdfsScanFileFormat::Parquet),
            Some(FileFormatConfig::Orc(_)) => Some(HdfsScanFileFormat::Orc),
            None => None,
        }
    }

    fn next_incremental_scan_range_id(&self) -> i32 {
        self.next_scan_range_id.fetch_add(1, Ordering::AcqRel)
    }

    fn lowered_delete_files_for_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<IcebergDeleteFileSpec>, String> {
        if let Some(range) =
            self.cfg.ranges.iter().find(|range| {
                range.path == path && range.offset == offset && range.length == length
            })
        {
            return Ok(range.delete_files.clone());
        }

        let same_path_delete_file_count = self
            .cfg
            .ranges
            .iter()
            .filter(|range| range.path == path && !range.delete_files.is_empty())
            .count();
        if same_path_delete_file_count > 0 {
            return Err(format!(
                "incremental HDFS range cannot safely reuse lowered Iceberg delete files for \
                 path={path} offset={offset} length={length}; found \
                 {same_path_delete_file_count} same-path lowered range(s) with delete files but \
                 no exact match"
            ));
        }

        Ok(Vec::new())
    }

    fn ordered_initial_ranges(&self) -> Vec<&FileScanRange> {
        let mut ranges = self.cfg.ranges.iter().collect::<Vec<_>>();
        if self.can_reorder_initial_ranges() {
            ranges.sort_by(|left, right| {
                right
                    .length
                    .cmp(&left.length)
                    .then_with(|| left.path.cmp(&right.path))
                    .then_with(|| left.offset.cmp(&right.offset))
            });
        }
        ranges
    }

    fn can_reorder_initial_ranges(&self) -> bool {
        !self.row_position_scan
            && self.cfg.ranges.iter().all(|range| {
                range.scan_range_id < 0
                    && range.first_row_id.is_none()
                    && range.data_sequence_number.is_none()
                    && range.ivm_change_op.is_none()
                    && range.delete_files.is_empty()
            })
    }

    fn has_iceberg_file_pruning_metadata(&self) -> bool {
        self.cfg
            .ranges
            .iter()
            .any(|range| range.iceberg_file_pruning.is_some())
    }

    fn has_iceberg_runtime_pruning_bindings(pruning_cfg: &HdfsIcebergRuntimePruningConfig) -> bool {
        !pruning_cfg.slot_to_column.is_empty() || !pruning_cfg.min_max_filter_columns.is_empty()
    }

    fn can_materialize_iceberg_runtime_file_pruning(&self) -> bool {
        self.cfg
            .iceberg_runtime_pruning
            .as_ref()
            .is_some_and(Self::has_iceberg_runtime_pruning_bindings)
            && self.has_iceberg_file_pruning_metadata()
    }

    fn build_morsels_from_ordered_ranges(
        &self,
        ranges: Vec<&FileScanRange>,
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::with_capacity(ranges.len());
        for r in ranges {
            morsels.push(ScanMorsel::FileRange {
                path: r.path.clone(),
                file_len: r.file_len,
                offset: r.offset,
                length: r.length,
                scan_range_id: r.scan_range_id,
                first_row_id: r.first_row_id,
                data_sequence_number: r.data_sequence_number,
                ivm_change_op: r.ivm_change_op,
                included_positions: r.included_positions.clone(),
                external_datacache: r.external_datacache.clone(),
                delete_files: r.delete_files.clone(),
                iceberg_file_pruning: r.iceberg_file_pruning.clone(),
            });
        }
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn flush_iceberg_runtime_pruning_profile(&self, profile: &RuntimeProfile) {
        if self
            .iceberg_runtime_pruning_profile_flushed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let snapshot = self.iceberg_runtime_pruning_counters.snapshot();
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesTotal",
            u64_to_i64_saturating(snapshot.files_total),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesSelected",
            u64_to_i64_saturating(snapshot.files_selected),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesPruned",
            u64_to_i64_saturating(snapshot.files_pruned),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Predicates",
            u64_to_i64_saturating(snapshot.predicates),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unsupported",
            u64_to_i64_saturating(snapshot.unsupported),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unavailable",
            u64_to_i64_saturating(snapshot.unavailable),
        );
    }

    #[cfg(test)]
    fn iceberg_runtime_pruning_counter_snapshot_for_test(
        &self,
    ) -> HdfsIcebergRuntimePruningCounterSnapshot {
        self.iceberg_runtime_pruning_counters.snapshot()
    }
}

impl ScanOp for HdfsScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let ScanMorsel::FileRange {
            path,
            file_len,
            offset,
            length,
            scan_range_id,
            first_row_id,
            data_sequence_number,
            ivm_change_op,
            included_positions,
            external_datacache,
            delete_files,
            iceberg_file_pruning,
        } = morsel
        else {
            return Err("hdfs scan received unexpected morsel".to_string());
        };
        let ranges = vec![FileScanRange {
            path,
            file_len,
            offset,
            length,
            scan_range_id,
            first_row_id,
            data_sequence_number,
            ivm_change_op,
            included_positions,
            external_datacache: external_datacache.clone(),
            delete_files,
            iceberg_file_pruning,
        }];
        let scan = FileScanContext::build(
            ranges,
            profile.clone(),
            self.cfg.object_store_config.as_ref(),
        )?;
        if let Some(profile) = profile.as_ref() {
            profile.add_info_string(
                "OriginalRangeCount",
                format!("{}", self.cfg.original_range_count),
            );
            profile.add_info_string("RangeCount", format!("{}", scan.ranges.len()));
        }
        let current_delete_files = scan
            .ranges
            .first()
            .map(|range| range.delete_files.as_slice())
            .unwrap_or(&[]);

        let Some(mut format) = self.cfg.format.clone() else {
            return Err("hdfs scan missing file format for non-empty morsel".to_string());
        };
        format = match format {
            FileFormatConfig::Parquet(mut parquet_cfg) => {
                parquet_cfg.datacache = parquet_cfg
                    .datacache
                    .with_external_range_options(external_datacache.as_ref())?;
                parquet_cfg.query_global_dicts = self.cfg.query_global_dicts.clone();
                apply_parquet_pruning_gate_for_delete_files(&mut parquet_cfg, current_delete_files);
                FileFormatConfig::Parquet(parquet_cfg)
            }
            FileFormatConfig::Orc(mut orc_cfg) => {
                orc_cfg.datacache = orc_cfg
                    .datacache
                    .with_external_range_options(external_datacache.as_ref())?;
                FileFormatConfig::Orc(orc_cfg)
            }
        };
        build_format_iter(scan, format, None, profile, runtime_filters)
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        self.build_morsels_from_ordered_ranges(self.ordered_initial_ranges())
    }

    fn materialize_morsels_after_runtime_filters(&self) -> bool {
        self.can_materialize_iceberg_runtime_file_pruning()
    }

    fn build_morsels_with_runtime_filters(
        &self,
        decision: ScanRuntimeFilterDecision<'_>,
    ) -> Result<ScanMorsels, String> {
        let Some(pruning_cfg) = self.cfg.iceberg_runtime_pruning.as_ref() else {
            return self.build_morsels();
        };
        if !Self::has_iceberg_runtime_pruning_bindings(pruning_cfg) {
            return self.build_morsels();
        }
        if !self.has_iceberg_file_pruning_metadata() {
            self.iceberg_runtime_pruning_counters
                .record_missing_metadata(self.cfg.ranges.len());
            return self.build_morsels();
        }
        let Some(AcquiredRuntimeFilters::Complete(snapshot)) = decision.acquired() else {
            self.iceberg_runtime_pruning_counters.record_unavailable();
            return self.build_morsels();
        };

        let runtime_ctx = RuntimeFilterContext::from_snapshot(snapshot.clone());
        let mut predicate_counters = RuntimeScanPredicateCounters::default();
        let predicates = runtime_filters_to_scan_predicates(
            &runtime_ctx,
            &RuntimeScanPredicateBindings {
                slot_to_column: pruning_cfg.slot_to_column.clone(),
                min_max_filter_columns: pruning_cfg.min_max_filter_columns.clone(),
            },
            RuntimeScanPredicateOptions {
                discrete_set_max_values: pruning_cfg.discrete_set_max_values,
                label: "iceberg",
            },
            &mut predicate_counters,
        )?;
        self.iceberg_runtime_pruning_counters
            .record_runtime_predicates(predicates.len(), &predicate_counters);
        if predicates.is_empty() {
            return self.build_morsels();
        }

        let mut file_counters = IcebergFilePruningCounters::default();
        let ranges = self
            .ordered_initial_ranges()
            .into_iter()
            .filter(|range| {
                iceberg_range_may_satisfy_scan_predicates(range, &predicates, &mut file_counters)
            })
            .collect::<Vec<_>>();
        self.iceberg_runtime_pruning_counters
            .record_file_counters(&file_counters);
        self.build_morsels_from_ordered_ranges(ranges)
    }

    fn flush_morsel_materialization_profile(&self, profile: &RuntimeProfile) {
        self.flush_iceberg_runtime_pruning_profile(profile);
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        true
    }

    fn build_incremental_morsels(
        &self,
        scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::new();
        let mut has_more = false;
        let expected_file_format = self.expected_hdfs_file_format();

        for scan_range in scan_ranges {
            if let Some(value) = scan_range.has_more() {
                has_more = value;
            }

            let IncrementalScanRange::Hdfs {
                range: hdfs_range, ..
            } = scan_range
            else {
                continue;
            };

            if let Some(expected) = expected_file_format {
                let file_format = hdfs_range.file_format.ok_or_else(|| {
                    "incremental hdfs scan range is missing file_format".to_string()
                })?;
                if file_format != expected {
                    return Err(format!(
                        "incremental hdfs scan range file_format mismatch: expected {:?}, got {:?}",
                        expected, file_format
                    ));
                }
            }

            let path = if let Some(path) = hdfs_range
                .full_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                path.to_string()
            } else if let Some(rel) = hdfs_range
                .relative_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                let table_id = hdfs_range.table_id.ok_or_else(|| {
                    "incremental hdfs scan range has relative_path but missing table_id".to_string()
                })?;
                let base = self
                    .cfg
                    .iceberg_table_locations
                    .get(&table_id)
                    .map(|s| s.trim_end_matches('/'))
                    .ok_or_else(|| {
                        format!(
                            "incremental hdfs scan range missing cached iceberg location for table_id={table_id}"
                        )
                    })?;
                let rel = rel.trim_start_matches('/');
                if rel.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}/{rel}")
                }
            } else {
                return Err(
                    "incremental hdfs scan range requires non-empty full_path or relative_path"
                        .to_string(),
                );
            };

            let file_len = hdfs_range.file_length;
            let file_len = if file_len > 0 { file_len as u64 } else { 0 };
            let offset = hdfs_range.offset;
            let offset = if offset >= 0 { offset as u64 } else { 0 };
            let length = hdfs_range.length;
            let mut length = if length > 0 { length as u64 } else { 0 };
            if length == 0 && file_len > offset {
                length = file_len - offset;
            }

            let (scan_range_id, first_row_id) = if self.row_position_scan {
                let first_row_id = hdfs_range.first_row_id.ok_or_else(|| {
                    "incremental hdfs scan range missing first_row_id for row position scan"
                        .to_string()
                })?;
                (self.next_incremental_scan_range_id(), Some(first_row_id))
            } else {
                (-1, None)
            };

            let delete_files = self.lowered_delete_files_for_range(&path, offset, length)?;
            let ivm_change_op = hdfs_range.ivm_change_op;
            // data_sequence_number is not carried by FE incremental ranges.
            // It is populated at initial lowering time from
            // the Iceberg manifest entry for V3 row-lineage tables.
            let data_sequence_number: Option<i64> = None;
            morsels.push(ScanMorsel::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number,
                ivm_change_op,
                included_positions: None,
                external_datacache: hdfs_range.external_datacache.clone(),
                delete_files,
                iceberg_file_pruning: None,
            });
        }

        Ok(ScanMorsels::new(morsels, has_more))
    }

    fn profile_name(&self) -> Option<String> {
        let prefix = "HDFS_SCAN";
        if let Some(label) = self.cfg.profile_label.as_deref()
            && let Some(id) = label
                .strip_prefix("hdfs_scan_node_id=")
                .and_then(|s| s.parse::<i32>().ok())
        {
            return Some(format!("{prefix} (id={id})"));
        }
        Some(prefix.to_string())
    }

    fn load_iceberg_position_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<roaring::RoaringTreemap>, String> {
        let ScanMorsel::FileRange {
            path, delete_files, ..
        } = morsel
        else {
            return Ok(None);
        };
        if delete_files.is_empty() {
            return Ok(None);
        }
        // Build a one-off scan context across the data file and all its delete
        // files so a single OpenDAL operator resolves OSS / HDFS credentials
        // for the entire set. We reuse `FileScanContext::build` for scheme
        // classification and credential resolution, passing zero-length
        // ranges because we never read the data file through this context —
        // only the delete parquet files are read.
        let mut loader_ranges: Vec<crate::fs::scan_context::FileScanRange> =
            Vec::with_capacity(1 + delete_files.len());
        loader_ranges.push(crate::fs::scan_context::FileScanRange {
            path: path.clone(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        });
        for del in delete_files {
            loader_ranges.push(crate::fs::scan_context::FileScanRange {
                path: del.path.clone(),
                file_len: del.length.unwrap_or(0),
                offset: 0,
                length: del.length.unwrap_or(0),
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            });
        }
        let ctx = crate::fs::scan_context::FileScanContext::build(
            loader_ranges,
            None,
            self.cfg.object_store_config.as_ref(),
        )?;
        // After credential resolution `ctx.ranges` carries scheme-normalized
        // paths suitable for the OpenDAL operator, but the delete parquet
        // files record the data-file path exactly as the Iceberg writer saw
        // it (`oss://bucket/...`, `hdfs://ns/...`, or an absolute filesystem
        // path). Compare against the original morsel path so writer-recorded
        // rows match regardless of how OpenDAL normalized the prefix.
        let data_file_path = path.clone();
        let normalized_delete_specs: Vec<IcebergDeleteFileSpec> = ctx
            .ranges
            .iter()
            .skip(1)
            .zip(delete_files.iter())
            .map(|(resolved, original)| IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original.length,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
            .collect();
        let deleted =
            load_position_deletes(&normalized_delete_specs, &data_file_path, &ctx.factory)?;
        if deleted.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deleted))
        }
    }

    fn load_iceberg_equality_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<Vec<crate::connector::iceberg::equality_delete::EqualityDeleteSet>>, String>
    {
        let ScanMorsel::FileRange {
            path, delete_files, ..
        } = morsel
        else {
            return Ok(None);
        };
        if !delete_files
            .iter()
            .any(|file| file.file_content == IcebergFileContent::EqualityDeletes)
        {
            return Ok(None);
        }
        let mut loader_ranges: Vec<crate::fs::scan_context::FileScanRange> =
            Vec::with_capacity(1 + delete_files.len());
        loader_ranges.push(crate::fs::scan_context::FileScanRange {
            path: path.clone(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        });
        for del in delete_files {
            loader_ranges.push(crate::fs::scan_context::FileScanRange {
                path: del.path.clone(),
                file_len: del.length.unwrap_or(0),
                offset: 0,
                length: del.length.unwrap_or(0),
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            });
        }
        let ctx = crate::fs::scan_context::FileScanContext::build(
            loader_ranges,
            None,
            self.cfg.object_store_config.as_ref(),
        )?;
        let normalized_delete_specs: Vec<IcebergDeleteFileSpec> = ctx
            .ranges
            .iter()
            .skip(1)
            .zip(delete_files.iter())
            .map(|(resolved, original)| IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original.length,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
            .collect();
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &normalized_delete_specs,
            &ctx.factory,
        )?;
        if sets.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sets))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::cache::{CacheOptions, DataCacheManager};
    use crate::common::ids::SlotId;
    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::connector::iceberg::delete_file::{
        IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
    };
    use crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprArena, ExprId};
    use crate::exec::node::RuntimeFilterProbeSpec;
    use crate::exec::node::scan::{
        HdfsScanFileFormat, IncrementalHdfsScanRange, IncrementalScanRange, ScanMorsel, ScanNode,
        ScanOp, ScanRuntimeFilterDecision,
    };
    use crate::exec::operators::scan::ScanSourceFactory;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::exec::runtime_filter::{
        RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeEmptyFilter, RuntimeFilterType, RuntimeInFilter,
        RuntimeMembershipFilter, RuntimeMinMaxFilter,
    };
    use crate::formats::parquet::{
        ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind, VariantPathPruningPredicate,
    };
    use crate::fs::scan_context::FileScanRange;
    use crate::runtime::profile::{OperatorProfiles, RuntimeProfile};
    use crate::runtime::runtime_filter_hub::{
        AcquiredRuntimeFilters, RuntimeFilterHub, RuntimeFilterSnapshot,
        RuntimeFilterUnavailableReason,
    };
    use crate::sql::catalog::IcebergColumnStats;

    use super::{
        HdfsIcebergRuntimePruningConfig, HdfsScanConfig, HdfsScanOp,
        apply_parquet_pruning_gate_for_delete_files,
    };

    fn make_hdfs_range(path: &str, first_row_id: Option<i64>) -> IncrementalScanRange {
        make_hdfs_range_with_change_op(path, first_row_id, None)
    }

    fn make_hdfs_range_with_change_op(
        path: &str,
        first_row_id: Option<i64>,
        ivm_change_op: Option<i8>,
    ) -> IncrementalScanRange {
        IncrementalScanRange::Hdfs {
            has_more: None,
            range: IncrementalHdfsScanRange {
                file_format: Some(HdfsScanFileFormat::Parquet),
                full_path: Some(path.to_string()),
                relative_path: None,
                table_id: None,
                file_length: 256,
                offset: 0,
                length: 100,
                first_row_id,
                ivm_change_op,
                external_datacache: None,
            },
        }
    }

    fn make_end_marker(has_more: bool) -> IncrementalScanRange {
        IncrementalScanRange::Empty {
            has_more: Some(has_more),
        }
    }

    fn test_datacache_context() -> crate::cache::DataCacheContext {
        let cache_options = CacheOptions::from_query_options(None).expect("cache options");
        DataCacheManager::instance().external_context(cache_options)
    }

    fn test_delete_file(file_content: IcebergFileContent) -> IcebergDeleteFileSpec {
        IcebergDeleteFileSpec {
            path: "delete.parquet".to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn test_iceberg_file_pruning_metadata() -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(10_i64.to_le_bytes().to_vec()),
                    upper_bound: Some(20_i64.to_le_bytes().to_vec()),
                },
            )]),
        }
    }

    fn iceberg_file_pruning_metadata_for_i32_range(
        column: &str,
        lower: i32,
        upper: i32,
    ) -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                column.to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(lower.to_le_bytes().to_vec()),
                    upper_bound: Some(upper.to_le_bytes().to_vec()),
                },
            )]),
        }
    }

    fn iceberg_file_range_for_runtime_pruning_test(
        path: &str,
        stats: Option<IcebergFilePruningMetadata>,
    ) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: stats,
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/hit.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn hdfs_cfg_with_all_pruned_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-a.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-b.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 20, 30)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_without_metadata_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/hit.parquet", None),
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/miss.parquet", None),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn runtime_in_filter_for_test(
        filter_id: i32,
        slot_id: SlotId,
        values: &[i32],
    ) -> RuntimeInFilter {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let mut filter = RuntimeInFilter::new_for_test(filter_id, slot_id, &DataType::Int32)
            .expect("create runtime in filter");
        filter.insert_array_for_test(&array).expect("insert values");
        filter
    }

    fn runtime_membership_filter_for_test(
        filter_id: i32,
        slot_id: SlotId,
        values: &[i32],
    ) -> RuntimeMembershipFilter {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let min_max = RuntimeMinMaxFilter::from_arrays(RuntimeFilterType::Int32, &[array])
            .expect("membership min/max filter");
        RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
            filter_id,
            slot_id,
            RuntimeFilterType::Int32,
            false,
            RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            0,
            min_max,
        ))
    }

    fn snapshot_with_runtime_in_filter_for_test(
        filter_id: i32,
        slot_id: SlotId,
        values: &[i32],
    ) -> RuntimeFilterSnapshot {
        RuntimeFilterSnapshot::from_filters(
            vec![runtime_in_filter_for_test(filter_id, slot_id, values)],
            Vec::new(),
        )
    }

    fn runtime_filter_probe_spec_for_test(
        filter_id: i32,
        slot_id: SlotId,
    ) -> RuntimeFilterProbeSpec {
        RuntimeFilterProbeSpec {
            filter_id,
            expr_id: ExprId(0),
            slot_id,
            data_type: DataType::Int32,
        }
    }

    fn hdfs_scan_node_for_runtime_pruning_test(
        op: Arc<HdfsScanOp>,
        runtime_filter_specs: Vec<RuntimeFilterProbeSpec>,
    ) -> ScanNode {
        ScanNode::new(op)
            .with_node_id(77)
            .with_runtime_filter_specs(runtime_filter_specs)
            .with_connector_io_tasks_per_scan_operator(Some(1))
            .with_accept_empty_scan_ranges(true)
    }

    fn hdfs_scan_source_for_runtime_pruning_test(
        scan: ScanNode,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
        driver_id: i32,
        profile: RuntimeProfile,
    ) -> Box<dyn crate::exec::pipeline::operator::Operator> {
        let factory =
            ScanSourceFactory::new(scan, runtime_filter_hub, Arc::new(ExprArena::default()));
        let mut source = factory.create(1, driver_id);
        source.set_profiles(OperatorProfiles::new(profile));
        source.prepare().expect("prepare scan source");
        source
    }

    fn runtime_filter_hub_for_test() -> Arc<RuntimeFilterHub> {
        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        hub.set_wait_timeouts(Some(std::time::Duration::from_secs(5)), None);
        hub
    }

    #[test]
    fn iceberg_runtime_file_pruning_removes_all_splits_for_pruned_file() {
        let cfg = hdfs_cfg_with_two_iceberg_files_for_test();
        let op = HdfsScanOp::new(cfg);
        let acquired = AcquiredRuntimeFilters::Complete(snapshot_with_runtime_in_filter_for_test(
            1,
            SlotId::new(3),
            &[100_i32],
        ));

        let morsels = op
            .build_morsels_with_runtime_filters(ScanRuntimeFilterDecision::from_acquired(Some(
                &acquired,
            )))
            .expect("build morsels");

        assert_eq!(morsels.morsels.len(), 1);
        assert!(morsels.morsels[0].describe().contains("hit.parquet"));
        let counters = op.iceberg_runtime_pruning_counter_snapshot_for_test();
        assert_eq!(counters.files_total, 2);
        assert_eq!(counters.files_selected, 1);
        assert_eq!(counters.files_pruned, 1);
        assert_eq!(counters.predicates, 1);
        assert_eq!(counters.unsupported, 0);
        assert_eq!(counters.unavailable, 0);
    }

    #[test]
    fn all_pruned_runtime_file_pruning_flushes_profile_without_morsels() {
        let op = Arc::new(HdfsScanOp::new(
            hdfs_cfg_with_all_pruned_iceberg_files_for_test(),
        ));
        let scan = hdfs_scan_node_for_runtime_pruning_test(
            Arc::clone(&op),
            vec![runtime_filter_probe_spec_for_test(1, SlotId::new(3))],
        );
        let filter = runtime_membership_filter_for_test(1, SlotId::new(3), &[100_i32]);
        let hub = runtime_filter_hub_for_test();
        let profile = RuntimeProfile::new("hdfs-scan");
        let mut source =
            hdfs_scan_source_for_runtime_pruning_test(scan, Arc::clone(&hub), 0, profile.clone());
        hub.publish_filters(&[], &[filter]);

        assert!(
            !source
                .as_processor_mut()
                .expect("scan source processor")
                .has_output()
        );

        let common = profile.child("CommonMetrics");
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/FilesTotal"),
            Some(2)
        );
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/FilesSelected"),
            Some(0)
        );
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/FilesPruned"),
            Some(2)
        );
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/Predicates"),
            Some(2)
        );
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/Unsupported"),
            Some(0)
        );
        assert_eq!(
            common.counter_value("IcebergRuntimeFilePruning/Unavailable"),
            Some(0)
        );
        let counters = op.iceberg_runtime_pruning_counter_snapshot_for_test();
        assert_eq!(counters.files_total, 2);
        assert_eq!(counters.files_selected, 0);
        assert_eq!(counters.files_pruned, 2);
    }

    #[test]
    fn iceberg_runtime_pruning_without_planned_filters_does_not_defer_or_count_unavailable() {
        let op = Arc::new(HdfsScanOp::new(hdfs_cfg_with_two_iceberg_files_for_test()));
        let scan = hdfs_scan_node_for_runtime_pruning_test(Arc::clone(&op), Vec::new());

        assert!(!scan.materialize_morsels_after_runtime_filters());
        let morsels = scan.build_morsels().expect("build static morsels");
        assert_eq!(morsels.morsels.len(), 2);

        let counters = op.iceberg_runtime_pruning_counter_snapshot_for_test();
        assert_eq!(counters.unavailable, 0);
        assert_eq!(counters.files_total, 0);
        assert_eq!(counters.files_selected, 0);
        assert_eq!(counters.files_pruned, 0);
    }

    #[test]
    fn runtime_file_pruning_profile_flush_is_not_duplicated_across_drivers() {
        let op = Arc::new(HdfsScanOp::new(
            hdfs_cfg_with_all_pruned_iceberg_files_for_test(),
        ));
        let scan = hdfs_scan_node_for_runtime_pruning_test(
            Arc::clone(&op),
            vec![runtime_filter_probe_spec_for_test(1, SlotId::new(3))],
        );
        let filter = runtime_membership_filter_for_test(1, SlotId::new(3), &[100_i32]);
        let hub = runtime_filter_hub_for_test();
        let factory =
            ScanSourceFactory::new(scan, Arc::clone(&hub), Arc::new(ExprArena::default()));
        let profile_a = RuntimeProfile::new("hdfs-scan-a");
        let profile_b = RuntimeProfile::new("hdfs-scan-b");
        let mut source_a = factory.create(2, 0);
        source_a.set_profiles(OperatorProfiles::new(profile_a.clone()));
        source_a.prepare().expect("prepare source a");
        let mut source_b = factory.create(2, 1);
        source_b.set_profiles(OperatorProfiles::new(profile_b.clone()));
        source_b.prepare().expect("prepare source b");
        hub.publish_filters(&[], &[filter]);

        let _ = source_a
            .as_processor_mut()
            .expect("source a processor")
            .has_output();
        let _ = source_b
            .as_processor_mut()
            .expect("source b processor")
            .has_output();

        let files_total_a = profile_a
            .child("CommonMetrics")
            .counter_value("IcebergRuntimeFilePruning/FilesTotal")
            .unwrap_or(0);
        let files_total_b = profile_b
            .child("CommonMetrics")
            .counter_value("IcebergRuntimeFilePruning/FilesTotal")
            .unwrap_or(0);
        assert_eq!(files_total_a + files_total_b, 2);
        assert!(
            (files_total_a == 2 && files_total_b == 0)
                || (files_total_a == 0 && files_total_b == 2)
        );

        let profile_c = RuntimeProfile::new("hdfs-scan-c");
        ScanOp::flush_morsel_materialization_profile(
            op.as_ref(),
            &profile_c.child("CommonMetrics"),
        );
        assert_eq!(
            profile_c
                .child("CommonMetrics")
                .counter_value("IcebergRuntimeFilePruning/FilesTotal"),
            None
        );
    }

    #[test]
    fn unavailable_runtime_filters_keep_static_ranges() {
        let cases = [
            (
                "timeout",
                Some(AcquiredRuntimeFilters::Unavailable(
                    RuntimeFilterUnavailableReason::Timeout,
                )),
            ),
            (
                "no_wait",
                Some(AcquiredRuntimeFilters::Unavailable(
                    RuntimeFilterUnavailableReason::NoWaitConfigured,
                )),
            ),
            ("none", None),
        ];

        for (case, acquired) in cases {
            let cfg = hdfs_cfg_with_two_iceberg_files_for_test();
            let op = HdfsScanOp::new(cfg);
            let morsels = op
                .build_morsels_with_runtime_filters(ScanRuntimeFilterDecision::from_acquired(
                    acquired.as_ref(),
                ))
                .unwrap_or_else(|err| panic!("{case} build morsels failed: {err}"));

            assert_eq!(morsels.morsels.len(), 2, "{case}");
            let counters = op.iceberg_runtime_pruning_counter_snapshot_for_test();
            assert_eq!(counters.unavailable, 1, "{case}");
        }
    }

    #[test]
    fn runtime_file_pruning_requires_metadata() {
        let cfg = hdfs_cfg_with_two_iceberg_files_without_metadata_for_test();
        let op = HdfsScanOp::new(cfg);
        let acquired = AcquiredRuntimeFilters::Complete(snapshot_with_runtime_in_filter_for_test(
            1,
            SlotId::new(3),
            &[100_i32],
        ));

        let morsels = op
            .build_morsels_with_runtime_filters(ScanRuntimeFilterDecision::from_acquired(Some(
                &acquired,
            )))
            .expect("build morsels");

        assert_eq!(morsels.morsels.len(), 2);
        let counters = op.iceberg_runtime_pruning_counter_snapshot_for_test();
        assert_eq!(counters.files_total, 2);
        assert_eq!(counters.files_selected, 2);
        assert_eq!(counters.files_pruned, 0);
        assert_eq!(counters.unsupported, 2);
    }

    fn test_prunable_parquet_config() -> ParquetScanConfig {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("__nr_var_payload_a", DataType::Int64, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![
                ParquetSlotKind::Regular,
                ParquetSlotKind::Regular,
                ParquetSlotKind::Variant,
            ],
            case_sensitive: true,
            enable_page_index: true,
            min_max_predicates: vec![MinMaxPredicate::Gt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            }],
            runtime_min_max_filter_columns: std::collections::HashMap::new(),
            variant_path_predicates: vec![VariantPathPruningPredicate {
                output_slot_id: SlotId::new(2),
                source_slot_id: SlotId::new(3),
                source_field_id: Some(10),
                canonical_path: "$.a".to_string(),
                requested_type: DataType::Int64,
                predicate: MinMaxPredicate::Gt {
                    column: "__nr_var_payload_a".to_string(),
                    value: MinMaxPredicateValue::Int64(7),
                },
            }],
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]))),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        }
    }

    #[test]
    fn hdfs_scan_position_delete_morsel_strips_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();
        parquet_cfg
            .runtime_min_max_filter_columns
            .insert(11, "id".to_string());

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::PositionDeletes)],
        );

        assert!(!parquet_cfg.enable_page_index);
        assert!(parquet_cfg.min_max_predicates.is_empty());
        assert!(parquet_cfg.runtime_min_max_filter_columns.is_empty());
        assert!(parquet_cfg.variant_path_predicates.is_empty());
    }

    #[test]
    fn hdfs_scan_equality_delete_morsel_keeps_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::EqualityDeletes)],
        );

        assert!(parquet_cfg.enable_page_index);
        assert_eq!(parquet_cfg.min_max_predicates.len(), 1);
        assert_eq!(parquet_cfg.variant_path_predicates.len(), 1);
    }

    #[test]
    fn incremental_hdfs_ranges_parse_data_and_end_marker() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                path,
                scan_range_id,
                ..
            } => {
                assert_eq!(path, "s3://bucket/path/file.parquet");
                assert_eq!(*scan_range_id, -1);
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_assign_row_position_scan_range_id_contiguously() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/seed.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: 7,
                first_row_id: Some(10),
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/a.parquet", Some(1000)),
                make_hdfs_range("s3://bucket/path/b.parquet", Some(2000)),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 2);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 8);
                assert_eq!(*first_row_id, Some(1000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
        match &morsels.morsels[1] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 9);
                assert_eq!(*first_row_id, Some(2000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reuse_lowered_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert_eq!(delete_files.len(), 1);
                assert_eq!(
                    delete_files[0].file_content,
                    IcebergFileContent::PositionDeletes
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reject_same_path_delete_files_without_exact_match() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 64,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let err = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect_err("same-path delete files without exact lowered range must fail closed");

        assert!(err.contains("cannot safely reuse lowered Iceberg delete files"));
        assert!(err.contains("s3://bucket/path/file.parquet"));
        assert!(err.contains("offset=0"));
        assert!(err.contains("length=100"));
    }

    #[test]
    fn incremental_hdfs_ranges_allow_empty_delete_files_without_same_path_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/other.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert!(delete_files.is_empty());
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_propagate_change_op_extended_column() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range_with_change_op(
                "s3://bucket/path/file.parquet",
                None,
                Some(-1),
            )])
            .expect("build incremental morsels");

        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange { ivm_change_op, .. } => {
                assert_eq!(*ivm_change_op, Some(-1));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn build_morsels_prioritizes_large_plain_ranges() {
        let cfg = HdfsScanConfig {
            ranges: vec![
                FileScanRange {
                    path: "s3://bucket/path/small-a.parquet".to_string(),
                    file_len: 1024,
                    offset: 0,
                    length: 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/large.parquet".to_string(),
                    file_len: 128 * 1024 * 1024,
                    offset: 0,
                    length: 128 * 1024 * 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/small-b.parquet".to_string(),
                    file_len: 2048,
                    offset: 0,
                    length: 2048,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
            ],
            original_range_count: 3,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        let paths = morsels
            .morsels
            .iter()
            .map(|morsel| match morsel {
                ScanMorsel::FileRange { path, .. } => path.as_str(),
                other => panic!("unexpected morsel: {:?}", other),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "s3://bucket/path/large.parquet",
                "s3://bucket/path/small-b.parquet",
                "s3://bucket/path/small-a.parquet",
            ]
        );
    }

    #[test]
    fn build_morsels_preserves_iceberg_file_pruning_metadata() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 1024,
                offset: 0,
                length: 1024,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: Some(test_iceberg_file_pruning_metadata()),
            }],
            original_range_count: 1,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                iceberg_file_pruning,
                ..
            } => {
                let metadata = iceberg_file_pruning
                    .as_ref()
                    .expect("iceberg pruning metadata");
                assert_eq!(
                    metadata.columns["id"].upper_bound,
                    Some(20_i64.to_le_bytes().to_vec())
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }
}
