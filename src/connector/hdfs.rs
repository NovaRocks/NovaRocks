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
use std::sync::atomic::{AtomicI32, Ordering};

use crate::cache::ExternalDataCacheRangeOptions;
use crate::descriptors;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
use crate::formats::{FileFormatConfig, build_format_iter};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::internal_service;
use crate::runtime::profile::RuntimeProfile;

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
    pub iceberg_table_locations: HashMap<crate::types::TTableId, String>,
}

#[derive(Clone, Debug)]
pub struct HdfsScanOp {
    cfg: HdfsScanConfig,
    row_position_scan: bool,
    next_scan_range_id: Arc<AtomicI32>,
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
        }
    }

    fn expected_hdfs_file_format(&self) -> Option<descriptors::THdfsFileFormat> {
        match self.cfg.format.as_ref() {
            Some(FileFormatConfig::Parquet(_)) => Some(descriptors::THdfsFileFormat::PARQUET),
            Some(FileFormatConfig::Orc(_)) => Some(descriptors::THdfsFileFormat::ORC),
            None => None,
        }
    }

    fn next_incremental_scan_range_id(&self) -> i32 {
        self.next_scan_range_id.fetch_add(1, Ordering::AcqRel)
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
            external_datacache,
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
            external_datacache: external_datacache.clone(),
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

        let Some(mut format) = self.cfg.format.clone() else {
            return Err("hdfs scan missing file format for non-empty morsel".to_string());
        };
        format = match format {
            FileFormatConfig::Parquet(mut parquet_cfg) => {
                parquet_cfg.datacache = parquet_cfg
                    .datacache
                    .with_external_range_options(external_datacache.as_ref())?;
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
        let mut morsels = Vec::with_capacity(self.cfg.ranges.len());
        for r in &self.cfg.ranges {
            morsels.push(ScanMorsel::FileRange {
                path: r.path.clone(),
                file_len: r.file_len,
                offset: r.offset,
                length: r.length,
                scan_range_id: r.scan_range_id,
                first_row_id: r.first_row_id,
                external_datacache: r.external_datacache.clone(),
            });
        }
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        true
    }

    fn build_incremental_morsels(
        &self,
        scan_ranges: &[internal_service::TScanRangeParams],
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::new();
        let mut has_more = false;
        let expected_file_format = self.expected_hdfs_file_format();

        for p in scan_ranges {
            if p.empty.unwrap_or(false) {
                if let Some(value) = p.has_more {
                    has_more = value;
                }
                continue;
            }
            if let Some(value) = p.has_more {
                has_more = value;
            }

            let Some(hdfs_range) = p.scan_range.hdfs_scan_range.as_ref() else {
                continue;
            };

            if let Some(expected) = expected_file_format {
                let file_format = hdfs_range.file_format.as_ref().ok_or_else(|| {
                    "incremental hdfs scan range is missing file_format".to_string()
                })?;
                if *file_format != expected {
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

            let file_len = hdfs_range.file_length.unwrap_or(0);
            let file_len = if file_len > 0 { file_len as u64 } else { 0 };
            let offset = hdfs_range.offset.unwrap_or(0);
            let offset = if offset >= 0 { offset as u64 } else { 0 };
            let length = hdfs_range.length.unwrap_or(0);
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

            morsels.push(ScanMorsel::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                external_datacache: build_external_datacache_options(hdfs_range),
            });
        }

        Ok(ScanMorsels::new(morsels, has_more))
    }

    fn profile_name(&self) -> Option<String> {
        let prefix = "HDFS_SCAN";
        if let Some(label) = self.cfg.profile_label.as_deref() {
            if let Some(id) = label
                .strip_prefix("hdfs_scan_node_id=")
                .and_then(|s| s.parse::<i32>().ok())
            {
                return Some(format!("{prefix} (id={id})"));
            }
        }
        Some(prefix.to_string())
    }
}

fn build_external_datacache_options(
    hdfs_range: &crate::plan_nodes::THdfsScanRange,
) -> Option<ExternalDataCacheRangeOptions> {
    let candidate_node = hdfs_range
        .candidate_node
        .as_ref()
        .map(|node| node.trim())
        .filter(|node| !node.is_empty())
        .map(|node| node.to_string());
    let options = ExternalDataCacheRangeOptions {
        modification_time: hdfs_range.modification_time,
        enable_populate_datacache: hdfs_range
            .datacache_options
            .as_ref()
            .and_then(|opts| opts.enable_populate_datacache),
        datacache_priority: hdfs_range
            .datacache_options
            .as_ref()
            .and_then(|opts| opts.priority),
        candidate_node,
    };
    if options.modification_time.is_some()
        || options.enable_populate_datacache.is_some()
        || options.datacache_priority.is_some()
        || options.candidate_node.is_some()
    {
        Some(options)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::descriptors;
    use crate::exec::node::scan::{ScanMorsel, ScanOp};
    use crate::fs::scan_context::FileScanRange;
    use crate::internal_service;
    use crate::plan_nodes;

    use super::{HdfsScanConfig, HdfsScanOp};

    fn make_hdfs_range(
        path: &str,
        first_row_id: Option<i64>,
    ) -> internal_service::TScanRangeParams {
        let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
            None::<String>,
            Some(0_i64),
            Some(100_i64),
            None::<i64>,
            Some(256_i64),
            Some(descriptors::THdfsFileFormat::PARQUET),
            None::<descriptors::TTextFileDesc>,
            Some(path.to_string()),
            None::<Vec<String>>,
            None::<bool>,
            None::<Vec<plan_nodes::TIcebergDeleteFile>>,
            None::<i64>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<crate::data_cache::TDataCacheOptions>,
            None::<Vec<crate::types::TSlotId>>,
            None::<bool>,
            None::<std::collections::BTreeMap<String, String>>,
            None::<Vec<crate::types::TSlotId>>,
            None::<bool>,
            None::<String>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<plan_nodes::TPaimonDeletionFile>,
            None::<std::collections::BTreeMap<crate::types::TSlotId, crate::exprs::TExpr>>,
            None::<descriptors::THdfsPartition>,
            None::<crate::types::TTableId>,
            None::<plan_nodes::TDeletionVectorDescriptor>,
            None::<String>,
            None::<i64>,
            None::<bool>,
            None::<std::collections::BTreeMap<i32, crate::exprs::TExprMinMaxValue>>,
            None::<i32>,
            first_row_id,
        );
        internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                None::<plan_nodes::TInternalScanRange>,
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                Some(hdfs_scan_range),
                None::<plan_nodes::TBinlogScanRange>,
            ),
            None::<i32>,
            Some(false),
            None::<bool>,
        )
    }

    fn make_end_marker(has_more: bool) -> internal_service::TScanRangeParams {
        internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                None::<plan_nodes::TInternalScanRange>,
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                None::<plan_nodes::THdfsScanRange>,
                None::<plan_nodes::TBinlogScanRange>,
            ),
            None::<i32>,
            Some(true),
            Some(has_more),
        )
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
                external_datacache: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
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
}
