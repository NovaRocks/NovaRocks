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
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
use crate::formats::{FileFormatConfig, build_format_iter};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
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
}

#[derive(Clone, Debug)]
pub struct HdfsScanOp {
    cfg: HdfsScanConfig,
}

impl HdfsScanOp {
    pub fn new(cfg: HdfsScanConfig) -> Self {
        Self { cfg }
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
        let scan = FileScanContext::build(ranges, profile.clone())?;
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
