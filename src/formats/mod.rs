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
use crate::exec::node::scan::RuntimeFilterContext;
use crate::fs::scan_context::FileScanContext;
use crate::runtime::profile::RuntimeProfile;

pub mod orc;
pub mod parquet;
pub mod starrocks;

#[derive(Clone, Debug)]
pub enum FileFormatConfig {
    Parquet(parquet::ParquetScanConfig),
    Orc(orc::OrcScanConfig),
}

pub fn build_format_iter(
    scan: FileScanContext,
    format: FileFormatConfig,
    limit: Option<usize>,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    match format {
        FileFormatConfig::Parquet(cfg) => {
            parquet::build_parquet_iter(scan, cfg, limit, profile, runtime_filters)
        }
        FileFormatConfig::Orc(cfg) => {
            orc::build_orc_iter(scan, cfg, limit, profile, runtime_filters)
        }
    }
}
