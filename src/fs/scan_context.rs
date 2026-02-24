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
use crate::cache::ExternalDataCacheRangeOptions;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::fs::path::{ScanPathScheme, classify_scan_paths, resolve_opendal_paths};
use crate::runtime::profile::RuntimeProfile;
use crate::novarocks_logging::debug;

#[derive(Clone, Debug)]
pub struct FileScanRange {
    pub path: String,
    pub file_len: u64,
    pub offset: u64,
    pub length: u64,
    pub scan_range_id: i32,
    pub first_row_id: Option<i64>,
    pub external_datacache: Option<ExternalDataCacheRangeOptions>,
}

#[derive(Clone)]
pub struct FileScanContext {
    pub ranges: Vec<FileScanRange>,
    pub factory: OpendalRangeReaderFactory,
    pub scheme: ScanPathScheme,
    pub root: Option<String>,
}

impl FileScanContext {
    pub fn build(
        ranges: Vec<FileScanRange>,
        profile: Option<RuntimeProfile>,
    ) -> Result<Self, String> {
        let paths = ranges.iter().map(|r| r.path.clone()).collect::<Vec<_>>();
        let scheme = classify_scan_paths(paths.iter().map(|s| s.as_str()))?;
        let object_store_cfg = match scheme {
            ScanPathScheme::Oss => {
                let first = paths
                    .first()
                    .ok_or_else(|| "empty scan paths for object store scan context".to_string())?;
                Some(crate::fs::oss::resolve_oss_config_for_path(first)?)
            }
            ScanPathScheme::Local => None,
        };

        let (op, resolved) = resolve_opendal_paths(&paths, object_store_cfg.as_ref())?;
        let factory = OpendalRangeReaderFactory::from_operator(op)
            .map_err(|e| e.to_string())?
            .with_profile(profile);

        let ranges = ranges
            .into_iter()
            .zip(resolved.paths.into_iter())
            .map(|(range, path)| FileScanRange { path, ..range })
            .collect::<Vec<_>>();

        match resolved.scheme {
            ScanPathScheme::Local => {
                let root = resolved.root.clone().unwrap_or_else(|| ".".to_string());
                debug!("file scan (local): {} ranges root={}", ranges.len(), root);
            }
            ScanPathScheme::Oss => {
                debug!("file scan (oss): {} ranges", ranges.len());
            }
        }

        Ok(Self {
            ranges,
            factory,
            scheme: resolved.scheme,
            root: resolved.root,
        })
    }
}
