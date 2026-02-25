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
use crate::novarocks_logging::debug;
use crate::runtime::profile::RuntimeProfile;

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
    /// Build a scan context for the given ranges.
    ///
    /// `oss_config` must be `Some` when the paths use the `oss://` / `s3://` scheme; it is
    /// unused for local and HDFS paths.  Callers are responsible for resolving the config from
    /// whatever source is appropriate (e.g. `THdfsScanNode.cloud_configuration` for Iceberg
    /// external tables, or the shard registry for native lake tablets).
    pub fn build(
        ranges: Vec<FileScanRange>,
        profile: Option<RuntimeProfile>,
        oss_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    ) -> Result<Self, String> {
        let paths = ranges.iter().map(|r| r.path.clone()).collect::<Vec<_>>();
        let scheme = classify_scan_paths(paths.iter().map(|s| s.as_str()))?;
        let object_store_cfg = match scheme {
            ScanPathScheme::Oss => {
                let cfg = oss_config.ok_or_else(|| {
                    let first = paths.first().map(|s| s.as_str()).unwrap_or("<empty>");
                    format!(
                        "missing object store config for OSS path={first}; \
                        provide credentials via THdfsScanNode.cloud_configuration \
                        (Iceberg external tables) or ensure AddShard / tablet runtime \
                        has been registered (native lake tablets)"
                    )
                })?;
                Some(cfg.clone())
            }
            ScanPathScheme::Local | ScanPathScheme::Hdfs => None,
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
            ScanPathScheme::Hdfs => {
                let root = resolved
                    .root
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string());
                debug!(
                    "file scan (hdfs): {} ranges namenode={}",
                    ranges.len(),
                    root
                );
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
