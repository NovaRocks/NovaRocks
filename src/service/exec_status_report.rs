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

use std::collections::BTreeMap;

use crate::common::types::UniqueId;
use crate::runtime::query_context::QueryId;
use crate::runtime::sink_commit;
use crate::{data_cache, frontend_service, runtime_profile, status, types};

pub(crate) struct ExecStatusReportInput {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) backend_num: i32,
    pub(crate) status: status::TStatus,
    pub(crate) done: bool,
    pub(crate) profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) tracking_url: Option<String>,
    pub(crate) load_channel_profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
}

pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    let sink_commit_infos = sink_commit::list(input.finst_id);
    let tablet_commit_infos = sink_commit::list_tablet_commit_infos(input.finst_id);
    let tablet_fail_infos = sink_commit::list_tablet_fail_infos(input.finst_id);
    let state_stats = sink_commit::get_load_stats(input.finst_id);
    let mut normal_rows: i64 = state_stats.loaded_rows.max(0);
    let mut loaded_bytes: i64 = state_stats.loaded_bytes.max(0);
    let filtered_rows: i64 = state_stats.filtered_rows.max(0);

    for info in &sink_commit_infos {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
        if let Some(file) = info.hive_file_info.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
    }

    let load_counters = if normal_rows > 0 || loaded_bytes > 0 || filtered_rows > 0 {
        let mut counters = BTreeMap::new();
        counters.insert("dpp.norm.ALL".to_string(), normal_rows.to_string());
        counters.insert("dpp.abnorm.ALL".to_string(), filtered_rows.to_string());
        if loaded_bytes > 0 {
            counters.insert("loaded.bytes".to_string(), loaded_bytes.to_string());
        }
        Some(counters)
    } else {
        None
    };

    let tablet_commit_infos = if tablet_commit_infos.is_empty() {
        None
    } else {
        Some(tablet_commit_infos)
    };
    let sink_commit_infos = if sink_commit_infos.is_empty() {
        None
    } else {
        Some(sink_commit_infos)
    };
    let tablet_fail_infos = if tablet_fail_infos.is_empty() {
        None
    } else {
        Some(tablet_fail_infos)
    };

    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId {
            hi: input.query_id.hi,
            lo: input.query_id.lo,
        }),
        Some(input.backend_num),
        Some(types::TUniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        Some(input.status),
        Some(input.done),
        input.profile,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        load_counters,
        input.tracking_url,
        Option::<Vec<String>>::None,
        tablet_commit_infos,
        (normal_rows > 0).then_some(normal_rows),
        Option::<i64>::None,
        (loaded_bytes > 0).then_some(loaded_bytes),
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<crate::internal_service::TLoadJobType>::None,
        tablet_fail_infos,
        (filtered_rows > 0).then_some(filtered_rows),
        Option::<i64>::None,
        Option::<i64>::None,
        sink_commit_infos,
        Option::<String>::None,
        input.load_channel_profile,
        input.load_datacache_metrics,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::sink_commit;
    use crate::{status_code, types};

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    #[test]
    fn builder_collects_sink_commit_infos_and_load_counters() {
        let finst_id = UniqueId { hi: 91, lo: 92 };
        sink_commit::register(finst_id);
        sink_commit::add(
            finst_id,
            types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some("s3://warehouse/table/data-1.parquet".to_string()),
                    record_count: Some(9),
                    file_size_in_bytes: Some(90),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        sink_commit::add_load_stats(finst_id, 3, 30, 2);

        let params = build_report_params(ExecStatusReportInput {
            finst_id,
            query_id: QueryId { hi: 81, lo: 82 },
            backend_num: 7,
            status: ok_status(),
            done: true,
            profile: None,
            tracking_url: None,
            load_channel_profile: None,
            load_datacache_metrics: None,
        });

        assert_eq!(params.query_id, Some(types::TUniqueId::new(81, 82)));
        assert_eq!(
            params.fragment_instance_id,
            Some(types::TUniqueId::new(91, 92))
        );
        assert_eq!(params.backend_num, Some(7));
        assert_eq!(params.done, Some(true));
        assert_eq!(
            params
                .sink_commit_infos
                .as_ref()
                .expect("sink commit infos")
                .len(),
            1
        );
        assert_eq!(params.loaded_rows, Some(12));
        assert_eq!(params.sink_load_bytes, Some(120));
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("dpp.norm.ALL")),
            Some(&"12".to_string())
        );
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("dpp.abnorm.ALL")),
            Some(&"2".to_string())
        );
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("loaded.bytes")),
            Some(&"120".to_string())
        );
        sink_commit::unregister(finst_id);
    }

    #[test]
    fn builder_preserves_tracking_url_from_caller() {
        let finst_id = UniqueId { hi: 93, lo: 94 };
        sink_commit::register(finst_id);

        let params = build_report_params(ExecStatusReportInput {
            finst_id,
            query_id: QueryId { hi: 83, lo: 84 },
            backend_num: 8,
            status: ok_status(),
            done: false,
            profile: None,
            tracking_url: Some("http://127.0.0.1:8040/api/_load_tracking/83/84".to_string()),
            load_channel_profile: None,
            load_datacache_metrics: None,
        });

        assert_eq!(
            params.tracking_url.as_deref(),
            Some("http://127.0.0.1:8040/api/_load_tracking/83/84")
        );
        sink_commit::unregister(finst_id);
    }
}
