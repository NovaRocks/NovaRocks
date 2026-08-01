use std::collections::BTreeMap;

use crate::thrift::{data_cache, frontend_service, runtime_profile, status, status_code, types};
use novarocks::connector::iceberg::{
    CompatIcebergColumnStats, CompatIcebergDataFile, CompatIcebergFileContent,
    CompatIcebergPartitionValue, CompatIcebergSinkCommitInfo,
};
use novarocks::novarocks_logging::debug;
use novarocks::runtime::sink_commit;
use novarocks_spi::connector::ConnectorStagedReport;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

pub(crate) struct ExecStatusReportInput {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) backend_num: i32,
    pub(crate) status: status::TStatus,
    pub(crate) done: bool,
    pub(crate) profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) tracking_url: Option<String>,
    pub(crate) load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
    pub(crate) connector_staged_reports: Vec<ConnectorStagedReport>,
    pub(crate) connector_staged_report_error: Option<String>,
}

pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    let mut status = input.status;
    let snapshot = sink_commit::report_snapshot(input.finst_id);
    let (sink_commit_infos, generic_rows, generic_bytes) = match input.connector_staged_report_error
    {
        Some(error) => {
            status =
                status::TStatus::new(status_code::TStatusCode::INTERNAL_ERROR, Some(vec![error]));
            (Vec::new(), 0, 0)
        }
        None if !input.connector_staged_reports.is_empty() => {
            match novarocks::connector::iceberg::project_compat_sink_commit_infos(
                &input.connector_staged_reports,
            ) {
                Ok(infos) => {
                    let rows =
                        input
                            .connector_staged_reports
                            .iter()
                            .fold(0_i64, |total, report| {
                                total.saturating_add(
                                    i64::try_from(report.summary().input_rows).unwrap_or(i64::MAX),
                                )
                            });
                    let bytes =
                        input
                            .connector_staged_reports
                            .iter()
                            .fold(0_i64, |total, report| {
                                total.saturating_add(
                                    i64::try_from(report.summary().staged_bytes)
                                        .unwrap_or(i64::MAX),
                                )
                            });
                    (
                        infos
                            .into_iter()
                            .map(compat_sink_commit_info_to_thrift)
                            .collect(),
                        rows,
                        bytes,
                    )
                }
                Err(error) => {
                    status = status::TStatus::new(
                        status_code::TStatusCode::INTERNAL_ERROR,
                        Some(vec![format!(
                            "project connector staged report for StarRocks: {error}"
                        )]),
                    );
                    (Vec::new(), 0, 0)
                }
            }
        }
        None => (Vec::new(), 0, 0),
    };
    let tablet_commit_infos = tablet_commit_infos_to_thrift(snapshot.tablet_commit_infos);
    let tablet_fail_infos = tablet_fail_infos_to_thrift(snapshot.tablet_fail_infos);
    let (normal_rows, loaded_bytes, filtered_rows) =
        load_stats_for_report(snapshot.load_stats, generic_rows, generic_bytes);

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

    debug!(
        target: "novarocks::sink_commit",
        finst_id = %input.finst_id,
        backend_num = input.backend_num,
        query_id = %input.query_id,
        tablet_commit_info_len = tablet_commit_infos.len(),
        tablet_fail_info_len = tablet_fail_infos.len(),
        commit_info_len = sink_commit_infos.len(),
        done = input.done,
        "reportExecStatus sink/tablet commit infos"
    );

    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId::new(
            input.query_id.high(),
            input.query_id.low(),
        )),
        Some(input.backend_num),
        Some(types::TUniqueId {
            hi: input.finst_id.high(),
            lo: input.finst_id.low(),
        }),
        Some(status),
        Some(input.done),
        input.profile,
        None::<Vec<String>>,
        None::<Vec<String>>,
        load_counters,
        input.tracking_url,
        None::<Vec<String>>,
        (!tablet_commit_infos.is_empty()).then_some(tablet_commit_infos),
        (normal_rows > 0).then_some(normal_rows),
        None::<i64>,
        (loaded_bytes > 0).then_some(loaded_bytes),
        None::<i64>,
        None::<i64>,
        None::<crate::thrift::internal_service::TLoadJobType>,
        (!tablet_fail_infos.is_empty()).then_some(tablet_fail_infos),
        (filtered_rows > 0).then_some(filtered_rows),
        None::<i64>,
        None::<i64>,
        (!sink_commit_infos.is_empty()).then_some(sink_commit_infos),
        None::<String>,
        None,
        input.load_datacache_metrics,
    )
}

fn load_stats_for_report(
    stats: sink_commit::SinkLoadStats,
    generic_rows: i64,
    generic_bytes: i64,
) -> (i64, i64, i64) {
    let normal_rows = stats.loaded_rows.max(0).saturating_add(generic_rows.max(0));
    let loaded_bytes = stats
        .loaded_bytes
        .max(0)
        .saturating_add(generic_bytes.max(0));
    let filtered_rows = stats.filtered_rows.max(0);
    (normal_rows, loaded_bytes, filtered_rows)
}

fn tablet_commit_infos_to_thrift(
    infos: Vec<sink_commit::TabletCommitInfo>,
) -> Vec<types::TTabletCommitInfo> {
    infos
        .into_iter()
        .map(|info| {
            types::TTabletCommitInfo::new(info.tablet_id, info.backend_id, None, None, None)
        })
        .collect()
}

fn tablet_fail_infos_to_thrift(
    infos: Vec<sink_commit::TabletFailInfo>,
) -> Vec<types::TTabletFailInfo> {
    infos
        .into_iter()
        .map(|info| types::TTabletFailInfo::new(Some(info.tablet_id), Some(info.backend_id)))
        .collect()
}

fn compat_sink_commit_info_to_thrift(info: CompatIcebergSinkCommitInfo) -> types::TSinkCommitInfo {
    types::TSinkCommitInfo {
        iceberg_data_file: Some(compat_iceberg_data_file_to_thrift(info.data_file)),
        hive_file_info: None,
        is_overwrite: info.is_overwrite,
        staging_dir: None,
        is_rewrite: info.is_rewrite,
    }
}

fn compat_iceberg_data_file_to_thrift(data_file: CompatIcebergDataFile) -> types::TIcebergDataFile {
    types::TIcebergDataFile {
        path: Some(data_file.path),
        format: Some(data_file.format),
        record_count: Some(data_file.record_count),
        file_size_in_bytes: Some(data_file.file_size_in_bytes),
        partition_path: Some(data_file.partition_path),
        split_offsets: data_file.split_offsets,
        column_stats: data_file.column_stats.map(compat_column_stats_to_thrift),
        partition_null_fingerprint: Some(data_file.partition_null_fingerprint),
        file_content: Some(compat_file_content_to_thrift(data_file.file_content)),
        referenced_data_file: data_file.referenced_data_file,
        first_row_id: data_file.first_row_id,
        equality_ids: data_file.equality_ids,
        key_metadata: data_file.key_metadata,
        partition_spec_id: Some(data_file.partition_spec_id),
        partition_values_descriptor: Some(types::TIcebergPartitionDescriptor {
            values: Some(
                data_file
                    .partition_values
                    .into_iter()
                    .map(compat_partition_value_to_thrift)
                    .collect(),
            ),
        }),
        content_offset: data_file.content_offset,
        content_size_in_bytes: data_file.content_size_in_bytes,
        cardinality: data_file.cardinality,
    }
}

fn compat_column_stats_to_thrift(stats: CompatIcebergColumnStats) -> types::TIcebergColumnStats {
    types::TIcebergColumnStats {
        column_sizes: stats.column_sizes,
        value_counts: stats.value_counts,
        null_value_counts: stats.null_value_counts,
        nan_value_counts: stats.nan_value_counts,
        lower_bounds: stats.lower_bounds,
        upper_bounds: stats.upper_bounds,
    }
}

fn compat_partition_value_to_thrift(
    value: CompatIcebergPartitionValue,
) -> types::TIcebergPartitionValue {
    types::TIcebergPartitionValue {
        is_null: Some(value.is_null),
        datum_bytes: value.datum_bytes,
    }
}

fn compat_file_content_to_thrift(value: CompatIcebergFileContent) -> types::TIcebergFileContent {
    match value {
        CompatIcebergFileContent::Data => types::TIcebergFileContent::DATA,
        CompatIcebergFileContent::PositionDeletes => types::TIcebergFileContent::POSITION_DELETES,
        CompatIcebergFileContent::EqualityDeletes => types::TIcebergFileContent::EQUALITY_DELETES,
    }
}
