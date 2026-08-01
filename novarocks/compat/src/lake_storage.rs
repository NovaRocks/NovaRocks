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

//! Compat-owned lake storage service boundary.
//!
//! This module is deliberately the only place that interprets lake-service
//! protobuf.  The core storage kernel receives command/result facts from
//! `lake::service_domain`; generated messages never cross that boundary.

use crate::proto::starrocks::{
    AbortCompactionRequest, AbortCompactionResponse, AbortTxnRequest, AbortTxnResponse,
    CompactRequest, CompactResponse, CompactStat, DeleteDataRequest, DeleteDataResponse,
    DeleteTabletRequest, DeleteTabletResponse, DropTableRequest, DropTableResponse,
    PublishLogVersionBatchRequest, PublishLogVersionRequest, PublishLogVersionResponse,
    PublishVersionRequest, PublishVersionResponse, ReshardingTabletInfoPb, StatusPb, TabletInfoPb,
    TabletStatRequest, TabletStatResponse, TxnInfoPb, TxnLogPb, TxnTypePb, VacuumRequest,
    VacuumResponse, tablet_stat_response,
};
use novarocks::connector::starrocks::lake::service_domain::{
    AbortCompactionCommand, AbortTransactionCommand, CompactParallelConfig, CompactTabletsCommand,
    CompactionStat, DeleteDataCommand, DeleteTabletsCommand, DropLakeTableCommand,
    LakeIdenticalTabletInfo, LakeMergingTabletInfo, LakeReshardingTabletInfo,
    LakeSplittingTabletInfo, LakeTransactionInfo, LakeTransactionType,
    PublishLogVersionBatchCommand, PublishLogVersionCommand, PublishVersionCommand,
    PublishVersionResult, TabletStatsCommand, TabletStatsResult, TabletVersion, VacuumCommand,
    VacuumResult,
};
use novarocks::connector::starrocks::lake::{
    execute_abort_compaction, execute_abort_txn, execute_compact, execute_delete_data,
    execute_delete_tablet, execute_drop_table, execute_get_tablet_stats,
    execute_publish_log_version, execute_publish_log_version_batch, execute_publish_version,
    execute_vacuum,
};
use novarocks::connector::starrocks::ports::LakeStorageDependencies;
use novarocks_types::UniqueId;
use prost::Message;

use crate::storage_wire::{decode_delete_predicate, decode_schema_key, encode_transaction_log};

const STATUS_CODE_OK: i32 = 0;

/// Host-owned adapter for lake BRPC operations.
///
/// The core functions receive the dependencies on every call.  This makes
/// provider propagation visible through publish cold recovery and compaction
/// instead of relying on an ambient callback.
#[derive(Clone)]
pub(crate) struct CompatLakeStorageService {
    dependencies: LakeStorageDependencies,
}

impl CompatLakeStorageService {
    pub(crate) fn new(dependencies: LakeStorageDependencies) -> Self {
        Self { dependencies }
    }

    pub(crate) fn publish_version(
        &self,
        request: &PublishVersionRequest,
    ) -> Result<PublishVersionResponse, String> {
        let result =
            execute_publish_version(&self.dependencies, &publish_version_command(request))?;
        Ok(publish_version_response(result))
    }

    pub(crate) fn publish_log_version(
        &self,
        request: &PublishLogVersionRequest,
    ) -> Result<PublishLogVersionResponse, String> {
        let result =
            execute_publish_log_version(&self.dependencies, &publish_log_version_command(request))?;
        Ok(PublishLogVersionResponse {
            failed_tablets: result.failed_tablets,
        })
    }

    pub(crate) fn publish_log_version_batch(
        &self,
        request: &PublishLogVersionBatchRequest,
    ) -> Result<PublishLogVersionResponse, String> {
        let result = execute_publish_log_version_batch(
            &self.dependencies,
            &publish_log_version_batch_command(request),
        )?;
        Ok(PublishLogVersionResponse {
            failed_tablets: result.failed_tablets,
        })
    }

    pub(crate) fn abort_txn(&self, request: &AbortTxnRequest) -> Result<AbortTxnResponse, String> {
        let result = execute_abort_txn(&self.dependencies, &abort_transaction_command(request))?;
        Ok(AbortTxnResponse {
            failed_tablets: result.failed_tablets,
        })
    }

    pub(crate) fn drop_table(
        &self,
        request: &DropTableRequest,
    ) -> Result<DropTableResponse, String> {
        execute_drop_table(&self.dependencies, &drop_table_command(request))?;
        Ok(ok_drop_table_response())
    }

    pub(crate) fn delete_tablet(
        &self,
        request: &DeleteTabletRequest,
    ) -> Result<DeleteTabletResponse, String> {
        let result = execute_delete_tablet(&self.dependencies, &delete_tablets_command(request))?;
        Ok(DeleteTabletResponse {
            failed_tablets: result.failed_tablets,
            status: Some(ok_status()),
        })
    }

    pub(crate) fn delete_data(
        &self,
        request: &DeleteDataRequest,
    ) -> Result<DeleteDataResponse, String> {
        let result = execute_delete_data(&self.dependencies, &delete_data_command(request))?;
        Ok(DeleteDataResponse {
            failed_tablets: result.failed_tablets,
        })
    }

    pub(crate) fn get_tablet_stats(
        &self,
        request: &TabletStatRequest,
    ) -> Result<TabletStatResponse, String> {
        let result = execute_get_tablet_stats(&self.dependencies, &tablet_stats_command(request))?;
        Ok(tablet_stats_response(result))
    }

    pub(crate) fn compact(&self, request: &CompactRequest) -> Result<CompactResponse, String> {
        let result = execute_compact(&self.dependencies, &compact_command(request))?;
        compact_response(result)
    }

    pub(crate) fn abort_compaction(
        &self,
        request: &AbortCompactionRequest,
    ) -> Result<AbortCompactionResponse, String> {
        execute_abort_compaction(
            &self.dependencies,
            &AbortCompactionCommand {
                txn_id: request.txn_id,
            },
        )?;
        Ok(AbortCompactionResponse {
            status: Some(ok_status()),
        })
    }

    pub(crate) fn vacuum(&self, request: &VacuumRequest) -> Result<VacuumResponse, String> {
        let result = execute_vacuum(&self.dependencies, &vacuum_command(request))?;
        Ok(vacuum_response(result))
    }
}

fn transaction_type(value: Option<i32>) -> LakeTransactionType {
    match value.unwrap_or(TxnTypePb::TxnNormal as i32) {
        value if value == TxnTypePb::TxnNormal as i32 => LakeTransactionType::Normal,
        value if value == TxnTypePb::TxnReplication as i32 => LakeTransactionType::Replication,
        value if value == TxnTypePb::TxnEmpty as i32 => LakeTransactionType::Empty,
        value if value == TxnTypePb::TxnTabletReshard as i32 => LakeTransactionType::TabletReshard,
        value => LakeTransactionType::Unknown(value),
    }
}

fn transaction_info(info: &TxnInfoPb) -> LakeTransactionInfo {
    LakeTransactionInfo {
        txn_id: info.txn_id.unwrap_or_default(),
        commit_time: info.commit_time,
        combined_txn_log: info.combined_txn_log.unwrap_or(false),
        transaction_type: transaction_type(info.txn_type),
        force_publish: info.force_publish.unwrap_or(false),
        rebuild_pindex: info.rebuild_pindex.unwrap_or(false),
        gtid: info.gtid.unwrap_or(0),
        load_ids: info
            .load_ids
            .iter()
            .map(|id| UniqueId::new(id.hi, id.lo))
            .collect(),
    }
}

fn resharding_tablet_info(info: &ReshardingTabletInfoPb) -> LakeReshardingTabletInfo {
    LakeReshardingTabletInfo {
        splitting: info
            .splitting_tablet_info
            .as_ref()
            .map(|value| LakeSplittingTabletInfo {
                old_tablet_id: value.old_tablet_id,
                new_tablet_ids: value.new_tablet_ids.clone(),
            }),
        merging: info
            .merging_tablet_info
            .as_ref()
            .map(|value| LakeMergingTabletInfo {
                old_tablet_ids: value.old_tablet_ids.clone(),
                new_tablet_id: value.new_tablet_id,
            }),
        identical: info
            .identical_tablet_info
            .as_ref()
            .map(|value| LakeIdenticalTabletInfo {
                old_tablet_id: value.old_tablet_id,
                new_tablet_id: value.new_tablet_id,
            }),
    }
}

fn publish_version_command(request: &PublishVersionRequest) -> PublishVersionCommand {
    PublishVersionCommand {
        tablet_ids: request.tablet_ids.clone(),
        transaction_ids: request.txn_ids.clone(),
        base_version: request.base_version,
        new_version: request.new_version,
        commit_time: request.commit_time,
        timeout_ms: request.timeout_ms,
        transactions: request.txn_infos.iter().map(transaction_info).collect(),
        rebuild_pindex_tablet_ids: request.rebuild_pindex_tablet_ids.clone(),
        enable_aggregate_publish: request.enable_aggregate_publish,
        resharding_tablet_infos: request
            .resharding_tablet_infos
            .iter()
            .map(resharding_tablet_info)
            .collect(),
    }
}

fn publish_log_version_command(request: &PublishLogVersionRequest) -> PublishLogVersionCommand {
    PublishLogVersionCommand {
        tablet_ids: request.tablet_ids.clone(),
        transaction_id: request.txn_id,
        version: request.version,
        transaction: request.txn_info.as_ref().map(transaction_info),
    }
}

fn publish_log_version_batch_command(
    request: &PublishLogVersionBatchRequest,
) -> PublishLogVersionBatchCommand {
    PublishLogVersionBatchCommand {
        tablet_ids: request.tablet_ids.clone(),
        transaction_ids: request.txn_ids.clone(),
        versions: request.versions.clone(),
        transactions: request.txn_infos.iter().map(transaction_info).collect(),
    }
}

fn abort_transaction_command(request: &AbortTxnRequest) -> AbortTransactionCommand {
    AbortTransactionCommand {
        tablet_ids: request.tablet_ids.clone(),
        transaction_ids: request.txn_ids.clone(),
        skip_cleanup: request.skip_cleanup,
        transaction_types: request
            .txn_types
            .iter()
            .copied()
            .map(|value| transaction_type(Some(value)))
            .collect(),
        transactions: request.txn_infos.iter().map(transaction_info).collect(),
    }
}

fn drop_table_command(request: &DropTableRequest) -> DropLakeTableCommand {
    DropLakeTableCommand {
        tablet_id: request.tablet_id,
        path: request.path.clone(),
    }
}

fn delete_tablets_command(request: &DeleteTabletRequest) -> DeleteTabletsCommand {
    DeleteTabletsCommand {
        tablet_ids: request.tablet_ids.clone(),
    }
}

fn delete_data_command(request: &DeleteDataRequest) -> DeleteDataCommand {
    DeleteDataCommand {
        tablet_ids: request.tablet_ids.clone(),
        txn_id: request.txn_id,
        delete_predicate: request
            .delete_predicate
            .clone()
            .map(decode_delete_predicate),
        schema_key: request.schema_key.clone().map(decode_schema_key),
    }
}

fn tablet_stats_command(request: &TabletStatRequest) -> TabletStatsCommand {
    TabletStatsCommand {
        tablet_versions: request
            .tablet_infos
            .iter()
            .map(|info| TabletVersion {
                tablet_id: info.tablet_id.unwrap_or_default(),
                version: info.version.unwrap_or_default(),
            })
            .collect(),
        timeout_ms: request.timeout_ms,
    }
}

fn compact_command(request: &CompactRequest) -> CompactTabletsCommand {
    CompactTabletsCommand {
        tablet_ids: request.tablet_ids.clone(),
        txn_id: request.txn_id,
        version: request.version,
        timeout_ms: request.timeout_ms,
        allow_partial_success: request.allow_partial_success,
        encryption_meta: request.encryption_meta.clone(),
        force_base_compaction: request.force_base_compaction,
        skip_write_txnlog: request.skip_write_txnlog,
        parallel_config: request
            .parallel_config
            .as_ref()
            .map(|config| CompactParallelConfig {
                enable_parallel: config.enable_parallel,
                max_parallel_per_tablet: config.max_parallel_per_tablet,
                max_bytes_per_subtask: config.max_bytes_per_subtask,
            }),
    }
}

fn vacuum_command(request: &VacuumRequest) -> VacuumCommand {
    VacuumCommand {
        tablet_ids: request.tablet_ids.clone(),
        tablet_min_versions: request
            .tablet_infos
            .iter()
            .map(|info| (info.tablet_id.unwrap_or_default(), info.min_version))
            .collect(),
        min_retain_version: request.min_retain_version,
        grace_timestamp: request.grace_timestamp,
        min_active_txn_id: request.min_active_txn_id,
        delete_txn_log: request.delete_txn_log,
        partition_id: request.partition_id,
        enable_file_bundling: request.enable_file_bundling,
        retain_versions: request.retain_versions.clone(),
    }
}

fn ok_status() -> StatusPb {
    StatusPb {
        status_code: STATUS_CODE_OK,
        error_msgs: Vec::new(),
    }
}

fn publish_version_response(result: PublishVersionResult) -> PublishVersionResponse {
    PublishVersionResponse {
        failed_tablets: result.failed_tablets,
        compaction_scores: result.compaction_scores,
        status: Some(ok_status()),
        tablet_row_nums: result.tablet_row_nums,
        tablet_metas: Default::default(),
        tablet_ranges: Default::default(),
    }
}

fn ok_drop_table_response() -> DropTableResponse {
    DropTableResponse {
        pad: None,
        status: Some(ok_status()),
    }
}

fn tablet_stats_response(result: TabletStatsResult) -> TabletStatResponse {
    TabletStatResponse {
        tablet_stats: result
            .tablet_stats
            .into_iter()
            .map(|stat| tablet_stat_response::TabletStat {
                tablet_id: Some(stat.tablet_id),
                num_rows: Some(stat.num_rows),
                data_size: Some(stat.data_size),
            })
            .collect(),
    }
}

fn compact_response(
    result: novarocks::connector::starrocks::lake::service_domain::CompactTabletsResult,
) -> Result<CompactResponse, String> {
    Ok(CompactResponse {
        failed_tablets: result.failed_tablets,
        status: Some(ok_status()),
        compact_stats: result.compact_stats.into_iter().map(compact_stat).collect(),
        success_compaction_input_file_size: Some(result.success_compaction_input_file_size),
        txn_logs: result
            .txn_logs
            .iter()
            .map(encode_transaction_log)
            .map(|result| {
                result.and_then(|bytes| {
                    TxnLogPb::decode(bytes.as_slice())
                        .map_err(|error| format!("encode compact txn log failed: {error}"))
                })
            })
            .collect::<Result<_, _>>()?,
        subtask_statuses: Vec::new(),
    })
}

fn compact_stat(stat: CompactionStat) -> CompactStat {
    CompactStat {
        tablet_id: Some(stat.tablet_id),
        read_time_remote: None,
        read_bytes_remote: None,
        read_time_local: None,
        read_bytes_local: None,
        total_compact_input_file_size: Some(stat.total_compact_input_file_size),
        read_segment_count: Some(stat.read_segment_count),
        write_segment_count: Some(stat.write_segment_count),
        write_segment_bytes: Some(stat.write_segment_bytes),
        write_time_remote: None,
        sub_task_count: None,
        in_queue_time_sec: None,
    }
}

fn vacuum_response(result: VacuumResult) -> VacuumResponse {
    VacuumResponse {
        status: Some(ok_status()),
        vacuumed_files: Some(result.vacuumed_files),
        vacuumed_file_size: Some(result.vacuumed_file_size),
        vacuumed_version: Some(result.vacuumed_version),
        tablet_infos: result
            .tablet_min_versions
            .into_iter()
            .map(|(tablet_id, min_version)| TabletInfoPb {
                tablet_id: Some(tablet_id),
                min_version: Some(min_version),
            })
            .collect(),
        extra_file_size: Some(result.extra_file_size),
    }
}

/// The result shape consumed by the opaque-context FFI adapter.
///
/// Return code `2` is reserved for malformed protobuf input, matching the
/// BRPC bridge; service failures retain code `1` and a textual error.
pub(crate) enum LakeWireResult {
    Response(Vec<u8>),
    Error { code: i32, message: String },
}

pub(crate) fn decode_lake_request<Request, Response>(
    operation: &str,
    bytes: &[u8],
    execute: impl FnOnce(&Request) -> Result<Response, String>,
) -> LakeWireResult
where
    Request: Message + Default,
    Response: Message,
{
    let request = match Request::decode(bytes) {
        Ok(request) => request,
        Err(error) => {
            return LakeWireResult::Error {
                code: 2,
                message: format!("decode lake {operation} request failed: {error}"),
            };
        }
    };
    match execute(&request) {
        Ok(response) => LakeWireResult::Response(response.encode_to_vec()),
        Err(message) => LakeWireResult::Error { code: 1, message },
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use crate::proto::starrocks::{
        PUniqueId, PublishVersionRequest, ReshardingTabletInfoPb, SplittingTabletInfoPb,
        TabletParallelConfig, TxnInfoPb,
    };
    use novarocks::connector::starrocks::lake::service_domain::{
        CompactTabletsResult, LakeTransactionType,
    };
    use novarocks::connector::starrocks::lake::storage_domain::StorageTransactionLog;

    use super::{
        CompactRequest, LakeWireResult, compact_command, compact_response, decode_lake_request,
        publish_version_command,
    };

    #[test]
    fn publish_version_command_preserves_transaction_and_resharding_facts() {
        let request = PublishVersionRequest {
            tablet_ids: vec![7],
            txn_ids: vec![11],
            base_version: Some(5),
            new_version: Some(6),
            commit_time: Some(123),
            timeout_ms: Some(456),
            txn_infos: vec![TxnInfoPb {
                txn_id: Some(11),
                commit_time: Some(123),
                combined_txn_log: Some(true),
                txn_type: Some(77),
                force_publish: Some(true),
                rebuild_pindex: Some(true),
                gtid: Some(99),
                load_ids: vec![PUniqueId { hi: 1, lo: -2 }],
            }],
            rebuild_pindex_tablet_ids: vec![7],
            enable_aggregate_publish: Some(true),
            resharding_tablet_infos: vec![ReshardingTabletInfoPb {
                splitting_tablet_info: Some(SplittingTabletInfoPb {
                    old_tablet_id: Some(7),
                    new_tablet_ids: vec![8, 9],
                }),
                merging_tablet_info: None,
                identical_tablet_info: None,
            }],
        };

        let command = publish_version_command(&request);
        assert_eq!(
            command.transactions[0].transaction_type,
            LakeTransactionType::Unknown(77)
        );
        assert_eq!(command.transactions[0].load_ids[0].high(), 1);
        assert_eq!(command.transactions[0].load_ids[0].low(), -2);
        assert_eq!(
            command.resharding_tablet_infos[0]
                .splitting
                .as_ref()
                .unwrap()
                .new_tablet_ids,
            vec![8, 9]
        );
    }

    #[test]
    fn compact_command_preserves_parallel_configuration() {
        let command = compact_command(&CompactRequest {
            tablet_ids: vec![1],
            txn_id: Some(2),
            version: Some(3),
            timeout_ms: Some(4),
            allow_partial_success: Some(true),
            encryption_meta: Some(vec![5]),
            force_base_compaction: Some(true),
            skip_write_txnlog: Some(true),
            parallel_config: Some(TabletParallelConfig {
                enable_parallel: Some(true),
                max_parallel_per_tablet: Some(5),
                max_bytes_per_subtask: Some(6),
            }),
        });

        assert_eq!(
            command.parallel_config.unwrap().max_bytes_per_subtask,
            Some(6)
        );
    }

    #[test]
    fn compact_response_uses_storage_codec_for_txn_logs() {
        let response = compact_response(CompactTabletsResult {
            txn_logs: vec![StorageTransactionLog {
                tablet_id: Some(1),
                txn_id: Some(2),
                write: None,
                compaction: None,
                schema_change: None,
                alter_metadata: None,
                replication: None,
                partition_id: None,
                load_id: None,
            }],
            ..Default::default()
        })
        .expect("transaction log must use the compat storage codec");

        assert_eq!(response.txn_logs[0].tablet_id, Some(1));
        assert_eq!(response.txn_logs[0].txn_id, Some(2));
    }

    #[test]
    fn wire_helper_preserves_decode_error_contract() {
        #[derive(Clone, PartialEq, Message)]
        struct TestRequest {
            #[prost(uint64, tag = "1")]
            value: u64,
        }
        #[derive(Clone, PartialEq, Message)]
        struct TestResponse {
            #[prost(uint64, tag = "1")]
            value: u64,
        }

        let LakeWireResult::Error { code, message } =
            decode_lake_request::<TestRequest, TestResponse>("publish_version", &[0xff], |_| {
                unreachable!("malformed protobuf must not execute the service")
            })
        else {
            panic!("malformed protobuf must return an error");
        };

        assert_eq!(code, 2);
        assert!(message.starts_with("decode lake publish_version request failed:"));
    }

    #[test]
    fn wire_helper_preserves_kernel_error_contract() {
        #[derive(Clone, PartialEq, Message)]
        struct TestRequest {
            #[prost(uint64, tag = "1")]
            value: u64,
        }
        #[derive(Clone, PartialEq, Message)]
        struct TestResponse {
            #[prost(uint64, tag = "1")]
            value: u64,
        }

        let request = TestRequest { value: 7 }.encode_to_vec();
        let LakeWireResult::Error { code, message } =
            decode_lake_request::<TestRequest, TestResponse>("compact", &request, |_| {
                Err("compact has non-positive tablet_id=0".to_string())
            })
        else {
            panic!("kernel error must return an error");
        };

        assert_eq!(code, 1);
        assert_eq!(message, "compact has non-positive tablet_id=0");
    }
}
