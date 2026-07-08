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

//! Helpers for building `TExecPlanFragmentParams` from compat fragment payloads.
//!
//! `build_exec_plan_fragment_params` consolidates root, CTE, and
//! Stream-source fragment metadata into one place, replacing ad-hoc
//! inline assembly in the coordinator.

use std::collections::BTreeMap;

use crate::runtime::fragment_exec_params::FragmentExecParams;
use crate::thrift::{
    data, data_sinks, descriptors, exprs, internal_service, partitions, plan_nodes, planner, types,
    work_group,
};

pub(crate) struct CompatFragmentPlanPayload {
    pub(crate) plan: plan_nodes::TPlan,
    pub(crate) desc_tbl: descriptors::TDescriptorTable,
    pub(crate) output_sink: data_sinks::TDataSink,
    pub(crate) output_exprs: Option<Vec<exprs::TExpr>>,
    pub(crate) fragment_partition: partitions::TDataPartition,
    pub(crate) query_global_dicts: Option<Vec<data::TGlobalDict>>,
    pub(crate) query_global_dict_exprs: Option<BTreeMap<i32, exprs::TExpr>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExecPlanFragmentParamOptions {
    pub(crate) backend_num: Option<i32>,
    pub(crate) novarocks_report_addr: Option<types::TNetworkAddress>,
    pub(crate) novarocks_typed_result_sink: bool,
}

/// Assemble a `TExecPlanFragmentParams` from a compat fragment payload.
///
/// The caller is responsible for:
/// - Mutating `exec_params.query_id`, `fragment_instance_id`,
///   `per_exch_num_senders`, and `runtime_filter_params` before calling.
/// - Passing a payload with the correct output sink (DATA_STREAM_SINK,
///   MULTI_CAST_DATA_STREAM_SINK, or RESULT_SINK).
/// - Passing `backend_num` = the FE-assigned instance index (ExecutionDAG
///   index). This becomes `RuntimeState.backend_num` and drives the sink's
///   `be_number`. See `src/service/internal_service.rs:1194`.
pub(crate) fn build_exec_plan_fragment_params(
    payload: CompatFragmentPlanPayload,
    exec_params: internal_service::TPlanFragmentExecParams,
    query_options: Option<internal_service::TQueryOptions>,
    pipeline_dop: i32,
    options: ExecPlanFragmentParamOptions,
) -> internal_service::TExecPlanFragmentParams {
    let thrift_fragment = planner::TPlanFragment::new(
        Some(payload.plan),
        payload.output_exprs,
        Some(payload.output_sink),
        payload.fragment_partition,
        None::<i64>,
        None::<i64>,
        payload.query_global_dicts,
        None::<Vec<data::TGlobalDict>>,
        None::<planner::TCacheParam>,
        payload.query_global_dict_exprs,
        None::<planner::TGroupExecutionParam>,
    );

    internal_service::TExecPlanFragmentParams::new(
        internal_service::InternalServiceVersion::V1,
        Some(thrift_fragment),
        Some(payload.desc_tbl),
        Some(exec_params),
        None::<types::TNetworkAddress>,          // coord
        options.backend_num,                     // backend_num (FE instance index)
        None::<internal_service::TQueryGlobals>, // query_globals
        query_options,
        None::<bool>,                                // enable_profile
        None::<types::TResourceInfo>,                // resource_info
        None::<String>,                              // import_label
        None::<String>,                              // db_name
        None::<i64>,                                 // load_job_id
        None::<internal_service::TLoadErrorHubInfo>, // load_error_hub_info
        Some(true),                                  // is_pipeline
        Some(pipeline_dop),
        None::<BTreeMap<types::TPlanNodeId, i32>>, // per_scan_node_dop
        None::<work_group::TWorkGroup>,            // workgroup
        None::<bool>,                              // enable_resource_group
        None::<i32>,                               // func_version
        None::<bool>,                              // enable_shared_scan
        None::<bool>,                              // is_stream_pipeline
        None::<internal_service::TAdaptiveDopParam>, // adaptive_dop_param
        None::<i32>,                               // group_execution_scan_dop
        None::<internal_service::TPredicateTreeParams>, // pred_tree_params
        None::<Vec<i32>>,                          // exec_stats_node_ids
        None::<i32>,                               // arrow_flight_sql_version
        options.novarocks_report_addr,
        options.novarocks_typed_result_sink.then_some(true),
        Some(true), // novarocks_generated_plan
    )
}

pub(crate) fn build_sink_exec_params_without_scan_ranges(
    params: &FragmentExecParams,
) -> internal_service::TPlanFragmentExecParams {
    internal_service::TPlanFragmentExecParams {
        query_id: types::TUniqueId::new(params.query_id().hi, params.query_id().lo),
        fragment_instance_id: types::TUniqueId::new(
            params.fragment_instance_id().hi,
            params.fragment_instance_id().lo,
        ),
        per_node_scan_ranges: BTreeMap::new(),
        per_exch_num_senders: params.per_exch_num_senders().clone(),
        destinations: Some(
            params
                .destinations()
                .iter()
                .cloned()
                .map(fragment_destination_to_thrift)
                .collect(),
        ),
        sender_id: None,
        num_senders: None,
        send_query_statistics_with_every_batch: None,
        use_vectorized: None,
        runtime_filter_params: None,
        instances_number: None,
        enable_exchange_pass_through: None,
        node_to_per_driver_seq_scan_ranges: None,
        enable_exchange_perf: None,
        pipeline_sink_dop: None,
        report_when_finish: None,
        exec_debug_options: None,
    }
}

pub(crate) fn fragment_destination_to_thrift(
    destination: crate::runtime::endpoint::FragmentDestination,
) -> data_sinks::TPlanFragmentDestination {
    data_sinks::TPlanFragmentDestination::new(
        destination.finst_id().clone(),
        None::<types::TNetworkAddress>,
        Some(destination.endpoint().to_network_address()),
        None::<i32>,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::thrift::data_sinks;
    use crate::thrift::descriptors;
    use crate::thrift::partitions;
    use crate::thrift::types;

    fn empty_fragment_payload_and_exec_params(
        finst_hi: i64,
        finst_lo: i64,
    ) -> (
        CompatFragmentPlanPayload,
        internal_service::TPlanFragmentExecParams,
    ) {
        let exec_params = internal_service::TPlanFragmentExecParams {
            query_id: types::TUniqueId::new(0, 0),
            fragment_instance_id: types::TUniqueId::new(finst_hi, finst_lo),
            per_node_scan_ranges: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
            destinations: None,
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: None,
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
        };

        let payload = CompatFragmentPlanPayload {
            plan: crate::thrift::plan_nodes::TPlan::new(vec![]),
            desc_tbl: descriptors::TDescriptorTable::new(vec![], vec![], vec![], false),
            output_sink: minimal_noop_sink(),
            output_exprs: None,
            fragment_partition: unpartitioned_partition(),
            query_global_dicts: None,
            query_global_dict_exprs: None,
        };

        (payload, exec_params)
    }

    fn minimal_noop_sink() -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            None::<data_sinks::TIcebergTableSink>,
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
            None::<data_sinks::TIcebergChangeStreamRouterSink>,
        )
    }

    fn unpartitioned_partition() -> partitions::TDataPartition {
        partitions::TDataPartition::new(
            partitions::TPartitionType::UNPARTITIONED,
            None::<Vec<crate::thrift::exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        )
    }

    #[test]
    fn preserves_fragment_instance_id_and_sets_query_id() {
        let finst_hi = 42i64;
        let finst_lo = 7i64;
        let query_hi = 100i64;
        let query_lo = 1i64;

        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(finst_hi, finst_lo);
        exec_params.query_id = types::TUniqueId::new(query_hi, query_lo);
        exec_params.fragment_instance_id = types::TUniqueId::new(finst_hi, finst_lo);

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            4,
            ExecPlanFragmentParamOptions::default(),
        );

        let params = result.params.expect("params must be present");
        assert_eq!(params.query_id.hi, query_hi);
        assert_eq!(params.query_id.lo, query_lo);
        assert_eq!(params.fragment_instance_id.hi, finst_hi);
        assert_eq!(params.fragment_instance_id.lo, finst_lo);
    }

    #[test]
    fn preserves_per_exch_num_senders() {
        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        exec_params.query_id = types::TUniqueId::new(1, 1);
        exec_params.fragment_instance_id = types::TUniqueId::new(1, 2);
        exec_params.per_exch_num_senders = BTreeMap::from([(3, 4)]);

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            4,
            ExecPlanFragmentParamOptions::default(),
        );

        let params = result.params.expect("params must be present");
        assert_eq!(params.per_exch_num_senders.get(&3), Some(&4));
    }

    #[test]
    fn preserves_runtime_filter_params() {
        use crate::thrift::runtime_filter;

        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        exec_params.query_id = types::TUniqueId::new(1, 1);
        exec_params.fragment_instance_id = types::TUniqueId::new(1, 2);
        let rf_params = runtime_filter::TRuntimeFilterParams::new(
            BTreeMap::new(),
            BTreeMap::new(),
            1024,
            None::<std::collections::BTreeSet<i32>>,
        );
        exec_params.runtime_filter_params = Some(rf_params.clone());

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            4,
            ExecPlanFragmentParamOptions::default(),
        );

        let params = result.params.expect("params must be present");
        assert!(
            params.runtime_filter_params.is_some(),
            "rf_params should be preserved"
        );
    }

    #[test]
    fn desc_tbl_is_embedded() {
        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        exec_params.query_id = types::TUniqueId::new(1, 1);
        exec_params.fragment_instance_id = types::TUniqueId::new(1, 2);

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            4,
            ExecPlanFragmentParamOptions::default(),
        );

        assert!(result.desc_tbl.is_some(), "desc_tbl should be embedded");
    }

    #[test]
    fn pipeline_dop_is_set() {
        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        exec_params.query_id = types::TUniqueId::new(1, 1);
        exec_params.fragment_instance_id = types::TUniqueId::new(1, 2);

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            8,
            ExecPlanFragmentParamOptions::default(),
        );

        assert_eq!(result.pipeline_dop, Some(8));
    }

    #[test]
    fn backend_num_is_threaded_to_params() {
        let (payload, mut exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        exec_params.query_id = types::TUniqueId::new(1, 1);
        exec_params.fragment_instance_id = types::TUniqueId::new(1, 2);

        let result = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            4,
            ExecPlanFragmentParamOptions {
                backend_num: Some(2),
                ..Default::default()
            },
        );

        assert_eq!(
            result.backend_num,
            Some(2),
            "backend_num must reflect the FE-assigned instance index"
        );
    }

    #[test]
    fn build_exec_params_preserves_novarocks_report_addr() {
        let (payload, exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        let report_addr = types::TNetworkAddress::new("127.0.0.1".to_string(), 18040);

        let params = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            1,
            ExecPlanFragmentParamOptions {
                backend_num: Some(3),
                novarocks_report_addr: Some(report_addr.clone()),
                ..Default::default()
            },
        );

        assert_eq!(params.novarocks_report_addr, Some(report_addr));
        assert_eq!(
            params.coord, None,
            "StarRocks FE coord must remain separate"
        );
    }

    #[test]
    fn build_exec_params_sets_typed_result_sink_only_when_requested() {
        let (legacy_payload, legacy_exec_params) = empty_fragment_payload_and_exec_params(1, 2);

        let legacy = build_exec_plan_fragment_params(
            legacy_payload,
            legacy_exec_params,
            None,
            1,
            ExecPlanFragmentParamOptions {
                backend_num: Some(3),
                ..Default::default()
            },
        );
        assert_eq!(legacy.novarocks_typed_result_sink, None);

        let (typed_payload, typed_exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        let typed = build_exec_plan_fragment_params(
            typed_payload,
            typed_exec_params,
            None,
            1,
            ExecPlanFragmentParamOptions {
                backend_num: Some(3),
                novarocks_typed_result_sink: true,
                ..Default::default()
            },
        );
        assert_eq!(typed.novarocks_typed_result_sink, Some(true));
    }

    #[test]
    fn build_exec_params_marks_novarocks_generated_plan() {
        let (payload, exec_params) = empty_fragment_payload_and_exec_params(1, 2);
        let params = build_exec_plan_fragment_params(
            payload,
            exec_params,
            None,
            1,
            ExecPlanFragmentParamOptions {
                backend_num: Some(0),
                novarocks_report_addr: None,
                novarocks_typed_result_sink: false,
            },
        );

        assert_eq!(params.novarocks_generated_plan, Some(true));
    }

    #[test]
    fn novarocks_report_addr_uses_private_thrift_field_id() {
        let idl = include_str!("../../idl/compat/thrift/InternalService.thrift");
        let struct_body = idl
            .split("struct TExecPlanFragmentParams {")
            .nth(1)
            .and_then(|rest| rest.split("\n}").next())
            .expect("TExecPlanFragmentParams struct must exist in InternalService.thrift");

        assert!(
            struct_body.contains("62: optional i32 arrow_flight_sql_version;"),
            "field 62 must stay aligned with the StarRocks FE-compatible wire contract"
        );
        assert!(
            !struct_body
                .lines()
                .any(|line| line.trim_start().starts_with("62:")
                    && (line.contains("novarocks_report_addr")
                        || line.contains("novarocks_typed_result_sink"))),
            "NovaRocks-private fields must not occupy field 62"
        );
        assert!(
            struct_body.contains("10001: optional Types.TNetworkAddress novarocks_report_addr;"),
            "novarocks_report_addr must use a NovaRocks-private high field id"
        );
        assert!(
            struct_body.contains("10002: optional bool novarocks_typed_result_sink;"),
            "novarocks_typed_result_sink must use a NovaRocks-private high field id"
        );
        assert!(
            struct_body.contains("10003: optional bool novarocks_generated_plan;"),
            "novarocks_generated_plan must use a NovaRocks-private high field id"
        );
    }
}
