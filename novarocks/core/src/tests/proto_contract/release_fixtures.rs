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

use prost::Message;

use crate::proto::{common, expr, filter, novarocks, plan};

const FETCH_RESULT_RESPONSE_FIXTURE_HEX: &str =
    "0801120572656164791a0c4e5258312d6669787475726520092801";
const REPORT_QUERY_TERMINAL_REQUEST_FIXTURE_HEX: &str = "0ab401080112080a040801100210011a140809120e0a093132372e302e302e3110903f184d222011111111111111111111111111111111111111111111111111111111111111112a202222222222222222222222222222222222222222222222222222222222222222324c0a0408031004100918013a04086410094a060809105a180152320a300a105465726d696e616c467261676d656e741009221a0a06736f7572636512107465726d696e616c2d66697874757265";
const LOOKUP_REQUEST_FIXTURE_HEX: &str = "0a04080110021021182c220a083710041a0400010203";
const LOOKUP_RESPONSE_FIXTURE_HEX: &str = "0a0412024f4b120a083710041a0403020100";
const PLAN_FRAGMENT_FIXTURE_HEX: &str = "0801128b03080a10011a010a28ffffffffffffffffff01426c080b10011a010b28ffffffffffffffffff0152580a0c0801120269641a040a02080552480a047470636812160a086c696e656974656d120a0a02696412040a0208051a086c696e656974656d220c0801120269641a040a0208052a0c0a040a0208015a040a021001320269644256080c10011a010c28ffffffffffffffffff015a42080312220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65791802220c0801120269641a040a0208052a0672656d6f74653202080152b0010a0c0801120269641a040a020805c2019e01080112480a220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b657912220a040a02080510015218080212086c696e656974656d1a0a6f5f6f726465726b657920022802324c084d12220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65791a220a040a02080510015218080212086c696e656974656d1a0a6f5f6f726465726b657928021a26080312220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65792226080312220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65792a02080132220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65793a0c0801120269641a040a020805";
const EXPR_FIXTURE_HEX: &str = "0a040a0208016234080a12220a040a02080510015218080112086c696e656974656d1a0a6c5f6f726465726b65791a0c0a040a0208055a040a02180a";

fn decode_fixture<M>(name: &str, hex: &str) -> M
where
    M: Message + Default,
{
    let bytes = decode_hex(name, hex);
    M::decode(bytes.as_slice())
        .unwrap_or_else(|err| panic!("{name}: failed to decode release fixture bytes: {err}"))
}

fn decode_hex(name: &str, hex: &str) -> Vec<u8> {
    let compact = hex
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != '_')
        .collect::<String>();
    assert!(
        !compact.is_empty(),
        "{name}: release fixture hex must be checked in"
    );
    assert_eq!(
        compact.len() % 2,
        0,
        "{name}: release fixture hex length must be even"
    );
    compact
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let s = std::str::from_utf8(pair).expect("hex pair must be utf8");
            u8::from_str_radix(s, 16)
                .unwrap_or_else(|err| panic!("{name}: invalid hex byte `{s}`: {err}"))
        })
        .collect()
}

fn id(hi: i64, lo: i64) -> common::UniqueId {
    common::UniqueId { hi, lo }
}

fn scalar_type(prim: common::PrimitiveType) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: prim as i32,
            len: None,
            precision: None,
            scale: None,
            time_unit: None,
        })),
    }
}

fn column_expr(column_id: u32, name: &str) -> expr::Expr {
    expr::Expr {
        r#type: Some(scalar_type(common::PrimitiveType::Bigint)),
        nullable: true,
        kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
            column_id,
            qualifier: Some("lineitem".to_string()),
            column: Some(name.to_string()),
        })),
    }
}

fn literal_bool(value: bool) -> expr::Expr {
    expr::Expr {
        r#type: Some(scalar_type(common::PrimitiveType::Boolean)),
        nullable: false,
        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
            value: Some(common::LiteralValue {
                value: Some(common::literal_value::Value::BoolValue(value)),
            }),
        })),
    }
}

fn output_column(column_id: u32, name: &str, prim: common::PrimitiveType) -> common::OutputColumn {
    common::OutputColumn {
        column_id,
        name: name.to_string(),
        r#type: Some(scalar_type(prim)),
        nullable: false,
        is_internal: false,
    }
}

fn release_query_options() -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: 4096,
        query_timeout: 300,
        enable_profile: true,
        pipeline_dop: 8,
        query_mem_limit: 512 << 20,
        connector_io_tasks_per_scan_operator: 4,
        runtime_filter_scan_wait_time_ms: Some(1500),
        runtime_filter_wait_timeout_ms: Some(3000),
        allow_throw_exception: true,
        group_concat_max_len: Some(65_536),
        enable_spill: true,
        spill_options: Some(novarocks::SpillOptions {
            spill_mode: 2,
            spill_mem_limit_threshold: 0.8,
            spill_operator_min_bytes: 1 << 20,
            spill_operator_max_bytes: 64 << 20,
            spill_encode_level: 1,
            enable_spill_buffer_read: true,
            max_spill_read_buffer_bytes_per_driver: 8 << 20,
            spill_mem_table_size: 16 << 20,
            spill_mem_table_num: 3,
        }),
        enable_scan_datacache: true,
        enable_populate_datacache: true,
        enable_datacache_async_populate_mode: true,
        enable_datacache_io_adaptor: true,
        enable_cache_select: true,
        datacache_evict_probability: Some(75),
        datacache_priority: 2,
        datacache_ttl_seconds: 3600,
        datacache_sharing_work_period: 10,
        query_delivery_timeout: 30,
        runtime_profile_report_interval: 7,
        enable_join_runtime_bitset_filter: Some(true),
        global_runtime_filter_build_max_size: 1 << 20,
        orc_use_column_names: false,
        enable_file_metacache: false,
        enable_file_pagecache: false,
        enable_parquet_reader_page_index: false,
    }
}

fn release_scan_range() -> novarocks::ScanRangeParams {
    novarocks::ScanRangeParams {
        range: Some(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::File(
                novarocks::FileScanRange {
                    file_format: "PARQUET".to_string(),
                    full_path: Some("s3://bucket/data.parquet".to_string()),
                    relative_path: Some("data.parquet".to_string()),
                    table_id: Some(99),
                    offset: 8,
                    length: 16,
                    file_length: 128,
                    delete_files: vec![novarocks::IcebergDeleteFile {
                        full_path: Some("s3://bucket/delete.parquet".to_string()),
                        file_format: "PARQUET".to_string(),
                        file_content: "POSITION_DELETES".to_string(),
                        length: Some(64),
                    }],
                    deletion_vector_descriptor: None,
                    first_row_id: Some(1_000),
                    data_sequence_number: Some(44),
                    modification_time: Some(123_456),
                    datacache_options: Some(novarocks::DatacacheOptions {
                        enable_populate_datacache: Some(true),
                        priority: Some(3),
                    }),
                    included_positions: vec![3, 5, 8],
                    serialized_split: Some("{\"split\":1}".to_string()),
                    use_iceberg_jni_metadata_reader: true,
                    change_op: Some(-1),
                    file_pruning_min_max_values: HashMap::from([(
                        1,
                        novarocks::FilePruningMinMaxValue {
                            value_kind: novarocks::FilePruningValueKind::FilePruningInt as i32,
                            has_null: true,
                            all_null: false,
                            min_int_value: Some(10),
                            max_int_value: Some(20),
                            min_float_value: None,
                            max_float_value: None,
                        },
                    )]),
                },
            )),
        }),
        volume_id: Some(13),
        empty: Some(false),
        has_more: Some(false),
    }
}

fn release_destination() -> novarocks::Destination {
    novarocks::Destination {
        finst_id: Some(id(3, 4)),
        endpoint: "10.0.0.8:8060".to_string(),
    }
}

fn release_plan_fragment() -> plan::PlanFragment {
    plan::PlanFragment {
        fragment_id: 1,
        root: Some(plan::DistributedNode {
            node_id: 10,
            fragment_id: 1,
            tuple_ids: vec![10],
            nullable_tuple_ids: vec![],
            limit: -1,
            runtime_filter_binding_ids: vec![],
            children: vec![
                plan::DistributedNode {
                    node_id: 11,
                    fragment_id: 1,
                    tuple_ids: vec![11],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    runtime_filter_binding_ids: vec![],
                    children: vec![],
                    payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                        output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                        kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                            database: "tpch".to_string(),
                            table: Some(plan::TableDef {
                                name: "lineitem".to_string(),
                                columns: vec![plan::ColumnDef {
                                    name: "id".to_string(),
                                    data_type: Some(scalar_type(common::PrimitiveType::Bigint)),
                                    nullable: false,
                                    write_default_json: None,
                                    logical_type: None,
                                }],
                                iceberg_row_lineage_metadata_columns: vec![],
                                source: None,
                            }),
                            alias: Some("lineitem".to_string()),
                            columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                            predicates: vec![literal_bool(true)],
                            required_columns: vec!["id".to_string()],
                            dict_columns: vec![],
                            variant_columns: vec![],
                            mv_rewritten_from: None,
                        })),
                    })),
                },
                plan::DistributedNode {
                    node_id: 12,
                    fragment_id: 1,
                    tuple_ids: vec![12],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    runtime_filter_binding_ids: vec![],
                    children: vec![],
                    payload: Some(plan::distributed_node::Payload::Exchange(
                        plan::ExchangeReceiver {
                            partition_type: plan::PartitionType::Hash as i32,
                            partition_exprs: vec![column_expr(1, "l_orderkey")],
                            source_fragment_id: 2,
                            output_columns: vec![output_column(
                                1,
                                "id",
                                common::PrimitiveType::Bigint,
                            )],
                            output_qualifier: Some("remote".to_string()),
                            flavor: Some(plan::ExchangeFlavor {
                                kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                            }),
                        },
                    )),
                },
            ],
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                kind: Some(plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                    join_type: plan::JoinKind::Inner as i32,
                    eq_conditions: vec![plan::HashJoinEqCondition {
                        left: Some(column_expr(1, "l_orderkey")),
                        right: Some(column_expr(2, "o_orderkey")),
                        null_safe: false,
                    }],
                    other_condition: None,
                    distribution: plan::JoinDistribution::Shuffle as i32,
                    execution_mode: Some(plan::JoinExecutionMode::Partitioned as i32),
                })),
            })),
        }),
        data_partition: Some(plan::DataPartition {
            kind: plan::PartitionKind::Hash as i32,
            exprs: vec![column_expr(1, "l_orderkey")],
        }),
        output_partition: Some(plan::DataPartition {
            kind: plan::PartitionKind::Hash as i32,
            exprs: vec![column_expr(1, "l_orderkey")],
        }),
        sink: Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::Result(true)),
        }),
        output_exprs: vec![column_expr(1, "l_orderkey")],
        output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
        cte_id: None,
        cte_exchange_nodes: vec![],
        runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![],
        }),
    }
}

fn release_stage_fragments_request() -> novarocks::StageFragmentsRequest {
    novarocks::StageFragmentsRequest {
        execution_id: Some(novarocks::QueryExecutionId {
            query_id: Some(id(1, 2)),
            attempt_id: 1,
        }),
        init_digest: vec![0x11; 32],
        stage_digest_version: 1,
        stage_digest: vec![0x22; 32],
        fragments: vec![novarocks::StageFragment {
            plan: Some(release_plan_fragment()),
            instance_params: Some(novarocks::InstanceParams {
                query_id: Some(id(1, 2)),
                fragment_instance_id: Some(id(3, 4)),
                backend_num: 9,
                per_node_scan_ranges: HashMap::from([(
                    11,
                    novarocks::ScanRangeList {
                        ranges: vec![release_scan_range()],
                    },
                )]),
                per_exch_num_senders: HashMap::from([(12, 3)]),
                destinations: vec![release_destination()],
                query_options: Some(release_query_options()),
                typed_result_sink: true,
            }),
        }],
    }
}

fn release_fetch_result_response() -> novarocks::FetchResultResponse {
    novarocks::FetchResultResponse {
        status: novarocks::fetch_result_response::Status::Ready as i32,
        message: "ready".to_string(),
        result_arrow_ipc: b"NRX1-fixture".to_vec(),
        packet_seq: 9,
        eos: true,
    }
}

fn release_report_query_terminal_request() -> novarocks::ReportQueryTerminalRequest {
    novarocks::ReportQueryTerminalRequest {
        snapshot: Some(novarocks::QueryTerminalSnapshot {
            version: 1,
            execution_id: Some(novarocks::QueryExecutionId {
                query_id: Some(id(1, 2)),
                attempt_id: 1,
            }),
            backend: Some(novarocks::ParticipantBackendIdentity {
                backend_id: 9,
                endpoint: Some(novarocks::QueryControlEndpoint {
                    host: "127.0.0.1".to_string(),
                    port: 8080,
                }),
                start_epoch: 77,
            }),
            init_digest: vec![0x11; 32],
            digest: vec![0x22; 32],
            fragments: vec![novarocks::QueryTerminalFragmentSnapshot {
                fragment_instance_id: Some(id(3, 4)),
                backend_num: 9,
                outcome: novarocks::QueryTerminalFragmentOutcome::Succeeded as i32,
                error_code: String::new(),
                error_detail: String::new(),
                connector_staged_report_frames: vec![],
                tablet_commit_infos: vec![novarocks::QueryTerminalTabletInfo {
                    tablet_id: 100,
                    backend_id: 9,
                }],
                tablet_fail_infos: vec![],
                load_stats: Some(novarocks::QueryTerminalLoadStats {
                    loaded_rows: 9,
                    loaded_bytes: 90,
                    filtered_rows: 1,
                }),
                profile: Some(novarocks::RuntimeProfileTree {
                    root: Some(novarocks::ProfileNode {
                        name: "TerminalFragment".to_string(),
                        node_id: 9,
                        counters: vec![],
                        info_strings: HashMap::from([(
                            "source".to_string(),
                            "terminal-fixture".to_string(),
                        )]),
                        children: vec![],
                    }),
                }),
            }],
        }),
    }
}

fn release_lookup_request() -> filter::LookupRequest {
    filter::LookupRequest {
        query_id: Some(id(1, 2)),
        lookup_node_id: 33,
        request_tuple_id: 44,
        request_columns: vec![filter::Column {
            slot_id: 55,
            data_size: 4,
            data: vec![0, 1, 2, 3],
        }],
    }
}

fn release_lookup_response() -> filter::LookupResponse {
    filter::LookupResponse {
        status: Some(common::Status {
            code: 0,
            message: "OK".to_string(),
        }),
        columns: vec![filter::Column {
            slot_id: 55,
            data_size: 4,
            data: vec![3, 2, 1, 0],
        }],
    }
}

fn release_expr() -> expr::Expr {
    expr::Expr {
        r#type: Some(scalar_type(common::PrimitiveType::Boolean)),
        nullable: false,
        kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
            op: expr::BinaryOp::Gt as i32,
            left: Some(Box::new(column_expr(1, "l_orderkey"))),
            right: Some(Box::new(expr::Expr {
                r#type: Some(scalar_type(common::PrimitiveType::Bigint)),
                nullable: false,
                kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::IntValue(10)),
                    }),
                })),
            })),
        }))),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn print_fixture<M: Message>(name: &str, message: &M) {
    println!("{name}={}", hex(&message.encode_to_vec()));
}

#[test]
#[ignore = "manual release fixture recorder; paste output into checked-in constants"]
fn print_release_fixture_hex() {
    print_fixture(
        "STAGE_FRAGMENTS_REQUEST",
        &release_stage_fragments_request(),
    );
    print_fixture("FETCH_RESULT_RESPONSE", &release_fetch_result_response());
    print_fixture(
        "REPORT_QUERY_TERMINAL_REQUEST",
        &release_report_query_terminal_request(),
    );
    print_fixture("LOOKUP_REQUEST", &release_lookup_request());
    print_fixture("LOOKUP_RESPONSE", &release_lookup_response());
    print_fixture("PLAN_FRAGMENT", &release_plan_fragment());
    print_fixture("EXPR", &release_expr());
}

#[test]
fn release_stage_fragments_request_fixture_decodes() {
    let request = release_stage_fragments_request();
    let bytes = request.encode_to_vec();
    let request = novarocks::StageFragmentsRequest::decode(bytes.as_slice())
        .expect("StageFragmentsRequest fixture decodes");
    assert_eq!(request.stage_digest_version, 1);
    assert_eq!(request.fragments.len(), 1);
    let fragment = request
        .fragments
        .first()
        .expect("StageFragmentsRequest fixture fragment");
    let plan = fragment
        .plan
        .as_ref()
        .expect("StageFragmentsRequest fixture plan");
    assert_eq!(plan.fragment_id, 1, "StageFragmentsRequest fixture plan id");

    let params = fragment
        .instance_params
        .as_ref()
        .expect("StageFragmentsRequest fixture instance_params");
    assert_eq!(
        params.backend_num, 9,
        "StageFragmentsRequest fixture backend_num"
    );
    let scan_ranges = params
        .per_node_scan_ranges
        .get(&11)
        .expect("StageFragmentsRequest fixture per_node_scan_ranges[11]");
    assert_eq!(
        scan_ranges.ranges.len(),
        1,
        "StageFragmentsRequest fixture per_node_scan_ranges[11].ranges.len"
    );
    let scan_range = scan_ranges
        .ranges
        .first()
        .and_then(|params| params.range.as_ref())
        .expect("StageFragmentsRequest fixture per_node_scan_ranges[11].ranges[0].range");
    let file_range = match scan_range.kind.as_ref() {
        Some(novarocks::scan_range::Kind::File(file)) => file,
        other => panic!(
            "StageFragmentsRequest fixture per_node_scan_ranges[11].ranges[0].range.kind expected File, got {other:?}"
        ),
    };
    assert_eq!(
        file_range.file_format, "PARQUET",
        "StageFragmentsRequest fixture FileScanRange.file_format"
    );
    assert_eq!(
        file_range.full_path.as_deref(),
        Some("s3://bucket/data.parquet"),
        "StageFragmentsRequest fixture FileScanRange.full_path"
    );
    assert_eq!(
        file_range.delete_files.len(),
        1,
        "StageFragmentsRequest fixture FileScanRange.delete_files.len"
    );
    let delete_file = &file_range.delete_files[0];
    assert_eq!(
        delete_file.full_path.as_deref(),
        Some("s3://bucket/delete.parquet"),
        "StageFragmentsRequest fixture FileScanRange.delete_files[0].full_path"
    );
    assert_eq!(
        delete_file.file_content, "POSITION_DELETES",
        "StageFragmentsRequest fixture FileScanRange.delete_files[0].file_content"
    );
    assert_eq!(
        delete_file.length,
        Some(64),
        "StageFragmentsRequest fixture FileScanRange.delete_files[0].length"
    );
    assert_eq!(
        file_range.first_row_id,
        Some(1_000),
        "StageFragmentsRequest fixture FileScanRange.first_row_id"
    );
    assert_eq!(
        file_range.data_sequence_number,
        Some(44),
        "StageFragmentsRequest fixture FileScanRange.data_sequence_number"
    );
    assert_eq!(
        file_range
            .datacache_options
            .as_ref()
            .and_then(|options| options.priority),
        Some(3),
        "StageFragmentsRequest fixture FileScanRange.datacache_options.priority"
    );
    assert_eq!(
        file_range.included_positions,
        vec![3, 5, 8],
        "StageFragmentsRequest fixture FileScanRange.included_positions"
    );
    assert_eq!(
        file_range.serialized_split.as_deref(),
        Some("{\"split\":1}"),
        "StageFragmentsRequest fixture FileScanRange.serialized_split"
    );
    assert_eq!(
        file_range.change_op,
        Some(-1),
        "StageFragmentsRequest fixture FileScanRange.change_op"
    );
    let pruning = file_range
        .file_pruning_min_max_values
        .get(&1)
        .expect("StageFragmentsRequest fixture FileScanRange.file_pruning_min_max_values[1]");
    assert_eq!(
        pruning.min_int_value,
        Some(10),
        "StageFragmentsRequest fixture FileScanRange.file_pruning_min_max_values[1].min_int_value"
    );
    assert_eq!(
        pruning.max_int_value,
        Some(20),
        "StageFragmentsRequest fixture FileScanRange.file_pruning_min_max_values[1].max_int_value"
    );

    let query_options = crate::protocol::decode_native_query_options(
        params
            .query_options
            .as_ref()
            .expect("StageFragmentsRequest fixture query_options"),
    )
    .expect("StageFragmentsRequest fixture query_options DTO");
    assert_eq!(query_options.exec_mem_limit(), Some(512 << 20));
    assert_eq!(query_options.pipeline_dop(), Some(8));
    assert_eq!(query_options.runtime_filter_scan_wait_time_ms(), Some(1500));
    assert_eq!(query_options.runtime_filter_wait_timeout_ms(), Some(3000));

    let destination = params
        .destinations
        .first()
        .expect("StageFragmentsRequest fixture destination");
    assert_eq!(destination.endpoint, "10.0.0.8:8060");
    assert!(destination.finst_id.is_some());
    assert!(
        params.per_node_scan_ranges[&11]
            .ranges
            .first()
            .and_then(|range| range.range.as_ref())
            .and_then(|range| range.kind.as_ref())
            .is_some_and(|kind| matches!(kind, novarocks::scan_range::Kind::File(_)))
    );
}

#[test]
fn release_fetch_result_response_fixture_decodes() {
    let response: novarocks::FetchResultResponse =
        decode_fixture("FetchResultResponse", FETCH_RESULT_RESPONSE_FIXTURE_HEX);
    assert_eq!(
        response.status,
        novarocks::fetch_result_response::Status::Ready as i32,
        "FetchResultResponse fixture status"
    );
    assert_eq!(response.message, "ready");
    assert_eq!(response.result_arrow_ipc, b"NRX1-fixture");
    assert_eq!(response.packet_seq, 9);
    assert!(response.eos, "FetchResultResponse fixture eos");
}

#[test]
fn release_report_query_terminal_request_fixture_decodes() {
    let request: novarocks::ReportQueryTerminalRequest = decode_fixture(
        "ReportQueryTerminalRequest",
        REPORT_QUERY_TERMINAL_REQUEST_FIXTURE_HEX,
    );
    let snapshot = request
        .snapshot
        .as_ref()
        .expect("ReportQueryTerminalRequest fixture snapshot");
    assert_eq!(snapshot.version, 1);
    assert_eq!(
        snapshot
            .execution_id
            .as_ref()
            .and_then(|execution_id| execution_id.query_id.as_ref())
            .expect("ReportQueryTerminalRequest fixture query id")
            .hi,
        1
    );
    assert_eq!(
        snapshot
            .backend
            .as_ref()
            .expect("ReportQueryTerminalRequest fixture backend")
            .start_epoch,
        77
    );
    assert_eq!(snapshot.init_digest, vec![0x11; 32]);
    assert_eq!(snapshot.digest, vec![0x22; 32]);
    assert_eq!(snapshot.fragments.len(), 1);
    let fragment = &snapshot.fragments[0];
    assert_eq!(fragment.backend_num, 9);
    assert_eq!(
        fragment.outcome,
        novarocks::QueryTerminalFragmentOutcome::Succeeded as i32
    );
    assert_eq!(fragment.tablet_commit_infos[0].tablet_id, 100);
    assert_eq!(
        fragment
            .load_stats
            .as_ref()
            .expect("ReportQueryTerminalRequest fixture load stats")
            .loaded_bytes,
        90
    );
    assert_eq!(
        fragment
            .profile
            .as_ref()
            .and_then(|profile| profile.root.as_ref())
            .expect("ReportQueryTerminalRequest fixture profile")
            .info_strings
            .get("source")
            .map(String::as_str),
        Some("terminal-fixture")
    );
}

#[test]
fn release_lookup_fixtures_decode() {
    let request: filter::LookupRequest =
        decode_fixture("LookupRequest", LOOKUP_REQUEST_FIXTURE_HEX);
    assert_eq!(request.query_id.as_ref().expect("lookup query_id").hi, 1);
    assert_eq!(request.lookup_node_id, 33);
    assert_eq!(request.request_tuple_id, 44);
    assert_eq!(request.request_columns.len(), 1);
    assert_eq!(request.request_columns[0].slot_id, 55);
    assert_eq!(request.request_columns[0].data, vec![0, 1, 2, 3]);

    let response: filter::LookupResponse =
        decode_fixture("LookupResponse", LOOKUP_RESPONSE_FIXTURE_HEX);
    assert_eq!(response.status.as_ref().expect("lookup status").code, 0);
    assert_eq!(response.columns.len(), 1);
    assert_eq!(response.columns[0].data, vec![3, 2, 1, 0]);
}

#[test]
fn release_plan_and_expr_fixtures_decode() {
    let fragment: plan::PlanFragment = decode_fixture("PlanFragment", PLAN_FRAGMENT_FIXTURE_HEX);
    assert_eq!(fragment.fragment_id, 1);
    let root = fragment.root.expect("PlanFragment fixture root");
    assert_eq!(root.node_id, 10);
    assert!(
        matches!(
            root.payload.as_ref(),
            Some(plan::distributed_node::Payload::Physical(node))
                if matches!(node.kind, Some(plan::plan_node::Kind::HashJoin(_)))
        ),
        "PlanFragment fixture root must be HashJoin"
    );
    assert!(
        root.children.iter().any(|child| matches!(
            child.payload.as_ref(),
            Some(plan::distributed_node::Payload::Physical(node))
                if matches!(node.kind, Some(plan::plan_node::Kind::Scan(_)))
        )),
        "PlanFragment fixture must include a Scan child"
    );
    assert!(
        root.children.iter().any(|child| matches!(
            child.payload.as_ref(),
            Some(plan::distributed_node::Payload::Exchange(_))
        )),
        "PlanFragment fixture must include an Exchange child"
    );
    assert_eq!(root.children.len(), 2);

    let expression: expr::Expr = decode_fixture("Expr", EXPR_FIXTURE_HEX);
    let binary = match expression.kind.as_ref() {
        Some(expr::expr::Kind::BinaryOp(binary)) => binary,
        other => panic!("Expr fixture kind expected BinaryOp, got {other:?}"),
    };
    assert_eq!(
        binary.op,
        expr::BinaryOp::Gt as i32,
        "Expr fixture BinaryOp.op"
    );
    let left = binary.left.as_ref().expect("Expr fixture BinaryOp.left");
    match left.kind.as_ref() {
        Some(expr::expr::Kind::ColumnRef(column)) => {
            assert_eq!(
                column.column_id, 1,
                "Expr fixture BinaryOp.left ColumnRef.column_id"
            );
            assert_eq!(
                column.column.as_deref(),
                Some("l_orderkey"),
                "Expr fixture BinaryOp.left ColumnRef.column"
            );
        }
        other => panic!("Expr fixture BinaryOp.left expected ColumnRef, got {other:?}"),
    }
    let right = binary.right.as_ref().expect("Expr fixture BinaryOp.right");
    match right
        .kind
        .as_ref()
        .and_then(|kind| match kind {
            expr::expr::Kind::Literal(literal) => literal.value.as_ref(),
            _ => None,
        })
        .and_then(|value| value.value.as_ref())
    {
        Some(common::literal_value::Value::IntValue(value)) => {
            assert_eq!(*value, 10, "Expr fixture BinaryOp.right Literal.int_value")
        }
        other => panic!("Expr fixture BinaryOp.right expected Literal int 10, got {other:?}"),
    }
}
