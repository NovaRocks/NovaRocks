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

use novarocks_proto_models::{common, novarocks};

const FETCH_RESULT_RESPONSE_FIXTURE_HEX: &str =
    "0801120572656164791a0c4e5258312d6669787475726520092801";

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

fn release_query_options() -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: 4096,
        query_timeout: 300,
        enable_profile: true,
        pipeline_dop: 8,
        query_mem_limit: 512 << 20,
        runtime_filter_scan_wait_time_ms: Some(1500),
        runtime_filter_wait_timeout_ms: Some(3000),
        allow_throw_exception: true,
        group_concat_max_len: Some(65_536),
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

fn release_destination() -> novarocks::TaskExchangeDestination {
    novarocks::TaskExchangeDestination {
        task: Some(novarocks::TaskIdentity {
            query_execution_id: Some(release_query_execution_id()),
            stage_id: 2,
            task_id: 1,
            backend_process_id: Some(release_backend_process_id()),
        }),
        task_key: Some(id(3, 4)),
        endpoint: Some(novarocks::QueryControlEndpoint {
            host: "10.0.0.8".to_string(),
            port: 8060,
        }),
        destination_node_id: 20,
    }
}

/// The two CreateTask carriers keep the plan independent of placement while
/// preserving the release corpus for scan ranges and runtime endpoints.
fn release_frozen_fragment() -> novarocks::FrozenFragment {
    novarocks::FrozenFragment {
        package: b"opaque release package".to_vec().into(),
    }
}

fn release_query_execution_id() -> novarocks::QueryExecutionId {
    novarocks::QueryExecutionId {
        query_id: Some(id(1, 2)),
        attempt_id: 1,
    }
}

fn release_backend_process_id() -> novarocks::BackendProcessId {
    novarocks::BackendProcessId {
        value: vec![1, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1],
    }
}

fn release_creation_metadata() -> novarocks::CreationMetadata {
    novarocks::CreationMetadata {
        participation: Some(novarocks::ParticipationRef {
            query_execution_id: Some(release_query_execution_id()),
            frontend_process_id: Some(novarocks::FrontendProcessId {
                value: vec![1, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 2],
            }),
            backend_process_id: Some(release_backend_process_id()),
        }),
        descriptor: Some(novarocks::TaskDescriptor {
            identity: Some(novarocks::TaskIdentity {
                query_execution_id: Some(release_query_execution_id()),
                stage_id: 1,
                task_id: 1,
                backend_process_id: Some(release_backend_process_id()),
            }),
            task_key: Some(id(3, 4)),
            pipeline_dop: 8,
            split_plan_nodes: vec![11],
            topology: Some(novarocks::TaskExchangeTopology {
                outbound: vec![novarocks::TaskExchangeEdge {
                    edge_id: 1,
                    destination_node_id: 20,
                    partitioning: novarocks::ExchangePartitioning::Hash as i32,
                    destinations: vec![release_destination()],
                    sender_ordinal: 0,
                    sender_count: 1,
                }],
                inbound: vec![],
            }),
        }),
        initial_domains: vec![],
        assignment: Some(novarocks::TaskAssignment {
            instance_ordinal: 9,
            initial_scan_ranges: vec![novarocks::TaskScanRanges {
                plan_node_id: 11,
                ranges: vec![release_scan_range()],
            }],
            sink_edge_ids: vec![1],
        }),
    }
}

fn release_create_task_request() -> novarocks::CreateTaskRequest {
    novarocks::CreateTaskRequest {
        frozen_fragment: release_frozen_fragment().encode_to_vec().into(),
        creation_metadata: release_creation_metadata().encode_to_vec().into(),
    }
}

fn release_fetch_result_response() -> novarocks::FetchResultResponse {
    novarocks::FetchResultResponse {
        status: novarocks::fetch_result_response::Status::Ready as i32,
        message: "ready".to_string(),
        result_arrow_ipc: b"NRX1-fixture".to_vec().into(),
        packet_seq: 9,
        eos: true,
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
    print_fixture("FROZEN_FRAGMENT", &release_frozen_fragment());
    print_fixture("CREATION_METADATA", &release_creation_metadata());
    print_fixture("CREATE_TASK_REQUEST", &release_create_task_request());
    print_fixture("FETCH_RESULT_RESPONSE", &release_fetch_result_response());
}

#[test]
fn release_create_task_carrier_fixtures_decode() {
    let request = release_create_task_request();
    let request = novarocks::CreateTaskRequest::decode(request.encode_to_vec().as_slice())
        .expect("CreateTaskRequest fixture decodes");
    let fragment = novarocks::FrozenFragment::decode(request.frozen_fragment.as_ref())
        .expect("FrozenFragment fixture decodes");
    let metadata = novarocks::CreationMetadata::decode(request.creation_metadata.as_ref())
        .expect("CreationMetadata fixture decodes");
    assert_eq!(fragment.package.as_ref(), b"opaque release package");
    assert_eq!(
        metadata.participation.as_ref().unwrap().query_execution_id,
        Some(release_query_execution_id())
    );
    assert_eq!(
        metadata.descriptor.as_ref().unwrap().task_key,
        Some(id(3, 4))
    );
    let assignment = metadata
        .assignment
        .as_ref()
        .expect("CreationMetadata fixture assignment");
    assert_eq!(
        assignment.instance_ordinal, 9,
        "CreationMetadata fixture instance_ordinal"
    );
    let scan_ranges = assignment
        .initial_scan_ranges
        .iter()
        .find(|node| node.plan_node_id == 11)
        .expect("CreationMetadata fixture initial_scan_ranges[11]");
    assert_eq!(
        scan_ranges.ranges.len(),
        1,
        "CreationMetadata fixture initial_scan_ranges[11].ranges.len"
    );
    let scan_range = scan_ranges
        .ranges
        .first()
        .and_then(|params| params.range.as_ref())
        .expect("CreationMetadata fixture initial_scan_ranges[11].ranges[0].range");
    let file_range = match scan_range.kind.as_ref() {
        Some(novarocks::scan_range::Kind::File(file)) => file,
        other => panic!(
            "CreationMetadata fixture initial_scan_ranges[11].ranges[0].range.kind expected File, got {other:?}"
        ),
    };
    assert_eq!(
        file_range.file_format, "PARQUET",
        "CreationMetadata fixture FileScanRange.file_format"
    );
    assert_eq!(
        file_range.full_path.as_deref(),
        Some("s3://bucket/data.parquet"),
        "CreationMetadata fixture FileScanRange.full_path"
    );
    assert_eq!(
        file_range.delete_files.len(),
        1,
        "CreationMetadata fixture FileScanRange.delete_files.len"
    );
    let delete_file = &file_range.delete_files[0];
    assert_eq!(
        delete_file.full_path.as_deref(),
        Some("s3://bucket/delete.parquet"),
        "CreationMetadata fixture FileScanRange.delete_files[0].full_path"
    );
    assert_eq!(
        delete_file.file_content, "POSITION_DELETES",
        "CreationMetadata fixture FileScanRange.delete_files[0].file_content"
    );
    assert_eq!(
        delete_file.length,
        Some(64),
        "CreationMetadata fixture FileScanRange.delete_files[0].length"
    );
    assert_eq!(
        file_range.first_row_id,
        Some(1_000),
        "CreationMetadata fixture FileScanRange.first_row_id"
    );
    assert_eq!(
        file_range.data_sequence_number,
        Some(44),
        "CreationMetadata fixture FileScanRange.data_sequence_number"
    );
    assert_eq!(
        file_range
            .datacache_options
            .as_ref()
            .and_then(|options| options.priority),
        Some(3),
        "CreationMetadata fixture FileScanRange.datacache_options.priority"
    );
    assert_eq!(
        file_range.included_positions,
        vec![3, 5, 8],
        "CreationMetadata fixture FileScanRange.included_positions"
    );
    assert_eq!(
        file_range.serialized_split.as_deref(),
        Some("{\"split\":1}"),
        "CreationMetadata fixture FileScanRange.serialized_split"
    );
    assert_eq!(
        file_range.change_op,
        Some(-1),
        "CreationMetadata fixture FileScanRange.change_op"
    );
    let pruning = file_range
        .file_pruning_min_max_values
        .get(&1)
        .expect("CreationMetadata fixture FileScanRange.file_pruning_min_max_values[1]");
    assert_eq!(
        pruning.min_int_value,
        Some(10),
        "CreationMetadata fixture FileScanRange.file_pruning_min_max_values[1].min_int_value"
    );
    assert_eq!(
        pruning.max_int_value,
        Some(20),
        "CreationMetadata fixture FileScanRange.file_pruning_min_max_values[1].max_int_value"
    );

    assert_eq!(assignment.sink_edge_ids, vec![1]);
    let edge = metadata
        .descriptor
        .as_ref()
        .and_then(|descriptor| descriptor.topology.as_ref())
        .and_then(|topology| topology.outbound.first())
        .expect("CreationMetadata fixture outbound edge");
    assert_eq!(edge.edge_id, assignment.sink_edge_ids[0]);
    assert_eq!(edge.destination_node_id, 20);
    assert_eq!((edge.sender_ordinal, edge.sender_count), (0, 1));
    let destination = edge
        .destinations
        .first()
        .expect("CreationMetadata fixture destination");
    let endpoint = destination.endpoint.as_ref().expect("destination endpoint");
    assert_eq!(endpoint.host, "10.0.0.8");
    assert_eq!(endpoint.port, 8060);
    assert_eq!(destination.task_key, Some(id(3, 4)));
    assert_eq!(destination.task.as_ref().unwrap().stage_id, 2);
    assert!(
        scan_ranges
            .ranges
            .first()
            .and_then(|range| range.range.as_ref())
            .and_then(|range| range.kind.as_ref())
            .is_some_and(|kind| matches!(kind, novarocks::scan_range::Kind::File(_)))
    );
}

/// Query-wide options travel only in the establish that creates a context: a
/// create carries a task's parallelism in its descriptor and nothing else of
/// them. The release options corpus therefore round-trips through its one
/// remaining carrier.
#[test]
fn release_query_options_fixture_round_trips_through_establish() {
    let establish = novarocks::EstablishParticipationRequest {
        query_options: Some(release_query_options()),
        ..Default::default()
    };
    let decoded =
        novarocks::EstablishParticipationRequest::decode(establish.encode_to_vec().as_slice())
            .expect("EstablishParticipationRequest fixture decodes");
    assert_eq!(decoded.query_options, Some(release_query_options()));
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
    assert_eq!(response.result_arrow_ipc.as_ref(), b"NRX1-fixture");
    assert_eq!(response.packet_seq, 9);
    assert!(response.eos, "FetchResultResponse fixture eos");
}
