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

use novarocks_proto_models::{common, novarocks, plan};

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn encoded_field_numbers<M: Message>(message: &M) -> Vec<u32> {
    let bytes = message.encode_to_vec();
    let mut fields = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let key = read_varint(&bytes, &mut offset);
        let field_number = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u8;
        fields.push(field_number);
        match wire_type {
            0 => {
                let _ = read_varint(&bytes, &mut offset);
            }
            1 => offset += 8,
            2 => {
                let len = read_varint(&bytes, &mut offset) as usize;
                offset += len;
            }
            5 => offset += 4,
            other => panic!("unsupported wire type {other} in encoded proto"),
        }
    }
    fields
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*offset)
            .unwrap_or_else(|| panic!("truncated varint at offset {}", *offset));
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift < 64, "varint overflow");
    }
}

fn id(hi: i64, lo: i64) -> common::UniqueId {
    common::UniqueId { hi, lo }
}

fn file_scan_range() -> novarocks::ScanRangeParams {
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
                            value_kind: 2,
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

fn destination() -> novarocks::TaskExchangeDestination {
    novarocks::TaskExchangeDestination {
        task: Some(novarocks::TaskIdentity {
            query_execution_id: Some(novarocks::QueryExecutionId {
                query_id: Some(id(1, 2)),
                attempt_id: 1,
            }),
            stage_id: 2,
            task_id: 1,
            backend_process_id: Some(novarocks::BackendProcessId {
                value: vec![1, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1].into(),
            }),
        }),
        fragment_instance_id: Some(id(3, 4)),
        endpoint: Some(novarocks::QueryControlEndpoint {
            host: "10.0.0.8".to_string(),
            port: 8060,
        }),
        destination_node_id: 20,
    }
}

fn topology() -> novarocks::TaskExchangeTopology {
    novarocks::TaskExchangeTopology {
        outbound: vec![novarocks::TaskExchangeEdge {
            edge_id: 1,
            destination_node_id: 20,
            partitioning: novarocks::ExchangePartitioning::Hash as i32,
            destinations: vec![destination()],
            sender_ordinal: 0,
            sender_count: 1,
        }],
        inbound: vec![],
    }
}

fn query_options() -> novarocks::QueryOptions {
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
        orc_use_column_names: true,
        enable_file_metacache: true,
        enable_file_pagecache: true,
        enable_parquet_reader_page_index: true,
    }
}

#[test]
fn query_options_use_pre_release_reset_tags() {
    let query_mem_limit_only = novarocks::QueryOptions {
        query_mem_limit: 512 << 20,
        ..Default::default()
    };
    let fields = encoded_field_numbers(&query_mem_limit_only);

    assert_eq!(fields, vec![5], "query_mem_limit must use reset tag 5");
}

#[test]
fn query_options_runtime_consumed_fields_use_native_tags() {
    let mut fields = encoded_field_numbers(&query_options());
    fields.sort_unstable();

    assert_eq!(
        fields,
        (1..=29).filter(|tag| *tag != 6).collect::<Vec<_>>(),
        "QueryOptions must keep native runtime consumed fields on tags 1..=29 except reserved 6"
    );
}

#[test]
fn task_topology_owns_runtime_endpoint_and_producer_sender_position() {
    let destination_value = destination();
    let mut destination_fields = encoded_field_numbers(&destination_value);
    destination_fields.sort_unstable();
    assert_eq!(
        destination_fields,
        vec![1, 2, 3, 4],
        "TaskExchangeDestination keeps the task fence, fragment instance, endpoint and node"
    );
    let endpoint = destination_value
        .endpoint
        .as_ref()
        .expect("destination endpoint");
    assert_eq!(endpoint.host, "10.0.0.8");
    assert_eq!(endpoint.port, 8060);
    assert_eq!(encoded_field_numbers(&topology()), vec![1]);

    // The producer's sender position is one fact of its edge.
    let mut edge = topology().outbound.remove(0);
    assert_eq!(encoded_field_numbers(&edge), vec![1, 2, 3, 4, 6]);
    edge.sender_ordinal = 1;
    edge.sender_count = 2;
    assert_eq!(encoded_field_numbers(&edge), vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn file_scan_range_survives_proto_roundtrip() {
    let decoded: novarocks::ScanRangeParams = roundtrip_message(&file_scan_range());
    assert_eq!(decoded, file_scan_range());
    let fields = encoded_field_numbers(&decoded);
    assert_eq!(fields, vec![1, 2, 3, 4]);
}

#[test]
fn create_task_carriers_separate_frozen_plan_from_task_assignment() {
    let fragment = novarocks::FrozenFragment {
        plan_version: vec![1; 16].into(),
        plan_contract_revision: 1,
        fragment_contract_version: 1,
        pipeline_dop_domain: Some(novarocks::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: false,
        }),
        plan: Some(plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                    dest_node_id: 20,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }),
    };
    let metadata = novarocks::CreationMetadata {
        query_context: None,
        descriptor: Some(novarocks::TaskDescriptor {
            topology: Some(topology()),
            ..Default::default()
        }),
        initial_domains: vec![],
        assignment: Some(novarocks::TaskAssignment {
            instance_ordinal: 1,
            initial_scan_ranges: vec![novarocks::TaskScanRanges {
                plan_node_id: 10,
                ranges: vec![file_scan_range()],
            }],
            sink_edge_ids: vec![1],
        }),
    };
    let request = novarocks::CreateTaskRequest {
        frozen_fragment: fragment.encode_to_vec().into(),
        creation_metadata: metadata.encode_to_vec().into(),
    };
    let fields = encoded_field_numbers(&request);

    assert_eq!(fields, vec![4, 5], "CreateTask must carry two byte fields");

    let decoded: novarocks::CreateTaskRequest = roundtrip_message(&request);
    let decoded_fragment = novarocks::FrozenFragment::decode(decoded.frozen_fragment.as_ref())
        .expect("decode frozen fragment");
    let decoded_metadata = novarocks::CreationMetadata::decode(decoded.creation_metadata.as_ref())
        .expect("decode creation metadata");
    assert_eq!(fragment, decoded_fragment);
    assert_eq!(metadata, decoded_metadata);
    assert_eq!(
        encoded_field_numbers(&decoded_fragment),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(encoded_field_numbers(&decoded_metadata), vec![2, 5]);
    let descriptor = decoded_metadata.descriptor.expect("task descriptor");
    let edge = &descriptor.topology.expect("task topology").outbound[0];
    let assignment = decoded_metadata.assignment.expect("task assignment");
    assert_eq!(edge.edge_id, assignment.sink_edge_ids[0]);
    assert_eq!(edge.destinations[0], destination());
    assert_eq!(
        assignment.initial_scan_ranges[0].ranges[0],
        file_scan_range()
    );
    assert_eq!(encoded_field_numbers(&assignment), vec![1, 2, 3]);
}
