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

use novarocks_proto::{common, novarocks, plan};

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

fn destination() -> novarocks::Destination {
    novarocks::Destination {
        finst_id: Some(id(3, 4)),
        endpoint: "10.0.0.8:8060".to_string(),
    }
}

fn query_options() -> novarocks::QueryOptions {
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
        (1..=29).collect::<Vec<_>>(),
        "QueryOptions must keep native runtime consumed fields on tags 1..=29"
    );
}

#[test]
fn runtime_endpoint_fields_use_native_endpoint_names_and_tags() {
    let destination_value = destination();
    let mut destination_fields = encoded_field_numbers(&destination_value);
    destination_fields.sort_unstable();
    assert_eq!(
        destination_fields,
        vec![1, 2],
        "Destination must keep finst_id=1 and endpoint=2"
    );

    let params = novarocks::InstanceParams {
        query_id: Some(id(1, 2)),
        fragment_instance_id: Some(id(3, 4)),
        backend_num: 1,
        per_node_scan_ranges: HashMap::new(),
        per_exch_num_senders: HashMap::new(),
        destinations: vec![destination()],
        query_options: Some(query_options()),
        typed_result_sink: true,
    };
    let params_fields = encoded_field_numbers(&params);
    assert!(
        !params_fields.contains(&7),
        "InstanceParams tag 7 is a permanent runtime_filter_params tombstone"
    );
    assert!(
        !params_fields.contains(&9),
        "InstanceParams tag 9 is a permanent native report field tombstone"
    );
}

#[test]
fn file_scan_range_survives_proto_roundtrip() {
    let decoded: novarocks::ScanRangeParams = roundtrip_message(&file_scan_range());
    assert_eq!(decoded, file_scan_range());
    let fields = encoded_field_numbers(&decoded);
    assert_eq!(fields, vec![1, 2, 3, 4]);
}

#[test]
fn instance_params_survives_proto_roundtrip() {
    let params = novarocks::InstanceParams {
        query_id: Some(id(1, 2)),
        fragment_instance_id: Some(id(3, 4)),
        backend_num: 9,
        per_node_scan_ranges: HashMap::from([(
            10,
            novarocks::ScanRangeList {
                ranges: vec![file_scan_range()],
            },
        )]),
        per_exch_num_senders: HashMap::from([(20, 3)]),
        destinations: vec![destination()],
        query_options: Some(query_options()),
        typed_result_sink: true,
    };

    let decoded: novarocks::InstanceParams = roundtrip_message(&params);
    assert_eq!(params, decoded);
}

#[test]
fn stage_fragment_request_carries_native_fields_only() {
    let request = novarocks::StageFragmentsRequest {
        execution_id: Some(novarocks::QueryExecutionId {
            query_id: Some(id(1, 2)),
            attempt_id: 1,
        }),
        init_digest: vec![1; 32],
        stage_digest_version: 1,
        stage_digest: vec![2; 32],
        fragments: vec![novarocks::StageFragment {
            plan: Some(plan::PlanFragment::default()),
            instance_params: Some(novarocks::InstanceParams {
                query_id: Some(id(1, 2)),
                fragment_instance_id: Some(id(3, 4)),
                backend_num: 1,
                per_node_scan_ranges: HashMap::new(),
                per_exch_num_senders: HashMap::new(),
                destinations: vec![destination()],
                query_options: Some(query_options()),
                typed_result_sink: true,
            }),
        }],
    };
    let fields = encoded_field_numbers(&request);

    assert!(
        fields.contains(&1),
        "execution_id must use StageFragmentsRequest tag 1"
    );
    assert!(
        fields.contains(&5),
        "fragments must use StageFragmentsRequest tag 5"
    );

    let decoded: novarocks::StageFragmentsRequest = roundtrip_message(&request);
    assert_eq!(request, decoded);
}
