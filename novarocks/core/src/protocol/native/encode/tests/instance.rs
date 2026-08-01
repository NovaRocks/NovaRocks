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

use super::super::instance;

#[test]
fn instance_params_encoder_maps_scan_ranges_destinations_rf_and_query_options() {
    use std::collections::BTreeMap;

    let mut scan_range = crate::runtime::scan_range::ScanRangeParams::file(
        crate::runtime::scan_range::FileScanRange {
            file_format: crate::runtime::scan_range::FileFormat::Parquet,
            full_path: Some("s3://bucket/data.parquet".to_string()),
            relative_path: Some("data.parquet".to_string()),
            table_id: Some(99),
            offset: 8,
            length: 16,
            file_length: 128,
            delete_files: Vec::new(),
            deletion_vector_descriptor: None,
            first_row_id: Some(1_000),
            data_sequence_number: Some(44),
            modification_time: None,
            datacache_options: None,
            candidate_node: None,
            included_positions: vec![3, 5, 8],
            serialized_split: Some("{\"split\":1}".to_string()),
            use_iceberg_jni_metadata_reader: true,
            ivm_change_op: Some(crate::exec::change_op::CHANGE_OP_DELETE),
            file_pruning_min_max_values: Some(BTreeMap::from([(
                0,
                crate::runtime::scan_range::FilePruningMinMaxValue {
                    value_kind: crate::runtime::scan_range::FilePruningValueKind::Int,
                    has_null: false,
                    all_null: false,
                    min_int_value: Some(10),
                    max_int_value: Some(20),
                    min_float_value: None,
                    max_float_value: None,
                },
            )])),
        },
    );
    scan_range.volume_id = Some(13);
    scan_range.empty = Some(true);
    scan_range.has_more = Some(false);
    let mut scan_ranges = BTreeMap::new();
    scan_ranges.insert(11, vec![scan_range]);
    let destination = crate::runtime::endpoint::FragmentDestination::new(
        novarocks_types::UniqueId::new(3, 4),
        crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.9", 8060)
            .expect("destination endpoint"),
    );
    let mut per_exch_num_senders = BTreeMap::new();
    per_exch_num_senders.insert(42, 2);
    let placement = crate::query_execution::schedule::FragmentInstancePlacement {
        fragment_id: 0,
        instance_index: 5,
        finst_id: novarocks_types::UniqueId::new(1, 2),
        backend_idx: 7,
        endpoint: crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.7", 8060)
            .expect("placement endpoint"),
        scan_ranges,
        connector_splits: BTreeMap::new(),
        destinations: vec![destination],
        per_exch_num_senders,
    };
    let query_options = crate::runtime::query_options::QueryOptions {
        batch_size: Some(4096),
        query_timeout: Some(60),
        query_delivery_timeout: Some(30),
        enable_profile: true,
        runtime_profile_report_interval: Some(7),
        pipeline_dop: Some(8),
        exec_mem_limit: Some(1 << 20),
        connector_io_tasks_per_scan_operator: Some(12),
        runtime_filter_scan_wait_time_ms: Some(250),
        runtime_filter_wait_timeout_ms: Some(5_000),
        allow_throw_exception: true,
        group_concat_max_len: Some(65_535),
        enable_join_runtime_bitset_filter: Some(false),
        global_runtime_filter_build_max_size: Some(1 << 19),
        cache: crate::runtime::query_options::QueryCacheOptions {
            enable_scan_datacache: true,
            enable_populate_datacache: true,
            enable_datacache_async_populate_mode: true,
            enable_datacache_io_adaptor: true,
            enable_cache_select: true,
            datacache_evict_probability: Some(75),
            datacache_priority: Some(2),
            datacache_ttl_seconds: Some(3600),
            datacache_sharing_work_period: Some(10),
        },
        ..Default::default()
    };
    let encoded = instance::encode_instance_params(
        &novarocks_types::UniqueId::new(100, 200),
        &placement,
        &query_options,
        5,
        true,
    )
    .expect("encode instance params");

    assert_eq!(encoded.query_id.as_ref().expect("query id").hi, 100);
    assert_eq!(
        encoded
            .fragment_instance_id
            .as_ref()
            .expect("fragment instance id")
            .lo,
        2
    );
    assert_eq!(encoded.backend_num, 5);
    assert_eq!(encoded.per_exch_num_senders.get(&42), Some(&2));
    assert_eq!(encoded.destinations[0].endpoint, "10.0.0.9:8060");
    assert!(encoded.typed_result_sink);
    let encoded_range = &encoded.per_node_scan_ranges[&11].ranges[0];
    assert_eq!(encoded_range.volume_id, Some(13));
    assert_eq!(encoded_range.empty, Some(true));
    assert_eq!(encoded_range.has_more, Some(false));
    let crate::proto::novarocks::scan_range::Kind::File(file) = encoded_range
        .range
        .as_ref()
        .and_then(|range| range.kind.as_ref())
        .expect("scan range kind")
    else {
        panic!("expected native file scan range");
    };
    assert_eq!(file.file_format, "PARQUET");
    assert_eq!(file.full_path.as_deref(), Some("s3://bucket/data.parquet"));
    assert_eq!(file.included_positions, vec![3, 5, 8]);
    assert!(file.use_iceberg_jni_metadata_reader);
    assert_eq!(
        file.change_op,
        Some(i32::from(crate::exec::change_op::CHANGE_OP_DELETE))
    );
    let pruning = file
        .file_pruning_min_max_values
        .get(&0)
        .expect("file pruning stats");
    assert_eq!(pruning.value_kind, 2);
    assert_eq!(pruning.min_int_value, Some(10));
    assert_eq!(pruning.max_int_value, Some(20));
    let opts = encoded.query_options.as_ref().expect("query options");
    assert_eq!(opts.batch_size, 4096);
    assert_eq!(opts.query_timeout, 60);
    assert_eq!(opts.query_delivery_timeout, 30);
    assert_eq!(opts.runtime_profile_report_interval, 7);
    assert_eq!(opts.pipeline_dop, 8);
    assert_eq!(opts.query_mem_limit, 1 << 20);
    assert_eq!(opts.runtime_filter_wait_timeout_ms, Some(5_000));
    assert!(opts.enable_scan_datacache);
    assert_eq!(opts.datacache_evict_probability, Some(75));
    assert_eq!(opts.datacache_sharing_work_period, 10);
    assert_eq!(opts.enable_join_runtime_bitset_filter, Some(false));
    assert_eq!(opts.global_runtime_filter_build_max_size, 1 << 19);
}

#[test]
fn instance_params_encoder_maps_starrocks_tablet_range() {
    use std::collections::BTreeMap;

    let placement = crate::query_execution::schedule::FragmentInstancePlacement {
        fragment_id: 0,
        instance_index: 0,
        finst_id: novarocks_types::UniqueId::new(1, 2),
        backend_idx: 0,
        endpoint: crate::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 8060)
            .expect("endpoint"),
        scan_ranges: BTreeMap::from([(
            11,
            vec![
                crate::runtime::scan_range::ScanRangeParams::starrocks_tablet(300, 100, 7)
                    .expect("StarRocks tablet range"),
            ],
        )]),
        connector_splits: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    };

    let query_options = crate::runtime::query_options::QueryOptions {
        pipeline_dop: Some(1),
        ..Default::default()
    };
    let encoded = instance::encode_instance_params(
        &novarocks_types::UniqueId::new(100, 200),
        &placement,
        &query_options,
        0,
        false,
    )
    .expect("encode instance params");
    let range = encoded.per_node_scan_ranges[&11].ranges[0]
        .range
        .as_ref()
        .and_then(|range| range.kind.as_ref())
        .expect("range kind");
    let crate::proto::novarocks::scan_range::Kind::StarrocksTablet(range) = range else {
        panic!("expected StarRocks tablet range, got {range:?}");
    };
    assert_eq!(
        (range.tablet_id, range.partition_id, range.version),
        (300, 100, 7)
    );
}
