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

/// Query-wide options travel once, in the establish that creates a context;
/// a task carries only its own width. The projection must keep every option
/// the native runtime consumes.
#[test]
fn query_options_encoder_maps_every_query_wide_option() {
    let query_options = novarocks_execution::runtime::query_options::QueryOptions {
        batch_size: Some(4096),
        query_timeout: Some(60),
        query_delivery_timeout: Some(30),
        enable_profile: true,
        runtime_profile_report_interval: Some(7),
        pipeline_dop: Some(8),
        exec_mem_limit: Some(1 << 20),
        runtime_filter_scan_wait_time_ms: Some(250),
        runtime_filter_wait_timeout_ms: Some(5_000),
        allow_throw_exception: true,
        group_concat_max_len: Some(65_535),
        enable_join_runtime_bitset_filter: Some(false),
        global_runtime_filter_build_max_size: Some(1 << 19),
        cache: novarocks_execution::runtime::query_options::QueryCacheOptions {
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
    let opts = instance::encode_query_options(&query_options);
    novarocks_proto_codec::lifecycle::QueryOptions::parse(opts)
        .expect("frontend query-options projection satisfies the Protocol contract");
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
fn fragment_pipeline_dop_obeys_the_frozen_domain_without_changing_query_options() {
    use novarocks_physical_plan::PipelineDopDomain;

    let single_reader = PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    };
    assert_eq!(
        instance::select_fragment_pipeline_dop(single_reader, 8).expect("single reader"),
        1
    );
    let wider_fragment = PipelineDopDomain {
        min: 1,
        max: 6,
        requires_power_of_two: false,
    };
    assert_eq!(
        instance::select_fragment_pipeline_dop(wider_fragment, 8).expect("bounded width"),
        6
    );
    let power_of_two = PipelineDopDomain {
        requires_power_of_two: true,
        ..wider_fragment
    };
    assert_eq!(
        instance::select_fragment_pipeline_dop(power_of_two, 8).expect("power-of-two width"),
        4
    );
    assert!(instance::select_fragment_pipeline_dop(single_reader, 0).is_err());
}
