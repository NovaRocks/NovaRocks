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

use super::{completed_plan_dop_domain, reconstruct_runtime_query_options};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_proto_models::novarocks;

#[test]
fn typed_task_and_root_fetch_failures_preserve_engine_identity_without_text_inference() {
    use novarocks_failure_contract::FailureProjection;
    let backend = novarocks_types::BackendProcessId::new_v7();
    let query = novarocks_types::QueryExecutionId::new(
        novarocks_types::QueryId::new(31, 32),
        novarocks_types::AttemptId::new(1).unwrap(),
    )
    .unwrap();
    let identity = novarocks_execution_contract::TaskIdentity::new(
        query,
        novarocks_types::StageId::new(1).unwrap(),
        novarocks_types::TaskId::new(1).unwrap(),
        backend,
    );
    for original in [
        novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "FULL OUTER hash join".into(),
        ),
        novarocks_plan_codec::PhysicalEncodeError::Invalid("FULL OUTER hash join".into()),
    ] {
        let fact = original.failure_fact();
        let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
            identity,
            novarocks_execution_contract::TerminationDetail::Failed(
                novarocks_execution_contract::TaskFailure::new_in_phase(
                    fact,
                    novarocks_execution_contract::SafeDetail::truncating("FULL OUTER hash join"),
                    novarocks_execution_contract::TaskFailurePhase::Preparation,
                ),
            ),
        );
        let input = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap();
        let error = super::DistributedQueryError::from_execution(input.clone());
        assert_eq!(error.clone().into_execution_failure_input(), input);
        let observed =
            novarocks_query_application::coordination::RootResultFetchFailure::from_input(
                input.clone(),
            );
        let error = super::DistributedQueryError::from_execution(observed.input().clone());
        assert_eq!(error.into_execution_failure_input(), input);
    }
}

#[test]
fn completion_missing_pure_implementation_preserves_only_its_sql_descriptor() {
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};
    use novarocks_type_contract::CompileControlError;

    let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
        FunctionOverloadId::try_new("fixture/completion/missing-pure").unwrap(),
    ));
    let SqlCompileError::Analyze(ref analyze) = compile else {
        panic!("missing pure implementation must retain SQL analysis identity");
    };
    let expected = analyze.to_user_error(None).to_string();
    let failure = super::DistributedQueryError::from_compile(compile);
    assert_eq!(failure.to_string(), expected);
    assert!(
        failure
            .to_string()
            .starts_with("[sql.analyze.unavailable_implementation] ")
    );
    assert_eq!(failure.compile_control_error(), None);

    let ordinary = super::DistributedQueryError::from_compile(SqlCompileError::Analyze(
        AnalyzeError::internal("unsupported diagnostic"),
    ));
    assert_eq!(ordinary.to_string(), "unsupported diagnostic");
    assert_eq!(ordinary.compile_control_error(), None);
    for control in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let failure = super::DistributedQueryError::from_compile(SqlCompileError::Analyze(
            AnalyzeError::control(control.clone()),
        ));
        assert_eq!(failure.compile_control_error(), Some(control));
    }
}

#[test]
fn encode_capability_refusal_is_not_a_contract_violation() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    for original in [
        novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("CTE multicast".into()),
        novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
    ] {
        let expected = original.failure_fact();
        let error = super::DistributedQueryError::from_encode(original);
        assert_eq!(error.execution_failure().unwrap().fact(), expected);
        assert_eq!(
            error.clone().into_execution_failure_input().fact(),
            expected
        );
        assert!(
            error
                .domain_source
                .as_ref()
                .unwrap()
                .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                .is_some()
        );
        assert_eq!(
            expected.class(),
            match error
                .domain_source
                .as_ref()
                .unwrap()
                .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                .unwrap()
            {
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(_) =>
                    FailureClass::Unsupported,
                novarocks_plan_codec::PhysicalEncodeError::Invalid(_) => FailureClass::Invariant,
                other => panic!("unexpected author: {other}"),
            }
        );
    }
}

#[test]
fn completed_plan_dop_domain_uses_resolved_driver_width_not_backend_count() {
    let explicit = QueryOptions::from_proto(novarocks::QueryOptions {
        pipeline_dop: 5,
        ..Default::default()
    });
    assert_eq!(completed_plan_dop_domain(Some(&explicit)).unwrap().max, 5);
    assert_eq!(
        completed_plan_dop_domain(None).unwrap().max as i32,
        novarocks_execution::runtime::exec_env::calc_pipeline_dop(0)
    );
}

#[test]
fn reconstructed_runtime_options_preserve_protocol_scalars() {
    let protocol = QueryOptions::from_proto(novarocks::QueryOptions {
        query_timeout: 60,
        query_delivery_timeout: 30,
        enable_profile: true,
        runtime_profile_report_interval: 7,
        pipeline_dop: 8,
        query_mem_limit: 1 << 20,
        runtime_filter_scan_wait_time_ms: Some(250),
        runtime_filter_wait_timeout_ms: Some(5000),

        enable_join_runtime_bitset_filter: Some(false),
        global_runtime_filter_build_max_size: 1 << 19,
        orc_use_column_names: true,
        enable_file_metacache: true,
        enable_file_pagecache: true,
        enable_parquet_reader_page_index: true,
        enable_scan_datacache: true,
        enable_populate_datacache: true,
        enable_datacache_async_populate_mode: true,
        enable_datacache_io_adaptor: true,
        enable_cache_select: true,
        datacache_evict_probability: Some(75),
        datacache_priority: 2,
        datacache_ttl_seconds: 3600,
        datacache_sharing_work_period: 10,
    });

    let runtime = reconstruct_runtime_query_options(&protocol);

    assert_eq!(runtime.query_timeout, Some(60));
    assert_eq!(runtime.query_delivery_timeout, Some(30));
    assert!(runtime.enable_profile);
    assert_eq!(runtime.runtime_profile_report_interval, Some(7));
    assert_eq!(runtime.pipeline_dop, Some(8));
    assert_eq!(runtime.exec_mem_limit, Some(1 << 20));
    assert_eq!(runtime.runtime_filter_scan_wait_time_ms, Some(250));
    assert_eq!(runtime.runtime_filter_wait_timeout_ms, Some(5000));

    assert_eq!(runtime.enable_join_runtime_bitset_filter, Some(false));
    assert_eq!(runtime.global_runtime_filter_build_max_size, Some(1 << 19));
    assert!(runtime.orc_use_column_names);
    assert!(runtime.enable_file_metacache);
    assert!(runtime.enable_file_pagecache);
    assert!(runtime.enable_parquet_reader_page_index);
    assert_eq!(runtime.cache.datacache_evict_probability, Some(75));
    assert_eq!(runtime.cache.datacache_priority, Some(2));
    assert_eq!(runtime.cache.datacache_ttl_seconds, Some(3600));
    assert_eq!(runtime.cache.datacache_sharing_work_period, Some(10));
}
