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

fn validate_completed_runtime_options(
    description: &novarocks_query_application::preparation::FrozenExecutionDescription,
    options: &super::ResolvedQueryOptions,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), super::DistributedQueryError> {
    description
        .validate_legacy_intrinsic_allow_throw_exception(
            options.runtime_options().allow_throw_exception,
            control,
        )
        .map_err(super::DistributedQueryError::from_compile)
}

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
fn synthetic_options_preserve_admitted_root_hints_and_absent_options_defaults() {
    use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings, statement_sql_semantics};

    for (session_mode, sql, expected_allow) in [
        ("32", "SELECT 1", false),
        ("ALLOW_THROW_EXCEPTION", "SELECT 1", true),
        (
            "32",
            "WITH SESSION sql_mode = 'ALLOW_THROW_EXCEPTION' SELECT 1",
            true,
        ),
        (
            "ALLOW_THROW_EXCEPTION",
            "WITH SESSION sql_mode = '32' SELECT 1",
            false,
        ),
    ] {
        let session =
            SqlSemanticSettings::default().with_sql_mode(SqlMode::from_assignment(session_mode));
        let statement = novarocks_parser::parser::parse_statement(sql).unwrap();
        let admitted_semantics = statement_sql_semantics(&session, &statement).unwrap();
        let cancellation = QueryCancellationSource::new();
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "default".to_owned(),
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
            admitted_semantics,
            Some(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: true,
                        variant_binary: true,
                    },
                ),
            ),
        ));
        let options = super::synthetic_statement_query_options(context.execution());
        assert_eq!(options.as_proto().allow_throw_exception, expected_allow);
        let reconstructed = reconstruct_runtime_query_options(&options);
        let mut expected = novarocks_execution::runtime::query_options::QueryOptions::default();
        expected.set_allow_throw_exception(expected_allow);
        assert_eq!(reconstructed, expected);

        let resolved = super::ResolvedQueryOptions::from_upstream(Some(options));
        let mut absent = super::ResolvedQueryOptions::from_upstream(None);
        absent.runtime.set_allow_throw_exception(expected_allow);
        assert_eq!(resolved.runtime_options(), absent.runtime_options());
        validate_completed_runtime_options(
            &intrinsic_runtime_description(expected_allow),
            &resolved,
            &runtime_gate_control(None),
        )
        .unwrap();
    }
}

fn intrinsic_runtime_description(
    allow: bool,
) -> novarocks_query_application::preparation::FrozenExecutionDescription {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        FragmentBuilder, FragmentId, FragmentSink, PipelineDopDomain, PlanBuilder, PlanVersionId,
        ValueOrigin, ValueType,
    };
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
        FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
    };
    use novarocks_type_contract::{
        DecimalOverflowPolicy, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
        SemanticParameterValue, SemanticParameters,
    };
    let mut fragment = FragmentBuilder::new(FragmentId::new(13));
    let node = fragment.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let policy = novarocks_functions::ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 4 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    };
    let source = novarocks_functions::ConstantValue::from_i64(
        std::sync::Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        4,
        policy,
        novarocks_type_contract::CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let pool = novarocks_physical_plan::ConstantPoolId::new(0);
    let reference = novarocks_physical_plan::ConstantReference {
        pool,
        ordinal: source.ordinal(),
    };
    use novarocks_expression_contract::{ExprKind, RootScope};
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, EvaluationDemand};
    let literal = fragment
        .unit(node)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Constant(reference))
        .unwrap();
    let cast = fragment
        .unit(node)
        .unwrap()
        .add(
            None,
            ty.clone(),
            ExprKind::Cast {
                array_failure_mode: None,
                expr: literal,
                target: DataType::Int64,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: SemanticParameterRef {
                    id: SemanticParameterId::new(u32::MAX),
                    expected_key: SemanticParameterKey::AllowThrowException,
                },
            },
        )
        .unwrap();
    let cast = fragment
        .unit(node)
        .unwrap()
        .root(cast, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let value = fragment
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    fragment
        .add_values(node, Box::from([Box::from([cast])]), Box::from([value]))
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let ports = fragment.node_ports(node).unwrap();
    let unit = fragment
        .take_unit_builder(node)
        .unwrap()
        .seal(ports, &mut work)
        .unwrap()
        .finish(Default::default(), &mut work)
        .unwrap();
    fragment
        .install_unit(node, std::sync::Arc::new(unit))
        .unwrap();
    work.finish().unwrap();
    let fragment = fragment
        .finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([29; 16]).unwrap(), policy)
        .with_semantic_parameters(
            SemanticParameters::try_new([(
                SemanticParameterId::new(u32::MAX),
                SemanticParameterValue::AllowThrowException(allow),
            )])
            .unwrap(),
        );
    builder
        .insert_constant_pool(pool, source.pool().clone())
        .unwrap();
    builder.add_fragment(fragment).unwrap();
    let candidate = CompletedPhysicalPlanCandidate::for_program(
        builder.finish_observed(&control).unwrap(),
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    FrozenExecutionDescription::for_completed_plan(
        novarocks_query_application::api::QueryExecutionKind::Write,
        candidate,
        Vec::new(),
        OutputContract::CompletionOnly,
        novarocks_coordination_contract::ExecutionEffect::None,
        novarocks_coordination_contract::RecoveryMode::NoRecovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
}

struct RuntimeGateControl {
    calls: std::sync::Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
    refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
}
impl novarocks_type_contract::PureCompileControl for RuntimeGateControl {
    fn checkpoint(
        &self,
        phase: novarocks_type_contract::CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((phase, units));
        match self.refuse.clone() {
            Some((ordinal, cause)) if calls.len() == ordinal + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}
fn runtime_gate_control(
    refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
) -> RuntimeGateControl {
    RuntimeGateControl {
        calls: std::sync::Mutex::new(Vec::new()),
        refuse,
    }
}

#[test]
fn native_runtime_gate_compares_actual_resolved_options_with_the_same_candidate() {
    for actual in [false, true] {
        let options = super::ResolvedQueryOptions::from_upstream(Some(QueryOptions::from_proto(
            novarocks::QueryOptions {
                allow_throw_exception: actual,
                ..Default::default()
            },
        )));
        let description = intrinsic_runtime_description(actual);
        validate_completed_runtime_options(&description, &options, &runtime_gate_control(None))
            .unwrap();
        let error = validate_completed_runtime_options(
            &intrinsic_runtime_description(!actual),
            &options,
            &runtime_gate_control(None),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            super::DistributedQueryErrorKind::ContractViolation
        );
        assert_eq!(error.compile_control_error(), None);
    }
    let default_options = super::ResolvedQueryOptions::from_upstream(None);
    assert!(
        validate_completed_runtime_options(
            &intrinsic_runtime_description(true),
            &default_options,
            &runtime_gate_control(None)
        )
        .is_err()
    );
}

#[test]
fn native_runtime_gate_preserves_all_typed_control_prefixes_in_the_terminal_owner() {
    use novarocks_type_contract::CompileControlError;
    let description = intrinsic_runtime_description(true);
    let options = super::ResolvedQueryOptions::from_upstream(Some(QueryOptions::from_proto(
        novarocks::QueryOptions {
            allow_throw_exception: true,
            ..Default::default()
        },
    )));
    for success in [true, false] {
        let description = if success {
            description.clone()
        } else {
            intrinsic_runtime_description(false)
        };
        let trace = runtime_gate_control(None);
        let result = validate_completed_runtime_options(&description, &options, &trace);
        assert_eq!(result.is_ok(), success);
        let callbacks = trace.calls.into_inner().unwrap();
        assert_eq!(callbacks.len(), 2);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for ordinal in 0..callbacks.len() {
                let trace = runtime_gate_control(Some((ordinal, cause.clone())));
                let error =
                    validate_completed_runtime_options(&description, &options, &trace).unwrap_err();
                assert_eq!(error.compile_control_error(), Some(cause.clone()));
                assert_eq!(*trace.calls.lock().unwrap(), callbacks[..=ordinal]);
            }
        }
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
        allow_throw_exception: true,
        group_concat_max_len: Some(65_535),
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
    assert!(runtime.allow_throw_exception);
    assert_eq!(runtime.group_concat_max_len, Some(65_535));
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
