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

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::common::backend_topology::BackendTopologySnapshot;
use crate::common::query_cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest,
    build_distributed_query_request_with_execution,
};
use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::statistics::{StatisticsExecutionMode, StatisticsExecutionPolicy};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_sql::test_support::{
    NativePreparationFixture, NativeWriteDataflowFixture, native_preparation_plan,
    native_write_dataflow_plan,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn test_execution(cancellation: QueryCancellationView) -> QueryExecutionContext {
    QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        BackendTopologySnapshot::empty(0),
        None,
        cancellation,
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
    )
}

fn real_execution_artifacts() -> (
    crate::query_execution::post_compile::NativeFragmentEncodingInput,
    crate::query_execution::native_fragment::NativeFragmentAttachment,
) {
    let plan = native_preparation_plan(NativePreparationFixture::ResultOutput)
        .expect("sealed result execution fixture");
    execution_artifacts_for_plan(plan)
}

fn execution_artifacts_for_plan(
    plan: novarocks_sql::plan_read::DistributedPlan,
) -> (
    crate::query_execution::post_compile::NativeFragmentEncodingInput,
    crate::query_execution::native_fragment::NativeFragmentAttachment,
) {
    let fragment_ids = plan
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<std::collections::BTreeSet<_>>();
    let registry = crate::connector::FixtureConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        None,
        None,
        crate::query_execution::preparation::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("prepare production execution artifact");
    let encoding = crate::query_execution::post_compile::NativeFragmentEncodingInput::new(prepared);
    let native_bundle = encoding
        .native_attachment_for_test(
            fragment_ids.iter().copied().map(|fragment_id| {
                novarocks_proto_models::plan::PlanFragment {
                    fragment_id,
                    ..Default::default()
                }
            }),
            &fragment_ids,
        )
        .expect("seal production execution artifact");
    (encoding, native_bundle)
}

#[test]
fn request_owns_prepared_and_native_artifacts() {
    let (encoding, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        Some(
            QueryOptions::parse(novarocks_proto_models::novarocks::QueryOptions {
                pipeline_dop: 3,
                ..Default::default()
            })
            .expect("valid protocol query options"),
        ),
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("valid production artifacts form an owned request");

    let read_execution = request
        .restartable_read()
        .expect("result fixture permits replacement before visibility");
    assert_eq!(
        read_execution
            .instantiate_artifacts_for_test()
            .scheduling_view()
            .fragment_ids()
            .collect::<Vec<_>>(),
        [7]
    );
    assert_eq!(
        request.options().native_submission_options().pipeline_dop(),
        3
    );
    let description = request.logical_execution().description();
    assert_eq!(
        description.kind(),
        novarocks_query_application::api::QueryExecutionKind::Read
    );
    assert_eq!(
        description.effect(),
        novarocks_query_application::coordination::ExecutionEffect::None
    );
    assert_eq!(
        description.recovery(),
        novarocks_query_application::coordination::RecoveryMode::RestartAttemptBeforeVisibility
    );
    assert_eq!(
        description.cost().root_rows().unknown_reason(),
        Some(novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected)
    );
    assert_eq!(
        description
            .resources()
            .minimum_memory_bytes()
            .unknown_reason(),
        Some(novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected)
    );
    let parts = request.into_parts();
    let cancellation = parts.cancellation;
    let completion = parts.completion;
    assert!(!cancellation.is_cancelled());
    assert_eq!(completion.intent(), DistributedQueryIntent::Result);
}

#[test]
fn replacement_read_attempt_reuses_one_logical_execution_with_fresh_attempt_typestate() {
    let cancellation = QueryCancellationSource::new();
    let first_execution = QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        BackendTopologySnapshot::empty(7),
        None,
        cancellation.view(),
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
    );
    let (encoding, native_bundle) = real_execution_artifacts();
    let first = build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        &first_execution,
    )
    .expect("first read attempt");
    let logical_execution = first
        .restartable_read()
        .expect("read request exposes its closed restart capability");

    let first_attempt = logical_execution.instantiate_artifacts_for_test();
    let handoff = first_attempt.runtime_filter_artifact_id();
    let first_bindings = first_attempt
        .runtime_filter_binding_view()
        .seal_empty()
        .expect("first attempt owns a writable native attachment");
    let _first_bound = first_attempt
        .attach_runtime_filter_bindings(first_bindings)
        .expect("first attempt consumes only its own attachment");

    let replacement_execution = QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        BackendTopologySnapshot::empty(8),
        None,
        cancellation.view(),
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
    );
    let replacement = logical_execution.instantiate_attempt(&replacement_execution);
    let replacement_logical_execution = replacement
        .restartable_read()
        .expect("replacement remains a restartable read");

    assert!(std::ptr::eq(
        first.logical_execution(),
        replacement.logical_execution()
    ));
    assert_eq!(first.topology().revision(), 7);
    assert_eq!(replacement.topology().revision(), 8);
    assert!(std::ptr::eq(
        first.logical_execution().description().plan(),
        replacement.logical_execution().description().plan()
    ));

    let replacement_attempt = replacement_logical_execution.instantiate_artifacts_for_test();
    assert_eq!(replacement_attempt.runtime_filter_artifact_id(), handoff);
    let replacement_bindings = replacement_attempt
        .runtime_filter_binding_view()
        .seal_empty()
        .expect("replacement owns an independent writable native attachment");
    let _replacement_bound = replacement_attempt
        .attach_runtime_filter_bindings(replacement_bindings)
        .expect("mutating the first attempt did not consume the replacement attachment");
}

#[test]
fn native_attachment_cannot_cross_assemblies() {
    let (first_encoding, _) = real_execution_artifacts();
    let (_, foreign_attachment) = real_execution_artifacts();
    let error = match build_distributed_query_request_with_execution(
        first_encoding,
        foreign_attachment,
        None,
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    ) {
        Ok(_) => panic!("a native attachment from another assembly must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert!(error.message().contains("does not match"));
}

#[test]
fn effectful_request_is_frozen_without_recovery() {
    let (encoding, native_bundle) = execution_artifacts_for_plan(
        native_write_dataflow_plan(NativeWriteDataflowFixture::SingleWriter)
            .expect("sealed write execution fixture"),
    );
    let request = build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        DistributedQueryIntent::Write,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("effectful request should freeze");
    let description = request.logical_execution().description();
    assert_eq!(
        description.kind(),
        novarocks_query_application::api::QueryExecutionKind::Write
    );
    assert_eq!(
        description.effect(),
        novarocks_query_application::coordination::ExecutionEffect::External
    );
    assert_eq!(
        description.recovery(),
        novarocks_query_application::coordination::RecoveryMode::NoRecovery
    );
    assert!(
        request.restartable_read().is_none(),
        "an effectful request must retain one move-only artifact owner"
    );
}

#[test]
fn cancellation_view_observes_injected_flag() {
    let cancelled = QueryCancellationSource::new();
    let view = cancelled.view();

    assert!(!view.is_cancelled());
    let _ = cancelled.request(QueryCancellationReason::ClientDisconnected);
    assert!(view.is_cancelled());
}

#[test]
fn outcome_factory_rejects_intent_mismatch() {
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Result).from_execution_result(
        crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_session: None,
            fragment_profiles: vec![
                crate::query_execution::profile::FragmentProfileTree::unattributed(
                    novarocks_execution::runtime::profile::Profiler::new("fragment-1")
                        .to_native_tree(),
                ),
            ],
        },
    );

    let Err(error) = result else {
        panic!("Result intent must reject a profile payload");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "Result outcome cannot contain write or profile payloads"
    );
}

#[test]
fn durable_statistics_attempt_ignores_statement_cancellation_and_is_bounded() {
    let policy = StatisticsExecutionPolicy::try_new(
        StatisticsExecutionMode::ProcessJobAttempt,
        std::time::Duration::from_secs(30 * 60),
    )
    .expect("maximum durable policy");
    assert!(!policy.mode().statement_cancellation_terminates_execution());
    assert_eq!(
        policy.attempt_timeout(),
        std::time::Duration::from_secs(30 * 60)
    );
    assert!(
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::ProcessJobAttempt,
            std::time::Duration::from_secs(30 * 60 + 1),
        )
        .is_ok(),
        "the shared LakePublicationRuntimePolicy, not this transport-neutral Core policy, owns the configured maximum"
    );
    assert!(
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::ProcessJobAttempt,
            std::time::Duration::ZERO,
        )
        .is_err()
    );
    assert!(StatisticsExecutionMode::SynchronousWait.statement_cancellation_terminates_execution());
}

#[test]
fn profile_outcome_preserves_fragment_profiles() {
    let profile = crate::query_execution::profile::FragmentProfileTree::for_fragment(
        7,
        novarocks_execution::runtime::profile::Profiler::new("fragment-7").to_native_tree(),
    );
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Profile)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "status",
                vec!["profiled".to_string()],
            )
            .expect("profile result"),
            write_session: None,
            fragment_profiles: vec![profile.clone()],
        })
        .expect("Profile intent accepts fragment profiles");

    let (result, profiles) = outcome
        .into_profile()
        .expect("profile outcome variant")
        .into_parts();
    assert_eq!(result.row_count(), 1);
    assert_eq!(profiles.into_profiles(), vec![profile]);
}

#[test]
fn result_outcome_preserves_query_result() {
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Result)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "value",
                vec!["kept".to_string()],
            )
            .expect("result payload"),
            write_session: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Result intent accepts a plain query result");

    assert_eq!(
        outcome
            .into_result()
            .expect("result outcome variant")
            .into_query_result()
            .row_count(),
        1
    );
}

struct RecordingCoordinator {
    calls: Arc<AtomicUsize>,
}

impl DistributedQueryCoordinator for RecordingCoordinator {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        request
            .into_parts()
            .completion
            .result(crate::runtime::query_result::QueryResult::empty())
    }
}

#[test]
fn query_execution_service_uses_explicitly_injected_coordinator() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = QueryExecutionService::new(Arc::new(RecordingCoordinator {
        calls: calls.clone(),
    }));
    let (encoding, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("build service request");

    let outcome = service
        .execute(request)
        .expect("injected coordinator result");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.intent(), DistributedQueryIntent::Result);
}

#[test]
fn generic_request_builder_rejects_statistics_without_a_typed_program() {
    let (encoding, native_bundle) = real_execution_artifacts();
    let result = build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        DistributedQueryIntent::Statistics,
        &test_execution(QueryCancellationSource::new().view()),
    );
    let Err(error) = result else {
        panic!("statistics must use the typed request builder");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert!(error.message().contains("StatisticsCollectionProgram"));
}

#[test]
fn request_builder_rejects_a_native_attachment_from_another_encoding_input() {
    let (encoding, _) = real_execution_artifacts();
    let (_, foreign_attachment) = real_execution_artifacts();

    let result = build_distributed_query_request_with_execution(
        encoding,
        foreign_attachment,
        None,
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    );
    let Err(error) = result else {
        panic!("a cross-plan native attachment must fail closed");
    };

    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert!(error.message().contains("sealed query encoding input"));
}
