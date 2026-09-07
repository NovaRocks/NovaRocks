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
use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
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
    crate::query_execution::preparation::PreparedFragmentSet,
    crate::query_execution::native_fragment::NativeFragmentAttachment,
) {
    let plan = native_preparation_plan(NativePreparationFixture::ResultOutput)
        .expect("sealed result execution fixture");
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
    let native_bundle =
        crate::query_execution::native_fragment::native_fragment_attachment_for_test(
            [novarocks_proto_models::plan::PlanFragment {
                fragment_id: 7,
                ..Default::default()
            }],
            &std::collections::BTreeSet::from([7]),
            None,
        )
        .expect("seal production execution artifact");
    (prepared, native_bundle)
}

#[test]
fn request_owns_prepared_and_native_artifacts() {
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        prepared,
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

    assert_eq!(
        request
            .artifacts()
            .scheduling_view()
            .fragment_ids()
            .collect::<Vec<_>>(),
        [7]
    );
    assert_eq!(
        request.options().native_submission_options().pipeline_dop(),
        3
    );
    let parts = request.into_parts();
    let cancellation = parts.cancellation;
    let completion = parts.completion;
    assert!(!cancellation.is_cancelled());
    assert_eq!(completion.intent(), DistributedQueryIntent::Result);
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
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        prepared,
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
    let (prepared, native_bundle) = real_execution_artifacts();
    let result = build_distributed_query_request_with_execution(
        prepared,
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
