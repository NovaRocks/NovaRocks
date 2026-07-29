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

use crate::query_execution::artifact::{
    BackendPlacement, FragmentScheduleDraft, NativeSubmissionContext, RuntimeFilterDeploymentEpoch,
    RuntimeFilterDeploymentOptions, RuntimeFilterInstallBarrier, RuntimeFilterInstallLease,
    RuntimeFilterInstallLeaseGuard, ValidatedFragmentSchedule,
};
use crate::query_execution::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest,
    build_distributed_query_request,
};
use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput};
use crate::runtime::query_options::QueryOptions;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
};
use crate::sql::planner::payload::PlanValuesNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct RecordingRuntimeFilterLease {
    releases: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
    armed: bool,
}

impl RuntimeFilterInstallLeaseGuard for RecordingRuntimeFilterLease {
    fn release(mut self: Box<Self>) {
        self.armed = false;
        self.releases.fetch_add(1, Ordering::SeqCst);
    }

    fn abort_preserving(mut self: Box<Self>, primary_error: String) -> String {
        self.armed = false;
        self.aborts.fetch_add(1, Ordering::SeqCst);
        format!("{primary_error}; test rollback completed")
    }
}

impl Drop for RecordingRuntimeFilterLease {
    fn drop(&mut self) {
        if self.armed {
            self.aborts.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct RecordingRuntimeFilterBarrier {
    calls: Arc<AtomicUsize>,
    participants: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
}

impl RuntimeFilterInstallBarrier for RecordingRuntimeFilterBarrier {
    fn install_all(
        &self,
        plan: crate::query_execution::artifact::RuntimeFilterInstallPlan,
    ) -> Result<RuntimeFilterInstallLease, DistributedQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.participants
            .store(plan.participant_count(), Ordering::SeqCst);
        Ok(RuntimeFilterInstallLease::new(Box::new(
            RecordingRuntimeFilterLease {
                releases: self.releases.clone(),
                aborts: self.aborts.clone(),
                armed: true,
            },
        )))
    }
}

fn real_execution_artifacts() -> (
    crate::query_execution::preparation::PreparedFragmentSet,
    crate::protocol::native::encode::NativeFragmentBundle,
) {
    let fragment = PlanFragment {
        fragment_id: 7,
        root: DistributedNode {
            node_id: 70,
            fragment_id: 7,
            tuple_ids: vec![70],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![fragment],
        root_fragment_id: 7,
        edges: Vec::new(),
        runtime_filter_graph: Default::default(),
    };
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &plan,
        &crate::connector::ConnectorRegistry::new(),
        None,
    )
    .expect("prepare production execution artifact");
    let native_bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&plan, &prepared)
            .expect("encode production execution artifact");
    (prepared, native_bundle)
}

#[test]
fn request_owns_prepared_and_native_artifacts() {
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(3),
            ..Default::default()
        }),
        DistributedQueryIntent::Result,
        QueryCancellationView::never_cancelled(),
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
fn empty_runtime_filter_graph_requires_explicit_barrier_before_assembly() {
    let calls = Arc::new(AtomicUsize::new(0));
    let participants = Arc::new(AtomicUsize::new(usize::MAX));
    let releases = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let barrier = RecordingRuntimeFilterBarrier {
        calls: calls.clone(),
        participants: participants.clone(),
        releases: releases.clone(),
        aborts: aborts.clone(),
    };
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        QueryCancellationView::never_cancelled(),
    )
    .expect("build request");
    let parts = request.into_parts();
    let query_id = crate::query_execution::contract::QueryId::new(41, 73);
    let mut draft = FragmentScheduleDraft::new();
    let endpoint = "127.0.0.1:19031".parse().unwrap();
    draft
        .assign_fragment(7, vec![BackendPlacement::new(3, endpoint)])
        .unwrap();
    let schedule =
        ValidatedFragmentSchedule::validate(parts.artifacts.scheduling_view(), query_id, draft)
            .unwrap();
    let scheduled = parts.artifacts.bind_schedule(schedule).unwrap();
    let deployment_options = RuntimeFilterDeploymentOptions::new(
        RuntimeFilterDeploymentEpoch::new(9).unwrap(),
        vec![(3, endpoint)],
        2,
        parts.options.runtime_filter_lifecycle(),
    )
    .unwrap();

    let ready = scheduled
        .prepare_runtime_filters(deployment_options, &barrier)
        .expect("empty graph still crosses the explicit barrier");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(participants.load(Ordering::SeqCst), 0);

    let execution = ready
        .assemble(NativeSubmissionContext::new(
            query_id,
            &parts.options,
            crate::query_execution::backend::CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().unwrap(),
            ),
            false,
        ))
        .expect("barrier-ready query assembles");
    let execution = execution.into_parts();
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 0);
    execution.runtime_filter_lease.release();
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn assembly_failure_aborts_the_lease_and_preserves_rollback_context() {
    let calls = Arc::new(AtomicUsize::new(0));
    let participants = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let barrier = RecordingRuntimeFilterBarrier {
        calls,
        participants,
        releases: releases.clone(),
        aborts: aborts.clone(),
    };
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        QueryCancellationView::never_cancelled(),
    )
    .expect("build request");
    let parts = request.into_parts();
    let scheduled_query_id = crate::query_execution::contract::QueryId::new(41, 73);
    let endpoint = "127.0.0.1:19031".parse().unwrap();
    let mut draft = FragmentScheduleDraft::new();
    draft
        .assign_fragment(7, vec![BackendPlacement::new(3, endpoint)])
        .unwrap();
    let schedule = ValidatedFragmentSchedule::validate(
        parts.artifacts.scheduling_view(),
        scheduled_query_id,
        draft,
    )
    .unwrap();
    let ready = parts
        .artifacts
        .bind_schedule(schedule)
        .unwrap()
        .prepare_runtime_filters(
            RuntimeFilterDeploymentOptions::new(
                RuntimeFilterDeploymentEpoch::new(9).unwrap(),
                vec![(3, endpoint)],
                2,
                parts.options.runtime_filter_lifecycle(),
            )
            .unwrap(),
            &barrier,
        )
        .unwrap();

    let error = match ready.assemble(NativeSubmissionContext::new(
        crate::query_execution::contract::QueryId::new(41, 74),
        &parts.options,
        crate::query_execution::backend::CoordinatorReportEndpoint::from_socket_addr(
            "127.0.0.1:19030".parse().unwrap(),
        ),
        false,
    )) {
        Ok(_) => panic!("query-id drift must fail assembly"),
        Err(error) => error,
    };

    assert_eq!(releases.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    assert_eq!(
        error.message(),
        "native submission context query id does not match validated schedule; test rollback completed"
    );
}

#[test]
#[cfg(feature = "query-execution-contract-test-support")]
fn nonempty_runtime_filter_graph_is_compiled_before_the_install_barrier() {
    let calls = Arc::new(AtomicUsize::new(0));
    let participants = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let barrier = RecordingRuntimeFilterBarrier {
        calls: calls.clone(),
        participants: participants.clone(),
        releases: releases.clone(),
        aborts: aborts.clone(),
    };
    let fixture =
        crate::query_execution::contract_test_support::non_empty_runtime_filter_contract_fixture();
    let live_backends = fixture.backends().to_vec();
    let request = fixture.into_request();
    let parts = request.into_parts();
    let query_id = crate::query_execution::contract::QueryId::new(41, 73);
    let mut draft = FragmentScheduleDraft::new();
    draft
        .assign_fragment(
            11,
            live_backends
                .iter()
                .map(|(backend_idx, endpoint)| BackendPlacement::new(*backend_idx, *endpoint))
                .collect(),
        )
        .unwrap();
    draft
        .assign_fragment(
            19,
            vec![BackendPlacement::new(
                live_backends[0].0,
                live_backends[0].1,
            )],
        )
        .unwrap();
    let schedule =
        ValidatedFragmentSchedule::validate(parts.artifacts.scheduling_view(), query_id, draft)
            .unwrap();
    let scheduled = parts.artifacts.bind_schedule(schedule).unwrap();
    let options = RuntimeFilterDeploymentOptions::new(
        RuntimeFilterDeploymentEpoch::new(17).unwrap(),
        live_backends,
        2,
        parts.options.runtime_filter_lifecycle(),
    )
    .unwrap();

    let ready = scheduled
        .prepare_runtime_filters(options, &barrier)
        .expect("compiled participant plan crosses the barrier");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(participants.load(Ordering::SeqCst), 2);
    drop(ready);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
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
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Result).write(
        crate::runtime::query_result::QueryResult::empty(),
        None,
        None,
    );

    let Err(error) = result else {
        panic!("Result intent must reject a Write outcome");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "distributed query outcome intent mismatch: expected Result, received Write"
    );
}

#[test]
fn write_outcome_preserves_commit_or_abort() {
    let write_id = crate::common::types::UniqueId { hi: 41, lo: 73 };
    let commit = WriteCommitInput {
        write_id,
        writers: Vec::new(),
    };
    let commit_outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Write)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "status",
                vec!["committed".to_string()],
            )
            .expect("commit result"),
            write_commit: Some(commit.clone()),
            write_abort: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Write intent accepts a commit payload");
    let (result, actual_commit, actual_abort) = commit_outcome
        .into_write()
        .expect("write outcome variant")
        .into_parts();
    assert_eq!(result.row_count(), 1);
    assert_eq!(actual_commit, Some(commit));
    assert_eq!(actual_abort, None);

    let abort = WriteAbortInput {
        write_id,
        reason: "writer failed".to_string(),
        completed_writer_outputs: Vec::new(),
        incomplete_writers: Vec::new(),
    };
    let abort_outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Write)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: None,
            write_abort: Some(abort.clone()),
            fragment_profiles: Vec::new(),
        })
        .expect("Write intent accepts an abort payload");
    let (_, actual_commit, actual_abort) = abort_outcome
        .into_write()
        .expect("write outcome variant")
        .into_parts();
    assert_eq!(actual_commit, None);
    assert_eq!(actual_abort, Some(abort));
}

#[test]
fn profile_outcome_preserves_fragment_profiles() {
    let profile = crate::runtime::profile::Profiler::new("fragment-7").to_native_tree();
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Profile)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "status",
                vec!["profiled".to_string()],
            )
            .expect("profile result"),
            write_commit: None,
            write_abort: None,
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
            write_commit: None,
            write_abort: None,
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

#[test]
fn write_outcome_rejects_commit_and_abort_together() {
    let write_id = crate::common::types::UniqueId { hi: 8, lo: 13 };
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Write).write(
        crate::runtime::query_result::QueryResult::empty(),
        Some(WriteCommitInput {
            write_id,
            writers: Vec::new(),
        }),
        Some(WriteAbortInput {
            write_id,
            reason: "ambiguous".to_string(),
            completed_writer_outputs: Vec::new(),
            incomplete_writers: Vec::new(),
        }),
    );

    let Err(error) = result else {
        panic!("Write outcome must reject simultaneous commit and abort payloads");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "Write outcome cannot contain both commit and abort payloads"
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
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        QueryCancellationView::never_cancelled(),
    )
    .expect("build service request");

    let outcome = service
        .execute(request)
        .expect("injected coordinator result");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.intent(), DistributedQueryIntent::Result);
}
