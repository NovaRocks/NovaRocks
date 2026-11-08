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

//! Core-selected diagnostic cut publication through the original owner.

use super::*;
use crate::native::task_result_transport::{DynamicFilterRead, DynamicFilterReadError};
use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
use crate::task_execution::status_intake::CountingWake;
use novarocks_attempt_protocol::publication::{
    DiagnosticClosure, DiagnosticMissing, DiagnosticPublication, DiagnosticTaskObservation,
};
use novarocks_coordination_contract::{AttemptComputationCut, ExecutionEffect, MonotonicClock};
use novarocks_execution_contract::{
    DomainVersion, OperationKind, ParticipationReceipt, ParticipationState, ReleaseOutcome,
    root_result::RootResultEnd,
};
use novarocks_query_application::api::ComputationObservationBudgets;
use novarocks_query_application::coordination::{
    AbortParticipationEffectPort, LogicalExecutionActorConfig,
};
use novarocks_query_application::test_support::LogicalExecutionTestHarness;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

struct NoDiagnosticRead;
impl TaskResultTransport for NoDiagnosticRead {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        panic!("core publication does not issue a final-info read")
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("core publication does not issue a dynamic-filter read")
    }
}

#[tokio::test]
async fn core_publication_rejects_foreign_cut_and_keeps_original_snapshot_immutable() {
    let (mut round, acks, release, clock, establish_facts) =
        crate::task_execution::tests::manifest_normal_release_ack_with_actor_facts_fixture();
    let root_task = round.root_task();
    let participation = *round.execution().graph().participations().next().unwrap();
    let cut = clock.now();
    let diagnostic_budget = Duration::from_millis(10);
    let convergence_budget = Duration::from_secs(60);
    let control = novarocks_workload_control::WorkloadControl::try_new(
        novarocks_workload_control::WorkloadConfig::default(),
        novarocks_workload_control::ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 10,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    let governed = control
        .try_begin_root(novarocks_workload_control::WorkRequest::new(
            novarocks_workload_control::WorkClass::Query,
        ))
        .unwrap();
    let stage = governed
        .owner
        .scope()
        .try_acquire(novarocks_workload_control::Stage::Execution)
        .unwrap();
    let (abort, _abort_intake) =
        crate::task_execution::abort_effect::NativeAbortEffectAdapter::bounded(
            NonZeroUsize::new(2).unwrap(),
            Arc::new(crate::task_execution::status_intake::CountingWake::default()),
        );
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        root_task.query_execution_id(),
        ExecutionEffect::None,
        NonZeroUsize::new(8).unwrap(),
        vec![participation],
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        governed.owner,
        stage,
    )
    .unwrap()
    .with_abort_participation_effect_port(
        abort as Arc<dyn AbortParticipationEffectPort>,
        NonZeroUsize::new(2).unwrap(),
    );
    let mut logical = LogicalExecutionTestHarness::install_internal(
        tokio::runtime::Handle::current(),
        config,
        &crate::query_execution::internal_consumer::test_fixture::internal_count_description().0,
        NonZeroUsize::new(1).unwrap(),
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        ComputationObservationBudgets::new(diagnostic_budget, convergence_budget),
        root_task.query_execution_id(),
        vec![participation],
    )
    .unwrap();
    let activation = logical.activate_initial().await.unwrap();
    crate::task_execution::tests::settle_original_actor_establish_facts(&logical, establish_facts)
        .await;
    let root_status = round
        .execution()
        .task(root_task.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let ready = logical
        .accept_internal_end(
            root_task,
            root_status,
            RootResultEnd {
                sequence: NonZeroU64::MIN,
                output_rows: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(ready.activation(), activation);
    assert_eq!(ready.root(), root_task);
    assert_eq!(ready.cut(), cut);
    assert_eq!(
        ready.diagnostic_deadline(),
        cut.saturating_add(diagnostic_budget)
    );
    assert_eq!(
        ready.convergence_deadline(),
        cut.saturating_add(convergence_budget)
    );

    let mut collector = TaskFinalInfoCollector::new(
        Arc::new(NoDiagnosticRead),
        true,
        &round,
        Arc::new(CountingWake::default()),
    );
    let observer = collector.enable_profile_observation().unwrap();
    collector.begin_profile_observation(ready).unwrap();
    let exact = AttemptComputationCut {
        activation: ready.activation(),
        root: ready.root(),
        cut: ready.cut(),
        diagnostic_deadline: ready.diagnostic_deadline(),
        convergence_deadline: ready.convergence_deadline(),
        end: ready.end(),
    };
    let missing = || {
        round
            .execution()
            .graph()
            .tasks()
            .map(|task| {
                (
                    task.identity(),
                    DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadPending),
                )
            })
            .collect::<Box<[_]>>()
    };
    let fragments = || {
        round
            .execution()
            .graph()
            .tasks()
            .map(|task| (task.identity(), task.fragment_id()))
            .collect::<Box<[_]>>()
    };
    let mut foreign = exact;
    foreign.cut = foreign.cut.saturating_add(Duration::from_nanos(1));
    assert!(
        collector
            .publish_core_cut(
                ready,
                DiagnosticPublication {
                    ready: foreign,
                    closure: DiagnosticClosure::ObservationDeadline,
                    tasks: missing()
                },
                round.execution().released_runtime_filter_contributions(),
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Pending),
                fragments()
            )
            .is_err()
    );
    assert!(observer.current(ready).unwrap().is_none());
    let mut bad_tasks = missing().into_vec();
    let status = round
        .execution()
        .task(root_task.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let root_info = FinalTaskInfo::try_new(root_task, status, Vec::new(), false).unwrap();
    let foreign_slot = bad_tasks
        .iter_mut()
        .find(|(task, _)| *task != root_task)
        .unwrap();
    foreign_slot.1 = DiagnosticTaskObservation::Available(Box::new(root_info));
    assert!(
        collector
            .publish_core_cut(
                ready,
                DiagnosticPublication {
                    ready: exact,
                    closure: DiagnosticClosure::ObservationDeadline,
                    tasks: bad_tasks.into_boxed_slice()
                },
                round.execution().released_runtime_filter_contributions(),
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Pending),
                fragments()
            )
            .is_err()
    );
    assert!(observer.current(ready).unwrap().is_none());
    assert!(
        collector
            .publish_core_cut(
                ready,
                DiagnosticPublication {
                    ready: exact,
                    closure: DiagnosticClosure::ObservationDeadline,
                    tasks: missing()
                },
                round.execution().released_runtime_filter_contributions(),
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Pending),
                fragments()
            )
            .unwrap()
    );
    let first = observer.current(ready).unwrap().unwrap();
    assert_eq!(first.ready, ready);
    assert_eq!(first.closure, ProfileDiagnosticClosure::ObservationDeadline);
    assert_eq!(first.tasks.len(), collector.expected.len());
    assert!(first.tasks.iter().all(|(_, fact)| matches!(
        fact,
        ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
    )));
    assert!(
        !collector
            .publish_core_cut(
                ready,
                DiagnosticPublication {
                    ready: exact,
                    closure: DiagnosticClosure::Cancelled,
                    tasks: missing()
                },
                round.execution().released_runtime_filter_contributions(),
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Failed),
                fragments()
            )
            .unwrap()
    );
    let second = observer.current(ready).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(
        second.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert!(collector.final_info_closed());

    // Settle the fixture's original Release and original QA owner rather than
    // treating dropped observation objects as actual participation exit.
    let OperationIntent::ReleaseParticipation(request) = &release else {
        unreachable!()
    };
    acks.publish(OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseParticipation,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: ParticipationReceipt::new(
                request.participation(),
                ParticipationState::TerminalRetained,
            ),
            outcome: ReleaseOutcome::Released,
            runtime_filter: Some(
                crate::task_execution::tests::fixture_runtime_filter_contribution(),
            ),
        },
    ));
    round.turn().unwrap();
    logical.abandon_running_attempt();
    logical
        .observe_worker_stopped_and_participation_fenced(participation)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
