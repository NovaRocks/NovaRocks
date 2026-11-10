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
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::host::publication::{
    DiagnosticSourceSnapshot, OriginalDiagnosticBinding, OriginalDiagnosticPayload,
    OriginalDiagnosticRecords,
};
use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
use crate::task_execution::status_intake::CountingWake;
use novarocks_attempt_protocol::publication::{
    DiagnosticClosure, DiagnosticMissing, DiagnosticPublication, DiagnosticSources,
    DiagnosticTaskObservation, RuntimeFilterDiagnostic, SplitDiagnosticObservation,
};
use novarocks_attempt_protocol::{input::FinalTaskInfoRef, start::PayloadRef};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_coordination_contract::{AttemptComputationCut, ExecutionEffect, MonotonicClock};
use novarocks_execution_contract::ParticipationRef;
use novarocks_execution_contract::{
    DomainVersion, OperationKind, ParticipationReceipt, ParticipationState, ReleaseOutcome,
    root_result::RootResultEnd,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::api::ComputationObservationBudgets;
use novarocks_query_application::coordination::{
    AbortParticipationEffectPort, LogicalExecutionActorConfig,
};
use novarocks_query_application::test_support::LogicalExecutionTestHarness;
use std::num::{NonZeroU64, NonZeroUsize};

struct OriginalRecords {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    parts: Vec<ParticipationRef>,
    fragments: Vec<(TaskIdentity, crate::query_execution::artifact::FragmentId)>,
    rows: BTreeMap<PayloadRef, OwnedPayload>,
}
enum OwnedPayload {
    RuntimeFilter(
        AttemptActivationIdentity,
        ParticipationRef,
        QueryTerminalProfileContributionTelemetry,
    ),
    SplitProfile(AttemptActivationIdentity, TaskIdentity, SplitSourceProfile),
}
impl OriginalRecords {
    fn new(round: &TaskRound, activation: AttemptActivationIdentity) -> Self {
        Self {
            activation,
            root: round.root_task(),
            parts: round
                .execution()
                .graph()
                .participations()
                .copied()
                .collect(),
            fragments: round
                .execution()
                .graph()
                .tasks()
                .map(|task| (task.identity(), task.fragment_id()))
                .collect(),
            rows: BTreeMap::new(),
        }
    }
}
impl OriginalDiagnosticRecords for OriginalRecords {
    fn binding(&self) -> OriginalDiagnosticBinding<'_> {
        OriginalDiagnosticBinding {
            activation: self.activation,
            root: self.root,
            participations: &self.parts,
            fragments: &self.fragments,
        }
    }
    fn payload(&self, reference: PayloadRef) -> Option<OriginalDiagnosticPayload> {
        Some(match self.rows.get(&reference)? {
            OwnedPayload::RuntimeFilter(activation, participation, telemetry) => {
                OriginalDiagnosticPayload::RuntimeFilter {
                    activation: *activation,
                    participation: *participation,
                    telemetry: telemetry.clone(),
                }
            }
            OwnedPayload::SplitProfile(activation, root, profile) => {
                OriginalDiagnosticPayload::SplitProfile {
                    activation: *activation,
                    root: *root,
                    profile: *profile,
                }
            }
        })
    }
}
fn reference(id: u64) -> PayloadRef {
    PayloadRef(NonZeroU64::new(id).unwrap())
}
fn information_reference(id: u64, task: TaskIdentity) -> FinalTaskInfoRef {
    FinalTaskInfoRef::new(
        novarocks_coordination_contract::AttemptRequestToken::new(NonZeroU64::new(id).unwrap()),
        task,
    )
}
fn unexpected_resolution(_: FinalTaskInfoRef) -> Result<Box<FinalTaskInfo>, TaskExecutionError> {
    panic!("this publication must not consume a canonical final-info record")
}
fn selected_cut(round: &TaskRound, ready: AttemptComputationCut) -> DiagnosticPublication {
    DiagnosticPublication {
        ready,
        closure: DiagnosticClosure::ObservationDeadline,
        sources: DiagnosticSources {
            selected_at: ready.cut.saturating_add(Duration::from_millis(10)),
            runtime_filters: RuntimeFilterDiagnostic {
                contributions: Vec::new(),
                complete: false,
            },
            split: SplitDiagnosticObservation::Pending,
        },
        tasks: round
            .execution()
            .graph()
            .tasks()
            .map(|task| {
                (
                    task.identity(),
                    DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadPending),
                )
            })
            .collect(),
    }
}
fn source_fixture() -> (TaskRound, DiagnosticPublication, OriginalRecords) {
    let (round, _) = crate::task_execution::tests::manifest_terminal_regression_fixture();
    let root = round.root_task();
    let now = round.execution().monotonic_now();
    let activation = AttemptActivationIdentity::from_parts(910, root.query_execution_id(), 1);
    let ready = AttemptComputationCut {
        activation,
        root,
        cut: now,
        diagnostic_deadline: now.saturating_add(Duration::from_secs(1)),
        convergence_deadline: now.saturating_add(Duration::from_secs(60)),
        end: RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 0,
        },
    };
    let cut = selected_cut(&round, ready);
    let records = OriginalRecords::new(&round, activation);
    (round, cut, records)
}

#[test]
fn core_publication_resolves_pre_cut_rows_after_later_rf_and_split_changes() {
    let (_, mut cut, mut records) = source_fixture();
    let original = crate::task_execution::tests::fixture_runtime_filter_contribution();
    records.rows.insert(
        reference(1),
        OwnedPayload::RuntimeFilter(records.activation, records.parts[0], original.clone()),
    );
    let before = SplitSourceProfile {
        files_considered: 10,
        splits_emitted: 3,
        ..Default::default()
    };
    records.rows.insert(
        reference(2),
        OwnedPayload::SplitProfile(records.activation, records.root, before),
    );
    cut.sources
        .runtime_filters
        .contributions
        .push((records.parts[0].backend_process_id(), reference(1)));
    cut.sources.split = SplitDiagnosticObservation::Available(reference(2));
    // Later rich records exist before charged expansion/resolution completes.
    // The old rows are still owned; no current-source sampling API is offered.
    let mut later = original.as_proto().clone();
    if let Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(profile)) = later.telemetry.as_mut() {
        profile.channels[0].published_count = 2;
        profile.channels[0].completed_count = 2;
    }
    let later = QueryTerminalProfileContributionTelemetry::parse(later).unwrap();
    records.rows.insert(
        reference(3),
        OwnedPayload::RuntimeFilter(records.activation, records.parts[0], later.clone()),
    );
    records.rows.insert(
        reference(4),
        OwnedPayload::SplitProfile(
            records.activation,
            records.root,
            SplitSourceProfile {
                files_considered: 20,
                splits_emitted: 8,
                ..Default::default()
            },
        ),
    );
    let resolved = DiagnosticSourceSnapshot::resolve(&cut, &records).unwrap();
    let (rf, split) = resolved
        .into_selected(&cut, &records.parts, &records.fragments)
        .unwrap();
    assert_eq!(
        rf.contributions(),
        &[(records.parts[0].backend_process_id(), original)]
    );
    assert!(
        !rf.is_complete(),
        "a later Release cannot complete the first cut"
    );
    assert!(matches!(split, ProfileSplitDiagnostic::Available(profile) if profile == before));
    // A later cut can select the new rows; it does not rewrite the first one.
    cut.sources.runtime_filters.contributions[0].1 = reference(3);
    cut.sources.runtime_filters.complete = true;
    cut.sources.split = SplitDiagnosticObservation::Available(reference(4));
    let (rf, split) = DiagnosticSourceSnapshot::resolve(&cut, &records)
        .unwrap()
        .into_selected(&cut, &records.parts, &records.fragments)
        .unwrap();
    assert_eq!(rf.contributions()[0].1, later);
    assert!(rf.is_complete());
    assert!(
        matches!(split, ProfileSplitDiagnostic::Available(profile) if profile.splits_emitted == 8)
    );
}

#[test]
fn core_publication_preserves_absent_pending_failed_and_source_gone_without_a_live_source() {
    let (_, mut cut, records) = source_fixture();
    cut.sources.runtime_filters.complete = true;
    for observation in [
        SplitDiagnosticObservation::Absent,
        SplitDiagnosticObservation::Pending,
        SplitDiagnosticObservation::Failed,
        SplitDiagnosticObservation::SourceGone,
    ] {
        cut.sources.split = observation;
        let (rf, split) = DiagnosticSourceSnapshot::resolve(&cut, &records)
            .unwrap()
            .into_selected(&cut, &records.parts, &records.fragments)
            .unwrap();
        assert!(rf.is_complete());
        assert!(
            rf.contributions().is_empty(),
            "all released with no contribution is complete empty data"
        );
        match observation {
            SplitDiagnosticObservation::Absent => assert!(
                matches!(split, ProfileSplitDiagnostic::Available(profile) if profile == SplitSourceProfile::default())
            ),
            SplitDiagnosticObservation::Pending => assert!(matches!(
                split,
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Pending)
            )),
            SplitDiagnosticObservation::Failed => assert!(matches!(
                split,
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Failed)
            )),
            SplitDiagnosticObservation::SourceGone => assert!(matches!(
                split,
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::SourceGone)
            )),
            SplitDiagnosticObservation::Available(_) => unreachable!(),
        }
    }
}

#[test]
fn core_publication_rejects_foreign_wrong_kind_and_changed_selection_refs() {
    let (_, mut cut, mut records) = source_fixture();
    cut.sources.split = SplitDiagnosticObservation::Available(reference(1));
    assert!(DiagnosticSourceSnapshot::resolve(&cut, &records).is_err());
    records.rows.insert(
        reference(1),
        OwnedPayload::RuntimeFilter(
            records.activation,
            records.parts[0],
            crate::task_execution::tests::fixture_runtime_filter_contribution(),
        ),
    );
    assert!(
        DiagnosticSourceSnapshot::resolve(&cut, &records).is_err(),
        "RF ref cannot resolve as split"
    );
    let foreign = AttemptActivationIdentity::from_parts(911, records.root.query_execution_id(), 1);
    records.rows.insert(
        reference(1),
        OwnedPayload::SplitProfile(foreign, records.root, SplitSourceProfile::default()),
    );
    assert!(DiagnosticSourceSnapshot::resolve(&cut, &records).is_err());
    records.rows.insert(
        reference(1),
        OwnedPayload::SplitProfile(
            records.activation,
            records.root,
            SplitSourceProfile::default(),
        ),
    );
    let resolved = DiagnosticSourceSnapshot::resolve(&cut, &records).unwrap();
    cut.sources.split = SplitDiagnosticObservation::SourceGone;
    assert!(
        resolved
            .into_selected(&cut, &records.parts, &records.fragments)
            .is_err(),
        "resolved old profile cannot be relabeled SourceGone"
    );
    cut.sources
        .runtime_filters
        .contributions
        .push((novarocks_types::BackendProcessId::new_v7(), reference(1)));
    assert!(DiagnosticSourceSnapshot::resolve(&cut, &records).is_err());
    let telemetry = crate::task_execution::tests::fixture_runtime_filter_contribution();
    records.rows.insert(
        reference(2),
        OwnedPayload::RuntimeFilter(foreign, records.parts[0], telemetry.clone()),
    );
    cut.sources.runtime_filters.contributions[0] =
        (records.parts[0].backend_process_id(), reference(2));
    assert!(
        DiagnosticSourceSnapshot::resolve(&cut, &records).is_err(),
        "same backend cannot authorize a foreign activation row"
    );
    records.rows.insert(
        reference(2),
        OwnedPayload::RuntimeFilter(records.activation, records.parts[0], telemetry),
    );
    cut.sources.runtime_filters.contributions[0].0 = novarocks_types::BackendProcessId::new_v7();
    assert!(
        DiagnosticSourceSnapshot::resolve(&cut, &records).is_err(),
        "a frozen RF row cannot be attributed to a different backend"
    );
}

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
    let mut records = OriginalRecords::new(&round, activation);
    let mut foreign = selected_cut(&round, exact);
    foreign.ready.cut = foreign.ready.cut.saturating_add(Duration::from_nanos(1));
    let sources = DiagnosticSourceSnapshot::resolve(&foreign, &records).unwrap();
    assert!(
        collector
            .publish_core_cut(ready, &foreign, sources, unexpected_resolution)
            .is_err()
    );
    assert!(observer.current(ready).unwrap().is_none());

    let mut bad_tasks = selected_cut(&round, exact);
    let status = round
        .execution()
        .task(root_task.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let mut root_info = Some(Box::new(
        FinalTaskInfo::try_new(root_task, status.clone(), Vec::new(), false).unwrap(),
    ));
    let sibling = bad_tasks
        .tasks
        .iter_mut()
        .find(|(task, _)| *task != root_task)
        .unwrap();
    let sibling_reference = information_reference(1, sibling.0);
    sibling.1 = DiagnosticTaskObservation::Available(sibling_reference);
    let sources = DiagnosticSourceSnapshot::resolve(&bad_tasks, &records).unwrap();
    assert!(matches!(
        collector.publish_core_cut(ready, &bad_tasks, sources, |reference| {
            assert_eq!(reference, sibling_reference);
            Ok(root_info.take().unwrap())
        }),
        Err(TaskExecutionError::Schedule(message))
            if message == "core diagnostic payload names a foreign Task"
    ));
    assert!(observer.current(ready).unwrap().is_none());

    // A row/reference mismatch is rejected before taking any rich object.
    let mut wrong_reference = selected_cut(&round, exact);
    wrong_reference
        .tasks
        .iter_mut()
        .find(|(task, _)| *task != root_task)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(information_reference(2, root_task));
    let sources = DiagnosticSourceSnapshot::resolve(&wrong_reference, &records).unwrap();
    assert!(matches!(
        collector.publish_core_cut(ready, &wrong_reference, sources, unexpected_resolution),
        Err(TaskExecutionError::Schedule(message))
            if message == "core diagnostic reference names a foreign Task"
    ));
    assert!(observer.current(ready).unwrap().is_none());

    // Resolver failure stays an error and cannot publish a partial snapshot.
    let root_reference = information_reference(3, root_task);
    let mut failed_resolution = selected_cut(&round, exact);
    failed_resolution
        .tasks
        .iter_mut()
        .find(|(task, _)| *task == root_task)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(root_reference);
    let sources = DiagnosticSourceSnapshot::resolve(&failed_resolution, &records).unwrap();
    assert!(matches!(
        collector.publish_core_cut(ready, &failed_resolution, sources, |reference| {
            assert_eq!(reference, root_reference);
            Err(TaskExecutionError::UnknownOperation)
        }),
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert!(observer.current(ready).unwrap().is_none());

    // Matching Task IDs cannot disguise a foreign frozen fragment binding.
    let mut wrong_binding = OriginalRecords::new(&round, activation);
    wrong_binding.fragments[0].1 = wrong_binding.fragments[0].1.wrapping_add(1);
    let cut = selected_cut(&round, exact);
    let sources = DiagnosticSourceSnapshot::resolve(&cut, &wrong_binding).unwrap();
    assert!(
        collector
            .publish_core_cut(ready, &cut, sources, unexpected_resolution)
            .is_err()
    );
    let before = SplitSourceProfile {
        files_considered: 7,
        splits_emitted: 2,
        ..Default::default()
    };
    records.rows.insert(
        reference(1),
        OwnedPayload::SplitProfile(activation, root_task, before),
    );
    let mut first_cut = selected_cut(&round, exact);
    first_cut.sources.split = SplitDiagnosticObservation::Available(reference(1));
    first_cut
        .tasks
        .iter_mut()
        .find(|(task, _)| *task == root_task)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(root_reference);
    let mut original_info = Some(Box::new(
        FinalTaskInfo::try_new(root_task, status, Vec::new(), false).unwrap(),
    ));
    let mut resolved = 0;
    let selected_at = first_cut.sources.selected_at;
    // The canonical row is sufficient even after the weak live source is gone.
    let sources = DiagnosticSourceSnapshot::resolve(&first_cut, &records).unwrap();
    assert!(
        collector
            .publish_core_cut(ready, &first_cut, sources, |reference| {
                assert_eq!(reference, root_reference);
                resolved += 1;
                Ok(original_info.take().unwrap())
            })
            .unwrap()
    );
    assert_eq!(resolved, 1);
    assert!(original_info.is_none());
    let first = observer.current(ready).unwrap().unwrap();
    assert_eq!(first.ready, ready);
    assert_eq!(first.selected_at, selected_at);
    assert_ne!(first.selected_at, ready.cut());
    assert!(matches!(first.split, ProfileSplitDiagnostic::Available(profile) if profile == before));
    assert!(first.tasks.iter().all(|(task, fact)| {
        if *task == root_task {
            matches!(fact, ProfileTaskDiagnostic::Available(info)
                if info.final_status().identity() == root_task)
        } else {
            matches!(
                fact,
                ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
            )
        }
    }));
    let mut later_cut = selected_cut(&round, exact);
    later_cut.closure = DiagnosticClosure::Cancelled;
    later_cut.sources.selected_at = selected_at.saturating_add(Duration::from_millis(1));
    later_cut.sources.runtime_filters.complete = true;
    later_cut.sources.split = SplitDiagnosticObservation::SourceGone;
    // Even a new Available ref cannot consume records after the first cut.
    later_cut
        .tasks
        .iter_mut()
        .find(|(task, _)| *task == root_task)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(information_reference(4, root_task));
    let sources = DiagnosticSourceSnapshot::resolve(&later_cut, &records).unwrap();
    assert!(
        !collector
            .publish_core_cut(ready, &later_cut, sources, unexpected_resolution)
            .unwrap()
    );
    let second = observer.current(ready).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(second.selected_at, selected_at);
    assert!(
        matches!(second.split, ProfileSplitDiagnostic::Available(profile) if profile == before)
    );
    assert!(!second.runtime_filters.is_complete());
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
