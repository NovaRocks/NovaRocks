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
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::graph::TaskGraph;
use crate::task_execution::host::publication::{
    DiagnosticPayloadAddress, DiagnosticSourceSnapshot, OriginalDiagnosticBinding,
    OriginalDiagnosticPayload, OriginalDiagnosticRecords,
};
use novarocks_attempt_protocol::publication::{
    DiagnosticClosure, DiagnosticMissing, DiagnosticPublication, DiagnosticSources,
    DiagnosticTaskObservation, RuntimeFilterDiagnostic, SplitDiagnosticObservation,
};
use novarocks_attempt_protocol::{input::FinalTaskInfoRef, start::PayloadRef};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_coordination_contract::{AttemptComputationCut, MonotonicClock};
use novarocks_execution_contract::ParticipationRef;
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use std::num::NonZeroU64;

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
    fn from_collector(
        collector: &TaskFinalInfoCollector,
        activation: AttemptActivationIdentity,
    ) -> Self {
        Self {
            activation,
            root: collector.root,
            parts: collector.expected_participations.to_vec(),
            fragments: collector.expected_fragments.to_vec(),
            rows: BTreeMap::new(),
        }
    }

    fn new(graph: &TaskGraph, activation: AttemptActivationIdentity) -> Self {
        Self {
            activation,
            root: graph.root_identity(),
            parts: graph.participations().copied().collect(),
            fragments: graph
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
    fn payload(&self, address: DiagnosticPayloadAddress) -> Option<OriginalDiagnosticPayload> {
        let reference = match address {
            DiagnosticPayloadAddress::RuntimeFilter { reference, .. }
            | DiagnosticPayloadAddress::SplitProfile(reference) => reference,
        };
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
fn selected_cut(graph: &TaskGraph, ready: AttemptComputationCut) -> DiagnosticPublication {
    selected_cut_for_tasks(graph.tasks().map(|task| task.identity()), ready)
}
fn selected_cut_for_tasks(
    tasks: impl IntoIterator<Item = TaskIdentity>,
    ready: AttemptComputationCut,
) -> DiagnosticPublication {
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
        tasks: tasks
            .into_iter()
            .map(|task| {
                (
                    task,
                    DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadPending),
                )
            })
            .collect(),
    }
}
/// Metadata tier only: the SAME 1-BE, 3-fragment/3-Task chain as the
/// retired manifest_terminal_regression_fixture's Harness::new(&[0], &[0],64).
/// These resolver tests read no Task state and create no Ready capability.
fn source_fixture() -> (TaskGraph, DiagnosticPublication, OriginalRecords) {
    let graph = crate::task_execution::graph_manifest_tests::unstarted_chain_graph();
    let root = graph.root_identity();
    // The old ManualClock was never advanced by its finite fixture turns.
    let now = MonotonicInstant::ORIGIN;
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
    let cut = selected_cut(&graph, ready);
    let records = OriginalRecords::new(&graph, activation);
    (graph, cut, records)
}

#[test]
fn core_publication_resolves_pre_cut_rows_after_later_rf_and_split_changes() {
    let (_, mut cut, mut records) = source_fixture();
    let original = crate::fixtures::facts::fixture_runtime_filter_contribution();
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
        .push((records.parts[0].backend_process_id(), reference(1).0));
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
    cut.sources.runtime_filters.contributions[0].1 = reference(3).0;
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
            crate::fixtures::facts::fixture_runtime_filter_contribution(),
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
        .push((novarocks_types::BackendProcessId::new_v7(), reference(1).0));
    assert!(DiagnosticSourceSnapshot::resolve(&cut, &records).is_err());
    let telemetry = crate::fixtures::facts::fixture_runtime_filter_contribution();
    records.rows.insert(
        reference(2),
        OwnedPayload::RuntimeFilter(foreign, records.parts[0], telemetry.clone()),
    );
    cut.sources.runtime_filters.contributions[0] =
        (records.parts[0].backend_process_id(), reference(2).0);
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

/// Both inputs are passive original metadata. This grants no Ready capability.
fn sources_for_graph(
    graph: &TaskGraph,
    original: AttemptComputationCut,
    cut: &DiagnosticPublication,
    records: &OriginalRecords,
) -> Result<(ReleasedRuntimeFilterContributions, ProfileSplitDiagnostic), TaskExecutionError> {
    let expected = graph
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    let parts = graph.participations().copied().collect::<Vec<_>>();
    let fragments = graph
        .tasks()
        .map(|task| (task.identity(), task.fragment_id()))
        .collect::<Vec<_>>();
    let sources = DiagnosticSourceSnapshot::resolve(cut, records)?;
    resolve_selected_diagnostic_sources(
        original,
        DiagnosticSelectionBinding {
            tasks: &expected,
            participations: &parts,
            fragments: &fragments,
        },
        cut,
        sources,
    )
}

#[test]
fn core_publication_expands_the_exact_original_three_task_rows_without_ready() {
    use novarocks_execution_contract::{TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion};
    let (graph, mut cut, mut records) = source_fixture();
    assert_eq!(
        graph.tasks().count(),
        3,
        "the original mixed-row case must remain nonempty"
    );
    let root = graph.root_identity();
    cut.ready.diagnostic_deadline = cut.ready.cut.saturating_add(Duration::from_millis(10));
    let original = cut.ready;
    // A resolver's typed metadata payload only, not a status publication,
    // Ready acceptance, Worker observation or native execution proof.
    let status = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(4).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    let mut foreign = cut.clone();
    foreign.ready.cut = foreign.ready.cut.saturating_add(Duration::from_nanos(1));
    assert!(sources_for_graph(&graph, original, &foreign, &records).is_err());

    let mut bad_tasks = cut.clone();
    let mut root_info = Some(Box::new(
        FinalTaskInfo::try_new(root, status.clone(), Vec::new(), false).unwrap(),
    ));
    let sibling = bad_tasks
        .tasks
        .iter_mut()
        .find(|(task, _)| *task != root)
        .unwrap();
    let sibling_reference = information_reference(1, sibling.0);
    sibling.1 = DiagnosticTaskObservation::Available(sibling_reference);
    sources_for_graph(&graph, original, &bad_tasks, &records).unwrap();
    assert!(
        matches!(expand_selected_diagnostic_rows(&bad_tasks, |reference| {
        assert_eq!(reference, sibling_reference);
        Ok(root_info.take().unwrap())
    }), Err(TaskExecutionError::Schedule(message))
        if message == "core diagnostic payload names a foreign Task")
    );

    let mut wrong_reference = cut.clone();
    wrong_reference
        .tasks
        .iter_mut()
        .find(|(task, _)| *task != root)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(information_reference(2, root));
    sources_for_graph(&graph, original, &wrong_reference, &records).unwrap();
    assert!(
        matches!(expand_selected_diagnostic_rows(&wrong_reference, unexpected_resolution),
        Err(TaskExecutionError::Schedule(message))
        if message == "core diagnostic reference names a foreign Task")
    );

    let root_reference = information_reference(3, root);
    let mut failed_resolution = cut.clone();
    failed_resolution
        .tasks
        .iter_mut()
        .find(|(task, _)| *task == root)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(root_reference);
    sources_for_graph(&graph, original, &failed_resolution, &records).unwrap();
    assert!(matches!(
        expand_selected_diagnostic_rows(&failed_resolution, |reference| {
            assert_eq!(reference, root_reference);
            Err(TaskExecutionError::UnknownOperation)
        }),
        Err(TaskExecutionError::UnknownOperation)
    ));

    let mut wrong_binding = OriginalRecords::new(&graph, records.activation);
    wrong_binding.fragments[0].1 = wrong_binding.fragments[0].1.wrapping_add(1);
    assert!(sources_for_graph(&graph, original, &cut, &wrong_binding).is_err());
    let before = SplitSourceProfile {
        files_considered: 7,
        splits_emitted: 2,
        ..Default::default()
    };
    records.rows.insert(
        reference(1),
        OwnedPayload::SplitProfile(records.activation, root, before),
    );
    cut.sources.split = SplitDiagnosticObservation::Available(reference(1));
    cut.tasks
        .iter_mut()
        .find(|(task, _)| *task == root)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(root_reference);
    let mut original_info = Some(Box::new(
        FinalTaskInfo::try_new(root, status, Vec::new(), false).unwrap(),
    ));
    let (rf, split) = sources_for_graph(&graph, original, &cut, &records).unwrap();
    let mut resolved = 0;
    let rows = expand_selected_diagnostic_rows(&cut, |reference| {
        assert_eq!(reference, root_reference);
        resolved += 1;
        Ok(original_info.take().unwrap())
    })
    .unwrap();
    assert_eq!(resolved, 1);
    assert!(original_info.is_none());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.iter().filter(|(task, _)| *task != root).count(), 2);
    assert!(rows.iter().all(|(task, fact)| if *task == root {
        matches!(fact, ProfileTaskDiagnostic::Available(info) if info.final_status().identity() == root)
    } else { matches!(fact, ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)) }));
    assert!(!rf.is_complete());
    assert!(matches!(split, ProfileSplitDiagnostic::Available(profile) if profile == before));
}

async fn drive_profile_original<F: std::future::Future>(
    fixture: &mut crate::fixtures::FrontendFixture,
    joint: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    clock: &crate::task_execution::clock::ManualClock,
    allow_original_clock: bool,
    original: F,
) -> F::Output {
    use crate::fixtures::{ProgressBudget, ProgressSelection};
    let mut original = Box::pin(original);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
        if let std::task::Poll::Ready(result) = futures::poll!(original.as_mut()) {
            return result;
        }
        let progressed = fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
        if allow_original_clock
            && progressed.actors == 0
            && progressed.hosts == 0
            && let Some(next) = fixture.runtime().next_deadline()
            && std::time::Duration::from_nanos(next.0) > clock.now().since_origin()
        {
            clock.advance(Duration::from_nanos(
                next.0 - u64::try_from(clock.now().since_origin().as_nanos()).unwrap(),
            ));
        }
        assert!(
            std::time::Instant::now() < deadline,
            "original diagnostic fixture work did not exit"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn core_publication_rejects_foreign_cut_and_keeps_original_snapshot_immutable() {
    use crate::fixtures::native_recipe::{
        OriginalRecipeContext, OriginalRecipeOperands, PreparedOriginalRecipeProcess, RecipeRoot,
        count_description, install_original_recipe, prepare_original_recipe_process,
    };
    use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
    use crate::query_execution::internal_consumer::profile::profile_computation_consumer;
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    };
    use std::sync::atomic::Ordering;
    let mut data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut peer = Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(novarocks_types::BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    let PreparedOriginalRecipeProcess {
        mut fixture,
        process,
        clock,
        mut joint,
        workload,
        mut supervisor,
        client,
        capacity,
        handle,
        descriptor,
    } = prepare_original_recipe_process(&data, descriptor, history, ResultCapacityConfig::V1, None);
    let root_work = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root_work.owner.scope();
    let business = root_work.business;
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Internal)
        .unwrap();
    let (description, plan) = count_description();
    let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
    let (mut logical, cohort, mut output) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut fixture,
            joint: &mut joint,
            process: &process,
            data: &data,
            clock: &clock,
            handle,
            descriptor,
            client,
        },
        OriginalRecipeOperands {
            owner: root_work.owner,
            binding: QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap(),
            description,
            plan,
            root: RecipeRoot::EmptyCountOnly,
            consumer,
            offers,
            output,
        },
    );
    logical
        .probe
        .profile_capture_requested
        .store(true, Ordering::Release);
    let installed = fixture.activate_attempt(&logical, &mut joint, false).await;
    let root = logical.probe.root.lock().unwrap().unwrap();
    let mut collector = logical
        .probe
        .profile_capture
        .lock()
        .unwrap()
        .take()
        .expect("collector froze the SAME actual graph before original installation");
    assert_eq!(collector.root, root);
    assert_eq!(
        collector.expected.as_ref(),
        logical.probe.tasks.lock().unwrap().as_slice()
    );
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    logical.probe.release_activation();
    let mut original = drive_profile_original(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        false,
        logical.ready.take().unwrap(),
    )
    .await
    .unwrap();
    let ready = *original
        .ready_token()
        .expect("actual Relay/Consumer accepted original Ready");
    let received = output.take_ready(ready).unwrap();
    let bound = logical.binding.bound().unwrap();
    assert_eq!(
        ready.activation(),
        logical.probe.activation.lock().unwrap().unwrap()
    );
    assert_eq!(ready.root(), root);
    assert_eq!(ready.cut(), clock.now());
    assert_eq!(
        ready.diagnostic_deadline(),
        ready
            .cut()
            .saturating_add(bound.start.budgets.observation.diagnostics)
    );
    assert_eq!(
        ready.convergence_deadline(),
        ready
            .cut()
            .saturating_add(bound.start.budgets.observation.convergence)
    );
    let exact = AttemptComputationCut {
        activation: ready.activation(),
        root: ready.root(),
        cut: ready.cut(),
        diagnostic_deadline: ready.diagnostic_deadline(),
        convergence_deadline: ready.convergence_deadline(),
        end: ready.end(),
    };
    let observer = collector.enable_profile_observation().unwrap();
    collector.begin_profile_observation(ready).unwrap();
    let mut records = OriginalRecords::from_collector(&collector, ready.activation());
    let mut foreign = selected_cut_for_tasks(collector.expected.iter().copied(), exact);
    foreign.ready.cut = foreign.ready.cut.saturating_add(Duration::from_nanos(1));
    let sources = DiagnosticSourceSnapshot::resolve(&foreign, &records).unwrap();
    assert!(
        collector
            .publish_core_cut(ready, &foreign, sources, unexpected_resolution)
            .is_err()
    );
    assert!(observer.current(ready).unwrap().is_none());
    // The original Ready gate also leaves the watch empty after a typed rich
    // resolver failure. The mixed three-Task payload/ref cases are above.
    let root_reference = information_reference(3, root);
    let mut rejected = selected_cut_for_tasks(collector.expected.iter().copied(), exact);
    rejected
        .tasks
        .iter_mut()
        .find(|(task, _)| *task == root)
        .unwrap()
        .1 = DiagnosticTaskObservation::Available(root_reference);
    let sources = DiagnosticSourceSnapshot::resolve(&rejected, &records).unwrap();
    assert!(matches!(
        collector.publish_core_cut(ready, &rejected, sources, |reference| {
            assert_eq!(reference, root_reference);
            Err(TaskExecutionError::UnknownOperation)
        }),
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert!(observer.current(ready).unwrap().is_none());
    let before = SplitSourceProfile {
        files_considered: 7,
        splits_emitted: 2,
        ..Default::default()
    };
    records.rows.insert(
        reference(1),
        OwnedPayload::SplitProfile(ready.activation(), root, before),
    );
    let mut first_cut = selected_cut_for_tasks(collector.expected.iter().copied(), exact);
    first_cut.sources.split = SplitDiagnosticObservation::Available(reference(1));
    let sources = DiagnosticSourceSnapshot::resolve(&first_cut, &records).unwrap();
    assert!(
        collector
            .publish_core_cut(ready, &first_cut, sources, unexpected_resolution)
            .unwrap()
    );
    let first = observer.current(ready).unwrap().unwrap();
    assert_eq!(first.ready, ready);
    assert_eq!(first.selected_at, first_cut.sources.selected_at);
    assert_ne!(first.selected_at, ready.cut());
    assert!(first.tasks.iter().all(|(_, row)| matches!(
        row,
        ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
    )));
    let mut later = selected_cut_for_tasks(collector.expected.iter().copied(), exact);
    later.closure = DiagnosticClosure::Cancelled;
    later.sources.selected_at = first.selected_at.saturating_add(Duration::from_millis(1));
    later.sources.runtime_filters.complete = true;
    later.sources.split = SplitDiagnosticObservation::SourceGone;
    later.tasks[0].1 = DiagnosticTaskObservation::Available(information_reference(4, root));
    let sources = DiagnosticSourceSnapshot::resolve(&later, &records).unwrap();
    assert!(
        !collector
            .publish_core_cut(ready, &later, sources, unexpected_resolution)
            .unwrap()
    );
    let second = observer.current(ready).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(second.selected_at, first_cut.sources.selected_at);
    assert!(
        matches!(second.split, ProfileSplitDiagnostic::Available(profile) if profile == before)
    );
    assert!(!second.runtime_filters.is_complete());
    assert!(collector.final_info_closed());
    drop((first, second, observer, collector, records, bound));
    let mut exit = original
        .take_exit_observer()
        .or_else(|| logical.exit.take())
        .expect("same original logical physical-exit observer");
    drop((received, original));
    let receipt = drive_profile_original(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(
        *installed.fixture_exit.observe_actual_exit().borrow(),
        "actual AP Actor and Scope removed"
    );
    assert_eq!(
        peer.single_root_result_progress(),
        (true, 1),
        "original End/ACK is independent of component publication"
    );
    drop((
        receipt, exit, installed, logical, output, cohort, window, capacity,
    ));
    drive_profile_original(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        supervisor.shutdown_until(std::time::Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    drop(supervisor);
    process.close_original_host_registration_for_fixture();
    fixture.finish().await;
    drop(fixture);
    business.release();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    tokio::time::timeout(Duration::from_secs(5), scope.wait_released())
        .await
        .unwrap();
    drop(scope);
    workload.close_admission();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    assert!(workload.shutdown().is_ok());
    data.drain_original_apply_until(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    data.drain_native_outgoing_until(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(peer.settle().await);
    assert!(process.original_registries_empty_for_fixture());
}
