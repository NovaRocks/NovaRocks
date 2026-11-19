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

//! The original asynchronous final-info collector, independent of business readiness.

#[cfg(all(test, novarocks_dev_controls))]
use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::read_stack::SplitSourceProfile;

use super::graph::{TaskGraph, TaskNode};
use crate::native::task_result_transport::TaskResultTransport;
use crate::wake::StatusIntakeWake;
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{FinalTaskInfo, OperationOutcome, TaskIdentity};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::api::ComputationReadyToken;
use novarocks_types::BackendProcessId;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProfileDiagnosticClosure {
    FrontierSettled,
    ObservationDeadline,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProfileDiagnosticMissing {
    Unavailable(OperationOutcome),
    ReadFailed,
    TerminalMismatch,
    ReadPending,
    TaskNotTerminal,
    NotRead,
}

#[derive(Clone, Debug)]
pub(crate) enum ProfileTaskDiagnostic {
    Available(Arc<FinalTaskInfo>),
    Missing(ProfileDiagnosticMissing),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProfileSplitDiagnosticMissing {
    Pending,
    Failed,
    SourceGone,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProfileSplitDiagnostic {
    Available(SplitSourceProfile),
    Missing(ProfileSplitDiagnosticMissing),
}

/// The release-carried runtime-filter contributions of one attempt.
///
/// Contributions remain selected canonical diagnostic rows. Completeness is
/// separate from row count and never substitutes for a participation's exit.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReleasedRuntimeFilterContributions {
    contributions: Vec<(BackendProcessId, QueryTerminalProfileContributionTelemetry)>,
    complete: bool,
    #[cfg(test)]
    participations: usize,
}

impl ReleasedRuntimeFilterContributions {
    /// Assemble only previously selected canonical diagnostic rows. The caller
    /// resolves them under the original Source's admitted publication work.
    pub(crate) fn from_selected_diagnostics(
        contributions: Vec<(BackendProcessId, QueryTerminalProfileContributionTelemetry)>,
        complete: bool,
        _participations: usize,
    ) -> Self {
        Self {
            contributions,
            complete,
            #[cfg(test)]
            participations: _participations,
        }
    }

    pub fn contributions(
        &self,
    ) -> &[(BackendProcessId, QueryTerminalProfileContributionTelemetry)] {
        &self.contributions
    }

    /// Whether every participation this attempt placed has answered its release.
    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

#[derive(Debug)]
pub(crate) struct ProfileDiagnosticSnapshot {
    pub(crate) ready: ComputationReadyToken,
    pub(crate) closure: ProfileDiagnosticClosure,
    #[allow(
        dead_code,
        reason = "The frozen profile records the original observation cutoff with its evidence."
    )]
    pub(crate) selected_at: MonotonicInstant,
    pub(crate) runtime_filters: ReleasedRuntimeFilterContributions,
    pub(crate) split: ProfileSplitDiagnostic,
    pub(crate) tasks: Box<[(TaskIdentity, ProfileTaskDiagnostic)]>,
    /// Actual graph attribution, in the same original expected Task order.
    /// Stage IDs are not fragment IDs and must never be reinterpreted as one.
    pub(crate) fragments: Box<[(TaskIdentity, crate::query_execution::artifact::FragmentId)]>,
}

impl ProfileDiagnosticSnapshot {
    pub(crate) fn complete(&self) -> bool {
        self.tasks
            .iter()
            .all(|(_, fact)| matches!(fact, ProfileTaskDiagnostic::Available(_)))
            && self.runtime_filters.is_complete()
            && self.runtime_filters.contributions().iter().all(|(_, telemetry)| {
                matches!(telemetry.as_proto().telemetry, Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(_)))
            })
            && matches!(self.split, ProfileSplitDiagnostic::Available(_))
    }
}

/// A read-only view of the original activation's one immutable diagnostic
/// snapshot. Publishing this view does not retire a pending RPC or its owner.
pub(crate) struct ProfileDiagnosticObserver {
    snapshot: watch::Receiver<Option<Arc<ProfileDiagnosticSnapshot>>>,
}

impl ProfileDiagnosticObserver {
    /// Observe the SAME immutable watch and its original snapshot Arc after
    /// the test releases its Ready/credential custody. This clones no data,
    /// publisher, Source, work, grant, budget owner or Ready authority.
    #[cfg(test)]
    pub(crate) fn clone_for_fixture(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
        }
    }

    pub(crate) fn current(
        &self,
        ready: ComputationReadyToken,
    ) -> Result<Option<Arc<ProfileDiagnosticSnapshot>>, super::error::TaskExecutionError> {
        let current = self.snapshot.borrow().clone();
        if current
            .as_ref()
            .is_some_and(|snapshot| snapshot.ready != ready)
        {
            return Err(super::error::TaskExecutionError::Schedule(
                "profile diagnostics belong to another computation cut".to_owned(),
            ));
        }
        Ok(current)
    }

    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot.changed().await
    }
}

struct ProfileDiagnosticPublication {
    ready: Option<ComputationReadyToken>,
    snapshot: watch::Sender<Option<Arc<ProfileDiagnosticSnapshot>>>,
}

/// Reads the bounded final info of every terminal task of this attempt.
///
/// Final info is observation only: success and failure are decided by
/// `TaskStatus` alone, so a task that cannot answer costs diagnostics and
/// nothing else. That is why a failed read is logged and skipped rather than
/// failing a query that already completed.
pub(crate) struct TaskFinalInfoCollector {
    pub(crate) collected: Vec<Arc<FinalTaskInfo>>,
    expected: Arc<[TaskIdentity]>,
    expected_fragments: Arc<[(TaskIdentity, crate::query_execution::artifact::FragmentId)]>,
    expected_participations: Arc<[novarocks_execution_contract::ParticipationRef]>,
    root: TaskIdentity,
    publication: Option<ProfileDiagnosticPublication>,
    enabled: bool,
}
/// Borrowed original frozen metadata. These private values grant no Ready,
/// source access, publication authority, capacity or new selection policy.
struct DiagnosticSelectionBinding<'a> {
    tasks: &'a [TaskIdentity],
    participations: &'a [novarocks_execution_contract::ParticipationRef],
    fragments: &'a [(TaskIdentity, crate::query_execution::artifact::FragmentId)],
}

fn resolve_selected_diagnostic_sources(
    original: novarocks_coordination_contract::AttemptComputationCut,
    binding: DiagnosticSelectionBinding<'_>,
    cut: &novarocks_attempt_protocol::publication::DiagnosticPublication,
    sources: super::host::publication::DiagnosticSourceSnapshot,
) -> Result<
    (ReleasedRuntimeFilterContributions, ProfileSplitDiagnostic),
    super::error::TaskExecutionError,
> {
    let exact = cut.ready.activation == original.activation
        && cut.ready.root == original.root
        && cut.ready.cut == original.cut
        && cut.ready.diagnostic_deadline == original.diagnostic_deadline
        && cut.ready.convergence_deadline == original.convergence_deadline
        && cut.ready.end == original.end;
    if !exact || cut.tasks.len() != binding.tasks.len() {
        return Err(super::error::TaskExecutionError::Schedule(
            "core diagnostic cut differs from its original computation/Task binding".to_owned(),
        ));
    }
    if cut
        .tasks
        .iter()
        .zip(binding.tasks.iter())
        .any(|((task, _), expected)| task != expected)
    {
        return Err(super::error::TaskExecutionError::Schedule(
            "core diagnostic publication differs from its exact frozen Task order".to_owned(),
        ));
    }
    sources.into_selected(cut, binding.participations, binding.fragments)
}

/// Expand only after the caller validates its original Ready and observes
/// that no immutable snapshot was published. Errors consume no later row.
fn expand_selected_diagnostic_rows(
    cut: &novarocks_attempt_protocol::publication::DiagnosticPublication,
    mut resolve: impl FnMut(
        novarocks_attempt_protocol::input::FinalTaskInfoRef,
    ) -> Result<Box<FinalTaskInfo>, super::error::TaskExecutionError>,
) -> Result<Box<[(TaskIdentity, ProfileTaskDiagnostic)]>, super::error::TaskExecutionError> {
    use novarocks_attempt_protocol::publication as core;
    let mut tasks = Vec::with_capacity(cut.tasks.len());
    for &(task, observation) in &cut.tasks {
        let observation = match observation {
            core::DiagnosticTaskObservation::Available(reference) => {
                if reference.task() != task {
                    return Err(super::error::TaskExecutionError::Schedule(
                        "core diagnostic reference names a foreign Task".to_owned(),
                    ));
                }
                let info = resolve(reference)?;
                if info.final_status().identity() != task {
                    return Err(super::error::TaskExecutionError::Schedule(
                        "core diagnostic payload names a foreign Task".to_owned(),
                    ));
                }
                ProfileTaskDiagnostic::Available(Arc::from(info))
            }
            core::DiagnosticTaskObservation::Missing(missing) => {
                ProfileTaskDiagnostic::Missing(match missing {
                    core::DiagnosticMissing::Unavailable(outcome) => {
                        ProfileDiagnosticMissing::Unavailable(outcome)
                    }
                    core::DiagnosticMissing::ReadFailed => ProfileDiagnosticMissing::ReadFailed,
                    core::DiagnosticMissing::TerminalMismatch => {
                        ProfileDiagnosticMissing::TerminalMismatch
                    }
                    core::DiagnosticMissing::ReadPending => ProfileDiagnosticMissing::ReadPending,
                    core::DiagnosticMissing::TaskNotTerminal => {
                        ProfileDiagnosticMissing::TaskNotTerminal
                    }
                    core::DiagnosticMissing::NotRead => ProfileDiagnosticMissing::NotRead,
                })
            }
        };
        tasks.push((task, observation));
    }
    Ok(tasks.into_boxed_slice())
}

impl TaskFinalInfoCollector {
    pub(crate) fn from_graph(
        transport: Arc<dyn TaskResultTransport>,
        enabled: bool,
        graph: &TaskGraph,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Self {
        let _ = (transport, wake);
        let expected: Arc<[TaskIdentity]> = if enabled {
            graph.tasks().map(TaskNode::identity).collect()
        } else {
            Arc::from([])
        };
        Self {
            collected: Vec::new(),
            expected,
            expected_fragments: graph
                .tasks()
                .map(|task| (task.identity(), task.fragment_id()))
                .collect(),
            expected_participations: graph.participations().copied().collect(),
            root: graph.root_identity(),
            publication: None,
            enabled,
        }
    }

    pub(crate) fn enable_profile_observation(
        &mut self,
    ) -> Result<ProfileDiagnosticObserver, super::error::TaskExecutionError> {
        if !self.enabled || self.publication.is_some() {
            return Err(super::error::TaskExecutionError::Schedule(
                "profile diagnostic observation is unavailable or already attached".to_owned(),
            ));
        }
        let (snapshot, receiver) = watch::channel(None);
        self.publication = Some(ProfileDiagnosticPublication {
            ready: None,
            snapshot,
        });
        Ok(ProfileDiagnosticObserver { snapshot: receiver })
    }

    pub(crate) fn begin_profile_observation(
        &mut self,
        ready: ComputationReadyToken,
    ) -> Result<(), super::error::TaskExecutionError> {
        if ready.root() != self.root {
            return Err(super::error::TaskExecutionError::Schedule(
                "profile computation cut has a foreign root".to_owned(),
            ));
        }
        let publication = self.publication.as_mut().ok_or_else(|| {
            super::error::TaskExecutionError::Schedule(
                "profile computation cut has no original diagnostic observer".to_owned(),
            )
        })?;
        if publication.ready.is_some_and(|original| original != ready) {
            return Err(super::error::TaskExecutionError::Schedule(
                "profile diagnostic observation cannot replace its original computation cut"
                    .to_owned(),
            ));
        }
        publication.ready = Some(ready);
        Ok(())
    }

    /// The current collector owns only the selected immutable publication.
    /// Final-info I/O and observation-window policy remain in their Sources/Core.
    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fn final_info_closed(&self) -> bool {
        self.publication
            .as_ref()
            .is_some_and(|publication| publication.snapshot.borrow().is_some())
    }

    /// Publish the already selected immutable core cut. Observation-window
    /// policy and final-info validation belong to the core, not this owner seam.
    /// RF/split were selected at the core boundary and resolved exclusively
    /// from retained original immutable rows, never from later live Sources.
    pub(crate) fn publish_core_cut(
        &mut self,
        ready: ComputationReadyToken,
        cut: &novarocks_attempt_protocol::publication::DiagnosticPublication,
        sources: super::host::publication::DiagnosticSourceSnapshot,
        mut resolve: impl FnMut(
            novarocks_attempt_protocol::input::FinalTaskInfoRef,
        ) -> Result<Box<FinalTaskInfo>, super::error::TaskExecutionError>,
    ) -> Result<bool, super::error::TaskExecutionError> {
        use novarocks_attempt_protocol::publication as core;
        let (runtime_filters, split) = resolve_selected_diagnostic_sources(
            novarocks_coordination_contract::AttemptComputationCut {
                activation: ready.activation(),
                root: ready.root(),
                cut: ready.cut(),
                diagnostic_deadline: ready.diagnostic_deadline(),
                convergence_deadline: ready.convergence_deadline(),
                end: ready.end(),
            },
            DiagnosticSelectionBinding {
                tasks: &self.expected,
                participations: &self.expected_participations,
                fragments: &self.expected_fragments,
            },
            cut,
            sources,
        )?;
        self.begin_profile_observation(ready)?;
        let publication = self
            .publication
            .as_ref()
            .expect("original diagnostic observer was validated");
        if publication.snapshot.borrow().is_some() {
            return Ok(false);
        }
        let tasks = expand_selected_diagnostic_rows(cut, &mut resolve)?;
        let closure = match cut.closure {
            core::DiagnosticClosure::FrontierSettled => ProfileDiagnosticClosure::FrontierSettled,
            core::DiagnosticClosure::ObservationDeadline => {
                ProfileDiagnosticClosure::ObservationDeadline
            }
            core::DiagnosticClosure::Cancelled => ProfileDiagnosticClosure::Cancelled,
        };
        publication
            .snapshot
            .send_replace(Some(Arc::new(ProfileDiagnosticSnapshot {
                ready,
                closure,
                selected_at: cut.sources.selected_at,
                runtime_filters,
                split,
                tasks,
                fragments: self.expected_fragments.as_ref().into(),
            })));
        Ok(true)
    }
}

#[cfg(test)]
#[path = "profile_diagnostics/property_tests.rs"]
mod property_tests;

#[cfg(all(test, novarocks_dev_controls))]
mod core_publication_tests;
