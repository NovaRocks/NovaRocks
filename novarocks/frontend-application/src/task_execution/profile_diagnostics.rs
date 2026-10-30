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

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::query_execution::split_assignment_round::{
    SplitAssignmentProfileRead, SplitAssignmentProfileSource,
};
use novarocks_spi::connector::read_stack::SplitSourceProfile;

use super::{
    completion::accept_final_info, graph::TaskNode, round::TaskRound,
    status_intake::StatusIntakeWake,
};
use crate::native::fragment_transport::{FinalTaskInfoRead, TaskResultTransport};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{FinalTaskInfo, OperationOutcome, TaskIdentity};
use novarocks_query_application::api::ComputationReadyToken;
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

#[derive(Debug)]
pub(crate) struct ProfileDiagnosticSnapshot {
    pub(crate) ready: ComputationReadyToken,
    pub(crate) closure: ProfileDiagnosticClosure,
    pub(crate) runtime_filters: super::execution::ReleasedRuntimeFilterContributions,
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
    transport: Arc<dyn TaskResultTransport>,
    pub(crate) collected: Vec<Arc<FinalTaskInfo>>,
    pub(crate) unread: BTreeSet<TaskIdentity>,
    terminal_cursor: usize,
    expected: Arc<[TaskIdentity]>,
    root: TaskIdentity,
    missing: BTreeMap<TaskIdentity, ProfileDiagnosticMissing>,
    publication: Option<ProfileDiagnosticPublication>,
    final_info_closure: Option<ProfileDiagnosticClosure>,
    split_source: Option<SplitAssignmentProfileSource>,
    pending: Option<FinalTaskInfoReadJob>,
    enabled: bool,
    accepting: bool,
    wake: Arc<dyn StatusIntakeWake>,
}
struct FinalTaskInfoReadJob {
    observed: novarocks_execution_contract::TaskStatus,
    future: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<FinalTaskInfoRead, String>> + Send>,
    >,
}
impl TaskFinalInfoCollector {
    pub(crate) fn new(
        transport: Arc<dyn TaskResultTransport>,
        enabled: bool,
        round: &TaskRound,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Self {
        let expected: Arc<[TaskIdentity]> = if enabled {
            round
                .execution()
                .graph()
                .tasks()
                .map(TaskNode::identity)
                .collect()
        } else {
            Arc::from([])
        };
        Self {
            transport,
            collected: Vec::new(),
            unread: expected.iter().copied().collect(),
            terminal_cursor: 0,
            expected,
            root: round.root_task(),
            missing: BTreeMap::new(),
            publication: None,
            final_info_closure: None,
            split_source: None,
            pending: None,
            enabled,
            accepting: true,
            wake,
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

    pub(crate) fn set_split_profile_source(
        &mut self,
        source: Option<SplitAssignmentProfileSource>,
    ) {
        assert!(
            self.publication.as_ref().is_none_or(|p| p.ready.is_none()),
            "split source is fixed before the computation cut"
        );
        self.split_source = source;
    }

    /// Final-info completion releases the remote hold independently of the
    /// final user snapshot, whose RF facts arrive on that very Release ACK.
    pub(crate) fn final_info_closed(&self) -> bool {
        !self.accepting
    }

    fn split_diagnostic(&self) -> ProfileSplitDiagnostic {
        use ProfileSplitDiagnostic as D;
        use ProfileSplitDiagnosticMissing as M;
        match self
            .split_source
            .as_ref()
            .map(SplitAssignmentProfileSource::current)
        {
            None => D::Available(SplitSourceProfile::default()),
            Some(SplitAssignmentProfileRead::Available(profile)) => D::Available(profile),
            Some(SplitAssignmentProfileRead::Pending) => D::Missing(M::Pending),
            Some(SplitAssignmentProfileRead::Failed) => D::Missing(M::Failed),
            Some(SplitAssignmentProfileRead::SourceGone) => D::Missing(M::SourceGone),
        }
    }

    pub(crate) fn observation_deadline(&self) -> Option<MonotonicInstant> {
        self.publication.as_ref().and_then(|publication| {
            publication
                .snapshot
                .borrow()
                .is_none()
                .then_some(publication.ready)
                .flatten()
                .map(|ready| ready.diagnostic_deadline())
        })
    }

    /// Stop new reads before polling at an expired/cancelled cut. The same
    /// pending future still receives every poll until its actual return.
    pub(crate) fn poll_profile_observation(
        &mut self,
        round: &TaskRound,
        now: MonotonicInstant,
        cancelled: bool,
        cx: &mut std::task::Context<'_>,
    ) {
        let deadline = self.observation_deadline();
        if self.final_info_closure.is_none()
            && let Some(deadline) = deadline
        {
            let closure = if cancelled {
                Some(ProfileDiagnosticClosure::Cancelled)
            } else if now.has_reached(deadline) {
                Some(ProfileDiagnosticClosure::ObservationDeadline)
            } else {
                None
            };
            if let Some(closure) = closure {
                self.accepting = false;
                self.final_info_closure = Some(closure);
                // Freeze what was received before the cut, even when the
                // original pending RPC becomes Ready in this very poll.
                self.publish_snapshot(round, closure);
            }
        }
        // A cut never detaches the original pending RPC.
        self.poll_observe(round, cx);
        if self.final_info_closure.is_none() && self.all_terminal_observed() && deadline.is_some() {
            self.accepting = false;
            self.final_info_closure = Some(ProfileDiagnosticClosure::FrontierSettled);
        }
        if let Some(deadline) = deadline {
            let expired = now.has_reached(deadline);
            let releases_complete = round.execution().graph().contexts().all(|context| {
                round
                    .execution()
                    .owner(*context)
                    .is_some_and(|owner| owner.is_released())
            });
            let split_finished = !matches!(
                self.split_diagnostic(),
                ProfileSplitDiagnostic::Missing(ProfileSplitDiagnosticMissing::Pending)
            );
            if cancelled
                || expired
                || (self.final_info_closed() && releases_complete && split_finished)
            {
                let closure = if cancelled {
                    ProfileDiagnosticClosure::Cancelled
                } else if expired {
                    ProfileDiagnosticClosure::ObservationDeadline
                } else {
                    ProfileDiagnosticClosure::FrontierSettled
                };
                self.publish_snapshot(round, closure);
            }
        }
    }

    fn publish_snapshot(&mut self, round: &TaskRound, closure: ProfileDiagnosticClosure) {
        let Some(publication) = self.publication.as_ref() else {
            return;
        };
        let Some(ready) = publication.ready else {
            return;
        };
        if publication.snapshot.borrow().is_some() {
            return;
        }
        self.accepting = false;
        let available = self
            .collected
            .iter()
            .map(|info| (info.final_status().identity(), Arc::clone(info)))
            .collect::<BTreeMap<_, _>>();
        let pending = self.pending.as_ref().map(|job| job.observed.identity());
        let tasks =
            self.expected
                .iter()
                .map(|&identity| {
                    let fact = if let Some(info) = available.get(&identity) {
                        ProfileTaskDiagnostic::Available(Arc::clone(info))
                    } else {
                        let missing = self.missing.get(&identity).copied().unwrap_or_else(|| {
                            if pending == Some(identity) {
                                ProfileDiagnosticMissing::ReadPending
                            } else if !round.execution().task(identity.task_id()).is_some_and(
                                |task| task.identity() == identity && task.is_terminal(),
                            ) {
                                ProfileDiagnosticMissing::TaskNotTerminal
                            } else {
                                ProfileDiagnosticMissing::NotRead
                            }
                        });
                        ProfileTaskDiagnostic::Missing(missing)
                    };
                    (identity, fact)
                })
                .collect();
        publication
            .snapshot
            .send_replace(Some(Arc::new(ProfileDiagnosticSnapshot {
                ready,
                closure,
                runtime_filters: round.execution().released_runtime_filter_contributions(),
                split: self.split_diagnostic(),
                tasks,
                fragments: round
                    .execution()
                    .graph()
                    .tasks()
                    .map(|task| (task.identity(), task.fragment_id()))
                    .collect(),
            })));
    }
    pub(crate) fn stop_starting(&mut self) {
        self.accepting = false;
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.pending.is_none()
    }
    pub(crate) fn all_terminal_observed(&self) -> bool {
        // Only the exact frozen Tasks whose terminal was observed can leave
        // this frontier. Disabled diagnostics do not manufacture that proof.
        self.enabled && self.unread.is_empty() && self.pending.is_none()
    }
    /// At most one bounded final-info read is in flight. Starting it records
    /// its exact observed Task and original transport in self before Pending.
    pub(crate) fn poll_observe(&mut self, round: &TaskRound, cx: &mut std::task::Context<'_>) {
        use std::task::Poll;
        if !self.enabled {
            return;
        }
        if self.pending.is_none() && self.accepting {
            // The original status reducer indexes each frozen Task's first
            // terminal once. A cursor consumes only newly indexed Tasks; idle
            // polling and sparsely arriving terminals never rotate N unread slots.
            let terminals = round.execution().accepted_terminal_tasks();
            let mut next = None;
            while let Some(&identity) = terminals.get(self.terminal_cursor) {
                self.terminal_cursor += 1;
                if !self.unread.remove(&identity) {
                    continue;
                }
                let observed = round
                    .execution()
                    .task(identity.task_id())
                    .filter(|task| task.identity() == identity && task.is_terminal())
                    .and_then(|task| task.status())
                    .cloned();
                if let Some(observed) = observed {
                    next = Some(observed);
                    break;
                }
                // The monotonic original status owner cannot normally regress
                // a terminal. Keep any contradictory observation explicit.
                self.missing
                    .insert(identity, ProfileDiagnosticMissing::TaskNotTerminal);
            }
            if let Some(observed) = next {
                let identity = observed.identity();
                let transport = Arc::clone(&self.transport);
                self.pending = Some(FinalTaskInfoReadJob {
                    observed,
                    future: Box::pin(async move { transport.final_task_info(identity).await }),
                });
            }
        }
        let Some(job) = self.pending.as_mut() else {
            return;
        };
        let result = match job.future.as_mut().poll(cx) {
            Poll::Pending => return,
            Poll::Ready(result) => result,
        };
        let job = self.pending.take().expect("same original final-info read");
        let identity = job.observed.identity();
        match result {
            Ok(FinalTaskInfoRead::Available(info)) => match accept_final_info(&job.observed, &info)
            {
                Ok(()) => self.collected.push(Arc::from(info)),
                Err(error) => {
                    self.missing
                        .insert(identity, ProfileDiagnosticMissing::TerminalMismatch);
                    tracing::warn!(task = %identity, %error, "Final task info disagrees with the observed terminal")
                }
            },
            Ok(FinalTaskInfoRead::Unavailable(outcome)) => {
                self.missing
                    .insert(identity, ProfileDiagnosticMissing::Unavailable(outcome));
                tracing::debug!(task = %identity, ?outcome, "Final task info is unavailable")
            }
            Err(error) => {
                self.missing
                    .insert(identity, ProfileDiagnosticMissing::ReadFailed);
                tracing::warn!(task = %identity, %error, "Final task info could not be read")
            }
        }
        if self.accepting && !self.unread.is_empty() {
            // The original waiter may have consumed the RPC wake before this
            // Ready poll. Continue serial collection without a timer turn.
            self.wake.wake();
        }
    }
}

#[cfg(test)]
#[path = "profile_diagnostics/property_tests.rs"]
mod property_tests;
