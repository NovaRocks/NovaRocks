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

//! Publication executes only core-selected facts through the original owners.
//! The common Source retains this move-only record and serializes its metadata
//! jobs; these methods run outside the actor poller and every runtime lock.

use super::pumps::{DiagnosticRecords, PumpRecordError};
use crate::{
    control_runtime::actor::{FrontendActor, FrontendEffect},
    task_execution::{
        bridge::root_control::{RootControlRequests, reply_kind},
        error::TaskExecutionError,
        execution::ReleasedRuntimeFilterContributions,
        profile_diagnostics::{ProfileSplitDiagnostic, TaskFinalInfoCollector},
        protocol_facts::AttemptProtocolFactsPublisher,
    },
};
use novarocks_attempt_protocol::{
    effect::Publication,
    failure::{
        AttemptFailure, AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
    },
    input::{AttemptInput, ExecutorFact, PublicationApplied},
    publication::{DiagnosticPublication, DiagnosticSources, SplitDiagnosticObservation},
    start::PayloadRef,
};
use novarocks_control_runtime::actor_turn::{ActorRuntime, ActorTurnError, EffectTicket};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptRootFailure, AttemptToLogical,
};
use novarocks_execution_contract::{
    ParticipationRef, SafeDetail, TaskIdentity, failure::FailureProjection,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::{
    api::{ComputationReadyToken, QueryExecutionError},
    coordination::{AcceptedAttemptFailure, AcceptedRootStatusSender},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_spi::connector::read_stack::SplitSourceProfile;
use std::sync::Arc;

/// A read-only projection of the original Source's frozen graph binding.
/// This carries no grant, executable callback, or new sampling authority.
pub(crate) struct OriginalDiagnosticBinding<'a> {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) root: TaskIdentity,
    pub(crate) participations: &'a [ParticipationRef],
    pub(crate) fragments: &'a [(TaskIdentity, crate::query_execution::artifact::FragmentId)],
}

/// One already-registered immutable rich row, never a read of a live Source.
/// A unified lookup preserves the original ref-kind distinction. The telemetry
/// is the original canonical alias, extracted under only the registry's short
/// lookup lock; resolution does not hold a runtime lock or reparse a payload.
pub(crate) enum OriginalDiagnosticPayload {
    RuntimeFilter {
        activation: AttemptActivationIdentity,
        participation: ParticipationRef,
        telemetry: QueryTerminalProfileContributionTelemetry,
    },
    SplitProfile {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        profile: SplitSourceProfile,
    },
}

/// Implemented only by the original preadmitted Source's canonical records.
/// Records remain retained by its publication/result legs until this resolver
/// transfers the exact rich values to the original watch/consumer. Consuming
/// the core input alone cannot retire them. No fallback to current is legal.
pub(crate) trait OriginalDiagnosticRecords: Send + Sync {
    fn binding(&self) -> OriginalDiagnosticBinding<'_>;
    fn payload(&self, reference: PayloadRef) -> Option<OriginalDiagnosticPayload>;
}

/// Constructible only by resolving the core's complete immutable selection.
/// Resolution runs in original credentialed publication work, outside polling
/// and runtime locks. Numeric refs grant neither access nor retained capacity.
pub(crate) struct DiagnosticSourceSnapshot {
    ready: novarocks_coordination_contract::AttemptComputationCut,
    selection: DiagnosticSources,
    runtime_filters: ReleasedRuntimeFilterContributions,
    split: ProfileSplitDiagnostic,
    participations: Box<[ParticipationRef]>,
    fragments: Box<[(TaskIdentity, crate::query_execution::artifact::FragmentId)]>,
}
impl DiagnosticSourceSnapshot {
    pub(crate) fn resolve(
        cut: &DiagnosticPublication,
        records: &dyn OriginalDiagnosticRecords,
    ) -> Result<Self, TaskExecutionError> {
        let binding = records.binding();
        let invalid = || {
            TaskExecutionError::Schedule(
                "diagnostic selection does not resolve through its original canonical records"
                    .into(),
            )
        };
        if binding.activation != cut.ready.activation
            || binding.root != cut.ready.root
            || binding.activation.execution() != binding.root.query_execution_id()
            || cut.sources.selected_at < cut.ready.cut
        {
            return Err(invalid());
        }
        let positions = binding
            .participations
            .iter()
            .enumerate()
            .map(|(position, part)| (*part, position))
            .collect::<std::collections::BTreeMap<_, _>>();
        if positions.len() != binding.participations.len() {
            return Err(invalid());
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut previous_part = None;
        let mut contributions = Vec::with_capacity(cut.sources.runtime_filters.contributions.len());
        for &(backend, reference) in &cut.sources.runtime_filters.contributions {
            if !seen.insert(backend) {
                return Err(invalid());
            }
            let Some(OriginalDiagnosticPayload::RuntimeFilter {
                activation,
                participation,
                telemetry,
            }) = records.payload(reference)
            else {
                return Err(invalid());
            };
            if activation != binding.activation
                || participation.backend_process_id() != backend
                || !positions.contains_key(&participation)
            {
                return Err(invalid());
            }
            let position = positions[&participation];
            if previous_part.is_some_and(|previous| previous >= position) {
                return Err(invalid());
            }
            previous_part = Some(position);
            contributions.push((backend, telemetry));
        }
        use crate::task_execution::profile_diagnostics::ProfileSplitDiagnosticMissing as Missing;
        let split = match cut.sources.split {
            SplitDiagnosticObservation::Absent => {
                ProfileSplitDiagnostic::Available(SplitSourceProfile::default())
            }
            SplitDiagnosticObservation::Pending => {
                ProfileSplitDiagnostic::Missing(Missing::Pending)
            }
            SplitDiagnosticObservation::Failed => ProfileSplitDiagnostic::Missing(Missing::Failed),
            SplitDiagnosticObservation::SourceGone => {
                ProfileSplitDiagnostic::Missing(Missing::SourceGone)
            }
            SplitDiagnosticObservation::Available(reference) => {
                let Some(OriginalDiagnosticPayload::SplitProfile {
                    activation,
                    root,
                    profile,
                }) = records.payload(reference)
                else {
                    return Err(invalid());
                };
                if activation != binding.activation || root != binding.root {
                    return Err(invalid());
                }
                ProfileSplitDiagnostic::Available(profile)
            }
        };
        Ok(Self {
            ready: cut.ready,
            selection: cut.sources.clone(),
            runtime_filters: ReleasedRuntimeFilterContributions::from_selected_diagnostics(
                contributions,
                cut.sources.runtime_filters.complete,
                binding.participations.len(),
            ),
            split,
            participations: binding.participations.into(),
            fragments: binding.fragments.into(),
        })
    }

    pub(crate) fn into_selected(
        self,
        cut: &DiagnosticPublication,
        participations: &[ParticipationRef],
        fragments: &[(TaskIdentity, crate::query_execution::artifact::FragmentId)],
    ) -> Result<(ReleasedRuntimeFilterContributions, ProfileSplitDiagnostic), TaskExecutionError>
    {
        if self.ready != cut.ready
            || self.selection != cut.sources
            || self.participations.as_ref() != participations
            || self.fragments.as_ref() != fragments
        {
            return Err(TaskExecutionError::Schedule(
                "resolved diagnostics differ from the complete original Source/graph selection"
                    .into(),
            ));
        }
        Ok((self.runtime_filters, self.split))
    }
}

#[derive(Debug)]
pub(crate) enum PublicationError {
    MissingRootPublisher,
    MissingRootControlReply,
    MissingProfileObserver,
    MissingDiagnosticRecords,
    MissingOriginalComputationCut,
    MissingDiagnosticSources,
    ForeignOriginalComputationCut,
    Protocol(TaskExecutionError),
    Diagnostic(PumpRecordError),
    Root(QueryExecutionError),
}
impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(error) => write!(f, "original attempt publication failed: {error}"),
            Self::Diagnostic(error) => write!(f, "original diagnostic publication failed: {error}"),
            Self::Root(error) => write!(f, "original root publication failed: {error}"),
            other => write!(f, "attempt publication custody invariant failed: {other:?}"),
        }
    }
}
impl std::error::Error for PublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Diagnostic(error) => Some(error),
            Self::Root(error) => Some(error),
            _ => None,
        }
    }
}

/// The original host error stays owned until the terminal or disposal
/// custodian receives it. The Core only sees the compact typed projection.
pub(crate) enum OriginalHostFailure {
    Subscription(super::subscriptions::OriginalSubscriptionFailure),
    Task(TaskExecutionError),
    Publication(PublicationError),
}

impl OriginalHostFailure {
    pub(crate) fn preparation_input(&self) -> ExecutionFailureInput {
        match self {
            Self::Subscription(original) => original.preparation_input(),
            Self::Task(original) | Self::Publication(PublicationError::Protocol(original)) => {
                original.preparation_input()
            }
            Self::Publication(PublicationError::Root(original))
                if original.execution_failure().is_some() =>
            {
                original.execution_failure().unwrap().clone()
            }
            Self::Publication(_) => ExecutionFailureInput::new(
                QueryFailureReason::PreparationProtocol.failure_fact(),
                SafeDetail::truncating("Native attempt publication failed"),
                ExecutionFailureSite::Coordinator,
            ),
        }
    }
}

/// Holds the canonical publisher and the original immutable diagnostic lookup.
/// The paid Publication Source admits one publication at a time.
pub(crate) struct AttemptPublicationOwner {
    activation: AttemptActivationIdentity,
    record: tokio::sync::Mutex<PublicationRecord>,
    diagnostics: Option<Arc<dyn OriginalDiagnosticRecords>>,
    root: Option<Arc<RootControlRequests>>,
}

impl AttemptPublicationOwner {
    pub(crate) fn new(
        record: PublicationRecord,
        diagnostics: Option<Arc<dyn OriginalDiagnosticRecords>>,
        root: Option<Arc<RootControlRequests>>,
    ) -> Arc<Self> {
        let owner = Arc::new(Self {
            activation: record.activation,
            record: tokio::sync::Mutex::new(record),
            diagnostics,
            root,
        });
        if let Some(root) = &owner.root {
            root.bind_publisher(&owner);
        }
        owner
    }

    pub(crate) fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }

    pub(crate) fn matches_root(&self, binding: &super::observations::RootControlBinding) -> bool {
        self.root.as_ref().is_some_and(|root| root.matches(binding))
    }

    pub(crate) fn commit_root_reply(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: Arc<AttemptToLogical<AttemptFailure>>,
        projection: Option<(HostFailureRef, ExecutionFailureInput)>,
    ) -> Result<(), (ActorTurnError, Arc<AttemptToLogical<AttemptFailure>>)> {
        let Some(root) = &self.root else {
            return Err((ActorTurnError::PublicationTargetMismatch, original));
        };
        root.commit_reply(runtime, ticket, original, projection)
    }

    pub(crate) async fn deliver_committed_root(
        &self,
        token: novarocks_coordination_contract::AttemptRequestToken,
    ) -> Result<(), PublicationError> {
        self.root
            .as_ref()
            .expect("original root-control request owner")
            .deliver(token, &mut *self.record.lock().await)
    }

    pub(crate) async fn deliver_root_reply(
        &self,
        port: &AttemptToLogical<AttemptFailure>,
    ) -> Result<
        Option<(AttemptInput, Option<(HostFailureRef, OriginalHostFailure)>)>,
        PublicationError,
    > {
        use super::failure_publication::RootControlReplyKind;
        let (token, kind) = reply_kind(&port.body).expect("original root-control reply");
        let root = self
            .root
            .as_ref()
            .expect("original root-control request owner");
        let result = self.deliver_committed_root(token).await;
        if matches!(result, Err(PublicationError::MissingRootControlReply)) && root.owner_failed() {
            return Ok(None);
        }
        match kind {
            RootControlReplyKind::Seal => Ok(Some(Self::publication_result(
                port.action,
                result.map(|()| PublicationApplied::SealResult),
            ))),
            RootControlReplyKind::TerminalControl => result.map(|()| None),
        }
    }

    pub(crate) async fn publish(
        &self,
        effect: AttemptActionId,
        publication: &Publication,
    ) -> (AttemptInput, Option<(HostFailureRef, OriginalHostFailure)>) {
        Self::publication_result(effect, self.apply(publication).await)
    }

    fn publication_result(
        effect: AttemptActionId,
        result: Result<PublicationApplied, PublicationError>,
    ) -> (AttemptInput, Option<(HostFailureRef, OriginalHostFailure)>) {
        let (result, failure) = match result {
            Ok(applied) => (Ok(applied), None),
            Err(error) => {
                let reference = HostFailureRef(std::num::NonZeroU64::new(effect.get()).unwrap());
                let original = OriginalHostFailure::Publication(error);
                let projected = AttemptFailure::Host(HostProjectedFailure {
                    original: reference,
                    kind: HostFailureKind::Execution,
                    class: original.preparation_input().attempt_class(),
                    site: AttemptFailureSite::Attempt,
                });
                (Err(projected), Some((reference, original)))
            }
        };
        (
            AttemptInput::Executor(ExecutorFact::PublicationApplied { effect, result }),
            failure,
        )
    }

    async fn apply(
        &self,
        publication: &Publication,
    ) -> Result<PublicationApplied, PublicationError> {
        let sources = match publication {
            Publication::Diagnostics(cut) => {
                let records = self
                    .diagnostics
                    .as_deref()
                    .ok_or(PublicationError::MissingDiagnosticSources)?;
                Some(
                    DiagnosticSourceSnapshot::resolve(cut, records)
                        .map_err(PublicationError::Protocol)?,
                )
            }
            _ => None,
        };
        self.record.lock().await.publish(publication, sources)
    }
}

pub(crate) struct PublicationRecord {
    activation: AttemptActivationIdentity,
    root: Option<AcceptedRootStatusSender>,
    protocol: AttemptProtocolFactsPublisher,
    profile: Option<TaskFinalInfoCollector>,
    diagnostic_records: Option<Arc<DiagnosticRecords>>,
    ready: Option<ComputationReadyToken>,
    #[cfg(novarocks_dev_controls)]
    root_observer: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl PublicationRecord {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        root: AcceptedRootStatusSender,
        protocol: AttemptProtocolFactsPublisher,
        profile: Option<TaskFinalInfoCollector>,
        diagnostic_records: Option<Arc<DiagnosticRecords>>,
    ) -> Self {
        Self {
            activation,
            root: Some(root),
            protocol,
            profile,
            diagnostic_records,
            ready: None,
            #[cfg(novarocks_dev_controls)]
            root_observer: None,
        }
    }
    /// The bridge transfers the original token before submitting convergence.
    /// A value projection cannot replace or remint this original token.
    pub(crate) fn register_computation_cut(
        &mut self,
        ready: ComputationReadyToken,
    ) -> Result<(), PublicationError> {
        if ready.activation() != self.activation || self.ready.is_some_and(|held| held != ready) {
            return Err(PublicationError::ForeignOriginalComputationCut);
        }
        self.ready = Some(ready);
        Ok(())
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn register_root_observer(
        &mut self,
        observer: crate::preparation_diagnostics::StatementObservationHandle,
    ) {
        assert!(
            self.root_observer.is_none(),
            "the original root observer is installed once"
        );
        self.root_observer = Some(observer);
    }
    pub(crate) fn publish(
        &mut self,
        publication: &Publication,
        mut sources: Option<DiagnosticSourceSnapshot>,
    ) -> Result<PublicationApplied, PublicationError> {
        match publication {
            Publication::RootStatus(projection) => {
                let failure = match &projection.failure {
                    AttemptRootFailure::None => AcceptedAttemptFailure::None,
                    AttemptRootFailure::DerivedPending => AcceptedAttemptFailure::DerivedPending,
                    AttemptRootFailure::Authoritative(failure) => {
                        AcceptedAttemptFailure::Authoritative(failure.clone())
                    }
                };
                self.root
                    .as_ref()
                    .ok_or(PublicationError::MissingRootPublisher)?
                    .publish_attempt_observation(projection.status.clone(), failure)
                    .map_err(PublicationError::Root)?;
                Ok(PublicationApplied::RootStatus)
            }
            Publication::ProtocolBatch(batch) => {
                let published = self
                    .protocol
                    .publish_core_batch(self.activation, batch)
                    .map_err(PublicationError::Protocol)?;
                Ok(PublicationApplied::ProtocolBatch { published })
            }
            Publication::FinalTaskInfo(reference) => {
                let profile = self
                    .profile
                    .as_mut()
                    .ok_or(PublicationError::MissingProfileObserver)?;
                let info = self
                    .diagnostic_records
                    .as_ref()
                    .ok_or(PublicationError::MissingDiagnosticRecords)?
                    .take_information(*reference)
                    .map_err(PublicationError::Diagnostic)?;
                if info.final_status().identity() != reference.task() {
                    return Err(PublicationError::Protocol(TaskExecutionError::Schedule(
                        "core diagnostic payload names a foreign Task".into(),
                    )));
                }
                profile.collected.push(Arc::from(info));
                Ok(PublicationApplied::FinalTaskInfo)
            }
            Publication::Diagnostics(cut) => {
                let ready = self
                    .ready
                    .ok_or(PublicationError::MissingOriginalComputationCut)?;
                let sources = sources
                    .take()
                    .ok_or(PublicationError::MissingDiagnosticSources)?;
                self.profile
                    .as_mut()
                    .ok_or(PublicationError::MissingProfileObserver)?
                    .publish_core_cut(ready, cut, sources, |reference| {
                        self.diagnostic_records
                            .as_ref()
                            .ok_or_else(|| {
                                TaskExecutionError::Schedule(
                                    "diagnostic selection has no original canonical records".into(),
                                )
                            })?
                            .take_information(reference)
                            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))
                    })
                    .map_err(PublicationError::Protocol)?;
                Ok(PublicationApplied::Diagnostics)
            }
            Publication::DevRootObservation { status, installed } => {
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = &self.root_observer {
                    observer.publish_root_status(
                        crate::preparation_diagnostics::StatementRootStatusObservation {
                            root: status.identity(),
                            installed: *installed,
                            status_version: status.version(),
                        },
                    );
                }
                #[cfg(not(novarocks_dev_controls))]
                let _ = (status, installed);
                Ok(PublicationApplied::DevRootObservation)
            }
        }
    }
    /// The bridge consumes this exact publisher for the original seal reply.
    /// There is no Clone path and no replacement root publisher is constructed.
    pub(crate) fn take_root_for_seal(
        &mut self,
    ) -> Result<AcceptedRootStatusSender, PublicationError> {
        self.root
            .take()
            .ok_or(PublicationError::MissingRootPublisher)
    }
}

#[cfg(test)]
pub(super) mod tests;
