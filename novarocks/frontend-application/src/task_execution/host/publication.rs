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
    query_execution::{
        lifecycle_diagnostics::{
            RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
        },
        runtime_filter_terminal_rollup::{
            RuntimeFilterParticipantTerminalTelemetry, project_release_contribution,
            rollup_projected_release_contributions,
        },
    },
    task_execution::{
        bridge::root_control::{RootControlRequests, reply_kind},
        error::TaskExecutionError,
        intent::{AckPayload, OperationAcknowledgement},
        profile_diagnostics::{
            ProfileSplitDiagnostic, ReleasedRuntimeFilterContributions, TaskFinalInfoCollector,
        },
        protocol_facts::AttemptProtocolFactsPublisher,
    },
};
use novarocks_attempt_protocol::{
    effect::{Publication, PumpKind},
    failure::{
        AttemptFailure, AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
    },
    input::{AttemptInput, ExecutorFact, PublicationApplied},
    publication::{
        DiagnosticPublication, DiagnosticSources, RuntimeFilterDiagnostic,
        SplitDiagnosticObservation,
    },
    start::{AttemptStart, PayloadRef},
};
use novarocks_control_runtime::actor_turn::{ActorRuntime, ActorTurnError, EffectTicket};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptRootFailure, AttemptToLogical,
};
use novarocks_execution_contract::{
    ParticipationRef, ReleaseOutcome, SafeDetail, TaskIdentity, failure::FailureProjection,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::{
    api::{ComputationReadyToken, QueryExecutionError},
    coordination::{AcceptedAttemptFailure, AcceptedRootStatusSender},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_spi::connector::read_stack::SplitSourceProfile;
use novarocks_types::BackendProcessId;
use std::sync::{Arc, Mutex, OnceLock};

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

#[derive(Clone, Copy)]
pub(crate) enum DiagnosticPayloadAddress {
    RuntimeFilter {
        backend: BackendProcessId,
        reference: PayloadRef,
    },
    SplitProfile(PayloadRef),
}

/// Implemented only by the original preadmitted Source's canonical records.
/// Records remain retained by its publication/result legs until this resolver
/// transfers the exact rich values to the original watch/consumer. Consuming
/// the core input alone cannot retire them. No fallback to current is legal.
pub(crate) trait OriginalDiagnosticRecords: Send + Sync {
    fn binding(&self) -> OriginalDiagnosticBinding<'_>;
    fn payload(&self, address: DiagnosticPayloadAddress) -> Option<OriginalDiagnosticPayload>;
    fn publication_complete(&self) {}
}

type SplitProfileLookup = (PayloadRef, Arc<OnceLock<SplitSourceProfile>>);

struct ReleaseDiagnostic {
    reference: PayloadRef,
    telemetry: QueryTerminalProfileContributionTelemetry,
}

/// Move the original reply aliases to the credentialed disposal executor.
pub(crate) struct RetainedReleaseDiagnostics(Box<[Option<ReleaseDiagnostic>]>);

struct ReleaseDiagnosticRows {
    profile: Option<RetainedReleaseDiagnostics>,
    lifecycle: Option<Box<[Option<LifecycleReleaseDiagnostic>]>>,
}

struct LifecycleReleaseDiagnostic {
    reference: PayloadRef,
    telemetry: RuntimeFilterParticipantTerminalTelemetry,
}

/// Exact immutable manifest binding and one preallocated release cell per
/// participant. A terminal release is first-wins for that participant, and
/// only the Core can select its reference for publication.
pub(crate) struct AttemptDiagnosticSources {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    participations: Box<[ParticipationRef]>,
    fragments: Box<[(TaskIdentity, crate::query_execution::artifact::FragmentId)]>,
    split: Option<SplitProfileLookup>,
    observe_lifecycle: bool,
    releases: Mutex<ReleaseDiagnosticRows>,
}

impl AttemptDiagnosticSources {
    pub(crate) fn new(start: &AttemptStart, split: Option<SplitProfileLookup>) -> Arc<Self> {
        let plan = &start.plan;
        let observe_lifecycle = start.observe_lifecycle_diagnostics;
        let expected_split = plan.pumps.iter().find_map(|&(reference, kind)| {
            (kind == PumpKind::SplitAssignment).then_some(reference)
        });
        assert_eq!(
            expected_split,
            split.as_ref().map(|(reference, _)| *reference)
        );
        let participations: Box<_> = plan.establish_material.keys().copied().collect();
        assert!(
            participations
                .windows(2)
                .all(|pair| pair[0].backend_process_id() < pair[1].backend_process_id()),
            "the original attempt has one participation per backend"
        );
        let profile = start.activation.policy.retain_diagnostics.then(|| {
            RetainedReleaseDiagnostics(
                std::iter::repeat_with(|| None)
                    .take(participations.len())
                    .collect(),
            )
        });
        let lifecycle = observe_lifecycle.then(|| {
            std::iter::repeat_with(|| None)
                .take(participations.len())
                .collect()
        });
        Arc::new(Self {
            activation: start.activation.identity,
            root: plan.graph.root,
            participations,
            fragments: plan
                .graph
                .tasks
                .values()
                .map(|task| (task.identity, task.fragment_id))
                .collect(),
            split,
            observe_lifecycle,
            releases: Mutex::new(ReleaseDiagnosticRows { profile, lifecycle }),
        })
    }

    /// Bind one physical send's paid receipt to its original participation.
    /// The ref is minted in the same host namespace before the producer starts.
    pub(crate) fn release_report(
        self: &Arc<Self>,
        participation: ParticipationRef,
        reference: PayloadRef,
    ) -> Option<DiagnosticReleaseReport> {
        let index = self.participations.binary_search(&participation).ok()?;
        Some(DiagnosticReleaseReport {
            source: Arc::clone(self),
            index,
            reference,
        })
    }

    pub(crate) fn take_release_records(&self) -> Option<RetainedReleaseDiagnostics> {
        self.releases
            .lock()
            .expect("original release diagnostics")
            .profile
            .take()
    }

    /// Resolve only the Core's accepted original references at final
    /// convergence, then transfer the value-only diagnostics once.
    pub(crate) fn take_lifecycle_rollup(
        &self,
        selection: Option<&RuntimeFilterDiagnostic>,
    ) -> Option<RuntimeFilterTerminalRollupSnapshot> {
        let mut rows = self
            .releases
            .lock()
            .expect("original release diagnostics")
            .lifecycle
            .take()?;
        let incomplete = || {
            Some(RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ))
        };
        let Some(selection) = selection.filter(|selection| selection.complete) else {
            return incomplete();
        };
        let mut selected = Vec::with_capacity(selection.contributions.len());
        let mut previous = None;
        for &(backend, reference) in &selection.contributions {
            let Ok(index) = self
                .participations
                .binary_search_by_key(&backend, |part| part.backend_process_id())
            else {
                return incomplete();
            };
            let Some(row) = rows[index]
                .take()
                .filter(|row| row.reference.0 == reference)
            else {
                return incomplete();
            };
            if previous.is_some_and(|previous| previous >= index) {
                return incomplete();
            }
            previous = Some(index);
            selected.push(row.telemetry);
        }
        Some(RuntimeFilterTerminalRollupSnapshot::Available(
            rollup_projected_release_contributions(selected),
        ))
    }
}

impl OriginalDiagnosticRecords for AttemptDiagnosticSources {
    fn binding(&self) -> OriginalDiagnosticBinding<'_> {
        OriginalDiagnosticBinding {
            activation: self.activation,
            root: self.root,
            participations: &self.participations,
            fragments: &self.fragments,
        }
    }

    fn payload(&self, address: DiagnosticPayloadAddress) -> Option<OriginalDiagnosticPayload> {
        match address {
            DiagnosticPayloadAddress::RuntimeFilter { backend, reference } => {
                let index = self
                    .participations
                    .binary_search_by_key(&backend, |part| part.backend_process_id())
                    .ok()?;
                let releases = self.releases.lock().expect("original release diagnostics");
                let original = releases.profile.as_ref()?.0[index].as_ref()?;
                (original.reference == reference).then(|| {
                    OriginalDiagnosticPayload::RuntimeFilter {
                        activation: self.activation,
                        participation: self.participations[index],
                        telemetry: original.telemetry.clone(),
                    }
                })
            }
            DiagnosticPayloadAddress::SplitProfile(reference) => {
                let (original, profile) = self.split.as_ref()?;
                if *original != reference {
                    return None;
                }
                Some(OriginalDiagnosticPayload::SplitProfile {
                    activation: self.activation,
                    root: self.root,
                    profile: *profile.get()?,
                })
            }
        }
    }

    fn publication_complete(&self) {
        drop(self.take_release_records());
    }
}

/// One report's writer. This travels with its original paid Native input and
/// borrows the canonical acknowledgement before the passive fact can become
/// visible. Only a settled Release can contribute to the Core's frontier.
pub(crate) struct DiagnosticReleaseReport {
    source: Arc<AttemptDiagnosticSources>,
    index: usize,
    reference: PayloadRef,
}

impl DiagnosticReleaseReport {
    pub(crate) fn record(
        &self,
        ack: &OperationAcknowledgement,
        reference: Option<PayloadRef>,
    ) -> bool {
        if !ack.is_applied() {
            return true;
        }
        let AckPayload::Release {
            receipt,
            outcome: ReleaseOutcome::Released | ReleaseOutcome::AlreadyTerminal,
            runtime_filter: Some(telemetry),
        } = ack.payload()
        else {
            return true;
        };
        if reference != Some(self.reference)
            || receipt.participation() != self.source.participations[self.index]
        {
            return false;
        }
        let lifecycle = self
            .source
            .observe_lifecycle
            .then(|| LifecycleReleaseDiagnostic {
                reference: self.reference,
                telemetry: project_release_contribution(
                    receipt.participation().backend_process_id(),
                    telemetry,
                ),
            });
        let mut releases = self
            .source
            .releases
            .lock()
            .expect("original release diagnostics");
        if releases
            .profile
            .as_ref()
            .is_some_and(|profile| profile.0[self.index].is_some())
            || releases
                .lifecycle
                .as_ref()
                .is_some_and(|rows| rows[self.index].is_some())
        {
            return false;
        }
        // Once the one-shot publication or failed source is closed, a late
        // operation can settle without taking a new canonical reply alias.
        if let Some(profile) = &mut releases.profile {
            profile.0[self.index] = Some(ReleaseDiagnostic {
                reference: self.reference,
                telemetry: telemetry.clone(),
            });
        }
        if let Some(rows) = &mut releases.lifecycle {
            rows[self.index] = lifecycle;
        }
        true
    }
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
            }) = records.payload(DiagnosticPayloadAddress::RuntimeFilter {
                backend,
                reference: PayloadRef(reference),
            })
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
                }) = records.payload(DiagnosticPayloadAddress::SplitProfile(reference))
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
#[expect(
    clippy::large_enum_variant,
    reason = "Publication refusal returns the original typed protocol error inline."
)]
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

    pub(crate) async fn register_computation_cut(
        &self,
        ready: ComputationReadyToken,
    ) -> Result<(), PublicationError> {
        self.record.lock().await.register_computation_cut(ready)
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
        let Publication::Diagnostics(cut) = publication else {
            return self.record.lock().await.publish(publication, None);
        };
        let records = self.diagnostics.as_deref();
        let sources = records
            .ok_or(PublicationError::MissingDiagnosticSources)
            .and_then(|records| {
                DiagnosticSourceSnapshot::resolve(cut, records).map_err(PublicationError::Protocol)
            });
        let (result, profile) = {
            let mut record = self.record.lock().await;
            let result = sources.and_then(|sources| record.publish(publication, Some(sources)));
            (result, record.profile.take())
        };
        // The one-shot cut is now settled. The receiver retains a published
        // snapshot, and a failed cut closes its watch. Leaving the sender in
        // the record would keep reply aliases through host exit, which itself
        // waits for the original send retirement.
        drop(profile);
        if let Some(records) = records {
            records.publication_complete();
        }
        result
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
    pub(in crate::task_execution) fn new(
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
