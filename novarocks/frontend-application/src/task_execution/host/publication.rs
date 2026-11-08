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

use crate::task_execution::{
    error::TaskExecutionError,
    execution::ReleasedRuntimeFilterContributions,
    profile_diagnostics::{ProfileSplitDiagnostic, TaskFinalInfoCollector},
    protocol_facts::AttemptProtocolFactsPublisher,
};
use novarocks_attempt_protocol::{
    effect::Publication,
    publication::{DiagnosticPublication, DiagnosticSources, SplitDiagnosticObservation},
    start::PayloadRef,
};
use novarocks_coordination_contract::{AttemptActivationIdentity, AttemptRootFailure};
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::{
    api::{ComputationReadyToken, QueryExecutionError},
    coordination::{AcceptedAttemptFailure, AcceptedRootStatusSender},
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
pub(crate) trait OriginalDiagnosticRecords {
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
    MissingProfileObserver,
    MissingOriginalComputationCut,
    MissingDiagnosticSources,
    ForeignOriginalComputationCut,
    Protocol(TaskExecutionError),
    Root(QueryExecutionError),
}
impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(error) => write!(f, "original attempt publication failed: {error}"),
            Self::Root(error) => write!(f, "original root publication failed: {error}"),
            other => write!(f, "attempt publication custody invariant failed: {other:?}"),
        }
    }
}
impl std::error::Error for PublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Root(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) struct PublicationRecord {
    activation: AttemptActivationIdentity,
    root: Option<AcceptedRootStatusSender>,
    protocol: AttemptProtocolFactsPublisher,
    profile: Option<TaskFinalInfoCollector>,
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
    ) -> Self {
        Self {
            activation,
            root: Some(root),
            protocol,
            profile,
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
        publication: Publication,
        mut sources: Option<DiagnosticSourceSnapshot>,
    ) -> Result<(), PublicationError> {
        match publication {
            Publication::RootStatus(projection) => {
                let failure = match projection.failure {
                    AttemptRootFailure::None => AcceptedAttemptFailure::None,
                    AttemptRootFailure::DerivedPending => AcceptedAttemptFailure::DerivedPending,
                    AttemptRootFailure::Authoritative(failure) => {
                        AcceptedAttemptFailure::Authoritative(failure)
                    }
                };
                self.root
                    .as_ref()
                    .ok_or(PublicationError::MissingRootPublisher)?
                    .publish_attempt_observation(projection.status, failure)
                    .map_err(PublicationError::Root)?;
            }
            Publication::ProtocolBatch(batch) => {
                self.protocol
                    .publish_core_batch(self.activation, batch)
                    .map_err(PublicationError::Protocol)?;
            }
            Publication::FinalTaskInfo(info) => {
                self.profile
                    .as_mut()
                    .ok_or(PublicationError::MissingProfileObserver)?
                    .collected
                    .push(Arc::new(info));
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
                    .publish_core_cut(ready, cut, sources)
                    .map_err(PublicationError::Protocol)?;
            }
            Publication::DevRootObservation { status, installed } => {
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = &self.root_observer {
                    observer.publish_root_status(
                        crate::preparation_diagnostics::StatementRootStatusObservation {
                            root: status.identity(),
                            installed,
                            status_version: status.version(),
                        },
                    );
                }
                #[cfg(not(novarocks_dev_controls))]
                let _ = (status, installed);
            }
        }
        Ok(())
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
