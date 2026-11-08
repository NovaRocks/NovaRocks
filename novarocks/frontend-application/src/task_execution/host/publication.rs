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
use novarocks_attempt_protocol::effect::Publication;
use novarocks_coordination_contract::{AttemptActivationIdentity, AttemptRootFailure};
use novarocks_execution_contract::TaskIdentity;
use novarocks_query_application::{
    api::{ComputationReadyToken, QueryExecutionError},
    coordination::{AcceptedAttemptFailure, AcceptedRootStatusSender},
};
use std::sync::Arc;

/// Values sampled from the same original host sources for this publication.
/// Sampling neither advances core protocol nor grants capacity/cleanup rights.
pub(crate) struct DiagnosticSourceSnapshot {
    pub(crate) runtime_filters: ReleasedRuntimeFilterContributions,
    pub(crate) split: ProfileSplitDiagnostic,
    pub(crate) fragments: Box<[(TaskIdentity, crate::query_execution::artifact::FragmentId)]>,
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
                    .publish_core_cut(
                        ready,
                        cut,
                        sources.runtime_filters,
                        sources.split,
                        sources.fragments,
                    )
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
