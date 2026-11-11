// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Loss handoff to the original logical execution convergence owner.

use super::{
    auxiliary::{AttemptInfrastructureFailure, AttemptInfrastructureHealth},
    failure_publication::FailurePublications,
    failure_supervisor::FailureCleanupRequest,
    transport::{FailureAbortAttempt, FailureResidualPort},
};
use crate::control_runtime::{host::HostExit, membership::FrozenMember};
use novarocks_attempt_protocol::failure::AttemptFailure;
use novarocks_control_runtime::actor_turn::ActorRegistration;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptToLogical, AttemptToLogicalBody,
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationService, NativeActiveConvergenceClosure,
    NativeActiveConvergenceRequest, NativeAttemptConvergence, NativeAttemptPreparationFailure,
    NativeAttemptTerminal, NativeParticipationConvergence, StatementNativeHealthFailure,
};
use std::{
    collections::BTreeSet,
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::Instant,
};
use tokio::sync::oneshot;

struct Residual {
    original: FailureCleanupRequest,
    attempts: Vec<FailureAbortAttempt>,
}
struct ResidualSender(oneshot::Sender<Residual>);
impl FailureResidualPort for ResidualSender {
    fn finish(
        self: Box<Self>,
        original: FailureCleanupRequest,
        attempts: Vec<FailureAbortAttempt>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            if let Err(original) = self.0.send(Residual { original, attempts }) {
                // The intended logical owner was lost. Preserve the entire
                // handoff as the original cleanup Work's failure payload.
                std::panic::resume_unwind(Box::new(original));
            }
        })
    }
}

/// Already bound to the original activation before the FE host can run.
#[must_use]
pub(crate) struct PendingFailureBridge {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    members: Arc<[FrozenMember]>,
    publications: Arc<FailurePublications>,
    receiver: oneshot::Receiver<Residual>,
    infrastructure: Option<AttemptInfrastructureHealth>,
}
impl PendingFailureBridge {
    pub(crate) fn new(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        members: Arc<[FrozenMember]>,
        publications: Arc<FailurePublications>,
    ) -> (Self, Box<dyn FailureResidualPort>) {
        assert!(publications.matches(&registration, activation));
        assert!(
            members
                .iter()
                .all(|member| member.participation.query_execution_id() == activation.execution())
        );
        assert!(
            members
                .windows(2)
                .all(|pair| pair[0].participation < pair[1].participation)
        );
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                registration,
                activation,
                members,
                publications,
                receiver,
                infrastructure: None,
            },
            Box::new(ResidualSender(sender)),
        )
    }

    pub(crate) fn with_infrastructure_health(
        mut self,
        health: AttemptInfrastructureHealth,
    ) -> Self {
        assert!(self.infrastructure.replace(health).is_none());
        self
    }

    pub(crate) fn bind(
        self,
        exit: HostExit,
        processes: BackendProcessObservationService,
    ) -> FailureBridge {
        assert!(exit.matches(&self.registration));
        FailureBridge {
            activation: self.activation,
            members: self.members,
            publications: self.publications,
            exit,
            processes,
            receiver: Some(self.receiver),
            loss: None,
            residual: None,
            replaced: BTreeSet::new(),
            deadline: None,
            closure: None,
            failure: None,
            infrastructure: self.infrastructure,
        }
    }
}

/// Held by the original logical attempt owner across borrowed converge polls.
/// A yield or shutdown reentry never consumes the only residual receiver or
/// renews the original observation deadline.
#[must_use]
pub(crate) struct FailureBridge {
    activation: AttemptActivationIdentity,
    members: Arc<[FrozenMember]>,
    publications: Arc<FailurePublications>,
    exit: HostExit,
    processes: BackendProcessObservationService,
    receiver: Option<oneshot::Receiver<Residual>>,
    loss: Option<Arc<AttemptToLogical<AttemptFailure>>>,
    residual: Option<Residual>,
    replaced: BTreeSet<ParticipationRef>,
    deadline: Option<Instant>,
    closure: Option<NativeActiveConvergenceClosure>,
    failure: Option<StatementNativeHealthFailure>,
    infrastructure: Option<AttemptInfrastructureHealth>,
}
impl FailureBridge {
    pub(crate) async fn terminal(&mut self) -> NativeAttemptTerminal {
        if self.loss.is_none() {
            self.loss = Some(self.publications.take_lost().await);
        }
        let loss = self.loss.as_ref().unwrap();
        assert_eq!(loss.activation, self.activation);
        assert!(matches!(
            loss.body,
            AttemptToLogicalBody::AttemptLost {
                failure: AttemptFailure::OwnerFailed,
                ..
            }
        ));
        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
            crate::task_execution::bridge::terminal::owner_failure(),
        ))
    }

    pub(crate) fn request_host_close(
        &self,
    ) -> Result<(), novarocks_control_runtime::actor_turn::ActorTurnError> {
        self.exit.request_close()
    }

    /// A late original Core loss still owns residual convergence, even if
    /// the normal protocol reports were already delivered.
    pub(crate) async fn normal_host_exit(&mut self) -> Result<bool, StatementNativeHealthFailure> {
        if self.loss.is_some() {
            return Ok(false);
        }
        if let Some(original) = &self.failure {
            return Err(original.clone());
        }
        let exited = tokio::select! {
            biased;
            lost = self.publications.take_lost() => {
                self.loss = Some(lost);
                return Ok(false);
            }
            failure = wait_infrastructure(&mut self.infrastructure) => {
                return Err(self.retain_infrastructure(failure));
            }
            exited = self.exit.wait() => exited,
        };
        if let Err(error) = exited {
            return Err(
                self.retain_failure("Native normal host exit was not observed", Arc::new(error))
            );
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn original_loss(&self) -> &AttemptToLogical<AttemptFailure> {
        self.loss.as_ref().expect("original terminal observed")
    }

    #[cfg(test)]
    pub(crate) fn original_residual(&self) -> (&FailureCleanupRequest, &[FailureAbortAttempt]) {
        let original = self
            .residual
            .as_ref()
            .expect("original residual transferred");
        (&original.original, &original.attempts)
    }

    pub(crate) async fn converge(
        &mut self,
        request: NativeActiveConvergenceRequest,
    ) -> Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure> {
        if let Some(original) = &self.closure {
            return Ok(original.clone());
        }
        let incoming = request.remote_observation_deadline();
        let deadline = self
            .deadline
            .map_or(incoming, |original| original.min(incoming));
        self.deadline = Some(deadline);
        if let Some(original) = &self.failure {
            return Err(original.clone());
        }
        if let Some(receiver) = self.receiver.as_mut() {
            let residual = tokio::select! {
                biased;
                failure = wait_infrastructure(&mut self.infrastructure) => {
                    return Err(self.retain_infrastructure(failure));
                }
                original = receiver => original,
            };
            self.receiver.take();
            match residual {
                Ok(original) => self.residual = Some(original),
                Err(error) => {
                    return Err(self.retain_failure(
                        "Native failure residual was not transferred",
                        Arc::new(error),
                    ));
                }
            }
        }
        if let Some(error) = self
            .residual
            .as_ref()
            .and_then(|residual| residual.original.root_reply.as_ref().err())
            .cloned()
        {
            return Err(self.retain_failure("Native original root reply delivery failed", error));
        }
        if let Some(error) = self
            .residual
            .as_ref()
            .and_then(|residual| residual.original.observation_drain.as_ref().err())
            .copied()
        {
            return Err(self.retain_failure(
                "Native failed observation payload transfer refused",
                Arc::new(error),
            ));
        }
        if !self.valid_residual() {
            return Err(self.retain_failure(
                "Native failure residual did not prove local transport custody",
                Arc::new(InvalidResidual),
            ));
        }
        // A remote deadline can expire while local work is live. Only this
        // same host's actual Actor and Scope removal opens the local gate.
        let exited = tokio::select! {
            biased;
            failure = wait_infrastructure(&mut self.infrastructure) => {
                return Err(self.retain_infrastructure(failure));
            }
            original = self.exit.wait() => original,
        };
        if let Err(error) = exited {
            return Err(
                self.retain_failure("Native failure host exit was not observed", Arc::new(error))
            );
        }
        let mut changes = Some(self.processes.subscribe_process_changes());
        let closure = loop {
            for member in &*self.members {
                let expected = member.participation.backend_process_id();
                if matches!(self.processes.observe_process_at_endpoint(expected, &member.endpoint), Ok(BackendProcessObservation::Replaced { current_process }) if current_process != expected)
                {
                    self.replaced.insert(member.participation);
                }
            }
            if self.replaced.len() == self.members.len() {
                let facts: Box<[_]> = self
                    .replaced
                    .iter()
                    .copied()
                    .map(NativeParticipationConvergence::worker_process_replaced)
                    .collect();
                break NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::participations(facts),
                );
            }
            if tokio::time::Instant::now() >= deadline.into() {
                break NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_participations: self
                        .members
                        .iter()
                        .map(|member| member.participation)
                        .collect(),
                };
            }
            if let Some(changes_rx) = changes.as_mut() {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {}
                    change = changes_rx.changed() => if change.is_err() { changes = None; },
                }
            } else {
                tokio::time::sleep_until(deadline.into()).await;
            }
        };
        self.closure = Some(closure.clone());
        Ok(closure)
    }

    fn valid_residual(&self) -> bool {
        let original = self
            .residual
            .as_ref()
            .expect("original residual already received");
        let exact = &original.original.unknown_participations;
        original.original.publication.is_ok()
            && exact.len() == original.attempts.len()
            && exact.windows(2).all(|pair| pair[0] < pair[1])
            && exact
                .iter()
                .zip(&original.attempts)
                .all(|(participation, attempt)| {
                    *participation == attempt.request.participation()
                        && self
                            .members
                            .iter()
                            .any(|member| member.participation == *participation)
                        && !attempt
                            .result
                            .as_ref()
                            .is_err_and(|error| error.exit_unproven())
                })
    }

    fn retain_failure(
        &mut self,
        detail: &'static str,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) -> StatementNativeHealthFailure {
        let failure = StatementNativeHealthFailure::actual_exit_unproven(detail, cause);
        self.failure = Some(failure.clone());
        failure
    }

    fn retain_infrastructure(
        &mut self,
        original: Arc<AttemptInfrastructureFailure>,
    ) -> StatementNativeHealthFailure {
        self.retain_failure(
            "Native attempt host infrastructure custody is unresolved",
            original,
        )
    }
}

async fn wait_infrastructure(
    health: &mut Option<AttemptInfrastructureHealth>,
) -> Arc<AttemptInfrastructureFailure> {
    match health {
        Some(health) => health.failed().await,
        None => pending().await,
    }
}

#[derive(Debug)]
struct InvalidResidual;
impl std::fmt::Display for InvalidResidual {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid original attempt failure residual")
    }
}
impl std::error::Error for InvalidResidual {}
