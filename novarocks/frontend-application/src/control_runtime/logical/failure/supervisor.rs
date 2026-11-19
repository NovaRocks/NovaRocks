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

//! Original credentialed cleanup after the logical Core's immutable fence.
//! Remote dispositions, physical Native exit and Source retirement are separate.
use super::{
    LogicalFailureCut, LogicalFailureOriginals, LogicalFailureProgressObligation,
    LogicalInfrastructureFailure,
};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect},
    logical::links::LogicalAttemptLinks,
};
use crate::task_execution::host::port::{AttemptPortEndpoints, PortDeliveryError};
use futures::{StreamExt, stream::FuturesUnordered};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    publication::FailurePublicationSession,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptConvergenceClosure, ExecutionFailure, QueryExecutionError,
};
use novarocks_logical_execution_protocol::{
    effect::{GovernanceEffect, NativeEffect},
    input::{GovernanceFact, NativeFact},
};
use novarocks_query_application::{
    api::logical_runtime::LogicalFuture,
    coordination::executor::{
        GovernanceObligationState, GovernanceOwnerState, NativeCleanupInventory,
    },
};
use novarocks_types::QueryExecutionId;
use std::sync::Arc;

pub(in crate::control_runtime::logical) struct LogicalFailureSupervisor {
    pub(in crate::control_runtime::logical) retain_diagnostics: bool,
    pub(in crate::control_runtime::logical) runtime: ActorRuntime<FrontendActor>,
    pub(in crate::control_runtime::logical) registration: ActorRegistration,
    pub(in crate::control_runtime::logical) ticket: Arc<EffectTicket<FrontendEffect>>,
    pub(in crate::control_runtime::logical) cut: LogicalFailureCut,
    pub(in crate::control_runtime::logical) originals: Option<Arc<dyn LogicalFailureOriginals>>,
    pub(in crate::control_runtime::logical) links: Arc<LogicalAttemptLinks>,
    pub(in crate::control_runtime::logical) progress: Arc<LogicalFailureProgressObligation>,
    pub(in crate::control_runtime::logical) failure: Arc<LogicalInfrastructureFailure>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Job {
    Registry,
    Adopt,
    SealRead(AttemptActivationIdentity),
    Active(AttemptActivationIdentity),
    Dormant(QueryExecutionId),
    Cancel(QueryExecutionId),
}
#[derive(Debug)]
struct Remote {
    activation: AttemptActivationIdentity,
    positive: bool,
}
impl LogicalFailureSupervisor {
    pub(in crate::control_runtime::logical) async fn run(mut self) {
        if let Err(error) = self.run_original().await {
            self.failure.retain_typed(error);
            // A refused/ambiguous original never becomes a successful release
            // through cancellation, timeout or destruction of this observer.
            std::future::pending::<()>().await;
        }
    }
    async fn run_original(&mut self) -> Result<(), QueryExecutionError> {
        #[cfg(test)]
        self.failure.observe_cut_for_fixture(self.cut, true);
        #[cfg(test)]
        self.failure
            .observe_cleanup_for_fixture("claim original publication".to_owned());
        let mut session = self
            .runtime
            .claim_failure_publication(&self.ticket)
            .unwrap_or_else(|error| carrier_invariant(error));
        let error = self.failure.error();
        let failure = ExecutionFailure::from_input(
            error
                .execution_failure()
                .expect("original infrastructure error is typed")
                .clone(),
        );
        if let Some(originals) = &self.originals {
            #[cfg(test)]
            self.failure
                .observe_cleanup_for_fixture("notify original application owners".to_owned());
            originals.begin_failure(error.clone());
        }
        let mut jobs: FuturesUnordered<
            LogicalFuture<'static, (Job, Result<(), QueryExecutionError>)>,
        > = FuturesUnordered::new();
        let mut issued = Vec::new();
        let mut completed = Vec::new();
        let mut remote: Vec<Remote> = Vec::new();
        let mut stopped = Vec::new();
        let mut local_unproven = false;
        let mut adoption_unproven = false;
        let changes = self.links.changes();
        let fixed_deadline = self
            .originals
            .as_ref()
            .map(|originals| originals.original_cleanup_deadline(self.cut.remote_deadline));
        let mut startup_observed = Vec::new();
        loop {
            let mut rescan = false;
            let changed = changes.notified();
            let progress = self.progress.changed.notified();
            tokio::pin!(changed, progress);
            changed.as_mut().enable();
            progress.as_mut().enable();
            // Notification futures are enabled before examining the original
            // banks. Their persistent notify_one permits close the last gap.
            let endpoints: Vec<_> = (0..self.links.cleanup_row_count())
                .filter_map(|index| self.links.cleanup_row(index).endpoints)
                .collect();
            let mut watches: FuturesUnordered<
                LogicalFuture<'static, Option<AttemptActivationIdentity>>,
            > = FuturesUnordered::new();
            for endpoint in &endpoints {
                let endpoint = endpoint.clone();
                let requests = endpoint.clone();
                watches.push(Box::pin(async move {
                    requests.requests.changed().await;
                    None
                }));
                let facts = endpoint.clone();
                watches.push(Box::pin(async move {
                    facts.facts.changed().await;
                    None
                }));
                if !stopped.contains(&endpoint.activation())
                    && !startup_observed.contains(&endpoint.activation())
                {
                    watches.push(Box::pin(async move {
                        let mut startup = endpoint.startup();
                        let _original_status = startup.wait_initialized().await;
                        Some(endpoint.activation())
                    }));
                }
            }
            // Keep original reply rejection live while the same Native body
            // completes/adopts its activation. No failed Core is consulted.
            self.service_requests(&mut session, &failure, &endpoints, &mut stopped, &[])?;
            let mut deadline = None;
            if let Some(originals) = &self.originals {
                let native = originals;
                if !issued.contains(&Job::Registry) {
                    issued.push(Job::Registry);
                    let native = native.clone();
                    jobs.push(Box::pin(async move {
                        (
                            Job::Registry,
                            native.close_registry_ingress_after_owner_failure().await,
                        )
                    }));
                }
                if !issued.contains(&Job::Adopt) {
                    issued.push(Job::Adopt);
                    let native = native.clone();
                    jobs.push(Box::pin(async move {
                        (
                            Job::Adopt,
                            native
                                .adopt_pending_activation_for_cleanup()
                                .await
                                .map(|_| ()),
                        )
                    }));
                }
                let inventory = native.cleanup_inventory()?;
                #[cfg(test)]
                self.failure.observe_cleanup_for_fixture(format!(
                    "inspect original endpoints={}, active={:?}, stopped={stopped:?}",
                    endpoints.len(),
                    inventory.active
                ));
                {
                    let mut controls_ready = Vec::new();
                    for endpoint in &endpoints {
                        let active = inventory
                            .active
                            .iter()
                            .find(|row| row.activation == endpoint.activation());
                        if let Some(row) = active
                            && let Some(root) = row.read_root
                            && !row.read_sealed
                        {
                            // The atomic Native close waits on the SAME
                            // in-flight callback when present, without calling
                            // it again. Do not wait for its header retirement:
                            // that may itself need this child's convergence.
                            let key = Job::SealRead(row.activation);
                            if !issued.contains(&key) {
                                let mut read=novarocks_execution_contract::root_lifetime::RootReadConvergence::new(root);
                                // The SAME retained failed-owner proof is this
                                // local termination author; it proves no remote
                                // close, read future exit or delivery settlement.
                                let sealed = read.seal_terminated();
                                let activation = row.activation;
                                let native = native.clone();
                                issued.push(key);
                                jobs.push(Box::pin(async move {
                                    let result=match native.execute_native(NativeEffect::SealRootRead {activation,sealed}).await {
                                        NativeFact::Applied=>Ok(()),
                                        NativeFact::Failed {failure,..}|NativeFact::NativeRunFailed {failure,..}=>Err(failure.error),
                                        _=>Err(QueryExecutionError::new(novarocks_coordination_contract::QueryExecutionErrorKind::InvalidRequest,"original read close returned another Native fact")),
                                    };
                                    (key,result)
                                }));
                            }
                            continue;
                        }
                        if active.is_none()
                            && (!completed.contains(&Job::Adopt) || adoption_unproven)
                        {
                            // A late original activation may still own its row.
                            // Absence before the stable body loan is not proof.
                            continue;
                        }
                        controls_ready.push(endpoint.activation());
                    }
                    self.service_requests(
                        &mut session,
                        &failure,
                        &endpoints,
                        &mut stopped,
                        &controls_ready,
                    )?;
                }
                let governor = originals.governance();
                remember_completed(&mut remote, &governor.cleanup_inventory())?;
                for row in governor.cleanup_inventory() {
                    if row.state != GovernanceObligationState::Held
                        || governor.cleanup_inventory().iter().any(|original| {
                            original.activation == row.activation
                                && matches!(
                                    original.state,
                                    GovernanceObligationState::Registering
                                        | GovernanceObligationState::Settling
                                )
                        })
                    {
                        continue;
                    }
                    let selected = remote.iter().find(|r| r.activation == row.activation);
                    let positive = match selected {
                        Some(selected) => Some(selected.positive),
                        None => {
                            let original_deadline = inventory
                                .preparations
                                .iter()
                                .find(|p| p.execution == row.activation.execution())
                                .and_then(|p| p.terminal_cut)
                                .map_or(self.cut.remote_deadline, |(_, _, end)| {
                                    end.min(self.cut.remote_deadline)
                                });
                            let absolute = shorten_deadline(
                                fixed_deadline.expect("same original Native deadline"),
                                self.cut.remote_deadline,
                                original_deadline,
                            )?;
                            let receipt = exact_closure(&inventory, row.activation, &endpoints)?;
                            match receipt {
                                Some(positive) => Some(positive),
                                None if tokio::time::Instant::now() >= absolute.into() => {
                                    Some(false)
                                }
                                None => {
                                    deadline = Some(
                                        deadline.map_or(absolute, |old: std::time::Instant| {
                                            old.min(absolute)
                                        }),
                                    );
                                    None
                                }
                            }
                        }
                    };
                    let Some(positive) = positive else {
                        continue;
                    };
                    if selected.is_none() {
                        remote.push(Remote {
                            activation: row.activation,
                            positive,
                        });
                    }
                    let effect = if positive {
                        GovernanceEffect::ResolveObligation {
                            activation: row.activation,
                            kind: row.kind,
                        }
                    } else {
                        GovernanceEffect::EndRemoteTracking {
                            activation: row.activation,
                            kind: row.kind,
                        }
                    };
                    #[cfg(test)]
                    self.failure.observe_cleanup_for_fixture(format!(
                        "await original governance {effect:?}"
                    ));
                    governance_applied(governor.execute(effect).await)?;
                }
                // Active resources require an immutable original remote
                // disposition; an empty/terminal mailbox never supplies one.
                for row in &inventory.active {
                    let key = Job::Active(row.activation);
                    if row.read_root.is_some() && !row.read_sealed {
                        continue;
                    }
                    if issued.contains(&key)
                        || governor.cleanup_inventory().iter().any(|original| {
                            original.activation == row.activation
                                && matches!(
                                    original.state,
                                    GovernanceObligationState::Registering
                                        | GovernanceObligationState::Settling
                                )
                        })
                    {
                        continue;
                    }
                    let selected = remote.iter().find(|r| r.activation == row.activation);
                    let Some(selected) = selected else {
                        // An already settled normal obligation keeps its end;
                        // select cleanup from the actual AP receipt or deadline.
                        let original_deadline = row
                            .terminal_cut
                            .map_or(self.cut.remote_deadline, |(_, end)| {
                                end.min(self.cut.remote_deadline)
                            });
                        let absolute = shorten_deadline(
                            fixed_deadline.expect("same original Native deadline"),
                            self.cut.remote_deadline,
                            original_deadline,
                        )?;
                        if let Some(positive) =
                            exact_closure(&inventory, row.activation, &endpoints)?
                        {
                            remote.push(Remote {
                                activation: row.activation,
                                positive,
                            });
                            rescan = true;
                        } else if tokio::time::Instant::now() >= absolute.into() {
                            remote.push(Remote {
                                activation: row.activation,
                                positive: false,
                            });
                            rescan = true;
                        } else {
                            deadline = Some(deadline.map_or(absolute, |old| old.min(absolute)));
                        }
                        continue;
                    };
                    let positive = selected.positive;
                    let activation = row.activation;
                    let adopted = row.adopted;
                    let cut = self.cut;
                    let native = native.clone();
                    issued.push(key);
                    jobs.push(Box::pin(async move {
                        let result = async {
                            native_applied(
                                native
                                    .execute_native(NativeEffect::RecordTerminalCut {
                                        activation,
                                        cut: cut.cut,
                                        remote_deadline: cut.remote_deadline,
                                    })
                                    .await,
                            )?;
                            let effect = if positive {
                                NativeEffect::FinishResources { activation }
                            } else {
                                NativeEffect::AbandonResources { activation }
                            };
                            native_applied(native.execute_native(effect).await)?;
                            if adopted {
                                native
                                    .retire_adopted_dormant_for_cleanup(activation)
                                    .await?;
                            }
                            Ok(())
                        }
                        .await;
                        (key, result)
                    }));
                }
                for row in &inventory.preparations {
                    let key = Job::Dormant(row.execution);
                    if issued.contains(&key)
                        || !completed.contains(&Job::Adopt)
                        || inventory
                            .active
                            .iter()
                            .any(|active| active.activation.execution() == row.execution)
                        || (0..self.links.cleanup_row_count()).any(|index| {
                            let link = self.links.cleanup_row(index);
                            link.activation
                                .is_some_and(|a| a.execution() == row.execution)
                                && !link.activation_exited
                        })
                    {
                        continue;
                    }
                    issued.push(key);
                    let execution = row.execution;
                    let native = native.clone();
                    jobs.push(Box::pin(async move {
                        (
                            key,
                            native.converge_dormant_for_owner_failure(execution).await,
                        )
                    }));
                }
                for row in &inventory.qualifications {
                    let key = Job::Cancel(row.identity.replacement);
                    if issued.contains(&key) {
                        continue;
                    }
                    if let Some(operation_id) = row.operation {
                        issued.push(key);
                        let native = native.clone();
                        let identity = row.identity;
                        jobs.push(Box::pin(async move {
                            (
                                key,
                                native_applied(
                                    native
                                        .execute_native(NativeEffect::CancelQualification {
                                            identity,
                                            operation_id,
                                        })
                                        .await,
                                ),
                            )
                        }));
                    }
                }
                let unsettled = governor.cleanup_inventory().iter().any(|row| {
                    matches!(
                        row.state,
                        GovernanceObligationState::Held
                            | GovernanceObligationState::Registering
                            | GovernanceObligationState::Settling
                    )
                });
                if self.progress.other_work_exited()
                    && !unsettled
                    && jobs.is_empty()
                    && !local_unproven
                {
                    break;
                }
            } else if self.progress.other_work_exited() {
                break;
            }
            if rescan {
                continue;
            }
            #[cfg(test)]
            self.failure.observe_cleanup_for_fixture(format!("await original events: issued={issued:?}, completed={completed:?}, remote={remote:?}, stopped={stopped:?}, local_unproven={local_unproven}, adoption_unproven={adoption_unproven}"));
            tokio::select! {
                biased;
                result = jobs.next(), if !jobs.is_empty() => { if let Some((key, result)) = result {
                    if let Err(error) = result { if key==Job::Adopt { adoption_unproven=true; } self.failure.retain_typed(error); local_unproven = true; }
                    completed.push(key);
                } },
                () = &mut changed => (),
                () = &mut progress => (),
                observed = watches.next(), if !watches.is_empty() => { if let Some(Some(activation)) = observed { startup_observed.push(activation); } },
                () = async { match deadline { Some(end) => tokio::time::sleep_until(end.into()).await, None => std::future::pending().await } } => (),
            }
        }
        if let Some(originals) = &self.originals {
            for row in originals.cleanup_inventory()?.qualifications {
                native_applied(
                    originals
                        .execute_native(NativeEffect::AbandonReservation {
                            identity: row.identity,
                        })
                        .await,
                )?;
            }
            native_applied(
                originals
                    .execute_native(NativeEffect::RetirePreparation)
                    .await,
            )?;
            originals.close_original_body_for_cleanup().await?;
        }
        // Every non-governance alias in the job is now relinquished. The
        // Source independently disposes its SAME App/Tail originals and joins.
        drop(self.originals.take());
        self.progress.request_disposal();
        self.progress.wait_disposed().await;
        if let Some(governance) = self.progress.governance() {
            if governance.stage_held() {
                governance_applied(
                    governance
                        .execute(GovernanceEffect::ReleaseExecutionStage)
                        .await,
                )?;
            }
            if governance.owner_state() != GovernanceOwnerState::Completed {
                governance_applied(
                    governance
                        .execute(GovernanceEffect::CompleteWorkOwner)
                        .await,
                )?;
            }
            drop(governance);
        }
        let governance = self
            .progress
            .take_completed_governance()
            .map_err(|()| self.failure.error())?;
        drop(governance);
        self.progress.observe_governance_completed();
        session.finish().unwrap_or_else(|(error, original)| {
            std::mem::forget(original);
            carrier_invariant(error)
        });
        if let Some(original) = self
            .runtime
            .take_failure_payload(self.registration.actor())
            .unwrap_or_else(|error| carrier_invariant(error))
        {
            self.failure.retain(original);
        }
        Ok(())
    }
    fn service_requests(
        &self,
        session: &mut FailurePublicationSession<FrontendActor>,
        failure: &ExecutionFailure,
        endpoints: &[Arc<AttemptPortEndpoints>],
        stopped: &mut Vec<AttemptActivationIdentity>,
        controls_ready: &[AttemptActivationIdentity],
    ) -> Result<(), QueryExecutionError> {
        if let Some(originals) = &self.originals {
            while originals.fail_one_pending_request() {}
        }
        for endpoint in endpoints {
            // Original pending request replies must precede a StartupReady
            // wait: the startup itself may be awaiting exactly this LP reply.
            loop {
                match endpoint.requests.publish_one_owner_failed(session, failure) {
                    Ok(Some(_)) => (),
                    Ok(None)
                    | Err(
                        crate::task_execution::host::port::requests::RequestError::ReplyBorrowed,
                    ) => break,
                    Err(crate::task_execution::host::port::requests::RequestError::Runtime(
                        ActorTurnError::ActorFailed,
                    )) => break,
                    Err(error) => {
                        return Err(QueryExecutionError::new(
                            novarocks_coordination_contract::QueryExecutionErrorKind::Failed,
                            format!("original failed-owner request reply was refused: {error:?}"),
                        ));
                    }
                }
            }
            if stopped.contains(&endpoint.activation())
                || !controls_ready.contains(&endpoint.activation())
            {
                continue;
            }
            if self.links.actual_child_exited(endpoint.activation())
                || self.runtime.failure_proof_for(endpoint.attempt()).is_ok()
            {
                stopped.push(endpoint.activation());
                continue;
            }
            let ready = self
                .originals
                .as_ref()
                .map(|originals| originals.computation_cut(endpoint.activation()))
                .transpose()?
                .flatten();
            let command = crate::task_execution::host::port::control::ConvergenceCommand {
                mode: novarocks_coordination_contract::AttemptConvergenceMode::Cleanup,
                remote_deadline: ready.map_or(self.cut.remote_deadline, |cut| {
                    self.cut.remote_deadline.min(cut.convergence_deadline)
                }),
                computation_ready: ready,
                retain_diagnostics: self.retain_diagnostics,
            };
            match endpoint.publish_owner_failed_control(&self.runtime, session, failure, command) {
                Ok(_) => stopped.push(endpoint.activation()),
                Err(PortDeliveryError::StartupPending) => (),
                Err(error) => {
                    return Err(QueryExecutionError::new(
                        novarocks_coordination_contract::QueryExecutionErrorKind::Failed,
                        error.to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}
fn native_applied(fact: NativeFact) -> Result<(), QueryExecutionError> {
    match fact {
        NativeFact::Failed { failure, .. } | NativeFact::NativeRunFailed { failure, .. } => {
            Err(failure.error)
        }
        _ => Ok(()),
    }
}
fn governance_applied(fact: GovernanceFact) -> Result<(), QueryExecutionError> {
    match fact {
        GovernanceFact::Failed(error) => Err(error),
        _ => Ok(()),
    }
}
fn exact_closure(
    inventory: &NativeCleanupInventory,
    activation: AttemptActivationIdentity,
    endpoints: &[Arc<AttemptPortEndpoints>],
) -> Result<Option<bool>, QueryExecutionError> {
    let Some(receipt) = endpoints
        .iter()
        .find(|endpoint| endpoint.activation() == activation)
        .and_then(|endpoint| endpoint.facts.completion_receipt())
    else {
        return Ok(None);
    };
    let Some(schedule) = inventory
        .preparations
        .iter()
        .find(|p| p.execution == activation.execution())
        .and_then(|p| p.schedule.as_ref())
    else {
        return Ok(None);
    };
    let valid = closure_matches_roster(schedule.participations(), receipt.closure());
    if receipt.activation() != activation || !valid {
        return Err(QueryExecutionError::new(
            novarocks_coordination_contract::QueryExecutionErrorKind::Failed,
            "original AP completion does not match its frozen Native roster",
        ));
    }
    Ok(Some(matches!(
        receipt.closure().as_ref(),
        AttemptConvergenceClosure::Evidence(_)
    )))
}
fn closure_matches_roster(
    expected: &[novarocks_execution_contract::ParticipationRef],
    closure: &AttemptConvergenceClosure,
) -> bool {
    use std::collections::BTreeSet;
    let frozen: BTreeSet<_> = expected.iter().collect();
    if frozen.len() != expected.len() {
        return false;
    }
    match closure {
        AttemptConvergenceClosure::Evidence(rows) => {
            rows.len() == expected.len()
                && rows
                    .iter()
                    .map(|(participation, _)| participation)
                    .collect::<BTreeSet<_>>()
                    == frozen
        }
        AttemptConvergenceClosure::RemoteTrackingEnded {
            exact_participations,
        } => {
            exact_participations.len() == expected.len()
                && exact_participations.iter().collect::<BTreeSet<_>>() == frozen
        }
    }
}

fn carrier_invariant(error: ActorTurnError) -> ! {
    tracing::error!(?error, "original logical failure carrier invariant");
    std::process::abort()
}

fn shorten_deadline(
    fixed: std::time::Instant,
    end: novarocks_coordination_contract::MonotonicInstant,
    earlier: novarocks_coordination_contract::MonotonicInstant,
) -> Result<std::time::Instant, QueryExecutionError> {
    fixed
        .checked_sub(end.since_origin().saturating_sub(earlier.since_origin()))
        .ok_or_else(|| {
            QueryExecutionError::new(
                novarocks_coordination_contract::QueryExecutionErrorKind::Failed,
                "original cleanup deadline is outside its Native clock domain",
            )
        })
}

// This consumes only passive facts retained by the original governor. It
// preserves a completed normal-Core disposition; it cannot create one from an
// identity, absence or a new failure observation.
fn remember_completed(
    remote: &mut Vec<Remote>,
    rows: &[novarocks_query_application::coordination::executor::GovernanceObligationSnapshot],
) -> Result<(), QueryExecutionError> {
    for row in rows {
        let positive = match row.state {
            GovernanceObligationState::Resolved => true,
            GovernanceObligationState::RemoteTrackingEnded => false,
            _ => continue,
        };
        if let Some(selected) = remote.iter().find(|r| r.activation == row.activation) {
            if selected.positive != positive {
                return Err(QueryExecutionError::new(
                    novarocks_coordination_contract::QueryExecutionErrorKind::Failed,
                    "original governance kinds have conflicting remote dispositions",
                ));
            }
        } else {
            remote.push(Remote {
                activation: row.activation,
                positive,
            });
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
