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

//! What drives one attempt's credential rotation.
//!
//! [`CredentialRefreshOwner`] decides *what* a rotation is: which epoch is
//! minted, which contexts still owe it, and when the identical request may be
//! resent. This is what makes it happen on a running query -- it obtains fresh
//! material from the provider, installs it in the attempt's own credential
//! table, mints the next epoch over it, and releases one `AdvanceDomain` per
//! context that has not accepted it.
//!
//! # Why a rotation that cannot finish fails the attempt
//!
//! A vended credential expires. A query that outlives its credential and keeps
//! reading is a query whose next object-store read fails somewhere deep in a
//! connector, at an arbitrary moment, with an error about access rather than
//! about a lease. The old supervisor aborted the query instead, and so does
//! this: past the hard deadline the material the backends hold can no longer be
//! relied on, and there is nothing to degrade to.
//!
//! # Two lease id spaces
//!
//! They are not the same and must never be conflated. The spi
//! `CredentialLeaseId` is sixteen bytes and names one vended storage lease --
//! it keys the table this rotates and it is what a provider refreshes. The
//! execution `CredentialLeaseId` is a `u64` and names the query context's
//! credential *domain*: one per attempt, and the progression token every
//! backend fences its next epoch against.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use novarocks_execution::task_execution::domain::CredentialEpoch;
use novarocks_execution::task_execution::identity::{QueryContextRef, TaskOperationId};
use novarocks_execution::task_execution::operation::{OperationOutcome, QueryContextDomainReceipt};
use novarocks_query_application::coordination::MonotonicInstant;
use novarocks_spi::connector::CredentialLeaseId as StorageLeaseId;
use novarocks_types::QueryExecutionId;

use crate::query_execution::lifecycle_plan::{
    AttemptCredentialStorage, QueryCredentialLeaseRefresh,
};

use super::blocking_io::{ConnectorBlockingIoJob, ConnectorBlockingIoSupervisor};
use super::clock::TaskProtocolClock;
use super::credential::{CredentialRefreshOwner, RefreshRefusal, refresh_timing};
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::intent::{AckPayload, OperationAcknowledgement};
use super::round::{AcknowledgementObserver, TurnPump};

/// How long a failed provider call waits before the next attempt.
///
/// Only a rate bound. The hard deadline is what decides whether there is still
/// time to try, so this never extends a rotation past the point the material
/// stops being usable.
const PROVIDER_RETRY_INITIAL: Duration = Duration::from_millis(200);
const PROVIDER_RETRY_MAX: Duration = Duration::from_secs(5);

/// What one provider call produced.
///
/// The refresh is boxed: it is much larger than a failure detail, and this
/// value only ever travels once, from the process supervisor into the slot the
/// turn reads.
enum VendOutcome {
    Refreshed(Box<QueryCredentialLeaseRefresh>),
    /// The provider did not answer. Trying again may work, inside the hard
    /// deadline.
    Retryable(String),
}

/// One in-flight provider call.
///
/// It does not remember which lease it asked about. A retry re-derives that
/// from the table, because the lease that expires first may have changed while
/// the call was outstanding, and asking about the wrong one would leave the
/// urgent lease unrotated.
struct VendingRound {
    /// When the epoch being replaced stops being usable.
    hard_deadline: MonotonicInstant,
    outcome: ConnectorBlockingIoJob<VendOutcome>,
    /// A completed outcome staged by deterministic unit tests.
    #[cfg(test)]
    staged_outcome: Option<Result<VendOutcome, super::blocking_io::ConnectorBlockingIoError>>,
}

struct RotationState {
    owner: CredentialRefreshOwner,
    /// The provider call currently outstanding, if any.
    vending: Option<VendingRound>,
    /// The earliest monotonic instant at which a provider call may start.
    ///
    /// Both the soft schedule and the retry backoff write here, so there is one
    /// answer to "may a call start now" rather than two competing timers.
    next_attempt_at: Option<MonotonicInstant>,
    retry_delay: Duration,
    /// The context each released advance was addressed to.
    in_flight: BTreeMap<TaskOperationId, QueryContextRef>,
    /// Contexts whose advance came back with a genuinely unknown outcome.
    ///
    /// The identical request has to be resent, and only the runner's turn may
    /// enqueue it: the acknowledgement observer sees the verdict but holds no
    /// state machine, so it records the context here and the next turn hands
    /// out the retained request.
    needs_retry: BTreeSet<QueryContextRef>,
    /// The highest epoch already reported as settled, so one rotation is
    /// counted once however many acknowledgements finish it.
    settled_epoch: Option<CredentialEpoch>,
    rotations_applied: usize,
    /// Set once the attempt has no credential to rotate any more.
    finished: bool,
}

/// The production driver of one attempt's credential rotation.
pub(crate) struct CredentialRotationPump {
    execution_id: QueryExecutionId,
    storage: Arc<AttemptCredentialStorage>,
    clock: Arc<dyn TaskProtocolClock>,
    blocking_io: ConnectorBlockingIoSupervisor,
    state: Mutex<RotationState>,
}

impl CredentialRotationPump {
    /// The label this owner is counted under when it is installed.
    pub(crate) const PUMP_NAME: &'static str = "credential_rotation";

    /// Builds the driver, or reports that this attempt has nothing to rotate.
    ///
    /// An attempt whose credential table holds no refreshable lease gets no
    /// driver at all: there is no provider to ask and no expiry to beat, and a
    /// driver that could never do anything would still have to be judged
    /// against a hard deadline it has no way to compute.
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        owner: CredentialRefreshOwner,
        storage: Arc<AttemptCredentialStorage>,
        clock: Arc<dyn TaskProtocolClock>,
        blocking_io: ConnectorBlockingIoSupervisor,
    ) -> Option<Arc<Self>> {
        if storage.refreshable().is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            execution_id,
            storage,
            clock,
            blocking_io,
            state: Mutex::new(RotationState {
                owner,
                vending: None,
                next_attempt_at: None,
                retry_delay: PROVIDER_RETRY_INITIAL,
                in_flight: BTreeMap::new(),
                needs_retry: BTreeSet::new(),
                settled_epoch: None,
                rotations_applied: 0,
                finished: false,
            }),
        }))
    }

    /// How many rotations this driver has minted and had accepted everywhere.
    ///
    /// The loop's observable: a driver that is built but never handed to the
    /// runner reports zero forever, which is exactly the defect this loop had.
    #[cfg(test)]
    pub(crate) fn rotations_applied(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .rotations_applied
    }

    #[cfg(test)]
    pub(crate) fn minted_epoch(&self) -> CredentialEpoch {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .owner
            .minted_epoch()
    }

    /// Moves a completed provider outcome into the serial owner's state.
    ///
    /// This exposes no production state transition. It lets a manual-clock
    /// test prove that the provider has already returned before advancing the
    /// clock past the hard deadline, without racing the blocking worker.
    #[cfg(test)]
    pub(crate) fn stage_provider_outcome_for_test(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(round) = state.vending.as_mut() else {
            return false;
        };
        if round.staged_outcome.is_none() {
            round.staged_outcome = round.outcome.try_take();
        }
        round.staged_outcome.is_some()
    }

    #[cfg(test)]
    pub(crate) fn provider_hard_deadline_for_test(&self) -> Option<MonotonicInstant> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .vending
            .as_ref()
            .map(|round| round.hard_deadline)
    }

    /// Stops rotating and drops the material.
    ///
    /// Called when the attempt is done with its credential. After it the driver
    /// can produce no further request, which is what makes it safe to keep on
    /// the runner while an attempt unwinds.
    pub(crate) fn wipe(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.finished = true;
        state.vending = None;
        state.in_flight.clear();
        state.owner.wipe();
    }

    fn drive_locked(
        &self,
        execution: &mut QueryTaskExecution,
    ) -> Result<usize, TaskExecutionError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished {
            return Ok(0);
        }
        let now = self.clock.now();
        let mut moved = 0;

        // A context that has been released can no longer accept anything, so
        // leaving it in the participant set would stall every later rotation
        // behind it.
        let released = execution
            .graph()
            .contexts()
            .copied()
            .filter(|context| {
                execution
                    .owner(*context)
                    .is_none_or(super::context_owner::QueryContextOwner::is_released)
            })
            .collect::<Vec<_>>();
        for context in released {
            state.owner.retire(context);
        }

        moved += self.settle_vending(&mut state, now)?;
        moved += self.start_vending(&mut state, now)?;
        moved += Self::release_advances(&mut state, execution, now)?;
        Ok(moved)
    }

    /// Adopts a finished provider call, or fails the attempt if it ran out of
    /// time.
    fn settle_vending(
        &self,
        state: &mut RotationState,
        now: MonotonicInstant,
    ) -> Result<usize, TaskExecutionError> {
        let Some(round) = state.vending.as_mut() else {
            return Ok(0);
        };
        let hard_deadline = round.hard_deadline;
        #[cfg(test)]
        let outcome = round
            .staged_outcome
            .take()
            .or_else(|| round.outcome.try_take());
        #[cfg(not(test))]
        let outcome = round.outcome.try_take();
        let Some(outcome) = outcome else {
            if now >= hard_deadline {
                return Err(self.rotation_failed(
                    "the credential provider did not answer before the credential stopped being \
                     usable",
                ));
            }
            return Ok(0);
        };
        state.vending = None;
        if now >= hard_deadline {
            return Err(self.rotation_failed(
                "the credential provider answered after the credential stopped being usable",
            ));
        }
        match outcome {
            Err(error) => {
                tracing::warn!(
                    execution_id = ?self.execution_id,
                    detail = %error,
                    "credential rotation worker failed and will be retried"
                );
                state.next_attempt_at =
                    Some(now.saturating_add(state.retry_delay).min(hard_deadline));
                state.retry_delay = state.retry_delay.saturating_mul(2).min(PROVIDER_RETRY_MAX);
                Ok(0)
            }
            Ok(outcome) => match outcome {
                VendOutcome::Refreshed(refreshed) => {
                    // The attempt's own table first: it is the frontend's read path
                    // as well as the source of the material the backends install,
                    // and a table that lagged the backends would leave this process
                    // reading with the epoch it just replaced.
                    self.storage
                        .apply_refresh(refreshed.as_ref())
                        .map_err(|error| {
                            self.rotation_failed(&format!(
                                "refreshed credential is not installable: {}",
                                error.message()
                            ))
                        })?;
                    let material = self.storage.freeze_material().map_err(|error| {
                        self.rotation_failed(&format!(
                            "refreshed credential batch is not installable: {}",
                            error.message()
                        ))
                    })?;
                    match state.owner.rotate(Arc::new(material), hard_deadline) {
                        Ok(epoch) => {
                            state.retry_delay = PROVIDER_RETRY_INITIAL;
                            state.next_attempt_at = None;
                            tracing::debug!(
                                execution_id = ?self.execution_id,
                                epoch = epoch.get(),
                                "credential domain minted a rotation"
                            );
                            Ok(1)
                        }
                        // Every context has left. There is nothing to rotate for,
                        // and no expiry left to beat.
                        Err(RefreshRefusal::NoParticipants) => {
                            state.finished = true;
                            state.owner.wipe();
                            Ok(0)
                        }
                        Err(refusal) => Err(self.rotation_failed(&refusal.to_string())),
                    }
                }
                VendOutcome::Retryable(detail) => {
                    if now >= hard_deadline {
                        return Err(self.rotation_failed(&format!(
                            "the credential provider failed and the credential stopped being \
                             usable: {detail}"
                        )));
                    }
                    tracing::warn!(
                        execution_id = ?self.execution_id,
                        detail,
                        "credential rotation will be retried"
                    );
                    // Never past the point the material stops being usable: the
                    // next attempt recomputes its own deadline from what is left of
                    // the lease, so a backoff that overshot this one would spend
                    // the whole remaining lifetime waiting to try again.
                    state.next_attempt_at =
                        Some(now.saturating_add(state.retry_delay).min(hard_deadline));
                    state.retry_delay = state.retry_delay.saturating_mul(2).min(PROVIDER_RETRY_MAX);
                    Ok(0)
                }
            },
        }
    }

    /// Starts one provider call when the earliest-expiring lease is due.
    fn start_vending(
        &self,
        state: &mut RotationState,
        now: MonotonicInstant,
    ) -> Result<usize, TaskExecutionError> {
        if state.vending.is_some() || state.owner.rotation_in_flight() {
            return Ok(0);
        }
        let Some((lease_id, remaining)) = self.earliest_refreshable() else {
            return Ok(0);
        };
        let timing = refresh_timing(self.execution_id, state.owner.lease_id(), remaining);
        let hard_deadline = now.saturating_add(timing.hard_delay());
        let due_at = state
            .next_attempt_at
            .unwrap_or_else(|| now.saturating_add(timing.soft_delay()));
        if state.next_attempt_at.is_none() {
            state.next_attempt_at = Some(due_at);
        }
        if now < due_at {
            return Ok(0);
        }
        let Some((descriptor, refresher)) = self.storage.refresh_source(lease_id) else {
            return Err(self.rotation_failed(
                "the credential table lost the refresh source of a lease it reported refreshable",
            ));
        };
        // The provider call is blocking and must not be made on the turn: the
        // same thread owns the result loop, and a provider that took a second
        // would stop settling acknowledgements and opening edges for a second.
        let outcome =
            self.blocking_io
                .spawn_protected(move || match refresher.refresh(&descriptor) {
                    Ok(refreshed) => VendOutcome::Refreshed(Box::new(refreshed)),
                    Err(detail) => VendOutcome::Retryable(detail),
                });
        state.vending = Some(VendingRound {
            hard_deadline,
            outcome,
            #[cfg(test)]
            staged_outcome: None,
        });
        state.next_attempt_at = None;
        Ok(1)
    }

    /// Releases one advance per context that still owes the minted epoch.
    fn release_advances(
        state: &mut RotationState,
        execution: &mut QueryTaskExecution,
        now: MonotonicInstant,
    ) -> Result<usize, TaskExecutionError> {
        if !state.owner.rotation_in_flight() {
            return Ok(0);
        }
        if let Some(hard_deadline) = state.owner.hard_deadline()
            && now >= hard_deadline
        {
            return Err(TaskExecutionError::Schedule(format!(
                "credential rotation of {:?} was not accepted by every query context before the \
                 credential stopped being usable",
                state.owner.lease_id()
            )));
        }
        let mut released = 0;
        // The identical request first: a context whose outcome was unknown is
        // resolved only by resending exactly what it was already sent, and
        // queueing a second, differently-identified request would leave the
        // backend with two claims on one epoch.
        for context in std::mem::take(&mut state.needs_retry) {
            let Some(request) = state.owner.retry(context) else {
                continue;
            };
            let operation_id = execution.enqueue_context_domain(request)?;
            state.in_flight.insert(operation_id, context);
            released += 1;
        }
        let contexts = execution.graph().contexts().copied().collect::<Vec<_>>();
        for context in contexts {
            let Some(request) = state.owner.advance_intent(context)? else {
                continue;
            };
            let operation_id = execution.enqueue_context_domain(request)?;
            state.in_flight.insert(operation_id, context);
            released += 1;
        }
        Ok(released)
    }

    /// The refreshable lease that expires first, and how long it has left.
    fn earliest_refreshable(&self) -> Option<(StorageLeaseId, Duration)> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.storage
            .refreshable()
            .into_iter()
            .min_by_key(|(_, not_after_unix_ms)| *not_after_unix_ms)
            .map(|(lease_id, not_after_unix_ms)| {
                (
                    lease_id,
                    Duration::from_millis(not_after_unix_ms.saturating_sub(now_ms)),
                )
            })
    }

    fn rotation_failed(&self, detail: &str) -> TaskExecutionError {
        TaskExecutionError::Schedule(format!(
            "credential rotation of {:?} failed: {detail}",
            self.execution_id
        ))
    }
}

impl TurnPump for Arc<CredentialRotationPump> {
    fn name(&self) -> &'static str {
        CredentialRotationPump::PUMP_NAME
    }

    fn drive(&mut self, execution: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
        self.drive_locked(execution)
    }
}

impl AcknowledgementObserver for CredentialRotationPump {
    fn observe_acknowledgement(&self, ack: &OperationAcknowledgement) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(context) = state.in_flight.remove(&ack.operation_id()) else {
            return Ok(());
        };
        if !ack.is_applied() {
            if matches!(
                ack.dispatch_result(),
                novarocks_query_application::coordination::OperationDispatchResult::TransportUnknown
            ) {
                // Recorded, not sent: the next turn hands out the identical
                // retained request, which is the only legal answer to a
                // genuinely unknown outcome.
                state.needs_retry.insert(context);
                return Ok(());
            }
            // A context that is over is not a rotation failure. It has no task
            // left that could read through the credential, and it can never
            // acknowledge anything again -- leaving it a participant would
            // stall every later rotation behind it and then fail the attempt
            // on that rotation's hard deadline.
            if matches!(
                ack.worker_outcome(),
                Some(OperationOutcome::ContextTerminalReceipt | OperationOutcome::Gone)
            ) {
                state.owner.retire(context);
                // Retiring the last laggard is what settles the rotation, so
                // the same completion check runs here as on an acceptance.
                self.note_if_settled(&mut state);
                return Ok(());
            }
            return Err(format!(
                "credential rotation for {context} failed closed as {:?}",
                ack.worker_outcome()
            ));
        }
        let AckPayload::Context(receipt) = ack.payload() else {
            return Err(format!(
                "an applied credential rotation for {context} carries no context receipt"
            ));
        };
        let accepted = receipt.domains().iter().find_map(|domain| match domain {
            QueryContextDomainReceipt::Credential {
                lease_id,
                accepted_epoch,
                ..
            } if *lease_id == state.owner.lease_id() => Some(*accepted_epoch),
            _ => None,
        });
        let Some(accepted) = accepted else {
            return Err(format!(
                "an applied credential rotation for {context} reports no credential domain receipt"
            ));
        };
        state.owner.accept(context, accepted);
        self.note_if_settled(&mut state);
        Ok(())
    }
}

impl CredentialRotationPump {
    /// Records one rotation once every participating context has caught up.
    ///
    /// A rotation settles either because the last laggard accepted it or
    /// because the last laggard left, and both have to reach the same place:
    /// counting only acceptances would leave a rotation whose final context
    /// was released reported as never applied.
    fn note_if_settled(&self, state: &mut RotationState) {
        if state.settled_epoch == Some(state.owner.minted_epoch()) {
            return;
        }
        if !state.owner.fully_accepted() {
            return;
        }
        let minted = state.owner.minted_epoch();
        state.settled_epoch = Some(minted);
        // The initial epoch is installed by every establish rather than
        // rotated, so it is not a rotation this owner performed.
        if minted == CredentialEpoch::FIRST {
            return;
        }
        state.rotations_applied += 1;
        emit_rotated_marker(self.execution_id, minted);
    }
}

/// Stable evidence that one rotation was accepted by every query context.
///
/// It fires where the loop completes its work, not where the driver is built: a
/// marker at construction would say only that a driver exists, which is exactly
/// the defect this loop had.
fn emit_rotated_marker(execution_id: QueryExecutionId, epoch: CredentialEpoch) {
    if !(cfg!(debug_assertions)
        && std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER").is_some())
    {
        return;
    }
    eprintln!(
        "NOVAROCKS_TASK_CREDENTIAL_ROTATED execution_id={}:{}:{} epoch={}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get(),
        epoch.get(),
    );
}
