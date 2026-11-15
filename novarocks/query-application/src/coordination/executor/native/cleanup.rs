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

//! Read-only original inventory for supervised physical cleanup. A snapshot
//! authorizes no dispatch, settlement, positive remote fact or actual exit.

use super::*;

/// Weak notification of the SAME original outer owner, independent of the
/// application executor's final Drop. It grants no publication, Work or exit.
#[derive(Clone)]
pub struct LogicalOwnerFailureObserver {
    original: Weak<OriginalLogicalExit>,
}
impl LogicalOwnerFailureObserver {
    pub fn report(&self, error: QueryExecutionError) -> bool {
        let Some(original) = self.original.upgrade() else {
            return false;
        };
        original.retain_owner_failure_silent(error);
        original.notify();
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeCleanupPreparationPhase {
    Pending,
    Failed,
    Prepared,
}
#[derive(Clone)]
pub struct NativeCleanupPreparation {
    pub execution: QueryExecutionId,
    pub phase: NativeCleanupPreparationPhase,
    // None is an unavailable frozen roster, never an empty roster.
    pub schedule: Option<Arc<AttemptSchedule>>,
    pub binding_complete: bool,
    pub activation_started: Option<AttemptActivationIdentity>,
    pub terminal_cut: Option<(
        AttemptActivationIdentity,
        MonotonicInstant,
        MonotonicInstant,
    )>,
    pub dormant: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeCleanupActive {
    pub activation: AttemptActivationIdentity,
    pub resources_exited: bool,
    pub adopted: bool,
    pub dormant_retired: bool,
    pub terminal_cut: Option<(MonotonicInstant, MonotonicInstant)>,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeCleanupQualification {
    pub identity: QualificationIdentity,
    pub operation: Option<TaskOperationId>,
    pub dispatch_started: bool,
    pub cancellation_issued: bool,
    pub activation_in_flight: bool,
    pub resources_retiring: bool,
    pub dispatch_settled: bool,
    pub qualified_owned: bool,
    pub active_owned: bool,
    pub activation: Option<AttemptActivationIdentity>,
    pub abandoned: bool,
}
#[derive(Clone)]
pub struct NativeCleanupInventory {
    pub initial_activation: AttemptActivationIdentity,
    pub preparations: Vec<NativeCleanupPreparation>,
    pub active: Vec<NativeCleanupActive>,
    pub qualifications: Vec<NativeCleanupQualification>,
}
impl NativeExecutor {
    /// Called only by the original credentialed failure job after its Source
    /// has observed OTHER original jobs/captures exiting. This closes the same
    /// Native body, not the App aliases, current job, actor or Registry Join.
    /// All local markers below belong to real original cleanup operations;
    /// absent resource Options alone never grant this close.
    pub fn close_original_body_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            use crate::coordination::original_exit::OriginalNativeBodyCloseRefusal as Refusal;
            let original = self
                .original_exit
                .upgrade()
                .ok_or_else(|| protocol("Native body cleanup lost its original logical source"))?;
            if !Arc::ptr_eq(&original.native, &self.body) {
                return Err(protocol(Refusal::ForeignSource.detail()));
            }
            // This existing operation verifies the SAME Registry entry,
            // typed owner failure and actual FA receiver failure fence. It
            // closes ingress without waiting for this caller's own HostExit.
            self.registry
                .close_runtime_ingress_after_owner_failure(&self.registration, &original)
                .await
                .map_err(|error| projected(error.failure_fact(), error.safe_detail()))?;
            original
                .close_runtime_native_body_for_cleanup(
                    &self.body,
                    self.registration.initial_execution(),
                    |body| self.qualify_original_body_cleanup(body),
                )
                .await
                .map_err(|refusal| match refusal {
                    Refusal::BodyUnproven | Refusal::BodyDropPanicked => {
                        panic_error(refusal.detail())
                    }
                    _ => protocol(refusal.detail()),
                })
        })
    }

    pub(super) fn qualify_original_body_cleanup(
        &self,
        body: &OriginalNativePreparation,
    ) -> Result<(), crate::coordination::original_exit::OriginalNativeBodyCloseRefusal> {
        use crate::coordination::original_exit::OriginalNativeBodyCloseRefusal as Refusal;
        let session = body.session.as_ref().ok_or(Refusal::BodyUnproven)?;
        if !session.preparation_retired_for_cleanup() {
            return Err(Refusal::LocalWorkPending);
        }
        let storage = self.lock();
        // A separately recovered output is not proof that its original future
        // destructor returned. Preserve that independent local UNKNOWN even
        // after the known dormant/active Box has actually been cleaned.
        if storage.causes.values().any(|causes| {
            causes
                .cleanup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some()
        }) {
            return Err(Refusal::BodyUnproven);
        }
        if storage.dormant_execution.is_some()
            || !body.dormant.owner_retired_for_cleanup()
                && storage.cleaned_dormant_execution.is_none()
        {
            return Err(Refusal::LocalWorkPending);
        }
        for execution in storage.preparations.keys() {
            let active = storage.active.iter().filter(|(key, _)| key.1 == *execution);
            let mut actual_active = false;
            for (_, original) in active {
                actual_active = true;
                if !original.resources_exited
                    || original.cleanup_adopted && !original.dormant_retired_for_cleanup
                {
                    return Err(Refusal::LocalWorkPending);
                }
            }
            if !actual_active && storage.cleaned_dormant_execution != Some(*execution) {
                return Err(Refusal::LocalWorkPending);
            }
        }
        for original in storage.qualifications.values() {
            if original.activation_in_flight
                || original.resources_retiring
                || original.dispatch_started && !original.dispatch_settled
                || !original.abandoned
                || original.qualified.is_some()
                || original.active.is_some()
            {
                return Err(Refusal::LocalWorkPending);
            }
        }
        if !storage.ready_tails.is_empty() {
            return Err(Refusal::LocalWorkPending);
        }
        for slot in storage.tails.values() {
            let state = slot.lock();
            if state.phase != TailPhase::Joined || !state.cleanup_returned {
                return Err(Refusal::LocalWorkPending);
            }
            if state.join_failure.is_some() || slot.causes.failed() {
                return Err(Refusal::BodyUnproven);
            }
        }
        Ok(())
    }

    pub fn owner_failure_observer(&self) -> LogicalOwnerFailureObserver {
        LogicalOwnerFailureObserver {
            original: self.original_exit.clone(),
        }
    }

    /// Recover only the SAME successful pending preparation. An earlier raw
    /// prepare panic is health, not permission to discard this owned output.
    /// No factory, schedule, RPC, activation or logical fact is created here.
    pub fn adopt_pending_preparation_for_cleanup(
        &self,
        execution: QueryExecutionId,
    ) -> LogicalFuture<'_, Result<bool, QueryExecutionError>> {
        Box::pin(async move {
            if !self.lock().preparations.contains_key(&execution) {
                return Err(protocol(
                    "pending preparation cleanup has no original Prepare action",
                ));
            }
            let mut body = self.body.lock().await;
            if body.pending_activated.is_some() {
                return Err(protocol(
                    "preparation cleanup retains an original active output",
                ));
            }
            let pending = body
                .session
                .as_ref()
                .ok_or_else(|| protocol("pending preparation cleanup lost its original session"))?
                .pending_prepared_execution_for_cleanup()
                .map_err(preparation_contract)?;
            let Some(pending) = pending else {
                return Ok(false);
            };
            if pending != execution {
                return Err(protocol(
                    "pending preparation cleanup has another original execution",
                ));
            }
            if self
                .lock()
                .dormant_execution
                .is_some_and(|prior| prior != execution)
            {
                return Err(protocol(
                    "pending preparation cleanup would replace another original dormant owner",
                ));
            }
            let OriginalNativePreparation {
                session, dormant, ..
            } = &mut *body;
            let session = session.as_mut().expect("same original preparation session");
            if session.preparation_drop_unproven() {
                return Err(panic_error(
                    "original preparation destruction remains unproven",
                ));
            }
            let facts = session
                .accept_pending_for_cleanup(execution, dormant)
                .map_err(preparation_contract)?;
            let mut storage = self.lock();
            storage.dormant_execution = Some(facts.execution);
            storage.cleaned_dormant_execution = None;
            // Frozen facts remain original; cleanup does not complete binding
            // or install a guessed schedule for an interrupted preparation.
            Ok(true)
        })
    }

    /// Physically converge/drop the SAME unactivated original. This reports
    /// only local cleanup, never DormantConverged or positive remote closure to
    /// the dead LP. Old raw activation/preparation causes remain retained.
    pub fn converge_dormant_for_owner_failure(
        &self,
        execution: QueryExecutionId,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            if self.lock().cleaned_dormant_execution == Some(execution) {
                return Ok(());
            }
            self.adopt_pending_preparation_for_cleanup(execution)
                .await?;
            let mut body = self.body.lock().await;
            let (phase, dormant) = {
                let storage = self.lock();
                if storage.active.keys().any(|key| key.1 == execution)
                    || body.pending_activated.is_some()
                {
                    return Err(protocol("dormant cleanup has an original active output"));
                }
                (
                    storage.preparations.get(&execution).copied(),
                    storage.dormant_execution,
                )
            };
            if dormant.is_some_and(|original| original != execution) {
                return Err(protocol("dormant cleanup has another original execution"));
            }
            if dormant.is_none() && !body.dormant.has_owner_for_cleanup() {
                if phase != Some(PreparationPhase::Failed) {
                    return Err(protocol(
                        "owner-free preparation cleanup has no original failed result",
                    ));
                }
                let destruction_unknown = self
                    .lock()
                    .causes
                    .range((execution, 0)..=(execution, u8::MAX))
                    .any(|(_, causes)| {
                        causes
                            .cleanup
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .is_some()
                    });
                if destruction_unknown {
                    return Err(panic_error(
                        "original preparation future destruction remains unproven",
                    ));
                }
                let session = body.session.as_mut().ok_or_else(|| {
                    protocol("failed preparation cleanup lost its original session")
                })?;
                if session.preparation_drop_unproven() {
                    return Err(panic_error(
                        "original preparation body destruction remains unproven",
                    ));
                }
                let pending = session
                    .pending_execution_for_cleanup()
                    .map_err(preparation_contract)?;
                if pending.is_some_and(|original| original != execution)
                    || pending.is_none() && !session.preparation_retired_for_cleanup()
                {
                    return Err(protocol(
                        "failed preparation cleanup has no exact original request",
                    ));
                }
                let causes = self.causes(execution, 26);
                if causes.failed() {
                    return Err(panic_error(
                        "failed preparation destruction remains unproven",
                    ));
                }
                if let Err(original) =
                    catch_unwind(AssertUnwindSafe(|| session.retire_preparation()))
                {
                    causes.retain(original, true);
                    return Err(panic_error(
                        "original failed preparation destruction panicked",
                    ));
                }
                if !session.preparation_retired_for_cleanup() {
                    return Err(panic_error("failed preparation destruction is incomplete"));
                }
                self.lock().cleaned_dormant_execution = Some(execution);
                return Ok(());
            }
            if dormant != Some(execution) {
                return Err(protocol(
                    "dormant cleanup lacks the exact original owner binding",
                ));
            }
            let causes = self.causes(execution, 25);
            if causes.failed() {
                return Err(panic_error(
                    "original dormant cleanup source remains unproven",
                ));
            }
            if !body.dormant.convergence_completed() {
                let (owner, original) = body
                    .dormant
                    .convergence_parts()
                    .ok_or_else(|| panic_error("original dormant convergence is unavailable"))?;
                let cancellation = self.cancellation.clone();
                let future = match catch_unwind(AssertUnwindSafe(move || {
                    owner.converge(cancellation)
                })) {
                    Ok(future) => future,
                    Err(payload) => {
                        *original = Some(crate::api::DormantConvergenceFailure::PollPanic(payload));
                        return Err(panic_error("original dormant cleanup factory panicked"));
                    }
                };
                let mut succeeded = false;
                let outcome = poll_original(future, &causes, None, None, |result| match result {
                    Ok(()) => succeeded = true,
                    Err(failure) => {
                        *original = Some(crate::api::DormantConvergenceFailure::Reported(failure))
                    }
                })
                .await;
                if !succeeded || outcome.is_err() || causes.failed() {
                    body.dormant.record_convergence_unproven();
                    return Err(panic_error("original dormant convergence is unproven"));
                }
                body.dormant.record_convergence_completed();
            }
            if body.dormant.convergence_failure().is_some()
                || body.dormant.drop_unproven() && !body.dormant.has_owner_for_cleanup()
            {
                return Err(panic_error("original dormant destruction remains unproven"));
            }
            let drop_causes = self.causes(execution, 26);
            match catch_unwind(AssertUnwindSafe(|| {
                body.dormant.retire_after_convergence_for_cleanup()
            })) {
                Ok(Ok(())) => {}
                Ok(Err(())) => return Err(panic_error("original dormant destruction is unproven")),
                Err(original) => {
                    drop_causes.retain(original, true);
                    return Err(panic_error("original dormant destruction panicked"));
                }
            }
            let mut storage = self.lock();
            storage.dormant_execution = None;
            storage.cleaned_dormant_execution = Some(execution);
            Ok(())
        })
    }

    /// Convert the immutable original failure deadline once when its cleanup
    /// owner starts. Reusing this returned instant never renews the budget.
    pub fn original_cleanup_deadline(&self, deadline: MonotonicInstant) -> Instant {
        self.absolute(deadline)
    }

    /// Request and settle the SAME Registry ingress after original owner
    /// failure. This does not join/retire the Registry or report a local exit.
    pub fn close_registry_ingress_after_owner_failure(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            let original = self
                .original_exit
                .upgrade()
                .ok_or_else(|| protocol("failed ingress close lost its original logical source"))?;
            self.registry
                .close_runtime_ingress_after_owner_failure(&self.registration, &original)
                .await
                .map_err(|error| projected(error.failure_fact(), error.safe_detail()))
        })
    }

    /// Inspect only retained original rows. This linear snapshot belongs in the
    /// credentialed cleanup job, never the bounded actor/Source poller. It does
    /// not wait for a borrowed Native owner or infer any missing owner exited.
    pub fn cleanup_inventory(&self) -> Result<NativeCleanupInventory, QueryExecutionError> {
        let storage = self.lock();
        let mut preparations = Vec::with_capacity(storage.preparations.len());
        for (execution, phase) in &storage.preparations {
            let prepared = storage.prepared.get(execution);
            if prepared.is_some_and(|row| row.schedule.execution() != *execution) {
                return Err(protocol(
                    "cleanup inventory has another original prepared execution",
                ));
            }
            preparations.push(NativeCleanupPreparation {
                execution: *execution,
                phase: match phase {
                    PreparationPhase::Pending => NativeCleanupPreparationPhase::Pending,
                    PreparationPhase::Failed => NativeCleanupPreparationPhase::Failed,
                    PreparationPhase::Prepared => NativeCleanupPreparationPhase::Prepared,
                },
                schedule: prepared.map(|row| Arc::clone(&row.schedule)),
                binding_complete: prepared.is_some_and(|row| row.binding_complete),
                activation_started: prepared.and_then(|row| row.activation_started),
                terminal_cut: prepared.and_then(|row| row.terminal_cut),
                dormant: storage.dormant_execution == Some(*execution),
            });
        }
        if storage
            .prepared
            .keys()
            .any(|execution| !storage.preparations.contains_key(execution))
        {
            return Err(protocol(
                "cleanup inventory has no original preparation phase",
            ));
        }
        let mut active = Vec::with_capacity(storage.active.len());
        for (key, row) in &storage.active {
            // Retain the actual Activate argument rather than rebuilding a full
            // generation from a registry generation or numeric map address.
            let activation = storage
                .prepared
                .get(&key.1)
                .and_then(|prepared| prepared.activation_started)
                .filter(|activation| activation_key(*activation) == *key)
                .ok_or_else(|| {
                    protocol("cleanup inventory lacks the exact original Activate identity")
                })?;
            active.push(NativeCleanupActive {
                activation,
                resources_exited: row.resources_exited,
                adopted: row.cleanup_adopted,
                dormant_retired: row.dormant_retired_for_cleanup,
                terminal_cut: row.terminal_cut,
            });
        }
        let qualifications = storage
            .qualifications
            .iter()
            .map(|(execution, row)| {
                if row.identity.replacement != *execution
                    || row.activation.is_some_and(|activation| {
                        activation.execution() != *execution
                            || activation.actor() != row.identity.actor
                            || activation.generation() != row.identity.generation
                    })
                {
                    return Err(protocol(
                        "cleanup inventory has another original qualification identity",
                    ));
                }
                Ok(NativeCleanupQualification {
                    identity: row.identity,
                    operation: row.operation,
                    dispatch_started: row.dispatch_started,
                    cancellation_issued: row.cancellation_issued,
                    activation_in_flight: row.activation_in_flight,
                    resources_retiring: row.resources_retiring,
                    dispatch_settled: row.dispatch_settled,
                    qualified_owned: row.qualified.is_some(),
                    active_owned: row.active.is_some(),
                    activation: row.activation,
                    abandoned: row.abandoned,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(NativeCleanupInventory {
            initial_activation: self.initial_activation,
            preparations,
            active,
            qualifications,
        })
    }
}

fn preparation_contract(error: crate::api::NativeExecutionContractError) -> QueryExecutionError {
    projected(error.failure_fact(), error.safe_detail())
}
