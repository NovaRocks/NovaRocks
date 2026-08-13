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

//! Frontend-owned operation authority for durable DML mutations.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorClusterIdentity, ConnectorError, ConnectorEstablishedWriteFence,
    ConnectorExternalFenceGeneration, ConnectorExternalFenceReceipt,
    ConnectorExternalOperationFence, ConnectorTableIdentity, ConnectorWriteOperationId,
    ConnectorWriteTargetRef,
};
use novarocks_spi::state_store::{StateStore, TransactionId, WriteTransaction};
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, LeaseGuard, WriteAdmission,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::coordination::FrontendCoordinationRuntime;
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::journal::{
    DmlIntentAdmissionValidator, DmlMutationAuthority, DmlMutationAuthorityValidator,
    OperationJournal, dml_operation_resource_key,
};
use crate::dml::model::{
    AddFilesMutationRequest, DML_COORDINATION_RESOURCE_CODEC_VERSION,
    DML_FOREGROUND_RECOVERY_VISIBILITY_MS, DmlCoordinationClaimRequest, DmlCoordinationProvenance,
    DmlCtasRecoveryMutationRequest, DmlCtasRecoveryRecord, DmlDirectMutationFenceMutationRequest,
    DmlDirectMutationFenceReceiptRecord, DmlDirectMutationKind, DmlExternalFenceGeneration,
    DmlExternalFenceMutationRequest, DmlExternalFenceReceiptRecord, DmlFencingTokenV1,
    DmlHistoricalWriteRecoveryRecord, DmlOperationId, DmlRecoveryDueRescheduleRequest,
    OperationFact, OperationMutationRequest, OperationPayload, OperationState, StoredOperation,
    operation_requires_recovery_scan,
};
use crate::dml::now_unix_millis;

#[derive(Clone)]
// Design: ADR-0054 (docs/adr/ADR-0054-frontend-dml-operation-authority-boundary.md)
pub(crate) struct DmlCoordinator {
    frontend: Arc<FrontendCoordinationRuntime>,
    runtime: Handle,
    closing: Arc<AtomicBool>,
    active: Arc<StdMutex<BTreeMap<DmlOperationId, Weak<DmlOperationAuthorityInner>>>>,
}

impl DmlCoordinator {
    pub(crate) fn new(frontend: Arc<FrontendCoordinationRuntime>, runtime: Handle) -> Self {
        Self {
            frontend,
            runtime,
            closing: Arc::new(AtomicBool::new(false)),
            active: Arc::new(StdMutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn admission(&self) -> Result<Arc<dyn DmlIntentAdmissionValidator>, DmlError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is shutting down",
            ));
        }
        let admission = self.blocking(async {
            self.frontend
                .admit_writes()
                .await
                .map_err(map_coordination_error)
        })?;
        Ok(Arc::new(CurrentDmlAdmission { admission }))
    }

    pub(crate) fn claim_foreground(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let admission = self.admission()?;
        self.claim_inner(journal, operation, Some(admission))
    }

    pub(crate) fn claim_recovery(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
    ) -> Result<ActiveDmlOperation, DmlError> {
        self.claim_inner(journal, operation, None)
    }

    fn claim_inner(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
    ) -> Result<ActiveDmlOperation, DmlError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is shutting down",
            )
            .with_operation_id(operation.operation_id));
        }
        let attempt_uuid = Uuid::now_v7();
        let attempt = AttemptId::try_from(attempt_uuid).map_err(map_coordination_error)?;
        let acquire_operation_uuid = Uuid::now_v7();
        let acquire_operation_id = OperationId::from(acquire_operation_uuid);
        let resource = dml_operation_resource_key(operation.operation_id)?;
        let manager = self.frontend.lease_manager();
        let outcome = self.blocking(async {
            match manager
                .acquire(resource.clone(), attempt, acquire_operation_id)
                .await
            {
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => manager
                    .recover_acquire(resource, attempt, acquire_operation_id)
                    .await
                    .map_err(map_coordination_error),
                Ok(outcome) => Ok(outcome),
                Err(error) => Err(map_coordination_error(error)),
            }
        })?;
        let guard = match outcome {
            AcquireOutcome::Acquired(guard) => guard,
            AcquireOutcome::Contended(observation) => {
                return Err(DmlError::coordination_contended(format!(
                    "DML operation lease is contended; retry after {:?}",
                    observation.retry_after()
                ))
                .with_operation_id(operation.operation_id));
            }
            AcquireOutcome::AwaitingTakeover(observation) => {
                return Err(DmlError::coordination_contended(format!(
                    "DML operation lease awaits takeover observation; retry after {:?}",
                    observation.retry_after()
                ))
                .with_operation_id(operation.operation_id));
            }
        };

        let inner = Arc::new(DmlOperationAuthorityInner {
            operation_id: operation.operation_id,
            coordination_attempt_id: attempt_uuid,
            guard: Arc::new(Mutex::new(guard)),
            lost: AtomicBool::new(false),
            released: AtomicBool::new(false),
            stop: watch::channel(false).0,
            renewal: StdMutex::new(None),
        });
        let authority = DmlOperationAuthority {
            inner: Arc::clone(&inner),
            runtime: self.runtime.clone(),
            active: Arc::downgrade(&self.active),
            closing: Arc::clone(&self.closing),
            store: self.frontend.store(),
        };
        let token = self.blocking(async {
            let guard = inner.guard.lock().await;
            DmlFencingTokenV1::try_from_token(guard.token()).map_err(DmlError::journal_corruption)
        })?;
        let provenance = DmlCoordinationProvenance {
            resource_codec_version: DML_COORDINATION_RESOURCE_CODEC_VERSION,
            holder_id: self.frontend.holder_uuid(),
            coordination_attempt_id: attempt_uuid,
            fencing_token: token,
            acquired_at_ms: now_unix_millis(),
        };
        let claim = DmlCoordinationClaimRequest {
            operation_id: operation.operation_id,
            expected_revision: operation.revision,
            mutation_id: Uuid::now_v7(),
            provenance,
            recovery_due_at_ms: now_unix_millis()
                .saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS),
        };
        let journal_authority = authority.journal_authority()?;
        let claim_result = match admission {
            Some(admission) => {
                journal.claim_operation_admitted(claim, admission, journal_authority)
            }
            None => journal.claim_operation(claim, journal_authority),
        };
        let claimed = match claim_result {
            Ok(claimed) => claimed,
            Err(error) => {
                let _ = authority.release();
                return Err(error.with_operation_id(operation.operation_id));
            }
        };
        authority.start_renewal();
        {
            let mut active = self
                .active
                .lock()
                .expect("DML authority registry lock poisoned");
            if self.closing.load(Ordering::Acquire) {
                drop(active);
                let _ = authority.release();
                return Err(DmlError::coordination_unresolved(
                    "frontend DML coordination shut down while claiming an operation",
                )
                .with_operation_id(operation.operation_id));
            }
            active.insert(operation.operation_id, Arc::downgrade(&inner));
        }
        Ok(ActiveDmlOperation {
            journal,
            stored: claimed,
            authority: Some(authority),
            #[cfg(test)]
            testing_fence: None,
        })
    }

    pub(crate) async fn shutdown(&self) -> Result<(), DmlError> {
        self.closing.store(true, Ordering::Release);
        let authorities = self
            .active
            .lock()
            .expect("DML authority registry lock poisoned")
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for inner in &authorities {
            inner.lost.store(true, Ordering::Release);
            inner.stop.send_replace(true);
        }
        let mut first_error = None;
        for inner in authorities {
            let authority = DmlOperationAuthority {
                inner,
                runtime: self.runtime.clone(),
                active: Arc::downgrade(&self.active),
                closing: Arc::clone(&self.closing),
                store: self.frontend.store(),
            };
            if let Err(error) = authority.release_async().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.active
            .lock()
            .expect("DML authority registry lock poisoned")
            .clear();
        first_error.map_or(Ok(()), Err)
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, DmlError>>,
    ) -> Result<T, DmlError> {
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                ))
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }
}

struct CurrentDmlAdmission {
    admission: WriteAdmission,
}

#[async_trait]
impl DmlIntentAdmissionValidator for CurrentDmlAdmission {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        self.admission
            .validate_in(transaction)
            .await
            .map_err(map_coordination_error)
    }
}

/// The control-plane half of one external operation fence (CP-3B spec D2).
///
/// The frontend owns cluster identity, the CP-1 generation scalars and the
/// coordination attempt; the connector owns the write operation identity and the
/// fenced resource. A proposal is therefore incomplete on purpose: only
/// [`DmlExternalFenceProposal::seal`] turns it into the SPI fence value a
/// provider compares at its external linearization point.
///
/// Every proposal is minted from the *current* fencing token of the live
/// operation lease. CP-3A rule 3 forbids capturing a one-shot snapshot: a renew
/// replaces the exact lease record version and a takeover replaces the resource
/// epoch, so a captured token could mint a fence that no longer matches durable
/// coordination truth.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlExternalFenceProposal {
    operation_id: DmlOperationId,
    cluster_id: String,
    coordination_attempt_id: Uuid,
    generation: DmlExternalFenceGeneration,
}

impl DmlExternalFenceProposal {
    fn try_new(
        operation_id: DmlOperationId,
        cluster_id: String,
        coordination_attempt_id: Uuid,
        generation: DmlExternalFenceGeneration,
    ) -> Result<Self, DmlError> {
        if cluster_id.is_empty() {
            return Err(DmlError::journal_corruption(
                "DML external fence requires a non-empty control-plane cluster id",
            ));
        }
        if generation.control_plane_incarnation == 0
            || generation.resource_epoch == 0
            || generation.fence_generation == 0
        {
            return Err(DmlError::journal_corruption(
                "DML external fence generation components must all be nonzero",
            ));
        }
        Ok(Self {
            operation_id,
            cluster_id,
            coordination_attempt_id,
            generation,
        })
    }

    /// Focused-test proposal. Production always mints one from the live lease
    /// guard through [`DmlOperationAuthority::external_fence`].
    #[cfg(test)]
    pub(crate) fn testing(
        operation_id: DmlOperationId,
        cluster_id: &str,
        coordination_attempt_id: Uuid,
        generation: DmlExternalFenceGeneration,
    ) -> Result<Self, DmlError> {
        Self::try_new(
            operation_id,
            cluster_id.to_string(),
            coordination_attempt_id,
            generation,
        )
    }

    pub const fn operation_id(&self) -> DmlOperationId {
        self.operation_id
    }

    pub const fn coordination_attempt_id(&self) -> Uuid {
        self.coordination_attempt_id
    }

    pub const fn generation(&self) -> DmlExternalFenceGeneration {
        self.generation
    }

    /// Complete this proposal into the sealed SPI fence value.
    ///
    /// The connector-owned half must come from the exact write authority that
    /// will later commit, abort or reconcile: the frontend never invents a
    /// connector write operation identity or a fenced resource identity.
    pub fn seal(
        &self,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableIdentity,
        target_ref: ConnectorWriteTargetRef,
    ) -> Result<ConnectorExternalOperationFence, ConnectorError> {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive(&self.cluster_id)?,
            ConnectorExternalFenceGeneration::try_new(
                self.generation.control_plane_incarnation,
                self.generation.resource_epoch,
                self.generation.fence_generation,
            )?,
            operation_id,
            *self.coordination_attempt_id.as_bytes(),
            table,
            target_ref,
        )
    }

    /// Fail closed unless `established` is exactly the fence this proposal
    /// asked for and its receipt acknowledges that same fence.
    ///
    /// This is the frontend half of the CP-3B D5 double check: an executor that
    /// established some *other* fence must never be able to make the runner
    /// record a receipt for it and then dispatch.
    pub fn validate_established(
        &self,
        established: &ConnectorEstablishedWriteFence,
    ) -> Result<(), DmlError> {
        self.validate_established_receipt(established.fence(), established.receipt())
    }

    /// The same double check for a provider that returns the bare receipt.
    ///
    /// Direct mutation establishes its fence through the data-mutation lease,
    /// which hands back only the receipt, so the caller supplies the fence it
    /// sealed. The check itself is identical and deliberately shared: a
    /// TRUNCATE must not be able to journal a receipt for some other fence and
    /// then dispatch.
    pub fn validate_established_receipt(
        &self,
        fence: &ConnectorExternalOperationFence,
        receipt: &ConnectorExternalFenceReceipt,
    ) -> Result<(), DmlError> {
        fence
            .validate()
            .map_err(|error| self.mismatch(format!("fence is not sealed: {error}")))?;
        receipt
            .validate()
            .map_err(|error| self.mismatch(format!("fence receipt is not sealed: {error}")))?;
        if !receipt.matches(fence) {
            return Err(self.mismatch("fence receipt acknowledges another fence"));
        }
        let expected_cluster = ConnectorClusterIdentity::derive(&self.cluster_id)
            .map_err(|error| self.mismatch(format!("cluster identity is unusable: {error}")))?;
        if fence.cluster() != expected_cluster {
            return Err(self.mismatch("fence was minted for another cluster identity"));
        }
        if fence.coordination_attempt_id() != *self.coordination_attempt_id.as_bytes() {
            return Err(self.mismatch("fence was minted for another coordination attempt"));
        }
        let generation = fence.generation();
        if generation.control_plane_incarnation() != self.generation.control_plane_incarnation
            || generation.resource_epoch() != self.generation.resource_epoch
            || generation.coordination_attempt() != self.generation.fence_generation
        {
            return Err(self.mismatch("fence generation does not match the proposed generation"));
        }
        Ok(())
    }

    fn mismatch(&self, reason: impl fmt::Display) -> DmlError {
        DmlError::journal_corruption(format!(
            "established external operation fence does not match this coordination attempt: {reason}"
        ))
        .with_operation_id(self.operation_id)
    }
}

/// Derive the fence generation from one observed CP-1 fencing token.
///
/// This is a pure projection of whatever token the caller just read from the
/// live lease guard. Keeping it pure makes the "always the latest token" rule
/// checkable: every field comes from the argument, so nothing about an earlier
/// token can survive into a later fence.
fn external_fence_generation(
    token: &novarocks_state_store::coordination::FencingToken,
    coordination_attempt_id: Uuid,
) -> Result<DmlExternalFenceGeneration, DmlError> {
    Ok(DmlExternalFenceGeneration {
        control_plane_incarnation: token.control_plane_incarnation().get(),
        resource_epoch: token.resource_epoch().get(),
        fence_generation: coordination_attempt_fence_generation(coordination_attempt_id)?,
    })
}

/// Derive the provider-visible monotone fence generation of one coordination
/// attempt from its UUIDv7 identity.
///
/// One resource epoch admits exactly one CP-3A claim, so the epoch already
/// separates attempts; this component keeps the total order strict inside an
/// epoch and stays monotone across claims because a later claim mints a later
/// UUIDv7 timestamp.
fn coordination_attempt_fence_generation(attempt_id: Uuid) -> Result<u64, DmlError> {
    let timestamp = attempt_id.get_timestamp().ok_or_else(|| {
        DmlError::journal_corruption("DML coordination attempt id carries no UUIDv7 timestamp")
    })?;
    let (seconds, nanos) = timestamp.to_unix();
    let generation = seconds
        .saturating_mul(1_000)
        .saturating_add(u64::from(nanos) / 1_000_000);
    if generation == 0 {
        return Err(DmlError::journal_corruption(
            "DML coordination attempt id has a zero UUIDv7 timestamp",
        ));
    }
    Ok(generation)
}

#[derive(Clone)]
pub(crate) struct DmlOperationAuthority {
    inner: Arc<DmlOperationAuthorityInner>,
    runtime: Handle,
    active: Weak<StdMutex<BTreeMap<DmlOperationId, Weak<DmlOperationAuthorityInner>>>>,
    closing: Arc<AtomicBool>,
    store: Arc<dyn StateStore>,
}

struct DmlOperationAuthorityInner {
    operation_id: DmlOperationId,
    coordination_attempt_id: Uuid,
    guard: Arc<Mutex<LeaseGuard>>,
    lost: AtomicBool,
    released: AtomicBool,
    stop: watch::Sender<bool>,
    renewal: StdMutex<Option<JoinHandle<()>>>,
}

impl DmlOperationAuthority {
    fn abandon(&self) {
        if self.inner.released.load(Ordering::Acquire) {
            return;
        }
        self.inner.lost.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
        let this = self.clone();
        self.runtime.spawn(async move {
            if let Err(error) = this.release_async().await {
                tracing::warn!(
                    operation_id = %this.inner.operation_id,
                    error = %error,
                    "best-effort DML authority release after operation drop failed"
                );
            }
        });
    }

    pub(crate) fn check_before_dispatch(&self) -> Result<(), DmlError> {
        if self.closing.load(Ordering::Acquire)
            || self.inner.lost.load(Ordering::Acquire)
            || self.inner.released.load(Ordering::Acquire)
        {
            return Err(DmlError::coordination_lost(
                "DML operation authority is no longer current",
            )
            .with_operation_id(self.inner.operation_id));
        }
        let runtime = self.runtime.clone();
        let this = self.clone();
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                )
                .with_operation_id(self.inner.operation_id))
            }
            Ok(_) => tokio::task::block_in_place(|| runtime.block_on(this.check_current_async())),
            Err(_) => runtime.block_on(this.check_current_async()),
        }
    }

    async fn check_current_async(&self) -> Result<(), DmlError> {
        let mut transaction = self
            .store
            .begin_write(
                TransactionId::from(Uuid::now_v7()),
                "validate DML operation authority before provider dispatch",
            )
            .await
            .map_err(DmlError::coordination_unresolved)?;
        let guard = self.inner.guard.lock().await;
        let validation = guard
            .fence()
            .validate_in(transaction.as_mut())
            .await
            .map_err(map_coordination_error);
        drop(guard);
        let abort = transaction
            .abort()
            .await
            .map_err(DmlError::coordination_unresolved);
        if validation.is_err() {
            self.inner.lost.store(true, Ordering::Release);
        }
        validation
            .and(abort)
            .map_err(|error| error.with_operation_id(self.inner.operation_id))
    }

    pub(crate) fn journal_authority(&self) -> Result<DmlMutationAuthority, DmlError> {
        DmlMutationAuthority::try_new(
            self.inner.coordination_attempt_id,
            Arc::new(CurrentDmlLeaseFence {
                guard: Arc::clone(&self.inner.guard),
                lost: Arc::downgrade(&self.inner),
            }),
        )
    }

    /// Mint this attempt's external operation fence proposal.
    ///
    /// The generation scalars are read from the live `LeaseGuard` on every
    /// call, exactly like the dynamic latest-fence validator above. Nothing is
    /// cached: a renewed or superseded lease must be observable here, so no
    /// caller can dispatch behind a fence built from a stale token.
    pub(crate) fn external_fence(&self) -> Result<DmlExternalFenceProposal, DmlError> {
        let runtime = self.runtime.clone();
        let this = self.clone();
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                )
                .with_operation_id(self.inner.operation_id))
            }
            Ok(_) => tokio::task::block_in_place(|| runtime.block_on(this.external_fence_async())),
            Err(_) => runtime.block_on(this.external_fence_async()),
        }
    }

    async fn external_fence_async(&self) -> Result<DmlExternalFenceProposal, DmlError> {
        if self.closing.load(Ordering::Acquire)
            || self.inner.lost.load(Ordering::Acquire)
            || self.inner.released.load(Ordering::Acquire)
        {
            return Err(DmlError::coordination_lost(
                "DML operation authority is no longer current",
            )
            .with_operation_id(self.inner.operation_id));
        }
        let (cluster_id, generation) = {
            // One lock, one observation. Reading the cluster id and the
            // generation scalars from two separate acquisitions could straddle
            // a renewal and mint a fence that never existed as a whole.
            let guard = self.inner.guard.lock().await;
            let token = guard.token();
            (
                token.cluster_id().to_string(),
                external_fence_generation(token, self.inner.coordination_attempt_id)?,
            )
        };
        DmlExternalFenceProposal::try_new(
            self.inner.operation_id,
            cluster_id,
            self.inner.coordination_attempt_id,
            generation,
        )
        .map_err(|error| error.with_operation_id(self.inner.operation_id))
    }

    fn start_renewal(&self) {
        let weak = Arc::downgrade(&self.inner);
        let mut stop = self.inner.stop.subscribe();
        let handle = self.runtime.spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let renew_after = {
                    let guard = inner.guard.lock().await;
                    guard.renew_after()
                };
                drop(inner);
                tokio::select! {
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(renew_after) => {}
                }
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let operation_id = OperationId::new_v7();
                let result = {
                    let mut guard = inner.guard.lock().await;
                    match guard.renew(operation_id).await {
                        Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                            guard.recover_renew(operation_id).await
                        }
                        result => result,
                    }
                };
                if result.is_err() {
                    inner.lost.store(true, Ordering::Release);
                    return;
                }
            }
        });
        *self
            .inner
            .renewal
            .lock()
            .expect("DML renewal task lock poisoned") = Some(handle);
    }

    pub(crate) fn release(&self) -> Result<(), DmlError> {
        let runtime = self.runtime.clone();
        let this = self.clone();
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                )
                .with_operation_id(self.inner.operation_id))
            }
            Ok(_) => tokio::task::block_in_place(|| runtime.block_on(this.release_async())),
            Err(_) => runtime.block_on(this.release_async()),
        }
    }

    async fn release_async(&self) -> Result<(), DmlError> {
        if self.inner.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.stop.send_replace(true);
        let renewal = self
            .inner
            .renewal
            .lock()
            .expect("DML renewal task lock poisoned")
            .take();
        if let Some(renewal) = renewal {
            let _ = renewal.await;
        }
        let operation_id = OperationId::new_v7();
        let result = {
            let mut guard = self.inner.guard.lock().await;
            match guard.release(operation_id).await {
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                    guard.recover_release(operation_id).await
                }
                result => result,
            }
        };
        if let Some(active) = self.active.upgrade() {
            let mut active = active.lock().expect("DML authority registry lock poisoned");
            let remove = active
                .get(&self.inner.operation_id)
                .and_then(Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, &self.inner));
            if remove {
                active.remove(&self.inner.operation_id);
            }
        }
        result.map_err(|error| {
            map_coordination_error(error).with_operation_id(self.inner.operation_id)
        })
    }
}

struct CurrentDmlLeaseFence {
    guard: Arc<Mutex<LeaseGuard>>,
    lost: Weak<DmlOperationAuthorityInner>,
}

#[async_trait]
impl DmlMutationAuthorityValidator for CurrentDmlLeaseFence {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        let guard = self.guard.lock().await;
        let result = guard.fence().validate_in(transaction).await;
        if result.is_err()
            && let Some(inner) = self.lost.upgrade()
        {
            inner.lost.store(true, Ordering::Release);
        }
        result.map_err(map_coordination_error)
    }
}

pub(crate) struct ActiveDmlOperation {
    pub(crate) journal: Arc<dyn OperationJournal>,
    pub(crate) stored: StoredOperation,
    authority: Option<DmlOperationAuthority>,
    /// Focused-test substitute for the live coordination authority. Production
    /// always mints the fence proposal from the live lease guard above; this
    /// field only exists so unit tests can drive the fence-before-dispatch
    /// ordering without a StateStore-backed lease manager.
    #[cfg(test)]
    testing_fence: Option<TestingExternalFenceAuthority>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestingExternalFenceAuthority {
    proposal: DmlExternalFenceProposal,
    validator: Arc<dyn DmlMutationAuthorityValidator>,
}

impl Drop for ActiveDmlOperation {
    fn drop(&mut self) {
        if let Some(authority) = &self.authority {
            authority.abandon();
        }
    }
}

impl ActiveDmlOperation {
    pub(crate) fn legacy(journal: Arc<dyn OperationJournal>, stored: StoredOperation) -> Self {
        Self {
            journal,
            stored,
            authority: None,
            #[cfg(test)]
            testing_fence: None,
        }
    }

    /// Focused-test operation that carries a fixed external fence proposal and
    /// an always-valid journal authority.
    ///
    /// This is `cfg(test)` on purpose: it must never become a production path
    /// that can mint a fence without a live coordination lease.
    #[cfg(test)]
    pub(crate) fn testing_fenced(
        journal: Arc<dyn OperationJournal>,
        stored: StoredOperation,
        proposal: DmlExternalFenceProposal,
        validator: Arc<dyn DmlMutationAuthorityValidator>,
    ) -> Self {
        Self {
            journal,
            stored,
            authority: None,
            testing_fence: Some(TestingExternalFenceAuthority {
                proposal,
                validator,
            }),
        }
    }

    pub(crate) fn operation_id(&self) -> DmlOperationId {
        self.stored.operation_id
    }

    /// Mint this attempt's external operation fence proposal from the live
    /// coordination authority.
    ///
    /// There is deliberately no unfenced answer: an operation without a
    /// coordination authority cannot prove which control-plane generation owns
    /// it, so it must not reach a writer or a commit at all.
    pub(crate) fn external_fence(&self) -> Result<DmlExternalFenceProposal, DmlError> {
        if let Some(authority) = self.authority.as_ref() {
            return authority.external_fence();
        }
        #[cfg(test)]
        if let Some(testing) = self.testing_fence.as_ref() {
            return Ok(testing.proposal.clone());
        }
        Err(
            DmlError::coordination_unresolved("DML operation has no coordination authority")
                .with_operation_id(self.operation_id()),
        )
    }

    /// Persist the confirmed external fence receipt of this attempt.
    ///
    /// The journal validates the dynamic latest lease fence, the expected
    /// operation revision and the coordination attempt inside the same
    /// StateStore transaction that writes the receipt, so a superseded holder
    /// can never install one.
    /// Refuse a fence this journal could never hold, before any external
    /// marker can exist.
    ///
    /// Establishing first and failing to record afterwards would leave a
    /// provider-side marker with no durable frontend proof, which is exactly
    /// the ambiguity historical write recovery must not have to guess about.
    pub(crate) fn preflight_external_fence(
        &self,
        proposal: &DmlExternalFenceProposal,
    ) -> Result<(), DmlError> {
        let probe = crate::dml::reconcile::external_fence_preflight_probe(
            *self.operation_id().as_uuid(),
            proposal.coordination_attempt_id(),
            proposal.generation(),
        )
        .map_err(DmlError::journal_corruption)?;
        self.journal
            .preflight_external_fence(&DmlExternalFenceMutationRequest {
                operation_id: self.operation_id(),
                expected_revision: self.stored.revision,
                mutation_id: Uuid::now_v7(),
                fence: probe,
            })
            .map_err(|error| error.with_operation_id(self.operation_id()))
    }

    pub(crate) fn record_external_fence(
        &mut self,
        fence: DmlExternalFenceReceiptRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let request = DmlExternalFenceMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            fence,
        };
        // Reject a receipt this journal could never encode before it is treated
        // as durable. A fence that cannot be recorded must not license
        // dispatch.
        self.journal
            .preflight_external_fence(&request)
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        self.stored = self
            .journal
            .record_external_fence_authorized(
                request,
                recovery_due_at_ms,
                self.external_fence_journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    /// Refuse a direct-mutation fence this journal could never hold, before any
    /// external marker can exist.
    ///
    /// TRUNCATE and ADD FILES publish a real provider marker, so establishing
    /// first and failing to record afterwards would leave external truth fenced
    /// with no durable frontend proof — exactly the ambiguity historical
    /// data-mutation recovery must not have to guess about.
    pub(crate) fn preflight_direct_mutation_fence(
        &self,
        proposal: &DmlExternalFenceProposal,
        operation_kind: DmlDirectMutationKind,
        source_scope_digest: Option<String>,
    ) -> Result<(), DmlError> {
        let probe = crate::dml::reconcile::direct_mutation_fence_preflight_probe(
            operation_kind,
            *self.operation_id().as_uuid(),
            proposal.coordination_attempt_id(),
            proposal.generation(),
            source_scope_digest,
        )
        .map_err(DmlError::journal_corruption)?;
        self.journal
            .preflight_direct_mutation_fence(&DmlDirectMutationFenceMutationRequest {
                operation_id: self.operation_id(),
                expected_revision: self.stored.revision,
                mutation_id: Uuid::now_v7(),
                fence: probe,
            })
            .map_err(|error| error.with_operation_id(self.operation_id()))
    }

    /// Persist the confirmed direct-mutation fence receipt of this attempt.
    ///
    /// The journal validates the dynamic latest lease fence, the expected
    /// operation revision, and the coordination attempt inside the same
    /// StateStore transaction that writes the receipt, so a superseded holder
    /// can never install one.
    pub(crate) fn record_direct_mutation_fence(
        &mut self,
        fence: DmlDirectMutationFenceReceiptRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let request = DmlDirectMutationFenceMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            fence,
        };
        // Reject a receipt this journal could never encode before it is treated
        // as durable. A fence that cannot be recorded must not license
        // dispatch.
        self.journal
            .preflight_direct_mutation_fence(&request)
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        self.stored = self
            .journal
            .record_direct_mutation_fence_authorized(
                request,
                recovery_due_at_ms,
                self.external_fence_journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    /// Validate and persist the provider-neutral CTAS recovery side record
    /// under the same live operation authority as the top-level saga.
    pub(crate) fn record_ctas_recovery(
        &mut self,
        recovery: DmlCtasRecoveryRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let recovery_due_at_ms = self.effective_recovery_due(
            self.stored.state,
            &self.stored.payload,
            recovery_due_at_ms,
        )?;
        let request = DmlCtasRecoveryMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            recovery,
        };
        self.journal
            .preflight_ctas_recovery(&request)
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        self.stored = self
            .journal
            .record_ctas_recovery_authorized(
                request,
                recovery_due_at_ms,
                self.external_fence_journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    fn external_fence_journal_authority(&self) -> Result<DmlMutationAuthority, DmlError> {
        #[cfg(test)]
        if self.authority.is_none()
            && let Some(testing) = self.testing_fence.as_ref()
        {
            return DmlMutationAuthority::try_new(
                testing.proposal.coordination_attempt_id(),
                Arc::clone(&testing.validator),
            );
        }
        self.journal_authority()
    }

    pub(crate) fn check_before_dispatch(&self) -> Result<(), DmlError> {
        self.authority
            .as_ref()
            .map_or(Ok(()), DmlOperationAuthority::check_before_dispatch)
    }

    pub(crate) fn transition(
        &mut self,
        to: OperationState,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        self.check_before_dispatch()?;
        let recovery_due_at_ms =
            self.effective_recovery_due(to, &self.stored.payload, recovery_due_at_ms)?;
        if self.authority.is_none() {
            self.journal.transition(self.operation_id(), to)?;
            self.reload()?;
            return Ok(());
        }
        self.stored = self
            .journal
            .transition_authorized(
                self.operation_id(),
                self.stored.revision,
                Uuid::now_v7(),
                to,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn record_fact(
        &mut self,
        fact: OperationFact,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let payload = OperationPayload::ConnectorWriteLifecycle(fact.lifecycle.clone());
        let recovery_due_at_ms =
            self.effective_recovery_due(fact.state, &payload, recovery_due_at_ms)?;
        if self.authority.is_none() {
            self.journal.record_fact(self.operation_id(), fact)?;
            self.reload()?;
            return Ok(());
        }
        self.stored = self
            .journal
            .record_fact_authorized(
                self.operation_id(),
                self.stored.revision,
                Uuid::now_v7(),
                fact,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn mutate_statement(
        &mut self,
        state: OperationState,
        payload: OperationPayload,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let recovery_due_at_ms =
            self.effective_recovery_due(state, &payload, recovery_due_at_ms)?;
        if self.authority.is_none() {
            self.stored = self
                .journal
                .mutate_statement_operation(OperationMutationRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    state,
                    payload,
                })?;
            return Ok(());
        }
        self.stored = self
            .journal
            .mutate_statement_operation_authorized(
                OperationMutationRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    state,
                    payload,
                },
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn apply_add_files_mutation(
        &mut self,
        mut request: AddFilesMutationRequest,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let recovery_due_at_ms = self.effective_recovery_due(
            request.operation.state,
            &request.operation.payload,
            recovery_due_at_ms,
        )?;
        request.operation.expected_revision = self.stored.revision;
        request.operation.mutation_id = Uuid::now_v7();
        if self.authority.is_none() {
            self.stored = self.journal.apply_add_files_mutation(request)?;
            return Ok(());
        }
        self.stored = self
            .journal
            .apply_add_files_mutation_authorized(
                request,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn reschedule_recovery_due(
        &mut self,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        self.stored = self
            .journal
            .reschedule_recovery_due(
                DmlRecoveryDueRescheduleRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    recovery_due_at_ms,
                },
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn release(&self) -> Result<(), DmlError> {
        self.authority
            .as_ref()
            .map_or(Ok(()), DmlOperationAuthority::release)
    }

    pub(crate) fn journal_authority(&self) -> Result<DmlMutationAuthority, DmlError> {
        #[cfg(test)]
        if self.authority.is_none()
            && let Some(testing) = self.testing_fence.as_ref()
        {
            return DmlMutationAuthority::try_new(
                testing.proposal.coordination_attempt_id(),
                Arc::clone(&testing.validator),
            );
        }
        self.authority
            .as_ref()
            .ok_or_else(|| {
                DmlError::coordination_unresolved("DML operation has no coordination authority")
            })?
            .journal_authority()
    }

    fn reload(&mut self) -> Result<(), DmlError> {
        self.stored = self.journal.load(self.operation_id())?.ok_or_else(|| {
            DmlError::journal_unresolved(format!(
                "DML operation {} cannot be read back after mutation",
                self.operation_id()
            ))
        })?;
        Ok(())
    }

    /// The recovery due this mutation must keep.
    ///
    /// A terminal statement result may only drop the due once nothing else
    /// still needs the bounded recovery scan. CP-3B and CP-3D add obligations
    /// the operation state cannot express: open historical write recovery and
    /// retained CTAS staging must survive a terminal user-visible outcome.
    fn effective_recovery_due(
        &self,
        state: OperationState,
        payload: &OperationPayload,
        requested: Option<i64>,
    ) -> Result<Option<i64>, DmlError> {
        if state.is_finished() {
            let historical = self.open_historical_write_recovery()?;
            let ctas = self.open_ctas_recovery()?;
            if !operation_requires_recovery_scan(state, payload, historical.as_ref())
                && ctas
                    .as_ref()
                    .is_none_or(|record| !record.requires_recovery_scan())
            {
                return Ok(None);
            }
        }
        Ok(requested.or(self.stored.recovery_due_at_ms).or_else(|| {
            Some(now_unix_millis().saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS))
        }))
    }

    /// The open historical write recovery record, if this journal keeps one.
    ///
    /// A journal without the CP-3B side record cannot hold a recovery
    /// obligation, so "unsupported" and "absent" are the same answer. Every
    /// other failure propagates: a journal that could answer but did not must
    /// never be allowed to silently drop a pending cleanup.
    fn open_historical_write_recovery(
        &self,
    ) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
        match self
            .journal
            .load_historical_write_recovery(self.operation_id())
        {
            Ok(record) => Ok(record),
            Err(error) if error.kind() == DmlErrorKind::JournalUnavailable => Ok(None),
            Err(error) => Err(error.with_operation_id(self.operation_id())),
        }
    }

    /// The open CTAS recovery record, if this journal keeps one.
    fn open_ctas_recovery(&self) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
        match self.journal.load_ctas_recovery(self.operation_id()) {
            Ok(record) => Ok(record),
            Err(error) if error.kind() == DmlErrorKind::JournalUnavailable => Ok(None),
            Err(error) => Err(error.with_operation_id(self.operation_id())),
        }
    }
}

fn map_coordination_error(error: CoordinationError) -> DmlError {
    match error.kind() {
        CoordinationErrorKind::WriteClosed | CoordinationErrorKind::NotBootstrapped => {
            DmlError::admission(error)
        }
        CoordinationErrorKind::FenceLost
        | CoordinationErrorKind::IncarnationChanged
        | CoordinationErrorKind::ClockUnsafe => DmlError::coordination_lost(error),
        CoordinationErrorKind::CommitUncertain
        | CoordinationErrorKind::OperationNotCommitted
        | CoordinationErrorKind::StoreUnavailable => DmlError::coordination_unresolved(error),
        CoordinationErrorKind::InvalidRequest
        | CoordinationErrorKind::LimitExceeded
        | CoordinationErrorKind::EpochExhausted
        | CoordinationErrorKind::IncarnationExhausted
        | CoordinationErrorKind::Corruption => DmlError::journal_corruption(error),
    }
}

#[cfg(test)]
mod tests {
    use novarocks_state_store::coordination::{
        ControlPlaneIncarnation, FencingToken, ResourceEpoch,
    };

    use super::*;

    fn token(incarnation: u64, epoch: u64) -> FencingToken {
        FencingToken::new(
            "nova-cluster",
            ControlPlaneIncarnation::new(incarnation).expect("incarnation"),
            ResourceEpoch::new(epoch).expect("epoch"),
        )
        .expect("fencing token")
    }

    #[test]
    fn the_fence_generation_follows_whatever_token_was_just_observed() {
        let attempt = Uuid::now_v7();
        let acquired = external_fence_generation(&token(1, 4), attempt).expect("acquired");
        // A takeover raises the resource epoch, and a new control plane raises
        // the incarnation. Both must be visible to a fence minted afterwards,
        // which is only true because the projection reads the supplied token.
        let renewed = external_fence_generation(&token(1, 5), attempt).expect("renewed");
        let reincarnated = external_fence_generation(&token(2, 1), attempt).expect("reincarnated");

        assert!(renewed > acquired, "a raised resource epoch outranks");
        assert!(
            reincarnated > renewed,
            "a raised control plane incarnation outranks any resource epoch"
        );
        assert_eq!(acquired.fence_generation, renewed.fence_generation);
        assert_ne!(acquired, renewed);
    }

    #[test]
    fn every_fence_generation_component_is_nonzero() {
        let generation =
            external_fence_generation(&token(1, 1), Uuid::now_v7()).expect("generation");
        assert!(generation.control_plane_incarnation > 0);
        assert!(generation.resource_epoch > 0);
        assert!(generation.fence_generation > 0);
    }

    #[test]
    fn a_later_coordination_attempt_mints_a_later_fence_generation() {
        let earlier = coordination_attempt_fence_generation(Uuid::now_v7()).expect("earlier");
        std::thread::sleep(std::time::Duration::from_millis(2));
        let later = coordination_attempt_fence_generation(Uuid::now_v7()).expect("later");
        assert!(later > earlier);
    }

    #[test]
    fn a_non_v7_coordination_attempt_cannot_mint_a_fence() {
        assert!(coordination_attempt_fence_generation(Uuid::nil()).is_err());
        assert!(coordination_attempt_fence_generation(Uuid::new_v4()).is_err());
    }

    #[test]
    fn a_proposal_requires_a_complete_generation_and_cluster() {
        let operation_id = DmlOperationId::new_v7();
        let attempt = Uuid::now_v7();
        let generation = DmlExternalFenceGeneration {
            control_plane_incarnation: 1,
            resource_epoch: 1,
            fence_generation: 1,
        };
        assert!(DmlExternalFenceProposal::testing(operation_id, "", attempt, generation).is_err());
        for zeroed in [
            DmlExternalFenceGeneration {
                control_plane_incarnation: 0,
                ..generation
            },
            DmlExternalFenceGeneration {
                resource_epoch: 0,
                ..generation
            },
            DmlExternalFenceGeneration {
                fence_generation: 0,
                ..generation
            },
        ] {
            assert!(
                DmlExternalFenceProposal::testing(operation_id, "nova", attempt, zeroed).is_err()
            );
        }
        DmlExternalFenceProposal::testing(operation_id, "nova", attempt, generation)
            .expect("a complete proposal");
    }
}
