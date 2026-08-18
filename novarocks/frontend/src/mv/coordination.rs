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

//! Cluster-wide MV refresh ownership.
//!
//! The process-local activity gate and the scheduler's running set decide
//! *fairness and capacity inside one frontend*. Neither can arbitrate between
//! two frontends sharing a StateStore: both can find the same target due and
//! both can start an attempt. This module supplies the missing arbiter — one
//! StateStore lease per MV target — and the fence validator that makes every
//! durable transition reject a superseded owner inside its own commit.
//!
//! Two properties are deliberate:
//!
//! * **One resource domain per target, whatever the entry point.** Manual SQL,
//!   the scheduler, and recovery all compete for the *same* lease. Splitting the
//!   domain by entry point, refresh policy, numeric `mv_id`, or frontend
//!   instance would let two of them run concurrently and call it correct.
//! * **The resource key is the stable target identity** frozen by the external
//!   publication fencing contract (provider ID + immutable target table UUID).
//!   A StateStore rebuild reassigns the numeric `mv_id`, so keying on it would
//!   silently split one target into two domains across a rebuild.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorMvPublicationTargetRequest, ConnectorMvRefreshResourceIdentity,
    ConnectorRequestContext, ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};
use novarocks_spi::state_store::StateStore;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, LeaseGuard, LeaseManager,
    ResourceKey, WriteAdmission,
};
use uuid::Uuid;

use crate::coordination::{CurrentLeaseFence, FenceValidator, FrontendCoordinationRuntime};
use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::mv::repository::MvRefreshFenceSource;

/// Prefix that scopes MV refresh leases away from every other coordinated
/// resource in the same StateStore.
const MV_REFRESH_RESOURCE_PREFIX: &[u8] = b"novarocks/mv/refresh/v1/";

/// How many definite acquire conflicts to absorb before giving up. Each one
/// proves the acquire did not happen, so retrying is safe; the bound keeps a
/// genuinely contended target from spinning.
const ACQUIRE_CONFLICT_RETRIES: u8 = 3;

/// Builds the per-target lease resource key from the stable target identity.
///
/// Only the provider ID and the immutable target table UUID contribute. A
/// display name, a numeric `mv_id`, or a catalog attachment lifecycle ID must
/// never appear here: the first two are reassigned by a rebuild and the third is
/// reused across DROP/recreate, so any of them would break the invariant that
/// one external table maps to exactly one refresh ownership domain.
pub(crate) fn mv_refresh_resource_key(
    resource: &ConnectorMvRefreshResourceIdentity,
) -> Result<ResourceKey, CoordinationError> {
    let canonical = resource.canonical_encoding();
    let mut bytes = Vec::with_capacity(MV_REFRESH_RESOURCE_PREFIX.len() + canonical.len());
    bytes.extend_from_slice(MV_REFRESH_RESOURCE_PREFIX);
    bytes.extend_from_slice(&canonical);
    ResourceKey::try_from(Bytes::from(bytes))
}

/// Per-target MV refresh ownership, shared by every refresh entry point.
#[derive(Clone)]
pub struct MvRefreshCoordination {
    frontend: FrontendCoordinationRuntime,
    manager: LeaseManager,
}

impl MvRefreshCoordination {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        let frontend = FrontendCoordinationRuntime::open(store).await?;
        Self::from_frontend(&frontend)
    }

    pub(crate) fn from_frontend(
        frontend: &FrontendCoordinationRuntime,
    ) -> Result<Self, CoordinationError> {
        Ok(Self {
            frontend: frontend.clone(),
            manager: frontend.lease_manager(),
        })
    }

    /// Competes for one target's refresh lease.
    ///
    /// A `CommitUncertain` acquire is recovered under the **same**
    /// `(resource, attempt, operation_id)` rather than retried with a fresh
    /// attempt. Minting a new attempt would let one logical acquisition appear
    /// twice in the lease record and defeat the fence it is supposed to
    /// establish.
    ///
    /// A definite conflict is the opposite case and takes the opposite action: it
    /// proves the acquire never landed, so there is nothing to recover and a
    /// fresh attempt is the correct move. The resource key is per target, so
    /// refreshes of one MV arriving back to back race their predecessor's
    /// release on the same record; without this the loser is reported as
    /// another frontend owning the target, which it does not.
    pub(crate) async fn acquire(
        &self,
        resource: &ConnectorMvRefreshResourceIdentity,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let key = mv_refresh_resource_key(resource)?;
        let mut remaining = ACQUIRE_CONFLICT_RETRIES;
        loop {
            let attempt = AttemptId::try_from(Uuid::now_v7())?;
            let operation_id = OperationId::new_v7();
            match self
                .manager
                .acquire(key.clone(), attempt, operation_id)
                .await
            {
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                    return self
                        .manager
                        .recover_acquire(key, attempt, operation_id)
                        .await;
                }
                Err(error)
                    if error.kind() == CoordinationErrorKind::OperationNotCommitted
                        && remaining > 0 =>
                {
                    remaining -= 1;
                }
                result => return result,
            }
        }
    }

    /// The global write-admission handle, composed into every fence validator so
    /// a refresh cannot write durable state while the control plane is still
    /// reconciling.
    pub(crate) async fn write_admission(&self) -> Result<WriteAdmission, CoordinationError> {
        self.frontend.admit_writes().await
    }

    /// Builds the validator a repository call must carry.
    ///
    /// Always composes admission with the lease fence: being inside an open
    /// write epoch and being the current owner are independent facts, and a
    /// refresh needs both to be true at commit time.
    pub(crate) async fn validator(
        &self,
        current: &CurrentLeaseFence,
    ) -> Result<FenceValidator, CoordinationError> {
        Ok(current.validator_with_admission(self.write_admission().await?))
    }
}

/// Resolves the stable refresh resource for a target through its provider.
///
/// This is the one place the frontend learns a target's immutable table UUID, and
/// it deliberately has no fallback. A provider that does not offer the fencing
/// capability cannot take part in fenced refresh at all, so the absence is an
/// error rather than a cue to derive an identity from a display name or a numeric
/// `mv_id` -- deriving one would silently reintroduce exactly the unstable key the
/// fence domain exists to avoid.
///
/// The observation is side-effect free, so it is safe to call before ownership has
/// been acquired. That ordering matters: the resource identity is what the lease
/// is keyed by, so it must be known first.
pub(crate) fn resolve_target_resource(
    binding: &ConnectorControlBinding,
    table: &ConnectorTableHandle,
    context: &ConnectorRequestContext,
) -> Result<ConnectorMvRefreshResourceIdentity, ConnectorError> {
    let fencing = binding.mv_publication_fencing().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector does not support MV publication fencing, so its targets cannot be \
             refreshed under cluster-wide ownership",
        )
    })?;
    let observation = fencing.observe_target(ConnectorMvPublicationTargetRequest {
        table: table.clone(),
        context: context.clone(),
    })?;
    let resource = observation.resource().clone();
    // The provider signs the identity; validating it here means a malformed
    // observation cannot become a lease key.
    resource.validate()?;
    if resource.provider_id() != &binding.descriptor().provider_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "MV target observation returned another provider's resource identity",
        ));
    }
    Ok(resource)
}

/// What a refresh is about to do that the outside world would notice.
///
/// Enumerated so losing ownership can be answered once, for every action, rather
/// than by remembering to check at each call site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshSideEffect {
    /// Create the staging branch a result will be written to.
    CreateStagingRef,
    /// Commit distributed write output.
    CommitDistributedWrite,
    /// Establish a new external publication fence.
    EstablishFence,
    /// Advance the target under a fence.
    Publish,
    /// Delete staging artifacts.
    Cleanup,
    /// Ask the provider what happened to an operation already issued.
    InspectIssuedOperation,
}

/// Whether this frontend still owns the refresh it is executing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshOwnershipState {
    Held,
    /// Renewal failed, the incarnation gate closed, or another frontend took over.
    Lost,
    /// This frontend is shutting down.
    ShuttingDown,
}

/// Decides whether a side effect may still be started.
///
/// Losing a lease stops *new* external side effects immediately. It deliberately
/// does not stop inspection: an operation already issued has an outcome in the
/// lake whether or not this frontend still owns the target, and reading it is how
/// the attempt gets classified rather than stranded. Inspection is also the one
/// action that cannot make things worse -- it writes nothing.
///
/// Nothing here is a retry decision. An operation whose outcome is unknown is
/// never re-issued by a frontend that lost ownership; the next owner resolves it
/// from lake evidence under a higher fence.
pub fn permits_side_effect(state: RefreshOwnershipState, effect: RefreshSideEffect) -> bool {
    match state {
        RefreshOwnershipState::Held => true,
        RefreshOwnershipState::Lost | RefreshOwnershipState::ShuttingDown => {
            matches!(effect, RefreshSideEffect::InspectIssuedOperation)
        }
    }
}

/// The coordination a frontend needs to own refreshes: the lease manager and the
/// registry the repository consults.
///
/// Bundled so composition passes one value and the refresh path cannot end up
/// holding a lease manager whose grants nobody records.
#[derive(Clone)]
pub struct MvRefreshOwnershipContext {
    pub coordination: MvRefreshCoordination,
    pub registry: Arc<MvRefreshOwnershipRegistry>,
    /// An explicit runtime handle, because the MV refresh worker is a plain OS
    /// thread with no ambient Tokio context. Relying on `Handle::current()` there
    /// panics and takes the worker down, which presents as refreshes silently
    /// never happening rather than as an error.
    runtime: tokio::runtime::Handle,
}

impl MvRefreshOwnershipContext {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        Ok(Self {
            coordination: MvRefreshCoordination::open(store).await?,
            registry: MvRefreshOwnershipRegistry::new(),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    /// Drives an acquisition to completion from either kind of caller.
    ///
    /// Manual refresh arrives on a Tokio worker, where blocking the thread
    /// directly would panic; the scheduler and recovery workers are plain OS
    /// threads with no runtime at all. Handling both is why the handle is carried
    /// rather than discovered.
    pub fn block_on_acquisition<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }

    /// Gives up every lease this frontend holds and stops their renewal.
    pub fn shutdown(&self) {
        self.registry.shutdown_in(&self.runtime);
    }

    pub fn registry(&self) -> Arc<dyn MvRefreshFenceSource> {
        Arc::clone(&self.registry) as Arc<dyn MvRefreshFenceSource>
    }
}

/// Resolves a target's stable refresh resource from its catalog coordinates.
///
/// Two provider calls, both side-effect free, both against the *same* control
/// binding: load the table to get its opaque handle, then observe the handle to
/// get the immutable UUID. Using one binding for both matters -- resolving the
/// table through one generation and observing it through another could pair a
/// handle with a UUID from a different provider incarnation.
///
/// `StrictBaseTable` resolution is deliberate: an MV target is a real table, and
/// a provider read alias must not be able to stand in for one when the answer
/// becomes a cluster-wide ownership key.
pub(crate) fn resolve_target_resource_for(
    binding: &ConnectorControlBinding,
    target: ConnectorTableIdentity,
    context: &ConnectorRequestContext,
) -> Result<ConnectorMvRefreshResourceIdentity, ConnectorError> {
    let metadata = binding.metadata().load_table(ConnectorTableRequest {
        table: target,
        resolution: ConnectorTableResolution::StrictBaseTable,
        context: context.clone(),
    })?;
    resolve_target_resource(binding, &metadata.table, context)
}

/// The refresh leases this process currently holds, keyed by MV.
///
/// This is the object that turns the repository's fence requirement into a real
/// one: it is installed as the repository's [`MvRefreshFenceSource`], so a
/// transition can only commit for a target whose lease is registered here.
///
/// Registration is keyed by `mv_id` because that is what a repository call
/// carries, while the *lease* is keyed by the stable target identity. Both are
/// recorded together so a takeover cannot leave an `mv_id` pointing at a fence
/// from a previous target incarnation.
#[derive(Default)]
pub struct MvRefreshOwnershipRegistry {
    held: RwLock<HashMap<i64, HeldRefreshLease>>,
    /// Signals every renewal loop to stop.
    ///
    /// Without it, a renewal loop sleeping on its interval keeps issuing
    /// StateStore writes while the process is tearing down, and the state store
    /// provider misses its shutdown deadline waiting to drain.
    shutdown: tokio::sync::Notify,
    shutting_down: std::sync::atomic::AtomicBool,
}

struct HeldRefreshLease {
    resource: ConnectorMvRefreshResourceIdentity,
    fence: Arc<CurrentLeaseFence>,
    admission: WriteAdmission,
    /// The lease itself, kept alive for as long as this frontend owns the target.
    ///
    /// Ownership is sticky per target rather than per refresh. Acquiring and
    /// releasing around each refresh cannot work: `LeaseGuard`'s release is
    /// spawned, so it completes asynchronously while acquisition is immediate, and
    /// a refresh issued right after recovery races a lease still on its way out.
    /// Holding it removes the churn entirely.
    ///
    /// Behind an async mutex because renewal needs `&mut` across an `.await`.
    guard: Arc<tokio::sync::Mutex<LeaseGuard>>,
}

impl MvRefreshOwnershipRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records that this process owns `mv_id`'s refresh resource.
    ///
    /// Re-registering the same MV replaces the entry: a takeover-then-reacquire
    /// must not leave the previous generation's fence reachable.
    pub fn register(
        &self,
        mv_id: i64,
        resource: ConnectorMvRefreshResourceIdentity,
        fence: Arc<CurrentLeaseFence>,
        admission: WriteAdmission,
        guard: Arc<tokio::sync::Mutex<LeaseGuard>>,
    ) -> Result<(), MvRepositoryError> {
        let mut held = self.held.write().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "MV refresh ownership registry lock poisoned",
            )
        })?;
        held.insert(
            mv_id,
            HeldRefreshLease {
                resource,
                fence,
                admission,
                guard,
            },
        );
        Ok(())
    }

    /// Drops ownership so later transitions fail closed.
    ///
    /// Called when a lease is released, lost, or the worker shuts down. After
    /// this, the repository rejects durable transitions for `mv_id` rather than
    /// letting them through unfenced.
    /// Gives up ownership of one target.
    ///
    /// The removed lease is dropped inside `runtime` rather than on the caller's
    /// thread. `LeaseGuard::drop` releases by spawning and silently skips the
    /// release when no runtime is current, and every caller here is either the MV
    /// refresh worker -- a plain OS thread -- or a renewal task tearing itself
    /// down. Dropping it on the worker thread would leave the lease held in the
    /// StateStore until its TTL expired.
    pub fn release_in(&self, mv_id: i64, runtime: &tokio::runtime::Handle) {
        let Ok(mut held) = self.held.write() else {
            return;
        };
        let Some(lease) = held.remove(&mv_id) else {
            return;
        };
        drop(held);
        let guard = lease.guard;
        runtime.spawn(async move {
            drop(guard);
        });
    }

    /// Stops every renewal loop and gives up every held lease.
    ///
    /// Called on frontend teardown. Renewal loops are woken rather than left to
    /// finish their sleep: the state store shuts down on a deadline, and a loop
    /// that wakes to renew during teardown keeps the provider from draining.
    pub fn shutdown_in(&self, runtime: &tokio::runtime::Handle) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.shutdown.notify_waiters();
        let held: Vec<i64> = self
            .held
            .read()
            .map(|held| held.keys().copied().collect())
            .unwrap_or_default();
        for mv_id in held {
            self.release_in(mv_id, runtime);
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Removes the entry without releasing the lease.
    ///
    /// Only correct when the lease is already gone -- a failed renewal means this
    /// frontend no longer holds it, and there is nothing to hand back.
    pub fn forget(&self, mv_id: i64) {
        if let Ok(mut held) = self.held.write() {
            held.remove(&mv_id);
        }
    }

    /// The stable resource this process holds for `mv_id`, if any.
    pub(crate) fn resource_for(&self, mv_id: i64) -> Option<ConnectorMvRefreshResourceIdentity> {
        self.held
            .read()
            .ok()?
            .get(&mv_id)
            .map(|held| held.resource.clone())
    }

    pub fn holds(&self, mv_id: i64) -> bool {
        self.held.read().is_ok_and(|held| held.contains_key(&mv_id))
    }

    /// The leases this frontend currently holds, for the renewal loop.
    fn renewable(&self) -> Vec<(i64, Arc<tokio::sync::Mutex<LeaseGuard>>)> {
        self.held
            .read()
            .map(|held| {
                held.iter()
                    .map(|(mv_id, lease)| (*mv_id, Arc::clone(&lease.guard)))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl MvRefreshFenceSource for MvRefreshOwnershipRegistry {
    fn validator_for(&self, mv_id: i64) -> Result<FenceValidator, MvRepositoryError> {
        let held = self.held.read().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "MV refresh ownership registry lock poisoned",
            )
        })?;
        let held = held.get(&mv_id).ok_or_else(|| {
            // Fail closed. This frontend is not the owner, so it must not write
            // durable refresh state for this target at all — not even the same
            // value it would have written while it was the owner.
            MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!("this frontend does not hold the refresh lease for mv {mv_id}"),
            )
        })?;
        Ok(held.fence.validator_with_admission(held.admission.clone()))
    }
}

/// Ownership of one target's refresh, held for as long as this value lives.
///
/// `Debug` prints only the target it owns: the lease guard behind it carries
/// coordination internals that have no business in a log line.
///
/// Dropping it releases registry ownership, so a durable transition attempted
/// after the handle is gone fails closed. Tying release to the handle's lifetime
/// rather than to an explicit call is deliberate: an early return or a panic on
/// the refresh path must not leave this frontend appearing to own a target it has
/// stopped working on.
pub struct OwnedRefresh {
    mv_id: i64,
}

impl OwnedRefresh {
    pub const fn mv_id(&self) -> i64 {
        self.mv_id
    }
}

impl std::fmt::Debug for OwnedRefresh {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedRefresh")
            .field("mv_id", &self.mv_id)
            .finish_non_exhaustive()
    }
}

// `OwnedRefresh` deliberately has no `Drop`. Ownership is sticky per target: it
// outlives the refresh that first took it, and is released when the owning
// worker stops or when renewal proves the lease was lost. Releasing here is what
// the per-refresh model did, and it could not be made correct -- `LeaseGuard`
// releases by spawning, so the release lands asynchronously while the next
// acquisition is immediate, and a refresh issued right after recovery loses a
// race against its own predecessor's release.

/// Why a refresh could not take ownership of its target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipRefusal {
    /// Another frontend currently holds the lease.
    Contended,
    /// The previous holder's lease has not yet aged out.
    AwaitingTakeover,
    /// Coordination itself was unavailable.
    Unavailable,
}

/// Acquires and registers ownership of one target's refresh.
///
/// The ordering is fixed and is the whole point: resolve the stable identity,
/// win the lease, then register so the repository can enforce it. Registering
/// before winning would make the repository accept writes this frontend has no
/// right to make, and writing before registering would leave the repository
/// unable to reject them.
///
/// Contention is not an error to surface to a user as a failure: manual refresh
/// maps it to a retryable conflict, and the workers back off. Only genuine
/// coordination unavailability is exceptional.
pub async fn acquire_refresh_ownership(
    context: &MvRefreshOwnershipContext,
    mv_id: i64,
    resource: ConnectorMvRefreshResourceIdentity,
) -> Result<OwnedRefresh, OwnershipRefusal> {
    let coordination = &context.coordination;
    let registry = &context.registry;
    // Already ours: reuse it. Re-acquiring per refresh is what made ownership
    // race itself, and the lease we hold is the same lease we would win.
    if registry.holds(mv_id) {
        return Ok(OwnedRefresh { mv_id });
    }
    let admission = coordination
        .write_admission()
        .await
        .map_err(|_| OwnershipRefusal::Unavailable)?;
    let guard = match coordination.acquire(&resource).await {
        Ok(AcquireOutcome::Acquired(guard)) => guard,
        Ok(AcquireOutcome::Contended(_)) => return Err(OwnershipRefusal::Contended),
        Ok(AcquireOutcome::AwaitingTakeover(_)) => {
            return Err(OwnershipRefusal::AwaitingTakeover);
        }
        Err(_) => return Err(OwnershipRefusal::Unavailable),
    };
    let fence = Arc::new(CurrentLeaseFence::new(guard.fence()));
    let renew_after = guard.renew_after();
    let guard = Arc::new(tokio::sync::Mutex::new(guard));
    registry
        .register(
            mv_id,
            resource,
            Arc::clone(&fence),
            admission,
            Arc::clone(&guard),
        )
        .map_err(|_| OwnershipRefusal::Unavailable)?;
    spawn_renewal(context, mv_id, guard, fence, renew_after);
    Ok(OwnedRefresh { mv_id })
}

/// Keeps a held lease alive for as long as this frontend claims the target.
///
/// Sticky ownership without renewal is strictly worse than no ownership: the
/// lease ages out, another frontend legitimately takes the target, and this one
/// keeps refreshing in the belief that it still owns it. That is a silent
/// split-brain, whereas an unfenced refresh is at least visibly unfenced.
///
/// So a failed renewal deregisters the target. The registry is the repository's
/// fence source and is fail-closed for unregistered targets, which turns a lost
/// lease into refused writes rather than unfenced ones.
/// How many definite renewal conflicts to absorb before giving up. Each one
/// proves the renewal did not land, so the lease still holds its existing
/// deadline and a fresh attempt is safe; the bound stops a lease that genuinely
/// cannot settle from being renewed past its own expiry.
const RENEW_CONFLICT_RETRIES: u8 = 3;

/// One renewal of a held lease, resolving the two outcomes that do not prove
/// the lease was lost.
///
/// Ownership here is sticky: the lease outlives the refresh that took it, so the
/// renewal task runs for as long as this frontend owns the target and a single
/// mishandled renewal forgets it. Whatever refresh is in flight at that moment
/// then fails its next durable write with "this frontend does not hold the
/// refresh lease", naming a lease that was never actually lost.
///
/// `CommitUncertain` is ambiguous and is recovered under the **same**
/// operation ID, exactly as the DML, statistics and table-maintenance renewal
/// loops already do -- a fresh ID would let one logical renewal appear twice.
/// A definite conflict is the opposite: it proves nothing landed, so there is
/// nothing to recover and a fresh ID may simply try again.
async fn renew_once(
    guard: &tokio::sync::Mutex<LeaseGuard>,
) -> Result<novarocks_state_store::coordination::LeaseFence, CoordinationError> {
    let mut remaining = RENEW_CONFLICT_RETRIES;
    loop {
        let operation_id = OperationId::new_v7();
        // The lock is held across the renewal await, which is why it is an
        // async mutex. Nothing else takes it except the release path.
        let mut guard = guard.lock().await;
        let result = match guard.renew(operation_id).await {
            Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                guard.recover_renew(operation_id).await
            }
            result => result,
        };
        match result {
            Ok(()) => return Ok(guard.fence()),
            Err(error)
                if error.kind() == CoordinationErrorKind::OperationNotCommitted
                    && remaining > 0 =>
            {
                remaining -= 1;
            }
            Err(error) => return Err(error),
        }
    }
}

fn spawn_renewal(
    context: &MvRefreshOwnershipContext,
    mv_id: i64,
    guard: Arc<tokio::sync::Mutex<LeaseGuard>>,
    fence: Arc<CurrentLeaseFence>,
    renew_after: Duration,
) {
    let registry = Arc::clone(&context.registry);
    context.runtime.spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(renew_after) => {}
                () = registry.shutdown.notified() => return,
            }
            // Stop as soon as the target is no longer held: the worker released
            // it, a previous iteration deregistered it, or teardown began between
            // the wake-up and here.
            if registry.is_shutting_down() || !registry.holds(mv_id) {
                return;
            }
            // Renewal advances the fence, so the registered one must advance with
            // it. Leaving it behind makes every durable transition validate
            // against a superseded fence and fail as a conflict -- this frontend
            // rejecting its own writes, moments after renewing the very lease that
            // authorises them.
            let advanced = match renew_once(&guard).await {
                Ok(current) => fence.replace(current).is_ok(),
                Err(_) => false,
            };
            if !advanced {
                registry.forget(mv_id);
                return;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorProviderId;

    fn resource(uuid: u128) -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(uuid),
        )
        .unwrap()
    }

    /// Wraps a store and refuses the next `armed` commits with a definite
    /// conflict, dropping their writes. That is what losing a version race looks
    /// like from above: nothing landed, so the loser may simply try again.
    struct ConflictingStore {
        inner: Arc<dyn StateStore>,
        armed: Arc<std::sync::atomic::AtomicUsize>,
        armed_uncertain: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct ConflictingTransaction {
        inner: Option<Box<dyn novarocks_spi::state_store::WriteTransaction>>,
        armed: Arc<std::sync::atomic::AtomicUsize>,
        armed_uncertain: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ConflictingTransaction {
        fn inner(&mut self) -> &mut dyn novarocks_spi::state_store::WriteTransaction {
            self.inner.as_mut().expect("transaction is active").as_mut()
        }
    }

    #[async_trait::async_trait]
    impl novarocks_spi::state_store::ReadTransaction for ConflictingTransaction {
        async fn get(
            &mut self,
            key: &novarocks_spi::state_store::Key,
        ) -> Result<
            Option<novarocks_spi::state_store::StateRecord>,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner().get(key).await
        }

        async fn range(
            &mut self,
            request: &novarocks_spi::state_store::RangeRequest,
        ) -> Result<
            novarocks_spi::state_store::RangePage,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner().range(request).await
        }

        async fn abort(
            mut self: Box<Self>,
        ) -> Result<(), novarocks_spi::state_store::StateStoreError> {
            self.inner
                .take()
                .expect("transaction is active")
                .abort()
                .await
        }
    }

    #[async_trait::async_trait]
    impl novarocks_spi::state_store::WriteTransaction for ConflictingTransaction {
        fn transaction_id(&self) -> &novarocks_spi::state_store::TransactionId {
            self.inner
                .as_ref()
                .expect("transaction is active")
                .transaction_id()
        }

        async fn put(
            &mut self,
            key: novarocks_spi::state_store::Key,
            value: novarocks_spi::state_store::Value,
            precondition: novarocks_spi::state_store::Precondition,
        ) -> Result<(), novarocks_spi::state_store::StateStoreError> {
            self.inner().put(key, value, precondition).await
        }

        async fn delete(
            &mut self,
            key: novarocks_spi::state_store::Key,
            precondition: novarocks_spi::state_store::Precondition,
        ) -> Result<(), novarocks_spi::state_store::StateStoreError> {
            self.inner().delete(key, precondition).await
        }

        async fn commit(mut self: Box<Self>) -> novarocks_spi::state_store::CommitOutcome {
            use std::sync::atomic::Ordering;
            // An ambiguous commit: the write may or may not have landed, so it
            // is applied and then reported as unknown.
            if self
                .armed_uncertain
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |armed| {
                    armed.checked_sub(1)
                })
                .is_ok()
            {
                let outcome = self
                    .inner
                    .take()
                    .expect("transaction is active")
                    .commit()
                    .await;
                if !matches!(
                    outcome,
                    novarocks_spi::state_store::CommitOutcome::Committed(_)
                ) {
                    return outcome;
                }
                return novarocks_spi::state_store::CommitOutcome::CommitUnknown(
                    novarocks_spi::state_store::StateStoreError::new(
                        novarocks_spi::state_store::StateStoreErrorKind::Internal,
                        "injected uncertain commit",
                    ),
                );
            }
            if self
                .armed
                // `checked_sub` is the whole point: `then_some(armed - 1)`
                // evaluates its argument eagerly and underflows at zero.
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |armed| {
                    armed.checked_sub(1)
                })
                .is_ok()
            {
                return novarocks_spi::state_store::CommitOutcome::Conflict(
                    novarocks_spi::state_store::StateStoreError::new(
                        novarocks_spi::state_store::StateStoreErrorKind::Conflict,
                        "injected definite conflict",
                    ),
                );
            }
            self.inner
                .take()
                .expect("transaction is active")
                .commit()
                .await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for ConflictingStore {
        fn limits(&self) -> &novarocks_spi::state_store::StateStoreLimits {
            self.inner.limits()
        }

        fn metrics_snapshot(&self) -> novarocks_spi::state_store::StateStoreMetricsSnapshot {
            self.inner.metrics_snapshot()
        }

        async fn begin_read(
            &self,
        ) -> Result<
            Box<dyn novarocks_spi::state_store::ReadTransaction>,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner.begin_read().await
        }

        async fn begin_write(
            &self,
            transaction_id: novarocks_spi::state_store::TransactionId,
            purpose: &str,
        ) -> Result<
            Box<dyn novarocks_spi::state_store::WriteTransaction>,
            novarocks_spi::state_store::StateStoreError,
        > {
            Ok(Box::new(ConflictingTransaction {
                inner: Some(self.inner.begin_write(transaction_id, purpose).await?),
                armed: Arc::clone(&self.armed),
                armed_uncertain: Arc::clone(&self.armed_uncertain),
            }))
        }

        async fn poll_changes(
            &self,
            request: &novarocks_spi::state_store::ChangePollRequest,
        ) -> Result<
            novarocks_spi::state_store::ChangePage,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner.poll_changes(request).await
        }

        async fn identity(
            &self,
        ) -> Result<
            novarocks_spi::state_store::StoreIdentity,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner.identity().await
        }

        async fn resolve_commit(
            &self,
            transaction_id: &novarocks_spi::state_store::TransactionId,
        ) -> Result<
            novarocks_spi::state_store::CommitResolution,
            novarocks_spi::state_store::StateStoreError,
        > {
            self.inner.resolve_commit(transaction_id).await
        }
    }

    async fn conflicting_coordination(
        path: &std::path::Path,
        armed: &Arc<std::sync::atomic::AtomicUsize>,
        uncertain: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> MvRefreshCoordination {
        let registry = novarocks_state_store::builtin_state_store_provider_registry()
            .expect("built-in provider registry");
        let inner = novarocks_state_store::StateStoreHost::open(
            &registry,
            novarocks_state_store::StateStoreHostConfig {
                state_store: novarocks_state_store::StateStoreAppConfig {
                    store: novarocks_state_store::StateStoreConfig {
                        cluster_id: "mv-refresh-lease-conflict".to_string(),
                        limits: novarocks_state_store::StateStoreLimitOverrides::default(),
                        provider: novarocks_state_store::StateStoreProviderConfig::Sqlite {
                            path: path.to_path_buf(),
                            deployment_owner: "mv-refresh-lease-conflict-fe".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            novarocks_spi::state_store::FeDeploymentView {
                active_fe_count: std::num::NonZeroUsize::new(1).unwrap(),
                topology_revision: Bytes::from_static(b"mv-refresh-lease-conflict"),
            },
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite state store")
        .state_store()
        .expect("SQLite state store exposure");
        let store = Arc::new(ConflictingStore {
            inner,
            armed: Arc::clone(armed),
            armed_uncertain: Arc::clone(uncertain),
        }) as Arc<dyn StateStore>;
        // Arm only after open, so the bootstrap writes are not the refused ones.
        MvRefreshCoordination::open(store)
            .await
            .expect("open MV refresh coordination")
    }

    /// Ownership is sticky, so the renewal task runs for as long as this
    /// frontend owns the target. A renewal that does not prove the lease was
    /// lost must not forget it: whatever refresh is in flight at that moment
    /// would fail its next durable write with "this frontend does not hold the
    /// refresh lease", naming a lease nobody took away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_renewal_that_did_not_lose_the_lease_keeps_ownership() {
        use std::sync::atomic::Ordering;
        let temp = tempfile::TempDir::new().expect("temporary directory");
        let armed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let uncertain = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let coordination =
            conflicting_coordination(&temp.path().join("state.sqlite"), &armed, &uncertain).await;
        let AcquireOutcome::Acquired(guard) = coordination
            .acquire(&resource(0xbeef))
            .await
            .expect("acquire")
        else {
            panic!("an uncontended target must be acquired");
        };
        let guard = tokio::sync::Mutex::new(guard);

        // An ambiguous renewal is resolved rather than treated as a loss.
        uncertain.store(1, Ordering::Release);
        renew_once(&guard)
            .await
            .expect("an uncertain renewal is recovered, not counted as a lost lease");
        assert_eq!(uncertain.load(Ordering::Acquire), 0);

        // A definite conflict proves the renewal did not land, and the lease
        // still holds its existing deadline, so a fresh attempt is correct.
        armed.store(2, Ordering::Release);
        renew_once(&guard)
            .await
            .expect("a definite renewal conflict is absorbed");
        assert_eq!(armed.load(Ordering::Acquire), 0);

        // A lease that genuinely cannot settle still gives up, so it cannot be
        // renewed past its own expiry.
        armed.store(64, Ordering::Release);
        let error = renew_once(&guard)
            .await
            .expect_err("the retry bound has to hold");
        assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_definite_acquire_conflict_is_absorbed_rather_than_refusing_the_caller() {
        use std::sync::atomic::Ordering;
        let temp = tempfile::TempDir::new().expect("temporary directory");
        let armed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let uncertain = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let coordination =
            conflicting_coordination(&temp.path().join("state.sqlite"), &armed, &uncertain).await;
        armed.store(2, Ordering::Release);

        let outcome = coordination
            .acquire(&resource(0x5eed))
            .await
            .expect("a definite conflict proves nothing landed, so the acquire retries");

        assert!(
            matches!(outcome, AcquireOutcome::Acquired(_)),
            "the retry has to end in a real lease: the caller turns anything else \
             into another frontend owning the target, which it does not"
        );
        assert_eq!(
            armed.load(Ordering::Acquire),
            0,
            "both injected conflicts should have been consumed by retries"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conflicts_past_the_retry_bound_still_surface() {
        use std::sync::atomic::Ordering;
        let temp = tempfile::TempDir::new().expect("temporary directory");
        let armed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let uncertain = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let coordination =
            conflicting_coordination(&temp.path().join("state.sqlite"), &armed, &uncertain).await;
        // A record that never settles must stop retrying rather than spin.
        armed.store(64, Ordering::Release);

        let error = coordination
            .acquire(&resource(0x5eed))
            .await
            .expect_err("the retry bound has to hold");
        assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
    }

    #[test]
    fn losing_ownership_stops_every_new_side_effect() {
        use RefreshSideEffect::*;

        // While owned, nothing is withheld.
        for effect in [
            CreateStagingRef,
            CommitDistributedWrite,
            EstablishFence,
            Publish,
            Cleanup,
            InspectIssuedOperation,
        ] {
            assert!(
                permits_side_effect(RefreshOwnershipState::Held, effect),
                "{effect:?} must be allowed while ownership is held"
            );
        }

        // Losing the lease and shutting down are the same answer: no new
        // external effect. Publishing or cleaning up here is what would let a
        // superseded owner corrupt a target another frontend now owns.
        for state in [
            RefreshOwnershipState::Lost,
            RefreshOwnershipState::ShuttingDown,
        ] {
            for effect in [
                CreateStagingRef,
                CommitDistributedWrite,
                EstablishFence,
                Publish,
                Cleanup,
            ] {
                assert!(
                    !permits_side_effect(state, effect),
                    "{effect:?} must be refused once ownership is {state:?}"
                );
            }
        }
    }

    #[test]
    fn inspection_survives_losing_ownership() {
        // An operation already issued has an outcome in the lake whether or not
        // this frontend still owns the target. Reading it writes nothing and is
        // how the attempt gets classified instead of stranded, so it is the one
        // action that must not be withheld.
        for state in [
            RefreshOwnershipState::Lost,
            RefreshOwnershipState::ShuttingDown,
        ] {
            assert!(permits_side_effect(
                state,
                RefreshSideEffect::InspectIssuedOperation
            ));
        }
    }

    #[test]
    fn registry_fails_closed_for_targets_this_frontend_does_not_own() {
        let registry = MvRefreshOwnershipRegistry::new();

        // Nothing registered: every target is unowned, so every durable
        // transition must be refused rather than run unfenced.
        // `FenceValidator` is a closure and has no `Debug`, so match rather than
        // `expect_err`.
        let error = match registry.validator_for(7) {
            Ok(_) => panic!("an unowned target must not yield a validator"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict, "{error}");
        assert!(!registry.holds(7));
        assert!(registry.resource_for(7).is_none());
    }

    /// A lost lease must revoke the ability to write, not merely stop new work
    /// from being scheduled.
    ///
    /// This is the failure mode sticky ownership introduces and must answer for.
    /// Ownership now outlives a single refresh, so a lease that ages out while
    /// this frontend still believes it owns the target would be a silent
    /// split-brain: two frontends both refreshing, both convinced they are the
    /// owner. The renewal loop's contract is that a failed renewal deregisters
    /// the target, and the registry is fail-closed for unregistered targets, so a
    /// lost lease turns into refused writes rather than unfenced ones.
    #[test]
    fn a_lost_lease_revokes_the_ability_to_write() {
        let registry = MvRefreshOwnershipRegistry::new();

        // `forget` is what the renewal loop calls when renewal fails. It must
        // leave the target in the same fail-closed state as one never owned.
        registry.forget(7);
        assert!(!registry.holds(7));
        assert!(
            registry.validator_for(7).is_err(),
            "a target whose lease was lost must fail closed"
        );
        assert!(
            registry.resource_for(7).is_none(),
            "a lost lease must not leave a resource behind"
        );
    }

    #[test]
    fn ownership_refusals_distinguish_contention_from_unavailability() {
        // Contention and awaiting-takeover are routine: manual refresh maps them
        // to a retryable conflict and the workers back off. Collapsing them into
        // "unavailable" would turn normal multi-frontend operation into errors.
        assert_ne!(OwnershipRefusal::Contended, OwnershipRefusal::Unavailable);
        assert_ne!(
            OwnershipRefusal::AwaitingTakeover,
            OwnershipRefusal::Unavailable
        );
        assert_ne!(
            OwnershipRefusal::Contended,
            OwnershipRefusal::AwaitingTakeover
        );
    }

    #[test]
    fn registry_tracks_the_stable_resource_alongside_the_numeric_mv_id() {
        // The registry is keyed by mv_id because that is what a repository call
        // carries, but it records the stable resource so a rebuild that reassigns
        // mv_id cannot leave an entry pointing at the wrong target.
        let registry = MvRefreshOwnershipRegistry::new();
        assert!(registry.resource_for(7).is_none());
        assert_ne!(
            mv_refresh_resource_key(&resource(0x1234)).unwrap(),
            mv_refresh_resource_key(&resource(0x9999)).unwrap(),
            "two targets must never share one ownership domain"
        );
    }

    #[test]
    fn resource_key_is_stable_and_target_scoped() {
        let first = mv_refresh_resource_key(&resource(0x1234)).unwrap();
        let again = mv_refresh_resource_key(&resource(0x1234)).unwrap();
        assert_eq!(
            first, again,
            "the same target must always map to the same lease resource"
        );

        let other = mv_refresh_resource_key(&resource(0x9999)).unwrap();
        assert_ne!(
            first, other,
            "distinct targets must hold distinct leases so they refresh in parallel"
        );
    }

    /// The key the implementation must produce: namespace prefix followed by
    /// exactly the stable identity's canonical encoding, and nothing else. This
    /// pins the invariant that no numeric `mv_id`, display name, or attachment
    /// id can leak into the ownership domain — anything extra would change this
    /// byte sequence.
    fn expected_key(identity: &ConnectorMvRefreshResourceIdentity) -> ResourceKey {
        let canonical = identity.canonical_encoding();
        let mut bytes = Vec::with_capacity(MV_REFRESH_RESOURCE_PREFIX.len() + canonical.len());
        bytes.extend_from_slice(MV_REFRESH_RESOURCE_PREFIX);
        bytes.extend_from_slice(&canonical);
        ResourceKey::try_from(Bytes::from(bytes)).unwrap()
    }

    #[test]
    fn resource_key_is_exactly_namespace_plus_stable_identity() {
        let identity = resource(0x1234);

        assert_eq!(
            mv_refresh_resource_key(&identity).unwrap(),
            expected_key(&identity),
            "the lease key must be the namespaced stable identity and carry nothing else"
        );

        // A bare canonical encoding without the namespace is a different key, so
        // MV refresh leases cannot collide with another coordinated domain that
        // happens to key on the same identity.
        let unnamespaced =
            ResourceKey::try_from(Bytes::from(identity.canonical_encoding())).unwrap();
        assert_ne!(mv_refresh_resource_key(&identity).unwrap(), unnamespaced);
    }
}
