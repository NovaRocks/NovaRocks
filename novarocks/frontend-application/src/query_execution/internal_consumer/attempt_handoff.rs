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

//! A finite, activation-exact handoff between the fixed FA consumer and Native owner.

use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::{SafeDetail, TaskIdentity, failure::FailureProjection};
use novarocks_query_application::api::{ComputationReadySource, PlanSeal, QueryExecutionError};
use novarocks_query_application::coordination::NativeAttemptDrive;
use novarocks_query_application::execution_error::{
    ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
};
use novarocks_types::QueryExecutionId;
use tokio::sync::Notify;

use crate::query_execution::lifecycle_plan::AttemptCredentialStorage;
use crate::task_execution::profile_diagnostics::ProfileDiagnosticObserver;
use crate::task_execution::protocol_facts::AttemptProtocolFactsReceiver;

/// These are the original attempt's concrete receiving capabilities. This
/// container neither grants Ready nor authorizes publication by itself.
pub(crate) struct AttemptHandoffResources {
    protocol: AttemptProtocolFactsReceiver,
    credentials: Option<Arc<AttemptCredentialStorage>>,
    profile: Option<ProfileDiagnosticObserver>,
}

impl AttemptHandoffResources {
    pub(crate) fn new(
        protocol: AttemptProtocolFactsReceiver,
        credentials: Option<Arc<AttemptCredentialStorage>>,
        profile: Option<ProfileDiagnosticObserver>,
    ) -> Self {
        Self {
            protocol,
            credentials,
            profile,
        }
    }
}

pub(crate) struct AttemptConsumerHandoff {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
    resources: AttemptHandoffResources,
}

impl AttemptConsumerHandoff {
    pub(crate) const fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    pub(crate) const fn root(&self) -> TaskIdentity {
        self.root
    }
    pub(crate) const fn plan(&self) -> PlanSeal {
        self.plan
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        AttemptProtocolFactsReceiver,
        Option<Arc<AttemptCredentialStorage>>,
        Option<ProfileDiagnosticObserver>,
    ) {
        (
            self.resources.protocol,
            self.resources.credentials,
            self.resources.profile,
        )
    }
}

struct Frozen {
    root: TaskIdentity,
    plan: PlanSeal,
    resources: AttemptHandoffResources,
}

#[derive(Clone, Copy)]
struct ClaimKey {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
}

impl ClaimKey {
    fn from_source(source: &ComputationReadySource) -> Self {
        Self {
            activation: source.activation(),
            root: source.root(),
            plan: source.plan(),
        }
    }
    fn verify_frozen(self, frozen: &Frozen) -> Result<(), QueryExecutionError> {
        if self.root != frozen.root || self.plan != frozen.plan {
            return Err(handoff_error(
                "internal handoff has a foreign frozen root or plan",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Closed {
    ProducerDropped,
    OwnerConverging,
    ReplacedBeforeClaim,
}

impl Closed {
    fn error(self) -> QueryExecutionError {
        handoff_error(match self {
            Self::ProducerDropped => "the original Native handoff producer exited before transfer",
            Self::OwnerConverging => "the original Native owner converged before consumer transfer",
            Self::ReplacedBeforeClaim => "the original unclaimed Native handoff was superseded",
        })
    }
}

enum State {
    Reserved,
    Staged(Frozen),
    Bound {
        activation: AttemptActivationIdentity,
        frozen: Frozen,
    },
    Claimed(ClaimKey),
    Closed(Closed),
}

struct Cell {
    execution: QueryExecutionId,
    state: Mutex<State>,
    changed: Notify,
}

impl Cell {
    /// Only an unclaimed offer may be retired. A claimed consumer retains its
    /// independent old cell and resources through its real future's exit.
    fn close(&self, reason: Closed) {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &*state {
                State::Claimed(_) | State::Closed(_) => None,
                _ => Some(std::mem::replace(&mut *state, State::Closed(reason))),
            }
        };
        self.changed.notify_waiters();
        // An Arc's final destructor may revoke an existing credential route;
        // no resource destructor runs while a handoff lock is held.
        drop(retired);
    }
}

#[derive(Default)]
struct Mailbox {
    // This is the sole unclaimed offer, never a map or a latest-facts cache.
    offer: Mutex<Option<Arc<Cell>>>,
}

/// Strong custody of the original directory, held by the prepared computation.
pub(crate) struct AttemptHandoffOfferPort {
    mailbox: Arc<Mailbox>,
}
/// Only the fixed consumer receives this endpoint. It cannot reserve, stage,
/// bind or redirect a Native attempt.
pub(crate) struct AttemptHandoffClaimPort {
    mailbox: Arc<Mailbox>,
}

impl std::fmt::Debug for AttemptHandoffClaimPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptHandoffClaimPort")
            .finish_non_exhaustive()
    }
}

pub(crate) fn attempt_handoff_mailbox() -> (AttemptHandoffOfferPort, AttemptHandoffClaimPort) {
    let mailbox = Arc::new(Mailbox::default());
    (
        AttemptHandoffOfferPort {
            mailbox: Arc::clone(&mailbox),
        },
        AttemptHandoffClaimPort { mailbox },
    )
}

impl AttemptHandoffOfferPort {
    /// A replayable factory can reserve through the original directory without
    /// becoming an owner of its staged attempt resources.
    pub(crate) fn reserve_view(&self) -> AttemptHandoffReserveView {
        AttemptHandoffReserveView {
            mailbox: Arc::downgrade(&self.mailbox),
        }
    }

    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        execution: QueryExecutionId,
    ) -> Result<AttemptHandoffProducer, QueryExecutionError> {
        self.mailbox.reserve(execution)
    }
}

#[derive(Clone)]
pub(crate) struct AttemptHandoffReserveView {
    mailbox: Weak<Mailbox>,
}
impl AttemptHandoffReserveView {
    pub(crate) fn reserve_in(
        &self,
        execution: QueryExecutionId,
        target: &mut Option<AttemptHandoffProducer>,
    ) -> Result<(), QueryExecutionError> {
        self.mailbox
            .upgrade()
            .ok_or_else(|| handoff_error("the original handoff directory has exited"))?
            .reserve_in(execution, target)
    }
}

impl Mailbox {
    /// Called once by the exact request's dormant factory, before projection makes any
    /// attempt resource live. A later admitted attempt can replace only the
    /// unclaimed directory offer, not a consumer's captured independent cell.
    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        execution: QueryExecutionId,
    ) -> Result<AttemptHandoffProducer, QueryExecutionError> {
        let mut target = None;
        self.reserve_in(execution, &mut target)?;
        Ok(target.unwrap())
    }
    pub(crate) fn reserve_in(
        &self,
        execution: QueryExecutionId,
        target: &mut Option<AttemptHandoffProducer>,
    ) -> Result<(), QueryExecutionError> {
        if target.is_some() {
            return Err(handoff_error("original handoff receiver is occupied"));
        }
        let cell = Arc::new(Cell {
            execution,
            state: Mutex::new(State::Reserved),
            changed: Notify::new(),
        });
        let previous = {
            let mut offer = self
                .offer
                .lock()
                .map_err(|_| handoff_error("internal handoff offer lock was poisoned"))?;
            if offer
                .as_ref()
                .is_some_and(|held| held.execution == execution)
            {
                return Err(handoff_error(
                    "internal handoff repeats an unclaimed Native attempt",
                ));
            }
            offer.replace(Arc::clone(&cell))
        };
        // The real new producer is in its receiving owner's stable field
        // before notifying or dropping the original previous offer.
        *target = Some(AttemptHandoffProducer { cell });
        if let Some(previous) = previous {
            previous.close(Closed::ReplacedBeforeClaim);
        }
        Ok(())
    }
}

/// Move-only and private to the original dormant/active Native behavior. The
/// actual Drive is the only production API that can bind its activation.
pub(crate) struct AttemptHandoffProducer {
    cell: Arc<Cell>,
}

impl AttemptHandoffProducer {
    pub(crate) fn stage(
        &mut self,
        execution: QueryExecutionId,
        root: TaskIdentity,
        plan: PlanSeal,
        resources: AttemptHandoffResources,
    ) -> Result<(), QueryExecutionError> {
        if execution != self.cell.execution || root.query_execution_id() != execution {
            return Err(handoff_error(
                "internal handoff staging differs from its reserved execution",
            ));
        }
        {
            let mut state = self
                .cell
                .state
                .lock()
                .map_err(|_| handoff_error("internal handoff cell lock was poisoned"))?;
            if !matches!(&*state, State::Reserved) {
                return Err(handoff_error(
                    "internal handoff can stage its original resources only once",
                ));
            }
            *state = State::Staged(Frozen {
                root,
                plan,
                resources,
            });
        }
        self.cell.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn bind(&mut self, drive: &NativeAttemptDrive) -> Result<(), QueryExecutionError> {
        self.bind_identity(drive.identity())
    }

    fn bind_identity(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        if activation.execution() != self.cell.execution {
            return Err(handoff_error(
                "internal handoff Drive belongs to another execution",
            ));
        }
        {
            let mut state = self
                .cell
                .state
                .lock()
                .map_err(|_| handoff_error("internal handoff cell lock was poisoned"))?;
            match &*state {
                State::Bound {
                    activation: original,
                    ..
                } if *original == activation => return Ok(()),
                State::Claimed(original) if original.activation == activation => return Ok(()),
                State::Staged(_) => {}
                State::Closed(reason) => return Err(reason.error()),
                _ => {
                    return Err(handoff_error(
                        "internal handoff Drive cannot replace its original binding",
                    ));
                }
            }
            let State::Staged(frozen) = std::mem::replace(&mut *state, State::Reserved) else {
                unreachable!("staging checked under the same lock")
            };
            *state = State::Bound { activation, frozen };
        }
        self.cell.changed.notify_waiters();
        Ok(())
    }

    /// Dormant/active convergence calls this before awaiting remote cleanup.
    /// It releases no Native pump, RPC, CPU receipt, or actual-exit obligation.
    pub(crate) fn retire_unclaimed(&mut self) {
        self.cell.close(Closed::OwnerConverging);
    }
}

impl Drop for AttemptHandoffProducer {
    fn drop(&mut self) {
        self.cell.close(Closed::ProducerDropped);
    }
}

impl AttemptHandoffClaimPort {
    /// Copy only the source's correlation values before returning the future;
    /// the move-only Ready source stays inside the domain consumer. No source
    /// borrow, mutex guard or payload borrow crosses the wait.
    pub(crate) fn claim(
        &self,
        source: &ComputationReadySource,
    ) -> impl Future<Output = Result<AttemptConsumerHandoff, QueryExecutionError>> + Send + 'static
    {
        let key = ClaimKey::from_source(source);
        let mailbox = Arc::clone(&self.mailbox);
        async move { claim_exact(mailbox, key).await }
    }
}

async fn claim_exact(
    mailbox: Arc<Mailbox>,
    key: ClaimKey,
) -> Result<AttemptConsumerHandoff, QueryExecutionError> {
    let cell = {
        let offer = mailbox
            .offer
            .lock()
            .map_err(|_| handoff_error("internal handoff offer lock was poisoned"))?;
        let cell = offer.as_ref().ok_or_else(|| {
            handoff_error("internal consumer has no original Native handoff offer")
        })?;
        if cell.execution != key.activation.execution()
            || key.root.query_execution_id() != cell.execution
        {
            return Err(handoff_error(
                "internal consumer names a stale or foreign Native handoff offer",
            ));
        }
        Arc::clone(cell)
    };
    loop {
        // Register the exact cell's waiter before inspecting state. A new
        // mailbox offer never redirects this waiter into another attempt.
        let notified = cell.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let claimed = {
            let mut state = cell
                .state
                .lock()
                .map_err(|_| handoff_error("internal handoff cell lock was poisoned"))?;
            match &*state {
                State::Reserved => None,
                State::Staged(frozen) => {
                    key.verify_frozen(frozen)?;
                    None
                }
                State::Bound { activation, frozen } => {
                    key.verify_frozen(frozen)?;
                    if key.activation != *activation {
                        return Err(handoff_error(
                            "internal consumer has a foreign Native activation",
                        ));
                    }
                    let State::Bound { frozen, .. } =
                        std::mem::replace(&mut *state, State::Claimed(key))
                    else {
                        unreachable!("binding checked under the same lock")
                    };
                    Some(AttemptConsumerHandoff {
                        activation: key.activation,
                        root: frozen.root,
                        plan: frozen.plan,
                        resources: frozen.resources,
                    })
                }
                State::Claimed(_) => {
                    return Err(handoff_error(
                        "the original Native handoff was already claimed",
                    ));
                }
                State::Closed(reason) => return Err(reason.error()),
            }
        };
        if let Some(claimed) = claimed {
            // Lock order never nests mailbox and cell locks. A replacement
            // installed in between is not removed by an old consumer.
            let mut offer = mailbox
                .offer
                .lock()
                .map_err(|_| handoff_error("internal handoff offer lock was poisoned"))?;
            if offer.as_ref().is_some_and(|held| Arc::ptr_eq(held, &cell)) {
                offer.take();
            }
            return Ok(claimed);
        }
        notified.await;
    }
}

fn handoff_error(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_physical_plan::PlanVersionId;
    use novarocks_types::{AttemptId, BackendProcessId, QueryId, StageId, TaskId};
    use std::pin::Pin;
    use std::task::Poll;

    fn execution(attempt: u32) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(23, 91),
            AttemptId::new(u64::from(attempt)).unwrap(),
        )
        .unwrap()
    }
    fn key(attempt: u32) -> ClaimKey {
        let execution = execution(attempt);
        ClaimKey {
            activation: AttemptActivationIdentity::from_parts(17, execution, u64::from(attempt)),
            root: TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            plan: PlanSeal::Version(PlanVersionId::try_new([3; 16]).unwrap()),
        }
    }
    async fn pending<F: Future>(future: Pin<&mut F>) {
        let mut future = future;
        std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("the original unbound handoff must still wait"),
        })
        .await;
    }
    #[test]
    fn new_original_producer_is_stable_before_the_previous_offer_wakes() {
        struct WakeFailure;
        struct PanicWake;
        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                std::panic::panic_any(WakeFailure);
            }
        }
        let (offers, _claims) = attempt_handoff_mailbox();
        let view = offers.reserve_view();
        let mut old_target = None;
        view.reserve_in(execution(1), &mut old_target).unwrap();
        let original = old_target.take().unwrap();
        let mut target = None;
        {
            let mut notified = std::pin::pin!(original.cell.changed.notified());
            let wake = std::task::Waker::from(Arc::new(PanicWake));
            let mut cx = std::task::Context::from_waker(&wake);
            assert!(notified.as_mut().poll(&mut cx).is_pending());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                view.reserve_in(execution(2), &mut target)
            }));
            let original_panic = result.expect_err("the previous real waiter must panic");
            assert!(original_panic.is::<WakeFailure>());
            let next = target
                .as_ref()
                .expect("the new original producer survived wake unwind");
            assert_eq!(next.cell.execution, execution(2));
            assert!(matches!(*next.cell.state.lock().unwrap(), State::Reserved));
            assert!(Arc::ptr_eq(
                offers.mailbox.offer.lock().unwrap().as_ref().unwrap(),
                &next.cell,
            ));
            assert!(matches!(
                *original.cell.state.lock().unwrap(),
                State::Closed(Closed::ReplacedBeforeClaim)
            ));
        }
        drop(target);
    }

    #[test]
    fn a_factory_view_cannot_recreate_an_exited_directory() {
        let (offers, claims) = attempt_handoff_mailbox();
        let view = offers.reserve_view();
        drop(offers);
        let mut target = None;
        view.reserve_in(execution(1), &mut target).unwrap();
        let producer = target.take().unwrap();
        drop(claims);
        assert!(view.mailbox.upgrade().is_none());
        assert!(view.reserve_in(execution(2), &mut target).is_err());
        assert!(target.is_none());
        assert!(matches!(
            *producer.cell.state.lock().unwrap(),
            State::Reserved
        ));
        // The actual producer retains its independent original cell until exit.
        drop(producer);
    }

    #[tokio::test]
    async fn producer_drop_wakes_the_exact_unbound_waiter() {
        let (offers, claims) = attempt_handoff_mailbox();
        let producer = offers.reserve(execution(1)).unwrap();
        let waiting = claim_exact(Arc::clone(&claims.mailbox), key(1));
        tokio::pin!(waiting);
        pending(waiting.as_mut()).await;
        drop(producer);
        assert!(waiting.await.is_err());
    }
    #[tokio::test]
    async fn a_stale_waiter_cannot_take_a_replacement_offer() {
        let (offers, claims) = attempt_handoff_mailbox();
        let old = offers.reserve(execution(1)).unwrap();
        let waiting = claim_exact(Arc::clone(&claims.mailbox), key(1));
        tokio::pin!(waiting);
        pending(waiting.as_mut()).await;
        let next = offers.reserve(execution(2)).unwrap();
        assert!(waiting.await.is_err());
        assert!(
            claim_exact(Arc::clone(&claims.mailbox), key(1))
                .await
                .is_err()
        );
        assert!(Arc::ptr_eq(
            claims.mailbox.offer.lock().unwrap().as_ref().unwrap(),
            &next.cell
        ));
        let new_waiter = claim_exact(Arc::clone(&claims.mailbox), key(2));
        tokio::pin!(new_waiter);
        pending(new_waiter.as_mut()).await;
        drop(old);
        pending(new_waiter.as_mut()).await;
        drop(next);
        assert!(new_waiter.await.is_err());
    }
    #[test]
    fn a_duplicate_offer_does_not_replace_the_original_cell() {
        let (offers, claims) = attempt_handoff_mailbox();
        let producer = offers.reserve(execution(1)).unwrap();
        assert!(offers.reserve(execution(1)).is_err());
        assert!(Arc::ptr_eq(
            claims.mailbox.offer.lock().unwrap().as_ref().unwrap(),
            &producer.cell
        ));
    }
}

#[cfg(all(test, novarocks_dev_controls))]
#[path = "attempt_handoff_component_tests.rs"]
pub(crate) mod component_tests;
