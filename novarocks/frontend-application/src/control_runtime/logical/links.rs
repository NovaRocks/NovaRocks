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

//! A fixed inventory of original activation links, never reconstructed from IDs.
use super::{funding::GENERATION, work::FailureReporter};
use crate::control_runtime::{actor::FrontendActor, host::HostExit};
use crate::task_execution::host::{funding::AttemptHostFunding, port::AttemptPortEndpoints};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    header_readiness::HeaderReadyRegistration,
    obligation::{ReservationObligation, WorkObligation},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{Generation, ScopeId},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_query_application::api::LogicalFuture;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::sync::Notify;

/// Checked by the original Work/header and parent Scope retirement gates.
/// Drop never substitutes for actual activation, loan or child exit.
struct ActivationLinkFundingObligation {
    work: WorkObligation,
    payer: ReservationObligation,
    readiness: HeaderReadyRegistration,
    exited: bool,
}
struct State {
    activation: Option<AttemptActivationIdentity>,
    original: Option<ActivationLinkFundingObligation>,
    borrowed: bool,
    activation_exited: bool,
    child: Option<ScopeId>,
    child_exit: Option<LogicalFuture<'static, Result<(), tokio::sync::watch::error::RecvError>>>,
    child_exit_observer: Option<tokio::sync::watch::Receiver<bool>>,
    child_exited: bool,
    endpoints: Option<Arc<AttemptPortEndpoints>>,
    input_author_closed: bool,
    storage_issued: bool,
    retired: bool,
}
struct Slot {
    state: Mutex<State>,
    wake: Waker,
    failure: FailureReporter,
    changes: Arc<Notify>,
}
pub(crate) struct LogicalAttemptLinks {
    slots: Vec<Arc<Slot>>,
    routes: Mutex<BTreeMap<(u64, novarocks_types::QueryExecutionId, u64), usize>>,
    changes: Arc<Notify>,
}
/// Passive facts from this exact prepaid row. No copied identity can create
/// a link, child, endpoint, Work or positive remote closure.
pub(super) struct LogicalAttemptCleanupRow {
    pub(super) activation: Option<AttemptActivationIdentity>,
    pub(super) endpoints: Option<Arc<AttemptPortEndpoints>>,
    pub(super) activation_exited: bool,
}
impl LogicalAttemptLinks {
    /// Validate the SAME borrowed allocation; copied identities/indexes never
    /// authorize binding metadata to a different logical inventory.
    pub(crate) fn matches_loan(
        &self,
        original: &AttemptActivationLinkLoan,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.slots.get(original.index).is_some_and(|slot| {
            Arc::ptr_eq(slot, &original.slot) && {
                let state = slot.state.lock().unwrap();
                state.borrowed && !state.retired && state.activation == Some(activation)
            }
        })
    }
    pub(super) fn cleanup_row_count(&self) -> usize {
        self.slots.len()
    }
    pub(super) fn cleanup_row(&self, index: usize) -> LogicalAttemptCleanupRow {
        let state = self.slots[index].state.lock().unwrap();
        LogicalAttemptCleanupRow {
            activation: state.activation,
            endpoints: state.endpoints.clone(),
            activation_exited: state.activation_exited,
        }
    }
    pub(crate) fn new(count: usize, wake: Waker, failure: FailureReporter) -> Arc<Self> {
        let changes = Arc::new(Notify::new());
        Arc::new(Self {
            routes: Mutex::new(BTreeMap::new()),
            changes: changes.clone(),
            slots: (0..count)
                .map(|_| {
                    Arc::new(Slot {
                        state: Mutex::new(State {
                            activation: None,
                            original: None,
                            borrowed: false,
                            activation_exited: false,
                            child: None,
                            child_exit: None,
                            child_exit_observer: None,
                            child_exited: false,
                            endpoints: None,
                            input_author_closed: false,
                            storage_issued: false,
                            retired: false,
                        }),
                        wake: wake.clone(),
                        failure: failure.clone(),
                        changes: changes.clone(),
                    })
                })
                .collect(),
        })
    }
    /// Passive observation of this same original inventory, without custody or
    /// authority. Enable `notified()` before scanning, then await it only if
    /// the scan still needs progress. Notifications carry no stored permit.
    /// Borrowing/scanning alone never notifies: that would wake the scanner
    /// itself indefinitely. Every notification runs outside the custody locks.
    pub(crate) fn changes(&self) -> Arc<Notify> {
        self.changes.clone()
    }
    /// Observe this SAME bound HostExit even before the parent Source gets its
    /// next progress turn. Missing routing or a disconnected false watch is
    /// not exit, and this says nothing about any remote participation.
    pub(super) fn actual_child_exited(&self, activation: AttemptActivationIdentity) -> bool {
        let Some(index) = self
            .routes
            .lock()
            .unwrap()
            .get(&Self::key(activation))
            .copied()
        else {
            return false;
        };
        let state = self.slots[index].state.lock().unwrap();
        state.activation == Some(activation)
            && (state.child_exited
                || state
                    .child_exit_observer
                    .as_ref()
                    .is_some_and(|exit| *exit.borrow()))
    }
    /// Called only with the same actual ActivateAttempt step's reported parent.
    pub(super) fn issue(
        &self,
        index: usize,
        activation: AttemptActivationIdentity,
        parent: &ReportedWork,
    ) -> Result<(), RuntimeError> {
        let slot = &self.slots[index];
        // This row is owned by the serial logical Source. No kernel call runs
        // under its small custody mutex; the original is published afterwards.
        assert!(
            slot.state.lock().unwrap().activation.is_none(),
            "one original activation link"
        );
        let guard = parent.work().scope().reserve_child_next(
            GENERATION,
            parent.reservation_id(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Business,
        )?;
        let (mut work, payer) = guard.bind(WorkKind::Business)?;
        let readiness = work.header_readiness_registration()?;
        let started = work.start_in_generation(GENERATION);
        let mut state = slot.state.lock().unwrap();
        state.activation = Some(activation);
        state.original = Some(ActivationLinkFundingObligation {
            work,
            payer,
            readiness,
            exited: false,
        });
        if started.is_err() {
            state.activation_exited = true;
        }
        drop(state);
        let prior = self
            .routes
            .lock()
            .unwrap()
            .insert(Self::key(activation), index);
        assert!(prior.is_none(), "one original activation routing row");
        self.changes.notify_waiters();
        started
    }
    /// The identity selects an existing live original; it cannot issue a link.
    /// One Native activation owns the move-only loan until its actual return.
    pub(crate) fn borrow(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Option<AttemptActivationLinkLoan> {
        let index = *self.routes.lock().unwrap().get(&Self::key(activation))?;
        let slot = &self.slots[index];
        let mut state = slot.state.lock().unwrap();
        if state.borrowed || state.activation_exited || state.child.is_some() || state.retired {
            return None;
        }
        let original = state.original.take()?;
        state.borrowed = true;
        Some(AttemptActivationLinkLoan {
            index,
            slot: slot.clone(),
            original: Some(original),
        })
    }
    fn key(a: AttemptActivationIdentity) -> (u64, novarocks_types::QueryExecutionId, u64) {
        (a.actor().get(), a.execution(), a.generation())
    }
    pub(crate) fn endpoint(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Option<Arc<AttemptPortEndpoints>> {
        let index = *self.routes.lock().unwrap().get(&Self::key(activation))?;
        let state = self.slots[index].state.lock().unwrap();
        if state.retired {
            None
        } else {
            state.endpoints.clone()
        }
    }
    pub(super) fn endpoint_progress(
        &self,
        index: usize,
        failed: bool,
        rescan: bool,
    ) -> Result<Option<crate::task_execution::host::port::facts::FactProgress>, ActorTurnError>
    {
        let endpoints = self.slots[index].state.lock().unwrap().endpoints.clone();
        endpoints
            .map(|e| {
                if rescan {
                    e.facts.request_rescan();
                }
                e.facts.progress(1, e.producer_exited(), failed)
            })
            .transpose()
    }
    pub(super) fn input_author_closed(&self, index: usize) {
        let slot = &self.slots[index];
        let changed = {
            let mut state = slot.state.lock().unwrap();
            let changed = !state.input_author_closed;
            state.input_author_closed = true;
            changed
        };
        if changed {
            slot.changes.notify_waiters();
        }
    }
    pub(super) fn activation_exited(&self, index: usize) {
        let slot = &self.slots[index];
        let changed = {
            let mut state = slot.state.lock().unwrap();
            let changed = !state.activation_exited;
            state.activation_exited = true;
            changed
        };
        if changed {
            slot.changes.notify_waiters();
        }
    }
    /// One paid Source visit. Child removal, Native exit and header reaping
    /// remain independent facts; no control call stops the child's Scope.
    pub(super) fn progress(
        &self,
        index: usize,
        runtime: &ActorRuntime<FrontendActor>,
        registration: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        let slot = &self.slots[index];
        let wait = {
            let mut state = slot.state.lock().unwrap();
            if state.child.is_some() && !state.child_exited {
                state.child_exit.take()
            } else {
                None
            }
        };
        if let Some(mut wait) = wait {
            let result = wait.as_mut().poll(&mut Context::from_waker(&slot.wake));
            match result {
                Poll::Ready(Ok(())) => {
                    slot.state.lock().unwrap().child_exited = true;
                    slot.changes.notify_waiters();
                }
                // Preserve the exact infrastructure failure outside custody
                // locks. Sender loss proves no physical child removal.
                Poll::Ready(Err(original)) => (slot.failure)(Box::new(original)),
                Poll::Pending => slot.state.lock().unwrap().child_exit = Some(wait),
            }
        }
        let mut original = {
            let mut state = slot.state.lock().unwrap();
            if state.activation.is_none() || state.retired {
                return Ok(true);
            }
            if state.borrowed || !state.activation_exited {
                return Ok(false);
            }
            if state.endpoints.is_some() && !state.input_author_closed {
                return Ok(false);
            }
            if state.child.is_some() && !state.child_exited {
                return Ok(false);
            }
            state
                .original
                .take()
                .expect("same returned activation link")
        };
        // The owner always returns this same original, including kernel error.
        let was_exited = original.exited;
        let result = (|| {
            if !original.exited {
                original.work.body_exited()?;
                original.work.captures_exited()?;
                original.exited = true;
            }
            let observer = original.work.retirement_observer();
            if original.work.actual_exit_ready()? {
                original.payer.finish()?;
                if original.work.scope().reap_observed_header(&observer)? {
                    original.readiness.cancel();
                    return Ok(true);
                }
            }
            original
                .readiness
                .arm(runtime, registration, &observer)
                .expect("original activation header wake");
            Ok(false)
        })();
        let changed = !was_exited && original.exited || matches!(result, Ok(true));
        let mut state = slot.state.lock().unwrap();
        if matches!(result, Ok(true)) {
            state.retired = true;
        } else {
            state.original = Some(original);
        }
        drop(state);
        if changed {
            slot.changes.notify_waiters();
        }
        result
    }
}
/// The same actual Business link. Drop returns custody; it proves no exit.
#[must_use = "Return the original activation link loan after actual installation"]
pub(crate) struct AttemptActivationLinkLoan {
    index: usize,
    slot: Arc<Slot>,
    original: Option<ActivationLinkFundingObligation>,
}
impl AttemptActivationLinkLoan {
    pub(crate) fn matches_child_registration(
        &self,
        activation: AttemptActivationIdentity,
        registration: &ActorRegistration,
    ) -> bool {
        let state = self.slot.state.lock().unwrap();
        state.activation == Some(activation) && state.child == Some(registration.scope_id())
    }
    pub(crate) fn matches_registered(
        &self,
        activation: AttemptActivationIdentity,
        registration: &ActorRegistration,
        exit: &HostExit,
    ) -> bool {
        self.matches_child_registration(activation, registration) && exit.matches(registration)
    }
    /// Confirm an already committed SAME exit binding after a notification
    /// boundary. The polling future can be temporarily loaned to its parent;
    /// the original watch channel remains the stable receiving receipt.
    #[cfg(test)]
    pub(crate) fn has_bound_host_exit(
        &self,
        activation: AttemptActivationIdentity,
        registration: &ActorRegistration,
        exit: &HostExit,
    ) -> bool {
        if !self.matches_registered(activation, registration, exit) {
            return false;
        }
        let original = exit.observe_actual_exit();
        self.slot
            .state
            .lock()
            .unwrap()
            .child_exit_observer
            .as_ref()
            .is_some_and(|bound| bound.same_channel(&original))
    }
    pub(crate) fn index(&self) -> usize {
        self.index
    }
    pub(crate) fn work(&self) -> &WorkObligation {
        &self.original.as_ref().unwrap().work
    }
    /// Construct only the real child funding from this original admitted link.
    /// One fixed reader/cell storage obligation is derived from this actual
    /// original link. A copied identity cannot allocate another reader author.
    pub(crate) fn reserve_port_storage(&self) -> Result<ReportedWork, RuntimeError> {
        {
            let mut state = self.slot.state.lock().unwrap();
            assert!(
                !state.storage_issued,
                "one original port reader storage author"
            );
            state.storage_issued = true;
        }
        let original = self.original.as_ref().expect("same live link loan");
        // An already-issued Native activation can still bind this SAME
        // settlement reader after its original logical Scope failed. This
        // derives no new Business link and uses only that Scope's actual epoch.
        let generation = original.work.scope().snapshot()?.generation;
        let guard = original.work.scope().reserve_child_next(
            generation,
            original.payer.id(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
        )?;
        Ok(super::funding::reported_step(guard, WorkKind::Settlement))
    }
    pub(crate) fn reserve_attempt(
        &self,
        ledger: &RuntimeLedger,
        generation: Generation,
        resources: ReservationResources,
    ) -> Result<AttemptHostFunding, RuntimeError> {
        assert!(
            self.slot.state.lock().unwrap().child.is_none(),
            "one original child Scope"
        );
        let funding =
            AttemptHostFunding::reserve(ledger, Some(self.work()), generation, resources)?;
        self.slot.state.lock().unwrap().child = Some(funding.scope().id());
        self.slot.changes.notify_waiters();
        Ok(funding)
    }
    pub(crate) fn retire_unpublished_child(
        &self,
        funding: AttemptHostFunding,
        ledger: &RuntimeLedger,
    ) {
        assert_eq!(
            self.slot.state.lock().unwrap().child,
            Some(funding.scope().id())
        );
        funding.retire_unpublished(ledger);
        let mut state = self.slot.state.lock().unwrap();
        assert!(
            state.child_exit.is_none(),
            "unpublished child has no installed host"
        );
        state.child = None;
        drop(state);
        self.slot.changes.notify_waiters();
    }
    /// Bind only endpoints created for this exact already-funded activation.
    /// The same original host entry owns this row; no second registry or Scope
    /// control authority is introduced by the passive lookup above.
    pub(crate) fn bind_endpoints(&self, endpoints: Arc<AttemptPortEndpoints>) {
        let mut state = self.slot.state.lock().unwrap();
        assert_eq!(state.activation, Some(endpoints.activation()));
        assert_eq!(state.child, Some(endpoints.attempt().scope_id()));
        assert!(endpoints.logical().matches_scope(self.work().scope()));
        assert!(state.endpoints.is_none(), "one original endpoint pair");
        state.endpoints = Some(endpoints);
        drop(state);
        self.slot.changes.notify_waiters();
    }
    pub(crate) fn bind_registered(&self, registration: &ActorRegistration, exit: &HostExit) {
        assert!(exit.matches(registration), "same original AP exit observer");
        let mut state = self.slot.state.lock().unwrap();
        assert_eq!(
            state.child,
            Some(registration.scope_id()),
            "same funded child Scope"
        );
        assert!(
            state.child_exit_observer.is_none(),
            "one original AP registration"
        );
        let mut exited = exit.observe_actual_exit();
        state.child_exit_observer = Some(exited.clone());
        state.child_exit = Some(Box::pin(async move {
            exited.wait_for(|removed| *removed).await.map(|_| ())
        }));
        drop(state);
        self.slot.changes.notify_waiters();
    }
}
impl Drop for AttemptActivationLinkLoan {
    fn drop(&mut self) {
        {
            let mut state = self.slot.state.lock().unwrap();
            assert!(
                state.borrowed && state.original.is_none(),
                "same checked link return"
            );
            state.original = self.original.take();
            state.borrowed = false;
        }
        // Return custody even during unwinding. A passive observer's wake must
        // not prevent the original host wake or turn this into a double panic.
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.slot.changes.notify_waiters()
        }));
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.slot.wake.wake_by_ref()
            }));
        } else {
            self.slot.wake.wake_by_ref();
            if let Err(original) = notified {
                std::panic::resume_unwind(original);
            }
        }
    }
}

#[cfg(test)]
mod tests;
