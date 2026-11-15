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

//! Original continuous-source positions and their prepaid Work. Caller views
//! are weak; only actual position closure permits the corresponding header exit.

use super::{
    failure::LogicalInfrastructureFailure,
    funding::{LogicalFundingObligation, SourceFundingObligation},
    geometry::LogicalGeometry,
    inlet::{InletFailureCallback, InletProgress, LogicalInletObligation},
    installed::{LogicalInstalledState, SenderCell},
};
use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, StandingInputObligation},
    obligation::WorkObligation,
};
use novarocks_control_runtime_core::{
    mailbox::InputClass, owned_storage::OwnedSlotNeed, scope::WorkKind,
};
use novarocks_coordination_contract::{AttemptActivationIdentity, LogicalExecutionActorId};
use novarocks_logical_execution_protocol::{
    LogicalSourceId,
    input::{LogicalInput, RegistryClose},
    start::LogicalStart,
};
use novarocks_query_application::api::{
    LogicalFuture, LogicalSourceClosed, LogicalSourceKind, LogicalSourceRefusal,
};
use novarocks_types::QueryId;
use std::{
    future::poll_fn,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
};

pub(super) struct InputProgress {
    pub(super) units: usize,
    pub(super) progressed: bool,
    /// Only an unfinished finite scan, never an asynchronous dependency wait.
    pub(super) more: bool,
    pub(super) complete: bool,
}

struct InputSlot {
    funding: Option<SourceFundingObligation>,
    position: Option<StandingInputObligation<FrontendActor>>,
    inlet: Option<LogicalInletObligation>,
    sender: Option<SenderCell>,
    id: LogicalSourceId,
    activation: Option<AttemptActivationIdentity>,
}
impl InputSlot {
    fn bind(
        &mut self,
        kind: LogicalSourceKind,
        installed: &LogicalInstalledState,
        wake: &Waker,
        failure: &Arc<LogicalInfrastructureFailure>,
        admission_closed: &Arc<AtomicBool>,
    ) -> SenderCell {
        let owner = Arc::clone(failure);
        let callback: InletFailureCallback =
            Arc::new(move |original| owner.retain(Box::new(original)));
        let mut inlet = LogicalInletObligation::new(
            self.position
                .take()
                .expect("original unbound standing input"),
            self.id,
            kind,
            wake.clone(),
            callback,
        )
        .with_admission_gate(Arc::clone(admission_closed));
        let sender = Arc::new(tokio::sync::Mutex::new(
            inlet.take_sender().expect("original inlet has one sender"),
        ));
        // Retain the strong original BEFORE publishing its weak caller view.
        self.sender = Some(Arc::clone(&sender));
        self.inlet = Some(inlet);
        installed.bind_source(kind, self.id, &sender);
        if kind == LogicalSourceKind::Registry {
            installed.bind_registry_close(
                self.inlet
                    .as_ref()
                    .expect("same original Registry inlet")
                    .ingress_close_observer(),
            );
        }
        sender
    }
    fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
        closing: bool,
        producer_exited: bool,
        failed: bool,
    ) -> Result<bool, ActorTurnError> {
        let mut changed = false;
        let closed = if let Some(inlet) = &mut self.inlet {
            if closing {
                inlet.close();
            }
            changed = inlet.progress(producer_exited, failed)? == InletProgress::Changed;
            if !inlet.is_closed() {
                return Ok(changed);
            }
            changed |= self.sender.take().is_some();
            true
        } else if (closing || failed) && producer_exited {
            if let Some(position) = &mut self.position {
                position.close_after_exit()?;
            }
            changed |= self.position.take().is_some();
            true
        } else {
            false
        };
        if closed
            && let Some(author) = &mut self.funding
            && author.progress(runtime, actor)?
        {
            self.funding.take();
            return Ok(true);
        }
        Ok(changed)
    }
}

struct ApInventory {
    funding: Option<SourceFundingObligation>,
    issued: bool,
    actual_inputs_closed: bool,
}

/// The one original Bootstrap position is loaned out only while filling Started.
/// A clone of this publisher neither retains its Work nor keeps the Scope open.
pub(super) struct LogicalBootstrapPublisher {
    original: Weak<BootstrapShared>,
}
struct BootstrapState {
    position: Option<StandingInputObligation<FrontendActor>>,
    claimed: bool,
    ended: bool,
    frame_live: bool,
    closing: bool,
    closed: bool,
    host_waiting: bool,
    publisher_waiting: Option<Waker>,
}
struct BootstrapShared {
    state: Mutex<BootstrapState>,
    wake: Waker,
    failure: Arc<LogicalInfrastructureFailure>,
    admission_closed: Arc<AtomicBool>,
}
impl BootstrapShared {
    fn lock(&self) -> MutexGuard<'_, BootstrapState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn loan(self: &Arc<Self>, host: bool) -> Option<BootstrapPositionLoanObligation> {
        let mut state = self.lock();
        let position = state.position.take();
        if position.is_none() && host {
            state.host_waiting = true;
        }
        position.map(|position| BootstrapPositionLoanObligation {
            original: Arc::clone(self),
            position: Some(position),
        })
    }
}
#[must_use = "Return the original bootstrap standing-position loan, including on unwind"]
struct BootstrapPositionLoanObligation {
    original: Arc<BootstrapShared>,
    position: Option<StandingInputObligation<FrontendActor>>,
}
impl Drop for BootstrapPositionLoanObligation {
    fn drop(&mut self) {
        let (host, publisher) = {
            let mut state = self.original.lock();
            assert!(state.position.is_none(), "one original bootstrap position");
            state.position = self.position.take();
            (
                std::mem::take(&mut state.host_waiting),
                state.publisher_waiting.take(),
            )
        };
        wake_returned(|| {
            if host {
                self.original.wake.wake_by_ref();
            }
            if let Some(wake) = publisher {
                wake.wake();
            }
        });
    }
}
#[must_use = "Publishing ends only when the original short publisher frame exits"]
struct BootstrapFrameObligation {
    original: Arc<BootstrapShared>,
}
impl Drop for BootstrapFrameObligation {
    fn drop(&mut self) {
        {
            let mut state = self.original.lock();
            state.frame_live = false;
            state.ended = true;
        }
        wake_returned(|| self.original.wake.wake_by_ref());
    }
}
fn wake_returned(wake: impl FnOnce()) {
    if std::thread::panicking() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wake));
    } else {
        wake();
    }
}
impl LogicalBootstrapPublisher {
    pub(super) fn publish_started(&self) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            let original = self.original.upgrade().ok_or(LogicalSourceClosed)?;
            {
                let mut state = original.lock();
                if state.claimed
                    || state.closing
                    || state.closed
                    || original.admission_closed.load(Ordering::Acquire)
                {
                    return Err(LogicalSourceClosed);
                }
                state.claimed = true;
                state.frame_live = true;
            }
            let _frame = BootstrapFrameObligation {
                original: Arc::clone(&original),
            };
            let loan = poll_fn(|cx| {
                let mut state = original.lock();
                if state.closing || original.admission_closed.load(Ordering::Acquire) {
                    Poll::Ready(None)
                } else if let Some(position) = state.position.take() {
                    Poll::Ready(Some(BootstrapPositionLoanObligation {
                        original: Arc::clone(&original),
                        position: Some(position),
                    }))
                } else {
                    state.publisher_waiting = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await
            .ok_or(LogicalSourceClosed)?;
            // The kernel and all foreign wakers execute outside our state mutex.
            if let Err(error) = loan
                .position
                .as_ref()
                .expect("original bootstrap loan")
                .fill(FrontendInput::Logical(Box::new(LogicalInput::Started)))
            {
                original.failure.retain(Box::new(error));
                return Err(LogicalSourceClosed);
            }
            // Do not await input retirement here: the SAME controlled job next
            // observes cancellation, which may itself depend on Started.
            Ok(())
        })
    }
}
struct BootstrapInput {
    original: Arc<BootstrapShared>,
    publisher: Arc<LogicalBootstrapPublisher>,
    funding: Option<SourceFundingObligation>,
}
impl BootstrapInput {
    fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
        closing: bool,
        producer_exited: bool,
        failed: bool,
    ) -> Result<bool, ActorTurnError> {
        if self.original.lock().closed {
            return retire_author(&mut self.funding, runtime, actor);
        }
        let publisher_wake = {
            let mut state = self.original.lock();
            if closing || failed {
                state.closing = true;
                state.publisher_waiting.take()
            } else {
                None
            }
        };
        if let Some(wake) = publisher_wake {
            wake.wake();
        }
        let Some(mut loan) = self.original.loan(true) else {
            return Ok(false);
        };
        let position = loan.position.as_mut().expect("original bootstrap loan");
        let mut changed = false;
        if position.in_flight()?.is_some() {
            if failed {
                match position.take_failed_role_payload() {
                    Ok(Some(original)) => {
                        original.dispose()?;
                        changed = true;
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            if !position.retire_ready_after_exit()? {
                return Ok(changed);
            }
            changed = true;
        }
        let can_close = {
            let state = self.original.lock();
            !state.frame_live && (state.ended || (state.closing && producer_exited))
        };
        if can_close {
            position.close_after_exit()?;
            self.original.lock().closed = true;
            changed = true;
        }
        drop(loan);
        if can_close {
            changed |= retire_author(&mut self.funding, runtime, actor)?;
        }
        Ok(changed)
    }
}
struct CloseInput {
    position: StandingInputObligation<FrontendActor>,
    funding: Option<SourceFundingObligation>,
    sent: u8,
    closed: bool,
    failure: Arc<LogicalInfrastructureFailure>,
}
impl CloseInput {
    fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
        requested: u8,
        closing: bool,
        producer_exited: bool,
        failed: bool,
    ) -> Result<bool, ActorTurnError> {
        if self.closed {
            return retire_author(&mut self.funding, runtime, actor);
        }
        let mut changed = false;
        if self.position.in_flight()?.is_some() {
            if failed {
                match self.position.take_failed_role_payload() {
                    Ok(Some(original)) => {
                        original.dispose()?;
                        changed = true;
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            if !self.position.retire_ready_after_exit()? {
                return Ok(changed);
            }
            changed = true;
        }
        if requested > self.sent && !failed {
            let close = if requested >= 2 {
                RegistryClose::Shutdown
            } else {
                RegistryClose::Retire
            };
            if let Err((error, original)) = self.position.fill(FrontendInput::Logical(Box::new(
                LogicalInput::RegistryClose(close),
            ))) {
                self.failure.retain(Box::new(original));
                return Err(error);
            }
            self.sent = requested;
            return Ok(true);
        }
        if (closing || failed) && producer_exited {
            self.position.close_after_exit()?;
            self.closed = true;
            changed = true;
            changed |= retire_author(&mut self.funding, runtime, actor)?;
        }
        Ok(changed)
    }
}
fn retire_author(
    author: &mut Option<SourceFundingObligation>,
    runtime: &ActorRuntime<FrontendActor>,
    actor: &ActorRegistration,
) -> Result<bool, ActorTurnError> {
    if let Some(original) = author
        && original.progress(runtime, actor)?
    {
        author.take();
        return Ok(true);
    }
    Ok(false)
}

/// Fixed original inventory. AP author Work is prepaid here, but its three
/// standing roles are bound only by P10 using the real AP actor address.
#[must_use = "Retire every original input and source header before logical Scope removal"]
pub(crate) struct LogicalInputs {
    registration: ActorRegistration,
    actor: LogicalExecutionActorId,
    query: QueryId,
    installed: Arc<LogicalInstalledState>,
    failure: Arc<LogicalInfrastructureFailure>,
    wake: Waker,
    data: Vec<InputSlot>,
    globals: Vec<InputSlot>,
    bootstrap: BootstrapInput,
    close_input: CloseInput,
    ap: Vec<ApInventory>,
    closing: bool,
    admission_closed: Arc<AtomicBool>,
    cursor: usize,
    scan_remaining: usize,
    live_authors: usize,
    retired: bool,
}
impl LogicalInputs {
    pub(super) fn reserve(
        runtime: &ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        funding: &LogicalFundingObligation,
        geometry: &LogicalGeometry,
        start: &LogicalStart,
        installed: Arc<LogicalInstalledState>,
        failure: Arc<LogicalInfrastructureFailure>,
        wake: Waker,
    ) -> Self {
        let admission_closed = Arc::new(AtomicBool::new(false));
        let mut ordinal = geometry.purposes;
        let mut reserve_position = |class| {
            let source = geometry
                .source(ordinal)
                .expect("original logical input address");
            ordinal = ordinal
                .checked_add(1)
                .expect("validated original source inventory");
            let author = reserve_author(funding, 1);
            let position = runtime
                .reserve_admitted_input_for(&registration, source, class, author.work())
                .expect("prepaid original logical input")
                .into_standing_input()
                .unwrap_or_else(|_| panic!("prepaid original logical standing input"));
            (
                author,
                position,
                LogicalSourceId::new(
                    NonZeroU64::new(source.get()).expect("nonzero original source"),
                ),
            )
        };
        let mut make_slot = |class| {
            let (author, position, id) = reserve_position(class);
            InputSlot {
                funding: Some(author),
                position: Some(position),
                inlet: None,
                sender: None,
                id,
                activation: None,
            }
        };
        let data = (0..geometry.attempts)
            .map(|_| make_slot(InputClass::Ordinary))
            .collect();
        let mut globals = Vec::with_capacity(4);
        for (kind, class) in [
            (LogicalSourceKind::Cancellation, InputClass::Control),
            (LogicalSourceKind::Consumer, InputClass::Ordinary),
            (LogicalSourceKind::Registry, InputClass::Lifecycle),
            (LogicalSourceKind::Snapshot, InputClass::Ordinary),
        ] {
            let mut slot = make_slot(class);
            slot.bind(kind, &installed, &wake, &failure, &admission_closed);
            globals.push(slot);
        }
        let (bootstrap_author, bootstrap_position, _) = reserve_position(InputClass::Lifecycle);
        let original = Arc::new(BootstrapShared {
            state: Mutex::new(BootstrapState {
                position: Some(bootstrap_position),
                claimed: false,
                ended: false,
                frame_live: false,
                closing: false,
                closed: false,
                host_waiting: false,
                publisher_waiting: None,
            }),
            wake: wake.clone(),
            failure: Arc::clone(&failure),
            admission_closed: Arc::clone(&admission_closed),
        });
        let publisher = Arc::new(LogicalBootstrapPublisher {
            original: Arc::downgrade(&original),
        });
        let (close_author, close_position, _) = reserve_position(InputClass::Control);
        let ap = (0..geometry.attempts)
            .map(|_| ApInventory {
                funding: Some(reserve_author(funding, 3)),
                issued: false,
                actual_inputs_closed: false,
            })
            .collect();
        let mut inputs = Self {
            registration,
            actor: start.actor_instance,
            query: start.identity,
            installed,
            failure: Arc::clone(&failure),
            wake,
            data,
            globals,
            bootstrap: BootstrapInput {
                original,
                publisher,
                funding: Some(bootstrap_author),
            },
            close_input: CloseInput {
                position: close_position,
                funding: Some(close_author),
                sent: 0,
                closed: false,
                failure: Arc::clone(&failure),
            },
            ap,
            closing: false,
            admission_closed,
            cursor: 0,
            scan_remaining: 0,
            live_authors: geometry.attempts * 2 + 6,
            retired: false,
        };
        inputs
            .bind_data_activation(start.first_attempt.activation)
            .expect("validated original initial activation");
        inputs
    }
    /// The SAME admission flag is observation/closure only. It grants no
    /// source author, position, activation or producer-exit authority.
    pub(super) fn admission_gate(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.admission_closed)
    }
    pub(super) fn bootstrap(&self) -> Arc<LogicalBootstrapPublisher> {
        Arc::clone(&self.bootstrap.publisher)
    }
    pub(crate) fn bind_data_activation(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<SenderCell, LogicalSourceRefusal> {
        if self.closing {
            return Err(LogicalSourceRefusal::Closed);
        }
        if activation.actor() != self.actor || activation.execution().query_id() != self.query {
            return Err(LogicalSourceRefusal::ForeignActivation);
        }
        if let Some(slot) = self
            .data
            .iter()
            .find(|slot| slot.activation == Some(activation))
        {
            return slot.sender.clone().ok_or(LogicalSourceRefusal::Closed);
        }
        if self.data.iter().any(|slot| {
            slot.activation
                .is_some_and(|prior| prior.execution() == activation.execution())
        }) {
            return Err(LogicalSourceRefusal::ForeignActivation);
        }
        let slot = self
            .data
            .iter_mut()
            .find(|slot| slot.activation.is_none())
            .ok_or(LogicalSourceRefusal::ForeignActivation)?;
        slot.activation = Some(activation);
        Ok(slot.bind(
            LogicalSourceKind::AttemptData(activation),
            &self.installed,
            &self.wake,
            &self.failure,
            &self.admission_closed,
        ))
    }
    pub(crate) fn ap_count(&self) -> usize {
        self.ap.len()
    }
    /// Borrowing records that the same author was exposed to P10. The three
    /// roles and original producer must then explicitly close before retirement.
    pub(crate) fn ap_author(&mut self, index: usize) -> Option<&WorkObligation> {
        if self.closing {
            return None;
        }
        let inventory = self.ap.get_mut(index)?;
        inventory.issued = true;
        inventory
            .funding
            .as_ref()
            .map(SourceFundingObligation::work)
    }
    /// A late original activation may bind after admission closes. Only the
    /// move-only loan from the already-issued Business link permits this path;
    /// numeric activation or row identities cannot recreate that authority.
    #[cfg_attr(
        test,
        expect(
            dead_code,
            reason = "P12 binds the original Native activation to its already-issued AP author"
        )
    )]
    pub(crate) fn ap_author_from_link(
        &mut self,
        link: &super::links::AttemptActivationLinkLoan,
    ) -> Option<&WorkObligation> {
        let inventory = self.ap.get_mut(link.index())?;
        let funding = inventory.funding.as_ref()?;
        if !funding.work().scope().same_scope(link.work().scope()) {
            return None;
        }
        inventory.issued = true;
        Some(funding.work())
    }
    pub(crate) fn ap_inputs_closed_after_producer_exit(
        &mut self,
        index: usize,
    ) -> Result<(), LogicalSourceRefusal> {
        let inventory = self
            .ap
            .get_mut(index)
            .ok_or(LogicalSourceRefusal::ForeignActivation)?;
        if !inventory.issued {
            return Err(LogicalSourceRefusal::ForeignActivation);
        }
        if !inventory.actual_inputs_closed {
            inventory.actual_inputs_closed = true;
            self.wake.wake_by_ref();
        }
        Ok(())
    }
    pub(super) fn close(&mut self) {
        if !self.closing {
            self.closing = true;
            self.admission_closed.store(true, Ordering::Release);
            self.scan_remaining = self.data.len() + self.globals.len() + 2 + self.ap.len();
        }
    }
    pub(super) fn is_retired(&self) -> bool {
        self.retired
    }
    pub(super) fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        producer_exited: bool,
        actor_failed: bool,
        budget: usize,
    ) -> Result<InputProgress, ActorTurnError> {
        if self.retired {
            return Ok(InputProgress {
                units: 0,
                progressed: false,
                more: false,
                complete: true,
            });
        }
        if actor_failed {
            self.close();
        }
        let rows = self.data.len() + self.globals.len() + 2 + self.ap.len();
        if self.scan_remaining == 0 {
            self.scan_remaining = rows;
        }
        let units = budget.min(self.scan_remaining);
        let mut progressed = false;
        for _ in 0..units {
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % rows;
            self.scan_remaining -= 1;
            let live_before = if index < self.data.len() {
                self.data[index].funding.is_some()
            } else if index < self.data.len() + self.globals.len() {
                self.globals[index - self.data.len()].funding.is_some()
            } else if index == self.data.len() + self.globals.len() {
                self.bootstrap.funding.is_some()
            } else if index == self.data.len() + self.globals.len() + 1 {
                self.close_input.funding.is_some()
            } else {
                self.ap[index - self.data.len() - self.globals.len() - 2]
                    .funding
                    .is_some()
            };
            if index < self.data.len() {
                progressed |= self.data[index].progress(
                    runtime,
                    &self.registration,
                    self.closing,
                    producer_exited,
                    actor_failed,
                )?;
            } else if index < self.data.len() + self.globals.len() {
                progressed |= self.globals[index - self.data.len()].progress(
                    runtime,
                    &self.registration,
                    self.closing,
                    producer_exited,
                    actor_failed,
                )?;
            } else if index == self.data.len() + self.globals.len() {
                progressed |= self.bootstrap.progress(
                    runtime,
                    &self.registration,
                    self.closing,
                    producer_exited,
                    actor_failed,
                )?;
            } else if index == self.data.len() + self.globals.len() + 1 {
                progressed |= self.close_input.progress(
                    runtime,
                    &self.registration,
                    self.installed.requested_close.load(Ordering::Acquire),
                    self.closing,
                    producer_exited,
                    actor_failed,
                )?;
            } else {
                let inventory = &mut self.ap[index - self.data.len() - self.globals.len() - 2];
                if inventory.actual_inputs_closed
                    || ((self.closing || actor_failed) && producer_exited && !inventory.issued)
                {
                    progressed |=
                        retire_author(&mut inventory.funding, runtime, &self.registration)?;
                }
            }
            let live_after = if index < self.data.len() {
                self.data[index].funding.is_some()
            } else if index < self.data.len() + self.globals.len() {
                self.globals[index - self.data.len()].funding.is_some()
            } else if index == self.data.len() + self.globals.len() {
                self.bootstrap.funding.is_some()
            } else if index == self.data.len() + self.globals.len() + 1 {
                self.close_input.funding.is_some()
            } else {
                self.ap[index - self.data.len() - self.globals.len() - 2]
                    .funding
                    .is_some()
            };
            if live_before && !live_after {
                self.live_authors -= 1;
            }
        }
        // The summary is incrementally maintained in the same paid row visit.
        self.retired = self.live_authors == 0;
        Ok(InputProgress {
            units,
            progressed,
            more: self.scan_remaining != 0 && !self.retired,
            complete: self.retired,
        })
    }
}
fn reserve_author(funding: &LogicalFundingObligation, inputs: usize) -> SourceFundingObligation {
    SourceFundingObligation::start(
        funding
            .reserve_step(
                WorkKind::Settlement,
                OwnedSlotNeed {
                    inputs,
                    work_headers: 1,
                    ..Default::default()
                },
            )
            .expect("original prepaid logical source Work"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::{FrontendEffect, TestActor};
    use novarocks_control_runtime::{actor::ActorStep, transaction::RuntimeLedger};
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        identity::{ActorId, Tick},
        mailbox::InputEnvelope,
    };
    use novarocks_coordination_contract::{
        ExecutionEffect, IdSeed, MonotonicInstant, RecoveryMode,
    };
    use novarocks_logical_execution_protocol::start::{
        InitialAttempt, LogicalBudgets, LogicalFinishBoundary, LogicalOutputMode, LogicalPolicy,
        ObservationBudgets,
    };
    use novarocks_query_application::api::{InstalledLogicalExecution, LogicalHostFact};
    use novarocks_types::{AttemptId, QueryExecutionId};
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        sync::atomic::AtomicUsize,
        task::Wake,
        time::Duration,
    };

    #[derive(Default)]
    struct Probe(AtomicUsize);
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Receiver(Arc<Mutex<Vec<LogicalInput>>>);
    impl TestActor for Receiver {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            assert!(step.charge(1));
            let FrontendInput::Logical(input) = input else {
                panic!("only original logical inputs expected")
            };
            self.0.lock().unwrap().push(*input);
        }
    }
    fn activation(n: u64, generation: u64) -> AttemptActivationIdentity {
        AttemptActivationIdentity::from_parts(
            71,
            QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(n).unwrap()).unwrap(),
            generation,
        )
    }
    fn start() -> LogicalStart {
        let first = activation(1, 12);
        LogicalStart {
            identity: first.execution().query_id(),
            actor_instance: first.actor(),
            policy: LogicalPolicy {
                recovery: RecoveryMode::NoRecovery,
                effect: ExecutionEffect::None,
                output: LogicalOutputMode::CompletionOnly,
                finish: LogicalFinishBoundary::TaskCompletion,
                retain_diagnostics: false,
                root_carrier: None,
                internal_plan: None,
            },
            budgets: LogicalBudgets {
                max_attempts: NonZeroU32::new(3).unwrap(),
                replacement_reservation_valid_for: None,
                remote_cleanup_timeout: Duration::from_millis(100),
                admission_issues_per_participation: NonZeroUsize::new(2).unwrap(),
                establish_per_participation: NonZeroUsize::new(2).unwrap(),
                abort_per_participation: NonZeroUsize::new(2).unwrap(),
                observation: ObservationBudgets {
                    diagnostics: Duration::from_millis(20),
                    convergence: Duration::from_millis(90),
                },
            },
            first_attempt: InitialAttempt {
                activation: first,
                participations: Box::new([]),
            },
            statement_deadline: None,
            id_seed: IdSeed::new(7, 100),
            started_at: MonotonicInstant::ORIGIN,
        }
    }
    struct Fixture {
        ledger: RuntimeLedger,
        funding: LogicalFundingObligation,
        marker_author: Option<SourceFundingObligation>,
        runtime: ActorRuntime<FrontendActor>,
        inputs: LogicalInputs,
        installed: Arc<LogicalInstalledState>,
        seen: Arc<Mutex<Vec<LogicalInput>>>,
        probe: Arc<Probe>,
    }
    impl Fixture {
        fn new() -> Self {
            let ledger = RuntimeLedger::new(ResourceVector::default());
            let funding = LogicalFundingObligation::reserve(&ledger).unwrap();
            let start = start();
            let geometry = LogicalGeometry::derive(funding.scope().id(), &start).unwrap();
            let runtime = ActorRuntime::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let registration = runtime
                .register_bound(
                    geometry.actor,
                    funding.scope().clone(),
                    FrontendActor::Test(Box::new(Receiver(Arc::clone(&seen)))),
                    geometry.config,
                )
                .unwrap_or_else(|_| panic!("original logical input test actor"));
            let marker_author = reserve_author(&funding, 1);
            let marker = runtime
                .reserve_admitted_input_for(
                    &registration,
                    geometry.source(geometry.purposes + 20).unwrap(),
                    InputClass::Lifecycle,
                    marker_author.work(),
                )
                .unwrap()
                .into_standing_input()
                .unwrap_or_else(|_| panic!("original prepaid failure marker"));
            let probe = Arc::new(Probe::default());
            let wake = Waker::from(Arc::clone(&probe));
            let failure = LogicalInfrastructureFailure::new(marker, wake.clone());
            let installed = LogicalInstalledState::new(wake.clone(), Arc::clone(&failure));
            let inputs = LogicalInputs::reserve(
                &runtime,
                registration,
                &funding,
                &geometry,
                &start,
                Arc::clone(&installed),
                failure,
                wake,
            );
            Self {
                ledger,
                funding,
                marker_author: Some(marker_author),
                runtime,
                inputs,
                installed,
                seen,
                probe,
            }
        }
        fn scan(&mut self, exited: bool) -> InputProgress {
            self.inputs
                .progress(&self.runtime, exited, false, usize::MAX)
                .unwrap()
        }
        fn consume(&self) {
            assert!(self.runtime.poll_one(Tick(1)).unwrap());
        }
        fn finish(mut self) {
            self.inputs.close();
            let progress = self.scan(true);
            assert!(
                progress.complete,
                "all original input headers must really retire"
            );
            assert!(self.inputs.failure.close_marker_after_exit(false).unwrap());
            assert!(
                retire_author(
                    &mut self.marker_author,
                    &self.runtime,
                    &self.inputs.registration
                )
                .unwrap()
            );
            self.funding.scope().close_external().unwrap();
            assert!(
                self.funding
                    .progress(&self.runtime, &self.inputs.registration)
                    .unwrap()
            );
            while let Some(host) = self.runtime.claim_host_actor() {
                host.release(false).unwrap();
            }
            self.runtime
                .remove_actor(self.inputs.registration.actor())
                .unwrap();
            self.ledger.remove_scope(self.funding.scope().id()).unwrap();
            assert!(
                self.ledger.is_empty(),
                "same original source reservations and Scope exit"
            );
        }
    }
    #[tokio::test]
    async fn bootstrap_returns_after_original_fill_and_retires_without_long_cancel_observer_exit() {
        let mut f = Fixture::new();
        let publisher = f.inputs.bootstrap();
        publisher.publish_started().await.unwrap();
        assert!(
            f.inputs
                .bootstrap
                .original
                .lock()
                .position
                .as_ref()
                .unwrap()
                .in_flight()
                .unwrap()
                .is_some()
        );
        assert!(f.inputs.bootstrap.funding.is_some());
        assert!(!f.scan(false).complete);
        assert!(
            f.inputs.bootstrap.funding.is_some(),
            "actual Started retirement remains required"
        );
        f.consume();
        f.scan(false);
        assert!(
            f.inputs.bootstrap.funding.is_none(),
            "short publisher frame ended; long cancellation observer may remain"
        );
        assert!(
            f.inputs.globals[0].funding.is_some(),
            "no fabricated cancellation producer exit"
        );
        assert_eq!(publisher.publish_started().await, Err(LogicalSourceClosed));
        assert!(matches!(
            f.seen.lock().unwrap().as_slice(),
            [LogicalInput::Started]
        ));
        f.finish();
    }
    #[test]
    fn shutdown_waits_for_same_retire_position_and_repeated_close_does_not_wake() {
        let mut f = Fixture::new();
        f.installed.requested_close.store(1, Ordering::Release);
        f.scan(false);
        let original = f.inputs.close_input.position.in_flight().unwrap().unwrap();
        f.installed.requested_close.store(2, Ordering::Release);
        for _ in 0..8 {
            assert!(!f.scan(false).progressed);
            assert_eq!(
                f.inputs.close_input.position.in_flight().unwrap(),
                Some(original)
            );
        }
        f.consume();
        f.scan(false);
        let shutdown = f.inputs.close_input.position.in_flight().unwrap().unwrap();
        assert_ne!(
            shutdown, original,
            "a new occurrence uses the same retired standing role"
        );
        f.consume();
        f.scan(false);
        let before = f.probe.0.load(Ordering::SeqCst);
        for _ in 0..16 {
            let progress = f.scan(false);
            assert!(!progress.progressed);
            assert!(!progress.more);
        }
        assert_eq!(
            f.probe.0.load(Ordering::SeqCst),
            before,
            "no wake for unchanged Retire/Shutdown"
        );
        assert!(matches!(
            f.seen.lock().unwrap().as_slice(),
            [
                LogicalInput::RegistryClose(RegistryClose::Retire),
                LogicalInput::RegistryClose(RegistryClose::Shutdown)
            ]
        ));
        f.finish();
    }
    #[tokio::test]
    async fn exact_data_binding_is_idempotent_and_weak_wrong_kind_after_close_cannot_reopen_failure_marker()
     {
        let mut f = Fixture::new();
        let first = f.inputs.bind_data_activation(activation(1, 12)).unwrap();
        let repeated = f.inputs.bind_data_activation(activation(1, 12)).unwrap();
        assert!(Arc::ptr_eq(&first, &repeated));
        assert!(matches!(
            f.inputs.bind_data_activation(activation(1, 13)),
            Err(LogicalSourceRefusal::ForeignActivation)
        ));
        let second = f.inputs.bind_data_activation(activation(2, 14)).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        let mut weak = f.installed.source(LogicalSourceKind::Cancellation).unwrap();
        f.inputs.close();
        assert!(
            !f.scan(false).complete,
            "closed gate does not exit original producers"
        );
        assert_eq!(
            weak.send(LogicalHostFact::ConsumerClosed).await,
            Err(LogicalSourceClosed)
        );
        assert!(
            !f.inputs.failure.failed(),
            "closed wrong-kind handle cannot publish a late owner failure"
        );
        // Keeping original inert sender cells and weak caller views is not Work.
        f.finish();
        assert_eq!(
            weak.send(LogicalHostFact::ConsumerClosed).await,
            Err(LogicalSourceClosed)
        );
        drop((first, repeated, second));
    }
    #[test]
    fn budgeted_scan_retires_unused_inventory_but_keeps_exposed_ap_work_until_original_roles_close()
    {
        let mut f = Fixture::new();
        assert_eq!(f.inputs.ap_count(), 3);
        let registration = f.inputs.registration.clone();
        let author = f.inputs.ap_author(0).unwrap();
        let mut roles = Vec::new();
        for class in [
            InputClass::Lifecycle,
            InputClass::Ordinary,
            InputClass::Ordinary,
        ] {
            roles.push(
                f.runtime
                    .reserve_admitted_input_for(
                        &registration,
                        ActorId::new(91).unwrap(),
                        class,
                        author,
                    )
                    .unwrap()
                    .into_standing_input()
                    .unwrap_or_else(|_| panic!("same original prepaid AP role")),
            );
        }
        f.inputs.close();
        let rows = f.inputs.data.len() + f.inputs.globals.len() + 2 + f.inputs.ap.len();
        for row in 0..rows {
            let progress = f.inputs.progress(&f.runtime, true, false, 1).unwrap();
            assert_eq!(progress.units, 1);
            assert_eq!(progress.more, row + 1 < rows);
            assert!(!progress.complete);
        }
        assert_eq!(
            f.inputs.live_authors, 1,
            "only the exposed original AP header remains"
        );
        assert!(
            f.inputs.ap[1..]
                .iter()
                .all(|unused| unused.funding.is_none())
        );
        for role in &mut roles {
            role.close_after_exit().unwrap();
        }
        f.inputs.ap_inputs_closed_after_producer_exit(0).unwrap();
        assert!(f.scan(true).complete);
        assert!(f.inputs.is_retired());
        f.finish();
    }
}
