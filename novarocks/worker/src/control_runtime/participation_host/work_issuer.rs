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

//! Domain-scoped issuance and the original Participation's exit custody.
//! Foreign owners receive existing ExitObligation values, never a general
//! WorkChildAuthority or a replacement work/convergence counter.

use crate::control_runtime::{
    actors::WorkerActor, participation_ready::ParticipationReadyNotice,
    reported_work_admission::adopt_reported_work, work_labels,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    exit_work::PanicSink,
    header_readiness::{HeaderReadyError, HeaderReadyNotice, HeaderReadyRegistration},
    obligation::{WorkChildAuthority, WorkObligation, WorkRetirementObserver},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    completion::ConsumerKind,
    identity::{ConsumerId, ScopedWorkRef, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    ParticipationRef,
    failure::{FailureClass, FailureFact, FailureProjection},
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const OBSERVER: ConsumerId = ConsumerId::new(2).unwrap();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipationWorkError {
    Runtime(RuntimeError),
    Exit(ExitCustodyError),
    Registration(ActorTurnError),
    Readiness(HeaderReadyError),
    Panicked,
    Unresolved,
}
impl std::fmt::Display for ParticipationWorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Participation work supervision failed: {self:?}")
    }
}
impl std::error::Error for ParticipationWorkError {}
impl ParticipationWorkError {
    pub fn is_stopped(self) -> bool {
        matches!(
            self,
            Self::Runtime(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::Stopped
            ))
        )
    }
}
impl FailureProjection for ParticipationWorkError {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::ParticipationWorkUnresolved.reason(),
        )
    }
}
impl From<RuntimeError> for ParticipationWorkError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}
impl From<ExitCustodyError> for ParticipationWorkError {
    fn from(value: ExitCustodyError) -> Self {
        Self::Exit(value)
    }
}
impl From<ActorTurnError> for ParticipationWorkError {
    fn from(value: ActorTurnError) -> Self {
        Self::Registration(value)
    }
}
impl From<HeaderReadyError> for ParticipationWorkError {
    fn from(value: HeaderReadyError) -> Self {
        Self::Readiness(value)
    }
}

#[derive(Clone)]
struct Parent {
    work: ScopedWorkRef,
    authority: WorkChildAuthority,
}
impl Parent {
    fn from_work(work: &WorkObligation) -> Result<Self, RuntimeError> {
        Ok(Self {
            work: work.reference(),
            authority: work.child_authority()?,
        })
    }
}
struct Entry {
    parent: WorkId,
    custody: ExitCustody,
    report: ReportSlot,
    retirement: WorkRetirementObserver,
    header: HeaderReadyRegistration,
    panic: PanicSink,
    observer_settled: bool,
}
struct State {
    entries: BTreeMap<WorkId, Entry>,
    retained: Option<Arc<Inner>>,
    scan_requested: bool,
    scan_upper: Option<WorkId>,
    scan_after: Option<WorkId>,
}
struct Wake {
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    notice: ParticipationReadyNotice,
}
struct Inner {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    participation: ParticipationRef,
    lifetime: Parent,
    wake: Option<Wake>,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
}

#[derive(Clone)]
pub(crate) struct ParticipationWorkRegistry(Arc<Inner>);
impl ParticipationWorkRegistry {
    pub(super) fn new(
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        participation: ParticipationRef,
        lifetime: &WorkObligation,
        runtime: ActorRuntime<WorkerActor>,
        registration: ActorRegistration,
        notice: ParticipationReadyNotice,
        health: Arc<ExecutionHealth>,
    ) -> Result<Self, ParticipationWorkError> {
        if !scope.same_scope(lifetime.scope())
            || !registration.matches_scope(&scope)
            || notice.participation() != participation
            || !notice.matches_registration(&runtime, &registration)
        {
            return Err(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::InvalidTransition,
            )
            .into());
        }
        runtime.validate_registration(&registration)?;
        Ok(Self(Arc::new(Inner {
            ledger,
            scope,
            participation,
            lifetime: Parent::from_work(lifetime)?,
            wake: Some(Wake {
                runtime,
                registration,
                notice,
            }),
            health,
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                retained: None,
                scan_requested: false,
                scan_upper: None,
                scan_after: None,
            }),
        })))
    }
    pub(crate) fn issue_instance(
        &self,
    ) -> Result<ParticipationInstanceGrant, ParticipationWorkError> {
        let (work, parent, _) =
            self.issue(&self.0.lifetime, work_labels::PARTICIPATION_INSTANCE)?;
        Ok(ParticipationInstanceGrant {
            work: Some(work),
            issuer: InstanceWorkIssuer {
                registry: self.clone(),
                parent,
            },
        })
    }
    fn issue(
        &self,
        parent: &Parent,
        label: &'static str,
    ) -> Result<(ExitObligation, Parent, PanicSink), ParticipationWorkError> {
        parent.authority.verify_live()?;
        let generation = self.0.scope.gates()?.generation;
        let mut state = self
            .0
            .state
            .lock()
            .expect("Participation work registry lock");
        let mut guard = self.0.scope.reserve_next(
            generation,
            ReservationResources::new(
                Default::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Business,
            WorkAdmission::Derived(parent.work.work),
        )?;
        let report = ReportSlot::reserve_next(
            &mut guard,
            generation,
            1,
            [(OBSERVER, ConsumerKind::Observer)],
        )?;
        let (work, capacity) = match guard.bind(WorkKind::Business) {
            Ok(original) => original,
            Err(error) => {
                report.revoke_unused()?;
                report.body_exited()?;
                report.captures_exited()?;
                assert!(report.try_retire()?);
                return Err(error.into());
            }
        };
        work.set_diagnostic_label(label)
            .expect("fresh original Work gets one diagnostic label");
        let child_parent = Parent::from_work(&work).expect("fresh original Work is still live");
        let retirement = work.retirement_observer();
        let mut admitted = adopt_reported_work(work, capacity, report.clone());
        let header = admitted
            .header_readiness_registration()
            .expect("fresh original Work has one readiness source");
        let id = admitted.work().id();
        let panic = PanicSink::new();
        let (custody, obligation) = ExitCustody::from_admitted(admitted);
        assert!(
            state
                .entries
                .insert(
                    id,
                    Entry {
                        parent: parent.work.work,
                        custody,
                        report,
                        retirement,
                        header,
                        panic: panic.clone(),
                        observer_settled: false,
                    }
                )
                .is_none()
        );
        if state.retained.is_none() {
            state.retained = Some(self.0.clone());
        }
        state.scan_requested = true;
        let arm = match &self.0.wake {
            Some(wake) => {
                let entry = state
                    .entries
                    .get_mut(&id)
                    .expect("original admitted custody");
                entry
                    .header
                    .arm(&wake.runtime, &wake.registration, &entry.retirement)
                    .map(|_| ())
            }
            None => Ok(()),
        };
        drop(state);
        if let Err(error) = arm {
            // A failed notification binding cannot discard bound originals or
            // manufacture a never-started exit. Retain the actual Unknown.
            drop(obligation);
            self.request_scan();
            return Err(error.into());
        }
        self.notify();
        Ok((obligation, child_parent, panic))
    }
    pub(crate) fn retire_ready(&self, budget: usize) -> Result<usize, ParticipationWorkError> {
        if budget == 0 {
            return Ok(0);
        }
        let mut state = self
            .0
            .state
            .lock()
            .expect("Participation work registry lock");
        if state.scan_upper.is_none() {
            if !state.scan_requested {
                return Ok(0);
            }
            state.scan_requested = false;
            state.scan_upper = state.entries.last_key_value().map(|(&work, _)| work);
            state.scan_after = None;
        }
        let Some(upper) = state.scan_upper else {
            return Ok(0);
        };
        // One event requests one finite sweep. New entries/events never reset
        // this cursor or extend its frozen upper bound while it is running.
        let lower = state
            .scan_after
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let keys: Vec<_> = state
            .entries
            .range((lower, std::ops::Bound::Included(upper)))
            .take(budget)
            .map(|(&work, _)| work)
            .collect();
        let mut retired = Vec::new();
        for key in keys {
            state.scan_after = Some(key);
            let has_child = state.entries.values().any(|entry| entry.parent == key);
            let entry = state
                .entries
                .get_mut(&key)
                .expect("same original registry entry");
            let phase = entry.custody.phase();
            if phase == ExitCustodyPhase::Unresolved {
                self.0.health.report_unresolved(
                    ExecutionExitOrigin::Process,
                    ParticipationWorkError::Unresolved.failure_fact(),
                );
            }
            if entry.panic.has_panic() {
                state.scan_upper = None;
                state.scan_after = None;
                return Err(ParticipationWorkError::Panicked);
            }
            if phase == ExitCustodyPhase::Unresolved {
                state.scan_upper = None;
                state.scan_after = None;
                return Err(ParticipationWorkError::Unresolved);
            }
            if has_child {
                continue;
            }
            if phase == ExitCustodyPhase::Exited {
                if !entry.observer_settled {
                    // A never-started report is RevokedUnused, not Filled.
                    // It is already retirement-ready and must not be consumed.
                    if !entry.report.retirement_ready()? {
                        entry.report.consume(OBSERVER)?;
                    }
                    entry.observer_settled = true;
                }
                if !entry.custody.try_retire()? {
                    continue;
                }
            } else if phase != ExitCustodyPhase::Retired {
                continue;
            }
            if !self.0.ledger.reap_observed_header(&entry.retirement)? {
                if let Some(wake) = &self.0.wake {
                    entry
                        .header
                        .arm(&wake.runtime, &wake.registration, &entry.retirement)?;
                }
                continue;
            }
            entry.header.cancel();
            retired.push(
                state
                    .entries
                    .remove(&key)
                    .expect("actually retired original entry"),
            );
            // A real child retirement can unblock a parent already visited in
            // this sweep. Request one following sweep, not an endless retry.
            state.scan_requested = true;
        }
        let lower = state
            .scan_after
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        if state
            .entries
            .range((lower, std::ops::Bound::Included(upper)))
            .next()
            .is_none()
        {
            state.scan_upper = None;
            state.scan_after = None;
        }
        let retention = if state.entries.is_empty() {
            state.scan_requested = false;
            state.scan_upper = None;
            state.scan_after = None;
            state.retained.take()
        } else {
            None
        };
        let more_scan = state.scan_upper.is_some() || state.scan_requested;
        drop(state);
        let count = retired.len();
        drop(retired);
        drop(retention);
        if count != 0 || more_scan {
            self.notify();
        }
        Ok(count)
    }
    pub(super) fn note_header_ready(&self, notice: &HeaderReadyNotice) -> bool {
        let mut state = self
            .0
            .state
            .lock()
            .expect("Participation work registry lock");
        if !state
            .entries
            .get(&notice.work().work)
            .is_some_and(|entry| entry.header.matches(notice))
        {
            return false;
        }
        state.scan_requested = true;
        drop(state);
        self.notify();
        true
    }
    fn request_scan(&self) {
        self.0
            .state
            .lock()
            .expect("Participation work registry lock")
            .scan_requested = true;
        self.notify();
    }
    fn notify(&self) {
        if let Some(wake) = &self.0.wake {
            wake.notice.notify();
        }
    }
}

/// Only Worker can issue this bundle for its original Participation. It moves
/// the existing ExitObligation together with the instance's narrow child issuer.
pub struct ParticipationInstanceGrant {
    work: Option<ExitObligation>,
    issuer: InstanceWorkIssuer,
}
impl ParticipationInstanceGrant {
    pub fn participation(&self) -> ParticipationRef {
        self.issuer.registry.0.participation
    }
    pub fn into_parts(mut self) -> (ExitObligation, InstanceWorkIssuer) {
        (
            self.work.take().expect("one original instance obligation"),
            self.issuer.clone(),
        )
    }
}
impl Drop for ParticipationInstanceGrant {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            work.revoke_unused_and_exit()
                .expect("unused original instance grant exits");
            self.issuer.registry.request_scan();
        }
    }
}

/// This issuer can only derive RF sender work under its original live instance.
/// It exposes no general child authority, caller-supplied identity or capacity.
#[derive(Clone)]
pub struct InstanceWorkIssuer {
    registry: ParticipationWorkRegistry,
    parent: Parent,
}
impl InstanceWorkIssuer {
    pub fn runtime_filter_sender(&self) -> Result<RuntimeFilterSenderWork, ParticipationWorkError> {
        let (work, _, panic) = self
            .registry
            .issue(&self.parent, work_labels::RUNTIME_FILTER_SENDER)?;
        Ok(RuntimeFilterSenderWork {
            work: Some(work),
            panic,
            wake: ParticipationWorkWake {
                registry: self.registry.clone(),
            },
        })
    }
}
/// An original RF sender obligation and its supervising owner's panic/wake ports.
pub struct RuntimeFilterSenderWork {
    work: Option<ExitObligation>,
    panic: PanicSink,
    wake: ParticipationWorkWake,
}
impl RuntimeFilterSenderWork {
    pub fn into_parts(mut self) -> (ExitObligation, PanicSink, ParticipationWorkWake) {
        (
            self.work.take().expect("one original sender obligation"),
            self.panic.clone(),
            self.wake.clone(),
        )
    }
}
impl Drop for RuntimeFilterSenderWork {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            work.revoke_unused_and_exit()
                .expect("unused original sender grant exits");
            self.wake.notify();
        }
    }
}
/// Metadata notification only, including destructor failures that cannot make
/// a header ready. It grants no exit, cancellation or retirement authority.
#[derive(Clone)]
pub struct ParticipationWorkWake {
    registry: ParticipationWorkRegistry,
}
impl ParticipationWorkWake {
    pub fn notify(&self) {
        self.registry.request_scan();
    }
}

#[cfg(any(test, novarocks_dev_controls))]
mod tests;
#[cfg(any(test, novarocks_dev_controls))]
pub use tests::ParticipationWorkTestOwner;
