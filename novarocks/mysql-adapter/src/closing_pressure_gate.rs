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

//! Private pressure instrumentation; the original listener and statement retain authority.
//! Install ClosingHeld inside the existing original Closing finish timeout only.
pub(crate) mod relay;

use novarocks_query_application::{
    client_connection::ClientConnectionToken, protocol_delivery::ClosingDelivery,
    session_control::StatementToken,
};
use novarocks_types::FrontendProcessId;
use novarocks_workload_control::WorkError;
use opensrv_mysql::{FramingCursor, WritePhase};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Instant,
};
use tokio::{io::AsyncWrite, sync::Notify};

pub(crate) const TARGETS: usize = 65;
pub(crate) const CLOSING_TARGETS: usize = 64;
pub(crate) const ROW_CUT: u64 = 1_048_575;
const POLL_BYTES: usize = 65_540;
const IO_SLICES: usize = 32;
/// Exact frozen original x workload; this is not a new query surface.
pub(crate) const ORIGINAL_SQL_SHA256: [u8; 32] = [
    0x3c, 0xf4, 0x35, 0x3c, 0xe6, 0xc2, 0x89, 0x49, 0xb0, 0x9b, 0x6c, 0xb3, 0xed, 0x0d, 0xc2, 0x24,
    0x47, 0xc5, 0x00, 0xb0, 0xcb, 0x18, 0x7f, 0x58, 0xe9, 0xe4, 0x91, 0x8d, 0xaa, 0xd7, 0x8f, 0xd2,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Failure {
    Identity = 1,
    Transition,
    Deadline,
    Length,
    Counter,
    Poison,
    InnerIo,
    Panic,
}
fn failure(code: u8) -> Option<Failure> {
    match code {
        0 => None,
        1 => Some(Failure::Identity),
        2 => Some(Failure::Transition),
        3 => Some(Failure::Deadline),
        4 => Some(Failure::Length),
        5 => Some(Failure::Counter),
        6 => Some(Failure::Poison),
        7 => Some(Failure::InnerIo),
        8 => Some(Failure::Panic),
        _ => Some(Failure::Counter),
    }
}
fn error(reason: Failure) -> io::Error {
    io::Error::new(
        if reason == Failure::Deadline {
            io::ErrorKind::TimedOut
        } else {
            io::ErrorKind::InvalidData
        },
        match reason {
            Failure::Identity => "pressure original identity mismatch",
            Failure::Transition => "pressure original transition refused",
            Failure::Deadline => "pressure original absolute clock expired",
            Failure::Length => "pressure original write length invalid",
            Failure::Counter => "pressure scalar counter overflow",
            Failure::Poison => "pressure scalar owner lock poisoned",
            Failure::InnerIo => "pressure original inner IO already failed",
            Failure::Panic => "pressure original poll panicked",
        },
    )
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Unarmed,
    Armed,
    Bound,
    Rows,
    CancelObserved,
    ClosingHeld,
    Released,
    CapacityRefused,
}
#[derive(Clone, Copy, Debug)]
pub struct ArmInput {
    pub handshake_connection_id: u32,
    pub original_sql_sha256: [u8; 32],
}
#[derive(Clone, Copy, Debug)]
pub struct SlotSnapshot {
    pub slot: usize,
    pub phase: Phase,
    pub failure: Option<Failure>,
    pub handshake_connection_id: Option<u32>,
    pub original_sql_sha256: Option<[u8; 32]>,
    pub connection: Option<ClientConnectionToken>,
    pub statement: Option<StatementToken>,
    pub cut_bytes: u64,
    pub accepted_prefix_bytes: u64,
    pub accepted_prefix_sha256: [u8; 32],
    pub rows_blocked: bool,
    pub closing_write_blocked: bool,
    pub closing_flush_blocked: bool,
    /// Actual Gate polls nested inside the original Closing finish future for this Core + slot.
    pub paired_closing_polls: u64,
    pub real_closing_observed: bool,
    pub baseline: Option<FramingCursor>,
    pub cancel_receipt: Option<FramingCursor>,
    pub scalar_inner_polls: u64,
    pub vectored_inner_polls: u64,
    pub flush_inner_polls: u64,
    pub successful_inner_writes: u64,
    pub writer_attached: bool,
    /// Actual concrete W destructor returned. Never a Closing-grant/last-alias assertion.
    pub writer_destructor_returned: bool,
    pub physical_shutdown_completed: bool,
    /// A moved original WorkError::Capacity source, not a fabricated EOF or grant.
    pub original_capacity_source_retained: bool,
}
struct State {
    facts: SlotSnapshot,
    digest: Sha256,
    refusal: Option<WorkError>,
    closing_poll_active: bool,
    capacity_eof_minted: bool,
}
struct Slot {
    state: Mutex<State>,
    waiter: Mutex<Option<Waker>>,
    inner_poll: AtomicBool,
}
struct Core {
    action_observation:
        Mutex<Option<Arc<crate::test_fixture_authorization::FixtureActionObservation>>>,
    frontend: FrontendProcessId,
    deadline: Instant,
    arm_consumed: AtomicBool,
    stopped: AtomicBool,
    first_failure: AtomicU8,
    selection: Mutex<()>,
    slots: Box<[Slot; TARGETS]>,
    changed: Notify,
    workload: Option<novarocks_workload_control::WorkloadObservationHandle>,
}
/// Listener-local diagnostic ownership only. No payload/credit/runtime capability.
pub(crate) struct PressureOwner {
    core: Arc<Core>,
}
/// Sole control owner. Not Clone; Drop wakes existing original writers for teardown.
pub(crate) struct PressureController {
    core: Arc<Core>,
}
/// A copy-free hook produced only by the original successful statement bind.
pub(crate) struct PressureScope {
    core: Arc<Core>,
    slot: usize,
}

impl Core {
    fn wake_all(&self) {
        for slot in self.slots.iter() {
            let waiter = slot
                .waiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(waiter) = waiter {
                waiter.wake();
            }
        }
        self.changed.notify_waiters();
    }
    fn fail(&self, reason: Failure) -> io::Error {
        let _ = self.first_failure.compare_exchange(
            0,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.wake_all();
        error(failure(self.first_failure.load(Ordering::Acquire)).unwrap_or(reason))
    }
    fn check(&self) -> io::Result<()> {
        if let Some(reason) = failure(self.first_failure.load(Ordering::Acquire)) {
            return Err(error(reason));
        }
        if self.stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "pressure owner stopped",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(self.fail(Failure::Deadline));
        }
        Ok(())
    }
    fn state(&self, index: usize) -> io::Result<MutexGuard<'_, State>> {
        let slot = self
            .slots
            .get(index)
            .ok_or_else(|| self.fail(Failure::Length))?;
        slot.state.lock().map_err(|_| self.fail(Failure::Poison))
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.wake_all();
    }
    fn snapshot(&self, index: usize) -> io::Result<SlotSnapshot> {
        let state = self.state(index)?;
        let mut facts = state.facts;
        facts.failure = failure(self.first_failure.load(Ordering::Acquire));
        facts.accepted_prefix_sha256 = state.digest.clone().finalize().into();
        facts.original_capacity_source_retained = state.refusal.is_some();
        Ok(facts)
    }
}
impl PressureOwner {
    pub(crate) fn install_action_observation(
        &self,
        observation: Arc<crate::test_fixture_authorization::FixtureActionObservation>,
    ) {
        *self
            .core
            .action_observation
            .lock()
            .expect("original pressure observation lock") = Some(observation);
    }

    pub(crate) fn new(
        actual_frontend: FrontendProcessId,
        original_deadline: Instant,
    ) -> io::Result<(Self, PressureController)> {
        Self::with_workload(actual_frontend, original_deadline, None)
    }
    pub(crate) fn with_workload(
        actual_frontend: FrontendProcessId,
        original_deadline: Instant,
        workload: Option<novarocks_workload_control::WorkloadObservationHandle>,
    ) -> io::Result<(Self, PressureController)> {
        if Instant::now() >= original_deadline {
            return Err(error(Failure::Deadline));
        }
        let slots = Box::new(std::array::from_fn(|slot| Slot {
            state: Mutex::new(State {
                facts: SlotSnapshot {
                    slot,
                    phase: Phase::Unarmed,
                    failure: None,
                    handshake_connection_id: None,
                    original_sql_sha256: None,
                    connection: None,
                    statement: None,
                    cut_bytes: ROW_CUT,
                    accepted_prefix_bytes: 0,
                    accepted_prefix_sha256: Sha256::digest([]).into(),
                    rows_blocked: false,
                    closing_write_blocked: false,
                    closing_flush_blocked: false,
                    paired_closing_polls: 0,
                    real_closing_observed: false,
                    baseline: None,
                    cancel_receipt: None,
                    scalar_inner_polls: 0,
                    vectored_inner_polls: 0,
                    flush_inner_polls: 0,
                    successful_inner_writes: 0,
                    writer_attached: false,
                    writer_destructor_returned: false,
                    physical_shutdown_completed: false,
                    original_capacity_source_retained: false,
                },
                digest: Sha256::new(),
                refusal: None,
                closing_poll_active: false,
                capacity_eof_minted: false,
            }),
            waiter: Mutex::new(None),
            inner_poll: AtomicBool::new(false),
        }));
        let core = Arc::new(Core {
            action_observation: Mutex::new(None),
            frontend: actual_frontend,
            deadline: original_deadline,
            arm_consumed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            first_failure: AtomicU8::new(0),
            selection: Mutex::new(()),
            slots,
            changed: Notify::new(),
            workload,
        });
        Ok((
            Self {
                core: Arc::clone(&core),
            },
            PressureController { core },
        ))
    }
    pub(crate) fn is_selected_connection(&self, connection: ClientConnectionToken) -> bool {
        for index in 0..TARGETS {
            match self.core.state(index) {
                Ok(state)
                    if state.facts.handshake_connection_id == Some(connection.connection_id()) =>
                {
                    return true;
                }
                Ok(_) => {}
                // A poisoned selection cannot authorize a protocol shortcut.
                Err(_) => return true,
            }
        }
        false
    }
    pub(crate) fn reject_unsupported_batch(
        &self,
        connection: ClientConnectionToken,
    ) -> io::Result<()> {
        if self.is_selected_connection(connection) {
            return Err(self.core.fail(Failure::Transition));
        }
        Ok(())
    }
    pub(crate) fn validate_streaming_owner(
        &self,
        result: &novarocks_query_application::protocol_delivery::StreamingStatementResult,
    ) -> io::Result<()> {
        if !self
            .core
            .workload
            .as_ref()
            .is_some_and(|observer| result.is_observed_by(observer))
        {
            return Err(self.core.fail(Failure::Identity));
        }
        self.core.check()
    }
    pub(crate) fn fail(&self, reason: Failure) {
        let _ = self.core.fail(reason);
    }
    /// Unrelated original connections remain raw; selected conflicts fail the whole pressure attempt.
    pub(crate) fn bind_statement(
        &self,
        connection: ClientConnectionToken,
        expected_session: novarocks_query_application::session_control::SessionToken,
        statement: StatementToken,
        actual_sql_sha256: [u8; 32],
    ) -> io::Result<Option<PressureScope>> {
        let _selection = self
            .core
            .selection
            .lock()
            .map_err(|_| self.core.fail(Failure::Poison))?;
        let mut selected = None;
        for index in 0..TARGETS {
            if self.core.state(index)?.facts.handshake_connection_id
                == Some(connection.connection_id())
            {
                selected = Some(index);
                break;
            }
        }
        let Some(index) = selected else {
            return Ok(None);
        };
        self.core.check()?;
        let mut state = self.core.state(index)?;
        // Only a later, different statement on the exact original recovered socket is raw.
        // This grants no query/control/Closing capability and cannot rebind the selected target.
        if state.facts.phase == Phase::Released
            && state.facts.connection == Some(connection)
            && state.facts.statement.is_some_and(|original| {
                original.session() == statement.session()
                    && statement.generation() > original.generation()
            })
            && actual_sql_sha256 != ORIGINAL_SQL_SHA256
        {
            return Ok(None);
        }
        if state.facts.phase != Phase::Armed {
            return Err(self.core.fail(Failure::Transition));
        }
        if state.facts.original_sql_sha256 != Some(actual_sql_sha256)
            || statement.session() != expected_session
            || statement.generation() == 0
        {
            return Err(self.core.fail(Failure::Identity));
        }
        state.facts.connection = Some(connection);
        state.facts.statement = Some(statement);
        state.facts.phase = Phase::Bound;
        drop(state);
        self.core.check()?;
        Ok(Some(PressureScope {
            core: Arc::clone(&self.core),
            slot: index,
        }))
    }
}
impl PressureController {
    /// Exactly 65 real handshake CIDs; the old Arm/wire protocol is not used.
    pub(crate) fn arm_targets(
        &mut self,
        actual_frontend: FrontendProcessId,
        targets: [ArmInput; TARGETS],
    ) -> io::Result<()> {
        self.core.check()?;
        let _selection = self
            .core
            .selection
            .lock()
            .map_err(|_| self.core.fail(Failure::Poison))?;
        if self.core.arm_consumed.swap(true, Ordering::AcqRel) {
            return Err(self.core.fail(Failure::Transition));
        }
        if actual_frontend != self.core.frontend {
            return Err(self.core.fail(Failure::Identity));
        }
        for (index, target) in targets.iter().enumerate() {
            if target.handshake_connection_id == 0
                || target.original_sql_sha256 != ORIGINAL_SQL_SHA256
                || targets[..index]
                    .iter()
                    .any(|other| other.handshake_connection_id == target.handshake_connection_id)
            {
                return Err(self.core.fail(Failure::Identity));
            }
        }
        for (index, target) in targets.iter().enumerate() {
            let mut state = self.core.state(index)?;
            if state.facts.phase != Phase::Unarmed {
                return Err(self.core.fail(Failure::Transition));
            }
            state.facts.phase = Phase::Armed;
            state.facts.handshake_connection_id = Some(target.handshake_connection_id);
            state.facts.original_sql_sha256 = Some(target.original_sql_sha256);
        }
        self.core.check()
    }
    /// Scalar-only single-slot response: no array-sized body or borrowed original IO.
    pub(crate) fn snapshot(&self, slot: usize) -> io::Result<SlotSnapshot> {
        self.core.snapshot(slot)
    }
    /// No Release can race this sole controller's synchronous held bracket.
    /// Writers still run their original clocks; any exit/failure invalidates it.
    pub(crate) fn joint_closing_snapshot(
        &self,
        require_refusal: bool,
    ) -> io::Result<crate::closing_pressure_fixture::ClosingPressureJointSnapshot> {
        let check_held = || -> io::Result<u64> {
            self.core.check()?;
            let mut minimum_polls = u64::MAX;
            for index in 0..CLOSING_TARGETS {
                let state = self.core.state(index)?;
                if state.facts.phase != Phase::ClosingHeld
                    || !state.facts.real_closing_observed
                    || !state.facts.writer_attached
                    || state.facts.writer_destructor_returned
                    || state.facts.cancel_receipt.is_none()
                    || state.facts.paired_closing_polls == 0
                    || (!state.facts.closing_write_blocked && !state.facts.closing_flush_blocked)
                {
                    return Err(self.core.fail(Failure::Transition));
                }
                minimum_polls = minimum_polls.min(state.facts.paired_closing_polls);
            }
            if require_refusal {
                let state = self.core.state(CLOSING_TARGETS)?;
                if state.facts.phase != Phase::CapacityRefused
                    || state.facts.cancel_receipt.is_none()
                    || !state.capacity_eof_minted
                    || !matches!(state.refusal, Some(WorkError::Capacity(_)))
                {
                    return Err(self.core.fail(Failure::Transition));
                }
            }
            self.core.check()?;
            Ok(minimum_polls)
        };
        let before = check_held()?;
        let workload = self
            .core
            .workload
            .as_ref()
            .ok_or_else(|| self.core.fail(Failure::Identity))?;
        let capacity = workload.result_capacity_snapshot();
        if capacity.held_positions[3] != CLOSING_TARGETS {
            return Err(self.core.fail(Failure::Identity));
        }
        let after = check_held()?;
        Ok(
            crate::closing_pressure_fixture::ClosingPressureJointSnapshot {
                capacity,
                closing_targets: CLOSING_TARGETS,
                minimum_original_closing_polls: before.min(after),
                original_capacity_refusal: require_refusal,
            },
        )
    }
    pub(crate) fn release(&mut self, slot: usize) -> io::Result<()> {
        self.core.check()?;
        if slot >= CLOSING_TARGETS {
            return Err(self.core.fail(Failure::Identity));
        }
        let mut state = self.core.state(slot)?;
        if state.facts.phase != Phase::ClosingHeld
            || state.facts.writer_destructor_returned
            || (!state.facts.closing_write_blocked && !state.facts.closing_flush_blocked)
            || (state.facts.real_closing_observed && state.facts.paired_closing_polls == 0)
        {
            return Err(self.core.fail(Failure::Transition));
        }
        state.facts.phase = Phase::Released;
        drop(state);
        self.core.wake_all();
        self.core.check()
    }
    pub(crate) fn fail(&self, reason: Failure) {
        let _ = self.core.fail(reason);
    }
    pub(crate) async fn wait_rows(&self, first: usize, count: usize) -> io::Result<()> {
        if count == 0 || first.checked_add(count).is_none_or(|end| end > TARGETS) {
            return Err(self.core.fail(Failure::Length));
        }
        self.wait_until(|| {
            let mut ready = true;
            for slot in first..first + count {
                let facts = self.core.snapshot(slot)?;
                if matches!(
                    facts.phase,
                    Phase::Unarmed | Phase::Released | Phase::CapacityRefused
                ) || facts.writer_destructor_returned
                {
                    return Err(self.core.fail(Failure::Transition));
                }
                ready &= facts.phase == Phase::Rows && facts.rows_blocked;
            }
            Ok(ready)
        })
        .await
    }
    pub(crate) async fn wait_joint_closing(
        &self,
        require_refusal: bool,
    ) -> io::Result<crate::closing_pressure_fixture::ClosingPressureJointSnapshot> {
        self.wait_until(|| {
            let mut ready = true;
            for slot in 0..CLOSING_TARGETS {
                let facts = self.core.snapshot(slot)?;
                if matches!(
                    facts.phase,
                    Phase::Unarmed | Phase::Released | Phase::CapacityRefused
                ) || facts.writer_destructor_returned
                {
                    return Err(self.core.fail(Failure::Transition));
                }
                ready &= facts.phase == Phase::ClosingHeld
                    && facts.real_closing_observed
                    && facts.writer_attached
                    && facts.cancel_receipt.is_some()
                    && facts.paired_closing_polls > 0
                    && (facts.closing_write_blocked || facts.closing_flush_blocked);
            }
            if require_refusal {
                let state = self.core.state(CLOSING_TARGETS)?;
                if state.facts.phase == Phase::Unarmed || state.facts.phase == Phase::Released {
                    return Err(self.core.fail(Failure::Transition));
                }
                ready &= state.facts.phase == Phase::CapacityRefused
                    && state.facts.cancel_receipt.is_some()
                    && state.capacity_eof_minted
                    && matches!(state.refusal, Some(WorkError::Capacity(_)));
            }
            Ok(ready)
        })
        .await?;
        self.joint_closing_snapshot(require_refusal)
    }
    fn original_writers_exited(&self) -> io::Result<bool> {
        self.core.check()?;
        if !self.core.arm_consumed.load(Ordering::Acquire) {
            return Err(self.core.fail(Failure::Transition));
        }
        let mut ready = true;
        for slot in 0..TARGETS {
            let state = self.core.state(slot)?;
            ready &= state.facts.writer_attached
                && state.facts.writer_destructor_returned
                && state.facts.cancel_receipt.is_some()
                && if slot < CLOSING_TARGETS {
                    state.facts.phase == Phase::Released
                } else {
                    state.facts.phase == Phase::CapacityRefused
                        && state.capacity_eof_minted
                        && matches!(state.refusal, Some(WorkError::Capacity(_)))
                };
        }
        self.core.check()?;
        Ok(ready)
    }
    pub(crate) async fn wait_original_writers_exited(&self) -> io::Result<()> {
        self.wait_until(|| self.original_writers_exited()).await
    }
    pub(crate) fn stop_after_original_writers_exited(&mut self) -> io::Result<()> {
        if !self.original_writers_exited()? {
            return Err(self.core.fail(Failure::Transition));
        }
        self.core.stop();
        Ok(())
    }
    async fn wait_until(&self, mut ready: impl FnMut() -> io::Result<bool>) -> io::Result<()> {
        let wait = async {
            loop {
                let changed = self.core.changed.notified();
                tokio::pin!(changed);
                // Register before reading the original facts: no lost writer wakeup.
                changed.as_mut().enable();
                self.core.check()?;
                if ready()? {
                    return self.core.check();
                }
                changed.await;
            }
        };
        tokio::time::timeout_at(tokio::time::Instant::from_std(self.core.deadline), wait)
            .await
            .map_err(|_| self.core.fail(Failure::Deadline))?
    }
    pub(crate) fn stop(&mut self) {
        self.core.stop();
    }
    /// Post-original-listener/session join validation only; this method performs no join itself.
    pub(crate) fn inspect_after_original_join(&self) -> io::Result<()> {
        if Instant::now() >= self.core.deadline {
            return Err(self.core.fail(Failure::Deadline));
        }
        if !self.core.stopped.load(Ordering::Acquire)
            || !self.core.arm_consumed.load(Ordering::Acquire)
        {
            return Err(self.core.fail(Failure::Transition));
        }
        for index in 0..TARGETS {
            let state = self.core.state(index)?;
            if !state.facts.writer_attached
                || !state.facts.writer_destructor_returned
                || state.facts.cancel_receipt.is_none()
                || (index < CLOSING_TARGETS && state.facts.phase != Phase::Released)
                || (index == CLOSING_TARGETS
                    && (state.facts.phase != Phase::CapacityRefused || state.refusal.is_none()))
            {
                return Err(self.core.fail(Failure::Transition));
            }
        }
        if let Some(reason) = failure(self.core.first_failure.load(Ordering::Acquire)) {
            return Err(error(reason));
        }
        if Instant::now() >= self.core.deadline {
            return Err(self.core.fail(Failure::Deadline));
        }
        Ok(())
    }
}
impl Drop for PressureController {
    fn drop(&mut self) {
        self.core.stop();
    }
}

impl PressureScope {
    fn mutate(
        &self,
        statement: StatementToken,
        operation: impl FnOnce(&mut State) -> Result<(), Failure>,
    ) -> io::Result<()> {
        self.core.check()?;
        let mut state = self.core.state(self.slot)?;
        if self.core.slots[self.slot]
            .inner_poll
            .load(Ordering::Acquire)
        {
            return Err(self.core.fail(Failure::Transition));
        }
        if state.facts.statement != Some(statement) {
            return Err(self.core.fail(Failure::Identity));
        }
        if state.facts.writer_destructor_returned {
            return Err(self.core.fail(Failure::Transition));
        }
        operation(&mut state).map_err(|reason| self.core.fail(reason))?;
        drop(state);
        self.core.changed.notify_waiters();
        self.core.check()
    }
    pub(crate) fn begin_rows(
        &self,
        statement: StatementToken,
        actual_baseline: FramingCursor,
    ) -> io::Result<()> {
        self.mutate(statement, |state| {
            if state.facts.phase != Phase::Bound
                || !state.facts.writer_attached
                || actual_baseline.phase != WritePhase::Boundary
                || actual_baseline.rows_completed != 0
            {
                return Err(Failure::Transition);
            }
            state.facts.baseline = Some(actual_baseline);
            state.facts.phase = Phase::Rows;
            Ok(())
        })
    }
    /// Invoke only after the original selected Data write future has actually dropped.
    pub(crate) fn observe_cancel(
        &self,
        statement: StatementToken,
        actual_receipt: FramingCursor,
    ) -> io::Result<()> {
        self.mutate(statement, |state| {
            let baseline = state.facts.baseline.ok_or(Failure::Transition)?;
            if state.facts.phase != Phase::Rows || !state.facts.rows_blocked {
                return Err(Failure::Transition);
            }
            if actual_receipt
                .committed_wire_bytes
                .checked_sub(baseline.committed_wire_bytes)
                != Some(ROW_CUT)
                || state.facts.accepted_prefix_bytes != ROW_CUT
                || actual_receipt.phase != WritePhase::Row
                || !actual_receipt.row_has_started()
                || actual_receipt.rows_completed != 0
                || actual_receipt.sequence != baseline.sequence
                || actual_receipt.logical_total != 1_048_580
                || actual_receipt.logical_written != 1_048_571
                || actual_receipt.packet_payload_length != 1_048_580
                || actual_receipt.packet_payload_written != 1_048_571
                || actual_receipt.header != [4, 0, 16, baseline.sequence]
                || actual_receipt.header_written != 4
                || actual_receipt.zero_terminal_pending
            {
                return Err(Failure::Identity);
            }
            state.facts.cancel_receipt = Some(actual_receipt);
            state.facts.phase = Phase::CancelObserved;
            Ok(())
        })
    }
    /// A borrow of the real already-installed production ClosingDelivery is mandatory.
    /// No constructor/retained_guard/grant or body clone is called by this observer.
    pub(crate) fn observe_installed_closing<W>(
        &self,
        statement: StatementToken,
        receipt: FramingCursor,
        _original: &ClosingDelivery<W>,
    ) -> io::Result<()> {
        self.install_observation(statement, receipt)?;
        let mut state = self.core.state(self.slot)?;
        state.facts.real_closing_observed = true;
        drop(state);
        self.core.check()
    }
    fn install_observation(
        &self,
        statement: StatementToken,
        receipt: FramingCursor,
    ) -> io::Result<()> {
        self.mutate(statement, |state| {
            if self.slot >= CLOSING_TARGETS || state.facts.phase != Phase::CancelObserved {
                return Err(Failure::Transition);
            }
            if state.facts.cancel_receipt != Some(receipt) {
                return Err(Failure::Identity);
            }
            state.facts.phase = Phase::ClosingHeld;
            Ok(())
        })
    }
    /// Only the original try_closing_capacity Err branch may pass its moved source here.
    pub(crate) fn observe_capacity_refused(
        &self,
        statement: StatementToken,
        receipt: FramingCursor,
        original_admission: WorkError,
    ) -> io::Result<()> {
        let mut original = Some(original_admission);
        let result = self.mutate(statement, |state| {
            if self.slot != CLOSING_TARGETS
                || state.facts.phase != Phase::CancelObserved
                || state.refusal.is_some()
            {
                return Err(Failure::Transition);
            }
            if state.facts.cancel_receipt != Some(receipt)
                || !matches!(original.as_ref(), Some(WorkError::Capacity(_)))
            {
                return Err(Failure::Identity);
            }
            state.refusal = original.take();
            state.facts.phase = Phase::CapacityRefused;
            Ok(())
        });
        match (result, original) {
            (Err(primary), Some(admission)) => Err(io::Error::new(
                primary.kind(),
                RejectedAdmission { primary, admission },
            )),
            (result, _) => result,
        }
    }
    pub(crate) fn snapshot(&self) -> io::Result<SlotSnapshot> {
        self.core.snapshot(self.slot)
    }
    pub(crate) async fn wait_rows_blocked(&self) -> io::Result<SlotSnapshot> {
        loop {
            let changed = self.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.core.check()?;
            let facts = self.snapshot()?;
            if facts.rows_blocked {
                self.core.check()?;
                return Ok(facts);
            }
            if facts.writer_destructor_returned {
                return Err(self.core.fail(Failure::Transition));
            }
            if tokio::time::timeout_at(self.core.deadline.into(), changed)
                .await
                .is_err()
            {
                return Err(self.core.fail(Failure::Deadline));
            }
        }
    }
}
struct RejectedAdmission {
    primary: io::Error,
    admission: WorkError,
}
impl std::fmt::Debug for RejectedAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RejectedPressureAdmission { original_sources_retained: true }")
    }
}
impl std::fmt::Display for RejectedAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for RejectedAdmission {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}

pub(crate) struct ClosingPressureGate<W> {
    inner: Option<W>,
    scope: PressureScope,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl<W> ClosingPressureGate<W> {
    pub(crate) fn new(inner: W, scope: &PressureScope) -> io::Result<Self> {
        scope.core.check()?;
        let mut state = scope.core.state(scope.slot)?;
        if state.facts.phase != Phase::Bound || state.facts.writer_attached {
            return Err(scope.core.fail(Failure::Transition));
        }
        state.facts.writer_attached = true;
        drop(state);
        Ok(Self {
            inner: Some(inner),
            scope: PressureScope {
                core: Arc::clone(&scope.core),
                slot: scope.slot,
            },
            deadline: Box::pin(tokio::time::sleep_until(scope.core.deadline.into())),
        })
    }
    fn clock(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Err(self.scope.core.fail(Failure::Deadline));
        }
        self.scope.core.check()
    }
    fn plan(&self, cx: &Context<'_>, flush: bool, nonempty: bool) -> io::Result<Option<PollPlan>> {
        let mut state = self.scope.core.state(self.scope.slot)?;
        let phase = state.facts.phase;
        let held = matches!(phase, Phase::CancelObserved | Phase::ClosingHeld);
        if phase == Phase::ClosingHeld && state.facts.real_closing_observed {
            if !state.closing_poll_active {
                return Err(self.scope.core.fail(Failure::Identity));
            }
            state.facts.paired_closing_polls = state
                .facts
                .paired_closing_polls
                .checked_add(1)
                .ok_or_else(|| self.scope.core.fail(Failure::Counter))?;
        }
        let remaining = ROW_CUT
            .checked_sub(state.facts.accepted_prefix_bytes)
            .ok_or_else(|| self.scope.core.fail(Failure::Counter))?;
        if held || (phase == Phase::Rows && remaining == 0 && nonempty && !flush) {
            if phase == Phase::Rows {
                state.facts.rows_blocked = true;
            }
            if phase == Phase::ClosingHeld {
                if flush {
                    state.facts.closing_flush_blocked = true;
                } else {
                    state.facts.closing_write_blocked = true;
                }
            }
            let mut waiter = match self.scope.core.slots[self.scope.slot].waiter.lock() {
                Ok(waiter) => waiter,
                Err(poisoned) => {
                    drop(poisoned);
                    return Err(self.scope.core.fail(Failure::Poison));
                }
            };
            *waiter = Some(cx.waker().clone());
            drop(waiter);
            let actual_closing_hold =
                phase == Phase::ClosingHeld && state.facts.real_closing_observed;
            drop(state);
            // Register first, then recheck Stop/failure so racing teardown cannot miss this waker.
            self.scope.core.check()?;
            if actual_closing_hold {
                if let Some(observation) = self
                    .scope
                    .core
                    .action_observation
                    .lock()
                    .map_err(|_| self.scope.core.fail(Failure::Poison))?
                    .as_ref()
                {
                    observation
                        .record_hit()
                        .map_err(|_| self.scope.core.fail(Failure::Transition))?;
                }
            }
            self.scope.core.changed.notify_waiters();
            return Ok(None);
        }
        if !matches!(phase, Phase::Bound | Phase::Rows | Phase::Released) {
            return Err(self.scope.core.fail(Failure::Transition));
        }
        Ok(Some(PollPlan {
            phase,
            budget: if phase == Phase::Rows && !flush {
                Some(remaining as usize)
            } else {
                None
            },
        }))
    }
    fn start_poll(&self, plan: PollPlan, vectored: bool, flush: bool) -> io::Result<PollGuard> {
        let slot = &self.scope.core.slots[self.scope.slot];
        let mut state = self.scope.core.state(self.scope.slot)?;
        self.scope.core.check()?;
        if state.facts.phase != plan.phase || state.facts.writer_destructor_returned {
            return Err(self.scope.core.fail(Failure::Transition));
        }
        // The state lock pairs reservation with controller's transition check. No IO lock is held.
        if slot.inner_poll.swap(true, Ordering::AcqRel) {
            return Err(self.scope.core.fail(Failure::Transition));
        }
        let guard = PollGuard {
            core: Arc::clone(&self.scope.core),
            slot: self.scope.slot,
        };
        let counter = if flush {
            &mut state.facts.flush_inner_polls
        } else if vectored {
            &mut state.facts.vectored_inner_polls
        } else {
            &mut state.facts.scalar_inner_polls
        };
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| self.scope.core.fail(Failure::Counter))?;
        Ok(guard)
    }
    fn accepted(
        &self,
        slices: &[IoSlice<'_>],
        n: usize,
        offered: usize,
        rows: bool,
    ) -> io::Result<()> {
        if n > offered {
            return Err(self.scope.core.fail(Failure::Length));
        }
        if !rows || n == 0 {
            return Ok(());
        }
        let mut state = self.scope.core.state(self.scope.slot)?;
        let total = state
            .facts
            .accepted_prefix_bytes
            .checked_add(n as u64)
            .ok_or_else(|| self.scope.core.fail(Failure::Counter))?;
        if total > ROW_CUT {
            return Err(self.scope.core.fail(Failure::Length));
        }
        let writes = state
            .facts
            .successful_inner_writes
            .checked_add(1)
            .ok_or_else(|| self.scope.core.fail(Failure::Counter))?;
        let mut left = n;
        for slice in slices {
            let take = left.min(slice.len());
            state.digest.update(&slice[..take]);
            left -= take;
            if left == 0 {
                break;
            }
        }
        if left != 0 {
            return Err(self.scope.core.fail(Failure::Length));
        }
        state.facts.accepted_prefix_bytes = total;
        state.facts.successful_inner_writes = writes;
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct PollPlan {
    phase: Phase,
    budget: Option<usize>,
}
struct PollGuard {
    core: Arc<Core>,
    slot: usize,
}
impl Drop for PollGuard {
    fn drop(&mut self) {
        self.core.slots[self.slot]
            .inner_poll
            .store(false, Ordering::Release);
        if std::thread::panicking() {
            let _ = self.core.fail(Failure::Panic);
        }
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for ClosingPressureGate<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.clock(cx) {
            return Poll::Ready(Err(error));
        }
        let plan = match this.plan(cx, false, !bytes.is_empty()) {
            Ok(Some(plan)) => plan,
            Ok(None) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let budget = plan.budget;
        let offered = bytes
            .len()
            .min(budget.map_or(bytes.len(), |remaining| remaining.min(POLL_BYTES)));
        let guard = match this.start_poll(plan, false, false) {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let result = Pin::new(
            this.inner
                .as_mut()
                .expect("pressure concrete writer retained"),
        )
        .poll_write(cx, &bytes[..offered]);
        if let Poll::Ready(Ok(n)) = &result {
            if let Err(error) = this.accepted(
                &[IoSlice::new(&bytes[..offered])],
                *n,
                offered,
                budget.is_some(),
            ) {
                return Poll::Ready(Err(error));
            }
        }
        if matches!(&result, Poll::Ready(Err(_))) {
            let _ = this.scope.core.fail(Failure::InnerIo);
        }
        drop(guard);
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.inner
            .as_ref()
            .expect("pressure concrete writer retained")
            .is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.clock(cx) {
            return Poll::Ready(Err(error));
        }
        let plan = match this.plan(cx, false, bytes.iter().any(|slice| !slice.is_empty())) {
            Ok(Some(plan)) => plan,
            Ok(None) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let budget = plan.budget;
        let mut slices = [IoSlice::new(&[]); IO_SLICES];
        let mut count = 0;
        let mut offered = 0usize;
        let selected = if let Some(remaining) = budget {
            if bytes.len() > IO_SLICES {
                return Poll::Ready(Err(this.scope.core.fail(Failure::Length)));
            }
            let mut remaining = remaining.min(POLL_BYTES);
            for slice in bytes {
                let take = remaining.min(slice.len());
                if take == 0 {
                    continue;
                }
                slices[count] = IoSlice::new(&slice[..take]);
                count += 1;
                offered += take;
                remaining -= take;
            }
            &slices[..count]
        } else {
            for slice in bytes {
                offered = match offered.checked_add(slice.len()) {
                    Some(value) => value,
                    None => return Poll::Ready(Err(this.scope.core.fail(Failure::Length))),
                };
            }
            bytes
        };
        let guard = match this.start_poll(plan, true, false) {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let result = Pin::new(
            this.inner
                .as_mut()
                .expect("pressure concrete writer retained"),
        )
        .poll_write_vectored(cx, selected);
        if let Poll::Ready(Ok(n)) = &result {
            if let Err(error) = this.accepted(selected, *n, offered, budget.is_some()) {
                return Poll::Ready(Err(error));
            }
        }
        if matches!(&result, Poll::Ready(Err(_))) {
            let _ = this.scope.core.fail(Failure::InnerIo);
        }
        drop(guard);
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.clock(cx) {
            return Poll::Ready(Err(error));
        }
        let plan = match this.plan(cx, true, true) {
            Ok(Some(plan)) => plan,
            Ok(None) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let guard = match this.start_poll(plan, false, true) {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let result = Pin::new(
            this.inner
                .as_mut()
                .expect("pressure concrete writer retained"),
        )
        .poll_flush(cx);
        if matches!(&result, Poll::Ready(Err(_))) {
            let _ = this.scope.core.fail(Failure::InnerIo);
        }
        drop(guard);
        result
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Physical shutdown bypasses test hold/failure/deadline; it is not writer Drop or a join.
        let result = Pin::new(
            this.inner
                .as_mut()
                .expect("pressure concrete writer retained"),
        )
        .poll_shutdown(cx);
        if matches!(&result, Poll::Ready(Ok(()))) {
            let mut state = this.scope.core.slots[this.scope.slot]
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.facts.physical_shutdown_completed = true;
        }
        if matches!(&result, Poll::Ready(Err(_))) {
            let _ = this.scope.core.fail(Failure::InnerIo);
        }
        result
    }
}
impl<W> Drop for ClosingPressureGate<W> {
    fn drop(&mut self) {
        drop(self.inner.take());
        let mut state = self.scope.core.slots[self.scope.slot]
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.facts.writer_destructor_returned = true;
        drop(state);
        self.scope.core.wake_all();
    }
}
#[cfg(test)]
#[path = "closing_pressure_gate_tests.rs"]
mod tests;
