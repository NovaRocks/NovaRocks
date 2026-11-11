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

//! The observation Source's original input positions and frame byte window.

use super::{
    ObservationBudget, ObservationCredit, ObservationError, StreamKey, backing,
    read_progress::{ReadProgressInput, ReadProgressReceipt},
};
use novarocks_attempt_protocol::input::ReadProgressSnapshot;
use novarocks_control_runtime::{
    obligation::{OwnedPositionObligation, ReservationObligation, WorkObligation},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    identity::DimensionId,
    owned_storage::{OwnedSlotKind, OwnedSlotNeed},
};
use novarocks_execution_contract::ParticipationRef;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Bound::{Excluded, Unbounded},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// The observation byte window is prepaid on the same ledger as its real
/// input positions. It is independent of the send queue/encoding dimensions.
pub(crate) const OBSERVATION_WINDOW_BYTES: DimensionId = DimensionId::new(12).unwrap();

impl ObservationBudget {
    pub(crate) fn byte_window(self) -> Result<ResourceVector, RuntimeError> {
        self.byte_windows(NonZeroUsize::MIN)
    }

    /// The process maximum follows the original FE scope inventory. Opening
    /// a scope and admitting one actual Source still require their own owners.
    pub(crate) fn byte_windows(self, scopes: NonZeroUsize) -> Result<ResourceVector, RuntimeError> {
        let bytes = u64::try_from(self.total_bytes)
            .ok()
            .zip(u64::try_from(scopes.get()).ok())
            .and_then(|(bytes, scopes)| bytes.checked_mul(scopes))
            .ok_or(CapacityError::Overflow)?;
        Ok(ResourceVector::new([(OBSERVATION_WINDOW_BYTES, bytes)])?)
    }
}

/// Retains the byte share once the split's Input positions move to their
/// actual owners. Abandoned Drop leaves the original ledger row unresolved.
struct ObservationWindow {
    original: ReservationObligation,
    bytes: ResourceVector,
}

impl ObservationWindow {
    fn retire_after_exit(&mut self) -> Result<(), ObservationError> {
        if !self.bytes.is_empty() {
            self.original
                .release_quota(&self.bytes)
                .map_err(|error| ObservationError::Runtime(error.into()))?;
            self.bytes = ResourceVector::default();
        }
        self.original
            .finish()
            .map_err(|error| ObservationError::Runtime(error.into()))
    }
}

#[derive(Default)]
struct ReadClaims {
    live: usize,
    waiter: Option<Waker>,
    publishing_progress: usize,
    progress: Option<ReadProgressReceipt>,
    progress_failed: bool,
}

impl ReadClaims {
    fn exited(&self) -> bool {
        self.live == 0
            && self.waiter.is_none()
            && self.publishing_progress == 0
            && !self.progress_failed
            && self
                .progress
                .as_ref()
                .is_none_or(ReadProgressReceipt::retired)
    }
}

struct Credits {
    participants: BTreeSet<ParticipationRef>,
    data: Vec<OwnedPositionObligation>,
    control: Option<OwnedPositionObligation>,
    window: Option<ObservationWindow>,
    data_bytes: usize,
    control_live: bool,
    streams: BTreeMap<StreamKey, ReadClaims>,
    closed: bool,
    retiring: bool,
}

#[derive(Clone, Copy)]
enum Claim {
    Read(StreamKey),
    Control,
}

pub(crate) struct ObservationCredits {
    budget: ObservationBudget,
    state: Mutex<Credits>,
    backing: Option<backing::ObservationBackingPin>,
}

impl ObservationCredits {
    /// Split the paid positions and bytes from the same original Source Work
    /// before publishing the pool. The roster bounds the waiter inventory.
    pub(crate) fn bind(
        budget: ObservationBudget,
        participants: impl IntoIterator<Item = ParticipationRef>,
        work: &WorkObligation,
        reservation: &mut ReservationObligation,
        backing: Option<backing::ObservationBackingPin>,
    ) -> Result<Arc<Self>, ObservationError> {
        budget.validate()?;
        if !reservation.belongs_to_bound_work(work.scope(), work.id()) {
            return Err(ObservationError::Budget);
        }
        let bytes = budget
            .byte_window()
            .map_err(|error| ObservationError::Runtime(error.into()))?;
        let original = reservation
            .split_next(&ReservationResources::new(
                bytes.clone(),
                OwnedSlotNeed {
                    inputs: budget.total_positions,
                    ..Default::default()
                },
            ))
            .map_err(|error| ObservationError::Runtime(error.into()))?;
        let mut window = ObservationWindow { original, bytes };
        let mut positions = Vec::with_capacity(budget.total_positions);
        for _ in 0..budget.total_positions {
            match window.original.take_position(OwnedSlotKind::Input) {
                Ok(position) => positions.push(position),
                Err(error) => {
                    drop(positions);
                    window.retire_after_exit()?;
                    return Err(ObservationError::Runtime(error.into()));
                }
            }
        }
        let mut roster = BTreeSet::new();
        for participation in participants {
            roster.insert(participation);
        }
        for position in &mut positions {
            position.bind().expect("new original observation position");
        }
        let control = positions.pop();
        Ok(Arc::new(Self {
            budget,
            state: Mutex::new(Credits {
                participants: roster,
                data: positions,
                control,
                window: Some(window),
                data_bytes: 0,
                control_live: false,
                streams: BTreeMap::new(),
                closed: false,
                retiring: false,
            }),
            backing,
        }))
    }

    pub(super) fn budget(&self) -> ObservationBudget {
        self.budget
    }

    pub(super) fn matches_roster(
        &self,
        participants: impl Iterator<Item = ParticipationRef>,
    ) -> bool {
        self.state
            .lock()
            .expect("observation credit owner")
            .participants
            .iter()
            .copied()
            .eq(participants)
    }

    pub(crate) fn register_stream(&self, stream: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.state.lock().expect("observation credit owner");
        if state.closed {
            return Err(ObservationError::Closed);
        }
        if !state.participants.contains(&stream.participation) {
            return Err(ObservationError::UnknownStream);
        }
        if state.streams.contains_key(&stream) {
            return Err(ObservationError::GenerationAlreadyRegistered);
        }
        // The legacy byte-window owner has the same live subscription cap.
        // A replacement can overlap a prior generation's actual exit.
        if state.streams.len() == self.budget.total_positions {
            return Err(ObservationError::Budget);
        }
        state.streams.insert(stream, ReadClaims::default());
        Ok(())
    }

    pub(crate) fn stream_claims_exited(&self, stream: StreamKey) -> bool {
        self.state
            .lock()
            .expect("observation credit owner")
            .streams
            .get(&stream)
            .is_some_and(ReadClaims::exited)
    }

    pub(crate) fn retire_stream(&self, stream: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.state.lock().expect("observation credit owner");
        let row = state.read_claims(stream)?;
        if !row.exited() {
            return Err(ObservationError::Busy);
        }
        state.streams.remove(&stream);
        Ok(())
    }

    pub(crate) fn poll_read(
        self: &Arc<Self>,
        stream: StreamKey,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ObservationCredit, ObservationError>> {
        let mut waiter = Some(cx.waker().clone());
        let reserved = self.budget.max_frame_bytes;
        let (position, old_waiter) = {
            let mut state = self.state.lock().expect("observation credit owner");
            if state.closed {
                return Poll::Ready(Err(ObservationError::Closed));
            }
            let bytes_available =
                self.budget.total_bytes - self.budget.control_bytes - state.data_bytes;
            let can_read = bytes_available >= reserved && !state.data.is_empty();
            let row = state.read_claims(stream)?;
            if !can_read {
                let old_waiter = std::mem::replace(&mut row.waiter, waiter.take());
                drop(state);
                drop(old_waiter);
                return Poll::Pending;
            }
            row.live += 1;
            let old_waiter = row.waiter.take();
            state.data_bytes += reserved;
            (
                state.data.pop().expect("original data position"),
                old_waiter,
            )
        };
        let credit = self.credit(position, Claim::Read(stream), reserved);
        drop(old_waiter);
        Poll::Ready(Ok(credit))
    }

    pub(crate) fn cancel_read_wait(&self, stream: StreamKey) {
        let waiter = self
            .state
            .lock()
            .expect("observation credit owner")
            .read_claims(stream)
            .ok()
            .and_then(|row| row.waiter.take());
        drop(waiter);
    }

    pub(crate) fn read_resumed(
        &self,
        source: &ReadProgressInput,
        progress: ReadProgressSnapshot,
    ) -> Result<(), ObservationError> {
        let stream = StreamKey {
            participation: progress.participation,
            generation: progress.generation,
        };
        {
            let mut state = self.state.lock().expect("observation credit owner");
            if state.closed {
                return Err(ObservationError::Closed);
            }
            let row = state.read_claims(stream)?;
            if row.live == 0 {
                return Err(ObservationError::Busy);
            }
            // Keep the exact generation live while fill can wake the host.
            row.publishing_progress += 1;
        }
        let published = source.publish(progress);
        let mut state = self.state.lock().expect("observation credit owner");
        let row = state
            .read_claims(stream)
            .expect("original progress publisher");
        row.publishing_progress -= 1;
        match published {
            Ok(receipt) => {
                if let Some(original) = row.progress.as_mut() {
                    original.retain_later(receipt);
                } else {
                    row.progress = Some(receipt);
                }
                Ok(())
            }
            Err(error) => {
                row.progress_failed = true;
                Err(error)
            }
        }
    }

    /// The canonical request owner must validate its original row before
    /// calling this method. The original control share is never used for data.
    pub(crate) fn try_control(self: &Arc<Self>) -> Result<ObservationCredit, ObservationError> {
        let position = {
            let mut state = self.state.lock().expect("observation credit owner");
            if state.closed {
                return Err(ObservationError::Closed);
            }
            if state.control_live {
                return Err(ObservationError::Busy);
            }
            let position = state.control.take().ok_or(ObservationError::Busy)?;
            state.control_live = true;
            position
        };
        Ok(self.credit(position, Claim::Control, self.budget.control_bytes))
    }

    fn credit(
        self: &Arc<Self>,
        position: OwnedPositionObligation,
        claim: Claim,
        reserved: usize,
    ) -> ObservationCredit {
        ObservationCredit::Original(BoundCredit {
            original: self.clone(),
            claim,
            position: Some(position),
            reserved,
            _backing: self.backing.clone(),
        })
    }

    pub(super) fn backing_bytes(
        positions: usize,
        participants: usize,
    ) -> Result<usize, ObservationError> {
        backing::sum([
            backing::arc::<Self>()?.size(),
            backing::array::<OwnedPositionObligation>(positions)?,
            backing::tree::<ParticipationRef, ()>(participants)?,
            backing::tree::<StreamKey, ReadClaims>(positions)?,
        ])
    }

    /// Take one original waiter under the lock, then wake it outside. Bound a
    /// release to one pass over distinct streams, even if a synchronous wake
    /// re-registers its own waiter or new generations arrive during the pass.
    fn wake_readers(&self) {
        let mut after = Unbounded;
        for _ in 0..self.budget.total_positions {
            let next = self
                .state
                .lock()
                .expect("observation credit owner")
                .streams
                .range_mut((after, Unbounded))
                .find_map(|(key, row)| row.waiter.take().map(|waiter| (*key, waiter)));
            let Some((key, waiter)) = next else {
                break;
            };
            after = Excluded(key);
            waiter.wake();
        }
    }

    fn return_claim(&self, claim: Claim) {
        let mut state = self.state.lock().expect("observation credit owner");
        match claim {
            Claim::Read(stream) => {
                state.read_claims(stream).expect("original read claim").live -= 1;
            }
            Claim::Control => state.control_live = false,
        }
    }

    /// Only the original Source calls this after all publishers, queued
    /// values, and runtime consumers have actually exited.
    pub(crate) fn retire_after_exit(&self) -> Result<(), ObservationError> {
        let (mut positions, mut control, mut window) = {
            let mut state = self.state.lock().expect("observation credit owner");
            state.closed = true;
            if state.retiring
                || state.control_live
                || state.data_bytes != 0
                || state.streams.values().any(|row| !row.exited())
            {
                return Err(ObservationError::Busy);
            }
            state.retiring = true;
            (
                std::mem::take(&mut state.data),
                state.control.take(),
                state.window.take(),
            )
        };
        let retired = (|| {
            while let Some(position) = positions.pop() {
                if let Err((error, original)) = position.retire_after_exit() {
                    positions.push(original);
                    return Err(ObservationError::Runtime(error.into()));
                }
            }
            if let Some(position) = control.take()
                && let Err((error, original)) = position.retire_after_exit()
            {
                control = Some(original);
                return Err(ObservationError::Runtime(error.into()));
            }
            if let Some(window) = window.as_mut() {
                window.retire_after_exit()?;
            }
            Ok(())
        })();
        let mut state = self.state.lock().expect("observation credit owner");
        state.retiring = false;
        if retired.is_ok() {
            state.streams.clear();
            state.participants.clear();
        } else {
            state.data = positions;
            state.control = control;
            state.window = window;
        }
        retired
    }
}

impl Credits {
    fn read_claims(&mut self, stream: StreamKey) -> Result<&mut ReadClaims, ObservationError> {
        self.streams
            .get_mut(&stream)
            .ok_or(ObservationError::UnknownStream)
    }
}

pub(crate) struct BoundCredit {
    original: Arc<ObservationCredits>,
    claim: Claim,
    position: Option<OwnedPositionObligation>,
    reserved: usize,
    // Drop the original credits Arc before releasing this borrower's pin.
    _backing: Option<backing::ObservationBackingPin>,
}

impl BoundCredit {
    pub(super) fn shrink_to(&mut self, charged_bytes: usize) -> Result<(), ObservationError> {
        let returned =
            self.reserved
                .checked_sub(charged_bytes)
                .ok_or(ObservationError::Oversized {
                    charged: charged_bytes,
                    reserved: self.reserved,
                })?;
        self.reserved = charged_bytes;
        if returned != 0 && matches!(self.claim, Claim::Read(_)) {
            self.original
                .state
                .lock()
                .expect("observation credit owner")
                .data_bytes -= returned;
            self.original.wake_readers();
        }
        Ok(())
    }
}

impl Drop for BoundCredit {
    fn drop(&mut self) {
        let position = self.position.take().expect("original observation position");
        {
            let mut state = self
                .original
                .state
                .lock()
                .expect("observation credit owner");
            match self.claim {
                Claim::Read(_) => {
                    state.data_bytes -= self.reserved;
                    state.data.push(position);
                }
                Claim::Control => {
                    assert!(state.control.replace(position).is_none());
                }
            }
        }
        if matches!(self.claim, Claim::Read(_)) {
            self.original.wake_readers();
        }
        // Synchronous wake failure leaves the original claim unresolved.
        self.original.return_claim(self.claim);
    }
}

#[cfg(test)]
mod tests;
