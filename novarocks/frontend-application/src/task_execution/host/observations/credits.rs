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
    InputSlotLoan, ObservationBudget, ObservationByteLoan, ObservationCredit, ObservationError,
    StreamKey,
};
use novarocks_control_runtime::obligation::{
    OwnedPositionObligation, ReservationObligation, WorkObligation,
};
use novarocks_control_runtime_core::owned_storage::OwnedSlotKind;
use novarocks_execution_contract::ParticipationRef;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[derive(Default)]
struct ReadClaims {
    live: usize,
    waiter: Option<Waker>,
}

struct Credits {
    data: Vec<OwnedPositionObligation>,
    control: Option<OwnedPositionObligation>,
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
    participants: BTreeSet<ParticipationRef>,
    state: Mutex<Credits>,
}

impl ObservationCredits {
    /// Bind the real Input positions against the original Source Work before
    /// publishing the pool. The fixed roster bounds the waiter/claim inventory.
    pub(crate) fn bind(
        budget: ObservationBudget,
        participants: impl IntoIterator<Item = ParticipationRef>,
        work: &WorkObligation,
        reservation: &mut ReservationObligation,
    ) -> Result<Arc<Self>, ObservationError> {
        budget.validate()?;
        if !reservation.belongs_to_bound_work(work.scope(), work.id()) {
            return Err(ObservationError::Budget);
        }
        let participants = participants.into_iter().collect();
        let mut positions = Vec::with_capacity(budget.total_positions);
        for _ in 0..budget.total_positions {
            // Refusal drops only positions that were still unbound.
            positions.push(
                reservation
                    .take_position(OwnedSlotKind::Input)
                    .map_err(|error| ObservationError::Runtime(error.into()))?,
            );
        }
        for position in &mut positions {
            position.bind().expect("new original observation position");
        }
        let control = positions.pop();
        Ok(Arc::new(Self {
            budget,
            participants,
            state: Mutex::new(Credits {
                data: positions,
                control,
                data_bytes: 0,
                control_live: false,
                streams: BTreeMap::new(),
                closed: false,
                retiring: false,
            }),
        }))
    }

    pub(crate) fn register_stream(&self, stream: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.state.lock().expect("observation credit owner");
        if state.closed {
            return Err(ObservationError::Closed);
        }
        if !self.participants.contains(&stream.participation) {
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
            .is_some_and(|row| row.live == 0 && row.waiter.is_none())
    }

    pub(crate) fn retire_stream(&self, stream: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.state.lock().expect("observation credit owner");
        let row = state.read_claims(stream)?;
        if row.live != 0 || row.waiter.is_some() {
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
        let slot_owner = self.clone();
        let claim_owner = self.clone();
        ObservationCredit::from_source(
            InputSlotLoan::from_source(position, move |position| {
                let waiters = {
                    let mut state = slot_owner.state.lock().expect("observation credit owner");
                    match claim {
                        Claim::Read(_) => {
                            state.data.push(position);
                            state.take_waiters()
                        }
                        Claim::Control => {
                            assert!(state.control.replace(position).is_none());
                            Vec::new()
                        }
                    }
                };
                for waiter in waiters {
                    waiter.wake();
                }
            }),
            Box::new(Bytes {
                original: self.clone(),
                claim,
                reserved,
            }),
            move || claim_owner.return_claim(claim),
        )
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
        let (mut positions, mut control) = {
            let mut state = self.state.lock().expect("observation credit owner");
            state.closed = true;
            if state.retiring
                || state.control_live
                || state.data_bytes != 0
                || state
                    .streams
                    .values()
                    .any(|row| row.live != 0 || row.waiter.is_some())
            {
                return Err(ObservationError::Busy);
            }
            state.retiring = true;
            (std::mem::take(&mut state.data), state.control.take())
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
            Ok(())
        })();
        let mut state = self.state.lock().expect("observation credit owner");
        state.retiring = false;
        state.data = positions;
        state.control = control;
        if retired.is_ok() {
            state.streams.clear();
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

    fn take_waiters(&mut self) -> Vec<Waker> {
        self.streams
            .values_mut()
            .filter_map(|row| row.waiter.take())
            .collect()
    }
}

struct Bytes {
    original: Arc<ObservationCredits>,
    claim: Claim,
    reserved: usize,
}

impl ObservationByteLoan for Bytes {
    fn reserved_bytes(&self) -> usize {
        self.reserved
    }

    fn shrink_to(&mut self, charged_bytes: usize) -> Result<(), ObservationError> {
        let returned =
            self.reserved
                .checked_sub(charged_bytes)
                .ok_or(ObservationError::Oversized {
                    charged: charged_bytes,
                    reserved: self.reserved,
                })?;
        self.reserved = charged_bytes;
        if returned != 0 && matches!(self.claim, Claim::Read(_)) {
            let waiters = {
                let mut state = self
                    .original
                    .state
                    .lock()
                    .expect("observation credit owner");
                state.data_bytes -= returned;
                state.take_waiters()
            };
            for waiter in waiters {
                waiter.wake();
            }
        }
        Ok(())
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        if matches!(self.claim, Claim::Read(_)) {
            // The following slot return notifies readers after both original
            // resources are available, before the final claim receipt.
            self.original
                .state
                .lock()
                .expect("observation credit owner")
                .data_bytes -= self.reserved;
        }
    }
}

#[cfg(test)]
mod tests;
