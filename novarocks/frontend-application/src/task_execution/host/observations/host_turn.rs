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

//! The original FE host indexes and retires the ordered writer's occurrence.

use super::{ObservationError, OrderedObservationInput, backing, read_progress::ReadProgressInput};
use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorRuntime, ActorTurnError, StandingInputId,
};
use novarocks_control_runtime_core::identity::InputId;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub(crate) struct OrderedObservationHost {
    registration: ActorRegistration,
    roles: [StandingInputId; 2],
    indexed: Option<IndexedObservation>,
    progress_roles: BTreeMap<StandingInputId, ReadProgressInput>,
    indexed_progress: VecDeque<(StandingInputId, InputId)>,
    ingress: OrderedObservationInput<FrontendActor>,
}

/// Moves actual failed observation payloads into the existing credentialed
/// supervisor without carrying any authority to publish or settle Core facts.
pub(crate) struct OrderedObservationFailure {
    roles: BTreeSet<StandingInputId>,
    progress: Vec<ReadProgressInput>,
    ingress: OrderedObservationInput<FrontendActor>,
}

impl OrderedObservationFailure {
    pub(crate) fn dispose_queued(&self) -> Result<(), ObservationError> {
        for role in &self.progress {
            role.abandon_pending_after_failure();
        }
        self.ingress.notify_source_progress();
        while let Some(original) = self
            .ingress
            .take_failed_source_payload(&self.roles)
            .map_err(ObservationError::Runtime)?
        {
            original.dispose().map_err(ObservationError::Runtime)?;
        }
        Ok(())
    }
    pub(crate) async fn dispose_tail(self) -> Result<(), ObservationError> {
        self.ingress.dispose_failed_tail().await
    }
}

struct IndexedObservation {
    input: InputId,
    control: bool,
    processed: bool,
}

impl OrderedObservationHost {
    pub(super) fn backing_bytes(participants: usize) -> Result<usize, ObservationError> {
        let failed_roles = participants
            .checked_add(2)
            .ok_or(ObservationError::Budget)?;
        backing::sum([
            backing::tree::<StandingInputId, ReadProgressInput>(participants)?,
            backing::array::<(StandingInputId, InputId)>(participants)?,
            backing::tree::<StandingInputId, ()>(failed_roles)?,
            backing::array::<ReadProgressInput>(participants)?,
        ])
    }

    pub(crate) fn new(ingress: OrderedObservationInput<FrontendActor>) -> Self {
        let registration = ingress.0.root_binding.registration().clone();
        let roles = ingress.standing_ids();
        Self {
            ingress,
            registration,
            roles,
            indexed: None,
            progress_roles: BTreeMap::new(),
            indexed_progress: VecDeque::new(),
        }
    }

    pub(crate) fn with_read_progress(
        mut self,
        roles: impl IntoIterator<Item = ReadProgressInput>,
    ) -> Self {
        assert!(self.progress_roles.is_empty());
        for role in roles {
            let id = role.standing_id();
            assert!(role.matches(&self.registration) && !self.roles.contains(&id));
            assert!(self.progress_roles.insert(id, role).is_none());
        }
        self.indexed_progress = VecDeque::with_capacity(self.progress_roles.len());
        self
    }

    /// At most one indexed occurrence per paid role plus the ordered writer.
    /// Leave a further unit for the independent Native retirement visit.
    pub(crate) fn fits_host_turn(&self, turn_budget: usize) -> bool {
        self.progress_roles.len() <= turn_budget.saturating_sub(2)
    }

    pub(crate) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    /// The host visits only inputs already committed and indexed by its
    /// dispatch cursor. Read the original standing role even if fill has not
    /// returned the new input id to the writer yet.
    pub(crate) fn index_committed(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        inputs: &[InputId],
    ) -> Result<bool, ActorTurnError> {
        let mut advanced = false;
        for &input in inputs {
            let Some(role) = runtime
                .input_custody(self.registration.actor(), input)?
                .and_then(|custody| custody.standing_input_id)
            else {
                continue;
            };
            if self.progress_roles.contains_key(&role) {
                assert!(self.indexed_progress.len() < self.progress_roles.len());
                self.indexed_progress.push_back((role, input));
                advanced = true;
                continue;
            }
            if !self.roles.contains(&role) {
                continue;
            }
            assert!(
                self.indexed
                    .replace(IndexedObservation {
                        input,
                        control: role == self.roles[1],
                        processed: false,
                    })
                    .is_none(),
                "only the original ordered occurrence can be in flight"
            );
            advanced = true;
        }
        Ok(advanced)
    }

    pub(crate) fn root_control_processed(
        &mut self,
        input: InputId,
    ) -> Result<(), ObservationError> {
        let indexed = self
            .indexed
            .as_mut()
            .filter(|indexed| indexed.input == input && indexed.control && !indexed.processed)
            .ok_or(ObservationError::InvalidRootControl)?;
        self.ingress.root_control_processed(input)?;
        indexed.processed = true;
        Ok(())
    }

    pub(crate) fn has_indexed(&self) -> bool {
        self.indexed.is_some()
    }

    /// The Core's committed batch or a completed effect alone cannot refund
    /// a frame. This retires the exact original input and only then releases
    /// the ingress's actual credit and any suffix blocked behind it.
    pub(crate) fn retire_ready(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<(), ObservationError> {
        let Some(input) = self
            .indexed
            .as_ref()
            .filter(|indexed| !indexed.control || indexed.processed)
            .map(|indexed| indexed.input)
        else {
            return Ok(());
        };
        match runtime.retire_input(self.registration.actor(), input) {
            Ok(()) => {}
            Err(
                ActorTurnError::InputStillQueued
                | ActorTurnError::EffectsRemain
                | ActorTurnError::ReservationsRemain
                | ActorTurnError::MetadataStillPending,
            ) => return Ok(()),
            Err(error) => return Err(ObservationError::Runtime(error)),
        }
        self.indexed = None;
        self.ingress.input_retired(input)
    }

    /// Visit each indexed progress role once under the registered host
    /// allowance. An outstanding effect can keep one occurrence live without
    /// preventing the other participants' actual returns.
    pub(crate) fn retire_read_progress(
        &mut self,
        budget: &mut usize,
    ) -> Result<(), ObservationError> {
        let visits = self.indexed_progress.len();
        assert!(
            visits <= *budget,
            "original host allowance covers the frozen roster"
        );
        for _ in 0..visits {
            *budget -= 1;
            let (role, input) = self.indexed_progress[0];
            let original = self
                .progress_roles
                .get(&role)
                .expect("indexed original role");
            let retired = original
                .retire_indexed(input)
                .map_err(ObservationError::Runtime)?;
            self.indexed_progress.pop_front();
            if !retired {
                self.indexed_progress.push_back((role, input));
            }
        }
        Ok(())
    }

    pub(crate) fn fence_failed(&mut self) -> OrderedObservationFailure {
        self.ingress.fence_failed_writer();
        // The failed path re-reads actual standing occurrences; the normal
        // committed index cannot account for payloads disposed off-poller.
        self.indexed = None;
        self.indexed_progress.clear();
        let mut roles = BTreeSet::new();
        for role in self
            .roles
            .into_iter()
            .chain(self.progress_roles.keys().copied())
        {
            roles.insert(role);
        }
        OrderedObservationFailure {
            roles,
            progress: self.progress_roles.values().cloned().collect(),
            ingress: self.ingress.clone(),
        }
    }

    /// The supervisor only disposes failed payloads. One original host
    /// retires both previously committed and failed/disposed occurrences.
    pub(crate) fn retire_failed(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        budget: &mut usize,
    ) -> Result<(), ObservationError> {
        let current = self.ingress.failed_current_input();
        assert!(
            self.progress_roles.len() + usize::from(current.is_some()) <= *budget,
            "original host allowance covers failed observation roles"
        );
        if let Some(input) = current {
            *budget -= 1;
            match runtime.retire_input(self.registration.actor(), input) {
                Ok(()) => self.ingress.failed_input_retired(input)?,
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain
                    | ActorTurnError::MetadataStillPending,
                ) => {}
                Err(error) => return Err(ObservationError::Runtime(error)),
            }
        }
        for role in self.progress_roles.values() {
            *budget -= 1;
            role.retire_failed().map_err(ObservationError::Runtime)?;
        }
        Ok(())
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.indexed.is_none() && self.indexed_progress.is_empty() && self.ingress.is_retired()
    }

    pub(crate) fn ready_for_close(&self) -> bool {
        self.indexed.is_none()
            && self.indexed_progress.is_empty()
            && self.ingress.ready_for_host_close()
    }

    pub(crate) fn backing_pin(&self) -> Option<backing::ObservationBackingPin> {
        self.ingress.1.clone()
    }

    pub(crate) fn backing_exit(
        &self,
    ) -> Option<novarocks_control_runtime::obligation::ReservationRetirementObserver> {
        self.ingress
            .1
            .as_ref()
            .map(backing::ObservationBackingPin::exit)
    }

    pub(crate) fn fence_root_controls(&self) {
        self.ingress.fence_root_controls_for_close();
    }

    /// The caller transferred the original ordered host into its blocking
    /// Work, so the same body destroys the retained roles and writer alias.
    pub(crate) fn close_on_executor(self) {
        if let Err(error) = self.ingress.close_after_exit() {
            std::panic::resume_unwind(Box::new((error, self)));
        }
    }
}
