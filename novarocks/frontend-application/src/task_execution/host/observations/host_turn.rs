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

use super::{ObservationError, OrderedObservationInput};
use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorRuntime, ActorTurnError, StandingInputId,
};
use novarocks_control_runtime_core::identity::InputId;

pub(crate) struct OrderedObservationHost {
    ingress: OrderedObservationInput<FrontendActor>,
    registration: ActorRegistration,
    roles: [StandingInputId; 2],
    indexed: Option<IndexedObservation>,
}

struct IndexedObservation {
    input: InputId,
    control: bool,
    processed: bool,
}

impl OrderedObservationHost {
    pub(crate) fn new(ingress: OrderedObservationInput<FrontendActor>) -> Self {
        let registration = ingress.0.root_binding.registration().clone();
        let roles = ingress.standing_ids();
        Self {
            ingress,
            registration,
            roles,
            indexed: None,
        }
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
                .filter(|role| self.roles.contains(role))
            else {
                continue;
            };
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

    pub(crate) fn is_retired(&self) -> bool {
        self.indexed.is_none() && self.ingress.is_retired()
    }
}
