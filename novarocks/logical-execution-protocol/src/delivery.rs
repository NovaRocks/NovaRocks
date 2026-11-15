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

//! Eligibility and visibility only. Payloads, schema and delivery receipts stay
//! with the original consumer and visibility owners.

#![cfg_attr(not(test), expect(dead_code, reason = "wired in the cutover commit"))]

use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_types::QueryId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryPhase {
    BeforeVisibility,
    Visible,
    Ended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryGateError {
    ForeignAttempt,
    NoEligibleAttempt,
    StaleEligibility,
    AlreadyVisible,
    AlreadyEnded,
    EligibilityExhausted,
}

/// Ordinary state, not a movable grant. Only the logical core mutates it.
#[derive(Debug)]
pub(crate) struct DeliveryGate {
    query: QueryId,
    actor_instance: u64,
    generation: u64,
    current: Option<AttemptActivationIdentity>,
    visible_to: Option<AttemptActivationIdentity>,
    phase: DeliveryPhase,
}

impl DeliveryGate {
    pub(crate) const fn new(initial: AttemptActivationIdentity) -> Self {
        Self {
            query: initial.execution().query_id(),
            actor_instance: initial.actor().get(),
            generation: initial.generation(),
            current: Some(initial),
            visible_to: None,
            phase: DeliveryPhase::BeforeVisibility,
        }
    }

    pub(crate) const fn phase(&self) -> DeliveryPhase {
        self.phase
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn current(&self) -> Option<AttemptActivationIdentity> {
        self.current
    }

    pub(crate) const fn visible_to(&self) -> Option<AttemptActivationIdentity> {
        self.visible_to
    }

    pub(crate) const fn output_visible(&self) -> bool {
        self.visible_to.is_some()
    }

    /// Called at the first client-visible grant, or the accepted internal Ready
    /// cut. No per-segment production request passes through this gate.
    pub(crate) fn mark_visible(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), DeliveryGateError> {
        self.require_current(activation)?;
        if self.phase == DeliveryPhase::Ended {
            return Err(DeliveryGateError::AlreadyEnded);
        }
        if self.visible_to.is_some() {
            return Err(DeliveryGateError::AlreadyVisible);
        }
        self.visible_to = Some(activation);
        self.phase = DeliveryPhase::Visible;
        Ok(())
    }

    pub(crate) fn revoke_for_replacement(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<u64, DeliveryGateError> {
        self.require_current(activation)?;
        if self.output_visible() {
            return Err(DeliveryGateError::AlreadyVisible);
        }
        if self.phase == DeliveryPhase::Ended {
            return Err(DeliveryGateError::AlreadyEnded);
        }
        let next = self
            .generation
            .checked_add(1)
            .ok_or(DeliveryGateError::EligibilityExhausted)?;
        self.generation = next;
        self.current = None;
        Ok(next)
    }

    pub(crate) fn activate(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), DeliveryGateError> {
        if self.phase == DeliveryPhase::Ended || self.output_visible() {
            return Err(DeliveryGateError::AlreadyEnded);
        }
        if self.current.is_some()
            || activation.actor().get() != self.actor_instance
            || activation.execution().query_id() != self.query
            || activation.generation() != self.generation
        {
            return Err(DeliveryGateError::StaleEligibility);
        }
        self.current = Some(activation);
        Ok(())
    }

    pub(crate) fn close(&mut self) {
        self.current = None;
        self.phase = DeliveryPhase::Ended;
    }

    pub(crate) fn require_current(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), DeliveryGateError> {
        match self.current {
            Some(current) if current == activation => Ok(()),
            Some(_) => Err(DeliveryGateError::ForeignAttempt),
            None => Err(DeliveryGateError::NoEligibleAttempt),
        }
    }
}

#[cfg(test)]
mod tests;
