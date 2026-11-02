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

//! The original actor owns one declared report leg, independently of the
//! scope that paid for its producer or a later execution-scope detachment.

use super::ReportSlot;
use crate::{
    actor::Actor,
    driver::{ActorFailureProof, ActorRegistration, ActorRuntime, DriverError},
    sync::Arc,
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind, history::HistoryFact, identity::ConsumerId,
};

/// A capability for this exact original report consumer. Clones refer to the
/// same leg; the original CompletionSlot still allows only its first outcome.
#[derive(Clone)]
pub struct ActorReportConsumer {
    report: ReportSlot,
    consumer: ConsumerId,
    registration: ActorRegistration,
}
impl std::fmt::Debug for ActorReportConsumer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorReportConsumer")
            .field("report", &self.report.id)
            .field("consumer", &self.consumer)
            .field("registration", &self.registration)
            .finish_non_exhaustive()
    }
}
impl ReportSlot {
    /// Bind the actual predeclared Owner leg to its original actor witness.
    /// Rebinding creates no new leg and cannot reset a terminal consumption.
    pub fn bind_actor_consumer<A: Actor>(
        &self,
        runtime: &ActorRuntime<A>,
        registration: &ActorRegistration,
        consumer: ConsumerId,
    ) -> Result<ActorReportConsumer, DriverError> {
        runtime.validate_registration(registration)?;
        let original_scope = registration.publication_scope();
        if !Arc::ptr_eq(&self.runtime.shared, &original_scope.runtime.shared) {
            return Err(DriverError::PublicationTargetMismatch);
        }
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("original actor report binding lock");
        let report = state.slots.get(&self.id).ok_or(RuntimeError::UnknownSlot)?;
        if report.consumer_kind(consumer)
            != Some(ConsumerKind::Owner {
                scope: registration.scope_id(),
                generation: registration.initial_epoch(),
            })
        {
            return Err(DriverError::PublicationTargetMismatch);
        }
        Ok(ActorReportConsumer {
            report: self.clone(),
            consumer,
            registration: registration.clone(),
        })
    }
}
impl ActorReportConsumer {
    pub fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub fn consume(&self) -> Result<(), RuntimeError> {
        self.report.consume(self.consumer)
    }
    /// Only this original actor's fenced failure abandons its declared leg.
    /// The old execution Scope need not exist; no financial Scope is failed.
    pub fn abandon(&self, proof: &ActorFailureProof) -> Result<(), DriverError> {
        proof.validate_consumer(&self.registration, &self.report.runtime)?;
        let mut state = self
            .report
            .runtime
            .shared
            .state
            .lock()
            .expect("original actor report abandonment lock");
        state
            .slots
            .get_mut(&self.report.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .abandon_owner(
                self.consumer,
                self.registration.scope_id(),
                self.registration.initial_epoch(),
            )
            .map_err(RuntimeError::from)?;
        self.report
            .runtime
            .shared
            .record(|| HistoryFact::SlotAbandoned {
                slot: self.report.id,
                consumer: self.consumer,
            });
        drop(state);
        self.report.runtime.shared.changed.notify();
        Ok(())
    }
}
