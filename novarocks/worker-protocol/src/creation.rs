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

//! Neutral admission intents. The embedding actor executes the port inline.
use novarocks_control_runtime_core::identity::{Generation, ReservationId};
use novarocks_execution_contract::operation::DomainFailureOutcome;
use novarocks_execution_contract::{
    OperationDomainFailure, OperationOutcome, ParticipationRef, TaskIdentity,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PayloadId(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreationCost {
    pub input_retained_bytes: u64,
    pub ingress_retained_bytes: u64,
    pub structural_bytes: u64,
}
impl CreationCost {
    pub fn preparation_bytes(self) -> Option<u64> {
        self.input_retained_bytes
            .checked_add(self.ingress_retained_bytes)?
            .checked_add(self.structural_bytes)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionNeed {
    ActiveTask {
        participation: ParticipationRef,
        task: TaskIdentity,
        generation: Generation,
    },
    Preparation {
        participation: ParticipationRef,
        task: TaskIdentity,
        /// The actual loading/Task lifetime reservation. The synchronous port
        /// resolves its live parent work inside the same ledger transaction.
        active: ReservationId,
        generation: Generation,
        preparing_in_participation: usize,
        bytes: u64,
    },
    Participation {
        participation: ParticipationRef,
        generation: Generation,
    },
}
/// The exact pre-Accepted refusal. Typed domain provenance remains intact
/// through Creating settlement and entity replay; this carries no raw source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionRefusal {
    Verdict(OperationOutcome),
    Domain(DomainFailureOutcome, OperationDomainFailure),
}
impl AdmissionRefusal {
    pub const fn outcome(&self) -> OperationOutcome {
        match self {
            Self::Verdict(outcome) => *outcome,
            Self::Domain(outcome, _) => outcome.outcome(),
        }
    }
}
impl From<OperationOutcome> for AdmissionRefusal {
    fn from(outcome: OperationOutcome) -> Self {
        Self::Verdict(outcome)
    }
}
/// Only a synchronous multidimensional scope/ledger transaction is permitted.
/// The returned reservation remains a scope-visible StepGuard until the entire
/// step batch is transferred. This port must never bind a queue, run a callback,
/// take a host lock, or wait for another actor. Unwind keeps guard custody.
pub trait AdmissionPort {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal>;
}

/// One admitted RPC reply role. The adapter copies the token from this
/// request's actual Runtime InputEnvelope in the destination actor, never from
/// a wire value or a separate protocol counter. Runtime issuance is unique and
/// never reuses an ID after retirement. Fair scheduling may consume independent
/// sources/classes in a different numeric order, so the core checks only that
/// a token is not currently owned; it keeps no unbounded retired-token history.
/// The operation identity may replay; each real waiter has its own token and
/// frozen absolute deadline. Immediate replies retain the same runtime no-reuse
/// obligation even though they need no pending entry in the pure core.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestReplyTag {
    /// The same original admitted R work, qualified by its actual scope.
    /// This neutral identity grants no runtime permission or capacity.
    pub author: novarocks_control_runtime_core::identity::ScopedWorkRef,
    pub token: novarocks_control_runtime_core::identity::InputId,
    pub operation: novarocks_execution_contract::TaskOperationId,
    pub deadline: crate::lease::MonotonicInstant,
}
