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
use novarocks_execution_contract::{OperationOutcome, QueryContextRef, TaskIdentity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PayloadId(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreationCost {
    pub input_retained_bytes: u64,
    pub ingress_retained_bytes: u64,
    pub structural_bytes: u64,
    pub inbound_records: u64,
    pub inbound_bytes: u64,
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
        context: QueryContextRef,
        task: TaskIdentity,
        generation: Generation,
    },
    Preparation {
        context: QueryContextRef,
        task: TaskIdentity,
        generation: Generation,
        preparing_in_context: usize,
        bytes: u64,
        inbound_records: u64,
        inbound_bytes: u64,
    },
    Context {
        context: QueryContextRef,
        generation: Generation,
    },
    Ticket {
        context: QueryContextRef,
    },
}
/// Only a synchronous multidimensional scope/ledger transaction is permitted.
/// The returned reservation remains a scope-visible StepGuard until the entire
/// step batch is transferred. This port must never bind a queue, run a callback,
/// take a host lock, or wait for another actor. Unwind keeps guard custody.
pub trait AdmissionPort {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, OperationOutcome>;
}

/// One admitted RPC reply role. The operation identity may replay, while the
/// token is unique to this exact waiter and its frozen absolute deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub token: novarocks_control_runtime_core::identity::InputId,
    pub operation: novarocks_execution_contract::TaskOperationId,
    pub deadline: crate::lease::MonotonicInstant,
}
