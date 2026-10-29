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

//! A unique original inbound-capacity payment follows the actual retained row.
//! Dropping an active payment is Unknown, never automatic successful refund.
use crate::control_admission::Dimension;
use novarocks_control_runtime::{
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeError,
};
use novarocks_execution_contract::{
    TaskIdentity,
    failure::{FailureClass, FailureFact, FailureProjection},
};

pub struct TaskInboundPermit {
    identity: TaskIdentity,
    retained_bytes: u64,
    payment: Option<CapacityBundle>,
}
impl std::fmt::Debug for TaskInboundPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskInboundPermit")
            .field("identity", &self.identity)
            .field("retained_bytes", &self.retained_bytes)
            .field("released", &self.payment.is_none())
            .finish()
    }
}
#[derive(Debug)]
pub enum TaskInboundPermitError {
    Runtime(RuntimeError),
}
impl std::fmt::Display for TaskInboundPermitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the original inbound capacity payment could not be transferred or released")
    }
}
impl std::error::Error for TaskInboundPermitError {}
impl FailureProjection for TaskInboundPermitError {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
        )
    }
}
impl TaskInboundPermit {
    /// Only the measured original Accepted payer issues this value after
    /// validating/splitting exactly Records1 and its positive original Bytes.
    pub(crate) fn from_original(
        identity: TaskIdentity,
        retained_bytes: u64,
        payment: CapacityBundle,
    ) -> Self {
        let resources = payment
            .resources()
            .expect("original inbound split remains live");
        assert_eq!(resources.get(Dimension::InboundRecords.id()), 1);
        assert_eq!(resources.get(Dimension::InboundBytes.id()), retained_bytes);
        assert!(retained_bytes > 0);
        assert!(
            resources
                .iter()
                .all(|(d, _)| d == Dimension::InboundRecords.id()
                    || d == Dimension::InboundBytes.id())
        );
        Self {
            identity,
            retained_bytes,
            payment: Some(payment),
        }
    }
    pub fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
    pub(crate) fn transfer_to(&mut self, owner: &WorkLease) -> Result<(), TaskInboundPermitError> {
        self.payment
            .as_mut()
            .expect("original live inbound payment")
            .transfer(owner)
            .map_err(TaskInboundPermitError::Runtime)
    }
    /// Consume this original permit only after its actual bank row was removed,
    /// outside its locks. Failure returns the same original active payment;
    /// successful release leaves no reusable permit value.
    pub fn release(mut self) -> Result<(), (TaskInboundPermitError, Self)> {
        let result = (|| {
            let payment = self
                .payment
                .as_mut()
                .expect("original live inbound payment");
            let resources = payment.resources()?;
            payment.release(&resources)
        })();
        match result {
            Ok(()) => {
                self.payment.take();
                Ok(())
            }
            Err(error) => Err((TaskInboundPermitError::Runtime(error), self)),
        }
    }
}
