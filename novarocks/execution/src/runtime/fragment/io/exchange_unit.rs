// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Unit transport capabilities. Generated wire values stay in the adapter.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_execution_contract::ExchangeStreamIdentity;
use novarocks_execution_contract::failure::FailureFact;
pub use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeAdmissionVerdict, ExchangeStreamFacts, ExchangeUnitDemand, ExchangeUnitItem,
    ExchangeUnitSequence,
};

use crate::runtime::endpoint::RuntimeEndpoint;

#[derive(Clone, Debug)]
pub struct ExchangeUnitFrame {
    pub destination: RuntimeEndpoint,
    pub receiver: ExchangeUnitReceiverFrame,
}

#[derive(Clone, Debug)]
pub struct ExchangeUnitReceiverFrame {
    pub stream: ExchangeStreamIdentity,
    pub sender_count: u32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub item: ExchangeUnitItem,
    pub payload: Bytes,
    pub next_demand: Option<ExchangeUnitDemand>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeUnitReceipt {
    pub verdict: ExchangeAdmissionVerdict,
    pub facts: ExchangeStreamFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeStreamObservation {
    pub stream: ExchangeStreamIdentity,
    pub facts: ExchangeStreamFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDemandOperation {
    Register(ExchangeUnitDemand),
    StopSender,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeDemandEntry {
    pub stream: ExchangeStreamIdentity,
    pub operation: ExchangeDemandOperation,
}

#[derive(Clone, Debug)]
pub enum ExchangeCallOutcome {
    Observed(ExchangeUnitReceipt),
    Refused(FailureFact),
    NotProcessed(Box<str>),
    Unknown(Box<str>),
}

#[derive(Clone, Debug)]
pub enum ExchangeDemandCallOutcome {
    Observed(
        Vec<(
            ExchangeStreamIdentity,
            Result<ExchangeStreamFacts, FailureFact>,
        )>,
    ),
    Refused(FailureFact),
    NotProcessed(Box<str>),
    Unknown(Box<str>),
}

/// Move this guard with the actual request owner, including any transport
/// aliases. A deadline observation must not drop it while that owner is live.
pub struct ExchangeCallActivity {
    on_exit: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl ExchangeCallActivity {
    pub fn new(on_exit: impl FnOnce() + Send + 'static) -> Self {
        Self {
            on_exit: Some(Box::new(on_exit)),
        }
    }
}

impl fmt::Debug for ExchangeCallActivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeCallActivity")
            .finish_non_exhaustive()
    }
}

impl Drop for ExchangeCallActivity {
    fn drop(&mut self) {
        if let Some(on_exit) = self.on_exit.take() {
            on_exit();
        }
    }
}

pub type ExchangeDeliveryCallback = Box<dyn FnOnce(ExchangeCallOutcome) + Send + 'static>;
pub type ExchangeDemandCallback = Box<dyn FnOnce(ExchangeDemandCallOutcome) + Send + 'static>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeGrantNotice {
    Facts(ExchangeStreamFacts),
    /// A complete subscription snapshot omitted this still-watched stream.
    /// Its owner must replay its original demand/item through Worker routing
    /// to obtain authoritative live or closed facts; absence is not a fact.
    Resynchronize,
    /// A definite observation-protocol refusal is terminal, not a reconnect.
    Refused(FailureFact),
}

pub type ExchangeGrantWake = Arc<dyn Fn(ExchangeGrantNotice) + Send + Sync + 'static>;

/// The receiver publishes after releasing its state lock. Observers only wake
/// or coalesce by exact stream; the receiver's cumulative state is authoritative.
pub trait ExchangeUnitStateListener: Send + Sync + 'static {
    fn changed(&self, observation: ExchangeStreamObservation);
}
