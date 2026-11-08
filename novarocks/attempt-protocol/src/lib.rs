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

//! Deterministic frontend attempt protocol contracts.
//!
//! The shared boundary is the neutral activation/port vocabulary, frozen plan,
//! input/effect values, seeded operation identities and synchronous step port.
//! Concrete state is implemented independently of the FE host. There is no SDK,
//! generated DTO, clock read, runnable, permit or rich-source custody here.
//! Payload/grant/reservation references identify original host records only.
//! They do not grant capacity or access and do not retain a foreign destructor.
//!
//! All times use the original process monotonic timeline. Root controls share
//! the observation source FIFO; a pending seal is reconsidered after each
//! observation, before applying the next. Read-state confirmation is prepaid
//! independently of that ingress and describes one consistent snapshot.
//!
//! Effect completion records host handoff. Actual work/capture exit is a
//! separate input authored from the original ExitCustody report.

#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

pub mod capacity;
pub mod config;
pub mod effect;
pub mod failure;
pub mod ids;
pub mod input;
pub mod start;

pub mod observation;
pub mod operation;
pub mod publication;

/// The pure protocol surface frozen for host adapters and deterministic worlds.
/// Implementations must finish application of one bounded input and charge all
/// derived traversal/effect work. Budget exhaustion retains an explicit cursor;
/// only a real remaining continuation may schedule an immediate deadline.
pub trait AttemptCore: Sized {
    fn new(start: start::AttemptStart) -> Result<Self, failure::AttemptFailure>;
    fn handle(
        &mut self,
        now: novarocks_coordination_contract::MonotonicInstant,
        input: input::AttemptInput,
        context: &mut dyn capacity::StepContext,
    ) -> Vec<effect::AttemptEffect>;
    fn next_deadline(&self) -> Option<novarocks_coordination_contract::MonotonicInstant>;
}

pub mod completion;
pub mod completion_facts;
pub mod conclusion;
pub mod domain_tracker;
pub mod error;
pub mod graph;
pub mod lease;
pub mod participation_owner;
pub mod participation_state;
pub mod status_validation;
#[cfg(test)]
mod test_support;

pub mod dispatch;
pub mod intent;
pub mod remote_task;
pub mod split_delivery;
pub mod stage;

pub mod execution;

pub mod root_control;

pub mod diagnostics;

pub(crate) mod terminal;
