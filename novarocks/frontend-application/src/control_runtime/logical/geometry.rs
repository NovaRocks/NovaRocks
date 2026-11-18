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

//! Fixed logical source inventory. Participation fan-in stays at its original
//! owner and cannot enlarge the logical mailbox at installation or replacement.

use novarocks_control_runtime::{actor::ActorConfig, causal_input::CAUSAL_INPUT_POSITIONS};
use novarocks_control_runtime_core::{
    identity::{ActorId, ScopeId},
    mailbox::InputClass,
};
use novarocks_logical_execution_protocol::start::LogicalStart;

pub(super) const OUTPUT_UNITS: usize = 32;
// Per activation: six active Native purposes; six replacement purposes;
// Start/Stop/Reply; schema/End reservation/End/attempt failure; register/settle
// each of two obligation kinds; port delivery; lifecycle diagnostic; ACK and
// consumer tails. A purpose is single-flight until its original roles retire.
pub(super) const ATTEMPT_PURPOSES: usize = 27;
// Original output/completion/failure/InternalReply/Ready/diagnostics/convergence;
// preparation retirement; stage/cancel/WorkOwner; snapshot/registry/retirable;
// the original bootstrap/cancellation observer, committed join-readiness notice,
// and the original client-close observer.
// These are original execution-wide purposes.
pub(super) const EXECUTION_PURPOSES: usize = 17;
const OVERFLOW: &str = "logical host geometry overflows";

#[derive(Clone, Debug)]
pub(super) struct LogicalGeometry {
    pub(super) actor: ActorId,
    pub(super) attempts: usize,
    pub(super) purposes: usize,
    pub(super) config: ActorConfig,
}
impl LogicalGeometry {
    pub(super) fn derive(scope: ScopeId, start: &LogicalStart) -> Result<Self, &'static str> {
        let attempts = usize::try_from(start.budgets.max_attempts.get()).map_err(|_| OVERFLOW)?;
        let purposes = attempts
            .checked_mul(ATTEMPT_PURPOSES)
            .and_then(|n| n.checked_add(EXECUTION_PURPOSES))
            .ok_or(OVERFLOW)?;
        // Result and actual exit use independent causal pairs. Waiting for a
        // result input's effects to retire before filling its own exit would
        // create a cycle when those effects depend on the actual exit fact.
        let reports = purposes
            .checked_mul(2 * CAUSAL_INPUT_POSITIONS)
            .ok_or(OVERFLOW)?;
        let mut mailbox = [0; InputClass::ALL.len()];
        // One ordered port-fact inlet per attempt, registry facts, bootstrap,
        // and the independently prepaid infrastructure marker.
        mailbox[InputClass::Lifecycle.index()] = reports
            .checked_add(attempts)
            .and_then(|n| n.checked_add(3))
            .ok_or(OVERFLOW)?;
        mailbox[InputClass::Control.index()] = 2; // original cancel cause and coalescing close
        // Authorization, pre-ready and data request inlets per attempt;
        // consumer and snapshot inlets are shared by the original execution.
        mailbox[InputClass::Ordinary.index()] = attempts
            .checked_mul(3)
            .and_then(|n| n.checked_add(2))
            .ok_or(OVERFLOW)?;
        mailbox[InputClass::Timer.index()] = 1;
        let max_batches = mailbox
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or(OVERFLOW)?;
        let max_step_work = OUTPUT_UNITS.checked_mul(2).ok_or(OVERFLOW)?;
        let config = ActorConfig {
            mailbox,
            max_batches,
            max_effects_per_step: OUTPUT_UNITS,
            max_dependencies_per_effect: 0,
            max_step_work,
            turn_budget: max_step_work
                .checked_add(1)
                .and_then(|n| n.checked_mul(InputClass::ALL.len()))
                .ok_or(OVERFLOW)?,
        }
        .validate()?;
        // Use the SAME actor address formula as AttemptHostGeometry. Scopes
        // are issued once by the common ledger, so actor kinds cannot collide
        // by choosing different block multipliers in a shared namespace.
        let actor = scope
            .get()
            .checked_mul(10)
            .and_then(|n| n.checked_sub(9))
            .and_then(ActorId::new)
            .ok_or(OVERFLOW)?;
        Ok(Self {
            actor,
            attempts,
            purposes,
            config,
        })
    }
    /// Source addresses are local keys in THIS actor's inbox. The actual
    /// admitted producer Work binds every position; an address grants nothing.
    pub(super) fn source(&self, ordinal: usize) -> Result<ActorId, &'static str> {
        // Actual FE actor addresses occupy offset 1 of the shared ten-address
        // scope blocks. QA source keys use offset 2, so a future attempt's
        // actual actor address can bind its fill_from_effect positions without
        // colliding with these already installed producer keys.
        let value = u64::try_from(ordinal)
            .ok()
            .and_then(|n| n.checked_mul(10))
            .and_then(|n| n.checked_add(2))
            .ok_or(OVERFLOW)?;
        ActorId::new(value).ok_or(OVERFLOW)
    }
}
