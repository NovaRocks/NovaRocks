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

//! Host inventory derived from the attempt's frozen sources and existing limits.
//! These counts describe the positions the original Source will reserve. The
//! common ledger and workload owner still authorize every actual occupation.

use super::{adapter::OUTPUT_UNITS, pumps::AttemptPumpSource};
use crate::native::transport_capacity::NativeTransportCapacity;
use novarocks_attempt_protocol::{
    config::DispatchBudget,
    effect::PumpKind,
    input::{AttemptSourceId, ObservationBatch},
    start::{AttemptSources, AttemptStart, PayloadRef},
};
use novarocks_control_runtime::{actor::ActorConfig, causal_input::CAUSAL_INPUT_POSITIONS};
use novarocks_control_runtime_core::{identity::ScopeId, mailbox::InputClass};
use novarocks_task_codec::TransportBudget;
use std::num::{NonZeroU64, NonZeroUsize};

/// One causal reply each for QA, process observation, publication, payload
/// release, read confirmation and subscription control (AttemptTransportBinding).
const CALL_SOURCES: usize = 6;
const OVERFLOW: &str = "attempt host geometry overflows";

#[derive(Debug)]
pub(crate) struct AttemptHostGeometry {
    pub(crate) sources: AttemptSources,
    /// Independent Ordinary and Control transport purposes, in that order.
    pub(crate) admission_results: [usize; 2],
    pub(crate) send_results: [usize; 2],
    pub(crate) transport_joins: usize,
    participants: NonZeroUsize,
    dispatch: DispatchBudget,
    transport: TransportBudget,
    pumps: Pumps,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Pumps {
    split: bool,
    dynamic: bool,
    diagnostics: bool,
}

impl Pumps {
    fn from_plan(
        pumps: &[(PayloadRef, PumpKind)],
        diagnostics: bool,
    ) -> Result<Self, &'static str> {
        let mut original = Self {
            split: false,
            dynamic: false,
            diagnostics,
        };
        for &(_, kind) in pumps {
            let present = match kind {
                PumpKind::SplitAssignment => &mut original.split,
                PumpKind::DynamicFilters => &mut original.dynamic,
            };
            if std::mem::replace(present, true) {
                return Err("attempt host pump appears more than once");
            }
        }
        Ok(original)
    }

    fn join_roles(self) -> usize {
        // Split Start and Stop have independent causal reply roles. Dormant
        // disposal can reuse the Start role after its original physical exit.
        AttemptPumpSource::role_count(self.dynamic, self.diagnostics) + 2 * usize::from(self.split)
    }

    fn callback_positions(self) -> (usize, usize) {
        // Split failure is Lifecycle causal. Profile is Ordinary causal with
        // a Lifecycle predecessor. One more Ordinary position carries delivery.
        let split = usize::from(self.split);
        ((CAUSAL_INPUT_POSITIONS + 1) * split, 2 * split)
    }
}

impl AttemptHostGeometry {
    pub(crate) fn derive(
        scope: ScopeId,
        participants: NonZeroUsize,
        dispatch: DispatchBudget,
        layout: &NativeTransportCapacity,
        pumps: &[(PayloadRef, PumpKind)],
        retain_diagnostics: bool,
    ) -> Result<Self, &'static str> {
        let pumps = Pumps::from_plan(pumps, retain_diagnostics)?;
        let transport = layout.transport_budget();
        let [ordinary, control] =
            transport_roles(participants.get(), dispatch, layout).ok_or(OVERFLOW)?;
        // Admission and send can be live concurrently even for the same
        // logical dispatch lane. Each purpose keeps its own exact credit.
        let admission_results = [ordinary, control];
        let send_results = [ordinary, control];

        let transport_joins = sum([
            ordinary,
            control,
            ordinary,
            control,
            CALL_SOURCES,
            participants.get(),
            pumps.join_roles(),
        ])
        .ok_or(OVERFLOW)?;
        let report_positions =
            send_report_positions(send_results, dispatch, transport).ok_or(OVERFLOW)?;
        let (split_lifecycle, split_ordinary) = pumps.callback_positions();

        // Every Join role has a Lifecycle causal reply (predecessor and
        // event). Standalone Lifecycle positions are startup, the shared
        // ordered observation writer and one phase publisher per live
        // covered subscription. The split callbacks have their own roles.
        let lifecycle = sum([
            transport_joins
                .checked_mul(CAUSAL_INPUT_POSITIONS)
                .ok_or(OVERFLOW)?,
            report_positions,
            1,
            1,
            participants.get(),
            split_lifecycle,
        ])
        .ok_or(OVERFLOW)?;
        // Control is one Abort per participant plus root, Stop and BeginConvergence.
        let control = participants.get().checked_add(3).ok_or(OVERFLOW)?;
        // Ordinary is one read-progress role per participant plus membership
        // and capacity. Split also owns its independent delivery and profile.
        let ordinary = sum([participants.get(), 2, split_ordinary]).ok_or(OVERFLOW)?;
        // CR owns one Timer occurrence. Its builtin IsolateScope failure
        // input and batch are funded independently by supervised registration.
        let mut mailbox = [0; InputClass::ALL.len()];
        mailbox[InputClass::Lifecycle.index()] = lifecycle;
        mailbox[InputClass::Control.index()] = control;
        mailbox[InputClass::Ordinary.index()] = ordinary;
        mailbox[InputClass::Timer.index()] = 1;

        Ok(Self {
            sources: attempt_sources(scope, mailbox)?,
            admission_results,
            send_results,
            transport_joins,
            participants,
            dispatch,
            transport,
            pumps,
        })
    }

    pub(crate) fn actor_config(&self, start: &AttemptStart) -> Result<ActorConfig, &'static str> {
        let plan = &start.plan;
        if start.sources != self.sources
            || plan.establish_material.len() != self.participants.get()
            || plan.coordination.dispatch != self.dispatch
            || plan.transport.max_batch_items != self.transport.max_batch_items()
            || plan.transport.max_query_backend_queued_operations
                != self.transport.max_query_backend_queued_operations()
            || plan.transport.max_backend_queued_operations
                != self.transport.max_backend_queued_operations()
            || Pumps::from_plan(&plan.pumps, start.activation.policy.retain_diagnostics)?
                != self.pumps
        {
            return Err("attempt host geometry differs from its frozen start");
        }
        self.config_with_core_floor(start.minimum_core_step_units().ok_or(OVERFLOW)?)
    }

    fn config_with_core_floor(&self, core: usize) -> Result<ActorConfig, &'static str> {
        // Core supplies the maximum indivisible dispatch continuation and
        // primary charge. The complete ordinary observation batch has one
        // primary charge and at most MAX_ITEMS frame charges. Keep one unit
        // for derived progress and the adapter's separate effect handoff.
        let core = core.max(ObservationBatch::MAX_ITEMS + 1);
        let max_step_work = sum([core, 1, OUTPUT_UNITS]).ok_or(OVERFLOW)?;
        let turn_budget = max_step_work
            .checked_add(1)
            .and_then(|step| step.checked_mul(InputClass::ALL.len()))
            .ok_or(OVERFLOW)?;
        ActorConfig {
            mailbox: self.sources.mailbox,
            // Each committed normal batch still owns its input position.
            // The complete inventory fits even when one purpose's handoff
            // is waiting and another position needs to run to unblock it.
            max_batches: sum(self.sources.mailbox).ok_or(OVERFLOW)?,
            max_effects_per_step: OUTPUT_UNITS,
            // Attempt descriptors have no explicit dependencies. The host
            // owns ordering between effects on the same dispatch lane.
            max_dependencies_per_effect: 0,
            turn_budget,
            max_step_work,
        }
        .validate()
    }
}

fn transport_roles(
    participants: usize,
    dispatch: DispatchBudget,
    layout: &NativeTransportCapacity,
) -> Option<[usize; 2]> {
    let facts = layout.capacity_sources();
    // The dispatcher enforces the per-attempt target queue limit for both
    // purposes. The process additionally withholds one maximum control batch
    // from ordinary traffic. The deployment window is exactly create_permits.
    let target_queue = facts.target_items.checked_mul(participants)?;
    let ordinary_queue = facts
        .process_items
        .checked_sub(facts.control_reserved_items)?
        .min(target_queue);
    let control_queue = facts.process_items.min(target_queue);
    // Update includes ordinary task updates and control-only exchange closes.
    // Lifecycle includes ordinary Acquire/Establish/domain advance and
    // control Renew/Abort/Release. Cancel and Quiesce use the Control lane.
    let shared = dispatch
        .update_permits()
        .checked_add(dispatch.lifecycle_permits())?;
    let ordinary_credit = shared.checked_add(dispatch.create_permits())?;
    let control_credit = shared.checked_add(dispatch.control_permits())?;
    Some([
        ordinary_credit
            .checked_mul(participants)?
            .min(ordinary_queue),
        control_credit.checked_mul(participants)?.min(control_queue),
    ])
}

fn send_report_positions(
    sends: [usize; 2],
    dispatch: DispatchBudget,
    transport: TransportBudget,
) -> Option<usize> {
    let members = |permits: usize| permits.min(transport.max_batch_items());
    // Each batch has one started report and one receipt per member.
    // An ordinary Lifecycle batch may also carry the first RPC poll of its
    // one Establish: a batch addresses exactly one backend participation.
    let ordinary = [
        members(dispatch.create_permits()).checked_add(1)?,
        members(dispatch.update_permits()).checked_add(1)?,
        members(dispatch.lifecycle_permits()).checked_add(2)?,
    ]
    .into_iter()
    .max()?;
    let control = members(
        dispatch
            .update_permits()
            .max(dispatch.lifecycle_permits())
            .max(dispatch.control_permits()),
    )
    .checked_add(1)?;
    // A received ACK can advance Core before the send and report positions
    // actually exit. Bound these reports by the independently owned send
    // roles, including old generations, rather than by today's Core credit.
    sends[0]
        .checked_mul(ordinary)?
        .checked_add(sends[1].checked_mul(control)?)
}

fn sum(values: impl IntoIterator<Item = usize>) -> Option<usize> {
    values.into_iter().try_fold(0, usize::checked_add)
}

fn attempt_sources(scope: ScopeId, mailbox: [usize; 4]) -> Result<AttemptSources, &'static str> {
    // Reserve a disjoint block of ten addresses in the process actor namespace
    // for each original ledger Scope. These are passive routing identities.
    const SOURCES: u64 = 10;
    let end = scope
        .get()
        .checked_mul(SOURCES)
        .ok_or("attempt source identities exhausted")?;
    let base = end - SOURCES;
    let source = |offset| {
        AttemptSourceId(NonZeroU64::new(base + offset).expect("nonzero source within scope block"))
    };
    Ok(AttemptSources {
        actor: source(1),
        publication: source(2),
        qa: source(3),
        process: source(4),
        native: source(5),
        transport: source(6),
        subscription: source(7),
        observation: source(8),
        capacity: source(9),
        membership: source(10),
        mailbox,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_participants_pay_causal_predecessors_and_each_send_report() {
        let layout = NativeTransportCapacity::from_transport(TransportBudget::DEFAULT).unwrap();
        let pumps = [
            (
                PayloadRef(NonZeroU64::new(1).unwrap()),
                PumpKind::SplitAssignment,
            ),
            (
                PayloadRef(NonZeroU64::new(2).unwrap()),
                PumpKind::DynamicFilters,
            ),
        ];
        let geometry = AttemptHostGeometry::derive(
            ScopeId::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
            DispatchBudget::DEFAULT,
            &layout,
            &pumps,
            true,
        )
        .unwrap();

        assert_eq!(geometry.admission_results, [96, 60]);
        assert_eq!(geometry.send_results, [96, 60]);
        assert_eq!(geometry.transport_joins, 329);
        // 658 causal positions, 2412 send reports, 5 standing, 3 split callbacks.
        assert_eq!(geometry.sources.mailbox, [3078, 6, 7, 1]);
        let config = geometry.config_with_core_floor(1).unwrap();
        assert_eq!(config.max_batches, 3092);
        assert_eq!(config.max_step_work, 261);
        assert_eq!(config.turn_budget, 1048);
        assert_eq!(config.max_effects_per_step, OUTPUT_UNITS);
        assert_eq!(config.max_dependencies_per_effect, 0);
    }

    #[test]
    fn tight_process_window_and_exact_scope_bound_original_host_roles() {
        let transport = TransportBudget::new(
            2,
            10,
            5,
            3,
            20,
            6,
            40,
            1,
            2,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let layout = NativeTransportCapacity::from_transport(transport).unwrap();
        let geometry = |scope| {
            AttemptHostGeometry::derive(
                ScopeId::new(scope).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                DispatchBudget::DEFAULT,
                &layout,
                &[],
                false,
            )
        };
        let first = geometry(1).unwrap();
        let second = geometry(2).unwrap();
        assert_eq!(first.admission_results, [4, 6]);
        assert_eq!(first.send_results, [4, 6]);
        assert_eq!(first.transport_joins, 28);
        assert_eq!(first.sources.mailbox, [94, 5, 4, 1]);
        assert!(first.sources.membership.0 < second.sources.actor.0);
        assert!(geometry(u64::MAX).is_err());
    }
}
