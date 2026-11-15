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

//! Pure decisions run on the same FE actor carrier as attempts. This adapter
//! binds actual step guards; it executes no application future or destructor.

use super::{funding::LogicalStepReservations, geometry::OUTPUT_UNITS};
use crate::control_runtime::actor::{FrontendEffect, FrontendInput};
use novarocks_control_runtime::actor::ActorStep;
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ReservationId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::WorkKind,
};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_logical_execution_protocol::{
    EffectSink, LogicalExecutionCore, StepBudget,
    effect::{LogicalEffect, LogicalLane, LogicalWorkKind},
    input::LogicalInput,
};
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub(crate) enum LogicalHostEffect {
    Protocol(Box<LogicalEffect>),
    /// Applied by the original Source only after this normal step commits.
    JoinReadiness(bool),
    /// Original source startup and its cancellation observation share one paid role.
    Bootstrap,
    ClientCloseObservation,
    /// The current source ticket already committed the exact port handoff.
    PortDelivered(novarocks_coordination_contract::AttemptActivationIdentity),
}
impl LogicalHostEffect {
    pub(crate) fn lane(&self) -> LogicalLane {
        match self {
            Self::Protocol(effect) => effect.lane(),
            Self::PortDelivered(activation) => LogicalLane::Attempt(*activation),
            Self::JoinReadiness(_) | Self::Bootstrap | Self::ClientCloseObservation => {
                LogicalLane::Diagnostics
            }
        }
    }
    pub(crate) fn work_kind(&self) -> LogicalWorkKind {
        match self {
            Self::Protocol(effect) => effect.work_kind(),
            Self::JoinReadiness(_)
            | Self::Bootstrap
            | Self::ClientCloseObservation
            | Self::PortDelivered(_) => LogicalWorkKind::Settlement,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct LogicalEffectEnvelope {
    pub(crate) body: LogicalHostEffect,
    pub(crate) reservation: ReservationId,
}
#[derive(Debug)]
pub(crate) enum LogicalActorFailure {
    Infrastructure,
    #[cfg(test)]
    InjectedCoreTurn,
    #[expect(
        dead_code,
        reason = "P11 projects this exact retained Core fault without reading failed state"
    )]
    Protocol(novarocks_logical_execution_protocol::CoreFault),
}

pub(crate) struct LogicalActor {
    core: LogicalExecutionCore,
    funding: LogicalStepReservations,
    join_ready: bool,
}
struct Budget {
    remaining: usize,
    initial: usize,
}
impl StepBudget for Budget {
    fn charge(&mut self, n: usize) -> bool {
        if n > self.remaining {
            return false;
        }
        self.remaining -= n;
        true
    }
}
#[derive(Default)]
struct Sink(Vec<LogicalEffect>);
impl EffectSink for Sink {
    fn emit(&mut self, effect: LogicalEffect) {
        self.0.push(effect)
    }
}

impl LogicalActor {
    pub(super) fn new(core: LogicalExecutionCore, funding: LogicalStepReservations) -> Self {
        Self {
            core,
            funding,
            join_ready: false,
        }
    }
    pub(crate) fn handle(
        &mut self,
        now: Tick,
        input: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        let value = match value {
            FrontendInput::Timer => LogicalInput::Timer,
            FrontendInput::Logical(value) => *value,
            FrontendInput::LogicalInfrastructureFailed => {
                assert_eq!(input.class, InputClass::Lifecycle);
                std::panic::panic_any(LogicalActorFailure::Infrastructure);
            }
            FrontendInput::LogicalHostSettled => return,
            #[cfg(test)]
            FrontendInput::Test(u64::MAX) => {
                std::panic::panic_any(LogicalActorFailure::InjectedCoreTurn)
            }
            _ => panic!("foreign input entered the logical execution actor"),
        };
        assert_eq!(
            input.class,
            input_class(&value),
            "logical input uses its original paid class"
        );
        // Core traversal/effect selection and actual ActorStep emission both
        // cost units. Keep one output and two units for a committed readiness
        // transition. No callback shares a mutable ActorStep with Core.
        let initial = (step.remaining().saturating_sub(2) / 2).min(OUTPUT_UNITS - 1);
        let mut budget = Budget {
            remaining: initial,
            initial,
        };
        let mut out = Sink::default();
        if let Err(error) = self.core.handle(
            MonotonicInstant::from_origin(Duration::from_nanos(now.0)),
            value,
            &mut budget,
            &mut out,
        ) {
            std::panic::panic_any(LogicalActorFailure::Protocol(error));
        }
        assert!(
            step.charge(budget.initial - budget.remaining),
            "reserved logical core work fits its real step"
        );
        let notice_id = out
            .0
            .iter()
            .map(|effect| effect.action.get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .expect("normal batch action identity exhausted");
        for effect in out.0 {
            let id = effect.action.get();
            self.emit(id, LogicalHostEffect::Protocol(Box::new(effect)), step);
        }
        let ready = self.core.facts().protocol_join_ready();
        if ready != self.join_ready {
            self.emit(notice_id, LogicalHostEffect::JoinReadiness(ready), step);
            self.join_ready = ready;
        }
    }
    fn emit(&self, id: u64, body: LogicalHostEffect, step: &mut ActorStep<'_, FrontendEffect>) {
        let (work_kind, effect_kind) = match body.work_kind() {
            LogicalWorkKind::Business => (WorkKind::Business, EffectKind::Business),
            LogicalWorkKind::Settlement => (WorkKind::Settlement, EffectKind::Settlement),
            LogicalWorkKind::Cleanup => (WorkKind::Cleanup, EffectKind::Cleanup),
        };
        let guard = self
            .funding
            .reserve(work_kind)
            .expect("original logical metadata reservation");
        let reservation = guard.reservation_id();
        let work = ScopedWorkRef {
            scope: guard.scope().id(),
            work: guard.work_id(),
        };
        assert!(
            step.reservation(guard).is_ok(),
            "same original logical guard fits its atomic step"
        );
        step.effect(
            EffectDescriptor {
                id: ActionId::new(id).expect("nonzero logical batch action"),
                work,
                kind: effect_kind,
                dependencies: Vec::new(),
            },
            FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope { body, reservation })),
        )
        .expect("bounded original logical effect handoff");
    }
    pub(crate) fn next_deadline(&self) -> Option<Tick> {
        self.core
            .next_deadline()
            .map(|at| Tick(u64::try_from(at.since_origin().as_nanos()).unwrap_or(u64::MAX)))
    }
}

pub(super) fn input_class(input: &LogicalInput) -> InputClass {
    use novarocks_coordination_contract::logical_port::AttemptToLogicalBody;
    match input {
        LogicalInput::Timer => InputClass::Timer,
        LogicalInput::Cancelled(_) | LogicalInput::RegistryClose(_) => InputClass::Control,
        LogicalInput::Relay { .. }
        | LogicalInput::Internal { .. }
        | LogicalInput::ConsumerClosed { .. }
        | LogicalInput::Snapshot { .. } => InputClass::Ordinary,
        LogicalInput::AttemptPort(message) => match message.body {
            AttemptToLogicalBody::AuthorizationRequest { .. }
            | AttemptToLogicalBody::PreReadyReplacementQuery { .. } => InputClass::Ordinary,
            _ => InputClass::Lifecycle,
        },
        LogicalInput::Started
        | LogicalInput::ActionResult { .. }
        | LogicalInput::ActionExited { .. }
        | LogicalInput::RegistryParticipation { .. }
        | LogicalInput::RegistryJoinReadiness { .. } => InputClass::Lifecycle,
    }
}
