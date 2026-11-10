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

//! The FE process owns one common actor carrier. P10a adds the real attempt
//! variant; protocol state remains pure and rich objects stay in host Sources.

use super::transport_withdrawal::AttemptEffectEnvelope;
use crate::task_execution::host::adapter::{AttemptActor, AttemptNotice, descriptor};
use novarocks_attempt_protocol::{config::DispatchLane, input::AttemptInput};
use novarocks_control_runtime::{
    actor::{Actor, ActorStep},
    actor_turn::Clock,
};
use novarocks_control_runtime_core::{
    effect::EffectKind,
    identity::{Generation, ScopeId, Tick},
    mailbox::InputEnvelope,
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_execution_contract::ParticipationRef;
use novarocks_types::BackendProcessId;
use std::{num::NonZeroU64, sync::Arc};

pub(crate) struct FrontendClock(pub(crate) Arc<dyn MonotonicClock>);
impl Clock for FrontendClock {
    fn now(&self) -> Tick {
        // The runtime's existing ProcessClock uses the same saturating u64-ns
        // representation. Keep the caller's original FE timeline origin.
        Tick(u64::try_from(self.0.now().since_origin().as_nanos()).unwrap_or(u64::MAX))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrontendActorKind {
    Attempt,
    #[cfg(test)]
    Test,
}
pub(crate) enum FrontendActor {
    Attempt(Box<AttemptActor>),
    #[cfg(test)]
    Test(Box<dyn TestActor>),
}
#[derive(Clone, Debug)]
pub(crate) enum FrontendInput {
    Timer,
    CapacityReleased,
    Membership(super::membership::MembershipUpdate),
    Attempt(Box<AttemptInput>),
    #[cfg(test)]
    Test(u64),
}
#[derive(Clone, Debug)]
pub(crate) enum FrontendEffect {
    // The host peeks and indexes this same immutable effect without cloning
    // a batch's operation list or publication rows.
    Attempt(Arc<AttemptEffectEnvelope>),
    AttemptNotice(Arc<AttemptNotice>),
    AttemptOwnerFailed {
        scope: ScopeId,
        generation: Generation,
        failed_input: InputEnvelope,
    },
    #[cfg(test)]
    Test {
        value: u64,
    },
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum FrontendLaneKey {
    Send {
        target: BackendProcessId,
        lane: DispatchLane,
        control_progress: bool,
    },
    Subscription {
        participation: ParticipationRef,
        generation: NonZeroU64,
    },
    Publication,
    #[cfg(test)]
    Test(u64),
}
impl FrontendActor {
    pub(crate) fn kind(&self) -> FrontendActorKind {
        match self {
            Self::Attempt(_) => FrontendActorKind::Attempt,
            #[cfg(test)]
            Self::Test(_) => FrontendActorKind::Test,
        }
    }
}
impl Actor for FrontendActor {
    type Input = FrontendInput;
    type Effect = FrontendEffect;
    fn timer_input(_: Tick) -> FrontendInput {
        FrontendInput::Timer
    }
    fn handle(
        &mut self,
        now: Tick,
        input: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        match self {
            Self::Attempt(actor) => actor.handle(now, input, value, step),
            #[cfg(test)]
            Self::Test(actor) => actor.handle(now, input, value, step),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        match self {
            Self::Attempt(actor) => actor.next_deadline(),
            #[cfg(test)]
            Self::Test(actor) => actor.next_deadline(),
        }
    }
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        input: InputEnvelope,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        if let Some(work) = step.failure_work() {
            step.effect(
                descriptor(1, work, EffectKind::Cleanup),
                FrontendEffect::AttemptOwnerFailed {
                    scope,
                    generation,
                    failed_input: input,
                },
            )
            .unwrap();
        }
    }
}
#[cfg(test)]
pub(crate) trait TestActor: Send + 'static {
    fn handle(
        &mut self,
        now: Tick,
        input: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    );
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
}
