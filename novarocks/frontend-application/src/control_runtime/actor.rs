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

use novarocks_control_runtime::{
    actor::{Actor, ActorStep},
    actor_turn::Clock,
};
use novarocks_control_runtime_core::{
    identity::{Generation, ScopeId, Tick},
    mailbox::InputEnvelope,
};
use novarocks_coordination_contract::MonotonicClock;
use std::sync::Arc;

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
    #[cfg(test)]
    Test,
}
pub(crate) enum FrontendActor {
    #[cfg(test)]
    Test(Box<dyn TestActor>),
}
#[derive(Clone, Debug)]
pub(crate) enum FrontendInput {
    Timer,
    CapacityReleased,
    Membership(super::membership::MembershipUpdate),
    #[cfg(test)]
    Test(u64),
}
#[derive(Clone, Debug)]
pub(crate) enum FrontendEffect {
    #[cfg(test)]
    Test { value: u64 },
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum FrontendLaneKey {
    #[cfg(test)]
    Test(u64),
}
impl FrontendActor {
    pub(crate) fn kind(&self) -> FrontendActorKind {
        #[cfg(test)]
        {
            match self {
                Self::Test(_) => FrontendActorKind::Test,
            }
        }
        #[cfg(not(test))]
        {
            match *self {}
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
        #[cfg(test)]
        {
            match self {
                Self::Test(actor) => actor.handle(now, input, value, step),
            }
        }
        #[cfg(not(test))]
        {
            let _ = (now, input, value, step);
            match *self {}
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        #[cfg(test)]
        {
            match self {
                Self::Test(actor) => actor.next_deadline(),
            }
        }
        #[cfg(not(test))]
        {
            match *self {}
        }
    }
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        input: InputEnvelope,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        // No suspect Core is inspected. P10a adds the attempt's fixed immutable
        // failure projection beside the test-only implementation.
        let _ = (scope, generation, input, step);
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
