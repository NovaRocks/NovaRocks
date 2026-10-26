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

//! One scheduler/poller pool runs both Worker actor units. The enum only
//! selects the protocol owner; it introduces no additional lifecycle policy.
use super::{ContextActor, ContextMessage, ControlActor, ControlMessage, WorkerAction};
use novarocks_control_runtime::actor::{Actor, StepContext};
use novarocks_control_runtime_core::{
    identity::{Generation, ScopeId, Tick},
    mailbox::InputEnvelope,
};

pub(crate) enum WorkerActor {
    Context(Box<ContextActor>),
    Control(Box<ControlActor>),
}
#[expect(
    clippy::large_enum_variant,
    reason = "Each prepaid input stores one bounded neutral command; there are no host payloads or foreign destructors."
)]
#[derive(Debug)]
pub(crate) enum WorkerMessage {
    Context(ContextMessage),
    Control(ControlMessage),
    Timer,
}
impl Actor for WorkerActor {
    type Input = WorkerMessage;
    type Effect = WorkerAction;
    fn timer_input(_: Tick) -> WorkerMessage {
        WorkerMessage::Timer
    }
    fn handle(
        &mut self,
        now: Tick,
        envelope: InputEnvelope,
        message: WorkerMessage,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        match (self, message) {
            (Self::Context(core), WorkerMessage::Context(input)) => {
                core.handle(now, envelope, input, step)
            }
            (Self::Control(core), WorkerMessage::Control(input)) => {
                core.handle(now, envelope, input, step)
            }
            (Self::Context(core), WorkerMessage::Timer) => {
                core.handle(now, envelope, ContextMessage::Timer, step)
            }
            (Self::Control(core), WorkerMessage::Timer) => {
                core.handle(now, envelope, ControlMessage::Timer, step)
            }
            _ => panic!("Worker input target differs from its admitted actor kind"),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        match self {
            Self::Context(core) => core.next_deadline(),
            Self::Control(core) => core.next_deadline(),
        }
    }
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        input: InputEnvelope,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        ContextActor::owner_failed(scope, generation, input, step);
    }
}
