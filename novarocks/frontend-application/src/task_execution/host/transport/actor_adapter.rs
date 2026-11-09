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

//! Only this adapter knows Actor routing and its original paid causal result.
//! Replacement qualification supplies its QA leg directly to the same kernel.
use super::{
    queued_job::{BoundQueue, QueueJob},
    send::{BoundSend, SendJob},
    source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg},
};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect},
    host::PreparedEffectJob,
};
use novarocks_control_runtime::{
    causal_input::CausalInputObligation,
    exit_work::{HandoffCell, PanicSink},
};
use std::sync::Arc;

/// The same Source owns the original result store. This port holds only a
/// Weak reference to its Join-owning record and is prepaid before dispatch.
pub(crate) trait ActorTransportResultPort: Send + Sync + 'static {
    fn body_finished(
        &self,
        routing: FrontendEffect,
        outcome: BodyOutcome,
        original: CausalInputObligation<FrontendActor>,
    );
}
struct ActorResultObligation {
    routing: FrontendEffect,
    original: CausalInputObligation<FrontendActor>,
    port: Arc<dyn ActorTransportResultPort>,
}
impl TransportCompletionObligation for ActorResultObligation {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome) {
        let Self {
            routing,
            original,
            port,
        } = *self;
        port.body_finished(routing, outcome, original);
    }
}
/// Source builds this binding from the exact admitted job and its canonical
/// original grants. No descriptor identity is accepted as an author.
pub(crate) struct BoundActorSend {
    pub(crate) send: Box<BoundSend>,
    pub(crate) handoff: Arc<HandoffCell>,
    pub(crate) panic: PanicSink,
    pub(crate) result: Arc<dyn ActorTransportResultPort>,
}
pub(crate) struct BoundActorQueue {
    pub(crate) queue: Box<BoundQueue>,
    pub(crate) handoff: Arc<HandoffCell>,
    pub(crate) panic: PanicSink,
    pub(crate) result: Arc<dyn ActorTransportResultPort>,
}
/// Ordinary refusal returns the entire original generic host job without
/// destroying rich captures or issuing another Work/result obligation.
pub(crate) fn bind_send(job: PreparedEffectJob) -> Result<SendJob, PreparedEffectJob> {
    let PreparedEffectJob {
        effect,
        exit,
        result,
        captures,
    } = job;
    match captures.downcast::<BoundActorSend>() {
        Ok(bound) => {
            let BoundActorSend {
                send,
                handoff,
                panic,
                result: port,
            } = *bound;
            Ok(SendJob {
                leg: TransportLeg {
                    exit,
                    handoff,
                    panic,
                    result: Box::new(ActorResultObligation {
                        routing: effect,
                        original: result,
                        port,
                    }),
                },
                bound: send,
            })
        }
        Err(captures) => Err(PreparedEffectJob {
            effect,
            exit,
            result,
            captures,
        }),
    }
}
pub(crate) fn bind_queue(job: PreparedEffectJob) -> Result<QueueJob, PreparedEffectJob> {
    let PreparedEffectJob {
        effect,
        exit,
        result,
        captures,
    } = job;
    match captures.downcast::<BoundActorQueue>() {
        Ok(bound) => {
            let BoundActorQueue {
                queue,
                handoff,
                panic,
                result: port,
            } = *bound;
            Ok(QueueJob {
                leg: TransportLeg {
                    exit,
                    handoff,
                    panic,
                    result: Box::new(ActorResultObligation {
                        routing: effect,
                        original: result,
                        port,
                    }),
                },
                bound: queue,
            })
        }
        Err(captures) => Err(PreparedEffectJob {
            effect,
            exit,
            result,
            captures,
        }),
    }
}
