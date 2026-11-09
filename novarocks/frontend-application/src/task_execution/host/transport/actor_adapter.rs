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
    queued_job::{BoundQueue, QueueJob, run as run_queue},
    send::{BoundSend, SendJob, run as run_send},
    source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg},
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect},
        host::PreparedEffectJob,
    },
    task_execution::host::custody::{CustodyError, TransportJoinHandoff},
};
use novarocks_attempt_protocol::effect::{AttemptEffect, EffectBody};
use novarocks_control_runtime::{
    causal_input::CausalInputObligation,
    exit_work::{HandoffCell, PanicSink},
};
use std::{alloc::Layout, sync::Arc};
use tokio::runtime::Handle;

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
/// The original Source binds the same admitted job, launch authority and
/// canonical grants before returning it to the common host.
pub(crate) struct BoundActorTransport {
    pub(crate) effect: Arc<AttemptEffect>,
    pub(crate) work: ActorTransportWork,
    pub(crate) launch: TransportJoinHandoff,
    pub(crate) handoff: Arc<HandoffCell>,
    pub(crate) panic: PanicSink,
    pub(crate) result: Arc<dyn ActorTransportResultPort>,
}
pub(crate) enum ActorTransportWork {
    Send(Box<BoundSend>),
    Admit(Box<BoundQueue>),
}
impl ActorTransportWork {
    fn matches(&self, body: &EffectBody) -> bool {
        match (self, body) {
            (Self::Send(bound), EffectBody::SendBatch { batch, .. }) => {
                bound.backend == batch.backend
                    && bound.control_progress == batch.control_progress
                    && bound.resolved.keys.len() == batch.operations.len()
                    && bound.resolved.keys.len() == batch.queued_inputs.len()
            }
            (
                Self::Admit(bound),
                EffectBody::AdmitQueuedInput {
                    operation,
                    generation,
                    ..
                },
            ) => {
                bound.key.operation == operation.operation_id()
                    && bound.key.generation == *generation
                    && bound.control == operation.requires_control_progress()
            }
            _ => false,
        }
    }
}

/// The Source quotes the actual allocations, including the selected boxed
/// native binding and the distinct actor-result wrapper.
pub(crate) fn actor_transport_layouts(send: bool) -> [Layout; 3] {
    [
        Layout::new::<BoundActorTransport>(),
        Layout::new::<ActorResultObligation>(),
        if send {
            Layout::new::<BoundSend>()
        } else {
            Layout::new::<BoundQueue>()
        },
    ]
}

pub(crate) enum ActorTransportHandoff {
    Spawned,
    Failed(CustodyError),
}

/// Type/origin refusal retains the entire original job. Spawned proves only
/// that the Source now has the actual Join. A factory failure is retained by
/// that same Source for failure cleanup and is not successful handoff.
pub(crate) fn handoff_actor_transport(
    runtime: &Handle,
    job: PreparedEffectJob,
) -> Result<ActorTransportHandoff, PreparedEffectJob> {
    let Some(bound) = job.captures.downcast_ref::<BoundActorTransport>() else {
        return Err(job);
    };
    let FrontendEffect::Attempt(effect) = &job.effect else {
        return Err(job);
    };
    if !Arc::ptr_eq(effect, &bound.effect)
        || !bound.work.matches(&effect.body)
        || !bound.launch.owns_exit(&job.exit)
    {
        return Err(job);
    }
    let PreparedEffectJob {
        effect,
        exit,
        result,
        captures,
    } = job;
    let BoundActorTransport {
        work,
        launch,
        handoff,
        panic,
        result: port,
        effect: _,
    } = *captures
        .downcast::<BoundActorTransport>()
        .expect("checked actor transport binding");
    let leg = |exit| TransportLeg {
        exit,
        handoff,
        panic,
        result: Box::new(ActorResultObligation {
            routing: effect,
            original: result,
            port,
        }),
    };
    let spawned = match work {
        ActorTransportWork::Send(bound) => launch.spawn(runtime, exit, |exit| {
            run_send(SendJob {
                leg: leg(exit),
                bound,
            })
        }),
        ActorTransportWork::Admit(bound) => launch.spawn(runtime, exit, |exit| {
            run_queue(QueueJob {
                leg: leg(exit),
                bound,
            })
        }),
    };
    Ok(match spawned {
        Ok(()) => ActorTransportHandoff::Spawned,
        Err(error) => ActorTransportHandoff::Failed(error),
    })
}
