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
        actor::FrontendEffect, host::PreparedEffectJob, transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::host::custody::{
        CustodyError, TransportJoinHandoff, TransportJoinRegistration,
    },
};
use novarocks_attempt_protocol::effect::EffectBody;
use novarocks_control_runtime::{actor_turn::InputAuthor, exit_work::PanicSink};
use std::{alloc::Layout, sync::Arc};
use tokio::runtime::Handle;

/// Holds only the authority to stage a body outcome in its original record.
struct ActorResultObligation(TransportJoinRegistration);
struct UnprovenBodyOutcome {
    _original: BodyOutcome,
}
impl TransportCompletionObligation for ActorResultObligation {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome) {
        if let Err(original) = self.0.stage_actor_outcome(outcome) {
            std::panic::resume_unwind(Box::new(UnprovenBodyOutcome {
                _original: original,
            }));
        }
    }
}
/// The original Source binds the same admitted job, launch authority and
/// canonical grants before returning it to the common host.
pub(crate) struct BoundActorTransport {
    pub(crate) effect: Arc<AttemptEffectEnvelope>,
    pub(crate) work: ActorTransportWork,
    pub(crate) launch: TransportJoinHandoff,
    pub(crate) panic: PanicSink,
    /// Exact pre-paid position selected by the Source, which can belong to
    /// a purpose author admitted before the send's step guard is bound.
    pub(crate) result_author: InputAuthor,
}
pub(crate) enum ActorTransportWork {
    Send(Box<BoundSend>),
    Admit(Box<BoundQueue>),
}
impl ActorTransportWork {
    fn matches(&self, effect: &Arc<AttemptEffectEnvelope>) -> bool {
        match (self, &effect.protocol.body) {
            (Self::Send(bound), EffectBody::SendBatch { batch, reservation }) => {
                let items = batch.operations.len();
                bound.resolved.matches_effect(effect)
                    && items != 0
                    && bound.queue.parent_reservation().get() == reservation.0.get()
                    && bound.backend == batch.backend
                    && bound.control_progress == batch.control_progress
                    && items == batch.queued_inputs.len()
                    && items == bound.resolved.keys.len()
                    && items == bound.resolved.operations.len()
                    && items == bound.resolved.queued_inputs.len()
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
pub(crate) fn actor_transport_layouts(
    send: bool,
) -> Result<[Layout; 5], novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    use novarocks_control_runtime::exit_work::HandoffCell;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    Ok([
        arc_layout(Layout::new::<AttemptEffectEnvelope>())?,
        arc_layout(Layout::new::<HandoffCell>())?,
        Layout::new::<BoundActorTransport>(),
        Layout::new::<ActorResultObligation>(),
        if send {
            Layout::new::<BoundSend>()
        } else {
            Layout::new::<BoundQueue>()
        },
    ])
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
        || !bound.work.matches(effect)
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
    let bound = captures
        .downcast_ref::<BoundActorTransport>()
        .expect("checked actor transport binding");
    if let Err(result) =
        bound
            .launch
            .bind_actor_result(bound.effect.clone(), &bound.result_author, result)
    {
        return Err(PreparedEffectJob {
            effect,
            exit,
            result,
            captures,
        });
    }
    let BoundActorTransport {
        work,
        launch,
        panic,
        effect,
        result_author: _,
    } = *captures
        .downcast::<BoundActorTransport>()
        .expect("checked actor transport binding");
    let handoff = effect
        .handoff()
        .expect("committed transport handoff")
        .clone();
    let leg = |exit, registration| TransportLeg {
        exit,
        handoff,
        panic,
        result: Box::new(ActorResultObligation(registration)),
    };
    let spawned = match work {
        ActorTransportWork::Send(bound) => launch.spawn(runtime, exit, |exit, registration| {
            run_send(SendJob {
                leg: leg(exit, registration.clone()),
                registration,
                bound,
            })
        }),
        ActorTransportWork::Admit(bound) => launch.spawn(runtime, exit, |exit, registration| {
            run_queue(QueueJob {
                leg: leg(exit, registration),
                bound,
            })
        }),
    };
    Ok(match spawned {
        Ok(()) => ActorTransportHandoff::Spawned,
        Err(error) => ActorTransportHandoff::Failed(error),
    })
}
