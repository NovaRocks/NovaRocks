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
    ActorSendReports,
    call_source::{AttemptCallReply, BoundAttemptCall},
    covered::BoundSubscription,
    queued_job::{BoundQueue, QueueJob, run as run_queue},
    send::{BoundSend, SendJob, run as run_send},
    source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg},
};
use crate::{
    control_runtime::{
        actor::FrontendEffect,
        host::{EffectExecutor, PreparedEffectJob},
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::host::{
        custody::{
            CustodyError, CustodyRefusal, OriginalTransportJoinObligation, TransportJoinHandoff,
            TransportJoinRegistration, TransportRetirement,
        },
        pumps::{BoundPumpWork, PumpOutcome},
    },
};
use novarocks_attempt_protocol::effect::EffectBody;
use novarocks_control_runtime::{
    actor_turn::InputAuthor,
    causal_input::CausalInputObligation,
    exit_custody::ExitCustody,
    exit_work::{
        ExitWorkError, PanicSink, run_blocking, run_blocking_in_generation, run_future,
        run_future_in_generation,
    },
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::identity::{ConsumerId, Generation};
use std::{alloc::Layout, collections::VecDeque, sync::Arc};
use tokio::runtime::Handle;

/// Holds only the authority to stage a body outcome in its original record.
struct ActorResultObligation(TransportJoinRegistration);
struct UnprovenBodyOutcome {
    _original: BodyOutcome,
}
impl TransportCompletionObligation for ActorResultObligation {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome) {
        stage_actor_outcome(&self.0, outcome);
    }
}
fn stage_actor_outcome(registration: &TransportJoinRegistration, outcome: BodyOutcome) {
    if let Err(original) = registration.stage_actor_outcome(outcome) {
        std::panic::resume_unwind(Box::new(UnprovenBodyOutcome {
            _original: original,
        }));
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
    Call(BoundAttemptCall),
    Subscription(Box<BoundSubscription>),
    Pump {
        work: Box<BoundPumpWork>,
        generation: Generation,
    },
}
impl ActorTransportWork {
    fn matches(&self, effect: &Arc<AttemptEffectEnvelope>) -> bool {
        match (self, &effect.protocol.body) {
            (Self::Send(bound), EffectBody::SendBatch { batch, reservation }) => {
                let items = batch.operations.len();
                bound.resolved.matches_effect(effect)
                    && bound.reports.actor().is_some_and(|reports| {
                        reports.matches(
                            effect.protocol.id,
                            &bound.resolved.keys,
                            &bound.resolved.operations,
                        )
                    })
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
            (Self::Call(call), _) => call.matches(effect),
            (Self::Subscription(bound), _) => bound.matches(effect),
            (Self::Pump { work, .. }, _) => work.matches(effect),
            _ => false,
        }
    }

    fn send_reports(&self) -> Option<Arc<ActorSendReports>> {
        match self {
            Self::Send(send) => send.reports.actor().cloned(),
            Self::Admit(_) | Self::Call(_) | Self::Subscription(_) | Self::Pump { .. } => None,
        }
    }
}

/// The Source quotes the actual allocations, including the selected boxed
/// native binding and the distinct actor-result wrapper.
pub(crate) fn actor_transport_layouts(
    send: bool,
) -> Result<[Layout; 6], novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    use novarocks_control_runtime::exit_work::HandoffCell;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    Ok([
        arc_layout(Layout::new::<AttemptEffectEnvelope>())?,
        arc_layout(Layout::new::<HandoffCell>())?,
        Layout::new::<BoundActorTransport>(),
        Layout::new::<ActorResultObligation>(),
        Layout::new::<TransportRetirement>(),
        if send {
            Layout::new::<BoundSend>()
        } else {
            Layout::new::<BoundQueue>()
        },
    ])
}

/// Prepare the job and its matching host retirement row from the same
/// already-admitted Work. The binder uses its actual Join registration to
/// attach any original queue window and WC grants.
pub(crate) fn prepare_actor_transport(
    effect: Arc<AttemptEffectEnvelope>,
    result: CausalInputObligation<crate::control_runtime::actor::FrontendActor>,
    original: ReportedWork,
    cleanup: VecDeque<ReportedWork>,
    observer: ConsumerId,
    bind: impl FnOnce(TransportJoinRegistration) -> ActorTransportWork,
) -> Result<PreparedEffectJob, TransportPreparationRefusal> {
    prepare_with_owner(effect, result, original, cleanup, observer, |owner| {
        bind(owner.registration())
    })
}

pub(crate) fn prepare_actor_pump(
    effect: Arc<AttemptEffectEnvelope>,
    result: CausalInputObligation<crate::control_runtime::actor::FrontendActor>,
    original: ReportedWork,
    observer: ConsumerId,
    generation: Generation,
    bind: impl FnOnce(ExitCustody) -> BoundPumpWork,
) -> Result<PreparedEffectJob, TransportPreparationRefusal> {
    prepare_with_owner(
        effect,
        result,
        original,
        VecDeque::new(),
        observer,
        |owner| ActorTransportWork::Pump {
            work: Box::new(bind(owner.exit_custody())),
            generation,
        },
    )
}

fn prepare_with_owner(
    effect: Arc<AttemptEffectEnvelope>,
    result: CausalInputObligation<crate::control_runtime::actor::FrontendActor>,
    original: ReportedWork,
    cleanup: VecDeque<ReportedWork>,
    observer: ConsumerId,
    bind: impl FnOnce(&OriginalTransportJoinObligation) -> ActorTransportWork,
) -> Result<PreparedEffectJob, TransportPreparationRefusal> {
    let result_author = result.event_author().expect("original prepaid result");
    let panic = PanicSink::new();
    let (owner, exit, launch) =
        match OriginalTransportJoinObligation::prepare(original, observer, panic.clone()) {
            Ok(prepared) => prepared,
            Err(refused) => {
                return Err(TransportPreparationRefusal {
                    refused,
                    result,
                    cleanup,
                });
            }
        };
    let work = bind(&owner);
    Ok(PreparedEffectJob {
        effect: FrontendEffect::Attempt(effect.clone()),
        exit,
        result,
        captures: Box::new(BoundActorTransport {
            effect,
            work,
            launch,
            panic,
            result_author,
        }),
        transport: Some(Box::new(TransportRetirement::new(owner, cleanup, observer))),
    })
}

#[must_use]
pub(crate) struct TransportPreparationRefusal {
    pub(crate) refused: CustodyRefusal,
    pub(crate) result: CausalInputObligation<crate::control_runtime::actor::FrontendActor>,
    pub(crate) cleanup: VecDeque<ReportedWork>,
}

pub(crate) enum ActorTransportHandoff {
    Spawned,
    Failed(CustodyError),
}

pub(crate) struct ActorTransportExecutor(pub(crate) Handle);
impl EffectExecutor for ActorTransportExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        // A failed launch has transferred the original result position. The
        // Source receives that same raw failure through the original owner.
        handoff_actor_transport(&self.0, job).map(|_| ())
    }
}

/// Type/origin refusal retains the entire original job. Spawned proves only
/// that the Source now has the actual Join. A factory failure is retained by
/// that same Source for failure cleanup and is not successful handoff.
pub(crate) fn handoff_actor_transport(
    runtime: &Handle,
    job: PreparedEffectJob,
) -> Result<ActorTransportHandoff, PreparedEffectJob> {
    if job.transport.is_some() {
        return Err(job);
    }
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
        transport,
    } = job;
    let bound = captures
        .downcast_ref::<BoundActorTransport>()
        .expect("checked actor transport binding");
    if let Err(result) = bound.launch.bind_actor_result(
        bound.effect.clone(),
        &bound.result_author,
        result,
        bound.work.send_reports(),
    ) {
        return Err(PreparedEffectJob {
            effect,
            exit,
            result,
            captures,
            transport,
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
    let leg = |exit, registration, panic| TransportLeg {
        exit,
        handoff: effect
            .handoff()
            .expect("committed transport handoff")
            .clone(),
        panic,
        result: Box::new(ActorResultObligation(registration)),
    };
    let spawned = match work {
        ActorTransportWork::Send(bound) => launch.spawn(runtime, exit, |exit, registration| {
            run_send(SendJob {
                leg: leg(exit, registration.clone(), panic),
                registration,
                bound,
            })
        }),
        ActorTransportWork::Admit(bound) => launch.spawn(runtime, exit, |exit, registration| {
            run_queue(QueueJob {
                leg: leg(exit, registration, panic),
                bound,
            })
        }),
        ActorTransportWork::Subscription(bound) => {
            let fact = bound.exit_fact();
            launch.spawn(runtime, exit, |exit, registration| async move {
                let outcome = match run_future(exit, bound.run(), panic).await {
                    Ok(Ok(()))
                    | Ok(Err(
                        crate::task_execution::host::subscriptions::SubscriptionRunError::Ingress(
                            crate::task_execution::host::observations::ObservationError::Closed,
                        ),
                    ))
                    | Err(ExitWorkError::NotStarted(_)) => {
                        BodyOutcome::Call(AttemptCallReply::Input {
                            input: novarocks_attempt_protocol::input::AttemptInput::Executor(fact),
                            failure: None,
                        })
                    }
                    Ok(Err(error)) => BodyOutcome::RawFailure(Box::new(error)),
                    Err(error) => BodyOutcome::RawFailure(Box::new(error)),
                };
                stage_actor_outcome(&registration, outcome);
            })
        }
        ActorTransportWork::Pump { work, generation } if work.runs_blocking() => launch
            .spawn_blocking(runtime, exit, |exit, registration| {
                let work = run_blocking_in_generation(
                    exit,
                    move || work.run_blocking(),
                    panic,
                    generation,
                );
                move || stage_actor_outcome(&registration, pump_outcome(work.run()))
            }),
        ActorTransportWork::Pump { work, generation } => {
            launch.spawn(runtime, exit, |exit, registration| async move {
                let outcome = run_future_in_generation(exit, work.run(), panic, generation).await;
                stage_actor_outcome(&registration, pump_outcome(outcome));
            })
        }
        ActorTransportWork::Call(bound) if effect.is_transport_release() => {
            launch.spawn_blocking(runtime, exit, |exit, registration| {
                let work = run_blocking(exit, move || bound.dispose(), panic);
                move || {
                    let outcome = match work.run() {
                        Ok(()) => BodyOutcome::Call(AttemptCallReply::NoInput),
                        Err(error) => BodyOutcome::RawFailure(Box::new(error)),
                    };
                    stage_actor_outcome(&registration, outcome);
                }
            })
        }
        ActorTransportWork::Call(bound) => {
            let confirms_read = effect.is_read_confirmation();
            launch.spawn(runtime, exit, |exit, registration| async move {
                let outcome = match run_future(exit, bound.run(effect), panic).await {
                    Ok(Ok(reply)) => BodyOutcome::Call(reply),
                    Ok(Err(error)) => BodyOutcome::RawFailure(error),
                    Err(ExitWorkError::NotStarted(_)) if confirms_read => {
                        // The start fence rejected the exact read and its
                        // captures already exited. There is no read fact.
                        BodyOutcome::Call(AttemptCallReply::NoInput)
                    }
                    Err(error) => BodyOutcome::RawFailure(Box::new(error)),
                };
                stage_actor_outcome(&registration, outcome);
            })
        }
    };
    Ok(match spawned {
        Ok(()) => ActorTransportHandoff::Spawned,
        Err(error) => ActorTransportHandoff::Failed(error),
    })
}

fn pump_outcome(
    result: Result<
        Result<
            Option<novarocks_attempt_protocol::input::ExecutorFact>,
            crate::task_execution::host::pumps::PumpRecordError,
        >,
        ExitWorkError,
    >,
) -> BodyOutcome {
    match result {
        Ok(Ok(fact)) => BodyOutcome::Pump(PumpOutcome::Completed(fact)),
        Err(ExitWorkError::NotStarted(_)) => BodyOutcome::Pump(PumpOutcome::Fenced),
        Ok(Err(error)) => BodyOutcome::RawFailure(Box::new(error)),
        Err(error) => BodyOutcome::RawFailure(Box::new(error)),
    }
}
