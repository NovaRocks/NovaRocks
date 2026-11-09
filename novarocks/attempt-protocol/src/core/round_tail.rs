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

//! Drive the original serial tail between the common Root cut and QA
//! settlement. Each call may hand off one effect from the charged step.

use super::{
    AckSettlementProgress, Core, schedule, stop::StopProgress, subscriptions::SubscriptionStep,
};
use crate::{
    capacity::StepContext,
    dispatch::BatchDispatch,
    effect::{AttemptEffect, EffectBody, EffectClass, Publication},
    error::TaskExecutionError,
    execution::QueueExpiryProgress,
    failure::AttemptFailure,
    publication::protocol_cursor::PublicationProgress,
};
use novarocks_coordination_contract::{AttemptActionId, AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::TaskOperationId;

#[derive(Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    SelectPublication,
    Publish,
    AwaitPublication,
    Cleanup,
    AbortGrants,
    LifecycleGrants,
    CreateGrants,
    QueueExpiry,
    Dispatch,
    Subscriptions,
    BeginSettlement,
    Settle,
}

#[derive(Debug, Default)]
pub(super) struct RoundTail {
    phase: Phase,
    dispatch: bool,
    abort_after: Option<TaskOperationId>,
    abort_admitted: bool,
    lifecycle_after: Option<TaskOperationId>,
    create_after: Option<TaskOperationId>,
}

impl RoundTail {
    pub(super) fn suppress_dispatch(&mut self) {
        self.dispatch = false;
    }

    pub(super) fn waiting_for_publication(&self) -> bool {
        self.phase == Phase::AwaitPublication
    }

    pub(super) fn is_idle(&self) -> bool {
        self.phase == Phase::Idle
    }
}

#[derive(Debug)]
pub(crate) enum RoundTailStep {
    Yield,
    AwaitPublication,
    AwaitQa,
    Effect(AttemptEffect),
    ExpiredAbort {
        release: AttemptEffect,
        operation: TaskOperationId,
    },
    RetiredAuthorization(TaskOperationId),
    Complete(Result<(), AttemptFailure>),
}

impl Core {
    /// Start at the original publication point. The selected common input
    /// and its Root effects have already been handled. Receive-only rounds
    /// retain publication and observation but do not send queued batches.
    pub(crate) fn begin_round_tail(&mut self, dispatch: bool) -> Result<(), TaskExecutionError> {
        if !self.round_tail.is_idle()
            || self.publication.has_pending_apply()
            || self.subscriptions.assembling()
        {
            return Err(schedule("the original round tail is already selected"));
        }
        let can_run = self
            .acknowledgements
            .run_round_tail()
            .map_err(|_| schedule("round tail began before the original ACK prefix completed"))?;
        self.round_tail = RoundTail {
            phase: if can_run && self.round_failure.is_none() {
                Phase::SelectPublication
            } else {
                Phase::BeginSettlement
            },
            dispatch,
            abort_after: None,
            abort_admitted: false,
            lifecycle_after: None,
            create_after: None,
        };
        Ok(())
    }

    /// The adapter has reserved one outbound effect slot, and can call again
    /// on the same charged step. Exhaustion leaves the exact current cursor.
    pub(crate) fn advance_round_tail(
        &mut self,
        now: MonotonicInstant,
        action: AttemptActionId,
        token: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Result<RoundTailStep, TaskExecutionError> {
        self.execution.set_now(now)?;
        loop {
            match self.advance_tail_phase(now, action, token, context) {
                Ok(Some(step)) => return Ok(step),
                Ok(None) => {}
                Err(error)
                    if !matches!(
                        self.round_tail.phase,
                        Phase::Idle | Phase::BeginSettlement | Phase::Settle
                    ) && self.acknowledgements.run_round_tail().is_ok()
                        && !self.publication.has_pending_apply()
                        && !self.subscriptions.assembling() =>
                {
                    // An ordinary tail failure still drains the whole old QA
                    // journal. Invariant failures in a live cursor fail closed.
                    self.acknowledgements
                        .fail_round_tail(error)
                        .expect("the original ACK prefix has already finished");
                    self.round_tail.phase = Phase::BeginSettlement;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn advance_tail_phase(
        &mut self,
        now: MonotonicInstant,
        action: AttemptActionId,
        token: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Result<Option<RoundTailStep>, TaskExecutionError> {
        if matches!(
            self.round_tail.phase,
            Phase::Cleanup | Phase::Dispatch | Phase::BeginSettlement
        ) && self.execution.has_split_notices()
        {
            if !context.charge(1) {
                return Ok(Some(RoundTailStep::Yield));
            }
            let body = self
                .execution
                .take_split_notice()
                .expect("original split notice");
            return Ok(Some(RoundTailStep::Effect(AttemptEffect {
                id: action,
                class: EffectClass::Settlement,
                body,
            })));
        }
        match self.round_tail.phase {
            Phase::Idle => return Err(schedule("no original round tail is selected")),
            Phase::SelectPublication => {
                self.round_tail.phase = if self.stop.business_closed() && self.publication.failed()
                {
                    Phase::Cleanup
                } else if self.select_protocol_publication()? {
                    Phase::Publish
                } else {
                    Phase::Cleanup
                };
            }
            Phase::Publish => match self.advance_protocol_publication(context)? {
                PublicationProgress::Yield => return Ok(Some(RoundTailStep::Yield)),
                PublicationProgress::Ready => {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    let publication = self
                        .take_protocol_publication(action)
                        .expect("selected original publication is ready");
                    self.round_tail.phase = Phase::AwaitPublication;
                    return Ok(Some(RoundTailStep::Effect(AttemptEffect {
                        id: action,
                        class: EffectClass::Publication,
                        body: EffectBody::Publish(Publication::ProtocolBatch(publication)),
                    })));
                }
                _ => return Err(schedule("selected publication cannot be assembled")),
            },
            Phase::AwaitPublication => {
                if self.publication.has_pending_apply() {
                    return Ok(Some(RoundTailStep::AwaitPublication));
                }
                self.round_tail.phase = if self.round_failure.is_some() {
                    Phase::BeginSettlement
                } else {
                    Phase::Cleanup
                };
            }
            Phase::Cleanup => {
                match self.stop.advance(&mut self.execution, action, context) {
                    StopProgress::Yield => return Ok(Some(RoundTailStep::Yield)),
                    StopProgress::Effect(effect) => return Ok(Some(RoundTailStep::Effect(effect))),
                    StopProgress::Done => {}
                }
                self.execution.try_begin_normal_drain();
                if self.candidates.enabled {
                    let running =
                        !self.stop.business_closed() && !self.execution.normal_drain_started();
                    if self.execution.split_pump_lifecycle_due(running) {
                        if !context.charge(1) {
                            return Ok(Some(RoundTailStep::Yield));
                        }
                        let body = self
                            .execution
                            .split_pump_lifecycle(running)
                            .expect("original split pump");
                        return Ok(Some(RoundTailStep::Effect(AttemptEffect {
                            id: action,
                            class: if running {
                                EffectClass::Business
                            } else {
                                EffectClass::Cleanup
                            },
                            body,
                        })));
                    }
                    if !running && let Some(task) = self.execution.split_task_after(None) {
                        if !context.charge(1) {
                            return Ok(Some(RoundTailStep::Yield));
                        }
                        let effect = self.execution.prepare_split_delivery(task, action)?;
                        debug_assert!(effect.is_none());
                        return Ok(None);
                    }
                }
                if self.execution.has_released_native_input() {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    let grant = self
                        .execution
                        .take_released_native_input()
                        .expect("original returned grant");
                    return Ok(Some(RoundTailStep::Effect(release(action, grant))));
                }
                if self.execution.has_task_input_cleanup() {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    if let Some(body) = self.execution.advance_task_input_cleanup() {
                        return Ok(Some(RoundTailStep::Effect(AttemptEffect {
                            id: action,
                            class: EffectClass::Cleanup,
                            body,
                        })));
                    }
                    return Ok(None);
                }
                let progressed = if self.execution.has_pending_terminal_cleanup() {
                    self.execution.advance_terminal_cleanup(context)?
                } else if self.execution.has_pending_normal_drain() {
                    self.execution.advance_normal_drain(context)?
                } else if self.execution.has_pending_stage_propagation() {
                    self.execution.propagate_stage_release(context)?
                } else if self.execution.has_pending_edge_derivation() {
                    self.execution.advance_edge_derivation(context)?
                } else {
                    self.round_tail.phase = Phase::AbortGrants;
                    return Ok(None);
                };
                if !progressed {
                    return Ok(Some(RoundTailStep::Yield));
                }
            }
            Phase::AbortGrants => {
                if !self.round_tail.abort_admitted && self.aborts.has_pending() {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    self.round_tail.abort_admitted = true;
                    let effect = self.aborts.admit_next().expect("original pending Abort");
                    return Ok(Some(RoundTailStep::Effect(effect)));
                }
                if let Some(operation) = self.aborts.next_granted(self.round_tail.abort_after) {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    self.aborts.advance_grant(&mut self.execution, operation)?;
                    self.round_tail.abort_after = Some(operation);
                } else {
                    self.round_tail.phase = Phase::LifecycleGrants;
                }
            }
            Phase::LifecycleGrants => {
                if let Some(operation) = self
                    .execution
                    .next_lifecycle_grant(self.round_tail.lifecycle_after)
                {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    self.round_tail.lifecycle_after = Some(operation);
                    if let crate::execution::CreateGrantProgress::Release(grant) =
                        self.execution.advance_lifecycle_grant(operation)?
                    {
                        return Ok(Some(RoundTailStep::Effect(release(action, grant))));
                    }
                } else {
                    self.round_tail.phase = Phase::CreateGrants;
                }
            }
            Phase::CreateGrants => {
                if let Some(operation) = self
                    .execution
                    .next_create_grant(self.round_tail.create_after)
                {
                    if !context.charge(1) {
                        return Ok(Some(RoundTailStep::Yield));
                    }
                    self.round_tail.create_after = Some(operation);
                    if let crate::execution::CreateGrantProgress::Release(grant) =
                        self.execution.advance_create_grant(operation)?
                    {
                        return Ok(Some(RoundTailStep::Effect(release(action, grant))));
                    }
                } else if self.round_tail.dispatch {
                    self.execution.begin_queue_expiry()?;
                    self.round_tail.phase = Phase::QueueExpiry;
                } else {
                    self.round_tail.phase = Phase::Subscriptions;
                }
            }
            Phase::QueueExpiry => {
                // Reserve the output unit before taking a possibly expired
                // grant. Retain the exact expiry clock and traversal on yield.
                if !context.charge(1) {
                    return Ok(Some(RoundTailStep::Yield));
                }
                match self.execution.advance_queue_expiry(context)? {
                    QueueExpiryProgress::Yield => return Ok(Some(RoundTailStep::Yield)),
                    QueueExpiryProgress::ReleaseQueuedInput {
                        grant,
                        definitely_unsent_abort,
                    } => {
                        let release = release(action, grant);
                        return Ok(Some(match definitely_unsent_abort {
                            Some(operation) => RoundTailStep::ExpiredAbort { release, operation },
                            None => RoundTailStep::Effect(release),
                        }));
                    }
                    QueueExpiryProgress::Complete(result) => {
                        result?;
                        self.round_tail.phase = Phase::Dispatch;
                    }
                }
            }
            Phase::Dispatch if !self.round_tail.dispatch => {
                self.round_tail.phase = Phase::Subscriptions;
            }
            Phase::Dispatch => {
                if !context.charge(1) {
                    return Ok(Some(RoundTailStep::Yield));
                }
                match self.advance_dispatch(action, context)? {
                    BatchDispatch::Ready(effect) => return Ok(Some(RoundTailStep::Effect(effect))),
                    BatchDispatch::Yield => return Ok(Some(RoundTailStep::Yield)),
                    BatchDispatch::Blocked | BatchDispatch::Empty => {
                        self.round_tail.phase = Phase::Subscriptions;
                    }
                }
            }
            Phase::Subscriptions => match self.advance_subscription(now, action, token, context)? {
                SubscriptionStep::Idle => self.round_tail.phase = Phase::BeginSettlement,
                SubscriptionStep::Yield => return Ok(Some(RoundTailStep::Yield)),
                SubscriptionStep::Effect(effect) => return Ok(Some(RoundTailStep::Effect(effect))),
            },
            Phase::BeginSettlement => {
                self.begin_ack_settlement(None)?;
                self.round_tail.phase = Phase::Settle;
            }
            Phase::Settle => match self.advance_ack_settlement(token, action, context)? {
                AckSettlementProgress::Waiting => return Ok(Some(RoundTailStep::AwaitQa)),
                AckSettlementProgress::Yield => return Ok(Some(RoundTailStep::Yield)),
                AckSettlementProgress::Request(effect) => {
                    return Ok(Some(RoundTailStep::Effect(effect)));
                }
                AckSettlementProgress::Consumed {
                    retire: Some(operation),
                } => {
                    return Ok(Some(RoundTailStep::RetiredAuthorization(operation)));
                }
                AckSettlementProgress::Consumed { retire: None } => {}
                AckSettlementProgress::Complete(result) => {
                    self.round_tail = RoundTail::default();
                    return Ok(Some(RoundTailStep::Complete(result)));
                }
            },
        }
        Ok(None)
    }

    /// Awaiting an actual result has its own wake. Idle queue/subscription
    /// timers must not spin while the original ordered tail is parked.
    pub(crate) fn next_round_deadline(&self) -> Option<MonotonicInstant> {
        match self.round_tail.phase {
            Phase::AwaitPublication if self.publication.has_pending_apply() => None,
            Phase::Idle | Phase::Settle if self.authorizations.waiting_for_reply() => None,
            Phase::Idle if self.acknowledgements.selected_source().is_some() => None,
            Phase::Idle => [
                self.execution.queue_deadline(),
                self.next_subscription_deadline(),
            ]
            .into_iter()
            .flatten()
            .min(),
            _ => Some(self.execution.monotonic_now()),
        }
    }
}

fn release(action: AttemptActionId, grant: crate::input::QueuedInputGrantRef) -> AttemptEffect {
    AttemptEffect {
        id: action,
        class: EffectClass::Cleanup,
        body: EffectBody::ReleaseQueuedInput { grant },
    }
}
