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

//! Select and consume the original mailbox claim at the protocol boundary.

use super::{Core, RootControlApplication, SealedRootCut, round_tail::RoundTailStep, schedule};
use crate::{
    AttemptCore,
    authorization::gate::QaReply,
    capacity::StepContext,
    effect::{AttemptEffect, EffectBody, EffectClass, Publication},
    error::TaskExecutionError,
    execution::CreateGrantProgress,
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceId, ExecutorFact,
        PublicationApplied,
    },
    root_control::SealSettlement,
    start::AttemptStart,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRequestToken, AttemptRootProjection, AttemptToLogical,
    AttemptToLogicalBody, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::TaskOperationId;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputSelection {
    Prefer(AttemptSourceId),
    Only(AttemptSourceId),
}

#[derive(Debug)]
pub struct AttemptStep {
    pub selection: InputSelection,
    pub effect: Option<AttemptEffect>,
    pub root_control_processed: Option<AttemptInputId>,
    pub definitely_unsent_abort: Option<TaskOperationId>,
    pub retired_authorization: Option<TaskOperationId>,
    /// The original round result goes to the terminal controller.
    pub round_result: Option<Result<(), AttemptFailure>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Bootstrap,
    Idle,
    Native,
    Common {
        source_ready: bool,
    },
    Root,
    AwaitRoot(AttemptActionId),
    Tail,
}

#[derive(Debug)]
struct RootWork {
    projection: Option<AttemptRootProjection>,
    seal: Option<SealSettlement>,
    publication_failure: Option<AttemptFailure>,
}

#[derive(Debug, Default)]
pub(super) struct Turn {
    phase: Phase,
    next: u64,
    root: Option<RootWork>,
}

impl Turn {
    fn issue(&mut self) -> Result<(AttemptActionId, AttemptRequestToken), TaskExecutionError> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| schedule("attempt step identities exhausted"))?;
        let id = NonZeroU64::new(self.next).expect("positive attempt identity");
        Ok((AttemptActionId::new(id), AttemptRequestToken::new(id)))
    }
}

impl AttemptCore for Core {
    fn new(start: AttemptStart) -> Result<Self, AttemptFailure> {
        Core::new(start)
    }

    fn handle(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
    ) -> Result<AttemptStep, TaskExecutionError> {
        self.execution.set_now(now)?;
        let units = match &input {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => batch.items().len(),
            AttemptInput::Executor(ExecutorFact::Withdrawn { .. }) => self
                .maximum_send_members
                .checked_mul(2)
                .ok_or_else(|| schedule("withdrawal input work quote overflow"))?,
            _ => 1,
        };
        if !context.charge(units) {
            return Err(schedule(
                "original step cannot apply its complete primary input",
            ));
        }
        let mut result = AttemptStep {
            selection: self.input_selection(),
            effect: None,
            root_control_processed: None,
            definitely_unsent_abort: None,
            retired_authorization: None,
            round_result: None,
        };
        self.apply_claim(now, claim, input, context, &mut result)?;
        result.selection = self.input_selection();
        Ok(result)
    }

    fn next_deadline(&self) -> Option<MonotonicInstant> {
        match self.turn.phase {
            Phase::Bootstrap
            | Phase::Native
            | Phase::AwaitRoot(_)
            | Phase::Common { source_ready: true } => None,
            Phase::Idle | Phase::Tail => self.next_round_deadline(),
            Phase::Common {
                source_ready: false,
            }
            | Phase::Root => Some(self.execution.monotonic_now()),
        }
    }
}

impl Core {
    fn input_selection(&self) -> InputSelection {
        use InputSelection::{Only, Prefer};
        match self.turn.phase {
            Phase::Bootstrap | Phase::Root => Only(self.sources.actor),
            Phase::Native => Only(self.sources.native),
            Phase::Common { .. } => Only(self.sources.observation),
            Phase::AwaitRoot(_) => Only(self.sources.publication),
            Phase::Tail
                if self.round_tail.waiting_for_publication()
                    && self.publication.has_pending_apply() =>
            {
                Only(self.sources.publication)
            }
            Phase::Tail | Phase::Idle if self.authorizations.waiting_for_reply() => {
                Only(self.sources.qa)
            }
            Phase::Tail => Only(self.sources.actor),
            Phase::Idle => Prefer(self.sources.native),
        }
    }

    fn apply_claim(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let own = claim.source == self.sources.actor;
        let timer = own && matches!(input, AttemptInput::Timer);
        match self.turn.phase {
            Phase::Bootstrap => {
                if !own || !matches!(input, AttemptInput::Bootstrap) {
                    return Err(schedule("attempt requires its original bootstrap input"));
                }
                self.turn.phase = Phase::Idle;
            }
            Phase::Idle => {
                if claim.source == self.sources.qa
                    && let AttemptInput::Port(port) = &input
                    && self.authorizations.waiting_for_reply()
                {
                    return self.accept_qa(port.clone(), result);
                }
                let cut = claim
                    .native_cut
                    .filter(|cut| cut.source == self.sources.native)
                    .ok_or_else(|| schedule("claim lacks its original Native pre-pop cut"))?;
                if cut.tail.is_some() {
                    if !timer && claim.source != self.sources.native {
                        return Err(schedule("primary input crossed the original Native prefix"));
                    }
                    self.begin_native_prefix(now, cut)?;
                    self.turn.phase = Phase::Native;
                    if !timer {
                        self.consume_native(now, claim, input, result)?;
                    }
                    return Ok(());
                }
                self.begin_native_prefix(now, cut)?;
                self.enter_common_or_skip()?;
                if self.turn.phase == Phase::Tail {
                    return Ok(());
                }
                if claim.source == self.sources.observation {
                    self.consume_common(now, claim, input, result)?;
                } else {
                    self.consume_independent(now, claim, input, result)?;
                }
            }
            Phase::Native if timer => {}
            Phase::Native => self.consume_native(now, claim, input, result)?,
            Phase::Common { .. } if timer => {
                let cut = claim
                    .selected_cut
                    .filter(|cut| cut.source == self.sources.observation)
                    .ok_or_else(|| schedule("timer lacks its selected observation Source cut"))?;
                if cut.tail.is_some() {
                    self.turn.phase = Phase::Common { source_ready: true };
                } else {
                    let seal = self.evaluate_pending_seal();
                    self.select_root(seal);
                }
            }
            Phase::Common { .. } => self.consume_common(now, claim, input, result)?,
            Phase::Root if timer => self.drive_root(context, result)?,
            Phase::AwaitRoot(_) if timer => {}
            Phase::AwaitRoot(expected) => {
                let AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: applied,
                }) = input
                else {
                    return Err(schedule("root publication awaited its actual result"));
                };
                self.verify_source(claim.source, self.sources.publication)?;
                if effect == expected
                    && (applied.is_err() || matches!(applied, Ok(PublicationApplied::RootStatus)))
                {
                    let root = self.turn.root.as_mut().expect("original root publication");
                    if let Err(failure) = applied {
                        root.publication_failure = Some(failure.clone());
                        self.round_failure.get_or_insert(failure);
                    }
                    self.turn.phase = Phase::Root;
                }
            }
            Phase::Tail => self.apply_tail(now, claim, input, context, result)?,
            _ => return Err(schedule("input crossed an unfinished Root phase")),
        }
        Ok(())
    }

    fn consume_native(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let AttemptInput::Executor(fact) = input else {
            return Err(schedule("Native Source supplied an unrelated input"));
        };
        self.verify_source(claim.source, self.sources.native)?;
        if let Some(body) = self.apply_native(now, claim.source, claim.input, &fact, |_, _| {
            Err(schedule(
                "actor Abort receipt requires its original Abort owner",
            ))
        })? {
            let (id, _) = self.turn.issue()?;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Cleanup,
                body,
            });
        }
        if self.acknowledgements.selected_source().is_none() {
            self.enter_common_or_skip()?;
        }
        Ok(())
    }

    fn enter_common_or_skip(&mut self) -> Result<(), TaskExecutionError> {
        if self.acknowledgements.run_round_tail() == Ok(true) {
            self.turn.phase = Phase::Common {
                source_ready: false,
            };
        } else {
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
        }
        Ok(())
    }

    fn consume_common(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let seal = match input {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                self.apply_observations(now, claim.source, batch)?
            }
            AttemptInput::Port(port) => {
                let RootControlApplication {
                    seal,
                    terminal_control,
                } = self.apply_root_control(now, claim.source, port)?;
                result.root_control_processed = Some(claim.input);
                if let Some((token, settled)) = terminal_control {
                    result.effect = Some(self.port_effect(
                        AttemptToLogicalBody::TerminalControlResult {
                            token,
                            result: settled.map_err(protocol_failure),
                        },
                    )?);
                }
                seal
            }
            _ => return Err(schedule("observation Source supplied an unrelated input")),
        };
        if self.acknowledgements.run_round_tail() == Ok(true) || seal.is_some() {
            self.select_root(seal);
        } else {
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
        }
        Ok(())
    }

    fn consume_independent(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        match input {
            AttemptInput::Timer => self.verify_source(claim.source, self.sources.actor)?,
            AttemptInput::CapacityReleased => {}
            AttemptInput::Executor(fact) => match fact {
                ExecutorFact::SubscriptionProgress(_)
                | ExecutorFact::ReadState(_)
                | ExecutorFact::ReadProgress(_)
                | ExecutorFact::SubscriptionExited { .. } => {
                    self.apply_subscription_input(now, &fact)?
                }
                ExecutorFact::Withdrawn { .. } | ExecutorFact::QueuedInputWithdrawn { .. } => self
                    .apply_transport_withdrawal(now, &fact, |_| {
                        Err(schedule(
                            "actor Abort withdrawal requires its original Abort owner",
                        ))
                    })?,
                ExecutorFact::QueuedInputGranted {
                    operation, grant, ..
                } => {
                    if let CreateGrantProgress::Release(grant) =
                        self.execution.observe_create_grant(operation, grant)?
                    {
                        let (id, _) = self.turn.issue()?;
                        result.effect = Some(AttemptEffect {
                            id,
                            class: EffectClass::Cleanup,
                            body: EffectBody::ReleaseQueuedInput { grant },
                        });
                    }
                }
                ExecutorFact::QueuedInputFailed {
                    operation,
                    generation,
                    failure,
                } => {
                    if self.execution.close_create_admission(operation, generation) {
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    }
                }
                _ => return Err(schedule("executor input has no active protocol owner")),
            },
            _ => return Err(schedule("input has no active protocol owner")),
        }
        Ok(())
    }

    fn select_root(&mut self, cut: Option<SealedRootCut>) {
        let (projection, seal) = cut.map_or_else(
            || {
                (
                    (!self.root.sealed())
                        .then(|| self.root_projection())
                        .flatten(),
                    None,
                )
            },
            |cut| {
                let projection = cut.root_before_seal.or_else(|| {
                    (self.acknowledgements.run_round_tail() == Ok(true))
                        .then(|| self.root_projection())
                        .flatten()
                });
                (projection, Some(cut.settlement))
            },
        );
        self.turn.root = Some(RootWork {
            projection,
            seal,
            publication_failure: None,
        });
        self.turn.phase = Phase::Root;
    }

    fn drive_root(
        &mut self,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let root = self.turn.root.as_mut().expect("selected root work");
        if root.projection.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let projection = root.projection.take().expect("selected projection");
            let (id, _) = self.turn.issue()?;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Publication,
                body: EffectBody::Publish(Publication::RootStatus(Box::new(projection))),
            });
            self.turn.phase = Phase::AwaitRoot(id);
            return Ok(());
        }
        if root.seal.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let seal = root.seal.take().expect("selected seal result");
            let settled = if let Some(failure) = root.publication_failure.take() {
                Err(failure)
            } else {
                seal.result
                    .map_err(|refusal| protocol_failure(schedule(refusal.message())))
            };
            result.effect = Some(self.port_effect(AttemptToLogicalBody::SealResult {
                token: seal.token,
                result: settled,
            })?);
        }
        self.turn.root = None;
        self.begin_round_tail(true)?;
        self.turn.phase = Phase::Tail;
        Ok(())
    }

    fn apply_tail(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        match input {
            AttemptInput::Timer => self.verify_source(claim.source, self.sources.actor)?,
            AttemptInput::Executor(ExecutorFact::PublicationApplied { effect, result }) => {
                self.verify_source(claim.source, self.sources.publication)?;
                self.protocol_publication_applied(effect, result);
            }
            AttemptInput::Port(port) => {
                self.verify_source(claim.source, self.sources.qa)?;
                self.accept_qa(port, result)?;
            }
            _ => return Err(schedule("primary input crossed the original round tail")),
        }
        let (action, token) = self.turn.issue()?;
        match self.advance_round_tail(now, action, token, context)? {
            RoundTailStep::Yield | RoundTailStep::AwaitPublication | RoundTailStep::AwaitQa => {}
            RoundTailStep::Effect(effect) => result.effect = Some(effect),
            RoundTailStep::ExpiredAbort { release, operation } => {
                result.effect = Some(release);
                result.definitely_unsent_abort = Some(operation);
            }
            RoundTailStep::RetiredAuthorization(operation) => {
                result.retired_authorization = Some(operation)
            }
            RoundTailStep::Complete(round) => {
                self.turn.phase = Phase::Idle;
                result.round_result = Some(round);
            }
        }
        Ok(())
    }

    fn accept_qa(
        &mut self,
        port: novarocks_coordination_contract::LogicalToAttempt<AttemptFailure>,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if !matches!(
            &port.body,
            LogicalToAttemptBody::AuthorizationReply { .. }
                | LogicalToAttemptBody::EstablishSettlementReply { .. }
        ) {
            return Err(schedule("QA continuation received an unrelated port reply"));
        }
        match self.apply_qa_reply(port)? {
            QaReply::ObserverComplete { retire } => result.retired_authorization = retire,
            QaReply::AuthorizationFailed { failure, .. } => {
                self.terminal.record_original_failure(failure)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn port_effect(
        &mut self,
        body: AttemptToLogicalBody<AttemptFailure>,
    ) -> Result<AttemptEffect, TaskExecutionError> {
        let (id, _) = self.turn.issue()?;
        Ok(AttemptEffect {
            id,
            class: EffectClass::Reply,
            body: EffectBody::Port(AttemptToLogical {
                activation: self.activation,
                action: id,
                body,
            }),
        })
    }

    fn verify_source(
        &self,
        actual: AttemptSourceId,
        expected: AttemptSourceId,
    ) -> Result<(), TaskExecutionError> {
        if actual != expected {
            return Err(schedule("continuation used a foreign Source"));
        }
        Ok(())
    }
}

fn protocol_failure(error: TaskExecutionError) -> AttemptFailure {
    AttemptFailure::TaskProtocol(Box::new(error))
}
