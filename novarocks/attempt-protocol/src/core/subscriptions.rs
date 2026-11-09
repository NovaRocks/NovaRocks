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

//! Route independent read-state inputs and drive original covered subscriptions.

use crate::{
    capacity::StepContext,
    effect::{AttemptEffect, EffectBody, EffectClass},
    error::TaskExecutionError,
    execution::{QueryTaskExecution, SubscriptionRequestBuild},
    ids::IdSeed,
    input::{ExecutorFact, ReceivedObservation},
    observation::controller::{
        CoveredSubscription, ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
    },
};
use novarocks_coordination_contract::{AttemptActionId, AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::ParticipationRef;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

#[derive(Debug)]
struct Row {
    controller: CoveredSubscription,
    deadline: Option<MonotonicInstant>,
    opened: Option<(AttemptActionId, NonZeroU64)>,
}

#[derive(Debug)]
struct Request {
    build: SubscriptionRequestBuild,
    open: bool,
}

#[derive(Debug)]
pub(super) struct Subscriptions {
    rows: BTreeMap<ParticipationRef, Row>,
    due: BTreeSet<(MonotonicInstant, ParticipationRef)>,
    adjust: BTreeSet<ParticipationRef>,
    request: Option<Request>,
}

#[derive(Debug)]
pub(crate) enum SubscriptionStep {
    Idle,
    Yield,
    Effect(AttemptEffect),
}

impl Subscriptions {
    pub(super) fn new(
        participations: impl Iterator<Item = ParticipationRef>,
        seed: IdSeed,
        error_budget: u32,
    ) -> Result<Self, TaskExecutionError> {
        let rows = participations
            .map(|participation| {
                Ok((
                    participation,
                    Row {
                        controller: CoveredSubscription::new(participation, seed, error_budget)?,
                        deadline: None,
                        opened: None,
                    },
                ))
            })
            .collect::<Result<_, TaskExecutionError>>()?;
        Ok(Self {
            rows,
            due: BTreeSet::new(),
            adjust: BTreeSet::new(),
            request: None,
        })
    }

    fn update<R>(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
        apply: impl FnOnce(&mut Row) -> Result<R, TaskExecutionError>,
    ) -> Result<R, TaskExecutionError> {
        let row = self
            .rows
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let result = apply(row);
        let deadline = row.controller.next_deadline(now);
        if row.deadline != deadline {
            if let Some(old) = row.deadline {
                self.due.remove(&(old, participation));
            }
            if let Some(next) = deadline {
                self.due.insert((next, participation));
            }
            row.deadline = deadline;
        }
        if row.controller.status() != SubscriptionStatus::Live {
            self.adjust.remove(&participation);
        }
        result
    }

    pub(super) fn activate(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
    ) -> Result<(), TaskExecutionError> {
        self.update(now, participation, |row| {
            row.controller.activate();
            Ok(())
        })
    }

    pub(super) fn stop(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
    ) -> Result<(), TaskExecutionError> {
        self.adjust.remove(&participation);
        self.update(now, participation, |row| {
            row.controller.stop();
            Ok(())
        })
    }

    pub(super) fn reconcile(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
    ) -> Result<(), TaskExecutionError> {
        self.update(now, participation, |row| {
            row.controller.reconcile(now);
            Ok(())
        })
    }

    pub(super) fn observe(
        &mut self,
        now: MonotonicInstant,
        observation: ReceivedObservation,
    ) -> Result<ObservationDisposition, TaskExecutionError> {
        self.update(now, observation.participation, |row| {
            let previous = row.controller.status();
            let disposition = row.controller.observe(observation)?;
            if let SubscriptionStatus::Fatal(failure) = row.controller.status()
                && previous != SubscriptionStatus::Fatal(failure)
            {
                return Err(TaskExecutionError::NativeTransport(failure.cause));
            }
            Ok(disposition)
        })
    }

    pub(super) fn applied(&mut self, participation: ParticipationRef) {
        if self.rows[&participation].controller.status() == SubscriptionStatus::Live {
            self.adjust.insert(participation);
        }
    }

    /// Retain the prior generation's close and actual exit independently. A
    /// stale exit or a result for another effect cannot enable a reconnect.
    pub(super) fn apply_independent(
        &mut self,
        now: MonotonicInstant,
        fact: &ExecutorFact,
    ) -> Result<(), TaskExecutionError> {
        match *fact {
            ExecutorFact::SubscriptionProgress(phase) => {
                self.update(now, phase.participation, |row| {
                    row.controller.observe_phase(phase)
                })
            }
            ExecutorFact::ReadState(snapshot) => self.update(now, snapshot.participation, |row| {
                row.controller.observe_read_state(snapshot)
            }),
            ExecutorFact::ReadProgress(progress) => {
                self.update(now, progress.participation, |row| {
                    row.controller.observe_read_progress(progress)
                })
            }
            ExecutorFact::SubscriptionExited {
                effect,
                participation,
                generation,
            } => self.update(now, participation, |row| {
                if row.opened == Some((effect, generation)) {
                    row.controller.observe_actual_exit(generation)?;
                    row.opened = None;
                }
                Ok(())
            }),
            _ => Err(TaskExecutionError::UnknownOperation),
        }
    }

    pub(super) fn assembling(&self) -> bool {
        self.request.is_some()
    }

    pub(super) fn next_deadline(&self, now: MonotonicInstant) -> Option<MonotonicInstant> {
        if self.assembling() || !self.adjust.is_empty() {
            return Some(now);
        }
        self.due.first().map(|&(at, _)| at)
    }

    pub(super) fn advance(
        &mut self,
        execution: &QueryTaskExecution,
        now: MonotonicInstant,
        action: AttemptActionId,
        token: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Result<SubscriptionStep, TaskExecutionError> {
        if self.request.is_none() {
            if let Some((at, participation)) = self.due.first().copied()
                && now.has_reached(at)
            {
                if !context.charge(1) {
                    return Ok(SubscriptionStep::Yield);
                }
                let minimum = execution.next_covered_generation(participation)?;
                let directive = self
                    .update(now, participation, |row| {
                        row.controller.drive(now, minimum, Some(token))
                    })?
                    .expect("indexed deadline has a real directive");
                let body = match directive {
                    SubscriptionDirective::Open { generation } => {
                        self.adjust.remove(&participation);
                        let build =
                            SubscriptionRequestBuild::new(execution, participation, generation)?;
                        self.request = Some(Request { build, open: true });
                        return self.finish_request(execution, action, context);
                    }
                    SubscriptionDirective::Close { generation } => {
                        self.adjust.remove(&participation);
                        EffectBody::CloseSubscription {
                            participation,
                            generation,
                        }
                    }
                    SubscriptionDirective::Confirm(request) => EffectBody::ConfirmReadState {
                        token: request.token,
                        participation: request.participation,
                        generation: request.generation,
                        requested_at: request.requested_at,
                    },
                };
                return Ok(SubscriptionStep::Effect(AttemptEffect {
                    id: action,
                    class: EffectClass::Settlement,
                    body,
                }));
            }
            let Some(participation) = self.adjust.first().copied() else {
                return Ok(SubscriptionStep::Idle);
            };
            if !context.charge(1) {
                return Ok(SubscriptionStep::Yield);
            }
            self.adjust.pop_first();
            let row = &self.rows[&participation];
            let generation = row
                .controller
                .generation()
                .expect("live subscription generation");
            let build = SubscriptionRequestBuild::new(execution, participation, generation)?;
            self.request = Some(Request { build, open: false });
        }
        self.finish_request(execution, action, context)
    }

    fn finish_request(
        &mut self,
        execution: &QueryTaskExecution,
        action: AttemptActionId,
        context: &mut dyn StepContext,
    ) -> Result<SubscriptionStep, TaskExecutionError> {
        let pending = self
            .request
            .as_mut()
            .expect("selected original subscription request");
        let Some(request) = pending.build.advance(execution, context)? else {
            return Ok(SubscriptionStep::Yield);
        };
        let body = if pending.open {
            self.rows
                .get_mut(&request.participation)
                .expect("original subscription")
                .opened = Some((action, request.generation));
            EffectBody::OpenSubscription(request)
        } else {
            EffectBody::AdjustReadWindow(request)
        };
        self.request = None;
        Ok(SubscriptionStep::Effect(AttemptEffect {
            id: action,
            class: EffectClass::Settlement,
            body,
        }))
    }
}
