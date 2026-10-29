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

//! The original Context's bounded failed-owner publication stage. The live
//! failure action and its prepaid Work remain owned here after publication;
//! source, callback, input, delivery and physical cleanup continue separately.

use super::*;
use crate::control_runtime::{
    context_host::requests::ContextRequestError, open_summary::OpenSummaryError,
};
use crate::settlement::SettlementDispatchError;
use crate::task_port::{ProjectionError, TaskProjection};
use novarocks_control_runtime::driver::FailurePublicationSession;
use novarocks_control_runtime_core::{
    effect::EffectState,
    identity::{ActionId, WorkId},
};
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug)]
pub(crate) enum FailurePublicationError {
    WrongOwner,
    AlreadyOwned,
    NotStarted,
    NotDrained,
    UnknownTask,
    Driver(DriverError),
    Runtime(RuntimeError),
    Projection(ProjectionError),
    Settlement(SettlementDispatchError),
    Request(ContextRequestError),
    Open(OpenSummaryError),
    HostCall(crate::control_runtime::task_host_call::TaskHostCallError),
}
/// Visits count actual mailbox classes, including empty ones. Taking a neutral
/// message neither retires its input receipt nor claims any source/R exit.
#[derive(Debug, Default)]
pub(crate) struct FailureInputProgress {
    pub units: usize,
    pub taken: usize,
    pub empty: bool,
}

#[derive(Debug)]
#[must_use = "Retain the same original failed-owner effect on refusal"]
pub(crate) struct FailurePublicationStartRefusal {
    pub error: FailurePublicationError,
    pub ticket: EffectTicket<WorkerAction>,
}

/// Only this module can construct the stage after traversing the original
/// Context's Task, RPC and runtime batch inventories. It borrows the still
/// running failure action: a failed flag or a Task snapshot cannot mint it.
/// This stage says nothing about physical source, callback or report exit.
pub(crate) struct ScopePublicationsDrained<'a> {
    session: &'a FailurePublicationSession<WorkerActor>,
}
impl ScopePublicationsDrained<'_> {
    pub(crate) fn validate(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
    ) -> Result<(), DriverError> {
        if self.session.matches_actor_runtime(runtime, actor)? {
            Ok(())
        } else {
            Err(DriverError::PublicationTargetMismatch)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Tasks(Option<TaskIdentity>),
    TaskProjection(TaskIdentity),
    Barrier(TaskIdentity),
    Requests(Option<WorkId>),
    Reply(WorkId),
    Batches(Option<InputId>),
    Effects {
        input: InputId,
        after: Option<ActionId>,
    },
    Reservations(InputId),
    Drained,
}

/// Cursors refer to actual owner tables. The pending projection, if any, is
/// the original refused publication value, not a second protocol/exit ledger.
#[must_use = "The original failure action stays Started through supervised cleanup"]
pub(super) struct ScopeFailureOwner {
    session: FailurePublicationSession<WorkerActor>,
    ticket: EffectTicket<WorkerAction>,
    phase: Phase,
    pending_projection: Option<Arc<TaskProjection>>,
    next_input_class: usize,
    empty_input_classes: usize,
}
impl ScopeFailureOwner {
    pub(super) fn is_drained(&self) -> bool {
        matches!(self.phase, Phase::Drained)
    }
    fn drained(&self) -> Result<ScopePublicationsDrained<'_>, FailurePublicationError> {
        if !self.is_drained() {
            return Err(FailurePublicationError::NotDrained);
        }
        Ok(ScopePublicationsDrained {
            session: &self.session,
        })
    }
    fn advance(
        &mut self,
        host: &mut ContextHostRecord,
        budget: usize,
    ) -> Result<bool, FailurePublicationError> {
        self.session
            .matches_actor_runtime(&host.control_runtime, host.actor)
            .map_err(FailurePublicationError::Driver)?
            .then_some(())
            .ok_or(FailurePublicationError::WrongOwner)?;
        for _ in 0..budget {
            match self.phase {
                Phase::Tasks(after) => {
                    let lower = after.map_or(Unbounded, Excluded);
                    let Some((&task, owner)) = host.tasks.range_mut((lower, Unbounded)).next()
                    else {
                        self.phase = Phase::Requests(None);
                        continue;
                    };
                    let (TaskHostOwner::Ready(task_host) | TaskHostOwner::Initializing(task_host)) =
                        owner
                    else {
                        // No port was exposed for this original partial. Keep
                        // its unbound/captured resources in the same owner.
                        self.phase = Phase::Tasks(Some(task));
                        continue;
                    };
                    task_host.close_request_admission();
                    task_host.port().ingress().fail();
                    // Visiting this table row and generating its publication
                    // spend distinct work units, including with budget one.
                    self.phase = Phase::TaskProjection(task);
                }
                Phase::TaskProjection(task) => {
                    let Some(
                        TaskHostOwner::Ready(task_host) | TaskHostOwner::Initializing(task_host),
                    ) = host.tasks.get_mut(&task)
                    else {
                        return Err(FailurePublicationError::UnknownTask);
                    };
                    let port = task_host.port();
                    let next = match self.pending_projection.take() {
                        Some(original) => original,
                        None => Arc::new(
                            port.prepare_owner_failure()
                                .map_err(FailurePublicationError::Projection)?,
                        ),
                    };
                    match self.session.commit_snapshot(port.publication(), next) {
                        Ok(old) => {
                            // Failure projection preserves the same rich
                            // accepted Arc in the newly published snapshot.
                            // The old projection cannot run that source's last
                            // destructor here; remaining fields are plain data.
                            drop(old);
                        }
                        Err((error, original)) => {
                            self.pending_projection = Some(original);
                            return Err(FailurePublicationError::Driver(error));
                        }
                    }
                    self.phase = Phase::Barrier(task);
                }
                Phase::Barrier(task) => {
                    let Some(
                        TaskHostOwner::Ready(task_host) | TaskHostOwner::Initializing(task_host),
                    ) = host.tasks.get_mut(&task)
                    else {
                        return Err(FailurePublicationError::UnknownTask);
                    };
                    if let Some(completion) = task_host.completion()
                        && !completion.settlement_barrier_committed()
                    {
                        completion
                            .commit_failure_settlement_barrier(&mut self.session, &task_host.port())
                            .map_err(FailurePublicationError::Settlement)?;
                    }
                    self.phase = Phase::Tasks(Some(task));
                }
                Phase::Requests(after) => {
                    let lower = after.map_or(Unbounded, Excluded);
                    let Some((&work, original)) =
                        host.resources.requests.range_mut((lower, Unbounded)).next()
                    else {
                        self.phase = Phase::Batches(None);
                        continue;
                    };
                    self.phase = Phase::Reply(work);
                }
                Phase::Reply(work) => {
                    // R may have genuinely retired after our row visit. It
                    // then has no remaining outbox obligation to publish.
                    if let Some(original) = host.resources.requests.get_mut(&work) {
                        original
                            .publish_owner_failure(&mut self.session)
                            .map_err(FailurePublicationError::Request)?;
                    }
                    self.phase = Phase::Requests(Some(work));
                }
                Phase::Batches(after) => {
                    let batches = host
                        .control_runtime
                        .pending_batches(host.actor, after, 1)
                        .map_err(FailurePublicationError::Driver)?;
                    if let Some(&input) = batches.first() {
                        if input == self.ticket.input {
                            self.phase = Phase::Batches(Some(input));
                        } else {
                            self.phase = Phase::Effects { input, after: None };
                        }
                    } else {
                        // Every remaining input/effect is still in the real
                        // runtime, or belongs to its original Started host
                        // action. No payload was dropped or exit invented.
                        // The neutral dispatch index is still paid and stays
                        // with this Context until its own bounded teardown.
                        self.phase = Phase::Drained;
                    }
                }
                Phase::Effects { input, after } => {
                    let effects = match host
                        .control_runtime
                        .effect_inventory(host.actor, input, after, 1)
                    {
                        Ok(effects) => effects,
                        // Another original completion may retire a previously
                        // completed batch between these bounded owner turns.
                        Err(DriverError::UnknownInput) => {
                            self.phase = Phase::Batches(Some(input));
                            continue;
                        }
                        Err(error) => return Err(FailurePublicationError::Driver(error)),
                    };
                    if let Some((effect, state)) = effects.first() {
                        if *state == EffectState::Pending && !effect.kind.allowed_after_failure() {
                            host.control_runtime
                                .abandon_unstarted_effect(host.actor, input, effect.id)
                                .map_err(FailurePublicationError::Driver)?;
                        }
                        // Started/Published actions retain their real host
                        // capture/body duties. Pending cleanup/settlement also
                        // stay owned; failure does not cancel their obligation.
                        self.phase = Phase::Effects {
                            input,
                            after: Some(effect.id),
                        };
                    } else {
                        self.phase = Phase::Reservations(input);
                    }
                }
                Phase::Reservations(input) => {
                    let reservations = match host
                        .control_runtime
                        .step_reservations(host.actor, input, None, 1)
                    {
                        Ok(reservations) => reservations,
                        Err(DriverError::UnknownInput) => {
                            self.phase = Phase::Batches(Some(input));
                            continue;
                        }
                        Err(error) => return Err(FailurePublicationError::Driver(error)),
                    };
                    if let Some(&(reservation, _)) = reservations.first() {
                        let original = host
                            .control_runtime
                            .take_failed_reservation(host.actor, input, reservation)
                            .map_err(FailurePublicationError::Driver)?;
                        // This exact guard was never bound or exposed and has
                        // no foreign captures. Its ordinary Drop rolls back
                        // only that unused admission, outside all inbox locks.
                        drop(original);
                    } else {
                        self.phase = Phase::Batches(Some(input));
                    }
                }
                Phase::Drained => return Ok(true),
            }
        }
        Ok(self.is_drained())
    }
}

impl ContextHostRecord {
    /// Take the actual failed Context action before its original cleanup Work
    /// starts. A foreign/duplicate ticket is returned whole and cannot replace
    /// the already retained session or the Context's original owner inventory.
    pub(crate) fn begin_failure_publications(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), FailurePublicationStartRefusal> {
        let error = if self.resources.failure.is_some() {
            Some(FailurePublicationError::AlreadyOwned)
        } else if ticket.actor != self.actor
            || ticket.descriptor.work != self.resources.roles[FAILURE].work.id()
            || !matches!(ticket.effect, WorkerAction::OwnerFailed { scope, .. } if scope == self.scope().id())
        {
            Some(FailurePublicationError::WrongOwner)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(FailurePublicationStartRefusal { error, ticket });
        }
        if let Err(error) = self.control_runtime.validate_failure_effect_ticket(&ticket) {
            return Err(FailurePublicationStartRefusal {
                error: FailurePublicationError::Driver(error),
                ticket,
            });
        }
        if let Err(error) = self.resources.roles[FAILURE].work.start() {
            return Err(FailurePublicationStartRefusal {
                error: FailurePublicationError::Runtime(error),
                ticket,
            });
        }
        let session = match self.control_runtime.claim_failure_publication(&ticket) {
            Ok(session) => session,
            Err(error) => {
                return Err(FailurePublicationStartRefusal {
                    error: FailurePublicationError::Driver(error),
                    ticket,
                });
            }
        };
        self.resources.failure = Some(ScopeFailureOwner {
            session,
            ticket,
            phase: Phase::Tasks(None),
            pending_projection: None,
            next_input_class: 0,
            empty_input_classes: 0,
        });
        self.notify_host_progress();
        Ok(())
    }
    /// One owner turn spends at most `budget` inventory/publication units. It
    /// calls no provider, continuation, stop observer or source destructor.
    /// The original failure Work remains live after this stage returns true.
    pub(crate) fn advance_failure_publications(
        &mut self,
        budget: usize,
    ) -> Result<bool, FailurePublicationError> {
        let mut original = self
            .resources
            .failure
            .take()
            .ok_or(FailurePublicationError::NotStarted)?;
        let result = original.advance(self, budget);
        self.resources.failure = Some(original);
        if budget > 0 && matches!(result, Ok(false)) {
            // Only finite metadata remains; no I/O or live holder is polled.
            self.notify_host_progress();
        }
        result
    }
    /// After the real publication stage, recover queued neutral messages in
    /// bounded fair class order. The runtime marks each original receipt as
    /// Abandoned; its original R/causal owner must still observe that receipt
    /// and its own true body/capture/report exit before retirement or refund.
    pub(crate) fn advance_failed_runtime_inputs(
        &mut self,
        budget: usize,
    ) -> Result<FailureInputProgress, FailurePublicationError> {
        const CLASSES: [InputClass; 4] = [
            InputClass::Lifecycle,
            InputClass::Control,
            InputClass::Ordinary,
            InputClass::Timer,
        ];
        let mut original = self
            .resources
            .failure
            .take()
            .ok_or(FailurePublicationError::NotStarted)?;
        let result = (|| {
            original
                .drained()?
                .validate(&self.control_runtime, self.actor)
                .map_err(FailurePublicationError::Driver)?;
            let mut progress = FailureInputProgress {
                empty: original.empty_input_classes == CLASSES.len(),
                ..Default::default()
            };
            for _ in 0..budget {
                if progress.empty {
                    break;
                }
                let class = CLASSES[original.next_input_class];
                original.next_input_class = (original.next_input_class + 1) % CLASSES.len();
                progress.units += 1;
                match self
                    .control_runtime
                    .take_failed_input(self.actor, class)
                    .map_err(FailurePublicationError::Driver)?
                {
                    Some((receipt, message)) => {
                        // WorkerMessage is the closed pure-protocol vocabulary,
                        // not a provider/callback/opaque source carrier. Drop
                        // this original neutral value after inbox locks exit.
                        drop(message);
                        original.empty_input_classes = 0;
                        progress.taken += 1;
                        self.notify_failed_request_input(receipt.input);
                    }
                    None => original.empty_input_classes += 1,
                }
                progress.empty = original.empty_input_classes == CLASSES.len();
            }
            Ok(progress)
        })();
        self.resources.failure = Some(original);
        if budget > 0 && result.as_ref().is_ok_and(|progress| !progress.empty) {
            self.notify_host_progress();
        }
        result
    }
    /// Borrow the stage produced by this original Context's completed scope
    /// publication traversal. Its live failure Work is revalidated; a caller
    /// cannot construct this authority from a failed flag or one Task value.
    pub(crate) fn failure_publication_stage(
        &self,
    ) -> Result<ScopePublicationsDrained<'_>, FailurePublicationError> {
        let original = self
            .resources
            .failure
            .as_ref()
            .ok_or(FailurePublicationError::NotStarted)?;
        let stage = original.drained()?;
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(FailurePublicationError::Driver)?;
        Ok(stage)
    }
    /// Withdraw only a pure resume hint after the actual failed-owner stage.
    /// The original queued input and every effect/report still drain normally.
    pub(crate) fn discard_failed_internal_hint(&mut self) -> Result<(), FailurePublicationError> {
        let original = self
            .resources
            .failure
            .as_ref()
            .ok_or(FailurePublicationError::NotStarted)?;
        let stage = original.drained()?;
        self.resources
            .internal
            .discard_failed_hint(&stage)
            .map_err(FailurePublicationError::Driver)
    }
    pub(crate) fn failure_publications_drained(&self) -> bool {
        self.resources
            .failure
            .as_ref()
            .is_some_and(ScopeFailureOwner::is_drained)
    }
    /// Borrow both original owners together. Failure publication and execution
    /// exit remain separate proofs; this method calls no host or destructor.
    pub(crate) fn take_failed_retire_borrowing_job(
        &self,
        task: TaskIdentity,
    ) -> Result<
        Option<crate::control_runtime::task_host_call::TaskHostCallJob>,
        FailurePublicationError,
    > {
        let stage = self.failure_publication_stage()?;
        let Some(TaskHostOwner::Ready(host) | TaskHostOwner::Initializing(host)) =
            self.tasks.get(&task)
        else {
            return Err(FailurePublicationError::UnknownTask);
        };
        host.take_failed_retire_borrowing_job(&stage)
            .map_err(FailurePublicationError::HostCall)
    }
    /// Access the original Task's unfilled open segment only after this exact
    /// Context has finished its actual scope-wide failure publication stage.
    pub(crate) fn take_failed_task_open(
        &mut self,
        task: TaskIdentity,
    ) -> Result<Option<crate::task_proposal_ingress::SummaryInput>, FailurePublicationError> {
        let original = self
            .resources
            .failure
            .as_ref()
            .ok_or(FailurePublicationError::NotStarted)?;
        let drained = original.drained()?;
        let Some(TaskHostOwner::Ready(host) | TaskHostOwner::Initializing(host)) =
            self.tasks.get_mut(&task)
        else {
            return Err(FailurePublicationError::UnknownTask);
        };
        host.open_summary_mut()
            .ok_or(FailurePublicationError::UnknownTask)?
            .take_failed_queued(&drained)
            .map_err(FailurePublicationError::Open)
    }
}

#[cfg(test)]
impl ContextHostRecord {
    /// This component fixture has really disposed every Task/R original and
    /// retired all other batches before the publication owner's stack returns.
    /// The Harness then observes the original cleanup lease's body/captures;
    /// this helper does not sign a source, callback or Work exit.
    pub(super) fn complete_failure_publications_for_test(&mut self) {
        let Some(original) = self.resources.failure.take() else {
            return;
        };
        assert!(original.is_drained());
        assert!(original.pending_projection.is_none());
        assert!(self.tasks.is_empty());
        assert!(self.resources.requests.is_empty());
        assert!(self.resources.request_partials.is_empty());
        assert!(
            original
                .session
                .matches_actor_runtime(&self.control_runtime, self.actor)
                .unwrap()
        );
        assert_eq!(
            self.control_runtime
                .pending_batches(self.actor, None, 2)
                .unwrap(),
            [original.ticket.input]
        );
        let input = original.ticket.input;
        self.control_runtime
            .effect_completed(original.ticket)
            .unwrap();
        self.control_runtime
            .retire_input(self.actor, input)
            .unwrap();
        drop(
            self.control_runtime
                .take_failure_payload(self.actor)
                .unwrap(),
        );
    }
}
