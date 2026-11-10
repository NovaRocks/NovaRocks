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

use super::{
    NativeReceiptInput, SendKey,
    receipt_input::{NativeReceiptError, RetiredNativeReceipt},
};
use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::{
        intent::{AckPayload, OperationAcknowledgement, OperationIntent},
        operation::OperationDispatchResult,
    },
};
use novarocks_attempt_protocol::{
    failure::HostFailureRef,
    input::{AttemptInput, ExecutorFact},
    start::PayloadRef,
};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
    task::Poll,
};

pub(crate) struct NativeProgressInput {
    pub(crate) position: StandingInputObligation<FrontendActor>,
    pub(crate) delivery: Option<Result<InputId, (ActorTurnError, FrontendInput)>>,
}
impl NativeProgressInput {
    pub(crate) fn new(position: StandingInputObligation<FrontendActor>) -> Self {
        Self {
            position,
            delivery: None,
        }
    }
    fn fill(&mut self, fact: ExecutorFact) {
        assert!(
            self.delivery.is_none(),
            "one original transport progress fact"
        );
        self.delivery = Some(self.position.fill(FrontendInput::Attempt(Box::new(
            AttemptInput::Executor(fact),
        ))));
    }

    fn retire_after_exit(&mut self) -> Result<Poll<Option<SendReportReturn>>, ActorTurnError> {
        if !self.position.retire_ready_after_exit()? {
            return Ok(Poll::Pending);
        }
        self.position.close_after_exit()?;
        Ok(Poll::Ready(match self.delivery.take() {
            Some(Err((error, value))) => Some(SendReportReturn::ProgressRefused { error, value }),
            _ => None,
        }))
    }
}

pub(crate) struct SendMemberReport {
    receipt: Option<NativeReceiptInput>,
    filter: Option<PayloadRef>,
    workload: Option<HostFailureRef>,
    pub(crate) first_poll: Option<(ParticipationRef, NativeProgressInput)>,
    pub(crate) receipt_delivery: Option<Result<InputId, ActorTurnError>>,
    pub(crate) refused_receipt: Option<(NativeReceiptError, OperationAcknowledgement)>,
}
impl SendMemberReport {
    pub(crate) fn new(
        receipt: NativeReceiptInput,
        filter: Option<PayloadRef>,
        workload: Option<HostFailureRef>,
        first_poll: Option<(ParticipationRef, NativeProgressInput)>,
    ) -> Self {
        Self {
            receipt: Some(receipt),
            filter,
            workload,
            first_poll,
            receipt_delivery: None,
            refused_receipt: None,
        }
    }
    fn record(&mut self, key: SendKey, ack: OperationAcknowledgement) {
        assert!(self.receipt_delivery.is_none() && self.refused_receipt.is_none());
        let filter = if matches!(
            ack.payload(),
            AckPayload::Release {
                runtime_filter: Some(_),
                ..
            }
        ) {
            self.filter
        } else {
            None
        };
        let workload = if ack.dispatch_result() == OperationDispatchResult::NonWorkerRejected
            && ack.local_failure().is_none()
            && ack.governance_failure().is_some()
        {
            self.workload
        } else {
            None
        };
        let receipt = self
            .receipt
            .as_mut()
            .expect("live original receipt position");
        match receipt.record(key, ack, filter, workload) {
            Ok(()) => self.receipt_delivery = Some(receipt.submit()),
            Err(refusal) => self.refused_receipt = Some(refusal),
        }
    }
}

pub(crate) struct SendReportRows {
    pub(crate) started: NativeProgressInput,
    pub(crate) members: Box<[SendMemberReport]>,
}
pub(super) fn close_unlaunched(reports: Arc<ActorSendReports>) {
    let mut retirement = SendReportRetirement::after_join(reports)
        .unwrap_or_else(|reports| std::panic::resume_unwind(Box::new(reports)));
    for _ in 0..=2 * retirement.positions() {
        match retirement
            .advance()
            .expect("unlaunched original report positions")
        {
            SendReportRetirementStep::Advanced(None) | SendReportRetirementStep::Pending => {}
            SendReportRetirementStep::Retired => return,
            SendReportRetirementStep::Advanced(Some(report)) => {
                std::panic::resume_unwind(Box::new(report))
            }
        }
    }
    std::panic::resume_unwind(Box::new(retirement));
}

/// The actual sender has exclusive access to the original prepaid positions
/// while reporting. The Source retains this same record through real Join
/// and consumes its raw or rejected payloads after their actual input exit.
pub(crate) struct ActorSendReports {
    effect: AttemptActionId,
    index: Box<[(TaskOperationId, usize)]>,
    rows: Mutex<Option<SendReportRows>>,
}

struct RowLoan<'a> {
    owner: &'a Mutex<Option<SendReportRows>>,
    rows: Option<SendReportRows>,
}
impl Drop for RowLoan<'_> {
    fn drop(&mut self) {
        *self.owner.lock().unwrap_or_else(|e| e.into_inner()) = self.rows.take();
    }
}

impl ActorSendReports {
    pub(super) fn matches_receiver(&self, registration: &ActorRegistration) -> bool {
        let matches = |input: &NativeProgressInput| {
            input
                .position
                .author()
                .is_ok_and(|author| registration.matches_input_author(&author))
        };
        self.rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|rows| {
                matches(&rows.started)
                    && rows.members.iter().all(|row| {
                        row.receipt
                            .as_ref()
                            .is_some_and(|input| input.matches_receiver(registration))
                            && row
                                .first_poll
                                .as_ref()
                                .is_none_or(|(_, input)| matches(input))
                    })
            })
    }

    pub(crate) fn new(effect: AttemptActionId, rows: SendReportRows) -> Self {
        let mut index = rows
            .members
            .iter()
            .enumerate()
            .map(|(index, row)| (row.receipt.as_ref().unwrap().key().operation, index))
            .collect::<Vec<_>>();
        index.sort_unstable_by_key(|&(operation, _)| operation);
        assert!(index.windows(2).all(|pair| pair[0].0 != pair[1].0));
        Self {
            effect,
            index: index.into_boxed_slice(),
            rows: Mutex::new(Some(rows)),
        }
    }

    pub(crate) fn matches(
        &self,
        effect: AttemptActionId,
        keys: &[SendKey],
        operations: &[OperationIntent],
    ) -> bool {
        self.effect == effect
            && self
                .rows
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .is_some_and(|rows| {
                    rows.members.len() == keys.len()
                        && rows.members.len() == operations.len()
                        && rows.members.iter().zip(keys).zip(operations).all(
                            |((row, key), operation)| {
                                let participation = match operation {
                                    OperationIntent::EstablishParticipation(request) => {
                                        Some(request.participation())
                                    }
                                    _ => None,
                                };
                                let receipt = row.receipt.as_ref().expect("live original receipt");
                                receipt.key() == *key
                                    && receipt.kind() == operation.kind()
                                    && row
                                        .first_poll
                                        .as_ref()
                                        .map(|(participation, _)| *participation)
                                        == participation
                            },
                        )
                })
    }

    fn into_rows(self) -> SendReportRows {
        self.rows
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .expect("original sender has exited")
    }

    fn loan(&self) -> RowLoan<'_> {
        let rows = self
            .rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("one reporting body for this send");
        RowLoan {
            owner: &self.rows,
            rows: Some(rows),
        }
    }

    fn member(&self, operation: TaskOperationId) -> Option<usize> {
        let index = self
            .index
            .binary_search_by_key(&operation, |&(id, _)| id)
            .ok()?;
        Some(self.index[index].1)
    }

    pub(super) fn record_ack(&self, key: SendKey, ack: OperationAcknowledgement) {
        let Some(index) = self.member(key.operation) else {
            std::panic::resume_unwind(Box::new(ack));
        };
        let mut loan = self.loan();
        loan.rows.as_mut().unwrap().members[index].record(key, ack);
    }

    pub(super) fn send_started(&self, at: MonotonicInstant) {
        let mut loan = self.loan();
        loan.rows
            .as_mut()
            .unwrap()
            .started
            .fill(ExecutorFact::SendStarted {
                effect: self.effect,
                started_at: at,
            });
    }

    pub(super) fn establish_rpc_started(
        &self,
        operation: TaskOperationId,
        participation: ParticipationRef,
        at: MonotonicInstant,
    ) {
        let index = self
            .member(operation)
            .expect("exact original Establish member");
        let mut loan = self.loan();
        let row = &mut loan.rows.as_mut().unwrap().members[index];
        let (expected, input) = row
            .first_poll
            .as_mut()
            .expect("prepaid Establish first-poll position");
        assert_eq!(*expected, participation);
        input.fill(ExecutorFact::EstablishRpcStarted {
            effect: self.effect,
            operation,
            participation,
            started_at: at,
        });
    }
}

/// Original values for the Source's domain or failure consumer. The
/// canonical Native reply remains live through that consumer's actual exit.
#[must_use = "transfer the original report to its final consumer"]
pub(crate) enum SendReportReturn {
    Receipt {
        key: SendKey,
        delivery_error: Option<ActorTurnError>,
        receipt: RetiredNativeReceipt,
    },
    ReceiptRefused {
        key: SendKey,
        error: NativeReceiptError,
        receipt: OperationAcknowledgement,
    },
    ProgressRefused {
        error: ActorTurnError,
        value: FrontendInput,
    },
}

pub(crate) enum SendReportRetirementStep {
    Advanced(Option<SendReportReturn>),
    Pending,
    Retired,
}

/// Bounded walk of the same original send's paid positions. Core commits
/// and effect completions wake the host when a position can be revisited.
pub(crate) struct SendReportRetirement {
    started: Option<NativeProgressInput>,
    members: Box<[SendMemberReport]>,
    next: usize,
    remaining: usize,
}

impl SendReportRetirement {
    pub(crate) fn after_join(
        reports: Arc<ActorSendReports>,
    ) -> Result<Self, Arc<ActorSendReports>> {
        let SendReportRows { started, members } = Arc::try_unwrap(reports)?.into_rows();
        let remaining = members.len() + 1;
        Ok(Self {
            started: Some(started),
            members,
            next: 0,
            remaining,
        })
    }

    pub(crate) fn positions(&self) -> usize {
        self.members.len() + 1
    }

    /// Visit one member or the batch-start role. A visit advances at most
    /// one actual position and returns at most one original value.
    pub(crate) fn advance(&mut self) -> Result<SendReportRetirementStep, ActorTurnError> {
        use SendReportRetirementStep::{Advanced, Pending, Retired};
        if self.remaining == 0 {
            return Ok(Retired);
        }
        let index = self.next;
        self.next = (index + 1) % self.positions();
        if index == 0 {
            let Some(started) = &mut self.started else {
                return Ok(Pending);
            };
            return match started.retire_after_exit()? {
                Poll::Pending => Ok(Pending),
                Poll::Ready(returned) => {
                    self.started = None;
                    self.remaining -= 1;
                    Ok(Advanced(returned))
                }
            };
        }
        let member = &mut self.members[index - 1];
        if let Some((_, first_poll)) = &mut member.first_poll
            && let Poll::Ready(returned) = first_poll.retire_after_exit()?
        {
            member.first_poll = None;
            self.remaining -= usize::from(member.receipt.is_none());
            return Ok(Advanced(returned));
        }
        let Some(receipt) = &mut member.receipt else {
            return Ok(Pending);
        };
        let key = receipt.key();
        let Poll::Ready(returned) = receipt.retire_ready_after_exit()? else {
            return Ok(Pending);
        };
        member.receipt = None;
        self.remaining -= usize::from(member.first_poll.is_none());
        let delivery_error = member.receipt_delivery.take().and_then(Result::err);
        let returned = match member.refused_receipt.take() {
            Some((error, receipt)) => Some(SendReportReturn::ReceiptRefused {
                key,
                error,
                receipt,
            }),
            None => returned.map(|receipt| SendReportReturn::Receipt {
                key,
                delivery_error,
                receipt,
            }),
        };
        Ok(Advanced(returned))
    }
}

pub(crate) fn actor_send_report_layouts(
    items: usize,
    establishes: usize,
) -> Result<[Layout; 4], novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    let inputs = items
        .checked_add(establishes)
        .and_then(|inputs| inputs.checked_add(1))
        .ok_or(LayoutResourceError::Arithmetic)?;
    Ok([
        arc_layout(Layout::new::<ActorSendReports>())?,
        Layout::array::<SendMemberReport>(items).map_err(|_| LayoutResourceError::Arithmetic)?,
        Layout::array::<(TaskOperationId, usize)>(items)
            .map_err(|_| LayoutResourceError::Arithmetic)?,
        Layout::array::<AttemptInput>(inputs).map_err(|_| LayoutResourceError::Arithmetic)?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::host::custody::tests::WithdrawalInbox;
    use novarocks_attempt_protocol::input::SendGeneration;
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        identity::Generation,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::OperationKind;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use std::{num::NonZeroU64, time::Duration};

    #[test]
    fn native_report_positions_deliver_exact_progress_and_preserve_refused_ack() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let (mut work, mut capacity) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        work.start().unwrap();
        let effect = AttemptActionId::new(NonZeroU64::new(3).unwrap());
        let key = |id| SendKey {
            operation: TaskOperationId::new_v7(),
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(id).unwrap())),
        };
        let establish = key(1);
        let cancel = key(2);
        let participation = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let progress = || NativeProgressInput::new(inbox.reserve_standing(&work));
        let reports = Arc::new(ActorSendReports::new(
            effect,
            SendReportRows {
                started: progress(),
                members: Box::new([
                    SendMemberReport::new(
                        NativeReceiptInput::new(
                            establish,
                            OperationKind::UpdateParticipation,
                            inbox.reserve_standing(&work),
                        ),
                        None,
                        None,
                        Some((participation, progress())),
                    ),
                    SendMemberReport::new(
                        NativeReceiptInput::new(
                            cancel,
                            OperationKind::CancelTask,
                            inbox.reserve_standing(&work),
                        ),
                        None,
                        None,
                        None,
                    ),
                ]),
            },
        ));
        let at = MonotonicInstant::from_origin(Duration::from_millis(3));
        reports.send_started(at);
        let started = reports
            .loan()
            .rows
            .as_ref()
            .unwrap()
            .started
            .delivery
            .as_ref()
            .unwrap()
            .as_ref()
            .copied()
            .unwrap();
        let rpc_at = at.saturating_add(Duration::from_millis(1));
        reports.establish_rpc_started(establish.operation, participation, rpc_at);
        let first_poll = reports.loan().rows.as_ref().unwrap().members[0]
            .first_poll
            .as_ref()
            .unwrap()
            .1
            .delivery
            .as_ref()
            .unwrap()
            .as_ref()
            .copied()
            .unwrap();
        let original = OperationAcknowledgement::transport_unknown(
            establish.operation,
            OperationKind::UpdateParticipation,
        );
        reports.record_ack(establish, original.clone());
        let acknowledged = reports.loan().rows.as_ref().unwrap().members[0]
            .receipt_delivery
            .as_ref()
            .unwrap()
            .as_ref()
            .copied()
            .unwrap();
        let refused = OperationAcknowledgement::transport_unknown(
            cancel.operation,
            OperationKind::CancelTask,
        );
        reports.record_ack(
            SendKey {
                generation: establish.generation,
                ..cancel
            },
            refused.clone(),
        );
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        use SendReportRetirementStep::{Advanced, Pending, Retired};
        let alias = reports.clone();
        let reports = SendReportRetirement::after_join(reports)
            .err()
            .expect("another holder retains the original report ledger");
        assert!(Arc::ptr_eq(&reports, &alias));
        drop(alias);
        let mut retirement = SendReportRetirement::after_join(reports)
            .unwrap_or_else(|_| panic!("same report ledger after producer exit"));
        assert!(matches!(retirement.advance().unwrap(), Pending));
        assert!(matches!(retirement.advance().unwrap(), Pending));
        assert!(matches!(
            retirement.advance().unwrap(),
            Advanced(Some(SendReportReturn::ReceiptRefused {
                key,
                error: NativeReceiptError::WrongSend,
                receipt,
            })) if key == cancel && receipt == refused
        ));
        assert!(matches!(inbox.commit(started),
            ExecutorFact::SendStarted { effect: id, started_at } if id == effect && started_at == at));
        assert!(matches!(retirement.advance().unwrap(), Advanced(None)));
        assert!(matches!(inbox.commit(first_poll),
            ExecutorFact::EstablishRpcStarted { effect: id, operation, participation: observed, started_at }
                if id == effect && operation == establish.operation && observed == participation && started_at == rpc_at));
        assert!(matches!(retirement.advance().unwrap(), Advanced(None)));
        assert!(matches!(inbox.commit(acknowledged),
            ExecutorFact::Operation(observation) if observation.generation == establish.generation
                && observation.reply.operation_id() == establish.operation));
        assert!(matches!(retirement.advance().unwrap(), Pending));
        assert!(matches!(retirement.advance().unwrap(), Pending));
        assert!(matches!(
            retirement.advance().unwrap(),
            Advanced(Some(SendReportReturn::Receipt {
                key,
                delivery_error: None,
                receipt: RetiredNativeReceipt { original: receipt, unpublished: None },
            })) if key == establish && receipt == original
        ));
        assert!(matches!(retirement.advance().unwrap(), Retired));
        inbox.assert_no_result();
        capacity.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 128).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
