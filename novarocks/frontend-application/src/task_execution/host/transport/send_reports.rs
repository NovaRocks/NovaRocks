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

use super::{NativeReceiptInput, SendKey, receipt_input::NativeReceiptError};
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
use novarocks_control_runtime::actor_turn::{ActorTurnError, StandingInputObligation};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
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
}

pub(crate) struct SendMemberReport {
    pub(crate) receipt: NativeReceiptInput,
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
            receipt,
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
        match self.receipt.record(key, ack, filter, workload) {
            Ok(()) => self.receipt_delivery = Some(self.receipt.submit()),
            Err(refusal) => self.refused_receipt = Some(refusal),
        }
    }
}

pub(crate) struct SendReportRows {
    pub(crate) started: NativeProgressInput,
    pub(crate) members: Box<[SendMemberReport]>,
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
    pub(crate) fn new(effect: AttemptActionId, rows: SendReportRows) -> Self {
        let mut index = rows
            .members
            .iter()
            .enumerate()
            .map(|(index, row)| (row.receipt.key().operation, index))
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
                                row.receipt.key() == *key
                                    && row.receipt.kind() == operation.kind()
                                    && row
                                        .first_poll
                                        .as_ref()
                                        .map(|(participation, _)| *participation)
                                        == participation
                            },
                        )
                })
    }

    pub(crate) fn into_rows(self) -> SendReportRows {
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
        let reports = ActorSendReports::new(
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
        );
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
        assert!(
            matches!(inbox.consume(started), ExecutorFact::SendStarted { effect: id, started_at } if id == effect && started_at == at)
        );
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
        assert!(matches!(inbox.consume(first_poll),
            ExecutorFact::EstablishRpcStarted { effect: id, operation, participation: observed, started_at }
                if id == effect && operation == establish.operation && observed == participation && started_at == rpc_at
        ));
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
        assert!(
            matches!(inbox.consume(acknowledged), ExecutorFact::Operation(observation)
            if observation.generation == establish.generation && observation.reply.operation_id() == establish.operation)
        );
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
        inbox.assert_no_result();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        let mut rows = reports.into_rows();
        rows.started.position.close_after_exit().unwrap();
        let [mut established, mut cancelled]: [SendMemberReport; 2] = rows
            .members
            .into_vec()
            .try_into()
            .unwrap_or_else(|_| panic!("same two members"));
        established
            .first_poll
            .take()
            .unwrap()
            .1
            .position
            .close_after_exit()
            .unwrap();
        let retired = established
            .receipt
            .retire_after_exit()
            .unwrap_or_else(|(error, _)| panic!("{error}"))
            .unwrap();
        assert_eq!(retired.original, original);
        assert!(retired.unpublished.is_none());
        let (error, raw) = cancelled.refused_receipt.take().unwrap();
        assert!(matches!(error, NativeReceiptError::WrongSend));
        assert_eq!(raw, refused);
        assert!(
            cancelled
                .receipt
                .retire_after_exit()
                .unwrap_or_else(|(error, _)| panic!("{error}"))
                .is_none()
        );
        capacity.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 128).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
