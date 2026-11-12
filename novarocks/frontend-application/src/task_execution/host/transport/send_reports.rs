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
    admission_source::FailureRecords,
    prepare::PreparationFailure,
    receipt_input::{NativeReceiptError, RetiredNativeReceipt, retire_passive_report_position},
    source_ports::BodyOutcome,
};
use crate::task_execution::bridge::abort::AbortReceiptSlot;
use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::{
        error::TaskExecutionError,
        host::publication::DiagnosticReleaseReport,
        intent::{AckPayload, OperationAcknowledgement, OperationIntent},
        operation::OperationDispatchResult,
    },
};
use novarocks_attempt_protocol::input::SendGeneration;
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
    num::NonZeroU64,
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
        if !retire_passive_report_position(&self.position)? {
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
    abort_receipt: Option<Arc<AbortReceiptSlot>>,
    filter: Option<PayloadRef>,
    failures: FailureRecords,
    pub(crate) first_poll: Option<(ParticipationRef, NativeProgressInput)>,
    pub(crate) receipt_delivery: Option<Result<InputId, ActorTurnError>>,
    pub(crate) refused_receipt: Option<(NativeReceiptError, OperationAcknowledgement)>,
}
impl SendMemberReport {
    pub(crate) fn new(
        receipt: NativeReceiptInput,
        filter: Option<PayloadRef>,
        failures: FailureRecords,
        first_poll: Option<(ParticipationRef, NativeProgressInput)>,
    ) -> Self {
        Self {
            receipt: Some(receipt),
            abort_receipt: None,
            filter,
            failures,
            first_poll,
            receipt_delivery: None,
            refused_receipt: None,
        }
    }

    pub(super) fn bind_release_diagnostic(&mut self, report: DiagnosticReleaseReport) {
        self.receipt
            .as_mut()
            .expect("original release report position")
            .bind_release_diagnostic(report);
    }

    pub(super) fn retain_abort_receipt(&mut self, participation: ParticipationRef) {
        let key = self
            .receipt
            .as_ref()
            .expect("original Abort report position")
            .key();
        assert!(
            self.abort_receipt
                .replace(Arc::new(AbortReceiptSlot::new(key, participation)))
                .is_none()
        );
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
        {
            ack.governance_failure().map(|cause| {
                let reference = HostFailureRef(
                    NonZeroU64::new(key.generation.admission_effect().get())
                        .expect("original queued admission action"),
                );
                (reference, TaskExecutionError::Workload(cause.clone()))
            })
        } else {
            None
        };
        let receipt = self
            .receipt
            .as_mut()
            .expect("live original receipt position");
        match receipt.record(
            key,
            ack,
            filter,
            workload.as_ref().map(|(reference, _)| *reference),
        ) {
            Ok(()) => {
                if let Some(original) = &self.abort_receipt {
                    original.record(
                        receipt
                            .prepared_ack()
                            .expect("original prepared Abort receipt"),
                    );
                }
                // Core can select this exact failure immediately after fill.
                // The canonical ACK itself stays in the original report.
                let retained = workload.map(|(reference, cause)| {
                    assert!(
                        self.failures
                            .lock()
                            .unwrap()
                            .insert(reference, cause)
                            .is_none()
                    );
                    reference
                });
                let delivery = receipt.submit();
                if matches!(
                    delivery,
                    Err(ActorTurnError::ActorFailed | ActorTurnError::Closed)
                ) && let Some(reference) = retained
                {
                    // No Core consumer could have seen the rejected input.
                    assert!(self.failures.lock().unwrap().remove(&reference).is_some());
                }
                self.receipt_delivery = Some(delivery);
            }
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

/// A non-actor send has one original reply cell.
pub(super) struct SingleSendReply {
    pub(super) key: SendKey,
    pub(super) kind: novarocks_execution_contract::OperationKind,
    pub(super) value: Mutex<Option<OperationAcknowledgement>>,
    pub(super) outcome: Mutex<Option<super::source_ports::BodyOutcome>>,
}

#[derive(Clone)]
pub(super) enum SendReports {
    Actor(Arc<ActorSendReports>),
    Single(Arc<SingleSendReply>),
}

impl SendReports {
    pub(super) fn actor(&self) -> Option<&Arc<ActorSendReports>> {
        match self {
            Self::Actor(reports) => Some(reports),
            Self::Single(_) => None,
        }
    }

    pub(super) fn record_ack(&self, key: SendKey, ack: OperationAcknowledgement) {
        match self {
            Self::Actor(reports) => reports.record_ack(key, ack),
            Self::Single(reply) => {
                assert_eq!(key, reply.key);
                assert_eq!(ack.operation_id(), key.operation);
                assert_eq!(ack.kind(), reply.kind);
                assert!(reply.value.lock().unwrap().replace(ack).is_none());
            }
        }
    }

    pub(super) fn preparation_failed(
        &self,
        operations: &[OperationIntent],
        keys: &[SendKey],
        failure: PreparationFailure,
    ) -> BodyOutcome {
        assert_eq!(operations.len(), keys.len());
        let channel_dispatch = match (self, &failure) {
            (Self::Actor(_), PreparationFailure::Channel(error)) => {
                let dispatch = super::execute::classify_channel_error(error);
                tracing::warn!(%error, ?dispatch, "task operation channel acquisition failed");
                Some(dispatch)
            }
            _ => None,
        };
        for (intent, &key) in operations.iter().zip(keys) {
            self.record_ack(
                key,
                OperationAcknowledgement::from_dispatch_result(
                    intent.operation_id(),
                    intent.kind(),
                    channel_dispatch.unwrap_or(OperationDispatchResult::NonWorkerRejected),
                    AckPayload::None,
                ),
            );
        }
        // Channel acquisition has the same typed outcome on both sides of
        // preparation. The Source still waits for the original Join and
        // report retirement before it releases this generation.
        match channel_dispatch {
            Some(_) => BodyOutcome::ReceiptsRecorded,
            None => BodyOutcome::RawFailure(Box::new(failure)),
        }
    }

    pub(super) fn send_started(&self, at: MonotonicInstant) {
        if let Some(actor) = self.actor() {
            actor.send_started(at);
        }
    }

    pub(super) fn establish_rpc_started(
        &self,
        operation: TaskOperationId,
        participation: ParticipationRef,
        at: MonotonicInstant,
    ) {
        self.actor()
            .expect("Establish always uses the admitted Core port")
            .establish_rpc_started(operation, participation, at);
    }
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

    pub(super) fn abort_receipts(&self) -> Vec<(SendKey, std::sync::Weak<AbortReceiptSlot>)> {
        self.rows
            .lock()
            .unwrap()
            .as_ref()
            .expect("unlaunched original send reports")
            .members
            .iter()
            .filter_map(|member| {
                let original = member.abort_receipt.as_ref()?;
                Some((
                    member.receipt.as_ref().unwrap().key(),
                    Arc::downgrade(original),
                ))
            })
            .collect()
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
        abort_receipt: Option<Arc<AbortReceiptSlot>>,
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

impl SendReportReturn {
    pub(super) fn is_disposable(&self) -> bool {
        match self {
            Self::Receipt { delivery_error, .. } => matches!(
                delivery_error,
                None | Some(ActorTurnError::ActorFailed | ActorTurnError::Closed)
            ),
            Self::ProgressRefused { error, .. } => {
                matches!(error, ActorTurnError::ActorFailed | ActorTurnError::Closed)
            }
            Self::ReceiptRefused { .. } => false,
        }
    }
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
                abort_receipt: member.abort_receipt.take(),
            }),
        };
        Ok(Advanced(returned))
    }
}

pub(crate) fn actor_send_report_layouts(
    items: usize,
    establishes: usize,
    aborts: usize,
) -> Result<[Layout; 6], novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    let inputs = items
        .checked_add(establishes)
        .and_then(|inputs| inputs.checked_add(1))
        .ok_or(LayoutResourceError::Arithmetic)?;
    let abort_slot = arc_layout(Layout::new::<AbortReceiptSlot>())?.pad_to_align();
    let abort_slots = Layout::from_size_align(
        abort_slot
            .size()
            .checked_mul(aborts)
            .ok_or(LayoutResourceError::Arithmetic)?,
        abort_slot.align(),
    )
    .map_err(|_| LayoutResourceError::Arithmetic)?;
    Ok([
        arc_layout(Layout::new::<ActorSendReports>())?,
        Layout::array::<SendMemberReport>(items).map_err(|_| LayoutResourceError::Arithmetic)?,
        Layout::array::<(TaskOperationId, usize)>(items)
            .map_err(|_| LayoutResourceError::Arithmetic)?,
        Layout::array::<AttemptInput>(inputs).map_err(|_| LayoutResourceError::Arithmetic)?,
        abort_slots,
        Layout::array::<(TaskOperationId, SendGeneration)>(aborts)
            .map_err(|_| LayoutResourceError::Arithmetic)?,
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
    fn retryable_channel_preparation_reports_unknown_for_the_original_abort() {
        use crate::native::transport::ChannelAcquisitionError;
        use novarocks_attempt_protocol::input::{LocalDispatchResult, OperationReplyResult};
        use novarocks_execution_contract::{AbortCause, AbortParticipation};

        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let participation = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(9, 13), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let effect = AttemptActionId::new(NonZeroU64::MIN);
        let key = SendKey {
            operation: TaskOperationId::new_v7(),
            generation: SendGeneration::new(effect),
        };
        let abort = OperationIntent::AbortParticipation(AbortParticipation::new(
            key.operation,
            participation,
            AbortCause::QueryFailed,
        ));
        let report = SendMemberReport::new(
            inbox.reserve_native_receipt(&work, key, abort.kind()),
            None,
            FailureRecords::default(),
            None,
        );
        let original = Arc::new(ActorSendReports::new(
            effect,
            SendReportRows {
                started: NativeProgressInput::new(inbox.reserve_standing(&work)),
                members: Box::new([report]),
            },
        ));
        let reports = SendReports::Actor(Arc::clone(&original));
        assert!(matches!(
            reports.preparation_failed(
                &[abort],
                &[key],
                PreparationFailure::Channel(ChannelAcquisitionError::retryable_network(
                    "original channel was unavailable",
                )),
            ),
            BodyOutcome::ReceiptsRecorded
        ));
        let (_, ExecutorFact::Operation(observed)) = inbox.try_commit_next().unwrap() else {
            panic!("expected the original operation result");
        };
        assert_eq!(observed.generation, key.generation);
        assert_eq!(observed.reply.operation_id(), key.operation);
        assert!(matches!(
            observed.reply.result,
            OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                ..
            }
        ));
        assert!(inbox.try_commit_next().is_none());

        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        drop(reports);
        let mut retirement = SendReportRetirement::after_join(original)
            .unwrap_or_else(|_| panic!("the original reporting body has exited"));
        assert!(matches!(
            retirement.advance().unwrap(),
            SendReportRetirementStep::Advanced(None)
        ));
        let SendReportRetirementStep::Advanced(Some(SendReportReturn::Receipt {
            key: returned,
            delivery_error: None,
            receipt,
            ..
        })) = retirement.advance().unwrap()
        else {
            panic!("expected the original receipt after actual exit");
        };
        assert_eq!(returned, key);
        assert_eq!(
            receipt.original.dispatch_result(),
            OperationDispatchResult::TransportUnknown
        );
        assert!(matches!(
            retirement.advance().unwrap(),
            SendReportRetirementStep::Retired
        ));
        payer.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }

    #[test]
    fn release_report_retains_only_the_exact_canonical_diagnostic_alias() {
        use crate::query_execution::lifecycle_diagnostics::RuntimeFilterTerminalRollupSnapshot;
        use crate::task_execution::host::{
            adapter::tests::start,
            publication::{
                AttemptDiagnosticSources, DiagnosticPayloadAddress, OriginalDiagnosticPayload,
                OriginalDiagnosticRecords,
            },
        };
        use novarocks_attempt_protocol::{
            effect::PumpKind,
            input::{AckPayload as PassiveAck, OperationReplyResult},
            publication::RuntimeFilterDiagnostic,
        };
        use novarocks_execution_contract::{
            OperationOutcome, ParticipationReceipt, ParticipationState, ReleaseOutcome,
            operation::OperationResult,
        };
        use novarocks_proto_codec::lifecycle::terminal::CanonicalTerminalTelemetry;
        use novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry as RawTelemetry;
        use novarocks_spi::connector::read_stack::SplitSourceProfile;
        use std::sync::OnceLock;

        #[derive(Debug)]
        struct OriginalTelemetry(RawTelemetry);
        impl CanonicalTerminalTelemetry for OriginalTelemetry {
            fn terminal_telemetry(&self, ordinal: usize) -> &RawTelemetry {
                assert_eq!(ordinal, 0);
                &self.0
            }

            fn release_alias(self: Arc<Self>) {
                drop(self);
            }
        }

        let mut start = start();
        start.activation.policy.retain_diagnostics = true;
        start.observe_lifecycle_diagnostics = true;
        let split_ref = PayloadRef(NonZeroU64::new(11).unwrap());
        start.plan.pumps = Box::new([(split_ref, PumpKind::SplitAssignment)]);
        let part = *start.plan.establish_material.first_key_value().unwrap().0;
        let profile = Arc::new(OnceLock::new());
        let sources = AttemptDiagnosticSources::new(&start, Some((split_ref, profile.clone())));
        start.activation.policy.retain_diagnostics = false;
        let lifecycle = AttemptDiagnosticSources::new(&start, Some((split_ref, profile.clone())));
        let binding = sources.binding();
        assert_eq!(binding.activation, start.activation.identity);
        assert_eq!(binding.root, start.plan.graph.root);
        assert_eq!(binding.participations, &[part]);
        assert_eq!(
            binding.fragments,
            &[(
                binding.root,
                start.plan.graph.tasks[&binding.root.task_id()].fragment_id
            )]
        );
        assert!(
            sources
                .payload(DiagnosticPayloadAddress::SplitProfile(split_ref))
                .is_none()
        );
        let original_profile = SplitSourceProfile {
            files_considered: 23,
            ..SplitSourceProfile::default()
        };
        profile.set(original_profile).unwrap();
        assert!(matches!(
            sources.payload(DiagnosticPayloadAddress::SplitProfile(split_ref)),
            Some(OriginalDiagnosticPayload::SplitProfile { activation, root, profile })
                if activation == binding.activation && root == binding.root && profile == original_profile
        ));
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let operation = TaskOperationId::new_v7();
        let report = |id| {
            let id = NonZeroU64::new(id).unwrap();
            let key = SendKey {
                operation,
                generation: SendGeneration::new(AttemptActionId::new(id)),
            };
            let reference = PayloadRef(id);
            let mut report = SendMemberReport::new(
                NativeReceiptInput::new(
                    key,
                    OperationKind::ReleaseParticipation,
                    inbox.reserve_standing(&work),
                ),
                Some(reference),
                FailureRecords::default(),
                None,
            );
            report.bind_release_diagnostic(sources.release_report(part, reference).unwrap());
            (key, reference, report)
        };
        let (earlier, earlier_ref, mut earlier_report) = report(17);
        let (later, later_ref, mut later_report) = report(19);
        let mut telemetry = crate::task_execution::tests::fixture_runtime_filter_contribution();
        let original = Arc::new(OriginalTelemetry(telemetry.as_proto().clone()));
        let backing: Arc<dyn CanonicalTerminalTelemetry> = original.clone();
        telemetry.bind_canonical_owner(Arc::downgrade(&backing), 0);
        drop(backing);
        let ack = |outcome, telemetry| {
            OperationAcknowledgement::worker_receipt(
                operation,
                OperationKind::ReleaseParticipation,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: ParticipationReceipt::new(part, ParticipationState::TerminalRetained),
                    outcome,
                    runtime_filter: Some(telemetry),
                },
            )
        };
        let lookup = |reference| DiagnosticPayloadAddress::RuntimeFilter {
            backend: part.backend_process_id(),
            reference,
        };
        earlier_report.record(earlier, ack(ReleaseOutcome::NotReady, telemetry.clone()));
        assert!(sources.payload(lookup(earlier_ref)).is_none());
        let later_ack = ack(ReleaseOutcome::Released, telemetry);
        assert!(
            lifecycle
                .release_report(part, later_ref)
                .unwrap()
                .record(&later_ack, Some(later_ref))
        );
        assert!(lifecycle.payload(lookup(later_ref)).is_none());
        assert!(lifecycle.take_release_records().is_none());
        later_report.record(later, later_ack);
        let Some(OriginalDiagnosticPayload::RuntimeFilter {
            telemetry: retained,
            ..
        }) = sources.payload(lookup(later_ref))
        else {
            panic!("canonical alias is available before the Core can select it");
        };
        assert!(std::ptr::eq(retained.as_proto(), &original.0));
        assert!(
            sources
                .payload(DiagnosticPayloadAddress::SplitProfile(later_ref))
                .is_none()
        );
        assert!(sources.payload(lookup(earlier_ref)).is_none());
        inbox.commit(earlier_report.receipt_delivery.take().unwrap().unwrap());
        let fact = inbox.commit(later_report.receipt_delivery.take().unwrap().unwrap());
        assert!(matches!(
            fact,
            ExecutorFact::Operation(observation)
                if observation.generation == later.generation
                    && matches!(&observation.reply.result,
                        OperationReplyResult::Worker(OperationResult::Verdict {
                            acknowledgement: Some(PassiveAck::Release {
                                runtime_filter: Some(reference), ..
                            }),
                            ..
                        }) if *reference == later_ref)
        ));
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        for mut report in [earlier_report, later_report] {
            assert!(matches!(
                report
                    .receipt
                    .take()
                    .unwrap()
                    .retire_ready_after_exit()
                    .unwrap(),
                Poll::Ready(Some(_))
            ));
        }
        assert_eq!(Arc::strong_count(&original), 3);
        sources.publication_complete();
        assert!(sources.payload(lookup(later_ref)).is_none());
        assert_eq!(Arc::strong_count(&original), 2);
        drop(retained);
        assert_eq!(Arc::strong_count(&original), 1);
        drop(original);
        let selection = RuntimeFilterDiagnostic {
            complete: true,
            contributions: vec![(part.backend_process_id(), later_ref.0)],
        };
        let Some(RuntimeFilterTerminalRollupSnapshot::Available(rollup)) =
            sources.take_lifecycle_rollup(Some(&selection))
        else {
            panic!("the opt-in lifecycle projection no longer needs the canonical reply");
        };
        assert_eq!(rollup.participants.len(), 1);
        assert_eq!(
            rollup.participants[0].participant.process_id,
            part.backend_process_id()
        );
        assert_eq!(
            lifecycle.take_lifecycle_rollup(Some(&selection)),
            Some(RuntimeFilterTerminalRollupSnapshot::Available(rollup))
        );
        assert!(sources.take_lifecycle_rollup(Some(&selection)).is_none());
        payer.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }

    #[test]
    fn abort_report_preserves_the_raw_worker_receipt_before_input_retirement() {
        use novarocks_execution_contract::{
            OperationOutcome, ParticipationReceipt, ParticipationState,
        };
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let part = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(9, 13), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let key = SendKey {
            operation: TaskOperationId::new_v7(),
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
        };
        let mut report = SendMemberReport::new(
            NativeReceiptInput::new(
                key,
                OperationKind::AbortParticipation,
                inbox.reserve_standing(&work),
            ),
            None,
            FailureRecords::default(),
            None,
        );
        report.retain_abort_receipt(part);
        let original = OperationAcknowledgement::worker_receipt(
            key.operation,
            OperationKind::AbortParticipation,
            OperationOutcome::Accepted,
            AckPayload::Participation(ParticipationReceipt::new(
                part,
                ParticipationState::Aborting,
            )),
        );
        report.record(key, original.clone());
        let input = report.receipt_delivery.take().unwrap().unwrap();
        let slot = report.abort_receipt.take().unwrap();
        assert_eq!(slot.original_for_test(), Some(&original));
        assert!(matches!(
            report
                .receipt
                .as_mut()
                .unwrap()
                .retire_ready_after_exit()
                .unwrap(),
            Poll::Pending
        ));
        assert!(matches!(
            inbox.commit(input),
            ExecutorFact::Operation(observation) if observation.generation == key.generation
        ));
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        let Poll::Ready(Some(retired)) = report
            .receipt
            .as_mut()
            .unwrap()
            .retire_ready_after_exit()
            .unwrap()
        else {
            panic!("original Abort report retired");
        };
        assert_eq!(retired.original, original);
        assert_eq!(slot.original_for_test(), Some(&original));
        payer.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }

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
                        FailureRecords::default(),
                        Some((participation, progress())),
                    ),
                    SendMemberReport::new(
                        NativeReceiptInput::new(
                            cancel,
                            OperationKind::CancelTask,
                            inbox.reserve_standing(&work),
                        ),
                        None,
                        FailureRecords::default(),
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
                abort_receipt: None,
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
