// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Canonical driver deliveries. The core selects every protocol outcome; this
//! host only preserves the same request, payload, operation and original ticket.

use super::{PumpRecordError, project_task_error};
use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    native::data_runtime::FrontendDataRuntime,
    query_execution::{
        connector_domain::TaskUpdateRequest,
        split_assignment::{
            AcceptedPlanNode, AssignmentTarget, SplitAssignmentDriverError, SplitAssignmentStop,
            TaskUpdateOutcome, TaskUpdateTicket, TaskUpdateTransport, TaskUpdateTransportError,
        },
        split_assignment_round::{
            RoundSplitAssignmentPlan, SplitAssignmentProfileRead,
            executor::{OriginalSplitPublisher, SplitAssignmentExecutor},
        },
    },
    task_execution::{error::TaskExecutionError, round::PumpCloseUnresolved},
    wake::StatusIntakeWake,
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, TaskDomainIntent},
    failure::{AttemptFailure, HostFailureRef},
    input::{AttemptInput, ExecutorFact, SplitDeliveryRef},
    publication::SplitDiagnosticObservation,
    split_delivery::SplitDeliverySettlement,
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor_turn::ActorTurnError,
    causal_input::CausalInputObligation,
    exit_custody::{ExitCustody, ExitCustodyPhase},
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{
    PlanNodeId, SplitAssignmentIntent, TaskDomainUpdate, TaskIdentity, TaskOperationId,
};
use novarocks_proto_codec::{FieldPath, lifecycle::QueryExecutionId};
use novarocks_proto_models::connector_read as wire;
use novarocks_query_application::execution_error::ExecutionFailureInput;
use novarocks_spi::connector::read_stack::SplitSourceProfile;
use novarocks_task_codec::domain::{WireContent, split_offer, wire_split_assignment};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_types::UniqueId;
use std::{
    alloc::Layout,
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Debug)]
pub(crate) enum SplitRecordError {
    MissingDelivery,
    MissingPayload,
    DuplicatePayload,
    WrongOperation,
    WrongPump,
    DuplicateRunner,
}
impl std::fmt::Display for SplitRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "split canonical record invariant failed: {self:?}")
    }
}
impl std::error::Error for SplitRecordError {}

type Position = CausalInputObligation<FrontendActor>;

/// The exact standing roles paid by the original Source. The runner fills
/// each once; the Source keeps them until its original Join and input exits.
pub(crate) struct SplitRunnerPublication {
    positions: Mutex<[Option<Position>; 2]>,
}

impl SplitRunnerPublication {
    pub(crate) fn new(failure: Position, profile: Position) -> Arc<Self> {
        Arc::new(Self {
            positions: Mutex::new([Some(failure), Some(profile)]),
        })
    }

    fn fill(&self, fact: ExecutorFact) -> Result<(), TaskExecutionError> {
        let index = match &fact {
            ExecutorFact::PumpFailed { .. } => 0,
            ExecutorFact::SplitProfile { .. } => 1,
            _ => unreachable!("original split publication kind"),
        };
        let mut positions = self.positions.lock().expect("original split positions");
        let position = positions[index].as_mut().expect("live split position");
        position
            .fill_with_receipt(
                None,
                FrontendInput::Attempt(Box::new(AttemptInput::Executor(fact))),
            )
            .map_err(|refused| {
                TaskExecutionError::Schedule(format!(
                    "original split publication refused: {:?}",
                    refused.error
                ))
            })?;
        Ok(())
    }

    pub(crate) fn owns_input(&self, input: InputId) -> bool {
        self.positions
            .lock()
            .expect("original split positions")
            .iter()
            .flatten()
            .any(|position| {
                position
                    .in_flight()
                    .expect("original split input")
                    .contains(&Some(input))
            })
    }

    /// Only the caller of the exact runner Join may retire its producer roles.
    pub(crate) fn retire_after_runner_exit(&self, failed: bool) -> Result<bool, ActorTurnError> {
        let mut positions = self.positions.lock().expect("original split positions");
        for position in positions.iter_mut() {
            let Some(original) = position.as_mut() else {
                continue;
            };
            if failed {
                match original.take_failed_payload() {
                    Ok(Some(neutral)) => {
                        neutral.dispose()?;
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            if original.retire_ready_after_exit()? {
                original.close_after_exit()?;
                *position = None;
            }
        }
        Ok(positions.iter().all(Option::is_none))
    }
}

#[derive(Default)]
struct SplitResults {
    selected_failure: Option<(HostFailureRef, AttemptFailure)>,
    original_failure: Option<TaskExecutionError>,
}

#[derive(Default)]
pub(crate) struct SplitFailureRecord(Mutex<SplitResults>);

/// One original opened assignment plan, its delivery tickets, and immutable
/// outcomes. The transport Source retains this while its runner or any
/// diagnostic publication still needs the original records.
pub(crate) struct SplitPump {
    pump: PayloadRef,
    executor: Arc<SplitAssignmentExecutor>,
    deliveries: Arc<SplitDeliveryRecords>,
    results: Arc<SplitFailureRecord>,
    profile: Arc<OnceLock<SplitSourceProfile>>,
}

impl SplitPump {
    pub(crate) fn prepare(
        pump: PayloadRef,
        execution: QueryExecutionId,
        plan: RoundSplitAssignmentPlan,
        tasks: BTreeMap<UniqueId, TaskIdentity>,
        wake: Arc<dyn StatusIntakeWake>,
        data_runtime: FrontendDataRuntime,
        profile: Arc<OnceLock<SplitSourceProfile>>,
    ) -> Option<Arc<Self>> {
        let deliveries = SplitDeliveryRecords::new(execution, tasks, wake);
        let executor =
            SplitAssignmentExecutor::prepare(execution, plan, deliveries.clone(), data_runtime)?;
        deliveries
            .install_runner_wake(executor.wake_handle())
            .expect("new split records have no runner");
        Some(Arc::new(Self {
            pump,
            executor,
            deliveries,
            results: Arc::default(),
            profile,
        }))
    }

    pub(crate) const fn reference(&self) -> PayloadRef {
        self.pump
    }

    pub(crate) fn bind_runner(
        self: &Arc<Self>,
        start: AttemptActionId,
        custody: ExitCustody,
        publication: Arc<SplitRunnerPublication>,
    ) -> BoundSplitRunner {
        BoundSplitRunner {
            original: self.clone(),
            failure: HostFailureRef(NonZeroU64::new(start.get()).expect("original split action")),
            custody,
            publication,
        }
    }

    pub(crate) fn request_stop(&self) {
        self.executor.request_stop();
    }

    pub(crate) fn has_started(&self) -> bool {
        self.executor.has_started()
    }

    /// Called inside a separate credentialed Cleanup job when the original
    /// Start never ran. The caller retains the same actual Join and receives
    /// the original close result without a Core split recipient.
    pub(crate) async fn close_unstarted(
        self: Arc<Self>,
        custody: ExitCustody,
    ) -> Result<(), TaskExecutionError> {
        if custody.phase() != ExitCustodyPhase::Started {
            return Err(TaskExecutionError::Schedule(
                "unstarted split close requires the original started Cleanup work".into(),
            ));
        }
        let Some(result) = self.executor.clone().close_unstarted().await else {
            return Err(TaskExecutionError::Schedule(
                "split cleanup cannot replace an already started runner".into(),
            ));
        };
        self.deliveries.close_external();
        result
    }

    pub(crate) fn unresolved(&self) -> Option<PumpCloseUnresolved> {
        self.executor.unresolved()
    }

    pub(crate) fn deliveries(&self) -> &SplitDeliveryRecords {
        &self.deliveries
    }

    pub(crate) fn failure_record(&self) -> Arc<SplitFailureRecord> {
        self.results.clone()
    }

    pub(crate) fn apply_notice(&self, body: &EffectBody) -> Result<(), SplitRecordError> {
        match body {
            EffectBody::PumpUpdateAdmitted {
                pump,
                delivery,
                settlement,
            } if *pump == self.pump => self
                .deliveries
                .admission_chosen(*delivery, settlement.clone()),
            EffectBody::PumpOperationReleased {
                pump,
                delivery,
                operation,
            } if *pump == self.pump => self.deliveries.operation_released(*delivery, *operation),
            EffectBody::PumpOperationSettled {
                pump,
                delivery,
                operation,
                settlement,
            } if *pump == self.pump => {
                self.deliveries
                    .operation_settled(*delivery, *operation, settlement.clone())
            }
            _ => Err(SplitRecordError::WrongPump),
        }
    }

    fn record_failure(
        &self,
        reference: HostFailureRef,
        original: TaskExecutionError,
    ) -> AttemptFailure {
        let mut results = self.results.0.lock().expect("original split results");
        if let Some((selected, failure)) = &results.selected_failure {
            assert_eq!(*selected, reference, "same original split runner");
            return failure.clone();
        }
        let failure = project_task_error(reference, &original);
        results.selected_failure = Some((reference, failure.clone()));
        results.original_failure = Some(original);
        failure
    }

    #[cfg(test)]
    pub(crate) fn profile_lookup(&self) -> (PayloadRef, Arc<OnceLock<SplitSourceProfile>>) {
        (self.pump, self.profile.clone())
    }
}

impl SplitFailureRecord {
    pub(crate) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<ExecutionFailureInput> {
        let results = self.0.lock().expect("original split results");
        if results.selected_failure.as_ref()?.0 != reference {
            return None;
        }
        Some(results.original_failure.as_ref()?.preparation_input())
    }

    pub(crate) fn take_failure(&self, reference: HostFailureRef) -> Option<TaskExecutionError> {
        let mut results = self.0.lock().expect("original split results");
        if results.selected_failure.as_ref()?.0 != reference {
            return None;
        }
        results.original_failure.take()
    }
}

/// Run under the common transport wrapper's original ExitObligation. The
/// returned PumpFinished is a result; that wrapper still owes actual exit.
pub(crate) struct BoundSplitRunner {
    original: Arc<SplitPump>,
    failure: HostFailureRef,
    custody: ExitCustody,
    publication: Arc<SplitRunnerPublication>,
}

impl BoundSplitRunner {
    pub(crate) async fn run(self) -> Result<ExecutorFact, PumpRecordError> {
        if self.custody.phase() != ExitCustodyPhase::Started {
            return Err(PumpRecordError::WrongIdentity);
        }
        let result = self.original.executor.clone().run_admitted(&self).await;
        self.original.deliveries.close_external();
        if let Some(unresolved) = self.original.unresolved() {
            return Err(PumpRecordError::SplitCloseUnresolved(unresolved));
        }
        Ok(ExecutorFact::PumpFinished {
            pump: self.original.pump,
            result: result
                .map(|_| ())
                .map_err(|original| self.original.record_failure(self.failure, original)),
        })
    }
}

impl OriginalSplitPublisher for BoundSplitRunner {
    fn publish_original_failure(
        &self,
        failure: SplitAssignmentDriverError,
    ) -> Result<(), TaskExecutionError> {
        let failure = self
            .original
            .record_failure(self.failure, TaskExecutionError::SplitAssignment(failure));
        self.publication.fill(ExecutorFact::PumpFailed {
            pump: self.original.pump,
            failure,
        })
    }

    fn publish_original_profile(
        &self,
        observation: SplitAssignmentProfileRead,
    ) -> Result<(), TaskExecutionError> {
        let observation = match observation {
            SplitAssignmentProfileRead::Available(profile) => {
                assert!(
                    self.original.profile.set(profile).is_ok(),
                    "one original split profile"
                );
                SplitDiagnosticObservation::Available(self.original.pump)
            }
            SplitAssignmentProfileRead::Failed => SplitDiagnosticObservation::Failed,
            SplitAssignmentProfileRead::Pending | SplitAssignmentProfileRead::SourceGone => {
                unreachable!("split runner publishes its original settled profile")
            }
        };
        self.publication.fill(ExecutorFact::SplitProfile {
            pump: self.original.pump,
            observation,
        })
    }
}

struct Delivery {
    task: TaskIdentity,
    update: TaskDomainUpdate,
    backing_bytes: usize,
    payload: Option<PayloadRef>,
    outcome: Option<SplitDeliverySettlement>,
}
struct Records {
    closing: bool,
    retired: VecDeque<Delivery>,
    next: u64,
    deliveries: BTreeMap<SplitDeliveryRef, Delivery>,
    pending: VecDeque<SplitDeliveryRef>,
    by_task: BTreeMap<TaskIdentity, SplitDeliveryRef>,
    by_operation: BTreeMap<TaskOperationId, SplitDeliveryRef>,
}
pub(crate) struct SplitDeliveryRecords {
    execution: QueryExecutionId,
    tasks: BTreeMap<UniqueId, TaskIdentity>,
    records: Mutex<Records>,
    wake: Arc<dyn StatusIntakeWake>,
    runner_wake: Mutex<Option<Arc<dyn StatusIntakeWake>>>,
}
/// Completed original records move intact to the Source disposal executor.
pub(crate) struct SplitRetiredDelivery {
    pub(crate) payload: Option<PayloadRef>,
    pub(crate) original: Box<dyn std::any::Any + Send>,
}
/// The original Source assigns a canonical payload reference before publishing
/// this delivery. The assignment aliases its original encoder-produced bytes.
pub(crate) struct SplitPayloadRegistration {
    pub(crate) delivery: SplitDeliveryRef,
    pub(crate) task: TaskIdentity,
    pub(crate) assignment: SplitAssignmentIntent,
    pub(crate) backing_bytes: usize,
}
impl SplitDeliveryRecords {
    pub(crate) fn new(
        execution: QueryExecutionId,
        tasks: BTreeMap<UniqueId, TaskIdentity>,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Arc<Self> {
        Arc::new(Self {
            execution,
            tasks,
            wake,
            runner_wake: Mutex::new(None),
            records: Mutex::new(Records {
                closing: false,
                retired: VecDeque::new(),
                next: 0,
                deliveries: BTreeMap::new(),
                pending: VecDeque::new(),
                by_task: BTreeMap::new(),
                by_operation: BTreeMap::new(),
            }),
        })
    }
    pub(crate) fn install_runner_wake(
        &self,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<(), SplitRecordError> {
        let mut original = self.runner_wake.lock().expect("original split runner wake");
        if original.is_some() {
            return Err(SplitRecordError::DuplicateRunner);
        }
        *original = Some(wake);
        Ok(())
    }
    fn wake_originals(&self) {
        let runner = self
            .runner_wake
            .lock()
            .expect("original split runner wake")
            .clone();
        if let Some(runner) = runner {
            runner.wake();
        }
        self.wake.wake();
    }
    /// The original driver returned from its provider close. Seal fresh input
    /// while retaining any canonical request and its original operation.
    fn close_external(&self) {
        self.records
            .lock()
            .expect("split canonical records")
            .closing = true;
        self.wake_originals();
    }
    /// Only tickets consumed by the original driver reach this queue. Source
    /// removal of the corresponding payload record and rich disposal remain
    /// outside this lock, through its existing admitted disposal work.
    pub(crate) fn take_retired_one(&self) -> Option<SplitRetiredDelivery> {
        let mut records = self.records.lock().expect("split canonical records");
        records.retired.pop_front().map(SplitRetiredDelivery::from)
    }

    #[cfg(test)]
    pub(crate) fn take_retired(&self) -> Vec<SplitRetiredDelivery> {
        std::iter::from_fn(|| self.take_retired_one()).collect()
    }

    /// The original Source calls this only after joining the runner and after
    /// its Core can no longer issue a disposition for the exact delivery.
    /// Preserve the canonical payload for the same credentialed disposal path.
    pub(crate) fn take_remaining_after_close(&self) -> Option<SplitRetiredDelivery> {
        let mut records = self.records.lock().expect("split canonical records");
        assert!(records.closing, "split delivery producer has not closed");
        let (delivery, original) = records.deliveries.pop_first()?;
        records.pending.retain(|held| *held != delivery);
        records.by_task.remove(&original.task);
        records.by_operation.retain(|_, held| *held != delivery);
        Some(original.into())
    }
    pub(crate) fn producer_closed(&self) -> bool {
        self.records
            .lock()
            .expect("split canonical records")
            .closing
    }

    pub(crate) fn next_unregistered(&self) -> Option<SplitPayloadRegistration> {
        let records = self.records.lock().expect("split canonical records");
        let &delivery = records.pending.front()?;
        let record = &records.deliveries[&delivery];
        if record.payload.is_some() {
            return None;
        }
        let TaskDomainUpdate::SplitAssignment(update) = &record.update else {
            unreachable!("the original bridge freezes only split assignments")
        };
        Some(SplitPayloadRegistration {
            delivery,
            task: record.task,
            assignment: update.clone(),
            backing_bytes: record.backing_bytes,
        })
    }
    pub(crate) fn register_payload(
        &self,
        delivery: SplitDeliveryRef,
        payload: PayloadRef,
    ) -> Result<(), SplitRecordError> {
        let mut records = self.records.lock().expect("split canonical records");
        if records
            .deliveries
            .values()
            .any(|r| r.payload == Some(payload))
        {
            return Err(SplitRecordError::DuplicatePayload);
        }
        let record = records
            .deliveries
            .get_mut(&delivery)
            .ok_or(SplitRecordError::MissingDelivery)?;
        if record.payload.is_some() {
            return Err(SplitRecordError::DuplicatePayload);
        }
        record.payload = Some(payload);
        Ok(())
    }
    /// Called by the original preadmitted Source, which fills its standing
    /// input before committing this dequeue. Ordinary Full keeps the same row.
    pub(crate) fn next_input(
        &self,
        pump: PayloadRef,
    ) -> Result<Option<ExecutorFact>, SplitRecordError> {
        let records = self.records.lock().expect("split canonical records");
        let Some(&delivery) = records.pending.front() else {
            return Ok(None);
        };
        let record = &records.deliveries[&delivery];
        let payload = record.payload.ok_or(SplitRecordError::MissingPayload)?;
        let TaskDomainUpdate::SplitAssignment(update) = &record.update else {
            unreachable!("split-only canonical input")
        };
        Ok(Some(ExecutorFact::PumpTaskUpdate {
            pump,
            delivery,
            task: record.task,
            domains: Box::new([TaskDomainIntent::SplitAssignment {
                node: update.node(),
                offer: update.offer(),
                payload,
                queued_bytes: update.payload().encoded_len(),
            }]),
        }))
    }
    pub(crate) fn input_published(
        &self,
        delivery: SplitDeliveryRef,
    ) -> Result<(), SplitRecordError> {
        let mut records = self.records.lock().expect("split canonical records");
        if records.pending.front() != Some(&delivery) {
            return Err(SplitRecordError::MissingDelivery);
        }
        records.pending.pop_front();
        Ok(())
    }
    /// None is the core's already-selected Queued/Preparing result. Host does
    /// not reclassify admission, terminal state or original frontend errors.
    pub(crate) fn admission_chosen(
        &self,
        delivery: SplitDeliveryRef,
        settlement: Option<SplitDeliverySettlement>,
    ) -> Result<(), SplitRecordError> {
        let mut records = self.records.lock().expect("split canonical records");
        let record = records
            .deliveries
            .get_mut(&delivery)
            .ok_or(SplitRecordError::MissingDelivery)?;
        if let Some(settlement) = settlement {
            record.outcome = Some(settlement);
        }
        drop(records);
        self.wake_originals();
        Ok(())
    }
    pub(crate) fn operation_released(
        &self,
        delivery: SplitDeliveryRef,
        operation: TaskOperationId,
    ) -> Result<(), SplitRecordError> {
        let mut records = self.records.lock().expect("split canonical records");
        if !records.deliveries.contains_key(&delivery) {
            return Err(SplitRecordError::MissingDelivery);
        }
        if records
            .by_operation
            .get(&operation)
            .is_some_and(|bound| *bound != delivery)
        {
            return Err(SplitRecordError::WrongOperation);
        }
        records.by_operation.insert(operation, delivery);
        Ok(())
    }
    pub(crate) fn operation_settled(
        &self,
        delivery: SplitDeliveryRef,
        operation: TaskOperationId,
        settlement: SplitDeliverySettlement,
    ) -> Result<(), SplitRecordError> {
        let mut records = self.records.lock().expect("split canonical records");
        if records.by_operation.get(&operation) != Some(&delivery) {
            return Err(SplitRecordError::WrongOperation);
        }
        let retains_request = matches!(settlement, SplitDeliverySettlement::Unknown(_));
        records
            .deliveries
            .get_mut(&delivery)
            .ok_or(SplitRecordError::MissingDelivery)?
            .outcome = Some(settlement);
        if !retains_request {
            records.by_operation.remove(&operation);
        }
        drop(records);
        self.wake_originals();
        Ok(())
    }
}

impl From<Delivery> for SplitRetiredDelivery {
    fn from(original: Delivery) -> Self {
        Self {
            payload: original.payload,
            original: Box::new(original),
        }
    }
}

impl TaskUpdateTransport for SplitDeliveryRecords {
    fn begin(
        &self,
        execution: QueryExecutionId,
        target: &AssignmentTarget,
        request: &TaskUpdateRequest,
    ) -> Result<TaskUpdateTicket, TaskUpdateTransportError> {
        if execution != self.execution {
            return Err(TaskUpdateTransportError::fatal(format!(
                "split delivery belongs to {:?}, not {execution:?}",
                self.execution
            )));
        }
        if request.task_key() != target.task_key {
            return Err(TaskUpdateTransportError::fatal(
                "split delivery request does not match its original target",
            ));
        }
        let task = self.tasks.get(&target.task_key).copied().ok_or_else(|| {
            TaskUpdateTransportError::fatal("fragment instance is not a task of this attempt")
        })?;
        let (update, backing_bytes) =
            freeze_update(request).map_err(TaskUpdateTransportError::fatal)?;
        let mut records = self.records.lock().expect("split canonical records");
        if records.closing {
            return Err(TaskUpdateTransportError::closed(
                "split assignment source admission is closed",
            ));
        }
        if let Some(delivery) = records.by_task.get(&task).copied() {
            if !same_update(&records.deliveries[&delivery].update, &update) {
                return Err(TaskUpdateTransportError::fatal(format!(
                    "task {} already has a different split assignment in flight",
                    task.task_id()
                )));
            }
            return Ok(TaskUpdateTicket(delivery.0.get()));
        }
        records.next = records.next.checked_add(1).ok_or_else(|| {
            TaskUpdateTransportError::fatal("split canonical delivery identity exhausted")
        })?;
        let delivery = SplitDeliveryRef(
            NonZeroU64::new(records.next).expect("first original delivery is one"),
        );
        records.deliveries.insert(
            delivery,
            Delivery {
                task,
                update,
                backing_bytes,
                payload: None,
                outcome: None,
            },
        );
        records.by_task.insert(task, delivery);
        records.pending.push_back(delivery);
        drop(records);
        self.wake_originals();
        Ok(TaskUpdateTicket(delivery.0.get()))
    }
    fn poll(
        &self,
        ticket: TaskUpdateTicket,
        stop: &SplitAssignmentStop,
    ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
        if stop.is_stopped() {
            return Some(Err(TaskUpdateTransportError::closed(
                "split assignment round stopped while a task update was outstanding",
            )));
        }
        let Some(id) = NonZeroU64::new(ticket.0) else {
            return Some(Err(TaskUpdateTransportError::fatal(
                "split delivery token is zero",
            )));
        };
        let delivery = SplitDeliveryRef(id);
        let mut records = self.records.lock().expect("split canonical records");
        let Some(record) = records.deliveries.get_mut(&delivery) else {
            return Some(Err(TaskUpdateTransportError::fatal(format!(
                "split delivery {} is not live",
                ticket.0
            ))));
        };
        let settlement = record.outcome.take()?;
        let retains_request = matches!(settlement, SplitDeliverySettlement::Unknown(_));
        if !retains_request {
            let task = record.task;
            let original = records
                .deliveries
                .remove(&delivery)
                .expect("same original settled delivery");
            records.retired.push_back(original);
            records.by_task.remove(&task);
            records.by_operation.retain(|_, bound| *bound != delivery);
        }
        drop(records);
        self.wake_originals();
        Some(match settlement {
            SplitDeliverySettlement::Accepted(nodes) => Ok(TaskUpdateOutcome::Accepted(
                nodes
                    .into_vec()
                    .into_iter()
                    .map(|node| AcceptedPlanNode {
                        plan_node_id: node.plan_node_id,
                        accepted_through_sequence: node.accepted_through_sequence,
                        no_more_splits: node.no_more_splits,
                        queued_splits: node.queued_splits,
                    })
                    .collect(),
            )),
            SplitDeliverySettlement::Rejected { reason, detail } => {
                Ok(TaskUpdateOutcome::Rejected { reason, detail })
            }
            SplitDeliverySettlement::DestinationFinished(detail) => {
                Ok(TaskUpdateOutcome::DestinationFinished { detail })
            }
            SplitDeliverySettlement::Local(detail) => Err(TaskUpdateTransportError::fatal(detail)),
            SplitDeliverySettlement::Unknown(detail) => {
                Err(TaskUpdateTransportError::retryable_network(detail))
            }
        })
    }
}
/// This is the old bridge's exact codec projection, not a protocol verdict.
fn freeze_update(request: &TaskUpdateRequest) -> Result<(TaskDomainUpdate, usize), String> {
    let [assignment] = request.assignments() else {
        return Err(format!(
            "a task update must carry exactly one assignment, not {}",
            request.assignments().len()
        ));
    };
    let node = PlanNodeId::new(assignment.plan_node_id())
        .map_err(|error| format!("plan node {}: {error}", assignment.plan_node_id()))?;
    let proto = request
        .to_proto_assignments()?
        .into_iter()
        .next()
        .ok_or_else(|| "a validated assignment produced no wire assignment".to_owned())?;
    let offer = split_offer(&proto, FieldPath::root("split_assignment"))
        .map_err(|error| error.to_string())?;
    let backing_bytes = split_backing_bytes(&proto)
        .ok_or_else(|| "split payload backing exceeds addressable size".to_owned())?;
    Ok((
        TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
            node,
            offer,
            wire_split_assignment(proto),
        )),
        backing_bytes,
    ))
}

/// The frozen Arc owns these exact generated vectors and strings. Measure
/// their capacities before moving them into the immutable wire carrier.
fn split_backing_bytes(assignment: &wire::SplitAssignment) -> Option<usize> {
    use wire::connector_split::Category;

    let mut total = arc_layout(Layout::new::<WireContent<wire::SplitAssignment>>())
        .ok()?
        .size();
    let mut add = |bytes| {
        total = total.checked_add(bytes)?;
        Some(())
    };
    add(
        Layout::array::<wire::ScheduledSplit>(assignment.splits.capacity())
            .ok()?
            .size(),
    )?;
    for scheduled in &assignment.splits {
        let Some(split) = &scheduled.split else {
            continue;
        };
        add(
            Layout::array::<wire::HostAddress>(split.addresses.capacity())
                .ok()?
                .size(),
        )?;
        for address in &split.addresses {
            add(address.host.capacity())?;
        }
        if let Some(affinity) = &split.affinity_key {
            add(affinity.capacity())?;
        }
        let payload = match &split.category {
            Some(Category::Data(source)) => source.provider_payload.as_ref(),
            Some(Category::TableChanges(source)) => source.provider_payload.as_ref(),
            Some(Category::ChangeWindow(source)) => source.provider_payload.as_ref(),
            Some(Category::SystemManifest(source)) => source.provider_payload.as_ref(),
            Some(Category::RewritePositionDeleteFiles(source)) => source.provider_payload.as_ref(),
            Some(Category::SystemRelation(source)) => source.provider_payload.as_ref(),
            None => None,
        };
        if let Some(payload) = payload {
            add(payload.payload.capacity())?;
            if let Some(header) = &payload.header {
                add(header.provider_id.capacity())?;
                if let Some(catalog) = &header.catalog {
                    add(catalog.catalog_name.capacity())?;
                    add(catalog.version.capacity())?;
                }
            }
        }
    }
    Some(total)
}
fn same_update(held: &TaskDomainUpdate, offered: &TaskDomainUpdate) -> bool {
    let (TaskDomainUpdate::SplitAssignment(held), TaskDomainUpdate::SplitAssignment(offered)) =
        (held, offered)
    else {
        return false;
    };
    held.node() == offered.node()
        && held.offer() == offered.offer()
        && held.payload().fingerprint() == offered.payload().fingerprint()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[test]
fn split_payload_quote_includes_original_capacity_outside_wire_length() {
    let empty = wire::SplitAssignment::default();
    let mut assignment = wire::SplitAssignment {
        splits: Vec::with_capacity(3),
        ..empty.clone()
    };
    let mut split = wire::ConnectorSplit {
        addresses: Vec::with_capacity(4),
        affinity_key: Some(String::with_capacity(19)),
        ..Default::default()
    };
    split.addresses.push(wire::HostAddress {
        host: String::with_capacity(27),
        port: 0,
    });
    let extra = assignment.splits.capacity() * size_of::<wire::ScheduledSplit>()
        + split.addresses.capacity() * size_of::<wire::HostAddress>()
        + split.addresses[0].host.capacity()
        + split.affinity_key.as_ref().unwrap().capacity();
    assignment.splits.push(wire::ScheduledSplit {
        split: Some(split),
        ..Default::default()
    });
    assert_eq!(
        split_backing_bytes(&assignment).unwrap(),
        split_backing_bytes(&empty).unwrap() + extra
    );
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dormant_split_cleanup_keeps_the_original_provider_close_and_work_exit() {
    use crate::{
        query_execution::split_assignment_round::failure_observation_plan,
        task_execution::{
            blocking_io::tests::{OriginalExecutorsFixture, until},
            status_intake::CountingWake,
        },
    };
    use novarocks_control_runtime::{
        completion::ReportSlot,
        exit_work::{PanicSink, run_future},
        reported_work::ReportedWork,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        completion::ConsumerKind,
        identity::{ConsumerId, Generation},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_types::{AttemptId, QueryId};
    use std::{
        sync::{atomic::Ordering, mpsc},
        time::Duration,
    };

    let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let supervisor = data_runtime.connector_blocking_io().clone();
    let mut workers = OriginalExecutorsFixture::install(&supervisor);
    let execution =
        QueryExecutionId::new(QueryId::new(19, 23), AttemptId::new(1).unwrap()).unwrap();
    let (release, released) = mpsc::channel();
    let (plan, enumerations, closes) =
        failure_observation_plan(execution, supervisor, Some(released));
    let original = SplitPump::prepare(
        PayloadRef(NonZeroU64::new(1).unwrap()),
        execution,
        plan,
        BTreeMap::new(),
        Arc::new(CountingWake::default()),
        data_runtime,
        Arc::default(),
    )
    .unwrap();
    assert!(!original.has_started());

    let ledger = RuntimeLedger::new(ResourceVector::default());
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let (work, mut payer) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let observer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut payer,
        generation,
        1,
        [(observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let (custody, exit) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, payer, report.clone()).unwrap());
    let job = tokio::spawn(run_future(
        exit,
        original.clone().close_unstarted(custody.clone()),
        PanicSink::new(),
    ));
    until(|| closes.load(Ordering::SeqCst) == 1);
    assert!(original.has_started());
    assert_eq!(enumerations.load(Ordering::SeqCst), 0);
    assert_eq!(custody.phase(), ExitCustodyPhase::Started);
    assert!(!job.is_finished());
    let duplicate = original.clone().close_unstarted(custody.clone()).await;
    assert!(matches!(duplicate, Err(TaskExecutionError::Schedule(_))));
    assert!(!original.deliveries.records.lock().unwrap().closing);

    release.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), job)
        .await
        .expect("original provider close returns")
        .expect("original Cleanup runner joins")
        .expect("original work exits");
    assert!(matches!(
        result,
        Err(TaskExecutionError::SplitAssignment(
            SplitAssignmentDriverError::SourceClose { .. }
        ))
    ));
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert_eq!(enumerations.load(Ordering::SeqCst), 0);
    assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
    assert!(original.profile_lookup().1.get().is_none());
    assert!(original.deliveries().take_remaining_after_close().is_none());
    report.consume(observer).unwrap();
    assert!(custody.try_retire().unwrap());
    ledger.reap_scope_ready(scope.id(), 32).unwrap();
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    drop(original);
    workers.shutdown().await;
}
