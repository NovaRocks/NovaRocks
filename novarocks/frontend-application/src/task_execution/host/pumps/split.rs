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

use crate::query_execution::{
    connector_domain::TaskUpdateRequest,
    split_assignment::{
        AcceptedPlanNode, AssignmentTarget, SplitAssignmentStop, TaskUpdateOutcome,
        TaskUpdateTicket, TaskUpdateTransport, TaskUpdateTransportError,
    },
};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_attempt_protocol::{
    effect::TaskDomainIntent,
    input::{ExecutorFact, SplitDeliveryRef},
    split_delivery::SplitDeliverySettlement,
    start::PayloadRef,
};
use novarocks_execution_contract::{
    CodecOwnedContent, PlanNodeId, SplitAssignmentIntent, TaskDomainUpdate, TaskIdentity,
    TaskOperationId,
};
use novarocks_proto_codec::{FieldPath, lifecycle::QueryExecutionId};
use novarocks_task_codec::domain::{split_offer, wire_split_assignment};
use novarocks_types::UniqueId;
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) enum SplitRecordError {
    MissingDelivery,
    MissingPayload,
    DuplicatePayload,
    WrongOperation,
    DuplicateRunner,
}
impl std::fmt::Display for SplitRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "split canonical record invariant failed: {self:?}")
    }
}
impl std::error::Error for SplitRecordError {}
struct Delivery {
    task: TaskIdentity,
    update: TaskDomainUpdate,
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
/// this delivery. The returned Arc aliases its original encoder-produced bytes.
pub(crate) struct SplitPayloadRegistration {
    pub(crate) delivery: SplitDeliveryRef,
    pub(crate) task: TaskIdentity,
    pub(crate) payload: Arc<dyn CodecOwnedContent>,
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
    /// Closing stops fresh driver input but does not erase a pending canonical
    /// request or operation. Their original outcomes and actual jobs still exit.
    pub(crate) fn close_external(&self) {
        self.records
            .lock()
            .expect("split canonical records")
            .closing = true;
        self.wake_originals();
    }
    /// Only tickets consumed by the original driver reach this queue. Source
    /// removal of the corresponding payload record and rich disposal remain
    /// outside this lock, through its existing admitted disposal work.
    pub(crate) fn take_retired(&self) -> Vec<SplitRetiredDelivery> {
        let mut records = self.records.lock().expect("split canonical records");
        records
            .retired
            .drain(..)
            .map(|record| SplitRetiredDelivery {
                payload: record.payload,
                original: Box::new(record),
            })
            .collect()
    }
    pub(crate) fn next_unregistered(&self) -> Option<SplitPayloadRegistration> {
        let records = self.records.lock().expect("split canonical records");
        let (&delivery, record) = records
            .deliveries
            .iter()
            .find(|(_, r)| r.payload.is_none())?;
        let TaskDomainUpdate::SplitAssignment(update) = &record.update else {
            unreachable!("the original bridge freezes only split assignments")
        };
        Some(SplitPayloadRegistration {
            delivery,
            task: record.task,
            payload: update.payload().clone(),
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
        let update = freeze_update(request).map_err(TaskUpdateTransportError::fatal)?;
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
fn freeze_update(request: &TaskUpdateRequest) -> Result<TaskDomainUpdate, String> {
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
    Ok(TaskDomainUpdate::SplitAssignment(
        SplitAssignmentIntent::new(node, offer, wire_split_assignment(proto)),
    ))
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
