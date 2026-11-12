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

use super::*;
use crate::query_execution::connector_domain::PlanNodeAssignmentState;
use crate::query_execution::split_assignment::TaskUpdateTransportErrorKind;
use crate::task_execution::status_intake::CountingWake;
use novarocks_spi::connector::ConnectorReadWireEncoder;
use novarocks_types::{AttemptId, BackendProcessId, QueryId, StageId, TaskId};

/// A split source is never enumerated by these tests, so only the encoder
/// methods a terminal marker reaches may be reachable.
struct TerminalOnlyEncoder;

impl ConnectorReadWireEncoder for TerminalOnlyEncoder {
    fn owner(&self) -> &str {
        "split-delivery-test"
    }

    fn encode_relation_payload(
        &self,
        _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadRelationPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("split delivery tests never encode a relation")
    }

    fn encode_column_payload(
        &self,
        _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("split delivery tests never encode a column")
    }

    fn encode_transaction_payload(
        &self,
        _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("split delivery tests never encode a transaction")
    }

    fn encode_split_payload(
        &self,
        _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadSplitPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("split delivery tests never enumerate a split")
    }
}

fn fixture() -> (Arc<SplitDeliveryRecords>, AssignmentTarget, TaskIdentity) {
    let execution =
        QueryExecutionId::new(QueryId::new(41, 42), AttemptId::new(1).unwrap()).unwrap();
    let task = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let target = AssignmentTarget {
        backend_idx: 0,
        task_key: UniqueId::new(1, 2),
    };
    let records = SplitDeliveryRecords::new(
        execution,
        BTreeMap::from([(target.task_key, task)]),
        Arc::new(CountingWake::default()),
    );
    (records, target, task)
}
fn request(target: &AssignmentTarget, node: i32) -> TaskUpdateRequest {
    let assignment = PlanNodeAssignmentState::new()
        .assign(node, vec![], true, Arc::new(TerminalOnlyEncoder))
        .unwrap();
    TaskUpdateRequest::new(target.task_key, vec![assignment])
}
fn publish(records: &SplitDeliveryRecords, pump: PayloadRef) -> SplitDeliveryRef {
    let original = records.next_unregistered().unwrap();
    records
        .register_payload(original.delivery, PayloadRef(NonZeroU64::new(17).unwrap()))
        .unwrap();
    let input = records.next_input(pump).unwrap().unwrap();
    assert!(
        matches!(input, ExecutorFact::PumpTaskUpdate { delivery, domains, .. }
        if delivery == original.delivery && domains.len() == 1)
    );
    records.input_published(original.delivery).unwrap();
    original.delivery
}
#[test]
fn unknown_reattaches_same_canonical_delivery_and_reports_exact_selected_queue_depth() {
    let (records, target, task) = fixture();
    let update = request(&target, 5);
    let ticket = records
        .begin(task.query_execution_id(), &target, &update)
        .unwrap();
    let pump = PayloadRef(NonZeroU64::new(11).unwrap());
    let delivery = publish(&records, pump);
    let operation = TaskOperationId::new_v7();
    records.operation_released(delivery, operation).unwrap();
    records
        .operation_settled(
            delivery,
            operation,
            SplitDeliverySettlement::Unknown("original transport outcome".into()),
        )
        .unwrap();
    let stop = SplitAssignmentStop::default();
    let observed = records.poll(ticket, &stop).unwrap().unwrap_err();
    assert_eq!(
        observed.kind(),
        TaskUpdateTransportErrorKind::RetryableNetwork
    );
    let same = records
        .begin(task.query_execution_id(), &target, &update)
        .unwrap();
    assert_eq!(same, ticket);
    assert!(records.next_input(pump).unwrap().is_none());
    records
        .operation_settled(
            delivery,
            operation,
            SplitDeliverySettlement::Accepted(Box::new([
                novarocks_attempt_protocol::split_delivery::AcceptedPlanNode {
                    plan_node_id: 5,
                    accepted_through_sequence: 29,
                    no_more_splits: true,
                    queued_splits: 7,
                },
            ])),
        )
        .unwrap();
    let result = records.poll(ticket, &stop).unwrap().unwrap();
    assert!(
        matches!(result, TaskUpdateOutcome::Accepted(nodes) if nodes.len()==1
        && nodes[0].plan_node_id == 5 && nodes[0].accepted_through_sequence == 29
        && nodes[0].no_more_splits && nodes[0].queued_splits == 7)
    );
    assert!(records.records.lock().unwrap().deliveries.is_empty());
}
#[test]
fn local_discard_resolves_the_original_ticket_without_a_fabricated_operation() {
    let (records, target, task) = fixture();
    let ticket = records
        .begin(task.query_execution_id(), &target, &request(&target, 5))
        .unwrap();
    let delivery = publish(&records, PayloadRef(NonZeroU64::new(11).unwrap()));
    records
        .admission_chosen(
            delivery,
            Some(SplitDeliverySettlement::DestinationFinished(
                "core selected original no-longer-needed input".into(),
            )),
        )
        .unwrap();
    let outcome = records
        .poll(ticket, &SplitAssignmentStop::default())
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome,
        TaskUpdateOutcome::DestinationFinished { .. }
    ));
    assert!(records.records.lock().unwrap().by_operation.is_empty());
}
#[test]
fn contrary_request_cannot_replace_the_original_canonical_payload() {
    let (records, target, task) = fixture();
    let original = request(&target, 5);
    let first = records
        .begin(task.query_execution_id(), &target, &original)
        .unwrap();
    let contrary = records
        .begin(task.query_execution_id(), &target, &request(&target, 6))
        .unwrap_err();
    assert_eq!(contrary.kind(), TaskUpdateTransportErrorKind::Fatal);
    assert_eq!(
        records
            .begin(task.query_execution_id(), &target, &original)
            .unwrap(),
        first
    );
    let records = records.records.lock().unwrap();
    assert_eq!(records.deliveries.len(), 1);
    assert_eq!(records.pending.len(), 1);
}

#[test]
fn selected_local_result_wakes_the_same_physical_runner_and_preserves_disposal() {
    let (records, target, task) = fixture();
    let runner_wake = Arc::new(CountingWake::default());
    records.install_runner_wake(runner_wake.clone()).unwrap();
    assert!(matches!(
        records.install_runner_wake(runner_wake.clone()),
        Err(SplitRecordError::DuplicateRunner)
    ));
    let ticket = records
        .begin(task.query_execution_id(), &target, &request(&target, 5))
        .unwrap();
    let delivery = publish(&records, PayloadRef(NonZeroU64::new(11).unwrap()));
    let before = runner_wake.count();
    records
        .admission_chosen(
            delivery,
            Some(SplitDeliverySettlement::Local("core discard".into())),
        )
        .unwrap();
    assert_eq!(runner_wake.count(), before + 1);
    assert!(
        records
            .poll(ticket, &SplitAssignmentStop::default())
            .unwrap()
            .is_err()
    );
    let retired = records.take_retired();
    assert_eq!(retired.len(), 1);
    assert_eq!(
        retired[0].payload,
        Some(PayloadRef(NonZeroU64::new(17).unwrap()))
    );
    assert!(retired[0].original.is::<Delivery>());
    assert!(records.take_retired().is_empty());
}
