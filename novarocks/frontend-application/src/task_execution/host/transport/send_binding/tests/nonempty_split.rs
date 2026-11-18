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

//! Original nonempty split records and Native payload storage. This is the
//! same component tier as the retired graph/retry test, not a BE execution.

use super::*;
use crate::query_execution::{
    connector_domain::{PlanNodeAssignmentState, Split, TaskUpdateRequest},
    split_assignment::{
        AssignmentTarget, SplitAssignmentStop, TaskUpdateOutcome, TaskUpdateTransport,
        TaskUpdateTransportErrorKind,
    },
};
use crate::task_execution::host::{
    pumps::split::SplitDeliveryRecords, transport::queue_input::tests::Governed,
};
use crate::wake::CountingWake;
use novarocks_attempt_protocol::{
    graph::TaskGraph,
    input::{ExecutorFact, LocalDispatchResult, OperationReply, OperationReplyResult},
    split_delivery::{self, SplitDeliverySettlement},
    start::{FrozenCreation, FrozenTask, FrozenTaskGraph},
};
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ConsumerId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    DomainProgression, PlanNodeId, PlanNodeSplitReceipt, SplitSequence, SplitWatermark,
    TaskDomainReceipt, UpdateTaskReceipt, operation::OperationResult,
};
use novarocks_spi::connector::{self as spi, read_stack as read};
use novarocks_types::{StageId, TaskId, UniqueId};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Column;
impl read::ColumnHandle for Column {}
#[derive(Debug)]
struct Unit;
impl read::ConnectorSplit for Unit {
    fn retained_size_in_bytes(&self) -> u64 {
        1
    }
}
struct Provider {
    descriptor: spi::ConnectorInstanceDescriptor,
    catalog: spi::CatalogHandle,
}
impl read::adapter::ProviderReadRuntime for Provider {
    type Table = ();
    type Column = Column;
    type Transaction = ();
    type Split = Unit;
    fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &spi::CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}
struct Encoder {
    payload: spi::ConnectorEncodedPayload,
}
impl spi::ConnectorReadWireEncoder for Encoder {
    fn owner(&self) -> &str {
        "original-nonempty-split"
    }
    fn encode_relation_payload(
        &self,
        _: &read::ConnectorReadRelation,
    ) -> Result<spi::ConnectorReadRelationPayload, spi::ConnectorCodecError> {
        unreachable!("this delivery component never encodes a relation")
    }
    fn encode_column_payload(
        &self,
        _: &read::ConnectorReadColumnHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        unreachable!("this delivery component never encodes a column")
    }
    fn encode_transaction_payload(
        &self,
        _: &read::ConnectorReadTransactionHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        unreachable!("this delivery component never encodes a transaction")
    }
    fn encode_split_payload(
        &self,
        _: &read::ConnectorReadSplit,
    ) -> Result<spi::ConnectorReadSplitPayload, spi::ConnectorCodecError> {
        Ok(spi::ConnectorReadSplitPayload::new(
            spi::ConnectorReadSplitCategory::SystemRelation,
            self.payload.clone(),
        ))
    }
}
fn original_request(target: &AssignmentTarget, node: PlanNodeId) -> TaskUpdateRequest {
    let instance = spi::ConnectorInstanceId::parse("original.nonempty").unwrap();
    let catalog = spi::CatalogHandle::new(
        instance.clone(),
        spi::CatalogVersion::from_bytes([0x29; 32]),
    );
    let descriptor = spi::ConnectorInstanceDescriptor {
        provider_id: spi::ConnectorProviderId::parse("original-nonempty").unwrap(),
        instance_id: instance,
    };
    let payload = spi::ConnectorEncodedPayload::new(
        spi::ConnectorEnvelopeHeader::new(
            descriptor.provider_id.clone(),
            catalog.clone(),
            spi::ConnectorCodecCategory::ReadSplit,
            spi::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![0x42].into(),
    );
    let adapter = read::adapter::ReadRuntimeAdapter::new(Arc::new(Provider {
        descriptor,
        catalog,
    }));
    let assignment = PlanNodeAssignmentState::new()
        .assign(
            node.get(),
            vec![Split::new(adapter.wrap_split(Unit))],
            true,
            Arc::new(Encoder { payload }),
        )
        .unwrap();
    assert_eq!(assignment.splits().len(), 1);
    assert!(assignment.no_more_splits());
    TaskUpdateRequest::new(target.task_key, vec![assignment])
}

/// A finite frozen component graph author, rather than guessed destinations
/// taken from a live query. Only the two leaf Tasks own this scan node.
fn original_graph(node: PlanNodeId) -> TaskGraph {
    let execution =
        QueryExecutionId::new(QueryId::new(711, 592), AttemptId::new(1).unwrap()).unwrap();
    let backends = [BackendProcessId::new_v7(), BackendProcessId::new_v7()];
    let frontend =
        FrontendProcessId::try_from_bytes(BackendProcessId::new_v7().to_bytes()).unwrap();
    let mut tasks = BTreeMap::new();
    let mut stages = BTreeMap::new();
    let mut root = None;
    for value in 1..=3u32 {
        let id = TaskId::new(value).unwrap();
        let fragment = if value == 1 { 3 } else { 1 };
        let stage = StageId::new(fragment).unwrap();
        let identity = TaskIdentity::new(execution, stage, id, backends[usize::from(value == 3)]);
        if value == 1 {
            root = Some(identity);
        }
        stages.entry(stage).or_insert_with(Vec::new).push(identity);
        tasks.insert(
            id,
            FrozenTask {
                identity,
                participation: ParticipationRef::new(
                    execution,
                    frontend,
                    identity.backend_process_id(),
                ),
                task_key: UniqueId::new(592, i64::from(value)),
                fragment_id: fragment,
                split_plan_nodes: if value == 1 {
                    Box::new([])
                } else {
                    Box::from([node])
                },
                creation: FrozenCreation {
                    resource: PayloadRef(NonZeroU64::new(u64::from(value)).unwrap()),
                    queued_bytes: 256,
                },
            },
        );
    }
    TaskGraph::from_frozen(FrozenTaskGraph {
        root: root.unwrap(),
        tasks,
        stages: stages
            .into_iter()
            .map(|(stage, rows)| (stage, rows.into_boxed_slice()))
            .collect(),
        edges: BTreeMap::new(),
    })
    .unwrap()
}

#[test]
fn nonempty_split_records_keep_both_original_graph_targets_and_the_same_native_request_on_unknown()
{
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let (mut parent, mut payer) = scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    parent.start().unwrap();
    let wc = Governed::new();
    let mut admission = AdmissionSource::new(
        &parent,
        &payer,
        generation,
        ConsumerId::new(1).unwrap(),
        wc.authority(),
        Default::default(),
    )
    .unwrap_or_else(|_| panic!("the original split payer"));
    let mut requests = FrozenSendRequests::default();
    let mut next = NonZeroU64::MIN;
    let node = PlanNodeId::new(5).unwrap();
    let graph = original_graph(node);
    let execution = graph.root_identity().query_execution_id();
    let tasks: BTreeMap<_, _> = graph
        .tasks()
        .map(|task| (task.task_key(), task.identity()))
        .collect();
    let records = SplitDeliveryRecords::new(execution, tasks, Arc::new(CountingWake::default()));
    // All assertion unwinds first drop borrowed request aliases, then this
    // component retires its actual original paid storage before rethrowing.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let targets: Vec<_> = graph
            .tasks()
            .filter(|task| task.split_plan_nodes().contains(&node))
            .collect();
        assert_eq!(targets.len(), 2);
        assert!(graph.tasks().all(|task| {
            !task
                .split_plan_nodes()
                .contains(&PlanNodeId::new(6).unwrap())
        }));
        let pump = PayloadRef(NonZeroU64::new(71).unwrap());
        let foreign = AssignmentTarget {
            backend_idx: 0,
            task_key: UniqueId::new(999, 592),
        };
        let refusal = records
            .begin(execution, &foreign, &original_request(&foreign, node))
            .unwrap_err();
        assert_eq!(refusal.kind(), TaskUpdateTransportErrorKind::Fatal);
        assert!(records.next_unregistered().is_none());
        for (index, task) in targets.iter().enumerate() {
            assert_eq!(task.fragment_id(), 1);
            let target = AssignmentTarget {
                backend_idx: index,
                task_key: task.task_key(),
            };
            let request = original_request(&target, node);
            let ticket = records.begin(execution, &target, &request).unwrap();
            let row = records.next_unregistered().unwrap();
            assert_eq!(row.task, task.identity());
            assert_eq!(row.assignment.node(), node);
            let canonical = row.assignment.payload().clone();
            let canonical_exit = Arc::downgrade(&canonical);
            assert!(canonical.encoded_len() > 0);
            let mut store = NativeSplitPayloadStore {
                requests: &mut requests,
                admission: &mut admission,
                next: &mut next,
            };
            let payload = store.install(&row).unwrap().unwrap();
            records.register_payload(row.delivery, payload).unwrap();
            let Some(ExecutorFact::PumpTaskUpdate {
                task: actual,
                delivery,
                domains,
                ..
            }) = records.next_input(pump).unwrap()
            else {
                panic!("the original nonempty split Source input")
            };
            assert_eq!(actual, task.identity());
            assert_eq!(delivery, row.delivery);
            assert_eq!(domains.len(), 1);
            let TaskDomainIntent::SplitAssignment {
                node: actual_node,
                offer,
                payload: same_payload,
                ..
            } = &domains[0]
            else {
                panic!("the original split domain")
            };
            assert_eq!((*actual_node, *same_payload), (node, payload));
            assert_eq!(
                offer.range(),
                Some((
                    SplitSequence::new(1).unwrap(),
                    SplitSequence::new(1).unwrap()
                ))
            );
            assert!(offer.no_more_splits());
            records.input_published(delivery).unwrap();
            let operation = TaskOperationId::new_v7();
            let envelope =
                OperationEnvelope::with_default_wait(operation, OperationKind::UpdateTask);
            let passive = ProtocolIntent::UpdateTask {
                envelope,
                identity: actual,
                domains: domains.into_vec().into(),
            };
            let quote = store.requests.encoded_protobuf_bytes(&passive).unwrap();
            assert!(
                store.requests.split_payloads[&payload]
                    .request
                    .get()
                    .is_none()
            );
            let contrary_task = ProtocolIntent::UpdateTask {
                envelope,
                identity: graph.root_identity(),
                domains: Arc::from([TaskDomainIntent::SplitAssignment {
                    node,
                    offer: row.assignment.offer(),
                    payload,
                    queued_bytes: canonical.encoded_len(),
                }]),
            };
            assert!(matches!(
                store.requests.resolve(&contrary_task),
                Err(SendBindingError::WrongRequest)
            ));
            let contrary_node = ProtocolIntent::UpdateTask {
                envelope,
                identity: actual,
                domains: Arc::from([TaskDomainIntent::SplitAssignment {
                    node: PlanNodeId::new(6).unwrap(),
                    offer: row.assignment.offer(),
                    payload,
                    queued_bytes: canonical.encoded_len(),
                }]),
            };
            assert!(matches!(
                store.requests.resolve(&contrary_node),
                Err(SendBindingError::WrongRequest)
            ));
            assert!(
                store.requests.split_payloads[&payload]
                    .request
                    .get()
                    .is_none()
            );
            let OperationIntent::UpdateTask(first) = store.requests.resolve(&passive).unwrap()
            else {
                panic!("original request")
            };
            let OperationIntent::UpdateTask(replay) = store.requests.resolve(&passive).unwrap()
            else {
                panic!("original replay")
            };
            assert!(Arc::ptr_eq(&first, &replay));
            let request_exit = Arc::downgrade(&first);
            let [TaskDomainUpdate::SplitAssignment(assignment)] = first.domains() else {
                panic!("one split assignment")
            };
            assert!(Arc::ptr_eq(assignment.payload(), &canonical));
            assert_eq!(
                quote,
                OperationIntent::UpdateTask(first.clone())
                    .encoded_protobuf_bytes()
                    .unwrap()
            );
            records.operation_released(delivery, operation).unwrap();
            let reply = |result| OperationReply {
                operation_id: operation,
                kind: OperationKind::UpdateTask,
                result,
            };
            let unknown = split_delivery::settle(&reply(OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                failure: None,
                protocol_failure: None,
                detail: None,
            }));
            assert!(matches!(&unknown, SplitDeliverySettlement::Unknown(_)));
            records
                .operation_settled(delivery, operation, unknown)
                .unwrap();
            let failure = records
                .poll(ticket, &SplitAssignmentStop::default())
                .unwrap()
                .unwrap_err();
            assert_eq!(
                failure.kind(),
                TaskUpdateTransportErrorKind::RetryableNetwork
            );
            assert_eq!(records.begin(execution, &target, &request).unwrap(), ticket);
            assert!(records.next_input(pump).unwrap().is_none());
            let OperationIntent::UpdateTask(same) = store.requests.resolve(&passive).unwrap()
            else {
                panic!("same original operation")
            };
            assert!(Arc::ptr_eq(&first, &same));
            assert_eq!(same.envelope().operation_id(), operation);
            let accepted = split_delivery::settle(&reply(OperationReplyResult::Worker(
                OperationResult::Verdict {
                    outcome: novarocks_execution_contract::OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(novarocks_attempt_protocol::input::AckPayload::Update(
                        UpdateTaskReceipt::new(
                            actual,
                            vec![TaskDomainReceipt::SplitAssignment {
                                nodes: vec![
                                    PlanNodeSplitReceipt::new(
                                        node,
                                        SplitWatermark::empty()
                                            .apply_batch(SplitSequence::new(1).unwrap(), true),
                                    )
                                    .with_queued_splits(2),
                                ],
                                progression: DomainProgression::Apply,
                            }],
                        ),
                    )),
                },
            )));
            records
                .operation_settled(delivery, operation, accepted)
                .unwrap();
            let TaskUpdateOutcome::Accepted(nodes) = records
                .poll(ticket, &SplitAssignmentStop::default())
                .unwrap()
                .unwrap()
            else {
                panic!("the same original accepted split receipt")
            };
            assert_eq!(nodes.len(), 1);
            assert_eq!(
                (
                    nodes[0].plan_node_id,
                    nodes[0].accepted_through_sequence,
                    nodes[0].no_more_splits,
                    nodes[0].queued_splits
                ),
                (node.get(), 1, true, 2)
            );
            let retired = records.take_retired_one().unwrap();
            assert_eq!(retired.payload, Some(payload));
            assert!(
                store.detach(payload).unwrap().is_none(),
                "the actual original request aliases remain live"
            );
            drop((retired, first, replay, same, canonical, row));
            assert!(canonical_exit.upgrade().is_some());
            assert!(request_exit.upgrade().is_some());
            drop(store.detach(payload).unwrap().unwrap());
            assert!(
                canonical_exit.upgrade().is_none(),
                "the SAME nonempty canonical payload actually exited"
            );
            assert!(
                request_exit.upgrade().is_none(),
                "the SAME resolved Native request actually exited"
            );
        }
        assert!(
            records
                .next_input(PayloadRef(NonZeroU64::new(71).unwrap()))
                .unwrap()
                .is_none()
        );
        assert!(records.take_retired().is_empty());
    }));
    drop(records);
    // No actor, runner or RPC was issued by this storage component. Any
    // remaining canonical row is disposed after all closure aliases exited.
    while let Some(payload) = requests.split_payloads.keys().next().copied() {
        let mut store = NativeSplitPayloadStore {
            requests: &mut requests,
            admission: &mut admission,
            next: &mut next,
        };
        drop(
            store
                .detach(payload)
                .unwrap()
                .expect("the component's original aliases exited"),
        );
    }
    assert!(requests.is_empty() && admission.is_drained());
    drop(admission);
    assert!(wc.finish());
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    assert!(
        scope
            .reap_observed_header(&parent.retirement_observer())
            .unwrap()
    );
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    if let Err(original) = result {
        std::panic::resume_unwind(original);
    }
}
