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

// Included in task_execution::tests; exercises the real Task owner, bridge and driver.
mod split_delivery_local_terminal_tests {
    use super::*;
    use crate::query_execution::split_assignment::{
        AssignmentTarget, ScanNodeKey, SplitAssignmentDriver, SplitAssignmentStop,
        TaskUpdateTransport,
    };
    use crate::task_execution::split_transport::SplitDeliveryBridge;
    use novarocks_spi::connector::ConnectorReadWireEncoder;

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

    fn late_terminal_delivery(
        harness: &Harness,
        task: TaskId,
    ) -> (Arc<SplitDeliveryBridge>, SplitAssignmentDriver, ScanNodeKey) {
        let graph = harness.execution.graph();
        let original = graph.task(task).unwrap();
        let scan = ScanNodeKey::new(LEAF_FRAGMENT, SCAN_NODE);
        let target = AssignmentTarget {
            backend_idx: 0,
            fragment_instance_id: original.fragment_instance_id(),
        };
        let bridge = SplitDeliveryBridge::for_graph(graph);
        let mut driver = SplitAssignmentDriver::new(
            graph.execution_id(),
            Arc::clone(&bridge) as Arc<dyn TaskUpdateTransport>,
            BTreeMap::from([(scan, vec![target])]),
            1,
            BTreeMap::from([(
                scan,
                Arc::new(TerminalOnlyEncoder) as Arc<dyn ConnectorReadWireEncoder>,
            )]),
            crate::task_protocol_config::TaskUpdateRetryPolicy::default(),
            SplitAssignmentStop::default(),
        );
        driver.start_delivery(scan, BTreeMap::new(), true).unwrap();
        assert!(driver.drive_delivery().unwrap());
        (bridge, driver, scan)
    }

    fn admit_original_pending(
        harness: &mut Harness,
        bridge: &SplitDeliveryBridge,
        task: TaskId,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        let mut pending = bridge.take_pending();
        assert_eq!(pending.len(), 1);
        let pending = pending.pop().unwrap();
        assert_eq!(pending.task(), task);
        let delivery = pending.delivery();
        let result = harness
            .execution
            .enqueue_task_update(task, pending.into_update());
        bridge.admit(delivery, result.as_ref().copied()).unwrap();
        result
    }

    #[test]
    fn original_normal_demand_withdrawal_settles_late_split_without_a_failure() {
        for terminal in [None, Some(TaskState::Finished), Some(TaskState::Canceled)] {
            let mut harness = Harness::new(&[0], &[0], 512);
            harness.settle_until_quiet(Duration::from_secs(10));
            let leaf = harness.stage_tasks(1)[0];
            let identity = harness.identity(leaf);
            harness.publish(leaf, TaskState::Running, None, false);
            match terminal {
                None => harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap(),
                Some(TaskState::Finished) => {
                    harness.publish(leaf, TaskState::Flushing, None, false);
                    harness.publish(leaf, TaskState::Finished, None, true);
                }
                Some(TaskState::Canceled) => {
                    let cause = TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded);
                    harness.publish(leaf, TaskState::Canceling, Some(cause.clone()), false);
                    harness.publish(leaf, TaskState::Canceled, Some(cause), false);
                }
                _ => unreachable!(),
            }
            let original_status = harness.execution.task(leaf).unwrap().status().cloned();
            let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
            assert_eq!(
                admit_original_pending(&mut harness, &bridge, leaf).unwrap(),
                UpdateAdmission::DiscardedNoLongerNeeded
            );
            assert!(driver.drive_delivery().unwrap());
            assert!(!driver.delivery_in_progress());
            assert!(driver.is_terminal_for(scan));
            assert!(bridge.take_pending().is_empty());
            assert_eq!(
                harness.execution.task(leaf).unwrap().status(),
                original_status.as_ref()
            );
            assert!(harness.execution.failure_cause().is_none());
        }
    }

    #[test]
    fn original_failure_is_not_normalized_by_earlier_or_later_stand_down() {
        let original = TerminationDetail::Failed(novarocks_execution_contract::TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Resource,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(17).unwrap(),
                ),
            ),
            novarocks_execution_contract::SafeDetail::truncating("original resource failure"),
        ));
        for stand_down_first in [true, false] {
            let mut harness = Harness::new(&[0], &[0], 512);
            harness.settle_until_quiet(Duration::from_secs(10));
            let leaf = harness.stage_tasks(1)[0];
            let identity = harness.identity(leaf);
            harness.publish(leaf, TaskState::Running, None, false);
            if stand_down_first {
                harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap();
            }
            harness.publish(leaf, TaskState::Failing, Some(original.clone()), false);
            harness.publish(leaf, TaskState::Failed, Some(original.clone()), false);
            if !stand_down_first {
                harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap();
            }
            let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
            // The accepted failure already closed admission on the original
            // QueryTaskExecution owner. It must never reach the normal-demand
            // discard branch or be mistaken for a successful destination.
            assert!(matches!(
                admit_original_pending(&mut harness, &bridge, leaf),
                Err(TaskExecutionError::Schedule(_))
            ));
            assert!(matches!(
                driver.drive_delivery(),
                Err(SplitAssignmentDriverError::Transport { .. })
            ));
            assert!(!driver.is_terminal_for(scan));
            assert_eq!(harness.execution.failure_cause(), Some(&original));
            assert_eq!(
                harness
                    .execution
                    .task(leaf)
                    .unwrap()
                    .status()
                    .unwrap()
                    .termination(),
                Some(&original)
            );
        }
    }

    #[test]
    fn missing_terminal_status_does_not_supply_normal_completion() {
        let mut harness = Harness::new(&[0], &[0], 512);
        harness.settle_until_quiet(Duration::from_secs(10));
        let leaf = harness.stage_tasks(1)[0];
        harness
            .execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
            .unwrap();
        let original = harness
            .released()
            .into_iter()
            .find(|intent| intent.kind() == OperationKind::UpdateTask)
            .unwrap();
        harness
            .update_ack(&original, OperationOutcome::TerminalRejected)
            .unwrap();
        assert!(!harness.execution.task(leaf).unwrap().is_terminal());
        let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
        assert_eq!(
            admit_original_pending(&mut harness, &bridge, leaf).unwrap(),
            UpdateAdmission::DiscardedTerminal
        );
        assert!(matches!(
            driver.drive_delivery(),
            Err(SplitAssignmentDriverError::Transport { .. })
        ));
        assert!(!driver.is_terminal_for(scan));
        assert!(!harness.execution.task(leaf).unwrap().is_terminal());
        assert!(harness.execution.failure_cause().is_none());
    }
    #[test]
    fn strict_tail_keeps_original_split_delivery_until_normal_close_or_real_cleanup() {
        use crate::query_execution::connector_domain::{
            PlanNodeAssignmentState, TaskUpdateRequest,
        };
        use crate::task_execution::attempt_convergence::{
            NativeAttemptConvergenceMode, NativeAttemptConvergenceOwner,
            NativeAttemptConvergenceTarget,
        };
        use novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
        use novarocks_query_application::api::{
            BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
        };
        struct CurrentProcess;
        impl BackendProcessObservationPort for CurrentProcess {
            fn observe_process_at_endpoint(
                &self,
                _: BackendProcessId,
                _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
            ) -> Result<BackendProcessObservation, BackendTopologyError> {
                Ok(BackendProcessObservation::Current)
            }
        }
        for requested_cleanup in [false, true] {
            let schedule = chain_schedule(&[0], &[1]);
            let mut edges = chain_edges();
            for edge in &mut edges {
                edge.delivery = MustDrain;
            }
            let graph = build_graph(&schedule, &edges, &backends(2), 512).unwrap();
            let bridge = SplitDeliveryBridge::for_graph(&graph);
            let sink = Arc::new(RecordingSink::default());
            let harness = Harness::from_graph_with_original_transport(
                graph,
                4096,
                sink.clone(),
                bridge.sink(sink.clone()),
            );
            let leaf = harness.stage_tasks(1)[0];
            let leaf_identity = harness.identity(leaf);
            let leaf_kernel = harness
                .execution
                .graph()
                .task(leaf)
                .unwrap()
                .fragment_instance_id();
            let (round, status, acks, creates) = round_waiting_on_creates(harness);
            let mut round = round.observing(bridge.clone());
            for create in &creates {
                let identity = create_identity(create);
                acks.publish(OperationAcknowledgement::new(
                    create.operation_id(),
                    OperationKind::CreateTask,
                    OperationOutcome::Accepted,
                    AckPayload::Create(Box::new(CreateTaskReceipt::new(
                        identity,
                        Vec::new(),
                        TaskStatus::created(identity).with_installed(),
                    ))),
                ));
            }
            round.turn().unwrap();
            // Settle the real edge-open domain operations before adding the
            // source's final delivery. No receipt is inferred from a send.
            for _ in 0..8 {
                let released = sink
                    .take()
                    .into_iter()
                    .flat_map(|(_, operations)| operations)
                    .collect::<Vec<_>>();
                if released.is_empty() {
                    break;
                }
                for intent in released {
                    if let OperationIntent::UpdateTask(update) = &intent {
                        acks.publish(OperationAcknowledgement::new(
                            intent.operation_id(),
                            OperationKind::UpdateTask,
                            OperationOutcome::Accepted,
                            AckPayload::Update(UpdateTaskReceipt::new(
                                update.identity(),
                                update.domains().iter().map(task_domain_receipt).collect(),
                            )),
                        ));
                    }
                }
                round.turn().unwrap();
            }
            let root = round.root_task();
            let source = round.take_root_status_source().unwrap();
            let mut seal = source.begin_success_seal_request().unwrap();
            // The original ordered intake applies this control before later statuses.
            round.turn().unwrap();
            assert!(matches!(
                seal.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            for (version, state, complete) in [
                (2, TaskState::Running, false),
                (3, TaskState::Flushing, false),
                (4, TaskState::Finished, true),
            ] {
                status.publish(StatusEvent::Published(Box::new(
                    TaskStatus::try_new(
                        root,
                        TaskStatusVersion::new(version).unwrap(),
                        state,
                        None,
                        TaskOutputFacts::new(complete),
                    )
                    .unwrap()
                    .with_installed(),
                )));
                round.turn().unwrap();
            }
            assert_eq!(seal.try_recv().unwrap(), Ok(()));
            assert!(!round.execution().normal_drain_started());
            let target = AssignmentTarget {
                backend_idx: 0,
                fragment_instance_id: leaf_kernel,
            };
            let scan = ScanNodeKey::new(LEAF_FRAGMENT, SCAN_NODE);
            let mut driver = SplitAssignmentDriver::new(
                leaf_identity.query_execution_id(),
                bridge.clone(),
                BTreeMap::from([(scan, vec![target.clone()])]),
                1,
                BTreeMap::from([(
                    scan,
                    Arc::new(TerminalOnlyEncoder) as Arc<dyn ConnectorReadWireEncoder>,
                )]),
                crate::task_protocol_config::TaskUpdateRetryPolicy::default(),
                SplitAssignmentStop::default(),
            );
            driver.start_delivery(scan, BTreeMap::new(), true).unwrap();
            assert!(driver.drive_delivery().unwrap());
            let targets = round
                .execution()
                .graph()
                .contexts()
                .map(|&context| {
                    NativeAttemptConvergenceTarget::new(
                        context,
                        novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                            "127.0.0.1",
                            1,
                        )
                        .unwrap(),
                    )
                })
                .collect();
            let mut convergence =
                NativeAttemptConvergenceOwner::new(&round, Arc::new(CurrentProcess), targets)
                    .unwrap();
            convergence
                .step(
                    &mut round,
                    &bridge,
                    NativeAttemptConvergenceMode::Normal,
                    false,
                )
                .unwrap();
            assert!(!round.execution().normal_drain_started());
            assert!(
                bridge.take_pending().is_empty(),
                "the original owner admitted the queued delivery"
            );
            let released = sink
                .take()
                .into_iter()
                .flat_map(|(_, operations)| operations)
                .collect::<Vec<_>>();
            let split = released.iter().find(|intent| matches!(intent, OperationIntent::UpdateTask(update)
                if update.identity() == leaf_identity && update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::SplitAssignment(_))))).expect("strict tail still releases its original split update");
            assert_no_strict_demand_controls(&released);
            let OperationIntent::UpdateTask(update) = split else {
                unreachable!()
            };
            acks.publish(OperationAcknowledgement::new(
                split.operation_id(),
                OperationKind::UpdateTask,
                OperationOutcome::Accepted,
                AckPayload::Update(UpdateTaskReceipt::new(
                    update.identity(),
                    update
                        .domains()
                        .iter()
                        .map(|domain| {
                            let mut receipt = task_domain_receipt(domain);
                            if let TaskDomainReceipt::SplitAssignment { nodes, .. } = &mut receipt {
                                // This original offer is terminal-only, with no queued split.
                                for node in nodes {
                                    *node = node.with_queued_splits(0);
                                }
                            }
                            receipt
                        })
                        .collect(),
                )),
            ));
            convergence
                .step(
                    &mut round,
                    &bridge,
                    NativeAttemptConvergenceMode::Normal,
                    false,
                )
                .unwrap();
            assert!(driver.drive_delivery().unwrap());
            assert!(driver.is_terminal_for(scan));
            if requested_cleanup {
                convergence
                    .step(
                        &mut round,
                        &bridge,
                        NativeAttemptConvergenceMode::Normal,
                        true,
                    )
                    .unwrap();
                assert!(
                    !round.execution().normal_drain_started(),
                    "cleanup cannot manufacture strict success"
                );
            } else {
                for create in creates
                    .iter()
                    .filter(|create| create_identity(create) != root)
                {
                    for (version, state, complete) in [
                        (2, TaskState::Running, false),
                        (3, TaskState::Flushing, false),
                        (4, TaskState::Finished, true),
                    ] {
                        status.publish(StatusEvent::Published(Box::new(
                            TaskStatus::try_new(
                                create_identity(create),
                                TaskStatusVersion::new(version).unwrap(),
                                state,
                                None,
                                TaskOutputFacts::new(complete),
                            )
                            .unwrap()
                            .with_installed(),
                        )));
                        convergence
                            .step(
                                &mut round,
                                &bridge,
                                NativeAttemptConvergenceMode::Normal,
                                false,
                            )
                            .unwrap();
                    }
                }
                assert!(round.execution().normal_drain_started());
            }
            let assignment = PlanNodeAssignmentState::new()
                .assign(SCAN_NODE, Vec::new(), true, Arc::new(TerminalOnlyEncoder))
                .unwrap();
            let request = TaskUpdateRequest::new(leaf_kernel, vec![assignment]);
            let error = bridge
                .begin(leaf_identity.query_execution_id(), &target, &request)
                .expect_err("only the original close cut abandons the bridge");
            assert!(error.detail().contains("abandoned"));
            assert!(
                round.accepted_root_success_sealed(),
                "cleanup keeps the original accepted root cut"
            );
        }
    }
}
