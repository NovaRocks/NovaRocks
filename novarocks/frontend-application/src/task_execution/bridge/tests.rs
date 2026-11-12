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

use std::{
    collections::BTreeSet,
    future::{Future, pending},
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use novarocks_attempt_protocol::{
    effect::Publication,
    failure::AttemptFailure,
    input::{AttemptInput, ExecutorFact, PublicationApplied},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRootFailure, AttemptRootProjection, AttemptTerminal, AttemptToLogical,
    AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    TaskIdentity, TaskStatus, root_lifetime::RootReadSealed, root_result::RootResultRead,
};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::{
    api::{
        BoundedRootReadPort, PlanSeal, QueryExecutionError, ResultRowCarrier, ResultSchema,
        RetainedRootReply,
    },
    coordination::{
        LogicalConclusion, LogicalExecutionActorConfig, RootRelayBinding, RootRelayFrontier,
        RootResultFetchFailure,
    },
    test_support::LogicalExecutionTestHarness,
};
use novarocks_result_contract::{
    ClientRowProfile, PerKind, RootOutputKind, RootProfileId, RootProfileV1,
};
use novarocks_workload_control::{
    CancellationReason, ResourceConfig, ResultCapacityConfig, ResultWindowAlias, ResultWindowClass,
    Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};

use super::{
    root_control::PendingRootControls,
    terminal::{TerminalOutbox, TerminalPublication},
};
use crate::task_execution::{
    host::publication::{AttemptPublicationOwner, PublicationRecord},
    protocol_facts::AttemptProtocolFactsPublisher,
};

#[derive(Default)]
struct RootReads(Mutex<Vec<RootResultRead>>);

impl BoundedRootReadPort for RootReads {
    fn read(
        &self,
        request: RootResultRead,
        window: ResultWindowAlias,
    ) -> Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>
    {
        self.0.lock().unwrap().push(request);
        Box::pin(async move {
            let _loan = window;
            pending().await
        })
    }

    fn seal(&self, _: RootReadSealed) -> Result<(), QueryExecutionError> {
        panic!("the transport probe never returned a result")
    }
}

#[tokio::test]
async fn original_qa_first_root_read_follows_accepted_exact_bridge_projection() {
    for terminal_first in [false, true] {
        let graph_fixture = crate::task_execution::tests::manifest_unstarted_regression_fixture();
        let graph = graph_fixture.execution().graph();
        let root = graph.root_identity();
        let execution = root.query_execution_id();
        let plan = PlanSeal::Version(PlanVersionId::try_new([192; 16]).unwrap());
        let (protocol, _facts) = AttemptProtocolFactsPublisher::for_graph(
            plan,
            graph,
            &BTreeSet::new(),
            &PerKind {
                write_counts: None,
                delete_verification: None,
            },
        )
        .unwrap();
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let work = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let requester = work.owner.cancellation_requester();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let kind = RootOutputKind::ClientRows;
        let config = LogicalExecutionActorConfig::single_attempt_read_rows(
            execution,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            work.owner,
            stage,
            ResultSchema::new(Vec::new()),
            NonZeroUsize::MIN,
            ResultRowCarrier::relayed(kind, Some(profile)).unwrap(),
        )
        .unwrap();
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution,
            Vec::new(),
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();

        let (sender, statuses, _pending_controls) = PendingRootControls::project(root);
        let publisher = AttemptPublicationOwner::new(
            PublicationRecord::new(drive.identity(), sender, protocol, None, None),
            None,
            None,
        );
        let outbox = TerminalOutbox::new(drive.identity());
        let read = Arc::new(RootReads::default());
        let mut relay = logical.run_original_root_relay(
            scope.clone(),
            RootRelayBinding {
                port: read.clone(),
                frontier: RootRelayFrontier::new(root, RootProfileId::V1, kind, Some(profile))
                    .unwrap(),
                window: window.retain_alias(),
                max_wait: Duration::from_millis(10),
            },
            statuses,
            {
                let outbox = Arc::clone(&outbox);
                async move { outbox.terminal().await.expect("the original terminal") }
            },
        );
        assert!(futures::poll!(relay.as_mut()).is_pending());
        logical.actor_snapshot().await.unwrap();
        assert!(futures::poll!(relay.as_mut()).is_pending());
        assert!(read.0.lock().unwrap().is_empty());

        // A lookalike Installed task cannot supply the original root projection.
        let foreign = TaskIdentity::new(
            execution,
            root.stage_id(),
            root.task_id(),
            novarocks_types::BackendProcessId::new_v7(),
        );
        let installed = |identity| {
            Publication::RootStatus(Box::new(AttemptRootProjection {
                status: TaskStatus::created(identity).with_installed(),
                failure: AttemptRootFailure::None,
            }))
        };
        let action = |ordinal| AttemptActionId::new(NonZeroU64::new(ordinal).unwrap());
        let (result, failure) = publisher.publish(action(1), &installed(foreign)).await;
        assert!(matches!(
            result,
            AttemptInput::Executor(ExecutorFact::PublicationApplied { result: Err(_), .. })
        ));
        assert!(failure.is_some());
        assert!(futures::poll!(relay.as_mut()).is_pending());
        assert!(read.0.lock().unwrap().is_empty());

        let (result, failure) = publisher.publish(action(2), &installed(root)).await;
        assert!(matches!(
            result,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                result: Ok(PublicationApplied::RootStatus),
                ..
            })
        ));
        assert!(failure.is_none());
        if terminal_first {
            // Both observations are ready on the next poll of this same drive.
            // The original Native terminal takes priority over starting a read.
            assert!(
                outbox
                    .publish(Box::new(TerminalPublication::new(AttemptToLogical {
                        activation: drive.identity(),
                        action: action(3),
                        body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(
                            AttemptFailure::OwnerFailed
                        )),
                    })))
                    .is_ok()
            );
        } else {
            assert!(futures::poll!(relay.as_mut()).is_pending());
            logical.actor_snapshot().await.unwrap();
            assert!(futures::poll!(relay.as_mut()).is_pending());
            {
                let reads = read.0.lock().unwrap();
                let [original] = reads.as_slice() else {
                    panic!("one original first fetch: {reads:?}")
                };
                assert_eq!(original.root_task(), root);
                assert_eq!(original.wanted(), Some(NonZeroU64::MIN));
                assert_eq!(original.consumed(), 0);
            }
            requester.request(CancellationReason::Requested).unwrap();
        }

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), relay)
                .await
                .unwrap(),
            if terminal_first {
                LogicalConclusion::Failed
            } else {
                LogicalConclusion::Cancelled
            },
        );
        assert_eq!(read.0.lock().unwrap().len(), usize::from(!terminal_first));
        drop(window);
        work.business.release();
        logical
            .finish_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
            .await
            .unwrap();
        workload.close_admission();
        assert!(workload.shutdown().is_ok());
    }
}
