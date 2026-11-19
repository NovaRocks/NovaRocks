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

//! Development-only result-stream and frozen-schedule fixtures. They expose
//! observed transport behavior without granting production construction rights.

use novarocks_execution_contract::ResultPacketSequence;
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{
    CancellationReason, LocalResourceAuthority, ResourceConfig, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use tokio::sync::watch;

use crate::api::result::{
    EndDelivery, QueryResultTransport, ResultDelivery, ResultDeliveryDisposition,
    ResultDeliveryReceipt,
};
use crate::api::{
    ExecutionOutput, ExecutionOutputObligation, QueryExecutionError, QueryResultStream,
    ResultField, ResultSchema,
};

/// Test-only observation of one move-only protocol delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestResultDeliveryDisposition {
    Completed,
    Failed(QueryExecutionError),
    Dropped,
}

/// Test-only receipt for the actor side of one protocol delivery.
pub struct TestResultDeliveryReceipt(ResultDeliveryReceipt);

impl TestResultDeliveryReceipt {
    pub async fn wait(self) -> TestResultDeliveryDisposition {
        match self
            .0
            .await
            .expect("test result delivery owner remains alive")
        {
            ResultDeliveryDisposition::Completed => TestResultDeliveryDisposition::Completed,
            ResultDeliveryDisposition::Failed(error) => {
                TestResultDeliveryDisposition::Failed(*error)
            }
            ResultDeliveryDisposition::Dropped => TestResultDeliveryDisposition::Dropped,
        }
    }
}

/// Feature-gated producer for exercising a real [`QueryResultStream`] from a
/// role adapter without exposing production construction authority.
pub struct ResultStreamTestProducer {
    execution_id: QueryExecutionId,
    transport: QueryResultTransport,
    failure: watch::Sender<Option<QueryExecutionError>>,
    _workload: WorkloadControl,
    root: Option<RootWork>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
}

impl ResultStreamTestProducer {
    pub fn open(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
    ) -> Result<
        (
            Self,
            ExecutionOutputObligation,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        Self::open_with_carrier(
            execution_id,
            fields,
            delivery_capacity,
            resource_config,
            crate::api::ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::ClientRows,
                Some(
                    novarocks_result_contract::ClientRowProfile::try_new(
                        novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                        novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .expect("frozen client profile"),
                ),
            )?,
        )
    }

    pub fn open_with_carrier(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
        carrier: crate::api::ResultRowCarrier,
    ) -> Result<
        (
            Self,
            ExecutionOutputObligation,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        let workload = WorkloadControl::try_new(WorkloadConfig::default(), resource_config)
            .expect("test result workload config must be valid");
        let capacity = Some({
            workload
                .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
                .expect("test result capacity")
        });
        workload
            .mark_ready()
            .expect("test result workload becomes ready");
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("test result root work is admitted");
        let window = capacity.as_ref().map(|capacity| {
            let class = match carrier {
                crate::api::ResultRowCarrier::Relayed {
                    kind: novarocks_result_contract::RootOutputKind::ClientRows,
                    ..
                } => novarocks_workload_control::ResultWindowClass::Client,
                _ => novarocks_workload_control::ResultWindowClass::Internal,
            };
            capacity
                .try_acquire(&root.owner.scope(), class)
                .expect("test root window")
        });
        let schema = ResultSchema::new(fields);
        let (transport, schema_receipt, failure, stream) = QueryResultStream::try_channel(
            execution_id.query_id(),
            schema,
            carrier,
            delivery_capacity,
        )?;
        let obligation = ExecutionOutputObligation::new(
            root.owner.cancellation_requester(),
            ExecutionOutput::Rows(stream),
        );
        let resources = workload.resources().unwrap();
        Ok((
            Self {
                execution_id,
                transport,
                failure,
                _workload: workload,
                root: Some(root),
                window,
                capacity,
            },
            obligation,
            resources,
            TestResultDeliveryReceipt(schema_receipt),
        ))
    }

    pub fn result_capacity(&self) -> novarocks_workload_control::ResultCapacityHandle {
        self.capacity
            .as_ref()
            .expect("test relayed capacity")
            .clone()
    }

    /// Encoded ClientRows fixture transferred through the real V1 validator.
    pub async fn enqueue_client_body(
        &self,
        sequence: u64,
        body: Vec<u8>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use novarocks_execution_contract::TaskIdentity;
        use novarocks_execution_contract::root_result::{
            RootReadOutcome, RootResultData, RootResultReply,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        use novarocks_types::{BackendProcessId, StageId, TaskId};
        let data = RootResultData::try_new(
            RootOutputKind::ClientRows,
            std::num::NonZeroU64::new(sequence.checked_add(1).ok_or_else(|| {
                crate::api::QueryExecutionError::new(
                    crate::api::QueryExecutionErrorKind::InvalidRequest,
                    "fixture sequence overflow",
                )
            })?)
            .unwrap(),
            bytes::Bytes::from(body),
            None,
        )
        .map_err(|error| {
            crate::api::QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        self.enqueue_segment(
            sequence,
            RootResultReply {
                root_task: TaskIdentity::new(
                    self.execution_id,
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: sequence,
                outcome: RootReadOutcome::Data(data),
            },
            Some((
                ClientRowProfile::try_new(
                    RootProfileV1::SEGMENT_BYTES,
                    RootProfileV1::ROW_PAYLOAD_BYTES,
                )
                .unwrap(),
                ClientRowStreamCursor::default(),
            )),
            rows,
        )
        .await
    }

    /// Test-only transfer through the real move-only segment/receipt boundary.
    pub async fn enqueue_segment(
        &self,
        sequence: u64,
        reply: novarocks_execution_contract::root_result::RootResultReply,
        client_rows: Option<(
            novarocks_result_contract::ClientRowProfile,
            novarocks_result_contract::ClientRowStreamCursor,
        )>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        let window = self
            .window
            .as_ref()
            .expect("relayed test stream has a window");
        let backing = match &reply.outcome {
            novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                data.body().len() as u64 + 4096
            }
            _ => 4096,
        };
        let retained = crate::api::RetainedRootReply::try_new(
            novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
            window.retain_alias(),
            backing,
        )
        .map_err(|error| {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        let (delivery, receipt) = crate::api::RootSegmentDelivery::try_new(
            self.execution_id,
            ResultPacketSequence::new(sequence),
            retained,
            client_rows,
            rows,
        )?;
        let permit = self.transport.reserve_owned().await?;
        self.transport
            .enqueue(permit, ResultDelivery::Segment(delivery));
        Ok(TestResultDeliveryReceipt(receipt))
    }

    /// Component-only two-item resident window, using the original publish/take/freeze owners.
    /// This performs no Native fetch and proves no Native actor or task lifecycle.
    /// Both items have frontier zero and no piggyback End; no delivery receipt has completed.
    pub async fn enqueue_resident_client_pair(
        &self,
        replies: [novarocks_execution_contract::root_result::RootResultReply; 2],
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use crate::api::{
            ResidentRootSegment, RetainedRootReply, RootRelayResidentWindow, RootReplyView,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        let invalid = || {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                "component resident pair differs from one contiguous ClientRows task",
            )
        };
        let window = self.window.as_ref().expect("relayed test window");
        let [first, next] = replies;
        if first.root_task != next.root_task
            || first.root_task.query_execution_id() != self.execution_id
            || [&first, &next].iter().any(|reply| {
                reply.kind != RootOutputKind::ClientRows
                    || reply.profile != RootProfileId::V1
                    || reply.accepted_consumed != 0
            })
        {
            return Err(invalid());
        }
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let resident = RootRelayResidentWindow::default();
        let mut cursor = ClientRowStreamCursor::new();
        let mut delivery = None;
        for (index, reply) in [first, next].into_iter().enumerate() {
            let backing = match &reply.outcome {
                novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                    if data.end_after_data().is_some() {
                        return Err(invalid());
                    }
                    data.body().len() as u64 + 4096
                }
                _ => return Err(invalid()),
            };
            let retained = std::sync::Arc::new(
                RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    window.retain_alias(),
                    backing,
                )
                .map_err(|_| invalid())?,
            );
            let RootReplyView::Data { sequence, body, .. } = retained.outcome() else {
                return Err(invalid());
            };
            if sequence.get() != index as u64 + 1 {
                return Err(invalid());
            }
            let after = cursor
                .validate_body(profile, body)
                .map_err(|_| invalid())?
                .after();
            let rows = after.completed_rows() - cursor.completed_rows();
            if !resident.publish(ResidentRootSegment {
                sequence,
                reply: retained.clone(),
                client_rows: Some((profile, cursor)),
                rows,
            }) {
                return Err(invalid());
            }
            if index == 0 {
                let _original_delivering = resident.take().ok_or_else(invalid)?;
                delivery = Some(crate::api::RootSegmentDelivery::try_new(
                    self.execution_id,
                    ResultPacketSequence::new(0),
                    retained,
                    Some((profile, cursor)),
                    rows,
                )?);
            }
            cursor = after;
        }
        cursor.validate_end().map_err(|_| invalid())?;
        let (delivery, receipt) = delivery.expect("first resident delivery");
        let permit = self.transport.reserve_owned().await?;
        self.transport.enqueue(
            permit,
            ResultDelivery::Segment(delivery.with_resident_window(resident)),
        );
        Ok(TestResultDeliveryReceipt(receipt))
    }

    pub async fn enqueue_end(&self, sequence: u64) -> TestResultDeliveryReceipt {
        let (delivery, receipt) =
            EndDelivery::success_eof(self.execution_id, ResultPacketSequence::new(sequence));
        let permit = self
            .transport
            .reserve_owned()
            .await
            .expect("test result stream remains connected");
        self.transport
            .enqueue(permit, ResultDelivery::End(delivery));
        TestResultDeliveryReceipt(receipt)
    }

    pub fn fail(&self, error: QueryExecutionError) {
        self.failure.send_replace(Some(error));
    }

    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        self.root
            .as_ref()
            .expect("test result root remains active")
            .owner
            .scope()
            .cancellation()
            .expect("test result scope remains observable")
            .reason()
    }

    pub fn finish(mut self) {
        drop(self.transport);
        drop(self.failure);
        let root = self.root.take().expect("test result root finishes once");
        root.owner.complete();
        root.business.release();
    }
}

/// Build a real QA schedule for a role adapter's exact frozen fixture facts.
/// This test-support module is absent from production composition.
pub fn qualification_schedule(
    execution: novarocks_types::QueryExecutionId,
    frontend: novarocks_types::identity::FrontendProcessId,
    eligible: &[novarocks_types::identity::BackendProcessId],
    facts: &crate::api::ExecutionSchedulingFacts,
) -> crate::coordination::AttemptSchedule {
    crate::coordination::build_attempt_schedule(
        execution,
        frontend,
        eligible,
        facts,
        #[cfg(novarocks_dev_controls)]
        Ok(None),
    )
    .expect("fixture uses the original QA schedule builder")
}

/// Schedule only the explicit placements of a finite role protocol fixture.
/// No actor, Root, request ticket, runtime or execution capability is created.
pub fn protocol_fixture_schedule(
    execution: novarocks_types::QueryExecutionId,
    frontend: novarocks_types::identity::FrontendProcessId,
    facts: &crate::api::ExecutionSchedulingFacts,
    placements: &std::collections::BTreeMap<u32, Vec<novarocks_types::identity::BackendProcessId>>,
) -> Result<crate::coordination::AttemptSchedule, String> {
    crate::coordination::protocol_fixture_schedule(execution, frontend, facts, placements)
        .map_err(|error| error.to_string())
}
