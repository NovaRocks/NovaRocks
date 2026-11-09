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

//! First-wins original receipt custody. Pure P05 projections classify wire
//! evidence; neither this guard nor the host chooses a retry or protocol exit.
use super::{queue_input::SendKey, source_ports::TransportReports};
use crate::{
    native::{
        apply_send_owner::OriginalReplyPublisher,
        task_transport::wire::{self, SentOperation},
    },
    task_execution::{
        intent::{AckPayload, OperationAcknowledgement},
        operation::OperationDispatchResult,
    },
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_proto_models::novarocks as proto;
use std::{collections::VecDeque, sync::Arc};

pub(crate) struct PendingReceipt {
    pub(crate) key: SendKey,
    pub(crate) sent: SentOperation,
}
pub(crate) struct AcceptedReceipts {
    pending: VecDeque<PendingReceipt>,
    publisher: OriginalReplyPublisher,
    reports: Arc<dyn TransportReports>,
}
impl AcceptedReceipts {
    /// All canonical Source cells were funded before the original operations
    /// became visible. The publisher is this same accepted WC action's author.
    pub(crate) fn new(
        sent: Vec<PendingReceipt>,
        publisher: OriginalReplyPublisher,
        reports: Arc<dyn TransportReports>,
    ) -> Self {
        assert!(
            !sent.is_empty(),
            "one original Native batch owns its receipt cells"
        );
        Self {
            pending: sent.into(),
            publisher,
            reports,
        }
    }
    pub(crate) fn establishes(&self) -> Vec<(TaskOperationId, ParticipationRef)> {
        self.pending
            .iter()
            .filter_map(|item| {
                item.sent
                    .establish_participation
                    .map(|participation| (item.key.operation, participation))
            })
            .collect()
    }
    pub(crate) async fn freeze_dispatch(&mut self, expires_at: tokio::time::Instant) -> bool {
        if self.publisher.freeze_dispatch(expires_at).await.is_err() {
            assert!(
                self.publisher.dispatch_rejected(),
                "the exact no-RPC fact precedes settlement"
            );
            self.uniform(OperationDispatchResult::NonWorkerRejected);
            false
        } else {
            true
        }
    }
    fn next(&mut self, ack: OperationAcknowledgement) {
        let item = self
            .pending
            .pop_front()
            .expect("a checked response cannot outnumber its original request");
        assert_eq!(
            item.key.operation,
            ack.operation_id(),
            "canonical request order is unchanged"
        );
        let original = self.publisher.canonicalize(ack);
        self.reports.record_ack(item.key, original);
    }
    pub(crate) fn uniform(&mut self, dispatch: OperationDispatchResult) {
        while let Some(item) = self.pending.front() {
            self.next(OperationAcknowledgement::from_dispatch_result(
                item.key.operation,
                item.sent.kind,
                dispatch,
                AckPayload::None,
            ));
        }
    }
    pub(crate) fn response(&mut self, response: proto::ApplyTaskOperationsResponse) {
        let ids = self
            .pending
            .iter()
            .map(|item| item.key.operation)
            .collect::<Vec<_>>();
        let headers = match wire::decode_operation_receipts(&response, &ids) {
            Ok(headers) => headers,
            Err(error) => {
                tracing::warn!(detail=%error,"task operation batch response is unusable");
                self.uniform(OperationDispatchResult::NonWorkerRejected);
                return;
            }
        };
        for (header, receipt) in headers.iter().zip(response.receipts) {
            let item = self
                .pending
                .front()
                .expect("validated original receipt count");
            let projection = wire::project_acknowledgement_owned(&item.sent, header, receipt);
            if let Some(detail) = projection.unreadable_ack.as_ref() {
                tracing::warn!(detail=%detail,"task operation acknowledgement is unreadable");
            }
            self.next(projection.acknowledgement);
        }
    }
}
impl Drop for AcceptedReceipts {
    fn drop(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        // A remote NO_ERROR GOAWAY is already classified by P05. A dropped
        // accepted future is Unknown unless this same original dispatch cut
        // durably recorded that no RPC was dispatched.
        let result = if self.publisher.dispatch_rejected() {
            OperationDispatchResult::NonWorkerRejected
        } else {
            OperationDispatchResult::TransportUnknown
        };
        self.uniform(result);
    }
}
