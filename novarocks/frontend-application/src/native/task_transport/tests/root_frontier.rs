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

//! Fixed external-peer protocol coverage; opaque bytes are not a row/domain
//! decoder fixture, AP success, Ready, Worker closure or original Source exit.
use super::*;
use crate::native::task_result_transport::{
    NativeBoundedRootReadPort, NativeTaskResultTransport, TaskReadGrace,
};
use novarocks_execution_contract::root_result::{
    RootReadOutcome, RootReadResponse, RootResultRead,
};
use novarocks_query_application::api::{BoundedRootReadPort, RetainedRootReply};
use novarocks_result_contract::{RootOutputKind, RootProfileId};

struct RootWireFixture {
    original: SinkFixture,
    port: NativeBoundedRootReadPort,
    root: TaskIdentity,
    kind: RootOutputKind,
}
impl RootWireFixture {
    async fn new(encoded: Option<(bytes::Bytes, u64)>) -> Self {
        // Reuse the existing real creation codec input and exact original
        // participation. Only the fixed external peer models Task execution.
        let seed = crate::task_execution::creation::tests::original_creation_seed();
        let root = seed.identity();
        let participation = seed.participation();
        let backend = root.backend_process_id();
        let peer = Loopback::start().await;
        let kind = if let Some((body, rows)) = encoded {
            let kind = RootOutputKind::ClientRows;
            peer.enable_single_root_encoded_result(root, kind, body, rows)
                .unwrap();
            kind
        } else {
            peer.enable_single_root_lifecycle(root, RootOutputKind::CountOnly);
            RootOutputKind::CountOnly
        };
        let original = sink_fixture(peer, backend);
        let admission = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            participation,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            test_attempt_facts().native_compatibility_id,
            novarocks_execution_contract::AdmissionEpochCapability::try_from_bytes([0x58; 16])
                .unwrap(),
        );
        let receipt = Self::send(
            &original,
            OperationIntent::AcquireParticipationAdmissionTicket(admission),
        )
        .await;
        let AckPayload::AdmissionTicket(ticket) = receipt.payload() else {
            panic!("the original paid admission RPC must return its exact ticket")
        };
        assert_eq!(ticket.participation(), participation);
        let establish = EstablishParticipation::new(
            TaskOperationId::new_v7(),
            participation,
            ticket.ticket_id(),
            Arc::new(WireContent::new(
                ESTABLISH_CATALOG_DOMAIN_TAG,
                catalog::CatalogSet::default(),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_FILTER_DOMAIN_TAG,
                proto::RuntimeFilterContribution::default(),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
                proto::QueryOptions::default(),
            )),
            CredentialUpdate::new(
                CredentialDomainId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(
                    WireCredential::decode(&[], FieldPath::root("root_frontier_credential"))
                        .unwrap(),
                ),
            ),
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        );
        Self::send(
            &original,
            OperationIntent::EstablishParticipation(Arc::new(establish)),
        )
        .await;
        let lengths = seed.lengths().unwrap();
        let parts = seed.freeze(lengths).unwrap();
        let create = crate::task_execution::creation::CreateTaskIntent::new(
            novarocks_execution_contract::OperationEnvelope::with_default_wait(
                TaskOperationId::new_v7(),
                OperationKind::CreateTask,
            ),
            root,
            parts,
            None,
        );
        let created = Self::send(&original, OperationIntent::CreateTask(create)).await;
        assert!(matches!(created.payload(), AckPayload::Create(receipt)
            if receipt.identity() == root));
        assert_eq!(
            original.loopback.applied_requests().len(),
            3,
            "admission, Establish and Create are actual paid RPCs, not peer state edits"
        );
        let transport = NativeTaskResultTransport::new(
            Arc::from([original.loopback.bounded_root_descriptor(backend)]),
            original.runtime.clone(),
            TaskReadGrace::new(Duration::from_secs(1)),
        )
        .unwrap();
        Self {
            original,
            port: NativeBoundedRootReadPort::new(Arc::new(transport)),
            root,
            kind,
        }
    }

    async fn send(original: &SinkFixture, intent: OperationIntent) -> OperationAcknowledgement {
        let id = intent.operation_id();
        let kind = intent.kind();
        submit_batch(
            original,
            released_batch(intent.backend_process_id(), vec![intent]),
        );
        let mut receipts = settled(&original.acks, 1).await;
        assert_eq!(receipts.len(), 1);
        let receipt = receipts.pop().unwrap();
        assert_eq!(receipt.operation_id(), id);
        assert_eq!(receipt.kind(), kind);
        assert_eq!(receipt.worker_outcome(), Some(OperationOutcome::Accepted));
        receipt
    }

    async fn read(
        &self,
        wanted: Option<u64>,
        consumed: u64,
    ) -> Result<RetainedRootReply, novarocks_query_application::coordination::RootResultFetchFailure>
    {
        self.port
            .read(
                RootResultRead::try_new(
                    self.root,
                    RootProfileId::V1,
                    self.kind,
                    wanted.map(|n| NonZeroU64::new(n).unwrap()),
                    consumed,
                    Duration::from_millis(10),
                )
                .unwrap(),
                self.original.governed.root_window_alias(),
            )
            .await
    }

    async fn finish(self) {
        let Self { original, port, .. } = self;
        drop(port);
        let SinkFixture {
            mut loopback,
            sink,
            acks,
            mut runtime,
            governed,
        } = original;
        drop((sink, acks));
        governed.drive();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        governed.drive();
        let apply = runtime.drain_original_apply_until(deadline).await;
        governed.drive();
        let outgoing = runtime.drain_native_outgoing_until(deadline).await;
        let peer_exit = loopback.settle().await;
        drop((runtime, loopback));
        let shutdown = governed.finish();
        if apply.is_err() || outgoing.is_err() || !peer_exit || shutdown.is_err() {
            std::panic::panic_any((apply, outgoing, peer_exit, shutdown));
        }
    }
}

fn reply(
    original: &RetainedRootReply,
) -> &novarocks_execution_contract::root_result::RootResultReply {
    let RootReadResponse::Reply(reply) = original.response() else {
        panic!("the actual installed peer must answer a protocol receipt")
    };
    reply
}

#[tokio::test(flavor = "multi_thread")]
async fn encoded_root_rpc_keeps_bytes_end_count_and_ack_frontier_after_replay() {
    let expected = bytes::Bytes::from_static(b"opaque-root-protocol-payload");
    let fixture = RootWireFixture::new(Some((expected.clone(), 37))).await;
    {
        let data = fixture.read(Some(1), 0).await.unwrap();
        let data_reply = reply(&data);
        assert_eq!(data_reply.root_task, fixture.root);
        assert_eq!(data_reply.profile, RootProfileId::V1);
        assert_eq!(data_reply.kind, RootOutputKind::ClientRows);
        assert_eq!(data_reply.accepted_consumed, 0);
        assert!(matches!(&data_reply.outcome, RootReadOutcome::Data(data)
            if data.sequence().get() == 1 && data.body() == &expected && data.end_after_data().is_none()));
        drop(data);
        let end = fixture.read(Some(2), 0).await.unwrap();
        assert!(matches!(reply(&end).outcome, RootReadOutcome::End(end)
            if end.sequence.get() == 2 && end.output_rows == 37));
        drop(end);
        let replay = fixture.read(Some(1), 0).await.unwrap();
        assert!(
            matches!(&reply(&replay).outcome, RootReadOutcome::Data(data)
            if data.sequence().get() == 1 && data.body() == &expected)
        );
        drop(replay);
        // Actual ACK2 after replay is the independent oracle: replaying DATA1
        // must not lower the already-offered End2 frontier to 1.
        let ack = fixture.read(None, 2).await.unwrap();
        assert_eq!(reply(&ack).accepted_consumed, 2);
        assert_eq!(reply(&ack).outcome, RootReadOutcome::AckOnly);
        assert_eq!(
            fixture.original.loopback.single_root_result_progress(),
            (true, 2)
        );
        drop(ack);
        let retired = fixture.read(Some(1), 2).await.unwrap();
        assert_eq!(reply(&retired).outcome, RootReadOutcome::Retired);
        assert_eq!(reply(&retired).accepted_consumed, 2);
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn encoded_root_rpc_rejects_skipped_data_and_ack_beyond_actual_offers() {
    let expected = bytes::Bytes::from_static(b"independent-protocol-body");
    let fixture = RootWireFixture::new(Some((expected.clone(), 11))).await;
    assert!(
        fixture.read(Some(2), 0).await.is_err(),
        "End2 cannot skip DATA1"
    );
    assert!(
        fixture.read(None, 2).await.is_err(),
        "ACK2 cannot precede any actual offer"
    );
    assert_eq!(
        fixture.original.loopback.single_root_result_progress(),
        (false, 0)
    );
    {
        let data = fixture.read(Some(1), 0).await.unwrap();
        assert!(matches!(&reply(&data).outcome, RootReadOutcome::Data(data)
            if data.sequence().get() == 1 && data.body() == &expected));
        assert!(
            fixture.read(None, 2).await.is_err(),
            "DATA1 alone does not authorize ACK2"
        );
        assert_eq!(
            fixture.original.loopback.single_root_result_progress(),
            (false, 0)
        );
        drop(data);
        let data_ack = fixture.read(None, 1).await.unwrap();
        assert_eq!(reply(&data_ack).accepted_consumed, 1);
        assert_eq!(reply(&data_ack).outcome, RootReadOutcome::AckOnly);
        drop(data_ack);
        let end = fixture.read(Some(2), 1).await.unwrap();
        assert!(matches!(reply(&end).outcome, RootReadOutcome::End(end)
            if end.sequence.get() == 2 && end.output_rows == 11));
        drop(end);
        let ack = fixture.read(None, 2).await.unwrap();
        assert_eq!(reply(&ack).accepted_consumed, 2);
        assert_eq!(reply(&ack).outcome, RootReadOutcome::AckOnly);
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn default_end_only_root_rpc_retains_end_one_and_exact_ack_one() {
    let fixture = RootWireFixture::new(None).await;
    {
        let end = fixture.read(Some(1), 0).await.unwrap();
        assert_eq!(reply(&end).kind, RootOutputKind::CountOnly);
        assert!(matches!(reply(&end).outcome, RootReadOutcome::End(end)
            if end.sequence.get() == 1 && end.output_rows == 0));
        assert!(
            fixture.read(None, 2).await.is_err(),
            "EndOnly never offers sequence2"
        );
        assert!(fixture.read(Some(2), 0).await.is_err());
        drop(end);
        let ack = fixture.read(None, 1).await.unwrap();
        assert_eq!(reply(&ack).accepted_consumed, 1);
        assert_eq!(reply(&ack).outcome, RootReadOutcome::AckOnly);
        assert_eq!(
            fixture.original.loopback.single_root_result_progress(),
            (true, 1)
        );
    }
    fixture.finish().await;
}
