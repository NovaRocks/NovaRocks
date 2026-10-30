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

use super::is_operation_reply_preflight_failure;
use crate::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
use novarocks_proto_models::novarocks as wire;
use prost::Message;

// This is the real generated client's Codec path. The in-memory transport
// returns one legal gRPC frame; it is not a replacement decoder or a peer task.
async fn decode_through_generated_client(
    response: wire::ApplyTaskOperationsResponse,
) -> Result<wire::ApplyTaskOperationsResponse, tonic::Status> {
    let payload = response.encode_to_vec();
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&payload);
    let frame = bytes::Bytes::from(frame);
    let service = tower::service_fn(move |_request: hyper::Request<tonic::body::BoxBody>| {
        let frame = frame.clone();
        async move {
            Ok::<_, std::convert::Infallible>(
                hyper::Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(axum::body::Body::from(frame))
                    .unwrap(),
            )
        }
    });
    NovaRocksGrpcClient::new(service)
        .apply_task_operations(wire::ApplyTaskOperationsRequest::default())
        .await
        .map(tonic::Response::into_inner)
}

#[tokio::test]
async fn generated_client_bounds_apply_reply_before_prost_expands_receipts() {
    let maximum = novarocks_task_codec::TransportBudget::DEFAULT.max_batch_items();
    let allowed = wire::ApplyTaskOperationsResponse {
        receipts: vec![wire::TaskOperationReceipt::default(); maximum],
    };
    assert_eq!(
        decode_through_generated_client(allowed)
            .await
            .unwrap()
            .receipts
            .len(),
        maximum
    );
    let error = decode_through_generated_client(wire::ApplyTaskOperationsResponse {
        receipts: vec![wire::TaskOperationReceipt::default(); maximum + 1],
    })
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
    assert!(is_operation_reply_preflight_failure(&error));
    assert!(!is_operation_reply_preflight_failure(
        &tonic::Status::data_loss("remote error")
    ));
}

#[tokio::test]
async fn generated_client_bounds_nested_ack_and_release_telemetry() {
    use wire::{task_operation_receipt::Result, task_operation_verdict::Ack};
    let domain_ack = Ack::CreateTask(wire::CreateTaskAck {
        accepted_domains: vec![
            wire::TaskDomainReceipt::default();
            novarocks_task_codec::domain::MAX_DOMAIN_UPDATES + 1
        ],
        ..Default::default()
    });
    let release_ack = Ack::ReleaseQueryContext(wire::ReleaseQueryContextAck {
        runtime_filter: Some(wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(wire::query_terminal_profile_contribution_telemetry::Telemetry::Available(wire::QueryTerminalProfileContributionV1 {
                version: 1,
                channels: vec![wire::QueryTerminalRuntimeFilterChannelV1::default(); novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES + 1],
                ..Default::default()
            }))
        }),
        ..Default::default()
    });
    for ack in [domain_ack, release_ack] {
        let error = decode_through_generated_client(wire::ApplyTaskOperationsResponse {
            receipts: vec![wire::TaskOperationReceipt {
                result: Some(Result::Verdict(wire::TaskOperationVerdict {
                    ack: Some(ack),
                    ..Default::default()
                })),
                ..Default::default()
            }],
        })
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert!(is_operation_reply_preflight_failure(&error));
    }
}
