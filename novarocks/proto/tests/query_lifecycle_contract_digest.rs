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

use std::time::Duration;

use novarocks_proto::{
    lifecycle::{
        AttemptId, ContractErrorCode, ParticipantBackendIdentity, ParticipantManifest,
        ParticipantRole, QueryControlEndpoint, QueryExecutionId, QueryInitRequest, QueryOptions,
        RuntimeFilterContribution,
    },
    novarocks,
};
use novarocks_types::QueryId;

fn request_with_runtime_filter() -> QueryInitRequest {
    let execution_id = QueryExecutionId::new(
        QueryId::new(41, 42),
        AttemptId::new(7).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let contribution = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
        participant_id: 3,
        contribution_digest: vec![0; 32],
        ..Default::default()
    })
    .expect("valid contribution");
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            2,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            11,
        )
        .expect("valid backend"),
        [ParticipantRole::RuntimeFilterService],
        [],
        QueryOptions::parse(novarocks::QueryOptions::default()).expect("valid query options"),
        10_000,
        [],
        Some(contribution),
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid manifest");
    QueryInitRequest::from_manifest(manifest)
}

#[test]
fn runtime_filter_contribution_digest_round_trips_canonical_payload() {
    let request = request_with_runtime_filter();
    let wire = request.as_proto().clone();

    let decoded = QueryInitRequest::parse(wire).expect("canonical request decodes");

    assert_eq!(
        decoded.manifest().expect("decoded manifest"),
        request.manifest().expect("request manifest")
    );
    assert_eq!(
        decoded.digest().expect("decoded digest"),
        request.digest().expect("request digest")
    );
}

#[test]
fn participant_manifest_digest_rejects_mutated_runtime_filter_digest() {
    let mut wire = request_with_runtime_filter().as_proto().clone();
    let contribution = wire
        .manifest
        .as_mut()
        .expect("manifest")
        .runtime_filter
        .as_mut()
        .expect("runtime filter contribution");
    contribution.contribution_digest[0] ^= 1;

    let error = QueryInitRequest::parse(wire)
        .expect_err("mutated runtime filter carrier must not retain the manifest digest");

    assert_eq!(error.code(), ContractErrorCode::InvalidValue);
    assert_eq!(
        error.detail(),
        "participant manifest digest does not match canonical projection"
    );
}
