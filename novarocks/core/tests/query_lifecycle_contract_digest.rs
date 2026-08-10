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

#![cfg(feature = "query-execution-contract-test-support")]

use std::time::Duration;

use novarocks::query_execution::lifecycle::contract::{
    QueryInitRequest, decode_query_init_request, encode_query_init_request,
};
use novarocks::query_execution::lifecycle::{
    AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions,
    ParticipantRole, QueryControlEndpoint, QueryExecutionId, QueryLifecycleErrorCode,
    RuntimeFilterContribution,
};
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_types::QueryId;

fn request_with_runtime_filter() -> QueryInitRequest {
    let execution_id = QueryExecutionId::new(
        QueryId::new(41, 42),
        AttemptId::new(7).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let contribution = RuntimeFilterContribution::empty_for_contract_test(execution_id, 3)
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
        ParticipantQueryOptions::new(QueryOptions::default()),
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
    let wire = encode_query_init_request(&request).expect("request encodes");

    let decoded = decode_query_init_request(&wire).expect("canonical request decodes");

    assert_eq!(decoded.manifest(), request.manifest());
    assert_eq!(decoded.digest(), request.digest());
}

#[test]
fn runtime_filter_contribution_digest_rejects_mutated_payload() {
    let mut wire =
        encode_query_init_request(&request_with_runtime_filter()).expect("request encodes");
    let lifecycle = wire
        .manifest
        .as_mut()
        .expect("manifest")
        .runtime_filter
        .as_mut()
        .expect("runtime filter contribution")
        .lifecycle
        .as_mut()
        .expect("runtime filter lifecycle");
    lifecycle.delivery_expire_ms += 1;

    let error = decode_query_init_request(&wire)
        .expect_err("mutated runtime filter payload must not retain the original digest");

    assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
    assert_eq!(
        error.detail(),
        "runtime filter contribution digest does not match canonical payload"
    );
}
