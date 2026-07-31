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

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorMutationOperationId, ConnectorProviderId,
    ConnectorRefreshPublicationGuard, ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit,
    ConnectorTableHandle, ExternalMutationEvidence, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[test]
fn provider_id_rejects_non_canonical_values() {
    assert_eq!(
        ConnectorProviderId::parse("iceberg")
            .expect("canonical provider ID")
            .as_str(),
        "iceberg"
    );
    for invalid in [
        "",
        "Iceberg",
        "iceberg/catalog",
        "iceberg catalog",
        "iceberg!",
    ] {
        assert_eq!(
            ConnectorProviderId::parse(invalid)
                .expect_err("non-canonical provider ID must fail")
                .kind(),
            ConnectorErrorKind::InvalidRequest,
            "{invalid}"
        );
    }
}

#[test]
fn instance_id_normalizes_catalog_identity_without_accepting_paths() {
    assert_eq!(
        ConnectorInstanceId::parse("Lake.Catalog")
            .expect("normalizable catalog instance")
            .as_str(),
        "lake.catalog"
    );
    assert_eq!(
        ConnectorInstanceId::parse("../lake")
            .expect_err("path-like instance ID must fail")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn connector_ids_reject_values_past_their_contract_limits() {
    assert_eq!(
        ConnectorProviderId::parse(&"a".repeat(65))
            .expect_err("provider IDs over 64 bytes must fail")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(
        ConnectorInstanceId::parse(&format!("a{}", "a".repeat(128)))
            .expect_err("instance IDs over 128 bytes must fail")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn cleanup_context_never_replaces_the_primary_read_error() {
    let error = ConnectorError::new(ConnectorErrorKind::Unavailable, "reader failed")
        .with_retryable_before_progress()
        .with_cleanup_context("close failed");

    assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
    assert!(error.retryable_before_progress());
    assert!(error.to_string().contains("cleanup: close failed"));
}

#[test]
fn connector_handles_preserve_their_typed_owner() {
    let owner = ConnectorInstanceId::parse("lake.catalog").expect("instance ID");
    let table = ConnectorTableHandle::try_new(owner.clone(), Bytes::from_static(b"table-v1"))
        .expect("bounded table handle");
    let scan = ConnectorScanHandle::try_new(owner.clone(), Bytes::from_static(b"scan-v1"))
        .expect("bounded scan handle");
    let split = ConnectorSplit::try_new(
        owner.clone(),
        "split-0001",
        Bytes::from_static(b"split-v1"),
        Some(128),
    )
    .expect("bounded split");

    assert_eq!(table.owner(), &owner);
    assert_eq!(scan.owner(), &owner);
    assert_eq!(split.owner(), &owner);
    assert_eq!(split.split_id(), "split-0001");
    assert_eq!(split.estimated_bytes(), Some(128));
}

#[test]
fn handle_payload_over_the_hard_limit_is_rejected() {
    let owner = ConnectorInstanceId::parse("file").expect("instance ID");
    let oversized = Bytes::from(vec![0_u8; MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES + 1]);

    assert_eq!(
        ConnectorTableHandle::try_new(owner, oversized)
            .expect_err("oversized payload must not allocate into a handle")
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

#[test]
fn request_context_rejects_an_unbounded_payload_budget() {
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(NeverCancelled),
            0,
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )
        .err()
        .expect("a zero handle budget would make admission ambiguous")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn request_context_allows_a_query_budget_for_multiple_handles() {
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(1),
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES * 2,
    )
    .expect("a query budget may cover more than one bounded handle");

    assert_eq!(
        context.max_total_payload_bytes(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES * 2
    );
}

#[test]
fn external_mutation_evidence_is_bounded_and_redacted() {
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
        instance_id: ConnectorInstanceId::parse("lake.catalog").expect("instance ID"),
    };
    let evidence = ExternalMutationEvidence::try_new(
        1,
        descriptor,
        ConnectorInstanceIncarnation::from_bytes([7; 16]),
        ConnectorMutationOperationId::from_bytes([9; 16]),
        "create-table",
        Bytes::from_static(b"secret-provider-payload"),
    )
    .expect("bounded evidence");
    assert_eq!(evidence.digest(), evidence.digest());
    let debug = format!("{evidence:?}");
    assert!(debug.contains("provider_payload_len"));
    assert!(!debug.contains("secret-provider-payload"));

    assert_eq!(
        ExternalMutationEvidence::try_new(
            1,
            evidence.descriptor().clone(),
            evidence.incarnation(),
            evidence.operation_id(),
            evidence.operation_kind(),
            Bytes::from(vec![0; MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES + 1]),
        )
        .expect_err("oversized evidence must fail")
        .kind(),
        ConnectorErrorKind::ResourceExhausted,
    );
}

#[test]
fn refresh_publication_guard_is_bounded_stable_and_redacted() {
    for (refresh_id, materialized_view_id, token) in [(0, 4, "token"), (3, 0, "token"), (3, 4, "")]
    {
        assert_eq!(
            ConnectorRefreshPublicationGuard::try_new(refresh_id, materialized_view_id, token)
                .expect_err("invalid guard identity must fail")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
    assert_eq!(
        ConnectorRefreshPublicationGuard::try_new(
            3,
            4,
            "x".repeat(ConnectorRefreshPublicationGuard::MAX_TOKEN_BYTES + 1),
        )
        .expect_err("oversized guard token must fail")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );

    let guard = ConnectorRefreshPublicationGuard::try_new(3, 4, "secret-refresh-token")
        .expect("bounded guard");
    let same = ConnectorRefreshPublicationGuard::try_new(3, 4, "secret-refresh-token")
        .expect("same guard");
    let different = ConnectorRefreshPublicationGuard::try_new(3, 4, "different-token")
        .expect("different guard");
    assert_eq!(guard.digest(), same.digest());
    assert_ne!(guard.digest(), different.digest());
    let debug = format!("{guard:?}");
    assert!(debug.contains("token_len"));
    assert!(!debug.contains("secret-refresh-token"));
}
