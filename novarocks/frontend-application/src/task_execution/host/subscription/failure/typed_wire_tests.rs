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

//! The current Native cause classifier and the existing AP retry owner.
use super::*;
use novarocks_attempt_protocol::{
    ids::IdSeed,
    observation::retry::{CoveredRetryBudget, RetryDisposition},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::time::Duration;
fn generation(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn key() -> StreamKey {
    StreamKey {
        participation: novarocks_execution_contract::ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(71, 73), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ),
        generation: generation(1),
    }
}
fn now() -> novarocks_coordination_contract::MonotonicInstant {
    novarocks_coordination_contract::MonotonicInstant::from_origin(Duration::from_secs(1))
}
fn original_loss(number: u64, error: NativeSubscriptionFailure, expected: bool) -> bool {
    let kind = error.kind();
    assert_eq!(
        kind,
        ObservationFailureKind::RecoverableTransport {
            observed_connection_loss: expected
        }
    );
    let key = key();
    let record = SubscriptionFailureRecord::new(AttemptActionId::new(generation(number)), key);
    let cause = record.retain_native_failure(key, error);
    assert_eq!(cause.original, record.reference());
    assert_eq!(
        cause.site,
        AttemptFailureSite::Participation(key.participation)
    );
    assert!(
        !record.is_terminal_candidate(),
        "the Native classifier owns no reconnect exhaustion policy"
    );
    let original = record.take(cause.original).unwrap();
    assert_eq!(
        original.original.kind(),
        kind,
        "the SAME original typed cause survives the passive projection"
    );
    assert!(record.take(cause.original).is_none());
    match kind {
        ObservationFailureKind::RecoverableTransport {
            observed_connection_loss,
        } => observed_connection_loss,
        other => panic!("the original recoverable transport kind changed: {other:?}"),
    }
}
#[test]
fn covered_connection_evidence_requires_typed_loss_and_resets_only_on_liveness() {
    let mut retry = CoveredRetryBudget::new(8, IdSeed::new(17, 1), 77).unwrap();
    for (n, status) in [
        tonic::Status::unavailable("connection refused is only untrusted text"),
        tonic::Status::deadline_exceeded("local read timeout"),
    ]
    .into_iter()
    .enumerate()
    {
        let n = u64::try_from(n).unwrap() + 1;
        retry.begin_generation(generation(n)).unwrap();
        let loss = original_loss(n, NativeSubscriptionFailure::Open(status), false);
        assert!(matches!(
            retry.failed(generation(n), now(), loss),
            RetryDisposition::Backoff { .. }
        ));
        assert!(!retry.typed_connection_loss());
    }
    let opaque_channel = NativeSubscriptionFailure::Channel(
        crate::native::transport::ChannelAcquisitionError::retryable_network("connection reset"),
    );
    assert!(!original_loss(3, opaque_channel, false));
    let reset = || {
        tonic::Status::from_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )))
    };
    let loss = original_loss(4, NativeSubscriptionFailure::Open(reset()), true);
    retry.begin_generation(generation(3)).unwrap();
    assert!(matches!(
        retry.failed(generation(3), now(), loss),
        RetryDisposition::Backoff { .. }
    ));
    assert!(retry.typed_connection_loss());
    // Opening another original generation is not a real liveness fact.
    retry.begin_generation(generation(4)).unwrap();
    assert!(retry.typed_connection_loss());
    assert!(matches!(
        retry.failed(generation(4), now(), false),
        RetryDisposition::Backoff { .. }
    ));
    assert!(retry.typed_connection_loss());
    retry.begin_generation(generation(5)).unwrap();
    retry.liveness(generation(5));
    assert!(!retry.typed_connection_loss());
    retry.failed(generation(5), now(), false);
    assert!(!retry.typed_connection_loss());
    retry.begin_generation(generation(6)).unwrap();
    retry.failed(
        generation(6),
        now(),
        original_loss(5, NativeSubscriptionFailure::Read(reset()), true),
    );
    assert!(retry.typed_connection_loss());
    // Preserve both exact old exhausted classifications using the production
    // AP owner rather than restoring the retired Native retry-state machine.
    let mut bounded = CoveredRetryBudget::new(2, IdSeed::new(19, 1), 77).unwrap();
    bounded.begin_generation(generation(1)).unwrap();
    bounded.failed(generation(1), now(), true);
    bounded.begin_generation(generation(2)).unwrap();
    assert_eq!(
        bounded.failed(generation(2), now(), false),
        RetryDisposition::Exhausted {
            connection_loss: true
        }
    );
    let mut ordinary = CoveredRetryBudget::new(1, IdSeed::new(23, 1), 77).unwrap();
    ordinary.begin_generation(generation(1)).unwrap();
    assert_eq!(
        ordinary.failed(generation(1), now(), false),
        RetryDisposition::Exhausted {
            connection_loss: false
        }
    );
}
