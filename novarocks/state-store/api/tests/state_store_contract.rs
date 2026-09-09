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
use novarocks_state_store_api::{
    ChangeCursor, Direction, Key, KeyRange, RangeRequest, StateStore, StateStoreErrorKind,
    StateStoreLimits, StateStoreMetricsSnapshot, StateStoreOperation, StateStoreOutcome,
    StateStoreProviderId, StoreRevision, Value, VersionToken,
};
use uuid::Uuid;

const TEST_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("contract-test");

#[allow(dead_code)]
fn assert_object_safe(_: &dyn StateStore) {}

fn key(value: &'static [u8]) -> Key {
    Key::try_from(Bytes::from_static(value)).expect("valid key")
}

#[test]
fn common_binary_limits_are_owned_by_spi() {
    let limits = StateStoreLimits::default();
    assert_eq!(limits.max_key_bytes, 8 * 1024);
    assert_eq!(limits.max_value_bytes, 64 * 1024);
    assert!(Key::try_from(Bytes::from(vec![0xff; limits.max_key_bytes])).is_ok());
    assert_eq!(
        Key::try_from(Bytes::from(vec![0; limits.max_key_bytes + 1]))
            .expect_err("oversized key")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert!(Value::try_from(Bytes::from(vec![0xff; limits.max_value_bytes])).is_ok());
    assert_eq!(
        Value::try_from(Bytes::from(vec![0; limits.max_value_bytes + 1]))
            .expect_err("oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
}

#[test]
fn continuation_is_bound_to_range_and_direction() {
    let request = RangeRequest {
        range: KeyRange::new(key(b"a"), key(b"z")).expect("valid range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: None,
    };
    let token = request
        .continuation_after(&key(b"m"))
        .expect("continuation");
    let mut wrong = request.clone();
    wrong.direction = Direction::Reverse;
    wrong.continuation = Some(token);
    assert_eq!(
        wrong
            .validate(&StateStoreLimits::default())
            .expect_err("direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let wrong_range = RangeRequest {
        range: KeyRange::new(key(b"a"), key(b"y")).expect("different valid range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: Some(
            request
                .continuation_after(&key(b"m"))
                .expect("continuation"),
        ),
    };
    assert_eq!(
        wrong_range
            .validate(&StateStoreLimits::default())
            .expect_err("range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn opaque_tokens_reject_empty_values() {
    assert!(VersionToken::try_from(Bytes::from_static(b"version")).is_ok());
    assert_eq!(
        VersionToken::try_from(Bytes::new())
            .expect_err("empty version token")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert!(StoreRevision::try_from(Bytes::from_static(b"revision")).is_ok());
    assert_eq!(
        StoreRevision::try_from(Bytes::new())
            .expect_err("empty store revision")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn range_page_size_enforces_zero_and_overflow_bounds() {
    let range = KeyRange::new(key(b"a"), key(b"z")).expect("valid range");
    for page_size in [0, StateStoreLimits::default().max_page_size + 1] {
        let request = RangeRequest {
            range: range.clone(),
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        assert_eq!(
            request
                .validate(&StateStoreLimits::default())
                .expect_err("invalid page size")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}

#[test]
fn prefix_ranges_require_a_finite_successor() {
    let range = KeyRange::for_prefix(key(&[0, 0xff])).expect("finite successor");
    assert_eq!(range.start.as_bytes(), &[0, 0xff]);
    assert_eq!(range.end.as_bytes(), &[1]);
    assert_eq!(
        KeyRange::for_prefix(key(&[0xff, 0xff]))
            .expect_err("no successor")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn change_cursors_reject_a_different_store() {
    let cursor = ChangeCursor::new(
        Uuid::from_u128(1),
        StoreRevision::try_from(Bytes::from_static(b"revision")).expect("revision"),
        42,
    )
    .expect("cursor");
    assert_eq!(
        cursor
            .decode(Uuid::from_u128(2))
            .expect_err("different store")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn default_limits_cover_all_contract_fields() {
    let limits = StateStoreLimits::default();
    assert_eq!(limits.max_key_bytes, 8 * 1024);
    assert_eq!(limits.max_value_bytes, 64 * 1024);
    assert_eq!(limits.max_page_size, 1_000);
    assert_eq!(limits.max_transaction_operations, 10_000);
    assert_eq!(limits.max_transaction_bytes, 4 * 1024 * 1024);
    assert_eq!(limits.transaction_deadline.as_secs(), 4);
    assert_eq!(limits.runner_max_attempts, 5);
}

#[test]
fn metrics_snapshot_uses_typed_provider_id_and_indexes_operation_outcome() {
    let mut operation_outcomes = [[0; 6]; 6];
    operation_outcomes[StateStoreOperation::Commit as usize]
        [StateStoreOutcome::Conflict as usize] = 7;
    let snapshot = StateStoreMetricsSnapshot {
        provider: TEST_STATE_STORE_PROVIDER_ID,
        begin_count: 0,
        get_count: 0,
        range_count: 0,
        put_count: 0,
        delete_count: 0,
        commit_count: 7,
        operation_outcomes,
        operation_duration_micros: [0; 6],
        operation_duration_observations: [0; 6],
        retry_count: 0,
        deadline_count: 0,
        blocking_failure_count: 0,
        bytes_read: 0,
        bytes_written: 0,
        page_records: 0,
        notification_lag_micros: 0,
        notification_lag_observations: 0,
    };
    assert_eq!(
        snapshot.operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Conflict),
        7
    );
    assert_eq!(snapshot.provider, TEST_STATE_STORE_PROVIDER_ID);
}
