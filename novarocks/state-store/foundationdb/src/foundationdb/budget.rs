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

use super::codec::{ATTEMPT_TAG_BYTES, REVISION_BYTES};
use novarocks_state_store_api::{
    Precondition, StateStoreError, StateStoreErrorKind, StateStoreLimits,
};

const RECORD_TAG_BYTES: usize = 1;
const RECORD_ENVELOPE_BYTES: usize = 1 + ATTEMPT_TAG_BYTES;
const VERSIONSTAMP_TRAILER_BYTES: usize = 4;
const COMMIT_TAG_BYTES: usize = 1;
const COMMITTED_VALUE_OPERAND_BYTES: usize = 1 + REVISION_BYTES + VERSIONSTAMP_TRAILER_BYTES;

#[derive(Clone, Debug)]
pub(super) struct TransactionBudget {
    operations: usize,
    bytes: usize,
    limits: StateStoreLimits,
    root_len: usize,
}

impl TransactionBudget {
    pub(super) fn new(limits: StateStoreLimits, root_len: usize) -> Result<Self, StateStoreError> {
        let bytes = fixed_envelope_bytes(root_len)?;
        if bytes > limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        Ok(Self {
            operations: 0,
            bytes,
            limits,
            root_len,
        })
    }

    pub(super) fn stage_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_put_bytes(
            self.root_len,
            key.len(),
            value.len(),
            precondition,
        )?)
    }

    pub(super) fn stage_delete(
        &mut self,
        key: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_delete_bytes(
            self.root_len,
            key.len(),
            precondition,
        )?)
    }

    pub(super) fn charge_get_conflict(&mut self, key_len: usize) -> Result<(), StateStoreError> {
        let physical_key = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, key_len)?;
        self.charge_bytes(exact_conflict_bytes(physical_key)?)
    }

    pub(super) fn charge_range_conflict(
        &mut self,
        start_len: usize,
        end_len: usize,
    ) -> Result<(), StateStoreError> {
        let start = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, start_len)?;
        let end = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, end_len)?;
        self.charge_bytes(checked_add(start, end)?)
    }

    fn stage(&mut self, increment: usize) -> Result<u64, StateStoreError> {
        let operations = self
            .operations
            .checked_add(1)
            .ok_or_else(|| limit_error("transaction operation limit exceeded"))?;
        if operations > self.limits.max_transaction_operations {
            return Err(limit_error("transaction operation limit exceeded"));
        }
        let bytes = checked_add(self.bytes, increment)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        self.operations = operations;
        self.bytes = bytes;
        u64::try_from(operations).map_err(|_| limit_error("transaction operation limit exceeded"))
    }

    fn charge_bytes(&mut self, increment: usize) -> Result<(), StateStoreError> {
        let bytes = checked_add(self.bytes, increment)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        self.bytes = bytes;
        Ok(())
    }

    #[cfg(test)]
    fn accounted_bytes(&self) -> usize {
        self.bytes
    }
}

fn fixed_envelope_bytes(root_len: usize) -> Result<usize, StateStoreError> {
    // The data transaction stages exactly one fixed mutation: the
    // versionstamped commit-state value that is this attempt's only durable
    // evidence. There is no separate reservation transaction and no change-feed
    // high watermark any more, so nothing else is charged up front.
    let commit_key = checked_add(root_len, COMMIT_TAG_BYTES + ATTEMPT_TAG_BYTES)?;
    let mut bytes = commit_key;
    bytes = checked_add(bytes, COMMITTED_VALUE_OPERAND_BYTES)?;
    bytes = checked_add(bytes, exact_conflict_bytes(commit_key)?)?;
    Ok(bytes)
}

fn accounted_put_bytes(
    root_len: usize,
    key_len: usize,
    value_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let record_key = checked_add(checked_add(root_len, RECORD_TAG_BYTES)?, key_len)?;
    let record_value = checked_add(RECORD_ENVELOPE_BYTES, value_len)?;
    let mut bytes = logical_request_bytes(key_len, value_len, precondition)?;
    bytes = checked_add(bytes, record_key)?;
    bytes = checked_add(bytes, record_value)?;
    // Non-snapshot precondition read and ordinary set exact conflicts.
    bytes = checked_add(bytes, checked_mul(exact_conflict_bytes(record_key)?, 2)?)?;
    Ok(bytes)
}

fn accounted_delete_bytes(
    root_len: usize,
    key_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let record_key = checked_add(checked_add(root_len, RECORD_TAG_BYTES)?, key_len)?;
    let mut bytes = logical_request_bytes(key_len, 0, precondition)?;
    bytes = checked_add(bytes, record_key)?;
    bytes = checked_add(bytes, checked_mul(exact_conflict_bytes(record_key)?, 2)?)?;
    Ok(bytes)
}

fn logical_request_bytes(
    key_len: usize,
    value_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let mut bytes = checked_add(1, key_len)?;
    bytes = checked_add(bytes, value_len)?;
    bytes = checked_add(bytes, 1)?;
    if let Precondition::Version(version) = precondition {
        bytes = checked_add(bytes, version.as_bytes().len())?;
    }
    Ok(bytes)
}

fn checked_add(left: usize, right: usize) -> Result<usize, StateStoreError> {
    left.checked_add(right)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn checked_mul(left: usize, right: usize) -> Result<usize, StateStoreError> {
    left.checked_mul(right)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn exact_conflict_bytes(key_len: usize) -> Result<usize, StateStoreError> {
    checked_add(checked_mul(key_len, 2)?, 1)
}

fn limit_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::LimitExceeded, message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use novarocks_state_store_api::VersionToken;

    fn limits(bytes: usize, operations: usize) -> StateStoreLimits {
        StateStoreLimits {
            max_transaction_bytes: bytes,
            max_transaction_operations: operations,
            ..StateStoreLimits::default()
        }
    }

    #[test]
    fn foundationdb_budget_includes_physical_envelopes() {
        let precondition = Precondition::Version(
            VersionToken::try_from(Bytes::from_static(b"persisted-version")).expect("version"),
        );
        let bytes = accounted_put_bytes(22, 3, 5, &precondition).expect("put accounting");
        assert!(bytes > 3 + 5 + b"persisted-version".len());
        assert!(fixed_envelope_bytes(22).expect("fixed accounting") > 0);
    }

    #[test]
    fn foundationdb_budget_counts_exact_point_conflict_endpoints() {
        let root_len = 22;
        let key_len = 3;
        let value_len = 5;
        let physical_key_len = root_len + RECORD_TAG_BYTES + key_len;

        assert_eq!(
            fixed_envelope_bytes(root_len).expect("fixed accounting"),
            157
        );
        assert_eq!(
            accounted_put_bytes(root_len, key_len, value_len, &Precondition::Any)
                .expect("put accounting"),
            172
        );
        assert_eq!(
            accounted_delete_bytes(root_len, key_len, &Precondition::Any)
                .expect("delete accounting"),
            137
        );

        let fixed = fixed_envelope_bytes(root_len).expect("fixed accounting");
        let mut budget =
            TransactionBudget::new(limits(fixed + 2 * physical_key_len + 1, 1), root_len)
                .expect("exact get budget");
        budget
            .charge_get_conflict(key_len)
            .expect("exact point read conflict fits");
        assert_eq!(budget.accounted_bytes(), fixed + 2 * physical_key_len + 1);
    }

    #[test]
    fn foundationdb_budget_rejects_one_byte_below_each_exact_boundary() {
        let root_len = 22;
        let key = b"key";
        let value = b"value";
        let fixed = 157;
        let put = 172;
        let delete = 137;
        let get = 2 * (root_len + RECORD_TAG_BYTES + key.len()) + 1;

        assert_eq!(
            TransactionBudget::new(limits(fixed - 1, 1), root_len)
                .expect_err("fixed envelope must include exact conflict ends")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );

        let mut get_budget =
            TransactionBudget::new(limits(fixed + get - 1, 1), root_len).expect("fixed budget");
        assert_eq!(
            get_budget
                .charge_get_conflict(key.len())
                .expect_err("point read conflict end exceeds boundary")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );

        let mut put_budget =
            TransactionBudget::new(limits(fixed + put - 1, 1), root_len).expect("fixed budget");
        assert_eq!(
            put_budget
                .stage_put(key, value, &Precondition::Any)
                .expect_err("put point conflict ends exceed boundary")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );

        let mut delete_budget =
            TransactionBudget::new(limits(fixed + delete - 1, 1), root_len).expect("fixed budget");
        assert_eq!(
            delete_budget
                .stage_delete(key, &Precondition::Any)
                .expect_err("delete point conflict ends exceed boundary")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    #[test]
    fn budget_is_charged_per_request_without_same_key_refunds() {
        let mut budget = TransactionBudget::new(limits(16 * 1024, 2), 22).expect("budget");
        let initial = budget.accounted_bytes();
        budget
            .stage_put(b"same", b"v1", &Precondition::Any)
            .expect("first put");
        let first = budget.accounted_bytes();
        budget
            .stage_put(b"same", b"v2", &Precondition::Any)
            .expect("second put");
        assert!(budget.accounted_bytes() > first);
        assert!(first > initial);
        assert_eq!(
            budget
                .stage_delete(b"same", &Precondition::Any)
                .expect_err("operation limit")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    #[test]
    fn budget_rejects_logical_maximum_when_physical_envelope_does_not_fit() {
        let mut budget = TransactionBudget::new(limits(16 * 1024, 10), 22).expect("budget");
        let value = vec![0; 16 * 1024];
        assert_eq!(
            budget
                .stage_put(b"key", &value, &Precondition::Any)
                .expect_err("physical overhead exceeds limit")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    #[test]
    fn conformance_limits_fit_four_max_puts_and_eight_small_mutations() {
        const CONFORMANCE_TRANSACTION_BYTES: usize = 16 * 1024;
        const CONFORMANCE_MAX_VALUE_BYTES: usize = 1_899;
        const KEYSPACE_ROOT_BYTES: usize = 5 + 16;
        const CONFORMANCE_KEY_BYTES: usize = 3;

        let mut max_value_budget = TransactionBudget::new(
            limits(CONFORMANCE_TRANSACTION_BYTES, 8),
            KEYSPACE_ROOT_BYTES,
        )
        .expect("conformance max-value budget");
        for suffix in 1_u8..=4 {
            max_value_budget
                .stage_put(
                    &[11, 0xfe, suffix],
                    &vec![suffix; CONFORMANCE_MAX_VALUE_BYTES],
                    &Precondition::Any,
                )
                .expect("four max-sized puts fit");
        }
        assert_eq!(
            max_value_budget.accounted_bytes(),
            fixed_envelope_bytes(KEYSPACE_ROOT_BYTES).expect("fixed envelope")
                + 4 * accounted_put_bytes(
                    KEYSPACE_ROOT_BYTES,
                    CONFORMANCE_KEY_BYTES,
                    CONFORMANCE_MAX_VALUE_BYTES,
                    &Precondition::Any,
                )
                .expect("max-sized put accounting")
        );
        assert!(max_value_budget.accounted_bytes() < CONFORMANCE_TRANSACTION_BYTES);
        assert_eq!(
            max_value_budget
                .stage_put(
                    &[11, 0xfe, 5],
                    &vec![5; CONFORMANCE_MAX_VALUE_BYTES],
                    &Precondition::Any,
                )
                .expect_err("fifth max-sized put exceeds byte budget")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );

        let mut operation_budget = TransactionBudget::new(
            limits(CONFORMANCE_TRANSACTION_BYTES, 8),
            KEYSPACE_ROOT_BYTES,
        )
        .expect("conformance operation budget");
        for index in 0_u8..8 {
            operation_budget
                .stage_put(&[11, 0, index], b"v", &Precondition::Any)
                .expect("eight small mutations fit the byte budget");
        }
        assert_eq!(
            operation_budget.accounted_bytes(),
            fixed_envelope_bytes(KEYSPACE_ROOT_BYTES).expect("fixed envelope")
                + 8 * accounted_put_bytes(
                    KEYSPACE_ROOT_BYTES,
                    CONFORMANCE_KEY_BYTES,
                    1,
                    &Precondition::Any,
                )
                .expect("small put accounting")
        );
        assert!(operation_budget.accounted_bytes() < CONFORMANCE_TRANSACTION_BYTES);
    }
}
