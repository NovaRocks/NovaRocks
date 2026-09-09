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

use std::mem::size_of;

use novarocks_state_store_api::{
    Precondition, StateStoreError, StateStoreErrorKind, StateStoreLimits,
};

const MUTATION_KIND_BYTES: usize = 1;
const PRECONDITION_KIND_BYTES: usize = 1;
const COMMIT_TRANSACTION_ID_BYTES: usize = 16;
const COMMIT_STATE_BYTES: usize = size_of::<u8>();
const COMMIT_RESERVATION_BYTES: usize = 16;
const COMMIT_REVISION_BYTES: usize = size_of::<u64>();
const COMMIT_UPDATED_AT_BYTES: usize = size_of::<u64>();
const CURRENT_REVISION_BYTES: usize = size_of::<u64>();
const CHANGE_REVISION_BYTES: usize = size_of::<u64>();
const CHANGE_SEQUENCE_BYTES: usize = size_of::<u32>();
const PERSISTED_VERSION_BYTES: usize = size_of::<u64>() + size_of::<u32>();
const PROVISIONAL_VERSION_BYTES: usize = 21 + 16 + size_of::<u64>();

// Account the complete v1 logical durability footprint, including the commit row and
// current-revision update owned by Task 6. This is intentionally not a MySQL wire-size estimate.
pub(super) const TRANSACTION_ENVELOPE_BYTES: usize = COMMIT_TRANSACTION_ID_BYTES
    + COMMIT_STATE_BYTES
    + COMMIT_RESERVATION_BYTES
    + COMMIT_REVISION_BYTES
    + COMMIT_UPDATED_AT_BYTES
    + CURRENT_REVISION_BYTES;

#[derive(Debug)]
pub(super) struct TransactionBudget {
    limits: StateStoreLimits,
    operations: usize,
    bytes: usize,
}

impl TransactionBudget {
    pub(super) fn new(limits: StateStoreLimits) -> Result<Self, StateStoreError> {
        if TRANSACTION_ENVELOPE_BYTES > limits.max_transaction_bytes {
            return Err(byte_limit());
        }
        Ok(Self {
            limits,
            operations: 0,
            bytes: TRANSACTION_ENVELOPE_BYTES,
        })
    }

    pub(super) fn stage_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_put_bytes(key, value, precondition)?)
    }

    pub(super) fn stage_delete(
        &mut self,
        key: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_delete_bytes(key, precondition)?)
    }

    fn stage(&mut self, increment: usize) -> Result<u64, StateStoreError> {
        let operations = self.operations.checked_add(1).ok_or_else(operation_limit)?;
        if operations > self.limits.max_transaction_operations {
            return Err(operation_limit());
        }
        let bytes = self.bytes.checked_add(increment).ok_or_else(byte_limit)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(byte_limit());
        }
        self.operations = operations;
        self.bytes = bytes;
        u64::try_from(operations).map_err(|_| operation_limit())
    }
}

pub(super) fn accounted_put_bytes(
    key: &[u8],
    value: &[u8],
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let mut bytes = accounted_mutation_base(key, precondition)?;
    bytes = checked_add(bytes, value.len())?;
    bytes = checked_add(bytes, PROVISIONAL_VERSION_BYTES)?;
    bytes = checked_add(bytes, PERSISTED_VERSION_BYTES)?;
    Ok(bytes)
}

pub(super) fn accounted_delete_bytes(
    key: &[u8],
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    accounted_mutation_base(key, precondition)
}

fn accounted_mutation_base(
    key: &[u8],
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let mut bytes = MUTATION_KIND_BYTES;
    bytes = checked_add(bytes, key.len())?;
    bytes = checked_add(bytes, PRECONDITION_KIND_BYTES)?;
    if let Precondition::Version(version) = precondition {
        bytes = checked_add(bytes, version.as_bytes().len())?;
    }
    bytes = checked_add(bytes, key.len())?;
    bytes = checked_add(bytes, CHANGE_REVISION_BYTES)?;
    checked_add(bytes, CHANGE_SEQUENCE_BYTES)
}

fn checked_add(total: usize, increment: usize) -> Result<usize, StateStoreError> {
    total.checked_add(increment).ok_or_else(byte_limit)
}

const fn operation_limit() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "transaction operation limit exceeded",
    )
}

const fn byte_limit() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "transaction byte limit exceeded",
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        TRANSACTION_ENVELOPE_BYTES, TransactionBudget, accounted_delete_bytes, accounted_put_bytes,
    };
    use novarocks_state_store_api::{
        Precondition, StateStoreErrorKind, StateStoreLimits, VersionToken,
    };

    fn limits(bytes: usize, operations: usize) -> StateStoreLimits {
        StateStoreLimits {
            max_transaction_bytes: bytes,
            max_transaction_operations: operations,
            ..StateStoreLimits::default()
        }
    }

    #[test]
    fn mysql_transaction_budget_checks_fixed_envelope_exactly() {
        let error = TransactionBudget::new(limits(TRANSACTION_ENVELOPE_BYTES - 1, 100))
            .expect_err("fixed envelope minus one must fail");
        assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);

        TransactionBudget::new(limits(TRANSACTION_ENVELOPE_BYTES, 100))
            .expect("exact fixed envelope must fit");
    }

    #[test]
    fn mysql_transaction_budget_accounts_put_and_delete_physical_boundaries() {
        let version =
            VersionToken::try_from(Bytes::from_static(b"expected-version")).expect("version token");
        let put_bytes = accounted_put_bytes(b"key", b"value", &Precondition::Version(version))
            .expect("put bytes");
        let delete_bytes =
            accounted_delete_bytes(b"other", &Precondition::Present).expect("delete bytes");
        let exact = TRANSACTION_ENVELOPE_BYTES + put_bytes + delete_bytes;

        let mut budget = TransactionBudget::new(limits(exact, 100)).expect("exact budget");
        budget
            .stage_put(
                b"key",
                b"value",
                &Precondition::Version(
                    VersionToken::try_from(Bytes::from_static(b"expected-version"))
                        .expect("version token"),
                ),
            )
            .expect("exact put");
        budget
            .stage_delete(b"other", &Precondition::Present)
            .expect("exact delete");

        let mut under = TransactionBudget::new(limits(exact - 1, 100)).expect("under budget");
        under
            .stage_put(
                b"key",
                b"value",
                &Precondition::Version(
                    VersionToken::try_from(Bytes::from_static(b"expected-version"))
                        .expect("version token"),
                ),
            )
            .expect("first mutation fits");
        assert_eq!(
            under
                .stage_delete(b"other", &Precondition::Present)
                .expect_err("exact minus one must reject")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    #[test]
    fn mysql_transaction_budget_operation_limit_is_independent() {
        let mutation_bytes =
            accounted_delete_bytes(b"k", &Precondition::Any).expect("delete bytes");
        let mut budget =
            TransactionBudget::new(limits(TRANSACTION_ENVELOPE_BYTES + mutation_bytes * 2, 1))
                .expect("byte budget fits two operations");
        budget
            .stage_delete(b"k", &Precondition::Any)
            .expect("one operation fits");
        assert_eq!(
            budget
                .stage_delete(b"k", &Precondition::Any)
                .expect_err("second operation exceeds operation limit")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}
