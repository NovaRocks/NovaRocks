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

use foundationdb::options::TransactionOption;
use foundationdb::{Database, FdbError, Transaction};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use super::codec::{KeyspaceCodec, REVISION_BYTES};
use novarocks_spi::state_store::{StateStoreError, StateStoreErrorKind, StoreIdentity};

const OPEN_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_AUTHORITATIVE_READ_ATTEMPTS: usize = 5;
const NOT_COMMITTED_ERROR_CODE: i32 = 1020;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdentityCommitErrorDisposition {
    AuthoritativeReload,
    FailFast,
}

fn classify_identity_commit_error(error: FdbError) -> IdentityCommitErrorDisposition {
    if error.code() == NOT_COMMITTED_ERROR_CODE || error.is_maybe_committed() {
        IdentityCommitErrorDisposition::AuthoritativeReload
    } else {
        IdentityCommitErrorDisposition::FailFast
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IdentitySnapshot {
    pub identity: StoreIdentity,
    pub high_watermark: [u8; REVISION_BYTES],
    pub retention_floor: [u8; REVISION_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum IdentityRead {
    Absent,
    Present(IdentitySnapshot),
}

pub(super) async fn open_identity(
    database: &Database,
    codec: &KeyspaceCodec,
    cluster_id: &str,
) -> Result<IdentitySnapshot, StateStoreError> {
    let deadline = Instant::now() + OPEN_TIMEOUT;
    probe_read_version(database, deadline).await?;

    let transaction = create_bounded_transaction(database, deadline)?;
    let values = read_identity_values(&transaction, codec, deadline).await?;
    match decode_identity_values(codec, cluster_id, &values)? {
        IdentityRead::Present(identity) => Ok(identity),
        IdentityRead::Absent => {
            let store_id = Uuid::new_v4();
            write_new_identity(&transaction, codec, cluster_id, store_id);
            match timeout_at(deadline, transaction.commit()).await {
                Ok(Ok(_)) => Ok(IdentitySnapshot {
                    identity: StoreIdentity {
                        store_id,
                        cluster_id: cluster_id.to_owned(),
                    },
                    high_watermark: [0; REVISION_BYTES],
                    retention_floor: [0; REVISION_BYTES],
                }),
                Ok(Err(error)) => match classify_identity_commit_error(*error) {
                    IdentityCommitErrorDisposition::AuthoritativeReload => {
                        authoritative_reload(database, codec, cluster_id, deadline).await
                    }
                    IdentityCommitErrorDisposition::FailFast => Err(provider_error()),
                },
                Err(_) => authoritative_reload(database, codec, cluster_id, deadline).await,
            }
        }
    }
}

pub(super) fn decode_identity_values(
    codec: &KeyspaceCodec,
    expected_cluster_id: &str,
    values: &[Option<Vec<u8>>; 6],
) -> Result<IdentityRead, StateStoreError> {
    if values.iter().all(Option::is_none) {
        return Ok(IdentityRead::Absent);
    }
    if values.iter().any(Option::is_none) {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "FoundationDB state store identity is incomplete",
        ));
    }

    let value = |index: usize| {
        values[index]
            .as_deref()
            .expect("identity completeness checked above")
    };
    codec.decode_schema_version(value(0))?;
    let stored_cluster_id = codec.decode_cluster_id(value(1))?;
    if stored_cluster_id != expected_cluster_id {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "FoundationDB state store cluster identity does not match configuration",
        ));
    }
    let store_id = codec.decode_store_id(value(2))?;
    // This experimental provider retains its legacy physical metadata while
    // the public StoreIdentity exposes only store and cluster identity.
    codec.decode_initial_incarnation(value(3))?;
    let high_watermark = codec.decode_revision(value(4))?;
    let retention_floor = codec.decode_revision(value(5))?;
    Ok(IdentityRead::Present(IdentitySnapshot {
        identity: StoreIdentity {
            store_id,
            cluster_id: stored_cluster_id,
        },
        high_watermark,
        retention_floor,
    }))
}

async fn probe_read_version(database: &Database, deadline: Instant) -> Result<(), StateStoreError> {
    let transaction = create_bounded_transaction(database, deadline)?;
    timeout_at(deadline, transaction.get_read_version())
        .await
        .map_err(|_| deadline_error())?
        .map_err(|_| provider_error())?;
    Ok(())
}

async fn authoritative_reload(
    database: &Database,
    codec: &KeyspaceCodec,
    cluster_id: &str,
    deadline: Instant,
) -> Result<IdentitySnapshot, StateStoreError> {
    for _ in 0..MAX_AUTHORITATIVE_READ_ATTEMPTS {
        if Instant::now() >= deadline {
            return Err(deadline_error());
        }
        let transaction = create_bounded_transaction(database, deadline)?;
        match read_identity_values(&transaction, codec, deadline).await {
            Ok(values) => match decode_identity_values(codec, cluster_id, &values)? {
                IdentityRead::Present(identity) => return Ok(identity),
                IdentityRead::Absent => continue,
            },
            Err(error)
                if matches!(
                    error.kind(),
                    StateStoreErrorKind::ProviderUnavailable
                        | StateStoreErrorKind::DeadlineExceeded
                ) =>
            {
                if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB identity initialization could not be confirmed",
    ))
}

fn create_bounded_transaction(
    database: &Database,
    deadline: Instant,
) -> Result<Transaction, StateStoreError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(deadline_error());
    }
    let timeout_ms = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
    let transaction = database.create_trx().map_err(|_| provider_error())?;
    transaction
        .set_option(TransactionOption::Timeout(timeout_ms))
        .map_err(|_| provider_error())?;
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .map_err(|_| provider_error())?;
    Ok(transaction)
}

async fn read_identity_values(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    deadline: Instant,
) -> Result<[Option<Vec<u8>>; 6], StateStoreError> {
    let keys = [
        codec.schema_version_key(),
        codec.cluster_id_key(),
        codec.store_id_key(),
        codec.initial_incarnation_key(),
        codec.high_watermark_key(),
        codec.retention_floor_key(),
    ];
    let mut values: [Option<Vec<u8>>; 6] = std::array::from_fn(|_| None);
    for (index, key) in keys.iter().enumerate() {
        let value = timeout_at(deadline, transaction.get(key, false))
            .await
            .map_err(|_| deadline_error())?
            .map_err(|_| provider_error())?;
        values[index] = value.map(|bytes| bytes.as_ref().to_vec());
    }
    Ok(values)
}

fn write_new_identity(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    cluster_id: &str,
    store_id: Uuid,
) {
    transaction.set(&codec.schema_version_key(), &codec.schema_version_value());
    transaction.set(&codec.cluster_id_key(), &codec.cluster_id_value(cluster_id));
    transaction.set(&codec.store_id_key(), &codec.store_id_value(store_id));
    transaction.set(
        &codec.initial_incarnation_key(),
        &codec.initial_incarnation_value(),
    );
    transaction.set(&codec.high_watermark_key(), &codec.zero_revision_value());
    transaction.set(&codec.retention_floor_key(), &codec.zero_revision_value());
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state store open exceeded four seconds",
    )
}

fn provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB state store identity operation failed",
    )
}

#[cfg(test)]
mod tests {
    use foundationdb::FdbError;
    use uuid::Uuid;

    use super::{
        IdentityCommitErrorDisposition, IdentityRead, classify_identity_commit_error,
        decode_identity_values,
    };
    use crate::codec::KeyspaceCodec;
    use novarocks_spi::state_store::StateStoreErrorKind;

    fn codec() -> KeyspaceCodec {
        KeyspaceCodec::new(Uuid::from_bytes([0x11; 16]))
    }

    fn complete_values(cluster_id: &str, store_id: Uuid) -> [Option<Vec<u8>>; 6] {
        let codec = codec();
        [
            Some(codec.schema_version_value()),
            Some(codec.cluster_id_value(cluster_id)),
            Some(codec.store_id_value(store_id)),
            Some(codec.initial_incarnation_value()),
            Some(codec.zero_revision_value()),
            Some(codec.zero_revision_value()),
        ]
    }

    #[test]
    fn absent_identity_is_distinct_from_complete_identity() {
        assert_eq!(
            decode_identity_values(&codec(), "cluster-a", &[None, None, None, None, None, None])
                .expect("fully absent identity"),
            IdentityRead::Absent
        );

        let store_id = Uuid::from_bytes([0x22; 16]);
        let decoded = decode_identity_values(
            &codec(),
            "cluster-a",
            &complete_values("cluster-a", store_id),
        )
        .expect("complete identity");
        let IdentityRead::Present(snapshot) = decoded else {
            panic!("complete identity must be present");
        };
        assert_eq!(snapshot.identity.store_id, store_id);
        assert_eq!(snapshot.identity.cluster_id, "cluster-a");
        assert_eq!(snapshot.high_watermark, [0; 10]);
        assert_eq!(snapshot.retention_floor, [0; 10]);
    }

    #[test]
    fn partial_identity_fails_closed() {
        let mut values = [None, None, None, None, None, None];
        values[0] = Some(vec![1]);
        assert_eq!(
            decode_identity_values(&codec(), "cluster-a", &values)
                .expect_err("partial identity must fail")
                .kind(),
            StateStoreErrorKind::Corruption
        );
    }

    #[test]
    fn malformed_identity_fields_fail_closed() {
        for (field, malformed) in [
            (0, vec![2]),
            (1, vec![0xff]),
            (2, vec![0; 15]),
            (3, [1_u64.to_be_bytes().as_slice(), &[0]].concat()),
            (4, vec![0; 11]),
            (5, vec![0; 9]),
        ] {
            let mut values = complete_values("cluster-a", Uuid::from_bytes([0x22; 16]));
            values[field] = Some(malformed);
            assert_eq!(
                decode_identity_values(&codec(), "cluster-a", &values)
                    .expect_err("malformed identity must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }
    }

    #[test]
    fn existing_identity_requires_exact_cluster_match() {
        let error = decode_identity_values(
            &codec(),
            "cluster-b",
            &complete_values("cluster-a", Uuid::from_bytes([0x22; 16])),
        )
        .expect_err("cluster mismatch must fail");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    }

    #[test]
    fn identity_commit_error_classifier_only_reloads_ambiguous_outcomes() {
        for code in [1020, 1021, 1039] {
            assert_eq!(
                classify_identity_commit_error(FdbError::from_code(code)),
                IdentityCommitErrorDisposition::AuthoritativeReload,
                "error code {code} must be resolved by an authoritative reload"
            );
        }
    }

    #[test]
    fn identity_commit_error_classifier_fails_fast_for_deterministic_errors() {
        for code in [1007, 2006, 6000] {
            assert_eq!(
                classify_identity_commit_error(FdbError::from_code(code)),
                IdentityCommitErrorDisposition::FailFast,
                "error code {code} must fail before observing another actor's identity"
            );
        }
    }
}
