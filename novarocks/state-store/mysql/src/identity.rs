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

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use uuid::{Uuid, Version};

use novarocks_spi::state_store::{StateStoreError, StateStoreErrorKind, StoreIdentity};

use super::codec::MysqlCodec;
use crate::MYSQL_MAX_META_VALUE_BYTES;

pub(super) const SCHEMA_VERSION_KEY: &[u8] = b"schema_version";
pub(super) const SCHEMA_DIGEST_KEY: &[u8] = b"schema_digest";
pub(super) const STORE_ID_KEY: &[u8] = b"store_id";
pub(super) const CLUSTER_ID_KEY: &[u8] = b"cluster_id";
pub(super) const INITIAL_INCARNATION_KEY: &[u8] = b"initial_incarnation";
pub(super) const CURRENT_REVISION_KEY: &[u8] = b"current_revision";
pub(super) const CHANGE_RETENTION_FLOOR_KEY: &[u8] = b"change_retention_floor";

pub(super) const META_KEYS: [&[u8]; 7] = [
    CHANGE_RETENTION_FLOOR_KEY,
    CLUSTER_ID_KEY,
    CURRENT_REVISION_KEY,
    INITIAL_INCARNATION_KEY,
    SCHEMA_DIGEST_KEY,
    SCHEMA_VERSION_KEY,
    STORE_ID_KEY,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MysqlIdentitySnapshot {
    pub identity: StoreIdentity,
    pub current_revision: u64,
    pub change_retention_floor: (u64, u32),
}

pub(super) fn advisory_lock_name(database: &str) -> String {
    let digest = Sha256::digest(database.as_bytes());
    format!("novarocks-ss3-{}", hex::encode(&digest[..24]))
}

pub(super) fn validate_cluster_id(cluster_id: &str) -> Result<(), StateStoreError> {
    if cluster_id.len() > MYSQL_MAX_META_VALUE_BYTES {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL state store configuration is invalid",
        ));
    }
    Ok(())
}

pub(super) fn initial_meta_rows(
    codec: &MysqlCodec,
    cluster_id: &str,
    schema_digest: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let store_id = Uuid::now_v7();
    vec![
        (SCHEMA_VERSION_KEY.to_vec(), 1_u32.to_be_bytes().to_vec()),
        (
            SCHEMA_DIGEST_KEY.to_vec(),
            schema_digest.as_bytes().to_vec(),
        ),
        (STORE_ID_KEY.to_vec(), codec.encode_uuid(store_id).to_vec()),
        (CLUSTER_ID_KEY.to_vec(), cluster_id.as_bytes().to_vec()),
        (
            INITIAL_INCARNATION_KEY.to_vec(),
            1_u64.to_be_bytes().to_vec(),
        ),
        (
            CURRENT_REVISION_KEY.to_vec(),
            codec.encode_revision(0).to_vec(),
        ),
        (
            CHANGE_RETENTION_FLOOR_KEY.to_vec(),
            codec.encode_cursor(0, u32::MAX).to_vec(),
        ),
    ]
}

pub(super) fn decode_meta_rows(
    codec: &MysqlCodec,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    expected_cluster_id: &str,
    expected_schema_digest: &str,
) -> Result<MysqlIdentitySnapshot, StateStoreError> {
    let meta = rows.into_iter().collect::<BTreeMap<_, _>>();
    if meta.len() != META_KEYS.len() || META_KEYS.iter().any(|key| !meta.contains_key::<[u8]>(*key))
    {
        return Err(corruption(
            "MySQL state store meta inventory is incomplete or unexpected",
        ));
    }

    let value = |key: &[u8]| {
        meta.get(key)
            .map(Vec::as_slice)
            .ok_or_else(|| corruption("MySQL state store meta inventory is incomplete"))
    };
    codec.decode_schema_version(value(SCHEMA_VERSION_KEY)?)?;
    let schema_digest = std::str::from_utf8(value(SCHEMA_DIGEST_KEY)?)
        .map_err(|_| corruption("MySQL state store schema digest is malformed"))?;
    if schema_digest != expected_schema_digest {
        return Err(corruption(
            "MySQL state store schema digest is malformed or unsupported",
        ));
    }
    let store_id = codec.decode_uuid(value(STORE_ID_KEY)?)?;
    if store_id.get_version() != Some(Version::SortRand) {
        return Err(corruption("MySQL state store identity is not UUIDv7"));
    }
    let cluster_id = codec.decode_cluster_id(value(CLUSTER_ID_KEY)?)?;
    if cluster_id != expected_cluster_id {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL state store cluster identity does not match configuration",
        ));
    }
    // This experimental provider retains its legacy physical metadata while
    // the public StoreIdentity exposes only store and cluster identity.
    codec.decode_initial_incarnation(value(INITIAL_INCARNATION_KEY)?)?;
    let current_revision = codec.decode_revision(value(CURRENT_REVISION_KEY)?)?;
    let change_retention_floor = codec.decode_cursor(value(CHANGE_RETENTION_FLOOR_KEY)?)?;
    if change_retention_floor != (0, u32::MAX) {
        return Err(corruption(
            "MySQL state store change retention floor is malformed or unsupported",
        ));
    }

    Ok(MysqlIdentitySnapshot {
        identity: StoreIdentity {
            store_id,
            cluster_id,
        },
        current_revision,
        change_retention_floor,
    })
}

fn corruption(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Corruption, message)
}
