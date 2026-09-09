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
use sha2::{Digest, Sha256};
use uuid::Uuid;

use novarocks_state_store_api::{Key, StateStoreError, StateStoreErrorKind};

use crate::MYSQL_MAX_KEY_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DurableCommitState {
    Pending([u8; 16]),
    Committed(u64),
    NotCommitted,
}

#[derive(Clone, Debug)]
pub(super) struct MysqlCodec {
    max_key_bytes: usize,
}

impl MysqlCodec {
    pub(super) fn new(max_key_bytes: usize) -> Result<Self, StateStoreError> {
        if max_key_bytes == 0 || max_key_bytes > MYSQL_MAX_KEY_BYTES {
            return Err(corruption());
        }
        Ok(Self { max_key_bytes })
    }

    pub(super) fn encode_uuid(&self, _uuid: Uuid) -> [u8; 16] {
        *_uuid.as_bytes()
    }

    pub(super) fn decode_uuid(&self, bytes: &[u8]) -> Result<Uuid, StateStoreError> {
        Ok(Uuid::from_bytes(copy_array(bytes)?))
    }

    pub(super) const fn encode_revision(&self, revision: u64) -> [u8; 8] {
        revision.to_be_bytes()
    }

    pub(super) fn decode_revision(&self, bytes: &[u8]) -> Result<u64, StateStoreError> {
        Ok(u64::from_be_bytes(copy_array(bytes)?))
    }

    pub(super) fn encode_version(&self, revision: u64, sequence: u32) -> [u8; 12] {
        encode_revision_sequence(revision, sequence)
    }

    pub(super) fn decode_version(&self, bytes: &[u8]) -> Result<(u64, u32), StateStoreError> {
        decode_revision_sequence(bytes)
    }

    pub(super) fn encode_cursor(&self, revision: u64, sequence: u32) -> [u8; 12] {
        encode_revision_sequence(revision, sequence)
    }

    pub(super) fn decode_cursor(&self, bytes: &[u8]) -> Result<(u64, u32), StateStoreError> {
        decode_revision_sequence(bytes)
    }

    pub(super) fn decode_commit_state(
        &self,
        state: u8,
        reservation_token: Option<&[u8]>,
        revision: Option<u64>,
    ) -> Result<DurableCommitState, StateStoreError> {
        match (state, reservation_token, revision) {
            (1, Some(token), None) => Ok(DurableCommitState::Pending(copy_array(token)?)),
            (2, None, Some(revision)) => Ok(DurableCommitState::Committed(revision)),
            (3, None, None) => Ok(DurableCommitState::NotCommitted),
            _ => Err(corruption()),
        }
    }

    pub(super) fn decode_schema_version(&self, bytes: &[u8]) -> Result<u32, StateStoreError> {
        let version = u32::from_be_bytes(copy_array(bytes)?);
        if version != 1 {
            return Err(corruption());
        }
        Ok(version)
    }

    pub(super) fn decode_initial_incarnation(&self, bytes: &[u8]) -> Result<u64, StateStoreError> {
        let incarnation = self.decode_revision(bytes)?;
        if incarnation != 1 {
            return Err(corruption());
        }
        Ok(incarnation)
    }

    pub(super) fn checked_next_revision(&self, revision: u64) -> Result<u64, StateStoreError> {
        revision.checked_add(1).ok_or_else(corruption)
    }

    pub(super) fn decode_cluster_id(&self, bytes: &[u8]) -> Result<String, StateStoreError> {
        let cluster_id = std::str::from_utf8(bytes).map_err(|_| corruption())?;
        if cluster_id.is_empty() {
            return Err(corruption());
        }
        Ok(cluster_id.to_owned())
    }

    pub(super) fn decode_persisted_key(&self, bytes: &[u8]) -> Result<Key, StateStoreError> {
        if bytes.len() > self.max_key_bytes {
            return Err(corruption());
        }
        Key::try_from(Bytes::copy_from_slice(bytes)).map_err(|_| corruption())
    }
}

pub(super) fn redacted_identity_hash(identity: &[u8]) -> String {
    let digest = Sha256::digest(identity);
    hex::encode(&digest[..8])
}

fn encode_revision_sequence(revision: u64, sequence: u32) -> [u8; 12] {
    let mut encoded = [0; 12];
    encoded[..8].copy_from_slice(&revision.to_be_bytes());
    encoded[8..].copy_from_slice(&sequence.to_be_bytes());
    encoded
}

fn decode_revision_sequence(bytes: &[u8]) -> Result<(u64, u32), StateStoreError> {
    let bytes: [u8; 12] = copy_array(bytes)?;
    let revision = u64::from_be_bytes(
        bytes[..8]
            .try_into()
            .expect("fixed revision prefix has exact length"),
    );
    let sequence = u32::from_be_bytes(
        bytes[8..]
            .try_into()
            .expect("fixed sequence suffix has exact length"),
    );
    Ok((revision, sequence))
}

fn copy_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], StateStoreError> {
    bytes.try_into().map_err(|_| corruption())
}

const fn corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL state store persisted state is malformed or unsupported",
    )
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{DurableCommitState, MysqlCodec, redacted_identity_hash};
    use novarocks_state_store_api::StateStoreErrorKind;

    fn assert_corruption<T: std::fmt::Debug>(
        result: Result<T, novarocks_state_store_api::StateStoreError>,
    ) {
        assert_eq!(
            result
                .expect_err("malformed persisted state must fail closed")
                .kind(),
            StateStoreErrorKind::Corruption
        );
    }

    #[test]
    fn mysql_codec_round_trips_uuid_revision_version_and_cursor() {
        let codec = MysqlCodec::new(3072).expect("codec");
        let uuid = Uuid::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba,
            0xdc, 0xfe,
        ]);

        let uuid_bytes = codec.encode_uuid(uuid);
        assert_eq!(uuid_bytes, *uuid.as_bytes());
        assert_eq!(codec.decode_uuid(&uuid_bytes).expect("UUID"), uuid);

        let revision = 0x0102_0304_0506_0708_u64;
        let revision_bytes = codec.encode_revision(revision);
        assert_eq!(revision_bytes, revision.to_be_bytes());
        assert_eq!(
            codec.decode_revision(&revision_bytes).expect("revision"),
            revision
        );

        let version = codec.encode_version(revision, 0x1122_3344);
        assert_eq!(
            version,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x22, 0x33, 0x44
            ]
        );
        assert_eq!(
            codec.decode_version(&version).expect("version"),
            (revision, 0x1122_3344)
        );

        let cursor = codec.encode_cursor(revision, u32::MAX);
        assert_eq!(
            codec.decode_cursor(&cursor).expect("cursor"),
            (revision, u32::MAX)
        );
    }

    #[test]
    fn mysql_codec_rejects_illegal_commit_state_combinations() {
        let codec = MysqlCodec::new(3072).expect("codec");
        let token = [0x5a; 16];

        assert_eq!(
            codec
                .decode_commit_state(1, Some(&token), None)
                .expect("pending"),
            DurableCommitState::Pending(token)
        );
        assert_eq!(
            codec
                .decode_commit_state(2, None, Some(7))
                .expect("committed"),
            DurableCommitState::Committed(7)
        );
        assert_eq!(
            codec
                .decode_commit_state(3, None, None)
                .expect("not committed"),
            DurableCommitState::NotCommitted
        );

        for result in [
            codec.decode_commit_state(0, None, None),
            codec.decode_commit_state(1, None, None),
            codec.decode_commit_state(1, Some(&token), Some(1)),
            codec.decode_commit_state(2, Some(&token), Some(1)),
            codec.decode_commit_state(2, None, None),
            codec.decode_commit_state(3, Some(&token), None),
            codec.decode_commit_state(3, None, Some(1)),
            codec.decode_commit_state(4, None, None),
        ] {
            assert_corruption(result);
        }
        assert_corruption(codec.decode_commit_state(1, Some(&token[..15]), None));
    }

    #[test]
    fn mysql_codec_rejects_overflow_unknown_trailing_and_malformed_fields() {
        let codec = MysqlCodec::new(3072).expect("codec");

        assert_eq!(
            codec
                .decode_schema_version(&1_u32.to_be_bytes())
                .expect("schema version"),
            1
        );
        assert_eq!(
            codec
                .decode_initial_incarnation(&1_u64.to_be_bytes())
                .expect("initial incarnation"),
            1
        );
        assert_eq!(codec.checked_next_revision(41).expect("next revision"), 42);
        assert_eq!(
            codec.decode_cluster_id(b"cluster-a").expect("cluster id"),
            "cluster-a"
        );

        assert_corruption(codec.decode_uuid(&[0; 15]));
        assert_corruption(codec.decode_revision(&[0; 7]));
        assert_corruption(codec.decode_revision(&[0; 9]));
        assert_corruption(codec.decode_version(&[0; 11]));
        assert_corruption(codec.decode_version(&[0; 13]));
        assert_corruption(codec.decode_cursor(&[0; 11]));
        assert_corruption(codec.decode_cursor(&[0; 13]));
        assert_corruption(codec.decode_schema_version(&[0, 0, 0, 0]));
        assert_corruption(codec.decode_schema_version(&[0, 0, 0, 2]));
        assert_corruption(codec.decode_schema_version(&[0, 0, 0, 1, 0]));
        assert_corruption(codec.decode_initial_incarnation(&0_u64.to_be_bytes()));
        assert_corruption(codec.decode_initial_incarnation(&2_u64.to_be_bytes()));
        assert_corruption(codec.checked_next_revision(u64::MAX));
        assert_corruption(codec.decode_cluster_id(b""));
        assert_corruption(codec.decode_cluster_id(&[0xff]));
    }

    #[test]
    fn mysql_codec_rejects_persisted_keys_beyond_effective_limit() {
        let codec = MysqlCodec::new(4).expect("codec");

        assert_eq!(
            codec
                .decode_persisted_key(&[0x00, 0x7f, 0x80, 0xff])
                .expect("key at limit")
                .as_bytes(),
            &[0x00, 0x7f, 0x80, 0xff]
        );
        assert_corruption(codec.decode_persisted_key(&[0; 5]));
        assert_corruption(MysqlCodec::new(0));
        assert_corruption(MysqlCodec::new(3073));
    }

    #[test]
    fn mysql_identity_hashes_do_not_expose_database_or_cluster() {
        let database = "novarocks_control_plane_sensitive";
        let cluster = "production-cluster-sensitive";
        let database_hash = redacted_identity_hash(database.as_bytes());
        let cluster_hash = redacted_identity_hash(cluster.as_bytes());

        for (raw, hash) in [(database, database_hash), (cluster, cluster_hash)] {
            assert_eq!(hash.len(), 16);
            assert!(
                hash.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            assert!(!hash.contains(raw));
            assert_eq!(hash, redacted_identity_hash(raw.as_bytes()));
        }
    }
}
