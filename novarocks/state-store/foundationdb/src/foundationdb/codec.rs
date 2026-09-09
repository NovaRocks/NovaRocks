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

#[cfg(test)]
use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};

const KEYSPACE_PREFIX: &[u8] = b"NRSS\x01";
const META_TAG: u8 = 0x00;
const RECORD_TAG: u8 = 0x01;
const CHANGE_TAG: u8 = 0x02;
const COMMIT_STATE_TAG: u8 = 0x03;
const SCHEMA_VERSION: u8 = 1;
const RECORD_FORMAT_VERSION: u8 = 1;
const PENDING_TAG: u8 = 0x01;
const COMMITTED_TAG: u8 = 0x02;
const NOT_COMMITTED_TAG: u8 = 0x03;
pub(super) const REVISION_BYTES: usize = 10;
const KEYSPACE_HASH_HEX_BYTES: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DurableCommitState {
    Pending([u8; 16]),
    Committed([u8; REVISION_BYTES]),
    NotCommitted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DecodedRecordValue {
    pub transaction_id: [u8; 16],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct KeyspaceCodec {
    root: Vec<u8>,
}

impl KeyspaceCodec {
    pub fn new(keyspace_id: Uuid) -> Self {
        Self {
            root: [KEYSPACE_PREFIX, keyspace_id.as_bytes()].concat(),
        }
    }

    pub fn root(&self) -> &[u8] {
        &self.root
    }

    pub fn keyspace_hash(&self) -> String {
        keyspace_hash(self.root())
    }

    fn meta_key(&self, field: u8) -> Vec<u8> {
        [self.root(), &[META_TAG, field]].concat()
    }

    pub fn schema_version_key(&self) -> Vec<u8> {
        self.meta_key(0x00)
    }

    pub fn cluster_id_key(&self) -> Vec<u8> {
        self.meta_key(0x01)
    }

    pub fn store_id_key(&self) -> Vec<u8> {
        self.meta_key(0x02)
    }

    pub fn initial_incarnation_key(&self) -> Vec<u8> {
        self.meta_key(0x03)
    }

    pub fn high_watermark_key(&self) -> Vec<u8> {
        self.meta_key(0x04)
    }

    pub fn retention_floor_key(&self) -> Vec<u8> {
        self.meta_key(0x05)
    }

    pub fn record_key(&self, logical_key: &[u8]) -> Vec<u8> {
        [self.root(), &[RECORD_TAG], logical_key].concat()
    }

    pub fn change_key(&self, revision: &[u8], sequence: u32) -> Result<Vec<u8>, StateStoreError> {
        let revision = self.decode_revision(revision)?;
        Ok([
            self.root(),
            &[CHANGE_TAG],
            &revision,
            &sequence.to_be_bytes(),
        ]
        .concat())
    }

    pub fn decode_change_key(
        &self,
        key: &[u8],
    ) -> Result<([u8; REVISION_BYTES], u32), StateStoreError> {
        let expected_len = self.root.len() + 1 + REVISION_BYTES + 4;
        if key.len() != expected_len
            || !key.starts_with(self.root())
            || key[self.root.len()] != CHANGE_TAG
        {
            return Err(corruption("FoundationDB change key is malformed"));
        }
        let revision_start = self.root.len() + 1;
        let revision = copy_array::<REVISION_BYTES>(
            &key[revision_start..revision_start + REVISION_BYTES],
            "FoundationDB change revision is malformed",
        )?;
        let sequence = u32::from_be_bytes(copy_array::<4>(
            &key[revision_start + REVISION_BYTES..],
            "FoundationDB change sequence is malformed",
        )?);
        Ok((revision, sequence))
    }

    pub fn change_key_operand(&self, sequence: u32) -> Vec<u8> {
        [
            self.root(),
            &[CHANGE_TAG],
            &[0xff; REVISION_BYTES],
            &sequence.to_be_bytes(),
            &((self.root.len() + 1) as u32).to_le_bytes(),
        ]
        .concat()
    }

    pub fn commit_state_key(&self, transaction_id: [u8; 16]) -> Vec<u8> {
        [self.root(), &[COMMIT_STATE_TAG], &transaction_id].concat()
    }

    pub fn schema_version_value(&self) -> Vec<u8> {
        vec![SCHEMA_VERSION]
    }

    pub fn decode_schema_version(&self, value: &[u8]) -> Result<u8, StateStoreError> {
        match value {
            [SCHEMA_VERSION] => Ok(SCHEMA_VERSION),
            _ => Err(corruption(
                "FoundationDB state store schema version is malformed or unsupported",
            )),
        }
    }

    pub fn cluster_id_value(&self, cluster_id: &str) -> Vec<u8> {
        cluster_id.as_bytes().to_vec()
    }

    pub fn decode_cluster_id(&self, value: &[u8]) -> Result<String, StateStoreError> {
        let cluster_id = std::str::from_utf8(value)
            .map_err(|_| corruption("FoundationDB state store cluster identity is malformed"))?;
        if cluster_id.is_empty() {
            return Err(corruption(
                "FoundationDB state store cluster identity is malformed",
            ));
        }
        Ok(cluster_id.to_owned())
    }

    pub fn store_id_value(&self, store_id: Uuid) -> Vec<u8> {
        store_id.as_bytes().to_vec()
    }

    pub fn decode_store_id(&self, value: &[u8]) -> Result<Uuid, StateStoreError> {
        Ok(Uuid::from_bytes(copy_array::<16>(
            value,
            "FoundationDB state store identity is malformed",
        )?))
    }

    pub fn initial_incarnation_value(&self) -> Vec<u8> {
        1_u64.to_be_bytes().to_vec()
    }

    pub fn decode_initial_incarnation(&self, value: &[u8]) -> Result<u64, StateStoreError> {
        let incarnation = u64::from_be_bytes(copy_array::<8>(
            value,
            "FoundationDB state store incarnation is malformed",
        )?);
        if incarnation != 1 {
            return Err(corruption(
                "FoundationDB state store incarnation is unsupported",
            ));
        }
        Ok(incarnation)
    }

    pub fn zero_revision_value(&self) -> Vec<u8> {
        vec![0; REVISION_BYTES]
    }

    pub fn decode_revision(&self, value: &[u8]) -> Result<[u8; REVISION_BYTES], StateStoreError> {
        copy_array::<REVISION_BYTES>(value, "FoundationDB state store revision is malformed")
    }

    pub fn record_value(&self, transaction_id: [u8; 16], payload: &[u8]) -> Vec<u8> {
        [&[RECORD_FORMAT_VERSION][..], &transaction_id, payload].concat()
    }

    pub fn decode_record_value(&self, value: &[u8]) -> Result<DecodedRecordValue, StateStoreError> {
        if value.len() < 17 || value[0] != RECORD_FORMAT_VERSION {
            return Err(corruption("FoundationDB state record is malformed"));
        }
        Ok(DecodedRecordValue {
            transaction_id: copy_array::<16>(
                &value[1..17],
                "FoundationDB state record transaction id is malformed",
            )?,
            payload: value[17..].to_vec(),
        })
    }

    pub fn pending_value(&self, reservation_token: [u8; 16]) -> Vec<u8> {
        [&[PENDING_TAG][..], &reservation_token].concat()
    }

    pub fn not_committed_value(&self) -> Vec<u8> {
        vec![NOT_COMMITTED_TAG]
    }

    pub fn decode_commit_state(&self, value: &[u8]) -> Result<DurableCommitState, StateStoreError> {
        match value {
            [PENDING_TAG, token @ ..] if token.len() == 16 => Ok(DurableCommitState::Pending(
                copy_array::<16>(token, "FoundationDB pending commit state is malformed")?,
            )),
            [COMMITTED_TAG, revision @ ..] if revision.len() == REVISION_BYTES => {
                Ok(DurableCommitState::Committed(copy_array::<REVISION_BYTES>(
                    revision,
                    "FoundationDB committed state is malformed",
                )?))
            }
            [NOT_COMMITTED_TAG] => Ok(DurableCommitState::NotCommitted),
            _ => Err(corruption("FoundationDB commit state is malformed")),
        }
    }

    pub fn high_watermark_operand(&self) -> Vec<u8> {
        [[0xff; REVISION_BYTES].as_slice(), &0_u32.to_le_bytes()].concat()
    }

    pub fn committed_value_operand(&self) -> Vec<u8> {
        [
            &[COMMITTED_TAG][..],
            &[0xff; REVISION_BYTES],
            &1_u32.to_le_bytes(),
        ]
        .concat()
    }

    #[cfg(test)]
    pub fn assign_change_sequences(
        changed_keys: impl IntoIterator<Item = Vec<u8>>,
    ) -> Vec<(Vec<u8>, u32)> {
        changed_keys
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(sequence, key)| {
                (
                    key,
                    u32::try_from(sequence)
                        .expect("state store operation limits keep change sequence in u32"),
                )
            })
            .collect()
    }
}

fn keyspace_hash(root: &[u8]) -> String {
    let digest = Sha256::digest(root);
    hex::encode(&digest[..KEYSPACE_HASH_HEX_BYTES])
}

#[cfg(test)]
mod observability_tests {
    use super::*;

    #[test]
    fn keyspace_hash_is_stable_and_does_not_expose_the_uuid() {
        let keyspace_id = Uuid::parse_str("22db595e-3031-48eb-8212-f56d3626ee41").unwrap();
        let codec = KeyspaceCodec::new(keyspace_id);
        let hash = codec.keyspace_hash();

        assert_eq!(hash.len(), 16);
        assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!hash.contains("22db595e"));
        assert_eq!(hash, KeyspaceCodec::new(keyspace_id).keyspace_hash());
    }
}

fn copy_array<const N: usize>(
    value: &[u8],
    message: &'static str,
) -> Result<[u8; N], StateStoreError> {
    value.try_into().map_err(|_| corruption(message))
}

fn corruption(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Corruption, message)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{DurableCommitState, KeyspaceCodec};
    use novarocks_state_store_api::StateStoreErrorKind;

    fn codec() -> KeyspaceCodec {
        KeyspaceCodec::new(Uuid::from_bytes([0x11; 16]))
    }

    #[test]
    fn physical_keys_are_byte_exact() {
        let codec = codec();
        let expected_root = [b"NRSS\x01".as_slice(), &[0x11; 16]].concat();
        assert_eq!(codec.root(), expected_root);
        assert_eq!(
            codec.schema_version_key(),
            [codec.root(), &[0x00, 0x00]].concat()
        );
        assert_eq!(
            codec.cluster_id_key(),
            [codec.root(), &[0x00, 0x01]].concat()
        );
        assert_eq!(codec.store_id_key(), [codec.root(), &[0x00, 0x02]].concat());
        assert_eq!(
            codec.initial_incarnation_key(),
            [codec.root(), &[0x00, 0x03]].concat()
        );
        assert_eq!(
            codec.high_watermark_key(),
            [codec.root(), &[0x00, 0x04]].concat()
        );
        assert_eq!(
            codec.retention_floor_key(),
            [codec.root(), &[0x00, 0x05]].concat()
        );
        assert_eq!(
            codec.record_key(b"a\0\xff"),
            [codec.root(), &[0x01], b"a\0\xff"].concat()
        );
        assert_eq!(
            codec.commit_state_key([0x22; 16]),
            [codec.root(), &[0x03], &[0x22; 16]].concat()
        );
    }

    #[test]
    fn versionstamp_operands_are_byte_exact() {
        let codec = codec();
        let change = codec.change_key_operand(0x0102_0304);
        assert_eq!(
            change,
            [
                codec.root(),
                &[0x02],
                &[0xff; 10],
                &0x0102_0304_u32.to_be_bytes(),
                &((codec.root().len() + 1) as u32).to_le_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            codec.high_watermark_operand(),
            [[0xff; 10].as_slice(), &0_u32.to_le_bytes()].concat()
        );
        assert_eq!(
            codec.committed_value_operand(),
            [&[0x02][..], &[0xff; 10], &1_u32.to_le_bytes(),].concat()
        );
    }

    #[test]
    fn record_and_commit_values_are_byte_exact() {
        let codec = codec();
        assert_eq!(
            codec.record_value([0x33; 16], b"\0payload\xff"),
            [&[0x01][..], &[0x33; 16], b"\0payload\xff"].concat()
        );
        let decoded = codec
            .decode_record_value(&codec.record_value([0x33; 16], b"\0payload\xff"))
            .expect("decode record envelope");
        assert_eq!(decoded.transaction_id, [0x33; 16]);
        assert_eq!(decoded.payload, b"\0payload\xff");

        assert_eq!(
            codec.pending_value([7; 16]),
            [&[0x01][..], &[7; 16]].concat()
        );
        assert_eq!(codec.not_committed_value(), vec![0x03]);
        assert_eq!(
            codec
                .decode_commit_state(&codec.pending_value([7; 16]))
                .expect("decode pending"),
            DurableCommitState::Pending([7; 16])
        );
        assert_eq!(
            codec
                .decode_commit_state(&[&[0x02][..], &[9; 10]].concat())
                .expect("decode committed"),
            DurableCommitState::Committed([9; 10])
        );
        assert_eq!(
            codec
                .decode_commit_state(&codec.not_committed_value())
                .expect("decode not committed"),
            DurableCommitState::NotCommitted
        );
    }

    #[test]
    fn exact_decoders_reject_malformed_unknown_and_trailing_bytes() {
        let codec = codec();
        for malformed in [
            vec![],
            vec![0x00],
            vec![0x01],
            [&[0x01][..], &[0; 15]].concat(),
            [&[0x02][..], &[0; 9]].concat(),
            vec![0x03, 0x00],
            vec![0xff],
        ] {
            assert_eq!(
                codec
                    .decode_commit_state(&malformed)
                    .expect_err("malformed commit state must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }

        for malformed in [
            vec![],
            vec![0x00],
            [&[0x01][..], &[0; 15]].concat(),
            [&[0x02][..], &[0; 16]].concat(),
        ] {
            assert_eq!(
                codec
                    .decode_record_value(&malformed)
                    .expect_err("malformed record envelope must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }

        for malformed in [vec![], vec![0], vec![2], vec![1, 0]] {
            assert_eq!(
                codec
                    .decode_schema_version(&malformed)
                    .expect_err("unknown or malformed schema must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }
        for malformed in [vec![0; 9], vec![0; 11]] {
            assert_eq!(
                codec
                    .decode_revision(&malformed)
                    .expect_err("revision length must be exact")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }
    }

    #[test]
    fn change_keys_round_trip_only_exact_revision_and_sequence() {
        let codec = codec();
        let key = codec
            .change_key(&[0x44; 10], 0x0102_0304)
            .expect("encode change key");
        assert_eq!(
            key,
            [
                codec.root(),
                &[0x02],
                &[0x44; 10],
                &0x0102_0304_u32.to_be_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            codec.decode_change_key(&key).expect("decode change key"),
            ([0x44; 10], 0x0102_0304)
        );

        for malformed in [
            key[..key.len() - 1].to_vec(),
            [key.as_slice(), &[0]].concat(),
            [&[0][..], &key[1..]].concat(),
        ] {
            assert_eq!(
                codec
                    .decode_change_key(&malformed)
                    .expect_err("malformed change key must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }
        assert_eq!(
            codec
                .change_key(&[0; 9], 0)
                .expect_err("short revision must fail")
                .kind(),
            StateStoreErrorKind::Corruption
        );
    }

    #[test]
    fn change_sequences_are_unique_and_bytewise_sorted() {
        let sequenced = KeyspaceCodec::assign_change_sequences([
            b"z".to_vec(),
            b"a".to_vec(),
            b"z".to_vec(),
            b"\0".to_vec(),
        ]);
        assert_eq!(
            sequenced,
            vec![(b"\0".to_vec(), 0), (b"a".to_vec(), 1), (b"z".to_vec(), 2),]
        );
    }
}
