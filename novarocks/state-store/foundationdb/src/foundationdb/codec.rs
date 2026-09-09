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

use sha2::{Digest, Sha256};
use uuid::Uuid;

use novarocks_state_store_api::{AttemptId, StateStoreError, StateStoreErrorKind};

const KEYSPACE_PREFIX: &[u8] = b"NRSS\x01";
const META_TAG: u8 = 0x00;
const RECORD_TAG: u8 = 0x01;
// 0x02 was the change-feed key tag. The change feed is gone and nothing writes
// that prefix any more; it is deliberately not reused, so an operator who
// inspects a keyspace built by an older schema version can still tell the
// orphaned rows apart. See `SCHEMA_VERSION`.
const COMMIT_STATE_TAG: u8 = 0x03;
/// Physical layout version of one keyspace.
///
/// Version 1 carried a change feed, a high watermark, a retention floor, and a
/// three-state commit-state value keyed by a caller-supplied transaction UUID.
/// None of those exist any more, and FoundationDB has no DDL that could migrate
/// them, so a version-1 keyspace is refused at open rather than reinterpreted.
const SCHEMA_VERSION: u8 = 2;
const RECORD_FORMAT_VERSION: u8 = 1;
const COMMITTED_TAG: u8 = 0x02;
pub(super) const REVISION_BYTES: usize = 10;
/// Physical width of one write attempt's identity: the opening instance's tag
/// followed by the attempt sequence.
pub(super) const ATTEMPT_TAG_BYTES: usize = 16 + 8;
const KEYSPACE_HASH_HEX_BYTES: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DecodedRecordValue {
    pub attempt_tag: [u8; ATTEMPT_TAG_BYTES],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct KeyspaceCodec {
    root: Vec<u8>,
    instance_tag: Uuid,
}

impl KeyspaceCodec {
    /// Builds the codec for one opened instance of one keyspace.
    ///
    /// `instance_tag` separates this open from every other open of the same
    /// keyspace. It exists because an [`AttemptId`]'s scope is deliberately not
    /// convertible to bytes: a store refuses any attempt another instance
    /// issued, so within one codec an attempt is fully addressed by its
    /// sequence, and this tag is what keeps two live instances from writing to
    /// each other's commit-state keys.
    pub fn new(keyspace_id: Uuid, instance_tag: Uuid) -> Self {
        Self {
            root: [KEYSPACE_PREFIX, keyspace_id.as_bytes()].concat(),
            instance_tag,
        }
    }

    pub fn root(&self) -> &[u8] {
        &self.root
    }

    pub fn keyspace_hash(&self) -> String {
        keyspace_hash(self.root())
    }

    /// Encodes one attempt as the fixed-width tag this keyspace addresses it by.
    ///
    /// Deliberately not the contract's `storage_key`, which SQLite and MySQL
    /// both use. A keyspace is ordered by raw bytes, so a big-endian sequence
    /// after a 16-byte instance tag makes one open's attempts a contiguous,
    /// ascending range that a single range read can scan or clear. The decimal
    /// text form would sort the same attempts lexicographically and cost more
    /// than twice the bytes on every key.
    pub fn attempt_tag(&self, attempt: AttemptId) -> [u8; ATTEMPT_TAG_BYTES] {
        let mut tag = [0_u8; ATTEMPT_TAG_BYTES];
        tag[..16].copy_from_slice(self.instance_tag.as_bytes());
        tag[16..].copy_from_slice(&attempt.sequence().to_be_bytes());
        tag
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

    pub fn record_key(&self, logical_key: &[u8]) -> Vec<u8> {
        [self.root(), &[RECORD_TAG], logical_key].concat()
    }

    pub fn commit_state_key(&self, attempt: AttemptId) -> Vec<u8> {
        [self.root(), &[COMMIT_STATE_TAG], &self.attempt_tag(attempt)].concat()
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

    pub fn record_value(&self, attempt_tag: [u8; ATTEMPT_TAG_BYTES], payload: &[u8]) -> Vec<u8> {
        [&[RECORD_FORMAT_VERSION][..], &attempt_tag, payload].concat()
    }

    pub fn decode_record_value(&self, value: &[u8]) -> Result<DecodedRecordValue, StateStoreError> {
        const HEADER: usize = 1 + ATTEMPT_TAG_BYTES;
        if value.len() < HEADER || value[0] != RECORD_FORMAT_VERSION {
            return Err(corruption("FoundationDB state record is malformed"));
        }
        Ok(DecodedRecordValue {
            attempt_tag: copy_array::<ATTEMPT_TAG_BYTES>(
                &value[1..HEADER],
                "FoundationDB state record attempt tag is malformed",
            )?,
            payload: value[HEADER..].to_vec(),
        })
    }

    /// Decodes the only value a commit-state key can carry.
    ///
    /// The key is written exactly once, by the data transaction itself, through
    /// a versionstamped mutation. Its presence therefore *is* the proof that
    /// the transaction committed, and its value is that commit's revision.
    /// Nothing writes a pending marker or a tombstone: an absent key proves
    /// nothing at all, which is why it is not representable here.
    pub fn decode_committed_revision(
        &self,
        value: &[u8],
    ) -> Result<[u8; REVISION_BYTES], StateStoreError> {
        match value {
            [COMMITTED_TAG, revision @ ..] if revision.len() == REVISION_BYTES => {
                copy_array::<REVISION_BYTES>(revision, "FoundationDB committed state is malformed")
            }
            _ => Err(corruption("FoundationDB commit state is malformed")),
        }
    }

    pub fn committed_value_operand(&self) -> Vec<u8> {
        [
            &[COMMITTED_TAG][..],
            &[0xff; REVISION_BYTES],
            &1_u32.to_le_bytes(),
        ]
        .concat()
    }
}

fn keyspace_hash(root: &[u8]) -> String {
    let digest = Sha256::digest(root);
    hex::encode(&digest[..KEYSPACE_HASH_HEX_BYTES])
}

/// Mints attempt identities for this crate's unit tests.
///
/// An [`AttemptId`] cannot be built from bytes, so a test that needs one has to
/// obtain it the way the provider does: from a supervisor. The supervisor here
/// is throwaway and its adjudicator is never driven, because these tests only
/// ever read an identity's rendering.
#[cfg(test)]
pub(crate) mod tests_support {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use novarocks_state_store_api::{
        AttemptId, AttemptOutcome, AttemptSupervisor, InDoubtAdjudicator, StateStoreError,
    };

    struct UnusedAdjudicator;

    #[async_trait::async_trait]
    impl InDoubtAdjudicator for UnusedAdjudicator {
        async fn adjudicate(&self, _: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
            unreachable!("test-support attempts are never adjudicated")
        }

        async fn release_evidence(&self, _: AttemptId) -> Result<(), StateStoreError> {
            unreachable!("test-support attempts never release evidence")
        }
    }

    /// Returns the identity of the `sequence`-th attempt of a fresh instance.
    ///
    /// Sequences are issued in order and cannot be chosen, so reaching a given
    /// one means reserving up to it. Callers pass small numbers.
    pub(crate) fn attempt_id(sequence: u64) -> AttemptId {
        assert!(sequence >= 1, "attempt sequences start at one");
        let supervisor = AttemptSupervisor::new(
            NonZeroUsize::new(64).expect("test-support capacity"),
            Arc::new(UnusedAdjudicator),
        );
        let mut held = Vec::new();
        for _ in 0..sequence {
            held.push(
                supervisor
                    .reserve()
                    .expect("reserve a test-support attempt"),
            );
        }
        held.last().expect("one reserved attempt").0.id()
    }
}

#[cfg(test)]
mod observability_tests {
    use super::*;

    #[test]
    fn keyspace_hash_is_stable_and_does_not_expose_the_uuid() {
        let keyspace_id = Uuid::parse_str("22db595e-3031-48eb-8212-f56d3626ee41").unwrap();
        let codec = KeyspaceCodec::new(keyspace_id, Uuid::from_bytes([0x44; 16]));
        let hash = codec.keyspace_hash();

        assert_eq!(hash.len(), 16);
        assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!hash.contains("22db595e"));
        // The hash names the keyspace, not the open: a second instance of the
        // same keyspace must remain recognisable in logs.
        assert_eq!(
            hash,
            KeyspaceCodec::new(keyspace_id, Uuid::from_bytes([0x55; 16])).keyspace_hash()
        );
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

    use super::KeyspaceCodec;
    use super::tests_support::attempt_id;
    use novarocks_state_store_api::StateStoreErrorKind;

    fn codec() -> KeyspaceCodec {
        KeyspaceCodec::new(Uuid::from_bytes([0x11; 16]), Uuid::from_bytes([0x22; 16]))
    }

    #[test]
    fn physical_keys_are_byte_exact() {
        let codec = codec();
        let first = attempt_id(1);
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
            codec.record_key(b"a\0\xff"),
            [codec.root(), &[0x01], b"a\0\xff"].concat()
        );
        assert_eq!(
            codec.commit_state_key(first),
            [
                codec.root(),
                &[0x03],
                &[0x22; 16],
                &first.sequence().to_be_bytes(),
            ]
            .concat()
        );
    }

    #[test]
    fn commit_state_keys_separate_attempts_and_separate_instances() {
        let first = attempt_id(1);
        let second = attempt_id(2);
        let keyspace_id = Uuid::from_bytes([0x11; 16]);
        let instance = KeyspaceCodec::new(keyspace_id, Uuid::from_bytes([0x22; 16]));
        let other_instance = KeyspaceCodec::new(keyspace_id, Uuid::from_bytes([0x33; 16]));

        assert_ne!(
            instance.commit_state_key(first),
            instance.commit_state_key(second),
            "two attempts of one instance never share evidence"
        );
        assert_ne!(
            instance.commit_state_key(first),
            other_instance.commit_state_key(first),
            "two opens of one keyspace never share evidence, even at the same sequence"
        );
        assert_eq!(
            instance.commit_state_key(first),
            instance.commit_state_key(first),
            "one attempt always addresses the same evidence"
        );
    }

    #[test]
    fn versionstamp_operands_are_byte_exact() {
        let codec = codec();
        assert_eq!(
            codec.committed_value_operand(),
            [&[0x02][..], &[0xff; 10], &1_u32.to_le_bytes(),].concat()
        );
    }

    #[test]
    fn record_and_commit_values_are_byte_exact() {
        let codec = codec();
        let attempt = attempt_id(1);
        let tag = codec.attempt_tag(attempt);
        assert_eq!(
            tag.as_slice(),
            [&[0x22; 16][..], &attempt.sequence().to_be_bytes()].concat()
        );

        assert_eq!(
            codec.record_value(tag, b"\0payload\xff"),
            [&[0x01][..], &tag, b"\0payload\xff"].concat()
        );
        let decoded = codec
            .decode_record_value(&codec.record_value(tag, b"\0payload\xff"))
            .expect("decode record envelope");
        assert_eq!(decoded.attempt_tag, tag);
        assert_eq!(decoded.payload, b"\0payload\xff");

        assert_eq!(
            codec
                .decode_committed_revision(&[&[0x02][..], &[9; 10]].concat())
                .expect("decode committed"),
            [9; 10]
        );
    }

    #[test]
    fn exact_decoders_reject_malformed_unknown_and_trailing_bytes() {
        let codec = codec();
        for malformed in [
            vec![],
            vec![0x00],
            vec![0x02],
            [&[0x02][..], &[0; 9]].concat(),
            [&[0x02][..], &[0; 11]].concat(),
            // The retired pending and tombstone encodings are not commit proof
            // and must not decode as one.
            [&[0x01][..], &[0; 16]].concat(),
            vec![0x03],
            vec![0xff],
        ] {
            assert_eq!(
                codec
                    .decode_committed_revision(&malformed)
                    .expect_err("malformed commit state must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }

        for malformed in [
            vec![],
            vec![0x00],
            [&[0x01][..], &[0; 23]].concat(),
            [&[0x02][..], &[0; 24]].concat(),
        ] {
            assert_eq!(
                codec
                    .decode_record_value(&malformed)
                    .expect_err("malformed record envelope must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }

        for malformed in [vec![], vec![0x00], vec![0x01], vec![0x03], vec![0x02, 0x00]] {
            assert_eq!(
                codec
                    .decode_schema_version(&malformed)
                    .expect_err("unknown or malformed schema must fail")
                    .kind(),
                StateStoreErrorKind::Corruption
            );
        }
    }

    #[test]
    fn the_current_schema_version_is_the_only_accepted_one() {
        let codec = codec();
        assert_eq!(codec.schema_version_value(), vec![0x02]);
        assert_eq!(
            codec
                .decode_schema_version(&codec.schema_version_value())
                .expect("current schema version"),
            2
        );
        // A keyspace written by the change-feed schema is refused rather than
        // reinterpreted: FoundationDB has no DDL that could migrate it.
        assert_eq!(
            codec
                .decode_schema_version(&[1])
                .expect_err("a version-1 keyspace must be refused")
                .kind(),
            StateStoreErrorKind::Corruption
        );
    }
}
