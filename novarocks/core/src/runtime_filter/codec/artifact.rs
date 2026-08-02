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

//! Consumer-side artifact delivery wire codec.
//!
//! This is the outbound dual of the producer-side contribution codec in
//! [`super::contribution`]. It frames a materialized [`ArtifactBundle`] (or an
//! `Unavailable` sentinel) for transport to a remote consumer and decodes such a
//! frame back into an owned bundle, verifying it against the consumer's
//! install-owned [`ConsumerArtifactProfile`].
//!
//! Framing is canonical: a fixed header carries the wire kind, the consumer
//! profile digest, the bundle logical version, and the body length; the body
//! reuses the leaf physical codecs (`encode_physical_leaf` / `decode_leaf` /
//! `decode_range`) for each artifact. Decoding performs a three-way profile
//! digest consistency check (envelope vs. installed expectation vs. frame) and a
//! canonical re-encode equality check, mirroring the contribution codec.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::runtime_filter::materializer::codec::{
    ArtifactCodecError, ArtifactDecodeExpectations, RangeDecodeExpectations, decode_leaf,
    decode_range,
};
use crate::runtime_filter::model::contract::ChannelId;
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactContractError, ArtifactKind, ArtifactSchemaDigest,
    ConsumerArtifactProfile, PhysicalArtifact,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::subscription::UnavailableReason;
use crate::runtime_filter::port::support::{ArtifactRetainedBudget, RuntimeFilterMemoryAccount};

const MAGIC: &[u8; 4] = b"NRFA";
const CODEC_VERSION: u16 = 1;
// MAGIC(4) + CODEC_VERSION(2) + wire_kind(1) + flags(1) + profile_digest(32)
// + logical_version(8) + body_len(8)
const HEADER_LEN: usize = 4 + 2 + 1 + 1 + 32 + 8 + 8;
// channel_id(4) + schema_digest(32) + artifact_count(2)
const BUNDLE_BODY_PREFIX_LEN: usize = 4 + 32 + 2;
// kind_tag(1) + leaf_len(8)
const ARTIFACT_RECORD_PREFIX_LEN: usize = 1 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WireFrameKind {
    Bundle,
    Unavailable,
}

impl WireFrameKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Bundle => 1,
            Self::Unavailable => 2,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Bundle),
            2 => Some(Self::Unavailable),
            _ => None,
        }
    }
}

const fn unavailable_reason_tag(reason: UnavailableReason) -> u8 {
    match reason {
        UnavailableReason::ResourceLimit => 1,
        UnavailableReason::IncompleteCoverage => 2,
        UnavailableReason::ProducerFailed => 3,
        UnavailableReason::MaterializationFailed => 4,
        UnavailableReason::RouteUnavailable => 5,
    }
}

const fn unavailable_reason_from_tag(tag: u8) -> Option<UnavailableReason> {
    match tag {
        1 => Some(UnavailableReason::ResourceLimit),
        2 => Some(UnavailableReason::IncompleteCoverage),
        3 => Some(UnavailableReason::ProducerFailed),
        4 => Some(UnavailableReason::MaterializationFailed),
        5 => Some(UnavailableReason::RouteUnavailable),
        _ => None,
    }
}

/// Immutable decode expectation borrowed from the consumer's install-owned
/// [`ConsumerArtifactProfile`]. The profile is the authority for the accepted
/// artifact kinds, the bloom hash contract, the range order contract, and the
/// profile identity digest; the wire decode never derives contract facts from
/// the payload.
#[derive(Clone, Copy, Debug)]
pub struct ArtifactDecodeExpectation<'a> {
    profile: &'a ConsumerArtifactProfile,
}

impl<'a> ArtifactDecodeExpectation<'a> {
    pub const fn new(profile: &'a ConsumerArtifactProfile) -> Self {
        Self { profile }
    }

    const fn profile(&self) -> &ConsumerArtifactProfile {
        self.profile
    }
}

/// A canonical artifact-delivery wire frame plus the consumer profile digest it
/// commits to. Dual of `EncodedContribution`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedArtifactFrame {
    profile_digest: [u8; 32],
    payload: Vec<u8>,
}

impl EncodedArtifactFrame {
    pub const fn profile_digest(&self) -> &[u8; 32] {
        &self.profile_digest
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> ([u8; 32], Vec<u8>) {
        (self.profile_digest, self.payload)
    }

    /// Build an opaque frame directly from parts. Reserved for tests that exercise
    /// transport plumbing (buffering / retry / ack release), which treat the frame
    /// as already-serialized bytes and never decode it.
    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub fn from_parts_for_test(profile_digest: [u8; 32], payload: Vec<u8>) -> Self {
        Self {
            profile_digest,
            payload,
        }
    }
}

pub fn encode_completed_without_artifact(
    expectation: ArtifactDecodeExpectation<'_>,
) -> EncodedArtifactFrame {
    EncodedArtifactFrame {
        profile_digest: expectation.profile().id().bytes(),
        payload: Vec::new(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactWireCodecError {
    Malformed,
    Truncated,
    UnknownVersion,
    UnknownKind,
    UnknownReason,
    UnknownArtifactKind,
    InvalidFlags,
    KindMismatch,
    KindNotAccepted,
    ProfileMismatch,
    SchemaMismatch,
    VersionMismatch,
    HashContractMismatch,
    ContractViolation,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    SemanticSizeExceeded,
    ResourceLimit,
}

impl fmt::Display for ArtifactWireCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime filter artifact frame: {self:?}")
    }
}

impl Error for ArtifactWireCodecError {}

/// Wire-ceiling budget helper (dual of `max_encoded_len_for_contribution_budget`).
///
/// Converts a semantic bundle budget (the maximum canonical bundle size) into the
/// corresponding maximum wire frame length by adding the fixed frame header.
pub fn max_encoded_len_for_artifact_budget(
    max_semantic_bytes: usize,
) -> Result<usize, ArtifactWireCodecError> {
    HEADER_LEN
        .checked_add(max_semantic_bytes)
        .ok_or(ArtifactWireCodecError::LengthOverflow)
}

/// Canonical semantic size of a bundle (dual of `semantic_contribution_bytes`).
///
/// This is the bundle's own canonical encoded length, independent of the wire
/// framing overhead, and is what callers compare against an installed semantic
/// budget.
pub fn semantic_artifact_bytes(bundle: &ArtifactBundle) -> Result<usize, ArtifactWireCodecError> {
    ArtifactBundle::canonical_encoded_len(bundle.artifacts())
        .map_err(|_| ArtifactWireCodecError::LengthOverflow)
}

pub fn encode_artifact_bundle(
    bundle: &ArtifactBundle,
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
) -> Result<EncodedArtifactFrame, ArtifactWireCodecError> {
    let profile_digest = bundle.profile_id().bytes();
    if profile_digest != expectation.profile().id().bytes() {
        return Err(ArtifactWireCodecError::ProfileMismatch);
    }
    let artifacts = bundle.artifacts();
    let (_, first_artifact) = artifacts.first().ok_or(ArtifactWireCodecError::Malformed)?;
    let schema_digest = first_artifact.schema_digest().bytes();
    let count =
        u16::try_from(artifacts.len()).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;

    let mut body_len = BUNDLE_BODY_PREFIX_LEN;
    for (_, artifact) in artifacts {
        let leaf_len = artifact.canonical_bytes().len();
        u64::try_from(leaf_len).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
        body_len = body_len
            .checked_add(ARTIFACT_RECORD_PREFIX_LEN)
            .and_then(|bytes| bytes.checked_add(leaf_len))
            .ok_or(ArtifactWireCodecError::LengthOverflow)?;
    }
    let body_len_u64 =
        u64::try_from(body_len).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
    let wire_len = HEADER_LEN
        .checked_add(body_len)
        .ok_or(ArtifactWireCodecError::LengthOverflow)?;

    let semantic = semantic_artifact_bytes(bundle)?;
    if semantic > max_encoded {
        return Err(ArtifactWireCodecError::SemanticSizeExceeded);
    }
    if wire_len > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }

    let mut payload = Vec::new();
    payload
        .try_reserve_exact(wire_len)
        .map_err(|_| ArtifactWireCodecError::ResourceLimit)?;
    write_frame_header(
        &mut payload,
        WireFrameKind::Bundle,
        &profile_digest,
        bundle.version(),
        body_len_u64,
    );
    payload.extend_from_slice(&bundle.channel_id().get().to_be_bytes());
    payload.extend_from_slice(&schema_digest);
    payload.extend_from_slice(&count.to_be_bytes());
    for (kind, artifact) in artifacts {
        let leaf = artifact.canonical_bytes();
        payload.push(kind.tag());
        payload.extend_from_slice(&(leaf.len() as u64).to_be_bytes());
        payload.extend_from_slice(leaf);
    }
    debug_assert_eq!(payload.len(), wire_len);
    Ok(EncodedArtifactFrame {
        profile_digest,
        payload,
    })
}

pub fn decode_artifact_bundle(
    payload: &[u8],
    envelope_profile_digest: &[u8; 32],
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
    retained_budget: Arc<ArtifactRetainedBudget>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<Arc<ArtifactBundle>, ArtifactWireCodecError> {
    if payload.len() > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let profile = expectation.profile();
    let installed_profile_digest = profile.id().bytes();

    let mut reader = Reader::new(payload);
    let header = parse_frame_header(&mut reader)?;
    let body = reader.read_exact(header.body_len)?;
    debug_assert!(reader.is_empty());

    if header.kind != WireFrameKind::Bundle {
        return Err(ArtifactWireCodecError::KindMismatch);
    }
    verify_profile_digests(
        &header.profile_digest,
        envelope_profile_digest,
        &installed_profile_digest,
    )?;
    let logical_version = LogicalVersion::new(header.logical_version);

    let mut body_reader = Reader::new(body);
    let channel_id = ChannelId::new(body_reader.read_u32()?);
    let schema_digest = body_reader.read_array::<32>()?;
    let count = usize::from(body_reader.read_u16()?);

    let mut artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)> = Vec::new();
    artifacts
        .try_reserve_exact(count)
        .map_err(|_| ArtifactWireCodecError::ResourceLimit)?;
    for _ in 0..count {
        let kind = ArtifactKind::from_tag(body_reader.read_u8()?)
            .ok_or(ArtifactWireCodecError::UnknownArtifactKind)?;
        let leaf_len = usize::try_from(body_reader.read_u64()?)
            .map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
        let leaf = body_reader.read_exact(leaf_len)?;
        if !profile.accepts(kind) {
            return Err(ArtifactWireCodecError::KindNotAccepted);
        }
        let artifact = if kind == ArtifactKind::Range {
            let expected_order_digest = profile
                .order_contract_digest()
                .ok_or(ArtifactWireCodecError::SchemaMismatch)?;
            decode_range(
                leaf,
                RangeDecodeExpectations {
                    expected_order_digest,
                    expected_logical_version: logical_version,
                },
                max_encoded,
                retained_budget.clone(),
                memory_account.clone(),
            )
            .map_err(map_leaf_error)?
        } else {
            let expected_hash_contract = if kind == ArtifactKind::Bloom {
                Some(
                    profile
                        .bloom_hash_contract()
                        .ok_or(ArtifactWireCodecError::HashContractMismatch)?,
                )
            } else {
                None
            };
            decode_leaf(
                leaf,
                ArtifactDecodeExpectations {
                    expected_kind: kind,
                    expected_schema_digest: ArtifactSchemaDigest::from_canonical_bytes(
                        schema_digest,
                    ),
                    expected_logical_version: logical_version,
                    expected_hash_contract,
                },
                max_encoded,
                retained_budget.clone(),
                memory_account.clone(),
            )
            .map_err(map_leaf_error)?
        };
        if artifact.schema_digest().bytes() != schema_digest {
            return Err(ArtifactWireCodecError::SchemaMismatch);
        }
        artifacts.push((kind, artifact));
    }
    if !body_reader.is_empty() {
        return Err(ArtifactWireCodecError::TrailingBytes);
    }

    let bundle = ArtifactBundle::new(channel_id, logical_version, profile, artifacts, max_encoded)
        .map_err(map_bundle_error)?;
    let reencoded = encode_artifact_bundle(&bundle, expectation, max_encoded)?;
    if reencoded.payload() != payload {
        return Err(ArtifactWireCodecError::NonCanonicalPayload);
    }
    Ok(Arc::new(bundle))
}

pub fn encode_unavailable(
    reason: UnavailableReason,
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
) -> Result<EncodedArtifactFrame, ArtifactWireCodecError> {
    let profile_digest = expectation.profile().id().bytes();
    let body_len: usize = 1;
    let wire_len = HEADER_LEN
        .checked_add(body_len)
        .ok_or(ArtifactWireCodecError::LengthOverflow)?;
    if wire_len > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(wire_len)
        .map_err(|_| ArtifactWireCodecError::ResourceLimit)?;
    write_frame_header(
        &mut payload,
        WireFrameKind::Unavailable,
        &profile_digest,
        LogicalVersion::new(0),
        body_len as u64,
    );
    payload.push(unavailable_reason_tag(reason));
    debug_assert_eq!(payload.len(), wire_len);
    Ok(EncodedArtifactFrame {
        profile_digest,
        payload,
    })
}

pub fn decode_unavailable(
    payload: &[u8],
    envelope_profile_digest: &[u8; 32],
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
) -> Result<UnavailableReason, ArtifactWireCodecError> {
    if payload.len() > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let installed_profile_digest = expectation.profile().id().bytes();

    let mut reader = Reader::new(payload);
    let header = parse_frame_header(&mut reader)?;
    let body = reader.read_exact(header.body_len)?;
    debug_assert!(reader.is_empty());

    if header.kind != WireFrameKind::Unavailable {
        return Err(ArtifactWireCodecError::KindMismatch);
    }
    // The logical version field is reserved (and canonically zero) for the
    // stateless Unavailable sentinel.
    if header.logical_version != 0 {
        return Err(ArtifactWireCodecError::NonCanonicalPayload);
    }
    verify_profile_digests(
        &header.profile_digest,
        envelope_profile_digest,
        &installed_profile_digest,
    )?;

    if body.len() != 1 {
        return Err(ArtifactWireCodecError::Malformed);
    }
    let reason =
        unavailable_reason_from_tag(body[0]).ok_or(ArtifactWireCodecError::UnknownReason)?;

    let reencoded = encode_unavailable(reason, expectation, max_encoded)?;
    if reencoded.payload() != payload {
        return Err(ArtifactWireCodecError::NonCanonicalPayload);
    }
    Ok(reason)
}

fn write_frame_header(
    payload: &mut Vec<u8>,
    kind: WireFrameKind,
    profile_digest: &[u8; 32],
    logical_version: LogicalVersion,
    body_len: u64,
) {
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&CODEC_VERSION.to_be_bytes());
    payload.push(kind.tag());
    payload.push(0);
    payload.extend_from_slice(profile_digest);
    payload.extend_from_slice(&logical_version.get().to_be_bytes());
    payload.extend_from_slice(&body_len.to_be_bytes());
}

struct ParsedFrameHeader {
    kind: WireFrameKind,
    profile_digest: [u8; 32],
    logical_version: u64,
    body_len: usize,
}

fn parse_frame_header(
    reader: &mut Reader<'_>,
) -> Result<ParsedFrameHeader, ArtifactWireCodecError> {
    if reader.read_exact(MAGIC.len())? != MAGIC {
        return Err(ArtifactWireCodecError::Malformed);
    }
    if reader.read_u16()? != CODEC_VERSION {
        return Err(ArtifactWireCodecError::UnknownVersion);
    }
    let kind =
        WireFrameKind::from_tag(reader.read_u8()?).ok_or(ArtifactWireCodecError::UnknownKind)?;
    if reader.read_u8()? != 0 {
        return Err(ArtifactWireCodecError::InvalidFlags);
    }
    let profile_digest = reader.read_array::<32>()?;
    let logical_version = reader.read_u64()?;
    let body_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
    match body_len.cmp(&reader.remaining_len()) {
        std::cmp::Ordering::Less => return Err(ArtifactWireCodecError::TrailingBytes),
        std::cmp::Ordering::Greater => return Err(ArtifactWireCodecError::Truncated),
        std::cmp::Ordering::Equal => {}
    }
    Ok(ParsedFrameHeader {
        kind,
        profile_digest,
        logical_version,
        body_len,
    })
}

fn verify_profile_digests(
    frame_profile_digest: &[u8; 32],
    envelope_profile_digest: &[u8; 32],
    installed_profile_digest: &[u8; 32],
) -> Result<(), ArtifactWireCodecError> {
    if frame_profile_digest != envelope_profile_digest
        || frame_profile_digest != installed_profile_digest
        || envelope_profile_digest != installed_profile_digest
    {
        return Err(ArtifactWireCodecError::ProfileMismatch);
    }
    Ok(())
}

fn map_leaf_error(error: ArtifactCodecError) -> ArtifactWireCodecError {
    match error {
        ArtifactCodecError::KindMismatch | ArtifactCodecError::UnsupportedKind => {
            ArtifactWireCodecError::KindMismatch
        }
        ArtifactCodecError::SchemaMismatch => ArtifactWireCodecError::SchemaMismatch,
        ArtifactCodecError::VersionMismatch => ArtifactWireCodecError::VersionMismatch,
        ArtifactCodecError::HashContractMismatch | ArtifactCodecError::InvalidHashContract => {
            ArtifactWireCodecError::HashContractMismatch
        }
        ArtifactCodecError::UnknownVersion => ArtifactWireCodecError::UnknownVersion,
        ArtifactCodecError::UnknownKind => ArtifactWireCodecError::UnknownArtifactKind,
        ArtifactCodecError::InvalidFlags => ArtifactWireCodecError::InvalidFlags,
        ArtifactCodecError::Truncated => ArtifactWireCodecError::Truncated,
        ArtifactCodecError::TrailingBytes => ArtifactWireCodecError::TrailingBytes,
        ArtifactCodecError::LengthOverflow => ArtifactWireCodecError::LengthOverflow,
        ArtifactCodecError::EncodedSizeExceeded => ArtifactWireCodecError::EncodedSizeExceeded,
        ArtifactCodecError::ContractViolation => ArtifactWireCodecError::ContractViolation,
        ArtifactCodecError::ResourceLimit | ArtifactCodecError::ResourceUnavailable => {
            ArtifactWireCodecError::ResourceLimit
        }
        ArtifactCodecError::Malformed | ArtifactCodecError::NonCanonicalPayload => {
            ArtifactWireCodecError::NonCanonicalPayload
        }
    }
}

fn map_bundle_error(error: ArtifactContractError) -> ArtifactWireCodecError {
    match error {
        ArtifactContractError::DuplicateKind => ArtifactWireCodecError::NonCanonicalPayload,
        ArtifactContractError::KindNotAccepted => ArtifactWireCodecError::KindNotAccepted,
        ArtifactContractError::KindMismatch => ArtifactWireCodecError::KindMismatch,
        ArtifactContractError::VersionMismatch => ArtifactWireCodecError::VersionMismatch,
        ArtifactContractError::SchemaMismatch => ArtifactWireCodecError::SchemaMismatch,
        ArtifactContractError::EncodedSizeExceeded => ArtifactWireCodecError::SemanticSizeExceeded,
        ArtifactContractError::EncodedSizeOverflow => ArtifactWireCodecError::LengthOverflow,
        ArtifactContractError::RetentionSizeMismatch
        | ArtifactContractError::ResidentSizeOverflow => ArtifactWireCodecError::ResourceLimit,
        _ => ArtifactWireCodecError::Malformed,
    }
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ArtifactWireCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ArtifactWireCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactWireCodecError> {
        Ok(self.read_exact(N)?.try_into().expect("exact array length"))
    }

    fn read_u8(&mut self) -> Result<u8, ArtifactWireCodecError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ArtifactWireCodecError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, ArtifactWireCodecError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, ArtifactWireCodecError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::{
        ArtifactDecodeExpectation, ArtifactWireCodecError, HEADER_LEN, decode_artifact_bundle,
        decode_unavailable, encode_artifact_bundle, encode_unavailable,
        max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
    };
    use crate::runtime_filter::materializer::bloom::BloomHashContract;
    use crate::runtime_filter::materializer::range::{
        RangeMaterializationOutcome, RangeMaterializer,
    };
    use crate::runtime_filter::materializer::{MaterializationOutcome, Materializer};
    use crate::runtime_filter::model::contract::{
        ChannelId, NullOrder, NullSemantics, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, ComparatorDigestV1, OrderedScalar, OrderedTuple,
        RuntimeOrderContract,
    };
    use crate::runtime_filter::port::subscription::UnavailableReason;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::OrderedBoundDomain;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    const ROOMY: usize = 1 << 20;

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    fn budget() -> Arc<ArtifactRetainedBudget> {
        Arc::new(ArtifactRetainedBudget::new(ROOMY))
    }

    fn account() -> Arc<dyn RuntimeFilterMemoryAccount> {
        Arc::new(UnlimitedMemory)
    }

    fn materialize_membership(
        values: MembershipValues,
        contains_null: bool,
        null_semantics: NullSemantics,
        accepted_kinds: BTreeSet<ArtifactKind>,
        max_artifact_bytes: usize,
    ) -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        let schema = ArtifactMembershipSchema::new(&values.data_type(), null_semantics).unwrap();
        let policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1).unwrap();
        let bloom_contract = accepted_kinds
            .contains(&ArtifactKind::Bloom)
            .then(|| BloomHashContract::new(&schema, policy).unwrap().digest());
        let profile = ConsumerArtifactProfile::new(accepted_kinds, bloom_contract).unwrap();
        let snapshot = LogicalSnapshot::first(
            ChannelId::new(7),
            ReducedMembershipDomain::new(values, contains_null),
            RetainedMemoryReservation::empty(),
        );
        let plan = Materializer::plan(
            Arc::new(snapshot),
            &schema,
            &profile,
            policy,
            max_artifact_bytes,
        )
        .unwrap();
        match Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            Arc::new(UnlimitedMemory),
        ) {
            MaterializationOutcome::Published(bundle) => (bundle, profile),
            other => panic!("expected a published bundle, got {other:?}"),
        }
    }

    fn value_set_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        materialize_membership(
            MembershipValues::int64([1, 1_000_000]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::Bloom,
                ArtifactKind::EmptyDomain,
            ]),
            4096,
        )
    }

    fn bitset_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        materialize_membership(
            MembershipValues::int64(100..164),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::EmptyDomain,
            ]),
            4096,
        )
    }

    fn bloom_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        materialize_membership(
            MembershipValues::int64((0..128).map(|value| value * 1_000_000)),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::Bloom,
                ArtifactKind::EmptyDomain,
            ]),
            512,
        )
    }

    fn empty_domain_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        materialize_membership(
            MembershipValues::int64([]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            4096,
        )
    }

    fn range_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let tuple = OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(11))]).unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(9),
            LogicalVersion::new(5),
            Arc::new(OrderedBoundDomain::new(contract.clone(), tuple)),
            RetainedMemoryReservation::empty(),
        ));
        let profile = ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap();
        match RangeMaterializer::materialize(
            snapshot,
            &profile,
            usize::MAX,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            Arc::new(UnlimitedMemory),
        ) {
            RangeMaterializationOutcome::Published(bundle) => (bundle, profile),
            other => panic!("expected a published range bundle, got {other:?}"),
        }
    }

    fn two_artifact_bundle() -> (Arc<ArtifactBundle>, ConsumerArtifactProfile) {
        let kinds = BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]);
        let (value_set, profile) = materialize_membership(
            MembershipValues::int64([1, 2, 3]),
            false,
            NullSemantics::NeverMatches,
            kinds.clone(),
            4096,
        );
        let (empty, _) = materialize_membership(
            MembershipValues::int64([]),
            false,
            NullSemantics::NeverMatches,
            kinds,
            4096,
        );
        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &profile,
            vec![
                value_set.artifacts()[0].clone(),
                empty.artifacts()[0].clone(),
            ],
            ROOMY,
        )
        .unwrap();
        (Arc::new(bundle), profile)
    }

    fn assert_bundle_logical_eq(decoded: &ArtifactBundle, expected: &ArtifactBundle) {
        assert_eq!(decoded.canonical_digest(), expected.canonical_digest());
        assert_eq!(decoded.channel_id(), expected.channel_id());
        assert_eq!(decoded.version(), expected.version());
        assert_eq!(decoded.profile_id(), expected.profile_id());
        assert_eq!(decoded.artifacts().len(), expected.artifacts().len());
        for ((decoded_kind, decoded_artifact), (expected_kind, expected_artifact)) in
            decoded.artifacts().iter().zip(expected.artifacts())
        {
            assert_eq!(decoded_kind, expected_kind);
            assert_eq!(
                decoded_artifact.canonical_bytes(),
                expected_artifact.canonical_bytes()
            );
            assert_eq!(
                decoded_artifact.schema_digest(),
                expected_artifact.schema_digest()
            );
            assert_eq!(decoded_artifact.version(), expected_artifact.version());
        }
    }

    fn assert_round_trip(bundle: &ArtifactBundle, profile: &ConsumerArtifactProfile) {
        let expectation = ArtifactDecodeExpectation::new(profile);
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(bundle).unwrap()).unwrap();
        let frame = encode_artifact_bundle(bundle, expectation, ceiling).unwrap();
        assert_eq!(frame.profile_digest(), &bundle.profile_id().bytes());
        assert!(frame.payload().len() <= ceiling);
        let decoded = decode_artifact_bundle(
            frame.payload(),
            frame.profile_digest(),
            expectation,
            ceiling,
            budget(),
            account(),
        )
        .unwrap();
        assert_bundle_logical_eq(&decoded, bundle);
    }

    #[test]
    fn artifact_wire_codec_value_set_bundle_round_trips() {
        let (bundle, profile) = value_set_bundle();
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
        assert_round_trip(&bundle, &profile);
    }

    #[test]
    fn artifact_wire_codec_bloom_bundle_round_trips() {
        let (bundle, profile) = bloom_bundle();
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Bloom);
        assert_round_trip(&bundle, &profile);
    }

    #[test]
    fn artifact_wire_codec_bitset_bundle_round_trips() {
        let (bundle, profile) = bitset_bundle();
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Bitset);
        assert_round_trip(&bundle, &profile);
    }

    #[test]
    fn artifact_wire_codec_range_bundle_round_trips() {
        let (bundle, profile) = range_bundle();
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Range);
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(&bundle).unwrap()).unwrap();
        let frame = encode_artifact_bundle(&bundle, expectation, ceiling).unwrap();
        let decoded = decode_artifact_bundle(
            frame.payload(),
            frame.profile_digest(),
            expectation,
            ceiling,
            budget(),
            account(),
        )
        .unwrap();
        assert_bundle_logical_eq(&decoded, &bundle);
        assert_eq!(
            decoded.artifacts()[0].1.range().unwrap().semantic_digest(),
            bundle.artifacts()[0].1.range().unwrap().semantic_digest()
        );
        assert_eq!(
            decoded.artifacts()[0].1.range().unwrap().bound(),
            bundle.artifacts()[0].1.range().unwrap().bound()
        );
    }

    #[test]
    fn artifact_wire_codec_empty_domain_bundle_round_trips() {
        let (bundle, profile) = empty_domain_bundle();
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::EmptyDomain);
        assert_round_trip(&bundle, &profile);
    }

    #[test]
    fn artifact_wire_codec_unavailable_reasons_round_trip() {
        let (_, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        for reason in [
            UnavailableReason::ResourceLimit,
            UnavailableReason::IncompleteCoverage,
            UnavailableReason::ProducerFailed,
            UnavailableReason::MaterializationFailed,
            UnavailableReason::RouteUnavailable,
        ] {
            let frame = encode_unavailable(reason, expectation, ROOMY).unwrap();
            assert_eq!(frame.profile_digest(), &profile.id().bytes());
            let decoded =
                decode_unavailable(frame.payload(), frame.profile_digest(), expectation, ROOMY)
                    .unwrap();
            assert_eq!(decoded, reason);
        }
    }

    #[test]
    fn artifact_wire_codec_rejects_wrong_frame_kind() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);

        let bundle_frame = encode_artifact_bundle(&bundle, expectation, ROOMY).unwrap();
        assert_eq!(
            decode_unavailable(
                bundle_frame.payload(),
                bundle_frame.profile_digest(),
                expectation,
                ROOMY,
            ),
            Err(ArtifactWireCodecError::KindMismatch)
        );

        let unavailable_frame =
            encode_unavailable(UnavailableReason::ResourceLimit, expectation, ROOMY).unwrap();
        assert_eq!(
            decode_artifact_bundle(
                unavailable_frame.payload(),
                unavailable_frame.profile_digest(),
                expectation,
                ROOMY,
                budget(),
                account(),
            )
            .map(|_| ()),
            Err(ArtifactWireCodecError::KindMismatch)
        );
    }

    #[test]
    fn artifact_wire_codec_rejects_profile_digest_mismatch() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let frame = encode_artifact_bundle(&bundle, expectation, ROOMY).unwrap();

        assert_eq!(
            decode_artifact_bundle(
                frame.payload(),
                &[0_u8; 32],
                expectation,
                ROOMY,
                budget(),
                account(),
            )
            .map(|_| ()),
            Err(ArtifactWireCodecError::ProfileMismatch)
        );
    }

    #[test]
    fn artifact_wire_codec_rejects_schema_digest_mismatch() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let frame = encode_artifact_bundle(&bundle, expectation, ROOMY).unwrap();

        // The bundle schema digest lives right after MAGIC/version/kind/flags,
        // the profile digest, the logical version and body length header, then
        // the four channel-id bytes.
        let mut tampered = frame.payload().to_vec();
        let schema_digest_offset = HEADER_LEN + 4;
        tampered[schema_digest_offset] ^= 0xFF;

        assert_eq!(
            decode_artifact_bundle(
                &tampered,
                frame.profile_digest(),
                expectation,
                ROOMY,
                budget(),
                account(),
            )
            .map(|_| ()),
            Err(ArtifactWireCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn artifact_wire_codec_rejects_noncanonical_reencode() {
        let (bundle, profile) = two_artifact_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let frame = encode_artifact_bundle(&bundle, expectation, ROOMY).unwrap();

        // A valid frame whose two artifact records are swapped decodes fine (both
        // leaves are valid) but re-encodes to the canonical sorted order, so the
        // canonical re-encode equality check must reject it.
        let swapped = swap_first_two_records(frame.payload());
        assert_eq!(
            decode_artifact_bundle(
                &swapped,
                frame.profile_digest(),
                expectation,
                ROOMY,
                budget(),
                account(),
            )
            .map(|_| ()),
            Err(ArtifactWireCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn artifact_wire_codec_rejects_wire_over_ceiling() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let frame = encode_artifact_bundle(&bundle, expectation, ROOMY).unwrap();

        assert_eq!(
            decode_artifact_bundle(
                frame.payload(),
                frame.profile_digest(),
                expectation,
                frame.payload().len() - 1,
                budget(),
                account(),
            )
            .map(|_| ()),
            Err(ArtifactWireCodecError::EncodedSizeExceeded)
        );

        // At exactly the semantic size the meaningful content fits, but the framed
        // wire frame does not, so encoding is rejected as a wire-ceiling overflow.
        let semantic = semantic_artifact_bytes(&bundle).unwrap();
        assert_eq!(
            encode_artifact_bundle(&bundle, expectation, semantic).map(|_| ()),
            Err(ArtifactWireCodecError::EncodedSizeExceeded)
        );
    }

    #[test]
    fn artifact_wire_codec_rejects_semantic_over_budget() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let semantic = semantic_artifact_bytes(&bundle).unwrap();

        assert_eq!(
            encode_artifact_bundle(&bundle, expectation, semantic - 1).map(|_| ()),
            Err(ArtifactWireCodecError::SemanticSizeExceeded)
        );
    }

    #[test]
    fn artifact_wire_codec_separates_wire_and_semantic_limits() {
        let (bundle, profile) = value_set_bundle();
        let expectation = ArtifactDecodeExpectation::new(&profile);
        let semantic = semantic_artifact_bytes(&bundle).unwrap();
        let semantic_budget = semantic - 1;
        let wire_ceiling = max_encoded_len_for_artifact_budget(semantic_budget).unwrap();

        let frame = encode_artifact_bundle(&bundle, expectation, wire_ceiling).unwrap();
        assert!(frame.payload().len() <= wire_ceiling);
        assert!(semantic_artifact_bytes(&bundle).unwrap() > semantic_budget);
    }

    #[test]
    fn artifact_wire_codec_checked_budget_overflow_is_rejected() {
        assert_eq!(
            max_encoded_len_for_artifact_budget(usize::MAX),
            Err(ArtifactWireCodecError::LengthOverflow)
        );
    }

    fn swap_first_two_records(payload: &[u8]) -> Vec<u8> {
        let records_start = HEADER_LEN + 4 + 32 + 2;
        let first_kind = records_start;
        let first_len =
            u64::from_be_bytes(payload[first_kind + 1..first_kind + 9].try_into().unwrap())
                as usize;
        let first_end = first_kind + 9 + first_len;
        let second_kind = first_end;
        let second_len = u64::from_be_bytes(
            payload[second_kind + 1..second_kind + 9]
                .try_into()
                .unwrap(),
        ) as usize;
        let second_end = second_kind + 9 + second_len;

        let mut swapped = Vec::with_capacity(payload.len());
        swapped.extend_from_slice(&payload[..records_start]);
        swapped.extend_from_slice(&payload[second_kind..second_end]);
        swapped.extend_from_slice(&payload[first_kind..first_end]);
        swapped.extend_from_slice(&payload[second_end..]);
        swapped
    }
}
