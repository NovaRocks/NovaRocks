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

pub mod bitset;
pub mod bloom;
pub mod codec;
pub mod range;

use std::sync::Arc;

use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    PhysicalArtifact,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::install::MaterializationPolicy;
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactRetention, ArtifactScratchBudget, ArtifactScratchReservation,
    RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::{LogicalSnapshot, MembershipValues};

use self::bitset::BitsetPlan;
use self::bloom::BloomHashContract;
use self::codec::{encode_membership_leaf, encode_physical_leaf, encoded_leaf_len};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationError {
    UnsupportedRange,
    SchemaMismatch,
    ProfileHashMismatch,
    SizeOverflow,
}

pub const fn validate_membership_kind(kind: ArtifactKind) -> Result<(), MaterializationError> {
    if matches!(kind, ArtifactKind::Range) {
        Err(MaterializationError::UnsupportedRange)
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedReason {
    NoAcceptedRepresentation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableReason {
    ResourceLimit,
    MaterializationFailed,
}

#[derive(Debug)]
pub enum MaterializationOutcome {
    Published(Arc<ArtifactBundle>),
    Unsupported(UnsupportedReason),
    Unavailable(UnavailableReason),
}

impl MaterializationOutcome {
    pub fn published_kind(&self) -> Option<ArtifactKind> {
        match self {
            Self::Published(bundle) => bundle.artifacts().first().map(|(kind, _)| *kind),
            Self::Unsupported(_) | Self::Unavailable(_) => None,
        }
    }

    pub fn published_digest(&self) -> Option<[u8; 32]> {
        match self {
            Self::Published(bundle) => Some(bundle.canonical_digest()),
            Self::Unsupported(_) | Self::Unavailable(_) => None,
        }
    }

    pub fn published_version(&self) -> Option<LogicalVersion> {
        match self {
            Self::Published(bundle) => Some(bundle.version()),
            Self::Unsupported(_) | Self::Unavailable(_) => None,
        }
    }

    pub fn published_channel(&self) -> Option<ChannelId> {
        match self {
            Self::Published(bundle) => Some(bundle.channel_id()),
            Self::Unsupported(_) | Self::Unavailable(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
enum SelectedRepresentation {
    EmptyDomain,
    ValueSet,
    Bitset(BitsetPlan),
    Bloom(BloomHashContract),
    Unsupported(UnsupportedReason),
    ResourceLimit,
}

#[derive(Debug)]
pub struct MaterializationPlan<'a> {
    snapshot: Arc<LogicalSnapshot>,
    schema: &'a ArtifactMembershipSchema,
    profile: &'a ConsumerArtifactProfile,
    policy: MaterializationPolicy,
    max_artifact_bytes: usize,
    selected: SelectedRepresentation,
    leaf_encoded_bytes: usize,
    payload_bytes: usize,
    max_scalar_frame_bytes: usize,
    resident_index_bytes: usize,
}

pub enum MaterializationAdmission<'a> {
    Ready(AdmittedMaterialization<'a>),
    Complete(MaterializationOutcome),
}

pub struct AdmittedMaterialization<'a> {
    plan: MaterializationPlan<'a>,
    _scratch: ArtifactScratchReservation,
    artifact_footprint: usize,
    total_footprint: usize,
    retained: Arc<ArtifactRetention>,
}

enum PreparationFailure {
    ResourceLimit,
    Internal,
}

pub struct Materializer;

impl Materializer {
    pub fn plan<'a>(
        snapshot: Arc<LogicalSnapshot>,
        schema: &'a ArtifactMembershipSchema,
        profile: &'a ConsumerArtifactProfile,
        policy: MaterializationPolicy,
        max_artifact_bytes: usize,
    ) -> Result<MaterializationPlan<'a>, MaterializationError> {
        if snapshot.domain().data_type() != *schema.data_type()
            || (snapshot.domain().contains_null()
                && schema.null_semantics() != NullSemantics::NullSafeEqual)
        {
            return Err(MaterializationError::SchemaMismatch);
        }
        let values = snapshot.domain().values();
        let max_scalar_frame_bytes = values
            .canonical_scalar_max_frame_len()
            .map_err(|_| MaterializationError::SizeOverflow)?;

        let candidate = |kind, payload_bytes, hash_contract| {
            let leaf = encoded_leaf_len(schema, hash_contract, payload_bytes)
                .map_err(|_| MaterializationError::SizeOverflow)?;
            let bundle = ArtifactBundle::canonical_encoded_len_for_single_artifact(leaf)
                .map_err(|_| MaterializationError::SizeOverflow)?;
            Ok::<_, MaterializationError>((kind, payload_bytes, leaf, bundle))
        };

        let (selected, payload_bytes, leaf_encoded_bytes) = if values.is_empty()
            && !snapshot.domain().contains_null()
        {
            if !profile.accepts(ArtifactKind::EmptyDomain) {
                (
                    SelectedRepresentation::Unsupported(
                        UnsupportedReason::NoAcceptedRepresentation,
                    ),
                    0,
                    0,
                )
            } else {
                let (_, payload, leaf, bundle) = candidate(ArtifactKind::EmptyDomain, 0, None)?;
                if bundle <= max_artifact_bytes {
                    (SelectedRepresentation::EmptyDomain, payload, leaf)
                } else {
                    (SelectedRepresentation::ResourceLimit, 0, 0)
                }
            }
        } else if values.is_empty() {
            if !profile.accepts(ArtifactKind::ValueSet) {
                (
                    SelectedRepresentation::Unsupported(
                        UnsupportedReason::NoAcceptedRepresentation,
                    ),
                    0,
                    0,
                )
            } else {
                let payload = values
                    .canonical_encoded_len()
                    .map_err(|_| MaterializationError::SizeOverflow)?;
                let (_, payload, leaf, bundle) = candidate(ArtifactKind::ValueSet, payload, None)?;
                if bundle <= max_artifact_bytes {
                    (SelectedRepresentation::ValueSet, payload, leaf)
                } else {
                    (SelectedRepresentation::ResourceLimit, 0, 0)
                }
            }
        } else {
            let mut exact = None;
            if profile.accepts(ArtifactKind::ValueSet) {
                let payload = values
                    .canonical_encoded_len()
                    .map_err(|_| MaterializationError::SizeOverflow)?;
                let (_, payload, leaf, bundle) = candidate(ArtifactKind::ValueSet, payload, None)?;
                if bundle <= max_artifact_bytes {
                    exact = Some((bundle, 1u8, SelectedRepresentation::ValueSet, payload, leaf));
                }
            }
            if profile.accepts(ArtifactKind::Bitset) {
                if let Ok(bitset) = BitsetPlan::new(values) {
                    let payload = bitset
                        .payload_len()
                        .map_err(|_| MaterializationError::SizeOverflow)?;
                    let (_, payload, leaf, bundle) =
                        candidate(ArtifactKind::Bitset, payload, None)?;
                    if bundle <= max_artifact_bytes {
                        let candidate = (
                            bundle,
                            0u8,
                            SelectedRepresentation::Bitset(bitset),
                            payload,
                            leaf,
                        );
                        if exact
                            .as_ref()
                            .is_none_or(|best| (candidate.0, candidate.1) < (best.0, best.1))
                        {
                            exact = Some(candidate);
                        }
                    }
                }
            }
            if let Some((_, _, selected, payload, leaf)) = exact {
                (selected, payload, leaf)
            } else if profile.accepts(ArtifactKind::Bloom) {
                let contract = BloomHashContract::new(schema, policy)
                    .map_err(|_| MaterializationError::ProfileHashMismatch)?;
                if profile.bloom_hash_contract() != Some(contract.digest()) {
                    return Err(MaterializationError::ProfileHashMismatch);
                }
                let payload = contract
                    .payload_len(values.len())
                    .map_err(|_| MaterializationError::SizeOverflow)?;
                let (_, payload, leaf, bundle) =
                    candidate(ArtifactKind::Bloom, payload, Some(contract.digest()))?;
                if bundle <= max_artifact_bytes {
                    (SelectedRepresentation::Bloom(contract), payload, leaf)
                } else {
                    (SelectedRepresentation::ResourceLimit, 0, 0)
                }
            } else {
                (SelectedRepresentation::ResourceLimit, 0, 0)
            }
        };

        let resident_index_bytes = match (&selected, values) {
            (SelectedRepresentation::ValueSet, MembershipValues::Utf8(values)) => values
                .len()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or(MaterializationError::SizeOverflow)?,
            _ => 0,
        };

        Ok(MaterializationPlan {
            snapshot,
            schema,
            profile,
            policy,
            max_artifact_bytes,
            selected,
            leaf_encoded_bytes,
            payload_bytes,
            max_scalar_frame_bytes,
            resident_index_bytes,
        })
    }

    pub fn materialize(
        plan: MaterializationPlan<'_>,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> MaterializationOutcome {
        match Self::admit(plan, retained_budget, scratch_budget, memory_account) {
            MaterializationAdmission::Complete(outcome) => outcome,
            MaterializationAdmission::Ready(prepared) => match Self::encode(prepared) {
                Ok(bundle) => MaterializationOutcome::Published(bundle),
                Err(()) => {
                    MaterializationOutcome::Unavailable(UnavailableReason::MaterializationFailed)
                }
            },
        }
    }

    pub fn admit<'a>(
        plan: MaterializationPlan<'a>,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> MaterializationAdmission<'a> {
        match &plan.selected {
            SelectedRepresentation::Unsupported(reason) => {
                return MaterializationAdmission::Complete(MaterializationOutcome::Unsupported(
                    *reason,
                ));
            }
            SelectedRepresentation::ResourceLimit => {
                return MaterializationAdmission::Complete(MaterializationOutcome::Unavailable(
                    UnavailableReason::ResourceLimit,
                ));
            }
            _ => {}
        }
        match Self::admit_selected(plan, retained_budget, scratch_budget, memory_account) {
            Ok(prepared) => MaterializationAdmission::Ready(prepared),
            Err(PreparationFailure::ResourceLimit) => MaterializationAdmission::Complete(
                MaterializationOutcome::Unavailable(UnavailableReason::ResourceLimit),
            ),
            Err(PreparationFailure::Internal) => MaterializationAdmission::Complete(
                MaterializationOutcome::Unavailable(UnavailableReason::MaterializationFailed),
            ),
        }
    }

    fn admit_selected<'a>(
        plan: MaterializationPlan<'a>,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<AdmittedMaterialization<'a>, PreparationFailure> {
        let bits_bytes = match &plan.selected {
            SelectedRepresentation::Bitset(bitset) => bitset.byte_count(),
            SelectedRepresentation::Bloom(contract) => contract
                .bit_count(plan.snapshot.domain().values().len())
                .ok()
                .and_then(|bits| usize::try_from(bits / 8).ok())
                .ok_or(PreparationFailure::Internal)?,
            _ => 0,
        };
        let scratch_bytes = plan
            .leaf_encoded_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(plan.payload_bytes))
            .and_then(|bytes| bytes.checked_add(bits_bytes))
            .and_then(|bytes| bytes.checked_add(plan.max_scalar_frame_bytes))
            .ok_or(PreparationFailure::Internal)?;
        if u64::try_from(scratch_bytes).map_err(|_| PreparationFailure::Internal)?
            > plan.policy.max_scratch_bytes_per_job()
        {
            return Err(PreparationFailure::ResourceLimit);
        }
        let scratch = ArtifactScratchReservation::try_new(
            scratch_bytes,
            scratch_budget,
            memory_account.clone(),
        )
        .map_err(|_| PreparationFailure::ResourceLimit)?;

        let artifact_footprint = if matches!(
            plan.selected,
            SelectedRepresentation::ValueSet | SelectedRepresentation::EmptyDomain
        ) {
            PhysicalArtifact::accounted_indexed_resident_component_bytes(
                plan.leaf_encoded_bytes,
                plan.resident_index_bytes,
            )
        } else {
            PhysicalArtifact::accounted_resident_component_bytes(plan.leaf_encoded_bytes)
        }
        .map_err(|_| PreparationFailure::Internal)?;
        let bundle_footprint = ArtifactBundle::accounted_resident_overhead(&plan.profile, 1)
            .map_err(|_| PreparationFailure::Internal)?;
        let total_footprint = artifact_footprint
            .checked_add(bundle_footprint)
            .ok_or(PreparationFailure::Internal)?;
        let retained = Arc::new(
            ArtifactRetention::try_new(total_footprint, retained_budget, memory_account)
                .map_err(|_| PreparationFailure::ResourceLimit)?,
        );

        Ok(AdmittedMaterialization {
            plan,
            _scratch: scratch,
            artifact_footprint,
            total_footprint,
            retained,
        })
    }

    pub fn encode(prepared: AdmittedMaterialization<'_>) -> Result<Arc<ArtifactBundle>, ()> {
        let AdmittedMaterialization {
            plan,
            _scratch,
            artifact_footprint,
            total_footprint,
            retained,
        } = prepared;

        let values = plan.snapshot.domain().values();
        let contains_null = plan.snapshot.domain().contains_null();
        let (kind, encoded) = match &plan.selected {
            SelectedRepresentation::EmptyDomain | SelectedRepresentation::ValueSet => {
                let encoded = encode_membership_leaf(
                    plan.snapshot.domain(),
                    plan.schema.null_semantics(),
                    plan.snapshot.version(),
                )
                .map_err(|_| ())?;
                let kind = if matches!(plan.selected, SelectedRepresentation::EmptyDomain) {
                    ArtifactKind::EmptyDomain
                } else {
                    ArtifactKind::ValueSet
                };
                (kind, encoded)
            }
            SelectedRepresentation::Bitset(bitset) => {
                let bits = bitset::build_bits(values, *bitset).map_err(|_| ())?;
                let mut payload = Vec::with_capacity(plan.payload_bytes);
                payload.push(bitset.type_tag());
                payload.extend_from_slice(&bitset.min().to_be_bytes());
                payload.extend_from_slice(&bitset.max().to_be_bytes());
                payload.extend_from_slice(&bitset.bit_count().to_be_bytes());
                payload.extend_from_slice(&bits);
                let encoded = encode_physical_leaf(
                    ArtifactKind::Bitset,
                    &plan.schema,
                    plan.snapshot.version(),
                    contains_null,
                    None,
                    &payload,
                )
                .map_err(|_| ())?;
                (ArtifactKind::Bitset, encoded)
            }
            SelectedRepresentation::Bloom(contract) => {
                let mut frame = Vec::with_capacity(plan.max_scalar_frame_bytes);
                let (bit_count, bits) =
                    bloom::build_bits(values, contract, &mut frame).map_err(|_| ())?;
                let mut payload = Vec::with_capacity(plan.payload_bytes);
                payload.extend_from_slice(&contract.algorithm_version().to_be_bytes());
                payload.extend_from_slice(&contract.scalar_framing_version().to_be_bytes());
                payload.extend_from_slice(&contract.seed().to_be_bytes());
                payload.extend_from_slice(&contract.bits_per_key().to_be_bytes());
                payload.extend_from_slice(&contract.hash_count().to_be_bytes());
                payload
                    .extend_from_slice(&u64::try_from(values.len()).map_err(|_| ())?.to_be_bytes());
                payload.extend_from_slice(&bit_count.to_be_bytes());
                payload.extend_from_slice(&bits);
                let encoded = encode_physical_leaf(
                    ArtifactKind::Bloom,
                    &plan.schema,
                    plan.snapshot.version(),
                    contains_null,
                    Some(contract.digest()),
                    &payload,
                )
                .map_err(|_| ())?;
                (ArtifactKind::Bloom, encoded)
            }
            SelectedRepresentation::Unsupported(_) | SelectedRepresentation::ResourceLimit => {
                return Err(());
            }
        };
        if encoded.len() != plan.leaf_encoded_bytes {
            return Err(());
        }
        let membership_index = if matches!(kind, ArtifactKind::ValueSet | ArtifactKind::EmptyDomain)
        {
            let index_plan = codec::inspect_membership_index(&encoded).map_err(|_| ())?;
            if index_plan.heap_bytes().map_err(|_| ())? != plan.resident_index_bytes {
                return Err(());
            }
            Some(codec::build_membership_index(&encoded, &index_plan).map_err(|_| ())?)
        } else {
            None
        };
        let encoded: Arc<[u8]> = encoded.into();
        let artifact = Arc::new(
            if let Some(index) = membership_index {
                PhysicalArtifact::from_shared_indexed_retained_bytes(
                    kind,
                    plan.schema.digest(),
                    plan.snapshot.version(),
                    contains_null,
                    encoded,
                    index,
                    artifact_footprint,
                    total_footprint,
                    retained.clone(),
                )
            } else {
                PhysicalArtifact::from_shared_retained_bytes(
                    kind,
                    plan.schema.digest(),
                    plan.snapshot.version(),
                    contains_null,
                    encoded,
                    artifact_footprint,
                    total_footprint,
                    retained.clone(),
                )
            }
            .map_err(|_| ())?,
        );
        let bundle = ArtifactBundle::new_retained(
            plan.snapshot.channel_id(),
            plan.snapshot.version(),
            &plan.profile,
            vec![(kind, artifact)],
            plan.max_artifact_bytes,
            retained,
        )
        .map_err(|_| ())?;
        Ok(Arc::new(bundle))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
        PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    use super::bloom::BloomHashContract;
    use super::{
        MaterializationError, MaterializationOutcome, Materializer, SelectedRepresentation,
        UnavailableReason, validate_membership_kind,
    };

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    #[derive(Default)]
    struct CountingMemory {
        current: AtomicUsize,
        peak: AtomicUsize,
    }

    #[derive(Default)]
    struct SingleReservationMemory {
        calls: AtomicUsize,
        current: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for SingleReservationMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) >= 2 {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.current.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let old = self.current.fetch_sub(bytes, Ordering::SeqCst);
            assert!(old >= bytes);
        }
    }

    impl RuntimeFilterMemoryAccount for CountingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            let current = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.peak.fetch_max(current, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    fn materialize(
        values: MembershipValues,
        contains_null: bool,
        null_semantics: NullSemantics,
        accepted_kinds: BTreeSet<ArtifactKind>,
        max_artifact_bytes: usize,
    ) -> MaterializationOutcome {
        let schema = ArtifactMembershipSchema::new(&values.data_type(), null_semantics).unwrap();
        let policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1).unwrap();
        let bloom_contract = accepted_kinds
            .contains(&ArtifactKind::Bloom)
            .then(|| BloomHashContract::new(&schema, policy).unwrap().digest());
        let profile = ConsumerArtifactProfile::new(accepted_kinds, bloom_contract).unwrap();
        let snapshot = LogicalSnapshot::first(
            ChannelId::new(7),
            ReducedMembershipDomain::new(values, contains_null),
            Default::default(),
        );
        let plan = Materializer::plan(
            Arc::new(snapshot),
            &schema,
            &profile,
            policy,
            max_artifact_bytes,
        )
        .unwrap();
        Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            Arc::new(UnlimitedMemory),
        )
    }

    #[test]
    fn range_kind_is_reserved_but_membership_materialization_is_unsupported() {
        assert_eq!(
            validate_membership_kind(ArtifactKind::Range),
            Err(MaterializationError::UnsupportedRange)
        );
    }

    #[test]
    fn small_sparse_domain_selects_value_set() {
        let outcome = materialize(
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
        );
        assert_eq!(outcome.published_kind(), Some(ArtifactKind::ValueSet));
    }

    #[test]
    fn dense_whitelisted_integral_domain_selects_smaller_exact_bitset() {
        let outcome = materialize(
            MembershipValues::int64(100..164),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::EmptyDomain,
            ]),
            4096,
        );
        assert_eq!(outcome.published_kind(), Some(ArtifactKind::Bitset));
    }

    #[test]
    fn exact_candidates_over_budget_select_deterministic_bloom() {
        let kinds = BTreeSet::from([
            ArtifactKind::ValueSet,
            ArtifactKind::Bitset,
            ArtifactKind::Bloom,
            ArtifactKind::EmptyDomain,
        ]);
        let first = materialize(
            MembershipValues::int64((0..128).map(|value| value * 1_000_000)),
            false,
            NullSemantics::NeverMatches,
            kinds.clone(),
            512,
        );
        let second = materialize(
            MembershipValues::int64((0..128).rev().map(|value| value * 1_000_000)),
            false,
            NullSemantics::NeverMatches,
            kinds,
            512,
        );
        assert_eq!(first.published_kind(), Some(ArtifactKind::Bloom));
        assert_eq!(first.published_digest(), second.published_digest());
    }

    #[test]
    fn all_sound_candidates_over_budget_is_profile_local_resource_limit() {
        let outcome = materialize(
            MembershipValues::utf8(["a long value", "another long value"]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            8,
        );
        assert!(matches!(outcome, MaterializationOutcome::Unavailable(_)));
    }

    #[test]
    fn membership_budget_failure_never_falls_back_to_range() {
        let outcome = materialize(
            MembershipValues::utf8(["a long value", "another long value"]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Range,
                ArtifactKind::EmptyDomain,
            ]),
            8,
        );
        assert!(matches!(outcome, MaterializationOutcome::Unavailable(_)));
    }

    #[test]
    fn materializer_never_turns_failure_or_null_only_into_empty_domain() {
        let null_only = materialize(
            MembershipValues::int64([]),
            true,
            NullSemantics::NullSafeEqual,
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            4096,
        );
        assert_eq!(null_only.published_kind(), Some(ArtifactKind::ValueSet));

        let failure = materialize(
            MembershipValues::utf8(["too-large"]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            1,
        );
        assert!(matches!(failure, MaterializationOutcome::Unavailable(_)));
        assert_ne!(failure.published_kind(), Some(ArtifactKind::EmptyDomain));
    }

    #[test]
    fn logical_version_is_preserved_in_materialized_bundle() {
        let outcome = materialize(
            MembershipValues::int32([1, 2, 3]),
            false,
            NullSemantics::NeverMatches,
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            4096,
        );
        assert_eq!(outcome.published_version(), Some(LogicalVersion::FIRST));
        assert_eq!(outcome.published_channel(), Some(ChannelId::new(7)));
        assert_eq!(DataType::Int32, MembershipValues::int32([1]).data_type());
    }

    #[test]
    fn full_bundle_budget_applies_to_empty_domain_at_the_exact_boundary() {
        let values = MembershipValues::int64([]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let snapshot = || {
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(8),
                ReducedMembershipDomain::new(values.clone(), false),
                Default::default(),
            ))
        };
        let probe = Materializer::plan(snapshot(), &schema, &profile, policy, usize::MAX).unwrap();
        let exact_bundle_bytes =
            ArtifactBundle::canonical_encoded_len_for_single_artifact(probe.leaf_encoded_bytes)
                .unwrap();
        let below = Materializer::plan(
            snapshot(),
            &schema,
            &profile,
            policy,
            exact_bundle_bytes - 1,
        )
        .unwrap();
        assert!(matches!(
            below.selected,
            SelectedRepresentation::ResourceLimit
        ));
        let exact =
            Materializer::plan(snapshot(), &schema, &profile, policy, exact_bundle_bytes).unwrap();
        assert!(matches!(
            exact.selected,
            SelectedRepresentation::EmptyDomain
        ));
    }

    #[test]
    fn selected_exact_reservation_failure_does_not_fallback_to_bloom() {
        let values = MembershipValues::int64([1, 1_000_000]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let bloom = BloomHashContract::new(&schema, policy).unwrap();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bloom,
                ArtifactKind::EmptyDomain,
            ]),
            Some(bloom.digest()),
        )
        .unwrap();
        let plan = Materializer::plan(
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(9),
                ReducedMembershipDomain::new(values, false),
                Default::default(),
            )),
            &schema,
            &profile,
            policy,
            4096,
        )
        .unwrap();
        assert!(matches!(plan.selected, SelectedRepresentation::ValueSet));
        let outcome = Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1)),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            Arc::new(UnlimitedMemory),
        );
        assert!(matches!(outcome, MaterializationOutcome::Unavailable(_)));
    }

    #[test]
    fn logical_and_artifact_memory_overlap_then_clone_and_drop_return_to_baseline() {
        let account = Arc::new(CountingMemory::default());
        let logical = RetainedMemoryReservation::new(account.clone(), 128);
        let values = MembershipValues::int64([1, 2, 3, 4]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let plan = Materializer::plan(
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(10),
                ReducedMembershipDomain::new(values, false),
                logical,
            )),
            &schema,
            &profile,
            policy,
            4096,
        )
        .unwrap();
        let budget = Arc::new(ArtifactRetainedBudget::new(1 << 20));
        let scratch = Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap());
        let outcome =
            Materializer::materialize(plan, budget.clone(), scratch.clone(), account.clone());
        let MaterializationOutcome::Published(bundle) = outcome else {
            panic!("materialization must publish");
        };
        assert!(account.peak.load(Ordering::SeqCst) > 128);
        assert_eq!(scratch.retained_bytes(), 0);
        assert_eq!(
            account.current.load(Ordering::SeqCst),
            budget.retained_bytes()
        );
        let clone = bundle.clone();
        assert_eq!(
            account.current.load(Ordering::SeqCst),
            budget.retained_bytes()
        );
        drop(bundle);
        assert_ne!(account.current.load(Ordering::SeqCst), 0);
        drop(clone);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn complete_footprint_uses_one_atomic_reservation_and_survives_artifact_escape() {
        let values = MembershipValues::int64([1, 2, 3, 4]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let plan = Materializer::plan(
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(11),
                ReducedMembershipDomain::new(values, false),
                Default::default(),
            )),
            &schema,
            &profile,
            policy,
            4096,
        )
        .unwrap();
        let total_footprint =
            PhysicalArtifact::accounted_resident_component_bytes(plan.leaf_encoded_bytes)
                .unwrap()
                .checked_add(ArtifactBundle::accounted_resident_overhead(&profile, 1).unwrap())
                .unwrap();
        let budget = Arc::new(ArtifactRetainedBudget::new(total_footprint));
        let account = Arc::new(SingleReservationMemory::default());

        let MaterializationOutcome::Published(bundle) = Materializer::materialize(
            plan,
            budget.clone(),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            account.clone(),
        ) else {
            panic!("the full footprint must be acquired atomically");
        };
        assert_eq!(account.calls.load(Ordering::SeqCst), 2);
        assert_eq!(account.current.load(Ordering::SeqCst), total_footprint);
        assert_eq!(budget.retained_bytes(), total_footprint);

        let artifact = bundle.artifacts()[0].1.clone();
        drop(bundle);
        assert_eq!(account.current.load(Ordering::SeqCst), total_footprint);
        assert_eq!(budget.retained_bytes(), total_footprint);
        drop(artifact);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn utf8_membership_index_is_included_in_local_exact_admission() {
        let values = MembershipValues::utf8(["a", "bb", "ccc"]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Utf8, NullSemantics::NeverMatches).unwrap();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let make_plan = || {
            Materializer::plan(
                Arc::new(LogicalSnapshot::first(
                    ChannelId::new(12),
                    ReducedMembershipDomain::new(values.clone(), false),
                    Default::default(),
                )),
                &schema,
                &profile,
                MaterializationPolicy::for_test(),
                4096,
            )
            .unwrap()
        };
        let plan = make_plan();
        assert_eq!(plan.resident_index_bytes, 3 * std::mem::size_of::<usize>());
        let total = PhysicalArtifact::accounted_indexed_resident_component_bytes(
            plan.leaf_encoded_bytes,
            plan.resident_index_bytes,
        )
        .unwrap()
        .checked_add(ArtifactBundle::accounted_resident_overhead(&profile, 1).unwrap())
        .unwrap();
        assert!(matches!(
            Materializer::materialize(
                plan,
                Arc::new(ArtifactRetainedBudget::new(total - 1)),
                Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
                Arc::new(UnlimitedMemory),
            ),
            MaterializationOutcome::Unavailable(UnavailableReason::ResourceLimit)
        ));

        let budget = Arc::new(ArtifactRetainedBudget::new(total));
        let MaterializationOutcome::Published(bundle) = Materializer::materialize(
            make_plan(),
            budget.clone(),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            Arc::new(UnlimitedMemory),
        ) else {
            panic!("exact local indexed footprint must publish");
        };
        assert_eq!(budget.retained_bytes(), total);
        assert_eq!(
            bundle.artifacts()[0]
                .1
                .membership_index()
                .unwrap()
                .heap_bytes()
                .unwrap(),
            3 * std::mem::size_of::<usize>()
        );
        drop(bundle);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn concurrent_jobs_reserve_only_one_complete_footprint_at_the_boundary() {
        let values = MembershipValues::int64([1, 2, 3, 4]);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let make_plan = || {
            Materializer::plan(
                Arc::new(LogicalSnapshot::first(
                    ChannelId::new(13),
                    ReducedMembershipDomain::new(values.clone(), false),
                    Default::default(),
                )),
                &schema,
                &profile,
                policy,
                4096,
            )
            .unwrap()
        };
        let first = make_plan();
        let total_footprint =
            PhysicalArtifact::accounted_resident_component_bytes(first.leaf_encoded_bytes)
                .unwrap()
                .checked_add(ArtifactBundle::accounted_resident_overhead(&profile, 1).unwrap())
                .unwrap();
        let second = make_plan();
        let budget = Arc::new(ArtifactRetainedBudget::new(total_footprint));
        let scratch = Arc::new(ArtifactScratchBudget::new(1 << 16, 2 << 16).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let outcomes = std::thread::scope(|scope| {
            let spawn = |plan| {
                let budget = budget.clone();
                let scratch = scratch.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    Materializer::materialize(plan, budget, scratch, Arc::new(UnlimitedMemory))
                })
            };
            let first = spawn(first);
            let second = spawn(second);
            barrier.wait();
            [first.join().unwrap(), second.join().unwrap()]
        });

        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, MaterializationOutcome::Published(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, MaterializationOutcome::Unavailable(_)))
                .count(),
            1
        );
        assert_eq!(budget.retained_bytes(), total_footprint);
        drop(outcomes);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn planning_state_is_inline_and_borrows_install_contracts() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<BloomHashContract>();

        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::for_test();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bloom,
                ArtifactKind::EmptyDomain,
            ]),
            Some(BloomHashContract::new(&schema, policy).unwrap().digest()),
        )
        .unwrap();
        let plan = Materializer::plan(
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(12),
                ReducedMembershipDomain::new(MembershipValues::int64([1, 1_000_000]), false),
                Default::default(),
            )),
            &schema,
            &profile,
            policy,
            4096,
        )
        .unwrap();

        assert!(std::ptr::eq(plan.schema, &schema));
        assert!(std::ptr::eq(plan.profile, &profile));
    }
}
