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

//! Backend admission and materialization of Execution-owned contributions.

pub mod bitset;
pub mod bloom;
pub mod range;

use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    LogicalVersion, RuntimeFilterMembershipSchema, contribution::ValueDomainDelta,
};

use crate::runtime_filter::artifact as worker_artifact;
use crate::runtime_filter::artifact::{
    ArtifactBundle, ArtifactContractError, ArtifactKind, ConsumerArtifactProfile,
};
use crate::runtime_filter::codec::leaf::{self, ArtifactCodecError, ArtifactDecodeExpectations};

use self::{bitset::BitsetPlan, bloom::BloomHashContract};

#[derive(Clone, Debug)]
pub struct MaterializationAdmission {
    max_artifact_bytes: usize,
    retained_budget: Arc<worker_artifact::ArtifactRetainedBudget>,
    scratch_budget: Arc<worker_artifact::ArtifactScratchBudget>,
}

/// Frozen physical Bloom parameters.  The Worker profile digest remains the
/// authority: callers must prove the derived contract digest matches it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BloomMaterializationPolicy {
    pub algorithm_version: u16,
    pub seed: u64,
    pub bits_per_key: u64,
    pub hash_count: u32,
}

impl MaterializationAdmission {
    pub fn new(max_artifact_bytes: usize) -> Self {
        Self {
            max_artifact_bytes,
            retained_budget: Arc::new(worker_artifact::ArtifactRetainedBudget::new(
                max_artifact_bytes,
            )),
            scratch_budget: Arc::new(worker_artifact::ArtifactScratchBudget::new(
                max_artifact_bytes,
            )),
        }
    }
    pub const fn max_artifact_bytes(&self) -> usize {
        self.max_artifact_bytes
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn with_retained_budget(
        max_artifact_bytes: usize,
        retained_budget: Arc<worker_artifact::ArtifactRetainedBudget>,
    ) -> Self {
        Self {
            max_artifact_bytes,
            retained_budget,
            scratch_budget: Arc::new(worker_artifact::ArtifactScratchBudget::new(
                max_artifact_bytes,
            )),
        }
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn with_budgets(
        max_artifact_bytes: usize,
        retained_budget: Arc<worker_artifact::ArtifactRetainedBudget>,
        scratch_budget: Arc<worker_artifact::ArtifactScratchBudget>,
    ) -> Self {
        Self {
            max_artifact_bytes,
            retained_budget,
            scratch_budget,
        }
    }
    fn retain(
        &self,
        profile: &ConsumerArtifactProfile,
        artifacts: &[(ArtifactKind, Arc<worker_artifact::PhysicalArtifact>)],
    ) -> Result<Arc<worker_artifact::ArtifactRetention>, ArtifactContractError> {
        let bytes = ArtifactBundle::accounted_resident_bytes(profile, artifacts)?;
        self.retained_budget.try_acquire(bytes).map(Arc::new)
    }
    pub(super) fn reserve_scratch(
        &self,
        bytes: usize,
    ) -> Result<Arc<worker_artifact::ArtifactScratchReservation>, ArtifactContractError> {
        self.scratch_budget.try_acquire(bytes).map(Arc::new)
    }
}

#[derive(Clone, Debug)]
pub enum MaterializationOutcome {
    Published(Arc<ArtifactBundle>),
    Unsupported(NoAcceptedRepresentation),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    Unavailable(MaterializationUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoAcceptedRepresentation {
    NoAcceptedRepresentation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationUnavailable {
    ResourceLimit,
    MaterializationFailed,
}

pub fn materialize_membership(
    channel_id: u32,
    domain: &ValueDomainDelta,
    schema: &RuntimeFilterMembershipSchema,
    logical_version: LogicalVersion,
    profile: &ConsumerArtifactProfile,
    admission: MaterializationAdmission,
) -> MaterializationOutcome {
    let required = if domain.values().is_empty() && !domain.contains_null() {
        ArtifactKind::EmptyDomain
    } else {
        ArtifactKind::ValueSet
    };
    if !profile.accepts(required) {
        return MaterializationOutcome::Unsupported(
            NoAcceptedRepresentation::NoAcceptedRepresentation,
        );
    }
    let leaf = match leaf::encode_membership_leaf(domain, schema, logical_version) {
        Ok(leaf) => leaf,
        Err(
            ArtifactCodecError::LengthOverflow
            | ArtifactCodecError::ResourceLimit
            | ArtifactCodecError::EncodedSizeExceeded,
        ) => return MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit),
        Err(_) => {
            return MaterializationOutcome::Unavailable(
                MaterializationUnavailable::MaterializationFailed,
            );
        }
    };
    let _scratch = match admission.reserve_scratch(leaf.len()) {
        Ok(reservation) => reservation,
        Err(_) => {
            return MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit);
        }
    };
    let artifact = match leaf::decode_leaf(
        &leaf,
        ArtifactDecodeExpectations {
            expected_kind: required,
            schema,
            expected_logical_version: logical_version,
            expected_hash_contract: None,
        },
        admission.max_artifact_bytes(),
    ) {
        Ok(artifact) => artifact,
        Err(
            ArtifactCodecError::EncodedSizeExceeded
            | ArtifactCodecError::LengthOverflow
            | ArtifactCodecError::ResourceLimit,
        ) => return MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit),
        Err(_) => {
            return MaterializationOutcome::Unavailable(
                MaterializationUnavailable::MaterializationFailed,
            );
        }
    };
    match retain_bundle(
        channel_id,
        logical_version,
        profile,
        vec![(required, artifact)],
        &admission,
    ) {
        Ok(bundle) => MaterializationOutcome::Published(Arc::new(bundle)),
        Err(
            ArtifactContractError::EncodedSizeExceeded
            | ArtifactContractError::LengthOverflow
            | ArtifactContractError::ResidentSizeOverflow
            | ArtifactContractError::RetentionCapacityExceeded,
        ) => MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit),
        Err(_) => {
            MaterializationOutcome::Unavailable(MaterializationUnavailable::MaterializationFailed)
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub fn materialize_membership_with_policy(
    channel_id: u32,
    domain: &ValueDomainDelta,
    schema: &RuntimeFilterMembershipSchema,
    logical_version: LogicalVersion,
    profile: &ConsumerArtifactProfile,
    admission: MaterializationAdmission,
    bloom_policy: BloomMaterializationPolicy,
) -> MaterializationOutcome {
    if domain.values().is_empty() {
        return materialize_membership(
            channel_id,
            domain,
            schema,
            logical_version,
            profile,
            admission,
        );
    }
    let mut exact = Vec::new();
    if profile.accepts(ArtifactKind::ValueSet)
        && let Ok(bytes) = leaf::encode_membership_leaf(domain, schema, logical_version)
    {
        exact.push((ArtifactKind::ValueSet, bytes));
    }
    if profile.accepts(ArtifactKind::Bitset)
        && let Ok(plan) = BitsetPlan::new(domain.values())
        && let Ok(bits) = bitset::build_bits(domain.values(), plan)
    {
        let mut payload = Vec::with_capacity(25 + bits.len());
        payload.push(plan.type_tag());
        payload.extend_from_slice(&plan.min().to_be_bytes());
        payload.extend_from_slice(&plan.max().to_be_bytes());
        payload.extend_from_slice(&plan.bit_count().to_be_bytes());
        payload.extend_from_slice(&bits);
        if let Ok(bytes) = leaf::encode_physical_leaf(
            ArtifactKind::Bitset,
            schema,
            logical_version,
            domain.contains_null(),
            None,
            &payload,
        ) {
            exact.push((ArtifactKind::Bitset, bytes));
        }
    }
    if let Some((kind, bytes)) = exact.into_iter().min_by_key(|(_, bytes)| bytes.len()) {
        return publish_leaf(
            channel_id,
            kind,
            bytes,
            schema,
            logical_version,
            profile,
            admission,
        );
    }
    if !profile.accepts(ArtifactKind::Bloom) {
        return MaterializationOutcome::Unsupported(
            NoAcceptedRepresentation::NoAcceptedRepresentation,
        );
    }
    let contract = match BloomHashContract::from_fields(
        worker_artifact::ArtifactSchemaDigest::new(schema.digest()),
        bloom_policy.algorithm_version,
        1,
        bloom_policy.seed,
        bloom_policy.bits_per_key,
        bloom_policy.hash_count,
    ) {
        Ok(contract) if profile.bloom_hash_contract() == Some(contract.digest()) => contract,
        Ok(_) => {
            return MaterializationOutcome::Unavailable(
                MaterializationUnavailable::MaterializationFailed,
            );
        }
        Err(_) => {
            return MaterializationOutcome::Unavailable(
                MaterializationUnavailable::MaterializationFailed,
            );
        }
    };
    let (bit_count, bits) = match bloom::build_bits(domain.values(), contract) {
        Ok(value) => value,
        Err(_) => {
            return MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit);
        }
    };
    let mut payload = Vec::with_capacity(bloom::METADATA_BYTES + bits.len());
    payload.extend_from_slice(&contract.algorithm_version().to_be_bytes());
    payload.extend_from_slice(&contract.scalar_framing_version().to_be_bytes());
    payload.extend_from_slice(&contract.seed().to_be_bytes());
    payload.extend_from_slice(&contract.bits_per_key().to_be_bytes());
    payload.extend_from_slice(&contract.hash_count().to_be_bytes());
    payload.extend_from_slice(&(membership_value_count(domain.values()) as u64).to_be_bytes());
    payload.extend_from_slice(&bit_count.to_be_bytes());
    payload.extend_from_slice(&bits);
    match leaf::encode_physical_leaf(
        ArtifactKind::Bloom,
        schema,
        logical_version,
        domain.contains_null(),
        Some(contract.digest()),
        &payload,
    ) {
        Ok(bytes) => publish_leaf(
            channel_id,
            ArtifactKind::Bloom,
            bytes,
            schema,
            logical_version,
            profile,
            admission,
        ),
        Err(_) => {
            MaterializationOutcome::Unavailable(MaterializationUnavailable::MaterializationFailed)
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn membership_value_count(
    values: &novarocks_execution::runtime_filter::contribution::MembershipValues,
) -> usize {
    match values {
        novarocks_execution::runtime_filter::contribution::MembershipValues::Boolean(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Int8(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Int16(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Int32(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Int64(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Temporal {
            values,
            ..
        } => values.len(),
        novarocks_execution::runtime_filter::contribution::MembershipValues::LargeInt(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Float32(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Float64(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Utf8(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Date32(values) => {
            values.len()
        }
        novarocks_execution::runtime_filter::contribution::MembershipValues::Timestamp {
            values,
            ..
        } => values.len(),
        novarocks_execution::runtime_filter::contribution::MembershipValues::Decimal128 {
            values,
            ..
        } => values.len(),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn publish_leaf(
    channel_id: u32,
    kind: ArtifactKind,
    bytes: Vec<u8>,
    schema: &RuntimeFilterMembershipSchema,
    logical_version: LogicalVersion,
    profile: &ConsumerArtifactProfile,
    admission: MaterializationAdmission,
) -> MaterializationOutcome {
    let _scratch = match admission.reserve_scratch(bytes.len()) {
        Ok(reservation) => reservation,
        Err(_) => {
            return MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit);
        }
    };
    let hash = (kind == ArtifactKind::Bloom)
        .then(|| profile.bloom_hash_contract())
        .flatten();
    let artifact = match leaf::decode_leaf(
        &bytes,
        ArtifactDecodeExpectations {
            expected_kind: kind,
            schema,
            expected_logical_version: logical_version,
            expected_hash_contract: hash,
        },
        admission.max_artifact_bytes(),
    ) {
        Ok(artifact) => artifact,
        Err(_) => {
            return MaterializationOutcome::Unavailable(
                MaterializationUnavailable::MaterializationFailed,
            );
        }
    };
    match retain_bundle(
        channel_id,
        logical_version,
        profile,
        vec![(kind, artifact)],
        &admission,
    ) {
        Ok(bundle) => MaterializationOutcome::Published(Arc::new(bundle)),
        Err(
            ArtifactContractError::EncodedSizeExceeded
            | ArtifactContractError::LengthOverflow
            | ArtifactContractError::ResidentSizeOverflow
            | ArtifactContractError::RetentionCapacityExceeded,
        ) => MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit),
        Err(_) => {
            MaterializationOutcome::Unavailable(MaterializationUnavailable::MaterializationFailed)
        }
    }
}

pub(super) fn retain_bundle(
    channel_id: u32,
    logical_version: LogicalVersion,
    profile: &ConsumerArtifactProfile,
    artifacts: Vec<(ArtifactKind, Arc<worker_artifact::PhysicalArtifact>)>,
    admission: &MaterializationAdmission,
) -> Result<ArtifactBundle, ArtifactContractError> {
    let retention = admission.retain(profile, &artifacts)?;
    let artifacts = artifacts
        .into_iter()
        .map(|(kind, artifact)| {
            let artifact = Arc::unwrap_or_clone(artifact).with_retention(retention.clone());
            (kind, Arc::new(artifact))
        })
        .collect();
    ArtifactBundle::new_retained(
        channel_id,
        logical_version,
        profile,
        artifacts,
        admission.max_artifact_bytes(),
        retention,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterNullSemantics, contribution::MembershipValues,
    };
    use std::collections::BTreeSet;

    #[test]
    fn admission_materializes_empty_domain_without_logical_reimplementation() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::EmptyDomain]), None)
                .unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([]), false);
        let MaterializationOutcome::Published(bundle) = materialize_membership(
            7,
            &domain,
            &schema,
            LogicalVersion::FIRST,
            &profile,
            MaterializationAdmission::new(1024),
        ) else {
            panic!("empty domain must be published when accepted")
        };
        assert!(matches!(
            bundle.artifacts(),
            [(ArtifactKind::EmptyDomain, _)]
        ));
    }

    #[test]
    fn admission_does_not_downgrade_an_unaccepted_representation() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bloom]),
            Some(worker_artifact::HashContractDigest::new([9; 32])),
        )
        .unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        assert!(matches!(
            materialize_membership(
                7,
                &domain,
                &schema,
                LogicalVersion::FIRST,
                &profile,
                MaterializationAdmission::new(1024)
            ),
            MaterializationOutcome::Unsupported(_)
        ));
    }

    #[test]
    fn retained_budget_exhaustion_is_typed_resource_unavailable() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::EmptyDomain]), None)
                .unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let budget = Arc::new(worker_artifact::ArtifactRetainedBudget::new(1));
        let domain = ValueDomainDelta::new(MembershipValues::int64([]), false);
        assert!(matches!(
            materialize_membership(
                7,
                &domain,
                &schema,
                LogicalVersion::FIRST,
                &profile,
                MaterializationAdmission::with_retained_budget(4096, budget),
            ),
            MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit)
        ));
    }

    #[test]
    fn scratch_budget_exhaustion_is_typed_resource_unavailable() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::EmptyDomain]), None)
                .unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([]), false);
        assert!(matches!(
            materialize_membership(
                7,
                &domain,
                &schema,
                LogicalVersion::FIRST,
                &profile,
                MaterializationAdmission::with_budgets(
                    4096,
                    Arc::new(worker_artifact::ArtifactRetainedBudget::new(4096)),
                    Arc::new(worker_artifact::ArtifactScratchBudget::new(1)),
                ),
            ),
            MaterializationOutcome::Unavailable(MaterializationUnavailable::ResourceLimit)
        ));
    }
}
