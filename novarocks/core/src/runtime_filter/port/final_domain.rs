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

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CompletionFenceKind, NullSemantics,
};

use super::artifact::ArtifactMembershipSchema;
use super::identity::{DeploymentEpoch, ProducerSequence, ProducerStreamId};
use super::producer::{RuntimeContractViolation, RuntimeContractViolationKind};
use super::value_domain::ValueDomainDelta;

const CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.completion-fence-contract";
const CONTRACT_VERSION: u16 = 1;
#[cfg(any(test, feature = "runtime-filter-test-support"))]
const AUTHORITY_SCOPE_DOMAIN: &[u8] = b"novarocks.runtime-filter.completion-fence-authority";
#[cfg(any(test, feature = "runtime-filter-test-support"))]
const AUTHORITY_SCOPE_VERSION: u16 = 1;
const FENCE_DOMAIN: &[u8] = b"novarocks.runtime-filter.completion-fence";
const FENCE_VERSION: u16 = 1;
const SHARD_DOMAIN: &[u8] = b"novarocks.runtime-filter.final-domain-shard";
const SHARD_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompletionFenceContractDigest([u8; 32]);

impl CompletionFenceContractDigest {
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalDomainError {
    ContractRequiresNullSafeEqual,
    FrozenProofMismatch,
    UnauthorizedBinding,
    UnauthorizedFragmentInstance,
    ContractMismatch,
    FenceIntegrityMismatch,
    DomainSchemaMismatch,
}

impl fmt::Display for FinalDomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let detail = match self {
            Self::ContractRequiresNullSafeEqual => {
                "fenced final domains require null-safe equality"
            }
            Self::FrozenProofMismatch => "committed-domain proof does not match fence authority",
            Self::UnauthorizedBinding => "producer stream binding does not match fence authority",
            Self::UnauthorizedFragmentInstance => {
                "producer stream fragment instance does not match fence authority"
            }
            Self::ContractMismatch => "completion fence contract mismatch",
            Self::FenceIntegrityMismatch => "completion fence digest mismatch",
            Self::DomainSchemaMismatch => "final domain schema does not match fence contract",
        };
        write!(formatter, "invalid final domain: {detail}")
    }
}

impl Error for FinalDomainError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCompletionFenceContract {
    query_id: UniqueId,
    deployment_epoch: DeploymentEpoch,
    channel_id: ChannelId,
    fence_kind: CompletionFenceKind,
    membership_schema: ArtifactMembershipSchema,
    digest: CompletionFenceContractDigest,
}

impl RuntimeCompletionFenceContract {
    pub fn try_from_install(
        query_id: UniqueId,
        deployment_epoch: DeploymentEpoch,
        channel_id: ChannelId,
        fence_kind: CompletionFenceKind,
        membership_schema: &ArtifactMembershipSchema,
    ) -> Result<Self, FinalDomainError> {
        if membership_schema.null_semantics() != NullSemantics::NullSafeEqual {
            return Err(FinalDomainError::ContractRequiresNullSafeEqual);
        }
        let mut canonical = Sha256::new();
        canonical.update(CONTRACT_DOMAIN);
        canonical.update(CONTRACT_VERSION.to_be_bytes());
        canonical.update(query_id.high().to_be_bytes());
        canonical.update(query_id.low().to_be_bytes());
        canonical.update(deployment_epoch.get().to_be_bytes());
        canonical.update(channel_id.get().to_be_bytes());
        canonical.update([fence_kind_tag(fence_kind)]);
        canonical.update(membership_schema.digest().bytes());
        let digest = CompletionFenceContractDigest(canonical.finalize().into());
        Ok(Self {
            query_id,
            deployment_epoch,
            channel_id,
            fence_kind,
            membership_schema: membership_schema.clone(),
            digest,
        })
    }

    pub const fn digest(&self) -> CompletionFenceContractDigest {
        self.digest
    }

    pub const fn membership_schema(&self) -> &ArtifactMembershipSchema {
        &self.membership_schema
    }
}

fn fence_kind_tag(kind: CompletionFenceKind) -> u8 {
    match kind {
        CompletionFenceKind::CommittedDomainFrozen => 1,
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
#[derive(Debug, Eq, PartialEq)]
pub struct CommittedDomainFrozenProof {
    authority_scope_digest: [u8; 32],
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
#[derive(Debug)]
pub struct CompletionFenceAuthority {
    contract: Arc<RuntimeCompletionFenceContract>,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    scope_digest: [u8; 32],
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
impl CompletionFenceAuthority {
    pub fn try_new(
        contract: Arc<RuntimeCompletionFenceContract>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<Self, FinalDomainError> {
        let scope_digest =
            authority_scope_digest(contract.digest(), binding_id, fragment_instance_id);
        Ok(Self {
            contract,
            binding_id,
            fragment_instance_id,
            scope_digest,
        })
    }

    pub fn issue(
        &self,
        proof: &CommittedDomainFrozenProof,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    ) -> Result<CompletionFence, FinalDomainError> {
        if proof.authority_scope_digest != self.scope_digest {
            return Err(FinalDomainError::FrozenProofMismatch);
        }
        if stream.binding_id() != self.binding_id {
            return Err(FinalDomainError::UnauthorizedBinding);
        }
        if stream.fragment_instance_id() != self.fragment_instance_id {
            return Err(FinalDomainError::UnauthorizedFragmentInstance);
        }
        Ok(CompletionFence::issue(
            self.contract.digest(),
            stream,
            sequence,
        ))
    }

    fn frozen_proof_for_test(&self) -> CommittedDomainFrozenProof {
        CommittedDomainFrozenProof {
            authority_scope_digest: self.scope_digest,
        }
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
#[derive(Debug)]
pub struct CollectingFinalDomainTestIssuer {
    authority: CompletionFenceAuthority,
    open_drivers: u32,
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
#[derive(Debug)]
pub enum FinalDomainTestIssuerTransition {
    Collecting(CollectingFinalDomainTestIssuer),
    Frozen(FrozenFinalDomainTestIssuer),
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
impl CollectingFinalDomainTestIssuer {
    pub fn new(authority: CompletionFenceAuthority, open_drivers: u32) -> Self {
        assert!(open_drivers > 0, "a collecting issuer needs an open driver");
        Self {
            authority,
            open_drivers,
        }
    }

    pub fn close_driver(mut self) -> FinalDomainTestIssuerTransition {
        self.open_drivers -= 1;
        if self.open_drivers == 0 {
            let proof = self.authority.frozen_proof_for_test();
            FinalDomainTestIssuerTransition::Frozen(FrozenFinalDomainTestIssuer {
                authority: self.authority,
                proof,
            })
        } else {
            FinalDomainTestIssuerTransition::Collecting(self)
        }
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
#[derive(Debug)]
pub struct FrozenFinalDomainTestIssuer {
    authority: CompletionFenceAuthority,
    proof: CommittedDomainFrozenProof,
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
impl FrozenFinalDomainTestIssuer {
    pub fn issue(
        &self,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    ) -> Result<CompletionFence, FinalDomainError> {
        self.authority.issue(&self.proof, stream, sequence)
    }

    pub fn issue_shard(
        &self,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> Result<FinalDomainShard, FinalDomainError> {
        let fence = self.issue(stream, sequence)?;
        FinalDomainShard::try_new(&self.authority.contract, fence, domain)
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
fn authority_scope_digest(
    contract_digest: CompletionFenceContractDigest,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(AUTHORITY_SCOPE_DOMAIN);
    canonical.update(AUTHORITY_SCOPE_VERSION.to_be_bytes());
    canonical.update(contract_digest.bytes());
    canonical.update(binding_id.get().to_be_bytes());
    canonical.update(fragment_instance_id.high().to_be_bytes());
    canonical.update(fragment_instance_id.low().to_be_bytes());
    canonical.finalize().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionFence {
    contract_digest: CompletionFenceContractDigest,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
    digest: [u8; 32],
}

impl CompletionFence {
    fn issue(
        contract_digest: CompletionFenceContractDigest,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    ) -> Self {
        Self {
            contract_digest,
            stream,
            sequence,
            digest: fence_digest(contract_digest, stream, sequence),
        }
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn try_from_remote_codec(
        contract_digest: CompletionFenceContractDigest,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        encoded_digest: [u8; 32],
    ) -> Result<Self, FinalDomainError> {
        let expected_digest = fence_digest(contract_digest, stream, sequence);
        if encoded_digest != expected_digest {
            return Err(FinalDomainError::FenceIntegrityMismatch);
        }
        Ok(Self {
            contract_digest,
            stream,
            sequence,
            digest: expected_digest,
        })
    }

    const fn canonical_bytes(&self) -> usize {
        FENCE_DOMAIN.len()
            + size_of::<u16>()
            + 32
            + size_of::<u32>()
            + size_of::<i64>() * 2
            + size_of::<u32>()
            + size_of::<u64>()
    }

    fn has_valid_digest(&self) -> bool {
        fence_digest(self.contract_digest, self.stream, self.sequence) == self.digest
    }
}

fn fence_digest(
    contract_digest: CompletionFenceContractDigest,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(FENCE_DOMAIN);
    canonical.update(FENCE_VERSION.to_be_bytes());
    canonical.update(contract_digest.bytes());
    canonical.update(stream.binding_id().get().to_be_bytes());
    canonical.update(stream.fragment_instance_id().high().to_be_bytes());
    canonical.update(stream.fragment_instance_id().low().to_be_bytes());
    canonical.update(stream.partition_id().get().to_be_bytes());
    canonical.update(sequence.get().to_be_bytes());
    canonical.finalize().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalDomainShard {
    fence: CompletionFence,
    domain: ValueDomainDelta,
    replay_digest: [u8; 32],
}

/// Backend-owned authority that binds a frozen final-domain payload to the
/// producer stream allowed to issue it. The Core contract validates only the
/// immutable proof and never names the participant service that owns it.
pub trait FinalDomainIssuanceAuthorizer: Send + Sync {
    fn authorizes_final_domain(&self, stream: ProducerStreamId, domain: &ValueDomainDelta) -> bool;
}

impl FinalDomainShard {
    pub fn issue_for_service(
        permit: &impl FinalDomainIssuanceAuthorizer,
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> Result<Self, FinalDomainError> {
        if !permit.authorizes_final_domain(stream, &domain) {
            return Err(FinalDomainError::FrozenProofMismatch);
        }
        let fence = CompletionFence::issue(contract.digest(), stream, sequence);
        Self::try_new(contract, fence, domain)
    }

    pub fn try_new(
        contract: &RuntimeCompletionFenceContract,
        fence: CompletionFence,
        domain: ValueDomainDelta,
    ) -> Result<Self, FinalDomainError> {
        if fence.contract_digest != contract.digest() {
            return Err(FinalDomainError::ContractMismatch);
        }
        if !fence.has_valid_digest() {
            return Err(FinalDomainError::FenceIntegrityMismatch);
        }
        if !domain.matches_data_type(contract.membership_schema.data_type()) {
            return Err(FinalDomainError::DomainSchemaMismatch);
        }
        let replay_digest = final_domain_replay_digest(&fence, &domain);
        Ok(Self {
            fence,
            domain,
            replay_digest,
        })
    }

    pub fn verify_scope(
        &self,
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    ) -> Result<(), RuntimeContractViolation> {
        if self.fence.contract_digest != contract.digest() || !self.fence.has_valid_digest() {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::TypeMismatch,
                "completion fence contract or digest mismatch",
            ));
        }
        if self.fence.stream.binding_id() != stream.binding_id() {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "completion fence binding does not match producer stream",
            ));
        }
        if self.fence.stream.fragment_instance_id() != stream.fragment_instance_id() {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "completion fence fragment instance does not match producer stream",
            ));
        }
        if self.fence.stream.partition_id() != stream.partition_id() {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::InvalidPartition,
                "completion fence partition does not match producer stream",
            ));
        }
        if self.fence.sequence != sequence {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ConflictingReplay,
                "completion fence sequence does not match contribution sequence",
            ));
        }
        if !self
            .domain
            .matches_data_type(contract.membership_schema.data_type())
        {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::TypeMismatch,
                "final domain schema does not match completion fence contract",
            ));
        }
        Ok(())
    }

    pub const fn domain(&self) -> &ValueDomainDelta {
        &self.domain
    }

    pub const fn fence_digest(&self) -> [u8; 32] {
        self.fence.digest()
    }

    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }

    pub fn canonical_contribution_bytes(&self) -> Option<usize> {
        canonical_contribution_bytes_for(
            self.fence.canonical_bytes(),
            self.domain.estimated_contribution_bytes().ok()?,
        )
    }
}

fn final_domain_replay_digest(fence: &CompletionFence, domain: &ValueDomainDelta) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(SHARD_DOMAIN);
    canonical.update(SHARD_VERSION.to_be_bytes());
    canonical.update((size_of::<[u8; 32]>() as u64).to_be_bytes());
    canonical.update(fence.digest());
    canonical.update((size_of::<[u8; 32]>() as u64).to_be_bytes());
    canonical.update(domain.fingerprint().bytes());
    canonical.finalize().into()
}

fn canonical_contribution_bytes_for(fence_bytes: usize, domain_bytes: usize) -> Option<usize> {
    u64::try_from(fence_bytes).ok()?;
    u64::try_from(domain_bytes).ok()?;
    SHARD_DOMAIN
        .len()
        .checked_add(size_of::<u16>())?
        .checked_add(size_of::<u64>())?
        .checked_add(fence_bytes)?
        .checked_add(size_of::<u64>())?
        .checked_add(domain_bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sha2::{Digest, Sha256};

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, ProducerStreamId,
    };
    use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    use super::*;

    const QUERY_ID: UniqueId = UniqueId::new(11, 12);
    const INSTANCE_ID: UniqueId = UniqueId::new(21, 22);

    fn schema(data_type: &DataType) -> ArtifactMembershipSchema {
        ArtifactMembershipSchema::new(data_type, NullSemantics::NullSafeEqual).unwrap()
    }

    fn contract_for(
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        channel_id: ChannelId,
        membership_schema: &ArtifactMembershipSchema,
    ) -> RuntimeCompletionFenceContract {
        RuntimeCompletionFenceContract::try_from_install(
            query_id,
            epoch,
            channel_id,
            CompletionFenceKind::CommittedDomainFrozen,
            membership_schema,
        )
        .unwrap()
    }

    fn contract() -> RuntimeCompletionFenceContract {
        contract_for(
            QUERY_ID,
            DeploymentEpoch::new(13),
            ChannelId::new(14),
            &schema(&DataType::Int64),
        )
    }

    fn stream(partition: u32) -> ProducerStreamId {
        ProducerStreamId::new(BindingId::new(20), INSTANCE_ID, PartitionId::new(partition))
    }

    fn test_authority(contract: Arc<RuntimeCompletionFenceContract>) -> CompletionFenceAuthority {
        CompletionFenceAuthority::try_new(contract, BindingId::new(20), INSTANCE_ID).unwrap()
    }

    fn frozen_issuer(authority: CompletionFenceAuthority) -> FrozenFinalDomainTestIssuer {
        match CollectingFinalDomainTestIssuer::new(authority, 1).close_driver() {
            FinalDomainTestIssuerTransition::Frozen(issuer) => issuer,
            FinalDomainTestIssuerTransition::Collecting(_) => {
                panic!("the only open driver must freeze the test issuer")
            }
        }
    }

    #[test]
    fn contract_digest_canonically_binds_install_coordinates_and_schema() {
        let membership_schema = schema(&DataType::Int64);
        let contract = contract_for(
            QUERY_ID,
            DeploymentEpoch::new(13),
            ChannelId::new(14),
            &membership_schema,
        );

        let mut expected = Sha256::new();
        expected.update(b"novarocks.runtime-filter.completion-fence-contract");
        expected.update(1_u16.to_be_bytes());
        expected.update(QUERY_ID.high().to_be_bytes());
        expected.update(QUERY_ID.low().to_be_bytes());
        expected.update(13_u64.to_be_bytes());
        expected.update(ChannelId::new(14).get().to_be_bytes());
        expected.update([1]);
        expected.update(membership_schema.digest().bytes());
        assert_eq!(
            contract.digest().bytes(),
            <[u8; 32]>::from(expected.finalize())
        );

        assert_ne!(
            contract.digest(),
            contract_for(
                UniqueId::new(99, 12),
                DeploymentEpoch::new(13),
                ChannelId::new(14),
                &membership_schema,
            )
            .digest()
        );
        assert_ne!(
            contract.digest(),
            contract_for(
                QUERY_ID,
                DeploymentEpoch::new(99),
                ChannelId::new(14),
                &membership_schema,
            )
            .digest()
        );
        assert_ne!(
            contract.digest(),
            contract_for(
                QUERY_ID,
                DeploymentEpoch::new(13),
                ChannelId::new(99),
                &membership_schema,
            )
            .digest()
        );
        assert_ne!(
            contract.digest(),
            contract_for(
                QUERY_ID,
                DeploymentEpoch::new(13),
                ChannelId::new(14),
                &schema(&DataType::Utf8),
            )
            .digest()
        );

        let non_null_safe =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        assert_eq!(
            RuntimeCompletionFenceContract::try_from_install(
                QUERY_ID,
                DeploymentEpoch::new(13),
                ChannelId::new(14),
                CompletionFenceKind::CommittedDomainFrozen,
                &non_null_safe,
            ),
            Err(FinalDomainError::ContractRequiresNullSafeEqual)
        );
    }

    #[test]
    fn test_issuer_cannot_issue_until_every_local_driver_closes() {
        let authority = test_authority(Arc::new(contract()));
        let collecting = CollectingFinalDomainTestIssuer::new(authority, 2);
        let collecting = match collecting.close_driver() {
            FinalDomainTestIssuerTransition::Collecting(collecting) => collecting,
            FinalDomainTestIssuerTransition::Frozen(_) => panic!("one driver is still open"),
        };
        let frozen = match collecting.close_driver() {
            FinalDomainTestIssuerTransition::Frozen(frozen) => frozen,
            FinalDomainTestIssuerTransition::Collecting(_) => panic!("all drivers are closed"),
        };
        let shard = frozen
            .issue_shard(
                stream(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([]), false),
            )
            .unwrap();
        assert_eq!(shard, shard.clone());
    }

    #[test]
    fn authority_issues_deterministic_cloneable_stream_sequence_fences() {
        let contract = Arc::new(contract());
        let authority = test_authority(contract.clone());
        let issuer = frozen_issuer(authority);
        let first = issuer.issue(stream(3), ProducerSequence::new(4)).unwrap();
        let replay = first.clone();
        let deterministic = issuer.issue(stream(3), ProducerSequence::new(4)).unwrap();

        assert_eq!(first.digest(), replay.digest());
        assert_eq!(first.digest(), deterministic.digest());
        assert_ne!(
            first.digest(),
            issuer
                .issue(stream(5), ProducerSequence::new(4))
                .unwrap()
                .digest()
        );
        assert_ne!(
            first.digest(),
            issuer
                .issue(stream(3), ProducerSequence::new(5))
                .unwrap()
                .digest()
        );

        let wrong_binding =
            ProducerStreamId::new(BindingId::new(99), INSTANCE_ID, PartitionId::new(3));
        assert_eq!(
            issuer.issue(wrong_binding, ProducerSequence::new(4)),
            Err(FinalDomainError::UnauthorizedBinding)
        );
        let wrong_instance = ProducerStreamId::new(
            BindingId::new(20),
            UniqueId::new(99, 22),
            PartitionId::new(3),
        );
        assert_eq!(
            issuer.issue(wrong_instance, ProducerSequence::new(4)),
            Err(FinalDomainError::UnauthorizedFragmentInstance)
        );

        let verifier_authority = test_authority(contract.clone());
        let other_authority =
            CompletionFenceAuthority::try_new(contract, BindingId::new(30), INSTANCE_ID).unwrap();
        let wrong_issuer = frozen_issuer(other_authority);
        assert_eq!(
            verifier_authority.issue(&wrong_issuer.proof, stream(3), ProducerSequence::new(4),),
            Err(FinalDomainError::FrozenProofMismatch)
        );
    }

    #[test]
    fn remote_fence_reconstruction_does_not_change_local_frozen_issue_path() {
        let contract = Arc::new(contract());
        let authority = test_authority(contract.clone());
        let issuer = frozen_issuer(authority);
        let stream = stream(3);
        let sequence = ProducerSequence::new(4);
        let local = issuer.issue(stream, sequence).unwrap();

        assert_eq!(contract.membership_schema().data_type(), &DataType::Int64);
        assert_eq!(
            CompletionFence::try_from_remote_codec(
                contract.digest(),
                stream,
                sequence,
                local.digest(),
            ),
            Ok(local.clone())
        );
        let mut invalid_digest = local.digest();
        invalid_digest[0] ^= 1;
        assert_eq!(
            CompletionFence::try_from_remote_codec(
                contract.digest(),
                stream,
                sequence,
                invalid_digest,
            ),
            Err(FinalDomainError::FenceIntegrityMismatch)
        );

        let collecting = CollectingFinalDomainTestIssuer::new(test_authority(contract), 1);
        assert!(matches!(
            collecting.close_driver(),
            FinalDomainTestIssuerTransition::Frozen(_)
        ));
    }

    #[test]
    fn shard_validates_contract_schema_and_full_scope() {
        let contract = Arc::new(contract());
        let authority = test_authority(contract.clone());
        let issuer = frozen_issuer(authority);
        let fence = issuer.issue(stream(3), ProducerSequence::new(4)).unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([1, 2]), true);
        let shard = FinalDomainShard::try_new(&contract, fence.clone(), domain).unwrap();

        assert_eq!(shard.domain().data_type(), DataType::Int64);
        assert!(
            shard
                .verify_scope(&contract, stream(3), ProducerSequence::new(4))
                .is_ok()
        );

        let other_contract = contract_for(
            UniqueId::new(90, 91),
            DeploymentEpoch::new(13),
            ChannelId::new(14),
            &schema(&DataType::Int64),
        );
        assert_eq!(
            FinalDomainShard::try_new(
                &other_contract,
                fence,
                ValueDomainDelta::new(MembershipValues::int64([1]), false),
            ),
            Err(FinalDomainError::ContractMismatch)
        );
        assert_eq!(
            FinalDomainShard::try_new(
                &contract,
                issuer.issue(stream(3), ProducerSequence::new(4)).unwrap(),
                ValueDomainDelta::new(MembershipValues::utf8(["wrong"]), false),
            ),
            Err(FinalDomainError::DomainSchemaMismatch)
        );

        let mut tampered_fence = issuer.issue(stream(3), ProducerSequence::new(4)).unwrap();
        tampered_fence.digest[0] ^= 1;
        assert_eq!(
            FinalDomainShard::try_new(
                &contract,
                tampered_fence,
                ValueDomainDelta::new(MembershipValues::int64([1]), false),
            ),
            Err(FinalDomainError::FenceIntegrityMismatch)
        );

        assert_eq!(
            shard
                .verify_scope(&contract, stream(9), ProducerSequence::new(4))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        let wrong_binding =
            ProducerStreamId::new(BindingId::new(99), INSTANCE_ID, PartitionId::new(3));
        assert_eq!(
            shard
                .verify_scope(&contract, wrong_binding, ProducerSequence::new(4))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        let wrong_instance = ProducerStreamId::new(
            BindingId::new(20),
            UniqueId::new(99, 22),
            PartitionId::new(3),
        );
        assert_eq!(
            shard
                .verify_scope(&contract, wrong_instance, ProducerSequence::new(4))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        assert_eq!(
            shard
                .verify_scope(&contract, stream(3), ProducerSequence::new(9))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            shard
                .verify_scope(&other_contract, stream(3), ProducerSequence::new(4))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );
    }

    #[test]
    fn explicit_empty_shard_is_valid_and_canonical_size_is_checked() {
        let contract = Arc::new(contract());
        let authority = test_authority(contract.clone());
        let issuer = frozen_issuer(authority);
        let fence = issuer.issue(stream(0), ProducerSequence::new(0)).unwrap();
        let empty = ValueDomainDelta::new(MembershipValues::int64([]), false);
        let domain_bytes = empty.estimated_contribution_bytes().unwrap();
        let shard = FinalDomainShard::try_new(&contract, fence, empty).unwrap();

        assert!(shard.domain().values().is_empty());
        let expected_bytes = SHARD_DOMAIN.len()
            + size_of::<u16>()
            + size_of::<u64>()
            + FENCE_DOMAIN.len()
            + size_of::<u16>()
            + 32
            + size_of::<u32>()
            + size_of::<i64>() * 2
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u64>()
            + domain_bytes;
        assert_eq!(shard.canonical_contribution_bytes(), Some(expected_bytes));
        assert!(shard.canonical_contribution_bytes().unwrap() > domain_bytes);
        assert_eq!(canonical_contribution_bytes_for(1, usize::MAX), None);

        let mut expected_replay = Sha256::new();
        expected_replay.update(SHARD_DOMAIN);
        expected_replay.update(SHARD_VERSION.to_be_bytes());
        expected_replay.update(32_u64.to_be_bytes());
        expected_replay.update(shard.fence.digest());
        expected_replay.update(32_u64.to_be_bytes());
        expected_replay.update(shard.domain.fingerprint().bytes());
        assert_eq!(
            shard.replay_digest(),
            <[u8; 32]>::from(expected_replay.finalize())
        );

        let replay = FinalDomainShard::try_new(
            &contract,
            issuer.issue(stream(0), ProducerSequence::new(0)).unwrap(),
            ValueDomainDelta::new(MembershipValues::int64([]), false),
        )
        .unwrap();
        assert_eq!(shard.replay_digest(), replay.replay_digest());

        let different_token = issuer
            .issue_shard(
                stream(0),
                ProducerSequence::new(1),
                ValueDomainDelta::new(MembershipValues::int64([]), false),
            )
            .unwrap();
        assert_ne!(shard.replay_digest(), different_token.replay_digest());

        let different_domain = issuer
            .issue_shard(
                stream(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([1]), false),
            )
            .unwrap();
        assert_ne!(shard.replay_digest(), different_domain.replay_digest());
    }
}
