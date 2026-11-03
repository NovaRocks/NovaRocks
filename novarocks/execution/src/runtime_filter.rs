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
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_local_program::{
    FilterMembershipSchema, FilterMembershipSchemaError, FilterNullSemantics,
};

pub use novarocks_functions::FunctionValueType;
pub use novarocks_local_program::is_aggregate_topn_key_carrier;

pub mod contribution;
pub mod evaluator;
mod failure;
pub mod feedback_domain;
mod order_contract;
mod owned;
pub use order_contract::RuntimeOrderContractRef;
pub mod row_group_domain;
pub use crate::exec::operators::AggregateTopNBoundaryError;
pub use failure::RuntimeFilterFailure;
pub use owned::CanonicalContributionBytes;
pub(crate) use owned::{OrderedTopNAllocationHost, OwnedOrderKey};

// These are the original Functions control/cause types used by the neutral
// controlled artifact-query port, not a second Worker control vocabulary.
pub use novarocks_functions::{KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK};

pub use evaluator::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterControlledQueryError,
    RuntimeFilterControlledScalarQuery, RuntimeFilterRowEffect, RuntimeFilterRowEvaluation,
    RuntimeFilterRowNotEvaluatedReason, RuntimeFilterRowOutcome, RuntimeFilterScalarRef,
};

macro_rules! id {
    ($name:ident, $raw:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($raw);
        impl $name {
            pub const fn new(raw: $raw) -> Self {
                Self(raw)
            }
            pub const fn get(self) -> $raw {
                self.0
            }
        }
    };
}

id!(RuntimeFilterBindingId, u32);
id!(RuntimeFilterChannelId, u32);
id!(PartitionId, u32);
id!(ProducerSequence, u64);

/// A non-zero, monotonically increasing logical publication version.
///
/// Zero is reserved by wire envelopes that do not carry a logical artifact
/// version (for example an unavailable frame), and is never a valid logical
/// version inside the execution domain.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogicalVersion(NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalVersionError {
    Zero,
}
impl fmt::Display for LogicalVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("logical runtime filter version must be non-zero"),
        }
    }
}
impl Error for LogicalVersionError {}

impl LogicalVersion {
    pub const FIRST: Self = match NonZeroU64::new(1) {
        Some(value) => Self(value),
        None => panic!("one is non-zero"),
    };

    /// Constructs a logical version while enforcing the non-zero invariant.
    ///
    /// Decoder and byte-boundary callers must use this fallible constructor so
    /// malformed zero values are reported as typed errors instead of entering
    /// the execution domain.
    pub const fn try_new(raw: u64) -> Result<Self, LogicalVersionError> {
        match NonZeroU64::new(raw) {
            Some(value) => Ok(Self(value)),
            None => Err(LogicalVersionError::Zero),
        }
    }

    /// Convenience for static, non-zero literals used by construction sites.
    ///
    /// Unlike the former tuple-like wrapper this validates `raw`; dynamic
    /// values at protocol boundaries must use [`Self::try_new`].
    pub const fn new(raw: u64) -> Self {
        match Self::try_new(raw) {
            Ok(value) => value,
            Err(_) => panic!("logical runtime filter version must be non-zero"),
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterExecutionContract {
    Membership(RuntimeFilterMembershipSchema),
    Ordered(RuntimeOrderContractRef),
}

impl RuntimeFilterExecutionContract {
    pub fn value_type(&self) -> Option<&novarocks_functions::FunctionValueType> {
        match self {
            Self::Membership(schema) => schema.value_type(),
            Self::Ordered(order) => order
                .keys()
                .first()
                .and_then(contribution::RuntimeOrderKey::value_type),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

/// The runtime view of a canonical membership artifact schema. The canonical
/// bytes and their digest are owned by `novarocks-local-program`, which is
/// also what a static membership contract was digested with; this type only
/// projects that one schema into the Execution vocabulary.
#[derive(Clone, Eq, PartialEq)]
pub struct RuntimeFilterMembershipSchema {
    canonical: FilterMembershipSchema,
    complete_type: Option<novarocks_functions::FunctionValueType>,
}

impl RuntimeFilterMembershipSchema {
    pub fn from_canonical_value_type(
        canonical: FilterMembershipSchema,
        value_type: novarocks_functions::FunctionValueType,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        if canonical.data_type() != value_type.data_type() {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "membership canonical carrier differs from complete execution declaration",
            ));
        }
        Ok(Self {
            canonical,
            complete_type: Some(value_type),
        })
    }

    pub fn new(
        data_type: &DataType,
        null_semantics: RuntimeFilterNullSemantics,
    ) -> Result<Self, FilterMembershipSchemaError> {
        let canonical = FilterMembershipSchema::try_new(data_type, null_semantics.canonical())?;
        Ok(Self {
            canonical,
            complete_type: None,
        })
    }

    /// The canonical bytes remain physical artifact facts; the complete value
    /// binding is the independent channel author and retains its creator hold.
    pub fn new_value_typed_observed<E>(
        value_type: novarocks_functions::FunctionValueType,
        null_semantics: RuntimeFilterNullSemantics,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E>
    where
        E: From<FilterMembershipSchemaError>
            + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        Self::new_value_typed_with_storage_observed(
            value_type,
            null_semantics,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            request,
            work,
        )
    }

    pub fn new_value_typed_with_storage_observed<E>(
        value_type: novarocks_functions::FunctionValueType,
        null_semantics: RuntimeFilterNullSemantics,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E>
    where
        E: From<FilterMembershipSchemaError>
            + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        let storage = storage.retaining_observed(
            &value_type
                .typed_binding()
                .layout()
                .arrow_backing_storage_lifetime(),
            &mut request,
        )?;
        let canonical = FilterMembershipSchema::try_new_with_storage_observed(
            value_type.data_type(),
            null_semantics.canonical(),
            storage,
            request,
            work,
        )?;
        Ok(Self {
            canonical,
            complete_type: Some(value_type),
        })
    }

    /// Borrow the physical artifact owner, whose clone retains its own
    /// canonical backing independently of the complete value declaration.
    pub fn canonical_schema(&self) -> &FilterMembershipSchema {
        &self.canonical
    }

    pub fn value_type(&self) -> Option<&novarocks_functions::FunctionValueType> {
        self.complete_type.as_ref()
    }

    pub fn from_canonical(
        canonical_bytes: impl AsRef<[u8]>,
        expected_digest: [u8; 32],
    ) -> Result<Self, FilterMembershipSchemaError> {
        let canonical = FilterMembershipSchema::from_canonical(canonical_bytes, expected_digest)?;
        Ok(Self {
            canonical,
            complete_type: None,
        })
    }

    pub const fn data_type(&self) -> &DataType {
        self.canonical.data_type()
    }

    pub const fn null_semantics(&self) -> RuntimeFilterNullSemantics {
        RuntimeFilterNullSemantics::from_canonical(self.canonical.null_semantics())
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        self.canonical.canonical_bytes()
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.canonical.digest()
    }
}

impl fmt::Debug for RuntimeFilterMembershipSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeFilterMembershipSchema")
            .field("data_type", self.data_type())
            .field("null_semantics", &self.null_semantics())
            .field("canonical_bytes", &self.canonical_bytes())
            .field("digest", &self.digest())
            .finish()
    }
}

impl RuntimeFilterNullSemantics {
    /// The same semantics in the canonical schema owner's vocabulary.
    pub const fn canonical(self) -> FilterNullSemantics {
        match self {
            Self::NeverMatches => FilterNullSemantics::NeverMatches,
            Self::NullSafeEqual => FilterNullSemantics::NullSafeEqual,
        }
    }

    pub const fn from_canonical(semantics: FilterNullSemantics) -> Self {
        match semantics {
            FilterNullSemantics::NeverMatches => Self::NeverMatches,
            FilterNullSemantics::NullSafeEqual => Self::NullSafeEqual,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContributionKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

/// Immutable canonical contribution bytes. The execution surface owns the
/// producer vocabulary; a role-local adapter owns codec validation and reducer
/// delivery for the installed contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContribution {
    kind: RuntimeFilterContributionKind,
    contract_digest: [u8; 32],
    canonical_bytes: CanonicalContributionBytes,
}

/// A frozen, fragment-local membership domain submitted through a final-domain
/// completion fence.
///
/// The payload is canonical and typed at the Execution boundary.  Completion
/// adapters must strictly decode it using the exact schema and digest instead
/// of receiving an untyped side-channel value.
#[derive(Clone, Eq, PartialEq)]
pub struct RuntimeFilterFinalDomain {
    canonical_bytes: Arc<[u8]>,
    data_type: DataType,
    contract_digest: [u8; 32],
}

impl RuntimeFilterFinalDomain {
    pub fn from_value_domain(
        domain: &contribution::ValueDomainDelta,
        contract_digest: [u8; 32],
        max_canonical_bytes: usize,
    ) -> Result<Self, contribution::ContributionCodecError> {
        let data_type = domain.data_type();
        let canonical_bytes =
            contribution::encode_value_domain(domain, max_canonical_bytes)?.into();
        Ok(Self {
            canonical_bytes,
            data_type,
            contract_digest,
        })
    }

    pub fn from_canonical(
        canonical_bytes: impl Into<Arc<[u8]>>,
        data_type: DataType,
        contract_digest: [u8; 32],
        max_canonical_bytes: usize,
    ) -> Result<Self, contribution::ContributionCodecError> {
        let canonical_bytes = canonical_bytes.into();
        contribution::decode_value_domain(&canonical_bytes, &data_type, max_canonical_bytes)?;
        Ok(Self {
            canonical_bytes,
            data_type,
            contract_digest,
        })
    }

    pub const fn canonical_bytes(&self) -> &Arc<[u8]> {
        &self.canonical_bytes
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
}

impl fmt::Debug for RuntimeFilterFinalDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeFilterFinalDomain")
            .field("canonical_bytes_len", &self.canonical_bytes.len())
            .finish_non_exhaustive()
    }
}

impl RuntimeFilterContribution {
    pub fn new(
        kind: RuntimeFilterContributionKind,
        contract_digest: [u8; 32],
        canonical_bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self::from_shared(kind, contract_digest, canonical_bytes.into().into())
    }

    /// Keep the complete immutable carrier through local/outbound/ingress
    /// handoff. Canonical validation remains the installed reducer's concern.
    pub fn from_shared(
        kind: RuntimeFilterContributionKind,
        contract_digest: [u8; 32],
        canonical_bytes: CanonicalContributionBytes,
    ) -> Self {
        Self {
            kind,
            contract_digest,
            canonical_bytes,
        }
    }

    pub const fn kind(&self) -> RuntimeFilterContributionKind {
        self.kind
    }

    pub const fn canonical_bytes(&self) -> &CanonicalContributionBytes {
        &self.canonical_bytes
    }

    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive {
        late_apply: RuntimeFilterLateApplyGranularity,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32, contract_digest: [u8; 32] },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterCompletion {
    ProducerClosed,
    FencedFinalDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContractViolationKind {
    MissingSession,
    UnauthorizedBinding,
    ContractMismatch,
    RoleMismatch,
    InvalidPartitionCount,
    ResourceLimit,
    SessionClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContractViolation {
    kind: RuntimeFilterContractViolationKind,
    detail: Arc<str>,
    source: Option<novarocks_failure_contract::SharedFailureCause>,
}

impl RuntimeFilterContractViolation {
    pub fn new(kind: RuntimeFilterContractViolationKind, detail: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            source: None,
        }
    }
    pub fn from_domain<E>(kind: RuntimeFilterContractViolationKind, source: E) -> Self
    where
        E: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
    {
        let source = novarocks_failure_contract::SharedFailureCause::new(source);
        Self {
            kind,
            detail: Arc::from(source.detail()),
            source: Some(source),
        }
    }
    pub const fn kind(&self) -> RuntimeFilterContractViolationKind {
        self.kind
    }
    pub fn detail(&self) -> &str {
        &self.detail
    }
}
impl fmt::Display for RuntimeFilterContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "runtime-filter contract violation {:?}: {}",
            self.kind, self.detail
        )
    }
}
impl Error for RuntimeFilterContractViolation {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl novarocks_failure_contract::FailureProjection for RuntimeFilterContractViolationKind {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match *self {
            RuntimeFilterContractViolationKind::MissingSession => (
                FailureClass::Invariant,
                ExecutionReason::FilterMissingSession,
            ),
            RuntimeFilterContractViolationKind::UnauthorizedBinding => (
                FailureClass::Invariant,
                ExecutionReason::FilterUnauthorizedBinding,
            ),
            RuntimeFilterContractViolationKind::ContractMismatch => (
                FailureClass::Invariant,
                ExecutionReason::FilterContractMismatch,
            ),
            RuntimeFilterContractViolationKind::RoleMismatch => {
                (FailureClass::Invariant, ExecutionReason::FilterRoleMismatch)
            }
            RuntimeFilterContractViolationKind::InvalidPartitionCount => (
                FailureClass::Invariant,
                ExecutionReason::FilterInvalidPartitionCount,
            ),
            RuntimeFilterContractViolationKind::ResourceLimit => {
                (FailureClass::Resource, ExecutionReason::FilterResourceLimit)
            }
            RuntimeFilterContractViolationKind::SessionClosed => {
                (FailureClass::Control, ExecutionReason::FilterSessionClosed)
            }
        };
        if matches!(self, Self::SessionClosed) {
            FailureFact::stop_observation(reason.reason())
        } else {
            FailureFact::new(class, reason.reason())
        }
    }
}
impl novarocks_failure_contract::FailureProjection for RuntimeFilterContractViolation {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match &self.source {
            Some(source) => source.failure_fact(),
            None => self.kind.failure_fact(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerContract {
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    kind: RuntimeFilterProducerKind,
    contract: RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
    completion: RuntimeFilterCompletion,
}
impl RuntimeFilterProducerContract {
    pub fn membership(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_kind(
            binding_id,
            channel_id,
            RuntimeFilterProducerKind::Membership,
            contract,
        )
    }

    pub fn ordered_bound(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_kind(
            binding_id,
            channel_id,
            RuntimeFilterProducerKind::OrderedBound,
            contract,
        )
    }

    pub fn top_k_summary(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        k: u32,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        if k == 0 {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "top-k runtime-filter producer requires a non-zero K",
            ));
        }
        let mut producer = Self::for_kind(
            binding_id,
            channel_id,
            RuntimeFilterProducerKind::TopKSummary,
            contract,
        )?;
        let RuntimeFilterExecutionContract::Ordered(order_contract) = producer.contract() else {
            unreachable!("kind and contract were validated above")
        };
        producer.reduction = RuntimeFilterReduction::MergeTopKSummary {
            k,
            contract_digest: order_contract.digest(),
        };
        Ok(producer)
    }

    pub fn final_domain(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_kind(
            binding_id,
            channel_id,
            RuntimeFilterProducerKind::FinalDomain,
            contract,
        )
    }

    fn for_kind(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        kind: RuntimeFilterProducerKind,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        let valid = matches!(
            (kind, &contract),
            (
                RuntimeFilterProducerKind::Membership,
                RuntimeFilterExecutionContract::Membership(_)
            ) | (
                RuntimeFilterProducerKind::OrderedBound,
                RuntimeFilterExecutionContract::Ordered(_)
            ) | (
                RuntimeFilterProducerKind::TopKSummary,
                RuntimeFilterExecutionContract::Ordered(_)
            ) | (
                RuntimeFilterProducerKind::FinalDomain,
                RuntimeFilterExecutionContract::Membership(_)
            )
        );
        if !valid {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "runtime-filter producer kind does not match its execution contract",
            ));
        }
        Ok(Self {
            binding_id,
            channel_id,
            kind,
            reduction: match kind {
                RuntimeFilterProducerKind::Membership | RuntimeFilterProducerKind::FinalDomain => {
                    RuntimeFilterReduction::SetUnion
                }
                RuntimeFilterProducerKind::OrderedBound => {
                    RuntimeFilterReduction::TightenOrderedBound
                }
                RuntimeFilterProducerKind::TopKSummary => {
                    let RuntimeFilterExecutionContract::Ordered(order_contract) = &contract else {
                        unreachable!("kind and contract were validated above")
                    };
                    RuntimeFilterReduction::MergeTopKSummary {
                        k: 0,
                        contract_digest: order_contract.digest(),
                    }
                }
            },
            completion: match kind {
                RuntimeFilterProducerKind::FinalDomain => {
                    RuntimeFilterCompletion::FencedFinalDomain
                }
                RuntimeFilterProducerKind::Membership
                | RuntimeFilterProducerKind::OrderedBound
                | RuntimeFilterProducerKind::TopKSummary => RuntimeFilterCompletion::ProducerClosed,
            },
            contract,
        })
    }

    /// Transitional constructor retained while Core plan carriers are cut to
    /// the named Execution constructors. New fragment callers must use the
    /// role-specific constructors above.
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        kind: RuntimeFilterProducerKind,
        contract: RuntimeFilterExecutionContract,
    ) -> Self {
        Self {
            binding_id,
            channel_id,
            kind,
            reduction: match kind {
                RuntimeFilterProducerKind::Membership | RuntimeFilterProducerKind::FinalDomain => {
                    RuntimeFilterReduction::SetUnion
                }
                RuntimeFilterProducerKind::OrderedBound => {
                    RuntimeFilterReduction::TightenOrderedBound
                }
                RuntimeFilterProducerKind::TopKSummary => {
                    RuntimeFilterReduction::MergeTopKSummary {
                        k: 0,
                        contract_digest: [0; 32],
                    }
                }
            },
            completion: match kind {
                RuntimeFilterProducerKind::FinalDomain => {
                    RuntimeFilterCompletion::FencedFinalDomain
                }
                RuntimeFilterProducerKind::Membership
                | RuntimeFilterProducerKind::OrderedBound
                | RuntimeFilterProducerKind::TopKSummary => RuntimeFilterCompletion::ProducerClosed,
            },
            contract,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
    pub const fn kind(&self) -> RuntimeFilterProducerKind {
        self.kind
    }
    pub const fn contract(&self) -> &RuntimeFilterExecutionContract {
        &self.contract
    }
    pub const fn reduction(&self) -> RuntimeFilterReduction {
        self.reduction
    }
    pub const fn completion(&self) -> RuntimeFilterCompletion {
        self.completion
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterConsumerContract {
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    activation: ConsumerActivation,
    contract: RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
}
impl RuntimeFilterConsumerContract {
    pub fn membership_blocking(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_activation(
            binding_id,
            channel_id,
            ConsumerActivation::BlockingSnapshot,
            contract,
            RuntimeFilterReduction::SetUnion,
        )
    }

    pub fn membership_live(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        late_apply: RuntimeFilterLateApplyGranularity,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_activation(
            binding_id,
            channel_id,
            ConsumerActivation::NonBlockingLive { late_apply },
            contract,
            RuntimeFilterReduction::SetUnion,
        )
    }

    pub fn ordered_live(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        late_apply: RuntimeFilterLateApplyGranularity,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        Self::for_activation(
            binding_id,
            channel_id,
            ConsumerActivation::NonBlockingLive { late_apply },
            contract,
            RuntimeFilterReduction::TightenOrderedBound,
        )
    }

    pub fn top_k_live(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        late_apply: RuntimeFilterLateApplyGranularity,
        k: u32,
        contract: RuntimeFilterExecutionContract,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        let RuntimeFilterExecutionContract::Ordered(order_contract) = &contract else {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "top-k runtime-filter consumer requires an ordered contract",
            ));
        };
        if k == 0 {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "top-k runtime-filter consumer requires a non-zero K",
            ));
        }
        let contract_digest = order_contract.digest();
        Self::for_activation(
            binding_id,
            channel_id,
            ConsumerActivation::NonBlockingLive { late_apply },
            contract,
            RuntimeFilterReduction::MergeTopKSummary { k, contract_digest },
        )
    }

    fn for_activation(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        activation: ConsumerActivation,
        contract: RuntimeFilterExecutionContract,
        reduction: RuntimeFilterReduction,
    ) -> Result<Self, RuntimeFilterContractViolation> {
        let valid = matches!(
            (&contract, reduction),
            (
                RuntimeFilterExecutionContract::Membership(_),
                RuntimeFilterReduction::SetUnion
            ) | (
                RuntimeFilterExecutionContract::Ordered(_),
                RuntimeFilterReduction::TightenOrderedBound
            ) | (
                RuntimeFilterExecutionContract::Ordered(_),
                RuntimeFilterReduction::MergeTopKSummary { .. }
            )
        );
        if !valid {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "runtime-filter consumer reduction does not match its execution contract",
            ));
        }
        if matches!(activation, ConsumerActivation::BlockingSnapshot)
            && !matches!(contract, RuntimeFilterExecutionContract::Membership(_))
        {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "ordered runtime-filter consumers cannot use blocking snapshots",
            ));
        }
        Ok(Self {
            binding_id,
            channel_id,
            activation,
            contract,
            reduction,
        })
    }

    /// Transitional constructor retained while Core plan carriers are cut to
    /// the named Execution constructors. New fragment callers must use the
    /// role-specific constructors above.
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        activation: ConsumerActivation,
        contract: RuntimeFilterExecutionContract,
    ) -> Self {
        let reduction = match &contract {
            RuntimeFilterExecutionContract::Membership(_) => RuntimeFilterReduction::SetUnion,
            RuntimeFilterExecutionContract::Ordered(_) => {
                RuntimeFilterReduction::TightenOrderedBound
            }
        };
        Self {
            binding_id,
            channel_id,
            activation,
            contract,
            reduction,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
    pub const fn activation(&self) -> ConsumerActivation {
        self.activation
    }
    pub const fn contract(&self) -> &RuntimeFilterExecutionContract {
        &self.contract
    }
    pub const fn reduction(&self) -> RuntimeFilterReduction {
        self.reduction
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerOpenRequest {
    contract: RuntimeFilterProducerContract,
    local_partition_count: u32,
}

/// Opens the aggregate-only completion fence associated with a FinalDomain
/// producer binding. It is deliberately separate from the regular producer
/// handle: the adapter retains issuance authority until every claimed local
/// partition has frozen and closed exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterFinalDomainOpenRequest {
    contract: RuntimeFilterProducerContract,
    local_partition_count: u32,
}

impl RuntimeFilterFinalDomainOpenRequest {
    pub const fn new(contract: RuntimeFilterProducerContract, local_partition_count: u32) -> Self {
        Self {
            contract,
            local_partition_count,
        }
    }

    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }

    pub const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }
}
impl RuntimeFilterProducerOpenRequest {
    pub const fn new(contract: RuntimeFilterProducerContract, local_partition_count: u32) -> Self {
        Self {
            contract,
            local_partition_count,
        }
    }
    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }
    pub const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterSubscriptionRequest {
    contract: RuntimeFilterConsumerContract,
}
impl RuntimeFilterSubscriptionRequest {
    pub const fn new(contract: RuntimeFilterConsumerContract) -> Self {
        Self { contract }
    }
    pub const fn contract(&self) -> &RuntimeFilterConsumerContract {
        &self.contract
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterBindOutcome<T> {
    Bound(T),
    Unavailable(UnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableReason {
    ResourceLimit,
    IncompleteCoverage,
    ProducerFailed,
    MaterializationFailed,
    RouteUnavailable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactUnsupportedReason {
    RangeDeferred,
    NoAcceptedRepresentation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveTerminal {
    Completed,
    CompletedWithoutArtifact,
    Unavailable(UnavailableReason),
    Cancelled,
}

#[derive(Clone)]
pub struct RuntimeFilterSnapshot {
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    contract_digest: [u8; 32],
    artifact_query: Arc<dyn evaluator::RuntimeFilterArtifactQuery>,
}
impl RuntimeFilterSnapshot {
    pub fn new(
        binding_id: RuntimeFilterBindingId,
        logical_version: LogicalVersion,
        contract_digest: [u8; 32],
        artifact_query: Arc<dyn evaluator::RuntimeFilterArtifactQuery>,
    ) -> Self {
        Self {
            binding_id,
            logical_version,
            contract_digest,
            artifact_query,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub const fn artifact_query(&self) -> &Arc<dyn evaluator::RuntimeFilterArtifactQuery> {
        &self.artifact_query
    }
}

#[derive(Clone)]
pub enum SnapshotAcquireOutcome {
    Published(Arc<RuntimeFilterSnapshot>),
    Unsupported(ArtifactUnsupportedReason),
    Unavailable(UnavailableReason),
    Cancelled,
    TimedOut,
}
pub enum LivePollOutcome {
    Updated {
        snapshot: Arc<RuntimeFilterSnapshot>,
        terminal: Option<LiveTerminal>,
    },
    Idle {
        latest_version: Option<LogicalVersion>,
        terminal: Option<LiveTerminal>,
    },
}

/// A consumer's subscription to one snapshot that its input waits for.
///
/// Nothing here blocks: the consumer's gate asks for the outcome, parks on
/// the outcome observable while it is pending, and decides itself when it
/// stops waiting.
pub trait BlockingSnapshotSubscription: Send + Sync {
    /// The published outcome, or `None` while the snapshot is still pending.
    /// Never `TimedOut`: only the consumer decides that it stopped waiting.
    fn try_outcome(&self) -> Option<SnapshotAcquireOutcome>;
    /// Notified once when the outcome is published. Its identity is stable
    /// for the subscription's lifetime.
    fn outcome_observable(&self) -> Arc<crate::runtime::observable::Observable>;
    /// Records the outcome the consumer finally acted on: the published one,
    /// or `TimedOut` when its wait ended first. Only the first record counts.
    fn record_consumer_outcome(&self, outcome: &SnapshotAcquireOutcome);
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>>;
}
pub trait NonBlockingLiveSubscription: Send + Sync {
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>>;
    fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome;
}
/// Holds the exact subscription state and any charged snapshot retained by it.
/// Publication, cancellation, and terminal outcomes use their explicit paths.
pub enum RuntimeFilterSubscriptionPin {
    Blocking(Arc<dyn BlockingSnapshotSubscription>),
    Live(Arc<dyn NonBlockingLiveSubscription>),
}

pub trait RuntimeFilterProducer: Send + Sync {
    /// The maximum canonical contribution payload accepted by this exact
    /// fragment-local producer route. It is an execution capability, not a
    /// deployment descriptor, so callers can bound local encoding without
    /// observing the installed service state.
    fn max_contribution_bytes(&self) -> usize;

    fn submit(
        &self,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
    fn close_partition(
        &self,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
}
/// Clones share the original producer and its retained session state.
pub type SharedRuntimeFilterProducer = Arc<dyn RuntimeFilterProducer>;

/// One-shot ownership of a FinalDomain partition.
///
/// This is an unchecked obligation: every claimed partition must be sealed
/// and closed explicitly. The current partition implementations have no Drop
/// hook; dropping an unclosed partition neither closes it nor publishes failure.
pub trait RuntimeFilterFinalDomainPartition: Send {
    fn seal(
        &mut self,
        domain: RuntimeFilterFinalDomain,
    ) -> Result<(), RuntimeFilterContractViolation>;
    fn close(&mut self) -> Result<(), RuntimeFilterContractViolation>;
}
pub type RuntimeFilterFinalDomainPartitionObligation = Box<dyn RuntimeFilterFinalDomainPartition>;

/// Fragment-local FinalDomain fence capability. It exposes only local
/// partition ownership, the frozen-domain budget, and typed terminal failure;
/// registry membership and service issuance proofs stay adapter-private.
pub trait RuntimeFilterFinalDomainCompletion: Send + Sync {
    fn membership_key_type(&self) -> DataType;
    fn max_domain_canonical_bytes(&self) -> usize;
    fn contract_digest(&self) -> [u8; 32];
    fn claim_partition(
        &self,
        partition: PartitionId,
    ) -> Result<RuntimeFilterFinalDomainPartitionObligation, RuntimeFilterContractViolation>;
    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
}
/// Clones share completion state and the same original producer ownership.
pub type SharedRuntimeFilterFinalDomainCompletion = Arc<dyn RuntimeFilterFinalDomainCompletion>;

/// A producer operation is admitted exactly once by the installed route. The
/// result preserves terminal and replay semantics without exposing the
/// role-local reducer's implementation types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterSubmitOutcome {
    Applied,
    Duplicate,
    Stale,
    SequenceAdvancedEqual,
    StreamAcceptedNoGlobalChange,
    Published,
    PendingGap,
    PendingFinalSnapshot,
    CoverageStillPossible,
    TerminalNoop,
    Completed,
    CompletedWithoutArtifact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerFailure {
    Cancelled,
    ExecutionFailed,
    UpstreamUnavailable,
}

pub trait RuntimeFilterSession: Send + Sync {
    fn open_producer(
        &self,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<RuntimeFilterBindOutcome<SharedRuntimeFilterProducer>, RuntimeFilterContractViolation>;
    fn subscribe(
        &self,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionPin>,
        RuntimeFilterContractViolation,
    >;
    fn open_final_domain_completion(
        &self,
        request: RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<SharedRuntimeFilterFinalDomainCompletion>,
        RuntimeFilterContractViolation,
    >;
}
pub type RuntimeFilterSessionRef = Arc<dyn RuntimeFilterSession>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeFilterLiveCursor {
    observed: Option<LogicalVersion>,
}
impl RuntimeFilterLiveCursor {
    pub const fn observed(self) -> Option<LogicalVersion> {
        self.observed
    }
    pub fn observe(
        &mut self,
        snapshot: &RuntimeFilterSnapshot,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if self
            .observed
            .is_some_and(|version| snapshot.logical_version() <= version)
        {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "runtime-filter live snapshot version regressed or repeated",
            ));
        }
        self.observed = Some(snapshot.logical_version());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use novarocks_spi::connector::ConnectorScalarValue;

    struct Query;
    impl RuntimeFilterArtifactQuery for Query {
        fn data_type(&self) -> &DataType {
            &DataType::Int64
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }

        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
    }
    struct Producer;
    impl RuntimeFilterProducer for Producer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }

        fn submit(
            &self,
            _: PartitionId,
            _: ProducerSequence,
            _: RuntimeFilterContribution,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
        fn close_partition(
            &self,
            _: PartitionId,
            _: ProducerSequence,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
        fn fail(
            &self,
            _: RuntimeFilterProducerFailure,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
    }

    struct FinalDomainFenceState {
        partition_count: u32,
        claimed: Mutex<BTreeSet<u32>>,
        closed: Mutex<BTreeSet<u32>>,
    }

    struct FinalDomainFence(Arc<FinalDomainFenceState>);

    impl FinalDomainFence {
        fn new(partition_count: u32) -> Self {
            Self(Arc::new(FinalDomainFenceState {
                partition_count,
                claimed: Mutex::new(BTreeSet::new()),
                closed: Mutex::new(BTreeSet::new()),
            }))
        }

        fn completed(&self) -> bool {
            self.0.closed.lock().expect("closed partitions").len()
                == self.0.partition_count as usize
        }
    }

    struct FinalDomainPartition {
        fence: Arc<FinalDomainFenceState>,
        partition: u32,
        sealed: bool,
    }

    impl RuntimeFilterFinalDomainPartition for FinalDomainPartition {
        fn seal(
            &mut self,
            _domain: RuntimeFilterFinalDomain,
        ) -> Result<(), RuntimeFilterContractViolation> {
            if self.sealed {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition was sealed twice",
                ));
            }
            self.sealed = true;
            Ok(())
        }

        fn close(&mut self) -> Result<(), RuntimeFilterContractViolation> {
            if !self.sealed {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition closed before seal",
                ));
            }
            self.fence
                .closed
                .lock()
                .expect("closed partitions")
                .insert(self.partition);
            Ok(())
        }
    }

    impl RuntimeFilterFinalDomainCompletion for FinalDomainFence {
        fn membership_key_type(&self) -> DataType {
            DataType::Int64
        }

        fn max_domain_canonical_bytes(&self) -> usize {
            1024
        }

        fn contract_digest(&self) -> [u8; 32] {
            [0; 32]
        }

        fn claim_partition(
            &self,
            partition: PartitionId,
        ) -> Result<RuntimeFilterFinalDomainPartitionObligation, RuntimeFilterContractViolation>
        {
            if partition.get() >= self.0.partition_count {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition is outside declared DOP",
                ));
            }
            let mut claimed = self.0.claimed.lock().expect("claimed partitions");
            if !claimed.insert(partition.get()) {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition was claimed twice",
                ));
            }
            Ok(Box::new(FinalDomainPartition {
                fence: Arc::clone(&self.0),
                partition: partition.get(),
                sealed: false,
            }))
        }

        fn fail(
            &self,
            _reason: RuntimeFilterProducerFailure,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }
    struct FakeSession;
    impl RuntimeFilterSession for FakeSession {
        fn open_producer(
            &self,
            request: RuntimeFilterProducerOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<SharedRuntimeFilterProducer>,
            RuntimeFilterContractViolation,
        > {
            if request.local_partition_count() == 0 {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::InvalidPartitionCount,
                    "partition count must be non-zero",
                ));
            }
            Ok(RuntimeFilterBindOutcome::Bound(Arc::new(Producer)))
        }
        fn subscribe(
            &self,
            request: RuntimeFilterSubscriptionRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterSubscriptionPin>,
            RuntimeFilterContractViolation,
        > {
            if request.contract().binding_id().get() == 99 {
                Ok(RuntimeFilterBindOutcome::Unavailable(
                    UnavailableReason::IncompleteCoverage,
                ))
            } else {
                Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "fake session has no subscription",
                ))
            }
        }

        fn open_final_domain_completion(
            &self,
            request: RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<SharedRuntimeFilterFinalDomainCompletion>,
            RuntimeFilterContractViolation,
        > {
            if request.contract().kind() != RuntimeFilterProducerKind::FinalDomain {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::RoleMismatch,
                    "fake session only exposes a final-domain completion fence",
                ));
            }
            Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "fake session has no final-domain completion fence",
            ))
        }
    }
    fn membership() -> RuntimeFilterExecutionContract {
        RuntimeFilterExecutionContract::Membership(
            RuntimeFilterMembershipSchema::new(
                &DataType::Int64,
                RuntimeFilterNullSemantics::NeverMatches,
            )
            .expect("membership schema is canonical"),
        )
    }

    #[test]
    fn membership_schema_preserves_the_v1_canonical_bytes_and_digest() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int32,
            RuntimeFilterNullSemantics::NullSafeEqual,
        )
        .expect("int32 null-safe schema is supported");

        assert_eq!(
            schema.canonical_bytes(),
            b"novarocks.runtime-filter.artifact-schema\x01\x04\x02"
        );
        assert_eq!(
            schema.digest(),
            [
                0x92, 0x5d, 0xde, 0xf2, 0x0b, 0x5b, 0xfa, 0x0e, 0xd3, 0xd6, 0xe3, 0x55, 0xf1, 0x18,
                0x79, 0xba, 0xeb, 0x2a, 0x0c, 0x7d, 0x31, 0x0b, 0x56, 0xe2, 0xcc, 0x03, 0x55, 0x57,
                0x10, 0xbf, 0x96, 0x41,
            ]
        );
        assert_eq!(
            RuntimeFilterMembershipSchema::from_canonical(
                schema.canonical_bytes(),
                schema.digest()
            )
            .expect("exact canonical bytes decode"),
            schema
        );
        let raw = RuntimeFilterMembershipSchema::from_canonical(schema.canonical_bytes(), [0; 32])
            .unwrap_err();
        assert_eq!(raw, FilterMembershipSchemaError::DigestMismatch);
        let outer: crate::runtime::task::ExecutionFailure = raw.into();
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&outer);
        assert_eq!(fact.class(), novarocks_failure_contract::FailureClass::Data);
        assert_eq!(
            fact.reason().domain(),
            novarocks_failure_contract::domain::LOCAL_PROGRAM.get()
        );
        assert_eq!(fact.reason().code(), 102);
        let crate::runtime::task::ExecutionFailureCause::RuntimeFilter(
            RuntimeFilterFailure::Schema(original),
        ) = outer.cause()
        else {
            panic!("the real decoder cause must survive the execution port");
        };
        assert_eq!(*original, raw);
    }

    #[test]
    fn named_membership_producer_contract_fixes_its_role() {
        let contract = RuntimeFilterProducerContract::membership(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(9),
            membership(),
        )
        .expect("membership execution contract is accepted");

        assert_eq!(contract.kind(), RuntimeFilterProducerKind::Membership);
        assert_eq!(contract.binding_id(), RuntimeFilterBindingId::new(7));
        assert_eq!(contract.channel_id(), RuntimeFilterChannelId::new(9));
    }
    #[test]
    fn fake_session_distinguishes_exact_admission_from_unavailability() {
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession);
        let producer = RuntimeFilterProducerContract::new(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterProducerKind::Membership,
            membership(),
        );
        assert!(matches!(
            session.open_producer(RuntimeFilterProducerOpenRequest::new(producer, 1)),
            Ok(RuntimeFilterBindOutcome::Bound(handle))
                if handle.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    RuntimeFilterContribution::new(
                        RuntimeFilterContributionKind::Membership,
                        [2; 32],
                        Arc::<[u8]>::from([1_u8, 2]),
                    ),
                ).is_ok()
        ));
        let consumer = RuntimeFilterConsumerContract::new(
            RuntimeFilterBindingId::new(99),
            RuntimeFilterChannelId::new(2),
            ConsumerActivation::BlockingSnapshot,
            membership(),
        );
        assert!(matches!(
            session.subscribe(RuntimeFilterSubscriptionRequest::new(consumer)),
            Ok(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::IncompleteCoverage
            ))
        ));
    }
    #[test]
    fn live_cursor_rejects_version_regression_and_effect_is_versioned() {
        let query = Arc::new(Query);
        let snapshot = RuntimeFilterSnapshot::new(
            RuntimeFilterBindingId::new(3),
            LogicalVersion::new(2),
            [9; 32],
            query,
        );
        let mut cursor = RuntimeFilterLiveCursor::default();
        assert!(cursor.observe(&snapshot).is_ok());
        assert!(cursor.observe(&snapshot).is_err());
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let outcome = evaluator::evaluate_rows(
            RuntimeFilterBindingId::new(3),
            LogicalVersion::new(2),
            snapshot.artifact_query().as_ref(),
            &values,
        )
        .expect("query evaluates");
        assert_eq!(outcome.effect().expect("evaluated query").output_rows(), 2);
    }

    #[test]
    fn logical_version_rejects_zero_at_the_domain_boundary() {
        assert_eq!(LogicalVersion::try_new(0), Err(LogicalVersionError::Zero));
        assert_eq!(LogicalVersion::FIRST.get(), 1);
    }

    #[test]
    fn final_domain_fence_requires_every_partition_to_seal_and_close() {
        let completion = Arc::new(FinalDomainFence::new(2));
        let mut first = completion
            .claim_partition(PartitionId::new(0))
            .expect("first partition is claimable");
        assert!(first.close().is_err());
        first
            .seal(final_domain_payload(7))
            .expect("first partition seals");
        first.close().expect("first partition closes");
        assert!(!completion.completed());

        let mut second = completion
            .claim_partition(PartitionId::new(1))
            .expect("second partition is claimable");
        second
            .seal(final_domain_payload(8))
            .expect("second partition seals");
        second.close().expect("second partition closes");
        assert!(completion.completed());
        assert!(completion.claim_partition(PartitionId::new(1)).is_err());
    }

    fn final_domain_payload(value: i64) -> RuntimeFilterFinalDomain {
        let domain = contribution::ValueDomainDelta::new(
            contribution::MembershipValues::int64([value]),
            false,
        );
        RuntimeFilterFinalDomain::from_value_domain(&domain, [7; 32], 1024)
            .expect("small final domain is canonical")
    }
}

#[cfg(test)]
mod typed_channel_identity_tests {
    use super::*;
    use novarocks_functions::FunctionValueType;
    #[test]
    fn membership_channel_keeps_complete_semantics_separate_from_physical_artifact_digest() {
        let text = FunctionValueType::new(DataType::Utf8, true);
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let construct = |value| {
            RuntimeFilterMembershipSchema::new_value_typed_observed::<FilterMembershipSchemaError>(
                value,
                RuntimeFilterNullSemantics::NeverMatches,
                |_| Ok(()),
                |_| Ok(()),
            )
            .unwrap()
        };
        let text = construct(text);
        let json = construct(json);
        assert_eq!(text.canonical_bytes(), json.canonical_bytes());
        assert_eq!(text.digest(), json.digest());
        assert_ne!(
            RuntimeFilterExecutionContract::Membership(text.clone()),
            RuntimeFilterExecutionContract::Membership(json.clone())
        );
        assert_eq!(
            json.value_type().unwrap().logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        assert_eq!(
            construct(FunctionValueType::new(DataType::Utf8, true)),
            text
        );
    }
}

pub(crate) fn producer_sequence_extent(
    binding: u32,
    current: u64,
) -> crate::runtime::task::ExecutionFailure {
    crate::runtime::task::ExecutionFailure::from_domain(ProducerSequenceExtent { binding, current })
}

#[derive(Debug)]
struct ProducerSequenceExtent {
    binding: u32,
    current: u64,
}
impl std::fmt::Display for ProducerSequenceExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native runtime-filter binding_id={} producer sequence overflow at {}",
            self.binding, self.current
        )
    }
}
impl std::error::Error for ProducerSequenceExtent {}
impl novarocks_failure_contract::FailureProjection for ProducerSequenceExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::task::ExecutionReason::FilterProducerSequence.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Operator),
            dimension: ResourceDimension::Items,
            requested: Some(1),
            required_growth: Some(1),
            limit: Some(u64::MAX),
            live: Some(self.current),
            committed: None,
        })
    }
}

#[cfg(test)]
pub(crate) fn assert_real_tracker_capacity(
    failure: &(impl std::error::Error + novarocks_failure_contract::FailureProjection + 'static),
    limit: i64,
) {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    let fact = failure.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    assert_eq!(fact.reason().domain(), domain::MEMORY.get());
    assert_eq!(fact.reason().code(), 8);
    assert!(fact.representation().is_none());
    let resource = fact.resource().unwrap();
    assert_eq!(resource.limit, Some(limit as u64));
    assert!(resource.requested.unwrap() > 0);
    assert!(resource.committed.unwrap() > limit as u64);
    assert_eq!(resource.request_scope, None);
    assert_eq!(resource.constraint_scope, None);
    let mut source: &dyn std::error::Error = failure;
    loop {
        if let Some(original) = source.downcast_ref::<novarocks_functions::KernelCapacityRefusal>()
        {
            assert_eq!(original.failure_fact(), fact);
            break;
        }
        source = source
            .source()
            .expect("the original tracker capacity receipt");
    }
}

#[cfg(test)]
pub(crate) fn assert_real_vector_refusal(
    failure: &(impl std::error::Error + novarocks_failure_contract::FailureProjection + 'static),
    expected_bytes: Option<u64>,
) {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    use novarocks_functions::allocation_failure::AllocationRefusal;
    let fact = failure.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    assert_eq!(fact.reason().domain(), domain::KERNEL.get());
    assert_eq!(fact.reason().code(), 169);
    assert!(fact.representation().is_none());
    let resource = fact.resource().unwrap();
    assert_eq!(resource.limit, None);
    assert_eq!(resource.committed, None);
    assert_eq!(resource.request_scope, None);
    assert_eq!(resource.constraint_scope, None);
    let mut source: &dyn std::error::Error = failure;
    loop {
        if let Some(original) =
            source.downcast_ref::<AllocationRefusal<allocator_api2::collections::TryReserveError>>()
        {
            let allocator_api2::collections::TryReserveErrorKind::AllocError { layout, .. } =
                original.original().kind()
            else {
                panic!("the actual small request must reach the allocator");
            };
            assert_eq!(resource.requested, Some(layout.size() as u64));
            assert!(layout.size() > 0);
            if let Some(expected) = expected_bytes {
                assert_eq!(layout.size() as u64, expected);
            }
            assert_eq!(fact, original.failure_fact());
            break;
        }
        source = source
            .source()
            .expect("the actual collection reservation error");
    }
}

#[cfg(test)]
mod contract_failure_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureEvidence, FailureProjection};
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    #[test]
    fn violation_frame_keeps_original_producer_fact_and_source() {
        let source = LayoutResourceError::Arithmetic;
        let error = RuntimeFilterContractViolation::from_domain(
            RuntimeFilterContractViolationKind::ContractMismatch,
            source,
        );
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        assert_eq!(error.failure_fact(), source.failure_fact());
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<LayoutResourceError>(),
            Some(&source)
        );
    }
    #[test]
    fn session_close_is_observation_not_a_new_task_origin() {
        let closed = RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::SessionClosed,
            "the session is already closed",
        );
        assert_eq!(
            closed.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert_eq!(closed.failure_fact().class(), FailureClass::Control);
        let capacity = RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::ResourceLimit,
            "actual request refused",
        );
        assert_eq!(
            capacity.failure_fact().evidence(),
            FailureEvidence::Originating
        );
    }
}
