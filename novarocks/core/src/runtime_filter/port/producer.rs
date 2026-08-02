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
use std::sync::{Arc, Weak};

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;

use super::final_domain::FinalDomainShard;
use super::identity::{PartitionId, ProducerSequence};
use super::ordered_bound::OrderedBoundUpdate;
use super::topk_summary::TopKSummary;
use super::value_domain::ValueDomainDelta;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    IgnoredEmpty,
    Installed,
    AlreadyInstalled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallContractErrorKind {
    InvalidEpoch,
    DuplicateIdentity,
    UnsupportedChannelContract,
    UnsupportedMembershipType,
    InvalidCoverage,
    UnknownCoverageWitness,
    DuplicateCoverageWitness,
    EmptyExpectedInstances,
    InvalidConsumerActivation,
    MissingMembershipCapability,
    InvalidPolicy,
    InvalidBudget,
    ConflictingDeployment,
    EpochMismatch,
    ServiceClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallContractError {
    kind: InstallContractErrorKind,
    detail: String,
}

impl InstallContractError {
    pub fn new(kind: InstallContractErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> InstallContractErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for InstallContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter install {:?}: {}",
            self.kind, self.detail
        )
    }
}

impl Error for InstallContractError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeContractViolationKind {
    UnauthorizedBinding,
    UnauthorizedFragmentInstance,
    InvalidPartitionCount,
    PartitionCountConflict,
    InvalidPartition,
    InvalidContributionLease,
    TypeMismatch,
    ConflictingReplay,
    ConflictingTerminalSequence,
    ConflictingArtifactPublish,
    SequenceOutsideTerminalRange,
    FinalDomainMissing,
    OrderedContractMismatch,
    OrderedBoundLoosened,
    LogicalVersionOverflow,
    ProducerPortMismatch,
    ConsumerPortMismatch,
    SubscriptionActivationMismatch,
    ServiceUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeContractViolation {
    kind: RuntimeContractViolationKind,
    detail: String,
}

impl RuntimeContractViolation {
    pub fn new(kind: RuntimeContractViolationKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> RuntimeContractViolationKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RuntimeContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter contract violation {:?}: {}",
            self.kind, self.detail
        )
    }
}

impl Error for RuntimeContractViolation {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerFailureReason {
    Cancelled,
    ExecutionFailed,
    UpstreamUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
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
pub struct ProducerOpenRequest {
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    local_partition_count: u32,
}

impl ProducerOpenRequest {
    pub const fn new(
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Self {
        Self {
            binding_id,
            fragment_instance_id,
            local_partition_count,
        }
    }

    pub const fn binding_id(self) -> BindingId {
        self.binding_id
    }

    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn local_partition_count(self) -> u32 {
        self.local_partition_count
    }
}

pub trait ProducerAdapter: Send + Sync {
    fn submit(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

pub trait OrderedBoundProducerAdapter: Send + Sync {
    fn submit_bound(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

pub trait TopKSummaryProducerAdapter: Send + Sync {
    fn submit_summary(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        summary: TopKSummary,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

pub trait FinalDomainProducerAdapter: Send + Sync {
    /// Submit an authority-signed immutable shard. Raw value domains are sealed by
    /// the Service-owned completion session and cannot enter this transport port.
    fn complete(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        shard: FinalDomainShard,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerPortKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

pub enum ProducerHandle {
    Membership(Arc<dyn ProducerAdapter>),
    OrderedBound(Arc<dyn OrderedBoundProducerAdapter>),
    TopKSummary(Arc<dyn TopKSummaryProducerAdapter>),
    FinalDomain(Arc<dyn FinalDomainProducerAdapter>),
}

impl fmt::Debug for ProducerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProducerHandle")
            .field(&self.kind())
            .finish()
    }
}

impl ProducerHandle {
    pub const fn kind(&self) -> ProducerPortKind {
        match self {
            Self::Membership(_) => ProducerPortKind::Membership,
            Self::OrderedBound(_) => ProducerPortKind::OrderedBound,
            Self::TopKSummary(_) => ProducerPortKind::TopKSummary,
            Self::FinalDomain(_) => ProducerPortKind::FinalDomain,
        }
    }

    pub fn downgrade(&self) -> ProducerHandleWeak {
        match self {
            Self::Membership(handle) => ProducerHandleWeak::Membership(Arc::downgrade(handle)),
            Self::OrderedBound(handle) => ProducerHandleWeak::OrderedBound(Arc::downgrade(handle)),
            Self::TopKSummary(handle) => ProducerHandleWeak::TopKSummary(Arc::downgrade(handle)),
            Self::FinalDomain(handle) => ProducerHandleWeak::FinalDomain(Arc::downgrade(handle)),
        }
    }

    pub fn into_membership(self) -> Result<Arc<dyn ProducerAdapter>, RuntimeContractViolation> {
        match self {
            Self::Membership(handle) => Ok(handle),
            Self::OrderedBound(_) => Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered producer handle cannot be used as a membership producer",
            )),
            Self::TopKSummary(_) => Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "top-k summary producer handle cannot be used as a membership producer",
            )),
            Self::FinalDomain(_) => Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "final-domain producer handle cannot be used as a membership producer",
            )),
        }
    }

    pub fn into_final_domain(
        self,
    ) -> Result<Arc<dyn FinalDomainProducerAdapter>, RuntimeContractViolation> {
        match self {
            Self::FinalDomain(handle) => Ok(handle),
            Self::Membership(_) | Self::OrderedBound(_) | Self::TopKSummary(_) => {
                Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "non-final producer handle cannot be used as a final-domain producer",
                ))
            }
        }
    }
}

pub enum ProducerHandleWeak {
    Membership(Weak<dyn ProducerAdapter>),
    OrderedBound(Weak<dyn OrderedBoundProducerAdapter>),
    TopKSummary(Weak<dyn TopKSummaryProducerAdapter>),
    FinalDomain(Weak<dyn FinalDomainProducerAdapter>),
}

impl ProducerHandleWeak {
    pub const fn kind(&self) -> ProducerPortKind {
        match self {
            Self::Membership(_) => ProducerPortKind::Membership,
            Self::OrderedBound(_) => ProducerPortKind::OrderedBound,
            Self::TopKSummary(_) => ProducerPortKind::TopKSummary,
            Self::FinalDomain(_) => ProducerPortKind::FinalDomain,
        }
    }

    pub fn upgrade(&self) -> Option<ProducerHandle> {
        match self {
            Self::Membership(handle) => handle.upgrade().map(ProducerHandle::Membership),
            Self::OrderedBound(handle) => handle.upgrade().map(ProducerHandle::OrderedBound),
            Self::TopKSummary(handle) => handle.upgrade().map(ProducerHandle::TopKSummary),
            Self::FinalDomain(handle) => handle.upgrade().map(ProducerHandle::FinalDomain),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::runtime_filter::port::final_domain::FinalDomainShard;
    use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
    use crate::runtime_filter::port::topk_summary::TopKSummary;

    use super::{
        FinalDomainProducerAdapter, ProducerFailureReason, ProducerHandle, ProducerPortKind,
        RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
        TopKSummaryProducerAdapter,
    };

    struct TopKAdapter;

    struct FinalDomainAdapter;

    impl FinalDomainProducerAdapter for FinalDomainAdapter {
        fn complete(
            &self,
            _partition_id: PartitionId,
            _sequence: ProducerSequence,
            _shard: FinalDomainShard,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not complete")
        }

        fn close_partition(
            &self,
            _partition_id: PartitionId,
            _terminal: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not close")
        }

        fn fail(
            &self,
            _reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not fail")
        }
    }

    impl TopKSummaryProducerAdapter for TopKAdapter {
        fn submit_summary(
            &self,
            _partition_id: PartitionId,
            _sequence: ProducerSequence,
            _summary: TopKSummary,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not submit")
        }

        fn close_partition(
            &self,
            _partition_id: PartitionId,
            _terminal: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not close")
        }

        fn fail(
            &self,
            _reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            unreachable!("typed handle tests do not fail")
        }
    }

    fn contract_violation_name(kind: RuntimeContractViolationKind) -> &'static str {
        match kind {
            RuntimeContractViolationKind::UnauthorizedBinding => "unauthorized-binding",
            RuntimeContractViolationKind::UnauthorizedFragmentInstance => {
                "unauthorized-fragment-instance"
            }
            RuntimeContractViolationKind::InvalidPartitionCount => "invalid-partition-count",
            RuntimeContractViolationKind::PartitionCountConflict => "partition-count-conflict",
            RuntimeContractViolationKind::InvalidPartition => "invalid-partition",
            RuntimeContractViolationKind::InvalidContributionLease => "invalid-contribution-lease",
            RuntimeContractViolationKind::TypeMismatch => "type-mismatch",
            RuntimeContractViolationKind::ConflictingReplay => "conflicting-replay",
            RuntimeContractViolationKind::ConflictingArtifactPublish => {
                "conflicting-artifact-publish"
            }
            RuntimeContractViolationKind::ConflictingTerminalSequence => {
                "conflicting-terminal-sequence"
            }
            RuntimeContractViolationKind::SequenceOutsideTerminalRange => {
                "sequence-outside-terminal-range"
            }
            RuntimeContractViolationKind::FinalDomainMissing => "final-domain-missing",
            RuntimeContractViolationKind::OrderedContractMismatch => "ordered-contract-mismatch",
            RuntimeContractViolationKind::OrderedBoundLoosened => "ordered-bound-loosened",
            RuntimeContractViolationKind::LogicalVersionOverflow => "logical-version-overflow",
            RuntimeContractViolationKind::ProducerPortMismatch => "producer-port-mismatch",
            RuntimeContractViolationKind::ConsumerPortMismatch => "consumer-port-mismatch",
            RuntimeContractViolationKind::SubscriptionActivationMismatch => {
                "subscription-activation-mismatch"
            }
            RuntimeContractViolationKind::ServiceUnavailable => "service-unavailable",
        }
    }

    #[test]
    fn runtime_contract_violations_exclude_resource_limits() {
        assert_eq!(
            contract_violation_name(RuntimeContractViolationKind::TypeMismatch),
            "type-mismatch"
        );
    }

    #[test]
    fn topk_summary_handle_downgrades_and_upgrades_without_losing_kind() {
        let adapter: Arc<dyn TopKSummaryProducerAdapter> = Arc::new(TopKAdapter);
        let handle = ProducerHandle::TopKSummary(adapter);
        let weak = handle.downgrade();

        assert_eq!(handle.kind(), ProducerPortKind::TopKSummary);
        assert_eq!(weak.kind(), ProducerPortKind::TopKSummary);
        assert_eq!(
            weak.upgrade().expect("strong typed handle is alive").kind(),
            ProducerPortKind::TopKSummary
        );
    }

    #[test]
    fn final_domain_handle_is_typed_and_wrong_handle_conversions_fail_closed() {
        let adapter: Arc<dyn FinalDomainProducerAdapter> = Arc::new(FinalDomainAdapter);
        let handle = ProducerHandle::FinalDomain(adapter);
        let weak = handle.downgrade();

        assert_eq!(handle.kind(), ProducerPortKind::FinalDomain);
        assert_eq!(weak.kind(), ProducerPortKind::FinalDomain);
        assert_eq!(
            weak.upgrade().expect("strong typed handle is alive").kind(),
            ProducerPortKind::FinalDomain
        );
        assert!(handle.into_final_domain().is_ok());

        for wrong in [ProducerHandle::TopKSummary(Arc::new(TopKAdapter))] {
            let Err(error) = wrong.into_final_domain() else {
                panic!("wrong typed producer handle must fail closed")
            };
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::ProducerPortMismatch
            );
        }
    }
}
