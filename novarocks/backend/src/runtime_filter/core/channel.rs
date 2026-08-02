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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow::datatypes::DataType;

use novarocks::runtime_filter_transition::model::contract::{
    BindingId, ChannelId, CompletionRequirement, CoverageWitnessId, NullSemantics,
    ReductionRequirement, RuntimeFilterLogicalDomain,
};
use novarocks::runtime_filter_transition::model::coverage::Coverage;
use novarocks::runtime_filter_transition::port::artifact::ArtifactMembershipSchema;
use novarocks::runtime_filter_transition::port::events::{
    FinalDomainRejectionKind, ProducerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
};
use novarocks::runtime_filter_transition::port::final_domain::{
    FinalDomainShard, RuntimeCompletionFenceContract,
};
use novarocks::runtime_filter_transition::port::identity::{
    ContributionIdentity, DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence,
    ProducerStreamId, RuntimeFilterParticipantId,
};
use novarocks::runtime_filter_transition::port::install::RuntimeFilterChannelDeployment;
use novarocks::runtime_filter_transition::port::ordered_bound::{
    OrderedBoundUpdate, RuntimeOrderContract,
};
use novarocks::runtime_filter_transition::port::producer::{
    ProducerFailureReason, RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
};
use novarocks::runtime_filter_transition::port::subscription::UnavailableReason;
use novarocks::runtime_filter_transition::port::support::{
    RetainedMemoryReservation, RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use novarocks::runtime_filter_transition::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummary,
};
#[cfg(test)]
use novarocks::runtime_filter_transition::port::value_domain::MembershipValues;
use novarocks::runtime_filter_transition::port::value_domain::{LogicalSnapshot, ValueDomainDelta};
use novarocks_types::UniqueId;

use super::coverage::{CoverageProgress, WitnessProgress, evaluate};
use super::error::ChannelBuildError;
use super::ordered_reducer::{OrderedApplyOutcome, OrderedCloseOutcome, OrderedReducer};
use super::reducer::{MembershipReducer, ReducerError};
use super::state::{InstanceState, LogicalTerminal, TerminalProgress};
use super::topk_reducer::{TopKApplyOutcome, TopKCloseOutcome, TopKSummaryReducer};

const REPLAY_METADATA_BYTES: usize = size_of::<u64>() + 32;
const TERMINAL_METADATA_BYTES: usize = size_of::<u64>();

#[derive(Debug)]
pub(crate) enum ChannelAction {
    None,
    Progress {
        order: Option<u64>,
        outcome: SubmitOutcome,
        events: Vec<RuntimeFilterEvent>,
    },
    VisibleSnapshot {
        order: u64,
        outcome: SubmitOutcome,
        version: LogicalVersion,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Completed {
        order: u64,
        outcome: SubmitOutcome,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Unavailable {
        order: u64,
        outcome: SubmitOutcome,
        reason: UnavailableReason,
        events: Vec<RuntimeFilterEvent>,
    },
    CompletedWithoutArtifact {
        order: u64,
        outcome: SubmitOutcome,
        events: Vec<RuntimeFilterEvent>,
    },
    DegradedLogical {
        order: u64,
        outcome: SubmitOutcome,
        reason: UnavailableReason,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Cancelled {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProducerIngressCoreSnapshot {
    pub(crate) local_partition_count: Option<u32>,
    pub(crate) materialized_partition_count: usize,
    pub(crate) terminal_progress: TerminalProgress,
    pub(crate) membership_values: Option<MembershipValues>,
}

impl ChannelAction {
    pub(crate) fn logical_terminal(&self) -> Option<LogicalTerminal> {
        match self {
            Self::Completed { .. } => Some(LogicalTerminal::Completed),
            Self::CompletedWithoutArtifact { .. } => {
                Some(LogicalTerminal::CompletedWithoutArtifact)
            }
            Self::DegradedLogical { reason, .. } => Some(LogicalTerminal::DegradedLogical(*reason)),
            Self::Unavailable { reason, .. } => Some(LogicalTerminal::Unavailable(*reason)),
            Self::Cancelled { .. } => Some(LogicalTerminal::Cancelled),
            Self::None | Self::Progress { .. } | Self::VisibleSnapshot { .. } => None,
        }
    }

    pub(crate) fn outcome(&self) -> SubmitOutcome {
        match self {
            Self::None | Self::Cancelled { .. } => SubmitOutcome::TerminalNoop,
            Self::Progress { outcome, .. }
            | Self::VisibleSnapshot { outcome, .. }
            | Self::Completed { outcome, .. }
            | Self::Unavailable { outcome, .. }
            | Self::CompletedWithoutArtifact { outcome, .. }
            | Self::DegradedLogical { outcome, .. } => *outcome,
        }
    }

    pub(crate) fn snapshot(&self) -> Option<Arc<LogicalSnapshot>> {
        match self {
            Self::VisibleSnapshot { snapshot, .. }
            | Self::Completed { snapshot, .. }
            | Self::DegradedLogical { snapshot, .. } => Some(snapshot.clone()),
            _ => None,
        }
    }

    pub(crate) const fn unavailable_reason(&self) -> Option<UnavailableReason> {
        match self {
            Self::Unavailable { reason, .. } | Self::DegradedLogical { reason, .. } => {
                Some(*reason)
            }
            _ => None,
        }
    }

    pub(crate) fn events(&self) -> &[RuntimeFilterEvent] {
        match self {
            Self::None => &[],
            Self::Progress { events, .. }
            | Self::VisibleSnapshot { events, .. }
            | Self::Completed { events, .. }
            | Self::Unavailable { events, .. }
            | Self::CompletedWithoutArtifact { events, .. }
            | Self::DegradedLogical { events, .. }
            | Self::Cancelled { events, .. } => events,
        }
    }

    pub(crate) const fn dispatch_order(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Progress { order, .. } => *order,
            Self::VisibleSnapshot { order, .. }
            | Self::Completed { order, .. }
            | Self::Unavailable { order, .. }
            | Self::CompletedWithoutArtifact { order, .. }
            | Self::DegradedLogical { order, .. }
            | Self::Cancelled { order, .. } => Some(*order),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FinalDomainRejection {
    violation: RuntimeContractViolation,
    action: ChannelAction,
}

impl FinalDomainRejection {
    #[cfg(test)]
    pub(crate) const fn kind(&self) -> RuntimeContractViolationKind {
        self.violation.kind()
    }

    pub(crate) fn into_parts(self) -> (RuntimeContractViolation, ChannelAction) {
        (self.violation, self.action)
    }
}

struct ProducerRuntime {
    witness_id: CoverageWitnessId,
    instances: BTreeMap<UniqueId, InstanceState>,
}

enum ChannelTerminal {
    Collecting,
    Completed {
        order: u64,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Unavailable {
        order: u64,
        reason: UnavailableReason,
        events: Vec<RuntimeFilterEvent>,
    },
    CompletedWithoutArtifact {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
    DegradedLogical {
        order: u64,
        reason: UnavailableReason,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Cancelled {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
}

#[derive(Debug)]
enum OrderedCoreReducer {
    Direct(OrderedReducer),
    TopK(TopKSummaryReducer),
}

impl OrderedCoreReducer {
    fn global(
        &self,
    ) -> Option<&Arc<novarocks::runtime_filter_transition::port::value_domain::OrderedBoundDomain>>
    {
        match self {
            Self::Direct(reducer) => reducer.global(),
            Self::TopK(reducer) => reducer.global(),
        }
    }

    fn estimated_retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Direct(reducer) => reducer.estimated_retained_bytes(),
            Self::TopK(reducer) => reducer.estimated_retained_bytes(),
        }
    }

    fn retain_protocol_tombstones(&mut self) -> Option<usize> {
        match self {
            Self::Direct(reducer) => reducer.retain_protocol_tombstones(),
            Self::TopK(reducer) => reducer.retain_protocol_tombstones(),
        }
    }

    fn terminal_partition_count(&self, binding_id: BindingId, instance: UniqueId) -> usize {
        match self {
            Self::Direct(reducer) => reducer.terminal_partition_count(binding_id, instance),
            Self::TopK(reducer) => reducer.terminal_partition_count(binding_id, instance),
        }
    }

    fn direct(&self) -> Option<&OrderedReducer> {
        match self {
            Self::Direct(reducer) => Some(reducer),
            Self::TopK(_) => None,
        }
    }

    fn topk(&self) -> Option<&TopKSummaryReducer> {
        match self {
            Self::Direct(_) => None,
            Self::TopK(reducer) => Some(reducer),
        }
    }

    fn topk_mut(&mut self) -> Option<&mut TopKSummaryReducer> {
        match self {
            Self::Direct(_) => None,
            Self::TopK(reducer) => Some(reducer),
        }
    }
}

struct OrderedCoreState {
    reducer: OrderedCoreReducer,
    availability_witnesses: BTreeMap<CoverageWitnessId, WitnessProgress>,
    latest: Option<Arc<LogicalSnapshot>>,
}

struct ChannelState {
    terminal: ChannelTerminal,
    producers: BTreeMap<BindingId, ProducerRuntime>,
    witnesses: BTreeMap<CoverageWitnessId, WitnessProgress>,
    reducer: Option<MembershipReducer>,
    ordered: Option<OrderedCoreState>,
    reservation: RetainedMemoryReservation,
    next_dispatch_order: u64,
}

enum MembershipContributionMode {
    Incremental,
    FencedFinal(Arc<RuntimeCompletionFenceContract>),
}

struct LockedAction {
    action: ChannelAction,
    release_after_unlock: Option<RetainedMemoryReservation>,
}

impl LockedAction {
    fn without_release(action: ChannelAction) -> Self {
        Self {
            action,
            release_after_unlock: None,
        }
    }

    fn finish(self) -> ChannelAction {
        drop(self.release_after_unlock);
        self.action
    }

    fn add_release_after_unlock(&mut self, release: RetainedMemoryReservation) {
        if release.bytes() == 0 {
            return;
        }
        if let Some(existing) = self.release_after_unlock.as_mut() {
            existing
                .absorb(release)
                .expect("deferred releases share the channel memory account");
        } else {
            self.release_after_unlock = Some(release);
        }
    }
}

pub(crate) struct RuntimeFilterChannel {
    event_identity: RuntimeFilterEventIdentity,
    channel_id: ChannelId,
    availability_coverage: Coverage,
    terminal_coverage: Coverage,
    data_type: Option<DataType>,
    null_semantics: Option<NullSemantics>,
    membership_mode: Option<MembershipContributionMode>,
    max_contribution_bytes: u64,
    max_reducer_bytes: u64,
    deadline: OnceLock<Instant>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    state: Mutex<ChannelState>,
    #[cfg(test)]
    before_final_semantic_rejection: Mutex<Option<Arc<dyn Fn(u64) + Send + Sync>>>,
}

impl RuntimeFilterChannel {
    fn final_domain_contract(&self) -> Option<&Arc<RuntimeCompletionFenceContract>> {
        match self.membership_mode.as_ref() {
            Some(MembershipContributionMode::FencedFinal(contract)) => Some(contract),
            Some(MembershipContributionMode::Incremental) | None => None,
        }
    }

    pub(crate) fn contribution_identity(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
    ) -> ContributionIdentity {
        ContributionIdentity::new(
            self.event_identity.query_id(),
            self.event_identity.participant_id(),
            self.channel_id,
            self.event_identity.epoch(),
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        )
    }

    #[cfg(test)]
    pub(crate) fn set_before_final_semantic_rejection_hook(
        &self,
        hook: Arc<dyn Fn(u64) + Send + Sync>,
    ) {
        *self.before_final_semantic_rejection.lock().unwrap() = Some(hook);
    }

    #[cfg(test)]
    fn run_before_final_semantic_rejection_hook(&self, next_dispatch_order: u64) {
        let hook = self.before_final_semantic_rejection.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(next_dispatch_order);
        }
    }

    pub(crate) fn ordered_rejection_action(
        &self,
        identity: ContributionIdentity,
        violation: RuntimeContractViolationKind,
    ) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        ChannelAction::Progress {
            order: Some(next_dispatch_order(&mut state)),
            outcome: SubmitOutcome::TerminalNoop,
            events: vec![RuntimeFilterEvent::OrderedUpdateRejected {
                identity,
                violation,
            }],
        }
    }

    pub(crate) fn topk_rejection_action(
        &self,
        identity: ContributionIdentity,
        violation: RuntimeContractViolationKind,
    ) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        ChannelAction::Progress {
            order: Some(next_dispatch_order(&mut state)),
            outcome: SubmitOutcome::TerminalNoop,
            events: vec![RuntimeFilterEvent::TopKSummaryRejected {
                identity,
                violation,
            }],
        }
    }

    fn reject_final_locked(
        &self,
        state: &mut ChannelState,
        identity: ContributionIdentity,
        violation: RuntimeContractViolation,
    ) -> FinalDomainRejection {
        let action = ChannelAction::Progress {
            order: Some(next_dispatch_order(state)),
            outcome: SubmitOutcome::TerminalNoop,
            events: vec![RuntimeFilterEvent::FinalDomainShardRejected {
                identity,
                rejection: FinalDomainRejectionKind::Contract(violation.kind()),
            }],
        };
        #[cfg(test)]
        self.run_before_final_semantic_rejection_hook(state.next_dispatch_order);
        FinalDomainRejection { violation, action }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
        deployment: &RuntimeFilterChannelDeployment,
        deadline: Instant,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, ChannelBuildError> {
        let channel =
            Self::new_unanchored(query_id, participant_id, epoch, deployment, memory_account)?;
        channel
            .deadline
            .set(deadline)
            .expect("new channel deadline is initialized exactly once");
        Ok(channel)
    }

    pub(crate) fn new_unanchored(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
        deployment: &RuntimeFilterChannelDeployment,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, ChannelBuildError> {
        Self::new_unanchored_with_final_domain_contract(
            query_id,
            participant_id,
            epoch,
            deployment,
            memory_account,
            None,
        )
    }

    pub(crate) fn new_unanchored_with_final_domain_contract(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
        deployment: &RuntimeFilterChannelDeployment,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        final_domain_contract: Option<Arc<RuntimeCompletionFenceContract>>,
    ) -> Result<Self, ChannelBuildError> {
        let (data_type, null_semantics, reducer, ordered_reducer) =
            match deployment.logical_domain() {
                RuntimeFilterLogicalDomain::Membership {
                    value_type,
                    null_semantics,
                } => {
                    let reducer = MembershipReducer::try_new(value_type.clone(), *null_semantics)
                        .map_err(|_| ChannelBuildError::UnsupportedMembershipType)?;
                    (
                        Some(value_type.clone()),
                        Some(*null_semantics),
                        Some(reducer),
                        None,
                    )
                }
                RuntimeFilterLogicalDomain::OrderedBound(plan) => {
                    let reducer = match deployment.reduction_requirement() {
                        ReductionRequirement::TightenOrderedBound => {
                            let contract = Arc::new(
                                RuntimeOrderContract::try_from_plan(plan)
                                    .map_err(|_| ChannelBuildError::UnsupportedContract)?,
                            );
                            OrderedCoreReducer::Direct(OrderedReducer::new(contract))
                        }
                        ReductionRequirement::MergeTopKSummary(requirement) => {
                            let contract = Arc::new(
                                RuntimeTopKSummaryContract::try_from_plan(plan, requirement)
                                    .map_err(|_| ChannelBuildError::UnsupportedContract)?,
                            );
                            OrderedCoreReducer::TopK(TopKSummaryReducer::new(contract))
                        }
                        ReductionRequirement::SetUnion => {
                            return Err(ChannelBuildError::UnsupportedContract);
                        }
                    };
                    (None, None, None, Some(reducer))
                }
            };
        let membership_mode = match (
            deployment.logical_domain(),
            deployment.completion_requirement(),
        ) {
            (
                RuntimeFilterLogicalDomain::Membership {
                    value_type,
                    null_semantics,
                },
                CompletionRequirement::FencedFinalDomain(fence_kind),
            ) => {
                let schema = ArtifactMembershipSchema::new(value_type, *null_semantics)
                    .map_err(|_| ChannelBuildError::UnsupportedMembershipType)?;
                let expected = RuntimeCompletionFenceContract::try_from_install(
                    query_id,
                    epoch,
                    deployment.channel_id(),
                    fence_kind,
                    &schema,
                )
                .map_err(|_| ChannelBuildError::UnsupportedContract)?;
                let contract = match final_domain_contract {
                    Some(contract) if *contract == expected => contract,
                    Some(_) => return Err(ChannelBuildError::UnsupportedContract),
                    None => Arc::new(expected),
                };
                Some(MembershipContributionMode::FencedFinal(contract))
            }
            (RuntimeFilterLogicalDomain::Membership { .. }, _) => {
                if final_domain_contract.is_some() {
                    return Err(ChannelBuildError::UnsupportedContract);
                }
                Some(MembershipContributionMode::Incremental)
            }
            (RuntimeFilterLogicalDomain::OrderedBound(_), _) => {
                if final_domain_contract.is_some() {
                    return Err(ChannelBuildError::UnsupportedContract);
                }
                None
            }
        };
        let mut witnesses = BTreeMap::new();
        let producers = deployment
            .producers()
            .iter()
            .map(|(binding_id, producer)| {
                witnesses.insert(producer.coverage_witness_id(), WitnessProgress::Pending);
                let instances = producer
                    .expected_fragment_instances()
                    .iter()
                    .copied()
                    .map(|instance| (instance, InstanceState::default()))
                    .collect();
                (
                    *binding_id,
                    ProducerRuntime {
                        witness_id: producer.coverage_witness_id(),
                        instances,
                    },
                )
            })
            .collect();
        if deployment
            .availability_coverage()
            .witness_ids_in_order()
            .iter()
            .chain(deployment.terminal_coverage().witness_ids_in_order().iter())
            .any(|witness| !witnesses.contains_key(witness))
        {
            return Err(ChannelBuildError::MissingCoverageWitness);
        }
        Ok(Self {
            event_identity: RuntimeFilterEventIdentity::new(
                query_id,
                participant_id,
                deployment.channel_id(),
                epoch,
            ),
            channel_id: deployment.channel_id(),
            availability_coverage: deployment.availability_coverage().clone(),
            terminal_coverage: deployment.terminal_coverage().clone(),
            data_type,
            null_semantics,
            membership_mode,
            max_contribution_bytes: deployment.policy().max_contribution_bytes,
            max_reducer_bytes: deployment.core_budget().max_reducer_bytes(),
            deadline: OnceLock::new(),
            memory_account,
            state: Mutex::new(ChannelState {
                terminal: ChannelTerminal::Collecting,
                producers,
                witnesses: witnesses.clone(),
                reducer,
                ordered: ordered_reducer.map(|reducer| OrderedCoreState {
                    reducer,
                    availability_witnesses: witnesses,
                    latest: None,
                }),
                reservation: RetainedMemoryReservation::empty(),
                next_dispatch_order: 0,
            }),
            #[cfg(test)]
            before_final_semantic_rejection: Mutex::new(None),
        })
    }

    pub(crate) fn initialize_deadline(&self, deadline: Instant) -> Result<(), ()> {
        self.deadline.set(deadline).map_err(|_| ())
    }

    pub(crate) fn open_producer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap();
        let installed_count =
            instance_ref(&state, binding_id, fragment_instance_id)?.local_partition_count();
        if local_partition_count == 0 {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartitionCount,
                "local partition count must be non-zero",
            ));
        }
        if let Some(installed_count) = installed_count {
            return if installed_count == local_partition_count {
                Ok(SubmitOutcome::Duplicate)
            } else {
                Err(violation(
                    RuntimeContractViolationKind::PartitionCountConflict,
                    "producer instance reopened with a different partition count",
                ))
            };
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(SubmitOutcome::TerminalNoop);
        }
        let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
        instance.open(local_partition_count);
        Ok(SubmitOutcome::Applied)
    }

    pub(crate) fn preflight_remote_open(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        partition_id: PartitionId,
    ) -> Result<(), RuntimeContractViolation> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let existing =
            instance_ref(&state, binding_id, fragment_instance_id)?.local_partition_count();
        if let Some(existing) = existing
            && existing != local_partition_count
        {
            return Err(violation(
                RuntimeContractViolationKind::PartitionCountConflict,
                "producer instance reopened with a different partition count",
            ));
        }
        if local_partition_count == 0 {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartitionCount,
                "local partition count must be non-zero",
            ));
        }
        if partition_id.get() >= local_partition_count {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartition,
                "producer partition is outside the declared local partition count",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn producer_ingress_core_snapshot(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> ProducerIngressCoreSnapshot {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let instance = instance_ref(&state, binding_id, fragment_instance_id)
            .expect("test observes an installed producer");
        ProducerIngressCoreSnapshot {
            local_partition_count: instance.local_partition_count(),
            materialized_partition_count: instance.materialized_partition_count(),
            terminal_progress: instance.progress,
            membership_values: state
                .reducer
                .as_ref()
                .map(|reducer| reducer.domain().values().clone()),
        }
    }

    pub(crate) fn authorize_submit(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
    ) -> Result<(), RuntimeContractViolation> {
        let state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        if self.final_domain_contract().is_some() {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "fenced-final channel cannot accept incremental membership deltas",
            ));
        }
        let Some(data_type) = self.data_type.as_ref() else {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered channel cannot accept membership deltas",
            ));
        };
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let mut incoming_reservation: Option<RetainedMemoryReservation> = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if !delta.matches_data_type(data_type) {
                return Err(violation(
                    RuntimeContractViolationKind::TypeMismatch,
                    "delta type does not match channel membership type",
                ));
            }
            let contribution_bytes = match delta.estimated_contribution_bytes() {
                Ok(bytes) => bytes,
                Err(_) => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            if temporary_lease.bytes() != contribution_bytes {
                return Err(violation(
                    RuntimeContractViolationKind::InvalidContributionLease,
                    "temporary contribution lease does not match canonical payload size",
                ));
            }
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let fingerprint = delta.fingerprint().bytes();
            {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                if let Some(previous) =
                    partition.and_then(|partition| partition.seen.get(&sequence))
                {
                    return if *previous == fingerprint {
                        Ok(ChannelAction::Progress {
                            order: Some(next_dispatch_order(&mut state)),
                            outcome: SubmitOutcome::Duplicate,
                            events: vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }],
                        })
                    } else {
                        Err(violation(
                            RuntimeContractViolationKind::ConflictingReplay,
                            "same contribution identity carried a different payload",
                        ))
                    };
                }
            }

            let instance_progress =
                instance_mut(&mut state, binding_id, fragment_instance_id)?.progress;
            let (partition_progress, terminal_sequence) = {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                partition.map_or((TerminalProgress::Pending, None), |partition| {
                    (partition.progress, partition.terminal_sequence)
                })
            };
            if instance_progress == TerminalProgress::Impossible
                || partition_progress == TerminalProgress::Impossible
            {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            if partition_progress == TerminalProgress::Satisfied {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "new delta arrived after partition close",
                ));
            }
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            if terminal_sequence.is_some_and(|terminal| sequence >= terminal) {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "delta sequence is outside the exclusive terminal range",
                ));
            }

            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            let projection = match state
                .reducer
                .as_ref()
                .expect("membership channel owns a membership reducer")
                .preflight(&delta)
            {
                Ok(projection) => projection,
                Err(ReducerError::TypeMismatch | ReducerError::UnsupportedType) => {
                    return Err(violation(
                        RuntimeContractViolationKind::TypeMismatch,
                        "delta type does not match channel membership type",
                    ));
                }
                Err(ReducerError::SizeOverflow) => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            let retained_growth = match projection
                .retained_growth()
                .checked_add(REPLAY_METADATA_BYTES)
            {
                Some(bytes) => bytes,
                None => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            let projected_total = state.reservation.bytes().checked_add(retained_growth);
            if projected_total
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            if incoming_reservation
                .as_ref()
                .map(|reservation| reservation.bytes())
                != Some(retained_growth)
            {
                if reservation_failed_for == Some(retained_growth) {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(incoming_reservation.take());
                match RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    retained_growth,
                ) {
                    Ok(reservation) => incoming_reservation = Some(reservation),
                    Err(_) => reservation_failed_for = Some(retained_growth),
                }
                continue;
            }
            let incoming = incoming_reservation
                .take()
                .expect("matching retained reservation must exist");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            state
                .reducer
                .as_mut()
                .expect("membership channel owns a membership reducer")
                .commit_preflighted(&delta)
                .expect("preflighted reducer commit must preserve type invariants");
            let partition = partition_mut_for_commit(
                &mut state,
                binding_id,
                fragment_instance_id,
                partition_id,
            );
            partition.seen.insert(sequence, fingerprint);
            if partition.is_gapless() {
                partition.progress = TerminalProgress::Satisfied;
            }
            let events = vec![RuntimeFilterEvent::DeltaAccepted { identity }];
            let locked = self.refresh_after_progress(
                &mut state,
                binding_id,
                fragment_instance_id,
                SubmitOutcome::Applied,
                events,
            );
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn authorize_final(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        authority_installed: bool,
        shard: &FinalDomainShard,
    ) -> Result<(), FinalDomainRejection> {
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let mut state = self.state.lock().unwrap();
        if !authority_installed {
            return Err(self.reject_final_locked(
                &mut state,
                identity,
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "producer adapter has no installed completion-fence authority",
                ),
            ));
        }
        let Some(contract) = self.final_domain_contract() else {
            return Err(self.reject_final_locked(
                &mut state,
                identity,
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "non-final channel cannot accept final-domain shards",
                ),
            ));
        };
        if let Err(error) = partition_state(&state, binding_id, fragment_instance_id, partition_id)
        {
            return Err(self.reject_final_locked(&mut state, identity, error));
        }
        if let Err(error) = shard.verify_scope(
            contract,
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        ) {
            return Err(self.reject_final_locked(&mut state, identity, error));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn complete_final(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        shard: FinalDomainShard,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, FinalDomainRejection> {
        let stream = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let mut incoming_reservation: Option<RetainedMemoryReservation> = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            let Some(contract) = self.final_domain_contract() else {
                return Err(self.reject_final_locked(
                    &mut state,
                    identity,
                    violation(
                        RuntimeContractViolationKind::ProducerPortMismatch,
                        "non-final channel cannot accept final-domain shards",
                    ),
                ));
            };
            if let Err(error) =
                partition_state(&state, binding_id, fragment_instance_id, partition_id)
            {
                return Err(self.reject_final_locked(&mut state, identity, error));
            }
            if let Err(error) = shard.verify_scope(contract, stream, sequence) {
                return Err(self.reject_final_locked(&mut state, identity, error));
            }
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                return Ok(terminal_action_from_state(&state));
            }
            let replay_digest = shard.replay_digest();
            let previous =
                match partition_state(&state, binding_id, fragment_instance_id, partition_id) {
                    Ok(partition) => partition
                        .and_then(|partition| partition.seen.get(&sequence))
                        .copied(),
                    Err(error) => {
                        return Err(self.reject_final_locked(&mut state, identity, error));
                    }
                };
            if let Some(previous) = previous {
                return if previous == replay_digest {
                    Ok(ChannelAction::Progress {
                        order: Some(next_dispatch_order(&mut state)),
                        outcome: SubmitOutcome::Duplicate,
                        events: vec![RuntimeFilterEvent::FinalDomainShardDuplicate { identity }],
                    })
                } else {
                    Err(self.reject_final_locked(
                        &mut state,
                        identity,
                        violation(
                            RuntimeContractViolationKind::ConflictingReplay,
                            "same final-domain contribution identity carried a different payload",
                        ),
                    ))
                };
            }
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            let instance_progress = match instance_mut(&mut state, binding_id, fragment_instance_id)
            {
                Ok(instance) => instance.progress,
                Err(error) => {
                    return Err(self.reject_final_locked(&mut state, identity, error));
                }
            };
            let (partition_progress, terminal_sequence) =
                match partition_state(&state, binding_id, fragment_instance_id, partition_id) {
                    Ok(partition) => partition
                        .map_or((TerminalProgress::Pending, None), |partition| {
                            (partition.progress, partition.terminal_sequence)
                        }),
                    Err(error) => {
                        return Err(self.reject_final_locked(&mut state, identity, error));
                    }
                };
            if instance_progress == TerminalProgress::Impossible
                || partition_progress == TerminalProgress::Impossible
            {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            if partition_progress == TerminalProgress::Satisfied {
                return Err(self.reject_final_locked(
                    &mut state,
                    identity,
                    violation(
                        RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                        "new final domain arrived after partition close",
                    ),
                ));
            }
            if terminal_sequence.is_some_and(|terminal| sequence >= terminal) {
                return Err(self.reject_final_locked(
                    &mut state,
                    identity,
                    violation(
                        RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                        "final-domain sequence is outside the exclusive terminal range",
                    ),
                ));
            }
            let contribution_bytes = match shard.canonical_contribution_bytes() {
                Some(bytes) => bytes,
                None => {
                    let locked = self.make_final_resource_unavailable(
                        &mut state,
                        identity,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            if temporary_lease.bytes() != contribution_bytes {
                return Err(self.reject_final_locked(
                    &mut state,
                    identity,
                    violation(
                        RuntimeContractViolationKind::InvalidContributionLease,
                        "temporary contribution lease does not match canonical final-domain size",
                    ),
                ));
            }
            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_final_resource_unavailable(
                    &mut state,
                    identity,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            let projection = match state
                .reducer
                .as_ref()
                .expect("membership channel owns a membership reducer")
                .preflight(shard.domain())
            {
                Ok(projection) => projection,
                Err(ReducerError::TypeMismatch | ReducerError::UnsupportedType) => {
                    return Err(self.reject_final_locked(
                        &mut state,
                        identity,
                        violation(
                            RuntimeContractViolationKind::TypeMismatch,
                            "final domain type does not match channel membership type",
                        ),
                    ));
                }
                Err(ReducerError::SizeOverflow) => {
                    let locked = self.make_final_resource_unavailable(
                        &mut state,
                        identity,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            let retained_growth = match projection
                .retained_growth()
                .checked_add(REPLAY_METADATA_BYTES)
            {
                Some(bytes) => bytes,
                None => {
                    let locked = self.make_final_resource_unavailable(
                        &mut state,
                        identity,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            if state
                .reservation
                .bytes()
                .checked_add(retained_growth)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_final_resource_unavailable(
                    &mut state,
                    identity,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            if incoming_reservation
                .as_ref()
                .map(RetainedMemoryReservation::bytes)
                != Some(retained_growth)
            {
                if reservation_failed_for == Some(retained_growth) {
                    let locked = self.make_final_resource_unavailable(
                        &mut state,
                        identity,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(incoming_reservation.take());
                match RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    retained_growth,
                ) {
                    Ok(reservation) => incoming_reservation = Some(reservation),
                    Err(_) => reservation_failed_for = Some(retained_growth),
                }
                continue;
            }
            let incoming = incoming_reservation
                .take()
                .expect("matching final-domain reservation must exist");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_final_resource_unavailable(
                    &mut state,
                    identity,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            state
                .reducer
                .as_mut()
                .expect("membership channel owns a membership reducer")
                .commit_preflighted(shard.domain())
                .expect("preflighted final-domain commit preserves type invariants");
            let partition = partition_mut_for_commit(
                &mut state,
                binding_id,
                fragment_instance_id,
                partition_id,
            );
            partition.seen.insert(sequence, replay_digest);
            if partition.is_gapless() {
                partition.progress = TerminalProgress::Satisfied;
            }
            let locked = self.refresh_after_progress(
                &mut state,
                binding_id,
                fragment_instance_id,
                SubmitOutcome::Applied,
                vec![RuntimeFilterEvent::FinalDomainShardAccepted { identity }],
            );
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn submit_ordered(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let contribution_bytes = update.canonical_contribution_bytes().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "ordered contribution canonical size overflowed",
            )
        })?;
        if temporary_lease.bytes() != contribution_bytes {
            return Err(violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "temporary contribution lease does not match canonical ordered payload size",
            ));
        }
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let stream_id = identity.stream();
        let mut metadata_reservation = None;
        let mut snapshot_reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept ordered bounds",
                )
            })?;
            let direct = ordered.reducer.direct().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "top-k summary channel cannot accept direct ordered bounds",
                )
            })?;
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                direct.validate_tombstone_update(stream_id, sequence, &update)?;
                return Ok(terminal_action_from_state(&state));
            }
            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let before_bytes = direct.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let mut next_reducer = direct.clone();
            let apply_outcome = next_reducer.apply(stream_id, sequence, update.clone())?;
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            let after_bytes = next_reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            debug_assert_eq!(
                state.reservation.bytes(),
                before_bytes,
                "ordered reducer reservation tracks current retained bytes"
            );
            let metadata_growth = after_bytes.saturating_sub(state.reservation.bytes());
            let mut availability_witnesses = ordered.availability_witnesses.clone();
            if !matches!(
                apply_outcome,
                OrderedApplyOutcome::Stale | OrderedApplyOutcome::Duplicate
            ) {
                let witness_id = state
                    .producers
                    .get(&binding_id)
                    .expect("authorized ordered producer")
                    .witness_id;
                availability_witnesses
                    .get_mut(&witness_id)
                    .expect("ordered availability witness is installed")
                    .advance(WitnessProgress::Satisfied);
            }
            let availability = evaluate(&self.availability_coverage, &availability_witnesses);
            let publish = availability == CoverageProgress::Satisfied
                && (ordered.latest.is_none()
                    || matches!(apply_outcome, OrderedApplyOutcome::GlobalTightened(_)));
            let (version, snapshot_bytes) = if publish {
                let version =
                    ordered
                        .latest
                        .as_ref()
                        .map_or(Ok(LogicalVersion::FIRST), |latest| {
                            latest.version().checked_next().ok_or_else(|| {
                                violation(
                                    RuntimeContractViolationKind::LogicalVersionOverflow,
                                    "ordered logical version overflowed",
                                )
                            })
                        })?;
                let bytes = next_reducer
                    .global()
                    .expect("satisfied ordered availability owns a global bound")
                    .estimated_retained_bytes()
                    .ok_or_else(|| {
                        violation(
                            RuntimeContractViolationKind::OrderedContractMismatch,
                            "ordered snapshot retained size overflowed",
                        )
                    })?;
                (Some(version), bytes)
            } else {
                (None, 0)
            };
            let projected_bytes = after_bytes.checked_add(snapshot_bytes);
            if projected_bytes
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let reservations_match = metadata_reservation
                .as_ref()
                .map(RetainedMemoryReservation::bytes)
                == Some(metadata_growth)
                && snapshot_reservation
                    .as_ref()
                    .map(RetainedMemoryReservation::bytes)
                    == Some(snapshot_bytes);
            if !reservations_match {
                let required = (metadata_growth, snapshot_bytes);
                if reservation_failed_for == Some(required) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(metadata_reservation.take());
                drop(snapshot_reservation.take());
                metadata_reservation = RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    metadata_growth,
                )
                .ok();
                snapshot_reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), snapshot_bytes)
                        .ok();
                if metadata_reservation.is_none() || snapshot_reservation.is_none() {
                    reservation_failed_for = Some(required);
                }
                continue;
            }

            let incoming_metadata = metadata_reservation
                .take()
                .expect("matching ordered metadata reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming_metadata) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let mut events = match apply_outcome {
                OrderedApplyOutcome::Stale => {
                    vec![RuntimeFilterEvent::OrderedUpdateStale { identity }]
                }
                OrderedApplyOutcome::Duplicate => {
                    vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }]
                }
                OrderedApplyOutcome::SequenceAdvancedEqual => {
                    vec![RuntimeFilterEvent::OrderedUpdateEqual { identity }]
                }
                OrderedApplyOutcome::StreamTightened => {
                    vec![RuntimeFilterEvent::OrderedStreamTightened { identity }]
                }
                OrderedApplyOutcome::GlobalTightened(_) => Vec::new(),
            };
            if !matches!(
                apply_outcome,
                OrderedApplyOutcome::Stale | OrderedApplyOutcome::Duplicate
            ) {
                events.insert(0, RuntimeFilterEvent::OrderedUpdateApplied { identity });
            }
            let outcome = match apply_outcome {
                OrderedApplyOutcome::Stale => SubmitOutcome::Stale,
                OrderedApplyOutcome::Duplicate => SubmitOutcome::Duplicate,
                OrderedApplyOutcome::SequenceAdvancedEqual => SubmitOutcome::SequenceAdvancedEqual,
                OrderedApplyOutcome::StreamTightened => SubmitOutcome::StreamAcceptedNoGlobalChange,
                OrderedApplyOutcome::GlobalTightened(_) => SubmitOutcome::Published,
            };
            let availability_was_satisfied = state.ordered.as_ref().is_some_and(|ordered| {
                evaluate(&self.availability_coverage, &ordered.availability_witnesses)
                    == CoverageProgress::Satisfied
            });
            let release_after_unlock = state.reservation.split_off_excess(after_bytes);
            let ordered = state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state");
            ordered.reducer = OrderedCoreReducer::Direct(next_reducer);
            ordered.availability_witnesses = availability_witnesses;
            if !availability_was_satisfied && availability == CoverageProgress::Satisfied {
                events.push(RuntimeFilterEvent::OrderedAvailabilityReached {
                    identity: self.event_identity,
                });
            }
            let published = version.map(|version| {
                let domain = ordered
                    .reducer
                    .global()
                    .expect("published ordered version owns a global bound")
                    .clone();
                let reservation = snapshot_reservation
                    .take()
                    .expect("published ordered version owns exact reservation");
                let snapshot = Arc::new(LogicalSnapshot::ordered(
                    self.channel_id,
                    version,
                    domain,
                    reservation,
                ));
                ordered.latest = Some(snapshot.clone());
                events.push(RuntimeFilterEvent::OrderedGlobalTightened { identity, version });
                events.push(RuntimeFilterEvent::LogicalVersionPublished {
                    identity: self.event_identity,
                    version,
                });
                snapshot
            });
            if refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id) {
                events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                    identity: ProducerEventIdentity::new(
                        self.event_identity,
                        binding_id,
                        fragment_instance_id,
                    ),
                });
            }
            let mut locked =
                self.refresh_after_ordered_progress(&mut state, outcome, published, events);
            locked.add_release_after_unlock(release_after_unlock);
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn submit_topk_summary(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        summary: TopKSummary,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let contribution_bytes = summary.canonical_contribution_bytes().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "top-k summary canonical size overflowed",
            )
        })?;
        if temporary_lease.bytes() != contribution_bytes {
            return Err(violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "temporary contribution lease does not match canonical top-k summary size",
            ));
        }
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let stream_id = identity.stream();
        let mut metadata_reservation = None;
        let mut snapshot_reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept top-k summaries",
                )
            })?;
            let topk = ordered.reducer.topk().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "direct ordered channel cannot accept top-k summaries",
                )
            })?;
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                topk.validate_tombstone_summary(stream_id, sequence, &summary)?;
                return Ok(terminal_action_from_state(&state));
            }
            let projection = topk.preflight_apply(stream_id, sequence, &summary)?;
            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let before_bytes = topk.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "top-k reducer retained size overflowed",
                )
            })?;
            let stream_was_covered = topk.stream_covered(stream_id);
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            debug_assert_eq!(
                state.reservation.bytes(),
                before_bytes,
                "top-k reducer reservation tracks current retained bytes"
            );
            let after_bytes = projection.retained_bytes();
            let metadata_growth = after_bytes.saturating_sub(state.reservation.bytes());
            let availability_witnesses = projected_topk_availability(
                &state,
                binding_id,
                fragment_instance_id,
                stream_was_covered,
                projection.stream_covered(),
            );
            let availability = evaluate(&self.availability_coverage, &availability_witnesses);
            let publish = availability == CoverageProgress::Satisfied
                && projection.global().is_some()
                && (ordered.latest.is_none()
                    || matches!(projection.outcome(), TopKApplyOutcome::GlobalTightened));
            let (version, snapshot_bytes) = if publish {
                let version =
                    ordered
                        .latest
                        .as_ref()
                        .map_or(Ok(LogicalVersion::FIRST), |latest| {
                            latest.version().checked_next().ok_or_else(|| {
                                violation(
                                    RuntimeContractViolationKind::LogicalVersionOverflow,
                                    "top-k logical version overflowed",
                                )
                            })
                        })?;
                let bytes = projection
                    .global()
                    .expect("published top-k projection owns a global bound")
                    .estimated_retained_bytes()
                    .ok_or_else(|| {
                        violation(
                            RuntimeContractViolationKind::OrderedContractMismatch,
                            "top-k snapshot retained size overflowed",
                        )
                    })?;
                (Some(version), bytes)
            } else {
                (None, 0)
            };
            if after_bytes
                .checked_add(snapshot_bytes)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let reservations_match = metadata_reservation
                .as_ref()
                .map(RetainedMemoryReservation::bytes)
                == Some(metadata_growth)
                && snapshot_reservation
                    .as_ref()
                    .map(RetainedMemoryReservation::bytes)
                    == Some(snapshot_bytes);
            if !reservations_match {
                let required = (metadata_growth, snapshot_bytes);
                if reservation_failed_for == Some(required) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(metadata_reservation.take());
                drop(snapshot_reservation.take());
                metadata_reservation = RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    metadata_growth,
                )
                .ok();
                snapshot_reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), snapshot_bytes)
                        .ok();
                if metadata_reservation.is_none() || snapshot_reservation.is_none() {
                    reservation_failed_for = Some(required);
                }
                continue;
            }

            let availability_was_satisfied =
                evaluate(&self.availability_coverage, &ordered.availability_witnesses)
                    == CoverageProgress::Satisfied;

            let incoming_metadata = metadata_reservation
                .take()
                .expect("matching top-k metadata reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming_metadata) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let apply_outcome = projection.outcome();
            let outcome = match apply_outcome {
                TopKApplyOutcome::Stale => SubmitOutcome::Stale,
                TopKApplyOutcome::Duplicate => SubmitOutcome::Duplicate,
                TopKApplyOutcome::SequenceAdvancedEqual => SubmitOutcome::SequenceAdvancedEqual,
                TopKApplyOutcome::StreamUpdated => SubmitOutcome::StreamAcceptedNoGlobalChange,
                TopKApplyOutcome::GlobalTightened if version.is_some() => SubmitOutcome::Published,
                TopKApplyOutcome::GlobalTightened => SubmitOutcome::StreamAcceptedNoGlobalChange,
            };
            let release_after_unlock = state.reservation.split_off_excess(after_bytes);
            let ordered = state
                .ordered
                .as_mut()
                .expect("top-k channel owns ordered state");
            ordered
                .reducer
                .topk_mut()
                .expect("top-k strategy remains installed")
                .commit_apply(projection);
            ordered.availability_witnesses = availability_witnesses;
            let mut events = match apply_outcome {
                TopKApplyOutcome::Stale => {
                    vec![RuntimeFilterEvent::TopKSummaryStale { identity }]
                }
                TopKApplyOutcome::Duplicate | TopKApplyOutcome::SequenceAdvancedEqual => {
                    vec![RuntimeFilterEvent::TopKSummaryEqual { identity }]
                }
                TopKApplyOutcome::GlobalTightened => Vec::new(),
                TopKApplyOutcome::StreamUpdated => {
                    vec![RuntimeFilterEvent::TopKStreamUpdated { identity }]
                }
            };
            if !matches!(
                apply_outcome,
                TopKApplyOutcome::Stale | TopKApplyOutcome::Duplicate
            ) {
                events.insert(0, RuntimeFilterEvent::TopKSummaryApplied { identity });
            }
            if !availability_was_satisfied && availability == CoverageProgress::Satisfied {
                events.push(RuntimeFilterEvent::OrderedAvailabilityReached {
                    identity: self.event_identity,
                });
            }
            let published = version.map(|version| {
                let domain = ordered
                    .reducer
                    .global()
                    .expect("published top-k version owns a global bound")
                    .clone();
                let reservation = snapshot_reservation
                    .take()
                    .expect("published top-k version owns exact reservation");
                let snapshot = Arc::new(LogicalSnapshot::ordered(
                    self.channel_id,
                    version,
                    domain,
                    reservation,
                ));
                ordered.latest = Some(snapshot.clone());
                events.push(RuntimeFilterEvent::OrderedGlobalTightened { identity, version });
                events.push(RuntimeFilterEvent::LogicalVersionPublished {
                    identity: self.event_identity,
                    version,
                });
                snapshot
            });
            if refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id) {
                events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                    identity: ProducerEventIdentity::new(
                        self.event_identity,
                        binding_id,
                        fragment_instance_id,
                    ),
                });
            }
            let mut locked =
                self.refresh_after_ordered_progress(&mut state, outcome, published, events);
            locked.add_release_after_unlock(release_after_unlock);
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn close_topk_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let identity = self.contribution_identity(
            binding_id,
            fragment_instance_id,
            partition_id,
            terminal_sequence,
        );
        let stream_id = identity.stream();
        let mut metadata_reservation = None;
        let mut snapshot_reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept top-k close",
                )
            })?;
            let topk = ordered.reducer.topk().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "direct ordered channel cannot accept top-k close",
                )
            })?;
            let before_bytes = topk.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "top-k reducer retained size overflowed",
                )
            })?;
            let stream_was_covered = topk.stream_covered(stream_id);
            let projection = topk.preflight_close(stream_id, terminal_sequence)?;
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            debug_assert_eq!(
                state.reservation.bytes(),
                before_bytes,
                "top-k reducer reservation tracks current retained bytes"
            );
            let after_bytes = projection.retained_bytes();
            let metadata_growth = after_bytes.saturating_sub(state.reservation.bytes());
            let availability_witnesses = projected_topk_availability(
                &state,
                binding_id,
                fragment_instance_id,
                stream_was_covered,
                projection.stream_covered(),
            );
            let availability = evaluate(&self.availability_coverage, &availability_witnesses);
            let publish = availability == CoverageProgress::Satisfied
                && ordered.latest.is_none()
                && topk.global().is_some();
            let (version, snapshot_bytes) = if publish {
                let bytes = topk
                    .global()
                    .expect("published top-k close owns a global bound")
                    .estimated_retained_bytes()
                    .ok_or_else(|| {
                        violation(
                            RuntimeContractViolationKind::OrderedContractMismatch,
                            "top-k snapshot retained size overflowed",
                        )
                    })?;
                (Some(LogicalVersion::FIRST), bytes)
            } else {
                (None, 0)
            };
            if after_bytes
                .checked_add(snapshot_bytes)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let reservations_match = metadata_reservation
                .as_ref()
                .map(RetainedMemoryReservation::bytes)
                == Some(metadata_growth)
                && snapshot_reservation
                    .as_ref()
                    .map(RetainedMemoryReservation::bytes)
                    == Some(snapshot_bytes);
            if !reservations_match {
                let required = (metadata_growth, snapshot_bytes);
                if reservation_failed_for == Some(required) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(metadata_reservation.take());
                drop(snapshot_reservation.take());
                metadata_reservation = RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    metadata_growth,
                )
                .ok();
                snapshot_reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), snapshot_bytes)
                        .ok();
                if metadata_reservation.is_none() || snapshot_reservation.is_none() {
                    reservation_failed_for = Some(required);
                }
                continue;
            }

            let availability_was_satisfied =
                evaluate(&self.availability_coverage, &ordered.availability_witnesses)
                    == CoverageProgress::Satisfied;

            let incoming_metadata = metadata_reservation
                .take()
                .expect("matching top-k close reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming_metadata) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let close_outcome = projection.outcome();
            let outcome = if version.is_some() {
                SubmitOutcome::Published
            } else {
                match close_outcome {
                    TopKCloseOutcome::Duplicate => SubmitOutcome::Duplicate,
                    TopKCloseOutcome::PendingFinalSnapshot => SubmitOutcome::PendingFinalSnapshot,
                    TopKCloseOutcome::Satisfied => SubmitOutcome::Applied,
                }
            };
            let release_after_unlock = state.reservation.split_off_excess(after_bytes);
            let ordered = state
                .ordered
                .as_mut()
                .expect("top-k channel owns ordered state");
            ordered
                .reducer
                .topk_mut()
                .expect("top-k strategy remains installed")
                .commit_close(projection);
            ordered.availability_witnesses = availability_witnesses;
            let mut events = match close_outcome {
                TopKCloseOutcome::Duplicate => {
                    vec![RuntimeFilterEvent::TopKSummaryEqual { identity }]
                }
                TopKCloseOutcome::PendingFinalSnapshot => {
                    vec![RuntimeFilterEvent::TopKStreamUpdated { identity }]
                }
                TopKCloseOutcome::Satisfied => {
                    vec![RuntimeFilterEvent::TopKSummaryApplied { identity }]
                }
            };
            if !availability_was_satisfied && availability == CoverageProgress::Satisfied {
                events.push(RuntimeFilterEvent::OrderedAvailabilityReached {
                    identity: self.event_identity,
                });
            }
            let published = version.map(|version| {
                let domain = ordered
                    .reducer
                    .global()
                    .expect("published top-k close owns a global bound")
                    .clone();
                let reservation = snapshot_reservation
                    .take()
                    .expect("published top-k close owns exact reservation");
                let snapshot = Arc::new(LogicalSnapshot::ordered(
                    self.channel_id,
                    version,
                    domain,
                    reservation,
                ));
                ordered.latest = Some(snapshot.clone());
                events.push(RuntimeFilterEvent::OrderedGlobalTightened { identity, version });
                events.push(RuntimeFilterEvent::LogicalVersionPublished {
                    identity: self.event_identity,
                    version,
                });
                snapshot
            });
            if refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id) {
                events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                    identity: ProducerEventIdentity::new(
                        self.event_identity,
                        binding_id,
                        fragment_instance_id,
                    ),
                });
            }
            let mut locked =
                self.refresh_after_ordered_progress(&mut state, outcome, published, events);
            locked.add_release_after_unlock(release_after_unlock);
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn close_ordered_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let stream_id = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let mut reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept ordered close",
                )
            })?;
            let direct = ordered.reducer.direct().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "top-k summary channel cannot accept direct ordered close",
                )
            })?;
            let before_bytes = direct.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let mut next_reducer = direct.clone();
            let close_outcome = next_reducer.close(stream_id, terminal_sequence)?;
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            let after_bytes = next_reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let growth = after_bytes.saturating_sub(before_bytes);
            if state
                .reservation
                .bytes()
                .checked_add(growth)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            if reservation.as_ref().map(RetainedMemoryReservation::bytes) != Some(growth) {
                if reservation_failed_for == Some(growth) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(reservation.take());
                reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), growth).ok();
                if reservation.is_none() {
                    reservation_failed_for = Some(growth);
                }
                continue;
            }
            let incoming = reservation
                .take()
                .expect("matching ordered close reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state")
                .reducer = OrderedCoreReducer::Direct(next_reducer);
            let mut events = Vec::new();
            if refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id) {
                events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                    identity: ProducerEventIdentity::new(
                        self.event_identity,
                        binding_id,
                        fragment_instance_id,
                    ),
                });
            }
            let outcome = match close_outcome {
                OrderedCloseOutcome::Duplicate => SubmitOutcome::Duplicate,
                OrderedCloseOutcome::PendingFinalSnapshot => SubmitOutcome::PendingFinalSnapshot,
                OrderedCloseOutcome::Satisfied => SubmitOutcome::Applied,
            };
            let locked = self.refresh_after_ordered_progress(&mut state, outcome, None, events);
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn close_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        if self.final_domain_contract().is_some() {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "fenced-final channel requires the final-domain close port",
            ));
        }
        self.close_membership_partition(
            binding_id,
            fragment_instance_id,
            partition_id,
            terminal_sequence,
        )
    }

    pub(crate) fn close_final_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        if self.final_domain_contract().is_none() {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "non-final channel cannot use the final-domain close port",
            ));
        }
        self.authorize_submit(binding_id, fragment_instance_id, partition_id)?;
        if terminal_sequence.get() == 0 {
            return Err(violation(
                RuntimeContractViolationKind::FinalDomainMissing,
                "fenced-final partition cannot close before final-domain sequence zero",
            ));
        }
        self.close_membership_partition(
            binding_id,
            fragment_instance_id,
            partition_id,
            terminal_sequence,
        )
    }

    fn close_membership_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let mut incoming_reservation: Option<RetainedMemoryReservation> = None;
        let mut reservation_failed = false;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                if let Some(previous) = partition.and_then(|partition| partition.terminal_sequence)
                {
                    if previous != terminal_sequence {
                        return Err(violation(
                            RuntimeContractViolationKind::ConflictingTerminalSequence,
                            "partition close replay changed terminal sequence",
                        ));
                    }
                    return Ok(progress(SubmitOutcome::Duplicate));
                }
            }
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
            if instance.progress == TerminalProgress::Impossible {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let partition =
                partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if partition.is_some_and(|partition| {
                partition
                    .seen
                    .keys()
                    .next_back()
                    .is_some_and(|sequence| *sequence >= terminal_sequence)
            }) {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "partition already contains a delta outside terminal range",
                ));
            }
            let projected_total = state
                .reservation
                .bytes()
                .checked_add(TERMINAL_METADATA_BYTES);
            if projected_total
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            if incoming_reservation
                .as_ref()
                .map(|reservation| reservation.bytes())
                != Some(TERMINAL_METADATA_BYTES)
            {
                if reservation_failed {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(incoming_reservation.take());
                match RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    TERMINAL_METADATA_BYTES,
                ) {
                    Ok(reservation) => incoming_reservation = Some(reservation),
                    Err(_) => reservation_failed = true,
                }
                continue;
            }
            let incoming = incoming_reservation
                .take()
                .expect("matching terminal reservation must exist");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let partition = partition_mut_for_commit(
                &mut state,
                binding_id,
                fragment_instance_id,
                partition_id,
            );
            partition.terminal_sequence = Some(terminal_sequence);
            let outcome = if partition.is_gapless() {
                partition.progress = TerminalProgress::Satisfied;
                SubmitOutcome::Applied
            } else {
                SubmitOutcome::PendingGap
            };
            let mut events = Vec::new();
            if outcome == SubmitOutcome::PendingGap {
                events.push(RuntimeFilterEvent::SequenceGapObserved {
                    identity: ContributionIdentity::new(
                        self.event_identity.query_id(),
                        self.event_identity.participant_id(),
                        self.channel_id,
                        self.event_identity.epoch(),
                        ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
                        terminal_sequence,
                    ),
                });
            }
            let locked = self.refresh_after_progress(
                &mut state,
                binding_id,
                fragment_instance_id,
                outcome,
                events,
            );
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn fail_instance(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        reason: ProducerFailureReason,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap();
        instance_mut(&mut state, binding_id, fragment_instance_id)?;
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        if state.ordered.is_some() {
            let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
            if instance.progress != TerminalProgress::Pending {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            instance.progress = TerminalProgress::Impossible;
            let witness_id = state
                .producers
                .get(&binding_id)
                .expect("authorized ordered producer")
                .witness_id;
            state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state")
                .availability_witnesses
                .get_mut(&witness_id)
                .expect("ordered availability witness is installed")
                .advance(WitnessProgress::Impossible);
            refresh_ordered_witness(&mut state, binding_id);
            let producer_identity =
                ProducerEventIdentity::new(self.event_identity, binding_id, fragment_instance_id);
            let locked = self.refresh_after_ordered_progress(
                &mut state,
                SubmitOutcome::CoverageStillPossible,
                None,
                vec![RuntimeFilterEvent::ProducerInstanceFailed {
                    identity: producer_identity,
                    reason,
                }],
            );
            drop(state);
            return Ok(locked.finish());
        }
        let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
        if instance.progress != TerminalProgress::Pending {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        instance.progress = TerminalProgress::Impossible;
        let producer_identity =
            ProducerEventIdentity::new(self.event_identity, binding_id, fragment_instance_id);
        let locked = self.refresh_after_progress(
            &mut state,
            binding_id,
            fragment_instance_id,
            SubmitOutcome::Applied,
            vec![RuntimeFilterEvent::ProducerInstanceFailed {
                identity: producer_identity,
                reason,
            }],
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn expire_deadline(&self, now: Instant) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if self.deadline.get().is_none_or(|deadline| now < *deadline)
            || !matches!(state.terminal, ChannelTerminal::Collecting)
        {
            return ChannelAction::None;
        }
        let locked = if state.ordered.is_some() {
            self.make_ordered_unavailable_or_degraded(
                &mut state,
                UnavailableReason::IncompleteCoverage,
                SubmitOutcome::TerminalNoop,
                Vec::new(),
            )
        } else {
            self.make_unavailable(
                &mut state,
                UnavailableReason::IncompleteCoverage,
                SubmitOutcome::TerminalNoop,
            )
        };
        drop(state);
        locked.finish()
    }

    pub(crate) fn cancel(&self) -> ChannelAction {
        self.cancel_with_pending_producer_failures(&BTreeSet::new())
    }

    /// Cancels the channel and, under the same core-state linearization point,
    /// records failures for the still-pending producer instances owned by this
    /// service participant. Keeping these events in the returned action makes
    /// them obey the channel dispatch FIFO together with `ChannelCancelled`.
    pub(crate) fn cancel_with_pending_producer_failures(
        &self,
        locally_owned: &BTreeSet<(BindingId, UniqueId)>,
    ) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return ChannelAction::None;
        }
        let mut events = state
            .producers
            .iter()
            .flat_map(|(binding_id, producer)| {
                producer
                    .instances
                    .iter()
                    .filter_map(move |(finst_id, instance)| {
                        let identity = (*binding_id, *finst_id);
                        (instance.progress == TerminalProgress::Pending
                            && locally_owned.contains(&identity))
                        .then(|| RuntimeFilterEvent::ProducerInstanceFailed {
                            identity: ProducerEventIdentity::new(
                                self.event_identity,
                                *binding_id,
                                *finst_id,
                            ),
                            reason: ProducerFailureReason::Cancelled,
                        })
                    })
            })
            .collect::<Vec<_>>();
        let release_after_unlock = self.detach_collecting_state(&mut state);
        events.push(RuntimeFilterEvent::ChannelCancelled {
            identity: self.event_identity,
        });
        let order = next_dispatch_order(&mut state);
        state.terminal = ChannelTerminal::Cancelled {
            order,
            events: events.clone(),
        };
        let action = ChannelAction::Cancelled { order, events };
        drop(state);
        drop(release_after_unlock);
        action
    }

    pub(crate) fn terminal_action(&self) -> ChannelAction {
        let state = self.state.lock().unwrap();
        terminal_action_from_state(&state)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reject_submit_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: &ValueDomainDelta,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        if self.final_domain_contract().is_some() {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "fenced-final channel cannot accept incremental membership deltas",
            ));
        }
        let Some(data_type) = self.data_type.as_ref() else {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered channel cannot accept membership deltas",
            ));
        };
        let identity = ContributionIdentity::new(
            self.event_identity.query_id(),
            self.event_identity.participant_id(),
            self.channel_id,
            self.event_identity.epoch(),
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        );
        let mut state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        if !delta.matches_data_type(data_type) {
            return Err(violation(
                RuntimeContractViolationKind::TypeMismatch,
                "delta type does not match channel membership type",
            ));
        }
        if matches!(
            state.terminal,
            ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
        ) {
            return Ok(terminal_action_from_state(&state));
        }
        let fingerprint = delta.fingerprint().bytes();
        if let Some(previous) =
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?
                .and_then(|partition| partition.seen.get(&sequence))
        {
            return if *previous == fingerprint {
                Ok(ChannelAction::Progress {
                    order: Some(next_dispatch_order(&mut state)),
                    outcome: SubmitOutcome::Duplicate,
                    events: vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }],
                })
            } else {
                Err(violation(
                    RuntimeContractViolationKind::ConflictingReplay,
                    "same contribution identity carried a different payload",
                ))
            };
        }
        let instance_progress =
            instance_mut(&mut state, binding_id, fragment_instance_id)?.progress;
        let (partition_progress, terminal_sequence) =
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?
                .map_or((TerminalProgress::Pending, None), |partition| {
                    (partition.progress, partition.terminal_sequence)
                });
        if instance_progress == TerminalProgress::Impossible
            || partition_progress == TerminalProgress::Impossible
        {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        if partition_progress == TerminalProgress::Satisfied {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "new delta arrived after partition close",
            ));
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        if terminal_sequence.is_some_and(|terminal| sequence >= terminal) {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "delta sequence is outside the exclusive terminal range",
            ));
        }
        let locked = self.make_unavailable(
            &mut state,
            UnavailableReason::ResourceLimit,
            SubmitOutcome::TerminalNoop,
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn reject_final_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        shard: &FinalDomainShard,
    ) -> Result<ChannelAction, FinalDomainRejection> {
        let stream = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let identity =
            self.contribution_identity(binding_id, fragment_instance_id, partition_id, sequence);
        let mut state = self.state.lock().unwrap();
        let Some(contract) = self.final_domain_contract() else {
            return Err(self.reject_final_locked(
                &mut state,
                identity,
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "non-final channel cannot accept final-domain shards",
                ),
            ));
        };
        if let Err(error) = partition_state(&state, binding_id, fragment_instance_id, partition_id)
        {
            return Err(self.reject_final_locked(&mut state, identity, error));
        }
        if let Err(error) = shard.verify_scope(contract, stream, sequence) {
            return Err(self.reject_final_locked(&mut state, identity, error));
        }
        if matches!(
            state.terminal,
            ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
        ) {
            return Ok(terminal_action_from_state(&state));
        }
        let replay_digest = shard.replay_digest();
        let previous = match partition_state(&state, binding_id, fragment_instance_id, partition_id)
        {
            Ok(partition) => partition
                .and_then(|partition| partition.seen.get(&sequence))
                .copied(),
            Err(error) => {
                return Err(self.reject_final_locked(&mut state, identity, error));
            }
        };
        if let Some(previous) = previous {
            return if previous == replay_digest {
                Ok(ChannelAction::Progress {
                    order: Some(next_dispatch_order(&mut state)),
                    outcome: SubmitOutcome::Duplicate,
                    events: vec![RuntimeFilterEvent::FinalDomainShardDuplicate { identity }],
                })
            } else {
                Err(self.reject_final_locked(
                    &mut state,
                    identity,
                    violation(
                        RuntimeContractViolationKind::ConflictingReplay,
                        "same final-domain contribution identity carried a different payload",
                    ),
                ))
            };
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        let instance_progress = match instance_mut(&mut state, binding_id, fragment_instance_id) {
            Ok(instance) => instance.progress,
            Err(error) => {
                return Err(self.reject_final_locked(&mut state, identity, error));
            }
        };
        let (partition_progress, terminal_sequence) =
            match partition_state(&state, binding_id, fragment_instance_id, partition_id) {
                Ok(partition) => partition.map_or((TerminalProgress::Pending, None), |partition| {
                    (partition.progress, partition.terminal_sequence)
                }),
                Err(error) => {
                    return Err(self.reject_final_locked(&mut state, identity, error));
                }
            };
        if instance_progress == TerminalProgress::Impossible
            || partition_progress == TerminalProgress::Impossible
        {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        if partition_progress == TerminalProgress::Satisfied
            || terminal_sequence.is_some_and(|terminal| sequence >= terminal)
        {
            return Err(self.reject_final_locked(
                &mut state,
                identity,
                violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "final-domain sequence is outside the exclusive terminal range",
                ),
            ));
        }
        let locked =
            self.make_final_resource_unavailable(&mut state, identity, SubmitOutcome::TerminalNoop);
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn reject_ordered_submit_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: &OrderedBoundUpdate,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let stream_id = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let mut state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        let ordered = state.ordered.as_ref().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "membership channel cannot accept ordered bounds",
            )
        })?;
        let direct = ordered.reducer.direct().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "top-k summary channel cannot accept direct ordered bounds",
            )
        })?;
        if matches!(
            state.terminal,
            ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
        ) {
            direct.validate_tombstone_update(stream_id, sequence, update)?;
        } else {
            let mut preflight = direct.clone();
            preflight.apply(stream_id, sequence, update.clone())?;
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        let locked = self.make_ordered_unavailable_or_degraded(
            &mut state,
            UnavailableReason::ResourceLimit,
            SubmitOutcome::TerminalNoop,
            Vec::new(),
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn reject_topk_submit_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        summary: &TopKSummary,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let stream_id = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let mut state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        let ordered = state.ordered.as_ref().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "membership channel cannot accept top-k summaries",
            )
        })?;
        let topk = ordered.reducer.topk().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "direct ordered channel cannot accept top-k summaries",
            )
        })?;
        if matches!(
            state.terminal,
            ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
        ) {
            topk.validate_tombstone_summary(stream_id, sequence, summary)?;
        } else {
            topk.preflight_apply(stream_id, sequence, summary)?;
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        let locked = self.make_ordered_unavailable_or_degraded(
            &mut state,
            UnavailableReason::ResourceLimit,
            SubmitOutcome::TerminalNoop,
            Vec::new(),
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn resource_exhausted(&self) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return terminal_action_from_state(&state);
        }
        let locked = if state.ordered.is_some() {
            self.make_ordered_unavailable_or_degraded(
                &mut state,
                UnavailableReason::ResourceLimit,
                SubmitOutcome::TerminalNoop,
                Vec::new(),
            )
        } else {
            self.make_unavailable(
                &mut state,
                UnavailableReason::ResourceLimit,
                SubmitOutcome::TerminalNoop,
            )
        };
        drop(state);
        locked.finish()
    }

    pub(crate) fn snapshot(&self) -> Option<Arc<LogicalSnapshot>> {
        let state = self.state.lock().unwrap();
        match &state.terminal {
            ChannelTerminal::Completed { snapshot, .. } => Some(snapshot.clone()),
            ChannelTerminal::DegradedLogical { snapshot, .. } => Some(snapshot.clone()),
            _ => state
                .ordered
                .as_ref()
                .and_then(|ordered| ordered.latest.clone()),
        }
    }

    pub(crate) fn availability_progress(&self) -> CoverageProgress {
        let state = self.state.lock().unwrap();
        evaluate(
            &self.availability_coverage,
            state
                .ordered
                .as_ref()
                .map_or(&state.witnesses, |ordered| &ordered.availability_witnesses),
        )
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(
            self.state.lock().unwrap().terminal,
            ChannelTerminal::Collecting
        )
    }

    fn refresh_after_progress(
        &self,
        state: &mut ChannelState,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        let producer = state
            .producers
            .get_mut(&binding_id)
            .expect("authorized producer binding");
        let instance = producer
            .instances
            .get_mut(&fragment_instance_id)
            .expect("authorized producer instance");
        let was_pending = instance.progress == TerminalProgress::Pending;
        instance.refresh_satisfied();
        if was_pending && instance.progress == TerminalProgress::Satisfied {
            events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                identity: ProducerEventIdentity::new(
                    self.event_identity,
                    binding_id,
                    fragment_instance_id,
                ),
            });
        }
        let witness_progress = if producer
            .instances
            .values()
            .any(|instance| instance.progress == TerminalProgress::Impossible)
        {
            WitnessProgress::Impossible
        } else if producer
            .instances
            .values()
            .all(|instance| instance.progress == TerminalProgress::Satisfied)
        {
            WitnessProgress::Satisfied
        } else {
            WitnessProgress::Pending
        };
        state
            .witnesses
            .get_mut(&producer.witness_id)
            .expect("installed producer witness")
            .advance(witness_progress);

        match evaluate(&self.terminal_coverage, &state.witnesses) {
            CoverageProgress::Satisfied => {
                let replacement = MembershipReducer::try_new(
                    self.data_type
                        .clone()
                        .expect("membership channel owns its data type"),
                    self.null_semantics
                        .expect("membership channel owns null semantics"),
                )
                .expect("validated membership type");
                let domain = std::mem::replace(
                    state
                        .reducer
                        .as_mut()
                        .expect("membership channel owns a membership reducer"),
                    replacement,
                )
                .into_domain();
                let reservation =
                    std::mem::replace(&mut state.reservation, RetainedMemoryReservation::empty());
                let snapshot =
                    Arc::new(LogicalSnapshot::first(self.channel_id, domain, reservation));
                events.push(RuntimeFilterEvent::ChannelCompleted {
                    identity: self.event_identity,
                    version: snapshot.version(),
                });
                let order = next_dispatch_order(state);
                state.terminal = ChannelTerminal::Completed {
                    order,
                    snapshot: snapshot.clone(),
                    events: events.clone(),
                };
                LockedAction::without_release(ChannelAction::Completed {
                    order,
                    outcome: SubmitOutcome::Completed,
                    snapshot,
                    events,
                })
            }
            CoverageProgress::Impossible => self.make_unavailable_with_events(
                state,
                UnavailableReason::ProducerFailed,
                outcome,
                events,
            ),
            CoverageProgress::Pending => {
                let order = (!events.is_empty()).then(|| next_dispatch_order(state));
                LockedAction::without_release(ChannelAction::Progress {
                    order,
                    outcome,
                    events,
                })
            }
        }
    }

    fn refresh_after_ordered_progress(
        &self,
        state: &mut ChannelState,
        outcome: SubmitOutcome,
        published: Option<Arc<LogicalSnapshot>>,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        match evaluate(&self.terminal_coverage, &state.witnesses) {
            CoverageProgress::Satisfied => {
                if let Some(snapshot) = state
                    .ordered
                    .as_ref()
                    .and_then(|ordered| ordered.latest.clone())
                {
                    events.push(RuntimeFilterEvent::ChannelCompleted {
                        identity: self.event_identity,
                        version: snapshot.version(),
                    });
                    let order = next_dispatch_order(state);
                    state.terminal = ChannelTerminal::Completed {
                        order,
                        snapshot: snapshot.clone(),
                        events: events.clone(),
                    };
                    LockedAction::without_release(ChannelAction::Completed {
                        order,
                        outcome: SubmitOutcome::Completed,
                        snapshot,
                        events,
                    })
                } else {
                    events.push(RuntimeFilterEvent::ChannelCompletedWithoutArtifact {
                        identity: self.event_identity,
                    });
                    let order = next_dispatch_order(state);
                    state.terminal = ChannelTerminal::CompletedWithoutArtifact {
                        order,
                        events: events.clone(),
                    };
                    LockedAction::without_release(ChannelAction::CompletedWithoutArtifact {
                        order,
                        outcome: SubmitOutcome::CompletedWithoutArtifact,
                        events,
                    })
                }
            }
            CoverageProgress::Impossible => self.make_ordered_unavailable_or_degraded(
                state,
                UnavailableReason::ProducerFailed,
                outcome,
                events,
            ),
            CoverageProgress::Pending => {
                if let Some(snapshot) = published {
                    let order = next_dispatch_order(state);
                    LockedAction::without_release(ChannelAction::VisibleSnapshot {
                        order,
                        outcome,
                        version: snapshot.version(),
                        snapshot,
                        events,
                    })
                } else {
                    let order = (!events.is_empty()).then(|| next_dispatch_order(state));
                    LockedAction::without_release(ChannelAction::Progress {
                        order,
                        outcome,
                        events,
                    })
                }
            }
        }
    }

    fn make_ordered_unavailable_or_degraded(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        if let Some(snapshot) = state
            .ordered
            .as_ref()
            .and_then(|ordered| ordered.latest.clone())
        {
            events.push(RuntimeFilterEvent::ChannelLogicalDegraded {
                identity: self.event_identity,
                reason,
                retained_version: snapshot.version(),
            });
            let order = next_dispatch_order(state);
            state.terminal = ChannelTerminal::DegradedLogical {
                order,
                reason,
                snapshot: snapshot.clone(),
                events: events.clone(),
            };
            LockedAction::without_release(ChannelAction::DegradedLogical {
                order,
                outcome,
                reason,
                snapshot,
                events,
            })
        } else {
            self.make_unavailable_with_events(state, reason, outcome, events)
        }
    }

    fn make_unavailable(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
    ) -> LockedAction {
        self.make_unavailable_with_events(state, reason, outcome, Vec::new())
    }

    fn make_final_resource_unavailable(
        &self,
        state: &mut ChannelState,
        identity: ContributionIdentity,
        outcome: SubmitOutcome,
    ) -> LockedAction {
        self.make_unavailable_with_events(
            state,
            UnavailableReason::ResourceLimit,
            outcome,
            vec![RuntimeFilterEvent::FinalDomainShardRejected {
                identity,
                rejection: FinalDomainRejectionKind::ResourceLimit,
            }],
        )
    }

    fn make_unavailable_with_events(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        let release_after_unlock = self.detach_collecting_state(state);
        events.push(RuntimeFilterEvent::ChannelUnavailable {
            identity: self.event_identity,
            reason,
        });
        let order = next_dispatch_order(state);
        state.terminal = ChannelTerminal::Unavailable {
            order,
            reason,
            events: events.clone(),
        };
        LockedAction {
            action: ChannelAction::Unavailable {
                order,
                outcome,
                reason,
                events,
            },
            release_after_unlock: Some(release_after_unlock),
        }
    }

    fn detach_collecting_state(&self, state: &mut ChannelState) -> RetainedMemoryReservation {
        let mut reservation =
            std::mem::replace(&mut state.reservation, RetainedMemoryReservation::empty());
        if self.data_type.is_some() {
            state.reducer = Some(
                MembershipReducer::try_new(
                    self.data_type
                        .clone()
                        .expect("membership channel owns its data type"),
                    self.null_semantics
                        .expect("membership channel owns null semantics"),
                )
                .expect("validated membership type"),
            );
        } else if let Some(ordered) = state.ordered.as_mut() {
            let tombstone_bytes = ordered
                .reducer
                .retain_protocol_tombstones()
                .expect("accounted ordered tombstone size remains representable");
            let released = reservation.split_off_excess(tombstone_bytes);
            state.reservation = reservation;
            reservation = released;
        }
        for producer in state.producers.values_mut() {
            for instance in producer.instances.values_mut() {
                instance.clear_partitions();
            }
        }
        reservation
    }
}

fn progress(outcome: SubmitOutcome) -> ChannelAction {
    ChannelAction::Progress {
        order: None,
        outcome,
        events: Vec::new(),
    }
}

fn projected_topk_availability(
    state: &ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    stream_was_covered: bool,
    stream_will_be_covered: bool,
) -> BTreeMap<CoverageWitnessId, WitnessProgress> {
    let ordered = state
        .ordered
        .as_ref()
        .expect("top-k channel owns ordered state");
    let topk = ordered
        .reducer
        .topk()
        .expect("top-k availability uses top-k strategy");
    let producer = state
        .producers
        .get(&binding_id)
        .expect("authorized top-k producer");
    let newly_covered = usize::from(!stream_was_covered && stream_will_be_covered);
    let binding_satisfied = producer.instances.iter().all(|(instance_id, instance)| {
        let Some(partition_count) = instance.local_partition_count() else {
            return false;
        };
        let projected_count = topk
            .covered_partition_count(binding_id, *instance_id)
            .saturating_add(if *instance_id == fragment_instance_id {
                newly_covered
            } else {
                0
            });
        usize::try_from(partition_count) == Ok(projected_count)
    });
    let mut witnesses = ordered.availability_witnesses.clone();
    if binding_satisfied {
        witnesses
            .get_mut(&producer.witness_id)
            .expect("top-k availability witness is installed")
            .advance(WitnessProgress::Satisfied);
    }
    witnesses
}

fn refresh_ordered_instance_progress(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> bool {
    let terminal_count = state
        .ordered
        .as_ref()
        .expect("ordered channel owns ordered state")
        .reducer
        .terminal_partition_count(binding_id, fragment_instance_id);
    let instance = instance_mut(state, binding_id, fragment_instance_id)
        .expect("authorized ordered producer instance");
    let was_pending = instance.progress == TerminalProgress::Pending;
    if instance.progress == TerminalProgress::Pending
        && instance
            .local_partition_count()
            .is_some_and(|count| usize::try_from(count) == Ok(terminal_count))
    {
        instance.progress = TerminalProgress::Satisfied;
    }
    let became_satisfied = was_pending && instance.progress == TerminalProgress::Satisfied;
    refresh_ordered_witness(state, binding_id);
    became_satisfied
}

fn refresh_ordered_witness(state: &mut ChannelState, binding_id: BindingId) {
    let producer = state
        .producers
        .get(&binding_id)
        .expect("authorized ordered producer");
    let progress = if producer
        .instances
        .values()
        .any(|instance| instance.progress == TerminalProgress::Impossible)
    {
        WitnessProgress::Impossible
    } else if producer
        .instances
        .values()
        .all(|instance| instance.progress == TerminalProgress::Satisfied)
    {
        WitnessProgress::Satisfied
    } else {
        WitnessProgress::Pending
    };
    state
        .witnesses
        .get_mut(&producer.witness_id)
        .expect("installed ordered terminal witness")
        .advance(progress);
}

fn terminal_action_from_state(state: &ChannelState) -> ChannelAction {
    match &state.terminal {
        ChannelTerminal::Collecting => ChannelAction::None,
        ChannelTerminal::Completed {
            order,
            snapshot,
            events,
        } => ChannelAction::Completed {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            snapshot: snapshot.clone(),
            events: events.clone(),
        },
        ChannelTerminal::Unavailable {
            order,
            reason,
            events,
        } => ChannelAction::Unavailable {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            reason: *reason,
            events: events.clone(),
        },
        ChannelTerminal::CompletedWithoutArtifact { order, events } => {
            ChannelAction::CompletedWithoutArtifact {
                order: *order,
                outcome: SubmitOutcome::TerminalNoop,
                events: events.clone(),
            }
        }
        ChannelTerminal::DegradedLogical {
            order,
            reason,
            snapshot,
            events,
        } => ChannelAction::DegradedLogical {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            reason: *reason,
            snapshot: snapshot.clone(),
            events: events.clone(),
        },
        ChannelTerminal::Cancelled { order, events } => ChannelAction::Cancelled {
            order: *order,
            events: events.clone(),
        },
    }
}

fn next_dispatch_order(state: &mut ChannelState) -> u64 {
    let order = state.next_dispatch_order;
    state.next_dispatch_order = state
        .next_dispatch_order
        .checked_add(1)
        .expect("runtime filter channel dispatch order exhausted");
    order
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

fn instance_mut(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> Result<&mut InstanceState, RuntimeContractViolation> {
    let producer = state.producers.get_mut(&binding_id).ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::UnauthorizedBinding,
            "producer binding is not installed for this channel",
        )
    })?;
    producer
        .instances
        .get_mut(&fragment_instance_id)
        .ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            )
        })
}

fn instance_ref(
    state: &ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> Result<&InstanceState, RuntimeContractViolation> {
    let producer = state.producers.get(&binding_id).ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::UnauthorizedBinding,
            "producer binding is not installed for this channel",
        )
    })?;
    producer
        .instances
        .get(&fragment_instance_id)
        .ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            )
        })
}

fn partition_state(
    state: &ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
) -> Result<Option<&super::state::PartitionState>, RuntimeContractViolation> {
    let instance = instance_ref(state, binding_id, fragment_instance_id)?;
    let count = instance.local_partition_count().ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::InvalidPartitionCount,
            "producer instance must be opened before mutation",
        )
    })?;
    if partition_id.get() >= count {
        return Err(violation(
            RuntimeContractViolationKind::InvalidPartition,
            "partition is outside the opened local partition range",
        ));
    }
    Ok(instance.partition(partition_id))
}

fn partition_mut_for_commit(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
) -> &mut super::state::PartitionState {
    instance_mut(state, binding_id, fragment_instance_id)
        .expect("authorized producer instance must remain installed")
        .partition_mut_for_commit(partition_id)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use novarocks::runtime_filter_transition::model::contract::*;
    use novarocks::runtime_filter_transition::model::coverage::Coverage;
    use novarocks::runtime_filter_transition::port::events::RuntimeFilterEvent;
    use novarocks::runtime_filter_transition::port::identity::*;
    use novarocks::runtime_filter_transition::port::install::*;
    use novarocks::runtime_filter_transition::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };
    use novarocks::runtime_filter_transition::port::producer::{
        ProducerFailureReason, RuntimeContractViolation, RuntimeContractViolationKind,
        SubmitOutcome,
    };
    use novarocks::runtime_filter_transition::port::subscription::UnavailableReason;
    use novarocks::runtime_filter_transition::port::support::{
        MemoryAccountError, RuntimeFilterMemoryAccount, TemporaryContributionLease,
    };
    use novarocks::runtime_filter_transition::port::topk_summary::{
        RuntimeTopKSummaryContract, TopKSummary,
    };
    use novarocks::runtime_filter_transition::port::value_domain::{
        LogicalSnapshot, MembershipValues, ValueDomainDelta,
    };
    use novarocks_types::UniqueId;

    use super::{
        ChannelAction, ChannelTerminal, FinalDomainRejection, RuntimeFilterChannel,
        TerminalProgress,
    };

    #[derive(Default)]
    struct Account {
        current: AtomicUsize,
        peak: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for Account {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            let current = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.peak.fetch_max(current, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct ArmableAccount {
        rejecting: AtomicBool,
        current: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for ArmableAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.rejecting.load(Ordering::SeqCst) {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.current.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    #[derive(Default)]
    struct ReentrantAccount {
        current: AtomicUsize,
        channel: Mutex<Option<Weak<RuntimeFilterChannel>>>,
    }

    impl ReentrantAccount {
        fn reenter(&self) {
            let channel = self
                .channel
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade);
            if let Some(channel) = channel {
                let _ = channel.snapshot();
                let _ = channel.is_terminal();
            }
        }
    }

    impl RuntimeFilterMemoryAccount for ReentrantAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.current.fetch_add(bytes, Ordering::SeqCst);
            self.reenter();
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
            self.reenter();
        }
    }

    struct RejectingAccount;

    impl RuntimeFilterMemoryAccount for RejectingAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    struct BlockingRejectingAccount {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl RuntimeFilterMemoryAccount for BlockingRejectingAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId::new(1, lo)
    }

    fn deployment_with_coverages(
        availability_coverage: Coverage,
        terminal_coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> RuntimeFilterChannelDeployment {
        let producers = producers
            .iter()
            .map(|(binding, witness, instance)| {
                (
                    BindingId::new(*binding),
                    ProducerDeployment::new(
                        CoverageWitnessId::new(*witness),
                        BTreeSet::from([uid(*instance)]),
                    ),
                )
            })
            .collect();
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage,
            terminal_coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: max,
                max_artifact_bytes: 1,
                deadline_ms: 10,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(budget),
            novarocks::runtime_filter_transition::port::install::MaterializationPolicy::for_test(),
            producers,
            BTreeMap::new(),
        )
    }

    fn deployment(
        coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> RuntimeFilterChannelDeployment {
        deployment_with_coverages(coverage.clone(), coverage, producers, budget, max)
    }

    fn channel_with(
        coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        let account = Arc::new(Account::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment(coverage, producers, budget, max),
            deadline,
            account.clone(),
        )
        .unwrap();
        (channel, account, deadline)
    }

    fn channel_from(
        deployment: RuntimeFilterChannelDeployment,
    ) -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        let account = Arc::new(Account::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment,
            deadline,
            account.clone(),
        )
        .unwrap();
        (channel, account, deadline)
    }

    fn one_channel() -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            4096,
        )
    }

    fn multi_instance_deployment() -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(1);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 1,
                deadline_ms: 10,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(4096),
            novarocks::runtime_filter_transition::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10), uid(11)])),
            )]),
            BTreeMap::new(),
        )
    }

    fn submit(
        channel: &RuntimeFilterChannel,
        account: Arc<Account>,
        binding: u32,
        instance: i64,
        sequence: u64,
        values: &[i64],
    ) -> Result<
        ChannelAction,
        novarocks::runtime_filter_transition::port::producer::RuntimeContractViolation,
    > {
        let delta = ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        channel.submit(
            BindingId::new(binding),
            uid(instance),
            PartitionId::new(0),
            ProducerSequence::new(sequence),
            delta,
            TemporaryContributionLease::new(account, bytes),
        )
    }

    #[test]
    fn complete_once_exposes_no_snapshot_before_terminal_coverage() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn complete_once_tracks_availability_without_publishing() {
        let terminal_coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, account, _) = channel_from(deployment_with_coverages(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage,
            &[(10, 1, 10), (20, 2, 20)],
            4096,
            4096,
        ));
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        submit(&channel, account, 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            channel.availability_progress(),
            crate::runtime_filter::core::coverage::CoverageProgress::Satisfied
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn complete_once_completes_with_union_and_final_version_one() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 1, &[2]).unwrap();
        submit(&channel, account, 10, 10, 0, &[1]).unwrap();
        let action = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(2),
            )
            .unwrap();
        let snapshot = action.snapshot().unwrap();
        assert_eq!(snapshot.version(), LogicalVersion::FIRST);
        assert_eq!(snapshot.domain().values(), &MembershipValues::int64([1, 2]));
    }

    #[test]
    fn complete_once_empty_union_is_valid_completed_domain() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let action = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(action.snapshot().unwrap().domain().values().is_empty());
    }

    #[test]
    fn expected_instances_must_open_and_all_close_before_completion() {
        let (channel, _, _) = channel_from(multi_instance_deployment());
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_none());
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(11), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(11),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn max_partition_count_open_stays_sparse_and_rejects_boundary() {
        let (channel, account, _) = one_channel();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), u32::MAX)
                .unwrap(),
            SubmitOutcome::Applied
        );
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(u32::MAX),
                    ProducerSequence::new(0),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(u32::MAX - 1),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn open_is_idempotent_but_conflicts_and_unauthorized_coordinates_fail_first() {
        let (channel, account, _) = one_channel();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(99), uid(10), 1)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(99), 1)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn remote_open_preflight_prioritizes_existing_count_conflict_without_mutation() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 2)
            .unwrap();

        assert_eq!(
            channel
                .preflight_remote_open(BindingId::new(10), uid(10), 1, PartitionId::new(1),)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
    }

    #[test]
    fn remote_open_preflight_rejects_new_invalid_partition_without_mutation() {
        let (channel, _, _) = one_channel();

        assert_eq!(
            channel
                .preflight_remote_open(BindingId::new(10), uid(10), 1, PartitionId::new(1),)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
    }

    #[test]
    fn producer_ingress_core_snapshot_is_read_only() {
        let (channel, _, _) = one_channel();

        let before = channel.producer_ingress_core_snapshot(BindingId::new(10), uid(10));
        let after = channel.producer_ingress_core_snapshot(BindingId::new(10), uid(10));

        assert_eq!(before, after);
        assert_eq!(before.local_partition_count, None);
        assert_eq!(before.materialized_partition_count, 0);
        assert_eq!(before.terminal_progress, TerminalProgress::Pending);
        assert!(before.membership_values.is_some());
    }

    #[test]
    fn contribution_lease_must_match_canonical_payload_size() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([1]), false),
                    TemporaryContributionLease::new(account.clone(), 0),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidContributionLease
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn value_domain_union_deduplicates_exact_replay_and_rejects_conflict_after_completion() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
    }

    #[test]
    fn type_mismatch_precedes_replay_and_completed_losing_stream_terminal_noop() {
        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, account, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();

        let mismatched_replay = ValueDomainDelta::new(MembershipValues::int32([1]), false);
        let bytes = mismatched_replay.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    mismatched_replay,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );

        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        let mismatched_loser = ValueDomainDelta::new(MembershipValues::int32([2]), false);
        let bytes = mismatched_loser.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(20),
                    uid(20),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    mismatched_loser,
                    TemporaryContributionLease::new(account, bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );
    }

    #[test]
    fn concurrent_duplicate_submits_reduce_exactly_once() {
        let (channel, account, _) = one_channel();
        let channel = Arc::new(channel);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let channel = channel.clone();
            let account = account.clone();
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                submit(&channel, account, 10, 10, 0, &[1])
                    .unwrap()
                    .outcome()
            }));
        }
        barrier.wait();
        let mut outcomes = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|outcome| match outcome {
            SubmitOutcome::Applied => 0,
            SubmitOutcome::Duplicate => 1,
            _ => 2,
        });
        assert_eq!(
            outcomes,
            vec![SubmitOutcome::Applied, SubmitOutcome::Duplicate]
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            channel.snapshot().unwrap().domain().values(),
            &MembershipValues::int64([1])
        );
    }

    #[test]
    fn unseen_sequence_order_and_exact_replay_do_not_change_complete_domain() {
        let orders = [
            [0_u64, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for order in orders {
            let (channel, account, _) = one_channel();
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            for sequence in order {
                let values = [i64::try_from(sequence).unwrap() + 10];
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, sequence, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::Applied
                );
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, sequence, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::Duplicate
                );
            }
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(3),
                )
                .unwrap();
            assert_eq!(
                channel.snapshot().unwrap().domain().values(),
                &MembershipValues::int64([10, 11, 12]),
                "order={order:?}"
            );
        }
    }

    #[test]
    fn close_waits_for_every_sequence_below_terminal_and_rejects_seen_outside() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 2, &[3]).unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(2)
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let pending = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(3),
            )
            .unwrap();
        assert_eq!(pending.outcome(), SubmitOutcome::PendingGap);
        assert!(
            pending
                .events()
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::SequenceGapObserved { .. }))
        );
        submit(&channel, account, 10, 10, 1, &[2]).unwrap();
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn any_of_replica_failure_does_not_override_remaining_replica() {
        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, _, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        assert_eq!(
            channel
                .fail_instance(
                    BindingId::new(10),
                    uid(10),
                    ProducerFailureReason::ExecutionFailed
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert!(!channel.is_terminal());
    }

    #[test]
    fn all_of_required_instance_failure_becomes_unavailable() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let action = channel
            .fail_instance(
                BindingId::new(10),
                uid(10),
                ProducerFailureReason::ExecutionFailed,
            )
            .unwrap();
        assert_eq!(
            action.unavailable_reason(),
            Some(UnavailableReason::ProducerFailed)
        );
    }

    #[test]
    fn close_before_fail_keeps_instance_satisfied() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .fail_instance(
                    BindingId::new(10),
                    uid(10),
                    ProducerFailureReason::ExecutionFailed
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn fail_before_close_keeps_instance_impossible() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .fail_instance(
                BindingId::new(10),
                uid(10),
                ProducerFailureReason::ExecutionFailed,
            )
            .unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0)
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(
            channel
                .snapshot()
                .map(|snapshot| snapshot.domain().values().clone()),
            None
        );
    }

    #[test]
    fn concurrent_close_fail_race_has_one_irreversible_terminal_result() {
        let (channel, _, _) = one_channel();
        let channel = Arc::new(channel);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let close = {
            let channel = channel.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                channel
                    .close_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    )
                    .unwrap()
            })
        };
        let fail = {
            let channel = channel.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                channel
                    .fail_instance(
                        BindingId::new(10),
                        uid(10),
                        ProducerFailureReason::ExecutionFailed,
                    )
                    .unwrap()
            })
        };
        barrier.wait();
        let actions = [close.join().unwrap(), fail.join().unwrap()];
        assert_eq!(
            actions
                .iter()
                .filter(|action| action.outcome() != SubmitOutcome::TerminalNoop)
                .count(),
            1
        );
        assert!(channel.is_terminal());
    }

    #[test]
    fn completed_channel_never_changes_version_or_domain() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account, 10, 10, 1, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        assert_eq!(
            channel.snapshot().unwrap().domain().values(),
            &MembershipValues::int64([1])
        );
    }

    #[test]
    fn resource_limits_are_unavailable_not_empty_domain() {
        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            1,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            1,
            4096,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            1,
            4096,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                )
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn hard_deadline_and_cancel_are_irreversible() {
        let (channel, account, deadline) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert!(account.current.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            channel.expire_deadline(deadline - Duration::from_nanos(1)),
            ChannelAction::None
        ));
        assert_eq!(
            channel.expire_deadline(deadline).unavailable_reason(),
            Some(UnavailableReason::IncompleteCoverage)
        );
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 1, &[2])
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
        assert!(matches!(channel.cancel(), ChannelAction::None));
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unavailable_and_cancelled_tombstones_discard_replay_metadata() {
        for cancel in [false, true] {
            let coverage = Coverage::AllOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]);
            let (channel, account, deadline) =
                channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap();
            assert!(account.current.load(Ordering::SeqCst) > 0);
            if cancel {
                assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
            } else {
                assert_eq!(
                    channel.expire_deadline(deadline).unavailable_reason(),
                    Some(UnavailableReason::IncompleteCoverage)
                );
            }
            assert_eq!(account.current.load(Ordering::SeqCst), 0);

            for values in [[1_i64], [2_i64]] {
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, 0, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::TerminalNoop
                );
            }
            for terminal in [1_u64, 2_u64] {
                assert_eq!(
                    channel
                        .close_partition(
                            BindingId::new(10),
                            uid(10),
                            PartitionId::new(0),
                            ProducerSequence::new(terminal),
                        )
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::TerminalNoop
                );
            }
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn completed_tombstone_retains_delta_and_close_replay_contract() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(2),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingTerminalSequence
        );
    }

    #[test]
    fn terminal_reopen_checks_existing_count_before_terminal_noop() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );

        let coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (unavailable, _, deadline) =
            channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        unavailable
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        unavailable.expire_deadline(deadline);
        assert_eq!(
            unavailable
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            unavailable
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );

        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, _, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .open_producer(BindingId::new(20), uid(20), 1)
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
    }

    #[test]
    fn temporary_and_retained_memory_have_distinct_lifetimes() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let retained = account.current.load(Ordering::SeqCst);
        assert!(retained > 0);
        assert!(account.peak.load(Ordering::SeqCst) > retained);
        let snapshot = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap()
            .snapshot()
            .unwrap();
        let completed_retained = account.current.load(Ordering::SeqCst);
        assert!(completed_retained > retained);
        drop(channel);
        assert_eq!(account.current.load(Ordering::SeqCst), completed_retained);
        drop(snapshot);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_memory_account_rejection_fails_open_as_resource_limit() {
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment(
                Coverage::Leaf(CoverageWitnessId::new(1)),
                &[(10, 1, 10)],
                4096,
                4096,
            ),
            deadline,
            Arc::new(RejectingAccount),
        )
        .unwrap();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let temporary = Arc::new(Account::default());
        let action = submit(&channel, temporary, 10, 10, 0, &[1]).unwrap();
        assert_eq!(
            action.unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn rejected_reservation_revalidates_terminal_state_before_resource_limit() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let account = Arc::new(BlockingRejectingAccount {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment(
                    Coverage::Leaf(CoverageWitnessId::new(1)),
                    &[(10, 1, 10)],
                    4096,
                    4096,
                ),
                Instant::now() + Duration::from_secs(10),
                account,
            )
            .unwrap(),
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let submit_channel = channel.clone();
        let temporary = Arc::new(Account::default());
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            done_tx
                .send(submit(&submit_channel, temporary, 10, 10, 0, &[1]))
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
        release_tx.send(()).unwrap();
        let action = done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
        assert!(matches!(
            channel.terminal_action(),
            ChannelAction::Cancelled { .. }
        ));
    }

    #[test]
    fn memory_account_callbacks_reenter_channel_without_deadlock() {
        let account = Arc::new(ReentrantAccount::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let deployment = deployment(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            4096,
        );
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment,
                deadline,
                account.clone(),
            )
            .unwrap(),
        );
        *account.channel.lock().unwrap() = Some(Arc::downgrade(&channel));
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
            let bytes = delta.estimated_contribution_bytes().unwrap();
            let result = channel.submit(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
                delta,
                TemporaryContributionLease::new(account.clone(), bytes),
            );
            if result.is_ok() {
                let _ = channel.cancel();
            }
            done_tx
                .send((result.is_ok(), account.current.load(Ordering::SeqCst)))
                .unwrap();
        });

        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (true, 0)
        );
    }

    #[test]
    fn temporary_lease_drops_on_duplicate_conflict_and_type_error() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let retained = account.current.load(Ordering::SeqCst);
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert_eq!(account.current.load(Ordering::SeqCst), retained);
        submit(&channel, account.clone(), 10, 10, 0, &[2]).unwrap_err();
        assert_eq!(account.current.load(Ordering::SeqCst), retained);

        let delta = ValueDomainDelta::new(MembershipValues::int32([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );
        assert_eq!(account.current.load(Ordering::SeqCst), retained);
    }

    #[derive(Debug, Eq, PartialEq)]
    enum TestAction {
        Published(u64, i64),
        SequenceAdvancedEqual,
        StreamAcceptedNoGlobalChange,
        PendingFinalSnapshot,
        CoverageStillPossible,
        Completed(Option<(u64, i64)>),
        CompletedWithoutArtifact,
        Other(SubmitOutcome),
    }

    fn ordered_value(snapshot: &LogicalSnapshot) -> i64 {
        let Some(OrderedScalar::Int64(value)) = snapshot
            .ordered_bound()
            .expect("ordered snapshot")
            .bound()
            .values()
            .first()
            .and_then(Option::as_ref)
        else {
            panic!("test ordered snapshot contains one int64")
        };
        *value
    }

    fn test_action(action: ChannelAction) -> TestAction {
        match action {
            ChannelAction::VisibleSnapshot {
                version, snapshot, ..
            } => TestAction::Published(version.get(), ordered_value(&snapshot)),
            ChannelAction::Completed { snapshot, .. } => {
                TestAction::Completed(Some((snapshot.version().get(), ordered_value(&snapshot))))
            }
            ChannelAction::CompletedWithoutArtifact { .. } => TestAction::CompletedWithoutArtifact,
            action => match action.outcome() {
                SubmitOutcome::SequenceAdvancedEqual => TestAction::SequenceAdvancedEqual,
                SubmitOutcome::StreamAcceptedNoGlobalChange => {
                    TestAction::StreamAcceptedNoGlobalChange
                }
                SubmitOutcome::PendingFinalSnapshot => TestAction::PendingFinalSnapshot,
                SubmitOutcome::CoverageStillPossible => TestAction::CoverageStillPossible,
                outcome => TestAction::Other(outcome),
            },
        }
    }

    fn int_bound(value: i64) -> i64 {
        value
    }

    struct OrderedChannelHarness {
        channel: Arc<RuntimeFilterChannel>,
        contract: Arc<RuntimeOrderContract>,
        streams: Vec<(BindingId, UniqueId)>,
        temporary_account: Arc<Account>,
    }

    impl OrderedChannelHarness {
        fn with_streams(count: usize) -> Self {
            Self::with_streams_and_limits(count, 1024, 4096, Arc::new(Account::default()))
        }

        fn with_streams_and_limits(
            count: usize,
            max_contribution_bytes: u64,
            max_reducer_bytes: u64,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            Self::with_streams_coverage_and_limits(
                count,
                true,
                max_contribution_bytes,
                max_reducer_bytes,
                memory_account,
            )
        }

        fn with_streams_coverage_and_limits(
            count: usize,
            any_of: bool,
            max_contribution_bytes: u64,
            max_reducer_bytes: u64,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
            let streams = (0..count)
                .map(|index| {
                    (
                        BindingId::new(10 + u32::try_from(index).unwrap()),
                        uid(10 + i64::try_from(index).unwrap()),
                    )
                })
                .collect::<Vec<_>>();
            let witnesses = (0..count)
                .map(|index| CoverageWitnessId::new(1 + u32::try_from(index).unwrap()))
                .collect::<Vec<_>>();
            let coverage_children = witnesses.iter().copied().map(Coverage::Leaf).collect();
            let coverage = if any_of {
                Coverage::AnyOf(coverage_children)
            } else {
                Coverage::AllOf(coverage_children)
            };
            let deployment = RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::OrderedBound(plan),
                RuntimeFilterLifecycle::MonotonicUpdates,
                coverage.clone(),
                coverage,
                ReductionRequirement::TightenOrderedBound,
                BTreeSet::from([
                    ContributionKind::OrderedBoundUpdate,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes,
                    max_artifact_bytes: 1024,
                    deadline_ms: 100,
                    max_retries: 0,
                },
                RuntimeFilterCoreBudget::new(max_reducer_bytes),
                MaterializationPolicy::for_test(),
                streams
                    .iter()
                    .zip(&witnesses)
                    .map(|((binding, instance), witness)| {
                        (
                            *binding,
                            ProducerDeployment::new(*witness, BTreeSet::from([*instance])),
                        )
                    })
                    .collect(),
                BTreeMap::new(),
            );
            let channel = Arc::new(
                RuntimeFilterChannel::new(
                    uid(99),
                    RuntimeFilterParticipantId::new(1),
                    DeploymentEpoch::new(1),
                    &deployment,
                    Instant::now() + Duration::from_secs(10),
                    memory_account,
                )
                .unwrap(),
            );
            for (binding, instance) in &streams {
                channel.open_producer(*binding, *instance, 1).unwrap();
            }
            Self {
                channel,
                contract,
                streams,
                temporary_account: Arc::new(Account::default()),
            }
        }

        fn single_stream_anyof() -> Self {
            Self::with_streams(1)
        }

        fn two_stream_anyof() -> Self {
            Self::with_streams(2)
        }

        fn submit(
            &self,
            stream: usize,
            sequence: u64,
            value: i64,
        ) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            let tuple =
                OrderedTuple::try_new(&self.contract, [Some(OrderedScalar::Int64(value))]).unwrap();
            let update = OrderedBoundUpdate::new(&self.contract, tuple).unwrap();
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            self.channel
                .submit_ordered(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(sequence),
                    update,
                    TemporaryContributionLease::new(
                        self.temporary_account.clone(),
                        contribution_bytes,
                    ),
                )
                .map(test_action)
        }

        fn close(
            &self,
            stream: usize,
            terminal: u64,
        ) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            self.channel
                .close_ordered_partition(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(terminal),
                )
                .map(test_action)
        }

        fn fail_stream(&self, stream: usize) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            self.channel
                .fail_instance(binding, instance, ProducerFailureReason::ExecutionFailed)
                .map(test_action)
        }

        fn latest(&self) -> Option<(u64, i64)> {
            self.channel
                .snapshot()
                .map(|snapshot| (snapshot.version().get(), ordered_value(&snapshot)))
        }

        fn state_digest(&self) -> String {
            let state = self.channel.state.lock().unwrap();
            let latest = state
                .ordered
                .as_ref()
                .and_then(|ordered| ordered.latest.as_ref())
                .map(|snapshot| (snapshot.version().get(), ordered_value(snapshot)));
            format!(
                "{:?}:{:?}:{}",
                state.ordered.as_ref().expect("ordered state").reducer,
                latest,
                state.next_dispatch_order
            )
        }
    }

    struct TopKChannelHarness {
        channel: Arc<RuntimeFilterChannel>,
        contract: Arc<RuntimeTopKSummaryContract>,
        streams: Vec<(BindingId, UniqueId)>,
        temporary_account: Arc<Account>,
    }

    impl TopKChannelHarness {
        fn with_streams(count: usize, k: u32) -> Self {
            Self::with_streams_and_account(count, k, Arc::new(Account::default()))
        }

        fn with_streams_and_account(
            count: usize,
            k: u32,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            Self::with_plan_and_limits(count, k, plan, 4096, 16 * 1024, memory_account)
        }

        fn with_plan_and_limits(
            count: usize,
            k: u32,
            plan: OrderContract,
            max_contribution_bytes: u64,
            max_reducer_bytes: u64,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            let requirement = TopKSummaryRequirement::try_new(k).unwrap();
            let contract =
                Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
            let streams = (0..count)
                .map(|index| {
                    (
                        BindingId::new(10 + u32::try_from(index).unwrap()),
                        uid(10 + i64::try_from(index).unwrap()),
                    )
                })
                .collect::<Vec<_>>();
            let witnesses = (0..count)
                .map(|index| CoverageWitnessId::new(1 + u32::try_from(index).unwrap()))
                .collect::<Vec<_>>();
            let coverage = if witnesses.len() == 1 {
                Coverage::Leaf(witnesses[0])
            } else {
                Coverage::AllOf(witnesses.iter().copied().map(Coverage::Leaf).collect())
            };
            let deployment = RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::OrderedBound(plan),
                RuntimeFilterLifecycle::MonotonicUpdates,
                coverage.clone(),
                coverage,
                ReductionRequirement::MergeTopKSummary(requirement),
                BTreeSet::from([
                    ContributionKind::TopKSummary,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes,
                    max_artifact_bytes: 4096,
                    deadline_ms: 100,
                    max_retries: 0,
                },
                RuntimeFilterCoreBudget::new(max_reducer_bytes),
                MaterializationPolicy::for_test(),
                streams
                    .iter()
                    .zip(&witnesses)
                    .map(|((binding, instance), witness)| {
                        (
                            *binding,
                            ProducerDeployment::new(*witness, BTreeSet::from([*instance])),
                        )
                    })
                    .collect(),
                BTreeMap::new(),
            );
            let channel = Arc::new(
                RuntimeFilterChannel::new(
                    uid(99),
                    RuntimeFilterParticipantId::new(1),
                    DeploymentEpoch::new(1),
                    &deployment,
                    Instant::now() + Duration::from_secs(10),
                    memory_account,
                )
                .unwrap(),
            );
            for (binding, instance) in &streams {
                channel.open_producer(*binding, *instance, 1).unwrap();
            }
            Self {
                channel,
                contract,
                streams,
                temporary_account: Arc::new(Account::default()),
            }
        }

        fn summary(&self, values: &[i64]) -> TopKSummary {
            TopKSummary::try_new(
                &self.contract,
                values
                    .iter()
                    .map(|value| {
                        OrderedTuple::try_new(
                            self.contract.order(),
                            [Some(OrderedScalar::Int64(*value))],
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap()
        }

        fn submit_summary(
            &self,
            stream: usize,
            sequence: u64,
            summary: TopKSummary,
        ) -> Result<TestAction, RuntimeContractViolation> {
            self.submit_raw_summary(stream, sequence, summary)
                .map(test_action)
        }

        fn submit_raw_summary(
            &self,
            stream: usize,
            sequence: u64,
            summary: TopKSummary,
        ) -> Result<ChannelAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            let bytes = summary.canonical_contribution_bytes().unwrap();
            self.channel.submit_topk_summary(
                binding,
                instance,
                PartitionId::new(0),
                ProducerSequence::new(sequence),
                summary,
                TemporaryContributionLease::new(self.temporary_account.clone(), bytes),
            )
        }

        fn submit(
            &self,
            stream: usize,
            sequence: u64,
            values: &[i64],
        ) -> Result<TestAction, RuntimeContractViolation> {
            let summary = self.summary(values);
            self.submit_summary(stream, sequence, summary)
        }

        fn close(
            &self,
            stream: usize,
            terminal: u64,
        ) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            self.channel
                .close_topk_partition(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(terminal),
                )
                .map(test_action)
        }

        fn latest(&self) -> Option<(u64, i64)> {
            self.channel
                .snapshot()
                .map(|snapshot| (snapshot.version().get(), ordered_value(&snapshot)))
        }
    }

    #[derive(Default)]
    struct InterleavingAccount {
        current: AtomicUsize,
        hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl RuntimeFilterMemoryAccount for InterleavingAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.current.fetch_add(bytes, Ordering::SeqCst);
            let hook = self.hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    mod topk {
        use super::*;

        fn wrong_contract_summary(harness: &TopKChannelHarness, values: &[i64]) -> TopKSummary {
            let requirement =
                TopKSummaryRequirement::try_new(harness.contract.k().get().checked_add(1).unwrap())
                    .unwrap();
            let keys = vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            let wrong_contract =
                RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap();
            TopKSummary::try_new(
                &wrong_contract,
                values
                    .iter()
                    .map(|value| {
                        OrderedTuple::try_new(
                            wrong_contract.order(),
                            [Some(OrderedScalar::Int64(*value))],
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap()
        }

        fn utf8_topk_plan() -> OrderContract {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            }
        }

        fn utf8_topk_summary(
            contract: &RuntimeTopKSummaryContract,
            values: &[String],
        ) -> TopKSummary {
            TopKSummary::try_new(
                contract,
                values
                    .iter()
                    .map(|value| {
                        OrderedTuple::try_new(
                            contract.order(),
                            [Some(OrderedScalar::Utf8(Arc::from(value.as_str())))],
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap()
        }

        fn topk_channel_state(harness: &TopKChannelHarness) -> String {
            let state = harness.channel.state.lock().unwrap();
            format!(
                "{:?}:{}:{}",
                state.ordered.as_ref().unwrap().reducer,
                state.reservation.bytes(),
                state.next_dispatch_order
            )
        }

        fn assert_topk_collecting(harness: &TopKChannelHarness) {
            let state = harness.channel.state.lock().unwrap();
            assert!(matches!(state.terminal, ChannelTerminal::Collecting));
        }

        #[test]
        fn oversized_topk_wrong_contract_digest_is_rejected_without_terminal_mutation() {
            let plan = utf8_topk_plan();
            let retained_account = Arc::new(Account::default());
            let harness = TopKChannelHarness::with_plan_and_limits(
                1,
                2,
                plan.clone(),
                256,
                16 * 1024,
                retained_account.clone(),
            );
            let wrong_contract = RuntimeTopKSummaryContract::try_from_plan(
                &plan,
                TopKSummaryRequirement::try_new(3).unwrap(),
            )
            .unwrap();
            let summary = utf8_topk_summary(&wrong_contract, &["x".repeat(512)]);
            assert!(summary.canonical_contribution_bytes().unwrap() > 256);
            let before = topk_channel_state(&harness);

            let error = harness.submit_raw_summary(0, 0, summary).unwrap_err();

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::OrderedContractMismatch
            );
            assert_eq!(topk_channel_state(&harness), before);
            assert_topk_collecting(&harness);
            assert_eq!(retained_account.current.load(Ordering::SeqCst), 0);
            assert_eq!(harness.temporary_account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn oversized_topk_invalid_cumulative_transition_is_rejected_without_terminal_mutation() {
            let retained_account = Arc::new(Account::default());
            let harness = TopKChannelHarness::with_plan_and_limits(
                1,
                4,
                utf8_topk_plan(),
                256,
                16 * 1024,
                retained_account.clone(),
            );
            let first = utf8_topk_summary(&harness.contract, &["a".to_owned(), "c".to_owned()]);
            assert!(first.canonical_contribution_bytes().unwrap() <= 256);
            assert_eq!(
                harness.submit_summary(0, 0, first).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            let retained_before = retained_account.current.load(Ordering::SeqCst);
            let before = topk_channel_state(&harness);
            let invalid = utf8_topk_summary(
                &harness.contract,
                &["a".to_owned(), format!("b{}", "x".repeat(512))],
            );
            assert!(invalid.canonical_contribution_bytes().unwrap() > 256);

            let error = harness.submit_raw_summary(0, 1, invalid).unwrap_err();

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::OrderedBoundLoosened
            );
            assert_eq!(topk_channel_state(&harness), before);
            assert_topk_collecting(&harness);
            assert_eq!(
                retained_account.current.load(Ordering::SeqCst),
                retained_before
            );
            assert_eq!(harness.temporary_account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn topk_cancelled_tombstone_accepts_legal_higher_sequence_as_terminal_noop() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            assert_eq!(
                harness.submit(0, 0, &[7]).unwrap(),
                TestAction::Published(1, 7)
            );
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));

            let action = harness
                .submit_raw_summary(0, 1, harness.summary(&[6]))
                .unwrap();

            assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
            assert!(matches!(action, ChannelAction::Cancelled { .. }));
        }

        #[test]
        fn topk_unavailable_tombstone_accepts_legal_higher_sequence_as_terminal_noop() {
            let harness = TopKChannelHarness::with_streams(1, 2);
            assert_eq!(
                harness.submit(0, 0, &[7]).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(
                harness.channel.resource_exhausted().unavailable_reason(),
                Some(UnavailableReason::ResourceLimit)
            );

            let action = harness
                .submit_raw_summary(0, 1, harness.summary(&[6, 7]))
                .unwrap();

            assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
            assert_eq!(
                action.unavailable_reason(),
                Some(UnavailableReason::ResourceLimit)
            );
        }

        #[test]
        fn topk_resource_rejection_uses_tombstone_validation_after_cancel() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            harness.submit(0, 0, &[7]).unwrap();
            drop(harness.channel.cancel());
            let (binding, instance) = harness.streams[0];
            let summary = harness.summary(&[6]);

            let action = harness
                .channel
                .reject_topk_submit_resource_exhausted(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    &summary,
                )
                .unwrap();

            assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
            assert!(matches!(action, ChannelAction::Cancelled { .. }));
        }

        #[test]
        fn topk_tombstone_preserves_invalid_contract_digest_rejection() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            harness.submit(0, 0, &[7]).unwrap();
            drop(harness.channel.cancel());

            let error = harness
                .submit_raw_summary(0, 1, wrong_contract_summary(&harness, &[6]))
                .unwrap_err();

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::OrderedContractMismatch
            );
        }

        #[test]
        fn topk_tombstone_preserves_terminal_range_rejection() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            harness.submit(0, 0, &[7]).unwrap();
            assert_eq!(
                harness.close(0, 2).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            drop(harness.channel.cancel());

            let error = harness.submit(0, 2, &[6]).unwrap_err();

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn topk_tombstone_preserves_conflicting_replay_rejection() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            harness.submit(0, 0, &[7]).unwrap();
            drop(harness.channel.cancel());

            let error = harness.submit(0, 0, &[6]).unwrap_err();

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::ConflictingReplay
            );
        }

        #[test]
        fn complete_stream_coverage_gates_first_summary_publication() {
            let harness = TopKChannelHarness::with_streams(2, 4);
            assert_eq!(
                harness.submit(0, 0, &[1, 4]).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(harness.latest(), None);
            assert_eq!(
                harness.submit(1, 0, &[2, 2]).unwrap(),
                TestAction::Published(1, 4)
            );
        }

        #[test]
        fn last_close_zero_can_publish_the_first_complete_bound() {
            let harness = TopKChannelHarness::with_streams(2, 4);
            assert_eq!(
                harness.submit(0, 0, &[1, 2, 3, 4]).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(harness.latest(), None);
            let (binding, instance) = harness.streams[1];
            let action = harness
                .channel
                .close_topk_partition(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                )
                .unwrap();
            assert_eq!(action.outcome(), SubmitOutcome::Published);
            assert_eq!(test_action(action), TestAction::Published(1, 4));
        }

        #[test]
        fn availability_requires_every_expected_instance_and_local_partition() {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            let requirement = TopKSummaryRequirement::try_new(2).unwrap();
            let contract =
                Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
            let witness = CoverageWitnessId::new(1);
            let binding = BindingId::new(10);
            let deployment = RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::OrderedBound(plan),
                RuntimeFilterLifecycle::MonotonicUpdates,
                Coverage::Leaf(witness),
                Coverage::Leaf(witness),
                ReductionRequirement::MergeTopKSummary(requirement),
                BTreeSet::from([
                    ContributionKind::TopKSummary,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 4096,
                    max_artifact_bytes: 4096,
                    deadline_ms: 100,
                    max_retries: 0,
                },
                RuntimeFilterCoreBudget::new(16 * 1024),
                MaterializationPolicy::for_test(),
                BTreeMap::from([(
                    binding,
                    ProducerDeployment::new(witness, BTreeSet::from([uid(10), uid(11)])),
                )]),
                BTreeMap::new(),
            );
            let account = Arc::new(Account::default());
            let channel = RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment,
                Instant::now() + Duration::from_secs(10),
                account.clone(),
            )
            .unwrap();
            let summary = |values: &[i64]| {
                TopKSummary::try_new(
                    &contract,
                    values
                        .iter()
                        .map(|value| {
                            OrderedTuple::try_new(
                                contract.order(),
                                [Some(OrderedScalar::Int64(*value))],
                            )
                            .unwrap()
                        })
                        .collect(),
                )
                .unwrap()
            };
            let submit = |instance: UniqueId, partition: u32, sequence: u64, values: &[i64]| {
                let summary = summary(values);
                let bytes = summary.canonical_contribution_bytes().unwrap();
                channel
                    .submit_topk_summary(
                        binding,
                        instance,
                        PartitionId::new(partition),
                        ProducerSequence::new(sequence),
                        summary,
                        TemporaryContributionLease::new(account.clone(), bytes),
                    )
                    .map(test_action)
            };

            channel.open_producer(binding, uid(10), 2).unwrap();
            assert_eq!(
                submit(uid(10), 0, 0, &[1, 4]).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(
                channel
                    .close_topk_partition(
                        binding,
                        uid(10),
                        PartitionId::new(1),
                        ProducerSequence::new(0),
                    )
                    .map(test_action)
                    .unwrap(),
                TestAction::Other(SubmitOutcome::Applied)
            );
            assert!(channel.snapshot().is_none());

            channel.open_producer(binding, uid(11), 2).unwrap();
            assert_eq!(
                channel
                    .close_topk_partition(
                        binding,
                        uid(11),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    )
                    .map(test_action)
                    .unwrap(),
                TestAction::Other(SubmitOutcome::Applied)
            );
            assert!(channel.snapshot().is_none());
            assert_eq!(
                channel
                    .close_topk_partition(
                        binding,
                        uid(11),
                        PartitionId::new(1),
                        ProducerSequence::new(2),
                    )
                    .map(test_action)
                    .unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert!(channel.snapshot().is_none());
            assert!(!channel.is_terminal());

            assert_eq!(
                submit(uid(11), 1, 1, &[2, 3]).unwrap(),
                TestAction::Published(1, 2)
            );
            assert!(!channel.is_terminal());
            assert_eq!(
                channel
                    .close_topk_partition(
                        binding,
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(1),
                    )
                    .map(test_action)
                    .unwrap(),
                TestAction::Completed(Some((1, 2)))
            );
            assert!(channel.is_terminal());
        }

        #[test]
        fn versions_advance_only_when_the_global_kth_tightens() {
            let harness = TopKChannelHarness::with_streams(2, 4);
            harness.submit(0, 0, &[1, 4]).unwrap();
            assert_eq!(
                harness.submit(1, 0, &[2, 2]).unwrap(),
                TestAction::Published(1, 4)
            );
            assert_eq!(
                harness.submit(0, 1, &[1, 4, 5]).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(harness.latest(), Some((1, 4)));
            assert_eq!(
                harness.submit(0, 2, &[0, 1, 4, 5]).unwrap(),
                TestAction::Published(2, 2)
            );
        }

        #[test]
        fn terminal_insufficient_k_completes_without_artifact() {
            let harness = TopKChannelHarness::with_streams(2, 4);
            harness.submit(0, 0, &[1]).unwrap();
            harness.submit(1, 0, &[2]).unwrap();
            assert_eq!(
                harness.close(0, 1).unwrap(),
                TestAction::Other(SubmitOutcome::Applied)
            );
            assert_eq!(
                harness.close(1, 1).unwrap(),
                TestAction::CompletedWithoutArtifact
            );
        }

        #[test]
        fn resource_failure_before_and_after_latest_uses_ordered_fail_open() {
            let before = TopKChannelHarness::with_streams(1, 1);
            assert!(matches!(
                before.channel.resource_exhausted(),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::ResourceLimit,
                    ..
                }
            ));

            let after = TopKChannelHarness::with_streams(1, 1);
            assert_eq!(
                after.submit(0, 0, &[7]).unwrap(),
                TestAction::Published(1, 7)
            );
            assert!(matches!(
                after.channel.resource_exhausted(),
                ChannelAction::DegradedLogical {
                    reason: UnavailableReason::ResourceLimit,
                    snapshot,
                    ..
                } if snapshot.version() == LogicalVersion::FIRST
                    && ordered_value(&snapshot) == 7
            ));
        }

        #[test]
        fn topk_retained_reservation_failure_before_and_after_latest_is_fail_open() {
            let before_account = Arc::new(ArmableAccount::default());
            before_account.rejecting.store(true, Ordering::SeqCst);
            let before = TopKChannelHarness::with_streams_and_account(1, 1, before_account.clone());
            let before_action = before
                .submit_raw_summary(0, 0, before.summary(&[7]))
                .unwrap();
            assert!(matches!(
                before_action,
                ChannelAction::Unavailable {
                    reason: UnavailableReason::ResourceLimit,
                    ..
                }
            ));
            assert!(before.channel.snapshot().is_none());
            assert_eq!(before_account.current.load(Ordering::SeqCst), 0);

            let after_account = Arc::new(ArmableAccount::default());
            let after = TopKChannelHarness::with_streams_and_account(1, 1, after_account.clone());
            assert_eq!(
                after.submit(0, 0, &[7]).unwrap(),
                TestAction::Published(1, 7)
            );
            let retained_before_failure = after_account.current.load(Ordering::SeqCst);
            after_account.rejecting.store(true, Ordering::SeqCst);
            let after_action = after.submit_raw_summary(0, 1, after.summary(&[6])).unwrap();
            assert!(matches!(
                after_action,
                ChannelAction::DegradedLogical {
                    reason: UnavailableReason::ResourceLimit,
                    ref snapshot,
                    ..
                } if snapshot.version() == LogicalVersion::FIRST
                    && ordered_value(snapshot) == 7
            ));
            assert_eq!(after.latest(), Some((1, 7)));
            assert!(after_account.current.load(Ordering::SeqCst) <= retained_before_failure);
            after_account.rejecting.store(false, Ordering::SeqCst);
            drop(after_action);
            drop(after);
            assert_eq!(after_account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn topk_utf8_replacement_releases_retained_bytes() {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            let account = Arc::new(Account::default());
            let harness =
                TopKChannelHarness::with_plan_and_limits(1, 2, plan, 4096, 8192, account.clone());
            let make_summary = |values: &[String]| {
                TopKSummary::try_new(
                    &harness.contract,
                    values
                        .iter()
                        .map(|value| {
                            OrderedTuple::try_new(
                                harness.contract.order(),
                                [Some(OrderedScalar::Utf8(Arc::from(value.as_str())))],
                            )
                            .unwrap()
                        })
                        .collect(),
                )
                .unwrap()
            };

            let first = harness
                .submit_raw_summary(0, 0, make_summary(&["y".repeat(512), "z".repeat(1024)]))
                .unwrap();
            assert_eq!(first.outcome(), SubmitOutcome::Published);
            drop(first);
            let first_retained = account.current.load(Ordering::SeqCst);
            assert_eq!(first_retained, 3_628);

            let replacement = harness
                .submit_raw_summary(0, 1, make_summary(&["a".to_owned(), "b".to_owned()]))
                .unwrap();
            assert_eq!(replacement.outcome(), SubmitOutcome::Published);
            drop(replacement);
            let replacement_retained = account.current.load(Ordering::SeqCst);
            assert_eq!(replacement_retained, 48);
            assert_eq!(first_retained - replacement_retained, 3_580);

            drop(harness.channel.cancel());
            {
                let state = harness.channel.state.lock().unwrap();
                let topk = state.ordered.as_ref().unwrap().reducer.topk().unwrap();
                assert!(topk.global().is_none());
                assert_eq!(state.reservation.bytes(), 40);
                assert_eq!(topk.estimated_retained_bytes(), Some(40));
            }
            assert_eq!(account.current.load(Ordering::SeqCst), 42);

            drop(harness);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn direct_submit_is_rejected_by_the_topk_strategy() {
            let harness = TopKChannelHarness::with_streams(1, 1);
            let tuple =
                OrderedTuple::try_new(harness.contract.order(), [Some(OrderedScalar::Int64(7))])
                    .unwrap();
            let update = OrderedBoundUpdate::new(harness.contract.order(), tuple).unwrap();
            let bytes = update.canonical_contribution_bytes().unwrap();
            let (binding, instance) = harness.streams[0];
            assert_eq!(
                harness
                    .channel
                    .submit_ordered(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        update,
                        TemporaryContributionLease::new(harness.temporary_account.clone(), bytes,),
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ProducerPortMismatch
            );
        }

        #[test]
        fn reservation_interleaving_repreflights_before_commit() {
            let account = Arc::new(InterleavingAccount::default());
            let harness = TopKChannelHarness::with_streams_and_account(1, 2, account.clone());
            let channel = harness.channel.clone();
            let contract = harness.contract.clone();
            let temporary = harness.temporary_account.clone();
            let (binding, instance) = harness.streams[0];
            *account.hook.lock().unwrap() = Some(Box::new(move || {
                let summary = TopKSummary::try_new(
                    &contract,
                    [5, 10]
                        .into_iter()
                        .map(|value| {
                            OrderedTuple::try_new(
                                contract.order(),
                                [Some(OrderedScalar::Int64(value))],
                            )
                            .unwrap()
                        })
                        .collect(),
                )
                .unwrap();
                let bytes = summary.canonical_contribution_bytes().unwrap();
                let action = channel
                    .submit_topk_summary(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(1),
                        summary,
                        TemporaryContributionLease::new(temporary, bytes),
                    )
                    .unwrap();
                assert!(matches!(
                    action,
                    ChannelAction::VisibleSnapshot {
                        version: LogicalVersion::FIRST,
                        ..
                    }
                ));
            }));

            assert_eq!(
                harness.submit(0, 0, &[10, 20]).unwrap(),
                TestAction::Other(SubmitOutcome::Stale)
            );
            assert_eq!(harness.latest(), Some((1, 10)));
        }

        #[test]
        fn topk_cancel_update_race_never_publishes_after_cancel() {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let harness = TopKChannelHarness::with_streams_and_account(
                1,
                1,
                Arc::new(BlockingRejectingAccount {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            );
            let channel = harness.channel.clone();
            let contract = harness.contract.clone();
            let temporary = harness.temporary_account.clone();
            let (binding, instance) = harness.streams[0];
            let (done_tx, done_rx) = mpsc::channel();
            let update = std::thread::spawn(move || {
                let summary = TopKSummary::try_new(
                    &contract,
                    vec![
                        OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(7))])
                            .unwrap(),
                    ],
                )
                .unwrap();
                let bytes = summary.canonical_contribution_bytes().unwrap();
                let result = channel.submit_topk_summary(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    summary,
                    TemporaryContributionLease::new(temporary, bytes),
                );
                done_tx.send(result).unwrap();
            });

            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            release_tx.send(()).unwrap();
            release_tx.send(()).unwrap();
            let action = done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap();
            assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
            assert!(action.snapshot().is_none());
            assert!(matches!(
                harness.channel.terminal_action(),
                ChannelAction::Cancelled { .. }
            ));
            assert!(harness.channel.snapshot().is_none());
            update.join().unwrap();
        }

        #[test]
        fn topk_close_update_race_completes_once_with_final_bound() {
            for _ in 0..32 {
                let harness = Arc::new(TopKChannelHarness::with_streams(1, 1));
                let start = Arc::new(Barrier::new(3));
                let close = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.close(0, 1)
                    })
                };
                let update = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.submit(0, 0, &[7])
                    })
                };
                start.wait();

                let outcomes = [
                    close.join().unwrap().unwrap(),
                    update.join().unwrap().unwrap(),
                ];
                assert_eq!(
                    outcomes
                        .iter()
                        .filter(|outcome| matches!(outcome, TestAction::Completed(Some((1, 7)))))
                        .count(),
                    1
                );
                assert!(outcomes.iter().any(|outcome| matches!(
                    outcome,
                    TestAction::PendingFinalSnapshot | TestAction::Published(1, 7)
                )));
                assert_eq!(harness.latest(), Some((1, 7)));
            }
        }
    }

    fn utf8_order_contract() -> (OrderContract, Arc<RuntimeOrderContract>) {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Utf8,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
        (plan, contract)
    }

    fn ordered_single_channel(
        plan: OrderContract,
        max_contribution_bytes: u64,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Arc<RuntimeFilterChannel> {
        ordered_single_channel_with_reducer_budget(
            plan,
            max_contribution_bytes,
            4096,
            memory_account,
        )
    }

    fn ordered_single_channel_with_reducer_budget(
        plan: OrderContract,
        max_contribution_bytes: u64,
        max_reducer_bytes: u64,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Arc<RuntimeFilterChannel> {
        let witness = CoverageWitnessId::new(1);
        let deployment = RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(plan),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes,
                max_artifact_bytes: 4096,
                deadline_ms: 100,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(max_reducer_bytes),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::new(),
        );
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment,
                Instant::now() + Duration::from_secs(10),
                memory_account,
            )
            .unwrap(),
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
    }

    fn utf8_update(contract: &RuntimeOrderContract, len: usize) -> OrderedBoundUpdate {
        OrderedBoundUpdate::new(
            contract,
            OrderedTuple::try_new(
                contract,
                [Some(OrderedScalar::Utf8(Arc::from("x".repeat(len))))],
            )
            .unwrap(),
        )
        .unwrap()
    }

    mod ordered {
        use super::*;

        #[test]
        fn higher_equal_advances_sequence_without_new_version() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 3, int_bound(100)).unwrap(),
                TestAction::Published(1, 100)
            );
            assert_eq!(
                harness.submit(0, 7, int_bound(100)).unwrap(),
                TestAction::SequenceAdvancedEqual
            );
            assert_eq!(harness.latest(), Some((1, 100)));
        }

        #[test]
        fn higher_looser_is_contract_violation_and_state_is_unchanged() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.submit(0, 3, int_bound(100)).unwrap();
            let before = harness.state_digest();
            let error = harness.submit(0, 4, int_bound(101)).unwrap_err();
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::OrderedBoundLoosened
            );
            assert_eq!(harness.state_digest(), before);
        }

        #[test]
        fn another_stream_may_be_looser_than_global_without_violation() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(50)).unwrap(),
                TestAction::Published(1, 50)
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(90)).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(harness.latest(), Some((1, 50)));
        }

        #[test]
        fn cumulative_close_zero_completes_without_artifact() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.close(0, 0).unwrap(),
                super::TestAction::CompletedWithoutArtifact
            );
        }

        #[test]
        fn ordered_close_emits_instance_closed_before_channel_completion() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.submit(0, 0, int_bound(40)).unwrap();
            let (binding_id, fragment_instance_id) = harness.streams[0];
            let action = harness
                .channel
                .close_ordered_partition(
                    binding_id,
                    fragment_instance_id,
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap();
            let instance_closed = action
                .events()
                .iter()
                .position(|event| {
                    matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. })
                })
                .expect("ordered terminal admission emits ProducerInstanceClosed");
            let channel_completed = action
                .events()
                .iter()
                .position(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }))
                .expect("final ordered terminal completes the channel");
            assert!(instance_closed < channel_completed);
        }

        #[test]
        fn cumulative_close_waits_only_for_terminal_minus_one_and_allows_gaps() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 7, super::int_bound(40)).unwrap(),
                super::TestAction::Published(1, 40)
            );
            assert_eq!(
                harness.close(0, 8).unwrap(),
                super::TestAction::Completed(Some((1, 40)))
            );
        }

        #[test]
        fn close_before_final_snapshot_and_snapshot_before_close_both_complete() {
            let close_first = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                close_first.close(0, 8).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert_eq!(
                close_first.submit(0, 7, int_bound(40)).unwrap(),
                TestAction::Completed(Some((1, 40)))
            );
            let update_first = OrderedChannelHarness::single_stream_anyof();
            update_first.submit(0, 7, int_bound(40)).unwrap();
            assert_eq!(
                update_first.close(0, 8).unwrap(),
                TestAction::Completed(Some((1, 40)))
            );
        }

        #[test]
        fn concurrent_close_and_final_snapshot_complete_exactly_once() {
            for _ in 0..32 {
                let harness = Arc::new(OrderedChannelHarness::single_stream_anyof());
                let start = Arc::new(Barrier::new(3));
                let close = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.close(0, 8)
                    })
                };
                let final_snapshot = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.submit(0, 7, int_bound(40))
                    })
                };
                start.wait();

                let outcomes = [
                    close.join().unwrap().unwrap(),
                    final_snapshot.join().unwrap().unwrap(),
                ];
                assert_eq!(
                    outcomes
                        .iter()
                        .filter(|outcome| matches!(outcome, TestAction::Completed(Some((1, 40)))))
                        .count(),
                    1
                );
                assert!(outcomes.iter().any(|outcome| matches!(
                    outcome,
                    TestAction::PendingFinalSnapshot | TestAction::Published(1, 40)
                )));
                assert_eq!(harness.latest(), Some((1, 40)));
                assert!(harness.channel.is_terminal());
            }
        }

        #[test]
        fn concurrent_tighter_and_higher_looser_update_preserves_tighter_state() {
            for _ in 0..32 {
                let harness = Arc::new(OrderedChannelHarness::single_stream_anyof());
                assert_eq!(
                    harness.submit(0, 0, int_bound(100)).unwrap(),
                    TestAction::Published(1, 100)
                );
                let start = Arc::new(Barrier::new(3));
                let tighter = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.submit(0, 1, int_bound(70))
                    })
                };
                let looser = {
                    let harness = harness.clone();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        harness.submit(0, 2, int_bound(110))
                    })
                };
                start.wait();

                assert_eq!(
                    tighter.join().unwrap().unwrap(),
                    TestAction::Published(2, 70)
                );
                assert_eq!(
                    looser.join().unwrap().unwrap_err().kind(),
                    RuntimeContractViolationKind::OrderedBoundLoosened
                );
                assert_eq!(harness.latest(), Some((2, 70)));
            }
        }

        #[test]
        fn availability_and_terminal_coverage_are_independent() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(80)).unwrap(),
                TestAction::Published(1, 80)
            );
            assert!(!harness.channel.is_terminal());
        }

        #[test]
        fn logical_versions_advance_only_when_global_bound_tightens() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(80)).unwrap(),
                TestAction::Published(1, 80)
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(90)).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(
                harness.submit(1, 1, int_bound(70)).unwrap(),
                TestAction::Published(2, 70)
            );
        }

        #[test]
        fn anyof_one_producer_failure_keeps_channel_available_until_other_completes() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            harness.submit(0, 0, int_bound(80)).unwrap();
            assert_eq!(
                harness.fail_stream(0).unwrap(),
                TestAction::CoverageStillPossible
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(70)).unwrap(),
                TestAction::Published(2, 70)
            );
            assert_eq!(
                harness.close(1, 1).unwrap(),
                TestAction::Completed(Some((2, 70)))
            );
        }

        #[test]
        fn conflicting_close_replay_is_rejected_after_channel_completion() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.close(0, 0).unwrap(),
                TestAction::CompletedWithoutArtifact
            );
            assert_eq!(
                harness.close(0, 1).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
        }

        #[test]
        fn update_at_terminal_is_rejected_after_channel_completion() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.submit(0, 0, int_bound(40)).unwrap();
            harness.close(0, 1).unwrap();
            assert_eq!(
                harness.submit(0, 1, int_bound(30)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn channel_action_exposes_exact_logical_terminal_mapping() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.close(0, 0).unwrap();
            assert_eq!(
                harness.channel.terminal_action().logical_terminal(),
                Some(crate::runtime_filter::core::state::LogicalTerminal::CompletedWithoutArtifact)
            );
        }

        #[test]
        fn submit_reservation_failure_revalidates_concurrent_cancel() {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let harness = OrderedChannelHarness::with_streams_and_limits(
                1,
                1024,
                4096,
                Arc::new(BlockingRejectingAccount {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            );
            let channel = harness.channel.clone();
            let contract = harness.contract.clone();
            let (binding, instance) = harness.streams[0];
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let tuple =
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(40))]).unwrap();
                let update = OrderedBoundUpdate::new(&contract, tuple).unwrap();
                let contribution_bytes = update.canonical_contribution_bytes().unwrap();
                done_tx
                    .send(channel.submit_ordered(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        update,
                        TemporaryContributionLease::new(
                            Arc::new(Account::default()),
                            contribution_bytes,
                        ),
                    ))
                    .unwrap();
            });

            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            release_tx.send(()).unwrap();
            release_tx.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert!(matches!(
                harness.channel.terminal_action(),
                ChannelAction::Cancelled { .. }
            ));
        }

        #[test]
        fn close_reservation_failure_revalidates_concurrent_cancel() {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let harness = OrderedChannelHarness::with_streams_and_limits(
                1,
                1024,
                4096,
                Arc::new(BlockingRejectingAccount {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            );
            let channel = harness.channel.clone();
            let (binding, instance) = harness.streams[0];
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                done_tx
                    .send(channel.close_ordered_partition(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    ))
                    .unwrap();
            });

            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            release_tx.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert!(matches!(
                harness.channel.terminal_action(),
                ChannelAction::Cancelled { .. }
            ));
        }

        #[test]
        fn unavailable_retains_ordered_protocol_tombstone() {
            let account = Arc::new(Account::default());
            let harness = OrderedChannelHarness::with_streams_coverage_and_limits(
                2,
                false,
                1024,
                4096,
                account.clone(),
            );
            assert_eq!(
                harness.submit(0, 0, int_bound(40)).unwrap(),
                TestAction::Other(SubmitOutcome::Published)
            );
            assert_eq!(
                harness.close(0, 2).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert!(matches!(
                harness
                    .channel
                    .expire_deadline(Instant::now() + Duration::from_secs(20)),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::IncompleteCoverage,
                    ..
                }
            ));
            {
                let state = harness.channel.state.lock().unwrap();
                let reducer = &state.ordered.as_ref().unwrap().reducer;
                assert!(reducer.global().is_none());
                assert!(state.reservation.bytes() > 0);
                assert_eq!(
                    state.reservation.bytes(),
                    reducer.estimated_retained_bytes().unwrap()
                );
                assert_eq!(
                    account.current.load(Ordering::SeqCst),
                    state.reservation.bytes()
                );
            }
            assert_eq!(
                harness.close(0, 3).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
            assert_eq!(
                harness.submit(0, 2, int_bound(40)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn cancelled_retains_ordered_protocol_tombstone() {
            let account = Arc::new(Account::default());
            let harness =
                OrderedChannelHarness::with_streams_and_limits(1, 1024, 4096, account.clone());
            assert_eq!(
                harness.submit(0, 0, int_bound(40)).unwrap(),
                TestAction::Published(1, 40)
            );
            assert_eq!(
                harness.close(0, 2).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            {
                let state = harness.channel.state.lock().unwrap();
                let ordered = state.ordered.as_ref().unwrap();
                assert!(ordered.reducer.global().is_none());
                assert!(state.reservation.bytes() > 0);
                assert_eq!(
                    state.reservation.bytes(),
                    ordered.reducer.estimated_retained_bytes().unwrap()
                );
                assert_eq!(
                    account.current.load(Ordering::SeqCst),
                    state.reservation.bytes()
                        + ordered.latest.as_ref().unwrap().retained_memory_bytes()
                );
            }
            assert_eq!(
                harness.close(0, 3).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
            assert_eq!(
                harness.submit(0, 2, int_bound(40)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn oversized_utf8_contribution_fails_open_without_reducer_mutation_or_leak() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 256);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes - 1).unwrap(),
                account.clone(),
            );
            let before_reducer = {
                let state = channel.state.lock().unwrap();
                format!("{:?}", state.ordered.as_ref().unwrap().reducer)
            };

            let action = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes),
                )
                .unwrap();
            assert_eq!(
                action.unavailable_reason(),
                Some(UnavailableReason::ResourceLimit)
            );
            let state = channel.state.lock().unwrap();
            assert_eq!(
                format!("{:?}", state.ordered.as_ref().unwrap().reducer),
                before_reducer
            );
            assert_eq!(state.reservation.bytes(), 0);
            drop(state);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn exact_ordered_contribution_budget_is_accepted_and_fully_released() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 64);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes).unwrap(),
                account.clone(),
            );

            let action = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes),
                )
                .unwrap();
            assert!(matches!(
                action,
                ChannelAction::VisibleSnapshot {
                    version: LogicalVersion::FIRST,
                    ..
                }
            ));
            drop(action);
            drop(channel);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn ordered_reservation_tracks_current_utf8_bound_under_exact_budget() {
            let (plan, contract) = utf8_order_contract();
            let probe_account = Arc::new(Account::default());
            let probe = ordered_single_channel(plan.clone(), 4096, probe_account.clone());
            let first = OrderedBoundUpdate::new(
                &contract,
                OrderedTuple::try_new(
                    &contract,
                    [Some(OrderedScalar::Utf8(Arc::from("z".repeat(256))))],
                )
                .unwrap(),
            )
            .unwrap();
            let first_bytes = first.canonical_contribution_bytes().unwrap();
            let first_action = probe
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    first,
                    TemporaryContributionLease::new(probe_account.clone(), first_bytes),
                )
                .unwrap();
            drop(first_action);
            let exact_budget = probe_account.current.load(Ordering::SeqCst);
            drop(probe);
            assert_eq!(probe_account.current.load(Ordering::SeqCst), 0);

            let account = Arc::new(Account::default());
            let channel = ordered_single_channel_with_reducer_budget(
                plan,
                4096,
                u64::try_from(exact_budget).unwrap(),
                account.clone(),
            );
            for (sequence, value) in [
                (0, "z".repeat(256)),
                (1, "y".to_owned()),
                (2, "x".repeat(256)),
            ] {
                let update = OrderedBoundUpdate::new(
                    &contract,
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Utf8(Arc::from(value)))])
                        .unwrap(),
                )
                .unwrap();
                let bytes = update.canonical_contribution_bytes().unwrap();
                let action = channel
                    .submit_ordered(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(sequence),
                        update,
                        TemporaryContributionLease::new(account.clone(), bytes),
                    )
                    .unwrap();
                assert!(matches!(action, ChannelAction::VisibleSnapshot { .. }));
                drop(action);
                let state = channel.state.lock().unwrap();
                let ordered = state.ordered.as_ref().unwrap();
                assert_eq!(
                    state.reservation.bytes(),
                    ordered.reducer.estimated_retained_bytes().unwrap()
                );
            }
            assert!(account.current.load(Ordering::SeqCst) <= exact_budget);
            drop(channel);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn mismatched_ordered_contribution_lease_preserves_state_and_releases_memory() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 32);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes).unwrap(),
                account.clone(),
            );
            let before = {
                let state = channel.state.lock().unwrap();
                format!("{:?}", state.ordered.as_ref().unwrap().reducer)
            };

            let error = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes - 1),
                )
                .unwrap_err();
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::InvalidContributionLease
            );
            let state = channel.state.lock().unwrap();
            assert_eq!(
                format!("{:?}", state.ordered.as_ref().unwrap().reducer),
                before
            );
            assert_eq!(state.reservation.bytes(), 0);
            drop(state);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }
    }

    mod fenced_final {
        use super::*;
        use novarocks::runtime_filter_transition::port::artifact::ArtifactMembershipSchema;
        use novarocks::runtime_filter_transition::port::final_domain::{
            CollectingFinalDomainTestIssuer, CompletionFenceAuthority,
            FinalDomainTestIssuerTransition, FrozenFinalDomainTestIssuer,
            RuntimeCompletionFenceContract,
        };

        struct BlockingOnceAccount {
            block_next: AtomicBool,
            current: AtomicUsize,
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }

        impl RuntimeFilterMemoryAccount for BlockingOnceAccount {
            fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
                if self.block_next.swap(false, Ordering::SeqCst) {
                    self.entered.send(()).unwrap();
                    self.release.lock().unwrap().recv().unwrap();
                }
                self.current.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }

            fn release(&self, bytes: usize) {
                let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
                assert!(previous >= bytes);
            }
        }

        fn blocking_once_account() -> (
            Arc<BlockingOnceAccount>,
            mpsc::Receiver<()>,
            mpsc::Sender<()>,
        ) {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            (
                Arc::new(BlockingOnceAccount {
                    block_next: AtomicBool::new(true),
                    current: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
                entered_rx,
                release_tx,
            )
        }

        fn final_deployment(
            producers: &[(u32, u32, i64)],
            local_partition_budget: u64,
        ) -> RuntimeFilterChannelDeployment {
            let coverage = Coverage::AllOf(
                producers
                    .iter()
                    .map(|(_, witness, _)| Coverage::Leaf(CoverageWitnessId::new(*witness)))
                    .collect(),
            );
            RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NullSafeEqual,
                },
                RuntimeFilterLifecycle::CompleteOnce,
                coverage.clone(),
                coverage,
                ReductionRequirement::SetUnion,
                BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 4096,
                    max_artifact_bytes: 1,
                    deadline_ms: 10,
                    max_retries: 0,
                },
                RuntimeFilterCoreBudget::new(local_partition_budget),
                MaterializationPolicy::for_test(),
                producers
                    .iter()
                    .map(|(binding, witness, instance)| {
                        (
                            BindingId::new(*binding),
                            ProducerDeployment::new(
                                CoverageWitnessId::new(*witness),
                                BTreeSet::from([uid(*instance)]),
                            ),
                        )
                    })
                    .collect(),
                BTreeMap::new(),
            )
        }

        fn frozen_issuer(binding: u32, instance: i64) -> FrozenFinalDomainTestIssuer {
            let schema =
                ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NullSafeEqual)
                    .unwrap();
            let contract = Arc::new(
                RuntimeCompletionFenceContract::try_from_install(
                    uid(99),
                    DeploymentEpoch::new(1),
                    ChannelId::new(1),
                    CompletionFenceKind::CommittedDomainFrozen,
                    &schema,
                )
                .unwrap(),
            );
            let authority =
                CompletionFenceAuthority::try_new(contract, BindingId::new(binding), uid(instance))
                    .unwrap();
            match CollectingFinalDomainTestIssuer::new(authority, 1).close_driver() {
                FinalDomainTestIssuerTransition::Frozen(issuer) => issuer,
                FinalDomainTestIssuerTransition::Collecting(_) => unreachable!(),
            }
        }

        fn final_channel(
            producers: &[(u32, u32, i64)],
            budget: u64,
        ) -> (RuntimeFilterChannel, Arc<Account>) {
            let account = Arc::new(Account::default());
            let channel = RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &final_deployment(producers, budget),
                Instant::now() + Duration::from_secs(10),
                account.clone(),
            )
            .unwrap();
            (channel, account)
        }

        fn final_channel_with_memory_account(
            producers: &[(u32, u32, i64)],
            budget: u64,
            account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Arc<RuntimeFilterChannel> {
            Arc::new(
                RuntimeFilterChannel::new(
                    uid(99),
                    RuntimeFilterParticipantId::new(1),
                    DeploymentEpoch::new(1),
                    &final_deployment(producers, budget),
                    Instant::now() + Duration::from_secs(10),
                    account,
                )
                .unwrap(),
            )
        }

        fn shard(
            issuer: &FrozenFinalDomainTestIssuer,
            binding: u32,
            instance: i64,
            partition: u32,
            sequence: u64,
            values: &[i64],
        ) -> novarocks::runtime_filter_transition::port::final_domain::FinalDomainShard {
            issuer
                .issue_shard(
                    ProducerStreamId::new(
                        BindingId::new(binding),
                        uid(instance),
                        PartitionId::new(partition),
                    ),
                    ProducerSequence::new(sequence),
                    ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false),
                )
                .unwrap()
        }

        fn complete(
            channel: &RuntimeFilterChannel,
            account: Arc<Account>,
            binding: u32,
            instance: i64,
            partition: u32,
            sequence: u64,
            shard: novarocks::runtime_filter_transition::port::final_domain::FinalDomainShard,
        ) -> Result<ChannelAction, FinalDomainRejection> {
            let bytes = shard.canonical_contribution_bytes().unwrap();
            channel.complete_final(
                BindingId::new(binding),
                uid(instance),
                PartitionId::new(partition),
                ProducerSequence::new(sequence),
                shard,
                TemporaryContributionLease::new(account, bytes),
            )
        }

        #[test]
        fn final_and_incremental_membership_modes_are_mutually_exclusive_and_scope_is_checked() {
            let (final_channel, account) = final_channel(&[(10, 1, 10)], 4096);
            final_channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            assert_eq!(
                submit(&final_channel, account.clone(), 10, 10, 0, &[1])
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ProducerPortMismatch
            );

            let wrong_scope = shard(&frozen_issuer(20, 10), 20, 10, 0, 0, &[1]);
            assert_eq!(
                complete(&final_channel, account, 10, 10, 0, 0, wrong_scope)
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::UnauthorizedBinding
            );

            let (ordinary, ordinary_account, _) = one_channel();
            ordinary
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let wrong_mode = shard(&frozen_issuer(10, 10), 10, 10, 0, 0, &[1]);
            assert_eq!(
                complete(&ordinary, ordinary_account, 10, 10, 0, 0, wrong_mode)
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ProducerPortMismatch
            );
        }

        #[test]
        fn out_of_order_replay_gap_and_close_zero_follow_exclusive_terminal_range() {
            let issuer = frozen_issuer(10, 10);
            let (channel, account) = final_channel(&[(10, 1, 10)], 4096);
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let seq1 = shard(&issuer, 10, 10, 0, 1, &[2]);
            let seq0 = shard(&issuer, 10, 10, 0, 0, &[1]);
            assert_eq!(
                channel
                    .close_final_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(2),
                    )
                    .unwrap()
                    .outcome(),
                SubmitOutcome::PendingGap
            );
            assert_eq!(
                channel
                    .close_final_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(3),
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
            assert_eq!(
                complete(&channel, account.clone(), 10, 10, 0, 1, seq1.clone())
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Applied
            );
            assert!(channel.snapshot().is_none());
            assert_eq!(
                complete(&channel, account.clone(), 10, 10, 0, 1, seq1)
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Duplicate
            );
            assert_eq!(
                complete(&channel, account, 10, 10, 0, 0, seq0)
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Completed
            );
            assert_eq!(channel.snapshot().unwrap().domain().values().len(), 2);

            let (empty, empty_account) = final_channel(&[(10, 1, 10)], 4096);
            empty.open_producer(BindingId::new(10), uid(10), 1).unwrap();
            assert_eq!(
                empty
                    .close_final_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::FinalDomainMissing
            );
            assert_eq!(empty_account.current.load(Ordering::SeqCst), 0);
            assert_eq!(
                empty
                    .state
                    .lock()
                    .unwrap()
                    .producers
                    .get(&BindingId::new(10))
                    .unwrap()
                    .instances
                    .get(&uid(10))
                    .unwrap()
                    .materialized_partition_count(),
                0
            );
            let explicit_empty = shard(&issuer, 10, 10, 0, 0, &[]);
            complete(&empty, empty_account, 10, 10, 0, 0, explicit_empty).unwrap();
            assert_eq!(
                empty
                    .close_final_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(1),
                    )
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Completed
            );
            assert!(empty.snapshot().unwrap().domain().values().is_empty());
        }

        #[test]
        fn multipartition_allof_and_explicit_empty_domains_complete_once() {
            let (channel, account) = final_channel(&[(10, 1, 10), (20, 2, 20)], 8192);
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap();
            channel
                .open_producer(BindingId::new(20), uid(20), 1)
                .unwrap();
            for (index, (binding, instance, partition, values)) in [
                (10, 10, 0, vec![1]),
                (10, 10, 1, Vec::new()),
                (20, 20, 0, vec![2]),
            ]
            .into_iter()
            .enumerate()
            {
                let issuer = frozen_issuer(binding, instance);
                let final_shard = shard(&issuer, binding, instance, partition, 0, &values);
                complete(
                    &channel,
                    account.clone(),
                    binding,
                    instance,
                    partition,
                    0,
                    final_shard,
                )
                .unwrap();
                channel
                    .close_final_partition(
                        BindingId::new(binding),
                        uid(instance),
                        PartitionId::new(partition),
                        ProducerSequence::new(1),
                    )
                    .unwrap();
                if index < 2 {
                    assert!(channel.snapshot().is_none());
                }
            }
            let snapshot = channel.snapshot().unwrap();
            assert_eq!(snapshot.version(), LogicalVersion::FIRST);
            assert_eq!(snapshot.domain().values().len(), 2);
        }

        #[test]
        fn semantic_validation_precedes_resource_and_terminal_precedence_keeps_completed_replay() {
            let issuer = frozen_issuer(10, 10);
            let (channel, account) = final_channel(&[(10, 1, 10)], 4096);
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let wrong = shard(&frozen_issuer(20, 10), 20, 10, 0, 0, &[9]);
            assert_eq!(
                channel
                    .reject_final_resource_exhausted(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        &wrong,
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::UnauthorizedBinding
            );
            assert!(!channel.is_terminal());

            let first = shard(&issuer, 10, 10, 0, 0, &[1]);
            complete(&channel, account.clone(), 10, 10, 0, 0, first.clone()).unwrap();
            channel
                .close_final_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap();
            assert_eq!(
                complete(&channel, account.clone(), 10, 10, 0, 0, first.clone())
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Duplicate
            );
            assert_eq!(
                channel
                    .reject_final_resource_exhausted(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        &first,
                    )
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Duplicate
            );
            let conflict = shard(&issuer, 10, 10, 0, 0, &[2]);
            assert_eq!(
                complete(&channel, account.clone(), 10, 10, 0, 0, conflict.clone())
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ConflictingReplay
            );
            assert_eq!(
                channel
                    .reject_final_resource_exhausted(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        &conflict,
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ConflictingReplay
            );
            let completed_late = shard(&issuer, 10, 10, 0, 1, &[3]);
            assert_eq!(
                channel
                    .complete_final(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(1),
                        completed_late.clone(),
                        TemporaryContributionLease::new(account.clone(), 0),
                    )
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert_eq!(
                channel
                    .reject_final_resource_exhausted(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(1),
                        &completed_late,
                    )
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );

            for terminal in ["unavailable", "cancelled"] {
                let (channel, account) = final_channel(&[(10, 1, 10)], 4096);
                channel
                    .open_producer(BindingId::new(10), uid(10), 1)
                    .unwrap();
                let valid = shard(&issuer, 10, 10, 0, 0, &[1]);
                complete(&channel, account.clone(), 10, 10, 0, 0, valid.clone()).unwrap();
                assert!(account.current.load(Ordering::SeqCst) > 0);
                if terminal == "unavailable" {
                    let next = shard(&issuer, 10, 10, 0, 1, &[2]);
                    channel
                        .reject_final_resource_exhausted(
                            BindingId::new(10),
                            uid(10),
                            PartitionId::new(0),
                            ProducerSequence::new(1),
                            &next,
                        )
                        .unwrap();
                } else {
                    drop(channel.cancel());
                }
                assert_eq!(account.current.load(Ordering::SeqCst), 0);
                assert_eq!(
                    channel
                        .state
                        .lock()
                        .unwrap()
                        .producers
                        .get(&BindingId::new(10))
                        .unwrap()
                        .instances
                        .get(&uid(10))
                        .unwrap()
                        .materialized_partition_count(),
                    0
                );
                let wrong = shard(&frozen_issuer(20, 10), 20, 10, 0, 0, &[9]);
                assert_eq!(
                    complete(&channel, account.clone(), 10, 10, 0, 0, wrong)
                        .unwrap_err()
                        .kind(),
                    RuntimeContractViolationKind::UnauthorizedBinding
                );
                assert_eq!(
                    channel
                        .complete_final(
                            BindingId::new(10),
                            uid(10),
                            PartitionId::new(0),
                            ProducerSequence::new(0),
                            valid,
                            TemporaryContributionLease::new(account, 0),
                        )
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::TerminalNoop
                );
            }
        }

        #[test]
        fn retained_rejection_after_temporary_success_is_atomic_and_scope_stays_first() {
            let channel =
                final_channel_with_memory_account(&[(10, 1, 10)], 4096, Arc::new(RejectingAccount));
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let temporary = Arc::new(Account::default());
            let wrong = shard(&frozen_issuer(20, 10), 20, 10, 0, 0, &[9]);
            let wrong_bytes = wrong.canonical_contribution_bytes().unwrap();
            assert_eq!(
                channel
                    .complete_final(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        wrong,
                        TemporaryContributionLease::new(temporary.clone(), wrong_bytes),
                    )
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::UnauthorizedBinding
            );
            assert!(!channel.is_terminal());
            assert_eq!(temporary.current.load(Ordering::SeqCst), 0);

            let valid = shard(&frozen_issuer(10, 10), 10, 10, 0, 0, &[1]);
            let valid_bytes = valid.canonical_contribution_bytes().unwrap();
            assert!(matches!(
                channel
                    .complete_final(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        valid,
                        TemporaryContributionLease::new(temporary.clone(), valid_bytes),
                    )
                    .unwrap(),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::ResourceLimit,
                    ..
                }
            ));
            assert_eq!(temporary.current.load(Ordering::SeqCst), 0);
            let state = channel.state.lock().unwrap();
            assert_eq!(state.reservation.bytes(), 0);
            assert!(state.reducer.as_ref().unwrap().domain().values().is_empty());
            assert_eq!(
                state
                    .producers
                    .get(&BindingId::new(10))
                    .unwrap()
                    .instances
                    .get(&uid(10))
                    .unwrap()
                    .materialized_partition_count(),
                0
            );
        }

        #[test]
        fn retained_reservation_repreflights_duplicate_and_rolls_back_after_cancel() {
            let issuer = frozen_issuer(10, 10);
            let (account, entered, release) = blocking_once_account();
            let channel = final_channel_with_memory_account(&[(10, 1, 10)], 4096, account.clone());
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let final_shard = shard(&issuer, 10, 10, 0, 0, &[1]);
            let outer_channel = channel.clone();
            let outer_shard = final_shard.clone();
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                done_tx
                    .send(complete(
                        &outer_channel,
                        Arc::new(Account::default()),
                        10,
                        10,
                        0,
                        0,
                        outer_shard,
                    ))
                    .unwrap();
            });
            entered.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                complete(
                    &channel,
                    Arc::new(Account::default()),
                    10,
                    10,
                    0,
                    0,
                    final_shard,
                )
                .unwrap()
                .outcome(),
                SubmitOutcome::Applied
            );
            let retained_after_inner = account.current.load(Ordering::SeqCst);
            assert!(retained_after_inner > 0);
            release.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::Duplicate
            );
            assert_eq!(account.current.load(Ordering::SeqCst), retained_after_inner);
            drop(channel.cancel());
            assert_eq!(account.current.load(Ordering::SeqCst), 0);

            let (account, entered, release) = blocking_once_account();
            let channel = final_channel_with_memory_account(&[(10, 1, 10)], 4096, account.clone());
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let final_shard = shard(&issuer, 10, 10, 0, 0, &[1]);
            let submit_channel = channel.clone();
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                done_tx
                    .send(complete(
                        &submit_channel,
                        Arc::new(Account::default()),
                        10,
                        10,
                        0,
                        0,
                        final_shard,
                    ))
                    .unwrap();
            });
            entered.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
            release.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn retained_account_callbacks_reenter_final_channel_without_locking_it() {
            let account = Arc::new(ReentrantAccount::default());
            let channel = final_channel_with_memory_account(&[(10, 1, 10)], 4096, account.clone());
            *account.channel.lock().unwrap() = Some(Arc::downgrade(&channel));
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let final_shard = shard(&frozen_issuer(10, 10), 10, 10, 0, 0, &[1]);
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let outcome = complete(
                    &channel,
                    Arc::new(Account::default()),
                    10,
                    10,
                    0,
                    0,
                    final_shard,
                )
                .map(|action| action.outcome());
                drop(channel.cancel());
                done_tx
                    .send((outcome, account.current.load(Ordering::SeqCst)))
                    .unwrap();
            });
            let (outcome, current) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(outcome.unwrap(), SubmitOutcome::Applied);
            assert_eq!(current, 0);
        }

        #[test]
        fn oversized_and_deadline_terminals_release_collecting_state_while_completed_keeps_replay_accounted()
         {
            let base = final_deployment(&[(10, 1, 10)], 4096);
            let mut tiny_policy = base.policy();
            tiny_policy.max_contribution_bytes = 1;
            let tiny = RuntimeFilterChannelDeployment::new(
                base.channel_id(),
                base.logical_domain().clone(),
                base.lifecycle(),
                base.availability_coverage().clone(),
                base.terminal_coverage().clone(),
                base.reduction_requirement(),
                base.allowed_contribution_kinds().clone(),
                base.completion_requirement(),
                tiny_policy,
                base.core_budget(),
                base.materialization_policy(),
                base.producers().clone(),
                base.consumers().clone(),
            );
            let tiny_account = Arc::new(Account::default());
            let tiny_channel = RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &tiny,
                Instant::now() + Duration::from_secs(10),
                tiny_account.clone(),
            )
            .unwrap();
            tiny_channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            let wrong = shard(&frozen_issuer(20, 10), 20, 10, 0, 0, &[9]);
            assert_eq!(
                complete(&tiny_channel, tiny_account.clone(), 10, 10, 0, 0, wrong,)
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::UnauthorizedBinding
            );
            assert!(!tiny_channel.is_terminal());
            let issuer = frozen_issuer(10, 10);
            let valid = shard(&issuer, 10, 10, 0, 0, &[1]);
            assert!(matches!(
                complete(&tiny_channel, tiny_account.clone(), 10, 10, 0, 0, valid,).unwrap(),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::ResourceLimit,
                    ..
                }
            ));
            assert_eq!(tiny_account.current.load(Ordering::SeqCst), 0);
            assert_eq!(
                tiny_channel
                    .state
                    .lock()
                    .unwrap()
                    .producers
                    .get(&BindingId::new(10))
                    .unwrap()
                    .instances
                    .get(&uid(10))
                    .unwrap()
                    .materialized_partition_count(),
                0
            );

            let (deadline_channel, deadline_account) = final_channel(&[(10, 1, 10)], 4096);
            deadline_channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            assert!(matches!(
                deadline_channel.expire_deadline(Instant::now() + Duration::from_secs(20)),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::IncompleteCoverage,
                    ..
                }
            ));
            let late = shard(&issuer, 10, 10, 0, 0, &[1]);
            assert_eq!(
                complete(
                    &deadline_channel,
                    deadline_account.clone(),
                    10,
                    10,
                    0,
                    0,
                    late,
                )
                .unwrap()
                .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert_eq!(deadline_account.current.load(Ordering::SeqCst), 0);

            let (completed, completed_account) = final_channel(&[(10, 1, 10)], 4096);
            completed
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            complete(
                &completed,
                completed_account.clone(),
                10,
                10,
                0,
                0,
                shard(&issuer, 10, 10, 0, 0, &[1]),
            )
            .unwrap();
            completed
                .close_final_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap();
            let snapshot = completed.snapshot().unwrap();
            assert!(snapshot.retained_memory_bytes() >= super::super::REPLAY_METADATA_BYTES);
            assert_eq!(
                completed_account.current.load(Ordering::SeqCst),
                snapshot.retained_memory_bytes()
            );
            drop(completed);
            assert!(completed_account.current.load(Ordering::SeqCst) > 0);
            drop(snapshot);
            assert_eq!(completed_account.current.load(Ordering::SeqCst), 0);
        }
    }
}
