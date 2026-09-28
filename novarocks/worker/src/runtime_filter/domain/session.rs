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

//! Backend-private runtime-filter session for one installed channel.
//!
//! Execution owns the producer/consumer contracts and contribution format.
//! Backend owns the installed binding/instance/route authority, reduction
//! lifetime, coverage fences, materialization, and subscription fan-out.  It
//! never evaluates Arrow rows or scan facts.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::{
    runtime_filter as worker_runtime_filter, runtime_filter::artifact::ConsumerArtifactProfile,
};
use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    LiveTerminal, LogicalVersion, PartitionId, ProducerSequence, RuntimeFilterBindOutcome,
    RuntimeFilterBindingId, RuntimeFilterConsumerContract, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterContribution, RuntimeFilterExecutionContract,
    RuntimeFilterProducer, RuntimeFilterProducerFailure, RuntimeFilterProducerHandle,
    RuntimeFilterProducerOpenRequest, RuntimeFilterSnapshot, RuntimeFilterSubmitOutcome,
    RuntimeFilterSubscriptionHandle, RuntimeFilterSubscriptionRequest, SnapshotAcquireOutcome,
    UnavailableReason,
};
use novarocks_types::UniqueId;

use super::{
    BackendChannelIdentity, BackendChannelInstall, BackendConsumerInstall, BackendCoverageProgress,
    BackendCoverageState, BackendFrontendFeedbackPublication, BackendInstallPolicy,
    BackendInstallPolicyError, BackendParticipantIdentity, BackendProducerInstall,
    BackendProducerStreamIdentity, BackendReducedLogicalDomain, BackendReducedLogicalSnapshot,
    BackendReductionApply, BackendReductionState, BackendReductionStateError, BackendRouteEdgeId,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendSubscriptionError,
    BackendSubscriptionGroup, MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE,
};

/// A Backend-owned encoded delivery ready for the participant's route authority.
/// Session materializes exactly one consumer profile; the participant chooses
/// the physical loopback or remote leg for the sealed route set.
#[derive(Clone, Debug)]
pub struct BackendMaterializedDelivery {
    channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
    route_edge_ids: Arc<[BackendRouteEdgeId]>,
    kind: super::BackendEnvelopeKind,
    schema_digest: [u8; 32],
    payload: Arc<[u8]>,
}

impl BackendMaterializedDelivery {
    pub fn new(
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        route_edge_ids: impl Into<Arc<[BackendRouteEdgeId]>>,
        kind: super::BackendEnvelopeKind,
        schema_digest: [u8; 32],
        payload: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            channel_id,
            route_edge_ids: route_edge_ids.into(),
            kind,
            schema_digest,
            payload: payload.into(),
        }
    }

    pub const fn channel_id(&self) -> novarocks_execution::runtime_filter::RuntimeFilterChannelId {
        self.channel_id
    }

    pub const fn route_edge_ids(&self) -> &Arc<[BackendRouteEdgeId]> {
        &self.route_edge_ids
    }

    pub const fn kind(&self) -> super::BackendEnvelopeKind {
        self.kind
    }

    pub const fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    pub const fn payload(&self) -> &Arc<[u8]> {
        &self.payload
    }
}

/// Participant-private physical fanout. It deliberately receives only an
/// encoded artifact frame and cannot observe reducer or evaluator state.
pub trait BackendMaterializedDeliverySink: Send + Sync {
    fn dispatch(
        &self,
        delivery: BackendMaterializedDelivery,
    ) -> Result<(), RuntimeFilterContractViolation>;
}

/// The terminal feedback path is intentionally independent from physical
/// artifact routing.  Its implementation belongs to the attempt-local query
/// lifecycle owner, which may drop it at cancellation without retaining this
/// runtime-filter session.
pub trait BackendFrontendFeedbackSink: Send + Sync {
    fn try_publish(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        deployment_epoch: u64,
        publication: &BackendFrontendFeedbackPublication,
        outcome: BackendFrontendFeedbackOutcome,
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendFrontendFeedbackOutcome {
    CanonicalDomain(Arc<[u8]>),
    DomainBudget,
    TypeUnsupported,
    ReductionUnavailable,
    ProducerUnavailable,
}

#[derive(Debug)]
pub enum BackendRuntimeFilterSessionError {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    MissingProducer,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    InstallPolicy(BackendInstallPolicyError),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    Reduction(BackendReductionStateError),
    ProducerShapeMismatch,
    ConsumerContractMismatch,
}

impl fmt::Display for BackendRuntimeFilterSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter channel session: {self:?}"
        )
    }
}

impl std::error::Error for BackendRuntimeFilterSessionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRuntimeFilterSessionSubmission {
    outcome: RuntimeFilterSubmitOutcome,
    publication: Option<BackendReducedLogicalSnapshot>,
}

impl BackendRuntimeFilterSessionSubmission {
    pub const fn outcome(&self) -> RuntimeFilterSubmitOutcome {
        self.outcome
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn publication(&self) -> Option<&BackendReducedLogicalSnapshot> {
        self.publication.as_ref()
    }
}

#[derive(Debug)]
struct BackendOpenedProducer {
    partition_count: u32,
    terminal_partitions: BTreeMap<PartitionId, ProducerSequence>,
}

impl BackendOpenedProducer {
    fn new(partition_count: u32) -> Self {
        Self {
            partition_count,
            terminal_partitions: BTreeMap::new(),
        }
    }

    fn contains_partition(&self, partition: PartitionId) -> bool {
        partition.get() < self.partition_count
    }

    fn is_closed(&self) -> bool {
        self.terminal_partitions.len() == self.partition_count as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendProducerBindingTerminal {
    Closed,
    Failed,
}

#[derive(Default)]
struct BackendProducerBindingProgress {
    instances: BTreeMap<UniqueId, BackendOpenedProducer>,
    terminal: Option<BackendProducerBindingTerminal>,
}

struct BackendInstalledConsumer {
    contract: RuntimeFilterConsumerContract,
    profile: ConsumerArtifactProfile,
    routes: BTreeSet<BackendRouteEdgeId>,
    subscriptions: BackendSubscriptionGroup,
}

/// A channel-wide Backend session.  A single strict reduction state is shared
/// by every installed producer binding only after construction verifies they
/// have identical Execution semantics and contribution budget.
pub struct BackendRuntimeFilterSession {
    // A participant that only consumes a remotely materialized artifact has
    // no local producer/reducer authority. Its subscription state remains a
    // first-class installed session, but every producer-side field is absent.
    policy: Option<BackendInstallPolicy>,
    participant: BackendParticipantIdentity,
    channel: BackendChannelInstall,
    producers: BTreeMap<RuntimeFilterBindingId, BackendProducerInstall>,
    producer_progress: Mutex<BTreeMap<RuntimeFilterBindingId, BackendProducerBindingProgress>>,
    consumers: BTreeMap<RuntimeFilterBindingId, BackendInstalledConsumer>,
    reduction: Option<Mutex<BackendReductionState>>,
    availability: Option<Mutex<BackendCoverageState>>,
    terminal: Option<Mutex<BackendCoverageState>>,
    materialized_delivery_sink: Mutex<Option<Arc<dyn BackendMaterializedDeliverySink>>>,
    frontend_feedback_sink: Mutex<Option<Weak<dyn BackendFrontendFeedbackSink>>>,
    frontend_feedback_published: AtomicBool,
    events: Arc<dyn BackendRuntimeFilterEventObserver>,
}

impl BackendRuntimeFilterSession {
    // Design: ADR-0044 (docs/adr/ADR-0044-backend-runtime-filter-participant-domain.md)
    /// Builds a session from one sealed channel installation. The caller
    /// supplies only the Backend event observer; no Core transition adapter is
    /// involved in construction.
    pub fn from_channel_install(
        participant: BackendParticipantIdentity,
        channel: BackendChannelInstall,
        events: Arc<dyn BackendRuntimeFilterEventObserver>,
    ) -> Result<Self, BackendRuntimeFilterSessionError> {
        let first = channel.producers().values().next();
        let (policy, reduction, availability, terminal) = if let Some(first) = first {
            validate_producer_shape(&channel, first, first)?;
            for producer in channel.producers().values() {
                validate_producer_shape(&channel, first, producer)?;
            }
            let policy = BackendInstallPolicy::new(
                participant,
                first.contract().clone(),
                channel.availability_coverage().clone(),
                first.max_contribution_bytes(),
            )
            .map_err(BackendRuntimeFilterSessionError::InstallPolicy)?;
            let reduction = BackendReductionState::new(policy.clone())
                .map_err(BackendRuntimeFilterSessionError::Reduction)?;
            let availability = BackendCoverageState::new(channel.availability_coverage())
                .expect("BackendChannelInstall has validated coverage");
            let terminal = BackendCoverageState::new(channel.terminal_coverage())
                .expect("BackendChannelInstall has validated coverage");
            (
                Some(policy),
                Some(Mutex::new(reduction)),
                Some(Mutex::new(availability)),
                Some(Mutex::new(terminal)),
            )
        } else {
            (None, None, None, None)
        };

        let mut consumers = BTreeMap::new();
        for (binding_id, consumer) in channel.consumers() {
            if let Some(first) = first {
                validate_consumer_shape(&channel, first, consumer)?;
            } else {
                validate_consumer_only_shape(&channel, consumer)?;
            }
            let session_channel =
                BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
            consumers.insert(
                *binding_id,
                BackendInstalledConsumer {
                    contract: consumer.contract().clone(),
                    profile: consumer.profile().clone(),
                    routes: consumer.route_edge_ids().clone(),
                    subscriptions: BackendSubscriptionGroup::new(
                        session_channel,
                        *binding_id,
                        consumer.contract().activation(),
                        consumer.route_edge_ids().iter().copied(),
                        consumer.expected_fragment_instances().iter().copied(),
                        Arc::clone(&events),
                    ),
                },
            );
        }

        for binding_id in channel.producers().keys().chain(channel.consumers().keys()) {
            events.record(BackendRuntimeFilterEvent::ChannelPlanned {
                channel: BackendChannelIdentity::new(
                    participant,
                    *binding_id,
                    channel.channel_id(),
                ),
            });
        }

        Ok(Self {
            policy,
            participant,
            producers: channel.producers().clone(),
            channel,
            producer_progress: Mutex::new(BTreeMap::new()),
            consumers,
            reduction,
            availability,
            terminal,
            materialized_delivery_sink: Mutex::new(None),
            frontend_feedback_sink: Mutex::new(None),
            frontend_feedback_published: AtomicBool::new(false),
            events,
        })
    }

    pub fn set_materialized_delivery_sink(&self, sink: Arc<dyn BackendMaterializedDeliverySink>) {
        *self
            .materialized_delivery_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sink);
    }

    pub fn set_frontend_feedback_sink(&self, sink: Weak<dyn BackendFrontendFeedbackSink>) {
        *self
            .frontend_feedback_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sink);
    }

    pub fn clear_frontend_feedback_sink(&self) {
        *self
            .frontend_feedback_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }

    pub const fn policy(&self) -> &BackendInstallPolicy {
        self.policy
            .as_ref()
            .expect("only an installed producer binding can request a contribution budget")
    }

    pub const fn channel(&self) -> &BackendChannelInstall {
        &self.channel
    }

    pub fn availability_progress(&self) -> BackendCoverageProgress {
        self.availability
            .as_ref()
            .map_or(BackendCoverageProgress::Satisfied, |availability| {
                availability
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .progress(self.channel.availability_coverage())
            })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn terminal_progress(&self) -> BackendCoverageProgress {
        self.terminal
            .as_ref()
            .map_or(BackendCoverageProgress::Satisfied, |terminal| {
                terminal
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .progress(self.channel.terminal_coverage())
            })
    }

    /// Opens one authorized producer binding/instance. Binding, witness, and
    /// expected instance authority all come from the sealed channel install.
    pub fn open_producer(
        self: &Arc<Self>,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<RuntimeFilterBindOutcome<RuntimeFilterProducerHandle>, RuntimeFilterContractViolation>
    {
        let binding_id = request.contract().binding_id();
        self.register_producer(binding_id, fragment_instance_id, request)?;
        Ok(RuntimeFilterBindOutcome::Bound(Arc::new(
            BackendRuntimeFilterProducer {
                session: Arc::clone(self),
                binding_id,
                fragment_instance_id,
            },
        )))
    }

    /// Resolves an installed consumer by its exact Execution binding contract
    /// and fragment instance. The sealed installation authorizes both values;
    /// an unexpected fragment instance is a contract violation, not a
    /// runtime route-unavailable outcome.
    pub fn subscribe(
        &self,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
        RuntimeFilterContractViolation,
    > {
        let binding_id = request.contract().binding_id();
        let consumer = self.consumers.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed in this Backend channel session",
            )
        })?;
        if request.contract() != &consumer.contract {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "consumer execution contract does not match the installed Backend binding",
            ));
        }
        let subscription = consumer.subscriptions.handle(fragment_instance_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer fragment instance is not authorized by the sealed Backend installation",
            )
        })?;
        Ok(RuntimeFilterBindOutcome::Bound(subscription))
    }

    /// Strictly reduces one canonical Execution contribution. A participant
    /// artifact owner may consume `publication`; this session never turns it
    /// into an artifact bundle itself.
    pub fn submit(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<BackendRuntimeFilterSessionSubmission, RuntimeFilterContractViolation> {
        let stream = self.open_stream(binding_id, fragment_instance_id, partition)?;
        let reduction = self.reduction.as_ref().ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only Backend channel cannot accept a producer contribution",
            )
        })?;
        // Keep reduction and publication in one ordered critical section.
        // Otherwise two producer threads can reduce v1 then v2 under this
        // mutex but publish them as v2 then v1 after releasing it.
        let mut reduction = reduction.lock().unwrap_or_else(|error| error.into_inner());
        let (apply, publication) = reduction
            .submit(stream, sequence, contribution)
            .map_err(reduction_violation)?;
        if let Some(snapshot) = publication.as_ref()
            && self.channel.lifecycle() == super::BackendChannelLifecycle::MonotonicUpdates
        {
            self.publish_reduced_snapshot(snapshot, None)?;
        }
        drop(reduction);
        Ok(BackendRuntimeFilterSessionSubmission {
            outcome: match apply {
                BackendReductionApply::Applied { .. } => RuntimeFilterSubmitOutcome::Published,
                BackendReductionApply::Duplicate => RuntimeFilterSubmitOutcome::Duplicate,
                BackendReductionApply::Stale => RuntimeFilterSubmitOutcome::Stale,
                BackendReductionApply::SequenceAdvancedEqual => {
                    RuntimeFilterSubmitOutcome::SequenceAdvancedEqual
                }
            },
            publication,
        })
    }

    /// Closes one producer partition. Its witness is marked satisfied only
    /// when every expected fragment instance for that same producer binding
    /// has opened and closed every declared local partition.
    pub fn close_partition(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        let binding_closed = {
            let mut progress = self
                .producer_progress
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let binding = progress.get_mut(&binding_id).ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer binding has not been opened in this Backend session",
                )
            })?;
            if binding.terminal.is_some() {
                return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
            }
            let instance = binding
                .instances
                .get_mut(&fragment_instance_id)
                .ok_or_else(|| {
                    contract_violation(
                        RuntimeFilterContractViolationKind::UnauthorizedBinding,
                        "producer fragment instance has not been opened in this Backend session",
                    )
                })?;
            validate_open_partition(instance, partition)?;
            match instance.terminal_partitions.get(&partition) {
                Some(previous) if *previous == terminal => {
                    return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
                }
                Some(_) => {
                    return Err(contract_violation(
                        RuntimeFilterContractViolationKind::ContractMismatch,
                        "producer partition closed with a conflicting terminal sequence",
                    ));
                }
                None => {
                    instance.terminal_partitions.insert(partition, terminal);
                    let closed = install
                        .expected_fragment_instances()
                        .iter()
                        .all(|expected| {
                            binding
                                .instances
                                .get(expected)
                                .is_some_and(BackendOpenedProducer::is_closed)
                        });
                    if closed {
                        binding.terminal = Some(BackendProducerBindingTerminal::Closed);
                    }
                    closed
                }
            }
        };
        if !binding_closed {
            return Ok(RuntimeFilterSubmitOutcome::CoverageStillPossible);
        }
        self.mark_satisfied(install.coverage_witness());
        Ok(match self.availability_progress() {
            BackendCoverageProgress::Satisfied => {
                let publication = self.reduction.as_ref().and_then(|reduction| {
                    reduction
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .latest_snapshot()
                });
                match publication {
                    Some(snapshot) => {
                        let terminal = Some(LiveTerminal::Completed);
                        self.publish_reduced_snapshot(&snapshot, terminal)?;
                        RuntimeFilterSubmitOutcome::PendingFinalSnapshot
                    }
                    None => {
                        self.publish_frontend_feedback(
                            BackendFrontendFeedbackOutcome::ReductionUnavailable,
                        );
                        self.publish_terminal(LiveTerminal::CompletedWithoutArtifact)?;
                        RuntimeFilterSubmitOutcome::CompletedWithoutArtifact
                    }
                }
            }
            BackendCoverageProgress::Pending => RuntimeFilterSubmitOutcome::CoverageStillPossible,
            BackendCoverageProgress::Impossible => {
                RuntimeFilterSubmitOutcome::CompletedWithoutArtifact
            }
        })
    }

    /// Fails one producer binding and fail-opens every installed consumer via
    /// the typed Execution unavailable outcome. No evaluator effect is made.
    pub fn fail(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        _reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        {
            let mut progress = self
                .producer_progress
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let binding = progress.get_mut(&binding_id).ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer binding has not been opened in this Backend session",
                )
            })?;
            if !binding.instances.contains_key(&fragment_instance_id) {
                return Err(contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer fragment instance has not been opened in this Backend session",
                ));
            }
            if binding.terminal.is_some() {
                return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
            }
            binding.terminal = Some(BackendProducerBindingTerminal::Failed);
        }
        self.mark_impossible(install.coverage_witness());
        self.record_channel_event(|channel| BackendRuntimeFilterEvent::ChannelUnavailable {
            channel,
            reason: UnavailableReason::ProducerFailed,
        });
        self.publish_frontend_feedback(BackendFrontendFeedbackOutcome::ProducerUnavailable);
        for consumer in self.consumers.values() {
            for route in &consumer.routes {
                consumer
                    .subscriptions
                    .publish(
                        *route,
                        SnapshotAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
                        None,
                    )
                    .map_err(subscription_violation)?;
            }
        }
        Ok(RuntimeFilterSubmitOutcome::CompletedWithoutArtifact)
    }

    /// Injects an immutable artifact-owner result for its authorized consumer
    /// route. It accepts neither Arrow values nor row/scan evaluator facts.
    pub fn publish_materialized(
        &self,
        route_edge_id: BackendRouteEdgeId,
        outcome: SnapshotAcquireOutcome,
        terminal: Option<LiveTerminal>,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let consumer = self
            .consumers
            .values()
            .find(|consumer| consumer.routes.contains(&route_edge_id))
            .ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "materialized artifact route is not installed in this Backend session",
                )
            })?;
        consumer
            .subscriptions
            .publish(route_edge_id, outcome.clone(), terminal)
            .map_err(subscription_violation)?;
        match &outcome {
            SnapshotAcquireOutcome::Published(snapshot) => {
                let version = snapshot.logical_version();
                self.record_channel_event(|channel| {
                    BackendRuntimeFilterEvent::LogicalVersionPublished { channel, version }
                });
                if terminal == Some(LiveTerminal::Completed) {
                    self.record_channel_event(|channel| {
                        BackendRuntimeFilterEvent::ChannelCompleted { channel, version }
                    });
                }
            }
            SnapshotAcquireOutcome::Unavailable(reason) => {
                let reason = *reason;
                self.record_channel_event(|channel| {
                    BackendRuntimeFilterEvent::ChannelUnavailable { channel, reason }
                });
            }
            SnapshotAcquireOutcome::Cancelled => self.record_channel_event(|channel| {
                BackendRuntimeFilterEvent::ChannelCancelled { channel }
            }),
            SnapshotAcquireOutcome::Unsupported(_) | SnapshotAcquireOutcome::TimedOut => {}
        }
        Ok(())
    }

    /// Materialize a reduced semantic snapshot into each accepted Backend
    /// profile and publish only an immutable Execution snapshot or a typed
    /// fail-open outcome. No Arrow batch or scan fact crosses this boundary.
    fn publish_reduced_snapshot(
        &self,
        snapshot: &BackendReducedLogicalSnapshot,
        terminal: Option<LiveTerminal>,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if terminal == Some(LiveTerminal::Completed) {
            self.publish_frontend_feedback_for_snapshot(snapshot);
        }
        let logical_version = self.materialized_logical_version(snapshot);
        self.record_channel_event(
            |channel| BackendRuntimeFilterEvent::LogicalVersionPublished {
                channel,
                version: logical_version,
            },
        );
        for consumer in self.consumers.values() {
            let outcome = match (snapshot.domain(), consumer.contract.contract()) {
                (
                    BackendReducedLogicalDomain::Membership(domain),
                    RuntimeFilterExecutionContract::Membership(schema),
                ) => match worker_runtime_filter::materializer::materialize_membership(
                    snapshot.channel_id().get(),
                    domain,
                    schema,
                    logical_version,
                    &consumer.profile,
                    worker_runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    worker_runtime_filter::materializer::MaterializationOutcome::Published(bundle) => {
                        match worker_runtime_filter::artifact_query::BackendRuntimeFilterArtifactQuery::membership(
                            &bundle,
                            schema.data_type().clone(),
                            schema.null_semantics(),
                        ) {
                            Ok(query) => SnapshotAcquireOutcome::Published(Arc::new(
                                RuntimeFilterSnapshot::new(
                                    consumer.contract.binding_id(),
                                    logical_version,
                                    schema.digest(),
                                    Arc::new(query),
                                ),
                            )),
                            Err(_) => SnapshotAcquireOutcome::Unavailable(
                                UnavailableReason::MaterializationFailed,
                            ),
                        }
                    }
                    worker_runtime_filter::materializer::MaterializationOutcome::Unsupported(_) => {
                        SnapshotAcquireOutcome::Unsupported(
                            novarocks_execution::runtime_filter::ArtifactUnsupportedReason::NoAcceptedRepresentation,
                        )
                    }
                    worker_runtime_filter::materializer::MaterializationOutcome::Unavailable(_) => {
                        SnapshotAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed)
                    }
                },
                (
                    BackendReducedLogicalDomain::OrderedBound(bound),
                    RuntimeFilterExecutionContract::Ordered(order),
                ) => match worker_runtime_filter::materializer::range::materialize_range(
                    snapshot.channel_id().get(),
                    order,
                    bound,
                    logical_version,
                    &consumer.profile,
                    &worker_runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    Ok(bundle) => match worker_runtime_filter::artifact_query::BackendRuntimeFilterArtifactQuery::ordered(
                        &bundle,
                        Arc::clone(order),
                    ) {
                        Ok(query) => SnapshotAcquireOutcome::Published(Arc::new(
                            RuntimeFilterSnapshot::new(
                                consumer.contract.binding_id(),
                                logical_version,
                                order.digest(),
                                Arc::new(query),
                            ),
                        )),
                        Err(_) => SnapshotAcquireOutcome::Unavailable(
                            UnavailableReason::MaterializationFailed,
                        ),
                    },
                    Err(_) => SnapshotAcquireOutcome::Unavailable(
                        UnavailableReason::MaterializationFailed,
                    ),
                },
                _ => SnapshotAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed),
            };
            for route in &consumer.routes {
                if !self.owns_outbound_materialization_route(*route) {
                    continue;
                }
                consumer
                    .subscriptions
                    .publish(*route, outcome.clone(), terminal)
                    .map_err(subscription_violation)?;
                if self.owns_outbound_materialization_route(*route) {
                    self.events
                        .record(BackendRuntimeFilterEvent::LoopbackDelivered {
                            channel: BackendChannelIdentity::new(
                                self.participant,
                                consumer.contract.binding_id(),
                                self.channel.channel_id(),
                            ),
                            consumer_binding_id: consumer.contract.binding_id(),
                            route_edge_id: *route,
                            version: logical_version,
                        });
                }
            }
        }
        if terminal == Some(LiveTerminal::Completed) {
            self.record_channel_event(|channel| BackendRuntimeFilterEvent::ChannelCompleted {
                channel,
                version: logical_version,
            });
        }
        self.dispatch_outbound_snapshot(snapshot, terminal)?;
        Ok(())
    }

    fn publish_terminal(
        &self,
        terminal: LiveTerminal,
    ) -> Result<(), RuntimeFilterContractViolation> {
        match terminal {
            LiveTerminal::CompletedWithoutArtifact => {
                self.record_channel_event(|channel| BackendRuntimeFilterEvent::ChannelUnavailable {
                    channel,
                    reason: UnavailableReason::IncompleteCoverage,
                })
            }
            LiveTerminal::Unavailable(reason) => self.record_channel_event(|channel| {
                BackendRuntimeFilterEvent::ChannelUnavailable { channel, reason }
            }),
            LiveTerminal::Cancelled => self.record_channel_event(|channel| {
                BackendRuntimeFilterEvent::ChannelCancelled { channel }
            }),
            LiveTerminal::Completed => {}
        }
        for consumer in self.consumers.values() {
            for route in &consumer.routes {
                if !self.owns_outbound_materialization_route(*route) {
                    continue;
                }
                consumer
                    .subscriptions
                    .publish_terminal(*route, terminal)
                    .map_err(subscription_violation)?;
            }
        }
        self.dispatch_outbound_terminal(terminal)?;
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn record_cancelled_if_open(&self) {
        self.record_channel_event(|channel| BackendRuntimeFilterEvent::ChannelCancelled {
            channel,
        });
    }

    fn record_channel_event(
        &self,
        event: impl Fn(BackendChannelIdentity) -> BackendRuntimeFilterEvent,
    ) {
        for binding_id in self
            .channel
            .producers()
            .keys()
            .chain(self.channel.consumers().keys())
        {
            self.events.record(event(BackendChannelIdentity::new(
                self.participant,
                *binding_id,
                self.channel.channel_id(),
            )));
        }
    }

    fn owns_outbound_materialization_route(&self, route: BackendRouteEdgeId) -> bool {
        self.channel
            .outbound_materialization_groups()
            .values()
            .any(|group| group.route_edge_ids().contains(&route))
    }

    fn materialized_logical_version(
        &self,
        snapshot: &BackendReducedLogicalSnapshot,
    ) -> LogicalVersion {
        match self.channel.lifecycle() {
            super::BackendChannelLifecycle::CompleteOnce => LogicalVersion::FIRST,
            super::BackendChannelLifecycle::MonotonicUpdates => snapshot.logical_version(),
        }
    }

    /// Attempts exactly one terminal feedback publication.  The receiver is
    /// deliberately weak and `try_publish` is nonblocking: losing this frame
    /// widens the Frontend's pruning decision but cannot affect query output.
    fn publish_frontend_feedback(&self, outcome: BackendFrontendFeedbackOutcome) {
        let Some(publication) = self.channel.frontend_feedback_publication() else {
            return;
        };
        if self
            .frontend_feedback_published
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let sink = self
            .frontend_feedback_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(sink) = sink {
            sink.try_publish(
                self.channel.channel_id(),
                self.participant.deployment_epoch(),
                publication,
                outcome,
            );
        }
    }

    fn publish_frontend_feedback_for_snapshot(&self, snapshot: &BackendReducedLogicalSnapshot) {
        let Some(publication) = self.channel.frontend_feedback_publication() else {
            return;
        };
        let outcome = match snapshot.domain() {
            BackendReducedLogicalDomain::Membership(domain) => {
                match novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomain::project(
                    domain,
                    publication.max_encoded_domain_bytes(),
                )
                .and_then(|domain| domain.encode(publication.max_encoded_domain_bytes()))
                {
                    Ok(encoded) => {
                        BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(encoded))
                    }
                    Err(
                        novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomainError::ResourceLimit,
                    ) => BackendFrontendFeedbackOutcome::DomainBudget,
                    Err(_) => BackendFrontendFeedbackOutcome::TypeUnsupported,
                }
            }
            BackendReducedLogicalDomain::OrderedBound(_) => {
                BackendFrontendFeedbackOutcome::TypeUnsupported
            }
        };
        self.publish_frontend_feedback(outcome);
    }

    fn dispatch_outbound_snapshot(
        &self,
        snapshot: &BackendReducedLogicalSnapshot,
        terminal: Option<LiveTerminal>,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let Some(sink) = self
            .materialized_delivery_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        else {
            return Ok(());
        };
        let logical_version = self.materialized_logical_version(snapshot);
        for group in self.channel.outbound_materialization_groups().values() {
            let frame = match (snapshot.domain(), self.channel.execution_contract()) {
                (
                    BackendReducedLogicalDomain::Membership(domain),
                    RuntimeFilterExecutionContract::Membership(schema),
                ) => match worker_runtime_filter::materializer::materialize_membership(
                    snapshot.channel_id().get(),
                    domain,
                    schema,
                    logical_version,
                    group.profile(),
                    worker_runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    worker_runtime_filter::materializer::MaterializationOutcome::Published(bundle) => {
                        worker_runtime_filter::codec::artifact::encode_artifact_bundle(
                            &bundle,
                            worker_runtime_filter::codec::artifact::ArtifactDecodeExpectation {
                                profile: group.profile(),
                                schema,
                                order_contract: None,
                            },
                            worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                                self.channel.max_artifact_bytes(),
                            )
                            .map_err(|error| materialization_violation(error.to_string()))?,
                        )
                        .map_err(|error| materialization_violation(error.to_string()))?
                    }
                    worker_runtime_filter::materializer::MaterializationOutcome::Unsupported(_) => {
                        worker_runtime_filter::codec::artifact::encode_unavailable(
                            UnavailableReason::MaterializationFailed,
                            group.profile(),
                            worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                                self.channel.max_artifact_bytes(),
                            )
                            .map_err(|error| materialization_violation(error.to_string()))?,
                        )
                        .map_err(|error| materialization_violation(error.to_string()))?
                    }
                    worker_runtime_filter::materializer::MaterializationOutcome::Unavailable(_) => {
                        worker_runtime_filter::codec::artifact::encode_unavailable(
                            UnavailableReason::MaterializationFailed,
                            group.profile(),
                            worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                                self.channel.max_artifact_bytes(),
                            )
                            .map_err(|error| materialization_violation(error.to_string()))?,
                        )
                        .map_err(|error| materialization_violation(error.to_string()))?
                    }
                },
                (
                    BackendReducedLogicalDomain::OrderedBound(bound),
                    RuntimeFilterExecutionContract::Ordered(order),
                ) => match worker_runtime_filter::materializer::range::materialize_range(
                    snapshot.channel_id().get(),
                    order,
                    bound,
                    logical_version,
                    group.profile(),
                    &worker_runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    Ok(bundle) => {
                        let placeholder = placeholder_membership_schema()?;
                        worker_runtime_filter::codec::artifact::encode_artifact_bundle(
                            &bundle,
                            worker_runtime_filter::codec::artifact::ArtifactDecodeExpectation {
                                profile: group.profile(),
                                schema: &placeholder,
                                order_contract: Some(order),
                            },
                            worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                                self.channel.max_artifact_bytes(),
                            )
                            .map_err(|error| materialization_violation(error.to_string()))?,
                        )
                        .map_err(|error| materialization_violation(error.to_string()))?
                    }
                    Err(_) => worker_runtime_filter::codec::artifact::encode_unavailable(
                        UnavailableReason::MaterializationFailed,
                        group.profile(),
                        worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                            self.channel.max_artifact_bytes(),
                        )
                        .map_err(|error| materialization_violation(error.to_string()))?,
                    )
                    .map_err(|error| materialization_violation(error.to_string()))?,
                },
                _ => worker_runtime_filter::codec::artifact::encode_unavailable(
                    UnavailableReason::MaterializationFailed,
                    group.profile(),
                    worker_runtime_filter::codec::artifact::max_encoded_len_for_artifact_budget(
                        self.channel.max_artifact_bytes(),
                    )
                    .map_err(|error| materialization_violation(error.to_string()))?,
                )
                .map_err(|error| materialization_violation(error.to_string()))?,
            };
            let is_artifact_bundle = frame.payload().get(6) == Some(&1);
            let kind = if is_artifact_bundle && terminal == Some(LiveTerminal::Completed) {
                super::BackendEnvelopeKind::FinalArtifact
            } else if is_artifact_bundle {
                super::BackendEnvelopeKind::Artifact
            } else {
                super::BackendEnvelopeKind::Unavailable
            };
            sink.dispatch(BackendMaterializedDelivery::new(
                snapshot.channel_id(),
                group.route_edge_ids().iter().copied().collect::<Vec<_>>(),
                kind,
                *frame.profile_digest(),
                Arc::<[u8]>::from(frame.payload()),
            ))?;
        }
        Ok(())
    }

    fn dispatch_outbound_terminal(
        &self,
        terminal: LiveTerminal,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if terminal != LiveTerminal::CompletedWithoutArtifact {
            return Ok(());
        }
        let Some(sink) = self
            .materialized_delivery_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        else {
            return Ok(());
        };
        for group in self.channel.outbound_materialization_groups().values() {
            sink.dispatch(BackendMaterializedDelivery::new(
                self.channel.channel_id(),
                group.route_edge_ids().iter().copied().collect::<Vec<_>>(),
                super::BackendEnvelopeKind::CompletedWithoutArtifact,
                group.profile().id().bytes(),
                Arc::<[u8]>::from([]),
            ))?;
        }
        Ok(())
    }

    fn register_producer(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        if request.contract() != install.contract() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "producer execution contract does not match the installed Backend binding",
            ));
        }
        if request.local_partition_count() == 0 {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::InvalidPartitionCount,
                "producer requires a non-zero local partition count",
            ));
        }
        if request.local_partition_count() > MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::InvalidPartitionCount,
                "producer local partition count exceeds the Backend observation bound",
            ));
        }
        if !install
            .expected_fragment_instances()
            .contains(&fragment_instance_id)
        {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer fragment instance is not authorized by the Backend installation",
            ));
        }
        let mut progress = self
            .producer_progress
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let binding = progress.entry(binding_id).or_default();
        if binding.terminal.is_some() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::SessionClosed,
                "producer binding is already terminal",
            ));
        }
        if let Some(existing) = binding.instances.get(&fragment_instance_id) {
            if existing.partition_count == request.local_partition_count() {
                return Ok(());
            }
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "producer instance was reopened with a different local partition count",
            ));
        }
        binding.instances.insert(
            fragment_instance_id,
            BackendOpenedProducer::new(request.local_partition_count()),
        );
        Ok(())
    }

    fn open_stream(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
    ) -> Result<BackendProducerStreamIdentity, RuntimeFilterContractViolation> {
        self.producer_install(binding_id)?;
        let progress = self
            .producer_progress
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let binding = progress.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding has not been opened in this Backend session",
            )
        })?;
        if binding.terminal.is_some() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::SessionClosed,
                "producer binding is already terminal",
            ));
        }
        let instance = binding
            .instances
            .get(&fragment_instance_id)
            .ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer fragment instance has not been opened in this Backend session",
                )
            })?;
        validate_open_partition(instance, partition)?;
        Ok(BackendProducerStreamIdentity::new(
            BackendChannelIdentity::new(self.participant, binding_id, self.channel.channel_id()),
            fragment_instance_id,
            partition,
        ))
    }

    fn producer_install(
        &self,
        binding_id: RuntimeFilterBindingId,
    ) -> Result<&BackendProducerInstall, RuntimeFilterContractViolation> {
        self.producers.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed in this Backend channel session",
            )
        })
    }

    fn mark_satisfied(&self, witness: super::BackendCoverageWitnessId) {
        if let Some(availability) = &self.availability {
            availability
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .mark_satisfied(witness);
        }
        if let Some(terminal) = &self.terminal {
            terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .mark_satisfied(witness);
        }
    }

    fn mark_impossible(&self, witness: super::BackendCoverageWitnessId) {
        if let Some(availability) = &self.availability {
            availability
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .mark_impossible(witness);
        }
        if let Some(terminal) = &self.terminal {
            terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .mark_impossible(witness);
        }
    }
}

struct BackendRuntimeFilterProducer {
    session: Arc<BackendRuntimeFilterSession>,
    binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
}

impl RuntimeFilterProducer for BackendRuntimeFilterProducer {
    fn max_contribution_bytes(&self) -> usize {
        self.session.policy().max_contribution_bytes()
    }

    fn submit(
        &self,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session
            .submit(
                self.binding_id,
                self.fragment_instance_id,
                partition,
                sequence,
                contribution,
            )
            .map(|submission| submission.outcome())
    }

    fn close_partition(
        &self,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session.close_partition(
            self.binding_id,
            self.fragment_instance_id,
            partition,
            terminal,
        )
    }

    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session
            .fail(self.binding_id, self.fragment_instance_id, reason)
    }
}

fn validate_producer_shape(
    channel: &BackendChannelInstall,
    expected: &BackendProducerInstall,
    producer: &BackendProducerInstall,
) -> Result<(), BackendRuntimeFilterSessionError> {
    if producer.contract().channel_id() != channel.channel_id()
        || producer.contract().contract() != channel.execution_contract()
        || producer.contract().kind() != expected.contract().kind()
        || producer.contract().reduction() != expected.contract().reduction()
        || producer.contract().completion() != expected.contract().completion()
        || producer.max_contribution_bytes() != expected.max_contribution_bytes()
    {
        return Err(BackendRuntimeFilterSessionError::ProducerShapeMismatch);
    }
    Ok(())
}

fn validate_consumer_shape(
    channel: &BackendChannelInstall,
    producer: &BackendProducerInstall,
    consumer: &BackendConsumerInstall,
) -> Result<(), BackendRuntimeFilterSessionError> {
    if consumer.contract().channel_id() != channel.channel_id()
        || consumer.contract().contract() != channel.execution_contract()
        || consumer.contract().reduction() != producer.contract().reduction()
    {
        return Err(BackendRuntimeFilterSessionError::ConsumerContractMismatch);
    }
    Ok(())
}

fn validate_consumer_only_shape(
    channel: &BackendChannelInstall,
    consumer: &BackendConsumerInstall,
) -> Result<(), BackendRuntimeFilterSessionError> {
    if consumer.contract().channel_id() != channel.channel_id()
        || consumer.contract().contract() != channel.execution_contract()
    {
        return Err(BackendRuntimeFilterSessionError::ConsumerContractMismatch);
    }
    Ok(())
}

fn validate_open_partition(
    producer: &BackendOpenedProducer,
    partition: PartitionId,
) -> Result<(), RuntimeFilterContractViolation> {
    if !producer.contains_partition(partition) {
        return Err(contract_violation(
            RuntimeFilterContractViolationKind::InvalidPartitionCount,
            "producer partition is outside its opened local partition count",
        ));
    }
    if producer.terminal_partitions.contains_key(&partition) {
        return Err(contract_violation(
            RuntimeFilterContractViolationKind::SessionClosed,
            "producer partition is already terminal",
        ));
    }
    Ok(())
}

fn reduction_violation(error: BackendReductionStateError) -> RuntimeFilterContractViolation {
    let kind = match &error {
        BackendReductionStateError::Install(BackendInstallPolicyError::ContributionTooLarge)
        | BackendReductionStateError::Reducer(super::ReducerError::SizeOverflow) => {
            RuntimeFilterContractViolationKind::ResourceLimit
        }
        _ => RuntimeFilterContractViolationKind::ContractMismatch,
    };
    contract_violation(
        kind,
        format!("Backend reduction rejected the Execution contribution: {error:?}"),
    )
}

fn subscription_violation(error: BackendSubscriptionError) -> RuntimeFilterContractViolation {
    contract_violation(
        RuntimeFilterContractViolationKind::UnauthorizedBinding,
        format!("Backend subscription publication was not installed: {error:?}"),
    )
}

fn materialization_violation(detail: impl Into<Arc<str>>) -> RuntimeFilterContractViolation {
    contract_violation(
        RuntimeFilterContractViolationKind::ContractMismatch,
        format!(
            "Backend materialization could not encode an outbound artifact: {}",
            detail.into()
        ),
    )
}

fn placeholder_membership_schema() -> Result<
    novarocks_execution::runtime_filter::RuntimeFilterMembershipSchema,
    RuntimeFilterContractViolation,
> {
    novarocks_execution::runtime_filter::RuntimeFilterMembershipSchema::new(
        &DataType::Int8,
        novarocks_execution::runtime_filter::RuntimeFilterNullSemantics::NeverMatches,
    )
    .map_err(|error| materialization_violation(error.to_string()))
}

fn contract_violation(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<Arc<str>>,
) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::thread;
    use std::time::Duration;

    use novarocks_execution::runtime_filter::{
        RuntimeFilterChannelId, RuntimeFilterConsumerContract, RuntimeFilterLateApplyGranularity,
        RuntimeFilterSubscriptionRequest,
    };

    use super::*;
    use crate::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use crate::runtime_filter::{
        domain::{
            BackendChannelLifecycle, BackendCoverage, BackendCoverageWitnessId,
            BackendMaterializationPolicy, BackendProducerInstall,
            CollectingBackendRuntimeFilterEventObserver,
        },
        fixture::BackendRuntimeFilterFixture,
    };

    fn instance(raw: i64) -> UniqueId {
        UniqueId::new(raw, raw + 1)
    }

    fn channel_with_lifecycle(
        lifecycle: BackendChannelLifecycle,
    ) -> (BackendChannelInstall, BackendRuntimeFilterFixture) {
        let fixture = BackendRuntimeFilterFixture::membership();
        let schema = fixture.producer_contract().contract().clone();
        let first = BackendProducerInstall::new(
            fixture.producer_contract(),
            BackendCoverageWitnessId::new(29),
            [instance(37), instance(41)],
            1024,
        )
        .unwrap();
        let second_contract =
            novarocks_execution::runtime_filter::RuntimeFilterProducerContract::membership(
                RuntimeFilterBindingId::new(8),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap();
        let second = BackendProducerInstall::new(
            second_contract,
            BackendCoverageWitnessId::new(31),
            [instance(43)],
            1024,
        )
        .unwrap();
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let blocking = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_blocking(
                RuntimeFilterBindingId::new(70),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap(),
            profile.clone(),
            [BackendRouteEdgeId::new(101)],
            [instance(51)],
        )
        .unwrap();
        let live = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_live(
                RuntimeFilterBindingId::new(71),
                RuntimeFilterChannelId::new(11),
                RuntimeFilterLateApplyGranularity::Row,
                schema.clone(),
            )
            .unwrap(),
            profile,
            [BackendRouteEdgeId::new(102)],
            [instance(53)],
        )
        .unwrap();
        let coverage = BackendCoverage::all_of([
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(BackendCoverageWitnessId::new(31)),
        ])
        .unwrap();
        let channel = BackendChannelInstall::new(
            RuntimeFilterChannelId::new(11),
            schema,
            lifecycle,
            coverage.clone(),
            coverage,
            BackendMaterializationPolicy::new(8, 3, 5, 1, 1024, 1024, 1).unwrap(),
            1024,
            1024,
            [first, second],
            [blocking, live],
            [],
        )
        .unwrap();
        (channel, fixture)
    }

    fn channel() -> (BackendChannelInstall, BackendRuntimeFilterFixture) {
        channel_with_lifecycle(BackendChannelLifecycle::CompleteOnce)
    }

    fn session() -> (
        Arc<BackendRuntimeFilterSession>,
        BackendRuntimeFilterFixture,
    ) {
        let (channel, fixture) = channel();
        (
            Arc::new(
                BackendRuntimeFilterSession::from_channel_install(
                    fixture.identity(),
                    channel,
                    Arc::new(CollectingBackendRuntimeFilterEventObserver::default()),
                )
                .unwrap(),
            ),
            fixture,
        )
    }

    #[test]
    fn consumer_only_channel_installs_and_accepts_remote_publication() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let schema = fixture.producer_contract().contract().clone();
        let consumer = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_blocking(
                RuntimeFilterBindingId::new(70),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap(),
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap(),
            [BackendRouteEdgeId::new(101)],
            [instance(51)],
        )
        .unwrap();
        let channel = BackendChannelInstall::new(
            RuntimeFilterChannelId::new(11),
            schema.clone(),
            BackendChannelLifecycle::CompleteOnce,
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendMaterializationPolicy::new(8, 3, 5, 1, 1024, 1024, 1).unwrap(),
            1024,
            1024,
            [],
            [consumer],
            [],
        )
        .unwrap();
        let session = BackendRuntimeFilterSession::from_channel_install(
            fixture.identity(),
            channel,
            Arc::new(CollectingBackendRuntimeFilterEventObserver::default()),
        )
        .expect("consumer-only channel is a valid remote artifact endpoint");
        let binding = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            schema,
        )
        .unwrap();
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(handle)) =
            session
                .subscribe(instance(51), RuntimeFilterSubscriptionRequest::new(binding))
                .unwrap()
        else {
            panic!("installed consumer-only binding must subscribe")
        };
        session
            .publish_materialized(
                BackendRouteEdgeId::new(101),
                SnapshotAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
                None,
            )
            .unwrap();
        assert!(matches!(
            handle.try_outcome(),
            Some(SnapshotAcquireOutcome::Unavailable(
                UnavailableReason::ProducerFailed
            ))
        ));
    }

    #[test]
    fn consumer_subscription_rejects_an_uninstalled_binding_at_bind_time() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let schema = fixture.producer_contract().contract().clone();
        let consumer = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_blocking(
                RuntimeFilterBindingId::new(70),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap(),
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap(),
            [BackendRouteEdgeId::new(101)],
            [instance(51)],
        )
        .unwrap();
        let channel = BackendChannelInstall::new(
            RuntimeFilterChannelId::new(11),
            schema.clone(),
            BackendChannelLifecycle::CompleteOnce,
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendMaterializationPolicy::new(8, 3, 5, 1, 1024, 1024, 1).unwrap(),
            1024,
            1024,
            [],
            [consumer],
            [],
        )
        .unwrap();
        let session = BackendRuntimeFilterSession::from_channel_install(
            fixture.identity(),
            channel,
            Arc::new(CollectingBackendRuntimeFilterEventObserver::default()),
        )
        .unwrap();
        let error = match session.subscribe(
            instance(51),
            RuntimeFilterSubscriptionRequest::new(
                RuntimeFilterConsumerContract::membership_blocking(
                    RuntimeFilterBindingId::new(71),
                    RuntimeFilterChannelId::new(11),
                    schema,
                )
                .unwrap(),
            ),
        ) {
            Ok(_) => panic!("uninstalled binding must fail before execution starts"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::UnauthorizedBinding
        );
    }

    #[test]
    fn consumer_subscription_rejects_an_unexpected_fragment_instance() {
        let (session, _) = session();
        let binding = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();

        let error =
            match session.subscribe(instance(52), RuntimeFilterSubscriptionRequest::new(binding)) {
                Ok(_) => panic!("an unsealed consumer fragment instance must be rejected"),
                Err(error) => error,
            };

        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::UnauthorizedBinding
        );
        assert!(error.detail().contains("fragment instance"));
    }

    #[test]
    fn binding_witness_waits_for_every_expected_instance_and_partition() {
        let (session, fixture) = session();
        let RuntimeFilterBindOutcome::Bound(first) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };
        first
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                fixture.membership_contribution(),
            )
            .unwrap();
        assert_eq!(
            first
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            RuntimeFilterSubmitOutcome::CoverageStillPossible
        );
        let RuntimeFilterBindOutcome::Bound(second_instance) = session
            .open_producer(
                instance(41),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("second expected producer instance must bind")
        };
        second_instance
            .close_partition(PartitionId::new(0), ProducerSequence::new(0))
            .unwrap();
        assert_eq!(
            session.availability_progress(),
            BackendCoverageProgress::Pending
        );
        let second_binding =
            novarocks_execution::runtime_filter::RuntimeFilterProducerContract::membership(
                RuntimeFilterBindingId::new(8),
                RuntimeFilterChannelId::new(11),
                fixture.producer_contract().contract().clone(),
            )
            .unwrap();
        let RuntimeFilterBindOutcome::Bound(second_binding_producer) = session
            .open_producer(
                instance(43),
                RuntimeFilterProducerOpenRequest::new(second_binding, 1),
            )
            .unwrap()
        else {
            panic!("second installed producer binding must bind")
        };
        assert_eq!(
            second_binding_producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(0))
                .unwrap(),
            RuntimeFilterSubmitOutcome::PendingFinalSnapshot
        );
        assert_eq!(
            session.availability_progress(),
            BackendCoverageProgress::Satisfied
        );
    }

    #[test]
    fn complete_once_materialization_uses_first_version_after_multiple_reductions() {
        let (session, fixture) = session();
        let producer_contract = fixture.producer_contract();
        let binding_id = producer_contract.binding_id();
        let RuntimeFilterBindOutcome::Bound(_) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(producer_contract, 1),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };
        let first = session
            .submit(
                binding_id,
                instance(37),
                PartitionId::new(0),
                ProducerSequence::new(0),
                fixture.membership_contribution_with_values([3, 9]),
            )
            .unwrap()
            .publication
            .expect("first reduction publishes internally");
        let second = session
            .submit(
                binding_id,
                instance(37),
                PartitionId::new(0),
                ProducerSequence::new(1),
                fixture.membership_contribution_with_values([12, 18]),
            )
            .unwrap()
            .publication
            .expect("second reduction publishes internally");

        assert_eq!(first.logical_version(), LogicalVersion::FIRST);
        assert_eq!(second.logical_version(), LogicalVersion::new(2));
        assert_eq!(
            session.materialized_logical_version(&second),
            LogicalVersion::FIRST
        );
    }

    struct BlockingFirstPublicationObserver {
        blocked: AtomicBool,
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl BackendRuntimeFilterEventObserver for BlockingFirstPublicationObserver {
        fn record(&self, event: BackendRuntimeFilterEvent) {
            if matches!(
                event,
                BackendRuntimeFilterEvent::LogicalVersionPublished {
                    version: LogicalVersion::FIRST,
                    ..
                }
            ) && !self.blocked.swap(true, Ordering::AcqRel)
            {
                self.entered
                    .send(())
                    .expect("publication test must observe the first version");
                self.release
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv()
                    .expect("publication test must release the first version");
            }
        }
    }

    #[test]
    fn monotonic_reduction_and_publication_share_one_ordered_critical_section() {
        let (channel, fixture) = channel_with_lifecycle(BackendChannelLifecycle::MonotonicUpdates);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let observer = Arc::new(BlockingFirstPublicationObserver {
            blocked: AtomicBool::new(false),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                fixture.identity(),
                channel,
                observer,
            )
            .unwrap(),
        );
        let RuntimeFilterBindOutcome::Bound(producer) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 2),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };

        let first_producer = Arc::clone(&producer);
        let first_contribution = fixture.membership_contribution_with_values([3]);
        let first = thread::spawn(move || {
            first_producer.submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                first_contribution,
            )
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first publication must reach the controlled observer");

        let second_producer = Arc::clone(&producer);
        let second_contribution = fixture.membership_contribution_with_values([9]);
        let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
        let second = thread::spawn(move || {
            let result = second_producer.submit(
                PartitionId::new(1),
                ProducerSequence::new(0),
                second_contribution,
            );
            second_done_tx
                .send(())
                .expect("test receiver must remain alive");
            result
        });

        let published_out_of_order = second_done_rx
            .recv_timeout(Duration::from_millis(50))
            .is_ok();
        release_tx
            .send(())
            .expect("blocked publication must still be waiting");
        first
            .join()
            .expect("first producer thread must join")
            .unwrap();
        second
            .join()
            .expect("second producer thread must join")
            .unwrap();
        assert!(
            !published_out_of_order,
            "v2 publication must not overtake a blocked v1 publication"
        );
    }

    #[test]
    fn producer_without_materialization_ownership_does_not_publish_local_snapshot() {
        let (session, fixture) = session();
        let consumer_contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
            subscription,
        )) = session
            .subscribe(
                instance(51),
                RuntimeFilterSubscriptionRequest::new(consumer_contract),
            )
            .unwrap()
        else {
            panic!("installed blocking consumer must bind")
        };

        for producer_instance in [instance(37), instance(41)] {
            let RuntimeFilterBindOutcome::Bound(producer) = session
                .open_producer(
                    producer_instance,
                    RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
                )
                .unwrap()
            else {
                panic!("installed producer must bind")
            };
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    fixture.membership_contribution(),
                )
                .unwrap();
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap();
        }
        let second_contract =
            novarocks_execution::runtime_filter::RuntimeFilterProducerContract::membership(
                RuntimeFilterBindingId::new(8),
                RuntimeFilterChannelId::new(11),
                session.channel().execution_contract().clone(),
            )
            .unwrap();
        let RuntimeFilterBindOutcome::Bound(second) = session
            .open_producer(
                instance(43),
                RuntimeFilterProducerOpenRequest::new(second_contract, 1),
            )
            .unwrap()
        else {
            panic!("second installed producer binding must bind")
        };
        second
            .close_partition(PartitionId::new(0), ProducerSequence::new(0))
            .unwrap();

        assert!(
            subscription.try_outcome().is_none(),
            "no snapshot is published while a producer route stays open"
        );
    }

    #[test]
    fn multiple_consumer_bindings_resolve_their_own_blocking_and_live_slots() {
        let (session, _) = session();
        let blocking = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        let live = RuntimeFilterConsumerContract::membership_live(
            RuntimeFilterBindingId::new(71),
            RuntimeFilterChannelId::new(11),
            RuntimeFilterLateApplyGranularity::Row,
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        assert!(matches!(
            session
                .subscribe(
                    instance(51),
                    RuntimeFilterSubscriptionRequest::new(blocking)
                )
                .unwrap(),
            RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(_))
        ));
        assert!(matches!(
            session
                .subscribe(instance(53), RuntimeFilterSubscriptionRequest::new(live))
                .unwrap(),
            RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Live(_))
        ));
    }

    #[test]
    fn producer_failure_fail_opens_every_consumer_binding() {
        let (session, fixture) = session();
        let RuntimeFilterBindOutcome::Bound(producer) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };
        let blocking = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
            subscription,
        )) = session
            .subscribe(
                instance(51),
                RuntimeFilterSubscriptionRequest::new(blocking),
            )
            .unwrap()
        else {
            panic!("installed blocking consumer must bind")
        };
        producer
            .fail(RuntimeFilterProducerFailure::ExecutionFailed)
            .unwrap();
        assert!(matches!(
            subscription.try_outcome(),
            Some(SnapshotAcquireOutcome::Unavailable(
                UnavailableReason::ProducerFailed
            ))
        ));
    }
}
