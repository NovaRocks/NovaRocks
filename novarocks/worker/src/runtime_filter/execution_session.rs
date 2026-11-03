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

//! Worker-owned fragment runtime-filter session binding.
//!
//! Native delivery is an outer adapter concern. This module binds one local
//! fragment to already-installed Worker sessions and reports outbound facts
//! through a narrow port without importing an envelope, RPC, or Backend host.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_execution::runtime_filter::{
    PartitionId, ProducerSequence, RuntimeFilterBindOutcome, RuntimeFilterBindingId,
    RuntimeFilterChannelId, RuntimeFilterContractViolation, RuntimeFilterContractViolationKind,
    RuntimeFilterContribution, RuntimeFilterExecutionContract, RuntimeFilterFinalDomainOpenRequest,
    RuntimeFilterProducer, RuntimeFilterProducerFailure, RuntimeFilterProducerOpenRequest,
    RuntimeFilterSession, RuntimeFilterSubmitOutcome, RuntimeFilterSubscriptionPin,
    RuntimeFilterSubscriptionRequest, SharedRuntimeFilterFinalDomainCompletion,
    SharedRuntimeFilterProducer, UnavailableReason,
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendParticipantIdentity, BackendProducerStreamIdentity,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendRuntimeFilterSession,
};
use super::final_domain::WorkerRuntimeFilterFinalDomainCompletion;
use super::observation::RuntimeFilterObservationEmitter;

/// Outer delivery port for locally produced runtime-filter facts.
///
/// The Worker owns the session, contribution ordering, and observation. The
/// Backend adapter alone turns the frozen values into a Native envelope and
/// chooses the transport implementation.
pub trait RuntimeFilterParticipantOutbound: Send + Sync {
    fn forward_producer_contribution(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        local_partition_count: u32,
        contribution: RuntimeFilterContribution,
    ) -> Result<(), RuntimeFilterContractViolation>;

    fn forward_producer_close(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        local_partition_count: u32,
    ) -> Result<(), RuntimeFilterContractViolation>;

    fn forward_producer_failure(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<(), RuntimeFilterContractViolation>;
}

/// One fragment's view of an installed Worker runtime-filter participant.
pub struct WorkerRuntimeFilterExecutionSession {
    task_key: UniqueId,
    participant: BackendParticipantIdentity,
    producers: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    consumers: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    outbound: Arc<dyn RuntimeFilterParticipantOutbound>,
    observation: Arc<RuntimeFilterObservationEmitter>,
    cancelled: Arc<AtomicBool>,
}

impl WorkerRuntimeFilterExecutionSession {
    pub fn new(
        task_key: UniqueId,
        participant: BackendParticipantIdentity,
        producers: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        consumers: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        outbound: Arc<dyn RuntimeFilterParticipantOutbound>,
        observation: Arc<RuntimeFilterObservationEmitter>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            task_key,
            participant,
            producers,
            consumers,
            outbound,
            observation,
            cancelled,
        }
    }
}

impl RuntimeFilterSession for WorkerRuntimeFilterExecutionSession {
    fn open_producer(
        &self,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<RuntimeFilterBindOutcome<SharedRuntimeFilterProducer>, RuntimeFilterContractViolation>
    {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            ));
        }
        let binding_id = request.contract().binding_id();
        let session = self.producers.get(&binding_id).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed for this Backend fragment",
            )
        })?;
        let local_partition_count = request.local_partition_count();
        match session.open_producer(self.task_key, request)? {
            RuntimeFilterBindOutcome::Bound(local) => {
                self.observation.register_producer_instance(
                    BackendChannelIdentity::new(
                        self.participant,
                        binding_id,
                        session.channel().channel_id(),
                    ),
                    self.task_key,
                    local_partition_count,
                );
                Ok(RuntimeFilterBindOutcome::Bound(Arc::new(
                    WorkerRuntimeFilterProducer {
                        local,
                        outbound: Arc::clone(&self.outbound),
                        participant: self.participant,
                        binding_id,
                        channel_id: session.channel().channel_id(),
                        task_key: self.task_key,
                        local_partition_count,
                        observation: Arc::clone(&self.observation),
                    },
                )))
            }
            RuntimeFilterBindOutcome::Unavailable(reason) => {
                Ok(RuntimeFilterBindOutcome::Unavailable(reason))
            }
        }
    }

    fn subscribe(
        &self,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionPin>,
        RuntimeFilterContractViolation,
    > {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            ));
        }
        let binding_id = request.contract().binding_id();
        let session = self.consumers.get(&binding_id).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed for this Backend fragment",
            )
        })?;
        session.subscribe(self.task_key, request)
    }

    fn open_final_domain_completion(
        &self,
        request: RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<SharedRuntimeFilterFinalDomainCompletion>,
        RuntimeFilterContractViolation,
    > {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            ));
        }
        let contract = request.contract().clone();
        if contract.kind()
            != novarocks_execution::runtime_filter::RuntimeFilterProducerKind::FinalDomain
        {
            return Err(violation(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "final-domain completion request does not carry a FinalDomain producer contract",
            ));
        }
        let RuntimeFilterExecutionContract::Membership(schema) = contract.contract() else {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain completion requires a membership execution contract",
            ));
        };
        let session = self.producers.get(&contract.binding_id()).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "FinalDomain producer binding is not installed for this Backend fragment",
            )
        })?;
        let producer = match self.open_producer(RuntimeFilterProducerOpenRequest::new(
            contract.clone(),
            request.local_partition_count(),
        ))? {
            RuntimeFilterBindOutcome::Bound(producer) => producer,
            RuntimeFilterBindOutcome::Unavailable(reason) => {
                return Ok(RuntimeFilterBindOutcome::Unavailable(reason));
            }
        };
        Ok(RuntimeFilterBindOutcome::Bound(Arc::new(
            WorkerRuntimeFilterFinalDomainCompletion::new(
                producer,
                schema.data_type().clone(),
                schema.digest(),
                session.policy().max_contribution_bytes(),
                request.local_partition_count(),
            ),
        )))
    }
}

struct WorkerRuntimeFilterProducer {
    local: SharedRuntimeFilterProducer,
    outbound: Arc<dyn RuntimeFilterParticipantOutbound>,
    participant: BackendParticipantIdentity,
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    task_key: UniqueId,
    local_partition_count: u32,
    observation: Arc<RuntimeFilterObservationEmitter>,
}

impl RuntimeFilterProducer for WorkerRuntimeFilterProducer {
    fn max_contribution_bytes(&self) -> usize {
        self.local.max_contribution_bytes()
    }

    fn submit(
        &self,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let stream = BackendProducerStreamIdentity::new(
            BackendChannelIdentity::new(self.participant, self.binding_id, self.channel_id),
            self.task_key,
            partition,
        );
        let outcome = match self.local.submit(partition, sequence, contribution.clone()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.observation.record(
                    if error.kind() == RuntimeFilterContractViolationKind::ResourceLimit {
                        BackendRuntimeFilterEvent::ContributionResourceLimitRejected {
                            stream,
                            sequence: sequence.get(),
                        }
                    } else {
                        BackendRuntimeFilterEvent::ContributionConflictRejected {
                            stream,
                            sequence: sequence.get(),
                        }
                    },
                );
                return Err(error);
            }
        };
        self.observation.record(match outcome {
            RuntimeFilterSubmitOutcome::Duplicate => {
                BackendRuntimeFilterEvent::ContributionDuplicateIgnored {
                    stream,
                    sequence: sequence.get(),
                }
            }
            RuntimeFilterSubmitOutcome::Stale => {
                BackendRuntimeFilterEvent::ContributionStaleIgnored {
                    stream,
                    sequence: sequence.get(),
                }
            }
            _ => BackendRuntimeFilterEvent::ContributionAccepted {
                stream,
                sequence: sequence.get(),
            },
        });
        self.outbound.forward_producer_contribution(
            self.channel_id,
            self.binding_id,
            self.task_key,
            partition,
            sequence,
            self.local_partition_count,
            contribution,
        )?;
        Ok(outcome)
    }

    fn close_partition(
        &self,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let outcome = self.local.close_partition(partition, terminal)?;
        self.outbound.forward_producer_close(
            self.channel_id,
            self.binding_id,
            self.task_key,
            partition,
            terminal,
            self.local_partition_count,
        )?;
        Ok(outcome)
    }

    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let outcome = self.local.fail(reason)?;
        self.outbound.forward_producer_failure(
            self.channel_id,
            self.binding_id,
            self.task_key,
            reason,
        )?;
        Ok(outcome)
    }
}

fn violation(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<Arc<str>>,
) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(kind, detail)
}
