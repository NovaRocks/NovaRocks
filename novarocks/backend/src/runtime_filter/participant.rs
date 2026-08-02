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

//! Attempt-scoped Backend runtime-filter participant ownership.
//!
//! The participant is deliberately the only Backend-private wrapper around a
//! concrete Service. Query lifecycle entries hold this type, while fragment
//! admission and gRPC ingress receive only its narrow session/dispatch
//! operations. Neither path can create or recover a participant by query id.

use std::sync::Arc;

use novarocks::query_execution::lifecycle::{
    QueryExecutionId, QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminationReason,
    RuntimeFilterContribution,
};
use novarocks::runtime::mem_tracker::MemTracker;
use novarocks::runtime_filter_transition::port::transport::{
    RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterEnvelopeKind,
    RuntimeFilterIngressResult,
};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::UniqueId;

use super::service::{
    InboundConsumerDispatchOutcome, InboundProducerDispatchOutcome,
    NativeRuntimeFilterExecutionContext, RuntimeFilterService,
};

const QUERY_UNAVAILABLE_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: runtime filter query is not active or in delivery grace";
const ACK_UNSUPPORTED_REJECTION: &str = "runtime filter ingress rejected [ack-unsupported]: runtime filter ack ingress is not supported";

struct BackendRuntimeFilterEventObserver {
    diagnostic_sink:
        Arc<dyn novarocks::runtime_filter_transition::port::events::RuntimeFilterEventSink>,
}

impl novarocks::runtime_filter_transition::port::events::RuntimeFilterEventSink
    for BackendRuntimeFilterEventObserver
{
    fn record(
        &self,
        event: novarocks::runtime_filter_transition::port::events::RuntimeFilterEvent,
    ) {
        // Core's legacy registry is an append-only diagnostic observer. The
        // Backend-owned Service never receives it back as a lookup or control
        // dependency.
        self.diagnostic_sink.record(event);
    }
}

/// Backend-private factory injected into the lifecycle registry. The entry
/// depends on this construction seam rather than on a process-global service
/// lookup, which also lets lifecycle tests exercise install failure and
/// publication races without exposing the concrete Service.
pub(crate) trait RuntimeFilterParticipantFactory: Send + Sync + 'static {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: RuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, QueryLifecycleError>;
}

#[derive(Default)]
pub(crate) struct BackendRuntimeFilterParticipantFactory;

impl RuntimeFilterParticipantFactory for BackendRuntimeFilterParticipantFactory {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: RuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, QueryLifecycleError> {
        let expected_epoch = execution_id.attempt_id().get();
        if contribution.install().epoch().get() != expected_epoch {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "runtime filter install epoch does not match query execution attempt",
            ));
        }
        let query_id = UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        );
        let memory = MemTracker::new_root(format!(
            "runtime_filter_participant_{:x}_{:x}_{}",
            query_id.high(),
            query_id.low(),
            execution_id.attempt_id().get()
        ));
        let diagnostic_sink =
            novarocks::runtime::runtime_filter_observability::backend_participant_event_sink(
                novarocks::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
                    query_id.high(),
                    query_id.low(),
                ),
            );
        let service = Arc::new(RuntimeFilterService::new_for_query(
            query_id,
            Arc::new(BackendRuntimeFilterEventObserver { diagnostic_sink }),
            &memory,
        ));
        if let Err(error) = service.install(contribution.install().clone()) {
            // The entry has not observed this participant yet. Tear the
            // concrete owner down before returning so an Init rejection never
            // leaves an unreachable half-installed service behind.
            service.shutdown();
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                error.to_string(),
            ));
        }
        Ok(Arc::new(RuntimeFilterParticipant {
            execution_id,
            epoch: contribution.install().epoch(),
            service,
            _memory: memory,
            close_hook: Arc::new(|service, _reason| {
                service.shutdown();
                Ok(())
            }),
        }))
    }
}

/// One installed Backend participant for exactly one full query attempt.
pub(crate) struct RuntimeFilterParticipant {
    execution_id: QueryExecutionId,
    epoch: novarocks::runtime_filter_transition::port::identity::DeploymentEpoch,
    service: Arc<RuntimeFilterService>,
    _memory: Arc<MemTracker>,
    close_hook: RuntimeFilterParticipantCloseHook,
}

pub(crate) type RuntimeFilterParticipantCloseHook = Arc<
    dyn Fn(&RuntimeFilterService, QueryTerminationReason) -> Result<(), QueryLifecycleError>
        + Send
        + Sync,
>;

impl RuntimeFilterParticipant {
    pub(crate) fn session_for_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        required: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, QueryLifecycleError> {
        if execution_id != self.execution_id {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "runtime filter participant does not belong to this execution attempt",
            ));
        }
        if !required {
            return Ok(None);
        }
        Ok(Some(Arc::new(NativeRuntimeFilterExecutionContext::new(
            Arc::clone(&self.service),
            UniqueId::new(
                execution_id.query_id().high(),
                execution_id.query_id().low(),
            ),
            self.epoch,
            fragment_instance_id,
        )) as RuntimeFilterSessionRef))
    }

    pub(crate) fn dispatch_envelope(
        &self,
        envelope: RuntimeFilterEnvelope,
    ) -> RuntimeFilterIngressResult {
        let same_query = envelope.query_id()
            == UniqueId::new(
                self.execution_id.query_id().high(),
                self.execution_id.query_id().low(),
            );
        if !same_query || envelope.deployment_epoch() != self.epoch {
            return RuntimeFilterIngressResult::rejected(QUERY_UNAVAILABLE_REJECTION)
                .expect("query-unavailable reason is non-empty");
        }
        match envelope.kind() {
            RuntimeFilterEnvelopeKind::Contribution
            | RuntimeFilterEnvelopeKind::ProducerClosed
            | RuntimeFilterEnvelopeKind::ProducerUnavailable => {
                match self.service.dispatch_inbound_producer(envelope) {
                    Ok(InboundProducerDispatchOutcome::Accepted) => {
                        RuntimeFilterIngressResult::accepted()
                    }
                    Ok(InboundProducerDispatchOutcome::Duplicate) => {
                        RuntimeFilterIngressResult::duplicate()
                    }
                    Err(error) => RuntimeFilterIngressResult::rejected(error.to_string())
                        .expect("typed producer rejection is non-empty"),
                }
            }
            RuntimeFilterEnvelopeKind::Artifact
            | RuntimeFilterEnvelopeKind::FinalArtifact
            | RuntimeFilterEnvelopeKind::Unavailable
            | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
            | RuntimeFilterEnvelopeKind::DegradedLogical => {
                match self.service.dispatch_inbound_consumer(envelope) {
                    Ok(InboundConsumerDispatchOutcome::Accepted) => {
                        RuntimeFilterIngressResult::accepted()
                    }
                    Ok(InboundConsumerDispatchOutcome::Duplicate) => {
                        RuntimeFilterIngressResult::duplicate()
                    }
                    Err(error) => RuntimeFilterIngressResult::rejected(error.to_string())
                        .expect("typed consumer rejection is non-empty"),
                }
            }
            RuntimeFilterEnvelopeKind::Ack => {
                RuntimeFilterIngressResult::rejected(ACK_UNSUPPORTED_REJECTION)
                    .expect("ack-unsupported reason is non-empty")
            }
        }
    }

    pub(crate) fn close(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        (self.close_hook)(&self.service, reason)
    }

    #[cfg(test)]
    pub(crate) fn with_close_hook_for_test(
        &self,
        close_hook: RuntimeFilterParticipantCloseHook,
    ) -> Arc<Self> {
        Arc::new(Self {
            execution_id: self.execution_id,
            epoch: self.epoch,
            service: Arc::clone(&self.service),
            _memory: Arc::clone(&self._memory),
            close_hook,
        })
    }
}

impl RuntimeFilterEnvelopeIngress for RuntimeFilterParticipant {
    fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
        self.dispatch_envelope(envelope)
    }
}
