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

//! Worker-owned process-local runtime-filter participant state.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use novarocks_execution::runtime::mem_tracker::{MemTracker, process_mem_tracker};
use novarocks_execution::runtime_filter::{
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterRowEffect, RuntimeFilterSessionRef,
};
use novarocks_types::UniqueId;

use crate::RuntimeFilterContractError;

use super::domain::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendFrontendFeedbackSink,
    BackendIngressDedupe, BackendIngressResult, BackendMaterializedDeliverySink,
    BackendParticipantInstall, BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver,
    BackendRuntimeFilterSession,
};
use super::execution_session::{
    RuntimeFilterParticipantOutbound, WorkerRuntimeFilterExecutionSession,
};
use super::observation::{RuntimeFilterObservationEmitter, RuntimeFilterObservationSnapshot};
use super::participant_ingress::{
    DeliveryIngressFrame, DeliveryIngressRoute, ProducerIngressCommand, ProducerIngressRoute,
    dispatch_delivery_frame, dispatch_producer_failure, dispatch_producer_frame,
};

const MAX_DELIVERY_IDENTITIES_PER_CHANNEL: usize = 16_384;

/// One sealed runtime-filter participant's local domain state.
///
/// Native adapters retain wire decoding and their transport egress, but cannot
/// own or recreate installed routing, session, observation, dedupe, or
/// cancellation state.
pub struct WorkerRuntimeFilterParticipant {
    install: BackendParticipantInstall,
    observation: Arc<RuntimeFilterObservationEmitter>,
    producer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    consumer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    delivery_dedupe: Arc<BackendIngressDedupe>,
    cancelled: Arc<AtomicBool>,
    _memory: Arc<MemTracker>,
}

impl WorkerRuntimeFilterParticipant {
    /// Builds the complete local state for one already-decoded sealed install.
    ///
    /// The native adapter supplies neither session maps nor lifecycle state:
    /// those are derived once from the Worker-owned installation authority.
    ///
    /// The participant's memory tracker attaches to the process root, because
    /// this seam holds no query tracker. It is never a root of its own: a
    /// disconnected root hides every runtime-filter byte from the process
    /// memory boundary. Use `from_install_under` when the query tracker is
    /// available.
    pub fn from_install(
        install: BackendParticipantInstall,
    ) -> Result<Self, RuntimeFilterContractError> {
        // Fallback parent, deliberately the process root and never a new root.
        //
        // This seam is reached from the Native adapter's participant factory,
        // which carries the sealed install and nothing else; the query tracker
        // lives in the Backend query-context registry, two crates above, and
        // the Worker crate cannot and must not reach back into it. Attaching
        // to the process root keeps the charge inside the one process
        // hierarchy - it loses per-query attribution, not the accounting.
        // `from_install_under` is the attributed form for every owner that
        // does hold the query tracker.
        Self::from_install_under(install, &process_mem_tracker())
    }

    /// Builds the local state and attributes its memory to `memory_parent`.
    ///
    /// `memory_parent` is the caller's query tracker wherever one is held, so
    /// runtime-filter memory counts against that query's limit instead of only
    /// against the process total.
    pub fn from_install_under(
        install: BackendParticipantInstall,
        memory_parent: &Arc<MemTracker>,
    ) -> Result<Self, RuntimeFilterContractError> {
        let participant = install.participant();
        let observation = RuntimeFilterObservationEmitter::from_install(&install, None);
        observation.record(BackendRuntimeFilterEvent::DeploymentInstalled { participant });
        let events: Arc<dyn BackendRuntimeFilterEventObserver> = observation.clone();
        let mut producer_sessions = BTreeMap::new();
        let mut consumer_sessions = BTreeMap::new();
        for channel in install.channels().values() {
            let session = Arc::new(
                BackendRuntimeFilterSession::from_channel_install(
                    participant,
                    channel.clone(),
                    Arc::clone(&events),
                )
                .map_err(|error| RuntimeFilterContractError::invalid_contract(error.to_string()))?,
            );
            for binding_id in channel.producers().keys() {
                if producer_sessions
                    .insert(*binding_id, Arc::clone(&session))
                    .is_some()
                {
                    return Err(RuntimeFilterContractError::invalid_contract(
                        "runtime filter producer binding is installed by multiple channels",
                    ));
                }
            }
            for binding_id in channel.consumers().keys() {
                if consumer_sessions
                    .insert(*binding_id, Arc::clone(&session))
                    .is_some()
                {
                    return Err(RuntimeFilterContractError::invalid_contract(
                        "runtime filter consumer binding is installed by multiple channels",
                    ));
                }
            }
        }
        let query_id = participant.query_id();
        let memory = MemTracker::new_child(
            format!(
                "runtime_filter_participant_{:x}_{:x}_{}",
                query_id.high(),
                query_id.low(),
                participant.deployment_epoch()
            ),
            memory_parent,
        );
        Ok(Self::new(
            install,
            observation,
            producer_sessions,
            consumer_sessions,
            Arc::new(BackendIngressDedupe::new(
                MAX_DELIVERY_IDENTITIES_PER_CHANNEL,
            )),
            Arc::new(AtomicBool::new(false)),
            memory,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        install: BackendParticipantInstall,
        observation: Arc<RuntimeFilterObservationEmitter>,
        producer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        consumer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        delivery_dedupe: Arc<BackendIngressDedupe>,
        cancelled: Arc<AtomicBool>,
        memory: Arc<MemTracker>,
    ) -> Self {
        Self {
            install,
            observation,
            producer_sessions,
            consumer_sessions,
            delivery_dedupe,
            cancelled,
            _memory: memory,
        }
    }

    pub const fn participant(&self) -> super::domain::BackendParticipantIdentity {
        self.install.participant()
    }

    pub fn observation_emitter(&self) -> Arc<RuntimeFilterObservationEmitter> {
        Arc::clone(&self.observation)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn session_for_fragment(
        &self,
        fragment_instance_id: UniqueId,
        outbound: Arc<dyn RuntimeFilterParticipantOutbound>,
    ) -> RuntimeFilterSessionRef {
        Arc::new(WorkerRuntimeFilterExecutionSession::new(
            fragment_instance_id,
            self.install.participant(),
            self.producer_sessions.clone(),
            self.consumer_sessions.clone(),
            outbound,
            Arc::clone(&self.observation),
            Arc::clone(&self.cancelled),
        )) as RuntimeFilterSessionRef
    }

    pub fn dispatch_delivery(
        &self,
        route: DeliveryIngressRoute,
        frame: DeliveryIngressFrame<'_>,
    ) -> BackendIngressResult {
        dispatch_delivery_frame(
            &self.install,
            &self.consumer_sessions,
            &self.delivery_dedupe,
            route,
            frame,
        )
    }

    pub fn dispatch_producer(
        &self,
        route: ProducerIngressRoute,
        command: ProducerIngressCommand,
    ) -> BackendIngressResult {
        dispatch_producer_frame(
            &self.install,
            &self.producer_sessions,
            &self.observation,
            route,
            command,
        )
    }

    pub fn dispatch_producer_failure(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> BackendIngressResult {
        dispatch_producer_failure(
            &self.install,
            &self.producer_sessions,
            channel_id,
            binding_id,
            fragment_instance_id,
        )
    }

    pub fn set_materialized_delivery_sink(&self, sink: Arc<dyn BackendMaterializedDeliverySink>) {
        for session in self.producer_sessions.values() {
            session.set_materialized_delivery_sink(Arc::clone(&sink));
        }
    }

    pub fn close(&self) {
        self.cancelled.store(true, Ordering::Release);
        for session in self.producer_sessions.values() {
            session.clear_frontend_feedback_sink();
        }
    }

    pub fn set_frontend_feedback_sink(&self, sink: Weak<dyn BackendFrontendFeedbackSink>) {
        for session in self.producer_sessions.values() {
            session.set_frontend_feedback_sink(sink.clone());
        }
    }

    pub fn capture_observation(&self) -> RuntimeFilterObservationSnapshot {
        self.observation.capture()
    }

    pub fn prepare_terminal_capture(
        &self,
        coordinator_finalize: bool,
    ) -> RuntimeFilterObservationSnapshot {
        if coordinator_finalize {
            self.observation.seal()
        } else {
            self.observation.cancel_open_channels_and_seal()
        }
    }

    pub fn record_row_effect(
        &self,
        fragment_instance_id: UniqueId,
        effect: RuntimeFilterRowEffect,
    ) {
        let Some(identity) = self.consumer_identity(effect.binding_id(), fragment_instance_id)
        else {
            return;
        };
        self.observation
            .record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity,
                logical_version: effect.logical_version(),
                input_rows: effect.input_rows(),
                output_rows: effect.output_rows(),
            });
    }

    fn consumer_identity(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> Option<BackendConsumerSubscriptionIdentity> {
        let session = self.consumer_sessions.get(&binding_id)?;
        Some(BackendConsumerSubscriptionIdentity::new(
            BackendChannelIdentity::new(
                self.install.participant(),
                binding_id,
                session.channel().channel_id(),
            ),
            binding_id,
            fragment_instance_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::domain::{BackendParticipantIdentity, BackendRoutingShard};

    fn install_for(query_id: UniqueId, deployment_epoch: u64) -> BackendParticipantInstall {
        let participant = BackendParticipantIdentity::new(query_id, deployment_epoch);
        let routing = BackendRoutingShard::new(participant, 1, [])
            .expect("empty routing is sufficient for memory-hierarchy tests");
        BackendParticipantInstall::new(participant, 1, [], routing)
            .expect("channel-less install is a valid sealed install")
    }

    fn expected_label(query_id: UniqueId, deployment_epoch: u64) -> String {
        format!(
            "runtime_filter_participant_{:x}_{:x}_{deployment_epoch}",
            query_id.high(),
            query_id.low()
        )
    }

    #[test]
    fn participant_memory_is_a_child_of_the_supplied_query_tracker() {
        let query_id = UniqueId::new(0x7e57_0001, 0x7e57_0002);
        let query = MemTracker::new_child(
            novarocks_execution::runtime::mem_tracker::query_tracker_label(
                query_id.high(),
                query_id.low(),
            ),
            &process_mem_tracker(),
        );

        let participant =
            WorkerRuntimeFilterParticipant::from_install_under(install_for(query_id, 23), &query)
                .expect("install builds participant state");

        let children = query.children();
        assert_eq!(
            children.len(),
            1,
            "the participant must own exactly one tracker under its query"
        );
        assert_eq!(children[0].label(), expected_label(query_id, 23));

        // A root would keep this charge out of the query subtree entirely.
        children[0].consume(64);
        assert_eq!(query.current(), 64);
        children[0].release(64);
        assert_eq!(query.current(), 0);
        drop(participant);
    }

    #[test]
    fn participant_memory_falls_back_to_the_process_root_never_to_a_new_root() {
        let query_id = UniqueId::new(0x7e57_0003, 0x7e57_0004);
        let label = expected_label(query_id, 29);

        let participant = WorkerRuntimeFilterParticipant::from_install(install_for(query_id, 29))
            .expect("install builds participant state");

        let matches: Vec<_> = process_mem_tracker()
            .children()
            .into_iter()
            .filter(|child| child.label() == label)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "the fallback must attach to the process root, not mint a second root"
        );
        drop(participant);
    }
}
