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

//! Native encoding of the Worker dynamic-filter feedback loop.
//!
//! A runtime-filter channel that reduces to a terminal logical domain has one
//! thing left to do with it: tell the frontend, so the frontend's connector
//! split sources can prune. On the old lifecycle that went out as a control
//! stream event. Here it goes onto the task substrate: the domain is retained
//! by a task's status owner, which advertises its version immediately, and the
//! frontend reads the payload back with `FetchTaskDynamicFilters`.
//!
//! # Why one carrier task per query context
//!
//! The filter participant belongs to the query context, not to any one task,
//! but a status version and a retained payload belong to a task. So one task of
//! the context is designated the carrier, first-wins, and every channel of that
//! context publishes through it. First-wins rather than last: two tasks
//! replacing each other's sink would make which task carries a domain depend on
//! submission order, and the frontend would be reading a task that no longer
//! publishes.
//!
//! # What this loses, and why it is safe to lose
//!
//! A status owner retains exactly one payload. Two channels that publish
//! between two frontend reads therefore leave only the later one readable, and
//! a carrier task that reaches its terminal retains nothing at all -- a
//! terminal task's retained record is deliberately secret-free. Both are
//! fail-open: a channel the frontend never learns about widens to `All`, the
//! split source waits out its own initial cap, and enumeration proceeds
//! unpruned. Neither can change a query's rows.

use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_execution::runtime_filter::RuntimeFilterChannelId;
use novarocks_execution_contract::task_execution::domain::DomainVersion;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_proto_models::filter;
use novarocks_task_codec::domain::wire_task_dynamic_filter;

use novarocks_worker::runtime_filter::domain::{
    BackendFrontendFeedbackOutcome, BackendFrontendFeedbackPublication, BackendFrontendFeedbackSink,
};

use novarocks_worker::{DynamicFilterSubmission, TaskDynamicFilterWriter};

/// Publishes one query context's terminal logical feedback through one task.
pub struct TaskRuntimeFilterFeedbackEgress {
    carrier: TaskIdentity,
    producer: TaskDynamicFilterWriter,
    /// The next domain version this carrier will advertise.
    ///
    /// Minted here rather than taken from the channel: a channel's own logical
    /// version counts that channel's reductions, and two channels publishing
    /// their first reduction would both call it version one. The frontend's
    /// cursor is per task, so the version has to be per task as well.
    next_version: AtomicU64,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<std::sync::Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observation_controls:
        Option<std::sync::Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl TaskRuntimeFilterFeedbackEgress {
    pub fn new(carrier: TaskIdentity, producer: TaskDynamicFilterWriter) -> Self {
        Self {
            carrier,
            producer,
            next_version: AtomicU64::new(DomainVersion::FIRST.get()),
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_observation_controls(
        mut self,
        controls: Option<
            std::sync::Arc<crate::test_observation_controls::NativeObservationControls>,
        >,
    ) -> Self {
        self.observation_controls = controls;
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: std::sync::Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
    }

    pub const fn carrier(&self) -> TaskIdentity {
        self.carrier
    }
}

impl BackendFrontendFeedbackSink for TaskRuntimeFilterFeedbackEgress {
    fn try_publish(
        &self,
        channel_id: RuntimeFilterChannelId,
        deployment_epoch: u64,
        publication: &BackendFrontendFeedbackPublication,
        outcome: BackendFrontendFeedbackOutcome,
    ) {
        // Runner-owned perturbation is claimed here, at the publication the
        // frontend actually reads, because this is where the retired control
        // stream's feedback sink used to claim it. A claim left behind on the
        // retired carrier is armed and unconsumed: the query succeeds
        // untouched and the case waits out its budget.
        #[cfg(novarocks_dev_controls)]
        let outcome = if self.test_controls.as_ref().is_some_and(|controls| {
            crate::task_protocol_fault::force_feedback_unavailable(controls, self.carrier)
        }) {
            BackendFrontendFeedbackOutcome::ProducerUnavailable
        } else {
            outcome
        };
        #[cfg(novarocks_dev_controls)]
        let deployment_epoch = if self.test_controls.as_ref().is_some_and(|controls| {
            crate::task_protocol_fault::forge_feedback_foreign_attempt(controls, self.carrier)
        }) {
            // Any epoch but this attempt's. Wrapping keeps it a legal u64
            // rather than saturating onto a value the frontend could still
            // read as the active attempt.
            deployment_epoch.wrapping_add(1)
        } else {
            deployment_epoch
        };
        let (kind, payload) = match &outcome {
            BackendFrontendFeedbackOutcome::CanonicalDomain(domain) => (
                filter::RuntimeFilterEnvelopeKind::DegradedLogical,
                domain.as_ref().to_vec(),
            ),
            // The four unavailability reasons collapse to one statement here,
            // because that is the only one the frontend consumes: this
            // publisher will never produce a usable domain for this channel.
            // The reason itself is logged rather than encoded, so it stays
            // observable where it is known instead of being re-encoded as a
            // value this carrier cannot represent.
            BackendFrontendFeedbackOutcome::DomainBudget
            | BackendFrontendFeedbackOutcome::TypeUnsupported
            | BackendFrontendFeedbackOutcome::ReductionUnavailable
            | BackendFrontendFeedbackOutcome::ProducerUnavailable => {
                tracing::debug!(
                    task = %self.carrier,
                    channel_id = channel_id.get(),
                    reason = ?outcome,
                    "runtime filter channel has no usable terminal domain"
                );
                (filter::RuntimeFilterEnvelopeKind::Unavailable, Vec::new())
            }
        };
        let query_id = self.carrier.query_execution_id().query_id();
        let mut schema_digest = publication.contract_digest().to_vec();
        #[cfg(novarocks_dev_controls)]
        if self.test_controls.as_ref().is_some_and(|controls| {
            crate::task_protocol_fault::corrupt_feedback_contract_digest(controls, self.carrier)
        }) {
            // One bit, not a truncation: the frontend refuses a digest of the
            // wrong width before it ever compares one, so a shortened digest
            // would exercise the decoder instead of the contract fence.
            schema_digest[0] ^= 1;
        }
        let envelope = filter::RuntimeFilterEnvelope {
            kind: kind as i32,
            query_id: Some(novarocks_proto_models::common::UniqueId {
                hi: query_id.high(),
                lo: query_id.low(),
            }),
            channel_id: channel_id.get(),
            deployment_epoch,
            // A terminal logical domain has no route: it is not delivered to a
            // consumer binding, it is read by the coordinator. Naming one would
            // claim a producer route this publication never used.
            route_identity: None,
            schema_digest,
            payload,
            producer_open: None,
        };
        let version = match DomainVersion::new(self.next_version.fetch_add(1, Ordering::Relaxed)) {
            Ok(version) => version,
            Err(_) => {
                // Unreachable: the counter starts at one and only rises. It is
                // reported rather than asserted because losing this frame
                // widens pruning and must not stop a running task.
                tracing::warn!(
                    task = %self.carrier,
                    channel_id = channel_id.get(),
                    "dynamic filter version space is exhausted; the domain is not advertised"
                );
                return;
            }
        };
        #[cfg(novarocks_dev_controls)]
        let notice = self.observation_controls.clone().map(|controls| {
            Box::new(FeedbackCommittedNotice {
                controls,
                carrier: self.carrier,
                channel: channel_id,
            }) as Box<dyn novarocks_worker::TaskDynamicFilterCommittedNotice>
        });
        #[cfg(not(novarocks_dev_controls))]
        let notice = None;
        let advance = self
            .producer
            .submit(version, 1, wire_task_dynamic_filter(envelope), notice);
        if advance != DynamicFilterSubmission::Pending {
            tracing::debug!(task=%self.carrier,channel_id=channel_id.get(),?advance,"dynamic filter candidate was not admitted by its original carrier");
        }
    }
}

#[cfg(novarocks_dev_controls)]
struct FeedbackCommittedNotice {
    controls: std::sync::Arc<crate::test_observation_controls::NativeObservationControls>,
    carrier: TaskIdentity,
    channel: RuntimeFilterChannelId,
}
#[cfg(novarocks_dev_controls)]
impl novarocks_worker::TaskDynamicFilterCommittedNotice for FeedbackCommittedNotice {
    fn committed(self: Box<Self>, task: TaskIdentity, version: DomainVersion) {
        assert_eq!(
            task, self.carrier,
            "original RF publication receipt identity"
        );
        emit_advertised_marker(Some(&self.controls), task, self.channel, version);
    }
}

/// Stable evidence that a task advertised a new dynamic filter version.
///
/// It fires at the publication, not where the sink is installed: a marker
/// logged at install time would say only that a carrier exists, which is
/// exactly the defect this loop had.
#[cfg(novarocks_dev_controls)]
fn emit_advertised_marker(
    controls: Option<&crate::test_observation_controls::NativeObservationControls>,
    carrier: TaskIdentity,
    channel_id: RuntimeFilterChannelId,
    version: DomainVersion,
) {
    let Some(controls) = controls else { return };
    if !controls.owns_backend(carrier.backend_process_id()) {
        return;
    }
    let observer = match controls.observation_for(
        novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str(),
        carrier.query_execution_id(),
    ) {
        Ok(Some(observer)) => observer,
        Ok(None) => return,
        Err(error) => {
            eprintln!("Native filter observation rejected: {error}");
            return;
        }
    };
    let execution = carrier.query_execution_id();
    observer.observe(&format!(
        "{} execution_id={}:{}:{} stage={} task={} channel={} version={}",
        novarocks_failpoint::observation_marker::TASK_DYNAMIC_FILTER_ADVERTISED,
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        carrier.stage_id().get(),
        carrier.task_id().get(),
        channel_id.get(),
        version.get(),
    ));
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use super::TaskRuntimeFilterFeedbackEgress;

    use std::sync::Arc;

    use novarocks_execution::runtime_filter::RuntimeFilterChannelId;
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_proto_models::filter;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use crate::backend_task_execution::tests::node_fixture::{
        NativeNodePreparation, PreparationStop,
    };
    use novarocks_worker::runtime_filter::domain::{
        BackendFrontendFeedbackOutcome, BackendFrontendFeedbackPublication,
        BackendFrontendFeedbackSink, BackendMaterializationOwner,
    };
    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(11, 12), AttemptId::new(3).expect("nonzero"))
                .expect("legal execution"),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(4).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn publication() -> BackendFrontendFeedbackPublication {
        BackendFrontendFeedbackPublication::new(
            BackendMaterializationOwner::Aggregator,
            [9; 32],
            64 * 1024,
        )
        .expect("a legal publication")
    }

    #[test]
    fn a_published_domain_is_advertised_and_reads_back_as_its_own_envelope() {
        // The publish path end to end on this side: the sink turns a reduced
        // domain into a retained payload that the fetch boundary can actually
        // project. A payload this boundary cannot project is refused as an
        // internal invariant violation, so a wrong tag or a wrong message type
        // here would make every real fetch fail rather than degrade.
        let identity = identity();
        let status = NativeNodePreparation::new(identity);
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, status.dynamic_filter_writer());

        egress.try_publish(
            RuntimeFilterChannelId::new(7),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(vec![1, 2, 3])),
        );

        let current = status.wait_status(|current| current.dynamic_filters().is_some());
        let advertised = current
            .dynamic_filters()
            .expect("the status advertises the version it published");
        assert_eq!(advertised.version().get(), 1);
        assert_eq!(advertised.domain_count(), 1);

        let read = status.dynamic_filters();
        let (version, payload) = read
            .dynamic_filter_payload()
            .expect("the payload is retained");
        assert_eq!(version.get(), 1);
        let response =
            novarocks_task_codec::domain::encode_task_dynamic_filter_domain(version, payload)
                .expect("a projectable payload");
        let envelope = response
            .envelope
            .as_ref()
            .expect("the domain carries its envelope");
        assert_eq!(
            envelope.kind,
            filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32
        );
        assert_eq!(envelope.channel_id, 7);
        assert_eq!(envelope.deployment_epoch, 3);
        assert_eq!(envelope.payload, vec![1, 2, 3]);
        assert_eq!(
            envelope.schema_digest,
            vec![9; 32],
            "the frontend fences feedback against the contract digest it declared"
        );
        assert_eq!(
            envelope.query_id.as_ref().map(|id| (id.hi, id.lo)),
            Some((11, 12))
        );
    }

    #[test]
    fn every_channel_gets_its_own_version_on_the_carrier_task() {
        // The frontend's cursor is per task, so two channels publishing their
        // first reduction must not both claim version one -- the second would
        // be at or below the cursor and never fetched.
        let identity = identity();
        let status = NativeNodePreparation::new(identity);
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, status.dynamic_filter_writer());

        egress.try_publish(
            RuntimeFilterChannelId::new(7),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(vec![1])),
        );
        egress.try_publish(
            RuntimeFilterChannelId::new(8),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::DomainBudget,
        );

        let current = status.wait_status(|current| {
            current
                .dynamic_filters()
                .is_some_and(|value| value.version().get() == 2)
        });
        let advertised = current.dynamic_filters().expect("advertised");
        assert_eq!(advertised.version().get(), 2);
        let read = status.dynamic_filters();
        let (version, payload) = read.dynamic_filter_payload().expect("retained");
        let response =
            novarocks_task_codec::domain::encode_task_dynamic_filter_domain(version, payload)
                .expect("projectable");
        let envelope = response.envelope.as_ref().expect("envelope");
        assert_eq!(envelope.channel_id, 8);
        assert_eq!(
            envelope.kind,
            filter::RuntimeFilterEnvelopeKind::Unavailable as i32,
            "an unavailable channel is one statement, whichever reason produced it"
        );
        assert!(
            envelope.payload.is_empty(),
            "an unavailable channel carries no domain"
        );
    }

    #[test]
    fn a_terminal_carrier_advertises_nothing_rather_than_claiming_a_version() {
        // A terminal task's retained record is secret-free and holds no
        // payload, so advertising onto one would promise a fetch that can
        // never be served. The frontend widens the channel instead.
        let identity = identity();
        let status = NativeNodePreparation::new(identity);
        let writer = status.dynamic_filter_writer();
        // Cancellation and physical preparation return go through the actual
        // Node. The old publisher's weak writer cannot reopen its terminal
        // carrier after the original Context release.
        status.stop(PreparationStop::Cancel(
            novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
        ));
        NativeNodePreparation::release_participation(std::slice::from_ref(&status));
        assert!(
            status
                .wait_status(|current| current.is_terminal())
                .is_terminal()
        );
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, writer);

        egress.try_publish(
            RuntimeFilterChannelId::new(7),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(vec![1])),
        );

        assert!(status.status().dynamic_filters().is_none());
        assert!(status.dynamic_filters().dynamic_filter_payload().is_none());
    }
}
