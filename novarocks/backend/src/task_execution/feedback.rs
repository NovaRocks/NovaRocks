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

//! The backend end of the dynamic filter feedback loop.
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
use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_proto_codec::task_execution::domain::wire_task_dynamic_filter;
use novarocks_proto_models::filter;

use crate::runtime_filter::domain::{
    BackendFrontendFeedbackOutcome, BackendFrontendFeedbackPublication, BackendFrontendFeedbackSink,
};

use super::fault;
use super::status::TaskStatusReporter;

/// Publishes one query context's terminal logical feedback through one task.
pub(crate) struct TaskRuntimeFilterFeedbackEgress {
    carrier: TaskIdentity,
    reporter: TaskStatusReporter,
    /// The next domain version this carrier will advertise.
    ///
    /// Minted here rather than taken from the channel: a channel's own logical
    /// version counts that channel's reductions, and two channels publishing
    /// their first reduction would both call it version one. The frontend's
    /// cursor is per task, so the version has to be per task as well.
    next_version: AtomicU64,
}

impl TaskRuntimeFilterFeedbackEgress {
    pub(crate) fn new(carrier: TaskIdentity, reporter: TaskStatusReporter) -> Self {
        Self {
            carrier,
            reporter,
            next_version: AtomicU64::new(DomainVersion::FIRST.get()),
        }
    }

    pub(crate) const fn carrier(&self) -> TaskIdentity {
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
        let outcome = if fault::force_feedback_unavailable(self.carrier) {
            BackendFrontendFeedbackOutcome::ProducerUnavailable
        } else {
            outcome
        };
        let deployment_epoch = if fault::forge_feedback_foreign_attempt(self.carrier) {
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
        if fault::corrupt_feedback_contract_digest(self.carrier) {
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
        let advance =
            self.reporter
                .advertise_dynamic_filters(version, 1, wire_task_dynamic_filter(envelope));
        if advance.published_version().is_none() {
            // The carrier reached its terminal before this domain was reduced.
            // Nothing can be advertised on a terminal task, and its retained
            // record holds no payload, so the frontend will widen this channel.
            tracing::debug!(
                task = %self.carrier,
                channel_id = channel_id.get(),
                advance = ?advance,
                "dynamic filter domain could not be advertised on its carrier task"
            );
            return;
        }
        emit_advertised_marker(self.carrier, channel_id, version);
    }
}

/// Stable evidence that a task advertised a new dynamic filter version.
///
/// It fires at the publication, not where the sink is installed: a marker
/// logged at install time would say only that a carrier exists, which is
/// exactly the defect this loop had.
fn emit_advertised_marker(
    carrier: TaskIdentity,
    channel_id: RuntimeFilterChannelId,
    version: DomainVersion,
) {
    if !crate::config::debug_emit_connector_reader_marker() {
        return;
    }
    let execution = carrier.query_execution_id();
    println!(
        "NOVAROCKS_TASK_DYNAMIC_FILTER_ADVERTISED execution_id={}:{}:{} stage={} task={} channel={} version={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        carrier.stage_id().get(),
        carrier.task_id().get(),
        channel_id.get(),
        version.get(),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use super::TaskRuntimeFilterFeedbackEgress;

    use std::sync::Arc;

    use novarocks_execution::runtime_filter::RuntimeFilterChannelId;
    use novarocks_execution::task_execution::identity::TaskIdentity;
    use novarocks_proto_models::filter;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use crate::runtime_filter::domain::{
        BackendFrontendFeedbackOutcome, BackendFrontendFeedbackPublication,
        BackendFrontendFeedbackSink, BackendMaterializationOwner,
    };
    use crate::task_execution::clock::ProcessMonotonicClock;
    use crate::task_execution::observation::TaskStatusSource;
    use crate::task_execution::shared_facts::encode_dynamic_filter_read;
    use crate::task_execution::status::{
        METRIC_PUBLISH_MIN_INTERVAL, TaskStatusOwner, TaskStatusReporter,
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

    fn owner(identity: TaskIdentity) -> (Arc<TaskStatusOwner>, TaskStatusReporter) {
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            Arc::new(TaskStatusSource::new()),
            Arc::new(ProcessMonotonicClock::new()),
            METRIC_PUBLISH_MIN_INTERVAL,
        ));
        owner.release_to_observers();
        (Arc::clone(&owner), TaskStatusReporter::new(owner))
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
        let (status, reporter) = owner(identity);
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, reporter);

        egress.try_publish(
            RuntimeFilterChannelId::new(7),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(vec![1, 2, 3])),
        );

        let advertised = status
            .current()
            .dynamic_filters()
            .expect("the status advertises the version it published");
        assert_eq!(advertised.version().get(), 1);
        assert_eq!(advertised.domain_count(), 1);

        let read = status.dynamic_filters().expect("the payload is retained");
        let response =
            encode_dynamic_filter_read(identity, Some(&read)).expect("a projectable payload");
        assert_eq!(response.version, 1);
        let envelope = response.domains[0]
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
        let (status, reporter) = owner(identity);
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, reporter);

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

        let advertised = status.current().dynamic_filters().expect("advertised");
        assert_eq!(advertised.version().get(), 2);
        let read = status.dynamic_filters().expect("retained");
        let response = encode_dynamic_filter_read(identity, Some(&read)).expect("projectable");
        let envelope = response.domains[0].envelope.as_ref().expect("envelope");
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
        let (status, reporter) = owner(identity);
        reporter.running();
        reporter.finished(novarocks_execution::task_execution::status::TaskOutputFacts::new(true));
        let egress = TaskRuntimeFilterFeedbackEgress::new(identity, reporter);

        egress.try_publish(
            RuntimeFilterChannelId::new(7),
            3,
            &publication(),
            BackendFrontendFeedbackOutcome::CanonicalDomain(Arc::from(vec![1])),
        );

        assert!(status.current().dynamic_filters().is_none());
        assert!(status.dynamic_filters().is_none());
    }
}
