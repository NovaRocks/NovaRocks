// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Included as a child of attempt_handoff; all product payloads stay concrete.
use super::*;
use crate::native::fragment_transport::{
    DynamicFilterRead, DynamicFilterReadError, FinalTaskInfoRead, TaskResultTransport,
};
use crate::native::task_transport::TaskAckIntakeHandle;
use crate::query_execution::internal_consumer::test_fixture::{
    internal_count_description, original_credentials,
};
use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
use crate::task_execution::{
    clock::ManualClock,
    profile_diagnostics::TaskFinalInfoCollector,
    round::TaskRound,
    status_intake::{CountingWake, ObservationFrame, ObservationIntakeEntry},
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_execution_contract::{
    DomainVersion, OperationKind, OperationOutcome, QueryContextConvergenceReceipt,
    QueryContextConvergenceState, QueryContextConvergenceVersion, QueryContextReceipt,
    QueryContextRef, QueryContextState, ReleaseOutcome,
};
use novarocks_query_application::api::{
    ComputationObservationBudgets, ComputationReadyToken, InternalConsumerActivation,
};
use novarocks_query_application::coordination::{
    AbortQueryContextEffectPort, LogicalExecutionActorConfig,
};
use novarocks_query_application::test_support::LogicalExecutionTestHarness;
use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};
use novarocks_types::{AttemptId, QueryId};
use novarocks_workload_control::{
    BusinessPermit, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::task::Poll;
use std::time::{Duration, Instant};

struct UnavailableFinalInfo;
impl TaskResultTransport for UnavailableFinalInfo {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        Box::pin(async { Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone)) })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("the handoff component must not fetch dynamic filters")
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WriteProtocolRoles {
    None,
    UpstreamWriters,
    IndependentSummarySource,
}

pub(crate) struct OriginalAttempt {
    pub(crate) round: TaskRound,
    pub(crate) acks: TaskAckIntakeHandle,
    pub(crate) release: Option<OperationIntent>,
    pub(crate) pending_upstream: Option<crate::task_execution::tests::ManifestReleaseTail>,
    pub(crate) scope: novarocks_workload_control::WorkScope,
    pub(crate) cancellation_requester:
        Option<novarocks_workload_control::WorkCancellationRequester>,
    pub(crate) capacity: novarocks_workload_control::ResultCapacityHandle,
    clock: Arc<ManualClock>,
    pub(crate) plan: PlanSeal,
    pub(crate) root: TaskIdentity,
    context: QueryContextRef,
    pub(crate) logical: LogicalExecutionTestHarness,
    // This stands for the original Native active behavior's strong custody.
    pub(crate) native_credentials: Arc<AttemptCredentialStorage>,
    diagnostics: TaskFinalInfoCollector,
    _control: WorkloadControl,
    business: Option<BusinessPermit>,
}
impl OriginalAttempt {
    async fn new(attempt: u64) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        Self::new_with_transport(attempt, Arc::new(UnavailableFinalInfo)).await
    }
    async fn new_with_transport(
        attempt: u64,
        transport: Arc<dyn TaskResultTransport>,
    ) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        let (description, plan) = internal_count_description();
        Self::new_with_description(attempt, transport, description, plan, false).await
    }
    async fn new_with_description(
        attempt: u64,
        transport: Arc<dyn TaskResultTransport>,
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        plan: PlanSeal,
        pending: bool,
    ) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        let execution =
            QueryExecutionId::new(QueryId::new(0x92, 0x31), AttemptId::new(attempt).unwrap())
                .unwrap();
        Self::new_bound(
            execution,
            transport,
            description,
            plan,
            pending,
            None,
            false,
        )
        .await
    }
    pub(crate) async fn new_bound(
        execution: QueryExecutionId,
        transport: Arc<dyn TaskResultTransport>,
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        plan: PlanSeal,
        pending: bool,
        native_credentials: Option<Arc<AttemptCredentialStorage>>,
        writers: bool,
    ) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        Self::new_bound_with_work(
            execution,
            transport,
            description,
            plan,
            pending,
            native_credentials,
            if writers {
                WriteProtocolRoles::UpstreamWriters
            } else {
                WriteProtocolRoles::None
            },
            None,
        )
        .await
    }

    pub(crate) async fn new_bound_with_summary_source(
        execution: QueryExecutionId,
        transport: Arc<dyn TaskResultTransport>,
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        plan: PlanSeal,
        native_credentials: Arc<AttemptCredentialStorage>,
    ) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        Self::new_bound_with_work(
            execution,
            transport,
            description,
            plan,
            true,
            Some(native_credentials),
            WriteProtocolRoles::IndependentSummarySource,
            None,
        )
        .await
    }

    // Test-only injection of an exact already-admitted owner. A child is not
    // relabelled as RootWork and never receives its parent's BusinessPermit.
    async fn new_bound_with_work(
        execution: QueryExecutionId,
        transport: Arc<dyn TaskResultTransport>,
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        plan: PlanSeal,
        pending: bool,
        native_credentials: Option<Arc<AttemptCredentialStorage>>,
        write_roles: WriteProtocolRoles,
        original: Option<(
            WorkloadControl,
            novarocks_workload_control::WorkOwner,
            novarocks_workload_control::ResultCapacityHandle,
        )>,
    ) -> (Self, AttemptHandoffResources, InternalConsumerActivation) {
        let (mut round, acks, release, pending_upstream, clock, protocol, establish_facts) =
            if write_roles == WriteProtocolRoles::IndependentSummarySource {
                assert!(pending);
                let (round, acks, tail, clock, protocol, facts) =
                    crate::task_execution::tests::manifest_pending_write_with_summary_source_protocol_fixture(
                        execution, plan,
                    );
                (round, acks, None, Some(tail), clock, protocol, facts)
            } else if write_roles == WriteProtocolRoles::UpstreamWriters {
                assert!(pending);
                let (round, acks, tail, clock, protocol, facts) =
                    crate::task_execution::tests::manifest_pending_write_with_protocol_fixture(
                        execution, plan,
                    );
                (round, acks, None, Some(tail), clock, protocol, facts)
            } else if pending {
                let (round, acks, tail, clock, protocol, facts) =
                    crate::task_execution::tests::manifest_pending_release_with_protocol_fixture(
                        execution, plan,
                    );
                (round, acks, None, Some(tail), clock, protocol, facts)
            } else {
                let (round, acks, release, clock, protocol, facts) =
                    crate::task_execution::tests::manifest_normal_release_ack_with_protocol_fixture(
                        execution, plan,
                    );
                (round, acks, Some(release), None, clock, protocol, facts)
            };
        let root = round.root_task();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (control, work, capacity, business) = if let Some((control, work, capacity)) = original
        {
            (control, work, capacity, None)
        } else {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1 << 10,
                    per_scope_bytes: 1 << 18,
                },
            )
            .unwrap();
            let capacity = control
                .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let governed = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            (control, governed.owner, capacity, Some(governed.business))
        };
        let scope = work.scope();
        let cancellation_requester = Some(work.cancellation_requester());
        let stage = scope
            .try_acquire(novarocks_workload_control::Stage::Execution)
            .unwrap();
        let (abort, abort_intake) =
            crate::task_execution::abort_effect::NativeAbortEffectAdapter::bounded(
                NonZeroUsize::new(2).unwrap(),
                Arc::new(CountingWake::default()),
            );
        round.install_abort_effect_intake(abort_intake);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            description.effect(),
            NonZeroUsize::new(8).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(
            abort as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install_internal(
            tokio::runtime::Handle::current(),
            config,
            &description,
            NonZeroUsize::new(1).unwrap(),
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            ComputationObservationBudgets::new(Duration::from_secs(1), Duration::from_secs(5)),
            execution,
            vec![context],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        crate::task_execution::tests::settle_original_actor_establish_facts(
            &logical,
            establish_facts,
        )
        .await;
        let activation = logical.internal_consumer_activation(root).await.unwrap();
        let mut diagnostics =
            TaskFinalInfoCollector::new(transport, true, &round, Arc::new(CountingWake::default()));
        let profile = diagnostics.enable_profile_observation().unwrap();
        let native_credentials =
            native_credentials.unwrap_or_else(|| original_credentials(execution));
        let resources = AttemptHandoffResources::new(
            protocol,
            Some(Arc::clone(&native_credentials)),
            Some(profile),
        );
        (
            Self {
                round,
                acks,
                release,
                pending_upstream,
                scope,
                cancellation_requester,
                capacity,
                clock,
                plan,
                root,
                context,
                logical,
                native_credentials,
                diagnostics,
                _control: control,
                business,
            },
            resources,
            activation,
        )
    }
    pub(crate) fn bind_original_drive(&mut self, producer: &mut AttemptHandoffProducer) {
        let drive = self.logical.native_attempt_drive();
        self.round.bind_protocol_drive(&drive).unwrap();
        producer.bind(&drive).unwrap();
    }
    async fn accept_original_end(
        &self,
        activation: InternalConsumerActivation,
    ) -> ComputationReadyToken {
        let status = self
            .round
            .execution()
            .task(self.root.task_id())
            .unwrap()
            .status()
            .unwrap()
            .clone();
        self.logical
            .accept_internal_end_from(
                activation,
                self.root,
                status,
                RootResultEnd {
                    sequence: NonZeroU64::MIN,
                    output_rows: 0,
                },
            )
            .await
            .unwrap()
    }
    async fn publish_original_snapshot(&mut self, ready: ComputationReadyToken) {
        self.diagnostics.begin_profile_observation(ready).unwrap();
        let count = self.round.execution().graph().tasks().count();
        for _ in 0..=count {
            std::future::poll_fn(|cx| {
                self.diagnostics
                    .poll_profile_observation(&self.round, self.clock.now(), false, cx);
                Poll::Ready(())
            })
            .await;
        }
        assert!(self.diagnostics.is_closed());
        assert!(self.diagnostics.all_terminal_observed());
        // Final info closes the existing Release hold; the final immutable
        // Profile also needs that original acknowledgement's RF contribution.
        self.settle_original_release();
        self.round.turn().unwrap();
        std::future::poll_fn(|cx| {
            self.diagnostics
                .poll_profile_observation(&self.round, self.clock.now(), false, cx);
            Poll::Ready(())
        })
        .await;
    }
    fn settle_original_release(&mut self) {
        if let Some(release) = self.release.as_ref() {
            let OperationIntent::ReleaseQueryContext(request) = release else {
                unreachable!("the fixture returns its original Release")
            };
            // A diagnostic consumer can already have settled this exact
            // Release. Never deliver its original acknowledgement twice.
            if !self
                .round
                .execution()
                .owner(self.context)
                .unwrap()
                .is_released()
            {
                self.acks.publish(OperationAcknowledgement::new(
                    release.operation_id(),
                    OperationKind::ReleaseQueryContext,
                    OperationOutcome::Accepted,
                    AckPayload::Release {
                        receipt: QueryContextReceipt::new(
                            request.context(),
                            QueryContextState::TerminalRetained,
                        ),
                        outcome: ReleaseOutcome::Released,
                        runtime_filter: None,
                    },
                ));
            }
        } else {
            assert!(self.pending_upstream.is_none());
            if self.round.execution().selected_task_failure().is_none() {
                let reason = self.scope.cancellation().unwrap().reason().unwrap();
                let expected_kind = match reason {
                    novarocks_workload_control::CancellationReason::Requested => {
                        novarocks_query_application::api::QueryExecutionErrorKind::Cancelled
                    }
                    novarocks_workload_control::CancellationReason::DeadlineExceeded => {
                        novarocks_query_application::api::QueryExecutionErrorKind::DeadlineExceeded
                    }
                    _ => panic!(
                        "only the original requested cancellation or deadline may settle here"
                    ),
                };
                let outcome = self.logical.original_internal_outcome();
                assert_eq!(
                    outcome
                        .borrow()
                        .as_ref()
                        .expect("the same actor published its original control outcome")
                        .as_ref()
                        .unwrap_err()
                        .kind(),
                    expected_kind
                );
            }
            assert!(
                self.round
                    .execution()
                    .owner(self.context)
                    .unwrap()
                    .is_released()
            );
            assert!(self.round.attempt_drained());
        }
    }

    pub(crate) async fn finish(self) {
        // Existing fixtures own their process only through this attempt.
        drop(self.finish_retaining_process().await);
    }

    pub(crate) async fn finish_retaining_process(mut self) -> WorkloadControl {
        self.settle_original_release();
        self.round.turn().unwrap();
        self.round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context: self.context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(
                            QueryContextConvergenceReceipt::new(
                                self.context,
                                QueryContextConvergenceVersion::FIRST,
                                QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                            ),
                        ),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
        assert!(self.round.attempt_drained());
        assert!(self.round.pumps_closed());
        assert!(self.round.execution().worker_contexts_stopped());
        // A Native failure may already have consumed the original permit.
        // Only an unconcluded attempt needs abandonment; finish_until drops
        // any retained permit without choosing another logical conclusion.
        if self
            .logical
            .actor_snapshot()
            .await
            .unwrap()
            .conclusion
            .is_none()
        {
            self.logical.abandon_running_attempt();
        }
        self.logical
            .observe_worker_stopped_and_context_fenced(self.context)
            .await
            .unwrap();
        self.logical
            .finish_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        if let Some(business) = self.business {
            business.release();
        }
        drop(self.cancellation_requester.take());
        self._control
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_staged_bundle_waits_for_drive_then_claims_once_and_reaches_real_ready() {
    let (offers, claims) = attempt_handoff_mailbox();
    let view = offers.reserve_view();
    drop(offers);
    let (mut attempt, resources, activation) = OriginalAttempt::new(1).await;
    let mut original_producer = None;
    view.reserve_in(attempt.root.query_execution_id(), &mut original_producer)
        .unwrap();
    let mut producer = original_producer.take().unwrap();
    producer
        .stage(
            attempt.root.query_execution_id(),
            attempt.root,
            attempt.plan,
            resources,
        )
        .unwrap();
    let handoff = {
        let claim = claims.claim(LogicalExecutionTestHarness::internal_consumer_source(
            &activation,
        ));
        tokio::pin!(claim);
        assert!(
            futures::poll!(claim.as_mut()).is_pending(),
            "staged resources are not an actual Drive binding"
        );
        attempt.bind_original_drive(&mut producer);
        claim.await.unwrap()
    };
    assert_eq!(
        handoff.activation(),
        LogicalExecutionTestHarness::internal_consumer_source(&activation).activation()
    );
    assert_eq!(handoff.root(), attempt.root);
    assert_eq!(handoff.plan(), attempt.plan);
    assert!(
        claims
            .claim(LogicalExecutionTestHarness::internal_consumer_source(
                &activation
            ))
            .await
            .is_err()
    );
    let (mut protocol, credentials, profile) = handoff.into_parts();
    assert!(Arc::ptr_eq(
        credentials.as_ref().unwrap(),
        &attempt.native_credentials
    ));
    let source = LogicalExecutionTestHarness::internal_consumer_source(&activation);
    let initial = protocol
        .take_changes(source.activation(), source.root(), source.plan())
        .unwrap()
        .unwrap();
    assert!(initial.is_initial());
    assert_eq!(
        initial.tasks().len(),
        attempt.round.execution().graph().tasks().count()
    );
    assert!(
        initial
            .tasks()
            .iter()
            .all(|change| change.identity().query_execution_id()
                == attempt.root.query_execution_id())
    );
    let ready = attempt.accept_original_end(activation).await;
    attempt.publish_original_snapshot(ready).await;
    let profile = profile.expect("the original Profile observer crossed the concrete bundle");
    assert_eq!(profile.current(ready).unwrap().unwrap().ready, ready);
    assert!(matches!(
        attempt.logical.actor_snapshot().await.unwrap().conclusion,
        Some(novarocks_query_application::coordination::LogicalConclusion::ComputationReady)
    ));
    producer.retire_unclaimed();
    drop(producer);
    assert!(Arc::ptr_eq(
        credentials.as_ref().unwrap(),
        &attempt.native_credentials
    ));
    drop((protocol, credentials, profile));
    attempt.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn claimed_old_bundle_survives_replacement_and_a_stale_source_cannot_take_the_new_offer() {
    let (offers, claims) = attempt_handoff_mailbox();
    let (mut old, resources, old_activation) = OriginalAttempt::new(1).await;
    let mut old_producer = offers.reserve(old.root.query_execution_id()).unwrap();
    old_producer
        .stage(old.root.query_execution_id(), old.root, old.plan, resources)
        .unwrap();
    old.bind_original_drive(&mut old_producer);
    let old_handoff = claims
        .claim(LogicalExecutionTestHarness::internal_consumer_source(
            &old_activation,
        ))
        .await
        .unwrap();
    let (mut old_protocol, old_credentials, old_profile) = old_handoff.into_parts();
    let old_profile =
        old_profile.expect("the claimed old observer remains owned by the old consumer");
    let (mut next, resources, next_activation) = OriginalAttempt::new(2).await;
    let mut next_producer = offers.reserve(next.root.query_execution_id()).unwrap();
    next_producer
        .stage(
            next.root.query_execution_id(),
            next.root,
            next.plan,
            resources,
        )
        .unwrap();
    assert!(
        claims
            .claim(LogicalExecutionTestHarness::internal_consumer_source(
                &old_activation
            ))
            .await
            .is_err()
    );
    let next_handoff = {
        let next_claim = claims.claim(LogicalExecutionTestHarness::internal_consumer_source(
            &next_activation,
        ));
        tokio::pin!(next_claim);
        assert!(futures::poll!(next_claim.as_mut()).is_pending());
        old_producer.retire_unclaimed();
        drop(old_producer);
        assert!(Arc::ptr_eq(
            old_credentials.as_ref().unwrap(),
            &old.native_credentials
        ));
        let old_source = LogicalExecutionTestHarness::internal_consumer_source(&old_activation);
        let original = old_protocol
            .take_changes(
                old_source.activation(),
                old_source.root(),
                old_source.plan(),
            )
            .unwrap()
            .unwrap();
        assert!(
            original
                .tasks()
                .iter()
                .all(|change| change.identity().query_execution_id()
                    == old.root.query_execution_id())
        );
        assert!(
            old_protocol
                .take_changes(
                    LogicalExecutionTestHarness::internal_consumer_source(&next_activation)
                        .activation(),
                    next.root,
                    next.plan
                )
                .is_err()
        );
        next.bind_original_drive(&mut next_producer);
        next_claim.await.unwrap()
    };
    let (mut next_protocol, next_credentials, next_profile) = next_handoff.into_parts();
    assert!(Arc::ptr_eq(
        next_credentials.as_ref().unwrap(),
        &next.native_credentials
    ));
    assert!(!Arc::ptr_eq(
        next_credentials.as_ref().unwrap(),
        old_credentials.as_ref().unwrap()
    ));
    let source = LogicalExecutionTestHarness::internal_consumer_source(&next_activation);
    let next_initial = next_protocol
        .take_changes(source.activation(), source.root(), source.plan())
        .unwrap()
        .unwrap();
    assert!(
        next_initial
            .tasks()
            .iter()
            .all(|change| change.identity().query_execution_id() == next.root.query_execution_id())
    );
    let ready = next.accept_original_end(next_activation).await;
    next.publish_original_snapshot(ready).await;
    let next_profile = next_profile.unwrap();
    assert_eq!(next_profile.current(ready).unwrap().unwrap().ready, ready);
    assert!(
        old_profile.current(ready).unwrap().is_none(),
        "the old observer cannot see the replacement snapshot"
    );
    // The mailbox test does not invent a QA recovery ticket: the two original
    // actors serve two real activation/Drive sources. Drop the unfinished old
    // consumer and then converge its original owner, independently of the new cut.
    drop(old_activation);
    drop((old_protocol, old_credentials, old_profile));
    next_producer.retire_unclaimed();
    drop(next_producer);
    drop((next_protocol, next_credentials, next_profile));
    old.finish().await;
    next.finish().await;
}

// Child tests reuse this original private fixture instead of cloning a driver.
#[path = "profile_component_tests.rs"]
mod profile_consumer_tests;

#[path = "statistics_component_tests.rs"]
mod statistics_consumer_tests;
