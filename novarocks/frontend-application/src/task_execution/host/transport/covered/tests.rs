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

use super::super::releases::TransportReleases;
use super::*;
use crate::{
    control_runtime::{
        actor::FrontendActorKind,
        host::{EffectSource, FrontendLanePolicy, HostBinding, HostRegistry, PendingHandoff},
        metrics::FrontendHostObservations,
    },
    native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback},
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::{
        bridge::root_control::RootControlRequests,
        host::{
            custody::{TransportRetirements, tests::WithdrawalInbox},
            failure_publication::FailurePublications,
            funding::AttemptHostFunding,
            observations::{
                ObservationBudget, OrderedObservationInput,
                backing::{
                    OBSERVATION_BACKING_BYTES, ObservationBackingQuote, PaidObservationMetadata,
                },
                credits::{OBSERVATION_WINDOW_BYTES, ObservationCredits},
                host_turn::OrderedObservationHost,
                read_progress::ReadProgressInput,
                source::BoundObservationSource,
                tests::ManualClock,
            },
            transport::ActorTransportExecutor,
        },
    },
};
use novarocks_attempt_protocol::{
    effect::{AttemptEffect, EffectClass},
    observation::SubscriptionRequest,
};
use novarocks_control_runtime::{
    actor::ActorConfig,
    actor_turn::{ActorHostTurn, ProcessClock},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::Tick,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_workload_control::{WorkClass, WorkRequest};
use std::{
    num::NonZeroU64,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

struct HostSource {
    subscriptions: AttemptSubscriptions,
    control: AttemptCallSource,
    releases: TransportReleases,
    sources_retired: Arc<AtomicBool>,
    ordered_handed_off: bool,
    open_pending: bool,
    closing: bool,
}
impl EffectSource for HostSource {
    fn prepay_result(&mut self, effect: &FrontendEffect) -> Option<Position> {
        let FrontendEffect::Attempt(effect) = effect else {
            unreachable!()
        };
        self.open_pending = effect.is_subscription_open();
        if self.open_pending {
            self.subscriptions.prepay(effect)
        } else {
            self.control.prepay(effect).unwrap()
        }
    }

    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        if self.open_pending {
            self.subscriptions.bind_job(ticket, result)
        } else {
            self.control.bind(ticket, result)
        }
    }

    fn return_unclaimed_result(&mut self, result: Position) {
        if self.open_pending {
            self.subscriptions.return_unclaimed(result);
        } else {
            self.control.return_unclaimed(result);
        }
    }

    fn core_consumed(&mut self, inputs: &[InputId]) {
        self.subscriptions.core_consumed(inputs);
    }

    fn transport_event(&mut self, effect: AttemptActionId, event: TransportSourceEvent) {
        if self.subscriptions.owns(effect) {
            match event {
                TransportSourceEvent::Result(original) => self
                    .subscriptions
                    .accept(effect, original)
                    .unwrap_or_else(|original| std::panic::resume_unwind(original)),
                TransportSourceEvent::Retired => self.subscriptions.transport_retired(effect),
                original => std::panic::resume_unwind(Box::new(original)),
            }
        } else {
            match event {
                TransportSourceEvent::Result(original) => self
                    .control
                    .accept(original)
                    .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original))),
                TransportSourceEvent::Retired => self.control.transport_retired(effect),
                original => std::panic::resume_unwind(Box::new(original)),
            }
        }
    }

    fn owner_failed_pending(&mut self, original: PendingHandoff) {
        std::panic::resume_unwind(Box::new(original))
    }

    fn close_external(&mut self) {
        self.closing = true;
    }

    fn result_admission_closed(&self) -> bool {
        self.closing
    }

    fn can_close_ordered_observations(&self, failed: bool) -> bool {
        assert!(!failed);
        self.closing
            && self.subscriptions.is_drained()
            && self.control.is_drained()
            && self.releases.is_empty()
    }

    fn close_ordered_observations(&mut self, original: OrderedObservationHost) {
        assert!(!std::mem::replace(&mut self.ordered_handed_off, true));
        self.releases.retain(ReleaseCapture::Observations(original));
    }

    fn progress_retirement(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let release_visit = usize::from(budget != 0 && self.releases.has_work());
        let released = release_visit != 0
            && self
                .releases
                .progress(&self.control)
                .unwrap_or_else(|failure| std::panic::resume_unwind(Box::new(failure)));
        let accepts = self.releases.accepts_batch();
        let allowance = if accepts {
            (budget - release_visit).min(1)
        } else {
            0
        };
        let mut original =
            self.subscriptions
                .progress(false, self.closing, true, allowance, |capture| {
                    self.releases.retain(capture)
                });
        original.units += release_visit;
        original.more &= accepts;
        original.more |= released || original.progressed && self.releases.has_work();
        original.progressed |= released;
        if original.units < budget {
            original.progressed |= self.control.retire_result().unwrap();
            if self.closing && self.releases.accepts_batch() {
                original.progressed |= self.control.close_idle().unwrap();
                if let Some(owner) = self.control.take_drained_owner() {
                    self.releases
                        .retain(ReleaseCapture::Original(Box::new(owner)));
                    original.progressed = true;
                    original.more = true;
                }
            }
            original.units += 1;
        }
        original.retired &= self.ordered_handed_off && self.can_close_ordered_observations(false);
        if original.retired {
            self.sources_retired.store(true, Ordering::Release);
        }
        original
    }
}

#[tokio::test]
async fn installed_subscription_uses_one_native_stream_and_reports_exact_actual_exit() {
    let generation = Generation::new(1).unwrap();
    let budget = ObservationBudget {
        queued_frames: 2,
        total_positions: 4,
        max_frame_bytes: 1024,
        total_bytes: 2560,
        control_bytes: 512,
    };
    let quote = ObservationBackingQuote::new(budget, 1).unwrap();
    let backing_bytes = quote.quota().get(OBSERVATION_BACKING_BYTES);
    let resources = budget
        .byte_window()
        .unwrap()
        .checked_add(quote.quota())
        .unwrap();
    let ledger = RuntimeLedger::new(resources.clone());
    let outer = ledger.open_scope_next(None, generation).unwrap();
    let (mut launcher, mut launcher_payer) = outer
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    launcher.start().unwrap();
    let mut funding = AttemptHostFunding::reserve(
        &ledger,
        Some(&launcher),
        generation,
        ReservationResources::new(
            resources,
            OwnedSlotNeed {
                work_headers: 1,
                inputs: budget.total_positions,
                ..Default::default()
            },
        ),
    )
    .unwrap();
    let scope = funding.scope().clone();
    let (mut command, mut command_payer) = scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    command.start().unwrap();
    let backend = BackendProcessId::new_v7();
    let execution = QueryExecutionId::new(QueryId::new(17, 1), AttemptId::new(1).unwrap()).unwrap();
    let participation = ParticipationRef::new(execution, FrontendProcessId::new_v7(), backend);
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation: NonZeroU64::MIN,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    let adjusted = Arc::new((*request).clone());
    let id = |n| AttemptActionId::new(NonZeroU64::new(n).unwrap());
    let effects = [
        EffectBody::OpenSubscription(request.clone()),
        EffectBody::AdjustReadWindow(adjusted.clone()),
        EffectBody::CloseSubscription {
            participation,
            generation: request.generation,
        },
    ];
    let mailbox = [8; 4];
    let inbox = WithdrawalInbox::with_commands(
        &scope,
        effects.into_iter().enumerate().map(|(n, body)| {
            (
                AttemptEffectEnvelope::new(AttemptEffect {
                    id: id(n as u64 + 1),
                    class: EffectClass::Settlement,
                    body,
                }),
                None,
            )
        }),
        mailbox,
    );
    let runtime = inbox.runtime();
    let registration = inbox.registration();
    let handle = tokio::runtime::Handle::current();
    let transport =
        TransportRetirements::new(runtime.clone(), registration.clone(), handle.clone(), 3);
    let root = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        backend,
    );
    let root = RootControlBinding::for_registered_source(
        registration.clone(),
        AttemptActivationIdentity::from_parts(1, execution, 1),
        root,
    )
    .unwrap();
    let (observation, observation_payer, root_control) = funding.observation_and_root_control();
    let control_scope = root_control.scope().clone();
    let metadata = PaidObservationMetadata::take_from(
        quote,
        observation,
        observation_payer,
        transport.host_waker(),
    )
    .unwrap();
    let credits = ObservationCredits::bind(
        budget,
        [participation],
        observation,
        observation_payer,
        Some(metadata.pin()),
    )
    .unwrap();
    let standing = |source, class, work| {
        runtime
            .reserve_admitted_input_for(registration, source, class, work)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let progress = ReadProgressInput::new(
        registration,
        participation,
        standing(inbox.source(), InputClass::Ordinary, observation),
    )
    .with_backing(Some(metadata.pin()));
    let requests = RootControlRequests::new(
        root.clone(),
        Arc::new(FailurePublications::new(
            registration.clone(),
            root.activation(),
            &scope,
        )),
    );
    let source = BoundObservationSource::new(
        root.clone(),
        requests.clone(),
        credits,
        BTreeMap::from([(participation, progress.clone())]),
        metadata,
    );
    let observation_source = ActorId::new(4).unwrap();
    let control_position = standing(observation_source, InputClass::Control, root_control);
    let reply_author = control_position.author().unwrap();
    let ingress = OrderedObservationInput::new(
        standing(observation_source, InputClass::Lifecycle, observation),
        control_position,
        source,
        root.clone(),
        Arc::new(ManualClock::default()),
        |input| FrontendInput::Attempt(Box::new(input)),
        transport.host_waker(),
    )
    .unwrap();
    root.bind_reply_author(reply_author);
    requests.bind_ingress(&ingress);
    let weak_ingress = ingress.weak();
    let (wc_owner, mut wc_driver, capacity) = wc::fixture();
    let wc_root = wc_owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let wc_scope = wc_root.owner.scope();
    wc::bind(&wc_scope, &mut wc_driver);
    let cohort = wc::cohort(&wc_scope, 1, &mut wc_driver);
    let data = FrontendDataRuntime::new(handle.clone());
    let mut peer = Loopback::start().await;
    let backends: Arc<[BackendProcessDescriptor]> = Arc::from([peer.descriptor(backend)]);
    let descriptor = &backends[0];
    let frozen = || FrozenSubscriptionInputs {
        backends: &backends,
        compatibility: descriptor.native_compatibility_id(),
        data: &data,
        scope: &wc_scope,
        cohort: &cohort,
        ingress: &ingress,
    };
    assert!(matches!(
        bind_subscription_endpoints(
            &[FrozenMember {
                participation,
                endpoint: descriptor.control_endpoint().clone(),
            }],
            frozen()
        ),
        Err(SubscriptionEndpointError::Roster)
    ));
    assert_eq!(cohort.remaining(), 1);
    let endpoints = bind_subscription_endpoints(
        &[FrozenMember {
            participation,
            endpoint: descriptor.endpoint().clone(),
        }],
        frozen(),
    )
    .unwrap();
    assert_eq!(cohort.remaining(), 0);
    let original = endpoints[&participation].source.clone();
    let observer = ConsumerId::new(1).unwrap();
    let (parent, payer) = funding.parent();
    let subscriptions = AttemptSubscriptions::bind(
        runtime.clone(),
        registration.clone(),
        inbox.source(),
        &root,
        parent,
        payer,
        observer,
        endpoints.clone(),
    )
    .unwrap();
    let control = AttemptCallSource::new(
        parent,
        payer,
        generation,
        observer,
        AttemptCallOwner::SubscriptionControl(endpoints),
        inbox.reserve_result(parent),
    );
    let host = HostRegistry::new(
        runtime.clone(),
        ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let sources_retired = Arc::new(AtomicBool::new(false));
    let mut funding = Some(funding);
    let mut exit = host
        .register_with_funding(
            HostBinding {
                scope: scope.clone(),
                registration: registration.clone(),
                kind: FrontendActorKind::Test,
                config: ActorConfig {
                    mailbox,
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                policy: FrontendLanePolicy::attempt(1),
                source: Box::new(HostSource {
                    subscriptions,
                    control,
                    releases: TransportReleases::new(handle.clone(), transport.host_waker()),
                    sources_retired: sources_retired.clone(),
                    ordered_handed_off: false,
                    open_pending: false,
                    closing: false,
                }),
                executor: Arc::new(ActorTransportExecutor(handle.clone())),
                transport: Some(transport),
                startup: None,
                capacity: None,
                membership: None,
                owner_controls: None,
                ordered_observations: Some(
                    OrderedObservationHost::new(ingress.clone()).with_read_progress([progress]),
                ),
            },
            &mut funding,
        )
        .unwrap_or_else(|_| panic!("original subscription host"));
    assert!(funding.is_none());

    let drive = || {
        while let Some(turn) = runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };
    let post = || inbox.submit_command(&command);
    let next_fact = || {
        while let Some((envelope, input)) = inbox.try_commit_envelope() {
            assert_eq!(envelope.source, inbox.source());
            let AttemptInput::Executor(fact) = input else {
                panic!("expected an original subscription fact");
            };
            if let ExecutorFact::ReadProgress(read) = fact {
                assert_eq!(envelope.class, InputClass::Ordinary);
                assert_eq!(
                    (read.participation, read.generation),
                    (participation, request.generation)
                );
                continue;
            }
            assert_eq!(envelope.class, InputClass::Lifecycle);
            return Some(fact);
        }
        None
    };
    let wait_handoff = |input| async move {
        loop {
            let _ = runtime.poll_one(Tick(1)).unwrap();
            drive();
            match runtime.retire_input(registration.actor(), input) {
                Ok(()) => return,
                Err(ActorTurnError::EffectsRemain | ActorTurnError::InputStillQueued) => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("original subscription effect retirement: {error:?}"),
            }
        }
    };
    let wait = Duration::from_secs(3);
    tokio::time::timeout(wait, wait_handoff(post()))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &original.applied_request(request.generation).unwrap(),
        &request
    ));

    tokio::time::timeout(wait, async {
        loop {
            drive();
            if let Some(fact) = next_fact() {
                let ExecutorFact::SubscriptionProgress(phase) = fact else {
                    panic!("original phase: {fact:?}")
                };
                assert_eq!(
                    (phase.participation, phase.generation),
                    (participation, request.generation)
                );
                if phase.opened_at.is_some() {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(peer.subscription_requests().len(), 1);

    tokio::time::timeout(wait, wait_handoff(post()))
        .await
        .unwrap();
    tokio::time::timeout(wait, async {
        while !Arc::ptr_eq(
            &original.applied_request(request.generation).unwrap(),
            &adjusted,
        ) {
            drive();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(peer.subscription_requests().len(), 1);
    assert!(next_fact().is_none());

    tokio::time::timeout(wait, wait_handoff(post()))
        .await
        .unwrap();
    tokio::time::timeout(wait, async {
        loop {
            drive();
            if let Some(ExecutorFact::SubscriptionExited {
                effect,
                participation: actual,
                generation,
            }) = next_fact()
            {
                assert_eq!(
                    (effect, actual, generation),
                    (id(1), participation, request.generation)
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    command.body_exited().unwrap();
    command.captures_exited().unwrap();
    command_payer.finish().unwrap();
    assert!(
        scope
            .reap_observed_header(&command.retirement_observer())
            .unwrap()
    );
    exit.request_close().unwrap();
    tokio::time::timeout(wait, async {
        while !sources_retired.load(Ordering::Acquire) {
            drive();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(original.scope().is_err());
    assert!(original.applied_request(request.generation).is_err());
    drop((ingress, original));
    let observation_capacity = || {
        ledger
            .try_capacity_dimensions([OBSERVATION_WINDOW_BYTES, OBSERVATION_BACKING_BYTES])
            .unwrap()
            .unwrap()
            .used
    };
    assert_eq!(observation_capacity(), [0, backing_bytes]);
    assert!(!host.is_empty());
    drop(weak_ingress);
    assert_eq!(observation_capacity(), [0, 0]);
    drive();
    tokio::time::timeout(wait, exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(host.is_empty() && runtime.is_empty());
    assert!(scope.snapshot().is_err() && control_scope.snapshot().is_err());
    launcher.body_exited().unwrap();
    launcher.captures_exited().unwrap();
    launcher_payer.finish().unwrap();
    assert!(
        outer
            .reap_observed_header(&launcher.retirement_observer())
            .unwrap()
    );
    outer.close_external().unwrap();
    ledger.remove_scope(outer.id()).unwrap();
    assert!(ledger.is_empty());
    drop((cohort, capacity, wc_scope));
    wc::finish(wc_owner, wc_root, wc_driver);
    data.drain_native_outgoing_until(Instant::now() + wait)
        .await
        .unwrap();
    assert!(peer.settle().await);
}
