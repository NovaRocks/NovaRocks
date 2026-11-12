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
use novarocks_workload_control::{CancellationReason, WorkClass, WorkError, WorkRequest};
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
        turn: &ActorHostTurn<FrontendActor>,
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
        if turn.was_notified() || released {
            self.subscriptions.request_retirement_scan();
        }
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
async fn subscription_staging_retires_only_its_unpublished_children() {
    let (wc_owner, mut wc_driver, capacity) = wc::fixture();
    let wc_root = wc_owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = wc_root.owner.scope();
    wc::bind(&scope, &mut wc_driver);
    let cohort = wc::cohort(&scope, 1, &mut wc_driver);
    let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut peer = Loopback::start().await;
    let execution = QueryExecutionId::new(QueryId::new(17, 2), AttemptId::new(1).unwrap()).unwrap();
    let frontend = FrontendProcessId::new_v7();
    let mut processes = [BackendProcessId::new_v7(), BackendProcessId::new_v7()];
    processes.sort_unstable();
    let backends: Arc<[BackendProcessDescriptor]> =
        Arc::from(processes.map(|process| peer.descriptor(process)));
    let roster = backends
        .iter()
        .map(|backend| FrozenMember {
            participation: ParticipationRef::new(execution, frontend, backend.process_id()),
            endpoint: backend.endpoint().clone(),
        })
        .collect::<Vec<_>>();
    let compatibility = backends[0].native_compatibility_id();
    assert!(matches!(
        stage_subscription_endpoints(
            &roster,
            FrozenSubscriptionStageInputs {
                backends: &backends,
                compatibility,
                data: &data,
                scope: &scope,
                cohort: &cohort,
                execution,
            }
        ),
        Err(SubscriptionEndpointError::Source {
            participation,
            original: SubscriptionSourceError::Work(WorkError::Capacity(_)),
        }) if participation == roster[1].participation
    ));
    assert_eq!(cohort.remaining(), 0);
    wc::drive(&mut wc_driver);
    assert_eq!(wc_owner.snapshot().scopes.len(), 1);

    let pending_cohort = wc::cohort(&scope, 2, &mut wc_driver);
    let one_backend = Arc::from([backends[0].clone()]);
    let pending = stage_subscription_endpoints(
        &roster[..1],
        FrozenSubscriptionStageInputs {
            backends: &one_backend,
            compatibility,
            data: &data,
            scope: &scope,
            cohort: &pending_cohort,
            execution,
        },
    )
    .unwrap();
    assert_eq!(wc_owner.snapshot().scopes.len(), 2);
    assert_eq!(pending_cohort.remaining(), 1);
    wc_root.owner.cancel(CancellationReason::Requested);
    drop(pending);
    wc::drive(&mut wc_driver);
    assert_eq!(wc_owner.snapshot().scopes.len(), 1);
    assert_eq!(pending_cohort.remaining(), 1);
    drop((pending_cohort, cohort, capacity, scope));
    wc::finish(wc_owner, wc_root, wc_driver);
    assert!(peer.settle().await);
}

#[tokio::test]
async fn installed_subscription_uses_one_native_stream_and_reports_exact_actual_exit() {
    run_installed_subscription(false).await;
}

#[tokio::test]
async fn installed_subscription_progress_and_actual_exit_survive_full_ordinary() {
    run_installed_subscription(true).await;
}

async fn run_installed_subscription(saturate_ordinary: bool) {
    use crate::{
        control_runtime::membership::{
            AttemptMembershipSource, MembershipRegistry, MembershipTarget, membership_input,
        },
        native::transport_capacity::{
            NativeTransportCapacity, TransportCapacityLane, TransportCapacityRefusal,
        },
        task_execution::host::{
            adapter::AttemptCapacitySource, geometry::AttemptHostGeometry,
            startup::AttemptStartupSource,
        },
    };
    use novarocks_attempt_protocol::{config::CoordinationBudgets, input::ObservationEvent};
    use novarocks_control_runtime::{
        actor_turn::InputPhase, coalescing_input::CoalescingInput, history::HistoryCapture,
        obligation::WorkObligation, transaction::RuntimeError,
    };
    use novarocks_control_runtime_core::history::HistoryFact;
    use novarocks_execution_contract::RuntimeEndpoint;
    use novarocks_query_application::api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
    };
    use std::{cell::Cell, num::NonZeroUsize, task::Waker};

    struct CurrentProcess(FrozenMember);
    impl BackendProcessObservationPort for CurrentProcess {
        fn observe_process_at_endpoint(
            &self,
            process: BackendProcessId,
            endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            assert_eq!(process, self.0.participation.backend_process_id());
            assert_eq!(endpoint, &self.0.endpoint);
            Ok(BackendProcessObservation::Current)
        }
    }

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
    let transport_budget = novarocks_task_codec::TransportBudget::DEFAULT;
    let layout = NativeTransportCapacity::from_transport(transport_budget).unwrap();
    let history = saturate_ordinary.then(|| HistoryCapture::new(4096));
    let ledger = if let Some(history) = &history {
        RuntimeLedger::with_keyed_families_and_observation(
            layout.process_limits().checked_add(&resources).unwrap(),
            layout.keyed_families(),
            [],
            Some(history.clone()),
            Default::default(),
        )
        .unwrap()
    } else {
        RuntimeLedger::new(resources.clone())
    };
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
    let handle = tokio::runtime::Handle::current();
    let (wc_owner, mut wc_driver, capacity) = wc::fixture();
    let wc_root = wc_owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let wc_scope = wc_root.owner.scope();
    wc::bind(&wc_scope, &mut wc_driver);
    let cohort = wc::cohort(&wc_scope, 2, &mut wc_driver);
    let data = FrontendDataRuntime::new(handle.clone());
    let mut peer = Loopback::start().await;
    let backends: Arc<[BackendProcessDescriptor]> = Arc::from([peer.descriptor(backend)]);
    let descriptor = &backends[0];
    let frozen = || FrozenSubscriptionStageInputs {
        backends: &backends,
        compatibility: descriptor.native_compatibility_id(),
        data: &data,
        scope: &wc_scope,
        cohort: &cohort,
        execution,
    };
    let roster = [FrozenMember {
        participation,
        endpoint: descriptor.endpoint().clone(),
    }];
    let wrong_roster = [FrozenMember {
        participation,
        endpoint: descriptor.control_endpoint().clone(),
    }];
    assert!(matches!(
        stage_subscription_endpoints(&wrong_roster, frozen()),
        Err(SubscriptionEndpointError::Roster)
    ));
    assert_eq!(cohort.remaining(), 2);
    let refused = stage_subscription_endpoints(&roster, frozen()).unwrap();
    let staged = stage_subscription_endpoints(&roster, frozen()).unwrap();
    let staged_id = staged.entries[0].owner.as_ref().unwrap().scope().id();
    assert_eq!(cohort.remaining(), 0);
    assert_eq!(wc_owner.snapshot().scopes.len(), 3);
    assert!(peer.subscription_requests().is_empty());

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
    let mut effects = vec![EffectBody::OpenSubscription(request.clone())];
    if !saturate_ordinary {
        effects.extend([
            EffectBody::AdjustReadWindow(adjusted.clone()),
            EffectBody::CloseSubscription {
                participation,
                generation: request.generation,
            },
        ]);
    }
    let sources = saturate_ordinary.then(|| {
        AttemptHostGeometry::derive(
            scope.id(),
            NonZeroUsize::MIN,
            CoordinationBudgets::DEFAULT.dispatch,
            &layout,
            &[],
            false,
        )
        .unwrap()
        .sources
    });
    let mailbox = sources.map_or([8; 4], |sources| sources.mailbox);
    let config = ActorConfig {
        mailbox,
        max_batches: if saturate_ordinary {
            mailbox.iter().sum()
        } else {
            4
        },
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: if saturate_ordinary { 16 } else { 8 },
        max_step_work: 1,
    };
    let commands = effects.into_iter().enumerate().map(|(n, body)| {
        (
            AttemptEffectEnvelope::new(AttemptEffect {
                id: id(n as u64 + 1),
                class: EffectClass::Settlement,
                body,
            }),
            None,
        )
    });
    let inbox = match (sources, &history) {
        (Some(sources), Some(history)) => WithdrawalInbox::with_attempt_geometry_and_history(
            &scope,
            commands,
            config,
            sources,
            history.clone(),
        ),
        _ => WithdrawalInbox::with_commands(&scope, commands, mailbox),
    };
    let runtime = inbox.runtime();
    let registration = inbox.registration();
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
    let standing = |source, class, work: &WorkObligation| {
        runtime
            .reserve_admitted_input_for(registration, source, class, work)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let progress_position = standing(inbox.source(), InputClass::Ordinary, observation);
    let progress_id = progress_position.id();
    let progress = ReadProgressInput::new(registration, participation, progress_position)
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
    let source_id = |source: novarocks_attempt_protocol::input::AttemptSourceId| {
        ActorId::new(source.0.get()).unwrap()
    };
    let observation_source =
        sources.map_or_else(|| ActorId::new(4).unwrap(), |s| source_id(s.observation));
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
    assert!(matches!(
        refused.bind(
            &wrong_roster,
            descriptor.native_compatibility_id(),
            &ingress
        ),
        Err(SubscriptionEndpointError::Roster)
    ));
    wc::drive(&mut wc_driver);
    assert_eq!(wc_owner.snapshot().scopes.len(), 2);
    let endpoints = staged
        .bind(&roster, descriptor.native_compatibility_id(), &ingress)
        .unwrap();
    assert_eq!(cohort.remaining(), 0);
    let original = endpoints[&participation].source.clone();
    assert_eq!(original.scope().unwrap().id(), staged_id);
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
    let membership_registry = Arc::new(MembershipRegistry::new());
    let queue_parent = payer.id();
    let ordinary = sources.map(|sources| {
        let (startup, witness) = AttemptStartupSource::new(
            registration.clone(),
            standing(registration.actor(), InputClass::Lifecycle, parent),
        );
        let membership_position =
            standing(source_id(sources.membership), InputClass::Ordinary, parent);
        let membership_id = membership_position.id();
        let member = MembershipTarget::register(
            registration.clone(),
            Arc::from(roster.clone()),
            CoalescingInput::new(membership_position, membership_input),
        );
        let membership =
            AttemptMembershipSource::new(membership_registry.clone(), member.target.clone());
        let capacity_position = standing(source_id(sources.capacity), InputClass::Ordinary, parent);
        let capacity_id = capacity_position.id();
        let registrant = Arc::new(scope.capacity_registrant(Waker::noop().clone()).unwrap());
        let capacity =
            AttemptCapacitySource::new(registration.clone(), capacity_position, registrant.clone());
        let roles = [
            (progress_id, inbox.source()),
            (membership_id, source_id(sources.membership)),
            (capacity_id, source_id(sources.capacity)),
        ];
        (
            (startup, capacity, membership),
            (witness, member, registrant, roles),
        )
    });
    let (startup, capacity_source, membership, ordinary) = match ordinary {
        Some(((startup, capacity, membership), ordinary)) => (
            Some(startup),
            Some(capacity),
            Some(membership),
            Some(ordinary),
        ),
        None => (None, None, None, None),
    };
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
                config,
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
                startup,
                capacity: capacity_source,
                membership,
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
    if let Some((witness, _, _, _)) = &ordinary {
        drive();
        for _ in 0..2 {
            assert!(runtime.poll_one(Tick(1)).unwrap());
            drive();
        }
        assert!(witness.ready_for(registration));
        assert!(!membership_registry.is_empty());
    }
    let post = || inbox.submit_command(&command);
    let lifecycle_filled = || {
        runtime
            .input_inventory(registration.actor(), None, 64)
            .unwrap()
            .into_iter()
            .filter(|row| {
                row.source == inbox.source()
                    && row.class == InputClass::Lifecycle
                    && row.phase == InputPhase::Filled
            })
            .map(|row| row.input)
            .collect::<Vec<_>>()
    };
    let eof_observed = Cell::new(false);
    let next_fact = || {
        while let Some((envelope, input)) = inbox.try_commit_envelope() {
            let AttemptInput::Executor(fact) = input else {
                panic!("expected an original subscription fact");
            };
            if saturate_ordinary && envelope.source == observation_source {
                assert_eq!(envelope.class, InputClass::Lifecycle);
                let ExecutorFact::Observations(batch) = fact else {
                    panic!("expected the ordered EOF from the original stream");
                };
                let [frame] = batch.items() else {
                    panic!("the loopback sent only EOF");
                };
                assert_eq!(
                    (frame.participation, frame.generation),
                    (participation, request.generation)
                );
                assert!(matches!(&frame.event, ObservationEvent::Closed));
                assert!(!eof_observed.replace(true));
                continue;
            }
            assert_eq!(envelope.source, inbox.source());
            if let ExecutorFact::ReadProgress(read) = fact {
                assert_eq!(envelope.class, InputClass::Ordinary);
                assert_eq!(
                    (read.participation, read.generation),
                    (participation, request.generation)
                );
                continue;
            }
            assert_eq!(envelope.class, InputClass::Lifecycle);
            return Some((envelope.input, fact));
        }
        None
    };
    let wait_handoff = |input| async move {
        if saturate_ordinary {
            assert!(runtime.poll_one(Tick(1)).unwrap());
        }
        loop {
            if !saturate_ordinary {
                let _ = runtime.poll_one(Tick(1)).unwrap();
            }
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

    let ordinary_inventory = || {
        runtime
            .standing_input_inventory(registration.actor(), None, 64)
            .unwrap()
            .into_iter()
            .filter(|row| row.class == InputClass::Ordinary)
            .collect::<Vec<_>>()
    };
    let filled_ordinary = || {
        let (_, _, _, roles) = ordinary.as_ref().unwrap();
        let rows = ordinary_inventory();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.len(), mailbox[InputClass::Ordinary.index()]);
        for &(id, source) in roles {
            let row = rows.iter().find(|row| row.standing_input_id == id).unwrap();
            assert_eq!(row.source, source);
            let input = row
                .in_flight
                .expect("the original Ordinary occurrence is filled");
            let custody = runtime
                .input_custody(registration.actor(), input)
                .unwrap()
                .unwrap();
            assert_eq!(
                (
                    custody.class,
                    custody.phase,
                    custody.source,
                    custody.standing_input_id
                ),
                (InputClass::Ordinary, InputPhase::Filled, source, Some(id)),
            );
            assert!(!custody.runtime_metadata);
        }
        rows
    };
    let mut wait_target = None;
    let ordinary_cut = if let Some((_, _, registrant, _)) = &ordinary {
        membership_registry.publish_for_test(1, &CurrentProcess(roster[0].clone()));
        let resources = |items| {
            layout
                .queue_resources(backend, TransportCapacityLane::Ordinary, items, 1)
                .unwrap()
        };
        let blocker = scope
            .reserve_next(
                generation,
                resources(layout.capacity_sources().target_items),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let denial = match scope.reserve_child_next_or_wait(
            generation,
            queue_parent,
            resources(1),
            WorkKind::Business,
            registrant,
        ) {
            Err(RuntimeError::CapacityDenied(denial)) => denial,
            _ => panic!("the original Ordinary queue wait must be denied"),
        };
        assert!(
            matches!(NativeTransportCapacity::classify_denial(denial).unwrap(),
            TransportCapacityRefusal::TargetFull(target) if target == backend)
        );
        wait_target = Some((registrant.id(), denial.wait_target()));
        drop(blocker);
        assert_eq!(ledger.capacity_waiter_count(), 0);
        // The actual stream takes its first original credit before the native
        // opening poll. Leave its genuine resume hint in the paid role.
        tokio::time::timeout(wait, async {
            while !ordinary_inventory()
                .iter()
                .any(|row| row.standing_input_id == progress_id && row.in_flight.is_some())
            {
                drive();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Some(filled_ordinary())
    } else {
        None
    };
    let mut phases = Vec::new();
    let phase_work = if let Some(rows) = &ordinary_cut {
        // Subscription progress and exit share the same original Source as
        // the earlier Ordinary read hint. Observe filled custody before
        // driving that FIFO rather than allowing later Lifecycle to overtake.
        let input = tokio::time::timeout(wait, async {
            loop {
                drive();
                assert_eq!(filled_ordinary(), *rows);
                let filled = lifecycle_filled();
                if let [phase] = filled.as_slice()
                    && peer.subscription_requests().len() == 1
                {
                    return *phase;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let authors = runtime
            .input_header_authors(registration.actor(), input, 4)
            .unwrap();
        let [original] = authors.as_slice() else {
            panic!("the phase input identifies its original subscription Work");
        };
        assert_eq!(original.scope, scope.id());
        Some((input, *original))
    } else {
        tokio::time::timeout(wait, async {
            loop {
                drive();
                if let Some((_, fact)) = next_fact() {
                    let ExecutorFact::SubscriptionProgress(phase) = fact else {
                        panic!("original phase: {fact:?}");
                    };
                    assert_eq!(
                        (phase.participation, phase.generation),
                        (participation, request.generation)
                    );
                    if phase.opened_at.is_some() {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        None
    };
    assert_eq!(peer.subscription_requests().len(), 1);

    if let Some(rows) = &ordinary_cut {
        assert_eq!(filled_ordinary(), *rows);
        assert_eq!(lifecycle_filled(), [phase_work.unwrap().0]);
        // EOF lets the real stream exit while the actor leaves every paid
        // input queued. Posting a command would poll their original FIFO.
        peer.close_held_subscriptions();
    } else {
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
    }
    let queued_exit = if let Some(rows) = &ordinary_cut {
        let (phase, work) = phase_work.unwrap();
        Some(
            tokio::time::timeout(wait, async {
                loop {
                    drive();
                    assert_eq!(filled_ordinary(), *rows);
                    let filled = lifecycle_filled();
                    if let [first, exit] = filled.as_slice() {
                        assert_eq!(*first, phase);
                        assert!(matches!(
                            scope.work_lifecycle(work.work).unwrap(),
                            Some((_, true, true, _)),
                        ));
                        return *exit;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let mut exit_input = None;
    let drained = tokio::time::timeout(wait, async {
        loop {
            drive();
            if let Some((input, fact)) = next_fact() {
                match fact {
                    ExecutorFact::SubscriptionExited {
                        effect,
                        participation: actual,
                        generation,
                    } => {
                        assert_eq!(
                            (effect, actual, generation),
                            (id(1), participation, request.generation)
                        );
                        assert!(exit_input.replace(input).is_none());
                        if let Some(queued) = queued_exit {
                            assert_eq!(input, queued);
                        } else {
                            return;
                        }
                    }
                    ExecutorFact::SubscriptionProgress(phase) if saturate_ordinary => {
                        assert_eq!(
                            (phase.participation, phase.generation),
                            (participation, request.generation)
                        );
                        phases.push(input);
                    }
                    other => panic!("expected an original subscription phase or exit: {other:?}"),
                }
            }
            if let Some(input) = exit_input
                && runtime
                    .input_custody(registration.actor(), input)
                    .unwrap()
                    .is_none()
                && ordinary_inventory()
                    .iter()
                    .all(|row| row.in_flight.is_none())
                && original.applied_request(request.generation).is_err()
                && eof_observed.get()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "original subscription drain stalled: exit={exit_input:?}, exit phase={:?}, ordinary={:?}, retained request={}, observed EOF={}",
        exit_input.and_then(|input| runtime
            .input_custody(registration.actor(), input)
            .unwrap()
            .map(|custody| custody.phase)),
        ordinary_inventory(),
        original.applied_request(request.generation).is_ok(),
        eof_observed.get(),
    );
    if let Some(rows) = &ordinary_cut {
        assert_eq!(phases.first(), Some(&phase_work.unwrap().0));
        let idle = rows
            .iter()
            .copied()
            .map(|mut row| {
                row.in_flight = None;
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(ordinary_inventory(), idle);
    }
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
    assert!(membership_registry.is_empty());
    if let Some(history) = history {
        let history = history.drain(4096);
        assert!(
            history.complete && history.remaining == 0,
            "{:?}",
            history.error
        );
        let once = |predicate: &dyn Fn(&HistoryFact) -> bool| {
            let ordinals = history
                .entries
                .iter()
                .filter(|entry| predicate(&entry.fact))
                .map(|entry| entry.ordinal)
                .collect::<Vec<_>>();
            let [only] = ordinals.as_slice() else {
                panic!("expected one matching original event, got {ordinals:?}");
            };
            *only
        };
        let (queued_phase, original) = phase_work.unwrap();
        let started = once(
            &|fact| matches!(fact, HistoryFact::Started { scope, work } if (*scope, *work) == (original.scope, original.work)),
        );
        let body = once(
            &|fact| matches!(fact, HistoryFact::BodyExited { scope, work } if (*scope, *work) == (original.scope, original.work)),
        );
        let captures = once(
            &|fact| matches!(fact, HistoryFact::CapturesExited { scope, work } if (*scope, *work) == (original.scope, original.work)),
        );
        let retired = once(
            &|fact| matches!(fact, HistoryFact::WorkRetired { scope, work } if (*scope, *work) == (original.scope, original.work)),
        );
        let consumed = |input| {
            once(
                &|fact| matches!(fact, HistoryFact::InputConsumed { target, input: exact } if *exact == input && target.actor == registration.actor() && target.scope_origin == scope.id()),
            )
        };
        let exit = consumed(exit_input.unwrap());
        let phase = consumed(queued_phase);
        assert!(started < body && body < captures && captures < phase && phase < exit);
        assert!(captures < retired);
        let mut preceding = captures;
        for input in phases {
            let next = consumed(input);
            assert!(preceding < next);
            preceding = next;
        }
        for role in ordinary_cut.unwrap() {
            let ordinary = consumed(role.in_flight.unwrap());
            assert!(captures < ordinary);
            if role.standing_input_id == progress_id {
                assert!(
                    ordinary < phase,
                    "the original Source keeps its predecessor first"
                );
            }
        }
        let (registrant, wait) = wait_target.unwrap();
        let registered = once(
            &|fact| matches!(fact, HistoryFact::CapacityWaitRegistered { registrant: id, target } if (*id, *target) == (registrant, wait)),
        );
        let notified = once(
            &|fact| matches!(fact, HistoryFact::CapacityWaitNotified { registrant: id, target } if (*id, *target) == (registrant, wait)),
        );
        assert!(registered < notified && notified < exit);
    }
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
