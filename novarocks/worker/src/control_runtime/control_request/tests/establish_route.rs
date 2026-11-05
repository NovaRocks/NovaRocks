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

//! Actual paid Root/Participation route custody. The metadata-only Participation bootstrap
//! never invokes Native materialization or invents a successful host result.
mod deferred;
mod late_return;
mod owner_turn_fairness;
mod partial;
use super::*;
use crate::{
    control_runtime::{
        control_internal::{CONTROL_INTERNAL_STORAGE, InternalAdvance},
        effect_dispatch::EffectRef,
        establish_ingress::{EstablishDisposalJob, EstablishIngressOutcome},
        participation_host::{
            PARTICIPATION_BOOTSTRAP_STORAGE, ParticipationHostBinding, ParticipationHostRecord,
        },
    },
    control_runtime_policy::{ControlRuntimePolicy, IngressPositionLimits},
};
use novarocks_control_runtime::{actor_turn::InputPhase, publication::PublishedSnapshot};
use novarocks_execution_contract::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, CredentialUpdate, EstablishParticipation, ParticipationState,
};
use novarocks_worker_protocol::{
    control::EstablishSemanticFacts,
    lease::LeaseBounds,
    participation::{
        EstablishReplayResult, ParticipationConfig, ParticipationCore, ParticipationEffect,
    },
};
use std::{
    sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

// Serial component-test driver only. Production must release RootHostCell's
// guard before bind, as the real owner-turn regression below verifies.
fn advance_route_bindings(
    root: &mut crate::control_runtime::process_control::ProcessControlOwner,
    budget: usize,
) -> Result<usize, ControlRequestError> {
    let mut routed = 0;
    for _ in 0..budget {
        let Some(original) = root.take_ready_route_binding()? else {
            break;
        };
        original.bind()?;
        routed += 1;
    }
    Ok(routed)
}

fn op(value: u8) -> TaskOperationId {
    let mut bytes = [value; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    TaskOperationId::try_from_bytes(bytes).unwrap()
}
fn at(nanos: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_nanos(nanos))
}
fn participation_actor() -> ActorId {
    ActorId::new(4).unwrap()
}
fn semantics() -> EstablishSemanticFacts {
    EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([1; 16]),
        shared_filter: ContentFingerprint::from_bytes([1; 16]),
        query_options: ContentFingerprint::from_bytes([1; 16]),
        credential_domain_id: CredentialDomainId::new(1),
        credential_epoch: CredentialEpoch::FIRST,
        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    }
}
struct DropGate {
    entered: Barrier,
    resume: Barrier,
}
impl DropGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
        })
    }
}
struct Content {
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    gate: Option<Arc<DropGate>>,
}
impl std::fmt::Debug for Content {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("opaque Debug is not a metadata operation")
    }
}
impl CodecOwnedContent for Content {
    fn fingerprint(&self) -> ContentFingerprint {
        self.getters.fetch_add(1, Ordering::SeqCst);
        ContentFingerprint::from_bytes([1; 16])
    }
    fn encoded_len(&self) -> usize {
        panic!("this actual ingress only reads the frozen fingerprint")
    }
}
impl Drop for Content {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.entered.wait();
            gate.resume.wait();
        }
    }
}
struct Credential(Arc<AtomicUsize>);
impl ConfidentialContent for Credential {
    fn encoded_len(&self) -> usize {
        panic!("no confidential bytes are projected")
    }
    fn matches(&self, _: &dyn ConfidentialContent) -> bool {
        panic!("a mismatch or an unmaterialized Participation cannot claim a material comparison")
    }
}
impl Drop for Credential {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ConflictObservation {
    participation: ParticipationRef,
    caller: thread::ThreadId,
    calls: AtomicUsize,
}
impl crate::TaskProtocolObserver for ConflictObservation {
    fn observe(&self, event: crate::TaskProtocolEvent) {
        assert_ne!(
            thread::current().id(),
            self.caller,
            "the actual observer callback runs on the physical host"
        );
        assert!(
            matches!(event,
            crate::TaskProtocolEvent::ParticipationEstablishConflict { participation }
            if participation == self.participation),
            "the original replay owes exactly its ParticipationConflict event"
        );
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
    }
}
impl crate::TaskResultLifecycle for ConflictObservation {
    fn discard_task(&self, _: novarocks_execution_contract::TaskIdentity) {
        panic!("this Participation-only replay admitted no Task result");
    }
    fn retire_task_result(&self, _: novarocks_execution_contract::TaskIdentity) {
        panic!("this Participation-only replay admitted no Task result");
    }
}
impl crate::TaskExecutionMetrics for ConflictObservation {
    fn record_task_created(&self) {
        panic!("this Participation-only replay admitted no Task");
    }
}

struct RouteFixture {
    f: Fixture,
    ticket: AdmissionTicketId,
    creator: InputId,
    creator_handle: Option<ControlRequestHandle>,
    host_storage: StorageNeed,
    snapshot: PublishedSnapshot<ParticipationState>,
    removed: Option<ParticipationHostRecord>,
    pending_bootstrap: Option<(EffectTicket<WorkerAction>, ParticipationHostBinding)>,
}
struct RouteStarted {
    input: InputId,
    handle: ControlRequestHandle,
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    refusal: Option<ControlRequestError>,
}
impl RouteFixture {
    fn new() -> Self {
        Self::with_geometry(2, StorageNeed::default(), false)
    }
    fn with_geometry(positions: usize, extra: StorageNeed, deferred: bool) -> Self {
        let policy = ControlRuntimePolicy::try_new(IngressPositionLimits {
            ordinary_running: positions - 1,
            ordinary_waiting: 0,
            control_running: 1,
            control_waiting: 0,
            max_batch_items: 1,
            root_read_holders: 1,
        })
        .unwrap();
        let participation_storage = PARTICIPATION_BOOTSTRAP_STORAGE.resources().unwrap();
        let participation = participation_storage.clone();
        let storage = AdmissionStorage {
            loading: policy.loading.resources().unwrap(),
            accepted: policy.accepted.resources().unwrap(),
            // The admission port adds the real Participation unit once;
            // this field contains only its already funded metadata.
            participation: participation_storage,
        };
        let host_storage = StorageNeed {
            inputs: 1,
            batches: 1,
            effects: ControlCore::MAX_STEP_WORK,
            work: 1,
            ..StorageNeed::default()
        };
        let mut additional = participation
            .checked_add(&CONTROL_INTERNAL_STORAGE.resources().unwrap())
            .unwrap()
            .checked_add(&host_storage.resources().unwrap())
            .unwrap()
            .checked_add(&extra.resources().unwrap())
            .unwrap();
        for _ in 1..positions {
            additional = additional
                .checked_add(
                    &ESTABLISH_REQUEST_STORAGE
                        .checked_sub(CONTROL_REQUEST_STORAGE)
                        .unwrap()
                        .resources()
                        .unwrap(),
                )
                .unwrap();
        }
        let mut f =
            Fixture::with_control_storage(positions, positions, storage.clone(), additional);
        f.root.enable_internal_source().unwrap();
        assert_eq!(f.root.advance_internal().unwrap(), InternalAdvance::Idle);
        let snapshot = PublishedSnapshot::new(
            &f.root.resources().scope,
            Arc::new(ParticipationState::Absent),
        );
        let mut this = Self {
            f,
            ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            creator: InputId::new(1).unwrap(),
            creator_handle: None,
            host_storage,
            snapshot,
            removed: None,
            pending_bootstrap: None,
        };
        let (acquire, mut handle) = this
            .f
            .root
            .admit_request(ActorId::new(2).unwrap(), op(10), at(100_000_000_000))
            .unwrap();
        this.f
            .root
            .submit_request(
                acquire,
                ControlInput::Acquire {
                    request: AcquireParticipationAdmissionTicket::new(
                        op(10),
                        this.f.participation,
                        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                        NativeCompatibilityId::new([3; 32]),
                        this.f.capability,
                    ),
                    ticket: this.ticket,
                },
            )
            .unwrap();
        this.f.runtime.poll_one(Tick(0)).unwrap();
        this.drain_root();
        let delivery = handle
            .try_take()
            .expect("the actual granted admission reply");
        assert!(
            matches!(delivery.body(),ControlReplyBody::Admission(Ok(receipt)) if receipt.ticket_id()==this.ticket)
        );
        drop(delivery);
        drop(handle);
        assert_eq!(this.f.root.reap_ready_requests(32).unwrap(), 1);
        let (creator, handle) = this
            .f
            .root
            .admit_request(ActorId::new(2).unwrap(), op(11), at(100_000_000_000))
            .unwrap();
        this.creator = creator;
        this.creator_handle = Some(handle);
        this.f
            .root
            .submit_request(
                creator,
                ControlInput::Establish {
                    request: op(11),
                    participation: this.f.participation,
                    ticket: this.ticket,
                    payload: PayloadId(100),
                    semantics: semantics(),
                    generation: generation(),
                },
            )
            .unwrap();
        this.f.runtime.poll_one(Tick(1)).unwrap();
        this.f.root.index_control_effects(64).unwrap();
        let mut materialize = this.f.root.take_control_effect(64).unwrap().unwrap();
        if matches!(&materialize.effect, WorkerAction::AdmissionEpoch(_)) {
            // The same committed directory inventory is published before the
            // original Materialize effect, just as the production driver does.
            this.complete_metadata(materialize);
            materialize = this.f.root.take_control_effect(64).unwrap().unwrap();
        }
        let establishment = match &materialize.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { establishment, .. },
                ..
            } => *establishment,
            _ => panic!("the original committed Materialize"),
        };
        let binding = ParticipationHostBinding {
            actor: participation_actor(),
            core: ParticipationCore::new(
                this.f.participation,
                generation(),
                establishment,
                ParticipationConfig {
                    max_tasks: 1,
                    max_creation_waiters: positions,
                    max_pending_requests: positions,
                    max_preparing: 1,
                    max_task_bytes: 1024,
                    step_budget: ParticipationCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                    request_horizon: crate::RequestHorizon::DEFAULT,
                },
            )
            .unwrap(),
            policy,
            storage,
            max_live_tasks: 1,
            max_live_roots: 1,
        };
        this.pending_bootstrap = Some((materialize, binding));
        if !deferred {
            this.adopt_pending_participation();
        }
        this
    }
    fn adopt_pending_participation(&mut self) {
        let (materialize, binding) = self.pending_bootstrap.take().unwrap();
        self.f
            .root
            .adopt_participation(&materialize, binding)
            .unwrap();
        // Actual metadata handoff; no Native materialization job was invoked.
        self.f.complete_body(materialize);
        self.f.root.note_control_input_progress(self.creator);
    }
    fn complete_metadata(&mut self, ticket: EffectTicket<WorkerAction>) {
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.f.complete_body(ticket);
        self.f.root.control_completion_observed(reference).unwrap();
    }
    fn drain_root(&mut self) {
        for _ in 0..64 {
            self.f.root.index_control_effects(64).unwrap();
            while let Some(ticket) = self.f.root.take_control_effect(64).unwrap() {
                match &ticket.effect {
                    WorkerAction::Control {
                        effect: ControlEffect::WakeInternal,
                        admitted: None,
                    } => self.f.root.handoff_internal_wake(ticket).unwrap(),
                    WorkerAction::AdmissionEpoch(_) => self.complete_metadata(ticket),
                    WorkerAction::ControlReply(_) => {
                        self.f.root.publish_request_reply(ticket).unwrap()
                    }
                    WorkerAction::Control {
                        effect: ControlEffect::AdmissionReply { .. },
                        ..
                    } => {
                        self.complete_metadata(ticket);
                    }
                    WorkerAction::Control {
                        effect:
                            ControlEffect::Reply { .. } | ControlEffect::DomainFailureReply { .. },
                        ..
                    } => self.complete_metadata(ticket),
                    WorkerAction::Control {
                        effect: ControlEffect::Route { state, .. },
                        ..
                    } => {
                        drop(
                            self.f
                                .runtime
                                .commit_snapshot(&ticket, &self.snapshot, Arc::new(*state))
                                .unwrap(),
                        );
                        self.f.root.control_publication_committed(&ticket).unwrap();
                        self.f.complete_body(ticket);
                    }
                    WorkerAction::Control {
                        effect: ControlEffect::RetireReservation(id),
                        ..
                    } => {
                        if let Some(owner) = self.removed.as_mut() {
                            assert_eq!(owner.admission(), *id);
                            owner.authorize_parent_retirement(&ticket).unwrap();
                        } else {
                            let address = self
                                .f
                                .root
                                .service()
                                .participation(self.f.participation)
                                .unwrap();
                            let mut owner = address.lock().unwrap();
                            assert_eq!(owner.admission(), *id);
                            owner.authorize_parent_retirement(&ticket).unwrap();
                        }
                        self.complete_metadata(ticket);
                    }
                    WorkerAction::Control {
                        effect: ControlEffect::RemoveHost { .. },
                        ..
                    } => {
                        let owner = self
                            .f
                            .root
                            .service()
                            .take_removing_participation(&ticket)
                            .unwrap()
                            .unwrap();
                        assert!(self.removed.replace(owner).is_none());
                        self.f.complete_body(ticket);
                    }
                    other => panic!("unexpected component Root effect: {other:?}"),
                }
            }
            match self.f.root.advance_internal().unwrap() {
                InternalAdvance::Idle | InternalAdvance::Waiting(_) => return,
                InternalAdvance::Submitted(_) => {
                    // The actual runtime clamps this to its current observed
                    // time; this does not advance any deadline or horizon.
                    assert!(self.f.runtime.poll_one(Tick(0)).unwrap());
                }
            }
        }
        panic!("the original bounded Root metadata source must settle its ready turn");
    }
    fn begin_route(
        &mut self,
        operation: TaskOperationId,
        deadline: u64,
        gate: Option<Arc<DropGate>>,
    ) -> (
        InputId,
        ControlRequestHandle,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let route = self.start_route(operation, deadline, gate);
        assert!(
            route.refusal.is_none(),
            "actual route initialization: {:?}",
            route.refusal
        );
        (route.input, route.handle, route.getters, route.drops)
    }
    fn start_route(
        &mut self,
        operation: TaskOperationId,
        deadline: u64,
        gate: Option<Arc<DropGate>>,
    ) -> RouteStarted {
        let (input, handle) = self
            .f
            .root
            .admit_establish_request(ActorId::new(2).unwrap(), operation, at(deadline))
            .unwrap();
        let getters = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let content = |gate| {
            Arc::new(Content {
                getters: getters.clone(),
                drops: drops.clone(),
                gate,
            }) as Arc<dyn CodecOwnedContent>
        };
        let request = EstablishParticipation::new(
            operation,
            self.f.participation,
            self.ticket,
            content(gate),
            content(None),
            content(None),
            CredentialUpdate::new(
                CredentialDomainId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(Credential(drops.clone())),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        );
        let job = self
            .f
            .root
            .begin_establish_ingress(input, request, PayloadId(200))
            .unwrap();
        thread::spawn(move || job.run()).join().unwrap();
        let projection = match self.f.root.take_establish_ingress(input).unwrap().unwrap() {
            EstablishIngressOutcome::Projected(projection) => projection,
            _ => panic!("actual getter projection"),
        };
        self.f
            .root
            .submit_request(
                input,
                ControlInput::Establish {
                    request: projection.operation,
                    participation: projection.participation,
                    ticket: projection.ticket,
                    payload: projection.payload,
                    semantics: projection.semantics,
                    generation: generation(),
                },
            )
            .unwrap();
        self.f.runtime.poll_one(Tick(2)).unwrap();
        self.f.root.index_control_effects(64).unwrap();
        let route = self.f.root.take_control_effect(64).unwrap().unwrap();
        assert!(
            matches!(&route.effect,WorkerAction::Control{effect:ControlEffect::RouteEstablish{request,..},..} if request.request.token==input)
        );
        self.f.root.handoff_establish_route(route).unwrap();
        let refusal = advance_route_bindings(&mut self.f.root, 1).err();
        RouteStarted {
            input,
            handle,
            getters,
            drops,
            refusal,
        }
    }
    fn probe(&mut self, input: InputId) -> EstablishReplayResult {
        self.f.runtime.poll_one(Tick(3)).unwrap();
        let runtime = self.f.runtime.clone();
        let host_address = self
            .f
            .root
            .service()
            .participation(self.f.participation)
            .unwrap();
        let mut host = host_address.lock().unwrap();
        host.dispatch.index_committed(&runtime, 64).unwrap();
        let probe = host.dispatch.take_ready(&runtime, 64).unwrap().unwrap();
        let result = match &probe.effect {
            WorkerAction::Participation(ParticipationEffect::EstablishProbed {
                request,
                result,
                ..
            }) if request.token == input => result.clone(),
            _ => panic!("actual Participation owner probe"),
        };
        drop(host);
        self.assert_original_probe_author(&probe, input);
        host_address
            .lock()
            .unwrap()
            .handoff_establish_probe(probe)
            .unwrap();
        result
    }
    fn assert_original_probe_author(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        input: InputId,
    ) {
        let original = &self.f.root.resources().requests[&input];
        let original_request = original.request();
        let route = original
            .route
            .as_ref()
            .expect("the same original R route")
            .clone();
        assert_eq!(route.request(), original_request);
        assert_eq!(
            ticket.descriptor.work.scope,
            self.f.root.resources().scope.id()
        );
        assert_ne!(
            ticket.descriptor.work, original_request.author,
            "the replay leg is its own originally prepaid role, not a new R"
        );
        let actual_pins = self
            .f
            .runtime
            .input_header_authors(
                participation_actor(),
                ticket.input,
                2 * crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS + 2,
            )
            .unwrap();
        assert!(
            actual_pins.contains(&ticket.descriptor.work),
            "the original probe batch really holds that financial header"
        );
        let address = self
            .f
            .root
            .service()
            .participation(self.f.participation)
            .unwrap();
        let mut host = address.lock().unwrap();
        assert!(route.target_matches(
            &self.f.runtime,
            host.registration(),
            self.f.participation,
            actor()
        ));
        assert!(Arc::ptr_eq(host.establish_route(input).unwrap(), &route));
        let execution_scope = host.resources_for_test().scope.as_ref().unwrap().clone();
        assert_ne!(
            ticket.descriptor.work.scope,
            execution_scope.id(),
            "the hot Participation scope does not fund its original R tail"
        );
    }
    fn finish_conflict_observation(&mut self, input: InputId) {
        let original = &self.f.root.resources().requests[&input];
        assert!(original.protocol_observation.has_pending());
        assert!(!original.protocol_observation.is_idle());
        assert!(original.endpoint.external_ended());
        let paid = self.f.ledger.used(Dimension::Requests.id());
        assert!(paid > 0);
        assert_eq!(
            self.f.root.reap_ready_requests(32).unwrap(),
            0,
            "raw disposal and external reply exit do not finish the real observer"
        );
        assert_eq!(self.f.ledger.used(Dimension::Requests.id()), paid);
        assert!(self.f.root.resources().requests.contains_key(&input));
        let observed = Arc::new(ConflictObservation {
            participation: self.f.participation,
            caller: thread::current().id(),
            calls: AtomicUsize::new(0),
        });
        let ports = crate::TaskExecutionPorts::new(
            observed.clone(),
            observed.clone(),
            observed.clone(),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new()),
        );
        let original_job = self
            .f
            .root
            .take_protocol_observation_for_test(input, &ports)
            .unwrap()
            .expect("the same original R's already staged physical observation");
        let original = &self.f.root.resources().requests[&input];
        assert!(!original.protocol_observation.is_idle());
        assert_eq!(self.f.ledger.used(Dimension::Requests.id()), paid);
        thread::spawn(move || original_job.run()).join().unwrap();
        assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
        assert!(
            self.f.root.resources().requests[&input]
                .protocol_observation
                .is_idle()
        );
    }
    fn dispose_participation_metadata(&mut self) {
        let runtime = self.f.runtime.clone();
        let ledger = self.f.ledger.clone();
        let root_scope = self.f.root.resources().scope.clone();
        let host_address = self
            .f
            .root
            .service()
            .participation(self.f.participation)
            .unwrap();
        let mut host = host_address.lock().unwrap();
        let resources = host.resources_for_test();
        assert!(resources.routes.is_empty());
        if resources.scope.is_none() {
            return;
        }
        runtime.close_timer_source(participation_actor()).unwrap();
        runtime
            .close_actor_admission(participation_actor())
            .unwrap();
        let mut other_claims = Vec::new();
        while let Some(original_host_turn) = runtime.claim_host_actor() {
            let ready = original_host_turn.actor();
            if ready == participation_actor() {
                assert!(runtime.pending_batches(ready, None, 1).unwrap().is_empty());
                original_host_turn.release(false).unwrap();
            } else {
                // This Participation's metadata exit does not require another
                // actor's committed input to retire. Retain each other
                // scheduling claim once, then restore its actual ready edge.
                other_claims.push(original_host_turn);
            }
        }
        for original_host_turn in other_claims {
            original_host_turn.release(true).unwrap();
        }
        assert!(resources.owner_control.close_for_teardown().unwrap());
        assert!(resources.internal.close_for_teardown().unwrap());
        runtime.seal_actor(participation_actor()).unwrap();
        let scope = resources.scope.take().unwrap();
        scope.close_external().unwrap();
        for role in resources.metadata_resources.iter_mut().rev() {
            role.work.body_exited().unwrap();
            role.work.captures_exited().unwrap();
        }
        ledger.reap_scope_ready(scope.id(), 32).unwrap();
        for role in &mut resources.metadata_resources {
            role.capacity
                .release_quota(&role.capacity.resources().unwrap().quota)
                .unwrap();
            role.capacity.finish().unwrap();
        }
        drop(runtime.remove_actor(participation_actor()).unwrap());
        resources.registered = false;
        assert!(scope.snapshot().unwrap().retirement_ready);
        ledger.remove_scope(scope.id()).unwrap();
        ledger.reap_scope_ready(root_scope.id(), 32).unwrap();
    }
    fn finish_model_refusal(mut self) {
        assert!(self.f.root.resources().requests.contains_key(&self.creator));
        self.dispose_participation_metadata();
        let refusal = self.f.host_input(
            ControlInput::EstablishmentDecided {
                participation: self.f.participation,
                decision:
                    novarocks_worker_protocol::participation::ParticipationEstablishmentDecision {
                        state: ParticipationState::Aborting,
                        outcome: Ok(OperationOutcome::OwnerFailed),
                        acknowledgement: None,
                        decided_at: MonotonicInstant::from_origin(Duration::from_nanos(100)),
                    },
            },
            self.host_storage,
        );
        self.f.runtime.poll_one(Tick(100)).unwrap();
        self.drain_root();
        self.f.finish_host_input(refusal);
        let mut handle = self.creator_handle.take().unwrap();
        let delivery = handle.try_take().unwrap();
        assert!(matches!(
            delivery.body(),
            ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
        ));
        drop(delivery);
        drop(handle);
        assert_eq!(self.f.root.reap_ready_requests(32).unwrap(), 1);
        let retained = self.f.host_input(
            ControlInput::ExecutionRetired {
                participation: self.f.participation,
                retirement:
                    novarocks_worker_protocol::retention::ParticipationRetirementIdentity::Protocol {
                        close_epoch: generation(),
                    },
            },
            self.host_storage,
        );
        self.f.runtime.poll_one(Tick(101)).unwrap();
        self.drain_root();
        self.f.finish_host_input(retained);

        // Expiry is reported by the original Participation lifecycle, not a second
        // Root retention timer. This model has already removed that child.
        let expired = self.f.host_input(
            ControlInput::ParticipationExpired {
                participation: self.f.participation,
                retirement:
                    novarocks_worker_protocol::retention::ParticipationRetirementIdentity::Protocol {
                        close_epoch: generation(),
                    },
            },
            self.host_storage,
        );
        self.f.runtime.poll_one(Tick(11_000_000_000)).unwrap();
        self.drain_root();
        self.f.finish_host_input(expired);
        let removed = self.f.host_input(
            ControlInput::HostRemoved {
                participation: self.f.participation,
            },
            self.host_storage,
        );
        self.f.runtime.poll_one(Tick(11_000_000_001)).unwrap();
        self.drain_root();
        self.f.finish_host_input(removed);
        let (mut resources, tasks) = self.removed.take().unwrap().into_resources();
        assert!(tasks.is_empty());
        assert!(resources.routes.is_empty());
        assert!(resources.scope.is_none());
        assert!(!resources.registered);
        assert!(resources.scope.is_none());
        resources.admission.work.body_exited().unwrap();
        resources.admission.work.captures_exited().unwrap();
        self.f
            .ledger
            .reap_scope_ready(self.f.root.resources().scope.id(), 32)
            .unwrap();
        resources
            .admission
            .capacity
            .release_quota(&resources.admission.capacity.resources().unwrap().quota)
            .and_then(|()| resources.admission.capacity.finish())
            .unwrap();
        self.f.runtime.close_timer_source(actor()).unwrap();
        self.drain_root();
        assert!(self.f.requests.is_empty());
        assert!(self.f.root.resources().requests.is_empty());
        assert_eq!(self.f.root.request_ready().registered(), 0);
        let (_service, mut root_resources) = self.f.root.into_parts();
        root_resources.scope.close_external().unwrap();
        root_resources
            .internal
            .as_mut()
            .expect("the original paid Root internal source")
            .close_after_drain()
            .unwrap();
        self.f.runtime.close_actor_admission(actor()).unwrap();
        while let Some(original_host_turn) = self.f.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        self.f.runtime.seal_actor(actor()).unwrap();
        for role in root_resources.metadata_resources.iter_mut().rev() {
            role.work.body_exited().unwrap();
            role.work.captures_exited().unwrap();
        }
        self.f
            .ledger
            .reap_scope_ready(root_resources.scope.id(), 32)
            .unwrap();
        drop(self.f.runtime.remove_actor(actor()).unwrap());
        for role in root_resources.metadata_resources.iter_mut().rev() {
            role.capacity
                .release_quota(&role.capacity.resources().unwrap().quota)
                .unwrap();
            role.capacity.finish().unwrap();
        }
        self.f
            .ledger
            .remove_scope(root_resources.scope.id())
            .unwrap();
        assert!(self.f.ledger.is_empty());
    }
}

#[test]
fn the_participation_leg_retires_before_root_consumes_its_original_routed_reply() {
    let mut f = RouteFixture::new();
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 100, None);
    assert!(matches!(
        f.probe(input),
        EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict
    ));
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    let returns = f.f.runtime.input_inventory(actor(), None, 16).unwrap();
    assert_eq!(
        returns
            .iter()
            .filter(|input| input.source == participation_actor()
                && input.phase == InputPhase::Filled
                && input.standing_input_id.is_some())
            .count(),
        1
    );
    f.dispose_participation_metadata();
    assert!(f.f.runtime.snapshot(participation_actor()).is_err());
    assert!(f.f.root.resources().requests.contains_key(&input));
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 2);
    assert!(
        handle.try_take().is_none(),
        "the original Root return has not run"
    );
    f.f.runtime.poll_one(Tick(4)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(
        matches!(&reply.effect,WorkerAction::ControlReply(reply) if reply.request.token==input && reply.result==RequestResult::Outcome(OperationOutcome::ParticipationConflict))
    );
    let disposal =
        f.f.root
            .begin_rejected_ingress_disposal(&reply)
            .unwrap()
            .unwrap();
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 2);
    assert!(thread::spawn(move || disposal.run()).join().unwrap());
    f.finish_conflict_observation(input);
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 1);
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    f.finish_model_refusal();
}

#[test]
fn a_root_timeout_cannot_dispose_raw_until_the_real_participation_leg_and_cleanup_job_exit() {
    let mut f = RouteFixture::new();
    let gate = DropGate::new();
    let (input, mut handle, getters, drops) = f.begin_route(op(11), 10, Some(gate.clone()));
    assert_eq!(f.probe(input), EstablishReplayResult::AwaitMaterialization);
    assert_eq!(f.f.runtime.advance_time(Tick(10), 2).unwrap(), 1);
    f.f.runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    assert!(
        matches!(&cancel.effect,WorkerAction::Control{effect:ControlEffect::CancelEstablishRoute{request,..},..} if request.token==input)
    );
    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let premature_disposal = f.f.root.take_retired_route_disposal(input);
    let rejected_premature_disposal = premature_disposal.is_none();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(
        matches!(&reply.effect,WorkerAction::ControlReply(reply) if reply.request.token==input && reply.result==RequestResult::TimeoutWhileAccepted)
    );
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    f.f.runtime.retire_input(actor(), timer_input).unwrap();
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    let disposal: EstablishDisposalJob = premature_disposal
        .or_else(|| f.f.root.take_retired_route_disposal(input))
        .expect("the same original source after actual Participation retirement");
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cleanup = thread::spawn(move || disposal.run());
    gate.entered.wait();
    let retained = f.f.root.reap_ready_requests(32);
    let paid = f.f.ledger.used(Dimension::Requests.id()) == 2;
    let present = f.f.root.resources().requests.contains_key(&input);
    gate.resume.wait();
    let exited = cleanup.join().unwrap();
    let retired = f.f.root.reap_ready_requests(32).unwrap();
    assert_eq!(retained.unwrap(), 0);
    assert!(rejected_premature_disposal);
    assert!(paid);
    assert!(present);
    assert!(exited);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 1);
    f.finish_model_refusal();
}
