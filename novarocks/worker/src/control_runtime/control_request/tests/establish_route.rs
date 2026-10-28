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

//! Actual paid Root/Context route custody. The metadata-only Context bootstrap
//! never invokes Native materialization or invents a successful host result.
mod deferred;
mod late_return;
mod owner_turn_fairness;
mod partial;
use super::*;
use crate::{
    control_runtime::{
        context_host::{CONTEXT_BOOTSTRAP_STORAGE, ContextHostBinding, ContextHostRecord},
        effect_dispatch::EffectRef,
        establish_ingress::{EstablishDisposalJob, EstablishIngressOutcome},
    },
    control_runtime_policy::{ControlRuntimePolicy, IngressRoleLimits},
};
use novarocks_control_runtime::{driver::InputPhase, publication::PublishedSnapshot};
use novarocks_execution_contract::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, EstablishQueryContext, QueryContextState,
};
use novarocks_worker_protocol::{
    context::{ContextConfig, ContextCore, ContextEffect, EstablishReplayResult},
    control::EstablishSemanticFacts,
    lease::LeaseBounds,
};
use std::{
    sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

fn op(value: u8) -> TaskOperationId {
    let mut bytes = [value; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    TaskOperationId::try_from_bytes(bytes).unwrap()
}
fn at(nanos: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_nanos(nanos))
}
fn context_actor() -> ActorId {
    ActorId::new(4).unwrap()
}
fn semantics() -> EstablishSemanticFacts {
    EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([1; 16]),
        shared_filter: ContentFingerprint::from_bytes([1; 16]),
        query_options: ContentFingerprint::from_bytes([1; 16]),
        credential_lease: CredentialLeaseId::new(1),
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
        panic!("a mismatch or an unmaterialized Context cannot claim a material comparison")
    }
}
impl Drop for Credential {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct RouteFixture {
    f: Fixture,
    ticket: AdmissionTicketId,
    creator: InputId,
    creator_handle: Option<ControlRequestHandle>,
    host_storage: StorageNeed,
    snapshot: PublishedSnapshot<QueryContextState>,
    removed: Option<ContextHostRecord>,
    pending_bootstrap: Option<(EffectTicket<WorkerAction>, ContextHostBinding)>,
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
        let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
            ordinary_running: positions - 1,
            ordinary_waiting: 0,
            control_running: 1,
            control_waiting: 0,
            max_batch_items: 1,
            root_read_holders: 1,
        })
        .unwrap();
        let context_storage = CONTEXT_BOOTSTRAP_STORAGE.resources().unwrap();
        let context = context_storage
            .checked_add(&ResourceVector::new([(Dimension::Contexts.id(), 1)]).unwrap())
            .unwrap();
        let ticket_storage = ResourceVector::new([(Dimension::Work.id(), 1)]).unwrap();
        let ticket = ticket_storage
            .checked_add(&ResourceVector::new([(Dimension::Tickets.id(), 1)]).unwrap())
            .unwrap();
        let storage = AdmissionStorage {
            loading: policy.loading.resources().unwrap(),
            accepted: policy.accepted.resources().unwrap(),
            // The admission port adds the business Context/Ticket unit once;
            // these fields contain only their already funded metadata.
            context: context_storage,
            ticket: ticket_storage,
        };
        let host_storage = StorageNeed {
            inputs: 1,
            batches: 1,
            effects: ControlCore::MAX_STEP_WORK,
            work: 1,
            ..StorageNeed::default()
        };
        let mut additional = context
            .checked_add(&ticket)
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
        // The baseline has at most sixteen actual reservations. Each extra
        // ingress source adds its remainder, escrow, source root and link;
        // separately paid fixture Work supplies its own additional records.
        let reservations = 16 + 4 * (positions - 2) + extra.work;
        let f = Fixture::with_geometry_records(
            positions,
            positions,
            positions + 2,
            storage.clone(),
            additional,
            positions + 1,
            2,
            reservations,
        );
        let snapshot = PublishedSnapshot::new(
            &f.root.resources().scope,
            Arc::new(QueryContextState::Absent),
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
                    request: AcquireQueryContextAdmissionTicket::new(
                        op(10),
                        this.f.context,
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
                    context: this.f.context,
                    ticket: this.ticket,
                    payload: PayloadId(100),
                    semantics: semantics(),
                    generation: generation(),
                },
            )
            .unwrap();
        this.f.runtime.poll_one(Tick(1)).unwrap();
        this.f.root.index_control_effects(64).unwrap();
        let materialize = this.f.root.take_control_effect(64).unwrap().unwrap();
        let establishment = match &materialize.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { establishment, .. },
                ..
            } => *establishment,
            _ => panic!("the original committed Materialize"),
        };
        let binding = ContextHostBinding {
            actor: context_actor(),
            core: ContextCore::new(
                this.f.context,
                generation(),
                establishment,
                ContextConfig {
                    max_tasks: 1,
                    max_creation_waiters: 1,
                    max_pending_requests: 4,
                    max_preparing: 1,
                    max_task_bytes: 1024,
                    step_budget: ContextCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
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
            this.adopt_pending_context();
        }
        this
    }
    fn adopt_pending_context(&mut self) {
        let (materialize, binding) = self.pending_bootstrap.take().unwrap();
        self.f.root.adopt_context(&materialize, binding).unwrap();
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
        self.f.root.index_control_effects(64).unwrap();
        while let Some(ticket) = self.f.root.take_control_effect(64).unwrap() {
            match &ticket.effect {
                WorkerAction::ControlReply(_) => self.f.root.publish_request_reply(ticket).unwrap(),
                WorkerAction::Control {
                    effect: ControlEffect::AdmissionReply { .. },
                    ..
                } => {
                    self.f.root.adopt_ticket(&ticket).unwrap();
                    self.complete_metadata(ticket);
                }
                WorkerAction::Control {
                    effect: ControlEffect::Reply { .. } | ControlEffect::DomainFailureReply { .. },
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
                    if self
                        .removed
                        .as_ref()
                        .is_some_and(|owner| owner.admission() == *id)
                    {
                        self.removed
                            .as_mut()
                            .unwrap()
                            .retire_active_context(&ticket)
                            .unwrap();
                    } else {
                        assert!(self.f.root.retire_ticket(&ticket).unwrap());
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
                        .take_removing_context(&ticket)
                        .unwrap()
                        .unwrap();
                    assert!(self.removed.replace(owner).is_none());
                    self.f.complete_body(ticket);
                }
                other => panic!("unexpected component Root effect: {other:?}"),
            }
        }
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
        let request = EstablishQueryContext::new(
            operation,
            self.f.context,
            self.ticket,
            content(gate),
            content(None),
            content(None),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
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
                    context: projection.context,
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
        let refusal = self.f.root.handoff_establish_route(route).unwrap();
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
        let host = self.f.root.service().context(self.f.context).unwrap();
        host.dispatch.index_committed(&runtime, 64).unwrap();
        let probe = host.dispatch.take_ready(&runtime, 64).unwrap().unwrap();
        let result = match &probe.effect {
            WorkerAction::Context(ContextEffect::EstablishProbed {
                request, result, ..
            }) if request.token == input => *result,
            _ => panic!("actual Context owner probe"),
        };
        host.handoff_establish_probe(probe).unwrap();
        result
    }
    fn dispose_context_metadata(&mut self) {
        let runtime = self.f.runtime.clone();
        let ledger = self.f.ledger.clone();
        let root_scope = self.f.root.resources().scope.clone();
        let host = self.f.root.service().context(self.f.context).unwrap();
        let resources = host.resources_for_test();
        assert!(resources.routes.is_empty());
        if resources.scope.is_none() {
            return;
        }
        runtime.close_timer_source(context_actor()).unwrap();
        runtime.close_actor_admission(context_actor()).unwrap();
        let mut other_claims = Vec::new();
        while let Some(ready) = runtime.claim_effect_actor() {
            if ready == context_actor() {
                assert!(runtime.pending_batches(ready, None, 1).unwrap().is_empty());
                runtime.release_effect_actor(ready, false).unwrap();
            } else {
                // This Context's metadata exit does not require another
                // actor's committed input to retire. Retain each other
                // scheduling claim once, then restore its actual ready edge.
                other_claims.push(ready);
            }
        }
        for ready in other_claims {
            runtime.release_effect_actor(ready, true).unwrap();
        }
        runtime.seal_actor(context_actor()).unwrap();
        let scope = resources.scope.take().unwrap();
        scope.close_external().unwrap();
        for role in resources.roles.iter_mut().rev() {
            role.work.body_exited().unwrap();
            role.work.captures_exited().unwrap();
        }
        ledger.reap_scope_ready(scope.id(), 32).unwrap();
        for role in &mut resources.roles {
            role.capacity
                .release(&role.capacity.resources().unwrap())
                .unwrap();
        }
        drop(runtime.remove_actor(context_actor()).unwrap());
        resources.registered = false;
        assert!(scope.snapshot().unwrap().retirement_ready);
        ledger.remove_scope(scope.id()).unwrap();
        ledger.reap_scope_ready(root_scope.id(), 32).unwrap();
    }
    fn finish_model_refusal(mut self) {
        assert!(self.f.root.resources().requests.contains_key(&self.creator));
        self.dispose_context_metadata();
        let refusal = self.f.host_input(
            ControlInput::ContextBootstrapFailed {
                context: self.f.context,
                refusal: OperationOutcome::OwnerFailed.into(),
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
            ControlInput::Retained {
                context: self.f.context,
            },
            self.host_storage,
        );
        self.f.runtime.poll_one(Tick(101)).unwrap();
        self.drain_root();
        self.f.finish_host_input(retained);
        assert_eq!(self.f.ledger.used(Dimension::Tickets.id()), 0);
        // Real builtin Root timer, not a guessed RemoveHost effect.
        assert_eq!(
            self.f
                .runtime
                .advance_time(Tick(11_000_000_000), 2)
                .unwrap(),
            1
        );
        self.f.runtime.poll_one(Tick(11_000_000_000)).unwrap();
        self.f.root.index_control_effects(64).unwrap();
        let timer_input = self
            .f
            .root
            .take_control_effect(64)
            .unwrap()
            .map(|ticket| {
                let input = ticket.input;
                assert!(matches!(
                    &ticket.effect,
                    WorkerAction::Control {
                        effect: ControlEffect::RemoveHost { .. },
                        ..
                    }
                ));
                let owner = self
                    .f
                    .root
                    .service()
                    .take_removing_context(&ticket)
                    .unwrap()
                    .unwrap();
                assert!(self.removed.replace(owner).is_none());
                self.f.complete_body(ticket);
                input
            })
            .expect("actual retention timer removal");
        self.drain_root();
        self.f.runtime.retire_input(actor(), timer_input).unwrap();
        let removed = self.f.host_input(
            ControlInput::HostRemoved {
                context: self.f.context,
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
        assert_eq!(self.f.ledger.used(Dimension::Contexts.id()), 0);
        resources.admission.work.body_exited().unwrap();
        resources.admission.work.captures_exited().unwrap();
        self.f
            .ledger
            .reap_scope_ready(self.f.root.resources().scope.id(), 32)
            .unwrap();
        resources
            .admission
            .capacity
            .release(&resources.admission.capacity.resources().unwrap())
            .unwrap();
        self.f.runtime.close_timer_source(actor()).unwrap();
        self.f.finish();
    }
}

#[test]
fn the_context_leg_retires_before_root_consumes_its_original_routed_reply() {
    let mut f = RouteFixture::new();
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 100, None);
    assert_eq!(
        f.probe(input),
        EstablishReplayResult::Decided(OperationOutcome::ContextConflict)
    );
    assert!(
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    let returns = f.f.runtime.input_inventory(actor(), None, 16).unwrap();
    assert_eq!(
        returns
            .iter()
            .filter(|input| input.source == context_actor()
                && input.phase == InputPhase::Filled
                && input.role.is_some())
            .count(),
        1
    );
    f.dispose_context_metadata();
    assert!(f.f.runtime.snapshot(context_actor()).is_err());
    assert!(f.f.root.resources().requests.contains_key(&input));
    assert_eq!(f.f.ledger.used(Dimension::Replies.id()), 2);
    assert!(
        handle.try_take().is_none(),
        "the original Root return has not run"
    );
    f.f.runtime.poll_one(Tick(4)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(
        matches!(&reply.effect,WorkerAction::ControlReply(reply) if reply.request.token==input && reply.result==RequestResult::Outcome(OperationOutcome::ContextConflict))
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
    assert_eq!(f.f.ledger.used(Dimension::Replies.id()), 2);
    assert!(thread::spawn(move || disposal.run()).join().unwrap());
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 1);
    assert_eq!(f.f.ledger.used(Dimension::Replies.id()), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    f.finish_model_refusal();
}

#[test]
fn a_root_timeout_cannot_dispose_raw_until_the_real_context_leg_and_cleanup_job_exit() {
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
            .context(f.f.context)
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    let disposal: EstablishDisposalJob = premature_disposal
        .or_else(|| f.f.root.take_retired_route_disposal(input))
        .expect("the same original source after actual Context retirement");
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cleanup = thread::spawn(move || disposal.run());
    gate.entered.wait();
    let retained = f.f.root.reap_ready_requests(32);
    let paid = f.f.ledger.used(Dimension::Replies.id()) == 2;
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
    assert_eq!(f.f.ledger.used(Dimension::Replies.id()), 1);
    f.finish_model_refusal();
}
