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

//! Real original-R/source ownership and host barriers. These tests use the
//! fresh, unknown-ticket route only; current-credential replay and Native M/R
//! are separate obligations, not inferred from successful fingerprinting.
mod materialization;
mod owner_failure;
mod source_loan;
mod source_pools;

use super::*;
use crate::control_runtime::{
    establish_ingress::{EstablishIngressJob, EstablishIngressOutcome},
    host_executor::{HostExecutor, HostExecutorLimits, HostSubmitError},
};
use novarocks_execution_contract::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, EstablishQueryContext,
};
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;

struct Gate {
    entered: Barrier,
    resume: Barrier,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
        })
    }
    fn stop(&self) {
        self.entered.wait();
        self.resume.wait();
    }
}
struct Content {
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    getter_gate: Option<Arc<Gate>>,
    drop_gate: Option<Arc<Gate>>,
    panic_drop: bool,
    panic_getter: bool,
}
impl std::fmt::Debug for Content {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("opaque Debug is never required")
    }
}
impl CodecOwnedContent for Content {
    fn fingerprint(&self) -> ContentFingerprint {
        self.getters.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_getter, "owned getter panic");
        if let Some(gate) = &self.getter_gate {
            gate.stop();
        }
        ContentFingerprint::from_bytes([1; 16])
    }
    fn encoded_len(&self) -> usize {
        panic!("this fingerprint-only projection has no size request")
    }
}
impl Drop for Content {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.drop_gate {
            gate.stop();
        }
        assert!(!self.panic_drop, "owned content destructor panic");
    }
}
struct Credential {
    drops: Arc<AtomicUsize>,
    panic_drop: bool,
}
impl ConfidentialContent for Credential {
    fn encoded_len(&self) -> usize {
        panic!("no confidential length is needed by projection")
    }
    fn matches(&self, _: &dyn ConfidentialContent) -> bool {
        panic!("fresh absence is not a comparison")
    }
}
impl Drop for Credential {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_drop, "owned credential destructor panic");
    }
}
fn fixture(scopes: usize) -> Fixture {
    Fixture::with_scopes(
        2,
        2,
        3,
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: ResourceVector::default(),
            ticket: ResourceVector::default(),
        },
        ESTABLISH_REQUEST_STORAGE
            .checked_sub(CONTROL_REQUEST_STORAGE)
            .unwrap()
            .resources()
            .unwrap(),
        scopes,
    )
}
fn request(
    f: &Fixture,
    getter_gate: Option<Arc<Gate>>,
    drop_gate: Option<Arc<Gate>>,
    panic_drop: bool,
) -> (EstablishQueryContext, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let getters = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let content = |getter_gate, drop_gate| {
        Arc::new(Content {
            getters: getters.clone(),
            drops: drops.clone(),
            getter_gate,
            drop_gate,
            panic_drop,
            panic_getter: false,
        }) as Arc<dyn CodecOwnedContent>
    };
    (
        EstablishQueryContext::new(
            operation(),
            f.context,
            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            content(getter_gate, drop_gate),
            content(None, None),
            content(None, None),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::new(1).unwrap(),
                Arc::new(Credential {
                    drops: drops.clone(),
                    panic_drop,
                }),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        ),
        getters,
        drops,
    )
}
fn admit(f: &mut Fixture) -> (InputId, ControlRequestHandle) {
    f.root
        .admit_establish_request(
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
        )
        .unwrap()
}
fn host(job: EstablishIngressJob) {
    let one = NonZeroUsize::new(1).unwrap();
    let executor = HostExecutor::start(
        HostExecutorLimits {
            contexts: one,
            jobs: one,
            per_context: one,
            workers: one,
        },
        |job: EstablishIngressJob| job.run(),
    )
    .unwrap_or_else(|_| panic!("one bounded getter worker"));
    executor
        .handle()
        .try_submit(job)
        .unwrap_or_else(|_| panic!("one original admitted job"));
    drop(
        executor
            .shutdown()
            .unwrap_or_else(|_| panic!("actual physical getter exit")),
    );
}
fn finish_rpc(
    f: &mut Fixture,
    input: InputId,
    handle: &mut ControlRequestHandle,
) -> crate::control_runtime::establish_ingress::EstablishDisposalJob {
    let projection = match f.root.take_establish_ingress(input).unwrap().unwrap() {
        EstablishIngressOutcome::Projected(projection) => projection,
        _ => panic!("actual host projection returned"),
    };
    assert_eq!(projection.operation, operation());
    assert_eq!(projection.context, f.context);
    f.root
        .submit_request(
            input,
            ControlInput::Establish {
                request: projection.operation,
                context: projection.context,
                ticket: projection.ticket,
                payload: projection.payload,
                semantics: projection.semantics,
                // This fixture has no Context or prior grant. No credential matches
                // are claimed; existing-context routing remains a separate test face.
                generation: generation(),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let body = f.root.take_control_effect(64).unwrap().unwrap();
    let reference = crate::control_runtime::effect_dispatch::EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    f.complete_body(body);
    f.root.control_completion_observed(reference).unwrap();
    let reply = f.root.take_control_effect(64).unwrap().unwrap();
    let disposal = f
        .root
        .begin_rejected_ingress_disposal(&reply)
        .unwrap()
        .unwrap();
    f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    assert_eq!(
        delivery.body(),
        &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Gone))
    );
    drop(delivery);
    disposal
}

#[test]
fn the_original_paid_rpc_survives_a_blocked_getter_and_a_blocked_raw_destructor() {
    let mut f = fixture(3);
    let getter = Gate::new();
    let dropping = Gate::new();
    let (raw, getters, drops) = request(&f, Some(getter.clone()), Some(dropping.clone()), false);
    let (input, mut external) = admit(&mut f);
    // Its earlier real Work will precede the child's parent link in the
    // metadata reaper. The original R must stay ready until its own header,
    // rather than merely somebody else's ready header, actually retires.
    let mut bytes = [12; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    let other_operation = TaskOperationId::try_from_bytes(bytes).unwrap();
    let (other, mut other_external) = f
        .root
        .admit_request(
            ActorId::new(3).unwrap(),
            other_operation,
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .unwrap();
    let work_before = f.ledger.used(Dimension::Work.id());
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(f.ledger.used(Dimension::Work.id()), work_before);
    let observed = f.runtime.change_version();
    let worker = thread::spawn(move || host(job));
    getter.entered.wait();
    let projection_waits = f.root.take_establish_ingress(input).unwrap().is_none();
    // Another actual Root RPC progresses while the first foreign getter owns
    // its loan. It does not need that source's lock or physical worker.
    f.root
        .submit_request(
            other,
            ControlInput::Acquire {
                request: AcquireQueryContextAdmissionTicket::new(
                    other_operation,
                    f.context,
                    LeaseValidFor::new(Duration::from_secs(6)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    f.capability,
                ),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let body = f.root.take_control_effect(64).unwrap().unwrap();
    let reference = crate::control_runtime::effect_dispatch::EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    f.complete_body(body);
    f.root.control_completion_observed(reference).unwrap();
    let reply = f.root.take_control_effect(64).unwrap().unwrap();
    f.root.publish_request_reply(reply).unwrap();
    let delivery = other_external.try_take().unwrap();
    drop(delivery);
    drop(other_external);
    let other_retired = f.root.reap_ready_requests(8).unwrap();
    getter.resume.wait();
    worker.join().unwrap();
    assert!(projection_waits);
    assert_eq!(other_retired, 1);
    assert_ne!(
        f.runtime.change_version(),
        observed,
        "same original concrete Ready wakes the actor runtime"
    );
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    let disposal = finish_rpc(&mut f, input, &mut external);
    drop(external);
    let cleaner = thread::spawn(move || disposal.run());
    dropping.entered.wait();
    let prematurely_retired = f.root.reap_ready_requests(8).unwrap();
    let held = f.ledger.used(Dimension::Replies.id());
    dropping.resume.wait();
    let source_exited = cleaner.join().unwrap();
    assert_eq!(prematurely_retired, 0);
    assert_eq!(held, 1);
    assert!(source_exited);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);
    assert_eq!(f.root.request_ready().registered(), 0);
    f.finish();
}

#[test]
fn a_child_factory_refusal_returns_the_untouched_raw_request_and_withdraws_no_core_consumer() {
    let mut f = fixture(1);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let refusal = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .err()
        .unwrap();
    let correct_refusal = matches!(
        refusal.error,
        ControlRequestError::Exit(ExitRoleError::Runtime(RuntimeError::ScopeCapacity))
    );
    drop(refusal.into_request());
    drop(external);
    assert!(correct_refusal);
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert!(
        f.root
            .begin_unsubmitted_withdrawal(input)
            .unwrap()
            .is_none()
    );
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);
    assert!(f.runtime.input_custody(actor(), input).unwrap().is_none());
    assert_eq!(f.ledger.used(Dimension::Replies.id()), 0);
    f.finish();
}

#[test]
fn a_physical_queue_refusal_returns_the_same_unstarted_job_and_can_revoke_the_original_rpc() {
    let mut f = fixture(2);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    let one = NonZeroUsize::new(1).unwrap();
    let executor = HostExecutor::start(
        HostExecutorLimits {
            contexts: one,
            jobs: one,
            per_context: one,
            workers: one,
        },
        |job: EstablishIngressJob| job.run(),
    )
    .unwrap_or_else(|_| panic!("bounded empty host"));
    let queue = executor.handle();
    queue.close_external();
    let (error, original) = queue.try_submit(job).err().unwrap();
    original.cancel_before_start();
    drop(
        executor
            .shutdown()
            .unwrap_or_else(|_| panic!("actually drained host")),
    );
    assert_eq!(error, HostSubmitError::Closed);
    assert!(matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::CancelledBeforeStart)
    ));
    drop(external);
    let cleanup = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 0);
    assert!(thread::spawn(move || cleanup.run()).join().unwrap());
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    f.finish();
}

#[test]
fn independent_raw_field_panics_never_unwind_through_another_field_or_return_rpc_capacity() {
    let mut f = fixture(2);
    let mut pools = source_pools::SourcePoolGuard::start(&mut f);
    let (raw, getters, drops) = request(&f, None, None, true);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    pools.getter(job);
    let projected = matches!(
        source_pools::wait_ingress(&mut f, input),
        EstablishIngressOutcome::Projected(_)
    );
    drop(external);
    let cleanup = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    pools.disposal(cleanup);
    let disposition = source_pools::wait_disposal(&f, input);
    let retired = f.root.advance_ready_requests(pools.handles(), 8);
    let held = f.ledger.used(Dimension::Replies.id());
    pools.finish();
    // This exact bounded Unknown is intentionally retained; it is not a green
    // cleanup result or proof that the missing S8 failure owner is complete.
    std::mem::forget(f);
    assert!(projected);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert_eq!(disposition, ExitRolePhase::Unresolved);
    assert!(
        matches!(retired, Err(crate::control_runtime::process_control::RootRequestAdvanceError::Request {
            input: actual,
            error: RequestIoError::Control(ControlRequestError::SourceUnresolved { input: source }),
        }) if actual == input && source == input)
    );
    assert_eq!(held, 1);
}

#[test]
fn timeout_while_the_original_materialize_effect_is_started_cannot_authorize_raw_disposal() {
    let ticket_storage = StorageNeed {
        captures: 1,
        work: 1,
        ..StorageNeed::default()
    };
    let context_storage = StorageNeed {
        captures: 1,
        work: 1,
        ..StorageNeed::default()
    };
    let additional = ESTABLISH_REQUEST_STORAGE
        .checked_sub(CONTROL_REQUEST_STORAGE)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(&ticket_storage.resources().unwrap())
        .unwrap()
        .checked_add(&context_storage.resources().unwrap())
        .unwrap()
        .checked_add(
            &ResourceVector::new([(Dimension::Tickets.id(), 1), (Dimension::Contexts.id(), 1)])
                .unwrap(),
        )
        .unwrap();
    let mut f = Fixture::with_scopes(
        2,
        2,
        3,
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: context_storage.resources().unwrap(),
            ticket: ticket_storage.resources().unwrap(),
        },
        additional,
        2,
    );
    let mut bytes = [12; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    let acquisition = TaskOperationId::try_from_bytes(bytes).unwrap();
    let (acquire, mut acquire_external) = f
        .root
        .admit_request(
            ActorId::new(3).unwrap(),
            acquisition,
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .unwrap();
    f.root
        .submit_request(
            acquire,
            ControlInput::Acquire {
                request: AcquireQueryContextAdmissionTicket::new(
                    acquisition,
                    f.context,
                    LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    f.capability,
                ),
                ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let grant = f.root.take_control_effect(64).unwrap().unwrap();
    let receipt = f.root.adopt_ticket(&grant).unwrap();
    let reference = crate::control_runtime::effect_dispatch::EffectRef {
        input: grant.input,
        action: grant.descriptor.id,
    };
    f.complete_body(grant);
    f.root.control_completion_observed(reference).unwrap();
    let reply = f.root.take_control_effect(64).unwrap().unwrap();
    f.root.publish_request_reply(reply).unwrap();
    drop(acquire_external.try_take().unwrap());
    drop(acquire_external);
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, mut external) = f
        .root
        .admit_establish_request(
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(1)),
        )
        .unwrap();
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(job);
    let EstablishIngressOutcome::Projected(projection) =
        f.root.take_establish_ingress(input).unwrap().unwrap()
    else {
        panic!("the original getter returned its full neutral projection")
    };
    assert_eq!(projection.ticket, receipt.ticket_id());
    f.root
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
    f.runtime.poll_one(Tick(2)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let materialize = f.root.take_control_effect(64).unwrap().unwrap();
    let reservation = match &materialize.effect {
        WorkerAction::Control {
            effect: ControlEffect::Materialize { reservation, .. },
            ..
        } => *reservation,
        _ => panic!("the exact redeemed ticket elected this actual materialize effect"),
    };
    let (mut context_work, context_capacity) = f
        .runtime
        .take_step_reservation(&materialize, reservation)
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    context_work.start().unwrap();
    // The elected ticket can lend only this original R's raw captures. The
    // admitted Context holder above is still separate; borrowing the fields
    // does not acknowledge that holder's physical body or publish Active.
    let loan = f.root.take_materialization_source(&materialize).unwrap();
    assert!(matches!(
        f.root.take_materialization_source(&materialize),
        Err(ControlRequestError::WrongPhase)
    ));
    let borrowed = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let borrowed_host = borrowed.clone();
    let resume_host = resume.clone();
    let expected_context = f.context;
    let host_call = thread::spawn(move || {
        assert_eq!(loan.shared_facts().context(), expected_context);
        borrowed_host.wait();
        resume_host.wait();
        // A borrowed host panic returns the source loan on unwind. The
        // original M holder is intentionally retained below, so this is no
        // fabricated HostReleased/MaterializationFinished or Work exit.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _original_loan = loan;
            panic!("borrowed materialization boundary");
        }))
        .is_err()
    });
    borrowed.wait();
    // This original host responsibility deliberately remains started. No
    // Native call or physical exit is fabricated by this controlled fixture.
    let deadline = Tick(1_000_000_000);
    assert_eq!(f.runtime.advance_time(deadline, 1).unwrap(), 1);
    f.runtime.poll_one(deadline).unwrap();
    f.root.index_control_effects(64).unwrap();
    let timeout = f.root.take_control_effect(64).unwrap().unwrap();
    let is_timeout = matches!(&timeout.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted);
    let (refused_disposal, retained_wrong_disposal) =
        match f.root.begin_rejected_ingress_disposal(&timeout) {
            Err(ControlRequestError::WrongEffect) => (true, None),
            Ok(job) => (false, job.map(std::mem::ManuallyDrop::new)),
            other => panic!("unexpected actual timeout disposition: {:?}", other.err()),
        };
    f.root.publish_request_reply(timeout).unwrap();
    drop(external.try_take().unwrap());
    drop(external);
    let retired = f.root.reap_ready_requests(8).unwrap();
    let fee = f.ledger.used(Dimension::Replies.id());
    let context_still_owned = f.ledger.used(Dimension::Contexts.id());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    resume.wait();
    assert!(host_call.join().unwrap());
    // The never-returned original host effect and source are intentionally
    // retained. This is a bounded pending/Unknown case, not successful cleanup.
    std::mem::forget((
        f,
        materialize,
        context_work,
        context_capacity,
        retained_wrong_disposal,
    ));
    assert!(is_timeout);
    assert!(refused_disposal);
    assert_eq!(retired, 0);
    assert_eq!(fee, 1);
    assert_eq!(context_still_owned, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn a_started_getter_can_withdraw_only_after_its_raw_child_really_exits_and_without_core_submission()
{
    let mut f = fixture(2);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(job);
    let EstablishIngressOutcome::Projected(projection) =
        f.root.take_establish_ingress(input).unwrap().unwrap()
    else {
        panic!("real projection");
    };
    let mut wrong = projection.semantics;
    wrong.catalog = ContentFingerprint::from_bytes([2; 16]);
    let refused = f
        .root
        .submit_request(
            input,
            ControlInput::Establish {
                request: projection.operation,
                context: projection.context,
                ticket: projection.ticket,
                payload: projection.payload,
                semantics: wrong,
                generation: generation(),
            },
        )
        .unwrap_err();
    assert!(matches!(refused.error, ControlRequestError::WrongRequest));
    assert!(refused.command.is_some());
    assert!(
        !f.runtime.poll_one(Tick(1)).unwrap(),
        "the wrong projection never filled this original input"
    );
    drop(external);
    let cleanup = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 0);
    assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
    assert!(thread::spawn(move || cleanup.run()).join().unwrap());
    assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);
    assert!(f.runtime.input_custody(actor(), input).unwrap().is_none());
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    f.finish();
}

#[test]
fn an_observed_getter_panic_reports_failure_then_retires_after_the_real_raw_exits() {
    let mut f = fixture(2);
    let getters = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let content = |panic_getter| {
        Arc::new(Content {
            getters: getters.clone(),
            drops: drops.clone(),
            getter_gate: None,
            drop_gate: None,
            panic_drop: false,
            panic_getter,
        }) as Arc<dyn CodecOwnedContent>
    };
    let raw = EstablishQueryContext::new(
        operation(),
        f.context,
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        content(true),
        content(false),
        content(false),
        CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::new(1).unwrap(),
            Arc::new(Credential {
                drops: drops.clone(),
                panic_drop: false,
            }),
        ),
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    );
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(job);
    let panicked = matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::GetterPanicked)
    );
    let actor_alive = !f.runtime.snapshot(actor()).unwrap().failed;
    let withdrawal_refused = matches!(
        f.root.begin_unsubmitted_withdrawal(input),
        Err(ControlRequestError::WrongPhase)
    );
    f.root.submit_establish_ingress_failure(input).unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let failure_reply = f.root.take_control_effect(64).unwrap().unwrap();
    let disposal = f
        .root
        .begin_rejected_ingress_disposal(&failure_reply)
        .unwrap()
        .unwrap();
    f.root.publish_request_reply(failure_reply).unwrap();
    let delivery = external.try_take().unwrap();
    let owner_failed = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed));
    drop(delivery);
    drop(external);
    let source_exited = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.root.reap_ready_requests(8).unwrap();
    let fee = f.ledger.used(Dimension::Replies.id());
    f.finish();
    assert!(panicked);
    assert!(actor_alive);
    assert!(withdrawal_refused);
    assert!(owner_failed);
    assert!(source_exited);
    assert_eq!(retired, 1);
    assert_eq!(fee, 0);
    assert_eq!(getters.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_foreign_root_rejection_cannot_take_the_original_establish_raw_disposal() {
    use crate::control_runtime::{
        effect_dispatch::EffectRef,
        process_control::{PROCESS_CONTROL_BOOTSTRAP_STORAGE, ProcessControlBinding},
    };

    // Two paid R positions, one Establish aggregate (3 Work / 2 reports /
    // 2 captures), and a second actual Root's original bootstrap vector.
    // No ticket or Context allocation is needed: both operations truly refuse.
    let additional = ESTABLISH_REQUEST_STORAGE
        .checked_sub(CONTROL_REQUEST_STORAGE)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(&PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap())
        .unwrap();
    let mut f = Fixture::with_geometry(
        2,
        2,
        3,
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: ResourceVector::default(),
            ticket: ResourceVector::default(),
        },
        additional,
        3,
        2,
    );
    let foreign_actor = ActorId::new(2).unwrap();
    let foreign_core = ControlCore::new(
        ControlConfig {
            max_contexts: 2,
            max_waiters: 4,
            max_pending_requests: 4,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 1,
            gone_fences: 1,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::new(1, Duration::from_secs(5), Duration::from_secs(10), 1).unwrap(),
        [8; 8],
        MonotonicInstant::ORIGIN,
    )
    .unwrap();
    let foreign_capability = foreign_core.admission().capability();
    let mut foreign = ProcessControlOwner::bootstrap(
        &f.runtime,
        &f.ledger,
        foreign_core,
        ProcessControlBinding {
            actor: foreign_actor,
            generation: generation(),
            max_work: 16,
            max_slots: 3,
            request_positions: NonZeroUsize::new(1).unwrap(),
            actor_config: ActorConfig {
                mailbox: [4, 4, 4, 1],
                max_batches: 8,
                max_effects_per_step: ControlCore::MAX_STEP_WORK,
                max_dependencies_per_effect: ControlCore::MAX_STEP_WORK,
                turn_budget: 64,
                max_step_work: ControlCore::MAX_STEP_WORK,
            },
            admission_storage: AdmissionStorage {
                loading: ResourceVector::default(),
                accepted: ResourceVector::default(),
                context: ResourceVector::default(),
                ticket: ResourceVector::default(),
            },
        },
    )
    .unwrap();

    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, mut external) = admit(&mut f);
    let getter = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(getter);
    let EstablishIngressOutcome::Projected(projection) =
        f.root.take_establish_ingress(input).unwrap().unwrap()
    else {
        panic!("the original source returned its real projection")
    };
    let getter_count = getters.load(Ordering::SeqCst);
    f.root
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
    let (foreign_input, mut foreign_external) = foreign
        .admit_request(ActorId::new(3).unwrap(), operation(), deadline)
        .unwrap();
    foreign
        .submit_request(
            foreign_input,
            ControlInput::Acquire {
                request: AcquireQueryContextAdmissionTicket::new(
                    operation(),
                    f.context,
                    // The second Root's real five-second rule yields a refusal,
                    // without asking for an unpaid ticket holder.
                    LeaseValidFor::new(Duration::from_secs(6)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    foreign_capability,
                ),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();

    foreign.index_control_effects(64).unwrap();
    let body = foreign.take_control_effect(64).unwrap().unwrap();
    let reference = EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    let body_id = body.descriptor.id;
    f.runtime.effect_completed(body).unwrap();
    for (descriptor, _) in f
        .runtime
        .effect_inventory(
            foreign_actor,
            foreign_input,
            None,
            ControlCore::MAX_STEP_WORK,
        )
        .unwrap()
    {
        if descriptor.dependencies.contains(&body_id) {
            f.runtime
                .acknowledge_dependency(foreign_actor, foreign_input, descriptor.id, body_id)
                .unwrap();
        }
    }
    foreign.control_completion_observed(reference).unwrap();
    let foreign_reply = foreign.take_control_effect(64).unwrap().unwrap();
    let (same_request_context, foreign_rejection) = match &foreign_reply.effect {
        WorkerAction::ControlReply(reply) => (
            reply.request == external.request(),
            matches!(
                &reply.body,
                ControlReplyBody::Admission(Err(OperationOutcome::InvalidStateOrRequest))
            ),
        ),
        _ => (false, false),
    };
    let different_actor_same_local_input = foreign_reply.actor == foreign_actor
        && foreign_actor != actor()
        && foreign_input == input
        && foreign_reply.input == input;

    // Even when testing a broken authorization gate, retain any incorrectly
    // returned original disposal. Do not run or drop it until A's own actual
    // rejected reply has arrived; normal cleanup precedes the safety assert.
    let (refused, wrong_disposal) = match f.root.begin_rejected_ingress_disposal(&foreign_reply) {
        Err(ControlRequestError::WrongRoot) => (true, None),
        Ok(job) => (false, job),
        Err(_) => (false, None),
    };
    let drops_after_foreign_attempt = drops.load(Ordering::SeqCst);
    foreign.publish_request_reply(foreign_reply).unwrap();
    let foreign_delivery = foreign_external.try_take().unwrap();
    drop(foreign_delivery);
    drop(foreign_external);
    let foreign_retired = foreign.reap_ready_requests(8).unwrap();

    f.root.index_control_effects(64).unwrap();
    let body = f.root.take_control_effect(64).unwrap().unwrap();
    let reference = EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    f.complete_body(body);
    f.root.control_completion_observed(reference).unwrap();
    let own_reply = f.root.take_control_effect(64).unwrap().unwrap();
    let own_disposal = f.root.begin_rejected_ingress_disposal(&own_reply).unwrap();
    // take_disposal is one-shot. A broken foreign gate may already hold that
    // same original job; the correct gate leaves it for this actual reply.
    let disposal = match (wrong_disposal, own_disposal) {
        (Some(original), None) | (None, Some(original)) => original,
        _ => panic!("exactly one original raw-source disposal must remain owned"),
    };
    f.root.publish_request_reply(own_reply).unwrap();
    let own_delivery = external.try_take().unwrap();
    let own_is_gone = own_delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Gone));
    drop(own_delivery);
    drop(external);
    let source_exited = thread::spawn(move || disposal.run()).join().unwrap();
    let original_retired = f.root.reap_ready_requests(8).unwrap();
    let raw_fields_dropped = drops.load(Ordering::SeqCst);

    // Fully remove B before Fixture::finish verifies the shared ledger is
    // empty. These are B's actual original metadata holders, not fake ACKs.
    let (service, mut resources) = foreign.into_parts();
    drop(service);
    resources.scope.close_external().unwrap();
    f.runtime.close_actor_admission(foreign_actor).unwrap();
    while let Some(ready) = f.runtime.claim_effect_actor() {
        f.runtime.release_effect_actor(ready, false).unwrap();
    }
    f.runtime.seal_actor(foreign_actor).unwrap();
    for role in resources.roles.iter_mut().rev() {
        role.work.body_exited().unwrap();
        role.work.captures_exited().unwrap();
    }
    f.ledger.reap_scope_ready(resources.scope.id(), 32).unwrap();
    drop(f.runtime.remove_actor(foreign_actor).unwrap());
    for role in resources.roles.iter_mut().rev() {
        role.capacity
            .release(&role.capacity.resources().unwrap())
            .unwrap();
    }
    f.ledger.remove_scope(resources.scope.id()).unwrap();
    drop(resources);
    f.finish();

    // All target assertions are after two real Roots / RPCs / four raw fields
    // have actually exited. A false authorization is an ordinary test failure,
    // not a leaked active job or a secondary strict-Drop abort.
    assert!(different_actor_same_local_input);
    assert!(same_request_context);
    assert!(foreign_rejection);
    assert!(refused);
    assert_eq!(drops_after_foreign_attempt, 0);
    assert_eq!(getter_count, 3);
    assert!(own_is_gone);
    assert!(source_exited);
    assert_eq!(foreign_retired, 1);
    assert_eq!(original_retired, 1);
    assert_eq!(raw_fields_dropped, 4);
}

#[test]
fn two_physical_ingress_workers_keep_same_context_rpcs_serial_and_progress_another_context() {
    use crate::control_runtime::preparation::{HostFairnessGroup, QueuedPreparation};
    use std::sync::mpsc;

    let delta = ESTABLISH_REQUEST_STORAGE
        .checked_sub(CONTROL_REQUEST_STORAGE)
        .unwrap()
        .resources()
        .unwrap();
    let additional = delta
        .checked_add(&delta)
        .unwrap()
        .checked_add(&delta)
        .unwrap();
    let mut f = Fixture::with_scopes(
        3,
        3,
        8,
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: ResourceVector::default(),
            ticket: ResourceVector::default(),
        },
        additional,
        4,
    );
    let original_context = f.context;
    let first_gate = Gate::new();
    let second_gate = Gate::new();
    let other_gate = Gate::new();
    let (first_raw, first_getters, first_drops) =
        request(&f, Some(first_gate.clone()), None, false);
    let (second_raw, second_getters, second_drops) =
        request(&f, Some(second_gate.clone()), None, false);
    let other_context = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(17001, 17002), AttemptId::new(1).unwrap()).unwrap(),
        original_context.frontend_process_id(),
        original_context.backend_process_id(),
    );
    f.context = other_context;
    let (other_raw, other_getters, other_drops) =
        request(&f, Some(other_gate.clone()), None, false);
    f.context = original_context;

    let (first_input, first_external) = admit(&mut f);
    let (second_input, second_external) = admit(&mut f);
    let (other_input, other_external) = admit(&mut f);
    let first = f
        .root
        .begin_establish_ingress(first_input, first_raw, PayloadId(1))
        .unwrap();
    let second = f
        .root
        .begin_establish_ingress(second_input, second_raw, PayloadId(2))
        .unwrap();
    let other = f
        .root
        .begin_establish_ingress(other_input, other_raw, PayloadId(3))
        .unwrap();
    let keys = [first.queue_key(), second.queue_key(), other.queue_key()];
    let groups = [
        first.fairness_group(keys[0].0),
        second.fairness_group(keys[1].0),
        other.fairness_group(keys[2].0),
    ];
    let metadata_correct = keys[0] != keys[1]
        && keys[0] != keys[2]
        && keys[1] != keys[2]
        && groups[0] == HostFairnessGroup::Context(original_context)
        && groups[1] == groups[0]
        && groups[2] == HostFairnessGroup::Context(other_context);

    // Observe entry without blocking the test on a Barrier that the correct
    // implementation must not reach yet. Both observation threads are released
    // unconditionally, including when the old scope grouping occupies workers
    // with the first two same-Context bodies.
    let (release_second_tx, release_second_rx) = mpsc::channel();
    let second_observer = thread::spawn(move || {
        second_gate.entered.wait();
        release_second_rx.recv().unwrap();
        second_gate.resume.wait();
    });
    let (entered_other_tx, entered_other_rx) = mpsc::channel();
    let (release_other_tx, release_other_rx) = mpsc::channel();
    let other_observer = thread::spawn(move || {
        other_gate.entered.wait();
        entered_other_tx.send(()).unwrap();
        release_other_rx.recv().unwrap();
        other_gate.resume.wait();
    });
    let executor = HostExecutor::start(
        HostExecutorLimits {
            // Three actual source scopes must fit even in the old grouping,
            // so a refusal cannot masquerade as a fairness test result.
            contexts: NonZeroUsize::new(3).unwrap(),
            jobs: NonZeroUsize::new(3).unwrap(),
            per_context: NonZeroUsize::new(2).unwrap(),
            workers: NonZeroUsize::new(2).unwrap(),
        },
        |job: EstablishIngressJob| job.run(),
    )
    .unwrap_or_else(|_| panic!("two actual bounded ingress workers"));
    let queue = executor.handle();
    queue
        .try_submit(first)
        .unwrap_or_else(|_| panic!("paid first original getter"));
    first_gate.entered.wait();
    queue
        .try_submit(second)
        .unwrap_or_else(|_| panic!("paid second original getter"));
    queue
        .try_submit(other)
        .unwrap_or_else(|_| panic!("paid other-context getter"));
    let other_progressed = entered_other_rx
        .recv_timeout(Duration::from_secs(5))
        .is_ok();
    let second_getters_before_release = second_getters.load(Ordering::SeqCst);
    let drops_before_release = first_drops.load(Ordering::SeqCst)
        + second_drops.load(Ordering::SeqCst)
        + other_drops.load(Ordering::SeqCst);

    // Never assert a progress failure with blocked physical workers. Signals
    // are buffered even when a late body has not reached its entry Barrier.
    release_second_tx.send(()).unwrap();
    release_other_tx.send(()).unwrap();
    first_gate.resume.wait();
    drop(
        executor
            .shutdown()
            .unwrap_or_else(|_| panic!("real physical ingress drain")),
    );
    second_observer.join().unwrap();
    other_observer.join().unwrap();
    drop(queue);

    let getter_counts = [
        first_getters.load(Ordering::SeqCst),
        second_getters.load(Ordering::SeqCst),
        other_getters.load(Ordering::SeqCst),
    ];
    let mut all_projected = true;
    let mut all_sources_exited = true;
    let mut retired = 0;
    for (input, external) in [
        (first_input, first_external),
        (second_input, second_external),
        (other_input, other_external),
    ] {
        all_projected &= matches!(
            f.root.take_establish_ingress(input).unwrap(),
            Some(EstablishIngressOutcome::Projected(_))
        );
        let disposal = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
        all_sources_exited &= thread::spawn(move || disposal.run()).join().unwrap();
        drop(external);
        retired += f.root.reap_ready_requests(16).unwrap();
    }
    let drop_counts = [
        first_drops.load(Ordering::SeqCst),
        second_drops.load(Ordering::SeqCst),
        other_drops.load(Ordering::SeqCst),
    ];
    f.finish();

    // The old per-source-Scope grouping may fail these assertions, but both
    // barriers, all workers, all original R roles and all twelve raw fields
    // have first completed their actual cleanup through the normal APIs.
    assert!(other_progressed);
    assert_eq!(second_getters_before_release, 0);
    assert!(metadata_correct);
    assert_eq!(drops_before_release, 0);
    assert_eq!(getter_counts, [3, 3, 3]);
    assert!(all_projected);
    assert!(all_sources_exited);
    assert_eq!(retired, 3);
    assert_eq!(drop_counts, [4, 4, 4]);
}
