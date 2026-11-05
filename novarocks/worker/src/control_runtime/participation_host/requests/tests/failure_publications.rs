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

//! The real Participation failure action owns the publication session. A failed
//! flag, Task projection or source exit alone never supplies its drained proof.
//! Materialization and protocol effects remain neutral component-model facts;
//! actual aliases, Work, reports, R delivery and fees use the real runtime.

use super::*;
use crate::{
    control_runtime::participation_host::{FAILURE, TIMER, failure::FailurePublicationError},
    task_proposal_ingress::PairRoute,
};
use novarocks_control_runtime::actor_turn::{FailureSourceState, InputPhase};
use novarocks_control_runtime_core::{effect::EffectState, identity::ActionId};
use novarocks_execution::runtime::task::ExecutionFailure;
use novarocks_execution_contract::{
    TaskConclusionProposal, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread::{self, ThreadId},
};

fn activate(f: &mut Harness) {
    let input = model_fact(
        f,
        ParticipationInput::MaterializationFinished {
            result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
        },
        1,
    );
    complete_model_effects(f, input);
    f.runtime
        .retire_input(participation_actor(), input)
        .unwrap();
}
pub(super) fn actual_failure(f: &mut Harness) -> EffectTicket<WorkerAction> {
    let unknown = crate::control_runtime::actors::tests::task(f.identity, 999);
    // The actual actor consumes this unknown Task input, its Core rejects it,
    // and the runtime installs its original failed-generation cleanup action.
    let input = model_fact(f, ParticipationInput::Installed { task: unknown }, 9);
    take_actual_failure(f, input)
}
/// Each fixture supplies the actual failed business occurrence; Runtime owns
/// its distinct prepaid supervisor source. No caller guesses that address.
pub(super) fn take_actual_failure(f: &Harness, input: InputId) -> EffectTicket<WorkerAction> {
    assert!(
        f.runtime
            .input_custody(participation_actor(), input)
            .unwrap()
            .is_some_and(|original| original.phase == InputPhase::AbandonedByOwnerFailure),
        "the original failing business input keeps its real abandoned receipt"
    );
    let FailureSourceState::ClosedPending(supervisor) = f
        .runtime
        .failure_source_state(participation_actor())
        .unwrap()
    else {
        panic!("the actual builtin failure source must own its pending batch");
    };
    assert_ne!(supervisor, input);
    let ticket = f
        .runtime
        .take_effect(participation_actor(), supervisor, ActionId::new(1).unwrap())
        .unwrap();
    f.runtime
        .retire_input(participation_actor(), input)
        .unwrap();
    assert!(
        f.runtime
            .input_custody(participation_actor(), input)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.runtime
            .failure_source_state(participation_actor())
            .unwrap(),
        FailureSourceState::ClosedPending(supervisor)
    );
    assert!(matches!(ticket.effect, WorkerAction::OwnerFailed { .. }));
    assert!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .scope()
            .snapshot()
            .unwrap()
            .failed
    );
    ticket
}
pub(super) fn complete_actual_failure_source(f: &Harness, input: InputId) {
    assert_eq!(
        f.runtime
            .failure_source_state(participation_actor())
            .unwrap(),
        FailureSourceState::ClosedPending(input)
    );
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .complete_failure_publications_for_test();
    assert_eq!(
        f.runtime
            .failure_source_state(participation_actor())
            .unwrap(),
        FailureSourceState::ClosedDrained
    );
    assert!(
        f.runtime
            .input_custody(participation_actor(), input)
            .unwrap()
            .is_none()
    );
}
pub(super) fn actual_participation_headers(
    f: &Harness,
) -> [novarocks_control_runtime::obligation::WorkRetirementObserver; 2] {
    let address = f.service.participation(f.identity).unwrap();
    let host = address.lock().unwrap();
    [TIMER, FAILURE].map(|role| {
        host.resources.metadata_resources[role]
            .work
            .retirement_observer()
    })
}
pub(super) fn finish_failed_fixture(f: Harness, input: InputId) {
    let headers = actual_participation_headers(&f);
    complete_actual_failure_source(&f, input);
    assert!(
        headers.iter().all(|header| !header.is_retired()),
        "supervisor input retirement does not exit the original body/capture holders"
    );
    finish(f);
    assert!(
        headers.iter().all(|header| header.is_retired()),
        "the original fixture must physically exit and retire both actual holders"
    );
}
fn failure_is_still_started(f: &mut Harness, input: InputId) -> bool {
    let host_address = f.service.participation(f.identity).unwrap();
    let host = host_address.lock().unwrap();
    host.resources.failure.is_some()
        && matches!(
            host.resources.metadata_resources[FAILURE]
                .work
                .actual_exit_ready(),
            Ok(false)
        )
        && !host.resources.metadata_resources[FAILURE]
            .work
            .retirement_observer()
            .is_retired()
        && f.runtime
            .effect_inventory(participation_actor(), input, None, 1)
            .is_ok_and(|inventory| {
                inventory.iter().any(|(effect, state)| {
                    effect.id == ActionId::new(1).unwrap() && *state == EffectState::Started
                })
            })
}
pub(super) fn drain_one_unit_turns(f: &mut Harness, input: InputId) -> (bool, bool) {
    let mut alive = true;
    for _ in 0..128 {
        alive &= failure_is_still_started(f, input);
        let drained = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1)
            .unwrap();
        alive &= failure_is_still_started(f, input);
        if drained {
            return (true, alive);
        }
    }
    (false, alive)
}

struct RawDomainDropGate {
    open: Mutex<bool>,
    changed: Condvar,
}
struct ReleaseRawDomainDrop(Arc<RawDomainDropGate>);
impl ReleaseRawDomainDrop {
    fn release(&self) {
        *self.0.open.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
impl Drop for ReleaseRawDomainDrop {
    fn drop(&mut self) {
        self.release();
    }
}
struct RawDomainDropProbe {
    entered: Option<SyncSender<ThreadId>>,
    gate: Arc<RawDomainDropGate>,
    drops: Arc<AtomicUsize>,
    getters: Arc<AtomicUsize>,
}
impl novarocks_execution_contract::ConfidentialContent for RawDomainDropProbe {
    fn encoded_len(&self) -> usize {
        self.getters.fetch_add(1, Ordering::SeqCst);
        1
    }
    fn matches(&self, _other: &dyn novarocks_execution_contract::ConfidentialContent) -> bool {
        self.getters.fetch_add(1, Ordering::SeqCst);
        false
    }
    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        self.getters.fetch_add(1, Ordering::SeqCst);
        None
    }
}
impl Drop for RawDomainDropProbe {
    fn drop(&mut self) {
        let _ = self.entered.take().unwrap().send(thread::current().id());
        let mut open = self.gate.open.lock().unwrap();
        while !*open {
            open = self.gate.changed.wait(open).unwrap();
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct OriginalDomainIngressMarker {
    _permit: tokio::sync::OwnedSemaphorePermit,
    drops: Arc<AtomicUsize>,
}
impl crate::WorkerIngressBacking for OriginalDomainIngressMarker {}
impl Drop for OriginalDomainIngressMarker {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn observe_original_request(
    f: &Harness,
    work: ScopedWorkRef,
) -> crate::control_runtime::participation_host::requests::RequestCustodyObservation {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .requests[&work]
        .observation_for_test()
        .unwrap()
}

type OriginalLifecycleExecutor = crate::control_runtime::host_executor::HostExecutor<
    crate::control_runtime::participation_host::task_io::TaskIoHostJob,
    fn(crate::control_runtime::participation_host::task_io::TaskIoHostJob),
>;

pub(super) fn original_lifecycle_executor(
    runtime: &ActorRuntime<WorkerActor>,
    root_registration: novarocks_control_runtime::actor_turn::ActorRegistration,
) -> (
    OriginalLifecycleExecutor,
    crate::control_runtime::participation_host::task_io::TaskIoPorts,
) {
    use crate::control_runtime::{
        host_executor::{HostExecutor, HostExecutorLimits},
        participation_host::task_io::{TaskIoHostJob, TaskIoPorts},
        task_capacity::TaskCapacitySignal,
    };
    let capacity = TaskCapacitySignal::new(runtime.clone(), root_registration).unwrap();
    let one = NonZeroUsize::new(1).unwrap();
    let executor = HostExecutor::start_with_waker(
        HostExecutorLimits {
            jobs: one,
            per_group: one,
            workers: one,
        },
        TaskIoHostJob::run_on_host as fn(TaskIoHostJob),
        capacity.waker(),
    )
    .unwrap_or_else(|_| panic!("the original bounded lifecycle executor starts"));
    let ports = TaskIoPorts {
        lifecycle: Some(executor.handle()),
        capacity: Some(capacity),
        ..TaskIoPorts::default()
    };
    (executor, ports)
}

#[test]
fn bounded_failure_publication_preserves_the_published_reply_and_fills_only_the_empty_outbox() {
    let mut f = original_participation(2);
    let ready = ready(&f, 2);
    let (published_work, mut published_handle) = admit(&mut f, &ready, operation(131));
    let published_input = published_handle.request().token;
    submit_renew(&mut f, published_work);
    let ticket = reply(&f, published_input);
    let decided = match &ticket.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("the original Core-authored reply"),
    };
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let published_delivery = published_handle.try_take().unwrap();
    let original_body = published_delivery.body() == decided.as_ref()
        && !matches!(
            published_delivery.body(),
            ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
        );
    let (empty_work, mut empty_handle) = admit(&mut f, &ready, operation(132));
    let (empty_header, participation_scope, root_registration) = {
        let address = f.service.participation(f.identity).unwrap();
        let host = address.lock().unwrap();
        let original = host.resources.requests[&empty_work]
            .observation_for_test()
            .unwrap();
        assert!(original.authority_present && !original.started && !original.submitted);
        (
            original.header,
            host.scope().clone(),
            host.control_registration.clone(),
        )
    };
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();

    let zero_did_not_drain = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failure_publications(0)
        .unwrap();
    let zero_did_not_publish = empty_handle.try_take().is_none();
    let one_did_not_scan_all = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failure_publications(1)
        .unwrap()
        && empty_handle.try_take().is_none();
    let alive_at_zero_and_one = failure_is_still_started(&mut f, failure_input);
    let refusal = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(91).unwrap(),
            InputClass::Ordinary,
            operation(133),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the failed owner refuses new R before ordinary capacity checks");
    let new_r_closed = matches!(refusal.error, ParticipationRequestError::OwnerFailed)
        && refusal.partial.is_none();
    let refusal = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(empty_work, renew())
        .unwrap_err();
    let original_command_returned = matches!(refusal.error, ParticipationRequestError::OwnerFailed)
        && matches!(refusal.command, Some(ParticipationCommand::Renew { .. }));

    // This is the already-admitted R, with the same operation and unfilled
    // actual input. The real failed actor refuses its feedback input gate.
    let raw_drops = Arc::new(AtomicUsize::new(0));
    let raw_getters = Arc::new(AtomicUsize::new(0));
    let marker_drops = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(RawDomainDropGate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseRawDomainDrop(gate.clone());
    let (entered, physical_drop) = sync_channel(1);
    let raw = novarocks_execution_contract::AdvanceParticipationDomain::new(
        operation(132),
        f.identity,
        novarocks_execution_contract::ParticipationDomainUpdate::Credential(
            novarocks_execution_contract::CredentialUpdate::new(
                novarocks_execution_contract::CredentialDomainId::new(1),
                novarocks_execution_contract::CredentialEpoch::new(2).unwrap(),
                Arc::new(RawDomainDropProbe {
                    entered: Some(entered),
                    gate,
                    drops: raw_drops.clone(),
                    getters: raw_getters.clone(),
                }),
            ),
        ),
    );
    let ingress_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let ingress =
        crate::WorkerIngressResponsibility::from_admitted(Arc::new(OriginalDomainIngressMarker {
            _permit: ingress_gate.clone().try_acquire_owned().unwrap(),
            drops: marker_drops.clone(),
        }));
    let participation_before_capture = participation_scope.snapshot().unwrap();
    let participation_work_before = participation_before_capture.work;
    let root_work_before = f.scope.snapshot().unwrap().work;
    {
        let address = f.service.participation(f.identity).unwrap();
        let mut host = address.lock().unwrap();
        let (error, raw) = host
            .capture_participation_domain(empty_work, raw, PayloadId(132))
            .expect_err("the actual failed Actor rejects the original feedback input");
        assert!(matches!(
            error,
            ParticipationRequestError::ActorTurn(ActorTurnError::ActorFailed)
        ));
        assert!(host.resources.request_partials.is_empty());
        assert!(
            host.resources.requests[&empty_work]
                .observation_for_test()
                .unwrap()
                .domain
                .as_ref()
                .unwrap()
                .uncaptured_unused
        );
        assert_eq!(
            participation_scope.snapshot().unwrap().work,
            participation_work_before + 3,
            "the three actual unused feedback originals belong to this R"
        );
        assert_eq!(f.scope.snapshot().unwrap().work, root_work_before);
        host.capture_refused_participation_domain(empty_work, raw, ingress)
            .unwrap_or_else(|_| {
                panic!("same original failed R captures its whole raw and Native marker")
            });
        assert!(
            !host.resources.requests[&empty_work]
                .observation_for_test()
                .unwrap()
                .authority_present
        );
    }
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 2);
    assert_eq!(
        observe_original_request(&f, empty_work).return_state,
        novarocks_control_runtime_core::completion::ReturnState::Reserved
    );
    assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
    assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
    assert_eq!(marker_drops.load(Ordering::SeqCst), 0);
    assert_eq!(ingress_gate.available_permits(), 0);
    let (drained, failure_alive_through_drain) = drain_one_unit_turns(&mut f, failure_input);
    let failed_delivery = empty_handle
        .try_take()
        .expect("the empty original outbox receives OwnerFailed");
    let only_empty_failed = matches!(
        failed_delivery.body(),
        ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    );
    let already_published_preserved = published_delivery.body() == decided.as_ref();
    let physical_deliveries_still_paid = f.ledger.used(Dimension::Requests.id()) == 2;
    let (executor, ports) = original_lifecycle_executor(&f.runtime, root_registration);
    let mut feedback_retired = false;
    for _ in 0..16 {
        let address = f.service.participation(f.identity).unwrap();
        let mut host = address.lock().unwrap();
        host.advance_domain_request(empty_work, &ports).unwrap();
        if host.resources.requests[&empty_work]
            .observation_for_test()
            .unwrap()
            .domain
            .as_ref()
            .unwrap()
            .retired
        {
            feedback_retired = true;
            break;
        }
    }
    assert!(feedback_retired);
    assert_eq!(
        participation_scope.snapshot().unwrap().work,
        participation_work_before,
        "the exact unused feedback Work/Source headers have really retired"
    );
    assert_eq!(
        participation_scope.snapshot().unwrap().storage,
        participation_before_capture.storage,
        "the original unused feedback reports retire with their real holders"
    );
    assert!(!empty_header.is_retired());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 2);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request_notification(empty_work, &ports)
        .unwrap();
    let entered = physical_drop.recv_timeout(Duration::from_secs(10));
    // Gather while the real foreign destructor is held, then unconditionally
    // release and physically join before asserting the held observations.
    let blocked_retirement = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(empty_work);
    let blocked_work = f.scope.work_lifecycle(empty_work.work);
    let held_drops = raw_drops.load(Ordering::SeqCst);
    let held_marker = marker_drops.load(Ordering::SeqCst);
    let held_requests = f.ledger.used(Dimension::Requests.id());
    release.release();
    drop(
        executor
            .shutdown()
            .unwrap_or_else(|_| panic!("the actual lifecycle body drains and joins")),
    );
    assert_ne!(
        entered.unwrap(),
        thread::current().id(),
        "raw Drop must run on the real host worker"
    );
    assert!(matches!(blocked_retirement, Ok(false)));
    assert!(matches!(
        blocked_work.unwrap(),
        Some((
            novarocks_control_runtime_core::scope::WorkPhase::Started,
            false,
            false,
            _
        ))
    ));
    assert_eq!((held_drops, held_marker, held_requests), (0, 0, 2));
    assert_eq!(raw_drops.load(Ordering::SeqCst), 1);
    assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
    assert_eq!(
        observe_original_request(&f, empty_work).return_state,
        novarocks_control_runtime_core::completion::ReturnState::Filled
    );
    assert!(
        !f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request(empty_work)
            .unwrap(),
        "taking actual authority does not end the live delivery"
    );
    assert_eq!(
        observe_original_request(&f, empty_work).return_state,
        novarocks_control_runtime_core::completion::ReturnState::Taken
    );
    {
        let address = f.service.participation(f.identity).unwrap();
        let host = address.lock().unwrap();
        let original = host.resources.requests[&empty_work]
            .observation_for_test()
            .unwrap();
        assert_eq!(original.work, empty_work);
        assert!(original.authority_present && original.started && !original.submitted);
        assert_eq!(
            original.exit_observation.as_ref().unwrap().phase,
            novarocks_control_runtime::exit_custody::ExitCustodyPhase::Started
        );
        assert!(!original.exit_observation.as_ref().unwrap().body_exited);
        assert!(!original.exit_observation.as_ref().unwrap().captures_exited);
    }
    assert!(!empty_header.is_retired());
    assert_eq!(marker_drops.load(Ordering::SeqCst), 0);
    assert_eq!(ingress_gate.available_permits(), 0);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 2);
    drop(ports);
    drop((
        published_handle,
        published_delivery,
        empty_handle,
        failed_delivery,
    ));
    let requests_retired = advance(&mut f, published_work) && advance(&mut f, empty_work);
    assert!(empty_header.is_retired());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(raw_drops.load(Ordering::SeqCst), 1);
    assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
    assert_eq!(marker_drops.load(Ordering::SeqCst), 1);
    assert_eq!(ingress_gate.available_permits(), 1);
    let supervision_still_owned = failure_is_still_started(&mut f, failure_input);
    finish_failed_fixture(f, failure_input);

    assert!(original_body && already_published_preserved && only_empty_failed);
    assert!(zero_did_not_drain && zero_did_not_publish && one_did_not_scan_all);
    assert!(new_r_closed && original_command_returned);
    assert!(drained && alive_at_zero_and_one && failure_alive_through_drain);
    assert!(physical_deliveries_still_paid && requests_retired && supervision_still_owned);
}

#[derive(Debug)]
struct QueuedSource(Arc<AtomicUsize>);
impl std::fmt::Display for QueuedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original failed-owner queued source")
    }
}
impl std::error::Error for QueuedSource {}
impl FailureProjection for QueuedSource {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::WORKER, NonZeroU16::new(65012).unwrap()),
        )
    }
}
impl Drop for QueuedSource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn source_fees(f: &Harness) -> [u64; 2] {
    actual_component_inventory(f)
}

#[test]
fn only_the_real_publication_drain_abandons_a_queued_source_and_its_alias_still_controls_refund() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let ingress = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .port()
        .ingress()
        .clone();
    let scope = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let original_generation = scope.gates().unwrap().generation;
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = ExecutionFailure::from_domain(QueuedSource(drops.clone()));
    let early_alias = cause.clone();
    let handoff = ingress.submit(TaskConclusionProposal::Failed {
        cause,
        phase: TaskFailurePhase::Execution,
    });
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    let source = ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .into_iter()
        .find_map(|position| position.source)
        .unwrap();
    let custody = ingress.source_inventory(source).unwrap().custody;
    let fees = source_fees(&f);
    let not_started = matches!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .take_failed_task_open(task),
        Err(FailurePublicationError::NotStarted)
    );
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let not_drained = matches!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .take_failed_task_open(task),
        Err(FailurePublicationError::NotDrained)
    );
    let (drained, failure_alive) = drain_one_unit_turns(&mut f, failure_input);
    let original = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .take_failed_task_open(task)
        .unwrap()
        .expect(
            "the actual queued open summary transfers without an input or Core acknowledgement",
        );
    let exact_summary = original.cut.is_none()
        && original.summary.candidate_sources().collect::<Vec<_>>() == vec![source];
    let (abandoned_not_consumed, never_filled) = {
        let address = f.service.participation(f.identity).unwrap();
        let mut host = address.lock().unwrap();
        let open = host.task(task).unwrap().open_summary_mut().unwrap();
        (
            open.source_core_consumption(source).unwrap() == Consumption::AbandonedByOwnerFailure,
            open.source_input(source).unwrap().is_none(),
        )
    };
    let ordinal = ingress
        .pair_inventory(PairRoute::ConsumingOpen)
        .unwrap()
        .iter()
        .position(|position| position.source == Some(source))
        .unwrap();
    let retained = ingress
        .take_failed_source(PairRoute::ConsumingOpen, ordinal)
        .unwrap()
        .unwrap();
    let last_alias = retained.cause().clone();
    drop((retained, early_alias));
    let alias_still_live = drops.load(Ordering::SeqCst) == 0
        && !custody.actual_exit_ready().unwrap()
        && source_fees(&f) == fees;
    let no_report_exit = matches!(
        custody.report().consume(ConsumerId::new(1).unwrap()),
        Err(RuntimeError::Completion(
            novarocks_control_runtime_core::completion::CompletionError::NotFilled
        ))
    );
    drop(last_alias);
    let actual_source_exit =
        drops.load(Ordering::SeqCst) == 1 && custody.actual_exit_ready().unwrap();
    for holder in ingress
        .take_unused_pair(PairRoute::ConsumingOpen)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    custody
        .report()
        .abandon_failed_owner(ConsumerId::new(1).unwrap(), &scope, original_generation)
        .unwrap();
    let original_core_leg_abandoned = custody
        .report()
        .consumption(ConsumerId::new(1).unwrap())
        .unwrap()
        == Consumption::AbandonedByOwnerFailure;
    custody
        .report()
        .consume(ConsumerId::new(2).unwrap())
        .unwrap();
    for original in ingress.pair_inventory(PairRoute::ConsumingOpen).unwrap() {
        assert!(original.custody.try_retire().unwrap());
    }
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    let reaped = ingress
        .prepare_pair_reap(PairRoute::ConsumingOpen)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .retire_after_reap(reaped)
        .unwrap_or_else(|_| panic!("the original abandoned pair actually reaped"));
    let after_generation = source_fees(&f);
    ingress.seal_source_admission();
    let reaped = ingress
        .prepare_pair_reap(PairRoute::ConsumingOpen)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let detached = ingress
        .release_closed_route_pair(PairRoute::ConsumingOpen, reaped)
        .unwrap_or_else(|_| panic!("the closed original source slots detach after true reaping"));
    let before_release = source_fees(&f);
    drop(detached); // The original Source bundle finished at its actual ExitCustody retirement.
    let after_release = source_fees(&f);
    let only_actual_slot_release_refunded = fees
        .iter()
        .zip(after_generation)
        .all(|(before, after)| before.checked_sub(after) == Some(2))
        && before_release == after_generation
        && after_release == before_release;
    let failure_work_still_alive = failure_is_still_started(&mut f, failure_input);
    drop((original, custody, ingress));
    let headers = actual_participation_headers(&f);
    super::source_slot_lifetime::finish_after_consuming_pair_detached_with(
        f,
        task,
        preparation,
        |f| {
            complete_actual_failure_source(f, failure_input);
            assert!(
                headers.iter().all(|header| !header.is_retired()),
                "real Task/Source teardown does not invent Participation holder exit"
            );
        },
    );
    assert!(
        headers.iter().all(|header| header.is_retired()),
        "the original Participation holders retire only through the actual fixture teardown"
    );

    assert!(not_started && not_drained && drained && failure_alive);
    assert!(exact_summary && abandoned_not_consumed && never_filled);
    assert!(alias_still_live && no_report_exit && actual_source_exit);
    assert!(
        original_core_leg_abandoned
            && only_actual_slot_release_refunded
            && failure_work_still_alive
    );
}

#[test]
fn a_foreign_same_number_failure_ticket_cannot_start_the_local_work_and_returns_to_its_actual_owner()
 {
    let mut local = original_participation(0);
    let mut foreign = original_participation(0);
    activate(&mut local);
    activate(&mut foreign);
    let local_ticket = actual_failure(&mut local);
    let foreign_ticket = actual_failure(&mut foreign);
    let local_input = local_ticket.input;
    let foreign_input = foreign_ticket.input;
    let same_numeric_identity = local_ticket.actor == foreign_ticket.actor
        && local_ticket.descriptor.work == foreign_ticket.descriptor.work
        && local_ticket.input == foreign_ticket.input
        && local
            .service
            .participation(local.identity)
            .unwrap()
            .lock()
            .unwrap()
            .scope()
            .id()
            == foreign
                .service
                .participation(foreign.identity)
                .unwrap()
                .lock()
                .unwrap()
                .scope()
                .id();
    let original_work = foreign_ticket.descriptor.work;
    let refusal = local
        .service
        .participation(local.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(foreign_ticket)
        .unwrap_err();
    let rejected_origin = matches!(
        refusal.error,
        FailurePublicationError::ActorTurn(ActorTurnError::EffectTicketOriginMismatch)
    );
    let returned_original = refusal.ticket.actor == participation_actor()
        && refusal.ticket.input == foreign_input
        && refusal.ticket.descriptor.work == original_work;
    // Common rejects start on an already Started lease. Successful use of the
    // local original ticket proves the foreign refusal left its Work Admitted;
    // successful foreign use proves the returned ticket retained its origin.
    local
        .service
        .participation(local.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(local_ticket)
        .unwrap();
    foreign
        .service
        .participation(foreign.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(refusal.ticket)
        .unwrap();
    let (local_drained, local_alive) = drain_one_unit_turns(&mut local, local_input);
    let (foreign_drained, foreign_alive) = drain_one_unit_turns(&mut foreign, foreign_input);
    finish_failed_fixture(local, local_input);
    finish_failed_fixture(foreign, foreign_input);

    assert!(same_numeric_identity && rejected_origin && returned_original);
    assert!(local_drained && foreign_drained && local_alive && foreign_alive);
}

#[test]
fn real_parent_fence_uses_one_prepaid_participation_role_and_failed_owner_abandons_its_queued_input()
 {
    use crate::control_runtime::participation_host::owner_control::OwnerFenceDelivery;
    use novarocks_control_runtime::actor_turn::InputPhase;

    let mut f = original_participation(1);
    let root_fence_input = f
        .input
        .fill(WorkerMessage::Control(ControlMessage::Command {
            work: f.metadata_resources[2].work.reference(),
            request: None,
            command: ControlInput::FenceFrontend {
                frontend: f.identity.frontend_process_id(),
            },
        }))
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    let wake_actions = f
        .runtime
        .ready_effects(
            control_actor(),
            root_fence_input,
            ControlCore::MAX_STEP_WORK,
        )
        .unwrap();
    let mut wake = None;
    for action in wake_actions {
        let ticket = f
            .runtime
            .take_effect(control_actor(), root_fence_input, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::AdmissionEpoch(_) => {
                // The real fence also updates its neutral admission inventory.
                f.runtime.effect_completed(ticket).unwrap();
            }
            WorkerAction::Control {
                effect: ControlEffect::WakeInternal,
                ..
            } => {
                assert!(
                    wake.replace(ticket).is_none(),
                    "one original fence continuation"
                );
            }
            _ => panic!("the original fence emits only its projection and continuation"),
        }
    }
    let wake = wake.expect("the real fence schedules its own continuation");
    assert!(matches!(
        &wake.effect,
        WorkerAction::Control {
            effect: ControlEffect::WakeInternal,
            ..
        }
    ));
    // Consume this actual neutral wake before reusing the fixture's original
    // serial Control input role for Core-selected internal work. No new role,
    // synthetic token, or guessed ContinueFence operation is introduced.
    f.runtime.effect_completed(wake).unwrap();
    f.runtime
        .retire_input(control_actor(), root_fence_input)
        .unwrap();
    let root_input = f
        .input
        .fill(WorkerMessage::Control(ControlMessage::Command {
            work: f.metadata_resources[2].work.reference(),
            request: None,
            command: ControlInput::NextInternal,
        }))
        .unwrap();
    f.runtime.poll_one(Tick(3)).unwrap();
    let actions = f
        .runtime
        .ready_effects(control_actor(), root_input, ControlCore::MAX_STEP_WORK)
        .unwrap();
    let mut abort = None;
    let mut route_published = false;
    for action in actions {
        let ticket = f
            .runtime
            .take_effect(control_actor(), root_input, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::Route {
                        participation,
                        state,
                        ..
                    },
                ..
            } => {
                assert_eq!(*participation, f.identity);
                assert_eq!(*state, ParticipationState::Aborting);
                drop(
                    f.runtime
                        .commit_snapshot(&ticket, &f.route, Arc::new(*state))
                        .unwrap(),
                );
                f.runtime.effect_completed(ticket).unwrap();
                route_published = true;
            }
            WorkerAction::Control {
                effect: ControlEffect::AbortParticipation { participation, .. },
                ..
            } => {
                assert_eq!(*participation, f.identity);
                assert!(abort.replace(ticket).is_none());
            }
            other => {
                panic!("unexpected real Root fence effect requires its actual owner: {other:?}")
            }
        }
    }
    let abort =
        abort.expect("the real fenced Participation has one original AbortParticipation effect");
    let directory = f.service.directory();
    let address = directory.participation(f.identity).unwrap();
    let scope = address.lock().unwrap().scope().clone();
    let before_scope = scope.snapshot().unwrap();
    let before_actor = f.runtime.snapshot(participation_actor()).unwrap();
    let before_roles = f
        .runtime
        .standing_input_inventory(participation_actor(), None, 16)
        .unwrap();
    let paid = actual_admission_usage(&f);
    let original_parent_role = before_roles
        .iter()
        .find(|role| role.source == control_actor() && role.class == InputClass::Control)
        .expect("bootstrap already admitted the original parent-control role")
        .standing_input_id;
    let unused_is_not_retired = !address.lock().unwrap().retire_owner_control().unwrap();
    let delivered = directory.handoff_owner_fence(&abort).unwrap();
    let after_first = f.runtime.snapshot(participation_actor()).unwrap();
    let duplicate = directory.handoff_owner_fence(&abort).unwrap();
    let after_duplicate = f.runtime.snapshot(participation_actor()).unwrap();
    let input = f
        .runtime
        .standing_input_inventory(participation_actor(), None, 16)
        .unwrap()
        .into_iter()
        .find(|role| role.standing_input_id == original_parent_role)
        .unwrap()
        .in_flight
        .expect("the same original parent role holds its one actual input");
    let original_queued = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_some_and(|original| {
            original.standing_input_id == Some(original_parent_role)
                && original.source == control_actor()
                && original.class == InputClass::Control
                && original.phase == InputPhase::Filled
        });
    let original_parent_author = address.lock().unwrap().resources.admission.work.reference();
    let original_headers = f
        .runtime
        .input_header_authors(participation_actor(), input, 2)
        .unwrap();
    let process_author_survives_execution =
        original_headers == [original_parent_author] && original_parent_author.scope != scope.id();
    let queued_cannot_retire = !address.lock().unwrap().retire_owner_control().unwrap();
    let one_original_position = after_first.inputs == before_actor.inputs + 1
        && after_first.standing_inputs == before_actor.standing_inputs
        && after_first.batches == before_actor.batches
        && after_duplicate == after_first;
    let no_new_work_or_funding =
        scope.snapshot().unwrap() == before_scope && paid == actual_admission_usage(&f);
    let no_rpc_capture = {
        let host = address.lock().unwrap();
        host.resources.requests.is_empty()
            && host.resources.request_partials.is_empty()
            && f.ledger.used(Dimension::Requests.id()) == 0
    };

    // Lifecycle input from another actual source wins class priority over
    // queued parent Control. The real Core panics on an unknown Task; it has
    // never run OwnerFenced's unassembled foreign close/stop host actions.
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    let parent_still_queued = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let failed_delivery = directory.handoff_owner_fence(&abort).unwrap();
    let no_refill_after_failure = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let original_root_ticket_still_valid = f.runtime.validate_effect_ticket(&abort).is_ok();
    address
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let (drained, failure_alive) = drain_one_unit_turns(&mut f, failure_input);
    let still_not_consumed_by_publication = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let mut abandoned = false;
    for _ in 0..8 {
        let progress = address
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .unwrap();
        assert!(progress.units <= 1 && progress.taken <= progress.units);
        if f.runtime
            .input_custody(participation_actor(), input)
            .unwrap()
            .is_some_and(|original| original.phase == InputPhase::AbandonedByOwnerFailure)
        {
            abandoned = true;
            break;
        }
    }
    let retired_after_abandon = address.lock().unwrap().retire_owner_control().unwrap();
    let original_input_removed = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_none();
    let no_parent_role_after_retirement = !f
        .runtime
        .standing_input_inventory(participation_actor(), None, 16)
        .unwrap()
        .iter()
        .any(|role| role.standing_input_id == original_parent_role);
    f.runtime.effect_completed(abort).unwrap();
    f.runtime.retire_input(control_actor(), root_input).unwrap();
    // Existing metadata-only teardown finishes the genuine failure session,
    // then the original Root refusal/retention/removal. It is not Native exit.
    finish_failed_fixture(f, failure_input);

    assert!(route_published && original_root_ticket_still_valid);
    assert_eq!(delivered, OwnerFenceDelivery::Delivered);
    assert_eq!(duplicate, OwnerFenceDelivery::AlreadyDelivered);
    assert!(unused_is_not_retired && original_queued && queued_cannot_retire);
    assert!(one_original_position && no_new_work_or_funding && no_rpc_capture);
    assert!(
        process_author_survives_execution,
        "the original parent fence must retain its process payer across execution detachment"
    );
    assert_eq!(failed_delivery, OwnerFenceDelivery::FailedOwner);
    assert!(parent_still_queued && no_refill_after_failure && still_not_consumed_by_publication);
    assert!(drained && failure_alive && abandoned);
    assert!(retired_after_abandon && original_input_removed && no_parent_role_after_retirement);
}
