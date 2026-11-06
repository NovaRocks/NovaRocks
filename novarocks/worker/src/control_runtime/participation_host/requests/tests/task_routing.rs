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

//! Task routing retains the same Participation-owned Task and original request
//! through admission closure and actual unused request retirement.

use super::*;
use crate::control_runtime::{
    actors::tests::task, causal::CausalError, participation_host::ParticipationHostError,
};

#[test]
fn unknown_task_request_is_refused_without_a_direct_participation_request_fallback() {
    let f = original_participation(1);
    let ready = ready(&f, 1);
    let missing = task(f.identity, 1);
    let refusal = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_request(
            &ready,
            missing,
            InputClass::Ordinary,
            operation(101),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the actual Participation owns no such Task");
    assert!(matches!(
        refusal.error,
        ParticipationRequestError::UnknownTask
    ));
    assert!(refusal.partial.is_none());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    finish(f);
}

#[test]
fn closed_task_admission_keeps_the_original_request_routable_until_unused_retirement() {
    let (mut f, task) = live_loading_task_fixture();
    let ready = ready(&f, 1);
    // Quiescing closes this actor's external intake while its actual Task
    // remains live. Domain updates keep the ordinary causal route in this
    // window; they must neither become terminal nor use the control lane.
    f.runtime
        .close_actor_admission(participation_actor())
        .unwrap();
    assert!(
        !f.runtime
            .snapshot(participation_actor())
            .unwrap()
            .external_open
    );
    let (work, handle) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_domain_request(
            &ready,
            task,
            operation(102),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);
    let host_address = f.service.participation(f.identity).unwrap();
    let mut host = host_address.lock().unwrap();
    host.close_task_request_admission(task).unwrap();
    host.close_task_request_admission(task).unwrap();
    let refusal = host
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(103),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("closing new admission must preserve the original request");
    assert!(matches!(
        refusal.error,
        ParticipationRequestError::Causal(CausalError::RequestAdmissionClosed)
    ));
    assert!(refusal.partial.is_none());
    let removal = host
        .take_task(task)
        .err()
        .expect("the same Task still owns the admitted request cut");
    assert!(matches!(removal, ParticipationHostError::TaskStillOwned));
    assert!(host.task(task).is_some());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);

    // This exact request has never submitted a command or started Native
    // execution. Ending its real external wait lets its original sender and
    // unused author/source holders retire through the actual host route.
    drop(host);
    drop(handle);
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    assert!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .task(task)
            .is_some()
    );
    assert!(
        matches!(
            f.service
                .participation(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .take_task(task),
            Err(ParticipationHostError::TaskStillOwned)
        ),
        "the original Source/lifetime publication inventory remains owned after R exits"
    );
    finish_task_fixture(f, task);
}

#[test]
fn closed_task_read_keeps_one_original_r_and_expires_before_the_actual_read_seal() {
    closed_task_command_expires_before_read_seal(|task| ParticipationCommand::GetFinalInfo {
        task,
    });
}

#[test]
fn closed_task_cancel_keeps_one_original_r_and_expires_before_the_actual_read_seal() {
    closed_task_command_expires_before_read_seal(|task| ParticipationCommand::Cancel {
        task,
        reason: novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
    });
}

fn closed_task_command_expires_before_read_seal(
    command: impl FnOnce(TaskIdentity) -> ParticipationCommand,
) {
    let (mut f, task, mut preparation) = accepted_task_fixture();
    let ready = ready(&f, 1);
    let operation = operation(123);
    let deadline = MonotonicInstant::from_origin(Duration::from_nanos(20));
    let address = f.service.participation(f.identity).unwrap();
    // The queued original preparation never exposed a Native body. End its
    // actual unused holder and reap its real payment before normal retirement.
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    preparation.work.body_exited().unwrap();
    preparation.work.captures_exited().unwrap();
    let scope = address.lock().unwrap().scope().clone();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    finish_unused_bundle(&mut preparation.capacity);
    drop(preparation);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 0);
    let (work, mut handle) = {
        let mut host = address.lock().unwrap();
        let owner = host.task(task).unwrap();
        assert!(owner.request_admission_open());
        // This component has never exposed a Native host callback. Revoke
        // only those original unused roles through their real exit protocol.
        crate::control_runtime::task::retire_unused_host_calls_for_test(
            &owner.resources_for_retirement().host_calls,
            &f.ledger,
        );
        // Exercise the real normal-retirement transition. Its one-header
        // budget leaves the original Source tail, not a fabricated read seal.
        owner.advance_normal_retirement(&f.ledger).unwrap();
        let ingress = owner.port().ingress().clone();
        assert!(ingress.open_summary_drained());
        assert!(
            ingress
                .pair_inventory(crate::task_proposal_ingress::PairRoute::Open)
                .unwrap()
                .iter()
                .any(|source| !source.custody.reusable()),
            "the same original Source headers still owe actual retirement"
        );
        assert!(
            !owner.request_admission_open(),
            "normal Source closure must first fence new hot RPC cuts"
        );
        assert!(host.task_read_awaits_seal(task));
        assert!(!host.has_retired_task_read(task));
        let (work, handle) = host
            .admit_request(
                &ready,
                ActorId::new(91).unwrap(),
                InputClass::Control,
                operation,
                deadline,
            )
            .unwrap();
        host.submit_deferred_task_read(work, command(task)).unwrap();
        (work, handle)
    };
    let original = handle.request();
    assert_eq!(original.author, work);
    assert_eq!(original.operation, operation);
    assert_eq!(original.deadline, deadline);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);
    assert!(f.runtime.poll_one(Tick(7)).unwrap());
    complete_model_effects(&mut f, original.token);
    assert!(handle.try_take().is_none());
    assert!(!address.lock().unwrap().advance_request(work).unwrap());
    assert_eq!(
        f.ledger.used(Dimension::ActiveTasks.id()),
        1,
        "the original Source tail still retains this same Task capacity"
    );
    assert!(!address.lock().unwrap().has_retired_task_read(task));

    // The same real builtin timer fires while that original holder stays
    // live. Neither Task read sealing nor another admission supplies expiry.
    assert!(f.runtime.advance_time(Tick(20), 8).unwrap() > 0);
    let timer = f
        .runtime
        .pending_timer_input(participation_actor())
        .unwrap()
        .expect("the original Participation deadline filled its builtin timer");
    assert!(f.runtime.poll_one(Tick(20)).unwrap());
    complete_model_effects(&mut f, timer);
    f.runtime
        .retire_input(participation_actor(), timer)
        .unwrap();
    let delivered = handle
        .try_take()
        .expect("the original read deadline settled without a seal");
    assert_eq!(delivered.request(), original);
    assert!(matches!(
        delivered.body(),
        ParticipationReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
    ));
    drop((delivered, handle));
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    let host = address.lock().unwrap();
    assert!(host.resources.requests.is_empty());
    assert!(!host.has_retired_task_read(task));
    assert!(host.task_read_waiters.is_empty_for_test());
    drop(host);
    finish_task_fixture(f, task);
}

#[test]
fn refused_seal_continuation_keeps_owner_failure_and_original_r_until_delivery_exits() {
    use novarocks_worker_protocol::participation::TaskReadCommand;

    let mut f = original_participation(1);
    let ready = ready(&f, 1);
    let operation = operation(124);
    let deadline = MonotonicInstant::from_origin(Duration::from_secs(5));
    let address = f.service.participation(f.identity).unwrap();
    let (work, mut handle) = address
        .lock()
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(91).unwrap(),
            InputClass::Control,
            operation,
            deadline,
        )
        .unwrap();
    let original = handle.request();
    let failure = failure_publications::actual_failure(&mut f);
    let failure_input = failure.input;
    {
        let mut host = address.lock().unwrap();
        // The original neutral input fails at the actual mailbox fill. No
        // Task command or read-seal continuation reaches the failed Core.
        let refused = host
            .submit_request(
                work,
                ParticipationCommand::AwaitTaskReadSeal {
                    task: task(f.identity, 1),
                    command: TaskReadCommand::Cancel {
                        reason: novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
                    },
                },
            )
            .unwrap_err();
        assert!(matches!(
            refused.error,
            ParticipationRequestError::ActorTurn(ActorTurnError::ActorFailed)
        ));
        assert!(
            f.runtime
                .input_custody(participation_actor(), original.token)
                .unwrap()
                .is_some_and(
                    |input| input.phase == novarocks_control_runtime::actor_turn::InputPhase::Bound
                )
        );
        host.begin_failure_publications(failure).unwrap();
    }
    assert!(failure_publications::drain_one_unit_turns(&mut f, failure_input).0);
    let delivery = handle
        .try_take()
        .expect("the original failed-owner outbox is committed");
    assert_eq!(delivery.request(), original);
    assert!(matches!(
        delivery.body(),
        ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    ));
    {
        let mut host = address.lock().unwrap();
        // Drive the same failed-input consumer as the physical handoff owner.
        // Publication alone does not consume this refused original input.
        let failure = host.resources.failure.take().unwrap();
        let stage = failure.drained().unwrap();
        assert!(
            host.advance_refused_neutral_inputs(&stage, &mut None, 1)
                .unwrap()
        );
        host.resources.failure = Some(failure);
    }
    assert!(
        f.runtime
            .input_custody(participation_actor(), original.token)
            .unwrap()
            .is_some_and(|input| input.phase
                == novarocks_control_runtime::actor_turn::InputPhase::AbandonedByOwnerFailure)
    );
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert!(
        !advance(&mut f, work),
        "the actual external delivery is still held"
    );
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    drop((delivery, handle));
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    assert!(address.lock().unwrap().resources.requests.is_empty());
    failure_publications::finish_failed_fixture(f, failure_input);
}

#[test]
fn task_update_inspection_wakes_pending_future_before_its_original_raw_drop() {
    use novarocks_execution_contract::{
        CodecOwnedContent, ContentFingerprint, DomainVersion, TaskDomainUpdate, UpdateTask,
    };
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        time::Instant,
    };

    #[derive(Debug)]
    struct Raw {
        entered: Arc<AtomicBool>,
        release: Mutex<mpsc::Receiver<()>>,
        drops: Arc<AtomicUsize>,
    }
    impl CodecOwnedContent for Raw {
        fn fingerprint(&self) -> ContentFingerprint {
            panic!("metadata inspection must never fingerprint rich content")
        }
        fn encoded_len(&self) -> usize {
            panic!("metadata inspection must never size rich content")
        }
    }
    impl Drop for Raw {
        fn drop(&mut self) {
            self.entered.store(true, Ordering::SeqCst);
            let _ = self.release.lock().unwrap().recv();
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Ingress;
    impl crate::WorkerIngressBacking for Ingress {}

    let mut f = original_participation(1);
    let input = model_fact(
        &mut f,
        ParticipationInput::MaterializationFinished {
            result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
        },
        1,
    );
    complete_model_effects(&mut f, input);
    f.runtime
        .retire_input(participation_actor(), input)
        .unwrap();
    let ready = ready(&f, 1);
    let missing = task(f.identity, 1);
    let operation = operation(124);
    let address = f.service.participation(f.identity).unwrap();
    let root = address.lock().unwrap().control_registration.clone();
    let (executor, ports) =
        super::failure_publications::original_lifecycle_executor(&f.runtime, root);
    let entered = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let (release, receiver) = mpsc::channel();
    let raw = UpdateTask::try_new(
        operation,
        missing,
        vec![TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: Arc::new(Raw {
                entered: entered.clone(),
                release: Mutex::new(receiver),
                drops: drops.clone(),
            }),
        }],
    )
    .unwrap();
    let (work, mut handle, token) = {
        let mut host = address.lock().unwrap();
        let (work, handle) = host
            .admit_task_update_inspection(
                &ready,
                ActorId::new(91).unwrap(),
                missing,
                operation,
                MonotonicInstant::from_origin(Duration::from_secs(5)),
            )
            .unwrap();
        if let Err((error, raw, ingress)) = host.capture_task_update_inspection(
            work,
            raw,
            crate::WorkerIngressResponsibility::from_admitted(Arc::new(Ingress)),
        ) {
            // A refused capture still returns the caller's actual raw. Let
            // that destructor finish before reporting the original error.
            let _ = release.send(());
            drop((raw, ingress));
            panic!("the original R capture was refused: {error:?}");
        }
        (work, handle, host.resources.requests[&work].request().token)
    };
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut handle).poll(&mut cx).is_pending());
    address
        .lock()
        .unwrap()
        .submit_request(
            work,
            ParticipationCommand::InspectTaskUpdate { task: missing },
        )
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    address
        .lock()
        .unwrap()
        .publish_request_reply(reply(&f, token))
        .unwrap();

    // Only the real notification job can wake this already-Pending Future.
    // The bounded test-thread wait never polls it or registers a timer waker.
    let deadline = Instant::now() + Duration::from_secs(5);
    while wakes.0.load(Ordering::SeqCst) == 0
        && !entered.load(Ordering::SeqCst)
        && Instant::now() < deadline
    {
        address
            .lock()
            .unwrap()
            .advance_request_notification(work, &ports)
            .unwrap();
        std::thread::yield_now();
    }
    let woke_before_drop = wakes.0.load(Ordering::SeqCst) > 0 && !entered.load(Ordering::SeqCst);
    let delivered = if woke_before_drop {
        match Pin::new(&mut handle).poll(&mut cx) {
            Poll::Ready(Ok(delivery)) => matches!(
                delivery.body(),
                ParticipationReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::InvalidStateOrRequest
                ))
            ),
            _ => false,
        }
    } else {
        false
    };
    // End the actual delivery/observer before raw disposal becomes eligible.
    drop(handle);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
        address
            .lock()
            .unwrap()
            .advance_request_notification(work, &ports)
            .unwrap();
        std::thread::yield_now();
    }
    let raw_held = entered.load(Ordering::SeqCst) && drops.load(Ordering::SeqCst) == 0;
    let held_requests = f.ledger.used(Dimension::Requests.id());
    let held_retirement = address.lock().unwrap().advance_request(work).unwrap();

    // Clean up even when an old ordering blocks the original wake behind Drop.
    let _ = release.send(());
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut retired = false;
    while !retired && Instant::now() < deadline {
        let mut host = address.lock().unwrap();
        host.advance_request_notification(work, &ports).unwrap();
        retired = host.advance_request(work).unwrap();
        drop(host);
        std::thread::yield_now();
    }
    drop(
        executor
            .shutdown()
            .unwrap_or_else(|_| panic!("the actual lifecycle executor joins")),
    );
    drop(ports);
    finish(f);
    assert!(
        woke_before_drop,
        "the original pending Future must receive a real wake before raw Drop"
    );
    assert!(
        delivered,
        "the original Core refusal must be ready on that wake"
    );
    assert!(raw_held && !held_retirement);
    assert_eq!(held_requests, 1);
    assert!(retired);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
