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

//! The production attempt assembler with its original Sources and no poller pool.

use super::*;
use crate::control_runtime::actor::FrontendEffect;
use crate::{
    native::task_transport::tests::Loopback,
    query_execution::native_execution_adapter::source_cohort, task_execution::clock::ManualClock,
};
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::joint_admission::JointProgressDriver;
use std::{future::Future, task::Poll, time::Duration};

// The original finite queue turns are driven even while the external native
// future is pending. No Core input, source callback or exit fact is synthesized.
async fn drive_until<F: Future>(
    process: &FrontendAttemptRuntime,
    joint: &mut JointProgressDriver,
    future: F,
    until: Instant,
) -> Option<F::Output> {
    drive_until_with_original_clock(process, joint, future, until, None).await
}

async fn drive_until_with_original_clock<F: Future>(
    process: &FrontendAttemptRuntime,
    joint: &mut JointProgressDriver,
    future: F,
    until: Instant,
    clock: Option<&ManualClock>,
) -> Option<F::Output> {
    let mut future = Box::pin(future);
    loop {
        source_cohort::tests::drive(joint);
        let now = process.clock.now();
        process.runtime.advance_time(now, 64).unwrap();
        let mut runnable = false;
        for _ in 0..64 {
            let core = process
                .runtime
                .poll_one_with_clock(process.clock.as_ref())
                .unwrap();
            let host_claimed = if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
                true
            } else {
                false
            };
            if !core && !host_claimed {
                break;
            }
            runnable = true;
        }
        if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
            return Some(value);
        }
        if Instant::now() >= until {
            return None;
        }
        if !runnable
            && let Some(clock) = clock
            && let Some(next) = process.runtime.next_deadline()
            && next > process.clock.now()
        {
            // Select only an existing indexed future deadline after all
            // currently runnable original Core/Host work has been polled.
            clock.advance(Duration::from_nanos(next.0 - process.clock.now().0));
        }
        tokio::task::yield_now().await;
    }
}

use crate::fixtures::native_recipe::{
    OriginalFixture, ReadCloseGate, count_description, install_original_fixture,
    install_original_fixture_shape,
};
use novarocks_query_application::coordination::LogicalExecutionSupervisor;

#[tokio::test]
async fn installed_unstarted_single_participation_closes_all_original_sources() {
    run_original_recipe(true, false, false, false).await;
}
#[tokio::test]
async fn installed_live_single_root_subscription_normally_converges_and_really_closes() {
    run_original_recipe(false, false, false, false).await;
}
#[tokio::test]
async fn installed_reopen_retires_original_final_phase_before_reusing_paid_source() {
    run_original_recipe(false, true, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn installed_core_failure_waits_for_original_read_close_and_really_retires() {
    run_original_recipe(false, false, true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn installed_shutdown_keeps_same_join_and_native_work_after_cancelled_wait() {
    run_original_recipe(false, false, false, true).await;
}
#[tokio::test]
async fn installed_three_task_protocol_recipe_keeps_actual_graph_and_closes_all_original_sources() {
    run_original_recipe_shape(false, false, false, false, true).await;
}
async fn run_original_recipe(
    unstarted: bool,
    controlled_reopen: bool,
    failure_during_read_close: bool,
    shutdown_during_read_close: bool,
) {
    run_original_recipe_shape(
        unstarted,
        controlled_reopen,
        failure_during_read_close,
        shutdown_during_read_close,
        false,
    )
    .await;
}
async fn run_original_recipe_shape(
    unstarted: bool,
    controlled_reopen: bool,
    failure_during_read_close: bool,
    shutdown_during_read_close: bool,
    three_tasks: bool,
) {
    let wait = Duration::from_secs(5);
    let mut data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut peer = Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    let OriginalFixture {
        mut fixture,
        mut logical,
        process,
        clock,
        mut joint,
        workload,
        mut supervisor,
        scope,
        business,
        capacity,
        window,
        cohort,
        requester,
        mut output,
    } = install_original_fixture_shape(&data, descriptor, history.clone(), three_tasks);
    let original_cancellation = scope.cancellation().unwrap();
    if three_tasks {
        assert!(
            !unstarted
                && !controlled_reopen
                && !failure_during_read_close
                && !shutdown_during_read_close
        );
        logical
            .probe
            .graph_capture_requested
            .store(true, std::sync::atomic::Ordering::Release);
    }
    let held_read_close = failure_during_read_close || shutdown_during_read_close;
    let read_close_gate = held_read_close.then(|| Arc::new(ReadCloseGate::default()));
    let _release_on_unwind = read_close_gate.as_ref().map(|gate| gate.release_guard());
    *logical.probe.read_close_gate.lock().unwrap() = read_close_gate.clone();
    let installed = fixture
        .activate_attempt(&logical, &mut joint, unstarted)
        .await;
    let (actor_id, subscription_source) = installed.fixture_sources;
    let root_task = logical.probe.root.lock().unwrap().unwrap();
    let original_participations = logical.probe.participations.lock().unwrap().clone();
    let three_task_roster = three_tasks.then(|| {
        let snapshot = logical
            .probe
            .graph_capture
            .lock()
            .unwrap()
            .take()
            .expect("same original protocol graph was captured before transfer");
        assert_eq!(snapshot.root, root_task);
        assert_eq!(snapshot.tasks.len(), 3);
        assert_eq!(snapshot.stages.len(), 3);
        assert_eq!(snapshot.edges.len(), 2);
        assert_eq!(snapshot.fragments, vec![1, 2, 3]);
        assert_eq!(snapshot.participations, original_participations);
        assert_eq!(snapshot.plan, count_description().1);
        let actual = snapshot
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        assert_eq!(
            actual
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            logical
                .probe
                .tasks
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
        );
        <[novarocks_execution_contract::TaskIdentity; 3]>::try_from(actual).unwrap()
    });
    let original_endpoint = held_read_close.then(|| {
        let activation = logical.probe.activation.lock().unwrap().unwrap();
        logical
            .binding
            .bound()
            .unwrap()
            .links
            .endpoint(activation)
            .unwrap()
    });
    let original_supervision = failure_during_read_close.then(|| {
        logical
            .exit
            .as_ref()
            .unwrap()
            .owner_failure_observation()
            .unwrap()
    });
    let kind = novarocks_result_contract::RootOutputKind::CountOnly;
    if !unstarted {
        if controlled_reopen {
            peer.enable_single_root_first_subscription_eof_before_create(root_task, kind);
        } else {
            peer.enable_single_root_lifecycle(root_task, kind);
        }
        if failure_during_read_close {
            peer.defer_single_root_release_until_abort().unwrap();
        }
        if let Some(roster) = three_task_roster {
            peer.enable_three_task_roster(roster).unwrap();
        }
        logical.probe.release_activation();
    } else {
        requester
            .request(novarocks_workload_control::CancellationReason::Requested)
            .unwrap();
    }
    let mut normal_future = logical.ready.take().unwrap();
    use novarocks_control_runtime::actor_turn::{ActorTurnError, InputPhase};
    use novarocks_control_runtime_core::{history::HistoryFact, mailbox::InputClass};
    let mut early_normal = None;
    let mut setup = None;
    let mut body_exit = false;
    let mut blocked = None;
    let mut second_subscribe = !controlled_reopen;
    let mut captured_history = Vec::new();
    if controlled_reopen {
        let mut first_open = None;
        let deadline = Instant::now() + wait;
        // Stop Core before handing off its first real Open. No phase or
        // input is assigned: the Host and original transport still progress.
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            process
                .runtime
                .advance_time(process.clock.now(), 64)
                .unwrap();
            if first_open.is_none() {
                process
                    .runtime
                    .poll_one_with_clock(process.clock.as_ref())
                    .unwrap();
                first_open = fixture_pending_open(&process, actor_id, None);
            }
            if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
            }
            if early_normal.is_none()
                && let Poll::Ready(value) = futures::poll!(normal_future.as_mut())
            {
                early_normal = Some(value);
            }
            if first_open.is_some() && peer.single_root_first_subscription_waiting() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let inputs = process
            .runtime
            .input_inventory(actor_id, None, 4096)
            .unwrap();
        let phases = inputs
            .iter()
            .filter(|row| {
                row.source == subscription_source
                    && row.class == InputClass::Lifecycle
                    && row.phase == InputPhase::Filled
            })
            .copied()
            .collect::<Vec<_>>();
        if let (Some(open), [phase]) = (first_open, phases.as_slice()) {
            let authors = process
                .runtime
                .input_header_authors(actor_id, phase.input, 4)
                .unwrap();
            if let [work] = authors.as_slice() {
                setup = Some((open, *phase, *work));
            }
        }
        peer.release_single_root_first_subscription();
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
            }
            captured_history.extend(history.drain(4096).entries);
            if let Some((_, _, original)) = setup {
                body_exit = captured_history.iter().any(|e| matches!(e.fact,
                    HistoryFact::BodyExited { scope, work } if (scope, work) == (original.scope, original.work)))
                    && captured_history.iter().any(|e| matches!(e.fact,
                    HistoryFact::CapturesExited { scope, work } if (scope, work) == (original.scope, original.work)));
                if body_exit {
                    // The result was published before these actual reports;
                    // drain its original Host without polling the paused Core.
                    for _ in 0..8 {
                        if let Some(turn) = process.runtime.claim_host_actor() {
                            process.host.drive(process.clock.now(), turn);
                        }
                        tokio::task::yield_now().await;
                    }
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        // The actual EOF was received at the frozen original clock reading.
        // Let its first retry (100 ms with at most 20% jitter in
        // CoveredRetryBudget) become due while Core still has queued work.
        // This advances only the injected clock, never an input or phase.
        if body_exit {
            clock.advance(Duration::from_millis(120));
        }
        // Never require the old early Exit publication. A fixed Source can
        // hold that original result until the final phase actually retires.
        let deadline = Instant::now() + wait;
        let mut advanced_retry = false;
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            process
                .runtime
                .advance_time(process.clock.now(), 64)
                .unwrap();
            let core_ran = process
                .runtime
                .poll_one_with_clock(process.clock.as_ref())
                .unwrap();
            let mut host_ran = false;
            if let Some(turn) = process.runtime.claim_host_actor() {
                host_ran = true;
                if let Some((first, phase, _)) = setup
                    && let Some(open) = fixture_pending_open(&process, actor_id, Some(first.3))
                {
                    let inputs = process
                        .runtime
                        .input_inventory(actor_id, None, 4096)
                        .unwrap();
                    if let Some(last) = inputs.iter().find(|row| {
                        row.source == subscription_source
                            && row.standing_input_id == phase.standing_input_id
                            && row.input != phase.input
                            && row.phase == InputPhase::Filled
                    }) && process.runtime.pending_timer_input(actor_id).unwrap() == Some(open.0)
                        && process
                            .runtime
                            .effect_ready_for(&turn, open.0, open.1)
                            .unwrap_or(false)
                        && process.runtime.input_retirement_status(actor_id, open.0)
                            == Err(ActorTurnError::EffectsRemain)
                    {
                        blocked = Some((open, last.input));
                    }
                }
                process.host.drive(process.clock.now(), turn);
            }
            captured_history.extend(history.drain(4096).entries);
            if early_normal.is_none()
                && let Poll::Ready(value) = futures::poll!(normal_future.as_mut())
            {
                early_normal = Some(value);
            }
            if peer.subscription_requests().len() >= 2 {
                second_subscribe = true;
                break;
            }
            // Advance only an original indexed future deadline, once, after
            // actual queued Core/Host work ran out. No Timer fact is authored.
            if !core_ran
                && !host_ran
                && !advanced_retry
                && let Some(next) = process.runtime.next_deadline()
                && next > process.clock.now()
            {
                clock.advance(Duration::from_nanos(next.0 - process.clock.now().0));
                advanced_retry = true;
            }
            tokio::task::yield_now().await;
        }
        peer.release_single_root_first_subscription();
        peer.release_single_root_create();
    }
    let normal_first = if early_normal.is_some() {
        early_normal
    } else {
        drive_until(
            &process,
            &mut joint,
            normal_future.as_mut(),
            Instant::now() + wait,
        )
        .await
    };
    let normal_within_watchdog = normal_first.is_some();
    let normal = if normal_first.is_some() {
        normal_first
    } else {
        // Keep this exact run/consumer/seal future pinned. Cancellation is
        // submitted to its original Work owner; no seal or run is recreated.
        let _ = requester.request(novarocks_workload_control::CancellationReason::Requested);
        drive_until(
            &process,
            &mut joint,
            normal_future.as_mut(),
            Instant::now() + wait,
        )
        .await
    };
    drop(normal_future);
    let mut ready_receiving = None;
    let mut actual_ready_cut = None;
    let mut original_business_error = None;
    let ready_ok = match normal {
        Some(Ok(mut original)) => {
            let token = *original
                .ready_token()
                .expect("real internal computation Ready");
            let received = output.take_ready(token).unwrap();
            actual_ready_cut = Some(token);
            assert_eq!(received.ready_token(), token);
            assert_eq!(received.output_rows(), token.end().output_rows);
            assert_eq!(
                received.expected_tasks(),
                logical.probe.tasks.lock().unwrap().as_slice()
            );
            if let Some(exit) = original.take_exit_observer() {
                assert!(logical.exit.is_none());
                logical.exit = Some(exit);
            }
            ready_receiving = Some((original, received));
            true
        }
        Some(Err(error)) => {
            original_business_error = Some(error);
            false
        }
        None => false,
    };
    let mut original_shutdown_wait = None;
    let mut original_shutdown_result = None;
    if let Some(gate) = &read_close_gate {
        assert!(ready_ok, "original Ready precedes the held cleanup");
        let entered = drive_until(
            &process,
            &mut joint,
            async {
                while !gate.entered() {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait,
        )
        .await;
        assert!(
            entered.is_some(),
            "same original seal callback never entered"
        );
        assert_eq!(gate.calls(), 1);
        assert!(
            !logical
                .probe
                .root_port
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_sealed_for_fixture()
        );
        assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
        if shutdown_during_read_close {
            // Cancelling this borrow must retain the same supervisor Join,
            // Registry registration and actual installed Native work.
            let shutdown_wait = original_shutdown_wait
                .insert(Box::pin(supervisor.shutdown_until(Instant::now() + wait)));
            assert!(futures::poll!(shutdown_wait.as_mut()).is_pending());
            let shutdown_observed = drive_until(
                &process,
                &mut joint,
                async {
                    loop {
                        assert!(futures::poll!(shutdown_wait.as_mut()).is_pending());
                        if scope.cancellation().unwrap().reason()
                            == Some(novarocks_workload_control::CancellationReason::ServerShutdown)
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                },
                Instant::now() + wait,
            )
            .await;
            assert!(
                shutdown_observed.is_some(),
                "original Registry close did not reach the same Root"
            );
            assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
            assert_eq!(gate.calls(), 1);
            {
                let exit = logical.exit.as_mut().unwrap();
                assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
            }
            let mut borrowed_wait = Box::pin(async { shutdown_wait.as_mut().await });
            assert!(futures::poll!(borrowed_wait.as_mut()).is_pending());
            drop(borrowed_wait);
            // Only this await borrow ended. The SAME pinned shutdown future,
            // original Join, registration and pending exit remain retained.
            assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
            {
                let exit = logical.exit.as_mut().unwrap();
                assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
            }
            gate.release();
        } else {
            assert!(!peer.single_root_release_observed());
            let original = logical.binding.bound().unwrap();
            let publications =
                crate::statistics::attempt_executor::StatisticsLateSupervisionProbes::arm(
                    original_supervision.as_ref().unwrap().clone(),
                )
                .await;
            let work = logical.binding.inject_original_core_fault_for_fixture();
            let failed_owner_started = drive_until(&process, &mut joint, async {
            loop {
                captured_history.extend(history.drain(4096).entries);
                let started = captured_history.iter().any(|entry| matches!(entry.fact, HistoryFact::Started { scope, work: id } if scope == work.scope && id == work.work));
                if started && process.runtime.failure_proof_for(&original.registration).is_ok() { break; }
                tokio::task::yield_now().await;
            }
        }, Instant::now()+wait).await;
            assert!(
                failed_owner_started.is_some(),
                "same paid failure work did not start after actual receiver failure"
            );
            assert!(
                original_supervision
                    .as_ref()
                    .unwrap()
                    .failure()
                    .unwrap()
                    .is_some(),
                "same outer statement observes failure before actual read close or runtime exit"
            );
            assert!(!captured_history.iter().any(|entry| matches!(entry.fact, HistoryFact::BodyExited { scope, work: id } | HistoryFact::CapturesExited { scope, work: id } | HistoryFact::WorkRetired { scope, work: id } if scope == work.scope && id == work.work)));
            assert_eq!(gate.calls(), 1);
            assert!(
                !logical
                    .probe
                    .root_port
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .is_sealed_for_fixture()
            );
            assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
            // The original callback must wake failed-owner control without any
            // clock advance. Require the actual AP Scope stop and first Abort RPC
            // before selecting its original retry/convergence deadlines.
            publications
                .release_and_check(
                    &original_supervision
                        .as_ref()
                        .unwrap()
                        .failure()
                        .unwrap()
                        .unwrap(),
                )
                .await;
            gate.release();
            let ap_scope = original_endpoint.as_ref().unwrap().attempt().scope_id();
            let stopped_without_clock = drive_until(&process, &mut joint, async {
            loop {
                captured_history.extend(history.drain(4096).entries);
                if peer.single_root_abort_observed() && captured_history.iter().any(|entry| matches!(entry.fact, HistoryFact::ScopeStopped {scope} if scope == ap_scope)) { break; }
                tokio::task::yield_now().await;
            }
        }, Instant::now()+wait).await;
            assert!(
                stopped_without_clock.is_some(),
                "same callback result did not drive the original AP stop and Abort RPC without a clock advance: {}",
                logical.binding.original_failure_for_fixture()
            );
        }
    }
    // Receiving Ready does not prove closure. Release the original receiving
    // value, then poll the same QA actual-exit observer and AP HostExit.
    drop(ready_receiving.take());
    let mut exit = logical.exit.take().expect("same original observed exit");
    let closure = drive_until_with_original_clock(
        &process,
        &mut joint,
        std::future::poll_fn(|cx| {
            // The same original shutdown sweep must keep progressing while
            // the independent original physical-exit observer is polled.
            let progress = original_shutdown_wait
                .as_mut()
                .map(|waiting| std::future::Future::poll(waiting.as_mut(), cx));
            if let Some(Poll::Ready(result)) = progress {
                original_shutdown_result = Some(result);
                original_shutdown_wait.take();
            }
            exit.poll_exit(cx)
        }),
        Instant::now() + wait,
        held_read_close.then_some(clock.as_ref()),
    )
    .await;
    if closure.is_none() {
        let lp = logical.binding.bound().unwrap().registration.actor();
        eprintln!(
            "original installed closure pending: clock={:?}, next={:?}, lp={:?}, ap={:?}, lp_inputs={:?}, lp_effects={:?}, ap_effects={:?}, unresolved={:?}, shutdown={original_shutdown_result:?}",
            process.clock.now(),
            process.runtime.next_deadline(),
            process.runtime.snapshot(lp),
            process.runtime.snapshot(actor_id),
            process.runtime.input_inventory(lp, None, 64),
            original_pending_protocol(&process, lp),
            original_pending_protocol(&process, actor_id),
            process.ledger.unresolved_work(),
        );
    }
    let convergence_within_watchdog = closure.is_some();
    let original_owner_error = closure
        .as_ref()
        .and_then(|result| result.as_ref().err())
        .cloned();
    let original_runtime_failure = closure.as_ref().and_then(|result| {
        let view = result.as_ref().ok()?.view();
        match view.runtime_actor_result()? {
            novarocks_query_application::api::logical_runtime::LogicalExit::OwnerFailed(error) => {
                Some(error.clone())
            }
            novarocks_query_application::api::logical_runtime::LogicalExit::Completed => None,
        }
    });
    // Preserve the original carrier's raw disposition. Paid Native tails
    // return their actual role footer; they do not mint a Tokio Source Join.
    let qa_local_return = closure
        .as_ref()
        .and_then(|result| result.as_ref().ok())
        .map(|receipt| receipt.local_disposition());
    let original_remote_closure = actual_ready_cut.is_some_and(|cut| {
        matches!(
            logical
                .binding
                .original_convergence_for_fixture(cut)
                .unwrap(),
            Some((true, _, true))
        )
    });
    if failure_during_read_close {
        // A health verdict can precede physical teardown. Independently wait
        // for the same AP witness and both actual runtime Scope removals.
        let physical_exit = drive_until_with_original_clock(
            &process,
            &mut joint,
            async {
                while !*installed.fixture_exit.observe_actual_exit().borrow()
                    || !process.runtime.is_empty()
                    || !process.host.is_empty()
                    || !process.ledger.is_empty()
                {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait,
            Some(clock.as_ref()),
        )
        .await;
        assert!(
            physical_exit.is_some(),
            "original failed-owner physical cleanup remains unproven: ap_exit={}, runtime_empty={}, host_empty={}, ledger_empty={}, lp={:?}, ap={:?}, native={}, owner_error={original_owner_error:?}, release={}, unresolved={:?}, lp_pending={:?}, ap_pending={:?}, ap_inputs={:?}",
            *installed.fixture_exit.observe_actual_exit().borrow(),
            process.runtime.is_empty(),
            process.host.is_empty(),
            process.ledger.is_empty(),
            process
                .runtime
                .snapshot(logical.binding.bound().unwrap().registration.actor()),
            process.runtime.snapshot(actor_id),
            logical.binding.original_failure_for_fixture(),
            peer.single_root_release_observed(),
            process.ledger.unresolved_work(),
            original_pending_protocol(
                &process,
                logical.binding.bound().unwrap().registration.actor()
            ),
            original_pending_protocol(&process, actor_id),
            process.runtime.input_inventory(actor_id, None, 64)
        );
    }
    let actual_ap_exit = *installed.fixture_exit.observe_actual_exit().borrow();
    if let Some(original) = &original_supervision {
        assert_eq!(original.failure().unwrap(), original_runtime_failure);
    }
    let subscribed = !peer.subscription_requests().is_empty();
    let released = peer.single_root_release_observed();
    let no_native_dispatch =
        peer.subscription_requests().is_empty() && peer.applied_requests().is_empty();
    let read_closed = logical
        .probe
        .root_port
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|port| port.is_sealed_for_fixture());
    let original_ap_closure = original_endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.facts.completion_receipt());
    let (end_offered, actual_acked) = peer.single_root_result_progress();
    eprintln!(
        "original actual fixture result: unstarted={unstarted}, ready_ok={ready_ok}, actual_ap_exit={actual_ap_exit}, qa_local_return={qa_local_return:?}, original_remote_closure={original_remote_closure}, read_closed={read_closed}, original_business_error={original_business_error:?}, native_failure={}",
        logical.binding.original_failure_for_fixture()
    );
    drop(original_endpoint);
    drop((
        closure, exit, installed, logical, output, cohort, window, requester, capacity,
    ));
    let waited = {
        let waiting = original_shutdown_wait.take();
        match waiting {
            Some(waiting) => {
                drive_until(&process, &mut joint, waiting, Instant::now() + wait).await
            }
            None => None,
        }
    };
    drop(original_shutdown_wait);
    let qa_exit = if shutdown_during_read_close {
        match original_shutdown_result {
            Some(result) => Some(result),
            None => waited,
        }
    } else {
        drive_until(
            &process,
            &mut joint,
            supervisor.shutdown_until(Instant::now() + wait),
            Instant::now() + wait,
        )
        .await
    };
    let qa_terminal = matches!(qa_exit, Some(Ok(())));
    if qa_terminal {
        drop(supervisor);
    } else {
        // Keep the same unresolved owner. A failed shutdown is not a process
        // exit and cannot authorize the production abandon operation.
        eprintln!("original QA shutdown remains unproven: {qa_exit:?}");
        static UNRESOLVED: std::sync::OnceLock<Mutex<Vec<LogicalExecutionSupervisor>>> =
            std::sync::OnceLock::new();
        UNRESOLVED
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .push(supervisor);
    }
    process.host.close_registration();
    let host_exit = drive_until(
        &process,
        &mut joint,
        async {
            while !process.host.is_empty() {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + wait,
    )
    .await;
    fixture.finish().await;
    drop(fixture);
    business.release();
    source_cohort::tests::drive(&mut joint);
    let scope_released = tokio::time::timeout(wait, scope.wait_released()).await;
    drop(scope);
    if !unstarted && !failure_during_read_close && !shutdown_during_read_close {
        assert_eq!(
            original_cancellation.reason(),
            None,
            "Registry retirement preserves the same concluded Root's uncancelled work"
        );
    }
    // The observation retains the original cancellation cell, although it
    // grants no Work authority. Its final alias must exit before WC drain.
    drop(original_cancellation);
    workload.close_admission();
    source_cohort::tests::drive(&mut joint);
    let workload_exit = workload.shutdown();
    let apply_exit = data.drain_original_apply_until(Instant::now() + wait).await;
    let outgoing_exit = data
        .drain_native_outgoing_until(Instant::now() + wait)
        .await;
    let peer_exit = peer.settle().await;
    let clean = process.host.is_empty()
        && process.runtime.is_empty()
        && process.ledger.is_empty()
        && process.members.is_empty();
    assert!(
        clean,
        "original runtime/host/ledger/membership cleanup failed"
    );
    assert!(
        qa_terminal && scope_released.is_ok() && workload_exit.is_ok(),
        "original QA/WC exit failed: qa={qa_exit:?}, scope={scope_released:?}, workload_ok={}",
        workload_exit.is_ok()
    );
    assert!(apply_exit.is_ok() && outgoing_exit.is_ok() && peer_exit);
    assert!(host_exit.is_some() && actual_ap_exit);
    if failure_during_read_close {
        assert!(
            original_owner_error.is_none() && original_runtime_failure.is_some(),
            "actual physical receipt retains its distinct original isolated failure"
        );
        assert!(
            qa_local_return.is_some(),
            "the same local return remains independent of its isolated failure"
        );
        let input = original_runtime_failure
            .as_ref()
            .unwrap()
            .execution_failure()
            .unwrap();
        use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
        assert_eq!(input.fact().class(), FailureClass::Invariant);
        assert_eq!(
            input.fact().reason(),
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(84).unwrap())
        );
        assert_eq!(
            input.site(),
            novarocks_coordination_contract::ExecutionFailureSite::Coordinator
        );
        assert!(normal_within_watchdog && convergence_within_watchdog && ready_ok);
        assert!(subscribed && read_closed && end_offered && actual_acked == 1);
        assert_eq!(read_close_gate.as_ref().unwrap().calls(), 1);
        let receipt = original_ap_closure
            .as_ref()
            .expect("same original AP convergence receipt");
        assert_eq!(receipt.activation(), actual_ready_cut.unwrap().activation());
        match receipt.closure().as_ref() {
            novarocks_coordination_contract::AttemptConvergenceClosure::Evidence(_) => assert!(
                released,
                "positive remote closure requires the actual peer's release"
            ),
            novarocks_coordination_contract::AttemptConvergenceClosure::RemoteTrackingEnded {
                exact_participations,
            } => {
                assert_eq!(
                    exact_participations.as_ref(),
                    original_participations.as_slice()
                );
                assert!(
                    !original_remote_closure,
                    "remote tracking end must not become a positive Ready convergence publication"
                );
            }
        }
        assert!(history.is_complete());
    } else {
        assert!(matches!(qa_local_return,Some(
            novarocks_query_application::coordination::LocalExecutionDisposition::ActualSourcesExited
            | novarocks_query_application::coordination::LocalExecutionDisposition::OriginalPaidOwnerReturn
        )));
    }
    if unstarted {
        assert!(no_native_dispatch);
    } else if shutdown_during_read_close {
        assert!(normal_within_watchdog && convergence_within_watchdog && ready_ok);
        assert!(original_owner_error.is_none() && original_runtime_failure.is_none());
        assert!(subscribed && read_closed && end_offered && actual_acked == 1);
        assert_eq!(read_close_gate.as_ref().unwrap().calls(), 1);
        let receipt =
            original_ap_closure.expect("same installed AP closure after original shutdown");
        assert_eq!(receipt.activation(), actual_ready_cut.unwrap().activation());
        match receipt.closure().as_ref() {
            novarocks_coordination_contract::AttemptConvergenceClosure::Evidence(_) => {
                assert!(
                    released,
                    "positive shutdown closure requires the actual peer release"
                );
            }
            novarocks_coordination_contract::AttemptConvergenceClosure::RemoteTrackingEnded {
                exact_participations,
            } => {
                assert_eq!(
                    exact_participations.as_ref(),
                    original_participations.as_slice()
                );
                assert!(
                    !original_remote_closure,
                    "tracking end is not positive Ready convergence"
                );
            }
        }
        assert!(history.is_complete());
    } else if !failure_during_read_close {
        assert!(
            normal_within_watchdog
                && convergence_within_watchdog
                && ready_ok
                && original_remote_closure
        );
        assert!(subscribed && released && read_closed && end_offered && actual_acked == 1);
    }
    if controlled_reopen {
        assert!(history.is_complete());
        assert!(setup.is_some() && body_exit);
        assert!(
            second_subscribe,
            "next original Open was stranded: {blocked:?}"
        );
    }
}

fn original_pending_protocol(
    process: &FrontendAttemptRuntime,
    actor: novarocks_control_runtime_core::identity::ActorId,
) -> Vec<String> {
    let mut result = Vec::new();
    for input in process
        .runtime
        .pending_batches(actor, None, 64)
        .unwrap_or_default()
    {
        for (descriptor, state) in process
            .runtime
            .effect_inventory(actor, input, None, 64)
            .unwrap()
        {
            // Attempt effects contain only the pure protocol vocabulary;
            // opaque material remains represented by its PayloadRef.
            let metadata = process
                .runtime
                .pending_effect_metadata(actor, input, descriptor.id)
                .unwrap()
                .map(|effect| match effect {
                    FrontendEffect::Attempt(original) => format!("{:?}", original.protocol.body),
                    FrontendEffect::Logical(_) => "original logical effect".to_owned(),
                    other => format!("{:?}", std::mem::discriminant(&other)),
                });
            result.push(format!(
                "input={input:?}, descriptor={descriptor:?}, state={state:?}, metadata={metadata:?}"
            ));
        }
    }
    result
}

fn fixture_pending_open(
    process: &FrontendAttemptRuntime,
    actor: novarocks_control_runtime_core::identity::ActorId,
    after: Option<std::num::NonZeroU64>,
) -> Option<(
    novarocks_control_runtime_core::identity::InputId,
    novarocks_control_runtime_core::identity::ActionId,
    novarocks_execution_contract::ParticipationRef,
    std::num::NonZeroU64,
)> {
    use crate::control_runtime::actor::FrontendEffect;
    use novarocks_attempt_protocol::effect::EffectBody;
    use novarocks_control_runtime_core::effect::EffectState;
    for (_, input) in process.runtime.committed_batches(actor, None, 256).unwrap() {
        for (descriptor, state) in process
            .runtime
            .effect_inventory(actor, input, None, 32)
            .unwrap()
        {
            if state != EffectState::Pending {
                continue;
            }
            let Some(FrontendEffect::Attempt(effect)) = process
                .runtime
                .pending_effect_metadata(actor, input, descriptor.id)
                .unwrap()
            else {
                continue;
            };
            if let EffectBody::OpenSubscription(request) = &effect.protocol.body
                && after.is_none_or(|old| request.generation > old)
            {
                return Some((
                    input,
                    descriptor.id,
                    request.participation,
                    request.generation,
                ));
            }
        }
    }
    None
}

// These two carriers intentionally retain an original raw destruction failure.
// Their only final release boundary is the actual child process exit, not a
// fabricated WC/Scope completion or an ordinary serving-process abandon.
#[test]
fn installed_activation_future_drop_keeps_native_unknown_after_actual_ap_exit() {
    run_activation_drop_subprocess(
        crate::fixtures::native_recipe::ActivationDropFault::Future,
        "installed_activation_future_drop_keeps_native_unknown_after_actual_ap_exit",
    );
}

#[test]
fn installed_dormant_drop_keeps_native_unknown_after_actual_ap_exit() {
    run_activation_drop_subprocess(
        crate::fixtures::native_recipe::ActivationDropFault::Dormant,
        "installed_dormant_drop_keeps_native_unknown_after_actual_ap_exit",
    );
}

fn run_activation_drop_subprocess(
    fault: crate::fixtures::native_recipe::ActivationDropFault,
    leaf: &str,
) {
    let output = run_installed_unknown_subprocess(
        leaf,
        "TST_G9B3_NATIVE_ACTIVATION_DROP_CHILD",
        |runtime| runtime.block_on(run_activation_drop_child(fault)),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(
            novarocks_failpoint::observation_marker::TST_G9B3_NATIVE_UNKNOWN_AFTER_AP_EXIT
        )
    );
}

/// Test-only process disposition for an original opaque UNKNOWN. All positive
/// Source/Join assertions belong in the child before its actual final exit.
fn run_installed_unknown_subprocess(
    leaf: &str,
    selector: &str,
    child: impl FnOnce(&tokio::runtime::Runtime),
) -> std::process::Output {
    if let Some(selected) = std::env::var_os(selector) {
        assert_eq!(selected, std::ffi::OsStr::new(leaf));
        let runtime = std::mem::ManuallyDrop::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(3)
                .enable_all()
                .build()
                .unwrap(),
        );
        let result: Result<(), Box<dyn std::any::Any + Send>> =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                child(&runtime);
                panic!("the original UNKNOWN child returned without its process disposition");
            }));
        if let Err(original) = result {
            // Keep the first actual panic hook output and every unresolved
            // original alive; never unwind through their checked destructors.
            drop(original);
            std::process::exit(101);
        }
        unreachable!();
    }
    let module = module_path!().split_once("::").unwrap().1;
    let exact = format!("{module}::{leaf}");
    // Validate the SAME test binary's exact registered selection. This is
    // libtest metadata, not a new application log marker or an exit receipt.
    let listed = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(&exact)
        .arg("--list")
        .output()
        .unwrap();
    assert!(listed.status.success());
    let expected = format!("{exact}: test");
    assert!(
        String::from_utf8_lossy(&listed.stdout)
            .lines()
            .any(|line| line == expected),
        "the exact original UNKNOWN child must be registered: {exact}"
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(exact)
        .arg("--nocapture")
        .env(selector, leaf)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "original installed child failed: status={:?}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

async fn run_activation_drop_child(fault: crate::fixtures::native_recipe::ActivationDropFault) {
    let wait = Duration::from_secs(5);
    let data =
        std::mem::ManuallyDrop::new(FrontendDataRuntime::new(tokio::runtime::Handle::current()));
    let peer = std::mem::ManuallyDrop::new(Loopback::start().await);
    let descriptor = peer.bounded_root_descriptor(BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    // Keep this WHOLE same original fixture, including the original Work and
    // joins. Neither a test assertion nor Source exit retires its Native UNKNOWN.
    let mut storage =
        std::mem::ManuallyDrop::new(install_original_fixture(&data, descriptor, history.clone()));
    let original: &mut OriginalFixture = &mut storage;
    original.logical.probe.arm_activation_drop(fault);
    let gate = Arc::new(ReadCloseGate::default());
    let _release_on_unwind = gate.release_guard();
    *original.logical.probe.read_close_gate.lock().unwrap() = Some(gate.clone());
    let installed = original
        .fixture
        .activate_attempt(&original.logical, &mut original.joint, false)
        .await;
    let root = original.logical.probe.root.lock().unwrap().unwrap();
    let activation = original.logical.probe.activation.lock().unwrap().unwrap();
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    original.logical.probe.release_activation();
    let reached = drive_until_with_original_clock(
        &original.process,
        &mut original.joint,
        async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + wait,
        Some(original.clock.as_ref()),
    )
    .await;
    assert!(
        reached.is_some(),
        "original failed-owner read close was not reached"
    );
    assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
    let causes = original.logical.probe.activation_drop_observation();
    assert_eq!(causes.activation_calls, 1);
    assert_eq!(causes.returned, 1);
    assert_eq!(causes.future_drops, 1);
    assert_eq!(causes.original_panic_drops, 0);
    assert!(causes.original_panic_alive);
    assert_eq!(causes.original_panic_fault, Some(fault));
    {
        let view = original.logical.exit.as_ref().unwrap().view().unwrap();
        assert_eq!(view.pending_native_returns(), 1);
        assert!(!view.native_preparation_exited());
    }
    // This releases the SAME real Root read call. It does not supply a Native
    // exit receipt, change Core phase or terminate a Work future.
    gate.release();
    let logical_exit = original.logical.exit.as_ref().unwrap();
    let probe = &original.logical.probe;
    let exited = drive_until_with_original_clock(
        &original.process,
        &mut original.joint,
        async {
            loop {
                let native_returned = logical_exit.view().unwrap().pending_native_returns() == 0;
                // The active return and AP exit can be visible before the
                // failure supervisor calls the separate dormant destructor.
                if native_returned
                    && *installed.fixture_exit.observe_actual_exit().borrow()
                    && probe.activation_drop_observation().dormant_drops != 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + wait,
        Some(original.clock.as_ref()),
    )
    .await;
    assert!(
        exited.is_some(),
        "actual AP exit, Native return, or dormant destructor remains unobserved"
    );
    let port_exit = novarocks_query_application::api::NativeInstalledAttemptPort::resources_exited(
        original.logical.binding.as_ref(),
        activation,
    )
    .await;
    assert!(
        port_exit.is_ok(),
        "the actual AP Source is independently healthy"
    );
    {
        let view = original.logical.exit.as_ref().unwrap().view().unwrap();
        assert!(!view.native_preparation_exited());
        assert_eq!(view.pending_native_returns(), 0);
        assert_eq!(view.native_return_disposition(), Some(
            novarocks_query_application::coordination::LocalExecutionDisposition::OriginalPaidOwnerReturn
        ));
    }
    let causes = probe.activation_drop_observation();
    assert_eq!(causes.activation_calls, 1, "no ordinary activation retry");
    assert_eq!(causes.returned, 1);
    assert_eq!(causes.future_drops, 1);
    assert_eq!(
        causes.dormant_drops, 1,
        "the original dormant destructor runs once"
    );
    assert!(causes.original_panic_alive);
    assert_eq!(causes.original_panic_fault, Some(fault));
    assert_eq!(causes.original_panic_drops, 0);
    // No assertion of body disposal, Gov complete, WC/ledger emptiness, panic
    // payload normal Drop, healthy QA shutdown or AP remote-positive evidence.
    println!(
        "{} activation={activation:?} fault={fault:?}",
        novarocks_failpoint::observation_marker::TST_G9B3_NATIVE_UNKNOWN_AFTER_AP_EXIT,
    );
    std::process::exit(0);
}

// The actual statistics case shares this process/control bootstrap, not the
// retired TaskRound component driver. Existing component module cfgs stay put.
#[cfg(all(test, novarocks_dev_controls))]
#[path = "installed_subscription_tests/statistics_recipe.rs"]
pub(crate) mod statistics_recipe;

// The same owned computation cell across real consumer/pump exit and P=1 reuse.
#[path = "installed_subscription_tests/p1.rs"]
mod p1;

#[path = "installed_subscription_tests/reply_wake.rs"]
mod reply_wake;
