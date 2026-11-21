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

//! Real LP/AP Core and Source cancellation while the original Provider call
//! owns its paid CPU work. The empty Provider does not model a BE split ACK.
use crate::control_runtime::{FrontendAttemptRuntime, actor::FrontendEffect};
use crate::fixtures::{
    history::{CapturedHistory, InputDisposition, WorkExpectation},
    native_recipe::{OriginalFixture, install_original_fixture, split::OriginalSplitObservation},
};
use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
use crate::task_execution::clock::ManualClock;
use crate::task_execution::host::transport::OriginalSendRelease;
use novarocks_attempt_protocol::{
    effect::EffectBody,
    port_output::{AttemptTerminal, AttemptToLogicalBody},
};
use novarocks_control_runtime::exit_work::HandoffState;
use novarocks_control_runtime_core::{
    history::HistoryFact,
    identity::{ActorId, InputId},
};
use novarocks_coordination_contract::QueryExecutionErrorKind;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::joint_admission::JointProgressDriver;
use std::{
    future::Future,
    sync::{Arc, Mutex, OnceLock, atomic::Ordering},
    task::Poll,
    time::{Duration, Instant},
};

type Terminal = AttemptTerminal<novarocks_attempt_protocol::failure::AttemptFailure>;
struct OriginalObservations {
    values: Vec<Terminal>,
    workloads: Vec<(
        novarocks_attempt_protocol::failure::HostFailureRef,
        novarocks_workload_control::WorkError,
    )>,
    source: std::sync::Weak<
        Mutex<
            std::collections::BTreeMap<
                novarocks_attempt_protocol::failure::HostFailureRef,
                crate::task_execution::error::TaskExecutionError,
            >,
        >,
    >,
    withdrawal: Option<(InputId, Arc<OnceLock<InputId>>)>,
}
impl OriginalObservations {
    fn observe_withdrawal(&self, process: &FrontendAttemptRuntime, actor: ActorId) {
        let Some((position, occurrence)) = &self.withdrawal else {
            return;
        };
        if occurrence.get().is_some() {
            return;
        }
        // The admitted reservation becomes this standing position. Each fill
        // gets a fresh occurrence, which the next original AP poll may consume.
        if let Some(input) = process
            .actor_runtime()
            .standing_input_inventory(actor, None, usize::MAX)
            .unwrap()
            .into_iter()
            .find(|row| row.standing_input_id.get() == position.get())
            .and_then(|row| row.in_flight)
        {
            occurrence
                .set(input)
                .expect("one original withdrawal occurrence");
        }
    }

    fn observe(&mut self, terminal: &Terminal) {
        if self.values.contains(terminal) {
            return;
        }
        if let AttemptTerminal::Failed(novarocks_attempt_protocol::failure::AttemptFailure::Host(
            failure,
        )) = terminal
        {
            assert_eq!(
                failure.kind,
                novarocks_attempt_protocol::failure::HostFailureKind::Workload,
                "an unknown Host failure cannot stand in for original cancellation",
            );
            let original = crate::task_execution::host::transport::AdmissionSource::workload_failure_for_fixture(
                &self.source,
                failure.original,
            )
            .expect("SAME exact admission Host failure retains its original WorkError");
            self.workloads.push((failure.original, original));
        }
        self.values.push(terminal.clone());
    }
}
struct OriginalRun {
    original: Option<OriginalFixture>,
    cleanup: Option<CleanupOriginals>,
    data: FrontendDataRuntime,
    executors: Box<dyn OriginalConnectorExecutorOwners>,
    peer: Option<Loopback>,
}
// A closed local capability keeps the SAME concrete CPU/blocking owners in
// one Box. It splits the whole-run Send proof without erasing resources or
// requiring the enclosing fixture's complete structural Send recursion here.
trait OriginalConnectorExecutorOwners: Send {
    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + '_>>;
}
impl OriginalConnectorExecutorOwners
    for crate::task_execution::blocking_io::tests::OriginalExecutorsFixture
{
    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(
            crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::shutdown(self),
        )
    }
}
type CleanupAliases = (
    crate::fixtures::native_recipe::ActualLogicalCase,
    crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    novarocks_workload_control::ResultWindowGrant,
    novarocks_workload_control::WorkCancellationRequester,
    novarocks_workload_control::ResultCapacityHandle,
);
// Every unproven teardown owner stays in this SAME whole-run slot across
// awaits. Consumed success removes only that owner's actual settled duty;
// consuming shutdown failure returns its original owner to this slot.
struct CleanupOriginals {
    fixture: crate::fixtures::FrontendFixture,
    process: Arc<FrontendAttemptRuntime>,
    clock: Arc<ManualClock>,
    joint: JointProgressDriver,
    workload: Option<novarocks_workload_control::WorkloadControl>,
    supervisor: Option<novarocks_query_application::coordination::LogicalExecutionSupervisor>,
    scope: Option<novarocks_workload_control::WorkScope>,
    business: Option<novarocks_workload_control::BusinessPermit>,
    aliases: Option<CleanupAliases>,
}
impl CleanupOriginals {
    fn from_original(original: OriginalFixture) -> Self {
        let OriginalFixture {
            fixture,
            logical,
            process,
            clock,
            joint,
            workload,
            supervisor,
            scope,
            business,
            capacity,
            window,
            cohort,
            requester,
            output,
        } = original;
        Self {
            fixture,
            process,
            clock,
            joint,
            workload: Some(workload),
            supervisor: Some(supervisor),
            scope: Some(scope),
            business: Some(business),
            aliases: Some((logical, output, cohort, window, requester, capacity)),
        }
    }
}
struct RetainedRun(Option<Box<OriginalRun>>);
impl Drop for RetainedRun {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            #[expect(
                clippy::vec_box,
                reason = "Retain the same allocated whole-run owner after unproven teardown"
            )]
            static UNKNOWN: OnceLock<Mutex<Vec<Box<OriginalRun>>>> = OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
async fn drive<F: Future>(
    process: &FrontendAttemptRuntime,
    clock: &ManualClock,
    joint: &mut JointProgressDriver,
    actor: ActorId,
    terminals: &mut OriginalObservations,
    advance: bool,
    future: F,
) -> F::Output {
    let mut future = Box::pin(future);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
            process
                .actor_runtime()
                .advance_time(process.clock.now(), 64)
                .unwrap();
            let mut runnable = false;
            for _ in 0..64 {
                terminals.observe_withdrawal(process, actor);
                let polled = process
                    .actor_runtime()
                    .poll_one_with_clock(process.clock.as_ref())
                    .unwrap();
                if let Ok(batches) = process.actor_runtime().committed_batches(actor, None, 64) {
                    for (_, input) in batches {
                        for (descriptor, _) in process
                            .actor_runtime()
                            .effect_inventory(actor, input, None, 64)
                            .unwrap()
                        {
                            if let Some(FrontendEffect::Attempt(effect)) = process
                                .actor_runtime()
                                .pending_effect_metadata(actor, input, descriptor.id)
                                .unwrap()
                                && let EffectBody::Port(message) = &effect.protocol.body
                                && let AttemptToLogicalBody::AttemptTerminal(terminal) =
                                    &message.body
                            {
                                // The real pending effect still borrows its
                                // SAME Source-held rich failure record here.
                                terminals.observe(terminal);
                            }
                        }
                    }
                }
                let claimed = if let Some(turn) = process.actor_runtime().claim_host_actor() {
                    process.host.drive(process.clock.now(), turn);
                    true
                } else {
                    false
                };
                runnable |= polled || claimed;
                if !polled && !claimed {
                    break;
                }
            }
            if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
                return value;
            }
            if !runnable
                && advance
                && let Some(next) = process.actor_runtime().next_deadline()
                && next > process.clock.now()
            {
                clock.advance(Duration::from_nanos(next.0 - process.clock.now().0));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same original LP/AP cancellation and physical exit")
}

#[tokio::test]
async fn actual_split_call_cancellation_stops_its_own_ap_and_waits_for_the_original_child_exit() {
    let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let executors = crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(
        data.connector_blocking_io(),
    );
    let mut retained = RetainedRun(Some(Box::new(OriginalRun {
        original: None,
        cleanup: None,
        data,
        executors: Box::new(executors),
        peer: None,
    })));
    let run = retained.0.as_mut().unwrap();
    run.peer = Some(Loopback::start().await);
    let descriptor = run
        .peer
        .as_ref()
        .unwrap()
        .bounded_root_descriptor(BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(65_536);
    run.original = Some(install_original_fixture(
        &run.data,
        descriptor,
        history.clone(),
    ));
    let original = run.original.as_mut().unwrap();
    let observed = Arc::new(OriginalSplitObservation::default());
    let _release_on_unwind = observed.call.release_guard();
    *original.logical.probe.split.lock().unwrap() = Some(observed.clone());
    super::actual_logical_metrics::assert_before_attempt(original, &history).await;
    let installed = original
        .fixture
        .activate_attempt(&original.logical, &mut original.joint, false)
        .await;
    let actor = installed.fixture_sources.0;
    let root = original.logical.probe.root.lock().unwrap().unwrap();
    let activation = original.logical.probe.activation.lock().unwrap().unwrap();
    let bound = original.logical.binding.bound().unwrap();
    let endpoint = bound.links.endpoint(activation).unwrap();
    let parent = bound.registration.scope_id();
    let child = endpoint.attempt().scope_id();
    run.peer
        .as_ref()
        .unwrap()
        .enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    run.peer
        .as_ref()
        .unwrap()
        .defer_single_root_terminal()
        .unwrap();
    run.peer
        .as_ref()
        .unwrap()
        .close_original_stopped_roster_on_abort()
        .unwrap();
    original.logical.probe.release_activation();
    let mut terminals = OriginalObservations {
        values: Vec::new(),
        workloads: Vec::new(),
        source: installed.fixture_admission_failures.clone(),
        withdrawal: None,
    };
    let (admission, held) = drive(
        &original.process,
        &original.clock,
        &mut original.joint,
        installed.fixture_sources.0,
        &mut terminals,
        false,
        async {
            loop {
                let gate = observed.admission_handoff.lock().unwrap().upgrade();
                if observed.call.entered()
                    && let Some(gate) = gate
                    && let Some(held) = gate.held()
                {
                    break (gate, held);
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(
        held.participation,
        original.logical.probe.participations.lock().unwrap()[0]
    );
    assert_eq!(held.result_actor, actor);
    assert_eq!(held.result_work.scope, child);
    assert_eq!(admission.held_handoff_state(), Some(HandoffState::Pending));
    assert_eq!(observed.call.calls(), 1);
    assert_eq!(observed.closes.load(Ordering::Acquire), 0);
    assert_eq!(observed.drops.load(Ordering::Acquire), 0);
    let metrics_registry = prometheus::Registry::new();
    let metrics = original.process.metrics(&metrics_registry).unwrap();
    metrics.sample();
    let input_metrics = metrics_registry
        .gather()
        .into_iter()
        .find(|family| family.get_name() == "novarocks_frontend_control_runtime_inputs")
        .unwrap();
    assert!(input_metrics.get_metric().iter().any(|metric| {
        metric
            .get_label()
            .iter()
            .any(|label| label.get_name() == "resource" && label.get_value() == "steps")
            && metric.get_gauge().get_value() > 0.0
    }));
    assert!(input_metrics.get_metric().iter().all(|metric| {
        metric
            .get_label()
            .iter()
            .all(|label| matches!(label.get_name(), "class" | "resource"))
    }));
    let mut captured = CapturedHistory::drain(&history, usize::MAX);
    captured
        .check_work(held.result_work, WorkExpectation::Unknown)
        .unwrap();
    original
        .requester
        .request(novarocks_workload_control::CancellationReason::Requested)
        .unwrap();
    drive(&original.process, &original.clock, &mut original.joint, installed.fixture_sources.0, &mut terminals, false, async {
        loop {
            captured.append_from(&history, usize::MAX);
            if admission.held_handoff_state() == Some(HandoffState::Withdrawn)
                && captured.entries().iter().any(|entry| matches!(entry.fact, HistoryFact::ScopeStopped { scope } if scope == child))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await;
    assert!(
        !original
            .process
            .actor_runtime()
            .snapshot(installed.fixture_sources.0)
            .unwrap()
            .failed
    );
    assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
    assert!(
        futures::poll!(std::future::poll_fn(|cx| original
            .logical
            .exit
            .as_mut()
            .unwrap()
            .poll_exit(cx)))
        .is_pending()
    );
    assert_eq!(observed.closes.load(Ordering::Acquire), 0);
    assert_eq!(observed.drops.load(Ordering::Acquire), 0);
    // Stop has reached the actual AP, but this SAME Provider F and its
    // responsibility remain live. Releasing a caller borrow proves no exit.
    assert_eq!(admission.held(), Some(held));
    assert_eq!(admission.additional_matches(), 0);
    assert_eq!(admission.accepted(), None);
    let withdrawal = Arc::new(OnceLock::new());
    terminals.withdrawal = Some((held.result_position, withdrawal.clone()));
    assert_eq!(admission.release(), OriginalSendRelease::Accepted(held));
    assert_eq!(admission.accepted(), Some(held));
    observed.call.release();
    let ready = original.logical.ready.take().unwrap();
    let (ready, (withdrawal_target, withdrawal_input)) = drive(
        &original.process,
        &original.clock,
        &mut original.joint,
        installed.fixture_sources.0,
        &mut terminals,
        false,
        async {
            let original_withdrawal = async {
                // The original Join publishes DefinitelyUnsent on this input
                // after actual transport exit. Keep the fixture clock fixed
                // until AP consumes it, before evaluating a remote deadline.
                loop {
                    captured.append_from(&history, usize::MAX);
                    if let Some(occurrence) = withdrawal.get().copied()
                        && let Some(target) =
                            captured
                                .entries()
                                .iter()
                                .find_map(|entry| match entry.fact {
                                    HistoryFact::InputConsumed { target, input }
                                        if target.actor == held.result_actor
                                            && input == occurrence =>
                                    {
                                        Some(target)
                                    }
                                    _ => None,
                                })
                    {
                        break (target, occurrence);
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::join!(ready, original_withdrawal)
        },
    )
    .await;
    let error = ready.err().expect("cancelled computation has no Ready");
    assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
    let mut exit = original.logical.exit.take().unwrap();
    let receipt = drive(
        &original.process,
        &original.clock,
        &mut original.joint,
        installed.fixture_sources.0,
        &mut terminals,
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(matches!(
        receipt.view().runtime_actor_result(),
        Some(novarocks_query_application::api::logical_runtime::LogicalExit::Completed)
    ));
    assert!(*installed.fixture_exit.observe_actual_exit().borrow());
    // One purposeful frontier observation. These are the SAME accepted
    // current-ticket completion and actual peer requests, never inferred from
    // an absent Abort or reconstructed from the final logical conclusion.
    let original_completion = endpoint.facts.completion_receipt().map(|original| {
        (
            original.activation(),
            original.action(),
            original.closure().as_ref().clone(),
        )
    });
    let original_rpc_kinds = run
        .peer
        .as_ref()
        .unwrap()
        .applied_requests()
        .iter()
        .flat_map(|request| {
            request
                .operations
                .iter()
                .map(|wire| {
                    use novarocks_task_codec::operation::{
                        DecodedOperation as D, DecodedUpdateParticipation as U,
                    };
                    match novarocks_task_codec::operation::decode_operation(
                        wire,
                        novarocks_proto_codec::FieldPath::root("original_split_cancel_frontier"),
                    ) {
                        Ok(D::AcquireParticipationAdmissionTicket(original)) => {
                            ("Acquire", Some(original.participation()), None)
                        }
                        Ok(D::UpdateParticipation(U::Establish(original))) => {
                            ("Establish", Some(original.participation()), None)
                        }
                        Ok(D::UpdateParticipation(U::RenewLease { participation, .. })) => {
                            ("Renew", Some(participation), None)
                        }
                        Ok(D::CreateTask(original)) => (
                            "Create",
                            Some(original.request().participation()),
                            Some(original.request().identity()),
                        ),
                        Ok(D::UpdateTask(original)) => {
                            ("UpdateTask", None, Some(original.request().identity()))
                        }
                        Ok(D::QuiesceParticipation(original)) => {
                            ("Quiesce", Some(original.participation()), None)
                        }
                        Ok(D::ReleaseParticipation(original)) => {
                            ("Release", Some(original.participation()), None)
                        }
                        Ok(D::AbortParticipation(original)) => {
                            ("Abort", Some(original.participation()), None)
                        }
                        Ok(D::CancelTask(original)) => {
                            ("CancelTask", None, Some(original.identity()))
                        }
                        Ok(_) => ("OtherDecoded", None, None),
                        Err(_) => ("DecodeRefused", None, None),
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    eprintln!(
        "original split cancellation frontier: activation={activation:?}, same_completion={original_completion:?}, actual_rpc_kinds={original_rpc_kinds:?}, original_abort_observed={}",
        run.peer.as_ref().unwrap().single_root_abort_observed()
    );
    let same_participation = {
        let participations = original.logical.probe.participations.lock().unwrap();
        assert_eq!(participations.len(), 1);
        participations[0]
    };
    let (completion_activation, _, closure) = original_completion.as_ref().unwrap();
    assert_eq!(*completion_activation, activation);
    assert_eq!(
        closure,
        &novarocks_coordination_contract::AttemptConvergenceClosure::Evidence(vec![(
            same_participation,
            novarocks_coordination_contract::AttemptParticipationConvergence::NeverEstablished,
        )]),
        "the original current-ticket completion supplies positive closure",
    );
    assert_eq!(
        original_rpc_kinds.as_slice(),
        &[("Abort", Some(same_participation), None)],
    );
    let applied = run.peer.as_ref().unwrap().applied_requests();
    let aborts = applied
        .iter()
        .flat_map(|request| request.operations.iter())
        .map(|wire| {
            novarocks_task_codec::operation::decode_operation(
                wire,
                novarocks_proto_codec::FieldPath::root("original_split_abort"),
            )
            .unwrap()
        })
        .filter_map(|operation| match operation {
            novarocks_task_codec::operation::DecodedOperation::AbortParticipation(abort) => {
                Some(abort)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(aborts.len(), 1);
    assert_eq!(aborts[0].participation(), same_participation);
    assert_eq!(
        aborts[0].cause(),
        novarocks_execution_contract::AbortCause::QueryFailed
    );
    assert_eq!(
        run.peer.as_ref().unwrap().single_root_abort_refusals(),
        vec![(
            aborts[0].envelope().operation_id(),
            tonic::Code::FailedPrecondition
        )],
        "the same early Abort RPC was refused, never a Worker close ACK",
    );
    assert!(!terminals.values.is_empty());
    assert!(terminals.values.iter().all(|terminal| {
        match terminal {
            AttemptTerminal::Failed(
                novarocks_attempt_protocol::failure::AttemptFailure::Logical(failure),
            ) => failure.error() == &error,
            AttemptTerminal::Failed(novarocks_attempt_protocol::failure::AttemptFailure::Host(
                failure,
            )) => {
                failure.kind == novarocks_attempt_protocol::failure::HostFailureKind::Workload
                    && failure.site
                        == novarocks_attempt_protocol::failure::AttemptFailureSite::Attempt
                    && failure.class
                        == novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure
                    && terminals.workloads.iter().any(|(reference, original)| {
                        *reference == failure.original
                            && *original
                                == novarocks_workload_control::WorkError::Cancelled(
                                    novarocks_workload_control::CancellationReason::Requested,
                                )
                    })
            }
            _ => false,
        }
    }));
    {
        let view = receipt.view();
        assert!(!view.role_failed());
        assert!(!view.internal_consumer_failed());
        assert!(view.native_preparation_failure().is_none());
        assert!(view.native_run_panic().is_none());
        assert!(view.native_run_cleanup_panic().is_none());
        assert!(view.native_body_cleanup_panic().is_none());
        assert!(!view.native_dormant_failed());
    }
    assert_eq!(observed.closes.load(Ordering::Acquire), 1);
    assert_eq!(observed.drops.load(Ordering::Acquire), 1);
    captured.append_from(&history, usize::MAX);
    captured.check_capture().unwrap();
    captured
        .check_input(
            withdrawal_target,
            withdrawal_input,
            InputDisposition::Consumed,
        )
        .unwrap();
    assert_eq!(admission.additional_matches(), 0);
    assert!(!captured.entries().iter().any(|entry| matches!(entry.fact,
        HistoryFact::FailurePublication { target, .. }
            if target.actor == actor || target.actor == bound.registration.actor()
    )));
    assert!(!captured.entries().iter().any(|entry| matches!(entry.fact,
        HistoryFact::OwnerFailed { scope, .. } if scope == child || scope == parent
    )));
    let stop = captured
        .entries()
        .iter()
        .find(|entry| matches!(entry.fact, HistoryFact::ScopeStopped { scope } if scope == child))
        .expect("AP stopped its own Scope");
    assert!(captured.entries().iter().any(|entry| matches!(entry.fact, HistoryFact::InputConsumed { target, .. } if target.actor == bound.registration.actor())));
    assert!(
        !captured
            .entries()
            .iter()
            .any(|entry| entry.ordinal < stop.ordinal
                && matches!(entry.fact, HistoryFact::ScopeRemoved { scope } if scope == parent))
    );
    drop((
        receipt,
        exit,
        endpoint,
        bound,
        installed,
        metrics,
        metrics_registry,
    ));
    run.cleanup = Some(CleanupOriginals::from_original(
        run.original.take().unwrap(),
    ));
    let cleanup = run.cleanup.as_mut().unwrap();
    drop(cleanup.aliases.take());
    drive(
        &cleanup.process,
        &cleanup.clock,
        &mut cleanup.joint,
        actor,
        &mut terminals,
        true,
        cleanup
            .supervisor
            .as_mut()
            .unwrap()
            .shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    // shutdown_until returned actual service/registry exit, not merely intent.
    drop(cleanup.supervisor.take());
    cleanup
        .process
        .close_original_host_registration_for_fixture();
    cleanup.fixture.finish().await;
    captured.append_from(&history, usize::MAX);
    captured
        .check_completed_runtime(cleanup.fixture.runtime(), cleanup.fixture.ledger())
        .unwrap();
    captured.check_child_scope_removed(child, parent).unwrap();
    cleanup.business.take().unwrap().release();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
        &mut cleanup.joint,
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        cleanup.scope.as_ref().unwrap().wait_released(),
    )
    .await
    .unwrap();
    drop(cleanup.scope.take());
    cleanup.workload.as_ref().unwrap().close_admission();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
        &mut cleanup.joint,
    );
    let shutdown_error = match cleanup.workload.take().unwrap().shutdown() {
        Ok(_) => None,
        Err(failure) => {
            let (error, owner) = failure.into_parts();
            cleanup.workload = Some(owner);
            Some(error)
        }
    };
    assert!(
        shutdown_error.is_none(),
        "same original WC shutdown failed: {shutdown_error:?}"
    );
    run.data
        .drain_original_apply_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    run.data
        .drain_native_outgoing_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(run.peer.as_mut().unwrap().settle().await);
    run.executors.shutdown().await;
    retained.0.take();
}
