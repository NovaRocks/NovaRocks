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

//! The original three M/R properties share the fresh production Node fixture.
//! Only foreign materialization is held; protocol, rows and cleanup stay real.
use super::*;
use std::sync::atomic::AtomicBool;

type BoundaryScene = Scenario<ColdNotificationPool>;
#[derive(Default)]
struct BoundaryState {
    released: bool,
    requested_samples: usize,
    samples: Vec<bool>,
}
#[derive(Default)]
struct Boundary {
    entered: AtomicBool,
    state: Mutex<BoundaryState>,
    changed: Condvar,
    forbidden: Mutex<Option<ParticipationRef>>,
    factory_thread: Mutex<Option<std::thread::ThreadId>>,
    materialize_thread: Mutex<Option<std::thread::ThreadId>>,
    release_thread: Mutex<Option<std::thread::ThreadId>>,
    disposal_thread: Mutex<Option<std::thread::ThreadId>>,
}
impl Boundary {
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
    fn materialize(&self, control: &dyn crate::ParticipationMaterializationControl) {
        self.entered.store(true, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        while !state.released {
            while state.samples.len() < state.requested_samples {
                // Only the original M thread reads the borrowed Scope control.
                // The test thread receives observations, never the capability.
                state.samples.push(control.is_stopped());
                self.changed.notify_all();
            }
            state = self.changed.wait(state).unwrap();
        }
    }
    fn sample_control(&self) -> Option<bool> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().unwrap();
        let index = state.requested_samples;
        state.requested_samples += 1;
        self.changed.notify_all();
        loop {
            if let Some(sample) = state.samples.get(index) {
                return Some(*sample);
            }
            if state.released {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
}
struct ReleaseOnExit(Arc<Boundary>);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct BoundaryFactory {
    calls: Arc<Calls>,
    boundary: Arc<Boundary>,
}
struct BoundaryInstance {
    participation: ParticipationRef,
    calls: Arc<Calls>,
    boundary: Arc<Boundary>,
}
impl ParticipationHostFactory for BoundaryFactory {
    fn create_instance(
        self: Arc<Self>,
        participation: ParticipationRef,
        grant: crate::ParticipationInstanceGrant,
    ) -> Result<Arc<dyn ParticipationHostInstance>, HostRejection> {
        assert_ne!(
            *self.boundary.forbidden.lock().unwrap(),
            Some(participation),
            "a withdrawn queued M cannot call its factory"
        );
        self.calls.observe_host();
        self.calls.factories.fetch_add(1, Ordering::SeqCst);
        *self.boundary.factory_thread.lock().unwrap() = Some(std::thread::current().id());
        Ok(grant
            .bind_test_instance(Arc::new(BoundaryInstance {
                participation,
                calls: self.calls.clone(),
                boundary: self.boundary.clone(),
            }))
            .expect("fixture binds its exact original instance"))
    }
}
impl ParticipationHostInstance for BoundaryInstance {
    fn participation(&self) -> ParticipationRef {
        self.participation
    }
    fn materialize(
        &self,
        request: SharedFactsRequest<'_>,
        control: &dyn crate::ParticipationMaterializationControl,
    ) -> Result<(), HostRejection> {
        assert_eq!(request.participation(), self.participation);
        self.calls.observe_host();
        self.calls.materializations.fetch_add(1, Ordering::SeqCst);
        *self.boundary.materialize_thread.lock().unwrap() = Some(std::thread::current().id());
        self.boundary.materialize(control);
        Ok(())
    }
    fn release(&self) -> ReleasedParticipationEvidence {
        self.calls.observe_host();
        self.calls.releases.fetch_add(1, Ordering::SeqCst);
        *self.boundary.release_thread.lock().unwrap() = Some(std::thread::current().id());
        ReleasedParticipationEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        panic!("the original M/R boundary has no domain update");
    }
}
impl Drop for BoundaryInstance {
    fn drop(&mut self) {
        self.calls.observe_host();
        self.calls.instance_drops.fetch_add(1, Ordering::SeqCst);
        *self.boundary.disposal_thread.lock().unwrap() = Some(std::thread::current().id());
    }
}
fn start(positions: usize, boundary: &Arc<Boundary>) -> StartedScenario<ColdNotificationPool> {
    start_scenario(
        1,
        positions,
        ReservationResources::default(),
        start_cold_notifications,
        |calls| {
            Arc::new(BoundaryFactory {
                calls,
                boundary: boundary.clone(),
            })
        },
    )
}
fn deadline() -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(100))
}
fn acquire(
    scene: &BoundaryScene,
    participation: ParticipationRef,
    capability: AdmissionEpochCapability,
    number: u8,
    ticket: AdmissionTicketId,
) {
    let operation = operation_number(number);
    let mut handle = scene
        .node
        .submit_control(
            scene.node.next_source(),
            operation,
            deadline(),
            ControlInput::Acquire {
                request: AcquireParticipationAdmissionTicket::new(
                    operation,
                    participation,
                    LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    capability,
                ),
                ticket,
            },
        )
        .unwrap();
    let delivery = wait_delivery(&mut handle).expect("the original ticket RPC completes");
    assert_eq!(delivery.request(), handle.request());
    assert!(
        matches!(delivery.body(), ControlReplyBody::Admission(Ok(receipt)) if receipt.participation() == participation && receipt.ticket_id() == ticket)
    );
    drop(delivery);
    drop(handle);
}
fn establish(
    scene: &BoundaryScene,
    operation: TaskOperationId,
    raw: EstablishParticipation,
) -> ControlRequestHandle {
    scene
        .node
        .submit_establish(
            scene.node.next_source(),
            operation,
            deadline(),
            raw,
            scene.node.next_payload(),
        )
        .unwrap()
}
fn settle_establish(handle: &mut ControlRequestHandle, expected: OperationOutcome) {
    let delivery = wait_delivery(handle).expect("the original Establish reply completes");
    assert_eq!(delivery.request(), handle.request());
    assert!(
        matches!(delivery.body(), ControlReplyBody::Participation(ack) if ack.outcome == expected)
    );
    drop(delivery);
}
fn quiesce(
    scene: &BoundaryScene,
    participation: ParticipationRef,
    number: u8,
    expired: bool,
    expected_requests: u64,
) {
    let mut handle = scene
        .node
        .submit_participation(
            participation,
            scene.node.next_source(),
            operation_number(number),
            deadline(),
            ParticipationCommand::Quiesce,
        )
        .unwrap();
    let delivery = wait_delivery(&mut handle).expect("control progresses while actual M is held");
    assert_eq!(delivery.request(), handle.request());
    if expired {
        assert!(matches!(
            delivery.body(),
            ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::LeaseExpired))
        ));
    } else {
        assert!(matches!(
            delivery.body(),
            ParticipationReplyBody::Quiesce {
                outcome: OperationOutcome::Accepted,
                ..
            }
        ));
    }
    // External delivery still owns this exact R. In the queued case this
    // is the derived peak A Establish + B Establish + B control = three.
    assert_eq!(
        scene.ledger.used(Dimension::Requests.id()),
        expected_requests
    );
    drop(delivery);
    drop(handle);
}
fn expire(scene: &BoundaryScene) {
    scene.clock.0.store(30_000_000_002, Ordering::SeqCst);
    scene.runtime.notify_pollers();
}
fn finish(scene: BoundaryScene, boundary: &Boundary) {
    let calls = scene.calls.clone();
    scene.node.begin_drain().unwrap();
    scene.node.confirm_ingress_closed().unwrap();
    if !wait_until(|| scene.runtime.is_empty() && scene.ledger.is_empty()) {
        scene.fail("same fresh Node original M/R/H drain", ());
    }
    finish_scene(
        scene,
        1,
        "OriginalMaterialization",
        shutdown_cold_notifications,
    );
    assert_eq!(calls.factories.load(Ordering::SeqCst), 1);
    assert_eq!(calls.materializations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.releases.load(Ordering::SeqCst), 1);
    assert_eq!(calls.instance_drops.load(Ordering::SeqCst), 1);
    assert_eq!(calls.on_host.load(Ordering::SeqCst), 4);
    let factory = *boundary.factory_thread.lock().unwrap();
    let materialize = *boundary.materialize_thread.lock().unwrap();
    let release = *boundary.release_thread.lock().unwrap();
    let disposal = *boundary.disposal_thread.lock().unwrap();
    assert!(factory.is_some() && release.is_some());
    assert_eq!(factory, materialize);
    assert_eq!(release, disposal);
    assert_ne!(materialize, release);
    assert_ne!(release, Some(std::thread::current().id()));
}
pub(super) fn accepted_creator_retains_only_participation_custody() {
    let boundary = Arc::new(Boundary::default());
    boundary.release();
    let _release = ReleaseOnExit(boundary.clone());
    let StartedScenario {
        scene,
        raw,
        getters,
        source_drops,
        participation,
        capability,
    } = start(2, &boundary);
    acquire(
        &scene,
        participation,
        capability,
        12,
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
    );
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 0
    ));
    let mut original = establish(&scene, operation(), raw);
    settle_establish(&mut original, OperationOutcome::Accepted);
    drop(original);
    let original_rpc_exited = wait_until(|| {
        scene.ledger.used(Dimension::Requests.id()) == 0 && source_drops.load(Ordering::SeqCst) == 3
    });
    let active_route = scene
        .node
        .route(participation)
        .is_some_and(|route| route.read().state == ParticipationState::Active);
    let releases_while_active = scene.calls.releases.load(Ordering::SeqCst);
    let mut observed = None;
    let active_custody_observed = wait_until(|| {
        if let Some(snapshot) = scene.node.try_execution_quiescence_snapshot() {
            observed = Some(snapshot);
            snapshot.participations_with_execution_custody == 1
        } else {
            false
        }
    });
    let getter_calls = getters.load(Ordering::SeqCst);
    // Preserve the original observations, then close and join the actual Node
    // before any failing assertion about a missing/busy observation.
    finish(scene, &boundary);
    assert!(original_rpc_exited);
    assert!(active_route);
    assert_eq!(releases_while_active, 0);
    assert!(
        active_custody_observed,
        "original active custody observation: {observed:?}"
    );
    assert_eq!(getter_calls, 3);
    assert_eq!(source_drops.load(Ordering::SeqCst), 4);
}
pub(super) fn blocked_materialization_keeps_control_live() {
    for lease_expired in [true, false] {
        blocked_materialization_observes_original_stop(lease_expired);
    }
}
fn blocked_materialization_observes_original_stop(lease_expired: bool) {
    let boundary = Arc::new(Boundary::default());
    let _release = ReleaseOnExit(boundary.clone());
    let StartedScenario {
        scene,
        raw,
        getters,
        source_drops,
        participation,
        capability,
    } = start(2, &boundary);
    acquire(
        &scene,
        participation,
        capability,
        12,
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
    );
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 0
    ));
    let mut original = establish(&scene, operation(), raw);
    assert!(wait_until(|| boundary.entered.load(Ordering::Acquire)));
    let before_quiesce = boundary.sample_control();
    quiesce(&scene, participation, 13, false, 2);
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 1
    ));
    let after_quiesce = boundary.sample_control();
    let expected = if lease_expired {
        expire(&scene);
        quiesce(&scene, participation, 14, true, 2);
        OperationOutcome::LeaseExpired
    } else {
        let mut abort = scene
            .node
            .submit_participation(
                participation,
                scene.node.next_source(),
                operation_number(14),
                deadline(),
                ParticipationCommand::Abort {
                    cause: novarocks_execution_contract::AbortCause::QueryFailed,
                },
            )
            .unwrap();
        let delivery = wait_delivery(&mut abort)
            .expect("the real Abort completes while original materialization stays blocked");
        assert_eq!(delivery.request(), abort.request());
        assert!(
            matches!(delivery.body(), ParticipationReplyBody::Participation(ack)
            if ack.outcome == OperationOutcome::Accepted)
        );
        assert_eq!(scene.ledger.used(Dimension::Requests.id()), 2);
        drop(delivery);
        drop(abort);
        OperationOutcome::ParticipationTerminalReceipt
    };
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 1
    ));
    let after_stop = boundary.sample_control();
    let repeated_stop = boundary.sample_control();
    let original_still_pending = original.try_take().is_none();
    let releases_before_materialization_exit = scene.calls.releases.load(Ordering::SeqCst);
    let source_drops_before_materialization_exit = source_drops.load(Ordering::SeqCst);
    // Stop observation must not substitute for the foreign call's real exit.
    // Preserve the held observations, then release/join before asserting them.
    boundary.release();
    settle_establish(&mut original, expected);
    drop(original);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    finish(scene, &boundary);
    assert_eq!(before_quiesce, Some(false));
    assert_eq!(after_quiesce, Some(false));
    assert_eq!(after_stop, Some(true));
    assert_eq!(repeated_stop, Some(true));
    assert!(original_still_pending);
    assert_eq!(releases_before_materialization_exit, 0);
    assert_eq!(source_drops_before_materialization_exit, 0);
    assert_eq!(source_drops.load(Ordering::SeqCst), 4);
}

fn second_raw(
    participation: ParticipationRef,
    operation: TaskOperationId,
    ticket: AdmissionTicketId,
) -> (EstablishParticipation, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let getters = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let content = || {
        Arc::new(Content {
            getters: getters.clone(),
            drops: drops.clone(),
            getter_gate: None,
            drop_gate: None,
            panic_drop: false,
            panic_getter: false,
        }) as Arc<dyn CodecOwnedContent>
    };
    let raw = EstablishParticipation::new(
        operation,
        participation,
        ticket,
        content(),
        content(),
        content(),
        CredentialUpdate::new(
            CredentialDomainId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(Credential {
                drops: drops.clone(),
                panic_drop: false,
            }),
        ),
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    );
    (raw, getters, drops)
}
pub(super) fn queued_materialization_withdraws_without_factory() {
    let boundary = Arc::new(Boundary::default());
    let _release = ReleaseOnExit(boundary.clone());
    let StartedScenario {
        scene,
        raw,
        getters,
        source_drops,
        participation,
        capability,
    } = start(3, &boundary);
    acquire(
        &scene,
        participation,
        capability,
        12,
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
    );
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 0
    ));
    let mut blocking = establish(&scene, operation(), raw);
    assert!(wait_until(|| boundary.entered.load(Ordering::Acquire)));
    let target = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(7001, 7003), AttemptId::new(1).unwrap()).unwrap(),
        participation.frontend_process_id(),
        participation.backend_process_id(),
    );
    *boundary.forbidden.lock().unwrap() = Some(target);
    let ticket = AdmissionTicketId::try_from_bytes([10; 16]).unwrap();
    acquire(&scene, target, capability, 15, ticket);
    assert!(wait_until(
        || scene.ledger.used(Dimension::Requests.id()) == 1
    ));
    let operation = operation_number(16);
    let (raw, target_getters, target_drops) = second_raw(target, operation, ticket);
    let mut queued = establish(&scene, operation, raw);
    let handles = scene.participations.handles().unwrap();
    assert!(wait_until(|| handles
        .original_materialization_queue_snapshot()
        .unwrap()
        .is_some_and(|s| s.queued == 1
            && s.running == 1
            && s.worker_limit == 1)));
    expire(&scene);
    quiesce(&scene, target, 17, true, 3);
    settle_establish(&mut queued, OperationOutcome::LeaseExpired);
    drop(queued);
    assert!(wait_until(|| scene.ledger.used(Dimension::Requests.id())
        == 1
        && target_drops.load(Ordering::SeqCst) == 4));
    assert_eq!(scene.calls.factories.load(Ordering::SeqCst), 1);
    assert_eq!(scene.calls.releases.load(Ordering::SeqCst), 0);
    assert!(blocking.try_take().is_none());
    assert_eq!(target_getters.load(Ordering::SeqCst), 3);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    boundary.release();
    settle_establish(&mut blocking, OperationOutcome::LeaseExpired);
    drop(blocking);
    drop(handles);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    finish(scene, &boundary);
    assert_eq!(target_drops.load(Ordering::SeqCst), 4);
    assert_eq!(source_drops.load(Ordering::SeqCst), 4);
}

struct SenderExitBoundary {
    runtime: tokio::runtime::Handle,
    polled: AtomicBool,
    drop_entered: AtomicBool,
    dropped: AtomicBool,
    allow_drop: Mutex<bool>,
    changed: Condvar,
}
impl SenderExitBoundary {
    fn open_drop(&self) {
        *self.allow_drop.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct OpenSenderDropOnExit(Arc<SenderExitBoundary>);
impl Drop for OpenSenderDropOnExit {
    fn drop(&mut self) {
        self.0.open_drop();
    }
}
struct SenderExitCapture(Arc<SenderExitBoundary>);
impl Drop for SenderExitCapture {
    fn drop(&mut self) {
        self.0.drop_entered.store(true, Ordering::Release);
        let mut allowed = self.0.allow_drop.lock().unwrap();
        while !*allowed {
            allowed = self.0.changed.wait(allowed).unwrap();
        }
        self.0.dropped.store(true, Ordering::Release);
    }
}
struct PendingOriginalSender {
    boundary: Arc<SenderExitBoundary>,
    _capture: SenderExitCapture,
}
impl std::future::Future for PendingOriginalSender {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.boundary.polled.store(true, Ordering::Release);
        std::task::Poll::Pending
    }
}
struct OriginalSenderWake(crate::ParticipationWorkWake);
impl Drop for OriginalSenderWake {
    fn drop(&mut self) {
        self.0.notify();
    }
}
struct OriginalSenderFuture<F> {
    // Match the Native carrier's physical order: inner future/captures first,
    // the original WorkWake second. The wake contains metadata only.
    future: std::pin::Pin<Box<F>>,
    _wake: OriginalSenderWake,
}
impl<F: std::future::Future> std::future::Future for OriginalSenderFuture<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
type SenderJoin =
    tokio::task::JoinHandle<Result<(), novarocks_control_runtime::exit_work::ExitWorkError>>;
struct SenderInstanceCaptures {
    participation: ParticipationRef,
    calls: Arc<Calls>,
    boundary: Arc<SenderExitBoundary>,
    issuer: crate::InstanceWorkIssuer,
    worker: Mutex<Option<SenderJoin>>,
    released: AtomicBool,
}
struct SenderInstance {
    captures: Option<SenderInstanceCaptures>,
    work: Option<novarocks_control_runtime::exit_custody::ExitObligation>,
}
struct SenderFactory {
    calls: Arc<Calls>,
    boundary: Arc<SenderExitBoundary>,
}
impl ParticipationHostFactory for SenderFactory {
    fn create_instance(
        self: Arc<Self>,
        participation: ParticipationRef,
        grant: crate::ParticipationInstanceGrant,
    ) -> Result<Arc<dyn ParticipationHostInstance>, HostRejection> {
        assert_eq!(grant.participation(), participation);
        self.calls.observe_host();
        self.calls.factories.fetch_add(1, Ordering::SeqCst);
        let (mut work, issuer) = grant.into_parts();
        work.start()
            .expect("the original granted instance starts once");
        Ok(Arc::new(SenderInstance {
            captures: Some(SenderInstanceCaptures {
                participation,
                calls: self.calls.clone(),
                boundary: self.boundary.clone(),
                issuer,
                worker: Mutex::new(None),
                released: AtomicBool::new(false),
            }),
            work: Some(work),
        }))
    }
}
impl ParticipationHostInstance for SenderInstance {
    fn participation(&self) -> ParticipationRef {
        self.captures.as_ref().unwrap().participation
    }
    fn materialize(
        &self,
        request: SharedFactsRequest<'_>,
        control: &dyn crate::ParticipationMaterializationControl,
    ) -> Result<(), HostRejection> {
        let captures = self.captures.as_ref().unwrap();
        assert_eq!(request.participation(), captures.participation);
        assert!(!control.is_stopped());
        captures.calls.observe_host();
        captures
            .calls
            .materializations
            .fetch_add(1, Ordering::SeqCst);
        let (work, panic, wake) = captures
            .issuer
            .runtime_filter_sender()
            .unwrap()
            .into_parts();
        let sender = PendingOriginalSender {
            boundary: captures.boundary.clone(),
            _capture: SenderExitCapture(captures.boundary.clone()),
        };
        let future = novarocks_control_runtime::exit_work::run_future(work, sender, panic);
        let worker = captures.boundary.runtime.spawn(OriginalSenderFuture {
            future: Box::pin(future),
            _wake: OriginalSenderWake(wake),
        });
        assert!(captures.worker.lock().unwrap().replace(worker).is_none());
        Ok(())
    }
    fn release(&self) -> ReleasedParticipationEvidence {
        let captures = self.captures.as_ref().unwrap();
        captures.calls.observe_host();
        captures.calls.releases.fetch_add(1, Ordering::SeqCst);
        assert!(!captures.released.swap(true, Ordering::AcqRel));
        captures.worker.lock().unwrap().take().unwrap().abort();
        ReleasedParticipationEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        unreachable!("the original sender wake scenario has no domain update")
    }
}
impl Drop for SenderInstance {
    fn drop(&mut self) {
        let captures = self.captures.take().unwrap();
        assert!(captures.released.load(Ordering::Acquire));
        assert!(captures.worker.lock().unwrap().is_none());
        let calls = captures.calls.clone();
        calls.observe_host();
        // Actual instance captures, including its original issuer, leave H
        // before the original work records exit. RF is an independent child.
        drop(captures);
        self.work.take().unwrap().exited().unwrap();
        calls.instance_drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn sender_exit_wakes_original_node_after_holder_disposal() {
    for workers in [1, 2] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let boundary = Arc::new(SenderExitBoundary {
            runtime: runtime.handle().clone(),
            polled: AtomicBool::new(false),
            drop_entered: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            allow_drop: Mutex::new(false),
            changed: Condvar::new(),
        });
        let _open_on_failure = OpenSenderDropOnExit(boundary.clone());
        let StartedScenario {
            scene,
            raw,
            getters,
            source_drops,
            participation,
            capability,
        } = start_scenario(
            workers,
            2,
            ReservationResources::default(),
            start_cold_notifications,
            |calls| {
                Arc::new(SenderFactory {
                    calls,
                    boundary: boundary.clone(),
                })
            },
        );
        let clock_origin = scene.clock.now();
        acquire(
            &scene,
            participation,
            capability,
            12,
            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        );
        assert!(wait_until(
            || scene.ledger.used(Dimension::Requests.id()) == 0
        ));
        let mut original = establish(&scene, operation(), raw);
        settle_establish(&mut original, OperationOutcome::Accepted);
        drop(original);
        if !wait_until(|| {
            boundary.polled.load(Ordering::Acquire)
                && scene.ledger.used(Dimension::Requests.id()) == 0
        }) {
            scene.fail("the original RF sender did not actually start", ());
        }
        let scope = scene
            .node
            .original_participation_scope_for_test(participation)
            .expect("original Participation scope is still attached");
        quiesce(&scene, participation, 13, false, 1);
        assert!(wait_until(
            || scene.ledger.used(Dimension::Requests.id()) == 0
        ));
        let expires = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() >= expires {
                scene.fail("RF scenario actual Release timeout", ());
            }
            let mut release = scene
                .node
                .submit_participation(
                    participation,
                    scene.node.next_source(),
                    operation_number(14),
                    deadline(),
                    ParticipationCommand::Release,
                )
                .unwrap();
            let delivery = wait_delivery(&mut release).expect("the original Release RPC completes");
            assert_eq!(delivery.request(), release.request());
            let released = match delivery.body() {
                ParticipationReplyBody::Release {
                    release: novarocks_execution_contract::ReleaseOutcome::Released,
                    ..
                } => true,
                ParticipationReplyBody::Release {
                    release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                    ..
                } => false,
                other => panic!("unexpected original RF Release result: {other:?}"),
            };
            drop(delivery);
            drop(release);
            assert!(wait_until_deadline(expires, || scene
                .ledger
                .used(Dimension::Requests.id())
                == 0));
            if released {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut blocked = None;
        if !wait_until(|| {
            if scene.calls.instance_drops.load(Ordering::SeqCst) != 1
                || !boundary.drop_entered.load(Ordering::Acquire)
            {
                return false;
            }
            let Ok(Some(inventory)) = scene.node.participation_detachment_blockers(participation)
            else {
                return false;
            };
            if inventory.entries.len() != 2 {
                return false;
            }
            let instance = inventory.entries.iter().find(|entry| {
                entry.label == Some(crate::control_runtime::work_labels::PARTICIPATION_INSTANCE)
            });
            let sender = inventory.entries.iter().find(|entry| {
                entry.label == Some(crate::control_runtime::work_labels::RUNTIME_FILTER_SENDER)
            });
            if let (Some(instance), Some(sender)) = (instance, sender)
                && instance.body_exited
                && instance.captures_exited
                && !sender.body_exited
                && !sender.captures_exited
                && sender.parent == Some(instance.work)
                && !instance.unresolved
                && !sender.unresolved
            {
                blocked = Some(inventory);
                true
            } else {
                false
            }
        }) {
            scene.fail(
                "H retired but original instance/RF custody was not preserved",
                (scope, blocked),
            );
        }
        let blocked = blocked.unwrap();
        assert_eq!(
            blocked.live_by_label[crate::control_runtime::work_labels::PARTICIPATION_INSTANCE],
            1
        );
        assert_eq!(
            blocked.live_by_label[crate::control_runtime::work_labels::RUNTIME_FILTER_SENDER],
            1
        );
        assert!(!boundary.dropped.load(Ordering::Acquire));
        assert!(scope.gates().is_ok());
        assert_eq!(scene.clock.now(), clock_origin);

        // Critical boundary: no more RPC, manual host/ledger progress, clock
        // advance or notify_pollers. Only the actual sender Drop can emit its
        // original header readiness and WorkWake to the original Node Actor.
        boundary.open_drop();
        if !wait_until(|| {
            matches!(
                scope.gates(),
                Err(novarocks_control_runtime::transaction::RuntimeError::UnknownScope)
            )
        }) {
            scene.fail(
                "actual RF exit did not wake original two-level retirement and Scope removal",
                blocked,
            );
        }
        assert!(boundary.dropped.load(Ordering::Acquire));
        assert_eq!(scene.clock.now(), clock_origin);
        if !wait_until(|| {
            scene
                .node
                .route(participation)
                .is_some_and(|route| route.read().state == ParticipationState::TerminalRetained)
        }) {
            scene.fail(
                "the original terminal route did not retain after execution Scope removal",
                (),
            );
        }
        assert_eq!(scene.calls.factories.load(Ordering::SeqCst), 1);
        assert_eq!(scene.calls.materializations.load(Ordering::SeqCst), 1);
        assert_eq!(scene.calls.releases.load(Ordering::SeqCst), 1);
        assert_eq!(scene.calls.instance_drops.load(Ordering::SeqCst), 1);
        assert_eq!(scene.calls.on_host.load(Ordering::SeqCst), 4);
        assert_eq!(getters.load(Ordering::SeqCst), 3);
        assert_eq!(source_drops.load(Ordering::SeqCst), 4);

        // Process closure follows the already-proven physical detachment and
        // joins the same original pools; it cannot make that proof pass.
        scene.node.begin_drain().unwrap();
        scene.node.confirm_ingress_closed().unwrap();
        if !wait_until(|| scene.runtime.is_empty() && scene.ledger.is_empty()) {
            scene.fail(
                "original RF Node process drain did not empty its runtime/ledger",
                (),
            );
        }
        finish_scene(
            scene,
            workers,
            "OriginalRfSenderWake",
            shutdown_cold_notifications,
        );
        drop(runtime);
    }
}
