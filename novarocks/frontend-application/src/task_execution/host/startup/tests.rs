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

//! Startup observations use the original paid position and actual committed
//! runtime inputs. This component does not simulate AP terminal or HostExit.
use super::*;
use crate::control_runtime::actor::{FrontendEffect, TestActor};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, Generation, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    sync::{Mutex, OnceLock},
    task::{Context, Poll, Waker},
};

struct Recorder {
    seen: Arc<Mutex<Vec<StartupInput>>>,
    stop_on_start: bool,
}
impl TestActor for Recorder {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        match value {
            FrontendInput::AttemptStartup(input) => {
                self.seen.lock().unwrap().push(input);
                if input == StartupInput::StartProtocol && self.stop_on_start {
                    step.stop_producing();
                }
            }
            FrontendInput::Test(1) => step.stop_producing(),
            _ => panic!("only original startup and explicit owner stop are admitted"),
        }
    }
}

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkObligation,
    payer: ReservationObligation,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    source: Option<AttemptStartupSource>,
    startup: AttemptStartup,
    control: StandingInputObligation<FrontendActor>,
    seen: Arc<Mutex<Vec<StartupInput>>>,
}

/// An assertion unwind retains the entire unresolved original fixture. It
/// never runs a second obligation destructor or claims asynchronous exit.
struct OriginalRun(Option<Box<Fixture>>);
impl Deref for OriginalRun {
    type Target = Fixture;
    fn deref(&self) -> &Fixture {
        self.0.as_deref().unwrap()
    }
}
impl DerefMut for OriginalRun {
    fn deref_mut(&mut self) -> &mut Fixture {
        self.0.as_deref_mut().unwrap()
    }
}
impl Drop for OriginalRun {
    fn drop(&mut self) {
        static RETAINED: OnceLock<Mutex<Vec<Fixture>>> = OnceLock::new();
        if let Some(original) = self.0.take() {
            RETAINED
                .get_or_init(Mutex::default)
                .lock()
                .unwrap()
                .push(*original);
        }
    }
}
impl OriginalRun {
    fn new(stop_on_start: bool) -> Self {
        let epoch = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, epoch).unwrap();
        let (mut work, payer) = scope
            .reserve_next(
                epoch,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: 2,
                        batches: 2,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(Recorder {
                    seen: seen.clone(),
                    stop_on_start,
                })),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    max_step_work: 2,
                    turn_budget: 16,
                },
            )
            .unwrap_or_else(|_| panic!("original startup actor registration"));
        let reserve = |n| {
            runtime
                .reserve_admitted_input_for(
                    &registration,
                    ActorId::new(n).unwrap(),
                    InputClass::Lifecycle,
                    &work,
                )
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let (source, startup) = AttemptStartupSource::new(registration.clone(), reserve(2));
        let control = reserve(3);
        Self(Some(Box::new(Fixture {
            ledger,
            scope,
            work,
            payer,
            runtime,
            registration,
            source: Some(source),
            startup,
            control,
            seen,
        })))
    }

    fn finish(mut self) {
        let f = self.0.as_mut().unwrap();
        assert!(
            f.source
                .as_ref()
                .is_none_or(AttemptStartupSource::is_closed)
        );
        assert!(f.control.retire_ready_after_exit().unwrap());
        f.control.close_after_exit().unwrap();
        f.work.body_exited().unwrap();
        f.work.captures_exited().unwrap();
        f.payer.finish().unwrap();
        assert!(
            f.scope
                .reap_observed_header(&f.work.retirement_observer())
                .unwrap()
        );
        f.scope.close_external().unwrap();
        while let Some(turn) = f.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        f.runtime.remove_actor(f.registration.actor()).unwrap();
        f.ledger.remove_scope(f.scope.id()).unwrap();
        assert!(f.runtime.is_empty() && f.ledger.is_empty());
        drop(self.0.take());
    }
}
impl Fixture {
    fn initialize(&mut self) {
        self.startup.release_initialization(&self.runtime).unwrap();
        assert!(
            !self
                .source
                .as_mut()
                .unwrap()
                .progress(false, false)
                .unwrap()
        );
        self.consume_startup();
        assert_eq!(*self.seen.lock().unwrap(), vec![StartupInput::Bootstrap]);
        assert!(self.startup.initialized_for(&self.registration));
    }
    fn consume_startup(&mut self) {
        let original = self.source.as_ref().unwrap().in_flight.unwrap();
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        assert!(self.source.as_mut().unwrap().indexed(&[original]));
        let actual = self.scope.snapshot().unwrap();
        assert!(
            self.source
                .as_mut()
                .unwrap()
                .progress(actual.failed, actual.stopped)
                .unwrap()
        );
    }
    fn stop(&mut self) {
        self.control.fill(FrontendInput::Test(1)).unwrap();
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        assert!(self.scope.snapshot().unwrap().stopped);
        assert!(self.control.retire_ready_after_exit().unwrap());
        let actual = self.scope.snapshot().unwrap();
        assert!(
            !self
                .source
                .as_mut()
                .unwrap()
                .progress(actual.failed, actual.stopped)
                .unwrap()
        );
        assert!(self.source.as_ref().unwrap().is_closed());
    }
    fn assert_no_exit(&self) {
        let actual = self.scope.snapshot().unwrap();
        assert!(!actual.join_ready && actual.work != 0);
        assert!(self.runtime.snapshot(self.registration.actor()).is_ok());
    }
}

#[tokio::test]
async fn original_protocol_close_before_run_is_not_ready_and_survives_source_drop() {
    let mut f = OriginalRun::new(false);
    f.initialize();
    f.stop();
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Ok(NativeAttemptStartupOutcome::ClosedByProtocol)
    );
    assert_eq!(
        f.startup.clone().wait_ready().await,
        Err(ActorTurnError::Closed)
    );
    drop(f.source.take());
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Ok(NativeAttemptStartupOutcome::ClosedByProtocol)
    );
    f.assert_no_exit();
    f.finish();
}

#[test]
fn original_protocol_close_between_watch_preflight_and_registration_survives_actual_actor_retirement()
 {
    let mut f = OriginalRun::new(false);
    f.initialize();
    let mut startup = f.startup.clone();
    let runtime = f.runtime.clone();
    let registration = f.registration.clone();
    let seen = f.seen.clone();
    let worker_runtime = runtime.clone();
    let (entered, wait_entered) = std::sync::mpsc::sync_channel(0);
    let (resume, wait_resume) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        let future = startup.start_protocol_inner(&worker_runtime, || {
            entered.send(()).unwrap();
            // Dropping the fixture-side sender on assertion unwind also
            // releases this passive observer; it owns no Source obligation.
            wait_resume.recv().unwrap();
        });
        let mut future = std::pin::pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        future.as_mut().poll(&mut cx)
    });
    wait_entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the original watch preflight reaches registration validation");
    f.stop();
    assert_eq!(*f.startup.status.borrow(), StartupStatus::Closed);
    assert!(f.source.as_ref().unwrap().is_closed());
    f.assert_no_exit();
    f.finish();
    assert_eq!(
        runtime.validate_registration(&registration),
        Err(ActorTurnError::UnknownActor),
        "only actual retirement removes the original registration"
    );
    resume.send(()).unwrap();
    assert_eq!(
        worker.join().unwrap(),
        Poll::Ready(Ok(NativeAttemptStartupOutcome::ClosedByProtocol))
    );
    assert_eq!(*seen.lock().unwrap(), vec![StartupInput::Bootstrap]);
    assert!(runtime.is_empty());
}

#[tokio::test]
async fn actual_protocol_step_close_during_run_settles_before_original_input_retirement() {
    let mut f = OriginalRun::new(true);
    f.initialize();
    let mut observer = f.startup.clone();
    let runtime = f.runtime.clone();
    let future = observer.start_protocol(&runtime);
    tokio::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert!(!f.source.as_mut().unwrap().progress(false, false).unwrap());
    let original = f.source.as_ref().unwrap().in_flight.unwrap();
    assert!(f.runtime.poll_one(Tick(1)).unwrap());
    let actual = f.scope.snapshot().unwrap();
    assert!(actual.stopped && !actual.failed);
    assert!(
        !f.source
            .as_mut()
            .unwrap()
            .progress(actual.failed, actual.stopped)
            .unwrap()
    );
    assert!(!f.source.as_ref().unwrap().is_closed());
    assert_eq!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Ok(NativeAttemptStartupOutcome::ClosedByProtocol))
    );
    f.assert_no_exit();
    assert!(
        f.runtime
            .input_custody(f.registration.actor(), original)
            .unwrap()
            .is_some()
    );
    assert!(f.source.as_mut().unwrap().indexed(&[original]));
    assert!(f.source.as_mut().unwrap().progress(false, true).unwrap());
    assert!(!f.source.as_mut().unwrap().progress(false, true).unwrap());
    assert!(f.source.as_ref().unwrap().is_closed());
    f.finish();
}

#[tokio::test]
async fn original_ready_is_retained_after_source_drop_without_claiming_scope_exit() {
    let mut f = OriginalRun::new(false);
    f.initialize();
    let mut observer = f.startup.clone();
    let runtime = f.runtime.clone();
    let future = observer.start_protocol(&runtime);
    tokio::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert!(!f.source.as_mut().unwrap().progress(false, false).unwrap());
    f.consume_startup();
    assert!(!f.source.as_mut().unwrap().progress(false, false).unwrap());
    assert_eq!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Ok(NativeAttemptStartupOutcome::Ready))
    );
    assert_eq!(
        *f.seen.lock().unwrap(),
        vec![StartupInput::Bootstrap, StartupInput::StartProtocol]
    );
    drop(f.source.take());
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Ok(NativeAttemptStartupOutcome::Ready)
    );
    f.startup.clone().wait_ready().await.unwrap();
    f.assert_no_exit();
    f.finish();
}

#[tokio::test]
async fn initialized_watch_loss_and_uninitialized_start_are_refused_without_protocol_input() {
    let mut f = OriginalRun::new(false);
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Err(ActorTurnError::Closed)
    );
    assert!(f.seen.lock().unwrap().is_empty());
    f.initialize();
    f.source.as_mut().unwrap().input.close_after_exit().unwrap();
    drop(f.source.take());
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Err(ActorTurnError::Closed)
    );
    assert_eq!(*f.seen.lock().unwrap(), vec![StartupInput::Bootstrap]);
    f.assert_no_exit();
    f.finish();
}

#[tokio::test]
async fn actual_scope_failure_is_failed_startup_even_after_source_drop() {
    let mut f = OriginalRun::new(false);
    f.initialize();
    f.scope.fail().unwrap();
    let actual = f.scope.snapshot().unwrap();
    assert!(actual.failed);
    assert!(
        !f.source
            .as_mut()
            .unwrap()
            .progress(actual.failed, actual.stopped)
            .unwrap()
    );
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Err(ActorTurnError::ActorFailed)
    );
    drop(f.source.take());
    assert_eq!(
        f.startup.clone().start_protocol(&f.runtime).await,
        Err(ActorTurnError::ActorFailed)
    );
    f.assert_no_exit();
    f.finish();
}

#[tokio::test]
async fn foreign_runtime_with_equal_public_actor_id_cannot_release_original_protocol() {
    let mut f = OriginalRun::new(false);
    let mut foreign = OriginalRun::new(false);
    f.initialize();
    foreign.initialize();
    assert_eq!(f.registration.actor(), foreign.registration.actor());
    assert_eq!(
        f.startup.clone().start_protocol(&foreign.runtime).await,
        Err(ActorTurnError::EffectTicketOriginMismatch)
    );
    assert_eq!(*f.seen.lock().unwrap(), vec![StartupInput::Bootstrap]);
    f.stop();
    foreign.stop();
    f.finish();
    foreign.finish();
}
