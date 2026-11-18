// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original F panic custody, independently of an ordered opening's poison.
use super::*;
use novarocks_workload_control::{WorkError, WorkloadConfig, WorkloadControl};
use std::mem::ManuallyDrop;

struct DirectFuture {
    body: Option<RawCause>,
    cleanup: Option<RawCause>,
    entered: Arc<AtomicBool>,
    drop_entered: Arc<AtomicBool>,
    drop_returned: Arc<AtomicBool>,
}
impl Future for DirectFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.entered.store(true, Ordering::Release);
        if let Some(original) = self.body.take() {
            std::panic::panic_any(original);
        }
        // A cleanup-only failure is the actual explicit destruction of this
        // SAME Ready F, not a local async value unwinding inside another F.
        Poll::Ready(())
    }
}
impl Drop for DirectFuture {
    fn drop(&mut self) {
        self.drop_entered.store(true, Ordering::Release);
        if let Some(original) = self.cleanup.take() {
            std::panic::panic_any(original);
        }
        self.drop_returned.store(true, Ordering::Release);
    }
}

/// The actual child process owns every unresolved original until process exit.
/// A successful single-panic case really closes its owners before that exit.
pub(super) fn run_process_case(
    module: &str,
    leaf: &str,
    child: impl FnOnce(&tokio::runtime::Runtime),
) {
    const SELECTOR: &str = "NOVAROCKS_TST_G9B3_SUBSCRIPTION_PANIC_CASE";
    if let Some(selected) = std::env::var_os(SELECTOR) {
        assert_eq!(selected, std::ffi::OsStr::new(leaf));
        let runtime = ManuallyDrop::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(3)
                .enable_all()
                .build()
                .unwrap(),
        );
        let result: Result<(), Box<dyn std::any::Any + Send>> =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                child(&runtime);
                panic!("original panic child returned without its explicit disposition");
            }));
        if let Err(original) = result {
            // Preserve the first hook output and opaque first failure. Do not
            // unwind through checked UNKNOWN owners or drop a foreign cause.
            let _original = ManuallyDrop::new(original);
            std::process::exit(101);
        }
        unreachable!();
    }
    let module = module.split_once("::").unwrap().1;
    let exact = format!("{module}::{leaf}");
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
        "the exact original panic child must be registered: {exact}"
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(exact)
        .arg("--nocapture")
        .env(SELECTOR, leaf)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "original panic child failed: {:?}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) async fn direct_causes(body_panic: bool, destructor_panic: bool) {
    // Stable original custody precedes every added assertion. The raw-panic
    // child never unwinds through these owners; positive cleanup takes them
    // back out only after their actual original obligations have retired.
    let f = ManuallyDrop::new(Fixture::new(8, 4));
    let control = ManuallyDrop::new(
        WorkloadControl::try_new_counted(WorkloadConfig {
            scope_records_limit: 2,
            root_limit: 1,
            business_limit: 1,
            ..Default::default()
        })
        .unwrap()
        .owner,
    );
    control.mark_ready().unwrap();
    let root = ManuallyDrop::new(
        control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let scope = root.owner.scope();
    let owner = ManuallyDrop::new(scope.child(WorkRequest::new(WorkClass::Query)).unwrap());
    let child = owner.scope();
    let body_hold = Hold::new();
    let cleanup_hold = Hold::new();
    let release = ReleaseOnExit([body_hold.clone(), cleanup_hold.clone()]);
    let watchdogs = [body_hold.watchdog(), cleanup_hold.watchdog()];
    let entered = Arc::new(AtomicBool::new(false));
    let drop_entered = Arc::new(AtomicBool::new(false));
    let drop_returned = Arc::new(AtomicBool::new(false));
    let (work, header) = original_work(&f);
    let panic = PanicSink::new();
    let (role, exit, handoff) =
        OriginalTransportJoinObligation::prepare(work, ConsumerId::new(1).unwrap(), panic.clone())
            .unwrap_or_else(|e| panic!("original direct-F role: {}", e.error));
    let mut role = ManuallyDrop::new(role);
    let original_custody = role.exit_custody();
    let panic_in = panic.clone();
    let entered_in = entered.clone();
    let drop_entered_in = drop_entered.clone();
    let drop_returned_in = drop_returned.clone();
    let body_hold_in = body_hold.clone();
    let cleanup_hold_in = cleanup_hold.clone();
    handoff
        .spawn(&Handle::current(), exit, move |exit, _| {
            let original = run_future(
                exit,
                DirectFuture {
                    body: body_panic.then(|| RawCause {
                        stage: "body",
                        hold: body_hold_in,
                    }),
                    cleanup: destructor_panic.then(|| RawCause {
                        stage: "cleanup",
                        hold: cleanup_hold_in,
                    }),
                    entered: entered_in,
                    drop_entered: drop_entered_in,
                    drop_returned: drop_returned_in,
                },
                panic_in,
            );
            async move {
                let _ = original.await;
            }
        })
        .unwrap();
    until(|| entered.load(Ordering::Acquire) && panic.has_panic()).await;
    let joined = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| role.poll_shared_completion(cx)),
    )
    .await
    .unwrap();
    assert!(drop_entered.load(Ordering::Acquire));
    assert_eq!(drop_returned.load(Ordering::Acquire), !destructor_panic);
    if destructor_panic {
        assert!(matches!(
            joined,
            Err(CustodyError::OriginalCapturesUnresolved)
        ));
        assert_eq!(original_custody.phase(), ExitCustodyPhase::Unresolved);
        assert!(!original_custody.body_exited() && !original_custody.captures_exited());
        assert!(!role.try_retire().unwrap());
        assert!(!f.scope.reap_observed_header(&header).unwrap());
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
    } else {
        assert!(joined.unwrap().is_none());
        assert_eq!(original_custody.phase(), ExitCustodyPhase::Exited);
        assert!(original_custody.body_exited() && original_custody.captures_exited());
    }
    let failed_join = role
        .take_join_failure()
        .expect("same original failed carrier");
    assert!(failed_join.is_panic());
    drop(failed_join);
    assert!(!body_hold.entered.load(Ordering::Acquire));
    assert!(!cleanup_hold.entered.load(Ordering::Acquire));
    let held_check = || {
        assert!(child.check().is_ok());
        assert!(
            control
                .snapshot()
                .scopes
                .iter()
                .any(|row| { row.id == child.id() && row.parent == Some(scope.id()) })
        );
        assert!(matches!(
            scope.child(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::Capacity("scope records"))
        ));
    };
    if body_panic && destructor_panic {
        let cleanup = panic.take_cleanup().expect("same explicit F Drop cause");
        let same_cleanup = cleanup.downcast_ref::<RawCause>().unwrap();
        assert_eq!(same_cleanup.stage, "cleanup");
        assert!(Arc::ptr_eq(&same_cleanup.hold, &cleanup_hold));
        assert!(panic.has_panic(), "same poll cause remains retained");
        dispose_original_checked(&f, cleanup, &cleanup_hold, Some(&body_hold), held_check).await;
    } else {
        assert!(panic.take_cleanup().is_none());
    }
    let original = role.take_work_panic().expect("same original raw cause");
    let same_original = original.downcast_ref::<RawCause>().unwrap();
    assert_eq!(
        same_original.stage,
        if body_panic { "body" } else { "cleanup" }
    );
    assert!(Arc::ptr_eq(
        &same_original.hold,
        if body_panic {
            &body_hold
        } else {
            &cleanup_hold
        }
    ));
    assert!(role.take_work_panic().is_none());
    assert!(!panic.has_panic());
    if destructor_panic {
        dispose_original_checked(
            &f,
            original,
            if body_panic {
                &body_hold
            } else {
                &cleanup_hold
            },
            None,
            held_check,
        )
        .await;
    } else {
        // Transfer the SAME original child into the actual raw disposal
        // capture. Its authority cannot return while that raw Drop is held.
        dispose_value_checked(
            &f,
            (original, ManuallyDrop::into_inner(owner)),
            |(raw, owner)| {
                drop(raw);
                owner.complete_after_terminal_cancel_settled();
            },
            &body_hold,
            None,
            held_check,
        )
        .await;
    }
    drop(release);
    for watchdog in watchdogs {
        watchdog.join().unwrap();
    }
    assert_eq!(body_hold.exited.load(Ordering::Acquire), body_panic);
    assert_eq!(
        cleanup_hold.exited.load(Ordering::Acquire),
        destructor_panic
    );
    if destructor_panic {
        // Both raw destructors and actual disposal jobs joined. The SAME
        // original F's failed Drop still supplies no Body/Captures exit.
        assert_eq!(original_custody.phase(), ExitCustodyPhase::Unresolved);
        assert!(!original_custody.body_exited() && !original_custody.captures_exited());
        assert!(!role.try_retire().unwrap());
        assert!(!header.is_retired());
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
        assert!(child.check().is_ok());
        // Preserve all original CR/WC custody to the real process boundary.
        // This is no claim of WC drain, capture exit or normal raw health.
        std::process::exit(0);
    }
    assert!(role.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&header).unwrap());
    tokio::time::timeout(Duration::from_secs(2), child.wait_released())
        .await
        .unwrap();
    assert!(matches!(child.check(), Err(WorkError::Released)));
    drop((
        ManuallyDrop::into_inner(role),
        original_custody,
        child,
        scope,
    ));
    let root = ManuallyDrop::into_inner(root);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    let control = ManuallyDrop::into_inner(control);
    control.close_admission();
    assert!(control.shutdown().is_ok());
    ManuallyDrop::into_inner(f).finish();
    std::process::exit(0);
}

pub(super) async fn source_open_poison() {
    let f = ManuallyDrop::new(Fixture::new(8, 4));
    let (control, driver, capacity) = wc::fixture();
    let control = ManuallyDrop::new(control);
    let mut driver = ManuallyDrop::new(driver);
    let capacity = ManuallyDrop::new(capacity);
    let root = ManuallyDrop::new(
        control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let scope = root.owner.scope();
    wc::bind(&scope, &mut driver);
    let grant = ManuallyDrop::new(
        capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap(),
    );
    let cohort = ManuallyDrop::new(wc::cohort(&scope, 1, &mut driver));
    let source = ManuallyDrop::new(
        SubscriptionSource::from_cohort(f.key.participation, &scope, &cohort, f.input.clone())
            .unwrap(),
    );
    let child = source.scope().unwrap();
    let generation = NonZeroU64::new(2).unwrap();
    let phase = f.phase(2);
    let request = Arc::new(SubscriptionRequest {
        participation: f.key.participation,
        generation,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    let job = source.prepare(request.clone(), &phase).unwrap();
    let hold = Hold::new();
    let release = ReleaseOnExit([hold.clone(), hold.clone()]);
    let watchdog = hold.watchdog();
    let entered = Arc::new(AtomicBool::new(false));
    let io = Arc::new(Io(Mutex::new(Some(OriginalOpen {
        body: Some(RawCause {
            stage: "body",
            hold: hold.clone(),
        }),
        cleanup: None,
        entered: entered.clone(),
    }))));
    let (work, header) = original_work(&f);
    let panic = PanicSink::new();
    let (role, exit, handoff) =
        OriginalTransportJoinObligation::prepare(work, ConsumerId::new(1).unwrap(), panic.clone())
            .unwrap_or_else(|e| panic!("actual opening panic role: {}", e.error));
    let mut role = ManuallyDrop::new(role);
    let custody = role.exit_custody();
    let panic_in = panic.clone();
    handoff
        .spawn(&Handle::current(), exit, move |exit, _| {
            let original = run_future(exit, job.run(io, Arc::new(NoFrames)), panic_in);
            async move {
                let _ = original.await;
            }
        })
        .unwrap();
    until(|| entered.load(Ordering::Acquire) && panic.has_panic()).await;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            poll_fn(|cx| role.poll_shared_completion(cx))
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
    let failed_join = role
        .take_join_failure()
        .expect("actual opening poll failed");
    assert!(failed_join.is_panic());
    drop(failed_join);
    let original = role.take_work_panic().expect("same opening poll raw cause");
    let same_original = original.downcast_ref::<RawCause>().unwrap();
    assert_eq!(same_original.stage, "body");
    assert!(Arc::ptr_eq(&same_original.hold, &hold));
    assert!(panic.take_cleanup().is_none());
    dispose_original(&f, original, &hold, None).await;
    assert!(!panic.has_panic());
    drop(release);
    watchdog.join().unwrap();
    source.close_new_jobs();
    f.drain_phase(&phase);
    f.drain_progress();
    // The real open poll frame unwound, the SAME original raw was disposed,
    // and the original publisher/captures physically exited. No wire frame
    // or successful stream open is fabricated by this opening-panic case.
    assert!(
        f.seen
            .lock()
            .unwrap()
            .iter()
            .any(|(_, seen)| matches!(seen, Seen::Progress(_)))
    );
    assert!(f.bank.progress_debts.lock().unwrap().is_empty());
    let key = StreamKey {
        participation: f.key.participation,
        generation,
    };
    assert!(f.bank.stream_claims_exited(key));
    {
        let state = f.input.0.state.lock().unwrap();
        let stream = state.streams.get(&key).unwrap();
        assert!(stream.poisoned);
        assert!(stream.closed);
        assert!(!stream.polling && !stream.confirming && !stream.pending);
        assert_eq!(stream.accepted_unretired, 0);
    }
    assert!(role.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&header).unwrap());
    assert!(matches!(
        source.reap_generation(generation),
        Err(
            crate::task_execution::host::subscriptions::SubscriptionSourceError::Ingress(
                ObservationError::Busy
            )
        )
    ));
    assert!(source.close_after_jobs().is_err());
    assert!(child.check().is_ok());
    assert!(
        control
            .snapshot()
            .scopes
            .iter()
            .any(|row| { row.id == child.id() && row.parent == Some(scope.id()) })
    );
    assert!(role.take_work_panic().is_none());
    // All actual carrier and raw-disposal jobs joined. The deliberate normal
    // stream poison refusal retains the SAME Source/WC child and originals;
    // no normal stream reap, Scope removal or WC-empty claim is made.
    let _original_grant = grant;
    std::process::exit(0);
}
