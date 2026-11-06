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

//! Driver-owned Fanout exit oracles; include from driver.rs with
//! #[path = "shape/fanout_exit_tests.rs"] mod fanout_exit_tests.
//! Unknown Driver Drop assertions require real production owner custody.
use super::{DriverState, PipelineDriver};
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    operator::{
        ExitFailure, ExitObservation, FinishWatch, InputConstructionFactory, InputEncodingPolicy,
        Operator, OperatorLifecycle,
    },
    shape::{
        Source, SourceKernel, SourceStart,
        fanout::{Fanout, FanoutRouter, RouteStep},
    },
    step::{NextStep, StepResult},
};
use crate::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    observable::Observable,
    runtime_state::{RuntimeErrorState, RuntimeState},
    task::{ExecutionFailure, ExecutionResult},
    task_control::{TaskExecutionControl, TaskExecutionPort, test_support::Owner as TaskOwner},
};
use novarocks_execution_contract::{
    AbortCause, AcceptedTaskConclusion, TaskConclusionPort, TaskConclusionProposal,
    TaskFailurePhase, TaskIdentity,
};
use novarocks_failure_contract::FailureFact;
use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct ExitRouter;
impl InputEncodingPolicy for ExitRouter {}
impl FanoutRouter for ExitRouter {
    type Cursor = ();
    fn begin(
        &mut self,
        _: PreparedInput,
        _: &dyn InputConstructionFactory,
        _: &mut DriverStep<'_>,
    ) -> ExecutionResult<()> {
        unreachable!("exit-only fanout fixture")
    }
    fn advance(
        &mut self,
        _: &mut (),
        _: &dyn InputConstructionFactory,
        _: &mut DriverStep<'_>,
    ) -> ExecutionResult<RouteStep> {
        unreachable!("exit-only fanout fixture")
    }
    fn accepts_branch(&self, _: usize) -> bool {
        true
    }
}

// Only the actual existing Task author decides conclusions. The execution
// port supplies the one shared Health used by this complete Driver/Fanout.
struct Port {
    task: Arc<TaskOwner>,
    health: Arc<ExecutionHealth>,
    cleanups: AtomicUsize,
}
impl TaskConclusionPort<ExecutionFailure> for Port {
    fn identity(&self) -> TaskIdentity {
        self.task.identity()
    }
    fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.task.accepted()
    }
    fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
        self.task.submit(proposal)
    }
}
impl TaskExecutionPort for Port {
    fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
        self.health.clone()
    }
    fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
        self.cleanups.fetch_add(1, Ordering::SeqCst);
        self.task.report_cleanup(cause, phase);
    }
    fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity(),
                phase,
            },
            fact,
        );
    }
    fn is_stopped(&self) -> bool {
        self.task.is_stopped()
    }
    fn stop_observable(&self) -> Arc<Observable> {
        self.task.stop_observable()
    }
    fn wait_until_stopped(&self, duration: Duration) -> bool {
        self.task.wait_until_stopped(duration)
    }
}
fn state() -> (Arc<Port>, Arc<RuntimeState>) {
    let port = Arc::new(Port {
        task: TaskOwner::new(),
        health: Arc::new(ExecutionHealth::default()),
        cleanups: AtomicUsize::new(0),
    });
    let control =
        TaskExecutionControl::for_test(port.identity(), port.clone(), TaskFailurePhase::Execution)
            .unwrap();
    let failures = Arc::new(RuntimeErrorState::for_task(Arc::new(control)));
    (
        port,
        Arc::new(RuntimeState::new(
            None, None, None, None, None, None, None, failures,
        )),
    )
}
struct Resource {
    values: Vec<u64>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Resource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Secondary {
    _resource: Arc<Resource>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Secondary {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Primary {
    resource: ManuallyDrop<Arc<Resource>>,
    drops: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
}
impl Drop for Primary {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        let resource = unsafe { ManuallyDrop::take(&mut self.resource) };
        std::panic::panic_any(Secondary {
            _resource: resource,
            drops: self.secondary.clone(),
        });
    }
}
#[derive(Clone, Copy)]
enum HookFault {
    None,
    BeginError,
    CloseError,
    BeginOpaque,
    CloseOpaque,
}
impl HookFault {
    fn opaque(self) -> bool {
        matches!(self, Self::BeginOpaque | Self::CloseOpaque)
    }
}
struct Wake(Arc<Observable>);
impl futures::task::ArcWake for Wake {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.notify_observers();
    }
}
struct BranchOwner {
    index: usize,
    begins: Arc<Vec<AtomicUsize>>,
    closes: Arc<Vec<AtomicUsize>>,
    job: Option<tokio::task::JoinHandle<()>>,
    has_job: bool,
    joined: Arc<AtomicBool>,
    event: Arc<Observable>,
    fault: HookFault,
    error: Option<ExecutionFailure>,
    opaque: Option<Box<dyn std::any::Any + Send>>,
    required: Option<ExecutionFailure>,
    emits: Arc<AtomicUsize>,
    // This plain owner field is not ManuallyDrop: the unknown Driver must
    // actually retain its original boxed operator, not just leak a payload.
    _owner_resource: Option<Arc<Resource>>,
}
impl OperatorLifecycle for BranchOwner {
    fn name(&self) -> &str {
        "FANOUT_ACTUAL_EXIT_OWNER"
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.begins[self.index].fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, HookFault::BeginOpaque) {
            std::panic::resume_unwind(self.opaque.take().unwrap());
        }
        if matches!(self.fault, HookFault::BeginError) {
            return Err(self.error.take().unwrap());
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        if self.has_job {
            assert!(
                self.joined.load(Ordering::Acquire),
                "pending branch cannot close across actual task borrow"
            );
        }
        self.closes[self.index].fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, HookFault::CloseOpaque) {
            std::panic::resume_unwind(self.opaque.take().unwrap());
        }
        if matches!(self.fault, HookFault::CloseError) {
            return Err(self.error.take().unwrap());
        }
        Ok(())
    }
    fn observe_exit(&mut self) -> ExitObservation {
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        if let Some(job) = self.job.as_mut() {
            let wake = Arc::new(Wake(self.event.clone()));
            let waker = futures::task::waker_ref(&wake);
            let mut cx = Context::from_waker(&waker);
            match std::pin::Pin::new(job).poll(&mut cx) {
                Poll::Pending => {
                    return ExitObservation::pending(FinishWatch::Notify(self.event.clone()));
                }
                Poll::Ready(result) => {
                    result.expect("real accepted branch task returned");
                    self.job = None;
                    self.joined.store(true, Ordering::Release);
                }
            }
        }
        if let Some(required) = self.required.take() {
            self.emits.fetch_add(1, Ordering::SeqCst);
            ExitObservation::exited().with_failure(ExitFailure::Required(required))
        } else {
            ExitObservation::exited()
        }
    }
}
impl SourceKernel for BranchOwner {
    fn start(&self) -> SourceStart {
        SourceStart::Finished
    }
    fn poll_output(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}
struct Release(Arc<tokio::sync::Semaphore>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
struct Fixture {
    driver: PipelineDriver,
    port: Arc<Port>,
    gate: Release,
    begins: Arc<Vec<AtomicUsize>>,
    closes: Arc<Vec<AtomicUsize>>,
    joined: Arc<AtomicBool>,
    emits: Arc<AtomicUsize>,
    task_resource: Weak<Resource>,
    task_drops: Arc<AtomicUsize>,
    owner_resource: Weak<Resource>,
    owner_drops: Arc<AtomicUsize>,
    opaque_resource: Weak<Resource>,
    opaque_drops: Arc<AtomicUsize>,
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
}
fn fixture(
    runtime: &tokio::runtime::Runtime,
    mode: HookFault,
    required: Option<ExecutionFailure>,
) -> Fixture {
    let (port, state) = state();
    let gate = Release(Arc::new(tokio::sync::Semaphore::new(0)));
    let task_gate = gate.0.clone();
    let begins = Arc::new(vec![AtomicUsize::new(0), AtomicUsize::new(0)]);
    let closes = Arc::new(vec![AtomicUsize::new(0), AtomicUsize::new(0)]);
    let joined = Arc::new(AtomicBool::new(false));
    let emits = Arc::new(AtomicUsize::new(0));
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let owner_resource = Arc::new(Resource {
        values: vec![2, 3, 5],
        drops: owner_drops.clone(),
    });
    let owner_weak = Arc::downgrade(&owner_resource);
    let task_drops = Arc::new(AtomicUsize::new(0));
    let task_resource = Arc::new(Resource {
        values: vec![7, 11, 19],
        drops: task_drops.clone(),
    });
    let task_weak = Arc::downgrade(&task_resource);
    let opaque_drops = Arc::new(AtomicUsize::new(0));
    let primary = Arc::new(AtomicUsize::new(0));
    let secondary = Arc::new(AtomicUsize::new(0));
    let (opaque, opaque_weak) = if mode.opaque() {
        let data = Arc::new(Resource {
            values: vec![31, 37, 41],
            drops: opaque_drops.clone(),
        });
        let weak = Arc::downgrade(&data);
        (
            Some(Box::new(Primary {
                resource: ManuallyDrop::new(data),
                drops: primary.clone(),
                secondary: secondary.clone(),
            }) as Box<dyn std::any::Any + Send>),
            weak,
        )
    } else {
        (None, Weak::new())
    };
    let cleanup: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
    let first = BranchOwner {
        index: 0,
        begins: begins.clone(),
        closes: closes.clone(),
        job: None,
        has_job: false,
        joined: Arc::new(AtomicBool::new(true)),
        event: Arc::new(Observable::new()),
        fault: mode,
        error: Some(cleanup),
        opaque,
        required,
        emits: emits.clone(),
        _owner_resource: Some(owner_resource),
    };
    let second = BranchOwner {
        index: 1,
        begins: begins.clone(),
        closes: closes.clone(),
        job: Some(runtime.spawn(async move {
            let original = task_resource;
            task_gate.acquire().await.unwrap().forget();
            assert_eq!(original.values, [7, 11, 19]);
        })),
        has_job: true,
        joined: joined.clone(),
        event: Arc::new(Observable::new()),
        fault: HookFault::None,
        error: None,
        opaque: None,
        required: None,
        emits: emits.clone(),
        _owner_resource: None,
    };
    let branches: Vec<Box<dyn Operator>> =
        vec![Box::new(Source::new(first)), Box::new(Source::new(second))];
    let mut fanout = Fanout::new("actual exit Fanout".into(), branches, ExitRouter, None);
    crate::exec::pipeline::shape::fanout::test_support::prepare_on(&mut fanout, &state);
    let driver = PipelineDriver::new(1, vec![Box::new(fanout)], None, vec![], state, None);
    Fixture {
        driver,
        port,
        gate,
        begins,
        closes,
        joined,
        emits,
        task_resource: task_weak,
        task_drops,
        owner_resource: owner_weak,
        owner_drops,
        opaque_resource: opaque_weak,
        opaque_drops,
        primary,
        secondary,
    }
}
fn wait_for_actual_join(fixture: &mut Fixture) -> DriverState {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = fixture.driver.process(Duration::ZERO);
        if fixture.joined.load(Ordering::Acquire) {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "real accepted sibling job must join"
        );
        std::thread::yield_now();
    }
}

#[test]
fn fanout_cleanup_error_or_opaque_panic_keeps_peer_and_closes_safe_actual_join_once() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    for mode in [
        HookFault::BeginError,
        HookFault::CloseError,
        HookFault::BeginOpaque,
        HookFault::CloseOpaque,
    ] {
        let mut f = fixture(&runtime, mode, None);
        f.port
            .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let peer = f.port.accepted().unwrap();
        assert_eq!(
            f.driver.finish_with_state(DriverState::Canceled),
            DriverState::PendingFinish
        );
        for begin in f.begins.iter() {
            assert_eq!(begin.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            f.closes[0].load(Ordering::SeqCst),
            1,
            "ready owner closes independently, including after its Begin unknown"
        );
        assert_eq!(
            f.closes[1].load(Ordering::SeqCst),
            0,
            "real gated sibling must not close early"
        );
        assert!(!f.joined.load(Ordering::Acquire));
        assert_eq!(f.task_resource.upgrade().unwrap().values, [7, 11, 19]);
        assert_eq!(f.task_drops.load(Ordering::SeqCst), 0);
        assert!(Arc::ptr_eq(&peer, &f.port.accepted().unwrap()));
        f.gate.0.add_permits(1);
        let outcome = wait_for_actual_join(&mut f);
        assert!(f.task_resource.upgrade().is_none());
        assert_eq!(f.task_drops.load(Ordering::SeqCst), 1);
        assert_eq!(f.closes[1].load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&peer, &f.port.accepted().unwrap()));
        if mode.opaque() {
            assert_eq!(outcome, DriverState::PendingFinish);
            assert!(!f.port.health.is_healthy());
            let health = f.port.health.failure();
            assert_eq!(f.primary.load(Ordering::SeqCst), 1);
            assert_eq!(f.secondary.load(Ordering::SeqCst), 0);
            assert_eq!(f.opaque_resource.upgrade().unwrap().values, [31, 37, 41]);
            for _ in 0..3 {
                assert_eq!(f.driver.process(Duration::ZERO), DriverState::PendingFinish);
            }
            assert_eq!(f.port.health.failure(), health);
        } else {
            assert_eq!(outcome, DriverState::Canceled);
            assert!(f.port.health.is_healthy());
        }
        for begin in f.begins.iter() {
            assert_eq!(begin.load(Ordering::SeqCst), 1);
        }
        for close in f.closes.iter() {
            assert_eq!(close.load(Ordering::SeqCst), 1);
        }
        assert!(f.port.cleanups.load(Ordering::SeqCst) > 0);
        assert_eq!(f.emits.load(Ordering::SeqCst), 0);
        // This is a real facade Drop, not a test ManuallyDrop/forget. The
        // production unknown Driver custody is a required integration gate.
        drop(f.driver);
        if mode.opaque() {
            assert_eq!(
                f.owner_resource
                    .upgrade()
                    .expect("original unknown Fanout branch Box remains in Driver custody")
                    .values,
                [2, 3, 5]
            );
            assert_eq!(f.owner_drops.load(Ordering::SeqCst), 0);
            assert_eq!(f.opaque_resource.upgrade().unwrap().values, [31, 37, 41]);
            assert_eq!(f.opaque_drops.load(Ordering::SeqCst), 0);
            assert_eq!(f.secondary.load(Ordering::SeqCst), 0);
        } else {
            assert!(f.owner_resource.upgrade().is_none());
            assert_eq!(f.owner_drops.load(Ordering::SeqCst), 1);
        }
        assert!(Arc::ptr_eq(&peer, &f.port.accepted().unwrap()));
    }
}

#[test]
fn fanout_required_event_refines_peer_once_before_independent_actual_sibling_join() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let original: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
    let mut f = fixture(&runtime, HookFault::None, Some(original.clone()));
    f.port
        .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
    let peer = f.port.accepted().unwrap();
    assert_eq!(
        f.driver.finish_with_state(DriverState::Finished),
        DriverState::PendingFinish
    );
    let accepted = f.port.accepted().unwrap();
    assert!(!Arc::ptr_eq(&peer, &accepted));
    assert!(
        accepted
            .originating_failure()
            .unwrap()
            .same_occurrence(&original)
    );
    assert_eq!(f.emits.load(Ordering::SeqCst), 1);
    assert_eq!(f.closes[0].load(Ordering::SeqCst), 1);
    assert_eq!(f.closes[1].load(Ordering::SeqCst), 0);
    assert_eq!(f.task_drops.load(Ordering::SeqCst), 0);
    for _ in 0..3 {
        assert_eq!(f.driver.process(Duration::ZERO), DriverState::PendingFinish);
        assert!(Arc::ptr_eq(&accepted, &f.port.accepted().unwrap()));
    }
    f.gate.0.add_permits(1);
    let DriverState::Failed(actual) = wait_for_actual_join(&mut f) else {
        panic!("actual Required event prevents success");
    };
    assert!(actual.same_occurrence(&original));
    assert_eq!(f.emits.load(Ordering::SeqCst), 1);
    assert_eq!(f.closes[1].load(Ordering::SeqCst), 1);
    assert!(f.task_resource.upgrade().is_none());
    assert_eq!(f.task_drops.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&accepted, &f.port.accepted().unwrap()));
    assert!(f.port.health.is_healthy());
    drop(f.driver);
    assert!(f.owner_resource.upgrade().is_none());
    assert_eq!(f.owner_drops.load(Ordering::SeqCst), 1);
}
