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

use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests::{
    drive, finish, fixture,
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    api::ReservedExecutionIdentity,
    cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner},
};
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingExit as Exit, StateStoreBlockingOrigin as Origin,
    StateStoreBlockingOriginal as Original, StateStoreBlockingPhase as Phase,
    StateStoreBlockingRequest as Request, StateStoreBlockingSlot,
    StateStoreBlockingSourceVocabulary as Vocabulary, StateStoreError, StateStoreErrorKind,
    StateStoreExecutionFailure as Failure, StateStoreProviderId,
};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_workload_control::{ResultWindowClass, RootWork, WorkClass, WorkOwner, WorkRequest};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
    time::Duration,
};
use tokio::sync::Notify;

// This probe uses the real ordinary QB result/footer. Its only operation is a
// rescueable physical gate; it does not synthesize a SQLite transaction result
// or copy a provider's adjudication algorithm.
pub(crate) struct Gate {
    open: Mutex<bool>,
    condition: Condvar,
    entered: AtomicBool,
    notification: Notify,
}
impl Gate {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            condition: Condvar::new(),
            entered: AtomicBool::new(false),
            notification: Notify::new(),
        })
    }
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        self.notification.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.condition.wait(open).unwrap();
        }
    }
    pub(crate) async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notice = self.notification.notified();
                tokio::pin!(notice);
                notice.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    break;
                }
                notice.await;
            }
        })
        .await
        .expect("the actual worker must reach its rescueable gate");
    }
    pub(crate) fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.condition.notify_all();
    }
}
pub(crate) struct Rescue(pub(crate) Arc<Gate>);
impl Drop for Rescue {
    fn drop(&mut self) {
        self.0.release();
    }
}
pub(crate) struct PauseResultWake {
    pub(crate) caller: std::thread::ThreadId,
    pub(crate) caller_wakes: AtomicUsize,
    pub(crate) read_returned: Arc<AtomicBool>,
    pub(crate) once: AtomicBool,
    pub(crate) footer: Arc<Gate>,
}
impl Wake for PauseResultWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // Borrower::notify_request synchronously wakes this same Tokio thread.
        // It must return so the test can release the physical read gate.
        if std::thread::current().id() == self.caller {
            self.caller_wakes.fetch_add(1, Ordering::AcqRel);
            return;
        }
        // The worker can notify queue capacity before running the call. Only
        // the first worker notification after this Get has returned can hold
        // completion.send, whose stable result precedes exit.publish.
        if self.read_returned.load(Ordering::Acquire) && !self.once.swap(true, Ordering::AcqRel) {
            self.footer.wait();
        }
    }
}

// One original receiving slot, first Get, then an idle obligation, then Close.
// Only actual receive_exit advances either phase. Idle is never complete.
pub(crate) struct IdleOriginal {
    pub(crate) instance: InstanceScope,
    pub(crate) slot: Mutex<Option<Box<dyn StateStoreBlockingSlot>>>,
    pub(crate) read: Arc<Gate>,
    pub(crate) read_returned: Arc<AtomicBool>,
    pub(crate) close: Arc<Gate>,
    pub(crate) closing: AtomicBool,
    pub(crate) results: AtomicUsize,
    pub(crate) exits: AtomicUsize,
}
impl IdleOriginal {
    fn request(phase: Phase) -> Request {
        Request {
            phase,
            revision: NonZeroU64::new(if phase == Phase::Get { 1 } else { 2 }).unwrap(),
        }
    }
    fn index(request: Request) -> usize {
        if request == Self::request(Phase::Get) {
            0
        } else {
            assert_eq!(request, Self::request(Phase::Close));
            1
        }
    }
}
impl Original for IdleOriginal {
    fn origin(&self) -> Origin {
        Origin::Instance(self.instance)
    }
    fn requested(&self) -> Option<Request> {
        match self.exits.load(Ordering::Acquire) {
            0 => Some(Self::request(Phase::Get)),
            1 if self.closing.load(Ordering::Acquire) => Some(Self::request(Phase::Close)),
            1 | 2 => None,
            _ => unreachable!(),
        }
    }
    fn publication_refusal(&self, _: Request) -> Option<Failure> {
        None
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        assert!(self.slot.lock().unwrap().is_some());
        assert_eq!(Self::index(request), self.exits.load(Ordering::Acquire));
        Ok(())
    }
    fn run_original_phase(&self, request: Request) {
        if Self::index(request) == 0 {
            self.read.wait();
            self.read_returned.store(true, Ordering::Release);
        } else {
            self.close.wait();
        }
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        result.expect("the actual original operation returns normally");
        assert_eq!(
            self.results.fetch_add(1, Ordering::AcqRel),
            Self::index(request)
        );
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        assert_eq!(exit.unwrap(), Exit::ActualExited);
        let index = Self::index(request);
        assert_eq!(self.results.load(Ordering::Acquire), index + 1);
        assert_eq!(self.exits.fetch_add(1, Ordering::AcqRel), index);
    }
    fn is_complete(&self) -> bool {
        self.exits.load(Ordering::Acquire) == 2
    }
    fn local_failure(&self) -> Option<Failure> {
        None
    }
    fn request_close(&self) {
        self.closing.store(true, Ordering::Release);
    }
    fn request_interrupt(&self) {}
}

struct NoNativeLaunch;
impl LogicalExecutionLauncher for NoNativeLaunch {
    #[cfg(novarocks_dev_controls)]
    fn resolve_statement_observation(
        &self,
        _: &novarocks_failpoint::target_binding::StatementWorkNomination,
    ) -> Option<crate::preparation_diagnostics::StatementObservationHandle> {
        panic!("this fixture cannot resolve a Native statement observation")
    }

    fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
        panic!("Failed Read cannot reserve Native work")
    }
    fn start_read_in(
        &self,
        _: &mut Option<crate::query_execution::completion::PreparedLogicalRead>,
        _: &mut Option<novarocks_workload_control::WorkOwner>,
        _: &QueryResultCapacityBinding,
        _: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
        _: &mut OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] _: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>
    {
        panic!("Failed Read fixture cannot start ordinary Read")
    }
    fn start_computation_in(
        &self,
        _: &mut Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
        _: &mut Option<WorkOwner>,
        _: &QueryResultCapacityBinding,
        _: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        _: &mut OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] _: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>
    {
        panic!("Failed Read cannot start Native work")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn read_error_hands_same_idle_original_to_residual_only_after_actual_footer() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let cancellation = scope.cancellation().unwrap();
    let mut work = Some(root.owner);
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let instance = InstanceScope::new();
    let original_owner = StateStoreOriginalOwner::for_root(
        &workers.executor(),
        Vocabulary::for_provider::<IdleOriginal, IdleOriginal, IdleOriginal>(
            StateStoreProviderId::new("read-error-residual-fixture"),
        ),
        instance,
        scope.clone(),
        StateStoreRunPolicy::default(),
    )
    .unwrap();
    let read_gate = Gate::new();
    let footer_gate = Gate::new();
    let close_gate = Gate::new();
    let _read_rescue = Rescue(read_gate.clone());
    let _footer_rescue = Rescue(footer_gate.clone());
    let _close_rescue = Rescue(close_gate.clone());
    let source = Arc::new(IdleOriginal {
        instance,
        slot: Mutex::new(None),
        read: read_gate.clone(),
        read_returned: Arc::new(AtomicBool::new(false)),
        close: close_gate.clone(),
        closing: AtomicBool::new(false),
        results: AtomicUsize::new(0),
        exits: AtomicUsize::new(0),
    });
    let context = original_owner.context(Instant::now() + Duration::from_secs(30));
    let mut slot = None;
    let original: Arc<dyn Original> = source.clone();
    context.bind_original_in(&original, &mut slot).unwrap();
    *source.slot.lock().unwrap() = slot;
    context.request_progress();
    drop(original);

    let profile = local_preparation_profile().unwrap();
    let io = ReadMetadataIoSource::new(scope.clone(), profile.clone());
    io.configure_read_source(&profile).unwrap();
    let mut bootstrap_ready = false;
    for _ in 0..100 {
        bootstrap_ready = io
            .poll_progress(
                work.as_ref().unwrap(),
                &mut Context::from_waker(Waker::noop()),
            )
            .unwrap();
        drive(&mut driver);
        if bootstrap_ready {
            break;
        }
    }
    assert!(bootstrap_ready, "the original MIO/Root must actually admit");
    let provider = FrontendCompletionCustody::new();
    let cause = SharedFailureCause::from_static_detail(
        StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "read preparation refused",
        ),
        "read preparation refused",
    );
    let selected = cause.clone();
    let input = source.clone();
    let preparation: Preparation = Box::pin(async move {
        Ok(async {
            std::future::poll_fn(|cx| {
                input
                    .slot
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .poll_phase(cx, IdleOriginal::request(Phase::Get))
            })
            .await
            .map_err(QueryServiceError::from_failure_cause)?;
            Err(QueryServiceError::from_failure_cause(selected))
        }
        .await)
    });
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = grant.retain_alias();
    let stage = StatementStageAdmission::new(scope.clone(), 0)
        .with_client_window(&alias)
        .unwrap();
    drop((grant, alias));
    assert_eq!(
        capacity.snapshot().held_positions,
        [1, 0, 0, 0],
        "only the original Stage now retains the Client position"
    );
    let planning_cpu = FrontendPlanningCpuSource::new(
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
            tokio::runtime::Handle::current(),
        ),
    );
    let mut action = ReadStatementAction {
        family: novarocks_result_contract::ClientPresentationFamily::MysqlText,
        planning_cpu: planning_cpu.clone(),
        preparation_health: None,
        provider: provider.clone(),
        provider_close: Some(
            Box::pin(close_provider(provider.clone(), planning_cpu)) as ProviderClose
        ),
        provider_close_result: None,
        local: None,
        tail: Some(Box::new(ReadNativeTail {
            services: ReadNativeServices::new(Some(original_owner), io),
            native: ReadNativeStageTail::new(stage),
            health: None,
        })),
        preparation: Some(preparation),
        prepared: None,
        read: None,
        capacity: None,
        original: OriginalComputationStart::default(),
        execution: None,
        error: None,
        completed_preparation: false,
        preparation_started: false,
        bootstrap_ready,
        result_transferred: false,
        launcher: Arc::new(NoNativeLaunch),
        #[cfg(novarocks_dev_controls)]
        observation: None,
    };
    let original_tail = std::ptr::from_ref(action.tail.as_deref().unwrap()).cast::<()>();
    let pause = Arc::new(PauseResultWake {
        caller: std::thread::current().id(),
        caller_wakes: AtomicUsize::new(0),
        read_returned: source.read_returned.clone(),
        once: AtomicBool::new(false),
        footer: footer_gate.clone(),
    });
    let wake = Waker::from(pause.clone());
    let mut owner = GovernedActionContext::new(&scope, &mut work, &cancellation, None, None);
    assert!(
        action
            .poll_action(&mut owner, &mut Context::from_waker(&wake))
            .is_pending()
    );
    assert!(
        pause.caller_wakes.load(Ordering::Acquire) > 0,
        "the real borrower's synchronous self-notification returned"
    );
    assert!(!footer_gate.entered.load(Ordering::Acquire));
    read_gate.reached().await;
    assert!(!action.completed_preparation);
    assert!(action.provider_close_result.is_none());
    read_gate.release();
    footer_gate.reached().await;
    assert!(
        action
            .poll_action(&mut owner, &mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(source.results.load(Ordering::Acquire), 1);
    assert_eq!(source.exits.load(Ordering::Acquire), 0);
    assert_eq!(
        capacity.snapshot().held_positions,
        [1, 0, 0, 0],
        "the Client position remains held before the actual footer"
    );
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(action.completed_preparation && action.preparation.is_none());
    assert!(matches!(action.provider_close_result, Some(Ok(()))));
    assert_eq!(action.error.as_ref().unwrap().failure_cause(), Some(&cause));
    assert!(
        !action.result_transferred,
        "a stable result is not physical exit"
    );

    footer_gate.release();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            drive(&mut driver);
            action.poll_action(&mut owner, cx)
        }),
    )
    .await
    .expect("the actual footer must wake business error delivery");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("the original preparation failure must remain a failure"),
    };
    assert_eq!(error.failure_cause(), Some(&cause));
    assert_eq!(source.exits.load(Ordering::Acquire), 1);
    assert!(!source.is_complete());
    assert!(
        !source.closing.load(Ordering::Acquire),
        "business delivery cannot force idle settlement"
    );
    assert!(action.local_health_failure().is_none());
    assert!(action.result_transferred);
    assert_eq!(
        capacity.snapshot().held_positions,
        [0; 4],
        "business actual exit releases the Stage-only Client alias"
    );
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(scope.check().is_ok());
    let mut residual_slot = None;
    action.handoff_residual_in(&mut residual_slot).unwrap();
    assert!(action.tail.is_none());
    let residual = residual_slot.as_mut().unwrap();
    assert_eq!(std::ptr::from_ref(&**residual).cast::<()>(), original_tail);
    drop(action);
    assert!(
        residual
            .poll_retirement(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    close_gate.reached().await;
    assert!(
        residual
            .poll_retirement(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(source.exits.load(Ordering::Acquire), 1);
    assert!(!source.is_complete());
    assert_eq!(
        capacity.snapshot().held_positions,
        [0; 4],
        "same residual close cannot retain the completed Client position"
    );
    assert_eq!(
        control.snapshot().root_responsibilities,
        1,
        "Client release cannot retire the original Root responsibility"
    );
    assert!(scope.check().is_ok());
    close_gate.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| residual.poll_retirement(cx)),
    )
    .await
    .expect("same original close must receive its actual footer")
    .unwrap();
    assert!(source.is_complete());
    assert_eq!(source.results.load(Ordering::Acquire), 2);
    assert_eq!(source.exits.load(Ordering::Acquire), 2);
    drop((context, source, provider));
    // The test's observer retains the cancellation cell after Work exits.
    // Release it before requiring the original fixed storage to fully drain.
    drop(cancellation);
    // Drop the same residual before retiring the original Root.
    drop(residual_slot);
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    finish(
        control,
        RootWork {
            owner: work.take().unwrap(),
            business: root.business,
        },
        driver,
    );
}
