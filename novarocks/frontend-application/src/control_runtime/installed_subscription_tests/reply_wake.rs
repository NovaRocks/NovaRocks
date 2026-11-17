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

//! Ordinary reply panic on the same installed Source and service bank.
//! Finite ProtocolRecipe carrier, no Worker execution or Native M5 claim.
use super::*;
use crate::fixtures::history::{CapturedHistory, WorkExpectation, WorkObservation};
use crate::fixtures::native_recipe::{
    OriginalRecipeProcessPolicy, prepare_original_recipe_process_with_policy,
};
use novarocks_control_runtime::actor_turn::ActorTurnError;
use novarocks_control_runtime_core::history::HistoryFact;
use novarocks_coordination_contract::SelectedTaskTermination;
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
use novarocks_execution_contract::{TaskFailure, TaskFailurePhase, TerminationDetail};
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
    ExecutionOutputObligation, QueryExecutionError,
};
use novarocks_query_application::coordination::{
    LogicalExecutionExitReceipt, LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig,
};
use std::{
    mem::ManuallyDrop,
    num::NonZeroU16,
    sync::{
        Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
};

struct OriginalReplyCause(Arc<AtomicUsize>);
impl Drop for OriginalReplyCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct OriginalReplyWake {
    original: Mutex<Option<Arc<OriginalReplyCause>>>,
    calls: AtomicUsize,
}
impl Wake for OriginalReplyWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let original = { self.original.lock().unwrap().take() };
        if let Some(original) = original {
            std::panic::panic_any(original);
        }
    }
}
// Freeze the two original peer observations before process/client/Root
// construction. This is a finite read-only port, not a backend registry.
struct OriginalPeers([BackendProcessDescriptor; 2]);
impl BackendProcessObservationPort for OriginalPeers {
    fn observe_process_at_endpoint(
        &self,
        process: BackendProcessId,
        endpoint: &novarocks_execution_contract::RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        let original = self
            .0
            .iter()
            .find(|descriptor| descriptor.process_id() == process)
            .ok_or_else(|| BackendTopologyError::Unavailable {
                message: "foreign original reply-wake peer process".into(),
            })?;
        if original.endpoint() != endpoint {
            return Err(BackendTopologyError::Unavailable {
                message: "foreign original reply-wake peer endpoint".into(),
            });
        }
        Ok(BackendProcessObservation::Current)
    }
}
#[derive(Default)]
struct Endpoint {
    owner: Option<novarocks_workload_control::WorkOwner>,
    business: Option<novarocks_workload_control::BusinessPermit>,
    scope: Option<novarocks_workload_control::WorkScope>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    logical: Option<ActualLogicalCase>,
    output: Option<ProfileComputationOutput>,
    cohort:
        Option<crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort>,
    installed: Option<Arc<crate::task_execution::host::install::InstalledAttemptHost>>,
    result: Option<Result<ExecutionOutputObligation, QueryExecutionError>>,
    exit: Option<LogicalExecutionExitReceipt>,
}
struct OriginalRun {
    data: FrontendDataRuntime,
    peer: Option<Loopback>,
    sibling_peer: Option<Loopback>,
    bootstrap: Option<PreparedOriginalRecipeProcess>,
    primary: Endpoint,
    sibling: Endpoint,
    raw: Weak<OriginalReplyCause>,
    wake: Option<Arc<OriginalReplyWake>>,
}
fn install_endpoint(
    original: &mut Endpoint,
    bootstrap: &mut PreparedOriginalRecipeProcess,
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
) {
    let root = bootstrap
        .workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    original.owner = Some(root.owner);
    original.business = Some(root.business);
    original.scope = Some(original.owner.as_ref().unwrap().scope());
    let scope = original.scope.as_ref().unwrap();
    original.window = Some(
        bootstrap
            .capacity
            .try_acquire(scope, ResultWindowClass::Internal)
            .unwrap(),
    );
    let (description, plan) = count_description();
    let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
    original.output = Some(output);
    let (logical, cohort, ()) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut bootstrap.fixture,
            joint: &mut bootstrap.joint,
            process: &bootstrap.process,
            data,
            clock: &bootstrap.clock,
            handle: bootstrap.handle.clone(),
            descriptor,
            client: bootstrap.client.clone(),
        },
        OriginalRecipeOperands {
            owner: original.owner.take().unwrap(),
            binding: QueryResultCapacityBinding::try_new(
                scope,
                original.window.as_ref().unwrap().retain_alias(),
            )
            .unwrap(),
            description,
            plan,
            root: RecipeRoot::EmptyCountOnly,
            consumer,
            offers,
            output: (),
        },
    );
    original.logical = Some(logical);
    original.cohort = Some(cohort);
}
fn original_task_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::MAX, NonZeroU16::MIN),
        ),
        novarocks_execution_contract::SafeDetail::new(
            "original Task failure before ordinary reply wake",
        )
        .unwrap(),
        TaskFailurePhase::Execution,
    )
}
fn assert_same_raw(original: &OriginalRun, drops: &AtomicUsize) {
    assert!(
        original
            .wake
            .as_ref()
            .unwrap()
            .original
            .lock()
            .unwrap()
            .is_none()
    );
    assert!(
        original.raw.upgrade().is_some(),
        "the same typed opaque cause remains retained"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}
#[test]
fn original_root_joins_installed_actor_after_actual_reply_wake_panics_and_keeps_serving() {
    const LEAF: &str = "reply_wake::original_root_joins_installed_actor_after_actual_reply_wake_panics_and_keeps_serving";
    let output = super::run_installed_unknown_subprocess(
        LEAF,
        "TST_G9B3_INSTALLED_REPLY_WAKE_CHILD",
        |runtime| runtime.block_on(run()),
    );
    // Verify the exact libtest selection separately, without a new application
    // log marker. The child may return zero only at its final process boundary.
    assert!(output.status.success());
}
async fn run() {
    let wait = Duration::from_secs(5);
    // The WHOLE same unresolved process is retained to actual process exit,
    // including when an assertion unwinds to the outer subprocess catcher.
    let mut storage = ManuallyDrop::new(OriginalRun {
        data: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        peer: None,
        sibling_peer: None,
        bootstrap: None,
        primary: Endpoint::default(),
        sibling: Endpoint::default(),
        raw: Weak::new(),
        wake: None,
    });
    storage.peer = Some(Loopback::start().await);
    storage.sibling_peer = Some(Loopback::start().await);
    let descriptor = storage
        .peer
        .as_ref()
        .unwrap()
        .bounded_root_descriptor(BackendProcessId::new_v7());
    let sibling_descriptor = storage
        .sibling_peer
        .as_ref()
        .unwrap()
        .bounded_root_descriptor(BackendProcessId::new_v7());
    assert_ne!(descriptor.process_id(), sibling_descriptor.process_id());
    assert_ne!(descriptor.endpoint(), sibling_descriptor.endpoint());
    let history = novarocks_control_runtime::history::HistoryCapture::new(65_536);
    storage.bootstrap = Some(prepare_original_recipe_process_with_policy(
        &storage.data,
        descriptor.clone(),
        history.clone(),
        ResultCapacityConfig::V1,
        None,
        OriginalRecipeProcessPolicy {
            // Preserve the existing recipe's exact default supervisor policy.
            supervisor: LogicalExecutionSupervisorConfig::new(
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    std::num::NonZeroUsize::MIN,
                    std::num::NonZeroU32::new(1).unwrap(),
                    Duration::from_secs(5),
                ),
            ),
            namespace: novarocks_types::identity::QueryProcessNamespace::new(712),
            observation_budget: None,
            process_observation: Arc::new(OriginalPeers([
                descriptor.clone(),
                sibling_descriptor.clone(),
            ])),
        },
    ));
    let original: &mut OriginalRun = &mut storage;
    install_endpoint(
        &mut original.primary,
        original.bootstrap.as_mut().unwrap(),
        &original.data,
        descriptor.clone(),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let raw = Arc::new(OriginalReplyCause(drops.clone()));
    original.raw = Arc::downgrade(&raw);
    original.wake = Some(Arc::new(OriginalReplyWake {
        original: Mutex::new(Some(raw)),
        calls: AtomicUsize::new(0),
    }));
    let waker = Waker::from(original.wake.as_ref().unwrap().clone());
    assert!(
        original
            .primary
            .logical
            .as_mut()
            .unwrap()
            .ready
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let gate = Arc::new(ReadCloseGate::default());
    let _release_on_unwind = gate.release_guard();
    let bootstrap = original.bootstrap.as_mut().unwrap();
    let logical = original.primary.logical.as_ref().unwrap();
    *logical.probe.read_close_gate.lock().unwrap() = Some(gate.clone());
    original.primary.installed = Some(
        bootstrap
            .fixture
            .activate_attempt(logical, &mut bootstrap.joint, false)
            .await,
    );
    let root = logical.probe.root.lock().unwrap().unwrap();
    assert_eq!(root.backend_process_id(), descriptor.process_id());
    let bound = logical.binding.bound().unwrap();
    let lp_actor = bound.registration.actor();
    let lp_identity = bound.start.actor_instance;
    let peer = original.peer.as_ref().unwrap();
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    peer.defer_single_root_terminal().unwrap();
    peer.defer_single_root_first_reply().unwrap();
    logical.probe.release_activation();
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while !peer.single_root_first_reply_waiting() {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert!(!peer.subscription_requests().is_empty());
    let selected =
        SelectedTaskTermination::new(root, TerminationDetail::Failed(original_task_failure()));
    let expected =
        novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(
            &selected,
        )
        .unwrap();
    peer.publish_single_root_terminal(Some(original_task_failure()))
        .unwrap();
    // Do not replace the exact registered reply Waker before its real send.
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while original.wake.as_ref().unwrap().calls.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert_eq!(
        original.wake.as_ref().unwrap().calls.load(Ordering::SeqCst),
        1
    );
    original.primary.result = Some(
        original
            .primary
            .logical
            .as_mut()
            .unwrap()
            .ready
            .take()
            .unwrap()
            .await,
    );
    let error = original
        .primary
        .result
        .as_ref()
        .unwrap()
        .as_ref()
        .err()
        .expect("same original ordinary failure promise");
    assert_eq!(error.execution_failure(), Some(&expected));
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while !bootstrap.process.runtime.snapshot(lp_actor).unwrap().failed {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while !gate.entered() {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert!(
        !*original
            .primary
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );
    let exit = original
        .primary
        .logical
        .as_mut()
        .unwrap()
        .exit
        .as_mut()
        .unwrap();
    assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
    gate.release();
    // The original cleanup has an indexed absolute deadline on this SAME
    // ManualClock. Reuse the original idle/deadline driver, never renew it.
    let exit_result = drive_until_with_original_clock(
        &bootstrap.process,
        &mut bootstrap.joint,
        std::future::poll_fn(|cx| {
            original
                .primary
                .logical
                .as_mut()
                .unwrap()
                .exit
                .as_mut()
                .unwrap()
                .poll_exit(cx)
        }),
        Instant::now() + wait,
        Some(bootstrap.clock.as_ref()),
    )
    .await;
    if exit_result.is_none() {
        let logical = original.primary.logical.as_ref().unwrap();
        let installed = original.primary.installed.as_ref().unwrap();
        let original_peer = original.peer.as_ref().unwrap();
        // Read original metadata only. In particular the failed LP Core is
        // never read and neither held physical gate is advanced here.
        eprintln!(
            "original reply-wake exit wait expired: clock={:?}, indexed_deadline={:?}, lp={:?}, ap={:?}, subscription={:?}, ledger_unresolved={:?}, ledger_empty={}, host_empty={}, read_close_entered={}, read_close_calls={}, actual_ap_exit={}, peer_root_progress={:?}, peer_first_reply_waiting={}, raw_retained={}, raw_drops={}, ordinary_reply_calls={}, original_ready_result={:?}, original_failure={}",
            bootstrap.process.clock.now(),
            bootstrap.process.actor_runtime().next_deadline(),
            bootstrap.process.actor_runtime().snapshot(lp_actor),
            bootstrap
                .process
                .actor_runtime()
                .snapshot(installed.fixture_sources.0),
            bootstrap
                .process
                .actor_runtime()
                .snapshot(installed.fixture_sources.1),
            bootstrap.process.ledger().unresolved_work(),
            bootstrap.process.ledger().is_empty(),
            bootstrap.process.host.is_empty(),
            gate.entered(),
            gate.calls(),
            *installed.fixture_exit.observe_actual_exit().borrow(),
            original_peer.single_root_result_progress(),
            original_peer.single_root_first_reply_waiting(),
            original.raw.upgrade().is_some(),
            drops.load(Ordering::SeqCst),
            original.wake.as_ref().unwrap().calls.load(Ordering::SeqCst),
            original
                .primary
                .result
                .as_ref()
                .and_then(|result| result.as_ref().err()),
            logical.binding.original_failure_for_fixture(),
        );
    }
    original.primary.exit = Some(exit_result.unwrap().unwrap());
    // The activated Native return has its own paid-return receipt. Physical
    // AP/LP job/Source exit is checked independently below; this enum proves
    // neither a raw-cause Drop nor process/WC responsibility reclamation.
    assert_eq!(
        original.primary.exit.as_ref().unwrap().local_disposition(),
        novarocks_query_application::api::LocalExecutionDisposition::OriginalPaidOwnerReturn
    );
    {
        let view = original.primary.exit.as_ref().unwrap().view();
        assert_eq!(view.pending_native_returns(), 0);
        assert!(view.native_preparation_exited());
    }
    assert!(matches!(
        original
            .primary
            .exit
            .as_ref()
            .unwrap()
            .view()
            .runtime_actor_result(),
        Some(novarocks_query_application::api::logical_runtime::LogicalExit::OwnerFailed(_))
    ));
    assert!(
        *original
            .primary
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while bootstrap.process.host.contains_logical(lp_identity) {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert!(matches!(
        bootstrap.process.runtime.snapshot(lp_actor),
        Err(ActorTurnError::UnknownActor)
    ));
    // Source final exit is gated by the SAME jobs' actual Tokio Join. Independently
    // require real Started -> BodyExited -> CapturesExited -> Retired for every
    // dispatched primary LP Work, including the original ordinary Consumer job.
    let captured = CapturedHistory::drain(&history, 65_536);
    captured.check_capture().unwrap();
    let primary_work = captured
        .effect_keys()
        .into_iter()
        .filter(|key| key.target.actor == lp_actor)
        .map(|key| key.financial_work)
        .collect::<Vec<_>>();
    assert!(!primary_work.is_empty());
    let mut actual_started = 0;
    for work in primary_work {
        let started = captured.entries().iter().any(|entry| {
            matches!(entry.fact,
            HistoryFact::Started { scope, work: id, .. } if scope == work.scope && id == work.work)
        });
        if started {
            actual_started += 1;
            assert_eq!(
                captured.check_work(work, WorkExpectation::Retired).unwrap(),
                WorkObservation::Retired
            );
        } else {
            // A sibling effect may have been refused before its launch at the
            // same failure cut. It needs its original unused exit, not Started.
            assert_eq!(
                captured
                    .check_work(work, WorkExpectation::UnusedRetired)
                    .unwrap(),
                WorkObservation::UnusedRetired
            );
        }
    }
    assert!(actual_started != 0);
    assert!(original.raw.upgrade().is_some());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!bootstrap.supervisor.service_has_exited());
    // This is the SAME client/bank/process; only a new original WC Root is
    // issued. Its separate fixed peer has not served any original RPC yet.
    install_endpoint(
        &mut original.sibling,
        bootstrap,
        &original.data,
        sibling_descriptor.clone(),
    );
    let sibling = original.sibling.logical.as_ref().unwrap();
    original.sibling.installed = Some(
        bootstrap
            .fixture
            .activate_attempt(sibling, &mut bootstrap.joint, false)
            .await,
    );
    let sibling_root = sibling.probe.root.lock().unwrap().unwrap();
    assert_eq!(
        sibling_root.backend_process_id(),
        sibling_descriptor.process_id()
    );
    let sibling_bound = sibling.binding.bound().unwrap();
    assert_ne!(sibling_bound.start.identity, bound.start.identity);
    let sibling_peer = original.sibling_peer.as_ref().unwrap();
    assert!(sibling_peer.subscription_requests().is_empty());
    sibling_peer.enable_single_root_lifecycle(
        sibling_root,
        novarocks_result_contract::RootOutputKind::CountOnly,
    );
    sibling.probe.release_activation();
    original.sibling.result = Some(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            original
                .sibling
                .logical
                .as_mut()
                .unwrap()
                .ready
                .take()
                .unwrap(),
            Instant::now() + wait,
        )
        .await
        .unwrap(),
    );
    assert!(
        original
            .sibling
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .ready_token()
            .is_some()
    );
    original.sibling.exit = Some(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            std::future::poll_fn(|cx| {
                original
                    .sibling
                    .logical
                    .as_mut()
                    .unwrap()
                    .exit
                    .as_mut()
                    .unwrap()
                    .poll_exit(cx)
            }),
            Instant::now() + wait,
        )
        .await
        .unwrap()
        .unwrap(),
    );
    // The activated Native return has its own paid-return receipt. Physical
    // AP/LP job/Source exit is checked independently below; this enum proves
    // neither a raw-cause Drop nor process/WC responsibility reclamation.
    assert_eq!(
        original.sibling.exit.as_ref().unwrap().local_disposition(),
        novarocks_query_application::api::LocalExecutionDisposition::OriginalPaidOwnerReturn
    );
    {
        let view = original.sibling.exit.as_ref().unwrap().view();
        assert_eq!(view.pending_native_returns(), 0);
        assert!(view.native_preparation_exited());
    }
    assert!(
        *original
            .sibling
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );
    assert!(
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            async {
                while bootstrap
                    .process
                    .host
                    .contains_logical(sibling_bound.start.actor_instance)
                {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait
        )
        .await
        .is_some()
    );
    assert!(matches!(
        bootstrap
            .process
            .runtime
            .snapshot(sibling_bound.registration.actor()),
        Err(ActorTurnError::UnknownActor)
    ));
    assert!(!bootstrap.supervisor.service_has_exited());
    assert!(!sibling_peer.subscription_requests().is_empty());
    // Both original AP/LP Sources have actually exited before these finite
    // test servers are stopped. These are the peers' own actual Joins, not
    // evidence for Native/raw/WC exit in the retained primary process.
    assert!(original.peer.as_mut().unwrap().settle().await);
    assert!(original.sibling_peer.as_mut().unwrap().settle().await);
    let residual = bootstrap.workload.snapshot();
    assert!(
        residual
            .scopes
            .iter()
            .any(|row| row.id == original.primary.scope.as_ref().unwrap().id())
    );
    assert!(original.primary.business.is_some() && original.primary.window.is_some());
    assert!(original.sibling.business.is_some() && original.sibling.window.is_some());
    eprintln!(
        "Original reply-wake process disposition retains actual Scope/Work responsibility: {residual:?}"
    );
    assert_same_raw(original, &drops);
    // Whole original receipts, windows/business permits and raw UNKNOWN stay in
    // this SAME process owner. No WC/Scope drain or raw-payload Drop is claimed.
    std::process::exit(0);
}
