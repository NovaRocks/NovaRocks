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

// Original supervisor-bank contracts with bounded preparation refusal.
// Actual AP/BE activation and resource exit remain separate.
mod supervisor_minimum_bank_migration {
    use super::*;
    use crate::coordination::{OriginalBackgroundActionInput, OriginalLogicalStartInput};
    use std::sync::{Mutex, OnceLock};

    fn retain_whole_original<T: Send + 'static>(original: Box<T>) {
        static ORIGINALS: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> = OnceLock::new();
        ORIGINALS.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner()).push(original);
    }
    struct WholeOriginal<T: Send + 'static>(Option<Box<T>>);
    impl<T: Send + 'static> std::ops::Deref for WholeOriginal<T> {
        type Target = T;
        fn deref(&self) -> &T { self.0.as_deref().unwrap() }
    }
    impl<T: Send + 'static> std::ops::DerefMut for WholeOriginal<T> {
        fn deref_mut(&mut self) -> &mut T { self.0.as_deref_mut().unwrap() }
    }
    impl<T: Send + 'static> Drop for WholeOriginal<T> {
        fn drop(&mut self) {
            if std::thread::panicking() && let Some(original) = self.0.take() {
                retain_whole_original(original);
            }
        }
    }
    #[derive(Debug, Default)]
    struct OriginalPrepareSignals {
        entered: AtomicBool,
        calls: AtomicU64,
        future_drops: AtomicU64,
        port_drops: AtomicU64,
        execution: Mutex<Option<QueryExecutionId>>,
        release: tokio::sync::Notify,
    }
    struct OriginalPrepareFrame(Arc<OriginalPrepareSignals>);
    impl Drop for OriginalPrepareFrame {
        fn drop(&mut self) { self.0.future_drops.fetch_add(1, Ordering::SeqCst); }
    }
    #[derive(Debug)]
    struct OriginalRefusalPort(Arc<OriginalPrepareSignals>);
    impl Drop for OriginalRefusalPort {
        fn drop(&mut self) { self.0.port_drops.fetch_add(1, Ordering::SeqCst); }
    }
    impl NativeAttemptPreparationPort for OriginalRefusalPort {
        fn prepare_in<'a>(&'a mut self, request: &'a mut crate::api::NativeAttemptPreparationWork)
            -> NativeAttemptPreparationFuture<'a> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            *self.0.execution.lock().unwrap() = Some(request.execution());
            let frame = OriginalPrepareFrame(self.0.clone());
            Box::pin(async move {
                let _same_original_frame = frame;
                self.0.entered.store(true, Ordering::SeqCst);
                self.0.release.notified().await;
                Err(NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                    "original finite preparation refusal after accepted-bank handoff",
                )).into())
            })
        }
    }
    #[derive(Default)]
    struct OriginalBackgroundSlots {
        input: Option<OriginalBackgroundActionInput>,
        ready: Option<crate::session_control::BackgroundActionReady>,
        receipt: Option<crate::session_control::BackgroundActionReceipt>,
        context: Option<crate::admitted_query_context::QueryExecutionContext>,
        permit: Option<novarocks_workload_control::QueryConcurrencyPermit>,
        payload: Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
        window: Option<novarocks_workload_control::ResultWindowGrant>,
        action: Option<Box<dyn crate::session_control::GovernedBackgroundAction>>,
    }
    struct OriginalBankFixture<F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static> {
        control: TestGovernance<F>,
        supervisor: LogicalExecutionSupervisor,
        client: QueryExecutionClient,
        input: Option<OriginalLogicalStartInput>,
        owner: Option<WorkOwner>,
        ready: Option<crate::api::QueryExecutionFuture>,
        exit: Option<crate::coordination::LogicalExecutionExitObserver>,
        receipt: Option<crate::coordination::LogicalExecutionExitReceipt>,
        business: Option<novarocks_workload_control::BusinessPermit>,
        scope: Option<novarocks_workload_control::WorkScope>,
        originals: Vec<Arc<OriginalPrepareSignals>>,
        retained_exits: Vec<crate::coordination::LogicalExecutionExitObserver>,
        background: OriginalBackgroundSlots,
        notification: Option<std::task::Waker>,
    }
    async fn assert_original_native_bank_class_and_reply(drop_reply: bool) {
        let control = governance_empty();
        let (supervisor, client) = test_supervisor(&control, Handle::current(),
            Arc::new(BindingNativePort), QueryProcessNamespace::new(0x9b56),
            FrontendProcessId::new_v7(), supervisor_config());
        let mut custody = WholeOriginal(Some(Box::new(OriginalBankFixture {
            control, supervisor, client, input: None, owner: None, ready: None, exit: None,
            receipt: None, business: None, scope: None, originals: Vec::new(),
            retained_exits: Vec::new(), background: OriginalBackgroundSlots::default(), notification: None,
        })));
        let original = &mut *custody;
        let classes: &[WorkClass] = if drop_reply { &[WorkClass::Query] } else { &[
            WorkClass::Query, WorkClass::MaterializedView, WorkClass::TableMaintenance, WorkClass::Management,
        ] };
        for class in classes {
            let root = original.control.begin_root_request(WorkRequest::new(*class)).await;
            original.scope = Some(root.owner.scope());
            original.owner = Some(root.owner);
            original.business = Some(root.business);
            let scope = original.scope.as_ref().unwrap().clone();
            let signals = Arc::new(OriginalPrepareSignals::default());
            original.originals.push(signals.clone());
            original.input = Some(original.client.prepare_observed_start(
                completion_request(OriginalRefusalPort(signals.clone())), None,
            ).unwrap_or_else(|_| panic!("original Native-bank input")));
            original.client.start_observed_in(&mut original.input, &mut original.owner,
                &mut original.ready, &mut original.exit).unwrap().finish().unwrap();
            if drop_reply {
                // Current-thread execution has not yielded to the real root.
                // Only the original caller's response future is destroyed.
                assert!(!signals.entered.load(Ordering::SeqCst));
                drop(original.ready.take());
            }
            wait_preparation_exit_fact(&signals.entered).await.unwrap();
            assert_eq!(signals.calls.load(Ordering::SeqCst), 1);
            assert_eq!(scope.cancellation().unwrap().reason(), None);
            assert_eq!(original.control.snapshot().scopes.iter().find(|r| r.id == scope.id()).unwrap().class, *class);
            assert_eq!(signals.future_drops.load(Ordering::SeqCst), 0);
            signals.release.notify_one();
            if !drop_reply {
                let error = original.ready.take().unwrap().await.err().expect("same original typed refusal");
                assert_eq!(error.execution_failure(), Some(&fixture_execution_input(
                    "original finite preparation refusal after accepted-bank handoff",
                )));
                // The observer/footer keeps the same original record visible.
                assert_eq!(original.control.snapshot().scopes.iter().find(|r| r.id == scope.id()).unwrap().class, *class);
            }
            original.receipt = Some(tokio::time::timeout(Duration::from_secs(2),
                std::future::poll_fn(|cx| original.exit.as_mut().unwrap().poll_exit(cx)),
            ).await.expect("actual original role/session exit").unwrap());
            assert_eq!(original.receipt.as_ref().unwrap().local_disposition(),
                crate::api::LocalExecutionDisposition::ActualSourcesExited);
            assert!(original.receipt.as_ref().unwrap().view().native_preparation_exited());
            assert_eq!(signals.future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(signals.port_drops.load(Ordering::SeqCst), 1);
            assert_eq!(signals.calls.load(Ordering::SeqCst), 1);
            assert!(signals.execution.lock().unwrap().is_some());
            original.business.take().unwrap().release();
            drop(original.receipt.take());
            drop(original.exit.take());
            acknowledge_all_control(&original.control);
            tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await.unwrap();
            assert_eq!(original.control.snapshot().root_responsibilities, 0);
            original.scope.take();
        }
        original.supervisor.shutdown_until(Instant::now() + Duration::from_secs(2)).await.unwrap();
        acknowledge_all_control(&original.control);
        original.control.finish().await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn one_original_native_bank_keeps_each_of_four_original_work_classes_through_handoff() {
        assert_original_native_bank_class_and_reply(false).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn dropped_unpolled_accepted_reply_keeps_same_native_preparation_frame_and_actual_exit() {
        assert_original_native_bank_class_and_reply(true).await;
    }

    struct OriginalReplyWakeCause(Arc<AtomicU64>);
    impl Drop for OriginalReplyWakeCause {
        fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    struct OriginalReplyWake(Mutex<Option<Box<OriginalReplyWakeCause>>>);
    impl std::task::Wake for OriginalReplyWake {
        fn wake(self: Arc<Self>) { self.wake_by_ref(); }
        fn wake_by_ref(self: &Arc<Self>) {
            let original = { self.0.lock().unwrap().take() };
            if let Some(original) = original { std::panic::panic_any(original); }
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn original_async_reply_wake_keeps_raw_cause_while_same_bank_serves_successful_statement_root() {
        let control = governance_empty();
        let (supervisor, client) = test_supervisor(&control, Handle::current(),
            Arc::new(BindingNativePort), QueryProcessNamespace::new(0x9b57),
            FrontendProcessId::new_v7(), supervisor_config());
        let mut custody = WholeOriginal(Some(Box::new(OriginalBankFixture {
            control, supervisor, client, input: None, owner: None, ready: None, exit: None,
            receipt: None, business: None, scope: None, originals: Vec::new(),
            retained_exits: Vec::new(), background: OriginalBackgroundSlots::default(), notification: None,
        })));
        let original = &mut *custody;
        let root = original.control.begin_root().await;
        let first_scope = root.owner.scope();
        original.owner = Some(root.owner);
        original.business = Some(root.business);
        original.scope = Some(first_scope.clone());
        let signals = Arc::new(OriginalPrepareSignals::default());
        original.originals.push(signals.clone());
        original.input = Some(original.client.prepare_observed_start(
            completion_request(OriginalRefusalPort(signals.clone())), None,
        ).unwrap_or_else(|_| panic!("same first source")));
        original.client.start_observed_in(&mut original.input, &mut original.owner,
            &mut original.ready, &mut original.exit).unwrap().finish().unwrap();
        let raw_drops = Arc::new(AtomicU64::new(0));
        let wake = std::task::Waker::from(Arc::new(OriginalReplyWake(Mutex::new(Some(Box::new(
            OriginalReplyWakeCause(raw_drops.clone()),
        ))))));
        original.notification = Some(wake.clone());
        assert!(original.ready.as_mut().unwrap().as_mut().poll(
            &mut std::task::Context::from_waker(&wake),
        ).is_pending());
        wait_preparation_exit_fact(&signals.entered).await.unwrap();
        signals.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while original.exit.as_ref().unwrap().view().unwrap().role_result().is_none() {
                tokio::task::yield_now().await;
            }
        }).await.expect("the real bank must collect its original role completion");
        {
            let view = original.exit.as_ref().unwrap().view().unwrap();
            match view.role_result().unwrap() {
                PollCompletion::Panicked(packet) => assert!(packet.value().primary().is::<Box<OriginalReplyWakeCause>>()),
                _ => panic!("the original reply Waker payload was replaced"),
            }
        }
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        assert!(futures::poll!(std::future::poll_fn(|cx| original.exit.as_mut().unwrap().poll_exit(cx))).is_pending());
        let error = original.ready.take().unwrap().await.err().expect("same sent reply value");
        assert_eq!(error.execution_failure(), Some(&fixture_execution_input(
            "original finite preparation refusal after accepted-bank handoff",
        )));
        original.business.take().unwrap().release();
        original.retained_exits.push(original.exit.take().unwrap());
        assert!(!original.supervisor.service_has_exited());

        // A real successful BackgroundAction uses the SAME installed bank and
        // its existing statement Root path. It does not impersonate Native/AP
        // completion or supply an actor/Scope exit by hand.
        let sibling = original.control.begin_root().await;
        let sibling_scope = sibling.owner.scope();
        original.owner = Some(sibling.owner);
        original.business = Some(sibling.business);
        let (permit, window) = sibling_scope.admit_query_with_result(
            novarocks_workload_control::ResultWindowClass::Internal,
        ).unwrap().await.unwrap();
        original.background.permit = Some(permit);
        original.background.window = Some(window);
        original.background.context = Some(background_host_context(
            &sibling_scope, original.background.window.as_ref().unwrap(),
        ));
        let sibling_signals = Arc::new(BackgroundHostSignals::default());
        sibling_signals.release.store(true, Ordering::Release);
        original.background.action = Some(Box::new(BackgroundHostAction {
            signals: sibling_signals.clone(),
            residual: Some(Box::new(BackgroundHostResidual(sibling_signals.clone()))), error: None,
        }));
        original.client.prepare_background_action_in(&original.owner,
            &mut original.background.context, &mut original.background.permit,
            &mut original.background.payload, &mut original.background.action,
            &mut original.background.input).unwrap();
        original.client.start_background_action_in(&mut original.background.input,
            &mut original.owner, &mut original.background.ready, &mut original.exit,
        ).unwrap().finish().unwrap();
        original.background.receipt = Some(tokio::time::timeout(Duration::from_secs(2),
            original.background.ready.take().unwrap(),
        ).await.unwrap().unwrap());
        assert!(matches!(original.background.receipt.as_ref().unwrap().view().result(),
            crate::session_control::BackgroundActionResultView::Succeeded));
        assert_eq!(sibling_scope.cancellation().unwrap().reason(), None);
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        assert!(!original.supervisor.service_has_exited());
        original.background.receipt.take().unwrap().finish();
        original.receipt = Some(tokio::time::timeout(Duration::from_secs(2),
            std::future::poll_fn(|cx| original.exit.as_mut().unwrap().poll_exit(cx)),
        ).await.unwrap().unwrap());
        assert_eq!(original.receipt.as_ref().unwrap().local_disposition(),
            crate::api::LocalExecutionDisposition::ActualSourcesExited);
        assert_eq!(sibling_signals.action_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(sibling_signals.residual_dropped.load(Ordering::SeqCst), 1);
        original.business.take().unwrap().release();
        drop(original.receipt.take());
        drop(original.exit.take());
        drop(original.background.window.take());
        acknowledge_all_control(&original.control);
        tokio::time::timeout(Duration::from_secs(2), sibling_scope.wait_released()).await.unwrap();
        assert!(futures::poll!(Box::pin(first_scope.wait_released())).is_pending());
        let shutdown = original.supervisor.shutdown_until(Instant::now() + Duration::from_secs(2)).await;
        assert!(matches!(shutdown, Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))));
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        original.retained_exits.clear();
        original.supervisor.abandon_for_process_exit();
        assert_eq!(raw_drops.load(Ordering::SeqCst), 1);
        acknowledge_all_control(&original.control);
        tokio::time::timeout(Duration::from_secs(2), first_scope.wait_released()).await.unwrap();
        original.scope.take();
        // Both original statement Scopes and all foreign work exited above.
        // Drop ordinary bank/client aliases before asking the owning WC test
        // authority to prove its separate process drain (the fixture contract).
        // The residual registered the real bank role Waker in these signals.
        // Its fixture observation alias must exit too before slot backing can
        // reclaim; Scope release alone does not prove the Waker has dropped.
        drop((wake, signals, sibling_signals, first_scope, sibling_scope));
        let OriginalBankFixture { control, supervisor, client, input, owner,
            ready, exit, receipt, business, scope, originals, retained_exits,
            background, notification } = *custody.0.take().unwrap();
        drop((supervisor, client, input, owner, ready, exit, receipt, business,
            scope, originals, retained_exits, background, notification));
        let mut authority = WholeOriginal(Some(Box::new(control)));
        authority.finish().await;
    }
}
