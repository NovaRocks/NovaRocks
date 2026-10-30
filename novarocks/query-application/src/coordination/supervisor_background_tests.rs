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

// Included in the original supervisor fixture; exercises its real fixed bank.
use std::task::Context;
#[derive(Default)]
struct BackgroundHostSignals {
    release: AtomicBool,
    action_dropped: AtomicU64,
    residual_dropped: AtomicU64,
    residual_wake: futures::task::AtomicWaker,
}
struct BackgroundHostResidual(Arc<BackgroundHostSignals>);
impl crate::session_control::GovernedStatementResidual for BackgroundHostResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        self.0.residual_wake.register(cx.waker());
        if self.0.release.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for BackgroundHostResidual {
    fn drop(&mut self) {
        self.0.residual_dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct BackgroundHostAction {
    signals: Arc<BackgroundHostSignals>,
    residual: Option<Box<dyn crate::session_control::GovernedStatementResidual>>,
    error: Option<crate::session_error::QueryServiceError>,
}
impl crate::session_control::GovernedBackgroundAction for BackgroundHostAction {
    fn poll_action(
        &mut self,
        owner: &mut crate::session_control::GovernedActionContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::session_error::QueryServiceError>> {
        assert!(owner.cancellation().is_for_scope(owner.scope()));
        assert!(owner.execution_owner_slot().is_some());
        assert_eq!(
            owner.seal_success_visibility(),
            crate::session_control::GovernedStatementVisibilitySealOutcome::Stale,
            "the original Background Root is not a SQL visibility authority"
        );
        Poll::Ready(self.error.take().map_or(Ok(()), Err))
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn crate::session_control::GovernedStatementResidual>>,
    ) -> Result<(), crate::api::StatementNativeHealthFailure> {
        assert!(target.is_none());
        *target = self.residual.take();
        Ok(())
    }
}
impl Drop for BackgroundHostAction {
    fn drop(&mut self) {
        self.signals.action_dropped.fetch_add(1, Ordering::SeqCst);
    }
}
fn background_host_context(
    scope: &novarocks_workload_control::WorkScope,
    window: &novarocks_workload_control::ResultWindowGrant,
) -> crate::admitted_query_context::QueryExecutionContext {
    crate::admitted_query_context::QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        crate::api::BackendTopologySnapshot::try_new(1, Vec::new()).unwrap(),
        None,
        crate::cancellation::QueryCancellationView::governed(scope.cancellation().unwrap(), None),
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .with_result_capacity(
        crate::admitted_query_context::QueryResultCapacityBinding::try_new(
            scope,
            window.retain_alias(),
        )
        .unwrap(),
    )
    .unwrap()
}
#[tokio::test]
async fn background_ready_retains_same_root_until_actual_residual_and_bank_footer() {
    for failed in [false, true] {
        let control = governance_with_query_limit(false, Some(1));
        let root = control.begin_root().await;
        let mut control = {
            let scope = root.owner.scope();
            let (permit, window) = tokio::time::timeout(
                Duration::from_secs(2),
                scope
                    .admit_query_with_result(
                        novarocks_workload_control::ResultWindowClass::Internal,
                    )
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(control.snapshot().admitted_queries, 1);
            let mut context = Some(background_host_context(&scope, &window));
            let mut permit = Some(permit);
            let mut payload = None; // Existing admitted jobs may bootstrap without a Root payload.
            let signals = Arc::new(BackgroundHostSignals::default());
            let mut action: Option<Box<dyn crate::session_control::GovernedBackgroundAction>> =
                Some(Box::new(BackgroundHostAction {
                    signals: Arc::clone(&signals),
                    residual: Some(Box::new(BackgroundHostResidual(Arc::clone(&signals)))),
                    error: failed.then(|| {
                        crate::session_error::QueryServiceError::new(
                            crate::session_error::QueryServiceErrorKind::Internal,
                            "original background failure",
                        )
                    }),
                }));
            let (mut bank, _shutdown, mut registry, client) =
                unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
            let mut owner = Some(root.owner);
            let mut input = None;
            client
                .prepare_background_action_in(
                    &owner,
                    &mut context,
                    &mut permit,
                    &mut payload,
                    &mut action,
                    &mut input,
                )
                .unwrap();
            assert!(owner.is_some() && context.is_none() && permit.is_none() && action.is_none());
            let mut ready = None;
            let mut exit = None;
            let tail = client
                .start_background_action_in(&mut input, &mut owner, &mut ready, &mut exit)
                .unwrap();
            assert!(tail.result().is_ok());
            assert!(input.is_none() && owner.is_none() && ready.is_some() && exit.is_some());
            tail.finish().unwrap();
            assert!(bank.progress_turn(NonZeroUsize::new(7).unwrap()).is_none());
            assert_eq!(signals.action_dropped.load(Ordering::SeqCst), 1);
            let receipt = tokio::time::timeout(Duration::from_secs(2), ready.take().unwrap())
                .await
                .unwrap()
                .unwrap();
            let message_ptr = {
                let guard = receipt.view();
                match guard.result() {
                    crate::session_control::BackgroundActionResultView::Succeeded => {
                        assert!(!failed);
                        None
                    }
                    crate::session_control::BackgroundActionResultView::Failed(error) => {
                        assert!(failed);
                        assert_eq!(error.message(), "original background failure");
                        Some(error.message().as_ptr())
                    }
                }
            };
            let mut exit = exit.take().unwrap();
            let mut wait = Box::pin(std::future::poll_fn(|cx| exit.poll_exit(cx)));
            assert!(futures::poll!(&mut wait).is_pending());
            drop(wait);
            assert!(
                scope.check().is_ok(),
                "business Ready does not release its original Root"
            );
            assert_eq!(signals.residual_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(
                control.snapshot().admitted_queries,
                0,
                "business Ready releases query concurrency while the same GC is pending"
            );
            let next = control.begin_root().await;
            let next_query = tokio::time::timeout(
                Duration::from_secs(2),
                next.owner.scope().admit_query().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(control.snapshot().admitted_queries, 1);
            next_query.release();
            next.owner.complete();
            next.business.release();
            assert!(bank.progress_turn(NonZeroUsize::new(7).unwrap()).is_none());
            assert_eq!(signals.residual_dropped.load(Ordering::SeqCst), 0);
            signals.release.store(true, Ordering::Release);
            signals.residual_wake.wake();
            let completion = bank
                .progress_turn(NonZeroUsize::new(7).unwrap())
                .expect("actual residual exit wakes the SAME original role");
            assert_eq!(signals.residual_dropped.load(Ordering::SeqCst), 1);
            assert!(
                matches!(completion.completion(), PollCompletion::Ready(result) if result.value().is_ok()),
                "ordinary background business errors are not role supervision failures"
            );
            let mut before_footer = Box::pin(std::future::poll_fn(|cx| exit.poll_exit(cx)));
            assert!(
                futures::poll!(&mut before_footer).is_pending(),
                "core/residual Ready alone never substitutes for the original bank footer"
            );
            drop(before_footer);
            assert!(
                scope.check().is_ok(),
                "same Root remains stable through bank reception"
            );
            let mut supervisor_failure = None;
            observe_logical_completion(
                &registry.handle(),
                &mut bank,
                completion,
                &mut supervisor_failure,
            );
            assert!(
                supervisor_failure.is_none(),
                "the real completion receiver must not poison shutdown for a business Err"
            );
            let actual = tokio::time::timeout(
                Duration::from_secs(2),
                std::future::poll_fn(|cx| exit.poll_exit(cx)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                actual.view().actor_phase(),
                crate::api::LogicalActorExitPhase::Unreserved
            );
            assert_eq!(
                actual.local_disposition(),
                crate::api::LocalExecutionDisposition::ActualSourcesExited
            );
            if let Some(pointer) = message_ptr {
                let original = actual.background_result().unwrap();
                let crate::session_control::BackgroundActionResultView::Failed(error) =
                    original.result()
                else {
                    panic!("the same canonical failure survives actual bank completion")
                };
                assert_eq!(error.message().as_ptr(), pointer, "no variable cause clone");
            }
            receipt.finish();
            root.business.release();
            drop((actual, exit, window, scope, client, bank));
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }
}
#[tokio::test]
async fn background_preparation_and_mount_refuse_without_consuming_original_slots() {
    let (control, root) = governance().await;
    let mut control = {
        let scope = root.owner.scope();
        let (query, window) = tokio::time::timeout(
            Duration::from_secs(2),
            scope
                .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut context = Some(background_host_context(&scope, &window));
        let mut query = Some(query);
        let mut payload = None;
        let signals = Arc::new(BackgroundHostSignals::default());
        let mut action: Option<Box<dyn crate::session_control::GovernedBackgroundAction>> =
            Some(Box::new(BackgroundHostAction {
                signals: Arc::clone(&signals),
                residual: None,
                error: None,
            }));
        let (mut bank, _shutdown, mut registry, client) =
            unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
        let mut owner = Some(root.owner);
        let mut input = None;
        // A foreign Root cannot borrow this job's actual query permit/window/cancel view.
        let foreign = control.begin_root().await;
        let mut foreign_owner = Some(foreign.owner);
        assert!(
            client
                .prepare_background_action_in(
                    &foreign_owner,
                    &mut context,
                    &mut query,
                    &mut payload,
                    &mut action,
                    &mut input
                )
                .is_err()
        );
        foreign_owner.take().unwrap().complete();
        foreign.business.release();
        assert!(
            owner.is_some()
                && context.is_some()
                && query.is_some()
                && action.is_some()
                && input.is_none()
        );
        client
            .prepare_background_action_in(
                &owner,
                &mut context,
                &mut query,
                &mut payload,
                &mut action,
                &mut input,
            )
            .unwrap();
        let mut ready = None;
        let mut exit = None;
        client
            .start_background_action_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        // Occupied destination refuses a second start without consuming either receiver.
        assert!(
            client
                .start_background_action_in(&mut input, &mut owner, &mut ready, &mut exit)
                .is_err()
        );
        assert!(ready.is_some() && exit.is_some());
        let completion = bank
            .progress_turn(NonZeroUsize::new(7).unwrap())
            .expect("no residual: actual bank exit does not wait for business observation");
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        tokio::time::timeout(Duration::from_secs(2), ready.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .finish();
        let mut exit = exit.take().unwrap();
        let actual = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| exit.poll_exit(cx)),
        )
        .await
        .unwrap()
        .unwrap();
        root.business.release();
        drop((actual, exit, window, scope, client, bank));
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}
