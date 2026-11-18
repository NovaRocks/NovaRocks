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

use super::*;
use crate::api::logical_runtime::LogicalExit;
use novarocks_types::{QueryId, identity::AttemptId};

#[test]
fn abandoning_the_unique_supervision_observer_releases_its_original_waker_captures() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CapturedWake {
        _original: Arc<OriginalLogicalExit>,
        dropped: Arc<AtomicUsize>,
        wakes: Arc<AtomicUsize>,
    }
    impl std::task::Wake for CapturedWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for CapturedWake {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let source = OriginalLogicalExit::new();
    let weak = Arc::downgrade(&source);
    let drops = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let mut observer = LogicalExecutionExitObserver::new(source.clone());
    let wake = std::task::Waker::from(Arc::new(CapturedWake {
        _original: source.clone(),
        dropped: drops.clone(),
        wakes: wakes.clone(),
    }));
    assert!(
        observer
            .poll_owner_failure(&mut Context::from_waker(&wake))
            .is_none()
    );
    drop(wake);
    drop(source);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(observer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    assert!(
        weak.upgrade().is_none(),
        "original Source is not held by a waker cycle"
    );
}

#[test]
fn isolated_failure_wake_is_independent_of_physical_exit_wait_and_read_only_weak_custody() {
    use novarocks_execution_contract::failure::FailureProjection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let source = OriginalLogicalExit::new();
    let mut original = LogicalExecutionExitObserver::new(source.clone());
    let observation = original.owner_failure_observation().unwrap();
    let failure_wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let exit_wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let fw = std::task::Waker::from(failure_wake.clone());
    let ew = std::task::Waker::from(exit_wake.clone());
    assert!(
        original
            .poll_owner_failure(&mut Context::from_waker(&fw))
            .is_none()
    );
    assert!(
        original
            .poll_exit(&mut Context::from_waker(&ew))
            .is_pending()
    );
    let error =
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, "same original owner cause");
    assert!(source.retain_owner_failure_silent(error.clone()));
    source.notify_owner_failure();
    source.notify();
    assert_eq!(failure_wake.0.load(Ordering::SeqCst), 1);
    assert_eq!(exit_wake.0.load(Ordering::SeqCst), 1);
    assert_eq!(observation.failure(), Ok(Some(error.clone())));
    assert!(
        !source.retain_owner_failure_silent(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "later cause"
        ))
    );
    assert_eq!(
        original.poll_owner_failure(&mut Context::from_waker(&fw)),
        Some(error.clone())
    );
    assert!(
        original
            .poll_exit(&mut Context::from_waker(&ew))
            .is_pending(),
        "typed failure is not actual exit"
    );
    assert!(!source.has_unhandled_failure());
    drop(original);
    assert_eq!(observation.failure(), Ok(Some(error)));
    drop(source);
    let missing = observation.failure().unwrap_err();
    assert_eq!(
        missing.execution_failure().unwrap().site(),
        crate::execution_error::ExecutionFailureSite::Coordinator
    );
    assert_eq!(
        missing.execution_failure().unwrap().fact(),
        crate::execution_error::QueryFailureReason::ResultProtocol.failure_fact()
    );
}

#[test]
fn runtime_failure_retains_original_typed_cause_without_forging_join_error() {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 1,
        initial: QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.runtime_installed_silent(association);
    assert!(!source.pending_actor());
    // This fixture reports the install-only role's actual exit separately.
    // Pending actor custody is a legacy role condition, not the Installed phase.
    source.role_ended_silent(false);
    assert!(!source.pending_actor());
    assert_eq!(source.lock().phase, LogicalActorExitPhase::Installed);
    assert!(source.lock().runtime.is_none());
    assert!(!source.has_failure());
    let error = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "original runtime cause");
    source.runtime_joined_silent(association, LogicalExit::OwnerFailed(error.clone()));
    let view = LogicalExecutionExitView {
        state: source.lock(),
    };
    assert_eq!(view.actor_phase(), LogicalActorExitPhase::Joined);
    assert!(view.role_ended());
    assert_eq!(
        view.runtime_actor_result(),
        Some(&LogicalExit::OwnerFailed(error))
    );
    drop(view);
    assert!(!source.pending_actor());
    assert!(source.has_failure());
    assert!(!source.has_unhandled_failure());
}

#[test]
fn runtime_completion_is_distinct_from_role_and_native_actual_exit() {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 1,
        initial: QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.runtime_installed_silent(association);
    // Model the original pre-actor body still retained by its source.
    source.lock().native_body_exited = false;
    source.runtime_joined_silent(association, LogicalExit::Completed);
    let state = source.lock();
    assert!(!state.native_body_exited);
    assert!(!state.role_ended);
    assert!(state.role.is_none());
    drop(state);
    assert!(!source.has_failure());
}

#[test]
fn isolated_owner_failure_keeps_original_verdict_without_process_health_or_invented_exit() {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 3,
        initial: QueryExecutionId::new(QueryId::new(5, 7), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.runtime_installed_silent(association);
    let error = QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "original logical owner cause",
    );
    source.retain_owner_failure_silent(error.clone());
    source.retain_owner_failure_silent(QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "later cause",
    ));
    assert_eq!(source.owner_failure(), Some(error));
    assert!(source.has_failure());
    assert!(!source.has_unhandled_failure());
    assert!(
        !source.run_failed(),
        "a typed owner failure is not a fabricated Native panic"
    );
    let state = source.lock();
    assert_eq!(state.phase, LogicalActorExitPhase::Installed);
    assert!(state.runtime.is_none() && state.role.is_none());
    assert!(state.run_panic.is_none() && state.run_cleanup_panic.is_none());
    drop(state);
    assert!(
        source.disposition().is_none(),
        "an isolated owner verdict never proves physical closure"
    );
    source.runtime_joined_silent(
        association,
        crate::api::logical_runtime::LogicalExit::OwnerFailed(source.owner_failure().unwrap()),
    );
    assert!(source.has_failure());
    assert!(!source.has_unhandled_failure());
    assert!(
        source.disposition().is_none(),
        "the original role/body must also exit"
    );
}

#[test]
fn install_only_role_completion_keeps_nonempty_native_body_until_actual_runtime_join() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Consumer(Arc<AtomicUsize>);
    impl Drop for Consumer {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl crate::api::InternalComputationConsumer for Consumer {
        fn begin_local_close(&self) {}
        fn poll_local_closed(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
            std::task::Poll::Ready(Ok(())) // This fixture issues no local work.
        }
        fn consume(
            &self,
            _: crate::api::InternalConsumerActivation,
        ) -> crate::api::InternalConsumerFuture {
            panic!("no consumer computation is issued by this body-lifetime fixture");
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let source = OriginalLogicalExit::new();
    source.native.try_lock().unwrap().consumer = Some(Arc::new(Consumer(drops.clone())));
    source.lock().native_body_exited = false; // Initial fixture now owns an actual nonempty body.
    let association = OriginalAssociation {
        registry: 1,
        initial: QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.runtime_installed_silent(association);
    source.role_ended_silent(false);
    source.retire_native_body_silent();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(source.native.try_lock().unwrap().consumer.is_some());
    assert!(!source.lock().native_body_exited);
    assert!(
        !source.pending_actor(),
        "a runtime installation is not legacy role-owned actor cleanup"
    );
    // The model's independent original actor author now reports its exit.
    // This is a source-state unit oracle, not native/HostExit evidence.
    source.runtime_joined_silent(association, LogicalExit::Completed);
    source.retire_native_body_silent();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(source.lock().native_body_exited);
    source.retire_native_body_silent();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct BodyDropCause;
#[derive(Debug)]
struct BodyConsumerProbe {
    source: std::sync::Weak<OriginalLogicalExit>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
    state_unlocked: Arc<std::sync::atomic::AtomicBool>,
    panics: bool,
}
impl Drop for BodyConsumerProbe {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.state_unlocked.store(
            self.source
                .upgrade()
                .is_some_and(|source| source.state.try_lock().is_ok()),
            Ordering::SeqCst,
        );
        if self.panics {
            std::panic::panic_any(BodyDropCause);
        }
    }
}
impl crate::api::InternalComputationConsumer for BodyConsumerProbe {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        Poll::Ready(Ok(())) // This unit fixture never issues consumer work.
    }
    fn consume(
        &self,
        _: crate::api::InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        panic!("body-close unit fixture must not issue a consumer future");
    }
}
fn original_body_fixture(
    panics: bool,
) -> (
    Arc<OriginalLogicalExit>,
    OriginalAssociation,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicBool>,
) {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 9,
        initial: QueryExecutionId::new(QueryId::new(21, 22), AttemptId::new(1).unwrap()).unwrap(),
        generation: 3,
    };
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    source.native.try_lock().unwrap().consumer = Some(Arc::new(BodyConsumerProbe {
        source: Arc::downgrade(&source),
        drops: drops.clone(),
        state_unlocked: unlocked.clone(),
        panics,
    }));
    // This source-state unit owns one real nonempty body and no Session,
    // Native task, AP child or consumer future. It does not model HostExit.
    source.lock().native_body_exited = false;
    source.reserve_silent(association);
    source.runtime_installed_silent(association);
    source.native_pre_actor_started_silent();
    (source, association, drops, unlocked)
}

#[tokio::test]
async fn explicit_body_close_records_actual_drop_without_actor_join_or_health_rewrite() {
    use std::sync::atomic::Ordering;
    let (source, association, drops, unlocked) = original_body_fixture(false);
    let owner_error = QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "same original owner failure",
    );
    source.retain_owner_failure_silent(owner_error.clone());
    source.role_ended_silent(false); // Independent original role-frame unit author.
    source.lock().native_failure = Some(Box::new("earlier Native cause"));
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |body| {
                assert!(body.consumer.is_some());
                Ok(())
            },)
            .await,
        Ok(())
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        unlocked.load(Ordering::SeqCst),
        "real destructor runs outside Source state lock"
    );
    {
        let state = source.lock();
        assert!(state.native_body_exited && !state.native_pre_actor_started);
        assert_eq!(state.phase, LogicalActorExitPhase::Installed);
        assert!(state.runtime.is_none() && state.role.is_none());
        assert!(state.role_ended && !state.role_failed);
        assert_eq!(state.owner_failure.as_ref(), Some(&owner_error));
        assert_eq!(
            state
                .native_failure
                .as_ref()
                .unwrap()
                .downcast_ref::<&str>(),
            Some(&"earlier Native cause")
        );
    }
    assert!(source.has_unhandled_failure() && source.disposition().is_none());
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| panic!(
                "completed original body must not be qualified/dropped twice"
            ),)
            .await,
        Ok(())
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn body_drop_panic_is_sticky_and_preserves_the_earlier_native_cause() {
    use std::sync::atomic::Ordering;
    let (source, association, drops, unlocked) = original_body_fixture(true);
    source.role_ended_silent(false);
    source.retain_owner_failure_silent(QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "owner failed",
    ));
    source.lock().native_failure = Some(Box::new("earlier Native cause"));
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| Ok(()),)
            .await,
        Err(OriginalNativeBodyCloseRefusal::BodyDropPanicked)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(unlocked.load(Ordering::SeqCst));
    let body = source.native.lock().await;
    assert!(
        body.exiting && body.consumer.is_none(),
        "an empty field is not a Drop receipt"
    );
    drop(body);
    {
        let state = source.lock();
        assert!(!state.native_body_exited && state.native_pre_actor_started);
        assert!(
            state
                .native_body_cleanup_panic
                .as_ref()
                .unwrap()
                .is::<BodyDropCause>()
        );
        assert_eq!(
            state
                .native_failure
                .as_ref()
                .unwrap()
                .downcast_ref::<&str>(),
            Some(&"earlier Native cause")
        );
        assert_eq!(state.phase, LogicalActorExitPhase::Installed);
        assert!(state.runtime.is_none());
    }
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| panic!(
                "unproven destruction must not retry"
            ),)
            .await,
        Err(OriginalNativeBodyCloseRefusal::BodyUnproven)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(source.has_unhandled_failure() && source.disposition().is_none());
}

#[tokio::test]
async fn body_close_rejects_foreign_body_and_missing_original_owner_failure() {
    use std::sync::atomic::Ordering;
    let (source, association, drops, _) = original_body_fixture(false);
    source.role_ended_silent(false);
    let foreign = OriginalLogicalExit::new();
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(
                &foreign.native,
                association.initial,
                |_| panic!("foreign original"),
            )
            .await,
        Err(OriginalNativeBodyCloseRefusal::ForeignSource)
    );
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| panic!(
                "not a failure cleanup"
            ),)
            .await,
        Err(OriginalNativeBodyCloseRefusal::NotRuntimeFailure)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(source.native.lock().await.consumer.is_some());
    assert!(!source.lock().native_body_exited);
}

#[tokio::test]
async fn body_close_keeps_original_fields_until_local_exit_qualification_succeeds() {
    use std::sync::atomic::Ordering;
    let (source, association, drops, _) = original_body_fixture(false);
    source.retain_owner_failure_silent(QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "owner failed",
    ));
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| panic!(
                "role still owns its frame"
            ),)
            .await,
        Err(OriginalNativeBodyCloseRefusal::LocalWorkPending)
    );
    source.role_ended_silent(false);
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| Err(
                OriginalNativeBodyCloseRefusal::LocalWorkPending
            ),)
            .await,
        Err(OriginalNativeBodyCloseRefusal::LocalWorkPending)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!source.native.lock().await.exiting);
    assert!(!source.lock().native_body_exited);
    assert_eq!(
        source
            .close_runtime_native_body_for_cleanup(&source.native, association.initial, |_| Ok(()),)
            .await,
        Ok(())
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
