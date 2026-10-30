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

//! Product uses its required observation effect; no absent-context fallback.
use super::*;
use crate::gc_observation::{GcOwnedRefObservationErrorKind, GcOwnedRefObservationInError};
use novarocks_state_store_api::{
    StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionContext,
    StateStoreExecutionFailure, StateStoreExecutionPort, StateStoreOriginalUnsupported,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct NoPhysicalPort;
impl StateStoreExecutionPort for NoPhysicalPort {
    fn bind_original_in(
        &self,
        _: &Arc<dyn StateStoreBlockingOriginal>,
        _: Instant,
        _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        panic!("unsupported original provider cannot publish physical work")
    }
    fn request_progress(&self) {
        panic!("unsupported original installed no source")
    }
}
struct OriginalGcEffects {
    context: Option<StateStoreExecutionContext>,
    candidates: Vec<CleanupCandidate>,
    received: Mutex<Option<GcOwnedRefObservationInError>>,
    observed: AtomicUsize,
    selected: AtomicUsize,
    executed: AtomicUsize,
}
struct OriginalGcSession<'a>(&'a OriginalGcEffects);
impl CleanupSession for OriginalGcSession<'_> {
    fn candidates(&self) -> &[CleanupCandidate] {
        &self.0.candidates
    }
    fn select_owned_refs<'a>(
        &'a mut self,
        _: &'a [usize],
    ) -> MaintenanceFuture<'a, Result<(), TerminalError>> {
        Box::pin(async move {
            self.0.selected.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn execute<'a>(&'a mut self) -> MaintenanceFuture<'a, Result<CleanupTerminal, TerminalError>> {
        Box::pin(async move {
            self.0.executed.fetch_add(1, Ordering::SeqCst);
            Ok(CleanupTerminal::NoOp {
                locations: Vec::new(),
            })
        })
    }
}
impl TableMaintenanceEffectPort for OriginalGcEffects {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        observations: &'a GcOwnedRefObservationAccelerator,
        observation: GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
        Box::pin(async move {
            self.observed.fetch_add(1, Ordering::SeqCst);
            match observations
                .observe_in(
                    self.context
                        .as_ref()
                        .expect("no-context NoOp must not observe"),
                    observation,
                    now_ms,
                    safe_gc_age_ms,
                )
                .await
            {
                Ok(decision) => Ok(decision),
                Err(error) => {
                    let cause = error
                        .original_failure()
                        .expect("actual unsupported original cause")
                        .clone();
                    *self.received.lock().unwrap() = Some(error);
                    Err(
                        TerminalError::pre_dispatch_failed("original GC observation refused")
                            .with_failure_cause(Some(cause)),
                    )
                }
            }
        })
    }
    fn reject_user_action_on_mv<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
    ) -> MaintenanceFuture<'a, Result<(), TerminalError>> {
        Box::pin(async { Ok(()) })
    }
    fn execute_metadata<'a>(
        &'a self,
        _: MaintenanceActionRequest,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>> {
        Box::pin(async { panic!("owned-ref cleanup cannot dispatch metadata") })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
        _: RewriteIntent,
    ) -> MaintenanceFuture<'a, Result<Box<dyn DistributedRewriteSession + 'a>, String>> {
        Box::pin(async { panic!("owned-ref cleanup cannot begin rewrite") })
    }
    fn begin_cleanup<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
        _: i64,
    ) -> MaintenanceFuture<'a, Result<Box<dyn CleanupSession + 'a>, TerminalError>> {
        Box::pin(
            async move { Ok(Box::new(OriginalGcSession(self)) as Box<dyn CleanupSession + 'a>) },
        )
    }
}
#[tokio::test]
async fn owned_ref_cleanup_requires_original_observation_and_never_falls_back_to_legacy() {
    use novarocks_failure_contract::FailureProjection;
    let store = Arc::new(InMemoryStateStore::new("product-original-gc-no-fallback"));
    let observations = Arc::new(
        GcOwnedRefObservationAccelerator::open(store, StateStoreRunPolicy::default())
            .await
            .unwrap(),
    );
    // Negative unsupported-provider fixture only: this context claims no
    // positive dispatch, Root grant, or physical-exit proof.
    let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
    let effects = OriginalGcEffects {
        context: Some(StateStoreExecutionContext::new(
            &port,
            Instant::now() + Duration::from_secs(5),
        )),
        candidates: vec![CleanupCandidate::OwnedRef(CleanupOwnedRefFact {
            table_uuid: Uuid::from_u128(71),
            ref_name: "original-ref".into(),
            head_snapshot_id: 1,
            provenance_version: 1,
            provenance_digest: [7; 32],
        })],
        received: Mutex::new(None),
        observed: AtomicUsize::new(0),
        selected: AtomicUsize::new(0),
        executed: AtomicUsize::new(0),
    };
    let product = TableMaintenanceProduct::new(Some(observations.clone()))
        .with_safe_gc_age(Duration::from_millis(1));
    let terminal = product
        .execute_user_action(
            &effects,
            MaintenanceActionRequest::RemoveOrphanFiles {
                target: MaintenanceTarget {
                    catalog: "ice".into(),
                    namespace: "db".into(),
                    table: "orders".into(),
                },
                older_than_ms: 1,
            },
        )
        .await
        .unwrap_err();
    {
        let received = effects.received.lock().unwrap();
        let received = received
            .as_ref()
            .expect("same full original error receiver");
        let cause = received.original_failure().unwrap();
        assert_eq!(received.kind(), GcOwnedRefObservationErrorKind::Store);
        assert!(
            cause
                .downcast_ref::<StateStoreOriginalUnsupported>()
                .is_some()
        );
        assert_eq!(terminal.failure_cause.as_ref(), Some(cause));
        assert_eq!(terminal.selected_failure_fact(), Some(cause.failure_fact()));
        assert_eq!(effects.observed.load(Ordering::SeqCst), 1);
        assert_eq!(
            effects.selected.load(Ordering::SeqCst),
            0,
            "no maturity means no selection authority"
        );
        assert_eq!(
            effects.executed.load(Ordering::SeqCst),
            0,
            "no original proof means no deletion"
        );
    }
    let observation = GcOwnedRefObservation::try_new(
        Uuid::from_u128(71),
        "original-ref".into(),
        1,
        1,
        [7; 32],
        2000,
    )
    .unwrap();
    assert_eq!(
        observations.observe(observation, 2000, 50).await.unwrap(),
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 2000
        },
        "only the explicit legacy probe establishes its own first record"
    );
}

#[tokio::test]
async fn cleanup_noop_needs_no_original_observation_capability() {
    let effects = OriginalGcEffects {
        context: None,
        candidates: Vec::new(),
        received: Mutex::new(None),
        observed: AtomicUsize::new(0),
        selected: AtomicUsize::new(0),
        executed: AtomicUsize::new(0),
    };
    let result = TableMaintenanceProduct::new(None)
        .with_safe_gc_age(Duration::from_millis(1))
        .execute_user_action(
            &effects,
            MaintenanceActionRequest::RemoveOrphanFiles {
                target: MaintenanceTarget {
                    catalog: "ice".into(),
                    namespace: "db".into(),
                    table: "empty".into(),
                },
                older_than_ms: 1,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(result, MaintenanceActionOutcome::RemoveOrphanFiles {
        orphan_file_locations
    } if orphan_file_locations.is_empty())
    );
    assert_eq!(effects.observed.load(Ordering::SeqCst), 0);
    assert_eq!(effects.selected.load(Ordering::SeqCst), 0);
    assert_eq!(effects.executed.load(Ordering::SeqCst), 1);
    assert!(effects.received.lock().unwrap().is_none());
}
