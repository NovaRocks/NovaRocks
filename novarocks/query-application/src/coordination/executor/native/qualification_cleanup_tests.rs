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

//! Opaque qualification calls use the actual executor and move-only wrappers.
//! These tests do not certify an AP Scope exit or Native attempt resource exit.

use super::*;
use crate::coordination::replacement::{
    ActiveAttemptIsolationOwner, AttemptIsolationActivationFailure, AttemptIsolationReservation,
    QualifiedWorkerAdmission,
};
use crate::coordination::{LogicalExecutionRuntimeRegistry, LogicalExecutionRuntimeReservation};
use novarocks_coordination_contract::{IdSeed, OperationIds};
use novarocks_execution_contract::ParticipationRef;
use novarocks_types::{AttemptId, BackendProcessId, QueryId};
use novarocks_workload_control::{
    ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Script {
    Success,
    ActivatePanic,
    ReservationAbandonPanic,
    ActiveAbandonPanic,
    FinishPanic,
}
struct Calls {
    native: Mutex<Weak<NativeExecutor>>,
    execution: QueryExecutionId,
    activates: AtomicUsize,
    reservation_abandons: AtomicUsize,
    active_abandons: AtomicUsize,
    finishes: AtomicUsize,
    reservation_drops: AtomicUsize,
    active_drops: AtomicUsize,
    reentries: AtomicUsize,
}
impl Calls {
    fn reenter(&self, activating: bool) {
        let native = self.native.lock().unwrap().upgrade().unwrap();
        let storage = native.storage.try_lock().unwrap_or_else(|_| {
            panic!("opaque qualification callback ran under NativeStorage lock")
        });
        let original = storage.qualifications.get(&self.execution).unwrap();
        if activating {
            assert!(original.activation_in_flight);
            assert!(!original.resources_retiring);
            assert!(original.qualified.is_none() && original.active.is_none());
        } else {
            assert!(original.resources_retiring);
            assert!(original.active.is_none());
        }
        self.reentries.fetch_add(1, Ordering::SeqCst);
    }
}
struct OpaqueReservation {
    identity: ReplacementQualificationIdentity,
    script: Script,
    calls: Arc<Calls>,
}
impl fmt::Debug for OpaqueReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueReservation")
            .field("script", &self.script)
            .finish()
    }
}
impl Drop for OpaqueReservation {
    fn drop(&mut self) {
        self.calls.reservation_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl AttemptIsolationReservation for OpaqueReservation {
    fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }
    fn topology_revision(&self) -> u64 {
        7
    }
    fn admissions(&self) -> &[QualifiedWorkerAdmission] {
        &[]
    }
    fn validate_binding(
        &self,
        old: &[ParticipationRef],
    ) -> Result<(), ReplacementQualificationFailure> {
        assert!(old.is_empty());
        Ok(())
    }
    fn activate(
        self: Box<Self>,
    ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure> {
        self.calls.activates.fetch_add(1, Ordering::SeqCst);
        self.calls.reenter(true);
        assert_ne!(
            self.script,
            Script::ActivatePanic,
            "original opaque activation panic"
        );
        Ok(Box::new(OpaqueActive {
            execution: self.identity.replacement(),
            script: self.script,
            calls: self.calls.clone(),
        }))
    }
    fn abandon(self: Box<Self>) {
        self.calls
            .reservation_abandons
            .fetch_add(1, Ordering::SeqCst);
        self.calls.reenter(false);
        assert_ne!(
            self.script,
            Script::ReservationAbandonPanic,
            "original opaque reservation abandon panic"
        );
    }
}
struct OpaqueActive {
    execution: QueryExecutionId,
    script: Script,
    calls: Arc<Calls>,
}
impl fmt::Debug for OpaqueActive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueActive")
            .field("script", &self.script)
            .finish()
    }
}
impl Drop for OpaqueActive {
    fn drop(&mut self) {
        self.calls.active_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl ActiveAttemptIsolationOwner for OpaqueActive {
    fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    fn finish(self: Box<Self>) {
        self.calls.finishes.fetch_add(1, Ordering::SeqCst);
        self.calls.reenter(false);
        assert_ne!(
            self.script,
            Script::FinishPanic,
            "original opaque finish panic"
        );
    }
    fn abandon(self: Box<Self>) {
        self.calls.active_abandons.fetch_add(1, Ordering::SeqCst);
        self.calls.reenter(false);
        assert_ne!(
            self.script,
            Script::ActiveAbandonPanic,
            "original opaque active abandon panic"
        );
    }
}
#[derive(Debug)]
struct Clock;
impl MonotonicClock for Clock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::ORIGIN
    }
}
fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(941, 942), AttemptId::new(attempt).unwrap()).unwrap()
}
fn schedule(execution: QueryExecutionId) -> Arc<AttemptSchedule> {
    let facts = crate::api::ExecutionSchedulingFacts {
        topological_fragment_order: vec![1],
        execution_anchor_fragment_id: 1,
        fragments: vec![crate::api::FragmentSchedulingFacts {
            fragment_id: 1,
            placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            scans: Vec::new(),
        }],
        edges: Vec::new(),
    };
    let process =
        BackendProcessId::try_from_bytes(OperationIds::new(IdSeed::new(941, 9)).issue().to_bytes())
            .unwrap();
    Arc::new(
        build_attempt_schedule(
            execution,
            FrontendProcessId::new_v7(),
            &[process],
            &facts,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap(),
    )
}
struct Fixture {
    native: Arc<NativeExecutor>,
    calls: Arc<Calls>,
    identity: QualificationIdentity,
    operation: TaskOperationId,
    registry: LogicalExecutionRuntimeRegistry,
    reservation: LogicalExecutionRuntimeReservation,
    source: Arc<OriginalLogicalExit>,
    work: RootWork,
    _control: WorkloadControl,
}
impl Fixture {
    fn new(script: Script) -> Self {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source = OriginalLogicalExit::new();
        let registry = LogicalExecutionRuntimeRegistry::new();
        let initial_schedule = schedule(execution(1));
        let mut reservation = registry
            .reserve_with_original_exit(
                execution(1),
                initial_schedule.participations().to_vec(),
                Some(source.clone()),
            )
            .unwrap();
        let registration = reservation.runtime_registration();
        let initial = AttemptActivationIdentity::from_parts(
            reservation.actor_instance().get(),
            execution(1),
            3,
        );
        let identity = QualificationIdentity {
            actor: initial.actor(),
            failed: execution(1),
            replacement: execution(2),
            generation: 4,
        };
        let operation = OperationIds::new(IdSeed::new(941, 12)).issue();
        let native = Arc::new(NativeExecutor::new(
            None,
            Arc::new(super::UnboundInstalledAttemptPort),
            source.native.clone(),
            Arc::downgrade(&source),
            registry.handle(),
            registration,
            initial,
            initial_schedule,
            FrontendProcessId::new_v7(),
            Arc::new(Clock),
            work.owner.scope().cancellation().unwrap(),
        ));
        let calls = Arc::new(Calls {
            native: Mutex::new(Arc::downgrade(&native)),
            execution: identity.replacement,
            activates: AtomicUsize::new(0),
            reservation_abandons: AtomicUsize::new(0),
            active_abandons: AtomicUsize::new(0),
            finishes: AtomicUsize::new(0),
            reservation_drops: AtomicUsize::new(0),
            active_drops: AtomicUsize::new(0),
            reentries: AtomicUsize::new(0),
        });
        let request = ReplacementQualificationRequest::from_logical(
            operation,
            ReplacementQualificationIdentity::new(
                identity.actor,
                identity.failed,
                identity.replacement,
                identity.generation,
            ),
            Arc::from([]),
            Arc::from([]),
            MonotonicInstant::from_origin(Duration::from_secs(20)),
            MonotonicInstant::from_origin(Duration::from_secs(25)),
            Instant::now() + Duration::from_secs(2),
            None,
        );
        let qualified = QualifiedReplacementReservation::try_new(
            &request,
            Box::new(OpaqueReservation {
                identity: request.identity(),
                script,
                calls: calls.clone(),
            }),
        )
        .unwrap();
        {
            let mut storage = native.lock();
            storage
                .preparations
                .insert(identity.replacement, PreparationPhase::Prepared);
            storage.prepared.insert(
                identity.replacement,
                PreparedAttempt {
                    schedule: schedule(identity.replacement),
                    preparation_deadline: None,
                    binding_complete: true,
                    activation_started: None,
                    terminal_cut: None,
                },
            );
            storage
                .ensure_qualification(initial, identity, Some(operation))
                .unwrap()
                .qualified = Some(qualified);
        }
        Self {
            native,
            calls,
            identity,
            operation,
            registry,
            reservation,
            source,
            work,
            _control: control,
        }
    }
    fn successor(&self) -> AttemptActivationIdentity {
        AttemptActivationIdentity::from_parts(
            self.identity.actor.get(),
            self.identity.replacement,
            self.identity.generation,
        )
    }
    async fn activate(&self) -> NativeFact {
        self.native
            .execute(NativeEffect::ActivateReservation {
                identity: self.identity,
                operation_id: self.operation,
            })
            .await
    }
    fn assert_phase(&self, activating: bool, retiring: bool, qualified: bool, active: bool) {
        let storage = self
            .native
            .storage
            .lock()
            .expect("foreign panic cannot poison original storage");
        let row = storage
            .qualifications
            .get(&self.identity.replacement)
            .unwrap();
        assert_eq!(row.identity, self.identity);
        assert_eq!(row.operation, Some(self.operation));
        assert_eq!(row.activation_in_flight, activating);
        assert_eq!(row.resources_retiring, retiring);
        assert_eq!(row.qualified.is_some(), qualified);
        assert_eq!(row.active.is_some(), active);
        assert!(!row.dispatch_settled, "no test-created dispatch completion");
        drop(storage);
        let inventory = self.native.cleanup_inventory().unwrap();
        let original = inventory
            .qualifications
            .iter()
            .find(|row| row.identity == self.identity)
            .unwrap();
        assert_eq!(original.operation, Some(self.operation));
        assert_eq!(original.activation_in_flight, activating);
        assert_eq!(original.resources_retiring, retiring);
        assert_eq!(original.qualified_owned, qualified);
        assert_eq!(original.active_owned, active);
        assert!(!original.dispatch_settled);
    }
    fn close(self) {
        let Self {
            native,
            calls,
            identity: _,
            operation: _,
            registry,
            reservation,
            source,
            work,
            _control: control,
        } = self;
        // These opaque scripts grant no actual Worker/resource capacity. Keep
        // their UNKNOWN row assertions separate from disposing this test owner.
        drop(native);
        assert!(calls.native.lock().unwrap().upgrade().is_none());
        drop(reservation);
        drop(source);
        registry
            .shutdown()
            .unwrap_or_else(|_| panic!("original uninstalled registry must be empty"));
        work.business.release();
        work.owner.complete();
        drop(control);
    }
}
fn original_panic_text(original: &(dyn Any + Send)) -> &str {
    original
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| original.downcast_ref::<&'static str>().copied())
        .expect("the original opaque panic payload")
}
fn assert_retained_panic(f: &Fixture, kind: u8, label: &str) {
    let causes = f.native.causes(f.identity.replacement, kind);
    let original = causes.primary.lock().unwrap();
    assert!(original_panic_text(original.as_ref().unwrap().as_ref()).contains(label));
}

#[tokio::test]
async fn opaque_activate_panic_retains_unknown_original_and_refuses_repeated_activation_or_abandon()
{
    let f = Fixture::new(Script::ActivatePanic);
    assert!(matches!(f.activate().await, NativeFact::Failed { .. }));
    f.assert_phase(true, false, true, false);
    assert_eq!(f.calls.activates.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.reentries.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.reservation_drops.load(Ordering::SeqCst), 1);
    assert!(f.native.causes(f.identity.replacement, 14).failed());
    assert_retained_panic(&f, 14, "original opaque activation panic");
    assert!(matches!(f.activate().await, NativeFact::Failed { .. }));
    for _ in 0..2 {
        assert!(matches!(
            f.native
                .execute(NativeEffect::AbandonReservation {
                    identity: f.identity
                })
                .await,
            NativeFact::Failed { .. }
        ));
        f.assert_phase(true, false, true, false);
    }
    assert_eq!(f.calls.activates.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.reservation_abandons.load(Ordering::SeqCst), 0);
    assert_eq!(f.calls.finishes.load(Ordering::SeqCst), 0);
    f.close();
}

#[tokio::test]
async fn opaque_reservation_abandon_panic_keeps_retiring_and_never_calls_same_original_twice() {
    let f = Fixture::new(Script::ReservationAbandonPanic);
    assert!(matches!(
        f.native
            .execute(NativeEffect::AbandonReservation {
                identity: f.identity
            })
            .await,
        NativeFact::Failed { .. }
    ));
    f.assert_phase(false, true, false, false);
    assert_eq!(f.calls.reservation_abandons.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.reservation_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.reentries.load(Ordering::SeqCst), 1);
    assert!(f.native.causes(f.identity.replacement, 16).failed());
    assert_retained_panic(&f, 16, "original opaque reservation abandon panic");
    assert!(matches!(
        f.native
            .execute(NativeEffect::AbandonReservation {
                identity: f.identity
            })
            .await,
        NativeFact::Failed { .. }
    ));
    assert!(matches!(f.activate().await, NativeFact::Failed { .. }));
    f.assert_phase(false, true, false, false);
    assert_eq!(f.calls.reservation_abandons.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.activates.load(Ordering::SeqCst), 0);
    f.close();
}

async fn active_retirement_panic(script: Script, finish: bool) {
    let f = Fixture::new(script);
    assert!(matches!(
        f.activate().await,
        NativeFact::ReservationActivated { .. }
    ));
    f.assert_phase(false, false, true, true);
    let original = catch_unwind(AssertUnwindSafe(|| {
        f.native
            .retire_isolation(f.successor(), finish, NativeFact::Applied)
    }));
    assert!(original.is_err(), "actual consuming opaque call panics");
    assert!(
        original_panic_text(original.as_ref().err().unwrap().as_ref()).contains(if finish {
            "original opaque finish panic"
        } else {
            "original opaque active abandon panic"
        })
    );
    f.assert_phase(false, true, true, false);
    assert_eq!(f.calls.reentries.load(Ordering::SeqCst), 2);
    assert_eq!(f.calls.finishes.load(Ordering::SeqCst), usize::from(finish));
    assert_eq!(
        f.calls.active_abandons.load(Ordering::SeqCst),
        usize::from(!finish)
    );
    assert_eq!(f.calls.active_drops.load(Ordering::SeqCst), 1);
    for repeat_finish in [true, false] {
        assert!(matches!(
            f.native
                .retire_isolation(f.successor(), repeat_finish, NativeFact::Applied),
            NativeFact::Failed { .. }
        ));
    }
    assert!(matches!(
        f.native
            .execute(NativeEffect::AbandonReservation {
                identity: f.identity
            })
            .await,
        NativeFact::Failed { .. }
    ));
    f.assert_phase(false, true, true, false);
    assert_eq!(f.calls.finishes.load(Ordering::SeqCst), usize::from(finish));
    assert_eq!(
        f.calls.active_abandons.load(Ordering::SeqCst),
        usize::from(!finish)
    );
    assert_eq!(f.calls.reservation_abandons.load(Ordering::SeqCst), 0);
    f.close();
}

#[tokio::test]
async fn opaque_finish_panic_keeps_original_resource_retirement_unknown() {
    active_retirement_panic(Script::FinishPanic, true).await;
}

#[tokio::test]
async fn opaque_active_abandon_panic_keeps_original_resource_retirement_unknown() {
    active_retirement_panic(Script::ActiveAbandonPanic, false).await;
}

#[tokio::test]
async fn opaque_activation_can_reenter_original_storage_and_successful_finish_has_one_owner() {
    let f = Fixture::new(Script::Success);
    assert!(matches!(
        f.activate().await,
        NativeFact::ReservationActivated { .. }
    ));
    f.assert_phase(false, false, true, true);
    assert!(matches!(
        f.native
            .retire_isolation(f.successor(), true, NativeFact::Applied),
        NativeFact::Applied
    ));
    f.assert_phase(false, false, true, false);
    assert!(matches!(
        f.native
            .execute(NativeEffect::AbandonReservation {
                identity: f.identity
            })
            .await,
        NativeFact::Applied
    ));
    assert_eq!(f.calls.activates.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.finishes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.active_abandons.load(Ordering::SeqCst), 0);
    assert_eq!(f.calls.reservation_abandons.load(Ordering::SeqCst), 0);
    assert_eq!(f.calls.reentries.load(Ordering::SeqCst), 2);
    assert_eq!(f.calls.reservation_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.active_drops.load(Ordering::SeqCst), 1);
    f.close();
}
