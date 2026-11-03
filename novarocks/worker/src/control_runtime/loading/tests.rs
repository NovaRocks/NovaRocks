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
use bytes::Bytes;
use novarocks_control_runtime::{
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    CreationContent, ExchangeTopology, FrozenBytes, ParticipationRef, TaskDescriptor, TaskIdentity,
    TaskOperationId,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    UniqueId,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Barrier, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn core() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn observer() -> ConsumerId {
    ConsumerId::new(2).unwrap()
}
struct Pending;
impl TaskPendingNotice for Pending {
    fn pending(&self) {}
}
#[derive(Debug)]
struct PanicGraph(Arc<AtomicUsize>);
impl Drop for PanicGraph {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Probe {
    getter_calls: Arc<AtomicUsize>,
    input_drops: Arc<AtomicUsize>,
    panic_drops: Arc<AtomicUsize>,
    holder: Arc<OnceLock<Weak<Custody>>>,
    panic_getter: bool,
    panic_drop: bool,
    gate: Option<Arc<Barrier>>,
}
impl std::fmt::Debug for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("loading input probe")
    }
}
impl CreationContent for Probe {
    fn encoded_len(&self) -> usize {
        1
    }
    fn retained_bytes(&self) -> usize {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        let holder = self.holder.get().unwrap().upgrade().unwrap();
        assert!(
            holder.sources.try_lock().is_ok(),
            "foreign getter must not hold the input loan lock"
        );
        if let Some(gate) = &self.gate {
            gate.wait();
            gate.wait();
        }
        if self.panic_getter {
            std::panic::panic_any(PanicGraph(self.panic_drops.clone()));
        }
        42
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.input_drops.fetch_add(1, Ordering::SeqCst);
        let holder = self.holder.get().unwrap().upgrade().unwrap();
        assert!(
            holder.sources.try_lock().is_ok(),
            "foreign destructor must not hold its original input loan lock"
        );
        if self.panic_drop {
            std::panic::panic_any(PanicGraph(self.panic_drops.clone()));
        }
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: WorkObligation,
    capacity: ReservationObligation,
    owner: LoaderCustody,
    job: Option<LoaderJob>,
    getter: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    panic_drops: Arc<AtomicUsize>,
}
fn reserve_role(
    scope: &ScopeHandle,
    parent: &ReservationObligation,
    kind: WorkKind,
    core_leg: bool,
) -> AdmittedCompletionTransfer {
    let mut guard = scope
        .reserve_child_next(
            generation(),
            parent.id(),
            novarocks_control_runtime_core::capacity::ReservationResources::new(
                ResourceVector::default(),
                novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                },
            ),
            kind,
        )
        .unwrap();
    let mut consumers = vec![(observer(), ConsumerKind::Observer)];
    if core_leg {
        consumers.push((
            core(),
            ConsumerKind::Owner {
                scope: scope.id(),
                generation: generation(),
            },
        ));
    }
    let report = ReportSlot::reserve_next(&mut guard, generation(), 2, consumers).unwrap();
    let (work, capacity) = guard.bind(kind).unwrap();
    AdmittedCompletionTransfer {
        work,
        capacity,
        report,
    }
}
impl Fixture {
    fn new(panic_getter: bool, panic_drop: bool, gate: Option<Arc<Barrier>>) -> Self {
        Self::with_domains(panic_getter, panic_drop, gate, vec![])
    }
    fn with_domains(
        panic_getter: bool,
        panic_drop: bool,
        gate: Option<Arc<Barrier>>,
        domains: Vec<novarocks_execution_contract::TaskDomainUpdate>,
    ) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 1)]).unwrap());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (parent, capacity) = scope
            .reserve_next(
                generation(),
                novarocks_control_runtime_core::capacity::ReservationResources::for_work(
                    ResourceVector::new([(dimension(), 1)]).unwrap(),
                ),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let loader = reserve_role(&scope, &capacity, WorkKind::Business, true);
        let input_role = reserve_role(&scope, &loader.capacity, WorkKind::Cleanup, false);
        let participation = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(7501, 7502), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let task = TaskIdentity::new(
            participation.query_execution_id(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            participation.backend_process_id(),
        );
        let descriptor = TaskDescriptor::try_new(
            task,
            UniqueId::new(1, 1),
            NonZeroUsize::new(1).unwrap(),
            vec![],
            ExchangeTopology::default(),
        )
        .unwrap();
        let mut op = [3; 16];
        op[6] = 0x70;
        op[8] = 0x80;
        let request = Arc::new(
            CreateTask::try_new(
                TaskOperationId::try_from_bytes(op).unwrap(),
                participation,
                descriptor,
                domains,
            )
            .unwrap(),
        );
        let getter = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let panic_drops = Arc::new(AtomicUsize::new(0));
        let holder = Arc::new(OnceLock::new());
        let input = TaskCreationInput::new(
            FrozenBytes::freeze(Bytes::from_static(b"plan")),
            Box::new(Probe {
                getter_calls: getter.clone(),
                input_drops: drops.clone(),
                panic_drops: panic_drops.clone(),
                holder: holder.clone(),
                panic_getter,
                panic_drop,
                gate,
            }),
        );
        let payload = TaskCreatePayload {
            id: PayloadId(1),
            request,
            input,
            ingress_retained_bytes: 5,
        };
        let (owner, job) = LoaderCustody::from_admitted(
            payload,
            loader,
            input_role,
            Arc::new(Pending),
            Arc::new(ExecutionHealth::default()),
        );
        assert!(holder.set(Arc::downgrade(&owner.0)).is_ok());
        Self {
            ledger,
            scope,
            parent,
            capacity,
            owner,
            job: Some(job),
            getter,
            drops,
            panic_drops,
        }
    }
    fn finish(mut self) {
        assert!(self.owner.ready_for_exit().unwrap());
        self.owner.report().consume(core()).unwrap();
        assert!(!self.owner.try_retire().unwrap());
        self.owner.report().consume(observer()).unwrap();
        assert!(self.owner.try_retire().unwrap());
        drop(self.owner);
        self.parent.body_exited().unwrap();
        self.parent.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        self.capacity
            .release_quota(&self.capacity.resources().unwrap().quota)
            .unwrap();
        self.capacity.finish().unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
#[test]
fn measured_input_is_borrowed_without_locks_and_whole_exit_waits_for_actual_disposal() {
    let mut f = Fixture::new(false, false, None);
    let cost = match f.job.take().unwrap().run() {
        LoaderOutcome::Loaded { result, .. } => match *result {
            Ok(cost) => cost,
            Err(_) => panic!("actual measure"),
        },
        _ => panic!("actual measure"),
    };
    assert_eq!(
        cost.input_retained_bytes,
        (std::mem::size_of::<TaskCreationInput>() + 4 + 42) as u64
    );
    assert_eq!(cost.ingress_retained_bytes, 5);
    assert!(cost.structural_bytes > 0);
    assert_eq!(f.getter.load(Ordering::SeqCst), 1);
    assert_eq!(f.drops.load(Ordering::SeqCst), 0);
    assert!(!f.owner.ready_for_exit().unwrap());
    let disposal = f.owner.take_input_disposal().unwrap();
    assert!(!f.owner.ready_for_exit().unwrap());
    f.scope.request_stop().unwrap();
    assert!(disposal.run());
    assert_eq!(f.drops.load(Ordering::SeqCst), 1);
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    f.finish();
}
#[test]
fn cancel_before_loader_start_never_calls_the_getter_and_still_requires_input_exit() {
    let mut f = Fixture::new(false, false, None);
    f.owner.cancel();
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::Canceled
    ));
    assert_eq!(f.getter.load(Ordering::SeqCst), 0);
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(f.owner.take_measured_sources().is_none());
    assert!(f.owner.take_input_disposal().unwrap().run());
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    f.finish();
}
#[test]
fn getter_panic_keeps_original_input_payload_and_unknown_work() {
    let mut f = Fixture::new(true, false, None);
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::Unresolved
    ));
    assert_eq!(f.getter.load(Ordering::SeqCst), 1);
    assert_eq!(f.drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.panic_drops.load(Ordering::SeqCst), 0);
    assert!(f.owner.take_measured_sources().is_none());
    assert!(f.owner.take_input_disposal().is_none());
    assert_eq!(
        f.owner.0.health.failure().unwrap().fact().class(),
        FailureClass::Unexpected
    );
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(!f.owner.try_retire().unwrap());
    assert_eq!(f.ledger.unresolved_work().len(), 1);
    assert_eq!(f.ledger.used(dimension()), 1);
    f.scope.close_external().unwrap();
    assert!(!f.scope.join_ready().unwrap());
}
#[test]
fn input_drop_panic_does_not_publish_loader_exit_or_destroy_its_caught_panic() {
    let mut f = Fixture::new(false, true, None);
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::Loaded { result, .. } if result.is_ok()
    ));
    assert!(!f.owner.take_input_disposal().unwrap().run());
    assert_eq!(f.drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.panic_drops.load(Ordering::SeqCst), 0);
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(!f.owner.try_retire().unwrap());
    assert_eq!(f.ledger.unresolved_work().len(), 1);
    assert_eq!(f.ledger.used(dimension()), 1);
}
#[test]
fn slow_getter_keeps_metadata_and_scope_ledger_available_for_cancel_and_drain() {
    let gate = Arc::new(Barrier::new(2));
    let mut f = Fixture::new(false, false, Some(gate.clone()));
    let job = f.job.take().unwrap();
    struct RunningGetter {
        gate: Arc<Barrier>,
        worker: Option<std::thread::JoinHandle<LoaderOutcome>>,
        released: bool,
    }
    impl RunningGetter {
        fn finish(mut self) -> LoaderOutcome {
            self.released = true;
            self.gate.wait();
            self.worker.take().unwrap().join().unwrap()
        }
    }
    impl Drop for RunningGetter {
        fn drop(&mut self) {
            if let Some(worker) = self.worker.take() {
                if !self.released {
                    self.gate.wait();
                }
                let _ = worker.join();
            }
        }
    }
    let worker = std::thread::spawn(move || job.run());
    gate.wait();
    let worker = RunningGetter {
        gate: gate.clone(),
        worker: Some(worker),
        released: false,
    };
    f.owner.cancel();
    f.scope.close_external().unwrap();
    assert!(!f.owner.ready_for_exit().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert!(!f.scope.join_ready().unwrap());
    assert!(matches!(worker.finish(), LoaderOutcome::Loaded { result, .. } if result.is_ok()));
    assert!(f.owner.take_input_disposal().unwrap().run());
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    f.finish();
}

#[test]
fn lost_loader_authority_keeps_its_original_input_and_reports_invariant_unknown_exit() {
    let mut f = Fixture::new(false, false, None);
    drop(f.job.take());
    assert_eq!(f.getter.load(Ordering::SeqCst), 0);
    assert_eq!(f.drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.owner.phase(), ExitCustodyPhase::Unresolved);
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(f.owner.take_input_disposal().is_none());
    assert_eq!(
        f.owner.0.health.failure().unwrap().fact().class(),
        FailureClass::Invariant
    );
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.ledger.unresolved_work().len(), 1);
}

#[test]
fn scope_stop_refuses_loader_business_but_allows_its_original_cleanup_child() {
    use novarocks_control_runtime::transaction::RuntimeError;
    use novarocks_control_runtime_core::scope::ScopeError;
    let mut f = Fixture::new(false, false, None);
    f.scope.request_stop().unwrap();
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::StartRefused(ExitCustodyError::Runtime(RuntimeError::Scope(
            ScopeError::Stopped
        )))
    ));
    assert_eq!(f.getter.load(Ordering::SeqCst), 0);
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(f.owner.take_input_disposal().unwrap().run());
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    f.finish();
}

#[test]
fn lost_input_disposal_job_restores_original_custody_and_never_claims_source_exit() {
    let mut f = Fixture::new(false, false, None);
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::Loaded { result, .. } if result.is_ok()
    ));
    drop(f.owner.take_input_disposal().unwrap());
    assert_eq!(f.drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        f.owner.0.input_exit_custody.phase(),
        ExitCustodyPhase::Unresolved
    );
    assert!(!f.owner.ready_for_exit().unwrap());
    assert_eq!(
        f.owner.0.health.failure().unwrap().fact().class(),
        FailureClass::Invariant
    );
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.ledger.unresolved_work().len(), 1);
}

#[derive(Debug)]
struct RequestDropProbe {
    drops: Arc<AtomicUsize>,
    panic_drops: Arc<AtomicUsize>,
}
impl novarocks_execution_contract::CodecOwnedContent for RequestDropProbe {
    fn fingerprint(&self) -> novarocks_execution_contract::ContentFingerprint {
        novarocks_execution_contract::ContentFingerprint::from_bytes([4; 16])
    }
    fn encoded_len(&self) -> usize {
        1
    }
}
impl Drop for RequestDropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(PanicGraph(self.panic_drops.clone()));
    }
}
#[test]
fn request_and_input_destructor_panics_have_independent_original_custody() {
    let request_drops = Arc::new(AtomicUsize::new(0));
    let request_panic_drops = Arc::new(AtomicUsize::new(0));
    let domains = vec![
        novarocks_execution_contract::TaskDomainUpdate::TaskDynamicFilter {
            version: novarocks_execution_contract::DomainVersion::new(1).unwrap(),
            payload: Arc::new(RequestDropProbe {
                drops: request_drops.clone(),
                panic_drops: request_panic_drops.clone(),
            }),
        },
    ];
    let mut f = Fixture::with_domains(false, true, None, domains);
    assert!(matches!(
        f.job.take().unwrap().run(),
        LoaderOutcome::Loaded { result, .. } if result.is_ok()
    ));
    assert!(!f.owner.take_input_disposal().unwrap().run());
    assert_eq!(request_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.drops.load(Ordering::SeqCst),
        1,
        "input destruction proceeds after the separate request boundary unwinds"
    );
    assert_eq!(request_panic_drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.panic_drops.load(Ordering::SeqCst), 0);
    let panics = f.owner.0.panic.lock().unwrap();
    assert!(panics[1].is_some() && panics[2].is_some());
    drop(panics);
    assert!(!f.owner.ready_for_exit().unwrap());
    assert!(!f.owner.try_retire().unwrap());
    assert_eq!(f.ledger.unresolved_work().len(), 1);
    assert_eq!(f.ledger.used(dimension()), 1);
}
