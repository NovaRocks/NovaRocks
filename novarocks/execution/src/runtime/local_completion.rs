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

//! One taskless fragment's finite completion roles. Registration only stores
//! its one callback; the physical completion poller invokes it after the
//! original finisher call returns. No Task identity or protocol is invented.

use std::sync::{Arc, Mutex};

use novarocks_control_runtime::{
    completion::ReportSlot,
    continuation::{ContinuationPhase, ContinuationSlot, InvocationResult, RegistrationToken},
    exit_custody::{ExitCustody, ExitCustodyPhase as Phase, ExitObligation},
    obligation::{ReservationObligation, WorkObligation},
    transaction::{DeclaredInput, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, AdmittedFinisher, AdmittedWorkParts, CompletionWorkCapability,
    SettledContinuation, SettlementCapability, SettlementPermit,
    failure::{FailureClass, FailureFact},
};

use crate::runtime::{
    task::{ExecutionFailure, ExecutionReason},
    task_control::ExecutionExitReporter,
};

type Snapshot = Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>;
const COMPLETION_ENTRY_COUNT: usize = 3;
const RESPONSIBILITIES: usize = COMPLETION_ENTRY_COUNT + 1;
const FINISHER: usize = 0;

struct LocalCompletionEntry {
    slot: ExitCustody,
    report: ReportSlot,
    consumed: bool,
}
struct State {
    entries: [LocalCompletionEntry; COMPLETION_ENTRY_COUNT],
    settlement_consumed: bool,
    settlement_retired: bool,
    removed: bool,
    run_claimed: bool,
}
struct Owner {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    state: Mutex<State>,
    settlement: ContinuationSlot<Snapshot>,
    report: ReportSlot,
    health: ExecutionExitReporter,
    #[cfg(test)]
    after_initial_reap: Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
}

/// The original executor holds this poller, independently of settlement
/// registration authority. It runs on its host thread, outside all role locks.
#[derive(Clone)]
pub(crate) struct LocalCompletionPoller(Arc<Owner>);

struct Work {
    owner: Arc<Owner>,
    authority: Option<ExitObligation>,
}
impl Work {
    fn exited(mut self) {
        self.authority
            .take()
            .expect("original local role authority")
            .exited()
            .expect("original local body and captures actually exited");
        self.owner.drain();
    }
}
impl CompletionWorkCapability for Work {
    fn started(&mut self) {
        self.authority
            .as_mut()
            .expect("original local role authority")
            .start()
            .expect("admitted local work starts");
    }
    fn completed(self: Box<Self>) {
        (*self).exited();
    }
    fn unused_exited(self: Box<Self>) {
        (*self).exited();
    }
    fn unresolved(mut self: Box<Self>) {
        // Drop records unknown exit; it is never positive body/capture proof.
        drop(self.authority.take());
        self.owner.health.report(FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::CompletionWorkUnresolved.reason(),
        ));
    }
}
struct Registration {
    token: RegistrationToken<Snapshot>,
    owner: Arc<Owner>,
}
impl SettlementCapability<ExecutionFailure> for Registration {
    fn register(self: Box<Self>, callback: SettledContinuation<ExecutionFailure>) {
        self.token
            .register(callback)
            .unwrap_or_else(|_| panic!("one admitted local settlement registration"));
        // No callback, destructor or poller runs on the registration stack.
    }
    fn revoke_unused(self: Box<Self>) {
        drop(self.token);
        self.owner.drain();
    }
}

fn consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn resources() -> ReservationResources {
    // One real role owns its header, reliable report and capture container.
    // The three OnceRoles and sole continuation do not consume a global quota.
    ReservationResources::new(
        ResourceVector::default(),
        OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
}
fn admit(
    scope: &ScopeHandle,
    admission: WorkAdmission,
    kind: WorkKind,
    resources: ReservationResources,
) -> (WorkObligation, ReservationObligation, ReportSlot) {
    let mut guard = scope
        .reserve_next(Generation::new(1).unwrap(), resources, kind, admission)
        .expect("finite original local completion roles");
    let report = ReportSlot::reserve_next(
        &mut guard,
        Generation::new(1).unwrap(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .expect("one original completion position per local role");
    let (work, capacity) = guard.bind(kind).expect("original local role holder");
    (work, capacity, report)
}
impl LocalCompletionPoller {
    pub(crate) fn admit(
        health: ExecutionExitReporter,
    ) -> (AdmittedWorkParts<ExecutionFailure>, Self) {
        // This geometry follows the three named OnceRoles and the sole child
        // continuation. It is unrelated to SQL data size or backend topology.
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let (finisher, mut finisher_capacity, finisher_report) = admit(
            &scope,
            WorkAdmission::External,
            WorkKind::Cleanup,
            resources()
                .checked_add(&resources())
                .expect("finisher and its one real child"),
        );
        let child = scope
            .reserve_child_from(
                &mut finisher_capacity,
                &finisher,
                DeclaredInput {
                    generation: Generation::new(1).unwrap(),
                    kind: WorkKind::Settlement,
                    resources: resources(),
                    consumers: vec![(consumer(), ConsumerKind::Observer)],
                },
            )
            .expect("one original settlement child from the finisher inventory");
        let child_report = child.report().clone();
        let (settlement, token) = ContinuationSlot::from_admitted(child);
        let make_entry = |work, capacity, report: ReportSlot| {
            let (slot, authority) = ExitCustody::from_admitted(
                novarocks_control_runtime::reported_work::ReportedWork::try_new(
                    work,
                    capacity,
                    report.clone(),
                )
                .unwrap(),
            );
            (
                LocalCompletionEntry {
                    slot,
                    report,
                    consumed: false,
                },
                authority,
            )
        };
        let (finisher, authority) = make_entry(finisher, finisher_capacity, finisher_report);
        let mut completion_entries = vec![finisher];
        let mut authorities = vec![authority];
        for _ in 1..COMPLETION_ENTRY_COUNT {
            let (work, capacity, report) = admit(
                &scope,
                WorkAdmission::External,
                WorkKind::Cleanup,
                resources(),
            );
            let (completion_entry, authority) = make_entry(work, capacity, report);
            completion_entries.push(completion_entry);
            authorities.push(authority);
        }
        scope.close_external().unwrap();
        ledger.close_admission();
        let owner = Arc::new(Owner {
            ledger,
            scope,
            state: Mutex::new(State {
                entries: completion_entries
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("three named roles")),
                settlement_consumed: false,
                settlement_retired: false,
                removed: false,
                run_claimed: false,
            }),
            settlement,
            report: child_report,
            health,
            #[cfg(test)]
            after_initial_reap: Mutex::new(None),
        });
        let work = |authority| {
            Box::new(Work {
                owner: owner.clone(),
                authority: Some(authority),
            }) as Box<dyn CompletionWorkCapability>
        };
        let permit = SettlementPermit::from_admitted_local_reservation(Box::new(Registration {
            token,
            owner: owner.clone(),
        }));
        let [finisher, wall_time, publisher]: [ExitObligation; COMPLETION_ENTRY_COUNT] =
            authorities
                .try_into()
                .unwrap_or_else(|_| unreachable!("three original role authorities"));
        let completion_parts = AdmittedWorkParts::from_admitted_parts(
            AdmittedFinisher::from_admitted_parts(permit, work(finisher)),
            work(wall_time),
            work(publisher),
        );
        (completion_parts, Self(owner))
    }
    pub(crate) fn run(&self) {
        let invoke = {
            let mut state = self.0.state.lock().expect("local completion custody");
            if !state.run_claimed && self.0.settlement.phase() == ContinuationPhase::Registered {
                state.run_claimed = true;
                true
            } else {
                false
            }
        };
        if invoke {
            self.0
                .settlement
                .publish_barrier(None)
                .expect("taskless local barrier");
            if self
                .0
                .settlement
                .invoke()
                .expect("admitted local settlement invocation")
                == InvocationResult::Unresolved
            {
                self.0.health.report(FailureFact::new(
                    FailureClass::Unexpected,
                    ExecutionReason::SettlementContinuationPanicked.reason(),
                ));
            }
        }
        self.0.drain();
    }
}
impl Owner {
    fn drain(&self) {
        let mut state = self.state.lock().expect("local completion custody");
        if state.removed {
            return;
        }
        if self.settlement.phase() == ContinuationPhase::Unused
            && state.entries[FINISHER].slot.body_exited()
            && state.entries[FINISHER].slot.captures_exited()
        {
            // Both facts are necessary: revocation alone is not original exit.
            self.settlement.unused_holder_exited().unwrap();
        }
        if self.settlement.phase() == ContinuationPhase::Exited {
            if !state.settlement_consumed {
                self.report.consume(consumer()).unwrap();
                state.settlement_consumed = true;
            }
            state.settlement_retired = self.settlement.try_retire().unwrap();
        }
        self.ledger
            .reap_scope_ready(self.scope.id(), RESPONSIBILITIES)
            .unwrap();
        #[cfg(test)]
        {
            let gate = self.after_initial_reap.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.wait();
                release.wait();
            }
        }
        for completion_entry in &mut state.entries {
            if completion_entry.slot.phase() != Phase::Exited {
                continue;
            }
            if !completion_entry.consumed {
                completion_entry.report.consume(consumer()).unwrap();
                completion_entry.consumed = true;
            }
            completion_entry
                .slot
                .try_retire()
                .expect("local original completion custody");
        }
        if state.settlement_retired
            && state
                .entries
                .iter()
                .all(|completion_entry| completion_entry.slot.phase() == Phase::Retired)
        {
            // An original can publish its real exit after the first reap and
            // before the loop observes/retire its custody. All four custody
            // roles are now retired and admission is already closed: drain
            // their remaining pure Work records with the same finite bound
            // before removing the Scope. No timeout or retry proves exit.
            self.ledger
                .reap_scope_ready(self.scope.id(), RESPONSIBILITIES)
                .unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            state.removed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{execution_health::ExecutionHealth, task_control::ExecutionExitReporter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ExitThreads {
        release: Option<Arc<std::sync::Barrier>>,
        threads: Vec<std::thread::JoinHandle<()>>,
    }
    impl ExitThreads {
        fn finish(&mut self) -> bool {
            if let Some(release) = self.release.take() {
                release.wait();
            }
            let mut exited = true;
            for thread in self.threads.drain(..) {
                exited &= thread.join().is_ok();
            }
            exited
        }
    }
    impl Drop for ExitThreads {
        fn drop(&mut self) {
            self.finish();
        }
    }

    fn fixture() -> (
        AdmittedWorkParts<ExecutionFailure>,
        LocalCompletionPoller,
        Arc<ExecutionHealth>,
    ) {
        let health = Arc::new(ExecutionHealth::default());
        let reporter = ExecutionExitReporter::new(
            health.clone(),
            crate::runtime::execution_health::ExecutionExitOrigin::Local,
        );
        let (completion_parts, poller) = LocalCompletionPoller::admit(reporter);
        (completion_parts, poller, health)
    }
    #[test]
    fn registration_returns_before_the_local_driver_and_keeps_the_original_finisher_counted() {
        let (completion_parts, poller, health) = fixture();
        let (finisher, wall_time, publisher) = completion_parts.into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        work.started();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        permit.register(Box::new(move |snapshot| {
            assert!(snapshot.is_none(), "no synthetic Task receipt");
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(poller.0.scope.snapshot().unwrap().work, RESPONSIBILITIES);
        poller.run();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            poller.0.scope.snapshot().unwrap().work,
            COMPLETION_ENTRY_COUNT
        );
        assert_eq!(
            poller.0.scope.snapshot().unwrap().storage,
            COMPLETION_ENTRY_COUNT,
            "the original OnceRoles still hold their actual completion reports"
        );
        work.completed();
        drop(wall_time);
        drop(publisher);
        assert!(poller.0.ledger.is_empty());
        assert!(health.failure().is_none());
    }
    #[test]
    fn an_unused_local_fragment_retires_only_after_all_original_holders_exit() {
        let (completion_parts, poller, _) = fixture();
        drop(completion_parts);
        assert!(poller.0.ledger.is_empty());
        poller.run();
        assert!(poller.0.ledger.is_empty());
    }
    #[test]
    fn original_exit_after_the_initial_reap_is_reaped_before_its_scope_is_removed() {
        use std::{
            sync::Barrier,
            time::{Duration, Instant},
        };
        let (completion_parts, poller, health) = fixture();
        let (finisher, wall_time, publisher) = completion_parts.into_parts();
        drop(finisher);
        let publisher_slot = poller.0.state.lock().unwrap().entries[2].slot.clone();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *poller.0.after_initial_reap.lock().unwrap() = Some((entered.clone(), release.clone()));
        let wall = std::thread::spawn(move || drop(wall_time));
        let mut exits = ExitThreads {
            release: Some(release),
            threads: vec![wall],
        };
        entered.wait();
        // The first drain holds only its own metadata lock after its initial
        // reap. The other original can still publish actual exit through the
        // common runtime, then wait to enter this same owner drain.
        exits
            .threads
            .push(std::thread::spawn(move || drop(publisher)));
        let deadline = Instant::now() + Duration::from_secs(2);
        while publisher_slot.phase() != Phase::Exited && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let exited_after_reap = publisher_slot.phase() == Phase::Exited;
        let joined = exits.finish();
        assert!(
            exited_after_reap,
            "the second original must actually exit at the controlled point"
        );
        assert!(
            joined,
            "Scope retirement must not race its original Work record's retirement"
        );
        assert!(poller.0.ledger.is_empty());
        assert!(health.failure().is_none());
    }
    #[test]
    fn a_local_continuation_panic_never_releases_its_original_capacity() {
        let (completion_parts, poller, health) = fixture();
        let (finisher, wall_time, publisher) = completion_parts.into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        work.started();
        permit.register(Box::new(|_| panic!("local settlement call exit unknown")));
        poller.run();
        assert!(health.failure().is_some());
        assert!(poller.0.settlement.phase() == ContinuationPhase::Unresolved);
        work.completed();
        drop(wall_time);
        drop(publisher);
        assert_eq!(poller.0.scope.snapshot().unwrap().work, 2);
        assert_eq!(
            poller.0.scope.snapshot().unwrap().storage,
            2,
            "the unresolved continuation and its original parent keep their actual reports"
        );
        assert!(!poller.0.scope.snapshot().unwrap().join_ready);
    }
}
