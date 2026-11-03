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

//! Original driver edge retirement and finite custody when exit is unknown.
//!
//! This metadata is installed before execution. It creates no Task conclusion,
//! scheduler, retry, or credit. Only actual destructors discharge edge custody.

use super::{
    driver_failure::{DriverFailure, ExitHook},
    input::RawInput,
    instance::OperatorInstance,
    operator::{ExitObservation, FinishWatch},
    operator_exit::{ExitAction, HookOutcome, OperatorExitScope},
};
use crate::{
    exec::chunk::Chunk,
    runtime::{
        mem_tracker::MemTracker,
        query_memory::QueryMemoryBinding,
        runtime_state::{RuntimeErrorState, RuntimeState},
        task::{
            ExecutionFailure,
            io::{RootInputPermit, RootWorkExitProgress, TaskEventSink},
        },
    },
};
use std::{
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};

/// Fixed custody for one original edge. Healthy Root quantum phases reuse
/// this same metadata only after every temporary owner actually returned or
/// transferred. Permanent/unknown retirement never resets its once phases.
pub(crate) struct EdgeRetirement {
    chunk: Option<ManuallyDrop<Chunk>>,
    permit: Option<ManuallyDrop<RootInputPermit>>,
    upstream_permit: Option<ManuallyDrop<RootInputPermit>>,
    permit_unresolved: bool,
    workspace_unresolved: bool,
    quantum_unresolved: bool,
    scope: OperatorExitScope,
}
impl EdgeRetirement {
    pub(crate) fn new() -> Self {
        Self {
            chunk: None,
            permit: None,
            upstream_permit: None,
            permit_unresolved: false,
            workspace_unresolved: false,
            quantum_unresolved: false,
            scope: OperatorExitScope::new(),
        }
    }
    pub(crate) fn retain_raw(&mut self, input: &mut Option<RawInput>) {
        if let Some(input) = input.take() {
            assert!(
                self.chunk.is_none() && self.permit.is_none(),
                "one permanently retired edge input"
            );
            let (chunk, permit) = input.into_edge_parts();
            self.chunk = Some(ManuallyDrop::new(chunk));
            self.permit = permit.map(ManuallyDrop::new);
        }
    }
    pub(crate) fn retain_upstream(&mut self, permit: RootInputPermit) {
        assert!(
            self.upstream_permit.is_none(),
            "one original upstream grant"
        );
        self.upstream_permit = Some(ManuallyDrop::new(permit));
    }
    pub(crate) fn is_unresolved(&self) -> bool {
        self.quantum_unresolved
            || self.workspace_unresolved
            || self.permit_unresolved
            || self.scope.is_unresolved()
    }
    pub(crate) fn quantum_unresolved(&self) -> bool {
        self.quantum_unresolved
    }
    pub(crate) fn workspace_unresolved(&self) -> bool {
        self.workspace_unresolved
    }
    /// The caller first installs the output Chunk and original permit together
    /// on its edge. Normal workspace exit does not retire that transferred
    /// permit: its receiving Root sink still owns the original input demand.
    pub(crate) fn finish_output_quantum(
        &mut self,
        failures: &RuntimeErrorState,
        release: impl FnOnce(),
    ) -> bool {
        assert!(self.chunk.is_none() && self.permit.is_none() && self.upstream_permit.is_none());
        self.retire_workspace(failures, release);
        let healthy = !self.is_unresolved() && self.scope.reset_healthy_root_quantum();
        self.workspace_unresolved |= !healthy;
        self.quantum_unresolved |= !healthy;
        healthy
    }
    /// An unused Root grant stays in original custody before workspace code.
    /// Only the exact permit receipt and empty healthy holders allow reuse.
    pub(crate) fn finish_unused_quantum(
        &mut self,
        failures: &RuntimeErrorState,
        permit: RootInputPermit,
        release: impl FnOnce(),
    ) -> bool {
        assert!(self.chunk.is_none() && self.permit.is_none() && self.upstream_permit.is_none());
        self.retain_upstream(permit);
        self.retire_workspace(failures, release);
        self.retire(failures);
        let empty = self.chunk.is_none() && self.permit.is_none() && self.upstream_permit.is_none();
        let healthy = empty && !self.is_unresolved() && self.scope.reset_healthy_root_quantum();
        self.quantum_unresolved |= !healthy;
        healthy
    }
    pub(crate) fn retire_workspace(
        &mut self,
        failures: &RuntimeErrorState,
        release: impl FnOnce(),
    ) {
        let outcome = self.scope.run(
            ExitAction::Begin,
            failures,
            || DriverFailure::ExitHookPanicked(ExitHook::ReleaseInput).into(),
            || {
                release();
                Ok(())
            },
        );
        // Neither a handled marker nor an already-started/unknown hook is a
        // new positive workspace receipt. This quantum may not refund a grant
        // unless this exact invocation returned normally.
        self.workspace_unresolved |= !matches!(&outcome, HookOutcome::Returned(Ok(())));
        if let Err(cause) = outcome.into_cleanup_result() {
            failures.report_cleanup(&cause);
        }
    }
    pub(crate) fn retire(&mut self, failures: &RuntimeErrorState) {
        // Unknown workspace may still retain/alias this input. Neither its
        // original Chunk nor its permission can be released or retried.
        if self.is_unresolved() {
            return;
        }
        let result = self
            .scope
            .run(
                ExitAction::ReleaseInput,
                failures,
                || DriverFailure::ExitHookPanicked(ExitHook::ReleaseInput).into(),
                || {
                    if let Some(mut chunk) = self.chunk.take() {
                        // The value is attempted once synchronously. No consumed
                        // ManuallyDrop bits survive an await or a later retry.
                        unsafe {
                            ManuallyDrop::drop(&mut chunk);
                        }
                    }
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(cause) = result {
            failures.report_cleanup(&cause);
        }
        // A Chunk or workspace unwind cannot refund its original permission.
        if self.scope.is_unresolved() {
            return;
        }
        for (action, permit) in [
            (ExitAction::Close, &mut self.permit),
            (ExitAction::Cancel, &mut self.upstream_permit),
        ] {
            let result = self
                .scope
                .run(
                    action,
                    failures,
                    || DriverFailure::ExitHookPanicked(ExitHook::ReleaseInput).into(),
                    || {
                        if let Some(owner) = permit.as_mut() {
                            // A supervised permit Drop can return with unknown
                            // credit custody. Only its exact receipt discharges
                            // this original edge obligation.
                            if owner.retire() == RootWorkExitProgress::Exited {
                                let mut owner = permit.take().unwrap();
                                unsafe {
                                    ManuallyDrop::drop(&mut owner);
                                }
                            } else {
                                self.permit_unresolved = true;
                            }
                        }
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(cause) = result {
                failures.report_cleanup(&cause);
            }
            if self.scope.is_unresolved() {
                break;
            }
        }
    }
    pub(crate) fn visit(
        &mut self,
        failures: &RuntimeErrorState,
        visit: &mut dyn FnMut(ExitObservation),
    ) {
        let mut receipt = Some(if self.is_unresolved() {
            self.scope.unresolved_observation()
        } else {
            ExitObservation::exited()
        });
        let observations = self.scope.observe(
            failures,
            || DriverFailure::ExitHookPanicked(ExitHook::Observe).into(),
            |collect| {
                collect(
                    receipt
                        .take()
                        .expect("one edge receipt per observation visit"),
                )
            },
        );
        for observation in observations {
            visit(observation);
        }
    }
}

pub(crate) struct RetainedDriverOwners {
    pub _process_panic: ProcessPanicCustody,
    pub _operators: Vec<OperatorInstance>,
    pub _edges: Vec<EdgeRetirement>,
    pub _edge_inputs: Vec<Option<RawInput>>,
    pub _upstream_grant: Option<(usize, RootInputPermit)>,
    pub _runtime: Arc<RuntimeState>,
    pub _query_memory: Option<QueryMemoryBinding>,
    pub _edge_trackers: Vec<Option<Arc<MemTracker>>>,
    pub _event_sink: Arc<dyn TaskEventSink>,
}
/// One original driver has exactly one slot. Only an unresolved Drop retains
/// its own slot; there is no process-global list or unbounded parking service.
struct DriverCustodyState {
    owners: Option<ManuallyDrop<RetainedDriverOwners>>,
    driver: Option<ManuallyDrop<Box<super::global_driver_executor::Retirement>>>,
    driver_installed: bool,
    driver_busy: bool,
    parking: Option<ManuallyDrop<Box<super::global_driver_executor::ParkingPhase>>>,
    parking_active: bool,
}
pub(crate) struct DriverExitCustody {
    state: Mutex<DriverCustodyState>,
    retained: Mutex<Option<Arc<Self>>>,
}
impl DriverExitCustody {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(DriverCustodyState {
                owners: None,
                driver: None,
                driver_installed: false,
                driver_busy: false,
                parking: None,
                parking_active: false,
            }),
            retained: Mutex::new(None),
        })
    }
    fn pin(self: &Arc<Self>) {
        let mut pin = self.retained.lock().unwrap_or_else(|e| e.into_inner());
        if pin.is_none() {
            *pin = Some(self.clone());
        }
    }
    pub(crate) fn retain(self: &Arc<Self>, owners: RetainedDriverOwners) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.owners.is_none(),
            "one original driver owner retention"
        );
        state.owners = Some(ManuallyDrop::new(owners));
        drop(state);
        self.pin();
    }
    pub(crate) fn has_retained_work(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .owners
            .is_some()
    }
    pub(crate) fn retain_driver_exit(
        self: &Arc<Self>,
        retirement: super::global_driver_executor::Retirement,
    ) {
        // Only a Pending/Unknown cold exit allocates this one original box.
        let retirement = ManuallyDrop::new(Box::new(retirement));
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!state.driver_installed, "one original task exit owner");
        state.driver_installed = true;
        state.driver = Some(retirement);
        drop(state);
        self.pin();
    }
    pub(crate) fn try_begin_parking(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.parking_active || state.parking.is_some() {
            return false;
        }
        state.parking_active = true;
        true
    }
    pub(crate) fn finish_parking(
        self: &Arc<Self>,
        phase: Option<super::global_driver_executor::ParkingPhase>,
    ) {
        let phase = phase.map(|phase| ManuallyDrop::new(Box::new(phase)));
        let faulted = phase.is_some();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.parking_active && state.parking.is_none(),
            "one original parking loan"
        );
        state.parking_active = false;
        state.parking = phase;
        drop(state);
        if faulted {
            self.pin();
        }
    }
    pub(crate) fn poll_driver_exit(&self) -> super::global_driver_executor::RetirementPoll {
        use super::global_driver_executor::RetirementPoll;
        let value = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let Some(value) = state.driver.take() else {
                return if state.driver_busy {
                    RetirementPoll::Pending
                } else {
                    RetirementPoll::Exited
                };
            };
            state.driver_busy = true;
            value
        };
        let mut loan = DriverExitLoan {
            owner: self,
            value: Some(value),
        };
        // A borrowed original holder and its restoring guard precede every
        // foreign phase. No custody lock covers callbacks or destructors.
        let progress = loan.value.as_mut().unwrap().tick();
        if progress == RetirementPoll::Exited {
            drop(ManuallyDrop::into_inner(loan.value.take().unwrap()));
        }
        progress
    }
}
struct DriverExitLoan<'a> {
    owner: &'a DriverExitCustody,
    value: Option<ManuallyDrop<Box<super::global_driver_executor::Retirement>>>,
}
impl Drop for DriverExitLoan<'_> {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.driver.is_none(), "one original task exit loan");
        state.driver = self.value.take();
        state.driver_busy = false;
        let release = if state.driver.is_none() && state.owners.is_none() && state.parking.is_none()
        {
            self.owner
                .retained
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
        } else {
            None
        };
        drop(state);
        drop(release);
    }
}

/// `process` enters ordinary work only until its first unwind. Exit hooks and
/// observers then have their own once scopes. This original driver slot keeps
/// raw payloads outside the accepted Task error/source graph.
#[derive(Default)]
pub(crate) struct ProcessPanicCustody {
    primary: [Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>; 2],
    secondary: [Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>; 2],
    failure: Option<ExecutionFailure>,
    unresolved: bool,
    watch: Option<Arc<crate::runtime::observable::Observable>>,
}
impl ProcessPanicCustody {
    pub(crate) fn new() -> Self {
        Self {
            watch: Some(Arc::new(crate::runtime::observable::Observable::new())),
            ..Self::default()
        }
    }
    pub(crate) fn capture(
        &mut self,
        payload: Box<dyn std::any::Any + Send>,
        failures: &RuntimeErrorState,
        publish: impl FnOnce(ExecutionFailure),
    ) -> ExecutionFailure {
        assert!(
            self.failure.is_none(),
            "ordinary driver work cannot re-enter after its first unwind"
        );
        self.primary[0] = Some(ManuallyDrop::new(payload));
        let cause: ExecutionFailure = DriverFailure::from_panic(
            self.primary[0]
                .as_ref()
                .expect("saved process panic")
                .as_ref(),
        )
        .into();
        self.failure = Some(cause.clone());
        // The exact existing Fragment/Runtime acceptance path remains the only
        // publisher. A notification unwind is a separate cleanup incident.
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| publish(cause.clone())))
        {
            self.primary[1] = Some(ManuallyDrop::new(payload));
            self.unresolved = true;
            failures.report_unresolved_exit(&cause);
            failures.report_cleanup(&cause);
        }
        for index in [1, 0] {
            if let Some(mut payload) = self.primary[index].take()
                && let Err(secondary) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                        ManuallyDrop::drop(&mut payload);
                    }))
            {
                // Save before any subsequent reporting or foreign action.
                self.secondary[index] = Some(ManuallyDrop::new(secondary));
                self.unresolved = true;
                failures.report_unresolved_exit(&cause);
                failures.report_cleanup(&cause);
            }
        }
        cause
    }
    pub(crate) fn visit(&self, visit: &mut dyn FnMut(ExitObservation)) {
        if self.unresolved {
            visit(ExitObservation::unresolved(FinishWatch::Notify(
                self.watch
                    .as_ref()
                    .expect("original process panic watch")
                    .clone(),
            )));
        }
    }
}

#[cfg(test)]
mod process_panic_tests {
    use super::*;
    use std::{
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct RetainedData {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for RetainedData {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Secondary {
        data: Arc<RetainedData>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Secondary {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Primary {
        data: Option<Arc<RetainedData>>,
        control: Arc<RuntimeErrorState>,
        drops: Arc<AtomicUsize>,
        secondary_drops: Arc<AtomicUsize>,
    }
    impl Drop for Primary {
        fn drop(&mut self) {
            assert!(
                self.control.error().is_some(),
                "the original work cause precedes foreign disposal"
            );
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic_any(Secondary {
                data: self.data.take().unwrap(),
                drops: self.secondary_drops.clone(),
            });
        }
    }
    #[test]
    fn ordinary_driver_panic_is_accepted_once_and_disposal_is_not_an_exit_fault() {
        let control = RuntimeErrorState::default();
        let payload = catch_unwind(|| panic!("original ordinary driver panic")).unwrap_err();
        let publications = AtomicUsize::new(0);
        let mut scope = ProcessPanicCustody::new();
        let original = scope.capture(payload, &control, |cause| {
            publications.fetch_add(1, Ordering::SeqCst);
            control.set_error(cause);
        });
        assert_eq!(publications.load(Ordering::SeqCst), 1);
        assert!(control.error().unwrap().same_occurrence(&original));
        assert!(original.detail().contains("original ordinary driver panic"));
        assert!(scope.primary.iter().all(Option::is_none));
        assert!(scope.secondary.iter().all(Option::is_none));
        assert!(control.local_exit_fault().is_none());
        let mut unresolved = false;
        scope.visit(&mut |_| unresolved = true);
        assert!(
            !unresolved,
            "actual operator exit is still independently observed by the driver"
        );
    }
    #[test]
    fn original_and_publication_opaque_disposal_keep_separate_real_resource_custody() {
        let control = Arc::new(RuntimeErrorState::default());
        let primary_drops = Arc::new(AtomicUsize::new(0));
        let secondary_drops = Arc::new(AtomicUsize::new(0));
        let data_drops = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(RetainedData {
            bytes: vec![41; 32768],
            drops: data_drops.clone(),
        });
        let weak_data = Arc::downgrade(&data);
        let primary = Primary {
            data: Some(data),
            control: control.clone(),
            drops: primary_drops.clone(),
            secondary_drops: secondary_drops.clone(),
        };
        let publication_data_drops = Arc::new(AtomicUsize::new(0));
        let publication_data = Arc::new(RetainedData {
            bytes: vec![73; 8192],
            drops: publication_data_drops.clone(),
        });
        let weak_publication = Arc::downgrade(&publication_data);
        let publication_primary_drops = Arc::new(AtomicUsize::new(0));
        let publication_secondary_drops = Arc::new(AtomicUsize::new(0));
        let notify_control = Arc::downgrade(&control);
        let notify_primary_drops = publication_primary_drops.clone();
        let notify_secondary_drops = publication_secondary_drops.clone();
        let notify_weak = weak_data.clone();
        let notify_drops = primary_drops.clone();
        let _subscription = control.stop_observable().subscribe(Arc::new(move || {
            let control = notify_control.upgrade().unwrap();
            assert!(control.error().is_some());
            assert_eq!(notify_drops.load(Ordering::SeqCst), 0);
            assert_eq!(notify_weak.upgrade().unwrap().bytes[0], 41);
            panic_any(Primary {
                data: Some(publication_data.clone()),
                control,
                drops: notify_primary_drops.clone(),
                secondary_drops: notify_secondary_drops.clone(),
            });
        }));
        let payload = catch_unwind(AssertUnwindSafe(|| panic_any(primary))).unwrap_err();
        let mut scope = ProcessPanicCustody::new();
        let original = scope.capture(payload, &control, |cause| control.set_error(cause));
        assert!(control.error().unwrap().same_occurrence(&original));
        assert!(control.local_exit_fault().is_some());
        assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(publication_primary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
        assert_eq!(publication_secondary_drops.load(Ordering::SeqCst), 0);
        assert!(scope.primary.iter().all(Option::is_none));
        for index in 0..2 {
            let secondary = scope.secondary[index]
                .as_ref()
                .unwrap()
                .downcast_ref::<Secondary>()
                .unwrap();
            assert_eq!(secondary.drops.load(Ordering::SeqCst), 0);
            assert!(!secondary.data.bytes.is_empty());
        }
        assert_eq!(weak_data.upgrade().unwrap().bytes, vec![41; 32768]);
        assert_eq!(weak_publication.upgrade().unwrap().bytes, vec![73; 8192]);
        assert_eq!(data_drops.load(Ordering::SeqCst), 0);
        assert_eq!(publication_data_drops.load(Ordering::SeqCst), 0);
        let mut first_watch = None;
        for _ in 0..3 {
            scope.visit(&mut |observation| {
                let super::super::operator::ExitProgress::Unresolved(FinishWatch::Notify(watch)) =
                    observation.progress
                else {
                    panic!("unknown disposal supplies no false physical exit")
                };
                if let Some(first) = &first_watch {
                    assert!(Arc::ptr_eq(first, &watch));
                } else {
                    first_watch = Some(watch);
                }
            });
        }
        assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
        // The enclosing real driver retains this entire scope when unresolved;
        // this unit oracle establishes only its finite opaque custody.
    }
}
