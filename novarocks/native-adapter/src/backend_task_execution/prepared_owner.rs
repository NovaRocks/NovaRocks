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

//! The Native preparation's two original, named physical obligations.
//! Worker still registers one child; no producer work runs on its polling owner.

use crate::native_fragment_query::NativeFragmentRegistrationLease;
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    fragment::{
        DormantFragmentHandle, ExecutionFailure, ExecutionResult, RunningFragmentHandle,
        handle::FragmentTerminalPublisher,
    },
    task_control::TaskExecutionControl,
};
use novarocks_execution_contract::{
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use novarocks_worker::root_result_channel::{RootPhysicalExitObservation, RootResultChannel};
use novarocks_worker::{
    TaskPreparationChild, TaskPreparationChildExit, TaskPreparationContext,
    TaskPreparationOutputExit,
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

type Payload = Box<dyn Any + Send>;
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Phase {
    RootRollback,
    RootPoll,
    RootAdopt,
    RootHolder,
    KernelStart,
    KernelPoll,
    KernelHolder,
    Registration,
    Control,
    KernelObserve,
    RootOutput,
}
const PHASES: usize = 11;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    NotStarted,
    Started,
    Exited,
    Unresolved,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kernel {
    Vacant,
    Building,
    Prepared,
    Offered,
    Adopted,
    Starting,
    Draining,
    RuntimeStarting,
    Transferred,
    Exited,
    Unresolved,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum NativePreparedOwnerFailure {
    DuplicateRoot,
    DuplicateKernel,
    OfferWithoutPrepared,
    AdoptWithoutOffer,
    StartWithoutAdoption,
    LateInstall,
    Panicked(Phase),
}
impl std::fmt::Display for NativePreparedOwnerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateRoot => f.write_str("Native preparation already owns its Root child"),
            Self::DuplicateKernel => {
                f.write_str("Native preparation already owns its prepared kernel")
            }
            Self::OfferWithoutPrepared => {
                f.write_str("Native runnable requires the original prepared kernel")
            }
            Self::AdoptWithoutOffer => {
                f.write_str("Native adoption requires an explicit runnable offer")
            }
            Self::StartWithoutAdoption => {
                f.write_str("Native kernel start requires its explicit adoption")
            }
            Self::LateInstall => f.write_str("Native preparation cannot install after rollback"),
            Self::Panicked(phase) => write!(f, "Native preparation exit phase {phase:?} panicked"),
        }
    }
}
impl std::error::Error for NativePreparedOwnerFailure {}
impl From<NativePreparedOwnerFailure> for ExecutionFailure {
    fn from(cause: NativePreparedOwnerFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl FailureProjection for NativePreparedOwnerFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::DuplicateRoot => (FailureClass::Invariant, 9640),
            Self::DuplicateKernel => (FailureClass::Invariant, 9641),
            Self::OfferWithoutPrepared => (FailureClass::Invariant, 9642),
            Self::AdoptWithoutOffer => (FailureClass::Invariant, 9643),
            Self::StartWithoutAdoption => (FailureClass::Invariant, 9644),
            Self::LateInstall => (FailureClass::Invariant, 9646),
            Self::Panicked(phase) => (FailureClass::Unexpected, 9650 + *phase as u16),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(reason).unwrap()),
        )
    }
}

enum RootBirth {
    NeverStarted,
    Building,
    NoOutputCreated,
    Born(RootPhysicalExitObservation),
}

struct State {
    retained: Option<Arc<Mutex<State>>>,
    rollback: bool,
    root_transferred: bool,
    root_birth: RootBirth,
    kernel: Kernel,
    root: Option<ManuallyDrop<Arc<dyn TaskPreparationChild>>>,
    dormant: Option<ManuallyDrop<DormantFragmentHandle>>,
    running: Option<ManuallyDrop<RunningFragmentHandle>>,
    publisher: Option<ManuallyDrop<FragmentTerminalPublisher>>,
    registration: Option<ManuallyDrop<NativeFragmentRegistrationLease>>,
    // Only real prepared kernel work keeps this capability, never Root metadata.
    control: Option<Arc<TaskExecutionControl>>,
    phases: [Action; PHASES],
    primary: [Option<ManuallyDrop<Payload>>; PHASES],
    secondary: [Option<ManuallyDrop<Payload>>; PHASES],
    publication: [Option<ManuallyDrop<Payload>>; PHASES],
}
/// One finite owner, registered before Root work can commit. Its original
/// preparation scope or explicit runnable offer retains it until handoff/exit.
pub(crate) struct NativePreparationChild {
    identity: TaskIdentity,
    health: Arc<ExecutionHealth>,
    state: Arc<Mutex<State>>,
    self_weak: std::sync::Weak<Self>,
    kernel_stopped: std::sync::atomic::AtomicBool,
    #[cfg(novarocks_dev_controls)]
    prepared_exit_gate: Mutex<Option<super::prepared_exit_control::PreparedExitGate>>,
}
impl NativePreparationChild {
    pub(crate) fn register(
        scope: &TaskPreparationContext<'_>,
        health: Arc<ExecutionHealth>,
    ) -> ExecutionResult<Arc<Self>> {
        let child = Arc::new_cyclic(|weak| Self {
            identity: scope.runtime_control().identity(),
            health,
            state: Arc::new(Mutex::new(State {
                retained: None,
                rollback: false,
                root_transferred: false,
                root_birth: RootBirth::NeverStarted,
                kernel: Kernel::Vacant,
                root: None,
                dormant: None,
                running: None,
                publisher: None,
                registration: None,
                control: None,
                phases: [Action::NotStarted; PHASES],
                primary: std::array::from_fn(|_| None),
                secondary: std::array::from_fn(|_| None),
                publication: std::array::from_fn(|_| None),
            })),
            self_weak: weak.clone(),
            kernel_stopped: std::sync::atomic::AtomicBool::new(false),
            #[cfg(novarocks_dev_controls)]
            prepared_exit_gate: Mutex::new(None),
        });
        scope
            .register_child(child.clone())
            .map_err(ExecutionFailure::from_domain)?;
        Ok(child)
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn inject_prepared_exit_failure(
        &self,
        controls: &crate::test_controls::NativeTestControls,
    ) -> Result<Option<ExecutionFailure>, String> {
        let Some(gate) =
            super::prepared_exit_control::PreparedExitGate::connect(controls, self.identity)?
        else {
            return Ok(None);
        };
        // A concurrent original stop may already have begun rollback while the
        // finite runner connection was established. It wins without a new fault.
        if self.state.lock().unwrap().kernel != Kernel::Prepared {
            return Ok(None);
        }
        let cause = gate.cause().clone();
        let mut slot = self.prepared_exit_gate.lock().unwrap();
        if slot.is_some() {
            return Err("prepared exit gate is already installed".into());
        }
        *slot = Some(gate);
        Ok(Some(cause))
    }

    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn offer_available(&self) -> bool {
        let state = self.state.lock().unwrap();
        !state.rollback && state.kernel == Kernel::Prepared
    }
    /// The original physical constructor reserves this once before opening a
    /// channel. Rollback and Root birth use this same original state lock.
    pub(crate) fn begin_root_birth(&self) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.rollback {
            return Err(NativePreparedOwnerFailure::LateInstall.into());
        }
        if !matches!(state.root_birth, RootBirth::NeverStarted) {
            return Err(NativePreparedOwnerFailure::DuplicateRoot.into());
        }
        state.root_birth = RootBirth::Building;
        Ok(())
    }
    pub(crate) fn root_birth_metadata_bytes() -> usize {
        std::mem::size_of::<RootBirth>()
    }
    /// A typed failed try_open returned no channel. Panic or lost construction
    /// remains Building and cannot manufacture NoOutputCreated.
    pub(crate) fn root_birth_refused(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(matches!(state.root_birth, RootBirth::Building));
        state.root_birth = RootBirth::NoOutputCreated;
    }
    /// Record the exact original physical metadata before any later fallible
    /// session construction. Even rollback winning now cannot lose this birth.
    pub(crate) fn record_root_birth(&self, channel: &RootResultChannel) {
        assert_eq!(channel.spec().task, self.identity);
        let observation = channel.physical_exit_observation();
        let mut state = self.state.lock().unwrap();
        assert!(matches!(state.root_birth, RootBirth::Building));
        state.root_birth = RootBirth::Born(observation);
    }
    pub(crate) fn root_birth_matches(&self, channel: &RootResultChannel) -> bool {
        let state = self.state.lock().unwrap();
        matches!(&state.root_birth, RootBirth::Born(original) if original.belongs_to(channel))
    }
    fn output_exit_after_root_drop(&self) -> TaskPreparationChildExit {
        let state = self.state.lock().unwrap();
        if state.root_transferred {
            return TaskPreparationChildExit::Exited {
                output: TaskPreparationOutputExit::Unknown,
            };
        }
        if !state.rollback {
            // A still-open birth gate is not a stable no-output observation.
            return TaskPreparationChildExit::Pending;
        }
        match &state.root_birth {
            RootBirth::NeverStarted | RootBirth::NoOutputCreated => {
                TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::NoOutputCreated,
                }
            }
            RootBirth::Born(original) if original.output_exited() => {
                TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::ActualOutputExited,
                }
            }
            RootBirth::Born(_) => TaskPreparationChildExit::Pending,
            RootBirth::Building => {
                drop(state);
                self.fault(Phase::RootOutput, None, None);
                TaskPreparationChildExit::Unresolved
            }
        }
    }
    pub(crate) fn install_root(
        &self,
        slot: &mut Option<Arc<dyn TaskPreparationChild>>,
    ) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.rollback {
            return Err(NativePreparedOwnerFailure::LateInstall.into());
        }
        if !matches!(state.root_birth, RootBirth::Born(_)) {
            return Err(NativePreparedOwnerFailure::LateInstall.into());
        }
        if state.root.is_some() {
            return Err(NativePreparedOwnerFailure::DuplicateRoot.into());
        }
        state.root = slot.take().map(ManuallyDrop::new);
        Ok(())
    }
    /// One linear construction token closes the slot before Core can build a
    /// second kernel. It grants ownership placement, never memory capacity.
    pub(crate) fn reserve_kernel(self: &Arc<Self>) -> ExecutionResult<PreparedKernelConstruction> {
        let mut state = self.state.lock().unwrap();
        if state.rollback {
            return Err(NativePreparedOwnerFailure::LateInstall.into());
        }
        if state.kernel != Kernel::Vacant {
            return Err(NativePreparedOwnerFailure::DuplicateKernel.into());
        }
        state.kernel = Kernel::Building;
        Ok(PreparedKernelConstruction {
            owner: self.clone(),
            installed: false,
        })
    }
    pub(crate) fn take_exchange_retirement(
        &self,
    ) -> Option<novarocks_execution::runtime::fragment::ExchangeReceiverRetirement> {
        self.state
            .lock()
            .unwrap()
            .dormant
            .as_mut()
            .expect("installed prepared kernel")
            .take_exchange_retirement()
    }
    pub(crate) fn offer(self: &Arc<Self>) -> ExecutionResult<PreparedKernelOffer> {
        let mut state = self.state.lock().unwrap();
        if state.rollback {
            return Err(NativePreparedOwnerFailure::LateInstall.into());
        }
        if state.kernel != Kernel::Prepared {
            return Err(NativePreparedOwnerFailure::OfferWithoutPrepared.into());
        }
        state.kernel = Kernel::Offered;
        Ok(PreparedKernelOffer {
            owner: self.clone(),
        })
    }
    fn claim(&self, phase: Phase) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.phases[phase as usize] != Action::NotStarted {
            return false;
        }
        state.phases[phase as usize] = Action::Started;
        true
    }
    fn exited(&self, phase: Phase) {
        self.state.lock().unwrap().phases[phase as usize] = Action::Exited;
    }
    fn fault(&self, phase: Phase, payload: Option<Payload>, cause: Option<ExecutionFailure>) {
        let payload = payload.map(ManuallyDrop::new);
        {
            let mut state = self.state.lock().unwrap();
            state.phases[phase as usize] = Action::Unresolved;
            state.primary[phase as usize] = payload;
            if matches!(
                phase,
                Phase::KernelStart
                    | Phase::KernelPoll
                    | Phase::KernelHolder
                    | Phase::Registration
                    | Phase::Control
                    | Phase::KernelObserve
            ) {
                state.kernel = Kernel::Unresolved;
            }
        }
        // This is the original finite owner; never a process-global parking list.
        self.state.lock().unwrap().retained = Some(Arc::clone(&self.state));
        let cause = cause.unwrap_or_else(|| NativePreparedOwnerFailure::Panicked(phase).into());
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase: TaskFailurePhase::Preparation,
            },
            cause.failure_fact(),
        );
        let control = self.state.lock().unwrap().control.clone();
        if let Some(control) = control
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| control.report_cleanup(&cause)))
        {
            self.state.lock().unwrap().publication[phase as usize] =
                Some(ManuallyDrop::new(payload));
        }
        let mut primary = self.state.lock().unwrap().primary[phase as usize].take();
        if primary.is_some() {
            let result = catch_unwind(AssertUnwindSafe(|| unsafe {
                ManuallyDrop::drop(primary.as_mut().unwrap());
            }));
            unsafe {
                std::ptr::write(&mut primary, None);
            }
            if let Err(payload) = result {
                self.state.lock().unwrap().secondary[phase as usize] =
                    Some(ManuallyDrop::new(payload));
            }
        }
    }
    fn root_action(&self, phase: Phase, action: impl FnOnce(&dyn TaskPreparationChild)) -> bool {
        if !self.claim(phase) {
            return self.state.lock().unwrap().phases[phase as usize] == Action::Exited;
        }
        let root = self.state.lock().unwrap().root.as_deref().cloned();
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(root) = &root {
                action(root.as_ref());
            }
        }));
        drop(root); // The original MD slot still retains the sole foreign holder.
        match result {
            Ok(()) => {
                self.exited(phase);
                true
            }
            Err(payload) => {
                self.fault(phase, Some(payload), None);
                false
            }
        }
    }
    fn poll_root(&self) -> TaskPreparationChildExit {
        if self.state.lock().unwrap().root_transferred {
            return if self
                .kernel_stopped
                .load(std::sync::atomic::Ordering::Acquire)
            {
                TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::Unknown,
                }
            } else {
                TaskPreparationChildExit::Pending
            };
        }
        if self.state.lock().unwrap().phases[Phase::RootPoll as usize] == Action::Unresolved {
            return TaskPreparationChildExit::Unresolved;
        }
        {
            let mut state = self.state.lock().unwrap();
            match state.phases[Phase::RootPoll as usize] {
                Action::Started => return TaskPreparationChildExit::Pending,
                Action::Exited => {
                    return TaskPreparationChildExit::Exited {
                        output: TaskPreparationOutputExit::Unknown,
                    };
                }
                Action::Unresolved => return TaskPreparationChildExit::Unresolved,
                Action::NotStarted => state.phases[Phase::RootPoll as usize] = Action::Started,
            }
        }
        let root = self.state.lock().unwrap().root.as_deref().cloned();
        let result = catch_unwind(AssertUnwindSafe(|| {
            root.as_ref().map_or(
                TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::Unknown,
                },
                |root| root.poll_exit(),
            )
        }));
        drop(root);
        match result {
            Ok(result) => {
                self.state.lock().unwrap().phases[Phase::RootPoll as usize] = match result {
                    TaskPreparationChildExit::Pending => Action::NotStarted,
                    TaskPreparationChildExit::Exited { .. } => Action::Exited,
                    TaskPreparationChildExit::Unresolved => Action::Unresolved,
                };
                result
            }
            Err(payload) => {
                self.fault(Phase::RootPoll, Some(payload), None);
                TaskPreparationChildExit::Unresolved
            }
        }
    }
    fn drop_root(&self) -> bool {
        if !self.claim(Phase::RootHolder) {
            return self.state.lock().unwrap().phases[Phase::RootHolder as usize] == Action::Exited;
        }
        let mut root = self.state.lock().unwrap().root.take();
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(root) = root.as_mut() {
                unsafe {
                    ManuallyDrop::drop(root);
                }
            }
        }));
        unsafe {
            std::ptr::write(&mut root, None);
        }
        match result {
            Ok(()) => {
                self.exited(Phase::RootHolder);
                true
            }
            Err(payload) => {
                self.fault(Phase::RootHolder, Some(payload), None);
                false
            }
        }
    }
    fn start_rollback(&self) {
        let (previous, control) = {
            let mut state = self.state.lock().unwrap();
            if !state.rollback
                || !matches!(
                    state.kernel,
                    Kernel::Prepared | Kernel::Offered | Kernel::Adopted
                )
            {
                return;
            }
            let previous = state.kernel;
            state.kernel = Kernel::Starting;
            state.phases[Phase::KernelStart as usize] = Action::Started;
            (
                previous,
                state
                    .control
                    .as_ref()
                    .expect("prepared kernel control")
                    .clone(),
            )
        };
        // Even the exact Task port is borrowed outside our state lock. An
        // unexpected port unwind cannot poison the original kernel holder.
        match catch_unwind(AssertUnwindSafe(|| control.is_stopped())) {
            Ok(true) => {}
            Ok(false) => {
                let mut state = self.state.lock().unwrap();
                state.kernel = previous;
                state.phases[Phase::KernelStart as usize] = Action::NotStarted;
                return;
            }
            Err(payload) => {
                self.fault(Phase::KernelStart, Some(payload), None);
                return;
            }
        }
        #[cfg(novarocks_dev_controls)]
        {
            let mut slot = self.prepared_exit_gate.lock().unwrap();
            if let Some(gate) = slot.as_mut() {
                let same = control
                    .accepted()
                    .and_then(|accepted| accepted.originating_failure().cloned())
                    .is_some_and(|cause| std::ptr::eq(cause.cause(), gate.cause().cause()));
                if same && !gate.poll_open() {
                    let mut state = self.state.lock().unwrap();
                    state.kernel = previous;
                    state.phases[Phase::KernelStart as usize] = Action::NotStarted;
                    return;
                }
                // Cancellation or another earlier failure must never be held
                // behind an action whose own originating cause lost acceptance.
                slot.take();
            }
        }
        let dormant = self
            .state
            .lock()
            .unwrap()
            .dormant
            .take()
            .map(ManuallyDrop::into_inner);
        let mut slot = dormant;
        let result = catch_unwind(AssertUnwindSafe(|| {
            DormantFragmentHandle::try_drain_stopped(&mut slot)
        }));
        // Restore any unconsumed original before health/diagnostic publication.
        if let Some(dormant) = slot.take() {
            self.state.lock().unwrap().dormant = Some(ManuallyDrop::new(dormant));
        }
        match result {
            Ok(Ok(running)) => {
                let mut state = self.state.lock().unwrap();
                state.running = Some(ManuallyDrop::new(running));
                state.kernel = Kernel::Draining;
                state.phases[Phase::KernelStart as usize] = Action::Exited;
                drop(state);
                let _ = self.bind_publisher(None);
            }
            Ok(Err(cause)) => self.fault(Phase::KernelStart, None, Some(cause)),
            Err(payload) => self.fault(Phase::KernelStart, Some(payload), None),
        }
    }
    fn finish_kernel(&self) -> TaskPreparationChildExit {
        if self.claim(Phase::KernelHolder) {
            let mut running = self.state.lock().unwrap().running.take();
            let result = catch_unwind(AssertUnwindSafe(|| {
                if let Some(running) = running.as_mut() {
                    unsafe {
                        ManuallyDrop::drop(running);
                    }
                }
            }));
            unsafe {
                std::ptr::write(&mut running, None);
            }
            match result {
                Ok(()) => self.exited(Phase::KernelHolder),
                Err(payload) => self.fault(Phase::KernelHolder, Some(payload), None),
            }
        }
        self.finish_registration();
        let mut state = self.state.lock().unwrap();
        let exited = [Phase::KernelHolder, Phase::Registration, Phase::Control]
            .iter()
            .all(|phase| state.phases[*phase as usize] == Action::Exited);
        if exited {
            state.kernel = Kernel::Exited;
            return TaskPreparationChildExit::Exited {
                output: TaskPreparationOutputExit::Unknown,
            };
        }
        if [Phase::KernelHolder, Phase::Registration, Phase::Control]
            .iter()
            .any(|phase| state.phases[*phase as usize] == Action::Unresolved)
        {
            TaskPreparationChildExit::Unresolved
        } else {
            TaskPreparationChildExit::Pending
        }
    }
    fn finish_registration(&self) {
        // Core's actual terminal publication owns this independent native
        // settlement even if a sibling Root fault stops Worker child polling.
        if self.claim(Phase::Registration) {
            let mut registration = self.state.lock().unwrap().registration.take();
            let result = catch_unwind(AssertUnwindSafe(|| {
                if let Some(registration) = registration.as_mut() {
                    unsafe {
                        ManuallyDrop::drop(registration);
                    }
                }
            }));
            unsafe {
                std::ptr::write(&mut registration, None);
            }
            match result {
                Ok(()) => self.exited(Phase::Registration),
                Err(payload) => self.fault(Phase::Registration, Some(payload), None),
            }
        }
        if self.claim(Phase::Control) {
            let control = self.state.lock().unwrap().control.take();
            match catch_unwind(AssertUnwindSafe(|| drop(control))) {
                Ok(()) => self.exited(Phase::Control),
                Err(payload) => self.fault(Phase::Control, Some(payload), None),
            }
        }
    }
    fn bind_publisher(
        &self,
        completion: Option<novarocks_worker::TaskCompletionSignal>,
    ) -> ExecutionResult<()> {
        if !self.claim(Phase::KernelObserve) {
            return Err(NativePreparedOwnerFailure::StartWithoutAdoption.into());
        }
        let rollback = completion.is_none();
        let weak = self.self_weak.clone();
        let publisher: FragmentTerminalPublisher = Box::new(move |fact| {
            if let Some(owner) = weak.upgrade() {
                owner
                    .kernel_stopped
                    .store(true, std::sync::atomic::Ordering::Release);
                if rollback {
                    owner.finish_registration();
                }
            }
            if let Some(completion) = &completion {
                completion.publish(fact.clone());
            }
        });
        self.state.lock().unwrap().publisher = Some(ManuallyDrop::new(publisher));
        let mut publisher = self
            .state
            .lock()
            .unwrap()
            .publisher
            .take()
            .map(ManuallyDrop::into_inner);
        let running = (**self.state.lock().unwrap().running.as_ref().unwrap()).clone();
        let result = catch_unwind(AssertUnwindSafe(|| {
            running.bind_terminal_publisher(&mut publisher)
        }));
        drop(running); // The original finite slot retains the actual holder.
        if let Some(publisher) = publisher.take() {
            self.state.lock().unwrap().publisher = Some(ManuallyDrop::new(publisher));
        }
        match result {
            Ok(Ok(())) => {
                self.exited(Phase::KernelObserve);
                Ok(())
            }
            Ok(Err(cause)) => {
                self.fault(Phase::KernelObserve, None, Some(cause));
                Err(NativePreparedOwnerFailure::Panicked(Phase::KernelObserve).into())
            }
            Err(payload) => {
                self.fault(Phase::KernelObserve, Some(payload), None);
                Err(NativePreparedOwnerFailure::Panicked(Phase::KernelObserve).into())
            }
        }
    }
    fn poll_kernel(&self) -> TaskPreparationChildExit {
        self.start_rollback();
        let running = {
            let mut state = self.state.lock().unwrap();
            match state.kernel {
                Kernel::Vacant if state.rollback => {
                    state.kernel = Kernel::Exited;
                    return TaskPreparationChildExit::Exited {
                        output: TaskPreparationOutputExit::Unknown,
                    };
                }
                Kernel::Exited => {
                    return TaskPreparationChildExit::Exited {
                        output: TaskPreparationOutputExit::Unknown,
                    };
                }
                Kernel::Unresolved => return TaskPreparationChildExit::Unresolved,
                Kernel::Transferred => {
                    return if self
                        .kernel_stopped
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        TaskPreparationChildExit::Exited {
                            output: TaskPreparationOutputExit::Unknown,
                        }
                    } else {
                        TaskPreparationChildExit::Pending
                    };
                }
                Kernel::Draining => state.running.as_ref().map(|running| (**running).clone()),
                _ => return TaskPreparationChildExit::Pending,
            }
        };
        {
            let mut state = self.state.lock().unwrap();
            match state.phases[Phase::KernelPoll as usize] {
                Action::Started => return TaskPreparationChildExit::Pending,
                Action::Unresolved => return TaskPreparationChildExit::Unresolved,
                Action::Exited => {
                    drop(state);
                    return self.finish_kernel();
                }
                Action::NotStarted => state.phases[Phase::KernelPoll as usize] = Action::Started,
            }
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            running.as_ref().unwrap().stopped_fact()
        }));
        drop(running); // Still protected by the original running slot.
        match result {
            Ok(Some(_)) => {
                self.exited(Phase::KernelPoll);
                self.finish_kernel()
            }
            Ok(None) => {
                self.state.lock().unwrap().phases[Phase::KernelPoll as usize] = Action::NotStarted;
                TaskPreparationChildExit::Pending
            }
            Err(payload) => {
                self.fault(Phase::KernelPoll, Some(payload), None);
                TaskPreparationChildExit::Unresolved
            }
        }
    }
    fn adopt(&self) -> ExecutionResult<()> {
        {
            let mut state = self.state.lock().unwrap();
            if state.rollback {
                return Err(NativePreparedOwnerFailure::LateInstall.into());
            }
            match state.kernel {
                Kernel::Offered => state.kernel = Kernel::Adopted,
                Kernel::Adopted => {}
                _ => return Err(NativePreparedOwnerFailure::AdoptWithoutOffer.into()),
            }
        }
        if !self.root_action(Phase::RootAdopt, |root| root.adopt_runtime()) {
            return Err(NativePreparedOwnerFailure::AdoptWithoutOffer.into());
        }
        if !self.drop_root() {
            return Err(NativePreparedOwnerFailure::AdoptWithoutOffer.into());
        }
        // Explicit handoff to this kernel's RootResultSink exit contract.
        // An empty root Option never supplies a later physical exit receipt.
        self.state.lock().unwrap().root_transferred = true;
        Ok(())
    }
    fn start_adopted(
        &self,
        initial_failure: Option<ExecutionFailure>,
    ) -> ExecutionResult<RunningFragmentHandle> {
        self.adopt()?; // Explicit committed offer also serves direct host callers.
        let dormant = {
            let mut state = self.state.lock().unwrap();
            if state.kernel != Kernel::Adopted {
                return Err(NativePreparedOwnerFailure::StartWithoutAdoption.into());
            }
            state.kernel = Kernel::Starting;
            state.phases[Phase::KernelStart as usize] = Action::Started;
            state
                .dormant
                .take()
                .expect("adopted owner retains its exact dormant")
        };
        let mut dormant = Some(dormant);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let dormant = ManuallyDrop::into_inner(dormant.take().unwrap());
            match initial_failure {
                Some(cause) => dormant.start_failed(cause),
                None => dormant.start(),
            }
        }));
        if let Some(dormant) = dormant.take() {
            self.state.lock().unwrap().dormant = Some(dormant);
        }
        let running = match result {
            Ok(running) => running,
            Err(payload) => {
                self.fault(Phase::KernelStart, Some(payload), None);
                return Err(NativePreparedOwnerFailure::Panicked(Phase::KernelStart).into());
            }
        };
        let returned = running.clone();
        let mut state = self.state.lock().unwrap();
        state.running = Some(ManuallyDrop::new(running));
        state.kernel = Kernel::RuntimeStarting;
        Ok(returned)
    }
    fn finish_handoff(
        &self,
        completion: novarocks_worker::TaskCompletionSignal,
    ) -> ExecutionResult<()> {
        if self.state.lock().unwrap().kernel != Kernel::RuntimeStarting {
            return Err(NativePreparedOwnerFailure::StartWithoutAdoption.into());
        }
        self.bind_publisher(Some(completion))?;
        // The original lease's into_running is pure: its Drop sees active=false
        // and cannot publish a pre-ready rollback snapshot.
        let registration = self
            .state
            .lock()
            .unwrap()
            .registration
            .take()
            .expect("one original registration transfer");
        ManuallyDrop::into_inner(registration).into_running();
        self.exited(Phase::Registration);
        // The exact NativeRunnable was attached before this handoff. Releasing
        // this handle therefore cannot be the last foreign Running holder.
        if self.claim(Phase::KernelHolder) {
            let running = self.state.lock().unwrap().running.take();
            drop(running.map(ManuallyDrop::into_inner));
            self.exited(Phase::KernelHolder);
        }
        if self.claim(Phase::Control) {
            let control = self.state.lock().unwrap().control.take();
            match catch_unwind(AssertUnwindSafe(|| drop(control))) {
                Ok(()) => self.exited(Phase::Control),
                Err(payload) => {
                    self.fault(Phase::Control, Some(payload), None);
                    return Err(NativePreparedOwnerFailure::Panicked(Phase::Control).into());
                }
            }
        }
        let mut state = self.state.lock().unwrap();
        state.phases[Phase::KernelStart as usize] = Action::Exited;
        state.kernel = Kernel::Transferred;
        Ok(())
    }
}
impl Drop for NativePreparationChild {
    fn drop(&mut self) {
        let vacant = self.state.lock().unwrap().kernel == Kernel::Vacant;
        if vacant {
            // With no installed kernel this is only Root exit metadata. The
            // original pool/issuer still own the actual job, including after
            // its Task reporter exits. Dropping this metadata proves no exit
            // and must not turn their ordinary Pending into a health incident.
            self.drop_root();
            return;
        }
        let remaining = {
            let state = self.state.lock().unwrap();
            state.root.is_some()
                || state.dormant.is_some()
                || state.running.is_some()
                || state.registration.is_some()
                || state.publisher.is_some()
                || state.control.is_some()
        };
        if remaining {
            // Last-owner loss with live work is not an asynchronous cleanup
            // service. Keep the exact finite original state addressable.
            self.fault(
                Phase::KernelHolder,
                None,
                Some(NativePreparedOwnerFailure::StartWithoutAdoption.into()),
            );
        }
    }
}
impl TaskPreparationChild for NativePreparationChild {
    fn request_rollback(&self) {
        self.state.lock().unwrap().rollback = true;
        self.root_action(Phase::RootRollback, |root| root.request_rollback());
        self.start_rollback();
    }
    fn poll_exit(&self) -> TaskPreparationChildExit {
        // Always advance both independent members before merging the result.
        let root = self.poll_root();
        let kernel = self.poll_kernel();
        let unknown = self
            .state
            .lock()
            .unwrap()
            .phases
            .contains(&Action::Unresolved);
        if unknown
            || root == TaskPreparationChildExit::Unresolved
            || kernel == TaskPreparationChildExit::Unresolved
        {
            return TaskPreparationChildExit::Unresolved;
        }
        if root == TaskPreparationChildExit::Pending || kernel == TaskPreparationChildExit::Pending
        {
            return TaskPreparationChildExit::Pending;
        }
        if self.drop_root() {
            // This Root child held metadata itself. Only its actual Drop can
            // precede the original full-output observation, avoiding self-wait.
            self.output_exit_after_root_drop()
        } else {
            TaskPreparationChildExit::Unresolved
        }
    }
    fn adopt_runtime(&self) {
        if let Err(cause) = self.adopt() {
            // Root handoff failure does not invalidate the independently
            // retained dormant kernel. A later stopped rollback must still
            // submit it through its original executor cleanup path.
            let already_reported = self
                .state
                .lock()
                .unwrap()
                .phases
                .contains(&Action::Unresolved);
            if !already_reported {
                self.fault(Phase::RootAdopt, None, Some(cause));
            }
            std::panic::panic_any(NativePreparationUnresolved);
        }
    }
}
/// A fieldless framework signal. Actual panic payloads remain in the original
/// named finite slots, never in Task sources or another unwinding owner.
#[derive(Debug)]
pub(crate) struct NativePreparationUnresolved;

pub(crate) struct PreparedKernelConstruction {
    owner: Arc<NativePreparationChild>,
    installed: bool,
}
impl PreparedKernelConstruction {
    /// Infallible pure transfer from the one actual Core construction. All
    /// validation occurred before that construction could create live drivers.
    pub(crate) fn install(
        mut self,
        dormant: DormantFragmentHandle,
        registration: NativeFragmentRegistrationLease,
        control: Arc<TaskExecutionControl>,
    ) {
        let mut state = self.owner.state.lock().unwrap();
        state.dormant = Some(ManuallyDrop::new(dormant));
        state.registration = Some(ManuallyDrop::new(registration));
        state.control = Some(control);
        if state.kernel != Kernel::Unresolved {
            state.kernel = Kernel::Prepared;
        }
        self.installed = true;
    }
}
impl Drop for PreparedKernelConstruction {
    fn drop(&mut self) {
        if !self.installed {
            let mut state = self.owner.state.lock().unwrap();
            if state.kernel == Kernel::Building {
                state.kernel = Kernel::Vacant;
            }
        }
    }
}
pub(crate) struct PreparedKernelOffer {
    owner: Arc<NativePreparationChild>,
}
impl PreparedKernelOffer {
    pub(crate) fn start(
        &self,
        initial_failure: Option<ExecutionFailure>,
    ) -> ExecutionResult<RunningFragmentHandle> {
        self.owner.start_adopted(initial_failure)
    }
    pub(crate) fn finish_handoff(
        &self,
        completion: novarocks_worker::TaskCompletionSignal,
    ) -> ExecutionResult<()> {
        self.owner.finish_handoff(completion)
    }
}
