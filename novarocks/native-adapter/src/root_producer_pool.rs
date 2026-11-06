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
//! A process-owned finite CPU pool. Jobs own no pool Arc; registrations hold
//! only Weak<Core>. Commit pins the real work until all once cleanup phases
//! and the final work reference actually exit. The process credit covers the fixed Rust
//! construction shapes below, not libc/TLS/guard pages or whole-process RSS.
use std::alloc::Layout;
use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::Waker;
use std::thread::{self, JoinHandle, Thread, ThreadId};

use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution::runtime::task::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_worker::result_buffer::ResultRetainedBudget;

const MAX_THREADS: usize = 64;
const MAX_POSITIONS: usize = 4096;
const MIN_STACK: usize = 1024 * 1024;
const MAX_STACK: usize = 8 * MIN_STACK;
thread_local! {
    static CURRENT_POOL: Cell<usize> = const { Cell::new(0) };
}

// Domain cleanup is a closed, immutable plan, supplied before installing work.
// No getter is called under a pool lock and execution never grows fault storage.
type PanicPayload = Box<dyn std::any::Any + Send + 'static>;
#[derive(Clone, Copy)]
pub(crate) struct RootCleanupPlan(&'static [u16]);
impl RootCleanupPlan {
    pub(crate) const fn new(phases: &'static [u16]) -> Self {
        Self(phases)
    }
}
/// A read-only receipt for this invocation's original cleanup prefix. The
/// pool issues it from completed callbacks, never from emptied owner fields
/// or the overall job failure bit. Independent later phases may ignore it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RootCleanupPredecessors {
    all_exited: bool,
}
impl RootCleanupPredecessors {
    pub(crate) const fn all_exited(self) -> bool {
        self.all_exited
    }
}
pub(crate) trait RootProducerJob: Send + Sync {
    fn turn(&self) -> RootProducerTurn;
    fn cancel(&self);
    /// Each supplied synchronous phase is invoked once. The domain must move
    /// each real holder into its own once state before foreign code, retain the
    /// original input/scratch grant on unknown exit, and return its exact
    /// receipt. This is not the whole job/issuer observation or a polling hook.
    fn cleanup_step(
        &self,
        phase: u16,
        predecessors: RootCleanupPredecessors,
    ) -> RootCleanupProgress;
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootCleanupProgress {
    /// This one supplied cleanup obligation actually exited.
    Exited,
    /// This obligation has unknown exit; later independent phases still run.
    /// The original job/slot/Resources remain retained permanently.
    Unresolved,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootProducerTurn {
    Yielded,
    Blocked,
    Idle,
    Complete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootProducerExitStatus {
    Pending,
    NoJobInstalled,
    Exited,
    Unresolved,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootExitOwner {
    Preparation,
    Runtime,
}
impl RootExitOwner {
    fn index(self) -> usize {
        match self {
            Self::Preparation => 0,
            Self::Runtime => 1,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootExitWatchError {
    DifferentWaker,
    MetadataUnavailable,
}
// Exactly two named owners, not a per-driver queue: the unique Root sink
// factory shares one Runtime metadata Waker across all <=64 root drivers.
const EXIT_ROLES: usize = 2;
const META_FAULTS: usize = EXIT_ROLES * 3; // clone, wake, holder destruction

enum RootExitMetadata {
    Root {
        _reservation: Arc<novarocks_worker::root_result_channel::RootMetadataReservation>,
    },
    #[cfg(test)]
    Test { _credit: ResultWriteCredit },
}
struct ExitState {
    status: RootProducerExitStatus,
    wakers: [Option<Waker>; EXIT_ROLES],
    metadata_failed: bool,
    faults: [Option<ManuallyDrop<PanicPayload>>; META_FAULTS],
}
struct ExitCell {
    state: Mutex<ExitState>,
    core: Weak<Core>,
    health: Arc<ExecutionHealth>,
    ticket: Ticket,
    // The original Root metadata grant follows the actual observation tail.
    _metadata: RootExitMetadata,
}
#[derive(Clone)]
pub(crate) struct RootProducerExitObservation(Arc<ExitCell>);
impl RootProducerExitObservation {
    pub(crate) fn status(&self) -> RootProducerExitStatus {
        self.0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .status
    }
    /// Only the private Preparation and Runtime metadata Wake implementations
    /// may register: no Task/runtime/input authority or arbitrary domain Fn.
    /// Same-role polling is serial and cannot replace an existing factory.
    pub(crate) fn poll(
        &self,
        role: RootExitOwner,
        waker: &Waker,
    ) -> Result<RootProducerExitStatus, RootExitWatchError> {
        let index = role.index();
        {
            let state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.status != RootProducerExitStatus::Pending {
                return Ok(state.status);
            }
            if state.metadata_failed {
                return Err(RootExitWatchError::MetadataUnavailable);
            }
            if let Some(old) = &state.wakers[index] {
                if old.will_wake(waker) {
                    return Ok(state.status);
                }
                drop(state);
                self.metadata_fault(index * 3, None);
                return Err(RootExitWatchError::DifferentWaker);
            }
        }
        let cloned = match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
            Ok(cloned) => cloned,
            Err(payload) => {
                self.metadata_fault(index * 3, Some(payload));
                return Err(RootExitWatchError::MetadataUnavailable);
            }
        };
        let unused = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.status == RootProducerExitStatus::Pending
                && !state.metadata_failed
                && state.wakers[index].is_none()
            {
                state.wakers[index] = Some(cloned);
                None
            } else {
                Some(cloned)
            }
        };
        if let Some(unused) = unused {
            self.drop_waker(index, unused);
        }
        Ok(self.status())
    }
    pub(crate) fn clear_preparation(&self) {
        let waker = self
            .0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .wakers[0]
            .take();
        if let Some(waker) = waker {
            self.drop_waker(0, waker);
        }
    }
    fn metadata_fault(&self, cell: usize, payload: Option<PanicPayload>) {
        {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            state.metadata_failed = true;
        }
        if let Some(core) = self.0.core.upgrade() {
            let mut state = core.state.lock().expect("root producer pool lock");
            state.record_failure(&core.health, RootProducerPoolFailure::Metadata);
            if let Some(slot) = state
                .slots
                .get_mut(self.0.ticket.index)
                .filter(|slot| slot.generation == self.0.ticket.generation)
            {
                slot.metadata_failed = true;
            }
            core.changed.notify_all();
        } else {
            // Observation tails keep the runtime's original neutral latch even
            // after the pool Core has normally exited. No Task source is held.
            self.0.health.report_unresolved(
                ExecutionExitOrigin::Process,
                RootProducerPoolFailure::Metadata.failure_fact(),
            );
        }
        if let Some(payload) = payload
            && let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload)))
        {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let entry = &mut state.faults[cell];
            // A failed role is disabled; each role's same-phase operation
            // has only one caller before disablement under its contract.
            assert!(
                entry.is_none(),
                "one secondary metadata fault per role phase"
            );
            *entry = Some(ManuallyDrop::new(secondary));
        }
    }
    fn drop_waker(&self, role: usize, waker: Waker) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waker))) {
            self.metadata_fault(role * 3 + 2, Some(payload));
        }
    }
    fn publish(&self, status: RootProducerExitStatus) {
        let wakers = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                state.status,
                RootProducerExitStatus::Pending,
                "one terminal exit publication"
            );
            state.status = status;
            std::mem::replace(&mut state.wakers, std::array::from_fn(|_| None))
        };
        // The sticky physical proof already exists. These qualified metadata
        // effects cannot undo it or become a second Task conclusion owner.
        for (index, waker) in wakers.into_iter().enumerate() {
            if let Some(waker) = waker {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake_by_ref())) {
                    self.metadata_fault(index * 3 + 1, Some(payload));
                }
                self.drop_waker(index, waker);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootProducerPoolFailure {
    Turn,
    Cancel,
    Cleanup,
    JobDrop,
    Metadata,
    Worker,
}
impl std::fmt::Display for RootProducerPoolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Turn => "root producer turn panicked",
            Self::Cancel => "root producer cancellation panicked",
            Self::Cleanup => "root producer cleanup phase did not establish exit",
            Self::JobDrop => "root producer work destruction panicked",
            Self::Metadata => "root producer metadata observation failed",
            Self::Worker => "root producer worker exited unexpectedly",
        })
    }
}
impl std::error::Error for RootProducerPoolFailure {}
impl FailureProjection for RootProducerPoolFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::Turn => (FailureClass::Unexpected, 611),
            Self::Cancel => (FailureClass::Unexpected, 612),
            Self::Cleanup => (FailureClass::Unexpected, 613),
            Self::JobDrop => (FailureClass::Unexpected, 614),
            // Only qualified private metadata Wakes enter this owner. A
            // different factory or an unwinding operation violates that
            // closed metadata contract; no text inspection classifies it.
            Self::Metadata => (FailureClass::Invariant, 615),
            Self::Worker => (FailureClass::Unexpected, 616),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
#[derive(Clone, Copy)]
struct Ticket {
    index: usize,
    generation: u64,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Phase {
    #[default]
    Free,
    Reserved,
    Installed,
    Running,
    Cleanup,
    Disposing,
    Unresolved,
}
#[derive(Default)]
struct Slot {
    generation: u64,
    phase: Phase,
    job: Option<Arc<dyn RootProducerJob>>,
    exit: Option<RootProducerExitObservation>,
    plan: &'static [u16],
    faults: Vec<Option<ManuallyDrop<PanicPayload>>>,
    queued: bool,
    running: bool,
    woken: bool,
    cleaning: bool,
    activated: bool,
    next_cleanup: usize,
    // An in-flight phase is represented by running + next_cleanup. Only a
    // normal Exited callback advances this prefix; an unknown gap stays open.
    cleanup_exited_through: usize,
    cancel_requested: bool,
    cancel_called: bool,
    execution_failed: bool,
    metadata_failed: bool,
    late_metadata_panic: Option<ManuallyDrop<PanicPayload>>,
}
struct State {
    slots: Vec<Slot>,
    ready: VecDeque<Ticket>,
    stopping: bool,
    failure: Option<RootProducerPoolFailure>,
    worker_payloads: [Option<ManuallyDrop<PanicPayload>>; MAX_THREADS],
    retained_resources: Option<Resources>,
    custody: Option<Arc<Core>>,
}
impl State {
    fn record_failure(&mut self, health: &ExecutionHealth, failure: RootProducerPoolFailure) {
        if self.failure.is_none() {
            self.failure = Some(failure);
            // OnceLock copies only neutral facts. This cannot call foreign
            // cleanup, format a source, or select/replace a Task conclusion.
            health.report_unresolved(ExecutionExitOrigin::Process, failure.failure_fact());
        }
    }
}
struct Core {
    state: Mutex<State>,
    changed: Condvar,
    health: Arc<ExecutionHealth>,
}
struct Resources {
    handles: Vec<JoinHandle<()>>,
    _credit: ResultWriteCredit,
    _budget: Arc<ResultRetainedBudget>,
}
pub struct RootProducerPool {
    core: Arc<Core>,
    resources: Mutex<Option<Resources>>,
    // A service reference carries no retained credit. Fixture admission must
    // not acquire the resources mutex that shutdown holds across worker joins.
    #[cfg(test)]
    fixture_budget: Arc<ResultRetainedBudget>,
    reserved_bytes: usize,
}
pub(crate) struct RootProducerReservation {
    core: Weak<Core>,
    ticket: Ticket,
    exit: RootProducerExitObservation,
    armed: bool,
}
pub(crate) struct RootProducerRegistration {
    core: Weak<Core>,
    ticket: Ticket,
    exit: RootProducerExitObservation,
}
#[derive(Clone)]
pub(crate) struct RootProducerWake {
    core: Weak<Core>,
    ticket: Ticket,
    _exit: RootProducerExitObservation,
}
pub(crate) struct RootProducerCommitRefusal {
    pub cause: RootProducerControlFailure,
    pub work: Arc<dyn RootProducerJob>,
}
#[derive(Debug)]
pub(crate) enum RootProducerReserveFailure {
    Control(RootProducerControlFailure),
    Layout {
        steps: usize,
        source: std::alloc::LayoutError,
    },
    Allocation {
        requested_bytes: usize,
        source: std::collections::TryReserveError,
    },
    Capacity {
        required: usize,
        actual: usize,
    },
    DuplicatePhase(u16),
}
impl std::fmt::Display for RootProducerReserveFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Layout { steps, source } => write!(
                f,
                "root cleanup plan with {steps} steps has an invalid backing layout: {source}"
            ),
            Self::Allocation {
                requested_bytes,
                source,
            } => write!(
                f,
                "root cleanup backing could not reserve {requested_bytes} bytes: {source}"
            ),
            Self::DuplicatePhase(id) => write!(f, "root cleanup plan repeats phase {id}"),
            Self::Capacity { required, actual } => write!(
                f,
                "root cleanup backing capacity {actual} differs from requested {required}"
            ),
        }
    }
}
impl std::error::Error for RootProducerReserveFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Layout { source, .. } => Some(source),
            Self::Allocation { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootProducerReserveFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        if let Self::Control(cause) = self {
            return cause.failure_fact();
        }
        let (class, code) = match self {
            Self::Layout { .. } => (FailureClass::Invariant, 607),
            Self::Allocation { .. } => (FailureClass::Resource, 608),
            Self::Capacity { .. } => (FailureClass::Invariant, 609),
            Self::DuplicatePhase(_) => (FailureClass::Invariant, 610),
            Self::Control(_) => unreachable!(),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::Allocation {
                requested_bytes, ..
            } => fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Process),
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested_bytes).ok(),
                required_growth: u64::try_from(*requested_bytes).ok(),
                limit: None,
                live: None,
                committed: None,
            }),
            Self::Capacity { required, actual } => fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Process),
                dimension: ResourceDimension::Items,
                requested: u64::try_from(*required).ok(),
                required_growth: u64::try_from(*required).ok(),
                limit: None,
                live: None,
                committed: u64::try_from(*actual).ok(),
            }),
            _ => fact,
        }
    }
}
/// Runtime pool control facts. Startup/CLI diagnostics have a separate
/// lifetime and do not grant Task recovery or rewrite an accepted conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootProducerControlFailure {
    Exited,
    Stopping,
    RegistrationExpired,
    JobExited,
    Positions { limit: usize },
    Unavailable,
}
impl std::fmt::Display for RootProducerControlFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Exited => "root producer pool has exited",
            Self::Stopping => "root producer pool is shut down",
            Self::RegistrationExpired => "root producer registration has expired",
            Self::JobExited => "root producer job has exited",
            Self::Positions { .. } => "root producer pool positions are exhausted",
            Self::Unavailable => "root producer pool is unhealthy",
        })
    }
}
impl std::error::Error for RootProducerControlFailure {}
impl novarocks_execution_contract::failure::FailureProjection for RootProducerControlFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::Exited => (FailureClass::Control, 600),
            Self::Stopping => (FailureClass::Control, 601),
            Self::RegistrationExpired => (FailureClass::Control, 602),
            Self::JobExited => (FailureClass::Control, 603),
            Self::Positions { .. } => (FailureClass::Resource, 604),
            Self::Unavailable => (FailureClass::Unexpected, 606),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        );
        if let Self::Positions { limit } = self {
            fact.with_resource(ResourceFacts {
                dimension: ResourceDimension::Positions,
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Process),
                requested: Some(1),
                required_growth: None,
                limit: u64::try_from(*limit).ok(),
                live: None,
                committed: None,
            })
        } else {
            fact
        }
    }
}

fn add(total: &mut usize, bytes: usize) -> Result<(), String> {
    *total = total
        .checked_add(bytes)
        .ok_or("root producer pool layout overflow")?;
    Ok(())
}
fn array<T>(count: usize) -> Result<usize, String> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| "root producer pool array layout overflow".into())
}
fn arc_layout<T>() -> Result<usize, String> {
    Layout::new::<[usize; 2]>()
        .extend(Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| "root producer pool Arc layout overflow".into())
}
/// Exact requested Arc<Core> allocation under pinned Rust 1.92: repr(C)
/// ArcInner's two AtomicUsize counters followed by the aligned Core value.
/// Weak handles retain this entire allocation after Core's fields have dropped;
/// they do not retain the slots/queue heap backings, jobs or worker stacks.
/// Every registration/callback owner independently pregrants this upper tail
/// before registration and retains its guard until its last actual token Drop.
/// It is allocation provenance only, not a second budget or whole-source proof.
pub(crate) fn weak_core_backing_bytes() -> usize {
    arc_layout::<Core>().expect("fixed root producer Core Arc layout fits usize")
}

fn construction_bytes(threads: usize, positions: usize, stack: usize) -> Result<usize, String> {
    // Source-audited Rust 1.92 std::thread::spawn_unchecked_ + Unix Thread::new:
    // ThreadInner Arc (unnamed CString option, id, Darwin's pointer+i8 Parker
    // upper bound also covers Linux's u32 Parker), Packet<()> Arc, main Box
    // (Core Arc, Thread, Packet Arc, empty ChildSpawnHooks), Unix ThreadData Box.
    // Stable 1.92 cannot install unstable spawn hooks. No thread name is set.
    // These are Layout upper bounds for requested Rust allocations. Darwin's
    // dispatch semaphore and libc TLS/stack cache are external platform facts.
    // Unix may raise stack to TLS minimum/round pages: only requested stack is
    // proved here. Host acceptance must separately verify its platform envelope.
    let mut total = arc_layout::<RootProducerPool>()?;
    // Darwin std 1.92 lazily Box-pins pthread primitives: two mutexes
    // (signature + 56 bytes each) and one condvar (signature + 40 bytes).
    // Initialize them serially before workers/publication to exclude duplicate
    // OnceBox candidates. Linux uses inline futex primitives instead.
    #[cfg(target_vendor = "apple")]
    {
        add(&mut total, array::<(isize, [u8; 56])>(2)?)?;
        add(&mut total, Layout::new::<(isize, [u8; 40])>().size())?;
    }
    add(&mut total, arc_layout::<Core>()?)?;
    add(&mut total, array::<Slot>(positions)?)?;
    add(&mut total, array::<Ticket>(positions)?)?;
    add(&mut total, array::<JoinHandle<()>>(threads)?)?;
    add(&mut total, array::<RootProducerRegistration>(positions)?)?;
    add(
        &mut total,
        Layout::new::<Weak<ResultRetainedBudget>>().size(),
    )?;
    let mut per_thread = stack;
    add(
        &mut per_thread,
        arc_layout::<(Option<CString>, ThreadId, (usize, i8))>()?,
    )?;
    add(
        &mut per_thread,
        arc_layout::<(Option<Arc<()>>, UnsafeCell<Option<thread::Result<()>>>)>()?,
    )?;
    add(
        &mut per_thread,
        Layout::new::<(
            Arc<Core>,
            Thread,
            Arc<()>,
            Option<Arc<()>>,
            Vec<Box<dyn FnOnce() + Send>>,
        )>()
        .size(),
    )?;
    add(
        &mut per_thread,
        Layout::new::<(Option<Box<str>>, Box<dyn FnOnce()>)>().size(),
    )?;
    // Each worker can concurrently hold one popped ticket and one Arc clone.
    add(
        &mut per_thread,
        Layout::new::<(Ticket, Arc<dyn RootProducerJob>)>().size(),
    )?;
    add(
        &mut total,
        per_thread
            .checked_mul(threads)
            .ok_or("root producer thread layout overflow")?,
    )?;
    Ok(total)
}
/// Validated local CPU resources, independent of task transport capacity.
/// This does not advertise result transport or product support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootProducerLimits {
    threads: NonZeroUsize,
    positions: NonZeroUsize,
    stack_bytes: usize,
}
impl RootProducerLimits {
    pub fn try_new(threads: usize, positions: usize, stack_bytes: usize) -> Result<Self, String> {
        let threads = NonZeroUsize::new(threads).ok_or("root producer threads must be positive")?;
        let positions =
            NonZeroUsize::new(positions).ok_or("root producer positions must be positive")?;
        if threads.get() > MAX_THREADS
            || positions.get() > MAX_POSITIONS
            || !(MIN_STACK..=MAX_STACK).contains(&stack_bytes)
        {
            return Err(
                "root producer pool requires threads<=64, positions<=4096 and stack in 1..=8 MiB"
                    .into(),
            );
        }
        Ok(Self {
            threads,
            positions,
            stack_bytes,
        })
    }
    pub fn threads(self) -> NonZeroUsize {
        self.threads
    }
    pub fn positions(self) -> NonZeroUsize {
        self.positions
    }
    pub fn stack_bytes(self) -> usize {
        self.stack_bytes
    }
}
impl RootProducerPool {
    #[expect(
        clippy::disallowed_methods,
        reason = "The original Root producer pool owns and joins its workers; see ADR-0218."
    )]
    pub fn try_new(
        threads: NonZeroUsize,
        positions: NonZeroUsize,
        stack_bytes: usize,
        budget: Arc<ResultRetainedBudget>,
        health: Arc<ExecutionHealth>,
    ) -> Result<Arc<Self>, String> {
        let limits = RootProducerLimits::try_new(threads.get(), positions.get(), stack_bytes)?;
        let threads = limits.threads().get();
        let positions = limits.positions().get();
        if !cfg!(all(
            target_pointer_width = "64",
            any(target_vendor = "apple", target_os = "linux")
        )) {
            return Err("root producer pool layout audit requires 64-bit Darwin or Linux".into());
        }
        // Supply a 64 KiB-aligned stack request, covering the audited Darwin
        // 16 KiB and Linux <=64 KiB page rounding without post-grant growth.
        let stack_bytes = stack_bytes
            .checked_add(65535)
            .ok_or("root producer stack layout overflow")?
            & !65535;
        let reserved_bytes = construction_bytes(threads, positions, stack_bytes)?;
        let credit = match budget
            .try_reserve_process(reserved_bytes)
            .map_err(|cause| cause.to_string())?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => {
                return Err("root producer pool process capacity is exhausted".into());
            }
        };
        // try_reserve_exact on these Rust 1.92 RawVec owners allocates the
        // requested Layout; there is no growth after accepting registrations.
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(positions)
            .map_err(|_| "root producer slot allocation failed")?;
        slots.resize_with(positions, Slot::default);
        let mut ready = VecDeque::new();
        ready
            .try_reserve_exact(positions)
            .map_err(|_| "root producer queue allocation failed")?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(threads)
            .map_err(|_| "root producer handle allocation failed")?;
        let core = Arc::new(Core {
            state: Mutex::new(State {
                slots,
                ready,
                stopping: false,
                failure: None,
                worker_payloads: std::array::from_fn(|_| None),
                retained_resources: None,
                custody: None,
            }),
            changed: Condvar::new(),
            health,
        });
        drop(core.state.lock().expect("root producer initial state lock"));
        core.changed.notify_all();
        for _ in 0..threads {
            let worker = Arc::clone(&core);
            match thread::Builder::new()
                .stack_size(stack_bytes)
                .spawn(move || run(worker))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    stop(&core);
                    let mut unknown = false;
                    for (index, handle) in handles.drain(..).enumerate() {
                        if let Err(payload) = handle.join() {
                            unknown = true;
                            core.state
                                .lock()
                                .expect("root producer pool lock")
                                .worker_payloads[index] = Some(ManuallyDrop::new(payload));
                        }
                    }
                    if unknown {
                        let mut state = core.state.lock().expect("root producer pool lock");
                        state.record_failure(&core.health, RootProducerPoolFailure::Worker);
                        state.retained_resources = Some(Resources {
                            handles,
                            _credit: credit,
                            _budget: budget,
                        });
                        state.custody = Some(Arc::clone(&core));
                    } else {
                        drop(core);
                        drop(credit);
                    }
                    return Err(format!("root producer thread spawn failed: {error}"));
                }
            }
        }
        let pool = Arc::new(Self {
            core,
            #[cfg(test)]
            fixture_budget: Arc::clone(&budget),
            resources: Mutex::new(Some(Resources {
                handles,
                _credit: credit,
                _budget: budget,
            })),
            reserved_bytes,
        });
        drop(
            pool.resources
                .lock()
                .expect("root producer initial resources lock"),
        );
        Ok(pool)
    }
}
pub(crate) fn root_exit_metadata_bytes(
    plan: RootCleanupPlan,
) -> Result<usize, std::alloc::LayoutError> {
    let count = plan
        .0
        .len()
        .checked_add(3)
        .ok_or_else(|| Layout::array::<u8>(usize::MAX).unwrap_err())?;
    let mut bytes = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<ExitCell>())?
        .0
        .pad_to_align()
        .size();
    bytes = bytes
        .checked_add(Layout::array::<Option<ManuallyDrop<PanicPayload>>>(count)?.size())
        .ok_or_else(|| Layout::array::<u8>(usize::MAX).unwrap_err())?;
    #[cfg(target_vendor = "apple")]
    {
        bytes += Layout::new::<(isize, [u8; 56])>().size();
    }
    Ok(bytes)
}
impl RootProducerPool {
    pub(crate) fn reserve(
        &self,
        plan: RootCleanupPlan,
        metadata: Arc<novarocks_worker::root_result_channel::RootMetadataReservation>,
    ) -> Result<RootProducerReservation, RootProducerReserveFailure> {
        self.reserve_inner(
            plan,
            RootExitMetadata::Root {
                _reservation: metadata,
            },
        )
    }
    fn reserve_inner(
        &self,
        plan: RootCleanupPlan,
        metadata: RootExitMetadata,
    ) -> Result<RootProducerReservation, RootProducerReserveFailure> {
        {
            let state = self.core.state.lock().expect("root producer pool lock");
            if state.stopping {
                return Err(RootProducerReserveFailure::Control(
                    RootProducerControlFailure::Stopping,
                ));
            }
            if state.failure.is_some() {
                return Err(RootProducerReserveFailure::Control(
                    RootProducerControlFailure::Unavailable,
                ));
            }
        }
        for (index, id) in plan.0.iter().enumerate() {
            if plan.0[..index].contains(id) {
                return Err(RootProducerReserveFailure::DuplicatePhase(*id));
            }
        }
        root_exit_metadata_bytes(plan).map_err(|source| RootProducerReserveFailure::Layout {
            steps: plan.0.len(),
            source,
        })?;
        let count = plan.0.len() + 3;
        let requested_bytes = Layout::array::<Option<ManuallyDrop<PanicPayload>>>(count)
            .map_err(|source| RootProducerReserveFailure::Layout {
                steps: plan.0.len(),
                source,
            })?
            .size();
        let mut faults = Vec::new();
        faults.try_reserve_exact(count).map_err(|source| {
            RootProducerReserveFailure::Allocation {
                requested_bytes,
                source,
            }
        })?;
        if faults.capacity() != count {
            return Err(RootProducerReserveFailure::Capacity {
                required: count,
                actual: faults.capacity(),
            });
        }
        faults.resize_with(count, || None);
        let mut state = self.core.state.lock().expect("root producer pool lock");
        if state.stopping {
            return Err(RootProducerReserveFailure::Control(
                RootProducerControlFailure::Stopping,
            ));
        }
        if state.failure.is_some() {
            return Err(RootProducerReserveFailure::Control(
                RootProducerControlFailure::Unavailable,
            ));
        }
        let limit = state.slots.len();
        let (index, slot) = state
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.phase == Phase::Free && slot.generation != u64::MAX)
            .ok_or(RootProducerReserveFailure::Control(
                RootProducerControlFailure::Positions { limit },
            ))?;
        let generation = slot.generation + 1;
        let ticket = Ticket { index, generation };
        let exit = RootProducerExitObservation(Arc::new(ExitCell {
            state: Mutex::new(ExitState {
                status: RootProducerExitStatus::Pending,
                wakers: std::array::from_fn(|_| None),
                metadata_failed: false,
                faults: std::array::from_fn(|_| None),
            }),
            core: Arc::downgrade(&self.core),
            health: Arc::clone(&self.core.health),
            ticket,
            _metadata: metadata,
        }));
        *slot = Slot {
            generation,
            phase: Phase::Reserved,
            exit: Some(exit.clone()),
            plan: plan.0,
            faults,
            ..Slot::default()
        };
        Ok(RootProducerReservation {
            core: Arc::downgrade(&self.core),
            ticket,
            exit,
            armed: true,
        })
    }
    // Fixtures use the same charged reservation state machine. This is not
    // a production admission alternative and never installs a weak work.
    #[cfg(test)]
    pub(crate) fn reserve_for_test(
        &self,
        plan: RootCleanupPlan,
    ) -> Result<RootProducerReservation, RootProducerReserveFailure> {
        let budget = &self.fixture_budget;
        let bytes = root_exit_metadata_bytes(plan).map_err(|source| {
            RootProducerReserveFailure::Layout {
                steps: plan.0.len(),
                source,
            }
        })?;
        let ResultWriteAdmission::Granted(credit) = budget
            .try_reserve_process(bytes)
            .expect("fixture metadata reservation")
        else {
            panic!("fixture metadata must be actually funded");
        };
        self.reserve_inner(plan, RootExitMetadata::Test { _credit: credit })
    }
    pub(crate) fn execution_health(&self) -> Arc<ExecutionHealth> {
        Arc::clone(&self.core.health)
    }
    pub fn poll_failure(&self) -> Option<RootProducerPoolFailure> {
        self.core
            .state
            .lock()
            .expect("root producer pool lock")
            .failure
    }
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
    pub fn shutdown(&self) -> Result<(), String> {
        if on_worker(&self.core) {
            return Err("root producer worker cannot join its own pool".into());
        }
        stop(&self.core);
        let mut resources = self.resources.lock().expect("root producer resources lock");
        if let Some(resources) = resources.as_mut() {
            for (index, handle) in resources.handles.drain(..).enumerate() {
                if let Err(payload) = handle.join() {
                    let mut state = self.core.state.lock().expect("root producer pool lock");
                    state.record_failure(&self.core.health, RootProducerPoolFailure::Worker);
                    state.worker_payloads[index] = Some(ManuallyDrop::new(payload));
                }
            }
        }
        self.poll_failure().map_or(Ok(()), |e| Err(e.to_string()))
    }
}
fn on_worker(core: &Arc<Core>) -> bool {
    CURRENT_POOL.with(|p| p.get() == Arc::as_ptr(core) as usize)
}
impl Drop for RootProducerPool {
    fn drop(&mut self) {
        if on_worker(&self.core) {
            stop(&self.core);
        } else {
            let _ = self.shutdown();
        }
        let resources = self
            .resources
            .get_mut()
            .expect("root producer resources lock")
            .take();
        let mut state = self.core.state.lock().expect("root producer pool lock");
        if state.slots.iter().any(|s| s.phase != Phase::Free)
            || state.worker_payloads.iter().any(Option::is_some)
        {
            state.retained_resources = resources;
            state.custody = Some(Arc::clone(&self.core));
        }
    }
}
impl RootProducerReservation {
    pub(crate) fn wake_handle(&self) -> RootProducerWake {
        RootProducerWake {
            core: self.core.clone(),
            ticket: self.ticket,
            _exit: self.exit.clone(),
        }
    }
    pub(crate) fn exit_observation(&self) -> RootProducerExitObservation {
        self.exit.clone()
    }
    pub(crate) fn commit(
        mut self,
        work: Arc<dyn RootProducerJob>,
    ) -> Result<RootProducerRegistration, RootProducerCommitRefusal> {
        let Some(core) = self.core.upgrade() else {
            return Err(RootProducerCommitRefusal {
                cause: RootProducerControlFailure::Exited,
                work,
            });
        };
        let cause =
            {
                let mut state = core.state.lock().expect("root producer pool lock");
                if state.stopping {
                    Some(RootProducerControlFailure::Stopping)
                } else if state.failure.is_some() {
                    Some(RootProducerControlFailure::Unavailable)
                } else if let Some(slot) = state.slots.get_mut(self.ticket.index).filter(|s| {
                    s.generation == self.ticket.generation && s.phase == Phase::Reserved
                }) {
                    slot.job = Some(work.clone());
                    slot.phase = Phase::Installed;
                    if slot.cancel_requested {
                        slot.queued = true;
                        state.ready.push_back(self.ticket);
                    }
                    None
                } else {
                    Some(RootProducerControlFailure::RegistrationExpired)
                }
            };
        if let Some(cause) = cause {
            return Err(RootProducerCommitRefusal { cause, work });
        }
        // This local reference is never the last one: the original slot now
        // strongly owns the installed work before a facade can be published.
        drop(work);
        self.armed = false;
        core.changed.notify_all();
        Ok(RootProducerRegistration {
            core: self.core.clone(),
            ticket: self.ticket,
            exit: self.exit.clone(),
        })
    }
}
impl Drop for RootProducerReservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(core) = self.core.upgrade() {
            withdraw(&core, self.ticket);
        }
    }
}
impl RootProducerRegistration {
    #[cfg(test)]
    pub(crate) fn exit_observation(&self) -> RootProducerExitObservation {
        self.exit.clone()
    }
    pub(crate) fn wake_handle(&self) -> RootProducerWake {
        RootProducerWake {
            core: self.core.clone(),
            ticket: self.ticket,
            _exit: self.exit.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn wake(&self) -> Result<(), RootProducerControlFailure> {
        self.wake_handle().wake()
    }
}
impl Drop for RootProducerRegistration {
    fn drop(&mut self) {
        let _ = self.wake_handle().request_cancel();
    }
}
impl RootProducerWake {
    /// Pure metadata request, including while construction owns a Reserved
    /// position. It neither invokes a foreign hook nor proves physical exit.
    pub(crate) fn request_cancel(&self) -> Result<(), RootProducerControlFailure> {
        let core = self
            .core
            .upgrade()
            .ok_or(RootProducerControlFailure::Exited)?;
        let mut state = core.state.lock().expect("root producer pool lock");
        let slot = state
            .slots
            .get_mut(self.ticket.index)
            .filter(|slot| slot.generation == self.ticket.generation && slot.phase != Phase::Free)
            .ok_or(RootProducerControlFailure::RegistrationExpired)?;
        slot.cancel_requested = true;
        if slot.job.is_some() && !matches!(slot.phase, Phase::Disposing | Phase::Unresolved) {
            queue(&mut state, self.ticket);
        }
        drop(state);
        core.changed.notify_all();
        Ok(())
    }
    pub(crate) fn wake(&self) -> Result<(), RootProducerControlFailure> {
        self.wake_impl(true)
    }
    pub(crate) fn wake_active(&self) -> Result<(), RootProducerControlFailure> {
        self.wake_impl(false)
    }
    fn wake_impl(&self, activate: bool) -> Result<(), RootProducerControlFailure> {
        let core = self
            .core
            .upgrade()
            .ok_or(RootProducerControlFailure::Exited)?;
        let mut state = core.state.lock().expect("root producer pool lock");
        if state.stopping {
            return Err(RootProducerControlFailure::Stopping);
        }
        let slot = state
            .slots
            .get_mut(self.ticket.index)
            .filter(|s| s.generation == self.ticket.generation && s.job.is_some())
            .ok_or(RootProducerControlFailure::RegistrationExpired)?;
        if matches!(slot.phase, Phase::Disposing | Phase::Unresolved) {
            return Err(RootProducerControlFailure::JobExited);
        }
        if !activate && !slot.activated {
            return Ok(());
        }
        slot.activated = true;
        queue(&mut state, self.ticket);
        drop(state);
        core.changed.notify_one();
        Ok(())
    }
}
fn queue(state: &mut State, ticket: Ticket) {
    let slot = &mut state.slots[ticket.index];
    if slot.job.is_none() || matches!(slot.phase, Phase::Disposing | Phase::Unresolved) {
        return;
    }
    if slot.running {
        slot.woken = true;
    } else if !slot.queued {
        slot.queued = true;
        state.ready.push_back(ticket);
    }
}
fn withdraw(core: &Arc<Core>, ticket: Ticket) {
    let exit = {
        let mut state = core.state.lock().expect("root producer pool lock");
        let Some(slot) = state
            .slots
            .get_mut(ticket.index)
            .filter(|s| s.generation == ticket.generation && s.phase == Phase::Reserved)
        else {
            return;
        };
        slot.phase = Phase::Disposing;
        slot.exit.as_ref().unwrap().clone()
    };
    // Only absence of an installed asynchronous CPU work is proved here.
    // Parent preparation still owns/proves its synchronous construction body.
    settle_exit(core, ticket, exit, RootProducerExitStatus::NoJobInstalled);
}
fn settle_exit(
    core: &Arc<Core>,
    ticket: Ticket,
    exit: RootProducerExitObservation,
    status: RootProducerExitStatus,
) {
    exit.publish(status);
    let retired = {
        let mut state = core.state.lock().expect("root producer pool lock");
        let slot = &mut state.slots[ticket.index];
        if slot.execution_failed || slot.metadata_failed {
            slot.phase = Phase::Unresolved;
            slot.running = false;
            slot.queued = false;
            None
        } else {
            slot.phase = Phase::Disposing;
            Some((slot.exit.take(), std::mem::take(&mut slot.faults)))
        }
    };
    if let Some((pooled_exit, faults)) = retired {
        // All cells are None. Free backing while its original grant is held.
        drop(faults);
        drop(pooled_exit); // local exit prevents a last reference Drop here
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(exit))) {
            {
                let mut state = core.state.lock().expect("root producer pool lock");
                state.record_failure(&core.health, RootProducerPoolFailure::Metadata);
                let slot = &mut state.slots[ticket.index];
                slot.metadata_failed = true;
                slot.phase = Phase::Unresolved;
                slot.running = false;
            }
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                core.state.lock().expect("root producer pool lock").slots[ticket.index]
                    .late_metadata_panic = Some(ManuallyDrop::new(secondary));
            }
        } else {
            let mut state = core.state.lock().expect("root producer pool lock");
            let slot = &mut state.slots[ticket.index];
            let generation = slot.generation;
            *slot = Slot {
                generation,
                ..Slot::default()
            };
        }
    }
    core.changed.notify_all();
}
fn execution_unresolved(core: &Arc<Core>, ticket: Ticket, kind: RootProducerPoolFailure) {
    let mut state = core.state.lock().expect("root producer pool lock");
    state.record_failure(&core.health, kind);
    let slot = &mut state.slots[ticket.index];
    // Sticky for this exact installed work. Later independently successful
    // cleanup never clears its responsibility or permits final job disposal.
    slot.execution_failed = true;
    slot.cleaning = true;
}
fn execution_fault(
    core: &Arc<Core>,
    ticket: Ticket,
    kind: RootProducerPoolFailure,
    cell: usize,
    payload: PanicPayload,
) {
    execution_unresolved(core, ticket, kind);
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        let mut state = core.state.lock().expect("root producer pool lock");
        let entry = &mut state.slots[ticket.index].faults[cell];
        assert!(entry.is_none(), "each closed plan phase executes once");
        *entry = Some(ManuallyDrop::new(secondary));
    }
}
fn stop(core: &Arc<Core>) {
    let count = {
        let mut state = core.state.lock().expect("root producer pool lock");
        state.stopping = true;
        let State { slots, ready, .. } = &mut *state;
        for (index, slot) in slots.iter_mut().enumerate() {
            if slot.job.is_some() && !matches!(slot.phase, Phase::Disposing | Phase::Unresolved) {
                slot.cancel_requested = true;
                if slot.running {
                    slot.woken = true;
                } else if !slot.queued {
                    slot.queued = true;
                    ready.push_back(Ticket {
                        index,
                        generation: slot.generation,
                    });
                }
            }
        }
        slots.len()
    };
    for index in 0..count {
        let ticket = {
            let state = core.state.lock().expect("root producer pool lock");
            Ticket {
                index,
                generation: state.slots[index].generation,
            }
        };
        withdraw(core, ticket);
    }
    core.changed.notify_all();
}
#[derive(Clone, Copy)]
enum Call {
    Turn,
    Cancel,
    Cleanup {
        id: u16,
        ordinal: usize,
        predecessors: RootCleanupPredecessors,
    },
    Dispose,
}
fn run(core: Arc<Core>) {
    CURRENT_POOL.with(|p| p.set(Arc::as_ptr(&core) as usize));
    loop {
        let (ticket, call, borrowed) = {
            let mut state = core.state.lock().expect("root producer pool lock");
            loop {
                if let Some(ticket) = state.ready.pop_front() {
                    let slot = &mut state.slots[ticket.index];
                    if slot.generation != ticket.generation || !slot.queued || slot.job.is_none() {
                        continue;
                    }
                    slot.queued = false;
                    slot.running = true;
                    slot.woken = false;
                    let call = if slot.cancel_requested && !slot.cancel_called {
                        slot.cancel_called = true;
                        slot.cleaning = true;
                        Call::Cancel
                    } else if slot.cleaning {
                        slot.phase = Phase::Cleanup;
                        if let Some(id) = slot.plan.get(slot.next_cleanup).copied() {
                            let ordinal = slot.next_cleanup;
                            slot.next_cleanup += 1;
                            Call::Cleanup {
                                id,
                                ordinal,
                                predecessors: RootCleanupPredecessors {
                                    all_exited: slot.cleanup_exited_through == ordinal,
                                },
                            }
                        } else {
                            slot.phase = Phase::Disposing;
                            Call::Dispose
                        }
                    } else {
                        slot.phase = Phase::Running;
                        Call::Turn
                    };
                    let borrowed = if matches!(call, Call::Dispose) {
                        None
                    } else {
                        Some(slot.job.as_ref().unwrap().clone())
                    };
                    break (ticket, call, borrowed);
                }
                if state.stopping
                    && state
                        .slots
                        .iter()
                        .all(|s| !s.running && (s.job.is_none() || s.phase == Phase::Unresolved))
                {
                    CURRENT_POOL.with(|p| p.set(0));
                    return;
                }
                state = core.changed.wait(state).expect("root producer wait lock");
            }
        };
        if matches!(call, Call::Dispose) {
            let (exit, work, failed, cell) = {
                let mut state = core.state.lock().expect("root producer pool lock");
                let slot = &mut state.slots[ticket.index];
                (
                    slot.exit.as_ref().unwrap().clone(),
                    if slot.execution_failed {
                        None
                    } else {
                        slot.job.take()
                    },
                    slot.execution_failed,
                    slot.plan.len() + 2,
                )
            };
            if !failed && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
                execution_fault(
                    &core,
                    ticket,
                    RootProducerPoolFailure::JobDrop,
                    cell,
                    payload,
                );
            }
            let unknown = core.state.lock().expect("root producer pool lock").slots[ticket.index]
                .execution_failed;
            settle_exit(
                &core,
                ticket,
                exit,
                if unknown {
                    RootProducerExitStatus::Unresolved
                } else {
                    RootProducerExitStatus::Exited
                },
            );
            continue;
        }
        let job = borrowed.as_ref().unwrap();
        let mut requeue = true;
        match call {
            Call::Turn => match catch_unwind(AssertUnwindSafe(|| job.turn())) {
                Ok(RootProducerTurn::Complete) => {
                    core.state.lock().expect("root producer pool lock").slots[ticket.index]
                        .cleaning = true
                }
                Ok(RootProducerTurn::Yielded) => {}
                Ok(RootProducerTurn::Blocked | RootProducerTurn::Idle) => requeue = false,
                Err(payload) => {
                    execution_fault(&core, ticket, RootProducerPoolFailure::Turn, 0, payload);
                    core.state.lock().expect("root producer pool lock").slots[ticket.index]
                        .cancel_requested = true;
                }
            },
            Call::Cancel => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| job.cancel())) {
                    execution_fault(&core, ticket, RootProducerPoolFailure::Cancel, 1, payload);
                }
            }
            Call::Cleanup {
                id,
                ordinal,
                predecessors,
            } => match catch_unwind(AssertUnwindSafe(|| job.cleanup_step(id, predecessors))) {
                Ok(RootCleanupProgress::Exited) => {
                    let mut state = core.state.lock().expect("root producer pool lock");
                    let slot = &mut state.slots[ticket.index];
                    if slot.cleanup_exited_through == ordinal {
                        slot.cleanup_exited_through += 1;
                    }
                }
                Ok(RootCleanupProgress::Unresolved) => {
                    execution_unresolved(&core, ticket, RootProducerPoolFailure::Cleanup);
                }
                Err(payload) => execution_fault(
                    &core,
                    ticket,
                    RootProducerPoolFailure::Cleanup,
                    ordinal + 2,
                    payload,
                ),
            },
            Call::Dispose => unreachable!(),
        }
        // The original strong work remains in the slot. This local borrowed
        // reference cannot perform the last job destructor on an unwind path.
        drop(borrowed);
        {
            let mut state = core.state.lock().expect("root producer pool lock");
            let slot = &mut state.slots[ticket.index];
            slot.running = false;
            requeue |= slot.woken || slot.cleaning || slot.cancel_requested;
            slot.phase = if slot.cleaning {
                Phase::Cleanup
            } else {
                Phase::Installed
            };
            if requeue {
                queue(&mut state, ticket);
            }
        }
        core.changed.notify_all();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::Wake;
    use std::time::{Duration, Instant};
    const WAIT: Duration = Duration::from_secs(5);
    const BUDGET: usize = 8 * MIN_STACK;
    const EXIT: RootCleanupPlan = RootCleanupPlan::new(&[0]);
    struct FakeJob {
        turn: Box<dyn Fn() -> RootProducerTurn + Send + Sync>,
        cancel: Box<dyn Fn() + Send + Sync>,
        exit: Box<dyn Fn() + Send + Sync>,
    }
    impl RootProducerJob for FakeJob {
        fn turn(&self) -> RootProducerTurn {
            (self.turn)()
        }
        fn cancel(&self) {
            (self.cancel)()
        }
        fn cleanup_step(
            &self,
            phase: u16,
            _predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            assert_eq!(phase, 0);
            (self.exit)();
            RootCleanupProgress::Exited
        }
    }
    #[derive(Default)]
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }
    impl Gate {
        fn enter(&self) {
            let mut s = self.state.lock().unwrap();
            s.0 = true;
            self.changed.notify_all();
            while !s.1 {
                s = self.changed.wait(s).unwrap();
            }
        }
        fn wait_entered(&self) {
            let s = self.state.lock().unwrap();
            let (s, _) = self.changed.wait_timeout_while(s, WAIT, |s| !s.0).unwrap();
            assert!(s.0, "actual worker must enter controlled work");
        }
        fn release(&self) {
            self.state.lock().unwrap().1 = true;
            self.changed.notify_all();
        }
    }
    struct ReleaseGate(Arc<Gate>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    fn pool(positions: usize) -> (Arc<RootProducerPool>, Arc<ResultRetainedBudget>) {
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(BUDGET).unwrap());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(positions).unwrap(),
            MIN_STACK,
            budget.clone(),
            Arc::new(ExecutionHealth::new()),
        )
        .unwrap();
        (pool, budget)
    }
    fn reserve(
        pool: &RootProducerPool,
        budget: &Arc<ResultRetainedBudget>,
        plan: RootCleanupPlan,
    ) -> Result<RootProducerReservation, RootProducerReserveFailure> {
        let bytes = root_exit_metadata_bytes(plan).unwrap();
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap()
        else {
            panic!("test metadata must be actually reserved");
        };
        pool.reserve_inner(plan, RootExitMetadata::Test { _credit: credit })
    }
    fn install(
        pool: &RootProducerPool,
        budget: &Arc<ResultRetainedBudget>,
        plan: RootCleanupPlan,
        job: Arc<dyn RootProducerJob>,
    ) -> RootProducerRegistration {
        match reserve(pool, budget, plan).unwrap().commit(job) {
            Ok(r) => r,
            Err(e) => panic!("unexpected commit refusal: {}", e.cause),
        }
    }

    fn register(
        pool: &RootProducerPool,
        job: &Arc<dyn RootProducerJob>,
    ) -> RootProducerRegistration {
        let budget = pool
            .resources
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            ._budget
            .clone();
        install(pool, &budget, EXIT, job.clone())
    }
    fn wait_drained(pool: &RootProducerPool) {
        let state = pool.core.state.lock().unwrap();
        let (state, _) = pool
            .core
            .changed
            .wait_timeout_while(state, WAIT, |s| {
                s.slots
                    .iter()
                    .any(|s| s.job.is_some() && s.phase != Phase::Unresolved)
            })
            .unwrap();
        assert!(
            state
                .slots
                .iter()
                .all(|s| s.job.is_none() || s.phase == Phase::Unresolved),
            "all schedulable CPU calls drain; Unresolved is not physical exit"
        );
    }
    // These regressions exercise real once cleanup callbacks and the Native
    // session's exact phase-8 predicate. Field emptiness is deliberately a
    // different oracle from the original callback's actual return.
    struct CleanupCapture {
        phase: u16,
        panic_phase: Option<u16>,
        gate: Option<Arc<Gate>>,
        drops: Arc<[AtomicUsize; 2]>,
    }
    impl Drop for CleanupCapture {
        fn drop(&mut self) {
            if let Some(gate) = &self.gate {
                gate.enter();
            }
            self.drops[usize::from(self.phase - 6)].fetch_add(1, Ordering::SeqCst);
            assert_ne!(
                self.panic_phase,
                Some(self.phase),
                "original cleanup capture destructor panic"
            );
        }
    }
    struct PrefixJob {
        client: Mutex<Option<CleanupCapture>>,
        builder: Mutex<Option<CleanupCapture>>,
        producer: Mutex<Option<novarocks_worker::root_result_channel::RootProducerExit>>,
        receipts: Arc<[AtomicUsize; 4]>,
        independent: Arc<AtomicUsize>,
        turn_panics: bool,
        unknown_phase: Option<u16>,
    }
    impl RootProducerJob for PrefixJob {
        fn turn(&self) -> RootProducerTurn {
            assert!(
                !self.turn_panics,
                "original turn panic before successful cleanup"
            );
            RootProducerTurn::Complete
        }
        fn cancel(&self) {}
        fn cleanup_step(
            &self,
            phase: u16,
            predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            self.receipts[usize::from(phase - 6)].store(
                if predecessors.all_exited() { 1 } else { 2 },
                Ordering::SeqCst,
            );
            match phase {
                6 => {
                    let capture = self.client.lock().unwrap().take();
                    drop(capture);
                }
                7 => {
                    let capture = self.builder.lock().unwrap().take();
                    drop(capture);
                }
                8 => {
                    if !crate::root_result_session::encoder_cleanup_ready(
                        predecessors,
                        false,
                        self.builder.lock().unwrap().is_some(),
                    ) {
                        return RootCleanupProgress::Unresolved;
                    }
                    let producer = self.producer.lock().unwrap().take();
                    if let Some(producer) = producer {
                        assert!(producer.finish_encoder().is_none());
                    }
                }
                9 => {
                    self.independent.fetch_add(1, Ordering::SeqCst);
                }
                _ => unreachable!("closed test cleanup plan"),
            }
            if self.unknown_phase == Some(phase) {
                RootCleanupProgress::Unresolved
            } else {
                RootCleanupProgress::Exited
            }
        }
    }
    fn prefix_channel() -> Arc<novarocks_worker::root_result_channel::RootResultChannel> {
        use novarocks_execution::runtime::task::io::RootResultWriteSpec;
        use novarocks_execution_contract::TaskIdentity;
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
            256 * 1024 * 1024,
            4 * 1024 * 1024 * 1024,
        )
        .unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        novarocks_worker::root_result_channel::RootResultChannel::try_open(
            RootResultWriteSpec {
                task: TaskIdentity::new(
                    QueryExecutionId::new(QueryId::new(117001, 117002), AttemptId::new(1).unwrap())
                        .unwrap(),
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly,
                )),
            },
            budget,
            limits,
            Arc::new(ExecutionHealth::default()),
        )
        .unwrap()
    }
    fn prefix_job(
        channel: &Arc<novarocks_worker::root_result_channel::RootResultChannel>,
        panic_phase: Option<u16>,
        turn_panics: bool,
        unknown_phase: Option<u16>,
        gate: Option<Arc<Gate>>,
    ) -> (Arc<PrefixJob>, Arc<[AtomicUsize; 2]>) {
        let producer = channel.start_producer().unwrap();
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        (
            Arc::new(PrefixJob {
                client: Mutex::new(Some(CleanupCapture {
                    phase: 6,
                    panic_phase,
                    gate: None,
                    drops: drops.clone(),
                })),
                builder: Mutex::new(Some(CleanupCapture {
                    phase: 7,
                    panic_phase,
                    gate,
                    drops: drops.clone(),
                })),
                producer: Mutex::new(Some(producer)),
                receipts: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
                independent: Arc::new(AtomicUsize::new(0)),
                turn_panics,
                unknown_phase,
            }),
            drops,
        )
    }
    #[test]
    fn a_taken_capture_with_panicked_or_unknown_exit_cannot_release_the_real_encoder() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[6, 7, 8, 9]);
        for phase in [6, 7] {
            for panic in [false, true] {
                let (pool, budget) = pool(1);
                let channel = prefix_channel();
                let (job, drops) = prefix_job(
                    &channel,
                    panic.then_some(phase),
                    false,
                    (!panic).then_some(phase),
                    None,
                );
                let registration = install(&pool, &budget, PLAN, job.clone());
                let exit = registration.exit_observation();
                registration.wake().unwrap();
                wait_drained(&pool);
                assert!(pool.shutdown().is_err());
                assert!(
                    job.client.lock().unwrap().is_none() && job.builder.lock().unwrap().is_none()
                );
                assert_eq!(drops[0].load(Ordering::SeqCst), 1);
                assert_eq!(drops[1].load(Ordering::SeqCst), 1);
                assert_eq!(
                    job.receipts[2].load(Ordering::SeqCst),
                    2,
                    "the pool reports the actual incomplete prefix even though both fields are empty"
                );
                assert!(
                    job.producer.lock().unwrap().is_some(),
                    "unknown capture exit retains the original encoder responsibility"
                );
                assert_eq!(
                    channel
                        .try_ownership_snapshot()
                        .unwrap()
                        .unwrap()
                        .producers_exited,
                    0,
                    "the original encoder has not supplied its physical exit"
                );
                assert_eq!(
                    job.independent.load(Ordering::SeqCst),
                    1,
                    "later independent reporting-authority cleanup still runs once"
                );
                assert_eq!(exit.status(), RootProducerExitStatus::Unresolved);
            }
        }
    }
    #[test]
    fn an_in_flight_capture_does_not_advance_the_original_cleanup_prefix() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[6, 7, 8, 9]);
        let (pool, budget) = pool(1);
        let channel = prefix_channel();
        let gate = Arc::new(Gate::default());
        let release = ReleaseGate(gate.clone());
        let (job, drops) = prefix_job(&channel, None, false, None, Some(gate.clone()));
        let registration = install(&pool, &budget, PLAN, job.clone());
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        gate.wait_entered();
        let (running, scheduled, exited) = {
            let state = pool.core.state.lock().unwrap();
            let slot = &state.slots[registration.ticket.index];
            (slot.running, slot.next_cleanup, slot.cleanup_exited_through)
        };
        let field_empty = job.builder.lock().unwrap().is_none();
        let encoder_held = job.producer.lock().unwrap().is_some();
        let observed = exit.status();
        drop(release);
        wait_drained(&pool);
        pool.shutdown().unwrap();
        assert!(running && field_empty && encoder_held);
        assert_eq!(
            (scheduled, exited),
            (2, 1),
            "Started is not Exited while the actual destructor is still running"
        );
        assert_eq!(observed, RootProducerExitStatus::Pending);
        assert_eq!(drops[1].load(Ordering::SeqCst), 1);
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
    }
    #[test]
    fn a_turn_failure_does_not_replace_the_actual_cleanup_prefix_receipt() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[6, 7, 8, 9]);
        let (pool, budget) = pool(1);
        let channel = prefix_channel();
        let (job, drops) = prefix_job(&channel, None, true, None, None);
        let registration = install(&pool, &budget, PLAN, job.clone());
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        wait_drained(&pool);
        assert!(pool.shutdown().is_err());
        assert_eq!(drops[0].load(Ordering::SeqCst), 1);
        assert_eq!(drops[1].load(Ordering::SeqCst), 1);
        for receipt in job.receipts.iter() {
            assert_eq!(receipt.load(Ordering::SeqCst), 1);
        }
        assert!(job.producer.lock().unwrap().is_none());
        assert_eq!(
            channel
                .try_ownership_snapshot()
                .unwrap()
                .unwrap()
                .producers_exited,
            1,
            "the encoder's real captures exited despite a separate unknown turn"
        );
        assert_eq!(job.independent.load(Ordering::SeqCst), 1);
        assert_eq!(
            exit.status(),
            RootProducerExitStatus::Unresolved,
            "successful cleanup never erases the unknown original job obligation"
        );
    }
    #[test]
    fn never_acquired_captures_still_complete_the_original_cleanup_prefix() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[6, 7, 8, 9]);
        let (pool, budget) = pool(1);
        let job = Arc::new(PrefixJob {
            client: Mutex::new(None),
            builder: Mutex::new(None),
            producer: Mutex::new(None),
            receipts: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
            independent: Arc::new(AtomicUsize::new(0)),
            turn_panics: false,
            unknown_phase: None,
        });
        let registration = install(&pool, &budget, PLAN, job.clone());
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        wait_drained(&pool);
        pool.shutdown().unwrap();
        for receipt in job.receipts.iter() {
            assert_eq!(receipt.load(Ordering::SeqCst), 1);
        }
        assert_eq!(job.independent.load(Ordering::SeqCst), 1);
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
    }

    #[test]
    fn a_complete_turn_requires_the_actual_channel_end_before_becoming_completed() {
        use crate::root_result_session::turn_has_published_end;
        let channel = prefix_channel();
        assert!(!turn_has_published_end(
            RootProducerTurn::Complete,
            channel.snapshot().end_sequence
        ));
        let producer = channel.start_producer().unwrap();
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        assert!(turn_has_published_end(
            RootProducerTurn::Complete,
            channel.snapshot().end_sequence
        ));
        assert!(!turn_has_published_end(
            RootProducerTurn::Idle,
            channel.snapshot().end_sequence
        ));
        assert!(producer.finish_encoder().is_none());
    }

    struct MetaWake {
        calls: Arc<AtomicUsize>,
        panic_payload: Option<(Arc<AtomicUsize>, Arc<AtomicUsize>)>,
    }
    impl Wake for MetaWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some((primary, secondary)) = &self.panic_payload {
                std::panic::panic_any(Opaque {
                    dropped: primary.clone(),
                    secondary: Some(secondary.clone()),
                });
            }
        }
    }
    fn metadata_waker(calls: Arc<AtomicUsize>) -> Waker {
        Waker::from(Arc::new(MetaWake {
            calls,
            panic_payload: None,
        }))
    }
    struct Secondary(Arc<AtomicUsize>);
    impl Drop for Secondary {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Opaque {
        dropped: Arc<AtomicUsize>,
        secondary: Option<Arc<AtomicUsize>>,
    }
    impl Drop for Opaque {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            if let Some(secondary) = &self.secondary {
                std::panic::panic_any(Secondary(secondary.clone()));
            }
        }
    }
    struct LastDrop {
        gate: Option<Arc<Gate>>,
        dropped: Arc<AtomicUsize>,
    }
    impl LastDrop {
        fn borrow(&self) {}
    }
    impl Drop for LastDrop {
        fn drop(&mut self) {
            if let Some(gate) = &self.gate {
                gate.enter();
            }
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reservation_withdrawal_proves_only_no_cpu_job_not_parent_construction_exit() {
        let (pool, budget) = pool(1);
        let reservation = reserve(&pool, &budget, EXIT).unwrap();
        let exit = reservation.exit_observation();
        assert_eq!(exit.status(), RootProducerExitStatus::Pending);
        let parent_dropped = Arc::new(AtomicUsize::new(0));
        let parent = LastDrop {
            gate: None,
            dropped: parent_dropped.clone(),
        };
        drop(reservation);
        assert_eq!(exit.status(), RootProducerExitStatus::NoJobInstalled);
        assert_ne!(exit.status(), RootProducerExitStatus::Exited);
        assert_eq!(
            parent_dropped.load(Ordering::SeqCst),
            0,
            "parent synchronous construction is still owned"
        );
        let next = reserve(&pool, &budget, EXIT).unwrap();
        drop(next);
        pool.shutdown().unwrap();
        drop(parent);
        assert_eq!(parent_dropped.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn shutdown_rejects_commit_and_returns_uninstalled_work_to_parent() {
        let (pool, budget) = pool(1);
        let reservation = reserve(&pool, &budget, EXIT).unwrap();
        let exit = reservation.exit_observation();
        pool.shutdown().unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let capture = LastDrop {
            gate: None,
            dropped: dropped.clone(),
        };
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(|| panic!("uninstalled work must never run")),
            cancel: Box::new(|| panic!("uninstalled work has no CPU cancel")),
            exit: Box::new(move || capture.borrow()),
        });
        let refusal = match reservation.commit(job) {
            Ok(_) => panic!("shutdown cannot install work"),
            Err(e) => e,
        };
        assert_eq!(refusal.cause, RootProducerControlFailure::Stopping);
        assert_eq!(exit.status(), RootProducerExitStatus::NoJobInstalled);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(refusal.work);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn installed_dormant_work_is_strong_and_budget_wake_does_not_activate_it() {
        let (pool, budget) = pool(1);
        let turns = Arc::new(AtomicUsize::new(0));
        let t = turns.clone();
        let dropped = Arc::new(AtomicUsize::new(0));
        let capture = LastDrop {
            gate: None,
            dropped: dropped.clone(),
        };
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                t.fetch_add(1, Ordering::SeqCst);
                RootProducerTurn::Complete
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || capture.borrow()),
        });
        let weak = Arc::downgrade(&job);
        let registration = install(&pool, &budget, EXIT, job);
        let exit = registration.exit_observation();
        for _ in 0..100 {
            registration.wake_handle().wake_active().unwrap();
        }
        assert!(weak.upgrade().is_some());
        assert_eq!(turns.load(Ordering::SeqCst), 0);
        drop(registration);
        pool.shutdown().unwrap();
        assert_eq!(turns.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
    }
    #[test]
    fn registration_drop_only_requests_cancel_and_cpu_owner_calls_once_after_turn() {
        let (pool, budget) = pool(1);
        let gate = Arc::new(Gate::default());
        let inside = gate.clone();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let c = cancelled.clone();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let e = cleaned.clone();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                inside.enter();
                RootProducerTurn::Blocked
            }),
            cancel: Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
            exit: Box::new(move || {
                e.fetch_add(1, Ordering::SeqCst);
            }),
        });
        let release = ReleaseGate(gate.clone());
        let registration = install(&pool, &budget, EXIT, job);
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        gate.wait_entered();
        drop(registration);
        let before = cancelled.load(Ordering::SeqCst);
        gate.release();
        pool.shutdown().unwrap();
        drop(release);
        assert_eq!(
            before, 0,
            "foreign cancel must not run inside registration Drop"
        );
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
    }
    #[test]
    fn completing_job_keeps_position_until_its_last_capture_actually_drops() {
        let (pool, budget) = pool(1);
        let turn_gate = Arc::new(Gate::default());
        let capture_gate = Arc::new(Gate::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let exit_calls = Arc::new(AtomicUsize::new(0));
        let calls = exit_calls.clone();
        let final_capture = LastDrop {
            gate: Some(capture_gate.clone()),
            dropped: dropped.clone(),
        };
        let turn = turn_gate.clone();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                turn.enter();
                RootProducerTurn::Complete
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                final_capture.borrow();
            }),
        });
        let _release_turn = ReleaseGate(turn_gate.clone());
        let _release_capture = ReleaseGate(capture_gate.clone());
        let registration = install(&pool, &budget, EXIT, job);
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        turn_gate.wait_entered();
        turn_gate.release();
        capture_gate.wait_entered();
        let admission = match reserve(&pool, &budget, EXIT) {
            Ok(uninstalled) => {
                drop(uninstalled);
                Ok(())
            }
            Err(RootProducerReserveFailure::Control(cause)) => Err(cause),
            Err(e) => panic!("unexpected reserve failure: {e}"),
        };
        let pending = exit.status();
        capture_gate.release();
        let shutdown = pool.shutdown();
        shutdown.unwrap();
        assert_eq!(exit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(pending, RootProducerExitStatus::Pending);
        assert_eq!(
            admission,
            Err(RootProducerControlFailure::Positions { limit: 1 })
        );
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
        drop(registration);
    }
    struct FaultJob {
        fault: usize,
        secondary: bool,
        turns: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
        phases: Arc<[AtomicUsize; 3]>,
        primary: Arc<AtomicUsize>,
        secondary_dropped: Arc<AtomicUsize>,
        work_dropped: Arc<AtomicUsize>,
    }
    impl FaultJob {
        fn fail(&self) {
            std::panic::panic_any(Opaque {
                dropped: self.primary.clone(),
                secondary: self.secondary.then(|| self.secondary_dropped.clone()),
            });
        }
    }
    impl RootProducerJob for FaultJob {
        fn turn(&self) -> RootProducerTurn {
            self.turns.fetch_add(1, Ordering::SeqCst);
            if self.fault == 0 {
                self.fail();
            }
            RootProducerTurn::Complete
        }
        fn cancel(&self) {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            if self.fault == 1 {
                self.fail();
            }
        }
        fn cleanup_step(
            &self,
            phase: u16,
            _predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            self.phases[usize::from(phase)].fetch_add(1, Ordering::SeqCst);
            if self.fault == 2 && phase == 0 {
                self.fail();
            }
            RootCleanupProgress::Exited
        }
    }
    impl Drop for FaultJob {
        fn drop(&mut self) {
            self.work_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn turn_cancel_and_cleanup_panics_keep_real_work_credit_and_drain_safe_siblings() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[0, 1, 2]);
        for fault in 0..3 {
            for secondary in [false, true] {
                let (pool, budget) = pool(2);
                let health = Arc::clone(&pool.core.health);
                assert!(health.is_healthy());
                let turns = Arc::new(AtomicUsize::new(0));
                let cancelled = Arc::new(AtomicUsize::new(0));
                let phases = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
                let primary = Arc::new(AtomicUsize::new(0));
                let secondary_dropped = Arc::new(AtomicUsize::new(0));
                let work_dropped = Arc::new(AtomicUsize::new(0));
                let job = Arc::new(FaultJob {
                    fault,
                    secondary,
                    turns: turns.clone(),
                    cancelled: cancelled.clone(),
                    phases: phases.clone(),
                    primary: primary.clone(),
                    secondary_dropped: secondary_dropped.clone(),
                    work_dropped: work_dropped.clone(),
                }) as Arc<dyn RootProducerJob>;
                let mut bad = Some(install(&pool, &budget, PLAN, job));
                let bad_exit = bad.as_ref().unwrap().exit_observation();
                let good_dropped = Arc::new(AtomicUsize::new(0));
                let capture = LastDrop {
                    gate: None,
                    dropped: good_dropped.clone(),
                };
                let (done, receiver) = mpsc::channel();
                let good: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
                    turn: Box::new(|| RootProducerTurn::Complete),
                    cancel: Box::new(|| {}),
                    exit: Box::new(move || {
                        capture.borrow();
                        done.send(()).unwrap();
                    }),
                });
                let good = install(&pool, &budget, EXIT, good);
                let good_exit = good.exit_observation();
                if fault == 1 {
                    drop(bad.take());
                } else {
                    bad.as_ref().unwrap().wake().unwrap();
                }
                good.wake().unwrap();
                receiver.recv_timeout(WAIT).unwrap();
                assert!(
                    matches!(
                        reserve(&pool, &budget, EXIT),
                        Err(RootProducerReserveFailure::Control(
                            RootProducerControlFailure::Unavailable
                        ))
                    ),
                    "health closes new admission before shutdown but accepted sibling was still drained"
                );
                let shutdown = pool.shutdown();
                drop(bad);
                drop(good);
                assert!(shutdown.is_err());
                assert_eq!(bad_exit.status(), RootProducerExitStatus::Unresolved);
                let original_fault = health
                    .failure()
                    .expect("unknown exit closes runtime health");
                assert_eq!(original_fault.origin(), ExecutionExitOrigin::Process);
                assert_eq!(
                    original_fault.fact(),
                    pool.poll_failure()
                        .expect("same original pool failure")
                        .failure_fact(),
                );
                assert!(!health.is_healthy());
                assert_eq!(good_exit.status(), RootProducerExitStatus::Exited);
                assert_eq!(
                    turns.load(Ordering::SeqCst),
                    if fault == 1 { 0 } else { 1 },
                    "normal turn must never replay after a fault"
                );
                for phase in phases.iter() {
                    assert_eq!(
                        phase.load(Ordering::SeqCst),
                        1,
                        "all independent cleanup phases execute exactly once"
                    );
                }
                assert_eq!(primary.load(Ordering::SeqCst), 1);
                assert_eq!(
                    secondary_dropped.load(Ordering::SeqCst),
                    0,
                    "secondary payload must remain in original finite custody"
                );
                assert_eq!(
                    work_dropped.load(Ordering::SeqCst),
                    0,
                    "unknown real work remains held by its original slot"
                );
                assert_eq!(good_dropped.load(Ordering::SeqCst), 1);
                assert!(matches!(
                    reserve(&pool, &budget, EXIT),
                    Err(RootProducerReserveFailure::Control(
                        RootProducerControlFailure::Stopping
                    ))
                ));
                let metadata = root_exit_metadata_bytes(PLAN).unwrap()
                    + root_exit_metadata_bytes(EXIT).unwrap();
                drop(pool);
                assert!(
                    matches!(
                        budget.try_reserve_process(BUDGET - metadata).unwrap(),
                        ResultWriteAdmission::Blocked
                    ),
                    "original pool credit, beyond exact live Root metadata, remains charged"
                );
                assert_eq!(health.failure(), Some(original_fault));
            }
        }
    }
    #[test]
    fn named_metadata_roles_share_one_runtime_waker_and_clear_preparation_on_handoff() {
        let (pool, budget) = pool(1);
        let registration = install(
            &pool,
            &budget,
            EXIT,
            Arc::new(FakeJob {
                turn: Box::new(|| RootProducerTurn::Complete),
                cancel: Box::new(|| {}),
                exit: Box::new(|| {}),
            }),
        );
        let exit = registration.exit_observation();
        let prep = Arc::new(AtomicUsize::new(0));
        let runtime = Arc::new(AtomicUsize::new(0));
        let pw = metadata_waker(prep.clone());
        let rw = metadata_waker(runtime.clone());
        assert_eq!(
            exit.poll(RootExitOwner::Preparation, &pw).unwrap(),
            RootProducerExitStatus::Pending
        );
        for _ in 0
            ..novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                .root_maximum_root_drivers
        {
            assert_eq!(
                exit.poll(RootExitOwner::Runtime, &rw).unwrap(),
                RootProducerExitStatus::Pending
            );
        }
        exit.clear_preparation();
        registration.wake().unwrap();
        pool.shutdown().unwrap();
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
        assert_eq!(prep.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.load(Ordering::SeqCst), 1);
        assert_eq!(
            exit.poll(RootExitOwner::Runtime, &rw).unwrap(),
            RootProducerExitStatus::Exited
        );
    }
    #[test]
    fn a_second_runtime_factory_is_a_typed_metadata_fault_not_silent_overwrite() {
        let (pool, budget) = pool(1);
        let registration = install(
            &pool,
            &budget,
            EXIT,
            Arc::new(FakeJob {
                turn: Box::new(|| RootProducerTurn::Complete),
                cancel: Box::new(|| {}),
                exit: Box::new(|| {}),
            }),
        );
        let exit = registration.exit_observation();
        let first = metadata_waker(Arc::new(AtomicUsize::new(0)));
        let second = metadata_waker(Arc::new(AtomicUsize::new(0)));
        exit.poll(RootExitOwner::Runtime, &first).unwrap();
        assert_eq!(
            exit.poll(RootExitOwner::Runtime, &second),
            Err(RootExitWatchError::DifferentWaker)
        );
        registration.wake().unwrap();
        assert!(pool.shutdown().is_err());
        assert_eq!(pool.poll_failure(), Some(RootProducerPoolFailure::Metadata));
        assert_eq!(
            exit.status(),
            RootProducerExitStatus::Exited,
            "qualified metadata fault cannot undo proven physical work exit"
        );
    }
    #[test]
    fn metadata_wake_panic_keeps_its_secondary_payload_without_reversing_physical_proof() {
        let (pool, budget) = pool(1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let capture = LastDrop {
            gate: None,
            dropped: dropped.clone(),
        };
        let registration = install(
            &pool,
            &budget,
            EXIT,
            Arc::new(FakeJob {
                turn: Box::new(|| RootProducerTurn::Complete),
                cancel: Box::new(|| {}),
                exit: Box::new(move || capture.borrow()),
            }),
        );
        let exit = registration.exit_observation();
        let calls = Arc::new(AtomicUsize::new(0));
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(MetaWake {
            calls: calls.clone(),
            panic_payload: Some((primary.clone(), secondary.clone())),
        }));
        exit.poll(RootExitOwner::Runtime, &waker).unwrap();
        registration.wake().unwrap();
        let shutdown = pool.shutdown();
        assert!(shutdown.is_err());
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(secondary.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn yield_round_robin_is_fair_and_repeated_queued_wakes_coalesce() {
        let (pool, _) = pool(3);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let log = Arc::new(Mutex::new(Vec::new()));
        let (exited, receiver) = mpsc::channel();
        let mut jobs = Vec::new();
        let mut registrations = Vec::new();
        for id in 0..3 {
            let turns = AtomicUsize::new(0);
            let log = Arc::clone(&log);
            let gate = Arc::clone(&gate);
            let exited = exited.clone();
            let weak_core = Arc::downgrade(&pool.core);
            let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
                turn: Box::new(move || {
                    let ordinal = turns.fetch_add(1, Ordering::SeqCst);
                    log.lock().unwrap().push(id);
                    if id == 0 && ordinal == 0 {
                        gate.enter();
                    }
                    if ordinal == 0 {
                        RootProducerTurn::Yielded
                    } else {
                        RootProducerTurn::Complete
                    }
                }),
                cancel: Box::new(|| {}),
                exit: Box::new(move || {
                    let core = weak_core.upgrade().unwrap();
                    assert!(
                        core.state.try_lock().is_ok(),
                        "exit hook must run outside the pool lock"
                    );
                    exited.send(id).unwrap();
                }),
            });
            registrations.push(register(&pool, &job));
            jobs.push(job);
        }
        registrations[0].wake().unwrap();
        gate.wait_entered();
        registrations[1].wake().unwrap();
        registrations[2].wake().unwrap();
        for _ in 0..100 {
            registrations[1].wake().unwrap();
            registrations[2].wake().unwrap();
        }
        assert_eq!(pool.core.state.lock().unwrap().ready.len(), 2);
        gate.release();
        for _ in 0..3 {
            receiver.recv_timeout(WAIT).unwrap();
        }
        wait_drained(&pool);
        assert_eq!(log.lock().unwrap().as_slice(), &[0, 1, 2, 0, 1, 2]);
        assert!(pool.core.state.lock().unwrap().ready.is_empty());
        pool.shutdown().unwrap();
    }

    #[test]
    fn wake_during_running_blocked_turn_is_not_lost() {
        let (pool, _) = pool(1);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let inside = Arc::clone(&gate);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    inside.enter();
                    RootProducerTurn::Blocked
                } else {
                    RootProducerTurn::Complete
                }
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        gate.wait_entered();
        let callback = registration.wake_handle();
        for _ in 0..100 {
            callback.wake_active().unwrap();
        }
        {
            let state = pool.core.state.lock().unwrap();
            assert!(state.ready.is_empty());
            assert!(state.slots[0].running && state.slots[0].woken);
        }
        gate.release();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        assert_eq!(turns.load(Ordering::SeqCst), 2);
        pool.shutdown().unwrap();
    }

    #[test]
    fn active_budget_wake_resumes_a_parked_blocked_job() {
        let (pool, _) = pool(1);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    RootProducerTurn::Blocked
                } else {
                    RootProducerTurn::Complete
                }
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        {
            let state = pool.core.state.lock().unwrap();
            let (state, _) = pool
                .core
                .changed
                .wait_timeout_while(state, WAIT, |state| {
                    let slot = &state.slots[0];
                    slot.running || slot.queued
                })
                .unwrap();
            assert!(state.slots[0].job.is_some());
            assert!(!state.slots[0].running && !state.slots[0].queued);
        }
        registration.wake_handle().wake_active().unwrap();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        assert_eq!(turns.load(Ordering::SeqCst), 2);
        pool.shutdown().unwrap();
    }

    #[test]
    fn local_configuration_and_process_capacity_reject_before_pool_allocations() {
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(1).unwrap());
        for (threads, positions, stack) in [
            (65, 1, MIN_STACK),
            (1, 4097, MIN_STACK),
            (1, 1, MIN_STACK - 1),
            (1, 1, MAX_STACK + 1),
            (1, 1, MIN_STACK),
        ] {
            assert!(
                RootProducerPool::try_new(
                    NonZeroUsize::new(threads).unwrap(),
                    NonZeroUsize::new(positions).unwrap(),
                    stack,
                    Arc::clone(&budget),
                    Arc::new(ExecutionHealth::new()),
                )
                .is_err()
            );
        }
        assert!(matches!(
            budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
        let (pool, _) = pool(3);
        assert!(pool.reserved_bytes() > MIN_STACK);
        {
            let state = pool.core.state.lock().unwrap();
            assert_eq!(state.slots.capacity(), 3);
            assert_eq!(state.ready.capacity(), 3);
        }
        pool.shutdown().unwrap();
    }

    #[test]
    fn wake_keeps_exact_core_arc_tail_after_shutdown_and_pool_owner_drop() {
        let (pool, budget) = pool(1);
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(|| RootProducerTurn::Complete),
            cancel: Box::new(|| {}),
            exit: Box::new(|| {}),
        });
        let registration = register(&pool, &job);
        let wake = registration.wake_handle();
        let pointer = wake.core.as_ptr();
        let header = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>();
        let (inner, _) = header.extend(Layout::new::<Core>()).unwrap();
        assert_eq!(weak_core_backing_bytes(), inner.pad_to_align().size());
        assert!(weak_core_backing_bytes() >= std::mem::size_of::<Core>() + header.size());
        pool.shutdown().unwrap();
        drop(registration);
        drop(job);
        drop(pool);
        assert_eq!(wake.core.strong_count(), 0);
        assert!(wake.core.upgrade().is_none());
        assert_eq!(
            wake.core.as_ptr(),
            pointer,
            "Weak retains the Arc allocation without retaining Core's live value"
        );
        assert!(wake.wake_active().is_err());
        // The pool credit has returned. The still-live wake retains exactly
        // its original Root metadata grant, separately from fixed pool backing.
        assert!(matches!(
            budget
                .try_reserve_process(BUDGET - root_exit_metadata_bytes(EXIT).unwrap())
                .unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
        let callback_alias = wake.clone();
        drop(wake);
        assert_eq!(callback_alias.core.as_ptr(), pointer);
        assert!(callback_alias.wake_active().is_err());
        drop(callback_alias);
    }

    #[test]
    fn shutdown_also_activates_dormant_jobs_for_cancel_cleanup() {
        let (pool, _) = pool(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_flag = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_flag.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    RootProducerTurn::Idle
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let _registration = register(&pool, &job);
        let before = Instant::now();
        pool.shutdown().unwrap();
        assert!(before.elapsed() < WAIT);
        receiver.recv_timeout(WAIT).unwrap();
        assert!(cancelled.load(Ordering::SeqCst));
    }

    struct DropPanicJob {
        phases: Arc<AtomicUsize>,
        attempts: Arc<AtomicUsize>,
        primary: Arc<AtomicUsize>,
        secondary: Option<Arc<AtomicUsize>>,
    }
    impl RootProducerJob for DropPanicJob {
        fn turn(&self) -> RootProducerTurn {
            RootProducerTurn::Complete
        }
        fn cancel(&self) {}
        fn cleanup_step(
            &self,
            phase: u16,
            _predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            assert_eq!(phase, 0);
            self.phases.fetch_add(1, Ordering::SeqCst);
            RootCleanupProgress::Exited
        }
    }
    impl Drop for DropPanicJob {
        fn drop(&mut self) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(Opaque {
                dropped: self.primary.clone(),
                secondary: self.secondary.clone(),
            });
        }
    }
    #[test]
    fn real_last_work_drop_panic_never_becomes_positive_exit_or_releases_pool_credit() {
        for secondary in [false, true] {
            let (pool, budget) = pool(1);
            let phases = Arc::new(AtomicUsize::new(0));
            let attempts = Arc::new(AtomicUsize::new(0));
            let primary = Arc::new(AtomicUsize::new(0));
            let secondary_count = Arc::new(AtomicUsize::new(0));
            let job: Arc<dyn RootProducerJob> = Arc::new(DropPanicJob {
                phases: phases.clone(),
                attempts: attempts.clone(),
                primary: primary.clone(),
                secondary: secondary.then(|| secondary_count.clone()),
            });
            let weak = Arc::downgrade(&job);
            let registration = install(&pool, &budget, EXIT, job);
            let exit = registration.exit_observation();
            registration.wake().unwrap();
            assert!(pool.shutdown().is_err());
            assert_eq!(pool.poll_failure(), Some(RootProducerPoolFailure::JobDrop));
            assert_eq!(phases.load(Ordering::SeqCst), 1);
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(primary.load(Ordering::SeqCst), 1);
            assert_eq!(secondary_count.load(Ordering::SeqCst), 0);
            assert!(
                weak.upgrade().is_none(),
                "a consumed failed destructor cannot be resurrected"
            );
            assert_eq!(
                exit.status(),
                RootProducerExitStatus::Unresolved,
                "Weak loss is never an exit receipt"
            );
            drop(registration);
            drop(pool);
            assert!(matches!(
                budget
                    .try_reserve_process(BUDGET - root_exit_metadata_bytes(EXIT).unwrap())
                    .unwrap(),
                ResultWriteAdmission::Blocked
            ));
        }
    }
    #[test]
    fn duplicate_domain_phase_is_rejected_before_any_cpu_work_is_installed() {
        let (pool, budget) = pool(1);
        assert!(matches!(
            reserve(&pool, &budget, RootCleanupPlan::new(&[7, 7])),
            Err(RootProducerReserveFailure::DuplicatePhase(7))
        ));
        let reservation = reserve(&pool, &budget, EXIT).unwrap();
        let exit = reservation.exit_observation();
        drop(reservation);
        assert_eq!(exit.status(), RootProducerExitStatus::NoJobInstalled);
        pool.shutdown().unwrap();
    }

    #[test]
    fn actual_exit_then_slot_reuse_rejects_old_generation_wakes() {
        let (pool, budget) = pool(1);
        let first = install(
            &pool,
            &budget,
            EXIT,
            Arc::new(FakeJob {
                turn: Box::new(|| RootProducerTurn::Complete),
                cancel: Box::new(|| {}),
                exit: Box::new(|| {}),
            }),
        );
        let exit = first.exit_observation();
        let stale = first.wake_handle();
        first.wake().unwrap();
        {
            let state = pool.core.state.lock().unwrap();
            let (state, _) = pool
                .core
                .changed
                .wait_timeout_while(state, WAIT, |s| s.slots[0].phase != Phase::Free)
                .unwrap();
            assert_eq!(state.slots[0].phase, Phase::Free);
        }
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
        let turns = Arc::new(AtomicUsize::new(0));
        let t = turns.clone();
        let second = install(
            &pool,
            &budget,
            EXIT,
            Arc::new(FakeJob {
                turn: Box::new(move || {
                    t.fetch_add(1, Ordering::SeqCst);
                    RootProducerTurn::Complete
                }),
                cancel: Box::new(|| {}),
                exit: Box::new(|| {}),
            }),
        );
        assert_eq!(
            stale.wake(),
            Err(RootProducerControlFailure::RegistrationExpired)
        );
        assert_eq!(
            stale.wake_active(),
            Err(RootProducerControlFailure::RegistrationExpired)
        );
        drop(first);
        assert_eq!(turns.load(Ordering::SeqCst), 0);
        drop(second);
        pool.shutdown().unwrap();
        assert_eq!(turns.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn shutdown_cancels_live_turn_then_joins_before_owner_credit_release() {
        let (pool, budget) = pool(2);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let inside = Arc::clone(&gate);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_flag = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (cancelled_sender, cancelled_receiver) = mpsc::channel();
        let (exited, exit_receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_flag.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    inside.enter();
                    RootProducerTurn::Blocked
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
                let _ = cancelled_sender.send(());
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        gate.wait_entered();
        let (done, done_receiver) = mpsc::channel();
        let owner = Arc::clone(&pool);
        let shutdown = thread::spawn(move || {
            owner.shutdown().unwrap();
            done.send(()).unwrap();
        });
        {
            let state = pool.core.state.lock().unwrap();
            let (state, _) = pool
                .core
                .changed
                .wait_timeout_while(state, WAIT, |s| !s.stopping)
                .unwrap();
            assert!(state.stopping);
        }
        assert!(
            !cancelled.load(Ordering::SeqCst),
            "CPU owner must not invoke cancel inside the running turn"
        );
        assert!(
            done_receiver.try_recv().is_err(),
            "join must await the still-running turn"
        );
        assert!(matches!(
            budget
                .try_reserve_process(BUDGET - root_exit_metadata_bytes(EXIT).unwrap())
                .unwrap(),
            ResultWriteAdmission::Blocked
        ));
        gate.release();
        cancelled_receiver.recv_timeout(WAIT).unwrap();
        exit_receiver.recv_timeout(WAIT).unwrap();
        done_receiver.recv_timeout(WAIT).unwrap();
        shutdown.join().unwrap();
        assert!(registration.wake().is_err());
        assert!(matches!(
            reserve(&pool, &budget, EXIT),
            Err(RootProducerReserveFailure::Control(
                RootProducerControlFailure::Stopping
            ))
        ));
        assert!(
            matches!(
                budget
                    .try_reserve_process(BUDGET - root_exit_metadata_bytes(EXIT).unwrap())
                    .unwrap(),
                ResultWriteAdmission::Blocked
            ),
            "shutdown leaves actual fixed pool backings alive"
        );
        let weak_core = registration.core.clone();
        drop(pool);
        assert!(weak_core.upgrade().is_none());
        assert!(matches!(
            budget
                .try_reserve_process(BUDGET - root_exit_metadata_bytes(EXIT).unwrap())
                .unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
    }

    #[test]
    fn pure_cancel_wake_does_not_run_foreign_cancel_inline_and_cpu_calls_once() {
        let (pool, budget) = pool(1);
        let gate = Arc::new(Gate::default());
        let inside = gate.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let e = cleaned.clone();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                inside.enter();
                RootProducerTurn::Blocked
            }),
            cancel: Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
            exit: Box::new(move || {
                e.fetch_add(1, Ordering::SeqCst);
            }),
        });
        let _release = ReleaseGate(gate.clone());
        let registration = install(&pool, &budget, EXIT, job);
        let exit = registration.exit_observation();
        registration.wake().unwrap();
        gate.wait_entered();
        let wake = registration.wake_handle();
        wake.request_cancel().unwrap();
        wake.request_cancel().unwrap();
        let before = calls.load(Ordering::SeqCst);
        gate.release();
        let shutdown = pool.shutdown();
        shutdown.unwrap();
        assert_eq!(
            before, 0,
            "metadata request cannot run a foreign cancel while CPU owner is gated"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
    }
    #[test]
    fn reserved_cancel_request_survives_commit_without_running_normal_production() {
        let (pool, budget) = pool(1);
        let reservation = reserve(&pool, &budget, EXIT).unwrap();
        let exit = reservation.exit_observation();
        let wake = reservation.wake_handle();
        wake.request_cancel().unwrap();
        assert_eq!(exit.status(), RootProducerExitStatus::Pending);
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let work: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(|| panic!("a reserved stop must enter cancel cleanup directly")),
            cancel: Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
            exit: Box::new(|| {}),
        });
        let registration = match reservation.commit(work) {
            Ok(r) => r,
            Err(e) => panic!(
                "reserved stop still installs owned cleanup work: {}",
                e.cause
            ),
        };
        pool.shutdown().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(exit.status(), RootProducerExitStatus::Exited);
        drop(registration);
    }

    struct UnresolvedPhaseJob {
        data: Arc<Vec<u8>>,
        unresolved_phase: u16,
        turns: Arc<AtomicUsize>,
        phases: Arc<[AtomicUsize; 3]>,
        dropped: Arc<AtomicUsize>,
    }
    impl RootProducerJob for UnresolvedPhaseJob {
        fn turn(&self) -> RootProducerTurn {
            self.turns.fetch_add(1, Ordering::SeqCst);
            RootProducerTurn::Complete
        }
        fn cancel(&self) {}
        fn cleanup_step(
            &self,
            phase: u16,
            _predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            assert_eq!(self.data.len(), 64 * 1024);
            assert!(self.data.iter().all(|byte| *byte == 151));
            self.phases[usize::from(phase)].fetch_add(1, Ordering::SeqCst);
            if phase == self.unresolved_phase {
                RootCleanupProgress::Unresolved
            } else {
                RootCleanupProgress::Exited
            }
        }
    }
    impl Drop for UnresolvedPhaseJob {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn unresolved_cleanup_keeps_real_work_credit_and_drains_independent_owners() {
        const PLAN: RootCleanupPlan = RootCleanupPlan::new(&[0, 1, 2]);
        for unresolved_phase in 0..3 {
            let (pool, budget) = pool(2);
            let health = pool.execution_health();
            assert!(Arc::ptr_eq(&health, &pool.core.health));
            assert!(health.is_healthy());
            let data = Arc::new(vec![151u8; 64 * 1024]);
            let actual_data = Arc::downgrade(&data);
            let turns = Arc::new(AtomicUsize::new(0));
            let phases = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
            let dropped = Arc::new(AtomicUsize::new(0));
            let job: Arc<dyn RootProducerJob> = Arc::new(UnresolvedPhaseJob {
                data,
                unresolved_phase,
                turns: turns.clone(),
                phases: phases.clone(),
                dropped: dropped.clone(),
            });
            let actual_work = Arc::downgrade(&job);
            let bad = install(&pool, &budget, PLAN, job);
            let original_ticket = bad.ticket;
            let bad_exit = bad.exit_observation();
            let good_dropped = Arc::new(AtomicUsize::new(0));
            let capture = LastDrop {
                gate: None,
                dropped: good_dropped.clone(),
            };
            let (done, receiver) = mpsc::channel();
            let good = install(
                &pool,
                &budget,
                EXIT,
                Arc::new(FakeJob {
                    turn: Box::new(|| RootProducerTurn::Complete),
                    cancel: Box::new(|| {}),
                    exit: Box::new(move || {
                        capture.borrow();
                        done.send(()).unwrap();
                    }),
                }),
            );
            let good_exit = good.exit_observation();
            bad.wake().unwrap();
            good.wake().unwrap();
            receiver.recv_timeout(WAIT).unwrap();
            wait_drained(&pool);
            assert!(matches!(
                reserve(&pool, &budget, EXIT),
                Err(RootProducerReserveFailure::Control(
                    RootProducerControlFailure::Unavailable
                ))
            ));
            let shutdown = pool.shutdown();
            assert!(shutdown.is_err());
            assert_eq!(bad_exit.status(), RootProducerExitStatus::Unresolved);
            assert_eq!(good_exit.status(), RootProducerExitStatus::Exited);
            assert_eq!(turns.load(Ordering::SeqCst), 1);
            for phase in phases.iter() {
                assert_eq!(
                    phase.load(Ordering::SeqCst),
                    1,
                    "every independent once phase still executes"
                );
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            assert_eq!(good_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(actual_data.upgrade().unwrap().len(), 64 * 1024);
            {
                let state = pool.core.state.lock().unwrap();
                let slot = &state.slots[original_ticket.index];
                assert_eq!(slot.generation, original_ticket.generation);
                assert_eq!(slot.phase, Phase::Unresolved);
                assert!(slot.execution_failed);
                assert!(Arc::ptr_eq(
                    slot.job.as_ref().unwrap(),
                    &actual_work.upgrade().unwrap()
                ));
                assert!(
                    slot.faults.iter().all(Option::is_none),
                    "a direct receipt does not fabricate a panic payload"
                );
            }
            assert_eq!(pool.poll_failure(), Some(RootProducerPoolFailure::Cleanup));
            let original_fault = health
                .failure()
                .expect("the original health owner records unknown exit");
            assert_eq!(original_fault.origin(), ExecutionExitOrigin::Process);
            assert_eq!(
                original_fault.fact(),
                RootProducerPoolFailure::Cleanup.failure_fact()
            );
            drop(bad);
            drop(good);
            drop(pool);
            assert_eq!(actual_data.upgrade().unwrap()[0], 151);
            assert!(actual_work.upgrade().is_some());
            let metadata =
                root_exit_metadata_bytes(PLAN).unwrap() + root_exit_metadata_bytes(EXIT).unwrap();
            assert!(
                matches!(
                    budget.try_reserve_process(BUDGET - metadata).unwrap(),
                    ResultWriteAdmission::Blocked
                ),
                "the original unresolved pool Resources credit is not refunded"
            );
            assert_eq!(health.failure(), Some(original_fault));
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
        }
    }
}
