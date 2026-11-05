use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::runtime::observable::{BoundedNotification, Observable};
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::{FieldRenderSchema, PrimitiveType, SlotId, UniqueId};

use super::TaskIoError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed(String),
    Cancelled(String),
}

/// Byte credit reserved by the result owner before the pipeline transfers a
/// chunk into the result stream.
///
/// The result owner decides when the reservation is released. A queue keeps
/// the credit beside the retained payload and drops it only after consumer
/// progress, acknowledgement, cancellation, or terminal cleanup releases
/// those bytes.
pub struct ResultWriteCredit {
    bytes: usize,
    release: Option<Box<dyn Fn(usize) + Send + Sync + 'static>>,
}

impl ResultWriteCredit {
    pub fn new(bytes: usize, release: impl Fn(usize) + Send + Sync + 'static) -> Self {
        Self {
            bytes,
            release: Some(Box::new(release)),
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Releases an unused suffix of this reservation before the retained
    /// payload takes ownership of the remaining bytes.
    pub fn shrink_to(&mut self, retained_bytes: usize) -> Result<(), String> {
        let released = self.bytes.checked_sub(retained_bytes).ok_or_else(|| {
            format!(
                "result credit cannot grow from {} to {retained_bytes} bytes",
                self.bytes
            )
        })?;
        if released > 0 {
            // Commit the smaller ownership before invoking host code so an
            // unwinding release callback cannot make Drop return the original
            // reservation a second time.
            self.bytes = retained_bytes;
            if let Some(release) = self.release.as_ref() {
                release(released);
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for ResultWriteCredit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResultWriteCredit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for ResultWriteCredit {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(self.bytes);
        }
    }
}

#[derive(Debug)]
pub enum ResultWriteAdmission {
    Granted(ResultWriteCredit),
    Blocked,
}

/// The original host I/O failure or the actual typed-construction failure.
/// This is specific to result writes; it adds no transport/status category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultWriteFailure {
    Io(TaskIoError),
    Execution(crate::runtime::task::ExecutionFailure),
}
impl ResultWriteFailure {
    pub fn into_execution(self) -> crate::runtime::task::ExecutionFailure {
        match self {
            Self::Io(error) => error.into(),
            Self::Execution(error) => error,
        }
    }
}
impl From<TaskIoError> for ResultWriteFailure {
    fn from(error: TaskIoError) -> Self {
        Self::Io(error)
    }
}
impl From<crate::runtime::task::ExecutionFailure> for ResultWriteFailure {
    fn from(error: crate::runtime::task::ExecutionFailure) -> Self {
        Self::Execution(error)
    }
}
impl std::fmt::Display for ResultWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Execution(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ResultWriteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Execution(error) => Some(error),
        }
    }
}

/// Non-allocating reservation facts. A host cannot inspect input columns
/// through this pre-acceptance planning surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultInputMetadata {
    rows: usize,
    retained_bytes: usize,
}
impl ResultInputMetadata {
    pub fn rows(self) -> usize {
        self.rows
    }
    pub fn retained_bytes(self) -> usize {
        self.retained_bytes
    }
}
impl From<&Chunk> for ResultInputMetadata {
    fn from(chunk: &Chunk) -> Self {
        Self {
            rows: chunk.len(),
            retained_bytes: chunk.logical_bytes(),
        }
    }
}

/// An opened host result stream. Encoding and presentation remain with the
/// host; the execution kernel only writes Arrow chunks and terminal state.
pub trait TaskResultSession: Send + Sync + 'static {
    /// Returns the upper-bound reservation required before the host encodes
    /// this chunk.
    ///
    /// The driver calls this before `try_acquire`, so a host must use the same
    /// configured packet bound, encode once in `write_with_credit`, reject an
    /// oversized encoding, and release the unused suffix before retaining it.
    fn reservation_bytes(&self, input: ResultInputMetadata) -> Result<usize, TaskIoError>;

    /// Attempts to reserve result-owned bytes without blocking the calling
    /// driver.
    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError>;

    /// Stable readiness observable for a blocked credit acquisition.
    fn writable_observable(&self) -> Option<Arc<Observable>>;

    /// Transfers both the chunk and its exact byte reservation to the result
    /// owner.
    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<(), ResultWriteFailure>;

    fn finish(&self) -> Result<(), TaskIoError>;
    fn abort(&self, reason: ResultAbort);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultPresentation {
    ArrowIpc,
}

#[derive(Clone, Debug)]
pub struct ResultProjection {
    slot_id: SlotId,
    primitive: PrimitiveType,
    field_schema: FieldRenderSchema,
}

impl ResultProjection {
    pub fn new(slot_id: SlotId, primitive: PrimitiveType, field_schema: FieldRenderSchema) -> Self {
        Self {
            slot_id,
            primitive,
            field_schema,
        }
    }

    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub const fn primitive(&self) -> PrimitiveType {
        self.primitive
    }

    pub fn field_schema(&self) -> &FieldRenderSchema {
        &self.field_schema
    }
}

#[derive(Clone, Debug)]
pub struct ResultWriteSpec {
    task_identity: Option<TaskIdentity>,
    task_key: UniqueId,
    presentation: ResultPresentation,
    projections: Option<Vec<ResultProjection>>,
    typed: bool,
}

impl ResultWriteSpec {
    pub fn new(
        task_key: UniqueId,
        presentation: ResultPresentation,
        projections: Option<Vec<ResultProjection>>,
        typed: bool,
    ) -> Self {
        Self {
            task_identity: None,
            task_key,
            presentation,
            projections,
            typed,
        }
    }

    /// Binds a native result stream to its complete Task identity.
    ///
    /// Standalone execution tests may omit this, but a native Worker result
    /// writer must reject an unbound spec rather than fall back to the kernel
    /// fragment id as an admission identity.
    pub const fn with_task_identity(mut self, identity: TaskIdentity) -> Self {
        self.task_identity = Some(identity);
        self
    }

    pub const fn task_identity(&self) -> Option<TaskIdentity> {
        self.task_identity
    }

    pub const fn task_key(&self) -> UniqueId {
        self.task_key
    }

    pub const fn presentation(&self) -> ResultPresentation {
        self.presentation
    }

    pub fn projections(&self) -> Option<&[ResultProjection]> {
        self.projections.as_deref()
    }

    pub const fn is_typed(&self) -> bool {
        self.typed
    }
}

/// Host-owned result-session factory. Presentation encoding remains outside
/// execution; the kernel only owns the lifecycle and Arrow chunk writes.
pub trait TaskResultWriter: Send + Sync + 'static {
    fn open(&self, spec: ResultWriteSpec) -> Result<Arc<dyn TaskResultSession>, TaskIoError>;
}

/// Exact root facts are mandatory before opening a bounded producer. The
/// contract stays separate from carrier/hydration capabilities and contains
/// no runtime writer, Arrow array or transport handle.
#[derive(Clone, Debug)]
pub struct RootResultWriteSpec {
    pub task: TaskIdentity,
    pub contract: Arc<novarocks_result_contract::RootOutputContract>,
}

/// The original issuer owns finite grant-exit records and one independent
/// notification continuation. Neither record is an additional byte wallet.
#[derive(Clone)]
pub struct RootInputAuthority {
    inner: Arc<RootInputAuthorityInner>,
}

type RootCreditRelease = Box<dyn Fn(usize) + Send + Sync + 'static>;
type RootInputPanic = Box<dyn std::any::Any + Send + 'static>;
const ROOT_EXIT_RECORDS: usize =
    novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
        .root_shared_input_positions as usize;
const ROOT_READINESS_CAPACITY: usize =
    novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
        .root_maximum_root_drivers as usize
        * 2
        + 1;

struct RootInputAuthorityInner {
    task: TaskIdentity,
    required_bytes: usize,
    health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    state: std::sync::Mutex<RootInputPosition>,
    records: [std::sync::Mutex<RootInputExitRecord>; ROOT_EXIT_RECORDS],
    notification: std::sync::Mutex<RootInputNotification>,
    receipt_observer: std::sync::Mutex<RootInputReceiptObserver>,
    notifying: std::sync::atomic::AtomicBool,
    notify_pending: std::sync::atomic::AtomicBool,
    unresolved: std::sync::atomic::AtomicBool,
    receipt_failed: std::sync::atomic::AtomicBool,
    observable: Arc<Observable>,
}
struct RootInputPosition {
    generation: u64,
    occupied: bool,
    closed: bool,
    records: [bool; ROOT_EXIT_RECORDS],
}
impl Default for RootInputPosition {
    fn default() -> Self {
        Self {
            generation: 0,
            occupied: false,
            closed: false,
            records: [false; ROOT_EXIT_RECORDS],
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootCreditExitPhase {
    Call = 0,
    CaptureDrop = 1,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RootExitAction {
    #[default]
    NotStarted,
    Started,
    Exited,
    Unresolved,
}
#[derive(Default)]
struct RootInputExitRecord {
    generation: u64,
    release: Option<std::mem::ManuallyDrop<RootCreditRelease>>,
    bytes: usize,
    phases: [RootExitAction; 2],
    primary: [Option<std::mem::ManuallyDrop<RootInputPanic>>; 2],
    secondary: [Option<std::mem::ManuallyDrop<RootInputPanic>>; 2],
    self_pin: Option<Arc<RootInputAuthorityInner>>,
}
// Snapshot, invocation, and three dependent holder destructors are separate
// original once roles. No callback or opaque is implicitly disposed on unwind.
const ROOT_READINESS_PHASES: usize = 1 + 4 * ROOT_READINESS_CAPACITY;
#[derive(Default)]
struct RootReadinessFault {
    primary: Option<std::mem::ManuallyDrop<RootInputPanic>>,
    secondary: Option<std::mem::ManuallyDrop<RootInputPanic>>,
}
struct RootReadinessWork {
    batch: BoundedNotification<ROOT_READINESS_CAPACITY>,
    phases: [RootExitAction; ROOT_READINESS_PHASES],
    faults: [RootReadinessFault; ROOT_READINESS_PHASES],
    failed: bool,
}
impl Default for RootReadinessWork {
    fn default() -> Self {
        Self {
            batch: BoundedNotification::new(),
            phases: [RootExitAction::NotStarted; ROOT_READINESS_PHASES],
            faults: std::array::from_fn(|_| RootReadinessFault::default()),
            failed: false,
        }
    }
}
impl RootReadinessWork {
    fn run(
        &mut self,
        owner: &RootInputAuthorityInner,
        phase: usize,
        action: impl FnOnce(&mut BoundedNotification<ROOT_READINESS_CAPACITY>),
    ) -> bool {
        if self.phases[phase] != RootExitAction::NotStarted {
            return false;
        }
        self.phases[phase] = RootExitAction::Started;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action(&mut self.batch))) {
            Ok(()) => {
                self.phases[phase] = RootExitAction::Exited;
                true
            }
            Err(payload) => {
                self.phases[phase] = RootExitAction::Unresolved;
                self.failed = true;
                self.faults[phase].primary = Some(std::mem::ManuallyDrop::new(payload));
                owner.report_readiness_fault(false);
                // This original synchronous borrower holds the opaque slot,
                // while the author mutex is free during foreign destruction.
                let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                    std::mem::ManuallyDrop::drop(self.faults[phase].primary.as_mut().unwrap());
                }));
                unsafe { std::ptr::write(&mut self.faults[phase].primary, None) };
                if let Err(secondary) = disposed {
                    self.faults[phase].secondary = Some(std::mem::ManuallyDrop::new(secondary));
                }
                false
            }
        }
    }
    fn notify(&mut self, owner: &RootInputAuthorityInner) {
        let mut valid = true;
        self.run(owner, 0, |batch| {
            if owner
                .observable
                .snapshot_bounded_notification(batch)
                .is_err()
            {
                valid = false;
            }
        });
        if !valid {
            self.phases[0] = RootExitAction::Unresolved;
            self.failed = true;
            owner.report_readiness_fault(true);
        }
        for index in 0..ROOT_READINESS_CAPACITY {
            if !self.batch.occupied(index) {
                continue;
            }
            let base = 1 + 4 * index;
            self.run(owner, base, |batch| batch.invoke(index));
            // Invocation unwind does not destroy the still-owned callback.
            // An unknown destructor keeps its own dependent registration and
            // backing; other snapshot entries continue independently.
            if !self.run(owner, base + 1, |batch| batch.drop_callback(index)) {
                continue;
            }
            if !self.run(owner, base + 2, |batch| batch.drop_registration(index)) {
                continue;
            }
            self.run(owner, base + 3, |batch| batch.drop_retained_owner(index));
        }
        if !self.failed {
            self.batch.reset_after_exit();
            self.phases.fill(RootExitAction::NotStarted);
        }
    }
}
struct RootInputNotification {
    work: Option<RootReadinessWork>,
    unknown: bool,
    self_pin: Option<Arc<RootInputAuthorityInner>>,
}
impl Default for RootInputNotification {
    fn default() -> Self {
        Self {
            work: Some(RootReadinessWork::default()),
            unknown: false,
            self_pin: None,
        }
    }
}
// The original finite storage is temporarily borrowed, not transferred to an
// unowned local snapshot. Unwind returns inert fields to that same issuer.
struct RootReadinessRun {
    owner: Arc<RootInputAuthorityInner>,
    work: Option<RootReadinessWork>,
    completed: bool,
}
impl Drop for RootReadinessRun {
    fn drop(&mut self) {
        let failed = !self.completed || self.work.as_ref().is_none_or(|work| work.failed);
        let pin = {
            let mut slot = self
                .owner
                .notification
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            slot.unknown |= failed;
            slot.work = self.work.take();
            if failed { None } else { slot.self_pin.take() }
        };
        if !self.completed {
            self.owner.report_readiness_fault(true);
            self.owner
                .notifying
                .store(false, std::sync::atomic::Ordering::Release);
        }
        // The borrower still owns the issuer, so a healthy pin cannot be its
        // final strong reference and cannot invoke foreign backing destruction.
        drop(pin);
    }
}

/// Metadata-only entrance to the original issuer's bounded readiness owner.
/// It retains no Task control, input, credit or producer capability.
#[derive(Clone)]
pub struct RootInputReadinessNotifier(std::sync::Weak<RootInputAuthorityInner>);
impl RootInputReadinessNotifier {
    pub fn notify(&self) {
        if let Some(owner) = self.0.upgrade() {
            owner.request_notify();
        }
    }
}
#[derive(Clone, Copy)]
struct RootInputTicket {
    record: usize,
    generation: u64,
}

impl RootInputAuthorityInner {
    fn report_unknown(&self) {
        use crate::runtime::execution_health::ExecutionExitOrigin;
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        self.unresolved
            .store(true, std::sync::atomic::Ordering::Release);
        // This pure health latch owns no callback or Task acceptance port.
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution,
            },
            FailureFact::new(
                FailureClass::Unexpected,
                ExecutionReason::RootInputPermitDropPanicked.reason(),
            ),
        );
    }
    fn save_credit_panic(
        self: &Arc<Self>,
        record: &mut RootInputExitRecord,
        phase: RootCreditExitPhase,
        payload: RootInputPanic,
    ) {
        let index = phase as usize;
        record.phases[index] = RootExitAction::Unresolved;
        record.primary[index] = Some(std::mem::ManuallyDrop::new(payload));
        record.self_pin = Some(Arc::clone(self));
        // The original addressable custody is installed before publication.
        self.report_unknown();
        let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            std::mem::ManuallyDrop::drop(record.primary[index].as_mut().unwrap());
        }));
        // Drop has been attempted exactly once, even if it unwound. Do not move
        // the invalid Box out of its ManuallyDrop slot or run its destructor again.
        unsafe { std::ptr::write(&mut record.primary[index], None) };
        if let Err(secondary) = disposed {
            record.secondary[index] = Some(std::mem::ManuallyDrop::new(secondary));
            // No foreign notification follows before this secondary is held.
            self.report_unknown();
        }
    }
    fn retire(
        self: &Arc<Self>,
        ticket: RootInputTicket,
        credit: Option<ResultWriteCredit>,
        receipt: &mut Option<RootWorkExitProgress>,
        notify: bool,
    ) -> RootWorkExitProgress {
        let mut record = self.records[ticket.record].lock().unwrap();
        assert_eq!(
            record.generation, ticket.generation,
            "exact root exit ticket"
        );
        if let Some(mut credit) = credit {
            record.bytes = credit.bytes;
            record.release = credit.release.take().map(std::mem::ManuallyDrop::new);
            // Only a fieldless credit shell is dropped here. Its actual callback
            // remains in the original issuer before any foreign invocation.
            drop(credit);
        }
        for phase in [RootCreditExitPhase::Call, RootCreditExitPhase::CaptureDrop] {
            let index = phase as usize;
            if record.phases[index] != RootExitAction::NotStarted {
                continue;
            }
            record.phases[index] = RootExitAction::Started;
            let result = match phase {
                RootCreditExitPhase::Call => {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if let Some(release) = record.release.as_ref() {
                            (release)(record.bytes);
                        }
                    }))
                }
                RootCreditExitPhase::CaptureDrop => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if let Some(release) = record.release.as_mut() {
                            unsafe { std::mem::ManuallyDrop::drop(release) };
                        }
                    }));
                    // The callback Box's one Drop is complete or unknown. Its
                    // invalid shell must never participate in unwind cleanup.
                    unsafe { std::ptr::write(&mut record.release, None) };
                    result
                }
            };
            match result {
                Ok(()) => record.phases[index] = RootExitAction::Exited,
                Err(payload) => self.save_credit_panic(&mut record, phase, payload),
            }
        }
        let mut unknown = record.phases.contains(&RootExitAction::Unresolved);
        drop(record);
        {
            let mut state = self.state.lock().unwrap();
            // A late notification belongs to this ticket, never a newly issued
            // position. Reusing an exit record cannot change the old receipt.
            if state.occupied && state.generation == ticket.generation {
                state.occupied = false;
            } else {
                // An internal ownership mismatch is sticky, not permission to
                // release whatever newer generation currently occupies the slot.
                self.records[ticket.record].lock().unwrap().self_pin = Some(Arc::clone(self));
                self.report_unknown();
                unknown = true;
            }
            // Freeze this exact ticket before its record can be reused. The
            // independent notifier has its own whole-issuer progress.
            *receipt = Some(if unknown {
                RootWorkExitProgress::Unresolved
            } else {
                RootWorkExitProgress::Exited
            });
            if notify {
                self.notify_pending
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            if !unknown {
                state.records[ticket.record] = false;
            }
        }
        // The independent notification owner is still visible as Pending
        // while callbacks run. Freeing a record never silently loses readiness.
        if notify {
            self.request_notify();
        } else {
            self.publish_receipt();
        }
        receipt.expect("exact grant receipt was frozen before record reuse")
    }
    fn request_notify(self: &Arc<Self>) {
        self.request_notify_body();
        self.publish_receipt();
    }
    fn report_readiness_fault(&self, invariant: bool) {
        use crate::runtime::{execution_health::ExecutionExitOrigin, task::ExecutionReason};
        use novarocks_failure_contract::{FailureClass, FailureFact};
        self.unresolved
            .store(true, std::sync::atomic::Ordering::Release);
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution,
            },
            FailureFact::new(
                if invariant {
                    FailureClass::Invariant
                } else {
                    FailureClass::Unexpected
                },
                if invariant {
                    ExecutionReason::RootReadinessNotificationInvariant.reason()
                } else {
                    ExecutionReason::RootReadinessNotificationPanicked.reason()
                },
            ),
        );
    }
    fn request_notify_body(self: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        self.notify_pending.store(true, Ordering::Release);
        if self
            .notifying
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Reentry coalesces into this original synchronous continuation.
            return;
        }
        loop {
            self.notify_pending.store(false, Ordering::Release);
            let work = {
                let mut slot = self.notification.lock().unwrap_or_else(|p| p.into_inner());
                if slot.unknown {
                    self.notifying.store(false, Ordering::Release);
                    return;
                }
                // Keep the original position/backing before any foreign call.
                slot.self_pin = Some(self.clone());
                slot.work.take()
            };
            let mut run = RootReadinessRun {
                owner: self.clone(),
                work,
                completed: false,
            };
            if let Some(work) = run.work.as_mut() {
                work.notify(self);
            } else {
                self.report_readiness_fault(true);
            }
            let failed = run.work.as_ref().is_none_or(|work| work.failed);
            run.completed = run.work.is_some();
            drop(run);
            self.notifying.store(false, Ordering::Release);
            if failed
                || !self.notify_pending.load(Ordering::Acquire)
                || self
                    .notifying
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
        }
    }
}

#[derive(Default)]
struct RootInputReceiptObserver {
    registered: Option<std::mem::ManuallyDrop<std::task::Waker>>,
    publishing: Option<std::mem::ManuallyDrop<std::task::Waker>>,
    registering: bool,
    active: bool,
    again: bool,
    failed: bool,
    primary: [Option<std::mem::ManuallyDrop<RootInputPanic>>; 3],
    secondary: [Option<std::mem::ManuallyDrop<RootInputPanic>>; 3],
    self_pin: Option<Arc<RootInputAuthorityInner>>,
    #[cfg(test)]
    mismatch_gate: Option<Arc<RootInputReceiptMismatchGate>>,
}
#[cfg(test)]
struct RootInputReceiptMismatchGate {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
#[derive(Clone, Copy)]
enum RootInputReceiptPhase {
    Clone = 0,
    Wake = 1,
    Drop = 2,
}

impl RootInputAuthorityInner {
    fn progress(&self) -> RootWorkExitProgress {
        use std::sync::atomic::Ordering;
        if self.unresolved.load(Ordering::Acquire) {
            return RootWorkExitProgress::Unresolved;
        }
        let state = self.state.lock().unwrap();
        if state.records.iter().any(|used| *used)
            || self.notifying.load(Ordering::Acquire)
            || self.notify_pending.load(Ordering::Acquire)
        {
            RootWorkExitProgress::Pending
        } else {
            RootWorkExitProgress::Exited
        }
    }
    fn receipt_fault(
        self: &Arc<Self>,
        phase: RootInputReceiptPhase,
        payload: Option<RootInputPanic>,
        changed: bool,
        publishing: Option<std::mem::ManuallyDrop<std::task::Waker>>,
    ) {
        use crate::runtime::{execution_health::ExecutionExitOrigin, task::ExecutionReason};
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let index = phase as usize;
        self.receipt_failed
            .store(true, std::sync::atomic::Ordering::Release);
        {
            let mut observer = self.receipt_observer.lock().unwrap();
            observer.failed = true;
            if let Some(publishing) = publishing {
                observer.publishing = Some(publishing);
            }
            observer.self_pin = Some(Arc::clone(self));
            observer.primary[index] = payload.map(std::mem::ManuallyDrop::new);
        }
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution,
            },
            FailureFact::new(
                if changed {
                    FailureClass::Invariant
                } else {
                    FailureClass::Unexpected
                },
                if changed {
                    ExecutionReason::RootInputExitObserverChanged.reason()
                } else {
                    ExecutionReason::RootInputExitObserverPanicked.reason()
                },
            ),
        );
        // The primary was addressable before publication. This same original
        // synchronous continuation now owns its one disposal; it cannot await
        // or unwind a raw payload through another foreign operation.
        let mut primary = self.receipt_observer.lock().unwrap().primary[index].take();
        if primary.is_some() {
            let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                std::mem::ManuallyDrop::drop(primary.as_mut().unwrap());
            }));
            unsafe { std::ptr::write(&mut primary, None) };
            if let Err(secondary) = disposed {
                self.receipt_observer.lock().unwrap().secondary[index] =
                    Some(std::mem::ManuallyDrop::new(secondary));
            }
        }
        // A metadata endpoint failure never rewrites a physical body receipt.
    }
    fn poll_receipt(self: &Arc<Self>, waker: &std::task::Waker) -> RootWorkExitProgress {
        let progress = self.progress();
        if progress != RootWorkExitProgress::Pending {
            return progress;
        }
        let clone = {
            let mut observer = self.receipt_observer.lock().unwrap();
            if observer.failed
                || self
                    .receipt_failed
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return progress;
            }
            if observer.registering {
                return self.progress();
            }
            if let Some(existing) = observer.registered.as_ref() {
                if !existing.will_wake(waker) {
                    // Freeze the epoch before unlocking: neither the publisher
                    // nor a new registrar may reuse this phase's opaque slots.
                    observer.failed = true;
                    self.receipt_failed
                        .store(true, std::sync::atomic::Ordering::Release);
                    #[cfg(test)]
                    let mismatch_gate = observer.mismatch_gate.clone();
                    drop(observer);
                    #[cfg(test)]
                    if let Some(gate) = mismatch_gate {
                        gate.entered.send(()).unwrap();
                        gate.release.lock().unwrap().recv().unwrap();
                    }
                    self.receipt_fault(RootInputReceiptPhase::Clone, None, true, None);
                    return self.progress();
                }
                false
            } else {
                observer.registering = true;
                true
            }
        };
        if clone {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.clone())) {
                Ok(cloned) => {
                    let mut observer = self.receipt_observer.lock().unwrap();
                    observer.registered = Some(std::mem::ManuallyDrop::new(cloned));
                    observer.registering = false;
                }
                Err(payload) => {
                    self.receipt_fault(RootInputReceiptPhase::Clone, Some(payload), false, None);
                    self.receipt_observer.lock().unwrap().registering = false;
                }
            }
        }
        let progress = self.progress();
        if progress != RootWorkExitProgress::Pending {
            self.publish_receipt();
        }
        progress
    }
    fn publish_receipt(self: &Arc<Self>) {
        loop {
            if self.progress() == RootWorkExitProgress::Pending {
                return;
            }
            let mut publishing = {
                let mut observer = self.receipt_observer.lock().unwrap();
                if observer.failed
                    || self
                        .receipt_failed
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    return;
                }
                if observer.active || observer.registering {
                    observer.again = true;
                    return;
                }
                let Some(waker) = observer.registered.take() else {
                    return;
                };
                observer.active = true;
                observer.again = false;
                Some(waker)
            };
            // One synchronous original-issuer continuation owns this Waker.
            // New epochs touch only registered; no MutexGuard field pointer or
            // foreign callback crosses the state lock, await or ownership scope.
            let woke = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                publishing.as_ref().unwrap().wake_by_ref();
            }));
            if let Err(payload) = woke {
                self.receipt_fault(
                    RootInputReceiptPhase::Wake,
                    Some(payload),
                    false,
                    publishing.take(),
                );
                self.receipt_observer.lock().unwrap().active = false;
                return;
            }
            let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                std::mem::ManuallyDrop::drop(publishing.as_mut().unwrap());
            }));
            // Its old shell is invalid on either return. Clear in place before
            // any further publication; moving a dropped Box/Waker is forbidden.
            unsafe { std::ptr::write(&mut publishing, None) };
            if let Err(payload) = dropped {
                self.receipt_fault(RootInputReceiptPhase::Drop, Some(payload), false, None);
                self.receipt_observer.lock().unwrap().active = false;
                return;
            }
            let mut observer = self.receipt_observer.lock().unwrap();
            observer.active = false;
            if !observer.again {
                return;
            }
        }
    }
}

impl RootInputAuthority {
    /// Actual inline record layout, finite readiness and pinned platform mutex
    /// backings. The caller admits this amount before construction.
    pub fn initial_backing_bytes() -> usize {
        use std::alloc::Layout;
        use std::sync::atomic::AtomicUsize;
        #[repr(C, align(2))]
        struct Header {
            strong: AtomicUsize,
            weak: AtomicUsize,
        }
        fn arc<T>() -> usize {
            Layout::new::<Header>()
                .extend(Layout::new::<T>())
                .unwrap()
                .0
                .pad_to_align()
                .size()
        }
        let bytes = arc::<RootInputAuthorityInner>()
            + arc::<Observable>()
            + Observable::bounded_backing_bytes(ROOT_READINESS_CAPACITY)
                .expect("frozen root readiness capacity is supported");
        #[cfg(target_vendor = "apple")]
        let bytes = bytes
            + Layout::array::<(isize, [u8; 56])>(ROOT_EXIT_RECORDS + 3)
                .expect("finite root mutex backings")
                .size();
        bytes
    }
    pub fn new(
        spec: &RootResultWriteSpec,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    ) -> Self {
        Self::try_new(spec, health).expect("root input readiness allocation")
    }
    pub fn try_new(
        spec: &RootResultWriteSpec,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    ) -> Result<Self, TaskIoError> {
        Self::try_new_with_owner(spec, health, None)
    }
    pub fn try_new_retained(
        spec: &RootResultWriteSpec,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Self, TaskIoError> {
        Self::try_new_with_owner(spec, health, Some(owner))
    }
    fn try_new_with_owner(
        spec: &RootResultWriteSpec,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Self, TaskIoError> {
        let geometry =
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1;
        let required_bytes = geometry.root_original_input_backing_capacity_bytes as usize
            + if spec.contract.kind() == novarocks_result_contract::RootOutputKind::CountOnly {
                0
            } else {
                geometry.root_additional_hydrate_backing_capacity_bytes as usize
            };
        let observable = match owner {
            Some(owner) => Observable::try_bounded_retained(ROOT_READINESS_CAPACITY, owner),
            None => Observable::try_bounded(ROOT_READINESS_CAPACITY),
        }
        .map_err(|_| {
            TaskIoError::new(
                super::TaskIoOperation::ResultOpen,
                super::TaskIoErrorKind::Unavailable,
                "root input readiness backing allocation failed",
            )
        })?;
        let authority = Self {
            inner: Arc::new(RootInputAuthorityInner {
                task: spec.task,
                required_bytes,
                health,
                state: std::sync::Mutex::new(RootInputPosition::default()),
                records: std::array::from_fn(|_| {
                    std::sync::Mutex::new(RootInputExitRecord::default())
                }),
                notification: std::sync::Mutex::new(RootInputNotification::default()),
                receipt_observer: std::sync::Mutex::new(RootInputReceiptObserver::default()),
                notifying: std::sync::atomic::AtomicBool::new(false),
                notify_pending: std::sync::atomic::AtomicBool::new(false),
                unresolved: std::sync::atomic::AtomicBool::new(false),
                receipt_failed: std::sync::atomic::AtomicBool::new(false),
                observable: Arc::new(observable),
            }),
        };
        drop(authority.inner.state.lock().unwrap());
        drop(authority.inner.notification.lock().unwrap());
        drop(authority.inner.receipt_observer.lock().unwrap());
        for record in &authority.inner.records {
            drop(record.lock().unwrap());
        }
        Ok(authority)
    }
    pub fn required_bytes(&self) -> usize {
        self.inner.required_bytes
    }
    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inner.observable)
    }
    pub fn readiness_notifier(&self) -> RootInputReadinessNotifier {
        RootInputReadinessNotifier(Arc::downgrade(&self.inner))
    }
    pub fn notify_readiness(&self) {
        self.inner.request_notify();
    }

    pub fn is_available(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        !self
            .inner
            .unresolved
            .load(std::sync::atomic::Ordering::Acquire)
            && !self
                .inner
                .receipt_failed
                .load(std::sync::atomic::Ordering::Acquire)
            && !state.closed
            && !state.occupied
            && state.records.iter().any(|used| !used)
    }
    /// Reserve only issuer metadata and its input position. This value grants
    /// no right to materialize a Chunk until complete retained credit is bound.
    pub fn try_reserve(&self) -> Result<RootInputReservationAdmission, TaskIoError> {
        let mut state = self.inner.state.lock().unwrap();
        if self
            .inner
            .unresolved
            .load(std::sync::atomic::Ordering::Acquire)
            || self
                .inner
                .receipt_failed
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(RootInputReservationAdmission::Blocked);
        }
        if state.closed {
            return Err(TaskIoError::new(
                super::TaskIoOperation::ResultWrite,
                super::TaskIoErrorKind::Cancelled,
                "root input issuer is closed",
            ));
        }
        if state.occupied {
            return Ok(RootInputReservationAdmission::Blocked);
        }
        let Some(index) = state.records.iter().position(|used| !used) else {
            return Ok(RootInputReservationAdmission::Blocked);
        };
        let generation = state.generation.checked_add(1).ok_or_else(|| {
            TaskIoError::new(
                super::TaskIoOperation::ResultWrite,
                super::TaskIoErrorKind::Internal,
                "root input generation overflow",
            )
        })?;
        let mut record = self.inner.records[index].lock().unwrap();
        *record = RootInputExitRecord {
            generation,
            ..RootInputExitRecord::default()
        };
        state.generation = generation;
        state.occupied = true;
        state.records[index] = true;
        Ok(RootInputReservationAdmission::Granted(
            RootInputReservation {
                issuer: Arc::clone(&self.inner),
                ticket: Some(RootInputTicket {
                    record: index,
                    generation,
                }),
            },
        ))
    }
    pub fn owns_original(&self, permit: &RootInputPermit) -> bool {
        if !Arc::ptr_eq(&self.inner, &permit.issuer) || permit.receipt.is_some() {
            return false;
        }
        let state = self.inner.state.lock().unwrap();
        state.occupied && state.generation == permit.generation
    }
    /// Whole issuer proof includes the independent notification continuation.
    /// It cannot be inferred from credit bytes or an individual permit receipt.
    pub fn input_exit_progress(&self) -> RootWorkExitProgress {
        self.inner.progress()
    }
    /// Registers the one original factory's qualified metadata Waker, then
    /// rechecks physical progress. The Waker must own no Task/input authority.
    /// Its clone/wake/drop failure is an independent supervision incident and
    /// never rolls back a body receipt already established before publication.
    pub fn poll_input_exit(&self, waker: &std::task::Waker) -> RootWorkExitProgress {
        self.inner.poll_receipt(waker)
    }
    pub fn close(&self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.closed = true;
            self.inner
                .notify_pending
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.inner.request_notify();
    }
}

pub enum RootInputReservationAdmission {
    Granted(RootInputReservation),
    Blocked,
}

/// A position/exit-record reservation, deliberately distinct from the funded
/// permit accepted by Arrow producers. It is unique and cannot be cloned.
pub struct RootInputReservation {
    issuer: Arc<RootInputAuthorityInner>,
    ticket: Option<RootInputTicket>,
}
impl RootInputReservation {
    pub fn required_bytes(&self) -> usize {
        self.issuer.required_bytes
    }
    /// Rejection leaves both original owners in their named slots. No callback
    /// or foreign destructor runs on this pure metadata/bind boundary.
    pub fn try_bind(
        &mut self,
        credit: &mut Option<ResultWriteCredit>,
    ) -> Result<RootInputAdmission, TaskIoError> {
        let ticket = self.ticket.ok_or_else(|| {
            TaskIoError::new(
                super::TaskIoOperation::ResultWrite,
                super::TaskIoErrorKind::Internal,
                "root input reservation was already consumed",
            )
        })?;
        if credit.as_ref().map(ResultWriteCredit::bytes) != Some(self.issuer.required_bytes) {
            return Err(TaskIoError::new(
                super::TaskIoOperation::ResultWrite,
                super::TaskIoErrorKind::InvalidResponse,
                "root input credit does not cover its exact frozen overlap",
            ));
        }
        let state = self.issuer.state.lock().unwrap();
        if self
            .issuer
            .unresolved
            .load(std::sync::atomic::Ordering::Acquire)
            || self
                .issuer
                .receipt_failed
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(RootInputAdmission::Blocked);
        }
        if state.closed {
            return Err(TaskIoError::new(
                super::TaskIoOperation::ResultWrite,
                super::TaskIoErrorKind::Cancelled,
                "root input issuer is closed",
            ));
        }
        assert!(state.occupied && state.generation == ticket.generation);
        let permit = RootInputPermit {
            credit: credit.take(),
            issuer: Arc::clone(&self.issuer),
            generation: ticket.generation,
            ticket: Some(ticket),
            receipt: None,
        };
        self.ticket = None;
        Ok(RootInputAdmission::Granted(permit))
    }
    /// Explicitly retire a rejected named credit in the already reserved finite
    /// custody, rather than letting an admission error implicitly drop it.
    pub fn retire_credit(mut self, credit: &mut Option<ResultWriteCredit>) -> RootWorkExitProgress {
        let ticket = self.ticket.take().expect("live root input reservation");
        self.issuer.retire(ticket, credit.take(), &mut None, true)
    }
    /// The retained-budget author actually returned Blocked without a credit.
    /// Its existing budget readiness owns the next retry; do not manufacture a
    /// writable transition for this unsuccessful tentative admission.
    pub fn release_unfunded(mut self) -> RootWorkExitProgress {
        let ticket = self.ticket.take().expect("live root input reservation");
        self.issuer.retire(ticket, None, &mut None, false)
    }
}
impl Drop for RootInputReservation {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.issuer.retire(ticket, None, &mut None, true);
        }
    }
}

/// Only a complete exact retained grant can become a RootInputPermit. Callers
/// retire data first, then explicitly observe this ticket's pure exit receipt.
pub struct RootInputPermit {
    credit: Option<ResultWriteCredit>,
    issuer: Arc<RootInputAuthorityInner>,
    generation: u64,
    ticket: Option<RootInputTicket>,
    receipt: Option<RootWorkExitProgress>,
}
impl RootInputPermit {
    pub fn retained_bytes(&self) -> usize {
        self.credit
            .as_ref()
            .expect("input retains its credit")
            .bytes()
    }
    pub fn task(&self) -> TaskIdentity {
        self.issuer.task
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Exact-once ticket exit, cached before any caller can reuse this object.
    /// Drop returning normally is not a physical receipt; use this result.
    pub fn retire(&mut self) -> RootWorkExitProgress {
        if let Some(receipt) = self.receipt {
            return receipt;
        }
        let ticket = self.ticket.take().expect("live root input permit ticket");
        self.issuer
            .retire(ticket, self.credit.take(), &mut self.receipt, true)
    }
}
impl Drop for RootInputPermit {
    fn drop(&mut self) {
        self.retire();
    }
}

pub enum RootInputAdmission {
    Granted(RootInputPermit),
    Blocked,
}

/// Bounded metadata about a producer's failure. The original source belongs
/// to its actual Task owner; retaining this value grants no execution or
/// source-retention capability and cannot manufacture an originating failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootFailureObservation {
    fact: novarocks_execution_contract::failure::FailureFact,
    detail: novarocks_execution_contract::status::SafeDetail,
}
impl RootFailureObservation {
    pub fn new(
        fact: novarocks_execution_contract::failure::FailureFact,
        detail: novarocks_execution_contract::status::SafeDetail,
    ) -> Self {
        // SafeDetail bounds text length, but a caller-supplied String may
        // retain a much larger backing. A retained root record owns only its
        // bounded text, independently of that caller's original allocation.
        let detail = novarocks_execution_contract::status::SafeDetail::truncating(detail.as_str());
        Self { fact, detail }
    }
    /// Freeze outside host/ingress locks: projecting a domain source is the
    /// originating owner's operation, never a retained metadata getter's job.
    pub fn from_failure(cause: &crate::runtime::task::ExecutionFailure) -> Self {
        use novarocks_execution_contract::failure::FailureProjection;
        let fact = cause.failure_fact();
        Self::new(fact, cause.safe_detail_for_fact(fact))
    }
    pub const fn fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        self.fact
    }
    pub fn detail(&self) -> &novarocks_execution_contract::status::SafeDetail {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootProducerState {
    Accepting,
    /// No more input is allowed; encoding/publication may still be running.
    Finishing,
    /// End is published, the encoder actually exited, and its exact context
    /// holds every retained result owner. Consumer progress is independent.
    ParticipationHeld,
    /// The producer is closed or has formed a failure whose original Task
    /// handoff/publication is still pending. Absence of that receipt is never
    /// normal completion; live drivers wait on their actual Task stop signal.
    AwaitTerminalControl,
    Failed(RootFailureObservation),
}

/// Physical completion of the entire accepted producer job. Logical End,
/// cancellation, and encoder handoff do not establish this fact. An unresolved
/// owner retains its original obligation and cannot subsequently become Exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootWorkExitProgress {
    Pending,
    Exited,
    Unresolved,
}

/// Original data and its exact protection grant. Field order alone does not
/// prove exit on unwind: the Sink retains this value in its continuation until
/// a host explicitly takes it, and each owner retires data before its permit.
/// Do not drop this combined value as a rejection or cleanup operation.
pub struct RootResultInput {
    chunk: Chunk,
    permit: RootInputPermit,
}
impl RootResultInput {
    pub fn new(chunk: Chunk, permit: RootInputPermit) -> Self {
        Self { chunk, permit }
    }
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }
    pub fn permit(&self) -> &RootInputPermit {
        &self.permit
    }
    /// Moves the original owners without releasing either responsibility.
    /// The host stores both in its cleanup continuation before running any
    /// destructor, and retires the permit only after every data holder exited.
    pub fn into_cleanup_parts(self) -> (Chunk, RootInputPermit) {
        (self.chunk, self.permit)
    }
}

/// Host-owned bounded root producer. Driver methods never synchronously
/// hydrate, count, render or wait for consumer acknowledgement. A dedicated
/// finite producer advances the one admitted input/cursor under its quantum.
pub trait RootResultSession: Send + Sync + 'static {
    fn spec(&self) -> &RootResultWriteSpec;
    fn try_acquire_input(&self) -> Result<RootInputAdmission, TaskIoError>;
    fn writable_observable(&self) -> Arc<Observable>;
    /// Before acceptance the caller retains its original slot, including on
    /// rejection or panic. The host validates by borrowing it, then performs
    /// only a pure move into its original protected input owner under its lock.
    /// Once taken, even a later error leaves ownership with that host. None
    /// acknowledges transfer only; it is never an actual-exit observation.
    fn submit_input(&self, input: &mut Option<RootResultInput>) -> Result<(), TaskIoError>;
    /// This requests finish. Only ContextHeld permits a successful root
    /// Task terminal; callers continue observing the producer until then.
    fn finish_input(&self) -> Result<(), TaskIoError>;
    fn producer_state(&self) -> RootProducerState;
    /// Dedicated physical-exit readiness. It is independent of writable
    /// credit, is stable for this session, and retains its admitted metadata
    /// backing through late subscription/notification tails.
    fn producer_exit_observable(&self) -> Arc<Observable>;
    /// Register the factory's shared metadata-only waker and observe the same
    /// whole-work receipt used by preparation rollback. Registration and the
    /// state observation must cover the final transition without a lost wake.
    /// Unresolved retains actual work and is never a successful exit.
    fn poll_producer_exit(&self, waker: &std::task::Waker) -> RootWorkExitProgress;
    fn abort(&self, reason: ResultAbort);
}

pub trait RootResultWriter: Send + Sync + 'static {
    fn open_root(
        &self,
        spec: RootResultWriteSpec,
    ) -> Result<Arc<dyn RootResultSession>, TaskIoError>;
}

#[cfg(test)]
pub(crate) fn discard_result_session() -> Arc<dyn TaskResultSession> {
    Arc::new(DiscardResultSession)
}

#[cfg(test)]
pub(crate) fn discard_result_writer() -> Arc<dyn TaskResultWriter> {
    Arc::new(DiscardResultWriter)
}

#[cfg(test)]
struct DiscardResultSession;

#[cfg(test)]
struct DiscardResultWriter;

#[cfg(test)]
impl TaskResultWriter for DiscardResultWriter {
    fn open(&self, _spec: ResultWriteSpec) -> Result<Arc<dyn TaskResultSession>, TaskIoError> {
        Ok(discard_result_session())
    }
}

#[cfg(test)]
impl TaskResultSession for DiscardResultSession {
    fn reservation_bytes(&self, input: ResultInputMetadata) -> Result<usize, TaskIoError> {
        Ok(input.retained_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
        _factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        _control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<(), ResultWriteFailure> {
        debug_assert_eq!(credit.bytes(), chunk.logical_bytes());
        Ok(())
    }

    fn finish(&self) -> Result<(), TaskIoError> {
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}

#[cfg(test)]
mod root_input_tests {
    use super::*;

    #[test]
    fn credit_exit_panic_returns_position_but_keeps_unknown_issuer_closed() {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        use std::sync::atomic::{AtomicBool, Ordering};
        let spec = RootResultWriteSpec {
            task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        };
        let health = local_health();
        let authority = Arc::new(RootInputAuthority::new(&spec, Arc::clone(&health)));
        let released = Arc::new(AtomicBool::new(false));
        let credit_released = Arc::clone(&released);
        let credit = ResultWriteCredit::new(authority.required_bytes(), move |_| {
            credit_released.store(true, Ordering::Release);
            std::panic::panic_any(17_u64);
        });
        let RootInputAdmission::Granted(mut permit) = acquire(&authority, credit).unwrap() else {
            panic!("first input");
        };
        let generation = permit.generation();
        let notified = Arc::new(AtomicBool::new(false));
        let callback_notified = Arc::clone(&notified);
        let callback_authority = Arc::downgrade(&authority);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                assert!(released.load(Ordering::Acquire));
                assert!(!callback_authority.upgrade().unwrap().is_available());
                callback_notified.store(true, Ordering::Release);
                std::panic::panic_any(23_u64);
            }))
            .unwrap();
        assert_eq!(permit.retire(), RootWorkExitProgress::Unresolved);
        assert!(notified.load(Ordering::Acquire));
        assert!(!health.is_healthy());
        assert_eq!(permit.generation(), generation);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        drop(subscription);
        drop(permit);
    }
    #[test]
    fn retained_readiness_backing_follows_observable_and_late_subscription() {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Owner(Arc<AtomicBool>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(Owner(Arc::clone(&dropped)));
        let spec = RootResultWriteSpec {
            task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        };
        let authority =
            RootInputAuthority::try_new_retained(&spec, local_health(), owner.clone()).unwrap();
        let observable = authority.observable();
        let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();
        drop(owner);
        drop(authority);
        assert!(!dropped.load(Ordering::Acquire));
        observable.notify_observers();
        drop(observable);
        assert!(!dropped.load(Ordering::Acquire));
        drop(subscription);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn root_input_is_issuer_bound_single_position_and_retained_until_actual_drop() {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let spec = RootResultWriteSpec {
            task,
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        };
        let authority = RootInputAuthority::new(&spec, local_health());
        let reconstructed = RootInputAuthority::new(&spec, local_health());
        let credit = || ResultWriteCredit::new(authority.required_bytes(), |_| {});
        let RootInputAdmission::Granted(permit) = acquire(&authority, credit()).unwrap() else {
            panic!("first input")
        };
        assert!(authority.owns_original(&permit));
        assert!(
            !reconstructed.owns_original(&permit),
            "exact Task identity does not forge an issuer"
        );
        assert!(matches!(
            acquire(&authority, credit()).unwrap(),
            RootInputAdmission::Blocked
        ));
        let first_generation = permit.generation();
        drop(permit);
        let RootInputAdmission::Granted(next) = acquire(&authority, credit()).unwrap() else {
            panic!("next input")
        };
        assert!(next.generation() > first_generation);
        authority.close();
        assert!(
            authority.owns_original(&next),
            "closing admission does not revoke the original input responsibility"
        );
        assert!(acquire(&authority, credit()).is_err());
        drop(next);
        assert!(acquire(&reconstructed, ResultWriteCredit::new(1, |_| {})).is_err());
    }

    fn local_health() -> Arc<crate::runtime::execution_health::ExecutionHealth> {
        Arc::new(crate::runtime::execution_health::ExecutionHealth::new())
    }
    fn test_spec() -> RootResultWriteSpec {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        RootResultWriteSpec {
            task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        }
    }
    // Existing fixture calls own known non-panicking rejection credits. Real
    // production callers retain named slots and retire them under their owner.
    fn acquire(
        authority: &RootInputAuthority,
        credit: ResultWriteCredit,
    ) -> Result<RootInputAdmission, TaskIoError> {
        let mut credit = Some(credit);
        let RootInputReservationAdmission::Granted(mut reservation) = authority.try_reserve()?
        else {
            drop(credit);
            return Ok(RootInputAdmission::Blocked);
        };
        match reservation.try_bind(&mut credit) {
            Ok(RootInputAdmission::Granted(permit)) => Ok(RootInputAdmission::Granted(permit)),
            Ok(RootInputAdmission::Blocked) => {
                reservation.retire_credit(&mut credit);
                Ok(RootInputAdmission::Blocked)
            }
            Err(error) => {
                reservation.retire_credit(&mut credit);
                Err(error)
            }
        }
    }

    #[test]
    fn one_actual_position_reservation_is_finite_and_unfunded_refund_does_not_self_notify() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let authority = RootInputAuthority::new(&test_spec(), local_health());
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notifications);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("first reservation")
        };
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Pending
        );
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(notifications.load(Ordering::SeqCst), 0);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("returned finite record")
        };
        drop(reservation);
        assert_eq!(notifications.load(Ordering::SeqCst), 1);
        drop(subscription);
    }

    #[test]
    fn bind_rejection_preserves_exact_named_credit_and_original_reservation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let authority = RootInputAuthority::new(&test_spec(), local_health());
        let RootInputReservationAdmission::Granted(mut reservation) =
            authority.try_reserve().unwrap()
        else {
            panic!("position")
        };
        let resource = Arc::new(vec![17_u8; 4096]);
        let weak = Arc::downgrade(&resource);
        let releases = Arc::new(AtomicUsize::new(0));
        let released = Arc::clone(&releases);
        let mut credit = Some(ResultWriteCredit::new(1, move |_| {
            std::hint::black_box(&resource);
            released.fetch_add(1, Ordering::SeqCst);
        }));
        let error = reservation.try_bind(&mut credit).err().unwrap();
        assert_eq!(error.kind(), super::super::TaskIoErrorKind::InvalidResponse);
        assert_eq!(credit.as_ref().unwrap().bytes(), 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert!(weak.upgrade().is_some());
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        assert_eq!(
            reservation.retire_credit(&mut credit),
            RootWorkExitProgress::Exited
        );
        assert!(credit.is_none());
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
    }

    struct HeldOriginal {
        _data: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
    }
    struct OpaqueDrop {
        data: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Drop for OpaqueDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
            std::panic::panic_any(HeldOriginal {
                _data: std::mem::ManuallyDrop::new(data),
            });
        }
    }
    struct CreditCapture {
        original: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        payload_drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl CreditCapture {
        fn original(&self) -> Arc<Vec<u8>> {
            Arc::clone(&self.original)
        }
    }
    impl Drop for CreditCapture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let original = unsafe { std::mem::ManuallyDrop::take(&mut self.original) };
            std::panic::panic_any(OpaqueDrop {
                data: std::mem::ManuallyDrop::new(original),
                drops: Arc::clone(&self.payload_drops),
            });
        }
    }
    #[test]
    fn credit_call_capture_drop_and_position_notification_keep_actual_opaque_originals_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let spec = test_spec();
        let authority = RootInputAuthority::new(&spec, Arc::clone(&health));
        let issuer = Arc::downgrade(&authority.inner);
        let original = Arc::new(vec![73_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let capture_drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let captured = CreditCapture {
            original: std::mem::ManuallyDrop::new(original),
            drops: Arc::clone(&capture_drops),
            payload_drops: Arc::clone(&payload_drops),
        };
        let called = Arc::clone(&callback_calls);
        let callback_payload_drops = Arc::clone(&payload_drops);
        let credit = ResultWriteCredit::new(authority.required_bytes(), move |_| {
            called.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaqueDrop {
                data: std::mem::ManuallyDrop::new(captured.original()),
                drops: Arc::clone(&callback_payload_drops),
            });
        });
        let RootInputAdmission::Granted(mut permit) = acquire(&authority, credit).unwrap() else {
            panic!("funded input")
        };
        let notified = Arc::new(AtomicUsize::new(0));
        let observer_calls = Arc::clone(&notified);
        let observer_data = weak.upgrade().unwrap();
        let observer_payload_drops = Arc::clone(&payload_drops);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                observer_calls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(OpaqueDrop {
                    data: std::mem::ManuallyDrop::new(Arc::clone(&observer_data)),
                    drops: Arc::clone(&observer_payload_drops),
                });
            }))
            .unwrap();
        assert_eq!(permit.retire(), RootWorkExitProgress::Unresolved);
        let first_health = health.failure().unwrap();
        assert_eq!(
            first_health.origin(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: spec.task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution
            }
        );
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        assert!(!authority.is_available());
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        assert_eq!(permit.retire(), RootWorkExitProgress::Unresolved);
        drop(permit);
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert_eq!(capture_drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 3);
        assert_eq!(notified.load(Ordering::SeqCst), 1);
        drop(subscription);
        drop(authority);
        assert!(
            issuer.upgrade().is_some(),
            "original finite issuer remains addressable"
        );
        let retained = weak
            .upgrade()
            .expect("actual Vec backing remains with opaque secondary custody");
        assert_eq!(retained.as_slice(), &[73_u8; 8192]);
        assert_eq!(health.failure(), Some(first_health));
    }

    #[test]
    fn gated_old_notification_cannot_rewrite_new_generation_or_old_receipt() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        };
        struct ReleaseGate(Option<mpsc::Sender<()>>);
        impl Drop for ReleaseGate {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let releases = Arc::new(AtomicUsize::new(0));
        let released = Arc::clone(&releases);
        let RootInputAdmission::Granted(mut first) = acquire(
            &authority,
            ResultWriteCredit::new(authority.required_bytes(), move |_| {
                released.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap() else {
            panic!("first input")
        };
        let first_generation = first.generation();
        let (entered_send, entered) = mpsc::channel();
        let (release_send, release) = mpsc::channel();
        let mut gate = ReleaseGate(Some(release_send));
        let release = Mutex::new(release);
        let observations = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&observations);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_send.send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                    std::panic::panic_any(17_u64);
                }
            }))
            .unwrap();
        let join = std::thread::spawn(move || {
            let receipt = first.retire();
            (first, receipt)
        });
        entered.recv().unwrap();
        // The old grant actually ended, but its foreign notification is gated.
        let while_notifying = authority.input_exit_progress();
        let released = Arc::clone(&releases);
        let next = acquire(
            &authority,
            ResultWriteCredit::new(authority.required_bytes(), move |_| {
                released.fetch_add(1, Ordering::SeqCst);
            }),
        );
        if let Some(send) = gate.0.take() {
            let _ = send.send(());
        }
        drop(gate);
        let joined = join.join();
        let (mut first, first_receipt) = joined.unwrap();
        let RootInputAdmission::Granted(mut next) = next.unwrap() else {
            panic!("next generation")
        };
        assert_eq!(while_notifying, RootWorkExitProgress::Pending);
        assert_eq!(first_receipt, RootWorkExitProgress::Exited);
        assert!(next.generation() > first_generation);
        assert!(
            authority.owns_original(&next),
            "late old notification never clears the newer position"
        );
        assert_eq!(first.retire(), RootWorkExitProgress::Exited);
        assert_eq!(next.retire(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        assert!(!health.is_healthy());
        assert_eq!(releases.load(Ordering::SeqCst), 2);
        assert_eq!(observations.load(Ordering::SeqCst), 1);
        drop(first);
        drop(next);
        drop(subscription);
    }

    #[test]
    fn credit_callback_sees_blocked_but_final_position_notify_can_bind_new_generation_inline() {
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        let authority = RootInputAuthority::new(&test_spec(), local_health());
        let source = Arc::new(vec![91_u8; 4096]);
        let source_weak = Arc::downgrade(&source);
        let saw_blocked = Arc::new(AtomicBool::new(false));
        let blocked = Arc::clone(&saw_blocked);
        let credit_issuer = Arc::downgrade(&authority.inner);
        let credit = ResultWriteCredit::new(authority.required_bytes(), move |_| {
            std::hint::black_box(&source);
            let issuer = RootInputAuthority {
                inner: credit_issuer.upgrade().unwrap(),
            };
            blocked.store(
                matches!(
                    issuer.try_reserve().unwrap(),
                    RootInputReservationAdmission::Blocked
                ),
                Ordering::SeqCst,
            );
        });
        let RootInputAdmission::Granted(mut first) = acquire(&authority, credit).unwrap() else {
            panic!("first input")
        };
        let next = Arc::new(Mutex::new(None));
        let next_slot = Arc::clone(&next);
        let observations = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&observations);
        let weak_issuer = Arc::downgrade(&authority.inner);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    let issuer = RootInputAuthority {
                        inner: weak_issuer.upgrade().unwrap(),
                    };
                    let RootInputReservationAdmission::Granted(mut reserved) =
                        issuer.try_reserve().unwrap()
                    else {
                        panic!("record was actually returned before readiness")
                    };
                    let mut credit =
                        Some(ResultWriteCredit::new(reserved.required_bytes(), |_| {}));
                    let RootInputAdmission::Granted(permit) =
                        reserved.try_bind(&mut credit).unwrap()
                    else {
                        panic!("healthy binding")
                    };
                    assert!(credit.is_none());
                    *next_slot.lock().unwrap() = Some(permit);
                }
            }))
            .unwrap();
        let generation = first.generation();
        assert_eq!(first.retire(), RootWorkExitProgress::Exited);
        assert!(saw_blocked.load(Ordering::SeqCst));
        assert!(
            source_weak.upgrade().is_none(),
            "the actual callback capture exited before readmission"
        );
        let mut next = next.lock().unwrap().take().unwrap();
        assert!(next.generation() > generation);
        assert!(authority.owns_original(&next));
        assert_eq!(first.retire(), RootWorkExitProgress::Exited);
        assert!(authority.owns_original(&next));
        assert_eq!(next.retire(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert_eq!(observations.load(Ordering::SeqCst), 2);
        drop(subscription);
    }

    #[test]
    fn permit_drop_during_existing_unwind_preserves_original_failure_occurrence() {
        use crate::runtime::task::ExecutionFailure;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let original = ExecutionFailure::from_domain(TaskIoError::new(
            super::super::TaskIoOperation::ResultWrite,
            super::super::TaskIoErrorKind::RemoteRejected,
            "original required result failure",
        ));
        let original_alias = original.clone();
        let data = Arc::new(vec![57_u8; 8192]);
        let weak = Arc::downgrade(&data);
        let calls = Arc::new(AtomicUsize::new(0));
        let called = Arc::clone(&calls);
        let credit = ResultWriteCredit::new(authority.required_bytes(), move |_| {
            std::hint::black_box(&data);
            called.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(17_u64);
        });
        let RootInputAdmission::Granted(permit) = acquire(&authority, credit).unwrap() else {
            panic!("input")
        };
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _original_permit_owner = permit;
            std::panic::panic_any(original);
        }))
        .unwrap_err();
        let observed = observed.downcast::<ExecutionFailure>().unwrap();
        assert!(observed.same_occurrence(&original_alias));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            weak.upgrade().is_none(),
            "the real credit capture Drop still completed"
        );
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        assert!(!health.is_healthy());
    }

    #[test]
    fn notification_unknown_between_actual_reservation_and_bind_preserves_both_original_slots() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        };
        struct Gate(Option<mpsc::Sender<()>>);
        impl Drop for Gate {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputAdmission::Granted(mut first) = acquire(
            &authority,
            ResultWriteCredit::new(authority.required_bytes(), |_| {}),
        )
        .unwrap() else {
            panic!("first input")
        };
        let (entered_send, entered) = mpsc::channel();
        let (release_send, release) = mpsc::channel();
        let mut gate = Gate(Some(release_send));
        let release = Mutex::new(release);
        let observed = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&observed);
        let subscription = authority
            .observable()
            .try_subscribe(Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_send.send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                    std::panic::panic_any(17_u64);
                }
            }))
            .unwrap();
        let joined = std::thread::spawn(move || first.retire());
        entered.recv().unwrap();
        let reserved = authority.try_reserve();
        let original = Arc::new(vec![61_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let released = Arc::new(AtomicUsize::new(0));
        let release_calls = Arc::clone(&released);
        let mut credit = Some(ResultWriteCredit::new(
            authority.required_bytes(),
            move |_| {
                std::hint::black_box(&original);
                release_calls.fetch_add(1, Ordering::SeqCst);
            },
        ));
        if let Some(send) = gate.0.take() {
            let _ = send.send(());
        }
        drop(gate);
        let exited = joined.join();
        assert_eq!(exited.unwrap(), RootWorkExitProgress::Exited);
        let RootInputReservationAdmission::Granted(mut reserved) = reserved.unwrap() else {
            panic!("reserved before late unknown")
        };
        let health_record = health.failure().unwrap();
        let rejected = reserved.try_bind(&mut credit).unwrap();
        assert!(matches!(rejected, RootInputAdmission::Blocked));
        assert!(credit.is_some());
        assert!(
            reserved.ticket.is_some(),
            "borrowed refusal keeps its exact reservation"
        );
        assert_eq!(released.load(Ordering::SeqCst), 0);
        assert!(weak.upgrade().is_some());
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        assert_eq!(
            reserved.retire_credit(&mut credit),
            RootWorkExitProgress::Exited
        );
        assert!(credit.is_none());
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        assert_eq!(health.failure(), Some(health_record));
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        drop(subscription);
    }

    struct ReceiptWake {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        original: Arc<Vec<u8>>,
        panic_wake: bool,
        payload_drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl std::task::Wake for ReceiptWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.panic_wake {
                std::panic::panic_any(OpaqueDrop {
                    data: std::mem::ManuallyDrop::new(Arc::clone(&self.original)),
                    drops: Arc::clone(&self.payload_drops),
                });
            }
        }
    }
    #[test]
    fn receipt_wake_after_actual_unfunded_exit_does_not_manufacture_data_readiness() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let authority = RootInputAuthority::new(&test_spec(), local_health());
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("actual pending reservation")
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(vec![71_u8; 4096]);
        let weak = Arc::downgrade(&data);
        let waker = std::task::Waker::from(Arc::new(ReceiptWake {
            calls: Arc::clone(&calls),
            original: data,
            panic_wake: false,
            payload_drops: Arc::new(AtomicUsize::new(0)),
        }));
        let data_generation = authority.observable().generation();
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        drop(waker);
        assert!(weak.upgrade().is_some());
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(authority.observable().generation(), data_generation);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(
            weak.upgrade().is_none(),
            "the healthy registered Waker really dropped"
        );
    }
    #[test]
    fn post_body_receipt_wake_opaque_fault_preserves_original_data_without_undoing_proof() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("actual reservation")
        };
        let data = Arc::new(vec![37_u8; 8192]);
        let weak = Arc::downgrade(&data);
        let calls = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let waker = std::task::Waker::from(Arc::new(ReceiptWake {
            calls: Arc::clone(&calls),
            original: data,
            panic_wake: true,
            payload_drops: Arc::clone(&payload_drops),
        }));
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Exited
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
        assert!(!health.is_healthy());
        drop(waker);
        drop(authority);
        assert_eq!(weak.upgrade().unwrap().as_slice(), &[37_u8; 8192]);
    }
    struct ReceiptDrop {
        data: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        payload_drops: Arc<std::sync::atomic::AtomicUsize>,
        wakes: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl std::task::Wake for ReceiptDrop {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl Drop for ReceiptDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
            std::panic::panic_any(OpaqueDrop {
                data: std::mem::ManuallyDrop::new(data),
                drops: Arc::clone(&self.payload_drops),
            });
        }
    }
    #[test]
    fn registered_waker_actual_capture_drop_unknown_keeps_stable_body_exit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("reservation")
        };
        let data = Arc::new(vec![43_u8; 8192]);
        let weak = Arc::downgrade(&data);
        let drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = std::task::Waker::from(Arc::new(ReceiptDrop {
            data: std::mem::ManuallyDrop::new(data),
            drops: Arc::clone(&drops),
            payload_drops: Arc::clone(&payload_drops),
            wakes: Arc::clone(&wakes),
        }));
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        drop(waker);
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
        assert!(!health.is_healthy());
        drop(authority);
        assert_eq!(weak.upgrade().unwrap().as_slice(), &[43_u8; 8192]);
    }

    struct GatedReceiptWake {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        original: Arc<Vec<u8>>,
    }
    impl std::task::Wake for GatedReceiptWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            std::hint::black_box(&self.original);
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
    }
    #[test]
    fn active_old_publisher_does_not_lose_new_actual_grant_observation_epoch() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        };
        struct Gate(Option<mpsc::Sender<()>>);
        impl Drop for Gate {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let authority = RootInputAuthority::new(&test_spec(), local_health());
        let RootInputReservationAdmission::Granted(first) = authority.try_reserve().unwrap() else {
            panic!("first grant")
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let original = Arc::new(vec![89_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let (entered_send, entered) = mpsc::channel();
        let (release_send, release) = mpsc::channel();
        let mut gate = Gate(Some(release_send));
        let waker = std::task::Waker::from(Arc::new(GatedReceiptWake {
            calls: Arc::clone(&calls),
            entered: entered_send,
            release: Mutex::new(release),
            original,
        }));
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        let first_exit = std::thread::spawn(move || first.release_unfunded());
        entered.recv().unwrap();
        let second = authority.try_reserve();
        let observed = authority.poll_input_exit(&waker);
        let second_exit = match second {
            Ok(RootInputReservationAdmission::Granted(second)) => Some(second.release_unfunded()),
            _ => None,
        };
        if let Some(send) = gate.0.take() {
            let _ = send.send(());
        }
        drop(gate);
        let first_exit = first_exit.join();
        drop(waker);
        assert_eq!(first_exit.unwrap(), RootWorkExitProgress::Exited);
        assert_eq!(second_exit, Some(RootWorkExitProgress::Exited));
        assert_eq!(observed, RootWorkExitProgress::Pending);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the second real observation epoch receives its own covered wake"
        );
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(
            weak.upgrade().is_none(),
            "both actual Waker captures have exited after real join"
        );
    }

    #[test]
    fn pending_epoch_rejects_another_live_observer_without_replacing_original() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("actual grant")
        };
        let original = Arc::new(vec![23_u8; 4096]);
        let weak = Arc::downgrade(&original);
        let old_calls = Arc::new(AtomicUsize::new(0));
        let new_calls = Arc::new(AtomicUsize::new(0));
        let first = std::task::Waker::from(Arc::new(ReceiptWake {
            calls: Arc::clone(&old_calls),
            original,
            panic_wake: false,
            payload_drops: Arc::new(AtomicUsize::new(0)),
        }));
        let second = std::task::Waker::from(Arc::new(ReceiptWake {
            calls: Arc::clone(&new_calls),
            original: Arc::new(vec![29_u8; 4096]),
            panic_wake: false,
            payload_drops: Arc::new(AtomicUsize::new(0)),
        }));
        assert_eq!(
            authority.poll_input_exit(&first),
            RootWorkExitProgress::Pending
        );
        assert_eq!(
            authority.poll_input_exit(&second),
            RootWorkExitProgress::Pending
        );
        let fault = health.failure().unwrap();
        assert_eq!(
            fault.fact().class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert_eq!(
            fault.fact().reason(),
            crate::runtime::task::ExecutionReason::RootInputExitObserverChanged.reason()
        );
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert_eq!(old_calls.load(Ordering::SeqCst), 0);
        assert_eq!(new_calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        drop(first);
        drop(second);
        drop(authority);
        assert!(
            weak.upgrade().is_some(),
            "failed endpoint retains the exact original observer, not the replacement"
        );
    }

    #[test]
    fn observer_mismatch_freezes_original_epoch_before_unlocked_fault_publication() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        };
        struct ReleaseGate(Option<mpsc::Sender<()>>);
        impl Drop for ReleaseGate {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputReservationAdmission::Granted(first_grant) = authority.try_reserve().unwrap()
        else {
            panic!("actual first reservation")
        };
        let original = Arc::new(vec![53_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let calls = Arc::new(AtomicUsize::new(0));
        let original_waker = std::task::Waker::from(Arc::new(ReceiptWake {
            calls: Arc::clone(&calls),
            original,
            panic_wake: false,
            payload_drops: Arc::new(AtomicUsize::new(0)),
        }));
        assert_eq!(
            authority.poll_input_exit(&original_waker),
            RootWorkExitProgress::Pending
        );
        drop(original_waker);
        let (entered_send, entered) = mpsc::channel();
        let (release_send, release) = mpsc::channel();
        let mut gate = ReleaseGate(Some(release_send));
        authority
            .inner
            .receipt_observer
            .lock()
            .unwrap()
            .mismatch_gate = Some(Arc::new(RootInputReceiptMismatchGate {
            entered: entered_send,
            release: Mutex::new(release),
        }));
        let mismatch_authority = authority.clone();
        let mismatch = std::thread::spawn(move || {
            let different_waker = std::task::Waker::from(Arc::new(ReceiptWake {
                calls: Arc::new(AtomicUsize::new(0)),
                original: Arc::new(vec![59_u8; 4096]),
                panic_wake: false,
                payload_drops: Arc::new(AtomicUsize::new(0)),
            }));
            mismatch_authority.poll_input_exit(&different_waker)
        });
        entered.recv().unwrap();
        // The real first body exits while fault publication is still gated.
        // Neither its publisher nor another registrar may take the old epoch.
        let first_exit = first_grant.release_unfunded();
        let before_report = health.failure();
        let second = authority.try_reserve();
        let (blocked, unexpected_exit) = match second {
            Ok(RootInputReservationAdmission::Blocked) => (true, None),
            Ok(RootInputReservationAdmission::Granted(second)) => {
                (false, Some(second.release_unfunded()))
            }
            Err(_) => (false, None),
        };
        let retained_before_report = weak.upgrade().is_some();
        let calls_before_report = calls.load(Ordering::SeqCst);
        if let Some(send) = gate.0.take() {
            let _ = send.send(());
        }
        drop(gate);
        let observed = mismatch.join();
        // Negative assertions follow the actual thread join, never a sleep or
        // a count standing in for completion of the foreign continuation.
        assert_eq!(observed.unwrap(), RootWorkExitProgress::Exited);
        assert_eq!(first_exit, RootWorkExitProgress::Exited);
        assert!(before_report.is_none());
        assert!(
            blocked,
            "mismatch closes admission before releasing its lock"
        );
        assert_eq!(unexpected_exit, None);
        assert!(retained_before_report);
        assert_eq!(calls_before_report, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        let fault = health.failure().unwrap();
        assert_eq!(
            fault.fact().class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert_eq!(
            fault.fact().reason(),
            crate::runtime::task::ExecutionReason::RootInputExitObserverChanged.reason()
        );
        assert!(matches!(
            authority.try_reserve().unwrap(),
            RootInputReservationAdmission::Blocked
        ));
        drop(authority);
        assert_eq!(weak.upgrade().unwrap().as_slice(), &[53_u8; 8192]);
        assert_eq!(health.failure(), Some(fault));
    }

    #[test]
    fn raw_waker_clone_fault_has_original_finite_custody_and_does_not_repeat_clone() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{RawWaker, RawWakerVTable, Waker};
        struct CloneSource {
            original: Arc<Vec<u8>>,
            clones: Arc<AtomicUsize>,
            payload_drops: Arc<AtomicUsize>,
        }
        unsafe fn clone_fault(pointer: *const ()) -> RawWaker {
            let source = unsafe { &*pointer.cast::<CloneSource>() };
            source.clones.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaqueDrop {
                data: std::mem::ManuallyDrop::new(Arc::clone(&source.original)),
                drops: Arc::clone(&source.payload_drops),
            });
        }
        unsafe fn consume(pointer: *const ()) {
            drop(unsafe { Arc::from_raw(pointer.cast::<CloneSource>()) });
        }
        unsafe fn by_ref(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fault, consume, by_ref, consume);
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), Arc::clone(&health));
        let RootInputReservationAdmission::Granted(reservation) = authority.try_reserve().unwrap()
        else {
            panic!("real pending grant")
        };
        let original = Arc::new(vec![19_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let clones = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(CloneSource {
            original,
            clones: Arc::clone(&clones),
            payload_drops: Arc::clone(&drops),
        });
        // SAFETY: the initial Waker owns this real Arc; only normal Drop/wake
        // consumes it. Its deliberately failing clone creates no new raw owner.
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(source).cast(), &VTABLE)) };
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        assert_eq!(
            authority.poll_input_exit(&waker),
            RootWorkExitProgress::Pending
        );
        assert_eq!(clones.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!health.is_healthy());
        assert_eq!(reservation.release_unfunded(), RootWorkExitProgress::Exited);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        drop(waker);
        drop(authority);
        assert_eq!(weak.upgrade().unwrap().as_slice(), &[19_u8; 8192]);
    }

    // Correctness oracles for the original bounded readiness author. These run
    // entirely in-process and join no detached work or external fixture.
    #[test]
    fn bounded_readiness_reentry_reuses_all_original_slots_and_weak_port() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let health = local_health();
        let authority = RootInputAuthority::new(&test_spec(), health.clone());
        let weak = Arc::downgrade(&authority.inner);
        let port = authority.readiness_notifier();
        let calls = Arc::new(AtomicUsize::new(0));
        let reentered = Arc::new(AtomicUsize::new(0));
        let mut subscriptions = Vec::new();
        for index in 0..ROOT_READINESS_CAPACITY {
            let calls = calls.clone();
            let reentered = reentered.clone();
            let port = port.clone();
            subscriptions.push(
                authority
                    .observable()
                    .try_subscribe(Arc::new(move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if index == 0 && reentered.fetch_add(1, Ordering::SeqCst) == 0 {
                            port.notify();
                        }
                    }))
                    .unwrap(),
            );
        }
        port.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 2 * ROOT_READINESS_CAPACITY);
        assert_eq!(authority.observable().generation(), 2);
        port.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 3 * ROOT_READINESS_CAPACITY);
        assert_eq!(
            authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(
            authority
                .inner
                .notification
                .lock()
                .unwrap()
                .work
                .as_ref()
                .unwrap()
                .batch
                .is_empty()
        );
        assert!(health.is_healthy());
        drop(subscriptions);
        drop(authority);
        assert!(weak.upgrade().is_none(), "metadata notifier is only weak");
        port.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 3 * ROOT_READINESS_CAPACITY);
    }

    #[test]
    fn readiness_call_and_capture_drop_keep_secondary_and_progress_other_entries() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        struct Payload {
            owner: std::sync::Weak<RootInputAuthorityInner>,
            data: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                let owner = self.owner.upgrade().unwrap();
                assert!(
                    owner.notification.try_lock().is_ok(),
                    "foreign Drop is outside author mutex"
                );
                self.drops.fetch_add(1, Ordering::SeqCst);
                owner.request_notify(); // Actual reentry while the original owner is active.
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                std::panic::panic_any(HeldOriginal {
                    _data: std::mem::ManuallyDrop::new(data),
                });
            }
        }
        struct Capture {
            fault: bool,
            owner: std::sync::Weak<RootInputAuthorityInner>,
            data: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
            drops: Arc<AtomicUsize>,
            payload_drops: Arc<AtomicUsize>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                if !self.fault {
                    unsafe { std::mem::ManuallyDrop::drop(&mut self.data) };
                    return;
                }
                let owner = self.owner.upgrade().unwrap();
                assert!(owner.notification.try_lock().is_ok());
                self.drops.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                std::panic::panic_any(Payload {
                    owner: self.owner.clone(),
                    data: std::mem::ManuallyDrop::new(data),
                    drops: self.payload_drops.clone(),
                });
            }
        }
        for capture_fault in [false, true] {
            let health = local_health();
            let spec = test_spec();
            let authority = RootInputAuthority::new(&spec, health.clone());
            let issuer = Arc::downgrade(&authority.inner);
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let capture_drops = Arc::new(AtomicUsize::new(0));
            let resource = Arc::new(vec![43_u8; 8192]);
            let weak_resource = Arc::downgrade(&resource);
            let callback_calls = Arc::new(AtomicUsize::new(0));
            let subscription = Arc::new(Mutex::new(None));
            let subscribed: Arc<Mutex<Option<crate::runtime::observable::ObserverSubscription>>> =
                subscription.clone();
            let calls = callback_calls.clone();
            let capture = Capture {
                fault: capture_fault,
                owner: issuer.clone(),
                data: std::mem::ManuallyDrop::new(resource),
                drops: capture_drops.clone(),
                payload_drops: payload_drops.clone(),
            };
            let observer: Arc<dyn Fn() + Send + Sync> = if capture_fault {
                Arc::new(move || {
                    std::hint::black_box(&capture);
                    calls.fetch_add(1, Ordering::SeqCst);
                    drop(subscribed.lock().unwrap().take());
                })
            } else {
                // Retain the original data in the registration; the actual
                // primary carries another alias, whose Drop creates secondary.
                Arc::new(move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::panic::panic_any(Payload {
                        owner: capture.owner.clone(),
                        data: std::mem::ManuallyDrop::new(Arc::clone(&capture.data)),
                        drops: capture.payload_drops.clone(),
                    });
                })
            };
            *subscription.lock().unwrap() =
                Some(authority.observable().try_subscribe(observer).unwrap());
            let sibling_calls = Arc::new(AtomicUsize::new(0));
            let actual_calls = sibling_calls.clone();
            let sibling = authority
                .observable()
                .try_subscribe(Arc::new(move || {
                    actual_calls.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            let RootInputAdmission::Granted(mut permit) = acquire(
                &authority,
                ResultWriteCredit::new(authority.required_bytes(), |_| {}),
            )
            .unwrap() else {
                panic!("original credit")
            };
            assert_eq!(
                permit.retire(),
                RootWorkExitProgress::Exited,
                "physical grant receipt survives metadata failure"
            );
            assert_eq!(
                authority.input_exit_progress(),
                RootWorkExitProgress::Unresolved
            );
            assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
            assert_eq!(sibling_calls.load(Ordering::SeqCst), 1);
            assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                capture_drops.load(Ordering::SeqCst),
                usize::from(capture_fault)
            );
            let fault = health.failure().unwrap();
            assert_eq!(
                fault.fact().class(),
                novarocks_failure_contract::FailureClass::Unexpected
            );
            assert_eq!(
                fault.fact().reason(),
                crate::runtime::task::ExecutionReason::RootReadinessNotificationPanicked.reason()
            );
            assert_eq!(
                fault.origin(),
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: spec.task,
                    phase: novarocks_execution_contract::TaskFailurePhase::Execution
                }
            );
            // Global health is diagnostic: a separately owned original issuer
            // still retires its actual credit on the same health latch.
            let safe = RootInputAuthority::new(&test_spec(), health.clone());
            let released = Arc::new(AtomicUsize::new(0));
            let noted = released.clone();
            let RootInputAdmission::Granted(mut safe_permit) = acquire(
                &safe,
                ResultWriteCredit::new(safe.required_bytes(), move |_| {
                    noted.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .unwrap() else {
                panic!("safe sibling credit")
            };
            assert_eq!(safe_permit.retire(), RootWorkExitProgress::Exited);
            assert_eq!(released.load(Ordering::SeqCst), 1);
            assert_eq!(safe.input_exit_progress(), RootWorkExitProgress::Exited);
            assert_eq!(health.failure(), Some(fault));
            // The call-fault registration has an ordinary non-panicking final
            // capture Drop; the capture-fault callback already withdrew its
            // token and retained its dependent snapshot fields in the author.
            drop(sibling);
            drop(subscription.lock().unwrap().take());
            drop(subscription);
            drop(permit);
            drop(authority);
            assert!(issuer.upgrade().is_some());
            assert_eq!(weak_resource.upgrade().unwrap().as_slice(), &[43_u8; 8192]);
        }
    }
}

#[cfg(test)]
mod root_failure_observation_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureFact};

    #[test]
    fn root_failure_observation_does_not_retain_a_callers_oversized_text_backing() {
        let mut original =
            String::with_capacity(8 * novarocks_execution_contract::status::SAFE_DETAIL_MAX_BYTES);
        original.push_str("root failure");
        let original_pointer = original.as_ptr();
        let fact = FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::RootInvalidDop.reason(),
        );
        let observation = RootFailureObservation::new(
            fact,
            novarocks_execution_contract::status::SafeDetail::new(original).unwrap(),
        );
        assert_eq!(observation.fact(), fact);
        assert_eq!(observation.detail().as_str(), "root failure");
        assert_ne!(observation.detail().as_str().as_ptr(), original_pointer);
    }
}
