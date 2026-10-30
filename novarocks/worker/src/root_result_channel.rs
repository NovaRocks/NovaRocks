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

//! One context-owned root stream. Task retirement does not close this owner.
//! Logical ACKs remove queue entries; a Bytes backing keeps its retained-pool
//! credit and physical position until its last real alias is destroyed.

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError, Weak};

use bytes::Bytes;
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution::runtime::fragment::io::{
    ResultWriteAdmission, ResultWriteCredit, RootProducerState, RootResultWriteSpec,
};
use novarocks_execution::runtime::observable::{
    BoundedNotification, Observable, ObserverSubscription,
};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::ordered_retained_stream::{
    OfferOutcome, OrderedRetainedStream, RetainedItemKind, RetainedStreamItem,
    RetainedStreamSnapshot, RetainedStreamWindow, StreamError,
};
use novarocks_execution_contract::root_lifetime::{RootResultLifetime, RootRetentionClose};
use novarocks_execution_contract::root_result::{
    RootReadOutcome, RootResultData, RootResultEnd, RootResultRead, RootResultReply,
};
use novarocks_result_contract::{RootOutputKind, RootProfileV1};
use tokio::sync::Notify;

use crate::result_buffer::{ResultBufferKey, ResultRetainedBudget};
use crate::{TaskStatusSource, WorkerResultRetainedLimits};

/// Read-only root ownership observations. Logical fields share the channel
/// lock; physical fields are independent atomic samples, not a settlement
/// receipt or a count of third-party allocations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RootOwnershipSnapshot {
    pub channels: usize,
    pub terminal_task_records: usize,
    pub producers_running: usize,
    pub producers_exited: usize,
    pub ends_published: usize,
    pub ends_acknowledged: usize,
    pub sealed: usize,
    pub data_positions: usize,
    pub payload_bytes: usize,
    pub segments: usize,
    pub deliveries: usize,
    pub retained_reservations: usize,
    pub metadata_holders: usize,
    pub metadata_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootOwnershipSnapshotError {
    RegistryPoisoned,
    ChannelPoisoned,
}

impl std::fmt::Display for RootOwnershipSnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RegistryPoisoned => "root ownership registry mutex poisoned",
            Self::ChannelPoisoned => "root ownership channel mutex poisoned",
        })
    }
}
impl std::error::Error for RootOwnershipSnapshotError {}

impl RootOwnershipSnapshot {
    pub(crate) fn try_add(&mut self, other: Self) -> Option<()> {
        self.channels = self.channels.checked_add(other.channels)?;
        self.terminal_task_records = self
            .terminal_task_records
            .checked_add(other.terminal_task_records)?;
        self.producers_running = self
            .producers_running
            .checked_add(other.producers_running)?;
        self.producers_exited = self.producers_exited.checked_add(other.producers_exited)?;
        self.ends_published = self.ends_published.checked_add(other.ends_published)?;
        self.ends_acknowledged = self
            .ends_acknowledged
            .checked_add(other.ends_acknowledged)?;
        self.sealed = self.sealed.checked_add(other.sealed)?;
        self.data_positions = self.data_positions.checked_add(other.data_positions)?;
        self.payload_bytes = self.payload_bytes.checked_add(other.payload_bytes)?;
        self.segments = self.segments.checked_add(other.segments)?;
        self.deliveries = self.deliveries.checked_add(other.deliveries)?;
        self.retained_reservations = self
            .retained_reservations
            .checked_add(other.retained_reservations)?;
        self.metadata_holders = self.metadata_holders.checked_add(other.metadata_holders)?;
        self.metadata_bytes = self.metadata_bytes.checked_add(other.metadata_bytes)?;
        Some(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootChannelError {
    Identity,
    UnknownRoot,
    Closed,
    Capacity(RootCapacityRefusal),
    MetadataTooSmall { requested: usize, minimum: usize },
    Backing(novarocks_execution_contract::failure::SharedFailureCause),
    Observable(novarocks_execution::runtime::observable::ObservableCapacityError),
    Execution(novarocks_execution::runtime::fragment::ExecutionFailure),
    Contract(novarocks_result_contract::RootContractError),
    Record(novarocks_execution_contract::root_result::RootResultError),
    Lifetime(novarocks_execution_contract::root_lifetime::RootLifetimeError),
    ForeignBuilder,
    SendGeometry,
    Funding(crate::result_buffer::ResultReservationError),
    Producer,
    Payload,
    Stream(StreamError),
}
#[derive(Debug)]
enum RootProducerFailure {
    RowCount { committed: u64, requested: u64 },
    ExitedWithoutEnd,
}
impl std::fmt::Display for RootProducerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowCount {
                committed,
                requested,
            } => write!(
                f,
                "root output row count exceeds its u64 carrier: {committed} + {requested}"
            ),
            Self::ExitedWithoutEnd => f.write_str("root producer exited without publishing End"),
        }
    }
}
impl std::error::Error for RootProducerFailure {}
impl novarocks_execution_contract::failure::FailureProjection for RootProducerFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        match self {
            Self::RowCount {
                committed,
                requested,
            } => FailureFact::new(FailureClass::Representation, R::RootOutputRowCount.reason())
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::MinimumUnit,
                    layout: RepresentationLayout {
                        domain: domain::WORKER,
                        code: std::num::NonZeroU16::new(1).unwrap(),
                    },
                    required: u64::try_from(u128::from(*committed) + u128::from(*requested)).ok(),
                    extent_limit: Some(u64::MAX),
                    representable_prefix: None,
                }),
            Self::ExitedWithoutEnd => FailureFact::new(
                FailureClass::Invariant,
                R::RootProducerExitedWithoutEnd.reason(),
            ),
        }
    }
}

pub enum ContextRootRoute {
    Read(RootChannelRead),
    AwaitTerminalControl { accepted_consumed: u64 },
    Preparing,
    UnknownRoot,
    Mismatch,
    Busy(novarocks_execution_contract::failure::ResourceFacts),
    Refused(RootChannelError),
}
impl std::fmt::Display for RootChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(error) => error.fmt(f),
            _ => write!(f, "bounded root channel rejected operation: {self:?}"),
        }
    }
}
impl std::error::Error for RootChannelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Funding(cause) => Some(cause),
            Self::Stream(cause) => Some(cause),
            Self::Capacity(cause) => Some(cause),
            Self::Backing(cause) => Some(cause),
            Self::Observable(cause) => Some(cause),
            Self::Execution(cause) => Some(cause),
            Self::Contract(cause) => Some(cause),
            Self::Record(cause) => Some(cause),
            Self::Lifetime(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootChannelError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        let (class, reason) = match self {
            Self::Funding(cause) => return cause.failure_fact(),
            Self::Stream(cause) => return cause.failure_fact(),
            Self::Capacity(cause) => return cause.failure_fact(),
            Self::Backing(cause) => return cause.failure_fact(),
            Self::Observable(cause) => return cause.failure_fact(),
            Self::Execution(cause) => return cause.failure_fact(),
            Self::Contract(cause) => return cause.failure_fact(),
            Self::Record(cause) => return cause.failure_fact(),
            Self::Lifetime(cause) => return cause.failure_fact(),
            Self::Closed => return FailureFact::stop_observation(R::RootChannelClosed.reason()),
            Self::Identity => (FailureClass::Data, R::RootReadIdentity),
            Self::UnknownRoot => (FailureClass::Data, R::RootReadUnknown),
            Self::Producer => (FailureClass::Invariant, R::RootProducerContract),
            Self::Payload => (FailureClass::Invariant, R::RootPayloadContract),
            Self::MetadataTooSmall { .. } => (FailureClass::Invariant, R::RootMetadataTooSmall),
            Self::ForeignBuilder => (FailureClass::Invariant, R::RootForeignBuilder),
            Self::SendGeometry => (FailureClass::Invariant, R::RootSendGeometry),
        };
        FailureFact::new(class, reason.reason())
    }
}
impl RootChannelError {
    pub fn into_execution(self) -> novarocks_execution::runtime::fragment::ExecutionFailure {
        match self {
            Self::Execution(cause) => cause,
            other => novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(other),
        }
    }
    fn reserve_backing(
        operation: &'static str,
        dimension: novarocks_execution_contract::failure::ResourceDimension,
        requested: usize,
        source: std::collections::TryReserveError,
    ) -> Self {
        Self::Backing(
            novarocks_execution_contract::failure::SharedFailureCause::new(RootBackingRefusal {
                operation,
                dimension,
                requested,
                source,
            }),
        )
    }
}
/// A refusal of this root's fixed metadata/position or actual backing coverage.
/// It grants no allocation authority and makes no physical allocation claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCapacityRefusal(novarocks_execution_contract::failure::ResourceFacts);
impl RootCapacityRefusal {
    pub const fn resource_facts(&self) -> novarocks_execution_contract::failure::ResourceFacts {
        self.0
    }
    fn bytes(requested: Option<usize>, limit: Option<usize>, committed: Option<usize>) -> Self {
        Self::facts(
            novarocks_execution_contract::failure::ResourceDimension::Bytes,
            requested,
            limit,
            committed,
            Some(novarocks_execution_contract::failure::ResourceScope::Task),
        )
    }
    fn positions(limit: usize, committed: usize) -> Self {
        Self::facts(
            novarocks_execution_contract::failure::ResourceDimension::Positions,
            Some(1),
            Some(limit),
            Some(committed),
            Some(novarocks_execution_contract::failure::ResourceScope::Task),
        )
    }
    fn unavailable(requested: usize) -> Self {
        Self::facts(
            novarocks_execution_contract::failure::ResourceDimension::Bytes,
            Some(requested),
            None,
            None,
            None,
        )
    }
    fn facts(
        dimension: novarocks_execution_contract::failure::ResourceDimension,
        requested: Option<usize>,
        limit: Option<usize>,
        committed: Option<usize>,
        scope: Option<novarocks_execution_contract::failure::ResourceScope>,
    ) -> Self {
        use novarocks_execution_contract::failure::*;
        Self(ResourceFacts {
            dimension,
            request_scope: Some(ResourceScope::Request),
            constraint_scope: scope,
            requested: requested.and_then(|n| u64::try_from(n).ok()),
            required_growth: None,
            limit: limit.and_then(|n| u64::try_from(n).ok()),
            committed: committed.and_then(|n| u64::try_from(n).ok()),
            live: None,
        })
    }
}
impl std::fmt::Display for RootCapacityRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "root capacity refused {:?}: requested={:?} committed={:?} limit={:?}",
            self.0.dimension, self.0.requested, self.0.committed, self.0.limit
        )
    }
}
impl std::error::Error for RootCapacityRefusal {}
impl novarocks_execution_contract::failure::FailureProjection for RootCapacityRefusal {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Resource,
            crate::failure_reason::WorkerFailureReason::RootCapacity.reason(),
        )
        .with_resource(self.0)
    }
}
#[derive(Debug)]
struct RootBackingRefusal {
    operation: &'static str,
    dimension: novarocks_execution_contract::failure::ResourceDimension,
    requested: usize,
    source: std::collections::TryReserveError,
}
impl std::fmt::Display for RootBackingRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "root {} backing could not reserve {} {:?}: {}",
            self.operation, self.requested, self.dimension, self.source
        )
    }
}
impl std::error::Error for RootBackingRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootBackingRefusal {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Resource,
            crate::failure_reason::WorkerFailureReason::RootBackingReservation.reason(),
        )
        .with_resource(ResourceFacts {
            dimension: self.dimension,
            requested: u64::try_from(self.requested).ok(),
            request_scope: Some(ResourceScope::Request),
            constraint_scope: None,
            required_growth: None,
            limit: None,
            live: None,
            committed: None,
        })
    }
}
impl From<StreamError> for RootChannelError {
    fn from(error: StreamError) -> Self {
        Self::Stream(error)
    }
}

const ROOT_WRITABLE_ENTRIES: usize = 2;
const ROOT_NOTIFICATION_PHASES: usize = 3 + 4 * ROOT_WRITABLE_ENTRIES;
type RootMetadataPanic = Box<dyn std::any::Any + Send + 'static>;
#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum RootMetadataPhase {
    #[default]
    NotStarted,
    Started,
    Exited,
    Unresolved,
}
#[derive(Default)]
struct RootMetadataFault {
    primary: Option<std::mem::ManuallyDrop<RootMetadataPanic>>,
    secondary: Option<std::mem::ManuallyDrop<RootMetadataPanic>>,
}
struct RootNotificationWork {
    batch: BoundedNotification<ROOT_WRITABLE_ENTRIES>,
    phases: [RootMetadataPhase; ROOT_NOTIFICATION_PHASES],
    faults: [RootMetadataFault; ROOT_NOTIFICATION_PHASES],
    failed: bool,
}
impl Default for RootNotificationWork {
    fn default() -> Self {
        Self {
            batch: BoundedNotification::new(),
            phases: [RootMetadataPhase::NotStarted; ROOT_NOTIFICATION_PHASES],
            faults: std::array::from_fn(|_| RootMetadataFault::default()),
            failed: false,
        }
    }
}
impl RootNotificationWork {
    fn run(
        &mut self,
        owner: &PhysicalOwners,
        phase: usize,
        action: impl FnOnce(&mut BoundedNotification<ROOT_WRITABLE_ENTRIES>),
    ) -> bool {
        if self.phases[phase] != RootMetadataPhase::NotStarted {
            return false;
        }
        self.phases[phase] = RootMetadataPhase::Started;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action(&mut self.batch))) {
            Ok(()) => {
                self.phases[phase] = RootMetadataPhase::Exited;
                true
            }
            Err(payload) => {
                self.phases[phase] = RootMetadataPhase::Unresolved;
                self.failed = true;
                // Save the original opaque before publishing neutral health or
                // attempting its one foreign destructor. It never enters Task.
                self.faults[phase].primary = Some(std::mem::ManuallyDrop::new(payload));
                owner.report_metadata_fault(false);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                    std::mem::ManuallyDrop::drop(self.faults[phase].primary.as_mut().unwrap());
                }));
                // A destructor that unwound leaves an invalid shell, never an
                // object which may be moved out or implicitly destroyed again.
                unsafe { std::ptr::write(&mut self.faults[phase].primary, None) };
                if let Err(secondary) = result {
                    self.faults[phase].secondary = Some(std::mem::ManuallyDrop::new(secondary));
                }
                false
            }
        }
    }
    fn notify(&mut self, owner: &PhysicalOwners) {
        self.run(owner, 0, |_| owner.changed.notify_waiters());
        self.run(owner, 1, |_| {
            if let Some(source) = owner.progress.get().and_then(Weak::upgrade) {
                source.note_progress();
            }
        });
        let mut snapshot_valid = true;
        self.run(owner, 2, |batch| {
            if owner.writable.snapshot_bounded_notification(batch).is_err() {
                snapshot_valid = false;
            }
        });
        if !snapshot_valid {
            self.phases[2] = RootMetadataPhase::Unresolved;
            self.failed = true;
            owner.report_metadata_fault(true);
        }
        for index in 0..ROOT_WRITABLE_ENTRIES {
            if !self.batch.occupied(index) {
                continue;
            }
            let base = 3 + 4 * index;
            self.run(owner, base, |batch| batch.invoke(index));
            // Call unwind does not invalidate its still-owned callback. Its
            // captured owners are independently destroyed exactly once.
            if !self.run(owner, base + 1, |batch| batch.drop_callback(index)) {
                continue;
            }
            // A callback destructor unknown must keep its original registration
            // position and backing; another entry remains independently safe.
            if !self.run(owner, base + 2, |batch| batch.drop_registration(index)) {
                continue;
            }
            self.run(owner, base + 3, |batch| batch.drop_retained_owner(index));
        }
        if !self.failed {
            self.batch.reset_after_exit();
            self.phases.fill(RootMetadataPhase::NotStarted);
        }
    }
}
struct RootWritableNotification {
    work: Option<RootNotificationWork>,
    failed: bool,
    self_pin: Option<Arc<PhysicalOwners>>,
    running: bool,
    pending: bool,
    joined: bool,
}
impl Default for RootWritableNotification {
    fn default() -> Self {
        Self {
            work: Some(RootNotificationWork::default()),
            failed: false,
            self_pin: None,
            running: false,
            pending: false,
            joined: false,
        }
    }
}
/// One synchronous borrower of the channel's existing finite notification
/// storage. Drop only returns inert ManuallyDrop fields to that original slot.
/// It never invokes a callback, disposes opaque, or drops a foreign holder.
struct RootNotificationRun {
    owner: Arc<PhysicalOwners>,
    work: Option<RootNotificationWork>,
    completed: bool,
}
impl Drop for RootNotificationRun {
    fn drop(&mut self) {
        let failed = !self.completed || self.work.as_ref().is_some_and(|work| work.failed);
        {
            let mut slot = self
                .owner
                .notification
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            slot.failed |= failed;
            slot.work = self.work.take();
            if failed {
                slot.running = false;
                // Unknown foreign work retains its original self pin and
                // physical notification position; it can never join.
            }
        }
        if !self.completed {
            self.owner.report_metadata_fault(true);
        }
        // Healthy per-pass return does not release notification ownership:
        // pending work is decided by the same gate before the final release.
    }
}

struct PhysicalOwners {
    task: novarocks_execution_contract::TaskIdentity,
    health: Arc<ExecutionHealth>,
    notification: Mutex<RootWritableNotification>,
    metadata_notification_held: AtomicBool,
    segments: AtomicUsize,
    deliveries: AtomicUsize,
    retained_reservations: AtomicUsize,
    fixed_metadata_bytes: AtomicUsize,
    fixed_metadata_holders: AtomicUsize,
    changed: Notify,
    writable: Arc<Observable>,
    progress: OnceLock<Weak<TaskStatusSource>>,
    // Fixed metadata follows all physical aliases, including End-only sends.
    _fixed_credit: ResultWriteCredit,
    // The original wallet itself also outlives the final physical alias.
    _budget_owner: Arc<ResultRetainedBudget>,
    #[cfg(test)]
    test_checkpoint: OnceLock<Arc<dyn Fn(RootNotificationTestPoint) + Send + Sync>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootNotificationTestPoint {
    OwnerExitCommitted,
    NotificationPassReturned,
}

/// A structural sub-allocation of the already charged fixed root envelope.
/// This issues no allocator wallet or MEM funding; hosts check the real
/// capacity of their finite driver/producer metadata before constructing it.
#[must_use = "retain root metadata coverage through the actual owner's exit"]
pub struct RootMetadataReservation {
    bytes: usize,
    owner: Arc<PhysicalOwners>,
}
impl Drop for RootMetadataReservation {
    fn drop(&mut self) {
        self.owner.exit_and_wake(|| {
            self.owner
                .fixed_metadata_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            self.owner
                .fixed_metadata_holders
                .fetch_sub(1, Ordering::AcqRel);
        });
    }
}
impl PhysicalOwners {
    fn report_metadata_fault(&self, invariant: bool) {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
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
                    R::RootMetadataNotificationInvariant.reason()
                } else {
                    R::RootMetadataNotification.reason()
                },
            ),
        );
    }
    // Only private, inert owner-counter/state mutations may run under this
    // gate. No callback, payload destruction, or foreign owner Drop runs here.
    fn arm_notification_locked(self: &Arc<Self>, slot: &mut RootWritableNotification) -> bool {
        if slot.joined || slot.failed {
            return false;
        }
        slot.pending = true;
        if slot.running {
            return false;
        }
        slot.running = true;
        self.metadata_notification_held
            .store(true, Ordering::Release);
        slot.self_pin = Some(self.clone());
        true
    }
    fn arm_exit(self: &Arc<Self>, exit: impl FnOnce()) -> bool {
        let mut slot = self.notification.lock().unwrap_or_else(|p| p.into_inner());
        let drive = self.arm_notification_locked(&mut slot);
        // The physical owner cannot disappear before notification ownership
        // has been installed, nor can a drainer consume this handoff first.
        exit();
        drive
    }
    fn exit_and_wake(self: &Arc<Self>, exit: impl FnOnce()) {
        let drive = self.arm_exit(exit);
        #[cfg(test)]
        self.checkpoint(RootNotificationTestPoint::OwnerExitCommitted);
        if drive {
            self.drain_notifications();
        }
    }
    fn note_progress(&self) {
        if let Some(source) = self.progress.get().and_then(Weak::upgrade) {
            source.note_progress();
        }
    }
    #[cfg(test)]
    fn checkpoint(&self, point: RootNotificationTestPoint) {
        if let Some(checkpoint) = self.test_checkpoint.get() {
            checkpoint(point);
        }
    }
    fn wake(self: &Arc<Self>) {
        let drive = {
            let mut slot = self.notification.lock().unwrap_or_else(|p| p.into_inner());
            self.arm_notification_locked(&mut slot)
        };
        if drive {
            self.drain_notifications();
        }
    }
    fn drain_notifications(self: &Arc<Self>) {
        loop {
            let work = {
                let mut slot = self.notification.lock().unwrap_or_else(|p| p.into_inner());
                if slot.failed {
                    slot.running = false;
                    return;
                }
                slot.pending = false;
                slot.work.take()
            };
            let mut run = RootNotificationRun {
                owner: self.clone(),
                work,
                completed: false,
            };
            if let Some(work) = run.work.as_mut() {
                work.notify(self);
            } else {
                self.report_metadata_fault(true);
            }
            let failed = run.work.as_ref().is_none_or(|work| work.failed);
            run.completed = run.work.is_some();
            drop(run);
            #[cfg(test)]
            self.checkpoint(RootNotificationTestPoint::NotificationPassReturned);
            if failed {
                return;
            }
            let pin = {
                let mut slot = self.notification.lock().unwrap_or_else(|p| p.into_inner());
                if slot.pending {
                    // One continuous physical owner covers every coalesced
                    // pass. There is no idle gap between callbacks.
                    continue;
                }
                slot.running = false;
                self.metadata_notification_held
                    .store(false, Ordering::Release);
                slot.self_pin.take()
            };
            // This method still strongly owns PhysicalOwners, so releasing
            // its healthy pin cannot run its final destructor under the gate.
            drop(pin);
            // This is revision-only, not a foreign readiness callback. Publish
            // it after the final physical notification position has exited.
            self.note_progress();
            return;
        }
    }
}
struct PhysicalSegmentPosition(Arc<PhysicalOwners>);
struct PhysicalReservationPosition(Arc<PhysicalOwners>, bool);
impl Drop for PhysicalReservationPosition {
    fn drop(&mut self) {
        if self.1 {
            self.0.exit_and_wake(|| {
                self.0.retained_reservations.fetch_sub(1, Ordering::AcqRel);
            });
        } else {
            {
                let _slot = self
                    .0
                    .notification
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                self.0.retained_reservations.fetch_sub(1, Ordering::AcqRel);
            }
            // A failed tentative admission has no foreign notification work.
            self.0.note_progress();
        }
    }
}
struct RetainedReservationOwner {
    credit: Mutex<ResultWriteCredit>,
    _position: PhysicalReservationPosition,
}
impl RetainedReservationOwner {
    fn release(&self, released: usize) {
        let mut credit = self.credit.lock().unwrap();
        let retained = credit
            .bytes()
            .checked_sub(released)
            .expect("root credit releases its own bytes");
        credit
            .shrink_to(retained)
            .expect("root credit only shrinks");
    }
}
impl Drop for PhysicalSegmentPosition {
    fn drop(&mut self) {
        self.0.exit_and_wake(|| {
            self.0.segments.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

/// Owns capacity before allocating an unpublished segment. There is one
/// active builder; queued and retired backing positions are independently
/// bounded. Publishing transfers this very allocation without a copy.
pub struct RootSegmentBuilder {
    // The backing is destroyed before its credit and physical position.
    backing: Vec<u8>,
    _credit: ResultWriteCredit,
    active: ActiveBuilderPosition,
    position: PhysicalSegmentPosition,
}
impl RootSegmentBuilder {
    pub fn output(&mut self) -> &mut [u8] {
        self.output_at(0)
    }
    /// Initialize no more than one encoding quantum. Reserving a segment
    /// must not perform a whole-megabyte zero-fill in the driver's turn.
    pub fn output_at(&mut self, offset: usize) -> &mut [u8] {
        assert!(offset <= self.backing.len() && offset <= RootProfileV1::SEGMENT_BYTES);
        let end = (offset + RootProfileV1::EMIT_BYTES_PER_TURN).min(RootProfileV1::SEGMENT_BYTES);
        if self.backing.len() < end {
            self.backing.resize(end, 0);
        }
        &mut self.backing[offset..end]
    }
    pub fn capacity_bytes(&self) -> usize {
        self.backing.capacity()
    }
    fn into_body(mut self, bytes: usize) -> Bytes {
        self.backing.truncate(bytes);
        crate::guarded_bytes::bytes_with_exit_guard(
            self.backing,
            RootSegmentExit {
                _credit: self._credit,
                _position: self.position,
                _active: self.active,
            },
        )
    }
}

// The vector and its Bytes wrapper allocation exit before these original
// grants. Keeping them as a separate guard closes the wrapper's allocator
// tail, including a last remote body alias after logical ACK or context seal.
struct RootSegmentExit {
    _credit: ResultWriteCredit,
    _active: ActiveBuilderPosition,
    _position: PhysicalSegmentPosition,
}
struct ActiveBuilderPosition(Option<Weak<RootResultChannel>>);
impl Drop for ActiveBuilderPosition {
    fn drop(&mut self) {
        if let Some(channel) = self.0.take().and_then(|channel| channel.upgrade()) {
            channel.state.lock().unwrap().active_builder = false;
            channel.physical.wake();
        }
    }
}

struct ChannelState {
    stream: OrderedRetainedStream,
    payloads: VecDeque<RootResultData>,
    retiring_payloads: Option<VecDeque<RootResultData>>,
    seal_in_progress: bool,
    lifetime: RootResultLifetime,
    context_owned: bool,
    producer_started: bool,
    producer_exited: bool,
    finishing: bool,
    failed: Option<novarocks_execution::runtime::fragment::ExecutionFailure>,
    output_rows: u64,
    active_builder: bool,
}

impl ChannelState {
    fn acknowledge_and_retire(
        &mut self,
        consumed: u64,
    ) -> Result<(u64, [Option<RootResultData>; 2]), RootChannelError> {
        let ack = self.stream.acknowledge(consumed)?;
        // The fixed W=2 queue exits outside the channel/context fence.
        let mut retired: [Option<RootResultData>; 2] = [None, None];
        for slot in &mut retired {
            if self
                .payloads
                .front()
                .is_some_and(|data| data.sequence().get() <= ack.accepted_consumed)
            {
                *slot = self.payloads.pop_front();
            }
        }
        Ok((ack.accepted_consumed, retired))
    }
}

// The pinned std Arc allocation is repr(C): two counters followed by T.
// Account for each actual heap owner once, including alignment padding.
#[repr(C)]
struct RootArcAllocation<T> {
    _strong: AtomicUsize,
    _weak: AtomicUsize,
    _value: T,
}

struct RootCoreMetadataLayout {
    bytes: usize,
    stream_backing_bytes: usize,
    payload_backing_bytes: usize,
}

pub struct RootResultChannel {
    spec: RootResultWriteSpec,
    budget: Arc<ResultRetainedBudget>,
    limits: WorkerResultRetainedLimits,
    physical: Arc<PhysicalOwners>,
    state: Mutex<ChannelState>,
    _budget_wake: ObserverSubscription,
}
impl RootResultChannel {
    // One installed producer bridge and one scoped host observer. Driver
    // readiness lives in the separate RootInputAuthority registry.
    const WRITABLE_CALLBACK_CAPACITY: usize = ROOT_WRITABLE_ENTRIES;

    fn retained_reservation_positions() -> usize {
        let geometry = NativeResultSupportGeometry::V1;
        (2 + geometry.root_live_send_holders * geometry.root_independent_payload_copies_per_send)
            as usize
    }

    fn credit_control_backing_bytes(task: novarocks_execution_contract::TaskIdentity) -> usize {
        let geometry = NativeResultSupportGeometry::V1;
        let positions = Self::retained_reservation_positions();
        let segments = (geometry.root_active_segment_positions
            + geometry.root_queued_segment_positions
            + geometry.root_retired_segment_tail_positions) as usize;
        // One budget closure for the fixed root grant, each segment and each
        // finite reservation. Reservation's second Box captures this exact
        // owner (the release closure calls its method without field splitting).
        let bytes = (1 + segments + positions)
            * ResultRetainedBudget::stream_credit_control_backing_bytes(ResultBufferKey::Task(
                task,
            ))
            + positions * std::mem::size_of::<RetainedReservationOwner>();
        // Source-audited pinned std 1.98.1 Darwin Box<pal::Mutex>: one for each
        // reservation's private credit lock, ChannelState and notification locks.
        // No concurrent initialization is possible for these private credits.
        #[cfg(target_vendor = "apple")]
        let bytes = bytes
            + std::alloc::Layout::array::<(isize, [u8; 56])>(positions + 2)
                .expect("finite root mutex layouts fit usize")
                .size();
        bytes
    }
    fn bytes_owner_metadata_bytes() -> usize {
        let geometry = NativeResultSupportGeometry::V1;
        let segments = (geometry.root_active_segment_positions
            + geometry.root_queued_segment_positions
            + geometry.root_retired_segment_tail_positions) as usize;
        let sends = geometry.root_live_send_holders as usize;
        segments * crate::guarded_bytes::owner_wrapper_bytes::<Vec<u8>, RootSegmentExit>()
            + sends
                * (crate::guarded_bytes::owner_wrapper_bytes::<Bytes, RootDeliveryOwner>()
                    + RootDeliveryOwner::backing_metadata_bytes())
    }
    fn budget_wake_callback(owner: Weak<PhysicalOwners>) -> impl Fn() + Send + Sync + 'static {
        move || {
            if let Some(physical) = owner.upgrade() {
                physical.wake();
            }
        }
    }

    fn core_metadata_layout(
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Result<RootCoreMetadataLayout, RootChannelError> {
        use std::alloc::Layout;
        let fixed =
            NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        let overflow =
            || RootChannelError::Capacity(RootCapacityRefusal::bytes(None, Some(fixed), None));
        let stream_positions = RootProfileV1::DATA_POSITIONS
            .checked_add(1)
            .ok_or_else(overflow)?;
        let stream_backing_bytes = Layout::array::<RetainedStreamItem>(stream_positions)
            .map_err(|_| overflow())?
            .size();
        let payload_backing_bytes = Layout::array::<RootResultData>(RootProfileV1::DATA_POSITIONS)
            .map_err(|_| overflow())?
            .size();
        fn callback_arc_bytes<T>(_: &T) -> usize {
            Layout::new::<RootArcAllocation<T>>().size()
        }
        // Construct only the same callback value on the stack. This preflight
        // does not allocate or upgrade its empty Weak metadata capability.
        let budget_wake = Self::budget_wake_callback(Weak::new());
        let parts = [
            Layout::new::<RootArcAllocation<Self>>().size(),
            Layout::new::<RootArcAllocation<PhysicalOwners>>().size(),
            Layout::new::<RootArcAllocation<novarocks_result_contract::RootOutputContract>>()
                .size(),
            Layout::new::<RootArcAllocation<Observable>>().size(),
            stream_backing_bytes,
            payload_backing_bytes,
            Observable::bounded_backing_bytes(Self::WRITABLE_CALLBACK_CAPACITY)
                .map_err(RootChannelError::Observable)?,
            Observable::unbounded_subscription_backing_bytes()
                .map_err(RootChannelError::Observable)?,
            callback_arc_bytes(&budget_wake),
            Self::credit_control_backing_bytes(task),
            Self::bytes_owner_metadata_bytes(),
        ];
        let bytes = parts.into_iter().try_fold(0usize, |sum, part| {
            sum.checked_add(part).ok_or_else(overflow)
        })?;
        Ok(RootCoreMetadataLayout {
            bytes,
            stream_backing_bytes,
            payload_backing_bytes,
        })
    }

    pub fn try_open(
        spec: RootResultWriteSpec,
        budget: Arc<ResultRetainedBudget>,
        limits: WorkerResultRetainedLimits,
        health: Arc<ExecutionHealth>,
    ) -> Result<Arc<Self>, RootChannelError> {
        spec.contract
            .validate_purpose()
            .map_err(RootChannelError::Contract)?;
        let fixed =
            NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        // Admit the sealed owner/queue layouts before constructing any of the
        // root's retained metadata. The wire frame envelope is independent.
        let layout = Self::core_metadata_layout(spec.task)?;
        let schema_bytes = match spec.contract.output() {
            novarocks_result_contract::FrozenRootOutput::ClientRows(schema) => {
                schema.backing_bytes()
            }
            novarocks_result_contract::FrozenRootOutput::ScalarValue(schema) => {
                schema.backing_bytes()
            }
            _ => 0,
        };
        let metadata_bytes = schema_bytes
            .checked_add(layout.bytes)
            .filter(|bytes| *bytes <= fixed)
            .ok_or_else(|| {
                RootChannelError::Capacity(RootCapacityRefusal::bytes(
                    schema_bytes.checked_add(layout.bytes),
                    Some(fixed),
                    None,
                ))
            })?;
        let credit = match budget
            .try_reserve(
                ResultBufferKey::Task(spec.task),
                limits.per_root().get(),
                fixed,
            )
            .map_err(RootChannelError::Funding)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => {
                return Err(RootChannelError::Capacity(
                    RootCapacityRefusal::unavailable(fixed),
                ));
            }
        };
        let stream = OrderedRetainedStream::try_new(RetainedStreamWindow::try_new(
            RootProfileV1::DATA_POSITIONS,
            RootProfileV1::DATA_POSITIONS * RootProfileV1::SEGMENT_BYTES,
        )?)?;
        // Public VecDeque constructors may return larger actual capacities.
        // Reject before constructing any further root-owned metadata.
        let actual_stream_bytes = stream.metadata_backing_bytes();
        if actual_stream_bytes > layout.stream_backing_bytes {
            return Err(RootChannelError::Capacity(RootCapacityRefusal::bytes(
                Some(actual_stream_bytes),
                Some(layout.stream_backing_bytes),
                None,
            )));
        }
        let mut payloads = VecDeque::new();
        payloads
            .try_reserve_exact(RootProfileV1::DATA_POSITIONS)
            .map_err(|source| {
                RootChannelError::reserve_backing(
                    "payload queue",
                    novarocks_execution_contract::failure::ResourceDimension::Items,
                    RootProfileV1::DATA_POSITIONS,
                    source,
                )
            })?;
        let actual_payload_bytes = payloads.capacity() * std::mem::size_of::<RootResultData>();
        if actual_payload_bytes > layout.payload_backing_bytes {
            return Err(RootChannelError::Capacity(RootCapacityRefusal::bytes(
                Some(actual_payload_bytes),
                Some(layout.payload_backing_bytes),
                None,
            )));
        }
        let physical = Arc::new(PhysicalOwners {
            task: spec.task,
            health,
            notification: Mutex::new(RootWritableNotification::default()),
            metadata_notification_held: AtomicBool::new(false),
            segments: AtomicUsize::new(0),
            deliveries: AtomicUsize::new(0),
            retained_reservations: AtomicUsize::new(0),
            fixed_metadata_bytes: AtomicUsize::new(metadata_bytes),
            fixed_metadata_holders: AtomicUsize::new(0),
            changed: Notify::new(),
            writable: Arc::new(
                Observable::try_bounded(Self::WRITABLE_CALLBACK_CAPACITY)
                    .map_err(RootChannelError::Observable)?,
            ),
            progress: OnceLock::new(),
            _fixed_credit: credit,
            _budget_owner: Arc::clone(&budget),
            #[cfg(test)]
            test_checkpoint: OnceLock::new(),
        });
        // Realize the new platform notification mutex before publication. Its
        // backing is included in the same finite core-capacity calculation.
        drop(physical.notification.lock().unwrap());
        let budget_wake =
            budget
                .writable_observable()
                .subscribe(Arc::new(Self::budget_wake_callback(Arc::downgrade(
                    &physical,
                ))));
        let channel = Arc::new(Self {
            state: Mutex::new(ChannelState {
                lifetime: RootResultLifetime::new(spec.task),
                stream,
                payloads,
                retiring_payloads: None,
                seal_in_progress: false,
                context_owned: false,
                producer_started: false,
                producer_exited: false,
                finishing: false,
                failed: None,
                output_rows: 0,
                active_builder: false,
            }),
            spec,
            budget,
            limits,
            physical,
            _budget_wake: budget_wake,
        });
        // Initialize Darwin's lazy mutex before this Arc can be shared. A
        // racing first lock would temporarily allocate multiple candidates.
        drop(channel.state.lock().unwrap());
        Ok(channel)
    }
    pub fn spec(&self) -> &RootResultWriteSpec {
        &self.spec
    }
    pub fn try_reserve_metadata(
        &self,
        bytes: usize,
    ) -> Result<RootMetadataReservation, RootChannelError> {
        let state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        if bytes < std::mem::size_of::<RootMetadataReservation>() {
            return Err(RootChannelError::MetadataTooSmall {
                requested: bytes,
                minimum: std::mem::size_of::<RootMetadataReservation>(),
            });
        }
        let geometry = NativeResultSupportGeometry::V1;
        let positions =
            (geometry.root_maximum_root_drivers + geometry.root_live_send_holders + 1) as usize;
        self.physical
            .fixed_metadata_holders
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < positions).then_some(n + 1)
            })
            .map_err(|committed| {
                RootChannelError::Capacity(RootCapacityRefusal::positions(positions, committed))
            })?;
        let limit = geometry.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        if let Err(committed) = self.physical.fixed_metadata_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |used| used.checked_add(bytes).filter(|total| *total <= limit),
        ) {
            self.physical
                .fixed_metadata_holders
                .fetch_sub(1, Ordering::AcqRel);
            return Err(RootChannelError::Capacity(RootCapacityRefusal::bytes(
                Some(bytes),
                Some(limit),
                Some(committed),
            )));
        }
        drop(state);
        let reservation = RootMetadataReservation {
            bytes,
            owner: Arc::clone(&self.physical),
        };
        Ok(reservation)
    }
    pub fn writable_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.physical.writable)
    }
    pub fn try_reserve(&self, bytes: usize) -> Result<ResultWriteAdmission, RootChannelError> {
        let state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        // One scratch owner, one input and the exact independent send copies.
        // Small reservations cannot grow an unbounded list of credit objects.
        let positions = Self::retained_reservation_positions();
        if self
            .physical
            .retained_reservations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < positions).then_some(n + 1)
            })
            .is_err()
        {
            return Ok(ResultWriteAdmission::Blocked);
        }
        let mut position = PhysicalReservationPosition(Arc::clone(&self.physical), false);
        drop(state);
        let granted = self
            .budget
            .try_reserve(
                ResultBufferKey::Task(self.spec.task),
                self.limits.per_root().get(),
                bytes,
            )
            .map_err(RootChannelError::Funding)?;
        let ResultWriteAdmission::Granted(credit) = granted else {
            return Ok(ResultWriteAdmission::Blocked);
        };
        if !self.state.lock().unwrap().lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        position.1 = true;
        let owner = RetainedReservationOwner {
            credit: Mutex::new(credit),
            _position: position,
        };
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            move |released| owner.release(released),
        )))
    }
    /// Only the context's registry binds its original progress source. This
    /// callback acquires no registry lock and cannot reopen a closed route.
    pub fn bind_context_progress(
        &self,
        source: &Arc<TaskStatusSource>,
    ) -> Result<(), RootChannelError> {
        if let Some(bound) = self.physical.progress.get() {
            if bound.ptr_eq(&Arc::downgrade(source)) {
                return Ok(());
            }
            return Err(RootChannelError::Identity);
        }
        self.physical
            .progress
            .set(Arc::downgrade(source))
            .map_err(|_| RootChannelError::Identity)
    }
    /// Called by the creation transaction after the exact context takes this
    /// channel. A provisional preparation channel is never a readable route.
    pub fn mark_context_owned(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        state.context_owned = true;
        Self::try_handoff(&mut state);
        Ok(())
    }
    pub fn start_producer(self: &Arc<Self>) -> Result<RootProducerExit, RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        if state.producer_started {
            return Err(RootChannelError::Producer);
        }
        state.producer_started = true;
        Ok(RootProducerExit {
            channel: Arc::clone(self),
        })
    }
    pub fn note_rows(&self, rows: u64) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() || state.finishing {
            return Err(RootChannelError::Closed);
        }
        let Some(total) = state.output_rows.checked_add(rows) else {
            if state.failed.is_none() {
                state.failed = Some(
                    novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                        RootProducerFailure::RowCount {
                            committed: state.output_rows,
                            requested: rows,
                        },
                    ),
                );
            }
            return Err(RootChannelError::Execution(
                state
                    .failed
                    .as_ref()
                    .expect("row failure was retained")
                    .clone(),
            ));
        };
        state.output_rows = total;
        Ok(())
    }
    pub fn request_finish(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        state.finishing = true;
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn is_closed(&self) -> bool {
        !self.state.lock().unwrap().lifetime.allows_read()
    }
    pub fn producer_state(&self) -> RootProducerState {
        let state = self.state.lock().unwrap();
        if let Some(error) = &state.failed {
            return RootProducerState::Failed(error.clone());
        }
        if state.lifetime.root_finished() {
            RootProducerState::ContextHeld
        } else if state.finishing {
            RootProducerState::Finishing
        } else {
            RootProducerState::Accepting
        }
    }
    pub fn try_segment(self: &Arc<Self>) -> Result<Option<RootSegmentBuilder>, RootChannelError> {
        self.try_segment_allocating(|backing, capacity| {
            backing.try_reserve_exact(capacity).map_err(|source| {
                RootChannelError::reserve_backing(
                    "segment",
                    novarocks_execution_contract::failure::ResourceDimension::Bytes,
                    capacity,
                    source,
                )
            })
        })
    }
    fn try_segment_allocating(
        self: &Arc<Self>,
        allocate: impl FnOnce(&mut Vec<u8>, usize) -> Result<(), RootChannelError>,
    ) -> Result<Option<RootSegmentBuilder>, RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if self.spec.contract.kind() == RootOutputKind::CountOnly {
            return Err(RootChannelError::Payload);
        }
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        if state.failed.is_some() || state.lifetime.end().is_some() {
            return Err(RootChannelError::Producer);
        }
        if state.active_builder
            || state.stream.snapshot().data_positions == RootProfileV1::DATA_POSITIONS
        {
            return Ok(None);
        }
        let geometry = NativeResultSupportGeometry::V1;
        let limit = (geometry.root_active_segment_positions
            + geometry.root_queued_segment_positions
            + geometry.root_retired_segment_tail_positions) as usize;
        if self.physical.segments.load(Ordering::Acquire) >= limit {
            return Ok(None);
        }
        let capacity = geometry.root_segment_backing_capacity_bytes as usize;
        let credit = match self
            .budget
            .try_reserve(
                ResultBufferKey::Task(self.spec.task),
                self.limits.per_root().get(),
                capacity,
            )
            .map_err(RootChannelError::Funding)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => return Ok(None),
        };
        self.physical.segments.fetch_add(1, Ordering::AcqRel);
        state.active_builder = true;
        drop(state);
        // Allocation failure destroys real owners and invokes readiness
        // callbacks. No such callback may run under the channel mutex.
        let mut builder = RootSegmentBuilder {
            backing: Vec::new(),
            _credit: credit,
            position: PhysicalSegmentPosition(Arc::clone(&self.physical)),
            active: ActiveBuilderPosition(Some(Arc::downgrade(self))),
        };
        allocate(&mut builder.backing, capacity)?;
        if builder.backing.capacity() > builder._credit.bytes() {
            return Err(RootChannelError::Capacity(RootCapacityRefusal::bytes(
                Some(builder.backing.capacity()),
                Some(builder._credit.bytes()),
                None,
            )));
        }
        Ok(Some(builder))
    }
    /// Publication consumes the sole builder. Data(+End) is immutable; a
    /// later finish publishes a separate End instead of changing old Data.
    pub fn publish_segment(
        &self,
        mut builder: RootSegmentBuilder,
        bytes: usize,
        end: bool,
    ) -> Result<(), RootChannelError> {
        if !Arc::ptr_eq(&builder.position.0, &self.physical) {
            return Err(RootChannelError::ForeignBuilder);
        }
        let mut state = self.state.lock().unwrap();
        if !state.active_builder {
            return Err(RootChannelError::Producer);
        }
        state.active_builder = false;
        builder.active.0 = None;
        if state.failed.is_some() {
            return Err(RootChannelError::Producer);
        }
        if bytes == 0
            || bytes > builder.backing.len()
            || bytes > RootProfileV1::SEGMENT_BYTES
            || self.spec.contract.kind() == RootOutputKind::CountOnly
        {
            return Err(RootChannelError::Payload);
        }
        if end && !state.finishing {
            return Err(RootChannelError::Producer);
        }
        let published = state.stream.publish_data(bytes, end)?;
        let terminal = published.end_sequence.map(|sequence| RootResultEnd {
            sequence: NonZeroU64::new(sequence).unwrap(),
            output_rows: state.output_rows,
        });
        let data = RootResultData::try_new(
            self.spec.contract.kind(),
            NonZeroU64::new(published.data_sequence.unwrap()).unwrap(),
            builder.into_body(bytes),
            terminal,
        )
        .map_err(RootChannelError::Record)?;
        state.payloads.push_back(data);
        if let Some(terminal) = terminal {
            state
                .lifetime
                .end_published(terminal)
                .map_err(RootChannelError::Lifetime)?;
        }
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn publish_end(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.finishing || state.active_builder || state.failed.is_some() {
            return Err(RootChannelError::Producer);
        }
        let published = state.stream.publish_end()?;
        let end = RootResultEnd {
            sequence: NonZeroU64::new(published.end_sequence.unwrap()).unwrap(),
            output_rows: state.output_rows,
        };
        state
            .lifetime
            .end_published(end)
            .map_err(RootChannelError::Lifetime)?;
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn snapshot(&self) -> RetainedStreamSnapshot {
        self.state.lock().unwrap().stream.snapshot()
    }
    /// Never waits for a producer/read fence and never creates a payload alias.
    /// A busy lock is unavailable, rather than a zero or stale ownership fact.
    pub fn try_ownership_snapshot(
        &self,
    ) -> Result<Option<RootOwnershipSnapshot>, RootOwnershipSnapshotError> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                return Err(RootOwnershipSnapshotError::ChannelPoisoned);
            }
        };
        let stream = state.stream.snapshot();
        Ok(Some(RootOwnershipSnapshot {
            channels: 1,
            terminal_task_records: 0, // The registry owns this independent fact.
            producers_running: usize::from(state.producer_started && !state.producer_exited),
            producers_exited: usize::from(state.producer_exited),
            ends_published: usize::from(stream.end_sequence.is_some()),
            ends_acknowledged: usize::from(
                stream
                    .end_sequence
                    .is_some_and(|end| stream.consumed_through >= end),
            ),
            sealed: usize::from(stream.sealed),
            data_positions: stream.data_positions,
            payload_bytes: stream.payload_bytes,
            segments: self.physical.segments.load(Ordering::Acquire),
            deliveries: self.physical.deliveries.load(Ordering::Acquire),
            retained_reservations: self.physical.retained_reservations.load(Ordering::Acquire),
            metadata_holders: self.physical.fixed_metadata_holders.load(Ordering::Acquire)
                + usize::from(
                    self.physical
                        .metadata_notification_held
                        .load(Ordering::Acquire),
                ),
            metadata_bytes: self.physical.fixed_metadata_bytes.load(Ordering::Acquire),
        }))
    }
    pub fn physical_idle(&self) -> bool {
        let state = self.state.lock().unwrap();
        (!state.producer_started || state.producer_exited)
            && !state.seal_in_progress
            && self.physical.segments.load(Ordering::Acquire) == 0
            && self.physical.deliveries.load(Ordering::Acquire) == 0
            && self.physical.retained_reservations.load(Ordering::Acquire) == 0
            && self.physical.fixed_metadata_holders.load(Ordering::Acquire) == 0
            && !self
                .physical
                .metadata_notification_held
                .load(Ordering::Acquire)
    }
    /// Close notification admission only after the sealed root's actual
    /// owners, seal work and entire notification drain have exited. Success
    /// is stable through the registry's later convergence assertion.
    pub(crate) fn try_join_physical(&self) -> bool {
        let state = self.state.lock().unwrap();
        let mut notification = self
            .physical
            .notification
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if notification.joined {
            return true;
        }
        if !state.stream.snapshot().sealed
            || state.seal_in_progress
            || (state.producer_started && !state.producer_exited)
            || self.physical.segments.load(Ordering::Acquire) != 0
            || self.physical.deliveries.load(Ordering::Acquire) != 0
            || self.physical.retained_reservations.load(Ordering::Acquire) != 0
            || self.physical.fixed_metadata_holders.load(Ordering::Acquire) != 0
            || notification.running
            || notification.pending
            || notification.failed
            || self
                .physical
                .metadata_notification_held
                .load(Ordering::Acquire)
        {
            return false;
        }
        notification.joined = true;
        true
    }
    pub fn close(&self, reason: RootRetentionClose) {
        self.seal_reads(reason);
        self.finish_seal();
    }
    /// The registry uses this under its context fence. Dropping payloads and
    /// invoking readiness callbacks is deferred until that fence is unlocked.
    pub fn seal_reads(&self, reason: RootRetentionClose) {
        let mut state = self.state.lock().unwrap();
        state.lifetime.close(reason);
        if !state.stream.snapshot().sealed {
            state.stream.seal();
            state.seal_in_progress = true;
            state.retiring_payloads = Some(std::mem::take(&mut state.payloads));
        }
    }
    pub fn finish_seal(&self) {
        let Some(retired) = self.state.lock().unwrap().retiring_payloads.take() else {
            return;
        };
        drop(retired);
        let drive = {
            let mut state = self.state.lock().unwrap();
            self.physical.arm_exit(|| state.seal_in_progress = false)
        };
        if drive {
            self.physical.drain_notifications();
        }
    }
    /// Mint the read owner while the registry still holds its context fence.
    /// A looked-up Arc alone does not prove an admitted read or join lifetime.
    pub fn begin_read(
        self: &Arc<Self>,
        read: &RootResultRead,
    ) -> Result<RootChannelRead, RootChannelError> {
        if read.root_task() != self.spec.task
            || read.profile() != self.spec.contract.profile()
            || read.kind() != self.spec.contract.kind()
        {
            return Err(RootChannelError::Identity);
        }
        let state = self.state.lock().unwrap();
        if !state.context_owned {
            return Err(RootChannelError::Identity);
        }
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        let limit = NativeResultSupportGeometry::V1.root_live_send_holders as usize;
        self.physical
            .deliveries
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then_some(n + 1)
            })
            .map_err(|committed| {
                RootChannelError::Capacity(RootCapacityRefusal::positions(limit, committed))
            })?;
        let guard = RootDeliveryOwner {
            position: Some(Arc::new(RootDeliveryPosition {
                physical: Arc::clone(&self.physical),
            })),
        };
        Ok(RootChannelRead {
            channel: Arc::clone(self),
            request: read.clone(),
            guard,
        })
    }
    pub async fn read(
        self: &Arc<Self>,
        read: &RootResultRead,
    ) -> Result<RootResultDelivery, RootChannelError> {
        match self.begin_read(read) {
            Ok(admitted) => admitted.read().await,
            Err(RootChannelError::Closed) => {
                let consumed = self
                    .state
                    .lock()
                    .unwrap()
                    .stream
                    .snapshot()
                    .consumed_through;
                Ok(RootResultDelivery {
                    reply: self.reply(consumed, RootReadOutcome::AwaitTerminalControl),
                    guard: RootDeliveryOwner { position: None },
                })
            }
            Err(error) => Err(error),
        }
    }
    async fn read_admitted(
        &self,
        read: &RootResultRead,
        guard: RootDeliveryOwner,
    ) -> Result<RootResultDelivery, RootChannelError> {
        let deadline = tokio::time::Instant::now() + read.max_wait();
        loop {
            let notified = self.physical.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let reply = self.read_once(read, &guard)?;
            if !matches!(reply.outcome, RootReadOutcome::NotReady)
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(RootResultDelivery { reply, guard });
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(RootResultDelivery { reply, guard });
            }
        }
    }
    fn read_once(
        &self,
        read: &RootResultRead,
        guard: &RootDeliveryOwner,
    ) -> Result<RootResultReply, RootChannelError> {
        let (reply, retired) = {
            let mut state = self.state.lock().unwrap();
            if !state.context_owned {
                return Err(RootChannelError::Identity);
            }
            if !state.lifetime.allows_read() {
                return Ok(self.reply(
                    state.stream.snapshot().consumed_through,
                    RootReadOutcome::AwaitTerminalControl,
                ));
            }
            let (accepted, retired) = state.acknowledge_and_retire(read.consumed())?;
            let outcome = (|| {
                Ok(match read.wanted() {
                    None => RootReadOutcome::AckOnly,
                    Some(wanted) => match state.stream.offer(wanted.get())? {
                        OfferOutcome::Retired { .. } => RootReadOutcome::Retired,
                        OfferOutcome::NotReady { .. } if state.failed.is_some() => {
                            RootReadOutcome::AwaitTerminalControl
                        }
                        OfferOutcome::NotReady { .. } => RootReadOutcome::NotReady,
                        OfferOutcome::Ready { item, .. } => match item.kind {
                            RetainedItemKind::Data { .. } => {
                                let data = state
                                    .payloads
                                    .iter()
                                    .find(|data| data.sequence().get() == item.sequence)
                                    .ok_or(RootChannelError::Payload)?;
                                let body = crate::guarded_bytes::bytes_with_exit_guard(
                                    data.body().clone(),
                                    guard.clone(),
                                );
                                RootReadOutcome::Data(
                                    RootResultData::try_new(
                                        data.kind(),
                                        data.sequence(),
                                        body,
                                        data.end_after_data(),
                                    )
                                    .map_err(|_| RootChannelError::Payload)?,
                                )
                            }
                            RetainedItemKind::End => RootReadOutcome::End(
                                state.lifetime.end().ok_or(RootChannelError::Producer)?,
                            ),
                        },
                    },
                })
            })();
            (
                outcome.map(|outcome| self.reply(accepted, outcome)),
                retired,
            )
        };
        drop(retired);
        self.physical.writable.notify_observers();
        reply
    }

    /// Apply only the request's cumulative ACK. No payload alias or new read
    /// position is constructed here. Retired allocations exit outside the
    /// channel lock, before a native sender asks for independent copy space.
    fn apply_consumed(&self, consumed: u64) -> Result<u64, RootChannelError> {
        let (accepted, retired) = {
            let mut state = self.state.lock().unwrap();
            if !state.context_owned {
                return Err(RootChannelError::Identity);
            }
            if !state.lifetime.allows_read() {
                return Err(RootChannelError::Closed);
            }
            state.acknowledge_and_retire(consumed)?
        };
        drop(retired);
        self.physical.writable.notify_observers();
        Ok(accepted)
    }
    fn reply(&self, accepted_consumed: u64, outcome: RootReadOutcome) -> RootResultReply {
        RootResultReply {
            root_task: self.spec.task,
            profile: self.spec.contract.profile(),
            kind: self.spec.contract.kind(),
            accepted_consumed,
            outcome,
        }
    }
    fn try_handoff(state: &mut ChannelState) {
        if state.context_owned
            && state.producer_exited
            && state.lifetime.end().is_some()
            && state.lifetime.allows_read()
        {
            state
                .lifetime
                .handoff_to_context()
                .expect("exact root context takes completed producer");
        }
    }
}

/// One channel-local encoder handoff. The token retains only the channel;
/// it does not prove a whole CPU job or any other owner has exited. Ordinary
/// Drop releases its channel reference without publishing encoder completion.
pub struct RootProducerExit {
    channel: Arc<RootResultChannel>,
}
impl RootProducerExit {
    /// Publish encoder completion only after the owning caller has actually
    /// destroyed its input, cursor, builder and encoder scratch. Cancellation,
    /// End publication and dropping this token do not satisfy that obligation.
    /// Whole-job exit remains the responsibility of its independent owner.
    pub fn finish_encoder(self) {
        let mut state = self.channel.state.lock().unwrap();
        let exited_without_end = (state.lifetime.end().is_none()
            && state.lifetime.allows_read()
            && state.failed.is_none())
        .then(|| {
            novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                RootProducerFailure::ExitedWithoutEnd,
            )
        });
        let drive = self.channel.physical.arm_exit(|| {
            state.producer_exited = true;
            state.lifetime.encoder_exited();
            if let Some(failure) = exited_without_end {
                state.failed = Some(failure);
            }
            RootResultChannel::try_handoff(&mut state);
        });
        drop(state);
        if drive {
            self.channel.physical.drain_notifications();
        }
    }
}
/// Strong-only read ownership. The last handle frees its private Arc before
/// publishing physical position exit; no Weak/raw Arc can prolong that heap.
pub struct RootDeliveryOwner {
    position: Option<Arc<RootDeliveryPosition>>,
}
struct RootDeliveryPosition {
    physical: Arc<PhysicalOwners>,
}
impl RootDeliveryOwner {
    fn backing_metadata_bytes() -> usize {
        std::alloc::Layout::new::<[AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<RootDeliveryPosition>())
            .expect("fixed root delivery Arc layout fits usize")
            .0
            .pad_to_align()
            .size()
    }
}
impl Clone for RootDeliveryOwner {
    fn clone(&self) -> Self {
        Self {
            position: self.position.as_ref().map(Arc::clone),
        }
    }
}
impl Drop for RootDeliveryOwner {
    fn drop(&mut self) {
        if let Some(position) = self.position.take() {
            drop(Arc::into_inner(position));
        }
    }
}
impl Drop for RootDeliveryPosition {
    fn drop(&mut self) {
        self.physical.exit_and_wake(|| {
            self.physical.deliveries.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

pub struct RootChannelRead {
    channel: Arc<RootResultChannel>,
    request: RootResultRead,
    guard: RootDeliveryOwner,
}
impl RootChannelRead {
    /// Apply this original admission's ACK before reserving native copy space.
    /// This neither offers a payload nor creates another read holder. A repeat
    /// by `read()` is idempotent; seal winning this cut refuses the late ACK.
    pub fn apply_consumed(&self) -> Result<u64, RootChannelError> {
        self.channel.apply_consumed(self.request.consumed())
    }
    /// Snapshot the actual applied ACK frontier of this original admission.
    /// In particular, send pregrant losing to seal can return a closed marker
    /// without rerouting, applying the late ACK or minting another read owner.
    pub fn accepted_consumed(&self) -> u64 {
        self.channel.snapshot().consumed_through
    }
    /// The admitted read position is already counted under the context fence.
    /// Reserve all independent native payload backings before any prost/HTTP
    /// allocation, using this original root/process budget. A concurrent seal
    /// rejects growth; it does not invent a new read position or a new wallet.
    pub fn try_reserve_native_send_backing(
        &self,
    ) -> Result<ResultWriteAdmission, RootChannelError> {
        let geometry = NativeResultSupportGeometry::V1;
        let bytes = geometry
            .root_segment_backing_capacity_bytes
            .checked_mul(geometry.root_independent_payload_copies_per_send)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(RootChannelError::SendGeometry)?;
        self.channel.try_reserve(bytes)
    }
    /// Actual copy demand, without inventing a budget limit or live usage.
    /// The caller uses these facts only after this root refuses copy capacity.
    pub fn native_send_backing_refusal(&self) -> Result<RootCapacityRefusal, RootChannelError> {
        let geometry = NativeResultSupportGeometry::V1;
        let bytes = geometry
            .root_segment_backing_capacity_bytes
            .checked_mul(geometry.root_independent_payload_copies_per_send)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(RootChannelError::SendGeometry)?;
        Ok(RootCapacityRefusal::unavailable(bytes))
    }
    /// Suballocate finite native wrapper/Arc metadata from the original fixed
    /// root envelope before constructing them. The returned owner follows the
    /// actual wrappers and aliases, independently of handler completion.
    pub fn try_reserve_native_send_metadata(
        &self,
        bytes: usize,
    ) -> Result<RootMetadataReservation, RootChannelError> {
        self.channel.try_reserve_metadata(bytes)
    }
    pub async fn read(self) -> Result<RootResultDelivery, RootChannelError> {
        self.channel.read_admitted(&self.request, self.guard).await
    }
}
/// Native moves the owner into its response-body/H2 lifetime. Returning from
/// a handler or serializing a protobuf is not a physical send-exit receipt.
pub struct RootResultDelivery {
    reply: RootResultReply,
    guard: RootDeliveryOwner,
}
impl RootResultDelivery {
    pub fn reply(&self) -> &RootResultReply {
        &self.reply
    }
    pub fn into_parts(self) -> (RootResultReply, RootDeliveryOwner) {
        (self.reply, self.guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn notification_checkpoint(
        channel: &Arc<RootResultChannel>,
        point: RootNotificationTestPoint,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        let released = Mutex::new(released);
        let once = AtomicBool::new(true);
        assert!(
            channel
                .physical
                .test_checkpoint
                .set(Arc::new(move |seen| {
                    if seen == point && once.swap(false, Ordering::AcqRel) {
                        let _ = entered.send(());
                        let _ = released.lock().unwrap().recv();
                    }
                }))
                .is_ok()
        );
        (observed, resume)
    }

    #[test]
    fn last_metadata_exit_hands_off_before_a_zero_owner_snapshot() {
        let (channel, _) = channel(facts());
        let metadata = channel
            .try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>())
            .unwrap();
        channel.close(RootRetentionClose::ContextReleased);
        let (entered, resume) =
            notification_checkpoint(&channel, RootNotificationTestPoint::OwnerExitCommitted);
        let exit = std::thread::spawn(move || drop(metadata));
        let observed = entered.recv_timeout(Duration::from_secs(5));
        let owners = channel
            .physical
            .fixed_metadata_holders
            .load(Ordering::Acquire);
        let idle = channel.physical_idle();
        let joined = channel.try_join_physical();
        // Join real threads before asserting, even if the regression returned
        // a false idle observation. No test failure strands the barrier.
        let _ = resume.send(());
        let exited = exit.join();
        assert!(observed.is_ok());
        assert!(exited.is_ok());
        assert_eq!(owners, 0);
        assert!(!idle, "last owner has handed off real notification work");
        assert!(!joined);
        assert!(channel.try_join_physical());
    }

    #[test]
    fn pending_notification_pass_has_no_physical_idle_gap() {
        let (channel, _) = channel(facts());
        channel.close(RootRetentionClose::ContextReleased);
        let weak = Arc::downgrade(&channel.physical);
        let once = AtomicBool::new(true);
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        channel
            .writable_observable()
            .try_add_observer(Arc::new(move || {
                counted.fetch_add(1, Ordering::AcqRel);
                if once.swap(false, Ordering::AcqRel) {
                    weak.upgrade().unwrap().wake();
                }
            }))
            .unwrap();
        let (entered, resume) = notification_checkpoint(
            &channel,
            RootNotificationTestPoint::NotificationPassReturned,
        );
        let physical = channel.physical.clone();
        let notification = std::thread::spawn(move || physical.wake());
        let observed = entered.recv_timeout(Duration::from_secs(5));
        let idle = channel.physical_idle();
        let joined = channel.try_join_physical();
        let _ = resume.send(());
        let exited = notification.join();
        assert!(observed.is_ok());
        assert!(exited.is_ok());
        assert!(
            !idle,
            "a pending second pass keeps the original physical owner"
        );
        assert!(!joined);
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert!(channel.try_join_physical());
    }

    #[test]
    fn sealed_empty_batch_and_late_wakes_obey_the_join_cut() {
        let (channel, _) = channel(facts());
        channel.seal_reads(RootRetentionClose::ContextReleased);
        channel.seal_reads(RootRetentionClose::ContextReleased);
        assert!(
            !channel.physical_idle(),
            "the first empty seal still has real finish work"
        );
        assert!(!channel.try_join_physical());
        channel.finish_seal();
        assert!(channel.physical_idle());
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        channel
            .writable_observable()
            .try_add_observer(Arc::new(move || {
                counted.fetch_add(1, Ordering::AcqRel);
            }))
            .unwrap();
        // These public repeated seal calls must not manufacture another empty
        // retirement or call a foreign observer, even before the join cut.
        channel.finish_seal();
        channel.seal_reads(RootRetentionClose::ContextAborted);
        channel.finish_seal();
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert!(channel.try_join_physical());
        channel.physical.wake();
        assert!(channel.physical_idle());
        assert_eq!(
            calls.load(Ordering::Acquire),
            0,
            "joined notification admission cannot reopen"
        );
    }

    #[test]
    fn active_builder_tail_exits_before_the_last_segment_position() {
        let (channel, _) = channel(facts());
        let builder = channel.try_segment().unwrap().unwrap();
        let weak = Arc::downgrade(&channel);
        let late_active = Arc::new(AtomicBool::new(false));
        let observed = late_active.clone();
        channel
            .writable_observable()
            .try_add_observer(Arc::new(move || {
                let root = weak.upgrade().unwrap();
                if root.physical.segments.load(Ordering::Acquire) == 0 {
                    observed.fetch_or(root.state.lock().unwrap().active_builder, Ordering::AcqRel);
                }
            }))
            .unwrap();
        drop(builder);
        assert!(!late_active.load(Ordering::Acquire));
        assert_eq!(channel.physical.segments.load(Ordering::Acquire), 0);
    }

    #[test]
    fn closed_channel_is_a_stop_observation_not_an_originating_task_failure() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureEvidence, FailureProjection,
        };
        let error = RootChannelError::Closed;
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Control);
        assert_eq!(fact.evidence(), FailureEvidence::StopObservation);
        assert_eq!(fact.reason().code(), 46);
        assert_eq!(fact.resource(), None);
        assert_eq!(error.into_execution().failure_fact(), fact);
    }

    #[tokio::test]
    async fn ownership_snapshot_distinguishes_ack_from_last_alias_and_producer_exit() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        channel.request_finish().unwrap();
        publish(&channel, b"abc", true);
        let offered = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&offered).body().clone();
        let before = channel.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!(
            (
                before.channels,
                before.producers_running,
                before.producers_exited
            ),
            (1, 1, 0)
        );
        assert_eq!(
            (
                before.ends_published,
                before.ends_acknowledged,
                before.data_positions
            ),
            (1, 0, 1)
        );
        assert_eq!((before.segments, before.deliveries), (1, 1));
        drop(offered);
        producer.finish_encoder();
        // End has sequence 2; ACK-only retires logical Data and End together.
        let ack = channel.read(&read(&channel, None, 2)).await.unwrap();
        drop(ack);
        let acknowledged = channel.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!(
            (
                acknowledged.producers_running,
                acknowledged.producers_exited
            ),
            (0, 1)
        );
        assert_eq!(
            (
                acknowledged.ends_acknowledged,
                acknowledged.data_positions,
                acknowledged.payload_bytes
            ),
            (1, 0, 0)
        );
        // The original offered body alias also keeps its read/delivery guard.
        assert_eq!((acknowledged.segments, acknowledged.deliveries), (1, 1));
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(channel.try_ownership_snapshot().unwrap().unwrap().sealed, 1);
        assert!(!channel.physical_idle());
        drop(alias);
        let exited = channel.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!((exited.segments, exited.deliveries), (0, 0));
        assert!(channel.physical_idle());
    }

    #[test]
    fn ownership_snapshot_busy_poison_and_overflow_are_not_zero_usage() {
        let (channel, _) = channel(facts());
        let guard = channel.state.lock().unwrap();
        assert_eq!(channel.try_ownership_snapshot().unwrap(), None);
        drop(guard);
        let poison = Arc::clone(&channel);
        assert!(
            std::thread::spawn(move || {
                let _guard = poison.state.lock().unwrap();
                panic!("poison actual root channel");
            })
            .join()
            .is_err()
        );
        assert_eq!(
            channel.try_ownership_snapshot(),
            Err(RootOwnershipSnapshotError::ChannelPoisoned)
        );
        let mut saturated = RootOwnershipSnapshot {
            segments: usize::MAX,
            ..Default::default()
        };
        assert!(
            saturated
                .try_add(RootOwnershipSnapshot {
                    segments: 1,
                    ..Default::default()
                })
                .is_none()
        );
    }

    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn channel(output: FrozenRootOutput) -> (Arc<RootResultChannel>, Arc<ResultRetainedBudget>) {
        let limits =
            WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
            Arc::new(ExecutionHealth::default()),
        )
        .unwrap();
        (channel, budget)
    }
    #[test]
    fn healthy_writable_epochs_reuse_original_slots_without_retaining_metadata() {
        let (channel, budget) = channel(FrozenRootOutput::CountOnly);
        let layout = RootResultChannel::core_metadata_layout(channel.spec.task).unwrap();
        let initial = channel
            .try_ownership_snapshot()
            .unwrap()
            .unwrap()
            .metadata_bytes;
        assert_eq!(initial, layout.bytes);
        let actual_notification_owner =
            std::alloc::Layout::new::<RootArcAllocation<PhysicalOwners>>().size();
        let actual_channel_owner =
            std::alloc::Layout::new::<RootArcAllocation<RootResultChannel>>().size();
        let actual_queues = {
            let state = channel.state.lock().unwrap();
            state.stream.metadata_backing_bytes()
                + state.payloads.capacity() * std::mem::size_of::<RootResultData>()
        };
        assert!(initial >= actual_channel_owner + actual_notification_owner + actual_queues);
        assert!(
            initial
                <= NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes
                    as usize
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        channel
            .writable_observable()
            .try_add_observer(Arc::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        let identity = Arc::as_ptr(&channel.physical.writable);
        let generation = channel.physical.writable.generation();
        let charged = budget.retained_bytes_for_test();
        for epoch in 1..=2 {
            channel.physical.wake();
            assert_eq!(calls.load(Ordering::SeqCst), epoch);
            assert_eq!(
                channel
                    .physical
                    .fixed_metadata_holders
                    .load(Ordering::Acquire),
                0
            );
            assert!(
                !channel
                    .physical
                    .metadata_notification_held
                    .load(Ordering::Acquire)
            );
            assert_eq!(Arc::as_ptr(&channel.physical.writable), identity);
            assert_eq!(
                channel.physical.writable.generation(),
                generation + epoch as u64
            );
            let slot = channel.physical.notification.lock().unwrap();
            assert!(slot.self_pin.is_none() && !slot.failed);
            let work = slot.work.as_ref().unwrap();
            assert!(work.batch.is_empty());
            assert!(
                work.phases
                    .iter()
                    .all(|p| *p == RootMetadataPhase::NotStarted)
            );
            assert!(
                work.faults
                    .iter()
                    .all(|p| p.primary.is_none() && p.secondary.is_none())
            );
        }
        assert_eq!(budget.retained_bytes_for_test(), charged);
        assert_eq!(
            channel
                .try_ownership_snapshot()
                .unwrap()
                .unwrap()
                .metadata_bytes,
            initial
        );
        assert!(channel.physical.health.is_healthy());
        drop(channel);
        assert_eq!(budget.retained_bytes_for_test(), 0);
    }

    #[test]
    fn encoder_metadata_unwind_keeps_original_health_credit_and_safe_sibling_progress() {
        struct Gate {
            open: Mutex<bool>,
            changed: std::sync::Condvar,
        }
        impl Gate {
            fn release(&self) {
                *self.open.lock().unwrap() = true;
                self.changed.notify_all();
            }
        }
        struct Release(Arc<Gate>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        struct Secondary {
            _bytes: Vec<u8>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Payload {
            gate: Arc<Gate>,
            entered: std::sync::mpsc::SyncSender<()>,
            drops: Arc<AtomicUsize>,
            secondary_drops: Arc<AtomicUsize>,
            secondary: bool,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                self.entered.send(()).unwrap();
                let mut open = self.gate.open.lock().unwrap();
                while !*open {
                    open = self.gate.changed.wait(open).unwrap();
                }
                drop(open); // the intentional payload panic must not poison the test gate
                if self.secondary {
                    std::panic::panic_any(Secondary {
                        _bytes: vec![0x5a; 32],
                        drops: self.secondary_drops.clone(),
                    });
                }
            }
        }
        for secondary in [false, true] {
            let (channel, budget) = channel(FrozenRootOutput::CountOnly);
            let health = channel.physical.health.clone();
            channel.mark_context_owned().unwrap();
            let producer = channel.start_producer().unwrap();
            channel.request_finish().unwrap();
            channel.publish_end().unwrap();
            // Admit both encoder owners before the health fault. This actual
            // sibling shares the same runtime and exact backend identity.
            let parent = channel.spec.task;
            let sibling = RootResultChannel::try_open(
                RootResultWriteSpec {
                    task: TaskIdentity::new(
                        parent.query_execution_id(),
                        parent.stage_id(),
                        TaskId::new(2).unwrap(),
                        parent.backend_process_id(),
                    ),
                    contract: Arc::new(RootOutputContract::new(
                        RootProfileId::V1,
                        FrozenRootOutput::CountOnly,
                    )),
                },
                budget.clone(),
                channel.limits,
                health.clone(),
            )
            .unwrap();
            sibling.mark_context_owned().unwrap();
            let sibling_producer = sibling.start_producer().unwrap();
            let gate = Arc::new(Gate {
                open: Mutex::new(false),
                changed: std::sync::Condvar::new(),
            });
            let _release = Release(gate.clone());
            let drops = Arc::new(AtomicUsize::new(0));
            let secondary_drops = Arc::new(AtomicUsize::new(0));
            let (entered, receiver) = std::sync::mpsc::sync_channel(1);
            let once = AtomicBool::new(true);
            let callback_gate = gate.clone();
            let callback_drops = drops.clone();
            let callback_secondary_drops = secondary_drops.clone();
            channel
                .writable_observable()
                .try_add_observer(Arc::new(move || {
                    if once.swap(false, Ordering::AcqRel) {
                        std::panic::panic_any(Payload {
                            gate: callback_gate.clone(),
                            entered: entered.clone(),
                            drops: callback_drops.clone(),
                            secondary_drops: callback_secondary_drops.clone(),
                            secondary,
                        });
                    }
                }))
                .unwrap();
            let second_calls = Arc::new(AtomicUsize::new(0));
            let counted = second_calls.clone();
            let subscription = channel
                .writable_observable()
                .try_subscribe(Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            let callback_thread = std::thread::spawn(move || producer.finish_encoder());
            if let Err(error) = receiver.recv_timeout(Duration::from_secs(5)) {
                gate.release();
                callback_thread.join().unwrap();
                panic!("the actual payload Drop did not enter: {error}");
            }
            // Collect while the real destructor is blocked. Assert only after
            // releasing and joining it so a failed oracle strands no thread.
            let fault = health.failure();
            let encoder_state = channel.producer_state();
            let original_idle = channel.physical_idle();
            let charged = budget.retained_bytes_for_test();
            // The independent already accepted encoder remains safe while
            // the first owner's actual opaque destructor is blocked.
            sibling.request_finish().unwrap();
            sibling.publish_end().unwrap();
            sibling_producer.finish_encoder();
            let sibling_exited = sibling.physical_idle();
            let sibling_state = sibling.producer_state();
            gate.release();
            callback_thread.join().unwrap();
            let fault = fault.expect("health was published before foreign payload Drop");
            assert_eq!(
                fault.origin(),
                ExecutionExitOrigin::Task {
                    identity: channel.spec.task,
                    phase: novarocks_execution_contract::TaskFailurePhase::Execution,
                }
            );
            assert_eq!(fault.fact().reason().code(), 163);
            assert_eq!(encoder_state, RootProducerState::ContextHeld);
            assert!(!original_idle);
            assert!(charged > 0);
            assert!(sibling_exited);
            assert_eq!(sibling_state, RootProducerState::ContextHeld);
            assert_eq!(health.failure(), Some(fault));
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
            assert_eq!(second_calls.load(Ordering::SeqCst), 1);
            let held = channel.physical.notification.lock().unwrap();
            assert!(held.failed && held.self_pin.is_some());
            assert_eq!(
                held.work.as_ref().unwrap().faults[3].secondary.is_some(),
                secondary
            );
            drop(held);
            drop(subscription);
            assert!(!channel.physical_idle());
        }
    }

    #[test]
    fn readiness_fault_does_not_replace_the_exact_originating_channel_failure() {
        let (channel, _) = channel(FrozenRootOutput::CountOnly);
        let producer = channel.start_producer().unwrap();
        channel.note_rows(u64::MAX).unwrap();
        let RootChannelError::Execution(original) = channel.note_rows(1).unwrap_err() else {
            panic!("real row-count author retains its original failure");
        };
        channel
            .writable_observable()
            .try_add_observer(Arc::new(|| {
                panic!("metadata fault after the original row-count failure");
            }))
            .unwrap();
        producer.finish_encoder();
        let RootProducerState::Failed(observed) = channel.producer_state() else {
            panic!("metadata cannot replace the domain failure");
        };
        assert!(std::ptr::eq(original.cause(), observed.cause()));
        assert_eq!(
            channel
                .physical
                .health
                .failure()
                .unwrap()
                .fact()
                .reason()
                .code(),
            163
        );
        assert!(channel.state.lock().unwrap().producer_exited);
        assert!(!channel.physical_idle());
    }

    fn facts() -> FrozenRootOutput {
        FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1)
    }
    fn read(channel: &RootResultChannel, wanted: Option<u64>, consumed: u64) -> RootResultRead {
        RootResultRead::try_new(
            channel.spec.task,
            channel.spec.contract.profile(),
            channel.spec.contract.kind(),
            wanted.map(|n| NonZeroU64::new(n).unwrap()),
            consumed,
            Duration::from_millis(1),
        )
        .unwrap()
    }
    fn publish(channel: &Arc<RootResultChannel>, body: &[u8], end: bool) {
        let mut builder = channel.try_segment().unwrap().unwrap();
        builder.output()[..body.len()].copy_from_slice(body);
        channel.publish_segment(builder, body.len(), end).unwrap();
    }
    fn data(delivery: &RootResultDelivery) -> &RootResultData {
        let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
            panic!("expected Data");
        };
        data
    }
    #[test]
    fn allocation_failure_releases_owners_without_a_callback_under_channel_lock() {
        let (channel, budget) = channel(facts());
        let before = budget.retained_bytes_for_test();
        let observed = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&observed);
        let weak = Arc::downgrade(&channel);
        let _registration = channel.writable_observable().subscribe(Arc::new(move || {
            let channel = weak.upgrade().unwrap();
            let _ = channel.snapshot();
            let _ = channel.producer_state();
            counted.fetch_add(1, Ordering::SeqCst);
        }));
        let cause = channel
            .try_segment_allocating(|backing, _| {
                backing.try_reserve_exact(usize::MAX).map_err(|source| {
                    RootChannelError::reserve_backing(
                        "allocation failure fixture",
                        novarocks_execution_contract::failure::ResourceDimension::Bytes,
                        usize::MAX,
                        source,
                    )
                })
            })
            .err()
            .expect("real SDK backing request must refuse");
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        let fact = cause.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 38);
        assert_eq!(
            fact.resource().unwrap().requested,
            u64::try_from(usize::MAX).ok()
        );
        assert_eq!(fact.representation(), None);
        let mut source = std::error::Error::source(&cause);
        let mut saw_sdk = false;
        while let Some(error) = source {
            saw_sdk |= error
                .downcast_ref::<std::collections::TryReserveError>()
                .is_some();
            source = error.source();
        }
        assert!(saw_sdk, "actual reserve refusal remains the local source");
        assert!(observed.load(Ordering::SeqCst) > 0);
        assert_eq!(budget.retained_bytes_for_test(), before);
        assert!(channel.physical_idle());
        let builder = channel
            .try_segment()
            .unwrap()
            .expect("failed allocation returned its position");
        drop(builder);
        assert!(channel.physical_idle());
    }
    #[test]
    fn schema_spare_and_core_metadata_share_the_same_fixed_grant() {
        use novarocks_result_contract::{
            ClientRenderSchema, NativeRenderType, RenderColumn, RenderField, RenderPresentation,
        };
        let root = task();
        let core = RootResultChannel::core_metadata_layout(root).unwrap().bytes;
        let fixed =
            NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        let name_capacity = fixed.checked_sub(core).unwrap().checked_add(1).unwrap();
        let mut name = String::with_capacity(name_capacity);
        name.push('x');
        let schema = ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name,
                field: RenderField {
                    native_type: NativeRenderType::String,
                    presentation: RenderPresentation::ScalarText,
                    nullable: true,
                },
            }],
            1,
        )
        .expect("wire is tiny and schema's separate limit is valid");
        assert!(schema.backing_bytes() >= name_capacity);
        let required = schema.backing_bytes().checked_add(core).unwrap();
        assert!(required > fixed);
        let limits =
            WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let failure = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: root,
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::ClientRows(schema),
                )),
            },
            Arc::clone(&budget),
            limits,
            Arc::new(ExecutionHealth::default()),
        )
        .err()
        .expect("schema and real core backing exceed their shared grant");
        let expected = RootCapacityRefusal::bytes(Some(required), Some(fixed), None);
        assert_eq!(failure, RootChannelError::Capacity(expected));
        assert_eq!(
            budget.retained_bytes_for_test(),
            0,
            "combined bound is checked before allocating the channel"
        );
    }
    #[test]
    fn host_metadata_suballocations_are_bounded_and_hold_until_actual_drop() {
        let (channel, budget) = channel(facts());
        let fixed =
            NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        let initial = channel
            .try_ownership_snapshot()
            .unwrap()
            .unwrap()
            .metadata_bytes;
        let available = fixed
            .checked_sub(initial)
            .expect("admitted core fits its fixed root grant");
        let first_bytes = available / 2;
        let first = channel.try_reserve_metadata(first_bytes).unwrap();
        let second = channel
            .try_reserve_metadata(available - first_bytes)
            .unwrap();
        assert_eq!(
            channel
                .try_ownership_snapshot()
                .unwrap()
                .unwrap()
                .metadata_bytes,
            fixed
        );
        assert!(matches!(
            channel.try_reserve_metadata(32),
            Err(RootChannelError::Capacity(_))
        ));
        assert_eq!(
            budget.retained_bytes_for_test(),
            1024 * 1024,
            "suballocation does not create another wallet or charge"
        );
        channel.close(RootRetentionClose::ContextReleased);
        assert!(!channel.physical_idle());
        drop(first);
        assert!(!channel.physical_idle());
        drop(second);
        assert!(channel.physical_idle());
        assert!(matches!(
            channel.try_reserve_metadata(32),
            Err(RootChannelError::Closed)
        ));
    }
    #[test]
    fn tiny_metadata_claims_cannot_create_unbounded_control_owners() {
        let (channel, _) = channel(facts());
        let geometry = NativeResultSupportGeometry::V1;
        let positions =
            (geometry.root_maximum_root_drivers + geometry.root_live_send_holders + 1) as usize;
        let mut claims = Vec::with_capacity(positions);
        for _ in 0..positions {
            claims.push(
                channel
                    .try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>())
                    .unwrap(),
            );
        }
        let cause = channel
            .try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>())
            .err()
            .expect("all fixed metadata positions are held");
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, ResourceDimension,
        };
        let fact = cause.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Positions);
        assert_eq!(resource.requested, Some(1));
        assert_eq!(resource.limit, Some(positions as u64));
        assert_eq!(resource.committed, Some(positions as u64));
        assert_eq!(resource.live, None);
        let malformed = channel
            .try_reserve_metadata(1)
            .err()
            .expect("the claim cannot hold its metadata owner");
        assert!(matches!(
            malformed,
            RootChannelError::MetadataTooSmall { requested: 1, .. }
        ));
        assert_eq!(malformed.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(malformed.failure_fact().reason().code(), 39);
        drop(claims);
        assert!(channel.physical_idle());
        drop(
            channel
                .try_reserve_metadata(32)
                .expect("physical exit returns metadata position"),
        );
    }
    #[test]
    fn blocked_raw_admission_does_not_emit_a_false_capacity_transition() {
        let (channel, _) = channel(facts());
        let ResultWriteAdmission::Granted(credit) = channel
            .try_reserve(256 * 1024 * 1024 - 1024 * 1024)
            .unwrap()
        else {
            panic!("complete remaining retained capacity");
        };
        let observable = channel.writable_observable();
        let generation = observable.generation();
        for _ in 0..128 {
            assert!(matches!(
                channel.try_reserve(1).unwrap(),
                ResultWriteAdmission::Blocked
            ));
        }
        assert_eq!(
            observable.generation(),
            generation,
            "failed tentative positions cannot cause a retry spin"
        );
        assert_eq!(
            channel
                .physical
                .retained_reservations
                .load(Ordering::Acquire),
            1
        );
        drop(credit);
        assert_ne!(
            observable.generation(),
            generation,
            "actual capacity return still wakes waiters"
        );
    }
    #[tokio::test]
    async fn immutable_replay_atomic_end_and_finish_ignore_consumer_ack() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        channel.note_rows(7).unwrap();
        channel.request_finish().unwrap();
        publish(&channel, b"abc", true);
        assert_eq!(channel.producer_state(), RootProducerState::Finishing);
        producer.finish_encoder();
        assert_eq!(channel.producer_state(), RootProducerState::ContextHeld);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let replay = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert_eq!(first.reply(), replay.reply());
        assert_eq!(data(&first).sequence().get(), 1);
        assert_eq!(data(&first).end_after_data().unwrap().sequence.get(), 2);
        assert_eq!(data(&first).end_after_data().unwrap().output_rows, 7);
        // Closing is legal without any final ACK and cannot erase RootFinished.
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(channel.snapshot().consumed_through, 0);
        assert!(!channel.physical_idle());
        drop(first);
        drop(replay);
        assert!(channel.physical_idle());
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        drop(channel);
        assert_eq!(budget.retained_bytes_for_test(), 0);
    }
    #[tokio::test]
    async fn applied_ack_survives_not_ready_and_lost_reply_with_backing_held() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&first).body().clone();
        drop(first);
        let held = budget.retained_bytes_for_test();
        let lost = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        assert_eq!(lost.reply().accepted_consumed, 1);
        assert_eq!(lost.reply().outcome, RootReadOutcome::NotReady);
        assert_eq!(channel.snapshot().data_positions, 0);
        assert_eq!(
            budget.retained_bytes_for_test(),
            held,
            "ACK is not physical release"
        );
        drop(lost);
        let retry = channel.read(&read(&channel, Some(1), 1)).await.unwrap();
        assert_eq!(retry.reply().outcome, RootReadOutcome::Retired);
        assert_eq!(retry.reply().accepted_consumed, 1);
        drop(retry);
        drop(alias);
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
    }

    fn channel_with_native_send_budget() -> (Arc<RootResultChannel>, Arc<ResultRetainedBudget>) {
        let geometry = NativeResultSupportGeometry::V1;
        let bytes = (geometry.root_fixed_schema_cursor_driver_capacity_bytes
            + geometry.root_independent_payload_copies_per_send
                * geometry.root_segment_backing_capacity_bytes) as usize;
        let limits = WorkerResultRetainedLimits::try_new(bytes, bytes).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, facts())),
            },
            Arc::clone(&budget),
            limits,
            Arc::new(ExecutionHealth::default()),
        )
        .unwrap();
        channel.mark_context_owned().unwrap();
        (channel, budget)
    }

    #[tokio::test]
    async fn native_fetch_ack_retires_backing_before_send_copy_pregrant() {
        let (channel, budget) = channel_with_native_send_budget();
        publish(&channel, b"a", false);
        drop(channel.read(&read(&channel, Some(1), 0)).await.unwrap());
        let admitted = channel.begin_read(&read(&channel, Some(2), 1)).unwrap();
        assert!(matches!(
            admitted.try_reserve_native_send_backing().unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert_eq!(channel.snapshot().consumed_through, 0);
        assert_eq!(admitted.apply_consumed().unwrap(), 1);
        assert_eq!(
            admitted.apply_consumed().unwrap(),
            1,
            "ACK replay is idempotent"
        );
        assert_eq!(channel.snapshot().data_positions, 0);
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        let ResultWriteAdmission::Granted(send) =
            admitted.try_reserve_native_send_backing().unwrap()
        else {
            panic!("actual ACK retirement freed enough space for the same admitted read");
        };
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 1);
        let reply = admitted.read().await.unwrap();
        assert_eq!(reply.reply().accepted_consumed, 1);
        assert_eq!(reply.reply().outcome, RootReadOutcome::NotReady);
        drop(reply);
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024 + send.bytes());
        drop(send);
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        assert!(channel.physical_idle());
    }

    #[tokio::test]
    async fn native_pregrant_ack_cannot_return_a_live_segment_alias_budget() {
        let (channel, budget) = channel_with_native_send_budget();
        publish(&channel, b"a", false);
        let offered = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&offered).body().clone();
        drop(offered);
        let retained = budget.retained_bytes_for_test();
        let admitted = channel.begin_read(&read(&channel, Some(2), 1)).unwrap();
        assert_eq!(admitted.apply_consumed().unwrap(), 1);
        assert_eq!(channel.snapshot().data_positions, 0);
        assert_eq!(budget.retained_bytes_for_test(), retained);
        assert!(matches!(
            admitted.try_reserve_native_send_backing().unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(alias);
        let ResultWriteAdmission::Granted(send) =
            admitted.try_reserve_native_send_backing().unwrap()
        else {
            panic!("last actual alias exit returned the capacity");
        };
        assert_eq!(admitted.accepted_consumed(), 1);
        drop(send);
        drop(admitted);
        assert!(channel.physical_idle());
    }

    #[tokio::test]
    async fn seal_before_native_ack_pregrant_preserves_the_applied_frontier() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        drop(channel.read(&read(&channel, Some(1), 0)).await.unwrap());
        let admitted = channel.begin_read(&read(&channel, None, 1)).unwrap();
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(admitted.apply_consumed(), Err(RootChannelError::Closed));
        assert_eq!(admitted.accepted_consumed(), 0);
        assert!(matches!(
            admitted.try_reserve_native_send_backing(),
            Err(RootChannelError::Closed)
        ));
        let reply = admitted.read().await.unwrap();
        assert_eq!(reply.reply().accepted_consumed, 0);
        assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        drop(reply);
        assert!(channel.physical_idle());
    }

    #[test]
    fn native_ack_pregrant_refuses_unoffered_data_without_retiring_it() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        let retained = budget.retained_bytes_for_test();
        let admitted = channel.begin_read(&read(&channel, None, 1)).unwrap();
        assert_eq!(
            admitted.apply_consumed(),
            Err(RootChannelError::Stream(
                StreamError::AcknowledgementBeyondOffered
            ))
        );
        assert_eq!(channel.snapshot().consumed_through, 0);
        assert_eq!(channel.snapshot().data_positions, 1);
        assert_eq!(budget.retained_bytes_for_test(), retained);
    }
    #[tokio::test]
    async fn aliases_hold_delivery_positions_after_handler_and_ack() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&first).body().clone();
        drop(first);
        drop(channel.read(&read(&channel, None, 1)).await.unwrap());
        publish(&channel, b"b", false);
        let second = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        let other_alias = data(&second).body().clone();
        drop(second);
        assert!(matches!(
            channel.read(&read(&channel, None, 2)).await,
            Err(RootChannelError::Capacity(_))
        ));
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 2);
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(channel.physical.segments.load(Ordering::Acquire), 2);
        assert!(!channel.physical_idle());
        drop(alias);
        drop(other_alias);
        assert!(channel.physical_idle());
    }
    #[tokio::test]
    async fn full_window_has_reserved_standalone_end_and_rejects_bad_ack_gap() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        publish(&channel, b"b", false);
        assert!(channel.try_segment().unwrap().is_none());
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        assert_eq!(channel.snapshot().end_sequence, Some(3));
        assert!(matches!(
            channel.read(&read(&channel, Some(2), 0)).await,
            Err(RootChannelError::Stream(StreamError::OfferGap))
        ));
        assert!(matches!(
            channel.read(&read(&channel, None, 1)).await,
            Err(RootChannelError::Stream(
                StreamError::AcknowledgementBeyondOffered
            ))
        ));
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert!(data(&first).end_after_data().is_none());
        drop(first);
        let second = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        assert!(data(&second).end_after_data().is_none());
        drop(second);
        let end = channel.read(&read(&channel, Some(3), 2)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd { output_rows: 0, .. })
        ));
    }
    #[tokio::test]
    async fn context_seal_wakes_long_poll_and_waits_actual_guard_exit() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        let request = RootResultRead::try_new(
            channel.spec.task,
            channel.spec.contract.profile(),
            channel.spec.contract.kind(),
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1000),
        )
        .unwrap();
        let reader = Arc::clone(&channel);
        let job = tokio::spawn(async move { reader.read(&request).await.unwrap() });
        tokio::task::yield_now().await;
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 1);
        channel.close(RootRetentionClose::LeaseExpired);
        let delivery = tokio::time::timeout(Duration::from_millis(100), job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            delivery.reply().outcome,
            RootReadOutcome::AwaitTerminalControl
        );
        assert!(!channel.physical_idle());
        drop(delivery);
        assert!(channel.physical_idle());
    }
    #[tokio::test]
    async fn provisional_channel_and_mismatched_kind_are_never_routes() {
        let (channel, _) = channel(facts());
        assert!(matches!(
            channel.read(&read(&channel, Some(1), 0)).await,
            Err(RootChannelError::Identity)
        ));
        channel.mark_context_owned().unwrap();
        let mismatch = RootResultRead::try_new(
            channel.spec.task,
            RootProfileId::V1,
            RootOutputKind::CountOnly,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1),
        )
        .unwrap();
        assert!(matches!(
            channel.read(&mismatch).await,
            Err(RootChannelError::Identity)
        ));
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn count_zero_overflow_and_actual_producer_exit() {
        let (channel, _) = channel(FrozenRootOutput::CountOnly);
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        assert!(matches!(
            channel.try_segment(),
            Err(RootChannelError::Payload)
        ));
        channel.note_rows(u64::MAX).unwrap();
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        assert_eq!(channel.producer_state(), RootProducerState::Finishing);
        producer.finish_encoder();
        let end = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd {
                output_rows: u64::MAX,
                ..
            })
        ));
        drop(end);
        let (empty, _) = super::tests::channel(FrozenRootOutput::CountOnly);
        empty.mark_context_owned().unwrap();
        let producer = empty.start_producer().unwrap();
        empty.request_finish().unwrap();
        empty.publish_end().unwrap();
        producer.finish_encoder();
        assert_eq!(empty.producer_state(), RootProducerState::ContextHeld);
        let end = empty.read(&read(&empty, Some(1), 0)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd { output_rows: 0, .. })
        ));
        let (overflow, _) = super::tests::channel(FrozenRootOutput::CountOnly);
        overflow.note_rows(u64::MAX).unwrap();
        assert!(
            matches!(overflow.note_rows(1), Err(RootChannelError::Execution(ref cause)) if novarocks_execution_contract::failure::FailureProjection::failure_fact(cause).reason().code() == 10)
        );
        overflow.request_finish().unwrap();
        assert_eq!(overflow.publish_end(), Err(RootChannelError::Producer));
        assert!(matches!(
            overflow.producer_state(),
            RootProducerState::Failed(_)
        ));
    }
    #[test]
    fn unused_builder_returns_capacity_only_after_actual_backing_drop() {
        let (channel, budget) = channel(facts());
        let initial = budget.retained_bytes_for_test();
        let builder = channel.try_segment().unwrap().unwrap();
        assert!(builder.capacity_bytes() >= RootProfileV1::SEGMENT_BYTES);
        assert!(channel.try_segment().unwrap().is_none());
        assert!(budget.retained_bytes_for_test() > initial);
        drop(builder);
        assert_eq!(budget.retained_bytes_for_test(), initial);
        assert_eq!(channel.physical.segments.load(Ordering::Acquire), 0);
        assert!(channel.try_segment().unwrap().is_some());
    }
    #[tokio::test]
    async fn encoder_finish_without_end_records_failure_and_cannot_publish_success() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        assert!(!channel.physical_idle());
        producer.finish_encoder();
        assert!(matches!(
            channel.producer_state(),
            RootProducerState::Failed(_)
        ));
        let reply = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        drop(reply);
        assert!(channel.physical_idle());
    }
    #[test]
    fn all_channel_and_segment_capacity_uses_original_shared_pool() {
        let limits = WorkerResultRetainedLimits::try_new(2 * 1024 * 1024, 2 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(2 * 1024 * 1024).unwrap());
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, facts())),
            },
            Arc::clone(&budget),
            limits,
            Arc::new(ExecutionHealth::default()),
        )
        .unwrap();
        assert!(
            channel.try_segment().unwrap().is_none(),
            "segment envelope is part of retained capacity"
        );
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        let ResultWriteAdmission::Granted(credit) = channel.try_reserve(1024 * 1024).unwrap()
        else {
            panic!("remaining shared capacity");
        };
        assert_eq!(budget.retained_bytes_for_test(), 2 * 1024 * 1024);
        assert!(matches!(
            channel.try_reserve(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(credit);
        drop(channel);
        assert_eq!(budget.retained_bytes_for_test(), 0);
    }

    #[tokio::test]
    async fn a_builder_admitted_before_failure_cannot_publish_data_or_end() {
        for with_end in [false, true] {
            let (channel, _) = channel(facts());
            channel.mark_context_owned().unwrap();
            let producer = channel.start_producer().unwrap();
            let mut builder = channel.try_segment().unwrap().unwrap();
            builder.output()[0] = b'x';
            channel.note_rows(u64::MAX).unwrap();
            assert!(
                matches!(channel.note_rows(1), Err(RootChannelError::Execution(ref cause)) if novarocks_execution_contract::failure::FailureProjection::failure_fact(cause).reason().code() == 10)
            );
            channel.request_finish().unwrap();
            assert_eq!(
                channel.publish_segment(builder, 1, with_end),
                Err(RootChannelError::Producer)
            );
            assert!(channel.snapshot().end_sequence.is_none());
            producer.finish_encoder();
            assert!(!channel.state.lock().unwrap().lifetime.root_finished());
            let reply = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
            assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        }
    }

    #[test]
    fn raw_retained_credit_and_its_control_position_follow_actual_drop() {
        let (channel, budget) = channel(facts());
        let ResultWriteAdmission::Granted(mut credit) = channel.try_reserve(4096).unwrap() else {
            panic!("admitted credit");
        };
        channel.close(RootRetentionClose::ContextReleased);
        assert!(!channel.physical_idle());
        assert_eq!(
            channel
                .physical
                .retained_reservations
                .load(Ordering::Acquire),
            1
        );
        credit.shrink_to(0).unwrap();
        assert!(
            !channel.physical_idle(),
            "credit control owner is still live even after shrinking its bytes"
        );
        drop(credit);
        assert!(channel.physical_idle());
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
    }

    #[tokio::test]
    async fn channel_metadata_and_wallet_follow_last_response_body_alias() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"x", false);
        let delivery = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&delivery).body().clone();
        drop(delivery);
        channel.close(RootRetentionClose::ContextReleased);
        drop(channel);
        let expected = 1024 * 1024
            + NativeResultSupportGeometry::V1.root_segment_backing_capacity_bytes as usize;
        assert_eq!(
            budget.retained_bytes_for_test(),
            expected,
            "metadata cannot be returned while response aliases hold it"
        );
        let weak_budget = Arc::downgrade(&budget);
        drop(budget);
        assert!(weak_budget.upgrade().is_some());
        drop(alias);
        assert!(weak_budget.upgrade().is_none());
    }

    #[test]
    fn even_tiny_credits_have_finite_physical_object_positions() {
        let (channel, _) = channel(facts());
        let mut credits = Vec::new();
        for _ in 0..6 {
            let ResultWriteAdmission::Granted(credit) = channel.try_reserve(1).unwrap() else {
                panic!("finite credit position");
            };
            credits.push(credit);
        }
        assert!(matches!(
            channel.try_reserve(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert!(!channel.physical_idle());
        drop(credits);
        assert!(channel.physical_idle());
    }
    #[test]
    fn producer_failure_retains_exact_count_cause_across_the_actual_exit() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, RepresentationBoundary, domain,
        };
        let (root, _) = channel(FrozenRootOutput::CountOnly);
        let producer = root.start_producer().unwrap();
        root.note_rows(u64::MAX).unwrap();
        assert!(
            matches!(root.note_rows(1), Err(RootChannelError::Execution(ref cause)) if novarocks_execution_contract::failure::FailureProjection::failure_fact(cause).reason().code() == 10)
        );
        let RootProducerState::Failed(original) = root.producer_state() else {
            panic!("actual counter refusal is visible before producer exit")
        };
        let fact = original.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(fact.reason().domain(), domain::WORKER.get());
        assert_eq!(fact.reason().code(), 10);
        let extent = fact.representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.extent_limit, Some(u64::MAX));
        assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(extent.representable_prefix, None);
        assert!(fact.resource().is_none());
        assert!(!root.state.lock().unwrap().producer_exited);
        producer.finish_encoder();
        assert!(root.state.lock().unwrap().producer_exited);
        assert_eq!(root.producer_state(), RootProducerState::Failed(original));
        let (root, _) = channel(FrozenRootOutput::CountOnly);
        root.start_producer().unwrap().finish_encoder();
        let RootProducerState::Failed(error) = root.producer_state() else {
            panic!("exit alone cannot publish success")
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 11);
    }

    #[test]
    fn ordinary_producer_token_drop_never_proves_encoder_exit_or_context_handoff() {
        for with_end in [false, true] {
            let (root, _) = channel(FrozenRootOutput::CountOnly);
            root.mark_context_owned().unwrap();
            let references = Arc::strong_count(&root);
            let producer = root.start_producer().unwrap();
            assert_eq!(Arc::strong_count(&root), references + 1);
            if with_end {
                root.request_finish().unwrap();
                root.publish_end().unwrap();
            }
            drop(producer);
            // Dropping this token really releases its one Arc. It establishes
            // no encoder-exit fact, even with an already published End.
            assert_eq!(Arc::strong_count(&root), references);
            let state = root.state.lock().unwrap();
            assert!(!state.producer_exited);
            assert!(!state.lifetime.root_finished());
            assert!(state.failed.is_none(), "token loss is not a producer exit");
            assert_eq!(
                state.lifetime.validate_task_retirement(),
                Err(novarocks_execution_contract::root_lifetime::RootLifetimeError::ContextNotHolding)
            );
            drop(state);
            let snapshot = root.try_ownership_snapshot().unwrap().unwrap();
            assert_eq!(
                (snapshot.producers_running, snapshot.producers_exited),
                (1, 0)
            );
            assert_ne!(root.producer_state(), RootProducerState::ContextHeld);
            assert!(!root.physical_idle());
        }
    }

    #[test]
    fn explicit_encoder_finish_follows_actual_builder_drop_and_allows_exact_context_handoff() {
        for context_first in [false, true] {
            let (root, budget) = channel(facts());
            if context_first {
                root.mark_context_owned().unwrap();
            }
            let producer = root.start_producer().unwrap();
            let builder = root.try_segment().unwrap().unwrap();
            assert_eq!(root.physical.segments.load(Ordering::Acquire), 1);
            let with_builder = budget.retained_bytes_for_test();
            root.request_finish().unwrap();
            assert_eq!(root.producer_state(), RootProducerState::Finishing);
            assert!(!root.state.lock().unwrap().producer_exited);
            // This is a real original builder/backing exit, not an intent,
            // status flag, ACK or deadline standing in for destruction.
            drop(builder);
            assert_eq!(root.physical.segments.load(Ordering::Acquire), 0);
            assert!(budget.retained_bytes_for_test() < with_builder);
            // The channel author requires all builders to retire before an
            // empty End; that fact alone still cannot hand off the encoder.
            root.publish_end().unwrap();
            assert!(!root.state.lock().unwrap().producer_exited);
            producer.finish_encoder();
            assert!(root.state.lock().unwrap().producer_exited);
            if !context_first {
                assert_eq!(root.producer_state(), RootProducerState::Finishing);
                root.mark_context_owned().unwrap();
            }
            assert_eq!(root.producer_state(), RootProducerState::ContextHeld);
            assert!(root.state.lock().unwrap().lifetime.root_finished());
            assert!(
                root.state
                    .lock()
                    .unwrap()
                    .lifetime
                    .validate_task_retirement()
                    .is_ok()
            );
            let snapshot = root.try_ownership_snapshot().unwrap().unwrap();
            assert_eq!(
                (snapshot.producers_running, snapshot.producers_exited),
                (0, 1)
            );
            root.close(RootRetentionClose::ContextReleased);
            assert!(root.physical_idle());
            drop(root);
            assert_eq!(budget.retained_bytes_for_test(), 0);
        }
    }

    #[test]
    fn context_cancellation_does_not_replace_explicit_encoder_exit() {
        let (root, _) = channel(FrozenRootOutput::CountOnly);
        root.mark_context_owned().unwrap();
        let producer = root.start_producer().unwrap();
        root.request_finish().unwrap();
        root.publish_end().unwrap();
        root.close(RootRetentionClose::ContextAborted);
        assert!(root.is_closed());
        assert!(!root.state.lock().unwrap().producer_exited);
        assert!(!root.state.lock().unwrap().lifetime.root_finished());
        assert_ne!(root.producer_state(), RootProducerState::ContextHeld);
        assert!(!root.physical_idle());
        let snapshot = root.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!(
            (snapshot.producers_running, snapshot.producers_exited),
            (1, 0)
        );
        // A cancelled encoder still must actually relinquish its inputs and
        // then explicitly finish. Closed retention never grants ContextHeld.
        producer.finish_encoder();
        assert!(root.state.lock().unwrap().producer_exited);
        assert!(!root.state.lock().unwrap().lifetime.root_finished());
        assert_ne!(root.producer_state(), RootProducerState::ContextHeld);
        let snapshot = root.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!(
            (snapshot.producers_running, snapshot.producers_exited),
            (0, 1)
        );
        assert!(root.physical_idle());
    }
}
