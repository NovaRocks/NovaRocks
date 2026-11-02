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

//! An original Root read R. Funding precedes channel admission; its Task cut
//! is created only after an actual EOF and that position's real ACK.

use super::{
    ContextHostRecord,
    requests::{ContextRequestAdmissionRefusal, TaskContextRequestHandle},
};
use crate::{
    control_runtime::{
        request_endpoint::RequestNotice, request_ready::ControlRequestReady,
        task::TaskRootReadError,
    },
    root_result_channel::{
        RootChannelError, RootChannelRead, RootEndAcknowledged, RootResultDelivery,
    },
    task_lifecycle::{
        TaskRootDrainCapability, TaskRootDrainFuture, TaskRootDrainOutcome, TaskRootDrainPermit,
    },
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole},
    lease::WorkRetirementObserver,
};
use novarocks_control_runtime_core::identity::{ActorId, ScopedWorkRef};
use novarocks_execution_contract::{
    TaskIdentity, TaskOperationId, TaskStatusVersion,
    root_result::{RootResultRead, RootResultReply},
};
use novarocks_worker_protocol::{
    context::{ContextReplyBody, RequestResult, RootDrainVerdict},
    lease::MonotonicInstant,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::ManuallyDrop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug)]
pub(crate) enum RootReadError {
    ForeignProcess,
    UnknownContext,
    ContextRemoved,
    Request(ContextRequestAdmissionRefusal),
    Task(TaskRootReadError),
    Channel(RootChannelError),
    Closed { accepted_consumed: u64 },
    UnknownTask,
    Capacity(novarocks_execution_contract::failure::ResourceFacts),
    WrongOwner,
}
fn root_position_capacity(
    limit: usize,
    committed: usize,
    scope: novarocks_execution_contract::failure::ResourceScope,
) -> RootReadError {
    use novarocks_execution_contract::failure::{ResourceDimension, ResourceFacts, ResourceScope};
    RootReadError::Capacity(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: Some(scope),
        dimension: ResourceDimension::Positions,
        requested: Some(1),
        required_growth: None,
        limit: u64::try_from(limit).ok(),
        committed: u64::try_from(committed).ok(),
        live: None,
    })
}
pub(super) struct DeferredRootRequest {
    pub(super) task: TaskIdentity,
    pub(super) funding: Option<super::requests::TaskRequestFunding>,
    pub(super) unused: Option<super::requests::ContextRequestPartial>,
    pub(super) unused_sources:
        Option<crate::control_runtime::source_slot_issuer::SourceSlotIssuePartial>,
    pub(super) data: Option<RootReadData>,
    pub(super) data_partial: Option<super::requests::ContextRequestPartial>,
    pub(super) shared: Arc<RootReadShared>,
    pub(super) retained: bool,
    pub(super) cut_waiting: bool,
    pub(super) binding_refusal: Option<super::requests::TaskRequestBindPartial>,
    pub(super) direct_outer: Option<novarocks_control_runtime::lease::ReservationBundle>,
}
struct RootSlot {
    // Installation transfers this original channel; task_retired separately
    // proves that its Task causal prefix may be bypassed for EOF replay.
    // An unexpected metadata Drop must not run its foreign/base-credit tail.
    retained: Option<ManuallyDrop<crate::control_runtime::task::RootContextRetention>>,
    // Installed ownership is not proof that the Task causal prefix retired.
    task_retired: bool,
    readers: BTreeSet<ScopedWorkRef>,
    cut_waiters: BTreeSet<ScopedWorkRef>,
    cut_after: Option<ScopedWorkRef>,
    cut_until: Option<ScopedWorkRef>,
    cut_notified_again: bool,
}
pub(in crate::control_runtime) struct RootReadState {
    max_roots: usize,
    per_root: usize,
    roots: BTreeMap<TaskIdentity, RootSlot>,
    cut_ready: novarocks_control_runtime_core::fair_queue::FairWorkQueue<TaskIdentity, ()>,
}
impl RootReadState {
    pub(in crate::control_runtime) fn new(max_roots: usize, per_root: usize) -> Self {
        Self {
            max_roots,
            per_root,
            roots: BTreeMap::new(),
            cut_ready: novarocks_control_runtime_core::fair_queue::FairWorkQueue::new(max_roots, 1),
        }
    }
    pub(super) fn reserve(
        &mut self,
        task: TaskIdentity,
        work: ScopedWorkRef,
    ) -> Result<(), RootReadError> {
        if !self.roots.contains_key(&task) {
            if self.roots.len() >= self.max_roots {
                return Err(root_position_capacity(
                    self.max_roots,
                    self.roots.len(),
                    novarocks_execution_contract::failure::ResourceScope::Query,
                ));
            }
            self.roots.insert(
                task,
                RootSlot {
                    retained: None,
                    task_retired: false,
                    readers: BTreeSet::new(),
                    cut_waiters: BTreeSet::new(),
                    cut_after: None,
                    cut_until: None,
                    cut_notified_again: false,
                },
            );
        }
        let row = self.roots.get_mut(&task).expect("same original Root slot");
        if row.readers.len() >= self.per_root {
            return Err(root_position_capacity(
                self.per_root,
                row.readers.len(),
                novarocks_execution_contract::failure::ResourceScope::Task,
            ));
        }
        assert!(row.readers.insert(work));
        Ok(())
    }
    pub(super) fn release(&mut self, task: TaskIdentity, work: ScopedWorkRef) {
        if let Some(row) = self.roots.get_mut(&task) {
            row.readers.remove(&work);
            row.cut_waiters.remove(&work);
            self.remove_empty_cut_notice(task);
        }
    }
    fn remove_empty_cut_notice(&mut self, task: TaskIdentity) {
        use novarocks_control_runtime_core::fair_queue::FairQueueError;
        let row = self.roots.get_mut(&task).expect("same original Root slot");
        if !row.cut_waiters.is_empty() {
            return;
        }
        row.cut_after = None;
        row.cut_until = None;
        row.cut_notified_again = false;
        match self.cut_ready.remove_queued(task, ()) {
            Ok(()) | Err(FairQueueError::UnknownWork) => {}
            Err(error) => {
                tracing::error!(
                    pid = std::process::id(),
                    ?error,
                    "Original Root cut notice removal failed"
                );
                std::process::abort();
            }
        }
    }
    pub(super) fn set_cut_waiting(
        &mut self,
        task: TaskIdentity,
        work: ScopedWorkRef,
        waiting: bool,
    ) {
        let row = self
            .roots
            .get_mut(&task)
            .expect("same original Root R slot");
        assert!(row.readers.contains(&work));
        if waiting {
            row.cut_waiters.insert(work);
        } else {
            row.cut_waiters.remove(&work);
            self.remove_empty_cut_notice(task);
        }
    }
    /// An actual Task source/cut exit starts one finite pass of that Task's
    /// original waiters. Fresh arrivals cannot reset a pass's frozen cursor.
    pub(super) fn source_progress(&mut self, task: TaskIdentity) {
        let Some(row) = self.roots.get_mut(&task) else {
            return;
        };
        if row.cut_waiters.is_empty() {
            return;
        }
        if row.cut_until.is_some() {
            row.cut_notified_again = true;
            return;
        }
        row.cut_after = None;
        row.cut_until = row.cut_waiters.last().copied();
        self.cut_ready
            .enqueue(task, ())
            .expect("same bounded original Root source notice");
    }
    pub(super) fn take_cut_retries(&mut self, budget: usize) -> Vec<ScopedWorkRef> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let mut result = Vec::new();
        for _ in 0..budget {
            let Some((task, ())) = self.cut_ready.pop() else {
                break;
            };
            self.cut_ready
                .finish(task, ())
                .expect("same original source notice");
            let row = self.roots.get_mut(&task).expect("same live Root slot");
            let until = row.cut_until.expect("actual source wake upper address");
            let next = row
                .cut_waiters
                .range((row.cut_after.map_or(Unbounded, Excluded), Included(until)))
                .next()
                .copied();
            if let Some(work) = next {
                row.cut_after = Some(work);
                result.push(work);
            }
            let remaining = row
                .cut_waiters
                .range((row.cut_after.map_or(Unbounded, Excluded), Included(until)))
                .next()
                .is_some();
            if !remaining {
                row.cut_until = None;
                row.cut_after = None;
                if row.cut_notified_again {
                    row.cut_notified_again = false;
                    row.cut_until = row.cut_waiters.last().copied();
                }
            }
            if row.cut_until.is_some() {
                self.cut_ready
                    .enqueue(task, ())
                    .expect("same bounded source wake continuation");
            }
        }
        result
    }
    pub(super) fn cut_retries_remaining(&self) -> bool {
        !self.cut_ready.is_empty()
    }
    /// Actual Task retirement hands over this exact root, rather than a Task
    /// snapshot/result cache. No raw Arc getter is exposed to readers.
    pub(crate) fn adopt_installed_root(
        &mut self,
        context: novarocks_execution_contract::QueryContextRef,
        scope: &novarocks_control_runtime::transaction::ScopeHandle,
        root: crate::control_runtime::task::RootContextRetention,
    ) -> Result<
        (),
        (
            RootReadError,
            crate::control_runtime::task::RootContextRetention,
        ),
    > {
        let task = root.identity();
        if root.context() != context || !root.matches_scope(scope) {
            return Err((RootReadError::WrongOwner, root));
        }
        if !self.roots.contains_key(&task) {
            if self.roots.len() >= self.max_roots {
                return Err((
                    root_position_capacity(
                        self.max_roots,
                        self.roots.len(),
                        novarocks_execution_contract::failure::ResourceScope::Query,
                    ),
                    root,
                ));
            }
            self.roots.insert(
                task,
                RootSlot {
                    retained: None,
                    task_retired: false,
                    readers: BTreeSet::new(),
                    cut_waiters: BTreeSet::new(),
                    cut_after: None,
                    cut_until: None,
                    cut_notified_again: false,
                },
            );
        }
        let row = self.roots.get_mut(&task).expect("same actual retired Root");
        if row.retained.is_some() {
            return Err((RootReadError::WrongOwner, root));
        }
        if let Err(error) = root.adopt() {
            return Err((RootReadError::Task(error), root));
        }
        row.retained = Some(ManuallyDrop::new(root));
        Ok(())
    }
    pub(super) fn has_retained(&self, task: TaskIdentity) -> bool {
        self.roots
            .get(&task)
            .is_some_and(|root| root.task_retired && root.retained.is_some())
    }
    pub(super) fn has_root(&self, task: TaskIdentity) -> bool {
        self.roots
            .get(&task)
            .is_some_and(|row| row.retained.is_some())
    }
    pub(super) fn mark_task_retired(&mut self, task: TaskIdentity) {
        if let Some(row) = self.roots.get_mut(&task)
            && !row.task_retired
            && let Some(root) = &row.retained
        {
            root.task_retired();
            row.task_retired = true;
            // EOF may have reached a closed Task issuer before this actual
            // seal/read handoff. Wake only its original parked R addresses;
            // they now observe this positive receipt and use the direct path.
            self.source_progress(task);
        }
    }
    /// One original, sealed Root per metadata slice. Closed read admission and
    /// real producer exit make physical convergence independent of Task slots.
    pub(super) fn next_release_root(
        &self,
        after: Option<TaskIdentity>,
    ) -> Option<(TaskIdentity, bool)> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.roots
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()
            .map(|(&task, row)| {
                (
                    task,
                    row.readers.is_empty()
                        && row
                            .retained
                            .as_ref()
                            .is_none_or(|root| root.physical_idle()),
                )
            })
    }
    pub(super) fn begin_retained(
        &self,
        read: &RootResultRead,
    ) -> Result<RootChannelRead, RootReadError> {
        self.roots
            .get(&read.root_task())
            .and_then(|row| row.retained.as_ref())
            .ok_or(RootReadError::UnknownTask)?
            .begin_original_root_read(read)
            .map_err(RootReadError::Task)
    }
    pub(super) fn closed_read_frontier(
        &self,
        read: &RootResultRead,
    ) -> Result<Option<u64>, RootReadError> {
        match self
            .roots
            .get(&read.root_task())
            .and_then(|row| row.retained.as_ref())
        {
            Some(root) => root.closed_read_frontier(read).map_err(RootReadError::Task),
            None => Ok(None),
        }
    }
    pub(crate) fn readers_empty(&self) -> bool {
        self.roots.values().all(|row| row.readers.is_empty()) && self.cut_ready.is_empty()
    }
    pub(super) fn prepare_retained_seal(
        &self,
        after: Option<TaskIdentity>,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
    ) -> Result<
        Option<(
            TaskIdentity,
            crate::control_runtime::task::RetainedRootSealPreparation,
        )>,
        crate::control_runtime::task::RetainedRootSealError,
    > {
        use crate::control_runtime::task::RetainedRootSealPreparation;
        use std::ops::Bound::{Excluded, Unbounded};
        let Some((&task, row)) = self
            .roots
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()
        else {
            return Ok(None);
        };
        let state = match &row.retained {
            Some(root) => root.prepare_seal_for_context(reason)?,
            // The live Task phase seals its actual roots before this pass.
            // This row contains no retained Context root to hand off.
            None => RetainedRootSealPreparation::Ready,
        };
        Ok(Some((task, state)))
    }
    pub(crate) fn take_for_physical_disposal(&mut self) -> Option<RootReadDisposal> {
        if !self.readers_empty() {
            return None;
        }
        Some(RootReadDisposal(ManuallyDrop::new(std::mem::take(
            &mut self.roots,
        ))))
    }
}
/// The original H/cleanup job owns this value until all actual channel and
/// RootBaseCredit destructors finish. It cannot be silently dropped on poller.
pub(crate) struct RootReadDisposal(ManuallyDrop<BTreeMap<TaskIdentity, RootSlot>>);
impl RootReadDisposal {
    pub(crate) fn dispose(self) {
        let roots = ManuallyDrop::into_inner(self.0);
        for (_, mut row) in roots {
            if let Some(root) = row.retained.take() {
                ManuallyDrop::into_inner(root).dispose_on_host();
            }
        }
    }
}

pub(super) struct RootReadData {
    pub(super) role: ExitRole,
    pub(super) report: ReportSlot,
    pub(super) observer: WorkRetirementObserver,
    pub(super) work: ScopedWorkRef,
    pub(super) consumed: bool,
}
struct DataBody {
    authority: Option<ExitAuthority>,
    failed: AtomicBool,
    shared: Arc<RootReadShared>,
}
impl crate::root_result_channel::RootDeliveryAttachment for DataBody {
    fn end_acknowledged(&self, fact: RootEndAcknowledged) {
        self.shared.observe_end_acknowledged(fact);
    }
}
impl Drop for DataBody {
    fn drop(&mut self) {
        if let Some(authority) = self.authority.take() {
            if self.failed.load(Ordering::Acquire) || std::thread::panicking() {
                drop(authority);
            } else {
                let _ = authority.exited();
            }
        }
        self.shared.notice.notify();
    }
}
#[derive(Clone)]
pub(crate) struct RootReadDataGuard(Arc<DataBody>);
impl Drop for RootReadDataGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.failed.store(true, Ordering::Release);
        }
    }
}
impl RootReadDataGuard {
    fn attachment(&self) -> Arc<dyn crate::root_result_channel::RootDeliveryAttachment> {
        self.0.clone()
    }
    pub(super) fn new(authority: ExitAuthority, shared: Arc<RootReadShared>) -> Self {
        Self(Arc::new(DataBody {
            authority: Some(authority),
            failed: AtomicBool::new(false),
            shared,
        }))
    }
}
pub(super) struct RootReadShared {
    end_acknowledged: Mutex<bool>,
    pub(super) notice: RequestNotice,
}
impl RootReadShared {
    pub(super) fn new(notice: RequestNotice) -> Arc<Self> {
        Arc::new(Self {
            end_acknowledged: Mutex::new(false),
            notice,
        })
    }
    // The private channel receipt is moved through this same R's admitted
    // delivery attachment. Coalescing it retains no Root/guard/second source.
    fn observe_end_acknowledged(&self, _fact: RootEndAcknowledged) {
        let changed = {
            let mut actual = self
                .end_acknowledged
                .lock()
                .expect("original Root R ACK fact");
            let changed = !*actual;
            *actual = true;
            changed
        };
        if changed {
            self.notice.notify();
        }
    }
    pub(super) fn end_acknowledged(&self) -> bool {
        *self
            .end_acknowledged
            .lock()
            .expect("original Root R ACK fact")
    }
}

pub(crate) struct NodeRootRead {
    pub(super) read: RootChannelRead,
    pub(super) task: TaskIdentity,
    pub(super) handle: TaskContextRequestHandle,
    pub(super) shared: Arc<RootReadShared>,
    pub(super) holder: RootReadDataGuard,
}
pub(crate) struct NodeRootDelivery {
    delivery: RootResultDelivery,
    drain: Option<TaskRootDrainPermit>,
    _holder: RootReadDataGuard,
}
impl NodeRootRead {
    // These forward the same admitted read. Native can reserve metadata before
    // applying the actual caller ACK, then acquire payload backing and read.
    pub(crate) fn try_reserve_native_send_metadata(
        &self,
        bytes: usize,
    ) -> Result<crate::root_result_channel::RootMetadataReservation, RootChannelError> {
        self.read.try_reserve_native_send_metadata(bytes)
    }
    pub(crate) fn apply_consumed(&self) -> Result<u64, RootChannelError> {
        self.read.apply_consumed()
    }
    pub(crate) fn accepted_consumed(&self) -> u64 {
        self.read.accepted_consumed()
    }
    pub(crate) fn try_reserve_native_send_backing(
        &self,
    ) -> Result<novarocks_execution::runtime::fragment::io::ResultWriteAdmission, RootChannelError>
    {
        self.read.try_reserve_native_send_backing()
    }
    pub(crate) fn native_send_backing_refusal(
        &self,
    ) -> Result<crate::root_result_channel::RootCapacityRefusal, RootChannelError> {
        self.read.native_send_backing_refusal()
    }
    pub(crate) async fn read(self) -> Result<NodeRootDelivery, RootReadError> {
        let delivery = self.read.read().await.map_err(RootReadError::Channel)?;
        // The fact was already accepted by this R before any data await.
        // Returning or dropping this observer cannot withdraw that handoff.
        let drain = self.shared.end_acknowledged().then(|| {
            TaskRootDrainPermit::from_admitted_role(
                self.task,
                Box::new(RootDrain {
                    handle: Some(self.handle),
                    shared: self.shared,
                    holder: self.holder.clone(),
                }),
            )
        });
        Ok(NodeRootDelivery {
            delivery,
            drain,
            _holder: self.holder,
        })
    }
}
impl NodeRootDelivery {
    pub(crate) fn reply(&self) -> &RootResultReply {
        self.delivery.reply()
    }
    pub(crate) fn take_drain_permit(&mut self) -> Option<TaskRootDrainPermit> {
        self.drain.take()
    }
    /// Move the same delivery position into Native's actual send owner. Its
    /// attachment retains this R's data holder through every backing alias.
    pub(crate) fn into_parts(
        self,
    ) -> (
        RootResultReply,
        crate::root_result_channel::RootDeliveryOwner,
        Option<TaskRootDrainPermit>,
    ) {
        let Self {
            delivery,
            drain,
            _holder,
        } = self;
        let (reply, guard) = delivery.into_parts();
        drop(_holder);
        (reply, guard, drain)
    }
}
struct RootDrain {
    handle: Option<TaskContextRequestHandle>,
    shared: Arc<RootReadShared>,
    holder: RootReadDataGuard,
}
impl TaskRootDrainCapability for RootDrain {
    fn submit(mut self: Box<Self>) -> TaskRootDrainFuture {
        // Only observe the R's existing fact/reply. The channel already
        // transferred responsibility even if this capability is never used.
        assert!(self.shared.end_acknowledged(), "original Root ACK fact");
        let handle = self.handle.take().expect("unique original Root R");
        let holder = self.holder.clone();
        Box::pin(async move {
            // The same generic R future owns the only reply observer. Data
            // custody remains independent through the real ACK and aliases.
            let _holder = holder;
            match handle.await {
                Ok(delivery) => drain_outcome(delivery.body()),
                Err(_) => root_owner_failed(),
            }
        })
    }
    fn revoke_unused(self: Box<Self>) {
        drop(self);
    }
}
fn drain_outcome(body: &ContextReplyBody) -> TaskRootDrainOutcome {
    match body {
        ContextReplyBody::RootDrain(RootDrainVerdict::AlreadyTerminal(state)) => {
            TaskRootDrainOutcome::AlreadyTerminal { state: *state }
        }
        ContextReplyBody::RootDrain(RootDrainVerdict::Published(version)) => {
            TaskStatusVersion::new(*version)
                .map(|version| TaskRootDrainOutcome::EndPublished { version })
                .unwrap_or_else(|_| root_owner_failed())
        }
        ContextReplyBody::Verdict(RequestResult::Outcome(
            novarocks_execution_contract::OperationOutcome::OwnerFailed,
        )) => root_owner_failed(),
        ContextReplyBody::Verdict(RequestResult::Outcome(outcome)) => {
            TaskRootDrainOutcome::Refused(*outcome)
        }
        _ => root_owner_failed(),
    }
}
fn root_owner_failed() -> TaskRootDrainOutcome {
    TaskRootDrainOutcome::OwnerFailed {
        fact: novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::ProtocolOwnerFailed.reason(),
        ),
    }
}
impl ContextHostRecord {
    pub(crate) fn begin_root_read(
        &mut self,
        ready: &ControlRequestReady,
        _source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        read: &RootResultRead,
    ) -> Result<NodeRootRead, RootReadError> {
        let task = read.root_task();
        if task.verify_query_context(self.identity).is_err() {
            return Err(RootReadError::WrongOwner);
        }
        let (work, handle, shared) =
            self.admit_deferred_root_request(ready, read, operation, deadline)?;
        let holder = match self.admit_root_read_data(work) {
            Ok(holder) => holder,
            Err(error) => {
                drop(handle);
                return Err(RootReadError::Request(error));
            }
        };
        let channel_read = if self.resources.root_reads.has_root(task) {
            self.resources.root_reads.begin_retained(read)
        } else {
            match self.tasks.get(&task) {
                Some(super::TaskHostOwner::Ready(owner)) => owner
                    .begin_original_root_read(read)
                    .map_err(RootReadError::Task),
                _ => self.resources.root_reads.begin_retained(read),
            }
        };
        let mut read = match channel_read {
            Ok(read) => read,
            Err(error) => {
                drop(handle);
                drop(holder);
                if matches!(
                    error,
                    RootReadError::Channel(RootChannelError::Closed)
                        | RootReadError::Task(TaskRootReadError::Channel(RootChannelError::Closed))
                ) && let Some(accepted_consumed) =
                    self.resources.root_reads.closed_read_frontier(read)?
                {
                    return Err(RootReadError::Closed { accepted_consumed });
                }
                return Err(error);
            }
        };
        // The original channel's guarded Bytes already retain this exact
        // position. Attach the admitted R body there before any read/alias.
        read.attach_delivery_lifetime(holder.attachment())
            .unwrap_or_else(|_original| {
                tracing::error!(
                    pid = std::process::id(),
                    "Original Root delivery lifetime attachment failed"
                );
                std::process::abort();
            });
        Ok(NodeRootRead {
            read,
            task,
            handle,
            shared,
            holder,
        })
    }
}

impl RootReadState {
    /// Sample only original installed tokens, including Context-retained Roots.
    pub(super) fn try_ownership_snapshot(
        &self,
    ) -> Option<crate::root_result_channel::RootOwnershipSnapshot> {
        let mut total = crate::root_result_channel::RootOwnershipSnapshot::default();
        for row in self.roots.values() {
            if let Some(root) = &row.retained {
                total.try_add(root.try_ownership_snapshot()?)?;
            }
        }
        Some(total)
    }
    pub(super) fn installed_root_tasks(&self) -> impl Iterator<Item = TaskIdentity> + '_ {
        self.roots
            .iter()
            .filter_map(|(task, row)| row.retained.as_ref().map(|_| *task))
    }
}
