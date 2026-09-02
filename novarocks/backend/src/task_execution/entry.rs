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

//! The owner's private state: one entry per query context, one entry per task.
//!
//! The four task states are what makes the creation transaction observable as
//! atomic. `Creating` is opaque to every operation except a converging create,
//! `Live` is the only findable running task, `Retired` is the secret-free
//! terminal record, and `Gone` is the retirement fence that keeps a legal late
//! request from being mistaken for a brand new task.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use novarocks_execution::task_execution::descriptor::TaskDescriptor;
use novarocks_execution::task_execution::domain::{
    ConfidentialContent, ContentFingerprint, CredentialDomain, ScalarDomain,
};
use novarocks_execution::task_execution::identity::{TaskIdentity, TaskOperationId};
use novarocks_execution::task_execution::lease::{InstalledLease, LeaseValidFor, MonotonicInstant};
use novarocks_execution::task_execution::operation::{
    CreateTaskReceipt, EstablishQueryContext, OperationOutcome,
};
use novarocks_execution::task_execution::status::{
    AbortCause, FinalTaskInfo, TaskStatus, TerminationDetail,
};
use novarocks_execution::task_execution::transition::{QueryContextState, TerminationLatch};

use super::domains::{InitialDomainKey, TaskDomains};
use super::host::RunnableTask;
use super::observation::TaskStatusSource;
use super::status::TaskStatusOwner;

/// The comparable, secret-free identity of one establish request.
///
/// The credential material is deliberately absent: it cannot be
/// fingerprinted, so an exact replay is recognised by comparing the live
/// material through [`ConfidentialContent::matches`] instead.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct EstablishRecord {
    pub(super) operation: TaskOperationId,
    pub(super) catalog: ContentFingerprint,
    pub(super) runtime_filter: ContentFingerprint,
    pub(super) credential_lease: novarocks_execution::task_execution::domain::CredentialLeaseId,
    pub(super) credential_epoch: novarocks_execution::task_execution::domain::CredentialEpoch,
    pub(super) lease_valid_for: LeaseValidFor,
}

impl EstablishRecord {
    pub(super) fn of(request: &EstablishQueryContext) -> Self {
        Self {
            operation: request.envelope().operation_id(),
            catalog: request.catalog_binding().fingerprint(),
            runtime_filter: request.initial_runtime_filter().fingerprint(),
            credential_lease: request.initial_credential().lease_id(),
            credential_epoch: request.initial_credential().epoch(),
            lease_valid_for: request.initial_lease_valid_for(),
        }
    }

    /// Whether two establishes are the same immutable request.
    ///
    /// The operation id is deliberately not compared: two concurrent
    /// establishes of the identical content are the same creation, and the
    /// frontend's own retry reuses its id anyway.
    pub(super) fn same_content(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.runtime_filter == other.runtime_filter
            && self.credential_lease == other.credential_lease
            && self.credential_epoch == other.credential_epoch
            && self.lease_valid_for == other.lease_valid_for
    }
}

/// Why a creation transaction rolled back.
///
/// It is shared with every create that converged on the same creation, so
/// they all observe the same failure rather than each retrying separately.
#[derive(Clone, Debug)]
pub(super) struct CreationFailure {
    pub(super) outcome: OperationOutcome,
    pub(super) detail: String,
}

/// The reservation one creation owner holds on a task identity.
pub(super) struct CreationCell {
    pub(super) fingerprint: ContentFingerprint,
    pub(super) initial_domains: Vec<InitialDomainKey>,
    failure: Mutex<Option<CreationFailure>>,
}

impl CreationCell {
    pub(super) fn new(
        fingerprint: ContentFingerprint,
        initial_domains: Vec<InitialDomainKey>,
    ) -> Self {
        Self {
            fingerprint,
            initial_domains,
            failure: Mutex::new(None),
        }
    }

    pub(super) fn same_creation(
        &self,
        fingerprint: ContentFingerprint,
        initial_domains: &[InitialDomainKey],
    ) -> bool {
        self.fingerprint == fingerprint && self.initial_domains == initial_domains
    }

    pub(super) fn fail(&self, failure: CreationFailure) {
        *self.failure.lock().expect("creation cell lock") = Some(failure);
    }

    pub(super) fn failure(&self) -> Option<CreationFailure> {
        self.failure.lock().expect("creation cell lock").clone()
    }
}

/// One installed, running task.
pub(super) struct LiveTask {
    pub(super) descriptor: Arc<TaskDescriptor>,
    pub(super) fingerprint: ContentFingerprint,
    pub(super) initial_domains: Vec<InitialDomainKey>,
    pub(super) receipt: CreateTaskReceipt,
    pub(super) status: Arc<TaskStatusOwner>,
    pub(super) runnable: Arc<dyn RunnableTask>,
    pub(super) domains: TaskDomains,
    pub(super) receiver_installed: bool,
    pub(super) capability_installed: bool,
}

/// The retained terminal record of one task.
///
/// It is secret-free by construction: the descriptor, the plan, the split
/// payloads, and every credential are dropped at retirement and only the
/// receipt, the descriptor fingerprint, the immutable terminal status, the
/// final info, and the retirement instant survive.
pub(super) struct RetiredTask {
    pub(super) fingerprint: ContentFingerprint,
    pub(super) initial_domains: Vec<InitialDomainKey>,
    pub(super) receipt: CreateTaskReceipt,
    pub(super) status: TaskStatus,
    pub(super) final_info: Option<FinalTaskInfo>,
    pub(super) retired_at: MonotonicInstant,
    pub(super) bytes: usize,
}

pub(super) enum TaskEntry {
    /// A creation transaction owns this identity. Nothing but a converging
    /// create can see it, so a rollback leaves no half task behind.
    Creating(Arc<CreationCell>),
    Live(Box<LiveTask>),
    Retired(Box<RetiredTask>),
    /// The retained record was reclaimed. The fence stays behind so a legal
    /// late request is answered `Gone` rather than being mistaken for a task
    /// that never existed. It deliberately carries nothing else: its content
    /// is exactly the fact that this identity was used and is finished.
    Gone,
}

impl TaskEntry {
    pub(super) const fn is_terminal_record(&self) -> bool {
        matches!(self, Self::Retired(_) | Self::Gone)
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Retired(retired) => retired.bytes,
            _ => 0,
        }
    }
}

/// The estimated retained footprint of one terminal record.
///
/// It is an accounting estimate, not an encoded size: the point of the bound
/// is that retention cannot grow without limit, and a structural estimate
/// achieves that without encoding a record just to measure it.
pub(super) fn estimate_retained_bytes(
    receipt: &CreateTaskReceipt,
    status: &TaskStatus,
    final_info: Option<&FinalTaskInfo>,
) -> usize {
    use std::mem::size_of;

    let mut bytes = size_of::<CreateTaskReceipt>()
        + size_of::<TaskStatus>()
        + std::mem::size_of_val(receipt.domains());
    bytes += termination_bytes(status.termination());
    if let Some(info) = final_info {
        bytes += size_of::<FinalTaskInfo>();
        for statistics in info.operator_statistics() {
            bytes += size_of::<novarocks_execution::task_execution::status::OperatorStatistics>()
                + statistics.operator().as_str().len();
        }
        bytes += termination_bytes(info.final_status().termination());
    }
    bytes
}

fn termination_bytes(termination: Option<&TerminationDetail>) -> usize {
    match termination {
        Some(TerminationDetail::Failed(failure)) => failure.detail().as_str().len(),
        _ => 0,
    }
}

/// One query context on this backend.
pub(super) struct ContextEntry {
    pub(super) state: QueryContextState,
    pub(super) latch: TerminationLatch,
    pub(super) lease: Option<InstalledLease>,
    pub(super) establish: Option<EstablishRecord>,
    /// Shared facts become observable only when the context reaches `Active`.
    pub(super) facts_visible: bool,
    pub(super) catalog: ScalarDomain,
    pub(super) shared_filter: ScalarDomain,
    pub(super) credential: Option<CredentialDomain>,
    pub(super) credential_material: Option<Arc<dyn ConfidentialContent>>,
    pub(super) source: Arc<TaskStatusSource>,
    pub(super) tasks: BTreeMap<TaskIdentity, TaskEntry>,
    pub(super) retired_at: Option<MonotonicInstant>,
    /// When this context entered `ABORTING`. It bounds how long an
    /// uncooperative task may delay cleanup.
    pub(super) terminating_since: Option<MonotonicInstant>,
    /// The observation revision the last retirement pass saw. A context whose
    /// revision has not moved cannot have a newly retirable task, which keeps
    /// one operation from costing a scan of every task on the backend.
    pub(super) last_retire_revision: Option<u64>,
    /// Set once the shared facts have been handed back to the host, so a
    /// rollback and a normal release cannot both release them.
    pub(super) facts_released: bool,
}

impl ContextEntry {
    pub(super) fn absent(source: Arc<TaskStatusSource>) -> Self {
        Self {
            state: QueryContextState::Absent,
            latch: TerminationLatch::open(),
            lease: None,
            establish: None,
            facts_visible: false,
            catalog: ScalarDomain::empty(),
            shared_filter: ScalarDomain::empty(),
            credential: None,
            credential_material: None,
            source,
            tasks: BTreeMap::new(),
            retired_at: None,
            terminating_since: None,
            last_retire_revision: None,
            facts_released: false,
        }
    }

    /// How many task identities this context currently occupies.
    ///
    /// A creation in progress counts: two concurrent creates must not both
    /// slip past the per-context bound.
    pub(super) fn occupied_slots(&self) -> usize {
        self.tasks
            .values()
            .filter(|entry| !matches!(entry, TaskEntry::Gone))
            .count()
    }

    /// The abort cause a terminal receipt reports.
    ///
    /// A task failure that latched the context is reported as
    /// `PEER_TASK_FAILED`: the context ack carries the shared-resource cause
    /// only and is never a second authority over a task's own terminal.
    pub(super) fn termination_cause(&self) -> Option<AbortCause> {
        match self.latch.cause() {
            Some(TerminationDetail::Aborted(cause)) => Some(*cause),
            Some(TerminationDetail::Failed(_)) => Some(AbortCause::PeerTaskFailed),
            Some(TerminationDetail::Canceled(_)) => Some(AbortCause::QueryFailed),
            None => None,
        }
    }
}
