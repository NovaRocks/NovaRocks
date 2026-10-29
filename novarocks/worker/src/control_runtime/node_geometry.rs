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

//! Checked composition geometry for the actual Node owners.
//!
//! Resource units, Scope Work/report records and capacity reservation rows are
//! different inventories. In particular a zero-resource reported Work owns a
//! reservation row, and its independent Source/host-call payment owns another.
//! No byte limit is used as a count of headers.
//!
//! This manifest covers the implemented Node/Task/Root-read factories. It does
//! not claim that domain waiters, covered subscriptions or an RF getter issuer
//! have been implemented merely because the fixed policy reserves their
//! vocabulary. A new issuer must preserve these original once-only holders or
//! extend this manifest before becoming an admitted production entry point.

use super::{
    context_host::{CONTEXT_BOOTSTRAP_STORAGE, CONTEXT_REQUEST_STORAGE},
    control_internal::CONTROL_INTERNAL_STORAGE,
    control_request::{CONTROL_REQUEST_STORAGE, ESTABLISH_REQUEST_STORAGE},
    process_control::PROCESS_CONTROL_BOOTSTRAP_STORAGE,
    task_host_call::HOST_CALL_KINDS,
};
use crate::{
    TaskInboundCapabilityLimits,
    control_admission::{AdmissionStorage, Dimension},
    control_runtime_policy::{
        ControlRuntimePolicy, PolicyError, ROOT_READ_DATA_STORAGE, StorageNeed, TaskLifecycleRole,
    },
};
use novarocks_control_runtime_core::capacity::ResourceVector;
use std::num::NonZeroUsize;

/// C includes retained and physically removing Contexts. Its original
/// Contexts unit must stay held through actual parent-link/admission teardown.
/// T bounds cumulative Task identities in one Context, including retained
/// records; an execution with no Root does not consume a separate Root slot.
#[derive(Clone, Copy, Debug)]
pub struct NodeGeometryInputs {
    pub contexts: NonZeroUsize,
    pub tasks_per_context: NonZeroUsize,
    pub active_tasks: NonZeroUsize,
    pub preparing_tasks: NonZeroUsize,
    pub preparing_bytes: NonZeroUsize,
    pub ticket_live: NonZeroUsize,
    pub ticket_records: NonZeroUsize,
    pub inbound: TaskInboundCapabilityLimits,
    /// The one process policy already includes actual Root-RPC positions in R.
    pub policy: ControlRuntimePolicy,
    /// Taken from the actual Native result geometry, never guessed from R or T.
    pub native_root_data_holders: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeGeometryError {
    Overflow,
    Policy(PolicyError),
    PolicyMismatch,
    RootHolderMismatch,
    TicketRecordsBelowLive,
    InvalidBootstrapManifest,
}
impl From<PolicyError> for NodeGeometryError {
    fn from(error: PolicyError) -> Self {
        Self::Policy(error)
    }
}
impl std::fmt::Display for NodeGeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("Node geometry overflowed"),
            Self::Policy(error) => write!(f, "invalid Node storage policy: {error}"),
            Self::PolicyMismatch => {
                f.write_str("Node policy differs from its actual ingress manifest")
            }
            Self::RootHolderMismatch => {
                f.write_str("Node Root data positions differ from Native geometry")
            }
            Self::TicketRecordsBelowLive => {
                f.write_str("ticket records cannot be below live ticket positions")
            }
            Self::InvalidBootstrapManifest => {
                f.write_str("Node bootstrap differs from its original owner manifest")
            }
        }
    }
}
impl std::error::Error for NodeGeometryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NodeScopeGeometry {
    pub max_work: usize,
    /// Report records plus actual actor attachments; not reservation rows.
    pub max_slots: usize,
}

/// These are disjoint families of original reservation holders. The shared R
/// term is the largest actual per-R branch, not a sum of all branch maxima.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NodeReservationGeometry {
    pub root_fixed: usize,
    pub context_bootstrap: usize,
    pub tickets: usize,
    pub task_non_source_work: usize,
    pub task_source_work: usize,
    pub task_source_payments: usize,
    pub task_host_payments: usize,
    pub shared_requests: usize,
    pub root_data_work: usize,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct NodeGeometry {
    pub(crate) limits: ResourceVector,
    pub(crate) admission: AdmissionStorage,
    pub(crate) storage: StorageNeed,
    pub(crate) request_per_position: StorageNeed,
    pub(crate) root_data_per_position: StorageNeed,
    pub(crate) reservations: NodeReservationGeometry,
    pub(crate) root_scope: NodeScopeGeometry,
    pub(crate) context_scope: NodeScopeGeometry,
    pub(crate) source_scope: NodeScopeGeometry,
    pub(crate) max_scopes: usize,
    pub(crate) max_actors: usize,
    pub(crate) max_roots: usize,
    pub(crate) root_data_positions: usize,
    pub(crate) request_positions: NonZeroUsize,
    pub(crate) inputs: NodeGeometryInputs,
}

/// One admitted ticket has one original Work/payment and one capture, but no
/// report or actor input. Replay/expired ticket records are pure metadata;
/// ticket_records does not manufacture another set of physical holders.
const TICKET_STORAGE: StorageNeed = StorageNeed {
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    completions: 0,
    captures: 1,
    work: 1,
    requests: 0,
};

fn add(a: usize, b: usize) -> Result<usize, NodeGeometryError> {
    a.checked_add(b).ok_or(NodeGeometryError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, NodeGeometryError> {
    a.checked_mul(b).ok_or(NodeGeometryError::Overflow)
}
fn sum(parts: impl IntoIterator<Item = usize>) -> Result<usize, NodeGeometryError> {
    parts.into_iter().try_fold(0, add)
}
fn units(value: usize) -> Result<u64, NodeGeometryError> {
    u64::try_from(value).map_err(|_| NodeGeometryError::Overflow)
}
fn envelope(a: StorageNeed, b: StorageNeed) -> StorageNeed {
    StorageNeed {
        inputs: a.inputs.max(b.inputs),
        batches: a.batches.max(b.batches),
        effects: a.effects.max(b.effects),
        cuts: a.cuts.max(b.cuts),
        replies: a.replies.max(b.replies),
        completions: a.completions.max(b.completions),
        captures: a.captures.max(b.captures),
        work: a.work.max(b.work),
        requests: a.requests.max(b.requests),
    }
}

impl NodeGeometry {
    /// The one shared actual external request-position budget.
    pub fn request_positions(&self) -> NonZeroUsize {
        self.request_positions
    }

    pub fn try_new(inputs: NodeGeometryInputs) -> Result<Self, NodeGeometryError> {
        let policy = inputs.policy;
        let actual = ControlRuntimePolicy::try_new_with_request_sources(
            policy.ingress,
            policy.root_request_positions,
            policy.observation_request_positions,
        )?;
        if actual != policy {
            return Err(NodeGeometryError::PolicyMismatch);
        }
        if policy.ingress.root_read_holders != inputs.native_root_data_holders.get() {
            return Err(NodeGeometryError::RootHolderMismatch);
        }
        if inputs.ticket_records < inputs.ticket_live {
            return Err(NodeGeometryError::TicketRecordsBelowLive);
        }
        let c = inputs.contexts.get();
        let t = inputs.tasks_per_context.get();
        let a = inputs.active_tasks.get();
        let r = policy.request_roles;
        let requests = NonZeroUsize::new(r).ok_or(NodeGeometryError::PolicyMismatch)?;
        let roots = mul(c, t)?;
        let root_holders = mul(roots, inputs.native_root_data_holders.get())?;
        // The original Root R and its reader position stay held until even a
        // refused data-role partial truly exits/reaps. DeferredRootRequest owns
        // that partial; it must never become an unrelated requests=0 sibling.
        let data_positions = r.min(root_holders);

        // Root, Context and Task RPCs compete for the same global R. Establish
        // includes its source child, Context probe and Root return. Root read
        // reuses the Task-R profile; its data holder uses the separate Root pool.
        let per_request = [
            CONTROL_REQUEST_STORAGE,
            ESTABLISH_REQUEST_STORAGE,
            CONTEXT_REQUEST_STORAGE,
            StorageNeed::domain_request()?,
        ]
        .into_iter()
        .reduce(envelope)
        .expect("the closed actual request manifest is nonempty");
        let storage = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .checked_add(CONTROL_INTERNAL_STORAGE)?
            .checked_add(CONTEXT_BOOTSTRAP_STORAGE.checked_mul(c)?)?
            .checked_add(TICKET_STORAGE.checked_mul(inputs.ticket_live.get())?)?
            .checked_add(policy.task_base.checked_mul(a)?)?
            .checked_add(ROOT_READ_DATA_STORAGE.checked_mul(data_positions)?)?
            .checked_add(per_request.checked_mul(r)?)?;
        let dimensions = ResourceVector::new([
            (Dimension::ActiveTasks.id(), units(a)?),
            (
                Dimension::PreparingTasks.id(),
                units(inputs.preparing_tasks.get())?,
            ),
            (
                Dimension::PreparingBytes.id(),
                units(inputs.preparing_bytes.get())?,
            ),
            (
                Dimension::InboundRecords.id(),
                units(inputs.inbound.max_records())?,
            ),
            (
                Dimension::InboundBytes.id(),
                units(inputs.inbound.max_bytes())?,
            ),
            (Dimension::Contexts.id(), units(c)?),
            (Dimension::Tickets.id(), units(inputs.ticket_live.get())?),
        ])
        .map_err(|_| NodeGeometryError::Overflow)?;
        let limits = storage
            .resources()?
            .checked_add(&dimensions)
            .map_err(|_| NodeGeometryError::Overflow)?;
        let admission = AdmissionStorage {
            loading: policy.loading.resources()?,
            accepted: policy.accepted.resources()?,
            context: CONTEXT_BOOTSTRAP_STORAGE.resources()?,
            ticket: TICKET_STORAGE.resources()?,
        };

        // Root fixed Work: lifetime, timer, failure and the one internal source.
        let root_fixed = add(
            PROCESS_CONTROL_BOOTSTRAP_STORAGE.work,
            CONTROL_INTERNAL_STORAGE.work,
        )?;
        // Context's two parent-side Work are admission and parent link. Its
        // other bootstrap Work live in the child; the link has no reservation.
        let parent_context_work = 2;
        let local_context_work = CONTEXT_BOOTSTRAP_STORAGE
            .work
            .checked_sub(parent_context_work)
            .ok_or(NodeGeometryError::InvalidBootstrapManifest)?;
        if root_fixed != 4 || local_context_work != 6 {
            return Err(NodeGeometryError::InvalidBootstrapManifest);
        }
        let root_scope = NodeScopeGeometry {
            max_work: sum([
                root_fixed,
                inputs.ticket_live.get(),
                mul(parent_context_work, c)?,
                mul(2, r)?,
            ])?,
            // Root R report; source child and Context probe reports live in
            // their own actual scopes. Root metadata/internal have no report.
            max_slots: add(1, r)?,
        };
        // Matches ContextHostRecord::adopt_with_routes. A single Context can
        // receive all R; this does not reserve C*R resource units. Route probes
        // use the R-funded escrow, not another Context charge.
        let context_scope = NodeScopeGeometry {
            max_work: sum([
                mul(policy.task_base.work, t)?,
                mul(policy.root_read_pool_per_root.work, t)?,
                policy.request_pool.work,
                r,
                local_context_work,
            ])?,
            max_slots: sum([
                mul(policy.task_base.completions, t)?,
                mul(policy.root_read_pool_per_root.completions, t)?,
                policy.request_pool.completions,
                r,
                1, // The actual Context actor attachment.
                super::context_io::CONTEXT_IO_STORAGE.completions,
            ])?,
        };

        // Fixed Task Common Work, counted from actual original issuers:
        // lifecycle manifest (loading's two authors share loader/input Work),
        // lifetime, preparation, the one refusal child, and two host calls.
        // Native preparation body/step counters themselves mint no Common
        // Work; PreparationParent::derive's sole production caller is the
        // once-only RefusalPosition in TaskPreparationJob::install_complete.
        let non_source_per_task = sum([
            TaskLifecycleRole::COUNT,
            1,
            1,
            crate::control_runtime::preparation_refusal::HOST_REFUSAL_WORK_POSITIONS,
            HOST_CALL_KINDS.len(),
        ])?;
        // Source *locations*, not their old funders: open/spare Source4 plus
        // one pair per finite original lifecycle cut (including pending/partial
        // admission or returned host_retired_pairs). RPC cuts use the per-R
        // term below. R refund waits true detachment of its returned prefix;
        // a funded pair rotated into future open replaces a base location.
        // Rearm requires true old-generation Work reaping and moves the same
        // fee, so it creates no simultaneous extra generation/fee here.
        let task_source_positions = mul(add(4, mul(2, TaskLifecycleRole::COUNT)?)?, a)?;
        // Each fee is one original Work1 payment. An unissued accepted source
        // pool contains >=1 such unit and occupies <= its remaining units;
        // the last unit moves whole. Empty/all split is forbidden by Common,
        // and release(empty remaining) removes the actual row. Thus pool,
        // detached fee and loose partial fee are alternatives, never +1 each.
        let source_payment_rows = task_source_positions;
        let task_host_payment_rows = mul(HOST_CALL_KINDS.len(), a)?;
        // Actual per-R row peaks: plain Root/Context=1; Establish=main +
        // source-child root + temporary Context zero + escrow=4 (parent link
        // has no row); Task=author+outer+2 source zero+2 source fee=6, plus
        // the separately paid domain feedback of that same six-row form. Partial
        // refusal retains subsets of these same originals. Notification uses
        // that original R Work and fixed captures, never another reservation.
        let request_row_peak = 1usize.max(4).max(6 + 6);
        let mut reservations = NodeReservationGeometry {
            root_fixed,
            context_bootstrap: mul(
                CONTEXT_BOOTSTRAP_STORAGE
                    .work
                    .checked_sub(1)
                    .ok_or(NodeGeometryError::InvalidBootstrapManifest)?,
                c,
            )?,
            tickets: inputs.ticket_live.get(),
            task_non_source_work: mul(non_source_per_task, a)?,
            task_source_work: task_source_positions,
            task_source_payments: source_payment_rows,
            task_host_payments: task_host_payment_rows,
            shared_requests: mul(request_row_peak, r)?,
            root_data_work: data_positions,
            total: 0,
        };
        reservations.total = sum([
            reservations.root_fixed,
            reservations.context_bootstrap,
            reservations.tickets,
            reservations.task_non_source_work,
            reservations.task_source_work,
            reservations.task_source_payments,
            reservations.task_host_payments,
            reservations.shared_requests,
            reservations.root_data_work,
        ])?;
        Ok(Self {
            limits,
            admission,
            storage,
            request_per_position: per_request,
            root_data_per_position: ROOT_READ_DATA_STORAGE,
            reservations,
            root_scope,
            context_scope,
            source_scope: NodeScopeGeometry {
                max_work: 1,
                max_slots: 1,
            },
            // One Root, C Context children and at most R actual Establish
            // source children. Parent links consume Work, not another scope.
            max_scopes: sum([1, c, r])?,
            max_actors: add(1, c)?,
            max_roots: roots,
            root_data_positions: data_positions,
            request_positions: requests,
            inputs,
        })
    }
}
