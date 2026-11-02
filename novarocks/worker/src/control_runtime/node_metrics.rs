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

//! Read-only observations of this same Node's actual admission ledger.

use crate::control_admission::Dimension;
pub use novarocks_control_runtime::observation::{
    CapacityRefusalKind as WorkerCapacityRefusalKind,
    LedgerInventoryObservation as WorkerRuntimeInventory,
    LedgerInventoryPage as WorkerRuntimeInventoryPage,
    RuntimeObservationSnapshot as WorkerRuntimeObservationSnapshot,
};
pub use novarocks_control_runtime::transaction::CapacityObservationError as WorkerRuntimeObservationError;
use novarocks_control_runtime::transaction::RuntimeLedger;
pub use novarocks_control_runtime_core::mailbox::InputClass as WorkerRuntimeInputClass;

/// The original Control Core's one published admission projection. Only the
/// epoch field carries its existing protocol meaning; the count grants nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ControlAdmissionProjection {
    pub epoch: novarocks_execution_contract::AdmissionEpochCapability,
    pub logical_live_grants: usize,
    pub admission_retention: novarocks_worker_protocol::admission::AdmissionRetentionInventory,
    pub control_retention: novarocks_worker_protocol::control::ControlRetentionInventory,
}

/// Issued/redeemed grants remain logical-live until Core commits their terminal
/// transition. This is a metadata count, not capacity or a physical-exit receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerAdmissionSnapshot {
    pub logical_live_grants: u64,
}

/// Resources are independently sampled from the original ledger. Contexts
/// count original metadata addresses, including retained actors, without a
/// quota. These samples are neither a coherent instant nor exit receipts.
/// Queue/running counts, Root census and limits are absent: no actual read port
/// for those observations is inferred from these charged-resource samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerNodeMetricsSnapshot {
    pub active_tasks: u64,
    pub preparing_tasks: u64,
    pub preparing_bytes: u64,
    pub requests: u64,
    pub contexts: u64,
}
impl WorkerNodeMetricsSnapshot {
    pub(super) fn sample(ledger: &RuntimeLedger, contexts: usize) -> Self {
        Self {
            active_tasks: ledger.used(Dimension::ActiveTasks.id()),
            preparing_tasks: ledger.used(Dimension::PreparingTasks.id()),
            preparing_bytes: ledger.used(Dimension::PreparingBytes.id()),
            requests: ledger.used(Dimension::Requests.id()),
            contexts: u64::try_from(contexts).expect("Context metadata count fits u64"),
        }
    }
}

/// Actual preparation charges and the original physical queue's occupancy.
/// The ledger fields share one transaction; queue fields share its own lock.
/// The two owners are sampled independently, never an exit/coherence receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPreparationSnapshot {
    pub positions: u64,
    pub position_limit: u64,
    pub context_positions: u64,
    pub context_position_limit: u64,
    pub queued_positions: u64,
    pub workers: u64,
    pub worker_limit: u64,
    pub bytes: u64,
    pub byte_limit: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerPreparationObservationError {
    Capacity(novarocks_control_runtime::transaction::CapacityObservationError),
    QueuePoisoned,
    MissingPhysicalOwner,
}
impl WorkerPreparationSnapshot {
    pub(super) fn sample(
        ledger: &RuntimeLedger,
        preparation: &super::host_executor::HostExecutorHandle<
            super::context_host::task_driver::TaskDriverHostJob,
        >,
        context_limit: usize,
    ) -> Result<Option<Self>, WorkerPreparationObservationError> {
        let Some(queue) = preparation
            .try_preparation_snapshot()
            .map_err(|_| WorkerPreparationObservationError::QueuePoisoned)?
        else {
            return Ok(None);
        };
        let Some(charges) = ledger
            .try_capacity_usage(
                [
                    Dimension::PreparingTasks.id(),
                    Dimension::PreparingBytes.id(),
                ],
                Dimension::PreparingTasks.id(),
            )
            .map_err(WorkerPreparationObservationError::Capacity)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            positions: charges.used[0],
            position_limit: charges.limits[0],
            context_positions: charges.max_owner_usage,
            context_position_limit: u64::try_from(context_limit)
                .expect("configured Context limit fits u64"),
            queued_positions: u64::try_from(queue.queued).expect("bounded physical queue fits u64"),
            workers: u64::try_from(queue.running).expect("configured physical workers fit u64"),
            worker_limit: u64::try_from(queue.worker_limit)
                .expect("configured physical worker limit fits u64"),
            bytes: charges.used[1],
            byte_limit: charges.limits[1],
        }))
    }
}

// These types are the same original Common metadata projection. No Worker
// counter, status algorithm, runtime owner or resource permission is copied.
pub use novarocks_control_runtime::driver::{
    PollerObservationSnapshot as WorkerControlPlaneSnapshot,
    PollerPhase as WorkerControlPlanePhase, PollerTimerSnapshot as WorkerControlPlaneTimerSnapshot,
    PollerWorkerSnapshot as WorkerControlPlaneWorkerSnapshot,
};

/// Execution responsibility and physical Root borrows sampled from original
/// owners. Cold directory/channel/core metadata are diagnostic, not busy work.
/// This bounded observation interval is not a cross-owner instant or an exit receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerExecutionQuiescenceSnapshot {
    pub active_tasks: u64,
    pub preparing_tasks: u64,
    pub preparing_bytes: u64,
    pub requests: u64,
    pub contexts_with_execution_custody: u64,
    pub root_producers_running: u64,
    pub root_data_positions: u64,
    pub root_payload_bytes: u64,
    pub root_segments: u64,
    pub root_deliveries: u64,
    pub root_retained_reservations: u64,
    pub root_metadata_holders: u64,
    pub context_records: u64,
    pub root_channels: u64,
    pub root_metadata_bytes: u64,
}
impl WorkerExecutionQuiescenceSnapshot {
    pub(super) fn sample(
        ledger: &RuntimeLedger,
        contexts_with_execution_custody: u64,
        context_records: usize,
        root: crate::root_result_channel::RootOwnershipSnapshot,
    ) -> Option<Self> {
        let charges = ledger
            .try_capacity_usage(
                [
                    Dimension::ActiveTasks.id(),
                    Dimension::PreparingTasks.id(),
                    Dimension::PreparingBytes.id(),
                    Dimension::Requests.id(),
                ],
                Dimension::PreparingTasks.id(),
            )
            .ok()??;
        Some(Self {
            active_tasks: charges.used[0],
            preparing_tasks: charges.used[1],
            preparing_bytes: charges.used[2],
            requests: charges.used[3],
            context_records: u64::try_from(context_records).ok()?,
            contexts_with_execution_custody,
            root_producers_running: u64::try_from(root.producers_running).ok()?,
            root_data_positions: u64::try_from(root.data_positions).ok()?,
            root_payload_bytes: u64::try_from(root.payload_bytes).ok()?,
            root_segments: u64::try_from(root.segments).ok()?,
            root_deliveries: u64::try_from(root.deliveries).ok()?,
            root_retained_reservations: u64::try_from(root.retained_reservations).ok()?,
            root_metadata_holders: u64::try_from(root.metadata_holders).ok()?,
            root_channels: u64::try_from(root.channels).ok()?,
            root_metadata_bytes: u64::try_from(root.metadata_bytes).ok()?,
        })
    }
}

/// Nonblocking original-owner observations, not a coherent instant or exit
/// receipt. Estimates cover only documented inline fields; all indirect backing
/// and allocator overhead remain unmeasured. No estimate is a capacity grant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkerContextRetentionSnapshot {
    pub compact_contexts: usize,
    pub failed_custodians: usize,
    pub decided_task_records: Option<usize>,
    pub metadata_bytes_estimate: Option<usize>,
}
impl WorkerContextRetentionSnapshot {
    pub(super) fn try_add(&mut self, other: Self) -> Option<()> {
        self.compact_contexts = self.compact_contexts.checked_add(other.compact_contexts)?;
        self.failed_custodians = self
            .failed_custodians
            .checked_add(other.failed_custodians)?;
        self.decided_task_records = self
            .decided_task_records
            .zip(other.decided_task_records)
            .and_then(|(a, b)| a.checked_add(b));
        self.metadata_bytes_estimate = self
            .metadata_bytes_estimate
            .zip(other.metadata_bytes_estimate)
            .and_then(|(a, b)| a.checked_add(b));
        Some(())
    }
    pub(super) fn empty() -> Self {
        Self {
            decided_task_records: Some(0),
            metadata_bytes_estimate: Some(0),
            ..Self::default()
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerProtocolRetentionSnapshot {
    pub contexts: WorkerContextRetentionSnapshot,
    pub admission: novarocks_worker_protocol::admission::AdmissionRetentionInventory,
    pub control: novarocks_worker_protocol::control::ControlRetentionInventory,
}
