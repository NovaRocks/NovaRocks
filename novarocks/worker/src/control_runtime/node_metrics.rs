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
use novarocks_control_runtime::transaction::RuntimeLedger;

/// Each field is independently sampled under the original ledger lock. These
/// are actual charged units, including retained and physically exiting holders.
/// They are neither an instantaneous cross-dimension sum nor exit receipts.
/// Queue/running counts, Root census and limits are absent: no actual read port
/// for those observations is inferred from these charged-resource samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerNodeMetricsSnapshot {
    pub active_tasks: u64,
    pub preparing_tasks: u64,
    pub preparing_bytes: u64,
    pub requests: u64,
    pub tickets: u64,
    pub contexts: u64,
}
impl WorkerNodeMetricsSnapshot {
    pub(super) fn sample(ledger: &RuntimeLedger) -> Self {
        Self {
            active_tasks: ledger.used(Dimension::ActiveTasks.id()),
            preparing_tasks: ledger.used(Dimension::PreparingTasks.id()),
            preparing_bytes: ledger.used(Dimension::PreparingBytes.id()),
            requests: ledger.used(Dimension::Requests.id()),
            tickets: ledger.used(Dimension::Tickets.id()),
            contexts: ledger.used(Dimension::Contexts.id()),
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
