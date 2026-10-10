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

//! Task-owned funding capability and one synchronous Ready continuation.
//! Observation is not admission. The caller supplies a source-proven workset,
//! stock, threshold and maintenance budget; this module invents none of them.
use crate::runtime::query_memory::QueryMemoryBinding;
use novarocks_memory::attribution::scope::AmbientStepObservation;
use novarocks_memory::{
    CapacityError, CoverageReceipt, FundingDomain, RequestOutcome, ScopeLease, ShortageReceipt,
    StepReceipt, TeardownError,
};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KernelMemoryRequest {
    pub workset_bytes: u64,
    pub stock_bytes: u64,
    pub threshold_bytes: u64,
    pub maintenance_budget: usize,
}

/// Admission precedes the body. Pending and shortage retain their original
/// nominal receipts, and cannot run or replay the supplied continuation.
#[derive(Debug)]
pub enum KernelMemoryAdmission {
    MissingQueryMemory,
    Granted(ReadyKernelMemory),
    SettlementPending(CoverageReceipt),
    SharedShortage(ShortageReceipt),
    Refused(CapacityError),
}

pub fn request(
    binding: Option<&QueryMemoryBinding>,
    request: KernelMemoryRequest,
) -> KernelMemoryAdmission {
    let Some(binding) = binding else {
        return KernelMemoryAdmission::MissingQueryMemory;
    };
    if request.stock_bytes > request.workset_bytes {
        return KernelMemoryAdmission::Refused(CapacityError::Invalid {
            detail: "kernel stock exceeds its explicit source workset",
        });
    }
    match binding.authority().request_domain(
        binding.account(),
        request.workset_bytes,
        request.maintenance_budget,
    ) {
        RequestOutcome::Granted(domain) => KernelMemoryAdmission::Granted(ReadyKernelMemory {
            domain,
            stock_bytes: request.stock_bytes,
            threshold_bytes: request.threshold_bytes,
            stopped: Cell::new(false),
        }),
        RequestOutcome::SettlementPending(receipt) => {
            KernelMemoryAdmission::SettlementPending(receipt)
        }
        RequestOutcome::SharedShortage(receipt) => KernelMemoryAdmission::SharedShortage(receipt),
        RequestOutcome::Refused(cause) => KernelMemoryAdmission::Refused(cause),
    }
}

/// Compose the strict complete-operation policy from the SAME bound authority.
/// The caller supplies a proven full coexistence peak; observation does not
/// consume ScopeLease stock and cannot provide intermediate threshold safety.
/// Its entire admitted workset is available before the body, and its actual
/// next-step receipt is checked only after that bounded synchronous body.
/// Registry lengths bound one sweep, not concurrent busy/stale qualification:
/// the original Pending and SharedShortage outcomes remain nominal.
pub fn request_complete_operation(
    binding: Option<&QueryMemoryBinding>,
    actual_peak: usize,
) -> KernelMemoryAdmission {
    let Some(binding) = binding else {
        return KernelMemoryAdmission::MissingQueryMemory;
    };
    let workset_bytes = match u64::try_from(actual_peak) {
        Ok(bytes) => bytes,
        Err(_) => {
            return KernelMemoryAdmission::Refused(CapacityError::Invalid {
                detail: "complete kernel operation workset exceeds funding width",
            });
        }
    };
    let maintenance_budget = match binding.authority().maintenance_scan_bound() {
        Ok(budget) => budget,
        Err(cause) => return KernelMemoryAdmission::Refused(cause),
    };
    request(
        Some(binding),
        KernelMemoryRequest {
            workset_bytes,
            stock_bytes: workset_bytes,
            threshold_bytes: 0,
            maintenance_budget,
        },
    )
}

/// Caller-owned fixed storage; no allocation, string formatting or fallible
/// callback is needed after a body error. The primary result stays with its
/// original owner; these secondary observations never replace that result.
#[derive(Debug, Default)]
pub struct KernelMemoryJournal {
    pub observation: AmbientStepObservation,
    pub settlement: Option<StepReceipt>,
    pub stopped: Option<Result<(), TeardownError>>,
}

#[derive(Debug)]
pub struct ReadyKernelMemory {
    domain: FundingDomain,
    stock_bytes: u64,
    threshold_bytes: u64,
    stopped: Cell<bool>,
}

struct SettleReady<'a> {
    lease: Option<ScopeLease>,
    domain: &'a FundingDomain,
    journal: &'a mut KernelMemoryJournal,
    stopped: &'a Cell<bool>,
}
impl Drop for SettleReady<'_> {
    fn drop(&mut self) {
        // Complete the same synchronous writer after restoration/flush. This
        // stops this producer, not the query account or surviving allocations.
        self.journal.settlement = Some(self.lease.take().expect("one active writer").finish());
        let stopped = self.domain.stop_producing();
        self.stopped.set(stopped.is_ok());
        self.journal.stopped = Some(stopped);
    }
}
impl ReadyKernelMemory {
    #[cfg(test)]
    pub(crate) fn stock_bytes(&self) -> u64 {
        self.stock_bytes
    }
    #[cfg(test)]
    pub(crate) fn threshold_bytes(&self) -> u64 {
        self.threshold_bytes
    }

    #[cfg(test)]
    pub fn domain(&self) -> &FundingDomain {
        &self.domain
    }

    /// The existing LaneHandle policy is preserved: a failed observation entry
    /// still calls the body under its existing binding. Its exact coverage fact
    /// remains in the journal, and is not forged into a grant or Control cause.
    /// M02b's ONE wrapped allocator publishes tagged facts. Do not also call
    /// ScopeLease::record_allocation for those same physical blocks.
    /// This synchronous body must not return a future as a continuation: the
    /// observation is restored before return, and does not propagate to polls.
    /// Environment allocations below 512 bytes retain the original process-only
    /// policy. Explicit R1 allocations remain their own owner's responsibility.
    pub fn run<T>(
        &self,
        journal: &mut KernelMemoryJournal,
        body: impl FnOnce() -> T,
    ) -> Result<T, CapacityError> {
        let lease = self
            .domain
            .activate(self.stock_bytes, self.threshold_bytes)?;
        *journal = KernelMemoryJournal::default();
        let mut guard = SettleReady {
            lease: Some(lease),
            domain: &self.domain,
            journal,
            stopped: &self.stopped,
        };
        let value = self
            .domain
            .lane()
            .run_observed(&mut guard.journal.observation, body);
        drop(guard);
        Ok(value)
    }
}

impl Drop for ReadyKernelMemory {
    fn drop(&mut self) {
        if !self.stopped.get() {
            // The private Ready owner cannot expose a writer or ExternalBound.
            // Cancellation before run therefore returns unused real rights.
            self.domain
                .stop_producing()
                .expect("private Ready domain has no escaped active writer or external bound");
        }
    }
}
