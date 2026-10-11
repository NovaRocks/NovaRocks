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

//! One synchronous preparation admission loop, borrowing the original control.
//! Pending retains only its unexecuted continuation, never an active writer.

use super::{
    kernel_memory::{KernelMemoryAdmission, KernelMemoryJournal, request_complete_operation},
    query_memory::QueryMemoryBinding,
};
use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
use novarocks_memory::{CapacityError, CoverageReceipt, ShortageReceipt};
use novarocks_type_contract::CompleteMetadataRequestFacts;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationMemoryStop {
    Cancel(CancelReason),
    Abort(AbortCause),
}
pub trait SynchronousPreparationControl {
    fn checkpoint(&self) -> Result<(), PreparationMemoryStop>;
    fn wait(&self) -> Result<(), PreparationMemoryStop>;
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparationMemoryRefusal {
    Stopped(PreparationMemoryStop),
    MissingQueryMemory,
    SharedShortage(ShortageReceipt),
    Capacity(CapacityError),
}
impl std::fmt::Display for PreparationMemoryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped(stop) => write!(f, "metadata preparation stopped: {stop:?}"),
            Self::MissingQueryMemory => f.write_str("metadata preparation requires query memory"),
            Self::SharedShortage(receipt) => {
                write!(f, "metadata preparation shared shortage: {receipt:?}")
            }
            Self::Capacity(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for PreparationMemoryRefusal {}

#[derive(Debug)]
pub enum SynchronousPreparationFailure<E> {
    Body(E),
    Host(PreparationMemoryRefusal),
}

/// Borrow the caller's fixed journal without changing its public source facts.
pub struct PreparationMemoryJournalLoan<'a> {
    pub workset_bytes: &'a mut Option<usize>,
    pub pending: &'a mut Option<CoverageReceipt>,
    pub shortage: &'a mut Option<ShortageReceipt>,
    pub capacity: &'a mut Option<CapacityError>,
    pub body: &'a mut KernelMemoryJournal,
}
pub struct SynchronousPreparationMemory<'a> {
    binding: Option<&'a QueryMemoryBinding>,
    control: &'a dyn SynchronousPreparationControl,
    journal: PreparationMemoryJournalLoan<'a>,
}
impl<'a> SynchronousPreparationMemory<'a> {
    pub fn new(
        binding: Option<&'a QueryMemoryBinding>,
        control: &'a dyn SynchronousPreparationControl,
        journal: PreparationMemoryJournalLoan<'a>,
    ) -> Self {
        Self {
            binding,
            control,
            journal,
        }
    }
    fn capacity(&mut self, error: CapacityError) -> PreparationMemoryRefusal {
        *self.journal.capacity = Some(error.clone());
        PreparationMemoryRefusal::Capacity(error)
    }
    pub fn materialize<T, E>(
        &mut self,
        facts: CompleteMetadataRequestFacts,
        overflow_detail: &'static str,
        body: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, SynchronousPreparationFailure<E>> {
        self.materialize_with_request(facts, overflow_detail, body, |binding, peak| {
            request_complete_operation(Some(binding), peak)
        })
    }

    /// Exercise the same loop with real bounded qualification in contract tests.
    /// Production uses only the strict complete-operation entry above.
    #[cfg(any(test, feature = "test-support"))]
    pub fn materialize_with_request_for_test<T, E, R>(
        &mut self,
        facts: CompleteMetadataRequestFacts,
        overflow_detail: &'static str,
        body: impl FnOnce() -> Result<T, E>,
        request: R,
    ) -> Result<T, SynchronousPreparationFailure<E>>
    where
        R: FnMut(&QueryMemoryBinding, usize) -> KernelMemoryAdmission,
    {
        self.materialize_with_request(facts, overflow_detail, body, request)
    }

    fn materialize_with_request<T, E, R>(
        &mut self,
        facts: CompleteMetadataRequestFacts,
        overflow_detail: &'static str,
        body: impl FnOnce() -> Result<T, E>,
        mut request: R,
    ) -> Result<T, SynchronousPreparationFailure<E>>
    where
        R: FnMut(&QueryMemoryBinding, usize) -> KernelMemoryAdmission,
    {
        let peak = facts
            .allocation_requests_upper_bound
            .checked_mul(novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES)
            .and_then(|tokens| {
                facts
                    .allocation_request_bytes_upper_bound
                    .checked_add(tokens)
            })
            .ok_or_else(|| {
                SynchronousPreparationFailure::Host(self.capacity(CapacityError::Invalid {
                    detail: overflow_detail,
                }))
            })?;
        *self.journal.workset_bytes = Some(peak);
        loop {
            self.control.checkpoint().map_err(|stop| {
                SynchronousPreparationFailure::Host(PreparationMemoryRefusal::Stopped(stop))
            })?;
            let admission = match self.binding {
                Some(binding) => request(binding, peak),
                None => KernelMemoryAdmission::MissingQueryMemory,
            };
            match admission {
                KernelMemoryAdmission::Granted(ready) => {
                    // A stop may win during admission. Dropping unused Ready
                    // returns its original rights without executing the body.
                    self.control.checkpoint().map_err(|stop| {
                        SynchronousPreparationFailure::Host(PreparationMemoryRefusal::Stopped(stop))
                    })?;
                    let result = ready.run(self.journal.body, body).map_err(|cause| {
                        SynchronousPreparationFailure::Host(self.capacity(cause))
                    })?;
                    match result {
                        Err(original) => return Err(SynchronousPreparationFailure::Body(original)),
                        Ok(value) => {
                            if let Some(cause) = self
                                .journal
                                .body
                                .settlement
                                .as_ref()
                                .and_then(|receipt| receipt.next_step.as_ref().err())
                                .cloned()
                            {
                                return Err(SynchronousPreparationFailure::Host(
                                    self.capacity(cause),
                                ));
                            }
                            return Ok(value);
                        }
                    }
                }
                KernelMemoryAdmission::SettlementPending(receipt) => {
                    *self.journal.pending = Some(receipt);
                    self.control.wait().map_err(|stop| {
                        SynchronousPreparationFailure::Host(PreparationMemoryRefusal::Stopped(stop))
                    })?;
                }
                KernelMemoryAdmission::SharedShortage(receipt) => {
                    *self.journal.shortage = Some(receipt.clone());
                    return Err(SynchronousPreparationFailure::Host(
                        PreparationMemoryRefusal::SharedShortage(receipt),
                    ));
                }
                KernelMemoryAdmission::Refused(cause) => {
                    return Err(SynchronousPreparationFailure::Host(self.capacity(cause)));
                }
                KernelMemoryAdmission::MissingQueryMemory => {
                    return Err(SynchronousPreparationFailure::Host(
                        PreparationMemoryRefusal::MissingQueryMemory,
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "preparation_memory_tests.rs"]
mod tests;
