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

//! Known original input layouts and the allocation-before-effect input gate.
//! These are not a complete action quote or a grant.

use super::*;
use crate::native::apply_send_owner::{OriginalInputLayouts, original_input_layouts};
use crate::native::transport_supervisor::NativeTransportQueuePermit;
use novarocks_workload_control::WorkError;
use std::{alloc::Layout, num::NonZeroUsize};

type Unencodable = (TaskOperationId, OperationKind, bool, String);

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeApplyKnownInputLayouts {
    pub(crate) original: OriginalInputLayouts,
    pub(crate) operation_slots: Layout,
    pub(crate) method_runs: Layout,
    pub(crate) receipts: Layout,
    pub(crate) unencodable_slots: Layout,
    pub(crate) queue_pointers: Layout,
    pub(crate) queue_payloads: Layout,
    pub(crate) one_run_operations: Layout,
    pub(crate) one_control_run: Layout,
    pub(crate) reply_ids: Layout,
    pub(crate) reply_headers: Layout,
    pub(crate) establishes: Layout,
}

/// Uses the same factory item as commit; no dummy F, request or captures are
/// constructed. Nested request/reply pointees and error strings are separate.
pub(crate) fn known_input_layouts_for_composition(
    items: NonZeroUsize,
    budget: TransportBudget,
) -> Result<NativeApplyKnownInputLayouts, WorkError> {
    let items = items.get();
    if items > budget.max_batch_items() {
        return Err(WorkError::Capacity("Native Apply batch items"));
    }
    fn original<F: Future<Output = ()>>(
        _factory: fn(ApplySend, Vec<EncodedMethodBatch>, tokio::time::Instant) -> F,
    ) -> Result<OriginalInputLayouts, WorkError> {
        original_input_layouts::<F>()
    }
    fn array<T>(items: usize) -> Result<Layout, WorkError> {
        Layout::array::<T>(items).map_err(|_| WorkError::ArithmeticOverflow)
    }
    Ok(NativeApplyKnownInputLayouts {
        original: original(apply_operations)?,
        operation_slots: array::<(bool, proto::TaskOperation)>(items)?,
        // The positive Native boundary accepts one exact method/deadline run.
        // encode_method_batches allocates its method container with runs=1.
        method_runs: array::<EncodedMethodBatch>(1)?,
        receipts: array::<SentOperation>(items)?,
        unencodable_slots: array::<Unencodable>(items)?,
        queue_pointers: array::<Box<dyn crate::task_execution::intent::TaskOperationQueuePermit>>(
            items,
        )?,
        queue_payloads: array::<NativeTransportQueuePermit>(items)?,
        one_run_operations: array::<proto::TaskOperation>(items)?,
        one_control_run: array::<proto::TaskControlOperation>(items)?,
        reply_ids: array::<TaskOperationId>(items)?,
        reply_headers: array::<ReceiptHeader>(items)?,
        establishes: array::<(TaskOperationId, QueryContextRef)>(items)?,
    })
}

pub(super) struct NativeApplyInputPlan {
    items: NonZeroUsize,
}
impl NativeApplyInputPlan {
    pub(super) fn from_operations(
        operations: &[OperationIntent],
        budget: TransportBudget,
    ) -> Result<Self, &'static str> {
        let items = NonZeroUsize::new(operations.len()).ok_or("empty operation batch")?;
        if items.get() > budget.max_batch_items() {
            return Err("operation count exceeds batch bound");
        }
        let mut queued_bytes = 0usize;
        for operation in operations {
            queued_bytes = queued_bytes
                .checked_add(operation.queued_bytes())
                .ok_or("operation carrier byte sum overflow")?;
        }
        // The original dispatcher uses the same carrier bound when forming a
        // batch. Recheck it here before any encoded DTO or task is created.
        if queued_bytes > budget.max_batch_encoded_bytes() {
            return Err("operation carriers exceed batch byte bound");
        }
        let _: super::NativeApplyKnownInputLayouts =
            super::known_input_layouts_for_composition(items, budget)
                .map_err(|_| "operation input layout exceeds addressable backing")?;
        Ok(Self { items })
    }
    pub(super) fn items(&self) -> usize {
        self.items.get()
    }
}
