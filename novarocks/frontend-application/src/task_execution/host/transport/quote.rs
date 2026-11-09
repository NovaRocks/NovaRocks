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

//! Allocation-free invoice of the same transport factory and codec inputs.
use super::{
    queue_input::{OriginalQueuedInput, SendInputLoan},
    send,
};
use crate::{
    native::task_transport::{
        clone_input_backing, original_run_encoding, wire::EncodedMethodBatch,
    },
    task_execution::intent::{OperationAcknowledgement, OperationIntent},
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::{TransportBudget, operation::ReceiptHeader};
use novarocks_workload_control::{WorkError, joint_admission::NativeAsyncActionQuote};
use std::{alloc::Layout, num::NonZeroU64};

/// P10a supplies layouts from its actual credentialed carrier/Source factories.
/// These are original allocation geometry, never a replacement capacity grant.
/// No default is provided: a caller must account for its actual wrappers and
/// original canonical ACK/progress/return cells, once each. Standard kernel
/// factories below already invoice their owned wait/RAII pointees; do not supply
/// them again. Custom `*_for_factory` callers supply their actual additional
/// pointees here. An outer F's inline Layout does not describe Box/Arc payloads.
pub(crate) struct SourceLayouts<'a> {
    pub(crate) original: &'a [Layout],
}
fn add(sum: &mut usize, value: usize) -> Result<(), WorkError> {
    *sum = sum
        .checked_add(value)
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok(())
}
fn array<T>(count: usize) -> Result<Layout, WorkError> {
    Layout::array::<T>(count).map_err(|_| WorkError::ArithmeticOverflow)
}
fn nz(value: usize) -> Result<NonZeroU64, WorkError> {
    NonZeroU64::new(u64::try_from(value).map_err(|_| WorkError::ArithmeticOverflow)?)
        .ok_or(WorkError::Conflict)
}
pub(crate) fn original_run(
    operations: &[OperationIntent],
    budget: TransportBudget,
    source: SourceLayouts<'_>,
) -> Result<
    (
        NativeAsyncActionQuote,
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    ),
    WorkError,
> {
    let layouts = send::backing_layouts();
    let preparation = layouts[0]
        .size()
        .checked_add(super::prepare::PrepareSend::owned_wait_bytes()?)
        .ok_or(WorkError::ArithmeticOverflow)?;
    let body = layout_sum(&[layouts[1], layouts[3]])?;
    let transition = body
        .checked_add(layouts[2].size())
        .ok_or(WorkError::ArithmeticOverflow)?;
    let encoded = body
        .checked_add(layouts[4].size())
        .ok_or(WorkError::ArithmeticOverflow)?;
    // destroy_pre releases P and its waits before GatedBody exists. The tuple
    // coexists with body creation; the encode F is allocated only after that
    // tuple has exited. Only the greatest supported phase is charged here.
    // ReservedNativeDial can move from preparation into the actual body. Keep
    // this original Native envelope across both phases, once. Its two notified
    // allocations also cover the exclusive warm generation/stream wait pair.
    let native =
        layout_sum(&crate::native::transport::NativeChannelSource::owned_source_layouts())?;
    let owned_peak = native
        .checked_add(preparation.max(transition).max(encoded))
        .ok_or(WorkError::ArithmeticOverflow)?;
    original_run_inventory(
        operations,
        budget,
        send::future_layout(),
        source,
        owned_peak,
    )
}

/// The same invoice for an original caller whose actual carrier factory is
/// different. That author supplies its own additional pointees in `source`;
/// the standard send's unrelated GatedWork/wait allocations are not added.
/// Geometry remains an observation; it issues no Work or capacity.
pub(crate) fn original_run_for_factory(
    operations: &[OperationIntent],
    budget: TransportBudget,
    factory: Layout,
    source: SourceLayouts<'_>,
) -> Result<
    (
        NativeAsyncActionQuote,
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    ),
    WorkError,
> {
    original_run_inventory(operations, budget, factory, source, 0)
}

fn layout_sum(layouts: &[Layout]) -> Result<usize, WorkError> {
    layouts.iter().try_fold(0usize, |sum, layout| {
        sum.checked_add(layout.size())
            .ok_or(WorkError::ArithmeticOverflow)
    })
}
fn original_run_inventory(
    operations: &[OperationIntent],
    budget: TransportBudget,
    factory: Layout,
    source: SourceLayouts<'_>,
    owned_peak: usize,
) -> Result<
    (
        NativeAsyncActionQuote,
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    ),
    WorkError,
> {
    let first = operations.first().ok_or(WorkError::Conflict)?;
    if operations.len() > budget.max_batch_items()
        || operations.iter().any(|op| {
            op.native_run_key() != first.native_run_key()
                || op.backend_process_id() != first.backend_process_id()
                || op.requires_control_progress() != first.requires_control_progress()
        })
    {
        return Err(WorkError::Conflict);
    }
    let encoding = original_run_encoding(operations, budget)?;
    let mut metadata = 0;
    for layout in [
        factory,
        array::<(bool, proto::TaskOperation)>(operations.len())?,
        array::<EncodedMethodBatch>(1)?,
        array::<super::settlement::PendingReceipt>(operations.len())?,
        array::<super::queue_input::SendKey>(operations.len())?,
        array::<SendInputLoan>(operations.len())?,
        array::<proto::TaskOperation>(operations.len())?,
        array::<proto::TaskControlOperation>(operations.len())?,
        array::<TaskOperationId>(operations.len())?,
        array::<ReceiptHeader>(operations.len())?,
        array::<(TaskOperationId, ParticipationRef)>(operations.len())?,
    ] {
        add(&mut metadata, layout.size())?;
    }
    add(&mut metadata, owned_peak)?;
    for layout in source.original {
        add(&mut metadata, layout.size())?;
    }
    let (tail, frame, body) =
        novarocks_native_adapter::native_lane::native_request_custody_layouts()
            .map_err(|_| WorkError::ArithmeticOverflow)?;
    for layout in [
        tail,
        frame,
        body,
        novarocks_native_adapter::native_codec::original_operation_encoder_layout(),
    ] {
        add(&mut metadata, layout.size())?;
    }
    add(
        &mut metadata,
        novarocks_workload_control::joint_admission::NativeDispatchSource::metadata_backing_bytes(
            operations.len(),
        )?,
    )?;
    let cloned = clone_input_backing(operations)?
        .checked_add(encoding.retained_bytes())
        .ok_or(WorkError::ArithmeticOverflow)?;
    let replies = array::<std::sync::OnceLock<OperationAcknowledgement>>(operations.len())?.size();
    let plan =
        novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_transport(budget);
    let reply = novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
        .checked_add(plan.peak_bytes())
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok((
        NativeAsyncActionQuote::for_original_native_transport(
            nz(operations.len())?,
            nz(metadata)?,
            nz(cloned.max(1))?,
            nz(encoding.protobuf_bytes())?,
            nz(replies)?,
            nz(reply)?,
        ),
        encoding,
    ))
}

/// Invoice the actual queued-input record and caller's exact original Source
/// pointees. Payload bytes must be the canonical original allocation invoice,
/// never a descriptor WorkId or a protobuf encoded-length approximation.
/// `new_payload_backing` includes the actual first creation owner headers only
/// before its original retention is established. Every later exact replay
/// passes zero here, including those creation-owner headers; this path never
/// calls legacy queued_creation_quote(0), whose headers still charge payload.
pub(crate) fn queued_input(
    new_payload_backing: usize,
    source: SourceLayouts<'_>,
) -> Result<novarocks_workload_control::joint_admission::NativeQueuedInputQuote, WorkError> {
    let layouts = super::queued_job::owned_preparation_layouts();
    let preparation = layout_sum(&[layouts[0], layouts[4]])?;
    let body = layout_sum(&[layouts[1], layouts[2], layouts[3]])?;
    queued_input_inventory(
        new_payload_backing,
        super::queued_job::future_layout(),
        source,
        preparation.max(body),
    )
}

/// The same queue invoice for a Source with a different actual outer carrier.
/// Its real additional body/capture/Source pointees still belong in `source`;
/// no default issuer or synthetic request is constructed to observe geometry.
pub(crate) fn queued_input_for_factory(
    new_payload_backing: usize,
    factory: Layout,
    source: SourceLayouts<'_>,
) -> Result<novarocks_workload_control::joint_admission::NativeQueuedInputQuote, WorkError> {
    queued_input_inventory(new_payload_backing, factory, source, 0)
}
fn queued_input_inventory(
    new_payload_backing: usize,
    factory: Layout,
    source: SourceLayouts<'_>,
    owned_peak: usize,
) -> Result<novarocks_workload_control::joint_admission::NativeQueuedInputQuote, WorkError> {
    let mut metadata = Layout::new::<OriginalQueuedInput>().size();
    add(&mut metadata, factory.size())?;
    add(&mut metadata, owned_peak)?;
    for layout in source.original {
        add(&mut metadata, layout.size())?;
    }
    Ok(novarocks_workload_control::joint_admission::NativeQueuedInputQuote::for_original_native_input(
        nz(metadata)?,
        u64::try_from(new_payload_backing).map_err(|_| WorkError::ArithmeticOverflow)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::joint_admission::JointResource;

    #[test]
    fn retained_creation_replay_quotes_only_the_new_original_queue_metadata() {
        // This is the actual canonical Source record layout invoice, separate
        // from the original creation backing retained by its first paid pin.
        let source_layouts = [Layout::new::<super::super::queued_job::BoundQueue>()];
        let first = queued_input(
            8192,
            SourceLayouts {
                original: &source_layouts,
            },
        )
        .unwrap()
        .custody();
        let replay = queued_input(
            0,
            SourceLayouts {
                original: &source_layouts,
            },
        )
        .unwrap()
        .custody();
        assert_eq!(first.get(JointResource::PayloadBytes), 8192);
        assert_eq!(
            replay.get(JointResource::PayloadBytes),
            0,
            "retention covers both original creation backing and its owner headers"
        );
        assert_eq!(
            first.get(JointResource::MetadataBytes),
            replay.get(JointResource::MetadataBytes)
        );
        assert_eq!(first.get(JointResource::QueueRecords), 1);
        assert_eq!(
            replay.get(JointResource::QueueRecords),
            1,
            "each exact replay still needs a fresh original queue grant"
        );
    }
}
