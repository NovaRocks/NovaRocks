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

//! Q emits selected target rows as deletions. T owns disjoint key ranges and
//! finalizes verification only after normal Q EOS and the last output handoff.

use super::super::*;
use super::{Host, columns, readable, wait};
use crate::exec::{
    chunk::Chunk,
    operators::logical_change_summary::SummaryUnitObligation,
    pipeline::{
        driver_step::DriverStep,
        operator::{ConstructionFailure, InputConstructionOwner},
        step::{NextStep, StepResult},
    },
    shared_stage::{funded_shared, funded_vec, stage_completion::TailStatus},
};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, new_null_array};
use novarocks_functions::{
    KernelEvaluationControl,
    selected_copy::{
        self, CopyConstructionError, CopyIndices, CopyOperationError, OriginalCopyData,
    },
};
use novarocks_result_contract::SummaryValue;
use novarocks_type_contract::ValueSchemaCreation;

pub(super) struct ProbeCursor {
    pub(super) input: Chunk,
    pub(super) next: usize,
}

pub(super) fn drive_probe(
    host: &Host,
    pending: &mut Option<ProbeCursor>,
    conversions: &mut ValueConversionContext,
    ctx: &mut DriverStep<'_>,
) -> ExecutionResult<StepResult> {
    let frame = &host.prepared()?.frame;
    let observable = frame.completion.observable();
    let generation = observable.generation();
    let Some(published) = frame.completion.published()? else {
        return Ok(wait(observable, generation));
    };
    let cursor = pending
        .as_mut()
        .ok_or_else(|| contract("content probe has no accepted input"))?;
    let rows = (cursor.input.len() - cursor.next)
        .min(host.stages.output_rows.get())
        .min(ctx.work().remaining() as usize);
    if rows == 0 && cursor.next != cursor.input.len() {
        return Ok(StepResult::new(None, NextStep::Yield, 0));
    }
    let output = if rows == 0 {
        None
    } else {
        if !ctx
            .work()
            .try_consume(rows.try_into().expect("bounded by the u32 turn"))
        {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let end = cursor.next + rows;
        let output = probe_page(
            host,
            &published.artifact,
            &cursor.input,
            cursor.next..end,
            conversions,
        )?;
        cursor.next = end;
        output
    };
    let next = if cursor.next == cursor.input.len() {
        *pending = None;
        NextStep::NeedInput
    } else if output.is_none() && ctx.work().remaining() == 0 {
        NextStep::Yield
    } else {
        NextStep::Runnable
    };
    Ok(StepResult::new(output, next, 0))
}

fn probe_page(
    host: &Host,
    index: &ContentApplyIndex,
    input: &Chunk,
    range: Range<usize>,
    conversions: &mut ValueConversionContext,
) -> ExecutionResult<Option<Chunk>> {
    let owner = host.owner()?;
    let work = host.work(owner.as_ref())?;
    let node = host.stages.spec()?;
    let readable = readable(
        ReachableRows::range(input.len(), range.clone()).map_err(ExecutionFailure::from_domain)?,
        &work,
    )?;
    let arrays = columns(input, &node.target_content, &work)?;
    let mut occurrences = funded_vec(work.owner, range.len())?;
    occurrences.extend(range.clone());
    let types = &host.prepared()?.frame.target_types;
    let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
    let keys = KeyInput::new_for_spec(
        index.spec(),
        &arrays,
        types,
        &readable,
        &occurrences,
        conversions,
        work.owner,
        &checkpoint,
    )?;
    let prepared =
        PreparedKeyBatch::from_captured(index.spec(), &keys, work.allocator.clone(), &checkpoint)?;
    let matches = index.probe(&prepared, 0..range.len(), &work)?;
    let position_arrays = columns(
        input,
        &[
            node.target.file,
            node.target.position,
            node.target.spec_id,
            node.target.partition,
        ],
        &work,
    )?;
    let files = position_arrays[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| contract("content target file has an invalid carrier"))?;
    let positions = position_arrays[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| contract("content target position has an invalid carrier"))?;
    let mut selected = funded_vec(work.owner, range.len())?;
    for (row, matched) in range.zip(matches.matches()) {
        checkpoint()?;
        if let BulkMatch::Found(key) = matched {
            if position_arrays.iter().any(|array| array.is_null(row)) {
                return Err(contract("content target position is incomplete"));
            }
            if index.select_position(*key, files.value(row), positions.value(row))? {
                selected.push(u64::try_from(row).map_err(|_| arithmetic())?);
            }
        }
    }
    if selected.is_empty() {
        return Ok(None);
    }
    let rows = selected.len();
    let indices = CopyIndices::UInt64(funded_shared(
        work.owner,
        arrow::array::UInt64Array::from(selected),
    )?);
    let input_storage = input.source_storage_lifetime_observed::<ConstructionFailure>(
        work.owner.storage_lifetime(),
        |layout| work.owner.request(layout),
    )?;
    let schema = &host.prepared()?.schema;
    let output = host.owner()?;
    let mut result = funded_vec(output.as_ref(), schema.slots().len())?;
    for array in arrays.into_iter().chain(position_arrays) {
        let copied = selected_copy::take_copy_in(
            array,
            indices.clone(),
            input_storage.clone(),
            work.host.clone(),
            work.control,
        )
        .map_err(|cause| match cause {
            CopyOperationError::Control(cause) => cause.into(),
            CopyOperationError::Preflight(cause) => cause.into_kernel().into(),
            CopyOperationError::OriginalData(cause) => {
                ExecutionFailure::from_domain(TargetGatherFailure(cause))
            }
        })?;
        result.push(copied.into_values());
    }
    output_chunk(host, output.as_ref(), result, rows, -1).map(Some)
}

#[derive(Debug)]
struct TargetGatherFailure(OriginalCopyData);

impl std::fmt::Display for TargetGatherFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "content apply target gather: {}", self.0)
    }
}

impl std::error::Error for TargetGatherFailure {}

impl novarocks_failure_contract::FailureProjection for TargetGatherFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::JoinGatherArrow.reason(),
        )
    }
}

fn with_creation<T>(
    owner: &dyn InputConstructionOwner,
    action: impl FnOnce(&mut ValueSchemaCreation<'_, ConstructionFailure>) -> ExecutionResult<T>,
) -> ExecutionResult<T> {
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    action(&mut ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    ))
}

fn copy_failure(error: CopyConstructionError<ConstructionFailure>) -> ExecutionFailure {
    match error {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

fn output_chunk(
    host: &Host,
    owner: &dyn InputConstructionOwner,
    mut columns: Vec<ArrayRef>,
    rows: usize,
    change: i8,
) -> ExecutionResult<Chunk> {
    use std::alloc::Layout;
    let schema = &host.prepared()?.schema;
    let effect = schema
        .slots()
        .get(columns.len())
        .ok_or_else(|| contract("missing content effect output"))?;
    owner.request(Layout::array::<i8>(rows).map_err(|_| arithmetic())?)?;
    with_creation(owner, |creation| {
        selected_copy::preflight_flat_array_structure_construction(
            effect.typed_binding().layout(),
            false,
            creation,
            &mut |boundary| {
                host.construction
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .observe_input_work(usize::from(!boundary))
            },
        )
        .map_err(copy_failure)?;
        Ok(())
    })?;
    host.control.checkpoint(0)?;
    columns.push(Arc::new(Int8Array::from(vec![change; rows])));
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(
        schema.clone(),
        columns,
        rows,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )?)
}

fn insertion_chunk(host: &Host, page: ExportedKeys) -> ExecutionResult<Chunk> {
    let owner = host.owner()?;
    let schema = &host.prepared()?.schema;
    let rows = page
        .arrays()
        .first()
        .ok_or_else(|| contract("content insertion omitted its key"))?
        .len();
    let mut columns = funded_vec(owner.as_ref(), schema.slots().len())?;
    columns.extend(page.arrays().iter().cloned());
    // Exported arrays already own their buffers under the original K host.
    for slot in &schema.slots()[columns.len()..schema.slots().len() - 1] {
        let layout = slot.typed_binding().layout();
        with_creation(owner.as_ref(), |creation| {
            selected_copy::preflight_null_construction(layout, rows, creation, &host.control)
                .map_err(copy_failure)?;
            Ok(())
        })?;
        host.control.checkpoint(0)?;
        columns.push(new_null_array(layout.data_type(), rows));
    }
    output_chunk(host, owner.as_ref(), columns, rows, 1)
}

pub(super) fn drive_tail(
    host: &Host,
    unit: &mut Option<SummaryUnitObligation>,
    tail: &mut Option<ContentApplyTail>,
    conversions: &mut ValueConversionContext,
    ctx: &mut DriverStep<'_>,
) -> ExecutionResult<StepResult> {
    let frame = &host.prepared()?.frame;
    let observable = frame.completion.observable();
    let generation = observable.generation();
    let index = match frame.completion.tail_status()? {
        TailStatus::Pending => return Ok(wait(observable, generation)),
        TailStatus::Unavailable => {
            return Err(contract("content verification lacks normal target EOS"));
        }
        TailStatus::Withdrawn => return Ok(StepResult::new(None, NextStep::Finished, 0)),
        TailStatus::Ready(published) => published.artifact,
    };
    if tail.is_none() {
        let slots = host.stages.participants.tails;
        let slot = host.slot.clone()?;
        let keys = index.len();
        let width = keys / slots;
        let extra = keys % slots;
        let start = slot * width + slot.min(extra);
        let end = start + width + usize::from(slot < extra);
        *tail = Some(ContentApplyTail::new(start..end, &index)?);
    }
    let tail = tail.as_mut().expect("installed tail range");
    if let Some(counts) = tail.counts() {
        let unit = unit
            .as_mut()
            .ok_or_else(|| contract("content tail has no summary unit"))?;
        unit.accept(SummaryValue::DeleteVerification(counts))?;
        unit.finish()?;
        return Ok(StepResult::new(None, NextStep::Finished, 0));
    }
    let owner = host.owner()?;
    let work = host.work(owner.as_ref())?;
    let page = tail.next_page(
        &index,
        host.stages.output_rows,
        ctx.work(),
        conversions,
        &work,
        || host.should_drop_insert(),
    )?;
    let output = page.map(|page| insertion_chunk(host, page)).transpose()?;
    // A subsequent admitted poll completes the summary after this output
    // (including the last page) has actually transferred to the handoff.
    let next = if output.is_none() && tail.counts().is_none() && ctx.work().remaining() == 0 {
        NextStep::Yield
    } else {
        NextStep::Runnable
    };
    Ok(StepResult::new(output, next, 0))
}
