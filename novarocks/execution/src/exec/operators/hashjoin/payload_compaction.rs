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

//! Long-lived payload pages reuse the original selected-copy and V compactor.
//! Original input stays borrowed across the real old-plus-new allocation peak.
use super::{
    build_pages::check,
    join_completion::{arithmetic, funded_vec},
    typed_gather,
};
use crate::{
    exec::{
        chunk::{Chunk, ChunkBatchProjection},
        hash_table::key_index::BulkWork,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{
    array::{Array, ArrayData, BinaryViewArray, StringViewArray, make_array},
    buffer::Buffer,
    datatypes::DataType,
};
use novarocks_value_arrays::{ValueFailure, compact::CompactPlan};
use std::alloc::Layout;

fn original_value(cause: ValueFailure<ExecutionFailure>) -> ExecutionFailure {
    match cause {
        ValueFailure::Caller(cause) => cause,
        ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
    }
}
fn reserve_metadata<T>(count: usize, work: &BulkWork<'_>) -> ExecutionResult<Vec<T>> {
    check(work)?;
    // This table is covered by the original whole-graph metadata reservation.
    let layout = Layout::array::<T>(count).map_err(|_| arithmetic())?;
    let mut table = Vec::new();
    table.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
            requested: layout.size(),
            source,
        })
    })?;
    Ok(table)
}
fn compact_tree(data: &ArrayData, work: &BulkWork<'_>) -> ExecutionResult<ArrayData> {
    check(work)?;
    if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        let source = make_array(data.clone());
        let plan = CompactPlan::plan_observed(
            &source,
            None,
            &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| check(work),
        )
        .map_err(original_value)?;
        let original_regions = match source.data_type() {
            DataType::Utf8View => source
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("original Arrow view constructor")
                .data_buffers()
                .len(),
            DataType::BinaryView => source
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .expect("original Arrow view constructor")
                .data_buffers()
                .len(),
            _ => unreachable!(),
        };
        // The original graph's four metadata envelopes cover reboxing. Only
        // the actual additional output-region headers need extra coverage.
        let additional = plan.facts().output_regions.saturating_sub(original_regions);
        if additional > 0 {
            let bytes = Layout::array::<Buffer>(additional)
                .map_err(|_| arithmetic())?
                .size()
                .checked_mul(4)
                .ok_or_else(arithmetic)?;
            work.owner.observe_opaque_upper_bound(bytes)?;
        }
        let copied = plan
            .execute_observed(
                &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| check(work),
            )
            .map_err(original_value)?;
        check(work)?;
        return Ok(copied.array.to_data());
    }
    let mut children = reserve_metadata(data.child_data().len(), work)?;
    for child in data.child_data() {
        children.push(compact_tree(child, work)?);
    }
    check(work)?;
    // Original datatype, offsets, lengths, validity and child order stay exact.
    // The original V author alone changes view descriptors and their backing.
    let rebuilt = data
        .clone()
        .into_builder()
        .child_data(children)
        .build()
        .map_err(typed_gather::gather_kernel)?;
    check(work)?;
    Ok(rebuilt)
}
struct PayloadCreator {
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
pub(super) fn retain_payload(
    source: &Chunk,
    work: &BulkWork<'_>,
) -> ExecutionResult<ChunkBatchProjection> {
    check(work)?;
    if !source.chunk_schema().is_typed() {
        return Err(super::build_state::build_contract(
            "retained join payload has no original signed schema",
        ));
    }
    let schema = source.chunk_schema_ref();
    let storage = work
        .owner
        .storage_lifetime()
        .retaining_observed(&schema.storage_lifetime(), |layout| {
            work.owner.request(layout)
        })?;
    let mut source_work = novarocks_functions::EvaluationCheckpoints::new(work.control);
    let creator = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
        PayloadCreator {
            _storage: storage.clone(),
        },
        work.host.clone(),
        &mut source_work,
    )?;
    source_work.finish()?;
    let mut choices = funded_vec(work.owner, source.len())?;
    for row in 0..source.len() {
        work.control.checkpoint(1)?;
        choices.push((0, row));
    }
    let mut columns = funded_vec(work.owner, schema.slots().len())?;
    for (array, slot) in source.columns().iter().zip(schema.slots()) {
        check(work)?;
        let dense = typed_gather::copy_payload_column(
            std::slice::from_ref(array),
            &choices,
            slot,
            work.owner,
            work.control,
        )
        .map_err(typed_gather::JoinGatherError::into_execution)?;
        let metadata =
            novarocks_functions::source_metadata_bytes_observed(dense.as_ref(), &mut |_| {
                check(work)
            })?;
        work.owner.observe_opaque_upper_bound(metadata)?;
        let original = dense.to_data();
        let data = compact_tree(&original, work)?;
        let copied = make_array(data);
        columns.push(novarocks_functions::retain_source_backing(
            copied,
            &creator,
            work.control,
        )?);
    }
    let output = Chunk::try_new_typed_with_columns_owned_observed::<ExecutionFailure, _>(
        schema,
        columns,
        source.len(),
        storage,
        |step| work.owner.observe(0, &[], step).map_err(Into::into),
    )?;
    check(work)?;
    Ok(output.into_batch_projection())
}

#[cfg(test)]
#[path = "payload_compaction_tests.rs"]
mod tests;
