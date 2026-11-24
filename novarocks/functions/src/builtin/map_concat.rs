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

//! The installed TRUNCATE bindings, effects and preparation share each exact owner.

//! Merge already coerced, non-NULL MAP values in the selected domain.
use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, MapArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use novarocks_type_contract::KeyComparison;
use novarocks_value_arrays::{
    ValueFailure,
    hash_eq::{BoundKeyArray, KeyRows, PreparedKeyOperations},
};

use super::collection_arrow::{ArrowSite, arrow_failure};
use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::invalid,
    kernel_input::EvaluationCheckpoints,
    selected_copy::{self, ExtendSegment},
};

pub(super) fn evaluate<'a>(
    key: &PreparedKeyOperations,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    let selection = input.selection();
    let target = input.contract().result_type().data_type();
    let DataType::Map(entries, false) = target else {
        return Err(invalid("map_concat needs its frozen MAP result"));
    };
    if input.arguments().len() < 2 {
        return Err(invalid("map_concat kernel requires at least two arguments"));
    }
    let mut maps = Vec::new();
    let mut keys = Vec::new();
    for argument in input.arguments() {
        work.step()?;
        let map = argument
            .array()
            .as_any()
            .downcast_ref::<MapArray>()
            .filter(|map| map.data_type() == target)
            .ok_or_else(|| invalid("map_concat input differs from the coerced MAP carrier"))?;
        maps.push(map);
        keys.push(
            key.bind_observed(map.keys(), KeyRows::All, &mut |units| {
                steps(&mut work, units)
            })
            .map_err(value_failure)?,
        );
    }
    let offset_count = super::collection_arrow::list_offset_entries(selection.len())?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    offsets.push(0);
    let mut winners = Vec::new();
    let mut candidates = HashMap::<u64, Vec<usize>>::new();

    for (ordinal, batch_row) in selection.iter().enumerate() {
        candidates.clear();
        for (source, (map, argument)) in maps.iter().zip(input.arguments()).enumerate() {
            work.step()?;
            let row = argument.value_row(ordinal, batch_row);
            if map.is_null(row) {
                return Err(invalid(
                    "map_concat ordered NULL controller passed a NULL argument",
                ));
            }
            let source_offsets = map.value_offsets();
            let bounds = source_offsets[row] as usize..source_offsets[row + 1] as usize;
            for entry in bounds {
                work.step()?;
                let hash = key
                    .hash_observed(&keys[source], entry, &mut |units| steps(&mut work, units))
                    .map_err(value_failure)?;
                candidates
                    .try_reserve(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let indexed = candidates.entry(hash).or_default();
                let position = matched(key, &keys, &winners, indexed, source, entry, &mut work)?;
                if let Some(position) = position {
                    winners[position] = (source, entry);
                } else {
                    indexed
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    winners
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    indexed.push(winners.len());
                    winners.push((source, entry));
                }
            }
        }
        offsets.push(crate::aggregate_scalar::checked_map_output_offset(
            0,
            winners.len(),
        )?);
    }

    let sources: Vec<&dyn Array> = maps.iter().map(|map| map.entries() as &dyn Array).collect();
    let segments: Vec<_> = winners
        .iter()
        .map(|&(source, start)| ExtendSegment {
            source,
            start,
            len: 1,
            repeats: 1,
            nulls: 0,
        })
        .collect();
    selected_copy::preflight_extend_multi(&sources, &segments, winners.len(), |boundary| {
        if boundary { work.flush() } else { work.step() }
    })
    .map_err(selected_copy::CopyError::into_kernel)?;
    work.flush()?;
    let data: Vec<_> = sources.iter().map(|source| source.to_data()).collect();
    let mut output = MutableArrayData::new(data.iter().collect(), false, winners.len());
    for (source, entry) in winners {
        work.step()?;
        output
            .try_extend(source, entry, entry + 1)
            .map_err(|source| arrow_failure("map_concat", ArrowSite::Extend, source))?;
    }
    work.flush()?;
    let output = MapArray::try_new(
        Arc::clone(entries),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        StructArray::from(output.freeze()),
        None,
        false,
    )
    .map_err(|source| arrow_failure("map_concat", ArrowSite::Construct, source))?;
    let result: ArrayRef = Arc::new(output);
    let output = SelectedValues::try_new_observed::<KernelFailure>(
        selection,
        target,
        result,
        Box::default(),
        || work.step(),
    )?;
    work.flush()?;
    Ok(output)
}

fn matched(
    key: &PreparedKeyOperations,
    arrays: &[BoundKeyArray<'_>],
    winners: &[(usize, usize)],
    indexed: &[usize],
    source: usize,
    entry: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    for &position in indexed {
        let (left, left_entry) = winners[position];
        match key
            .compare_observed(
                &arrays[left],
                left_entry,
                &arrays[source],
                entry,
                &mut |_| Ok(()),
                &mut |units| steps(work, units),
            )
            .map_err(value_failure)?
        {
            KeyComparison::True => return Ok(Some(position)),
            KeyComparison::False => {}
            _ => return Err(invalid("map_concat key identity must be total")),
        }
    }
    Ok(None)
}

fn steps(work: &mut EvaluationCheckpoints<'_>, units: usize) -> Result<(), KernelFailure> {
    for _ in 0..units {
        work.step()?;
    }
    Ok(())
}

fn value_failure(error: ValueFailure<KernelFailure>) -> KernelFailure {
    match error {
        ValueFailure::Value(error) => KernelFailure::from_domain(error),
        ValueFailure::Caller(error) => error,
    }
}
