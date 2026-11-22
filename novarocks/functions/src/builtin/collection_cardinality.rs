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

//! Selected CARDINALITY counts root List/Map offsets without inspecting children.
//! Output Layout checks establish representability, not a formal memory grant.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ListArray, MapArray};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

struct CollectionOffsets<'a> {
    offsets: &'a [i32],
    children: usize,
}

fn checked_input<'a>(
    source: &FunctionValueType,
    array: &'a dyn Array,
) -> Result<CollectionOffsets<'a>, KernelFailure> {
    if source.logical_type() != ValueLogicalType::Physical {
        return Err(invalid(
            "cardinality requires its selected Physical collection domain",
        ));
    }
    // The original ScalarCallInput wrapper has already compared full fields
    // through the observed type author. Here only the concrete root class is
    // checked; a second uncontrolled recursive Arrow equality is unnecessary.
    match source.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                internal("cardinality exact List source has a foreign carrier class")
            })?;
            Ok(CollectionOffsets {
                offsets: list.value_offsets(),
                children: list.values().len(),
            })
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                internal("cardinality exact Map source has a foreign carrier class")
            })?;
            Ok(CollectionOffsets {
                offsets: map.value_offsets(),
                children: map.entries().len(),
            })
        }
        _ => Err(invalid(
            "cardinality requires its exact selected List or Map source",
        )),
    }
}

pub(super) fn evaluate_collection_cardinality<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = std::cell::RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "cardinality requires one exact checked value argument",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type() == ValueLogicalType::Physical
            && target.data_type() == &DataType::Int64
            && target.nullable();
        work.borrow_mut().step()?;
        if !exact {
            return Err(invalid(
                "cardinality differs from its exact nullable Int64 result",
            ));
        }
        let collection = checked_input(source, argument.array().as_ref());
        work.borrow_mut().step()?;
        let collection = collection?;
        let selection = input.selection();
        let mut observer = |event| match event {
            super::array_literal_core::CollectionObservation::Step => work.borrow_mut().step(),
            super::array_literal_core::CollectionObservation::OpaqueBoundary => {
                work.borrow_mut().flush()
            }
        };
        let array = super::collection_cardinality_core::count_observed(
            argument.array().as_ref(),
            selection,
            |ordinal, batch_row| {
                let row = argument.value_row(ordinal, batch_row);
                let valid_address = row < argument.array().len();
                work.borrow_mut().step()?;
                if !valid_address {
                    return Err(internal("cardinality selected row is out of bounds"));
                }
                let source_null = argument.array().is_null(row);
                work.borrow_mut().step()?;
                if source_null && !source.nullable() {
                    return Err(internal(
                        "cardinality non-null source contains selected SQL NULL",
                    ));
                }
                if !source_null {
                    let next = row.checked_add(1);
                    let pair = next.and_then(|next| {
                        Some((
                            *collection.offsets.get(row)?,
                            *collection.offsets.get(next)?,
                        ))
                    });
                    work.borrow_mut().step()?;
                    let (start, end) =
                        pair.ok_or_else(|| internal("cardinality selected offsets are missing"))?;
                    let valid = start >= 0 && end >= start && (end as usize) <= collection.children;
                    work.borrow_mut().step()?;
                    if !valid {
                        return Err(internal(
                            "cardinality selected offsets violate the collection backing",
                        ));
                    }
                    // Validated monotone, nonnegative i32 offsets prove the original subtraction is representable.
                }
                Ok(row)
            },
            &mut observer,
        )
        .map_err(|failure| match failure {
            super::collection_offset_count::OffsetCountFailure::Control(cause) => cause,
            super::collection_offset_count::OffsetCountFailure::Operation(cause) => cause,
        })?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            array,
            Box::default(),
            || work.borrow_mut().step(),
        )
    })();
    work.into_inner().finish_result(result)
}

#[cfg(test)]
#[path = "collection_cardinality_tests.rs"]
mod tests;
