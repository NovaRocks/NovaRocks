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
//! Exact selected adapters for ONE original constructor and map lookup cores.

//! The selected MAP constructor pairs the original array children by position.

use std::{cell::RefCell, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, ListArray, MapArray, StructArray, UInt32Array, new_empty_array,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use novarocks_type_contract::{
    FunctionValueType, ValueKind, ValueLogicalType, arrow_data_types_exact_observed,
};

use super::{
    collection_arrow::{ArrowSite, arrow_failure},
    collection_selected::{copy_error, reserve},
    map_lookup_core::{MapKeyIdentity, MapKeyOperations},
};
use crate::{
    DataFailureReason, EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy::{CopyIndexView, preflight_original_take},
};

#[derive(Clone, Copy, Debug)]
// Design: ADR-0254 (docs/adr/ADR-0254-collection-calls-and-relational-sequence.md)
pub(super) struct Recipe {
    keys: MapKeyOperations,
}

impl Recipe {
    pub(super) fn prepare(
        contract: &ScalarCallContract,
        mut observe: impl FnMut() -> Result<(), KernelFailure>,
    ) -> Result<Self, KernelFailure> {
        let target = contract.result_type();
        let (_, fields, _) = map_layout(target.data_type())?;
        let key = target
            .value_type()
            .child_type_at(0)
            .ok_or_else(|| invalid("map result lacks its frozen key declaration"))?;
        let logical = match key.kind() {
            ValueKind::Nominal(logical) => *logical,
            _ => ValueLogicalType::Physical,
        };
        let recipe = Self {
            keys: MapKeyOperations::prepare(fields[0].data_type(), logical)
                .ok_or_else(|| invalid("map has no installed identity for its frozen key"))?,
        };
        let arguments = &contract.selected().argument_types;
        if !matches!(arguments.len(), 0 | 2) {
            return Err(invalid("map requires zero or two frozen value arguments"));
        }
        let mut nullable = false;
        for (index, (argument, field)) in arguments.iter().zip(fields).enumerate() {
            observe()?;
            let FunctionArgumentType::Value(source) = argument else {
                return Err(invalid("map requires exactly two value arrays"));
            };
            nullable |= source.nullable();
            if source.data_type() == &DataType::Null {
                if field.data_type() != &DataType::Null || !source.nullable() {
                    return Err(invalid(
                        "map UNKNOWN argument has an incompatible output child",
                    ));
                }
                continue;
            }
            let DataType::List(input) = source.data_type() else {
                return Err(invalid("map requires its exact frozen List arguments"));
            };
            let child = source
                .value_type()
                .child_type_at(0)
                .ok_or_else(|| invalid("map List input lacks its frozen child declaration"))?;
            let expected = target
                .value_type()
                .child_type_at(index)
                .ok_or_else(|| invalid("map output lacks a frozen child declaration"))?;
            if source.logical_type() != ValueLogicalType::Physical
                || !arrow_data_types_exact_observed::<KernelFailure>(
                    field.data_type(),
                    input.data_type(),
                    &mut observe,
                )?
                || !FunctionValueType::borrowed_value_domains_equal_observed(
                    expected,
                    child,
                    &mut observe,
                )?
            {
                return Err(invalid(
                    "map array item differs from the frozen output child",
                ));
            }
        }
        if nullable != target.nullable()
            || (arguments.is_empty() && fields.iter().any(|f| f.data_type() != &DataType::Null))
        {
            return Err(invalid(
                "map result differs from its frozen NULL or empty contract",
            ));
        }
        Ok(recipe)
    }

    pub(super) fn evaluate<'a>(
        self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let work = RefCell::new(EvaluationCheckpoints::new(control));
        let result = (|| {
            let selection = input.selection();
            let target = input.contract().result_type().data_type();
            let (entries, fields, sorted) = map_layout(target)?;
            let rows = selection.len();
            let offset_count = super::collection_arrow::list_offset_entries(rows)?;
            let mut offsets = reserve::<i32>(offset_count, &work)?;
            let mut nulls = NullBufferBuilder::new(rows);
            let mut errors = reserve::<RowDataError>(rows, &work)?;
            let mut offset = 0;
            offsets.push(offset);
            let values = match input.arguments() {
                [] => {
                    for _ in selection.iter() {
                        work.borrow_mut().step()?;
                        offsets.push(0);
                    }
                    fields
                        .iter()
                        .map(|field| new_empty_array(field.data_type()))
                        .collect()
                }
                [keys, values] => {
                    let keys = Source::from_argument(*keys)?;
                    let values = Source::from_argument(*values)?;
                    let mut key_indices = Vec::new();
                    let mut value_indices = Vec::new();
                    let mut seen = hashbrown::HashSet::new();
                    for (ordinal, row) in selection.iter().enumerate() {
                        work.borrow_mut().step()?;
                        match (keys.range(ordinal, row)?, values.range(ordinal, row)?) {
                            (Some(keys_range), Some(values_range)) => {
                                let issue = if keys_range.len() != values_range.len() {
                                    Some("Key and value arrays must be the same length")
                                } else {
                                    seen.clear();
                                    work.borrow_mut().flush()?;
                                    seen.try_reserve(keys_range.len())
                                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                                    self.invalid_keys(
                                        keys.items()?,
                                        keys_range.clone(),
                                        &mut seen,
                                        &work,
                                    )?
                                };
                                if let Some(issue) = issue {
                                    errors.push(RowDataError::with_reason(
                                        ordinal,
                                        DataFailureReason::Domain(
                                            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                                        ),
                                        issue,
                                    ));
                                    nulls.append(false);
                                } else {
                                    offset = crate::aggregate_scalar::checked_map_output_offset(
                                        offset,
                                        keys_range.len(),
                                    )?;
                                    append_indices(&mut key_indices, keys_range, &work)?;
                                    append_indices(&mut value_indices, values_range, &work)?;
                                    nulls.append(true);
                                }
                            }
                            _ => nulls.append(false),
                        }
                        offsets.push(offset);
                    }
                    vec![
                        take_items(&keys, fields[0].data_type(), key_indices, &work)?,
                        take_items(&values, fields[1].data_type(), value_indices, &work)?,
                    ]
                }
                _ => {
                    return Err(internal(
                        "map runtime arguments differ from the prepared call",
                    ));
                }
            };
            work.borrow_mut().flush()?;
            let contents = StructArray::try_new(fields.clone(), values, None)
                .map_err(|error| arrow_failure("map entries", ArrowSite::Construct, error))?;
            let map = MapArray::try_new(
                entries.clone(),
                OffsetBuffer::new(offsets.into()),
                contents,
                nulls.finish(),
                sorted,
            )
            .map_err(|error| arrow_failure("map", ArrowSite::Construct, error))?;
            work.borrow_mut().flush()?;
            SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                target,
                Arc::new(map),
                errors.into_boxed_slice(),
                || work.borrow_mut().step(),
            )
        })();
        work.into_inner().finish_result(result)
    }

    fn invalid_keys<'a>(
        &self,
        keys: &'a ArrayRef,
        positions: Range<usize>,
        seen: &mut hashbrown::HashSet<MapKeyIdentity<'a>>,
        work: &RefCell<EvaluationCheckpoints<'_>>,
    ) -> Result<Option<&'static str>, KernelFailure> {
        for position in positions {
            work.borrow_mut().step()?;
            let Some(key) = self.keys.identity(keys.as_ref(), position)? else {
                return Ok(Some("map key cannot be null"));
            };
            work.borrow_mut().flush()?;
            if !seen.insert(key) {
                return Ok(Some("Duplicate map keys are not allowed"));
            }
            work.borrow_mut().flush()?;
        }
        Ok(None)
    }
}

fn map_layout(target: &DataType) -> Result<(&FieldRef, &Fields, bool), KernelFailure> {
    let DataType::Map(entries, sorted) = target else {
        return Err(invalid("map result must be its frozen Map carrier"));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(invalid(
            "map output entries must use the frozen Struct carrier",
        ));
    };
    if entries.is_nullable() || fields.len() != 2 || fields[0].is_nullable() || *sorted {
        return Err(invalid(
            "map output has an invalid key, entry or order layout",
        ));
    }
    Ok((entries, fields, *sorted))
}

struct Source<'a> {
    arg: EvaluatedArgument<'a>,
    list: Option<&'a ListArray>,
}
impl<'a> Source<'a> {
    fn from_argument(arg: EvaluatedArgument<'a>) -> Result<Self, KernelFailure> {
        let list = if arg.array().data_type() == &DataType::Null {
            None
        } else {
            Some(
                arg.array()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| internal("map argument is outside its prepared List profile"))?,
            )
        };
        Ok(Self { arg, list })
    }

    fn range(&self, ordinal: usize, row: usize) -> Result<Option<Range<usize>>, KernelFailure> {
        let row = self.arg.value_row(ordinal, row);
        if row >= self.arg.array().len() {
            return Err(internal("map selection is outside its prepared source"));
        }
        let Some(list) = self.list.filter(|list| list.is_valid(row)) else {
            return Ok(None);
        };
        let offsets = list.value_offsets();
        Ok(Some(offsets[row] as usize..offsets[row + 1] as usize))
    }

    fn items(&self) -> Result<&'a ArrayRef, KernelFailure> {
        self.list
            .map(ListArray::values)
            .ok_or_else(|| internal("map UNKNOWN input has no reachable child"))
    }
}

fn append_indices(
    indices: &mut Vec<u32>,
    positions: Range<usize>,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<(), KernelFailure> {
    work.borrow_mut().flush()?;
    indices
        .try_reserve(positions.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for position in positions {
        work.borrow_mut().step()?;
        indices.push(
            u32::try_from(position).map_err(|_| internal("map input List offset exceeds u32"))?,
        );
    }
    Ok(())
}

fn take_items(
    source: &Source<'_>,
    carrier: &DataType,
    indices: Vec<u32>,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<ArrayRef, KernelFailure> {
    work.borrow_mut().flush()?;
    if indices.is_empty() {
        return Ok(new_empty_array(carrier));
    }
    let indices = UInt32Array::from(indices);
    let items = source.items()?;
    preflight_original_take(
        items.as_ref(),
        CopyIndexView::UInt32(&indices),
        |boundary| {
            if boundary {
                work.borrow_mut().flush()
            } else {
                work.borrow_mut().step()
            }
        },
    )
    .map_err(copy_error)?;
    work.borrow_mut().flush()?;
    arrow_select::take::take(items.as_ref(), &indices, None)
        .map_err(|error| arrow_failure("map", ArrowSite::Take, error))
}
