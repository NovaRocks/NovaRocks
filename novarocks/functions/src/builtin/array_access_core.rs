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

//! ONE original array access normalizer, index algorithm and UTF8 projection.
use super::{
    array_literal_core::CollectionObservation,
    collection_arrow::{ArrowSite, arrow_failure, cast_output, contract},
    map_lookup_core::row_index,
};
use crate::{KernelFailure, RowDataError};
use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StringArray, UInt32Array,
};
use arrow_schema::DataType;
use arrow_select::take::take;
#[derive(Debug)]
pub enum AccessFailure<E> {
    Operation(KernelFailure),
    Control(E),
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), AccessFailure<E>> {
    observer(event).map_err(AccessFailure::Control)
}
pub struct ArrayAccessInputs {
    array: ArrayRef,
    subscript: ArrayRef,
    check: Option<ArrayRef>,
    strict: bool,
}
impl ArrayAccessInputs {
    pub fn new<E>(
        array: ArrayRef,
        mut subscript: ArrayRef,
        check: Option<ArrayRef>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<Self, AccessFailure<E>> {
        array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            AccessFailure::Operation(contract(format_args!(
                "element_at expects ListArray, got {:?}",
                array.data_type()
            )))
        })?;
        if subscript.data_type() != &DataType::Int32 {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            // The ORIGINAL common::cast_with_special_rules always reaches Arrow's
            // default cast for target Int32. No date/decimal/VARCHAR branch applies.
            let normalized = cast_output(
                subscript.clone(),
                Some(&DataType::Int32),
                "element_at subscript",
            )
            .map_err(AccessFailure::Operation)?;
            subscript = normalized;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
        }
        subscript
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                AccessFailure::Operation(contract("element_at expects INT subscript"))
            })?;
        if let Some(flags) = &check {
            flags
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    AccessFailure::Operation(contract("element_at check flag must be BOOLEAN"))
                })?;
        }
        Ok(Self {
            array,
            subscript,
            check,
            strict: false,
        })
    }
    pub(super) fn strict(array: ArrayRef, subscript: ArrayRef) -> Result<Self, KernelFailure> {
        if !array.as_any().is::<ListArray>() || !subscript.as_any().is::<Int64Array>() {
            return Err(contract(
                "array subscript requires its exact ARRAY and BIGINT carriers",
            ));
        }
        Ok(Self {
            array,
            subscript,
            check: None,
            strict: true,
        })
    }

    fn index_value(&self, row: usize) -> i64 {
        if self.strict {
            self.subscript
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("strict BIGINT admission")
                .value(row)
        } else {
            i64::from(self.subscript().value(row))
        }
    }

    pub fn list(&self) -> &ListArray {
        self.array
            .as_any()
            .downcast_ref()
            .expect("original List admission")
    }
    fn subscript(&self) -> &Int32Array {
        self.subscript
            .as_any()
            .downcast_ref()
            .expect("original INT admission")
    }
    fn flags(&self) -> Option<&BooleanArray> {
        self.check.as_ref().map(|a| {
            a.as_any()
                .downcast_ref()
                .expect("original BOOLEAN admission")
        })
    }
    pub fn validate_direct_lengths(&self) -> Result<(), KernelFailure> {
        let list = self.list();
        let subscript_arr = self.subscript();
        let check_arr = self.flags();
        if subscript_arr.len() != 1 && subscript_arr.len() != list.len() {
            return Err(contract(format_args!(
                "element_at subscript length mismatch: list rows={}, subscript rows={}",
                list.len(),
                subscript_arr.len()
            )));
        }
        if let Some(flags) = check_arr
            && flags.len() != 1
            && flags.len() != list.len()
        {
            return Err(contract(format_args!(
                "element_at check flag length mismatch: list rows={}, check rows={}",
                list.len(),
                flags.len()
            )));
        }
        Ok(())
    }
    pub fn direct_rows(&self, row: usize) -> AccessRows {
        AccessRows {
            list: row,
            subscript: row_index(row, self.subscript.len()),
            check: self.check.as_ref().map(|f| row_index(row, f.len())),
        }
    }
}
#[derive(Clone, Copy)]
pub struct AccessRows {
    pub list: usize,
    pub subscript: usize,
    pub check: Option<usize>,
}
pub fn lookup_observed<E>(
    inputs: &ArrayAccessInputs,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<AccessRows, E>,
    output_type: Option<&DataType>,
    mut before_take: impl FnMut(&dyn Array, &UInt32Array) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<(ArrayRef, Box<[RowDataError]>), AccessFailure<E>> {
    let list = inputs.list();
    let subscript_arr = &inputs.subscript;
    let check_arr = inputs.flags();
    let offsets = list.value_offsets();
    let values = list.values();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    super::string_failure::array_layout::<Option<u32>>(selection.len())
        .map_err(AccessFailure::Operation)?;
    let mut indices = Vec::new();
    let mut row_errors = Vec::new();
    indices
        .try_reserve_exact(selection.len())
        .map_err(|source| {
            AccessFailure::Operation(super::string_failure::reservation::<Option<u32>>(
                selection.len(),
                source,
            ))
        })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let rows = rows(ordinal, row).map_err(AccessFailure::Control)?;
        if rows.list >= list.len() || rows.subscript >= subscript_arr.len() {
            return Err(AccessFailure::Operation(contract(
                "array access row mapping is outside its physical carriers",
            )));
        }
        match (check_arr, rows.check) {
            (None, None) => {}
            (Some(flags), Some(index)) if index < flags.len() => {}
            _ => {
                return Err(AccessFailure::Operation(contract(
                    "array access check address differs from its prepared flag",
                )));
            }
        }
        let row = rows.list;
        let subscript_idx = rows.subscript;
        let check_idx = rows.check;
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);
        if list.is_null(row) || subscript_arr.is_null(subscript_idx) {
            indices.push(None);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let row_len = end - start;
        let subscript = inputs.index_value(subscript_idx);
        let undefined = if subscript <= 0 {
            Some(AccessCause::BelowOne { ordinal, subscript })
        } else if subscript as u64 > row_len as u64 {
            Some(AccessCause::AboveLength {
                ordinal,
                subscript,
                len: row_len,
            })
        } else {
            None
        };
        if let Some(cause) = undefined {
            if inputs.strict {
                row_errors
                    .try_reserve(1)
                    .map_err(|_| AccessFailure::Operation(KernelFailure::ResourceExhausted))?;
                row_errors.push(
                    RowDataError::from_domain(ordinal, StrictSubscriptCause(cause))
                        .map_err(AccessFailure::Operation)?,
                );
            } else if check_out_of_bounds {
                return Err(AccessFailure::Operation(KernelFailure::from_domain(cause)));
            }
            indices.push(None);
            continue;
        }
        // The checked List offsets and row length bound the positive BIGINT.
        let subscript = subscript as usize;
        let target = start + subscript - 1;
        let target = u32::try_from(target).map_err(|source| {
            AccessFailure::Operation(KernelFailure::from_domain(AccessCause::Conversion {
                operation: "element_at index exceeds UInt32 range",
                source,
            }))
        })?;
        indices.push(Some(target));
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let indices = UInt32Array::from(indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = if values.is_empty() {
        // No source value can be addressed. A NULL result of the known value
        // type does not require a dummy Arrow take at physical index zero.
        arrow_array::new_null_array(values.data_type(), selection.len())
    } else {
        before_take(values.as_ref(), &indices).map_err(AccessFailure::Control)?;
        take(values.as_ref(), &indices, None).map_err(|source| {
            AccessFailure::Operation(arrow_failure("element_at", ArrowSite::Take, source))
        })?
    };
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = if inputs.strict {
        out
    } else {
        let projected = maybe_unquote_wrapped_utf8_observed(out, observer)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        cast_output(projected, output_type, "element_at").map_err(AccessFailure::Operation)?
    };
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok((out, row_errors.into_boxed_slice()))
}
pub fn maybe_unquote_wrapped_utf8_observed<E>(
    array: ArrayRef,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AccessFailure<E>> {
    if array.data_type() != &DataType::Utf8 {
        return Ok(array);
    }
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AccessFailure::Operation(contract("failed to downcast to StringArray")))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut builder = StringBuilder::new();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for row in 0..arr.len() {
        observe(observer, CollectionObservation::Step)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        if arr.is_null(row) {
            builder.append_null();
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            continue;
        }
        let value = arr.value(row);
        if value.len() >= 2 && value.starts_with('\"') && value.ends_with('\"') {
            let inner = &value[1..value.len() - 1];
            if !inner.contains('\"') && !inner.contains('\\') {
                builder.append_value(inner);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                continue;
            }
        }
        builder.append_value(value);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let result = std::sync::Arc::new(builder.finish()) as ArrayRef;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(result)
}

#[derive(Debug)]
enum AccessCause {
    BelowOne {
        ordinal: usize,
        subscript: i64,
    },
    AboveLength {
        ordinal: usize,
        subscript: i64,
        len: usize,
    },
    Conversion {
        operation: &'static str,
        source: std::num::TryFromIntError,
    },
}
impl std::fmt::Display for AccessCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BelowOne { ordinal, subscript } => {
                let _ = (ordinal, subscript);
                f.write_str("Array subscript start at 1")
            }
            Self::AboveLength {
                ordinal,
                subscript,
                len,
            } => {
                let _ = ordinal;
                write!(
                    f,
                    "Array subscript must be less than or equal to array length: {subscript} > {len}"
                )
            }
            Self::Conversion { operation, .. } => f.write_str(operation),
        }
    }
}
impl std::error::Error for AccessCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for AccessCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::Conversion { .. } => (FailureClass::Invariant, 380),
            _ => (FailureClass::Data, 379),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for AccessCause {}

/// Keep the Trino data reason distinct from the original element_at contract.
#[derive(Debug)]
struct StrictSubscriptCause(AccessCause);

impl std::fmt::Display for StrictSubscriptCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            AccessCause::BelowOne { subscript: 0, .. } => {
                f.write_str("SQL array indices start at 1")
            }
            AccessCause::BelowOne { subscript, .. } => {
                write!(f, "Array subscript is negative: {subscript}")
            }
            cause => cause.fmt(f),
        }
    }
}

impl std::error::Error for StrictSubscriptCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl novarocks_failure_contract::FailureProjection for StrictSubscriptCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, kernel_reason};
        FailureFact::new(FailureClass::Data, kernel_reason::INVALID_FUNCTION_ARGUMENT)
    }
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::{error::Error, sync::Arc};
    #[test]
    fn required_strict_subscript_failure_keeps_ordinal_and_does_not_retain_input_arrays() {
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Int32Builder::new());
        builder.values().append_value(7);
        builder.append(true);
        let list: ArrayRef = Arc::new(builder.finish());
        let weak = Arc::downgrade(&list);
        let index: ArrayRef = Arc::new(Int32Array::from(vec![0]));
        let flag: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let mut observer = |_| Ok::<_, KernelFailure>(());
        let inputs = ArrayAccessInputs::new(list, index, Some(flag), &mut observer).unwrap();
        let error = lookup_observed(
            &inputs,
            crate::Selection::all(1),
            |_, row| Ok(inputs.direct_rows(row)),
            None,
            |_, _| panic!("strict subscript error precedes any take"),
            &mut observer,
        )
        .unwrap_err();
        let AccessFailure::Operation(failure) = error else {
            panic!("strict domain cause");
        };
        drop(inputs);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 379);
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<AccessCause>()
            .unwrap();
        assert!(matches!(
            source,
            AccessCause::BelowOne {
                ordinal: 0,
                subscript: 0
            }
        ));
    }
    #[test]
    fn empty_list_of_fixed_lists_produces_typed_null_without_a_dummy_take() {
        let child = Arc::new(arrow_schema::Field::new("nested", DataType::Int32, false));
        let values: ArrayRef = Arc::new(arrow_array::FixedSizeListArray::new(
            child,
            2,
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let expected = values.data_type().clone();
        let field = Arc::new(arrow_schema::Field::new("item", expected.clone(), true));
        let list: ArrayRef = Arc::new(ListArray::new(
            field,
            arrow_buffer::OffsetBuffer::new(vec![0i32, 0].into()),
            values,
            None,
        ));
        let index: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let mut observer = |_| Ok::<_, KernelFailure>(());
        let inputs = ArrayAccessInputs::new(list, index, None, &mut observer).unwrap();
        let (output, row_errors) = lookup_observed(
            &inputs,
            crate::Selection::all(1),
            |_, row| Ok(inputs.direct_rows(row)),
            None,
            |_, _| panic!("an empty source has no take input"),
            &mut observer,
        )
        .unwrap();
        assert!(row_errors.is_empty());
        assert_eq!(output.data_type(), &expected);
        assert_eq!(output.len(), 1);
        assert_eq!(output.null_count(), 1);
    }
}
