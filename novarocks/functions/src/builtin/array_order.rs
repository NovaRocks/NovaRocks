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

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray};
use arrow_schema::{DataType, FieldRef};
use novarocks_type_contract::{
    FunctionValueType, ValueKind, ValueLogicalType, ValueTypeRef, arrow_data_types_exact_observed,
};

use super::array_scalar_output::scratch;
use super::array_structure::{append_segment, item_type, list, row_range, validate_output_item};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ExtremaDirection;
use crate::comparison::ExtremaOrder;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::selected_copy::{array_from_segments_in, list_from_segments_in};
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
};

#[derive(Clone, Copy, Debug)]
pub(super) enum OrderOp {
    Sort,
    Minimum,
    Maximum,
}

impl OrderOp {
    pub(super) fn for_name(name: &str) -> Option<Self> {
        match name {
            "array_sort" => Some(Self::Sort),
            "array_min" => Some(Self::Minimum),
            "array_max" => Some(Self::Maximum),
            _ => None,
        }
    }

    fn direction(self) -> ExtremaDirection {
        match self {
            Self::Sort | Self::Minimum => ExtremaDirection::Minimum,
            Self::Maximum => ExtremaDirection::Maximum,
        }
    }
}

/// Known Trino non-orderable types fail the signature constraint before the
/// installed recipe check. Trino ARRAY and ROW inherit their members' domains.
pub(super) fn is_trino_orderable<E>(
    argument: &FunctionValueType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let Some((item, _)) = item_type(argument) else {
        return Ok(false);
    };
    is_orderable_type(item, &mut observe)
}

pub(super) fn is_orderable_type<E>(
    ty: &ValueTypeRef,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    observe()?;
    match ty.kind() {
        ValueKind::Map { .. }
        | ValueKind::Nominal(ValueLogicalType::Json | ValueLogicalType::Variant) => Ok(false),
        ValueKind::List(item) => is_orderable_type(item, observe),
        ValueKind::Struct(fields) => {
            for field in fields {
                if !is_orderable_type(field.value_type(), observe)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

pub(super) fn supports<E>(
    argument: &FunctionValueType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let Some((item, field)) = item_type(argument) else {
        return Ok(false);
    };
    ExtremaOrder::supports(item, field.data_type(), observe)
}

pub(super) fn prepare(
    operation: OrderOp,
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<ExtremaOrder, KernelFailure> {
    let mut arguments = contract.value_argument_types();
    let (Some(argument), None) = (arguments.next(), arguments.next()) else {
        return Err(invalid("Natural array order requires one List argument"));
    };
    let source = item_type(argument)
        .ok_or_else(|| invalid("Natural array order requires a frozen List argument"))?;
    let result = contract.result_type();
    match operation {
        OrderOp::Sort => {
            if argument.nullable() && !result.nullable() {
                return Err(invalid("Array sort cannot discard a nullable array root"));
            }
            let output = item_type(result)
                .ok_or_else(|| invalid("Array sort requires a frozen List result"))?;
            validate_output_item(source, output, &mut observe)?;
        }
        OrderOp::Minimum | OrderOp::Maximum => {
            if !result.nullable()
                || !FunctionValueType::borrowed_value_domains_equal_observed(
                    source.0,
                    result.value_type(),
                    &mut observe,
                )?
                || !arrow_data_types_exact_observed::<KernelFailure>(
                    source.1.data_type(),
                    result.data_type(),
                    &mut observe,
                )?
            {
                return Err(invalid(
                    "Array extrema requires the same nullable element type",
                ));
            }
        }
    }
    ExtremaOrder::prepare(source.0, source.1.data_type(), observe)?
        .ok_or_else(|| invalid("Array operation lacks the frozen element order"))
}

pub(super) fn evaluate<'a>(
    operation: OrderOp,
    order: &ExtremaOrder,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let argument = input.arguments()[0];
    let array = list(argument.array().as_ref())?;
    let target = input.contract().result_type().data_type();
    let selection = input.selection();
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let output = match operation {
            OrderOp::Sort => {
                let DataType::List(field) = target else {
                    return Err(internal("Array sort has a non-List result"));
                };
                sort(order, &input, array, field, host, allocator, &mut work)?
            }
            OrderOp::Minimum | OrderOp::Maximum => extrema(
                operation.direction(),
                order,
                &input,
                array,
                host,
                allocator,
                &mut work,
            )?,
        };
        SelectedValues::try_new_observed(selection, target, output, Box::default(), || work.step())
    })();
    work.finish_result(result)
}

fn sort(
    order: &ExtremaOrder,
    input: &ScalarCallInput<'_, '_>,
    array: &ListArray,
    field: &FieldRef,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let selection = input.selection();
    let argument = input.arguments()[0];
    let values = array.values();
    work.flush()?;
    let mut lengths = scratch(selection.len(), allocator)?;
    let mut segments = scratch(0, allocator)?;
    let mut positions = scratch(0, allocator)?;
    let mut auxiliary = scratch(0, allocator)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, row);
        if array.is_null(row) {
            lengths.push(None);
            continue;
        }
        let positions_in_row = row_range(array, row);
        let len = positions_in_row.len();
        lengths.push(Some(len));
        if len <= 1 {
            append_segment(
                &mut segments,
                positions_in_row.start,
                len,
                0,
                allocator,
                work,
            )?;
            continue;
        }
        positions.clear();
        auxiliary.clear();
        if len > positions.capacity() {
            work.flush()?;
            positions.try_reserve_exact(len).map_err(|_| {
                allocator
                    .take_recorded_failure()
                    .unwrap_or(KernelFailure::ResourceExhausted)
            })?;
        }
        if len > auxiliary.capacity() {
            work.flush()?;
            auxiliary.try_reserve_exact(len).map_err(|_| {
                allocator
                    .take_recorded_failure()
                    .unwrap_or(KernelFailure::ResourceExhausted)
            })?;
        }
        for position in positions_in_row {
            work.step()?;
            positions.push(position);
            auxiliary.push(position);
        }
        stable_positions(&mut positions, &mut auxiliary, work, |left, right, work| {
            order.compare(
                (values.as_ref(), left),
                (values.as_ref(), right),
                ExtremaDirection::Minimum,
                work,
            )
        })?;
        for &position in &positions {
            work.step()?;
            append_segment(&mut segments, position, 1, 0, allocator, work)?;
        }
    }
    drop(auxiliary);
    drop(positions);
    list_from_segments_in(
        field,
        std::slice::from_ref(values),
        &segments,
        &lengths,
        host,
        allocator,
        work,
    )
}

pub(super) fn stable_positions(
    positions: &mut [usize],
    temporary: &mut [usize],
    work: &mut EvaluationCheckpoints<'_>,
    mut compare: impl FnMut(
        usize,
        usize,
        &mut EvaluationCheckpoints<'_>,
    ) -> Result<Ordering, KernelFailure>,
) -> Result<(), KernelFailure> {
    let mut width = 1usize;
    let mut primary_source = true;
    while width < positions.len() {
        let (source, target) = if primary_source {
            (&*positions, &mut *temporary)
        } else {
            (&*temporary, &mut *positions)
        };
        let mut start = 0;
        while start < source.len() {
            let middle = start.saturating_add(width).min(source.len());
            let end = middle.saturating_add(width).min(source.len());
            let (mut left, mut right) = (start, middle);
            for position in &mut target[start..end] {
                work.step()?;
                if left < middle
                    && (right == end
                        || compare(source[left], source[right], work)? != Ordering::Greater)
                {
                    *position = source[left];
                    left += 1;
                } else {
                    *position = source[right];
                    right += 1;
                }
            }
            start = end;
        }
        width = width.saturating_mul(2);
        primary_source = !primary_source;
    }
    if !primary_source {
        for (target, source) in positions.iter_mut().zip(temporary) {
            work.step()?;
            *target = *source;
        }
    }
    Ok(())
}

fn extrema(
    direction: ExtremaDirection,
    order: &ExtremaOrder,
    input: &ScalarCallInput<'_, '_>,
    array: &ListArray,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let values = array.values();
    let selection = input.selection();
    let argument = input.arguments()[0];
    work.flush()?;
    let mut segments = scratch(selection.len(), allocator)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, row);
        let winner = if array.is_null(row) {
            None
        } else {
            winner(
                direction,
                order,
                values.as_ref(),
                row_range(array, row),
                work,
            )?
        };
        let (start, len, nulls) = winner.map_or((0, 0, 1), |index| (index, 1, 0));
        append_segment(&mut segments, start, len, nulls, allocator, work)?;
    }
    array_from_segments_in(
        std::slice::from_ref(values),
        &segments,
        selection.len(),
        host,
        allocator,
        work,
    )
}

fn winner(
    direction: ExtremaDirection,
    order: &ExtremaOrder,
    values: &dyn Array,
    positions: Range<usize>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    let better = match direction {
        ExtremaDirection::Minimum => Ordering::Less,
        ExtremaDirection::Maximum => Ordering::Greater,
    };
    let mut winner = None;
    for position in positions {
        work.step()?;
        if values.data_type() == &DataType::Null || values.is_null(position) {
            return Ok(None);
        }
        let Some(current) = winner else {
            winner = Some(position);
            continue;
        };
        if order.compare((values, position), (values, current), direction, work)? == better {
            winner = Some(position);
        }
    }
    Ok(winner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::array_invocation_test_support::Control;
    use arrow_array::Float64Array;

    fn float_order() -> ExtremaOrder {
        let ty = FunctionValueType::new(DataType::Float64, true);
        ExtremaOrder::prepare(ty.value_type(), ty.data_type(), || {
            Ok::<_, KernelFailure>(())
        })
        .unwrap()
        .unwrap()
    }

    #[test]
    fn orderability_uses_nominal_child_semantics_and_keeps_unknown() {
        use arrow_schema::Field;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;

        let typed_field = |logical| {
            let mut item = Field::new_list_field(DataType::LargeBinary, true);
            if let Some(logical) = logical {
                item = item.with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), logical)]);
            }
            item
        };
        for (logical, expected) in [(None, true), (Some("json".to_owned()), false)] {
            let row = DataType::Struct(vec![Arc::new(typed_field(logical))].into());
            let list = DataType::List(Arc::new(Field::new_list_field(row, true)));
            let argument = FunctionValueType::from_legacy_declared_projection(list, true);
            assert_eq!(
                is_trino_orderable(&argument, || Ok::<_, KernelFailure>(())).unwrap(),
                expected,
            );
        }
        let unknown = FunctionValueType::new(
            DataType::List(Arc::new(Field::new_list_field(DataType::Null, true))),
            true,
        );
        assert!(is_trino_orderable(&unknown, || Ok::<_, KernelFailure>(())).unwrap());
    }

    #[test]
    fn natural_order_is_stable_on_equal_zero_nan_and_null() {
        let values = Float64Array::from(vec![
            Some(f64::NAN),
            Some(-0.0),
            None,
            Some(0.0),
            Some(1.0),
            Some(f64::NAN),
            None,
        ]);
        let mut positions = [0, 1, 2, 3, 4, 5, 6];
        let mut temporary = [0; 7];
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        let order = float_order();
        stable_positions(
            &mut positions,
            &mut temporary,
            &mut work,
            |left, right, work| {
                order.compare(
                    (&values, left),
                    (&values, right),
                    ExtremaDirection::Minimum,
                    work,
                )
            },
        )
        .unwrap();
        assert_eq!(positions, [1, 3, 4, 0, 5, 2, 6]);
    }

    #[test]
    fn extrema_returns_null_on_empty_or_any_null_and_prefers_ordered_values() {
        let values =
            Float64Array::from(vec![Some(f64::NAN), Some(-0.0), Some(0.0), Some(1.0), None]);
        let order = float_order();
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        for direction in [ExtremaDirection::Minimum, ExtremaDirection::Maximum] {
            assert_eq!(
                winner(direction, &order, &values, 0..0, &mut work).unwrap(),
                None
            );
            assert_eq!(
                winner(direction, &order, &values, 0..5, &mut work).unwrap(),
                None
            );
            assert_eq!(
                winner(direction, &order, &values, 0..1, &mut work).unwrap(),
                Some(0)
            );
        }
        assert_eq!(
            winner(ExtremaDirection::Minimum, &order, &values, 0..4, &mut work).unwrap(),
            Some(1)
        );
        assert_eq!(
            winner(ExtremaDirection::Maximum, &order, &values, 0..4, &mut work).unwrap(),
            Some(3)
        );
    }

    #[test]
    fn long_single_array_sort_observes_inner_progress_and_original_refusal() {
        let mut positions = (0..4096).rev().collect::<Vec<_>>();
        let mut temporary = vec![0; positions.len()];
        let control = Control {
            refusal: Some((2, KernelFailure::Cancelled)),
            ..Default::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        assert_eq!(
            stable_positions(&mut positions, &mut temporary, &mut work, |a, b, _| Ok(
                a.cmp(&b)
            )),
            Err(KernelFailure::Cancelled),
        );
        assert_eq!(control.trace.into_inner().unwrap(), vec![256; 3]);
    }
}
