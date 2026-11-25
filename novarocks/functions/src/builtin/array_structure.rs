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

use std::ops::Range;
use std::sync::Arc;

use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_schema::{DataType, FieldRef};
use novarocks_type_contract::{
    FunctionValueType, ValueKind, ValueTypeRef, arrow_data_types_exact_observed,
};

use super::array_scalar_output::scratch;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::selected_copy::{
    ExtendSegment, array_from_segments_in, list_child_count, list_from_segments_in,
    list_from_struct_children_in,
};
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
};

#[derive(Clone, Copy, Debug)]
pub(super) enum StructureOp {
    Reverse,
    Flatten,
    Zip,
}

impl StructureOp {
    pub(super) fn dynamic(name: &str) -> Option<Self> {
        match name {
            "flatten" => Some(Self::Flatten),
            "zip" => Some(Self::Zip),
            _ => None,
        }
    }
}

pub(super) fn item_type(value: &FunctionValueType) -> Option<(&ValueTypeRef, &FieldRef)> {
    let (ValueKind::List(item), DataType::List(field)) =
        (value.value_type().kind(), value.data_type())
    else {
        return None;
    };
    Some((item, field))
}

pub(super) fn validate_output_item(
    (input, carrier): (&ValueTypeRef, &FieldRef),
    (output, field): (&ValueTypeRef, &FieldRef),
    observe: &mut impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    if (input.nullable() && !output.nullable())
        || !FunctionValueType::borrowed_value_domains_equal_observed(input, output, &mut *observe)?
        || !arrow_data_types_exact_observed::<KernelFailure>(
            carrier.data_type(),
            field.data_type(),
            &mut *observe,
        )?
    {
        return Err(invalid("Array operation changed the frozen element type"));
    }
    Ok(())
}

pub(super) fn validate_profile(
    operation: StructureOp,
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let mut arguments = contract.value_argument_types();
    let arity = arguments.len();
    let result = contract.result_type();
    let result_item = item_type(result)
        .ok_or_else(|| invalid("Array operation requires a frozen List result"))?;
    if !result.nullable()
        && contract
            .value_argument_types()
            .any(FunctionValueType::nullable)
    {
        return Err(invalid(
            "Array operation cannot discard a nullable array root",
        ));
    }
    match (operation, arity) {
        (StructureOp::Reverse, 1) => validate_output_item(
            arguments
                .next()
                .and_then(item_type)
                .ok_or_else(|| invalid("Array reversal requires a List argument"))?,
            result_item,
            &mut observe,
        ),
        (StructureOp::Flatten, 1) => {
            let Some((outer, outer_field)) = arguments.next().and_then(item_type) else {
                return Err(invalid("Array flatten requires a List argument"));
            };
            let (ValueKind::List(inner), DataType::List(inner_field)) =
                (outer.kind(), outer_field.data_type())
            else {
                return Err(invalid("Array flatten requires List elements"));
            };
            validate_output_item((inner, inner_field), result_item, &mut observe)
        }
        (StructureOp::Zip, 2..=5) => {
            let (ValueKind::Struct(declared), DataType::Struct(fields)) =
                (result_item.0.kind(), result_item.1.data_type())
            else {
                return Err(invalid("Array zip requires a Struct item"));
            };
            if result_item.0.nullable()
                || result_item.1.is_nullable()
                || declared.len() != arity
                || fields.len() != declared.len()
            {
                return Err(invalid(
                    "Array zip requires a non-null ROW item with one field per input",
                ));
            }
            for ((source, declared), field) in arguments.zip(declared).zip(fields.iter()) {
                observe()?;
                if declared.row_name().is_some()
                    || !declared.value_type().nullable()
                    || !field.is_nullable()
                {
                    return Err(invalid("Array zip requires anonymous, nullable ROW fields"));
                }
                validate_output_item(
                    item_type(source)
                        .ok_or_else(|| invalid("Array zip requires List arguments"))?,
                    (declared.value_type(), field),
                    &mut observe,
                )?;
            }
            Ok(())
        }
        _ => Err(invalid("Array operation has the wrong number of arguments")),
    }
}

pub(super) fn list(values: &dyn Array) -> Result<&ListArray, KernelFailure> {
    values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| internal("Array operation received a non-List carrier"))
}

pub(super) fn row_range(values: &ListArray, row: usize) -> Range<usize> {
    let offsets = values.value_offsets();
    offsets[row] as usize..offsets[row + 1] as usize
}

pub(super) fn append_segment(
    segments: &mut HostVec<ExtendSegment, HostAggregateAllocator>,
    start: usize,
    len: usize,
    nulls: usize,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    if len == 0 && nulls == 0 {
        return Ok(());
    }
    if let Some(previous) = segments.last_mut() {
        if len == 0 {
            previous.nulls = previous
                .nulls
                .checked_add(nulls)
                .ok_or(KernelFailure::ResourceExhausted)?;
            return Ok(());
        }
        if previous.nulls == 0 && previous.start + previous.len == start {
            previous.len += len;
            previous.nulls = nulls;
            return Ok(());
        }
    }
    if segments.len() == segments.capacity() {
        work.flush()?;
        segments.try_reserve(1).map_err(|_| {
            allocator
                .take_recorded_failure()
                .unwrap_or(KernelFailure::ResourceExhausted)
        })?;
    }
    segments.push(ExtendSegment {
        source: 0,
        start,
        len,
        repeats: 1,
        nulls,
    });
    Ok(())
}

pub(super) fn evaluate<'a>(
    operation: StructureOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let target = input.contract().result_type().data_type();
    let DataType::List(field) = target else {
        return Err(internal("Array operation did not freeze a List result"));
    };
    let result = (|| {
        let output = match operation {
            StructureOp::Reverse | StructureOp::Flatten => {
                reorder(operation, &input, field, host, allocator, &mut work)?
            }
            StructureOp::Zip => zip(&input, field, host, allocator, &mut work)?,
        };
        SelectedValues::try_new_observed(input.selection(), target, output, Box::default(), || {
            work.step()
        })
    })();
    work.finish_result(result)
}

fn reorder(
    operation: StructureOp,
    input: &ScalarCallInput<'_, '_>,
    item: &FieldRef,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let argument = input.arguments()[0];
    let array = list(argument.array().as_ref())?;
    let inner = match operation {
        StructureOp::Flatten => Some(list(array.values().as_ref())?),
        StructureOp::Reverse => None,
        StructureOp::Zip => return Err(internal("Array zip is not a one-input reorder")),
    };
    let child = inner.map_or_else(|| array.values(), ListArray::values);
    let selection = input.selection();
    work.flush()?;
    let mut row_lengths = scratch(selection.len(), allocator)?;
    let mut segments = scratch(0, allocator)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, row);
        if array.is_null(row) {
            row_lengths.push(None);
            continue;
        }
        let positions = row_range(array, row);
        let mut length = 0usize;
        if let Some(inner) = inner {
            for index in positions {
                work.step()?;
                if inner.is_null(index) {
                    continue;
                }
                let span = row_range(inner, index);
                length += span.len();
                append_segment(&mut segments, span.start, span.len(), 0, allocator, work)?;
            }
        } else {
            length = positions.len();
            for index in positions.rev() {
                work.step()?;
                append_segment(&mut segments, index, 1, 0, allocator, work)?;
            }
        }
        row_lengths.push(Some(length));
    }
    list_from_segments_in(
        item,
        std::slice::from_ref(child),
        &segments,
        &row_lengths,
        host,
        allocator,
        work,
    )
}

fn zip(
    input: &ScalarCallInput<'_, '_>,
    item: &FieldRef,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let DataType::Struct(fields) = item.data_type() else {
        return Err(internal("Array zip requires a frozen ROW item"));
    };
    let arguments = input.arguments();
    let selection = input.selection();
    work.flush()?;
    let mut arrays = scratch(arguments.len(), allocator)?;
    let mut row_lengths = scratch(selection.len(), allocator)?;
    for argument in arguments {
        work.step()?;
        arrays.push(list(argument.array().as_ref())?);
    }
    for (ordinal, row) in selection.iter().enumerate() {
        let mut length = Some(0usize);
        for (argument, array) in arguments.iter().zip(&arrays) {
            work.step()?;
            let row = argument.value_row(ordinal, row);
            if array.is_null(row) {
                length = None;
                break;
            }
            length = length.map(|previous| previous.max(row_range(array, row).len()));
        }
        row_lengths.push(length);
    }
    let total = list_child_count(&row_lengths, work)?;
    work.flush()?;
    let mut children = scratch(arguments.len(), allocator)?;
    let mut segments = scratch(selection.len(), allocator)?;
    for (argument, array) in arguments.iter().zip(&arrays) {
        segments.clear();
        for ((ordinal, row), length) in selection.iter().enumerate().zip(&row_lengths) {
            work.step()?;
            let Some(length) = *length else {
                continue;
            };
            let source = row_range(array, argument.value_row(ordinal, row));
            append_segment(
                &mut segments,
                source.start,
                source.len(),
                length - source.len(),
                allocator,
                work,
            )?;
        }
        children.push(array_from_segments_in(
            std::slice::from_ref(array.values()),
            &segments,
            total,
            host,
            allocator,
            work,
        )?);
    }
    drop(segments);
    list_from_struct_children_in(item, fields, &children, &row_lengths, host, allocator, work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::array_invocation_test_support::{Control, Host};
    use arrow_array::{Int32Array, StringArray, StructArray};

    #[test]
    fn unknown_array_structure_freezes_list_ports_before_preparation() {
        use crate::{FunctionArgument, FunctionArgumentType, FunctionBindingRequest, FunctionKind};

        let catalog = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let list_type =
            |element| DataType::List(Arc::new(arrow_schema::Field::new_list_field(element, true)));
        let unknown_list = list_type(DataType::Null);
        let integer_list = list_type(DataType::Int32);
        let nested_unknown = list_type(unknown_list.clone());
        for (name, inputs, expected_targets) in [
            (
                "flatten",
                vec![DataType::Null],
                vec![nested_unknown.clone()],
            ),
            ("flatten", vec![unknown_list.clone()], vec![nested_unknown]),
            (
                "zip",
                vec![DataType::Null, integer_list.clone()],
                vec![unknown_list, integer_list],
            ),
        ] {
            let source = inputs
                .into_iter()
                .map(|ty| FunctionArgument::Value {
                    value_type: FunctionValueType::new(ty, true),
                    constant: None,
                })
                .collect::<Vec<_>>();
            let resolve = |arguments: &[FunctionArgument]| {
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments,
                            logical_argument_count: arguments.len(),
                        },
                        crate::binding_test_control(),
                    )
                    .unwrap()
            };
            let bound = resolve(&source);
            let targets = bound
                .selected
                .argument_types
                .iter()
                .zip(expected_targets)
                .map(|(argument, expected)| {
                    let FunctionArgumentType::Value(value_type) = argument else {
                        panic!("expected array value argument");
                    };
                    assert_eq!(value_type.data_type(), &expected);
                    FunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(resolve(&targets).selected, bound.selected);
        }
    }

    fn evaluate_selected(
        name: &str,
        sources: &[ArrayRef],
        active: &[usize],
        host: Arc<dyn AggregateStateAllocator>,
    ) -> ArrayRef {
        use crate::{
            CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument,
            FunctionBindingRequest, FunctionKind, PreparedPureKernel, PureCallPreparation,
            ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
        };
        use novarocks_type_contract::{
            CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
            ExpressionEffectContext, ExpressionUseId, SemanticParameters,
        };

        let catalog = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let types = sources
            .iter()
            .map(|source| FunctionArgument::Value {
                value_type: FunctionValueType::new(source.data_type().clone(), true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &types,
            logical_argument_count: types.len(),
        };
        let compile = crate::binding_test_control();
        let bound = catalog
            .resolve_bound_user(name, FunctionKind::Scalar, request, compile)
            .unwrap();
        let selected = Arc::new(bound.selected);
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: EvaluationRegionId::new(0),
            demand: EvaluationDemand::Value,
        };
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = (1..=types.len() as u32)
            .map(|index| Some(ExpressionUseId::new(index)))
            .collect::<Vec<_>>();
        let prepared = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context: context.into(),
                    argument_uses: CallArgumentUses::SelectedChannels(&uses),
                    function_id: &bound.function_id,
                    kind: FunctionKind::Scalar,
                    selected: &selected,
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Unconditional,
                },
                Arc::clone(&selected),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context),
                },
                compile,
            )
            .unwrap()
            .into_prepared();
        let PreparedPureKernel::Scalar(prepared) = prepared else {
            panic!("expected scalar kernel");
        };
        let mut instance =
            ScalarEvaluationInstance::instantiate_with_allocator(prepared, Some(host)).unwrap();
        let evaluated = sources
            .iter()
            .map(EvaluatedArgument::Column)
            .collect::<Vec<_>>();
        let rows = Selection::try_sparse(sources[0].len(), active).unwrap();
        let (_, values, errors) = instance
            .evaluate(rows, &evaluated, &Control::default())
            .unwrap()
            .into_parts();
        assert!(errors.is_empty());
        values
    }

    #[test]
    fn reverse_preserves_array_elements_and_sparse_row_order() {
        use arrow_array::types::Int32Type;
        let source = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(2)]),
            Some(vec![Some(99)]),
            Some(vec![Some(3), Some(4)]),
        ])) as ArrayRef;
        let host = Host::with_limit(1024 * 1024);
        let result = evaluate_selected("reverse", &[source], &[0, 2], host.clone());
        let array = list(result.as_ref()).unwrap();
        assert_eq!(array.value_offsets(), &[0, 3, 5]);
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(2), None, Some(1), Some(4), Some(3)]
        );
        drop(result);
        host.released();
    }

    #[test]
    fn array_order_preserves_nested_lists_with_directional_inner_nulls() {
        use arrow_array::types::Int32Type;
        use arrow_buffer::OffsetBuffer;

        let nested = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(1)]),
            None,
        ]);
        let source: ArrayRef = Arc::new(ListArray::new(
            Arc::new(arrow_schema::Field::new_list_field(
                nested.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(vec![0, 3, 4, 4].into()),
            Arc::new(nested),
            None,
        ));
        let host = Host::with_limit(1024 * 1024);
        let arguments = &[source];
        let output = evaluate_selected("array_sort", arguments, &[0, 1, 2], host.clone());
        let sorted = list(output.as_ref()).unwrap();
        let children = list(sorted.values().as_ref()).unwrap();
        let values = children
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(sorted.value_offsets(), &[0, 3, 4, 4]);
        assert!(children.is_null(3));
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(1), Some(2), Some(1), None]
        );
        drop(output);

        for (name, expected) in [("array_min", 1), ("array_max", 2)] {
            let result = evaluate_selected(name, arguments, &[0, 1, 2], host.clone());
            let result = list(result.as_ref()).unwrap();
            let child = result
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(result.value_offsets(), &[0, 2, 2, 2]);
            assert_eq!(
                child.iter().collect::<Vec<_>>(),
                vec![Some(1), Some(expected)]
            );
            assert!(result.is_null(1));
            assert!(result.is_null(2));
        }
        host.released();
    }

    #[test]
    fn flatten_ignores_unreachable_subarrays_and_preserves_selected_nulls() {
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        let item = Arc::new(arrow_schema::Field::new_list_field(DataType::Int32, true));
        let inner = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2, 3, 4, 5].into()),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(99),
                Some(3),
                Some(88),
            ])),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let outer = Arc::new(ListArray::new(
            Arc::new(arrow_schema::Field::new_list_field(
                inner.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(vec![0, 3, 4, 4].into()),
            Arc::new(inner),
            Some(NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;
        let host = Host::with_limit(1024 * 1024);
        let result = evaluate_selected("flatten", &[outer], &[0, 2], host.clone());
        let array = list(result.as_ref()).unwrap();
        assert_eq!(array.value_offsets(), &[0, 3, 3]);
        assert!(array.is_null(1));
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3)]
        );
        drop(result);
        host.released();
    }

    #[test]
    fn zip_uses_maximum_length_and_preserves_strict_array_roots() {
        use arrow_array::types::{Int32Type, Int64Type};
        let first = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(3), None]),
            None,
            Some(vec![Some(99)]),
        ]);
        let second = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(2)]),
            Some(vec![Some(8)]),
            Some(vec![Some(98)]),
        ]);
        let host = Host::with_limit(1024 * 1024);
        let result = evaluate_selected(
            "zip",
            &[Arc::new(first), Arc::new(second)],
            &[0, 1],
            host.clone(),
        );
        let array = list(result.as_ref()).unwrap();
        assert_eq!(array.value_offsets(), &[0, 2, 2]);
        assert!(array.is_null(1));
        let rows = array
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(rows.null_count(), 0);
        assert!(rows.column(0).is_null(1) && rows.column(1).is_null(1));
        drop(result);
        host.released();
    }

    #[test]
    fn zip_preserves_funded_children_until_the_last_alias() {
        let tracked = Host::with_limit(1024 * 1024);
        let host: Arc<dyn AggregateStateAllocator> = tracked.clone();
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        let allocator = HostAggregateAllocator::try_new(Arc::clone(&host)).unwrap();
        let fields = [
            arrow_schema::Field::new("field1", DataType::Int32, true),
            arrow_schema::Field::new("field2", DataType::Utf8, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<arrow_schema::Fields>();
        let item = Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Struct(fields.clone()),
            false,
        ));
        let first: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None]));
        let second: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
        let copied = array_from_segments_in(
            &[second],
            &[ExtendSegment {
                source: 0,
                start: 0,
                len: 1,
                repeats: 1,
                nulls: 1,
            }],
            2,
            &host,
            &allocator,
            &mut work,
        )
        .unwrap();
        let zipped = list_from_struct_children_in(
            &item,
            &fields,
            &[first, copied],
            &[Some(2), None, Some(0)],
            &host,
            &allocator,
            &mut work,
        )
        .unwrap();
        let lists = list(zipped.as_ref()).unwrap();
        assert_eq!(lists.value_offsets(), &[0, 2, 2, 2]);
        assert!(lists.is_null(1));
        let alias = Arc::clone(lists.values());
        let rows = alias.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(rows.null_count(), 0);
        assert!(rows.column(0).is_null(1) && rows.column(1).is_null(1));
        drop(zipped);
        drop(allocator);
        assert!(tracked.live_blocks() > 0);
        drop(alias);
        tracked.released();
    }

    #[test]
    fn zip_list_extent_refuses_before_any_child_allocation() {
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        assert!(list_child_count(&[Some(i32::MAX as usize), Some(1)], &mut work).is_err());
    }
}
