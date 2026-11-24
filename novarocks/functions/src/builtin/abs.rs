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

//! Selected ABS contracts and addresses delegate to the ONE original core.
use super::abs_core::{AbsError, AbsObservation, evaluate_abs_core};
use crate::{
    DataFailureReason, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
#[cfg(test)]
use crate::{EvaluatedArgument, Selection};
#[cfg(test)]
use arrow_array::ArrayRef;
use arrow_array::types::{Decimal128Type, DecimalType, validate_decimal_precision_and_scale};
use arrow_array::{Array, Decimal128Array, UInt64Array};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
pub(super) fn evaluate_abs<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [FunctionArgumentType::Value(source)] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("ABS requires one exact value argument"));
    };
    let [argument] = input.arguments() else {
        return Err(invalid("ABS requires one evaluated argument"));
    };
    let target = input.contract().result_type();
    if source.nullable() != target.nullable() {
        return Err(invalid(
            "ABS result must preserve selected input nullability",
        ));
    }
    if source.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != source.data_type()
    {
        return Err(invalid("ABS must preserve its exact input type"));
    }
    let width = match source.data_type() {
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        DataType::Decimal128(p, s) => {
            validate_decimal_precision_and_scale::<Decimal128Type>(*p, *s)
                .map_err(|_| invalid("ABS selected Decimal128 precision or scale is invalid"))?;
            16
        }
        _ => return Err(invalid("ABS input differs from its installed types")),
    };
    let selection = input.selection();
    output_capacity(selection.len(), width.max(8))?;
    let array = argument.array();
    if array.data_type() != source.data_type() {
        return Err(internal(
            "ABS selected primitive carrier cannot be downcast",
        ));
    }
    let decimal = if matches!(source.data_type(), DataType::Decimal128(..)) {
        Some(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| internal("ABS selected primitive carrier cannot be downcast"))?,
        )
    } else {
        None
    };
    let signed_min = match source.data_type() {
        DataType::Int8 => Some(i64::from(i8::MIN)),
        DataType::Int16 => Some(i64::from(i16::MIN)),
        DataType::Int32 => Some(i64::from(i32::MIN)),
        DataType::Int64 => Some(i64::MIN),
        _ => None,
    };
    let signed = signed_min
        .map(|min| {
            crate::math_numeric::NumericArrayView::new(array)
                .map(|view| (view, min))
                .map_err(KernelFailure::from_domain)
        })
        .transpose()?;
    let mut errors = Vec::new();
    let mut work = EvaluationCheckpoints::new(control);
    let mut identity = selection.len() == array.len();
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= array.len() {
            return Err(internal(
                "ABS selected argument row is outside its checked carrier",
            ));
        }
        if array.is_null(row) {
            if !target.nullable() {
                return Err(internal("ABS non-null selected argument contains SQL NULL"));
            }
        } else if signed
            .as_ref()
            .is_some_and(|(view, min)| view.value_i64(row) == Some(*min))
        {
            errors
                .try_reserve(1)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            errors.push(RowDataError::with_reason(
                ordinal,
                DataFailureReason::NumericOutOfRange,
                "Absolute value exceeds the signed result width",
            ));
            identity = false;
        } else if let (Some(decimal), DataType::Decimal128(p, s)) = (decimal, source.data_type()) {
            // This is the original selected input contract check, not a second ABS
            // algorithm or another decimal rescaling author.
            Decimal128Type::validate_decimal_precision(decimal.value(row), *p, *s)
                .map_err(|_| internal("ABS input exceeds selected Decimal128 precision"))?;
        }
        identity &= row == ordinal;
    }
    work.flush()?;
    let compact = if identity {
        Arc::clone(array)
    } else {
        work.flush()?;
        let mut addresses = Vec::with_capacity(selection.len());
        work.flush()?;
        let mut invalid_rows = errors.iter().map(RowDataError::selected_ordinal).peekable();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            work.step()?;
            if invalid_rows.next_if_eq(&ordinal).is_some() {
                addresses.push(None);
            } else {
                addresses.push(Some(argument.value_row(ordinal, batch_row) as u64));
            }
        }
        work.flush()?;
        let addresses = UInt64Array::from(addresses);
        work.flush()?;
        let compact = arrow_select::take::take(array.as_ref(), &addresses, None)
            .map_err(|source| AbsError::gather(source).into_kernel())?;
        work.flush()?;
        compact
    };
    let values = evaluate_abs_core(
        compact,
        target.data_type(),
        &mut |observation| match observation {
            AbsObservation::Step => work.step(),
            AbsObservation::OpaqueBoundary => work.flush(),
        },
    )
    .map_err(AbsError::into_kernel)?;
    work.finish()?;
    SelectedValues::try_new(
        selection,
        target.data_type(),
        values,
        errors.into_boxed_slice(),
    )
    .map_err(|_| internal("ABS compact output violates its selected contract"))
}
/// Check Rust allocation representability, not an application capacity grant.
fn output_capacity(rows: usize, width: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(width)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConstantPolicy, ConstantPool, FunctionValueType, ScalarEvaluationInstance};
    use arrow_array::{Decimal128Array, Float32Array, Float64Array, Int8Array, Int64Array};
    use novarocks_type_contract::CompilePhase;
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = &self.refusal
                && index == *at
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ABS must never wait");
        }
    }

    fn instance(source: FunctionValueType) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate(super::super::abs_owner::prepared_for_test(source))
            .unwrap()
    }
    fn physical(data_type: DataType, nullable: bool) -> FunctionValueType {
        FunctionValueType::from_legacy_declared_projection(data_type, nullable)
    }
    fn column_result(array: ArrayRef, source: FunctionValueType) -> ArrayRef {
        let mut instance = instance(source);
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(Selection::all(array.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    #[test]
    fn float32_abs_clears_only_sign_bits_including_nan_payloads() {
        let bits = [
            0x80000000u32,
            0,
            0xff800000,
            0x7f800000,
            0xffc12345,
            0x7fc54321,
            0xbf800000,
        ];
        let output = column_result(
            Arc::new(Float32Array::from(
                bits.iter()
                    .map(|bits| f32::from_bits(*bits))
                    .collect::<Vec<_>>(),
            )),
            physical(DataType::Float32, false),
        );
        let values = output.as_any().downcast_ref::<Float32Array>().unwrap();
        for (row, bits) in bits.iter().enumerate() {
            assert_eq!(values.value(row).to_bits(), bits & 0x7fffffff);
        }
    }
    #[test]
    fn float64_abs_clears_only_sign_bits_including_nan_payloads() {
        let bits = [
            0x8000000000000000u64,
            0,
            0xfff0000000000000,
            0x7ff0000000000000,
            0xfff8123456789abc,
            0x7ff8abcdef123456,
            0xbff0000000000000,
        ];
        let output = column_result(
            Arc::new(Float64Array::from(
                bits.iter()
                    .map(|bits| f64::from_bits(*bits))
                    .collect::<Vec<_>>(),
            )),
            physical(DataType::Float64, false),
        );
        let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, bits) in bits.iter().enumerate() {
            assert_eq!(values.value(row).to_bits(), bits & 0x7fffffffffffffff);
        }
    }
    #[test]
    fn decimal128_preserves_precision_scale_and_exact_unscaled_values() {
        for (precision, scale, negative, expected) in [
            (3, 1, -999i128, 999i128),
            (18, -2, -123456789012345678, 123456789012345678),
            (
                38,
                7,
                -99999999999999999999999999999999999999,
                99999999999999999999999999999999999999,
            ),
        ] {
            let array = Decimal128Array::from(vec![Some(negative), Some(0), None])
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let output = column_result(
                Arc::new(array),
                physical(DataType::Decimal128(precision, scale), true),
            );
            assert_eq!(output.data_type(), &DataType::Decimal128(precision, scale));
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(expected), Some(0), None]
            );
        }
    }

    #[test]
    fn sparse_decimal_does_not_inspect_unselected_invalid_precision_values() {
        let source = physical(DataType::Decimal128(3, 1), true);
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![9999, -123, 9999])
                .with_precision_and_scale(3, 1)
                .unwrap(),
        );
        let rows = [1];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let args = [EvaluatedArgument::Column(&array)];
        let mut sparse = instance(source.clone());
        let output = sparse
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            123
        );
        assert!(output.errors().is_empty());
        let mut dense = instance(source);
        assert!(matches!(
            dense.evaluate(Selection::all(3), &args, &Control::default()),
            Err(KernelFailure::Internal(_))
        ));
    }

    #[test]
    fn selected_column_uses_compact_ordinals_and_scalar_broadcast_is_explicit() {
        let rows = [2, 7, 9];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let array: ArrayRef = Arc::new(Int8Array::from(vec![Some(-4), None, Some(-8)]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int8, array, Box::default()).unwrap();
        let arguments = [EvaluatedArgument::SelectedColumn(&compact)];
        let mut compact_instance = instance(physical(DataType::Int8, true));
        let result = compact_instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(4), None, Some(8)]
        );

        let scalar: ArrayRef = Arc::new(Int8Array::from(vec![-7]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut scalar_instance = instance(physical(DataType::Int8, false));
        let result = scalar_instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[7, 7, 7]
        );
        let arguments = [EvaluatedArgument::Column(&scalar)];
        let mut wrong_shape = instance(physical(DataType::Int8, false));
        assert!(matches!(
            wrong_shape.evaluate(selection, &arguments, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }

    #[test]
    fn constant_nonzero_pool_ordinal_broadcasts_only_its_value() {
        let ty = physical(DataType::Int8, true);
        let array: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(-3), Some(-8)]));
        // Explicit finite fixture limits, not a production policy or MEM grant.
        let policy = ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 1,
            max_logical_elements: 3,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            // The existing shared model includes bounded diagnostic/header
            // scratch in addition to this three-value Arrow backing.
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("abs-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let value = pool.value(2).unwrap();
        let rows = [1, 6];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let arguments = [EvaluatedArgument::Constant(&value)];
        let mut instance = instance(ty);
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[8, 8]
        );
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
    }

    #[test]
    fn empty_selection_returns_exact_empty_output_without_abs_body() {
        let rows = [];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut instance = instance(physical(DataType::Int64, true));
        let control = Control::default();
        let output = instance.evaluate(selection, &arguments, &control).unwrap();
        assert!(output.values().is_empty());
        assert_eq!(output.values().data_type(), &DataType::Int64);
        assert_eq!(
            instance.contract().result_type().logical_type(),
            ValueLogicalType::Physical
        );
        assert!(output.errors().is_empty());
        // Wrapper entry and argument entry only; the private body has its own
        // zero-work entry, which must not run on an empty selected domain.
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            2
        );
    }

    #[test]
    fn actual_body_entry_256_and_tail_refusals_preserve_each_outer_category() {
        let scalar: ArrayRef = Arc::new(Int8Array::from(vec![-2]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let source = physical(DataType::Int8, true);
        let selection = Selection::all(320);
        let control = Control::default();
        let mut baseline = instance(source.clone());
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body_entry = calls
            .iter()
            .enumerate()
            .filter(|(_, units)| **units == 0)
            .nth(2)
            .unwrap()
            .0;
        let body_interior = calls.iter().position(|units| *units == 256).unwrap();
        let body_tail = body_interior + 1;
        assert!(body_entry < body_interior);
        assert_eq!(calls[body_tail], 64);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body_entry, body_interior, body_tail] {
                let control = Control::refusing(at, error.clone());
                let mut instance = instance(source.clone());
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(
                    control.calls().len(),
                    at + 1,
                    "failure must stop before publishing or later checkpoints"
                );
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &Control::default())
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
            }
        }
    }

    #[test]
    fn selected_output_capacity_overflow_is_refused_before_any_row_or_allocation() {
        for data_type in [DataType::Int8, DataType::Int64, DataType::Float64] {
            let scalar: ArrayRef = match data_type {
                DataType::Int8 => Arc::new(Int8Array::from(vec![-1])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![-1])),
                _ => Arc::new(Float64Array::from(vec![-1.0])),
            };
            let arguments = [EvaluatedArgument::Scalar(&scalar)];
            // Nullable avoids a non-null selected-row scan before the actual
            // body. Selection itself retains no dense row storage.
            let mut instance = instance(physical(data_type, true));
            let control = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(usize::MAX), &arguments, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|units| *units < 256));
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                3
            );
        }
    }
}

#[cfg(test)]
mod shared_core_failure_tests {
    use super::*;
    use crate::{
        EvaluatedArgument, FunctionValueType, KernelDiagnostic, ScalarEvaluationInstance, Selection,
    };
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((wanted, error)) = &self.refusal
                && at == *wanted
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ABS cannot wait")
        }
    }
    fn failures() -> [KernelFailure; 7] {
        [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("ABS invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("ABS internal")),
            KernelFailure::Operational(KernelDiagnostic::new("ABS operational")),
            KernelFailure::InstanceFailed,
        ]
    }
    #[test]
    fn shared_abs_signed_gather_builder_preserves_every_checkpoint_cause_and_latch() {
        let scalar: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![i64::MIN + 1]));
        let source = FunctionValueType::new(DataType::Int64, false);
        let make = || {
            ScalarEvaluationInstance::instantiate(super::super::abs_owner::prepared_for_test(
                source.clone(),
            ))
            .unwrap()
        };
        let args = [EvaluatedArgument::Scalar(&scalar)];
        let selection = Selection::all(320);
        let control = Control::default();
        let mut baseline = make();
        let values = baseline.evaluate(selection, &args, &control).unwrap();
        assert!(values.errors().is_empty());
        let expected = control.calls.lock().unwrap().clone();
        assert!(expected.contains(&256));
        for failure in failures() {
            for at in 0..expected.len() {
                let control = Control {
                    calls: Mutex::new(Vec::new()),
                    refusal: Some((at, failure.clone())),
                };
                let mut instance = make();
                assert_eq!(
                    instance.evaluate(selection, &args, &control).unwrap_err(),
                    failure
                );
                assert_eq!(*control.calls.lock().unwrap(), expected[..=at]);
                let retry = Control::default();
                assert_eq!(
                    instance.evaluate(selection, &args, &retry).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(retry.calls.lock().unwrap().is_empty());
            }
        }
    }
}
