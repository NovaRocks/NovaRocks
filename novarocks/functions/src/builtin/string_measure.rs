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
//! ONE original byte/scalar measurement with explicit carrier projections.
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray,
    builder::{Int32Builder, StringBuilder},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringMeasureOp {
    Ascii,
    Bytes,
    Characters,
    Chr,
    Codepoint,
}
#[derive(Debug)]
enum MeasureFailure {
    Carrier(StringMeasureOp),
    Int32 {
        value: i64,
        source: std::num::TryFromIntError,
    },
    MissingReturnType,
    ReturnType(novarocks_failure_contract::BoundedDetail),
}
impl std::fmt::Display for MeasureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(StringMeasureOp::Ascii) => f.write_str("ascii expects string"),
            Self::Carrier(_) => f.write_str("length expects string"),
            Self::Int32 { value, .. } => write!(f, "length result out of INT range: {value}"),
            Self::MissingReturnType => f.write_str("length return type is missing"),
            Self::ReturnType(value) => write!(
                f,
                "length return type must be INT/BIGINT, got {}",
                value.as_str()
            ),
        }
    }
}
impl std::error::Error for MeasureFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Int32 { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MeasureFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        match self {
            Self::Int32 { .. } => crate::DataFailureReason::NumericOutOfRange.failure_fact(),
            _ => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(357).unwrap()),
            ),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for MeasureFailure {}
fn string_reader(array: &ArrayRef, op: StringMeasureOp) -> Result<&StringArray, KernelFailure> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelFailure::from_domain(MeasureFailure::Carrier(op)))
}
fn original_value(
    op: StringMeasureOp,
    text: &str,
    work: &mut Option<EvaluationCheckpoints<'_>>,
) -> Result<i64, KernelFailure> {
    Ok(match op {
        StringMeasureOp::Ascii => i64::from(text.as_bytes().first().copied().unwrap_or(0) as i32),
        StringMeasureOp::Bytes => text.len() as i64,
        StringMeasureOp::Characters => {
            // Keep the original chars().count() author. Observe its original
            // byte workload before the opaque library operation, preserving
            // the existing byte quantum rather than replacing its mathematics.
            if let Some(work) = work {
                for _ in text.as_bytes() {
                    work.step()?;
                }
            }
            text.chars().count() as i64
        }
        StringMeasureOp::Chr | StringMeasureOp::Codepoint => {
            return Err(invalid(
                "legacy string measurement cannot compute a Unicode codepoint",
            ));
        }
    })
}
fn compute_rows(
    op: StringMeasureOp,
    values: &StringArray,
    address: impl Fn(usize, usize) -> usize,
    selection: Selection<'_>,
    nullable: Option<bool>,
    work: &mut Option<EvaluationCheckpoints<'_>>,
    mut output: impl FnMut(Option<i64>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    for (ordinal, batch) in selection.iter().enumerate() {
        if let Some(work) = work {
            work.step()?;
        }
        let row = address(ordinal, batch);
        if nullable.is_some() && row >= values.len() {
            return Err(internal(
                "string measurement selected row is outside its checked carrier",
            ));
        }
        if values.is_null(row) {
            if nullable == Some(false) {
                return Err(internal(
                    "string measurement non-null input contains selected SQL NULL",
                ));
            }
            output(None)?;
            continue;
        }
        output(Some(original_value(op, values.value(row), work)?))?;
    }
    Ok(())
}
fn int32_value(value: i64) -> Result<i32, KernelFailure> {
    i32::try_from(value)
        .map_err(|source| KernelFailure::from_domain(MeasureFailure::Int32 { value, source }))
}
fn length_int32(values: Vec<Option<i64>>) -> Result<ArrayRef, KernelFailure> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(value.map(int32_value).transpose()?);
    }
    Ok(Arc::new(Int32Array::from(out)))
}
/// Original static failures are shared without constructing CPU diagnostics during admission.
#[derive(Clone, Copy)]
pub(super) enum StaticProfileFailure {
    Argument,
    Type,
}
impl StaticProfileFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Argument => "string measurement requires exactly one checked value argument",
            Self::Type => "string measurement differs from its exact installed profile",
        }
    }
}
pub(super) fn check_source(
    types: &[FunctionArgumentType],
    arguments: usize,
) -> Result<&crate::FunctionValueType, StaticProfileFailure> {
    if arguments != 1 {
        return Err(StaticProfileFailure::Argument);
    }
    let [FunctionArgumentType::Value(source)] = types else {
        return Err(StaticProfileFailure::Argument);
    };
    Ok(source)
}
pub(super) fn check_types(
    op: StringMeasureOp,
    source: &crate::FunctionValueType,
    target: &crate::FunctionValueType,
) -> Result<(), StaticProfileFailure> {
    let (source_matches, target_carrier, nullable) = match op {
        StringMeasureOp::Chr => (source.data_type() == &DataType::Int64, DataType::Utf8, true),
        StringMeasureOp::Codepoint => {
            (source.data_type() == &DataType::Utf8, DataType::Int32, true)
        }
        StringMeasureOp::Bytes if target.data_type() == &DataType::Int64 => (
            matches!(source.data_type(), DataType::Utf8 | DataType::Binary),
            DataType::Int64,
            source.nullable(),
        ),
        _ => (
            source.data_type() == &DataType::Utf8,
            DataType::Int32,
            op == StringMeasureOp::Ascii || source.nullable(),
        ),
    };
    if source.logical_type() != ValueLogicalType::Physical
        || !source_matches
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &target_carrier
        || target.nullable() != nullable
    {
        return Err(StaticProfileFailure::Type);
    }
    Ok(())
}

fn evaluate_trino_measurement<'a>(
    op: StringMeasureOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let argument = &input.arguments()[0];
        let array = argument.array();
        let selection = input.selection();
        let mut errors = Vec::new();
        let output: ArrayRef = match op {
            StringMeasureOp::Bytes => {
                let mut values = Vec::new();
                values
                    .try_reserve_exact(selection.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                match array.data_type() {
                    DataType::Utf8 => {
                        let strings = array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| internal("length selected source is not Utf8"))?;
                        let mut observed = Some(EvaluationCheckpoints::new(control));
                        compute_rows(
                            StringMeasureOp::Characters,
                            strings,
                            |ordinal, batch| argument.value_row(ordinal, batch),
                            selection,
                            Some(true),
                            &mut observed,
                            |value| {
                                values.push(value);
                                Ok(())
                            },
                        )?;
                        observed.take().unwrap().finish()?;
                    }
                    DataType::Binary => {
                        let binaries = array
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| internal("length selected source is not Binary"))?;
                        for (ordinal, batch) in selection.iter().enumerate() {
                            let row = argument.value_row(ordinal, batch);
                            if row >= binaries.len() {
                                return Err(internal(
                                    "length selected byte address is out of bounds",
                                ));
                            }
                            values.push(
                                binaries
                                    .is_valid(row)
                                    .then(|| binaries.value(row).len() as i64),
                            );
                            work.step()?;
                        }
                    }
                    _ => return Err(invalid("length differs from its checked carrier")),
                }
                Arc::new(Int64Array::from(values))
            }
            StringMeasureOp::Chr => {
                let positions = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| internal("chr selected source is not Int64"))?;
                let bytes = selection
                    .len()
                    .checked_mul(4)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
                let mut strings = StringBuilder::with_capacity(selection.len(), bytes);
                for (ordinal, batch) in selection.iter().enumerate() {
                    let row = argument.value_row(ordinal, batch);
                    if row >= positions.len() {
                        return Err(internal("chr selected address is out of bounds"));
                    }
                    if positions.is_null(row) {
                        strings.append_null();
                    } else if let Some(ch) = unicode_scalar(positions.value(row)) {
                        let mut encoded = [0u8; 4];
                        strings.append_value(ch.encode_utf8(&mut encoded));
                    } else {
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(crate::RowDataError::with_reason(ordinal,
                        crate::DataFailureReason::Domain(novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT), "Not a valid Unicode code point"));
                        strings.append_null();
                    }
                    work.step()?;
                }
                Arc::new(strings.finish())
            }
            StringMeasureOp::Codepoint => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("codepoint selected source is not Utf8"))?;
                output_capacity(selection.len())?;
                let mut values = Int32Builder::with_capacity(selection.len());
                for (ordinal, batch) in selection.iter().enumerate() {
                    let row = argument.value_row(ordinal, batch);
                    if row >= strings.len() {
                        return Err(internal("codepoint selected address is out of bounds"));
                    }
                    if strings.is_null(row) {
                        values.append_null();
                    } else if let Some(value) = single_codepoint(strings.value(row)) {
                        values.append_value(value);
                    } else {
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(crate::RowDataError::with_reason(ordinal,
                        crate::DataFailureReason::Domain(novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT), "Input string must be a single character string"));
                        values.append_null();
                    }
                    work.step()?;
                }
                Arc::new(values.finish())
            }
            _ => return Err(invalid("unexpected Trino string measurement operation")),
        };
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            output,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

fn unicode_scalar(value: i64) -> Option<char> {
    u32::try_from(value).ok().and_then(char::from_u32)
}

fn single_codepoint(text: &str) -> Option<i32> {
    let mut characters = text.chars();
    let first = characters.next()?;
    characters.next().is_none().then_some(first as i32)
}

pub(super) fn evaluate_string_measure<'a>(
    op: StringMeasureOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let source = check_source(
        input.contract().selected().argument_types.as_ref(),
        input.arguments().len(),
    )
    .map_err(|error| invalid(error.message()))?;
    let argument = &input.arguments()[0];
    let target = input.contract().result_type();
    check_types(op, source, target).map_err(|error| invalid(error.message()))?;
    if matches!(op, StringMeasureOp::Chr | StringMeasureOp::Codepoint)
        || (op == StringMeasureOp::Bytes && target.data_type() == &DataType::Int64)
    {
        return evaluate_trino_measurement(op, input, control);
    }
    if argument.array().data_type() != &DataType::Utf8 {
        return Err(internal(
            "string measurement carrier differs from its checked argument",
        ));
    }
    let values = string_reader(argument.array(), op)
        .map_err(|_| internal("string measurement selected Utf8 carrier cannot be downcast"))?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut work = Some(EvaluationCheckpoints::new(control));
    let mut builder = Int32Builder::with_capacity(selection.len());
    compute_rows(
        op,
        values,
        |ordinal, batch| argument.value_row(ordinal, batch),
        selection,
        Some(source.nullable()),
        &mut work,
        |value| {
            match value {
                Some(value) => builder.append_value(int32_value(value)?),
                None => builder.append_null(),
            };
            Ok(())
        },
    )?;
    let output = Arc::new(builder.finish()) as ArrayRef;
    work.take().unwrap().finish()?;
    SelectedValues::try_new(selection, target.data_type(), output, Box::default())
        .map_err(|_| internal("string measurement compact output violates its selected contract"))
}
/// V1 retains its original source length and ignores the result type.
pub fn evaluate_required_ascii(array: &ArrayRef) -> Result<ArrayRef, KernelFailure> {
    let values = string_reader(array, StringMeasureOp::Ascii)?;
    let mut rows = Vec::with_capacity(array.len());
    compute_rows(
        StringMeasureOp::Ascii,
        values,
        |_, batch| batch,
        Selection::all(array.len()),
        None,
        &mut None,
        |value| {
            rows.push(value.map(int32_value).transpose()?);
            Ok(())
        },
    )?;
    Ok(Arc::new(Int32Array::from(rows)))
}
/// V1 computes all input rows before checking its original requested carrier.
pub fn evaluate_required_length(
    array: &ArrayRef,
    characters: bool,
    target: Option<&DataType>,
) -> Result<ArrayRef, KernelFailure> {
    let op = if characters {
        StringMeasureOp::Characters
    } else {
        StringMeasureOp::Bytes
    };
    let values = string_reader(array, op)?;
    let mut rows = Vec::with_capacity(array.len());
    compute_rows(
        op,
        values,
        |_, batch| batch,
        Selection::all(array.len()),
        None,
        &mut None,
        |value| {
            rows.push(value);
            Ok(())
        },
    )?;
    match target.ok_or_else(|| KernelFailure::from_domain(MeasureFailure::MissingReturnType))? {
        DataType::Int32 => length_int32(rows),
        DataType::Int64 => Ok(Arc::new(arrow_array::Int64Array::from(rows))),
        other => Err(KernelFailure::from_domain(MeasureFailure::ReturnType(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "{other:?}"
            )),
        ))),
    }
}
/// Allocation representability only; the host owns formal memory admission.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(4)
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
    use crate::{
        ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType,
        ScalarEvaluationInstance, Selection,
    };
    use arrow_array::{Int32Array, Int64Array};
    use novarocks_type_contract::{CompilePhase, DecimalOverflowPolicy};
    use std::{sync::Mutex, time::Duration};

    const FUNCTIONS: [&str; 3] = ["ascii", "length", "char_length"];
    // Independent expected byte codes, UTF-8 byte lengths and scalar counts.
    // Combining marks and joiners count as scalars, not grapheme clusters.
    const EXPECTED: [(&str, [i32; 3]); 9] = [
        ("", [0, 0, 0]),
        ("A", [65, 1, 1]),
        ("你好", [228, 6, 2]),
        ("e\u{301}", [101, 3, 2]),
        ("🦀", [240, 4, 1]),
        ("👩‍💻", [240, 11, 3]),
        ("\0x", [0, 2, 2]),
        ("é", [195, 2, 1]),
        ("�", [239, 3, 1]),
    ];

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
        fn refusing(at: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::default(),
                refusal: Some((at, error)),
            }
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((index, error)) = &self.refusal
                && *index == at
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("string measurements must not wait");
        }
    }
    fn source(nullable: bool) -> FunctionValueType {
        FunctionValueType::new(DataType::Utf8, nullable)
    }
    fn instance(name: &str, nullable: bool) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate(
            super::super::string_measure_owner::prepared_for_test(name, &source(nullable)).unwrap(),
        )
        .unwrap()
    }
    fn strings(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }
    fn output(array: &ArrayRef) -> Vec<Option<i32>> {
        assert_eq!(array.data_type(), &DataType::Int32);
        array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn pool(array: ArrayRef) -> ConstantPool {
        let ty = source(true);
        let rows = u64::try_from(array.len()).unwrap();
        ConstantPool::try_new(
            Arc::new(ty.try_to_field("measured").unwrap()),
            ty,
            array.to_data(),
            ConstantPolicy {
                max_rows: rows,
                max_array_nodes: 1,
                max_logical_elements: rows,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 1,
                max_type_nodes: 1,
                max_dictionary_depth: 0,
                max_metadata_bytes: 1024,
                max_library_validation_work: 8192,
                max_library_validation_bytes: 8192,
            },
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap()
    }

    #[test]
    fn exact_three_functions_keep_independent_unicode_and_byte_oracles() {
        let array = strings(EXPECTED.iter().map(|(text, _)| Some(*text)).collect());
        let args = [EvaluatedArgument::Column(&array)];
        for (index, name) in FUNCTIONS.into_iter().enumerate() {
            for nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let prepared =
                        super::super::string_measure_owner::prepared_for_test_with_policy(
                            name,
                            &source(nullable),
                            policy,
                        )
                        .unwrap();
                    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                    assert_eq!(kernel.contract().decimal_overflow_policy(), policy);
                    let ty = kernel.contract().result_type();
                    assert_eq!(ty.logical_type(), ValueLogicalType::Physical);
                    assert_eq!(ty.data_type().clone(), DataType::Int32);
                    assert_eq!(ty.nullable(), name == "ascii" || nullable);
                    let result = kernel
                        .evaluate(Selection::all(EXPECTED.len()), &args, &Control::default())
                        .unwrap();
                    assert!(result.errors().is_empty());
                    assert_eq!(
                        output(result.values()),
                        EXPECTED.map(|(_, values)| Some(values[index]))
                    );
                }
            }
        }
    }

    #[test]
    fn sparse_sliced_and_compact_arguments_keep_selected_mapping() {
        let backing = strings(vec![
            Some("discard"),
            Some("你好"),
            None,
            Some("🦀"),
            Some("discard"),
        ]);
        let sliced = backing.slice(1, 3);
        let rows = [0, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let compact = SelectedValues::try_new(
            selection,
            &DataType::Utf8,
            strings(vec![Some("你好"), Some("🦀")]),
            Box::default(),
        )
        .unwrap();
        for (index, name) in FUNCTIONS.into_iter().enumerate() {
            for argument in [
                EvaluatedArgument::Column(&sliced),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                let mut kernel = instance(name, false);
                let args = [argument];
                let result = kernel
                    .evaluate(selection, &args, &Control::default())
                    .unwrap();
                assert_eq!(result.selection(), selection);
                assert_eq!(
                    output(result.values()),
                    [Some([228, 6, 2][index]), Some([240, 4, 1][index])]
                );
                assert!(result.errors().is_empty());
            }
        }
    }

    #[test]
    fn scalar_and_nonzero_constant_ordinal_broadcast_without_pool_slicing() {
        let scalar = strings(vec![Some("e\u{301}")]);
        let constants = pool(strings(vec![None, Some("unselected"), Some("e\u{301}")]));
        let constant = constants.value(2).unwrap();
        assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
        for (index, name) in FUNCTIONS.into_iter().enumerate() {
            for argument in [
                EvaluatedArgument::Scalar(&scalar),
                EvaluatedArgument::Constant(&constant),
            ] {
                let mut kernel = instance(name, true);
                let args = [argument];
                let result = kernel
                    .evaluate(Selection::all(4), &args, &Control::default())
                    .unwrap();
                assert_eq!(output(result.values()), vec![Some([101, 3, 2][index]); 4]);
            }
            let null = constants.value(0).unwrap();
            let args = [EvaluatedArgument::Constant(&null)];
            let mut kernel = instance(name, true);
            let result = kernel
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap();
            assert_eq!(output(result.values()), [None; 3]);
            assert!(result.errors().is_empty());
        }
    }

    #[test]
    fn strict_null_and_nonnullable_contradiction_keep_outer_failure_and_latch() {
        let array = strings(vec![None, Some(""), Some("A")]);
        let args = [EvaluatedArgument::Column(&array)];
        for name in FUNCTIONS {
            let mut nullable = instance(name, true);
            let result = nullable
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap();
            assert_eq!(
                output(result.values()),
                [None, Some(0), Some(1 + i32::from(name == "ascii") * 64)]
            );
            assert!(result.errors().is_empty());
            let mut nonnull = instance(name, false);
            assert!(matches!(
                nonnull.evaluate(Selection::all(3), &args, &Control::default()),
                Err(ref cause) if crate::kernel_control::test_contract::input(cause)
            ));
            let after = Control::default();
            assert_eq!(
                nonnull
                    .evaluate(Selection::all(0), &[], &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.calls().is_empty());
        }
    }

    #[test]
    fn only_exact_utf8_profile_and_checked_argument_shapes_are_consumed() {
        let short = strings(vec![Some("A")]);
        let wrong: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        for name in FUNCTIONS {
            for bad in [
                FunctionValueType::new(DataType::LargeUtf8, false),
                FunctionValueType::new(DataType::Binary, false),
                FunctionValueType::new(DataType::Int64, false),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ] {
                assert!(super::super::string_measure_owner::prepared_for_test(name, &bad).is_err());
            }
            for array in [&short, &wrong] {
                let mut kernel = instance(name, false);
                let args = [EvaluatedArgument::Column(array)];
                assert!(matches!(
                    kernel.evaluate(Selection::all(2), &args, &Control::default()),
                    Err(ref cause) if crate::kernel_control::test_contract::input(cause)
                ));
            }
            let mut kernel = instance(name, false);
            assert!(matches!(
                kernel.evaluate(Selection::all(1), &[], &Control::default()),
                Err(ref cause) if crate::kernel_control::test_contract::input(cause)
            ));
        }
    }

    #[test]
    fn foreign_selection_and_child_row_errors_cannot_be_collapsed_into_null() {
        let rows = [0];
        let other_rows = [1];
        let selection = Selection::try_sparse(2, &rows).unwrap();
        let other = Selection::try_sparse(2, &other_rows).unwrap();
        let wrong = SelectedValues::try_new(
            other,
            &DataType::Utf8,
            strings(vec![Some("A")]),
            Box::default(),
        )
        .unwrap();
        let poisoned = SelectedValues::try_new(
            selection,
            &DataType::Utf8,
            strings(vec![None]),
            vec![crate::RowDataError::new(0, "child failed")].into_boxed_slice(),
        )
        .unwrap();
        for name in FUNCTIONS {
            for compact in [&wrong, &poisoned] {
                let mut kernel = instance(name, true);
                let args = [EvaluatedArgument::SelectedColumn(compact)];
                assert!(matches!(
                    kernel.evaluate(selection, &args, &Control::default()),
                    Err(ref cause) if crate::kernel_control::test_contract::input(cause)
                ));
            }
        }
    }

    #[test]
    fn character_byte_quanta_tail_and_publication_preserve_typed_refusal() {
        let text = "é".repeat(300);
        let array = strings(vec![Some(&text)]);
        let args = [EvaluatedArgument::Scalar(&array)];
        let trace = Control::default();
        let mut kernel = instance("char_length", false);
        assert_eq!(
            output(
                kernel
                    .evaluate(Selection::all(1), &args, &trace)
                    .unwrap()
                    .values()
            ),
            [Some(300)]
        );
        let calls = trace.calls();
        assert_eq!(calls.iter().filter(|units| **units == 256).count(), 2);
        assert!(calls.iter().any(|units| *units > 0 && *units < 256));
        assert_eq!(calls.last(), Some(&0));
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in 0..calls.len() {
                let control = Control::refusing(at, error.clone());
                let mut kernel = instance("char_length", false);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(control.calls(), calls[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.calls().is_empty());
            }
        }
    }

    #[test]
    fn ascii_and_bytes_do_not_scan_text_and_unselected_text_is_not_visited() {
        let long = "é".repeat(10_000);
        let long_scalar = strings(vec![Some(&long)]);
        let short_scalar = strings(vec![Some("é")]);
        for (name, expected) in [("ascii", 195), ("length", 20_000)] {
            let mut traces = Vec::new();
            for array in [&long_scalar, &short_scalar] {
                let mut kernel = instance(name, false);
                let trace = Control::default();
                let args = [EvaluatedArgument::Scalar(array)];
                let result = kernel.evaluate(Selection::all(1), &args, &trace).unwrap();
                if Arc::ptr_eq(array, &long_scalar) {
                    assert_eq!(output(result.values()), [Some(expected)]);
                }
                traces.push(trace.calls());
            }
            assert_eq!(traces[0], traces[1]);
            assert!(!traces[0].contains(&256));
        }
        let wide = strings(vec![Some(&long), Some("A"), None]);
        let short = strings(vec![Some(""), Some("A"), Some("")]);
        let rows = [1];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        for name in FUNCTIONS {
            let mut traces = Vec::new();
            for array in [&wide, &short] {
                let mut kernel = instance(name, false);
                let args = [EvaluatedArgument::Column(array)];
                let trace = Control::default();
                let result = kernel.evaluate(selection, &args, &trace).unwrap();
                assert_eq!(
                    output(result.values()),
                    [Some(if name == "ascii" { 65 } else { 1 })]
                );
                traces.push(trace.calls());
            }
            assert_eq!(traces[0], traces[1]);
            assert!(!traces[0].contains(&256));
        }
    }

    #[test]
    fn all_functions_observe_actual_row_quanta_and_refuse_without_publication() {
        let array = strings(vec![Some(""); 321]);
        let args = [EvaluatedArgument::Column(&array)];
        // Nullable input avoids the non-null selected-row validation loop;
        // the positive quantum here is the actual body row loop.
        for name in FUNCTIONS {
            let trace = Control::default();
            let mut kernel = instance(name, true);
            assert_eq!(
                output(
                    kernel
                        .evaluate(Selection::all(321), &args, &trace)
                        .unwrap()
                        .values()
                ),
                vec![Some(0); 321]
            );
            let calls = trace.calls();
            let at = calls.iter().position(|units| *units == 256).unwrap();
            for error in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let control = Control::refusing(at, error.clone());
                let mut kernel = instance(name, true);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(321), &args, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(control.calls(), calls[..=at]);
            }
        }
    }

    #[test]
    fn batch_partition_preserves_stateless_measurements_and_nulls() {
        let array = strings(vec![Some("你好"), None, Some(""), Some("🦀")]);
        for name in FUNCTIONS {
            let mut full = instance(name, true);
            let args = [EvaluatedArgument::Column(&array)];
            let expected = output(
                full.evaluate(Selection::all(4), &args, &Control::default())
                    .unwrap()
                    .values(),
            );
            let mut partitioned = instance(name, true);
            let mut actual = Vec::new();
            for (offset, len) in [(0, 1), (1, 2), (3, 1)] {
                let slice = array.slice(offset, len);
                let args = [EvaluatedArgument::Column(&slice)];
                actual.extend(output(
                    partitioned
                        .evaluate(Selection::all(len), &args, &Control::default())
                        .unwrap()
                        .values(),
                ));
            }
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn empty_demand_skips_body_and_capacity_overflow_refuses_before_allocation() {
        let null = strings(vec![None]);
        let args = [EvaluatedArgument::Scalar(&null)];
        for name in FUNCTIONS {
            let trace = Control::default();
            let mut kernel = instance(name, false);
            let result = kernel.evaluate(Selection::all(0), &args, &trace).unwrap();
            assert!(result.values().is_empty());
            assert!(result.errors().is_empty());
            assert_eq!(trace.calls().iter().filter(|units| **units == 0).count(), 2);
            let mut kernel = instance(name, true);
            let trace = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(usize::MAX), &args, &trace)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(!trace.calls().contains(&256));
        }
        assert_eq!(
            output_capacity(usize::MAX).unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert_eq!(
            output_capacity((isize::MAX as usize / 4) + 1).unwrap_err(),
            KernelFailure::ResourceExhausted
        );
    }
}

#[cfg(test)]
#[path = "string_measure_shared_control_tests.rs"]
mod shared_control_tests;

#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn direct_carrier_and_frozen_return_guards_do_not_retain_arrays_or_field_metadata() {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let weak = Arc::downgrade(&source);
        let failure = evaluate_required_ascii(&source).unwrap_err();
        drop(source);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 357);
        assert_eq!(failure.to_string(), "ascii expects string");
        let field = Arc::new(arrow_schema::Field::new(
            "ResourceExhausted",
            DataType::Int32,
            true,
        ));
        let weak_field = Arc::downgrade(&field);
        let target = DataType::Struct(arrow_schema::Fields::from(vec![field]));
        let source: ArrayRef = Arc::new(StringArray::from(vec!["🦀"]));
        let weak = Arc::downgrade(&source);
        let failure = evaluate_required_length(&source, true, Some(&target)).unwrap_err();
        drop(source);
        drop(target);
        assert!(weak.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 357);
        assert!(failure.failure_fact().resource().is_none());
    }
}
