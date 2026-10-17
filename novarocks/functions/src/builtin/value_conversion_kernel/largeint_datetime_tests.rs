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

//! Real selected checked narrowing followed by the existing prepared calendar.
use super::*;
use crate::builtin::value_conversion::conversion_intermediate_type;
use crate::{CastOperation, CastRowResult, PreparedCastRecipe};
fn target() -> FunctionValueType {
    physical(
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        true,
    )
}
fn compose(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<Vec<CastRowResult>, KernelFailure> {
    let source = large_type(true);
    let target = target();
    let intermediate = conversion_intermediate_type(&source, &target)
        .unwrap()
        .unwrap();
    let mut narrowing =
        ScalarEvaluationInstance::instantiate(prepared(&source, &intermediate)).unwrap();
    let calendar = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &intermediate,
        &target,
        DecimalOverflowPolicy::ReportError,
        true,
        &CompileControl::default(),
    )
    .unwrap();
    let arguments = [argument];
    let narrowed = narrowing.evaluate(selection, &arguments, control)?;
    assert!(narrowed.errors().is_empty());
    let mut result = Vec::new();
    for (ordinal, row) in selection.iter().enumerate() {
        result.push(calendar.evaluate_row(
            EvaluatedArgument::SelectedColumn(&narrowed),
            ordinal,
            row,
            control,
        )?);
    }
    Ok(result)
}
#[test]
fn largeint_datetime_real_two_stage_recipe_preserves_checked_narrowing_and_calendar_null_grammar() {
    let input = large(&[
        Some(-18446744073709551494),
        Some(18446744073709551615),
        Some(i128::MIN),
        Some(i128::MAX),
        Some(i64::MIN as i128),
        Some(i64::MAX as i128),
        Some(0),
        Some(1),
        Some(19700101000000),
        Some(20240229123456),
        Some(20240230000000),
        None,
    ]);
    let output = compose(
        EvaluatedArgument::Column(&input),
        Selection::all(12),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        output,
        vec![
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Timestamp(0),
            CastRowResult::Timestamp(1709210096000000),
            CastRowResult::Null,
            CastRowResult::Null
        ]
    );
}
#[test]
fn largeint_datetime_sparse_scalar_and_exact_constant_ordinal_keep_real_addresses() {
    let values = large(&[
        Some(18446744073709551615),
        Some(20240229123456),
        None,
        Some(19700101000000),
    ]);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::FixedSizeBinary(16),
        values.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    let expected = vec![
        CastRowResult::Timestamp(1709210096000000),
        CastRowResult::Null,
        CastRowResult::Timestamp(0),
    ];
    for argument in [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        assert_eq!(
            compose(argument, selection, &Control::default()).unwrap(),
            expected
        );
    }
    let source = large_type(true);
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("actual").unwrap()),
        source,
        values.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    assert_eq!(value.ordinal(), 1);
    let scalar = values.slice(1, 1);
    for argument in [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        assert_eq!(
            compose(argument, Selection::all(2), &Control::default()).unwrap(),
            vec![CastRowResult::Timestamp(1709210096000000); 2]
        );
    }
    let null = pool.value(2).unwrap();
    assert_eq!(
        compose(
            EvaluatedArgument::Constant(&null),
            Selection::all(2),
            &Control::default()
        )
        .unwrap(),
        vec![CastRowResult::Null; 2]
    );
}
#[test]
fn largeint_datetime_real_both_stage_callbacks_preserve_first_typed_failure_without_tail() {
    let input = large(&[Some(20240229123456), Some(18446744073709551615)]);
    let baseline = Control::default();
    compose(
        EvaluatedArgument::Column(&input),
        Selection::all(2),
        &baseline,
    )
    .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                compose(
                    EvaluatedArgument::Column(&input),
                    Selection::all(2),
                    &control
                )
                .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
