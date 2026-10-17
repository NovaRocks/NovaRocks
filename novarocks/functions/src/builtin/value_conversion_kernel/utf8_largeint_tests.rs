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
//! Exact Utf8 -> nominal LARGEINT, preserving the original trim/i128 grammar.
use super::*;
use crate::builtin::value_conversion::{conversion_intermediate_type, resolve_value_conversion};

#[test]
fn utf8_largeint_exact_nullable_family_keeps_complete_binding_and_rejects_other_domains() {
    for nullable in [false, true] {
        let source = physical(DataType::Utf8, nullable);
        let target = large_type(true);
        let selected =
            resolve_value_conversion(&source, &target, &CompileControl::default()).unwrap();
        assert_eq!(
            selected.overload.as_str(),
            "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"
        );
        assert_eq!(
            selected.argument_types.as_ref(),
            &[crate::FunctionArgumentType::Value(source.clone())]
        );
        assert_eq!(
            selected.result_type,
            crate::FunctionResultType::Scalar(target.clone())
        );
        assert_eq!(
            conversion_intermediate_type(&source, &target).unwrap(),
            Some(target)
        );
        assert!(
            resolve_value_conversion(&source, &large_type(false), &CompileControl::default())
                .is_err()
        );
    }
    let utf8 = physical(DataType::Utf8, true);
    let large = large_type(true);
    for source in [
        physical(DataType::LargeUtf8, true),
        physical(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(resolve_value_conversion(&source, &large, &CompileControl::default()).is_err());
    }
    for target in [
        physical(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ] {
        assert!(resolve_value_conversion(&utf8, &target, &CompileControl::default()).is_err());
    }
}

#[test]
fn utf8_largeint_original_parse_grammar_and_full_i128_range_are_successful_null_or_exact_values() {
    let source = physical(DataType::Utf8, true);
    let target = large_type(true);
    // Hand-authored expected values, including numbers impossible through i64.
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("-"),
        Some("+"),
        Some(""),
        Some("  "),
        Some("42"),
        Some("-17"),
        Some("  9000 "),
        Some("9223372036854775808"),
        Some("170141183460469231731687303715884105727"),
        Some("-170141183460469231731687303715884105728"),
        Some("170141183460469231731687303715884105728"),
        Some("-170141183460469231731687303715884105729"),
        Some("1_000"),
        Some("1e3"),
        Some("0x10"),
        Some("--1"),
        Some("+0012"),
        Some("\u{2003}-17\u{2003}"),
        None,
    ]));
    let expected = large(&[
        None,
        None,
        None,
        None,
        Some(42),
        Some(-17),
        Some(9000),
        Some(9223372036854775808),
        Some(i128::MAX),
        Some(i128::MIN),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(12),
        Some(-17),
        None,
    ]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let prepared = super::super::super::value_conversion_owner::prepared_for_test_with_policy(
            &source, &target, policy,
        )
        .unwrap();
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let arguments = [EvaluatedArgument::Column(&input)];
        let result = instance
            .evaluate(Selection::all(input.len()), &arguments, &Control::default())
            .unwrap();
        assert!(
            result.errors().is_empty(),
            "parse failure is successful NULL, not RowData or suppressed control"
        );
        assert_eq!(result.values().to_data(), expected.to_data());
    }
}

#[test]
fn utf8_largeint_dense_sparse_compact_scalar_and_checked_constant_use_exact_original_rows() {
    let source = physical(DataType::Utf8, true);
    let target = large_type(true);
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("poison"),
        Some("9223372036854775808"),
        None,
        Some("-170141183460469231731687303715884105728"),
    ]));
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        source.data_type(),
        values.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    let expected = large(&[Some(9223372036854775808), None, Some(i128::MIN)]);
    for argument in [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        assert_eq!(
            run(&source, &target, argument, selection).to_data(),
            expected.to_data()
        );
    }
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("actual").unwrap()),
        source.clone(),
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
    let value = pool.value(3).unwrap();
    assert_eq!(value.ordinal(), 3);
    let scalar = values.slice(3, 1);
    for argument in [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        assert_eq!(
            run(&source, &target, argument, Selection::all(2)).to_data(),
            large(&[Some(i128::MIN); 2]).to_data()
        );
    }
    let null = pool.value(2).unwrap();
    assert_eq!(
        run(
            &source,
            &target,
            EvaluatedArgument::Constant(&null),
            Selection::all(2)
        )
        .to_data(),
        large(&[None, None]).to_data()
    );
    assert_eq!(
        run(
            &source,
            &target,
            EvaluatedArgument::Column(&values),
            Selection::try_sparse(4, &[]).unwrap()
        )
        .len(),
        0
    );
}

#[test]
fn utf8_largeint_every_real_runtime_callback_preserves_first_typed_cause_and_failed_replay() {
    let source = physical(DataType::Utf8, true);
    let target = large_type(true);
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("  -170141183460469231731687303715884105728  "),
        Some("+"),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let baseline = Control::default();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
    instance
        .evaluate(Selection::all(3), &arguments, &baseline)
        .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in failures() {
            let c = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut instance =
                ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
            assert_eq!(
                instance
                    .evaluate(Selection::all(3), &arguments, &c)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            let replay = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(3), &arguments, &replay)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(replay.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn utf8_largeint_every_real_binding_callback_preserves_compile_control_cause() {
    let source = physical(DataType::Utf8, false);
    let target = large_type(true);
    let baseline = CompileControl::default();
    resolve_value_conversion(&source, &target, &baseline).unwrap();
    let count = baseline.trace.lock().unwrap().len();
    assert!(count > 0);
    for at in 0..count {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = CompileControl {
                refusal: Some((at, cause)),
                ..Default::default()
            };
            assert_eq!(
                resolve_value_conversion(&source, &target, &c),
                Err(crate::FunctionBindingError::Control(cause))
            );
            assert_eq!(c.trace.lock().unwrap().len(), at + 1);
        }
    }
}
