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
//! Accurate nominal text conversion, preserving carrier-cast and UUID gates.
use super::*;
use crate::builtin::value_conversion::{conversion_intermediate_type, resolve_value_conversion};
#[test]
fn nominal_largeint_text_exact_binding_field_target_and_unrelated_domains() {
    for nullable in [false, true] {
        let source = large_type(nullable);
        let target = physical(DataType::Utf8, nullable);
        let selected =
            resolve_value_conversion(&source, &target, &CompileControl::default()).unwrap();
        assert_eq!(
            selected.overload.as_str(),
            "builtin.scalar/value_domain_conversion/largeint_to_utf8_text/v1"
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
        let field = source.try_to_field("authored-largeint").unwrap();
        assert_eq!(field.metadata()[NR_LOGICAL_TYPE_KEY], "largeint");
        assert_eq!(FunctionValueType::try_from_field(&field).unwrap(), source);
    }
    let target = physical(DataType::Utf8, true);
    for source in [
        physical(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(resolve_value_conversion(&source, &target, &CompileControl::default()).is_err());
    }
    assert!(
        resolve_value_conversion(
            &large_type(true),
            &physical(DataType::Utf8, false),
            &CompileControl::default()
        )
        .is_err()
    );
    assert!(
        resolve_value_conversion(
            &large_type(true),
            &physical(DataType::LargeUtf8, true),
            &CompileControl::default()
        )
        .is_err()
    );
    let good = CompileControl::default();
    resolve_value_conversion(&large_type(true), &target, &good).unwrap();
    let count = good.trace.lock().unwrap().len();
    for stop in 0..count {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = CompileControl {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                resolve_value_conversion(&large_type(true), &target, &c),
                Err(crate::FunctionBindingError::Control(cause))
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
#[test]
fn nominal_largeint_text_selected_scalar_constant_ordinal_and_null_preserve_values() {
    let source = large_type(true);
    let target = physical(DataType::Utf8, true);
    let values = large(&[Some(999), Some(i128::MIN), None, Some(i128::MAX)]);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        source.data_type(),
        values.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    let expected = StringArray::from(vec![
        Some("-170141183460469231731687303715884105728"),
        None,
        Some("170141183460469231731687303715884105727"),
    ]);
    for arg in [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        assert_eq!(
            run(&source, &target, arg, selection).to_data(),
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
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap();
    let value = pool.value(3).unwrap();
    let scalar = values.slice(3, 1);
    for arg in [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        assert_eq!(
            run(&source, &target, arg, Selection::all(2)).to_data(),
            StringArray::from(vec!["170141183460469231731687303715884105727"; 2]).to_data()
        );
    }
    assert_eq!(
        run(
            &source,
            &target,
            EvaluatedArgument::Column(&values),
            Selection::try_sparse(4, &[]).unwrap()
        )
        .to_data(),
        StringArray::from(Vec::<Option<&str>>::new()).to_data()
    );
    let nonnull = large(&[Some(-1)]);
    assert_eq!(
        run(
            &large_type(false),
            &physical(DataType::Utf8, false),
            EvaluatedArgument::Column(&nonnull),
            Selection::all(1)
        )
        .to_data(),
        StringArray::from(vec!["-1"]).to_data()
    );
}
#[test]
fn nominal_largeint_text_every_runtime_callback_keeps_original_cause_and_latch() {
    let source = large_type(true);
    let target = physical(DataType::Utf8, true);
    let values = large(&[Some(i128::MIN), Some(42)]);
    let args = [EvaluatedArgument::Column(&values)];
    let good = Control::default();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
    instance.evaluate(Selection::all(2), &args, &good).unwrap();
    let count = good.trace.lock().unwrap().len();
    for stop in 0..count {
        for cause in failures() {
            let mut instance =
                ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
            let c = Control {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                instance.evaluate(Selection::all(2), &args, &c).unwrap_err(),
                cause
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
