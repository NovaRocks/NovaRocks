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
use super::*;
use crate::{
    EvaluatedArgument, FunctionValueType, KernelEvaluationControl, ScalarEvaluationInstance,
    kernel_control::{internal, invalid},
};
use arrow_array::{LargeBinaryArray, LargeStringArray, StructArray};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    reject: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("MD5 calculation never waits")
    }
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.reject {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = &self.reject
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
}
fn prepared(name: &str, sources: &[FunctionValueType]) -> Arc<dyn crate::PreparedScalarKernel> {
    match name {
        "md5" => super::super::string_md5_owner::prepared_for_test_with_policy(
            name,
            sources,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        )
        .unwrap(),
        "md5sum" => super::super::md5sum_owner::tests::prepared_for_test(name, sources).unwrap(),
        "md5sum_numeric" => {
            super::super::md5sum_numeric_owner::tests::prepared_for_test(name, sources).unwrap()
        }
        _ => panic!("uninstalled test owner"),
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn md5_family_actual_instances_keep_seven_causes_at_every_checkpoint_without_replay() {
    for name in ["md5", "md5sum", "md5sum_numeric"] {
        let text = "aé\0".repeat(129);
        let first = Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            None,
            Some("hello"),
            Some(""),
        ])) as ArrayRef;
        let second = Arc::new(LargeBinaryArray::from(vec![
            Some(b"prefix".as_slice()),
            Some(b"masked".as_slice()),
            Some(text.as_bytes()),
            Some(b"".as_slice()),
        ])) as ArrayRef;
        let arrays = if name == "md5" {
            vec![first]
        } else {
            vec![first, second]
        };
        let sources = arrays
            .iter()
            .map(|array| {
                FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true)
            })
            .collect::<Vec<_>>();
        let prepared = prepared(name, &sources);
        let rows = [0, 1, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let arguments = arrays
            .iter()
            .map(EvaluatedArgument::Column)
            .collect::<Vec<_>>();
        let probe = Control::default();
        ScalarEvaluationInstance::instantiate(prepared.clone())
            .unwrap()
            .evaluate(selection, &arguments, &probe)
            .unwrap();
        let trace = probe.trace.into_inner().unwrap();
        assert!(trace.len() > 8);
        assert!(trace.contains(&256));
        for cause in causes() {
            for at in 0..trace.len() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    reject: Some((at, cause.clone())),
                };
                let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn md5_family_selected_compact_and_scalar_addresses_skip_null_without_delimiters() {
    for name in ["md5sum", "md5sum_numeric"] {
        let first = Arc::new(LargeStringArray::from(vec![
            "inactive", "a", "inactive", "hello",
        ])) as ArrayRef;
        let compact = Arc::new(Int64Array::from(vec![Some(42), None])) as ArrayRef;
        let scalar = Arc::new(BinaryArray::from(vec![b"b".as_slice()])) as ArrayRef;
        let sources = [
            FunctionValueType::new(DataType::LargeUtf8, false),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Binary, false),
        ];
        let rows = [1, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact_values =
            crate::SelectedValues::try_new(selection, &DataType::Int64, compact, Box::default())
                .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&first),
            EvaluatedArgument::SelectedColumn(&compact_values),
            EvaluatedArgument::Scalar(&scalar),
        ];
        let output = ScalarEvaluationInstance::instantiate(prepared(name, &sources))
            .unwrap()
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        let input = to_owned_bytes_array_with_varchar_cast(
            Arc::new(StringArray::from(vec!["a42b", "hellob"])),
            name,
            0,
        )
        .unwrap();
        let op = if name == "md5sum" {
            Operation::Md5sum
        } else {
            Operation::Md5sumNumeric
        };
        let target = if name == "md5sum" {
            DataType::Utf8
        } else {
            DataType::FixedSizeBinary(16)
        };
        let expected = evaluate_required(op, &[input], 2, Some(&target)).unwrap();
        assert_eq!(output.values().to_data(), expected.to_data());
        assert!(output.errors().is_empty());
    }
}
#[test]
fn required_digest_failure_preserves_full_sdk_source_and_bounded_original_prefix() {
    let input = to_owned_bytes_array(
        Arc::new(StringArray::from(vec!["hello"])),
        "md5sum_numeric",
        0,
    )
    .unwrap();
    let target = DataType::Struct(
        vec![Arc::new(arrow_schema::Field::new(
            "long".repeat(180),
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let raw = evaluate_required(
        Operation::Md5sumNumeric,
        std::slice::from_ref(&input),
        1,
        None,
    )
    .unwrap();
    let expected = arrow_cast::cast(&raw, &target)
        .map_err(|error| format!("md5sum_numeric: failed to cast output: {}", error))
        .unwrap_err();
    assert!(expected.len() > 512);
    use std::error::Error;
    let failure =
        evaluate_required(Operation::Md5sumNumeric, &[input], 1, Some(&target)).unwrap_err();
    assert_eq!(
        failure.to_string(),
        novarocks_failure_contract::BoundedDetail::truncating_display(&expected).to_string()
    );
    let sdk = failure
        .source()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<arrow_schema::ArrowError>()
        .unwrap();
    assert_eq!(
        format!("md5sum_numeric: failed to cast output: {sdk}"),
        expected
    );
    assert_eq!(
        to_owned_bytes_array(Arc::new(Int64Array::from(vec![1])), "md5", 0)
            .err()
            .unwrap()
            .to_string(),
        "md5: arg0 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn digest_reader_core_is_label_free_and_required_boundary_preserves_original_prefixes() {
    let source = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    assert!(matches!(
        to_owned_bytes_array_observed(source.clone(), 3, &mut |_| Ok(()))
            .err()
            .unwrap(),
        CoreError::BytesRequired { arg_idx: 3 }
    ));
    for label in ["md5", "md5sum", "aes_encrypt", "aes_decrypt"] {
        assert_eq!(
            to_owned_bytes_array(source.clone(), label, 3)
                .err()
                .unwrap()
                .to_string(),
            format!("{label}: arg3 must be VARCHAR or VARBINARY")
        );
    }
}
#[test]
fn digest_varchar_conversion_retains_the_actual_failed_sdk_conversion() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    let field = Arc::new(arrow_schema::Field::new(
        "ResourceExhausted",
        DataType::Int64,
        false,
    ));
    let weak_field = Arc::downgrade(&field);
    let source: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int64Array::from(vec![1]))],
        None,
    ));
    let expected = arrow_cast::cast(&source, &DataType::Utf8)
        .unwrap_err()
        .to_string();
    let weak = Arc::downgrade(&source);
    let error = to_owned_bytes_array_with_varchar_cast_observed(source.clone(), 4, &mut |_| Ok(()))
        .err()
        .unwrap();
    let CoreError::InputCast {
        arg_idx: 4,
        source: actual,
    } = error
    else {
        panic!("actual Arrow conversion cause");
    };
    assert_eq!(actual.to_string(), expected);
    let failure = to_owned_bytes_array_with_varchar_cast(source, "md5sum", 4)
        .err()
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert!(weak_field.upgrade().is_none());
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(failure.failure_fact().reason().code(), 370);
    let sdk = failure
        .source()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<arrow_schema::ArrowError>()
        .unwrap();
    assert_eq!(sdk.to_string(), expected);
    assert_eq!(
        failure.to_string(),
        format!("md5sum: failed to cast arg4 to VARCHAR: {expected}")
    );
}
#[test]
fn md5_typed_error_projection_preserves_actual_cast_and_kernel_failure() {
    let source = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let target = DataType::Struct(
        vec![Arc::new(arrow_schema::Field::new(
            "x",
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let typed = cast_output_observed(source.clone(), Some(&target), &mut |_| Ok(())).unwrap_err();
    let CoreError::OutputCast(cause) = typed else {
        panic!("exact original Arrow cast cause")
    };
    let expected = arrow_cast::cast(&source, &target).unwrap_err().to_string();
    assert_eq!(cause.to_string(), expected);
    assert_eq!(
        into_kernel(CoreError::OutputCast(cause), "aes_encrypt").to_string(),
        format!("aes_encrypt: failed to cast output: {expected}")
    );
    for cause in causes() {
        for stage in 0..3 {
            let mut calls = 0;
            let mut observe = |_| {
                calls += 1;
                assert_eq!(calls, 1, "no callback after refusal");
                Err(cause.clone())
            };
            let failure = match stage {
                0 => to_owned_bytes_array_observed(source.clone(), 0, &mut observe)
                    .err()
                    .unwrap(),
                1 => {
                    to_owned_bytes_array_with_varchar_cast_observed(source.clone(), 0, &mut observe)
                        .err()
                        .unwrap()
                }
                _ => cast_output_observed(source.clone(), Some(&target), &mut observe)
                    .err()
                    .unwrap(),
            };
            assert_eq!(calls, 1);
            let CoreError::Kernel(actual) = failure else {
                panic!("unchanged typed control cause")
            };
            assert_eq!(actual, cause);
        }
    }
}

#[test]
fn digest_large_readers_keep_original_buffers_and_null_readers_allocate_no_text_carrier() {
    let backing: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"unused".as_slice()),
        Some(b"abc".as_slice()),
        None,
        Some(b"".as_slice()),
    ]));
    let input = backing.slice(1, 3);
    let original = input.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let reader = to_owned_bytes_array(input.clone(), "md5", 0).unwrap();
    let OwnedBytesArray::LargeBinary(view) = &reader else {
        panic!("original i64 offset reader");
    };
    assert_eq!(view.value_data().as_ptr(), original.value_data().as_ptr());
    assert_eq!(reader.bytes(0).as_ptr(), original.value(0).as_ptr());
    let output = evaluate_required(Operation::Md5, &[reader], 3, None).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("900150983cd24fb0d6963f7d28e17f72"),
            None,
            Some("d41d8cd98f00b204e9800998ecf8427e")
        ]
    );
    let source: ArrayRef = Arc::new(arrow_array::NullArray::new(3));
    let reader = to_owned_bytes_array(source, "md5", 0).unwrap();
    assert!(matches!(&reader, OwnedBytesArray::Null { len: 3 }));
    let output = evaluate_required(Operation::Md5, &[reader], 3, None).unwrap();
    assert_eq!(output.null_count(), 3);
}

#[test]
fn real_digest_cast_failure_prevents_a_subsequent_observer_from_replacing_it() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let target = DataType::Struct(
        vec![Arc::new(arrow_schema::Field::new(
            "x",
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let mut calls = 0;
    let failure = cast_output_observed(source, Some(&target), &mut |_| {
        calls += 1;
        assert_eq!(
            calls, 1,
            "a real originating SDK failure has no optional footer"
        );
        Ok(())
    })
    .unwrap_err();
    assert_eq!(calls, 1);
    assert!(matches!(failure, CoreError::OutputCast(_)));
}

#[test]
fn md5_varchar_cast_keeps_arrow_named_timezone_values() {
    // Independent wall-clock expectations exercise Arrow's IANA timezone
    // parser and formatter through the original digest input conversion.
    for (zone, values, expected) in [
        (
            "Asia/Shanghai",
            vec![Some(0), Some(-1), None],
            [
                Some("1970-01-01T08:00:00+08:00"),
                Some("1970-01-01T07:59:59+08:00"),
                None,
            ],
        ),
        (
            "America/New_York",
            vec![Some(1_615_705_199), Some(1_615_705_200), None],
            [
                Some("2021-03-14T01:59:59-05:00"),
                Some("2021-03-14T03:00:00-04:00"),
                None,
            ],
        ),
    ] {
        let source: ArrayRef =
            Arc::new(arrow_array::TimestampSecondArray::from(values).with_timezone(zone));
        let actual = to_owned_bytes_array_with_varchar_cast(source, "md5sum", 0).unwrap();
        assert_eq!(actual.len(), expected.len());
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(actual.utf8(row), expected, "{zone} row {row}");
        }
    }
}
