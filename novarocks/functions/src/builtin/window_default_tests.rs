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
    ConstantPolicy, ConstantPool, ConstantValue, KernelDiagnostic, RowDataError, SelectedValues,
};
use novarocks_type_contract::CompileControlError;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after primary");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("default conversion never waits")
    }
}
fn failures() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, nullable)
}
fn recipe(source: &FunctionValueType, target: &FunctionValueType) -> DefaultValueRecipe {
    DefaultValueRecipe::try_new(source, target, &CompileControl::default()).unwrap()
}
fn signed_array(kind: &DataType, values: &[Option<i64>]) -> ArrayRef {
    match kind {
        DataType::Int8 => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|x| x.map(|n| n as i8))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|x| x.map(|n| n as i16))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|x| x.map(|n| n as i32))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("signed fixture"),
    }
}
fn ints(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|x| x.map(i64::from))
            .collect(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|x| x.map(i64::from))
            .collect(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|x| x.map(i64::from))
            .collect(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("signed output"),
    }
}
fn strings(array: &ArrayRef) -> Vec<Option<&str>> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect()
}
fn maximum(kind: &DataType) -> i64 {
    match kind {
        DataType::Int8 => i8::MAX as i64,
        DataType::Int16 => i16::MAX as i64,
        DataType::Int32 => i32::MAX as i64,
        DataType::Int64 => i64::MAX,
        _ => panic!("signed maximum"),
    }
}
fn fits(value: i64, kind: &DataType) -> bool {
    match kind {
        DataType::Int8 => i8::try_from(value).is_ok(),
        DataType::Int16 => i16::try_from(value).is_ok(),
        DataType::Int32 => i32::try_from(value).is_ok(),
        DataType::Int64 => true,
        _ => panic!("signed bounds"),
    }
}
fn constant(array: ArrayRef, ordinal: u32) -> ConstantValue {
    let value_type = ty(array.data_type().clone(), true);
    let policy = ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    };
    ConstantPool::try_new(
        Arc::new(value_type.try_to_field("original").unwrap()),
        value_type,
        array.to_data(),
        policy,
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}

#[test]
fn all_sixteen_signed_safe_cast_profiles_keep_original_null_and_narrowing_rules() {
    let kinds = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    for source in &kinds {
        let values = [
            Some(7),
            Some(-7),
            Some(0),
            Some(maximum(source)),
            Some(-maximum(source) - 1),
            None,
        ];
        let array = signed_array(source, &values);
        for target in &kinds {
            let r = recipe(&ty(source.clone(), true), &ty(target.clone(), true));
            assert_eq!(r.source_type(), &ty(source.clone(), true));
            assert_eq!(r.result_type(), &ty(target.clone(), true));
            let actual = r
                .evaluate_complete(EvaluatedArgument::Column(&array), 6, &Control::default())
                .unwrap();
            let expected: Vec<_> = values
                .iter()
                .map(|x| x.filter(|v| fits(*v, target)))
                .collect();
            assert_eq!(ints(&actual), expected, "{source:?} -> {target:?}");
            let original = arrow_cast::cast(array.as_ref(), target).unwrap();
            assert_eq!(ints(&actual), ints(&original));
            assert!(actual.get_array_memory_size() <= r.retained_upper_bound(6).unwrap());
        }
    }
}

#[test]
fn original_utf8_parse_and_i64_formatter_have_independent_complete_oracles() {
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        Some("7"),
        Some("-7"),
        Some("+7"),
        Some(" 7"),
        Some("7 "),
        Some(""),
        Some("9223372036854775808"),
        Some("-9223372036854775808"),
        Some("é"),
        Some("7\0"),
        None,
    ]));
    let parse = recipe(&ty(DataType::Utf8, true), &ty(DataType::Int64, true));
    let actual = parse
        .evaluate_complete(EvaluatedArgument::Column(&source), 11, &Control::default())
        .unwrap();
    assert_eq!(
        ints(&actual),
        vec![
            Some(7),
            Some(-7),
            Some(7),
            None,
            None,
            None,
            None,
            Some(i64::MIN),
            None,
            None,
            None
        ]
    );
    // The raw parser contract above is independent of the current SDK parser.
    let scalar = source.slice(0, 1);
    let broadcast = parse
        .evaluate_complete(EvaluatedArgument::Scalar(&scalar), 11, &Control::default())
        .unwrap();
    assert_eq!(ints(&broadcast), vec![Some(7); 11]);
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MIN),
        Some(i64::MAX),
        Some(0),
        Some(-1),
        None,
    ]));
    let format = recipe(&ty(DataType::Int64, true), &ty(DataType::Utf8, true));
    let text = format
        .evaluate_complete(EvaluatedArgument::Column(&numbers), 5, &Control::default())
        .unwrap();
    assert_eq!(
        strings(&text),
        vec![
            Some("-9223372036854775808"),
            Some("9223372036854775807"),
            Some("0"),
            Some("-1"),
            None
        ]
    );
    assert_eq!(
        strings(&text),
        strings(&arrow_cast::cast(numbers.as_ref(), &DataType::Utf8).unwrap())
    );
    assert!(text.get_array_memory_size() <= format.retained_upper_bound(5).unwrap());
}

#[test]
fn complete_defaults_gather_slices_scalar_compact_and_only_selected_pool_ordinal() {
    let backing: ArrayRef = Arc::new(StringArray::from(vec![
        "unused invalid",
        "27",
        "-8",
        "unused overflow9999999999999999999999999",
    ]));
    let sliced = backing.slice(1, 2);
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["42"]));
    let cv = constant(backing.clone(), 1);
    let compact = SelectedValues::try_new(
        Selection::all(2),
        &DataType::Utf8,
        sliced.clone(),
        Box::default(),
    )
    .unwrap();
    let parse = recipe(&ty(DataType::Utf8, true), &ty(DataType::Int64, true));
    for (input, rows, expected) in [
        (
            EvaluatedArgument::Column(&sliced),
            2,
            vec![Some(27), Some(-8)],
        ),
        (
            EvaluatedArgument::SelectedColumn(&compact),
            2,
            vec![Some(27), Some(-8)],
        ),
        (EvaluatedArgument::Scalar(&scalar), 4, vec![Some(42); 4]),
        (EvaluatedArgument::Constant(&cv), 4, vec![Some(27); 4]),
    ] {
        let out = parse
            .evaluate_complete(input, rows, &Control::default())
            .unwrap();
        assert_eq!(ints(&out), expected);
    }
    let out = parse
        .evaluate_complete(EvaluatedArgument::Column(&backing), 4, &Control::default())
        .unwrap();
    assert_eq!(ints(&out), vec![None, Some(27), Some(-8), None]);
}

#[test]
fn typed_null_identity_logical_domain_and_empty_partition_are_truthful() {
    let nulls: ArrayRef = Arc::new(NullArray::new(3));
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Utf8,
    ] {
        let r = recipe(&ty(DataType::Null, true), &ty(target.clone(), true));
        let actual = r
            .evaluate_complete(EvaluatedArgument::Column(&nulls), 3, &Control::default())
            .unwrap();
        assert_eq!(actual.data_type(), &target);
        assert_eq!(actual.null_count(), 3);
        assert!(actual.get_array_memory_size() <= r.retained_upper_bound(3).unwrap());
    }
    let json = FunctionValueType::try_from_legacy_declared_projection(
        DataType::Utf8,
        false,
        ValueLogicalType::Json,
    )
    .expect("valid temporary legacy function value declaration");
    let mut target = json.clone();
    target = target
        .with_nullable(true)
        .expect("valid function nullability declaration");
    let r = recipe(&json, &target);
    assert_eq!(r.source_type().logical_type(), ValueLogicalType::Json);
    let source: ArrayRef = Arc::new(StringArray::from(vec!["{}", "[1]"]));
    assert_eq!(
        strings(
            &r.evaluate_complete(EvaluatedArgument::Column(&source), 2, &Control::default())
                .unwrap()
        ),
        vec![Some("{}"), Some("[1]")]
    );
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let r = recipe(&ty(DataType::Int64, false), &ty(DataType::Utf8, true));
    let out = r
        .evaluate_complete(EvaluatedArgument::Column(&empty), 0, &Control::default())
        .unwrap();
    assert_eq!(out.len(), 0);
    // Original formatter still owns its default builder capacities for zero rows.
    assert!(out.get_array_memory_size() <= r.retained_upper_bound(0).unwrap());
}

#[test]
fn unsupported_nominal_and_nonnullable_targets_refuse_without_rebinding() {
    for (source, target) in [
        (ty(DataType::Int64, false), ty(DataType::Int64, false)),
        (ty(DataType::Int64, true), ty(DataType::Boolean, true)),
        (ty(DataType::UInt64, true), ty(DataType::Int64, true)),
        (ty(DataType::LargeUtf8, true), ty(DataType::Int64, true)),
        (
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::Utf8,
                true,
                ValueLogicalType::Json,
            )
            .expect("valid temporary legacy function value declaration"),
            ty(DataType::Int64, true),
        ),
        (
            ty(DataType::Utf8, true),
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::Utf8,
                true,
                ValueLogicalType::Json,
            )
            .expect("valid temporary legacy function value declaration"),
        ),
    ] {
        assert!(matches!(
            DefaultValueRecipe::try_new(&source, &target, &CompileControl::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let r = recipe(&ty(DataType::Int64, false), &ty(DataType::Int8, true));
    let null: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    assert!(
        r.evaluate_complete(EvaluatedArgument::Column(&null), 1, &Control::default())
            .is_err()
    );
    let source: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    assert!(
        r.evaluate_complete(EvaluatedArgument::Column(&source), 2, &Control::default())
            .is_err()
    );
    let wrong: ArrayRef = Arc::new(StringArray::from(vec!["7"]));
    assert!(
        r.evaluate_complete(EvaluatedArgument::Column(&wrong), 1, &Control::default())
            .is_err()
    );
    let required = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        null.clone(),
        vec![RowDataError::new(0, "required child")].into(),
    )
    .unwrap();
    assert!(
        r.evaluate_complete(
            EvaluatedArgument::SelectedColumn(&required),
            1,
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn every_actual_compile_boundary_keeps_all_three_original_causes_and_ordinary_tail() {
    for target in [ty(DataType::Int64, true), ty(DataType::Boolean, true)] {
        let source = ty(DataType::Utf8, true);
        let baseline = CompileControl::default();
        let _ = DefaultValueRecipe::try_new(&source, &target, &baseline);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(
            trace.last().unwrap().1 > 0,
            "ordinary and success pending tail observed"
        );
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                let error = DefaultValueRecipe::try_new(&source, &target, &c).unwrap_err();
                assert_eq!(error, compile_failure(cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn every_actual_runtime_boundary_keeps_seven_causes_without_later_callbacks() {
    let r = recipe(&ty(DataType::Utf8, true), &ty(DataType::Int64, true));
    let valid: ArrayRef = Arc::new(StringArray::from(vec!["7", "bad", "-8"]));
    let wrong: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9]));
    for array in [&valid, &wrong] {
        let baseline = Control::default();
        let _ = r.evaluate_complete(EvaluatedArgument::Column(array), 3, &baseline);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop in 0..trace.len() {
            for cause in failures() {
                let c = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                let error = r
                    .evaluate_complete(EvaluatedArgument::Column(array), 3, &c)
                    .unwrap_err();
                assert_eq!(error, cause);
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn wide_complete_mapping_has_real_quantum_and_checked_retained_layout_bounds() {
    let r = recipe(&ty(DataType::Int64, false), &ty(DataType::Utf8, true));
    let source: ArrayRef = Arc::new(Int64Array::from(
        (0..320).map(i64::from).collect::<Vec<_>>(),
    ));
    let c = Control::default();
    let out = r
        .evaluate_complete(EvaluatedArgument::Column(&source), 320, &c)
        .unwrap();
    assert_eq!(strings(&out)[319], Some("319"));
    let trace = c.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(out.get_array_memory_size() <= r.retained_upper_bound(320).unwrap());
    // Force actual builder payload growth beyond its 1024-byte initial buffer.
    let largest: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN; 320]));
    let grown = r
        .evaluate_complete(
            EvaluatedArgument::Column(&largest),
            320,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(strings(&grown), vec![Some("-9223372036854775808"); 320]);
    assert!(grown.get_array_memory_size() <= r.retained_upper_bound(320).unwrap());
    for stop in [
        0,
        trace.iter().position(|n| *n == 256).unwrap(),
        trace.len() - 1,
    ] {
        for cause in failures() {
            let c = Control {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                r.evaluate_complete(EvaluatedArgument::Column(&source), 320, &c)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    assert_eq!(
        gather_layout(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        r.retained_upper_bound(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        formatter_capacities(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_layout(&DataType::Utf8, 1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn raw_window_parse_rejects_ascii_and_unicode_edges_and_preserves_boundaries() {
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        Some("\t7"),
        Some("7\n"),
        Some("\u{a0}7"),
        Some("7\u{2003}"),
        Some("\u{85}7"),
        Some("7\u{3000}"),
        Some("\u{feff}7"),
        Some("+9223372036854775807"),
        Some("-9223372036854775808"),
        Some("-9223372036854775809"),
        Some("-0"),
        None,
    ]));
    let parse = recipe(&ty(DataType::Utf8, true), &ty(DataType::Int64, true));
    let actual = parse
        .evaluate_complete(EvaluatedArgument::Column(&source), 12, &Control::default())
        .unwrap();
    assert_eq!(
        ints(&actual),
        vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(i64::MAX),
            Some(i64::MIN),
            None,
            Some(0),
            None
        ]
    );
}
