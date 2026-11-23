// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frozen original timezone, exact constant-format proof and selected addresses.
use super::super::calendar_unixtime_owner::prepared_for_test;
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, ConstantValue, EvaluatedArgument, FunctionValueType,
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, ScalarEvaluationInstance,
    SelectedValues, Selection,
    kernel_control::{internal, invalid},
};
use arrow_array::{Array, Int64Array};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{sync::Mutex, time::Duration as WaitDuration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after refusal");
        }
        trace.push(n);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: WaitDuration) -> Result<(), KernelFailure> {
        panic!("unixtime never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop);
        }
        trace.push(n);
        if let Some((stop, cause)) = self.refusal.clone()
            && at == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn policy() -> ConstantPolicy {
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
    }
}
fn format(value: Option<&str>) -> ConstantValue {
    let values = StringArray::from(vec![value]);
    let ty = FunctionValueType::new(DataType::Utf8, value.is_none());
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-format").unwrap()),
        ty,
        values.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap()
    .value(0)
    .unwrap()
}
fn one_kernel(
    zone: Option<&str>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test(
        &[FunctionValueType::new(DataType::Int64, true)],
        &[None],
        zone,
        control,
    )
}
fn format_kernel(
    value: &ConstantValue,
    zone: Option<&str>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test(
        &[
            FunctionValueType::new(DataType::Utf8, true),
            value.value_type().clone(),
        ],
        &[None, Some(value.clone())],
        zone,
        control,
    )
}
#[test]
fn unixtime_one_exact_int64_profile_sparse_bounds_null_empty_and_frozen_rule() {
    let values = Arc::new(Int64Array::from(vec![
        Some(0),
        Some(1),
        Some(-1),
        None,
        Some(253402243200),
    ])) as ArrayRef;
    let rows = [0, 1, 2, 3, 4];
    let mut instance = ScalarEvaluationInstance::instantiate(
        one_kernel(Some("UTC"), &CompileControl::default()).unwrap(),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let out = instance
        .evaluate(
            Selection::try_sparse(5, &rows).unwrap(),
            &arguments,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        out.values().to_data(),
        StringArray::from(vec![
            Some("1970-01-01 00:00:00"),
            Some("1970-01-01 00:00:01"),
            None,
            None,
            None
        ])
        .to_data()
    );
    assert!(out.errors().is_empty());
    assert!(
        instance
            .evaluate(
                Selection::try_sparse(5, &[]).unwrap(),
                &arguments,
                &Control::default()
            )
            .unwrap()
            .values()
            .is_empty()
    );
    for (zone, expected) in [
        ("+10:00", "1970-01-01 10:00:00"),
        ("America/New_York", "1969-12-31 19:00:00"),
        ("Asia/Shanghai", "1970-01-01 08:00:00"),
    ] {
        let source = values.slice(0, 1);
        let args = [EvaluatedArgument::Column(&source)];
        let out = ScalarEvaluationInstance::instantiate(
            one_kernel(Some(zone), &CompileControl::default()).unwrap(),
        )
        .unwrap()
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
        assert_eq!(
            out.values().to_data(),
            StringArray::from(vec![Some(expected)]).to_data()
        );
    }
}
#[test]
fn unixtime_utf8_constant_format_proof_preserves_normal_null_128_129_and_expansion_null() {
    let source = Arc::new(StringArray::from(vec![
        Some("0"),
        Some(" 1.9 "),
        Some("bad"),
        None,
    ])) as ArrayRef;
    let f128 = ":".repeat(128);
    let f129 = ":".repeat(129);
    let expand = "%Y".repeat(64);
    for (value, expected) in [
        (Some("%Y"), Some("1970")),
        (Some(""), None),
        (Some("plain"), None),
        (Some(f128.as_str()), Some(f128.as_str())),
        (Some(f129.as_str()), None),
        (Some(expand.as_str()), None),
        (None, None),
    ] {
        let constant = format(value);
        let mut instance = ScalarEvaluationInstance::instantiate(
            format_kernel(&constant, Some("UTC"), &CompileControl::default()).unwrap(),
        )
        .unwrap();
        let args = [
            EvaluatedArgument::Column(&source),
            EvaluatedArgument::Constant(&constant),
        ];
        let out = instance
            .evaluate(Selection::all(4), &args, &Control::default())
            .unwrap();
        assert_eq!(
            out.values().to_data(),
            StringArray::from(vec![expected, expected, None, None]).to_data()
        );
    }
}
#[test]
fn unixtime_prepare_names_missing_local_author_declared_raw_errors_and_original_format_panic_shapes()
 {
    assert!(one_kernel(None, &CompileControl::default()).is_err());
    for zone in ["local", "bad-zone"] {
        assert!(matches!(
            one_kernel(Some(zone), &CompileControl::default()),
            Err(crate::FunctionSpecializationFailure::InvalidInput(
                "invalid statement time zone"
            ))
        ));
    }
    let constant = format(Some("%Y"));
    for source in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ] {
        assert!(
            prepared_for_test(
                &[
                    FunctionValueType::from_legacy_declared_projection(source, true),
                    constant.value_type().clone()
                ],
                &[None, Some(constant.clone())],
                Some("UTC"),
                &CompileControl::default()
            )
            .is_err()
        );
    }
    assert!(
        prepared_for_test(
            &[
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(DataType::Utf8, true)
            ],
            &[None, None],
            Some("UTC"),
            &CompileControl::default()
        )
        .is_err()
    );
    for malformed in ["%%", "%Y%%"] {
        assert!(
            format_kernel(
                &format(Some(malformed)),
                Some("UTC"),
                &CompileControl::default()
            )
            .is_err()
        );
    }
    // The real HOUR declared result is Int32 while raw returns Int64; no factory silently repairs it.
    assert!(super::super::calendar_unixtime_owner::operation("hour_from_unixtime").is_none());
}
#[test]
fn unixtime_selected_origins_compact_slice_scalar_and_nonzero_constant_are_independent() {
    let source = Arc::new(Int64Array::from(vec![Some(9), Some(0), None, Some(1)])) as ArrayRef;
    let ty = FunctionValueType::new(DataType::Int64, true);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        source.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    for arg in [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let args = [arg];
        let out = ScalarEvaluationInstance::instantiate(
            one_kernel(Some("UTC"), &CompileControl::default()).unwrap(),
        )
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
        assert_eq!(
            out.values().to_data(),
            StringArray::from(vec![
                Some("1970-01-01 00:00:00"),
                None,
                Some("1970-01-01 00:00:01")
            ])
            .to_data()
        );
    }
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-epoch").unwrap()),
        ty,
        source.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let scalar = source.slice(1, 1);
    for arg in [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        let rows = [7];
        let args = [arg];
        let out = ScalarEvaluationInstance::instantiate(
            one_kernel(Some("UTC"), &CompileControl::default()).unwrap(),
        )
        .unwrap()
        .evaluate(
            Selection::try_sparse(8, &rows).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            out.values().to_data(),
            StringArray::from(vec![Some("1970-01-01 00:00:00")]).to_data()
        );
    }
}
#[test]
fn unixtime_every_runtime_and_compile_prefix_preserves_all_seven_and_three_primary_causes() {
    let source = Arc::new(StringArray::from(vec![
        Some("0"),
        Some("1"),
        None,
        Some("bad"),
    ])) as ArrayRef;
    let constant = format(Some("%Y-%m-%d %H:%i:%s"));
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Constant(&constant),
    ];
    let kernel = format_kernel(&constant, Some("UTC"), &CompileControl::default()).unwrap();
    let good = Control::default();
    ScalarEvaluationInstance::instantiate(kernel.clone())
        .unwrap()
        .evaluate(Selection::all(4), &args, &good)
        .unwrap();
    let count = good.trace.lock().unwrap().len();
    for stop in 0..count {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            invalid("original invalid"),
            internal("original internal"),
            KernelFailure::Operational(KernelDiagnostic::new("original operation")),
            KernelFailure::InstanceFailed,
        ] {
            let c = Control {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            };
            let mut instance = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
            assert_eq!(
                instance.evaluate(Selection::all(4), &args, &c).unwrap_err(),
                cause
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
    let good = CompileControl::default();
    format_kernel(&constant, Some("UTC"), &good).unwrap();
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
            let error = format_kernel(&constant, Some("UTC"), &c).err().unwrap();
            let actual = match error {
                crate::FunctionSpecializationFailure::Binding(
                    crate::FunctionBindingError::Control(cause),
                )
                | crate::FunctionSpecializationFailure::Control(cause) => Some(cause),
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                    Some(CompileControlError::Cancelled)
                }
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                    Some(CompileControlError::DeadlineExceeded)
                }
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                    Some(CompileControlError::ResourceExhausted)
                }
                _ => None,
            };
            assert_eq!(actual, Some(cause));
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
