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
//! Exact text-to-TIME profile and every originating host-control refusal.
use crate::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedCastRecipe,
};
use arrow_array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "checkpoint after original refusal");
        }
        t.push(n);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("TIME cast never waits")
    }
}
fn recipe(dtype: DataType, allow: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(dtype, true),
        &FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true),
        DecimalOverflowPolicy::ReportError,
        allow,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn text_time_kernel_every_actual_callback_seven_typed_causes_no_failure_footer() {
    let long = format!("{}:00:00", "0".repeat(320));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![long.as_str()])),
        Arc::new(LargeStringArray::from(vec![long.as_str()])),
        Arc::new(StringViewArray::from(vec![long.as_str()])),
    ];
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("actual host invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("actual host internal")),
        KernelFailure::Operational(KernelDiagnostic::new("actual host operational")),
        KernelFailure::InstanceFailed,
    ];
    for array in arrays {
        for allow in [false, true] {
            let recipe = recipe(array.data_type().clone(), allow);
            let control = Control::default();
            let _ = recipe
                .evaluate_row(EvaluatedArgument::Column(&array), 0, 0, &control)
                .unwrap();
            let trace = control.trace.lock().unwrap().clone();
            assert!(trace.contains(&256));
            for at in 0..trace.len() {
                for cause in &causes {
                    let control = Control {
                        trace: Mutex::new(Vec::new()),
                        refusal: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        recipe
                            .evaluate_row(EvaluatedArgument::Column(&array), 0, 0, &control)
                            .unwrap_err(),
                        *cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
#[test]
fn text_time_kernel_original_parser_full_domain_keeps_distinct_carriers_and_typed_mode() {
    use crate::time_text_cast::{TimeTextParseMode, evaluate_arrays};
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        "25:00:00",
        "+01:02:03",
        "1970-01-01 01:01:01",
    ]));
    let duration = evaluate_arrays(
        &source,
        &DataType::Time64(TimeUnit::Microsecond),
        TimeTextParseMode::Duration,
    )
    .unwrap();
    let datetime = evaluate_arrays(
        &source,
        &DataType::Time64(TimeUnit::Microsecond),
        TimeTextParseMode::Datetime,
    )
    .unwrap();
    use arrow_array::{Array, Time64MicrosecondArray};
    let duration = duration
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    let datetime = datetime
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(
        duration.iter().collect::<Vec<_>>(),
        vec![Some(90_000_000_000), None, None]
    );
    assert_eq!(
        datetime.iter().collect::<Vec<_>>(),
        vec![None, None, Some(3_661_000_000)]
    );
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let target = FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false);
        assert!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(dtype, false),
                &target,
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default()
            )
            .is_err()
        );
    }
}
#[test]
fn text_time_kernel_checked_scalar_address_does_not_read_unselected_invalid_or_null() {
    use arrow_array::Array;
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("00:00:00"),
        Some("25:00:00"),
        Some("+01:02:03"),
    ]));
    let recipe = recipe(DataType::Utf8, true);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Column(&values),
                0,
                2,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Signed(90_000_000_000)
    );
    let scalar = values.slice(1, 1);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                16,
                999,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Signed(0)
    );
}

#[test]
fn text_time_kernel_exact_compact_and_nonzero_pool_ordinals_keep_original_row_addresses() {
    use crate::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
    use arrow_array::Array;
    let rows = [1, 2, 3];
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let raw = [Some("ignored"), Some("01:02:03"), None, Some("+01:02:03")];
        let input: ArrayRef = match dtype {
            DataType::Utf8 => Arc::new(StringArray::from(raw.to_vec())),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(raw.to_vec())),
            DataType::Utf8View => Arc::new(StringViewArray::from(raw.to_vec())),
            _ => unreachable!(),
        };
        let recipe = recipe(dtype.clone(), false);
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact =
            SelectedValues::try_new(selection, &dtype, input.slice(1, 3), Box::default()).unwrap();
        let expected = [
            CastRowResult::Signed(3_723_000_000),
            CastRowResult::Null,
            CastRowResult::Null,
        ];
        for (ordinal, row) in selection.iter().enumerate() {
            assert_eq!(
                recipe
                    .evaluate_row(
                        EvaluatedArgument::SelectedColumn(&compact),
                        ordinal,
                        row,
                        &Control::default()
                    )
                    .unwrap(),
                expected[ordinal]
            );
        }
        assert!(matches!(
            recipe.evaluate_row(
                EvaluatedArgument::SelectedColumn(&compact),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let ty = FunctionValueType::from_legacy_declared_projection(dtype, true);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("actual-time-text").unwrap()),
            ty,
            input.to_data(),
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
            &Control::default(),
        )
        .unwrap();
        for ordinal in 1..=3 {
            let value = pool.value(ordinal).unwrap();
            assert_eq!(
                recipe
                    .evaluate_row(
                        EvaluatedArgument::Constant(&value),
                        17,
                        999,
                        &Control::default()
                    )
                    .unwrap(),
                expected[ordinal as usize - 1]
            );
        }
    }
}
#[test]
fn text_time_frozen_target_contract_precedes_any_observer() {
    use crate::time_text_cast::{TimeTextParseMode, evaluate_arrays_observed};
    let input: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    let target = DataType::Time64(TimeUnit::Nanosecond);
    let mut observer =
        |_| -> Result<(), KernelFailure> { panic!("target contract failure precedes observation") };
    let error = evaluate_arrays_observed(
        &input,
        &target,
        TimeTextParseMode::Duration,
        Some(&mut observer),
    )
    .unwrap()
    .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(
        error.to_string(),
        "CAST failed: TIME target must be Time64(Microsecond), got Time64(Nanosecond)"
    );
}

#[test]
fn raw_time_text_keeps_distinct_carrier_whitespace_and_numeric_grammar() {
    use arrow_array::{Array, Time64MicrosecondArray};
    let raw = vec![
        Some(" 01:02:03 "),
        Some("01:02:03"),
        Some("123"),
        Some(" 123 "),
        None,
    ];
    for source in [
        Arc::new(LargeStringArray::from(raw.clone())) as ArrayRef,
        Arc::new(StringViewArray::from(raw.clone())) as ArrayRef,
    ] {
        let output = crate::time_text_cast::evaluate_arrays(
            &source,
            &DataType::Time64(TimeUnit::Microsecond),
            crate::time_text_cast::TimeTextParseMode::Duration,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(3_723_000_000), Some(123), None, None]
        );
        let prepared = recipe(source.data_type().clone(), true);
        assert_eq!(
            prepared
                .evaluate_row(
                    EvaluatedArgument::Column(&source),
                    9,
                    0,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Null
        );
    }
}
