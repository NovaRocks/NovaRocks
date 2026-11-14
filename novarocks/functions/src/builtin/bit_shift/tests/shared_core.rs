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
//! Actual observer refusal points for the shared loop/materializers.
use super::super::*;
use crate::{KernelDiagnostic, ScalarEvaluationInstance};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Trace {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push(units);
        if let Some((at, failure)) = &self.refusal
            && index == *at
        {
            return Err(failure.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("shifts must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original host cause"),
        internal("original host cause"),
        KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn shared_shift_all_profiles_preserve_all_seven_actual_checkpoint_causes_and_latch() {
    let profiles: Vec<(FunctionValueType, ArrayRef)> = vec![
        (
            FunctionValueType::new(DataType::Int8, false),
            Arc::new(arrow_array::Int8Array::from(vec![1])),
        ),
        (
            FunctionValueType::new(DataType::Int16, false),
            Arc::new(arrow_array::Int16Array::from(vec![1])),
        ),
        (
            FunctionValueType::new(DataType::Int32, false),
            Arc::new(arrow_array::Int32Array::from(vec![1])),
        ),
        (
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![1])),
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
            crate::largeint::array_from_i128(&[Some(1)]).unwrap(),
        ),
    ];
    let right: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let selection = crate::Selection::all(513);
    for name in [
        "bit_shift_left",
        "bit_shift_right",
        "bit_shift_right_logical",
    ] {
        for (source, left) in &profiles {
            let types = [
                source.clone(),
                FunctionValueType::new(DataType::Int64, false),
            ];
            let args = [
                EvaluatedArgument::Scalar(left),
                EvaluatedArgument::Scalar(&right),
            ];
            let prepare = || {
                ScalarEvaluationInstance::instantiate(
                    super::super::super::bit_shift_owner::prepared_for_test(name, &types).unwrap(),
                )
                .unwrap()
            };
            let mut baseline = prepare();
            let trace = Trace::default();
            baseline.evaluate(selection, &args, &trace).unwrap();
            let count = trace.calls.lock().unwrap().len();
            assert!(trace.calls.lock().unwrap().contains(&256));
            for cause in causes() {
                for at in 0..count {
                    let mut instance = prepare();
                    let failure = Trace {
                        calls: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        instance.evaluate(selection, &args, &failure).unwrap_err(),
                        cause,
                        "{name} {source:?} checkpoint {at}"
                    );
                    assert_eq!(failure.calls.lock().unwrap().len(), at + 1);
                    let after = Trace::default();
                    assert_eq!(
                        instance.evaluate(selection, &args, &after).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.calls.lock().unwrap().is_empty());
                }
            }
        }
    }
}
#[test]
fn shared_bit_array_default_safe_conversion_keeps_invalid_rows_null() {
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    let source: ArrayRef =
        std::sync::Arc::new(StringArray::from(vec![Some("-42"), Some("invalid"), None]));
    assert_eq!(
        crate::bit_array::to_i64_array(&source, 1).unwrap(),
        Int64Array::from(vec![Some(-42), None, None])
    );
}
