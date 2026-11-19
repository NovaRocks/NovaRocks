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
use crate::{KernelDiagnostic, ScalarEvaluationInstance, Selection};
use novarocks_type_contract::ValueLogicalType;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
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
        panic!("bitwise calls must not wait")
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
fn shared_bitwise_all_20_profiles_preserve_all_seven_actual_causes_and_failed_latch() {
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
            Arc::new(arrow_array::Int64Array::from(vec![1])),
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
    let selection = Selection::all(513);
    for name in ["bitand", "bitor", "bitxor", "bitnot"] {
        for (source, values) in &profiles {
            let arity = if name == "bitnot" { 1 } else { 2 };
            let types = vec![source.clone(); arity];
            let args = vec![EvaluatedArgument::Scalar(values); arity];
            let prepare = || {
                ScalarEvaluationInstance::instantiate(
                    super::super::super::bitwise_owner::prepared_for_test(name, &types).unwrap(),
                )
                .unwrap()
            };
            let mut baseline = prepare();
            let trace = Trace::default();
            baseline.evaluate(selection, &args, &trace).unwrap();
            let calls = trace.calls.lock().unwrap().clone();
            assert!(calls.contains(&256));
            for cause in causes() {
                for at in 0..calls.len() {
                    let mut instance = prepare();
                    let refusal = Trace {
                        calls: Mutex::new(Vec::new()),
                        refusal: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        instance.evaluate(selection, &args, &refusal).unwrap_err(),
                        cause
                    );
                    assert_eq!(refusal.calls.lock().unwrap().as_slice(), &calls[..=at]);
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
