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
//! Exact shared-reader/body/projection checkpoints preserve every host cause.
use super::*;
use crate::{EvaluatedArgument, FunctionValueType, KernelDiagnostic, ScalarEvaluationInstance};
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
        let at = calls.len();
        calls.push(units);
        if let Some((fail, cause)) = &self.refusal
            && at == *fail
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("string measurement never waits")
    }
}
#[test]
fn string_measure_shared_all_profiles_seven_causes_actual_callbacks_and_failed_latch() {
    for name in ["length"] {
        for (source, rows) in [
            (
                Arc::new(StringArray::from(vec![
                    Some("é\0🦀".repeat(300)),
                    None,
                    Some(String::new()),
                ])) as ArrayRef,
                3,
            ),
            (
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
                0,
            ),
        ] {
            let ty = FunctionValueType::new(DataType::Utf8, true);
            let p = super::super::string_measure_owner::prepared_for_test(name, &ty).unwrap();
            let args = [EvaluatedArgument::Column(&source)];
            let good = Trace::default();
            ScalarEvaluationInstance::instantiate(p.clone())
                .unwrap()
                .evaluate(Selection::all(rows), &args, &good)
                .unwrap();
            let trace = good.calls.lock().unwrap().clone();
            for at in 0..trace.len() {
                for cause in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                    invalid("original host cause"),
                    internal("original host cause"),
                    KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
                    KernelFailure::InstanceFailed,
                ] {
                    let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                    let ctl = Trace {
                        calls: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        k.evaluate(Selection::all(rows), &args, &ctl).unwrap_err(),
                        cause,
                        "{name} {at}"
                    );
                    assert_eq!(*ctl.calls.lock().unwrap(), trace[..=at]);
                    let after = Trace::default();
                    assert_eq!(
                        k.evaluate(Selection::all(rows), &args, &after).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.calls.lock().unwrap().is_empty());
                }
            }
        }
    }
}
#[test]
fn string_measure_shared_original_int_projection_preserves_full_overflow_message() {
    for value in [i64::MIN, i32::MIN as i64 - 1, i32::MAX as i64 + 1, i64::MAX] {
        let error = length_int32(vec![None, Some(value)]).unwrap_err();
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(error.failure_fact().reason().code(), 41);
        let _ = super::super::string_failure::original_source::<std::num::TryFromIntError>(&error);
        assert_eq!(
            error.to_string(),
            format!("length result out of INT range: {value}")
        );
    }
    let output = length_int32(vec![Some(i32::MIN as i64), None, Some(i32::MAX as i64)]).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(i32::MIN), None, Some(i32::MAX)]
    );
}
