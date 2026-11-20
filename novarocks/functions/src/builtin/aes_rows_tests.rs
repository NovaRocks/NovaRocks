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
use super::super::{bytes_output, md5_shared::Observation};
use crate::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::{sync::Mutex, time::Duration};

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
        if let Some((stop, _)) = self.refusal.as_ref() {
            assert!(at <= *stop, "callback after originating refusal");
        }
        trace.push(units);
        match self.refusal.as_ref() {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("AES must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("AES original invalid cause")),
        KernelFailure::Internal(KernelDiagnostic::new("AES original internal cause")),
        KernelFailure::Operational(KernelDiagnostic::new("AES original operational cause")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn aes_bytes_output_null_empty_loop_observes_real_256_quantum_and_each_cause() {
    for projection in [
        bytes_output::TextProjection::Latin1,
        bytes_output::TextProjection::Utf8Lossy,
    ] {
        let values: Vec<_> = (0..321)
            .map(|i| if i % 2 == 0 { None } else { Some(vec![]) })
            .collect();
        let invoke = |control: &Control| -> Result<ArrayRef, KernelFailure> {
            control.checkpoint(0)?;
            let mut work = EvaluationCheckpoints::new(control);
            let output = bytes_output::build_bytes_output_observed(
                values.clone(),
                Some(&DataType::Utf8),
                projection,
                &mut |event| match event {
                    Observation::Step => work.step(),
                    Observation::OpaqueBoundary => work.flush(),
                },
            );
            work.finish_result(output)
        };
        let recorder = Control::default();
        let out = invoke(&recorder).unwrap();
        assert_eq!(out.len(), 321);
        assert_eq!(out.null_count(), 161);
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(invoke(&control),Err(actual)if actual==cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
