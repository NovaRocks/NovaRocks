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

use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use std::{sync::Mutex, time::Duration};

/// A callback fixture that refuses only at the chosen checkpoint and rejects
/// any later control work on that failed operation.
pub struct CallbackControl {
    cause: KernelFailure,
    stop_at: usize,
    trace: Mutex<Vec<u32>>,
}

impl CallbackControl {
    pub fn new(cause: KernelFailure, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(Vec::new()),
        }
    }

    pub fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}

impl KernelEvaluationControl for CallbackControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        assert!(
            self.stop_at == 0 || trace.len() < self.stop_at,
            "control callback after primary refusal"
        );
        trace.push(units);
        if trace.len() == self.stop_at {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("synchronous expression fixture must not wait")
    }
}

pub fn callback_causes([invalid, internal, operational]: [&str; 3]) -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new(invalid)),
        KernelFailure::Internal(KernelDiagnostic::new(internal)),
        KernelFailure::Operational(KernelDiagnostic::new(operational)),
        KernelFailure::InstanceFailed,
    ]
}
