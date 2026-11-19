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

use novarocks_functions::{CompileKernelControl, KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::time::Duration;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn compile_control_adapter_never_allows_a_wait() {
    let control = CompileKernelControl::new(&Control, CompilePhase::FunctionSpecialization);
    assert!(matches!(
        control.wait(Duration::ZERO),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
