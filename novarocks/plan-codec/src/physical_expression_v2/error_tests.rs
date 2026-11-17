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
use novarocks_constant_contract::ConstantError;
use novarocks_physical_plan::{ConstantPoolId, ConstantReferenceError};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::sync::Mutex;

struct Control(Mutex<Vec<u32>>);
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        self.0.lock().unwrap().push(units);
        Ok(())
    }
}

#[test]
fn receiving_dependency_refusals_never_reobserve_an_originating_cause() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for lifted in [
            ExpressionCodecError::from(ValueCodecError::Control(cause.clone())),
            ExpressionCodecError::from(
                novarocks_physical_plan::ExprArenaConstructionError::Control(cause.clone()),
            ),
            ExpressionCodecError::from(ConstantReferenceError::Control(cause.clone())),
            ExpressionCodecError::from(ConstantReferenceError::Constant(ConstantError::Control(
                cause.clone(),
            ))),
        ] {
            let control = Control(Mutex::new(Vec::new()));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            // An enclosing scope has completed work before its dependency
            // refuses. That originating refusal must not trigger a new tail.
            work.step().unwrap();
            let result = namespace::finish::<()>(work, Err(lifted));
            assert!(
                matches!(result, Err(ExpressionCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.0.lock().unwrap(), [0]);
        }
    }
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    work.step().unwrap();
    let limit = ConstantReferenceError::Constant(ConstantError::Limit("selected source envelope"));
    assert!(matches!(
        namespace::finish::<()>(work, Err(limit.into())),
        Err(ExpressionCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.0.lock().unwrap(), [0]);
}

#[test]
fn receiving_ordinary_dependency_errors_keep_the_completed_tail_and_typed_detail() {
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    work.step().unwrap();
    let error = ConstantReferenceError::Table(
        novarocks_constant_contract::ConstantTableError::MissingPool(ConstantPoolId::new(u32::MAX)),
    );
    assert!(matches!(namespace::finish::<()>(work, Err(error.into())),
        Err(ExpressionCodecError::Constant(ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::MissingPool(id))))
        if id.get() == u32::MAX));
    assert_eq!(*control.0.lock().unwrap(), [0, 1]);
}

#[test]
fn receiving_sparse_arena_structure_error_keeps_original_detail_and_ordinary_tail() {
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    work.step().unwrap();
    let error = novarocks_physical_plan::ExprArenaConstructionError::DuplicateDefinition(
        novarocks_physical_plan::ExprId::new(u32::MAX),
    );
    assert!(
        matches!(namespace::finish::<()>(work,Err(error.into())),Err(ExpressionCodecError::Arena(novarocks_physical_plan::ExprArenaConstructionError::DuplicateDefinition(id))) if id.get()==u32::MAX)
    );
    assert_eq!(*control.0.lock().unwrap(), [0, 1]);
}
