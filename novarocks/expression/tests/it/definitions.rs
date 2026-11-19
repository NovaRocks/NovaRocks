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

use arrow::datatypes::DataType;
use novarocks_expression::*;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
struct TestControl {
    trace: std::sync::Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl TestControl {
    fn new(stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: std::sync::Mutex::new(Vec::new()),
            stop,
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for TestControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push(units);
        if let Some((at, cause)) = self.stop.clone()
            && index == at
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn boolean() -> novarocks_functions::FunctionValueType {
    novarocks_functions::FunctionValueType::new(DataType::Boolean, false)
}
fn slot() -> StaticExprNode {
    StaticExprNode::new_value_typed(StaticExprKind::Input, boolean())
}
#[test]
fn rejects_forward_references_and_cycles_before_freeze() {
    let nodes = vec![StaticExprNode::new_value_typed(
        StaticExprKind::Not(CompiledExprId::new(0)),
        boolean(),
    )];
    assert!(matches!(
        ImmutableExpressions::try_new_for_compile(nodes, &TestControl::new(None)),
        Err(ExpressionsCompileError::Expression(
            StaticExpressionError::InvalidReference
        ))
    ));
}
fn assert_every_checkpoint_refuses(nodes: &[StaticExprNode]) -> Vec<u32> {
    let baseline = TestControl::new(None);
    ImmutableExpressions::try_new_for_compile(nodes.to_vec(), &baseline).unwrap();
    let trace = baseline.trace();
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = TestControl::new(Some((at, cause.clone())));
            let result = ImmutableExpressions::try_new_for_compile(nodes.to_vec(), &control);
            assert!(
                matches!(result,Err(ExpressionsCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(
                control.trace(),
                trace[..=at],
                "no callback after the first interruption"
            );
        }
    }
    trace
}
#[test]
fn compile_reference_loop_observes_real_quantum_and_all_tails() {
    let nodes = vec![
        slot(),
        StaticExprNode::new_value_typed(
            StaticExprKind::NaryAnd {
                args: vec![CompiledExprId::new(0); 320],
            },
            boolean(),
        ),
    ];
    let trace = assert_every_checkpoint_refuses(&nodes);
    assert_eq!(trace.first(), Some(&0));
    assert!(trace.contains(&256));
    assert!(trace.iter().any(|units| *units > 0 && *units < 256));
    assert_eq!(trace.last(), Some(&0));
}
#[test]
fn compile_ordinary_error_tail_preserves_original_control_and_expression_error() {
    let nodes = vec![StaticExprNode::new_value_typed(
        StaticExprKind::Not(CompiledExprId::new(0)),
        boolean(),
    )];
    let baseline = TestControl::new(None);
    assert!(matches!(
        ImmutableExpressions::try_new_for_compile(nodes.clone(), &baseline),
        Err(ExpressionsCompileError::Expression(
            StaticExpressionError::InvalidReference
        ))
    ));
    let trace = baseline.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = TestControl::new(Some((trace.len() - 1, cause.clone())));
        assert!(
            matches!(ImmutableExpressions::try_new_for_compile(nodes.clone(),&control),Err(ExpressionsCompileError::Control(actual)) if actual==cause)
        );
    }
}
