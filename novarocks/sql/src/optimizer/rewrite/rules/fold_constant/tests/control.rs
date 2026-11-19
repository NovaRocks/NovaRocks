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
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use novarocks_functions::ConstantValue;
use novarocks_type_contract::{CompileControlError, PureCompileControl};

use super::{fixtures::*, *};
use crate::compiler::SqlFoldDependencyObserver;

#[derive(Default)]
struct TraceControl {
    steps: Mutex<Vec<(CompilePhase, u32)>>,
    reject: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut steps = self.steps.lock().unwrap();
        steps.push((phase, units));
        if let Some((ordinal, error)) = &self.reject
            && steps.len() == *ordinal
        {
            return Err(error.clone());
        }
        Ok(())
    }
}

fn apply_with_control(
    fixture: &Fixture,
    plan: OptExpr,
    control: &dyn PureCompileControl,
) -> Result<RewriteResult, SqlCompileError> {
    let mut ctx = RewriteContext::for_query_with_settings(
        Default::default(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        control,
    );
    install_inputs(&mut ctx, Rc::clone(&fixture.arena));
    FoldConstant.apply(plan, &mut ctx)
}

#[test]
fn fold_control_observes_real_nodes_cached_edges_and_final_publication() {
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let product = fixture.binary(BinOp::Mul, sum, sum);
    let trace = TraceControl::default();
    assert_int_literal(
        &fixture,
        project_expr(&changed(
            apply_with_control(&fixture, project(product), &trace).unwrap(),
        )),
        4,
    );
    let steps = trace.steps.into_inner().unwrap();
    for phase in [
        CompilePhase::Validate,
        CompilePhase::LowerProgram,
        CompilePhase::FunctionSpecialization,
    ] {
        let ordinal = steps
            .iter()
            .position(|&(observed, _)| observed == phase)
            .unwrap()
            + 1;
        let trace = TraceControl {
            reject: Some((ordinal, CompileControlError::Cancelled)),
            ..Default::default()
        };
        assert!(matches!(
            apply_with_control(&fixture, project(product), &trace),
            Err(SqlCompileError::Cancelled)
        ));
    }
    let trace = TraceControl {
        reject: Some((steps.len(), CompileControlError::DeadlineExceeded)),
        ..Default::default()
    };
    assert!(matches!(
        apply_with_control(&fixture, project(product), &trace),
        Err(SqlCompileError::DeadlineExceeded)
    ));
}

#[test]
fn fold_control_covers_no_evaluator_and_declined_or_failed_folds() {
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let zero = fixture.int_literal(0);
    let miss = fixture.binary(BinOp::Div, one, zero);
    let trace = TraceControl::default();
    assert!(matches!(
        apply_with_control(&fixture, project(miss), &trace).unwrap(),
        RewriteResult::Unchanged
    ));
    let steps = trace.steps.into_inner().unwrap();
    let trace = TraceControl {
        reject: Some((steps.len(), CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        apply_with_control(&fixture, project(miss), &trace),
        Err(SqlCompileError::Cancelled)
    ));
}

#[test]
fn folded_output_and_arena_do_not_retain_request_control() {
    struct LifetimeControl(Arc<()>);
    impl PureCompileControl for LifetimeControl {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            assert_eq!(Arc::strong_count(&self.0), 1);
            Ok(())
        }
    }

    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let token = Arc::new(());
    let weak: Weak<()> = Arc::downgrade(&token);
    let control = LifetimeControl(token);
    let result = changed(apply_with_control(&fixture, project(sum), &control).unwrap());
    drop(control);
    assert!(weak.upgrade().is_none());
    assert_int_literal(&fixture, project_expr(&result), 2);
}

#[test]
fn evaluator_receives_original_phase_control_and_control_errors_never_decline() {
    struct RejectReceipt;
    impl SqlFoldDependencyObserver for RejectReceipt {
        fn observe_fold_unit_observed(
            &self,
            _source: &SqlAuthoredScalarUnit,
            result: Option<&ConstantValue>,
            control: &dyn PureCompileControl,
        ) -> Result<(), CompileControlError> {
            assert!(result.is_some());
            control.checkpoint(CompilePhase::FunctionSpecialization, u32::MAX)
        }
    }
    struct RejectReceiptWork;
    impl PureCompileControl for RejectReceiptWork {
        fn checkpoint(&self, _phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            if units == u32::MAX {
                return Err(CompileControlError::ResourceExhausted);
            }
            Ok(())
        }
    }
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let mut ctx = RewriteContext::for_query_with_settings(
        Default::default(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        &RejectReceiptWork,
    );
    install_inputs(&mut ctx, Rc::clone(&fixture.arena));
    ctx.set_fold_dependency_observer(Some(Arc::new(RejectReceipt)));
    for _attempt in 0..2 {
        assert!(matches!(
            FoldConstant.apply(project(sum), &mut ctx),
            Err(SqlCompileError::ResourceExhausted)
        ));
    }
}
