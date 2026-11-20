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

use arrow::array::BooleanArray;
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, StaticExprKind,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            CallbackControl, NARY_CONTROL as Control, causes,
            nary::{AND, Connective, OR, Use, program, sparse_batch},
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::EvaluationDemand;

#[test]
fn filter_truth_only_nary_preserves_exact_keep_set_without_claiming_null_and_false_are_distinct() {
    for op in [Connective::And, Connective::Or] {
        for width in [3, 320] {
            let program = program(op, Use::TruthOnly, width);
            let (input, rows) = sparse_batch(&program);
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
            let output = program
                .instance(&Control)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            let expected = match op {
                Connective::And => AND,
                Connective::Or => OR,
            };
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value == Some(true))
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|value| value == Some(true))
                    .collect::<Vec<_>>()
            );
            let unit = &program.unit;
            let root_use = unit.root(program.root).unwrap().use_id;
            let flow = unit.control_flow();
            let invocation = &flow.uses()[&root_use];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            assert_eq!(invocation.control, op.shape());
            let arena = unit.definitions();
            let args = match arena.node(invocation.definition).unwrap().kind() {
                StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } => args,
                _ => panic!("actual intrinsic definition"),
            };
            assert_eq!(args.len(), width);
            for (definition, child) in args.iter().zip(&invocation.arguments) {
                assert_eq!(flow.uses()[child].definition, *definition);
                assert_eq!(
                    flow.uses()[child].context.demand,
                    EvaluationDemand::TruthOnly
                );
            }
            assert!(unit.calls().is_empty());
        }
    }
}

#[test]
fn every_nary_constructor_callback_preserves_all_seven_original_failure_categories() {
    let program = program(Connective::And, Use::Value, 320);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX, Control);
    let _ =
        CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("nary-original") {
            let control = CallbackControl::new(cause.clone(), index, Control);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}

#[test]
fn value_boolean_nodes_preserve_all_nine_three_valued_pairs() {
    use crate::zero_port;
    use arrow::{array::ArrayRef, datatypes::DataType};
    use novarocks_expression_contract::ExprKind;
    use novarocks_type_contract::{FunctionValueType, SemanticParameters};
    use std::sync::Arc;

    let result = FunctionValueType::new(DataType::Boolean, true);
    for (left, right, and, or) in [
        (Some(false), Some(false), Some(false), Some(false)),
        (Some(false), Some(true), Some(false), Some(true)),
        (Some(false), None, Some(false), None),
        (Some(true), Some(false), Some(false), Some(true)),
        (Some(true), Some(true), Some(true), Some(true)),
        (Some(true), None, None, Some(true)),
        (None, Some(false), Some(false), None),
        (None, Some(true), None, Some(true)),
        (None, None, None, None),
    ] {
        for (disjunction, expected) in [(false, and), (true, or)] {
            let inputs =
                [left, right].map(|value| Arc::new(BooleanArray::from(vec![value])) as ArrayRef);
            let unit = zero_port::primitive(
                inputs,
                result.clone(),
                |children| {
                    let args = children.into_boxed_slice();
                    if disjunction {
                        ExprKind::Disjunction { args }
                    } else {
                        ExprKind::Conjunction { args }
                    }
                },
                &SemanticParameters::default(),
            )
            .unwrap();
            let output = zero_port::evaluate(unit);
            assert!(output.errors().is_empty());
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .next(),
                Some(expected),
            );
        }
    }
}
