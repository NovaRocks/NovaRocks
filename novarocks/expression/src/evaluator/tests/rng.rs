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

use super::super::{CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure};
use crate::testkit::{
    PassThroughCallHost,
    rng_fixture::{RuntimeControl, source_unit},
};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};

#[test]
fn oversized_actual_empty_port_domain_is_refused_before_controller_allocation() {
    let unit = source_unit();
    let root = RootOrdinal::new(0);
    let schema = unit.scopes()[0].schema().clone();
    assert!(schema.fields().is_empty());
    let input = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(usize::MAX)),
    )
    .unwrap();
    let mut evaluator = CompiledExpressionInstance::try_new(unit, root, &RuntimeControl).unwrap();
    assert!(matches!(
        evaluator.evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(usize::MAX), Selection::all(usize::MAX)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::ResourceExhausted
        ))
    ));
    assert!(evaluator.instances.is_empty());
    assert!(matches!(
        evaluator.evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(usize::MAX), Selection::all(usize::MAX)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
}
