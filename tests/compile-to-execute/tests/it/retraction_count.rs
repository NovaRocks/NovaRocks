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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, testkit::PassThroughCallHost,
};
use novarocks_functions::{
    DataFailureReason, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    ScalarInvocationScope, Selection,
};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results_with_declared_read_fields;

struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("count reconstruction does not wait")
    }
}

#[test]
fn retraction_count_case_preserves_missing_old_state_and_signed_overflow() {
    let fields = [
        Field::new("old_count", DataType::Int64, true),
        Field::new("delta", DataType::Int64, false),
    ];
    let source = sql_source_with_declared_fields(
        "SELECT CASE WHEN old_count IS NULL THEN delta \
         ELSE COALESCE(old_count, BIGINT '0') + delta END FROM fixture.source",
        &fields,
    );
    let programs = compiler_results_with_declared_read_fields(
        &source,
        &fields,
        &installed_builtin_owner_catalogue(),
    );
    let mut site = None;
    for program in programs.values() {
        let program = program.as_ref().unwrap();
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { input, exprs, .. } = node.kind()
                && let [expression] = &exprs[..]
                && matches!(
                    program.graph().nodes()[input.index()].kind(),
                    ProgramNodeKind::Scan { .. }
                )
            {
                assert!(
                    site.replace((
                        program.unit(expression.unit).unwrap().clone(),
                        expression.root
                    ))
                    .is_none()
                );
            }
        }
    }
    let (unit, root) = site.expect("the actual scan-fed count expression");
    let scope = unit.root(root).unwrap().scope;
    let schema = unit.scope(scope).unwrap().schema().clone();
    let old: ArrayRef = Arc::new(Int64Array::from(vec![
        None,
        Some(i64::MAX),
        Some(i64::MIN),
        Some(7),
    ]));
    let delta: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1, -1, -3]));
    let fields = schema.fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields.iter().filter(|field| field.is_nullable()).count(), 1);
    let columns = fields
        .iter()
        .map(|field| {
            if field.is_nullable() {
                old.clone()
            } else {
                delta.clone()
            }
        })
        .collect();
    let data = RecordBatch::try_new(schema, columns).unwrap();
    let selection = Selection::all(4);
    let mut instance = CompiledExpressionInstance::try_new(unit, root, &Control).unwrap();
    let result = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(values, [Some(i64::MAX), None, None, Some(4)]);
    assert_eq!(
        result
            .errors()
            .iter()
            .map(|error| (error.selected_ordinal(), error.reason()))
            .collect::<Vec<_>>(),
        [
            (1, DataFailureReason::NumericOutOfRange),
            (2, DataFailureReason::NumericOutOfRange)
        ],
    );
}
