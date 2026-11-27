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

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, testkit::PassThroughCallHost,
};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;
use novarocks_type_contract::OperatorFunction;

struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("comparison arguments do not wait")
    }
}

fn evaluate_comparison(
    expression: &str,
    selected: OperatorFunction,
    input: Vec<i64>,
) -> (Vec<Option<bool>>, Vec<(usize, String)>) {
    let source = sql_source(
        &format!("SELECT {expression} FROM fixture.source"),
        DataType::Int64,
    );
    let calls = source
        .plan()
        .fragments()
        .values()
        .flat_map(novarocks_sql_testkit::unit_definitions)
        .filter(|(_, _, _, definition)| {
            matches!(
                &definition.kind,
                ExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str() == selected.function_id_text()
            )
        })
        .count();
    assert_eq!(
        calls, 1,
        "expected one selected {selected:?} call for {expression}"
    );
    let programs = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { exprs, .. } = node.kind()
                && let [site] = &exprs[..]
                && node.output_layout().schema().field(0).data_type() == &DataType::Boolean
            {
                assert!(found.replace((program, *site)).is_none());
            }
        }
    }
    let (program, site) = found.expect("the actual Boolean projection");
    let unit = program.unit(site.unit).unwrap();
    let scope = unit.root(site.root).unwrap().scope;
    let schema = unit.scope(scope).unwrap().schema().clone();
    let input: ArrayRef = Arc::new(Int64Array::from(input));
    let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
    let selection = Selection::all(batch.num_rows());
    let mut instance =
        CompiledExpressionInstance::try_new(unit.clone(), site.root, &Control).unwrap();
    let result = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let errors = result
        .errors()
        .iter()
        .map(|error| (error.selected_ordinal(), error.message().to_owned()))
        .collect();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect();
    (values, errors)
}

#[test]
fn ordinary_greater_comparisons_freeze_the_right_operand_as_the_first_demand() {
    for (symbol, selected, reason) in [
        (
            ">",
            OperatorFunction::LessThan,
            "Cast value is outside the target numeric range",
        ),
        (
            ">=",
            OperatorFunction::LessThanOrEqual,
            "Cast value is outside the target numeric range",
        ),
        ("<=", OperatorFunction::LessThanOrEqual, "Division by zero"),
    ] {
        let expression = format!("BIGINT '1' / (k - k) {symbol} CAST(k AS TINYINT)");
        let (_, errors) = evaluate_comparison(&expression, selected, vec![256]);
        assert_eq!(errors, vec![(0, reason.to_owned())], "{expression}");
    }
}

#[test]
fn ordinary_greater_comparisons_short_circuit_after_the_selected_first_null() {
    let left = "BIGINT '10' / k";
    let right = "IF(k = 0, CAST(NULL AS BIGINT), BIGINT '10')";
    for (symbol, selected, expected) in [
        (">", OperatorFunction::LessThan, false),
        (">=", OperatorFunction::LessThanOrEqual, true),
    ] {
        let expression = format!("{left} {symbol} {right}");
        let (values, errors) = evaluate_comparison(&expression, selected, vec![0, 1]);
        assert!(errors.is_empty(), "{expression}");
        assert_eq!(values, vec![None, Some(expected)], "{expression}");
    }
    let expression = format!("{left} <= {right}");
    let (_, errors) =
        evaluate_comparison(&expression, OperatorFunction::LessThanOrEqual, vec![0, 1]);
    assert_eq!(errors, vec![(0, "Division by zero".to_owned())]);
}
