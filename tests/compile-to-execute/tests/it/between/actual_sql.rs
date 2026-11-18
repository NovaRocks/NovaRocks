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

//! Actual SQL/package/Frame witnesses with independent interval truth tables.
use arrow::array::{ArrayRef, BooleanArray, Decimal128Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_sql_testkit::sql_source;
use novarocks_test_compile_to_execute::programs::programs_with_catalogue_and_original_scan_residuals;
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BETWEEN never waits")
    }
}
fn between_count(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .fragments()
        .values()
        .flat_map(novarocks_sql_testkit::unit_definitions)
        .filter(|(_, _, _, e)| matches!(e.kind, ExprKind::Between { .. }))
        .count()
}
fn evaluate(sql: &str, input: ArrayRef, expected: &[Option<bool>], truth: bool) {
    assert_eq!(expected.len(), input.len());
    let source = sql_source(sql, input.data_type().clone());
    assert_eq!(
        between_count(&source),
        1,
        "fixture must preserve actual BETWEEN, not SQL rewrite into an assumed closure"
    );
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_scan_residuals(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            let site = match node.kind() {
                ProgramNodeKind::Project { exprs, .. }
                    if !truth
                        && exprs.len() == 1
                        && node.output_layout().schema().field(0).data_type()
                            == &DataType::Boolean =>
                {
                    exprs[0]
                }
                ProgramNodeKind::Filter { predicates, .. } if truth => predicates[0],
                ProgramNodeKind::Scan { residuals, .. } if truth && residuals.len() == 1 => {
                    residuals[0]
                }
                _ => continue,
            };

            if program
                .unit(site.unit)
                .and_then(|unit| unit.root(site.root))
                .is_some()
            {
                assert!(found.is_none(), "fixture owns one actual predicate root");
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual SQL predicate producer");
    let unit = program.unit(site.unit).unwrap();
    let schema = unit
        .scope(unit.root(site.root).unwrap().scope)
        .unwrap()
        .schema()
        .clone();
    // The actual source preserves a one-column scan; do not retag or forge FVT.
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    for rows in [(0..input.len()).collect::<Vec<_>>(), vec![0], Vec::new()] {
        let selection = Selection::try_sparse(input.len(), &rows).unwrap();
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
        )
        .unwrap();
        let out = frame
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
        assert!(out.errors().is_empty());
        // The SQL predicate's value channel is three-valued. Its truth-only
        // filter channel maps UNKNOWN to false, independently of Frame output.
        let wanted: ArrayRef = if truth {
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| expected[*row].unwrap_or(false))
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| expected[*row]).collect::<Vec<_>>(),
            ))
        };
        assert_eq!(out.values().to_data(), wanted.to_data());
    }
}
#[test]
fn between_actual_compiler_positive_value_and_truthonly_int64_source() {
    for (sql, truth) in [
        (
            "SELECT k BETWEEN 1 AND 5 AS original_range FROM fixture.source",
            false,
        ),
        ("SELECT k FROM fixture.source WHERE k BETWEEN 1 AND 5", true),
    ] {
        evaluate(
            sql,
            Arc::new(Int64Array::from(vec![0, 1, 3, 5, 9])),
            // Inclusive interval [1, 5]: 0 and 9 are outside; both ends
            // and the interior value 3 are inside.
            &[Some(false), Some(true), Some(true), Some(true), Some(false)],
            truth,
        );
    }
}
#[test]
fn between_actual_compiler_negated_value_source() {
    evaluate(
        "SELECT k NOT BETWEEN 1 AND 5 AS original_range FROM fixture.source",
        Arc::new(Int64Array::from(vec![0, 1, 3, 5, 9])),
        // With no NULL inputs, NOT BETWEEN complements [1, 5].
        &[
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ],
        false,
    );
}
#[test]
fn between_actual_compiler_required_decimal_bound_source() {
    let make = |p, s, values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    evaluate(
        "SELECT k BETWEEN CAST(100.0 AS DECIMAL(6,1)) AND CAST(150 AS DECIMAL(4,0)) AS original_range FROM fixture.source",
        make(7, 2, vec![12000, 9000]),
        // Coefficient 12000 at scale 2 is 120, between 1000/10 and
        // 150/1; coefficient 9000 is 90 and is below the lower bound.
        &[Some(true), Some(false)],
        false,
    );
}

#[test]
fn between_actual_compiler_decimal_literals_without_integral_cast_source() {
    let make = |values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(7, 2)
                .unwrap(),
        ) as ArrayRef
    };
    // This separate fixture leaves the required mixed-bound test untouched.
    // Decimal literals avoid the independently missing Int64-to-Decimal128 cast.
    evaluate(
        "SELECT k BETWEEN 100.0 AND 150.0 AS original_range FROM fixture.source",
        make(vec![12000, 9000]),
        // Decimal literals [100.0, 150.0] name the same numeric interval
        // independently of the mixed-precision bound carrier cast above.
        &[Some(true), Some(false)],
        false,
    );
}
