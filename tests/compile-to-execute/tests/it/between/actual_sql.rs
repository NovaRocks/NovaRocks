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
use arrow::datatypes::{DataType, Field};
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
use novarocks_sql_testkit::{sql_source, try_sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::programs::{
    producer_root, programs_with_catalogue_and_original_scan_residuals,
};
use novarocks_type_contract::OperatorFunction;
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
fn comparison_count(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .fragments()
        .values()
        .flat_map(novarocks_sql_testkit::unit_definitions)
        .filter(|(_, _, _, e)| matches!(&e.kind, ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == OperatorFunction::LessThanOrEqual.function_id_text()))
        .count()
}
fn evaluate(sql: &str, input: ArrayRef, expected: &[Option<bool>], truth: bool) {
    assert_eq!(expected.len(), input.len());
    let source = sql_source(sql, input.data_type().clone());
    assert_eq!(
        comparison_count(&source),
        2,
        "fixture must preserve both authored range comparisons"
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
fn between_actual_sql_roundtrip_preserves_the_original_bound_depth_boundary() {
    for (negated, explicit_not) in [(false, false), (true, false), (false, true)] {
        check_bound_depth_boundary(negated, explicit_not);
    }
}

fn check_bound_depth_boundary(negated: bool, explicit_not: bool) {
    use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};

    let fields = [Field::new("k", DataType::Int64, false)];
    let predicate = if negated { "NOT BETWEEN" } else { "BETWEEN" };
    let prefix = if explicit_not { "NOT " } else { "" };
    let last_admitted = if explicit_not { 93 } else { 94 };
    for (probe, is_dynamic, lower_hit, upper_hit) in [
        (
            "k",
            false,
            [true, true, true, false],
            [false, true, true, true],
        ),
        ("(k + 1)", true, [true, true, false, false], [false; 4]),
    ] {
        for (deep_lower_bound, wanted) in [(true, lower_hit), (false, upper_hit)] {
            let sql = |nested: usize| {
                let operand = format!("{}k{}", "abs(".repeat(nested), ")".repeat(nested));
                let (low, high) = if deep_lower_bound {
                    (operand.as_str(), "8")
                } else {
                    ("1", operand.as_str())
                };
                format!(
                    "SELECT {prefix}({probe} {predicate} {low} AND {high}) AS in_range FROM fixture.source"
                )
            };
            let source = try_sql_source_with_declared_fields(&sql(last_admitted), &fields).unwrap_or_else(|error| {
                panic!("previously admitted {prefix}{predicate} with probe {probe} and deep lower bound {deep_lower_bound} failed: {error:?}")
            });
            assert_eq!(comparison_count(&source), 2);
            let mut expanded_depth = 0;
            let mut bindings = 0;
            let mut expansions = 0;
            let programs =
                crate::fixtures::compile_sql_package_roundtrip(&source, &fields, |package| {
                    for node in package.fragment().nodes().values() {
                        let Some(unit) = node.expressions.as_ref() else {
                            continue;
                        };
                        bindings += unit
                            .definitions()
                            .iter()
                            .filter(|node| matches!(node.kind, ExprKind::Let { .. }))
                            .count();
                        expansions += unit.control_expansions().records().len();
                        expanded_depth =
                            expanded_depth.max(unit.control_flow().physical_use_depth());
                    }
                });
            assert_eq!(bindings, usize::from(is_dynamic));
            assert_eq!(expansions, 1);
            assert_eq!(
                expanded_depth,
                97 + usize::from(is_dynamic) + usize::from(negated)
            );
            eprintln!(
                "{predicate} probe={probe}, explicit_not={explicit_not}, deep_lower_bound={deep_lower_bound}, authored_use_depth={expanded_depth}"
            );
            let (program, site) = producer_root(&programs);
            let unit = program.unit(site.unit).unwrap();
            let schema = unit
                .scope(unit.root(site.root).unwrap().scope)
                .unwrap()
                .schema()
                .clone();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![0i64, 1, 8, 9]))],
            )
            .unwrap();
            let rows = Selection::all(batch.num_rows());
            let output = CompiledExpressionInstance::try_new(Arc::clone(unit), site.root, &Control)
                .unwrap()
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(rows, rows),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            let actual = output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert_eq!(
                actual.iter().collect::<Vec<_>>(),
                wanted.map(|value| Some(value ^ (negated || explicit_not)))
            );

            let Err(SqlCompileProgressError::Compile(SqlCompileError::CompilationSource(cause))) =
                try_sql_source_with_declared_fields(&sql(last_admitted + 1), &fields)
            else {
                panic!(
                    "the first source above the original control depth must fail during compilation"
                );
            };
            assert_eq!(cause.detail(), "invalid expression control flow: TooDeep");
        }
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
