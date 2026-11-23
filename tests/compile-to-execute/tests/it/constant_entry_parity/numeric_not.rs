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

use super::{Control, Expected, Family, Fixture, FoldOutcome, boolean};
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{CompiledExpressionInstance, EvaluationInput};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use std::sync::Arc;

#[test]
fn numeric_not_actual_sql_program_and_constant_entry_keep_the_same_sparse_values() {
    use novarocks_sql_testkit::sql_source;
    use novarocks_test_compile_to_execute::programs::{batch, programs_with_catalogue};
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let inputs: Vec<(&str, [&str; 4], ArrayRef)> = vec![
        (
            "TINYINT",
            ["0", "1", "0-1", "0"],
            Arc::new(arrow::array::Int8Array::from(vec![0, 1, -1, 0])),
        ),
        (
            "SMALLINT",
            ["0", "1", "0-1", "0"],
            Arc::new(arrow::array::Int16Array::from(vec![0, 1, -1, 0])),
        ),
        (
            "INT",
            ["0", "1", "0-1", "0"],
            Arc::new(arrow::array::Int32Array::from(vec![0, 1, -1, 0])),
        ),
        (
            "BIGINT",
            ["0", "1", "0-1", "0"],
            Arc::new(Int64Array::from(vec![0, 1, -1, 0])),
        ),
        (
            "REAL",
            ["0e0", "1e0", "'NaN'", "'-0'"],
            Arc::new(arrow::array::Float32Array::from(vec![
                0.0,
                1.0,
                f32::NAN,
                -0.0,
            ])),
        ),
        (
            "DOUBLE",
            ["0e0", "1e0", "'NaN'", "'-0'"],
            Arc::new(Float64Array::from(vec![0.0, 1.0, f64::NAN, -0.0])),
        ),
        (
            "DECIMAL(18,4)",
            ["'0'", "'0.0001'", "'-0.0001'", "'0'"],
            Arc::new(
                arrow::array::Decimal128Array::from(vec![0, 1, -1, 0])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
            ),
        ),
    ];
    for (sql_type, literals, input) in inputs {
        let source = sql_source(
            "SELECT NOT k AS inverted FROM fixture.source",
            input.data_type().clone(),
        );
        let programs = programs_with_catalogue(&source, &functions);
        let mut found = None;
        for program in programs.values() {
            for (unit_index, unit) in program.units().iter().enumerate() {
                for (ordinal, root) in unit.roots().iter().enumerate() {
                    if matches!(
                        unit.definitions().node(root.definition).unwrap().kind(),
                        novarocks_expression::StaticExprKind::Not(_)
                    ) {
                        assert!(found.is_none());
                        let reference = novarocks_local_program::ProgramRootRef {
                            unit: novarocks_local_program::ProgramUnitId::new(unit_index),
                            root: novarocks_expression_contract::RootOrdinal::new(
                                u32::try_from(ordinal).unwrap(),
                            ),
                        };
                        found = Some((program.clone(), reference));
                    }
                }
            }
        }
        let (program, root) = found.expect("published numeric NOT root");
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &Control,
        )
        .unwrap();
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(
                    novarocks_functions::Selection::all(4),
                    novarocks_functions::Selection::all(4),
                ),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output.values().to_data(),
            BooleanArray::from(vec![true, false, false, true]).to_data()
        );
        let rows = [0, 2];
        let sparse = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = novarocks_functions::Selection::try_sparse(4, &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(sparse.errors().is_empty());
        assert_eq!(
            sparse.values().to_data(),
            BooleanArray::from(vec![true, false]).to_data()
        );
        for (literal, expected) in literals.into_iter().zip([true, false, false, true]) {
            let expression = format!("NOT CAST({literal} AS {sql_type})");
            Fixture::compile(&expression, Family::Not, "32", &functions).assert_both(
                &functions,
                FoldOutcome::Folded,
                Expected::Value(boolean(Some(expected))),
            );
        }
    }
}
