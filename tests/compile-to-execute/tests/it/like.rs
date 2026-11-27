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

//! Actual original LIKE source FVT and permanent Frame success oracles.
//! Before preparation these are RED at UnsupportedExpression(Like).
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{inspect_like_source, sql_source_with_single_field_and_core_residuals};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue_and_original_inlist_source as original_source_programs;
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("LIKE never waits")
    }
}
fn evaluate(
    sql: &str,
    field: Field,
    input: ArrayRef,
    expected_rows: Vec<Option<bool>>,
    truth: bool,
) {
    // Same original SQL's legitimate optimizer variant, not a claim that
    // the native default producer emitted this exact residual site.
    let source = sql_source_with_single_field_and_core_residuals(sql, field);
    assert!(inspect_like_source(&source) > 0);
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    // This actual package effects author is deliberately RED before LIKE support.
    let programs = original_source_programs(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            let sites = match node.kind() {
                ProgramNodeKind::Project { exprs, .. }
                    if !truth
                        && exprs.len() == 1
                        && node.output_layout().schema().field(0).data_type()
                            == &DataType::Boolean =>
                {
                    vec![exprs[0]]
                }
                ProgramNodeKind::Filter { predicates, .. } if truth => predicates.to_vec(),
                ProgramNodeKind::Scan { residuals, .. } if truth => {
                    // The original installed Scan author owns these ordered
                    // roots. Enumerate its exact residual ordinals; no assumed
                    // root zero or synthesized Filter predicate supplies one.
                    residuals.to_vec()
                }
                _ => Vec::new(),
            };

            for site in sites {
                if program
                    .unit(site.unit)
                    .and_then(|unit| unit.root(site.root))
                    .is_some()
                {
                    assert!(found.is_none(), "fixture has one actual LIKE root");
                    found = Some((program.clone(), site));
                }
            }
        }
    }
    let (program, site) = found.expect("actual original LIKE root");
    let unit = program.unit(site.unit).unwrap();
    let schema = unit
        .scope(unit.root(site.root).unwrap().scope)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    // These fixed truth/NULL vectors come from the explicit SQL patterns and
    // inputs below. No expression evaluator authors the expected values.
    assert_eq!(expected_rows.len(), input.len());
    let expected_full: ArrayRef = Arc::new(BooleanArray::from(expected_rows));
    for rows in [
        (0..input.len()).collect::<Vec<_>>(),
        (0..input.len()).filter(|r| r % 2 == 0).collect(),
        Vec::new(),
    ] {
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
        assert_eq!(out.selection(), selection);
        let indices = UInt64Array::from(
            rows.iter()
                .map(|r| u64::try_from(*r).unwrap())
                .collect::<Vec<_>>(),
        );
        let expected = arrow::compute::take(expected_full.as_ref(), &indices, None).unwrap();
        assert_eq!(out.values().to_data(), expected.to_data());
    }
}

fn input() -> ArrayRef {
    Arc::new(StringArray::from(vec![
        Some("apple"),
        Some("banana"),
        None,
        Some("é中"),
        Some("a\0b"),
        Some("a%b"),
        Some(r"a\"),
    ]))
}
#[test]
fn like_actual_compiler_value_original_required_prefix() {
    evaluate(
        "SELECT name LIKE 'a%' FROM t_filter_in_between_like",
        Field::new("name", DataType::Utf8, true),
        input(),
        vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ],
        false,
    );
}
#[test]
fn like_actual_compiler_notlike_dynamic_source_value() {
    // Every non-NULL input matches its own pattern, including '%' and the
    // historical literal terminal backslash. NOT LIKE therefore yields false.
    evaluate(
        "SELECT name NOT LIKE name FROM t_filter_in_between_like",
        Field::new("name", DataType::Utf8, true),
        input(),
        vec![
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        ],
        false,
    );
}
#[test]
fn like_actual_compiler_truthonly_original_required_prefix() {
    evaluate(
        "SELECT name FROM t_filter_in_between_like WHERE name LIKE 'a%'",
        Field::new("name", DataType::Utf8, true),
        input(),
        vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ],
        true,
    );
}
#[test]
fn like_actual_compiler_notlike_truthonly_original_required_prefix() {
    evaluate(
        "SELECT name FROM t_filter_in_between_like WHERE name NOT LIKE 'a%'",
        Field::new("name", DataType::Utf8, true),
        input(),
        vec![
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
        ],
        true,
    );
}

#[test]
fn like_actual_compiler_explicit_escape_matches_literal_percent() {
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a!x"),
        Some("a%"),
        None,
        Some("a!"),
    ]));
    evaluate(
        "SELECT name LIKE 'a!%' ESCAPE '!' FROM t_filter_in_between_like",
        Field::new("name", DataType::Utf8, true),
        input,
        vec![Some(false), Some(true), None, Some(false)],
        false,
    );
}

#[test]
fn like_actual_compiler_unescaped_backslash_is_literal() {
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a_b"),
        Some(r"a\xb"),
        None,
        Some(r"a\_b"),
    ]));
    evaluate(
        r"SELECT name LIKE 'a\_b' FROM t_filter_in_between_like",
        Field::new("name", DataType::Utf8, true),
        input,
        vec![Some(false), Some(true), None, Some(true)],
        false,
    );
}
