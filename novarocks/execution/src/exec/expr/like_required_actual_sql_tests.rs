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

//! LIKE source FVT and permanent Frame success oracles.
use super::compiled_program::CompiledExpressionInstance;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source as original_source_programs;
use super::sql_test_source::{
    sql_source_with_single_field, sql_source_with_single_field_and_core_residuals,
};
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind, ProgramRootInput,
    root_input_layout,
};
use novarocks_physical_plan::ExprKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("LIKE never waits")
    }
}
fn inspect(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut n = 0;
    for (&id, fragment) in source.plan().fragments() {
        for (definition, node) in fragment.expressions().iter() {
            if let ExprKind::FunctionCall { function, args } = &node.kind
                && function.function_id.as_str() == "builtin.scalar/__like_with_escape/v1"
            {
                n += 1;
                assert_eq!(args.len(), 3);
                let mut nullable = false;
                for argument in args.iter() {
                    let ty = &fragment.expressions().get(*argument).unwrap().ty;
                    assert_eq!(ty.data_type(), &DataType::Utf8);
                    nullable |= ty.nullable();
                }
                assert_eq!(node.ty.data_type(), &DataType::Boolean);
                assert_eq!(node.ty.nullable(), nullable);
            } else if let ExprKind::Like {
                expr,
                pattern,
                negated,
            } = &node.kind
            {
                n += 1;
                let input = fragment.expressions().get(*expr).unwrap();
                let pattern = fragment.expressions().get(*pattern).unwrap();
                println!(
                    "actual original LIKE fragment={id:?} definition={definition:?} operand={:?} pattern={:?} result={:?} negated={negated}",
                    input.ty, pattern.ty, node.ty
                );
                assert_eq!(input.ty.data_type().clone(), DataType::Utf8);
                assert_eq!(pattern.ty.data_type().clone(), DataType::Utf8);
                assert_eq!(node.ty.data_type().clone(), DataType::Boolean);
                assert_eq!(
                    node.ty.nullable(),
                    input.ty.nullable() || pattern.ty.nullable()
                );
            }
        }
    }
    n
}
#[test]
fn like_sql_source_original_required_name_pattern_full_fvt() {
    for sql in [
        "SELECT name LIKE 'a%' FROM t_filter_in_between_like",
        "SELECT name NOT LIKE 'a%' FROM t_filter_in_between_like",
        "SELECT name LIKE name FROM t_filter_in_between_like",
        "SELECT name LIKE 'a!%' ESCAPE '!' FROM t_filter_in_between_like",
        "SELECT name FROM t_filter_in_between_like WHERE name LIKE 'a%'",
    ] {
        // Exact original required column declaration; isolated LIKE subtree.
        // This does not claim the multi-column native plan was reproduced.
        let source = sql_source_with_single_field(sql, Field::new("name", DataType::Utf8, true));
        assert!(inspect(&source) > 0);
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
    assert!(inspect(&source) > 0);
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
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
                    vec![ProgramExpressionRootSite::Node {
                        node: node.local_id().unwrap(),
                        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
                    }]
                }
                ProgramNodeKind::Filter { .. } if truth => vec![ProgramExpressionRootSite::Node {
                    node: node.local_id().unwrap(),
                    role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
                }],
                ProgramNodeKind::Scan { residuals, .. } if truth => {
                    // The original installed Scan author owns these ordered
                    // roots. Enumerate its exact residual ordinals; no assumed
                    // root zero or synthesized Filter predicate supplies one.
                    residuals
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| ProgramExpressionRootSite::Node {
                            node: node.local_id().unwrap(),
                            role: ProgramNodeExpressionRole::ScanResidual {
                                predicate: u32::try_from(ordinal).unwrap(),
                            },
                        })
                        .collect()
                }
                _ => Vec::new(),
            };
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            for site in sites {
                if snapshot.bindings().contains_key(&site) {
                    assert!(found.is_none(), "fixture has one actual LIKE root");
                    found = Some((program.clone(), site));
                }
            }
        }
    }
    let (program, site) = found.expect("actual original LIKE root");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("actual source owns a scan input")
    };
    let schema = program
        .checked()
        .channels()
        .channel_layout(node, role)
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
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame.evaluate(&data, selection, &Control).unwrap();
        assert!(out.errors().is_empty());
        assert_eq!(out.selection(), selection);
        let indices = UInt64Array::from(
            rows.iter()
                .map(|r| u64::try_from(*r).unwrap())
                .collect::<Vec<_>>(),
        );
        let expected = arrow::compute::take(expected_full.as_ref(), &indices, None).unwrap();
        let wanted = if truth {
            Arc::new(BooleanArray::from(
                expected
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap_or(false))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        } else {
            expected
        };
        assert_eq!(out.values().to_data(), wanted.to_data());
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
