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

use arrow::{
    array::{Array, ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, StaticExprKind,
    testkit::{HarnessControl, PassThroughCallHost},
};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    FunctionArgumentType, ScalarInvocationActivation, ScalarInvocationScope, Selection,
    testkit::installed_builtin_owner_catalogue,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_sql_testkit::sql_source_with_declared_fields;
use novarocks_test_compile_to_execute::compiler_results::compiler_results;
use novarocks_type_contract::FunctionValueType;
use std::{collections::BTreeMap, sync::Arc};

fn substring_fields() -> Vec<Field> {
    [
        ("txt", DataType::Utf8),
        ("delim", DataType::Utf8),
        ("cnt", DataType::Int32),
    ]
    .into_iter()
    .map(|(n, t)| Field::new(n, t, true))
    .collect()
}

fn sql_frame(
    sql: &str,
    fields: &[Field],
    name: &str,
    inputs: &[Option<ArrayRef>],
) -> (Arc<LocalProgram>, ProgramRootRef, RecordBatch) {
    let source = sql_source_with_declared_fields(sql, fields);
    let functions = installed_builtin_owner_catalogue();
    let mut found = None;
    for program in compiler_results(&source, &functions).into_values() {
        let program = Arc::new(program.expect("original SQL package compiles"));
        for node in program.graph().nodes() {
            let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                continue;
            };
            let layout = program.graph().nodes()[input.index()].output_layout();
            for &site in exprs.iter() {
                let unit = program.unit(site.unit).unwrap();
                let root = unit.root(site.root).unwrap();
                let Some(call) = unit.calls().get(&root.use_id) else {
                    continue;
                };
                if call.call_contract().function_id().as_str()
                    != format!("builtin.scalar/{name}/v1")
                {
                    continue;
                }
                let arena = unit.definitions();
                let flow = unit.control_flow();
                let invocation = &flow.uses()[&root.use_id];
                let StaticExprKind::BoundCall { args, .. } =
                    arena.node(root.definition).unwrap().kind()
                else {
                    panic!("original bound root");
                };
                assert_eq!(args.len(), inputs.len());
                let mut input_slots = BTreeMap::new();
                for (&use_id, values) in invocation.arguments.iter().zip(inputs) {
                    let Some(values) = values else { continue };
                    let binding = unit.input(use_id).expect("actual compiled input use");
                    assert_eq!(binding.scope, root.scope);
                    assert!(
                        input_slots
                            .insert(layout.slots()[binding.ordinal as usize], values.clone())
                            .is_none()
                    );
                }
                let arrays = layout
                    .slots()
                    .iter()
                    .map(|slot| {
                        input_slots
                            .remove(slot)
                            .expect("each original input slot has explicit data")
                    })
                    .collect();
                assert!(input_slots.is_empty());
                let batch = RecordBatch::try_new(layout.schema().clone(), arrays).unwrap();
                assert!(
                    found.replace((program.clone(), site, batch)).is_none(),
                    "one original producer"
                );
            }
        }
    }
    found.expect("actual installed producer root")
}

fn substring_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<String>> {
    assert!(output.errors().is_empty());
    assert_eq!(output.values().data_type(), &DataType::Utf8);
    output
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}

#[test]
fn c12_p3_substring_index_actual_compiled_slots_constants_strict_null_and_explicit_empty_match_v1_sql()
 {
    let mut observations = 0;
    for all_null in [false, true] {
        let text = if all_null {
            vec![None; 4]
        } else {
            vec![Some("aaaaa"), None, Some("é::你好::\0"), Some("/edge/")]
        };
        let text = Arc::new(StringArray::from(text)) as ArrayRef;
        let delimiter = Arc::new(StringArray::from(vec![
            Some("aa"),
            Some("/"),
            Some("::"),
            Some("/"),
        ])) as ArrayRef;
        for (count_sql, counts, expected) in [
            (
                "cnt",
                Some(
                    Arc::new(Int32Array::from(vec![Some(-1), Some(0), Some(-2), Some(1)]))
                        as ArrayRef,
                ),
                vec![Some("a"), None, Some("你好::\0"), Some("")],
            ),
            ("-1", None, vec![Some("a"), None, Some("\0"), Some("")]),
            // The frozen SUBSTRING_INDEX owner returns SQL NULL for zero count.
            ("0", None, vec![None, None, None, None]),
        ] {
            let (program, site, batch) = sql_frame(
                &format!("SELECT substring_index(txt,delim,{count_sql}) FROM t"),
                &substring_fields(),
                "substring_index",
                &[Some(text.clone()), Some(delimiter.clone()), counts],
            );
            for rows in [&[0, 1, 2, 3][..], &[0, 2, 3][..], &[][..]] {
                let selection = if rows.len() == 4 {
                    Selection::all(4)
                } else {
                    Selection::try_sparse(4, rows).unwrap()
                };
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &HarnessControl,
                )
                .unwrap();
                let output = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &HarnessControl,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(output.selection(), selection);
                let expected = rows
                    .iter()
                    .map(|row| {
                        if all_null {
                            None
                        } else {
                            expected[*row].map(str::to_owned)
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(substring_values(&output), expected);
                observations += 1;
            }
        }
    }
    assert_eq!(observations, 18);
}

#[test]
fn pure_differential_field_actual_sql_null_coercion_and_generic_nominal_tags_sql() {
    let fields = [
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let sql = "SELECT field(NULL,a,b) FROM t";
    let source = sql_source_with_declared_fields(sql, &fields);
    let mut found = 0;
    for fragment in source.plan().fragments().values() {
        for unit in fragment
            .nodes()
            .values()
            .filter_map(|node| fragment.unit(node.id))
        {
            for node in unit.definitions() {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/field/v1" {
                    continue;
                }
                let request = unit.requests().get(&node.id).unwrap();
                assert_eq!(args.len(), 3);
                assert_eq!(request.logical_argument_count, 3);
                assert_eq!(request.arguments.len(), 3);
                assert_eq!(function.argument_types.len(), 3);
                for (ordinal, arg) in args.iter().enumerate() {
                    let StaticFunctionArgument::Value { value_type, .. } =
                        &request.arguments[ordinal]
                    else {
                        panic!("real source value")
                    };
                    let FunctionArgumentType::Value(selected) = &function.argument_types[ordinal]
                    else {
                        panic!("selected FIELD value")
                    };
                    assert_eq!(value_type, selected);
                    assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                    assert_eq!(&unit.definition(*arg).unwrap().ty, selected);
                }
                assert_eq!(node.ty, FunctionValueType::new(DataType::Int32, false));
                found += 1;
            }
        }
    }
    assert!(found > 0, "actual SQL NULL is coerced to selected UTF8");
    let (program, site, batch) = sql_frame(
        sql,
        &fields,
        "field",
        &[
            None,
            Some(Arc::new(StringArray::from(vec!["a", "b", "c", "d"]))),
            Some(Arc::new(StringArray::from(vec!["b", "a", "c", "x"]))),
        ],
    );
    for rows in [&[0, 1, 2, 3][..], &[0, 2, 3][..], &[][..]] {
        let selection = Selection::try_sparse(4, rows).unwrap();
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &HarnessControl,
        )
        .unwrap();
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(output.selection(), selection);
        assert_eq!(output.values().data_type(), &DataType::Int32);
        assert_eq!(output.values().null_count(), 0);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            vec![0; rows.len()].as_slice()
        );
    }
}
