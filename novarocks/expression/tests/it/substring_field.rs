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

use super::string_fixture::{COMPILE_POLICY, StringFixture, string_fixture};
use arrow::{
    array::{Array, ArrayRef, Int32Array, StringArray},
    datatypes::DataType,
};
use novarocks_constant_contract::ConstantPoolId;
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure, StaticExprKind,
    testkit::{
        ClaimAuthor, HarnessControl, PassThroughCallHost, add_call, constant_policy,
        register_constant,
    },
};
use novarocks_expression_contract::{ExprKind, ValueId};
use novarocks_functions::{
    ConstantValue, FunctionArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
    testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{CompilePhase, ControlShape, FunctionValueType, ValueLogicalType};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

fn slot_program(
    name: &str,
    types: &[FunctionValueType],
    constant_count: Option<i32>,
) -> StringFixture {
    let functions = installed_builtin_owner_catalogue();
    let inputs = types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            (
                ValueId::new(10 + u32::try_from(i).unwrap()),
                format!("local_1_{i}"),
                ty.clone(),
            )
        })
        .collect::<Vec<_>>();
    string_fixture(&functions, &inputs, |draft, constants, authors| {
        let mut args = Vec::new();
        let mut requests = Vec::new();
        for (id, _, ty) in &inputs {
            args.push(draft.add(None, ty.clone(), ExprKind::Value(*id)).unwrap());
            requests.push(FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            });
        }
        if let Some(count) = constant_count {
            let ty = FunctionValueType::new(DataType::Int32, true);
            let value = ConstantValue::from_i32(
                Arc::new(ty.try_to_field("count").unwrap()),
                ty.clone(),
                count,
                constant_policy(),
                CompilePhase::FunctionSpecialization,
                &HarnessControl,
            )
            .unwrap();
            // The two original producer columns were registered first.
            constants
                .insert(ConstantPoolId::new(2), value.pool().clone())
                .unwrap();
            let reference = register_constant(constants, &value);
            args.push(
                draft
                    .add(None, ty.clone(), ExprKind::Constant(reference))
                    .unwrap(),
            );
            requests.push(FunctionArgument::Value {
                value_type: ty,
                constant: Some(value),
            });
        }
        let author = ClaimAuthor::with_legacy_binding(
            &functions,
            name,
            requests,
            ControlShape::Eager,
            None,
            COMPILE_POLICY,
            &HarnessControl,
        );
        assert_eq!(
            author.result(),
            FunctionValueType::new(
                if name == "field" {
                    DataType::Int32
                } else {
                    DataType::Utf8
                },
                name != "field"
            ),
        );
        add_call(draft, authors, author, args)
    })
}

fn required_text_program(concat_text: bool) -> StringFixture {
    let functions = installed_builtin_owner_catalogue();
    let string = FunctionValueType::new(DataType::Utf8, true);
    let integer = FunctionValueType::new(DataType::Int32, true);
    let inputs = [
        (ValueId::new(0), "txt".into(), string.clone()),
        (ValueId::new(1), "delim".into(), string.clone()),
        (ValueId::new(2), "cnt".into(), integer),
    ];
    string_fixture(&functions, &inputs, |draft, _, authors| {
        let mut args = inputs
            .iter()
            .map(|(id, _, ty)| draft.add(None, ty.clone(), ExprKind::Value(*id)).unwrap())
            .collect::<Vec<_>>();
        let mut types = inputs
            .iter()
            .map(|(_, _, ty)| ty.clone())
            .collect::<Vec<_>>();
        let value = |value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        };
        if concat_text {
            let author = ClaimAuthor::with_legacy_binding(
                &functions,
                "concat",
                vec![value(string.clone()), value(string.clone())],
                ControlShape::Eager,
                None,
                COMPILE_POLICY,
                &HarnessControl,
            );
            types[0] = author.result();
            args[0] = add_call(draft, authors, author, vec![args[0], args[0]]);
        }
        let author = ClaimAuthor::with_legacy_binding(
            &functions,
            "substring_index",
            types.into_iter().map(value).collect(),
            ControlShape::Eager,
            None,
            COMPILE_POLICY,
            &HarnessControl,
        );
        add_call(draft, authors, author, args)
    })
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

// Both checked count literals must retain their source-pool values. The
// SQL-authored slot and literal versions remain in compile-to-execute.
#[test]
fn c12_p3_substring_index_actual_compiled_slots_constants_strict_null_and_explicit_empty_match_v1_expression()
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
        for (count, expected) in [
            (-1, vec![Some("a"), None, Some("\0"), Some("")]),
            (0, vec![None; 4]),
        ] {
            let program = slot_program(
                "substring_index",
                &[
                    FunctionValueType::new(DataType::Utf8, true),
                    FunctionValueType::new(DataType::Utf8, true),
                ],
                Some(count),
            );
            let root = program.unit.root(program.root).unwrap();
            let arena = program.unit.definitions();
            let StaticExprKind::BoundCall { args, .. } =
                arena.node(root.definition).unwrap().kind()
            else {
                panic!("actual substring root");
            };
            let StaticExprKind::Constant(value) = arena.node(args[2]).unwrap().kind() else {
                panic!("real count constant retains its pool origin");
            };
            assert_eq!(
                value.value_type(),
                &FunctionValueType::new(DataType::Int32, true)
            );
            assert_eq!(value.ordinal(), 0);
            let batch = program.batch(vec![text.clone(), delimiter.clone()]);
            for rows in [&[0, 1, 2, 3][..], &[0, 2, 3][..], &[][..]] {
                let selection = if rows.len() == 4 {
                    Selection::all(4)
                } else {
                    Selection::try_sparse(4, rows).unwrap()
                };
                let output = program
                    .instance()
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
    assert_eq!(observations, 12);
}

#[derive(Default)]
struct ChildControl {
    trace: Mutex<Vec<u32>>,
    stop: Option<usize>,
}
impl KernelEvaluationControl for ChildControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some(stop) = self.stop {
            assert!(at <= stop, "callback after original failure");
        }
        trace.push(units);
        if self.stop == Some(at) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure string child never waits")
    }
}
#[test]
fn c12_p3_substring_index_real_required_text_child_control_failure_survives_strict_null_and_latches()
 {
    let wide = "x".repeat(1025);
    let arrays = [
        Arc::new(StringArray::from(vec![Some(wide.as_str())])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    ];
    let mut child = None;
    for concat_text in [false, true] {
        let program = required_text_program(concat_text);
        let batch = program.batch(arrays.to_vec());
        let control = ChildControl::default();
        let mut frame = program.instance();
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert!(output.values().is_null(0));
        let trace = control.trace.lock().unwrap().clone();
        if !concat_text {
            assert!(!trace.contains(&256));
        } else {
            assert!(trace.contains(&256));
            child = Some((program, batch, trace));
        }
    }
    let (program, batch, trace) = child.unwrap();
    let at = trace.iter().position(|units| *units == 256).unwrap();
    let control = ChildControl {
        trace: Mutex::new(vec![]),
        stop: Some(at),
    };
    let mut frame = program.instance();
    assert_eq!(
        frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
    let replay = ChildControl::default();
    assert_eq!(
        frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &replay,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(replay.trace.lock().unwrap().is_empty());
}

// Existing nominal bytes go straight through the generic equality owner.
#[test]
fn pure_differential_field_actual_sql_null_coercion_and_generic_nominal_tags_expression() {
    for logical in [ValueLogicalType::Json, ValueLogicalType::Uuid] {
        let values: ArrayRef = if logical == ValueLogicalType::Json {
            Arc::new(StringArray::from(vec![
                Some("{}"),
                Some("raw-not-json"),
                None,
            ]))
        } else {
            novarocks_functions::largeint::array_from_i128(&[Some(i128::MIN), Some(7), None])
                .unwrap()
        };
        let ty =
            FunctionValueType::try_with_logical_type(values.data_type().clone(), true, logical)
                .unwrap();
        let program = slot_program("field", &[ty.clone(), ty.clone()], None);
        assert_eq!(program.schema.fields().len(), 2);
        let use_id = program.unit.root(program.root).unwrap().use_id;
        let call = &program.unit.calls()[&use_id];
        assert_eq!(
            call.call_contract()
                .selected()
                .argument_types
                .iter()
                .map(|argument| match argument {
                    FunctionArgumentType::Value(ty) => ty.clone(),
                    _ => panic!("FIELD runtime value type"),
                })
                .collect::<Vec<_>>(),
            vec![ty.clone(), ty]
        );
        let batch = program.batch(vec![values.clone(), values]);
        for rows in [&[0, 1, 2][..], &[1, 2][..], &[][..]] {
            let selection = Selection::try_sparse(3, rows).unwrap();
            let mut frame = program.instance();
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
            let expected = rows.iter().map(|row| [1, 1, 0][*row]).collect::<Vec<_>>();
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                expected.as_slice()
            );
        }
    }
}
