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
    array::{Array, ArrayRef, StringArray},
    datatypes::DataType,
};
use novarocks_constant_contract::ConstantPoolId;
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        ClaimAuthor, HarnessControl, PassThroughCallHost, add_call, constant_policy,
        register_constant,
    },
};
use novarocks_expression_contract::{ExprKind, ValueId};
use novarocks_functions::{
    ConstantValue, FunctionArgument, KernelEvaluationControl, KernelFailure,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{CompilePhase, ControlShape, FunctionValueType};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const CONCAT_NAMES: [&str; 2] = ["concat", "concat_ws"];
const TRIM_NAMES: [&str; 3] = ["trim", "ltrim", "rtrim"];
const WHITE: &str = "\t\n\u{b}\u{c}\r \u{1c}\u{1d}\u{1e}\u{1f}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{205f}\u{3000}";

fn compiled_slot_program(
    name: &str,
    arrays: &[ArrayRef],
    constant: Option<(usize, Option<&str>)>,
    trim_child: bool,
) -> StringFixture {
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let inputs = arrays
        .iter()
        .enumerate()
        .map(|(i, array)| {
            (
                ValueId::new(10 + u32::try_from(i).unwrap()),
                format!("local_1_{i}"),
                FunctionValueType::new(array.data_type().clone(), true),
            )
        })
        .collect::<Vec<_>>();
    string_fixture(&functions, &inputs, |draft, constants, authors| {
        let mut args = Vec::new();
        let mut requests = Vec::new();
        for (i, (input, _, ty)) in inputs.iter().enumerate() {
            let (kind, constant) =
                if let Some((index, text)) = constant.filter(|(index, _)| *index == i) {
                    let field = Arc::new(ty.try_to_field("constant").unwrap());
                    let value = match text {
                        Some(text) => ConstantValue::from_utf8(
                            field,
                            ty.clone(),
                            text,
                            constant_policy(),
                            CompilePhase::FunctionSpecialization,
                            &HarnessControl,
                        ),
                        None => ConstantValue::null(
                            field,
                            ty.clone(),
                            constant_policy(),
                            CompilePhase::FunctionSpecialization,
                            &HarnessControl,
                        ),
                    }
                    .unwrap();
                    assert_eq!(index, i);
                    // The original producer registered one pool per incoming column.
                    let pool = ConstantPoolId::new(u32::try_from(inputs.len()).unwrap());
                    constants.insert(pool, value.pool().clone()).unwrap();
                    (
                        ExprKind::Constant(register_constant(constants, &value)),
                        Some(value),
                    )
                } else {
                    (ExprKind::input(*input), None)
                };
            args.push(draft.add(None, ty.clone(), kind).unwrap());
            requests.push(FunctionArgument::Value {
                value_type: ty.clone(),
                constant,
            });
        }
        if trim_child {
            assert_eq!(name, "concat");
            let child = ClaimAuthor::with_legacy_binding(
                &functions,
                "trim",
                vec![requests[1].clone()],
                ControlShape::Eager,
                None,
                COMPILE_POLICY,
                &HarnessControl,
            );
            requests[1] = FunctionArgument::Value {
                value_type: child.result(),
                constant: None,
            };
            args[1] = add_call(draft, authors, child, vec![args[1]]);
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
        add_call(draft, authors, author, args)
    })
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn assert_selections(
    program: StringFixture,
    arrays: &[ArrayRef],
    expected: &[Option<&str>],
) -> usize {
    let batch = program.batch(arrays.to_vec());
    assert_eq!(batch.num_rows(), expected.len());
    let sparse = [0, 2, 3];
    for rows in [None, Some(&sparse[..]), Some(&[][..])] {
        let selection = rows.map_or_else(
            || Selection::all(expected.len()),
            |rows| Selection::try_sparse(expected.len(), rows).unwrap(),
        );
        let mut instance = program.instance();
        let actual = instance
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
        assert_eq!(actual.selection(), selection);
        assert_eq!(actual.values().data_type(), &DataType::Utf8);
        assert!(actual.errors().is_empty());
        let values = actual
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.len(), selection.len());
        for (ordinal, row) in selection.iter().enumerate() {
            assert_eq!(values.is_null(ordinal), expected[row].is_none());
            if let Some(text) = expected[row] {
                assert_eq!(values.value(ordinal), text);
            }
        }
    }
    3
}

// Historical required names stay stable for the retention inventory. The
// values now come from the literal examples, with no legacy evaluator call.
#[test]
fn c12_p2_concat_actual_compiled_slots_and_sparse_null_match_v1() {
    let mut observations = 0;
    for name in CONCAT_NAMES {
        for all_null in [false, true] {
            let a = if all_null {
                strings(&[None; 4])
            } else {
                strings(&[Some("é\0"), None, Some(""), Some("x")])
            };
            let b = strings(&[Some("尾"), Some(""), Some("b"), None]);
            let (arrays, expected): (Vec<ArrayRef>, [Option<&str>; 4]) = match (name, all_null) {
                ("concat", false) => (vec![a, b], [Some("é\0尾"), None, Some("b"), None]),
                ("concat", true) => (vec![a, b], [None; 4]),
                ("concat_ws", false) => (
                    vec![strings(&[Some("|"), Some("-"), Some(""), Some("🧪")]), a, b],
                    [Some("é\0|尾"), Some(""), Some("b"), Some("x")],
                ),
                ("concat_ws", true) => (
                    vec![strings(&[Some("|"), Some("-"), Some(""), Some("🧪")]), a, b],
                    [Some("尾"), Some(""), Some("b"), Some("")],
                ),
                _ => unreachable!(),
            };
            // CONCAT is strict; CONCAT_WS skips NULL values but keeps empty
            // strings as values, inserting the separator between survivors.
            observations += assert_selections(
                compiled_slot_program(name, &arrays, None, false),
                &arrays,
                &expected,
            );
        }
    }
    assert_eq!(observations, 12);
}

#[test]
fn c12_p1_trim_actual_compiled_slot_and_sparse_strict_null_match_v1() {
    let mut observations = 0;
    for name in TRIM_NAMES {
        for all_null in [false, true] {
            let arrays = [if all_null {
                strings(&[None; 4])
            } else {
                strings(&[Some(" \u{2003}x\u{a0}"), None, Some(""), Some(WHITE)])
            }];
            // Trino trims U+2003 EM SPACE and preserves U+00A0 NO-BREAK SPACE.
            let expected = if all_null {
                [None; 4]
            } else {
                match name {
                    "trim" => [Some("x\u{a0}"), None, Some(""), Some("")],
                    "ltrim" => [Some("x\u{a0}"), None, Some(""), Some("")],
                    "rtrim" => [Some(" \u{2003}x\u{a0}"), None, Some(""), Some("")],
                    _ => unreachable!(),
                }
            };
            observations += assert_selections(
                compiled_slot_program(name, &arrays, None, false),
                &arrays,
                &expected,
            );
        }
    }
    assert_eq!(observations, 18);
}

#[test]
fn concat_trim_checked_constants_keep_pool_values_and_selection() {
    let a = strings(&[Some("é\0"), None, Some(""), Some("x")]);
    let b = strings(&[Some("尾"), Some(""), Some("b"), None]);
    let arrays = [a.clone(), b.clone()];
    assert_selections(
        compiled_slot_program("concat", &arrays, Some((1, Some("尾"))), false),
        &arrays,
        &[Some("é\0尾"), None, Some("尾"), Some("x尾")],
    );
    assert_selections(
        compiled_slot_program("concat", &arrays, Some((0, None)), false),
        &arrays,
        &[None; 4],
    );
    // Deliberately different slot separators prove that checked constant pool
    // addressing is used, including an empty value preceding a nonempty one.
    let arrays = [strings(&[Some("wrong"); 4]), a, b];
    assert_selections(
        compiled_slot_program("concat_ws", &arrays, Some((0, Some("|"))), false),
        &arrays,
        &[Some("é\0|尾"), Some(""), Some("|b"), Some("x")],
    );
    assert_selections(
        compiled_slot_program("concat_ws", &arrays, Some((0, None)), false),
        &arrays,
        &[None; 4],
    );
    for (name, text) in [
        ("trim", "x\u{a0}"),
        ("ltrim", "x\u{a0}"),
        ("rtrim", " \u{2003}x\u{a0}"),
    ] {
        let arrays = [strings(&[Some("wrong"), None, Some(""), Some(WHITE)])];
        assert_selections(
            compiled_slot_program(name, &arrays, Some((0, Some(" \u{2003}x\u{a0}"))), false),
            &arrays,
            &[Some(text); 4],
        );
        assert_selections(
            compiled_slot_program(name, &arrays, Some((0, None)), false),
            &arrays,
            &[None; 4],
        );
    }
}

#[derive(Default)]
struct ChildControl {
    trace: Mutex<Vec<u32>>,
    stop: Option<usize>,
}
impl KernelEvaluationControl for ChildControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some(stop) = self.stop {
            assert!(at <= stop, "callback after original cause");
        }
        trace.push(units);
        if self.stop == Some(at) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure child never waits")
    }
}

#[test]
fn concat_only_demands_trim_after_non_null_prefix_and_latches_interruption() {
    let wide = format!("{}x", "\u{2003}".repeat(1025));
    let arrays = [
        strings(&[None, Some("prefix")]),
        strings(&[Some(wide.as_str()), Some(wide.as_str())]),
    ];
    let program = compiled_slot_program("concat", &arrays, None, true);
    let batch = program.batch(arrays.to_vec());
    let mut demanded_trace = None;
    for row in [0, 1] {
        let positions = [row];
        let selection = Selection::try_sparse(2, &positions).unwrap();
        let good = ChildControl::default();
        let mut instance = program.instance();
        let actual = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &good,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(actual.errors().is_empty());
        assert_eq!(actual.values().is_null(0), row == 0);
        let trace = good.trace.lock().unwrap().clone();
        if row == 1 {
            let values = actual
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), "prefixx");
            assert!(
                trace.contains(&256),
                "the demanded trim child must scan whitespace"
            );
            demanded_trace = Some(trace);
        } else {
            assert!(
                !trace.contains(&256),
                "the strict NULL prefix skips the long trim child"
            );
        }
    }
    let trace = demanded_trace.unwrap();
    let demanded = Selection::try_sparse(2, &[1]).unwrap();
    let at = trace.iter().position(|units| *units == 256).unwrap();
    let control = ChildControl {
        trace: Mutex::new(vec![]),
        stop: Some(at),
    };
    let mut instance = program.instance();
    assert_eq!(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None
                },
                ScalarInvocationScope::new(demanded, demanded),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
    let after = ChildControl::default();
    assert_eq!(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None
                },
                ScalarInvocationScope::new(demanded, demanded),
                ScalarInvocationActivation::Activated,
                &after,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(after.trace.lock().unwrap().is_empty());

    // Empty positive-carrier demand must not scan the long child.
    let empty = ChildControl::default();
    let mut instance = program.instance();
    let selection = Selection::try_sparse(2, &[]).unwrap();
    let actual = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &empty,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(actual.selection(), selection);
    assert_eq!(actual.values().len(), 0);
    assert!(actual.errors().is_empty());
    assert!(!empty.trace.lock().unwrap().contains(&256));
}
