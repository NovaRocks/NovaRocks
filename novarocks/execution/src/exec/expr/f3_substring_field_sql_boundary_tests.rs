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

//! Retained SQL binding and compiled Frame properties with independent values.
//! Historical required test names are stable retention-map keys, not v1 oracles.
use novarocks_expression::CompiledExpressionInstance;
use super::{
    filter_conjunction_actual_sql_compiler_tests::compiler_results,
    numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue,
    sql_test_source::sql_source_with_declared_fields, test_support::HarnessControl,
};
use arrow::{
    array::{Array, ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure, StaticExprKind};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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

#[test]
fn c12_p3_substring_index_actual_sql_keeps_ordered_three_child_types_for_compiled() {
    let fields = substring_fields();
    let source =
        sql_source_with_declared_fields("SELECT substring_index(txt,delim,cnt) FROM t", &fields);
    let mut found = 0;
    for fragment in source.plan().fragments().values() {
        for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/substring_index/v1" {
                continue;
            }
            let request = unit.requests().get(id).unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(request.logical_argument_count, 3);
            assert_eq!(request.arguments.len(), 3);
            assert_eq!(function.argument_types.len(), 3);
            for (i, arg) in args.iter().enumerate() {
                let StaticFunctionArgument::Value {
                    value_type,
                    constant,
                } = &request.arguments[i]
                else {
                    panic!("actual value channel")
                };
                let FunctionArgumentType::Value(selected) = &function.argument_types[i] else {
                    panic!("actual selected channel")
                };
                assert!(constant.is_none());
                assert_eq!(value_type, selected);
                assert_eq!(
                    selected,
                    &FunctionValueType::new(fields[i].data_type().clone(), true)
                );
                let child = unit.definition(*arg).unwrap();
                assert_eq!(&child.ty, selected);
                assert!(!matches!(child.kind, ExprKind::Constant(_)));
            }
            assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
            found += 1;
        }
    }
    assert!(found > 0);
}

// The original SQL packages, provider declarations and LocalCompiler are used
// intact. Frame inputs are assigned by actual ordered argument SlotIds, never
// by assumptions about a scan/project's reordered physical column positions.
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
        for (site, unit, root) in super::sql_test_source::compiled_roots(&program) {
            let Some(call) = unit.calls().get(&root.use_id) else {
                continue;
            };
            if call.call_contract().function_id().as_str() != format!("builtin.scalar/{name}/v1") {
                continue;
            }
            let arena = unit.definitions();
            let flow = unit.control_flow();
            let invocation = &flow.uses()[&root.use_id];
            let StaticExprKind::BoundCall { args, .. } =
                arena.node(root.definition).unwrap().kind()
            else {
                panic!("original bound root")
            };
            assert_eq!(args.len(), inputs.len());
            let mut input_slots = BTreeMap::new();
            for (argument, values) in invocation.arguments.iter().zip(inputs) {
                let Some(values) = values else {
                    continue;
                };
                // Follow the real CONCAT child use to the actual compiled Input binding.
                let mut occurrence = *argument;
                let slot = loop {
                    let source = &flow.uses()[&occurrence];
                    match arena.node(source.definition).unwrap().kind() {
                        StaticExprKind::Input => {
                            break super::sql_test_source::project_input_slot_for_use(
                                &program, site, occurrence,
                            );
                        }
                        StaticExprKind::BoundCall { .. } => occurrence = source.arguments[0],
                        other => panic!("actual fixture Input or CONCAT child: {other:?}"),
                    }
                };
                assert!(input_slots.insert(slot, values.clone()).is_none());
            }
            let input = program
                .graph()
                .nodes()
                .iter()
                .find_map(|node| match node.kind() {
                    ProgramNodeKind::Project { input, exprs, .. } if exprs.contains(&site) => {
                        Some(*input)
                    }
                    _ => None,
                })
                .expect("actual producer project");
            let layout = program.graph().nodes()[input.index()].output_layout();
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
fn c12_p3_substring_index_actual_compiled_slots_constants_strict_null_and_explicit_empty_match_v1()
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
            let mut programs = vec![(program, site, batch)];
            if count_sql != "cnt" {
                let program = slot_program(
                    "substring_index",
                    &[
                        FunctionValueType::new(DataType::Utf8, true),
                        FunctionValueType::new(DataType::Utf8, true),
                    ],
                    Some(if count_sql == "-1" { -1 } else { 0 }),
                );
                let root = program.graph().root();
                let ProgramNodeKind::Project { exprs, .. } =
                    program.graph().nodes()[root.index()].kind()
                else {
                    panic!("actual project roots")
                };
                let site = exprs[0];
                let ProgramNodeKind::Project { input, .. } =
                    program.graph().nodes()[root.index()].kind()
                else {
                    panic!("actual constant-count project")
                };
                let unit = program.unit(site.unit).unwrap();
                let use_id = unit.root(site.root).unwrap().use_id;
                let arena = unit.definitions();
                let flow = unit.control_flow();
                let StaticExprKind::BoundCall { args, .. } =
                    arena.node(flow.uses()[&use_id].definition).unwrap().kind()
                else {
                    panic!("actual substring root")
                };
                let StaticExprKind::Constant(value) = arena.node(args[2]).unwrap().kind() else {
                    panic!("real count constant retains its pool origin")
                };
                assert_eq!(
                    value.value_type(),
                    &FunctionValueType::new(DataType::Int32, true)
                );
                assert_eq!(value.ordinal(), 0);

                let batch = RecordBatch::try_new(
                    program.graph().nodes()[input.index()]
                        .output_layout()
                        .schema()
                        .clone(),
                    vec![text.clone(), delimiter.clone()],
                )
                .unwrap();
                programs.push((program, site, batch));
            }
            for (program, site, batch) in programs {
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
    }
    assert_eq!(observations, 30);
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
        Some(Arc::new(StringArray::from(vec![Some(wide.as_str())])) as ArrayRef),
        Some(Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef),
        Some(Arc::new(Int32Array::from(vec![1])) as ArrayRef),
    ];
    let mut child = None;
    for text in ["txt", "concat(txt,txt)"] {
        let (program, site, batch) = sql_frame(
            &format!("SELECT substring_index({text},delim,cnt) FROM t"),
            &substring_fields(),
            "substring_index",
            &arrays,
        );
        let control = ChildControl::default();
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
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert!(output.values().is_null(0));
        let trace = control.trace.lock().unwrap().clone();
        if text == "txt" {
            assert!(!trace.contains(&256));
        } else {
            assert!(trace.contains(&256));
            child = Some((program, site, batch, trace));
        }
    }
    let (program, site, batch, trace) = child.unwrap();
    let at = trace.iter().position(|units| *units == 256).unwrap();
    let control = ChildControl {
        trace: Mutex::new(vec![]),
        stop: Some(at),
    };
    let mut frame = CompiledExpressionInstance::try_new(
        program.unit(site.unit).unwrap().clone(),
        site.root,
        &HarnessControl,
    )
    .unwrap();
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

// Nominal JSON/UUID carrier facts cannot be authored by converting ordinary
// strings/integers in SQL: that would test their decoders instead of generic
// FIELD equality. Keep the real catalog author and checked compiler here.
fn slot_program(
    name: &str,
    types: &[FunctionValueType],
    constant_count: Option<i32>,
) -> Arc<LocalProgram> {
    use super::fixtures as host;
    use novarocks_expression_contract::RootScope;
    use novarocks_physical_plan::{
        FragmentBuilder, FragmentId, NodeId, ResultField, ResultPort, ValueDef, ValueId,
        ValueOrigin,
    };
    use novarocks_type_contract::{ControlShape, EvaluationDemand, SemanticParameters};
    let functions = installed_builtin_owner_catalogue();
    let fragment_id = FragmentId::new(196);
    let output = NodeId::new(0);
    let input = NodeId::new(1);
    let source = NodeId::new(2);
    let mut builder = FragmentBuilder::new(fragment_id);
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let mut values = vec![];
    let mut args = vec![];
    for (i, ty) in types.iter().enumerate() {
        let value = ValueId::new(i as u32 + 10);
        let null = novarocks_functions::ConstantValue::null(
            Arc::new(ty.try_to_field("input").unwrap()),
            ty.clone(),
            super::test_support::constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            &HarnessControl,
        )
        .unwrap();
        let reference = super::test_support::register_constant(&mut source_pools, &null);
        let literal = builder
            .unit(input)
            .unwrap()
            .add(None, ty.clone(), ExprKind::Constant(reference))
            .unwrap();
        let literal = builder
            .unit(input)
            .unwrap()
            .root(literal, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: literal,
                },
            })
            .unwrap();
        projections.push((literal, value));
        values.push(value);
        args.push(
            builder
                .unit(output)
                .unwrap()
                .add(None, ty.clone(), ExprKind::Value(value))
                .unwrap(),
        );
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            values.into_boxed_slice(),
        )
        .unwrap();
    let mut requests = types
        .iter()
        .map(|ty| FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    if let Some(count) = constant_count {
        let ty = FunctionValueType::new(DataType::Int32, true);
        let value = novarocks_functions::ConstantValue::from_i32(
            Arc::new(ty.try_to_field("count").unwrap()),
            ty.clone(),
            count,
            super::test_support::constant_policy(),
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap();
        args.push(
            builder
                .unit(output)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    ExprKind::Constant(super::test_support::register_constant(
                        &mut source_pools,
                        &value,
                    )),
                )
                .unwrap(),
        );
        requests.push(FunctionArgument::Value {
            value_type: ty,
            constant: Some(value),
        });
    }
    let author = host::author(&functions, name, requests, ControlShape::Eager);
    let result_ty = author.result();
    assert_eq!(
        result_ty,
        FunctionValueType::new(
            if name == "field" {
                DataType::Int32
            } else {
                DataType::Utf8
            },
            name != "field",
        )
    );
    let mut authors = BTreeMap::new();
    let call = host::call(&mut builder, &mut authors, author, args);
    let call = builder
        .unit(output)
        .unwrap()
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            result_ty.clone(),
            ValueOrigin::Expr {
                node: output,
                root: call,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(call, value)]),
            Box::from([value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_ty),
            name: name.into(),
            alias: None,
            value,
            ty: result_ty,
        }]),
    };
    host::compile_checked_fragment_with_parameters(
        &functions,
        builder,
        output,
        &authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
        source_pools,
    )
}

#[test]
fn pure_differential_field_actual_sql_null_coercion_and_generic_nominal_tags() {
    let fields = [
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let sql = "SELECT field(NULL,a,b) FROM t";
    let source = sql_source_with_declared_fields(sql, &fields);
    let mut found = 0;
    for fragment in source.plan().fragments().values() {
        for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/field/v1" {
                continue;
            }
            let request = unit.requests().get(id).unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(request.logical_argument_count, 3);
            assert_eq!(request.arguments.len(), 3);
            assert_eq!(function.argument_types.len(), 3);
            for (ordinal, arg) in args.iter().enumerate() {
                let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[ordinal]
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
    for logical in [ValueLogicalType::Json, ValueLogicalType::Uuid] {
        let values: ArrayRef = if logical == ValueLogicalType::Json {
            Arc::new(StringArray::from(vec![
                Some("{}"),
                Some("raw-not-json"),
                None,
            ]))
        } else {
            novarocks_types::largeint::array_from_i128(&[Some(i128::MIN), Some(7), None]).unwrap()
        };
        let ty =
            FunctionValueType::try_with_logical_type(values.data_type().clone(), true, logical)
                .unwrap();
        let program = slot_program("field", &[ty.clone(), ty.clone()], None);
        let root = program.graph().root();
        let ProgramNodeKind::Project { exprs, .. } = program.graph().nodes()[root.index()].kind()
        else {
            panic!("actual project roots")
        };
        let site = exprs[0];
        let ProgramNodeKind::Project { input, .. } = program.graph().nodes()[root.index()].kind()
        else {
            panic!("actual FIELD project")
        };
        let layout = program.graph().nodes()[input.index()].output_layout();
        assert_eq!(layout.schema().fields().len(), 2);
        let unit = program.unit(site.unit).unwrap();
        let use_id = unit.root(site.root).unwrap().use_id;
        let call = &unit.calls()[&use_id];
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
        let batch =
            RecordBatch::try_new(layout.schema().clone(), vec![values.clone(), values]).unwrap();
        for rows in [&[0, 1, 2][..], &[1, 2][..], &[][..]] {
            let selection = Selection::try_sparse(3, rows).unwrap();
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
