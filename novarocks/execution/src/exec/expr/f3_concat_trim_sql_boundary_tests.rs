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

//! Real SQL and checked LocalProgram boundaries retained independently of the
//! removed evaluator. Expected values below are literal SQL/Unicode examples.

use novarocks_expression::CompiledExpressionInstance;
use crate::exec::expr::{
    fixtures as host,
    test_support::{HarnessControl, constant_policy},
};
use arrow::{
    array::{Array, ArrayRef, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_expression_contract::ExprKind;
use novarocks_expression_contract::RootScope;
use novarocks_functions::{
    ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
    InstalledPureKernel, KernelEvaluationControl, KernelFailure, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, NodeId, ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const CONCAT_NAMES: [&str; 2] = ["concat", "concat_ws"];
const TRIM_NAMES: [&str; 3] = ["trim", "ltrim", "rtrim"];
const WHITE: &str = "\t\n\u{b}\u{c}\r \u{85}\u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";

#[test]
fn c12_p2_concat_real_sql_calls_keep_ordered_child_type_correspondence_for_compiled() {
    use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::FunctionArgumentType;
    use novarocks_physical_plan::StaticFunctionArgument;

    {
        for name in CONCAT_NAMES {
            let fields = ["sep", "a", "b", "c"].map(|n| Field::new(n, DataType::Utf8, true));
            let sql = if name == "concat" {
                "SELECT concat(a,b,c) FROM t"
            } else {
                "SELECT concat_ws(sep,a,b,c) FROM t"
            };
            let owner = sql_source_with_declared_fields(sql, &fields);
            let mut count = 0;
            for f in owner.plan().fragments().values() {
                for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(f) {
                    let ExprKind::FunctionCall { function, args } = &node.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != format!("builtin.scalar/{name}/v1") {
                        continue;
                    }
                    let request = unit.requests().get(id).unwrap();
                    let arity = if name == "concat" { 3 } else { 4 };
                    assert_eq!(args.len(), arity);
                    assert_eq!(request.logical_argument_count, arity);
                    // This provider fixture has identical opaque column payloads.
                    // Check ordered child/request types here; distinct column value
                    // order is pinned by the literal compiled-slot expectations below.
                    for (i, arg) in args.iter().enumerate() {
                        let StaticFunctionArgument::Value {
                            value_type,
                            constant,
                        } = &request.arguments[i]
                        else {
                            panic!("real value source")
                        };
                        assert!(constant.is_none());
                        let FunctionArgumentType::Value(selected) = &function.argument_types[i]
                        else {
                            panic!("selected value source")
                        };
                        assert_eq!(value_type, selected);
                        assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                        let child = unit.definition(*arg).unwrap();
                        assert_eq!(&child.ty, selected);
                        assert!(!matches!(child.kind, ExprKind::Constant(_)));
                    }
                    assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                    println!(
                        "C12-P2 retained compiled/{name} definition={id:?} children={args:?} request={request:?}"
                    );
                    count += 1;
                }
            }
            assert!(count > 0, "real retained call required");
        }
    }
}

#[test]
fn c12_p1_trim_real_sql_column_calls_keep_ordered_requests_for_compiled() {
    use crate::exec::expr::sql_test_source::sql_source_with_single_field;
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::FunctionArgumentType;
    use novarocks_physical_plan::StaticFunctionArgument;

    {
        for name in TRIM_NAMES {
            let owner = sql_source_with_single_field(
                &format!("SELECT {name}(k) FROM t"),
                Field::new("k", DataType::Utf8, true),
            );
            let mut count = 0;
            for f in owner.plan().fragments().values() {
                for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(f) {
                    let ExprKind::FunctionCall { function, args } = &node.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != format!("builtin.scalar/{name}/v1") {
                        continue;
                    }
                    let request = unit.requests().get(id).unwrap();
                    assert_eq!(args.len(), 1);
                    assert_eq!(request.logical_argument_count, 1);
                    let StaticFunctionArgument::Value {
                        value_type,
                        constant,
                    } = &request.arguments[0]
                    else {
                        panic!("real value source")
                    };
                    assert!(constant.is_none());
                    let FunctionArgumentType::Value(selected) = &function.argument_types[0] else {
                        panic!("selected value source")
                    };
                    assert_eq!(value_type, selected);
                    assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                    let child = unit.definition(args[0]).unwrap();
                    assert_eq!(&child.ty, selected);
                    assert!(
                        !matches!(child.kind, ExprKind::Constant(_)),
                        "column call must not become a constant"
                    );
                    assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                    println!(
                        "C12-P1 retained compiled/{name} definition={id:?} child={child:?} request={request:?}"
                    );
                    count += 1;
                }
            }
            assert!(count > 0, "real retained call required");
        }
    }
}

fn compiled_slot_program(
    name: &str,
    arrays: &[ArrayRef],
    constant: Option<(usize, Option<&str>)>,
    trim_child: bool,
) -> Arc<novarocks_local_program::LocalProgram> {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let names = if trim_child {
        vec![name, "trim"]
    } else {
        vec![name]
    };
    let mut catalog = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for name in names {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        catalog.register(definition.clone()).unwrap();
        installed.push(InstalledPureKernel {
            function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: definition.binding_declaration().unwrap().overloads()[0]
                    .identity
                    .clone(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/{name}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    let functions = catalog.seal_pure(installed).unwrap();
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
    let mut requests = vec![];
    for (i, array) in arrays.iter().enumerate() {
        let ty = FunctionValueType::new(array.data_type().clone(), true);
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
        args.push(if constant.is_some_and(|(index, _)| index == i) {
            None
        } else {
            Some(
                builder
                    .unit(output)
                    .unwrap()
                    .add(None, ty.clone(), ExprKind::Value(value))
                    .unwrap(),
            )
        });
        requests.push(FunctionArgument::Value {
            value_type: ty,
            constant: None,
        });
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            values.into_boxed_slice(),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    if let Some((index, text)) = constant {
        let ty = FunctionValueType::new(DataType::Utf8, true);
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
        // Preserve the actual source table before merging original request backings.
        args[index] = Some(
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
        requests[index] = FunctionArgument::Value {
            value_type: ty,
            constant: Some(value),
        };
    }
    let mut args = args
        .into_iter()
        .map(|argument| argument.expect("every argument has its actual slot or constant source"))
        .collect::<Vec<_>>();
    if trim_child {
        assert_eq!(name, "concat");
        let child_author = host::author(
            &functions,
            "trim",
            vec![requests[1].clone()],
            ControlShape::Eager,
        );
        let child_type = child_author.result();
        args[1] = host::call(&mut builder, &mut authors, child_author, vec![args[1]]);
        requests[1] = FunctionArgument::Value {
            value_type: child_type,
            constant: None,
        };
    }
    let author = host::author(&functions, name, requests, ControlShape::Eager);
    let result_ty = author.result();
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

fn compiled_input(program: &LocalProgram, arrays: &[ArrayRef]) -> (RecordBatch, ProgramRootRef) {
    let root_id = program.graph().root();
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|n| n.local_id() == Some(root_id))
        .unwrap();
    let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
        panic!("actual compiled Project root")
    };
    let input_node = program
        .graph()
        .nodes()
        .iter()
        .find(|n| n.local_id() == Some(*input))
        .unwrap();
    let batch =
        RecordBatch::try_new(input_node.output_layout().schema().clone(), arrays.to_vec()).unwrap();
    (batch, exprs[0])
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn assert_selections(
    program: Arc<LocalProgram>,
    arrays: &[ArrayRef],
    expected: &[Option<&str>],
) -> usize {
    let (batch, root) = compiled_input(&program, arrays);
    assert_eq!(batch.num_rows(), expected.len());
    let sparse = [0, 2, 3];
    for rows in [None, Some(&sparse[..]), Some(&[][..])] {
        let selection = rows.map_or_else(
            || Selection::all(expected.len()),
            |rows| Selection::try_sparse(expected.len(), rows).unwrap(),
        );
        let mut instance = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &HarnessControl,
        )
        .unwrap();
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
            // U+2003 EM SPACE and U+00A0 NO-BREAK SPACE are Unicode
            // White_Space; left/right retain the opposite boundary verbatim.
            let expected = if all_null {
                [None; 4]
            } else {
                match name {
                    "trim" => [Some("x"), None, Some(""), Some("")],
                    "ltrim" => [Some("x\u{a0}"), None, Some(""), Some("")],
                    "rtrim" => [Some(" \u{2003}x"), None, Some(""), Some("")],
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
    for (name, text) in [("trim", "x"), ("ltrim", "x\u{a0}"), ("rtrim", " \u{2003}x")] {
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
fn concat_required_trim_child_control_survives_strict_null_and_latches() {
    let wide = format!("{}x", "\u{2003}".repeat(1025));
    let arrays = [strings(&[None]), strings(&[Some(wide.as_str())])];
    let mut child_trace = None;
    for trim_child in [false, true] {
        let program = compiled_slot_program("concat", &arrays, None, trim_child);
        let (batch, root) = compiled_input(&program, &arrays);
        let good = ChildControl::default();
        let mut instance = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &HarnessControl,
        )
        .unwrap();
        let actual = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &good,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(actual.errors().is_empty());
        assert!(actual.values().is_null(0));
        let trace = good.trace.lock().unwrap().clone();
        if trim_child {
            assert!(
                trace.contains(&256),
                "required real trim child must scan whitespace"
            );
            child_trace = Some((program, batch, root, trace));
        } else {
            assert!(
                !trace.contains(&256),
                "strict NULL skips CONCAT row payload reads"
            );
        }
    }
    let (program, batch, root, trace) = child_trace.unwrap();
    let at = trace.iter().position(|units| *units == 256).unwrap();
    let control = ChildControl {
        trace: Mutex::new(vec![]),
        stop: Some(at),
    };
    let mut instance = CompiledExpressionInstance::try_new(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(
        instance
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
    let after = ChildControl::default();
    assert_eq!(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
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
    let mut instance = CompiledExpressionInstance::try_new(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &HarnessControl,
    )
    .unwrap();
    let selection = Selection::try_sparse(1, &[]).unwrap();
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
