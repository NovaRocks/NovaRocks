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
//! Full PARSE_URL profiles through the actual checked Physical -> LocalCompiler -> controller route.
use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::{ArrayRef, StringArray};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("parse_url", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/parse_url/(utf8,utf8)->utf8;strict;legacy",
                "builtin.scalar/parse_url/(utf8,utf8,utf8)->utf8;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/parse_url/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/parse_url/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}
fn program(arity: usize, constant_part: Option<&str>) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let fragment_id = FragmentId::new(196);
    let source = NodeId::new(0);
    let input = NodeId::new(1);
    let output = NodeId::new(2);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let values: Vec<_> = (0..arity).map(|i| ValueId::new(91 + i as u32)).collect();
    for value in &values {
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        let input_root = builder
            .unit(input)
            .unwrap()
            .root(
                expr,
                EvaluationDemand::Value,
                novarocks_expression_contract::RootScope::Port(0),
            )
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        projections.push((input_root, *value));
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            values.clone().into_boxed_slice(),
        )
        .unwrap();
    let mut args = vec![];
    let mut arguments = vec![];
    for (i, value) in values.iter().enumerate() {
        let cv = if i == 1 {
            constant_part.map(|text| {
                ConstantValue::from_utf8(
                    Arc::new(ty.try_to_field("actual checked part").unwrap()),
                    ty.clone(),
                    text,
                    options().constants,
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap()
            })
        } else {
            None
        };
        let expr = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                ty.clone(),
                if let Some(value) = &cv {
                    ExprKind::Constant(crate::exec::expr::test_support::register_constant(
                        &mut source_pools,
                        value,
                    ))
                } else {
                    ExprKind::Value(*value)
                },
            )
            .unwrap();
        args.push(expr);
        arguments.push(argument(ty.clone(), cv));
    }
    let owner = author(&functions, "parse_url", arguments, ControlShape::Eager);
    let call = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function().clone(),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    authors.insert(call, owner);
    let output_root = builder
        .unit(output)
        .unwrap()
        .root(
            call,
            EvaluationDemand::Value,
            novarocks_expression_contract::RootScope::Port(0),
        )
        .unwrap();
    let result_value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: output,
                root: output_root,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(output_root, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&ty),
            name: "url part".into(),
            alias: None,
            value: result_value,
            ty,
        }]),
    };
    compile_checked_fragment(&functions, builder, output, &authors, result, source_pools)
}
#[test]
fn parse_url_actual_compiler_both_profiles_dynamic_and_native_constants_keep_key_null_semantics() {
    for arity in [2, 3] {
        for constant_part in [None, Some("HOST"), Some("QUERY")] {
            let program = program(arity, constant_part);
            let text: ArrayRef = Arc::new(StringArray::from(vec![
                Some("guard"),
                Some("https://Example.com/a?q=a+b&q=second"),
                Some("https://x/b?q=c"),
                None,
                Some("bad"),
                Some("guard"),
            ]));
            let part: ArrayRef = Arc::new(StringArray::from(vec![
                Some("guard"),
                Some("QUERY"),
                Some("HOST"),
                Some("PATH"),
                Some("HOST"),
                Some("guard"),
            ]));
            let key: ArrayRef = Arc::new(StringArray::from(vec![
                Some("guard"),
                Some("q"),
                None,
                None,
                Some("q"),
                Some("guard"),
            ]));
            let arrays = [text.slice(1, 4), part.slice(1, 4), key.slice(1, 4)];
            let batch = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                arrays[..arity].to_vec(),
            )
            .unwrap();
            let rows = [0, 1, 2, 3];
            let selection = Selection::try_sparse(4, &rows).unwrap();
            let mut evaluator = instance(&program);
            let result = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            let expected = match constant_part {
                Some("HOST") => vec![Some("example.com"), Some("x"), None, None],
                Some("QUERY") if arity == 2 => {
                    vec![Some("q=a+b&q=second"), Some("q=c"), None, None]
                }
                Some("QUERY") => vec![Some("a b"), None, None, None],
                None if arity == 2 => vec![Some("q=a+b&q=second"), Some("x"), None, None],
                None => vec![Some("a b"), Some("x"), None, None],
                _ => unreachable!(),
            };
            assert_eq!(
                result
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(result.selection(), selection);
            assert!(result.errors().is_empty());
            let empty = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(4, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(empty.values().len(), 0);
            assert_eq!(empty.values().data_type(), &DataType::Utf8);
            assert!(empty.errors().is_empty());
        }
    }
}
#[test]
fn parse_url_actual_compiler_all_seven_runtime_causes_keep_first_cause_and_instance_latch() {
    let long = format!("https://x/{}", "a".repeat(700));
    for arity in [2, 3] {
        let program = program(arity, None);
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some(long.as_str()), None]));
        let part: ArrayRef = Arc::new(StringArray::from(vec![Some("PATH"), Some("HOST")]));
        let key: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 2]));
        let arrays = [text, part, key];
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            arrays[..arity].to_vec(),
        )
        .unwrap();
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &recorder,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = recorder.trace();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|units| *units <= 256));
        for index in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = instance(&program);
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(2), Selection::all(2)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                        ScalarInvocationActivation::Activated,
                        &after,
                        &mut PassThroughCallHost
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace().is_empty());
            }
        }
    }
}
