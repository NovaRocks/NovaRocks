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
//! Actual local compiler uses the same native-v1 source author, with no CV inference.
use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::{Array, ArrayRef, StringArray};
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::RegexpCountPatternSource as Source;
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("regexp_count", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/regexp_count/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.scalar/regexp_count/(utf8,utf8)->i64;strict;legacy",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.scalar/regexp_count/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap()
}
fn program(constant: Option<&str>) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let fragment_id = FragmentId::new(194);
    let source = NodeId::new(0);
    let input = NodeId::new(1);
    let output = NodeId::new(2);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let values = [ValueId::new(91), ValueId::new(7)];
    for value in values {
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
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        projections.push((input_root, value));
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            Box::from(values),
        )
        .unwrap();
    let text = builder
        .unit(output)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(values[0]))
        .unwrap();
    let cv = constant.map(|text| {
        ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("actual checked constant").unwrap()),
            ty.clone(),
            text,
            options().constants,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    });
    let pattern = builder
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
                ExprKind::Value(values[1])
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let owner = author(
        &functions,
        "regexp_count",
        vec![argument(ty.clone(), None), argument(ty, cv)],
        ControlShape::Eager,
    );
    let count = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function().clone(),
                args: Box::from([text, pattern]),
            },
        )
        .unwrap();
    authors.insert(count, owner);
    let result_ty = FunctionValueType::new(DataType::Int64, true);
    let output_root = builder
        .unit(output)
        .unwrap()
        .root(
            count,
            EvaluationDemand::Value,
            novarocks_expression_contract::RootScope::Port(0),
        )
        .unwrap();
    let result_value = builder
        .add_value(
            result_ty.clone(),
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
            domain: crate::test_result_domain::result_value_domain(&result_ty),
            name: "count".into(),
            alias: None,
            value: result_value,
            ty: result_ty,
        }]),
    };
    compile_checked_fragment(&functions, builder, output, &authors, result, source_pools)
}
#[test]
fn regexp_count_actual_compiler_emitted_constant_and_dynamic_bytes_keep_original_error_policy() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("guard"),
        None,
        Some("éé"),
        Some("ababa"),
        Some("guard"),
    ]));
    let patterns: ArrayRef = Arc::new(StringArray::from(vec![
        Some("guard"),
        Some("["),
        Some("."),
        Some("aba"),
        Some("guard"),
    ]));
    let text = text.slice(1, 3);
    let patterns = patterns.slice(1, 3);
    for constant in [None, Some("["), Some("."), Some("a{,}"), Some("")] {
        let p = program(constant);
        let batch = RecordBatch::try_new(
            p.graph().nodes()[1].output_layout().schema().clone(),
            vec![text.clone(), patterns.clone()],
        )
        .unwrap();
        let selection = Selection::try_sparse(3, &[0, 1, 2]).unwrap();
        let mut instance = instance(&p);
        let out = instance
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
        let expected = match constant {
            None => vec![None, Some(2), Some(1)],
            Some("[") => vec![None, None, None],
            Some(".") => vec![None, Some(2), Some(5)],
            Some("a{,}") => vec![None, Some(0), Some(0)],
            Some("") => vec![None, Some(3), Some(6)],
            _ => unreachable!(),
        };
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        if constant == Some("[") {
            assert_eq!(out.errors().len(), 2);
            assert!(out.errors().iter().all(|e| {
                e.message()
                    .starts_with("Invalid regex expression: [. Detail message:")
            }));
        } else {
            assert!(out.errors().is_empty());
        }
        assert!(
            instance
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(3, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost
                )
                .unwrap()
                .errors()
                .is_empty()
        );
    }
}
#[test]
fn regexp_count_shared_native_projection_distinguishes_constant_from_value_without_cv() {
    let constant = ExprKind::Constant(novarocks_physical_plan::ConstantReference {
        pool: novarocks_physical_plan::ConstantPoolId::new(71),
        ordinal: 2,
    });
    assert_eq!(
        novarocks_expression_contract::native_v1_emitted_constant_reference(&constant),
        Some(novarocks_physical_plan::ConstantReference {
            pool: novarocks_physical_plan::ConstantPoolId::new(71),
            ordinal: 2
        })
    );

    assert_eq!(
        novarocks_expression_contract::native_v1_emitted_constant_reference(&ExprKind::Value(
            ValueId::new(91)
        )),
        None
    );
    assert_ne!(Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent);
}
