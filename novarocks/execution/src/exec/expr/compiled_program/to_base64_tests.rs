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
//! Actual Physical source author -> frozen contract -> LocalCompiler -> controller witnesses.
//! This executes that checked layer, not the separate native protobuf emitter/BE decoder.
use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::{ArrayRef, StringArray};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::{
    SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
    ToBase64ByteSource as Source,
};
#[derive(Clone, Copy)]
enum Shape {
    Slot,
    Constant,
    FromBase64,
    CastFromBase64,
    ConcatFromBase64,
}
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for name in ["to_base64", "from_base64", "concat"] {
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: None,
        }];
        let selected = actual
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap();
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        installed.push(InstalledPureKernel {
            function: selected.function_id,
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: selected.selected.overload,
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/{name}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    builder.seal_pure(installed).unwrap()
}
fn program(shape: Shape) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let fragment_id = FragmentId::new(197);
    let source = NodeId::new(0);
    let input = NodeId::new(1);
    let output = NodeId::new(2);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let input_expr = builder
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
        .root(input_expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = ValueId::new(91);
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
    builder
        .add_project(
            input,
            source,
            Box::from([(input_root, value)]),
            Box::from([value]),
        )
        .unwrap();
    let cv = matches!(shape, Shape::Constant).then(|| {
        ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("actual checked source").unwrap()),
            ty.clone(),
            "ÿ",
            options().constants,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    });
    let leaf = builder
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
                ExprKind::Value(value)
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let mut child = leaf;
    if matches!(
        shape,
        Shape::FromBase64 | Shape::CastFromBase64 | Shape::ConcatFromBase64
    ) {
        let owner = author(
            &functions,
            "from_base64",
            vec![argument(ty.clone(), None)],
            ControlShape::Eager,
        );
        child = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                owner.result(),
                ExprKind::FunctionCall {
                    function: owner.function().clone(),
                    args: Box::from([child]),
                },
            )
            .unwrap();
        authors.insert(child, owner);
    }
    if matches!(shape, Shape::CastFromBase64) {
        child = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                ty.clone(),
                ExprKind::Cast {
                    expr: child,
                    target: DataType::Utf8,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: SemanticParameterRef {
                        id: SemanticParameterId::new(0),
                        expected_key: SemanticParameterKey::AllowThrowException,
                    },
                },
            )
            .unwrap();
    }
    if matches!(shape, Shape::ConcatFromBase64) {
        let owner = author(
            &functions,
            "concat",
            vec![argument(ty.clone(), None)],
            ControlShape::Eager,
        );
        child = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                owner.result(),
                ExprKind::FunctionCall {
                    function: owner.function().clone(),
                    args: Box::from([child]),
                },
            )
            .unwrap();
        authors.insert(child, owner);
    }
    let owner = author(
        &functions,
        "to_base64",
        vec![argument(ty.clone(), cv)],
        ControlShape::Eager,
    );
    let call = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function().clone(),
                args: Box::from([child]),
            },
        )
        .unwrap();
    authors.insert(call, owner);
    let output_root = builder
        .unit(output)
        .unwrap()
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
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
            name: "base64".into(),
            alias: None,
            value: result_value,
            ty,
        }]),
    };
    let parameters = if matches!(shape, Shape::CastFromBase64) {
        SemanticParameters::try_new([(
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap()
    } else {
        SemanticParameters::default()
    };
    compile_checked_fragment_with_parameters(
        &functions,
        builder,
        output,
        &authors,
        result,
        parameters,
        source_pools,
    )
}
// RFC 4648 section 4 uses the standard alphabet and '=' padding. These
// values are independent byte derivations, not output from either evaluator:
// UTF-8 U+00FF = c3 bf -> w78=; U+0100 = c4 80 -> xIA=;
// ff -> /w==; ff 00 -> /wA=; UTF-8 U+00FF + NUL = c3 bf 00 -> w78A.
// The sealed NovaRocks TO_BASE64 contract maps empty bytes and SQL NULL to
// successful NULL. That NULL rule is separate from RFC 4648's empty encoding.
fn independent_expected(shape: Shape) -> Vec<Option<String>> {
    let rows = match shape {
        Shape::Slot => [Some("w78="), Some("xIA="), None, None],
        Shape::Constant => [Some("w78="); 4],
        Shape::FromBase64 => [Some("/w=="), Some("/wA="), None, None],
        Shape::CastFromBase64 | Shape::ConcatFromBase64 => [Some("w78="), Some("w78A"), None, None],
    };
    rows.into_iter().map(|row| row.map(str::to_owned)).collect()
}

#[test]
fn to_base64_actual_compiler_source_sensitive_full_profile_matches_rfc4648_vectors() {
    for shape in [
        Shape::Slot,
        Shape::Constant,
        Shape::FromBase64,
        Shape::CastFromBase64,
        Shape::ConcatFromBase64,
    ] {
        let input: ArrayRef = Arc::new(StringArray::from(if matches!(shape, Shape::Slot) {
            vec![
                Some("guard"),
                Some("ÿ"),
                Some("Ā"),
                Some(""),
                None,
                Some("guard"),
            ]
        } else if matches!(shape, Shape::Constant) {
            vec![
                Some("guard"),
                Some("ÿ"),
                Some("ÿ"),
                Some("ÿ"),
                Some("ÿ"),
                Some("guard"),
            ]
        } else {
            vec![
                Some("guard"),
                Some("/w=="),
                Some("/wA="),
                Some(""),
                None,
                Some("guard"),
            ]
        }));
        let input = input.slice(1, 4);
        let program = program(shape);
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            vec![input.clone()],
        )
        .unwrap();
        let mut evaluator = instance(&program);
        let selection = Selection::try_sparse(4, &[0, 1, 2, 3]).unwrap();
        let out = evaluator
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
        let expected = independent_expected(shape);
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_owned))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(out.errors().is_empty());
        let expected_source = if matches!(shape, Shape::FromBase64) {
            Source::NativeV1EncryptionLatin1
        } else {
            Source::Ordinary
        };
        let records = root_unit(&program).calls();
        let own: Vec<_> = records
            .values()
            .filter(|record| {
                record.call_contract().function_id().as_str() == "builtin.scalar/to_base64/v1"
            })
            .collect();
        assert_eq!(own.len(), 1);
        assert_eq!(
            own[0].call_contract().to_base64_byte_source(),
            Some(expected_source)
        );
        let sparse = Selection::try_sparse(4, &[0, 3]).unwrap();
        let out = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(sparse, sparse),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(out.selection(), sparse);
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_owned))
                .collect::<Vec<_>>(),
            vec![expected[0].clone(), expected[3].clone()]
        );
        assert!(
            instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(4, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost
                )
                .unwrap()
                .values()
                .is_empty()
        );
    }
}
#[test]
fn to_base64_actual_native_identity_projection_matches_independent_source_table() {
    // The frozen native-v1 source contract canonicalizes wire names and
    // aliases case-insensitively before testing three exact encryption tags.
    // Expected facts are pinned independently; do not call a registry lookup
    // (or the classifier being tested) to calculate their own expected value.
    for (name, expected) in [
        ("aes_encrypt", Source::NativeV1EncryptionLatin1),
        ("AES_ENCRYPT", Source::NativeV1EncryptionLatin1),
        ("from_base64", Source::NativeV1EncryptionLatin1),
        ("FROM_BASE64", Source::NativeV1EncryptionLatin1),
        ("base64_decode_binary", Source::NativeV1EncryptionLatin1),
        ("BASE64_DECODE_STRING", Source::NativeV1EncryptionLatin1),
        ("to_binary", Source::NativeV1EncryptionLatin1),
        ("TO_BINARY", Source::NativeV1EncryptionLatin1),
        ("concat", Source::Ordinary),
        ("md5", Source::Ordinary),
        ("upper", Source::Ordinary),
    ] {
        let id = FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap();
        let emitted = novarocks_expression_contract::native_v1_function_name(&id).unwrap();
        assert_eq!(emitted, name);
        assert_eq!(
            Source::from_immediate_native_function_name(Some(emitted)),
            expected,
            "the complete source fact follows the actual emitted native name"
        );
        assert_eq!(
            Source::from_immediate_native_function_name(Some(emitted)).prefers_latin1(),
            matches!(expected, Source::NativeV1EncryptionLatin1),
            "the source fact also retains its byte interpretation"
        );
    }
    for raw_tag in [
        "AES_ENCRYPT",
        "base64_decode_binary",
        "base64_decode_string",
    ] {
        assert_eq!(
            Source::from_immediate_encryption_identity(Some(raw_tag)),
            Source::Ordinary,
            "a raw encryption identity does not resolve a wire alias"
        );
    }
}
#[test]
fn to_base64_actual_compiler_runtime_seven_causes_keep_source_and_first_failure_latch() {
    let long = "/w==";
    for shape in [Shape::Slot, Shape::FromBase64, Shape::CastFromBase64] {
        let program = program(shape);
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some(long); 321]));
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            vec![input],
        )
        .unwrap();
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
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
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(321), Selection::all(321)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual))if actual==cause)
                );
                assert_eq!(control.trace(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
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

/// Tests the sole Physical immediate-source author with actual catalogue signatures.
/// AES admission here is metadata; its separately missing pure callee is not claimed executable.
#[test]
fn to_base64_physical_author_exact_declared_carriers_and_immediate_definitions() {
    let catalog =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let utf8 = FunctionValueType::new(DataType::Utf8, true);
    let leaf = |id, kind, ty| novarocks_expression_contract::ExprNode {
        id: ExprId::new(id),
        lambda_scope: None,
        ty,
        kind,
    };
    let classify = |nodes: Vec<novarocks_expression_contract::ExprNode>, args: &[ExprId]| {
        let mut draft = novarocks_expression_contract::BoundExprSetBuilder::new(
            novarocks_expression_contract::UnitLimits {
                expression_semantic_depth: novarocks_physical_plan::PlanLimits::FROZEN
                    .expression_semantic_depth,
            },
        );
        for node in nodes {
            let id = draft.add(node.lambda_scope, node.ty, node.kind).unwrap();
            assert_eq!(id, node.id);
        }
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &Control,
            novarocks_type_contract::CompilePhase::Validate,
        )
        .unwrap();
        let result = novarocks_expression_contract::to_base64_byte_source_from_lookup_observed(
            |id| draft.definition(id),
            args,
            &mut work,
        );
        work.finish().unwrap();
        result
    };
    for kind in [
        ExprKind::Value(ValueId::new(91)),
        ExprKind::Constant(novarocks_physical_plan::ConstantReference {
            pool: novarocks_physical_plan::ConstantPoolId::new(2),
            ordinal: 7,
        }),
    ] {
        assert_eq!(
            classify(vec![leaf(0, kind, utf8.clone())], &[ExprId::new(0)]).unwrap(),
            Source::Ordinary
        );
    }

    assert!(matches!(
        classify(
            vec![leaf(0, ExprKind::Value(ValueId::new(91)), utf8.clone())],
            &[]
        ),
        Err(
            novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                "to_base64 source requires one argument"
            )
        )
    ));
    assert!(matches!(
        classify(
            vec![leaf(0, ExprKind::Value(ValueId::new(91)), utf8.clone())],
            &[ExprId::new(8)]
        ),
        Err(
            novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                "to_base64 source definition is absent"
            )
        )
    ));
    for (name, count, expected) in [
        ("aes_encrypt", 2, Source::NativeV1EncryptionLatin1),
        ("from_base64", 1, Source::NativeV1EncryptionLatin1),
        ("to_binary", 1, Source::NativeV1EncryptionLatin1),
        ("concat", 1, Source::Ordinary),
    ] {
        let arguments = vec![
            FunctionArgument::Value {
                value_type: utf8.clone(),
                constant: None
            };
            count
        ];
        let bound = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: count,
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap();
        let FunctionResultType::Scalar(result) = bound.selected.result_type.clone() else {
            panic!("actual scalar declaration");
        };
        let function = BoundFunction::from_exact_signature(
            bound.function_id,
            bound.selected.overload,
            FunctionKind::Scalar,
            bound.selected.argument_types,
            result.clone(),
        );
        let mut nodes: Vec<_> = (0..count)
            .map(|id| {
                leaf(
                    id as u32,
                    ExprKind::Value(ValueId::new(91 + id as u32)),
                    utf8.clone(),
                )
            })
            .collect();
        let call = ExprId::new(count as u32);
        nodes.push(leaf(
            call.get(),
            ExprKind::FunctionCall {
                function,
                args: (0..count).map(|i| ExprId::new(i as u32)).collect(),
            },
            result.clone(),
        ));
        let direct = classify(nodes.clone(), &[call]);
        if name == "to_binary" {
            assert_eq!(result.data_type().clone(), DataType::Binary);
            assert!(
                matches!(
                    direct,
                    Err(
                        novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                            "to_base64 emitted source is not exact Physical Utf8"
                        )
                    )
                ),
                "the actual Binary declaration is rejected before runtime NULL handling"
            );
        } else {
            assert_eq!(result.data_type().clone(), DataType::Utf8);
            assert_eq!(direct.unwrap(), expected);
        }
        let cast = ExprId::new(count as u32 + 1);
        nodes.push(leaf(
            cast.get(),
            ExprKind::Cast {
                expr: call,
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                },
            },
            utf8.clone(),
        ));
        assert_eq!(
            classify(nodes, &[cast]).unwrap(),
            Source::Ordinary,
            "CAST is the immediate emitted node, regardless of child identity"
        );
    }
}
