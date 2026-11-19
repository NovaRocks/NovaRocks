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
//! Selected string and byte evaluation from the original checked source forms.

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPools, ConstantValue};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprEvaluationFailure, ScopeLayouts,
    testkit::{
        CallbackControl, ClaimAuthor, HarnessControl, PassThroughCallHost, add_call,
        callback_causes, checked_constants, compile_fixture_unit, declare_call_controls,
        finish_unit, fixture_scope,
        frame_fixtures::{Control, POLICY},
        register_call_requests, register_constant,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingRequest, FunctionId,
    FunctionKind, FunctionOverloadId, InstalledPureKernel, KernelFailure,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, ToBase64ByteSource as Source,
};
use std::{collections::BTreeMap, sync::Arc};

struct Program {
    unit: Arc<CompiledExprSet>,
    root: RootOrdinal,
    schema: SchemaRef,
}

fn program(
    functions: &PureEngineFunctionCatalog,
    columns: &[ValueId],
    parameters: SemanticParameters,
    build: impl FnOnce(
        &mut BoundExprSetBuilder,
        &mut ConstantPools,
        &mut BTreeMap<ExprId, ClaimAuthor>,
        &FunctionValueType,
    ) -> ExprId,
) -> Program {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let mut constants = ConstantPools::empty();
    let mut authors = BTreeMap::new();
    // These typed NULLs originally authored the producer Project's columns.
    // Keep their ordered source addresses before consumer literal registration.
    for _ in columns {
        checked_constants::kind(&mut constants, checked_constants::null(&ty, POLICY));
    }
    let definition = build(&mut draft, &mut constants, &mut authors, &ty);
    let root = draft
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, _)| Field::new(format!("local_1_{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let port = ExprPort {
        values: columns.iter().map(|&value| (value, ty.clone())).collect(),
    };
    let scope = fixture_scope(&port, schema.clone());
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    declare_call_controls(
        &mut draft,
        functions,
        &constants,
        &parameters,
        POLICY,
        &mut work,
    );
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        POLICY,
        &mut work,
    );
    work.finish().unwrap();
    let unit = compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        POLICY,
        functions,
    );
    Program { unit, root, schema }
}

fn instance(program: &Program) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap()
}

fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}

fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
) -> ClaimAuthor {
    ClaimAuthor::with_legacy_binding(
        functions,
        name,
        arguments,
        ControlShape::Eager,
        None,
        POLICY,
        &Control,
    )
}

fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "guarded-origin-invalid",
        "guarded-origin-internal",
        "guarded-origin-operational",
    ])
}

fn parse_url_catalogue() -> PureEngineFunctionCatalog {
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

fn parse_url_program(arity: usize, constant_part: Option<&str>) -> Program {
    let functions = parse_url_catalogue();
    let values = (0..arity)
        .map(|i| ValueId::new(91 + u32::try_from(i).unwrap()))
        .collect::<Vec<_>>();
    program(
        &functions,
        &values,
        SemanticParameters::try_new([]).unwrap(),
        |draft, source_pools, authors, ty| {
            let mut args = Vec::new();
            let mut arguments = Vec::new();
            for (i, value) in values.iter().enumerate() {
                let cv = if i == 1 {
                    constant_part.map(|text| {
                        ConstantValue::from_utf8(
                            Arc::new(ty.try_to_field("actual checked part").unwrap()),
                            ty.clone(),
                            text,
                            POLICY,
                            CompilePhase::Validate,
                            &Control,
                        )
                        .unwrap()
                    })
                } else {
                    None
                };
                let expr = draft
                    .add(
                        None,
                        ty.clone(),
                        if let Some(value) = &cv {
                            ExprKind::Constant(register_constant(source_pools, value))
                        } else {
                            ExprKind::Value(*value)
                        },
                    )
                    .unwrap();
                args.push(expr);
                arguments.push(argument(ty.clone(), cv));
            }

            let owner = author(&functions, "parse_url", arguments);
            assert_eq!(&owner.result(), ty);
            add_call(draft, authors, owner, args)
        },
    )
}

#[test]
fn parse_url_actual_compiler_both_profiles_dynamic_and_native_constants_keep_key_null_semantics() {
    for arity in [2, 3] {
        for constant_part in [None, Some("HOST"), Some("QUERY")] {
            let program = parse_url_program(arity, constant_part);
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
            let batch =
                RecordBatch::try_new(program.schema.clone(), arrays[..arity].to_vec()).unwrap();
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
        let program = parse_url_program(arity, None);
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some(long.as_str()), None]));
        let part: ArrayRef = Arc::new(StringArray::from(vec![Some("PATH"), Some("HOST")]));
        let key: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 2]));
        let arrays = [text, part, key];
        let batch = RecordBatch::try_new(program.schema.clone(), arrays[..arity].to_vec()).unwrap();
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

fn regexp_count_catalogue() -> PureEngineFunctionCatalog {
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

fn regexp_count_program(constant: Option<&str>) -> Program {
    let functions = regexp_count_catalogue();
    let values = [ValueId::new(91), ValueId::new(7)];
    program(
        &functions,
        &values,
        SemanticParameters::try_new([]).unwrap(),
        |draft, source_pools, authors, ty| {
            let text = draft
                .add(None, ty.clone(), ExprKind::Value(values[0]))
                .unwrap();
            let cv = constant.map(|text| {
                ConstantValue::from_utf8(
                    Arc::new(ty.try_to_field("actual checked constant").unwrap()),
                    ty.clone(),
                    text,
                    POLICY,
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap()
            });
            let pattern = draft
                .add(
                    None,
                    ty.clone(),
                    if let Some(value) = &cv {
                        ExprKind::Constant(register_constant(source_pools, value))
                    } else {
                        ExprKind::Value(values[1])
                    },
                )
                .unwrap();

            let owner = author(
                &functions,
                "regexp_count",
                vec![argument(ty.clone(), None), argument(ty.clone(), cv)],
            );
            assert_eq!(
                owner.result(),
                FunctionValueType::new(DataType::Int64, true)
            );
            add_call(draft, authors, owner, vec![text, pattern])
        },
    )
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
        let p = regexp_count_program(constant);
        let batch =
            RecordBatch::try_new(p.schema.clone(), vec![text.clone(), patterns.clone()]).unwrap();
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

#[derive(Clone, Copy)]
enum Shape {
    Slot,
    Constant,
    FromBase64,
    CastFromBase64,
    ConcatFromBase64,
}
fn base64_catalogue() -> PureEngineFunctionCatalog {
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

fn base64_program(shape: Shape) -> Program {
    let functions = base64_catalogue();
    let value = ValueId::new(91);
    let parameters = if matches!(shape, Shape::CastFromBase64) {
        SemanticParameters::try_new([(
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap()
    } else {
        SemanticParameters::default()
    };

    program(
        &functions,
        &[value],
        parameters,
        |draft, source_pools, authors, ty| {
            let cv = matches!(shape, Shape::Constant).then(|| {
                ConstantValue::from_utf8(
                    Arc::new(ty.try_to_field("actual checked source").unwrap()),
                    ty.clone(),
                    "ÿ",
                    POLICY,
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap()
            });
            let leaf = draft
                .add(
                    None,
                    ty.clone(),
                    if let Some(value) = &cv {
                        ExprKind::Constant(register_constant(source_pools, value))
                    } else {
                        ExprKind::Value(value)
                    },
                )
                .unwrap();
            let mut child = leaf;
            if matches!(
                shape,
                Shape::FromBase64 | Shape::CastFromBase64 | Shape::ConcatFromBase64
            ) {
                let owner = author(&functions, "from_base64", vec![argument(ty.clone(), None)]);
                child = add_call(draft, authors, owner, vec![child]);
            }
            if matches!(shape, Shape::CastFromBase64) {
                child = draft
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
                let owner = author(&functions, "concat", vec![argument(ty.clone(), None)]);
                child = add_call(draft, authors, owner, vec![child]);
            }

            let owner = author(&functions, "to_base64", vec![argument(ty.clone(), cv)]);
            assert_eq!(&owner.result(), ty);
            add_call(draft, authors, owner, vec![child])
        },
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
        let program = base64_program(shape);
        let batch = RecordBatch::try_new(program.schema.clone(), vec![input.clone()]).unwrap();
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
        let records = program.unit.calls();
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
fn to_base64_actual_compiler_runtime_seven_causes_keep_source_and_first_failure_latch() {
    let long = "/w==";
    for shape in [Shape::Slot, Shape::FromBase64, Shape::CastFromBase64] {
        let program = base64_program(shape);
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some(long); 321]));
        let batch = RecordBatch::try_new(program.schema.clone(), vec![input]).unwrap();
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
