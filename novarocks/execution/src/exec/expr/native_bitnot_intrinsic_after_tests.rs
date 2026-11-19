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
//! Typed native BitwiseNot packages and independently authored named SQL controls.
use super::compiled_program::CompiledExpressionInstance;
use super::compiled_program::guarded_tests::{
    call, trusted_author, try_compile_checked_fragment_with_parameters,
};
use super::numeric_unary_owned_transaction_tests::{batch, programs_with_catalogue};
use super::sql_test_source::sql_source;
use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection,
};
use novarocks_local_compiler::FragmentCompileError;
use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite};
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
};
use novarocks_physical_plan::{
    ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeId, PipelineDopDomain,
    ResultField, ResultPort, StagePlacementPolicy, UnaryOperator, ValueOrigin,
};
use novarocks_type_contract::{
    ControlShape, FunctionValueType, SemanticParameters, ValueLogicalType,
};
use std::collections::BTreeMap;

use std::sync::{Arc, Mutex};
use std::time::Duration;
struct Control;
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        n: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BitwiseNot frame never waits")
    }
}
// Loan the already installed catalogue author; the old unary fixture's
// deliberately IF/COALESCE-only wrapper is unchanged.
fn programs(
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
) -> std::collections::BTreeMap<
    novarocks_physical_plan::FragmentId,
    Arc<novarocks_local_program::LocalProgram>,
> {
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    programs_with_catalogue(source, &functions)
}
enum Operand {
    Column,
    Literal(LiteralValue),
}

fn nullable_largeint() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

fn intrinsic_program(
    source_type: FunctionValueType,
    operand: Operand,
    conversion_overload: Option<&str>,
) -> Result<
    (
        Arc<novarocks_local_program::LocalProgram>,
        ProgramExpressionRootSite,
    ),
    FragmentCompileError,
> {
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let fragment_id = FragmentId::new(211);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let mut builder = FragmentBuilder::new(fragment_id);
    let source = match operand {
        Operand::Column => {
            // The empty VALUES leaf declares the exact input port. Each
            // frame test lends a matching batch directly to the project.
            let value = builder
                .add_value(
                    source_type.clone(),
                    ValueOrigin::NodeOutput {
                        node: input,
                        output_ordinal: 0,
                    },
                )
                .unwrap();
            builder
                .add_values(input, Box::default(), Box::from([value]))
                .unwrap();
            ExprKind::Value(value)
        }
        Operand::Literal(value) => {
            builder
                .add_values(input, Box::from([Box::default()]), Box::default())
                .unwrap();
            ExprKind::Literal(value)
        }
    };
    let source = builder
        .add_expression(output, source_type.clone(), source)
        .unwrap();
    let mut authors = BTreeMap::new();
    let (input_expr, result_type) = if conversion_overload.is_some() {
        let target = nullable_largeint();
        let owner = trusted_author(
            &functions,
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME,
            vec![FunctionArgument::Value {
                value_type: source_type,
                constant: None,
            }],
            ControlShape::Eager,
            target.clone(),
        );
        (
            call(&mut builder, &mut authors, owner, vec![source]),
            target,
        )
    } else {
        (source, source_type)
    };
    let bitnot = builder
        .add_expression(
            output,
            result_type.clone(),
            ExprKind::Unary {
                op: UnaryOperator::BitwiseNot,
                expr: input_expr,
            },
        )
        .unwrap();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: bitnot,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(bitnot, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    if let Some(overload) = conversion_overload {
        let ExprKind::FunctionCall { function, .. } =
            &fragment.expressions().get(input_expr).unwrap().kind
        else {
            panic!("expected the trusted LARGEINT conversion");
        };
        assert_eq!(function.overload.as_str(), overload);
    }
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "original_inverted".into(),
            alias: None,
            value: result_value,
            ty: result_type.clone(),
        }]),
    };
    let program = try_compile_checked_fragment_with_parameters(
        &functions,
        fragment,
        &authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
    )?;
    let resolved = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let mut roots = resolved.bindings().iter().filter_map(|(&root, &use_id)| {
        let occurrence = novarocks_local_program::ProgramUseRef {
            arena: root.arena(),
            use_id,
        };
        program
            .native_bitnot_recipe(occurrence)
            .map(|recipe| (root, recipe))
    });
    let (root, recipe) = roots.next().expect("one native BitwiseNot project root");
    assert!(roots.next().is_none());
    assert_eq!(recipe.source_type(), &result_type);
    assert_eq!(recipe.result_type(), &result_type);
    let ProgramExpressionRootSite::Node { node, .. } = root else {
        panic!("native BitwiseNot project site");
    };
    let actual = program
        .checked()
        .channels()
        .channel_type(ProgramChannelSite::Layout {
            node,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        });
    assert_eq!(actual, Some(&result_type));
    Ok((program, root))
}

fn intrinsic_constant(
    source: FunctionValueType,
    operand: LiteralValue,
    conversion_overload: Option<&str>,
) -> ArrayRef {
    let (program, root) =
        intrinsic_program(source, Operand::Literal(operand), conversion_overload).unwrap();
    let ProgramExpressionRootSite::Node { node, .. } = root else {
        panic!("constant project");
    };
    let ProgramNodeKind::Project { input, .. } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("constant project");
    };
    let ProgramNodeKind::Values { values } = program.graph().nodes()[input.index()].kind() else {
        panic!("constant VALUES");
    };
    let data = values.batch().unwrap();
    assert_eq!(data.num_rows(), 1);
    let mut frame = CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
    let out = frame.evaluate(data, Selection::all(1), &Control).unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().len(), 1);
    out.values().clone()
}

#[test]
fn native_bitnot_intrinsic_typed_signed_four_widths_preserve_full_fvt() {
    // For a fixed signed two's-complement width, complement swaps MIN/MAX,
    // maps -1 to zero, zero to -1, and one to -2. These are independent inputs
    // and fixed expected arrays, not an evaluator over the supplied input.
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int8Array::from(vec![i8::MAX, 0, -1, -2, i8::MIN])),
        ),
        (
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int16Array::from(vec![i16::MAX, 0, -1, -2, i16::MIN])),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            Arc::new(Int32Array::from(vec![i32::MAX, 0, -1, -2, i32::MIN])),
        ),
        (
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, 0, -1, -2, i64::MIN])),
        ),
    ];
    for (input, expected) in cases {
        let ty = FunctionValueType::new(input.data_type().clone(), false);
        let (program, root) = intrinsic_program(ty, Operand::Column, None).unwrap();
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
        let all = frame
            .evaluate(&data, Selection::all(input.len()), &Control)
            .unwrap();
        assert!(all.errors().is_empty());
        assert_eq!(all.values().to_data(), expected.to_data());
        let rows = [0, 3, 4];
        let sparse = frame
            .evaluate(
                &data,
                Selection::try_sparse(input.len(), &rows).unwrap(),
                &Control,
            )
            .unwrap();
        assert!(sparse.errors().is_empty());
        let wanted = arrow::compute::take(
            expected.as_ref(),
            &arrow::array::UInt64Array::from(vec![0, 3, 4]),
            None,
        )
        .unwrap();
        assert_eq!(sparse.values().to_data(), wanted.to_data());
        assert_eq!(
            frame
                .evaluate(
                    &data,
                    Selection::try_sparse(input.len(), &[]).unwrap(),
                    &Control
                )
                .unwrap()
                .values()
                .len(),
            0
        );
        assert_eq!(
            frame
                .evaluate(&data.slice(1, 0), Selection::all(0), &Control)
                .unwrap()
                .values()
                .data_type(),
            input.data_type()
        );
    }
}
#[test]
fn native_bitnot_intrinsic_typed_signed_to_largeint_uses_exact_logical_recipe() {
    let (program, root) = intrinsic_program(
        FunctionValueType::new(DataType::Int64, false),
        Operand::Column,
        Some("builtin.scalar/value_domain_conversion/signed_to_largeint/v1"),
    )
    .unwrap();
    let data = batch(
        &program,
        root,
        Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, i64::MAX])),
    );
    let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
    let out = frame.evaluate(&data, Selection::all(4), &Control).unwrap();
    // The SQL cast sign-extends each INT64 to 128 bits. Its complement
    // follows ~x = -x - 1 over signed 128-bit values, without narrowing back.
    let expected = novarocks_types::largeint::array_from_i128(&[
        Some(9_223_372_036_854_775_807),
        Some(0),
        Some(-1),
        Some(-9_223_372_036_854_775_808),
    ])
    .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().to_data(), expected.to_data());
}
#[test]
fn native_bitnot_intrinsic_typed_nullable_values_preserve_output_and_selection() {
    let (program, root) = intrinsic_program(
        FunctionValueType::new(DataType::Int64, true),
        Operand::Column,
        None,
    )
    .unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]));
    let data = batch(&program, root, input.clone());
    let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
    let out = frame.evaluate(&data, Selection::all(3), &Control).unwrap();
    assert!(out.errors().is_empty());
    // Complement keeps NULL and maps 1 -> -2, -2 -> 1.
    assert_eq!(
        out.values().to_data(),
        Int64Array::from(vec![Some(-2), None, Some(1)]).to_data()
    );
    let rows = [0, 2];
    let sparse = frame
        .evaluate(&data, Selection::try_sparse(3, &rows).unwrap(), &Control)
        .unwrap();
    assert_eq!(
        sparse.values().to_data(),
        Int64Array::from(vec![-2, 1]).to_data()
    );
    assert!(sparse.errors().is_empty());
}
fn constant_outputs(sql: &str) -> Vec<ArrayRef> {
    let source = sql_source(sql, DataType::Int64);
    let plans = programs(&source);
    let mut actual = Vec::new();
    for program in plans.values() {
        for node in program.graph().nodes() {
            let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                continue;
            };
            let parent = &program.graph().nodes()[input.index()];
            let ProgramNodeKind::Values { values } = parent.kind() else {
                panic!("this actual scalar source must retain its original VALUES invocation")
            };
            let data = values
                .batch()
                .expect("the original table-free input is a constant VALUES batch");
            for ordinal in 0..exprs.len() {
                let root = ProgramExpressionRootSite::Node {
                    node: node.local_id().unwrap(),
                    role: ProgramNodeExpressionRole::ProjectOutput {
                        expression: u32::try_from(ordinal).unwrap(),
                    },
                };
                let mut frame =
                    CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
                let out = frame
                    .evaluate(data, Selection::all(data.num_rows()), &Control)
                    .unwrap();
                assert!(out.errors().is_empty());
                assert_eq!(out.values().len(), 1);
                actual.push(out.values().clone());
            }
        }
    }
    actual
}
#[test]
fn native_bitnot_intrinsic_actual_project_sql_unchanged_constants_execute_each_root() {
    let sql = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/project/sql/project_bitnot_operator_semantics.sql"
    ));
    let outputs = constant_outputs(sql);
    let actual = outputs
        .iter()
        .map(|out| out.as_any().downcast_ref::<Int64Array>().unwrap().value(0))
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![0, -1, -2, -1025]);
}
#[test]
fn native_bitnot_intrinsic_typed_largeint_full128_and_nullable_constant_sources() {
    for (value, expected) in [
        (Some(i128::MIN), Some(i128::MAX)),
        (Some(i128::MAX), Some(i128::MIN)),
        // 2^100 + 5 complements to -2^100 - 6. The decimal expected value
        // is fixed independently of the runtime bit-array implementation.
        (
            Some((1_i128 << 100) + 5),
            Some(-1_267_650_600_228_229_401_496_703_205_382_i128),
        ),
        (None, None),
    ] {
        let output = match value {
            Some(value) => intrinsic_constant(
                FunctionValueType::new(DataType::Utf8, false),
                LiteralValue::Utf8(value.to_string().into_boxed_str()),
                Some("builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"),
            ),
            None => intrinsic_constant(nullable_largeint(), LiteralValue::Null, None),
        };
        let expected = novarocks_types::largeint::array_from_i128(&[expected]).unwrap();
        assert_eq!(output.to_data(), expected.to_data());
    }
}
#[test]
fn native_bitnot_intrinsic_exact_physical_null_is_refused_before_folding() {
    let Err(refused) = intrinsic_program(
        FunctionValueType::new(DataType::Null, true),
        Operand::Literal(LiteralValue::Null),
        None,
    ) else {
        panic!("physical NULL must fail during LocalCompiler preparation");
    };
    assert!(matches!(refused, FragmentCompileError::Owner { .. }));
    assert!(
        refused
            .to_string()
            .contains("native BitwiseNot unsupported source shape"),
        "typed pre-frame refusal: {refused}"
    );
}
struct Refuse {
    at: usize,
    cause: KernelFailure,
    trace: Mutex<Vec<u32>>,
    failed: Mutex<bool>,
}
impl Refuse {
    fn new(at: usize, cause: KernelFailure) -> Self {
        Self {
            at,
            cause,
            trace: Mutex::new(vec![]),
            failed: Mutex::new(false),
        }
    }
}
impl KernelEvaluationControl for Refuse {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut failed = self.failed.lock().unwrap();
        assert!(!*failed, "no failure footer is permitted");
        let mut trace = self.trace.lock().unwrap();
        trace.push(n);
        if trace.len() == self.at {
            *failed = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BitwiseNot never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("bitnot-test-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("bitnot-test-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("bitnot-test-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn native_bitnot_intrinsic_actual_frame_every_constructor_and_runtime_callback_keeps_cause_and_never_replays()
 {
    let (program, root) = intrinsic_program(
        FunctionValueType::new(DataType::Int64, false),
        Operand::Column,
        None,
    )
    .unwrap();
    let data = batch(
        &program,
        root,
        Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
    );
    let recorder = Refuse::new(usize::MAX, KernelFailure::Cancelled);
    let _ = CompiledExpressionInstance::try_new(program.clone(), root, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 1..=trace.len() {
        for cause in causes() {
            let control = Refuse::new(at, cause.clone());
            let result = CompiledExpressionInstance::try_new(program.clone(), root, &control);
            assert!(matches!(result,Err(actual) if actual==cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
    let mut frame = CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
    let recorder = Refuse::new(usize::MAX, KernelFailure::Cancelled);
    frame.evaluate(&data, Selection::all(5), &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 1..=trace.len() {
        for cause in causes() {
            let mut frame =
                CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
            let control = Refuse::new(at, cause.clone());
            assert!(
                matches!(frame.evaluate(&data,Selection::all(5),&control),Err(actual) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            assert!(matches!(
                frame.evaluate(&data, Selection::all(5), &Control),
                Err(KernelFailure::InstanceFailed)
            ));
        }
    }
}

#[test]
fn native_bitnot_intrinsic_original_and_exact_utf8_largeint_source_keeps_conversion_and_frame_result()
 {
    use novarocks_functions::builtin::value_conversion::{
        VALUE_CONVERSION_FUNCTION_ID, resolve_value_conversion,
    };
    use novarocks_functions::{FunctionArgumentType, FunctionValueType};
    use novarocks_physical_plan::ExprKind;
    use novarocks_type_contract::ValueLogicalType;

    let sql = "SELECT bitnot(CAST('-170141183460469231731687303715884105728' AS LARGEINT))";
    let expected_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    {
        let source = sql_source(sql, DataType::Int64);
        assert_eq!(
            source.plan().result_port().unwrap().fields[0].ty,
            expected_type
        );
        let mut conversions = 0;
        for fragment in source.plan().fragments().values() {
            for (_, expression) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &expression.kind else {
                    continue;
                };
                if function.function_id.as_str() != VALUE_CONVERSION_FUNCTION_ID {
                    continue;
                }
                conversions += 1;
                assert_eq!(
                    function.overload.as_str(),
                    "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"
                );
                assert_eq!(expression.ty, expected_type);
                assert_eq!(args.len(), 1);
                let input = &fragment.expressions().get(args[0]).unwrap().ty;
                assert_eq!(input, &FunctionValueType::new(DataType::Utf8, false));
                assert_eq!(
                    function.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(input.clone())]
                );
                assert_eq!(function.result_type, expected_type);
            }
        }
        assert_eq!(
            conversions, 1,
            "the original declared conversion remains explicit"
        );
    }
    // The installed owner, package author, LocalCompiler and Frame execute the
    // frozen conversion before BitwiseNot. MIN's complement is independently MAX.
    let outputs = constant_outputs(sql);
    assert_eq!(outputs.len(), 1);
    let expected = novarocks_types::largeint::array_from_i128(&[Some(i128::MAX)]).unwrap();
    assert_eq!(outputs[0].to_data(), expected.to_data());

    // Equal-width bytes are not authorization for either the conversion or
    // the exact BitwiseNot recipe. Only the explicit nominal source is admitted.
    for source in [
        FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(resolve_value_conversion(&source, &expected_type, &Control).is_err());
        assert!(!novarocks_functions::native_bitnot_source_supported(
            &source
        ));
    }
}
