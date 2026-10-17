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
//! Actual source journal/effects, original package author, LocalCompiler and
//! Frame. The shared SQL fixture deliberately disables folding; native whole
//! cases and production FE folding remain separate acceptance obligations.
use super::compiled_program::CompiledExpressionInstance;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use super::numeric_unary_owned_transaction_tests::{batch, programs_with_catalogue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, function};
use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_types::SlotId;
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
fn original(input: ArrayRef) -> ArrayRef {
    let dtype = input.data_type().clone();
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "operand",
            dtype.clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(data.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let mut arena = ExprArena::default();
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), dtype.clone());
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::lookup_function("bitnot").unwrap(),
            args: vec![child],
        },
        dtype,
    );
    arena
        .eval(root, &Chunk::new_with_chunk_schema(data, schema))
        .unwrap()
}
const EXACT: SqlPhysicalEmissionMode =
    SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration;
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
fn producer_root(
    plans: &std::collections::BTreeMap<
        novarocks_physical_plan::FragmentId,
        Arc<novarocks_local_program::LocalProgram>,
    >,
) -> (
    Arc<novarocks_local_program::LocalProgram>,
    ProgramExpressionRootSite,
) {
    let mut found = None;
    for program in plans.values() {
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        for (&root, &use_id) in snapshot.bindings() {
            let occurrence = novarocks_local_program::ProgramUseRef {
                arena: root.arena(),
                use_id,
            };
            if program.native_bitnot_recipe(occurrence).is_some() {
                assert!(
                    found.is_none(),
                    "this actual SQL source declares exactly one BitwiseNot output root"
                );
                found = Some((program.clone(), root));
            }
        }
    }
    found.expect("the actual published root binding retains its owned BitwiseNot recipe")
}
#[test]
fn native_bitnot_intrinsic_actual_sql_signed_four_widths_frame_matches_original_and_full_fvt() {
    let cases: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
        Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
        Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
        Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
    ];
    for input in cases {
        let source = sql_source(
            "SELECT ~k AS original_inverted FROM fixture.source",
            input.data_type().clone(),
            EXACT,
        );
        let public = source
            .checked_original_result_declaration_observed(&Control)
            .unwrap()
            .unwrap();
        let computed = source.plan().result_port().unwrap();
        assert_eq!(public.original_port(), computed);
        assert!(!public.fields()[0].ty.nullable());
        let plans = programs(&source);
        let (program, root) = producer_root(&plans);
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
        let expected = original(input.clone());
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
fn native_bitnot_intrinsic_actual_sql_largeint_cast_uses_exact_logical_recipe() {
    let source = sql_source(
        "SELECT ~CAST(k AS LARGEINT) AS original_inverted FROM fixture.source",
        DataType::Int64,
        EXACT,
    );
    let plans = programs(&source);
    let (program, root) = producer_root(&plans);
    let data = batch(
        &program,
        root,
        Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, i64::MAX])),
    );
    let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
    let out = frame.evaluate(&data, Selection::all(4), &Control).unwrap();
    let argument = novarocks_types::largeint::array_from_i128(&[
        Some(i64::MIN as i128),
        Some(-1),
        Some(0),
        Some(i64::MAX as i128),
    ])
    .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().to_data(), original(argument).to_data());
    assert_eq!(
        source.plan().result_port().unwrap().fields[0]
            .ty
            .logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
}
#[test]
fn native_bitnot_intrinsic_actual_sql_nullable_values_current_domain_matches_original() {
    let source = sql_source(
        "SELECT ~i AS original_inverted FROM (VALUES (CAST(1 AS BIGINT)),(CAST(NULL AS BIGINT)),(CAST(-2 AS BIGINT))) AS source(i)",
        DataType::Int64,
        EXACT,
    );
    let plans = programs(&source);
    let (program, root) = producer_root(&plans);
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]));
    let data = batch(&program, root, input.clone());
    let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
    let out = frame.evaluate(&data, Selection::all(3), &Control).unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().to_data(), original(input).to_data());
    let rows = [0, 2];
    let sparse = frame
        .evaluate(&data, Selection::try_sparse(3, &rows).unwrap(), &Control)
        .unwrap();
    assert_eq!(
        sparse.values().to_data(),
        Int64Array::from(vec![-2, 1]).to_data()
    );
    assert!(source.plan().result_port().unwrap().fields[0].ty.nullable());
}
fn constant_outputs(sql: &str) -> Vec<ArrayRef> {
    let source = sql_source(sql, DataType::Int64, EXACT);
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
fn native_bitnot_intrinsic_actual_sql_largeint_full128_and_nullable_constant_sources() {
    for value in [
        Some(i128::MIN),
        Some(i128::MAX),
        Some((1_i128 << 100) + 5),
        None,
    ] {
        // Numeric literals exercise full-width authoring independently of the
        // explicit Utf8 -> LargeInt conversion covered below.
        let operand = match value {
            Some(value) => value.to_string(),
            None => "CAST(NULL AS LARGEINT)".to_owned(),
        };
        let outputs = constant_outputs(&format!("SELECT ~{operand} AS original_inverted"));
        assert_eq!(outputs.len(), 1);
        let original_input = novarocks_types::largeint::array_from_i128(&[value]).unwrap();
        assert_eq!(outputs[0].to_data(), original(original_input).to_data());
    }
}
#[test]
fn native_bitnot_intrinsic_exact_physical_null_is_refused_before_folding() {
    let refused = std::panic::catch_unwind(|| sql_source("SELECT ~NULL", DataType::Int64, EXACT))
        .unwrap_err();
    let message = refused
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| refused.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(
        message.contains("native BitwiseNot unsupported source shape"),
        "actual pre-fold source refusal: {message}"
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
    let source = sql_source("SELECT ~k FROM fixture.source", DataType::Int64, EXACT);
    let plans = programs(&source);
    let (program, root) = producer_root(&plans);
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

    let sql = "SELECT ~CAST('-170141183460469231731687303715884105728' AS LARGEINT)";
    let expected_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for mode in [SqlPhysicalEmissionMode::OriginalNativeV1, EXACT] {
        let source = sql_source(sql, DataType::Int64, mode);
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
