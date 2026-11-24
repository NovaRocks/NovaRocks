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
use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_local_program::{ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_sql_testkit::sql_source;
use novarocks_test_compile_to_execute::programs::{batch, programs_with_catalogue};
use std::sync::Arc;
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
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    programs_with_catalogue(source, &functions)
}
fn scalar_call_root(
    plans: &std::collections::BTreeMap<
        novarocks_physical_plan::FragmentId,
        Arc<novarocks_local_program::LocalProgram>,
    >,
    function_id: &str,
    argument: &FunctionValueType,
    result: &FunctionValueType,
) -> (Arc<novarocks_local_program::LocalProgram>, ProgramRootRef) {
    let mut found = None;
    for program in plans.values() {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                if let Some(call) = unit.calls().get(&root.use_id)
                    && call.call_contract().function_id().as_str() == function_id
                {
                    assert!(
                        found.is_none(),
                        "the SQL has exactly one root for {function_id}"
                    );
                    let contract = call.call_contract();
                    assert_eq!(contract.logical_argument_count(), 1);
                    assert_eq!(
                        contract.selected().argument_types.as_slice(),
                        &[FunctionArgumentType::Value(argument.clone())]
                    );
                    assert_eq!(
                        contract.selected().result_type,
                        FunctionResultType::Scalar(result.clone())
                    );
                    assert_eq!(
                        unit.definitions()
                            .node(root.definition)
                            .unwrap()
                            .value_type(),
                        Some(result)
                    );
                    found = Some((
                        program.clone(),
                        ProgramRootRef {
                            unit: ProgramUnitId::new(unit_ordinal),
                            root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                        },
                    ));
                }
            }
        }
    }
    found.expect("the actual SQL output retains the bound scalar call")
}
#[test]
fn bitwise_not_widens_signed_input_widths_to_bigint_in_the_frame() {
    // Narrow inputs are sign-extended to BIGINT before complement. The
    // expected arrays contain independent known complements of those values.
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int64Array::from(vec![127, 0, -1, -2, -128])),
        ),
        (
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int64Array::from(vec![32767, 0, -1, -2, -32768])),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            Arc::new(Int64Array::from(vec![2147483647, 0, -1, -2, -2147483648])),
        ),
        (
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, 0, -1, -2, i64::MIN])),
        ),
    ];
    for (input, expected) in cases {
        let source = sql_source(
            "SELECT bitwise_not(k) AS original_inverted FROM fixture.source",
            input.data_type().clone(),
        );
        let public = source
            .checked_original_result_declaration_observed(&Control)
            .unwrap()
            .unwrap();
        let computed = source.plan().result_port().unwrap();
        assert_eq!(public.original_port(), computed);
        let argument = FunctionValueType::new(DataType::Int64, false);
        let result = FunctionValueType::new(DataType::Int64, true);
        assert_eq!(public.fields()[0].ty, result);
        let plans = programs(&source);
        let (program, root) =
            scalar_call_root(&plans, "builtin.scalar/bitwise_not/v1", &argument, &result);
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &Control,
        )
        .unwrap();
        let all = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = Selection::all(input.len());
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(all.errors().is_empty());
        assert_eq!(all.values().to_data(), expected.to_data());
        let rows = [0, 3, 4];
        let sparse = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(input.len(), &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
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
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(input.len(), &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .values()
                .len(),
            0
        );
        assert_eq!(
            frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data.slice(1, 0),
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .values()
                .data_type(),
            &DataType::Int64
        );
    }
}
#[test]
fn actual_sql_rejects_retired_largeint_casts_at_the_type_declaration() {
    use arrow::datatypes::Field;
    use novarocks_sql::analyze_error::AnalyzeErrorKind;
    use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;

    let columns = [Field::new("k", DataType::Int64, false)];
    for sql in [
        "SELECT CAST(k AS LARGEINT) AS original_value FROM fixture.source",
        "SELECT CAST('-170141183460469231731687303715884105728' AS LARGEINT)",
        "SELECT CAST('170141183460469231731687303715884105727' AS LARGEINT)",
        "SELECT CAST('1267650600228229401496703205381' AS LARGEINT)",
        "SELECT CAST(NULL AS LARGEINT)",
        "SELECT bitwise_not(CAST(k AS LARGEINT)) FROM fixture.source",
    ] {
        let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) =
            try_sql_source_with_declared_fields(sql, &columns)
        else {
            panic!("Expected a SQL analysis error for {sql}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch, "{sql}");
        assert_eq!(error.code().as_str(), "sql.analyze.type_mismatch", "{sql}");
        assert_eq!(error.message(), "Unknown type: largeint", "{sql}");
        let span = error.span().expect("retired type's source span");
        assert_eq!(&sql[span.start()..span.end()], "LARGEINT", "{sql}");
    }
}
#[test]
fn named_bitwise_not_actual_sql_nullable_values_preserve_nulls_and_sparse_results() {
    let source = sql_source(
        "SELECT bitwise_not(i) AS original_inverted FROM (VALUES (CAST(1 AS BIGINT)),(CAST(NULL AS BIGINT)),(CAST(-2 AS BIGINT))) AS source(i)",
        DataType::Int64,
    );
    let nullable_bigint = FunctionValueType::new(DataType::Int64, true);
    let plans = programs(&source);
    let (program, root) = scalar_call_root(
        &plans,
        "builtin.scalar/bitwise_not/v1",
        &nullable_bigint,
        &nullable_bigint,
    );
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]));
    let data = batch(&program, root, input.clone());
    let mut frame = CompiledExpressionInstance::try_new(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &Control,
    )
    .unwrap();
    let out = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(out.errors().is_empty());
    // Complement keeps NULL and maps 1 -> -2, -2 -> 1.
    assert_eq!(
        out.values().to_data(),
        Int64Array::from(vec![Some(-2), None, Some(1)]).to_data()
    );
    let rows = [0, 2];
    let sparse = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        sparse.values().to_data(),
        Int64Array::from(vec![-2, 1]).to_data()
    );
    assert_eq!(
        source.plan().result_port().unwrap().fields[0].ty,
        nullable_bigint
    );
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
            for &root in exprs {
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(root.unit).unwrap().clone(),
                    root.root,
                    &Control,
                )
                .unwrap();
                let out = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: data,
                            source: None,
                        },
                        {
                            let selection = Selection::all(data.num_rows());
                            ScalarInvocationScope::new(selection, selection)
                        },
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
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
fn native_bitwise_not_intrinsic_actual_project_sql_unchanged_constants_execute_each_root() {
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
fn typed_largeint_conversion_retains_exact_signed_text_and_null_results() {
    use arrow::array::{NullArray, StringArray};
    use arrow::record_batch::RecordBatch;
    use novarocks_expression::testkit::guarded_fixtures::value_conversion::{
        direct_program, largeint_type,
    };
    use novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID;

    let signed = [i64::MIN, -1, 0, i64::MAX];
    let wide = [i128::MIN, i128::MAX, (1_i128 << 100) + 5];
    let cases: [(ArrayRef, &str, Vec<Option<i128>>); 3] = [
        (
            Arc::new(Int64Array::from(signed.to_vec())),
            "builtin.scalar/value_domain_conversion/signed_to_largeint/v1",
            signed
                .into_iter()
                .map(|value| Some(i128::from(value)))
                .collect(),
        ),
        (
            Arc::new(StringArray::from(
                wide.map(|value| value.to_string()).to_vec(),
            )),
            "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1",
            wide.into_iter().map(Some).collect(),
        ),
        (
            Arc::new(NullArray::new(1)),
            "builtin.scalar/value_domain_conversion/null_to_typed_nullable/v1",
            vec![None],
        ),
    ];
    let target = largeint_type();
    for (input, overload, expected) in cases {
        let source = FunctionValueType::new(input.data_type().clone(), true);
        let program = direct_program(source.clone(), target.clone());
        let root = program.unit.root(program.root).unwrap();
        let call = program
            .unit
            .calls()
            .get(&root.use_id)
            .unwrap()
            .call_contract();
        assert_eq!(call.function_id().as_str(), VALUE_CONVERSION_FUNCTION_ID);
        assert_eq!(call.logical_argument_count(), 1);
        assert_eq!(call.selected().overload.as_str(), overload);
        assert_eq!(
            call.selected().argument_types.as_slice(),
            &[FunctionArgumentType::Value(source)]
        );
        assert_eq!(
            call.selected().result_type,
            FunctionResultType::Scalar(target.clone())
        );
        assert_eq!(
            program
                .unit
                .definitions()
                .node(root.definition)
                .unwrap()
                .value_type(),
            Some(&target)
        );

        let data = RecordBatch::try_new(program.input_schema().clone(), vec![input]).unwrap();
        let selection = Selection::all(data.num_rows());
        let out = program
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(out.errors().is_empty());
        let expected = novarocks_types::largeint::array_from_i128(&expected).unwrap();
        assert_eq!(out.values().to_data(), expected.to_data());
    }
}

#[test]
fn typed_largeint_conversion_accepts_exact_nonnull_text_and_rejects_equal_width_aliases() {
    use novarocks_expression::testkit::guarded_fixtures::value_conversion::largeint_type;
    use novarocks_functions::builtin::value_conversion::resolve_value_conversion;
    use novarocks_type_contract::ValueLogicalType;

    let target = largeint_type();
    let text = FunctionValueType::new(DataType::Utf8, false);
    let selected = resolve_value_conversion(&text, &target, &Control).unwrap();
    assert_eq!(
        selected.overload.as_str(),
        "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"
    );
    assert_eq!(
        selected.argument_types.as_slice(),
        &[FunctionArgumentType::Value(text)]
    );
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );

    for source in [
        FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(resolve_value_conversion(&source, &target, &Control).is_err());
    }
}
