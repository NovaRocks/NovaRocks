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

use super::compiled_program::CompiledExpressionInstance;
use super::filter_conjunction_actual_sql_compiler_tests::{compile_options, validate_package};
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields;
use super::numeric_unary_owned_transaction_tests::frozen_reads_with_explicit_source_shape;
use arrow::{
    array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, Selection,
};
use novarocks_local_compiler::compile_fragment;
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
};
use novarocks_physical_plan::{ExprKind, PhysicalCallDefinition, StaticFunctionArgument};
use novarocks_plan_codec::{
    physical_package_v2::{
        decode_fragment_package, encode_fragment_package,
        test_support::{decode_limits, encode_limits},
    },
    resource_preflight_v2::FragmentDecodeResourceModel,
};
use novarocks_sql::compiler::{
    SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode, author_fragment_package_semantics,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionArgumentType, FunctionValueType, PureCompileControl,
};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("signed array index never waits");
    }
}

fn source_packages(
    source: &SqlAuthoredPhysicalPlan,
    policy: novarocks_functions::ConstantPolicy,
) -> BTreeMap<novarocks_physical_plan::FragmentId, Arc<novarocks_physical_plan::FragmentPackage>> {
    use novarocks_physical_plan::{
        FragmentPackageAdmission, PlanLimits, PropertyProofProjectionLimits,
        extract_fragment_packages,
    };
    let semantics = author_fragment_package_semantics(source, policy, &Control).unwrap();
    let uses = semantics
        .iter()
        .map(|(&id, s)| (id, s.expression_uses.clone()))
        .collect();
    let calls = semantics
        .iter()
        .map(|(&id, s)| (id, s.calls.clone()))
        .collect();
    let pruning = semantics
        .iter()
        .map(|(&id, s)| (id, s.pruning.clone()))
        .collect();
    let admissions = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FragmentPackageAdmission {
                    plan_limits: PlanLimits::FROZEN,
                    source_retained_bytes: 64 << 20,
                    property_projection_limits: PropertyProofProjectionLimits {
                        max_request_bytes: 16 << 20,
                        max_coexisting_bytes: 256 << 20,
                        max_projection_work: 16 << 20,
                    },
                },
            )
        })
        .collect();
    extract_fragment_packages(
        source.plan(),
        &frozen_reads_with_explicit_source_shape(source, false, true),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap()
    .into_iter()
    .map(|(id, package)| (id, Arc::new(package)))
    .collect()
}

fn check_array_source(
    index_expression: &str,
    field: Field,
    input_values: ArrayRef,
    expected_index_source: FunctionValueType,
    expected_values: &[Option<i8>],
) {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let sql = format!("SELECT [10,20][{index_expression}] AS picked FROM t");
        let source = sql_source_with_declared_fields(&sql, &[field.clone()], mode);
        let declaration = source
            .checked_original_result_declaration_observed(&Control)
            .unwrap()
            .unwrap();
        assert_eq!(declaration.fields().len(), 1);
        assert_eq!(declaration.fields()[0].alias.as_deref(), Some("picked"));
        assert_eq!(
            declaration.fields()[0].ty,
            FunctionValueType::new(DataType::Int8, true)
        );
        let mut indexes = 0;
        for fragment in source.plan().fragments().values() {
            for (definition, expression) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &expression.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/__array_element_at/v1" {
                    continue;
                }
                let index = fragment.expressions().get(args[1]).unwrap();
                let expected = FunctionValueType::new(
                    DataType::Int32,
                    expected_index_source.nullable
                        || expected_index_source.data_type == DataType::Int64,
                );
                assert_eq!(index.ty, expected);
                assert_eq!(
                    function.argument_types[1],
                    FunctionArgumentType::Value(expected.clone())
                );
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Expression(*definition))
                    .unwrap();
                let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[1] else {
                    panic!("real index source");
                };
                assert_eq!(value_type, &expected);
                if expected_index_source.data_type == DataType::Int32 {
                    assert_eq!(index.ty, expected_index_source);
                } else {
                    let ExprKind::Cast { expr, target, .. } = &index.kind else {
                        panic!("implicit signed index cast");
                    };
                    assert_eq!(target, &DataType::Int32);
                    assert_eq!(
                        fragment.expressions().get(*expr).unwrap().ty,
                        expected_index_source
                    );
                }
                indexes += 1;
            }
        }
        assert_eq!(indexes, 1);
        let policy = novarocks_functions::ConstantPolicy {
            max_rows: 64,
            max_array_nodes: 1024,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        };
        let semantics = author_fragment_package_semantics(&source, policy, &Control);
        semantics.unwrap();
        let functions = installed_builtin_owner_catalogue();
        let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
        let mut producer = None;
        let mut failures = vec![];
        for package in source_packages(&source, policy).into_values() {
            let encoded = encode_fragment_package(&package, &encode_limits(), &Control)
                .unwrap()
                .encode_to_vec();
            let decoded = Arc::new(
                decode_fragment_package(&encoded, &model, &decode_limits(), &Control).unwrap(),
            );
            assert_eq!(
                encode_fragment_package(&decoded, &encode_limits(), &Control)
                    .unwrap()
                    .encode_to_vec(),
                encoded
            );
            for (id, original) in package.fragment().expressions().iter() {
                let roundtrip = decoded.fragment().expressions().get(*id).unwrap();
                assert_eq!(roundtrip.ty, original.ty);
                if let ExprKind::FunctionCall {
                    function: original, ..
                } = &original.kind
                {
                    let ExprKind::FunctionCall {
                        function: roundtrip,
                        ..
                    } = &roundtrip.kind
                    else {
                        panic!("wire retains function call");
                    };
                    assert_eq!(roundtrip.argument_types, original.argument_types);
                    assert_eq!(roundtrip.result_type, original.result_type);
                    assert!(roundtrip.legacy_metadata.is_none());
                }
            }
            for (id, original) in package.fragment().values() {
                assert_eq!(decoded.fragment().values().get(id).unwrap().ty, original.ty);
            }
            assert_eq!(
                format!("{:?}", decoded.fragment().call_requests()),
                format!("{:?}", package.fragment().call_requests())
            );
            let program = match compile_fragment(
                validate_package(decoded),
                &functions,
                compile_options(),
                &Control,
            ) {
                Ok(program) => Arc::new(program),
                Err(error) => {
                    failures.push(format!("{error:?}"));
                    continue;
                }
            };
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            for (site, _) in snapshot.bindings() {
                let ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::ProjectOutput { .. },
                } = site
                else {
                    continue;
                };
                let ProgramNodeKind::Project { input, .. } =
                    program.graph().nodes()[node.index()].kind()
                else {
                    continue;
                };
                assert!(producer.replace((program.clone(), *site, *input)).is_none());
            }
        }
        assert!(failures.is_empty());
        let (program, site, input) = producer.expect("real project producer");
        let layout = program.graph().nodes()[input.index()].output_layout();
        assert_eq!(layout.schema().fields().len(), 1);
        assert_eq!(layout.schema().field(0).data_type(), field.data_type());
        assert_eq!(layout.schema().field(0).is_nullable(), field.is_nullable());
        let batch = RecordBatch::try_new(
            layout.schema().clone(),
            vec![input_values.slice(1, expected_values.len())],
        )
        .unwrap();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("signed-index-actual-frame");
        tracker.install_limit_once(8 << 20).unwrap();
        let all: Vec<_> = (0..expected_values.len()).collect();
        let sparse: Vec<_> = (0..expected_values.len()).step_by(2).collect();
        for rows in [&all[..], &sparse[..], &[1][..], &[][..]] {
            let host = crate::exec::operators::compiled_aggregate::expression_allocation_host(
                tracker.clone(),
            );
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                program.clone(),
                site,
                &Control,
                Some(host),
            )
            .unwrap();
            let output = frame
                .evaluate_evaluation(
                    &batch,
                    Selection::try_sparse(expected_values.len(), rows).unwrap(),
                    ScalarInvocationActivation::Activated,
                    &Control,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            assert_eq!(output.values().data_type(), &DataType::Int8);
            let values = output
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                rows.iter()
                    .map(|&row| expected_values[row])
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn actual_primitive_if_index_retains_exact_type_and_overflow_is_successful_null() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![99, 1, 2, 3, 99]));
    for (expression, expected) in [
        ("IF(id=2,2,1)", vec![Some(10), Some(20), Some(10)]),
        ("IF(id=2,2147483648,1)", vec![Some(10), None, Some(10)]),
    ] {
        check_array_source(
            expression,
            Field::new("id", DataType::Int32, false),
            input.clone(),
            FunctionValueType::new(DataType::Int64, false),
            &expected,
        );
    }
}

#[test]
fn actual_stored_signed_indexes_keep_width_source_null_and_sparse_sliced_results() {
    for carrier in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for nullable in [false, true] {
            let mut values = vec![Some(1), Some(2), Some(0), Some(-1), Some(3)];
            if nullable {
                values.push(None);
            }
            let (min, max) = match carrier {
                DataType::Int8 => (i64::from(i8::MIN), i64::from(i8::MAX)),
                DataType::Int16 => (i64::from(i16::MIN), i64::from(i16::MAX)),
                DataType::Int32 => (i64::from(i32::MIN), i64::from(i32::MAX)),
                DataType::Int64 => (i64::MIN, i64::MAX),
                _ => unreachable!(),
            };
            values.extend([Some(min), Some(max)]);
            if carrier == DataType::Int64 {
                values.extend([
                    Some(2147483647),
                    Some(2147483648),
                    Some(-2147483648),
                    Some(-2147483649),
                ]);
            }
            let expected: Vec<_> = values
                .iter()
                .map(|v| match v {
                    Some(1) => Some(10),
                    Some(2) => Some(20),
                    _ => None,
                })
                .collect();
            values.insert(0, Some(99));
            values.push(Some(99));
            macro_rules! array {
                ($array:ty,$native:ty) => {
                    Arc::new(<$array>::from(
                        values
                            .iter()
                            .map(|value| value.map(|v| <$native>::try_from(v).unwrap()))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                };
            }
            let input = match carrier {
                DataType::Int8 => array!(Int8Array, i8),
                DataType::Int16 => array!(Int16Array, i16),
                DataType::Int32 => array!(Int32Array, i32),
                DataType::Int64 => array!(Int64Array, i64),
                _ => unreachable!(),
            };
            check_array_source(
                "k",
                Field::new("k", carrier.clone(), nullable),
                input,
                FunctionValueType::new(carrier.clone(), nullable),
                &expected,
            );
        }
    }
}

#[test]
fn actual_map_key_keeps_original_exact_binding_and_candidate_named_refusal() {
    use super::numeric_unary_original_nonnull_sql_baseline_tests::try_sql_source_with_declared_fields;
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let result = try_sql_source_with_declared_fields(
            "SELECT map{CAST(1 AS TINYINT):10,CAST(2 AS TINYINT):20}[k] AS picked FROM t",
            &[Field::new("k", DataType::Int64, false)],
            mode,
        );
        if mode == SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration {
            let failure = result.unwrap_err();
            let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
                panic!("actual FE named missing implementation refusal: {failure:?}");
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
            assert_eq!(
                error.message(),
                "selected overload `builtin.scalar/map/dynamic-v1` has no installed implementation for the selected carrier"
            );
            continue;
        }
        let source = result.unwrap();
        let mut found = false;
        for fragment in source.plan().fragments().values() {
            for (definition, expression) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &expression.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/__map_element_at/v1" {
                    continue;
                }
                let index = fragment.expressions().get(args[1]).unwrap();
                let ExprKind::Cast { expr, target, .. } = &index.kind else {
                    panic!("map implicit key cast");
                };
                assert_eq!(
                    fragment.expressions().get(*expr).unwrap().ty,
                    FunctionValueType::new(DataType::Int64, false)
                );
                assert_eq!(*target, DataType::Int8);
                assert_eq!(index.ty, FunctionValueType::new(DataType::Int8, true));
                assert_eq!(
                    function.argument_types[1],
                    FunctionArgumentType::Value(index.ty.clone())
                );
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Expression(*definition))
                    .unwrap();
                let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[1] else {
                    panic!("real MAP key request");
                };
                assert_eq!(value_type, &index.ty);
                found = true;
            }
        }
        assert!(found, "actual original MAP key binding");
    }
}
