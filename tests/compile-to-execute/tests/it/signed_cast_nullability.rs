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

use arrow::{
    array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_execution::exec::expr::compiled_program::CompiledExpressionInstance;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, Selection,
};
use novarocks_local_compiler::compile_fragment;
use novarocks_local_program::ProgramNodeKind;
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_plan_codec::{
    physical_package_v2::{
        decode_fragment_package, encode_fragment_package,
        test_support::{decode_limits, encode_limits},
    },
    resource_preflight_v2::FragmentDecodeResourceModel,
};
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_sql_testkit::installed_builtin_owner_catalogue;
use novarocks_sql_testkit::sql_source_with_declared_fields;
use novarocks_test_compile_to_execute::compiler_results::{compile_options, validate_package};
use novarocks_test_compile_to_execute::programs::frozen_reads_with_explicit_source_shape;
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
        FragmentPackageAdmission, FrozenFragmentPruning, PlanLimits, PropertyProofProjectionLimits,
        extract_fragment_packages,
    };
    source.require_statement_constant_policy(policy).unwrap();
    let pruning = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
            )
        })
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
    expected_values: &[Option<i32>],
) {
    {
        let sql = format!("SELECT [10,20][{index_expression}] AS picked FROM t");
        let source = sql_source_with_declared_fields(&sql, std::slice::from_ref(&field));
        let declaration = source
            .checked_original_result_declaration_observed(&Control)
            .unwrap()
            .unwrap();
        assert_eq!(declaration.fields().len(), 1);
        assert_eq!(declaration.fields()[0].alias.as_deref(), Some("picked"));
        assert_eq!(
            declaration.fields()[0].ty,
            FunctionValueType::new(DataType::Int32, true)
        );
        let mut indexes = 0;
        for unit in source
            .plan()
            .fragments()
            .values()
            .flat_map(|f| f.nodes().values())
            .filter_map(|node| node.expressions.as_deref())
        {
            for expression in unit.definitions() {
                let ExprKind::FunctionCall { function, args } = &expression.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/__array_element_at/v1" {
                    continue;
                }
                let index = unit.definition(args[1]).unwrap();
                let expected = FunctionValueType::new(
                    DataType::Int32,
                    expected_index_source.nullable()
                        || expected_index_source.data_type() == &DataType::Int64,
                );
                assert_eq!(index.ty, expected);
                assert_eq!(
                    function.argument_types[1],
                    FunctionArgumentType::Value(expected.clone())
                );
                let request = unit.requests().get(&expression.id).unwrap();
                let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[1] else {
                    panic!("real index source");
                };
                assert_eq!(value_type, &expected);
                if expected_index_source.data_type() == &DataType::Int32 {
                    assert_eq!(index.ty, expected_index_source);
                } else {
                    let ExprKind::Cast { expr, target, .. } = &index.kind else {
                        panic!("implicit signed index cast");
                    };
                    assert_eq!(target, &DataType::Int32);
                    assert_eq!(unit.definition(*expr).unwrap().ty, expected_index_source);
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
        source.require_statement_constant_policy(policy).unwrap();
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
            for node in package.fragment().nodes().values() {
                let Some(unit) = node.expressions.as_deref() else {
                    continue;
                };
                let decoded_unit = decoded.fragment().unit(node.id).unwrap();
                assert_eq!(decoded_unit.requests(), unit.requests());
                for original in unit.definitions() {
                    let roundtrip = decoded_unit.definition(original.id).unwrap();
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
            }
            for (id, original) in package.fragment().values() {
                assert_eq!(decoded.fragment().values().get(id).unwrap().ty, original.ty);
            }
            assert_eq!(
                decoded.fragment().call_requests(),
                package.fragment().call_requests()
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
            for node in program.graph().nodes() {
                let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                    continue;
                };
                for site in exprs {
                    assert!(producer.replace((program.clone(), *site, *input)).is_none());
                }
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
        let allocator = Arc::new(super::fixtures::CountingAllocator::new(8 << 20));
        let all: Vec<_> = (0..expected_values.len()).collect();
        let sparse: Vec<_> = (0..expected_values.len()).step_by(2).collect();
        for rows in [&all[..], &sparse[..], &[1][..], &[][..]] {
            let host = allocator.clone();
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                program.unit(site.unit).unwrap().clone(),
                site.root,
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
            assert_eq!(output.values().data_type(), &DataType::Int32);
            let values = output
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                rows.iter()
                    .map(|&row| expected_values[row])
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(allocator.outstanding(), 0);
    }
}

#[test]
fn actual_primitive_if_index_retains_exact_type_and_overflow_is_successful_null() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![99, 1, 2, 3, 99]));
    for (expression, index_type, expected) in [
        (
            "IF(id=2,2,1)",
            DataType::Int32,
            vec![Some(10), Some(20), Some(10)],
        ),
        (
            "IF(id=2,2147483648,1)",
            DataType::Int64,
            vec![Some(10), None, Some(10)],
        ),
    ] {
        check_array_source(
            expression,
            Field::new("id", DataType::Int32, false),
            input.clone(),
            FunctionValueType::new(index_type, false),
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
