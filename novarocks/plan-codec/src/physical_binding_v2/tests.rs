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

use super::*;
use crate::physical_type_v2::{TypeProjectionLimits, encode_type_table_sources};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{
    FunctionArgumentEvaluation, FunctionArgumentType, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionValueType,
    FunctionVolatility, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    ValueLogicalType,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const SOURCE: usize = 64 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1024,
        max_type_references: 4096,
        max_request_bytes: 1024 * 1024,
        max_allocation_requests: 4096,
        max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
        max_work: 128 * 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    }
}
fn scalar(
    kind: FunctionKind,
    arguments: Vec<FunctionArgumentType>,
    result: FunctionValueType,
) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("builtin/test-signature/v1").unwrap(),
        overload: FunctionOverloadId::try_new("builtin/test-signature/exact-v1").unwrap(),
        kind,
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            arguments.into_boxed_slice(),
        ),
        result_type: result,

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::new([]),
        }),
    }
}
fn table(
    arguments: Vec<FunctionArgumentType>,
    results: Vec<FunctionValueType>,
) -> BoundTableFunction {
    BoundTableFunction {
        function_id: FunctionId::try_new("builtin/table-signature/v1").unwrap(),
        overload: FunctionOverloadId::try_new("builtin/table-signature/exact-v1").unwrap(),
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            arguments.into_boxed_slice(),
        ),
        result_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
            results.into_boxed_slice(),
        ),

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::new([]),
        }),
    }
}
fn check_prefix<T>(call: impl Fn(&Control) -> Result<T, BindingCodecError>, success: bool) {
    let good = Control::default();
    assert_eq!(call(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(BindingCodecError::Control(actual)) if actual == cause),
                "refusal {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
fn finish<T>(
    control: &Control,
    call: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, BindingCodecError>,
) -> Result<T, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = call(&mut work);
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn exact_signature_wire_covers_all_kinds_lambda_relation_and_sparse_namespaces() {
    let int = FunctionValueType::new(DataType::Int64, false);
    let text = FunctionValueType::new(DataType::Utf8, true);
    let values = [(0, int.clone()), (u32::MAX, text.clone())];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let a = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(text.clone())],
        int.clone(),
    );
    let b = scalar(
        FunctionKind::Aggregate,
        vec![FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                vec![int.clone(), text.clone()].into_boxed_slice(),
            ),
            result_type: int.clone(),
        }],
        text.clone(),
    );
    let c = scalar(FunctionKind::Window, vec![], int.clone());
    let d = table(
        vec![FunctionArgumentType::Value(int.clone())],
        vec![int, text],
    );
    let lambda_parameters = [0, u32::MAX];
    let relation = [0, u32::MAX];
    let aa = [ArgumentTypeIds::Value(u32::MAX)];
    let ba = [ArgumentTypeIds::Lambda {
        parameters: &lambda_parameters,
        result: 0,
    }];
    let da = [ArgumentTypeIds::Value(0)];
    let inputs = [
        FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&a),
            arguments: &aa,
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: 5,
            source: BindingSource::Scalar(&b),
            arguments: &ba,
            result: ResultTypeIds::Scalar(u32::MAX),
        },
        FunctionBindingInput {
            id: 9,
            source: BindingSource::Scalar(&c),
            arguments: &[],
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: u32::MAX,
            source: BindingSource::Table(&d),
            arguments: &da,
            result: ResultTypeIds::Relation(&relation),
        },
    ];
    let token =
        encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default()).unwrap();
    let expected = vec![
        wire::FunctionBindingDefinition {
            id: 0,
            function_id: "builtin/test-signature/v1".into(),
            overload_id: "builtin/test-signature/exact-v1".into(),
            kind: wire::FunctionKind::Scalar as i32,
            arguments: vec![wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::ValueTypeId(u32::MAX)),
            }],
            result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(0)),
        },
        wire::FunctionBindingDefinition {
            id: 5,
            function_id: "builtin/test-signature/v1".into(),
            overload_id: "builtin/test-signature/exact-v1".into(),
            kind: wire::FunctionKind::Aggregate as i32,
            arguments: vec![wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::Lambda(
                    wire::LambdaArgumentType {
                        parameter_value_type_ids: vec![0, u32::MAX],
                        result_value_type_id: Some(0),
                    },
                )),
            }],
            result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(u32::MAX)),
        },
        wire::FunctionBindingDefinition {
            id: 9,
            function_id: "builtin/test-signature/v1".into(),
            overload_id: "builtin/test-signature/exact-v1".into(),
            kind: wire::FunctionKind::Window as i32,
            arguments: vec![],
            result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(0)),
        },
        wire::FunctionBindingDefinition {
            id: u32::MAX,
            function_id: "builtin/table-signature/v1".into(),
            overload_id: "builtin/table-signature/exact-v1".into(),
            kind: wire::FunctionKind::Table as i32,
            arguments: vec![wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::ValueTypeId(0)),
            }],
            result: Some(wire::function_binding_definition::Result::Relation(
                wire::RelationResultTypes {
                    value_type_ids: vec![0, u32::MAX],
                },
            )),
        },
    ];
    assert_eq!(token.as_wire(), expected);
    assert_eq!(token.facts().definition_count, 4);
    assert_eq!(token.facts().type_reference_count, 10);
    assert!(std::ptr::eq(token.type_sources(), &types));
    assert_eq!(token.source_counts(), 4);
    assert!(
        finish(&Control::default(), |work| token
            .scalar_binding_observed(5, work)
            .map(|v| std::ptr::eq(v.unwrap(), &b)))
        .unwrap()
    );
    assert!(
        finish(&Control::default(), |work| token
            .table_binding_observed(u32::MAX, work)
            .map(|v| std::ptr::eq(v.unwrap(), &d)))
        .unwrap()
    );
    assert!(
        finish(&Control::default(), |work| token
            .scalar_binding_observed(u32::MAX, work))
        .unwrap()
        .is_none()
    );
    assert_eq!(token.into_wire(), expected);
}

#[test]
fn binding_shape_missing_ids_and_full_type_disagreements_refuse() {
    let int = FunctionValueType::new(DataType::Int64, true);
    let nested = |value: &str| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(
                    Field::new(value, DataType::Int64, true)
                        .with_metadata(HashMap::from([("id".into(), value.into())])),
                )]
                .into(),
            ),
            true,
        )
    };
    let values = [
        (0, int.clone()),
        (3, nested("original")),
        (
            4,
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        ),
    ];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let args = [ArgumentTypeIds::Value(0)];
    for source_type in [
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        let source = scalar(
            FunctionKind::Scalar,
            vec![FunctionArgumentType::Value(source_type)],
            int.clone(),
        );
        let inputs = [FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&source),
            arguments: &args,
            result: ResultTypeIds::Scalar(0),
        }];
        assert!(
            encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default())
                .is_err()
        );
    }
    let source = scalar(FunctionKind::Scalar, vec![], nested("different"));
    for result in [
        ResultTypeIds::Scalar(3),
        ResultTypeIds::Scalar(u32::MAX),
        ResultTypeIds::Relation(&[]),
    ] {
        let inputs = [FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&source),
            arguments: &[],
            result,
        }];
        assert!(
            encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default())
                .is_err()
        );
    }
    let source = scalar(FunctionKind::Table, vec![], int);
    let inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&source),
        arguments: &[],
        result: ResultTypeIds::Scalar(0),
    }];
    assert!(
        encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default()).is_err()
    );
    let source = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        )],
        values[0].1.clone(),
    );
    let ids = [ArgumentTypeIds::Value(4)];
    let inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&source),
        arguments: &ids,
        result: ResultTypeIds::Scalar(0),
    }];
    assert!(
        encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default()).is_err()
    );
    let wrong_shape = [ArgumentTypeIds::Lambda {
        parameters: &[],
        result: 0,
    }];
    let inputs = [FunctionBindingInput {
        arguments: &wrong_shape,
        ..inputs[0]
    }];
    assert!(
        encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default()).is_err()
    );
}

#[test]
fn binding_limits_are_independent_and_exact_boundaries_are_admitted() {
    assert!(matches!(
        encode::add(usize::MAX, 1),
        Err(BindingCodecError::InvalidShape(_))
    ));
    let values = [(0, FunctionValueType::new(DataType::Int64, true))];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let source = scalar(FunctionKind::Scalar, vec![], values[0].1.clone());
    let inputs = [FunctionBindingInput {
        id: u32::MAX,
        source: BindingSource::Scalar(&source),
        arguments: &[],
        result: ResultTypeIds::Scalar(0),
    }];
    let facts = *encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default())
        .unwrap()
        .facts();
    assert_eq!(facts.allocation_requests_upper_bound, 3);
    assert_eq!(
        facts.request_bytes_upper_bound,
        std::mem::size_of::<wire::FunctionBindingDefinition>()
            + "builtin/test-signature/v1".len()
            + "builtin/test-signature/exact-v1".len()
    );
    let exact = BindingProjectionLimits {
        max_definitions: 1,
        max_type_references: 1,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(encode_function_bindings(&types, &inputs, SOURCE, exact, &Control::default()).is_ok());
    for component in 0..6 {
        let mut narrow = exact;
        match component {
            0 => narrow.max_definitions -= 1,
            1 => narrow.max_type_references -= 1,
            2 => narrow.max_request_bytes -= 1,
            3 => narrow.max_allocation_requests -= 1,
            4 => narrow.max_coexisting_source_and_request_bytes -= 1,
            _ => narrow.max_work -= 1,
        }
        assert!(
            encode_function_bindings(&types, &inputs, SOURCE, narrow, &Control::default()).is_err()
        );
    }
    assert!(encode_function_bindings(&types, &inputs, 0, limits(), &Control::default()).is_err());
    let source_floor = std::mem::size_of::<FunctionBindingInput<'_>>()
        + std::mem::size_of::<BoundFunction>()
        + source.function_id.as_str().len()
        + source.overload.as_str().len();
    assert!(matches!(
        encode_function_bindings(
            &types,
            &inputs,
            source_floor - 1,
            limits(),
            &Control::default()
        ),
        Err(BindingCodecError::InvalidShape(
            "binding source invoice omits original signature backing"
        ))
    ));
    let repeated = [inputs[0], inputs[0]];
    assert!(
        encode_function_bindings(&types, &repeated, SOURCE, limits(), &Control::default()).is_err()
    );
}

#[test]
fn signature_comparison_ignores_five_legacy_fields_but_keeps_full_ordered_contract() {
    let int = FunctionValueType::new(DataType::Int64, true);
    let source = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(int.clone())],
        int.clone(),
    );
    let mut legacy = source.clone();
    legacy.legacy_metadata.as_mut().unwrap().volatility = FunctionVolatility::Volatile;
    legacy.legacy_metadata.as_mut().unwrap().argument_evaluation =
        FunctionArgumentEvaluation::ShortCircuit;
    legacy.legacy_metadata.as_mut().unwrap().failure_behavior =
        FunctionFailureBehavior::ReturnsNull;
    legacy.legacy_metadata.as_mut().unwrap().intrinsic_row_error =
        FunctionIntrinsicRowError::MayRaise;
    legacy.legacy_metadata.as_mut().unwrap().semantic_parameters = vec![SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    }]
    .into_boxed_slice();
    assert!(
        finish(&Control::default(), |work| verify_scalar_signature(
            &source,
            &legacy,
            SOURCE,
            limits().max_work,
            work
        ))
        .unwrap()
        .matches()
    );
    for change in 0..4 {
        let mut other = legacy.clone();
        match change {
            0 => other.overload = FunctionOverloadId::try_new("different").unwrap(),
            1 => other.kind = FunctionKind::Window,
            2 => {
                other.argument_types =
                    novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                        vec![FunctionArgumentType::Lambda {
                            parameter_types:
                                novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                    Box::new([]),
                                ),
                            result_type: int.clone(),
                        }]
                        .into_boxed_slice(),
                    )
            }
            _ => other.result_type = other.result_type.with_nullable(false).unwrap(),
        }
        assert!(
            !finish(&Control::default(), |work| verify_scalar_signature(
                &source,
                &other,
                SOURCE,
                limits().max_work,
                work
            ))
            .unwrap()
            .matches()
        );
    }
    let same = finish(&Control::default(), |work| {
        verify_scalar_signature(&source, &source, SOURCE, 1, work)
    })
    .unwrap();
    assert!(same.matches());
    assert_eq!(same.work_upper_bound(), 1);
    check_prefix(
        |control| {
            finish(control, |work| {
                verify_scalar_signature(&source, &legacy, SOURCE, limits().max_work, work)
            })
        },
        true,
    );
}

#[test]
fn actual_binding_encode_every_boundary_preserves_three_causes_and_ordinary_tail() {
    let values = [(0, FunctionValueType::new(DataType::Int64, true))];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let source = scalar(FunctionKind::Scalar, vec![], values[0].1.clone());
    let inputs: Vec<_> = (0..320)
        .map(|id| FunctionBindingInput {
            id,
            source: BindingSource::Scalar(&source),
            arguments: &[],
            result: ResultTypeIds::Scalar(0),
        })
        .collect();
    let good = Control::default();
    let result = encode_function_bindings(&types, &inputs, SOURCE, limits(), &good).unwrap();
    assert_eq!(result.as_wire().len(), 320);
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    check_prefix(
        |control| {
            encode_function_bindings(&types, &inputs, SOURCE, limits(), control)
                .map(|value| value.into_wire())
        },
        true,
    );
    let bad = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&source),
        arguments: &[],
        result: ResultTypeIds::Scalar(u32::MAX),
    }];
    check_prefix(
        |control| {
            encode_function_bindings(&types, &bad, SOURCE, limits(), control)
                .map(|value| value.into_wire())
        },
        false,
    );
}

#[test]
fn actual_source_lookup_observes_long_roots_missing_ids_and_exact_original_owner() {
    let values = [(0, FunctionValueType::new(DataType::Int64, true))];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let source = scalar(FunctionKind::Scalar, vec![], values[0].1.clone());
    let inputs: Vec<_> = (0..320)
        .map(|id| FunctionBindingInput {
            id,
            source: BindingSource::Scalar(&source),
            arguments: &[],
            result: ResultTypeIds::Scalar(0),
        })
        .collect();
    let token =
        encode_function_bindings(&types, &inputs, SOURCE, limits(), &Control::default()).unwrap();
    for id in [319, u32::MAX] {
        let call = |control: &Control| {
            finish(control, |work| {
                let found = token.scalar_binding_observed(id, work)?;
                if id == 319 {
                    assert!(std::ptr::eq(found.unwrap(), &source));
                }
                found.ok_or(BindingCodecError::InvalidShape("test binding absent"))
            })
        };
        check_prefix(call, id == 319);
    }
    let empty =
        encode_function_bindings(&types, &[], SOURCE, limits(), &Control::default()).unwrap();
    assert!(empty.as_wire().is_empty());
    assert_eq!(empty.source_counts(), 0);
    assert!(
        finish(&Control::default(), |work| empty
            .scalar_binding_observed(0, work))
        .unwrap()
        .is_none()
    );
}

#[test]
fn table_source_identity_uses_actual_sparse_aliases_and_rejects_equal_foreign_owners() {
    let values = [(0, FunctionValueType::new(DataType::Int64, true))];
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let source = table(vec![], vec![values[0].1.clone()]);
    let foreign = source.clone();
    let result = [0];
    let aliases = [
        FunctionBindingInput {
            id: 0,
            source: BindingSource::Table(&source),
            arguments: &[],
            result: ResultTypeIds::Relation(&result),
        },
        FunctionBindingInput {
            id: u32::MAX,
            source: BindingSource::Table(&source),
            arguments: &[],
            result: ResultTypeIds::Relation(&result),
        },
    ];
    let encoded =
        encode_function_bindings(&types, &aliases, SOURCE, limits(), &Control::default()).unwrap();
    check_prefix(
        |control| {
            finish(control, |work| {
                let id = encoded.table_source_id_observed(&source, work)?;
                assert_eq!(id, 0);
                assert!(std::ptr::eq(
                    encoded.table_binding_observed(id, work)?.unwrap(),
                    &source
                ));
                Ok(id)
            })
        },
        true,
    );
    check_prefix(
        |control| {
            finish(control, |work| {
                encoded.table_source_id_observed(&foreign, work)
            })
        },
        false,
    );
    let last = &aliases[1..];
    let encoded =
        encode_function_bindings(&types, last, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(
        finish(&Control::default(), |work| encoded
            .table_source_id_observed(&source, work))
        .unwrap(),
        u32::MAX
    );
    let encoded =
        encode_function_bindings(&types, &[], SOURCE, limits(), &Control::default()).unwrap();
    assert!(matches!(
        finish(&Control::default(), |work| encoded
            .table_source_id_observed(&source, work)),
        Err(BindingCodecError::InvalidShape(_))
    ));
}
