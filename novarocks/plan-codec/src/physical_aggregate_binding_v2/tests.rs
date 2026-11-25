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
use crate::{
    physical_binding_v2::{
        ArgumentTypeIds, BindingSource, FunctionBindingInput, ResultTypeIds,
        encode_function_bindings,
    },
    physical_type_v2::{TypeProjectionLimits, encode_type_table_sources},
};
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::AggregateSequenceId;
use novarocks_type_contract::{
    AggregateStateFormatId, CompileControlError, FunctionArgumentEvaluation, FunctionArgumentType,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionOverloadId,
    FunctionValueType, FunctionVolatility, ValueLogicalType,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const SOURCE: usize = 65536;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
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
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1024,
        max_type_references: 1024,
        max_request_bytes: 1 << 20,
        max_allocation_requests: 2048,
        max_coexisting_source_and_request_bytes: 1 << 21,
        max_work: 1 << 30,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    }
}
fn function(value: &FunctionValueType) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("test.aggregate/full-signature").unwrap(),
        overload: FunctionOverloadId::try_new("test.aggregate/full-signature/one").unwrap(),
        kind: FunctionKind::Aggregate,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(value.clone()),
        ])),
        result_type: value.clone(),

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            semantic_parameters: Box::default(),
        }),
    }
}
fn aggregate(
    function: &BoundFunction,
    intermediate: &FunctionValueType,
    phase: AggregatePhase,
) -> AggregateBinding {
    AggregateBinding {
        state_interpretation: None,
        function: function.clone(),
        phase,
        logical_argument_count: 1,
        intermediate_type: intermediate.clone(),
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        state_format: AggregateStateFormatId::try_new("test/state-identity-v1").unwrap(),
    }
}
fn values() -> [(u32, FunctionValueType); 2] {
    [
        (0, FunctionValueType::new(DataType::Int64, true)),
        (u32::MAX, FunctionValueType::new(DataType::Binary, false)),
    ]
}

#[test]
fn aggregate_emission_preserves_all_phases_sparse_ids_and_original_source_loans() {
    let values = values();
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let function = function(&values[0].1);
    let args = [ArgumentTypeIds::Value(0)];
    let functions_input = [FunctionBindingInput {
        id: u32::MAX,
        source: BindingSource::Scalar(&function),
        arguments: &args,
        result: ResultTypeIds::Scalar(0),
    }];
    let functions = encode_function_bindings(
        &types,
        &functions_input,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    let phases = [
        AggregatePhase::Single,
        AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(0),
        },
        AggregatePhase::Intermediate {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
        AggregatePhase::Final {
            sequence: AggregateSequenceId::new(0),
        },
    ];
    let mut sources = phases.map(|phase| aggregate(&function, &values[1].1, phase));
    for source in sources.iter_mut().skip(1).step_by(2) {
        source.state_argument_contract =
            AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    }
    let inputs = [0, 1, 2, u32::MAX]
        .into_iter()
        .zip(&sources)
        .map(|(id, source)| AggregateBindingInput {
            id,
            source,
            function_binding_id: u32::MAX,
            intermediate_value_type_id: u32::MAX,
        })
        .collect::<Vec<_>>();
    let encoded = encode_aggregate_bindings(
        &types,
        &functions,
        &inputs,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    let kinds = [
        wire::aggregate_phase::Kind::Single(physical_control_v2::Empty {}),
        wire::aggregate_phase::Kind::PartialSequenceId(0),
        wire::aggregate_phase::Kind::IntermediateSequenceId(u32::MAX),
        wire::aggregate_phase::Kind::FinalSequenceId(0),
    ];
    for (ordinal, ((definition, input), kind)) in
        encoded.as_wire().iter().zip(&inputs).zip(kinds).enumerate()
    {
        assert_eq!(
            *definition,
            wire::AggregateBindingDefinition {
                state_interpretation: None,
                id: input.id,
                function_binding_id: Some(u32::MAX),
                phase: Some(wire::AggregatePhase { kind: Some(kind) }),
                logical_argument_count: 1,
                intermediate_value_type_id: Some(u32::MAX),
                state_format: "test/state-identity-v1".into(),
                state_argument_contract: if ordinal % 2 == 0 { 1 } else { 2 },
            }
        );
    }
    assert_eq!(encoded.source_counts(), 4);
    assert!(std::ptr::eq(encoded.type_sources(), &types));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(std::ptr::eq(
        encoded
            .binding_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        &sources[3]
    ));
    assert!(encoded.binding_observed(8, &mut work).unwrap().is_none());
    work.finish().unwrap();
    assert_eq!(encoded.into_wire().len(), 4);
}

#[test]
fn aggregate_signature_checks_complete_phase_format_function_and_nested_type() {
    let ty = FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new("original", DataType::Int64, false)
                    .with_metadata(HashMap::from([("source.identity".into(), "kept".into())])),
            )]
            .into(),
        ),
        false,
    );
    let function = function(&ty);
    let left = aggregate(
        &function,
        &ty,
        AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(0),
        },
    );
    // Compare the actual emitted definition namespaces. The old standalone
    // aggregate comparer belonged to the retired scalar Window bank; current
    // bindings are associated by their actual source, and their full data must
    // still survive the sole type/function/aggregate codec authors.
    let project = |binding: &AggregateBinding| {
        let FunctionArgumentType::Value(argument) = &binding.function.argument_types[0] else {
            panic!("fixed value argument");
        };
        let roots = [
            (0, argument.clone()),
            (1, binding.function.result_type.clone()),
            (2, binding.intermediate_type.clone()),
        ];
        let control = Control::default();
        let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
        let args = [ArgumentTypeIds::Value(0)];
        let inputs = [FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&binding.function),
            arguments: &args,
            result: ResultTypeIds::Scalar(1),
        }];
        let functions =
            encode_function_bindings(&types, &inputs, SOURCE, limits(), &control).unwrap();
        let inputs = [AggregateBindingInput {
            id: 0,
            source: binding,
            function_binding_id: 0,
            intermediate_value_type_id: 2,
        }];
        let aggregate =
            encode_aggregate_bindings(&types, &functions, &inputs, SOURCE, limits(), &control)
                .unwrap();
        (
            types.as_wire().clone(),
            functions.as_wire().to_vec(),
            aggregate.as_wire().to_vec(),
        )
    };
    let expected = project(&left);
    let compare = |right: &AggregateBinding| project(right) == expected;
    let mut right = left.clone();
    // Legacy occurrence projections are deliberately outside the definition signature.
    right.function.legacy_metadata.as_mut().unwrap().volatility = FunctionVolatility::Volatile;
    assert!(compare(&right));
    right.phase = AggregatePhase::Final {
        sequence: AggregateSequenceId::new(0),
    };
    assert!(!compare(&right));
    right = left.clone();
    right.state_argument_contract = AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    assert!(!compare(&right));
    right = left.clone();
    right.logical_argument_count = 0;
    assert!(!compare(&right));
    right = left.clone();
    right.state_format = AggregateStateFormatId::try_new("test/other-v1").unwrap();
    assert!(!compare(&right));
    right = left.clone();
    right.function.overload = FunctionOverloadId::try_new("test/other-overload").unwrap();
    assert!(!compare(&right));
    right = left.clone();
    right.intermediate_type = right.intermediate_type.with_nullable(true).unwrap();
    assert!(!compare(&right));
    right = left.clone();
    right.intermediate_type = FunctionValueType::new(
        DataType::Struct(vec![Arc::new(Field::new("changed", DataType::Int64, false))].into()),
        right.intermediate_type.nullable(),
    );
    assert!(!compare(&right));
    right = left.clone();
    right.function.result_type = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    assert!(!compare(&right));
}

#[test]
fn aggregate_refuses_foreign_type_emission_missing_ids_and_wrong_source_facts() {
    let values = values();
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let foreign =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let function = function(&values[0].1);
    let args = [ArgumentTypeIds::Value(0)];
    let functions_input = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&function),
        arguments: &args,
        result: ResultTypeIds::Scalar(0),
    }];
    let functions = encode_function_bindings(
        &types,
        &functions_input,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    let source = aggregate(&function, &values[1].1, AggregatePhase::Single);
    let base = AggregateBindingInput {
        id: 0,
        source: &source,
        function_binding_id: 0,
        intermediate_value_type_id: u32::MAX,
    };
    assert!(
        encode_aggregate_bindings(
            &foreign,
            &functions,
            &[base],
            SOURCE,
            limits(),
            &Control::default()
        )
        .is_err()
    );
    for input in [
        AggregateBindingInput {
            function_binding_id: 8,
            ..base
        },
        AggregateBindingInput {
            intermediate_value_type_id: 8,
            ..base
        },
        AggregateBindingInput {
            intermediate_value_type_id: 0,
            ..base
        },
    ] {
        assert!(
            encode_aggregate_bindings(
                &types,
                &functions,
                &[input],
                SOURCE,
                limits(),
                &Control::default()
            )
            .is_err()
        );
    }
    assert!(
        encode_aggregate_bindings(
            &types,
            &functions,
            &[base, base],
            SOURCE,
            limits(),
            &Control::default()
        )
        .is_err()
    );
    let mut wrong = source.clone();
    wrong.function.kind = FunctionKind::Scalar;
    assert!(
        encode_aggregate_bindings(
            &types,
            &functions,
            &[AggregateBindingInput {
                source: &wrong,
                ..base
            }],
            SOURCE,
            limits(),
            &Control::default()
        )
        .is_err()
    );
    let mut wrong = source.clone();
    wrong.function.result_type = wrong.function.result_type.with_nullable(false).unwrap();
    assert!(
        encode_aggregate_bindings(
            &types,
            &functions,
            &[AggregateBindingInput {
                source: &wrong,
                ..base
            }],
            SOURCE,
            limits(),
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn aggregate_exact_resource_envelopes_and_checked_arithmetic_refuse_underbounds() {
    let values = values();
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let function = function(&values[0].1);
    let args = [ArgumentTypeIds::Value(0)];
    let function_inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&function),
        arguments: &args,
        result: ResultTypeIds::Scalar(0),
    }];
    let functions = encode_function_bindings(
        &types,
        &function_inputs,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    let source = aggregate(&function, &values[1].1, AggregatePhase::Single);
    let inputs = [AggregateBindingInput {
        id: 0,
        source: &source,
        function_binding_id: 0,
        intermediate_value_type_id: u32::MAX,
    }];
    let facts = *encode_aggregate_bindings(
        &types,
        &functions,
        &inputs,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap()
    .facts();
    let exact = BindingProjectionLimits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(
        encode_aggregate_bindings(
            &types,
            &functions,
            &inputs,
            SOURCE,
            exact,
            &Control::default()
        )
        .is_ok()
    );
    for cap in [
        BindingProjectionLimits {
            max_definitions: 0,
            ..exact
        },
        BindingProjectionLimits {
            max_type_references: 0,
            ..exact
        },
        BindingProjectionLimits {
            max_request_bytes: exact.max_request_bytes - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_allocation_requests: exact.max_allocation_requests - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_coexisting_source_and_request_bytes: exact.max_coexisting_source_and_request_bytes
                - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_work: exact.max_work - 1,
            ..exact
        },
    ] {
        assert!(
            encode_aggregate_bindings(
                &types,
                &functions,
                &inputs,
                SOURCE,
                cap,
                &Control::default()
            )
            .is_err()
        );
    }
    assert!(
        encode_aggregate_bindings(
            &types,
            &functions,
            &inputs,
            0,
            limits(),
            &Control::default()
        )
        .is_err()
    );
    assert!(add(usize::MAX, 1).is_err());
    assert!(bytes::<[u8; 2]>(usize::MAX).is_err());
    assert!(bytes::<wire::AggregateBindingDefinition>(usize::MAX).is_err());
}

#[test]
fn aggregate_original_control_prefixes_cover_quantum_success_and_ordinary_tails() {
    let values = values();
    let types =
        encode_type_table_sources(&values, &[], type_limits(), &Control::default()).unwrap();
    let function = function(&values[0].1);
    let args = [ArgumentTypeIds::Value(0)];
    let function_inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&function),
        arguments: &args,
        result: ResultTypeIds::Scalar(0),
    }];
    let functions = encode_function_bindings(
        &types,
        &function_inputs,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    let source = aggregate(&function, &values[1].1, AggregatePhase::Single);
    let inputs = (0..320)
        .map(|id| AggregateBindingInput {
            id,
            source: &source,
            function_binding_id: 0,
            intermediate_value_type_id: u32::MAX,
        })
        .collect::<Vec<_>>();
    let ordinary = [AggregateBindingInput {
        function_binding_id: 8,
        ..inputs[0]
    }];
    let mut independent_source = source.clone();
    independent_source.state_argument_contract =
        AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    let independent = [AggregateBindingInput {
        source: &independent_source,
        ..inputs[0]
    }];
    for group in [&inputs[..1], &independent[..], &ordinary[..], &[][..]] {
        let success = Control::default();
        let result =
            encode_aggregate_bindings(&types, &functions, group, SOURCE, limits(), &success);
        if std::ptr::eq(group.as_ptr(), ordinary.as_ptr()) {
            assert!(matches!(result, Err(BindingCodecError::InvalidShape(_))));
        } else {
            assert!(result.is_ok());
        }
        let trace = success.trace.lock().unwrap().clone();
        for cause in CAUSES {
            for at in 0..trace.len() {
                let control = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(encode_aggregate_bindings(&types, &functions, group, SOURCE, limits(), &control),
                    Err(BindingCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    // A real large namespace reaches the work quantum. Exhaustive prefixes
    // above use the small success/error paths; here each quantum is refused.
    let success = Control::default();
    encode_aggregate_bindings(&types, &functions, &inputs, SOURCE, limits(), &success).unwrap();
    let trace = success.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for (at, units) in trace.iter().enumerate().filter(|(_, units)| **units == 256) {
        assert_eq!(*units, 256);
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(encode_aggregate_bindings(&types, &functions, &inputs, SOURCE, limits(), &control),
                Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn aggregate_source_identity_uses_original_pointer_and_first_sparse_alias() {
    let values = values();
    let control = Control::default();
    let types = encode_type_table_sources(&values, &[], type_limits(), &control).unwrap();
    let function = function(&values[0].1);
    let args = [ArgumentTypeIds::Value(0)];
    let function_inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&function),
        arguments: &args,
        result: ResultTypeIds::Scalar(0),
    }];
    let functions =
        encode_function_bindings(&types, &function_inputs, SOURCE, limits(), &control).unwrap();
    let source = aggregate(&function, &values[1].1, AggregatePhase::Single);
    let inputs = [0, u32::MAX].map(|id| AggregateBindingInput {
        id,
        source: &source,
        function_binding_id: 0,
        intermediate_value_type_id: u32::MAX,
    });
    let encoded =
        encode_aggregate_bindings(&types, &functions, &inputs, SOURCE, limits(), &control).unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert_eq!(encoded.source_id_observed(&source, &mut work).unwrap(), 0);
    let foreign = source.clone();
    assert!(matches!(
        encoded.source_id_observed(&foreign, &mut work),
        Err(BindingCodecError::InvalidShape(
            "aggregate signature is not an original emitted source"
        ))
    ));
    work.finish().unwrap();
}
