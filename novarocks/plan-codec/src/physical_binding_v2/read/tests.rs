// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::sync::{Arc, Mutex};

const SOURCE: usize = 256 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
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
        max_type_references: 8192,
        max_request_bytes: 1024 * 1024,
        max_allocation_requests: 1,
        max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn types() -> DecodedTypeTable {
    #[allow(deprecated)]
    let child = Field::new_dict(
        "actual_child",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        i64::MIN,
        true,
    )
    .with_metadata(std::collections::HashMap::<String, String>::from([(
        "author".to_owned(),
        "original".to_owned(),
    )]));
    let input = [
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
        (
            7,
            FunctionValueType::new(DataType::Struct(vec![Arc::new(child)].into()), true),
        ),
    ];
    let bounds = TypeProjectionLimits {
        max_definitions: 64,
        max_expanded_nodes: 256,
        max_string_bytes: 8192,
    };
    let wire = encode_type_table(&input, bounds, &Setup).unwrap();
    decode_type_table(&wire, bounds, &Setup).unwrap()
}
fn scalar(id: u32, kind: wire::FunctionKind) -> wire::FunctionBindingDefinition {
    wire::FunctionBindingDefinition {
        id,
        function_id: "original/identity\0|,函".into(),
        overload_id: "original overload".into(),
        kind: kind as i32,
        arguments: vec![
            wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::ValueTypeId(0)),
            },
            wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::Lambda(
                    wire::LambdaArgumentType {
                        parameter_value_type_ids: vec![u32::MAX, 7],
                        result_value_type_id: Some(0),
                    },
                )),
            },
        ],
        result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(7)),
    }
}
fn definitions() -> Vec<wire::FunctionBindingDefinition> {
    let mut table = scalar(u32::MAX, wire::FunctionKind::Table);
    table.result = Some(wire::function_binding_definition::Result::Relation(
        wire::RelationResultTypes {
            value_type_ids: vec![u32::MAX, 0, 7],
        },
    ));
    // Receiving order is deliberately different from the encoder's sorted-ID contract.
    vec![
        table,
        scalar(9, wire::FunctionKind::Window),
        scalar(0, wire::FunctionKind::Scalar),
        scalar(5, wire::FunctionKind::Aggregate),
    ]
}
fn invalid(result: Result<PreparedFunctionBindingHeaders<'_>, BindingCodecError>) {
    assert!(matches!(result, Err(BindingCodecError::InvalidShape(_))));
}
fn prefixes(call: impl Fn(&Control) -> Result<(), BindingCodecError>, success: bool, all: bool) {
    let good = Control::default();
    assert_eq!(call(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for (at, (_, units)) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
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

#[test]
fn receiving_headers_borrow_all_kinds_original_arguments_results_and_complete_types() {
    let definitions = definitions();
    let types = types();
    let control = Control::default();
    let token =
        prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), &control).unwrap();
    assert!(std::ptr::eq(token.as_wire(), definitions.as_slice()));
    assert!(std::ptr::eq(token.type_table(), &types));
    assert_eq!(token.facts().definition_count, 4);
    assert_eq!(token.facts().type_reference_count, 22);
    assert_eq!(
        token.facts().request_bytes_upper_bound,
        4 * size_of::<usize>()
    );
    assert_eq!(token.facts().allocation_requests_upper_bound, 1);
    assert_eq!(
        token
            .facts()
            .coexisting_source_and_request_bytes_upper_bound,
        SOURCE + 4 * size_of::<usize>()
    );
    for original in &definitions {
        assert!(std::ptr::eq(
            token.definition(original.id).unwrap().unwrap(),
            original
        ));
        assert_eq!(original.arguments.len(), 2);
    }
    assert_eq!(
        types.value_type(u32::MAX).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    assert!(types.value_type(u32::MAX).unwrap().nullable());
    assert_eq!(types.value_type(0).unwrap().data_type(), &DataType::Int64);
    assert!(!types.value_type(0).unwrap().nullable());
    let DataType::Struct(fields) = &token.type_table().value_type(7).unwrap().data_type() else {
        panic!("not original Struct")
    };
    assert_eq!(fields[0].name(), "actual_child");
    assert!(fields[0].metadata().is_empty());
    #[allow(deprecated)]
    {
        assert_eq!(fields[0].dict_id(), None);
        assert_eq!(fields[0].dict_is_ordered(), None);
    }
    assert!(std::ptr::eq(
        token.type_table().value_type(7).unwrap(),
        types.value_type(7).unwrap()
    ));
}

#[test]
fn receiving_headers_sparse_unsorted_lookup_uses_original_control_and_returns_original_dto() {
    let definitions = definitions();
    let types = types();
    prefixes(
        |control| {
            let token =
                prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), control)?;
            for (id, ordinal) in [(0, 2), (5, 3), (9, 1), (u32::MAX, 0)] {
                assert!(std::ptr::eq(
                    token.definition(id)?.unwrap(),
                    &definitions[ordinal]
                ));
            }
            assert!(token.definition(1)?.is_none());
            assert!(token.definition(u32::MAX - 1)?.is_none());
            Ok(())
        },
        true,
        true,
    );
}

#[test]
fn receiving_headers_reject_missing_unknown_or_disagreeing_header_shapes() {
    let types = types();
    for fault in 0..15 {
        let mut defs = vec![scalar(0, wire::FunctionKind::Scalar)];
        match fault {
            0 => defs[0].function_id.clear(),
            1 => defs[0].overload_id.clear(),
            2 => defs[0].function_id = "x".repeat(1025),
            3 => defs[0].kind = i32::MAX,
            4 => defs[0].result = None,
            5 => {
                defs[0].result = Some(wire::function_binding_definition::Result::Relation(
                    wire::RelationResultTypes {
                        value_type_ids: vec![0],
                    },
                ))
            }
            6 => defs[0].kind = wire::FunctionKind::Table as i32,
            7 => defs[0].arguments[0].kind = None,
            8 => {
                defs[0].arguments[0].kind =
                    Some(wire::function_argument_type::Kind::ValueTypeId(123))
            }
            9 => {
                let Some(wire::function_argument_type::Kind::Lambda(lambda)) =
                    &mut defs[0].arguments[1].kind
                else {
                    unreachable!()
                };
                lambda.result_value_type_id = None;
            }
            10 => {
                let Some(wire::function_argument_type::Kind::Lambda(lambda)) =
                    &mut defs[0].arguments[1].kind
                else {
                    unreachable!()
                };
                lambda.parameter_value_type_ids[0] = 123;
            }
            11 => {
                defs[0].result =
                    Some(wire::function_binding_definition::Result::ScalarValueTypeId(123))
            }
            12 => defs.push(defs[0].clone()),
            13 => {
                let Some(wire::function_argument_type::Kind::Lambda(lambda)) =
                    &mut defs[0].arguments[1].kind
                else {
                    unreachable!()
                };
                lambda.result_value_type_id = Some(123);
            }
            14 => defs[0].overload_id = "x".repeat(1025),
            _ => unreachable!(),
        }
        invalid(prepare_function_binding_headers(
            &defs,
            &types,
            SOURCE,
            limits(),
            &Control::default(),
        ));
    }
}

#[test]
fn receiving_headers_six_explicit_limits_accept_exact_and_refuse_one_under() {
    let definitions = definitions();
    let types = types();
    let recording = Control::default();
    let facts =
        *prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), &recording)
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
    prepare_function_binding_headers(&definitions, &types, SOURCE, exact, &Control::default())
        .unwrap();
    for limit in [
        BindingProjectionLimits {
            max_definitions: exact.max_definitions - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_type_references: exact.max_type_references - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_request_bytes: exact.max_request_bytes - 1,
            ..exact
        },
        BindingProjectionLimits {
            max_allocation_requests: 0,
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
        invalid(prepare_function_binding_headers(
            &definitions,
            &types,
            SOURCE,
            limit,
            &Control::default(),
        ));
    }
    invalid(prepare_function_binding_headers(
        &definitions,
        &types,
        0,
        limits(),
        &Control::default(),
    ));
}

#[test]
fn receiving_headers_source_invoice_counts_owned_spare_string_and_vector_capacity() {
    let types = types();
    for owner in 0..4 {
        let mut defs = vec![scalar(0, wire::FunctionKind::Scalar)];
        match owner {
            0 => defs[0].function_id.reserve_exact(2 * SOURCE),
            1 => defs[0]
                .arguments
                .reserve_exact(2 * SOURCE / size_of::<wire::FunctionArgumentType>()),
            2 => {
                let Some(wire::function_argument_type::Kind::Lambda(lambda)) =
                    &mut defs[0].arguments[1].kind
                else {
                    unreachable!()
                };
                lambda.parameter_value_type_ids.reserve_exact(SOURCE);
            }
            3 => {
                defs[0].kind = wire::FunctionKind::Table as i32;
                let mut ids = vec![0];
                ids.reserve_exact(SOURCE);
                defs[0].result = Some(wire::function_binding_definition::Result::Relation(
                    wire::RelationResultTypes {
                        value_type_ids: ids,
                    },
                ));
            }
            _ => unreachable!(),
        }
        let refusing = Control::default();
        invalid(prepare_function_binding_headers(
            &defs,
            &types,
            SOURCE,
            limits(),
            &refusing,
        ));
        // Identity/ref payload counts did not grow; only retained source capacity did.
        assert!(
            refusing
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|(_, units)| *units < 256)
        );
    }
}

#[test]
fn receiving_headers_success_ordinary_errors_and_entry_tail_keep_every_original_cause() {
    let types = types();
    let definitions = vec![scalar(0, wire::FunctionKind::Scalar)];
    prefixes(
        |control| {
            prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), control)
                .map(|_| ())
        },
        true,
        true,
    );
    let mut unknown = definitions.clone();
    unknown[0].result = Some(wire::function_binding_definition::Result::ScalarValueTypeId(123));
    prefixes(
        |control| {
            prepare_function_binding_headers(&unknown, &types, SOURCE, limits(), control)
                .map(|_| ())
        },
        false,
        true,
    );
    let duplicate = vec![definitions[0].clone(), definitions[0].clone()];
    prefixes(
        |control| {
            prepare_function_binding_headers(&duplicate, &types, SOURCE, limits(), control)
                .map(|_| ())
        },
        false,
        true,
    );
    prefixes(
        |control| {
            prepare_function_binding_headers(
                &definitions,
                &types,
                SOURCE,
                BindingProjectionLimits {
                    max_request_bytes: 0,
                    ..limits()
                },
                control,
            )
            .map(|_| ())
        },
        false,
        true,
    );
}

#[test]
fn receiving_headers_empty_namespace_has_no_index_request_and_still_observes_control() {
    let types = types();
    let definitions = [];
    let limits = BindingProjectionLimits {
        max_definitions: 0,
        max_type_references: 0,
        max_request_bytes: 0,
        max_allocation_requests: 0,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 128,
    };
    let control = Control::default();
    let token =
        prepare_function_binding_headers(&definitions, &types, SOURCE, limits, &control).unwrap();
    assert_eq!(token.facts().definition_count, 0);
    assert_eq!(token.facts().allocation_requests_upper_bound, 0);
    assert_eq!(token.facts().request_bytes_upper_bound, 0);
    assert!(token.definition(0).unwrap().is_none());
    prefixes(
        |control| {
            prepare_function_binding_headers(&definitions, &types, SOURCE, limits, control)
                .map(|_| ())
        },
        true,
        true,
    );
}

#[test]
fn receiving_headers_wide_real_index_and_reference_loops_preserve_quantum_prefixes() {
    let types = types();
    let definitions = (0..320)
        .rev()
        .map(|id| {
            let mut definition = scalar(id, wire::FunctionKind::Scalar);
            definition.arguments.clear();
            definition
        })
        .collect::<Vec<_>>();
    let good = Control::default();
    let token =
        prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), &good).unwrap();
    assert_eq!(token.facts().definition_count, 320);
    assert_eq!(token.facts().type_reference_count, 320);
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    assert!(std::ptr::eq(
        token.definition(0).unwrap().unwrap(),
        &definitions[319]
    ));
    assert!(std::ptr::eq(
        token.definition(319).unwrap().unwrap(),
        &definitions[0]
    ));
    prefixes(
        |control| {
            prepare_function_binding_headers(&definitions, &types, SOURCE, limits(), control)
                .map(|_| ())
        },
        true,
        false,
    );
}
