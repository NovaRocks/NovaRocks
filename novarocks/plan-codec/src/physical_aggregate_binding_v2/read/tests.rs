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
use crate::physical_binding_v2::prepare_function_binding_headers;
use crate::physical_type_v2::{
    DecodedTypeTable, TypeProjectionLimits, decode_type_table, encode_type_table,
};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

const FUNCTION_SOURCE: usize = 256 * 1024;
const SOURCE: usize = 512 * 1024;
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
    active: AtomicBool,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn activate(&self) {
        self.active.store(true, Ordering::SeqCst);
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        // Fixture setup uses this same original control without injecting a
        // refusal. The tested stage starts only after its prerequisites exist.
        if !self.active.load(Ordering::SeqCst) {
            return Ok(());
        }
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
    let child =
        Field::new("nested", DataType::Int32, false).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            "original".into(),
            "retained".into(),
        )]));
    let values = [
        (
            0,
            FunctionValueType::new(DataType::Struct(vec![Arc::new(child)].into()), true),
        ),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
    ];
    let limits = TypeProjectionLimits {
        max_definitions: 32,
        max_expanded_nodes: 128,
        max_string_bytes: 4096,
    };
    let wire = encode_type_table(&values, limits, &Setup).unwrap();
    decode_type_table(&wire, limits, &Setup).unwrap()
}
fn functions() -> Vec<wire::FunctionBindingDefinition> {
    [
        (u32::MAX, wire::FunctionKind::Scalar),
        (0, wire::FunctionKind::Aggregate),
        (9, wire::FunctionKind::Window),
    ]
    .into_iter()
    .map(|(id, kind)| wire::FunctionBindingDefinition {
        id,
        function_id: "source/function".into(),
        overload_id: "source/overload".into(),
        kind: kind as i32,
        arguments: vec![],
        result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(0)),
    })
    .collect()
}
fn with_functions<T>(
    control: &Control,
    run: impl FnOnce(&PreparedFunctionBindingHeaders<'_>) -> T,
) -> T {
    let types = types();
    let definitions = functions();
    let token =
        prepare_function_binding_headers(&definitions, &types, FUNCTION_SOURCE, limits(), control)
            .unwrap();
    run(&token)
}
fn definition(id: u32, phase: wire::aggregate_phase::Kind) -> wire::AggregateBindingDefinition {
    wire::AggregateBindingDefinition {
        state_interpretation: None,
        id,
        function_binding_id: Some(0),
        phase: Some(wire::AggregatePhase { kind: Some(phase) }),
        logical_argument_count: u32::MAX,
        intermediate_value_type_id: Some(0),
        state_format: "aggregate/state-v1".into(),
        state_argument_contract:
            novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature as i32,
    }
}
fn definitions() -> Vec<wire::AggregateBindingDefinition> {
    let mut final_state = definition(
        u32::MAX,
        wire::aggregate_phase::Kind::FinalSequenceId(u32::MAX),
    );
    final_state.intermediate_value_type_id = Some(u32::MAX);
    vec![
        final_state,
        definition(
            0,
            wire::aggregate_phase::Kind::Single(physical_control_v2::Empty {}),
        ),
        definition(9, wire::aggregate_phase::Kind::IntermediateSequenceId(0)),
        definition(5, wire::aggregate_phase::Kind::PartialSequenceId(u32::MAX)),
    ]
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
                active: AtomicBool::new(false),
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(BindingCodecError::Control(actual)) if actual == cause),
                "refusal at {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
fn check(
    definitions: &[wire::AggregateBindingDefinition],
    source: usize,
    bounds: BindingProjectionLimits,
    control: &Control,
) -> Result<(), BindingCodecError> {
    with_functions(control, |functions| {
        control.activate();
        prepare_aggregate_binding_headers(definitions, functions, source, bounds).map(|_| ())
    })
}

#[test]
fn receiving_aggregate_headers_borrow_all_phases_sparse_ids_and_exact_type_owner() {
    let definitions = definitions();
    let control = Control::default();
    with_functions(&control, |functions| {
        control.activate();
        let token =
            prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits()).unwrap();
        assert!(std::ptr::eq(token.as_wire(), definitions.as_slice()));
        assert!(std::ptr::eq(token.functions(), functions));
        assert!(std::ptr::eq(
            token.functions().type_table(),
            functions.type_table()
        ));
        assert_eq!(token.facts().definition_count, 4);
        assert_eq!(token.facts().type_reference_count, 4);
        assert_eq!(
            token.facts().request_bytes_upper_bound,
            4 * size_of::<usize>()
        );
        for original in &definitions {
            let actual = token.definition(original.id).unwrap().unwrap();
            assert!(std::ptr::eq(actual, original));
            assert_eq!(actual.logical_argument_count, u32::MAX);
            assert_eq!(actual.phase, original.phase);
            assert_eq!(
                actual.intermediate_value_type_id,
                original.intermediate_value_type_id
            );
        }
        assert!(token.definition(123).unwrap().is_none());
        let root = functions.type_table().value_type(0).unwrap();
        let DataType::Struct(fields) = &root.data_type() else {
            panic!("original nested type");
        };
        assert!(fields[0].metadata().as_arc().is_none());
        assert_eq!(fields[0].name(), "nested");
        assert!(!fields[0].is_nullable());
        assert_eq!(
            functions
                .type_table()
                .value_type(u32::MAX)
                .unwrap()
                .logical_type(),
            ValueLogicalType::Json
        );
    });
}

#[test]
fn receiving_state_argument_contract_is_mandatory_closed_and_independent_of_phase() {
    // Both contracts are explicit owner facts, independent of these identical
    // function signatures, state formats and all four phase representations.
    for (code, expected) in [
        (1, AggregateStateArgumentContract::ExactSignature),
        (
            2,
            AggregateStateArgumentContract::ValueRootNullabilityIndependent,
        ),
    ] {
        let mut definitions = definitions();
        for definition in &mut definitions {
            definition.state_argument_contract = code;
        }
        let control = Control::default();
        with_functions(&control, |functions| {
            control.activate();
            let token =
                prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits())
                    .unwrap();
            for original in &definitions {
                let borrowed = token.definition(original.id).unwrap().unwrap();
                assert!(std::ptr::eq(borrowed, original));
                assert_eq!(borrowed.state_argument_contract, code);
                assert_eq!(decode_state_argument_contract(code).unwrap(), expected);
            }
        });
        prefixes(
            |control| check(&definitions, SOURCE, limits(), control),
            true,
            true,
        );
    }
    for code in [0, -1, 3, i32::MAX] {
        let mut definitions = definitions();
        definitions[0].state_argument_contract = code;
        assert!(matches!(
            check(&definitions, SOURCE, limits(), &Control::default()),
            Err(BindingCodecError::InvalidShape(
                "aggregate header state argument contract is absent or unknown"
            ))
        ));
        let baseline = Control::default();
        assert!(check(&definitions, SOURCE, limits(), &baseline).is_err());
        let trace = baseline.trace.lock().unwrap().clone();
        // The completed closed-enum check reaches the ordinary footer, before
        // any sparse-index allocation or function lookup.
        assert!(trace.last().unwrap().1 > 0);
        prefixes(
            |control| check(&definitions, SOURCE, limits(), control),
            false,
            true,
        );
    }
}

#[test]
fn receiving_aggregate_header_presence_kind_identity_and_duplicate_failures_are_exact() {
    for case in 0..14 {
        let mut definitions = definitions();
        match case {
            0 => definitions[0].function_binding_id = None,
            1 => definitions[0].function_binding_id = Some(77),
            2 => definitions[0].function_binding_id = Some(u32::MAX),
            3 => definitions[0].function_binding_id = Some(9),
            4 => definitions[0].intermediate_value_type_id = None,
            5 => definitions[0].intermediate_value_type_id = Some(77),
            6 => definitions[0].phase = None,
            7 => definitions[0].phase = Some(wire::AggregatePhase { kind: None }),
            8 => definitions[0].state_format.clear(),
            9 => definitions[0].state_format = "x".repeat(1025),
            10 => definitions[0].state_format = "state with space".into(),
            11 => definitions[0].state_format = "state|v1".into(),
            12 => definitions[0].state_format = "state,函".into(),
            13 => definitions[0].id = definitions[1].id,
            _ => unreachable!(),
        }
        assert!(
            matches!(
                check(&definitions, SOURCE, limits(), &Control::default()),
                Err(BindingCodecError::InvalidShape(_))
            ),
            "case {case}"
        );
    }
    let control = Control::default();
    with_functions(&control, |functions| {
        control.activate();
        let token = prepare_aggregate_binding_headers(&[], functions, SOURCE, limits()).unwrap();
        assert!(token.as_wire().is_empty());
        assert_eq!(token.facts().allocation_requests_upper_bound, 0);
        assert_eq!(token.facts().request_bytes_upper_bound, 0);
        assert!(token.definition(u32::MAX).unwrap().is_none());
    });
}

#[test]
fn receiving_aggregate_six_envelopes_exact_bounds_and_capacity_source_floor() {
    let definitions = definitions();
    let control = Control::default();
    with_functions(&control, |functions| {
        control.activate();
        let facts = *prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits())
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
        prepare_aggregate_binding_headers(&definitions, functions, SOURCE, exact).unwrap();
        for field in 0..6 {
            let mut smaller = exact;
            match field {
                0 => smaller.max_definitions -= 1,
                1 => smaller.max_type_references -= 1,
                2 => smaller.max_request_bytes -= 1,
                3 => smaller.max_allocation_requests -= 1,
                4 => smaller.max_coexisting_source_and_request_bytes -= 1,
                5 => smaller.max_work -= 1,
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    prepare_aggregate_binding_headers(&definitions, functions, SOURCE, smaller),
                    Err(BindingCodecError::InvalidShape(_))
                ),
                "envelope {field}"
            );
        }
        let mut spare = definitions.clone();
        spare[0].state_format.reserve_exact(4096);
        let roots = bytes::<wire::AggregateBindingDefinition>(spare.len()).unwrap();
        let visible = spare
            .iter()
            .map(|value| value.state_format.len())
            .sum::<usize>();
        let insufficient = functions.retained_invoice_floor().unwrap() + roots + visible;
        assert!(matches!(
            prepare_aggregate_binding_headers(&spare, functions, insufficient, limits()),
            Err(BindingCodecError::InvalidShape(_))
        ));
        let full = functions.retained_invoice_floor().unwrap()
            + roots
            + spare
                .iter()
                .map(|value| value.state_format.capacity())
                .sum::<usize>();
        prepare_aggregate_binding_headers(&spare, functions, full, limits()).unwrap();
        assert!(matches!(
            add(usize::MAX, 1),
            Err(BindingCodecError::InvalidShape(_))
        ));
        assert!(matches!(
            bytes::<usize>(usize::MAX),
            Err(BindingCodecError::InvalidShape(_))
        ));
    });
}

#[test]
fn receiving_aggregate_success_and_ordinary_failure_keep_every_original_control_prefix() {
    let definitions = definitions();
    prefixes(
        |control| check(&definitions, SOURCE, limits(), control),
        true,
        true,
    );
    let mut missing = definitions.clone();
    missing[1].intermediate_value_type_id = Some(77);
    prefixes(
        |control| check(&missing, SOURCE, limits(), control),
        false,
        true,
    );
    let mut invalid_state = definitions.clone();
    invalid_state[2].state_format = "valid-prefix|bad".into();
    prefixes(
        |control| check(&invalid_state, SOURCE, limits(), control),
        false,
        true,
    );
    let mut duplicate = definitions.clone();
    duplicate[1].id = duplicate[0].id;
    prefixes(
        |control| check(&duplicate, SOURCE, limits(), control),
        false,
        true,
    );
}

#[test]
fn receiving_aggregate_long_state_and_index_have_actual_quantum_without_new_cap() {
    let definitions = (0..320)
        .rev()
        .map(|id| {
            let mut value = definition(id, wire::aggregate_phase::Kind::PartialSequenceId(0));
            value.state_format = "s".repeat(384);
            value
        })
        .collect::<Vec<_>>();
    let mut long_state = definition(0, wire::aggregate_phase::Kind::PartialSequenceId(0));
    long_state.state_format = "s".repeat(1024);
    let state_control = Control::default();
    check(&[long_state], SOURCE, limits(), &state_control).unwrap();
    assert!(
        state_control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    let good = Control::default();
    check(&definitions, SOURCE, limits(), &good).unwrap();
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    // Keep wide validation finite: sample the first genuine quantum plus
    // entry/tail; small fixtures above cover every callback and all causes.
    let trace = good.trace.lock().unwrap().clone();
    let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                active: AtomicBool::new(false),
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(check(&definitions, SOURCE, limits(), &control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn receiving_aggregate_lookup_keeps_original_control_and_borrowed_source() {
    let definitions = definitions();
    for sought in [0, u32::MAX, 123] {
        prefixes(
            |control| {
                with_functions(control, |functions| {
                    let token = prepare_aggregate_binding_headers(
                        &definitions,
                        functions,
                        SOURCE,
                        limits(),
                    )?;
                    control.activate();
                    let actual = token.definition(sought)?;
                    assert_eq!(actual.is_some(), sought != 123);
                    if let Some(actual) = actual {
                        assert!(std::ptr::eq(
                            actual,
                            definitions.iter().find(|value| value.id == sought).unwrap()
                        ));
                    }
                    Ok(())
                })
            },
            true,
            true,
        );
    }
}

#[test]
fn receiving_aggregate_retained_floor_preserves_whole_invoice_and_actual_index_only_once() {
    let mut definitions = definitions();
    // A large original String capacity is already in the supplied invoice,
    // not recreated or charged again by the receiving loan.
    definitions[0].state_format.reserve_exact(4096);
    let control = Control::default();
    with_functions(&control, |functions| {
        let low =
            prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits()).unwrap();
        let high =
            prepare_aggregate_binding_headers(&definitions, functions, SOURCE + 8192, limits())
                .unwrap();
        assert!(std::ptr::eq(low.as_wire(), definitions.as_slice()));
        assert!(std::ptr::eq(low.functions(), functions));
        assert!(std::ptr::eq(
            low.original_control(),
            &control as &dyn PureCompileControl
        ));
        assert_eq!(
            low.index.backing_bytes().unwrap(),
            high.index.backing_bytes().unwrap()
        );
        let low_floor = low.retained_invoice_floor().unwrap();
        let high_floor = high.retained_invoice_floor().unwrap();
        assert_eq!(high_floor - low_floor, 8192);
        // Inspect the real receiving allocation capacity, never assume that
        // requested count equals allocator-retained capacity.
        assert_eq!(
            low_floor - SOURCE,
            std::mem::size_of_val(&low) + low.index.backing_bytes().unwrap()
        );
        assert!(low_floor < SOURCE + functions.retained_invoice_floor().unwrap());
        assert_eq!(definitions[0].state_format, "aggregate/state-v1");
        let empty = prepare_aggregate_binding_headers(&[], functions, SOURCE, limits()).unwrap();
        assert_eq!(empty.index.backing_bytes().unwrap(), 0);
        assert_eq!(
            empty.retained_invoice_floor().unwrap(),
            SOURCE + std::mem::size_of_val(&empty)
        );
    });
}

#[test]
fn receiving_aggregate_retained_floor_overflow_is_explicit_without_control_replay() {
    let definitions = definitions();
    let control = Control::default();
    with_functions(&control, |functions| {
        let source = usize::MAX - bytes::<usize>(definitions.len()).unwrap();
        let token = prepare_aggregate_binding_headers(
            &definitions,
            functions,
            source,
            BindingProjectionLimits {
                max_coexisting_source_and_request_bytes: usize::MAX,
                ..limits()
            },
        )
        .unwrap();
        control.activate();
        assert!(matches!(
            token.retained_invoice_floor(),
            Err(BindingCodecError::InvalidShape(
                "aggregate header arithmetic overflow"
            ))
        ));
        assert!(control.trace.lock().unwrap().is_empty());
        assert!(std::ptr::eq(
            token.definition(0).unwrap().unwrap(),
            &definitions[1]
        ));
    });
}

fn observed_lookup(
    token: &PreparedAggregateBindingHeaders<'_, '_>,
    sought: u32,
) -> Result<Option<u32>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(token.original_control(), CompilePhase::Decode)?;
    let result = token.definition_observed(sought, &mut work)?;
    work.finish()?;
    Ok(result.map(|definition| definition.id))
}

#[test]
fn receiving_aggregate_observed_lookup_matches_public_trace_and_original_prefixes() {
    let definitions = definitions();
    for sought in [0, u32::MAX, 123] {
        let public_control = Control::default();
        with_functions(&public_control, |functions| {
            let token =
                prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits())
                    .unwrap();
            public_control.activate();
            assert_eq!(
                token
                    .definition(sought)
                    .unwrap()
                    .map(|definition| definition.id),
                (sought != 123).then_some(sought)
            );
        });
        let observed_control = Control::default();
        with_functions(&observed_control, |functions| {
            let token =
                prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits())
                    .unwrap();
            observed_control.activate();
            assert_eq!(
                observed_lookup(&token, sought).unwrap(),
                (sought != 123).then_some(sought)
            );
        });
        assert_eq!(
            *public_control.trace.lock().unwrap(),
            *observed_control.trace.lock().unwrap()
        );
        prefixes(
            |control| {
                with_functions(control, |functions| {
                    let token = prepare_aggregate_binding_headers(
                        &definitions,
                        functions,
                        SOURCE,
                        limits(),
                    )?;
                    control.activate();
                    let mut work = CompileCheckpoints::try_new(
                        token.original_control(),
                        CompilePhase::Decode,
                    )?;
                    let actual = token.definition_observed(sought, &mut work)?;
                    if let Some(actual) = actual {
                        assert!(std::ptr::eq(
                            actual,
                            definitions
                                .iter()
                                .find(|definition| definition.id == sought)
                                .unwrap()
                        ));
                    }
                    work.finish()?;
                    Ok(())
                })
            },
            true,
            true,
        );
    }
}

#[test]
fn receiving_aggregate_repeated_lookup_uses_one_caller_scope_and_actual_quantum() {
    let definitions = definitions();
    let call = |control: &Control| {
        with_functions(control, |functions| {
            let token =
                prepare_aggregate_binding_headers(&definitions, functions, SOURCE, limits())?;
            control.activate();
            let mut work =
                CompileCheckpoints::try_new(token.original_control(), CompilePhase::Decode)?;
            for id in [0, u32::MAX, 123].into_iter().cycle().take(320) {
                let actual = token.definition_observed(id, &mut work)?;
                assert_eq!(actual.is_some(), id != 123);
            }
            work.finish()?;
            Ok(())
        })
    };
    let control = Control::default();
    call(&control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    assert_eq!(trace[0], (CompilePhase::Decode, 0));
    assert_eq!(trace.iter().filter(|(_, units)| *units == 0).count(), 1);
    assert!(trace.iter().any(|(_, units)| *units == 256));
    // Wide selection is sampled; small public/observed lookups above replay
    // every actual callback for all original causes.
    prefixes(call, true, false);
}
