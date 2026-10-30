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
use crate::{
    physical_binding_v2::{
        materialize_function_bindings, prepare_function_binding_headers,
        prepare_function_bindings_materialization,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table},
};
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::{AggregatePhase, AggregateSequenceId};
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{
    AggregateStateArgumentContract, CompileControlError, FunctionArgumentType, ValueLogicalType,
};
use std::{
    alloc::Layout,
    collections::HashMap,
    sync::{Arc, Mutex},
};

const FUNCTION_SOURCE: usize = 128 * 1024;
const AGGREGATE_SOURCE: usize = 256 * 1024;
const SOURCE: usize = 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    active: bool,
    stop: Option<(usize, CompileControlError)>,
    trace: Vec<u32>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            active: true,
            stop,
            trace: vec![],
        };
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut state = self.0.lock().unwrap();
        if !state.active {
            return Ok(());
        }
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let at = state.trace.len();
        if let Some((stop, _)) = state.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        state.trace.push(units);
        match state.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1024,
        max_type_references: 8192,
        max_request_bytes: 8 * 1024 * 1024,
        max_allocation_requests: 8192,
        max_coexisting_source_and_request_bytes: 16 * 1024 * 1024,
        max_work: 1024 * 1024 * 1024,
    }
}
fn types(control: &Control) -> DecodedTypeTable {
    let field = Arc::new(
        Field::new("authored-name", DataType::Int32, true)
            .with_metadata(HashMap::from([("original".into(), "metadata".into())])),
    );
    let originals = [
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            u32::MAX,
            FunctionValueType::new(DataType::Struct(vec![field.clone()].into()), true),
        ),
        (
            8,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            9,
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ),
    ];
    assert_eq!(field.metadata()["original"], "metadata");
    let limits = TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    };
    let raw = encode_type_table(&originals, limits, control).unwrap();
    decode_type_table(&raw, limits, control).unwrap()
}
fn value(id: u32) -> wire::FunctionArgumentType {
    wire::FunctionArgumentType {
        kind: Some(wire::function_argument_type::Kind::ValueTypeId(id)),
    }
}
fn raw_function(wide: bool) -> wire::FunctionBindingDefinition {
    // A complete static signature fixture, not a claim that an installed
    // aggregate accepts Lambda. Receiving signatures remain owner-neutral.
    wire::FunctionBindingDefinition {
        id: u32::MAX,
        function_id: "test/aggregate".into(),
        overload_id: "test/aggregate/overload".into(),
        kind: wire::FunctionKind::Aggregate as i32,
        arguments: if wide {
            vec![value(0); 320]
        } else {
            vec![
                value(8),
                wire::FunctionArgumentType {
                    kind: Some(wire::function_argument_type::Kind::Lambda(
                        wire::LambdaArgumentType {
                            parameter_value_type_ids: vec![0, u32::MAX],
                            result_value_type_id: Some(9),
                        },
                    )),
                },
            ]
        },
        result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(0)),
    }
}
fn raw_aggregate(id: u32, phase: wire::aggregate_phase::Kind) -> wire::AggregateBindingDefinition {
    wire::AggregateBindingDefinition {
        state_interpretation: None,
        id,
        function_binding_id: Some(u32::MAX),
        phase: Some(wire::AggregatePhase { kind: Some(phase) }),
        logical_argument_count: 2,
        intermediate_value_type_id: Some(8),
        state_format: "test/aggregate/state-v1".into(),
        state_argument_contract:
            novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature as i32,
    }
}
fn four_phases() -> Vec<wire::AggregateBindingDefinition> {
    let mut independent = raw_aggregate(0, wire::aggregate_phase::Kind::PartialSequenceId(0));
    independent.state_argument_contract =
        novarocks_proto_models::common::AggregateStateArgumentContract::ValueRootNullabilityIndependent as i32;
    independent.logical_argument_count = 0;
    vec![
        raw_aggregate(
            u32::MAX,
            wire::aggregate_phase::Kind::FinalSequenceId(u32::MAX),
        ),
        independent,
        raw_aggregate(9, wire::aggregate_phase::Kind::Single(Empty {})),
        raw_aggregate(7, wire::aggregate_phase::Kind::IntermediateSequenceId(0)),
    ]
}
fn with_receivers<T>(
    control: &Control,
    raw: &[wire::AggregateBindingDefinition],
    wide: bool,
    call: impl FnOnce(
        &PreparedAggregateBindingHeaders<'_, '_>,
        &MaterializedFunctionBindings<'_, '_>,
    ) -> T,
) -> T {
    let types = types(control);
    let definitions = [raw_function(wide)];
    let functions =
        prepare_function_binding_headers(&definitions, &types, FUNCTION_SOURCE, limits(), control)
            .unwrap();
    let owned = materialize_function_bindings(
        prepare_function_bindings_materialization(&functions, AGGREGATE_SOURCE, limits()).unwrap(),
    )
    .unwrap();
    let headers = super::super::prepare_aggregate_binding_headers(
        raw,
        &functions,
        AGGREGATE_SOURCE,
        limits(),
    )
    .unwrap();
    call(&headers, &owned)
}
fn prefixes(
    call: impl Fn(&Control, Option<(usize, CompileControlError)>) -> Result<(), Error>,
    success: bool,
) {
    let baseline = Control::default();
    assert_eq!(call(&baseline, None).is_ok(), success);
    let trace = baseline.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::default();
            assert!(
                matches!(call(&control, Some((at,cause.clone()))), Err(Error::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn exact(f: BindingProjectionFacts) -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: f.definition_count,
        max_type_references: f.type_reference_count,
        max_request_bytes: f.request_bytes_upper_bound,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn under(mut l: BindingProjectionLimits, axis: usize) -> BindingProjectionLimits {
    match axis {
        0 => l.max_definitions -= 1,
        1 => l.max_type_references -= 1,
        2 => l.max_request_bytes -= 1,
        3 => l.max_allocation_requests -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        5 => l.max_work -= 1,
        _ => unreachable!(),
    }
    l
}

#[test]
fn aggregate_materialization_all_four_phases_preserve_sparse_order_and_seven_fields() {
    let control = Control::default();
    let raw = four_phases();
    with_receivers(&control, &raw, false, |headers, functions| {
        control.arm(None);
        let prepared =
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap();
        let facts = *prepared.facts();
        let output = materialize_aggregate_bindings(prepared).unwrap();
        assert_eq!(
            output
                .definitions()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            [u32::MAX, 0, 9, 7]
        );
        assert_eq!(
            output.facts().type_reference_count,
            facts.type_reference_count
        );
        let expected = [
            AggregatePhase::Final {
                sequence: AggregateSequenceId::new(u32::MAX),
            },
            AggregatePhase::Partial {
                sequence: AggregateSequenceId::new(0),
            },
            AggregatePhase::Single,
            AggregatePhase::Intermediate {
                sequence: AggregateSequenceId::new(0),
            },
        ];
        for (at, (_, binding)) in output.definitions().iter().enumerate() {
            assert_eq!(binding.phase, expected[at]);
            assert_eq!(binding.logical_argument_count, if at == 1 { 0 } else { 2 });
            assert_eq!(binding.state_format.as_str(), "test/aggregate/state-v1");
            assert_eq!(
                binding.state_argument_contract,
                if at == 1 {
                    AggregateStateArgumentContract::ValueRootNullabilityIndependent
                } else {
                    AggregateStateArgumentContract::ExactSignature
                }
            );
            assert_eq!(binding.intermediate_type.data_type(), &DataType::Utf8);
            assert!(binding.intermediate_type.nullable());
            assert_eq!(binding.function.kind, FunctionKind::Aggregate);
            assert_eq!(binding.function.function_id.as_str(), "test/aggregate");
            assert_eq!(
                binding.function.overload.as_str(),
                "test/aggregate/overload"
            );
            assert_eq!(
                binding.function.result_type,
                FunctionValueType::new(DataType::Int64, false)
            );
            assert!(binding.function.legacy_metadata.is_none());
            assert!(
                matches!(&binding.function.argument_types[0],FunctionArgumentType::Value(t) if t.data_type() == &DataType::Utf8 && t.nullable())
            );
        }
        let owned = output.into_definitions();
        assert_eq!(owned.len(), 4);
        assert!(!std::ptr::eq(&owned[0].1.function, &owned[1].1.function));
    });
    let control = Control::default();
    with_receivers(&control, &[], false, |headers, functions| {
        let output = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap(),
        )
        .unwrap();
        assert!(output.definitions().is_empty());
        assert_eq!(output.facts().definition_count, 0);
        assert_eq!(output.facts().type_reference_count, 0);
        assert_eq!(output.facts().allocation_requests_upper_bound, 0);
        assert_eq!(output.facts().request_bytes_upper_bound, 0);
    });
}

#[test]
fn aggregate_materialization_full_lambda_nominal_metadata_and_dictionary_owned_copy() {
    let control = Control::default();
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    with_receivers(&control, &raw, false, |headers, functions| {
        let output = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap(),
        )
        .unwrap();
        assert!(std::ptr::eq(output.headers(), headers));
        assert!(std::ptr::eq(output.functions(), functions));
        let binding = &output.definitions()[0].1;
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &binding.function.argument_types[1]
        else {
            panic!("Lambda")
        };
        assert_eq!(
            parameter_types[0],
            FunctionValueType::new(DataType::Int64, false)
        );
        assert_eq!(result_type.logical_type(), ValueLogicalType::LargeInt);
        assert_eq!(result_type.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!result_type.nullable());
        let DataType::Struct(fields) = &parameter_types[1].data_type() else {
            panic!("Struct")
        };
        let DataType::Struct(original) = &headers
            .functions()
            .type_table()
            .value_type(u32::MAX)
            .unwrap()
            .data_type()
        else {
            panic!("original Struct")
        };
        assert!(Arc::ptr_eq(&fields[0], &original[0]));
        assert_eq!(fields[0].name(), "authored-name");
        assert!(fields[0].is_nullable());
        assert!(fields[0].metadata().as_arc().is_none());
        let original = headers.functions().type_table().value_type(8).unwrap();
        assert_eq!(binding.intermediate_type, *original);
        assert_eq!(binding.intermediate_type.data_type(), &DataType::Utf8);
        assert!(
            binding
                .intermediate_type
                .value_type()
                .shares_storage(original.value_type())
        );
        assert!(std::ptr::eq(
            binding.intermediate_type.data_type(),
            original.data_type()
        ));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(std::ptr::eq(
            output.definition_observed(0, &mut work).unwrap().unwrap(),
            binding
        ));
        assert!(
            output
                .definition_observed(u32::MAX, &mut work)
                .unwrap()
                .is_none()
        );
        work.finish().unwrap();
        let foreign = Control::default();
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
        assert!(matches!(
            output.definition_observed(0, &mut work),
            Err(Error::InvalidShape(
                "materialized aggregate lookup has a different original control"
            ))
        ));
        work.finish().unwrap();
    });
}

#[test]
fn aggregate_materialization_requests_have_independent_layout_and_retained_oracles() {
    let control = Control::default();
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    with_receivers(&control, &raw, false, |headers, functions| {
        let prepared =
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap();
        let facts = *prepared.facts();
        let retained = Layout::array::<(u32, AggregateBinding)>(1).unwrap().size()
            + "test/aggregate/state-v1".len();
        // Only the outer Vec/Box and newly decoded state text/identity owner
        // are constructed. The exact function/type signature is shared.
        let requested = 2 * Layout::array::<(u32, AggregateBinding)>(1).unwrap().size()
            + "test/aggregate/state-v1".len()
            + novarocks_type_contract::AggregateStateFormatId::storage_allocation_layout()
                .unwrap()
                .size();
        assert_eq!(facts.definition_count, 1);
        assert_eq!(facts.type_reference_count, 6);
        assert_eq!(facts.allocation_requests_upper_bound, 4);
        assert_eq!(facts.request_bytes_upper_bound, requested);
        assert_eq!(
            facts.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + requested
        );
        // This explicit pure test author observes the real construction body;
        // it neither grants BE memory nor synthesizes source provenance.
        let mut layouts = Vec::new();
        let mut request = |layout| {
            layouts.push(layout);
            Ok::<(), std::convert::Infallible>(())
        };
        let mut opaque = |_| -> Result<(), std::convert::Infallible> {
            panic!("this author uses only actual known layouts")
        };
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut opaque,
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let output =
            materialize_aggregate_bindings_with_creator_in(prepared, &mut creation, &mut work)
                .unwrap();
        work.finish().unwrap();
        drop(creation);
        assert_eq!(
            layouts,
            [
                Layout::array::<(u32, AggregateBinding)>(1).unwrap(),
                Layout::array::<u8>("test/aggregate/state-v1".len()).unwrap(),
                novarocks_type_contract::AggregateStateFormatId::storage_allocation_layout()
                    .unwrap(),
            ]
        );
        // Vec-to-Box shrink may request the same outer layout once more. The
        // original four-request upper covers it, without billing shared types.
        assert_eq!(
            requested,
            layouts.iter().map(Layout::size).sum::<usize>()
                + Layout::array::<(u32, AggregateBinding)>(1).unwrap().size()
        );
        let original_function = match &functions.definitions()[0].1 {
            crate::physical_binding_v2::MaterializedFunctionBinding::Scalar(value) => value,
            _ => panic!("aggregate scalar signature"),
        };
        assert!(std::ptr::eq(
            output.definitions()[0].1.function.argument_types.as_ptr(),
            original_function.argument_types.as_ptr()
        ));
        assert_eq!(
            output.retained_output_floor().unwrap(),
            size_of::<MaterializedAggregateBindings<'_, '_, '_>>() + retained
        );
        assert_eq!(
            output.retained_invoice_floor().unwrap(),
            SOURCE + size_of::<MaterializedAggregateBindings<'_, '_, '_>>() + retained
        );
        assert!(requested > retained);
    });
}

#[test]
fn aggregate_materialization_six_axes_source_floor_and_foreign_equal_namespace_refuse() {
    let control = Control::default();
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    with_receivers(&control, &raw, false, |headers, functions| {
        let baseline =
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap();
        let facts = *baseline.facts();
        materialize_aggregate_bindings(baseline).unwrap();
        materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, exact(facts))
                .unwrap(),
        )
        .unwrap();
        for axis in 0..6 {
            assert!(
                matches!(
                    prepare_aggregate_bindings_materialization(
                        headers,
                        functions,
                        SOURCE,
                        under(exact(facts), axis)
                    ),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ),
                "axis {axis}"
            );
        }
        let known =
            headers.retained_invoice_floor().unwrap() + functions.retained_output_floor().unwrap();
        assert!(
            prepare_aggregate_bindings_materialization(headers, functions, known, limits()).is_ok()
        );
        assert!(matches!(
            prepare_aggregate_bindings_materialization(headers, functions, known - 1, limits()),
            Err(Error::InvalidShape(_))
        ));
        let definitions = [raw_function(false)];
        let foreign = prepare_function_binding_headers(
            &definitions,
            headers.functions().type_table(),
            FUNCTION_SOURCE,
            limits(),
            &control,
        )
        .unwrap();
        let foreign = materialize_function_bindings(
            prepare_function_bindings_materialization(&foreign, AGGREGATE_SOURCE, limits())
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            prepare_aggregate_bindings_materialization(headers, &foreign, SOURCE, limits()),
            Err(Error::InvalidShape(
                "aggregate materialization has a different original function namespace"
            ))
        ));
    });
}

#[test]
fn aggregate_materialization_original_control_success_and_ordinary_all_prefixes() {
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    for ordinary in [false, true] {
        prefixes(
            |control, stop| {
                with_receivers(control, &raw, false, |headers, functions| {
                    control.arm(stop);
                    let source = if ordinary { 0 } else { SOURCE };
                    let output = materialize_aggregate_bindings(
                        prepare_aggregate_bindings_materialization(
                            headers,
                            functions,
                            source,
                            limits(),
                        )?,
                    )?;
                    assert!(output.definitions()[0].1.function.legacy_metadata.is_none());
                    Ok(())
                })
            },
            !ordinary,
        );
    }
    // A known numeric refusal precedes any later checkpoint. This is not an
    // ordinary source-floor failure and receives no replacement footer cause.
    for cause in CAUSES {
        let control = Control::default();
        with_receivers(&control, &raw, false, |headers, functions| {
            control.arm(Some((1, cause)));
            let mut l = limits();
            l.max_definitions = 0;
            assert!(matches!(
                prepare_aggregate_bindings_materialization(headers, functions, SOURCE, l),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
        });
    }
}

#[test]
fn aggregate_materialization_wide_actual_320_arguments_and_consumed_emit_prefixes() {
    let raw = [raw_aggregate(
        u32::MAX,
        wire::aggregate_phase::Kind::FinalSequenceId(0),
    )];
    let baseline = Control::default();
    let prepare_trace = with_receivers(&baseline, &raw, true, |headers, functions| {
        baseline.arm(None);
        let prepared =
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap();
        assert_eq!(prepared.facts().type_reference_count, 322);
        baseline.trace()
    });
    assert!(prepare_trace.contains(&256));
    for at in (0..prepare_trace.len())
        .filter(|at| *at == 0 || *at + 1 == prepare_trace.len() || prepare_trace[*at] == 256)
    {
        for cause in CAUSES {
            let control = Control::default();
            with_receivers(&control, &raw, true, |headers, functions| {
                control.arm(Some((at, cause.clone())));
                assert!(
                    matches!(prepare_aggregate_bindings_materialization(headers,functions,SOURCE,limits()),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), prepare_trace[..=at]);
            });
        }
    }
    let baseline = Control::default();
    let emit_trace = with_receivers(&baseline, &raw, true, |headers, functions| {
        let prepared =
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap();
        baseline.arm(None);
        let output = materialize_aggregate_bindings(prepared).unwrap();
        assert_eq!(output.definitions()[0].1.function.argument_types.len(), 320);
        for argument in &output.definitions()[0].1.function.argument_types {
            assert_eq!(
                argument,
                &FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, false))
            );
        }
        assert!(output.definitions()[0].1.function.legacy_metadata.is_none());
        baseline.trace()
    });
    // Clone/reserve calls legitimately flush frequently. Sample actual entry,
    // interior and tail without asserting an opaque emit quantum of 256.
    for at in [0, emit_trace.len() / 2, emit_trace.len() - 1] {
        for cause in CAUSES {
            let control = Control::default();
            with_receivers(&control, &raw, true, |headers, functions| {
                let prepared = prepare_aggregate_bindings_materialization(
                    headers,
                    functions,
                    SOURCE,
                    limits(),
                )
                .unwrap();
                control.arm(Some((at, cause.clone())));
                assert!(
                    matches!(materialize_aggregate_bindings(prepared),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), emit_trace[..=at]);
            });
        }
    }
}

mod owner_tests {
    include!("owner_tests.rs");
}

#[test]
fn logical_aggregate_types_keep_actual_source_encoding_and_attrs_separate() {
    use novarocks_type_contract::{
        ValueKind, ValueLayout, ValueTypeError, ValueTypeRef, ValueTypeStorageLifetime,
        value_layout_matches_carrier_observed,
    };
    let carrier = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let source = Arc::new(
        Field::new("source-column", carrier.clone(), true).with_metadata(HashMap::from([
            ("nr_logical_type".to_owned(), "json".to_owned()),
            ("provider.opaque".to_owned(), "original".to_owned()),
        ])),
    );
    let pure = ValueTypeStorageLifetime::pure();
    // This explicit String author does not interpret either opaque source key.
    let logical = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::String, true, pure.clone()).unwrap(),
        ValueLayout::try_new(source.data_type(), pure).unwrap(),
    )
    .unwrap();
    let limits = TypeProjectionLimits {
        max_definitions: 32,
        max_expanded_nodes: 128,
        max_string_bytes: 4096,
    };
    let control = Control::default();
    let wire = encode_type_table(&[(7, logical.clone())], limits, &control).unwrap();
    let decoded = decode_type_table(&wire, limits, &control).unwrap();
    let actual = decoded.value_type(7).unwrap();
    assert_eq!(actual, &logical);
    assert_eq!(actual.data_type(), &DataType::Utf8);
    assert_eq!(actual.logical_type(), ValueLogicalType::Physical);
    assert_eq!(source.data_type(), &carrier);
    assert_eq!(source.metadata()["nr_logical_type"], "json");
    assert_eq!(source.metadata()["provider.opaque"], "original");
    let matches = |carrier: &DataType| {
        value_layout_matches_carrier_observed(actual.typed_binding().layout(), carrier, |_| {
            Ok::<(), ValueTypeError>(())
        })
        .unwrap()
    };
    assert!(matches(source.data_type()));
    assert!(!matches(&DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::LargeUtf8)
    )));
    // Logical String agrees, but offset width is a real physical binding fact.
    assert!(actual.same_semantics(&FunctionValueType::new(DataType::LargeUtf8, true)));
    assert_ne!(actual, &FunctionValueType::new(DataType::LargeUtf8, true));
}
