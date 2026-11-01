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
use crate::physical_aggregate_binding_v2 as aggregate;
use crate::physical_binding_v2::{
    ArgumentTypeIds, BindingSource, EncodedFunctionBindings, FunctionBindingInput,
    MaterializationModel, ResultTypeIds, encode_function_bindings,
};
use crate::physical_type_v2::{EncodedTypeTable, encode_type_table_sources};

fn finish<T>(out: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&out, Err(Error::Control(_))) {
        return out;
    }
    work.finish()?;
    out
}
fn materialize_in<'a, 'b, 'c>(
    headers: &'a PreparedAggregateBindingHeaders<'b, 'c>,
    functions: &'a MaterializedFunctionBindings<'b, 'c>,
    caps: BindingProjectionLimits,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<aggregate::MaterializedAggregateBindings<'a, 'b, 'c>, Error> {
    let prepared = aggregate::prepare_aggregate_bindings_materialization_in(
        headers, functions, SOURCE, caps, admit, work,
    )?;
    aggregate::materialize_aggregate_bindings_in(prepared, admit, work)
}

fn with_sender<T>(
    control: &Control,
    call: impl FnOnce(
        &EncodedTypeTable<'_>,
        &EncodedFunctionBindings<'_, '_>,
        &[aggregate::AggregateBindingInput<'_>],
    ) -> T,
) -> T {
    let raw = four_phases();
    with_receivers(control, &raw, false, |headers, functions| {
        let originals = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(headers, functions, SOURCE, limits())
                .unwrap(),
        )
        .unwrap();
        // These are genuine owned receiving signatures. This fixture proves
        // representation and source loans, not phase closure or an installed owner.
        let values = [0, 8, 9, u32::MAX].map(|id| {
            (
                id,
                headers
                    .functions()
                    .type_table()
                    .value_type(id)
                    .unwrap()
                    .clone(),
            )
        });
        let types = encode_type_table_sources(
            &values,
            &[],
            TypeProjectionLimits {
                max_definitions: 4096,
                max_expanded_nodes: 16384,
                max_string_bytes: 65536,
            },
            control,
        )
        .unwrap();
        let params = [0, u32::MAX];
        let args = [
            ArgumentTypeIds::Value(8),
            ArgumentTypeIds::Lambda {
                parameters: &params,
                result: 9,
            },
        ];
        let function_inputs = [FunctionBindingInput {
            id: u32::MAX,
            source: BindingSource::Scalar(&originals.definitions()[0].1.function),
            arguments: &args,
            result: ResultTypeIds::Scalar(0),
        }];
        let functions =
            encode_function_bindings(&types, &function_inputs, SOURCE, limits(), control).unwrap();
        let inputs = [1, 3, 2, 0].map(|at| aggregate::AggregateBindingInput {
            id: originals.definitions()[at].0,
            source: &originals.definitions()[at].1,
            function_binding_id: u32::MAX,
            intermediate_value_type_id: 8,
        });
        call(&types, &functions, &inputs)
    })
}

#[test]
fn aggregate_caller_chain_retains_four_phases_full_types_and_actual_namespace_loans() {
    let control = Control::default();
    with_sender(&control, |types, functions, inputs| {
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut snapshots = Vec::new();
        let encoded = aggregate::encode_aggregate_bindings_in(
            types,
            functions,
            inputs,
            SOURCE,
            limits(),
            &mut |f| {
                snapshots.push(*f);
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(
            encoded.as_wire().iter().map(|d| d.id).collect::<Vec<_>>(),
            [0, 7, 9, u32::MAX]
        );
        let phases = [
            wire::aggregate_phase::Kind::PartialSequenceId(0),
            wire::aggregate_phase::Kind::IntermediateSequenceId(0),
            wire::aggregate_phase::Kind::Single(Empty {}),
            wire::aggregate_phase::Kind::FinalSequenceId(u32::MAX),
        ];
        for (at, raw) in encoded.as_wire().iter().enumerate() {
            assert_eq!(raw.phase.as_ref().unwrap().kind.as_ref(), Some(&phases[at]));
            assert_eq!(raw.function_binding_id, Some(u32::MAX));
            assert_eq!(raw.intermediate_value_type_id, Some(8));
            assert_eq!(raw.logical_argument_count, if at == 0 { 0 } else { 2 });
            assert_eq!(raw.state_format, "test/aggregate/state-v1");
            assert_eq!(
                raw.state_argument_contract,
                if at == 0 {
                    novarocks_proto_models::common::AggregateStateArgumentContract::ValueRootNullabilityIndependent as i32
                } else {
                    novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature
                        as i32
                }
            );
            let loan = encoded
                .binding_in(raw.id, &mut |_| Ok(()), &mut work)
                .unwrap()
                .unwrap();
            assert!(std::ptr::eq(loan, inputs[at].source));
            assert_eq!(
                encoded
                    .source_id_in(loan, &mut |_| Ok(()), &mut work)
                    .unwrap(),
                raw.id
            );
        }
        assert!(
            snapshots
                .windows(2)
                .all(|p| p[0].cumulative_work_upper_bound <= p[1].cumulative_work_upper_bound)
        );
        work.finish().unwrap();
    });
    let control = Control::default();
    let raw = four_phases();
    with_receivers(&control, &raw, false, |headers, functions| {
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let fresh = aggregate::prepare_aggregate_binding_headers_in(
            &raw,
            headers.functions(),
            AGGREGATE_SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let out = materialize_in(&fresh, functions, limits(), &mut |_| Ok(()), &mut work).unwrap();
        assert!(std::ptr::eq(out.headers(), &fresh));
        assert!(std::ptr::eq(out.functions(), functions));
        assert_eq!(
            out.definitions()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            [u32::MAX, 0, 9, 7]
        );
        for (_, binding) in out.definitions() {
            assert!(binding.function.legacy_metadata.is_none());
        }
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &out.definitions()[0].1.function.argument_types[1]
        else {
            panic!("Lambda");
        };
        assert_eq!(result_type.logical_type(), ValueLogicalType::LargeInt);
        let DataType::Struct(fields) = &parameter_types[1].data_type() else {
            panic!("Struct");
        };
        let DataType::Struct(original) = &fresh
            .functions()
            .type_table()
            .value_type(u32::MAX)
            .unwrap()
            .data_type()
        else {
            panic!("source Struct");
        };
        assert!(Arc::ptr_eq(&fields[0], &original[0]));
        assert!(fields[0].metadata().as_arc().is_none());
        assert!(fields[0].is_nullable());
        assert!(std::ptr::eq(
            out.definition_in(0, &mut |_| Ok(()), &mut work)
                .unwrap()
                .unwrap(),
            &out.definitions()[1].1
        ));
        work.finish().unwrap();
    });
}

#[test]
fn aggregate_caller_owned_layout_is_four_requests_six_refs_and_source_once() {
    let control = Control::default();
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    with_receivers(&control, &raw, false, |headers, functions| {
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let prepared = aggregate::prepare_aggregate_bindings_materialization_in(
            headers,
            functions,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let f = *prepared.facts();
        let request_bytes = 2 * Layout::array::<(u32, AggregateBinding)>(1).unwrap().size()
            + "test/aggregate/state-v1".len()
            + novarocks_type_contract::AggregateStateFormatId::storage_allocation_layout()
                .unwrap()
                .size();
        assert_eq!(f.definition_count, 1);
        assert_eq!(f.type_reference_count, 6);
        assert_eq!(f.allocation_requests_upper_bound, 4);
        assert_eq!(f.request_bytes_upper_bound, request_bytes);
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + request_bytes
        );
        let out =
            aggregate::materialize_aggregate_bindings_in(prepared, &mut |_| Ok(()), &mut work)
                .unwrap();
        assert_eq!(out.facts().request_bytes_upper_bound, request_bytes);
        work.finish().unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        materialize_in(headers, functions, exact(f), &mut |_| Ok(()), &mut work).unwrap();
        work.finish().unwrap();
        for axis in 0..6 {
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            assert!(matches!(
                aggregate::prepare_aggregate_bindings_materialization_in(
                    headers,
                    functions,
                    SOURCE,
                    under(exact(f), axis),
                    &mut |_| Ok(()),
                    &mut work
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
}

#[test]
fn aggregate_known_root_and_dictionary_requests_precede_pending_late_causes() {
    let control = Control::default();
    let raw = [raw_aggregate(
        0,
        wire::aggregate_phase::Kind::Single(Empty {}),
    )];
    with_receivers(&control, &raw, false, |headers, functions| {
        for pending in [254, 255] {
            for cause in CAUSES {
                control.arm(Some((1, cause)));
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let mut cap = limits();
                cap.max_allocation_requests = 1;
                assert!(matches!(
                    aggregate::prepare_aggregate_bindings_materialization_in(
                        headers,
                        functions,
                        SOURCE,
                        cap,
                        &mut |_| Ok(()),
                        &mut work
                    ),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(control.trace(), [0]);
            }
        }
        // An immutable type loan adds no request, even when its source was
        // dictionary encoded. Original control still stops the real clone.
        let value = headers.functions().type_table().value_type(8).unwrap();
        assert_eq!(value.data_type(), &DataType::Utf8);
        for cause in CAUSES {
            control.arm(Some((1, cause.clone())));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut model = MaterializationModel::for_composition(1, 0, SOURCE, 0);
            let mut cap = limits();
            cap.max_allocation_requests = 0;
            cap.max_request_bytes = 0;
            let result = model.count_owned_type_clone_in(value, cap, &mut |_| Ok(()), &mut work);
            assert!(matches!(result, Err(Error::Control(actual)) if actual == cause));
            assert_eq!(model.facts.allocation_requests_upper_bound, 0);
            assert_eq!(model.facts.request_bytes_upper_bound, 0);
            assert_eq!(control.trace(), [0, 255]);
        }
    });
}

fn caller_workflow(
    control: &Control,
    stop: Option<(usize, CompileControlError)>,
    bad: bool,
) -> Result<(), Error> {
    let mut raw = four_phases();
    if bad {
        raw[2].phase = None;
    }
    let definitions = [raw_function(false)];
    let types = types(control);
    let functions =
        prepare_function_binding_headers(&definitions, &types, FUNCTION_SOURCE, limits(), control)?;
    let owned = materialize_function_bindings(prepare_function_bindings_materialization(
        &functions,
        AGGREGATE_SOURCE,
        limits(),
    )?)?;
    control.arm(stop);
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let out = (|| {
        let headers = aggregate::prepare_aggregate_binding_headers_in(
            &raw,
            &functions,
            AGGREGATE_SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        let owned = materialize_in(&headers, &owned, limits(), &mut |_| Ok(()), &mut work)?;
        assert_eq!(owned.definitions().len(), 4);
        owned.definition_in(0, &mut |_| Ok(()), &mut work)?;
        Ok(())
    })();
    finish(out, work)
}
#[test]
fn aggregate_caller_every_success_and_ordinary_callback_preserves_first_cause() {
    prefixes(|c, stop| caller_workflow(c, stop, false), true);
    prefixes(|c, stop| caller_workflow(c, stop, true), false);
    prefixes(
        |c, stop| {
            with_sender(c, |types, functions, inputs| {
                c.arm(stop);
                let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
                let out = aggregate::encode_aggregate_bindings_in(
                    types,
                    functions,
                    inputs,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                finish(out, work)
            })
        },
        true,
    );
    prefixes(
        |c, stop| {
            with_sender(c, |types, functions, inputs| {
                let mut repeated = inputs.to_vec();
                repeated[1].id = repeated[0].id;
                c.arm(stop);
                let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
                let out = aggregate::encode_aggregate_bindings_in(
                    types,
                    functions,
                    &repeated,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                finish(out, work)
            })
        },
        false,
    );
}

#[test]
fn aggregate_foreign_equal_headers_and_replacement_controller_are_not_original_loans() {
    let control = Control::default();
    let raw = four_phases();
    with_receivers(&control, &raw, false, |headers, functions| {
        let foreign_raw = raw.clone();
        let other_functions = prepare_function_binding_headers(
            headers.functions().as_wire(),
            headers.functions().type_table(),
            FUNCTION_SOURCE,
            limits(),
            &control,
        )
        .unwrap();
        let foreign = aggregate::prepare_aggregate_binding_headers(
            &foreign_raw,
            &other_functions,
            AGGREGATE_SOURCE,
            limits(),
        )
        .unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let out = aggregate::prepare_aggregate_bindings_materialization_in(
            &foreign,
            functions,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(finish(out, work), Err(Error::InvalidShape(_))));
        let replacement = Control::default();
        let mut work = CompileCheckpoints::try_new(&replacement, CompilePhase::Decode).unwrap();
        let out = aggregate::prepare_aggregate_bindings_materialization_in(
            headers,
            functions,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(finish(out, work), Err(Error::InvalidShape(_))));
    });
    let control = Control::default();
    with_sender(&control, |types, functions, inputs| {
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let encoded = aggregate::encode_aggregate_bindings_in(
            types,
            functions,
            inputs,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let foreign_equal = inputs[0].source.clone();
        let result = encoded.source_id_in(&foreign_equal, &mut |_| Ok(()), &mut work);
        assert!(matches!(
            finish(result, work),
            Err(Error::InvalidShape(
                "aggregate signature is not an original emitted source"
            ))
        ));
    });
}

#[test]
fn aggregate_wide_real_signature_copies_preserve_order_and_sampled_prefixes() {
    fn wide(c: &Control, stop: Option<(usize, CompileControlError)>) -> Result<(), Error> {
        let raw = [raw_aggregate(
            u32::MAX,
            wire::aggregate_phase::Kind::PartialSequenceId(0),
        )];
        with_receivers(c, &raw, true, |headers, functions| {
            c.arm(stop);
            let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
            let out = (|| {
                let out = materialize_in(
                    headers,
                    functions,
                    BindingProjectionLimits {
                        max_type_references: 200_000,
                        max_allocation_requests: 200_000,
                        max_request_bytes: 64 * 1024 * 1024,
                        max_coexisting_source_and_request_bytes: 128 * 1024 * 1024,
                        max_work: usize::MAX,
                        ..limits()
                    },
                    &mut |_| Ok(()),
                    &mut work,
                )?;
                assert_eq!(out.definitions().len(), 1);
                for (id, binding) in out.definitions() {
                    assert_eq!(*id, u32::MAX);
                    assert_eq!(
                        binding.phase,
                        AggregatePhase::Partial {
                            sequence: AggregateSequenceId::new(0)
                        }
                    );
                    assert_eq!(binding.function.argument_types.len(), 320);
                    assert!(binding.function.argument_types.iter().all(|t| matches!(t, FunctionArgumentType::Value(v) if *v == FunctionValueType::new(DataType::Int64,false))));
                }
                Ok(())
            })();
            finish(out, work)
        })
    }
    let c = Control::default();
    wide(&c, None).unwrap();
    let trace = c.trace();
    // This checks actual owned count loops, not an assumption about opaque
    // Arrow cloning. Emission has more frequent genuine library flushes.
    let quantum = trace
        .iter()
        .position(|n| *n == 256)
        .expect("actual argument count quantum");
    for at in [0, quantum, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let c = Control::default();
            assert!(
                matches!(wide(&c,Some((at,cause.clone()))),Err(Error::Control(actual)) if actual == cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn aggregate_sender_and_header_six_axes_replay_exact_and_refuse_one_under() {
    let c = Control::default();
    with_sender(&c, |types, functions, inputs| {
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let encoded = aggregate::encode_aggregate_bindings_in(
            types,
            functions,
            inputs,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let f = *encoded.facts();
        // Four distinct state strings and one original root Vec. Each state
        // occurrence is copied, even though all strings have equal content.
        assert_eq!(f.definition_count, 4);
        assert_eq!(f.type_reference_count, 4);
        assert_eq!(f.allocation_requests_upper_bound, 5);
        assert_eq!(
            f.request_bytes_upper_bound,
            Layout::array::<wire::AggregateBindingDefinition>(4)
                .unwrap()
                .size()
                + 4 * "test/aggregate/state-v1".len()
        );
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + f.request_bytes_upper_bound
        );
        aggregate::encode_aggregate_bindings_in(
            types,
            functions,
            inputs,
            SOURCE,
            exact(f),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        for axis in 0..6 {
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let result = aggregate::encode_aggregate_bindings_in(
                types,
                functions,
                inputs,
                SOURCE,
                under(exact(f), axis),
                &mut |_| Ok(()),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
    let c = Control::default();
    let raw = four_phases();
    with_receivers(&c, &raw, false, |headers, _| {
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let fresh = aggregate::prepare_aggregate_binding_headers_in(
            &raw,
            headers.functions(),
            AGGREGATE_SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let f = *fresh.facts();
        assert_eq!(f.definition_count, 4);
        assert_eq!(f.type_reference_count, 4);
        assert_eq!(f.allocation_requests_upper_bound, 1);
        assert_eq!(
            f.request_bytes_upper_bound,
            Layout::array::<usize>(4).unwrap().size()
        );
        aggregate::prepare_aggregate_binding_headers_in(
            &raw,
            headers.functions(),
            AGGREGATE_SOURCE,
            exact(f),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        for axis in 0..6 {
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            assert!(matches!(
                aggregate::prepare_aggregate_binding_headers_in(
                    &raw,
                    headers.functions(),
                    AGGREGATE_SOURCE,
                    under(exact(f), axis),
                    &mut |_| Ok(()),
                    &mut work
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
}

#[test]
fn aggregate_actual_captured_signature_prefixes_admit_before_completed_lookup() {
    let c = Control::default();
    let raw = four_phases();
    with_receivers(&c, &raw, false, |headers, functions| {
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            // The original controller identity comparison consumes one step;
            // the matched source must be captured before its next comparison.
            for _ in 0..254 {
                work.step().unwrap();
            }
            let mut seen = 0;
            let result = functions.definition_captured(
                u32::MAX,
                &mut |source, work| {
                    seen += 1;
                    let crate::physical_binding_v2::MaterializedFunctionBinding::Scalar(binding) =
                        source
                    else {
                        panic!("scalar");
                    };
                    let mut model = MaterializationModel::for_composition(1, 0, SOURCE, 0);
                    let mut cap = limits();
                    cap.max_type_references = 0;
                    crate::physical_binding_v2::preflight_scalar_signature_copy_in(
                        binding,
                        &mut model,
                        cap,
                        &mut |_| Ok(()),
                        work,
                    )
                },
                &mut work,
            );
            assert_eq!(seen, 1);
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
        }
        let source = functions
            .definitions()
            .iter()
            .find_map(|(_, s)| match s {
                crate::physical_binding_v2::MaterializedFunctionBinding::Scalar(s) => Some(s),
                _ => None,
            })
            .unwrap();
        let equal_foreign = source.clone();
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let mut bound = 0;
        let receipt = crate::physical_binding_v2::verify_scalar_signature_admitted(
            source,
            &equal_foreign,
            SOURCE,
            limits().max_work,
            &mut |next| {
                assert!(next >= bound);
                bound = next;
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert!(receipt.matches());
        assert_eq!(receipt.work_upper_bound(), bound);
        work.finish().unwrap();
        let baseline = c.trace();
        for at in 0..baseline.len() {
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                let mut work = match CompileCheckpoints::try_new(&c, CompilePhase::Decode) {
                    Ok(w) => w,
                    Err(actual) => {
                        assert_eq!(actual, cause);
                        assert_eq!(at, 0);
                        continue;
                    }
                };
                let result = crate::physical_binding_v2::verify_scalar_signature_admitted(
                    source,
                    &equal_foreign,
                    SOURCE,
                    limits().max_work,
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                assert!(matches!(finish(result,work),Err(Error::Control(actual)) if actual==cause));
                assert_eq!(c.trace(), baseline[..=at]);
            }
        }
        // The header namespace is original and remains borrowed throughout.
        assert!(std::ptr::eq(functions.headers(), headers.functions()));
    });
}

#[test]
fn aggregate_actual_empty_namespace_invoice_overflow_precedes_pending_control() {
    let control = Control::default();
    let types = types(&control);
    let unlimited = BindingProjectionLimits {
        max_definitions: usize::MAX,
        max_type_references: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    // The genuine empty original header accepts this source invoice: it has
    // no index allocation. Its separately retained token still costs bytes.
    let huge_functions =
        prepare_function_binding_headers(&[], &types, usize::MAX, unlimited, &control).unwrap();
    assert!(matches!(
        huge_functions.retained_invoice_floor(),
        Err(Error::InvalidShape(_))
    ));
    for pending in [254, 255] {
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut admissions = 0;
            let result = aggregate::prepare_aggregate_binding_headers_in(
                &[],
                &huge_functions,
                usize::MAX,
                unlimited,
                &mut |_| {
                    admissions += 1;
                    Ok(())
                },
                &mut work,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(admissions, 0);
            assert_eq!(control.trace(), [0]);
        }
    }
    control.arm(None);
    assert!(matches!(
        aggregate::prepare_aggregate_binding_headers(&[], &huge_functions, usize::MAX, unlimited,),
        Err(Error::InvalidShape(_))
    ));
    // Build real, separately owned empty function output with a finite invoice.
    control.0.lock().unwrap().active = false;
    let functions_headers =
        prepare_function_binding_headers(&[], &types, FUNCTION_SOURCE, unlimited, &control)
            .unwrap();
    let functions = materialize_function_bindings(
        prepare_function_bindings_materialization(&functions_headers, SOURCE, unlimited).unwrap(),
    )
    .unwrap();
    assert!(functions.retained_output_floor().unwrap() > 0);
    for invoice in [
        usize::MAX,
        usize::MAX - size_of::<PreparedAggregateBindingHeaders<'_, '_>>(),
    ] {
        let headers = aggregate::prepare_aggregate_binding_headers(
            &[],
            &functions_headers,
            invoice,
            unlimited,
        )
        .unwrap();
        if invoice == usize::MAX {
            assert!(matches!(
                headers.retained_invoice_floor(),
                Err(Error::InvalidShape(_))
            ));
        } else {
            assert_eq!(headers.retained_invoice_floor().unwrap(), usize::MAX);
        }
        for pending in [254, 255] {
            for cause in CAUSES {
                control.arm(Some((1, cause)));
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let mut admissions = 0;
                let result = aggregate::prepare_aggregate_bindings_materialization_in(
                    &headers,
                    &functions,
                    usize::MAX,
                    unlimited,
                    &mut |_| {
                        admissions += 1;
                        Ok(())
                    },
                    &mut work,
                );
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(admissions, 0);
                assert_eq!(control.trace(), [0]);
            }
        }
        control.0.lock().unwrap().active = false;
    }
}
