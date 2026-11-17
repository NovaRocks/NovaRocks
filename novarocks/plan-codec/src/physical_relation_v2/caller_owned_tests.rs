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
use std::alloc::Layout;

fn monotone(previous: &mut Option<RelationProjectionFacts>, next: &RelationProjectionFacts) {
    if let Some(previous) = previous {
        assert_eq!(previous.definition_count, next.definition_count);
        assert!(previous.schema_field_count <= next.schema_field_count);
        assert!(previous.predicate_guarantee_count <= next.predicate_guarantee_count);
        assert!(previous.metadata_kind_bytes <= next.metadata_kind_bytes);
        assert!(previous.coverage_bytes <= next.coverage_bytes);
        assert!(previous.allocation_requests_upper_bound <= next.allocation_requests_upper_bound);
        assert!(
            previous.allocation_request_bytes_upper_bound
                <= next.allocation_request_bytes_upper_bound
        );
        assert!(
            previous.coexisting_source_and_request_bytes_upper_bound
                <= next.coexisting_source_and_request_bytes_upper_bound
        );
        assert!(previous.cumulative_work_upper_bound <= next.cumulative_work_upper_bound);
    }
    *previous = Some(*next);
}

#[test]
fn relation_caller_owned_hand_wire_layout_and_original_namespace_loans() {
    let control = Control::default();
    with_sources(&control, |sources, reads, types| {
        let ids = [0, u32::MAX, 42];
        let input = inputs(sources, &ids);
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
        let mut last = None;
        assert!(
            std::ptr::addr_eq(reads.original_control(), work.control()),
            "exact controller address must match"
        );
        let encoded = encode_relations_in(
            &input,
            reads,
            types,
            SOURCE,
            limits(),
            &mut |next| {
                monotone(&mut last, next);
                Ok(())
            },
            &mut work,
        )?;
        assert_eq!(encoded.as_wire(), [expected(0), expected(1)]);
        assert!(std::ptr::eq(encoded.reads(), reads));
        assert!(std::ptr::eq(encoded.types(), types));
        assert!(std::ptr::eq(
            encoded.relation_in(0, &mut work)?.unwrap(),
            &sources[1]
        ));
        assert_eq!(encoded.source_id_in(&sources[0], &mut work)?, u32::MAX);
        let foreign = sources[0].clone();
        assert!(matches!(
            encoded.source_id_in(&foreign, &mut work),
            Err(Error::InvalidShape(_))
        ));
        let kind_bytes = "files / ✓".len();
        // Two root Vecs, Data three payloads plus ordering, Metadata five
        // payloads plus ordering. Shared FieldArcs are not deep copies.
        let expected_bytes = Layout::array::<usize>(2).unwrap().size()
            + Layout::array::<wire::RelationDefinition>(2).unwrap().size()
            + 2 * (Layout::array::<wire::RelationField>(3).unwrap().size()
                + Layout::array::<wire::PredicateGuarantee>(3).unwrap().size()
                + 32
                + Layout::array::<wire::OrderingKey>(2).unwrap().size())
            + kind_bytes
            + 3;
        assert_eq!(encoded.facts().allocation_requests_upper_bound, 12);
        assert_eq!(
            encoded.facts().allocation_request_bytes_upper_bound,
            expected_bytes
        );
        work.finish()?;
        let bindings = decode_provider_bindings(
            reads.bindings().as_wire(),
            PRIOR,
            binding_limits(),
            &control,
        )
        .unwrap();
        let payloads = decode_connector_payloads(
            reads.payloads().as_wire(),
            PRIOR,
            payload_limits(),
            &control,
        )
        .unwrap();
        let receiving_reads = decode_provider_reads(
            reads.as_wire(),
            &bindings,
            &payloads,
            256 * 1024,
            read_limits(),
        )
        .unwrap();
        let receiving_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
        let raw = [expected(0), expected(1)];
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
        let mut last = None;
        let decoded = decode_relations_in(
            &raw,
            &receiving_reads,
            &receiving_types,
            SOURCE,
            limits(),
            &mut |next| {
                monotone(&mut last, next);
                Ok(())
            },
            &mut work,
        )?;
        assert_eq!(decoded.relation_in(u32::MAX, &mut work)?, Some(&sources[0]));
        assert_eq!(decoded.relation_in(0, &mut work)?, Some(&sources[1]));
        assert!(std::ptr::eq(decoded.as_wire(), raw.as_slice()));
        let source = receiving_types.value_type(42).unwrap();
        let actual = &decoded.relation_in(0, &mut work)?.unwrap().schema()[2].ty;
        match (&source.data_type(), &actual.data_type()) {
            (DataType::Struct(a), DataType::Struct(b)) => assert!(Arc::ptr_eq(&a[0], &b[0])),
            _ => panic!("complete Struct source lost"),
        }
        // Per occurrence: schema Vec/Box, guarantees Vec/Box, ordering
        // Vec/Box and five promotable Shared ceilings (3 columns +
        // table/view). Type handles share backing; Metadata adds String+coverage2.
        assert_eq!(decoded.facts().allocation_requests_upper_bound, 27);
        let expected_bytes = Layout::array::<usize>(2).unwrap().size()
            + Layout::array::<p::Relation>(2).unwrap().size()
            + 2 * (2 * Layout::array::<p::RelationField>(3).unwrap().size()
                + 2 * Layout::array::<p::PredicateGuarantee>(3).unwrap().size()
                + 2 * Layout::array::<p::OrderingKey>(2).unwrap().size())
            + kind_bytes
            + 6
            + 10 * bytes_shared_upper()?;
        assert_eq!(
            decoded.facts().allocation_request_bytes_upper_bound,
            expected_bytes
        );
        assert_eq!(
            decoded
                .facts()
                .coexisting_source_and_request_bytes_upper_bound,
            SOURCE + expected_bytes
        );
        work.finish()?;
        Ok(())
    })
    .unwrap();
}

fn run_in(
    control: &Control,
    stop: Option<(usize, CompileControlError)>,
    decode: bool,
    bad: bool,
) -> Result<(), Error> {
    with_sources(control, |sources, reads, types| {
        if decode {
            let bindings = decode_provider_bindings(
                reads.bindings().as_wire(),
                PRIOR,
                binding_limits(),
                control,
            )
            .unwrap();
            let payloads = decode_connector_payloads(
                reads.payloads().as_wire(),
                PRIOR,
                payload_limits(),
                control,
            )
            .unwrap();
            let reads = decode_provider_reads(
                reads.as_wire(),
                &bindings,
                &payloads,
                256 * 1024,
                read_limits(),
            )
            .unwrap();
            let types = decode_type_table(types.as_wire(), type_limits(), control).unwrap();
            let mut raw = expected(1);
            if bad && let Some(wire::relation_definition::Kind::Metadata(metadata)) = &mut raw.kind
            {
                metadata.kind.clear();
            }
            let definitions = [raw];
            control.arm(stop.clone());
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
            let result = decode_relations_in(
                &definitions,
                &reads,
                &types,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            finish(result, work)
        } else {
            let ids = if bad {
                [u32::MAX, u32::MAX, 42]
            } else {
                [0, u32::MAX, 42]
            };
            let input = inputs(sources, &ids);
            control.arm(stop.clone());
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = encode_relations_in(
                &input,
                reads,
                types,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            finish(result, work)
        }
    })
}

#[test]
fn relation_caller_owned_every_actual_success_and_ordinary_footer_prefix_three_causes() {
    for decode in [false, true] {
        for bad in [false, true] {
            let control = Control::default();
            let result = run_in(&control, None, decode, bad);
            assert_eq!(result.is_ok(), !bad);
            let baseline = trace(&control);
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    let control = Control::default();
                    assert!(
                        matches!(run_in(&control, Some((at, cause.clone())), decode, bad), Err(Error::Control(actual)) if actual == cause)
                    );
                    assert_eq!(trace(&control), baseline[..=at]);
                }
            }
        }
    }
}

#[test]
fn relation_caller_owned_all_nine_axes_exact_and_one_under_original_source() {
    let control = Control::default();
    with_sources(&control, |sources, reads, types| {
        let ids = [0, u32::MAX, 42];
        let input = inputs(sources, &ids);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
        let encoded = encode_relations_in(
            &input,
            reads,
            types,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        let bounded = exact(encoded.facts());
        work.finish()?;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
        encode_relations_in(
            &input,
            reads,
            types,
            SOURCE,
            bounded,
            &mut |_| Ok(()),
            &mut work,
        )?;
        work.finish()?;
        for axis in 0..9 {
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
            let result = encode_relations_in(
                &input,
                reads,
                types,
                SOURCE,
                under(bounded, axis),
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        let bindings = decode_provider_bindings(
            reads.bindings().as_wire(),
            PRIOR,
            binding_limits(),
            &control,
        )
        .unwrap();
        let payloads = decode_connector_payloads(
            reads.payloads().as_wire(),
            PRIOR,
            payload_limits(),
            &control,
        )
        .unwrap();
        let reads = decode_provider_reads(
            reads.as_wire(),
            &bindings,
            &payloads,
            256 * 1024,
            read_limits(),
        )
        .unwrap();
        let types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
        let definitions = [expected(0), expected(1)];
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
        let decoded = decode_relations_in(
            &definitions,
            &reads,
            &types,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        let bounded = exact(decoded.facts());
        work.finish()?;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
        decode_relations_in(
            &definitions,
            &reads,
            &types,
            SOURCE,
            bounded,
            &mut |_| Ok(()),
            &mut work,
        )?;
        work.finish()?;
        for axis in 0..9 {
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
            let result = decode_relations_in(
                &definitions,
                &reads,
                &types,
                SOURCE,
                under(bounded, axis),
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn relation_caller_owned_known_root_and_dictionary_requests_precede_late_control() {
    let control = Control::default();
    with_sources(&control, |sources, reads, types| {
        let ids = [0, u32::MAX, 42];
        let input = inputs(sources, &ids);
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
            for _ in 0..255 {
                work.step()?;
            }
            let mut bounded = limits();
            bounded.max_allocation_requests = 0;
            let result = encode_relations_in(
                &input,
                reads,
                types,
                SOURCE,
                bounded,
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(trace(&control), [(CompilePhase::Encode, 0)]);
        }
        control.arm(None);
        let dictionary = &sources[0].schema()[1].ty;
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
            for _ in 0..255 {
                work.step()?;
            }
            let mut bounded = limits();
            bounded.max_work = 0;
            let mut admission = |_: &RelationProjectionFacts| Ok(());
            let mut model = Model {
                admission: Some(Admission {
                    definitions: 1,
                    source: SOURCE,
                    limits: bounded,
                    callback: &mut admission,
                }),
                ..Model::default()
            };
            // The original two-handle clone work is admitted before any
            // pending observation, without predicting Dictionary boxes.
            let result = model.clone_prefix(dictionary, &mut work, true).map(|_| ());
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(trace(&control), [(CompilePhase::Decode, 0)]);
        }
        control.arm(None);
        Ok(())
    })
    .unwrap();
}

fn run_selected(
    control: &Control,
    stop: Option<(usize, CompileControlError)>,
    missing: bool,
) -> Result<(), Error> {
    with_sources(control, |sources, reads, types| {
        let bindings =
            decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), control)
                .unwrap();
        let payloads =
            decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), control)
                .unwrap();
        let reads = decode_provider_reads(
            reads.as_wire(),
            &bindings,
            &payloads,
            256 * 1024,
            read_limits(),
        )
        .unwrap();
        let types = decode_type_table(types.as_wire(), type_limits(), control).unwrap();
        let definitions = [expected(0), expected(1)];
        let namespace = decode_relations(&definitions, &reads, &types, SOURCE, limits())?;
        control.arm(stop.clone());
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let mut last = None;
        let mut admit = |next: &RelationProjectionFacts| {
            monotone(&mut last, next);
            Ok(())
        };
        let result = prepare_relation_materialization_in(
            &namespace,
            if missing { 19 } else { 0 },
            2 * SOURCE,
            limits(),
            &mut admit,
            &mut work,
        )
        .and_then(|prepared| {
            assert_eq!(prepared.source_id(), 0);
            prepared.emit_in(&mut admit, &mut work)
        })
        .map(|relation| {
            assert_eq!(relation, sources[1]);
            match (
                &namespace.relations[1].schema()[2].ty.data_type(),
                &relation.schema()[2].ty.data_type(),
            ) {
                (DataType::Struct(a), DataType::Struct(b)) => assert!(Arc::ptr_eq(&a[0], &b[0])),
                _ => panic!("selected full Struct carrier changed"),
            }
        });
        finish(result, work)
    })
}

#[test]
fn relation_selected_materialization_uses_original_loans_and_every_control_prefix() {
    for missing in [false, true] {
        let control = Control::default();
        let result = run_selected(&control, None, missing);
        assert_eq!(result.is_ok(), !missing);
        let baseline = trace(&control);
        for at in 0..baseline.len() {
            for cause in CAUSES {
                let control = Control::default();
                assert!(
                    matches!(run_selected(&control, Some((at, cause.clone())), missing), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), baseline[..=at]);
            }
        }
    }
}

#[test]
fn relation_caller_owned_actual_wide_guarantees_keep_order_and_sample_real_quantum() {
    let control = Control::default();
    with_source_mode(&control, true, |sources, reads, types| {
        let input = (0..8)
            .map(|ordinal| RelationSource {
                id: u32::MAX - ordinal,
                relation: &sources[0],
                value_type_ids: &[],
            })
            .collect::<Vec<_>>();
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
        let encoded = encode_relations_in(
            &input,
            reads,
            types,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        assert_eq!(encoded.facts().predicate_guarantee_count, 2560);
        for (ordinal, definition) in encoded.as_wire().iter().enumerate() {
            assert_eq!(definition.id, u32::MAX - ordinal as u32);
            let Some(wire::relation_definition::Kind::Data(data)) = &definition.kind else {
                panic!("original Data kind changed");
            };
            assert_eq!(data.schema.len(), 0);
            for (ordinal, guarantee) in data.predicate_guarantees.iter().enumerate() {
                assert_eq!(guarantee.predicate_root, Some(u32::MAX - ordinal as u32));
                assert_eq!(guarantee.kind, wire::PredicateGuaranteeKind::Exact as i32);
            }
        }
        work.finish()?;
        let baseline = trace(&control);
        let quantum = baseline
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("actual guarantee walk quantum");
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause.clone())));
                let mut work = match CompileCheckpoints::try_new(&control, CompilePhase::Encode) {
                    Ok(work) => work,
                    Err(actual) => {
                        assert_eq!(actual, cause);
                        assert_eq!(trace(&control), baseline[..=at]);
                        continue;
                    }
                };
                let result = encode_relations_in(
                    &input,
                    reads,
                    types,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                assert!(
                    matches!(finish(result, work), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), baseline[..=at]);
            }
        }
        control.arm(None);
        let bindings = decode_provider_bindings(
            reads.bindings().as_wire(),
            PRIOR,
            binding_limits(),
            &control,
        )
        .unwrap();
        let payloads = decode_connector_payloads(
            reads.payloads().as_wire(),
            PRIOR,
            payload_limits(),
            &control,
        )
        .unwrap();
        let reads = decode_provider_reads(
            reads.as_wire(),
            &bindings,
            &payloads,
            256 * 1024,
            read_limits(),
        )
        .unwrap();
        let types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
        let decoded = decode_relations_in(
            encoded.as_wire(),
            &reads,
            &types,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        for (actual, source) in decoded.relations.iter().zip(&input) {
            assert_eq!(actual, source.relation);
        }
        work.finish()?;
        let baseline = trace(&control);
        let quantum = baseline
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("actual receiving guarantee walk quantum");
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause.clone())));
                let mut work = match CompileCheckpoints::try_new(&control, CompilePhase::Decode) {
                    Ok(work) => work,
                    Err(actual) => {
                        assert_eq!(actual, cause);
                        assert_eq!(trace(&control), baseline[..=at]);
                        continue;
                    }
                };
                let result = decode_relations_in(
                    encoded.as_wire(),
                    &reads,
                    &types,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                assert!(
                    matches!(finish(result, work), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), baseline[..=at]);
            }
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn relation_receiver_actual_ordering_request_beats_late_control() {
    let control = Control::default();
    with_sources(&control, |_, reads, types| {
        let bindings = decode_provider_bindings(
            reads.bindings().as_wire(),
            PRIOR,
            binding_limits(),
            &control,
        )
        .unwrap();
        let payloads = decode_connector_payloads(
            reads.payloads().as_wire(),
            PRIOR,
            payload_limits(),
            &control,
        )
        .unwrap();
        let reads = decode_provider_reads(
            reads.as_wire(),
            &bindings,
            &payloads,
            256 * 1024,
            read_limits(),
        )
        .unwrap();
        let types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
        let definitions = [expected(0)];
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
        let mut captured_at = None;
        decode_relations_in(
            &definitions,
            &reads,
            &types,
            SOURCE,
            limits(),
            &mut |facts| {
                // One root index+output, schema2, guarantees2, five Shared
                // ceilings, ordering2 = 13. Type-handle clones add no requests.
                if facts.allocation_requests_upper_bound == 13 && captured_at.is_none() {
                    captured_at = Some(trace(&control).len());
                }
                Ok(())
            },
            &mut work,
        )?;
        work.finish()?;
        let baseline = trace(&control);
        let at = captured_at.expect("actual ordering container request snapshot");
        assert!(at < baseline.len());
        for cause in CAUSES {
            control.arm(Some((at, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
            let mut bounded = limits();
            bounded.max_allocation_requests = 12;
            let result = decode_relations_in(
                &definitions,
                &reads,
                &types,
                SOURCE,
                bounded,
                &mut |_| Ok(()),
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(trace(&control), baseline[..at]);
        }
        control.arm(None);
        Ok(())
    })
    .unwrap();
}

#[test]
fn relation_numeric_policy_keeps_plain_shape_but_in_resource_without_observation() {
    let mut admit = |_: &RelationProjectionFacts| Ok(());
    let observed = Model {
        admission: Some(Admission {
            definitions: 1,
            source: SOURCE,
            limits: limits(),
            callback: &mut admit,
        }),
        ..Model::default()
    };
    for result in [
        observed.sum(usize::MAX, 1),
        observed.product(usize::MAX, 2),
        observed.buffer_bytes::<u64>(usize::MAX),
    ] {
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
    }
    let plain = Model::default();
    for result in [
        plain.sum(usize::MAX, 1),
        plain.product(usize::MAX, 2),
        plain.buffer_bytes::<u64>(usize::MAX),
    ] {
        assert!(matches!(result, Err(Error::InvalidShape(_))));
    }
}
