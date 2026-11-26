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

fn run_owned(
    raw: &wire::FragmentCallRequests,
    types: &DecodedTypeTable,
    pools: &ConstantPools,
    source: usize,
    limits: Limits,
    c: &Control,
    snapshots: &mut Vec<Facts>,
) -> Result<Vec<(PhysicalCallSite, PhysicalCallRequest)>, E> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = (|| {
        let mut admit = |facts: &Facts| {
            snapshots.push(*facts);
            Ok(())
        };
        let token = prepare_call_requests_decode_in(
            Some(raw),
            types,
            pools,
            source,
            limits,
            &mut admit,
            &mut work,
        )?;
        decode_call_requests_in(token, &mut admit, &mut work)
    })();
    finish(work, result)
}
fn exact(f: Facts) -> Limits {
    Limits {
        max_definitions: f.definition_count,
        max_type_references: f.type_reference_count,
        max_request_bytes: f.request_bytes_upper_bound,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn below(f: Facts, axis: usize) -> Limits {
    let mut l = exact(f);
    let bound = match axis {
        0 => &mut l.max_definitions,
        1 => &mut l.max_type_references,
        2 => &mut l.max_request_bytes,
        3 => &mut l.max_allocation_requests,
        4 => &mut l.max_coexisting_source_and_request_bytes,
        _ => &mut l.max_work,
    };
    assert!(*bound > 0);
    *bound -= 1;
    l
}

#[test]
fn caller_request_receiver_retains_original_none_null_lambda_constraint_and_sparse_order() {
    let t = types();
    let p = pools();
    let mut s = source();
    s.entries.push(request(0));
    let mut snapshots = Vec::new();
    let output = run_owned(
        &s,
        &t,
        &p,
        SOURCE,
        limits(),
        &Control::default(),
        &mut snapshots,
    )
    .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].0, window_site(u32::MAX));
    assert_eq!(output[1].0, window_site(0));
    let request = &output[0].1;
    assert_eq!(request.logical_argument_count, 2);
    assert_eq!(
        request.expected_result_type.as_ref(),
        t.value_type(u32::MAX)
    );
    assert!(
        matches!(&request.arguments[0], StaticFunctionArgument::Value { value_type, constant: None } if value_type==t.value_type(0).unwrap())
    );
    let StaticFunctionArgument::Value {
        value_type,
        constant: Some(address),
    } = &request.arguments[1]
    else {
        panic!("original Some channel is absent")
    };
    assert_eq!(
        *address,
        ConstantReference {
            pool: ConstantPoolId::new(7),
            ordinal: 1
        }
    );
    let mut lookup = CompileCheckpoints::try_new(&Setup, CompilePhase::Validate).unwrap();
    let cv = p
        .resolve_observed(*address, value_type, &mut lookup)
        .unwrap();
    lookup.finish().unwrap();
    let original = p
        .entries()
        .get(&ConstantPoolId::new(7))
        .unwrap()
        .value(1)
        .unwrap();
    assert_eq!(
        cv.pool().backing_identity(),
        original.pool().backing_identity()
    );
    assert!(Arc::ptr_eq(
        cv.pool().field_ref(),
        original.pool().field_ref()
    ));
    assert_eq!(cv.ordinal(), 1);
    let StaticFunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = &request.arguments[2]
    else {
        panic!("original Lambda channel is absent")
    };
    assert_eq!(parameter_types.len(), 4);
    for (actual, id) in parameter_types.iter().zip([u32::MAX, 42, 8, 0]) {
        assert_eq!(actual, t.value_type(id).unwrap());
    }
    let DataType::Struct(actual) = &parameter_types[0].data_type() else {
        panic!("nested source carrier lost")
    };
    let DataType::Struct(original) = &t.value_type(u32::MAX).unwrap().data_type() else {
        panic!("source fixture lost")
    };
    assert!(Arc::ptr_eq(&actual[0], &original[0]));
    assert_eq!(result_type, t.value_type(7).unwrap());
    assert!(output[1].1.arguments.is_empty());
    assert!(output[1].1.expected_result_type.is_none());
    assert!(snapshots.windows(2).all(|pair| {
        let (a, b) = (pair[0], pair[1]);
        a.allocation_requests_upper_bound <= b.allocation_requests_upper_bound
            && a.request_bytes_upper_bound <= b.request_bytes_upper_bound
            && a.cumulative_work_upper_bound <= b.cumulative_work_upper_bound
    }));
}

#[test]
fn caller_request_receiver_final_facts_replay_all_six_axes_and_dictionary_layout() {
    let t = types();
    let p = pools();
    let s = source();
    let mut snapshots = Vec::new();
    run_owned(
        &s,
        &t,
        &p,
        SOURCE,
        limits(),
        &Control::default(),
        &mut snapshots,
    )
    .unwrap();
    let final_facts = *snapshots.last().unwrap();
    assert_eq!(final_facts.definition_count, 1);
    assert_eq!(final_facts.type_reference_count, 8);
    // One index/output Vec, argument Vec->Box, Lambda Vec->Box and its
    // guarded collection Arc. The normalized FVT owners are cheap clones.
    assert_eq!(final_facts.allocation_requests_upper_bound, 7);
    let expected = Layout::array::<PhysicalCallSite>(1).unwrap().size()
        + Layout::array::<(PhysicalCallSite, PhysicalCallRequest)>(1)
            .unwrap()
            .size()
        + 2 * Layout::array::<StaticFunctionArgument>(3).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(4).unwrap().size()
        + novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout()
            .unwrap()
            .size();
    assert_eq!(final_facts.request_bytes_upper_bound, expected);
    assert_eq!(
        final_facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + size_of::<PreparedCallRequestsDecode<'_>>() + expected
    );
    let mut replay = Vec::new();
    run_owned(
        &s,
        &t,
        &p,
        SOURCE,
        exact(final_facts),
        &Control::default(),
        &mut replay,
    )
    .unwrap();
    assert_eq!(replay, snapshots);
    for axis in 0..6 {
        assert!(matches!(
            run_owned(
                &s,
                &t,
                &p,
                SOURCE,
                below(final_facts, axis),
                &Control::default(),
                &mut Vec::new()
            ),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
}

#[test]
fn caller_request_receiver_root_and_captured_dictionary_numeric_refusal_precedes_late_control() {
    let t = types();
    let p = ConstantPools::empty();
    let mut entry = request(0);
    entry.arguments = vec![value_argument(42, None)];
    entry.logical_argument_count = Some(1);
    let s = wire::FragmentCallRequests {
        entries: vec![entry],
    };
    for pending in [254, 255] {
        for cause in CAUSES {
            let c = Control {
                stop: Some((1, cause)),
                ..Control::default()
            };
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut l = limits();
            l.max_allocation_requests = 0;
            assert!(matches!(
                prepare_call_requests_decode_in(
                    Some(&s),
                    &t,
                    &p,
                    SOURCE,
                    l,
                    &mut |_| Ok(()),
                    &mut work
                ),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*c.trace.lock().unwrap(), vec![(CompilePhase::Decode, 0)]);
        }
    }
    // Captured execution bindings share both immutable owners. A zero new
    // allocation cap permits the clone; the real pending control still wins.
    for cause in CAUSES {
        let c = Control {
            stop: Some((1, cause.clone())),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut model = Model {
            facts: Facts {
                definition_count: 0,
                type_reference_count: 1,
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                coexisting_source_and_request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0,
            },
            arguments: 0,
            parameters: 0,
            comparisons: 0,
            known: 0,
            maximum_type_backing: 0,
            source: SOURCE,
            types: 0,
            pools: 0,
            raw_work: 0,
            peak_work: 0,
            output_map_work: 0,
        };
        let mut l = limits();
        l.max_request_bytes = 0;
        assert!(matches!(
            model.value(
                CallRequestTypeOwner::Package(&t),
                42,
                l.into(),
                &mut Some(&mut |_| Ok(())),
                &mut work
            ),
            Err(E::Control(actual)) if actual == cause
        ));
        assert_eq!(model.facts.allocation_requests_upper_bound, 0);
        assert_eq!(model.facts.request_bytes_upper_bound, 0);
        assert_eq!(
            *c.trace.lock().unwrap(),
            vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 256)]
        );
    }
}

#[test]
fn caller_request_receiver_every_success_and_original_ordinary_callback_has_primary_control() {
    let t = types();
    let p = pools();
    let s = source();
    assert_causes(|c| run_owned(&s, &t, &p, SOURCE, limits(), c, &mut Vec::new()).map(|_| ()));
    for case in 0..4 {
        let mut s = source();
        match case {
            0 => s.entries[0].logical_argument_count = None,
            1 => {
                s.entries[0].arguments[1] = value_argument(
                    0,
                    Some(wire::ConstantReference {
                        pool_id: Some(99),
                        row_ordinal: 1,
                    }),
                )
            }
            2 => {
                s.entries[0].arguments[1] = value_argument(
                    0,
                    Some(wire::ConstantReference {
                        pool_id: Some(7),
                        row_ordinal: 2,
                    }),
                )
            }
            _ => {
                s.entries[0].arguments[1] = value_argument(
                    7,
                    Some(wire::ConstantReference {
                        pool_id: Some(7),
                        row_ordinal: 1,
                    }),
                )
            }
        }
        let ordinary = run_owned(
            &s,
            &t,
            &p,
            SOURCE,
            limits(),
            &Control::default(),
            &mut Vec::new(),
        );
        match case {
            0 => assert!(matches!(ordinary, Err(E::InvalidShape(_)))),
            1 => assert!(matches!(
                ordinary,
                Err(E::Constant(
                    novarocks_physical_plan::ConstantReferenceError::Table(
                        novarocks_constant_contract::ConstantTableError::MissingPool(_)
                    )
                ))
            )),
            2 => assert!(matches!(
                ordinary,
                Err(E::Constant(
                    novarocks_physical_plan::ConstantReferenceError::Constant(_)
                ))
            )),
            _ => assert!(matches!(
                ordinary,
                Err(E::Constant(
                    novarocks_physical_plan::ConstantReferenceError::Table(
                        novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(_)
                    )
                ))
            )),
        }
        assert_causes(|c| run_owned(&s, &t, &p, SOURCE, limits(), c, &mut Vec::new()).map(|_| ()));
    }
}

#[test]
fn caller_request_receiver_emit_refuses_foreign_controller_before_parent_and_reserve() {
    let t = types();
    let p = pools();
    let s = source();
    let original = Control::default();
    let foreign = Control::default();
    let mut first = CompileCheckpoints::try_new(&original, CompilePhase::Decode).unwrap();
    let token = prepare_call_requests_decode_in(
        Some(&s),
        &t,
        &p,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut first,
    )
    .unwrap();
    first.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    let mut called = false;
    let result = decode_call_requests_in(
        token,
        &mut |_| {
            called = true;
            Ok(())
        },
        &mut work,
    );
    assert!(matches!(result, Err(E::InvalidShape(_))));
    assert!(!called);
    assert_eq!(
        *foreign.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0)]
    );
}

#[test]
fn caller_request_receiver_wide_actual_index_quantum_and_complete_emit_admission() {
    let t = types();
    let p = ConstantPools::empty();
    let s = wire::FragmentCallRequests {
        entries: (0..320).rev().map(request).collect(),
    };
    let c = Control::default();
    let mut snapshots = Vec::new();
    let output = run_owned(&s, &t, &p, SOURCE, limits(), &c, &mut snapshots).unwrap();
    assert_eq!(output.len(), 320);
    for (actual, expected) in output.iter().zip((0..320).rev()) {
        assert_eq!(actual.0, window_site(expected));
        assert!(actual.1.arguments.is_empty());
    }
    let baseline = c.trace.lock().unwrap().clone();
    let quantum = baseline
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual request/index author did not reach quantum");
    for at in [0, quantum, baseline.len() - 1] {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(run_owned(&s,&t,&p,SOURCE,limits(),&c,&mut Vec::new()),Err(E::Control(actual)) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), baseline[..=at]);
        }
    }
    // Emission admits the entire immutable prepared contribution before its
    // first reserve. Rejecting it cannot produce a partial output Vec.
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let token = prepare_call_requests_decode_in(
        Some(&s),
        &t,
        &p,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut w,
    )
    .unwrap();
    let before = c.trace.lock().unwrap().clone();
    assert!(matches!(
        decode_call_requests_in(
            token,
            &mut |_| Err(CompileControlError::ResourceExhausted),
            &mut w
        ),
        Err(E::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(*c.trace.lock().unwrap(), before);
}

#[test]
fn caller_request_receiver_actual_cv_comparison_numeric_prefix_precedes_lookup_completion() {
    let t = types();
    let p = pools();
    let mut entry = request(0);
    entry.logical_argument_count = Some(1);
    entry.arguments = vec![value_argument(0, None)];
    let mut raw = wire::FragmentCallRequests {
        entries: vec![entry],
    };
    let mut without_comparison = Vec::new();
    run_owned(
        &raw,
        &t,
        &p,
        SOURCE,
        limits(),
        &Control::default(),
        &mut without_comparison,
    )
    .unwrap();
    let raw_work = without_comparison
        .last()
        .unwrap()
        .cumulative_work_upper_bound;
    raw.entries[0].arguments[0] = value_argument(
        0,
        Some(wire::ConstantReference {
            pool_id: Some(7),
            row_ordinal: 1,
        }),
    );
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let mut prefix = None;
    let token = prepare_call_requests_decode_in(
        Some(&raw),
        &t,
        &p,
        SOURCE,
        limits(),
        &mut |facts| {
            if prefix.is_none() && facts.cumulative_work_upper_bound > raw_work {
                prefix = Some(c.trace.lock().unwrap().len());
            }
            Ok(())
        },
        &mut w,
    )
    .unwrap();
    assert!(token.facts().cumulative_work_upper_bound > raw_work);
    w.finish().unwrap();
    let baseline = c.trace.lock().unwrap().clone();
    let before_known_comparison =
        prefix.expect("actual CV did not contribute its original comparison work");
    assert!(before_known_comparison > 0 && before_known_comparison < baseline.len());
    for cause in CAUSES {
        let c = Control {
            stop: Some((before_known_comparison, cause)),
            ..Control::default()
        };
        let mut l = limits();
        l.max_work = raw_work;
        let result = run_owned(&raw, &t, &p, SOURCE, l, &c, &mut Vec::new());
        assert!(matches!(
            result,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(
            *c.trace.lock().unwrap(),
            baseline[..before_known_comparison]
        );
    }
}
