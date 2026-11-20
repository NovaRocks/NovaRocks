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

fn invoke(
    fixture: &Fixture,
    types: &EncodedTypeTable<'_>,
    ids: &[UnitRequestTypeIds<'_>],
    control: &Control,
    invoice: usize,
    limits: CallRequestProjectionLimits,
    parent: &mut CallRequestAdmit<'_>,
) -> Result<(Vec<wire::UnitCallRequest>, CallRequestProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_unit_requests_in(
        &fixture.unit,
        types,
        ids,
        &fixture.pools,
        invoice,
        limits,
        parent,
        &mut work,
    );
    finish(work, result)
}
fn ids<'a>(parameters: &'a [u32]) -> [ArgumentTypeIds<'a>; 3] {
    [
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Lambda {
            parameters,
            result: 0,
        },
    ]
}
fn exact(facts: CallRequestProjectionFacts) -> CallRequestProjectionLimits {
    CallRequestProjectionLimits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    }
}

#[test]
fn caller_sender_retains_original_none_cv_lambda_constraint_and_exact_six_axes() {
    let f = fixture();
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    let params = [u32::MAX];
    let args = ids(&params);
    let requests = project(&params, &args);
    let (plain, plain_facts) = run(&f, &Control::default(), limits(), SOURCE, false).unwrap();
    c.arm(None);
    let mut prefixes = Vec::new();
    let (actual, facts) = invoke(
        &f,
        &types,
        &requests,
        &c,
        SOURCE,
        limits(),
        &mut |current| {
            prefixes.push(*current);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(actual, plain);
    assert_eq!(facts, plain_facts);
    assert_eq!(facts.definition_count, 1);
    assert_eq!(facts.type_reference_count, 5);
    assert_eq!(facts.allocation_requests_upper_bound, 3);
    assert_eq!(
        facts.request_bytes_upper_bound,
        size_of::<wire::UnitCallRequest>()
            + 3 * size_of::<wire::OriginalFunctionArgument>()
            + size_of::<u32>()
    );
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + facts.request_bytes_upper_bound
    );
    for pair in prefixes.windows(2) {
        assert!(pair[0].request_bytes_upper_bound <= pair[1].request_bytes_upper_bound);
        assert!(pair[0].cumulative_work_upper_bound <= pair[1].cumulative_work_upper_bound);
    }
    assert_eq!(prefixes.last(), Some(&facts));
    assert!(matches!(&actual[0].arguments[0].kind,
        Some(wire::original_function_argument::Kind::Value(v)) if v.constant.is_none()));
    assert!(matches!(&actual[0].arguments[1].kind,
        Some(wire::original_function_argument::Kind::Value(v)) if v.constant.as_ref().is_some_and(|r| r.pool_id==Some(u32::MAX) && r.row_ordinal==1)));
    let original = f
        .pools
        .resolve_source_observed(
            p::ConstantReference {
                pool: p::ConstantPoolId::new(u32::MAX),
                ordinal: 1,
            },
            &mut CompileCheckpoints::try_new(&Control::default(), CompilePhase::Validate).unwrap(),
        )
        .unwrap();
    assert_eq!(original.ordinal(), 1);
    assert!(Arc::ptr_eq(
        original.pool().field_ref(),
        f.pools.entries()[&p::ConstantPoolId::new(u32::MAX)].field_ref()
    ));
    c.arm(None);
    invoke(&f, &types, &requests, &c, SOURCE, exact(facts), &mut |_| {
        Ok(())
    })
    .unwrap();
    for axis in 0..6 {
        let mut cap = exact(facts);
        match axis {
            0 => cap.max_definitions -= 1,
            1 => cap.max_type_references -= 1,
            2 => cap.max_request_bytes -= 1,
            3 => cap.max_allocation_requests -= 1,
            4 => cap.max_coexisting_source_and_request_bytes -= 1,
            _ => cap.max_work -= 1,
        }
        c.arm(None);
        assert!(
            matches!(
                invoke(&f, &types, &requests, &c, SOURCE, cap, &mut |_| Ok(())),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "axis {axis}"
        );
    }
    // The fixture author explicitly supplies an unconstrained original
    // request; it is not inferred from the call result or physical children.
    let mut unconstrained = f.unit.requests()[&ExprId::new(0)].clone();
    unconstrained.expected_result_type = None;
    // The replacement passes the actual unit gate before projection.
    let source = fixture().with_request(unconstrained);
    let requests = [UnitRequestTypeIds {
        definition: ExprId::new(0),
        arguments: &args,
        expected_result_type: None,
    }];
    c.arm(None);
    let (wire, facts) = invoke(
        &source,
        &types,
        &requests,
        &c,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(wire[0].expected_result_value_type_id, None);
    assert_eq!(facts.type_reference_count, 4);
}

#[test]
fn caller_sender_known_header_and_emit_refusal_precede_pending_control_and_foreign_work() {
    let f = fixture();
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    let params = [u32::MAX];
    let args = ids(&params);
    let requests = project(&params, &args);
    for cause in CAUSES {
        for local in [true, false] {
            c.arm(Some((1, cause.clone())));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut count = 0;
            let mut cap = limits();
            if local {
                cap.max_request_bytes = 0;
            }
            let outcome = encode_unit_requests_in(
                &f.unit,
                &types,
                &requests,
                &f.pools,
                SOURCE,
                cap,
                &mut |facts| {
                    count += 1;
                    assert!(facts.request_bytes_upper_bound > 0);
                    Err(CompileControlError::ResourceExhausted)
                },
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(work, outcome),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(count, usize::from(!local));
            assert_eq!(c.trace(), [(CompilePhase::Encode, 0)]);
        }
        // Unit requests have one borrowed operation, not a transferable token.
        // The original token refusal law is exercised on this actual checked
        // Window relation, whose real context and request are attached.
        let relation = relation_fixture();
        let relation_ids = [CallRequestTypeIds {
            definition: PhysicalCallSite::Window {
                node: p::NodeId::new(42),
                call: 0,
            },
            arguments: &[],
            expected_result_type: None,
        }];
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let token = prepare_call_requests_encode_in(
            relation.call_requests(),
            &types,
            &relation_ids,
            &f.pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        c.arm(Some((0, cause)));
        let result = encode_call_requests_in(
            token,
            &mut |_| Err(CompileControlError::ResourceExhausted),
            &mut work,
        );
        assert!(matches!(
            finish(work, result),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(c.trace().is_empty());
        c.arm(None);
        let token = prepare_call_requests_encode_in(
            relation.call_requests(),
            &types,
            &relation_ids,
            &f.pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap(),
        )
        .unwrap();
        let foreign = Control::default();
        foreign.arm(None);
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        let mut parent_calls = 0;
        assert!(matches!(
            token.emit_in(
                &mut |_| {
                    parent_calls += 1;
                    Ok(())
                },
                &mut work
            ),
            Err(Error::InvalidShape(
                "request encoder belongs to another controller"
            ))
        ));
        assert_eq!(parent_calls, 0);
        assert_eq!(foreign.trace(), [(CompilePhase::Encode, 0)]);
    }
}

#[test]
fn caller_sender_actual_success_and_ordinary_failure_preserve_every_control_prefix() {
    let f = fixture();
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    for bad in [false, true] {
        let params = [if bad { 7 } else { u32::MAX }];
        let args = ids(&params);
        let requests = project(&params, &args);
        c.arm(None);
        let baseline = invoke(&f, &types, &requests, &c, SOURCE, limits(), &mut |_| Ok(()));
        if bad {
            assert!(matches!(
                baseline,
                Err(Error::InvalidShape(
                    "request complete type differs from its supplied type root"
                ))
            ));
        } else {
            baseline.unwrap();
        }
        let trace = c.trace();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(invoke(&f,&types,&requests,&c,SOURCE,limits(),&mut |_|Ok(())),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
    c.arm(None);
    assert!(matches!(
        invoke(
            &f,
            &types,
            &project(&[u32::MAX], &ids(&[u32::MAX])),
            &c,
            0,
            limits(),
            &mut |_| Ok(())
        ),
        Err(Error::InvalidShape(
            "request source invoice omits original backing"
        ))
    ));
}

#[test]
fn caller_sender_captured_constant_address_and_complete_type_keep_original_failures() {
    for reference in [
        p::ConstantReference {
            pool: p::ConstantPoolId::new(1),
            ordinal: 1,
        },
        p::ConstantReference {
            pool: p::ConstantPoolId::new(u32::MAX),
            ordinal: 3,
        },
    ] {
        let f = fixture_with_reference(reference);
        let c = Control::default();
        let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
        let params = [u32::MAX];
        let args = ids(&params);
        let requests = project(&params, &args);
        c.arm(None);
        let result = invoke(&f, &types, &requests, &c, SOURCE, limits(), &mut |_| Ok(()));
        if reference.pool.get() == 1 {
            assert!(
                matches!(result,Err(Error::Constant(p::ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::MissingPool(id)))) if id==reference.pool)
            );
        } else {
            assert!(matches!(
                result,
                Err(Error::Constant(p::ConstantReferenceError::Constant(_)))
            ));
        }
    }
    let mut f = fixture();
    let setup = Control::default();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let pool = p::ConstantPool::try_new(
        Arc::new(Field::new("different-nullability", DataType::Int64, false)),
        ty,
        Arc::new(Int64Array::from(vec![11, 22, 33])).to_data(),
        admission(),
        CompilePhase::Validate,
        &setup,
    )
    .unwrap();
    f.pools = ConstantPools::empty();
    f.pools
        .insert(p::ConstantPoolId::new(u32::MAX), pool)
        .unwrap();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &setup).unwrap();
    let params = [u32::MAX];
    let args = ids(&params);
    let requests = project(&params, &args);
    setup.arm(None);
    assert!(
        matches!(invoke(&f,&types,&requests,&setup,SOURCE,limits(),&mut |_|Ok(())),Err(Error::Constant(p::ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(r)))) if r.ordinal==1 && r.pool.get()==u32::MAX)
    );
}

#[test]
fn caller_sender_wide_actual_lambda_emission_observes_and_refuses_real_quantum() {
    let f = fixture_with_parameters(
        p::ConstantReference {
            pool: p::ConstantPoolId::new(u32::MAX),
            ordinal: 1,
        },
        320,
    );
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    let params = vec![u32::MAX; 320];
    let args = ids(&params);
    let requests = project(&params, &args);
    let mut cap = limits();
    cap.max_work = 1usize << 34;
    c.arm(None);
    let (output, _) = invoke(&f, &types, &requests, &c, SOURCE, cap, &mut |_| Ok(())).unwrap();
    assert!(
        matches!(&output[0].arguments[2].kind,Some(wire::original_function_argument::Kind::Lambda(v)) if v.parameter_value_type_ids==params)
    );
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual owned Lambda emission quantum");
    for at in [0, quantum, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            c.arm(Some((at, cause.clone())));
            assert!(
                matches!(invoke(&f,&types,&requests,&c,SOURCE,cap,&mut |_|Ok(())),Err(Error::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn caller_sender_actual_captured_type_and_cv_prefix_refuse_before_pending_flush() {
    let f = fixture();
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    let mut initial =
        Model::<std::convert::Infallible>::new(SOURCE, &types, &f.pools, None).unwrap();
    initial.check(limits()).unwrap();
    let initial_work = initial.facts.cumulative_work_upper_bound;
    // Exercise the actual captured loan helpers at a caller-owned pending
    // tail. This private seam is not a complete Package publication proof.
    for constant in [false, true] {
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut captures = 0;
            let mut parent = |facts: &CallRequestProjectionFacts| {
                if facts.cumulative_work_upper_bound > initial_work {
                    captures += 1;
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            };
            let mut host_parent = |facts: &CallRequestProjectionFacts| {
                parent(facts).map_err(HostError::<std::convert::Infallible>::from)
            };
            let mut model = Model::new(SOURCE, &types, &f.pools, Some(&mut host_parent)).unwrap();
            let outcome = if constant {
                validate_constant(
                    p::ConstantReference {
                        pool: p::ConstantPoolId::new(u32::MAX),
                        ordinal: 1,
                    },
                    &int(),
                    &f.pools,
                    &mut model,
                    limits(),
                    &mut work,
                )
            } else {
                verify_id(&types, 0, &int(), &mut model, limits(), &mut work)
            };
            assert!(matches!(
                finish(work, outcome.map_err(ProjectionFailure::without_host)),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(captures, 1);
            assert_eq!(c.trace(), [(CompilePhase::Encode, 0)]);
        }
    }
}

fn with_result_constrained_request(
    test: impl FnOnce(
        &FragmentCallRequests,
        &EncodedTypeTable<'_>,
        &ConstantPools,
        CallRequestTypeIds<'_>,
    ),
) {
    let fragment = relation_fixture_with_result_constraint(Some(int()));
    let requests = fragment.call_requests();
    let site = *requests.entries().first_key_value().unwrap().0;
    let roots = [(0, int())];
    let types =
        encode_type_table_sources(&roots, &[], types_limits(), &Control::default()).unwrap();
    test(
        requests,
        &types,
        &ConstantPools::empty(),
        CallRequestTypeIds {
            definition: site,
            arguments: &[],
            expected_result_type: Some(0),
        },
    );
}

#[test]
fn sql_dependency_request_host_abort_three_control_causes_at_every_checkpoint_have_no_tail() {
    use std::convert::Infallible;

    with_result_constrained_request(|requests, types, pools, ids| {
        let ids = [ids];
        let control = Control::default();
        let invoke = || -> Result<wire::FragmentCallRequests, HostError<Infallible>> {
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
            let mut admit = |_: &CallRequestProjectionFacts| Ok(());
            let token = prepare_call_requests_encode_with_host_in(
                requests,
                types,
                &ids,
                pools,
                SOURCE,
                limits(),
                &mut admit,
                &mut work,
            )?;
            let wire = token.emit_with_host_in(&mut admit, &mut work)?;
            work.finish()?;
            Ok(wire)
        };

        control.arm(None);
        invoke().unwrap();
        let trace = control.trace();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                control.arm(Some((at, cause.clone())));
                assert!(matches!(
                    invoke(),
                    Err(ProjectionFailure::Codec(Error::Control(actual))) if actual == cause
                ));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    });
}

#[test]
fn sql_dependency_request_host_abort_foreign_controller_preserves_original_text_before_callback() {
    use std::convert::Infallible;

    with_result_constrained_request(|requests, types, pools, ids| {
        let ids = [ids];
        let control = Control::default();
        let other = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut foreign = CompileCheckpoints::try_new(&other, CompilePhase::Encode).unwrap();
        let plain = prepare_call_requests_encode_in(
            requests,
            types,
            &ids,
            pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let mut called = 0;
        let plain = plain
            .emit_in(
                &mut |_| {
                    called += 1;
                    Ok(())
                },
                &mut foreign,
            )
            .unwrap_err();
        assert_eq!(called, 0);

        let host = prepare_call_requests_encode_in(
            requests,
            types,
            &ids,
            pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let host = host
            .emit_with_host_in(
                &mut |_| -> Result<(), AdmissionRefusal<Infallible>> {
                    called += 1;
                    Ok(())
                },
                &mut foreign,
            )
            .unwrap_err()
            .without_host();
        assert_eq!(host.to_string(), plain.to_string());
        assert_eq!(called, 0);
    });
}

#[test]
fn sql_dependency_request_host_abort_invalid_type_id_keeps_original_codec_error() {
    use std::convert::Infallible;

    with_result_constrained_request(|requests, types, pools, ids| {
        // The checked relation came with a declared result constraint. Only
        // its value-type table ID is stale in the projection input.
        let invalid = [CallRequestTypeIds {
            expected_result_type: Some(1),
            ..ids
        }];
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let plain = prepare_call_requests_encode_in(
            requests,
            types,
            &invalid,
            pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .err()
        .unwrap();

        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let host = prepare_call_requests_encode_with_host_in(
            requests,
            types,
            &invalid,
            pools,
            SOURCE,
            limits(),
            &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
            &mut work,
        )
        .err()
        .unwrap()
        .without_host();
        assert_eq!(host.to_string(), plain.to_string());
        assert_eq!(
            plain.to_string(),
            "request supplied value type ID is absent"
        );
    });
}
