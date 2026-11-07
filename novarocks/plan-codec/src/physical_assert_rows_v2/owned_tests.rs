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

fn parent_axes(f: &NodeProjectionFacts) -> [usize; 7] {
    [
        f.input_node_count,
        f.value_reference_count,
        f.list_item_count,
        f.allocation_requests_upper_bound,
        f.allocation_request_bytes_upper_bound,
        f.coexisting_source_and_request_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    ]
}
fn parent_exact(f: &NodeProjectionFacts) -> AssertRowsNodeProjectionLimits {
    let mut l = limits();
    l.max_input_nodes = f.input_node_count;
    l.max_value_references = f.value_reference_count;
    l.max_list_items = f.list_item_count;
    l.max_allocation_requests = f.allocation_requests_upper_bound;
    l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.max_coexisting_source_and_request_bytes = f.coexisting_source_and_request_bytes_upper_bound;
    l.max_work = f.cumulative_work_upper_bound;
    l
}
fn parent_under(f: &NodeProjectionFacts, axis: usize) -> AssertRowsNodeProjectionLimits {
    let mut l = parent_exact(f);
    match axis {
        0 => l.max_input_nodes -= 1,
        1 => l.max_value_references -= 1,
        2 => l.max_list_items -= 1,
        3 => l.max_allocation_requests -= 1,
        4 => l.max_allocation_request_bytes -= 1,
        5 => l.max_coexisting_source_and_request_bytes -= 1,
        6 => l.max_work -= 1,
        _ => unreachable!(),
    };
    l
}

#[test]
fn caller_owned_full_family_matches_original_oracles_and_seven_exact_axes() {
    let c = Control::default();
    with_namespaces(&definitions(), &c, |values, decoded| {
        for (node, wire) in [(keyed(), expected_keyed())] {
            c.arm(None);
            let (plain, expected_encode) =
                encode_assert_rows_node(&node, values, SOURCE, limits()).unwrap();
            assert_eq!(plain, wire);
            c.arm(None);
            let (plain, expected_decode) =
                decode_assert_rows_node(&wire, decoded, SOURCE, limits()).unwrap();
            assert_eq!(plain, node);
            for receiving in [false, true] {
                let expected = if receiving {
                    expected_decode
                } else {
                    expected_encode
                };
                for axis in 0..=7 {
                    c.arm(None);
                    let mut work =
                        CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
                    let mut last = None;
                    let mut admit = |f: &NodeProjectionFacts| {
                        if let Some(old) = last {
                            for (a, b) in parent_axes(&old).into_iter().zip(parent_axes(f)) {
                                assert!(a <= b);
                            }
                        }
                        last = Some(*f);
                        Ok(())
                    };
                    let l = if axis == 7 {
                        parent_exact(&expected)
                    } else {
                        parent_under(&expected, axis)
                    };
                    let result = if receiving {
                        prepare_assert_rows_node_decode_in(
                            &wire, decoded, SOURCE, l, &mut admit, &mut work,
                        )
                        .and_then(|p| p.emit_in(&mut admit, &mut work))
                        .map(|(actual, f)| {
                            assert_eq!(actual, node);
                            f
                        })
                    } else {
                        prepare_assert_rows_node_encode_in(
                            &node, values, SOURCE, l, &mut admit, &mut work,
                        )
                        .and_then(|p| p.emit_in(&mut admit, &mut work))
                        .map(|(actual, f)| {
                            assert_eq!(actual, wire);
                            f
                        })
                    };
                    let result = finish(work, result);
                    if axis == 7 {
                        assert_eq!(result.unwrap(), expected);
                        assert_eq!(last, Some(expected));
                    } else {
                        assert!(matches!(
                            result,
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                    }
                    assert!(
                        c.trace()
                            .iter()
                            .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
                    );
                }
            }
        }
    });
}

#[test]
fn caller_owned_known_header_and_emit_requests_precede_late_control_and_foreign_work() {
    let c = Control::default();
    with_namespaces(&definitions(), &c, |values, decoded| {
        let (node, wire) = vec![(keyed(), expected_keyed())]
            .into_iter()
            .next()
            .unwrap();
        for receiving in [false, true] {
            for late in CAUSES {
                c.arm(Some((1, late.clone())));
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
                for _ in 0..255 {
                    work.step().unwrap();
                }
                let mut l = limits();
                l.max_allocation_request_bytes = 0;
                let mut calls = 0;
                let mut admit = |_: &NodeProjectionFacts| {
                    calls += 1;
                    Ok(())
                };
                let result = if receiving {
                    prepare_assert_rows_node_decode_in(
                        &wire, decoded, SOURCE, l, &mut admit, &mut work,
                    )
                    .map(|_| ())
                } else {
                    prepare_assert_rows_node_encode_in(
                        &node, values, SOURCE, l, &mut admit, &mut work,
                    )
                    .map(|_| ())
                };
                assert!(matches!(
                    finish(work, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(calls, 0);
                assert_eq!(c.trace(), [(CompilePhase::LowerProgram, 0)]);
                c.arm(None);
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
                if receiving {
                    let p = prepare_assert_rows_node_decode_in(
                        &wire,
                        decoded,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .unwrap();
                    c.arm(Some((0, late)));
                    assert!(matches!(
                        p.emit_in(
                            &mut |_| Err(CompileControlError::ResourceExhausted),
                            &mut work
                        ),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                } else {
                    let p = prepare_assert_rows_node_encode_in(
                        &node,
                        values,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .unwrap();
                    c.arm(Some((0, late)));
                    assert!(matches!(
                        p.emit_in(
                            &mut |_| Err(CompileControlError::ResourceExhausted),
                            &mut work
                        ),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                }
                assert!(c.trace().is_empty());
            }
        }
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
        let result = prepare_assert_rows_node_encode_in(
            &node,
            values,
            0,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .map(|_| ());
        assert!(matches!(finish(work, result), Err(Error::InvalidShape(_))));
        let foreign = Control::default();
        foreign.arm(None);
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::LowerProgram).unwrap();
        let mut calls = 0;
        let result = prepare_assert_rows_node_encode_in(
            &node,
            values,
            SOURCE,
            limits(),
            &mut |_| {
                calls += 1;
                Ok(())
            },
            &mut work,
        )
        .map(|_| ());
        assert!(matches!(result, Err(Error::InvalidShape(_))));
        assert_eq!(calls, 0);
        assert_eq!(foreign.trace(), [(CompilePhase::LowerProgram, 0)]);
    });
}

#[test]
fn caller_owned_all_actual_small_prefixes_and_wide_nested_quantum() {
    let c = Control::default();
    with_namespaces(&definitions(), &c, |values, decoded| {
        let (node, wire) = vec![(keyed(), expected_keyed())]
            .into_iter()
            .next()
            .unwrap();
        let invoke =
            |receiving: bool, ordinary: bool, stop: Option<(usize, CompileControlError)>| {
                let mut node = node.clone();
                let mut wire = wire.clone();
                if ordinary {
                    node.output.columns[0] = p::ValueId::new(99);
                    wire.output.as_mut().unwrap().value_ids[0] = 99;
                }
                c.arm(stop);
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram)?;
                let result = if receiving {
                    prepare_assert_rows_node_decode_in(
                        &wire,
                        decoded,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .and_then(|p| p.emit_in(&mut |_| Ok(()), &mut work))
                    .map(|_| ())
                } else {
                    prepare_assert_rows_node_encode_in(
                        &node,
                        values,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .and_then(|p| p.emit_in(&mut |_| Ok(()), &mut work))
                    .map(|_| ())
                };
                finish(work, result)
            };
        for receiving in [false, true] {
            for ordinary in [false, true] {
                let result = invoke(receiving, ordinary, None);
                assert_eq!(result.is_err(), ordinary);
                let baseline = c.trace();
                assert!(!baseline.is_empty());
                for at in 0..baseline.len() {
                    for cause in CAUSES {
                        assert!(
                            matches!(invoke(receiving,ordinary,Some((at,cause.clone()))),Err(Error::Control(actual)) if actual==cause)
                        );
                        assert_eq!(c.trace(), baseline[..=at]);
                    }
                }
            }
        }
        let node = source(p::RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: novarocks_physical_plan::AssertionApplicability::AllRows,
            keys: vec![p::ValueId::new(0); 320].into_boxed_slice(),
            labels: vec![Box::<str>::from("Ω\0界"); 320].into_boxed_slice(),
            message: "actual wide owned assertion".into(),
        });
        // Independent actual first nested request Layout, before any source
        // counting callback: the full node header and the captured child list.
        let captured_bytes = std::mem::size_of::<u32>()
            + std::mem::size_of::<wire::PhysicalProperties>()
            + 3 * std::mem::size_of::<u32>()
            + 320 * std::mem::size_of::<u32>()
            + 320 * std::mem::size_of::<String>()
            + "actual wide owned assertion".len()
            + "Ω\0界".len();
        for late in CAUSES {
            c.arm(Some((1, late)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut captures = 0;
            let result = prepare_assert_rows_node_encode_in(
                &node,
                values,
                SOURCE,
                limits(),
                &mut |f| {
                    captures += 1;
                    assert_eq!(f.allocation_request_bytes_upper_bound, captured_bytes);
                    if f.allocation_request_bytes_upper_bound > captured_bytes - 1 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            )
            .map(|_| ());
            assert!(matches!(
                finish(work, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(captures, 1);
            assert_eq!(c.trace(), [(CompilePhase::LowerProgram, 0)]);
        }
        c.arm(None);
        let wire = encode_assert_rows_node(&node, values, SOURCE, limits())
            .unwrap()
            .0;
        for receiving in [false, true] {
            let run = |stop| {
                c.arm(stop);
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram)?;
                let result = if receiving {
                    prepare_assert_rows_node_decode_in(
                        &wire,
                        decoded,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .and_then(|p| p.emit_in(&mut |_| Ok(()), &mut work))
                    .map(|(actual, _)| assert_eq!(actual, node))
                } else {
                    prepare_assert_rows_node_encode_in(
                        &node,
                        values,
                        SOURCE,
                        limits(),
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .and_then(|p| p.emit_in(&mut |_| Ok(()), &mut work))
                    .map(|(actual, _)| assert_eq!(actual, wire))
                };
                finish(work, result)
            };
            run(None).unwrap();
            let baseline = c.trace();
            let quantum = baseline
                .iter()
                .position(|(_, n)| *n == 256)
                .expect("actual wide source reaches original quantum");
            for at in [0, quantum, baseline.len() - 1] {
                for cause in CAUSES {
                    assert!(
                        matches!(run(Some((at,cause.clone()))),Err(Error::Control(actual)) if actual==cause)
                    );
                    assert_eq!(c.trace(), baseline[..=at]);
                }
            }
        }
    });
}

#[test]
fn caller_owned_empty_inner_headers_refuse_before_floor_pending_quantum() {
    let c = Control::default();
    with_namespaces(&definitions(), &c, |values, decoded| {
        let global = global(p::RowCountAssertion::Eq, 0);
        let global_raw = expected(wire::row_count_assertion_node::Kind::Global(
            wire::GlobalRowCountAssertion {
                subject: "行数\0é".into(),
                desired_rows: 0,
                comparison: 1,
            },
        ));
        let empty = source(p::RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: novarocks_physical_plan::AssertionApplicability::AllRows,
            keys: Box::default(),
            labels: Box::default(),
            message: "empty keys".into(),
        });
        let empty_raw = expected(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(
            wire::PerKeyRowCountAssertion {
                declared_sql_violation: None,
                applicability: Some(wire::AssertionApplicability {
                    kind: Some(wire::assertion_applicability::Kind::AllRows(
                        novarocks_proto_models::physical_control_v2::Empty {},
                    )),
                }),
                key_value_ids: vec![],
                labels: vec![],
                message: "empty keys".into(),
            },
        ));
        for (node, raw) in [(global, global_raw), (empty, empty_raw)] {
            for receiving in [false, true] {
                for late in CAUSES {
                    for local_limit in [false, true] {
                        c.arm(Some((1, late.clone())));
                        let mut work =
                            CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
                        for _ in 0..255 {
                            work.step().unwrap();
                        }
                        let mut l = limits();
                        if local_limit {
                            l.max_allocation_request_bytes = 0;
                        }
                        let mut calls = 0;
                        let mut admit = |facts: &NodeProjectionFacts| {
                            calls += 1;
                            assert!(facts.allocation_request_bytes_upper_bound > 0);
                            Err(CompileControlError::ResourceExhausted)
                        };
                        let result = if receiving {
                            prepare_assert_rows_node_decode_in(
                                &raw, decoded, SOURCE, l, &mut admit, &mut work,
                            )
                            .map(|_| ())
                        } else {
                            prepare_assert_rows_node_encode_in(
                                &node, values, SOURCE, l, &mut admit, &mut work,
                            )
                            .map(|_| ())
                        };
                        assert!(matches!(
                            finish(work, result),
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                        assert_eq!(calls, usize::from(!local_limit));
                        assert_eq!(c.trace(), [(CompilePhase::LowerProgram, 0)]);
                    }
                }
            }
        }
    });
}
