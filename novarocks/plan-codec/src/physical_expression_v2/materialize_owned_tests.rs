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

use super::super::super::decode_expression_definitions_in;
use super::*;

fn invoke(
    f: &Fixture,
    defs: &[wire::ExpressionDefinition],
    c: &Control,
    source: usize,
    caps: Limits,
    stop: Option<(usize, CompileControlError)>,
) -> Result<(Facts, Facts, Box<[novarocks_expression_contract::ExprNode]>), Error> {
    f.with_tokens(defs, c, |original, functions, aggregates| {
        c.arm(stop);
        let mut w = match CompileCheckpoints::try_new(c, CompilePhase::Decode) {
            Ok(work) => work,
            Err(cause) => {
                c.disarm();
                return Err(Error::Control(cause));
            }
        };
        let result = (|| {
            let mut read_snapshot = Facts::default();
            let decoded = decode_expression_definitions_in(
                crate::physical_expression_v2::ExpressionDefinitionInput::new(defs, &[]),
                original.values(),
                original.functions(),
                original.aggregates(),
                original.parameters(),
                original.pools(),
                EXPR_SOURCE,
                limits(),
                &mut |facts| {
                    read_snapshot = *facts;
                    Ok(())
                },
                &mut w,
            )?;
            assert_eq!(*decoded.facts(), read_snapshot);
            let mut snapshot = Facts::default();
            let prepared = prepare_expression_materialization_in(
                &decoded,
                functions,
                aggregates,
                &p::PlanLimits::FROZEN,
                source,
                caps,
                &mut |facts| {
                    snapshot = *facts;
                    Ok(())
                },
                &mut w,
            )?;
            assert_eq!(*prepared.facts(), snapshot);
            let owned = materialize_expressions_in(
                prepared,
                &mut |facts| {
                    assert_eq!(*facts, snapshot);
                    Ok(())
                },
                &mut w,
            )?;
            Ok((read_snapshot, snapshot, owned.into_definitions()))
        })();
        let result = finish(w, result);
        c.disarm();
        result
    })
}
#[test]
fn caller_owned_receiving_and_materializing_preserve_original_complete_kinds_and_dictionary_roots()
{
    let c = Control::default();
    let f = Fixture::new(&c);
    let defs = f.all();
    let (_, _, actual) = invoke(&f, &defs, &c, SOURCE, limits(), None).unwrap();
    let expected = owned(&f, &defs);
    assert_eq!(actual.len(), 17);
    for node in expected.iter() {
        assert_eq!(actual.get(node.id.get() as usize), Some(node));
    }
    let (_, _, lambda) = invoke(&f, &golden(), &c, SOURCE, limits(), None).unwrap();
    let novarocks_expression_contract::ExprKind::Lambda {
        parameter_types, ..
    } = &lambda.get(3).unwrap().kind
    else {
        panic!("original lambda")
    };
    assert_eq!(parameter_types[0].data_type(), &DataType::Utf8);
    assert_eq!(parameter_types[1], *f.types.value_type(4).unwrap());
}
#[test]
fn caller_owned_materialization_seven_axes_exact_replay_and_one_under() {
    let c = Control::default();
    let f = Fixture::new(&c);
    let defs = golden();
    let (_, facts, _) = invoke(&f, &defs, &c, SOURCE, limits(), None).unwrap();
    let exact = Limits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_expression_references: facts.expression_reference_count,
        max_new_allocation_requests: facts.new_allocation_requests_upper_bound,
        max_new_allocation_request_bytes: facts.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_work: facts.cumulative_work_upper_bound,
    };
    assert_eq!(invoke(&f, &defs, &c, SOURCE, exact, None).unwrap().1, facts);
    for axis in 0..7 {
        let mut l = exact;
        let n = match axis {
            0 => &mut l.max_definitions,
            1 => &mut l.max_type_references,
            2 => &mut l.max_expression_references,
            3 => &mut l.max_new_allocation_requests,
            4 => &mut l.max_new_allocation_request_bytes,
            5 => &mut l.max_coexisting_source_and_request_bytes,
            _ => &mut l.max_cumulative_work,
        };
        assert!(*n > 0);
        *n -= 1;
        assert!(
            matches!(
                invoke(&f, &defs, &c, SOURCE, l, None),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "axis {axis}"
        );
    }
}
#[test]
fn caller_owned_expression_indexes_and_stage_requests_win_before_pending_quantum() {
    let c = Control::default();
    let f = Fixture::new(&c);
    let defs = small();
    f.with_tokens(&defs, &c, |e, functions, aggregates| {
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                w.step().unwrap();
            }
            let mut l = limits();
            l.max_new_allocation_requests = 0;
            let actual = prepare_expression_materialization_in(
                e,
                functions,
                aggregates,
                &p::PlanLimits::FROZEN,
                SOURCE,
                l,
                &mut |_| Ok(()),
                &mut w,
            );
            assert!(matches!(
                actual,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
            c.disarm();
        }
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                w.step().unwrap();
            }
            let mut l = limits();
            l.max_new_allocation_request_bytes = 0;
            let actual = decode_expression_definitions_in(
                crate::physical_expression_v2::ExpressionDefinitionInput::new(&defs, &[]),
                e.values(),
                e.functions(),
                e.aggregates(),
                e.parameters(),
                e.pools(),
                EXPR_SOURCE,
                l,
                &mut |_| Ok(()),
                &mut w,
            );
            assert!(matches!(
                actual,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
            c.disarm();
        }
    });
}
#[test]
fn caller_owned_receiving_success_and_source_floor_refusal_preserve_every_actual_primary_prefix() {
    let c = Control::default();
    let f = Fixture::new(&c);
    let defs = small();
    for source in [SOURCE, 0] {
        let baseline = invoke(&f, &defs, &c, source, limits(), None);
        assert_eq!(baseline.is_ok(), source == SOURCE);
        let trace = c.trace();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                assert!(
                    matches!(invoke(&f,&defs,&c,source,limits(),Some((at,cause.clone()))),Err(Error::Control(actual)) if actual==cause),
                    "source={source} callback={at}"
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}
#[test]
fn caller_owned_wide_lists_and_clones_have_actual_quantum_and_original_source_loans() {
    let c = Control::default();
    let f = Fixture::new(&c);
    let mut defs = small();
    defs.push(definition(
        2,
        3,
        wire::expression_definition::Kind::InList(wire::InListExpression {
            expr_id: Some(0),
            list_expr_ids: vec![0; 320],
            negated: false,
        }),
    ));
    let (_, facts, arena) = invoke(&f, &defs, &c, SOURCE, limits(), None).unwrap();
    let novarocks_expression_contract::ExprKind::InList { list: args, .. } =
        &arena.get(2).unwrap().kind
    else {
        panic!("original in-list")
    };
    assert_eq!(args.len(), 320);
    assert!(args.iter().all(|id| id.get() == 0));
    assert!(
        facts.new_allocation_request_bytes_upper_bound
            >= 2 * Layout::array::<p::ExprId>(320).unwrap().size()
    );
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("actual source/emit quantum");
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            assert!(
                matches!(invoke(&f,&defs,&c,SOURCE,limits(),Some((at,cause.clone()))),Err(Error::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
    f.with_tokens(&small(), &c, |e, functions, aggregates| {
        let foreign = Control::default();
        let mut w = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
        let result = prepare_expression_materialization_in(
            e,
            functions,
            aggregates,
            &p::PlanLimits::FROZEN,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut w,
        );
        assert!(matches!(
            result,
            Err(Error::InvalidShape(
                "expression caller work has a different original control"
            ))
        ));
    });
}

#[test]
fn caller_owned_captured_value_literal_and_signature_pairs_preserve_facts_and_primary_prefixes() {
    let c = Control::default();
    let f = Fixture::new(&c);
    let good = f.all();
    let mut bad = good.clone();
    let literal = bad
        .iter_mut()
        .find(|d| matches!(d.kind, Some(wire::expression_definition::Kind::Constant(_))))
        .unwrap();
    literal.value_type_id = Some(0);
    for defs in [&good, &bad] {
        let run = |stop| {
            f.with_tokens(&good, &c, |original, _, _| {
                c.arm(stop);
                let mut w = match CompileCheckpoints::try_new(&c, CompilePhase::Decode) {
                    Ok(w) => w,
                    Err(cause) => {
                        c.disarm();
                        return Err(Error::Control(cause));
                    }
                };
                let result = decode_expression_definitions_in(
                    crate::physical_expression_v2::ExpressionDefinitionInput::new(defs, &[]),
                    original.values(),
                    original.functions(),
                    original.aggregates(),
                    original.parameters(),
                    original.pools(),
                    EXPR_SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut w,
                )
                .map(|decoded| *decoded.facts());
                let result = finish(w, result);
                c.disarm();
                result
            })
        };
        let baseline = run(None);
        if std::ptr::eq(defs, &good) {
            let facts = baseline.unwrap();
            f.with_tokens(&good, &c, |original, _, _| {
                assert_eq!(facts, *original.facts())
            });
        } else {
            assert!(matches!(
                baseline,
                Err(Error::Constant(p::ConstantReferenceError::Table(
                    novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(_)
                )))
            ));
        }
        let trace = c.trace();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                assert!(
                    matches!(run(Some((at,cause.clone()))), Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
    f.with_tokens(&good, &c, |original, _, _| {
        let value = original
            .values()
            .value_observed(
                0,
                &mut CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap(),
            )
            .unwrap()
            .unwrap();
        let header = f.types.value_type(0).unwrap();
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                w.step().unwrap();
            }
            let mut caps = limits();
            caps.max_cumulative_work = 0;
            let mut admit = |_: &Facts| Ok(());
            let mut admission = super::super::super::owner_admission::Admission {
                parent: Some(&mut admit),
                source: EXPR_SOURCE,
                limits: caps,
            };
            let result = super::super::super::namespace::compare_types(
                header,
                &value.ty,
                &mut Facts::default(),
                &mut admission,
                &mut w,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
            c.disarm();
        }
    });
}

#[test]
fn caller_owned_materialization_known_header_plus_owned_floor_overflow_is_primary() {
    let c = Control::default();
    let f = Fixture::new(&c);
    f.with_tokens(&small(), &c, |original, functions, aggregates| {
        let caps = Limits {
            max_definitions: usize::MAX,
            max_type_references: usize::MAX,
            max_expression_references: usize::MAX,
            max_new_allocation_requests: usize::MAX,
            max_new_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_cumulative_work: usize::MAX,
        };
        // The actual nonempty namespace has an exact retained floor MAX.
        // Its real dense output request must overflow coexistence before a
        // pending checkpoint, without adding an already invoiced source twice.
        let invoice = usize::MAX - original.retained_output_floor().unwrap();
        let huge = super::super::super::decode_expression_definitions(
            original.as_wire(),
            &[],
            original.values(),
            original.functions(),
            original.aggregates(),
            original.parameters(),
            original.pools(),
            invoice,
            caps,
        )
        .unwrap();
        assert_eq!(huge.retained_floor_header_in().unwrap(), usize::MAX);
        assert!(huge.source_count() > 0);
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                w.step().unwrap();
            }
            let result = prepare_expression_materialization_in(
                &huge,
                functions,
                aggregates,
                &p::PlanLimits::FROZEN,
                usize::MAX,
                caps,
                &mut |_| Ok(()),
                &mut w,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
            c.disarm();
        }
    });
}
