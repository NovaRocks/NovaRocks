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
use crate::physical_unpivot_v2::tests::{Control, Fixture, limits};
use novarocks_type_contract::CompileControlError;
use std::alloc::Layout;
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
fn reference(pool: u32, ordinal: u32) -> p::ConstantReference {
    p::ConstantReference {
        pool: p::ConstantPoolId::new(pool),
        ordinal,
    }
}
fn source() -> p::WriterGroupedUnpivotSpec {
    let constants = |id| {
        Box::from([
            p::UnpivotConstant::Scalar(p::ExprId::new(id)),
            p::UnpivotConstant::Int32List(reference(0, 1)),
            p::UnpivotConstant::Utf8Map(reference(7, 1)),
        ])
    };
    p::WriterGroupedUnpivotSpec {
        // The component preserves raw target order/multiplicity. The Fragment
        // writer author independently rejects duplicate/unsorted targets.
        statistics_target_ordinals: Box::from([
            p::WriteTargetOrdinal::try_new(4095).unwrap(),
            p::WriteTargetOrdinal::try_new(0).unwrap(),
            p::WriteTargetOrdinal::try_new(4095).unwrap(),
        ]),
        grouping_input: p::ValueId::new(0),
        grouping_output: p::ValueId::new(u32::MAX),
        passthrough_output: p::ValueId::new(7),
        value_output: p::ValueId::new(7),
        literal_outputs: Box::from([
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]),
        mappings: Box::from([
            p::WriterGroupedUnpivotMapping {
                write_target_ordinal: p::WriteTargetOrdinal::try_new(4095).unwrap(),
                input: p::ValueId::new(0),
                constants: constants(0),
            },
            p::WriterGroupedUnpivotMapping {
                write_target_ordinal: p::WriteTargetOrdinal::try_new(0).unwrap(),
                input: p::ValueId::new(7),
                constants: constants(u32::MAX),
            },
        ]),
        max_output_rows: u64::MAX,
        max_output_bytes: 1 << 63,
    }
}
fn expected() -> wire::WriterGroupedUnpivot {
    let constants = |id| {
        vec![
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::ScalarExprId(id)),
            },
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::Int32List(
                    wire::ConstantReference {
                        pool_id: Some(0),
                        row_ordinal: 1,
                    },
                )),
            },
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::Utf8Map(
                    wire::ConstantReference {
                        pool_id: Some(7),
                        row_ordinal: 1,
                    },
                )),
            },
        ]
    };
    wire::WriterGroupedUnpivot {
        statistics_target_ordinals: vec![4095, 0, 4095],
        grouping_input_value_id: Some(0),
        grouping_output_value_id: Some(u32::MAX),
        passthrough_output_value_id: Some(7),
        value_output_id: Some(7),
        literal_output_ids: vec![u32::MAX, 0, u32::MAX],
        mappings: vec![
            wire::WriterGroupedUnpivotMapping {
                write_target_ordinal: 4095,
                input_value_id: Some(0),
                constants: constants(0),
            },
            wire::WriterGroupedUnpivotMapping {
                write_target_ordinal: 0,
                input_value_id: Some(7),
                constants: constants(u32::MAX),
            },
        ],
        max_output_rows: u64::MAX,
        max_output_bytes: 1 << 63,
    }
}
fn exact(f: WriterGroupedUnpivotProjectionFacts) -> WriterGroupedUnpivotProjectionLimits {
    WriterGroupedUnpivotProjectionLimits {
        max_input_nodes: 0,
        max_value_references: f.value_reference_count,
        max_list_items: f.list_item_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
        ..limits()
    }
}
fn under(
    mut l: WriterGroupedUnpivotProjectionLimits,
    at: usize,
) -> WriterGroupedUnpivotProjectionLimits {
    match at {
        0 => l.max_value_references -= 1,
        1 => l.max_list_items -= 1,
        2 => l.max_allocation_requests -= 1,
        3 => l.max_allocation_request_bytes -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        5 => l.max_work -= 1,
        _ => panic!(),
    }
    l
}
fn assert_prefix<T>(c: &Control, run: impl Fn() -> Result<T, Error>) {
    c.arm(None);
    let _ = run();
    let trace = c.trace();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in CAUSES {
            c.arm(Some((stop, cause.clone())));
            assert!(matches!(run(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(c.trace(), trace[..=stop]);
        }
    }
    c.arm(None);
}

#[test]
fn grouped_unpivot_exact_raw_payload_and_independent_allocation_layouts() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (encoded, f) =
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, limits())
                .unwrap();
        assert_eq!(encoded, expected());
        c.arm(None);
        let (decoded, r) =
            decode_writer_grouped_unpivot(&expected(), read, SOURCE, limits()).unwrap();
        assert_eq!(decoded, source());
        assert_eq!(f.input_node_count, 0);
        assert_eq!(f.value_reference_count, 9);
        assert_eq!(f.list_item_count, 14);
        assert_eq!(f.allocation_requests_upper_bound, 5);
        assert_eq!(r.allocation_requests_upper_bound, 10);
        let size = |layout: Layout| layout.size();
        let send = size(Layout::array::<u32>(6).unwrap())
            + size(Layout::array::<wire::WriterGroupedUnpivotMapping>(2).unwrap())
            + size(Layout::array::<wire::UnpivotConstant>(6).unwrap());
        let receive = 2
            * (size(Layout::array::<p::WriteTargetOrdinal>(3).unwrap())
                + size(Layout::array::<p::ValueId>(3).unwrap())
                + size(Layout::array::<p::WriterGroupedUnpivotMapping>(2).unwrap())
                + size(Layout::array::<p::UnpivotConstant>(6).unwrap()));
        assert_eq!(f.allocation_request_bytes_upper_bound, send);
        assert_eq!(r.allocation_request_bytes_upper_bound, receive);
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + send
        );
        assert_eq!(
            r.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + receive
        );
        assert!(std::ptr::eq(expressions.pools(), read.pools()));
        let mut zero = source();
        zero.max_output_rows = 0;
        zero.max_output_bytes = 0;
        c.arm(None);
        let (wire, _) =
            encode_writer_grouped_unpivot(&zero, values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(
            decode_writer_grouped_unpivot(&wire, read, SOURCE, limits())
                .unwrap()
                .0,
            zero
        );
        let mut empty = source();
        empty.statistics_target_ordinals = Box::default();
        empty.literal_outputs = Box::default();
        empty.mappings = Box::default();
        c.arm(None);
        let (encoded, facts) =
            encode_writer_grouped_unpivot(&empty, values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(facts.allocation_requests_upper_bound, 0);
        assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
        assert_eq!(facts.value_reference_count, 4);
        let (decoded, facts) =
            decode_writer_grouped_unpivot(&encoded, read, SOURCE, limits()).unwrap();
        assert_eq!(decoded, empty);
        assert_eq!(facts.allocation_requests_upper_bound, 0);
    });
}
#[test]
fn grouped_unpivot_prepared_tokens_keep_original_namespaces_and_refuse_foreign_loans() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let c = Control::default();
    let different = Control::default();
    let input = source();
    let received = expected();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let prepared =
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
                .unwrap();
        let f = *prepared.facts();
        assert_eq!(prepared.emit().unwrap(), (expected(), f));
        c.arm(None);
        let prepared =
            prepare_writer_grouped_unpivot_decode(&received, read, SOURCE, limits()).unwrap();
        let f = *prepared.facts();
        assert_eq!(prepared.emit().unwrap(), (source(), f));
        c.arm(None);
        other.with_tokens(&c, |_, foreign, _| {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, foreign, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
        different.arm(None);
        other.with_tokens(&different, |_, foreign, _| {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, foreign, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
    });
}
#[test]
fn grouped_unpivot_closed_presence_and_selected_address_errors_remain_distinct() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in 0..7 {
            let mut raw = expected();
            match at {
                0 => raw.grouping_input_value_id = None,
                1 => raw.grouping_output_value_id = None,
                2 => raw.passthrough_output_value_id = None,
                3 => raw.value_output_id = None,
                4 => raw.mappings[0].input_value_id = None,
                5 => raw.mappings[0].constants[0].kind = None,
                6 => {
                    raw.mappings[0].constants[1].kind = Some(
                        wire::unpivot_constant::Kind::Int32List(wire::ConstantReference {
                            pool_id: None,
                            row_ordinal: 1,
                        }),
                    )
                }
                _ => unreachable!(),
            }
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for id in [7, 99] {
            let mut input = source();
            input.mappings[0].constants[0] = p::UnpivotConstant::Scalar(p::ExprId::new(id));
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            let mut raw = expected();
            raw.mappings[0].constants[0].kind =
                Some(wire::unpivot_constant::Kind::ScalarExprId(id));
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for invalid_ordinal in [4096, u32::MAX] {
            for mapping in [false, true] {
                let mut raw = expected();
                if mapping {
                    raw.mappings[0].write_target_ordinal = invalid_ordinal;
                } else {
                    raw.statistics_target_ordinals[0] = invalid_ordinal;
                }
                c.arm(None);
                assert!(matches!(
                    decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                    Err(Error::Identity(novarocks_connector_contract::ConnectorIdentityError::InvalidWriteTargetOrdinal))
                ));
            }
        }
        for selected in [reference(99, 0), reference(0, 2)] {
            let mut input = source();
            input.mappings[0].constants[1] = p::UnpivotConstant::Int32List(selected);
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
            let mut raw = expected();
            raw.mappings[0].constants[1].kind = Some(wire::unpivot_constant::Kind::Int32List(
                wire::ConstantReference {
                    pool_id: Some(selected.pool.get()),
                    row_ordinal: selected.ordinal,
                },
            ));
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
        }
        let mut input = source();
        input.grouping_output = p::ValueId::new(99);
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        let mut raw = expected();
        raw.grouping_output_value_id = Some(99);
        c.arm(None);
        assert!(matches!(
            decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        // This component only verifies original addresses. Swapped collection
        // profiles and raw target order still require the writer Fragment gate.
        let mut input = source();
        input.mappings[0].constants[1] = p::UnpivotConstant::Int32List(reference(7, 1));
        c.arm(None);
        assert!(
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()).is_ok()
        );
    });
}
#[test]
fn grouped_unpivot_six_resource_axes_and_actual_wire_capacities_precede_emission() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (_, f) =
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, limits())
                .unwrap();
        c.arm(None);
        assert!(
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, exact(f)).is_ok()
        );
        for axis in 0..6 {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(
                    &source(),
                    values,
                    expressions,
                    SOURCE,
                    under(exact(f), axis)
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        c.arm(None);
        let (_, f) = decode_writer_grouped_unpivot(&expected(), read, SOURCE, limits()).unwrap();
        c.arm(None);
        assert!(decode_writer_grouped_unpivot(&expected(), read, SOURCE, exact(f)).is_ok());
        for axis in 0..6 {
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&expected(), read, SOURCE, under(exact(f), axis)),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        let mut capacity = expected();
        capacity.statistics_target_ordinals.reserve(SOURCE);
        c.arm(None);
        assert!(matches!(
            decode_writer_grouped_unpivot(&capacity, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        // Even before walking 320 inner constants, their actual count must
        // obey the outer caller's source/list/work envelope.
        let mut wide = source();
        wide.mappings[0].constants =
            vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
        let mut l = limits();
        l.max_list_items = 20;
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&wide, values, expressions, SOURCE, l),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, units)| *units == 256));
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&source(), values, expressions, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn grouped_unpivot_every_small_callback_preserves_three_primary_causes_and_ordinary_tails() {
    let fixture = Fixture::new();
    let c = Control::default();
    let input = source();
    let raw = expected();
    fixture.with_tokens(&c, |values, expressions, read| {
        assert_prefix(&c, || {
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
        });
        assert_prefix(&c, || {
            decode_writer_grouped_unpivot(&raw, read, SOURCE, limits())
        });
        assert_prefix(&c, || {
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
        });
        assert_prefix(&c, || {
            prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits())
        });
        let mut bad = source();
        bad.mappings[1].constants[2] = p::UnpivotConstant::Scalar(p::ExprId::new(7));
        assert_prefix(&c, || {
            encode_writer_grouped_unpivot(&bad, values, expressions, SOURCE, limits())
        });
        let mut bad = expected();
        bad.mappings[1].constants[2].kind = Some(wire::unpivot_constant::Kind::ScalarExprId(7));
        assert_prefix(&c, || {
            decode_writer_grouped_unpivot(&bad, read, SOURCE, limits())
        });
        // A prepared emitter does not re-prepare. Each rerun obtains a fresh
        // admitted token before arming only this emitter's original meter.
        c.arm(None);
        let token =
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
                .unwrap();
        c.arm(None);
        token.emit().unwrap();
        let trace = c.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                c.arm(None);
                let token = prepare_writer_grouped_unpivot_encode(
                    &input,
                    values,
                    expressions,
                    SOURCE,
                    limits(),
                )
                .unwrap();
                c.arm(Some((stop, cause.clone())));
                assert!(matches!(token.emit(), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
        c.arm(None);
        let token = prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits()).unwrap();
        c.arm(None);
        token.emit().unwrap();
        let trace = c.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                c.arm(None);
                let token =
                    prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits()).unwrap();
                c.arm(Some((stop, cause.clone())));
                assert!(matches!(token.emit(), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
        c.arm(None);
    });
}
#[test]
fn grouped_unpivot_wide_inner_constants_observe_real_quantum_and_no_retry() {
    let fixture = Fixture::new();
    let c = Control::default();
    let mut input = source();
    input.mappings[0].constants =
        vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
    let mut raw = expected();
    raw.mappings[0].constants = vec![
        wire::UnpivotConstant {
            kind: Some(wire::unpivot_constant::Kind::ScalarExprId(0))
        };
        320
    ];
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()).unwrap();
        let send = c.trace();
        c.arm(None);
        decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).unwrap();
        let receive = c.trace();
        for (decode, trace) in [(false, send), (true, receive)] {
            let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
            for stop in [0, quantum, trace.len() - 1] {
                for cause in CAUSES {
                    c.arm(Some((stop, cause.clone())));
                    let refused = if decode {
                        decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).map(|_| ())
                    } else {
                        encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
                            .map(|_| ())
                    };
                    assert!(matches!(refused, Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(c.trace(), trace[..=stop]);
                }
            }
        }
        c.arm(None);
    });
}

#[test]
fn grouped_known_root_and_nested_requests_precede_the_next_real_wide_quantum() {
    let fixture = Fixture::new();
    let c = Control::default();
    let mut input = source();
    input.mappings[0].constants =
        vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
    let mut raw = expected();
    raw.mappings[0].constants = vec![
        wire::UnpivotConstant {
            kind: Some(wire::unpivot_constant::Kind::ScalarExprId(0)),
        };
        320
    ];
    fixture.with_tokens(&c, |values, expressions, read| {
        for decode in [false, true] {
            c.arm(None);
            if decode {
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).unwrap();
            } else {
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
                    .unwrap();
            }
            let success = c.trace();
            let quantum = success.iter().position(|(_, units)| *units == 256).unwrap();
            let root_bytes = if decode {
                2 * (Layout::array::<p::WriteTargetOrdinal>(3).unwrap().size()
                    + Layout::array::<p::ValueId>(3).unwrap().size()
                    + Layout::array::<p::WriterGroupedUnpivotMapping>(2)
                        .unwrap()
                        .size())
            } else {
                Layout::array::<u32>(6).unwrap().size()
                    + Layout::array::<wire::WriterGroupedUnpivotMapping>(2)
                        .unwrap()
                        .size()
            };
            let first_nested_bytes = if decode {
                2 * Layout::array::<p::UnpivotConstant>(320).unwrap().size()
            } else {
                Layout::array::<wire::UnpivotConstant>(320).unwrap().size()
            };
            for bytes in [root_bytes, root_bytes + first_nested_bytes] {
                let mut l = limits();
                l.max_allocation_request_bytes = bytes - 1;
                c.arm(None);
                let refused = if decode {
                    decode_writer_grouped_unpivot(&raw, read, SOURCE, l).map(|_| ())
                } else {
                    encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, l)
                        .map(|_| ())
                };
                assert!(matches!(
                    refused,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                let refused_trace = c.trace();
                assert!(refused_trace.len() <= quantum);
                assert!(!refused_trace.iter().any(|(_, units)| *units == 256));
                for cause in CAUSES {
                    c.arm(Some((quantum, cause)));
                    let refused = if decode {
                        decode_writer_grouped_unpivot(&raw, read, SOURCE, l).map(|_| ())
                    } else {
                        encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, l)
                            .map(|_| ())
                    };
                    assert!(matches!(
                        refused,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(c.trace(), refused_trace);
                }
            }
        }
        c.arm(None);
    });
}

fn observed_encode(
    input: &p::WriterGroupedUnpivotSpec,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    projection: GroupedProjection,
) -> Result<(wire::WriterGroupedUnpivot, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts =
            preflight_grouped_encode_observed(input, values, expressions, projection, &mut work)?;
        Ok((emit_encode(input, &mut work)?, facts))
    })();
    finish(work, result)
}
fn observed_decode(
    input: &wire::WriterGroupedUnpivot,
    expressions: &DecodedExpressions<'_, '_, '_>,
    projection: GroupedProjection,
) -> Result<(p::WriterGroupedUnpivotSpec, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(expressions.control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = preflight_grouped_decode_observed(input, expressions, projection, &mut work)?;
        Ok((emit_decode(input, &mut work)?, facts))
    })();
    finish(work, result)
}

#[test]
fn grouped_observed_ports_keep_standalone_success_and_ordinary_control_traces() {
    let fixture = Fixture::new();
    let c = Control::default();
    let input = source();
    let raw = expected();
    let projection = GroupedProjection {
        source: SOURCE,
        limits: limits(),
        parent: None,
    };
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let public =
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()).unwrap();
        let trace = c.trace();
        c.arm(None);
        let observed = observed_encode(&input, values, expressions, projection).unwrap();
        assert_eq!(observed, public);
        assert_eq!(c.trace(), trace);
        c.arm(None);
        let public = decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).unwrap();
        let trace = c.trace();
        c.arm(None);
        assert_eq!(observed_decode(&raw, read, projection).unwrap(), public);
        assert_eq!(c.trace(), trace);
        assert_prefix(&c, || {
            observed_encode(&input, values, expressions, projection)
        });
        assert_prefix(&c, || observed_decode(&raw, read, projection));
        let mut bad = raw.clone();
        bad.mappings[1].input_value_id = None;
        c.arm(None);
        assert!(matches!(
            decode_writer_grouped_unpivot(&bad, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        let trace = c.trace();
        c.arm(None);
        assert!(matches!(
            observed_decode(&bad, read, projection),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(c.trace(), trace);
        assert_prefix(&c, || observed_decode(&bad, read, projection));
        c.arm(None);
    });
}

#[test]
fn grouped_nonempty_header_parent_merges_value_occurrences_and_source_invoice_once() {
    let fixture = Fixture::new();
    let c = Control::default();
    let input = source();
    let props = p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    // This is an actual header allocation contribution, not a certified
    // TableFinish or a fabricated namespace. Its writer schemas stay outside
    // this narrow composition oracle.
    let node = p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([props.clone()]),
        output_properties: props,
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([
                p::ValueId::new(0),
                p::ValueId::new(7),
                p::ValueId::new(u32::MAX),
            ]),
        },
        kind: p::NodeKind::TableFinish(p::WriterFinishSpec {
            count_mode: novarocks_physical_plan::FinishCountMode::WriterReported,
            inputs: novarocks_physical_plan::FinishInputs {
                writer_results: 0,
                summaries: Default::default(),
            },
            summaries: Default::default(),
            expected_target_ordinals: Box::default(),
            input_schema: p::WriterRelationSchema {
                revision: 0,
                fields: Box::default(),
            },
            output_schema: p::WriterRelationSchema {
                revision: 0,
                fields: Box::default(),
            },
            final_aggregates: Box::default(),
            grouped_unpivot: Some(input.clone()),
        }),
    };
    fixture.with_tokens(&c, |values, expressions, read| {
        for decode in [false, true] {
            let mut base = Model {
                inputs: 1,
                refs: 3,
                items: 4,
                ..Model::default()
            };
            let header_bytes = if decode {
                base.request::<p::NodeId>(1, 2).unwrap();
                base.request::<p::PhysicalProperties>(1, 2).unwrap();
                base.request::<p::ValueId>(3, 2).unwrap();
                2 * (Layout::array::<p::NodeId>(1).unwrap().size()
                    + Layout::array::<p::PhysicalProperties>(1).unwrap().size()
                    + Layout::array::<p::ValueId>(3).unwrap().size())
            } else {
                encode_header_requests(&node, &mut base).unwrap();
                Layout::array::<u32>(1).unwrap().size()
                    + Layout::array::<wire::PhysicalProperties>(1).unwrap().size()
                    + Layout::array::<u32>(3).unwrap().size()
            };
            c.arm(None);
            let child = if decode {
                decode_writer_grouped_unpivot(&expected(), read, SOURCE, limits())
                    .unwrap()
                    .1
            } else {
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
                    .unwrap()
                    .1
            };
            let requested = header_bytes + child.allocation_request_bytes_upper_bound;
            let height = (usize::BITS - values.count().leading_zeros()) as usize + 1;
            let whole_work = 256
                + 19 * 32
                + 12 * (height + 32)
                + requested * 4
                + child.cumulative_work_upper_bound;
            let whole = NodeProjectionFacts {
                input_node_count: 1,
                value_reference_count: 12,
                list_item_count: 18,
                allocation_requests_upper_bound: if decode { 16 } else { 8 },
                allocation_request_bytes_upper_bound: requested,
                coexisting_source_and_request_bytes_upper_bound: SOURCE + requested,
                cumulative_work_upper_bound: whole_work,
            };
            let mut l = exact(whole);
            l.max_input_nodes = 1;
            let parent = GroupedNodeAdmission {
                base,
                values: values.count(),
                limits: l,
            };
            assert_eq!(
                parent
                    .merge(child)
                    .unwrap()
                    .numerical_facts(SOURCE, values.count(), l)
                    .unwrap(),
                whole
            );
            let run = |parent| {
                let projection = GroupedProjection {
                    source: SOURCE,
                    limits: limits(),
                    parent: Some(parent),
                };
                if decode {
                    observed_decode(&expected(), read, projection).map(|(_, facts)| facts)
                } else {
                    observed_encode(&input, values, expressions, projection).map(|(_, facts)| facts)
                }
            };
            c.arm(None);
            assert_eq!(run(parent).unwrap(), child);
            assert_prefix(&c, || run(parent));
            for axis in 0..7 {
                let mut under_limits = l;
                if axis == 6 {
                    under_limits.max_input_nodes -= 1;
                } else {
                    under_limits = under(under_limits, axis);
                }
                c.arm(None);
                assert!(matches!(
                    run(GroupedNodeAdmission {
                        limits: under_limits,
                        ..parent
                    }),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                let trace = c.trace();
                for cause in CAUSES {
                    c.arm(Some((trace.len(), cause)));
                    assert!(matches!(
                        run(GroupedNodeAdmission {
                            limits: under_limits,
                            ..parent
                        }),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(c.trace(), trace);
                }
            }
        }
        c.arm(None);
    });
}

#[test]
fn grouped_parent_exact_source_occurrences_and_every_small_callback() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let input = source(); let raw = expected();
        for decode in [false, true] {
            let run = |l, snapshots: &mut Vec<NodeProjectionFacts>| {
                let owner = if decode { read.original_control() } else { expressions.original_control() };
                let phase = if decode { CompilePhase::Decode } else { CompilePhase::Encode };
                let mut work = CompileCheckpoints::try_new(owner, phase)?;
                let mut admit = |f: &NodeProjectionFacts| { snapshots.push(*f); Ok(()) };
                let result = if decode {
                    prepare_writer_grouped_unpivot_decode_in(&raw, read, SOURCE, l, &mut admit, &mut work)
                        .and_then(|p| p.emit_in(&mut admit, &mut work)).map(|(out, f)| { assert_eq!(out, input); f })
                } else {
                    prepare_writer_grouped_unpivot_encode_in(&input, values, expressions, SOURCE, l, &mut admit, &mut work)
                        .and_then(|p| p.emit_in(&mut admit, &mut work)).map(|(out, f)| { assert_eq!(out, raw); f })
                };
                finish(work, result)
            };
            c.arm(None); let mut snapshots = vec![];
            let f = run(limits(), &mut snapshots).unwrap(); let trace = c.trace();
            for prefix in snapshots {
                assert!(prefix.allocation_requests_upper_bound <= f.allocation_requests_upper_bound);
                assert!(prefix.cumulative_work_upper_bound <= f.cumulative_work_upper_bound);
                assert_eq!(prefix.coexisting_source_and_request_bytes_upper_bound, SOURCE + prefix.allocation_request_bytes_upper_bound);
            }
            c.arm(None); assert_eq!(run(exact(f), &mut vec![]).unwrap(), f);
            for axis in 0..6 {
                c.arm(None);
                assert!(matches!(run(under(exact(f), axis), &mut vec![]), Err(Error::Control(CompileControlError::ResourceExhausted))));
            }
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(run(limits(), &mut vec![]), Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        }
    });
}

#[test]
fn grouped_parent_known_layout_beats_pending_control_in_both_directions() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let input = source();
        let raw = expected();
        for decode in [false, true] {
            for cause in CAUSES {
                c.arm(Some((1, cause)));
                let owner = if decode {
                    read.original_control()
                } else {
                    expressions.original_control()
                };
                let mut work = CompileCheckpoints::try_new(owner, CompilePhase::Decode).unwrap();
                for _ in 0..255 {
                    work.step().unwrap();
                }
                let mut l = limits();
                l.max_allocation_requests = 0;
                let mut admit = |_: &NodeProjectionFacts| -> Result<(), CompileControlError> {
                    panic!("known layout precedes parent")
                };
                let result = if decode {
                    prepare_writer_grouped_unpivot_decode_in(
                        &raw, read, SOURCE, l, &mut admit, &mut work,
                    )
                    .map(|_| ())
                } else {
                    prepare_writer_grouped_unpivot_encode_in(
                        &input,
                        values,
                        expressions,
                        SOURCE,
                        l,
                        &mut admit,
                        &mut work,
                    )
                    .map(|_| ())
                };
                assert!(matches!(
                    finish(work, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
            }
        }
    });
}
