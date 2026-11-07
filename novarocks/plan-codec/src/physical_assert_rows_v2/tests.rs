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
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::{
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::datatypes::DataType;
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{CompileControlError, FunctionValueType, PureCompileControl};
use std::{alloc::Layout, sync::Mutex};

const SOURCE: usize = 1024 * 1024;
const PRIOR: usize = 64 * 1024;
// The Value phase coexists with the prepared payload owners. Its invoice
// includes their retained state in addition to the original backing.
const VALUE_SOURCE: usize = 256 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        let stop = self.stop.lock().unwrap().clone();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> AssertRowsNodeProjectionLimits {
    AssertRowsNodeProjectionLimits {
        max_input_nodes: 1024,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
            max_work: 64 * 1024 * 1024,
        },
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 16,
        max_payload_bytes: 4096,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 64 * 1024,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 4096,
        max_string_bytes: 1024 * 1024,
    }
}
fn value_limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 1024,
        max_origin_references: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 64 * 1024 * 1024,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 64,
            max_allocation_request_bytes: 64 * 1024,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1024 * 1024,
        },
    }
}
fn definitions() -> Vec<p::ValueDef> {
    [0, 1, 2, 3, u32::MAX]
        .into_iter()
        .map(|id| p::ValueDef {
            id: p::ValueId::new(id),
            ty: FunctionValueType::new(DataType::Int64, id == 2),
            origin: if id == 2 {
                p::ValueOrigin::NullExtended {
                    node: p::NodeId::new(u32::MAX),
                    of: p::ValueId::new(0),
                }
            } else {
                p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(if id == 3 { u32::MAX } else { 0 }),
                    output_ordinal: if id == 3 { 2 } else { id },
                }
            },
        })
        .collect()
}
fn with_namespaces<R>(
    definitions: &[p::ValueDef],
    control: &Control,
    run: impl FnOnce(&EncodedValues<'_, '_, '_>, &DecodedValues<'_, '_, '_>) -> R,
) -> R {
    let roots: Vec<_> = definitions
        .iter()
        .enumerate()
        .map(|(i, value)| (u32::try_from(i).unwrap(), value.ty.clone()))
        .collect();
    let types = encode_type_table_sources(&roots, &[], type_limits(), control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), control).unwrap();
    let payloads = encode_connector_payloads(&[], PRIOR, payload_limits(), control).unwrap();
    let decoded_payloads =
        decode_connector_payloads(payloads.as_wire(), PRIOR, payload_limits(), control).unwrap();
    let inputs: Vec<_> = definitions
        .iter()
        .enumerate()
        .map(|(i, source)| ValueSource {
            source,
            value_type_id: u32::try_from(i).unwrap(),
        })
        .collect();
    let encoded = encode_values(&inputs, &payloads, &types, VALUE_SOURCE, value_limits()).unwrap();
    let decoded = decode_values(
        encoded.as_wire(),
        &decoded_payloads,
        &decoded_types,
        VALUE_SOURCE,
        value_limits(),
    )
    .unwrap();
    control.arm(None);
    run(&encoded, &decoded)
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn required_property() -> p::PhysicalProperties {
    p::passthrough_requirement(&property())
}
fn expected_property() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Singleton(Empty {})),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn expected_required_property() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Unconstrained(Empty {})),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn assert_prefix<T>(
    control: &Control,
    phase: CompilePhase,
    operation: impl Fn() -> Result<T, Error>,
) {
    control.arm(None);
    operation().unwrap_or_else(|e| panic!("success fixture: {e}"));
    let baseline = control.trace();
    assert!(baseline.len() >= 2);
    assert!(baseline.iter().all(|(p, _)| *p == phase));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(operation(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    control.arm(None);
}
fn assert_ordinary_prefix<T>(
    control: &Control,
    phase: CompilePhase,
    operation: impl Fn() -> Result<T, Error>,
) {
    control.arm(None);
    assert!(matches!(operation(), Err(error) if !matches!(error, Error::Control(_))));
    let baseline = control.trace();
    assert!(baseline.len() >= 2);
    assert_eq!(baseline[0], (phase, 0));
    assert_eq!(baseline.last().unwrap().0, phase);
    // A zero-unit tail is legal after delegates flush completed work.
    for at in 0..baseline.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(operation(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    control.arm(None);
}

fn source(spec: p::RowCountAssertionSpec) -> p::PhysicalNode {
    p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([required_property()]),
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
                p::ValueId::new(u32::MAX),
            ]),
        },
        kind: p::NodeKind::AssertOneRow(spec),
    }
}
fn global(comparison: p::RowCountAssertion, desired_rows: u64) -> p::PhysicalNode {
    source(p::RowCountAssertionSpec::Global {
        subject: "行数\0é".into(),
        desired_rows,
        comparison,
    })
}
fn keyed() -> p::PhysicalNode {
    source(p::RowCountAssertionSpec::PerKeyAtMostOne {
        declared_violation: None,
        applicability: novarocks_physical_plan::AssertionApplicability::AllRows,
        keys: Box::from([
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]),
        labels: Box::from([
            Box::<str>::from("Ω\0"),
            Box::<str>::from("第二"),
            Box::<str>::from("Ω\0"),
        ]),
        message: "duplicate\0行".into(),
    })
}
fn expected(kind: wire::row_count_assertion_node::Kind) -> wire::PhysicalNode {
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![expected_required_property()],
        output_properties: Some(expected_property()),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![u32::MAX, 0, u32::MAX],
        }),
        kind: Some(wire::physical_node::Kind::AssertOneRow(
            wire::RowCountAssertionNode { kind: Some(kind) },
        )),
    }
}
fn expected_keyed() -> wire::PhysicalNode {
    expected(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(
        wire::PerKeyRowCountAssertion {
            declared_sql_violation: None,
            applicability: Some(wire::AssertionApplicability {
                kind: Some(wire::assertion_applicability::Kind::AllRows(
                    novarocks_proto_models::physical_control_v2::Empty {},
                )),
            }),
            key_value_ids: vec![u32::MAX, 0, u32::MAX],
            labels: vec!["Ω\0".into(), "第二".into(), "Ω\0".into()],
            message: "duplicate\0行".into(),
        },
    ))
}
fn assertion_mut(node: &mut wire::PhysicalNode) -> &mut wire::RowCountAssertionNode {
    match node.kind.as_mut().unwrap() {
        wire::physical_node::Kind::AssertOneRow(v) => v,
        _ => panic!("fixture kind"),
    }
}

#[test]
fn all_six_global_comparisons_preserve_zero_max_count_and_complete_hand_wire() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        for (comparison, number) in [
            (p::RowCountAssertion::Eq, 1),
            (p::RowCountAssertion::Ne, 2),
            (p::RowCountAssertion::Lt, 3),
            (p::RowCountAssertion::Le, 4),
            (p::RowCountAssertion::Gt, 5),
            (p::RowCountAssertion::Ge, 6),
        ] {
            for count in [0, u64::MAX] {
                control.arm(None);
                let original = global(comparison, count);
                let hand = expected(wire::row_count_assertion_node::Kind::Global(
                    wire::GlobalRowCountAssertion {
                        subject: "行数\0é".into(),
                        desired_rows: count,
                        comparison: number,
                    },
                ));
                let (actual, facts) =
                    encode_assert_rows_node(&original, encoded, SOURCE, limits()).unwrap();
                assert_eq!(actual, hand);
                let (received, receiving) =
                    decode_assert_rows_node(&hand, decoded, SOURCE, limits()).unwrap();
                assert_eq!(received, original);
                assert_eq!(
                    (
                        facts.input_node_count,
                        facts.value_reference_count,
                        facts.list_item_count
                    ),
                    (1, 3, 4)
                );
                assert_eq!(
                    (
                        receiving.input_node_count,
                        receiving.value_reference_count,
                        receiving.list_item_count
                    ),
                    (1, 3, 4)
                );
            }
        }
    });
}

#[test]
fn keyed_ordered_repetitions_unicode_nul_and_property_identity_survive() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = keyed();
        let hand = expected_keyed();
        let (actual, facts) =
            encode_assert_rows_node(&original, encoded, SOURCE, limits()).unwrap();
        assert_eq!(actual, hand);
        let (received, receiving) =
            decode_assert_rows_node(&hand, decoded, SOURCE, limits()).unwrap();
        assert_eq!(received, original);
        assert_eq!(
            (facts.value_reference_count, facts.list_item_count),
            (6, 10)
        );
        assert_eq!(
            (receiving.value_reference_count, receiving.list_item_count),
            (6, 10)
        );
        // Test sparse node IDs independently of semantic Fragment closure.
        let mut zero = original.clone();
        zero.id = p::NodeId::new(0);
        zero.output.node = p::NodeId::new(0);
        zero.inputs = Box::from([p::NodeId::new(u32::MAX), p::NodeId::new(0)]);
        zero.required_inputs = Box::from([required_property(), required_property()]);
        let (actual, _) = encode_assert_rows_node(&zero, encoded, SOURCE, limits()).unwrap();
        assert_eq!(actual.input_node_ids, [u32::MAX, 0]);
        assert_eq!(
            decode_assert_rows_node(&actual, decoded, SOURCE, limits())
                .unwrap()
                .0,
            zero
        );
    });
}

#[test]
fn original_fragment_constructor_remains_the_semantic_owner_after_receiving() {
    let control = Control::default();
    // Make lawful child output origins; the codec is not their registry owner.
    // The existing mutable Value allocator cannot insert MAX because it
    // must retain a next identity. Other hand-wire tests independently cover
    // 0/MAX; this original-constructor composition uses lawful Value IDs 0/1.
    let mut defs: Vec<_> = definitions()
        .into_iter()
        .filter(|v| matches!(v.id.get(), 0 | u32::MAX))
        .collect();
    for value in &mut defs {
        let maximum = value.id.get() == u32::MAX;
        if maximum {
            value.id = p::ValueId::new(1);
        }
        value.origin = p::ValueOrigin::NodeOutput {
            node: p::NodeId::new(0),
            output_ordinal: if maximum { 0 } else { 1 },
        };
    }
    let child = p::PhysicalNode {
        id: p::NodeId::new(0),
        inputs: Box::default(),
        required_inputs: Box::default(),
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(0),
            columns: Box::from([p::ValueId::new(1), p::ValueId::new(0)]),
        },
        kind: p::NodeKind::Values {
            rows: Box::default(),
        },
    };
    with_namespaces(&defs, &control, |encoded, decoded| {
        for original in [global(p::RowCountAssertion::Ge, 0), keyed()] {
            let mut original = original;
            if let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                ..
            }) = &mut original.kind
            {
                for key in keys {
                    if key.get() == u32::MAX {
                        *key = p::ValueId::new(1);
                    }
                }
            }
            // Actual builder assertion requires the precise child distribution.
            original.required_inputs = Box::from([property()]);
            original.output.columns = child.output.columns.clone();
            let received = decode_assert_rows_node(
                &encode_assert_rows_node(&original, encoded, SOURCE, limits())
                    .unwrap()
                    .0,
                decoded,
                SOURCE,
                limits(),
            )
            .unwrap()
            .0;
            let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
            for id in [1, 0] {
                builder
                    .insert_value(defs.iter().find(|v| v.id.get() == id).unwrap().clone())
                    .unwrap();
            }
            builder.insert_node_unchecked(child.clone()).unwrap();
            builder.insert_node_unchecked(received).unwrap();
            builder
                .finish_structure(
                    p::NodeId::new(u32::MAX),
                    p::FragmentSink::Noop,
                    p::PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false,
                    },
                    p::PlanLimits::FROZEN,
                    &control,
                    novarocks_physical_plan::StagePlacementPolicy::Singleton,
                )
                .unwrap();
        }
        let mut empty = source(p::RowCountAssertionSpec::Global {
            subject: "".into(),
            desired_rows: 0,
            comparison: p::RowCountAssertion::Eq,
        });
        empty.output.columns = child.output.columns.clone();
        // The wire can represent empty text. It does not relax original semantics.
        let received = decode_assert_rows_node(
            &encode_assert_rows_node(&empty, encoded, SOURCE, limits())
                .unwrap()
                .0,
            decoded,
            SOURCE,
            limits(),
        )
        .unwrap()
        .0;
        let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
        for id in [1, 0] {
            builder
                .insert_value(defs.iter().find(|v| v.id.get() == id).unwrap().clone())
                .unwrap();
        }
        builder.insert_node_unchecked(child.clone()).unwrap();
        builder.insert_node_unchecked(received).unwrap();
        assert!(
            builder
                .finish_structure(
                    p::NodeId::new(u32::MAX),
                    p::FragmentSink::Noop,
                    p::PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false
                    },
                    p::PlanLimits::FROZEN,
                    &control,
                    novarocks_physical_plan::StagePlacementPolicy::Singleton
                )
                .is_err()
        );
    });
}

#[test]
fn wire_presence_unknown_comparison_and_exact_value_namespace_refuse() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        for bad_number in [0, -1, 7, i32::MAX] {
            let bad = expected(wire::row_count_assertion_node::Kind::Global(
                wire::GlobalRowCountAssertion {
                    subject: "x".into(),
                    desired_rows: 0,
                    comparison: bad_number,
                },
            ));
            assert!(matches!(
                decode_assert_rows_node(&bad, decoded, SOURCE, limits()),
                Err(Error::InvalidShape(
                    "row-count comparison is absent or unknown"
                ))
            ));
        }
        for case in 0..8 {
            let mut bad = expected_keyed();
            match case {
                0 => bad.kind = None,
                1 => assertion_mut(&mut bad).kind = None,
                2 => bad.output = None,
                3 => bad.output.as_mut().unwrap().node_id = None,
                4 => bad.output_properties = None,
                5 => bad.output.as_mut().unwrap().value_ids[0] = 999,
                6 => {
                    if let Some(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v)) =
                        assertion_mut(&mut bad).kind.as_mut()
                    {
                        v.key_value_ids[0] = 999;
                    }
                }
                7 => bad.required_inputs[0].row_multiplicity = 0,
                _ => unreachable!(),
            }
            assert!(
                decode_assert_rows_node(&bad, decoded, SOURCE, limits()).is_err(),
                "presence/reference case {case}"
            );
        }
        let mut missing = keyed();
        if let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne {
            keys, ..
        }) = &mut missing.kind
        {
            keys[0] = p::ValueId::new(999);
        }
        assert!(encode_assert_rows_node(&missing, encoded, SOURCE, limits()).is_err());
        let wrong = p::PhysicalNode {
            kind: p::NodeKind::Limit {
                limit: Some(0),
                offset: 0,
            },
            ..keyed()
        };
        assert!(encode_assert_rows_node(&wrong, encoded, SOURCE, limits()).is_err());
    });
}

#[test]
fn independent_owner_layout_totals_and_every_exact_resource_ceiling() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = keyed();
        let hand = expected_keyed();
        let text = "Ω\0".len() * 2 + "第二".len() + "duplicate\0行".len();
        // Actual output owners, not the shared Model arithmetic: three header
        // vectors, one keys vector, one String/Box slice, four owned texts.
        let encode_bytes = Layout::array::<u32>(1).unwrap().size()
            + Layout::array::<wire::PhysicalProperties>(1).unwrap().size()
            + 2 * Layout::array::<u32>(3).unwrap().size()
            + Layout::array::<String>(3).unwrap().size()
            + text;
        let decode_bytes = 2
            * (Layout::array::<p::NodeId>(1).unwrap().size()
                + Layout::array::<p::PhysicalProperties>(1).unwrap().size()
                + 2 * Layout::array::<p::ValueId>(3).unwrap().size()
                + Layout::array::<Box<str>>(3).unwrap().size()
                + text);
        for receiving in [false, true] {
            let run = |l| {
                if receiving {
                    decode_assert_rows_node(&hand, decoded, SOURCE, l).map(|(_, f)| f)
                } else {
                    encode_assert_rows_node(&original, encoded, SOURCE, l).map(|(_, f)| f)
                }
            };
            let facts = run(limits()).unwrap();
            assert_eq!(
                facts.allocation_request_bytes_upper_bound,
                if receiving {
                    decode_bytes
                } else {
                    encode_bytes
                }
            );
            assert_eq!(
                facts.allocation_requests_upper_bound,
                if receiving { 18 } else { 9 }
            );
            assert_eq!(
                facts.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + facts.allocation_request_bytes_upper_bound
            );
            let exact = AssertRowsNodeProjectionLimits {
                max_input_nodes: facts.input_node_count,
                max_value_references: facts.value_reference_count,
                max_list_items: facts.list_item_count,
                max_allocation_requests: facts.allocation_requests_upper_bound,
                max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
                max_coexisting_source_and_request_bytes: facts
                    .coexisting_source_and_request_bytes_upper_bound,
                max_work: facts.cumulative_work_upper_bound,
                ..limits()
            };
            assert_eq!(run(exact).unwrap(), facts);
            for dimension in 0..7 {
                let mut under = exact;
                match dimension {
                    0 => under.max_input_nodes -= 1,
                    1 => under.max_value_references -= 1,
                    2 => under.max_list_items -= 1,
                    3 => under.max_allocation_requests -= 1,
                    4 => under.max_allocation_request_bytes -= 1,
                    5 => under.max_coexisting_source_and_request_bytes -= 1,
                    6 => under.max_work -= 1,
                    _ => unreachable!(),
                }
                assert!(
                    matches!(
                        run(under),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ),
                    "resource dimension {dimension}"
                );
            }
        }
    });
}

#[test]
fn original_invoice_and_unused_received_string_capacity_dominate_before_copy() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        assert!(encode_assert_rows_node(&keyed(), encoded, VALUE_SOURCE - 1, limits()).is_err());
        assert!(
            decode_assert_rows_node(&expected_keyed(), decoded, VALUE_SOURCE - 1, limits())
                .is_err()
        );
        let mut wire = expected_keyed();
        if let Some(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v)) =
            assertion_mut(&mut wire).kind.as_mut()
        {
            v.labels[0].reserve(2 * SOURCE);
        }
        assert!(decode_assert_rows_node(&wire, decoded, SOURCE, limits()).is_err());
        let mut ordinary = global(p::RowCountAssertion::Eq, 0);
        if let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::Global { subject, .. }) =
            &mut ordinary.kind
        {
            *subject = "large".repeat(SOURCE / 5).into_boxed_str();
        }
        assert!(encode_assert_rows_node(&ordinary, encoded, SOURCE, limits()).is_err());
    });
}

#[test]
fn sealed_preparation_emits_without_repeating_namespace_lookup_or_source_clones() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = keyed();
        let hand = expected_keyed();
        let prepared =
            prepare_assert_rows_node_encode(&original, encoded, SOURCE, limits()).unwrap();
        let prepared_facts = *prepared.facts();
        control.arm(None);
        let (actual, facts) = prepared.emit().unwrap();
        assert_eq!(facts, prepared_facts);
        assert_eq!(actual, hand);
        let prepared = prepare_assert_rows_node_decode(&hand, decoded, SOURCE, limits()).unwrap();
        let receiving_facts = *prepared.facts();
        control.arm(None);
        let (actual, facts) = prepared.emit().unwrap();
        assert_eq!(facts, receiving_facts);
        assert_eq!(actual, original);
        control.arm(None);
    });
}

#[test]
fn every_small_success_and_ordinary_tail_callback_keeps_three_primary_causes() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = keyed();
        let wire = expected_keyed();
        assert_prefix(&control, CompilePhase::Encode, || {
            encode_assert_rows_node(&original, encoded, SOURCE, limits())
        });
        assert_prefix(&control, CompilePhase::Decode, || {
            decode_assert_rows_node(&wire, decoded, SOURCE, limits())
        });
        let mut invalid = wire.clone();
        if let Some(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v)) =
            assertion_mut(&mut invalid).kind.as_mut()
        {
            v.key_value_ids[2] = 999;
        }
        assert_ordinary_prefix(&control, CompilePhase::Decode, || {
            decode_assert_rows_node(&invalid, decoded, SOURCE, limits())
        });
        let mut l = limits();
        l.max_allocation_request_bytes = 0;
        control.arm(None);
        assert!(matches!(
            encode_assert_rows_node(&original, encoded, SOURCE, l),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        let resource_prefix = control.trace();
        assert_eq!(resource_prefix.first(), Some(&(CompilePhase::Encode, 0)));
        for cause in CAUSES {
            for at in 0..resource_prefix.len() {
                control.arm(Some((at, cause.clone())));
                assert!(matches!(
                    encode_assert_rows_node(&original, encoded, SOURCE, l),
                    Err(Error::Control(actual)) if actual == cause
                ));
                assert_eq!(control.trace(), resource_prefix[..=at]);
            }
            control.arm(Some((resource_prefix.len(), cause)));
            assert!(matches!(
                encode_assert_rows_node(&original, encoded, SOURCE, l),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), resource_prefix);
        }
        let bad = expected(wire::row_count_assertion_node::Kind::Global(
            wire::GlobalRowCountAssertion {
                subject: "valid".into(),
                desired_rows: 0,
                comparison: 0,
            },
        ));
        assert_ordinary_prefix(&control, CompilePhase::Decode, || {
            decode_assert_rows_node(&bad, decoded, SOURCE, limits())
        });
    });
}

#[test]
fn wide_ordered_keys_and_utf8_copy_reach_actual_quantum_without_after_refusal() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let mut original = keyed();
        if let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: novarocks_physical_plan::AssertionApplicability::AllRows,
            keys,
            labels,
            message,
        }) = &mut original.kind
        {
            *keys = vec![p::ValueId::new(u32::MAX); 320].into_boxed_slice();
            *labels = (0..320)
                .map(|_| Box::<str>::from("key"))
                .collect::<Vec<_>>()
                .into_boxed_slice();
            *message = format!("{}\0尾", "é".repeat(161)).into_boxed_str();
        }
        let wire = encode_assert_rows_node(&original, encoded, SOURCE, limits())
            .unwrap()
            .0;
        for receiving in [false, true] {
            let run = || {
                if receiving {
                    decode_assert_rows_node(&wire, decoded, SOURCE, limits()).map(|(_, f)| f)
                } else {
                    encode_assert_rows_node(&original, encoded, SOURCE, limits()).map(|(_, f)| f)
                }
            };
            control.arm(None);
            let facts = run().unwrap();
            let baseline = control.trace();
            assert!(baseline.iter().any(|(_, units)| *units == 256));
            assert!(
                baseline.iter().map(|(_, n)| *n as usize).sum::<usize>()
                    <= facts.cumulative_work_upper_bound
            );
            for at in (0..baseline.len())
                .filter(|at| *at == 0 || *at + 1 == baseline.len() || baseline[*at].1 == 256)
            {
                for cause in CAUSES {
                    control.arm(Some((at, cause.clone())));
                    assert!(matches!(run(), Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(control.trace(), baseline[..=at]);
                }
            }
            control.arm(None);
        }
    });
}

#[path = "owned_tests.rs"]
mod owned_tests;

#[test]
fn applicability_is_one_original_reference_without_a_new_buffer() {
    let control = Control::default();
    let mut defs = definitions();
    defs.push(p::ValueDef {
        id: p::ValueId::new(7),
        ty: FunctionValueType::new(DataType::Boolean, true),
        origin: p::ValueOrigin::NodeOutput {
            node: p::NodeId::new(0),
            output_ordinal: 7,
        },
    });
    with_namespaces(&defs, &control, |encoded, decoded| {
        let all = keyed();
        control.arm(None);
        let (all_wire, all_facts) =
            encode_assert_rows_node(&all, encoded, SOURCE, limits()).unwrap();
        let mut conditional = all.clone();
        let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne {
            applicability,
            ..
        }) = &mut conditional.kind
        else {
            panic!("keyed assertion")
        };
        *applicability = p::AssertionApplicability::WhenTrue(p::ValueId::new(7));
        control.arm(None);
        let (raw, facts) =
            encode_assert_rows_node(&conditional, encoded, SOURCE, limits()).unwrap();
        assert_eq!(
            facts.value_reference_count,
            all_facts.value_reference_count + 1
        );
        assert_eq!(facts.list_item_count, all_facts.list_item_count + 1);
        assert_eq!(
            facts.allocation_requests_upper_bound,
            all_facts.allocation_requests_upper_bound
        );
        assert_eq!(
            facts.allocation_request_bytes_upper_bound,
            all_facts.allocation_request_bytes_upper_bound
        );
        let mut below = limits();
        below.max_value_references = all_facts.value_reference_count;
        control.arm(None);
        assert!(encode_assert_rows_node(&conditional, encoded, SOURCE, below).is_err());
        control.arm(None);
        let (_, all_received) =
            decode_assert_rows_node(&all_wire, decoded, SOURCE, limits()).unwrap();
        control.arm(None);
        let (owned, received) = decode_assert_rows_node(&raw, decoded, SOURCE, limits()).unwrap();
        assert_eq!(owned, conditional);
        assert_eq!(
            received.value_reference_count,
            all_received.value_reference_count + 1
        );
        assert_eq!(received.list_item_count, all_received.list_item_count + 1);
        control.arm(None);
        assert!(decode_assert_rows_node(&raw, decoded, SOURCE, below).is_err());
    });
}

#[test]
fn declared_sql_assertion_reason_round_trips_and_unknown_identity_refuses() {
    use novarocks_failure_contract::sql_reason;
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        for reason in [
            sql_reason::MERGE_TARGET_ROW_MULTIPLE_MATCHES,
            sql_reason::UPDATE_TARGET_ROW_MULTIPLE_MATCHES,
        ] {
            let mut original = keyed();
            let p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne {
                declared_violation,
                ..
            }) = &mut original.kind
            else {
                unreachable!()
            };
            *declared_violation = Some(reason);
            let (wire, _) = encode_assert_rows_node(&original, encoded, SOURCE, limits()).unwrap();
            assert_eq!(
                decode_assert_rows_node(&wire, decoded, SOURCE, limits())
                    .unwrap()
                    .0,
                original
            );
        }
        let mut wire = expected_keyed();
        let Some(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(spec)) =
            &mut assertion_mut(&mut wire).kind
        else {
            unreachable!()
        };
        spec.declared_sql_violation = Some(14);
        assert!(decode_assert_rows_node(&wire, decoded, SOURCE, limits()).is_err());
    });
}
