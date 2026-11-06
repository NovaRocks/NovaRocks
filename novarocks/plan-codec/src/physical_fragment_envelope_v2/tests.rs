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
use arrow::datatypes::DataType;
use novarocks_type_contract::CompileControlError;
use std::{alloc::Layout, sync::Mutex};

const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        events.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
fn limits() -> FragmentEnvelopeProjectionLimits {
    FragmentEnvelopeProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1 << 20,
        max_coexisting_source_and_request_bytes: 4 << 20,
        max_work: 64 << 20,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1024,
        },
    }
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> p::PipelineDopDomain {
    p::PipelineDopDomain {
        min: 2,
        max: 8,
        requires_power_of_two: true,
    }
}
fn route(
    id: u8,
    target: u32,
    edge: u32,
    effects: Box<[ConnectorRowMutationEffect]>,
) -> p::ChangeStreamRoute {
    p::ChangeStreamRoute {
        route_id: ConnectorWriteRouteId::from_bytes([id; 32]),
        write_target_ordinal: WriteTargetOrdinal::try_new(target).unwrap(),
        accepted_effects: effects,
        input_mapping: Box::from([
            (
                ConnectorWriteFieldToken::from_bytes([3; 32]),
                p::ValueId::new(1),
            ),
            (
                ConnectorWriteFieldToken::from_bytes([4; 32]),
                p::ValueId::new(1),
            ),
        ]),
        requirements: p::WriteRouteRequirements {
            colocation: p::WriteColocation::ByKey(Box::from([p::ValueId::new(1)])),
            layout: p::WriteLayoutPreference::None,
        },
        edge: p::EdgeId::new(edge),
    }
}
fn summary() -> p::SummaryOutput {
    p::SummaryOutput {
        edge: p::EdgeId::new(1),
        columns: p::SummaryColumns::write_counts(
            p::ValueId::new(2),
            p::ValueId::new(3),
            p::ValueId::new(4),
        ),
        author: p::SummaryAuthor::RouterSink,
    }
}
fn router() -> p::FragmentSink {
    p::FragmentSink::Router {
        effect: p::ValueId::new(0),
        routes: Box::from([
            route(
                9,
                0,
                u32::MAX,
                Box::from([
                    ConnectorRowMutationEffect::Insert,
                    ConnectorRowMutationEffect::Delete,
                ]),
            ),
            route(8, 1, 0, Box::from([ConnectorRowMutationEffect::Replace])),
        ]),
    }
}
// Real construction author, with an exact generated Int8 effect and nullable
// assigned data. Header projection does not independently certify this plan.
fn fragment(sink: p::FragmentSink, wide: bool) -> p::Fragment {
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(u32::MAX));
    builder
        .add_values(
            p::NodeId::new(0),
            Box::from([Box::<[p::ExprId]>::default()]),
            Box::default(),
        )
        .unwrap();
    let root = p::NodeId::new(u32::MAX);
    builder
        .insert_value(p::ValueDef {
            id: p::ValueId::new(0),
            ty: p::ValueType::new(DataType::Int8, false),
            origin: p::ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 0,
            },
        })
        .unwrap();
    builder
        .insert_value(p::ValueDef {
            id: p::ValueId::new(1),
            ty: p::ValueType::new(DataType::Int64, true),
            origin: p::ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 1,
            },
        })
        .unwrap();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: root,
            inputs: Box::from([p::NodeId::new(0)]),
            required_inputs: Box::from([property()]),
            output_properties: property(),
            output: p::OutputPort {
                node: root,
                columns: Box::from([p::ValueId::new(0), p::ValueId::new(1)]),
            },
            kind: p::NodeKind::ChangeEventExpand {
                events: Box::from([p::ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([(p::ValueId::new(1), None)]),
                }]),
                effect_output: p::ValueId::new(0),
            },
        })
        .unwrap();
    if matches!(&sink, p::FragmentSink::Router { .. }) {
        let summary = summary();
        builder.set_summaries(Box::new([summary]));
        for (field, id) in summary.columns.fields() {
            builder
                .insert_value(p::ValueDef {
                    id,
                    ty: p::ValueType::new(DataType::Int64, false),
                    origin: p::ValueOrigin::Summary { field },
                })
                .unwrap();
        }
    }
    for id in if wide {
        (0..319)
            .chain(std::iter::once(u32::MAX))
            .collect::<Vec<_>>()
    } else {
        vec![0, u32::MAX]
    } {
        builder
            .attach_runtime_filter(p::RuntimeFilterId::new(id))
            .unwrap();
    }
    builder
        .finish_structure(
            root,
            sink,
            dop(),
            p::PlanLimits::FROZEN,
            &Control::default(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn raw(output: EncodedFragmentEnvelope) -> wire::Fragment {
    // Other component tables are intentionally absent from this raw envelope
    // fixture; this is not a decoded Fragment or Package publication.
    wire::Fragment {
        id: output.id,
        root_node_id: Some(output.root_node_id),
        sink: Some(output.sink),
        summaries: output.summaries,
        dop_domain: Some(output.dop_domain),
        stage_placement: Some(output.stage_placement),
        runtime_filter_ids: output.runtime_filter_ids,
        ..Default::default()
    }
}
fn encoded(source: &p::Fragment) -> (wire::Fragment, FragmentEnvelopeProjectionFacts) {
    let control = Control::default();
    let prepared = prepare_fragment_envelope_encode(source, SOURCE, limits(), &control).unwrap();
    let expected = *prepared.facts();
    let (output, facts) = prepared.emit().unwrap();
    assert_eq!(facts, expected);
    (raw(output), facts)
}
fn decode(
    source: &wire::Fragment,
    l: FragmentEnvelopeProjectionLimits,
    c: &Control,
) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
    let prepared = prepare_fragment_envelope_decode(source, SOURCE, l, c)?;
    let expected = *prepared.facts();
    let (output, facts) = prepared.emit()?;
    assert_eq!(facts, expected);
    Ok((output, facts))
}
fn raw_router(source: &mut wire::Fragment) -> &mut wire::RouterSink {
    let wire::fragment_sink::Kind::Router(router) =
        source.sink.as_mut().unwrap().kind.as_mut().unwrap()
    else {
        panic!("real router fixture")
    };
    router
}
fn prefixes<T>(call: impl Fn(&Control) -> Result<T, Error>, success: bool, wide: bool) {
    let baseline = Control::default();
    assert_eq!(call(&baseline).is_ok(), success);
    let events = baseline.trace();
    assert!(!events.is_empty());
    assert_eq!(events[0].1, 0);
    if wide {
        assert!(events.iter().any(|(_, units)| *units == 256));
    }
    for (at, (_, units)) in events.iter().enumerate() {
        if wide && at != 0 && at + 1 != events.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = Control {
                events: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(matches!(call(&control),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(control.trace(), events[..=at]);
        }
    }
}
#[test]
fn envelope_all_five_sinks_preserve_hand_authored_headers_and_ordered_routes() {
    for sink in [
        p::FragmentSink::Result,
        p::FragmentSink::Noop,
        p::FragmentSink::Stream {
            edge: p::EdgeId::new(0),
        },
        p::FragmentSink::Multicast {
            edges: Box::from([p::EdgeId::new(u32::MAX), p::EdgeId::new(0)]),
        },
        router(),
    ] {
        let source = fragment(sink.clone(), false);
        let (raw, _) = encoded(&source);
        assert_eq!(raw.id, u32::MAX);
        assert_eq!(raw.root_node_id, Some(u32::MAX));
        assert_eq!(raw.runtime_filter_ids, [0, u32::MAX]);
        assert_eq!(
            raw.dop_domain,
            Some(wire::PipelineDopDomain {
                min: 2,
                max: 8,
                requires_power_of_two: true
            })
        );
        if let Some(wire::fragment_sink::Kind::Router(v)) =
            raw.sink.as_ref().and_then(|v| v.kind.as_ref())
        {
            assert_eq!(v.effect_value_id, Some(0));
            assert_eq!(
                raw.summaries,
                [wire::SummaryOutput {
                    edge_id: Some(1),
                    columns: Some(wire::SummaryColumns {
                        kind: 1,
                        value_ids: vec![2, 3, 4]
                    }),
                    author: Some(wire::SummaryAuthor {
                        kind: Some(wire::summary_author::Kind::RouterSink(Empty {}))
                    }),
                }]
            );
            assert_eq!(v.routes.len(), 2);
            assert_eq!(v.routes[0].route_id, [9; 32]);
            assert_eq!(v.routes[1].route_id, [8; 32]);
            assert_eq!(
                v.routes[0].accepted_effects,
                [
                    wire::RowMutationEffect::Insert as i32,
                    wire::RowMutationEffect::Delete as i32
                ]
            );
            assert_eq!(
                v.routes[1].accepted_effects,
                [wire::RowMutationEffect::Replace as i32]
            );
            assert_eq!(v.routes[0].edge_id, Some(u32::MAX));
            assert_eq!(v.routes[1].edge_id, Some(0));
            assert_eq!(
                v.routes[0]
                    .input_mapping
                    .iter()
                    .map(|v| v.value_id)
                    .collect::<Vec<_>>(),
                [Some(1), Some(1)]
            );
            assert_eq!(v.routes[0].input_mapping[0].field_token, [3; 32]);
            assert_eq!(v.routes[0].input_mapping[1].field_token, [4; 32]);
        }
        let (owned, _) = decode(&raw, limits(), &Control::default()).unwrap();
        assert_eq!(owned.id, source.id());
        assert_eq!(owned.root, source.root());
        assert_eq!(owned.sink, sink);
        assert_eq!(owned.dop_domain, dop());
        assert_eq!(&*owned.runtime_filters, source.runtime_filters());
    }
}
#[test]
fn envelope_presence_unknown_effect_tokens_and_original_ordinal_author_reject() {
    let (source, _) = encoded(&fragment(router(), false));
    for fault in 0..18 {
        let mut bad = source.clone();
        match fault {
            0 => bad.root_node_id = None,
            1 => bad.sink = None,
            2 => bad.dop_domain = None,
            3 => bad.sink.as_mut().unwrap().kind = None,
            4 => raw_router(&mut bad).effect_value_id = None,
            5 => raw_router(&mut bad).routes[0].edge_id = None,
            6 => raw_router(&mut bad).routes[0].input_mapping[0].value_id = None,
            7 => raw_router(&mut bad).routes[0].accepted_effects[0] = 0,
            8 => raw_router(&mut bad).routes[0].accepted_effects[0] = i32::MAX,
            9 => raw_router(&mut bad).routes[0]
                .route_id
                .pop()
                .map(|_| ())
                .unwrap(),
            10 => raw_router(&mut bad).routes[0].input_mapping[0]
                .field_token
                .push(0),
            11 => raw_router(&mut bad).routes[0].write_target_ordinal = 4096,
            12 => bad.summaries[0].author = None,
            13 => bad.summaries[0].edge_id = None,
            14 => bad.summaries[0].columns = None,
            15 => bad.summaries[0].columns.as_mut().unwrap().kind = 0,
            16 => {
                bad.summaries[0]
                    .columns
                    .as_mut()
                    .unwrap()
                    .value_ids
                    .remove(1);
            }
            17 => {
                bad.summaries[0].columns.as_mut().unwrap().value_ids.push(7);
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(
                decode(&bad, limits(), &Control::default()),
                Err(Error::InvalidShape(_) | Error::Identity(_))
            ),
            "fault {fault}"
        );
    }
    let mut bad = source.clone();
    raw_router(&mut bad).routes[0].write_target_ordinal = u32::MAX;
    assert!(matches!(
        decode(&bad, limits(), &Control::default()),
        Err(Error::Identity(_))
    ));
    let mut representable = source;
    raw_router(&mut representable).routes[0].write_target_ordinal = 4095;
    let (owned, _) = decode(&representable, limits(), &Control::default()).unwrap();
    let p::FragmentSink::Router { routes, .. } = owned.sink else {
        panic!("router")
    };
    assert_eq!(routes[0].write_target_ordinal.get(), 4095);
    // Dense route association is the original mandatory semantic gate's job.
}
#[test]
fn envelope_zero_max_dop_and_sparse_repeated_references_are_not_defaulted_or_certified() {
    let (mut raw, _) = encoded(&fragment(router(), false));
    raw.id = 0;
    raw.root_node_id = Some(0);
    raw.dop_domain = Some(wire::PipelineDopDomain {
        min: 0,
        max: u32::MAX,
        requires_power_of_two: false,
    });
    raw.runtime_filter_ids = vec![u32::MAX, 0, u32::MAX];
    raw_router(&mut raw).effect_value_id = Some(u32::MAX);
    let (parts, _) = decode(&raw, limits(), &Control::default()).unwrap();
    assert_eq!(parts.id, p::FragmentId::new(0));
    assert_eq!(parts.root, p::NodeId::new(0));
    assert_eq!(
        parts.dop_domain,
        p::PipelineDopDomain {
            min: 0,
            max: u32::MAX,
            requires_power_of_two: false
        }
    );
    assert_eq!(
        &*parts.runtime_filters,
        &[
            p::RuntimeFilterId::new(u32::MAX),
            p::RuntimeFilterId::new(0),
            p::RuntimeFilterId::new(u32::MAX)
        ]
    );
    assert!(
        matches!(parts.sink,p::FragmentSink::Router{effect,..} if effect==p::ValueId::new(u32::MAX))
    );
    // No semantic DOP or reference validation is duplicated by this component.
}
fn layout<T>(n: usize) -> usize {
    Layout::array::<T>(n).unwrap().size()
}
#[test]
fn envelope_independent_layout_golden_counts_separate_vec_box_requests_and_retained_backing() {
    let source = fragment(router(), false);
    let (raw, sent) = encoded(&source);
    let (owned, received) = decode(&raw, limits(), &Control::default()).unwrap();
    let send_bytes = layout::<u32>(2)
        + layout::<wire::ChangeStreamRoute>(2)
        + 2 * 32
        + layout::<i32>(3)
        + 2 * layout::<wire::WriteInputMapping>(2)
        + 2 * layout::<u32>(1)
        + 4 * 32
        + layout::<wire::SummaryOutput>(1)
        + layout::<u32>(3);
    let receive_retained = layout::<p::RuntimeFilterId>(2)
        + layout::<p::ChangeStreamRoute>(2)
        + layout::<ConnectorRowMutationEffect>(3)
        + 2 * layout::<(ConnectorWriteFieldToken, p::ValueId)>(2)
        + 2 * layout::<p::ValueId>(1)
        + layout::<p::SummaryOutput>(1);
    assert_eq!(sent.input_node_count, 0);
    assert_eq!(received.input_node_count, 0);
    assert_eq!(sent.value_reference_count, 11);
    assert_eq!(received.value_reference_count, 11);
    assert_eq!(sent.list_item_count, 17);
    assert_eq!(received.list_item_count, 17);
    assert_eq!(sent.allocation_requests_upper_bound, 16);
    assert_eq!(received.allocation_requests_upper_bound, 18);
    assert_eq!(sent.allocation_request_bytes_upper_bound, send_bytes);
    assert_eq!(
        received.allocation_request_bytes_upper_bound,
        2 * receive_retained
    );
    assert_eq!(
        sent.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + send_bytes
    );
    assert_eq!(
        received.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + 2 * receive_retained
    );
    let p::FragmentSink::Router { routes, .. } = owned.sink else {
        panic!("router")
    };
    assert_eq!(owned.runtime_filters.len(), 2);
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].input_mapping.len(), 2);
    // Output retained allocations use one copy; the request/peak model covers
    // Vec plus Box coexistence with two copies. These are numerical facts only.
}
fn exact(f: FragmentEnvelopeProjectionFacts) -> FragmentEnvelopeProjectionLimits {
    FragmentEnvelopeProjectionLimits {
        max_input_nodes: f.input_node_count,
        max_value_references: f.value_reference_count,
        max_list_items: f.list_item_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
        ..limits()
    }
}
fn under(mut l: FragmentEnvelopeProjectionLimits, axis: usize) -> FragmentEnvelopeProjectionLimits {
    match axis {
        0 => l.max_value_references -= 1,
        1 => l.max_list_items -= 1,
        2 => l.max_allocation_requests -= 1,
        3 => l.max_allocation_request_bytes -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        5 => l.max_work -= 1,
        _ => unreachable!(),
    };
    l
}
#[test]
fn envelope_all_nonzero_axes_exact_one_under_and_source_floor_precede_emission() {
    let source = fragment(router(), false);
    let (raw, sent) = encoded(&source);
    let (_, received) = decode(&raw, limits(), &Control::default()).unwrap();
    prepare_fragment_envelope_encode(&source, SOURCE, exact(sent), &Control::default())
        .unwrap()
        .emit()
        .unwrap();
    decode(&raw, exact(received), &Control::default()).unwrap();
    for axis in 0..6 {
        assert!(
            matches!(
                prepare_fragment_envelope_encode(
                    &source,
                    SOURCE,
                    under(exact(sent), axis),
                    &Control::default()
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "encode axis {axis}"
        );
        assert!(
            matches!(
                decode(&raw, under(exact(received), axis), &Control::default()),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "decode axis {axis}"
        );
    }
    assert!(matches!(
        prepare_fragment_envelope_encode(&source, 0, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    assert!(matches!(
        prepare_fragment_envelope_decode(&raw, 0, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    let mut capacity_source = raw.clone();
    raw_router(&mut capacity_source).routes[1].input_mapping[1]
        .field_token
        .reserve_exact(4096);
    let wire::fragment_sink::Kind::Router(router) = capacity_source
        .sink
        .as_ref()
        .unwrap()
        .kind
        .as_ref()
        .unwrap()
    else {
        panic!("router")
    };
    let known = size_of::<wire::Fragment>()
        + layout::<u32>(capacity_source.runtime_filter_ids.capacity())
        + layout::<wire::ChangeStreamRoute>(router.routes.capacity())
        + router
            .routes
            .iter()
            .map(|route| {
                route.route_id.capacity()
                    + layout::<i32>(route.accepted_effects.capacity())
                    + layout::<wire::WriteInputMapping>(route.input_mapping.capacity())
                    + match route
                        .requirements
                        .as_ref()
                        .unwrap()
                        .colocation
                        .as_ref()
                        .unwrap()
                    {
                        wire::write_route_requirements::Colocation::ByKey(keys) => {
                            layout::<u32>(keys.value_ids.capacity())
                        }
                        _ => 0,
                    }
                    + match route
                        .requirements
                        .as_ref()
                        .unwrap()
                        .layout
                        .as_ref()
                        .unwrap()
                    {
                        wire::write_route_requirements::Layout::ClusterBy(keys) => {
                            layout::<u32>(keys.value_ids.capacity())
                        }
                        _ => 0,
                    }
                    + route
                        .input_mapping
                        .iter()
                        .map(|mapping| mapping.field_token.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>();
    // The token length/content still denotes a legal fixed token. Its larger
    // original allocation capacity is retained source, not output payload.
    assert!(decode(&capacity_source, limits(), &Control::default()).is_ok());
    assert!(matches!(
        prepare_fragment_envelope_decode(
            &capacity_source,
            known - 1,
            limits(),
            &Control::default()
        ),
        Err(Error::InvalidShape(_))
    ));
}
#[test]
fn envelope_original_control_every_actual_success_and_ordinary_tail_prefix() {
    let source = fragment(router(), false);
    let (raw, _) = encoded(&source);
    prefixes(
        |c| prepare_fragment_envelope_encode(&source, SOURCE, limits(), c)?.emit(),
        true,
        false,
    );
    prefixes(|c| decode(&raw, limits(), c), true, false);
    let mut bad = raw.clone();
    raw_router(&mut bad).routes[1].input_mapping[1].value_id = None;
    prefixes(|c| decode(&bad, limits(), c), false, false);
    prefixes(
        |c| prepare_fragment_envelope_encode(&source, 0, limits(), c).map(|_| ()),
        false,
        false,
    );
}
#[test]
fn envelope_known_numeric_resource_has_no_later_cancel_or_deadline_callback() {
    let source = fragment(router(), false);
    let (raw, _) = encoded(&source);
    for cause in CAUSES {
        let control = Control {
            events: Mutex::new(vec![]),
            stop: Some((1, cause.clone())),
        };
        let mut l = limits();
        l.max_list_items = 0;
        assert!(matches!(
            prepare_fragment_envelope_encode(&source, SOURCE, l, &control),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Encode, 0)]);
        let control = Control {
            events: Mutex::new(vec![]),
            stop: Some((1, cause)),
        };
        assert!(matches!(
            decode(&raw, l, &control),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Decode, 0)]);
    }
}
#[test]
fn envelope_real_wide_320_reference_emission_samples_256_and_original_first_cause() {
    let source = fragment(p::FragmentSink::Noop, true);
    let (raw, _) = encoded(&source);
    assert_eq!(raw.runtime_filter_ids.len(), 320);
    assert_eq!(raw.runtime_filter_ids[319], u32::MAX);
    prefixes(
        |c| prepare_fragment_envelope_encode(&source, SOURCE, limits(), c)?.emit(),
        true,
        true,
    );
    prefixes(|c| decode(&raw, limits(), c), true, true);
}

fn caller_encode(
    source: &p::Fragment,
    l: FragmentEnvelopeProjectionLimits,
    control: &Control,
) -> Result<(EncodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = prepare_fragment_envelope_encode_in(source, SOURCE, l, &mut |_| Ok(()), &mut work)
        .and_then(|prepared| prepared.emit_in(&mut |_| Ok(()), &mut work));
    finish(work, result)
}
fn caller_decode(
    source: &wire::Fragment,
    l: FragmentEnvelopeProjectionLimits,
    control: &Control,
) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = prepare_fragment_envelope_decode_in(source, SOURCE, l, &mut |_| Ok(()), &mut work)
        .and_then(|prepared| prepared.emit_in(&mut |_| Ok(()), &mut work));
    finish(work, result)
}
#[test]
fn caller_envelope_all_sinks_use_original_mapper_and_independent_layouts() {
    for sink in [
        p::FragmentSink::Result,
        p::FragmentSink::Noop,
        p::FragmentSink::Stream {
            edge: p::EdgeId::new(0),
        },
        p::FragmentSink::Multicast {
            edges: Box::from([p::EdgeId::new(u32::MAX), p::EdgeId::new(0)]),
        },
        router(),
    ] {
        let source = fragment(sink.clone(), false);
        let (plain, plain_facts) = encoded(&source);
        let control = Control::default();
        let (sent, sent_facts) = caller_encode(&source, limits(), &control).unwrap();
        assert_eq!(raw(sent), plain);
        assert_eq!(sent_facts, plain_facts);
        assert!(
            control
                .trace()
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
        );
        let received = caller_decode(&plain, limits(), &Control::default()).unwrap();
        assert_eq!(received.0.sink, sink);
        assert_eq!(received.0.id, p::FragmentId::new(u32::MAX));
        assert_eq!(received.0.dop_domain, dop());
        assert_eq!(
            &*received.0.runtime_filters,
            &[
                p::RuntimeFilterId::new(0),
                p::RuntimeFilterId::new(u32::MAX)
            ]
        );
        if matches!(source.sink(), p::FragmentSink::Router { .. }) {
            let sent_bytes = layout::<u32>(2)
                + layout::<wire::ChangeStreamRoute>(2)
                + 2 * 32
                + layout::<i32>(3)
                + 2 * layout::<wire::WriteInputMapping>(2)
                + 2 * layout::<u32>(1)
                + 4 * 32
                + layout::<wire::SummaryOutput>(1)
                + layout::<u32>(3);
            let received_bytes = 2
                * (layout::<p::RuntimeFilterId>(2)
                    + layout::<p::ChangeStreamRoute>(2)
                    + layout::<ConnectorRowMutationEffect>(3)
                    + 2 * layout::<(ConnectorWriteFieldToken, p::ValueId)>(2)
                    + 2 * layout::<p::ValueId>(1)
                    + layout::<p::SummaryOutput>(1));
            assert_eq!(sent_facts.allocation_requests_upper_bound, 16);
            assert_eq!(sent_facts.allocation_request_bytes_upper_bound, sent_bytes);
            assert_eq!(received.1.allocation_requests_upper_bound, 18);
            assert_eq!(
                received.1.allocation_request_bytes_upper_bound,
                received_bytes
            );
            assert_eq!(received.1.value_reference_count, 11);
            assert_eq!(received.1.list_item_count, 17);
        }
    }
}
#[test]
fn caller_envelope_every_actual_success_and_ordinary_footer_prefix_keeps_first_cause() {
    let source = fragment(router(), false);
    let input = encoded(&source).0;
    prefixes(|c| caller_encode(&source, limits(), c), true, false);
    prefixes(|c| caller_decode(&input, limits(), c), true, false);
    let mut malformed = input;
    raw_router(&mut malformed).routes[0].input_mapping[0]
        .field_token
        .pop();
    prefixes(|c| caller_decode(&malformed, limits(), c), false, false);
    assert!(matches!(
        caller_decode(&malformed, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
}
#[test]
fn caller_envelope_known_root_requests_and_source_overflow_precede_pending_control() {
    let source = fragment(router(), false);
    let input = encoded(&source).0;
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            for receiving in [false, true] {
                for overflow in [false, true] {
                    let control = Control {
                        events: Mutex::new(Vec::new()),
                        stop: Some((1, cause.clone())),
                    };
                    let mut work =
                        CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
                    for _ in 0..pending {
                        work.step().unwrap();
                    }
                    let mut l = limits();
                    let source_bytes = if overflow { usize::MAX } else { SOURCE };
                    if overflow {
                        l.max_coexisting_source_and_request_bytes = usize::MAX;
                    } else {
                        l.max_allocation_requests = 0;
                    }
                    let result = if receiving {
                        prepare_fragment_envelope_decode_in(
                            &input,
                            source_bytes,
                            l,
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        prepare_fragment_envelope_encode_in(
                            &source,
                            source_bytes,
                            l,
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    };
                    assert!(matches!(
                        result,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(control.trace(), [(CompilePhase::LowerProgram, 0)]);
                }
            }
        }
    }
}
#[test]
fn caller_envelope_captured_route_requests_are_admitted_before_the_next_callback() {
    let source = fragment(router(), false);
    let input = encoded(&source).0;
    for receiving in [false, true] {
        // Root arrays include the summary declaration; encoding additionally
        // allocates its ordered ValueId vector before any route allocation.
        let root_requests = if receiving { 6 } else { 4 };
        let expected_requests = if receiving { 12 } else { 10 };
        let baseline = Control::default();
        let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::LowerProgram).unwrap();
        let mut capture = None;
        let mut admit = |facts: &FragmentEnvelopeProjectionFacts| {
            if capture.is_none() && facts.allocation_requests_upper_bound > root_requests {
                capture = Some((*facts, baseline.trace().len()));
            }
            Ok(())
        };
        let result = if receiving {
            prepare_fragment_envelope_decode_in(&input, SOURCE, limits(), &mut admit, &mut work)
                .and_then(|token| token.emit_in(&mut admit, &mut work))
                .map(|_| ())
        } else {
            prepare_fragment_envelope_encode_in(&source, SOURCE, limits(), &mut admit, &mut work)
                .and_then(|token| token.emit_in(&mut admit, &mut work))
                .map(|_| ())
        };
        finish(work, result).unwrap();
        let (expected, at) = capture.unwrap();
        assert_eq!(expected.allocation_requests_upper_bound, expected_requests);
        // The independent summary adds three values and its own edge ID.
        assert_eq!(expected.value_reference_count, 4 + 4);
        assert_eq!(expected.list_item_count, 9 + 4);
        let events = baseline.trace();
        for cause in CAUSES {
            let control = Control {
                events: Mutex::new(Vec::new()),
                stop: Some((at, cause)),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
            let mut refused = None;
            let mut admit = |facts: &FragmentEnvelopeProjectionFacts| {
                if facts.allocation_requests_upper_bound > root_requests {
                    refused = Some(*facts);
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            let result = if receiving {
                prepare_fragment_envelope_decode_in(&input, SOURCE, limits(), &mut admit, &mut work)
                    .map(|_| ())
            } else {
                prepare_fragment_envelope_encode_in(
                    &source,
                    SOURCE,
                    limits(),
                    &mut admit,
                    &mut work,
                )
                .map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(refused, Some(expected));
            assert_eq!(control.trace(), events[..at]);
        }
    }
}
#[test]
fn caller_envelope_exact_axes_and_each_positive_one_under_preserve_numeric_policy() {
    let source = fragment(router(), false);
    let input = encoded(&source).0;
    for receiving in [false, true] {
        let f = if receiving {
            caller_decode(&input, limits(), &Control::default())
                .unwrap()
                .1
        } else {
            caller_encode(&source, limits(), &Control::default())
                .unwrap()
                .1
        };
        let mut exact = limits();
        exact.max_input_nodes = 0; // No input-node namespace is projected by an envelope.
        exact.max_value_references = f.value_reference_count;
        exact.max_list_items = f.list_item_count;
        exact.max_allocation_requests = f.allocation_requests_upper_bound;
        exact.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
        exact.max_coexisting_source_and_request_bytes =
            f.coexisting_source_and_request_bytes_upper_bound;
        exact.max_work = f.cumulative_work_upper_bound;
        for under in 0..=6 {
            let mut l = exact;
            match under {
                0 => {}
                1 => l.max_value_references -= 1,
                2 => l.max_list_items -= 1,
                3 => l.max_allocation_requests -= 1,
                4 => l.max_allocation_request_bytes -= 1,
                5 => l.max_coexisting_source_and_request_bytes -= 1,
                6 => l.max_work -= 1,
                _ => unreachable!(),
            }
            let result = if receiving {
                caller_decode(&input, l, &Control::default()).map(|x| x.1)
            } else {
                caller_encode(&source, l, &Control::default()).map(|x| x.1)
            };
            if under == 0 {
                assert_eq!(result.unwrap(), f);
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        }
    }
}
#[test]
fn caller_envelope_original_token_control_and_wide_source_quantum_are_exact() {
    let source = fragment(router(), true);
    let input = encoded(&source).0;
    prefixes(|c| caller_encode(&source, limits(), c), true, true);
    prefixes(|c| caller_decode(&input, limits(), c), true, true);
    for receiving in [false, true] {
        let original = Control::default();
        let foreign = Control::default();
        let mut owner_work =
            CompileCheckpoints::try_new(&original, CompilePhase::LowerProgram).unwrap();
        let mut foreign_work =
            CompileCheckpoints::try_new(&foreign, CompilePhase::LowerProgram).unwrap();
        let before = foreign.trace();
        let mut parent = |_: &FragmentEnvelopeProjectionFacts| -> Result<(), CompileControlError> {
            panic!("foreign token must not replace the parent's contribution");
        };
        let result = if receiving {
            prepare_fragment_envelope_decode_in(
                &input,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut owner_work,
            )
            .unwrap()
            .emit_in(&mut parent, &mut foreign_work)
            .map(|_| ())
        } else {
            prepare_fragment_envelope_encode_in(
                &source,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut owner_work,
            )
            .unwrap()
            .emit_in(&mut parent, &mut foreign_work)
            .map(|_| ())
        };
        assert!(matches!(result, Err(Error::InvalidShape(_))));
        assert_eq!(foreign.trace(), before);
    }
}

#[test]
fn package_envelope_preserves_the_unbound_scalar_root_declaration() {
    use novarocks_result_contract::{
        FrozenRootOutput, RootOutputContract, RootProfileId, ScalarField, ScalarSchema,
        ScalarValueType,
    };
    let expected = RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::ScalarValue(
            ScalarSchema::try_new(ScalarField {
                nullable: false,
                value_type: ScalarValueType::SignedInteger(64),
            })
            .unwrap(),
        ),
    );
    let mut raw = encoded(&fragment(p::FragmentSink::Noop, false)).0;
    raw.sink = Some(wire::FragmentSink {
        kind: Some(wire::fragment_sink::Kind::RootResult(
            novarocks_proto_codec::root_result::encode_root_contract(&expected),
        )),
    });
    raw.nodes = vec![wire::PhysicalNode {
        id: u32::MAX,
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![0],
        }),
        ..Default::default()
    }];
    // The envelope owns only header vocabulary; this raw fixture does not
    // independently publish or certify a complete physical package.
    let mut bound = limits();
    bound.max_input_nodes = 1;
    bound.max_allocation_request_bytes = root_projection_request_bytes().unwrap() * 2 + 4096;
    bound.max_coexisting_source_and_request_bytes = SOURCE + bound.max_allocation_request_bytes;
    let (decoded, _) = decode(&raw, bound, &Control::default()).unwrap();
    assert_eq!(
        decoded.sink,
        p::FragmentSink::RootResult(Box::new(expected))
    );
}

#[test]
fn route_colocation_and_layout_are_independent_closed_original_facts() {
    for colocation in [
        p::WriteColocation::None,
        p::WriteColocation::Singleton,
        p::WriteColocation::ByKey(Box::from([p::ValueId::new(1)])),
    ] {
        for layout in [
            p::WriteLayoutPreference::None,
            p::WriteLayoutPreference::ClusterBy(Box::from([p::ValueId::new(1)])),
        ] {
            let mut sink = router();
            let p::FragmentSink::Router { routes, .. } = &mut sink else {
                panic!("router")
            };
            routes[0].requirements = p::WriteRouteRequirements {
                colocation: colocation.clone(),
                layout,
            };
            let source = fragment(sink.clone(), false);
            let (raw, _) = encoded(&source);
            assert_eq!(
                decode(&raw, limits(), &Control::default()).unwrap().0.sink,
                sink
            );
        }
    }
    let (base, sent) = encoded(&fragment(router(), false));
    let mut sink = router();
    let p::FragmentSink::Router { routes, .. } = &mut sink else {
        panic!("router")
    };
    routes[0].requirements.layout =
        p::WriteLayoutPreference::ClusterBy(Box::from([p::ValueId::new(1)]));
    let (with_layout, layout_sent) = encoded(&fragment(sink, false));
    assert_eq!(
        layout_sent.value_reference_count,
        sent.value_reference_count + 1
    );
    assert_eq!(layout_sent.list_item_count, sent.list_item_count + 1);
    assert_eq!(
        layout_sent.allocation_requests_upper_bound,
        sent.allocation_requests_upper_bound + 1
    );
    assert_eq!(
        layout_sent.allocation_request_bytes_upper_bound,
        sent.allocation_request_bytes_upper_bound + layout::<u32>(1)
    );
    let base_recv = decode(&base, limits(), &Control::default()).unwrap().1;
    let recv = decode(&with_layout, limits(), &Control::default())
        .unwrap()
        .1;
    assert_eq!(
        recv.value_reference_count,
        base_recv.value_reference_count + 1
    );
    assert_eq!(recv.list_item_count, base_recv.list_item_count + 1);
    assert_eq!(
        recv.allocation_requests_upper_bound,
        base_recv.allocation_requests_upper_bound + 2
    );
    assert_eq!(
        recv.allocation_request_bytes_upper_bound,
        base_recv.allocation_request_bytes_upper_bound + 2 * layout::<p::ValueId>(1)
    );
    for fault in 0..5 {
        let mut raw = base.clone();
        let route = &mut raw_router(&mut raw).routes[0];
        match fault {
            0 => route.requirements = None,
            1 => route.requirements.as_mut().unwrap().colocation = None,
            2 => route.requirements.as_mut().unwrap().layout = None,
            3 => {
                route.requirements.as_mut().unwrap().colocation =
                    Some(wire::write_route_requirements::Colocation::ByKey(
                        wire::WriteRouteKeyFields { value_ids: vec![] },
                    ))
            }
            _ => {
                route.requirements.as_mut().unwrap().layout =
                    Some(wire::write_route_requirements::Layout::ClusterBy(
                        wire::WriteRouteKeyFields { value_ids: vec![] },
                    ))
            }
        }
        assert!(
            decode(&raw, limits(), &Control::default()).is_err(),
            "fault {fault}"
        );
    }
}
