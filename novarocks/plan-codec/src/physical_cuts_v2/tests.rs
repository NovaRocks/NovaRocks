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
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{CompilePhase, OrderedComparisonAlgorithm, PureCompileControl};
use std::{alloc::Layout, sync::Mutex};
const B: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, u: u32) -> Result<(), CompileControlError> {
        assert!(u <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        let stop = self.stop.lock().unwrap().clone();
        if let Some((s, _)) = stop {
            assert!(at <= s, "callback after refusal");
        }
        t.push(u);
        match stop {
            Some((s, c)) if s == at => Err(c),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn reset(&self, s: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = s;
    }
    fn events(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
fn tl() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 10000,
        max_expanded_nodes: 10000,
        max_string_bytes: B,
    }
}
fn limits() -> CutsProjectionLimits {
    CutsProjectionLimits {
        node: NodeProjectionLimits {
            max_input_nodes: 10000,
            max_value_references: 10000,
            max_list_items: 10000,
            max_allocation_requests: 10000,
            max_allocation_request_bytes: 16 * B,
            max_coexisting_source_and_request_bytes: 32 * B,
            max_work: usize::MAX / 8,
            properties: props::PhysicalPropertyProjectionLimits {
                max_value_references: 10000,
                max_allocation_requests: 10000,
                max_allocation_request_bytes: 16 * B,
                max_coexisting_source_and_request_bytes: 32 * B,
                max_work: usize::MAX / 8,
            },
        },
        binding: BindingProjectionLimits {
            max_definitions: 10000,
            max_type_references: 10000,
            max_allocation_requests: 10000,
            max_request_bytes: 16 * B,
            max_coexisting_source_and_request_bytes: 32 * B,
            max_work: usize::MAX / 8,
        },
    }
}
fn ty() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn partition() -> p::EdgePartitioning {
    p::EdgePartitioning {
        source: p::Distribution::Singleton,
        source_multiplicity: p::RowMultiplicity::SingleCopy,
        destination: p::Distribution::Unconstrained,
        destination_multiplicity: p::RowMultiplicity::Replicated,
    }
}
// Representation-only sparse declarations. Provider-route and writer-result
// payloads occupy separate cuts. The writer cut has no import type occurrence,
// so the independent source order remains route import -> writer field -> the
// two outbound type occurrences. This fixture does not certify a complete graph.
fn cut() -> p::FragmentCuts {
    p::FragmentCuts {
        inbound: Box::from([
            p::InboundFragmentCut {
                edge: p::EdgeId::new(0),
                kind: p::EdgeKind::Stream,
                source_fragment: p::FragmentId::new(u32::MAX),
                destination_node: p::NodeId::new(0),
                imports: Box::from([p::CutImport {
                    source: p::CutValue {
                        value: p::ValueId::new(u32::MAX),
                        ty: ty(),
                    },
                    destination: p::ValueId::new(0),
                }]),
                partitioning: partition(),
                source_port: p::FragmentOutputPort::Root,
                delivery: p::DeliveryRequirement::MustDrain,
                payload: p::ExchangePayloadContract::ProviderRoute(p::ChangeStreamWriterCut {
                    route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
                    write_target_ordinal: WriteTargetOrdinal::try_new(4095).unwrap(),
                    fields: Box::from([p::ChangeStreamWriterCutField {
                        token: ConnectorWriteFieldToken::from_bytes([9; 32]),
                        source: p::ValueId::new(0),
                        destination: p::ValueId::new(u32::MAX),
                    }]),
                }),
            },
            p::InboundFragmentCut {
                edge: p::EdgeId::new(1),
                kind: p::EdgeKind::Stream,
                source_fragment: p::FragmentId::new(2),
                destination_node: p::NodeId::new(1),
                imports: Box::default(),
                partitioning: partition(),
                source_port: p::FragmentOutputPort::Root,
                delivery: p::DeliveryRequirement::MustDrain,
                payload: p::ExchangePayloadContract::WriterResults(p::WriterResultCut {
                    write_target_ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
                    schema_revision: u32::MAX,
                    fields: Box::from([p::WriterResultCutField {
                        source: p::ValueId::new(u32::MAX),
                        destination: p::ValueId::new(0),
                        name: "雪\0".into(),
                        ty: ty(),
                        role: p::WriterRelationFieldRole::Auxiliary,
                    }]),
                }),
            },
        ]),
        outbound: Box::from([p::OutboundFragmentCut {
            edge: p::EdgeId::new(u32::MAX),
            kind: p::EdgeKind::CteMulticast,
            destination_fragment: p::FragmentId::new(0),
            destination_node: p::NodeId::new(u32::MAX),
            projection: Box::from([p::CutValue {
                value: p::ValueId::new(0),
                ty: ty(),
            }]),
            destination_imports: Box::from([p::CutImport {
                source: p::CutValue {
                    value: p::ValueId::new(0),
                    ty: ty(),
                },
                destination: p::ValueId::new(u32::MAX),
            }]),
            partitioning: partition(),
            source_port: p::FragmentOutputPort::Root,
            delivery: p::DeliveryRequirement::DemandDriven,
            payload: p::ExchangePayloadContract::Relational,
        }]),
        runtime_filters: Box::default(),
        runtime_filter_bindings: Box::default(),
    }
}
fn raw_route(cut: &wire::InboundFragmentCut) -> &wire::ChangeStreamWriterCut {
    match cut.payload.as_ref().expect("explicit cut payload") {
        wire::inbound_fragment_cut::Payload::ChangeStreamWriter(route) => route,
        _ => panic!("expected provider route payload"),
    }
}
fn raw_route_mut(cut: &mut wire::InboundFragmentCut) -> &mut wire::ChangeStreamWriterCut {
    match cut.payload.as_mut().expect("explicit cut payload") {
        wire::inbound_fragment_cut::Payload::ChangeStreamWriter(route) => route,
        _ => panic!("expected provider route payload"),
    }
}
fn raw_writer(cut: &wire::InboundFragmentCut) -> &wire::WriterResultCut {
    match cut.payload.as_ref().expect("explicit cut payload") {
        wire::inbound_fragment_cut::Payload::WriterResult(writer) => writer,
        _ => panic!("expected writer result payload"),
    }
}
fn raw_writer_mut(cut: &mut wire::InboundFragmentCut) -> &mut wire::WriterResultCut {
    match cut.payload.as_mut().expect("explicit cut payload") {
        wire::inbound_fragment_cut::Payload::WriterResult(writer) => writer,
        _ => panic!("expected writer result payload"),
    }
}
fn run<T>(
    control: &Control,
    phase: CompilePhase,
    f: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, E>,
) -> Result<T, E> {
    let mut w = CompileCheckpoints::try_new(control, phase)?;
    let r = f(&mut w);
    if matches!(&r, Err(E::Control(_))) {
        return r;
    }
    w.finish()?;
    r
}
fn encode(
    p: &p::FragmentCuts,
    c: &Control,
    l: CutsProjectionLimits,
) -> Result<(wire::FragmentCuts, NodeProjectionFacts), E> {
    let roots = [(0, ty())];
    c.reset(None);
    let t = encode_type_table_sources(&roots, &[], tl(), c).unwrap();
    c.reset(None);
    let mut n = 0;
    types_physical(p, |_| {
        n += 1;
        Ok(())
    })
    .unwrap();
    let ids = vec![0; n];
    run(c, CompilePhase::Encode, |w| {
        encode_fragment_cuts_observed(
            p,
            EncodedCutsContext {
                types: &t,
                type_ids: &CutsTypeIds::new(p, &ids),
                source_retained_bytes: B,
                limits: l,
            },
            &mut |_| Ok(()),
            w,
        )
    })
}
fn decode(
    p: &wire::FragmentCuts,
    c: &Control,
    l: CutsProjectionLimits,
) -> Result<(p::FragmentCuts, NodeProjectionFacts), E> {
    let roots = [(0, ty())];
    c.reset(None);
    let t = encode_type_table_sources(&roots, &[], tl(), c).unwrap();
    let d = decode_type_table(t.as_wire(), tl(), c).unwrap();
    c.reset(None);
    run(c, CompilePhase::Decode, |w| {
        decode_fragment_cuts_observed(p, &d, B, l, &mut |_| Ok(()), w)
    })
}
#[test]
fn full_cut_fields_have_independent_sparse_wire_oracle() {
    let c = Control::default();
    let src = cut();
    let (raw, _) = encode(&src, &c, limits()).unwrap();
    let x = &raw.inbound[0];
    assert_eq!(
        (x.edge_id, x.source_fragment_id, x.destination_node_id),
        (Some(0), Some(u32::MAX), Some(0))
    );
    assert_eq!(
        x.imports[0],
        wire::CutImport {
            source: Some(wire::CutValue {
                value_id: Some(u32::MAX),
                value_type_id: Some(0)
            }),
            destination_value_id: Some(0)
        }
    );
    assert_eq!(raw_route(x).route_id, [7; 32]);
    assert_eq!(raw_route(x).fields[0].field_token, [9; 32]);
    assert_eq!(raw_route(x).write_target_ordinal, 4095);
    assert_eq!(raw_writer(&raw.inbound[1]).schema_revision, u32::MAX);
    assert_eq!(raw_writer(&raw.inbound[1]).fields[0].name, "雪\0");
    assert_eq!(
        raw_writer(&raw.inbound[1]).fields[0].role,
        wire::WriterRelationFieldRole::Auxiliary as i32
    );
    assert_eq!(raw.outbound[0].kind, wire::EdgeKind::CteMulticast as i32);
    let (owned, _) = decode(&raw, &c, limits()).unwrap();
    assert_eq!(owned, src);
}
fn coverage() -> p::RuntimeFilterCoverage {
    p::RuntimeFilterCoverage {
        nodes: Box::from([
            p::RuntimeFilterCoverageNode::Witness(p::RuntimeFilterWitnessId::new(0)),
            p::RuntimeFilterCoverageNode::Witness(p::RuntimeFilterWitnessId::new(u32::MAX)),
            p::RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0, 1]),
            },
            p::RuntimeFilterCoverageNode::AnyOf {
                children: Box::from([0, 2]),
            },
        ]),
        root: 3,
    }
}
fn endpoint() -> p::RuntimeFilterEndpoint {
    p::RuntimeFilterEndpoint {
        fragment: p::FragmentId::new(u32::MAX),
        node: p::NodeId::new(0),
        values: Box::from([
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
        ]),
    }
}
fn lineage() -> Box<[p::RuntimeFilterLineageStep]> {
    let f = p::FragmentId::new(0);
    let n = p::NodeId::new(u32::MAX);
    Box::from([
        p::RuntimeFilterLineageStep::FilterPassThrough {
            fragment: f,
            node: n,
            input_ordinal: 0,
        },
        p::RuntimeFilterLineageStep::SortPassThrough {
            fragment: f,
            node: n,
            input_ordinal: 1,
        },
        p::RuntimeFilterLineageStep::ProjectIdentity {
            fragment: f,
            node: n,
            output_ordinal: 2,
        },
        p::RuntimeFilterLineageStep::JoinEquality {
            fragment: f,
            node: n,
            key_ordinal: 3,
            source_side: p::JoinSide::Left,
            target_side: p::JoinSide::Right,
        },
        p::RuntimeFilterLineageStep::JoinOutputPassThrough {
            fragment: f,
            node: n,
            input_ordinal: 4,
        },
        p::RuntimeFilterLineageStep::AggregateGroupKey {
            fragment: f,
            node: n,
            group_key_ordinal: 5,
        },
        p::RuntimeFilterLineageStep::UnionAllBranch {
            fragment: f,
            node: n,
            input_ordinal: 6,
            output_ordinal: 7,
        },
        p::RuntimeFilterLineageStep::ExchangeMapping {
            edge: p::EdgeId::new(u32::MAX),
            mapping_ordinal: 8,
        },
    ])
}
fn filter() -> p::RuntimeFilter {
    p::RuntimeFilter {
        id: p::RuntimeFilterId::new(u32::MAX),
        kind: p::RuntimeFilterKind::MinMax,
        domain: p::RuntimeFilterDomain::Ordered {
            key: p::RuntimeFilterOrderKey {
                ty: ty(),
                direction: p::SortDirection::Descending,
                null_ordering: p::NullOrdering::Last,
            },
            inclusive: false,
            comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: p::RuntimeFilterLifecycle::MonotonicUpdates,
        reduction: p::RuntimeFilterReduction::TightenOrderedBound,
        availability_coverage: coverage(),
        terminal_coverage: coverage(),
        equality_witnesses: Box::from([p::RuntimeFilterEqualityWitness {
            id: p::RuntimeFilterEqualityWitnessId::new(0),
            fragment: p::FragmentId::new(u32::MAX),
            join: p::NodeId::new(0),
            key_ordinal: 7,
            domain_side: p::JoinSide::Right,
        }]),
        producers: Box::from([p::RuntimeFilterProducer {
            witness: p::RuntimeFilterWitnessId::new(0),
            endpoint: endpoint(),
            apply_point: p::RuntimeFilterApplyPoint::NodeInput { input_ordinal: 5 },
            contribution_kinds: Box::from([
                p::RuntimeFilterContributionKind::ValueDomainDelta,
                p::RuntimeFilterContributionKind::FinalDomainShard,
                p::RuntimeFilterContributionKind::OrderedBoundUpdate,
                p::RuntimeFilterContributionKind::FinalOrderedHullShard,
                p::RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: p::RuntimeFilterCompletion::FencedCommittedDomain,
            progress: p::RuntimeFilterProducerProgress {
                build_edges: Box::from([p::EdgeId::new(u32::MAX), p::EdgeId::new(0)]),
                non_build_edges: Box::from([p::EdgeId::new(0), p::EdgeId::new(0)]),
            },
            target: p::RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 4,
                topn: p::NodeId::new(u32::MAX),
                phase: p::TopNPhase::Final {
                    sequence: p::TopNSequenceId::new(0),
                },
                order_key_ordinal: 9,
                limit: u64::MAX,
                offset: u64::MAX - 1,
                direction: p::SortDirection::Ascending,
                null_ordering: p::NullOrdering::First,
            },
        }]),
        consumers: Box::from([p::RuntimeFilterConsumer {
            endpoint: endpoint(),
            apply_point: p::RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([
                p::RuntimeFilterArtifactCapability::Membership,
                p::RuntimeFilterArtifactCapability::OrderedRange,
                p::RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: p::RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: p::LateApplyGranularity::File,
            },
            target: p::RuntimeFilterConsumerTarget::AggregateTopNScanField {
                producer: p::RuntimeFilterWitnessId::new(u32::MAX),
                lineage: lineage(),
            },
        }]),
        policy: p::RuntimeFilterPolicy {
            max_contribution_bytes: 0,
            max_artifact_bytes: u64::MAX,
            deadline_ms: 17,
            max_retries: u32::MAX,
        },
    }
}
#[test]
fn runtime_filter_full_flat_coverage_lineage_and_progress_have_hand_oracles() {
    let c = Control::default();
    let src = p::FragmentCuts {
        runtime_filters: Box::from([filter()]),
        ..Default::default()
    };
    let (raw, _) = encode(&src, &c, limits()).unwrap();
    let f = &raw.runtime_filters[0];
    assert_eq!(
        f.policy.as_ref().unwrap(),
        &wire::RuntimeFilterPolicy {
            max_contribution_bytes: 0,
            max_artifact_bytes: u64::MAX,
            deadline_ms: 17,
            max_retries: u32::MAX
        }
    );
    assert_eq!(
        f.availability_coverage.as_ref().unwrap().root_index,
        Some(3)
    );
    assert_eq!(
        f.producers[0].progress.as_ref().unwrap().non_build_edge_ids,
        [0, 0]
    );
    assert_eq!(
        f.consumers[0].endpoint.as_ref().unwrap().value_ids,
        [0, u32::MAX, 0]
    );
    let target = f.consumers[0].target.as_ref().unwrap();
    let Some(wire::runtime_filter_consumer_target::Kind::AggregateTopnScanField(t)) = &target.kind
    else {
        panic!("wrong target")
    };
    assert_eq!(t.lineage.len(), 8);
    assert!(matches!(
        t.lineage[3].kind,
        Some(wire::runtime_filter_lineage_step::Kind::JoinEquality(_))
    ));
    let Some(wire::runtime_filter_producer_target::Kind::AggregateTopnKey(t)) =
        &f.producers[0].target.as_ref().unwrap().kind
    else {
        panic!("wrong producer")
    };
    assert_eq!((t.limit, t.offset), (u64::MAX, u64::MAX - 1));
    assert_eq!(decode(&raw, &c, limits()).unwrap().0, src);
}
#[test]
fn remaining_closed_options_preserve_authored_phase_and_activation() {
    let c = Control::default();
    let mut f = filter();
    f.kind = p::RuntimeFilterKind::Bloom;
    f.domain = p::RuntimeFilterDomain::Membership {
        ty: ty(),
        null_semantics: p::RuntimeFilterNullSemantics::NullSafeEqual,
    };
    f.lifecycle = p::RuntimeFilterLifecycle::CompleteOnce;
    f.reduction = p::RuntimeFilterReduction::SetUnion;
    f.producers[0].target = p::RuntimeFilterProducerTarget::JoinBuildKey {
        equality: p::RuntimeFilterEqualityWitnessId::new(u32::MAX),
    };
    f.producers[0].completion = p::RuntimeFilterCompletion::ProducerClosed;
    f.producers[0].apply_point = p::RuntimeFilterApplyPoint::NodeOutput;
    let base = f.consumers[0].clone();
    f.consumers = Box::from([
        p::RuntimeFilterConsumer {
            activation: p::RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: p::RuntimeFilterConsumerTarget::JoinProbeKey {
                equality: p::RuntimeFilterEqualityWitnessId::new(0),
            },
            ..base.clone()
        },
        p::RuntimeFilterConsumer {
            activation: p::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                late_apply: p::LateApplyGranularity::Row,
            },
            target: p::RuntimeFilterConsumerTarget::ScanField {
                equality: p::RuntimeFilterEqualityWitnessId::new(0),
                lineage: lineage(),
            },
            ..base.clone()
        },
        p::RuntimeFilterConsumer {
            activation: p::RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: p::LateApplyGranularity::Batch,
            },
            ..base.clone()
        },
        p::RuntimeFilterConsumer {
            activation: p::RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: p::LateApplyGranularity::RowGroup,
            },
            ..base.clone()
        },
        p::RuntimeFilterConsumer {
            activation: p::RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: p::LateApplyGranularity::Split,
            },
            ..base
        },
    ]);
    let source = p::FragmentCuts {
        runtime_filters: Box::from([f]),
        ..Default::default()
    };
    let (raw, _) = encode(&source, &c, limits()).unwrap();
    let f = &raw.runtime_filters[0];
    assert_eq!(f.kind, wire::RuntimeFilterKind::Bloom as i32);
    assert_eq!(
        f.lifecycle,
        wire::RuntimeFilterLifecycle::CompleteOnce as i32
    );
    assert_eq!(f.reduction, wire::RuntimeFilterReduction::SetUnion as i32);
    assert!(matches!(
        f.consumers[0].activation.as_ref().unwrap().kind,
        Some(wire::runtime_filter_consumer_activation::Kind::BlockingSnapshot(_))
    ));
    assert!(matches!(
        f.consumers[1].target.as_ref().unwrap().kind,
        Some(wire::runtime_filter_consumer_target::Kind::ScanField(_))
    ));
    assert_eq!(decode(&raw, &c, limits()).unwrap().0, source);
    for phase in [
        p::TopNPhase::Single,
        p::TopNPhase::Partial {
            sequence: p::TopNSequenceId::new(u32::MAX),
        },
        p::TopNPhase::Final {
            sequence: p::TopNSequenceId::new(0),
        },
    ] {
        let x = p::RuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal: 0,
            topn: p::NodeId::new(0),
            phase,
            order_key_ordinal: 0,
            limit: 0,
            offset: u64::MAX,
            direction: p::SortDirection::Ascending,
            null_ordering: p::NullOrdering::First,
        };
        assert_eq!(
            read_producer_target(&encode_producer_target(&x)).unwrap(),
            x
        );
    }
    let mut source = source;
    source.runtime_filters[0].kind = p::RuntimeFilterKind::InList;
    source.runtime_filters[0].domain = p::RuntimeFilterDomain::Membership {
        ty: ty(),
        null_semantics: p::RuntimeFilterNullSemantics::NeverMatches,
    };
    source.runtime_filters[0].reduction = p::RuntimeFilterReduction::UnionOrderedHull;
    source.inbound = cut().inbound;
    source.inbound[0].kind = p::EdgeKind::ChangeStreamRouter;
    assert_eq!(
        decode(&encode(&source, &c, limits()).unwrap().0, &c, limits())
            .unwrap()
            .0,
        source
    );
}
#[test]
fn missing_closed_headers_and_remote_reference_presence_refuse_without_local_lookup() {
    let c = Control::default();
    let mut src = cut();
    src.runtime_filters = Box::from([filter()]);
    let (raw, _) = encode(&src, &c, limits()).unwrap();
    for case in 0..23 {
        let mut x = raw.clone();
        match case {
            0 => x.inbound[0].edge_id = None,
            1 => x.inbound[0].source_fragment_id = None,
            2 => x.inbound[0].destination_node_id = None,
            3 => x.inbound[0].imports[0].source = None,
            4 => x.inbound[0].imports[0].destination_value_id = None,
            5 => x.inbound[0].partitioning = None,
            6 => x.inbound[0].kind = 0,
            7 => raw_route_mut(&mut x.inbound[0])
                .route_id
                .pop()
                .map(|_| ())
                .unwrap(),
            8 => raw_route_mut(&mut x.inbound[0]).fields[0]
                .field_token
                .push(0),
            9 => raw_route_mut(&mut x.inbound[0]).write_target_ordinal = 4096,
            10 => raw_writer_mut(&mut x.inbound[1]).fields[0].role = i32::MAX,
            11 => x.runtime_filters[0].domain = None,
            12 => {
                x.runtime_filters[0]
                    .availability_coverage
                    .as_mut()
                    .unwrap()
                    .root_index = None
            }
            13 => x.runtime_filters[0].producers[0].progress = None,
            14 => {
                x.runtime_filters[0].consumers[0]
                    .activation
                    .as_mut()
                    .unwrap()
                    .kind = None
            }
            15 => x.runtime_filters[0].policy = None,
            16 => x.runtime_filters[0].consumers[0].capabilities[0] = 0,
            17 => x.inbound[0].payload = None,
            18 => x.inbound[0].source_port = 0,
            19 => x.inbound[0].source_port = i32::MAX,
            20 => x.inbound[0].delivery = 0,
            21 => x.inbound[0].delivery = i32::MAX,
            _ => x.outbound[0].payload = None,
        }
        assert!(
            matches!(
                decode(&x, &c, limits()),
                Err(E::InvalidShape(_)) | Err(E::Properties(_))
            ),
            "case {case}"
        );
    }
    let mut wrong = raw.clone();
    wrong.inbound[0].imports[0]
        .source
        .as_mut()
        .unwrap()
        .value_type_id = Some(77);
    assert!(matches!(
        decode(&wrong, &c, limits()),
        Err(E::InvalidShape("cut value type is unknown"))
    ));
}
#[test]
fn independent_container_layout_and_all_seven_exact_under_envelopes() {
    let c = Control::default();
    let source = p::FragmentCuts {
        inbound: Box::from([p::InboundFragmentCut {
            edge: p::EdgeId::new(0),
            kind: p::EdgeKind::Stream,
            source_fragment: p::FragmentId::new(u32::MAX),
            destination_node: p::NodeId::new(0),
            imports: Box::from([p::CutImport {
                source: p::CutValue {
                    value: p::ValueId::new(0),
                    ty: ty(),
                },
                destination: p::ValueId::new(0),
            }]),
            partitioning: partition(),
            source_port: p::FragmentOutputPort::Root,
            delivery: p::DeliveryRequirement::DemandDriven,
            payload: p::ExchangePayloadContract::Relational,
        }]),
        ..Default::default()
    };
    let (raw, ef) = encode(&source, &c, limits()).unwrap();
    let (_, df) = decode(&raw, &c, limits()).unwrap();
    let eb = Layout::array::<wire::InboundFragmentCut>(1).unwrap().size()
        + Layout::array::<wire::CutImport>(1).unwrap().size();
    let db = 2
        * (Layout::array::<p::InboundFragmentCut>(1).unwrap().size()
            + Layout::array::<p::CutImport>(1).unwrap().size());
    assert_eq!(
        (
            ef.allocation_requests_upper_bound,
            ef.allocation_request_bytes_upper_bound
        ),
        (2, eb)
    );
    assert_eq!(
        (
            df.allocation_requests_upper_bound,
            df.allocation_request_bytes_upper_bound
        ),
        (4, db)
    );
    for receiving in [false, true] {
        let f = if receiving { df } else { ef };
        let mut exact = limits();
        exact.node.max_input_nodes = f.input_node_count;
        exact.node.max_value_references = f.value_reference_count;
        exact.node.max_list_items = f.list_item_count;
        exact.node.max_allocation_requests = f.allocation_requests_upper_bound;
        exact.node.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
        exact.node.max_coexisting_source_and_request_bytes =
            f.coexisting_source_and_request_bytes_upper_bound;
        exact.node.max_work = f.cumulative_work_upper_bound;
        if receiving {
            decode(&raw, &c, exact).unwrap();
        } else {
            encode(&source, &c, exact).unwrap();
        }
        for axis in 0..7 {
            let mut l = exact;
            let v = match axis {
                0 => &mut l.node.max_input_nodes,
                1 => &mut l.node.max_value_references,
                2 => &mut l.node.max_list_items,
                3 => &mut l.node.max_allocation_requests,
                4 => &mut l.node.max_allocation_request_bytes,
                5 => &mut l.node.max_coexisting_source_and_request_bytes,
                _ => &mut l.node.max_work,
            };
            assert!(*v > 0);
            *v -= 1;
            let error = if receiving {
                decode(&raw, &c, l).unwrap_err()
            } else {
                encode(&source, &c, l).unwrap_err()
            };
            assert!(
                matches!(error, E::Control(CompileControlError::ResourceExhausted)),
                "axis {axis}"
            );
        }
    }
}
#[test]
fn normalized_occurrence_copies_share_complete_binding_and_count_only_cut_containers() {
    use std::{collections::HashMap, sync::Arc};
    let c = Control::default();
    let field = Arc::new(
        Field::new("雪", DataType::Int64, false)
            .with_metadata(HashMap::from([("tag".into(), "exact".into())])),
    );
    let full = FunctionValueType::new(
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Struct(vec![Arc::clone(&field)].into())),
        ),
        true,
    );
    let mut source = cut();
    source.outbound = Box::default();
    // This test isolates one relational import owner; the separate writer cut
    // is outside its source and cannot consume another type occurrence.
    source.inbound = Box::from([source.inbound[0].clone()]);
    source.inbound[0].payload = p::ExchangePayloadContract::Relational;
    source.inbound[0].delivery = p::DeliveryRequirement::DemandDriven;
    source.inbound[0].imports[0].source.ty = full;
    let roots = [(u32::MAX, source.inbound[0].imports[0].source.ty.clone())];
    let encoded = encode_type_table_sources(&roots, &[], tl(), &c).unwrap();
    let decoded = decode_type_table(encoded.as_wire(), tl(), &c).unwrap();
    c.reset(None);
    let (raw, _) = run(&c, CompilePhase::Encode, |w| {
        encode_fragment_cuts_observed(
            &source,
            EncodedCutsContext {
                types: &encoded,
                type_ids: &CutsTypeIds::new(&source, &[u32::MAX]),
                source_retained_bytes: B,
                limits: limits(),
            },
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    c.reset(None);
    let (out, facts) = run(&c, CompilePhase::Decode, |w| {
        decode_fragment_cuts_observed(&raw, &decoded, B, limits(), &mut |_| Ok(()), w)
    })
    .unwrap();
    assert_eq!(out, source);
    let expected = 2
        * (Layout::array::<p::InboundFragmentCut>(1).unwrap().size()
            + Layout::array::<p::CutImport>(1).unwrap().size());
    assert_eq!(facts.allocation_requests_upper_bound, 4);
    assert_eq!(facts.allocation_request_bytes_upper_bound, expected);
    let actual = &out.inbound[0].imports[0].source.ty;
    let original = decoded.value_type(u32::MAX).unwrap();
    assert!(std::ptr::eq(actual.data_type(), original.data_type()));
    let DataType::Struct(fields) = actual.data_type() else {
        panic!("normalized Struct")
    };
    assert_eq!(fields[0].name(), "雪");
    assert!(fields[0].metadata().as_arc().is_none());
    assert_eq!(field.metadata()["tag"], "exact");
}

#[test]
fn every_actual_success_and_ordinary_tail_callback_preserves_three_primary_causes() {
    let c = Control::default();
    let source = cut();
    let roots = [(0, ty())];
    let t = encode_type_table_sources(&roots, &[], tl(), &c).unwrap();
    let d = decode_type_table(t.as_wire(), tl(), &c).unwrap();
    let ids = [0; 4];
    c.reset(None);
    let raw = run(&c, CompilePhase::Encode, |w| {
        encode_fragment_cuts_observed(
            &source,
            EncodedCutsContext {
                types: &t,
                type_ids: &CutsTypeIds::new(&source, &ids),
                source_retained_bytes: B,
                limits: limits(),
            },
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap()
    .0;
    for receiving in [false, true] {
        for ordinary in [false, true] {
            let mut raw = raw.clone();
            if ordinary {
                raw.inbound[0].imports[0].source = None;
            }
            let bad_ids = [0; 3];
            let chosen = if ordinary { &bad_ids[..] } else { &ids[..] };
            let call = |w: &mut CompileCheckpoints<'_>| {
                if receiving {
                    decode_fragment_cuts_observed(&raw, &d, B, limits(), &mut |_| Ok(()), w)
                        .map(|_| ())
                } else {
                    encode_fragment_cuts_observed(
                        &source,
                        EncodedCutsContext {
                            types: &t,
                            type_ids: &CutsTypeIds::new(&source, chosen),
                            source_retained_bytes: B,
                            limits: limits(),
                        },
                        &mut |_| Ok(()),
                        w,
                    )
                    .map(|_| ())
                }
            };
            c.reset(None);
            let result = run(
                &c,
                if receiving {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                },
                call,
            );
            assert_eq!(result.is_err(), ordinary);
            let baseline = c.events();
            assert!(!baseline.is_empty());
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    c.reset(Some((at, cause.clone())));
                    assert!(
                        matches!(run(&c,if receiving{CompilePhase::Decode}else{CompilePhase::Encode},call),Err(E::Control(e)) if e==cause)
                    );
                    assert_eq!(c.events(), baseline[..=at]);
                }
            }
        }
    }
}
#[test]
fn source_namespace_loans_and_known_numeric_refusal_precede_late_control() {
    let c = Control::default();
    let source = cut();
    let roots = [(0, ty())];
    let t = encode_type_table_sources(&roots, &[], tl(), &c).unwrap();
    let d = decode_type_table(t.as_wire(), tl(), &c).unwrap();
    let ids = [0; 4];
    let equal = source.clone();
    c.reset(None);
    assert!(matches!(
        run(&c, CompilePhase::Encode, |w| encode_fragment_cuts_observed(
            &source,
            EncodedCutsContext {
                types: &t,
                type_ids: &CutsTypeIds::new(&equal, &ids),
                source_retained_bytes: B,
                limits: limits()
            },
            &mut |_| Ok(()),
            w
        )),
        Err(E::InvalidShape(
            "cut type view belongs to a different source"
        ))
    ));
    let raw = encode(&source, &c, limits()).unwrap().0;
    for receiving in [false, true] {
        for cause in CAUSES {
            c.reset(Some((1, cause)));
            let mut l = limits();
            l.node.max_allocation_requests = 0;
            let error = run(
                &c,
                if receiving {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                },
                |w| {
                    for _ in 0..255 {
                        w.step()?;
                    }
                    if receiving {
                        decode_fragment_cuts_observed(&raw, &d, B, l, &mut |_| Ok(()), w)
                            .map(|_| ())
                    } else {
                        encode_fragment_cuts_observed(
                            &source,
                            EncodedCutsContext {
                                types: &t,
                                type_ids: &CutsTypeIds::new(&source, &ids),
                                source_retained_bytes: B,
                                limits: l,
                            },
                            &mut |_| Ok(()),
                            w,
                        )
                        .map(|_| ())
                    }
                },
            )
            .unwrap_err();
            assert!(matches!(
                error,
                E::Control(CompileControlError::ResourceExhausted)
            ));
            assert_eq!(c.events(), [0]);
        }
    }
    let mut too_big = raw.clone();
    too_big.inbound.reserve_exact(10000);
    c.reset(None);
    let source_floor = size_of::<wire::FragmentCuts>()
        + Layout::array::<wire::InboundFragmentCut>(too_big.inbound.capacity())
            .unwrap()
            .size();
    assert!(matches!(
        run(&c, CompilePhase::Decode, |w| decode_fragment_cuts_observed(
            &too_big,
            &d,
            source_floor - 1,
            limits(),
            &mut |_| Ok(()),
            w
        )),
        Err(E::InvalidShape("cuts source invoice omits retained source"))
    ));
}
#[test]
fn actual_wide_imports_preserve_order_and_sample_real_quantum() {
    let c = Control::default();
    let mut source = cut();
    source.outbound = Box::default();
    // This test isolates one relational import owner; the separate writer cut
    // is outside its source and cannot consume another type occurrence.
    source.inbound = Box::from([source.inbound[0].clone()]);
    source.inbound[0].payload = p::ExchangePayloadContract::Relational;
    source.inbound[0].delivery = p::DeliveryRequirement::DemandDriven;
    source.inbound[0].imports = (0..320)
        .map(|i| p::CutImport {
            source: p::CutValue {
                value: p::ValueId::new(if i % 2 == 0 { u32::MAX } else { i }),
                ty: ty(),
            },
            destination: p::ValueId::new(i),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let roots = [(0, ty())];
    let t = encode_type_table_sources(&roots, &[], tl(), &c).unwrap();
    let d = decode_type_table(t.as_wire(), tl(), &c).unwrap();
    let ids = vec![0; 320];
    c.reset(None);
    let (raw, _) = run(&c, CompilePhase::Encode, |w| {
        encode_fragment_cuts_observed(
            &source,
            EncodedCutsContext {
                types: &t,
                type_ids: &CutsTypeIds::new(&source, &ids),
                source_retained_bytes: B,
                limits: limits(),
            },
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    for (i, x) in raw.inbound[0].imports.iter().enumerate() {
        assert_eq!(x.destination_value_id, Some(i as u32));
        assert_eq!(x.source.as_ref().unwrap().value_type_id, Some(0));
        assert_eq!(
            x.source.as_ref().unwrap().value_id,
            Some(if i % 2 == 0 { u32::MAX } else { i as u32 })
        );
    }
    for receiving in [false, true] {
        let call = |w: &mut CompileCheckpoints<'_>| {
            if receiving {
                decode_fragment_cuts_observed(&raw, &d, B, limits(), &mut |_| Ok(()), w).map(|x| {
                    assert_eq!(x.0.inbound[0].imports.len(), 320);
                    assert_eq!(x.0.inbound[0].imports[319].destination.get(), 319);
                })
            } else {
                encode_fragment_cuts_observed(
                    &source,
                    EncodedCutsContext {
                        types: &t,
                        type_ids: &CutsTypeIds::new(&source, &ids),
                        source_retained_bytes: B,
                        limits: limits(),
                    },
                    &mut |_| Ok(()),
                    w,
                )
                .map(|_| ())
            }
        };
        c.reset(None);
        run(
            &c,
            if receiving {
                CompilePhase::Decode
            } else {
                CompilePhase::Encode
            },
            call,
        )
        .unwrap();
        let baseline = c.events();
        let quantum = baseline
            .iter()
            .position(|u| *u == 256)
            .expect("real owned source loops reach quantum");
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                c.reset(Some((at, cause.clone())));
                assert!(
                    matches!(run(&c,if receiving{CompilePhase::Decode}else{CompilePhase::Encode},call),Err(E::Control(e)) if e==cause)
                );
                assert_eq!(c.events(), baseline[..=at]);
            }
        }
    }
}
#[test]
fn lawful_original_checked_package_cuts_project_without_fake_topology() {
    use novarocks_connector_contract as c;
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let binding = c::ConnectorWriteBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            c::ConnectorCodecCategory::WriteHandle,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    let recipe = c::ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        c::ConnectorWriteInputShape::Data {
            fields: vec![c::ConnectorWriteFieldBinding::new(
                c::ConnectorWriteFieldToken::from_bytes([0; 32]),
                Field::new("x", DataType::Int64, false),
            )],
        },
    )
    .unwrap();
    let package = crate::physical_type_v2::sender_tests::checked_writer_package(recipe);
    assert!(!package.cuts().outbound.is_empty() || !package.cuts().inbound.is_empty());
    let cuts = package.cuts();
    let mut roots = Vec::new();
    types_physical(cuts, |t| {
        roots.push((roots.len() as u32, t.clone()));
        Ok(())
    })
    .unwrap();
    let ids = (0..roots.len() as u32).collect::<Vec<_>>();
    let c = Control::default();
    let t = encode_type_table_sources(&roots, &[], tl(), &c).unwrap();
    let d = decode_type_table(t.as_wire(), tl(), &c).unwrap();
    c.reset(None);
    let (raw, _) = run(&c, CompilePhase::Encode, |w| {
        encode_fragment_cuts_observed(
            cuts,
            EncodedCutsContext {
                types: &t,
                type_ids: &CutsTypeIds::new(cuts, &ids),
                source_retained_bytes: B,
                limits: limits(),
            },
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    let expected = &cuts.outbound[0];
    assert_eq!(raw.outbound[0].edge_id, Some(expected.edge.get()));
    assert_eq!(
        raw.outbound[0].destination_fragment_id,
        Some(expected.destination_fragment.get())
    );
    assert_eq!(
        raw.outbound[0]
            .projection
            .iter()
            .map(|v| v.value_id.unwrap())
            .collect::<Vec<_>>(),
        expected
            .projection
            .iter()
            .map(|v| v.value.get())
            .collect::<Vec<_>>()
    );
    c.reset(None);
    let (decoded, _) = run(&c, CompilePhase::Decode, |w| {
        decode_fragment_cuts_observed(&raw, &d, B, limits(), &mut |_| Ok(()), w)
    })
    .unwrap();
    assert_eq!(&decoded, cuts);
    let mut original_input = package.into_input();
    original_input.cuts = decoded;
    let admitted = p::FragmentPackage::try_new(
        original_input,
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: 128 * B,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 64 * B,
                max_coexisting_bytes: 512 * B,
                max_projection_work: 128 * B,
            },
        },
        &c,
    );
    assert!(admitted.is_ok());
}
fn bindings() -> Box<[p::RuntimeFilterBindingCut]> {
    Box::from([
        p::RuntimeFilterBindingCut {
            binding_id: 1,
            filter: p::RuntimeFilterId::new(u32::MAX),
            role: p::RuntimeFilterBindingRole::Producer(0),
        },
        p::RuntimeFilterBindingCut {
            binding_id: u32::MAX,
            filter: p::RuntimeFilterId::new(0),
            role: p::RuntimeFilterBindingRole::Consumer(u32::MAX as usize),
        },
        p::RuntimeFilterBindingCut {
            binding_id: 0,
            filter: p::RuntimeFilterId::new(u32::MAX),
            role: p::RuntimeFilterBindingRole::Consumer(0),
        },
    ])
}
// Remote identities are preserved exactly, including ones the Package law
// will refuse: the codec projects the table, the Package judges it.
#[test]
fn runtime_filter_bindings_have_hand_oracles_and_roundtrip_byte_identically() {
    use prost::Message;
    use wire::runtime_filter_binding_cut::Role;
    let c = Control::default();
    let mut src = cut();
    src.runtime_filters = Box::from([filter()]);
    src.runtime_filter_bindings = bindings();
    let (raw, _) = encode(&src, &c, limits()).unwrap();
    assert_eq!(
        raw.runtime_filter_bindings,
        [
            wire::RuntimeFilterBindingCut {
                binding_id: Some(1),
                runtime_filter_id: Some(u32::MAX),
                role: Some(Role::ProducerIndex(0)),
            },
            wire::RuntimeFilterBindingCut {
                binding_id: Some(u32::MAX),
                runtime_filter_id: Some(0),
                role: Some(Role::ConsumerIndex(u32::MAX)),
            },
            wire::RuntimeFilterBindingCut {
                binding_id: Some(0),
                runtime_filter_id: Some(u32::MAX),
                role: Some(Role::ConsumerIndex(0)),
            },
        ]
    );
    let (owned, _) = decode(&raw, &c, limits()).unwrap();
    assert_eq!(owned, src);
    let (again, _) = encode(&owned, &c, limits()).unwrap();
    assert_eq!(again.encode_to_vec(), raw.encode_to_vec());

    for case in 0..3 {
        let mut x = raw.clone();
        match case {
            0 => x.runtime_filter_bindings[1].binding_id = None,
            1 => x.runtime_filter_bindings[1].runtime_filter_id = None,
            _ => x.runtime_filter_bindings[1].role = None,
        }
        assert!(
            matches!(
                decode(&x, &c, limits()),
                Err(E::InvalidShape("cut required field is absent"))
            ),
            "case {case}"
        );
    }
    if let Ok(index) = usize::try_from(u64::from(u32::MAX) + 1) {
        let mut wide = src;
        wide.runtime_filter_bindings[0].role = p::RuntimeFilterBindingRole::Producer(index);
        assert!(matches!(
            encode(&wide, &c, limits()),
            Err(E::InvalidShape(
                "cut runtime-filter binding index exceeds u32"
            ))
        ));
    }
}
#[test]
fn runtime_filter_bindings_are_one_admitted_root_extent_under_all_seven_envelopes() {
    let c = Control::default();
    let source = p::FragmentCuts {
        runtime_filter_bindings: bindings(),
        ..Default::default()
    };
    let (raw, ef) = encode(&source, &c, limits()).unwrap();
    let (_, df) = decode(&raw, &c, limits()).unwrap();
    // One table of three fixed-size entries, one filter reference each.
    assert_eq!(
        (
            ef.input_node_count,
            ef.value_reference_count,
            ef.allocation_requests_upper_bound,
            ef.allocation_request_bytes_upper_bound
        ),
        (
            3,
            3,
            1,
            Layout::array::<wire::RuntimeFilterBindingCut>(3)
                .unwrap()
                .size()
        )
    );
    assert_eq!(
        (
            df.input_node_count,
            df.value_reference_count,
            df.allocation_requests_upper_bound,
            df.allocation_request_bytes_upper_bound
        ),
        (
            3,
            3,
            2,
            2 * Layout::array::<p::RuntimeFilterBindingCut>(3)
                .unwrap()
                .size()
        )
    );
    for receiving in [false, true] {
        let f = if receiving { df } else { ef };
        let mut exact = limits();
        exact.node.max_input_nodes = f.input_node_count;
        exact.node.max_value_references = f.value_reference_count;
        exact.node.max_list_items = f.list_item_count;
        exact.node.max_allocation_requests = f.allocation_requests_upper_bound;
        exact.node.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
        exact.node.max_coexisting_source_and_request_bytes =
            f.coexisting_source_and_request_bytes_upper_bound;
        exact.node.max_work = f.cumulative_work_upper_bound;
        if receiving {
            decode(&raw, &c, exact).unwrap();
        } else {
            encode(&source, &c, exact).unwrap();
        }
        for axis in 0..7 {
            let mut l = exact;
            let v = match axis {
                0 => &mut l.node.max_input_nodes,
                1 => &mut l.node.max_value_references,
                2 => &mut l.node.max_list_items,
                3 => &mut l.node.max_allocation_requests,
                4 => &mut l.node.max_allocation_request_bytes,
                5 => &mut l.node.max_coexisting_source_and_request_bytes,
                _ => &mut l.node.max_work,
            };
            assert!(*v > 0);
            *v -= 1;
            let error = if receiving {
                decode(&raw, &c, l).unwrap_err()
            } else {
                encode(&source, &c, l).unwrap_err()
            };
            assert!(
                matches!(error, E::Control(CompileControlError::ResourceExhausted)),
                "receiving {receiving} axis {axis}"
            );
        }
    }
}
