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
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;
use crate::*;
use arrow_schema::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorReadRelationPayload,
};
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, owned_resources::copy::copy_string,
};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original index refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn singleton() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn empty() -> Fragment {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    builder
        .add_values(NodeId::new(0), Box::from([Box::default()]), Box::default())
        .unwrap();
    builder
        .finish_fixture_definition(
            NodeId::new(0),
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn node(id: u32, inputs: &[u32], outputs: &[u32]) -> PhysicalNode {
    PhysicalNode {
        expressions: None,
        id: NodeId::new(id),
        inputs: inputs.iter().copied().map(NodeId::new).collect(),
        required_inputs: vec![singleton(); inputs.len()].into_boxed_slice(),
        output_properties: singleton(),
        output: OutputPort {
            node: NodeId::new(id),
            columns: outputs.iter().copied().map(ValueId::new).collect(),
        },
        kind: NodeKind::Values {
            rows: Box::default(),
        },
    }
}
// Original private parts expose malformed producer states to the original
// index. These fixtures do not claim structural or Package certification.
fn ports_fixture(inputs: &[u32]) -> Fragment {
    let mut parts = empty().into_parts();
    parts.nodes = BTreeMap::from([
        (NodeId::new(0), node(0, &[], &[42])),
        (NodeId::new(1), node(1, inputs, &[])),
    ]);
    parts.into()
}
fn run(
    fragment: &Fragment,
    control: &Control,
) -> Result<(FragmentValidationIndexes, ControlOwnedResourceFacts), ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut counter = ControlResourceCounter::default();
    let indexes =
        FragmentValidationIndexes::new_in(fragment, &mut counter, &mut |_| Ok(()), &mut work)?;
    work.finish()?;
    Ok((indexes, counter.facts()))
}
fn assert_same(plain: &FragmentValidationIndexes, borrowed: &FragmentValidationIndexes) {
    assert_eq!(
        plain.output_ports.keys().collect::<Vec<_>>(),
        borrowed.output_ports.keys().collect::<Vec<_>>()
    );
    for (id, original) in &plain.output_ports {
        assert_eq!(original.occurrences, borrowed.output_ports[id].occurrences);
    }
    for (id, original) in &plain.visible_inputs {
        let actual = &borrowed.visible_inputs[id];
        match (original, actual) {
            (VisibleInputIndex::Empty, VisibleInputIndex::Empty) => {}
            (VisibleInputIndex::One(a), VisibleInputIndex::One(b)) => {
                assert_eq!(a.occurrences, b.occurrences)
            }
            (VisibleInputIndex::Many(a), VisibleInputIndex::Many(b)) => {
                assert_eq!(a.len(), b.len());
                for (a, b) in a.iter().zip(b.iter()) {
                    assert_eq!(a.occurrences, b.occurrences);
                }
            }
            _ => panic!("original visible-port variant changed"),
        }
    }
}

#[test]
fn missing_and_repeated_inputs_preserve_empty_one_many_and_real_trim_invoice() {
    let base = run(&ports_fixture(&[]), &Control::default()).unwrap().1;
    for inputs in [&[99, 0, 0, 99][..], &[99, 0][..], &[99, 98][..]] {
        let fragment = ports_fixture(inputs);
        let (borrowed, facts) = run(&fragment, &Control::default()).unwrap();
        assert_same(&FragmentValidationIndexes::new(&fragment), &borrowed);
        let port = &borrowed.output_ports[&NodeId::new(0)];
        match &borrowed.visible_inputs[&NodeId::new(1)] {
            VisibleInputIndex::Many(ports) => {
                assert_eq!(ports.len(), 2);
                assert!(ports.iter().all(|actual| Arc::ptr_eq(actual, port)));
                // Independent layouts: raw four-input temporary then two
                // matched handles in the conditional boxed trim.
                assert_eq!(
                    facts.allocation_requests_upper_bound - base.allocation_requests_upper_bound,
                    2
                );
                assert_eq!(
                    facts.allocation_request_bytes_upper_bound
                        - base.allocation_request_bytes_upper_bound,
                    Layout::array::<Arc<ValuePortIndex>>(4).unwrap().size()
                        + Layout::array::<Arc<ValuePortIndex>>(2).unwrap().size()
                );
            }
            VisibleInputIndex::One(actual) => {
                assert_eq!(inputs, [99, 0]);
                assert!(Arc::ptr_eq(actual, port));
                assert_eq!(Arc::strong_count(port), 2);
                assert_eq!(
                    facts.allocation_requests_upper_bound - base.allocation_requests_upper_bound,
                    1
                );
            }
            VisibleInputIndex::Empty => {
                assert_eq!(inputs, [99, 98]);
                assert_eq!(
                    facts.allocation_requests_upper_bound - base.allocation_requests_upper_bound,
                    1
                );
            }
        }
    }
}

#[test]
fn malformed_duplicate_node_identity_preserves_last_original_winner_and_occurrences() {
    let mut parts = empty().into_parts();
    parts.nodes = BTreeMap::from([
        (NodeId::new(0), node(7, &[], &[11])),
        (NodeId::new(1), node(7, &[], &[22, 22, 33])),
        (NodeId::new(u32::MAX), node(8, &[7], &[])),
    ]);
    let fragment: Fragment = parts.into();
    let (borrowed, facts) = run(&fragment, &Control::default()).unwrap();
    assert_same(&FragmentValidationIndexes::new(&fragment), &borrowed);
    assert_eq!(borrowed.output_ports.len(), 2);
    assert_eq!(
        borrowed.output(NodeId::new(7)).unwrap().occurrences,
        BTreeMap::from([(ValueId::new(22), 2), (ValueId::new(33), 1)])
    );
    assert!(
        !borrowed
            .output(NodeId::new(7))
            .unwrap()
            .contains(&ValueId::new(11))
    );
    assert!(
        matches!(borrowed.visible_input(NodeId::new(8)), Some(VisibleInputIndex::One(port)) if Arc::ptr_eq(port, &borrowed.output_ports[&NodeId::new(7)]))
    );
    // Both displaced/retained inner inputs contribute their occurrence
    // request bounds: 3 outer + 3 outer + 3 Arc + (1+3) inner + 1 temp.
    assert_eq!(facts.allocation_requests_upper_bound, 14);
}

fn scan_fixture(count: usize) -> Fragment {
    let instance = ConnectorInstanceId::parse("index-source").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    );
    let encoded = |category| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![1].into(),
        )
    };
    let column = ProviderColumnReference {
        column_payload: encoded(ConnectorCodecCategory::ReadColumn),
    };
    let relation = Relation::Metadata(MetadataRelation {
        kind: MetadataRelationKind::try_new("iceberg.manifest.entries").unwrap(),
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                encoded(ConnectorCodecCategory::ReadTable),
                encoded(ConnectorCodecCategory::ReadView),
            ),
        },

        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column: column.clone(),
            ty: ValueType::new(DataType::Int64, false),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: singleton(),
        coverage_evidence: Box::from([4]),
    });
    let mut scan = node(0, &[], &[42]);
    scan.kind = NodeKind::Scan {
        occurrence: ProviderReadOccurrenceId::new(0),
        relation: Box::new(relation),
        provider_outputs: Box::from([(column, ValueId::new(7))]),
        residuals: Box::default(),
        derived_values: (0..count)
            .map(|ordinal| ValueId::new(if ordinal % 2 == 0 { 7 } else { 9 }))
            .collect(),
    };
    let mut parts = empty().into_parts();
    parts.nodes.insert(NodeId::new(0), scan);
    parts.into()
}
#[test]
fn scan_provider_and_derived_occurrences_create_their_own_shared_port_and_real_quantum() {
    let fragment = scan_fixture(320);
    let control = Control::default();
    let (borrowed, _) = run(&fragment, &control).unwrap();
    assert_same(&FragmentValidationIndexes::new(&fragment), &borrowed);
    let VisibleInputIndex::One(port) = borrowed.visible_input(NodeId::new(0)).unwrap() else {
        panic!("scan must have one own port")
    };
    assert!(!Arc::ptr_eq(port, &borrowed.output_ports[&NodeId::new(0)]));
    assert_eq!(
        port.occurrences,
        BTreeMap::from([(ValueId::new(7), 161), (ValueId::new(9), 160)])
    );
    let trace = control.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("actual scan-copy quantum");
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert_eq!(run(&fragment, &control).err(), Some(cause.into()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn every_actual_small_index_callback_preserves_three_causes_and_original_caller_tail() {
    for fragment in [ports_fixture(&[99, 0, 0]), scan_fixture(2)] {
        let control = Control::default();
        run(&fragment, &control).unwrap();
        let trace = control.trace.lock().unwrap().clone();
        assert!(trace.len() > 1);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(run(&fragment, &control).err(), Some(cause.into()));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn original_empty_output_arc_header_refuses_before_real_pending_copy_late_control() {
    for cause in CAUSES {
        let control = Control {
            refusal: Some((3, cause)),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let text = "x".repeat(255);
        let copied = copy_string::<ControlResourceError>(&text, &mut work).unwrap();
        assert_eq!(copied, text);
        assert_ne!(copied.as_ptr(), text.as_ptr());
        assert_eq!(*control.trace.lock().unwrap(), [0, 0, 1]);
        let mut counter = ControlResourceCounter::default();
        let result = FragmentValidationIndexes::new_in(
            &empty(),
            &mut counter,
            &mut |facts| {
                // Two outer insertion bounds plus the empty-output Arc are
                // three original requests, known before any index callback.
                assert_eq!(facts.allocation_requests_upper_bound, 3);
                if facts.allocation_requests_upper_bound > 2 {
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            },
            &mut work,
        );
        assert_eq!(
            result.err(),
            Some(CompileControlError::ResourceExhausted.into())
        );
        assert_eq!(*control.trace.lock().unwrap(), [0, 0, 1]);
    }
}

#[test]
fn genuine_wide_project_fanout_keeps_one_source_port_with_full_original_occurrences() {
    let mut builder = FragmentBuilder::new(FragmentId::new(904));
    let source = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let values = (0..4096)
        .map(|ordinal| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            output: OutputPort {
                node: source,
                columns: values.clone().into_boxed_slice(),
            },
            ..node(source.get(), &[], &[])
        })
        .unwrap();
    let mut projects = Vec::new();
    let mut mappings = Vec::new();
    for value in values.iter().take(256) {
        let project = builder.reserve_node_id().unwrap();
        let expression = builder
            .define(project, ty.clone(), ExprKind::input(*value))
            .unwrap();
        let expression = builder.root_definition(
            project,
            expression,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        );
        let mut item = node(project.get(), &[source.get()], &[value.get()]);
        item.kind = NodeKind::Project {
            expressions: Box::from([(expression, *value)]),
        };
        builder.insert_node_unchecked(item).unwrap();
        projects.push(project);
        mappings.push(Box::from([*value]));
    }
    let union = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node: union,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let mut item = node(
        union.get(),
        &projects.iter().map(|id| id.get()).collect::<Vec<_>>(),
        &[value.get()],
    );
    item.kind = NodeKind::SetOp {
        kind: SetOperationKind::UnionAll,
        input_mappings: mappings.into_boxed_slice(),
    };
    builder.insert_node_unchecked(item).unwrap();
    let fragment = builder
        .finish_fixture_definition(
            union,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (borrowed, _) = run(&fragment, &Control::default()).unwrap();
    assert_same(&FragmentValidationIndexes::new(&fragment), &borrowed);
    let port = &borrowed.output_ports[&source];
    assert_eq!(port.occurrences.len(), 4096);
    assert!(projects.iter().all(|id| matches!(borrowed.visible_input(*id),Some(VisibleInputIndex::One(actual)) if Arc::ptr_eq(actual,port))));
    assert_eq!(Arc::strong_count(port), 257);
}
