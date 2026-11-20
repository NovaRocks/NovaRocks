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

use novarocks_physical_plan::{
    DeliveryRequirement, Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning,
    EdgeSource, FragmentBuilder, FragmentId, FragmentOutputPort, FragmentSink, NodeId, NodeKind,
    OutputRole, PipelineDopDomain, RequiredInputs, RowMultiplicity, SetOperationKind,
    StagePlacementPolicy,
    wait_graph::{WaitGraph, WaitResolution},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::collections::BTreeMap;

struct Control;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

const DOP: PipelineDopDomain = PipelineDopDomain {
    min: 1,
    max: 1,
    requires_power_of_two: false,
};

fn resolve(shared: [usize; 2], kind: SetOperationKind) -> WaitResolution {
    let source = FragmentId::new(0);
    let destination = FragmentId::new(1);
    let source_root = NodeId::new(0);
    let inputs = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let root = NodeId::new(3);

    let mut producer = FragmentBuilder::new(source);
    producer
        .add_values(source_root, Box::default(), Box::default())
        .unwrap();
    let mut consumer = FragmentBuilder::new(destination);
    let mut edges = BTreeMap::new();
    for (ordinal, node) in inputs.into_iter().enumerate() {
        if !shared.contains(&ordinal) {
            consumer
                .add_values(node, Box::default(), Box::default())
                .unwrap();
            continue;
        }
        let id = EdgeId::new(u32::try_from(edges.len()).unwrap());
        consumer
            .add_exchange_source(
                node,
                id,
                Box::default(),
                Box::default(),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .unwrap();
        edges.insert(
            id,
            Edge {
                id,
                role: OutputRole::Relational,
                delivery: DeliveryRequirement::DemandDriven,
                kind: EdgeKind::CteMulticast,
                source: EdgeSource {
                    fragment: source,
                    port: FragmentOutputPort::Root,
                    projection: Box::default(),
                },
                destination: EdgeDestination {
                    fragment: destination,
                    node,
                    receive_mapping: Box::default(),
                },
                partitioning: EdgePartitioning {
                    source: Distribution::Singleton,
                    source_multiplicity: RowMultiplicity::SingleCopy,
                    destination: Distribution::Singleton,
                    destination_multiplicity: RowMultiplicity::SingleCopy,
                    routing_only: Box::default(),
                },
            },
        );
    }
    consumer
        .add_row_consuming(
            root,
            Box::new(inputs),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::default(),
            NodeKind::SetOp {
                kind,
                input_mappings: Box::new([Box::default(), Box::default(), Box::default()]),
            },
        )
        .unwrap();
    let producer = producer
        .finish_definition(
            source_root,
            FragmentSink::Multicast {
                edges: edges.keys().copied().collect(),
            },
            DOP,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let consumer = consumer
        .finish_definition(
            root,
            FragmentSink::Result,
            DOP,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let fragments = BTreeMap::from([(source, producer), (destination, consumer)]);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let mut graph = WaitGraph::build_observed(&fragments, &edges, [], &mut work).unwrap();
    let resolution = graph.resolve_observed(&mut work).unwrap();
    work.finish().unwrap();
    resolution
}

#[test]
fn set_op_probes_can_share_one_producer() {
    for kind in [SetOperationKind::Intersect, SetOperationKind::Except] {
        let resolution = resolve([1, 2], kind);
        assert!(resolution.is_acyclic(), "{kind:?}: {resolution:?}");
    }
}

#[test]
fn set_op_build_and_probe_sharing_a_producer_still_form_a_cycle() {
    for kind in [SetOperationKind::Intersect, SetOperationKind::Except] {
        let resolution = resolve([0, 2], kind);
        assert!(!resolution.is_acyclic(), "{kind:?}: {resolution:?}");
    }
}

#[test]
fn union_all_still_allows_shared_input() {
    assert!(resolve([0, 2], SetOperationKind::UnionAll).is_acyclic());
}
