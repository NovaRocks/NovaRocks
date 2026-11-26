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

use super::{NodeGraphEvent, validate_node_graph, visit_node_graph_child_first};
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;
use crate::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, SemanticParameters,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
const PROJECTION: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};
// Explicit fixture invoice; this does not model the validator's opaque scratch.
const SOURCE: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after the original graph refusal");
        }
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn diamond() -> Fragment {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let source = NodeId::new(u32::MAX);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in [0, 7] {
        builder
            .add_limit(NodeId::new(id), source, Some(1), 0)
            .unwrap();
    }
    builder
        .add_row_consuming(
            NodeId::new(1),
            Box::from([NodeId::new(0), NodeId::new(7), NodeId::new(0)]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::default(),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: Box::from([Box::default(), Box::default(), Box::default()]),
            },
        )
        .unwrap();
    builder
        .finish_fixture_definition(
            NodeId::new(1),
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn wide() -> Fragment {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let mut child = NodeId::new(u32::MAX);
    builder
        .add_values(child, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in 0..319 {
        let parent = NodeId::new(id);
        builder.add_limit(parent, child, Some(1), 0).unwrap();
        child = parent;
    }
    builder
        .finish_fixture_definition(
            child,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn collect(fragment: &Fragment) -> (Option<usize>, Vec<NodeId>) {
    let mut ready = vec![];
    let complete = visit_node_graph_child_first(fragment, |event| {
        if let NodeGraphEvent::Ready(id) = event {
            ready.push(id);
        }
        Ok::<_, std::convert::Infallible>(true)
    })
    .unwrap();
    (complete, ready)
}
fn assert_child_first(fragment: &Fragment, ready: &[NodeId]) {
    assert_eq!(ready.len(), fragment.nodes().len());
    let mut seen = BTreeSet::new();
    for id in ready {
        let node = &fragment.nodes()[id];
        assert!(node.inputs.iter().all(|child| seen.contains(child)));
        assert!(seen.insert(*id), "a definition was visited twice");
        assert!(std::ptr::eq(node, fragment.nodes().get(id).unwrap()));
    }
}
fn graph_errors(fragment: &Fragment) -> Vec<String> {
    let mut errors = crate::validation::ValidationContext::new();
    validate_node_graph(fragment, &mut errors);
    errors
        .into_vec()
        .into_iter()
        .map(|e| e.message().to_owned())
        .collect()
}
// Composition test: the caller owns entry, each actual event and the tail.
fn observed(fragment: &Fragment, control: &Control) -> Result<Option<usize>, CompileControlError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = visit_node_graph_child_first(fragment, |_| {
        work.step()?;
        Ok::<_, CompileControlError>(true)
    })?;
    work.finish()?;
    Ok(result)
}
fn prefixes<T>(call: impl Fn(&Control) -> Result<T, CompileControlError>, all: bool) {
    let baseline = Control::default();
    assert!(call(&baseline).is_ok());
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    for (at, units) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(matches!(call(&control), Err(actual) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn node_graph_sparse_shared_duplicate_ports_are_child_first_once() {
    let fragment = diamond();
    crate::validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    let (complete, ready) = collect(&fragment);
    assert_eq!(complete, Some(4));
    assert_eq!(
        ready,
        [
            NodeId::new(u32::MAX),
            NodeId::new(7),
            NodeId::new(0),
            NodeId::new(1)
        ]
    );
    assert_child_first(&fragment, &ready);
    assert!(graph_errors(&fragment).is_empty());
}

#[test]
fn node_graph_explicit_stop_at_step_or_ready_never_reports_completion() {
    let fragment = diamond();
    for stop_on_ready in [false, true] {
        let mut ready = vec![];
        let complete = visit_node_graph_child_first(&fragment, |event| {
            if let NodeGraphEvent::Ready(id) = event {
                ready.push(id);
            }
            Ok::<_, std::convert::Infallible>(if stop_on_ready {
                !matches!(event, NodeGraphEvent::Ready(_))
            } else {
                false
            })
        })
        .unwrap();
        assert_eq!(complete, None);
        assert_eq!(ready.len(), usize::from(stop_on_ready));
    }
}

#[test]
fn node_graph_original_cycle_unreachable_and_missing_reference_guards_remain() {
    let fragment = diamond();
    let mut cycle = fragment.clone().into_parts();
    cycle.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(7)]);
    cycle.nodes.get_mut(&NodeId::new(7)).unwrap().inputs = Box::from([NodeId::new(0)]);
    let cycle = Fragment::from(cycle);
    assert_eq!(collect(&cycle).0, Some(1));
    assert!(
        graph_errors(&cycle)
            .iter()
            .any(|e| e == "node graph contains a cycle")
    );
    assert!(crate::validate_fragment(&cycle, &FragmentCuts::default()).is_err());

    let mut unreachable = fragment.clone().into_parts();
    let mut extra = unreachable.nodes[&NodeId::new(u32::MAX)].clone();
    extra.id = NodeId::new(99);
    extra.output.node = extra.id;
    unreachable.nodes.insert(extra.id, extra);
    let unreachable = Fragment::from(unreachable);
    assert_eq!(collect(&unreachable).0, Some(5));
    assert!(
        graph_errors(&unreachable)
            .iter()
            .any(|e| e == "fragment contains nodes unreachable from its root")
    );
    assert!(crate::validate_fragment(&unreachable, &FragmentCuts::default()).is_err());

    let mut missing = fragment.into_parts();
    missing.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(123)]);
    let missing = Fragment::from(missing);
    // Kahn counts only defined inputs; the original reference validator rejects it.
    assert_eq!(collect(&missing).0, Some(4));
    assert!(crate::validate_fragment(&missing, &FragmentCuts::default()).is_err());
}

#[test]
fn node_graph_caller_control_every_small_prefix_success_and_incomplete_tail() {
    let fragment = diamond();
    prefixes(|control| observed(&fragment, control), true);
    let baseline = Control::default();
    assert_eq!(observed(&fragment, &baseline).unwrap(), Some(4));
    assert!(*baseline.trace.lock().unwrap().last().unwrap() > 0);
    let mut cycle = fragment.into_parts();
    cycle.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(7)]);
    cycle.nodes.get_mut(&NodeId::new(7)).unwrap().inputs = Box::from([NodeId::new(0)]);
    let cycle = Fragment::from(cycle);
    prefixes(|control| observed(&cycle, control), true);
    assert_eq!(observed(&cycle, &Control::default()).unwrap(), Some(1));
}

#[test]
fn node_graph_wide_real_events_reach_quantum_and_keep_child_first_order() {
    let fragment = wide();
    let (complete, ready) = collect(&fragment);
    assert_eq!(complete, Some(320));
    assert_child_first(&fragment, &ready);
    let control = Control::default();
    assert_eq!(observed(&fragment, &control).unwrap(), Some(320));
    assert!(control.trace.lock().unwrap().contains(&256));
    prefixes(|control| observed(&fragment, control), false);
    let input = package_input(fragment);
    let control = Control::default();
    assert!(property(&input, &control).unwrap());
    assert!(control.trace.lock().unwrap().contains(&256));
    prefixes(|control| property(&input, control), false);
}

fn package_input(fragment: Fragment) -> FragmentPackageInput {
    let setup = Control::default();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &setup).unwrap();
    FragmentPackageInput {
        constants: ConstantPools::empty(),
        version: PlanVersionId::try_new([9; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment,

        pruning,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    }
}
fn property(input: &FragmentPackageInput, control: &Control) -> Result<bool, CompileControlError> {
    match validate_fragment_output_properties_observed(
        &input.fragment,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        control,
    ) {
        Ok(_) => Ok(true),
        Err(FragmentPropertyError::Control(cause)) => Err(cause),
        Err(FragmentPropertyError::Structure(_)) => Ok(false),
        Err(error) => panic!("unexpected property source error: {error}"),
    }
}

#[test]
fn node_graph_public_frozen_property_and_package_use_same_checked_snapshot() {
    let input = package_input(diamond());
    assert!(property(&input, &Control::default()).unwrap());
    prefixes(|control| property(&input, control), true);
    let package = FragmentPackage::try_new(
        input,
        FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: PROJECTION,
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(collect(package.fragment()).0, Some(4));
    assert!(
        validate_fragment_output_properties_observed(
            package.fragment(),
            PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            &Control::default(),
        )
        .is_ok()
    );

    let mut wrong = diamond().into_parts();
    wrong
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .output_properties
        .distribution = Distribution::RoundRobin;
    let wrong = package_input(Fragment::from(wrong));
    assert!(!property(&wrong, &Control::default()).unwrap());
    prefixes(|control| property(&wrong, control), true);
}
