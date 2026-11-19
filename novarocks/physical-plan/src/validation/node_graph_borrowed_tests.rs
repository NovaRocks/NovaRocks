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
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;
use crate::{FragmentBuilder, PipelineDopDomain, RequiredInputs, SetOperationKind};
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, owned_resources::copy::copy_string,
};
use std::{alloc::Layout, sync::Mutex};

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
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
    fn at(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::default(),
            refusal: Some((at, cause)),
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after first refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
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
    let child = NodeId::new(u32::MAX);
    builder
        .add_values(child, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in [0, 7] {
        builder
            .add_limit(NodeId::new(id), child, Some(1), 0)
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
    for raw in 0..319 {
        let parent = NodeId::new(raw);
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
fn plain_events(fragment: &Fragment) -> (Option<usize>, Vec<NodeGraphEvent>) {
    let mut events = Vec::new();
    let completed = visit_node_graph_child_first(fragment, |event| {
        events.push(event);
        Ok::<_, std::convert::Infallible>(true)
    })
    .unwrap();
    (completed, events)
}
fn borrowed_events(
    fragment: &Fragment,
    control: &Control,
    stop: Option<usize>,
) -> Result<(Option<usize>, Vec<NodeGraphEvent>), ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut counter = ControlResourceCounter::default();
    let mut events = Vec::new();
    let completed = visit_node_graph_child_first_in(
        fragment,
        &mut counter,
        &mut |_| Ok(()),
        &mut work,
        |event, work| {
            events.push(event);
            work.step()?;
            Ok::<_, ControlResourceError>(stop != Some(events.len() - 1))
        },
    )?;
    work.finish()?;
    Ok((completed, events))
}
fn borrowed_errors(
    fragment: &Fragment,
    control: &Control,
) -> Result<Vec<String>, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut counter = ControlResourceCounter::default();
    let mut errors = ValidationContext::new();
    validate_node_graph_in(
        fragment,
        &mut errors,
        &mut counter,
        &mut |_| Ok(()),
        &mut work,
    )?;
    work.finish()?;
    Ok(errors
        .into_vec()
        .into_iter()
        .map(|error| error.message().to_owned())
        .collect())
}
fn plain_errors(fragment: &Fragment) -> Vec<String> {
    let mut errors = ValidationContext::new();
    validate_node_graph(fragment, &mut errors);
    errors
        .into_vec()
        .into_iter()
        .map(|error| error.message().to_owned())
        .collect()
}
fn prefixes<T>(call: impl Fn(&Control) -> Result<T, ControlResourceError>) {
    let baseline = Control::default();
    assert!(call(&baseline).is_ok());
    let trace = baseline.trace();
    assert_eq!(trace[0], 0);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::at(at, cause.clone());
            assert!(
                matches!(call(&control), Err(ControlResourceError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn borrowed_kahn_keeps_exact_sparse_shared_duplicate_events_and_explicit_stops() {
    let fragment = diamond();
    crate::validate_fragment(&fragment, &crate::FragmentCuts::default()).unwrap();
    let (completed, events) = plain_events(&fragment);
    assert_eq!(completed, Some(4));
    let ready = events
        .iter()
        .filter_map(|event| match event {
            NodeGraphEvent::Ready(id) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ready,
        [
            NodeId::new(u32::MAX),
            NodeId::new(7),
            NodeId::new(0),
            NodeId::new(1)
        ]
    );
    assert_eq!(
        borrowed_events(&fragment, &Control::default(), None).unwrap(),
        (completed, events.clone())
    );
    for stop in [
        0,
        events
            .iter()
            .position(|event| matches!(event, NodeGraphEvent::Ready(_)))
            .unwrap(),
    ] {
        let control = Control::default();
        let (completed, prefix) = borrowed_events(&fragment, &control, Some(stop)).unwrap();
        assert_eq!(completed, None);
        assert_eq!(prefix, events[..=stop]);
    }
}

#[test]
fn borrowed_node_validator_keeps_original_cycle_unreachable_and_unknown_id_diagnostics() {
    let source = diamond();
    let mut cycle = source.clone().into_parts();
    cycle.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(7)]);
    cycle.nodes.get_mut(&NodeId::new(7)).unwrap().inputs = Box::from([NodeId::new(0)]);
    let mut unreachable = source.clone().into_parts();
    let mut extra = unreachable.nodes[&NodeId::new(u32::MAX)].clone();
    extra.id = NodeId::new(99);
    extra.output.node = extra.id;
    unreachable.nodes.insert(extra.id, extra);
    let mut unknown = source.clone().into_parts();
    unknown.nodes.get_mut(&NodeId::new(1)).unwrap().inputs = Box::from([
        NodeId::new(0),
        NodeId::new(7),
        NodeId::new(123),
        NodeId::new(123),
    ]);
    // This malformed root is still a real fragment representation; the
    // original full validator rejects it, independently of Kahn scratch.
    let mut unknown_root = source.into_parts();
    unknown_root.root = NodeId::new(123);
    for parts in [cycle, unreachable, unknown, unknown_root] {
        let fragment = Fragment::from(parts);
        assert!(crate::validate_fragment(&fragment, &crate::FragmentCuts::default()).is_err());
        assert_eq!(
            borrowed_errors(&fragment, &Control::default()).unwrap(),
            plain_errors(&fragment)
        );
    }
}

#[test]
fn borrowed_kahn_hand_tree_and_actual_vec_request_oracle_preserves_parent_and_exact_replay() {
    let fragment = diamond();
    let parent = Vec::<u64>::with_capacity(2);
    assert_eq!(parent.capacity(), 2);
    let mut expected = ControlResourceCounter::default();
    expected.buffer::<u64>(2, 1).unwrap();
    expected.tree::<NodeId, usize>(4).unwrap();
    expected.tree::<NodeId, Vec<NodeId>>(4).unwrap();
    expected.work(16).unwrap();
    let per_input = 4 * ControlResourceCounter::lookup_work(4).unwrap() + 4;
    for raw in [1, 3, 1, 0] {
        expected.tree::<NodeId, ()>(raw).unwrap();
        expected.work(raw * per_input).unwrap();
    }
    // Actual DAG: MAX has two users in one Vec; 0 and 7 each one user;
    // all ready nodes reuse the same capacity-four Vec. Four requests total.
    for _ in 0..4 {
        expected
            .layout(Layout::array::<NodeId>(4).unwrap(), 1)
            .unwrap();
    }
    let expected = expected.facts();
    for exact in [false, true] {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut counter = ControlResourceCounter::default();
        counter.buffer::<u64>(parent.capacity(), 1).unwrap();
        let initial = counter.facts();
        let mut snapshots = Vec::new();
        let result = visit_node_graph_child_first_in::<ControlResourceError>(
            &fragment,
            &mut counter,
            &mut |facts| {
                assert!(
                    facts.allocation_requests_upper_bound
                        >= initial.allocation_requests_upper_bound
                );
                assert!(
                    facts.allocation_request_bytes_upper_bound
                        >= initial.allocation_request_bytes_upper_bound
                );
                snapshots.push(*facts);
                if exact {
                    assert!(
                        facts.allocation_requests_upper_bound
                            <= expected.allocation_requests_upper_bound
                    );
                    assert!(
                        facts.allocation_request_bytes_upper_bound
                            <= expected.allocation_request_bytes_upper_bound
                    );
                    assert!(
                        facts.cumulative_work_upper_bound <= expected.cumulative_work_upper_bound
                    );
                }
                Ok(())
            },
            &mut work,
            |_, work| {
                work.step()?;
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(result, Some(4));
        assert_eq!(counter.facts(), expected);
        assert_eq!(snapshots.last(), Some(&expected));
        for pair in snapshots.windows(2) {
            assert!(
                pair[0].allocation_requests_upper_bound <= pair[1].allocation_requests_upper_bound
            );
            assert!(
                pair[0].allocation_request_bytes_upper_bound
                    <= pair[1].allocation_request_bytes_upper_bound
            );
            assert!(pair[0].cumulative_work_upper_bound <= pair[1].cumulative_work_upper_bound);
        }
        work.finish().unwrap();
    }
}

#[test]
fn borrowed_kahn_header_and_actual_dedup_refusals_precede_late_control_from_real_copy() {
    let fragment = diamond();
    let mut header = ControlResourceCounter::default();
    header.tree::<NodeId, usize>(4).unwrap();
    header.tree::<NodeId, Vec<NodeId>>(4).unwrap();
    header.work(16).unwrap();
    let header = header.facts();
    for axis in 0..3 {
        for cause in CAUSES {
            let control = Control::at(3, cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let original = "x".repeat(255);
            let copied = copy_string::<ControlResourceError>(&original, &mut work).unwrap();
            assert_eq!(copied.as_bytes(), [b'x'; 255]);
            assert_eq!(control.trace(), [0, 0, 1]);
            let mut counter = ControlResourceCounter::default();
            let result = visit_node_graph_child_first_in::<ControlResourceError>(
                &fragment,
                &mut counter,
                &mut |facts| {
                    let (value, limit) = match axis {
                        0 => (
                            facts.allocation_requests_upper_bound,
                            header.allocation_requests_upper_bound - 1,
                        ),
                        1 => (
                            facts.allocation_request_bytes_upper_bound,
                            header.allocation_request_bytes_upper_bound - 1,
                        ),
                        _ => (
                            facts.cumulative_work_upper_bound,
                            header.cumulative_work_upper_bound - 1,
                        ),
                    };
                    if value > limit {
                        return Err(CompileControlError::ResourceExhausted);
                    }
                    Ok(())
                },
                &mut work,
                |_, work| {
                    work.step()?;
                    Ok(true)
                },
            );
            assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
            assert_eq!(control.trace(), [0, 0, 1]);
        }
    }
    // Node0's one raw input contributes its own dedup tree before the first
    // contains_key/insert observation. This does not preload caller work.
    for cause in CAUSES {
        let control = Control::at(1, cause);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut counter = ControlResourceCounter::default();
        let mut hooks = 0;
        let result = visit_node_graph_child_first_in::<ControlResourceError>(
            &fragment,
            &mut counter,
            &mut |facts| {
                hooks += 1;
                if hooks == 2 {
                    assert!(
                        facts.allocation_request_bytes_upper_bound
                            > header.allocation_request_bytes_upper_bound
                    );
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            },
            &mut work,
            |_, work| {
                work.step()?;
                Ok(true)
            },
        );
        assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
        assert_eq!(control.trace(), [0]);
    }
}

#[test]
fn borrowed_graph_every_actual_small_callback_preserves_primary_causes_for_success_stop_and_errors()
{
    let fragment = diamond();
    prefixes(|control| borrowed_events(&fragment, control, None));
    prefixes(|control| borrowed_events(&fragment, control, Some(0)));
    prefixes(|control| borrowed_errors(&fragment, control));
    let mut cycle = fragment.into_parts();
    cycle.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(7)]);
    cycle.nodes.get_mut(&NodeId::new(7)).unwrap().inputs = Box::from([NodeId::new(0)]);
    let cycle = Fragment::from(cycle);
    assert!(
        !borrowed_errors(&cycle, &Control::default())
            .unwrap()
            .is_empty()
    );
    prefixes(|control| borrowed_errors(&cycle, control));
}

#[test]
fn borrowed_graph_actual_320_definitions_keep_source_order_and_all_growth_on_one_caller() {
    let fragment = wide();
    assert_eq!(fragment.nodes().len(), 320);
    let control = Control::default();
    let actual = borrowed_events(&fragment, &control, None).unwrap();
    assert_eq!(actual, plain_events(&fragment));
    assert_eq!(actual.0, Some(320));
    let ready = actual
        .1
        .iter()
        .filter_map(|event| match event {
            NodeGraphEvent::Ready(id) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(ready[0], NodeId::new(u32::MAX));
    assert_eq!(ready[319], NodeId::new(318));
    let trace = control.trace();
    // Library flushes may keep this trace below a full256 quantum. These are
    // actual beginning/interior/tail callbacks, not invented per-node probes.
    for at in [0, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control::at(at, cause.clone());
            assert_eq!(
                borrowed_events(&fragment, &control, None),
                Err(cause.into())
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
