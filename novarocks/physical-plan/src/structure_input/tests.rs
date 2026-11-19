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
#[path = "owned_tests.rs"]
mod owned_tests;
use crate::{
    Distribution, NodeKind, OutputPort, PhysicalProperties, RowMultiplicity,
    ValidationErrorCategory, ValueOrigin, ValueType,
};
use arrow_schema::DataType;
use novarocks_type_contract::CompileControlError;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn input(
    count: usize,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> FragmentStructureInput {
    let ids = (0..count)
        .map(|i| if i + 1 == count { u32::MAX } else { i as u32 })
        .collect::<Vec<_>>();
    let mut unit = novarocks_expression_contract::BoundExprSetBuilder::new(
        novarocks_expression_contract::UnitLimits {
            expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
        },
    );
    let mut roots = Vec::new();
    for id in &ids {
        let definition = unit
            .add(
                None,
                ValueType::new(DataType::Int64, false),
                fixture_constants.i64(i64::from(*id), &ValueType::new(DataType::Int64, false)),
            )
            .unwrap();
        roots.push(
            unit.root(
                definition,
                novarocks_type_contract::EvaluationDemand::Value,
                novarocks_expression_contract::RootScope::AllPorts,
            )
            .unwrap(),
        );
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = unit
        .seal(vec![], &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    let values = ids
        .iter()
        .enumerate()
        .map(|(ordinal, id)| {
            (
                ValueId::new(*id),
                ValueDef {
                    id: ValueId::new(*id),
                    ty: ValueType::new(DataType::Int64, false),
                    origin: ValueOrigin::NodeOutput {
                        node: NodeId::new(0),
                        output_ordinal: ordinal as u32,
                    },
                },
            )
        })
        .collect();
    let columns = ids.iter().map(|id| ValueId::new(*id)).collect::<Box<_>>();
    let rows = Box::new([roots.into_boxed_slice()]);
    let source = PhysicalNode {
        expressions: Some(std::sync::Arc::new(unit)),
        id: NodeId::new(0),
        inputs: Box::default(),
        required_inputs: Box::default(),
        output_properties: properties(),
        output: OutputPort {
            node: NodeId::new(0),
            columns: columns.clone(),
        },
        kind: NodeKind::Values { rows },
    };
    let root = PhysicalNode {
        expressions: None,
        id: NodeId::new(u32::MAX),
        inputs: Box::from([NodeId::new(0)]),
        required_inputs: Box::from([properties()]),
        output_properties: properties(),
        output: OutputPort {
            node: NodeId::new(u32::MAX),
            columns,
        },
        kind: NodeKind::Limit {
            limit: Some(2),
            offset: 0,
        },
    };
    FragmentStructureInput {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: FragmentId::new(u32::MAX),
        root: root.id,
        values,
        nodes: BTreeMap::from([(source.id, source), (root.id, root)]),
        sink: FragmentSink::Noop,
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 4,
            requires_power_of_two: true,
        },
        runtime_filters: Box::from([RuntimeFilterId::new(u32::MAX), RuntimeFilterId::new(0)]),
    }
}
fn construct(
    input: FragmentStructureInput,
    limits: PlanLimits,
    control: &Control,
) -> Result<Fragment, FragmentStructureError> {
    Fragment::try_from_structure_observed(input, limits, control)
}
#[test]
fn receiving_structure_preserves_zero_max_namespaces_and_original_ordered_fields() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let source = input(2, &mut fixture_constants);
    let expected = source.clone();
    let result = construct(source, PlanLimits::FROZEN, &Control::default()).unwrap();
    assert_eq!(result.id(), FragmentId::new(u32::MAX));
    assert_eq!(result.root(), NodeId::new(u32::MAX));
    assert_eq!(result.values(), &expected.values);
    assert_eq!(result.nodes(), &expected.nodes);
    assert_eq!(
        result.unit(NodeId::new(0)),
        expected.nodes[&NodeId::new(0)].expressions.as_deref()
    );
    assert_eq!(result.sink(), &expected.sink);
    assert_eq!(result.dop_domain(), expected.dop_domain);
    assert_eq!(result.runtime_filters(), &*expected.runtime_filters);
    assert_eq!(
        result
            .values()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(result.call_requests().fragment(), result.id());
    // This unpublished structural stage does not fabricate call requests,
    // frozen effects, property proof or a final Package certificate. Supplied
    // immutable units keep their already-authored roots without rebuilding them.
    assert!(result.call_requests().entries().is_empty());

    assert_eq!(
        result
            .unit(NodeId::new(0))
            .unwrap()
            .definitions()
            .iter()
            .map(|node| node.id.get())
            .collect::<Vec<_>>(),
        [0, 1]
    );
}
#[test]
fn receiving_structure_rejects_key_drift_and_keeps_original_count_graph_and_dop_gates() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let exact = PlanLimits {
        fragment_nodes: 2,
        fragment_values: 2,
        fragment_expressions: 2,
        plan_runtime_filters: 2,
        ..PlanLimits::FROZEN
    };
    construct(input(2, &mut fixture_constants), exact, &Control::default()).unwrap();
    for axis in 0..4 {
        let mut l = exact;
        match axis {
            0 => l.fragment_nodes = 1,
            1 => l.fragment_values = 1,
            2 => l.fragment_expressions = 1,
            3 => l.plan_runtime_filters = 1,
            _ => unreachable!(),
        }
        let Err(FragmentStructureError::Structure(errors)) =
            construct(input(2, &mut fixture_constants), l, &Control::default())
        else {
            panic!("axis {axis}")
        };
        assert!(
            errors
                .errors()
                .iter()
                .any(|e| e.category() == ValidationErrorCategory::ResourceLimit)
        );
    }
    for fault in 0..5 {
        let mut s = input(2, &mut fixture_constants);
        match fault {
            0 => s.values.get_mut(&ValueId::new(0)).unwrap().id = ValueId::new(7),
            1 => s.nodes.get_mut(&NodeId::new(0)).unwrap().id = NodeId::new(7),
            2 => s.root = NodeId::new(7),
            3 => s.dop_domain.min = 0,
            4 => {
                s.nodes.get_mut(&NodeId::new(u32::MAX)).unwrap().inputs =
                    Box::from([NodeId::new(7)])
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(
                construct(s, exact, &Control::default()),
                Err(FragmentStructureError::Structure(_))
            ),
            "fault {fault}"
        );
    }
}
fn prefixes(source: &FragmentStructureInput, success: bool, wide: bool) {
    let c = Control::default();
    assert_eq!(
        construct(source.clone(), PlanLimits::FROZEN, &c).is_ok(),
        success
    );
    let events = c.trace.lock().unwrap().clone();
    assert!(!events.is_empty());
    if wide {
        assert!(events.iter().any(|(_, units)| *units == 256));
    }
    for (at, (_, units)) in events.iter().enumerate() {
        if wide && at != 0 && at + 1 != events.len() && *units != 256 {
            continue;
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            // Fixture clones are made before the product entry and are not
            // credited as observed receiving work or allocation evidence.
            let owned = source.clone();
            let c = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(construct(owned,PlanLimits::FROZEN,&c),Err(FragmentStructureError::Control(actual))if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), events[..=at]);
        }
    }
}
#[test]
fn receiving_structure_observes_each_small_success_and_ordinary_failure_prefix() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let s = input(2, &mut fixture_constants);
    prefixes(&s, true, false);
    let mut malformed = s.clone();
    malformed.root = NodeId::new(7);
    prefixes(&malformed, false, false);
    let mut key_drift = s;
    key_drift.values.get_mut(&ValueId::new(0)).unwrap().id = ValueId::new(7);
    prefixes(&key_drift, false, false);
}
#[test]
fn receiving_structure_wide_quantum_comes_from_actual_sparse_definition_checks() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    prefixes(&input(320, &mut fixture_constants), true, true);
}
