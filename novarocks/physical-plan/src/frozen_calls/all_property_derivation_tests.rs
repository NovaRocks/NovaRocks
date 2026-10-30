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

const SOURCE: usize = 4 * 1024 * 1024;
const PROJECTION: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct DeriveControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for DeriveControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the original derivation refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
pub(super) fn frozen_fixture(fragment: Fragment) -> Fixture {
    let uses = leaf_roots(&fragment);
    let calls = uses
        .flow()
        .uses()
        .values()
        .map(|invocation| FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(invocation.context.use_id),
            context: invocation.context,
            effects: effects(FunctionKind::Scalar, invocation.context.domain),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        })
        .collect();
    Fixture {
        fragment,
        uses,
        calls,
    }
}
pub(super) fn replace_properties(
    fixture: &mut Fixture,
    change: impl FnOnce(&mut crate::plan::FragmentParts),
) {
    let mut parts = fixture.fragment.clone().into_parts();
    change(&mut parts);
    fixture.fragment = Fragment::from(parts);
    fixture.uses = leaf_roots(&fixture.fragment);
}
fn broadcast() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    }
}
fn unconstrained(multiplicity: RowMultiplicity) -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: multiplicity,
        ordering: Box::default(),
    }
}
pub(super) fn unsafe_filter() -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let source = NodeId::new(u32::MAX);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let filter = NodeId::new(0);
    let request = zero_argument_request();
    let predicate = builder
        .add_expression(
            filter,
            boolean(),
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        )
        .unwrap();
    // One definition is actually invoked twice, under sparse independent uses.
    builder
        .add_filter(filter, source, Box::from([predicate, predicate]))
        .unwrap();
    let project = NodeId::new(7);
    builder
        .add_project(project, filter, Box::default(), Box::default())
        .unwrap();
    let repeat = NodeId::new(1);
    builder
        .add_repeat(
            repeat,
            project,
            Box::default(),
            Box::from([Box::default()]),
            Box::default(),
            Box::default(),
            Box::default(),
        )
        .unwrap();
    let mut fixture = frozen_fixture(
        builder
            .finish_definition(
                repeat,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap()
            .with_call_requests_observed(
                vec![(PhysicalCallDefinition::Expression(predicate), request)],
                &Control::default(),
            )
            .unwrap(),
    );
    replace_properties(&mut fixture, |parts| {
        for node in parts.nodes.values_mut() {
            node.output_properties = broadcast();
            for required in &mut node.required_inputs {
                *required = unconstrained(RowMultiplicity::Replicated);
            }
        }
    });
    // Keep old binding metadata immutable: the exact second occurrence refutes it.
    assert_eq!(fixture.calls.len(), 2);
    fixture.calls[1].effects.value_stability = FunctionVolatility::Volatile;
    fixture
}
fn run(
    fixture: &Fixture,
    cuts: &FragmentCuts,
    calls: &FrozenFragmentCalls,
    control: &DeriveControl,
) -> Result<
    (
        BTreeMap<NodeId, PhysicalProperties>,
        PropertyProofProjectionFacts,
    ),
    FragmentPropertyError,
> {
    derive_fragment_output_properties_observed(
        &fixture.fragment,
        cuts,
        &fixture.uses,
        calls,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        control,
    )
}
fn prefixes(
    call: impl Fn(
        &DeriveControl,
    ) -> Result<
        (
            BTreeMap<NodeId, PhysicalProperties>,
            PropertyProofProjectionFacts,
        ),
        FragmentPropertyError,
    >,
    succeeds: bool,
    all: bool,
) {
    let baseline = DeriveControl::default();
    assert_eq!(call(&baseline).is_ok(), succeeds);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    assert!(trace.len() >= 2);
    for (at, units) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = DeriveControl {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FragmentPropertyError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn all_derivation_unsafe_sparse_occurrence_propagates_through_project_repeat_without_mutation() {
    let fixture = unsafe_filter();
    let calls = fixture.checked().unwrap();
    assert_eq!(fixture.calls[0].context.use_id, ExpressionUseId::new(0));
    assert_eq!(
        fixture.calls[1].context.use_id,
        ExpressionUseId::new(u32::MAX)
    );
    let before = fixture
        .fragment
        .nodes()
        .iter()
        .map(|(id, node)| (*id, node.output_properties.clone()))
        .collect::<BTreeMap<_, _>>();
    let (candidate, facts) = run(
        &fixture,
        &FragmentCuts::default(),
        &calls,
        &DeriveControl::default(),
    )
    .unwrap();
    assert_eq!(candidate.len(), 4);
    assert_eq!(candidate[&NodeId::new(u32::MAX)], broadcast());
    for id in [0, 7, 1] {
        assert_eq!(
            candidate[&NodeId::new(id)],
            unconstrained(RowMultiplicity::Replicated)
        );
    }
    assert!(facts.request_bytes > 0);
    for (id, original) in before {
        assert_eq!(fixture.fragment.nodes()[&id].output_properties, original);
    }
    assert_eq!(
        calls.entries()[&fixture.calls[1].site]
            .effects
            .value_stability,
        FunctionVolatility::Volatile
    );
    prefixes(
        |control| run(&fixture, &FragmentCuts::default(), &calls, control),
        true,
        true,
    );
}

#[test]
fn all_derivation_required_broadcast_consumer_refuses_candidate_without_old_fallback() {
    let mut fixture = unsafe_filter();
    replace_properties(&mut fixture, |parts| {
        parts
            .nodes
            .get_mut(&NodeId::new(7))
            .unwrap()
            .required_inputs[0] = broadcast();
    });
    let calls = fixture.checked().unwrap();
    assert!(matches!(
        run(
            &fixture,
            &FragmentCuts::default(),
            &calls,
            &DeriveControl::default()
        ),
        Err(FragmentPropertyError::Structure(_))
    ));
    assert_eq!(
        fixture.fragment.nodes()[&NodeId::new(0)].output_properties,
        broadcast()
    );
    prefixes(
        |control| run(&fixture, &FragmentCuts::default(), &calls, control),
        false,
        true,
    );
}

pub(super) fn single_copy_dag() -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(41));
    let source = NodeId::new(u32::MAX);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in [0, 7] {
        builder
            .add_project(NodeId::new(id), source, Box::default(), Box::default())
            .unwrap();
    }
    let union = NodeId::new(1);
    builder
        .add_row_consuming(
            union,
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
    let join = NodeId::new(2);
    let mut requests = Vec::new();
    let mut key = || {
        let request = zero_argument_request();
        let expression = builder
            .add_expression(
                join,
                integer(),
                ExprKind::FunctionCall {
                    function: function(FunctionKind::Scalar, integer()),
                    args: Box::default(),
                },
            )
            .unwrap();
        requests.push((PhysicalCallDefinition::Expression(expression), request));
        expression
    };
    let left = key();
    let right = key();
    builder
        .add_join(
            join,
            [union, source],
            Box::from([properties(), properties()]),
            Box::default(),
            Distribution::Singleton,
            NodeKind::HashJoin {
                kind: JoinKind::Inner,
                condition: crate::HashJoinCondition::Ordinary {
                    keys: Box::from([JoinKey {
                        left,
                        right,
                        null_safe: false,
                    }]),
                    residual: None,
                },
                build_side: JoinSide::Right,
                distribution: JoinDistribution::Singleton,
                null_extended: Box::default(),
            },
        )
        .unwrap();
    let mut fixture = frozen_fixture(
        builder
            .finish_definition(
                join,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap()
            .with_call_requests_observed(requests, &Control::default())
            .unwrap(),
    );
    replace_properties(&mut fixture, |parts| {
        // These are admitted coarse declarations. Derivation must use the stronger
        // actual child candidate, not these stale output declarations.
        for id in [0, 7] {
            parts
                .nodes
                .get_mut(&NodeId::new(id))
                .unwrap()
                .output_properties = unconstrained(RowMultiplicity::SingleCopy);
        }
        for required in &mut parts.nodes.get_mut(&union).unwrap().required_inputs {
            *required = unconstrained(RowMultiplicity::SingleCopy);
        }
    });
    fixture
}

#[test]
fn all_derivation_child_first_shared_union_hash_join_uses_stronger_actual_candidates() {
    let fixture = single_copy_dag();
    let calls = fixture.checked().unwrap();
    let (candidate, _) = run(
        &fixture,
        &FragmentCuts::default(),
        &calls,
        &DeriveControl::default(),
    )
    .unwrap();
    assert_eq!(candidate.len(), 5);
    for id in [u32::MAX, 0, 7, 1, 2] {
        assert_eq!(candidate[&NodeId::new(id)], properties());
    }
    assert_eq!(
        fixture.fragment.nodes()[&NodeId::new(0)].output_properties,
        unconstrained(RowMultiplicity::SingleCopy)
    );
    prefixes(
        |control| run(&fixture, &FragmentCuts::default(), &calls, control),
        true,
        true,
    );
}

#[test]
fn all_derivation_values_anchor_effect_authority_and_foreign_proof_stay_exact() {
    let mut fixture = scalar_fixture(2, 53);
    replace_properties(&mut fixture, |parts| {
        parts.nodes.get_mut(&parts.root).unwrap().output_properties = broadcast();
        let definitions = parts
            .expressions
            .iter()
            .map(|(_, expr)| expr.clone())
            .collect::<Vec<_>>();
        for mut expr in definitions {
            let ExprKind::FunctionCall { function, .. } = &mut expr.kind else {
                unreachable!()
            };
            function.legacy_metadata.as_mut().unwrap().volatility = FunctionVolatility::Volatile;
            parts.expressions.insert(expr);
        }
    });
    for call in &mut fixture.calls {
        call.effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    }
    let calls = fixture.checked().unwrap();
    let (candidate, _) = run(
        &fixture,
        &FragmentCuts::default(),
        &calls,
        &DeriveControl::default(),
    )
    .unwrap();
    assert_eq!(candidate[&fixture.fragment.root()], broadcast());
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
        broadcast()
    );
    let control = DeriveControl::default();
    let proof = calls
        .property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            &control,
        )
        .unwrap();
    let foreign = fixture.fragment.clone();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert_eq!(
        proof.require_fragment(&foreign, &mut work),
        Err(FrozenCallError::WrongFragment)
    );
    work.finish().unwrap();
}

pub(super) fn exchange() -> (Fixture, FragmentCuts) {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let node = NodeId::new(u32::MAX);
    let edge = EdgeId::new(u32::MAX);
    builder
        .add_exchange_source(
            node,
            edge,
            Box::default(),
            Box::default(),
            Distribution::Singleton,
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let fixture = frozen_fixture(
        builder
            .finish_definition(
                node,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );
    let cuts = FragmentCuts {
        inbound: Box::from([InboundFragmentCut {
            edge,
            kind: EdgeKind::Stream,
            source_fragment: FragmentId::new(72),
            destination_node: node,
            imports: Box::default(),
            partitioning: EdgePartitioning {
                source: Distribution::RoundRobin,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::RoundRobin,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
            source_port: crate::FragmentOutputPort::Root,
            delivery: crate::DeliveryRequirement::DemandDriven,
            payload: crate::ExchangePayloadContract::Relational,
        }]),
        outbound: Box::default(),
        runtime_filters: Box::default(),
        runtime_filter_bindings: Box::default(),
    };
    (fixture, cuts)
}

#[test]
fn all_derivation_exchange_uses_exact_inbound_cut_and_missing_cut_never_falls_back() {
    let (fixture, cuts) = exchange();
    let calls = fixture.checked().unwrap();
    let (candidate, _) = run(&fixture, &cuts, &calls, &DeriveControl::default()).unwrap();
    assert_eq!(
        candidate[&fixture.fragment.root()],
        PhysicalProperties {
            distribution: Distribution::RoundRobin,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }
    );
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
        properties()
    );
    assert!(matches!(
        run(
            &fixture,
            &FragmentCuts::default(),
            &calls,
            &DeriveControl::default()
        ),
        Err(FragmentPropertyError::Structure(_))
    ));
    prefixes(|control| run(&fixture, &cuts, &calls, control), true, true);
    prefixes(
        |control| run(&fixture, &FragmentCuts::default(), &calls, control),
        false,
        true,
    );
}

#[test]
fn all_derivation_missing_child_cycle_and_unreachable_are_not_partial_candidates() {
    for mode in 0..3 {
        let mut fixture = unsafe_filter();
        replace_properties(&mut fixture, |parts| match mode {
            0 => {
                parts.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(321)])
            }
            1 => parts.nodes.get_mut(&NodeId::new(0)).unwrap().inputs = Box::from([NodeId::new(1)]),
            2 => {
                parts.root = NodeId::new(7);
            }
            _ => unreachable!(),
        });
        let calls = fixture.checked().unwrap();
        assert!(matches!(
            run(
                &fixture,
                &FragmentCuts::default(),
                &calls,
                &DeriveControl::default()
            ),
            Err(FragmentPropertyError::Structure(_))
        ));
        prefixes(
            |control| run(&fixture, &FragmentCuts::default(), &calls, control),
            false,
            true,
        );
    }
}

#[test]
fn all_derivation_wide_real_project_graph_quantum_uses_same_immutable_source() {
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let mut child = NodeId::new(u32::MAX);
    builder
        .add_values(child, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in 0..319 {
        let node = NodeId::new(id);
        builder
            .add_project(node, child, Box::default(), Box::default())
            .unwrap();
        child = node;
    }
    let mut fixture = frozen_fixture(
        builder
            .finish_definition(
                child,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );
    replace_properties(&mut fixture, |parts| {
        for node in parts.nodes.values_mut() {
            if !node.inputs.is_empty() {
                node.output_properties = unconstrained(RowMultiplicity::SingleCopy);
                node.required_inputs[0] = unconstrained(RowMultiplicity::SingleCopy);
            }
        }
    });
    let calls = fixture.checked().unwrap();
    let control = DeriveControl::default();
    let (candidate, _) = run(&fixture, &FragmentCuts::default(), &calls, &control).unwrap();
    assert_eq!(candidate.len(), 320);
    assert!(
        candidate
            .values()
            .all(|candidate| *candidate == properties())
    );
    assert!(control.trace.lock().unwrap().contains(&256));
    prefixes(
        |control| run(&fixture, &FragmentCuts::default(), &calls, control),
        true,
        false,
    );
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
        unconstrained(RowMultiplicity::SingleCopy)
    );
}
