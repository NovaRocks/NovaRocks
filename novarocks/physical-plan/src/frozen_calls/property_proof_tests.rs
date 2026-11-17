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

use super::super::property_proof::{PropertyProofProjectionLimits, projection_facts, source_floor};
use super::*;

const SOURCE_INVOICE: usize = 4 * 1024 * 1024;
const PROJECTION_LIMITS: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};

#[derive(Default)]
struct ProofControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for ProofControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn safe(
    calls: &FrozenFragmentCalls,
    fixture: &Fixture,
    node: NodeId,
    control: &ProofControl,
) -> Result<bool, FrozenCallError> {
    let proof = calls.property_proof(
        &fixture.fragment,
        &fixture.uses,
        &PlanLimits::FROZEN,
        SOURCE_INVOICE,
        PROJECTION_LIMITS,
        control,
    )?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        proof.require_fragment(&fixture.fragment, &mut work)?;
        proof.replica_safe(node, &mut work)
    })();
    finish_frozen_calls(work, result)
}
fn prefixes(run: impl Fn(&ProofControl) -> Result<(), FrozenCallError>, succeeds: bool) {
    let original = ProofControl::default();
    assert_eq!(run(&original).is_ok(), succeeds);
    let trace = original.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for expected in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = ProofControl {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, expected.clone())),
            };
            assert!(
                matches!(run(&control), Err(FrozenCallError::Control(actual)) if actual == expected)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn occurrence_property_proof_preserves_each_shared_use_and_guarded_domain() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut fixture_constants);
    assert_eq!(
        fixture.uses.flow().uses()[&ExpressionUseId::new(0)].definition,
        fixture.uses.flow().uses()[&ExpressionUseId::new(u32::MAX)].definition
    );
    assert_ne!(
        fixture.calls[0].context.region,
        fixture.calls[1].context.region
    );
    assert!(
        safe(
            &fixture.checked().unwrap(),
            &fixture,
            fixture.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    for unsafe_at in 0..2 {
        fixture.calls[unsafe_at]
            .effects
            .observable_effects
            .rng_sampling = true;
        assert!(
            !safe(
                &fixture.checked().unwrap(),
                &fixture,
                fixture.fragment.root(),
                &ProofControl::default()
            )
            .unwrap()
        );
        fixture.calls[unsafe_at].effects.observable_effects = ObservableEffects::NONE;
    }
    let mut guarded = case_fixture(&mut fixture_constants);
    assert!(guarded.calls.iter().any(|call| {
        guarded.uses.flow().regions()[&call.context.region]
            .guard
            .is_some()
    }));
    guarded.calls[1].effects.observable_effects.warnings = true;
    assert!(
        !safe(
            &guarded.checked().unwrap(),
            &guarded,
            guarded.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
}

#[test]
fn occurrence_property_proof_keeps_row_errors_and_relational_lifecycle() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut scalar = scalar_fixture(1, u32::MAX, &mut fixture_constants);
    scalar.calls[0].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    assert!(
        safe(
            &scalar.checked().unwrap(),
            &scalar,
            scalar.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    scalar.calls[0].effects.instance_state = FunctionInstanceState::ScalarInstance;
    assert!(
        !safe(
            &scalar.checked().unwrap(),
            &scalar,
            scalar.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    let mut relational = special_fixture(&mut fixture_constants);
    let calls = relational.checked().unwrap();
    for node in relational.fragment.nodes().keys() {
        assert!(safe(&calls, &relational, *node, &ProofControl::default()).unwrap());
    }
    let table = relational
        .calls
        .iter()
        .position(|call| matches!(call.site, PhysicalCallSite::Table { .. }))
        .unwrap();
    let PhysicalCallSite::Table { node } = relational.calls[table].site else {
        unreachable!()
    };
    relational.calls[table].effects.observable_effects.warnings = true;
    let calls = relational.checked().unwrap();
    assert!(!safe(&calls, &relational, node, &ProofControl::default()).unwrap());
    for other in relational
        .fragment
        .nodes()
        .keys()
        .filter(|other| **other != node)
    {
        assert!(safe(&calls, &relational, *other, &ProofControl::default()).unwrap());
    }
}

#[test]
fn occurrence_property_proof_sparse_node_index_refuses_foreign_snapshot_and_missing_node() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, u32::MAX, &mut fixture_constants);
    // Move the one node and its original definitions, retaining their facts.
    // Sparse MAX is a map key; it never becomes a vector capacity.
    let mut parts = fixture.fragment.into_parts();
    let old = parts.root;
    let mut node = parts.nodes.remove(&old).unwrap();
    node.id = NodeId::new(u32::MAX);
    node.output.node = node.id;
    parts.root = node.id;
    for value in parts.values.values_mut() {
        match &mut value.origin {
            ValueOrigin::NodeOutput { node: owner, .. } | ValueOrigin::Expr { node: owner, .. } => {
                *owner = node.id
            }
            _ => unreachable!(),
        }
    }
    let definitions = parts
        .expressions
        .iter()
        .map(|(_, definition)| definition.clone())
        .collect::<Vec<_>>();
    for mut definition in definitions {
        definition.owner = node.id;
        parts.expressions.insert(definition);
    }
    parts.nodes.insert(node.id, node);
    fixture.fragment = Fragment::from(parts);
    fixture.uses = leaf_roots(&fixture.fragment);
    let calls = fixture.checked().unwrap();
    let proof = calls
        .property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default(),
        )
        .unwrap();
    let control = ProofControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(
        proof
            .replica_safe(NodeId::new(u32::MAX), &mut work)
            .unwrap()
    );
    assert_eq!(
        proof.replica_safe(NodeId::new(0), &mut work),
        Err(FrozenCallError::InvalidSite)
    );
    assert_eq!(
        proof.require_fragment(&fixture.fragment.clone(), &mut work),
        Err(FrozenCallError::WrongFragment)
    );
    work.finish().unwrap();
}

#[test]
fn occurrence_property_proof_bounds_source_before_index_and_rechecks_call_correspondence() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 71, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    for limits in [
        PlanLimits {
            fragment_nodes: 0,
            ..PlanLimits::FROZEN
        },
        PlanLimits {
            fragment_values: 0,
            ..PlanLimits::FROZEN
        },
        PlanLimits {
            fragment_expressions: 0,
            ..PlanLimits::FROZEN
        },
    ] {
        prefixes(
            |control| {
                calls
                    .property_proof(
                        &fixture.fragment,
                        &fixture.uses,
                        &limits,
                        SOURCE_INVOICE,
                        PROJECTION_LIMITS,
                        control,
                    )
                    .map(|_| ())
            },
            false,
        );
        assert!(matches!(
            calls.property_proof(
                &fixture.fragment,
                &fixture.uses,
                &limits,
                SOURCE_INVOICE,
                PROJECTION_LIMITS,
                &ProofControl::default()
            ),
            Err(FrozenCallError::TooManyItems)
        ));
    }
    let mut missing = calls.clone();
    Arc::make_mut(&mut missing.entries).remove(&fixture.calls[1].site);
    assert!(matches!(
        missing.property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default()
        ),
        Err(FrozenCallError::MissingSite(_))
    ));
    prefixes(
        |control| {
            missing
                .property_proof(
                    &fixture.fragment,
                    &fixture.uses,
                    &PlanLimits::FROZEN,
                    SOURCE_INVOICE,
                    PROJECTION_LIMITS,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    let mut changed = calls.clone();
    Arc::make_mut(&mut changed.entries)
        .get_mut(&fixture.calls[0].site)
        .unwrap()
        .context
        .region = EvaluationRegionId::new(99);
    assert!(matches!(
        changed.property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default()
        ),
        Err(FrozenCallError::WrongContext)
    ));
}

#[test]
fn occurrence_property_proof_actual_quantum_and_every_callback_keep_original_causes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(320, 17, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    let control = ProofControl::default();
    assert!(safe(&calls, &fixture, fixture.fragment.root(), &control).unwrap());
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(
        |control| safe(&calls, &fixture, fixture.fragment.root(), control).map(|_| ()),
        true,
    );
    prefixes(
        |control| safe(&calls, &fixture, NodeId::new(u32::MAX), control).map(|_| ()),
        false,
    );
}

#[test]
fn occurrence_property_proof_exact_projection_envelopes_and_overflow_are_checked() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(320, 17, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    let measured = calls
        .property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default(),
        )
        .unwrap()
        .facts();
    let exact = PropertyProofProjectionLimits {
        max_request_bytes: measured.request_bytes,
        max_coexisting_bytes: measured.coexisting_bytes,
        max_projection_work: measured.projection_work,
    };
    prefixes(
        |control| {
            calls
                .property_proof(
                    &fixture.fragment,
                    &fixture.uses,
                    &PlanLimits::FROZEN,
                    SOURCE_INVOICE,
                    exact,
                    control,
                )
                .map(|_| ())
        },
        true,
    );
    for under in [
        PropertyProofProjectionLimits {
            max_request_bytes: exact.max_request_bytes - 1,
            ..exact
        },
        PropertyProofProjectionLimits {
            max_coexisting_bytes: exact.max_coexisting_bytes - 1,
            ..exact
        },
        PropertyProofProjectionLimits {
            max_projection_work: exact.max_projection_work - 1,
            ..exact
        },
    ] {
        prefixes(
            |control| {
                calls
                    .property_proof(
                        &fixture.fragment,
                        &fixture.uses,
                        &PlanLimits::FROZEN,
                        SOURCE_INVOICE,
                        under,
                        control,
                    )
                    .map(|_| ())
            },
            false,
        );
        assert!(matches!(
            calls.property_proof(
                &fixture.fragment,
                &fixture.uses,
                &PlanLimits::FROZEN,
                SOURCE_INVOICE,
                under,
                &ProofControl::default()
            ),
            Err(FrozenCallError::TooManyItems)
        ));
    }
    let floor = source_floor(&fixture.fragment, &fixture.uses, &calls).unwrap();
    assert!(floor > 0 && floor < SOURCE_INVOICE);
    prefixes(
        |control| {
            calls
                .property_proof(
                    &fixture.fragment,
                    &fixture.uses,
                    &PlanLimits::FROZEN,
                    floor - 1,
                    exact,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    for (nodes, uses, calls, source) in [
        (usize::MAX, 0, 0, 0),
        (1, usize::MAX, 0, 0),
        (1, 0, usize::MAX, 0),
        (1, 0, 0, usize::MAX),
    ] {
        assert!(matches!(
            projection_facts(nodes, uses, calls, source),
            Err(FrozenCallError::TooManyItems)
        ));
    }
}

#[test]
fn occurrence_property_proof_broadcast_error_retains_original_first_site() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 73, &mut fixture_constants);
    fixture.calls[0].effects.observable_effects.warnings = true;
    fixture.calls[1].effects.observable_effects.rng_sampling = true;
    let root = fixture.fragment.root();
    let mut parts = fixture.fragment.into_parts();
    parts
        .nodes
        .get_mut(&root)
        .unwrap()
        .output_properties
        .distribution = Distribution::Broadcast;
    fixture.fragment = Fragment::from(parts);
    let calls = fixture.checked().unwrap();
    prefixes(
        |control| {
            let proof = calls.property_proof(
                &fixture.fragment,
                &fixture.uses,
                &PlanLimits::FROZEN,
                SOURCE_INVOICE,
                PROJECTION_LIMITS,
                control,
            )?;
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
            let result = proof.require_declared_broadcast_equivalence(&mut work);
            finish_frozen_calls(work, result)
        },
        false,
    );
    let control = ProofControl::default();
    let proof = calls
        .property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &control,
        )
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = proof.require_declared_broadcast_equivalence(&mut work);
    assert_eq!(
        finish_frozen_calls(work, result),
        Err(FrozenCallError::ReplicaEquivalence(fixture.calls[0].site))
    );
}

#[test]
fn occurrence_property_proof_global_broadcast_fault_follows_visit_order_not_node_order() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = special_fixture(&mut fixture_constants);
    let aggregate_site = fixture.calls[0].site;
    let window_site = fixture.calls[2].site;
    fixture.calls[0].effects.observable_effects.warnings = true;
    fixture.calls[2].effects.observable_effects.warnings = true;
    let PhysicalCallSite::Aggregate {
        node: aggregate, ..
    } = aggregate_site
    else {
        panic!("aggregate fixture")
    };
    let PhysicalCallSite::Expression(window_use) = window_site else {
        panic!("window fixture")
    };
    let window = fixture
        .fragment
        .expressions()
        .get(fixture.uses.flow().uses()[&window_use].definition)
        .unwrap()
        .owner;
    assert!(aggregate < window);
    let mut parts = fixture.fragment.into_parts();
    for node in [aggregate, window] {
        parts
            .nodes
            .get_mut(&node)
            .unwrap()
            .output_properties
            .distribution = Distribution::Broadcast;
    }
    fixture.fragment = Fragment::from(parts);
    let calls = fixture.checked().unwrap();
    let expected = Err(FrozenCallError::ReplicaEquivalence(window_site));
    assert_eq!(
        calls.validate_replica_equivalence(
            &fixture.fragment,
            &fixture.uses,
            &ProofControl::default()
        ),
        expected
    );
    let run = |control: &ProofControl| {
        let proof = calls.property_proof(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            control,
        )?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = proof.require_declared_broadcast_equivalence(&mut work);
        finish_frozen_calls(work, result)
    };
    assert_eq!(run(&ProofControl::default()), expected);
    prefixes(run, false);
}

#[derive(Default)]
struct BorrowedProofControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for BorrowedProofControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram, "unexpected nested scope");
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after borrowed primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn borrowed_proof_run(
    fixture: &Fixture,
    calls: &FrozenFragmentCalls,
    source: usize,
    complete: bool,
    control: &BorrowedProofControl,
    admit: &mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), CompileControlError>,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = if complete {
        crate::validation::validate_fragment_output_properties_in(
            &fixture.fragment,
            &fixture.uses,
            calls,
            PlanLimits::FROZEN,
            source,
            PROJECTION_LIMITS,
            admit,
            &mut work,
        )
    } else {
        calls
            .property_proof_in(
                &fixture.fragment,
                &fixture.uses,
                &PlanLimits::FROZEN,
                source,
                PROJECTION_LIMITS,
                admit,
                &mut work,
            )
            .map(|proof| proof.facts())
            .map_err(FragmentPropertyError::from)
    };
    if matches!(&result, Err(FragmentPropertyError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn borrowed_facts_axes(f: novarocks_type_contract::ControlOwnedResourceFacts) -> [usize; 3] {
    [
        f.allocation_requests_upper_bound,
        f.allocation_request_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    ]
}
fn borrowed_gate(
    facts: &novarocks_type_contract::ControlOwnedResourceFacts,
    maximum: [usize; 3],
) -> Result<(), CompileControlError> {
    if borrowed_facts_axes(*facts)
        .iter()
        .zip(maximum)
        .any(|(value, max)| *value > max)
    {
        return Err(CompileControlError::ResourceExhausted);
    }
    Ok(())
}

#[test]
fn borrowed_property_source_and_complete_formulas_keep_actual_admission_exact_replay() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, u32::MAX, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    for complete in [false, true] {
        let plain = if complete {
            crate::validation::validate_fragment_output_properties_observed(
                &fixture.fragment,
                &fixture.uses,
                &calls,
                PlanLimits::FROZEN,
                SOURCE_INVOICE,
                PROJECTION_LIMITS,
                &ProofControl::default(),
            )
            .unwrap()
        } else {
            calls
                .property_proof(
                    &fixture.fragment,
                    &fixture.uses,
                    &PlanLimits::FROZEN,
                    SOURCE_INVOICE,
                    PROJECTION_LIMITS,
                    &ProofControl::default(),
                )
                .unwrap()
                .facts()
        };
        let control = BorrowedProofControl::default();
        let mut maximum = [0; 3];
        let mut captures = 0;
        let result = borrowed_proof_run(
            &fixture,
            &calls,
            SOURCE_INVOICE,
            complete,
            &control,
            &mut |facts| {
                captures += 1;
                for (index, value) in borrowed_facts_axes(*facts).into_iter().enumerate() {
                    maximum[index] = maximum[index].max(value);
                }
                borrowed_gate(facts, [usize::MAX; 3])
            },
        )
        .unwrap();
        assert!(captures > 0);
        assert!(maximum.iter().all(|value| *value > 0));
        assert_eq!(
            [
                result.request_bytes,
                result.coexisting_bytes,
                result.projection_work
            ],
            [
                plain.request_bytes,
                plain.coexisting_bytes,
                plain.projection_work
            ]
        );
        assert_eq!(
            result.coexisting_bytes,
            SOURCE_INVOICE + result.request_bytes
        );
        let control = BorrowedProofControl::default();
        borrowed_proof_run(
            &fixture,
            &calls,
            SOURCE_INVOICE,
            complete,
            &control,
            &mut |facts| borrowed_gate(facts, maximum),
        )
        .unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = BorrowedProofControl::default();
            let mut at_refusal = None;
            assert!(
                matches!(borrowed_proof_run(&fixture, &calls, SOURCE_INVOICE, complete, &control,
                &mut |_| {
                    assert!(at_refusal.is_none(), "admission after first refusal");
                    at_refusal = Some(control.trace.lock().unwrap().clone());
                    Err(cause.clone())
                }), Err(FragmentPropertyError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), at_refusal.unwrap());
        }
        for axis in 0..3 {
            let mut tight = maximum;
            tight[axis] -= 1;
            let control = BorrowedProofControl::default();
            assert!(matches!(
                borrowed_proof_run(
                    &fixture,
                    &calls,
                    SOURCE_INVOICE,
                    complete,
                    &control,
                    &mut |facts| borrowed_gate(facts, tight)
                ),
                Err(FragmentPropertyError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
        }
    }
}

#[test]
fn borrowed_property_success_and_ordinary_source_error_keep_every_actual_primary_prefix() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 71, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    for complete in [false, true] {
        for source in [SOURCE_INVOICE, 0] {
            let control = BorrowedProofControl::default();
            let baseline =
                borrowed_proof_run(&fixture, &calls, source, complete, &control, &mut |facts| {
                    borrowed_gate(facts, [usize::MAX; 3])
                });
            if source == 0 {
                assert!(matches!(
                    baseline,
                    Err(FragmentPropertyError::Calls(FrozenCallError::TooManyItems))
                ));
            } else {
                baseline.unwrap();
            }
            let trace = control.trace.lock().unwrap().clone();
            assert!(trace.len() > 1);
            for at in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = BorrowedProofControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    assert!(
                        matches!(borrowed_proof_run(&fixture, &calls, source, complete, &control,
                        &mut |facts| borrowed_gate(facts, [usize::MAX; 3])), Err(FragmentPropertyError::Control(actual)) if actual==cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn borrowed_property_empty_calls_and_foreign_source_still_use_original_owners() {
    // The original empty Values fixture has no values, expressions or calls;
    // it has no unreachable definition and still uses real root/flow owners.
    let fixture = empty_fixture();
    let calls = fixture.checked().unwrap();
    assert!(calls.entries().is_empty());
    assert!(fixture.uses.bindings().is_empty());
    for complete in [false, true] {
        borrowed_proof_run(
            &fixture,
            &calls,
            SOURCE_INVOICE,
            complete,
            &BorrowedProofControl::default(),
            &mut |facts| borrowed_gate(facts, [usize::MAX; 3]),
        )
        .unwrap();
    }
    let control = BorrowedProofControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let proof = calls
        .property_proof_in(
            &fixture.fragment,
            &fixture.uses,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &mut |facts| borrowed_gate(facts, [usize::MAX; 3]),
            &mut work,
        )
        .unwrap();
    let other = fixture.fragment.clone();
    assert_eq!(
        proof.require_fragment(&other, &mut work),
        Err(FrozenCallError::WrongFragment)
    );
    work.finish().unwrap();
}
