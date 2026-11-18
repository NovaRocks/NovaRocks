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
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut constants);
    assert_eq!(
        fixture.unit().control_flow().uses()[&ExpressionUseId::new(0)].definition,
        fixture.unit().control_flow().uses()[&ExpressionUseId::new(u32::MAX)].definition
    );
    assert_ne!(
        fixture.claims()[&ExpressionUseId::new(0)].context.region,
        fixture.claims()[&ExpressionUseId::new(u32::MAX)]
            .context
            .region
    );
    assert!(
        safe(
            fixture.checked().unwrap(),
            &fixture,
            fixture.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    for id in [ExpressionUseId::new(0), ExpressionUseId::new(u32::MAX)] {
        fixture.edit_unit(|parts| {
            parts
                .claims
                .get_mut(&id)
                .unwrap()
                .effects
                .observable_effects
                .rng_sampling = true
        });
        assert!(
            !safe(
                fixture.checked().unwrap(),
                &fixture,
                fixture.fragment.root(),
                &ProofControl::default()
            )
            .unwrap()
        );
        fixture.edit_unit(|parts| {
            parts
                .claims
                .get_mut(&id)
                .unwrap()
                .effects
                .observable_effects = ObservableEffects::NONE
        });
    }
    let mut guarded = case_fixture(&mut constants);
    assert!(guarded.claims().values().any(|call| {
        guarded.unit().control_flow().regions()[&call.context.region]
            .guard
            .is_some()
    }));
    guarded.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(u32::MAX))
            .unwrap()
            .effects
            .observable_effects
            .warnings = true
    });
    assert!(
        !safe(
            guarded.checked().unwrap(),
            &guarded,
            guarded.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
}

#[test]
fn occurrence_property_proof_keeps_row_errors_and_relational_lifecycle() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut scalar = scalar_fixture(1, u32::MAX, &mut constants);
    scalar.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .effects
            .own_row_error = FunctionIntrinsicRowError::MayRaise
    });
    assert!(
        safe(
            scalar.checked().unwrap(),
            &scalar,
            scalar.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    scalar.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .effects
            .instance_state = FunctionInstanceState::ScalarInstance
    });
    assert!(
        !safe(
            scalar.checked().unwrap(),
            &scalar,
            scalar.fragment.root(),
            &ProofControl::default()
        )
        .unwrap()
    );
    let mut relational = special_fixture(&mut constants);
    for node in relational.fragment.nodes().keys() {
        assert!(
            safe(
                relational.checked().unwrap(),
                &relational,
                *node,
                &ProofControl::default()
            )
            .unwrap()
        );
    }
    let table = *relational
        .relations()
        .keys()
        .find(|site| matches!(site, PhysicalCallSite::Table { .. }))
        .unwrap();
    let node = table.node();
    relational.fragment = relational
        .candidate_relations(|calls| {
            calls
                .iter_mut()
                .find(|call| call.site == table)
                .unwrap()
                .effects
                .observable_effects
                .warnings = true;
        })
        .unwrap();
    let calls = relational.checked().unwrap();
    assert!(!safe(calls, &relational, node, &ProofControl::default()).unwrap());
    for other in relational
        .fragment
        .nodes()
        .keys()
        .filter(|other| **other != node)
    {
        assert!(safe(calls, &relational, *other, &ProofControl::default()).unwrap());
    }
}

#[test]
fn occurrence_property_proof_sparse_node_index_refuses_foreign_snapshot_and_missing_node() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, u32::MAX, &mut constants);
    let mut parts = fixture.fragment.into_parts();
    let old = parts.root;
    let mut node = parts.nodes.remove(&old).unwrap();
    let unit = node.expressions.as_ref().unwrap().clone();
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
    // Local definitions have no node owner to relabel. Preserve the exact
    // unit Arc and the fragment-owned relation table during this node move.
    assert!(std::sync::Arc::ptr_eq(
        &unit,
        node.expressions.as_ref().unwrap()
    ));
    parts.nodes.insert(node.id, node);
    fixture.fragment = parts.into();
    fixture.primary = NodeId::new(u32::MAX);
    let calls = fixture.checked().unwrap();
    let control = ProofControl::default();
    let proof = calls
        .property_proof(
            &fixture.fragment,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &control,
        )
        .unwrap();
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
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 71, &mut constants);
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
                &limits,
                SOURCE_INVOICE,
                PROJECTION_LIMITS,
                &ProofControl::default()
            ),
            Err(FrozenCallError::TooManyItems)
        ));
    }
    // A detached but equal table cannot override the immutable fragment's
    // actual relation source. Correspondence errors below use owned tables.
    let detached = (*calls).clone();
    assert!(matches!(
        detached.property_proof(
            &fixture.fragment,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default()
        ),
        Err(FrozenCallError::WrongFragment)
    ));
    let mut missing = special_fixture(&mut constants);
    let mut parts = missing.fragment.into_parts();
    let omitted = *parts.calls.entries.keys().next().unwrap();
    std::sync::Arc::make_mut(&mut parts.calls.entries).remove(&omitted);
    missing.fragment = parts.into();
    let calls = missing.fragment.calls();
    assert!(
        matches!(calls.property_proof(&missing.fragment, &PlanLimits::FROZEN, SOURCE_INVOICE, PROJECTION_LIMITS, &ProofControl::default()), Err(FrozenCallError::MissingSite(site)) if site == omitted)
    );
    prefixes(
        |control| {
            calls
                .property_proof(
                    &missing.fragment,
                    &PlanLimits::FROZEN,
                    SOURCE_INVOICE,
                    PROJECTION_LIMITS,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    let mut changed = special_fixture(&mut constants);
    let mut parts = changed.fragment.into_parts();
    std::sync::Arc::make_mut(&mut parts.calls.entries)
        .values_mut()
        .next()
        .unwrap()
        .context
        .demand = EvaluationDemand::TruthOnly;
    changed.fragment = parts.into();
    assert!(matches!(
        changed.fragment.calls().property_proof(
            &changed.fragment,
            &PlanLimits::FROZEN,
            SOURCE_INVOICE,
            PROJECTION_LIMITS,
            &ProofControl::default()
        ),
        Err(FrozenCallError::WrongContext)
    ));
    // Invalid scalar claim/context is refused by the real unit constructor;
    // its unchanged cases are proposed at expression-contract's owner.
}

#[test]
fn occurrence_property_proof_actual_quantum_and_every_callback_keep_original_causes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(320, 17, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    let control = ProofControl::default();
    assert!(safe(calls, &fixture, fixture.fragment.root(), &control).unwrap());
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(
        |control| safe(calls, &fixture, fixture.fragment.root(), control).map(|_| ()),
        true,
    );
    prefixes(
        |control| safe(calls, &fixture, NodeId::new(u32::MAX), control).map(|_| ()),
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
                &PlanLimits::FROZEN,
                SOURCE_INVOICE,
                under,
                &ProofControl::default()
            ),
            Err(FrozenCallError::TooManyItems)
        ));
    }
    let floor = source_floor(&fixture.fragment, calls).unwrap();
    assert!(floor > 0 && floor < SOURCE_INVOICE);
    prefixes(
        |control| {
            calls
                .property_proof(
                    &fixture.fragment,
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
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 73, &mut constants);
    fixture.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .effects
            .observable_effects
            .warnings = true;
        parts
            .claims
            .get_mut(&ExpressionUseId::new(u32::MAX))
            .unwrap()
            .effects
            .observable_effects
            .rng_sampling = true;
    });
    let root = fixture.fragment.root();
    let mut parts = fixture.fragment.into_parts();
    parts
        .nodes
        .get_mut(&root)
        .unwrap()
        .output_properties
        .distribution = Distribution::Broadcast;
    fixture.fragment = parts.into();
    let calls = fixture.checked().unwrap();
    prefixes(
        |control| {
            let proof = calls.property_proof(
                &fixture.fragment,
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
        Err(FrozenCallError::ReplicaExpression {
            node: root,
            invocation: ExpressionUseId::new(0)
        })
    );
}

#[test]
fn occurrence_property_proof_global_broadcast_fault_follows_visit_order_not_node_order() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = special_fixture(&mut constants);
    let aggregate_site = *fixture
        .relations()
        .keys()
        .find(|site| matches!(site, PhysicalCallSite::Aggregate { call: 0, .. }))
        .unwrap();
    fixture.fragment = fixture
        .candidate_relations(|calls| {
            calls
                .iter_mut()
                .find(|call| call.site == aggregate_site)
                .unwrap()
                .effects
                .observable_effects
                .warnings = true;
        })
        .unwrap();
    // Window is now relational, so comparing Aggregate versus Window alone
    // cannot distinguish scalar-first visitation from node-index sorting.
    // Add one real scalar Project after the unchanged relation seed instead.
    let scalar = append_late_scalar(&mut fixture);
    let aggregate = aggregate_site.node();
    assert!(aggregate < scalar);
    let mut parts = fixture.fragment.into_parts();
    for node in [aggregate, scalar] {
        parts
            .nodes
            .get_mut(&node)
            .unwrap()
            .output_properties
            .distribution = Distribution::Broadcast;
    }
    fixture.fragment = parts.into();
    let calls = fixture.checked().unwrap();
    let expected = Err(FrozenCallError::ReplicaExpression {
        node: scalar,
        invocation: ExpressionUseId::new(0),
    });
    assert_eq!(
        calls.validate_replica_equivalence(&fixture.fragment, &ProofControl::default()),
        expected
    );
    let run = |control: &ProofControl| {
        let proof = calls.property_proof(
            &fixture.fragment,
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
    source: usize,
    complete: bool,
    control: &BorrowedProofControl,
    admit: &mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), CompileControlError>,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    let calls = fixture.fragment.calls();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = if complete {
        crate::validation::validate_fragment_output_properties_in(
            &fixture.fragment,
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

// The original two Boolean scalar uses remain unit-owned. A real zero-argument
// relation provides the temporary identity-set allocation whose independent
// request-count/byte/work ceilings this borrowed admission test exercises.
fn borrowed_source_with_real_relation(
    constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut fixture = scalar_fixture(2, u32::MAX, constants);
    assert_eq!(fixture.claims().len(), 2);
    assert_eq!(
        fixture.unit().root_use_ids(),
        &[ExpressionUseId::new(0), ExpressionUseId::new(u32::MAX)]
    );
    let original_unit = Arc::clone(
        fixture.fragment.nodes()[&fixture.primary]
            .expressions
            .as_ref()
            .unwrap(),
    );
    let mut parts = fixture.fragment.into_parts();
    let input = parts.root;
    assert_eq!(input, fixture.primary);
    let pass = parts.nodes[&input].output.columns[0];
    let node = NodeId::new(1);
    assert!(!parts.nodes.contains_key(&node));
    let value = ValueId::new(1);
    assert!(!parts.values.contains_key(&value));
    parts.values.insert(
        value,
        ValueDef {
            id: value,
            ty: integer(),
            origin: ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        },
    );
    let selected = function(FunctionKind::Table, integer());
    let required = parts.nodes[&input].output_properties.clone();
    parts.nodes.insert(
        node,
        PhysicalNode {
            id: node,
            expressions: None,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([pass, value]),
            },
            kind: NodeKind::TableFunction {
                function: BoundTableFunction {
                    function_id: selected.function_id,
                    overload: selected.overload,
                    argument_types: selected.argument_types,
                    result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::from([integer()]),
                    ),
                    legacy_metadata: selected.legacy_metadata,
                },
                arguments: Box::default(),
                outputs: Box::from([
                    TableFunctionOutput::PassThrough(pass),
                    TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value,
                    },
                ]),
                left_outer: false,
            },
        },
    );
    parts.root = node;
    let fragment = Fragment::from(parts);
    let site = PhysicalCallSite::Table { node };
    let fragment = fragment
        .with_call_requests_observed(vec![(site, zero_argument_request())], &Control::default())
        .unwrap();
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(17),
    );
    fixture.fragment = fragment
        .with_calls_observed(
            vec![FrozenPhysicalCall {
                site,
                context,
                materialized_writer_input: None,
                effects: declared_effects(
                    FunctionKind::Table,
                    CallProofScope::Relation(context.identity),
                ),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            }],
            &Control::default(),
        )
        .unwrap();
    assert!(Arc::ptr_eq(
        fixture.fragment.nodes()[&fixture.primary]
            .expressions
            .as_ref()
            .unwrap(),
        &original_unit
    ));
    validate_fragment_definition(&fixture.fragment).unwrap();
    fixture
}

#[test]
fn borrowed_property_source_and_complete_formulas_keep_actual_admission_exact_replay() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = borrowed_source_with_real_relation(&mut fixture_constants);
    let calls = fixture.checked().unwrap();
    for complete in [false, true] {
        let plain = if complete {
            crate::validation::validate_fragment_output_properties_observed(
                &fixture.fragment,
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
        let result =
            borrowed_proof_run(&fixture, SOURCE_INVOICE, complete, &control, &mut |facts| {
                captures += 1;
                for (index, value) in borrowed_facts_axes(*facts).into_iter().enumerate() {
                    maximum[index] = maximum[index].max(value);
                }
                borrowed_gate(facts, [usize::MAX; 3])
            })
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
        borrowed_proof_run(&fixture, SOURCE_INVOICE, complete, &control, &mut |facts| {
            borrowed_gate(facts, maximum)
        })
        .unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = BorrowedProofControl::default();
            let mut at_refusal = None;
            assert!(
                matches!(borrowed_proof_run(&fixture, SOURCE_INVOICE, complete, &control,
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
                borrowed_proof_run(&fixture, SOURCE_INVOICE, complete, &control, &mut |facts| {
                    borrowed_gate(facts, tight)
                }),
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
    fixture.checked().unwrap();
    for complete in [false, true] {
        for source in [SOURCE_INVOICE, 0] {
            let control = BorrowedProofControl::default();
            let baseline = borrowed_proof_run(&fixture, source, complete, &control, &mut |facts| {
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
                        matches!(borrowed_proof_run(&fixture, source, complete, &control,
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
    // it has no unreachable definition or synthetic root/flow owners.
    let fixture = empty_fixture();
    let calls = fixture.checked().unwrap();
    assert!(calls.entries().is_empty());
    assert!(fixture.fragment.nodes().values().all(|node| {
        node.expressions
            .as_ref()
            .is_none_or(|unit| unit.roots().is_empty())
    }));
    for complete in [false, true] {
        borrowed_proof_run(
            &fixture,
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

fn append_late_scalar(fixture: &mut Fixture) -> NodeId {
    use novarocks_expression_contract::{
        BoundExprSetBuilder, DefinitionControl, ExprPort, RootScope,
    };
    let mut parts = fixture.fragment.clone().into_parts();
    let input = parts.root;
    let node = NodeId::new(u32::MAX);
    assert!(!parts.nodes.contains_key(&node));
    let output = ValueId::new(
        parts
            .values
            .keys()
            .map(|id| id.get())
            .max()
            .unwrap()
            .checked_add(1)
            .unwrap(),
    );
    let input_node = &parts.nodes[&input];
    let ports = vec![ExprPort {
        values: input_node
            .output
            .columns
            .iter()
            .map(|id| (*id, parts.values[id].ty.clone()))
            .collect(),
    }];
    let required = input_node.output_properties.clone();
    let mut draft = BoundExprSetBuilder::new(unit_limits());
    let definition = draft
        .add(
            None,
            boolean(),
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        )
        .unwrap();
    draft.request(definition, zero_argument_request()).unwrap();
    draft
        .definition_control(
            definition,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = draft.seal(ports, &mut work).unwrap();
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, invocation)| {
            let mut claim = scalar_claim(invocation.context);
            claim.effects.observable_effects.warnings = true;
            (id, claim)
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    parts.values.insert(
        output,
        ValueDef {
            id: output,
            ty: boolean(),
            origin: ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        },
    );
    parts.nodes.insert(
        node,
        PhysicalNode {
            id: node,
            expressions: Some(std::sync::Arc::new(unit)),
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([(root, output)]),
            },
        },
    );
    parts.root = node;
    fixture.fragment = parts.into();
    node
}

// Real node/port sources isolate the newly metered shallow floor. They are
// created before the tested control starts and contain no invented calls/CVs.
fn floor_source(nodes: usize, ports: usize) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(991));
    let source = NodeId::new(u32::MAX);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut root = source;
    if ports != 0 {
        assert_eq!(nodes, 2);
        root = NodeId::new(0);
        builder
            .add_row_consuming(
                root,
                vec![source; ports].into_boxed_slice(),
                RequiredInputs::Singleton,
                Distribution::Singleton,
                Box::default(),
                NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: (0..ports).map(|_| Box::default()).collect(),
                },
            )
            .unwrap();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let draft = novarocks_expression_contract::BoundExprSetBuilder::new(unit_limits());
        let unit = draft
            .seal(builder.node_ports(root).unwrap(), &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        work.finish().unwrap();
        builder.install_unit(root, Arc::new(unit)).unwrap();
    } else {
        for ordinal in 0..nodes - 1 {
            let node = NodeId::new(u32::try_from(ordinal).unwrap());
            builder
                .add_project(node, root, Box::default(), Box::default())
                .unwrap();
            root = node;
        }
    }
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Noop,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap()
        .with_calls_observed(vec![], &Control::default())
        .unwrap();
    assert_eq!(fragment.nodes().len(), nodes);
    Fixture {
        fragment,
        primary: root,
    }
}

#[test]
fn shallow_source_floor_counts_inline_relation_header_once() {
    let fixture = empty_fixture();
    assert!(
        fixture
            .fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    assert!(fixture.fragment.values().is_empty());
    assert!(fixture.relations().is_empty());
    let expected = std::mem::size_of_val(&fixture.fragment)
        + fixture.fragment.nodes().len() * std::mem::size_of::<(NodeId, PhysicalNode)>();
    assert_eq!(
        source_floor(&fixture.fragment, fixture.fragment.calls()).unwrap(),
        expected
    );
}

#[test]
fn source_floor_real_many_ports_observe_quantum_before_index_projection() {
    let fixture = floor_source(2, 320);
    assert_eq!(fixture.unit().ports().len(), 320);
    assert!(fixture.unit().definitions().is_empty());
    assert!(fixture.unit().claims().is_empty());
    let run = |control: &ProofControl| {
        fixture
            .fragment
            .calls()
            .property_proof(
                &fixture.fragment,
                &PlanLimits::FROZEN,
                SOURCE_INVOICE,
                PROJECTION_LIMITS,
                control,
            )
            .map(|_| ())
    };
    let control = ProofControl::default();
    run(&control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    // The complete floor itself reaches this callback before projection/index
    // admission. The unmetered source implementation flushes only a short tail.
    assert_eq!(trace[0], (CompilePhase::Validate, 0));
    assert_eq!(trace[1], (CompilePhase::Validate, 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = ProofControl {
            refusal: Some((1, cause.clone())),
            ..Default::default()
        };
        assert!(matches!(run(&control),Err(FrozenCallError::Control(actual)) if actual==cause));
        assert_eq!(*control.trace.lock().unwrap(), trace[..=1]);
    }
}

#[test]
fn source_floor_real_unitless_nodes_finish_before_projection_envelope_refusal() {
    let fixture = floor_source(320, 0);
    assert!(
        fixture
            .fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    let projection = PropertyProofProjectionLimits {
        max_request_bytes: 0,
        ..PROJECTION_LIMITS
    };
    let run = |control: &ProofControl| {
        fixture
            .fragment
            .calls()
            .property_proof(
                &fixture.fragment,
                &PlanLimits::FROZEN,
                SOURCE_INVOICE,
                projection,
                control,
            )
            .map(|_| ())
    };
    let control = ProofControl::default();
    assert!(matches!(run(&control), Err(FrozenCallError::TooManyItems)));
    let trace = control.trace.lock().unwrap().clone();
    // UnitCounts scans 320 nodes, the shallow source floor scans those same
    // 320 real nodes as separate actual work, then the envelope refuses.
    assert_eq!(
        trace,
        vec![
            (CompilePhase::Validate, 0),
            (CompilePhase::Validate, 256),
            (CompilePhase::Validate, 256),
            (CompilePhase::Validate, 137)
        ]
    );
    prefixes(run, false);
}

#[test]
fn known_header_refusal_precedes_late_control_for_plain_and_borrowed_proof() {
    struct LateControl {
        trace: Mutex<Vec<u32>>,
        cause: CompileControlError,
    }
    impl PureCompileControl for LateControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Validate);
            self.trace.lock().unwrap().push(units);
            if units == 0 {
                Ok(())
            } else {
                Err(self.cause.clone())
            }
        }
    }
    let fixture = floor_source(320, 0);
    for source_missing in [false, true] {
        let source = if source_missing { 0 } else { SOURCE_INVOICE };
        let limits = if source_missing {
            PlanLimits::FROZEN
        } else {
            PlanLimits {
                fragment_nodes: 319,
                ..PlanLimits::FROZEN
            }
        };
        for borrowed in [false, true] {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = LateControl {
                    trace: Mutex::new(vec![]),
                    cause,
                };
                let calls = fixture.fragment.calls();
                let result = if borrowed {
                    let mut work =
                        CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
                    let result = calls
                        .property_proof_in(
                            &fixture.fragment,
                            &limits,
                            source,
                            PROJECTION_LIMITS,
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ());
                    finish_frozen_calls(work, result)
                } else {
                    calls
                        .property_proof(
                            &fixture.fragment,
                            &limits,
                            source,
                            PROJECTION_LIMITS,
                            &control,
                        )
                        .map(|_| ())
                };
                assert!(matches!(result, Err(FrozenCallError::TooManyItems)));
                assert_eq!(*control.trace.lock().unwrap(), vec![0, 0]);
            }
        }
    }
}
