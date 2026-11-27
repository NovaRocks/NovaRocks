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
use crate::plan::FragmentParts;
use novarocks_type_contract::SemanticParameters;

fn use_id(at: usize) -> ExpressionUseId {
    ExpressionUseId::new(match at {
        0 => 0,
        1 => u32::MAX,
        n => u32::try_from(n - 1).unwrap(),
    })
}
fn claim(fixture: &Fixture, at: usize) -> &novarocks_expression_contract::ExpressionCallClaim {
    &fixture.claims()[&use_id(at)]
}
fn edit_claim(
    fixture: &mut Fixture,
    at: usize,
    edit: impl FnOnce(&mut novarocks_expression_contract::ExpressionCallClaim),
) {
    fixture.edit_unit(|parts| edit(parts.claims.get_mut(&use_id(at)).unwrap()));
}
fn expression_failure(fixture: &Fixture, at: usize) -> FrozenCallError {
    FrozenCallError::ReplicaExpression {
        node: fixture.primary,
        invocation: use_id(at),
    }
}
fn legacy_scalar_volatile(fixture: &mut Fixture) {
    fixture.edit_unit(|parts| {
        for definition in &mut parts.definitions {
            if let ExprKind::FunctionCall { function, .. } = &mut definition.kind {
                function.legacy_metadata.as_mut().unwrap().volatility =
                    FunctionVolatility::Volatile;
            }
        }
    });
}
fn edit_relation(
    fixture: &mut Fixture,
    site: PhysicalCallSite,
    edit: impl FnOnce(&mut FrozenPhysicalCall),
) {
    fixture.fragment = fixture
        .candidate_relations(|calls| {
            edit(calls.iter_mut().find(|call| call.site == site).unwrap());
        })
        .unwrap();
}

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct ReplicaControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for ReplicaControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original control refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn edit_fragment(fragment: &Fragment, edit: impl FnOnce(&mut FragmentParts)) -> Fragment {
    let mut parts = fragment.clone().into_parts();
    edit(&mut parts);
    parts.into()
}
fn broadcast(fixture: &mut Fixture) {
    fixture.fragment = edit_fragment(&fixture.fragment, |parts| {
        let node = parts.nodes.get_mut(&parts.root).unwrap();
        node.output_properties.distribution = Distribution::Broadcast;
        node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
    });
}
fn check(fixture: &Fixture, control: &ReplicaControl) -> Result<(), FrozenCallError> {
    fixture
        .fragment
        .calls()
        .validate_replica_equivalence(&fixture.fragment, control)
}
fn prefixes(
    call: impl Fn(&ReplicaControl) -> Result<(), FrozenCallError>,
    success: bool,
    all: bool,
) {
    let good = ReplicaControl::default();
    assert_eq!(call(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    let positions = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| {
            (all || at == 0 || at + 1 == trace.len() || *units == 256).then_some(at)
        })
        .collect::<Vec<_>>();
    for at in positions {
        for cause in CAUSES {
            let control = ReplicaControl {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FrozenCallError::Control(actual)) if actual == cause),
                "refusal at {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn frozen_construction_success_duplicate_and_missing_site_observe_all_original_tails() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let original: Vec<_> = fixture.relations().values().cloned().collect();
    for variant in 0..3 {
        let mut calls = original.clone();
        let expected = match variant {
            0 => None,
            1 => {
                calls.push(calls[0].clone());
                Some(FrozenCallError::DuplicateSite)
            }
            2 => Some(FrozenCallError::MissingSite(calls.pop().unwrap().site)),
            _ => unreachable!(),
        };
        let good = ReplicaControl::default();
        assert_eq!(
            FrozenFragmentCalls::try_new(&fixture.fragment, calls.clone(), &good).err(),
            expected
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(trace.last().is_some());
        if variant == 1 {
            assert_eq!(
                trace,
                vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 4)]
            );
        }
        prefixes(
            |control| {
                FrozenFragmentCalls::try_new(&fixture.fragment, calls.clone(), control).map(|_| ())
            },
            variant == 0,
            true,
        );
    }
}

#[test]
fn frozen_validation_success_ordinary_context_and_public_shape_keep_every_tail_cause() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let site = *fixture.relations().keys().next().unwrap();
    for variant in 0..3 {
        let mut actual = fixture.fragment.calls().clone();
        let call = Arc::make_mut(&mut actual.entries).get_mut(&site).unwrap();
        let expected = match variant {
            0 => None,
            1 => {
                call.context.demand = EvaluationDemand::TruthOnly;
                Some(FrozenCallError::WrongContext)
            }
            2 => {
                call.effects.argument_control = ArgumentControl::TypeOnly;
                Some(FrozenCallError::InvalidEffects(
                    EffectContractError::KindMismatch,
                ))
            }
            _ => unreachable!(),
        };
        assert_eq!(
            actual
                .validate_fragment(&fixture.fragment, &ReplicaControl::default())
                .err(),
            expected
        );
        prefixes(
            |control| actual.validate_fragment(&fixture.fragment, control),
            variant == 0,
            true,
        );
    }
}

#[test]
fn frozen_validation_foreign_fragment_and_extra_site_keep_zero_tail_observation() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let checked = fixture.checked().unwrap();
    let foreign = edit_fragment(&fixture.fragment, |parts| parts.id = FragmentId::new(713));
    assert_eq!(
        checked.validate_fragment(&foreign, &ReplicaControl::default()),
        Err(FrozenCallError::WrongFragment)
    );
    prefixes(
        |control| checked.validate_fragment(&foreign, control),
        false,
        true,
    );
    let mut extra = checked.clone();
    let mut call = fixture.relations().values().next().unwrap().clone();
    call.site = PhysicalCallSite::WriterPartial {
        node: fixture.fragment.root(),
        call: u32::MAX,
    };
    Arc::make_mut(&mut extra.entries).insert(call.site, call);
    assert_eq!(
        extra.validate_fragment(&fixture.fragment, &ReplicaControl::default()),
        Err(FrozenCallError::InvalidSite)
    );
    prefixes(
        |control| extra.validate_fragment(&fixture.fragment, control),
        false,
        true,
    );
}

#[test]
fn frozen_dynamic_item_invoice_success_uses_original_control_for_each_tail() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut fixture_constants);
    let checked = fixture.checked().unwrap();
    let expected = checked.entries().len()
        + checked
            .entries()
            .values()
            .map(|call| call.effects.environment.len())
            .sum::<usize>();
    assert_eq!(
        checked.dynamic_items_observed(&ReplicaControl::default()),
        Ok(expected)
    );
    prefixes(
        |control| checked.dynamic_items_observed(control).map(|_| ()),
        true,
        true,
    );
}

#[test]
fn replica_equivalence_shared_definition_checks_each_sparse_use_and_domain() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut constants);
    broadcast(&mut fixture);
    let uses = fixture.unit().control_flow().uses();
    assert_eq!(uses[&use_id(0)].definition, uses[&use_id(1)].definition);
    assert_ne!(
        claim(&fixture, 0).context.region,
        claim(&fixture, 1).context.region
    );
    check(&fixture, &ReplicaControl::default()).unwrap();
    for at in 0..2 {
        edit_claim(&mut fixture, at, |c| {
            c.effects.observable_effects.rng_sampling = true
        });
        assert_eq!(
            check(&fixture, &ReplicaControl::default()),
            Err(expression_failure(&fixture, at))
        );
        edit_claim(&mut fixture, at, |c| {
            c.effects.observable_effects = ObservableEffects::NONE
        });
    }
}

#[test]
fn replica_equivalence_complete_claims_override_contradictory_legacy_volatility() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, u32::MAX, &mut constants);
    broadcast(&mut fixture);
    edit_claim(&mut fixture, 0, |c| {
        c.effects.value_stability = FunctionVolatility::Volatile
    });
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(expression_failure(&fixture, 0))
    );
    edit_claim(&mut fixture, 0, |c| {
        c.effects.value_stability = FunctionVolatility::Immutable
    });
    legacy_scalar_volatile(&mut fixture);
    check(&fixture, &ReplicaControl::default()).unwrap();
    edit_claim(&mut fixture, 0, |c| {
        c.effects.value_stability = FunctionVolatility::Stable
    });
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(expression_failure(&fixture, 0))
    );
}

#[test]
fn replica_equivalence_immutable_state_and_each_observable_are_refused() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, 0, &mut constants);
    broadcast(&mut fixture);
    edit_claim(&mut fixture, 0, |c| {
        c.effects.instance_state = FunctionInstanceState::ScalarInstance
    });
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(expression_failure(&fixture, 0))
    );
    edit_claim(&mut fixture, 0, |c| {
        c.effects.instance_state = FunctionInstanceState::None
    });
    for observable in [
        ObservableEffects {
            rng_sampling: true,
            warnings: false,
            controlled_wait: false,
        },
        ObservableEffects {
            rng_sampling: false,
            warnings: true,
            controlled_wait: false,
        },
        ObservableEffects {
            rng_sampling: false,
            warnings: false,
            controlled_wait: true,
        },
    ] {
        edit_claim(&mut fixture, 0, |c| {
            c.effects.observable_effects = observable
        });
        assert_eq!(
            check(&fixture, &ReplicaControl::default()),
            Err(expression_failure(&fixture, 0))
        );
    }
}

#[test]
fn replica_equivalence_keeps_same_occurrence_domain_proof_and_row_errors() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut constants);
    broadcast(&mut fixture);
    edit_claim(&mut fixture, 0, |c| {
        c.effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
        c.effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    });
    edit_claim(&mut fixture, 1, |c| {
        c.effects.proof_scope = CallProofScope::Unconditional
    });
    check(&fixture, &ReplicaControl::default()).unwrap();
    let first = claim(&fixture, 0);
    assert_eq!(
        first.effects.own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert_eq!(
        first.effects.failure_behavior,
        FunctionFailureBehavior::ReturnsNull
    );
    assert_eq!(
        first.effects.proof_scope,
        CallProofScope::Region(first.context.region)
    );
    assert_eq!(
        claim(&fixture, 1).effects.proof_scope,
        CallProofScope::Unconditional
    );
}

#[test]
fn replica_equivalence_single_copy_does_not_invent_a_replay_requirement() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut constants);
    fixture.edit_unit(|parts| {
        for call in parts.claims.values_mut() {
            call.effects.value_stability = FunctionVolatility::Volatile;
            call.effects.instance_state = FunctionInstanceState::ScalarInstance;
            call.effects.observable_effects = ObservableEffects {
                rng_sampling: true,
                warnings: true,
                controlled_wait: true,
            };
        }
    });
    check(&fixture, &ReplicaControl::default()).unwrap();
    broadcast(&mut fixture);
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(expression_failure(&fixture, 0))
    );
}

#[test]
fn replica_equivalence_revalidates_fragment_roots_and_context_before_claims() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let calls = fixture.checked().unwrap();
    let foreign = edit_fragment(&fixture.fragment, |parts| parts.id = FragmentId::new(7));
    assert_eq!(
        calls.validate_replica_equivalence(&foreign, &ReplicaControl::default()),
        Err(FrozenCallError::WrongFragment)
    );
    // Root correspondence now belongs to the node/unit structural gate, not the relational table.
    let changed = edit_fragment(&fixture.fragment, |parts| {
        let NodeKind::Values { rows } = &mut parts.nodes.get_mut(&fixture.primary).unwrap().kind
        else {
            panic!("values")
        };
        rows[0][0] = RootOrdinal::new(u32::MAX);
    });
    let error = validate_fragment_definition(&changed).unwrap_err();
    assert!(
        error
            .errors()
            .iter()
            .any(|e| e.message().contains("absent unit root"))
    );
    let mut invalid = calls.clone();
    let site = *fixture.relations().keys().next().unwrap();
    Arc::make_mut(&mut invalid.entries)
        .get_mut(&site)
        .unwrap()
        .context = novarocks_type_contract::RelationEffectContext::materialized_input(
        novarocks_type_contract::RelationCallId::new(0),
    );
    assert_eq!(
        invalid.validate_replica_equivalence(&fixture.fragment, &ReplicaControl::default()),
        Err(FrozenCallError::WrongContext)
    );
    prefixes(
        |control| invalid.validate_replica_equivalence(&fixture.fragment, control),
        false,
        true,
    );
}

#[test]
fn replica_equivalence_small_success_error_and_wide_quantum_preserve_all_control_causes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut small = scalar_fixture(2, 0, &mut fixture_constants);
    broadcast(&mut small);
    prefixes(|control| check(&small, control), true, true);
    edit_claim(&mut small, 1, |c| {
        c.effects.observable_effects.warnings = true
    });
    prefixes(|control| check(&small, control), false, true);
    let mut wide = scalar_fixture(320, u32::MAX, &mut fixture_constants);
    broadcast(&mut wide);
    let good = ReplicaControl::default();
    check(&wide, &good).unwrap();
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(|control| check(&wide, control), true, false);
}

fn table_source(
    scalar_input: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    use novarocks_expression_contract::{BoundExprSetBuilder, ExprPort, RootScope};
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = add_values(&mut builder, 1, scalar_input, fixture_constants);
    let input_value = ValueId::new(0);
    assert!(
        matches!(builder.value(input_value).unwrap().origin,ValueOrigin::NodeOutput{node,output_ordinal:0} if node==input)
    );
    let input_type = if scalar_input { boolean() } else { integer() };
    let table = builder.reserve_node_id().unwrap();
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let selected = function(FunctionKind::Table, integer());
    let mut unit_builder = BoundExprSetBuilder::new(unit_limits());
    let arguments = if scalar_input {
        let def = unit_builder
            .add(None, input_type.clone(), ExprKind::input(input_value))
            .unwrap();
        // Value leaves use their intrinsic Eager flow; no function control record.
        Box::from([unit_builder
            .root(def, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap()])
    } else {
        Box::default()
    };
    install_node(
        &mut builder,
        table,
        vec![input],
        vec![input_value, output],
        NodeKind::TableFunction {
            function: BoundTableFunction {
                function_id: selected.function_id,
                overload: selected.overload,
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    if scalar_input {
                        Box::from([FunctionArgumentType::Value(input_type.clone())])
                    } else {
                        Box::default()
                    },
                ),
                result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                    [integer()],
                )),
                legacy_metadata: selected.legacy_metadata,
            },
            arguments,
            outputs: Box::from([
                TableFunctionOutput::PassThrough(input_value),
                TableFunctionOutput::FunctionResult {
                    result_ordinal: 0,
                    value: output,
                },
            ]),
            left_outer: false,
        },
    );
    if scalar_input {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let sealed = unit_builder
            .seal(
                vec![ExprPort {
                    values: Box::from([(input_value, input_type.clone())]),
                }],
                &mut work,
            )
            .unwrap();
        let unit = sealed.finish(BTreeMap::new(), &mut work).unwrap();
        work.finish().unwrap();
        builder.install_unit(table, Arc::new(unit)).unwrap();
    }
    let fragment = builder
        .finish_structure(
            table,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let site = PhysicalCallSite::Table { node: table };
    let mut request = zero_argument_request();
    if scalar_input {
        request.logical_argument_count = 1;
        request.arguments = Box::from([crate::StaticFunctionArgument::Value {
            value_type: input_type.clone(),
            constant: None,
        }]);
    }
    let fragment = fragment
        .with_call_requests_observed(vec![(site, request)], &Control::default())
        .unwrap();
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(1),
    );
    let fragment = fragment
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
    let mut parts = fragment.into_parts();
    for node in parts.nodes.values_mut() {
        node.output_properties.distribution = Distribution::Broadcast;
        node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
        for required in &mut node.required_inputs {
            required.distribution = Distribution::Broadcast;
            required.row_multiplicity = RowMultiplicity::Replicated;
        }
    }
    Fixture {
        fragment: parts.into(),
        primary: input,
    }
}

fn broadcast_table_fixture(
    constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    table_source(true, constants)
}

#[test]
fn replica_equivalence_canonical_table_lifecycle_accepts_broadcast_and_refuses_exact_unsafe_site() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = broadcast_table_fixture(&mut constants);
    let site = *fixture.relations().keys().next().unwrap();
    let call = &fixture.relations()[&site];
    assert!(matches!(site, PhysicalCallSite::Table { .. }));
    assert_eq!(
        call.effects.instance_state,
        FunctionInstanceState::TableInstance
    );
    assert!(call.effects.observable_effects.is_empty());
    validate_fragment(&fixture.fragment, &FragmentCuts::default()).unwrap();
    check(&fixture, &ReplicaControl::default()).unwrap();
    edit_relation(&mut fixture, site, |c| {
        c.effects.observable_effects.warnings = true
    });
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    edit_relation(&mut fixture, site, |c| {
        c.effects.observable_effects = ObservableEffects::NONE;
        c.effects.value_stability = FunctionVolatility::Volatile;
    });
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    assert!(matches!(
        fixture.candidate_relations(|calls| {
            let c = calls.iter_mut().find(|c| c.site == site).unwrap();
            c.effects.value_stability = FunctionVolatility::Immutable;
            c.effects.instance_state = FunctionInstanceState::None;
        }),
        Err(FrozenCallError::InvalidEffects(_))
    ));
}

fn scalar_package_input(fixture: &Fixture) -> FragmentPackageInput {
    FragmentPackageInput {
        constants: ConstantPools::empty(),
        version: PlanVersionId::try_new([71; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment: fixture.fragment.clone(),

        pruning: FrozenFragmentPruning::try_new(fixture.fragment.id(), vec![], &Control::default())
            .unwrap(),
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    }
}

#[test]
fn replica_equivalence_package_rejects_one_unsafe_shared_scalar_occurrence() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, u32::MAX, &mut constants);
    broadcast(&mut fixture);
    let uses = fixture.unit().control_flow().uses();
    assert_eq!(uses[&use_id(0)].definition, uses[&use_id(1)].definition);
    FragmentPackage::try_new(
        scalar_package_input(&fixture),
        package_admission(),
        &ReplicaControl::default(),
    )
    .unwrap();
    for mode in 0..2 {
        edit_claim(&mut fixture, 1, |c| {
            c.effects.instance_state = if mode == 0 {
                FunctionInstanceState::ScalarInstance
            } else {
                FunctionInstanceState::None
            };
            c.effects.observable_effects.warnings = mode == 1;
        });
        let error = FragmentPackage::try_new(
            scalar_package_input(&fixture),
            package_admission(),
            &ReplicaControl::default(),
        )
        .unwrap_err();
        assert!(
            matches!(error,FragmentPackageError::Calls(actual) if actual==expression_failure(&fixture,1))
        );
    }
}

// Explicit small-fixture source invoice and independent property projection
// ceilings. These are test inputs, not a production default or MEM grant.
fn package_admission() -> crate::FragmentPackageAdmission {
    crate::FragmentPackageAdmission {
        plan_limits: crate::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: crate::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

#[test]
fn package_property_author_uses_complete_occurrence_facts_and_ignores_legacy_volatility() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 809, &mut constants);
    broadcast(&mut fixture);
    legacy_scalar_volatile(&mut fixture);
    edit_claim(&mut fixture, 0, |c| {
        c.effects.own_row_error = FunctionIntrinsicRowError::MayRaise
    });
    FragmentPackage::try_new(
        scalar_package_input(&fixture),
        package_admission(),
        &ReplicaControl::default(),
    )
    .unwrap();
    assert_eq!(
        claim(&fixture, 0).effects.own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    edit_claim(&mut fixture, 1, |c| {
        c.effects.observable_effects.rng_sampling = true
    });
    assert!(
        matches!(FragmentPackage::try_new(scalar_package_input(&fixture),package_admission(),&ReplicaControl::default()),Err(FragmentPackageError::Calls(actual)) if actual==expression_failure(&fixture,1))
    );
}

#[test]
fn package_property_projection_requires_each_explicit_ceiling_and_original_source_invoice() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 810, &mut fixture_constants);
    let input = scalar_package_input(&fixture);
    let base = package_admission();
    let facts = validate_fragment_output_properties_observed(
        &input.fragment,
        base.plan_limits,
        base.source_retained_bytes,
        base.property_projection_limits,
        &ReplicaControl::default(),
    )
    .unwrap();
    for variant in 0..5 {
        let mut admission = base;
        match variant {
            0 => admission.source_retained_bytes = 0,
            1 => admission.property_projection_limits.max_request_bytes = facts.request_bytes - 1,
            2 => {
                admission.property_projection_limits.max_coexisting_bytes =
                    facts.coexisting_bytes - 1
            }
            3 => {
                admission.property_projection_limits.max_projection_work = facts.projection_work - 1
            }
            4 => admission.plan_limits.fragment_nodes = 0,
            _ => unreachable!(),
        }
        let rejected =
            FragmentPackage::try_new(input.clone(), admission, &ReplicaControl::default());
        if variant == 3 {
            // The complete projection includes the guarantee source walk.
            // This one-under still admits the earlier occurrence-only index.
            let Err(FragmentPackageError::Structure(errors)) = rejected else {
                panic!("expected the combined guarantee-work envelope rejection");
            };
            assert!(errors.errors().iter().any(|error| {
                error.category() == ValidationErrorCategory::ResourceLimit
                    && error.path() == "fragment.guarantees.resources"
            }));
        } else {
            assert!(matches!(
                rejected,
                Err(FragmentPackageError::Calls(FrozenCallError::TooManyItems))
            ));
        }
    }
    let exact = FragmentPackageAdmission {
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: facts.request_bytes,
            max_coexisting_bytes: facts.coexisting_bytes,
            max_projection_work: facts.projection_work,
        },
        ..base
    };
    FragmentPackage::try_new(input, exact, &ReplicaControl::default()).unwrap();
}
