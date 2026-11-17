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
use arrow_schema::Field;
use std::collections::HashMap;

const SOURCE: usize = 4 * 1024 * 1024;
const PROJECTION: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};
#[derive(Clone, Copy)]
enum Operator {
    Filter,
    Project,
    Table,
}
#[derive(Default)]
struct DeriveControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for DeriveControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "property derivation callback after refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

// Fixed leaf fixtures explicitly declare Eager zero-argument calls; no
// binding metadata is used to recover either control or effect authority.
fn seal_leaf_units(builder: &mut FragmentBuilder) {
    let nodes = builder
        .construction_nodes()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for node in nodes {
        let Some(draft) = builder.construction_unit(node) else {
            continue;
        };
        let calls = draft
            .definitions()
            .filter_map(|definition| match &definition.kind {
                ExprKind::FunctionCall { args, .. } => {
                    assert!(args.is_empty());
                    Some(definition.id)
                }
                ExprKind::Constant(_) | ExprKind::Value(_) => None,
                _ => panic!("this fixed fixture declares only leaf expressions"),
            })
            .collect::<Vec<_>>();
        let ports = builder.node_ports(node).unwrap();
        let mut draft = builder.take_unit_builder(node).unwrap();
        for definition in calls {
            draft.request(definition, zero_argument_request()).unwrap();
            draft
                .definition_control(
                    definition,
                    novarocks_expression_contract::DefinitionControl {
                        shape: ControlShape::Eager,
                        temporal_sources: None,
                    },
                )
                .unwrap();
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let sealed = draft.seal(ports, &mut work).unwrap();
        let claims = sealed
            .uses()
            .iter()
            .filter(|(_, invocation)| {
                matches!(
                    sealed.definition(invocation.definition).unwrap().kind,
                    ExprKind::FunctionCall { .. }
                )
            })
            .map(|(&id, invocation)| {
                let mut claim = scalar_claim(invocation.context);
                claim.decimal_overflow_policy =
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError;
                (id, claim)
            })
            .collect();
        let unit = sealed.finish(claims, &mut work).unwrap();
        work.finish().unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
}
fn target_effect(fixture: &Fixture) -> &CallEffects {
    if let Some(unit) = fixture.fragment.unit(fixture.primary) {
        assert_eq!(unit.claims().len(), 1);
        &unit.claims().first_key_value().unwrap().1.effects
    } else {
        &fixture.relations()[&PhysicalCallSite::Table {
            node: fixture.primary,
        }]
            .effects
    }
}
fn edit_target_effect(fixture: &mut Fixture, change: impl FnOnce(&mut CallEffects)) {
    if fixture.fragment.unit(fixture.primary).is_some() {
        fixture.edit_unit(|parts| {
            assert_eq!(parts.claims.len(), 1);
            change(&mut parts.claims.first_entry().unwrap().get_mut().effects);
        });
    } else {
        let site = PhysicalCallSite::Table {
            node: fixture.primary,
        };
        fixture.fragment = fixture
            .candidate_relations(|calls| {
                change(
                    &mut calls
                        .iter_mut()
                        .find(|call| call.site == site)
                        .unwrap()
                        .effects,
                )
            })
            .unwrap();
    }
}
fn rich_result() -> ValueType {
    ValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new("exact-child", DataType::Int64, true).with_metadata(HashMap::from([(
                    "unrecognized-source-key".to_owned(),
                    "retained".to_owned(),
                )])),
            )]
            .into(),
        ),
        true,
    )
}

fn fixture(
    operator: Operator,
    ordered: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(match operator {
        Operator::Filter => 801,
        Operator::Project => 802,
        Operator::Table => 803,
    }));
    let values = add_values(&mut builder, 1, false, fixture_constants);
    let input_value = ValueId::new(0);
    let child = if ordered {
        let sort = builder.reserve_node_id().unwrap();
        let expression = builder
            .define(sort, integer(), ExprKind::Value(input_value))
            .unwrap();
        let expression = builder.root_definition(
            sort,
            expression,
            EvaluationDemand::Value,
            RootScope::Port(0),
        );
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: sort,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: PhysicalProperties {
                    ordering: Box::from([OrderingKey {
                        value: input_value,
                        direction: SortDirection::Descending,
                        null_ordering: NullOrdering::Last,
                    }]),
                    ..properties()
                },
                output: OutputPort {
                    node: sort,
                    columns: Box::from([input_value]),
                },
                kind: NodeKind::Sort {
                    mode: SortMode::Global,
                    order_by: Box::from([SortExpr {
                        expr: expression,
                        direction: SortDirection::Descending,
                        null_ordering: NullOrdering::Last,
                    }]),
                },
            })
            .unwrap();
        sort
    } else {
        values
    };
    let target = builder.reserve_node_id().unwrap();
    let result = if matches!(operator, Operator::Filter) {
        boolean()
    } else {
        rich_result()
    };
    match operator {
        Operator::Filter => {
            let predicate = builder
                .define(
                    target,
                    result.clone(),
                    ExprKind::FunctionCall {
                        function: function(FunctionKind::Scalar, result),
                        args: Box::default(),
                    },
                )
                .unwrap();
            let predicate = builder.root_definition(
                target,
                predicate,
                EvaluationDemand::TruthOnly,
                RootScope::Port(0),
            );
            builder
                .add_filter(target, child, Box::from([predicate]))
                .unwrap();
        }
        Operator::Project => {
            let expression = builder
                .define(
                    target,
                    result.clone(),
                    ExprKind::FunctionCall {
                        function: function(FunctionKind::Scalar, result.clone()),
                        args: Box::default(),
                    },
                )
                .unwrap();
            let expression = builder.root_definition(
                target,
                expression,
                EvaluationDemand::Value,
                RootScope::Port(0),
            );
            let output = builder
                .add_value(
                    result,
                    ValueOrigin::Expr {
                        node: target,
                        root: expression,
                    },
                )
                .unwrap();
            let passthrough = builder
                .define(target, integer(), ExprKind::Value(input_value))
                .unwrap();
            let repeated = builder.root_definition(
                target,
                passthrough,
                EvaluationDemand::Value,
                RootScope::Port(0),
            );
            let passthrough = builder.root_definition(
                target,
                passthrough,
                EvaluationDemand::Value,
                RootScope::Port(0),
            );
            builder
                .add_project(
                    target,
                    child,
                    Box::from([
                        (passthrough, input_value),
                        (expression, output),
                        (repeated, input_value),
                    ]),
                    Box::from([input_value, output, input_value]),
                )
                .unwrap();
        }
        Operator::Table => {
            let output = builder
                .add_value(
                    result.clone(),
                    ValueOrigin::NodeOutput {
                        node: target,
                        output_ordinal: 2,
                    },
                )
                .unwrap();
            let selected = function(FunctionKind::Table, result.clone());
            let input_properties = builder.node_output_properties(child).unwrap().clone();
            builder
                .insert_node_unchecked(PhysicalNode {
                    expressions: None,
                    id: target,
                    inputs: Box::from([child]),
                    required_inputs: Box::from([passthrough_requirement(&input_properties)]),
                    output_properties: input_properties,
                    output: OutputPort {
                        node: target,
                        columns: Box::from([input_value, input_value, output]),
                    },
                    kind: NodeKind::TableFunction {
                        function: BoundTableFunction {
                            function_id: selected.function_id,
                            overload: selected.overload,
                            argument_types:
                                novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                                    Box::default(),
                                ),
                            result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                                Box::from([result]),
                            ),
                            legacy_metadata: Some(crate::LegacyBindingMetadata {
                                volatility: selected.legacy_metadata.as_ref().unwrap().volatility,
                                argument_evaluation: selected
                                    .legacy_metadata
                                    .as_ref()
                                    .unwrap()
                                    .argument_evaluation,
                                failure_behavior: selected
                                    .legacy_metadata
                                    .as_ref()
                                    .unwrap()
                                    .failure_behavior,
                                intrinsic_row_error: selected
                                    .legacy_metadata
                                    .as_ref()
                                    .unwrap()
                                    .intrinsic_row_error,
                                semantic_parameters: Box::default(),
                            }),
                        },
                        arguments: Box::default(),
                        outputs: Box::from([
                            TableFunctionOutput::PassThrough(input_value),
                            TableFunctionOutput::PassThrough(input_value),
                            TableFunctionOutput::FunctionResult {
                                result_ordinal: 0,
                                value: output,
                            },
                        ]),
                        left_outer: false,
                    },
                })
                .unwrap();
        }
    }
    seal_leaf_units(&mut builder);
    let fragment = builder
        .finish_structure(
            target,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let fragment = crate::tests::original_call_requests::with_original_relational_requests(
        fragment,
        &Control::default(),
    );
    let fragment = crate::tests::unit_fixture::with_immutable_relation_claims(fragment);
    let relation_claims = fragment
        .calls()
        .entries()
        .values()
        .cloned()
        .map(|mut claim| {
            claim.decimal_overflow_policy =
                novarocks_type_contract::DecimalOverflowPolicy::ReportError;
            claim
        })
        .collect();
    let fragment = fragment
        .with_calls_observed(relation_claims, &Control::default())
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    Fixture {
        fragment,
        primary: target,
    }
}

fn edit(fixture: &mut Fixture, change: impl FnOnce(&mut crate::plan::FragmentParts)) {
    let mut parts = fixture.fragment.clone().into_parts();
    change(&mut parts);
    fixture.fragment = Fragment::from(parts);
}
fn broadcast(fixture: &mut Fixture) {
    edit(fixture, |parts| {
        for node in parts.nodes.values_mut() {
            node.output_properties.distribution = Distribution::Broadcast;
            node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
            for required in &mut node.required_inputs {
                required.row_multiplicity = RowMultiplicity::Replicated;
            }
        }
    });
}
fn derive(
    fixture: &Fixture,
    node: NodeId,
    source: usize,
    control: &dyn PureCompileControl,
) -> Result<Option<(PhysicalProperties, PropertyProofProjectionFacts)>, FragmentPropertyError> {
    derive_replica_sensitive_output_properties_observed(
        &fixture.fragment,
        node,
        PlanLimits::FROZEN,
        source,
        PROJECTION,
        control,
    )
}
fn validate(fixture: &Fixture) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    validate_fragment_output_properties_observed(
        &fixture.fragment,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        &DeriveControl::default(),
    )
}
fn prefixes(
    fixture: &Fixture,
    node: NodeId,
    source: usize,
    expected_ok: bool,
    expected_some: bool,
) {
    let before = fixture.fragment.nodes()[&fixture.fragment.root()]
        .output_properties
        .clone();
    let baseline = DeriveControl::default();
    let result = derive(fixture, node, source, &baseline);
    assert_eq!(result.is_ok(), expected_ok);
    if let Ok(value) = result {
        assert_eq!(value.is_some(), expected_some);
    }
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert_eq!(trace.last().unwrap().0, CompilePhase::Validate);
    // An enclosing finish flushes actual pending work, or observes zero when
    // the preceding boundary already flushed it. Neither tail is fabricated.
    if expected_ok {
        assert_eq!(trace.last(), Some(&(CompilePhase::Validate, 0)));
    }
    for stop in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = DeriveControl {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(derive(fixture, node, source, &control), Err(FragmentPropertyError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            assert_eq!(
                fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
                before
            );
        }
    }
}

#[test]
fn replica_property_derivation_uses_three_original_formulas_and_keeps_row_error_claims() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let fixture = fixture(operator, false, &mut fixture_constants);
        fixture.checked().unwrap();
        let (actual, facts) = derive(
            &fixture,
            fixture.fragment.root(),
            SOURCE,
            &DeriveControl::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(actual, properties());
        assert!(facts.request_bytes > 0);
        validate(&fixture).unwrap();
        let mut with_row_error = fixture;
        edit_target_effect(&mut with_row_error, |effects| {
            effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
            effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        });
        with_row_error.checked().unwrap();
        assert_eq!(
            derive(
                &with_row_error,
                with_row_error.fragment.root(),
                SOURCE,
                &DeriveControl::default()
            )
            .unwrap()
            .unwrap()
            .0,
            properties()
        );
        assert_eq!(
            target_effect(&with_row_error).own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
    }
}

#[test]
fn replica_property_derivation_relinquishes_only_unsafe_broadcast_without_mutating_source() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        for mode in 0..3 {
            let mut fixture = fixture(operator, false, &mut fixture_constants);
            broadcast(&mut fixture);
            edit_target_effect(&mut fixture, |effects| match mode {
                0 => effects.value_stability = FunctionVolatility::Volatile,
                1 => effects.observable_effects.warnings = true,
                2 => {
                    effects.instance_state = if matches!(operator, Operator::Table) {
                        effects.observable_effects.rng_sampling = true;
                        FunctionInstanceState::TableInstance
                    } else {
                        FunctionInstanceState::ScalarInstance
                    };
                }
                _ => unreachable!(),
            });
            fixture.checked().unwrap();
            let error = validate(&fixture).unwrap_err();
            match operator {
                Operator::Table => assert!(
                    matches!(error, FragmentPropertyError::Calls(FrozenCallError::ReplicaEquivalence(site)) if site == PhysicalCallSite::Table { node: fixture.primary })
                ),
                Operator::Filter | Operator::Project => {
                    let invocation = *fixture.claims().first_key_value().unwrap().0;
                    assert!(
                        matches!(error, FragmentPropertyError::Calls(FrozenCallError::ReplicaExpression { node, invocation: actual }) if node == fixture.primary && actual == invocation)
                    );
                }
            }
            let original = fixture.fragment.nodes()[&fixture.fragment.root()]
                .output_properties
                .clone();
            let derived = derive(
                &fixture,
                fixture.fragment.root(),
                SOURCE,
                &DeriveControl::default(),
            )
            .unwrap()
            .unwrap()
            .0;
            assert_eq!(
                derived,
                PhysicalProperties {
                    distribution: Distribution::Unconstrained,
                    row_multiplicity: RowMultiplicity::Replicated,
                    ordering: Box::default()
                }
            );
            assert_eq!(
                fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
                original
            );
            assert_eq!(original.distribution, Distribution::Broadcast);
        }
    }
}

#[test]
fn replica_property_derivation_immutable_table_lifecycle_and_maskable_errors_preserve_broadcast() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let mut fixture = fixture(operator, false, &mut fixture_constants);
        broadcast(&mut fixture);
        edit_target_effect(&mut fixture, |effects| {
            effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
            effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        });
        fixture.checked().unwrap();
        validate(&fixture).unwrap();
        assert_eq!(
            derive(
                &fixture,
                fixture.fragment.root(),
                SOURCE,
                &DeriveControl::default()
            )
            .unwrap()
            .unwrap()
            .0,
            PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::Replicated,
                ordering: Box::default()
            }
        );
        if matches!(operator, Operator::Table) {
            assert_eq!(
                target_effect(&fixture).instance_state,
                FunctionInstanceState::TableInstance
            );
        }
    }
}

#[test]
fn replica_property_derivation_keeps_final_child_ordering_repeated_columns_and_complete_types() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let fixture = fixture(operator, true, &mut fixture_constants);
        fixture.checked().unwrap();
        validate(&fixture).unwrap();
        let child = fixture.fragment.nodes()[&fixture.fragment.root()].inputs[0];
        let expected = PhysicalProperties {
            ordering: Box::from([OrderingKey {
                value: ValueId::new(0),
                direction: SortDirection::Descending,
                null_ordering: NullOrdering::Last,
            }]),
            ..properties()
        };
        assert_eq!(fixture.fragment.nodes()[&child].output_properties, expected);
        assert_eq!(
            derive(
                &fixture,
                fixture.fragment.root(),
                SOURCE,
                &DeriveControl::default()
            )
            .unwrap()
            .unwrap()
            .0,
            expected
        );
        if !matches!(operator, Operator::Filter) {
            let columns = &fixture.fragment.nodes()[&fixture.fragment.root()]
                .output
                .columns;
            assert_eq!(
                columns[0],
                columns[1 + usize::from(matches!(operator, Operator::Project))]
            );
            let output = columns[if matches!(operator, Operator::Project) {
                1
            } else {
                2
            }];
            let DataType::Struct(fields) = &fixture.fragment.values()[&output].ty.data_type()
            else {
                panic!("full source Struct")
            };
            assert_eq!(fields[0].name(), "exact-child");
            assert!(
                fields[0].metadata().is_empty(),
                "provider facts do not become value layout identity"
            );
            let selected_fields = match &fixture.fragment.nodes()[&fixture.fragment.root()].kind {
                NodeKind::Project { expressions } => {
                    let ExprKind::FunctionCall { function, .. } = &fixture
                        .fragment
                        .root_expression(fixture.primary, expressions[1].0)
                        .unwrap()
                        .kind
                    else {
                        panic!()
                    };
                    let DataType::Struct(fields) = &function.result_type.data_type() else {
                        panic!()
                    };
                    fields
                }
                NodeKind::TableFunction { function, .. } => {
                    let DataType::Struct(fields) = &function.result_types[0].data_type() else {
                        panic!()
                    };
                    fields
                }
                _ => unreachable!(),
            };
            assert!(Arc::ptr_eq(&fields[0], &selected_fields[0]));
        }
    }
}

#[test]
fn replica_property_derivation_corrects_wrong_provisional_claim_and_leaves_other_node_without_default()
 {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = fixture(Operator::Project, true, &mut fixture_constants);
    edit(&mut fixture, |parts| {
        parts
            .nodes
            .get_mut(&parts.root)
            .unwrap()
            .output_properties
            .ordering = Box::default();
    });
    fixture.checked().unwrap();
    assert!(matches!(
        validate(&fixture),
        Err(FragmentPropertyError::Structure(_))
    ));
    let derived = derive(
        &fixture,
        fixture.fragment.root(),
        SOURCE,
        &DeriveControl::default(),
    )
    .unwrap()
    .unwrap()
    .0;
    assert_eq!(
        derived.ordering.as_ref(),
        &[OrderingKey {
            value: ValueId::new(0),
            direction: SortDirection::Descending,
            null_ordering: NullOrdering::Last
        }]
    );
    let values = NodeId::new(0);
    assert!(matches!(
        fixture.fragment.nodes()[&values].kind,
        NodeKind::Values { .. }
    ));
    assert!(
        derive(&fixture, values, SOURCE, &DeriveControl::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn replica_property_derivation_unknown_node_and_source_floor_have_typed_ordinary_refusals() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = fixture(Operator::Filter, false, &mut fixture_constants);
    let calls = fixture.checked().unwrap();
    let control = DeriveControl::default();
    let error = derive(&fixture, NodeId::new(u32::MAX), SOURCE, &control).unwrap_err();
    // The absent-node lookup completed one step; ordinary finish exposes it.
    assert_eq!(
        control.trace.lock().unwrap().last(),
        Some(&(CompilePhase::Validate, 1))
    );
    let FragmentPropertyError::Structure(errors) = error else {
        panic!("typed absent node")
    };
    assert_eq!(errors.errors().len(), 1);
    assert_eq!(
        errors.errors()[0].path(),
        "fragment.property_derivation.node"
    );
    assert_eq!(
        errors.errors()[0].message(),
        "property derivation references an absent node"
    );
    let floor =
        crate::frozen_calls::property_proof::source_floor(&fixture.fragment, calls).unwrap();
    assert!(floor > 0);
    assert!(matches!(
        derive(
            &fixture,
            fixture.fragment.root(),
            floor - 1,
            &DeriveControl::default()
        ),
        Err(FragmentPropertyError::Calls(FrozenCallError::TooManyItems))
    ));
    // The known floor is not an invoice of all opaque retained backing.
}

#[test]
fn replica_property_derivation_every_actual_callback_keeps_three_causes_and_ordinary_tails() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let mut fixture = fixture(operator, false, &mut fixture_constants);
        broadcast(&mut fixture);
        edit_target_effect(&mut fixture, |effects| {
            effects.observable_effects.warnings = true
        });
        let calls = fixture.checked().unwrap();
        prefixes(&fixture, fixture.fragment.root(), SOURCE, true, true);
        prefixes(&fixture, NodeId::new(0), SOURCE, true, false);
        prefixes(&fixture, NodeId::new(u32::MAX), SOURCE, false, false);
        let floor =
            crate::frozen_calls::property_proof::source_floor(&fixture.fragment, calls).unwrap();
        prefixes(&fixture, fixture.fragment.root(), floor - 1, false, false);
    }
}
