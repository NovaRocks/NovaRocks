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

// An explicit test flow for literal, Value and zero-argument function roots.
// No production control inference or new effect owner is implied.
fn roots_for_fixture(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control::default()).unwrap();
    let mut bindings = Vec::new();
    let mut invocations = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        match &fragment.expressions().get(root.expr).unwrap().kind {
            ExprKind::Literal(_) | ExprKind::Value(_) => {}
            ExprKind::FunctionCall { args, .. } => assert!(args.is_empty()),
            _ => panic!("fixture uses explicit leaf roots only"),
        }
        let current = context(ordinal as u32, 0, root.demand);
        bindings.push((*site, current.use_id));
        invocations.push(ExpressionInvocation {
            context: current,
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    let flow = ExpressionControlFlow::try_new(
        vec![domain(0)],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control::default()).unwrap()
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

fn fixture(operator: Operator, ordered: bool) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(match operator {
        Operator::Filter => 801,
        Operator::Project => 802,
        Operator::Table => 803,
    }));
    let (values, _) = add_values(&mut builder, 1, false);
    let input_value = ValueId::new(0);
    let child = if ordered {
        let sort = builder.reserve_node_id().unwrap();
        let expression = builder
            .add_expression(sort, integer(), ExprKind::Value(input_value))
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
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
                .add_expression(
                    target,
                    result.clone(),
                    ExprKind::FunctionCall {
                        function: function(FunctionKind::Scalar, result),
                        args: Box::default(),
                    },
                )
                .unwrap();
            builder
                .add_filter(target, child, Box::from([predicate]))
                .unwrap();
        }
        Operator::Project => {
            let expression = builder
                .add_expression(
                    target,
                    result.clone(),
                    ExprKind::FunctionCall {
                        function: function(FunctionKind::Scalar, result.clone()),
                        args: Box::default(),
                    },
                )
                .unwrap();
            let output = builder
                .add_value(
                    result,
                    ValueOrigin::Expr {
                        node: target,
                        expr: expression,
                    },
                )
                .unwrap();
            let passthrough = builder
                .add_expression(target, integer(), ExprKind::Value(input_value))
                .unwrap();
            builder
                .add_project(
                    target,
                    child,
                    Box::from([
                        (passthrough, input_value),
                        (expression, output),
                        (passthrough, input_value),
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
    let fragment = builder
        .finish_definition(
            target,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    let uses = roots_for_fixture(&fragment);
    let mut calls: Vec<_> = uses
        .flow()
        .uses()
        .values()
        .filter(|&invocation| {
            matches!(
                fragment
                    .expressions()
                    .get(invocation.definition)
                    .unwrap()
                    .kind,
                ExprKind::FunctionCall { .. }
            )
        })
        .map(|invocation| FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(invocation.context.use_id),
            context: invocation.context,
            effects: effects(FunctionKind::Scalar, invocation.context.region),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        })
        .collect();
    if matches!(operator, Operator::Table) {
        let current = context(777, 0, EvaluationDemand::Value);
        calls.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Table { node: target },
            context: current,
            effects: effects(FunctionKind::Table, current.region),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        });
    }
    Fixture {
        fragment,
        uses,
        calls,
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
    calls: &FrozenFragmentCalls,
    node: NodeId,
    source: usize,
    control: &dyn PureCompileControl,
) -> Result<Option<(PhysicalProperties, PropertyProofProjectionFacts)>, FragmentPropertyError> {
    derive_replica_sensitive_output_properties_observed(
        &fixture.fragment,
        &fixture.uses,
        calls,
        node,
        PlanLimits::FROZEN,
        source,
        PROJECTION,
        control,
    )
}
fn validate(
    fixture: &Fixture,
    calls: &FrozenFragmentCalls,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    validate_fragment_output_properties_observed(
        &fixture.fragment,
        &fixture.uses,
        calls,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        &DeriveControl::default(),
    )
}
fn prefixes(
    fixture: &Fixture,
    calls: &FrozenFragmentCalls,
    node: NodeId,
    source: usize,
    expected_ok: bool,
    expected_some: bool,
) {
    let before = fixture.fragment.nodes()[&fixture.fragment.root()]
        .output_properties
        .clone();
    let baseline = DeriveControl::default();
    let result = derive(fixture, calls, node, source, &baseline);
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
                matches!(derive(fixture, calls, node, source, &control), Err(FragmentPropertyError::Control(actual)) if actual == cause)
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
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let fixture = fixture(operator, false);
        let calls = fixture.checked().unwrap();
        let (actual, facts) = derive(
            &fixture,
            &calls,
            fixture.fragment.root(),
            SOURCE,
            &DeriveControl::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(actual, properties());
        assert!(facts.request_bytes > 0);
        validate(&fixture, &calls).unwrap();
        let mut with_row_error = fixture;
        with_row_error.calls[0].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
        with_row_error.calls[0].effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        let calls = with_row_error.checked().unwrap();
        assert_eq!(
            derive(
                &with_row_error,
                &calls,
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
            calls.entries()[&with_row_error.calls[0].site]
                .effects
                .own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
    }
}

#[test]
fn replica_property_derivation_relinquishes_only_unsafe_broadcast_without_mutating_source() {
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        for mode in 0..3 {
            let mut fixture = fixture(operator, false);
            broadcast(&mut fixture);
            match mode {
                0 => fixture.calls[0].effects.value_stability = FunctionVolatility::Volatile,
                1 => fixture.calls[0].effects.observable_effects.warnings = true,
                2 => {
                    fixture.calls[0].effects.instance_state = if matches!(operator, Operator::Table)
                    {
                        fixture.calls[0].effects.observable_effects.rng_sampling = true;
                        FunctionInstanceState::TableInstance
                    } else {
                        FunctionInstanceState::ScalarInstance
                    }
                }
                _ => unreachable!(),
            }
            let calls = fixture.checked().unwrap();
            assert!(matches!(validate(&fixture, &calls),
                Err(FragmentPropertyError::Calls(FrozenCallError::ReplicaEquivalence(site))) if site == fixture.calls[0].site));
            let original = fixture.fragment.nodes()[&fixture.fragment.root()]
                .output_properties
                .clone();
            let derived = derive(
                &fixture,
                &calls,
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
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let mut fixture = fixture(operator, false);
        broadcast(&mut fixture);
        fixture.calls[0].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
        fixture.calls[0].effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        let calls = fixture.checked().unwrap();
        validate(&fixture, &calls).unwrap();
        assert_eq!(
            derive(
                &fixture,
                &calls,
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
                calls.entries()[&fixture.calls[0].site]
                    .effects
                    .instance_state,
                FunctionInstanceState::TableInstance
            );
        }
    }
}

#[test]
fn replica_property_derivation_keeps_final_child_ordering_repeated_columns_and_complete_types() {
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let fixture = fixture(operator, true);
        let calls = fixture.checked().unwrap();
        validate(&fixture, &calls).unwrap();
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
                &calls,
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
                        .expressions()
                        .get(expressions[1].0)
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
    let mut fixture = fixture(Operator::Project, true);
    edit(&mut fixture, |parts| {
        parts
            .nodes
            .get_mut(&parts.root)
            .unwrap()
            .output_properties
            .ordering = Box::default();
    });
    let calls = fixture.checked().unwrap();
    assert!(matches!(
        validate(&fixture, &calls),
        Err(FragmentPropertyError::Structure(_))
    ));
    let derived = derive(
        &fixture,
        &calls,
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
        derive(&fixture, &calls, values, SOURCE, &DeriveControl::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn replica_property_derivation_unknown_node_and_source_floor_have_typed_ordinary_refusals() {
    let fixture = fixture(Operator::Filter, false);
    let calls = fixture.checked().unwrap();
    let control = DeriveControl::default();
    let error = derive(&fixture, &calls, NodeId::new(u32::MAX), SOURCE, &control).unwrap_err();
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
        crate::frozen_calls::property_proof::source_floor(&fixture.fragment, &fixture.uses, &calls)
            .unwrap();
    assert!(floor > 0);
    assert!(matches!(
        derive(
            &fixture,
            &calls,
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
    for operator in [Operator::Filter, Operator::Project, Operator::Table] {
        let mut fixture = fixture(operator, false);
        broadcast(&mut fixture);
        fixture.calls[0].effects.observable_effects.warnings = true;
        let calls = fixture.checked().unwrap();
        prefixes(
            &fixture,
            &calls,
            fixture.fragment.root(),
            SOURCE,
            true,
            true,
        );
        prefixes(&fixture, &calls, NodeId::new(0), SOURCE, true, false);
        prefixes(
            &fixture,
            &calls,
            NodeId::new(u32::MAX),
            SOURCE,
            false,
            false,
        );
        let floor = crate::frozen_calls::property_proof::source_floor(
            &fixture.fragment,
            &fixture.uses,
            &calls,
        )
        .unwrap();
        prefixes(
            &fixture,
            &calls,
            fixture.fragment.root(),
            floor - 1,
            false,
            false,
        );
    }
}
