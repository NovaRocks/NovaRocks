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

use super::super::property_proof::PropertyProofProjectionLimits;
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
struct FormulaControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for FormulaControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after first property refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn run(
    fixture: &Fixture,
    calls: &FrozenFragmentCalls,
    source: usize,
    control: &FormulaControl,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    validate_fragment_output_properties_observed(
        &fixture.fragment,
        &fixture.uses,
        calls,
        PlanLimits::FROZEN,
        source,
        PROJECTION,
        control,
    )
}
fn edit(fixture: &mut Fixture, change: impl FnOnce(&mut crate::plan::FragmentParts)) {
    let mut parts = fixture.fragment.clone().into_parts();
    change(&mut parts);
    fixture.fragment = Fragment::from(parts);
}
fn broadcast(fixture: &mut Fixture) {
    edit(fixture, |parts| {
        let root = parts.nodes.get_mut(&parts.root).unwrap();
        root.output_properties.distribution = Distribution::Broadcast;
        root.output_properties.row_multiplicity = RowMultiplicity::Replicated;
    });
}
fn legacy_volatile(fixture: &mut Fixture) {
    edit(fixture, |parts| {
        let definitions = parts
            .expressions
            .iter()
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        for mut definition in definitions {
            if let ExprKind::FunctionCall { function, .. } = &mut definition.kind {
                function.legacy_metadata.as_mut().unwrap().volatility =
                    FunctionVolatility::Volatile;
            }
            parts.expressions.insert(definition);
        }
        for node in parts.nodes.values_mut() {
            if let NodeKind::TableFunction { function, .. } = &mut node.kind {
                function.legacy_metadata.as_mut().unwrap().volatility =
                    FunctionVolatility::Volatile;
            }
        }
    });
}
fn prefixes(
    call: impl Fn(&FormulaControl) -> Result<PropertyProofProjectionFacts, FragmentPropertyError>,
    succeeds: bool,
    all: bool,
) {
    let original = FormulaControl::default();
    assert_eq!(call(&original).is_ok(), succeeds);
    let baseline = original.trace.lock().unwrap().clone();
    assert!(baseline.len() >= 2);
    for (at, (_, units)) in baseline.iter().enumerate() {
        if !all && at != 0 && at + 1 != baseline.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = FormulaControl {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FragmentPropertyError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), baseline[..=at]);
        }
    }
}

#[test]
fn frozen_property_formulas_use_complete_occurrences_despite_legacy_volatile_and_keep_row_errors() {
    let mut fixture = scalar_fixture(2, u32::MAX);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    assert!(validate_fragment_definition(&fixture.fragment).is_err());
    assert_eq!(fixture.calls[0].context.use_id, ExpressionUseId::new(0));
    assert_eq!(
        fixture.calls[1].context.use_id,
        ExpressionUseId::new(u32::MAX)
    );
    assert_ne!(
        fixture.calls[0].context.region,
        fixture.calls[1].context.region
    );
    for call in &mut fixture.calls {
        call.effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
        call.effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    }
    let calls = fixture.checked().unwrap();
    let facts = run(&fixture, &calls, SOURCE, &FormulaControl::default()).unwrap();
    assert!(facts.request_bytes > 0);
    for call in &fixture.calls {
        assert_eq!(calls.entries()[&call.site].effects, call.effects);
        assert_eq!(
            call.effects.proof_scope,
            CallProofScope::Region(call.context.region)
        );
    }
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()]
            .output_properties
            .distribution,
        Distribution::Broadcast
    );
    // This is replication proof, not permission to erase or hoist row errors.
}

#[test]
fn frozen_property_formulas_reject_each_sparse_unsafe_use_with_exact_global_replica_site() {
    for at in 0..2 {
        for mode in 0..4 {
            let mut fixture = scalar_fixture(2, 23);
            broadcast(&mut fixture);
            match mode {
                0 => {
                    fixture.calls[at].effects.instance_state = FunctionInstanceState::ScalarInstance
                }
                1 => fixture.calls[at].effects.observable_effects.warnings = true,
                2 => fixture.calls[at].effects.observable_effects.rng_sampling = true,
                3 => fixture.calls[at].effects.value_stability = FunctionVolatility::Stable,
                _ => unreachable!(),
            }
            let calls = fixture.checked().unwrap();
            assert!(matches!(
                run(&fixture, &calls, SOURCE, &FormulaControl::default()),
                Err(FragmentPropertyError::Calls(FrozenCallError::ReplicaEquivalence(site))) if site == fixture.calls[at].site
            ));
        }
    }
}

#[test]
fn frozen_property_formulas_single_copy_does_not_impose_replication_or_drop_effects() {
    let mut fixture = scalar_fixture(2, 29);
    fixture.calls[0].effects.instance_state = FunctionInstanceState::ScalarInstance;
    fixture.calls[0].effects.observable_effects.rng_sampling = true;
    fixture.calls[1].effects.value_stability = FunctionVolatility::Volatile;
    fixture.calls[1].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    let calls = fixture.checked().unwrap();
    run(&fixture, &calls, SOURCE, &FormulaControl::default()).unwrap();
    assert_eq!(
        calls.entries()[&fixture.calls[0].site]
            .effects
            .instance_state,
        FunctionInstanceState::ScalarInstance
    );
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
        properties()
    );
}

fn table_fixture() -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(37));
    let (input, _) = add_values(&mut builder, 1, false);
    let input_value = ValueId::new(0);
    assert!(matches!(builder.value(input_value).unwrap().origin,
        ValueOrigin::NodeOutput { node, output_ordinal: 0 } if node == input));
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
                    Box::default(),
                ),
                result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                    [integer()],
                )),
                legacy_metadata: Some(crate::LegacyBindingMetadata {
                    volatility: selected.legacy_metadata.as_ref().unwrap().volatility,
                    argument_evaluation: selected
                        .legacy_metadata
                        .as_ref()
                        .unwrap()
                        .argument_evaluation,
                    failure_behavior: selected.legacy_metadata.as_ref().unwrap().failure_behavior,
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
                TableFunctionOutput::FunctionResult {
                    result_ordinal: 0,
                    value: output,
                },
            ]),
            left_outer: false,
        },
    );
    let fragment = builder
        .finish_definition(
            table,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let uses = leaf_roots(&fragment);
    let current = context(1, 0, EvaluationDemand::Value);
    let mut fixture = Fixture {
        fragment,
        uses,
        calls: vec![FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Table { node: table },
            context: current,
            effects: effects(FunctionKind::Table, current.region),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        }],
    };
    edit(&mut fixture, |parts| {
        for node in parts.nodes.values_mut() {
            node.output_properties.distribution = Distribution::Broadcast;
            node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
            for required in &mut node.required_inputs {
                required.distribution = Distribution::Broadcast;
                required.row_multiplicity = RowMultiplicity::Replicated;
            }
        }
    });
    fixture
}

#[test]
fn frozen_property_formulas_canonical_relational_lifecycle_is_not_scalar_state() {
    let mut fixture = table_fixture();
    legacy_volatile(&mut fixture);
    assert!(validate_fragment_definition(&fixture.fragment).is_err());
    assert_eq!(
        fixture.calls[0].effects.instance_state,
        FunctionInstanceState::TableInstance
    );
    let calls = fixture.checked().unwrap();
    run(&fixture, &calls, SOURCE, &FormulaControl::default()).unwrap();
    fixture.calls[0].effects.observable_effects.warnings = true;
    let calls = fixture.checked().unwrap();
    assert!(
        matches!(run(&fixture, &calls, SOURCE, &FormulaControl::default()),
        Err(FragmentPropertyError::Calls(FrozenCallError::ReplicaEquivalence(site))) if site == fixture.calls[0].site)
    );
    let relational = special_fixture();
    let calls = relational.checked().unwrap();
    run(&relational, &calls, SOURCE, &FormulaControl::default()).unwrap();
    assert!(
        relational
            .calls
            .iter()
            .any(|call| call.effects.instance_state == FunctionInstanceState::AggregateInstance)
    );
    assert!(
        relational
            .calls
            .iter()
            .any(|call| call.effects.instance_state == FunctionInstanceState::WindowPartition)
    );
}

#[test]
fn frozen_property_formulas_keep_required_inputs_ordering_and_geometry_rejections() {
    for bad in 0..3 {
        let mut fixture = table_fixture();
        edit(&mut fixture, |parts| {
            let root = parts.nodes.get_mut(&parts.root).unwrap();
            match bad {
                0 => root.required_inputs[0] = properties(),
                1 => {
                    root.output_properties.ordering = Box::from([OrderingKey {
                        value: ValueId::new(u32::MAX),
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::First,
                    }])
                }
                2 => root.output.columns = Box::from([root.output.columns[0]]),
                _ => unreachable!(),
            }
        });
        let calls = fixture.checked().unwrap();
        assert!(
            matches!(
                run(&fixture, &calls, SOURCE, &FormulaControl::default()),
                Err(FragmentPropertyError::Structure(_))
            ),
            "malformed shape {bad}"
        );
    }
    let mut fixture = scalar_fixture(1, 43);
    broadcast(&mut fixture);
    edit(&mut fixture, |parts| {
        let value = parts.nodes[&parts.root].output.columns[0];
        parts
            .nodes
            .get_mut(&parts.root)
            .unwrap()
            .output_properties
            .ordering = Box::from([OrderingKey {
            value,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::First,
        }])
    });
    let calls = fixture.checked().unwrap();
    assert!(matches!(
        run(&fixture, &calls, SOURCE, &FormulaControl::default()),
        Err(FragmentPropertyError::Structure(_))
    ));
}

#[test]
fn frozen_property_formulas_keep_closed_values_scope_separate_from_legacy_volatility() {
    let mut fixture = scalar_fixture(1, 47);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    let root_expression = fixture.uses.flow().uses()[&ExpressionUseId::new(0)].definition;
    assert!(crate::expression::expressions_have_closed_value_scope(
        fixture.fragment.expressions(),
        [root_expression]
    ));
    let calls = fixture.checked().unwrap();
    run(&fixture, &calls, SOURCE, &FormulaControl::default()).unwrap();
    edit(&mut fixture, |parts| {
        let mut expression = parts.expressions.get(root_expression).unwrap().clone();
        expression.kind = ExprKind::Value(parts.nodes[&parts.root].output.columns[0]);
        parts.expressions.insert(expression);
    });
    let roots = PhysicalExpressionRoots::try_new(&fixture.fragment, &Control::default()).unwrap();
    let current = context(0, 0, EvaluationDemand::Value);
    let flow = ExpressionControlFlow::try_new(
        vec![domain(0)],
        vec![ExpressionInvocation {
            context: current,
            definition: root_expression,
            control: ControlShape::Eager,
            arguments: Box::default(),
        }],
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    fixture.uses = PhysicalRootUses::try_new(
        &fixture.fragment,
        flow,
        roots
            .sites()
            .keys()
            .map(|site| (*site, current.use_id))
            .collect(),
        &Control::default(),
    )
    .unwrap();
    fixture.calls.clear();
    assert!(!crate::expression::expressions_have_closed_value_scope(
        fixture.fragment.expressions(),
        [root_expression]
    ));
    let calls = fixture.checked().unwrap();
    assert!(matches!(
        run(&fixture, &calls, SOURCE, &FormulaControl::default()),
        Err(FragmentPropertyError::Structure(_))
    ));
}

#[test]
fn frozen_property_formulas_success_ordinary_and_exact_call_error_observe_all_original_tails() {
    let mut fixture = scalar_fixture(2, 53);
    broadcast(&mut fixture);
    let calls = fixture.checked().unwrap();
    prefixes(|control| run(&fixture, &calls, SOURCE, control), true, true);
    prefixes(|control| run(&fixture, &calls, 0, control), false, true);
    let mut malformed = fixture;
    edit(&mut malformed, |parts| {
        let value = parts.nodes[&parts.root].output.columns[0];
        parts
            .nodes
            .get_mut(&parts.root)
            .unwrap()
            .output_properties
            .ordering = Box::from([OrderingKey {
            value,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::First,
        }])
    });
    prefixes(
        |control| run(&malformed, &calls, SOURCE, control),
        false,
        true,
    );
    let mut unsafe_fixture = scalar_fixture(2, 59);
    broadcast(&mut unsafe_fixture);
    unsafe_fixture.calls[1].effects.observable_effects.warnings = true;
    let unsafe_calls = unsafe_fixture.checked().unwrap();
    prefixes(
        |control| run(&unsafe_fixture, &unsafe_calls, SOURCE, control),
        false,
        true,
    );
}

#[test]
fn frozen_property_formulas_real_wide_occurrences_keep_actual_quantum_and_sampled_primary_prefixes()
{
    let mut fixture = scalar_fixture(320, 61);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    let calls = fixture.checked().unwrap();
    let control = FormulaControl::default();
    run(&fixture, &calls, SOURCE, &control).unwrap();
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(
        |control| run(&fixture, &calls, SOURCE, control),
        true,
        false,
    );
}
