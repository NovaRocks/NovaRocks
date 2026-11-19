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
    source: usize,
    control: &FormulaControl,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    validate_fragment_output_properties_observed(
        &fixture.fragment,
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
    legacy_scalar_volatile(fixture);
    edit(fixture, |parts| {
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
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, u32::MAX, &mut constants);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    // The full local gate now consumes the installed claims as well.
    validate_fragment_definition(&fixture.fragment).unwrap();
    assert_eq!(claim(&fixture, 0).context.use_id, use_id(0));
    assert_eq!(claim(&fixture, 1).context.use_id, use_id(1));
    assert_ne!(
        claim(&fixture, 0).context.region,
        claim(&fixture, 1).context.region
    );
    fixture.edit_unit(|parts| {
        for call in parts.claims.values_mut() {
            call.effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
            call.effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        }
    });
    let original = fixture.claims().clone();
    let facts = run(&fixture, SOURCE, &FormulaControl::default()).unwrap();
    assert!(facts.request_bytes > 0);
    assert_eq!(fixture.claims(), &original);
    for call in fixture.claims().values() {
        assert_eq!(
            call.effects.proof_scope,
            CallProofScope::Region(call.context.region)
        );
        assert_eq!(
            call.effects.own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
        assert_eq!(
            call.effects.failure_behavior,
            FunctionFailureBehavior::ReturnsNull
        );
    }
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()]
            .output_properties
            .distribution,
        Distribution::Broadcast
    );
}

#[test]
fn frozen_property_formulas_reject_each_sparse_unsafe_use_with_exact_global_replica_site() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    for at in 0..2 {
        for mode in 0..4 {
            let mut fixture = scalar_fixture(2, 23, &mut constants);
            broadcast(&mut fixture);
            edit_claim(&mut fixture, at, |call| match mode {
                0 => call.effects.instance_state = FunctionInstanceState::ScalarInstance,
                1 => call.effects.observable_effects.warnings = true,
                2 => call.effects.observable_effects.rng_sampling = true,
                3 => call.effects.value_stability = FunctionVolatility::Stable,
                _ => unreachable!(),
            });
            assert!(
                matches!(run(&fixture,SOURCE,&FormulaControl::default()),Err(FragmentPropertyError::Calls(actual)) if actual==expression_failure(&fixture,at))
            );
        }
    }
}

#[test]
fn frozen_property_formulas_single_copy_does_not_impose_replication_or_drop_effects() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 29, &mut constants);
    edit_claim(&mut fixture, 0, |c| {
        c.effects.instance_state = FunctionInstanceState::ScalarInstance;
        c.effects.observable_effects.rng_sampling = true;
    });
    edit_claim(&mut fixture, 1, |c| {
        c.effects.value_stability = FunctionVolatility::Volatile;
        c.effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    });
    let original = fixture.claims().clone();
    run(&fixture, SOURCE, &FormulaControl::default()).unwrap();
    assert_eq!(fixture.claims(), &original);
    assert_eq!(
        claim(&fixture, 0).effects.instance_state,
        FunctionInstanceState::ScalarInstance
    );
    assert_eq!(
        fixture.fragment.nodes()[&fixture.fragment.root()].output_properties,
        properties()
    );
}

fn table_source(
    scalar_input: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    use novarocks_expression_contract::{BoundExprSetBuilder, ExprPort, RootScope};
    let mut builder = FragmentBuilder::new(FragmentId::new(37));
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
            .add(None, input_type.clone(), ExprKind::Value(input_value))
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

fn table_fixture(constants: &mut crate::tests::constant_fixture::ConstantFixture) -> Fixture {
    table_source(false, constants)
}

#[test]
fn frozen_property_formulas_canonical_relational_lifecycle_is_not_scalar_state() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = table_fixture(&mut constants);
    legacy_volatile(&mut fixture);
    validate_fragment_definition(&fixture.fragment).unwrap();
    let site = *fixture.relations().keys().next().unwrap();
    assert_eq!(
        fixture.relations()[&site].effects.instance_state,
        FunctionInstanceState::TableInstance
    );
    run(&fixture, SOURCE, &FormulaControl::default()).unwrap();
    edit_relation(&mut fixture, site, |c| {
        c.effects.observable_effects.warnings = true
    });
    assert!(
        matches!(run(&fixture,SOURCE,&FormulaControl::default()),Err(FragmentPropertyError::Calls(FrozenCallError::ReplicaEquivalence(actual))) if actual==site)
    );
    let relational = special_fixture(&mut constants);
    run(&relational, SOURCE, &FormulaControl::default()).unwrap();
    assert!(
        relational
            .relations()
            .values()
            .any(|c| c.effects.instance_state == FunctionInstanceState::AggregateInstance)
    );
    assert!(
        relational
            .relations()
            .values()
            .any(|c| c.effects.instance_state == FunctionInstanceState::WindowPartition)
    );
}

#[test]
fn frozen_property_formulas_keep_required_inputs_ordering_and_geometry_rejections() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for bad in 0..3 {
        let mut fixture = table_fixture(&mut fixture_constants);
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
        assert!(
            matches!(
                run(&fixture, SOURCE, &FormulaControl::default()),
                Err(FragmentPropertyError::Structure(_))
            ),
            "malformed shape {bad}"
        );
    }
    let mut fixture = scalar_fixture(1, 43, &mut fixture_constants);
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
    assert!(matches!(
        run(&fixture, SOURCE, &FormulaControl::default()),
        Err(FragmentPropertyError::Structure(_))
    ));
}

fn broadcast_input_read_fixture(
    constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    use novarocks_expression_contract::{BoundExprSetBuilder, ExprPort, RootScope};
    let mut builder = FragmentBuilder::new(FragmentId::new(47));
    let input = add_values(&mut builder, 1, false, constants);
    let value = ValueId::new(0);
    let node = builder.reserve_node_id().unwrap();
    let mut draft = BoundExprSetBuilder::new(unit_limits());
    let def = draft.add(None, integer(), ExprKind::Value(value)).unwrap();
    // Value leaves use their intrinsic Eager flow; no function control record.
    let root = draft
        .root(def, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let output = builder
        .add_value(integer(), ValueOrigin::Expr { node, root })
        .unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = draft
        .seal(
            vec![ExprPort {
                values: Box::from([(value, integer())]),
            }],
            &mut work,
        )
        .unwrap();
    let unit = sealed.finish(BTreeMap::new(), &mut work).unwrap();
    work.finish().unwrap();
    install_node(
        &mut builder,
        node,
        vec![input],
        vec![output],
        NodeKind::Project {
            expressions: Box::from([(root, output)]),
        },
    );
    builder.install_unit(node, Arc::new(unit)).unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap()
        .with_calls_observed(vec![], &Control::default())
        .unwrap();
    let mut fixture = Fixture {
        fragment,
        primary: input,
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
fn frozen_property_formulas_keep_closed_values_scope_separate_from_legacy_volatility() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, 47, &mut constants);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    assert!(crate::expression::expressions_have_closed_value_scope(
        fixture.unit(),
        [RootOrdinal::new(0)]
    ));
    run(&fixture, SOURCE, &FormulaControl::default()).unwrap();
    // A checked Value definition requires an actual declared port; the old
    // self-output Values injection belongs to the unit gate (see owner candidate).
    let mut input_read = broadcast_input_read_fixture(&mut constants);
    let unit = input_read
        .fragment
        .unit(input_read.fragment.root())
        .unwrap();
    assert!(!crate::expression::expressions_have_closed_value_scope(
        unit,
        [RootOrdinal::new(0)]
    ));
    run(&input_read, SOURCE, &FormulaControl::default()).unwrap();
    // Reusing that valid input-reading unit on the zero-input Values source is
    // independently rejected by the node-port correspondence author.
    let borrowed = Arc::clone(
        input_read.fragment.nodes()[&input_read.fragment.root()]
            .expressions
            .as_ref()
            .unwrap(),
    );
    let source_node = input_read.primary;
    edit(&mut input_read, |parts| {
        parts.nodes.get_mut(&source_node).unwrap().expressions = Some(borrowed)
    });
    assert!(matches!(
        run(&input_read, SOURCE, &FormulaControl::default()),
        Err(FragmentPropertyError::Structure(_))
    ));
}

#[test]
fn frozen_property_formulas_success_ordinary_and_exact_call_error_observe_all_original_tails() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 53, &mut fixture_constants);
    broadcast(&mut fixture);
    prefixes(|control| run(&fixture, SOURCE, control), true, true);
    prefixes(|control| run(&fixture, 0, control), false, true);
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
    prefixes(|control| run(&malformed, SOURCE, control), false, true);
    let mut unsafe_fixture = scalar_fixture(2, 59, &mut fixture_constants);
    broadcast(&mut unsafe_fixture);
    edit_claim(&mut unsafe_fixture, 1, |c| {
        c.effects.observable_effects.warnings = true
    });
    prefixes(|control| run(&unsafe_fixture, SOURCE, control), false, true);
}

#[test]
fn frozen_property_formulas_real_wide_occurrences_keep_actual_quantum_and_sampled_primary_prefixes()
{
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(320, 61, &mut fixture_constants);
    broadcast(&mut fixture);
    legacy_volatile(&mut fixture);
    let control = FormulaControl::default();
    run(&fixture, SOURCE, &control).unwrap();
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(|control| run(&fixture, SOURCE, control), true, false);
}
