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

use arrow_schema::DataType;
use novarocks_expression_contract::{ExprKind, ExprPort, RootScope, UnaryOperator};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Arc};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn boolean(nullable: bool) -> ValueType {
    ValueType::new(DataType::Boolean, nullable)
}
fn finish(builder: FragmentBuilder, node: NodeId) -> Result<Fragment, ValidationErrors> {
    builder.finish_definition(
        node,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        StagePlacementPolicy::Singleton,
    )
}
fn source(builder: &mut FragmentBuilder, node: NodeId, types: &[ValueType]) -> Vec<ValueId> {
    let values = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    // This contract fixture is an empty relation with its actual typed output.
    builder
        .add_values(node, Box::default(), values.clone().into_boxed_slice())
        .unwrap();
    values
}
fn install(builder: &mut FragmentBuilder, node: NodeId, ports: Vec<ExprPort>) {
    let draft = builder.take_unit_builder(node).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let sealed = draft.seal(ports, &mut work).unwrap();
    let unit = sealed.finish(BTreeMap::new(), &mut work).unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}
fn filter(demand: EvaluationDemand, scope: RootScope) -> (FragmentBuilder, NodeId, ValueId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let source_node = builder.reserve_node_id().unwrap();
    let value = source(&mut builder, source_node, &[boolean(false)])[0];
    let node = builder.reserve_node_id().unwrap();
    let draft = builder.unit(node).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(value))
        .unwrap();
    let root = draft.root(definition, demand, scope).unwrap();
    builder
        .add_filter(node, source_node, Box::new([root]))
        .unwrap();
    (builder, node, value)
}

#[derive(Clone, Copy)]
enum AggregateFilterDefinition {
    BooleanInput,
    ComputedBoolean,
    IntegerInput,
    WrongScope,
}

fn aggregate_filter(
    phase: AggregatePhase,
    predicate: AggregateFilterDefinition,
) -> Result<Fragment, FragmentStructureError> {
    use novarocks_expression_contract::BoundFunction;
    use novarocks_type_contract::{AggregateStateArgumentContract, TypedFactSlice};

    let mut builder = FragmentBuilder::new(FragmentId::new(773));
    let input = builder.reserve_node_id().unwrap();
    let value_type = ValueType::new(DataType::Int64, false);
    let state_type = ValueType::new(DataType::Binary, false);
    let argument_type = if phase.consumes_logical_arguments() {
        value_type.clone()
    } else {
        state_type.clone()
    };
    let filter_type = match predicate {
        AggregateFilterDefinition::IntegerInput => value_type.clone(),
        _ => boolean(true),
    };
    let input_values = source(
        &mut builder,
        input,
        &[filter_type.clone(), argument_type.clone()],
    );
    let node = builder.reserve_node_id().unwrap();
    let draft = builder.unit(node).unwrap();
    let filter_input = draft
        .add(None, filter_type.clone(), ExprKind::Value(input_values[0]))
        .unwrap();
    let filter_definition = match predicate {
        AggregateFilterDefinition::ComputedBoolean => draft
            .add(
                None,
                filter_type,
                ExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: filter_input,
                },
            )
            .unwrap(),
        _ => filter_input,
    };
    let filter_demand = match predicate {
        // The unit itself rejects non-Boolean TruthOnly roots. Seal a valid
        // Value root here so the aggregate field checks the wrong source.
        AggregateFilterDefinition::IntegerInput => EvaluationDemand::Value,
        _ => EvaluationDemand::TruthOnly,
    };
    let filter_scope = match predicate {
        AggregateFilterDefinition::WrongScope => RootScope::AllPorts,
        _ => RootScope::Port(0),
    };
    let filter_root = draft
        .root(filter_definition, filter_demand, filter_scope)
        .unwrap();
    let argument = draft
        .add(None, argument_type, ExprKind::Value(input_values[1]))
        .unwrap();
    let argument_root = draft
        .root(argument, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let call = AggregateCallId::new(0);
    let (output_type, origin, grouping) = if phase.produces_final_result() {
        (
            value_type.clone(),
            ValueOrigin::AggregateResult { call },
            AggregateGrouping::Complete,
        )
    } else {
        (
            state_type.clone(),
            ValueOrigin::AggregateState { call, phase },
            AggregateGrouping::Partial,
        )
    };
    let output = builder.add_value(output_type, origin).unwrap();
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::new([input]),
            required_inputs: Box::new([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::new([output]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: Box::new([AggregateCall {
                    id: call,
                    binding: AggregateBinding {
                        function: BoundFunction::from_exact_signature(
                            FunctionId::try_new("fixture/aggregate/count").unwrap(),
                            FunctionOverloadId::try_new("fixture/aggregate/count/i64").unwrap(),
                            FunctionKind::Aggregate,
                            TypedFactSlice::from_pure_boxed(Box::new([
                                FunctionArgumentType::Value(value_type.clone()),
                            ])),
                            value_type,
                        ),
                        phase,
                        logical_argument_count: 1,
                        intermediate_type: state_type,
                        state_format: AggregateStateFormatId::try_new("fixture/aggregate/state")
                            .unwrap(),
                        state_argument_contract: AggregateStateArgumentContract::ExactSignature,
                        state_interpretation: None,
                    },
                    arguments: Box::new([argument_root]),
                    distinct: false,
                    order_by: Box::default(),
                    filter_root: Some(filter_root),
                    output,
                }]),
                grouping,
            },
            expressions: None,
        })
        .unwrap();
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    builder.finish_structure(
        node,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        PlanLimits::FROZEN,
        &Control,
        StagePlacementPolicy::Singleton,
    )
}

#[test]
fn aggregate_filter_root_reads_the_original_boolean_input_only_on_update() {
    let sequence = AggregateSequenceId::new(0);
    for phase in [AggregatePhase::Single, AggregatePhase::Partial { sequence }] {
        let fragment = aggregate_filter(phase, AggregateFilterDefinition::BooleanInput).unwrap();
        let node = &fragment.nodes()[&fragment.root()];
        let NodeKind::Aggregate { calls, .. } = &node.kind else {
            panic!("expected aggregate update");
        };
        let root = calls[0].filter_root.unwrap();
        assert_eq!(root.get(), 0);
        let expression = fragment.root_expression(node.id, root).unwrap();
        let filter_value = fragment.nodes()[&node.inputs[0]].output.columns[0];
        assert!(matches!(expression.kind, ExprKind::Value(value) if value == filter_value));
        assert_eq!(expression.ty, boolean(true));
    }
    for phase in [
        AggregatePhase::Intermediate { sequence },
        AggregatePhase::Final { sequence },
    ] {
        let error = aggregate_filter(phase, AggregateFilterDefinition::BooleanInput).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("state-consuming aggregate phase cannot apply FILTER")
        );
    }
}

#[test]
fn aggregate_filter_field_rejects_computed_nonboolean_and_out_of_scope_roots() {
    for (predicate, diagnostic) in [
        (
            AggregateFilterDefinition::ComputedBoolean,
            "aggregate FILTER must directly reference a BOOLEAN input value",
        ),
        (
            AggregateFilterDefinition::IntegerInput,
            "aggregate FILTER must directly reference a BOOLEAN input value",
        ),
        (
            AggregateFilterDefinition::WrongScope,
            "unit root demand or scope differs from its node field",
        ),
    ] {
        let error = aggregate_filter(AggregatePhase::Single, predicate).unwrap_err();
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
}

// Public predicate responsibilities use the actual immutable node unit.
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::Mutex;

fn predicate_fragment(
    depth: usize,
    wrappers: Option<bool>,
    roots: usize,
) -> (Fragment, PredicateSourceSite, ExprId) {
    let mut b = FragmentBuilder::new(FragmentId::new(771));
    let input = b.reserve_node_id().unwrap();
    let value = source(&mut b, input, &[boolean(true)])[0];
    let node = b.reserve_node_id().unwrap();
    let draft = b.unit(node).unwrap();
    let leaf = draft
        .add(None, boolean(true), ExprKind::Value(value))
        .unwrap();
    let definition = if let Some(null_test) = wrappers {
        draft
            .add(
                None,
                boolean(!null_test),
                if null_test {
                    ExprKind::IsNull {
                        expr: leaf,
                        negated: false,
                    }
                } else {
                    ExprKind::Unary {
                        op: UnaryOperator::Not,
                        expr: leaf,
                    }
                },
            )
            .unwrap()
    } else {
        let mut parent = leaf;
        for _ in 1..depth {
            parent = draft
                .add(
                    None,
                    boolean(true),
                    ExprKind::Conjunction {
                        args: Box::new([parent, leaf]),
                    },
                )
                .unwrap();
        }
        parent
    };
    let ordinals = (0..roots)
        .map(|_| {
            draft
                .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let root = *ordinals.last().unwrap();
    b.add_filter(node, input, ordinals.into_boxed_slice())
        .unwrap();
    let ports = b.node_ports(node).unwrap();
    install(&mut b, node, ports);
    (
        finish(b, node).unwrap(),
        PredicateSourceSite { node, root },
        leaf,
    )
}

#[test]
fn positive_sources_keep_nullable_truth_uses_and_original_root_responsibility() {
    let (fragment, site, leaf) = predicate_fragment(3, None, 1);
    let unit = fragment.unit(site.node).unwrap();
    let root_use = unit.root_use(site.root).unwrap();
    let identity = PredicateConjunctSource::try_new(&fragment, site, vec![], &Control).unwrap();
    let nested = PredicateConjunctSource::try_new(&fragment, site, vec![0, 1], &Control).unwrap();
    let repeated = PredicateConjunctSource::try_new(&fragment, site, vec![1], &Control).unwrap();
    assert_eq!(identity.context().use_id, root_use);
    assert_eq!(nested.definition(), leaf);
    assert_eq!(repeated.definition(), leaf);
    assert_ne!(nested.context().use_id, repeated.context().use_id);
    assert_eq!(nested.context().demand, EvaluationDemand::TruthOnly);
    assert!(unit.definition(leaf).unwrap().ty.nullable());
    assert_eq!(nested.argument_ordinals(), &[0, 1]);
    assert_eq!(
        nested.responsibility().anchor(),
        PredicateResponsibilityRef {
            fragment: fragment.id(),
            site,
            use_id: root_use
        }
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, site, vec![2], &Control).unwrap_err(),
        PredicateSourceError::InvalidArgument
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, site, vec![1, 0], &Control).unwrap_err(),
        PredicateSourceError::NotPositiveConjunction
    );
    assert_eq!(
        PredicateConjunctSource::try_new(
            &fragment,
            site,
            vec![0; novarocks_type_contract::MAX_CONTROL_DEPTH],
            &Control
        )
        .unwrap_err(),
        PredicateSourceError::TooDeep
    );
}

#[test]
fn positive_sources_do_not_cross_not_or_null_testing() {
    for wrapper in [false, true] {
        let (fragment, site, _) = predicate_fragment(1, Some(wrapper), 1);
        assert_eq!(
            PredicateConjunctSource::try_new(&fragment, site, vec![0], &Control).unwrap_err(),
            PredicateSourceError::NotPositiveConjunction
        );
    }
}

#[test]
fn truth_only_change_condition_is_not_a_relation_search_predicate() {
    let mut b = FragmentBuilder::new(FragmentId::new(772));
    let input = b.reserve_node_id().unwrap();
    let value = source(&mut b, input, &[boolean(true)])[0];
    let node = b.reserve_node_id().unwrap();
    let draft = b.unit(node).unwrap();
    let leaf = draft
        .add(None, boolean(true), ExprKind::Value(value))
        .unwrap();
    let root = draft
        .root(leaf, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let effect = b
        .add_value(
            ValueType::new(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    b.insert_node_unchecked(PhysicalNode {
        id: node,
        inputs: Box::new([input]),
        required_inputs: Box::new([PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }]),
        output_properties: PhysicalProperties {
            distribution: Distribution::Unconstrained,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        },
        output: OutputPort {
            node,
            columns: Box::new([effect]),
        },
        kind: NodeKind::ChangeEventExpand {
            events: Box::new([ChangeEventSpec {
                predicate: Some(root),
                effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                assignments: Box::default(),
            }]),
            effect_output: effect,
        },
        expressions: None,
    })
    .unwrap();
    let ports = b.node_ports(node).unwrap();
    install(&mut b, node, ports);
    let fragment = finish(b, node).unwrap();
    assert_eq!(
        fragment.unit(node).unwrap().root(root).unwrap().demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(
        ExactPredicateResponsibility::try_new(
            &fragment,
            PredicateSourceSite { node, root },
            &Control
        )
        .unwrap_err(),
        PredicateSourceError::NotSearchPredicate
    );
}

struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl Trace {
    fn new(fail: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::default(),
            fail,
        }
    }
    fn calls(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push((phase, units));
        match &self.fail {
            Some((at, cause)) if *at == index => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
fn path_attempt(
    fragment: &Fragment,
    site: PredicateSourceSite,
    path: Vec<u32>,
    control: &Trace,
) -> Result<PredicateConjunctSource, PredicateSourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = PredicateConjunctSource::try_new_in(fragment, site, path, &mut work);
    if matches!(result, Err(PredicateSourceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
#[test]
fn borrowed_predicate_source_success_and_ordinary_errors_keep_every_caller_prefix() {
    let (fragment, site, _) = predicate_fragment(3, None, 1);
    for (path, expected) in [
        (vec![0, 1], None),
        (vec![2], Some(PredicateSourceError::InvalidArgument)),
        (
            vec![1, 0],
            Some(PredicateSourceError::NotPositiveConjunction),
        ),
    ] {
        let good = Trace::new(None);
        let baseline = path_attempt(&fragment, site, path.clone(), &good);
        match expected {
            Some(error) => assert_eq!(baseline.unwrap_err(), error),
            None => {
                baseline.unwrap();
            }
        }
        let trace = good.calls();
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        assert!(trace.len() > 1);
        assert!(
            trace
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Decode)
        );
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusing = Trace::new(Some((at, cause.clone())));
                assert_eq!(
                    path_attempt(&fragment, site, path.clone(), &refusing).unwrap_err(),
                    PredicateSourceError::Control(cause)
                );
                assert_eq!(refusing.calls(), trace[..=at]);
            }
        }
    }
}
#[test]
fn positive_source_depth_keeps_the_original_invocation_bound() {
    let depth = novarocks_type_contract::MAX_CONTROL_DEPTH;
    let (fragment, site, leaf) = predicate_fragment(depth, None, 1);
    let near =
        PredicateConjunctSource::try_new(&fragment, site, vec![0; depth - 1], &Control).unwrap();
    assert_eq!(near.definition(), leaf);
    assert!(
        fragment
            .unit(site.node)
            .unwrap()
            .definition(leaf)
            .unwrap()
            .ty
            .nullable()
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, site, vec![0; depth], &Control).unwrap_err(),
        PredicateSourceError::TooDeep
    );
}
#[test]
fn wide_filter_responsibility_observes_actual_field_membership_work() {
    let (fragment, site, _) = predicate_fragment(1, None, 512);
    let run = |control: &Trace| -> Result<ExactPredicateResponsibility, PredicateSourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = ExactPredicateResponsibility::try_new_in(&fragment, site, &mut work);
        if matches!(result, Err(PredicateSourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let good = Trace::new(None);
    run(&good).unwrap();
    let trace = good.calls();
    assert!(
        trace.contains(&(CompilePhase::Decode, 256)),
        "actual membership scans must not hide one wide source traversal"
    );
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Trace::new(Some((at, cause.clone())));
            assert_eq!(
                run(&refusing).unwrap_err(),
                PredicateSourceError::Control(cause)
            );
            assert_eq!(refusing.calls(), trace[..=at]);
        }
    }
}
#[test]
fn installed_unit_keeps_the_exact_source_arc_and_original_hold_after_fragment_drop() {
    struct Hold;
    impl ValueTypeStorageHold for Hold {}
    let hold = Arc::new(Hold);
    let weak = Arc::downgrade(&hold);
    let (mut b, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let ports = b.node_ports(node).unwrap();
    let draft = b
        .take_unit_builder(node)
        .unwrap()
        .with_storage_lifetime(ValueTypeStorageLifetime::held(hold.clone()))
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = Arc::new(
        draft
            .seal(ports, &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap(),
    );
    work.finish().unwrap();
    let original = Arc::clone(&unit);
    b.install_unit(node, unit).unwrap();
    let fragment = finish(b, node).unwrap();
    assert!(Arc::ptr_eq(
        &original,
        fragment.nodes()[&node].expressions.as_ref().unwrap()
    ));
    let late = Arc::clone(fragment.nodes()[&node].expressions.as_ref().unwrap());
    drop(original);
    drop(fragment);
    drop(hold);
    assert!(weak.upgrade().is_some());
    assert_eq!(late.root_use_ids().len(), 1);
    drop(late);
    assert!(weak.upgrade().is_none());
}
