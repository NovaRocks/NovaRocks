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
use crate::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionArgumentEvaluation, FunctionFailureBehavior,
    FunctionIntrinsicRowError, FunctionVolatility, PureCompileControl,
};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[test]
fn structure_stage_keeps_actual_nested_loop_placement_before_effect_proof() {
    let make = |mode: NestLoopJoinDistribution, fault: u8| {
        let mut builder = empty_builder(0);
        builder
            .add_values(
                NodeId::new(1),
                Box::from([Box::<[crate::RootOrdinal]>::default()]),
                Box::default(),
            )
            .unwrap();
        let right = builder.nodes.get_mut(&NodeId::new(1)).unwrap();
        if mode == NestLoopJoinDistribution::BroadcastRight && fault != 1 {
            right.output_properties.distribution = Distribution::Broadcast;
            right.output_properties.row_multiplicity = RowMultiplicity::Replicated;
        }
        if fault == 2 {
            right.output_properties.row_multiplicity = RowMultiplicity::Replicated;
        }
        let required_right = right.output_properties.clone();
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: NodeId::new(u32::MAX),
                inputs: Box::from([NodeId::new(0), NodeId::new(1)]),
                required_inputs: Box::from([properties(), required_right]),
                output_properties: properties(),
                output: OutputPort {
                    node: NodeId::new(u32::MAX),
                    columns: Box::default(),
                },
                kind: NodeKind::NestLoopJoin {
                    kind: if fault == 3 {
                        JoinKind::RightOuter
                    } else {
                        JoinKind::Cross
                    },
                    distribution: mode,
                    predicate: None,
                    null_extended: Box::default(),
                },
            })
            .unwrap();
        builder
    };
    for mode in [
        NestLoopJoinDistribution::Singleton,
        NestLoopJoinDistribution::BroadcastRight,
    ] {
        finish(
            make(mode, 0),
            u32::MAX,
            PlanLimits::FROZEN,
            &Control::default(),
        )
        .unwrap();
        make(mode, 0)
            .finish_fixture_definition(
                NodeId::new(u32::MAX),
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap();
    }
    for (mode, fault) in [
        (NestLoopJoinDistribution::BroadcastRight, 1),
        (NestLoopJoinDistribution::Singleton, 2),
        (NestLoopJoinDistribution::BroadcastRight, 3),
    ] {
        reject_both(|| make(mode, fault), u32::MAX);
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original control failure");
        }
        trace.push((phase, units));
        if let Some((stop, cause)) = self.refusal.clone()
            && at == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn empty_builder(node: u32) -> FragmentBuilder {
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    builder
        .add_values(
            NodeId::new(node),
            Box::from([Box::<[crate::RootOrdinal]>::default()]),
            Box::default(),
        )
        .unwrap();
    builder
}
fn finish(
    builder: FragmentBuilder,
    root: u32,
    limits: PlanLimits,
    control: &Control,
) -> Result<Fragment, FragmentStructureError> {
    builder.finish_structure(
        NodeId::new(root),
        FragmentSink::Noop,
        dop(),
        limits,
        control,
        crate::StagePlacementPolicy::Singleton,
    )
}
fn source_function() -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("fixture/structure/scalar").unwrap(),
        overload: FunctionOverloadId::try_new("fixture/structure/zero-to-i64").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type: ValueType::new(DataType::Int64, false),
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    }
}
fn function_unit(
    function: BoundFunction,
) -> Result<novarocks_expression_contract::BoundExprSet, novarocks_expression_contract::UnitError> {
    use novarocks_expression_contract::{
        BoundExprSetBuilder, DefinitionControl, RootScope, UnitLimits,
    };
    use novarocks_type_contract::*;
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    });
    let call = draft.add(
        None,
        ValueType::new(DataType::Int64, false),
        ExprKind::FunctionCall {
            function,
            args: Box::default(),
        },
    )?;
    draft.root(call, EvaluationDemand::Value, RootScope::AllPorts)?;
    draft.request(
        call,
        PhysicalCallRequest {
            arguments: Box::default(),
            logical_argument_count: 0,
            expected_result_type: None,
            constant_policy: crate::tests::constant_fixture::policy(),
        },
    )?;
    draft.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )?;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
    let sealed = draft.seal(vec![], &mut work)?;
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, invocation)| {
            (
                id,
                novarocks_expression_contract::ExpressionCallClaim {
                    context: invocation.context,
                    effects: CallEffects {
                        value_stability: FunctionVolatility::Immutable,
                        own_row_error: FunctionIntrinsicRowError::NoRowError,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        null_behavior: FunctionNullBehavior::CalledOnNull,
                        argument_control: ArgumentControl::Eager,
                        instance_state: FunctionInstanceState::None,
                        observable_effects: ObservableEffects::NONE,
                        environment: Box::default(),
                        proof_scope: CallProofScope::Region(invocation.context.region),
                    },
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let result = sealed.finish(claims, &mut work);
    if matches!(
        result,
        Err(novarocks_expression_contract::UnitError::Control(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}
fn builder_with_unit(unit: novarocks_expression_contract::BoundExprSet) -> FragmentBuilder {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let node = NodeId::new(0);
    let value = builder
        .add_value(
            ValueType::new(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    // This is the receiving seam for an independently checked source unit.
    // Draft-only typed constructors cannot resolve a not-yet-installed unit.
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([crate::RootOrdinal::new(0)])]),
            },
            expressions: None,
        })
        .unwrap();
    builder
        .install_unit(node, std::sync::Arc::new(unit))
        .unwrap();
    builder
}
fn function_builder() -> FragmentBuilder {
    builder_with_unit(function_unit(source_function()).unwrap())
}
fn project_builder() -> FragmentBuilder {
    let mut builder = empty_builder(0);
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: NodeId::new(u32::MAX),
            inputs: Box::from([NodeId::new(0)]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: NodeId::new(u32::MAX),
                columns: Box::default(),
            },
            kind: NodeKind::Project {
                expressions: Box::default(),
            },
        })
        .unwrap();
    builder
}
fn reject_both(make: impl Fn() -> FragmentBuilder, root: u32) {
    assert!(matches!(
        finish(make(), root, PlanLimits::FROZEN, &Control::default()),
        Err(FragmentStructureError::Structure(_))
    ));
    assert!(
        make()
            .finish_fixture_definition(
                NodeId::new(root),
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton
            )
            .is_err()
    );
}
fn compare_prefixes(
    call: impl Fn(&Control) -> Result<Fragment, FragmentStructureError>,
    success: bool,
    all: bool,
) {
    let recording = Control::default();
    assert_eq!(call(&recording).is_ok(), success);
    let trace = recording.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for (at, (_, units)) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let refusing = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&refusing), Err(FragmentStructureError::Control(actual)) if actual == cause),
                "refusal at {at}: {cause:?}"
            );
            assert_eq!(*refusing.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn structure_stage_preserves_sparse_zero_max_and_original_one_empty_row() {
    for id in [0, u32::MAX] {
        let fragment = finish(
            empty_builder(id),
            id,
            PlanLimits::FROZEN,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(fragment.id().get(), u32::MAX);
        assert_eq!(fragment.root().get(), id);
        assert_eq!(fragment.nodes().len(), 1);
        let NodeKind::Values { rows } = &fragment.nodes()[&NodeId::new(id)].kind else {
            panic!("not Values")
        };
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_empty());
        validate_fragment_definition(&fragment).unwrap();
    }
}

#[test]
fn structure_stage_defers_property_derivation_but_definition_and_package_still_reject() {
    let make = || {
        let mut builder = project_builder();
        builder
            .nodes
            .get_mut(&NodeId::new(u32::MAX))
            .unwrap()
            .output_properties
            .distribution = Distribution::RoundRobin;
        builder
    };
    let fragment = finish(make(), u32::MAX, PlanLimits::FROZEN, &Control::default()).unwrap();
    assert_eq!(
        fragment.nodes()[&fragment.root()]
            .output_properties
            .distribution,
        Distribution::RoundRobin
    );
    assert!(
        make()
            .finish_fixture_definition(
                NodeId::new(u32::MAX),
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton
            )
            .is_err()
    );
    let setup = Control::default();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &setup).unwrap();
    let input = FragmentPackageInput {
        constants: ConstantPools::empty(),
        version: PlanVersionId::try_new([9; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment,
        statement_constant_policy: crate::tests::constant_fixture::policy(),
        pruning,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    };
    assert!(FragmentPackage::try_new(input, package_admission(), &Control::default()).is_err());
}

#[test]
fn structure_stage_keeps_reference_type_owner_and_both_dag_rejection() {
    for fault in [0, 1, 4, 5] {
        reject_both(
            || {
                let mut builder = function_builder();
                match fault {
                    0 => {
                        let NodeKind::Values { rows } =
                            &mut builder.nodes.get_mut(&NodeId::new(0)).unwrap().kind
                        else {
                            unreachable!()
                        };
                        rows[0][0] = crate::RootOrdinal::new(u32::MAX);
                    }
                    1 => {
                        builder.values.get_mut(&ValueId::new(0)).unwrap().ty =
                            ValueType::new(DataType::Boolean, false);
                    }
                    4 => {
                        let node = builder.nodes.get_mut(&NodeId::new(0)).unwrap();
                        node.inputs = Box::from([node.id]);
                        node.required_inputs = Box::from([properties()]);
                    }
                    5 => {
                        builder.values.get_mut(&ValueId::new(0)).unwrap().origin =
                            ValueOrigin::NodeOutput {
                                node: NodeId::new(0),
                                output_ordinal: 1,
                            }
                    }
                    _ => unreachable!(),
                }
                builder
            },
            0,
        );
    }
}

#[test]
fn structure_stage_keeps_property_keys_requirements_and_replica_shape() {
    for fault in 0..4 {
        reject_both(
            || {
                let mut builder = project_builder();
                let root = builder.nodes.get_mut(&NodeId::new(u32::MAX)).unwrap();
                match fault {
                    0 => {
                        root.output_properties.ordering = Box::from([OrderingKey {
                            value: ValueId::new(u32::MAX),
                            direction: SortDirection::Ascending,
                            null_ordering: NullOrdering::First,
                        }])
                    }
                    1 => root.required_inputs[0].distribution = Distribution::RoundRobin,
                    2 => root.output_properties.distribution = Distribution::Broadcast,
                    3 => root.output.columns = Box::from([ValueId::new(u32::MAX)]),
                    _ => unreachable!(),
                }
                builder
            },
            u32::MAX,
        );
    }
}

#[test]
fn structure_stage_consumes_sealed_effects_without_legacy_intrinsic_authority() {
    let mut function = source_function();
    function
        .legacy_metadata
        .as_mut()
        .unwrap()
        .intrinsic_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    let make = || builder_with_unit(function_unit(function.clone()).unwrap());
    finish(make(), 0, PlanLimits::FROZEN, &Control::default()).unwrap();
    make()
        .finish_fixture_definition(
            NodeId::new(0),
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    for kind_or_signature in 0..2 {
        let mut wrong = function.clone();
        if kind_or_signature == 0 {
            wrong.kind = FunctionKind::Table;
        } else {
            wrong.argument_types =
                novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                    FunctionArgumentType::Value(ValueType::new(DataType::Int64, false)),
                ]));
        }
        assert!(function_unit(wrong).is_err());
    }
}

#[test]
fn structure_stage_honors_explicit_count_limits_without_defaulting_zero() {
    let exact = PlanLimits {
        fragment_nodes: 1,
        fragment_values: 1,
        fragment_expressions: 1,
        ..PlanLimits::FROZEN
    };
    finish(function_builder(), 0, exact, &Control::default()).unwrap();
    for limits in [
        PlanLimits {
            fragment_nodes: 0,
            ..exact
        },
        PlanLimits {
            fragment_values: 0,
            ..exact
        },
        PlanLimits {
            fragment_expressions: 0,
            ..exact
        },
    ] {
        assert!(matches!(
            finish(function_builder(), 0, limits, &Control::default()),
            Err(FragmentStructureError::Structure(_))
        ));
    }
}

#[test]
fn structure_stage_original_controls_cover_success_ordinary_tail_and_runtime_filter_quantum() {
    compare_prefixes(
        |control| finish(function_builder(), 0, PlanLimits::FROZEN, control),
        true,
        true,
    );
    compare_prefixes(
        |control| {
            let mut builder = function_builder();
            builder
                .nodes
                .get_mut(&NodeId::new(0))
                .unwrap()
                .output
                .columns = Box::from([ValueId::new(u32::MAX)]);
            finish(builder, 0, PlanLimits::FROZEN, control)
        },
        false,
        true,
    );
    let wide = |control: &Control| {
        let mut builder = empty_builder(u32::MAX);
        for id in 0..320 {
            builder
                .attach_runtime_filter(RuntimeFilterId::new(id))
                .unwrap();
        }
        finish(builder, u32::MAX, PlanLimits::FROZEN, control)
    };
    let good = Control::default();
    let fragment = wide(&good).unwrap();
    assert_eq!(fragment.runtime_filters().len(), 320);
    let with_filter_limit = |limit| {
        let mut builder = empty_builder(u32::MAX);
        for id in 0..320 {
            builder
                .attach_runtime_filter(RuntimeFilterId::new(id))
                .unwrap();
        }
        finish(
            builder,
            u32::MAX,
            PlanLimits {
                plan_runtime_filters: limit,
                ..PlanLimits::FROZEN
            },
            &Control::default(),
        )
    };
    with_filter_limit(320).unwrap();
    assert!(matches!(
        with_filter_limit(319),
        Err(FragmentStructureError::Structure(_))
    ));
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    compare_prefixes(wide, true, false);
}

#[test]
fn structure_signature_correspondence_ignores_only_legacy_effects_and_keeps_full_types() {
    let original = &source_function();
    let mut legacy_changed = original.clone();
    legacy_changed.legacy_metadata.as_mut().unwrap().volatility = FunctionVolatility::Volatile;
    legacy_changed
        .legacy_metadata
        .as_mut()
        .unwrap()
        .argument_evaluation = FunctionArgumentEvaluation::ShortCircuit;
    legacy_changed
        .legacy_metadata
        .as_mut()
        .unwrap()
        .failure_behavior = FunctionFailureBehavior::ReturnsNull;
    legacy_changed
        .legacy_metadata
        .as_mut()
        .unwrap()
        .intrinsic_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    legacy_changed
        .legacy_metadata
        .as_mut()
        .unwrap()
        .semantic_parameters = Box::from([novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
        expected_key: novarocks_type_contract::SemanticParameterKey::TimeZone,
    }]);
    assert!(original.signature_matches(&legacy_changed));
    assert_ne!(original, &legacy_changed);
    for dimension in 0..6 {
        let mut changed = legacy_changed.clone();
        match dimension {
            0 => changed.function_id = FunctionId::try_new("fixture/structure/other").unwrap(),
            1 => {
                changed.overload =
                    FunctionOverloadId::try_new("fixture/structure/other-overload").unwrap()
            }
            2 => changed.kind = FunctionKind::Window,
            3 => {
                changed.argument_types =
                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                        FunctionArgumentType::Value(ValueType::new(DataType::Int64, false)),
                    ]))
            }
            4 => {
                changed.result_type =
                    ValueType::new(DataType::Boolean, changed.result_type.nullable())
            }
            5 => changed.result_type = changed.result_type.with_nullable(true).unwrap(),
            _ => unreachable!(),
        }
        assert!(
            !original.signature_matches(&changed),
            "signature dimension {dimension}"
        );
    }
    let mut nominal = original.clone();
    nominal.result_type = ValueType::new(DataType::LargeBinary, true);
    let mut json = nominal.clone();
    json.result_type = ValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    assert!(!nominal.signature_matches(&json));

    #[allow(deprecated)]
    let dictionary_field = arrow_schema::Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        0,
        false,
    );
    let mut nested = original.clone();
    nested.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
        FunctionArgumentType::Value(ValueType::new(
            DataType::Struct(vec![std::sync::Arc::new(dictionary_field.clone())].into()),
            false,
        )),
    ]));
    for change in 0..3 {
        #[allow(deprecated)]
        let field = match change {
            0 => arrow_schema::Field::new_dict(
                "dictionary",
                dictionary_field.data_type().clone(),
                true,
                i64::MAX,
                false,
            ),
            1 => arrow_schema::Field::new_dict(
                "dictionary",
                dictionary_field.data_type().clone(),
                true,
                0,
                true,
            ),
            2 => dictionary_field.clone().with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "source".to_owned(),
                    "different".to_owned(),
                )]),
            ),
            _ => unreachable!(),
        };
        let mut changed = nested.clone();
        changed.argument_types =
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(ValueType::new(
                    DataType::Struct(vec![std::sync::Arc::new(field)].into()),
                    false,
                )),
            ]));
        assert!(
            nested.signature_matches(&changed),
            "runtime encoding/provider metadata do not change semantic signature {change}"
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
fn exact_signature_keeps_full_types_and_sealed_calls_supply_effect_authority() {
    let source = source_function();
    let exact = BoundFunction::from_exact_signature(
        source.function_id.clone(),
        source.overload.clone(),
        source.kind,
        source.argument_types.clone(),
        source.result_type.clone(),
    );
    assert!(exact.legacy_metadata.is_none());
    let make = || builder_with_unit(function_unit(exact.clone()).unwrap());
    finish(make(), 0, PlanLimits::FROZEN, &Control::default()).unwrap();
    make()
        .finish_fixture_definition(
            NodeId::new(0),
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    for fault in [1, 2] {
        let mut wrong = exact.clone();
        if fault == 1 {
            wrong.kind = FunctionKind::Window;
        } else {
            wrong.result_type = ValueType::new(
                DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
                    "element",
                    DataType::Int64,
                    false,
                ))),
                false,
            );
        }
        assert!(function_unit(wrong).is_err());
    }
}
