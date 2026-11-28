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
use novarocks_expression_contract::{ExprKind, ExprPort, RootScope};
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
fn rejects_with(builder: FragmentBuilder, node: NodeId, message: &str) {
    let errors = finish(builder, node).unwrap_err();
    assert!(
        errors
            .errors()
            .iter()
            .any(|error| error.message().contains(message)),
        "{errors}"
    );
}
#[test]
fn filter_unit_keeps_exact_input_root_and_full_type() {
    let (mut builder, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    let fragment = finish(builder, node).unwrap();
    let unit = fragment.unit(node).unwrap();
    assert_eq!(unit.roots().len(), 1);
    assert_eq!(unit.root_use_ids().len(), 1);
    assert_eq!(unit.ports()[0].values[0].1, boolean(false));
}
#[test]
fn fragment_rejects_value_demand_for_filter_truth_field() {
    let (mut builder, node, _) = filter(EvaluationDemand::Value, RootScope::Port(0));
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    rejects_with(builder, node, "demand or scope");
}
#[test]
fn fragment_rejects_all_ports_for_one_input_filter_field() {
    let (mut builder, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::AllPorts);
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    rejects_with(builder, node, "demand or scope");
}
#[test]
fn fragment_rejects_valid_unit_with_extra_input_port() {
    let (mut builder, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let mut ports = builder.node_ports(node).unwrap();
    ports.push(ExprPort {
        values: Box::default(),
    });
    install(&mut builder, node, ports);
    rejects_with(builder, node, "direct input count");
}
#[test]
fn fragment_rejects_valid_unit_reading_a_different_input_value() {
    let (mut builder, node, value) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    // Construct a different valid unit, whose own declaration and definition
    // agree. Only the fragment can reject its disagreement with the real child.
    drop(builder.take_unit_builder(node).unwrap());
    let foreign = ValueId::new(value.get() + 100);
    let draft = builder.unit(node).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(foreign))
        .unwrap();
    draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    install(
        &mut builder,
        node,
        vec![ExprPort {
            values: Box::new([(foreign, boolean(false))]),
        }],
    );
    rejects_with(builder, node, "exact ordered input value/type");
}
#[test]
fn fragment_rejects_root_not_referenced_by_any_node_field() {
    let (mut builder, node, value) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let draft = builder.unit(node).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(value))
        .unwrap();
    draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    rejects_with(builder, node, "exactly cover");
}
#[test]
fn finishing_cannot_discard_an_unsealed_draft() {
    let (builder, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    rejects_with(builder, node, "has not been sealed and installed");
}
#[test]
fn fragment_rejects_missing_unit_after_draft_is_taken() {
    let (mut builder, node, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    drop(builder.take_unit_builder(node).unwrap());
    rejects_with(builder, node, "requires an installed unit");
}
#[test]
fn units_in_different_nodes_keep_independent_dense_definition_ids() {
    let (mut builder, first, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let ports = builder.node_ports(first).unwrap();
    install(&mut builder, first, ports);
    let second = builder.reserve_node_id().unwrap();
    let input_value = builder.construction_node(first).unwrap().output.columns[0];
    let draft = builder.unit(second).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(input_value))
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    builder.add_filter(second, first, Box::new([root])).unwrap();
    let ports = builder.node_ports(second).unwrap();
    install(&mut builder, second, ports);
    let fragment = finish(builder, second).unwrap();
    let a = fragment.unit(first).unwrap();
    let b = fragment.unit(second).unwrap();
    assert_eq!(a.definitions()[0].id, b.definitions()[0].id);
    assert!(!std::ptr::eq(a, b));
    assert_eq!(
        fragment.root_expression(first, root).unwrap().ty,
        boolean(false)
    );
    assert_eq!(
        fragment.root_expression(second, root).unwrap().ty,
        boolean(false)
    );
}

fn window_fragment(count: u32) -> Fragment {
    use novarocks_expression_contract::BoundFunction;
    use novarocks_type_contract::{FunctionId, FunctionOverloadId, TypedFactSlice};
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let input = builder.reserve_node_id().unwrap();
    source(&mut builder, input, &[]);
    let node = builder.reserve_node_id().unwrap();
    let result = ValueType::new(DataType::Int64, false);
    let binding = BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/window/row-number").unwrap(),
        FunctionOverloadId::try_new("fixture/window/exact").unwrap(),
        FunctionKind::Window,
        TypedFactSlice::from_pure_boxed(Box::default()),
        result.clone(),
    );
    let calls = (0..count)
        .map(|ordinal| {
            let output = builder
                .add_value(
                    result.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal,
                    },
                )
                .unwrap();
            WindowCallSpec {
                binding: binding.clone(),
                distinct: false,
                arguments: Box::default(),
                order_by: Box::default(),
                frame: None,
                ignore_nulls: false,
                aggregate_binding: None,
                output,
            }
        })
        .collect::<Vec<_>>();
    let properties = builder.node_output_properties(input).unwrap().clone();
    let output = calls.iter().map(|call| call.output).collect();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::new([input]),
            required_inputs: Box::new([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: calls.into_boxed_slice(),
            }),
            expressions: None,
        })
        .unwrap();
    // This is the explicit unpublished stage. Relation claims are installed
    // afterwards; complete fragment/package publication must recheck them.
    builder
        .finish_structure(
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
        .unwrap()
}
fn window_claim(site: PhysicalCallSite, identity: u32) -> FrozenPhysicalCall {
    use novarocks_type_contract::{
        ArgumentControl, CallEffects, CallProofScope, FunctionFailureBehavior,
        FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility,
        ObservableEffects, RelationCallId, RelationEffectContext,
    };
    let context = RelationEffectContext::call(RelationCallId::new(identity));
    FrozenPhysicalCall {
        site,
        context,
        materialized_writer_input: None,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        effects: CallEffects {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: ArgumentControl::Window,
            instance_state: FunctionInstanceState::WindowPartition,
            observable_effects: ObservableEffects::NONE,
            environment: Box::default(),
            proof_scope: CallProofScope::Relation(context.identity),
        },
    }
}
#[test]
fn zero_argument_window_is_a_real_relation_site_without_a_scalar_unit() {
    let fragment = window_fragment(1);
    assert!(fragment.unit(fragment.root()).is_none());
    let site = PhysicalCallSite::Window {
        node: fragment.root(),
        call: 0,
    };
    let claim = window_claim(site, 0);
    let fragment = fragment.with_calls_observed(vec![claim], &Control).unwrap();
    let binding = fragment.calls().binding(&fragment, site).unwrap();
    assert_eq!(binding.kind(), FunctionKind::Window);
    fragment
        .calls()
        .validate_fragment(&fragment, &Control)
        .unwrap();
}
#[test]
fn window_claim_must_name_its_actual_ordered_call_site() {
    let fragment = window_fragment(1);
    let correct = PhysicalCallSite::Window {
        node: fragment.root(),
        call: 0,
    };
    let claim = window_claim(
        PhysicalCallSite::Window {
            node: fragment.root(),
            call: 1,
        },
        0,
    );
    assert!(
        matches!(fragment.with_calls_observed(vec![claim],&Control),Err(FrozenCallError::MissingSite(site)) if site==correct)
    );
}
#[test]
fn real_window_call_cannot_use_a_materialized_input_role() {
    use novarocks_type_contract::RelationEffectContext;
    let fragment = window_fragment(1);
    let mut claim = window_claim(
        PhysicalCallSite::Window {
            node: fragment.root(),
            call: 0,
        },
        0,
    );
    claim.context = RelationEffectContext::materialized_input(claim.context.identity.call);
    assert!(matches!(
        fragment.with_calls_observed(vec![claim], &Control),
        Err(FrozenCallError::WrongContext)
    ));
}
#[test]
fn two_real_window_calls_cannot_share_a_relational_identity() {
    let fragment = window_fragment(2);
    let claims = (0..2)
        .map(|call| {
            window_claim(
                PhysicalCallSite::Window {
                    node: fragment.root(),
                    call,
                },
                0,
            )
        })
        .collect();
    assert!(matches!(
        fragment.with_calls_observed(claims, &Control),
        Err(FrozenCallError::DuplicateIdentity)
    ));
}

fn scan_with_guarantee(residual: bool, scope: RootScope) -> (FragmentBuilder, NodeId) {
    use novarocks_connector_contract::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
        ConnectorReadRelationPayload,
    };
    let instance = ConnectorInstanceId::parse("fixture").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    );
    let payload = |category, byte| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![byte].into(),
        )
    };
    let column = ProviderColumnReference {
        column_payload: payload(ConnectorCodecCategory::ReadColumn, 33),
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            boolean(false),
            ValueOrigin::ProviderField {
                scan_node: node,
                field: column.clone(),
            },
        )
        .unwrap();
    let draft = builder.unit(node).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(value))
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::TruthOnly, scope)
        .unwrap();
    let properties = PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let relation = Relation::Metadata(MetadataRelation {
        kind: MetadataRelationKind::try_new("iceberg.manifest.entries").unwrap(),
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                payload(ConnectorCodecCategory::ReadTable, 1),
                payload(ConnectorCodecCategory::ReadView, 2),
            ),
        },
        selection_digest: [8; 32],
        schema: Box::new([RelationField {
            column: column.clone(),
            ty: boolean(false),
        }]),
        predicate_guarantees: Box::new([PredicateGuarantee {
            predicate: root,
            kind: PredicateGuaranteeKind::Exact,
        }]),
        provided_properties: properties.clone(),
        coverage_evidence: Box::new([4]),
    });
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::new([value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                provider_outputs: Box::new([(column, value)]),
                residuals: if residual {
                    Box::new([root])
                } else {
                    Box::default()
                },
                derived_values: Box::default(),
            },
            expressions: None,
        })
        .unwrap();
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    (builder, node)
}
#[test]
fn exact_scan_guarantee_can_reference_metadata_root_without_recomputation() {
    let (builder, node) = scan_with_guarantee(false, RootScope::Port(0));
    let fragment = finish(builder, node).unwrap();
    let NodeKind::Scan {
        residuals,
        relation,
        ..
    } = &fragment.nodes()[&node].kind
    else {
        unreachable!()
    };
    assert!(residuals.is_empty());
    assert_eq!(relation.predicate_guarantees().len(), 1);
    assert_eq!(fragment.unit(node).unwrap().roots().len(), 1);
}
#[test]
fn scan_guarantee_can_reference_the_existing_residual_root() {
    let (builder, node) = scan_with_guarantee(true, RootScope::Port(0));
    let fragment = finish(builder, node).unwrap();
    let NodeKind::Scan {
        residuals,
        relation,
        ..
    } = &fragment.nodes()[&node].kind
    else {
        unreachable!()
    };
    assert_eq!(
        residuals.as_ref(),
        [relation.predicate_guarantees()[0].predicate]
    );
    assert_eq!(fragment.unit(node).unwrap().roots().len(), 1);
}
#[test]
fn metadata_guarantee_root_still_requires_exact_provider_scope() {
    let (builder, node) = scan_with_guarantee(false, RootScope::AllPorts);
    rejects_with(
        builder,
        node,
        "scan guarantee requires a TruthOnly root over the provider port",
    );
}

fn fixture_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn complete_pure_plan(builder: FragmentBuilder, node: NodeId) -> PhysicalPlan {
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.finish().unwrap()
}
#[test]
fn fragment_checks_the_callers_semantic_depth_against_an_already_sealed_unit() {
    use novarocks_expression_contract::UnaryOperator;
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let input = builder.reserve_node_id().unwrap();
    let value = source(&mut builder, input, &[boolean(false)])[0];
    let node = builder.reserve_node_id().unwrap();
    let draft = builder.unit(node).unwrap();
    let mut definition = draft
        .add(None, boolean(false), ExprKind::Value(value))
        .unwrap();
    for _ in 0..2 {
        definition = draft
            .add(
                None,
                boolean(false),
                ExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: definition,
                },
            )
            .unwrap();
    }
    let root = draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    builder.add_filter(node, input, Box::new([root])).unwrap();
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    let plan = complete_pure_plan(builder, node);
    let errors = validate_plan_with_limits(
        &plan,
        PlanLimits {
            expression_semantic_depth: 2,
            ..PlanLimits::FROZEN
        },
    )
    .unwrap_err();
    assert!(errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.is_producer_defect());
    assert!(errors.to_string().contains("semantic depth"));
}
#[test]
fn fragment_definition_limit_is_the_sum_of_its_node_units() {
    let (mut builder, first, _) = filter(EvaluationDemand::TruthOnly, RootScope::Port(0));
    let ports = builder.node_ports(first).unwrap();
    install(&mut builder, first, ports);
    let value = builder.construction_node(first).unwrap().output.columns[0];
    let second = builder.reserve_node_id().unwrap();
    let draft = builder.unit(second).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Value(value))
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    builder.add_filter(second, first, Box::new([root])).unwrap();
    let ports = builder.node_ports(second).unwrap();
    install(&mut builder, second, ports);
    let plan = complete_pure_plan(builder, second);
    let fragment = &plan.fragments()[&FragmentId::new(0)];
    assert_eq!(fragment.unit(first).unwrap().definitions().len(), 1);
    assert_eq!(fragment.unit(second).unwrap().definitions().len(), 1);
    let errors = validate_plan_with_limits(
        &plan,
        PlanLimits {
            fragment_expressions: 1,
            ..PlanLimits::FROZEN
        },
    )
    .unwrap_err();
    assert!(errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.is_producer_defect());
}

#[test]
fn local_definition_finish_keeps_the_original_values_placement_proof_gate() {
    let source = novarocks_constant_contract::ConstantValue::from_boolean(
        Arc::new(arrow_schema::Field::new("source", DataType::Boolean, false)),
        boolean(false),
        true,
        fixture_policy(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(4),
        ordinal: source.ordinal(),
    };
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, source.pool().clone()).unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let node = builder.reserve_node_id().unwrap();
    let draft = builder.unit(node).unwrap();
    let definition = draft
        .add(None, boolean(false), ExprKind::Constant(reference))
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let value = builder
        .add_value(
            boolean(false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Unconstrained,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::new([value]),
            },
            kind: NodeKind::Values {
                rows: Box::new([Box::new([root])]),
            },
            expressions: None,
        })
        .unwrap();
    install(&mut builder, node, vec![]);
    rejects_with(
        builder,
        node,
        "VALUES distribution and row multiplicity lack an exact placement proof",
    );
    drop(pools);
}

#[test]
fn window_request_keeps_logical_arguments_separate_from_function_order_channels() {
    use novarocks_expression_contract::{BoundFunction, PhysicalCallRequest};
    use novarocks_type_contract::TypedFactSlice;
    let mut builder = FragmentBuilder::new(FragmentId::new(991));
    let input = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let inputs = source(&mut builder, input, &[ty.clone(), ty.clone()]);
    let node = builder.reserve_node_id().unwrap();
    let draft = builder.unit(node).unwrap();
    let roots = inputs
        .iter()
        .map(|value| {
            let definition = draft
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap();
            draft
                .root(definition, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 2,
            },
        )
        .unwrap();
    let binding = BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/window/ordered-aggregate").unwrap(),
        FunctionOverloadId::try_new("fixture/window/one-logical-one-order").unwrap(),
        FunctionKind::Aggregate,
        TypedFactSlice::from_pure_boxed(Box::new([
            FunctionArgumentType::Value(ty.clone()),
            FunctionArgumentType::Value(ty.clone()),
        ])),
        ty.clone(),
    );
    let aggregate = AggregateBinding {
        function: binding.clone(),
        phase: AggregatePhase::Single,
        logical_argument_count: 1,
        merge_state: Some(novarocks_physical_plan::AggregateMergeBinding {
            intermediate_type: ty.clone(),
            state_format: AggregateStateFormatId::try_new("fixture/window/state").unwrap(),
            state_argument_contract:
                novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
            state_interpretation: None,
        }),
    };
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::new([input]),
            required_inputs: Box::new([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::new([inputs[0], inputs[1], output]),
            },
            expressions: None,
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::new([WindowCallSpec {
                    binding,
                    arguments: Box::new([roots[0]]),
                    order_by: Box::new([SortExpr {
                        expr: roots[1],
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::Last,
                    }]),
                    distinct: false,
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: Some(Box::new(aggregate)),
                    output,
                }]),
            }),
        })
        .unwrap();
    let ports = builder.node_ports(node).unwrap();
    install(&mut builder, node, ports);
    let fragment = builder
        .finish_structure(
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
        .unwrap();
    let site = PhysicalCallSite::Window { node, call: 0 };
    let mut claim = window_claim(site, 0);
    claim.effects.argument_control = novarocks_type_contract::ArgumentControl::Aggregate;
    claim.effects.instance_state =
        novarocks_type_contract::FunctionInstanceState::AggregateInstance;
    let fragment = fragment.with_calls_observed(vec![claim], &Control).unwrap();
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    let original = PhysicalCallRequest {
        arguments: Box::new([
            StaticFunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            },
            StaticFunctionArgument::Value {
                value_type: ty,
                constant: None,
            },
        ]),
        logical_argument_count: 1,
        expected_result_type: None,
    };
    fragment
        .clone()
        .with_call_requests_observed(vec![(site, original.clone())], &Control)
        .unwrap();
    for logical_argument_count in [0, 2] {
        let wrong = PhysicalCallRequest {
            logical_argument_count,
            ..original.clone()
        };
        assert_eq!(
            fragment
                .clone()
                .with_call_requests_observed(vec![(site, wrong)], &Control)
                .unwrap_err(),
            CallRequestError::InvalidArgumentCount(site)
        );
    }
}

#[test]
fn window_offset_only_pool_is_in_the_complete_plan_constant_closure() {
    let policy = fixture_policy();
    let ty = ValueType::new(DataType::Int64, false);
    let offset = novarocks_constant_contract::ConstantValue::from_i64(
        Arc::new(ty.try_to_field("original.window.offset").unwrap()),
        ty,
        1,
        policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: offset.ordinal(),
    };
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, offset.pool().clone()).unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let input = builder.reserve_node_id().unwrap();
    source(&mut builder, input, &[]);
    let node = builder.reserve_node_id().unwrap();
    let result = ValueType::new(DataType::Int64, false);
    let output = builder
        .add_value(
            result.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let binding = novarocks_expression_contract::BoundFunction::from_exact_signature(
        novarocks_type_contract::FunctionId::try_new("fixture/window/row-number").unwrap(),
        novarocks_type_contract::FunctionOverloadId::try_new("fixture/window/exact").unwrap(),
        FunctionKind::Window,
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result,
    );
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::from([WindowCallSpec {
                    binding,
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: Some(WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start: WindowBound::Preceding(reference),
                        end: WindowBound::CurrentRow,
                        exclusion: WindowFrameExclusion::NoOthers,
                    }),
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output,
                }]),
            }),
        })
        .unwrap();
    let source_fragment = builder
        .finish_structure(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let site = PhysicalCallSite::Window { node, call: 0 };
    let fragment = source_fragment
        .with_calls_observed(vec![window_claim(site, 0)], &Control)
        .unwrap()
        .with_call_requests_observed(
            vec![(
                site,
                novarocks_expression_contract::PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
            )],
            &Control,
        )
        .unwrap();
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    assert!(
        fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let local = project_fragment_constants_observed(&pools, &fragment, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(
        local.entries().keys().copied().collect::<Vec<_>>(),
        vec![reference.pool]
    );
    assert_eq!(
        local.entries()[&reference.pool].backing_identity(),
        offset.pool().backing_identity()
    );
    let mut plan =
        PlanBuilder::new(PlanVersionId::try_new([19; 16]).unwrap()).with_constant_pools(pools);
    plan.add_fragment(fragment.clone()).unwrap();
    // Red before R1: PlanConstructionError::Constants(UnusedPools). The pool
    // has one real consumer, the frame, even though no scalar unit names it.
    let plan = plan.finish_observed(&Control).unwrap();
    let pruning = BTreeMap::from([(
        fragment.id(),
        FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control).unwrap(),
    )]);
    let admissions = BTreeMap::from([(
        fragment.id(),
        FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: 64 * 1024 * 1024,
            property_projection_limits: PropertyProofProjectionLimits {
                max_request_bytes: 16 * 1024 * 1024,
                max_coexisting_bytes: 256 * 1024 * 1024,
                max_projection_work: 16 * 1024 * 1024,
            },
        },
    )]);
    let packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap();
    assert_eq!(
        packages[&fragment.id()]
            .constants()
            .entries()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![reference.pool]
    );
    assert_eq!(
        packages[&fragment.id()].constants().entries()[&reference.pool].backing_identity(),
        offset.pool().backing_identity()
    );
}

#[test]
fn definition_source_observes_each_unitless_node_on_the_original_caller_scope() {
    use std::sync::Mutex;
    struct Caller {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Caller {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Decode);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            if let Some((at, _)) = self.stop {
                assert!(trace.len() <= at, "callback after original refusal");
            }
            let at = trace.len();
            trace.push((phase, units));
            match &self.stop {
                Some((stop, cause)) if *stop == at => Err(cause.clone()),
                _ => Ok(()),
            }
        }
    }
    let mut builder = FragmentBuilder::new(FragmentId::new(888));
    let mut root = builder.reserve_node_id().unwrap();
    source(&mut builder, root, &[]);
    for _ in 1..320 {
        let node = builder.reserve_node_id().unwrap();
        builder.add_limit(node, root, Some(1), 0).unwrap();
        root = node;
    }
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    assert_eq!(fragment.nodes().len(), 320);
    assert!(fragment.values().is_empty());
    assert!(
        fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    let run = |control: &Caller| -> Result<(), CompileControlError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let mut captures = 0;
        visit_fragment_definitions_observed::<CompileControlError>(
            &fragment,
            &mut work,
            |_, _| {
                captures += 1;
                Ok(())
            },
        )?;
        assert_eq!(captures, 0);
        // The source owns 320 actual node-presence checks; no emitted
        // definition/callback can fund or observe this otherwise hidden walk.
        work.finish()
    };
    let baseline = Caller {
        trace: Mutex::default(),
        stop: None,
    };
    run(&baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(
        trace,
        vec![
            (CompilePhase::Decode, 0),
            (CompilePhase::Decode, 256),
            (CompilePhase::Decode, 64)
        ]
    );
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let caller = Caller {
                trace: Mutex::default(),
                stop: Some((at, cause.clone())),
            };
            assert_eq!(run(&caller), Err(cause));
            assert_eq!(*caller.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn completed_relation_table_moves_without_copy_and_rechecks_actual_source_and_control() {
    let original = window_fragment(1);
    let site = PhysicalCallSite::Window {
        node: original.root(),
        call: 0,
    };
    let table =
        FrozenFragmentCalls::try_new(&original, vec![window_claim(site, 0)], &Control).unwrap();
    let retained = table.clone();
    let installed = original
        .clone()
        .with_frozen_calls_observed(table, &Control)
        .unwrap();
    assert!(std::ptr::eq(
        installed.calls().entries(),
        retained.entries()
    ));
    assert_eq!(installed.calls().entries().len(), 1);
    let wider = window_fragment(2);
    assert!(matches!(
        wider.with_frozen_calls_observed(retained.clone(), &Control),
        Err(FrozenCallError::MissingSite(PhysicalCallSite::Window {
            call: 1,
            ..
        }))
    ));
    let mut other = FragmentBuilder::new(FragmentId::new(37));
    let root = other.reserve_node_id().unwrap();
    source(&mut other, root, &[]);
    let other = finish(other, root).unwrap();
    assert!(matches!(
        other.with_frozen_calls_observed(retained.clone(), &Control),
        Err(FrozenCallError::WrongFragment)
    ));
    struct Stop(CompileControlError, std::sync::atomic::AtomicUsize);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            let next = self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            assert_eq!(next, 1, "callback after the original refusal");
            Err(self.0.clone())
        }
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Stop(cause.clone(), std::sync::atomic::AtomicUsize::new(0));
        assert!(
            matches!(original.clone().with_frozen_calls_observed(retained.clone(), &control), Err(FrozenCallError::Control(actual)) if actual == cause)
        );
        assert_eq!(control.1.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    assert_eq!(retained.entries().len(), 1);
}
