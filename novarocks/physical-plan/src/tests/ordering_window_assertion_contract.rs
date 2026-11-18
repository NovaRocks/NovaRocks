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

#[derive(Clone, Copy)]
enum SourceDistribution {
    Singleton,
    Hash(u8),
    Broadcast,
}

#[derive(Clone, Copy)]
enum SortFixtureMode {
    Global,
    Analytic,
    PartitionTopN(u64),
}

fn ordering(value: ValueId, null_ordering: NullOrdering) -> OrderingKey {
    OrderingKey {
        value,
        direction: SortDirection::Ascending,
        null_ordering,
    }
}

fn sort_expr(expression: crate::RootOrdinal, null_ordering: NullOrdering) -> SortExpr {
    SortExpr {
        expr: expression,
        direction: SortDirection::Ascending,
        null_ordering,
    }
}

fn distribution(kind: SourceDistribution, key: ValueId) -> Distribution {
    match kind {
        SourceDistribution::Singleton => Distribution::Singleton,
        SourceDistribution::Hash(seed) => Distribution::Hash {
            keys: Box::from([key]),
            scheme: hash_scheme(seed),
        },
        SourceDistribution::Broadcast => Distribution::Broadcast,
    }
}

fn append_exchange_source(
    builder: &mut FragmentBuilder,
    fragment_seed: u32,
    distribution_kind: SourceDistribution,
    ordered: bool,
) -> (NodeId, ValueId, ValueId, PhysicalProperties) {
    let node = builder.reserve_node_id().unwrap();
    let edge = EdgeId::new(fragment_seed);
    let source_key = ValueId::new(fragment_seed + 10_000);
    let source_order = ValueId::new(fragment_seed + 20_000);
    let key = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: source_key,
            },
        )
        .unwrap();
    let order = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: source_order,
            },
        )
        .unwrap();
    let distribution = distribution(distribution_kind, key);
    let properties = PhysicalProperties {
        row_multiplicity: if distribution == Distribution::Broadcast {
            RowMultiplicity::Replicated
        } else {
            RowMultiplicity::SingleCopy
        },
        distribution,
        ordering: if ordered {
            Box::from([
                ordering(key, NullOrdering::First),
                ordering(order, NullOrdering::Last),
            ])
        } else {
            Box::default()
        },
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties.clone(),
            output: OutputPort {
                node,
                columns: Box::from([key, order]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_key, key), (source_order, order)]),
            },
        })
        .unwrap();
    (node, key, order, properties)
}

fn finish_sort(
    distribution_kind: SourceDistribution,
    fixture_mode: SortFixtureMode,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(401));
    let (source, key, order, input_properties) =
        append_exchange_source(&mut builder, 401, distribution_kind, false);
    let node = builder.reserve_node_id().unwrap();
    let order_expression = builder
        .define(node, ty(DataType::Int64, false), ExprKind::Value(order))
        .unwrap();
    let order_expression = builder.root_definition(
        node,
        order_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let order_by: Box<[SortExpr]> = Box::from([sort_expr(order_expression, NullOrdering::Last)]);
    let mode = match fixture_mode {
        SortFixtureMode::Global => SortMode::Global,
        SortFixtureMode::Analytic | SortFixtureMode::PartitionTopN(_) => {
            let partition_expression = builder
                .define(node, ty(DataType::Int64, false), ExprKind::Value(key))
                .unwrap();
            let partition_expression = builder.root_definition(
                node,
                partition_expression,
                novarocks_type_contract::EvaluationDemand::Value,
                crate::RootScope::Port(0),
            );
            let partition_by = Box::from([sort_expr(partition_expression, NullOrdering::First)]);
            match fixture_mode {
                SortFixtureMode::Analytic => SortMode::Analytic { partition_by },
                SortFixtureMode::PartitionTopN(limit) => SortMode::PartitionTopN {
                    partition_by,
                    limit,
                    kind: PartitionTopNType::RowNumber,
                },
                SortFixtureMode::Global => unreachable!(),
            }
        }
    };
    let output_ordering = match fixture_mode {
        SortFixtureMode::Global => Box::from([ordering(order, NullOrdering::Last)]),
        SortFixtureMode::Analytic | SortFixtureMode::PartitionTopN(_) => Box::from([
            ordering(key, NullOrdering::First),
            ordering(order, NullOrdering::Last),
        ]),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([input_properties.clone()]),
            output_properties: PhysicalProperties {
                distribution: input_properties.distribution,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: output_ordering,
            },
            output: OutputPort {
                node,
                columns: Box::from([key, order]),
            },
            kind: NodeKind::Sort { order_by, mode },
        })
        .unwrap();
    builder.finish_fixture_definition(
        node,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

fn finish_topn(
    distribution_kind: SourceDistribution,
    phase: TopNPhase,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(402));
    let (source, key, order, input_properties) =
        append_exchange_source(&mut builder, 402, distribution_kind, false);
    let node = builder.reserve_node_id().unwrap();
    let order_expression = builder
        .define(node, ty(DataType::Int64, false), ExprKind::Value(order))
        .unwrap();
    let order_expression = builder.root_definition(
        node,
        order_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([input_properties.clone()]),
            output_properties: PhysicalProperties {
                distribution: input_properties.distribution,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::from([ordering(order, NullOrdering::Last)]),
            },
            output: OutputPort {
                node,
                columns: Box::from([key, order]),
            },
            kind: NodeKind::TopN {
                reduction: crate::TopNReduction::Rows,
                order_by: Box::from([sort_expr(order_expression, NullOrdering::Last)]),
                limit: 10,
                offset: 0,
                phase,
            },
        })
        .unwrap();
    builder.finish_fixture_definition(
        node,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

fn finish_topn_reduction(
    partial_sequence: TopNSequenceId,
    final_sequence: Option<TopNSequenceId>,
    partial_limit: u64,
    partial_offset: u64,
    final_limit: u64,
    final_offset: u64,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Result<PhysicalPlan, String> {
    let source_fragment = FragmentId::new(410);
    let partial_fragment = FragmentId::new(411);
    let final_fragment = FragmentId::new(412);
    let partial_edge = EdgeId::new(410);
    let final_edge = EdgeId::new(411);
    let (source, source_value) = literal_fragment(
        source_fragment,
        FragmentSink::Stream { edge: partial_edge },
        false,
        fixture_constants,
    );

    let mut partial_builder = FragmentBuilder::new(partial_fragment);
    let partial_source = partial_builder.reserve_node_id().unwrap();
    let partial_value = partial_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge: partial_edge,
                source_value,
            },
        )
        .unwrap();
    let partial_distribution = Distribution::Hash {
        keys: Box::from([partial_value]),
        scheme: hash_scheme(81),
    };
    partial_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: partial_source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: partial_distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: partial_source,
                columns: Box::from([partial_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge: partial_edge,
                imports: Box::from([(source_value, partial_value)]),
            },
        })
        .unwrap();
    let partial_topn = partial_builder.reserve_node_id().unwrap();
    let partial_order = partial_builder
        .define(
            partial_topn,
            ty(DataType::Int64, false),
            ExprKind::Value(partial_value),
        )
        .unwrap();
    let partial_order = partial_builder.root_definition(
        partial_topn,
        partial_order,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    partial_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: partial_topn,
            inputs: Box::from([partial_source]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: partial_distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: PhysicalProperties {
                distribution: partial_distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::from([ordering(partial_value, NullOrdering::Last)]),
            },
            output: OutputPort {
                node: partial_topn,
                columns: Box::from([partial_value]),
            },
            kind: NodeKind::TopN {
                reduction: crate::TopNReduction::Rows,
                order_by: Box::from([sort_expr(partial_order, NullOrdering::Last)]),
                limit: partial_limit,
                offset: partial_offset,
                phase: TopNPhase::Partial {
                    sequence: partial_sequence,
                },
            },
        })
        .unwrap();
    let partial = partial_builder
        .finish_fixture_definition(
            partial_topn,
            FragmentSink::Stream { edge: final_edge },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .map_err(|error| error.to_string())?;

    let mut final_builder = FragmentBuilder::new(final_fragment);
    let final_source = final_builder.reserve_node_id().unwrap();
    let final_value = final_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge: final_edge,
                source_value: partial_value,
            },
        )
        .unwrap();
    final_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: final_source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: final_source,
                columns: Box::from([final_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge: final_edge,
                imports: Box::from([(partial_value, final_value)]),
            },
        })
        .unwrap();
    let final_topn = final_builder.reserve_node_id().unwrap();
    let final_order = final_builder
        .define(
            final_topn,
            ty(DataType::Int64, false),
            ExprKind::Value(final_value),
        )
        .unwrap();
    let final_order = final_builder.root_definition(
        final_topn,
        final_order,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    final_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: final_topn,
            inputs: Box::from([final_source]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::from([ordering(final_value, NullOrdering::Last)]),
            },
            output: OutputPort {
                node: final_topn,
                columns: Box::from([final_value]),
            },
            kind: NodeKind::TopN {
                reduction: crate::TopNReduction::Rows,
                order_by: Box::from([sort_expr(final_order, NullOrdering::Last)]),
                limit: final_limit,
                offset: final_offset,
                phase: final_sequence
                    .map_or(TopNPhase::Single, |sequence| TopNPhase::Final { sequence }),
            },
        })
        .unwrap();
    let final_stage = final_builder
        .finish_fixture_definition(
            final_topn,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .map_err(|error| error.to_string())?;

    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(partial).unwrap();
    plan.add_fragment(final_stage).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: partial_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: source_fragment,
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: partial_fragment,
            node: partial_source,
            receive_mapping: Box::from([(source_value, partial_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Hash {
                keys: Box::from([source_value]),
                scheme: hash_scheme(81),
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: partial_distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: final_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: partial_fragment,
            projection: Box::from([partial_value]),
        },
        destination: EdgeDestination {
            fragment: final_fragment,
            node: final_source,
            receive_mapping: Box::from([(partial_value, final_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    crate::tests::constant_fixture::publish_plan(plan, fixture_constants)
        .map_err(|error| error.to_string())
}

fn window_function() -> BoundFunction {
    BoundFunction {
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            semantic_parameters: Box::default(),
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error:
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
        }),
        function_id: FunctionId::try_new("builtin/row_number/v1").unwrap(),
        overload: FunctionOverloadId::try_new("row-number-empty").unwrap(),
        kind: FunctionKind::Window,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type: ty(DataType::Int64, false),
    }
}

#[derive(Clone, Copy)]
enum WindowFixtureLayout {
    Exact,
    MissingOrdering,
    StrongerOrdering,
    HashKeySubset,
    Singleton,
}

fn finish_window(
    build_frame: impl FnOnce(&mut crate::tests::constant_fixture::ConstantFixture) -> WindowFrame,
    layout: WindowFixtureLayout,
) -> Result<(Fragment, ConstantPools), ValidationErrors> {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(403));
    let distribution_kind = match layout {
        WindowFixtureLayout::Singleton => SourceDistribution::Singleton,
        WindowFixtureLayout::Exact
        | WindowFixtureLayout::MissingOrdering
        | WindowFixtureLayout::StrongerOrdering
        | WindowFixtureLayout::HashKeySubset => SourceDistribution::Hash(73),
    };
    // Complete the real pending-call construction from a valid ordered
    // source; inject only MissingOrdering's original fault at the full gate.
    let child_is_ordered = true;
    let (source, key, order, _) =
        append_exchange_source(&mut builder, 403, distribution_kind, child_is_ordered);
    let node = builder.reserve_node_id().unwrap();
    let partition_expression = builder
        .define(node, ty(DataType::Int64, false), ExprKind::Value(key))
        .unwrap();
    let partition_expression = builder.root_definition(
        node,
        partition_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let order_expression = (!matches!(layout, WindowFixtureLayout::StrongerOrdering)).then(|| {
        let definition = builder
            .define(node, ty(DataType::Int64, false), ExprKind::Value(order))
            .unwrap();
        builder.root_definition(
            node,
            definition,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        )
    });
    let frame = build_frame(&mut fixture_constants);
    let output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 2,
            },
        )
        .unwrap();
    let output_columns = vec![key, order, output];
    let calls = Box::from([crate::WindowCallSpec {
        binding: window_function(),
        distinct: false,
        arguments: Box::default(),
        order_by: Box::default(),
        frame: Some(frame),
        ignore_nulls: false,
        aggregate_binding: None,
        output,
    }]);
    let partition_by = if matches!(layout, WindowFixtureLayout::HashKeySubset) {
        Box::from([
            sort_expr(partition_expression, NullOrdering::First),
            sort_expr(order_expression.unwrap(), NullOrdering::Last),
        ])
    } else {
        Box::from([sort_expr(partition_expression, NullOrdering::First)])
    };
    let order_by = if matches!(
        layout,
        WindowFixtureLayout::StrongerOrdering | WindowFixtureLayout::HashKeySubset
    ) {
        Box::default()
    } else {
        Box::from([sort_expr(order_expression.unwrap(), NullOrdering::Last)])
    };
    let input_properties = PhysicalProperties {
        distribution: distribution(distribution_kind, key),
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: if child_is_ordered {
            Box::from([
                ordering(key, NullOrdering::First),
                ordering(order, NullOrdering::Last),
            ])
        } else {
            Box::default()
        },
    };
    let required_properties = PhysicalProperties {
        distribution: input_properties.distribution.clone(),
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: if matches!(layout, WindowFixtureLayout::StrongerOrdering) {
            Box::from([ordering(key, NullOrdering::First)])
        } else {
            Box::from([
                ordering(key, NullOrdering::First),
                ordering(order, NullOrdering::Last),
            ])
        },
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([required_properties]),
            output_properties: input_properties,
            output: OutputPort {
                node,
                columns: output_columns.into_boxed_slice(),
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by,
                order_by,
                calls,
            }),
        })
        .unwrap();
    crate::tests::unit_fixture::seal_pure_unit(&mut builder, node);
    let fragment = builder
        .finish_structure(
            node,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &WindowConstantControl,
            crate::StagePlacementPolicy::Singleton,
        )
        .map_err(|error| match error {
            crate::FragmentStructureError::Structure(errors) => errors,
            other => panic!("fixed window structure fixture failed: {other}"),
        })?;
    let fragment = super::original_call_requests::with_original_relational_requests(
        fragment,
        &WindowConstantControl,
    );
    use novarocks_type_contract::{
        ArgumentControl, CallEffects, CallProofScope, FunctionInstanceState, FunctionNullBehavior,
        ObservableEffects, RelationCallId, RelationEffectContext,
    };
    let context = RelationEffectContext::call(RelationCallId::new(0));
    let mut fragment = fragment
        .with_calls_observed(
            vec![crate::FrozenPhysicalCall {
                site: crate::PhysicalCallSite::Window { node, call: 0 },
                context,
                materialized_writer_input: None,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
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
            }],
            &WindowConstantControl,
        )
        .expect("fixture authored its exact Window relation claim");
    if matches!(layout, WindowFixtureLayout::MissingOrdering) {
        let mut parts = fragment.into_parts();
        parts
            .nodes
            .get_mut(&source)
            .unwrap()
            .output_properties
            .ordering = Box::default();
        parts
            .nodes
            .get_mut(&node)
            .unwrap()
            .output_properties
            .ordering = Box::default();
        fragment = parts.into();
    }
    crate::validate_fragment_definition(&fragment)?;
    let pools = fixture_constants.into_pools();
    if let Err(error) = crate::constants::validate_fragment_constants_observed(
        &fragment,
        &pools,
        true,
        PlanLimits::FROZEN,
        &WindowConstantControl,
    ) {
        let mut errors = crate::validation::ValidationContext::new();
        errors.push(ValidationError::new("window.constants", error.to_string()));
        return Err(ValidationErrors::from_collector(errors));
    }
    Ok((fragment, pools))
}
struct WindowConstantControl;
impl novarocks_type_contract::PureCompileControl for WindowConstantControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

fn finish_assertion(
    distribution_kind: SourceDistribution,
    assertion: impl FnOnce(ValueId, ValueId) -> RowCountAssertionSpec,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(404));
    let (source, key, order, properties) =
        append_exchange_source(&mut builder, 404, distribution_kind, false);
    let node = builder.reserve_node_id().unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([key, order]),
            },
            kind: NodeKind::AssertOneRow(assertion(key, order)),
        })
        .unwrap();
    builder.finish_fixture_definition(
        node,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

fn contains_error<T: std::fmt::Debug>(result: Result<T, ValidationErrors>, expected: &str) -> bool {
    result
        .expect_err("fixture must be rejected")
        .to_string()
        .contains(expected)
}

#[test]
fn sort_modes_close_global_analytic_and_partition_topn_contracts() {
    finish_sort(SourceDistribution::Singleton, SortFixtureMode::Global).unwrap();
    finish_sort(SourceDistribution::Hash(71), SortFixtureMode::Analytic).unwrap();
    finish_sort(
        SourceDistribution::Hash(72),
        SortFixtureMode::PartitionTopN(10),
    )
    .unwrap();
    assert!(contains_error(
        finish_sort(SourceDistribution::Hash(71), SortFixtureMode::Global),
        "sort mode lacks its exact input and output distribution contract"
    ));
    assert!(contains_error(
        finish_sort(
            SourceDistribution::Hash(72),
            SortFixtureMode::PartitionTopN(0),
        ),
        "partition TopN sort requires a non-zero limit"
    ));
}

#[test]
fn topn_phase_contract_separates_global_and_partial_completion() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    finish_topn(SourceDistribution::Singleton, TopNPhase::Single).unwrap();
    let sequence = TopNSequenceId::new(1);
    finish_topn_reduction(
        sequence,
        Some(sequence),
        15,
        0,
        10,
        5,
        &mut fixture_constants,
    )
    .unwrap();

    assert!(contains_error(
        finish_topn(SourceDistribution::Hash(74), TopNPhase::Final { sequence }),
        "TopN phase lacks its exact distribution contract"
    ));
    finish_topn(
        SourceDistribution::Singleton,
        TopNPhase::Partial { sequence },
    )
    .unwrap();

    let orphan =
        finish_topn_reduction(sequence, None, 10, 0, 10, 0, &mut fixture_constants).unwrap_err();
    assert!(orphan.contains("TopN sequence must have exactly one final node"));

    let mismatch = finish_topn_reduction(
        sequence,
        Some(TopNSequenceId::new(2)),
        10,
        0,
        10,
        0,
        &mut fixture_constants,
    )
    .unwrap_err();
    assert!(mismatch.contains("TopN sequence must have exactly one final node"));

    let partial_offset = finish_topn_reduction(
        sequence,
        Some(sequence),
        15,
        1,
        10,
        5,
        &mut fixture_constants,
    )
    .unwrap_err();
    assert!(
        partial_offset.contains("partial TopN cannot apply an offset before global completion")
    );

    let undersized_partial = finish_topn_reduction(
        sequence,
        Some(sequence),
        10,
        0,
        10,
        5,
        &mut fixture_constants,
    )
    .unwrap_err();
    assert!(
        undersized_partial
            .contains("TopN partial paths do not reduce exactly into their matching final")
    );
}

#[test]
fn window_owns_one_partition_order_and_preserves_frame_exclusion() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Groups,
        start: WindowBound::CurrentRow,
        end: WindowBound::UnboundedFollowing,
        exclusion: WindowFrameExclusion::Ties,
    };
    let (fragment, _pools) = finish_window(|_| frame, WindowFixtureLayout::Exact).unwrap();
    let NodeKind::Window(spec) = &fragment.nodes()[&fragment.root()].kind else {
        panic!("expected window root");
    };
    let frame = spec.calls[0].frame.as_ref().expect("window frame");
    assert_eq!(frame.exclusion, WindowFrameExclusion::Ties);

    assert!(contains_error(
        finish_window(
            |_| WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowBound::CurrentRow,
                end: WindowBound::UnboundedFollowing,
                exclusion: WindowFrameExclusion::NoOthers,
            },
            WindowFixtureLayout::MissingOrdering,
        ),
        "window child and output properties differ from its exact partition ordering"
    ));
    assert!(contains_error(
        finish_window(
            |_| WindowFrame {
                units: WindowFrameUnits::Range,
                start: WindowBound::UnboundedFollowing,
                end: WindowBound::UnboundedFollowing,
                exclusion: WindowFrameExclusion::Group,
            },
            WindowFixtureLayout::Exact,
        ),
        "window frame start follows its end"
    ));

    for (start_offset, end_offset, preceding) in [(5, 10, true), (10, 5, false)] {
        let result = finish_window(
            |fixture_constants| {
                let ExprKind::Constant(start) =
                    fixture_constants.u64(start_offset, &ty(DataType::UInt64, false))
                else {
                    unreachable!("checked constant fixture returns its exact reference")
                };
                let ExprKind::Constant(end) =
                    fixture_constants.u64(end_offset, &ty(DataType::UInt64, false))
                else {
                    unreachable!("checked constant fixture returns its exact reference")
                };
                if preceding {
                    WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start: WindowBound::Preceding(start),
                        end: WindowBound::Preceding(end),
                        exclusion: WindowFrameExclusion::NoOthers,
                    }
                } else {
                    WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start: WindowBound::Following(start),
                        end: WindowBound::Following(end),
                        exclusion: WindowFrameExclusion::NoOthers,
                    }
                }
            },
            WindowFixtureLayout::Exact,
        );
        assert!(contains_error(
            result,
            "window frame start follows its end after comparing exact constant offsets"
        ));
    }

    assert!(contains_error(
        finish_window(
            |fixture_constants| {
                let ExprKind::Constant(offset) =
                    fixture_constants.i64(1, &ty(DataType::Int64, false))
                else {
                    unreachable!("checked constant fixture returns its exact reference")
                };
                WindowFrame {
                    units: WindowFrameUnits::Range,
                    start: WindowBound::Preceding(offset),
                    end: WindowBound::CurrentRow,
                    exclusion: WindowFrameExclusion::NoOthers,
                }
            },
            WindowFixtureLayout::Exact,
        ),
        "RANGE window offsets require typed order-key arithmetic not represented by contract revision 1"
    ));
}

#[test]
fn window_accepts_singleton_coarser_hash_and_stronger_ordering() {
    let frame = |_: &mut crate::tests::constant_fixture::ConstantFixture| WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowBound::UnboundedPreceding,
        end: WindowBound::CurrentRow,
        exclusion: WindowFrameExclusion::NoOthers,
    };
    finish_window(frame, WindowFixtureLayout::Singleton).unwrap();
    finish_window(frame, WindowFixtureLayout::HashKeySubset).unwrap();
    finish_window(frame, WindowFixtureLayout::StrongerOrdering).unwrap();
}

#[test]
fn assertion_modes_require_global_or_colocated_distribution() {
    finish_assertion(SourceDistribution::Singleton, |_, _| {
        RowCountAssertionSpec::Global {
            subject: "scalar subquery".into(),
            desired_rows: 1,
            comparison: RowCountAssertion::Le,
        }
    })
    .unwrap();
    finish_assertion(SourceDistribution::Hash(75), |key, _| {
        RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: crate::AssertionApplicability::AllRows,
            keys: Box::from([key]),
            labels: Box::from([Box::<str>::from("account_id")]),
            message: "duplicate mutation match".into(),
        }
    })
    .unwrap();

    finish_assertion(SourceDistribution::Singleton, |key, _| {
        RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: crate::AssertionApplicability::AllRows,
            keys: Box::from([key]),
            labels: Box::from([Box::<str>::from("account_id")]),
            message: "duplicate mutation match".into(),
        }
    })
    .unwrap();
    finish_assertion(SourceDistribution::Hash(75), |key, order| {
        RowCountAssertionSpec::PerKeyAtMostOne {
            declared_violation: None,
            applicability: crate::AssertionApplicability::AllRows,
            keys: Box::from([key, order]),
            labels: Box::from([Box::<str>::from("account_id"), Box::<str>::from("event_id")]),
            message: "duplicate mutation match".into(),
        }
    })
    .unwrap();
    assert!(contains_error(
        finish_assertion(SourceDistribution::Hash(75), |_, _| {
            RowCountAssertionSpec::Global {
                subject: "scalar subquery".into(),
                desired_rows: 1,
                comparison: RowCountAssertion::Le,
            }
        }),
        "row-count assertion lacks its exact distribution contract"
    ));
    assert!(contains_error(
        finish_assertion(SourceDistribution::Hash(75), |_, _| {
            RowCountAssertionSpec::PerKeyAtMostOne {
                declared_violation: None,
                applicability: crate::AssertionApplicability::AllRows,
                keys: Box::default(),
                labels: Box::default(),
                message: "duplicate mutation match".into(),
            }
        }),
        "matching non-empty keys and labels"
    ));
}

#[test]
fn table_function_preserves_ordering_prefix_and_drops_volatile_broadcast() {
    let fragment = finish_table_function(SourceDistribution::Hash(76), false, false);
    let node = fragment.root();
    let key = fragment.nodes()[&node].output.columns[0];
    assert_eq!(
        fragment.nodes()[&node].output_properties.ordering.as_ref(),
        &[ordering(key, NullOrdering::First)]
    );

    let fragment = finish_table_function(SourceDistribution::Broadcast, false, true);
    assert_eq!(
        fragment.nodes()[&fragment.root()].output_properties,
        PhysicalProperties {
            distribution: Distribution::Unconstrained,
            row_multiplicity: RowMultiplicity::Replicated,
            ordering: Box::default(),
        }
    );

    let error = finish_table_function_with_sink(
        SourceDistribution::Broadcast,
        false,
        true,
        FragmentSink::Result,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("result sink requires single-copy row ownership"));
}

fn finish_table_function(
    distribution_kind: SourceDistribution,
    passthrough_order: bool,
    volatile: bool,
) -> Fragment {
    finish_table_function_with_sink(
        distribution_kind,
        passthrough_order,
        volatile,
        FragmentSink::Noop,
    )
    .unwrap()
}

fn finish_table_function_with_sink(
    distribution_kind: SourceDistribution,
    passthrough_order: bool,
    volatile: bool,
    sink: FragmentSink,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(405));
    let source_ordered = matches!(distribution_kind, SourceDistribution::Hash(_));
    let (source, key, order, properties) =
        append_exchange_source(&mut builder, 405, distribution_kind, source_ordered);
    let node = builder.reserve_node_id().unwrap();
    let output_ordinal = if passthrough_order { 2 } else { 1 };
    let result = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal,
            },
        )
        .unwrap();
    let outputs: Box<[TableFunctionOutput]> = if passthrough_order {
        Box::from([
            TableFunctionOutput::PassThrough(key),
            TableFunctionOutput::PassThrough(order),
            TableFunctionOutput::FunctionResult {
                result_ordinal: 0,
                value: result,
            },
        ])
    } else {
        Box::from([
            TableFunctionOutput::PassThrough(key),
            TableFunctionOutput::FunctionResult {
                result_ordinal: 0,
                value: result,
            },
        ])
    };
    let output_properties = match distribution_kind {
        SourceDistribution::Hash(_) => PhysicalProperties {
            distribution: properties.distribution.clone(),
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::from([ordering(key, NullOrdering::First)]),
        },
        SourceDistribution::Broadcast if volatile => PhysicalProperties {
            distribution: Distribution::Unconstrained,
            row_multiplicity: RowMultiplicity::Replicated,
            ordering: Box::default(),
        },
        SourceDistribution::Broadcast | SourceDistribution::Singleton => properties.clone(),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([properties]),
            output_properties,
            output: OutputPort {
                node,
                columns: outputs.iter().map(|output| output.value()).collect(),
            },
            kind: NodeKind::TableFunction {
                function: BoundTableFunction {
                    legacy_metadata: Some(crate::LegacyBindingMetadata {
                        semantic_parameters: Box::default(),
                        volatility: if volatile {
                            FunctionVolatility::Volatile
                        } else {
                            FunctionVolatility::Immutable
                        },
                        argument_evaluation: FunctionArgumentEvaluation::Eager,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        intrinsic_row_error:
                            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                    }),
                    function_id: FunctionId::try_new("builtin/generate_one/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("empty-to-i64").unwrap(),
                    argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::default(),
                    ),
                    result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::from([ty(DataType::Int64, false)]),
                    ),
                },
                arguments: Box::default(),
                outputs,
                left_outer: false,
            },
        })
        .unwrap();
    let fragment = builder.finish_fixture_structure(
        node,
        sink,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )?;
    let fragment = super::original_call_requests::with_original_relational_requests(
        fragment,
        &WindowConstantControl,
    );
    use novarocks_type_contract::{
        ArgumentControl, CallEffects, CallProofScope, FunctionInstanceState, FunctionNullBehavior,
        ObservableEffects, RelationCallId, RelationEffectContext,
    };
    let context = RelationEffectContext::call(RelationCallId::new(0));
    // The fixed fixture's input, not binding metadata inspection, authors FX.
    let effects = CallEffects {
        value_stability: if volatile {
            FunctionVolatility::Volatile
        } else {
            FunctionVolatility::Immutable
        },
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Table,
        instance_state: FunctionInstanceState::TableInstance,
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Relation(context.identity),
    };
    let fragment = fragment
        .with_calls_observed(
            vec![crate::FrozenPhysicalCall {
                site: crate::PhysicalCallSite::Table { node },
                context,
                materialized_writer_input: None,
                effects,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            }],
            &WindowConstantControl,
        )
        .expect("fixture authored its exact Table relation claim");
    crate::validate_fragment_definition(&fragment)?;
    Ok(fragment)
}
