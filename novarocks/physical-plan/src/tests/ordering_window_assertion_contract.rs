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

fn sort_expr(expression: ExprId, null_ordering: NullOrdering) -> SortExpr {
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
        .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(order))
        .unwrap();
    let order_by: Box<[SortExpr]> = Box::from([sort_expr(order_expression, NullOrdering::Last)]);
    let mode = match fixture_mode {
        SortFixtureMode::Global => SortMode::Global,
        SortFixtureMode::Analytic | SortFixtureMode::PartitionTopN(_) => {
            let partition_expression = builder
                .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(key))
                .unwrap();
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
    builder.finish_definition(node, FragmentSink::Noop, dop())
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
        .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(order))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
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
                order_by: Box::from([sort_expr(order_expression, NullOrdering::Last)]),
                limit: 10,
                offset: 0,
                phase,
            },
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

fn finish_topn_reduction(
    partial_sequence: TopNSequenceId,
    final_sequence: Option<TopNSequenceId>,
    partial_limit: u64,
    partial_offset: u64,
    final_limit: u64,
    final_offset: u64,
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
        .add_expression(
            partial_topn,
            ty(DataType::Int64, false),
            ExprKind::Value(partial_value),
        )
        .unwrap();
    partial_builder
        .insert_node_unchecked(PhysicalNode {
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
        .finish_definition(
            partial_topn,
            FragmentSink::Stream { edge: final_edge },
            dop(),
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
        .add_expression(
            final_topn,
            ty(DataType::Int64, false),
            ExprKind::Value(final_value),
        )
        .unwrap();
    final_builder
        .insert_node_unchecked(PhysicalNode {
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
                order_by: Box::from([sort_expr(final_order, NullOrdering::Last)]),
                limit: final_limit,
                offset: final_offset,
                phase: final_sequence
                    .map_or(TopNPhase::Single, |sequence| TopNPhase::Final { sequence }),
            },
        })
        .unwrap();
    let final_stage = final_builder
        .finish_definition(final_topn, FragmentSink::Noop, dop())
        .map_err(|error| error.to_string())?;

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(partial).unwrap();
    plan.add_fragment(final_stage).unwrap();
    plan.add_edge(Edge {
        id: partial_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    plan.add_edge(Edge {
        id: final_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    plan.finish().map_err(|error| error.to_string())
}

fn window_function() -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("builtin/row_number/v1").unwrap(),
        overload: FunctionOverloadId::try_new("row-number-empty").unwrap(),
        kind: FunctionKind::Window,
        argument_types: Box::default(),
        result_type: ty(DataType::Int64, false),
        volatility: FunctionVolatility::Immutable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
    }
}

#[derive(Clone, Copy)]
enum WindowFixtureLayout {
    Exact,
    MissingOrdering,
    StrongerOrdering,
    HashKeySubset,
    Singleton,
    ReusedCallAsChild,
}

fn finish_window(
    build_frame: impl FnOnce(&mut FragmentBuilder, NodeId) -> WindowFrame,
    layout: WindowFixtureLayout,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(403));
    let distribution_kind = match layout {
        WindowFixtureLayout::Singleton => SourceDistribution::Singleton,
        WindowFixtureLayout::Exact
        | WindowFixtureLayout::MissingOrdering
        | WindowFixtureLayout::StrongerOrdering
        | WindowFixtureLayout::HashKeySubset
        | WindowFixtureLayout::ReusedCallAsChild => SourceDistribution::Hash(73),
    };
    let child_is_ordered = !matches!(layout, WindowFixtureLayout::MissingOrdering);
    let (source, key, order, _) =
        append_exchange_source(&mut builder, 403, distribution_kind, child_is_ordered);
    let node = builder.reserve_node_id().unwrap();
    let partition_expression = builder
        .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(key))
        .unwrap();
    let order_expression = (!matches!(layout, WindowFixtureLayout::StrongerOrdering)).then(|| {
        builder
            .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(order))
            .unwrap()
    });
    let frame = build_frame(&mut builder, node);
    let call = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::WindowCall {
                function: window_function(),
                distinct: false,
                args: Box::default(),
                function_order_by: Box::default(),
                frame: Some(frame),
                ignore_nulls: false,
                aggregate_binding: None,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::Expr { node, expr: call },
        )
        .unwrap();
    let mut output_columns = vec![key, order, output];
    let mut window_expressions = vec![WindowExpression {
        expression: call,
        output,
    }];
    if matches!(layout, WindowFixtureLayout::ReusedCallAsChild) {
        let wrapper = builder
            .add_expression(
                node,
                ty(DataType::Int64, false),
                ExprKind::Unary {
                    op: UnaryOperator::Plus,
                    expr: call,
                },
            )
            .unwrap();
        let wrapper_output = builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::Expr {
                    node,
                    expr: wrapper,
                },
            )
            .unwrap();
        output_columns.push(wrapper_output);
        window_expressions.push(WindowExpression {
            expression: wrapper,
            output: wrapper_output,
        });
    }
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
                expressions: window_expressions.into_boxed_slice(),
            }),
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
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
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

fn contains_error(result: Result<Fragment, ValidationErrors>, expected: &str) -> bool {
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
    finish_topn(SourceDistribution::Singleton, TopNPhase::Single).unwrap();
    let sequence = TopNSequenceId::new(1);
    finish_topn_reduction(sequence, Some(sequence), 15, 0, 10, 5).unwrap();

    assert!(contains_error(
        finish_topn(SourceDistribution::Hash(74), TopNPhase::Final { sequence }),
        "TopN phase lacks its exact distribution contract"
    ));
    finish_topn(
        SourceDistribution::Singleton,
        TopNPhase::Partial { sequence },
    )
    .unwrap();

    let orphan = finish_topn_reduction(sequence, None, 10, 0, 10, 0).unwrap_err();
    assert!(orphan.contains("TopN sequence must have exactly one final node"));

    let mismatch =
        finish_topn_reduction(sequence, Some(TopNSequenceId::new(2)), 10, 0, 10, 0).unwrap_err();
    assert!(mismatch.contains("TopN sequence must have exactly one final node"));

    let partial_offset = finish_topn_reduction(sequence, Some(sequence), 15, 1, 10, 5).unwrap_err();
    assert!(
        partial_offset.contains("partial TopN cannot apply an offset before global completion")
    );

    let undersized_partial =
        finish_topn_reduction(sequence, Some(sequence), 10, 0, 10, 5).unwrap_err();
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
    let fragment = finish_window(|_, _| frame, WindowFixtureLayout::Exact).unwrap();
    let NodeKind::Window(spec) = &fragment.nodes()[&fragment.root()].kind else {
        panic!("expected window root");
    };
    let ExprKind::WindowCall {
        frame: Some(frame), ..
    } = &fragment
        .expressions()
        .get(spec.expressions[0].expression)
        .expect("window expression")
        .kind
    else {
        panic!("expected window call");
    };
    assert_eq!(frame.exclusion, WindowFrameExclusion::Ties);

    assert!(contains_error(
        finish_window(
            |_, _| WindowFrame {
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
            |_, _| WindowFrame {
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
            |builder, owner| {
                let start = builder
                    .add_expression(
                        owner,
                        ty(DataType::UInt64, false),
                        ExprKind::Literal(LiteralValue::UInt64(start_offset)),
                    )
                    .unwrap();
                let end = builder
                    .add_expression(
                        owner,
                        ty(DataType::UInt64, false),
                        ExprKind::Literal(LiteralValue::UInt64(end_offset)),
                    )
                    .unwrap();
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
            "window frame start follows its end after comparing exact offsets"
        ));
    }

    assert!(contains_error(
        finish_window(
            |builder, owner| {
                let offset = builder
                    .add_expression(
                        owner,
                        ty(DataType::Int64, false),
                        ExprKind::Literal(LiteralValue::Int64(1)),
                    )
                    .unwrap();
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
fn window_call_cannot_be_reused_as_an_expression_child() {
    let error = finish_window(
        |_, _| WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        },
        WindowFixtureLayout::ReusedCallAsChild,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("window call must be a top-level expression of its owning Window node"));
}

#[test]
fn window_accepts_singleton_coarser_hash_and_stronger_ordering() {
    let frame = |_: &mut FragmentBuilder, _: NodeId| WindowFrame {
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
            keys: Box::from([key]),
            labels: Box::from([Box::<str>::from("account_id")]),
            message: "duplicate mutation match".into(),
        }
    })
    .unwrap();

    finish_assertion(SourceDistribution::Singleton, |key, _| {
        RowCountAssertionSpec::PerKeyAtMostOne {
            keys: Box::from([key]),
            labels: Box::from([Box::<str>::from("account_id")]),
            message: "duplicate mutation match".into(),
        }
    })
    .unwrap();
    finish_assertion(SourceDistribution::Hash(75), |key, order| {
        RowCountAssertionSpec::PerKeyAtMostOne {
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
                    function_id: FunctionId::try_new("builtin/generate_one/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("empty-to-i64").unwrap(),
                    argument_types: Box::default(),
                    result_types: Box::from([ty(DataType::Int64, false)]),
                    volatility: if volatile {
                        FunctionVolatility::Volatile
                    } else {
                        FunctionVolatility::Immutable
                    },
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                },
                arguments: Box::default(),
                outputs,
                left_outer: false,
            },
        })
        .unwrap();
    builder.finish_definition(node, sink, dop())
}
