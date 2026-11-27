// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact grouped-state installation, independent use sites and fresh channels.
//! Existing Rows tests do not exercise intermediate call scope or state output.
//! The oracle is the declared physical binding/source and explicit key order,
//! not runtime arithmetic. Inputs/catalog are local; no process/fixture cleanup
//! or timing gate is needed. LocalCompiler owns this short affected-change lane.

use super::*;
use crate::expressions::checked_reference_tests::values_lowering_tests::{
    build, checked_pool, compile_options, providers as value_providers,
};
use arrow_array::{ArrayRef, Int64Array};
use novarocks_local_program::{ProgramAggregateCallScope, ProgramCallSite, ProgramStateTemplate};

fn append_count_chain(
    builder: &mut FragmentBuilder,
    source: NodeId,
    keys: &[ValueId],
    count: &CountStar,
    ordering: &[(usize, SortDirection, NullOrdering)],
    producer_distinct: bool,
) -> NodeId {
    let partial = NodeId::new(201);
    let grouped = NodeId::new(202);
    let final_aggregate = NodeId::new(203);
    let final_topn = NodeId::new(204);
    let sequence = AggregateSequenceId::new(41);
    let mut input = source;
    let mut prior = None;
    let mut intermediate = None;
    for (node, id, phase) in [
        (partial, 51, AggregatePhase::Partial { sequence }),
        (grouped, 52, AggregatePhase::Intermediate { sequence }),
        (final_aggregate, 53, AggregatePhase::Final { sequence }),
    ] {
        let mut groups = Vec::new();
        for key in keys {
            let ty = builder.value(*key).unwrap().ty.clone();
            groups.push((
                builder
                    .fixture_value_root(node, ty, ExprKind::Value(*key))
                    .unwrap(),
                *key,
            ));
        }
        let arguments = prior
            .map(|value| {
                let ty = builder.value(value).unwrap().ty.clone();
                Box::from([builder
                    .fixture_value_root(node, ty, ExprKind::Value(value))
                    .unwrap()])
            })
            .unwrap_or_default();
        let output = builder
            .add_value(
                count.output_type(phase),
                if phase.produces_final_result() {
                    ValueOrigin::AggregateResult {
                        call: AggregateCallId::new(id),
                    }
                } else {
                    ValueOrigin::AggregateState {
                        call: AggregateCallId::new(id),
                        phase,
                    }
                },
            )
            .unwrap();
        let mut call = count.call(id, phase, arguments, output);
        if node == partial {
            call.distinct = producer_distinct;
        }
        let required = builder.node_output_properties(input).unwrap().clone();
        let mut produced = required.clone();
        produced.ordering = Box::default();
        let kind = if node == grouped {
            let order = ordering
                .iter()
                .map(|&(ordinal, direction, null_ordering)| SortExpr {
                    expr: builder
                        .fixture_repeated_root(node, groups[ordinal].0)
                        .unwrap(),
                    direction,
                    null_ordering,
                })
                .collect::<Box<[_]>>();
            produced.ordering = order
                .iter()
                .zip(ordering.iter().map(|entry| entry.0))
                .map(|(key, ordinal)| OrderingKey {
                    value: keys[ordinal],
                    direction: key.direction,
                    null_ordering: key.null_ordering,
                })
                .collect();
            intermediate = Some(output);
            NodeKind::TopN {
                order_by: order,
                limit: 2,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(42),
                },
                reduction: TopNReduction::GroupedStates {
                    group_by: groups.into_boxed_slice(),
                    calls: Box::from([call]),
                    comparator:
                        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            }
        } else {
            NodeKind::Aggregate {
                group_by: groups.into_boxed_slice(),
                calls: Box::from([call]),
                grouping: if node == partial {
                    AggregateGrouping::Partial
                } else {
                    AggregateGrouping::Complete
                },
            }
        };
        let mut outputs = keys.to_vec();
        outputs.push(output);
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: node,
                inputs: Box::from([input]),
                required_inputs: Box::from([required]),
                output_properties: produced,
                output: OutputPort {
                    node,
                    columns: outputs.into_boxed_slice(),
                },
                kind,
            })
            .unwrap();
        input = node;
        prior = Some(output);
    }
    let order = ordering
        .iter()
        .map(|&(ordinal, direction, null_ordering)| {
            let ty = builder.value(keys[ordinal]).unwrap().ty.clone();
            SortExpr {
                expr: builder
                    .fixture_value_root(final_topn, ty, ExprKind::Value(keys[ordinal]))
                    .unwrap(),
                direction,
                null_ordering,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .add_top_n(
            final_topn,
            final_aggregate,
            order,
            2,
            0,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(42),
            },
        )
        .unwrap();
    assert_ne!(intermediate.unwrap(), prior.unwrap());
    final_topn
}

fn grouped_package() -> (Arc<FragmentPackage>, PureEngineFunctionCatalog) {
    let catalog = count_catalog();
    let count = CountStar::bind(&catalog);
    let pool = checked_pool(
        Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef,
        true,
        ValueLogicalType::Physical,
    );
    let base = build(
        &[pool],
        &[vec![(0, 0), (0, 2)], vec![(0, 1), (0, 0)]],
        &[0, 0],
        false,
    );
    let mut builder = FragmentBuilder::new(base.fragment().id());
    for value in base.fragment().values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for node in base.fragment().nodes().values() {
        builder.insert_node_unchecked(node.clone()).unwrap();
    }
    let values = base.fragment().root();
    let keys = base.fragment().nodes()[&values].output.columns.to_vec();
    let final_topn = append_count_chain(
        &mut builder,
        values,
        &keys,
        &count,
        &[
            (1, SortDirection::Descending, NullOrdering::First),
            (0, SortDirection::Ascending, NullOrdering::Last),
        ],
        false,
    );
    let fragment = finish_with_requests(builder, final_topn, FragmentSink::Result, &catalog);
    let mut input = base.as_ref().clone().into_input();
    let output = fragment.nodes()[&final_topn].output.clone();
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: ResultValueDomain::Plain,
                name: format!("field-{ordinal}").into(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect(),
    });
    input.fragment = fragment;
    let package =
        Arc::new(FragmentPackage::try_new(input, package_admission(), &FixtureControl).unwrap());
    (package, catalog)
}

pub(crate) fn ordered_scan_package(
    nullable: bool,
    direction: SortDirection,
    null_ordering: NullOrdering,
) -> (Arc<FragmentPackage>, PureEngineFunctionCatalog) {
    ordered_scan_package_with_distinct(nullable, direction, null_ordering, false)
}

fn ordered_scan_plan(
    nullable: bool,
    direction: SortDirection,
    null_ordering: NullOrdering,
    producer_distinct: bool,
) -> (PlanBuilder, PureEngineFunctionCatalog) {
    let catalog = count_catalog();
    let count = CountStar::bind(&catalog);
    let ty = ValueType::new(DataType::Utf8, nullable);
    let mut builder = FragmentBuilder::new(PROBE);
    let keys = add_scan_typed(
        &mut builder,
        Distribution::Singleton,
        std::slice::from_ref(&ty),
    );
    let root = append_count_chain(
        &mut builder,
        SCAN,
        &keys,
        &count,
        &[(0, direction, null_ordering)],
        producer_distinct,
    );
    builder.attach_runtime_filter(FILTER).unwrap();
    let fragment = finish_with_requests(builder, root, FragmentSink::Result, &catalog);
    let result = ResultPort {
        scalar_schema: None,
        fragment: PROBE,
        output: fragment.nodes()[&root].output.clone(),
        fields: fragment.nodes()[&root]
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: ResultValueDomain::Plain,
                name: format!("field-{ordinal}").into(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect(),
    };
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(result).unwrap();
    plan.add_runtime_filter(ordered_filter(
        PROBE,
        NodeId::new(201),
        SCAN,
        NodeId::new(202),
        TopNPhase::Partial {
            sequence: TopNSequenceId::new(42),
        },
        keys[0],
        ty,
        direction,
        null_ordering,
        2,
    ))
    .unwrap();
    (plan, catalog)
}

fn ordered_scan_package_with_distinct(
    nullable: bool,
    direction: SortDirection,
    null_ordering: NullOrdering,
    producer_distinct: bool,
) -> (Arc<FragmentPackage>, PureEngineFunctionCatalog) {
    let (plan, catalog) = ordered_scan_plan(nullable, direction, null_ordering, producer_distinct);
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let read = frozen_read_typed(
        true,
        &[ConnectorValueType::Varchar],
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "v0",
            DataType::Utf8,
            nullable,
        )]),
        vec![dynamic_filter()],
    );
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]),
        &BTreeMap::new(),
        &BTreeMap::from([(
            PROBE,
            FrozenFragmentPruning::try_new(PROBE, vec![], &FixtureControl).unwrap(),
        )]),
        &BTreeMap::from([(PROBE, package_admission())]),
        &FixtureControl,
    )
    .unwrap();
    (Arc::new(packages.remove(&PROBE).unwrap()), catalog)
}

#[test]
fn ordered_topn_compiler_installs_exact_existing_roots_bindings_and_complete_types() {
    use novarocks_local_program::{
        FilterLateApplyGranularity, FilterNullOrder, FilterSortDirection,
    };
    for nullable in [false, true] {
        for direction in [SortDirection::Ascending, SortDirection::Descending] {
            for null_ordering in [NullOrdering::First, NullOrdering::Last] {
                let (package, catalog) = ordered_scan_package(nullable, direction, null_ordering);
                let validated =
                    validate_fragment_providers(package.clone(), &providers(), &FixtureControl)
                        .unwrap();
                let program =
                    compile_fragment(validated, &catalog, options(None), &FixtureControl).unwrap();
                let (aggregate_id, aggregate) = program
                    .graph()
                    .nodes()
                    .iter()
                    .enumerate()
                    .find_map(|(id, node)| match node.kind() {
                        ProgramNodeKind::Aggregate {
                            group_by,
                            topn_filters,
                            ..
                        } if !topn_filters.is_empty() => {
                            Some((ProgramNodeId::new(id), (group_by, topn_filters)))
                        }
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(aggregate.1.len(), 1);
                let filter = &aggregate.1[0];
                assert_eq!(filter.group_key_ordinal, 0);
                assert_eq!(filter.group_key_expr, aggregate.0[0]);
                assert_eq!(filter.limit.get(), 2);
                assert_eq!(filter.producer.kind(), FilterProducerKind::OrderedBound);
                assert_eq!(
                    filter.producer.reduction(),
                    FilterReduction::TightenOrderedBound
                );
                let (scan_id, scan) = program
                    .graph()
                    .nodes()
                    .iter()
                    .enumerate()
                    .find_map(|(id, node)| match node.kind() {
                        ProgramNodeKind::Scan {
                            runtime_filters, ..
                        } => Some((ProgramNodeId::new(id), runtime_filters)),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(scan.len(), 1);
                let consumer = &scan[0];
                assert_eq!(consumer.consumer.contract(), filter.producer.contract());
                assert_eq!(
                    consumer.consumer.activation(),
                    FilterConsumerActivation::NonBlockingLive {
                        late_apply: FilterLateApplyGranularity::Batch
                    }
                );
                assert_eq!(consumer.consumer.channel_id(), FILTER.get());
                for (role, actual) in [
                    (
                        RuntimeFilterBindingRole::Producer(0),
                        filter.producer.binding_id(),
                    ),
                    (
                        RuntimeFilterBindingRole::Consumer(0),
                        consumer.consumer.binding_id(),
                    ),
                ] {
                    let original = package
                        .cuts()
                        .runtime_filter_bindings
                        .iter()
                        .find(|binding| binding.role == role)
                        .unwrap();
                    assert_eq!(actual, original.binding_id);
                    assert_ne!(actual, FILTER.get());
                }
                let StaticFilterContract::Ordered { keys, .. } = filter.producer.contract() else {
                    unreachable!()
                };
                assert_eq!(keys[0].value_type, ValueType::new(DataType::Utf8, nullable));
                assert_eq!(
                    keys[0].direction,
                    if direction == SortDirection::Ascending {
                        FilterSortDirection::Ascending
                    } else {
                        FilterSortDirection::Descending
                    }
                );
                assert_eq!(
                    keys[0].null_order,
                    if null_ordering == NullOrdering::First {
                        FilterNullOrder::First
                    } else {
                        FilterNullOrder::Last
                    }
                );
                let (group_unit, group_root) =
                    crate::test_units::compiled_root(&program, filter.group_key_expr);
                let (scan_unit, scan_root) =
                    crate::test_units::compiled_root(&program, consumer.expr_id);
                assert_eq!(filter.group_key_expr, aggregate.0[0]);
                assert_eq!(
                    group_unit.control_flow().uses()[&group_root.use_id].definition,
                    group_root.definition
                );
                assert_eq!(
                    scan_unit.control_flow().uses()[&scan_root.use_id].definition,
                    scan_root.definition
                );
                assert_ne!(filter.group_key_expr.unit, consumer.expr_id.unit);
                assert_eq!(
                    crate::test_units::input_ordinal(&program, consumer.expr_id),
                    0
                );
                let _ = (aggregate_id, scan_id);
            }
        }
    }
}

#[test]
fn grouped_topn_installs_intermediate_scope_independent_roots_and_fresh_state_channels() {
    let (package, catalog) = grouped_package();
    let program = compile_fragment(
        value_providers(Arc::clone(&package)),
        &catalog,
        compile_options(),
        &FixtureControl,
    )
    .unwrap();
    let (index, node) = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ProgramNodeKind::GroupedTopN { .. }))
        .unwrap();
    let id = ProgramNodeId::new(index);
    let ProgramNodeKind::GroupedTopN {
        input,
        group_by,
        order_by,
        rank_group_ordinals,
        functions,
        comparator,
        limit,
    } = node.kind()
    else {
        unreachable!()
    };
    assert_eq!(rank_group_ordinals, &[1, 0]);
    assert_eq!(*limit, 2);
    assert_eq!(
        *comparator,
        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1
    );
    assert!(!order_by[0].asc && order_by[0].nulls_first);
    assert!(order_by[1].asc && !order_by[1].nulls_first);
    assert_eq!(group_by.len(), 2);
    assert_eq!(functions.len(), 1);
    let site = ProgramCallSite::TopNState { node: id, call: 0 };
    let Some(ProgramStateTemplate::Aggregate { scope, kernel }) = program.state_template(site)
    else {
        panic!("exact grouped-state template");
    };
    assert_eq!(
        scope,
        ProgramAggregateCallScope::TopNState { node: id, call: 0 }
    );
    assert_eq!(
        kernel.contract().phase(),
        AggregateKernelPhase::Intermediate
    );
    assert_eq!(kernel.contract().call().selected().argument_types.len(), 0);
    assert_eq!(functions[0].inputs.len(), 1);
    let child = &program.graph().nodes()[input.index()];
    assert_ne!(
        node.output_layout().slots()[2],
        child.output_layout().slots()[2]
    );
    assert_eq!(
        node.output_layout().schema().field(2).data_type(),
        kernel.contract().intermediate_type().data_type()
    );
    let references = [
        group_by[0],
        group_by[1],
        order_by[0].expr,
        order_by[1].expr,
        functions[0].inputs[0],
    ];
    let mut uses = Vec::new();
    for reference in references {
        let (unit, root) = crate::test_units::compiled_root(&program, reference);
        uses.push((reference.unit, root.use_id));
        assert_eq!(
            unit.scope(root.scope).unwrap().schema().as_ref(),
            child.output_layout().schema().as_ref()
        );
        assert!(
            unit.input(root.use_id).unwrap().ordinal < child.output_layout().slots().len() as u32
        );
    }
    assert_eq!(
        uses.iter().collect::<std::collections::BTreeSet<_>>().len(),
        5
    );
}

#[test]
fn ordered_topn_distinct_count_chain_is_refused_before_compilation() {
    // The original GroupedStates TopN state-path rejection is proved by
    // physical-plan's aggregate_sequence_contract::grouped_topn_rejects_missing_contributions_row_budgets_and_binding_drift
    // for its actual DistinctState input. The native COUNT owner refuses the
    // real DISTINCT preparation before this compiler can receive that plan.
    let catalog = count_catalog();
    let count = CountStar::bind(&catalog);
    let selected = Arc::new(count.selected.clone());
    let context = RelationEffectContext::call(RelationCallId::new(0));
    let parameters = SemanticParameters::try_new([]).unwrap();
    let result = catalog.prepare_fresh(
        CallEffectInput {
            context: context.into(),
            argument_uses: CallArgumentUses::SelectedChannels(&[]),
            function_id: &count.function_id,
            kind: FunctionKind::Aggregate,
            selected: selected.as_ref(),
            request: FunctionBindingRequest {
                arguments: &[],
                logical_argument_count: 0,
                expected_result_type: None,
            },
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Relation(context.identity),
        },
        Arc::clone(&selected),
        PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::primitive(context, ExpressionEffects::PURE_VALUE),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase: AggregateKernelPhase::Partial,
                distinct: true,
                order_keys: Arc::from([]),
                state_input_type: None,
            },
        },
        &FixtureControl,
    );
    match result {
        Err(novarocks_functions::FunctionSpecializationFailure::Kernel(
            novarocks_functions::KernelFailure::InvalidProgram(diagnostic),
        )) => assert_eq!(
            diagnostic.message(),
            "COUNT lifecycle does not implement DISTINCT or function ORDER BY"
        ),
        Err(other) => panic!("original COUNT preparation cause: {other}"),
        Ok(_) => panic!("native COUNT DISTINCT must refuse before compilation"),
    }
}
