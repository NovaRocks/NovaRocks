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

//! SQL-authored aggregate channels through the frozen package and LocalCompiler.
use arrow::datatypes::DataType;
use novarocks_expression::StaticExprKind;
use novarocks_expression_contract::{ExprKind, RootOrdinal, RootScope};
use novarocks_local_program::{
    DiagnosticSourceNodeId, LocalProgram, ProgramNode, ProgramNodeKind, ProgramRootRef,
};
use novarocks_physical_plan::{
    AggregateCall, AggregatePhase, Distribution, Fragment, FragmentId, NodeKind, PhysicalNode,
    PhysicalPlan, ValueId, ValueOrigin,
};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;
use novarocks_type_contract::EvaluationDemand;

fn aggregates(
    plan: &PhysicalPlan,
) -> impl Iterator<Item = (&Fragment, &PhysicalNode, &[AggregateCall])> {
    plan.fragments().values().flat_map(|fragment| {
        fragment.nodes().values().filter_map(|node| {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                return None;
            };
            Some((fragment, node, calls.as_ref()))
        })
    })
}

fn input_value(fragment: &Fragment, node: &PhysicalNode, root: RootOrdinal) -> ValueId {
    let expression = fragment.root_expression(node.id, root).unwrap();
    let ExprKind::Value(value) = expression.kind else {
        panic!("expected the root to read the existing input directly");
    };
    value
}

fn source_value(
    plan: &PhysicalPlan,
    mut fragment: FragmentId,
    mut value: ValueId,
) -> (FragmentId, ValueId) {
    while let ValueOrigin::ExchangeImport { edge, source_value } =
        &plan.fragments()[&fragment].values()[&value].origin
    {
        fragment = plan.edges()[edge].source.fragment;
        value = *source_value;
    }
    (fragment, value)
}

fn local_aggregate<'a>(program: &'a LocalProgram, source: &PhysicalNode) -> &'a ProgramNode {
    program
        .graph()
        .nodes()
        .iter()
        .find(|node| {
            node.physical_sources() == [DiagnosticSourceNodeId::new(source.id.get())]
                && matches!(node.kind(), ProgramNodeKind::Aggregate { .. })
        })
        .expect("the original Aggregate lowers to a local Aggregate")
}

fn assert_local_read(
    program: &LocalProgram,
    site: ProgramRootRef,
    demand: EvaluationDemand,
    expected_ordinal: usize,
    expected_type: &DataType,
) {
    let unit = program.unit(site.unit).unwrap();
    let root = unit.root(site.root).unwrap();
    let definition = unit.definitions().node(root.definition).unwrap();
    assert!(matches!(definition.kind(), StaticExprKind::Input));
    assert_eq!(definition.value_type().unwrap().data_type(), expected_type);
    assert_eq!(root.scope, RootScope::Port(0));
    assert_eq!(
        unit.input(root.use_id).unwrap().ordinal,
        u32::try_from(expected_ordinal).unwrap()
    );
    assert_eq!(
        unit.control_flow().uses()[&root.use_id].context.demand,
        demand
    );
}

#[test]
fn grouping_sets_carry_the_three_aggregate_inputs_from_before_repeat() {
    let source = sql_source(
        "SELECT k, array_agg(k + 1 ORDER BY k + 2 DESC NULLS LAST) FILTER (WHERE k > 0) \
         FROM fixture.source GROUP BY GROUPING SETS ((k), ())",
        DataType::Int64,
    );
    let plan = source.plan();
    let programs = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
    let (fragment, node, call) = aggregates(plan)
        .flat_map(|(fragment, node, calls)| calls.iter().map(move |call| (fragment, node, call)))
        .find(|(_, _, call)| call.binding.phase.consumes_logical_arguments())
        .expect("the ordered aggregate has an update phase");
    let [argument] = call.arguments.as_ref() else {
        panic!("the FILTER is separate from the value argument");
    };
    let [order] = call.order_by.as_ref() else {
        panic!("the ordered call has its original order key");
    };
    let roots = [
        *argument,
        order.expr,
        call.filter_root.expect("the update reads its FILTER"),
    ];
    let values = roots.map(|root| input_value(fragment, node, root));
    assert!(values[0] != values[1] && values[0] != values[2] && values[1] != values[2]);

    let program = &programs[&fragment.id()];
    let ProgramNodeKind::Aggregate {
        input, functions, ..
    } = local_aggregate(program, node).kind()
    else {
        unreachable!();
    };
    let [local] = functions.as_slice() else {
        panic!("one ordered update call");
    };
    let [argument, order] = local.inputs.as_slice() else {
        panic!("the value and ordering channels remain independent of FILTER");
    };
    assert_eq!(local.order.is_asc_order, [false]);
    assert_eq!(local.order.nulls_first, [false]);
    let filter = local.filter_root.expect("the local call has a FILTER site");
    let input_layout = program.graph().nodes()[input.index()].output_layout();
    let input_values = &fragment.nodes()[&node.inputs[0]].output.columns;

    for ((physical_value, local_site), (data_type, demand)) in
        values.into_iter().zip([*argument, *order, filter]).zip([
            (DataType::Int64, EvaluationDemand::Value),
            (DataType::Int64, EvaluationDemand::Value),
            (DataType::Boolean, EvaluationDemand::TruthOnly),
        ])
    {
        let ordinal = input_values
            .iter()
            .position(|value| *value == physical_value)
            .unwrap();
        assert_eq!(input_layout.schema().field(ordinal).data_type(), &data_type);
        assert_local_read(program, local_site, demand, ordinal, &data_type);
        let (producer_fragment, value) = source_value(plan, fragment.id(), physical_value);
        let producer_fragment = &plan.fragments()[&producer_fragment];
        let ValueOrigin::Expr {
            node: producer,
            root,
        } = producer_fragment.values()[&value].origin
        else {
            panic!("the original row expression must already have been evaluated by Project");
        };
        let producer = &producer_fragment.nodes()[&producer];
        let NodeKind::Project { expressions } = &producer.kind else {
            panic!("the original row input is projected");
        };
        assert!(expressions.contains(&(root, value)));
        let repeat = producer_fragment
            .nodes()
            .values()
            .find(|node| matches!(node.kind, NodeKind::Repeat { .. }))
            .expect("the original projection precedes the Repeat in its source fragment");
        let NodeKind::Repeat {
            grouping_values, ..
        } = &repeat.kind
        else {
            unreachable!();
        };
        let repeat_input = &producer_fragment.nodes()[&repeat.inputs[0]];
        assert!(repeat_input.output.columns.contains(&value));
        assert!(repeat.output.columns.contains(&value));
        assert!(!grouping_values.iter().any(|&(key, _)| key == value));
    }
}

#[test]
fn volatile_filters_keep_independent_inputs_and_order_by_uses_the_selected_alias() {
    let source = sql_source(
        "SELECT k, count(*) FILTER (WHERE random() < 0.5) AS a, \
         count(*) FILTER (WHERE random() < 0.5) AS b \
         FROM fixture.source GROUP BY k ORDER BY a DESC",
        DataType::Int64,
    );
    let plan = source.plan();
    let programs = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
    let mut selected_alias = None;
    let mut updates = 0;
    for (fragment, node, calls) in aggregates(plan) {
        let [first, second] = calls else {
            panic!("two authored volatile occurrences must keep two result values");
        };
        assert_ne!(first.output, second.output);
        if first.binding.phase.produces_final_result() {
            assert!(
                selected_alias
                    .replace((fragment.id(), first.output))
                    .is_none()
            );
        }
        if first.binding.phase.consumes_logical_arguments() {
            updates += 1;
            for call in calls {
                assert!(call.arguments.is_empty());
                assert_eq!(call.binding.logical_argument_count, 0);
            }
            let first = input_value(fragment, node, first.filter_root.unwrap());
            let second = input_value(fragment, node, second.filter_root.unwrap());
            assert_ne!(first, second);
            let program = &programs[&fragment.id()];
            let ProgramNodeKind::Aggregate { functions, .. } =
                local_aggregate(program, node).kind()
            else {
                unreachable!();
            };
            let [first, second] = functions.as_slice() else {
                panic!("two local calls");
            };
            assert_ne!(first.filter_root.unwrap(), second.filter_root.unwrap());
        }
    }
    assert_eq!(updates, 1);
    let alias = selected_alias.expect("the first call produces the output bound by alias a");
    let output = plan.result_port().unwrap();
    assert_eq!(
        source_value(plan, output.fragment, output.fields[1].value),
        alias
    );
    let sort_reads_alias = plan.fragments().values().any(|fragment| {
        fragment.nodes().values().any(|node| {
            let NodeKind::Sort { order_by, .. } = &node.kind else {
                return false;
            };
            order_by.first().is_some_and(|key| {
                source_value(plan, fragment.id(), input_value(fragment, node, key.expr)) == alias
            })
        })
    });
    assert!(sort_reads_alias);
}

#[test]
fn partial_calls_filter_original_rows_and_final_merges_the_matching_states() {
    let source = sql_source(
        "SELECT count(*) FILTER (WHERE k > 0), sum(k) FILTER (WHERE k <= 0) FROM fixture.source",
        DataType::Int64,
    );
    let plan = source.plan();
    let programs = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
    let [
        (partial_fragment, partial_node, partial),
        (final_fragment, final_node, final_calls),
    ] = {
        let mut phases = aggregates(plan).collect::<Vec<_>>();
        phases.sort_by_key(|(_, _, calls)| calls[0].binding.phase.produces_final_result());
        phases
            .try_into()
            .expect("one Partial and one Final aggregate")
    };
    assert_eq!(partial.len(), 2);
    assert_eq!(final_calls.len(), 2);
    assert_ne!(partial_fragment.id(), final_fragment.id());
    let partial_program = &programs[&partial_fragment.id()];
    let final_program = &programs[&final_fragment.id()];
    let ProgramNodeKind::Aggregate {
        functions: local_partial,
        ..
    } = local_aggregate(partial_program, partial_node).kind()
    else {
        unreachable!();
    };
    let ProgramNodeKind::Aggregate {
        functions: local_final,
        ..
    } = local_aggregate(final_program, final_node).kind()
    else {
        unreachable!();
    };
    for ((update, merge), (local_update, local_merge)) in partial
        .iter()
        .zip(final_calls)
        .zip(local_partial.iter().zip(local_final))
    {
        let AggregatePhase::Partial { sequence } = update.binding.phase else {
            panic!("the producer consumes original rows");
        };
        assert_eq!(merge.binding.phase, AggregatePhase::Final { sequence });
        assert!(update.filter_root.is_some());
        assert!(local_update.filter_root.is_some());
        assert!(merge.filter_root.is_none());
        assert!(local_merge.filter_root.is_none());
        assert!(!local_update.input_is_intermediate);
        assert!(local_merge.input_is_intermediate);
        let [state] = merge.arguments.as_ref() else {
            panic!("the merge consumes one already filtered state");
        };
        let imported = input_value(final_fragment, final_node, *state);
        assert_eq!(
            source_value(plan, final_fragment.id(), imported),
            (partial_fragment.id(), update.output)
        );
    }
    assert_ne!(
        partial[0].binding.phase.sequence(),
        partial[1].binding.phase.sequence()
    );
}

#[test]
fn filtered_distinct_and_its_sibling_update_after_a_single_gather() {
    let source = sql_source(
        "SELECT k % 2, count(DISTINCT k) FILTER (WHERE k > 0), sum(k) \
         FROM fixture.source GROUP BY k % 2",
        DataType::Int64,
    );
    let plan = source.plan();
    let programs = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
    let nodes = aggregates(plan).collect::<Vec<_>>();
    let [(fragment, node, [distinct, sibling])] = nodes.as_slice() else {
        panic!("the filtered DISTINCT and its sibling share one Single aggregate");
    };
    assert_eq!(distinct.binding.phase, AggregatePhase::Single);
    assert_eq!(sibling.binding.phase, AggregatePhase::Single);
    assert!(distinct.distinct);
    assert!(distinct.filter_root.is_some());
    assert!(sibling.filter_root.is_none());
    assert_eq!(
        node.required_inputs[0].distribution,
        Distribution::Singleton
    );
    let input = &fragment.nodes()[&node.inputs[0]];
    assert!(matches!(&input.kind, NodeKind::ExchangeSource { edge, .. }
        if plan.edges()[edge].partitioning.destination == Distribution::Singleton));
    let ProgramNodeKind::Aggregate { functions, .. } =
        local_aggregate(&programs[&fragment.id()], node).kind()
    else {
        unreachable!();
    };
    assert!(functions[0].filter_root.is_some());
    assert!(functions[0].order.is_distinct);
    assert!(functions[1].filter_root.is_none());
}
