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
use crate::analysis::ProjectItem;
use crate::planner::distributed::build::lowered_draft::{
    AggregateStateEndpoint, AggregateStateTransport, CheckedAggregateLogicalSourceEntry,
    SqlSourceJournalError,
};
use crate::planner::distributed::build::physical_aggregate_requests::{
    PhysicalAggregateRequestError, author_physical_aggregate_merge_request_observed,
};
use crate::planner::payload::PlanProjectNode;

fn local(name: &str, args: Vec<TypedExpr>) -> PhysicalPlanNode {
    let mut result = aggregate(name, args, vec![], values(vec![], vec![vec![]]));
    let PhysicalPlanKind::HashAggregate(spec) = &mut result.kind else {
        unreachable!()
    };
    let intermediate = spec.aggregates[0]
        .source
        .binding()
        .selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let mut state = column(
        91,
        "state",
        intermediate.data_type.clone(),
        intermediate.nullable,
    );
    state.value_type = intermediate;
    spec.mode = AggMode::Local;
    spec.aggregates[0].result_type = state.value_type.data_type.clone();
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
    spec.output_columns = vec![state.clone()];
    result.output_columns = vec![state];
    result
}
fn merge(
    name: &str,
    args: Vec<TypedExpr>,
    child: PhysicalPlanNode,
    intermediate: bool,
) -> PhysicalPlanNode {
    let mut result = aggregate(name, args, vec![], child);
    let PhysicalPlanKind::HashAggregate(spec) = &mut result.kind else {
        unreachable!()
    };
    spec.mode = if intermediate {
        AggMode::DistinctLocal
    } else {
        AggMode::Global
    };
    spec.is_merge = vec![true];
    if intermediate {
        let ty = spec.aggregates[0]
            .source
            .binding()
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
            .clone();
        let mut state = column(91, "intermediate_state", ty.data_type.clone(), ty.nullable);
        state.value_type = ty;
        spec.aggregates[0].result_type = state.value_type.data_type.clone();
        spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
        spec.output_columns = vec![state.clone()];
        result.output_columns = vec![state];
    }
    result
}
fn gather(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let columns = child.output_columns.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: columns.clone(),
        }),
        children: vec![child],
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn union(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
    let mut result = left.output_columns[0].clone();
    result.column_id = ColumnId(190);
    result.name = "union_state".into();
    let mappings = vec![left.output_columns.clone(), right.output_columns.clone()];
    PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![result.clone()],
            child_output_columns: mappings,
        }),
        children: vec![left, right],
        output_columns: vec![result],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn duplicate_alias(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let state = &child.output_columns[0];
    let mut alias = state.clone();
    alias.column_id = ColumnId(191);
    alias.name = "alias".into();
    let item = ProjectItem {
        expr: reference(state),
        output_name: alias.name.clone(),
        output_column_id: alias.column_id,
    };
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![item.clone(), item],
            output_qualifier: None,
        }),
        children: vec![child],
        output_columns: vec![alias.clone(), alias],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn single_alias(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let state = &child.output_columns[0];
    let mut alias = state.clone();
    alias.column_id = ColumnId(192);
    alias.name = "single_alias".into();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: reference(state),
                output_name: alias.name.clone(),
                output_column_id: alias.column_id,
            }],
            output_qualifier: None,
        }),
        children: vec![child],
        output_columns: vec![alias],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn final_entry(owner: &SqlAuthoredPhysicalPlan) -> CheckedAggregateLogicalSourceEntry<'_> {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                if !matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                    continue;
                }
                let control = Control::default();
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                        .unwrap();
                let entry = owner
                    .checked_aggregate_source_observed(
                        fragment,
                        node,
                        novarocks_physical_plan::PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: u32::try_from(ordinal).unwrap(),
                        },
                        call,
                        &mut work,
                    )
                    .unwrap();
                work.finish().unwrap();
                return entry;
            }
        }
    }
    panic!("actual complete source must emit a Final call");
}
type ObservedEmission = (AggregateStateEndpoint, AggregatePhase, usize, Option<u32>);
type ObservedLink = (u32, u32, AggregateStateEndpoint);
type ObservedTransport = (
    AggregateStateEndpoint,
    AggregateStateTransport,
    Vec<ObservedLink>,
);

#[derive(Default)]
struct Visit {
    emissions: Vec<ObservedEmission>,
    transports: Vec<ObservedTransport>,
}
fn visit(
    entry: &CheckedAggregateLogicalSourceEntry<'_>,
    control: &dyn PureCompileControl,
) -> Result<Visit, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let graph = entry.state_inputs_observed(&mut work)?;
        let mut result = Visit::default();
        graph.visit_observed(
            &mut work,
            |producer, endpoint, work| {
                assert_eq!(producer.source().output, endpoint.value);
                assert_eq!(producer.node().id, endpoint.node);
                assert_eq!(producer.fragment().id(), endpoint.fragment);
                let request = producer.captured().request();
                let ordinal = request
                    .arguments
                    .first()
                    .and_then(|arg| value(arg).1.map(|cv| cv.ordinal()));
                result.emissions.push((
                    endpoint,
                    producer.phase(),
                    request.logical_argument_count,
                    ordinal,
                ));
                work.step()?;
                Ok(())
            },
            |endpoint, kind, links, work| {
                result.transports.push((
                    endpoint,
                    kind,
                    links
                        .iter()
                        .map(|link| (link.input_ordinal, link.mapping_ordinal, link.source))
                        .collect(),
                ));
                work.step()?;
                Ok(())
            },
        )?;
        Ok(result)
    })();
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn check_merge(
    entry: &CheckedAggregateLogicalSourceEntry<'_>,
    control: &dyn PureCompileControl,
) -> Result<(), PhysicalAggregateRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let graph = entry
            .state_inputs_observed(&mut work)
            .map_err(|error| match error {
                SqlSourceJournalError::Control(cause) => {
                    PhysicalAggregateRequestError::Control(cause)
                }
                _ => PhysicalAggregateRequestError::InvalidSource(
                    "fixture state input unexpectedly absent",
                ),
            })?;
        let request = author_physical_aggregate_merge_request_observed(entry, &mut work)?;
        assert_eq!(request.state_inputs().root(), graph.root());
        assert!(std::ptr::eq(
            request.request().arguments,
            entry.captured().request().arguments
        ));
        assert!(std::ptr::eq(request.source(), entry.source()));
        assert!(std::ptr::eq(request.node(), entry.node()));
        assert_eq!(request.site(), entry.site());
        Ok(())
    })();
    if matches!(&result, Err(PhysicalAggregateRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn state_sources_grouped_topn_refuses_transported_state_without_a_rows_fallback() {
    fn topn(child: PhysicalPlanNode, phase: SqlTopNPhase) -> PhysicalPlanNode {
        let column = child.output_columns[0].clone();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(crate::planner::physical::PhysicalTopNNode {
                items: vec![SortItem {
                    expr: reference(&column),
                    asc: true,
                    nulls_first: false,
                }],
                limit: Some(1),
                offset: Some(0),
                phase,
                is_split: phase == SqlTopNPhase::Final,
            }),
            output_columns: child.output_columns.clone(),
            children: vec![child],
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    // Reuse the original actual Partial -> Project -> Stream -> Final source
    // fixture. This unsupported author shape must fail before row-quota
    // transport can discard state contributions. The oracle is its named
    // lowering refusal; no service, historical evidence or runtime hook is
    // needed. SQL owns this short source-family regression on affected changes.
    let partial = topn(single_alias(local("count", vec![])), SqlTopNPhase::Partial);
    let plan = topn(
        merge("count", vec![], gather(partial), false),
        SqlTopNPhase::Final,
    );
    let control = Control::default();
    let result = lower_final_physical_plan(
        &plan,
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        crate::compiler::SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &control,
    );
    let Err(error) = result else {
        panic!("transported state must not be lowered as ordinary Rows");
    };
    assert!(
        matches!(
            &error,
            ContractLoweringError::UnsupportedTopNShape {
                detail: "GroupedStates TopN requires a direct partial aggregate producer"
            }
        ),
        "{error:?}"
    );
}

#[test]
fn state_sources_count_star_stream_loans_exact_actual_partial_and_final_own_request() {
    let source = merge("count", vec![], gather(local("count", vec![])), false);
    let owner = authored(&source, &Control::default()).unwrap();
    let entry = final_entry(&owner);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let graph = entry.state_inputs_observed(&mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(graph.root().fragment, entry.fragment().id());
    assert_eq!(graph.root().node, entry.node().inputs[0]);
    let ContractExprKind::Value(state) = entry
        .fragment()
        .expressions()
        .get(entry.source().arguments[0])
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    assert_eq!(graph.root().value, state);
    let actual = visit(&entry, &Control::default()).unwrap();
    assert_eq!(actual.emissions.len(), 1);
    assert_eq!(actual.emissions[0].2, 0);
    assert_eq!(actual.transports.len(), 1);
    let (_, AggregateStateTransport::Stream(edge), links) = &actual.transports[0] else {
        panic!("actual stream transport")
    };
    let wire = owner.plan().edges().get(edge).unwrap();
    assert_eq!(wire.source.fragment, actual.emissions[0].0.fragment);
    assert_eq!(links.as_slice(), &[(0, 0, actual.emissions[0].0)]);
    assert!(
        entry.canonical().is_none(),
        "Final still owns its own logical capture"
    );
    check_merge(&entry, &Control::default()).unwrap();
}

#[test]
fn state_sources_project_repeated_mapping_keeps_order_without_duplicate_emission_visits() {
    // The original sequence author refuses a repeated Stream mapping. A real
    // single-output Project before the Stream preserves that original gate.
    let rejected = merge(
        "count",
        vec![],
        gather(duplicate_alias(local("count", vec![]))),
        false,
    );
    let Err(ContractLoweringError::Validation(errors)) = authored(&rejected, &Control::default())
    else {
        panic!("the original repeated Stream sequence must be refused")
    };
    let first = errors.errors().first().unwrap();
    assert!(first.path().starts_with("aggregate_sequences["));
    assert_eq!(
        first.message(),
        "aggregate state paths do not reduce exactly into their matching final"
    );

    let source = merge(
        "count",
        vec![],
        gather(single_alias(duplicate_alias(local("count", vec![])))),
        false,
    );
    let owner = authored(&source, &Control::default()).unwrap();
    let entry = final_entry(&owner);
    let actual = visit(&entry, &Control::default()).unwrap();
    assert_eq!(actual.emissions.len(), 1);
    assert_eq!(actual.transports.len(), 3);
    let projects = actual
        .transports
        .iter()
        .filter(|(_, kind, _)| *kind == AggregateStateTransport::Project)
        .collect::<Vec<_>>();
    assert_eq!(projects.len(), 2);
    let outer = projects[0];
    let inner = projects[1];
    assert_ne!(outer.0.node, inner.0.node);
    assert_eq!(
        inner.2.as_slice(),
        &[(0, 0, actual.emissions[0].0), (0, 1, actual.emissions[0].0)]
    );
    assert_eq!(outer.2.as_slice(), &[(0, 0, inner.0)]);
    let stream = actual
        .transports
        .iter()
        .find(|(_, kind, _)| matches!(kind, AggregateStateTransport::Stream(_)))
        .unwrap();
    assert_eq!(stream.2.as_slice(), &[(0, 0, outer.0)]);
    let fragment = &owner.plan().fragments()[&inner.0.fragment];
    let inner_node = fragment.nodes().get(&inner.0.node).unwrap();
    let outer_node = fragment.nodes().get(&outer.0.node).unwrap();
    assert_eq!(
        inner_node.output.columns.as_ref(),
        &[inner.0.value, inner.0.value]
    );
    assert_eq!(outer_node.output.columns.as_ref(), &[outer.0.value]);
    assert_eq!(outer_node.inputs.as_ref(), &[inner.0.node]);
    check_merge(&entry, &Control::default()).unwrap();
}

#[test]
fn state_sources_union_two_partials_and_intermediate_keep_every_actual_source_in_order() {
    let contributions = union(local("count", vec![]), local("count", vec![]));
    let intermediate = merge("count", vec![], contributions, true);
    let source = merge("count", vec![], gather(intermediate), false);
    let owner = authored(&source, &Control::default()).unwrap();
    let entry = final_entry(&owner);
    let actual = visit(&entry, &Control::default()).unwrap();
    assert_eq!(actual.emissions.len(), 3);
    assert!(matches!(
        actual.emissions[0].1,
        AggregatePhase::Intermediate { .. }
    ));
    assert!(
        actual.emissions[1..]
            .iter()
            .all(
                |(_, phase, logical, _)| matches!(phase, AggregatePhase::Partial { .. })
                    && *logical == 0
            )
    );
    let union = actual
        .transports
        .iter()
        .find(|(_, kind, _)| *kind == AggregateStateTransport::UnionAll)
        .unwrap();
    assert_eq!(
        union.2.as_slice(),
        &[(0, 0, actual.emissions[1].0), (1, 0, actual.emissions[2].0)]
    );
    let graph_node = owner.plan().fragments()[&union.0.fragment]
        .nodes()
        .get(&union.0.node)
        .unwrap();
    assert_eq!(
        graph_node.inputs.as_ref(),
        &[actual.emissions[1].0.node, actual.emissions[2].0.node]
    );
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    entry
        .state_inputs_observed(&mut work)
        .unwrap()
        .visit_observed(
            &mut work,
            |producer, _, work| {
                assert!(!std::ptr::eq(
                    producer.captured().binding().resolved(),
                    entry.captured().binding().resolved()
                ));
                assert!(
                    !producer
                        .captured()
                        .logical_identity()
                        .same_lineage(entry.captured().logical_identity())
                );
                work.step()?;
                Ok(())
            },
            |_, _, _, _| Ok(()),
        )
        .unwrap();
    work.finish().unwrap();
    check_merge(&entry, &Control::default()).unwrap();
}

#[test]
fn state_sources_none_and_nonzero_cv_keep_own_backing_and_same_lineage_new_revision_is_not_pair_proof()
 {
    let none = TypedExpr {
        kind: ExprKind::Nested(Box::new(integer(7))),
        value_type: ValueType::new(DataType::Int64, false),
    };
    let left = local("min", vec![none.clone()]);
    let PhysicalPlanKind::HashAggregate(spec) = &left.kind else {
        unreachable!()
    };
    let old = spec.aggregates[0].source.clone();
    let field = Arc::new(
        Field::new("actual_cv", DataType::Int64, false)
            .with_metadata([("provider-origin".into(), "original".into())].into()),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ValueType::new(DataType::Int64, false),
        Int64Array::from(vec![101, 202, 303]).to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let right = local(
        "min",
        vec![TypedExpr {
            kind: ExprKind::Constant(pool.value(1).unwrap()),
            value_type: pool.value_type().clone(),
        }],
    );
    let mut source = merge("min", vec![none], gather(union(left, right)), false);
    let PhysicalPlanKind::HashAggregate(spec) = &mut source.kind else {
        unreachable!()
    };
    spec.aggregates[0].source = old.clone();
    spec.aggregates[0].source.rewrite_channels(|_, _| ());
    let owner = authored(&source, &Control::default()).unwrap();
    let entry = final_entry(&owner);
    assert!(
        entry
            .captured()
            .logical_identity()
            .same_lineage(old.logical_identity().unwrap())
    );
    assert!(
        !entry
            .captured()
            .logical_identity()
            .same_revision(old.logical_identity().unwrap())
    );
    assert!(value(&entry.captured().request().arguments[0]).1.is_none());
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut seen = vec![];
    entry
        .state_inputs_observed(&mut work)
        .unwrap()
        .visit_observed(
            &mut work,
            |producer, _, work| {
                let cv = value(&producer.captured().request().arguments[0]).1;
                seen.push(cv.map(|value| value.ordinal()));
                match cv {
                    None => assert!(
                        producer
                            .captured()
                            .logical_identity()
                            .same_revision(old.logical_identity().unwrap())
                    ),
                    Some(value) => {
                        assert_eq!(value.ordinal(), 1);
                        assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
                        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
                        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
                        assert_eq!(producer.captured().constant_policy(), policy());
                        assert!(
                            !producer
                                .captured()
                                .logical_identity()
                                .same_lineage(entry.captured().logical_identity())
                        );
                    }
                }
                work.step()?;
                Ok(())
            },
            |_, _, _, _| Ok(()),
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(seen, vec![None, Some(1)]);
    check_merge(&entry, &Control::default()).unwrap();
    // Source lineage/revision describes transport; it never replaces mandatory
    // state compatibility or proves these contributors share a request value.
}

#[test]
fn state_sources_foreign_same_signature_and_every_actual_visit_merge_control_prefix_refuse() {
    let plan = || merge("count", vec![], gather(local("count", vec![])), false);
    let owner = authored(&plan(), &Control::default()).unwrap();
    let foreign = authored(&plan(), &Control::default()).unwrap();
    let entry = final_entry(&owner);
    let other = final_entry(&foreign);
    assert_eq!(
        entry.source().binding.function,
        other.source().binding.function
    );
    let invoke_foreign = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = owner.checked_aggregate_source_observed(
            other.fragment(),
            other.node(),
            other.site(),
            other.source(),
            &mut work,
        );
        if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
            return result.map(|_| ());
        }
        work.finish()?;
        result.map(|_| ())
    };
    let baseline = Control::default();
    assert!(matches!(
        invoke_foreign(&baseline),
        Err(SqlSourceJournalError::InvalidSource(
            "aggregate journal loans a foreign plan or node"
        ))
    ));
    let expected = baseline.trace.into_inner().unwrap();
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(invoke_foreign(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
    for merge in [false, true] {
        let invoke = |control: &Control| -> Result<(), CompileControlError> {
            if merge {
                check_merge(&entry, control).map_err(|error| match error {
                    PhysicalAggregateRequestError::Control(cause) => cause,
                    other => panic!("unexpected ordinary merge failure {other:?}"),
                })
            } else {
                visit(&entry, control)
                    .map(|_| ())
                    .map_err(|error| match error {
                        SqlSourceJournalError::Control(cause) => cause,
                        other => panic!("unexpected ordinary visit failure {other:?}"),
                    })
            }
        };
        let baseline = Control::default();
        invoke(&baseline).unwrap();
        let expected = baseline.trace.into_inner().unwrap();
        assert!(expected.len() > 1);
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Default::default(),
                    refusal: Some((at, cause)),
                };
                assert_eq!(invoke(&control).unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

#[test]
fn state_sources_wide_320_ordered_project_links_visit_one_emission_and_sample_actual_control() {
    const WIDTH: usize = 320;
    let mut repeated = duplicate_alias(local("count", vec![]));
    let PhysicalPlanKind::Project(project) = &mut repeated.kind else {
        unreachable!()
    };
    project.items.resize(WIDTH, project.items[0].clone());
    repeated
        .output_columns
        .resize(WIDTH, repeated.output_columns[0].clone());
    let source = merge("count", vec![], gather(single_alias(repeated)), false);
    let owner = authored(&source, &Control::default()).unwrap();
    let entry = final_entry(&owner);
    let actual = visit(&entry, &Control::default()).unwrap();
    assert_eq!(actual.emissions.len(), 1);
    assert_eq!(actual.transports.len(), 3);
    let inner = actual
        .transports
        .iter()
        .find(|(_, kind, links)| *kind == AggregateStateTransport::Project && links.len() == WIDTH)
        .unwrap();
    let expected = (0..WIDTH)
        .map(|ordinal| (0, u32::try_from(ordinal).unwrap(), actual.emissions[0].0))
        .collect::<Vec<_>>();
    assert_eq!(inner.2, expected);
    let node = &owner.plan().fragments()[&inner.0.fragment].nodes()[&inner.0.node];
    assert_eq!(node.output.columns.len(), WIDTH);
    assert!(
        node.output
            .columns
            .iter()
            .all(|value| *value == inner.0.value)
    );
    let NodeKind::Project { expressions } = &node.kind else {
        panic!("the wide links must belong to the original Project")
    };
    assert_eq!(expressions.len(), WIDTH);

    // Sample actual callback boundaries rather than predicting how many
    // observations an opaque source lookup or flush will produce.
    for merge in [false, true] {
        let invoke = |control: &Control| -> Result<(), CompileControlError> {
            if merge {
                check_merge(&entry, control).map_err(|error| match error {
                    PhysicalAggregateRequestError::Control(cause) => cause,
                    other => panic!("unexpected ordinary wide merge failure {other:?}"),
                })
            } else {
                visit(&entry, control)
                    .map(|_| ())
                    .map_err(|error| match error {
                        SqlSourceJournalError::Control(cause) => cause,
                        other => panic!("unexpected ordinary wide visit failure {other:?}"),
                    })
            }
        };
        let baseline = Control::default();
        invoke(&baseline).unwrap();
        let expected = baseline.trace.into_inner().unwrap();
        assert!(!expected.is_empty());
        assert!(expected.iter().any(|(_, units)| *units > 0));
        assert!(expected.iter().all(|(_, units)| *units <= 256));
        let mut boundaries = vec![0, expected.len() - 1];
        boundaries.extend(
            expected
                .iter()
                .enumerate()
                .filter_map(|(at, (_, units))| (*units == 256).then_some(at)),
        );
        boundaries.sort_unstable();
        boundaries.dedup();
        for at in boundaries {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Default::default(),
                    refusal: Some((at, cause)),
                };
                assert_eq!(invoke(&control).unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}
