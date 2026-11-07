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

use super::super::lowered_draft::{
    AggregateStateEndpoint, AggregateStateLink, AggregateStateTransport,
    CheckedWriterAggregateLogicalSourceEntry, SqlSourceJournalError,
};
use super::tests::{column, dop, literal_int, stats, values, version, write_handle};
use super::*;
use crate::compiler::{SqlAuthoredPhysicalPlan, SqlFunctionCatalog};
use crate::planner::distributed::write::auxiliary::{
    WriterStatisticsTargetInput, plan_writer_statistics,
};
use crate::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec;
use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;
use crate::planner::physical::{
    DistributedChangeEventExpandNode, DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
};
use arrow::datatypes::{Field, Schema};
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
};
use novarocks_functions::{EngineFunctionCatalogBuilder, FunctionArgument};
use novarocks_physical_plan::{PhysicalCallSite, PhysicalNode, WriterAggregateCall};
use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
    ConnectorWriteRouteId, StatisticsArtifactIdentity, StatisticsRequiredAggregation,
    StatisticsScanColumn,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::{cell::RefCell, sync::Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn catalogue() -> Arc<dyn SqlFunctionCatalog> {
    let registration = iceberg_theta_registration().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(registration.definition().clone()).unwrap();
    Arc::new(builder.seal_bound().unwrap())
}

// Each branch is authored by the real router, auxiliary planner and Writer
// lowerer. Requirements are distinct provider artifacts, not forged calls.
pub(crate) fn authored(counts: &[usize], nullable: &[bool]) -> SqlAuthoredPhysicalPlan {
    authored_with_catalog(counts, nullable, catalogue())
}

pub(crate) fn authored_with_catalog(
    counts: &[usize],
    nullable: &[bool],
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
) -> SqlAuthoredPhysicalPlan {
    assert_eq!(counts.len(), nullable.len());
    assert!(!counts.is_empty());
    let schemas = nullable
        .iter()
        .map(|nullable| Schema::new(vec![Field::new("order_id", DataType::Int64, *nullable)]))
        .collect::<Vec<_>>();
    let requirements = counts
        .iter()
        .enumerate()
        .map(|(branch, count)| {
            (0..*count)
                .map(|occurrence| {
                    StatisticsRequiredAggregation::try_new(
                        StatisticsScanColumn::try_new(
                            0,
                            "order_id",
                            FunctionValueType::new(DataType::Int64, nullable[branch]),
                        )
                        .unwrap(),
                        ICEBERG_THETA_AGGREGATE_NAME,
                        StatisticsArtifactIdentity::try_new(
                            vec![i32::try_from(branch * 1000 + occurrence + 1).unwrap()],
                            "test-writer-state-v1",
                        )
                        .unwrap(),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let sink = |i: usize| {
        let mut sink = simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal);
        sink.contract.input_columns[0].nullable = nullable[i];
        sink.contract.target.fields[0].column.nullable = nullable[i];
        sink
    };
    // The original writer declaration authors each branch's complete input;
    // the separate Arrow schema above only describes its physical relation.
    let declared_inputs = (0..counts.len()).map(sink).collect::<Vec<_>>();
    let targets = (0..counts.len())
        .map(|i| WriterStatisticsTargetInput {
            target: WriteTargetOrdinal::try_new(u32::try_from(i).unwrap()).unwrap(),
            input_schema: &schemas[i],
            input_columns: &declared_inputs[i].contract.input_columns,
            requirements: &requirements[i],
        })
        .collect::<Vec<_>>();
    let setup = crate::compiler::SqlCompileControl::unbounded();
    let auxiliary = plan_writer_statistics(
        &targets,
        functions.as_ref(),
        DecimalOverflowPolicy::OutputNull,
        crate::constant::test_constant_policy(),
        &setup,
    )
    .unwrap();

    let input = column(1, "order_id", DataType::Int64, false);
    let source = values(vec![input.clone()], vec![vec![literal_int(7)]]);
    let draft = if counts.len() == 1 {
        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        lower_final_physical_write_plan(
            &source,
            version(),
            dop(),
            FinalWriteLowering {
                reads: None,
                write: declared_inputs[0].clone(),
                write_target_ordinal: ordinal,
                auxiliary: &auxiliary,
                targets: FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap(),
            },
            functions,
            false,
            crate::constant::test_constant_policy(),
            &setup,
        )
        .unwrap()
    } else {
        let data = column(2, "order_id", DataType::Int64, false);
        let effect = column(3, "effect", DataType::Int8, false);
        let expanded = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: vec![DistributedChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: vec![DistributedChangeEventOutputExpr {
                        output_column_id: data.column_id,
                        expr: Some(TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: input.column_id,
                                qualifier: None,
                                column: "order_id".into(),
                            },
                            value_type: FunctionValueType::new(DataType::Int64, false),
                        }),
                    }],
                }],
                output_columns: vec![data.clone(), effect.clone()],
                effect_column_id: effect.column_id,
            }),
            children: vec![source],
            output_columns: vec![data, effect],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let routes = (0..counts.len())
            .map(|i| {
                ChangeStreamWriteRouteSpec {
            requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
                route_id: ConnectorWriteRouteId::from_bytes([u8::try_from(i + 1).unwrap(); 32]),
                write_target_ordinal: WriteTargetOrdinal::try_new(u32::try_from(i).unwrap())
                    .unwrap(),
                accepted_effects: vec![ConnectorRowMutationEffect::Replace],
                input_ordinals: vec![ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    0,
                )],


                sink: declared_inputs[i].clone(),
            }
            })
            .collect();
        let handles = (0..counts.len()).map(|i| {
            (
                WriteTargetOrdinal::try_new(u32::try_from(i).unwrap()).unwrap(),
                write_handle(),
            )
        });
        lower_final_change_stream_write_plan(
            &expanded,
            version(),
            dop(),
            FinalChangeStreamWriteLowering {
                reads: None,
                dag: ChangeStreamWriteDagSpec::for_test(1, routes),
                auxiliary: &auxiliary,
                targets: FinalizedWriteTargetSet::try_new(handles).unwrap(),
            },
            functions,
            false,
            crate::constant::test_constant_policy(),
            &setup,
        )
        .unwrap()
    };
    draft.finish_observed(&setup).unwrap()
}
pub(crate) fn final_call(
    owner: &SqlAuthoredPhysicalPlan,
    ordinal: usize,
) -> (
    &Fragment,
    &PhysicalNode,
    PhysicalCallSite,
    &WriterAggregateCall,
) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::TableFinish(finish) = &node.kind {
                return (
                    fragment,
                    node,
                    PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    },
                    &finish.final_aggregates[ordinal],
                );
            }
        }
    }
    panic!("actual Final call is missing")
}
pub(crate) fn final_entry<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    ordinal: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedWriterAggregateLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let (fragment, node, site, call) = final_call(owner, ordinal);
    owner.checked_writer_aggregate_source_observed(fragment, node, site, call, work)
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Emission {
        endpoint: AggregateStateEndpoint,
        target: u32,
        call: u32,
        captured: usize,
    },
    Transport {
        endpoint: AggregateStateEndpoint,
        kind: AggregateStateTransport,
        links: Vec<(u32, u32, AggregateStateEndpoint)>,
    },
    None {
        endpoint: AggregateStateEndpoint,
        target: u32,
        ordinal: usize,
    },
}
#[derive(Clone, Copy)]
enum Refuse {
    Emission,
    Transport,
    None,
}
fn walk(
    owner: &SqlAuthoredPhysicalPlan,
    ordinal: usize,
    control: &dyn PureCompileControl,
    refuse: Option<Refuse>,
) -> Result<Vec<Event>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let events = RefCell::new(Vec::new());
    let result = (|| {
        let final_source = final_entry(owner, ordinal, &mut work)?;
        let final_canonical = final_source.canonical().unwrap();
        assert!(final_canonical.belongs_to(final_source.captured()));
        assert_eq!(
            final_canonical.selected().argument_types.as_ref(),
            final_source
                .source()
                .binding
                .function
                .argument_types
                .as_ref()
        );
        let captured = final_source.captured();
        let state = final_source.state_inputs_observed(&mut work)?;
        let root = state.root();
        assert_eq!(root.value, final_source.source().input);
        let NodeKind::TableFinish(finish) = &final_source.node().kind else {
            panic!("actual Writer Final must belong to TableFinish")
        };
        assert_eq!(
            root.node,
            final_source.node().inputs[finish.inputs.writer_results as usize]
        );
        if let Some(summary) = finish.inputs.summaries.write_counts {
            assert_ne!(summary, finish.inputs.writer_results);
            assert_ne!(root.node, final_source.node().inputs[summary as usize]);
        }
        state.visit_observed(
            &mut work,
            |source, endpoint, work| {
                let NodeKind::TableWriter { target } = &source.node().kind else {
                    panic!("Writer emission")
                };
                let PhysicalCallSite::WriterPartial { call, .. } = source.site() else {
                    panic!("Partial site")
                };
                assert!(std::ptr::eq(
                    source.source(),
                    &target.partial_aggregates[call as usize]
                ));
                assert_eq!(endpoint.value, source.source().output);
                assert_eq!(endpoint.node, source.node().id);
                assert_eq!(endpoint.fragment, source.fragment().id());
                let canonical = source.canonical().unwrap();
                assert!(canonical.belongs_to(source.captured()));
                assert!(!Arc::ptr_eq(final_canonical, canonical));
                assert_eq!(
                    canonical.selected().argument_types.as_ref(),
                    source.source().binding.function.argument_types.as_ref()
                );
                assert!(matches!(
                    &canonical.request().arguments[0],
                    FunctionArgument::Value { constant: None, .. }
                ));
                work.step()?;
                if matches!(refuse, Some(Refuse::Emission)) {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "test refuses actual emission",
                    ));
                }
                events.borrow_mut().push(Event::Emission {
                    endpoint,
                    target: target.write_target_ordinal.get(),
                    call,
                    captured: std::ptr::from_ref(source.captured()) as usize,
                });
                Ok(())
            },
            |endpoint, kind, links: &[AggregateStateLink], work| {
                work.step()?;
                if matches!(refuse, Some(Refuse::Transport)) {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "test refuses actual transport",
                    ));
                }
                events.borrow_mut().push(Event::Transport {
                    endpoint,
                    kind,
                    links: links
                        .iter()
                        .map(|link| (link.input_ordinal, link.mapping_ordinal, link.source))
                        .collect(),
                });
                Ok(())
            },
            |endpoint, work| {
                let fragment = &owner.plan().fragments()[&endpoint.fragment];
                let NodeKind::TableWriter { target } = &fragment.nodes()[&endpoint.node].kind
                else {
                    panic!("non-contribution Writer")
                };
                let ordinal = target
                    .output_schema
                    .fields
                    .iter()
                    .position(|field| field.value == endpoint.value)
                    .unwrap();
                assert_eq!(
                    target.output_schema.fields[ordinal].role,
                    novarocks_physical_plan::WriterRelationFieldRole::Auxiliary
                );
                assert!(
                    target
                        .partial_aggregates
                        .iter()
                        .all(|call| call.output != endpoint.value)
                );
                work.step()?;
                if matches!(refuse, Some(Refuse::None)) {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "test refuses actual non-contribution",
                    ));
                }
                events.borrow_mut().push(Event::None {
                    endpoint,
                    target: target.write_target_ordinal.get(),
                    ordinal,
                });
                Ok(())
            },
        )?;
        // The Final keeps its own loan, even when several independent Partial
        // requests contribute to its input state.
        let again = final_entry(owner, ordinal, &mut work)?;
        assert!(std::ptr::eq(again.captured(), captured));
        Ok(events.into_inner())
    })();
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn assert_topology(
    owner: &SqlAuthoredPhysicalPlan,
    ordinal: usize,
    expected: &[(u32, Option<u32>)],
) -> Vec<Event> {
    let events = walk(owner, ordinal, &Control::default(), None).unwrap();
    let terminals = events
        .iter()
        .filter_map(|event| match event {
            Event::Emission { target, call, .. } => Some((*target, Some(*call))),
            Event::None { target, .. } => Some((*target, None)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(terminals, expected);
    let (_, finish, _, call) = final_call(owner, ordinal);
    let fragment = final_call(owner, ordinal).0;
    let child = &fragment.nodes()[&finish.inputs[0]];
    let root = AggregateStateEndpoint {
        fragment: fragment.id(),
        node: child.id,
        value: call.input,
    };
    assert!(matches!(events.first(), Some(Event::Transport { endpoint, .. }) if *endpoint == root));
    for event in &events {
        let Event::Transport {
            endpoint,
            kind,
            links,
        } = event
        else {
            continue;
        };
        let f = &owner.plan().fragments()[&endpoint.fragment];
        let node = &f.nodes()[&endpoint.node];
        match (kind, &node.kind) {
            (AggregateStateTransport::UnionAll, NodeKind::SetOp { input_mappings, .. }) => {
                assert_eq!(links.len(), expected.len());
                let output = node
                    .output
                    .columns
                    .iter()
                    .position(|value| value == &endpoint.value)
                    .unwrap();
                for (i, (input, mapping, source)) in links.iter().enumerate() {
                    assert_eq!(*input as usize, i);
                    assert_eq!(*mapping as usize, output);
                    assert_eq!(source.fragment, endpoint.fragment);
                    assert_eq!(source.node, node.inputs[i]);
                    assert_eq!(source.value, input_mappings[i][output]);
                }
            }
            (AggregateStateTransport::Stream(edge), NodeKind::ExchangeSource { .. }) => {
                assert_eq!(links.len(), 1);
                let edge = &owner.plan().edges()[edge];
                let (input, mapping, source) = links[0];
                assert_eq!(input, 0);
                assert_eq!(
                    edge.destination.receive_mapping[mapping as usize],
                    (source.value, endpoint.value)
                );
                assert_eq!(source.fragment, edge.source.fragment);
                assert_eq!(
                    source.node,
                    owner.plan().fragments()[&source.fragment].root()
                );
            }
            _ => panic!("unexpected Writer transport"),
        }
    }
    let endpoints = events
        .iter()
        .map(|event| match event {
            Event::Emission { endpoint, .. }
            | Event::Transport { endpoint, .. }
            | Event::None { endpoint, .. } => *endpoint,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        endpoints.len(),
        events.len(),
        "visit each actual endpoint once"
    );
    events
}

#[test]
fn writer_state_single_stream_keeps_actual_partial_and_original_final() {
    let owner = authored(&[1], &[false]);
    let events = assert_topology(&owner, 0, &[(0, Some(0))]);
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        Event::Transport {
            kind: AggregateStateTransport::Stream(_),
            ..
        }
    ));
    let cloned = owner.clone();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let first = final_entry(&owner, 0, &mut work).unwrap();
    let second = final_entry(&cloned, 0, &mut work).unwrap();
    assert!(std::ptr::eq(first.captured(), second.captured()));
    let request = first.captured().request();
    assert_eq!(request.logical_argument_count, 1);
    assert!(
        matches!(&request.arguments[0], FunctionArgument::Value { value_type, constant: None }
        if value_type == &FunctionValueType::new(DataType::Int64, false))
    );
    assert_eq!(
        first.source().binding.function.result_type,
        FunctionValueType::new(DataType::Binary, false)
    );
    work.finish().unwrap();
}
#[test]
fn writer_state_same_channel_retains_both_ordered_writer_contributors() {
    let owner = authored(&[1, 1], &[false, false]);
    let events = assert_topology(&owner, 0, &[(0, Some(0)), (1, Some(0))]);
    assert_eq!(events.len(), 5);
    let captures = events
        .iter()
        .filter_map(|event| match event {
            Event::Emission { captured, .. } => Some(*captured),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_ne!(captures[0], captures[1], "retain both actual source loans");
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let final_source = final_entry(&owner, 0, &mut work).unwrap();
    assert!(
        captures
            .iter()
            .all(|capture| *capture != std::ptr::from_ref(final_source.captured()) as usize)
    );
    work.finish().unwrap();
    assert!(matches!(
        events[0],
        Event::Transport {
            kind: AggregateStateTransport::UnionAll,
            ..
        }
    ));
}
#[test]
fn writer_state_repeated_and_sparse_overlapping_channels_have_real_non_contributions() {
    let owner = authored(&[2, 1], &[false, false]);
    assert_topology(&owner, 0, &[(0, Some(0)), (1, Some(0))]);
    let events = assert_topology(&owner, 1, &[(0, Some(1)), (1, None)]);
    let (fragment, finish, _, _) = final_call(&owner, 1);
    let NodeKind::TableFinish(finish_spec) = &finish.kind else {
        unreachable!()
    };
    assert_eq!(finish_spec.final_aggregates.len(), 2);
    assert_ne!(
        finish_spec.final_aggregates[0].input,
        finish_spec.final_aggregates[1].input
    );
    assert_ne!(
        finish_spec.final_aggregates[0].binding.phase,
        finish_spec.final_aggregates[1].binding.phase
    );
    let no = events
        .iter()
        .find_map(|event| match event {
            Event::None { ordinal, .. } => Some(*ordinal),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        no,
        novarocks_spi::connector::write_stack::WRITE_RELATION_COLUMN_COUNT + 1
    );
    assert_eq!(fragment.nodes().get(&finish.id).unwrap().id, finish.id);
}
#[test]
fn writer_state_disjoint_original_signatures_do_not_drop_sparse_branches() {
    let owner = authored(&[1, 1], &[false, true]);
    assert_topology(&owner, 0, &[(0, Some(0)), (1, None)]);
    assert_topology(&owner, 1, &[(0, None), (1, Some(0))]);
    // This is source provenance, not ExactSignature compatibility permission.
    let (fragment, node, _, _) = final_call(&owner, 0);
    let NodeKind::TableFinish(finish) = &node.kind else {
        unreachable!()
    };
    assert_eq!(finish.final_aggregates.len(), 2);
    assert_eq!(
        fragment.nodes()[&node.inputs[0]].output.columns.len(),
        novarocks_spi::connector::write_stack::WRITE_RELATION_COLUMN_COUNT + 2
    );
}
#[test]
fn writer_state_actual_visitor_callbacks_and_ordinary_refusals_preserve_every_control_prefix() {
    let owner = authored(&[2, 1], &[false, false]);
    for refuse in [
        None,
        Some(Refuse::Emission),
        Some(Refuse::Transport),
        Some(Refuse::None),
    ] {
        let baseline = Control::default();
        let result = walk(&owner, 1, &baseline, refuse);
        assert_eq!(result.is_ok(), refuse.is_none());
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.iter().any(|(_, units)| *units > 0));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(
                    matches!(walk(&owner, 1, &control, refuse), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn writer_state_foreign_equal_owner_cannot_supply_the_final_loan() {
    let owner = authored(&[1], &[false]);
    let foreign = authored(&[1], &[false]);
    let (fragment, node, site, call) = final_call(&foreign, 0);
    let run = |control: &dyn PureCompileControl| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = owner
            .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
            .map(|_| ());
        if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let baseline = Control::default();
    assert!(matches!(
        run(&baseline),
        Err(SqlSourceJournalError::InvalidSource(_))
    ));
    let trace = baseline.trace.into_inner().unwrap();
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(run(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn writer_state_wide_actual_channels_keep_all_routes_and_sample_real_port_scan_quantum() {
    let owner = authored(&[320, 319], &[false, false]);
    let mut emissions = 0;
    let mut absent = 0;
    let mut endpoints = std::collections::BTreeSet::new();
    for ordinal in 0..320 {
        let expected = if ordinal == 319 {
            [(0, Some(319)), (1, None)]
        } else {
            [(0, Some(ordinal as u32)), (1, Some(ordinal as u32))]
        };
        let events = assert_topology(&owner, ordinal, &expected);
        assert_eq!(events.len(), 5);
        for event in events {
            let endpoint = match event {
                Event::Emission { endpoint, .. } => {
                    emissions += 1;
                    endpoint
                }
                Event::None { endpoint, .. } => {
                    absent += 1;
                    endpoint
                }
                Event::Transport { endpoint, .. } => endpoint,
            };
            assert!(
                endpoints.insert(endpoint),
                "distinct channels retain distinct routes"
            );
        }
    }
    assert_eq!(emissions, 639);
    assert_eq!(absent, 1);
    assert_eq!(endpoints.len(), 1_600);
    let (fragment, finish, _, _) = final_call(&owner, 319);
    assert_eq!(
        fragment.nodes()[&finish.inputs[0]].output.columns.len(),
        324
    );
    let baseline = Control::default();
    walk(&owner, 319, &baseline, None).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    // The checked merge input scans all 324 actual child output occurrences
    // with work.step. This is the owner's real port loop, not Theta internals.
    let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
    let positions =
        std::collections::BTreeSet::from([0, quantum, trace.len() / 2, trace.len() - 1]);
    for stop in positions {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(walk(&owner, 319, &control, None),
                Err(SqlSourceJournalError::Control(actual)) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn writer_state_partial_cannot_borrow_merge_input_and_preserves_ordinary_tail_prefixes() {
    let owner = authored(&[1], &[false]);
    let (fragment, node, call) = owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| match &node.kind {
                NodeKind::TableWriter { target } => {
                    Some((fragment, node, &target.partial_aggregates[0]))
                }
                _ => None,
            })
        })
        .unwrap();
    let run = |control: &dyn PureCompileControl| -> Result<(), SqlSourceJournalError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            let entry = owner.checked_writer_aggregate_source_observed(
                fragment,
                node,
                PhysicalCallSite::WriterPartial {
                    node: node.id,
                    call: 0,
                },
                call,
                &mut work,
            )?;
            entry.state_inputs_observed(&mut work).map(|_| ())
        })();
        if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let baseline = Control::default();
    assert!(matches!(
        run(&baseline),
        Err(SqlSourceJournalError::InvalidSource(
            "Writer update has no merge state inputs"
        ))
    ));
    let trace = baseline.trace.into_inner().unwrap();
    assert!(
        trace.last().unwrap().1 > 0,
        "the completed demand refusal reaches the original footer"
    );
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(run(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
