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
use crate::{
    AggregateTopNFilter, BindingRequirement, BindingRequirements, CompileProfile, FilterNullOrder,
    FilterNullSemantics, FilterOrderKey, FilterProducerAtExpr, FilterProducerKind, FilterReduction,
    FilterSortDirection, JoinDistributionMode, JoinType, KernelAbiVersion, LocalProgramError,
    ProgramNode, StaticExprKind, StaticExprNode, StaticFilterContract, StaticFilterProducer,
    StaticLayout, StaticLiteral, StaticSinkProgram, StaticStreamBranch, StaticValues,
    StaticWriterProjection, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan,
};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::WriteTargetOrdinal;
use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    sync::Mutex,
};

#[derive(Default)]
struct Control {
    failure: Option<CompileControlError>,
    positive: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if !self.positive || work > 0 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}
fn arena(nodes: Vec<StaticExprNode>) -> Arc<ImmutableExpressions> {
    Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap())
}
fn integer_arena(value: i64) -> Arc<ImmutableExpressions> {
    arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Int64(value)),
        DataType::Int64,
        None,
    )])
}
fn boolean_arena() -> Arc<ImmutableExpressions> {
    arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Bool(true)),
        DataType::Boolean,
        None,
    )])
}
fn values(columns: usize) -> (StaticValues, StaticLayout) {
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|column| Field::new(format!("v{column}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..columns)
        .map(|column| Arc::new(Int64Array::from(vec![column as i64 + 1])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let slots = (0..columns)
        .map(|column| SlotId::new(column as u32 + 1))
        .collect::<Vec<_>>();
    let layout = StaticLayout::try_new(schema, slots.into()).unwrap();
    (
        StaticValues::try_new(batch, layout.clone()).unwrap(),
        layout,
    )
}
fn profile(layout: &StaticLayout) -> CompileProfile {
    CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    )
}
fn metadata_source_program(
    join: bool,
    wrong_definition: bool,
) -> Result<LocalProgramGraph, LocalProgramError> {
    let (source, layout) = values(1);
    let definitions = arena(vec![
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(7)),
            DataType::Int64,
            None,
        ),
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(9)),
            DataType::Int64,
            None,
        ),
    ]);
    let mut nodes = vec![ProgramNode::new(
        10,
        ProgramNodeKind::Values {
            values: source.clone(),
        },
        layout.clone(),
    )];
    let source_definition = ProgramExprId::new(usize::from(wrong_definition));
    let root = if join {
        let producer = StaticFilterProducer::try_new(
            7,
            8,
            FilterProducerKind::Membership,
            StaticFilterContract::membership(
                &novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                FilterNullSemantics::NeverMatches,
            )
            .unwrap(),
            FilterReduction::SetUnion,
        )
        .unwrap();
        nodes.push(ProgramNode::new(
            11,
            ProgramNodeKind::Values { values: source },
            layout.clone(),
        ));
        nodes.push(ProgramNode::new(
            20,
            ProgramNodeKind::Join {
                left: ProgramNodeId::new(0),
                right: ProgramNodeId::new(1),
                join_type: JoinType::Inner,
                distribution_mode: JoinDistributionMode::SharedBuild,
                left_layout: layout.clone(),
                right_layout: layout.clone(),
                join_scope_layout: layout.clone(),
                condition: crate::ProgramJoinCondition::Ordinary {
                    keys: vec![crate::ProgramJoinKey {
                        probe: ProgramExprId::new(0),
                        build: ProgramExprId::new(0),
                        null_safe: false,
                    }],
                    residual_predicate: None,
                },
                runtime_filter_consumers: vec![],
                runtime_filters: vec![FilterProducerAtExpr {
                    expr_id: source_definition,
                    key_ordinal: 0,
                    producer,
                }],
            },
            layout.clone(),
        ));
        ProgramNodeId::new(2)
    } else {
        let producer = StaticFilterProducer::try_new(
            7,
            8,
            FilterProducerKind::OrderedBound,
            StaticFilterContract::Ordered {
                keys: crate::FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                    direction: FilterSortDirection::Ascending,
                    null_order: FilterNullOrder::Last,
                }])),
                comparator_digest: [1; 32],
                contract_digest: [2; 32],
            },
            FilterReduction::TightenOrderedBound,
        )
        .unwrap();
        nodes.push(ProgramNode::new(
            20,
            ProgramNodeKind::Aggregate {
                input: ProgramNodeId::new(0),
                group_by: vec![ProgramExprId::new(0)],
                functions: vec![],
                need_finalize: true,
                input_is_intermediate: false,
                topn_filters: vec![AggregateTopNFilter {
                    group_key_expr: source_definition,
                    group_key_ordinal: 0,
                    limit: NonZeroU32::new(3).unwrap(),
                    producer,
                }]
                .into(),
                streaming_preaggregation_mode: None,
            },
            layout.clone(),
        ));
        ProgramNodeId::new(1)
    };
    LocalProgramGraph::try_new(
        nodes,
        root,
        definitions,
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::RuntimeFilter { binding_id: 7 }])
            .unwrap(),
    )
}

#[test]
fn group_and_build_filter_producers_reference_evaluated_arrays_without_extra_calls() {
    for (join, expected) in [(false, 1), (true, 2)] {
        let program = metadata_source_program(join, false).unwrap();
        let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
        assert_eq!(roots.sites().len(), expected);
        assert!(roots.sites().keys().all(|site| matches!(
            site,
            ProgramExpressionRootSite::Node {
                role: ProgramNodeExpressionRole::AggregateGroup { .. }
                    | ProgramNodeExpressionRole::JoinProbeKey { .. }
                    | ProgramNodeExpressionRole::JoinBuildKey { .. },
                ..
            }
        )));
        assert_eq!(
            metadata_source_program(join, true).unwrap_err(),
            LocalProgramError::InvalidNodeShape
        );
    }
}
fn branch(index: usize, keys: usize, columns: usize) -> StaticStreamBranch {
    StaticStreamBranch::try_new(
        index as i32 + 1,
        DataStreamPartitionType::HashPartitioned,
        vec![ProgramExprId::new(0); keys],
        (0..columns)
            .map(|column| SlotId::new(column as u32 + 1))
            .collect(),
        None,
        crate::StaticStreamContract {
            role: crate::ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            routing: crate::StaticStreamRouting::Ordinary,
        },
    )
    .unwrap()
}
fn scoped_program(shared: bool) -> LocalProgramGraph {
    let (values, layout) = values(2);
    let main = integer_arena(7);
    let writer = if shared {
        main.clone()
    } else {
        integer_arena(8)
    };
    let sink_arena = if shared {
        main.clone()
    } else {
        boolean_arena()
    };
    let nodes = vec![
        ProgramNode::new(0, ProgramNodeKind::Values { values }, layout.clone()),
        ProgramNode::new(
            1,
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![ProgramExprId::new(0); 2],
                expr_slot_ids: vec![SlotId::new(1), SlotId::new(2)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout.clone(),
        ),
        ProgramNode::new(
            2,
            ProgramNodeKind::TableWriter {
                input: ProgramNodeId::new(1),
                target: WriteTargetOrdinal::try_new(0).unwrap(),
                expected_layout: layout.clone(),
                projection: StaticWriterProjection {
                    arena: writer,
                    expressions: vec![ProgramExprId::new(0); 2],
                    layout: layout.clone(),
                },
                writer_multiplex_layout: layout.clone(),
                partial_aggregates: vec![],
                partial_preparation: None,
            },
            layout.clone(),
        ),
    ];
    let sink = if shared {
        StaticSinkProgram::try_multicast(vec![branch(0, 1, 2), branch(1, 1, 2)], sink_arena)
            .unwrap()
    } else {
        StaticSinkProgram::try_split(
            vec![branch(0, 1, 2), branch(1, 1, 2)],
            vec![ProgramExprId::new(0); 2],
            sink_arena,
            true,
        )
        .unwrap()
    };
    let requirements = BindingRequirements::try_new(vec![
        BindingRequirement::TableWriter {
            node: ProgramNodeId::new(2),
            layout: layout.clone(),
        },
        BindingRequirement::ExchangeOutput {
            role: crate::ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            branch: 0,
            layout: layout.clone(),
        },
        BindingRequirement::ExchangeOutput {
            role: crate::ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            branch: 1,
            layout: layout.clone(),
        },
    ])
    .unwrap();
    LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(2),
        main,
        profile(&layout),
        requirements,
        Some(sink),
    )
    .unwrap()
}
#[test]
fn repeated_definition_zero_keeps_every_actual_main_writer_and_sink_site_with_exact_backing() {
    let program = scoped_program(false);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.arenas().len(), 3);
    assert_eq!(roots.sites().len(), 8);
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::Main],
        program.expressions()
    ));
    let ProgramNodeKind::TableWriter { projection, .. } = program.nodes()[2].kind() else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::WriterProjection(ProgramNodeId::new(2))],
        &projection.arena
    ));
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::Sink],
        program.sink().unwrap().arena().unwrap()
    ));
    let literal = |scope| match roots.arenas()[&scope]
        .node(ProgramExprId::new(0))
        .unwrap()
        .kind()
    {
        StaticExprKind::Literal(value) => value.clone(),
        _ => panic!("fixture root is literal"),
    };
    assert!(matches!(
        literal(ProgramExpressionArena::Main),
        StaticLiteral::Int64(7)
    ));
    assert!(matches!(
        literal(ProgramExpressionArena::WriterProjection(
            ProgramNodeId::new(2)
        )),
        StaticLiteral::Int64(8)
    ));
    assert!(matches!(
        literal(ProgramExpressionArena::Sink),
        StaticLiteral::Bool(true)
    ));
    for expression in 0..2 {
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role: ProgramNodeExpressionRole::ProjectOutput { expression }
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::WriterProjection {
                node: ProgramNodeId::new(2),
                expression
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::SinkPartition {
                branch: expression,
                key: 0
            }]
                .demand,
            EvaluationDemand::Value
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::SinkSplitPredicate { branch: expression }]
                .demand,
            EvaluationDemand::TruthOnly
        );
    }
}
#[test]
fn shared_arc_backing_does_not_merge_arena_scope_or_root_occurrences() {
    let program = scoped_program(true);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.arenas().len(), 3);
    assert_eq!(roots.sites().len(), 6);
    assert!(
        roots
            .arenas()
            .values()
            .all(|arena| Arc::ptr_eq(arena, program.expressions()))
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| site.arena() == ProgramExpressionArena::Main)
            .count(),
        2
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| site.arena() == ProgramExpressionArena::Sink)
            .count(),
        2
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| matches!(site.arena(), ProgramExpressionArena::WriterProjection(_)))
            .count(),
        2
    );
}
fn finish_program(constants: Vec<Vec<UnpivotConstant>>) -> LocalProgramGraph {
    let (values, layout) = values(1);
    let expressions = integer_arena(7);
    let unpivot = WriterGroupedUnpivotPlan {
        grouping_input_slot_id: SlotId::new(1),
        grouping_output_slot_id: SlotId::new(2),
        passthrough_output_slot_id: SlotId::new(3),
        value_output_slot_id: SlotId::new(4),
        literal_output_slot_ids: vec![SlotId::new(5), SlotId::new(6), SlotId::new(7)],
        mappings: constants
            .into_iter()
            .enumerate()
            .map(|(mapping, constants)| WriterGroupedUnpivotMapping {
                grouping_key: mapping as u32,
                input_value_slot_id: SlotId::new(1),
                constants,
            })
            .collect(),
        max_output_rows: 16,
        max_output_bytes: 1024,
    };
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                1,
                ProgramNodeKind::TableFinish {
                    count_mode: novarocks_result_contract::FinishCountMode::WriterReported,
                    input_roles: novarocks_result_contract::FinishInputs {
                        writer_results: 0,
                        summaries: Default::default(),
                    },
                    summary_layouts: novarocks_result_contract::PerKind::empty(),
                    inputs: vec![ProgramNodeId::new(0)],
                    expected_targets: vec![WriteTargetOrdinal::try_new(0).unwrap()],
                    writer_multiplex_layout: layout.clone(),
                    root_result_layout: layout.clone(),
                    final_aggregates: WriterFinalAggregatePlan {
                        prepared: None,
                        calls: vec![],
                        unpivot: Some(unpivot),
                    },
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(1),
        expressions,
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::TableFinish {
            node: ProgramNodeId::new(1),
            layout,
        }])
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn finish_grouped_unpivot_mixed_constants_keep_scalar_mapping_and_constant_ordinals() {
    let scalar = || UnpivotConstant::Scalar {
        expr_id: ProgramExprId::new(0),
        nullable: false,
    };
    let list = || UnpivotConstant::Int32List(vec![2, 3]);
    let map = || UnpivotConstant::Utf8Map(vec![(Arc::from("k"), Arc::from("v"))]);
    let program = finish_program(vec![
        vec![list(), scalar(), map()],
        vec![scalar(), map(), scalar()],
    ]);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.sites().len(), 3);
    for (mapping, constant) in [(0, 1), (1, 0), (1, 2)] {
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role: ProgramNodeExpressionRole::FinishUnpivotConstant { mapping, constant }
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
    }
    assert_eq!(roots.arenas().len(), 1);
}
fn sink_budget_program(extra_partition: bool, main_root: bool) -> LocalProgramGraph {
    let (values, layout) = values(1);
    let expressions = integer_arena(7);
    let mut nodes = vec![ProgramNode::new(
        0,
        ProgramNodeKind::Values { values },
        layout.clone(),
    )];
    if main_root {
        nodes.push(ProgramNode::new(
            1,
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![ProgramExprId::new(0)],
                expr_slot_ids: vec![SlotId::new(1)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout.clone(),
        ));
    }
    let mut branches = (0..16)
        .map(|index| branch(index, 4096, 1))
        .collect::<Vec<_>>();
    if extra_partition {
        branches.push(branch(16, 1, 1));
    }
    let requirements = BindingRequirements::try_new(
        (0..branches.len())
            .map(|branch| BindingRequirement::ExchangeOutput {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                branch,
                layout: layout.clone(),
            })
            .collect(),
    )
    .unwrap();
    let sink = StaticSinkProgram::try_multicast(branches, integer_arena(8)).unwrap();
    let root = ProgramNodeId::new(nodes.len() - 1);
    LocalProgramGraph::try_new_with_sink(
        nodes,
        root,
        expressions,
        profile(&layout),
        requirements,
        Some(sink),
    )
    .unwrap()
}
#[test]
fn one_global_root_budget_accepts_65536_and_rejects_extra_sink_or_main_occurrence() {
    assert_eq!(MAX_CONTROL_USE_REFERENCES, 65536);
    let near = sink_budget_program(false, false);
    let roots = ProgramExpressionRoots::collect(&near, &Control::default()).unwrap();
    assert_eq!(roots.sites().len(), MAX_CONTROL_USE_REFERENCES);
    assert!(
        near.sink()
            .unwrap()
            .branches()
            .iter()
            .all(|branch| branch.partition_exprs().len() == 4096)
    );
    for program in [
        sink_budget_program(true, false),
        sink_budget_program(false, true),
    ] {
        assert_eq!(
            ProgramExpressionRoots::collect(&program, &Control::default()).unwrap_err(),
            ProgramExpressionRootError::TooManyRoots
        );
    }
}
#[test]
fn entry_and_positive_quantum_failures_keep_exact_typed_control_categories() {
    let program = sink_budget_program(false, false);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive in [false, true] {
            let control = Control {
                failure: Some(failure.clone()),
                positive,
                work: Mutex::default(),
            };
            assert_eq!(
                ProgramExpressionRoots::collect(&program, &control).unwrap_err(),
                ProgramExpressionRootError::Control(failure.clone())
            );
            assert_eq!(
                *control.work.lock().unwrap(),
                if positive { vec![0, 256] } else { vec![0] }
            );
        }
    }
}
#[test]
fn non_expression_constants_and_zero_expression_nodes_still_observe_work() {
    let program = finish_program(
        (0..300)
            .map(|_| {
                vec![
                    UnpivotConstant::Int32List(vec![]),
                    UnpivotConstant::Utf8Map(vec![]),
                    UnpivotConstant::Int32List(vec![]),
                ]
            })
            .collect(),
    );
    let control = Control::default();
    let roots = ProgramExpressionRoots::collect(&program, &control).unwrap();
    assert!(roots.sites().is_empty());
    assert!(control.work.lock().unwrap().contains(&256));
    let (values, layout) = values(1);
    let mut nodes = (0..300)
        .map(|index| {
            ProgramNode::new(
                index,
                ProgramNodeKind::Values {
                    values: values.clone(),
                },
                layout.clone(),
            )
        })
        .collect::<Vec<_>>();
    nodes.push(ProgramNode::new(
        300,
        ProgramNodeKind::UnionAll {
            inputs: (0..300).map(ProgramNodeId::new).collect(),
        },
        layout.clone(),
    ));
    let zero = LocalProgramGraph::try_new(
        nodes,
        ProgramNodeId::new(300),
        arena(vec![]),
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout }]).unwrap(),
    )
    .unwrap();
    let control = Control::default();
    assert!(
        ProgramExpressionRoots::collect(&zero, &control)
            .unwrap()
            .sites()
            .is_empty()
    );
    assert!(control.work.lock().unwrap().contains(&256));
    for program in [&program, &zero] {
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure.clone()),
                positive: true,
                work: Mutex::default(),
            };
            assert_eq!(
                ProgramExpressionRoots::collect(program, &control).unwrap_err(),
                ProgramExpressionRootError::Control(failure)
            );
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
    }
}

// The supplied owner is lifetime-only. Its observer records actual public
// requests and locked opaque operation bounds, without manufacturing a grant.
struct CreationHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CreationHold {}
impl Drop for CreationHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationEvent {
    Layout(usize, usize),
    Opaque(usize),
}
#[derive(Debug, Eq, PartialEq)]
struct CallerRefusal {
    operation: usize,
    original: u64,
}
fn creation_storage(drops: &Arc<std::sync::atomic::AtomicUsize>) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(CreationHold(drops.clone())))
}

#[test]
fn root_creation_keeps_actual_tables_to_last_independent_map_reference() {
    let program = metadata_source_program(false, false).unwrap();
    let expected = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let layouts = events.clone();
    let mut request = move |layout: std::alloc::Layout| -> Result<(), CallerRefusal> {
        layouts
            .lock()
            .unwrap()
            .push(CreationEvent::Layout(layout.size(), layout.align()));
        Ok(())
    };
    let opaque = events.clone();
    let mut quote = move |bytes| -> Result<(), CallerRefusal> {
        opaque.lock().unwrap().push(CreationEvent::Opaque(bytes));
        Ok(())
    };
    let mut creation = ValueSchemaCreation::new(creation_storage(&drops), &mut request, &mut quote);
    let roots = ProgramExpressionRoots::collect_with_creator_in(
        &program,
        &mut creation,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(roots.sites(), expected.sites());
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::Main],
        program.expressions()
    ));
    let late_map = roots.sites.clone();
    drop(roots);
    drop(program);
    drop(creation);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!late_map.is_empty());
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, CreationEvent::Layout(..)))
            .count(),
        2
    );
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, CreationEvent::Opaque(bytes) if *bytes > 0))
    );
    drop(late_map);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[test]
fn each_root_before_construction_refusal_returns_original_caller_without_tail() {
    let program = metadata_source_program(false, false).unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let run = |fail_at: Option<usize>| {
        observed.lock().unwrap().clear();
        let layout_events = observed.clone();
        let mut request = move |layout: std::alloc::Layout| {
            let mut events = layout_events.lock().unwrap();
            let ordinal = events.len();
            events.push(CreationEvent::Layout(layout.size(), layout.align()));
            if fail_at == Some(ordinal) {
                Err(CallerRefusal {
                    operation: ordinal,
                    original: 0xabc,
                })
            } else {
                Ok(())
            }
        };
        let opaque_events = observed.clone();
        let mut opaque = move |bytes| {
            let mut events = opaque_events.lock().unwrap();
            let ordinal = events.len();
            events.push(CreationEvent::Opaque(bytes));
            if fail_at == Some(ordinal) {
                Err(CallerRefusal {
                    operation: ordinal,
                    original: 0xabc,
                })
            } else {
                Ok(())
            }
        };
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut creation =
            ValueSchemaCreation::new(creation_storage(&drops), &mut request, &mut opaque);
        ProgramExpressionRoots::collect_with_creator_in(
            &program,
            &mut creation,
            &Control::default(),
        )
    };
    let complete = run(None).unwrap();
    drop(complete);
    let original = observed.lock().unwrap().clone();
    for operation in 0..original.len() {
        assert!(
            matches!(run(Some(operation)), Err(ProgramCreationFailure::Creator(CallerRefusal {operation: at, original: 0xabc})) if at == operation)
        );
        assert_eq!(*observed.lock().unwrap(), original[..=operation]);
    }
    let control = Control {
        failure: Some(CompileControlError::Cancelled),
        ..Control::default()
    };
    let mut request =
        |_| -> Result<(), CallerRefusal> { panic!("request after entry cancellation") };
    let mut opaque =
        |_| -> Result<(), CallerRefusal> { panic!("opaque request after entry cancellation") };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    assert!(matches!(
        ProgramExpressionRoots::collect_with_creator_in(&program, &mut creation, &control),
        Err(ProgramCreationFailure::Control(
            CompileControlError::Cancelled
        ))
    ));
}

fn role_join_key(probe: usize, build: usize, null_safe: bool) -> crate::ProgramJoinKey {
    crate::ProgramJoinKey {
        probe: ProgramExprId::new(probe),
        build: ProgramExprId::new(build),
        null_safe,
    }
}
fn role_naa_condition() -> crate::ProgramJoinCondition {
    crate::ProgramJoinCondition::NullAwareEqual {
        value_equal_keys: vec![role_join_key(0, 0, true)],
        scope_keys: vec![role_join_key(1, 1, false), role_join_key(0, 1, true)],
        eligibility: Some(ProgramExprId::new(2)),
    }
}
fn role_join_program(
    join_type: JoinType,
    condition: crate::ProgramJoinCondition,
    runtime_filters: Vec<FilterProducerAtExpr>,
) -> Result<LocalProgramGraph, LocalProgramError> {
    let (source, layout) = values(1);
    let definitions = arena(vec![
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(7)),
            DataType::Int64,
            None,
        ),
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(9)),
            DataType::Int64,
            None,
        ),
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Bool(true)),
            DataType::Boolean,
            None,
        ),
    ]);
    let requirements = if runtime_filters.is_empty() {
        vec![]
    } else {
        vec![BindingRequirement::RuntimeFilter { binding_id: 7 }]
    };
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(
                10,
                ProgramNodeKind::Values {
                    values: source.clone(),
                },
                layout.clone(),
            ),
            ProgramNode::new(
                11,
                ProgramNodeKind::Values { values: source },
                layout.clone(),
            ),
            ProgramNode::new(
                20,
                ProgramNodeKind::Join {
                    left: ProgramNodeId::new(0),
                    right: ProgramNodeId::new(1),
                    join_type,
                    distribution_mode: JoinDistributionMode::SharedBuild,
                    left_layout: layout.clone(),
                    right_layout: layout.clone(),
                    join_scope_layout: layout.clone(),
                    condition,
                    runtime_filters,
                    runtime_filter_consumers: vec![],
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        definitions,
        profile(&layout),
        BindingRequirements::try_new(requirements).unwrap(),
    )
}

#[test]
fn naa_value_scope_and_eligibility_keep_separate_value_roots_and_input_ports() {
    use crate::{ProgramChannelLayoutRole as Layout, ProgramRootInput};
    use ProgramNodeExpressionRole as Role;
    let program =
        role_join_program(JoinType::NullAwareLeftAnti, role_naa_condition(), vec![]).unwrap();
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    let node = ProgramNodeId::new(2);
    let expected = [
        (Role::NaaValueProbeKey { key: 0 }, 0, Layout::JoinLeft),
        (Role::NaaValueBuildKey { key: 0 }, 0, Layout::JoinRight),
        (Role::NaaScopeProbeKey { key: 0 }, 1, Layout::JoinLeft),
        (Role::NaaScopeBuildKey { key: 0 }, 1, Layout::JoinRight),
        (Role::NaaScopeProbeKey { key: 1 }, 0, Layout::JoinLeft),
        (Role::NaaScopeBuildKey { key: 1 }, 1, Layout::JoinRight),
        (Role::NaaEligibility, 2, Layout::JoinScope),
    ];
    assert_eq!(roots.sites().len(), expected.len());
    for (role, definition, layout_role) in expected {
        let site = ProgramExpressionRootSite::Node { node, role };
        assert_eq!(
            roots.sites()[&site],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(definition),
                demand: EvaluationDemand::Value,
            }
        );
        assert_eq!(
            crate::root_input_layout(&program, site).unwrap(),
            ProgramRootInput::Layout {
                node,
                role: layout_role
            }
        );
    }
}

#[test]
fn ordinary_join_residual_retains_truth_demand_and_original_roles() {
    use ProgramNodeExpressionRole as Role;
    let program = role_join_program(
        JoinType::Inner,
        crate::ProgramJoinCondition::Ordinary {
            keys: vec![role_join_key(0, 1, false)],
            residual_predicate: Some(ProgramExprId::new(2)),
        },
        vec![],
    )
    .unwrap();
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.sites().len(), 3);
    for (role, definition, demand) in [
        (Role::JoinProbeKey { key: 0 }, 0, EvaluationDemand::Value),
        (Role::JoinBuildKey { key: 0 }, 1, EvaluationDemand::Value),
        (Role::JoinResidual, 2, EvaluationDemand::TruthOnly),
    ] {
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(2),
                role
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(definition),
                demand
            }
        );
    }
}

#[test]
fn naa_kind_and_closed_condition_must_agree_and_values_cannot_be_empty() {
    let ordinary = crate::ProgramJoinCondition::Ordinary {
        keys: vec![role_join_key(0, 0, false)],
        residual_predicate: None,
    };
    let empty_values = crate::ProgramJoinCondition::NullAwareEqual {
        value_equal_keys: vec![],
        scope_keys: vec![role_join_key(0, 0, false)],
        eligibility: None,
    };
    for (join_type, condition) in [
        (JoinType::NullAwareLeftAnti, ordinary),
        (JoinType::Inner, role_naa_condition()),
        (JoinType::NullAwareLeftAnti, empty_values),
    ] {
        assert_eq!(
            role_join_program(join_type, condition, vec![]).unwrap_err(),
            LocalProgramError::InvalidNodeShape
        );
    }
    let uncorrelated = crate::ProgramJoinCondition::NullAwareEqual {
        value_equal_keys: vec![role_join_key(0, 0, false)],
        scope_keys: vec![],
        eligibility: None,
    };
    assert!(role_join_program(JoinType::NullAwareLeftAnti, uncorrelated, vec![]).is_ok());
}

#[test]
fn every_naa_role_participates_in_expression_reference_validation() {
    for role in 0..5 {
        let mut condition = role_naa_condition();
        let crate::ProgramJoinCondition::NullAwareEqual {
            value_equal_keys,
            scope_keys,
            eligibility,
        } = &mut condition
        else {
            unreachable!()
        };
        let invalid = ProgramExprId::new(999);
        match role {
            0 => value_equal_keys[0].probe = invalid,
            1 => value_equal_keys[0].build = invalid,
            2 => scope_keys[0].probe = invalid,
            3 => scope_keys[0].build = invalid,
            4 => *eligibility = Some(invalid),
            _ => unreachable!(),
        }
        assert_eq!(
            role_join_program(JoinType::NullAwareLeftAnti, condition, vec![]).unwrap_err(),
            LocalProgramError::InvalidExpression
        );
    }
}

#[test]
fn naa_runtime_filter_ordinals_name_value_keys_and_never_scope_keys() {
    let original = metadata_source_program(true, false).unwrap();
    let ProgramNodeKind::Join {
        runtime_filters, ..
    } = original.nodes()[2].kind()
    else {
        unreachable!()
    };
    let producer = runtime_filters[0].clone();
    assert!(
        role_join_program(
            JoinType::NullAwareLeftAnti,
            role_naa_condition(),
            vec![producer.clone()]
        )
        .is_ok()
    );
    let mut out_of_value_domain = producer.clone();
    out_of_value_domain.key_ordinal = 1;
    assert_eq!(
        role_join_program(
            JoinType::NullAwareLeftAnti,
            role_naa_condition(),
            vec![out_of_value_domain]
        )
        .unwrap_err(),
        LocalProgramError::InvalidNodeShape
    );
    let mut scope_definition = producer;
    scope_definition.expr_id = ProgramExprId::new(1);
    assert_eq!(
        role_join_program(
            JoinType::NullAwareLeftAnti,
            role_naa_condition(),
            vec![scope_definition]
        )
        .unwrap_err(),
        LocalProgramError::InvalidNodeShape
    );
}
