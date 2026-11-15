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

//! Original Writer source graph producer, feature=test-support only.
use super::contract_lowering::*;
use crate::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
use crate::column_id::ColumnId;
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::distributed::write::ConnectorWriteInputBinding;
use crate::planner::distributed::write::auxiliary::{
    WriterStatisticsTargetInput, plan_writer_statistics,
};
use crate::planner::distributed::write::change_stream::{
    ChangeStreamWriteDagSpec, ChangeStreamWriteRouteSpec,
};
use crate::planner::distributed::write::contract::{
    FinalizedWriteTargetSet, test_support::simple_sql_write_plan_input,
};
use crate::planner::payload::PlanValuesNode;
use crate::planner::physical::{
    DistributedChangeEventExpandNode, DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
    PhysicalPlanKind, PhysicalPlanNode, PhysicalPlanStats, PlannerConfidence,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_physical_plan::{NodeKind, PhysicalCallSite, PipelineDopDomain, PlanVersionId};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{collections::HashMap, sync::Arc};
fn version() -> PlanVersionId {
    PlanVersionId::try_new([41; 16]).unwrap()
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: true,
    }
}
fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

        is_internal: false,
    }
}

fn literal_int(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 1.0,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn values(columns: Vec<OutputColumn>, rows: Vec<Vec<TypedExpr>>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows,
            columns: columns.clone(),
        }),
        children: Vec::new(),
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn write_handle() -> ConnectorEncodedPayload {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let instance = ConnectorInstanceId::parse("warehouse").unwrap();
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            CatalogHandle::new(instance, CatalogVersion::from_bytes([9; 32])),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    )
}

pub fn writer_state_source_for_test(
    aggregate_name: &str,
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
                        aggregate_name,
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
                        write_input: None,
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
                dag: ChangeStreamWriteDagSpec {
                    effect_output_ordinal: 1,
                    routes,
                },
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

#[derive(Debug, Eq, PartialEq)]
pub struct WriterStateSourceObservation {
    pub partials: usize,
    pub no_contributions: usize,
    pub independent_lineages: usize,
    pub request_errors: Vec<String>,
}
pub fn writer_state_observe_for_test(
    owner: &SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> Result<WriterStateSourceObservation, String> {
    let mut observed = WriterStateSourceObservation {
        partials: 0,
        no_contributions: 0,
        independent_lineages: 0,
        request_errors: Vec::new(),
    };
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(|e| format!("{e:?}"))?;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::TableFinish(finish) = &node.kind {
                for (ordinal, call) in finish.final_aggregates.iter().enumerate() {
                    let site = PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let entry = owner
                        .checked_writer_aggregate_source_observed(
                            fragment, node, site, call, &mut work,
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    // Original mode can retain a historical internal request
                    // mismatch. Observe this proof independently; no failed
                    // request check certifies the positive state-route facts.
                    if let Err(error) =
                        super::physical_writer_requests::author_physical_writer_request_observed(
                            &entry, &mut work,
                        )
                    {
                        if let super::physical_writer_requests::PhysicalWriterRequestError::Control(cause) = &error {
                            return Err(cause.to_string());
                        }
                        observed.request_errors.push(format!("{error:?}"));
                    }
                    entry
                        .state_inputs_observed(&mut work)
                        .map_err(|e| format!("{e:?}"))?
                        .visit_observed(
                            &mut work,
                            |producer, _, work| {
                                observed.partials += 1;
                                if !producer
                                    .captured()
                                    .logical_identity()
                                    .same_lineage(entry.captured().logical_identity())
                                {
                                    observed.independent_lineages += 1;
                                }
                                work.step()?;
                                Ok(())
                            },
                            |_, _, _, _| Ok(()),
                            |_, work| {
                                observed.no_contributions += 1;
                                work.step()?;
                                Ok(())
                            },
                        )
                        .map_err(|e| format!("{e:?}"))?;
                }
            }
        }
    }
    work.finish().map_err(|e| format!("{e:?}"))?;
    Ok(observed)
}
