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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::expr::{encode_expr, encode_sort_items, encode_window_frame};
use super::types::encode_type;
use crate::proto::{common, plan};
use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn, TypedExpr};
use crate::sql::catalog;
use crate::sql::codegen::expr_compiler::infer_agg_function_types;
use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind, FragmentStreamKind};
use crate::sql::common::{ChangeStreamBranchKind, JoinKind};
use crate::sql::parser::ast::SqlType;
use crate::sql::planner::plan::{
    AggregateCall, ExchangeFlavor, PhysicalHashAggregateNode, PhysicalPlanKind,
    PlanRowCountAssertion, PlanSetOpKind, RedistributeMode,
};
use crate::sql::planner::runtime_filter::{
    JoinExecutionMode, WiredRuntimeFilterBuild, WiredRuntimeFilterProbe,
};
use crate::sql::planner::write_sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
};
use crate::sql::planner::{
    AggMode, DataPartition, DataSink, DistributedNode, DistributedPayload, DistributedPlan,
    ExchangeReceiver, HashSource, IcebergWriteInputBinding, JoinDistribution, PartitionKind,
    PlanFragment, TopNPhase,
};

pub(crate) struct NativePlanEncodeContext<'a> {
    pub(crate) mv_refresh_ctx:
        Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
}

pub(crate) fn encode_distributed_plan(
    src: &DistributedPlan,
) -> Result<plan::DistributedPlan, String> {
    encode_distributed_plan_with_context(
        src,
        NativePlanEncodeContext {
            mv_refresh_ctx: None,
        },
    )
}

pub(crate) fn encode_distributed_plan_with_context(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    let mut fragments = src
        .fragments
        .iter()
        .map(|fragment| encode_plan_fragment_with_context(fragment, &ctx))
        .collect::<Result<Vec<_>, _>>()?;
    attach_stream_sinks(src, &mut fragments)?;
    Ok(plan::DistributedPlan {
        fragments,
        root_fragment_id: src.root_fragment_id,
        edges: src
            .edges
            .iter()
            .map(encode_fragment_edge)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn attach_stream_sinks(
    src: &DistributedPlan,
    fragments: &mut [plan::PlanFragment],
) -> Result<(), String> {
    let fragment_index_by_id = fragments
        .iter()
        .enumerate()
        .map(|(idx, fragment)| (fragment.fragment_id, idx))
        .collect::<HashMap<_, _>>();
    let source_fragment_by_id = src
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect::<HashMap<_, _>>();

    for edge in &src.edges {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        let idx = *fragment_index_by_id
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                format!(
                    "native stream edge source fragment {} missing encoded fragment",
                    edge.source_fragment_id
                )
            })?;
        let target_idx = *fragment_index_by_id
            .get(&edge.target_fragment_id)
            .ok_or_else(|| {
                format!(
                    "native stream edge target fragment {} missing encoded fragment",
                    edge.target_fragment_id
                )
            })?;
        let source = source_fragment_by_id
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                format!(
                    "native stream edge source fragment {} missing source fragment",
                    edge.source_fragment_id
                )
            })?;
        let target_exchange_columns = encoded_exchange_receiver_output_columns(
            &fragments[target_idx],
            edge.target_exchange_node_id,
        )?;
        let stream_output_columns =
            stream_edge_output_columns(source, &fragments[idx], edge, &target_exchange_columns)?;
        let stream_output_slot_ids = stream_output_slot_ids(&stream_output_columns)?;
        let fragment = &mut fragments[idx];
        if !matches!(
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref()),
            Some(plan::data_sink::Kind::Noop(true))
        ) {
            return Err(format!(
                "native stream edge source fragment {} must have a NOOP sink before stream attachment",
                edge.source_fragment_id
            ));
        }
        fragment.sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                dest_node_id: edge.target_exchange_node_id,
                output_partition: Some(encode_data_partition(&source.output_partition)?),
                output_columns: stream_output_slot_ids,
                limit: None,
            })),
        });
        patch_exchange_receiver_output_columns(
            &mut fragments[target_idx],
            edge.target_exchange_node_id,
            stream_output_columns,
        )?;
    }
    Ok(())
}

fn stream_edge_output_columns(
    source: &PlanFragment,
    encoded_source: &plan::PlanFragment,
    edge: &FragmentEdge,
    target_exchange_columns: &[common::OutputColumn],
) -> Result<Vec<common::OutputColumn>, String> {
    if source.output_columns.is_empty()
        && edge.output_slot_ids.is_empty()
        && target_exchange_columns.is_empty()
    {
        return Ok(Vec::new());
    }
    let columns = match encoded_fragment_root_output_columns(encoded_source) {
        Ok(columns) if !columns.is_empty() => columns,
        Ok(_) if !source.output_columns.is_empty() => encode_output_columns(&source.output_columns)?,
        Ok(_) => Vec::new(),
        Err(root_err) if !source.output_columns.is_empty() => {
            encode_output_columns(&source.output_columns).map_err(|source_err| {
                format!(
                    "native stream source root output unavailable ({root_err}); fragment output encoding failed: {source_err}"
                )
            })?
        }
        Err(root_err) => return Err(root_err),
    };
    if !target_exchange_columns.is_empty()
        && let Some(projected) = project_output_columns_for_requested_exchange(
            columns.clone(),
            &source.output_columns,
            target_exchange_columns,
        )?
    {
        return Ok(projected);
    }
    project_output_columns_for_edge(columns, &source.output_columns, &edge.output_slot_ids)
}

fn stream_output_slot_ids(
    stream_output_columns: &[common::OutputColumn],
) -> Result<Vec<i32>, String> {
    stream_output_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id).map_err(|_| {
                format!(
                    "native stream edge output column {} cannot convert to slot id",
                    column.column_id
                )
            })
        })
        .collect()
}

pub(crate) fn encoded_fragment_root_output_columns(
    fragment: &plan::PlanFragment,
) -> Result<Vec<common::OutputColumn>, String> {
    let root = fragment
        .root
        .as_ref()
        .ok_or_else(|| format!("native fragment {} missing root", fragment.fragment_id))?;
    encoded_node_output_columns(root)
}

fn encoded_node_output_columns(
    node: &plan::DistributedNode,
) -> Result<Vec<common::OutputColumn>, String> {
    match node.payload.as_ref() {
        Some(plan::distributed_node::Payload::Physical(physical)) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Project(project)) => {
                encoded_project_output_columns(node, project)
            }
            Some(plan::plan_node::Kind::Filter(_))
            | Some(plan::plan_node::Kind::Limit(_))
            | Some(plan::plan_node::Kind::AssertOneRow(_))
            | Some(plan::plan_node::Kind::Sort(_))
            | Some(plan::plan_node::Kind::Topn(_)) => {
                encoded_unary_passthrough_output_columns(node, "native stream source")
            }
            Some(plan::plan_node::Kind::HashAggregate(aggregate)) => {
                if !aggregate.output_columns.is_empty() {
                    return Ok(aggregate.output_columns.clone());
                }
                let output_layout = aggregate.output_layout.as_ref().ok_or_else(|| {
                    format!(
                        "native stream source aggregate node {} missing output_layout",
                        node.node_id
                    )
                })?;
                let mut columns = Vec::with_capacity(
                    output_layout.group_key_columns.len() + output_layout.aggregate_columns.len(),
                );
                columns.extend(output_layout.group_key_columns.clone());
                columns.extend(output_layout.aggregate_columns.clone());
                Ok(columns)
            }
            Some(plan::plan_node::Kind::GenerateSeries(generate_series)) => {
                Ok(vec![common::OutputColumn {
                    column_id: generate_series.output_column_id,
                    name: if generate_series.column_name.is_empty() {
                        "generate_series".to_string()
                    } else {
                        generate_series.column_name.clone()
                    },
                    r#type: Some(encode_type(&DataType::Int64)?),
                    nullable: false,
                    is_internal: false,
                }])
            }
            Some(plan::plan_node::Kind::Scan(scan)) => encoded_scan_output_columns(scan),
            Some(plan::plan_node::Kind::Values(values)) if values.columns.is_empty() => {
                Ok(Vec::new())
            }
            Some(_) if !physical.output_columns.is_empty() => Ok(physical.output_columns.clone()),
            Some(kind) => Err(format!(
                "native stream source node {} has no output columns for {:?}",
                node.node_id, kind
            )),
            None => Err(format!(
                "native stream source physical node {} missing kind",
                node.node_id
            )),
        },
        Some(plan::distributed_node::Payload::Exchange(exchange)) => {
            Ok(exchange.output_columns.clone())
        }
        None => Err(format!(
            "native stream source node {} missing payload",
            node.node_id
        )),
    }
}

fn encoded_unary_passthrough_output_columns(
    node: &plan::DistributedNode,
    context: &str,
) -> Result<Vec<common::OutputColumn>, String> {
    let [child] = node.children.as_slice() else {
        return Err(format!(
            "{context} node {} expected one child for output columns, got {}",
            node.node_id,
            node.children.len()
        ));
    };
    encoded_node_output_columns(child)
}

struct EncodedProjectItemOutput {
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    output_name: String,
    r#type: common::TypeDesc,
    nullable: bool,
}

fn encoded_project_output_columns(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
) -> Result<Vec<common::OutputColumn>, String> {
    let item_outputs = project
        .items
        .iter()
        .enumerate()
        .map(encoded_project_item_output)
        .collect::<Result<Vec<_>, _>>()?;
    let input_column_ids = match node.children.as_slice() {
        [] => HashSet::new(),
        [child] => encoded_node_output_columns(child)?
            .into_iter()
            .map(|column| column.column_id)
            .collect::<HashSet<_>>(),
        _ => {
            return Err(format!(
                "native stream source project node {} expected at most one child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
    };
    let output_column_id_candidates = item_outputs
        .iter()
        .map(|item| item.output_column_id)
        .collect::<HashSet<_>>();
    let mut used_output_column_ids = HashSet::new();
    let mut used_compute_column_ids = input_column_ids.clone();
    let mut next_synthetic_column_id = output_column_id_candidates
        .iter()
        .chain(used_compute_column_ids.iter())
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let mut first_expr_index_by_column_id = HashMap::new();
    let mut computed_columns = Vec::new();
    let mut output_columns = Vec::with_capacity(project.items.len());

    for item in item_outputs {
        let preferred_compute_column_id = item.preferred_compute_column_id;
        let mut compute_column_id = if item.can_reuse_input_slot
            || !input_column_ids.contains(&preferred_compute_column_id)
        {
            preferred_compute_column_id
        } else {
            allocate_project_boundary_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
            compute_column_id = allocate_project_boundary_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?;
        }

        if item.can_reuse_input_slot
            && first_expr_index_by_column_id.contains_key(&compute_column_id)
        {
            // Repeated slot-ref projections share the same computed value but
            // still need distinct visible output slots below.
        } else {
            let computed_idx = computed_columns.len();
            first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
            used_compute_column_ids.insert(compute_column_id);
            computed_columns.push(compute_column_id);
        }

        let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
            item.output_column_id
        } else {
            allocate_project_boundary_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        output_columns.push(common::OutputColumn {
            column_id: output_column_id,
            name: item.output_name,
            r#type: Some(item.r#type),
            nullable: item.nullable,
            is_internal: false,
        });
    }

    Ok(output_columns)
}

fn encoded_project_item_output(
    (idx, item): (usize, &plan::ProjectItem),
) -> Result<EncodedProjectItemOutput, String> {
    let expr = item
        .expr
        .as_ref()
        .ok_or_else(|| format!("native stream source project item {} missing expr", idx))?;
    let r#type = expr.r#type.clone().ok_or_else(|| {
        format!(
            "native stream source project item {} missing expr type",
            idx
        )
    })?;
    let (preferred_compute_column_id, can_reuse_input_slot) = match expr.kind.as_ref() {
        Some(crate::proto::expr::expr::Kind::ColumnRef(column)) => (column.column_id, true),
        _ => (item.output_column_id, false),
    };
    Ok(EncodedProjectItemOutput {
        preferred_compute_column_id,
        output_column_id: item.output_column_id,
        can_reuse_input_slot,
        output_name: item.output_name.clone(),
        r#type,
        nullable: expr.nullable,
    })
}

fn allocate_project_boundary_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> Result<u32, String> {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id
            .checked_add(1)
            .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id
        .checked_add(1)
        .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    Ok(synthetic)
}

fn project_output_columns_for_edge(
    columns: Vec<common::OutputColumn>,
    source_output_columns: &[AnalysisOutputColumn],
    output_slot_ids: &[i32],
) -> Result<Vec<common::OutputColumn>, String> {
    if output_slot_ids.is_empty() {
        return Ok(columns);
    }
    if !source_output_columns.is_empty() {
        let source_encoded_columns = encode_output_columns(source_output_columns)?;
        let mut ordinals_by_column_id: HashMap<u32, Vec<usize>> = HashMap::new();
        for (idx, column) in source_output_columns.iter().enumerate() {
            ordinals_by_column_id
                .entry(column.column_id.0)
                .or_default()
                .push(idx);
        }
        let columns_by_id = columns
            .iter()
            .cloned()
            .map(|column| (column.column_id, column))
            .collect::<HashMap<_, _>>();
        let mut next_ordinal_by_column_id = HashMap::new();
        let mut resolved = Vec::with_capacity(output_slot_ids.len());
        let mut resolved_all_by_ordinal = true;
        for slot_id in output_slot_ids {
            let column_id = u32::try_from(*slot_id).map_err(|_| {
                format!("native stream edge output slot id {slot_id} cannot convert to u32")
            })?;
            let next = next_ordinal_by_column_id.entry(column_id).or_insert(0);
            let Some(ordinals) = ordinals_by_column_id.get(&column_id) else {
                resolved_all_by_ordinal = false;
                break;
            };
            let Some(ordinal) = ordinals.get(*next).copied() else {
                resolved_all_by_ordinal = false;
                break;
            };
            if ordinal >= source_encoded_columns.len() {
                resolved_all_by_ordinal = false;
                break;
            }
            *next += 1;
            let source_column = source_encoded_columns.get(ordinal).ok_or_else(|| {
                format!("native stream edge source output ordinal {ordinal} is missing")
            })?;
            let encoded = if ordinals.len() > 1 {
                columns
                    .get(ordinal)
                    .cloned()
                    .or_else(|| columns_by_id.get(&source_column.column_id).cloned())
            } else {
                columns_by_id
                    .get(&source_column.column_id)
                    .cloned()
                    .or_else(|| {
                        if source_encoded_columns.len() == columns.len() {
                            columns.get(ordinal).cloned()
                        } else {
                            None
                        }
                    })
            };
            let Some(encoded) = encoded else {
                resolved_all_by_ordinal = false;
                break;
            };
            resolved.push(encoded);
        }
        if resolved_all_by_ordinal {
            return Ok(resolved);
        }
    }
    let columns_by_id = columns
        .iter()
        .cloned()
        .map(|column| (column.column_id, column))
        .collect::<HashMap<_, _>>();
    let mut resolved = Vec::with_capacity(output_slot_ids.len());
    let mut missing_slot_id = None;
    for slot_id in output_slot_ids.iter().copied() {
        let column_id = u32::try_from(slot_id).map_err(|_| {
            format!("native stream edge output slot id {slot_id} cannot convert to u32")
        })?;
        if let Some(column) = columns_by_id.get(&column_id) {
            resolved.push(column.clone());
        } else {
            missing_slot_id = Some(slot_id);
            break;
        }
    }
    if missing_slot_id.is_none() {
        return Ok(resolved);
    }
    if output_slot_ids.len() >= columns.len() {
        return Ok(columns);
    }
    let missing_slot_id = missing_slot_id.expect("checked above");
    Err(format!(
        "native stream edge output slot id {missing_slot_id} missing from source output columns"
    ))
}

fn project_output_columns_for_requested_exchange(
    columns: Vec<common::OutputColumn>,
    source_output_columns: &[AnalysisOutputColumn],
    requested_columns: &[common::OutputColumn],
) -> Result<Option<Vec<common::OutputColumn>>, String> {
    if requested_columns.is_empty() {
        return Ok(None);
    }
    let requested_slot_ids = requested_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id).map_err(|_| {
                format!(
                    "native stream target exchange output column {} cannot convert to slot id",
                    column.column_id
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    match project_output_columns_for_edge(columns, source_output_columns, &requested_slot_ids) {
        Ok(projected) if projected.len() == requested_columns.len() => Ok(Some(projected)),
        Ok(_) => Ok(None),
        Err(err) if err.contains("missing from source output columns") => Ok(None),
        Err(err) => Err(err),
    }
}

fn encoded_exchange_receiver_output_columns(
    fragment: &plan::PlanFragment,
    target_exchange_node_id: i32,
) -> Result<Vec<common::OutputColumn>, String> {
    let root = fragment
        .root
        .as_ref()
        .ok_or_else(|| format!("native fragment {} missing root", fragment.fragment_id))?;
    find_exchange_receiver_output_columns(root, target_exchange_node_id).ok_or_else(|| {
        format!(
            "native stream edge target fragment {} missing exchange node {}",
            fragment.fragment_id, target_exchange_node_id
        )
    })
}

fn find_exchange_receiver_output_columns(
    node: &plan::DistributedNode,
    target_exchange_node_id: i32,
) -> Option<Vec<common::OutputColumn>> {
    if node.node_id == target_exchange_node_id
        && let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref()
    {
        return Some(exchange.output_columns.clone());
    }
    node.children
        .iter()
        .find_map(|child| find_exchange_receiver_output_columns(child, target_exchange_node_id))
}

fn patch_exchange_receiver_output_columns(
    fragment: &mut plan::PlanFragment,
    target_exchange_node_id: i32,
    output_columns: Vec<common::OutputColumn>,
) -> Result<(), String> {
    let root = fragment
        .root
        .as_mut()
        .ok_or_else(|| format!("native fragment {} missing root", fragment.fragment_id))?;
    if patch_exchange_receiver_output_columns_in_node(
        root,
        target_exchange_node_id,
        &output_columns,
    ) {
        normalize_encoded_subtree_output_columns(root)?;
        return Ok(());
    }
    Err(format!(
        "native stream edge target fragment {} missing exchange node {}",
        fragment.fragment_id, target_exchange_node_id
    ))
}

fn patch_exchange_receiver_output_columns_in_node(
    node: &mut plan::DistributedNode,
    target_exchange_node_id: i32,
    output_columns: &[common::OutputColumn],
) -> bool {
    if node.node_id == target_exchange_node_id
        && let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_mut()
    {
        exchange.output_columns = output_columns.to_vec();
        return true;
    }
    node.children.iter_mut().any(|child| {
        patch_exchange_receiver_output_columns_in_node(
            child,
            target_exchange_node_id,
            output_columns,
        )
    })
}

fn normalize_encoded_subtree_output_columns(
    node: &mut plan::DistributedNode,
) -> Result<(), String> {
    for child in &mut node.children {
        normalize_encoded_subtree_output_columns(child)?;
    }
    normalize_encoded_node_output_columns(node)
}

pub(crate) fn encode_plan_fragment(src: &PlanFragment) -> Result<plan::PlanFragment, String> {
    encode_plan_fragment_with_context(
        src,
        &NativePlanEncodeContext {
            mv_refresh_ctx: None,
        },
    )
}

fn encode_plan_fragment_with_context(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanFragment, String> {
    let root = encode_node_with_context(&src.root, ctx)?;
    let (output_exprs, output_columns) = encode_fragment_output_contract(src, &root)?;
    Ok(plan::PlanFragment {
        fragment_id: src.fragment_id,
        root: Some(root),
        data_partition: Some(encode_data_partition(&src.data_partition)?),
        output_partition: Some(encode_data_partition(&src.output_partition)?),
        sink: Some(encode_data_sink(&src.sink, &src.output_columns)?),
        output_exprs,
        output_columns,
        cte_id: src.cte_id,
        cte_exchange_nodes: src
            .cte_exchange_nodes
            .iter()
            .map(|(cte_id, node_id, column_ids)| plan::CteExchangeBinding {
                cte_id: *cte_id,
                node_id: *node_id,
                column_ids: column_ids.iter().map(|id| id.0).collect(),
            })
            .collect(),
    })
}

fn encode_fragment_output_contract(
    src: &PlanFragment,
    encoded_root: &plan::DistributedNode,
) -> Result<(Vec<crate::proto::expr::Expr>, Vec<common::OutputColumn>), String> {
    if let DataSink::IcebergWrite(sink) = &src.sink {
        let output_exprs = if let Some(exprs) = src.output_exprs.as_ref() {
            if exprs.len() != sink.spec.target_columns.len() {
                return Err(format!(
                    "native Iceberg write fragment output_exprs count {} does not match target column count {}",
                    exprs.len(),
                    sink.spec.target_columns.len()
                ));
            }
            encode_exprs(exprs)?
        } else {
            let sink_columns =
                iceberg_write_sink_columns_for_input(&src.output_columns, &sink.input)?;
            if sink_columns.len() != sink.spec.target_columns.len() {
                return Err(format!(
                    "native Iceberg write sink input column count {} does not match target column count {}",
                    sink_columns.len(),
                    sink.spec.target_columns.len()
                ));
            }
            let exprs = sink_columns
                .iter()
                .map(column_ref_expr_for_output_column)
                .collect::<Vec<_>>();
            encode_exprs(&exprs)?
        };
        let output_columns = encode_iceberg_write_sink_output_columns(&sink.spec.target_columns)?;
        return Ok((output_exprs, output_columns));
    }

    let output_exprs = src
        .output_exprs
        .as_ref()
        .map(|exprs| encode_exprs(exprs))
        .transpose()?
        .unwrap_or_default();
    let output_columns = encode_fragment_execution_output_columns(src, encoded_root)?;
    Ok((output_exprs, output_columns))
}

fn encode_fragment_execution_output_columns(
    src: &PlanFragment,
    encoded_root: &plan::DistributedNode,
) -> Result<Vec<common::OutputColumn>, String> {
    match encoded_node_output_columns(encoded_root) {
        Ok(root_output_columns)
            if src.output_columns.is_empty() && matches!(src.sink, DataSink::Noop) =>
        {
            Ok(root_output_columns)
        }
        Ok(_) if src.output_columns.is_empty() => Ok(Vec::new()),
        Ok(root_output_columns) if root_output_columns.is_empty() => {
            encode_output_columns(&src.output_columns)
        }
        Ok(root_output_columns) if root_output_columns.len() == src.output_columns.len() => {
            Ok(root_output_columns)
        }
        Ok(root_output_columns) if matches!(src.sink, DataSink::Noop) => Ok(root_output_columns),
        Ok(root_output_columns) => Err(format!(
            "native fragment {} root output column count {} does not match fragment output column count {}",
            src.fragment_id,
            root_output_columns.len(),
            src.output_columns.len()
        )),
        Err(root_err) => encode_output_columns(&src.output_columns).map_err(|source_err| {
            format!(
                "native fragment {} root output unavailable ({root_err}); fragment output encoding failed: {source_err}",
                src.fragment_id
            )
        }),
    }
}

fn iceberg_write_sink_columns_for_input(
    output_columns: &[AnalysisOutputColumn],
    input: &IcebergWriteInputBinding,
) -> Result<Vec<AnalysisOutputColumn>, String> {
    match input {
        IcebergWriteInputBinding::RootOutputByOrdinal => Ok(output_columns.to_vec()),
        IcebergWriteInputBinding::OutputOrdinals(ordinals) => ordinals
            .iter()
            .copied()
            .map(|ordinal| {
                output_columns.get(ordinal).cloned().ok_or_else(|| {
                    format!("native Iceberg write sink output ordinal {ordinal} is out of range")
                })
            })
            .collect(),
    }
}

fn column_ref_expr_for_output_column(column: &AnalysisOutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn encode_iceberg_write_sink_output_columns(
    target_columns: &[catalog::ColumnDef],
) -> Result<Vec<common::OutputColumn>, String> {
    target_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let ordinal = idx
                .checked_add(1)
                .ok_or_else(|| "native Iceberg write sink output ordinal overflow".to_string())?;
            let column_id = u32::try_from(ordinal)
                .map_err(|_| "native Iceberg write sink output column id overflow".to_string())?;
            Ok(common::OutputColumn {
                column_id,
                name: column.name.clone(),
                r#type: Some(encode_type(&column.data_type)?),
                nullable: column.nullable,
                is_internal: false,
            })
        })
        .collect()
}

pub(crate) fn encode_node(src: &DistributedNode) -> Result<plan::DistributedNode, String> {
    encode_node_with_context(
        src,
        &NativePlanEncodeContext {
            mv_refresh_ctx: None,
        },
    )
}

fn encode_node_with_context(
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedNode, String> {
    let children = src
        .children
        .iter()
        .map(|child| encode_node_with_context(child, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let payload = match &src.payload {
        DistributedPayload::Physical(physical) => {
            plan::distributed_node::Payload::Physical(encode_physical_node(physical, ctx)?)
        }
        DistributedPayload::Exchange(exchange) => {
            plan::distributed_node::Payload::Exchange(encode_exchange_receiver(exchange)?)
        }
    };
    let mut node = plan::DistributedNode {
        node_id: src.node_id,
        fragment_id: src.fragment_id,
        tuple_ids: src.tuple_ids.clone(),
        nullable_tuple_ids: src.nullable_tuple_ids.clone(),
        limit: src.limit,
        build_runtime_filters: src
            .build_runtime_filters
            .iter()
            .map(encode_wired_runtime_filter_build)
            .collect::<Result<Vec<_>, _>>()?,
        probe_runtime_filters: src
            .probe_runtime_filters
            .iter()
            .map(encode_wired_runtime_filter_probe)
            .collect::<Result<Vec<_>, _>>()?,
        children,
        payload: Some(payload),
    };
    normalize_encoded_node_output_columns(&mut node)?;
    Ok(node)
}

fn normalize_encoded_node_output_columns(node: &mut plan::DistributedNode) -> Result<(), String> {
    let normalized_join_output_columns = match node.payload.as_ref() {
        Some(plan::distributed_node::Payload::Physical(physical)) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::HashJoin(join)) => Some(normalize_join_output_columns(
                join.join_type,
                &physical.output_columns,
                &node.children,
                "HashJoinNode",
            )?),
            Some(plan::plan_node::Kind::NestLoopJoin(join)) => Some(normalize_join_output_columns(
                join.join_type,
                &physical.output_columns,
                &node.children,
                "NestLoopJoinNode",
            )?),
            _ => None,
        },
        _ => None,
    };
    if let Some(output_columns) = normalized_join_output_columns {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut() {
            physical.output_columns = output_columns;
        }
        return Ok(());
    }

    let normalized_scan_columns = match node.payload.as_ref() {
        Some(plan::distributed_node::Payload::Physical(physical)) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Scan(scan))
                if output_columns_have_duplicate_ids(&scan.columns)
                    || !scan.required_columns.is_empty() =>
            {
                let scan_columns = if output_columns_have_duplicate_ids(&scan.columns) {
                    deduplicate_output_columns_by_id(&scan.columns)
                } else {
                    scan.columns.clone()
                };
                let output_columns = encoded_scan_output_columns_from_columns(
                    &scan_columns,
                    &scan.required_columns,
                )?;
                Some((output_columns, scan_columns))
            }
            _ => None,
        },
        _ => None,
    };
    if let Some((output_columns, scan_columns)) = normalized_scan_columns {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut() {
            physical.output_columns = output_columns.clone();
            if let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_mut() {
                scan.columns = scan_columns;
            }
        }
        return Ok(());
    }

    let normalized_set_op_child_columns = match node.payload.as_ref() {
        Some(plan::distributed_node::Payload::Physical(physical)) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::SetOp(set_op))
                if !set_op.child_output_columns.is_empty()
                    && set_op.child_output_columns.len() == node.children.len() =>
            {
                let mut changed = false;
                let mut child_output_columns =
                    Vec::with_capacity(set_op.child_output_columns.len());
                for (child_columns, child) in set_op.child_output_columns.iter().zip(&node.children)
                {
                    if output_columns_have_duplicate_ids(&child_columns.columns) {
                        let normalized = encoded_node_output_columns(child)?;
                        if normalized.len() != child_columns.columns.len() {
                            return Err(format!(
                                "SetOpNode child output column count {} does not match child output column count {}",
                                child_columns.columns.len(),
                                normalized.len()
                            ));
                        }
                        child_output_columns.push(plan::OutputColumnList {
                            columns: normalized,
                        });
                        changed = true;
                    } else {
                        child_output_columns.push(child_columns.clone());
                    }
                }
                changed.then_some(child_output_columns)
            }
            _ => None,
        },
        _ => None,
    };
    if let Some(child_output_columns) = normalized_set_op_child_columns
        && let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        && let Some(plan::plan_node::Kind::SetOp(set_op)) = physical.kind.as_mut()
    {
        set_op.child_output_columns = child_output_columns;
    }

    let normalized_output_columns = match node.payload.as_ref() {
        Some(plan::distributed_node::Payload::Physical(physical)) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Sort(sort)) => {
                let requested = if sort.output_columns.is_empty() {
                    physical.output_columns.as_slice()
                } else {
                    sort.output_columns.as_slice()
                };
                if output_columns_have_duplicate_ids(requested) {
                    Some(encoded_passthrough_boundary_output_columns(
                        node, requested, "SortNode",
                    )?)
                } else {
                    None
                }
            }
            _ => None,
        },
        _ => None,
    };

    if let Some(output_columns) = normalized_output_columns
        && let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
    {
        physical.output_columns = output_columns.clone();
        if let Some(plan::plan_node::Kind::Sort(sort)) = physical.kind.as_mut() {
            sort.output_columns = output_columns;
        }
    }
    Ok(())
}

fn normalize_join_output_columns(
    join_type: i32,
    requested: &[common::OutputColumn],
    children: &[plan::DistributedNode],
    node_kind: &str,
) -> Result<Vec<common::OutputColumn>, String> {
    let [left, right] = children else {
        return Ok(requested.to_vec());
    };
    let left = encoded_node_output_columns(left)?;
    let right = encoded_node_output_columns(right)?;
    let derived = derive_join_output_columns(join_type, left, right, node_kind)?;
    if requested.is_empty() || !same_output_column_ids(requested, &derived) {
        return Ok(derived);
    }
    Ok(requested.to_vec())
}

fn derive_join_output_columns(
    join_type: i32,
    left: Vec<common::OutputColumn>,
    right: Vec<common::OutputColumn>,
    node_kind: &str,
) -> Result<Vec<common::OutputColumn>, String> {
    let join_type = plan::JoinKind::try_from(join_type)
        .map_err(|_| format!("{node_kind} unknown join_type {join_type}"))?;
    Ok(match join_type {
        plan::JoinKind::Inner | plan::JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
        plan::JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(nullable_output_columns(right));
            out
        }
        plan::JoinKind::RightOuter => {
            let mut out = nullable_output_columns(left);
            out.extend(right);
            out
        }
        plan::JoinKind::FullOuter => {
            let mut out = nullable_output_columns(left);
            out.extend(nullable_output_columns(right));
            out
        }
        plan::JoinKind::LeftSemi | plan::JoinKind::LeftAnti | plan::JoinKind::NullAwareLeftAnti => {
            left
        }
        plan::JoinKind::RightSemi | plan::JoinKind::RightAnti => right,
        plan::JoinKind::Unspecified => {
            return Err(format!("{node_kind} join_type is unspecified"));
        }
    })
}

fn nullable_output_columns(mut columns: Vec<common::OutputColumn>) -> Vec<common::OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

fn same_output_column_ids(left: &[common::OutputColumn], right: &[common::OutputColumn]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.column_id == right.column_id)
}

fn encoded_scan_output_columns(scan: &plan::ScanNode) -> Result<Vec<common::OutputColumn>, String> {
    encoded_scan_output_columns_from_columns(&scan.columns, &scan.required_columns)
}

fn encoded_scan_output_columns_from_columns(
    columns: &[common::OutputColumn],
    required_columns: &[String],
) -> Result<Vec<common::OutputColumn>, String> {
    if columns.is_empty() {
        return Err("ScanNode columns are empty".to_string());
    }
    if required_columns.is_empty() {
        return Ok(columns.to_vec());
    }

    let required = required_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let output_columns = columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if output_columns.is_empty() {
        return Err(format!(
            "ScanNode required_columns {:?} do not match any scan columns",
            required_columns
        ));
    }
    Ok(output_columns)
}

fn encoded_passthrough_boundary_output_columns(
    node: &plan::DistributedNode,
    requested: &[common::OutputColumn],
    context: &str,
) -> Result<Vec<common::OutputColumn>, String> {
    let child_output_columns = encoded_unary_passthrough_output_columns(node, context)?;
    if child_output_columns.len() != requested.len() {
        return Err(format!(
            "{context} node {} duplicate output column count {} does not match child output column count {}",
            node.node_id,
            requested.len(),
            child_output_columns.len()
        ));
    }
    Ok(child_output_columns)
}

fn output_columns_have_duplicate_ids(columns: &[common::OutputColumn]) -> bool {
    let mut seen = HashSet::with_capacity(columns.len());
    columns.iter().any(|column| !seen.insert(column.column_id))
}

fn deduplicate_output_columns_by_id(columns: &[common::OutputColumn]) -> Vec<common::OutputColumn> {
    let mut seen = HashSet::with_capacity(columns.len());
    columns
        .iter()
        .filter(|column| seen.insert(column.column_id))
        .cloned()
        .collect()
}

#[cfg(test)]
pub(super) fn encoded_physical_variant_names_for_test() -> &'static [&'static str] {
    &[
        "Scan",
        "Filter",
        "Project",
        "Sort",
        "Limit",
        "Values",
        "Repeat",
        "Window",
        "GenerateSeries",
        "TableFunction",
        "AssertOneRow",
        "TopN",
        "HashAggregate",
        "HashJoin",
        "NestLoopJoin",
        "SetOp",
        "ChangeEventExpand",
        "CTEAnchor",
        "CTEProduce",
        "CTEConsume",
        "Redistribute",
    ]
}

fn encode_physical_node(
    src: &PhysicalPlanKind,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanNode, String> {
    use plan::plan_node::Kind;

    let (output_columns, kind) = match src {
        PhysicalPlanKind::Scan(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Scan(encode_scan_node(node, ctx)?),
        ),
        PhysicalPlanKind::Filter(node) => (
            Vec::new(),
            Kind::Filter(plan::FilterNode {
                predicate: Some(encode_expr(&node.predicate)?),
            }),
        ),
        PhysicalPlanKind::Project(node) => (
            Vec::new(),
            Kind::Project(plan::ProjectNode {
                items: node
                    .items
                    .iter()
                    .map(|item| {
                        Ok(plan::ProjectItem {
                            expr: Some(encode_expr(&item.expr)?),
                            output_name: item.output_name.clone(),
                            output_column_id: item.output_column_id.0,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_qualifier: node.output_qualifier.clone(),
            }),
        ),
        PhysicalPlanKind::Sort(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Sort(plan::SortNode {
                items: encode_sort_items(&node.items)?,
                analytic_partition_by: encode_exprs(&node.analytic_partition_by)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                offset: node.offset,
                partition_limit: node.partition_limit.map(usize_to_u64),
                topn_type: node.topn_type.map(encode_sort_topn_type),
            }),
        ),
        PhysicalPlanKind::Limit(node) => (
            Vec::new(),
            Kind::Limit(plan::LimitNode {
                limit: node.limit,
                offset: node.offset,
            }),
        ),
        PhysicalPlanKind::Values(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Values(plan::ValuesNode {
                rows: node
                    .rows
                    .iter()
                    .map(|row| {
                        Ok(plan::ExprList {
                            values: encode_exprs(row)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                columns: encode_output_columns(&node.columns)?,
            }),
        ),
        PhysicalPlanKind::Repeat(node) => (
            Vec::new(),
            Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: node
                    .repeat_column_ref_list
                    .iter()
                    .map(|values| plan::StringList {
                        values: values.clone(),
                    })
                    .collect(),
                repeat_column_ref_ids: node
                    .repeat_column_ref_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.iter().map(|id| id.0).collect(),
                grouping_key_aliases: node
                    .grouping_key_aliases
                    .iter()
                    .map(|(first, second)| plan::StringPair {
                        first: first.clone(),
                        second: second.clone(),
                    })
                    .collect(),
                grouping_fn_args: node
                    .grouping_fn_args
                    .iter()
                    .map(|(name, values)| plan::NamedStringList {
                        name: name.clone(),
                        values: values.clone(),
                    })
                    .collect(),
                grouping_fn_arg_ids: node
                    .grouping_fn_arg_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_fn_ids: node
                    .grouping_fn_ids
                    .iter()
                    .map(|(name, value)| plan::NamedUInt32 {
                        name: name.clone(),
                        value: value.0,
                    })
                    .collect(),
                virtual_tuple_id: node.virtual_tuple_id,
            }),
        ),
        PhysicalPlanKind::Window(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Window(plan::WindowNode {
                window_exprs: node
                    .window_exprs
                    .iter()
                    .map(|expr| {
                        Ok(plan::WindowExpr {
                            name: expr.name.clone(),
                            args: encode_exprs(&expr.args)?,
                            distinct: expr.distinct,
                            partition_by: encode_exprs(&expr.partition_by)?,
                            order_by: encode_sort_items(&expr.order_by)?,
                            window_frame: expr
                                .window_frame
                                .as_ref()
                                .map(encode_window_frame)
                                .transpose()?,
                            result_type: Some(encode_type(&expr.result_type)?),
                            output_name: expr.output_name.clone(),
                            output_column_id: expr.output_column_id.0,
                            ignore_nulls: expr.ignore_nulls,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::GenerateSeries(node) => (
            Vec::new(),
            Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id.0,
            }),
        ),
        PhysicalPlanKind::TableFunction(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::TableFunction(plan::TableFunctionNode {
                function_name: node.function_name.clone(),
                args: encode_exprs(&node.args)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            }),
        ),
        PhysicalPlanKind::AssertOneRow(node) => (
            Vec::new(),
            Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: node.subquery_text.clone(),
                desired_num_rows: node.desired_num_rows,
                assertion: encode_row_count_assertion(node.assertion),
                group_key_column_ids: node
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| column_id.0)
                    .collect(),
                group_key_labels: node.group_key_labels.clone(),
                keyed_message_prefix: node.keyed_message_prefix.clone(),
            }),
        ),
        PhysicalPlanKind::TopN(node) => (
            Vec::new(),
            Kind::Topn(plan::TopNNode {
                items: encode_sort_items(&node.items)?,
                limit: node.limit,
                offset: node.offset,
                phase: encode_topn_phase(node.phase),
                is_split: node.is_split,
            }),
        ),
        PhysicalPlanKind::HashAggregate(node) => {
            let wire = hash_aggregate_wire_output_columns(node)?;
            (
                encode_output_columns(&wire.output_columns)?,
                Kind::HashAggregate(plan::HashAggregateNode {
                    mode: encode_agg_mode(node.mode),
                    group_by: encode_exprs(&node.group_by)?,
                    aggregates: node
                        .aggregates
                        .iter()
                        .map(|call| {
                            Ok(plan::PlanAggregateCall {
                                name: call.name.clone(),
                                args: encode_exprs(&call.args)?,
                                distinct: call.distinct,
                                result_type: Some(encode_type(&call.result_type)?),
                                order_by: encode_sort_items(&call.order_by)?,
                                output_column_id: call.output_column_id.0,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    is_merge: node.is_merge.clone(),
                    output_layout: Some(plan::AggregateOutputLayout {
                        group_key_columns: encode_output_columns(&wire.group_key_columns)?,
                        aggregate_columns: encode_output_columns(&wire.aggregate_columns)?,
                    }),
                    output_columns: encode_output_columns(&wire.output_columns)?,
                }),
            )
        }
        PhysicalPlanKind::HashJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::HashJoin(plan::HashJoinNode {
                join_type: encode_join_kind(node.join_type),
                eq_conditions: node
                    .eq_conditions
                    .iter()
                    .map(|cond| {
                        Ok(plan::HashJoinEqCondition {
                            left: Some(encode_expr(&cond.left)?),
                            right: Some(encode_expr(&cond.right)?),
                            null_safe: cond.null_safe,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                other_condition: node.other_condition.as_ref().map(encode_expr).transpose()?,
                distribution: encode_join_distribution(&node.distribution),
                execution_mode: node.execution_mode.map(encode_join_execution_mode),
                build_runtime_filters: node
                    .build_runtime_filters
                    .iter()
                    .map(|rf| {
                        Ok(plan::RuntimeFilterBuildIntent {
                            filter_id: rf.filter_id,
                            build_expr: Some(encode_expr(&rf.build_expr)?),
                            probe_expr: Some(encode_expr(&rf.probe_expr)?),
                            expr_order: usize_to_u32(rf.expr_order)?,
                            execution_mode: encode_join_execution_mode(rf.execution_mode),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        PhysicalPlanKind::NestLoopJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: encode_join_kind(node.join_type),
                condition: node.condition.as_ref().map(encode_expr).transpose()?,
            }),
        ),
        PhysicalPlanKind::SetOp(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::SetOp(plan::SetOpNode {
                kind: encode_set_op_kind(node.kind),
                output_columns: encode_output_columns(&node.output_columns)?,
                child_output_columns: node
                    .child_output_columns
                    .iter()
                    .map(|columns| {
                        Ok(plan::OutputColumnList {
                            columns: encode_output_columns(columns)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        PhysicalPlanKind::ChangeEventExpand(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: node
                    .events
                    .iter()
                    .map(|event| {
                        Ok(plan::DistributedChangeEventSpec {
                            predicate: event.predicate.as_ref().map(encode_expr).transpose()?,
                            branch_kind: encode_change_stream_branch_kind(event.branch_kind),
                            assignments: event
                                .assignments
                                .iter()
                                .map(|assignment| {
                                    Ok(plan::DistributedChangeEventOutputExpr {
                                        output_column_id: assignment.output_column_id.0,
                                        expr: assignment
                                            .expr
                                            .as_ref()
                                            .map(encode_expr)
                                            .transpose()?,
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
                change_op_column_id: node.change_op_column_id.0,
                data_route_column_id: node.data_route_column_id.map(|id| id.0),
            }),
        ),
        PhysicalPlanKind::CTEAnchor(node) => (
            Vec::new(),
            Kind::CteAnchor(plan::CteAnchorNode {
                cte_id: node.cte_id,
            }),
        ),
        PhysicalPlanKind::CTEProduce(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteProduce(plan::CteProduceNode {
                cte_id: node.cte_id,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::CTEConsume(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteConsume(plan::CteConsumeNode {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: encode_output_columns(&node.output_columns)?,
                producer_column_ids: node.producer_column_ids.iter().map(|id| id.0).collect(),
            }),
        ),
        PhysicalPlanKind::Redistribute(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Redistribute(plan::RedistributeNode {
                mode: Some(encode_redistribute_mode(&node.mode)),
                partition_exprs: encode_exprs(&node.partition_exprs)?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
    };

    Ok(plan::PlanNode {
        output_columns,
        kind: Some(kind),
    })
}

struct HashAggregateWireOutputColumns {
    group_key_columns: Vec<AnalysisOutputColumn>,
    aggregate_columns: Vec<AnalysisOutputColumn>,
    output_columns: Vec<AnalysisOutputColumn>,
}

fn hash_aggregate_wire_output_columns(
    node: &PhysicalHashAggregateNode,
) -> Result<HashAggregateWireOutputColumns, String> {
    if node.output_layout.aggregate_columns.len() != node.aggregates.len() {
        return Err(format!(
            "native HashAggregate output_layout aggregate column count {} does not match aggregate count {}",
            node.output_layout.aggregate_columns.len(),
            node.aggregates.len()
        ));
    }

    let group_key_columns = node.output_layout.group_key_columns.clone();
    let mut aggregate_columns = node.output_layout.aggregate_columns.clone();
    if hash_aggregate_outputs_intermediate(node.mode) {
        for (idx, (column, call)) in aggregate_columns
            .iter_mut()
            .zip(node.aggregates.iter())
            .enumerate()
        {
            column.data_type = aggregate_intermediate_type(call).map_err(|err| {
                format!("native HashAggregate aggregate {idx} intermediate type: {err}")
            })?;
        }
    }

    let mut full_output_columns =
        Vec::with_capacity(group_key_columns.len() + aggregate_columns.len());
    full_output_columns.extend(group_key_columns.iter().cloned());
    full_output_columns.extend(aggregate_columns.iter().cloned());

    let output_columns = if node.output_columns.is_empty() {
        full_output_columns
    } else {
        let data_type_by_id = full_output_columns
            .iter()
            .map(|column| (column.column_id, column.data_type.clone()))
            .collect::<HashMap<_, _>>();
        let mut output_columns = node.output_columns.clone();
        for column in &mut output_columns {
            let data_type = data_type_by_id.get(&column.column_id).ok_or_else(|| {
                format!(
                    "native HashAggregate output column {} missing from output_layout",
                    column.column_id.0
                )
            })?;
            column.data_type = data_type.clone();
        }
        output_columns
    };

    Ok(HashAggregateWireOutputColumns {
        group_key_columns,
        aggregate_columns,
        output_columns,
    })
}

fn hash_aggregate_outputs_intermediate(mode: AggMode) -> bool {
    !matches!(mode, AggMode::Single | AggMode::Global)
}

fn aggregate_intermediate_type(call: &AggregateCall) -> Result<DataType, String> {
    let function_name = aggregate_function_name(call);
    let arg_types = call
        .args
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();
    infer_agg_function_types(&function_name, &arg_types, call.distinct)?
        .1
        .ok_or_else(|| format!("{} does not expose an intermediate type", function_name))
}

fn aggregate_function_name(call: &AggregateCall) -> String {
    let name = call.name.to_ascii_lowercase();
    if !call.distinct {
        return name;
    }
    match name.as_str() {
        "count" => "multi_distinct_count".to_string(),
        "sum" => "multi_distinct_sum".to_string(),
        "array_agg" => "array_agg_distinct".to_string(),
        _ => name,
    }
}

fn encode_row_count_assertion(assertion: PlanRowCountAssertion) -> i32 {
    match assertion {
        PlanRowCountAssertion::Eq => plan::RowCountAssertion::Eq as i32,
        PlanRowCountAssertion::Ne => plan::RowCountAssertion::Ne as i32,
        PlanRowCountAssertion::Lt => plan::RowCountAssertion::Lt as i32,
        PlanRowCountAssertion::Le => plan::RowCountAssertion::Le as i32,
        PlanRowCountAssertion::Gt => plan::RowCountAssertion::Gt as i32,
        PlanRowCountAssertion::Ge => plan::RowCountAssertion::Ge as i32,
    }
}

fn encode_scan_node(
    src: &crate::sql::planner::plan::PlanScanNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanNode, String> {
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(&src.table, ctx)?),
        alias: src.alias.clone(),
        columns: encode_output_columns(&src.columns)?,
        predicates: encode_exprs(&src.predicates)?,
        required_columns: src.required_columns.clone().unwrap_or_default(),
        dict_columns: Vec::new(),
        variant_columns: src
            .variant_columns
            .iter()
            .map(|column| {
                Ok(plan::ScanVariantColumn {
                    source_column_id: column.source_column_id.0,
                    source_column: column.source_column.clone(),
                    synthetic_column_id: column.synthetic_column_id.0,
                    synthetic_column: column.synthetic_column.clone(),
                    canonical_path: column.canonical_path.clone(),
                    requested_type: Some(encode_type(&column.requested_type)?),
                    strict: column.strict,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        mv_rewritten_from: src.mv_rewritten_from.clone(),
    })
}

fn encode_exchange_receiver(src: &ExchangeReceiver) -> Result<plan::ExchangeReceiver, String> {
    Ok(plan::ExchangeReceiver {
        partition_type: encode_edge_partition_type(&src.partition),
        partition_exprs: encode_exprs(&src.partition.exprs)?,
        source_fragment_id: src.source_fragment_id,
        output_columns: encode_output_columns(&src.output_columns)?,
        output_qualifier: src.output_qualifier.clone(),
        flavor: Some(encode_exchange_flavor(&src.flavor)?),
    })
}

fn encode_exchange_flavor(src: &ExchangeFlavor) -> Result<plan::ExchangeFlavor, String> {
    use plan::exchange_flavor::Kind;

    Ok(plan::ExchangeFlavor {
        kind: Some(match src {
            ExchangeFlavor::Distribution => Kind::Distribution(true),
            ExchangeFlavor::LimitOffset { limit, offset } => {
                Kind::LimitOffset(plan::LimitOffsetFlavor {
                    limit: *limit,
                    offset: *offset,
                })
            }
            ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => Kind::TopnSplit(plan::TopNSplitFlavor {
                items: encode_sort_items(items)?,
                limit: *limit,
                offset: *offset,
            }),
            ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastFlavor {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
        }),
    })
}

pub(crate) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
    Ok(plan::DataPartition {
        kind: match src.kind {
            PartitionKind::Unpartitioned => plan::PartitionKind::Unpartitioned as i32,
            PartitionKind::Random => plan::PartitionKind::Random as i32,
            PartitionKind::Hash => plan::PartitionKind::Hash as i32,
        },
        exprs: encode_exprs(&src.exprs)?,
    })
}

fn encode_data_sink(
    src: &DataSink,
    fragment_output_columns: &[AnalysisOutputColumn],
) -> Result<plan::DataSink, String> {
    use plan::data_sink::Kind;

    Ok(plan::DataSink {
        kind: Some(match src {
            DataSink::Result => Kind::Result(true),
            DataSink::Noop => Kind::Noop(true),
            DataSink::IcebergWrite(sink) => Kind::IcebergWrite(plan::IcebergWriteFragmentSink {
                descriptor_database: sink.descriptor_database.clone(),
                spec: Some(encode_iceberg_write_sink_spec(&sink.spec)?),
                input: Some(encode_iceberg_write_input_binding(&sink.input)),
            }),
            DataSink::IcebergChangeStreamRouter(sink) => {
                Kind::IcebergChangeStreamRouter(plan::IcebergChangeStreamRouterSink {
                    group_id: sink.group_id,
                    change_op_output_ordinal: usize_to_u64(sink.change_op_output_ordinal),
                    data_route_output_ordinal: sink.data_route_output_ordinal.map(usize_to_u64),
                    branches: sink
                        .branches
                        .iter()
                        .map(|branch| {
                            Ok(plan::IcebergChangeStreamBranchRoute {
                                branch_id: branch.branch_id,
                                branch_kind: encode_change_stream_branch_kind(branch.branch_kind),
                                target_fragment_id: branch.target_fragment_id,
                                target_exchange_node_id: branch.target_exchange_node_id,
                                output_ordinals: branch
                                    .output_ordinals
                                    .iter()
                                    .map(|value| usize_to_u64(*value))
                                    .collect(),
                                output_partition_ordinals: branch
                                    .output_partition_ordinals
                                    .iter()
                                    .map(|value| usize_to_u64(*value))
                                    .collect(),
                                output_partition: Some(encode_change_stream_branch_partition(
                                    branch,
                                    fragment_output_columns,
                                )?),
                                destinations: None,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            }
        }),
    })
}

fn encode_change_stream_branch_partition(
    branch: &crate::sql::planner::IcebergChangeStreamBranchRoute,
    fragment_output_columns: &[AnalysisOutputColumn],
) -> Result<plan::DataPartition, String> {
    if branch.output_partition_ordinals.is_empty() {
        return Ok(plan::DataPartition {
            kind: plan::PartitionKind::Unpartitioned as i32,
            exprs: Vec::new(),
        });
    }
    let exprs = branch
        .output_partition_ordinals
        .iter()
        .map(|ordinal| {
            fragment_output_columns
                .get(*ordinal)
                .ok_or_else(|| {
                    format!(
                        "native Iceberg change-stream router branch {} partition ordinal {} is out of range",
                        branch.branch_id, ordinal
                    )
                })
                .map(column_ref_expr_for_output_column)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(plan::DataPartition {
        kind: plan::PartitionKind::Hash as i32,
        exprs: encode_exprs(&exprs)?,
    })
}

fn encode_fragment_edge(src: &FragmentEdge) -> Result<plan::FragmentEdge, String> {
    Ok(plan::FragmentEdge {
        source_fragment_id: src.source_fragment_id,
        target_fragment_id: src.target_fragment_id,
        target_exchange_node_id: src.target_exchange_node_id,
        output_partition: encode_edge_partition_type(&src.output_partition),
        stream_kind: match src.stream_kind {
            FragmentStreamKind::Gather => plan::FragmentStreamKind::Gather as i32,
            FragmentStreamKind::Broadcast => plan::FragmentStreamKind::Broadcast as i32,
            FragmentStreamKind::Partitioned => plan::FragmentStreamKind::Partitioned as i32,
            FragmentStreamKind::Other => plan::FragmentStreamKind::Other as i32,
        },
        edge_kind: Some(encode_fragment_edge_kind(&src.edge_kind)),
        output_slot_ids: src.output_slot_ids.clone(),
    })
}

fn encode_fragment_edge_kind(src: &FragmentEdgeKind) -> plan::FragmentEdgeKind {
    use plan::fragment_edge_kind::Kind;

    plan::FragmentEdgeKind {
        kind: Some(match src {
            FragmentEdgeKind::Stream => Kind::Stream(true),
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastEdge {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
            FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id,
                branch_id,
                branch_kind,
            } => Kind::IcebergChangeStreamRouter(plan::IcebergChangeStreamRouterEdge {
                router_group_id: *router_group_id,
                branch_id: *branch_id,
                branch_kind: encode_change_stream_branch_kind(*branch_kind),
            }),
        }),
    }
}

fn encode_wired_runtime_filter_build(
    src: &WiredRuntimeFilterBuild,
) -> Result<plan::RuntimeFilterBuild, String> {
    Ok(plan::RuntimeFilterBuild {
        filter_id: src.filter_id,
        build_expr: Some(encode_expr(&src.build_expr)?),
        probe_expr: Some(encode_expr(&src.probe_expr)?),
        expr_order: usize_to_u32(src.expr_order)?,
        execution_mode: encode_join_execution_mode(src.execution_mode),
        source_fragment_id: src.source_fragment_id,
        target_fragment_ids: src.target_fragment_ids.clone(),
    })
}

fn encode_wired_runtime_filter_probe(
    src: &WiredRuntimeFilterProbe,
) -> Result<plan::RuntimeFilterProbe, String> {
    Ok(plan::RuntimeFilterProbe {
        filter_id: src.filter_id,
        probe_expr: Some(encode_expr(&src.probe_expr)?),
        source_fragment_id: src.source_fragment_id,
    })
}

fn encode_table_def(src: &catalog::TableDef) -> Result<plan::TableDef, String> {
    encode_table_def_with_context(
        src,
        &NativePlanEncodeContext {
            mv_refresh_ctx: None,
        },
    )
}

fn encode_table_def_with_context(
    src: &catalog::TableDef,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::TableDef, String> {
    Ok(plan::TableDef {
        name: src.name.clone(),
        columns: src
            .columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        iceberg_row_lineage_metadata_columns: src
            .iceberg_row_lineage_metadata_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        source: Some(encode_scan_source(&src.source, ctx)?),
    })
}

fn encode_column_def(src: &catalog::ColumnDef) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        write_default_json: src
            .write_default
            .as_ref()
            .map(|literal| encode_column_write_default_json(src, literal))
            .transpose()?,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn encode_column_write_default_json(
    column: &catalog::ColumnDef,
    literal: &iceberg::spec::Literal,
) -> Result<String, String> {
    let iceberg_type = iceberg_type_for_column_def(column)?;
    literal
        .clone()
        .try_into_json(&iceberg_type)
        .map(|json| json.to_string())
        .map_err(|err| {
            format!(
                "encode write_default_json for column `{}` as {:?}: {err}",
                column.name, iceberg_type
            )
        })
}

fn iceberg_type_for_column_def(column: &catalog::ColumnDef) -> Result<Type, String> {
    if let Some(logical_type) = column.logical_type.as_ref() {
        let mut next_field_id = 1;
        return crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
            logical_type,
            &mut next_field_id,
        );
    }
    iceberg_type_for_arrow_data_type(&column.data_type)
}

fn iceberg_type_for_arrow_data_type(data_type: &DataType) -> Result<Type, String> {
    if let Some(primitive) = iceberg_primitive_type_for_arrow_data_type(data_type)? {
        return Ok(Type::Primitive(primitive));
    }

    match data_type {
        DataType::Struct(fields) => Ok(Type::Struct(StructType::new(
            fields
                .iter()
                .map(|field| iceberg_nested_field_for_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::List(element) | DataType::LargeList(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_for_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "native plan MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "native plan MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_for_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_for_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "native plan cannot encode write_default_json for Arrow type {other:?} without a logical Iceberg type"
        )),
    }
}

fn iceberg_primitive_type_for_arrow_data_type(
    data_type: &DataType,
) -> Result<Option<PrimitiveType>, String> {
    Ok(Some(match data_type {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => PrimitiveType::Time,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            PrimitiveType::TimestampNs
        }
        DataType::Decimal128(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                format!("Decimal128 negative scale {scale} is not supported by Iceberg defaults")
            })?;
            PrimitiveType::Decimal {
                precision: u32::from(*precision),
                scale,
            }
        }
        _ => return Ok(None),
    }))
}

fn iceberg_nested_field_for_arrow_field(
    field: &Field,
) -> Result<iceberg::spec::NestedFieldRef, String> {
    let field_id = arrow_field_id(field)?;
    let field_type = iceberg_type_for_arrow_data_type(field.data_type())?;
    Ok(Arc::new(NestedField::new(
        field_id,
        field.name(),
        field_type,
        !field.is_nullable(),
    )))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "native plan field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|err| {
        format!(
            "native plan field {} has invalid parquet field id {raw}: {err}",
            field.name()
        )
    })
}

fn encode_scan_source(
    src: &catalog::ScanSource,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    Ok(plan::ScanSource {
        kind: Some(match src {
            catalog::ScanSource::StarRocks { db_id, table_id } => {
                return Err(format!(
                    "StarRocks scan source (db_id={db_id}, table_id={table_id}) is compat-only; native plan encoding does not support it"
                ));
            }
            catalog::ScanSource::IcebergDataFiles {
                table,
                files,
                cloud_properties,
                binding,
            } => Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(table)?),
                files: files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                binding: match binding {
                    catalog::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    catalog::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            }),
            catalog::ScanSource::IcebergMetadataTable {
                table,
                metadata_table_type,
                serialized_table,
                cloud_properties,
                metadata_payload,
            } => Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
                table: Some(encode_iceberg_table_info(table)?),
                metadata_table_type: encode_iceberg_metadata_table_type(metadata_table_type),
                serialized_table: serialized_table.clone(),
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                metadata_payload: metadata_payload.clone(),
            }),
            catalog::ScanSource::IcebergDeltaTable {
                table,
                from_snapshot_id,
                to_snapshot_id,
            } => {
                let runtime_plan =
                    crate::sql::codegen::iceberg_delta_scan_wire::build_iceberg_delta_scan_runtime_plan(
                        table,
                        *from_snapshot_id,
                        *to_snapshot_id,
                        ctx.mv_refresh_ctx,
                    )
                    .map_err(|err| format!("Iceberg delta scan native sidecar: {err}"))?;

                Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    from_snapshot_id: *from_snapshot_id,
                    to_snapshot_id: *to_snapshot_id,
                    delta_plan: Some(
                        crate::sql::codegen::iceberg_delta_scan_wire::encode_iceberg_delta_scan_plan_native(
                            &runtime_plan,
                        )?,
                    ),
                })
            }
            catalog::ScanSource::IcebergVersionTable { table, snapshot_id } => {
                Kind::IcebergVersionTable(plan::IcebergVersionTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    snapshot_id: *snapshot_id,
                })
            }
            catalog::ScanSource::IcebergMvTargetState(scan) => {
                Kind::IcebergMvTargetState(plan::IcebergMvTargetState {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    aggregate_state_layout_version: u32::from(scan.aggregate_state_layout_version),
                    columns: scan
                        .columns
                        .iter()
                        .map(encode_column_def)
                        .collect::<Result<Vec<_>, _>>()?,
                    group_key_names: scan.group_key_names.clone(),
                    aggregate_state_names: scan.aggregate_state_names.clone(),
                    physical_column_names: scan.physical_column_names.clone(),
                    row_id_column_name: scan.row_id_column_name.clone(),
                    row_filter: Some(encode_mv_target_state_row_filter(&scan.row_filter)),
                    partition_constraint: match scan.partition_constraint {
                        catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                            plan::IcebergMvTargetStatePartitionConstraint::Unpartitioned as i32
                        }
                        catalog::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                            plan::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired as i32
                        }
                    },
                })
            }
            catalog::ScanSource::IcebergMvTargetLocator(scan) => {
                Kind::IcebergMvTargetLocator(plan::IcebergMvTargetLocator {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    apply_key_column: scan.apply_key_column.clone(),
                    branch_id_column: scan.branch_id_column.clone(),
                })
            }
        }),
    })
}

fn encode_mv_target_state_row_filter(
    src: &catalog::IcebergMvTargetStateRowFilter,
) -> plan::IcebergMvTargetStateRowFilter {
    use plan::iceberg_mv_target_state_row_filter::Kind;

    plan::IcebergMvTargetStateRowFilter {
        kind: Some(match src {
            catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } => Kind::DeltaInputRowIds(plan::DeltaInputRowIdsFilter {
                row_id_column_name: row_id_column_name.clone(),
                branch_scope: branch_scope.as_ref().map(|scope| plan::BranchScope {
                    branch_id_column_name: scope.branch_id_column_name.clone(),
                    branch_id: scope.branch_id,
                }),
            }),
        }),
    }
}

fn encode_iceberg_table_info(
    src: &catalog::IcebergTableInfo,
) -> Result<plan::IcebergTableInfo, String> {
    Ok(plan::IcebergTableInfo {
        catalog: src.catalog.clone(),
        namespace: src.namespace.clone(),
        table: src.table.clone(),
        table_uuid: src.table_uuid.clone(),
        current_snapshot_id: src.current_snapshot_id,
        schema_id: src.schema_id,
        location: src.location.clone(),
        schema: Some(encode_iceberg_schema_def(&src.schema)?),
        serialized_metadata: src.serialized_metadata.clone(),
        serialized_metadata_rows: src.serialized_metadata_rows.clone(),
    })
}

fn encode_iceberg_schema_def(
    src: &catalog::IcebergSchemaDef,
) -> Result<plan::IcebergSchemaDef, String> {
    Ok(plan::IcebergSchemaDef {
        fields: src
            .fields
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_field(
    src: &catalog::IcebergSchemaFieldDef,
) -> Result<plan::IcebergSchemaFieldDef, String> {
    Ok(plan::IcebergSchemaFieldDef {
        field_id: src.field_id,
        name: src.name.clone(),
        initial_default_json: encode_iceberg_schema_default_json(
            "initial_default",
            src.initial_default_json.as_ref(),
            src.initial_default.as_ref(),
        )?,
        write_default_json: encode_iceberg_schema_default_json(
            "write_default",
            src.write_default_json.as_ref(),
            src.write_default.as_ref(),
        )?,
        children: src
            .children
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_default_json(
    label: &str,
    precomputed_json: Option<&String>,
    literal: Option<&iceberg::spec::Literal>,
) -> Result<Option<String>, String> {
    if let Some(json) = precomputed_json {
        return Ok(Some(json.clone()));
    }
    literal
        .map(crate::sql::codegen::descriptors::serialize_iceberg_literal_json)
        .transpose()
        .map_err(|err| format!("encode Iceberg schema {label} JSON: {err}"))
}

fn encode_iceberg_data_file_info(
    src: &catalog::IcebergDataFileInfo,
) -> Result<plan::IcebergDataFileInfo, String> {
    Ok(plan::IcebergDataFileInfo {
        path: src.path.clone(),
        size: src.size,
        row_count: src.row_count,
        column_stats: src
            .column_stats
            .as_ref()
            .map(|stats| plan::IcebergColumnStatsMap {
                entries: stats
                    .iter()
                    .map(|(name, stats)| (name.clone(), encode_iceberg_column_stats(stats)))
                    .collect::<HashMap<_, _>>(),
            }),
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        ivm_change_op: src.ivm_change_op.map(i32::from),
        included_positions: src
            .included_positions
            .as_ref()
            .map(|values| plan::Int64List {
                values: values.clone(),
            }),
        delete_files: src
            .delete_files
            .iter()
            .map(encode_iceberg_delete_file_info)
            .collect(),
        manifest_path: src.manifest_path.clone(),
        partition_values: src
            .partition_values
            .iter()
            .map(encode_iceberg_partition_field_value)
            .collect(),
    })
}

fn encode_iceberg_column_stats(src: &catalog::IcebergColumnStats) -> plan::IcebergColumnStats {
    plan::IcebergColumnStats {
        null_count: src.null_count,
        value_count: src.value_count,
        column_size: src.column_size,
        lower_bound: src.lower_bound.clone(),
        upper_bound: src.upper_bound.clone(),
    }
}

fn encode_iceberg_delete_file_info(
    src: &catalog::IcebergDeleteFileInfo,
) -> plan::IcebergDeleteFileInfo {
    plan::IcebergDeleteFileInfo {
        path: src.path.clone(),
        file_format: match src.file_format {
            catalog::IcebergDeleteFileFormat::Parquet => {
                plan::IcebergDeleteFileFormat::Parquet as i32
            }
            catalog::IcebergDeleteFileFormat::Puffin => {
                plan::IcebergDeleteFileFormat::Puffin as i32
            }
        },
        file_content: match src.file_content {
            catalog::IcebergDeleteFileContent::Position => {
                plan::IcebergDeleteFileContent::Position as i32
            }
            catalog::IcebergDeleteFileContent::Equality => {
                plan::IcebergDeleteFileContent::Equality as i32
            }
        },
        length: src.length,
        content_offset: src.content_offset,
        content_size_in_bytes: src.content_size_in_bytes,
        sequence_number: src.sequence_number,
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        equality_column_names: src.equality_column_names.clone(),
        equality_field_ids: src.equality_field_ids.clone(),
    }
}

fn encode_iceberg_partition_field_value(
    src: &catalog::IcebergPartitionFieldValue,
) -> plan::IcebergPartitionFieldValue {
    plan::IcebergPartitionFieldValue {
        source_column: src.source_column.clone(),
        field_name: src.field_name.clone(),
        transform: src.transform.clone(),
        value: src.value.as_ref().map(encode_iceberg_partition_value),
    }
}

fn encode_iceberg_partition_value(
    src: &catalog::IcebergPartitionValue,
) -> plan::IcebergPartitionValue {
    use plan::iceberg_partition_value::Value;

    plan::IcebergPartitionValue {
        value: Some(match src {
            catalog::IcebergPartitionValue::Boolean(value) => Value::BoolValue(*value),
            catalog::IcebergPartitionValue::Int32(value) => Value::Int32Value(*value),
            catalog::IcebergPartitionValue::Int64(value) => Value::Int64Value(*value),
            catalog::IcebergPartitionValue::Float(value) => Value::FloatValue(*value),
            catalog::IcebergPartitionValue::Double(value) => Value::DoubleValue(*value),
            catalog::IcebergPartitionValue::String(value) => Value::StringValue(value.clone()),
            catalog::IcebergPartitionValue::Binary(value) => Value::BinaryValue(value.clone()),
        }),
    }
}

fn encode_iceberg_write_sink_spec(
    src: &IcebergWriteSinkSpec,
) -> Result<plan::IcebergWriteSinkSpec, String> {
    Ok(plan::IcebergWriteSinkSpec {
        mode: encode_iceberg_write_sink_mode(src.mode),
        target_table_id: src.target_table_id,
        target_table: Some(encode_table_def(&src.target_table)?),
        iceberg: Some(encode_iceberg_table_info(&src.iceberg)?),
        target_columns: src
            .target_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        table_location: src.table_location.clone(),
        data_location: src.data_location.clone(),
        target_partition_spec_id: src.target_partition_spec_id,
        cloud_properties: src.cloud_properties.clone().into_iter().collect(),
        file_format: src.file_format.clone(),
        compression: match src.compression {
            IcebergWriteFileCompression::Snappy => plan::IcebergWriteFileCompression::Snappy as i32,
        },
        position_delete_output_descriptor: src
            .position_delete_output_descriptor
            .as_ref()
            .map(encode_position_delete_descriptor)
            .transpose()?,
    })
}

fn encode_iceberg_write_input_binding(
    src: &IcebergWriteInputBinding,
) -> plan::IcebergWriteInputBinding {
    use plan::iceberg_write_input_binding::Kind;

    plan::IcebergWriteInputBinding {
        kind: Some(match src {
            IcebergWriteInputBinding::RootOutputByOrdinal => Kind::RootOutputByOrdinal(true),
            IcebergWriteInputBinding::OutputOrdinals(values) => {
                Kind::OutputOrdinals(plan::UInt64List {
                    values: values.iter().map(|value| usize_to_u64(*value)).collect(),
                })
            }
        }),
    }
}

fn encode_position_delete_descriptor(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
) -> Result<plan::PositionDeleteDescriptorInput, String> {
    Ok(plan::PositionDeleteDescriptorInput {
        file_path: Some(encode_position_delete_output_field(&src.file_path)?),
        pos: Some(encode_position_delete_output_field(&src.pos)?),
        partition_source_fields: src
            .partition_source_fields
            .iter()
            .map(encode_position_delete_partition_source_field)
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: src.target_partition_spec_id,
    })
}

fn encode_position_delete_output_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
) -> Result<plan::PositionDeleteOutputField, String> {
    Ok(plan::PositionDeleteOutputField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        field_id: src.field_id,
    })
}

fn encode_position_delete_partition_source_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
) -> Result<plan::PositionDeletePartitionSourceField, String> {
    Ok(plan::PositionDeletePartitionSourceField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        source_column_name: src.source_column_name.clone(),
        partition_field_name: src.partition_field_name.clone(),
        transform_expr: src.transform_expr.clone(),
        source_field_id: src.source_field_id,
        data_type: Some(encode_type(&src.data_type)?),
    })
}

fn encode_output_columns(
    src: &[crate::sql::analysis::OutputColumn],
) -> Result<Vec<common::OutputColumn>, String> {
    src.iter().map(encode_output_column).collect()
}

fn encode_output_column(
    src: &crate::sql::analysis::OutputColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: src.column_id.0,
        name: src.name.clone(),
        r#type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        is_internal: src.is_internal,
    })
}

fn encode_exprs(
    src: &[crate::sql::analysis::TypedExpr],
) -> Result<Vec<crate::proto::expr::Expr>, String> {
    src.iter().map(encode_expr).collect()
}

fn encode_sql_type(src: &SqlType) -> Result<common::TypeDesc, String> {
    use common::type_desc::Kind;

    Ok(common::TypeDesc {
        kind: Some(match src {
            SqlType::Array(element) => Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_sql_type(element)?)),
            })),
            SqlType::Map(key, value) => Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_sql_type(key)?)),
                value: Some(Box::new(encode_sql_type(value)?)),
            })),
            SqlType::Struct(fields) => Kind::Strct(common::StructType {
                fields: fields
                    .iter()
                    .map(|(name, ty)| {
                        Ok(common::StructField {
                            name: name.clone(),
                            r#type: Some(encode_sql_type(ty)?),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
            other => Kind::Scalar(sql_scalar_type(other)?),
        }),
    })
}

fn sql_scalar_type(src: &SqlType) -> Result<common::ScalarType, String> {
    use common::PrimitiveType;

    let (primitive, precision, scale, time_unit) = match src {
        SqlType::TinyInt => (PrimitiveType::Tinyint, None, None, None),
        SqlType::SmallInt => (PrimitiveType::Smallint, None, None, None),
        SqlType::Int => (PrimitiveType::Int, None, None, None),
        SqlType::BigInt => (PrimitiveType::Bigint, None, None, None),
        SqlType::LargeInt => (PrimitiveType::Largeint, None, None, None),
        SqlType::Float => (PrimitiveType::Float, None, None, None),
        SqlType::Double => (PrimitiveType::Double, None, None, None),
        SqlType::Decimal { precision, scale } => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
            None,
        ),
        SqlType::String => (PrimitiveType::Varchar, None, None, None),
        SqlType::Json => (PrimitiveType::Json, None, None, None),
        SqlType::Binary => (PrimitiveType::Varbinary, None, None, None),
        SqlType::Bitmap => (PrimitiveType::Bitmap, None, None, None),
        SqlType::Hll => (PrimitiveType::Hll, None, None, None),
        SqlType::Boolean => (PrimitiveType::Boolean, None, None, None),
        SqlType::Date => (PrimitiveType::Date, None, None, None),
        SqlType::DateTime => (PrimitiveType::Datetime, None, None, None),
        SqlType::DateTimeNs => (PrimitiveType::Datetime, None, None, Some(3)),
        SqlType::Time => (PrimitiveType::Time, None, None, None),
        SqlType::Variant => (PrimitiveType::Variant, None, None, None),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err("nested SqlType cannot be encoded as scalar TypeDesc".to_string());
        }
    };
    Ok(common::ScalarType {
        r#type: primitive as i32,
        len: None,
        precision,
        scale,
        time_unit,
    })
}

fn encode_edge_partition_type(src: &DataPartition) -> i32 {
    match src.kind {
        PartitionKind::Unpartitioned => plan::PartitionType::Unpartitioned as i32,
        PartitionKind::Random => plan::PartitionType::Random as i32,
        PartitionKind::Hash => plan::PartitionType::Hash as i32,
    }
}

fn encode_join_kind(src: JoinKind) -> i32 {
    match src {
        JoinKind::Inner => plan::JoinKind::Inner as i32,
        JoinKind::LeftOuter => plan::JoinKind::LeftOuter as i32,
        JoinKind::RightOuter => plan::JoinKind::RightOuter as i32,
        JoinKind::FullOuter => plan::JoinKind::FullOuter as i32,
        JoinKind::Cross => plan::JoinKind::Cross as i32,
        JoinKind::LeftSemi => plan::JoinKind::LeftSemi as i32,
        JoinKind::RightSemi => plan::JoinKind::RightSemi as i32,
        JoinKind::LeftAnti => plan::JoinKind::LeftAnti as i32,
        JoinKind::RightAnti => plan::JoinKind::RightAnti as i32,
        JoinKind::NullAwareLeftAnti => plan::JoinKind::NullAwareLeftAnti as i32,
    }
}

fn encode_join_distribution(src: &JoinDistribution) -> i32 {
    match src {
        JoinDistribution::Unknown => plan::JoinDistribution::Unknown as i32,
        JoinDistribution::Shuffle => plan::JoinDistribution::Shuffle as i32,
        JoinDistribution::Broadcast => plan::JoinDistribution::Broadcast as i32,
        JoinDistribution::Colocate => plan::JoinDistribution::Colocate as i32,
    }
}

fn encode_join_execution_mode(src: JoinExecutionMode) -> i32 {
    match src {
        JoinExecutionMode::Broadcast => plan::JoinExecutionMode::Broadcast as i32,
        JoinExecutionMode::Partitioned => plan::JoinExecutionMode::Partitioned as i32,
        JoinExecutionMode::Colocate => plan::JoinExecutionMode::Colocate as i32,
    }
}

fn encode_agg_mode(src: AggMode) -> i32 {
    match src {
        AggMode::Single => plan::AggMode::Single as i32,
        AggMode::Local => plan::AggMode::Local as i32,
        AggMode::Global => plan::AggMode::Global as i32,
        AggMode::DistinctGlobal => plan::AggMode::DistinctGlobal as i32,
        AggMode::DistinctLocal => plan::AggMode::DistinctLocal as i32,
    }
}

fn encode_topn_phase(src: TopNPhase) -> i32 {
    match src {
        TopNPhase::Partial => plan::TopNPhase::TopnPhasePartial as i32,
        TopNPhase::Final => plan::TopNPhase::TopnPhaseFinal as i32,
    }
}

fn encode_set_op_kind(src: PlanSetOpKind) -> i32 {
    match src {
        PlanSetOpKind::UnionAll => plan::PlanSetOpKind::UnionAll as i32,
        PlanSetOpKind::UnionDistinct => plan::PlanSetOpKind::UnionDistinct as i32,
        PlanSetOpKind::Intersect => plan::PlanSetOpKind::Intersect as i32,
        PlanSetOpKind::Except => plan::PlanSetOpKind::Except as i32,
    }
}

fn encode_change_stream_branch_kind(src: ChangeStreamBranchKind) -> i32 {
    match src {
        ChangeStreamBranchKind::DeleteDv => plan::ChangeStreamBranchKind::DeleteDv as i32,
        ChangeStreamBranchKind::ReuseData => plan::ChangeStreamBranchKind::ReuseData as i32,
        ChangeStreamBranchKind::FreshData => plan::ChangeStreamBranchKind::FreshData as i32,
    }
}

fn encode_sort_topn_type(src: crate::exec::node::sort::SortTopNType) -> i32 {
    match src {
        crate::exec::node::sort::SortTopNType::RowNumber => {
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        }
        crate::exec::node::sort::SortTopNType::Rank => plan::SortTopNType::SortTopnTypeRank as i32,
        crate::exec::node::sort::SortTopNType::DenseRank => {
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        }
    }
}

fn encode_hash_source(src: HashSource) -> i32 {
    match src {
        HashSource::ShuffleAgg => plan::HashSource::ShuffleAgg as i32,
        HashSource::ShuffleJoin => plan::HashSource::ShuffleJoin as i32,
    }
}

fn encode_redistribute_mode(src: &RedistributeMode) -> plan::RedistributeMode {
    use plan::redistribute_mode::Mode;

    plan::RedistributeMode {
        mode: Some(match src {
            RedistributeMode::Gather => Mode::Gather(true),
            RedistributeMode::Hash { cols, source } => Mode::Hash(plan::RedistributeHash {
                cols: cols.iter().map(|id| id.0).collect(),
                source: encode_hash_source(*source),
            }),
            RedistributeMode::Broadcast => Mode::Broadcast(true),
        }),
    }
}

fn encode_iceberg_metadata_table_type(
    src: &crate::connector::iceberg::IcebergMetadataTableType,
) -> i32 {
    match src {
        crate::connector::iceberg::IcebergMetadataTableType::Files => {
            plan::IcebergMetadataTableType::Files as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Manifests => {
            plan::IcebergMetadataTableType::Manifests as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata => {
            plan::IcebergMetadataTableType::LogicalIcebergMetadata as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Snapshots => {
            plan::IcebergMetadataTableType::Snapshots as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::History => {
            plan::IcebergMetadataTableType::History as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Refs => {
            plan::IcebergMetadataTableType::Refs as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Partitions => {
            plan::IcebergMetadataTableType::Partitions as i32
        }
    }
}

fn encode_iceberg_write_sink_mode(src: IcebergWriteSinkMode) -> i32 {
    match src {
        IcebergWriteSinkMode::Data => plan::IcebergWriteSinkMode::Data as i32,
        IcebergWriteSinkMode::RowLineageData => plan::IcebergWriteSinkMode::RowLineageData as i32,
        IcebergWriteSinkMode::PositionDeletes => plan::IcebergWriteSinkMode::PositionDeletes as i32,
        IcebergWriteSinkMode::DeletionVectors => plan::IcebergWriteSinkMode::DeletionVectors as i32,
        IcebergWriteSinkMode::EqualityDeletes => plan::IcebergWriteSinkMode::EqualityDeletes as i32,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    value as u64
}

fn usize_to_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("value {value} does not fit in u32"))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::proto::expr::expr;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::{
        DataPartition, IcebergChangeStreamBranchRoute, IcebergChangeStreamRouterSink,
        PhysicalPlanStats, PlannerConfidence,
    };

    #[test]
    fn change_stream_router_encoder_materializes_partition_exprs() {
        let plan = single_fragment_router_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let root = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == encoded.root_fragment_id)
            .expect("root fragment");
        let Some(plan::data_sink::Kind::IcebergChangeStreamRouter(router)) =
            root.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected Iceberg change-stream router sink");
        };
        let branch = router.branches.first().expect("router branch");
        assert_eq!(branch.output_partition_ordinals, vec![2]);
        let partition = branch
            .output_partition
            .as_ref()
            .expect("branch output partition");
        assert_eq!(partition.kind, plan::PartitionKind::Hash as i32);
        let [expr] = partition.exprs.as_slice() else {
            panic!("expected one materialized partition expr");
        };
        let Some(expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref() else {
            panic!("expected partition expr to be a column ref");
        };
        assert_eq!(column_ref.column_id, 3);
    }

    #[test]
    fn stream_sink_projection_and_receiver_schema_follow_edge_output_slots() {
        let plan = two_fragment_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![2, 1]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "delta"), (1, "old")]
        );
    }

    #[test]
    fn stream_sink_uses_source_slots_while_receiver_schema_uses_exchange_columns() {
        let plan = two_fragment_stream_plan_with_lowered_slots_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![10, 20]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(10, "employee_id"), (20, "name")]
        );
    }

    #[test]
    fn stream_edge_output_slots_use_root_slots_when_fragment_outputs_are_retagged() {
        let root_columns = encode_output_columns(&[
            output_column(3, "a", DataType::Int64),
            output_column(4, "b", DataType::Int64),
        ])
        .expect("encode root columns");
        let source_outputs = vec![
            output_column(5, "a", DataType::Int64),
            output_column(6, "b", DataType::Int64),
        ];

        let resolved = project_output_columns_for_edge(root_columns, &source_outputs, &[5, 6])
            .expect("resolve stream edge output columns");

        assert_eq!(
            resolved
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn stream_edge_output_slots_fall_back_to_root_when_requested_slots_are_stale_superset() {
        let root_columns = encode_output_columns(&[
            output_column(2, "l_partkey", DataType::Int64),
            output_column(4, "l_shipdate", DataType::Date32),
            output_column(1, "l_orderkey", DataType::Int64),
            output_column(3, "l_suppkey", DataType::Int64),
        ])
        .expect("encode root columns");
        let source_outputs = vec![
            output_column(1, "l_orderkey", DataType::Int64),
            output_column(2, "l_partkey", DataType::Int64),
            output_column(3, "l_suppkey", DataType::Int64),
            output_column(4, "l_shipdate", DataType::Date32),
            output_column(5, "col1", DataType::Utf8),
        ];

        let resolved =
            project_output_columns_for_edge(root_columns, &source_outputs, &[1, 2, 3, 4, 5])
                .expect("resolve stream edge output columns");

        assert_eq!(
            resolved
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![2, 4, 1, 3]
        );
    }

    #[test]
    fn stream_sink_projection_uses_unique_project_output_ids_for_duplicate_slots() {
        let mut source = duplicate_projection_fragment_for_test(DataSink::Noop);
        source.fragment_id = 1;
        source.root.fragment_id = 1;
        source.root.children[0].fragment_id = 1;

        let target_output_columns = source.output_columns.clone();
        let exchange = DistributedNode {
            node_id: 20,
            fragment_id: 0,
            tuple_ids: vec![20],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedPayload::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: 1,
                output_columns: target_output_columns.clone(),
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        };
        let target = PlanFragment {
            fragment_id: 0,
            root: DistributedNode {
                node_id: 21,
                fragment_id: 0,
                tuple_ids: vec![21],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: vec![exchange],
                stats: stats(),
                payload: DistributedPayload::Physical(PhysicalPlanKind::Sort(
                    crate::sql::planner::plan::PlanSortNode {
                        items: Vec::new(),
                        analytic_partition_by: Vec::new(),
                        output_columns: target_output_columns,
                        offset: None,
                        partition_limit: None,
                        topn_type: None,
                    },
                )),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1, 1],
            }],
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![1, 3]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = receiver.payload.as_ref()
        else {
            panic!("expected Sort root");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "c1"), (3, "c1")]
        );
        let Some(plan::plan_node::Kind::Sort(sort)) = physical.kind.as_ref() else {
            panic!("expected Sort root");
        };
        assert_eq!(
            sort.output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "c1"), (3, "c1")]
        );
        let receiver = receiver.children.first().expect("exchange child");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "c1"), (3, "c1")]
        );
    }

    #[test]
    fn stream_sink_allows_zero_column_values_source() {
        let plan = two_fragment_zero_column_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert!(sink.output_columns.is_empty());

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert!(exchange.output_columns.is_empty());
    }

    #[test]
    fn iceberg_delta_table_encoder_requires_mv_refresh_context() {
        use crate::sql::codegen::proto_encode::plan;

        let plan = iceberg_delta_distributed_plan_for_test();

        let err = plan::encode_distributed_plan_with_context(
            &plan,
            plan::NativePlanEncodeContext {
                mv_refresh_ctx: None,
            },
        )
        .expect_err("Iceberg delta native sidecar must require MV refresh context");

        assert!(err.contains("Iceberg delta scan native sidecar"), "{err}");
        assert!(err.contains("MV refresh context"), "{err}");
    }

    fn iceberg_delta_distributed_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(1, "order_id", DataType::Int64)];
        DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
                    build_runtime_filters: Vec::new(),
                    probe_runtime_filters: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedPayload::Physical(PhysicalPlanKind::Scan(
                        crate::sql::planner::plan::PlanScanNode {
                            database: "db".to_string(),
                            table: iceberg_delta_table_for_test(),
                            alias: None,
                            columns: output_columns.clone(),
                            predicates: Vec::new(),
                            required_columns: None,
                            variant_columns: Vec::new(),
                            mv_rewritten_from: None,
                        },
                    )),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        }
    }

    fn iceberg_delta_table_for_test() -> catalog::TableDef {
        catalog::TableDef {
            name: "orders".to_string(),
            columns: vec![catalog::ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: catalog::ScanSource::IcebergDeltaTable {
                table: catalog::IcebergTableInfo {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                    table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
                    current_snapshot_id: Some(2),
                    schema_id: 1,
                    location: "file:///warehouse/orders".to_string(),
                    schema: catalog::IcebergSchemaDef {
                        fields: vec![catalog::IcebergSchemaFieldDef {
                            field_id: 1,
                            name: "order_id".to_string(),
                            initial_default: None,
                            write_default: None,
                            initial_default_json: None,
                            write_default_json: None,
                            children: Vec::new(),
                        }],
                    },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                },
                from_snapshot_id: 1,
                to_snapshot_id: 2,
            },
        }
    }

    #[test]
    fn stream_sink_derives_generate_series_source_schema() {
        let plan = two_fragment_generate_series_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![7]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str(), column.nullable))
                .collect::<Vec<_>>(),
            vec![(7, "generate_series", false)]
        );
    }

    #[test]
    fn project_root_output_columns_allocate_unique_ids_for_duplicate_projection_items() {
        let child_columns = vec![
            output_column(1, "c1", DataType::Int64),
            output_column(2, "c2", DataType::Int64),
        ];
        let duplicate_project = DistributedNode {
            node_id: 30,
            fragment_id: 0,
            tuple_ids: vec![30],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![DistributedNode {
                node_id: 29,
                fragment_id: 0,
                tuple_ids: vec![29],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                    crate::sql::planner::plan::PlanValuesNode {
                        rows: Vec::new(),
                        columns: child_columns,
                    },
                )),
            }],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::Project(
                crate::sql::planner::plan::PlanProjectNode {
                    items: vec![
                        crate::sql::analysis::ProjectItem {
                            expr: crate::sql::analysis::TypedExpr {
                                kind: crate::sql::analysis::ExprKind::ColumnRef {
                                    column_id: ColumnId::new_for_test(1),
                                    qualifier: None,
                                    column: "c1".to_string(),
                                },
                                data_type: DataType::Int64,
                                nullable: false,
                            },
                            output_name: "c1".to_string(),
                            output_column_id: ColumnId::new_for_test(1),
                        },
                        crate::sql::analysis::ProjectItem {
                            expr: crate::sql::analysis::TypedExpr {
                                kind: crate::sql::analysis::ExprKind::ColumnRef {
                                    column_id: ColumnId::new_for_test(1),
                                    qualifier: None,
                                    column: "c1".to_string(),
                                },
                                data_type: DataType::Int64,
                                nullable: false,
                            },
                            output_name: "c1".to_string(),
                            output_column_id: ColumnId::new_for_test(1),
                        },
                    ],
                    output_qualifier: None,
                },
            )),
        };
        let encoded = encode_node(&duplicate_project).expect("encode node");

        let columns = encoded_fragment_root_output_columns(&plan::PlanFragment {
            fragment_id: 0,
            root: Some(encoded),
            data_partition: None,
            output_partition: None,
            sink: None,
            output_exprs: Vec::new(),
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        })
        .expect("root output columns");

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column_id, 1);
        assert_eq!(columns[1].column_id, 3);
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c1", "c1"]
        );
    }

    fn duplicate_projection_fragment_for_test(sink: DataSink) -> PlanFragment {
        let child_columns = vec![
            output_column(1, "c1", DataType::Int64),
            output_column(2, "c2", DataType::Int64),
        ];
        let duplicate_output = vec![
            output_column(1, "c1", DataType::Int64),
            output_column(1, "c1", DataType::Int64),
        ];
        let root = DistributedNode {
            node_id: 30,
            fragment_id: 0,
            tuple_ids: vec![30],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![DistributedNode {
                node_id: 29,
                fragment_id: 0,
                tuple_ids: vec![29],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                    crate::sql::planner::plan::PlanValuesNode {
                        rows: Vec::new(),
                        columns: child_columns,
                    },
                )),
            }],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::Project(
                crate::sql::planner::plan::PlanProjectNode {
                    items: duplicate_output
                        .iter()
                        .map(|column| crate::sql::analysis::ProjectItem {
                            expr: crate::sql::analysis::TypedExpr {
                                kind: crate::sql::analysis::ExprKind::ColumnRef {
                                    column_id: column.column_id,
                                    qualifier: None,
                                    column: column.name.clone(),
                                },
                                data_type: column.data_type.clone(),
                                nullable: column.nullable,
                            },
                            output_name: column.name.clone(),
                            output_column_id: column.column_id,
                        })
                        .collect(),
                    output_qualifier: None,
                },
            )),
        };
        PlanFragment {
            fragment_id: 0,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink,
            output_exprs: None,
            output_columns: duplicate_output,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    #[test]
    fn result_fragment_output_columns_follow_project_root_unique_ids() {
        let fragment = duplicate_projection_fragment_for_test(DataSink::Result);

        let encoded = encode_plan_fragment(&fragment).expect("encode result fragment");

        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c1", "c1"]
        );
    }

    #[test]
    fn noop_fragment_output_columns_follow_project_root_unique_ids() {
        let fragment = duplicate_projection_fragment_for_test(DataSink::Noop);

        let encoded = encode_plan_fragment(&fragment).expect("encode noop fragment");

        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c1", "c1"]
        );
    }

    #[test]
    fn sort_output_columns_follow_child_unique_ids_for_duplicate_projection_items() {
        let mut fragment = duplicate_projection_fragment_for_test(DataSink::Result);
        let sort_output_columns = fragment.output_columns.clone();
        let child = fragment.root;
        fragment.root = DistributedNode {
            node_id: 31,
            fragment_id: 0,
            tuple_ids: vec![31],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![child],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::Sort(
                crate::sql::planner::plan::PlanSortNode {
                    items: Vec::new(),
                    analytic_partition_by: Vec::new(),
                    output_columns: sort_output_columns,
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                },
            )),
        };

        let encoded = encode_plan_fragment(&fragment).expect("encode sort fragment");
        let root = encoded.root.as_ref().expect("encoded root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical sort root");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        let Some(plan::plan_node::Kind::Sort(sort)) = physical.kind.as_ref() else {
            panic!("expected sort root");
        };
        assert_eq!(
            sort.output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn topn_root_output_columns_follow_child_unique_ids_for_duplicate_projection_items() {
        let mut fragment = duplicate_projection_fragment_for_test(DataSink::Result);
        let child = fragment.root;
        fragment.root = DistributedNode {
            node_id: 32,
            fragment_id: 0,
            tuple_ids: vec![32],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![child],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::TopN(
                crate::sql::planner::plan::PhysicalTopNNode {
                    items: Vec::new(),
                    limit: Some(10),
                    offset: None,
                    phase: TopNPhase::Final,
                    is_split: false,
                },
            )),
        };

        let encoded = encode_plan_fragment(&fragment).expect("encode topn fragment");

        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            encoded_fragment_root_output_columns(&encoded)
                .expect("root output columns")
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn hash_join_root_output_columns_follow_join_schema() {
        let output_columns = vec![
            output_column(1, "l_k", DataType::Int64),
            output_column(2, "r_k", DataType::Int64),
        ];
        let join = DistributedNode {
            node_id: 40,
            fragment_id: 0,
            tuple_ids: vec![1, 2],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::HashJoin(Box::new(
                crate::sql::planner::plan::PhysicalHashJoinNode {
                    join_type: JoinKind::Inner,
                    eq_conditions: Vec::new(),
                    other_condition: None,
                    distribution: JoinDistribution::Unknown,
                    execution_mode: None,
                    build_runtime_filters: Vec::new(),
                    output_columns: output_columns.clone(),
                },
            ))),
        };

        let encoded = encode_node(&join).expect("encode hash join");

        assert_eq!(
            encoded_node_output_columns(&encoded)
                .expect("hash join output columns")
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "l_k"), (2, "r_k")]
        );
    }

    #[test]
    fn nest_loop_join_root_output_columns_follow_join_schema() {
        let output_columns = vec![
            output_column(1, "l_k", DataType::Int64),
            output_column(2, "r_k", DataType::Int64),
        ];
        let join = DistributedNode {
            node_id: 41,
            fragment_id: 0,
            tuple_ids: vec![1, 2],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::NestLoopJoin(
                crate::sql::planner::plan::PhysicalNestLoopJoinNode {
                    join_type: JoinKind::Inner,
                    condition: None,
                    output_columns: output_columns.clone(),
                },
            )),
        };

        let encoded = encode_node(&join).expect("encode nest loop join");

        assert_eq!(
            encoded_node_output_columns(&encoded)
                .expect("nest loop join output columns")
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "l_k"), (2, "r_k")]
        );
    }

    #[test]
    fn assert_one_row_root_output_columns_follow_child_schema() {
        let child_column = output_column(1, "only_row", DataType::Int64);
        let node = DistributedNode {
            node_id: 42,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![DistributedNode {
                node_id: 43,
                fragment_id: 0,
                tuple_ids: vec![1],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                    crate::sql::planner::plan::PlanValuesNode {
                        rows: Vec::new(),
                        columns: vec![child_column.clone()],
                    },
                )),
            }],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::AssertOneRow(
                crate::sql::planner::plan::PlanAssertOneRowNode::global_at_most_one("select 1"),
            )),
        };

        let encoded = encode_node(&node).expect("encode assert one row");

        assert_eq!(
            encoded_node_output_columns(&encoded)
                .expect("assert one row output columns")
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "only_row")]
        );
    }

    #[test]
    fn sort_root_output_columns_follow_child_schema_when_physical_output_is_stale() {
        let actual_columns = encode_output_columns(&[
            output_column(4, "l_shipdate", DataType::Date32),
            output_column(1, "l_orderkey", DataType::Int64),
        ])
        .expect("encode actual columns");
        let stale_columns = encode_output_columns(&[
            output_column(1, "l_orderkey", DataType::Int64),
            output_column(2, "l_partkey", DataType::Int64),
            output_column(3, "l_suppkey", DataType::Int64),
        ])
        .expect("encode stale columns");
        let node = plan::DistributedNode {
            node_id: 42,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![plan::DistributedNode {
                node_id: 41,
                fragment_id: 0,
                tuple_ids: vec![1],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                    output_columns: actual_columns.clone(),
                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                        rows: Vec::new(),
                        columns: actual_columns.clone(),
                    })),
                })),
            }],
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: stale_columns,
                kind: Some(plan::plan_node::Kind::Sort(plan::SortNode {
                    items: Vec::new(),
                    analytic_partition_by: Vec::new(),
                    output_columns: Vec::new(),
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                })),
            })),
        };

        let resolved = encoded_node_output_columns(&node).expect("sort output columns");

        assert_eq!(
            resolved
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![4, 1]
        );
    }

    #[test]
    fn stream_sink_projection_uses_join_child_schema_when_join_output_ids_are_stale() {
        let actual_left = output_column(1, "l_k", DataType::Int64);
        let actual_right = output_column(2, "r_k", DataType::Int64);
        let stale_source_columns = vec![
            output_column(10, "l_k", DataType::Int64),
            output_column(11, "r_k", DataType::Int64),
            output_column(999, "pruned", DataType::Int64),
        ];
        let receiver_columns = vec![
            output_column(84, "l_k", DataType::Int64),
            output_column(85, "r_k", DataType::Int64),
        ];
        let source = PlanFragment {
            fragment_id: 1,
            root: DistributedNode {
                node_id: 10,
                fragment_id: 1,
                tuple_ids: vec![10, 11],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: vec![
                    DistributedNode {
                        node_id: 11,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: Vec::new(),
                                columns: vec![actual_left.clone()],
                            },
                        )),
                    },
                    DistributedNode {
                        node_id: 12,
                        fragment_id: 1,
                        tuple_ids: vec![11],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: Vec::new(),
                                columns: vec![actual_right.clone()],
                            },
                        )),
                    },
                ],
                stats: stats(),
                payload: DistributedPayload::Physical(PhysicalPlanKind::HashJoin(Box::new(
                    crate::sql::planner::plan::PhysicalHashJoinNode {
                        join_type: JoinKind::Inner,
                        eq_conditions: Vec::new(),
                        other_condition: None,
                        distribution: JoinDistribution::Unknown,
                        execution_mode: None,
                        build_runtime_filters: Vec::new(),
                        output_columns: stale_source_columns.clone(),
                    },
                ))),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: stale_source_columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let target = PlanFragment {
            fragment_id: 0,
            root: DistributedNode {
                node_id: 20,
                fragment_id: 0,
                tuple_ids: vec![20],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedPayload::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: 1,
                    output_columns: receiver_columns,
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![84, 85, 999],
            }],
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![1, 2]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "l_k"), (2, "r_k")]
        );
    }

    #[test]
    fn scan_output_columns_drop_duplicate_column_ids() {
        let duplicate_scan_columns = vec![
            output_column(1, "c1", DataType::Int64),
            output_column(2, "c2", DataType::Int64),
            output_column(1, "c1", DataType::Int64),
        ];
        let duplicate_scan_columns =
            encode_output_columns(&duplicate_scan_columns).expect("encode output columns");
        let mut node = plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: vec![10],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: duplicate_scan_columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: None,
                    alias: None,
                    columns: duplicate_scan_columns,
                    predicates: Vec::new(),
                    required_columns: Vec::new(),
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        };

        normalize_encoded_node_output_columns(&mut node).expect("normalize scan");

        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        else {
            panic!("expected physical scan");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan");
        };
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn scan_boundary_output_columns_follow_required_columns_without_dropping_scan_columns() {
        let scan_columns = encode_output_columns(&[
            output_column(1, "l_orderkey", DataType::Int64),
            output_column(2, "l_partkey", DataType::Int64),
            output_column(3, "l_suppkey", DataType::Int64),
            output_column(4, "l_shipdate", DataType::Date32),
            output_column(5, "_row_id", DataType::Int64),
            output_column(6, "_last_updated_sequence_number", DataType::Int64),
        ])
        .expect("encode scan columns");
        let mut node = plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: vec![10],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: scan_columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: None,
                    alias: None,
                    columns: scan_columns.clone(),
                    predicates: Vec::new(),
                    required_columns: vec![
                        "l_orderkey".to_string(),
                        "l_partkey".to_string(),
                        "l_suppkey".to_string(),
                        "l_shipdate".to_string(),
                    ],
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        };

        normalize_encoded_node_output_columns(&mut node).expect("normalize scan");

        let resolved = encoded_node_output_columns(&node).expect("scan output columns");
        assert_eq!(
            resolved
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        else {
            panic!("expected physical scan");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan");
        };
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn set_op_child_output_columns_follow_child_unique_ids() {
        let child = duplicate_projection_fragment_for_test(DataSink::Noop).root;
        let duplicate_child_output_columns = vec![
            output_column(1, "c1", DataType::Int64),
            output_column(1, "c1", DataType::Int64),
        ];
        let set_op = DistributedNode {
            node_id: 50,
            fragment_id: 0,
            tuple_ids: vec![50],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![child.clone(), child],
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::SetOp(
                crate::sql::planner::plan::PhysicalSetOpNode {
                    kind: PlanSetOpKind::UnionAll,
                    output_columns: vec![
                        output_column(10, "c1", DataType::Int64),
                        output_column(11, "c1", DataType::Int64),
                    ],
                    child_output_columns: vec![
                        duplicate_child_output_columns.clone(),
                        duplicate_child_output_columns,
                    ],
                },
            )),
        };

        let encoded = encode_node(&set_op).expect("encode set op");

        let Some(plan::distributed_node::Payload::Physical(physical)) = encoded.payload.as_ref()
        else {
            panic!("expected physical set op");
        };
        let Some(plan::plan_node::Kind::SetOp(set_op)) = physical.kind.as_ref() else {
            panic!("expected set op");
        };
        assert_eq!(
            set_op
                .child_output_columns
                .iter()
                .map(|columns| columns
                    .columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![1, 3], vec![1, 3]]
        );
    }

    fn two_fragment_stream_plan_for_test() -> DistributedPlan {
        let source_columns = vec![
            output_column(1, "old", DataType::Int64),
            output_column(2, "delta", DataType::Int64),
        ];
        let receiver_columns = vec![source_columns[1].clone(), source_columns[0].clone()];
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: Vec::new(),
                                columns: source_columns.clone(),
                            },
                        )),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns,
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: receiver_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![2, 1],
            }],
        }
    }

    fn two_fragment_stream_plan_with_lowered_slots_for_test() -> DistributedPlan {
        let source_columns = vec![
            output_column(10, "employee_id", DataType::Int64),
            output_column(20, "name", DataType::Utf8),
            output_column(30, "title", DataType::Utf8),
        ];
        let receiver_columns = source_columns[..2].to_vec();
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: Vec::new(),
                                columns: source_columns.clone(),
                            },
                        )),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns,
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: receiver_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![43, 44],
            }],
        }
    }

    fn two_fragment_zero_column_stream_plan_for_test() -> DistributedPlan {
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: vec![Vec::new()],
                                columns: Vec::new(),
                            },
                        )),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: Vec::new(),
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: Vec::new(),
            }],
        }
    }

    fn two_fragment_generate_series_stream_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(7, "generate_series", DataType::Int64)];
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Physical(PhysicalPlanKind::GenerateSeries(
                            crate::sql::planner::plan::PlanGenerateSeriesNode {
                                start: 1,
                                end: 3,
                                step: 1,
                                column_name: "generate_series".to_string(),
                                alias: None,
                                output_column_id: ColumnId::new_for_test(7),
                            },
                        )),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        build_runtime_filters: Vec::new(),
                        probe_runtime_filters: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedPayload::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![7],
            }],
        }
    }

    fn single_fragment_router_plan_for_test() -> DistributedPlan {
        let output_columns = vec![
            output_column(1, "op", DataType::Int32),
            output_column(2, "route", DataType::Int32),
            output_column(3, "bucket", DataType::Int32),
        ];
        DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
                    build_runtime_filters: Vec::new(),
                    probe_runtime_filters: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedPayload::Physical(PhysicalPlanKind::Values(
                        crate::sql::planner::plan::PlanValuesNode {
                            rows: Vec::new(),
                            columns: output_columns.clone(),
                        },
                    )),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::IcebergChangeStreamRouter(IcebergChangeStreamRouterSink {
                    group_id: 0,
                    change_op_output_ordinal: 0,
                    data_route_output_ordinal: Some(1),
                    branches: vec![IcebergChangeStreamBranchRoute {
                        branch_id: 0,
                        branch_kind: ChangeStreamBranchKind::DeleteDv,
                        target_fragment_id: 1,
                        target_exchange_node_id: 20,
                        output_ordinals: vec![2],
                        output_partition_ordinals: vec![2],
                    }],
                }),
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        }
    }

    fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
