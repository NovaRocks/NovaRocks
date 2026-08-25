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
use novarocks_sql::plan_read::OutputColumn;

pub(super) fn encode_fragment_output_contract(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<(Vec<novarocks_proto::expr::Expr>, Vec<common::OutputColumn>), String> {
    if matches!(&src.sink, DataSink::ConnectorWrite(sink) if sink.has_output_contract()) {
        // The planner finalized the connector writer output expressions and target output
        // schema at seal (CGO-9C Task 3). The encoder maps them 1:1 instead of
        // synthesizing the target schema or falling back to the fragment's output
        // columns / input binding.
        let contract = required_context_ref(ctx.write_contracts, || {
            format!(
                "native connector write fragment {} has no sealed write contract",
                src.fragment_id
            )
        })?
        .connector_write_output(src.fragment_id)
        .ok_or_else(|| {
            format!(
                "native connector write fragment {} is missing from the sealed write contract",
                src.fragment_id
            )
        })?;
        let output_exprs = encode_exprs(&contract.output_exprs)?;
        let output_columns = contract
            .target_schema
            .iter()
            .map(|column| {
                Ok(common::OutputColumn {
                    column_id: column.column_id,
                    name: column.name.clone(),
                    r#type: Some(encode_type(&column.data_type)?),
                    nullable: column.nullable,
                    is_internal: column.is_internal,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        return Ok((output_exprs, output_columns));
    }

    let output_exprs = src
        .output_exprs
        .as_ref()
        .map(|exprs| encode_exprs(exprs))
        .transpose()?
        .unwrap_or_default();
    let output_columns = encode_finalized_fragment_output_columns(src, ctx)?;
    Ok((output_exprs, output_columns))
}

/// Map a fragment's finalized output columns from the sealed fragment/edge
/// contract. The planner already reconciled the fragment's declared output with
/// its root's execution output (unique wire ids for re-materialized projections,
/// producer fragments forwarding their root wholesale); the encoder maps the
/// result 1:1 instead of re-walking the encoded tree or falling back.
fn encode_finalized_fragment_output_columns(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<Vec<common::OutputColumn>, String> {
    let catalog = required_context_ref(ctx.fragment_edge_outputs, || {
        format!(
            "native fragment {} has no sealed output contract",
            src.fragment_id
        )
    })?;
    let columns = catalog
        .fragment_output_columns(src.fragment_id)
        .ok_or_else(|| {
            format!(
                "native fragment {} is missing from the sealed output contract",
                src.fragment_id
            )
        })?;
    encode_output_columns(columns)
}

/// Bind the encoded node's execution output columns from the sealed node-output
/// contract for the covered kinds (join / scan / set-op / sort / hash-aggregate).
/// The planner has already finalized and validated those outputs at seal time, so
/// the encoder maps them 1:1 here rather than re-deriving or repairing them.
///
/// A `HashAggregate` additionally carries a finalized group-key + aggregate-state
/// wire layout (with per-mode intermediate types applied); this maps that layout —
/// and the visible-or-full output columns — into the `HashAggregateNode` payload,
/// replacing the raw baseline `encode_physical_node` produced. The intermediate
/// aggregate-state type determination lives entirely in the planner.
///
/// `ctx.node_outputs` is `None` only in the bare-node encoder unit tests, which
/// have no sealed plan; there the payload columns encoded by `encode_physical_node`
/// already stand (the same data the catalog is built from).
pub(super) fn apply_sealed_node_output_columns(
    node: &mut plan::DistributedNode,
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<(), String> {
    let Some(catalog) = optional_context_ref(ctx.node_outputs) else {
        return Ok(());
    };
    let Some(output) = catalog.output_for(src.fragment_id, src.node_id) else {
        // Not a covered kind; its output columns come straight from encoding.
        return Ok(());
    };
    let output_columns = output
        .columns
        .iter()
        .map(encode_node_execution_column)
        .collect::<Result<Vec<_>, _>>()?;
    let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut() else {
        return Err(format!(
            "native node {} carries a sealed execution output but is not a physical payload",
            src.node_id
        ));
    };
    physical.output_columns = output_columns;

    // A HashAggregate maps its finalized wire layout (and visible output columns)
    // 1:1 from the contract, overriding the raw baseline.
    if matches!(physical.kind, Some(plan::plan_node::Kind::HashAggregate(_))) {
        let layout = catalog
            .aggregate_layout(src.fragment_id, src.node_id)
            .ok_or_else(|| {
                format!(
                    "native HashAggregate node {} has a covered execution output but no sealed wire layout",
                    src.node_id
                )
            })?;
        let group_key_columns = encode_output_columns(&layout.group_key_columns)?;
        let aggregate_columns = encode_output_columns(&layout.aggregate_columns)?;
        let visible_output_columns = physical.output_columns.clone();
        let Some(plan::plan_node::Kind::HashAggregate(aggregate)) = physical.kind.as_mut() else {
            unreachable!("physical.kind was just matched as HashAggregate");
        };
        aggregate.output_layout = Some(plan::AggregateOutputLayout {
            group_key_columns,
            aggregate_columns,
        });
        aggregate.output_columns = visible_output_columns;
    }
    Ok(())
}

fn encode_node_execution_column(
    column: &NodeExecutionColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.column_id.0,
        name: column.name.clone(),
        r#type: Some(encode_type(&column.data_type)?),
        nullable: column.nullable,
        is_internal: column.is_internal,
    })
}

pub(super) fn encode_output_columns(
    src: &[OutputColumn],
) -> Result<Vec<common::OutputColumn>, String> {
    src.iter().map(encode_output_column).collect()
}

pub(super) fn encode_output_column(src: &OutputColumn) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: src.column_id.0,
        name: src.name.clone(),
        r#type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        is_internal: src.is_internal,
    })
}
