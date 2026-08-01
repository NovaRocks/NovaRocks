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

use std::collections::HashMap;
use std::sync::Arc;

use super::super::context::NativePlanDecodeContext;
use super::super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::super::node::DecodedNode;
use super::common::{
    DecodedScanOutputColumns, lower_scan_predicate, parse_scan_limit, scan_batch_size,
};
use super::native_starrocks::decode_starrocks_scan_preparation;
use novarocks::connector::StarRocksScanConfig;
use novarocks::connector::starrocks::plan_native_starrocks_read_source_with_cancellation;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::protocol::ProtocolErrorKind;
use novarocks_protocol::plan;

pub(super) fn validate_starrocks_output_columns(
    output_columns: &DecodedScanOutputColumns,
    source: &plan::StarRocksTableSource,
) -> Result<(), NativeFragmentDecodeError> {
    let storage_names = source
        .storage_columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for (index, column) in output_columns.columns().iter().enumerate() {
        if !storage_names.contains(&column.name.to_ascii_lowercase()) {
            return Err(NativeFragmentDecodeError::inconsistent(
                output_columns.source_path(index).field("name"),
                format!(
                    "StarRocks native scan column {} is missing storage metadata",
                    column.name
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn lower_starrocks_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::StarRocksTableSource,
    output_columns: &DecodedScanOutputColumns,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
        let prepared = decode_starrocks_scan_preparation(
            node.node_id,
            scan,
            source,
            ctx.query_id(),
            ctx.scan_ranges(node.node_id)?,
        )?;
        let layout = output_columns.layout();
        let output_schema = starrocks_chunk_schema(output_columns, source)?;
        let limit = parse_scan_limit(node.limit)?;
        let batch_size = i32::try_from(scan_batch_size(ctx.query_options())?).map_err(|_| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "batch_size",
                format!(
                    "StarRocks ScanNode node_id={} batch_size exceeds i32",
                    node.node_id
                ),
            )
        })?;
        let query_timeout = positive_query_option(
            node.node_id,
            "query_timeout",
            ctx.query_options()
                .and_then(|options| options.query_timeout()),
        )?;
        let mem_limit = positive_query_option(
            node.node_id,
            "exec_mem_limit",
            ctx.query_options()
                .and_then(|options| options.exec_mem_limit()),
        )?;
        let predicate = lower_scan_predicate(scan, arena, &layout, ctx)?;
        let min_max_predicates = predicate
            .map(|root| {
                crate::native::expression::extract_min_max_predicates(
                    arena,
                    root,
                    output_schema.as_ref(),
                )
            })
            .unwrap_or_default();
        let cfg = StarRocksScanConfig {
            db_name: Some(scan.database.clone()),
            table_name: scan.table.as_ref().map(|table| table.name.clone()),
            properties: prepared.properties,
            ranges: prepared.ranges,
            has_more: false,
            required_chunk_schema: Arc::clone(&output_schema),
            output_chunk_schema: Arc::clone(&output_schema),
            query_global_dicts: Default::default(),
            limit,
            batch_size: Some(batch_size),
            query_timeout,
            mem_limit,
            profile_label: Some(format!("starrocks_scan_node_id={}", node.node_id)),
            min_max_predicates,
            lake_schema_meta: Some(prepared.lake_schema_meta),
            deferred_lake_resolution: Some(prepared.deferred_lake_resolution),
            topn_filter_column_map: HashMap::new(),
        };
        let query_options = ctx.query_options().cloned().unwrap_or_default();
        let source = plan_native_starrocks_read_source_with_cancellation(
            ctx.query_id(),
            node.node_id,
            cfg,
            &query_options,
            ctx.connector_cancellation()?,
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "source",
                error,
            )
        })?;
        ctx.capture_scan_ranges(
            node.node_id,
            novarocks::exec::node::scan::BoundScanRanges::None,
        );
        let scan_node = novarocks::exec::node::scan::ScanNode::new(source)
            .with_node_id(node.node_id)
            .with_output_chunk_schema(Arc::clone(&output_schema))
            .with_limit(limit)
            .with_conjunct_predicate(predicate)
            .with_connector_io_tasks_per_scan_operator(
                ctx.query_options()
                    .and_then(|options| options.connector_io_tasks_per_scan_operator()),
            )
            .with_accept_empty_scan_ranges(true);
        Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Scan(scan_node),
            },
            layout,
            output_schema,
        })
    })();
    decoded
}

fn positive_query_option<T>(
    node_id: i32,
    field: &'static str,
    value: Option<T>,
) -> Result<Option<T>, NativeFragmentLeafDecodeError>
where
    T: Copy + PartialOrd + From<i32> + std::fmt::Display,
{
    if let Some(value) = value
        && value <= T::from(0)
    {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            field,
            format!(
                "StarRocks ScanNode node_id={node_id} query option {field} must be positive, got {value}"
            ),
        ));
    }
    Ok(value)
}

fn starrocks_chunk_schema(
    columns: &DecodedScanOutputColumns,
    source: &plan::StarRocksTableSource,
) -> Result<ChunkSchemaRef, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<ChunkSchemaRef, NativeFragmentLeafDecodeError> {
        let storage_by_name = source
            .storage_columns
            .iter()
            .map(|column| (column.name.to_ascii_lowercase(), column))
            .collect::<HashMap<_, _>>();
        let decoded_schema = columns.output_schema();
        let slots = columns
            .columns()
            .iter()
            .zip(decoded_schema.slots())
            .map(|(column, slot)| {
                let storage = storage_by_name
                    .get(&column.name.to_ascii_lowercase())
                    .ok_or_else(|| {
                        NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::MissingField,
                            "storage_columns",
                            format!(
                                "StarRocks native scan output column {} is missing storage metadata",
                                column.name
                            ),
                        )
                    })?;
                ChunkSlotSchema::try_new_with_field(
                    slot.slot_id(),
                    slot.field().clone(),
                    Some(slot.field_schema().clone()),
                    Some(storage.unique_id),
                )
                .map_err(|error| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        "storage_columns",
                        error,
                    )
                })
            })
            .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()?;
        ChunkSchema::try_new(slots).map(Arc::new).map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "storage_columns",
                error,
            )
        })
    })();
    decoded
}
