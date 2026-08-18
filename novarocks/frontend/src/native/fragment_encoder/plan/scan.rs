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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use super::super::expr::encode_sort_items;
use super::output::{encode_output_column, encode_output_columns};
use super::type_mapping::encode_type;
use super::type_mapping::{encode_edge_partition_type, encode_sql_type};
use super::{NativePlanEncodeContext, encode_exprs};
use crate::query_execution::preparation::{
    NativeScanBindingView, NativeScanColumnKind, NativeScanColumnView, NativeScanExecutionKind,
};
use novarocks_protocol::{common, plan};
use novarocks_sql::plan_read::{
    ColumnId, ExchangeFlavor, ExchangeReceiver, OutputColumn as AnalysisOutputColumn,
    ScanVariantColumn, SqlPlanScanNodeRead, SqlScanSourceRead, SqlTableDefRead,
};

pub(super) fn encode_scan_node(
    src: &SqlPlanScanNodeRead,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanNode, String> {
    let binding = scan_binding_for_source(node_id, &src.table.source, ctx)?;
    let columns = match binding {
        Some(binding) => encode_bound_scan_output_columns(src, binding)?,
        None => encode_output_columns(&src.columns)?,
    };
    let required_columns = binding.map_or_else(
        || src.required_columns.clone().unwrap_or_default(),
        |binding| encode_bound_required_columns(src, binding),
    );
    // Connector planning may remove only predicates explicitly negotiated as
    // Exact. All other scan sources, and PruningOnly/Unsupported connector
    // predicates, retain the original Core residuals.
    let residual_predicates = ctx
        .scan_facts
        .and_then(|facts| facts.connector_read_for_node(node_id))
        .map(|planned| planned.residual_predicates())
        .unwrap_or(&src.predicates);
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(
            &src.table,
            Some(node_id),
            Some(&src.columns),
            Some(&columns),
            Some(&required_columns),
            Some(&src.variant_columns),
            binding,
            ctx,
        )?),
        alias: src.alias.clone(),
        columns,
        predicates: encode_exprs(residual_predicates)?,
        required_columns,
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

fn encode_bound_scan_output_columns(
    src: &SqlPlanScanNodeRead,
    binding: NativeScanBindingView<'_>,
) -> Result<Vec<common::OutputColumn>, String> {
    let physical_by_planner_id = binding
        .physical_columns()
        .map(|column| (column.planner().column_id, column))
        .collect::<HashMap<_, _>>();
    let synthetic_ids = src
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let mut encoded = Vec::with_capacity(src.columns.len());
    let mut seen_physical_ids = HashSet::new();
    for column in &src.columns {
        if let Some(bound) = physical_by_planner_id.get(&column.column_id) {
            encoded.push(encode_bound_scan_output_column(*bound)?);
            seen_physical_ids.insert(column.column_id);
        } else if synthetic_ids.contains(&column.column_id) {
            encoded.push(encode_output_column(column)?);
        }
    }
    for bound in binding.physical_columns() {
        if seen_physical_ids.insert(bound.planner().column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
        }
    }
    Ok(encoded)
}

fn encode_bound_required_columns(
    src: &SqlPlanScanNodeRead,
    binding: NativeScanBindingView<'_>,
) -> Vec<String> {
    let mut required = binding
        .required_reads()
        .map(|read| read.source().name.clone())
        .collect::<Vec<_>>();
    for variant in &src.variant_columns {
        let required_by_planner = src.required_columns.as_ref().is_none_or(|columns| {
            columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        });
        if required_by_planner
            && !required
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        {
            required.push(variant.synthetic_column.clone());
        }
    }
    required
}

fn encode_bound_scan_output_column(
    column: NativeScanColumnView<'_>,
) -> Result<common::OutputColumn, String> {
    let source = column.source();
    let planner = column.planner();
    let data_type = match source.logical_type.as_ref() {
        Some(logical_type) => encode_sql_type(logical_type)?,
        None => encode_type(&source.data_type)?,
    };
    Ok(common::OutputColumn {
        column_id: planner.column_id.0,
        name: source.name.clone(),
        r#type: Some(data_type),
        nullable: source.nullable,
        is_internal: planner.is_internal,
    })
}

/// Encode an exchange receiver. `output_columns` is the receiver's finalized
/// wire schema: for a stream-edge target it is the planner's reconciled edge
/// projection (kept equal to what the sender sends); otherwise it is the
/// receiver's own declared columns.
pub(super) fn encode_exchange_receiver(
    src: &ExchangeReceiver,
    output_columns: &[AnalysisOutputColumn],
) -> Result<plan::ExchangeReceiver, String> {
    Ok(plan::ExchangeReceiver {
        partition_type: encode_edge_partition_type(&src.partition),
        partition_exprs: encode_exprs(&src.partition.exprs)?,
        source_fragment_id: src.source_fragment_id,
        output_columns: encode_output_columns(output_columns)?,
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

pub(super) fn encode_table_def_with_context(
    src: &SqlTableDefRead,
    scan_node_id: Option<i32>,
    scan_columns: Option<&[AnalysisOutputColumn]>,
    scan_output_columns: Option<&[common::OutputColumn]>,
    scan_required_columns: Option<&[String]>,
    scan_variant_columns: Option<&[ScanVariantColumn]>,
    binding: Option<NativeScanBindingView<'_>>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::TableDef, String> {
    let (columns, metadata_columns) = match binding {
        Some(binding) if scan_source_requires_resolved_binding(&src.source) => {
            resolved_binding_table_columns(binding)
        }
        Some(binding) => merged_bound_table_columns(src, scan_columns.unwrap_or_default(), binding),
        None => (
            src.columns.clone(),
            src.iceberg_row_lineage_metadata_columns.clone(),
        ),
    };
    Ok(plan::TableDef {
        name: src.name.clone(),
        columns: columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        iceberg_row_lineage_metadata_columns: metadata_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        source: Some(encode_scan_source(
            &src.source,
            scan_node_id,
            scan_columns,
            scan_output_columns,
            scan_required_columns,
            scan_variant_columns.unwrap_or_default(),
            binding,
            ctx,
        )?),
    })
}

fn scan_source_requires_resolved_binding(_: &SqlScanSourceRead) -> bool {
    true
}

fn resolved_binding_table_columns(
    binding: NativeScanBindingView<'_>,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut metadata_columns = Vec::new();
    let mut seen = HashSet::new();

    for bound in binding.physical_columns() {
        if !seen.insert(bound.source().name.to_ascii_lowercase()) {
            continue;
        }
        match bound.kind() {
            NativeScanColumnKind::PhysicalTable => columns.push(bound.source().clone()),
            NativeScanColumnKind::IcebergMetadata => metadata_columns.push(bound.source().clone()),
        }
    }
    for read in binding.required_reads() {
        if seen.insert(read.source().name.to_ascii_lowercase()) {
            columns.push(read.source().clone());
        }
    }

    (columns, metadata_columns)
}

fn merged_bound_table_columns(
    src: &SqlTableDefRead,
    scan_columns: &[AnalysisOutputColumn],
    binding: NativeScanBindingView<'_>,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = src.columns.clone();
    let mut metadata_columns = src.iceberg_row_lineage_metadata_columns.clone();
    for bound in binding.physical_columns() {
        let target = match bound.kind() {
            NativeScanColumnKind::PhysicalTable => &mut columns,
            NativeScanColumnKind::IcebergMetadata => &mut metadata_columns,
        };
        let planner_source_name = scan_columns
            .iter()
            .find(|column| column.column_id == bound.planner().column_id)
            .map(|column| column.name.as_str());
        overlay_bound_column(
            target,
            &bound.planner().name,
            planner_source_name,
            bound.source(),
        );
    }
    for read in binding.required_reads() {
        if replace_column_by_name(&mut columns, read.source())
            || replace_column_by_name(&mut metadata_columns, read.source())
        {
            continue;
        }
        columns.push(read.source().clone());
    }
    (columns, metadata_columns)
}

fn overlay_bound_column(
    columns: &mut Vec<novarocks_catalog::schema::ColumnDef>,
    planner_name: &str,
    planner_source_name: Option<&str>,
    source: &novarocks_catalog::schema::ColumnDef,
) {
    if let Some(index) = columns.iter().position(|column| {
        column.name.eq_ignore_ascii_case(planner_name)
            || planner_source_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name))
            || column.name.eq_ignore_ascii_case(&source.name)
    }) {
        columns[index] = source.clone();
    } else {
        columns.push(source.clone());
    }
}

fn replace_column_by_name(
    columns: &mut [novarocks_catalog::schema::ColumnDef],
    source: &novarocks_catalog::schema::ColumnDef,
) -> bool {
    let Some(column) = columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(&source.name))
    else {
        return false;
    };
    *column = source.clone();
    true
}

pub(super) fn encode_column_def(
    src: &novarocks_catalog::schema::ColumnDef,
) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        // Deprecated wire field: no decoder consumes `write_default_json`, so
        // the native encoder never fills it. Write defaults reach execution
        // through the connector-owned provider schema instead.
        write_default_json: None,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn scan_binding_for_source<'a>(
    node_id: i32,
    source: &SqlScanSourceRead,
    ctx: &'a NativePlanEncodeContext<'_>,
) -> Result<Option<NativeScanBindingView<'a>>, String> {
    let binding = ctx.scan_facts.and_then(|facts| facts.binding(node_id));
    let required = scan_source_requires_resolved_binding(source);
    if required && binding.is_none() {
        return Err(match source {
            SqlScanSourceRead::Delta {
                from_snapshot_id,
                to_snapshot_id,
            } => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={} from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                scan_source_kind(source)
            ),
            _ => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={}",
                scan_source_kind(source)
            ),
        });
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    if binding.node_id() != node_id {
        return Err(format!(
            "native scan encoder binding node mismatch: requested node_id={node_id}, binding node_id={}",
            binding.node_id()
        ));
    }
    let valid_execution = match source {
        SqlScanSourceRead::ConnectorRead => {
            matches!(binding.execution(), NativeScanExecutionKind::ConnectorRead)
        }
        SqlScanSourceRead::Delta { .. } => {
            matches!(
                binding.execution(),
                NativeScanExecutionKind::SealedConnectorScan
            )
        }
        SqlScanSourceRead::Data
        | SqlScanSourceRead::FrozenInputSet
        | SqlScanSourceRead::MvTargetState
        | SqlScanSourceRead::MvTargetLocator
        | SqlScanSourceRead::Metadata => {
            matches!(
                binding.execution(),
                NativeScanExecutionKind::AdmittedConnectorRead
            )
        }
    };
    if !valid_execution {
        return Err(format!(
            "native scan encoder execution variant mismatch for node_id={node_id} source={}: binding={}",
            scan_source_kind(source),
            resolved_execution_kind(binding.execution())
        ));
    }
    Ok(Some(binding))
}

fn scan_source_kind(source: &SqlScanSourceRead) -> &'static str {
    match source {
        SqlScanSourceRead::ConnectorRead => "SqlConnectorRead",
        SqlScanSourceRead::Data => "SqlData",
        SqlScanSourceRead::FrozenInputSet => "SqlFrozenInputSet",
        SqlScanSourceRead::Metadata => "SqlMetadata",
        SqlScanSourceRead::Delta { .. } => "SqlDelta",
        SqlScanSourceRead::MvTargetState => "SqlMvTargetState",
        SqlScanSourceRead::MvTargetLocator => "SqlMvTargetLocator",
    }
}

fn resolved_execution_kind(execution: NativeScanExecutionKind) -> &'static str {
    match execution {
        NativeScanExecutionKind::ConnectorRead => "ConnectorRead",
        NativeScanExecutionKind::AdmittedConnectorRead => "AdmittedConnectorRead",
        NativeScanExecutionKind::SealedConnectorScan => "SealedConnectorScan",
    }
}

fn encode_scan_source(
    src: &SqlScanSourceRead,
    scan_node_id: Option<i32>,
    scan_analysis_columns: Option<&[AnalysisOutputColumn]>,
    scan_output_columns: Option<&[common::OutputColumn]>,
    scan_required_columns: Option<&[String]>,
    scan_variant_columns: &[ScanVariantColumn],
    binding: Option<NativeScanBindingView<'_>>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    if let Some(planned) = scan_node_id.and_then(|node_id| {
        ctx.scan_facts
            .and_then(|facts| facts.connector_read_for_node(node_id))
    }) {
        return Ok(plan::ScanSource {
            kind: Some(Kind::ConnectorRead(plan::ConnectorReadSource {
                instance_id: planned
                    .declaration()
                    .descriptor()
                    .instance_id
                    .as_str()
                    .to_string(),
                instance_incarnation: planned.declaration().incarnation().to_bytes().to_vec(),
                scan_payload: planned.scan().handle().payload().to_vec(),
                splits: Vec::new(),
                max_batch_rows: u64::try_from(planned.batch().max_rows.get())
                    .map_err(|_| "connector batch row budget does not fit u64".to_string())?,
                max_batch_bytes: u64::try_from(planned.batch().max_bytes.get())
                    .map_err(|_| "connector batch byte budget does not fit u64".to_string())?,
                max_handle_payload_bytes: u64::try_from(
                    novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                )
                .map_err(|_| "connector handle payload budget does not fit u64".to_string())?,
                max_total_payload_bytes: u64::try_from(
                    novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                )
                .map_err(|_| "connector total payload budget does not fit u64".to_string())?,
                expected_schema_ipc: encode_connector_expected_schema_ipc(
                    scan_output_columns.unwrap_or_default(),
                    scan_analysis_columns.unwrap_or_default(),
                    scan_required_columns.unwrap_or_default(),
                    scan_variant_columns,
                    binding,
                    Some(planned.scan().output_schema()),
                )?,
            })),
        });
    }

    let source_kind = scan_source_kind(src);
    Err(format!(
        "native SQL scan node_id={} source={} must be materialized as ConnectorReadSource before encoding",
        scan_node_id
            .map(|node_id| node_id.to_string())
            .unwrap_or_else(|| "<none>".to_string()),
        source_kind,
    ))
}

fn encode_connector_expected_schema_ipc(
    output_columns: &[common::OutputColumn],
    analysis_columns: &[AnalysisOutputColumn],
    required_columns: &[String],
    variant_columns: &[ScanVariantColumn],
    binding: Option<NativeScanBindingView<'_>>,
    provider_schema: Option<&arrow::datatypes::SchemaRef>,
) -> Result<Vec<u8>, String> {
    let required = (!required_columns.is_empty()).then(|| {
        required_columns
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    });
    let synthetic_ids = variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let required_variant_source_ids = variant_columns
        .iter()
        .filter(|column| {
            required.as_ref().is_none_or(|required| {
                required.contains(&column.synthetic_column.to_ascii_lowercase())
            })
        })
        .map(|column| column.source_column_id)
        .collect::<HashSet<_>>();
    let selected = output_columns
        .iter()
        .filter(|column| {
            !synthetic_ids.contains(&ColumnId(column.column_id))
                && (required
                    .as_ref()
                    .is_none_or(|required| required.contains(&column.name.to_ascii_lowercase()))
                    || required_variant_source_ids.contains(&ColumnId(column.column_id)))
        })
        .map(|column| {
            let domain_column = binding
                .and_then(|binding| {
                    binding
                        .physical_columns()
                        .find(|bound| bound.planner().column_id.0 == column.column_id)
                        .map(|bound| (bound.source().data_type.clone(), bound.source().nullable))
                })
                .or_else(|| {
                    analysis_columns
                        .iter()
                        .find(|candidate| candidate.column_id.0 == column.column_id)
                        .map(|candidate| (candidate.data_type.clone(), candidate.nullable))
                })
                .ok_or_else(|| {
                    format!(
                        "ConnectorReadSource output column {} is missing its domain type",
                        column.column_id
                    )
                })?;
            Ok::<Field, String>(Field::new(&column.name, domain_column.0, domain_column.1))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let selected_schema = Schema::new(selected);
    let schema = if let Some(provider_schema) = provider_schema {
        // The read provider owns field metadata such as Iceberg field IDs and
        // initial defaults. A physical scan can also carry execution-only
        // columns (for example DML equality keys), so retain provider fields
        // only where the native output actually consumes the same field.
        Schema::new(
            selected_schema
                .fields()
                .iter()
                .map(|selected| {
                    provider_schema
                        .fields()
                        .iter()
                        .find(|provider| {
                            provider.name() == selected.name()
                                && provider.is_nullable() == selected.is_nullable()
                                && provider.data_type() == selected.data_type()
                        })
                        .cloned()
                        .unwrap_or_else(|| selected.clone())
                })
                .collect::<Vec<_>>(),
        )
    } else {
        selected_schema
    };
    let mut writer = StreamWriter::try_new(Vec::new(), &schema)
        .map_err(|error| format!("encode ConnectorReadSource expected schema: {error}"))?;
    writer
        .finish()
        .map_err(|error| format!("finish ConnectorReadSource expected schema: {error}"))?;
    let bytes = writer.get_ref().clone();
    if bytes.len() > novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(format!(
            "ConnectorReadSource expected schema exceeds {} bytes",
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        ));
    }
    Ok(bytes)
}
