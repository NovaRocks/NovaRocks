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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::super::expr::encode_sort_items;
use super::output::{encode_output_column, encode_output_columns};
use super::type_mapping::{
    encode_edge_partition_type, encode_iceberg_metadata_table_type, encode_sql_type,
};
use super::{NativePlanEncodeContext, encode_exprs, optional_context_ref};
use crate::connector::iceberg::scan_model as iceberg_scan_model;
use crate::connector::scan_model::starrocks::{
    StarRocksColumnSchemaDescriptor, StarRocksKeysTypeDescriptor, StarRocksTabletSchemaDescriptor,
};
use crate::coordinator::prepare::scan::{
    ResolvedScanBinding, ResolvedScanColumnKind, ResolvedScanExecution,
};
use crate::proto::{common, plan};
use crate::protocol::native::type_mapping::encode_type;
use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
use crate::sql::planner::distributed::{ExchangeFlavor, ExchangeReceiver};
use crate::sql::planner::table as table_model;
use novarocks_catalog::schema::{ColumnDefault, validate_column_default};

pub(super) fn encode_scan_node(
    src: &crate::sql::planner::payload::PlanScanNode,
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
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(
            &src.table,
            Some(node_id),
            Some(&src.columns),
            binding,
            ctx,
        )?),
        alias: src.alias.clone(),
        columns,
        predicates: encode_exprs(&src.predicates)?,
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
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Result<Vec<common::OutputColumn>, String> {
    let physical_by_planner_id = binding
        .physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
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
            encoded.push(encode_bound_scan_output_column(bound)?);
            seen_physical_ids.insert(column.column_id);
        } else if synthetic_ids.contains(&column.column_id) {
            encoded.push(encode_output_column(column)?);
        }
    }
    for bound in &binding.physical_columns {
        if seen_physical_ids.insert(bound.planner.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
        }
    }
    Ok(encoded)
}

fn encode_bound_required_columns(
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Vec<String> {
    let mut required = binding
        .required_reads
        .iter()
        .map(|read| read.source.name.clone())
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
    column: &crate::coordinator::prepare::scan::ResolvedScanColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.planner.column_id.0,
        name: column.source.name.clone(),
        r#type: Some(encode_type(&column.source.data_type)?),
        nullable: column.source.nullable,
        is_internal: column.planner.is_internal,
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
    src: &table_model::TableDef,
    scan_node_id: Option<i32>,
    scan_columns: Option<&[AnalysisOutputColumn]>,
    binding: Option<&ResolvedScanBinding>,
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
        source: Some(encode_scan_source(&src.source, scan_node_id, binding, ctx)?),
    })
}

fn scan_source_requires_resolved_binding(source: &table_model::ScanSource) -> bool {
    matches!(
        source,
        table_model::ScanSource::IcebergDeltaTable { .. }
            | table_model::ScanSource::IcebergVersionTable { .. }
            | table_model::ScanSource::IcebergMvTargetState(_)
            | table_model::ScanSource::IcebergMvTargetLocator(_)
    )
}

fn resolved_binding_table_columns(
    binding: &ResolvedScanBinding,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut metadata_columns = Vec::new();
    let mut seen = HashSet::new();

    for bound in &binding.physical_columns {
        if !seen.insert(bound.source.name.to_ascii_lowercase()) {
            continue;
        }
        match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => columns.push(bound.source.clone()),
            ResolvedScanColumnKind::IcebergMetadataColumn => {
                metadata_columns.push(bound.source.clone())
            }
        }
    }
    for read in &binding.required_reads {
        if seen.insert(read.source.name.to_ascii_lowercase()) {
            columns.push(read.source.clone());
        }
    }

    (columns, metadata_columns)
}

fn merged_bound_table_columns(
    src: &table_model::TableDef,
    scan_columns: &[AnalysisOutputColumn],
    binding: &ResolvedScanBinding,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = src.columns.clone();
    let mut metadata_columns = src.iceberg_row_lineage_metadata_columns.clone();
    for bound in &binding.physical_columns {
        let target = match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => &mut columns,
            ResolvedScanColumnKind::IcebergMetadataColumn => &mut metadata_columns,
        };
        let planner_source_name = scan_columns
            .iter()
            .find(|column| column.column_id == bound.planner.column_id)
            .map(|column| column.name.as_str());
        overlay_bound_column(
            target,
            &bound.planner.name,
            planner_source_name,
            &bound.source,
        );
    }
    for read in &binding.required_reads {
        if replace_column_by_name(&mut columns, &read.source)
            || replace_column_by_name(&mut metadata_columns, &read.source)
        {
            continue;
        }
        columns.push(read.source.clone());
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
        write_default_json: src
            .write_default
            .as_ref()
            .map(|literal| encode_column_write_default_json(src, literal))
            .transpose()?,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn encode_column_write_default_json(
    column: &novarocks_catalog::schema::ColumnDef,
    value: &ColumnDefault,
) -> Result<String, String> {
    validate_column_default(value)?;
    let iceberg_type = iceberg_type_for_column_def(column)?;
    let normalized_value;
    let value = match (value, &iceberg_type) {
        (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            Type::Primitive(PrimitiveType::Timestamp),
        ) => {
            normalized_value = ColumnDefault::TimestampMicros {
                micros_since_epoch: *micros_since_epoch,
            };
            &normalized_value
        }
        (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            Type::Primitive(PrimitiveType::TimestampNs),
        ) => {
            normalized_value = ColumnDefault::TimestampNanos {
                nanos_since_epoch: *nanos_since_epoch,
            };
            &normalized_value
        }
        _ => value,
    };
    crate::connector::iceberg::default_value::column_default_to_iceberg_literal(
        value,
        &iceberg_type,
    )
    .and_then(|literal| {
        literal
            .try_into_json(&iceberg_type)
            .map(|json| json.to_string())
            .map_err(|err| err.to_string())
    })
    .map_err(|err| {
        format!(
            "encode write_default_json for column `{}` as {:?}: {err}",
            column.name, iceberg_type
        )
    })
}

fn iceberg_type_for_column_def(
    column: &novarocks_catalog::schema::ColumnDef,
) -> Result<Type, String> {
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

fn scan_binding_for_source<'a>(
    node_id: i32,
    source: &table_model::ScanSource,
    ctx: &'a NativePlanEncodeContext<'_>,
) -> Result<Option<&'a ResolvedScanBinding>, String> {
    let binding =
        optional_context_ref(ctx.scan_bindings).and_then(|bindings| bindings.binding(node_id));
    let required = scan_source_requires_resolved_binding(source);
    if required && binding.is_none() {
        return Err(match source {
            table_model::ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
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
    if binding.node_id != node_id {
        return Err(format!(
            "native scan encoder binding node mismatch: requested node_id={node_id}, binding node_id={}",
            binding.node_id
        ));
    }
    let valid_execution = match source {
        table_model::ScanSource::IcebergDeltaTable { .. } => {
            matches!(binding.execution, ResolvedScanExecution::IcebergDelta(_))
        }
        table_model::ScanSource::IcebergDataFiles { .. }
        | table_model::ScanSource::IcebergVersionTable { .. }
        | table_model::ScanSource::IcebergMvTargetState(_)
        | table_model::ScanSource::IcebergMvTargetLocator(_) => {
            matches!(binding.execution, ResolvedScanExecution::IcebergFiles(_))
        }
        table_model::ScanSource::IcebergMetadataTable { .. }
        | table_model::ScanSource::StarRocks { .. } => false,
    };
    if !valid_execution {
        return Err(format!(
            "native scan encoder execution variant mismatch for node_id={node_id} source={}: binding={}",
            scan_source_kind(source),
            resolved_execution_kind(&binding.execution)
        ));
    }
    Ok(Some(binding))
}

fn scan_source_kind(source: &table_model::ScanSource) -> &'static str {
    match source {
        table_model::ScanSource::StarRocks { .. } => "StarRocks",
        table_model::ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        table_model::ScanSource::IcebergMetadataTable { .. } => "IcebergMetadataTable",
        table_model::ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        table_model::ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        table_model::ScanSource::IcebergMvTargetState(_) => "IcebergMvTargetState",
        table_model::ScanSource::IcebergMvTargetLocator(_) => "IcebergMvTargetLocator",
    }
}

fn resolved_execution_kind(execution: &ResolvedScanExecution) -> &'static str {
    match execution {
        ResolvedScanExecution::IcebergFiles(_) => "IcebergFiles",
        ResolvedScanExecution::IcebergDelta(_) => "IcebergDelta",
    }
}

fn encode_scan_source(
    src: &table_model::ScanSource,
    scan_node_id: Option<i32>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    if let Some(ResolvedScanExecution::IcebergFiles(files)) =
        binding.map(|binding| &binding.execution)
    {
        return Ok(plan::ScanSource {
            kind: Some(Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(&files.table)?),
                files: files
                    .files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: files.cloud_properties.clone().into_iter().collect(),
                binding: match files.binding {
                    iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            })),
        });
    }

    Ok(plan::ScanSource {
        kind: Some(match src {
            table_model::ScanSource::StarRocks { .. } => {
                let node_id = scan_node_id.ok_or_else(|| {
                    "StarRocks table source is only valid on a native ScanNode".to_string()
                })?;
                let descriptor = optional_context_ref(ctx.scan_bindings)
                    .and_then(|bindings| bindings.starrocks_source(node_id))
                    .ok_or_else(|| {
                        format!(
                            "StarRocks ScanNode node_id={node_id} missing native source descriptor"
                        )
                    })?;
                Kind::StarrocksTable(plan::StarRocksTableSource {
                    catalog_name: descriptor.catalog_name.clone(),
                    db_id: descriptor.db_id,
                    table_id: descriptor.table_id,
                    schema_id: descriptor.schema_id,
                    storage_columns: descriptor
                        .storage_columns
                        .iter()
                        .map(|column| plan::StarRocksColumnStorageMeta {
                            name: column.name.clone(),
                            unique_id: column.unique_id,
                            default_value: column.default_value.clone(),
                        })
                        .collect(),
                    current_schema: Some(encode_starrocks_tablet_schema(
                        &descriptor.tablet_schema,
                    )),
                })
            }
            table_model::ScanSource::IcebergDataFiles {
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
                    iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            }),
            table_model::ScanSource::IcebergMetadataTable {
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
            table_model::ScanSource::IcebergDeltaTable {
                table,
                from_snapshot_id,
                to_snapshot_id,
            } => {
                let Some(ResolvedScanExecution::IcebergDelta(delta)) =
                    binding.map(|binding| &binding.execution)
                else {
                    return Err(format!(
                        "native scan encoder missing prepared IcebergDelta binding for node_id={}",
                        scan_node_id
                            .map(|node_id| node_id.to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    ));
                };

                Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    from_snapshot_id: *from_snapshot_id,
                    to_snapshot_id: *to_snapshot_id,
                    delta_plan: Some(
                        super::super::iceberg_delta_scan::encode_iceberg_delta_scan_plan_native(
                            &delta.runtime_plan,
                        )?,
                    ),
                })
            }
            table_model::ScanSource::IcebergVersionTable { table, snapshot_id } => {
                Kind::IcebergVersionTable(plan::IcebergVersionTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    snapshot_id: *snapshot_id,
                })
            }
            table_model::ScanSource::IcebergMvTargetState(scan) => {
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
                        table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                            plan::IcebergMvTargetStatePartitionConstraint::Unpartitioned as i32
                        }
                        table_model::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                            plan::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired as i32
                        }
                    },
                })
            }
            table_model::ScanSource::IcebergMvTargetLocator(scan) => {
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

fn encode_starrocks_tablet_schema(
    schema: &StarRocksTabletSchemaDescriptor,
) -> plan::StarRocksTabletSchema {
    plan::StarRocksTabletSchema {
        schema_id: schema.schema_id,
        keys_type: match schema.keys_type {
            StarRocksKeysTypeDescriptor::Duplicate => {
                plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32
            }
            StarRocksKeysTypeDescriptor::Unique => {
                plan::StarRocksKeysType::StarrocksKeysTypeUnique as i32
            }
            StarRocksKeysTypeDescriptor::Aggregate => {
                plan::StarRocksKeysType::StarrocksKeysTypeAggregate as i32
            }
            StarRocksKeysTypeDescriptor::Primary => {
                plan::StarRocksKeysType::StarrocksKeysTypePrimary as i32
            }
        },
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        columns: schema
            .columns
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn encode_starrocks_column_schema(
    column: &StarRocksColumnSchemaDescriptor,
) -> plan::StarRocksColumnSchema {
    plan::StarRocksColumnSchema {
        unique_id: column.unique_id,
        name: column.name.clone(),
        physical_type: column.physical_type.clone(),
        is_key: Some(column.is_key),
        aggregation: column.aggregation.clone(),
        nullable: Some(column.nullable),
        default_value: column.default_value.clone(),
        precision: column.precision,
        scale: column.scale,
        visible: Some(column.visible),
        children: column
            .children
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn encode_mv_target_state_row_filter(
    src: &table_model::IcebergMvTargetStateRowFilter,
) -> plan::IcebergMvTargetStateRowFilter {
    use plan::iceberg_mv_target_state_row_filter::Kind;

    plan::IcebergMvTargetStateRowFilter {
        kind: Some(match src {
            table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
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

pub(super) fn encode_iceberg_table_info(
    src: &iceberg_scan_model::IcebergTableInfo,
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
    src: &iceberg_scan_model::IcebergSchemaDef,
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
    src: &iceberg_scan_model::IcebergSchemaFieldDef,
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
        .map(super::super::iceberg_literal_json::serialize_iceberg_literal_json)
        .transpose()
        .map_err(|err| format!("encode Iceberg schema {label} JSON: {err}"))
}

fn encode_iceberg_data_file_info(
    src: &iceberg_scan_model::IcebergDataFileInfo,
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

fn encode_iceberg_column_stats(
    src: &iceberg_scan_model::IcebergColumnStats,
) -> plan::IcebergColumnStats {
    plan::IcebergColumnStats {
        null_count: src.null_count,
        value_count: src.value_count,
        column_size: src.column_size,
        lower_bound: src.lower_bound.clone(),
        upper_bound: src.upper_bound.clone(),
    }
}

fn encode_iceberg_delete_file_info(
    src: &iceberg_scan_model::IcebergDeleteFileInfo,
) -> plan::IcebergDeleteFileInfo {
    plan::IcebergDeleteFileInfo {
        path: src.path.clone(),
        file_format: match src.file_format {
            iceberg_scan_model::IcebergDeleteFileFormat::Parquet => {
                plan::IcebergDeleteFileFormat::Parquet as i32
            }
            iceberg_scan_model::IcebergDeleteFileFormat::Puffin => {
                plan::IcebergDeleteFileFormat::Puffin as i32
            }
        },
        file_content: match src.file_content {
            iceberg_scan_model::IcebergDeleteFileContent::Position => {
                plan::IcebergDeleteFileContent::Position as i32
            }
            iceberg_scan_model::IcebergDeleteFileContent::Equality => {
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
    src: &iceberg_scan_model::IcebergPartitionFieldValue,
) -> plan::IcebergPartitionFieldValue {
    plan::IcebergPartitionFieldValue {
        source_column: src.source_column.clone(),
        field_name: src.field_name.clone(),
        transform: src.transform.clone(),
        value: src.value.as_ref().map(encode_iceberg_partition_value),
    }
}

fn encode_iceberg_partition_value(
    src: &iceberg_scan_model::IcebergPartitionValue,
) -> plan::IcebergPartitionValue {
    use plan::iceberg_partition_value::Value;

    plan::IcebergPartitionValue {
        value: Some(match src {
            iceberg_scan_model::IcebergPartitionValue::Boolean(value) => Value::BoolValue(*value),
            iceberg_scan_model::IcebergPartitionValue::Int32(value) => Value::Int32Value(*value),
            iceberg_scan_model::IcebergPartitionValue::Int64(value) => Value::Int64Value(*value),
            iceberg_scan_model::IcebergPartitionValue::Float(value) => Value::FloatValue(*value),
            iceberg_scan_model::IcebergPartitionValue::Double(value) => Value::DoubleValue(*value),
            iceberg_scan_model::IcebergPartitionValue::String(value) => {
                Value::StringValue(value.clone())
            }
            iceberg_scan_model::IcebergPartitionValue::Binary(value) => {
                Value::BinaryValue(value.clone())
            }
        }),
    }
}
