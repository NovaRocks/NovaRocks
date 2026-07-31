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

use crate::connector::iceberg::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent};
use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::planner::logical::*;
use crate::sql::planner::payload::*;

use super::output::adapt_plan_output_with_qualifier;
use super::query::plan_scoped_query;

// ---------------------------------------------------------------------------
// FROM clause planning
// ---------------------------------------------------------------------------

pub(super) fn plan_relation_scoped(
    relation: Relation,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    match relation {
        Relation::Scan(scan) => {
            // G1: reuse the ColumnIds the analyzer already minted for this
            // table's columns (carried on `scan.column_ids`). Minting fresh
            // ids here would desync the analyzer-produced `ColumnRef`s in
            // the rest of the plan (Window PARTITION BY, GROUP BY, ORDER BY,
            // join eq keys, etc.) from the scan output, and distribution
            // matching would fail.
            let base_len = scan.table.columns.len();
            let mut columns: Vec<OutputColumn> = scan
                .table
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| OutputColumn {
                    column_id: scan.column_ids.get(idx).copied().unwrap_or_else(|| {
                        factory.create(
                            scan.alias.as_ref().or(Some(&scan.table.name)).cloned(),
                            c.name.clone(),
                            c.data_type.clone(),
                            c.nullable,
                        )
                    }),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    is_internal: false,
                })
                .collect();
            for (meta_idx, c) in scan
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .enumerate()
            {
                let col_id_idx = base_len + meta_idx;
                columns.push(OutputColumn {
                    column_id: scan.column_ids.get(col_id_idx).copied().unwrap_or_else(|| {
                        factory.create(
                            scan.alias.as_ref().or(Some(&scan.table.name)).cloned(),
                            c.name.clone(),
                            c.data_type.clone(),
                            c.nullable,
                        )
                    }),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    is_internal: false,
                });
            }
            Ok(LogicalPlanNode::new(
                LogicalPlanKind::Scan(PlanScanNode {
                    database: scan.database,
                    table: scan.table,
                    alias: scan.alias,
                    columns: columns,
                    predicates: vec![],
                    required_columns: None,
                    variant_columns: vec![],
                    mv_rewritten_from: None,
                }),
                vec![],
                None,
            ))
        }
        Relation::Subquery {
            query,
            alias,
            output_columns,
        } => {
            let inner_plan = plan_scoped_query(*query, cte_registry, factory)?;
            adapt_plan_output_with_qualifier(inner_plan, &output_columns, Some(&alias))
        }
        Relation::Join(join_rel) => {
            let JoinRelation {
                left,
                right,
                join_type,
                condition,
            } = *join_rel;
            match right {
                Relation::Unnest(unnest) => {
                    let is_left_join = match join_type {
                        JoinKind::Cross | JoinKind::Inner => false,
                        JoinKind::LeftOuter => true,
                        other => {
                            return Err(format!(
                                "LATERAL UNNEST supports CROSS/INNER/LEFT joins, got {other:?}"
                            ));
                        }
                    };
                    if !is_lateral_unnest_condition_supported(&condition) {
                        return Err(
                            "LATERAL UNNEST currently requires no condition or ON TRUE".into()
                        );
                    }
                    let left = plan_relation_scoped(left, cte_registry, factory)?;
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::TableFunction(PlanTableFunctionNode {
                            function_name: "unnest".to_string(),
                            args: unnest.args,
                            output_columns: unnest.output_columns,
                            alias: unnest.alias,
                            is_left_join: is_left_join,
                        }),
                        vec![left],
                        None,
                    ))
                }
                right => {
                    let left = plan_relation_scoped(left, cte_registry, factory)?;
                    let right = plan_relation_scoped(right, cte_registry, factory)?;
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::Join(LogicalJoinNode {
                            join_type: join_type,
                            condition: condition,
                        }),
                        vec![left, right],
                        None,
                    ))
                }
            }
        }
        Relation::GenerateSeries(gs) => Ok(LogicalPlanNode::new(
            LogicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: gs.start,
                end: gs.end,
                step: gs.step,
                column_name: gs.column_name,
                alias: gs.alias,
                output_column_id: gs.output_column_id,
            }),
            vec![],
            None,
        )),
        Relation::Unnest(_) => Err("UNNEST is currently supported only in LATERAL JOIN".into()),
        Relation::CTEConsume {
            cte_id,
            alias,
            output_columns,
            producer_column_ids,
        } => Ok(LogicalPlanNode::new(
            LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id: cte_id,
                alias: alias,
                output_columns: output_columns,
                producer_column_ids: producer_column_ids,
            }),
            vec![],
            None,
        )),
        Relation::IcebergMetadataScan(rel) => plan_iceberg_metadata_scan(rel, factory),
        Relation::IcebergDeltaScan(rel) => plan_iceberg_delta_scan(rel, factory),
    }
}

fn is_lateral_unnest_condition_supported(condition: &Option<TypedExpr>) -> bool {
    matches!(
        condition,
        None | Some(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            ..
        })
    )
}

/// Lower an analyzer-built `IcebergMetadataScanRelation` into a regular
/// `LogicalPlanKind::Scan` whose `TableDef` carries the synthetic
/// `ScanSource::IcebergMetadataTable` source. The optimizer treats it
/// like any other Scan; codegen branches on the source variant to emit
/// an `HDFS_SCAN_NODE` whose lowering wires up the native-Rust
/// Iceberg metadata SPI reader (no JVM / JNI bridge — the embedded-Java
/// path was removed in favor of iceberg-rust).
fn plan_iceberg_metadata_scan(
    rel: IcebergMetadataScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::sql::analyzer::iceberg_metadata::metadata_table_schema_for_source;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    let cols =
        metadata_table_schema_for_source(rel.metadata_table_type.clone(), &rel.table.source)?;
    if cols.is_empty() {
        return Err(format!(
            "iceberg metadata table type {:?} is not supported",
            rel.metadata_table_type
        ));
    }
    let column_defs: Vec<ColumnDef> = cols
        .iter()
        .map(|c| ColumnDef {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            write_default: None,
            logical_type: None,
        })
        .collect();
    // Reuse the ColumnIds that the analyzer already minted for this metadata
    // table's columns (carried on `rel.column_ids`). Creating fresh ids here
    // would desync the `ColumnRef` ids in the rest of the plan (SELECT list,
    // WHERE, etc.) from the scan's output_columns, causing Phase-2 column
    // pruning to incorrectly prune needed columns (same pattern as Relation::Scan).
    let output_columns: Vec<OutputColumn> = cols
        .iter()
        .enumerate()
        .map(|(idx, c)| OutputColumn {
            column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                factory.create(None, c.name.clone(), c.data_type.clone(), c.nullable)
            }),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            is_internal: false,
        })
        .collect();
    let table_info = iceberg_table_info(&rel.table.source)
        .ok_or_else(|| {
            format!(
                "iceberg metadata table {} requires iceberg table identity; \
                 table was not loaded through an iceberg catalog",
                rel.table.name
            )
        })?
        .clone();
    let serialized_table = table_info.serialized_metadata.clone().ok_or_else(|| {
        format!(
            "iceberg metadata table {} requires serialized metadata; \
                 table was not loaded through an iceberg catalog",
            rel.table.name
        )
    })?;
    let cloud_properties = match &rel.table.source {
        ScanSource::IcebergDataFiles {
            cloud_properties, ..
        } => cloud_properties.clone(),
        _ => Default::default(),
    };
    let metadata_payload =
        build_iceberg_metadata_payload(&rel.metadata_table_type, &rel.table.source)?;
    let synthetic_name = format!("{}__nr_meta__", rel.table.name);
    let synthetic_table = TableDef {
        name: synthetic_name,
        columns: column_defs,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::IcebergMetadataTable {
            table: table_info,
            metadata_table_type: rel.metadata_table_type,
            serialized_table,
            cloud_properties,
            metadata_payload,
        },
    };
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Scan(PlanScanNode {
            database: rel.database,
            table: synthetic_table,
            alias: rel.alias,
            columns: output_columns,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        vec![],
        None,
    ))
}

#[derive(Default)]
struct PartitionMetadataAgg {
    record_count: i64,
    file_count: i64,
    total_data_file_size_in_bytes: i64,
    position_delete_files: std::collections::BTreeSet<String>,
    equality_delete_files: std::collections::BTreeSet<String>,
}

fn build_iceberg_metadata_payload(
    metadata_table_type: &crate::connector::iceberg::IcebergMetadataTableType,
    storage: &crate::sql::planner::table::ScanSource,
) -> Result<Option<String>, String> {
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::sql::planner::table::ScanSource;
    match metadata_table_type {
        IcebergMetadataTableType::Partitions => {
            let ScanSource::IcebergDataFiles { files, .. } = storage else {
                return Err(
                    "iceberg partitions metadata table requires catalog-resolved data files"
                        .to_string(),
                );
            };
            build_iceberg_partitions_payload(files).map(Some)
        }
        IcebergMetadataTableType::Files
        | IcebergMetadataTableType::Manifests
        | IcebergMetadataTableType::LogicalIcebergMetadata => {
            let table_info = iceberg_table_info(storage).ok_or_else(|| {
                "iceberg files/manifests/entries metadata table requires iceberg table identity"
                    .to_string()
            })?;
            table_info
                .serialized_metadata_rows
                .clone()
                .map(Some)
                .ok_or_else(|| {
                    "iceberg metadata rows were not resolved at catalog lookup time".to_string()
                })
        }
        IcebergMetadataTableType::Snapshots
        | IcebergMetadataTableType::History
        | IcebergMetadataTableType::Refs => Ok(None),
    }
}

fn build_iceberg_partitions_payload(files: &[IcebergDataFileInfo]) -> Result<String, String> {
    let mut groups = std::collections::BTreeMap::<(i32, String), PartitionMetadataAgg>::new();
    for file in files {
        let spec_id = file.partition_spec_id.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires partition spec id for data file {}",
                file.path
            )
        })?;
        let record_count = file.row_count.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires record_count for data file {}",
                file.path
            )
        })?;
        let partition_key = file
            .partition_key
            .clone()
            .unwrap_or_else(|| "Struct([])".to_string());
        let agg = groups.entry((spec_id, partition_key)).or_default();
        agg.record_count = agg
            .record_count
            .checked_add(record_count)
            .ok_or_else(|| "iceberg partitions metadata record_count overflow".to_string())?;
        agg.file_count = agg
            .file_count
            .checked_add(1)
            .ok_or_else(|| "iceberg partitions metadata file_count overflow".to_string())?;
        agg.total_data_file_size_in_bytes = agg
            .total_data_file_size_in_bytes
            .checked_add(file.size)
            .ok_or_else(|| {
                "iceberg partitions metadata total_data_file_size_in_bytes overflow".to_string()
            })?;
        for delete_file in &file.delete_files {
            match delete_file.file_content {
                IcebergDeleteFileContent::Position => {
                    agg.position_delete_files.insert(delete_file.path.clone());
                }
                IcebergDeleteFileContent::Equality => {
                    agg.equality_delete_files.insert(delete_file.path.clone());
                }
            }
        }
    }
    let rows = groups
        .into_iter()
        .map(
            |((spec_id, partition), agg)| -> Result<serde_json::Value, String> {
                let position_delete_file_count = i64::try_from(agg.position_delete_files.len())
                    .map_err(|_| {
                        "iceberg partitions metadata position_delete_file_count overflow"
                            .to_string()
                    })?;
                let equality_delete_file_count = i64::try_from(agg.equality_delete_files.len())
                    .map_err(|_| {
                        "iceberg partitions metadata equality_delete_file_count overflow"
                            .to_string()
                    })?;
                Ok(serde_json::json!({
                    "spec_id": spec_id,
                    "partition": partition,
                    "record_count": agg.record_count,
                    "file_count": agg.file_count,
                    "total_data_file_size_in_bytes": agg.total_data_file_size_in_bytes,
                    "position_delete_file_count": position_delete_file_count,
                    "equality_delete_file_count": equality_delete_file_count,
                }))
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&serde_json::json!({
        "version": 1,
        "rows": rows,
    }))
    .map_err(|e| format!("serialize iceberg partitions metadata payload failed: {e}"))
}

/// Lower an analyzer-built `IcebergDeltaScanRelation` into a regular
/// `LogicalPlanKind::Scan` whose `TableDef` carries the synthetic
/// `ScanSource::IcebergDeltaTable` storage. Codegen recognizes this
/// storage variant and emits `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`
/// (rather than `HDFS_SCAN_NODE`). Refresh/codegen expands the storage
/// variant into a typed explicit payload; lower only consumes that payload.
fn plan_iceberg_delta_scan(
    rel: IcebergDeltaScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::sql::planner::table::{ScanSource, TableDef};

    // Output schema: base columns + iceberg v3 row-lineage metadata columns.
    // The delta scan emits both: scanner-side projection re-uses the same
    // column ordering as the base scan, plus the row-lineage virtual columns
    // for downstream row-identity matching.
    //
    // Reuse the ColumnIds that the analyzer already minted for this delta scan's
    // columns (carried on `rel.column_ids`). Creating fresh ids here would desync
    // the `ColumnRef` ids in the rest of the plan from the scan's output columns,
    // causing Phase-2 column pruning to incorrectly prune needed scan columns.
    let base_col_count = rel.table.columns.len();
    let mut output_columns: Vec<OutputColumn> = rel
        .table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, c)| OutputColumn {
            column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                factory.create(None, c.name.clone(), c.data_type.clone(), c.nullable)
            }),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            is_internal: false,
        })
        .collect();
    for (meta_idx, col) in rel
        .table
        .iceberg_row_lineage_metadata_columns
        .iter()
        .enumerate()
    {
        let col_id_idx = base_col_count + meta_idx;
        output_columns.push(OutputColumn {
            column_id: rel.column_ids.get(col_id_idx).copied().unwrap_or_else(|| {
                factory.create(None, col.name.clone(), col.data_type.clone(), col.nullable)
            }),
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
            is_internal: false,
        });
    }
    let table_info = iceberg_table_info(&rel.table.source)
        .ok_or_else(|| {
            format!(
                "iceberg delta scan {}.{}.{} requires iceberg table identity",
                rel.catalog, rel.namespace, rel.table_name
            )
        })?
        .clone();
    let synthetic_table = TableDef {
        name: rel.table.name.clone(),
        columns: rel.table.columns.clone(),
        iceberg_row_lineage_metadata_columns: rel
            .table
            .iceberg_row_lineage_metadata_columns
            .clone(),
        source: ScanSource::IcebergDeltaTable {
            table: table_info,
            from_snapshot_id: rel.from_snapshot_id,
            to_snapshot_id: rel.to_snapshot_id,
        },
    };
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Scan(PlanScanNode {
            database: rel.namespace,
            table: synthetic_table,
            alias: rel.alias,
            columns: output_columns,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        vec![],
        None,
    ))
}

fn iceberg_table_info(
    source: &crate::sql::planner::table::ScanSource,
) -> Option<&crate::connector::iceberg::scan_model::IcebergTableInfo> {
    match source {
        crate::sql::planner::table::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::planner::table::ScanSource::ConnectorPinned
        | crate::sql::planner::table::ScanSource::StarRocks { .. }
        | crate::sql::planner::table::ScanSource::IcebergMvTargetState { .. }
        | crate::sql::planner::table::ScanSource::IcebergMvTargetLocator { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

pub(super) fn plan_set_operation_scoped(
    set_op: ResolvedSetOp,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    // Build position-aligned output schema before consuming the branches.
    // For each position we widen the type across left/right (matching
    // the analyzer's wider_type logic), keep the left branch ColumnId and
    // name, and union the nullable flags. This mirrors what derive_output_columns
    // and visit_set_op_common use as the canonical union output schema.
    let output_columns: Vec<OutputColumn> = set_op
        .left
        .output_columns
        .iter()
        .zip(set_op.right.output_columns.iter())
        .map(|(lc, rc)| {
            let dt = novarocks_types::wider_type(&lc.data_type, &rc.data_type);
            OutputColumn {
                column_id: lc.column_id,
                name: lc.name.clone(),
                data_type: dt,
                nullable: lc.nullable || rc.nullable,
                is_internal: lc.is_internal && rc.is_internal,
            }
        })
        .collect();

    let left = plan_scoped_query(*set_op.left, cte_registry, factory)?;
    let right = plan_scoped_query(*set_op.right, cte_registry, factory)?;

    match set_op.kind {
        SetOpKind::Union => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: set_op.all,
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
        SetOpKind::Intersect => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Intersect(LogicalIntersectNode {
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
        SetOpKind::Except => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Except(LogicalExceptNode {
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
    }
}

// ---------------------------------------------------------------------------
// VALUES planning
// ---------------------------------------------------------------------------

pub(super) fn plan_values(
    values: ResolvedValues,
    _factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let columns = values.output_columns;
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: values.rows,
            columns: columns,
        }),
        vec![],
        None,
    ))
}
