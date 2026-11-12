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

use crate::analysis::cte::CTERegistry;
use crate::analysis::*;
use crate::column_id::ColumnRefFactory;
use crate::compiler::SqlCompileError;
use crate::planner::logical::*;
use crate::planner::payload::*;
use novarocks_type_contract::PureCompileControl;

use super::output::adapt_plan_output_with_qualifier;
use super::query::plan_scoped_query;

// ---------------------------------------------------------------------------
// FROM clause planning
// ---------------------------------------------------------------------------

pub(super) fn plan_relation_scoped(
    relation: Relation,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    match relation {
        Relation::SystemTable {
            declaration,
            output_columns,
            ..
        } => {
            let columns = output_columns
                .iter()
                .map(|column| column.planner_column())
                .collect::<Vec<_>>();
            let required_columns = (0..columns.len()).collect();
            Ok(LogicalPlanNode::new(
                LogicalPlanKind::SystemTable(
                    crate::planner::system_relation::PlanSystemTableNode {
                        declaration,
                        columns,
                        required_columns,
                    },
                ),
                vec![],
                None,
            ))
        }
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
                .map(|(idx, c)| {
                    let value_type = c.declared_value_type().map_err(|error| error.to_string())?;
                    Ok(OutputColumn {
                        column_id: scan.column_ids.get(idx).copied().unwrap_or_else(|| {
                            factory.create(
                                scan.alias.as_ref().or(Some(&scan.table.name)).cloned(),
                                c.name.clone(),
                                value_type.clone(),
                            )
                        }),
                        name: c.name.clone(),
                        value_type,

                        is_internal: false,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            for (meta_idx, c) in scan
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .enumerate()
            {
                let col_id_idx = base_len + meta_idx;
                columns.push({
                    let value_type = c.declared_value_type().map_err(|error| error.to_string())?;
                    OutputColumn {
                        column_id: scan.column_ids.get(col_id_idx).copied().unwrap_or_else(|| {
                            factory.create(
                                scan.alias.as_ref().or(Some(&scan.table.name)).cloned(),
                                c.name.clone(),
                                value_type.clone(),
                            )
                        }),
                        name: c.name.clone(),
                        value_type,

                        is_internal: false,
                    }
                });
            }
            Ok(LogicalPlanNode::new(
                LogicalPlanKind::scan(PlanScanNode {
                    database: scan.database,
                    table: scan.table,
                    alias: scan.alias,
                    columns,
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
            let inner_plan = plan_scoped_query(*query, cte_registry, factory, control)?;
            adapt_plan_output_with_qualifier(
                inner_plan,
                &crate::analysis::schema::planner_columns(&output_columns),
                Some(&alias),
            )
            .map_err(SqlCompileError::Compilation)
        }
        Relation::Join(join_rel) => {
            let JoinRelation {
                naa_author,
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
                            )
                            .into());
                        }
                    };
                    if !is_lateral_unnest_condition_supported(&condition) {
                        return Err(
                            "LATERAL UNNEST currently requires no condition or ON TRUE".into()
                        );
                    }
                    let left = plan_relation_scoped(left, cte_registry, factory, control)?;
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::TableFunction(PlanTableFunctionNode {
                            function_name: "unnest".to_string(),
                            args: unnest.args,
                            binding: unnest.binding,
                            output_columns: crate::analysis::schema::planner_columns(
                                &unnest.output_columns,
                            ),
                            alias: unnest.alias,
                            is_left_join,
                        }),
                        vec![left],
                        None,
                    ))
                }
                right => {
                    let left = plan_relation_scoped(left, cte_registry, factory, control)?;
                    let right = plan_relation_scoped(right, cte_registry, factory, control)?;
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::Join(LogicalJoinNode {
                            naa_author: naa_author.map(Box::new),
                            join_type,
                            condition,
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
                cte_id,
                alias,
                output_columns: crate::analysis::schema::planner_columns(&output_columns),
                producer_column_ids,
            }),
            vec![],
            None,
        )),
        Relation::IcebergMetadataScan(rel) => {
            plan_iceberg_metadata_scan(rel, factory).map_err(SqlCompileError::Compilation)
        }
        Relation::IcebergDeltaScan(rel) => {
            plan_iceberg_delta_scan(rel, factory).map_err(SqlCompileError::Compilation)
        }
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
/// `LogicalPlanKind::Scan` whose `TableDef` carries a token-bound
/// `SqlScanSource::Metadata` source. The optimizer treats it
/// like any other Scan; codegen branches on the source variant to emit
/// an `HDFS_SCAN_NODE` whose lowering wires up the native-Rust
/// Iceberg metadata SPI reader (no JVM / JNI bridge — the embedded-Java
/// path was removed in favor of iceberg-rust).
fn plan_iceberg_metadata_scan(
    rel: IcebergMetadataScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::planner::table::{ScanSource, TableDef};

    if rel.table.columns.is_empty() {
        return Err(format!(
            "iceberg metadata table type {:?} is not supported",
            rel.metadata_table_type
        ));
    }
    // Reuse the ColumnIds that the analyzer already minted for this metadata
    // table's columns (carried on `rel.column_ids`). Creating fresh ids here
    // would desync the `ColumnRef` ids in the rest of the plan (SELECT list,
    // WHERE, etc.) from the scan's output_columns, causing Phase-2 column
    // pruning to incorrectly prune needed columns (same pattern as Relation::Scan).
    let output_columns: Vec<OutputColumn> =
        rel.table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let value_type = c.declared_value_type().map_err(|error| error.to_string())?;
                Ok(OutputColumn {
                    column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                        factory.create(None, c.name.clone(), value_type.clone())
                    }),
                    name: c.name.clone(),
                    value_type,

                    is_internal: false,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
    let ScanSource::Sql(source) = rel.table.source;
    if !matches!(
        source.kind,
        crate::planner::table::SqlScanKind::Metadata { .. }
    ) {
        return Err(format!(
            "iceberg metadata table {} has a non-metadata SQL scan source",
            rel.table.name
        ));
    }
    let synthetic_table = TableDef {
        // Metadata identity is already structured in `SqlScanKind::Metadata`;
        // no normalizer-injected display-name marker is needed.
        name: rel.table.name,
        columns: rel.table.columns,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::Sql(source),
    };
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::scan(PlanScanNode {
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

/// Lower an analyzer-built `IcebergDeltaScanRelation` into a regular
/// `LogicalPlanKind::Scan` whose `TableDef` carries a token-bound
/// `SqlScanSource::Delta` fact. Preparation resolves the exact provider
/// materialization through that same request-local token; the logical plan
/// neither receives nor reconstructs Iceberg metadata.
fn plan_iceberg_delta_scan(
    rel: IcebergDeltaScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::planner::table::{ScanSource, TableDef};

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
    let mut output_columns: Vec<OutputColumn> =
        rel.table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let value_type = c.declared_value_type().map_err(|error| error.to_string())?;
                Ok(OutputColumn {
                    column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                        factory.create(None, c.name.clone(), value_type.clone())
                    }),
                    name: c.name.clone(),
                    value_type,

                    is_internal: false,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
    for (meta_idx, col) in rel
        .table
        .iceberg_row_lineage_metadata_columns
        .iter()
        .enumerate()
    {
        let col_id_idx = base_col_count + meta_idx;
        output_columns.push({
            let value_type = col
                .declared_value_type()
                .map_err(|error| error.to_string())?;
            OutputColumn {
                column_id: rel
                    .column_ids
                    .get(col_id_idx)
                    .copied()
                    .unwrap_or_else(|| factory.create(None, col.name.clone(), value_type.clone())),
                name: col.name.clone(),
                value_type,

                is_internal: false,
            }
        });
    }
    let ScanSource::Sql(source) = rel.table.source;
    if !matches!(
        &source.kind,
        crate::planner::table::SqlScanKind::Data { .. }
            | crate::planner::table::SqlScanKind::FrozenInputSet { .. }
    ) {
        return Err(format!(
            "iceberg delta scan {}.{}.{} requires a data or frozen-input SQL scan source",
            rel.catalog, rel.namespace, rel.table_name
        ));
    }
    if source.table.catalog != rel.catalog
        || source.table.namespace != rel.namespace
        || source.table.table != rel.table_name
    {
        return Err(format!(
            "iceberg delta scan {}.{}.{} binding identity does not match the resolved table",
            rel.catalog, rel.namespace, rel.table_name
        ));
    }
    let delta_source = crate::planner::table::SqlScanSource::new(
        source.binding,
        source.table,
        crate::planner::table::SqlScanKind::Delta {
            from_snapshot_id: rel.from_snapshot_id,
            to_snapshot_id: rel.to_snapshot_id,
        },
    );
    let synthetic_table = TableDef {
        name: rel.table.name.clone(),
        columns: rel.table.columns.clone(),
        iceberg_row_lineage_metadata_columns: rel
            .table
            .iceberg_row_lineage_metadata_columns
            .clone(),
        source: ScanSource::Sql(delta_source),
    };
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::scan(PlanScanNode {
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

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

pub(super) fn plan_set_operation_scoped(
    set_op: ResolvedSetOp,
    output_columns: &[OutputColumn],
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    // The parser associates a long set-operation chain to the left. Walk
    // that spine explicitly so an ordinary wide UNION ALL consumes bounded
    // planning stack while retaining every pairwise type-widening step.
    let mut operations = Vec::new();
    let mut current = set_op;
    let mut declared_output = output_columns.to_vec();
    let base_left = loop {
        let ResolvedSetOp {
            kind,
            all,
            left,
            right,
        } = current;
        // The analyzer already authored each pairwise common value type,
        // including nominal children and NULL neutrality. Flattening the set
        // spine must carry that declaration instead of deriving a new type
        // from Arrow carriers and erasing the original domains.
        operations.push((kind, all, right, declared_output));
        let ResolvedQuery {
            body,
            order_by,
            limit,
            offset,
            output_columns,
            local_cte_ids,
        } = *left;
        let body = match body {
            QueryBody::SetOperation(inner)
                if order_by.is_empty()
                    && limit.is_none()
                    && offset.is_none()
                    && local_cte_ids.is_empty() =>
            {
                declared_output = crate::analysis::schema::planner_columns(&output_columns);
                current = inner;
                continue;
            }
            other => other,
        };
        break ResolvedQuery {
            body,
            order_by,
            limit,
            offset,
            output_columns,
            local_cte_ids,
        };
    };
    let mut left = plan_scoped_query(base_left, cte_registry, factory, control)?;
    while let Some((kind, all, right, output_columns)) = operations.pop() {
        let right = plan_scoped_query(*right, cte_registry, factory, control)?;
        let payload = match kind {
            SetOpKind::Union => LogicalPlanKind::Union(LogicalUnionNode {
                all,
                output_columns,
            }),
            SetOpKind::Intersect => {
                LogicalPlanKind::Intersect(LogicalIntersectNode { output_columns })
            }
            SetOpKind::Except => LogicalPlanKind::Except(LogicalExceptNode { output_columns }),
        };
        left = LogicalPlanNode::new(payload, vec![left, right], None);
    }
    Ok(left)
}

// ---------------------------------------------------------------------------
// VALUES planning
// ---------------------------------------------------------------------------

pub(super) fn plan_values(
    values: ResolvedValues,
    _factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let columns = crate::analysis::schema::planner_columns(&values.output_columns);
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: values.rows,
            columns,
        }),
        vec![],
        None,
    ))
}
