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

use arrow::datatypes::DataType;
use novarocks_type_contract::{FunctionValueType, PartitionHashAlgorithm};

use crate::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::common::{CHANGE_OP_COLUMN, ICEBERG_FILE_PATH_COL, ICEBERG_ROW_POS_COL};
use crate::compiler::{SqlCompileError, mv_rewrite::SqlImvRewriteSnapshot};
use crate::optimizer::{operator::ContentApplyOp, rewrite::context::RewriteContext};
use crate::planner::{
    logical::{LogicalPlanKind, LogicalPlanNode},
    payload::{PlanProjectNode, PlanScanNode},
    table::{ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef},
};
use novarocks_types::schema::ColumnDef;

use super::{
    action_column::ImvActionColumn,
    column_alloc::{allocate_imv_output_column, declared_imv_column_value_type},
    join_delta::plan_output_columns,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentKeyInadmissible {
    UnsupportedColumn(usize),
    NoExchangeKey,
}

impl std::fmt::Display for ContentKeyInadmissible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedColumn(ordinal) => write!(
                f,
                "MV content key is inadmissible: field {} cannot be indexed",
                ordinal + 1
            ),
            Self::NoExchangeKey => {
                f.write_str("MV content key is inadmissible: no field supports exchange hashing")
            }
        }
    }
}

impl std::error::Error for ContentKeyInadmissible {}

/// Select a subset of the signed content layout for co-location. Every field
/// still participates in the owner's exact content equality and fingerprint.
pub fn content_route_keys(
    types: &[FunctionValueType],
) -> Result<Box<[u32]>, ContentKeyInadmissible> {
    let mut keys = Vec::new();
    for (ordinal, ty) in types.iter().enumerate() {
        if !novarocks_physical_plan::supports_result_content_key(ty.data_type()) {
            return Err(ContentKeyInadmissible::UnsupportedColumn(ordinal));
        }
        if PartitionHashAlgorithm::NativeExchangeV1.supports_partition_value_type(ty)
            && let Ok(ordinal) = u32::try_from(ordinal)
        {
            keys.push(ordinal);
        }
    }
    if keys.is_empty() {
        return Err(ContentKeyInadmissible::NoExchangeKey);
    }
    Ok(keys.into_boxed_slice())
}

pub(super) fn assemble(
    plan: LogicalPlanNode,
    snapshot: &SqlImvRewriteSnapshot,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let read_columns = snapshot.content_target.as_deref().ok_or_else(|| {
        SqlCompileError::InvalidRequest(
            "MV content application requires the mapped target fields".into(),
        )
    })?;
    let target_snapshot_id = snapshot.target_snapshot_id.ok_or_else(|| {
        SqlCompileError::InvalidRequest("MV content application requires a target snapshot".into())
    })?;
    let read_types = read_columns
        .iter()
        .map(|field| declared_imv_column_value_type(field, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let route_keys = content_route_keys(&read_types).map_err(|cause| {
        crate::analyze_error::AnalyzeError::unsupported_query_shape(
            cause.to_string(),
            novarocks_parser::SYNTHETIC,
        )
    })?;
    let mut demand_keys = Vec::new();
    for &ordinal in snapshot
        .content_demand_ordinals
        .as_deref()
        .unwrap_or_default()
    {
        let ty = &read_types[ordinal as usize];
        if novarocks_local_program::is_semantic_filter_key_type(ty) {
            demand_keys.push(ordinal);
        }
    }

    let columns = plan_output_columns(&plan)?;
    let weight = ImvActionColumn::declared(&columns)
        .map_err(|error| format!("weighted MV tuple has an invalid action column: {error:?}"))?
        .ok_or_else(|| "weighted MV tuple has no action column".to_string())?;
    let visible = columns
        .iter()
        .filter(|column| !column.is_internal)
        .collect::<Vec<_>>();
    if visible.len() != read_columns.len() {
        return Err("weighted MV tuple must cover the whole visible mapping"
            .to_string()
            .into());
    }
    let mut weighted_items = Vec::with_capacity(visible.len() + 1);
    let mut apply_output = Vec::with_capacity(visible.len() + 5);
    for ((source, target), target_type) in visible
        .into_iter()
        .zip(&snapshot.schema_contract.target.visible_columns)
        .zip(&read_types)
    {
        if source.name != target.output_name {
            return Err("weighted MV tuple differs from the definition output order"
                .to_string()
                .into());
        }
        let output = allocate_imv_output_column(ctx, &source.name, target_type.clone(), false)?;
        let expr = to_storage_content(source, target_type, ctx)?;
        weighted_items.push(project_item(&output, expr));
        apply_output.push(allocate_imv_output_column(
            ctx,
            &output.name,
            target_type.clone(),
            false,
        )?);
    }
    weighted_items.push(project_item(weight, column_ref(weight)));
    let weighted = LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items: weighted_items,
            output_qualifier: None,
        }),
        vec![plan],
        None,
    );

    let metadata = snapshot.target_file_metadata.as_deref().ok_or_else(|| {
        SqlCompileError::InvalidRequest("MV content target has no frozen position metadata".into())
    })?;
    let mut file_fields = vec![
        ColumnDef::from_value_type(
            ICEBERG_FILE_PATH_COL.to_string(),
            FunctionValueType::new(DataType::Utf8, false),
            None,
        )
        .map_err(|cause| cause.to_string())?,
        ColumnDef::from_value_type(
            ICEBERG_ROW_POS_COL.to_string(),
            FunctionValueType::new(DataType::Int64, false),
            None,
        )
        .map_err(|cause| cause.to_string())?,
        metadata[0].clone(),
        metadata[1].clone(),
    ];
    // The content-specific provider handle promises an exact partition tuple
    // even for unpartitioned files. The historical child fields remain nullable.
    file_fields[3].nullable = false;

    let mut target_columns = read_columns
        .iter()
        .zip(&read_types)
        .map(|(field, ty)| allocate_imv_output_column(ctx, &field.name, ty.clone(), false))
        .collect::<Result<Vec<_>, _>>()?;
    for field in &file_fields {
        let ty = declared_imv_column_value_type(field, ctx)?;
        target_columns.push(allocate_imv_output_column(
            ctx,
            &field.name,
            ty.clone(),
            true,
        )?);
        apply_output.push(allocate_imv_output_column(
            ctx,
            &field.name,
            ty.with_nullable(true).map_err(|cause| cause.to_string())?,
            true,
        )?);
    }
    apply_output.push(allocate_imv_output_column(
        ctx,
        CHANGE_OP_COLUMN,
        weight.value_type.clone(),
        true,
    )?);
    let target = &snapshot.target;
    let target = LogicalPlanNode::new(
        LogicalPlanKind::scan(PlanScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns: read_columns.to_vec(),
                iceberg_row_lineage_metadata_columns: file_fields,
                source: ScanSource::Sql(SqlScanSource::new(
                    snapshot.target_binding,
                    SqlTableIdentity {
                        catalog: target.catalog.clone(),
                        namespace: target.namespace.clone(),
                        table: target.table.clone(),
                    },
                    SqlScanKind::MvContentTarget {
                        target_table_uuid: snapshot.target_table_uuid.clone(),
                        target_snapshot_id,
                    },
                )),
            },
            alias: None,
            columns: target_columns,
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        Vec::new(),
        None,
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::ContentApply(ContentApplyOp {
            route_keys,
            demand_keys: demand_keys.into_boxed_slice(),
            output_columns: apply_output,
        }),
        vec![weighted, target],
        None,
    ))
}

fn to_storage_content(
    source: &OutputColumn,
    target: &FunctionValueType,
    ctx: &RewriteContext,
) -> Result<TypedExpr, SqlCompileError> {
    let expr = column_ref(source);
    if &source.value_type == target {
        return Ok(expr);
    }
    if source.value_type.nullable() && !target.nullable() {
        return Err(
            "MV content mapping requires a nullable value in a required field"
                .to_string()
                .into(),
        );
    }
    // A frozen M read declaration authorizes the storage-domain projection.
    // Cast lowering still checks the normal complete value-conversion contract.
    Ok(TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(expr),
            target: target.data_type().clone(),
            decimal_overflow_policy: ctx.decimal_overflow_policy(),
        },
        value_type: target.clone(),
    })
}

fn column_ref(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
    }
}

fn project_item(column: &OutputColumn, expr: TypedExpr) -> ProjectItem {
    ProjectItem {
        output_column_id: column.column_id,
        output_name: column.name.clone(),
        expr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_use_hashable_subset_but_require_every_content_column() {
        let key = FunctionValueType::new(DataType::Int64, true);
        let content_only = FunctionValueType::new(DataType::UInt64, true);
        assert_eq!(
            content_route_keys(&[content_only.clone(), key])
                .unwrap()
                .as_ref(),
            [1]
        );
        assert_eq!(
            content_route_keys(&[content_only]),
            Err(ContentKeyInadmissible::NoExchangeKey)
        );
    }
}
