pub(crate) const ICEBERG_MV_APPLY_KEY_COLUMN: &str = "__nova_base_row_id";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID: &str = "base._row_id";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_COLUMN: &str = "novarocks.mv.apply-key.column";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_SOURCE: &str = "novarocks.mv.apply-key.source";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID: &str = "novarocks.mv.apply-key.field-id";

pub(crate) fn apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::BigInt,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn iceberg_mv_physical_select_sql(select_sql: &str) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV physical SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV physical SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("iceberg MV physical SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };

    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                if expr
                    .to_string()
                    .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
                {
                    return Err(format!(
                        "Iceberg MV output column name {ICEBERG_MV_APPLY_KEY_COLUMN} is reserved for internal apply key"
                    ));
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                if alias
                    .value
                    .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
                {
                    return Err(format!(
                        "Iceberg MV output column name {ICEBERG_MV_APPLY_KEY_COLUMN} is reserved for internal apply key"
                    ));
                }
            }
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
    }

    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
            alias: sqlparser::ast::Ident::new(ICEBERG_MV_APPLY_KEY_COLUMN),
        });
    Ok(stmt.to_string())
}

pub(crate) fn find_apply_key_field_id(table: &iceberg::table::Table) -> Result<i32, String> {
    let mut matches = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN));
    let Some(field) = matches.next() else {
        return Err(format!(
            "iceberg MV target schema is missing apply-key column {ICEBERG_MV_APPLY_KEY_COLUMN}"
        ));
    };
    if matches.next().is_some() {
        return Err(format!(
            "iceberg MV target schema has duplicate apply-key column {ICEBERG_MV_APPLY_KEY_COLUMN}"
        ));
    }
    Ok(field.id)
}

pub(crate) fn ensure_base_row_lineage_contract(
    table: &iceberg::table::Table,
    base_fqn: &str,
) -> Result<(), String> {
    let metadata = table.metadata();
    if metadata.format_version() != iceberg::spec::FormatVersion::V3
        || !row_lineage_property_enabled(metadata.properties())
    {
        return Err(format!(
            "iceberg-backed materialized views require base table {base_fqn} to be Iceberg format-version=3 with write.row-lineage=true; \
             upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        ));
    }
    Ok(())
}

fn row_lineage_property_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get("write.row-lineage")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub(crate) fn extract_apply_key_values_from_chunks(
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<i64>, String> {
    use arrow::array::Array;

    let mut out = Vec::new();
    for chunk in chunks {
        let schema = chunk.batch.schema();
        let idx = schema.index_of(ICEBERG_MV_APPLY_KEY_COLUMN).map_err(|e| {
            format!(
                "iceberg MV projected changes missing apply-key column {ICEBERG_MV_APPLY_KEY_COLUMN}: {e}"
            )
        })?;
        let casted =
            arrow::compute::cast(chunk.batch.column(idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| format!("cast {ICEBERG_MV_APPLY_KEY_COLUMN} to BIGINT failed: {e}"))?;
        let values = casted
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| format!("{ICEBERG_MV_APPLY_KEY_COLUMN} is not BIGINT after cast"))?;
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(format!(
                    "iceberg MV projected changes contain NULL {ICEBERG_MV_APPLY_KEY_COLUMN}"
                ));
            }
            out.push(values.value(row));
        }
    }
    Ok(out)
}

pub(crate) fn load_target_apply_locator_inputs(
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_table: &iceberg::table::Table,
) -> Result<
    (
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
        crate::engine::delete_flow::ReferencedDataFilePartitions,
    ),
    String,
> {
    let snapshot_id = target_table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let existing_deletes_by_file =
        crate::engine::delete_flow::load_existing_delete_visibility_by_data_file_at(
            target_table,
            snapshot_id,
            target_entry.object_store_config(),
        )?;
    if existing_deletes_by_file
        .values()
        .any(|visibility| !visibility.equality_deletes.is_empty())
    {
        return Err(
            "iceberg MV target row locator cannot apply on a target snapshot with equality deletes; compact the target first"
                .to_string(),
        );
    }
    let referenced_data_file_partitions =
        crate::engine::delete_flow::load_referenced_data_file_partitions_at(
            target_table,
            snapshot_id,
        )?;
    Ok((existing_deletes_by_file, referenced_data_file_partitions))
}

pub(crate) async fn locate_target_rows_by_apply_key(
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    use arrow::array::{Array, Int64Array, StringArray};
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    if base_row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let requested = base_row_ids
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let scan = target_table
        .scan()
        .select(vec![
            "_file".to_string(),
            "_pos".to_string(),
            ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        ])
        .build()
        .map_err(|e| format!("build iceberg MV target locator scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg MV target locator files failed: {e}"))?;
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.deletes.clear();
            task.predicate = None;
            task
        })
    });
    let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("read iceberg MV target locator scan failed: {e}"))?;

    let mut matches = std::collections::HashMap::<i64, (String, i64)>::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg MV target locator scan error: {e}"))?;
        let schema = batch.schema();
        let file_idx = schema
            .index_of("_file")
            .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
        let pos_idx = schema
            .index_of("_pos")
            .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
        let key_idx = schema.index_of(ICEBERG_MV_APPLY_KEY_COLUMN).map_err(|e| {
            format!("iceberg MV target locator scan missing {ICEBERG_MV_APPLY_KEY_COLUMN}: {e}")
        })?;
        let file_col =
            arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
                .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
        let pos_col =
            arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| {
                    format!("cast target {ICEBERG_MV_APPLY_KEY_COLUMN} to BIGINT failed: {e}")
                })?;
        let files = file_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
        let positions = pos_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
        let keys = key_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!("target {ICEBERG_MV_APPLY_KEY_COLUMN} is not BIGINT after cast")
            })?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let key = keys.value(row);
            if !requested.contains(&key) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::engine::delete_flow::data_file_row_is_visible(
                &batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            if matches.insert(key, (file.to_string(), pos)).is_some() {
                return Err(format!(
                    "iceberg MV target has duplicate rows for base row id {key}"
                ));
            }
        }
    }

    for key in &requested {
        if !matches.contains_key(key) {
            return Err(format!(
                "iceberg MV target row not found for base row id {key}"
            ));
        }
    }

    let mut by_file = std::collections::BTreeMap::<String, Vec<i64>>::new();
    for (_key, (file, pos)) in matches {
        by_file.entry(file).or_default().push(pos);
    }
    by_file
        .into_iter()
        .map(|(referenced_data_file, mut positions)| {
            positions.sort_unstable();
            let partition = referenced_data_file_partitions
                .get(&referenced_data_file)
                .ok_or_else(|| {
                    format!(
                        "matched iceberg MV target data file `{referenced_data_file}` is missing partition metadata"
                    )
                })?;
            Ok(crate::connector::iceberg::commit::PositionDeleteGroup {
                referenced_data_file,
                partition_spec_id: partition.partition_spec_id,
                partition_values: partition.partition_values.clone(),
                positions,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_key_table_column_is_required_bigint() {
        let column = apply_key_table_column();

        assert_eq!(column.name, "__nova_base_row_id");
        assert_eq!(column.data_type, crate::sql::parser::ast::SqlType::BigInt);
        assert!(!column.nullable);
        assert!(column.aggregation.is_none());
        assert!(column.default.is_none());
    }

    #[test]
    fn iceberg_mv_physical_select_appends_base_row_id() {
        let sql =
            iceberg_mv_physical_select_sql("SELECT id, amount FROM ice.ns.orders WHERE amount > 0")
                .expect("physical sql");

        assert_eq!(
            sql,
            "SELECT id, amount, _row_id AS __nova_base_row_id FROM ice.ns.orders WHERE amount > 0"
        );
    }

    #[test]
    fn iceberg_mv_physical_select_rejects_star_projection() {
        let err = iceberg_mv_physical_select_sql("SELECT * FROM ice.ns.orders")
            .expect_err("star projection must fail");

        assert!(err.contains("explicit projection columns"), "{err}");
    }

    #[test]
    fn iceberg_mv_physical_select_rejects_visible_apply_key_collision() {
        let err =
            iceberg_mv_physical_select_sql("SELECT id AS __nova_base_row_id FROM ice.ns.orders")
                .expect_err("reserved alias must fail");

        assert!(err.contains("__nova_base_row_id"), "{err}");
        assert!(err.contains("reserved"), "{err}");
    }
}
