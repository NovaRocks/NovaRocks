pub(crate) const ICEBERG_MV_APPLY_KEY_COLUMN: &str = "__nova_base_row_id";
pub(crate) const ICEBERG_MV_JOIN_APPLY_KEY_COLUMN: &str = "__nova_join_row_key";
pub(crate) const ICEBERG_MV_GROUP_APPLY_KEY_COLUMN: &str = "__row_id__";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID: &str = "base._row_id";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY: &str = "JoinRowKey";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID: &str = "GroupRowId";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_COLUMN: &str = "novarocks.mv.apply-key.column";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_SOURCE: &str = "novarocks.mv.apply-key.source";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID: &str = "novarocks.mv.apply-key.field-id";
pub(crate) const ICEBERG_MV_PROP_HIDDEN_COLUMNS: &str = "novarocks.mv.hidden-columns";

pub(crate) fn apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::BigInt,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn join_apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::String,
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
    find_apply_key_field_id_by_column(table, ICEBERG_MV_APPLY_KEY_COLUMN)
}

pub(crate) fn find_apply_key_field_id_by_column(
    table: &iceberg::table::Table,
    apply_key_column: &str,
) -> Result<i32, String> {
    let mut matches = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(apply_key_column));
    let Some(field) = matches.next() else {
        return Err(format!(
            "iceberg MV target schema is missing apply-key column {apply_key_column}"
        ));
    };
    if matches.next().is_some() {
        return Err(format!(
            "iceberg MV target schema has duplicate apply-key column {apply_key_column}"
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
    locate_target_rows_by_apply_key_impl(
        target_table,
        ICEBERG_MV_APPLY_KEY_COLUMN,
        ApplyKeyRequest::Int64(base_row_ids),
        existing_deletes_by_file,
        referenced_data_file_partitions,
    )
    .await
}

pub(crate) async fn locate_target_rows_by_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::Utf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
    )
    .await
}

#[derive(Clone, Copy)]
enum ApplyKeyRequest<'a> {
    Int64(&'a [i64]),
    Utf8(&'a [String]),
}

impl ApplyKeyRequest<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Int64(keys) => keys.is_empty(),
            Self::Utf8(keys) => keys.is_empty(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ApplyKeyValue {
    Int64(i64),
    Utf8(String),
}

impl std::fmt::Display for ApplyKeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64(value) => write!(f, "{value}"),
            Self::Utf8(value) => write!(f, "{value}"),
        }
    }
}

fn requested_apply_key_values(
    requested_keys: ApplyKeyRequest<'_>,
) -> std::collections::HashSet<ApplyKeyValue> {
    match requested_keys {
        ApplyKeyRequest::Int64(keys) => keys
            .iter()
            .copied()
            .map(ApplyKeyValue::Int64)
            .collect::<std::collections::HashSet<_>>(),
        ApplyKeyRequest::Utf8(keys) => keys
            .iter()
            .cloned()
            .map(ApplyKeyValue::Utf8)
            .collect::<std::collections::HashSet<_>>(),
    }
}

fn record_visible_apply_key_match(
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    key: ApplyKeyValue,
    file: &str,
    pos: i64,
) -> Result<(), String> {
    if !requested.contains(&key) {
        return Ok(());
    }
    if matches
        .insert(key.clone(), (file.to_string(), pos))
        .is_some()
    {
        return Err(format!(
            "iceberg MV target has duplicate rows for apply key {key}"
        ));
    }
    Ok(())
}

fn ensure_all_requested_apply_keys_matched(
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &std::collections::HashMap<ApplyKeyValue, (String, i64)>,
) -> Result<(), String> {
    for key in requested {
        if !matches.contains_key(key) {
            return Err(format!(
                "iceberg MV target row not found for apply key {key}"
            ));
        }
    }
    Ok(())
}

fn process_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    apply_key_column: &str,
    request_is_i64: bool,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let key_idx = schema
        .index_of(apply_key_column)
        .map_err(|e| format!("iceberg MV target locator scan missing {apply_key_column}: {e}"))?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let files = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
    let positions = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
    if request_is_i64 {
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| format!("cast target {apply_key_column} to BIGINT failed: {e}"))?;
        let keys = key_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| format!("target {apply_key_column} is not BIGINT after cast"))?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::engine::delete_flow::data_file_row_is_visible(
                batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            record_visible_apply_key_match(
                matches,
                requested,
                ApplyKeyValue::Int64(keys.value(row)),
                file,
                pos,
            )?;
        }
    } else {
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Utf8)
                .map_err(|e| format!("cast target {apply_key_column} to STRING failed: {e}"))?;
        let keys = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("target {apply_key_column} is not STRING after cast"))?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::engine::delete_flow::data_file_row_is_visible(
                batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            record_visible_apply_key_match(
                matches,
                requested,
                ApplyKeyValue::Utf8(keys.value(row).to_string()),
                file,
                pos,
            )?;
        }
    }
    Ok(())
}

fn build_position_delete_groups_from_apply_key_matches(
    matches: std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
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

async fn locate_target_rows_by_apply_key_impl(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    if requested_keys.is_empty() {
        return Ok(Vec::new());
    }

    let requested = requested_apply_key_values(requested_keys);
    let request_is_i64 = matches!(requested_keys, ApplyKeyRequest::Int64(_));
    let scan = target_table
        .scan()
        .select(vec![
            "_file".to_string(),
            "_pos".to_string(),
            apply_key_column.to_string(),
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

    let mut matches = std::collections::HashMap::<ApplyKeyValue, (String, i64)>::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg MV target locator scan error: {e}"))?;
        process_apply_key_locator_batch(
            &batch,
            apply_key_column,
            request_is_i64,
            &requested,
            &mut matches,
            existing_deletes_by_file,
        )?;
    }

    ensure_all_requested_apply_keys_matched(&requested, &matches)?;
    build_position_delete_groups_from_apply_key_matches(matches, referenced_data_file_partitions)
}

pub(crate) async fn locate_target_rows_by_apply_key_string(
    target_table: &iceberg::table::Table,
    join_row_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_string_apply_key(
        target_table,
        ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        join_row_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::Struct;
    use std::sync::Arc;

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
    fn join_apply_key_table_column_is_required_string() {
        let column = join_apply_key_table_column();

        assert_eq!(column.name, "__nova_join_row_key");
        assert_eq!(column.data_type, crate::sql::parser::ast::SqlType::String);
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

    #[test]
    fn apply_key_match_helper_accepts_exact_utf8_requested_key() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("match");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("complete");

        assert_eq!(
            matches.get(&ApplyKeyValue::Utf8("group-1".to_string())),
            Some(&("file-a.parquet".to_string(), 7))
        );
    }

    #[test]
    fn apply_key_match_helper_ignores_unrequested_utf8_key_and_reports_missing() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-2".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("unrequested ignored");
        let err = ensure_all_requested_apply_keys_matched(&requested, &matches).unwrap_err();

        assert!(err.contains("group-1"), "err={err}");
    }

    #[test]
    fn apply_key_match_helper_rejects_duplicate_utf8_target_rows() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("first match");
        let err = record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-b.parquet",
            9,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("group-1"), "err={err}");
    }

    #[test]
    fn utf8_locator_scan_path_returns_position_delete_group_for_requested_key() {
        let batch = utf8_locator_batch(&[
            ("file-a.parquet", 7, "group-1"),
            ("file-b.parquet", 9, "group-2"),
        ]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_apply_key_locator_batch(
            &batch,
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("requested key");
        let groups =
            build_position_delete_groups_from_apply_key_matches(matches, &referenced_partitions())
                .expect("delete groups");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, "file-a.parquet");
        assert_eq!(groups[0].partition_spec_id, 0);
        assert_eq!(groups[0].positions, vec![7]);
    }

    #[test]
    fn utf8_locator_scan_path_ignores_unrequested_rows_and_errors_on_missing_key() {
        let batch = utf8_locator_batch(&[("file-b.parquet", 9, "group-2")]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_apply_key_locator_batch(
            &batch,
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        let err = ensure_all_requested_apply_keys_matched(&requested, &matches).unwrap_err();

        assert!(err.contains("group-1"), "err={err}");
        assert!(matches.is_empty());
    }

    #[test]
    fn utf8_locator_scan_path_errors_on_duplicate_visible_target_rows() {
        let batch = utf8_locator_batch(&[
            ("file-a.parquet", 7, "group-1"),
            ("file-b.parquet", 9, "group-1"),
        ]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_apply_key_locator_batch(
            &batch,
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("group-1"), "err={err}");
    }

    fn utf8_locator_batch(rows: &[(&str, i32, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(ICEBERG_MV_GROUP_APPLY_KEY_COLUMN, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _)| *pos),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("locator batch")
    }

    fn referenced_partitions() -> crate::engine::delete_flow::ReferencedDataFilePartitions {
        let mut partitions = std::collections::HashMap::new();
        for file in ["file-a.parquet", "file-b.parquet"] {
            partitions.insert(
                file.to_string(),
                crate::engine::delete_flow::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: Struct::empty(),
                },
            );
        }
        partitions
    }
}
