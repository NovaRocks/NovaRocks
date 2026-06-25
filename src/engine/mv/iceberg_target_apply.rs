use crate::engine::mv::partition::TargetPartitionFilter;

pub(crate) const ICEBERG_MV_APPLY_KEY_COLUMN: &str = "__nova_base_row_id";
pub(crate) const ICEBERG_MV_JOIN_APPLY_KEY_COLUMN: &str = "__nova_join_row_key";
pub(crate) const ICEBERG_MV_BRANCH_ID_COLUMN: &str = "__branch_id__";
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

pub(crate) fn branch_id_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::Int,
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
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_apply_key_with_matches(
        target_table,
        base_row_ids,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

pub(crate) async fn locate_target_rows_by_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        ICEBERG_MV_APPLY_KEY_COLUMN,
        ApplyKeyRequest::Int64(base_row_ids),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

pub(crate) async fn locate_target_rows_by_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_string_apply_key_with_matches(
        target_table,
        apply_key_column,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

pub(crate) async fn locate_target_rows_by_string_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::Utf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BranchApplyKey {
    pub branch_id: i32,
    pub base_row_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BranchStringApplyKey {
    pub branch_id: i32,
    pub key: String,
}

pub(crate) async fn locate_target_rows_by_branch_apply_key(
    target_table: &iceberg::table::Table,
    requested_keys: &[BranchApplyKey],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_branch_apply_key_with_matches(
        target_table,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

pub(crate) async fn locate_target_rows_by_branch_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    requested_keys: &[BranchApplyKey],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        ICEBERG_MV_APPLY_KEY_COLUMN,
        ApplyKeyRequest::BranchInt64(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

pub(crate) async fn locate_target_rows_by_branch_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[BranchStringApplyKey],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_branch_string_apply_key_with_matches(
        target_table,
        apply_key_column,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

pub(crate) async fn locate_target_rows_by_branch_string_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[BranchStringApplyKey],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::BranchUtf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[derive(Clone, Copy)]
enum ApplyKeyRequest<'a> {
    Int64(&'a [i64]),
    Utf8(&'a [String]),
    BranchInt64(&'a [BranchApplyKey]),
    BranchUtf8(&'a [BranchStringApplyKey]),
}

impl ApplyKeyRequest<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Int64(keys) => keys.is_empty(),
            Self::Utf8(keys) => keys.is_empty(),
            Self::BranchInt64(keys) => keys.is_empty(),
            Self::BranchUtf8(keys) => keys.is_empty(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ApplyKeyValue {
    Int64(i64),
    Utf8(String),
    BranchInt64(BranchApplyKey),
    BranchUtf8(BranchStringApplyKey),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetRowPositionSet {
    pub(crate) referenced_data_file: String,
    pub(crate) positions: Vec<i64>,
}

pub(crate) struct TargetApplyLocatorResult {
    pub(crate) delete_groups: Vec<crate::connector::iceberg::commit::PositionDeleteGroup>,
    pub(crate) matched_positions: Vec<TargetRowPositionSet>,
}

impl std::fmt::Display for ApplyKeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64(value) => write!(f, "{value}"),
            Self::Utf8(value) => write!(f, "{value}"),
            Self::BranchInt64(value) => {
                write!(
                    f,
                    "branch {} apply key {}",
                    value.branch_id, value.base_row_id
                )
            }
            Self::BranchUtf8(value) => {
                write!(f, "branch {} apply key {}", value.branch_id, value.key)
            }
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
        ApplyKeyRequest::BranchInt64(keys) => keys
            .iter()
            .copied()
            .map(ApplyKeyValue::BranchInt64)
            .collect::<std::collections::HashSet<_>>(),
        ApplyKeyRequest::BranchUtf8(keys) => keys
            .iter()
            .cloned()
            .map(ApplyKeyValue::BranchUtf8)
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

fn process_branch_i64_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let branch_idx = schema.index_of(ICEBERG_MV_BRANCH_ID_COLUMN).map_err(|e| {
        format!("iceberg MV target locator scan missing {ICEBERG_MV_BRANCH_ID_COLUMN}: {e}")
    })?;
    let key_idx = schema.index_of(ICEBERG_MV_APPLY_KEY_COLUMN).map_err(|e| {
        format!("iceberg MV target locator scan missing {ICEBERG_MV_APPLY_KEY_COLUMN}: {e}")
    })?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let branch_col =
        arrow::compute::cast(batch.column(branch_idx), &arrow::datatypes::DataType::Int32)
            .map_err(|e| format!("cast target {ICEBERG_MV_BRANCH_ID_COLUMN} to INT failed: {e}"))?;
    let key_col = arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Int64)
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
    let branches = branch_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| format!("target {ICEBERG_MV_BRANCH_ID_COLUMN} is not INT after cast"))?;
    let keys = key_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("target {ICEBERG_MV_APPLY_KEY_COLUMN} is not BIGINT after cast"))?;

    for row in 0..batch.num_rows() {
        if files.is_null(row)
            || positions.is_null(row)
            || branches.is_null(row)
            || keys.is_null(row)
        {
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
            ApplyKeyValue::BranchInt64(BranchApplyKey {
                branch_id: branches.value(row),
                base_row_id: keys.value(row),
            }),
            file,
            pos,
        )?;
    }
    Ok(())
}

fn process_branch_utf8_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    apply_key_column: &str,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let branch_idx = schema.index_of(ICEBERG_MV_BRANCH_ID_COLUMN).map_err(|e| {
        format!("iceberg MV target locator scan missing {ICEBERG_MV_BRANCH_ID_COLUMN}: {e}")
    })?;
    let key_idx = schema
        .index_of(apply_key_column)
        .map_err(|e| format!("iceberg MV target locator scan missing {apply_key_column}: {e}"))?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let branch_col =
        arrow::compute::cast(batch.column(branch_idx), &arrow::datatypes::DataType::Int32)
            .map_err(|e| format!("cast target {ICEBERG_MV_BRANCH_ID_COLUMN} to INT failed: {e}"))?;
    let key_col = arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target {apply_key_column} to STRING failed: {e}"))?;
    let files = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
    let positions = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
    let branches = branch_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| format!("target {ICEBERG_MV_BRANCH_ID_COLUMN} is not INT after cast"))?;
    let keys = key_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("target {apply_key_column} is not STRING after cast"))?;

    for row in 0..batch.num_rows() {
        if files.is_null(row)
            || positions.is_null(row)
            || branches.is_null(row)
            || keys.is_null(row)
        {
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
            ApplyKeyValue::BranchUtf8(BranchStringApplyKey {
                branch_id: branches.value(row),
                key: keys.value(row).to_string(),
            }),
            file,
            pos,
        )?;
    }
    Ok(())
}

fn build_position_delete_groups_from_apply_key_matches(
    matches: std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(build_target_apply_locator_result_from_apply_key_matches(
        matches,
        referenced_data_file_partitions,
    )?
    .delete_groups)
}

fn build_target_apply_locator_result_from_apply_key_matches(
    matches: std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<TargetApplyLocatorResult, String> {
    let mut by_file = std::collections::BTreeMap::<String, Vec<i64>>::new();
    for (_key, (file, pos)) in matches {
        by_file.entry(file).or_default().push(pos);
    }

    let mut delete_groups = Vec::with_capacity(by_file.len());
    let mut matched_positions = Vec::with_capacity(by_file.len());
    for (referenced_data_file, mut positions) in by_file {
        positions.sort_unstable();
        let partition = referenced_data_file_partitions
            .get(&referenced_data_file)
            .ok_or_else(|| {
                format!(
                    "matched iceberg MV target data file `{referenced_data_file}` is missing partition metadata"
                )
            })?;
        matched_positions.push(TargetRowPositionSet {
            referenced_data_file: referenced_data_file.clone(),
            positions: positions.clone(),
        });
        delete_groups.push(crate::connector::iceberg::commit::PositionDeleteGroup {
            referenced_data_file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions,
        });
    }

    Ok(TargetApplyLocatorResult {
        delete_groups,
        matched_positions,
    })
}

async fn locate_target_rows_by_apply_key_impl(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    if requested_keys.is_empty() {
        return Ok(TargetApplyLocatorResult {
            delete_groups: Vec::new(),
            matched_positions: Vec::new(),
        });
    }

    let requested = requested_apply_key_values(requested_keys);
    let request_is_i64 = matches!(requested_keys, ApplyKeyRequest::Int64(_));
    let mut select_columns = vec!["_file".to_string(), "_pos".to_string()];
    if matches!(
        requested_keys,
        ApplyKeyRequest::BranchInt64(_) | ApplyKeyRequest::BranchUtf8(_)
    ) {
        select_columns.push(ICEBERG_MV_BRANCH_ID_COLUMN.to_string());
    }
    select_columns.push(apply_key_column.to_string());
    let scan = target_table
        .scan()
        .select(select_columns)
        .build()
        .map_err(|e| format!("build iceberg MV target locator scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg MV target locator files failed: {e}"))?;
    let target_metadata = target_table.metadata_ref();
    let filter_owned = partition_filter.clone();
    let cleaned_tasks = task_stream.map(move |task_result| {
        let mut task = task_result?;
        task.deletes.clear();
        task.predicate = None;
        if filter_owned.is_allow_list() {
            let Some(partition_struct) = task.partition.as_ref() else {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: file scan task for data file `{}` is missing partition metadata",
                        task.data_file_path
                    ),
                ));
            };
            // iceberg-rust 0.9 always sets partition_spec = None in FileScanTask
            // (library TODO in scan/context.rs:139).  Fall back to the table's
            // default partition spec id so the call never errors unconditionally.
            let spec_id = task
                .partition_spec
                .as_ref()
                .map(|spec| spec.spec_id())
                .unwrap_or_else(|| target_metadata.default_partition_spec().spec_id());
            let values = crate::connector::iceberg::changes::change_partition_field_values(
                &target_metadata,
                spec_id,
                partition_struct,
            )
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: cannot derive partition values for `{}`: {e}",
                        task.data_file_path
                    ),
                )
            })?;
            let mut fields = Vec::with_capacity(values.len());
            for value in &values {
                let mv_value =
                    crate::engine::mv::partition::mapping::change_partition_value_to_mv_value(
                        &task.data_file_path,
                        &value.value,
                    )
                    .map_err(|e| {
                        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e)
                    })?;
                fields.push(crate::engine::mv::partition::MvPartitionKeyField::new(
                    value.field_name.clone(),
                    mv_value,
                ));
            }
            // Use the allow-list's own spec_id as the canonical key spec_id.
            // The AllowList is built from the schema contract's target_spec_id,
            // which may differ from the table's raw default spec_id when the
            // contract was persisted before a partition spec evolution.
            // All keys in a single allow-list share the same spec_id (they come
            // from one contract refresh pass), so picking any key's spec_id is
            // safe.  For an empty allow-list, fall back to spec_id derived above;
            // filter_owned.matches will then return false (empty set has no
            // members), so the task is correctly dropped.
            let key_spec_id = match &filter_owned {
                TargetPartitionFilter::AllowList(set) => {
                    set.iter().next().map(|k| k.spec_id).unwrap_or(spec_id)
                }
                TargetPartitionFilter::None => spec_id,
            };
            let key = crate::engine::mv::partition::MvPartitionKey::new(key_spec_id, fields);
            if !filter_owned.matches(&key) {
                return Ok(None);
            }
        }
        Ok(Some(task))
    });
    let cleaned_tasks = cleaned_tasks.filter_map(|task_or_skip| async move {
        match task_or_skip {
            Ok(Some(task)) => Some(Ok(task)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
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
        if matches!(requested_keys, ApplyKeyRequest::BranchInt64(_)) {
            process_branch_i64_apply_key_locator_batch(
                &batch,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        } else if matches!(requested_keys, ApplyKeyRequest::BranchUtf8(_)) {
            process_branch_utf8_apply_key_locator_batch(
                &batch,
                apply_key_column,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        } else {
            process_apply_key_locator_batch(
                &batch,
                apply_key_column,
                request_is_i64,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        }
    }

    ensure_all_requested_apply_keys_matched(&requested, &matches)?;
    build_target_apply_locator_result_from_apply_key_matches(
        matches,
        referenced_data_file_partitions,
    )
}

pub(crate) async fn locate_target_rows_by_apply_key_string(
    target_table: &iceberg::table::Table,
    join_row_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_string_apply_key(
        target_table,
        ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        join_row_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::TargetPartitionFilter;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::Struct;
    use std::sync::Arc;

    /// Build a minimal `MemoryCatalog`-backed iceberg table that can serve as
    /// the target for `locate_target_rows_by_apply_key` tests.  The table has
    /// a single `i64` column named `ICEBERG_MV_APPLY_KEY_COLUMN` and no data
    /// files.  The `_row_ids` slice is accepted for future extension but
    /// currently unused (no data is written; tests that exercise the no-request
    /// path need an empty target table).
    fn build_memory_iceberg_apply_key_target(_row_ids: &[i64]) -> iceberg::table::Table {
        use iceberg::Catalog;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
        };
        use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
        use std::collections::HashMap;
        use uuid::Uuid;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let warehouse = format!("memory://test-warehouse-{}", Uuid::new_v4());
            let catalog = MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .expect("MemoryCatalog::load");

            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("create_namespace");

            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        ICEBERG_MV_APPLY_KEY_COLUMN,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .expect("build schema");

            let table_ident = TableIdent::new(namespace.clone(), "mv_target".to_string());
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("mv_target".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create_table");

            catalog.load_table(&table_ident).await.expect("load_table")
        })
    }

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
    fn branch_id_table_column_is_required_int() {
        let col = branch_id_table_column();
        assert_eq!(col.name, ICEBERG_MV_BRANCH_ID_COLUMN);
        assert_eq!(col.name, "__branch_id__");
        assert!(!col.nullable);
        assert!(matches!(
            col.data_type,
            crate::sql::parser::ast::SqlType::Int
        ));
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
    fn locator_result_preserves_sorted_matched_positions() {
        let mut matches = std::collections::HashMap::new();
        matches.insert(
            ApplyKeyValue::Utf8("group-b".to_string()),
            ("file-b.parquet".to_string(), 9),
        );
        matches.insert(
            ApplyKeyValue::Utf8("group-a2".to_string()),
            ("file-a.parquet".to_string(), 3),
        );
        matches.insert(
            ApplyKeyValue::Utf8("group-a1".to_string()),
            ("file-a.parquet".to_string(), 7),
        );

        let result = build_target_apply_locator_result_from_apply_key_matches(
            matches,
            &referenced_partitions(),
        )
        .expect("locator result");

        assert_eq!(result.delete_groups.len(), 2);
        assert_eq!(
            result.delete_groups[0].referenced_data_file,
            "file-a.parquet"
        );
        assert_eq!(result.delete_groups[0].positions, vec![3, 7]);
        assert_eq!(
            result.delete_groups[1].referenced_data_file,
            "file-b.parquet"
        );
        assert_eq!(result.delete_groups[1].positions, vec![9]);
        assert_eq!(
            result.matched_positions,
            vec![
                TargetRowPositionSet {
                    referenced_data_file: "file-a.parquet".to_string(),
                    positions: vec![3, 7],
                },
                TargetRowPositionSet {
                    referenced_data_file: "file-b.parquet".to_string(),
                    positions: vec![9],
                },
            ]
        );
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

    #[test]
    fn branch_apply_key_locator_scan_distinguishes_same_base_row_id_across_branches() {
        let batch = branch_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 0, 42),
            ("file-b.parquet", 9, 1, 42),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_branch_i64_apply_key_locator_batch(
            &batch,
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
        assert_eq!(groups[0].referenced_data_file, "file-b.parquet");
        assert_eq!(groups[0].positions, vec![9]);
    }

    #[test]
    fn branch_scoped_string_key_matches_only_same_branch() {
        let batch = branch_string_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 0, "group-1"),
            ("file-b.parquet", 9, 1, "group-1"),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchUtf8(&[BranchStringApplyKey {
                branch_id: 1,
                key: "group-1".to_string(),
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_branch_utf8_apply_key_locator_batch(
            &batch,
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
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
        assert_eq!(groups[0].referenced_data_file, "file-b.parquet");
        assert_eq!(groups[0].positions, vec![9]);
    }

    #[test]
    fn branch_apply_key_locator_scan_rejects_duplicate_visible_target_rows() {
        let batch = branch_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 1, 42),
            ("file-b.parquet", 9, 1, 42),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_branch_i64_apply_key_locator_batch(
            &batch,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("branch 1"), "err={err}");
        assert!(err.contains("42"), "err={err}");
    }

    #[test]
    fn branch_apply_key_locator_scan_rejects_missing_branch_column() {
        let batch = utf8_locator_batch(&[("file-a.parquet", 7, "group-1")]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_branch_i64_apply_key_locator_batch(
            &batch,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains(ICEBERG_MV_BRANCH_ID_COLUMN), "err={err}");
        assert!(err.contains("missing"), "err={err}");
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

    fn branch_apply_key_locator_batch(rows: &[(&str, i32, i32, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(ICEBERG_MV_BRANCH_ID_COLUMN, DataType::Int32, false),
            Field::new(ICEBERG_MV_APPLY_KEY_COLUMN, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _, _)| *pos),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, _, branch, _)| *branch),
                )) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, _, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("branch locator batch")
    }

    fn branch_string_apply_key_locator_batch(rows: &[(&str, i32, i32, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(ICEBERG_MV_BRANCH_ID_COLUMN, DataType::Int32, false),
            Field::new(ICEBERG_MV_GROUP_APPLY_KEY_COLUMN, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _, _)| *pos),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, _, branch, _)| *branch),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, _, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("branch string locator batch")
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

    #[test]
    fn empty_request_with_filter_none_returns_empty_groups() {
        // No request → no scan → empty groups, regardless of filter shape.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target_table = build_memory_iceberg_apply_key_target(&[]);
        let existing = std::collections::HashMap::new();
        let referenced = std::collections::HashMap::new();
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &TargetPartitionFilter::None,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }

    #[test]
    fn empty_request_with_empty_allow_list_returns_empty_groups() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target_table = build_memory_iceberg_apply_key_target(&[]);
        let existing = std::collections::HashMap::new();
        let referenced = std::collections::HashMap::new();
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &filter,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }

    /// Build a partitioned apply-key target table with two data files:
    ///   region=a → apply_key = "key-a" at position 0
    ///   region=b → apply_key = "key-b" at position 0
    ///
    /// Schema: `ICEBERG_MV_JOIN_APPLY_KEY_COLUMN` (Utf8 required, field_id=1),
    ///         `region` (Utf8 optional, field_id=2).
    /// Partition spec: identity(region), bound spec_id=0.
    ///
    struct PartitionedApplyKeyTargetFixture {
        table: iceberg::table::Table,
        file_paths: Vec<String>,
        _catalog: std::sync::Arc<dyn iceberg::Catalog>,
        _warehouse_dir: tempfile::TempDir,
    }

    /// Returns a real MV-target-shaped Iceberg table fixture. The tempdir and
    /// catalog guards must stay alive while the table is scanned.
    fn build_partitioned_apply_key_target_with_rows() -> PartitionedApplyKeyTargetFixture {
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
            UnboundPartitionSpec,
        };
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        use iceberg::{NamespaceIdent, TableCreation, TableIdent};
        use uuid::Uuid;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let warehouse_dir = tempfile::Builder::new()
            .prefix("novarocks-target-apply-")
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse = format!("file://{}", warehouse_dir.path().display());
        let (table, file_paths, catalog) = rt.block_on(async {
            let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("build hadoop catalog entry");
            let catalog =
                crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
                    .expect("build hadoop catalog");

            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("create_namespace");

            // Schema: apply_key (String, required), region (String, optional).
            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .expect("build schema");

            // Partition spec: identity(region) using source field_id=2.
            let partition_spec = UnboundPartitionSpec::builder()
                .add_partition_field(2, "region", Transform::Identity)
                .expect("add partition field")
                .build();

            let table_ident = TableIdent::new(namespace.clone(), "mv_apply_target".to_string());
            let table = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("mv_apply_target".to_string())
                        .schema(schema)
                        .partition_spec(partition_spec)
                        .properties([
                            ("write.row-lineage".to_string(), "true".to_string()),
                            (
                                ICEBERG_MV_PROP_APPLY_KEY_COLUMN.to_string(),
                                ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
                            ),
                            (
                                ICEBERG_MV_PROP_APPLY_KEY_SOURCE.to_string(),
                                ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY.to_string(),
                            ),
                            (
                                ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID.to_string(),
                                "1".to_string(),
                            ),
                        ])
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create_table");

            // Two batches: region=a and region=b, each with one apply_key row.
            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, true),
            ]));
            let batch_a = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["key-a"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                ],
            )
            .expect("batch_a");
            let batch_b = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec!["key-b"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
                ],
            )
            .expect("batch_b");

            // Write region=a first, then region=b.  The writer produces one data
            // file per partition, so data_files[0] = region=a and
            // data_files[1] = region=b.
            let data_files =
                crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                    &table,
                    vec![batch_a, batch_b],
                )
                .await
                .expect("write data files");
            assert_eq!(data_files.len(), 2, "expected one data file per partition");

            let file_paths: Vec<String> = data_files
                .iter()
                .map(|f| f.file_path().to_string())
                .collect();

            // Commit both data files via fast_append.
            let tx = Transaction::new(&table);
            let action = tx
                .fast_append()
                .add_data_files(data_files)
                .set_commit_uuid(Uuid::new_v4());
            let tx = action.apply(tx).expect("fast_append apply");
            let _table_after: iceberg::table::Table = tx
                .commit(catalog.as_ref())
                .await
                .expect("fast_append commit");

            let refreshed = catalog
                .load_table(&table_ident)
                .await
                .expect("reload table");
            (refreshed, file_paths, catalog)
        });
        PartitionedApplyKeyTargetFixture {
            table,
            file_paths,
            _catalog: catalog,
            _warehouse_dir: warehouse_dir,
        }
    }

    fn loaded_partitioned_apply_key_target(
        target_table: &iceberg::table::Table,
    ) -> crate::connector::iceberg::catalog::IcebergLoadedTable {
        crate::connector::iceberg::catalog::IcebergLoadedTable {
            table: target_table.clone(),
            columns: vec![
                crate::engine::ColumnDef {
                    name: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::engine::ColumnDef {
                    name: "region".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            logical_types: std::collections::HashMap::new(),
            key_desc: None,
            column_aggregations: std::collections::HashMap::new(),
            object_store_config: None,
        }
    }

    fn assert_standard_mv_target_table_def_hides_physical_apply_key(
        table_def: &crate::sql::catalog::TableDef,
    ) {
        assert!(
            table_def.columns.iter().all(|column| !column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_JOIN_APPLY_KEY_COLUMN)),
            "standard MV target registration must hide the physical apply-key column"
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_file")
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_pos")
        );
    }

    fn expose_physical_apply_key_for_locator_test_registration(
        mut table_def: crate::sql::catalog::TableDef,
    ) -> crate::sql::catalog::TableDef {
        assert_standard_mv_target_table_def_hides_physical_apply_key(&table_def);
        table_def.columns.insert(
            0,
            crate::engine::ColumnDef {
                name: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
        );
        table_def
    }

    #[test]
    fn spike_framework_select_file_pos_on_target() {
        use arrow::array::{Int64Array, StringArray};

        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_partitioned_apply_key_target_with_rows();
        let target_table = &fixture.table;
        let file_paths = &fixture.file_paths;
        assert_eq!(
            target_table.metadata().format_version(),
            iceberg::spec::FormatVersion::V3
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get("write.row-lineage")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(ICEBERG_MV_PROP_APPLY_KEY_COLUMN)
                .map(String::as_str),
            Some(ICEBERG_MV_JOIN_APPLY_KEY_COLUMN)
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(ICEBERG_MV_PROP_APPLY_KEY_SOURCE)
                .map(String::as_str),
            Some(ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY)
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID)
                .map(String::as_str),
            Some("1")
        );

        let snapshot_id = target_table
            .metadata()
            .current_snapshot()
            .expect("target snapshot")
            .snapshot_id();
        let data_files =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                target_table,
                snapshot_id,
            )
            .expect("extract target data files");
        assert_eq!(data_files.len(), 2, "expected one data file per partition");

        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    target_table
                        .metadata()
                        .location()
                        .strip_suffix("/db/mv_apply_target")
                        .expect("target table location under warehouse")
                        .to_string(),
                ),
            ],
        )
        .expect("build iceberg catalog entry");

        let standard_table_def =
            crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
                &entry,
                "ice",
                "db",
                "mv_target",
                loaded_partitioned_apply_key_target(target_table),
                data_files,
            )
            .expect("build standard target table def");
        assert_standard_mv_target_table_def_hides_physical_apply_key(&standard_table_def);

        let table_def = expose_physical_apply_key_for_locator_test_registration(standard_table_def);
        assert!(
            table_def.columns.iter().any(|column| column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_JOIN_APPLY_KEY_COLUMN)),
            "locator/test registration must expose the physical apply-key column"
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_file")
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_pos")
        );
        {
            let mut catalog_guard = state.catalog.write().expect("standalone catalog");
            catalog_guard.create_database("db").expect("create db");
            catalog_guard
                .register("db", table_def)
                .expect("register target table def");
        }

        let session = crate::engine::StandaloneSession {
            inner: std::sync::Arc::clone(&state),
        };
        let sql = format!(
            "SELECT _file, _pos, {apply_key} \
             FROM db.mv_target \
             WHERE {apply_key} IN ('key-a')",
            apply_key = ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
        );
        let result = match session
            .execute_in_context(&sql, None, "db", None)
            .expect("framework SELECT")
        {
            crate::engine::StatementResult::Query(result) => result,
            crate::engine::StatementResult::Ok => panic!("SELECT returned Ok"),
        };

        assert_eq!(result.row_count(), 1, "result={result:?}");
        let chunk = result
            .chunks
            .iter()
            .find(|chunk| chunk.batch.num_rows() == 1)
            .expect("one-row chunk");
        let file = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("_file utf8");
        let pos = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_pos int64");
        let apply_key = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("apply-key utf8");

        assert!(
            file.value(0).contains(&file_paths[0]),
            "_file={} file_a={}",
            file.value(0),
            file_paths[0]
        );
        assert_eq!(pos.value(0), 0);
        assert_eq!(apply_key.value(0), "key-a");
        drop(loopback_backend);
    }

    /// Verify that the AllowList pruning path:
    ///   (a) does not error when `task.partition_spec` is None (iceberg-rust 0.9
    ///       always sets it to None — library TODO in scan/context.rs:139),
    ///   (b) correctly uses the contract's `target_spec_id` (here: 7) rather
    ///       than the table's raw default spec_id (here: 0) when constructing
    ///       the comparison key, so that the allow-list lookup succeeds.
    ///
    /// The test builds a two-partition table (region=a, region=b) and calls the
    /// locator with an AllowList whose single key carries `spec_id=7` (the
    /// contract spec_id) and `region=a`.  Only the region=a file passes the
    /// filter; exactly one PositionDeleteGroup is produced.
    #[test]
    fn allow_list_with_contract_spec_id_keeps_matching_partition() {
        use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = build_partitioned_apply_key_target_with_rows();
        let target_table = &fixture.table;
        let file_paths = &fixture.file_paths;

        // The contract's target_spec_id is 7 — intentionally different from the
        // table's raw default spec_id (0) to reproduce the production mismatch.
        // All keys in one AllowList share the same spec_id (single contract pass).
        const CONTRACT_SPEC_ID: i32 = 7;

        let allow_key = MvPartitionKey::new(
            CONTRACT_SPEC_ID,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String("a".to_string()),
            )],
        );
        let filter = TargetPartitionFilter::AllowList(
            std::iter::once(allow_key).collect::<std::collections::BTreeSet<_>>(),
        );

        // Populate referenced_data_file_partitions for both files so the locator
        // can build PositionDeleteGroups after finding the match.  The table was
        // just created so its only partition spec has id=0.
        let mut referenced: crate::engine::delete_flow::ReferencedDataFilePartitions =
            std::collections::HashMap::new();
        for path in file_paths {
            referenced.insert(
                path.clone(),
                crate::engine::delete_flow::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
            );
        }

        let existing = std::collections::HashMap::new();
        let join_keys = vec!["key-a".to_string()];

        let result = rt
            .block_on(super::locate_target_rows_by_string_apply_key_with_matches(
                target_table,
                ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
                &join_keys,
                &existing,
                &referenced,
                &filter,
            ))
            .expect("locator must not error (old bug triggered: 'missing partition spec')");

        // The AllowList kept region=a and pruned region=b, so exactly one
        // PositionDeleteGroup must be returned.  The referenced file must be
        // one of the two data files (it will be the region=a one), and it must
        // contain exactly one row at position 0.
        assert_eq!(
            result.delete_groups.len(),
            1,
            "expected exactly one delete group (region=b must be pruned by AllowList)"
        );
        assert!(
            file_paths.contains(&result.delete_groups[0].referenced_data_file),
            "delete group references an unknown file: {}",
            result.delete_groups[0].referenced_data_file
        );
        assert_eq!(
            result.delete_groups[0].positions,
            vec![0i64],
            "one row at position 0 in the matched data file"
        );
        assert_eq!(
            result.matched_positions,
            vec![TargetRowPositionSet {
                referenced_data_file: result.delete_groups[0].referenced_data_file.clone(),
                positions: vec![0],
            }],
        );
    }
}
