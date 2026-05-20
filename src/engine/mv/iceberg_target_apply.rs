use crate::engine::mv::partition::TargetPartitionFilter;

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
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
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
    partition_filter: &TargetPartitionFilter,
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
    use arrow::array::{ArrayRef, Int32Array, StringArray};
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
                .create_namespace(&namespace, HashMap::new())
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
    /// Returns `(Table, data_file_paths, Arc<Catalog>)`. The catalog arc must
    /// be kept alive for the duration of the test; `data_file_paths[0]` holds
    /// the region=a file path and `data_file_paths[1]` the region=b path.
    fn build_partitioned_apply_key_target_with_rows() -> (
        iceberg::table::Table,
        Vec<String>,
        std::sync::Arc<dyn iceberg::Catalog>,
    ) {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
            UnboundPartitionSpec,
        };
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
        use std::collections::HashMap;
        use uuid::Uuid;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let warehouse = format!("memory://test-warehouse-{}", Uuid::new_v4());
            let catalog: std::sync::Arc<dyn iceberg::Catalog> = std::sync::Arc::new(
                MemoryCatalogBuilder::default()
                    .load(
                        "memory",
                        HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                    )
                    .await
                    .expect("MemoryCatalog::load"),
            );

            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
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
                        .format_version(FormatVersion::V2)
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
        })
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
        let (target_table, file_paths, _catalog) =
            build_partitioned_apply_key_target_with_rows();

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
        for path in &file_paths {
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

        let groups = rt
            .block_on(super::locate_target_rows_by_string_apply_key(
                &target_table,
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
            groups.len(),
            1,
            "expected exactly one delete group (region=b must be pruned by AllowList)"
        );
        assert!(
            file_paths.contains(&groups[0].referenced_data_file),
            "delete group references an unknown file: {}",
            groups[0].referenced_data_file
        );
        assert_eq!(
            groups[0].positions,
            vec![0i64],
            "one row at position 0 in the matched data file"
        );
    }
}
