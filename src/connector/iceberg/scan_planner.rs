use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::connector::iceberg::catalog::registry::IcebergCatalogRegistry;
use crate::connector::scan_planning::{
    ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle, ScanHandle, Split,
};
use crate::sql::catalog::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    IcebergTableInfo,
};
use crate::{descriptors, exprs, internal_service, partitions, plan_nodes, types};

const CONNECTOR_ID: &str = "iceberg";
const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum IcebergSplitSource {
    CurrentSnapshot,
    ExplicitFiles(Vec<IcebergDataFileInfo>),
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) split_source: IcebergSplitSource,
    pub(crate) column_names: Vec<String>,
}

impl ConnectorTableHandle for IcebergTableHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergScanHandle {
    pub(crate) table: IcebergTableHandle,
}

impl ConnectorScanHandle for IcebergScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergSplit {
    pub(crate) data_file: IcebergDataFileInfo,
}

impl ConnectorSplit for IcebergSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn iceberg_scan_handle(scan: &ScanHandle) -> Result<&IcebergScanHandle, String> {
    scan.downcast_ref::<IcebergScanHandle>()
        .ok_or_else(|| "expected IcebergScanHandle for iceberg scan".to_string())
}

pub(crate) fn iceberg_split(split: &Split) -> Result<&IcebergSplit, String> {
    split
        .downcast_ref::<IcebergSplit>()
        .ok_or_else(|| "expected IcebergSplit for iceberg split".to_string())
}

use crate::connector::scan_planning::{
    BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, TableHandle, ThriftScanContext,
    ThriftScanPlan, validate_split_connectors,
};

#[derive(Default)]
pub(crate) struct IcebergConnectorScanPlanner {
    registry: Option<Arc<RwLock<IcebergCatalogRegistry>>>,
}

impl fmt::Debug for IcebergConnectorScanPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergConnectorScanPlanner")
            .field("has_registry", &self.registry.is_some())
            .finish()
    }
}

impl IcebergConnectorScanPlanner {
    pub(crate) fn new() -> Self {
        Self { registry: None }
    }

    pub(crate) fn with_catalog_registry(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self {
            registry: Some(registry),
        }
    }

    pub(crate) fn table_handle_from_source(
        catalog: &str,
        namespace: &str,
        table: &str,
        snapshot_id: Option<i64>,
        table_info: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        column_names: Vec<String>,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            IcebergTableHandle {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                snapshot_id,
                table_info,
                split_source: IcebergSplitSource::ExplicitFiles(files),
                column_names,
            },
        )
    }

    pub(crate) fn table_handle_for_current_snapshot(
        catalog: &str,
        namespace: &str,
        table: &str,
        table_info: IcebergTableInfo,
        column_names: Vec<String>,
    ) -> TableHandle {
        // CurrentSnapshot is scan intent, not a snapshot pin. Split planning
        // reloads the table's current snapshot through the registry; schema
        // and metadata-evolution validation belongs to catalog/schema checks.
        TableHandle::new(
            CONNECTOR_ID,
            IcebergTableHandle {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                snapshot_id: None,
                table_info,
                split_source: IcebergSplitSource::CurrentSnapshot,
                column_names,
            },
        )
    }

    fn plan_files_for_scan(
        &self,
        table: &IcebergTableHandle,
    ) -> Result<Vec<IcebergDataFileInfo>, String> {
        match &table.split_source {
            IcebergSplitSource::ExplicitFiles(files) => Ok(files.clone()),
            IcebergSplitSource::CurrentSnapshot => self.plan_current_snapshot_files(table),
        }
    }

    fn plan_current_snapshot_files(
        &self,
        table: &IcebergTableHandle,
    ) -> Result<Vec<IcebergDataFileInfo>, String> {
        let registry = self.registry.as_ref().ok_or_else(|| {
            format!(
                "Iceberg current-snapshot scan {}.{}.{} requires a catalog registry",
                table.catalog, table.namespace, table.table
            )
        })?;
        let entry = {
            let guard = registry
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            guard.get(&table.catalog)?
        };
        let loaded = crate::connector::iceberg::catalog::registry::load_table(
            &entry,
            &table.namespace,
            &table.table,
        )?;
        let Some(snapshot_id) = loaded.table.metadata().current_snapshot_id() else {
            return Ok(vec![]);
        };
        let data_files = if let Some(cached) =
            entry.cached_data_files(&table.namespace, &table.table, Some(snapshot_id))?
        {
            cached
        } else {
            let extracted =
                crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                    &loaded.table,
                    snapshot_id,
                )?;
            entry.cache_data_files(
                &table.namespace,
                &table.table,
                Some(snapshot_id),
                extracted.clone(),
            )?;
            extracted
        };
        Ok(data_files
            .into_iter()
            .map(
                crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info,
            )
            .collect())
    }
}

impl ConnectorScanPlanner for IcebergConnectorScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(&self, table: TableHandle, _ctx: BeginScanContext) -> Result<ScanHandle, String> {
        let inner = table
            .downcast_ref::<IcebergTableHandle>()
            .ok_or_else(|| "expected IcebergTableHandle for iceberg scan".to_string())?
            .clone();
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle { table: inner },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = iceberg_scan_handle(scan)?;
        Ok(self
            .plan_files_for_scan(&scan.table)?
            .into_iter()
            .map(|file| Split::new(CONNECTOR_ID, IcebergSplit { data_file: file }))
            .collect())
    }

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String> {
        validate_split_connectors(scan, splits)?;
        let scan = iceberg_scan_handle(scan)?;
        let scan_ranges = build_iceberg_scan_ranges(splits, &ctx)?;
        let node = build_iceberg_hdfs_scan_node(scan, &ctx);
        Ok(ThriftScanPlan {
            node: Some(node),
            scan_ranges,
        })
    }
}

fn build_iceberg_scan_ranges(
    splits: &[Split],
    ctx: &ThriftScanContext,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    let mut ranges = Vec::new();
    for split in splits {
        let file = &iceberg_split(split)?.data_file;
        if !crate::sql::codegen::nodes::file_may_satisfy_min_max(file, &ctx.min_max_predicates) {
            continue;
        }
        ranges.extend(build_hdfs_scan_range_params_for_file(
            file,
            ctx.change_op_slot,
        )?);
    }
    Ok(ranges)
}

fn build_iceberg_hdfs_scan_node(
    scan: &IcebergScanHandle,
    ctx: &ThriftScanContext,
) -> plan_nodes::TPlanNode {
    let mut node = crate::sql::codegen::nodes::default_plan_node();
    node.node_id = ctx.node_id;
    node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![ctx.scan_tuple_id];
    node.nullable_tuples = vec![];
    let min_max_conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    let min_max_tuple_id = min_max_conjuncts.as_ref().map(|_| ctx.scan_tuple_id);
    node.conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    node.compact_data = true;
    node.hdfs_scan_node = Some(plan_nodes::THdfsScanNode::new(
        Some(ctx.scan_tuple_id),
        None::<BTreeMap<types::TTupleId, Vec<exprs::TExpr>>>,
        min_max_conjuncts,
        min_max_tuple_id,
        None::<BTreeMap<types::TSlotId, Vec<i32>>>,
        None::<Vec<exprs::TExpr>>,
        Some(scan.table.column_names.clone()),
        Some(ctx.table.clone()),
        None::<String>,
        None::<String>,
        None::<String>,
        Some(true),
        Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(ctx.cloud_properties.clone()),
            None::<bool>,
        )),
        None::<bool>,
        None::<bool>,
        None::<bool>,
        None::<types::TTupleId>,
        None::<String>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<Vec<partitions::TBucketProperty>>,
        None::<bool>,
        None::<i64>,
        None::<Vec<plan_nodes::TColumnAccessPath>>,
    ));
    node
}

pub(crate) fn build_hdfs_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    change_op_slot: Option<types::TSlotId>,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    validate_iceberg_delete_apply_cost(&file.path, &file.delete_files)?;
    let splits = plan_hdfs_file_splits(file);
    splits
        .into_iter()
        .map(|(offset, length)| {
            build_hdfs_scan_range_params(
                &file.path,
                file.size,
                offset,
                length,
                file.first_row_id,
                file.data_sequence_number,
                file.ivm_change_op,
                file.included_positions.as_ref(),
                change_op_slot,
                &file.delete_files,
            )
        })
        .collect()
}

fn plan_hdfs_file_splits(file: &IcebergDataFileInfo) -> Vec<(i64, i64)> {
    let file_len = file.size.max(0);
    if file_len <= ICEBERG_SCAN_SPLIT_TARGET_BYTES
        || file.first_row_id.is_some()
        || !file.delete_files.is_empty()
        || file.included_positions.is_some()
    {
        return vec![(0, file_len)];
    }

    let mut out = Vec::new();
    let mut offset = 0_i64;
    while offset < file_len {
        let remaining = file_len - offset;
        let length = remaining.min(ICEBERG_SCAN_SPLIT_TARGET_BYTES);
        out.push((offset, length));
        offset += length;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

fn validate_iceberg_delete_apply_cost(
    data_path: &str,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<(), String> {
    if delete_files.len() > ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE {
        return Err(format!(
            "too many Iceberg delete files attached to data file {data_path}: count={} max={}",
            delete_files.len(),
            ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE
        ));
    }
    let total_bytes = delete_files.iter().try_fold(0_i64, |acc, delete_file| {
        let Some(length) = delete_file.length else {
            return Ok(acc);
        };
        acc.checked_add(length.max(0))
            .ok_or_else(|| format!("Iceberg delete file length overflow for data file {data_path}"))
    })?;
    if total_bytes > ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE {
        return Err(format!(
            "Iceberg delete files attached to data file {data_path} are too large: bytes={total_bytes} max={ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE}"
        ));
    }
    Ok(())
}

fn int_literal_expr(value: i64) -> exprs::TExpr {
    exprs::TExpr::new(vec![crate::sql::codegen::expr_compiler::int_literal_node(
        value,
    )])
}

pub(crate) fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    included_positions: Option<&Vec<i64>>,
    change_op_slot: Option<types::TSlotId>,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<internal_service::TScanRangeParams, String> {
    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                let file_content = match delete_file.file_content {
                    IcebergDeleteFileContent::Position => {
                        types::TIcebergFileContent::POSITION_DELETES
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The Thrift scan range only needs to identify the
                        // delete file as an equality-delete file.
                        types::TIcebergFileContent::EQUALITY_DELETES
                    }
                };
                parquet_delete_files.push(plan_nodes::TIcebergDeleteFile::new(
                    Some(delete_file.path.clone()),
                    Some(descriptors::THdfsFileFormat::PARQUET),
                    Some(file_content),
                    delete_file.length,
                ));
            }
            IcebergDeleteFileFormat::Puffin => {
                if deletion_vector_descriptor.is_some() {
                    return Err(format!(
                        "multiple Puffin deletion vectors are attached to data file {}",
                        full_path
                    ));
                }
                let offset = delete_file.content_offset.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_offset",
                        delete_file.path, full_path
                    )
                })?;
                let size = delete_file.content_size_in_bytes.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_size_in_bytes",
                        delete_file.path, full_path
                    )
                })?;
                deletion_vector_descriptor = Some(plan_nodes::TDeletionVectorDescriptor::new(
                    Some("PUFFIN".to_string()),
                    Some(delete_file.path.clone()),
                    Some(offset),
                    Some(size),
                    None::<i64>,
                ));
            }
        }
    }
    let parquet_delete_files = if parquet_delete_files.is_empty() {
        None
    } else {
        Some(parquet_delete_files)
    };
    let extended_columns = match (ivm_change_op, change_op_slot) {
        (Some(op), Some(slot_id)) => {
            crate::exec::change_op::validate_change_op_value(op)?;
            Some(BTreeMap::from([(slot_id, int_literal_expr(op as i64))]))
        }
        _ => None,
    };
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(offset),
        Some(length),
        None::<i64>,
        Some(file_len),
        Some(descriptors::THdfsFileFormat::PARQUET),
        None::<descriptors::TTextFileDesc>,
        Some(full_path.to_string()),
        None::<Vec<String>>,
        None::<bool>,
        parquet_delete_files,
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        extended_columns,
        None::<descriptors::THdfsPartition>,
        None::<types::TTableId>,
        deletion_vector_descriptor,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, exprs::TExprMinMaxValue>>,
        None::<i32>,
        first_row_id,
        data_sequence_number,
        included_positions.cloned(),
    );

    Ok(internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            None::<plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            Some(hdfs_scan_range),
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use crate::connector::scan_planning::{
        ScanHandle, Split, ThriftScanContext, validate_split_connectors,
    };
    use crate::plan_nodes;
    use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::{Literal, SqlType, TableColumnDef};

    fn dummy_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn dummy_iceberg_file() -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: "s3://bucket/data/file.parquet".to_string(),
            size: 1024,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(7),
            schema_id: 0,
            location: "s3://bucket/t".to_string(),
            schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn test_data_file(path: &str) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 1,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn test_data_file_with_stats(
        path: &str,
    ) -> crate::connector::iceberg::catalog::registry::DataFileWithStats {
        crate::connector::iceberg::catalog::registry::DataFileWithStats {
            path: path.to_string(),
            size: 1,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: vec![],
            first_row_id: None,
            data_sequence_number: None,
            delete_files: vec![],
        }
    }

    fn registry_with_empty_table(
        test_name: &str,
    ) -> (
        Arc<RwLock<IcebergCatalogRegistry>>,
        crate::connector::iceberg::catalog::registry::IcebergCatalogEntry,
        tempfile::TempDir,
    ) {
        let warehouse = tempfile::Builder::new()
            .prefix(&format!("novarocks_scan_planner_test_{test_name}_"))
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let registry = Arc::new(RwLock::new(IcebergCatalogRegistry::default()));
        {
            let mut guard = registry.write().expect("iceberg catalog write lock");
            guard
                .create_catalog(
                    "ice",
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                        ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
                    ],
                )
                .expect("create catalog");
        }
        let entry = {
            let guard = registry.read().expect("iceberg catalog read lock");
            guard.get("ice").expect("catalog entry")
        };
        crate::connector::iceberg::catalog::registry::create_namespace(&entry, "db")
            .expect("create namespace");
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            "db",
            "t",
            &[TableColumnDef {
                name: "id".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("create table");
        (registry, entry, warehouse)
    }

    #[test]
    fn current_snapshot_table_handle_does_not_embed_files() {
        let table_info = test_iceberg_table_info();
        let handle = IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
            "ice",
            "db",
            "t",
            table_info,
            vec!["id".to_string()],
        );
        let inner = handle
            .downcast_ref::<IcebergTableHandle>()
            .expect("iceberg table handle");

        assert!(matches!(
            inner.split_source,
            IcebergSplitSource::CurrentSnapshot
        ));
    }

    #[test]
    fn current_snapshot_plan_requires_registry() {
        let planner = IcebergConnectorScanPlanner::new();
        let handle = IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
            "ice",
            "db",
            "t",
            test_iceberg_table_info(),
            vec!["id".to_string()],
        );
        let scan = planner
            .begin_scan(handle, Default::default())
            .expect("begin scan");

        let err = planner
            .plan_splits(&scan, Default::default())
            .expect_err("registry required");

        assert!(
            err.contains("Iceberg current-snapshot scan ice.db.t requires a catalog registry"),
            "{err}"
        );
    }

    #[test]
    fn position_bound_file_carries_included_positions_without_splitting() {
        let mut file = dummy_iceberg_file();
        file.size = ICEBERG_SCAN_SPLIT_TARGET_BYTES + 1;
        file.included_positions = Some(vec![3, 9, 11]);

        let ranges = build_hdfs_scan_range_params_for_file(&file, None).expect("scan ranges");

        assert_eq!(ranges.len(), 1);
        let hdfs_range = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        assert_eq!(hdfs_range.offset, Some(0));
        assert_eq!(hdfs_range.length, Some(file.size));
        assert_eq!(hdfs_range.included_positions, Some(vec![3, 9, 11]));
    }

    #[test]
    fn current_snapshot_empty_table_returns_empty_splits() {
        let (registry, _entry, _warehouse) = registry_with_empty_table("empty_current_snapshot");
        let planner = IcebergConnectorScanPlanner::with_catalog_registry(registry);
        let handle = IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
            "ice",
            "db",
            "t",
            test_iceberg_table_info(),
            vec!["id".to_string()],
        );
        let scan = planner
            .begin_scan(handle, Default::default())
            .expect("begin scan");

        let splits = planner
            .plan_splits(&scan, Default::default())
            .expect("plan splits");

        assert!(splits.is_empty());
    }

    #[test]
    fn current_snapshot_plans_loaded_snapshot_files_and_uses_cache() {
        let (registry, entry, _warehouse) = registry_with_empty_table("current_snapshot_cache");
        crate::connector::iceberg::catalog::registry::insert_rows(
            &entry,
            "db",
            "t",
            &[vec![Literal::Int(1)]],
        )
        .expect("insert row");
        let loaded = crate::connector::iceberg::catalog::registry::load_table(&entry, "db", "t")
            .expect("load table");
        let snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .expect("current snapshot id");

        let planner = IcebergConnectorScanPlanner::with_catalog_registry(registry);
        let handle = IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
            "ice",
            "db",
            "t",
            IcebergTableInfo {
                current_snapshot_id: Some(snapshot_id + 1),
                ..test_iceberg_table_info()
            },
            vec!["id".to_string()],
        );
        let scan = planner
            .begin_scan(handle, Default::default())
            .expect("begin scan");

        let splits = planner
            .plan_splits(&scan, Default::default())
            .expect("plan current snapshot splits");

        assert_eq!(splits.len(), 1);
        let split = iceberg_split(&splits[0]).expect("iceberg split");
        assert_eq!(split.data_file.row_count, Some(1));
        assert!(split.data_file.path.ends_with(".parquet"));
        assert!(
            entry
                .cached_data_files("db", "t", Some(snapshot_id))
                .expect("read cached files")
                .is_some()
        );

        entry
            .cache_data_files(
                "db",
                "t",
                Some(snapshot_id),
                vec![test_data_file_with_stats("file:///cached-snapshot.parquet")],
            )
            .expect("replace cached files");
        let cached_splits = planner
            .plan_splits(&scan, Default::default())
            .expect("plan cached current snapshot splits");

        assert_eq!(cached_splits.len(), 1);
        assert_eq!(
            iceberg_split(&cached_splits[0])
                .expect("cached split")
                .data_file
                .path,
            "file:///cached-snapshot.parquet"
        );
    }

    #[test]
    fn explicit_file_table_handle_preserves_files() {
        let file = test_data_file("s3://bucket/old.parquet");
        let handle = IcebergConnectorScanPlanner::table_handle_from_source(
            "ice",
            "db",
            "t",
            Some(7),
            test_iceberg_table_info(),
            vec![file.clone()],
            vec!["id".to_string()],
        );
        let inner = handle
            .downcast_ref::<IcebergTableHandle>()
            .expect("iceberg table handle");

        let IcebergSplitSource::ExplicitFiles(files) = &inner.split_source else {
            panic!("expected explicit files");
        };
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, file.path);
    }

    #[test]
    fn downcasts_iceberg_scan_and_split() {
        let table = IcebergTableHandle {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(42),
            table_info: dummy_iceberg_table_info(),
            split_source: IcebergSplitSource::ExplicitFiles(vec![dummy_iceberg_file()]),
            column_names: vec!["id".to_string()],
        };
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle {
                table: table.clone(),
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            IcebergSplit {
                data_file: dummy_iceberg_file(),
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(
            iceberg_scan_handle(&scan).expect("scan").table.table,
            "orders"
        );
        assert_eq!(
            iceberg_split(&splits[0]).expect("split").data_file.path,
            "s3://bucket/data/file.parquet"
        );
    }

    #[test]
    fn to_thrift_scan_returns_hdfs_scan_node_and_scan_ranges() {
        let planner = IcebergConnectorScanPlanner::new();
        let table_info = dummy_iceberg_table_info();
        let catalog = table_info.catalog.clone();
        let namespace = table_info.namespace.clone();
        let table = table_info.table.clone();
        let snapshot_id = table_info.current_snapshot_id;
        let table_handle = IcebergConnectorScanPlanner::table_handle_from_source(
            &catalog,
            &namespace,
            &table,
            snapshot_id,
            table_info,
            vec![dummy_iceberg_file()],
            vec!["id".to_string()],
        );
        let scan = planner
            .begin_scan(table_handle, Default::default())
            .expect("begin_scan");
        let splits = planner
            .plan_splits(&scan, Default::default())
            .expect("plan_splits");

        let plan = planner
            .to_thrift_scan(
                &scan,
                &splits,
                ThriftScanContext {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    node_id: 17,
                    scan_tuple_id: 2,
                    cloud_properties: BTreeMap::from([(
                        "aws.s3.endpoint".to_string(),
                        "http://localhost:9000".to_string(),
                    )]),
                    ..ThriftScanContext::default()
                },
            )
            .expect("to_thrift_scan");

        let node = plan.node.expect("hdfs scan node");
        assert_eq!(node.node_id, 17);
        assert_eq!(node.node_type, plan_nodes::TPlanNodeType::HDFS_SCAN_NODE);
        let hdfs = node.hdfs_scan_node.as_ref().expect("hdfs scan payload");
        assert_eq!(hdfs.tuple_id, Some(2));
        assert_eq!(hdfs.hive_column_names, Some(vec!["id".to_string()]));
        assert_eq!(hdfs.table_name.as_deref(), Some("orders"));
        assert!(hdfs.cloud_configuration.is_some());

        assert_eq!(plan.scan_ranges.len(), 1);
        let range = plan.scan_ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        assert_eq!(
            range.full_path.as_deref(),
            Some("s3://bucket/data/file.parquet")
        );
    }
}
