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

use std::any::Any;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::connector::iceberg::catalog::registry::IcebergCatalogRegistry;
use crate::connector::iceberg::scan_model::{IcebergDataFileInfo, IcebergTableInfo};
use crate::connector::scan_planning::{
    ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle, ScanHandle, Split,
};

const CONNECTOR_ID: &str = "iceberg";

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
    BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, TableHandle,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
    use crate::sql::{Literal, TableColumnDef};
    use novarocks_catalog::schema::SqlType;

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
            schema: crate::connector::iceberg::scan_model::IcebergSchemaDef { fields: vec![] },
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
}
