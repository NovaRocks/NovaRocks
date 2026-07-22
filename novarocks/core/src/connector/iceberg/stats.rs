#![allow(dead_code)]
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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use arrow::datatypes::DataType;

use crate::connector::iceberg::catalog::IcebergCatalogRegistry;
use crate::connector::iceberg::catalog::backend::{
    data_file_with_stats_to_iceberg_data_file_info, iceberg_schema_def_for_codegen,
};
use crate::connector::iceberg::catalog::registry::{extract_data_files_with_stats_at, load_table};
use crate::connector::iceberg::scan_model::IcebergTableInfo;
use crate::connector::stats::{
    ScanSourceIdentity, StatsProviderError, TableSnapshotRef, TableStatsProvider, TableStatsRequest,
};
use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};
use novarocks_catalog::schema::ColumnDef;

pub(crate) struct IcebergTableStatsProvider {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergTableStatsProvider {
    pub(crate) fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableStatsProvider for IcebergTableStatsProvider {
    fn estimate_table_statistics(
        &self,
        request: &TableStatsRequest,
    ) -> Result<BaseTableStatistics, StatsProviderError> {
        let (catalog, namespace, table) = match &request.source {
            ScanSourceIdentity::IcebergTable {
                catalog,
                namespace,
                table,
            } => (catalog.as_str(), namespace.as_str(), table.as_str()),
            ScanSourceIdentity::Unsupported { reason } => {
                return Err(StatsProviderError::Unsupported(reason.clone()));
            }
        };

        if matches!(
            request.snapshot,
            Some(TableSnapshotRef::Branch(_)) | Some(TableSnapshotRef::Tag(_))
        ) {
            // Provider-level stats currently require a concrete snapshot id.
            // Callers that expose branch/tag SQL must resolve those refs before
            // constructing a TableStatsRequest for this provider.
            return Ok(BaseTableStatistics::missing(
                StatsMissingReason::NoCurrentSnapshot,
            ));
        }

        let entry = {
            let guard = self.registry.read().map_err(|err| {
                StatsProviderError::Catalog(format!("iceberg catalog registry read lock: {err}"))
            })?;
            guard.get(catalog).map_err(StatsProviderError::Catalog)?
        };
        let loaded = load_table(&entry, namespace, table).map_err(StatsProviderError::Catalog)?;
        let snapshot_id = match &request.snapshot {
            None | Some(TableSnapshotRef::Current) => loaded.table.metadata().current_snapshot_id(),
            Some(TableSnapshotRef::SnapshotId(id)) => Some(*id),
            Some(TableSnapshotRef::Branch(_)) | Some(TableSnapshotRef::Tag(_)) => unreachable!(),
        };
        let Some(snapshot_id) = snapshot_id else {
            return Ok(BaseTableStatistics::missing(
                StatsMissingReason::NoCurrentSnapshot,
            ));
        };
        let metadata = loaded.table.metadata();
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            StatsProviderError::Metadata(format!("snapshot {snapshot_id} not found"))
        })?;
        let snapshot_schema = snapshot.schema(metadata).map_err(|err| {
            StatsProviderError::Metadata(format!("resolve snapshot schema {snapshot_id}: {err}"))
        })?;
        let stats_columns = columns_for_stats_schema(
            snapshot_schema.as_ref(),
            catalog,
            namespace,
            table,
            snapshot_id,
        )?;

        let data_files = if let Some(cached) = entry
            .cached_data_files(namespace, table, Some(snapshot_id))
            .map_err(StatsProviderError::Metadata)?
        {
            cached
        } else {
            let extracted = extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(StatsProviderError::Metadata)?;
            entry
                .cache_data_files(namespace, table, Some(snapshot_id), extracted.clone())
                .map_err(StatsProviderError::Metadata)?;
            extracted
        };
        let files = data_files
            .into_iter()
            .map(data_file_with_stats_to_iceberg_data_file_info)
            .collect::<Vec<_>>();
        let table_info = iceberg_table_info_for_stats(
            catalog,
            namespace,
            table,
            &loaded,
            snapshot_schema.as_ref(),
        )?;
        let (ndv_by_name, name_to_field_id) = load_iceberg_puffin_ndv_from_metadata_with_file_io(
            &table_info,
            metadata,
            snapshot_id,
            loaded.table.file_io(),
        );
        Ok(
            crate::sql::optimizer::statistics::build_base_table_statistics_with_ndv(
                &files,
                &stats_columns,
                &ndv_by_name,
                &name_to_field_id,
            ),
        )
    }
}

fn iceberg_table_info_for_stats(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    schema: &iceberg::spec::Schema,
) -> Result<IcebergTableInfo, StatsProviderError> {
    let metadata = loaded.table.metadata();
    Ok(IcebergTableInfo {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: schema.schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def_for_codegen(schema),
        serialized_metadata: Some(serde_json::to_string(metadata).map_err(|err| {
            StatsProviderError::Metadata(format!("serialize iceberg table metadata failed: {err}"))
        })?),
        serialized_metadata_rows: None,
    })
}

fn columns_for_stats_schema(
    schema: &iceberg::spec::Schema,
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: i64,
) -> Result<Vec<ColumnDef>, StatsProviderError> {
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(schema).map_err(|err| {
        StatsProviderError::Metadata(format!("convert snapshot schema to Arrow failed: {err}"))
    })?;
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let iceberg_field = schema.field_by_name(field.name()).ok_or_else(|| {
                StatsProviderError::Metadata(format!(
                    "snapshot schema field `{}` missing from Iceberg schema",
                    field.name()
                ))
            })?;
            let data_type = match iceberg_field.field_type.as_ref() {
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Variant) => {
                    DataType::LargeBinary
                }
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Binary) => {
                    DataType::Binary
                }
                _ => field.data_type().clone(),
            };
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: iceberg_field
                    .write_default
                    .as_ref()
                    .map(|literal| {
                        crate::connector::iceberg::default_value::iceberg_literal_to_column_default(
                            literal,
                            iceberg_field.field_type.as_ref(),
                        )
                        .map_err(|error| {
                            StatsProviderError::Metadata(format!(
                                "convert Iceberg write-default for table `{catalog}.{namespace}.{table}`, snapshot `{snapshot_id}`, column `{}` failed: {error}",
                                field.name(),
                            ))
                        })
                    })
                    .transpose()?,
                logical_type: None,
            })
        })
        .collect()
}

pub(crate) fn load_iceberg_puffin_ndv(
    iceberg_table: Option<&IcebergTableInfo>,
    cloud_properties: &BTreeMap<String, String>,
) -> (HashMap<String, f64>, HashMap<String, i32>) {
    let empty = (HashMap::new(), HashMap::new());
    let Some(info) = iceberg_table else {
        return empty;
    };
    let Some(serialized) = info.serialized_metadata.as_ref() else {
        return empty;
    };
    let metadata: iceberg::spec::TableMetadata = match serde_json::from_str(serialized) {
        Ok(m) => m,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: parse table metadata json failed");
            return empty;
        }
    };
    let Some(snapshot) = metadata.current_snapshot() else {
        return empty;
    };
    load_iceberg_puffin_ndv_from_metadata(info, cloud_properties, &metadata, snapshot.snapshot_id())
}

fn load_iceberg_puffin_ndv_for_snapshot(
    iceberg_table: Option<&IcebergTableInfo>,
    cloud_properties: &BTreeMap<String, String>,
    snapshot_id: i64,
) -> (HashMap<String, f64>, HashMap<String, i32>) {
    let empty = (HashMap::new(), HashMap::new());
    let Some(info) = iceberg_table else {
        return empty;
    };
    let Some(serialized) = info.serialized_metadata.as_ref() else {
        return empty;
    };
    let metadata: iceberg::spec::TableMetadata = match serde_json::from_str(serialized) {
        Ok(m) => m,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: parse table metadata json failed");
            return empty;
        }
    };
    load_iceberg_puffin_ndv_from_metadata(info, cloud_properties, &metadata, snapshot_id)
}

fn load_iceberg_puffin_ndv_from_metadata(
    info: &IcebergTableInfo,
    cloud_properties: &BTreeMap<String, String>,
    metadata: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
) -> (HashMap<String, f64>, HashMap<String, i32>) {
    use crate::connector::iceberg::stats_loader::StatsLoader;
    use crate::runtime::global_async_runtime::data_block_on;

    let empty = (HashMap::new(), HashMap::new());
    if metadata.statistics_for_snapshot(snapshot_id).is_none() {
        return empty;
    }

    let file_io = match build_stats_file_io(&info.location, cloud_properties) {
        Ok(io) => io,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: build FileIO failed");
            return empty;
        }
    };

    let ndv_by_field_id =
        match data_block_on(StatsLoader::load_ndv(metadata, snapshot_id, &file_io)) {
            Ok(map) => map,
            Err(err) => {
                tracing::debug!(error = %err, "iceberg ndv: block_on StatsLoader::load_ndv failed");
                return empty;
            }
        };

    load_iceberg_puffin_ndv_from_field_map(info, ndv_by_field_id)
}

fn load_iceberg_puffin_ndv_from_metadata_with_file_io(
    info: &IcebergTableInfo,
    metadata: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file_io: &iceberg::io::FileIO,
) -> (HashMap<String, f64>, HashMap<String, i32>) {
    use crate::connector::iceberg::stats_loader::StatsLoader;
    use crate::runtime::global_async_runtime::data_block_on;

    let empty = (HashMap::new(), HashMap::new());
    if metadata.statistics_for_snapshot(snapshot_id).is_none() {
        return empty;
    }

    let ndv_by_field_id = match data_block_on(StatsLoader::load_ndv(metadata, snapshot_id, file_io))
    {
        Ok(map) => map,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: block_on StatsLoader::load_ndv failed");
            return empty;
        }
    };

    load_iceberg_puffin_ndv_from_field_map(info, ndv_by_field_id)
}

fn load_iceberg_puffin_ndv_from_field_map(
    info: &IcebergTableInfo,
    ndv_by_field_id: HashMap<i32, f64>,
) -> (HashMap<String, f64>, HashMap<String, i32>) {
    let mut name_to_field_id: HashMap<String, i32> = HashMap::new();
    for field in &info.schema.fields {
        name_to_field_id.insert(field.name.to_lowercase(), field.field_id);
    }

    let mut field_id_to_name: HashMap<i32, String> = HashMap::new();
    for (name, fid) in &name_to_field_id {
        field_id_to_name.insert(*fid, name.clone());
    }
    let mut ndv_by_name: HashMap<String, f64> = HashMap::new();
    for (field_id, ndv) in ndv_by_field_id {
        if let Some(name) = field_id_to_name.get(&field_id) {
            ndv_by_name.insert(name.clone(), ndv);
        }
    }
    (ndv_by_name, name_to_field_id)
}

fn build_stats_file_io(
    location: &str,
    cloud_properties: &BTreeMap<String, String>,
) -> Result<iceberg::io::FileIO, String> {
    let scheme = location.split("://").next().unwrap_or("");
    let is_s3 = matches!(scheme, "s3" | "s3a" | "oss");
    if !is_s3 {
        return Ok(crate::connector::iceberg::fs_io::build_file_io_for_location(location, None));
    }

    let props = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let object_store_config =
        crate::connector::iceberg::fs_io::object_store_config_from_catalog_properties(&props)?
            .ok_or_else(|| {
                "object-store stats FileIO requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                    .to_string()
            })?;
    Ok(
        crate::connector::iceberg::fs_io::build_file_io_for_location(
            location,
            Some(&object_store_config),
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, RwLock};

    use crate::connector::iceberg::catalog::registry::{
        DataFileWithStats, IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg,
        build_iceberg_catalog,
    };
    use crate::connector::iceberg::commit::statistics::commit_statistics_file;
    use crate::connector::iceberg::stats::IcebergTableStatsProvider;
    use crate::connector::iceberg::stats_assembler::{puffin_path_for_snapshot, write_puffin};
    use crate::connector::iceberg::theta_sketch::ThetaSketchHandle;
    use crate::connector::stats::{
        ScanSourceIdentity, TableSnapshotRef, TableStatsProvider, TableStatsRequest,
    };
    use crate::sql::optimizer::statistics::Confidence;
    use crate::sql::optimizer::stats_input::{
        BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };
    use crate::sql::{Literal, TableColumnDef};
    use novarocks_catalog::schema::SqlType;

    fn s3_cloud_properties(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn build_stats_file_io_uses_shared_s3_credentials_aliases() {
        let props = s3_cloud_properties(&[
            ("aws.s3.endpoint_url", "http://localhost:9000"),
            ("aws.s3.accessKeyId", "ak"),
            ("aws.s3.accessKeySecret", "sk"),
            ("aws.s3.enable_path_style_access", "1"),
        ]);

        let _file_io =
            super::build_stats_file_io("s3://bucket/warehouse/table", &props).expect("FileIO");
    }

    #[test]
    fn build_stats_file_io_keeps_local_fallback_without_credentials() {
        let props = BTreeMap::new();

        let _file_io = super::build_stats_file_io("file:///tmp/warehouse/table", &props)
            .expect("local FileIO");
    }

    #[test]
    fn build_stats_file_io_rejects_object_store_without_credentials() {
        let props = s3_cloud_properties(&[("aws.s3.endpoint_url", "http://localhost:9000")]);

        let err = super::build_stats_file_io("s3://bucket/warehouse/table", &props)
            .expect_err("missing S3 credentials should be explicit");

        assert!(err.contains("aws.s3.access_key"), "{err}");
    }

    #[test]
    fn stats_write_default_error_includes_table_snapshot_and_column_identity() {
        let field = iceberg::spec::NestedField::optional(
            1,
            "order_id",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
        )
        .with_write_default(iceberg::spec::Literal::Primitive(
            iceberg::spec::PrimitiveLiteral::String("wrong-type".to_string()),
        ));
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![field.into()])
            .build()
            .expect("schema fixture");

        let error =
            super::columns_for_stats_schema(&schema, "lakehouse", "analytics", "orders", 42)
                .expect_err("type mismatch must fail");
        let crate::connector::stats::StatsProviderError::Metadata(message) = error else {
            panic!("expected metadata error, got {error:?}");
        };

        assert!(
            message.contains("table `lakehouse.analytics.orders`"),
            "{message}"
        );
        assert!(message.contains("snapshot `42`"), "{message}");
        assert!(message.contains("column `order_id`"), "{message}");
        assert!(message.contains("type does not match"), "{message}");
    }

    #[test]
    fn iceberg_provider_rejects_non_iceberg_source_without_registry_lookup() {
        let provider = IcebergTableStatsProvider::new(Arc::new(RwLock::new(
            IcebergCatalogRegistry::default(),
        )));
        let err = provider
            .estimate_table_statistics(&TableStatsRequest {
                catalog: None,
                database: "db".to_string(),
                table: "t".to_string(),
                source: ScanSourceIdentity::Unsupported {
                    reason: "jdbc".to_string(),
                },
                snapshot: None,
            })
            .expect_err("non-iceberg source should be rejected");

        assert_eq!(
            err.into_missing_reason(),
            crate::sql::optimizer::stats_input::StatsMissingReason::ConnectorUnsupported(
                "jdbc".to_string()
            )
        );
    }

    #[test]
    fn iceberg_provider_returns_missing_for_unresolved_branch_or_tag() {
        let provider = IcebergTableStatsProvider::new(Arc::new(RwLock::new(
            IcebergCatalogRegistry::default(),
        )));
        for snapshot in [
            TableSnapshotRef::Branch("main".to_string()),
            TableSnapshotRef::Tag("release".to_string()),
        ] {
            let stats = provider
                .estimate_table_statistics(&TableStatsRequest {
                    catalog: Some("ice".to_string()),
                    database: "db".to_string(),
                    table: "t".to_string(),
                    source: ScanSourceIdentity::IcebergTable {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "t".to_string(),
                    },
                    snapshot: Some(snapshot),
                })
                .expect("branch/tag should produce missing stats, not registry lookup error");

            assert_eq!(
                stats,
                BaseTableStatistics::missing(StatsMissingReason::NoCurrentSnapshot)
            );
        }
    }

    #[test]
    fn iceberg_provider_loads_current_and_explicit_snapshot_stats_with_cache_and_puffin_ndv() {
        let (registry, entry, _warehouse, snapshot_id) =
            registry_with_inserted_table_and_puffin_stats("provider_happy_path");
        assert!(
            entry
                .cached_data_files("db", "t", Some(snapshot_id))
                .expect("read initial cache")
                .is_none()
        );
        let provider = IcebergTableStatsProvider::new(registry);

        let current_stats = provider
            .estimate_table_statistics(&stats_request(Some(TableSnapshotRef::Current)))
            .expect("current snapshot stats");

        assert_eq!(
            current_stats.row_count,
            StatValue::known(3, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_puffin_ndv_about_two(&current_stats);
        assert_eq!(
            current_stats.columns.keys().cloned().collect::<Vec<_>>(),
            vec!["id".to_string()]
        );
        assert!(
            entry
                .cached_data_files("db", "t", Some(snapshot_id))
                .expect("read populated cache")
                .is_some()
        );

        entry
            .cache_data_files(
                "db",
                "t",
                Some(snapshot_id),
                vec![cached_data_file_with_row_count(11)],
            )
            .expect("replace cached files");
        let explicit_stats = provider
            .estimate_table_statistics(&stats_request(Some(TableSnapshotRef::SnapshotId(
                snapshot_id,
            ))))
            .expect("explicit snapshot stats from cache");

        assert_eq!(
            explicit_stats.row_count,
            StatValue::known(11, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_puffin_ndv_about_two(&explicit_stats);
    }

    fn stats_request(snapshot: Option<TableSnapshotRef>) -> TableStatsRequest {
        TableStatsRequest {
            catalog: Some("ice".to_string()),
            database: "db".to_string(),
            table: "t".to_string(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
            },
            snapshot,
        }
    }

    fn assert_puffin_ndv_about_two(stats: &BaseTableStatistics) {
        let id = stats.columns.get("id").expect("id stats");
        let StatValue::Known {
            value,
            confidence,
            source,
        } = id.ndv
        else {
            panic!("expected Puffin NDV, got {:?}", id.ndv);
        };
        assert_eq!(confidence, Confidence::Exact);
        assert_eq!(source, StatsSource::IcebergPuffin);
        assert!(
            (1.5..=2.5).contains(&value),
            "Puffin NDV should be about 2, got {value}"
        );
    }

    fn registry_with_inserted_table_and_puffin_stats(
        test_name: &str,
    ) -> (
        Arc<RwLock<IcebergCatalogRegistry>>,
        IcebergCatalogEntry,
        tempfile::TempDir,
        i64,
    ) {
        let warehouse = tempfile::Builder::new()
            .prefix(&format!("novarocks_stats_provider_test_{test_name}_"))
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
        crate::connector::iceberg::catalog::registry::insert_rows(
            &entry,
            "db",
            "t",
            &[
                vec![Literal::Int(1)],
                vec![Literal::Int(2)],
                vec![Literal::Int(2)],
            ],
        )
        .expect("insert rows");

        let loaded = crate::connector::iceberg::catalog::registry::load_table(&entry, "db", "t")
            .expect("load table after insert");
        let snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .expect("current snapshot id");
        register_puffin_stats_for_id(&entry, &loaded, snapshot_id);
        entry.invalidate_table_cache("db", "t");

        (registry, entry, warehouse, snapshot_id)
    }

    fn register_puffin_stats_for_id(
        entry: &IcebergCatalogEntry,
        loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
        snapshot_id: i64,
    ) {
        let metadata = loaded.table.metadata();
        let mut sketch = ThetaSketchHandle::new(12);
        sketch.update(1_i32);
        sketch.update(2_i32);
        sketch.update(2_i32);
        let mut sketches = HashMap::new();
        sketches.insert(1_i32, sketch);
        let puffin_path = puffin_path_for_snapshot(metadata, snapshot_id);
        let stats_file = block_on_iceberg(write_puffin(
            loaded.table.file_io(),
            &puffin_path,
            snapshot_id,
            metadata.last_sequence_number(),
            &sketches,
        ))
        .expect("write puffin runtime")
        .expect("write puffin")
        .expect("statistics file");
        let catalog = build_iceberg_catalog(entry).expect("build iceberg catalog");
        block_on_iceberg(commit_statistics_file(
            &loaded.table,
            catalog.as_ref(),
            stats_file,
        ))
        .expect("commit stats runtime")
        .expect("commit stats file");
    }

    fn cached_data_file_with_row_count(row_count: i64) -> DataFileWithStats {
        DataFileWithStats {
            path: "file:///cached-provider-row-count.parquet".to_string(),
            size: 1,
            record_count: Some(row_count),
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
}
