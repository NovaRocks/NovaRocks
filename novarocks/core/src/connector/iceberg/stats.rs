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
use crate::connector::stats::StatsProviderError;
use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};
use novarocks_catalog::schema::ColumnDef;

pub(crate) fn read_pinned_table_statistics(
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
    catalog: &str,
    namespace: &str,
    table: &str,
    requested_snapshot_id: Option<i64>,
) -> Result<BaseTableStatistics, StatsProviderError> {
    let entry = {
        let guard = registry.read().map_err(|err| {
            StatsProviderError::Catalog(format!("iceberg catalog registry read lock: {err}"))
        })?;
        guard.get(catalog).map_err(StatsProviderError::Catalog)?
    };
    let loaded = load_table(&entry, namespace, table).map_err(StatsProviderError::Catalog)?;
    let snapshot_id =
        requested_snapshot_id.or_else(|| loaded.table.metadata().current_snapshot_id());
    let Some(snapshot_id) = snapshot_id else {
        return Ok(BaseTableStatistics::missing(
            StatsMissingReason::NoCurrentSnapshot,
        ));
    };
    let metadata = loaded.table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| StatsProviderError::Metadata(format!("snapshot {snapshot_id} not found")))?;
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
    let table_info =
        iceberg_table_info_for_stats(catalog, namespace, table, &loaded, snapshot_schema.as_ref())?;
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
