//! Schema-level table metadata for the connector metadata layer.
//!
//! `TableMetadata` is what the analyzer needs to resolve a table: identity +
//! columns + a backend `TableBinding` that says *where* scan-binding will be
//! resolved later (in codegen). It deliberately carries NO scan-binding data
//! (no Iceberg data files, no StarRocks tablets, no snapshot) so it is stable
//! and safe to cache.

use std::collections::BTreeMap;

use crate::sql::catalog::{ColumnDef, IcebergTableInfo, ScanSource, TableDef};

/// Fully-qualified table identity. Used as the schema-cache key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TableIdentity {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl TableIdentity {
    pub(crate) fn new(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }
}

/// Backend-specific locator for scan-binding. Carries identity only, never data.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TableBinding {
    /// Local / StarRocks table. Tablets live in `InMemoryCatalog`
    /// (`PhysicalTableLayout`); resolved at plan time, not here.
    Internal { db_id: i64, table_id: i64 },
    /// Iceberg table. `info` carries identity + schema; the current snapshot's
    /// data files are resolved at codegen time, never stored here.
    Iceberg {
        info: IcebergTableInfo,
        cloud_properties: BTreeMap<String, String>,
    },
}

/// Schema-level metadata returned by `Catalog::get_table_metadata`. Cacheable.
#[derive(Clone, Debug)]
pub(crate) struct TableMetadata {
    pub identity: TableIdentity,
    pub columns: Vec<ColumnDef>,
    pub iceberg_row_lineage_columns: Vec<ColumnDef>,
    pub binding: TableBinding,
}

impl TableMetadata {
    /// Build schema-level metadata from a legacy `TableDef`, dropping any
    /// scan-binding data (Iceberg files). Only catalog base-table sources are
    /// accepted; synthetic plan-time sources are rejected (fail fast).
    pub(crate) fn from_table_def(identity: TableIdentity, td: &TableDef) -> Result<Self, String> {
        let binding = match &td.source {
            ScanSource::StarRocks { db_id, table_id } => TableBinding::Internal {
                db_id: *db_id,
                table_id: *table_id,
            },
            ScanSource::IcebergDataFiles {
                table,
                cloud_properties,
                ..
            } => {
                let mut info = table.clone();
                info.current_snapshot_id = None;
                info.serialized_metadata = None;
                TableBinding::Iceberg {
                    info,
                    cloud_properties: cloud_properties.clone(),
                }
            }
            ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState { .. } => {
                return Err(format!(
                    "synthetic plan-time scan source is not a catalog base table: {}.{}.{}",
                    identity.catalog, identity.namespace, identity.table
                ));
            }
        };
        Ok(Self {
            identity,
            columns: td.columns.clone(),
            iceberg_row_lineage_columns: td.iceberg_row_lineage_metadata_columns.clone(),
            binding,
        })
    }

    pub(crate) fn to_table_def(&self) -> TableDef {
        let source = match &self.binding {
            TableBinding::Internal { db_id, table_id } => ScanSource::StarRocks {
                db_id: *db_id,
                table_id: *table_id,
            },
            TableBinding::Iceberg {
                info,
                cloud_properties,
            } => ScanSource::IcebergDataFiles {
                table: info.clone(),
                files: Vec::new(),
                cloud_properties: cloud_properties.clone(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        TableDef {
            name: self.identity.table.clone(),
            columns: self.columns.clone(),
            iceberg_row_lineage_metadata_columns: self.iceberg_row_lineage_columns.clone(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use arrow::datatypes::DataType;
    use std::collections::BTreeMap;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "t".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 3,
            location: "s3://w/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: Some("{\"format-version\":2}".to_string()),
            serialized_metadata_rows: None,
        }
    }

    fn cloud_properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "aws.s3.endpoint".to_string(),
                "http://minio:9000".to_string(),
            ),
            ("aws.s3.region".to_string(), "us-east-1".to_string()),
        ])
    }

    #[test]
    fn from_table_def_maps_starrocks_binding() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![col("a")],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 10,
                table_id: 20,
            },
        };
        let id = TableIdentity::new("default_catalog", "db", "t");
        let meta = TableMetadata::from_table_def(id.clone(), &td).expect("convert");
        assert_eq!(meta.identity, id);
        assert_eq!(meta.columns.len(), 1);
        assert_eq!(
            meta.binding,
            TableBinding::Internal {
                db_id: 10,
                table_id: 20
            }
        );
    }

    #[test]
    fn from_table_def_maps_iceberg_binding_and_drops_files() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![col("a"), col("b")],
            iceberg_row_lineage_metadata_columns: vec![col("_row_id")],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(),
                files: vec![], // files should be dropped, not carried into TableMetadata
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let id = TableIdentity::new("ice", "ns", "t");
        let meta = TableMetadata::from_table_def(id.clone(), &td).expect("convert");
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.iceberg_row_lineage_columns.len(), 1);
        match meta.binding {
            TableBinding::Iceberg {
                info,
                cloud_properties,
            } => {
                assert_eq!(info.schema_id, 3);
                assert_eq!(info.table, "t");
                assert_eq!(info.current_snapshot_id, None);
                assert_eq!(info.serialized_metadata, None);
                assert!(cloud_properties.is_empty());
            }
            other => panic!("expected Iceberg binding, got {other:?}"),
        }
    }

    #[test]
    fn from_table_def_rejects_synthetic_source() {
        let td = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergVersionTable {
                table: iceberg_info(),
                snapshot_id: 7,
            },
        };
        let id = TableIdentity::new("ice", "ns", "t");
        let err = TableMetadata::from_table_def(id, &td).expect_err("must reject synthetic");
        assert!(err.contains("synthetic"), "got: {err}");
    }

    #[test]
    fn table_metadata_to_table_def_rebuilds_schema_only_iceberg_source() {
        let id = TableIdentity::new("ice", "ns", "orders");
        let meta = TableMetadata {
            identity: id,
            columns: vec![col("id")],
            iceberg_row_lineage_columns: vec![col("_row_id")],
            binding: TableBinding::Iceberg {
                info: iceberg_info(),
                cloud_properties: Default::default(),
            },
        };

        let table_def = meta.to_table_def();

        assert_eq!(table_def.name, "orders");
        assert_eq!(table_def.columns.len(), 1);
        assert_eq!(table_def.iceberg_row_lineage_metadata_columns.len(), 1);
        let ScanSource::IcebergDataFiles { files, binding, .. } = table_def.source else {
            panic!("expected iceberg source");
        };
        assert!(files.is_empty());
        assert_eq!(
            binding,
            crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot
        );
    }

    #[test]
    fn table_metadata_round_trip_preserves_iceberg_cloud_properties() {
        let td = TableDef {
            name: "orders".to_string(),
            columns: vec![col("id")],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(),
                files: vec![],
                cloud_properties: cloud_properties(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let id = TableIdentity::new("ice", "ns", "orders");

        let meta = TableMetadata::from_table_def(id, &td).expect("convert");
        let table_def = meta.to_table_def();

        let ScanSource::IcebergDataFiles {
            cloud_properties: restored_cloud_properties,
            ..
        } = table_def.source
        else {
            panic!("expected iceberg source");
        };
        assert_eq!(restored_cloud_properties, cloud_properties());
    }
}
