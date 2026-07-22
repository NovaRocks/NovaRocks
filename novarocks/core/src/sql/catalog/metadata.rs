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

use std::collections::BTreeMap;

use crate::connector::iceberg::scan_model::IcebergTableInfo;
use novarocks_catalog::table::CatalogTable;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CatalogRuntimeBinding {
    Internal {
        db_id: i64,
        table_id: i64,
    },
    Iceberg {
        info: IcebergTableInfo,
        cloud_properties: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogRuntimeMetadata {
    pub(super) table: CatalogTable,
    pub(super) binding: CatalogRuntimeBinding,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::{CatalogRuntimeBinding, CatalogRuntimeMetadata};
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::identifier::TableIdentity;
    use novarocks_catalog::schema::ColumnDef;

    fn column(name: &str) -> ColumnDef {
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
            table: "orders".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(17),
            schema_id: 3,
            location: "s3://warehouse/ns/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: Some("{\"current-snapshot-id\":17}".to_string()),
            serialized_metadata_rows: Some("[{\"file_path\":\"data.parquet\"}]".to_string()),
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
    fn iceberg_runtime_metadata_drops_scan_payload_and_preserves_identity() {
        let table_def = TableDef {
            name: "orders".to_string(),
            columns: vec![column("id")],
            iceberg_row_lineage_metadata_columns: vec![column("_row_id")],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(),
                files: vec![IcebergDataFileInfo::for_test(
                    "s3://warehouse/ns/orders/data.parquet",
                    128,
                    4,
                )],
                cloud_properties: cloud_properties(),
                binding: IcebergDataFileBinding::ExplicitFiles,
            },
        };
        let identity = TableIdentity::new("ice", "ns", "orders");

        let metadata =
            CatalogRuntimeMetadata::from_table_def(identity.clone(), &table_def).expect("convert");

        assert_eq!(metadata.table.identity, identity);
        assert_eq!(metadata.table.columns.len(), 1);
        assert_eq!(metadata.table.hidden_columns.len(), 1);
        match &metadata.binding {
            CatalogRuntimeBinding::Iceberg {
                info,
                cloud_properties: preserved_cloud_properties,
            } => {
                assert_eq!(info.schema_id, 3);
                assert_eq!(info.table_uuid.as_deref(), Some("uuid-1"));
                assert_eq!(info.current_snapshot_id, None);
                assert_eq!(info.serialized_metadata, None);
                assert_eq!(
                    info.serialized_metadata_rows.as_deref(),
                    Some("[{\"file_path\":\"data.parquet\"}]")
                );
                assert_eq!(preserved_cloud_properties, &cloud_properties());
            }
            other => panic!("expected Iceberg binding, got {other:?}"),
        }

        let rebuilt = metadata.to_table_def();
        let ScanSource::IcebergDataFiles { files, binding, .. } = rebuilt.source else {
            panic!("expected Iceberg data-file source");
        };
        assert!(files.is_empty());
        assert_eq!(binding, IcebergDataFileBinding::CurrentSnapshot);
    }

    #[test]
    fn runtime_metadata_rejects_synthetic_scan_sources() {
        let table_def = TableDef {
            name: "orders".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergVersionTable {
                table: iceberg_info(),
                snapshot_id: 17,
            },
        };

        let error = CatalogRuntimeMetadata::from_table_def(
            TableIdentity::new("ice", "ns", "orders"),
            &table_def,
        )
        .expect_err("synthetic source must be rejected");

        assert_eq!(
            error,
            "synthetic plan-time scan source is not a catalog base table: ice.ns.orders"
        );
    }
}
