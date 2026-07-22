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

use crate::connector::iceberg::scan_model::IcebergDataFileBinding;
use crate::sql::catalog::metadata::{CatalogRuntimeBinding, CatalogRuntimeMetadata};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::table::CatalogTable;

impl CatalogRuntimeMetadata {
    pub(super) fn from_table_def(
        identity: TableIdentity,
        table_def: &TableDef,
    ) -> Result<Self, String> {
        let binding = match &table_def.source {
            ScanSource::StarRocks { db_id, table_id } => CatalogRuntimeBinding::Internal {
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
                CatalogRuntimeBinding::Iceberg {
                    info,
                    cloud_properties: cloud_properties.clone(),
                }
            }
            ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState { .. }
            | ScanSource::IcebergMvTargetLocator { .. } => {
                return Err(format!(
                    "synthetic plan-time scan source is not a catalog base table: {}.{}.{}",
                    identity.catalog, identity.namespace, identity.table
                ));
            }
        };
        Ok(Self {
            table: CatalogTable {
                identity,
                columns: table_def.columns.clone(),
                hidden_columns: table_def.iceberg_row_lineage_metadata_columns.clone(),
            },
            binding,
        })
    }

    pub(super) fn to_table_def(&self) -> TableDef {
        let source = match &self.binding {
            CatalogRuntimeBinding::Internal { db_id, table_id } => ScanSource::StarRocks {
                db_id: *db_id,
                table_id: *table_id,
            },
            CatalogRuntimeBinding::Iceberg {
                info,
                cloud_properties,
            } => ScanSource::IcebergDataFiles {
                table: info.clone(),
                files: Vec::new(),
                cloud_properties: cloud_properties.clone(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        TableDef {
            name: self.table.identity.table.clone(),
            columns: self.table.columns.clone(),
            iceberg_row_lineage_metadata_columns: self.table.hidden_columns.clone(),
            source,
        }
    }
}
