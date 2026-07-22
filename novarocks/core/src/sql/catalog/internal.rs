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

use std::sync::{Arc, RwLock};

use crate::sql::catalog::{CatalogRuntimeMetadata, local::PlannerMemoryCatalog};
use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::Catalog;

pub(super) struct InternalCatalog {
    name: String,
    local: Arc<RwLock<PlannerMemoryCatalog>>,
}

impl InternalCatalog {
    pub(super) fn new(name: &str, local: Arc<RwLock<PlannerMemoryCatalog>>) -> Self {
        Self {
            name: name.to_string(),
            local,
        }
    }
}

impl Catalog<CatalogRuntimeMetadata> for InternalCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<CatalogRuntimeMetadata, String> {
        let table_def = self
            .local
            .read()
            .expect("internal catalog read lock")
            .get(namespace, table)?;
        CatalogRuntimeMetadata::from_table_def(
            TableIdentity::new(&self.name, namespace, table),
            &table_def,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use arrow::datatypes::DataType;

    use super::InternalCatalog;
    use crate::sql::catalog::{
        CatalogRuntimeBinding, CatalogRuntimeMetadata, local::PlannerMemoryCatalog,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::registry::Catalog;
    use novarocks_catalog::schema::ColumnDef;

    fn starrocks_table_def() -> TableDef {
        TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 5,
                table_id: 6,
            },
        }
    }

    #[test]
    fn default_catalog_resolves_registered_local_table() {
        let mut local = PlannerMemoryCatalog::default();
        local.create_database("sales").expect("create database");
        local
            .register("sales", starrocks_table_def())
            .expect("register table");
        let catalog = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(local)));

        let metadata: CatalogRuntimeMetadata = catalog
            .get_table_metadata("sales", "orders")
            .expect("resolve table");

        assert_eq!(metadata.table.identity.catalog, "default_catalog");
        assert_eq!(metadata.table.identity.namespace, "sales");
        assert_eq!(metadata.table.identity.table, "orders");
        assert_eq!(metadata.table.columns.len(), 1);
        assert_eq!(
            metadata.binding,
            CatalogRuntimeBinding::Internal {
                db_id: 5,
                table_id: 6,
            }
        );
    }

    #[test]
    fn default_catalog_preserves_local_missing_table_error() {
        let mut local = PlannerMemoryCatalog::default();
        local.create_database("sales").expect("create database");
        let catalog = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(local)));

        assert_eq!(
            catalog.get_table_metadata("sales", "missing").unwrap_err(),
            "unknown table: missing"
        );
    }
}
