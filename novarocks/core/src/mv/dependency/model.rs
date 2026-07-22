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

use serde::{Deserialize, Serialize};

use novarocks_catalog::identifier::TableIdentity;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyObjectType {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyStorageEngine {
    StarRocks,
    Iceberg,
    ExternalTable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MvDependencyObjectRef {
    pub catalog: Option<String>,
    pub database_or_namespace: String,
    pub name: String,
    pub object_type: MvDependencyObjectType,
    pub storage_engine: MvDependencyStorageEngine,
}

impl MvDependencyObjectRef {
    pub fn display_name(&self) -> String {
        let object = match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database_or_namespace, self.name),
            None => format!("{}.{}", self.database_or_namespace, self.name),
        };
        match self.object_type {
            MvDependencyObjectType::Table => object,
            MvDependencyObjectType::MaterializedView => format!("mv:{object}"),
        }
    }
}

pub(crate) fn iceberg_table_dependency_ref(base: &TableIdentity) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(base.catalog.clone()),
        database_or_namespace: base.namespace.clone(),
        name: base.table.clone(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn iceberg_mv_dependency_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn starrocks_mv_dependency_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::StarRocks,
    }
}

pub(crate) fn iceberg_table_object_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn starrocks_table_object_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::StarRocks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_ref_display_distinguishes_table_and_mv() {
        let table = iceberg_table_dependency_ref(&TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        });
        let mv = iceberg_mv_dependency_ref("ice", "sales", "orders_mv");

        assert_eq!(table.display_name(), "ice.sales.orders");
        assert_eq!(mv.display_name(), "mv:ice.sales.orders_mv");
    }
}
