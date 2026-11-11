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

//! Pure declaration lookup. Snapshot data belongs to the statement CPU source.
use super::*;
use novarocks_query_application::system_tables::SystemTableLookup;
use novarocks_sql::compiler::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, SqlCompileError,
};
use novarocks_sql::system_table::{SystemTableBinding, SystemTableLookupError};
use novarocks_types::naming::{DEFAULT_CATALOG, SYSTEM_CATALOG};

impl CatalogServiceMaterializer<'_> {
    pub(super) fn system_relation_fact(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<CatalogRelationFact, SqlCompileError>> {
        if need.target() != CatalogLookupTarget::SystemTable {
            return None;
        }
        Some((|| {
            let registry = self.system_relations.as_ref().ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "system table declarations are not installed".into(),
                )
            })?;
            let relation = need.relation();
            let binding = if relation.catalog == SYSTEM_CATALOG {
                SystemTableBinding::System
            } else {
                SystemTableBinding::Catalog(relation.catalog.clone())
            };
            let observation = if relation.catalog == SYSTEM_CATALOG
                || relation.catalog == DEFAULT_CATALOG
            {
                None
            } else {
                match self.require_catalog_admission(&relation.catalog) {
                    Ok(observation) => {
                        if self
                            .service
                            .registry()
                            .read()
                            .expect("catalog service declaration registry")
                            .get_catalog(&relation.catalog)
                            .is_err()
                        {
                            return CatalogRelationFact::system_table(
                                need,
                                Err(SystemTableLookupError::CatalogNotFound),
                            )
                            .map_err(|error| SqlCompileError::InvalidRequest(error.to_string()));
                        }
                        observation
                    }
                    Err(CatalogResolutionError::Missing { .. }) => {
                        return CatalogRelationFact::system_table(
                            need,
                            Err(SystemTableLookupError::CatalogNotFound),
                        )
                        .map_err(|error| SqlCompileError::InvalidRequest(error.to_string()));
                    }
                    Err(error) => return Err(error.into_compile()),
                }
            };
            let declared = match registry.declare(&binding, &relation.namespace, &relation.table) {
                SystemTableLookup::Declared(declaration) => Ok(declaration),
                SystemTableLookup::UnknownSchema => Err(SystemTableLookupError::SchemaNotFound),
                SystemTableLookup::UnknownTable => Err(SystemTableLookupError::TableNotFound),
            };
            let fact = CatalogRelationFact::system_table(need, declared)
                .map_err(|error| SqlCompileError::InvalidRequest(error.to_string()))?;
            if relation.catalog != SYSTEM_CATALOG && relation.catalog != DEFAULT_CATALOG {
                self.verify_catalog_admission(&relation.catalog, observation.as_ref())
                    .map_err(CatalogResolutionError::into_compile)?;
            }
            Ok(fact)
        })())
    }
}

#[cfg(test)]
mod tests;
