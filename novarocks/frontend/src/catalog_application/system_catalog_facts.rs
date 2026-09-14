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

//! Frontend adapter for the query application's system-catalog facts port.

use std::sync::Arc;

use novarocks_query_application::system_catalog_rewrite::{
    SystemCatalogFacts, SystemCatalogFactsPort,
};
use novarocks_spi::connector::ConnectorControlRegistry;

use crate::catalog_application::query_catalog::QueryCatalogService;

/// Reads only the catalog names that information_schema providers need.
/// Connector admission and metadata transport remain Frontend-owned.
pub(crate) struct FrontendSystemCatalogFacts {
    catalog_service: Arc<QueryCatalogService>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
}

impl FrontendSystemCatalogFacts {
    pub(crate) fn new(
        catalog_service: Arc<QueryCatalogService>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
    ) -> Self {
        Self {
            catalog_service,
            connector_control,
        }
    }
}

impl SystemCatalogFactsPort for FrontendSystemCatalogFacts {
    fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
        let mut schema_names = {
            let catalog = self
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock");
            catalog
                .database_names()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        schema_names.sort();
        schema_names.dedup();
        Ok(SystemCatalogFacts {
            catalog_name: "default_catalog".to_string(),
            schema_names,
            table_names: Vec::new(),
        })
    }

    fn external_system_catalog_facts(
        &self,
        request: &novarocks_spi::connector::ConnectorRequestContext,
        catalog_name: &str,
        include_table_names: bool,
    ) -> Result<Option<SystemCatalogFacts>, String> {
        let lease = match crate::connector::acquire_metadata_planning_lease(
            self.connector_control.as_ref(),
            catalog_name,
        ) {
            Ok(lease) => lease,
            // Preserve the pre-existing unknown-catalog path: the regular SQL
            // resolver owns rendering that error after this rewriter declines.
            Err(_) => return Ok(None),
        };
        let listing_lease = lease.clone();
        let mut schema_names =
            crate::connector::metadata_list_namespaces_with_planning_lease(lease, request.clone())?
                .into_iter()
                .map(|namespace| namespace.namespace.to_string())
                .collect::<Vec<_>>();
        schema_names.sort();
        schema_names.dedup();

        let mut table_names = Vec::new();
        if include_table_names {
            for schema_name in &schema_names {
                let tables = crate::connector::metadata_list_tables_with_planning_lease(
                    &listing_lease,
                    request.clone(),
                    schema_name,
                )?;
                table_names.extend(tables.into_iter().map(|table| (schema_name.clone(), table)));
            }
        }
        Ok(Some(SystemCatalogFacts {
            catalog_name: catalog_name.to_string(),
            schema_names,
            table_names,
        }))
    }
}
