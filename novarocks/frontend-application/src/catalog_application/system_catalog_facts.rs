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
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorListingBound, ConnectorListingBudget,
};

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

/// Copy local schema names into one bounded snapshot. The names are admitted
/// before any of them is copied; an over-bound catalog is refused, never
/// truncated.
fn bounded_local_schema_names<'a, I>(
    bound: ConnectorListingBound,
    names: impl Fn() -> I,
) -> Result<Vec<String>, String>
where
    I: Iterator<Item = &'a str>,
{
    let mut budget = ConnectorListingBudget::new(bound).map_err(|error| error.to_string())?;
    budget
        .admit_names(names())
        .map_err(|error| format!("list local schemas: {error}"))?;
    let mut schema_names = names().map(str::to_string).collect::<Vec<_>>();
    schema_names.sort();
    schema_names.dedup();
    Ok(schema_names)
}

/// Collect one catalog's schema and table names as one bounded snapshot.
///
/// Each source listing is bounded by what the snapshot has left, and its
/// names are charged against the whole snapshot before they are retained, so
/// many namespaces with many tables cannot together exceed `bound`. A table
/// entry retains its schema name as well and is charged for both.
type BoundedExternalNames = (Vec<String>, Vec<(String, String)>);

fn bounded_external_names(
    bound: ConnectorListingBound,
    include_table_names: bool,
    list_namespaces: impl FnOnce(ConnectorListingBound) -> Result<Vec<String>, String>,
    mut list_tables: impl FnMut(&str, ConnectorListingBound) -> Result<Vec<String>, String>,
) -> Result<BoundedExternalNames, String> {
    let mut budget = ConnectorListingBudget::new(bound).map_err(|error| error.to_string())?;
    let mut schema_names = list_namespaces(budget.remaining_bound())?;
    schema_names.sort();
    schema_names.dedup();
    budget
        .admit_names(schema_names.iter().map(String::as_str))
        .map_err(|error| format!("list external schemas: {error}"))?;

    let mut table_names = Vec::new();
    if include_table_names {
        for schema_name in &schema_names {
            let tables = list_tables(schema_name, budget.remaining_bound())?;
            budget
                .admit_qualified_names(schema_name, tables.iter().map(String::as_str))
                .map_err(|error| format!("list external tables in {schema_name}: {error}"))?;
            table_names.extend(tables.into_iter().map(|table| (schema_name.clone(), table)));
        }
    }
    Ok((schema_names, table_names))
}

impl SystemCatalogFactsPort for FrontendSystemCatalogFacts {
    fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
        let schema_names = {
            let catalog = self
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock");
            bounded_local_schema_names(ConnectorListingBound::V1, || catalog.database_names())?
        };
        Ok(SystemCatalogFacts {
            catalog_name: "default_catalog".to_string(),
            schema_names,
            table_names: Vec::new(),
        })
    }

    fn external_system_catalog_facts_async<'a>(
        &'a self,
        request: &'a novarocks_spi::connector::ConnectorRequestContext,
        catalog_name: &'a str,
        include_table_names: bool,
    ) -> futures::future::BoxFuture<'a, Result<Option<SystemCatalogFacts>, String>> {
        Box::pin(async move {
            let lease = match crate::connector::acquire_metadata_planning_lease(
                self.connector_control.as_ref(),
                catalog_name,
            ) {
                Ok(lease) => lease,
                Err(_) => return Ok(None),
            };
            let mut budget = ConnectorListingBudget::new(ConnectorListingBound::V1)
                .map_err(|e| e.to_string())?;
            let mut schema_names =
                crate::connector::metadata_list_namespaces_with_planning_lease_async(
                    lease.clone(),
                    request.clone(),
                    budget.remaining_bound(),
                )
                .await?
                .into_iter()
                .map(|n| n.namespace.to_string())
                .collect::<Vec<_>>();
            schema_names.sort();
            schema_names.dedup();
            budget
                .admit_names(schema_names.iter().map(String::as_str))
                .map_err(|e| format!("list external schemas: {e}"))?;
            let mut table_names = Vec::new();
            if include_table_names {
                for schema in &schema_names {
                    let tables = crate::connector::metadata_list_tables_with_planning_lease_async(
                        &lease,
                        request.clone(),
                        schema,
                        budget.remaining_bound(),
                    )
                    .await?;
                    budget
                        .admit_qualified_names(schema, tables.iter().map(String::as_str))
                        .map_err(|e| format!("list external tables in {schema}: {e}"))?;
                    table_names.extend(tables.into_iter().map(|table| (schema.clone(), table)));
                }
            }
            Ok(Some(SystemCatalogFacts {
                catalog_name: catalog_name.to_owned(),
                schema_names,
                table_names,
            }))
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
        let (schema_names, table_names) = bounded_external_names(
            ConnectorListingBound::V1,
            include_table_names,
            |bound| {
                Ok(
                    crate::connector::metadata_list_namespaces_with_planning_lease(
                        lease,
                        request.clone(),
                        bound,
                    )?
                    .into_iter()
                    .map(|namespace| namespace.namespace.to_string())
                    .collect(),
                )
            },
            |schema_name, bound| {
                crate::connector::metadata_list_tables_with_planning_lease(
                    &listing_lease,
                    request.clone(),
                    schema_name,
                    bound,
                )
            },
        )?;
        Ok(Some(SystemCatalogFacts {
            catalog_name: catalog_name.to_string(),
            schema_names,
            table_names,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_information_schema_listing_errors_preserve_connector_error_text() {
        use crate::catalog_application::statement::external_listing_tests::{
            FailurePoint, ListingFixture, error_kinds,
        };

        for kind in error_kinds() {
            for (point, include_tables, expected_calls) in [
                (FailurePoint::Namespaces, false, vec!["namespaces"]),
                (FailurePoint::Namespaces, true, vec!["namespaces"]),
                (FailurePoint::Tables, true, vec!["namespaces", "tables:a"]),
                (
                    FailurePoint::SecondNamespaceTables,
                    true,
                    vec!["namespaces", "tables:a", "tables:b"],
                ),
            ] {
                let fixture = ListingFixture::new(point, kind);
                let facts = FrontendSystemCatalogFacts::new(
                    fixture.catalog_service.clone(),
                    fixture.registry.clone(),
                );
                let result = facts.external_system_catalog_facts(
                    &crate::connector::test_request_context(),
                    "catalog",
                    include_tables,
                );
                // The FE port has a String error boundary. Compare its complete
                // text, including the connector classification, rather than
                // claiming that this boundary retains ConnectorError itself.
                assert_eq!(
                    result.err(),
                    Some(fixture.expected_error()),
                    "{point:?}, {kind:?}"
                );
                assert_eq!(fixture.calls(), expected_calls);
            }
        }
    }

    fn bound() -> ConnectorListingBound {
        ConnectorListingBound {
            entries: 5,
            total_name_bytes: 16,
            ..ConnectorListingBound::V1
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn external_snapshot_bounds_every_listing_by_what_is_left() {
        let mut table_bounds = Vec::new();
        let (schemas, tables) = bounded_external_names(
            bound(),
            true,
            |bound| {
                assert_eq!(bound.entries, 5);
                assert_eq!(bound.total_name_bytes, 16);
                Ok(names(&["b", "a", "a"]))
            },
            |schema, bound| {
                table_bounds.push((schema.to_string(), bound.entries, bound.total_name_bytes));
                Ok(match schema {
                    "a" => names(&["t1"]),
                    _ => names(&["t2", "t3"]),
                })
            },
        )
        .unwrap();
        assert_eq!(schemas, names(&["a", "b"]));
        assert_eq!(
            tables,
            [
                ("a".to_string(), "t1".to_string()),
                ("b".to_string(), "t2".to_string()),
                ("b".to_string(), "t3".to_string()),
            ]
        );
        // Each table entry is charged its schema name as well: 1 + 2 bytes.
        assert_eq!(
            table_bounds,
            [("a".to_string(), 3, 14), ("b".to_string(), 2, 11)]
        );
    }

    #[test]
    fn many_small_listings_cannot_together_exceed_the_snapshot() {
        let mut listed = Vec::new();
        let error = bounded_external_names(
            bound(),
            true,
            |_| Ok(names(&["a", "b", "c"])),
            |schema, _| {
                listed.push(schema.to_string());
                Ok(names(&["t1", "t2"]))
            },
        )
        .unwrap_err();
        assert!(error.contains("entries bound"), "{error}");
        // The second table listing overflows the snapshot and is refused; the
        // third namespace is never listed.
        assert_eq!(listed, names(&["a", "b"]));

        let error = bounded_external_names(
            ConnectorListingBound {
                total_name_bytes: 8,
                ..bound()
            },
            true,
            |_| Ok(names(&["ab"])),
            |_, _| Ok(names(&["t1", "t2"])),
        )
        .unwrap_err();
        assert!(error.contains("total_name_bytes bound"), "{error}");
    }

    #[test]
    fn local_schema_names_are_refused_whole_when_over_bound() {
        let local = ["sales", "ops", "hr"];
        assert_eq!(
            bounded_local_schema_names(bound(), || local.iter().copied()).unwrap(),
            names(&["hr", "ops", "sales"])
        );
        let error = bounded_local_schema_names(
            ConnectorListingBound {
                entries: 2,
                ..bound()
            },
            || local.iter().copied(),
        )
        .unwrap_err();
        assert!(error.contains("entries bound"), "{error}");
    }
}
