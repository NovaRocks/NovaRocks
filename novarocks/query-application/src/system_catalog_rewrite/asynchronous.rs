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

//! Original AST rewrite with the selected external fact source awaited in place.
use super::*;
pub fn rewrite_query_async<'a>(
    facts_port: &'a dyn SystemCatalogFactsPort,
    system_catalog: &'a dyn SystemCatalog,
    context: &'a ConnectorRequestContext,
    query: &'a mut ast::Query,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    rewrite_query_inner(facts_port, system_catalog, context, query)
}
fn rewrite_query_inner<'a>(
    facts_port: &'a dyn SystemCatalogFactsPort,
    system_catalog: &'a dyn SystemCatalog,
    connector_context: &'a ConnectorRequestContext,
    query: &'a mut ast::Query,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        if let Some(with_clause) = query.with.as_mut() {
            for cte in with_clause.ctes.iter_mut() {
                rewrite_query_inner(
                    facts_port,
                    system_catalog,
                    connector_context,
                    cte.query.as_mut(),
                )
                .await?;
            }
        }
        rewrite_set_expr(
            facts_port,
            system_catalog,
            connector_context,
            query.body.as_mut(),
        )
        .await
    })
}

fn rewrite_set_expr<'a>(
    facts_port: &'a dyn SystemCatalogFactsPort,
    system_catalog: &'a dyn SystemCatalog,
    connector_context: &'a ConnectorRequestContext,
    expr: &'a mut ast::SetExpr,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        match expr {
            ast::SetExpr::Select(select) => {
                for twj in select.from.iter_mut() {
                    rewrite_table_factor(
                        facts_port,
                        system_catalog,
                        connector_context,
                        &mut twj.relation,
                    )
                    .await?;
                    for join in twj.joins.iter_mut() {
                        rewrite_table_factor(
                            facts_port,
                            system_catalog,
                            connector_context,
                            &mut join.relation,
                        )
                        .await?;
                    }
                }
            }
            ast::SetExpr::Query(q) => {
                rewrite_query_inner(facts_port, system_catalog, connector_context, q.as_mut())
                    .await?
            }
            ast::SetExpr::SetOperation(operation) => {
                rewrite_set_expr(
                    facts_port,
                    system_catalog,
                    connector_context,
                    operation.left.as_mut(),
                )
                .await?;
                rewrite_set_expr(
                    facts_port,
                    system_catalog,
                    connector_context,
                    operation.right.as_mut(),
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn rewrite_table_factor<'a>(
    facts_port: &'a dyn SystemCatalogFactsPort,
    system_catalog: &'a dyn SystemCatalog,
    connector_context: &'a ConnectorRequestContext,
    factor: &'a mut ast::TableFactor,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        match factor {
            ast::TableFactor::Table { name, alias, .. } => {
                let parts = object_name_idents(name);
                // Recognize 2-part `information_schema.X` and 3-part
                // `<catalog>.information_schema.X`.
                //
                // For `default_catalog`, the role adapter snapshots local names.
                //
                // For any other 3-part name with an admitted external connector,
                // intercept `information_schema.{schemata,tables}` through its exact
                // control generation.  This bypasses the provider table-load path,
                // which cannot represent a catalog namespace or table scan.
                //
                // We do NOT match plain 1-part references because the session's current
                // database may legitimately shadow them with a real table.
                let key: Option<(String, String)> = match parts.as_slice() {
                    [db, tbl] => Some((db.clone(), tbl.clone())),
                    [cat, db, tbl] if cat.eq_ignore_ascii_case("default_catalog") => {
                        Some((db.clone(), tbl.clone()))
                    }
                    [cat, db, tbl]
                        if db.eq_ignore_ascii_case(INFORMATION_SCHEMA_DB)
                            && (tbl.eq_ignore_ascii_case("schemata")
                                || tbl.eq_ignore_ascii_case("tables")) =>
                    {
                        // The adapter preserves normal unknown-catalog resolution
                        // by returning None and keeps exact connector admission
                        // outside this application domain.
                        let Some(facts) = facts_port
                            .external_system_catalog_facts_async(
                                connector_context,
                                cat,
                                tbl.eq_ignore_ascii_case("tables"),
                            )
                            .await?
                        else {
                            return Ok(());
                        };
                        let inputs = facts.inputs();
                        let Some(data) =
                            system_catalog.resolve(INFORMATION_SCHEMA_DB, tbl, &inputs)?
                        else {
                            return Ok(());
                        };
                        let tbl_name = tbl.clone();
                        let alias = alias.take().unwrap_or_else(|| table_alias(&tbl_name));
                        *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
                        return Ok(());
                    }
                    _ => None,
                };
                let Some((db, tbl)) = key else {
                    return Ok(());
                };
                // Only information_schema hosts system tables; gate before gathering inputs so
                // ordinary table references never trigger a catalog read (behavior-preserving).
                if !db.eq_ignore_ascii_case(INFORMATION_SCHEMA_DB) {
                    return Ok(());
                }
                let facts = facts_port.local_system_catalog_facts()?;
                let inputs = facts.inputs();
                let Some(data) = system_catalog.resolve(&db, &tbl, &inputs)? else {
                    return Ok(());
                };

                let alias = alias.take().unwrap_or_else(|| table_alias(&tbl));
                *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
                Ok(())
            }
            ast::TableFactor::Derived { subquery, .. } => {
                rewrite_query_inner(
                    facts_port,
                    system_catalog,
                    connector_context,
                    subquery.as_mut(),
                )
                .await
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                rewrite_table_factor(
                    facts_port,
                    system_catalog,
                    connector_context,
                    &mut table_with_joins.relation,
                )
                .await?;
                for join in table_with_joins.joins.iter_mut() {
                    rewrite_table_factor(
                        facts_port,
                        system_catalog,
                        connector_context,
                        &mut join.relation,
                    )
                    .await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    struct AsyncFacts {
        ready: AtomicBool,
        polled: AtomicBool,
    }
    impl SystemCatalogFactsPort for AsyncFacts {
        fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
            Ok(SystemCatalogFacts::default())
        }
        fn external_system_catalog_facts(
            &self,
            _: &ConnectorRequestContext,
            _: &str,
            _: bool,
        ) -> Result<Option<SystemCatalogFacts>, String> {
            panic!("async Read must never enter the sync catalog bridge")
        }
        fn external_system_catalog_facts_async<'a>(
            &'a self,
            _: &'a ConnectorRequestContext,
            catalog: &'a str,
            tables: bool,
        ) -> futures::future::BoxFuture<'a, Result<Option<SystemCatalogFacts>, String>> {
            Box::pin(std::future::poll_fn(move |_| {
                self.polled.store(true, Ordering::SeqCst);
                if !self.ready.load(Ordering::SeqCst) {
                    return Poll::Pending;
                }
                assert_eq!(catalog, "ice");
                assert!(tables);
                Poll::Ready(Ok(Some(SystemCatalogFacts {
                    catalog_name: catalog.to_owned(),
                    schema_names: vec!["warehouse".into()],
                    table_names: vec![("warehouse".into(), "orders".into())],
                })))
            }))
        }
    }
    #[test]
    fn actual_ast_rewrite_awaits_same_external_source_and_keeps_original_query_until_ready() {
        let facts = AsyncFacts {
            ready: AtomicBool::new(false),
            polled: AtomicBool::new(false),
        };
        let catalog = crate::system_catalog::SystemCatalogService::with_defaults();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let mut statements =
            novarocks_parser::parse("SELECT table_name FROM ice.information_schema.tables")
                .unwrap();
        let ast::Statement::Query(mut query) = statements.remove(0) else {
            panic!("query");
        };
        let mut future = rewrite_query_async(&facts, &catalog, &context, &mut query);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(facts.polled.load(Ordering::SeqCst));
        facts.ready.store(true, Ordering::SeqCst);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        drop(future);
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select");
        };
        assert!(matches!(
            select.from[0].relation,
            ast::TableFactor::Derived { .. }
        ));
    }
}
