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

//! Declaration lookup must never issue provider listing or construct data.
use super::*;
use novarocks_query_application::system_tables::SystemTableRegistry;
use novarocks_query_application::view::{QueryViewService, ViewService};
use novarocks_sql::compiler::{
    CatalogRelationOutcome, SqlCapturedCatalogPreparation, SqlCompileControl,
};
use std::sync::atomic::{AtomicUsize, Ordering};
struct NoTableLoader;
impl QueryTableBindingLoader for NoTableLoader {
    fn load_strict_base_table(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        panic!("an installed definition system relation does not load a connector table")
    }
    fn load_metadata_table(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: novarocks_sql::planning::catalog::MetadataTableKind,
        _: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        panic!("system relations are not connector metadata aliases")
    }
}

fn query(sql: &str) -> novarocks_parser::ast::Query {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query fixture");
    };
    query.clone()
}
fn preparation(sql: &str) -> SqlCapturedCatalogPreparation {
    SqlCapturedCatalogPreparation::try_new_definition(
        query(sql),
        "default_catalog",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn materializer(service: &QueryCatalogService) -> CatalogServiceMaterializer<'_> {
    CatalogServiceMaterializer::new(
        Some("default_catalog"),
        service,
        Arc::new(QueryTableBindingStore::try_new().unwrap()),
        Box::new(NoTableLoader),
    )
    .with_local_views(QueryViewService::new().catalog_snapshot().unwrap())
    .with_system_relations(Arc::new(SystemTableRegistry::try_new([]).unwrap()))
}
#[test]
fn definition_and_current_system_needs_share_declaration_only_lookup() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let provider = materializer(&service);
    for preparation in [
        preparation("SELECT * FROM information_schema.schemata"),
        SqlCapturedCatalogPreparation::try_new(
            query("SELECT * FROM information_schema.schemata"),
            Some("default_catalog"),
            "db",
            SqlCompileControl::unbounded(),
        )
        .unwrap(),
    ] {
        let [need] = preparation.needs() else {
            panic!("one declaration need");
        };
        let fact = provider.resolve_relation_fact(need).unwrap();
        let CatalogRelationOutcome::SystemTable(Ok(declaration)) = fact.outcome() else {
            panic!("pure system declaration");
        };
        assert_eq!(declaration.binding.catalog_name(), "default_catalog");
        assert_eq!(declaration.schema, "information_schema");
        assert_eq!(declaration.table, "schemata");
        assert_eq!(declaration.columns.len(), 5);
        assert_eq!(declaration.columns[0].name, "catalog_name");
        assert_eq!(fact.id(), need.id());
    }
}
#[test]
fn system_lookup_preserves_schema_and_table_absence_categories() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let provider = materializer(&service);
    for (sql, expected) in [
        (
            "SELECT * FROM system.absent.schemata",
            SystemTableLookupError::SchemaNotFound,
        ),
        (
            "SELECT * FROM system.metadata.absent",
            SystemTableLookupError::TableNotFound,
        ),
        (
            "SELECT * FROM information_schema.materialized_views",
            SystemTableLookupError::TableNotFound,
        ),
    ] {
        let preparation = preparation(sql);
        let fact = provider
            .resolve_relation_fact(&preparation.needs()[0])
            .unwrap();
        assert!(
            matches!(fact.outcome(), CatalogRelationOutcome::SystemTable(Err(actual)) if *actual == expected)
        );
    }
}
struct AbsentCatalogAdmission {
    calls: AtomicUsize,
}
impl novarocks_catalog_application::CatalogApplicationPort for AbsentCatalogAdmission {
    fn create_catalog(
        &self,
        _: novarocks_catalog_application::CatalogCreateCommand,
    ) -> Result<
        novarocks_catalog_application::CatalogRuntimeObservation,
        novarocks_catalog_application::CatalogApplicationError,
    > {
        unreachable!("definition lookup must not create a catalog")
    }
    fn drop_catalog(
        &self,
        _: novarocks_catalog_application::CatalogDropCommand,
    ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
        unreachable!("definition lookup must not drop a catalog")
    }
    fn admit_catalog(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> novarocks_catalog_application::CatalogAdmission {
        assert_eq!(
            instance_id,
            &novarocks_spi::connector::ConnectorInstanceId::parse("catalog").unwrap()
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        novarocks_catalog_application::CatalogAdmission::Absent
    }
}

#[test]
fn absent_external_system_catalog_answers_exact_need_without_listing() {
    use crate::catalog_application::statement::external_listing_tests::{
        FailurePoint, ListingFixture,
    };
    use novarocks_spi::connector::ConnectorErrorKind;
    let fixture = ListingFixture::new(FailurePoint::Namespaces, ConnectorErrorKind::Unavailable);
    let application = AbsentCatalogAdmission {
        calls: AtomicUsize::new(0),
    };
    let provider =
        materializer(fixture.catalog_service.as_ref()).with_catalog_application(Some(&application));
    let preparation = preparation("SELECT * FROM catalog.information_schema.schemata");
    let fact = provider
        .resolve_relation_fact(&preparation.needs()[0])
        .unwrap();
    assert!(matches!(
        fact.outcome(),
        CatalogRelationOutcome::SystemTable(Err(SystemTableLookupError::CatalogNotFound))
    ));
    assert_eq!(application.calls.load(Ordering::SeqCst), 1);
}
