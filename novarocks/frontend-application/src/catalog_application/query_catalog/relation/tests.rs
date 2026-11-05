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

use super::*;
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_type_contract::FunctionValueType;
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Counts {
    acquired: AtomicUsize,
    table_load: AtomicUsize,
    table_prepare: AtomicUsize,
    table_create: AtomicUsize,
    view_load: AtomicUsize,
    view_prepare: AtomicUsize,
    view_create: AtomicUsize,
    reference: AtomicUsize,
    resolutions: Mutex<Vec<ConnectorTableResolution>>,
    events: Mutex<Vec<&'static str>>,
}
struct ObservedMetadata {
    owner: ConnectorInstanceId,
    error: Option<ConnectorError>,
    counts: Arc<Counts>,
}
impl ObservedMetadata {
    fn create(
        &self,
        request: ConnectorTableRequest,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorTableMetadata, PureProviderCompileError<ConnectorError>> {
        let data_type = match request.resolution {
            ConnectorTableResolution::HistoricalSnapshot(10) => arrow::datatypes::DataType::Int32,
            _ => arrow::datatypes::DataType::Int64,
        };
        crate::connector::schema_creation::declared_test_metadata_with_creation(
            ConnectorTableMetadataHeader {
                identity: request.table,
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    self.owner.clone(),
                    bytes::Bytes::from_static(b"same observed table"),
                )?,
            },
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", data_type.clone(), false),
            ])),
            vec![FunctionValueType::new(data_type, false)],
            creation,
        )
    }
    fn references(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        self.counts.reference.fetch_add(1, Ordering::Relaxed);
        self.counts.events.lock().unwrap().push("reference");
        ConnectorReadReferenceFacts::try_new(
            vec![10, 20],
            vec![],
            vec![ConnectorReadNamedReference {
                name: "backup".into(),
                kind: ConnectorReadReferenceKind::Tag,
                snapshot_id: 10,
            }],
            Some(20),
            &request.context,
        )
    }
}
impl ConnectorMetadata for ObservedMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.owner
    }
    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        panic!("no namespace existence probe")
    }
    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        panic!("must use the real table observation")
    }
    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        panic!("no listing")
    }
    fn load_table(
        &self,
        request: ConnectorTableRequest,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorTableMetadata, PureProviderCompileError<ConnectorError>> {
        self.counts.table_load.fetch_add(1, Ordering::Relaxed);
        self.counts.events.lock().unwrap().push("table");
        self.counts
            .resolutions
            .lock()
            .unwrap()
            .push(request.resolution);
        if let Some(error) = &self.error {
            return Err(error.clone().into());
        }
        self.create(request, creation)
    }
    fn prepare_table(&self, request: ConnectorTableRequest) -> ConnectorTablePreparationFuture<'_> {
        Box::pin(async move {
            self.counts.table_prepare.fetch_add(1, Ordering::Relaxed);
            self.counts.events.lock().unwrap().push("table-prepare");
            self.counts
                .resolutions
                .lock()
                .unwrap()
                .push(request.resolution);
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            tokio::task::yield_now().await;
            Ok(ConnectorTablePreparation::new(self.owner.clone(), request))
        })
    }
    fn create_prepared_table(
        &self,
        prepared: ConnectorTablePreparation,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorTableMetadata, PureProviderCompileError<ConnectorError>> {
        let request = prepared.into_facts::<ConnectorTableRequest>(&self.owner)?;
        self.counts.table_create.fetch_add(1, Ordering::Relaxed);
        self.create(request, creation)
    }
    fn read_reference_facts(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        self.references(request)
    }
    fn read_reference_facts_async(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorReadReferenceFacts, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move { self.references(request) })
    }
}
struct ObservedView {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    counts: Arc<Counts>,
    error: Option<ConnectorError>,
}
impl ObservedView {
    fn create(
        &self,
        request: ConnectorViewRequest,
        version: Arc<str>,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        ConnectorViewMetadataValue::try_new(
            request.view,
            ConnectorViewDefinition {
                dialect: ConnectorViewDialect::StarRocks,
                raw_sql: "SELECT 1 AS x".into(),
                default_catalog: Some(self.descriptor.instance_id.as_str().into()),
                default_namespace: "db".into(),
                columns: vec![ConnectorViewColumn {
                    name: "x".into(),
                    value_type: FunctionValueType::new(arrow::datatypes::DataType::Int64, true),
                }]
                .into(),
            },
            version,
            None,
            vec![],
            &request.context,
        )
    }
}
impl ConnectorViewMetadata for ObservedView {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
        panic!("no view existence probe")
    }
    fn list_views(
        &self,
        _: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
        panic!("no view listing")
    }
    fn load_view(
        &self,
        request: ConnectorViewRequest,
        _: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorViewMetadataValue, PureProviderCompileError<ConnectorError>> {
        self.counts.view_load.fetch_add(1, Ordering::Relaxed);
        if let Some(error) = &self.error {
            return Err(error.clone().into());
        }
        Ok(self.create(request, "observed-v1".into())?)
    }
    fn prepare_view(&self, request: ConnectorViewRequest) -> ConnectorViewPreparationFuture<'_> {
        Box::pin(async move {
            self.counts.view_prepare.fetch_add(1, Ordering::Relaxed);
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            let facts = (request, Arc::<str>::from("observed-v1"));
            tokio::task::yield_now().await;
            Ok(ConnectorViewPreparation::new(
                &self.descriptor,
                self.epoch,
                facts,
            ))
        })
    }
    fn create_prepared_view(
        &self,
        prepared: ConnectorViewPreparation,
        _: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorViewMetadataValue, PureProviderCompileError<ConnectorError>> {
        let (request, version) = prepared
            .into_facts::<(ConnectorViewRequest, Arc<str>)>(&self.descriptor, self.epoch)?;
        self.counts.view_create.fetch_add(1, Ordering::Relaxed);
        Ok(self.create(request, version)?)
    }
}
struct CountedResolver {
    binding: Arc<ConnectorControlBinding>,
    counts: Arc<Counts>,
}
impl ConnectorControlResolver for CountedResolver {
    fn observe_current_binding(
        &self,
        _: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        panic!("must keep the acquired planning pin")
    }
    fn observe_current_control_runtime(
        &self,
        _: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        panic!("must keep the acquired planning pin")
    }
    fn acquire_current(
        &self,
        _: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningPin, ConnectorError> {
        self.counts.acquired.fetch_add(1, Ordering::Relaxed);
        Ok(ConnectorControlPlanningPin::new(
            self.binding.clone(),
            || {},
        ))
    }
}
fn fixture(
    table_error: Option<ConnectorError>,
    view: Option<Option<ConnectorError>>,
) -> CountedResolver {
    let base =
        crate::connector::scan_model::planned_files_fixture_binding("ice", HashMap::new(), None);
    let counts = Arc::new(Counts::default());
    let metadata = Arc::new(ObservedMetadata {
        owner: base.descriptor().instance_id.clone(),
        error: table_error,
        counts: counts.clone(),
    });
    let capability = view.map(|error| {
        Arc::new(ObservedView {
            descriptor: base.descriptor().clone(),
            epoch: base.incarnation(),
            counts: counts.clone(),
            error,
        }) as Arc<dyn ConnectorViewMetadata>
    });
    let binding = ConnectorControlBinding::try_new(
        base.descriptor().clone(),
        base.incarnation(),
        metadata,
        base.planning().clone(),
        base.execution_distribution().clone(),
        None,
    )
    .unwrap()
    .with_catalog_properties(base.catalog_properties().unwrap().clone())
    .unwrap()
    .try_with_view_metadata(capability)
    .unwrap();
    CountedResolver {
        binding: Arc::new(binding),
        counts,
    }
}
fn need(sql: &str) -> CatalogRelationNeed {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query")
    };
    let source = novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
        query.clone(),
        Some("ice"),
        "db",
        novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    source
        .needs()
        .iter()
        .find(|need| need.relation().table == "orders")
        .unwrap()
        .clone()
}
fn context() -> ConnectorRequestContext {
    crate::connector::test_request_context()
}
fn absent() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, "actual absence")
}
fn source_kind(error: CatalogResolutionError) -> ConnectorErrorKind {
    let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) = error.into_compile()
    else {
        panic!("original provider cause")
    };
    source.downcast_ref::<ConnectorError>().unwrap().kind()
}

#[test]
fn one_planning_pin_table_success_does_not_probe_a_view() {
    let resolver = fixture(None, Some(None));
    let ConnectorQueryRelationMaterialization::Table {
        materialization,
        selection,
    } = load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders")).unwrap()
    else {
        panic!("table")
    };
    assert!(selection.is_none());
    assert_eq!(
        materialization.planning_pin.control_runtime_id(),
        resolver.binding.control_runtime_id()
    );
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
}
#[test]
fn only_actual_table_absence_enters_the_same_generation_view_capability() {
    let resolver = fixture(Some(absent()), Some(None));
    let ConnectorQueryRelationMaterialization::View(view) =
        load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders")).unwrap()
    else {
        panic!("view")
    };
    assert_eq!(view.definition.version.as_ref(), "observed-v1");
    assert_eq!(
        view.planning_pin.control_runtime_id(),
        resolver.binding.control_runtime_id()
    );
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 1);
    for kind in [
        ConnectorErrorKind::Unavailable,
        ConnectorErrorKind::Unsupported,
        ConnectorErrorKind::CorruptData,
    ] {
        let resolver = fixture(
            Some(ConnectorError::new(
                kind,
                "unknown table: message must not select absence",
            )),
            Some(None),
        );
        let Err(error) = load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders"))
        else {
            panic!("hard failure")
        };
        assert_eq!(source_kind(error), kind);
        assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
    }
    let resolver = fixture(Some(absent()), None);
    let Err(error) = load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders"))
    else {
        panic!("no capability")
    };
    assert_eq!(source_kind(error), ConnectorErrorKind::Unsupported);
    let resolver = fixture(Some(absent()), Some(Some(absent())));
    assert!(matches!(
        load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders")),
        Err(CatalogResolutionError::Missing { .. })
    ));
}
#[tokio::test]
async fn async_view_capture_prepares_then_creates_without_a_joining_reload() {
    let resolver = fixture(Some(absent()), Some(None));
    let request = need("SELECT x FROM ice.db.orders");
    let future = load_relation_async(&resolver, context(), &request);
    tokio::pin!(future);
    assert!(futures::poll!(&mut future).is_pending());
    assert_eq!(resolver.counts.view_create.load(Ordering::Relaxed), 0);
    let ConnectorQueryRelationMaterialization::View(view) = future.await.unwrap() else {
        panic!("view")
    };
    assert_eq!(view.definition.version.as_ref(), "observed-v1");
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_create.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.view_create.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
}
#[tokio::test]
async fn historical_selection_uses_the_same_planning_pin_and_actual_selected_schema() {
    let resolver = fixture(None, Some(None));
    let ConnectorQueryRelationMaterialization::Table {
        materialization,
        selection,
    } = load_relation_async(
        &resolver,
        context(),
        &need("SELECT x FROM ice.db.orders FOR VERSION AS OF 'backup'"),
    )
    .await
    .unwrap()
    else {
        panic!("selected table")
    };
    assert_eq!(selection.unwrap().snapshot_id(), 10);
    assert_eq!(
        materialization.read_selector,
        ConnectorReadSelector::SnapshotId(10)
    );
    assert_eq!(
        materialization.read_schema.field(0).data_type(),
        &arrow::datatypes::DataType::Int32
    );
    assert_eq!(
        materialization.planning_pin.control_runtime_id(),
        resolver.binding.control_runtime_id()
    );
    assert_eq!(
        *resolver.counts.events.lock().unwrap(),
        vec!["reference", "table-prepare"]
    );
    assert_eq!(
        *resolver.counts.resolutions.lock().unwrap(),
        vec![ConnectorTableResolution::HistoricalSnapshot(10)]
    );
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_create.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 0);
}
#[tokio::test]
async fn production_relation_loader_memo_keeps_the_view_across_materializer_rounds() {
    use crate::catalog_application::{
        query_bindings::QueryTableBindingStore,
        query_materializer::{CatalogServiceMaterializer, iceberg_table_binding_loader},
    };
    let resolver = fixture(Some(absent()), Some(None));
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    for sql in [
        "SELECT x FROM ice.db.orders",
        "SELECT b.x FROM ice.db.other a JOIN ice.db.orders b ON 1=1",
    ] {
        let source = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings.clone(),
            iceberg_table_binding_loader(&resolver, context()),
        );
        let request = need(sql);
        let fact = source.resolve_relation_fact_async(&request).await.unwrap();
        assert_eq!(fact.id(), request.id());
        let novarocks_sql::compiler::CatalogRelationOutcome::View(view) = fact.outcome() else {
            panic!("view")
        };
        assert_eq!(view.version.as_ref(), "observed-v1");
    }
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 1);
    assert!(bindings.captured_bindings().is_empty());
}
#[tokio::test]
async fn unknown_ref_is_a_pure_fact_and_its_memo_does_not_reload_provider_metadata() {
    use crate::catalog_application::{
        query_bindings::QueryTableBindingStore,
        query_materializer::{CatalogServiceMaterializer, iceberg_table_binding_loader},
    };
    let resolver = fixture(None, Some(None));
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    for sql in [
        "SELECT x FROM ice.db.orders FOR VERSION AS OF 'missing'",
        "SELECT b.x FROM ice.db.other a JOIN ice.db.orders FOR VERSION AS OF 'missing' b ON 1=1",
    ] {
        let source = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings.clone(),
            iceberg_table_binding_loader(&resolver, context()),
        );
        let request = need(sql);
        let fact = source.resolve_relation_fact_async(&request).await.unwrap();
        assert_eq!(fact.id(), request.id());
        assert!(matches!(
            fact.outcome(),
            novarocks_sql::compiler::CatalogRelationOutcome::SelectionRefused(_)
        ));
    }
    assert_eq!(resolver.counts.reference.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn production_materialization_keeps_the_frozen_snapshot_scan_and_reuses_its_binding() {
    use crate::catalog_application::{
        query_bindings::QueryTableBindingStore,
        query_materializer::{CatalogServiceMaterializer, iceberg_table_binding_loader},
    };
    let resolver = fixture(None, Some(None));
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let mut first_binding = None;
    for sql in [
        "SELECT x FROM ice.db.orders FOR VERSION AS OF 'backup'",
        "SELECT b.x FROM ice.db.other a JOIN ice.db.orders FOR VERSION AS OF 'backup' b ON 1=1",
    ] {
        let source = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings.clone(),
            iceberg_table_binding_loader(&resolver, context()),
        );
        let request = need(sql);
        let fact = source.resolve_relation_fact_async(&request).await.unwrap();
        assert_eq!(fact.id(), request.id());
        let novarocks_sql::compiler::CatalogRelationOutcome::Resolved(table) = fact.outcome()
        else {
            panic!("table")
        };
        let catalog = novarocks_sql::planning::catalog::catalog_table(table);
        assert_eq!(catalog.identity, request.relation().clone());
        assert_eq!(catalog.identity.table, "orders");
        assert_eq!(
            catalog.columns[0].data_type,
            arrow::datatypes::DataType::Int32
        );
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(table),
            Some(10)
        );
        let binding = novarocks_sql::planning::catalog::table_binding_id(table);
        if let Some(first) = first_binding {
            assert_eq!(binding, first)
        } else {
            first_binding = Some(binding)
        }
    }
    assert_eq!(resolver.counts.reference.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
    assert_eq!(bindings.captured_bindings().len(), 1);
}

#[test]
fn original_schema_source_control_is_not_reclassified_as_absence_or_a_view_probe() {
    use novarocks_type_contract::{
        CompileControlError, owned_resources::layout::LayoutResourceError,
    };
    let original = CompileControlError::from_domain(LayoutResourceError::SourceModel);
    let resolver = fixture(
        Some(ConnectorError::compile_interruption(original.clone())),
        Some(None),
    );
    let Err(error) = load_relation(&resolver, context(), &need("SELECT x FROM ice.db.orders"))
    else {
        panic!("source refusal")
    };
    assert_eq!(
        error.into_compile(),
        novarocks_sql::compiler::SqlCompileError::from(original)
    );
    assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn legacy_and_explicit_snapshot_names_share_one_input_without_sharing_the_sql_identity() {
    use crate::catalog_application::{
        query_bindings::QueryTableBindingStore,
        query_materializer::{CatalogServiceMaterializer, iceberg_table_binding_loader},
    };
    let legacy = "SELECT x FROM ice.db.__sqlx1_tt_orders_10";
    let physical = "SELECT x FROM ice.db.orders FOR VERSION AS OF 10";
    for inputs in [[legacy, physical], [physical, legacy]] {
        let resolver = fixture(None, Some(None));
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let mut first = None;
        for sql in inputs {
            let statements = novarocks_parser::parse(sql).unwrap();
            let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
                panic!("query")
            };
            let preparation = novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
                query.clone(),
                Some("ice"),
                "db",
                novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
            let request = &preparation.needs()[0];
            let source = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                iceberg_table_binding_loader(&resolver, context()),
            );
            let fact = source.resolve_relation_fact_async(request).await.unwrap();
            let novarocks_sql::compiler::CatalogRelationOutcome::Resolved(table) = fact.outcome()
            else {
                panic!("frozen input")
            };
            assert_eq!(
                novarocks_sql::planning::catalog::catalog_table(table).identity,
                request.relation().clone()
            );
            assert_eq!(
                novarocks_sql::planning::catalog::frozen_input_snapshot_id(table),
                Some(10)
            );
            let binding = novarocks_sql::planning::catalog::table_binding_id(table);
            if let Some(first) = first {
                assert_eq!(binding, first)
            } else {
                first = Some(binding)
            }
            // The real captured catalog also validates both identity carriers.
            preparation.capture(&[fact]).unwrap();
        }
        assert_eq!(bindings.captured_bindings().len(), 1);
        assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
        assert!(resolver.counts.reference.load(Ordering::Relaxed) <= 1);
    }
}

#[path = "tests/gated_original.rs"]
mod gated_original;

#[test]
fn a_provider_table_from_another_relation_cannot_be_relabelled_as_the_requested_input() {
    for (namespace, table) in [("other", "orders"), ("db", "other")] {
        let resolver = fixture(None, Some(None));
        let (planning_pin, expected, context) =
            begin(&resolver, context(), &need("SELECT x FROM ice.db.orders")).unwrap();
        let mut returned = expected.clone();
        returned.namespace = namespace.into();
        returned.table = table.into();
        let metadata = planning_pin
            .binding()
            .metadata()
            .load_frontend_table(ConnectorTableRequest {
                table: returned,
                resolution: ConnectorTableResolution::StrictBaseTable,
                context,
            })
            .unwrap();
        let error = match finish_table(metadata, planning_pin, None, &expected) {
            Err(error) => error,
            Ok(_) => panic!("a different relation cannot supply the requested table"),
        };
        assert_eq!(source_kind(error), ConnectorErrorKind::CorruptData);
        assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
    }
}
