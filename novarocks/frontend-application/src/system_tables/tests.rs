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

//! Focused provider facts from fake statement sources. No external service or
//! process is started; held fixture storage proves snapshot custody transfers.
use super::*;
use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use novarocks_mv_application::freshness::{MvSourceObservation, MvSourceState};
use novarocks_mv_application::persistence::codec::{PublicationInput, RelationOccurrence};
use novarocks_mv_application::persistence::identity::{
    NativeDataVersion, ObjectIdentity, SchemaVersion,
};
use novarocks_query_application::{
    system_tables::{
        SystemTableError, SystemTableLookup, SystemTableProvider, SystemTableRegistry,
        SystemTableRequest, SystemTableSnapshot,
    },
    view::{LocalViewSnapshot, QueryViewService, ViewService},
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, ConnectorStopOwner,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_sql::system_table::{
    SystemTableBinding, SystemTableDeclaration, SystemTableKeyConstraint, SystemTableKeyValues,
};
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Hold {
    _batches: Vec<ValueTypeStorageLifetime>,
}
impl ValueTypeStorageHold for Hold {}
fn held() -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(Hold { _batches: vec![] }))
}
struct Inventory(std::vec::IntoIter<MaterializedViewFacts>);
#[async_trait]
impl MaterializedViewInventory for Inventory {
    async fn next(&mut self) -> Result<Option<MaterializedViewFacts>, SystemTableError> {
        Ok(self.0.next())
    }
}
struct SnapshotInput {
    declaration: Arc<SystemTableDeclaration>,
    columns: Vec<usize>,
    rows: Vec<SystemTableRow>,
    batches: Vec<MaterializedSystemTableBatch>,
}
struct Source {
    local: LocalViewSnapshot,
    calls: Mutex<Vec<String>>,
    tables: Vec<String>,
    views: Result<Vec<String>, ConnectorError>,
    table_error: Option<ConnectorError>,
    stop_after_view: Option<ConnectorStopOwner>,
    mvs: Vec<MaterializedViewFacts>,
    observed: Result<Vec<MvSourceObservation>, ConnectorError>,
    batch_sizes: Mutex<Vec<usize>>,
    snapshot: Mutex<Option<SnapshotInput>>,
}
impl Source {
    fn new() -> Self {
        Self {
            local: QueryViewService::new().catalog_snapshot().unwrap(),
            calls: Mutex::new(vec![]),
            tables: vec!["t".into()],
            views: Ok(vec!["v".into()]),
            table_error: None,
            stop_after_view: None,
            mvs: vec![],
            observed: Ok(vec![]),
            batch_sizes: Mutex::new(vec![]),
            snapshot: Mutex::new(None),
        }
    }
    fn call(&self, name: impl Into<String>) {
        self.calls.lock().unwrap().push(name.into());
    }
}
#[async_trait]
impl StatementSystemTableSource for Source {
    async fn schema_names(
        &self,
        _catalog: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        self.call("schemas");
        Ok(vec!["db".into(), "other".into()])
    }
    async fn schema_exists(
        &self,
        _catalog: &str,
        schema: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<bool, SystemTableError> {
        self.call(format!("exists:{schema}"));
        Ok(schema == "db" || schema == "other")
    }
    async fn table_names(
        &self,
        _catalog: &str,
        schema: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        self.call(format!("tables:{schema}"));
        if let Some(error) = &self.table_error {
            return Err(error.clone().into());
        }
        Ok(self.tables.clone())
    }
    async fn view_names(
        &self,
        _catalog: &str,
        schema: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        self.call(format!("views:{schema}"));
        if let Some(stop) = &self.stop_after_view {
            stop.request_stop();
        }
        self.views.clone().map_err(Into::into)
    }
    fn local_view_snapshot(&self) -> &LocalViewSnapshot {
        &self.local
    }
    async fn materialized_views(
        &self,
        _fresh: bool,
        _definition: bool,
        _context: &ConnectorRequestContext,
    ) -> Result<Box<dyn MaterializedViewInventory>, SystemTableError> {
        self.call("inventory");
        Ok(Box::new(Inventory(self.mvs.clone().into_iter())))
    }
    async fn current_sources(
        &self,
        _occurrences: &[RelationOccurrence],
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<MvSourceObservation>, SystemTableError> {
        self.call("observe");
        self.observed.clone().map_err(Into::into)
    }
    fn begin_snapshot(
        &self,
        declaration: Arc<SystemTableDeclaration>,
        columns: &[usize],
    ) -> Result<(), SystemTableError> {
        *self.snapshot.lock().unwrap() = Some(SnapshotInput {
            declaration,
            columns: columns.to_vec(),
            rows: vec![],
            batches: vec![],
        });
        Ok(())
    }
    fn stage_row(&self, values: &[Option<&str>]) -> Result<RowStage, SystemTableError> {
        let mut snapshot = self.snapshot.lock().unwrap();
        let input = snapshot.as_mut().unwrap();
        input.rows.push(
            input
                .columns
                .iter()
                .map(|ordinal| values[*ordinal].map(str::to_owned))
                .collect(),
        );
        Ok(RowStage::Staged)
    }
    async fn materialize_batch(&self) -> Result<(), SystemTableError> {
        let mut snapshot = self.snapshot.lock().unwrap();
        let input = snapshot.as_mut().unwrap();
        self.batch_sizes.lock().unwrap().push(input.rows.len());
        // This is a fake fact source, not a proof of production capacity. The
        // real CPU source owns the original-window construction tests.
        let mut exact =
            |_: std::alloc::Layout| Ok::<_, novarocks_type_contract::CompileControlError>(());
        let mut opaque = |_: usize| Ok::<_, novarocks_type_contract::CompileControlError>(());
        let storage = held();
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            storage.clone(),
            &mut exact,
            &mut opaque,
        );
        let batch = build_system_table_batch(
            &input.declaration,
            &input.columns,
            &input.rows,
            &mut creation,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )?;
        input
            .batches
            .push(MaterializedSystemTableBatch { batch, storage });
        input.rows.clear();
        Ok(())
    }
    async fn finish_snapshot(&self) -> Result<SystemTableSnapshot, SystemTableError> {
        let input = self.snapshot.lock().unwrap().take().unwrap();
        let (data, storage): (Vec<_>, Vec<_>) = input
            .batches
            .into_iter()
            .map(|batch| (batch.batch, batch.storage))
            .unzip();
        SystemTableSnapshot::try_new(
            data,
            ValueTypeStorageLifetime::held(Arc::new(Hold { _batches: storage })),
        )
    }
}
fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap()
}
fn declaration(
    registry: &SystemTableRegistry,
    binding: SystemTableBinding,
    schema: &str,
    table: &str,
) -> Arc<SystemTableDeclaration> {
    let SystemTableLookup::Declared(value) = registry.declare(&binding, schema, table) else {
        panic!("declared")
    };
    value
}
fn keys(values: &[(usize, &[&str])]) -> SystemTableKeyConstraint {
    let mut keys = SystemTableKeyConstraint::default();
    for (ordinal, values) in values {
        keys.set_values(
            *ordinal,
            SystemTableKeyValues::Values(values.iter().map(|v| (*v).into()).collect()),
        );
    }
    keys
}
async fn snapshot(
    source: Arc<Source>,
    binding: SystemTableBinding,
    schema: &str,
    table: &str,
    columns: Vec<usize>,
    keys: SystemTableKeyConstraint,
    target: usize,
) -> Result<SystemTableSnapshot, SystemTableError> {
    let registry = Arc::new(SystemTableRegistry::try_new([]).unwrap());
    let declaration = declaration(&registry, binding, schema, table);
    let context = context();
    let request =
        SystemTableRequest::try_new(declaration, columns, keys, &context, target).unwrap();
    FrontendSystemTableProvider::new(registry, source)
        .snapshot(&request)
        .await
}
fn rows(snapshot: &SystemTableSnapshot) -> Vec<Vec<Option<String>>> {
    snapshot
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .is_valid(row)
                            .then(|| {
                                column
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap()
                                    .value(row)
                                    .to_string()
                            })
                    })
                    .collect()
            })
        })
        .collect()
}
#[tokio::test]
async fn schema_and_type_constraints_prune_provider_calls_and_preserve_literals() {
    let source = Arc::new(Source::new());
    let result = snapshot(
        source.clone(),
        SystemTableBinding::Catalog("ice".into()),
        "information_schema",
        "tables",
        vec![0, 1, 2, 3],
        keys(&[(1, &["db"]), (3, &["VIEW"])]),
        1,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&result),
        vec![vec![
            Some("ice".into()),
            Some("db".into()),
            Some("v".into()),
            Some("VIEW".into())
        ]]
    );
    assert_eq!(*source.calls.lock().unwrap(), ["exists:db", "views:db"]);
    let source = Arc::new(Source::new());
    let result = snapshot(
        source.clone(),
        SystemTableBinding::Catalog("ice".into()),
        "information_schema",
        "schemata",
        vec![1, 2, 3, 4],
        keys(&[(1, &["db", "DB"])]),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&result),
        vec![vec![
            Some("db".into()),
            Some("utf8".into()),
            Some("utf8_general_ci".into()),
            None
        ]]
    );
    assert_eq!(*source.calls.lock().unwrap(), ["exists:DB", "exists:db"]);
    let source = Arc::new(Source::new());
    let mut none = SystemTableKeyConstraint::default();
    none.set_values(1, SystemTableKeyValues::None);
    assert!(
        snapshot(
            source.clone(),
            SystemTableBinding::Catalog("ice".into()),
            "information_schema",
            "tables",
            vec![],
            none,
            2
        )
        .await
        .unwrap()
        .batches
        .is_empty()
    );
    assert!(source.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn unsupported_views_are_empty_but_table_and_io_errors_keep_the_original_fact() {
    let mut source = Source::new();
    source.views = Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "no view listing",
    ));
    assert!(
        snapshot(
            Arc::new(source),
            SystemTableBinding::Catalog("ice".into()),
            "information_schema",
            "tables",
            vec![2],
            keys(&[(1, &["db"]), (3, &["VIEW"])]),
            2
        )
        .await
        .unwrap()
        .batches
        .is_empty()
    );
    for kind in [
        ConnectorErrorKind::Unsupported,
        ConnectorErrorKind::Unavailable,
        ConnectorErrorKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded,
    ] {
        let original = ConnectorError::new(kind, "original listing failure");
        let mut source = Source::new();
        source.table_error = Some(original.clone());
        assert_eq!(
            snapshot(
                Arc::new(source),
                SystemTableBinding::Catalog("ice".into()),
                "information_schema",
                "tables",
                vec![2],
                keys(&[(1, &["db"]), (3, &["BASE TABLE"])]),
                2
            )
            .await
            .unwrap_err(),
            SystemTableError::Provider(original)
        );
    }
}
#[tokio::test]
async fn system_catalog_uses_registry_and_large_zero_column_snapshots_keep_cardinality() {
    let source = Arc::new(Source::new());
    assert_eq!(
        rows(
            &snapshot(
                source.clone(),
                SystemTableBinding::System,
                "information_schema",
                "schemata",
                vec![1],
                SystemTableKeyConstraint::default(),
                2
            )
            .await
            .unwrap()
        ),
        vec![vec![Some("metadata".into())]]
    );
    assert_eq!(
        rows(
            &snapshot(
                source.clone(),
                SystemTableBinding::System,
                "information_schema",
                "tables",
                vec![1, 2, 3],
                SystemTableKeyConstraint::default(),
                2
            )
            .await
            .unwrap()
        ),
        vec![vec![
            Some("metadata".into()),
            Some("materialized_views".into()),
            Some("BASE TABLE".into())
        ]]
    );
    assert!(source.calls.lock().unwrap().is_empty());
    let mut source = Source::new();
    source.tables = (0..65537).map(|i| format!("t{i:08}")).collect();
    let source = Arc::new(source);
    let result = snapshot(
        source.clone(),
        SystemTableBinding::Catalog("ice".into()),
        "information_schema",
        "tables",
        vec![],
        keys(&[(1, &["db"]), (3, &["BASE TABLE"])]),
        4096,
    )
    .await
    .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        65537
    );
    assert_eq!(result.batches.len(), 17);
    assert!(result.batches.iter().all(|batch| batch.num_columns() == 0));
    assert!(!result.storage.is_pure());
    assert_eq!(*source.batch_sizes.lock().unwrap().last().unwrap(), 1);
}
fn occurrence() -> RelationOccurrence {
    RelationOccurrence {
        occurrence_id: 1,
        catalog_at_binding: "ice".into(),
        namespace_at_binding: "db".into(),
        relation_at_binding: "base".into(),
        qualifier_at_binding: "b".into(),
        object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
        schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
        fields: vec![],
    }
}
fn input() -> PublicationInput {
    PublicationInput {
        relation_occurrence_id: 1,
        object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
        native_data_version: NativeDataVersion::try_new(vec![1]).unwrap(),
    }
}
fn mv(unavailable: bool) -> MaterializedViewFacts {
    MaterializedViewFacts {
        catalog: "ice".into(),
        schema: "db".into(),
        name: "mv".into(),
        definition: "SELECT /* original */ x FROM base".into(),
        unavailable,
        occurrences: vec![occurrence()],
        publication: Some(vec![input()]),
    }
}
#[tokio::test]
async fn mv_rows_keep_definition_storage_fields_and_only_observe_projected_freshness() {
    let mut source = Source::new();
    source.mvs = vec![mv(false)];
    let source = Arc::new(source);
    let result = snapshot(
        source.clone(),
        SystemTableBinding::System,
        "metadata",
        "materialized_views",
        vec![0, 1, 2, 3, 4, 5, 8, 9],
        SystemTableKeyConstraint::default(),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&result)[0],
        vec![
            Some("ice".into()),
            Some("db".into()),
            Some("mv".into()),
            Some("ice".into()),
            Some("db".into()),
            Some("mv".into()),
            Some("".into()),
            Some("SELECT /* original */ x FROM base".into())
        ]
    );
    assert_eq!(*source.calls.lock().unwrap(), ["inventory"]);
    let mut source = Source::new();
    source.mvs = vec![mv(true)];
    let source = Arc::new(source);
    assert_eq!(
        rows(
            &snapshot(
                source.clone(),
                SystemTableBinding::System,
                "metadata",
                "materialized_views",
                vec![6],
                SystemTableKeyConstraint::default(),
                2
            )
            .await
            .unwrap()
        ),
        vec![vec![Some("UNKNOWN".into())]]
    );
    assert_eq!(*source.calls.lock().unwrap(), ["inventory"]);
    let mut source = Source::new();
    source.mvs = vec![mv(false)];
    let i = input();
    source.observed = Ok(vec![MvSourceObservation {
        relation_occurrence_id: 1,
        state: MvSourceState::Present {
            object_id: i.object_id,
            native_data_version: i.native_data_version,
        },
    }]);
    let source = Arc::new(source);
    let result = snapshot(
        source.clone(),
        SystemTableBinding::System,
        "metadata",
        "materialized_views",
        vec![6, 7],
        SystemTableKeyConstraint::default(),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "FRESH"
    );
    assert!(result.batches[0].column(1).is_null(0));
    assert_eq!(
        result.batches[0].column(1).data_type(),
        &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into()))
    );
    let mut source = Source::new();
    source.mvs = vec![mv(false)];
    let original = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original base read failure",
    );
    source.observed = Err(original.clone());
    assert_eq!(
        snapshot(
            Arc::new(source),
            SystemTableBinding::System,
            "metadata",
            "materialized_views",
            vec![6],
            SystemTableKeyConstraint::default(),
            2
        )
        .await
        .unwrap_err(),
        SystemTableError::Provider(original)
    );
}

struct ViewEngine;
impl novarocks_query_application::view::ViewEngine for ViewEngine {
    fn create_external_view(
        &self,
        _request: novarocks_query_application::view::CreateExternalViewRequest,
        _context: &ConnectorRequestContext,
    ) -> Result<(), novarocks_query_application::api::CommandError> {
        panic!("local fixture never creates an external view")
    }
    fn drop_external_view(
        &self,
        _target: &novarocks_query_application::view::ViewTarget,
        _context: &ConnectorRequestContext,
        _policy: novarocks_spi::connector::DropPolicy,
    ) -> Result<(), novarocks_query_application::api::CommandError> {
        panic!("local fixture never drops an external view")
    }
    fn load_external_view(
        &self,
        _target: &novarocks_query_application::view::ViewTarget,
        _context: &ConnectorRequestContext,
    ) -> Result<
        Option<novarocks_query_application::view::ResolvedExternalView>,
        novarocks_query_application::api::CommandError,
    > {
        panic!("local fixture never loads an external view")
    }
    fn analyze_view_definition(
        &self,
        _request: novarocks_query_application::view::ViewDefinitionAnalysisRequest<'_>,
        _context: &ConnectorRequestContext,
    ) -> Result<
        Vec<novarocks_query_application::view::ViewColumnDefinition>,
        novarocks_sql::compiler::SqlCompileError,
    > {
        Ok(vec![
            novarocks_query_application::view::ViewColumnDefinition {
                name: "x".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            },
        ])
    }
}
#[tokio::test]
async fn default_catalog_reads_the_original_local_view_snapshot_without_table_listing() {
    let service = QueryViewService::new();
    let context = context();
    let statements = novarocks_parser::parse("CREATE VIEW v AS SELECT 1 AS x").unwrap();
    let [novarocks_parser::ast::Statement::View(statement)] = statements.as_slice() else {
        panic!("view")
    };
    service
        .execute_statement(
            &ViewEngine,
            statement,
            novarocks_query_application::view::ViewRequestContext {
                current_catalog: None,
                current_database: "db",
                connector_context: Some(&context),
            },
        )
        .unwrap();
    let mut source = Source::new();
    source.local = service.catalog_snapshot().unwrap();
    let source = Arc::new(source);
    service.drop_database("default_catalog", "db").unwrap();
    let result = snapshot(
        source.clone(),
        SystemTableBinding::Catalog("default_catalog".into()),
        "information_schema",
        "tables",
        vec![2, 3],
        keys(&[(1, &["db"])]),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&result),
        vec![vec![Some("v".into()), Some("VIEW".into())]]
    );
    assert_eq!(*source.calls.lock().unwrap(), ["exists:db"]);
}
#[tokio::test]
async fn mv_key_pruning_and_coverage_errors_do_not_hide_provider_failure() {
    let mut source = Source::new();
    source.mvs = vec![mv(false)];
    let source = Arc::new(source);
    let result = snapshot(
        source.clone(),
        SystemTableBinding::System,
        "metadata",
        "materialized_views",
        vec![6],
        keys(&[(2, &["other"])]),
        2,
    )
    .await
    .unwrap();
    assert!(result.batches.is_empty());
    assert_eq!(*source.calls.lock().unwrap(), ["inventory"]);
    let mut source = Source::new();
    source.mvs = vec![mv(false)];
    assert!(matches!(
        snapshot(
            Arc::new(source),
            SystemTableBinding::System,
            "metadata",
            "materialized_views",
            vec![6],
            SystemTableKeyConstraint::default(),
            2
        )
        .await,
        Err(SystemTableError::Internal(_))
    ));
}
#[tokio::test]
async fn request_stop_and_deadline_fail_before_any_listing() {
    let registry = Arc::new(SystemTableRegistry::try_new([]).unwrap());
    let declaration = declaration(
        &registry,
        SystemTableBinding::Catalog("ice".into()),
        "information_schema",
        "tables",
    );
    let stop = ConnectorStopOwner::new();
    let cancelled = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    stop.request_stop();
    let deadline = ConnectorRequestContext::try_new(
        Instant::now() - Duration::from_secs(1),
        ConnectorStopOwner::new().view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    for (context, kind) in [
        (&cancelled, ConnectorErrorKind::Cancelled),
        (&deadline, ConnectorErrorKind::DeadlineExceeded),
    ] {
        let source = Arc::new(Source::new());
        let request = SystemTableRequest::try_new(
            declaration.clone(),
            vec![2],
            SystemTableKeyConstraint::default(),
            context,
            2,
        )
        .unwrap();
        let error = FrontendSystemTableProvider::new(registry.clone(), source.clone())
            .snapshot(&request)
            .await
            .unwrap_err();
        assert!(matches!(error,SystemTableError::Provider(error) if error.kind()==kind));
        assert!(source.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn original_view_provider_failure_precedes_a_concurrent_stop() {
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let original = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original view provider failure",
    );
    let mut source = Source::new();
    source.stop_after_view = Some(stop);
    source.views = Err(original.clone());
    let registry = Arc::new(SystemTableRegistry::try_new([]).unwrap());
    let declaration = declaration(
        &registry,
        SystemTableBinding::Catalog("ice".into()),
        "information_schema",
        "tables",
    );
    let request = SystemTableRequest::try_new(
        declaration,
        vec![2],
        keys(&[(1, &["db"]), (3, &["VIEW"])]),
        &context,
        2,
    )
    .unwrap();
    assert_eq!(
        FrontendSystemTableProvider::new(registry, Arc::new(source))
            .snapshot(&request)
            .await
            .unwrap_err(),
        SystemTableError::Provider(original)
    );
    assert!(context.is_cancelled());
}
