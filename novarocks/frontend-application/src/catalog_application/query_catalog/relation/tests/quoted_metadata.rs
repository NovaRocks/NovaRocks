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

//! Exercise FE qualification, lease custody and metadata materialization with
//! installed catalog application and control owners. Only Provider I/O is doubled.

use super::*;
use crate::catalog_application::{
    CatalogRuntimeProjection, MvCatalogReferenceReader, query_bindings::QueryTableBindingKey,
    query_catalog::QueryCatalogService, query_materializer::OriginalCatalogLookup,
};
use novarocks_catalog_application::{
    CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogApplicationService, CatalogConfigurationAdmission, CatalogDesiredStateEntry,
    CatalogDesiredStateSnapshot, CatalogDesiredStateSource, CatalogDesiredStateSourceMode,
    CatalogLogicalConfig, CatalogSourceEntryIdentity, ConnectorControlHost,
};
use novarocks_spi::connector::{
    CatalogProperties, CatalogScopeIdentity, ConnectorControlRoleBinding,
    ConnectorControlRoleBindingFactory, ConnectorExecutionDistribution,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, ConnectorProviderBinding, ConnectorProviderId,
    MaterializationContext, NormalizedCatalogProperties, ProviderCatalogControlFactory,
};
use novarocks_sql::{
    compiler::{
        CatalogRelationFact, CatalogRelationOutcome, SqlCapturedCatalogPreparation,
        SqlCompileControl, SqlCompileError, quoted_metadata_catalogs,
    },
    planning::catalog::{MetadataTableKind, catalog_table, table_binding_id},
};

#[derive(Clone)]
struct ObservedIcebergCatalog(Arc<Counts>);

impl ConnectorControlRoleBindingFactory for ObservedIcebergCatalog {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").unwrap()
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        NormalizedCatalogProperties::try_new(properties).map_err(|detail| {
            ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::InvalidDefinition,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                detail,
            )
        })
    }

    fn open_catalog(
        &self,
        identity: CatalogScopeIdentity,
    ) -> Arc<dyn ProviderCatalogControlFactory> {
        assert_eq!(identity.instance_id().as_str(), "ice");
        assert_eq!(identity.provider_id(), &self.provider_id());
        Arc::new(self.clone())
    }
}

impl ProviderCatalogControlFactory for ObservedIcebergCatalog {
    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        _: MaterializationContext,
    ) -> futures::future::BoxFuture<
        'static,
        Result<ConnectorControlRoleBinding, ConnectorMaterializationError>,
    > {
        let counts = self.0.clone();
        Box::pin(async move {
            let control = binding_at_epoch(0, counts)
                .with_catalog_properties(properties.as_catalog_properties().clone())
                .map_err(ConnectorMaterializationError::from)?;
            ConnectorControlRoleBinding::try_new(properties, Arc::new(control), None, None)
                .map_err(ConnectorMaterializationError::from)
        })
    }
}

struct ObservedIcebergDistribution {
    instance: ConnectorInstanceId,
    epoch: ProviderBindingEpoch,
}

impl ConnectorExecutionDistribution for ObservedIcebergDistribution {
    fn declaration(
        &self,
        _: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        ConnectorProviderBinding::iceberg(self.instance.as_str(), self.epoch.to_bytes(), "fixture")
    }
}

fn binding_at_epoch(epoch: u8, counts: Arc<Counts>) -> ConnectorControlBinding {
    let base = crate::connector::scan_model::planned_files_fixture_binding_for_provider(
        ConnectorProviderId::parse("iceberg").unwrap(),
        "ice",
        HashMap::new(),
        None,
    );
    let epoch = ProviderBindingEpoch::from_bytes([epoch; 16]);
    let instance = base.descriptor().instance_id.clone();
    let metadata = Arc::new(ObservedMetadata {
        owner: instance.clone(),
        epoch,
        error: None,
        counts,
    });
    ConnectorControlBinding::try_new(
        base.descriptor().clone(),
        epoch,
        metadata,
        base.planning().clone(),
        Arc::new(ObservedIcebergDistribution { instance, epoch }),
        None,
    )
    .unwrap()
}

struct InstalledIcebergCatalog {
    service: Arc<QueryCatalogService>,
    controls: Arc<ConnectorControlHost>,
    application: Arc<dyn CatalogApplicationPort>,
    counts: Arc<Counts>,
}

impl InstalledIcebergCatalog {
    async fn new() -> Self {
        let counts = Arc::new(Counts::default());
        let controls = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(ObservedIcebergCatalog(
                counts.clone(),
            ))])
            .unwrap(),
        );
        let entry = CatalogDesiredStateEntry::new(
            CatalogSourceEntryIdentity::new(uuid::Uuid::now_v7()),
            CatalogLogicalConfig::try_new(
                ice(),
                ConnectorProviderId::parse("iceberg").unwrap(),
                "ice",
                vec![("type".into(), "iceberg".into())],
                vec![],
                1,
            )
            .unwrap(),
        );
        let snapshot = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::StaticFile,
            vec![entry],
        )
        .unwrap();
        let source = CatalogDesiredStateSource::static_file(snapshot).unwrap();
        let projection = CatalogRuntimeProjection::new();
        let service =
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service());
        projection
            .bind_query_catalog(service.clone(), controls.clone())
            .unwrap();
        let owner = Arc::new(CatalogApplicationService::new(
            source,
            controls.clone(),
            projection.publisher(),
            tokio::runtime::Handle::current(),
            Arc::new(MvCatalogReferenceReader),
        ));
        let application = projection.bind_application(owner.clone());
        owner.reconcile_with_page_size(16, 1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(
                application.admit_catalog_configuration(&ice()),
                CatalogConfigurationAdmission::Ready(_)
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real catalog application must install the Iceberg control generation");
        Self {
            service,
            controls,
            application,
            counts,
        }
    }

    fn materializer<'a>(
        &'a self,
        controls: &'a dyn ConnectorControlResolver,
        context: ConnectorRequestContext,
        bindings: &Arc<QueryTableBindingStore>,
    ) -> CatalogServiceMaterializer<'a> {
        CatalogServiceMaterializer::new(
            Some("ice"),
            &self.service,
            bindings.clone(),
            iceberg_table_binding_loader(controls, context),
        )
        .with_catalog_application(Some(self.application.as_ref()))
    }

    fn quoted_preparation(
        &self,
        bindings: &QueryTableBindingStore,
    ) -> SqlCapturedCatalogPreparation {
        let query = novarocks_parser::parser::parse_query(r#"SELECT x FROM ice.db."orders$files""#)
            .unwrap();
        let compile_control = SqlCompileControl::unbounded();
        let candidates =
            quoted_metadata_catalogs(&query, Some("ice"), "db", &compile_control).unwrap();
        assert_eq!(candidates, [ice()]);
        let qualifications = bindings
            .observe_catalog_qualifications(&candidates, Some(self.application.as_ref()))
            .unwrap();
        SqlCapturedCatalogPreparation::try_new_with_qualifications(
            query.clone(),
            Some("ice"),
            "db",
            compile_control,
            qualifications,
        )
        .unwrap()
    }
}

fn ice() -> ConnectorInstanceId {
    ConnectorInstanceId::try_from_canonical("ice").unwrap()
}

struct CountedInstalledControls<'a> {
    host: &'a ConnectorControlHost,
    acquired: Mutex<Vec<ConnectorControlRuntimeId>>,
}

impl<'a> CountedInstalledControls<'a> {
    fn new(host: &'a ConnectorControlHost) -> Self {
        Self {
            host,
            acquired: Mutex::default(),
        }
    }
}

impl ConnectorControlResolver for CountedInstalledControls<'_> {
    fn observe_current_binding(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        self.host.observe_current_binding(instance)
    }

    fn observe_current_control_runtime(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        self.host.observe_current_control_runtime(instance)
    }

    fn acquire_current(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningPin, ConnectorError> {
        assert_eq!(instance, &ice());
        let pin = self.host.acquire_current(instance)?;
        self.acquired.lock().unwrap().push(pin.control_runtime_id());
        Ok(pin)
    }
}

async fn drive_metadata_read(
    gate: &Arc<MvRefreshPreparationIo>,
    context: &ConnectorRequestContext,
    materializer: &CatalogServiceMaterializer<'_>,
    controls: &CountedInstalledControls<'_>,
    need: &CatalogRelationNeed,
) -> (CatalogRelationFact, Arc<ConnectorControlBinding>) {
    let OriginalCatalogLookup::Metadata(lookup) = materializer
        .begin_original_metadata_lookup(need, controls)
        .unwrap()
    else {
        panic!("cold metadata lookup must enter the named gate");
    };
    assert_eq!(lookup.namespace, "db");
    assert_eq!(lookup.physical_table, "orders$FILES");
    assert_eq!(
        lookup.resolution,
        ConnectorTableResolution::ProviderReadAlias
    );
    let pin = lookup.planning_pin.binding().clone();
    let observed = drive(
        gate,
        gate.metadata(
            &lookup.planning_pin,
            context.clone(),
            &lookup.namespace,
            &lookup.physical_table,
            lookup.resolution,
        ),
    )
    .await;
    let fact = materializer
        .finish_original_metadata_lookup(lookup, observed)
        .unwrap();
    (fact, pin)
}

#[tokio::test(flavor = "current_thread")]
async fn installed_quoted_metadata_uses_one_exact_pin_and_a_separate_statement_memo() {
    with_gate(|gate, context| {
        Box::pin(async move {
            let installed = InstalledIcebergCatalog::new().await;
            let controls = CountedInstalledControls::new(&installed.controls);
            let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
            let materializer = installed.materializer(&controls, context.clone(), &bindings);
            let quoted = installed.quoted_preparation(&bindings);
            let [quoted_need] = quoted.needs() else {
                panic!("one quoted relation need");
            };
            assert_eq!(
                quoted_need.quoted_metadata_binding(),
                Some(&installed.controls.observe_current_binding(&ice()).unwrap()),
            );
            assert!(controls.acquired.lock().unwrap().is_empty());
            let (fact, pin) =
                drive_metadata_read(gate, context, &materializer, &controls, quoted_need).await;
            let CatalogRelationOutcome::Resolved(table) = fact.outcome() else {
                panic!("the provider alias returned a typed SQL table");
            };
            assert_eq!(&catalog_table(table).identity, quoted_need.relation());
            assert_eq!(catalog_table(table).columns[0].name, "x");
            let quoted_token = table_binding_id(table);
            quoted.capture(&[fact]).unwrap();
            let scan = bindings
                .scan_materialization(quoted_token)
                .unwrap()
                .unwrap();
            let admission = bindings
                .binding(quoted_token)
                .unwrap()
                .admission
                .exact_planning_pin()
                .unwrap();
            assert!(Arc::ptr_eq(scan.planning_pin.binding(), &pin));
            assert!(Arc::ptr_eq(admission.binding(), &pin));
            assert_eq!(
                *controls.acquired.lock().unwrap(),
                [pin.control_runtime_id()]
            );
            assert_eq!(installed.counts.table_prepare.load(Ordering::Relaxed), 1);

            let new_round = installed.materializer(&controls, context.clone(), &bindings);
            let memo = new_round
                .resolve_relation_fact_async(quoted_need)
                .await
                .unwrap();
            let CatalogRelationOutcome::Resolved(table) = memo.outcome() else {
                panic!("the original quoted memo is a successful alias");
            };
            assert_eq!(table_binding_id(table), quoted_token);
            assert_eq!(controls.acquired.lock().unwrap().len(), 1);
            assert_eq!(installed.counts.table_prepare.load(Ordering::Relaxed), 1);

            assert!(
                novarocks_parser::parser::parse_query("SELECT x FROM ice.db.orders$files").is_err()
            );
            let other_bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
            let other_materializer =
                installed.materializer(&controls, context.clone(), &other_bindings);
            let other = installed.quoted_preparation(&other_bindings);
            let [other_need] = other.needs() else {
                panic!("one quoted relation need in the second statement");
            };
            assert_eq!(other_need.relation(), quoted_need.relation());
            assert_eq!(other_need.target(), quoted_need.target());
            assert_eq!(
                other_need.quoted_metadata_binding(),
                quoted_need.quoted_metadata_binding()
            );
            let (other_fact, other_pin) =
                drive_metadata_read(gate, context, &other_materializer, &controls, other_need)
                    .await;
            let CatalogRelationOutcome::Resolved(table) = other_fact.outcome() else {
                panic!("the second statement obtained its own metadata observation");
            };
            assert!(Arc::ptr_eq(&pin, &other_pin));
            assert_ne!(table_binding_id(table), quoted_token);
            other.capture(&[other_fact]).unwrap();

            assert_eq!(bindings.captured_bindings().len(), 1);
            assert_eq!(other_bindings.captured_bindings().len(), 1);
            assert_eq!(controls.acquired.lock().unwrap().len(), 2);
            assert_eq!(installed.counts.table_prepare.load(Ordering::Relaxed), 2);
            assert_eq!(installed.counts.table_create.load(Ordering::Relaxed), 2);
            assert_eq!(installed.counts.table_load.load(Ordering::Relaxed), 0);
            assert_eq!(installed.counts.view_prepare.load(Ordering::Relaxed), 0);
            assert_eq!(installed.counts.reference.load(Ordering::Relaxed), 0);
            assert_eq!(
                *installed.counts.resolutions.lock().unwrap(),
                [ConnectorTableResolution::ProviderReadAlias; 2],
            );
            let alias = ConnectorTableIdentity {
                instance_id: ice(),
                namespace: "db".into(),
                table: "orders$FILES".into(),
            };
            assert_eq!(
                *installed.counts.table_requests.lock().unwrap(),
                [alias.clone(), alias],
            );
        })
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_mv_metadata_alias_preserves_actual_provider_cause_and_one_lookup() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_spi::connector::ConnectorErrorKind;

    let installed = InstalledIcebergCatalog::new().await;
    let controls = CountedInstalledControls::new(&installed.controls);
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let materializer = installed.materializer(&controls, context(), &bindings);
    let preparation = installed.quoted_preparation(&bindings);
    let [need] = preparation.needs() else {
        panic!("one actual relation need");
    };
    let OriginalCatalogLookup::Metadata(lookup) = materializer
        .begin_original_metadata_lookup(need, &controls)
        .unwrap()
    else {
        panic!("the original lookup must request metadata exactly once");
    };
    assert_eq!(lookup.namespace, "db");
    assert_eq!(lookup.physical_table, "orders$FILES");
    assert_eq!(
        lookup.resolution,
        ConnectorTableResolution::ProviderReadAlias
    );
    let exact_runtime = lookup.planning_pin.control_runtime_id();
    let original = ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_workload_control::WorkError::Capacity("original catalog observation"),
    );
    let fact = original.failure_fact();
    let cause = std::error::Error::source(&original)
        .unwrap()
        .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
        .unwrap()
        .clone();
    let failed = materializer
        .finish_original_metadata_lookup(lookup, Err(original))
        .unwrap_err();
    assert_eq!(failed.original_failure(), Some(&cause));
    assert_eq!(cause.failure_fact(), fact);
    assert_eq!(fact.class(), FailureClass::Resource);
    assert!(matches!(
        bindings.resolved_lookup(&QueryTableBindingKey::metadata_with_qualification(
            "ice",
            "db",
            "orders",
            MetadataTableKind::Files,
            need.quoted_metadata_binding(),
        )),
        Some(Err(_))
    ));
    // A subsequent completion round reuses the original memo and typed cause.
    let next_round = installed.materializer(&controls, context(), &bindings);
    let repeated = match next_round.begin_original_metadata_lookup(need, &controls) {
        Err(error) => error,
        Ok(_) => panic!("failed original metadata may not be loaded again"),
    };
    assert_eq!(repeated.original_failure(), Some(&cause));
    assert_eq!(*controls.acquired.lock().unwrap(), [exact_runtime]);
}

fn assert_catalog_refusal(
    error: SqlCompileError,
    kind: CatalogApplicationErrorKind,
    message: &str,
) {
    let SqlCompileError::CompilationSource(source) = error else {
        panic!("the original catalog failure must be preserved");
    };
    let actual = source.downcast_ref::<CatalogApplicationError>().unwrap();
    assert_eq!(actual.kind(), kind);
    assert!(actual.to_string().contains(message));
}

#[tokio::test(flavor = "current_thread")]
async fn quoted_metadata_fences_displaced_catalog_and_wrong_planning_pin_before_provider_read() {
    let installed = InstalledIcebergCatalog::new().await;
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let quoted = installed.quoted_preparation(&bindings);
    let [need] = quoted.needs() else {
        panic!("one quoted relation need");
    };

    let replacement_reads = Arc::new(Counts::default());
    let alternate_host = ConnectorControlHost::new();
    alternate_host
        .register(binding_at_epoch(1, replacement_reads.clone()))
        .unwrap();
    let wrong_controls = CountedInstalledControls::new(&alternate_host);
    let wrong = installed.materializer(&wrong_controls, context(), &bindings);
    assert_catalog_refusal(
        wrong.resolve_relation_fact_async(need).await.unwrap_err(),
        CatalogApplicationErrorKind::Conflict,
        "metadata alias planning pin differs",
    );
    assert_eq!(wrong_controls.acquired.lock().unwrap().len(), 1);

    installed.controls.retire_current(&ice()).unwrap();
    installed
        .controls
        .register(binding_at_epoch(2, replacement_reads.clone()))
        .unwrap();
    let controls = CountedInstalledControls::new(&installed.controls);
    let displaced = installed.materializer(&controls, context(), &bindings);
    assert_catalog_refusal(
        displaced
            .resolve_relation_fact_async(need)
            .await
            .unwrap_err(),
        CatalogApplicationErrorKind::Unavailable,
        "catalog control binding changed during configuration admission",
    );
    assert!(controls.acquired.lock().unwrap().is_empty());

    for counts in [&installed.counts, &replacement_reads] {
        assert!(counts.table_requests.lock().unwrap().is_empty());
        assert_eq!(counts.table_prepare.load(Ordering::Relaxed), 0);
        assert_eq!(counts.table_load.load(Ordering::Relaxed), 0);
        assert_eq!(counts.view_prepare.load(Ordering::Relaxed), 0);
        assert_eq!(counts.reference.load(Ordering::Relaxed), 0);
    }
    assert!(bindings.captured_bindings().is_empty());
}
