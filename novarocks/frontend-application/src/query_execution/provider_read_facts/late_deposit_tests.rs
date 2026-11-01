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
use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
};
use bytes::Bytes;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFilterApplication, ProviderReadLimitApplication,
    ProviderReadMetadata, ProviderReadRuntime, ProviderReadSplitManager, ProviderReadSplitSource,
    ProviderReadSystemTablePlan, ReadRuntimeAdapter,
};
use novarocks_spi::connector::read_stack::*;
use novarocks_spi::connector::*;
use novarocks_workload_control::{CancellationReason, ResultWindowClass};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LateColumn;
impl ColumnHandle for LateColumn {}
#[derive(Debug)]
struct LateSplit;
impl novarocks_spi::connector::read_stack::ConnectorSplit for LateSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

struct ActualFrozenTable {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    destroyed: Arc<AtomicBool>,
}
impl std::fmt::Debug for ActualFrozenTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActualFrozenTableDropWitness")
    }
}
impl Drop for ActualFrozenTable {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.get_mut().unwrap().recv();
        self.destroyed.store(true, Ordering::Release);
    }
}
#[derive(Debug, Eq, PartialEq)]
enum ReadOpenCall {
    Generic(
        SchemaTableName,
        ConnectorReadRelationVersion,
        Option<String>,
    ),
    WholeMv(SchemaTableName, ConnectorTableObjectId, Option<i64>),
    PartitionMv(SchemaTableName, ConnectorMvTargetPartitionSelection),
    Prepared(ConnectorReadSourceSelection, ConnectorReadSourceProjection),
    Delta(SchemaTableName, ConnectorReadChangeWindow),
}

struct TinySourceObservation {
    metadata: Arc<ConnectorTableMetadata>,
    object: ConnectorTableObjectId,
    owner: ConnectorProviderBindingKey,
}

struct LateProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
    table: Mutex<Option<Arc<ActualFrozenTable>>>,
    declared_type: novarocks_type_contract::FunctionValueType,
    opens: AtomicUsize,
    freezes: AtomicUsize,
    public_schemas: AtomicUsize,
    read_opens: Mutex<Vec<ReadOpenCall>>,
    tiny_source: Mutex<Option<TinySourceObservation>>,
    source_prepares: AtomicUsize,
}
impl LateProvider {
    fn open_table(
        &self,
        name: &SchemaTableName,
        call: ReadOpenCall,
    ) -> Result<Option<Arc<ActualFrozenTable>>, ConnectorError> {
        self.read_opens.lock().unwrap().push(call);
        self.opens.fetch_add(1, Ordering::AcqRel);
        if name.schema_name() != "db" || name.table_name() != "orders" {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "wrong read relation",
            ));
        }
        Ok(self.table.lock().unwrap().take())
    }
}
impl ProviderReadRuntime for LateProvider {
    type Table = Arc<ActualFrozenTable>;
    type Column = LateColumn;
    type Transaction = ();
    type Split = LateSplit;
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}
impl ProviderReadMetadata for LateProvider {
    type PreparedSourceRead = (ConnectorReadSourceSelection, ConnectorTableObjectId);

    fn prepare_source_read<'a>(
        &'a self,
        _: &'a ConnectorSession,
        request: &'a ConnectorReadSourcePreparationRequest,
        _: &'a Arc<dyn ProviderMetadataCpuPort>,
        _: &'a ConnectorOriginalResultScope,
        _: &'a mut Option<ConnectorProcessLocalPayload>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<
            novarocks_spi::connector::read_stack::adapter::ProviderPreparedSourceRead<
                Self::PreparedSourceRead,
            >,
            ConnectorError,
        >,
    > {
        Box::pin(async move {
            let state = self.tiny_source.lock().unwrap();
            let source = state.as_ref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "fixture has no original tiny source",
                )
            })?;
            request
                .observed_source_facts()
                .validate_owner(&source.owner)?;
            let object = request
                .observed_source_facts()
                .value::<ConnectorTableObjectId>(source.owner.instance_id.as_str())?;
            let supported = match request.selection() {
                ConnectorReadSourceSelection::Snapshot(41) => true,
                ConnectorReadSourceSelection::ChangeWindow(window) => {
                    window == ConnectorReadChangeWindow::new(40, 41)
                }
                _ => false,
            };
            if request.name() != &SchemaTableName::try_new("db", "orders")?
                || request.observed_table() != &source.metadata.table
                || request.object_id() != &source.object
                || object.as_ref() != &source.object
                || request.to_revision().canonical_read_point()
                    != Some(ConnectorCanonicalReadPoint::Snapshot(Some(41)))
                || request.to_revision().object_identity().value() != source.object.as_bytes()
                || matches!(
                    request.selection(),
                    ConnectorReadSourceSelection::ChangeWindow(_)
                ) && !request.from_revision().is_some_and(|from| {
                    from.canonical_read_point()
                        == Some(ConnectorCanonicalReadPoint::Snapshot(Some(40)))
                        && from.object_identity() == request.to_revision().object_identity()
                })
                || !supported
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "tiny source request differs from its original fixture observation",
                ));
            }
            self.source_prepares.fetch_add(1, Ordering::AcqRel);
            let facts = (request.selection(), source.object.clone());
            let metadata = Arc::clone(&source.metadata);
            Ok(match request.selection() {
                ConnectorReadSourceSelection::Snapshot(_) => novarocks_spi::connector::read_stack::adapter::ProviderPreparedSourceRead::snapshot(facts, metadata),
                ConnectorReadSourceSelection::ChangeWindow(_) => novarocks_spi::connector::read_stack::adapter::ProviderPreparedSourceRead::change_window(facts, ConnectorChangeWindowAdmission::Incremental { has_inserts: true, has_deletes: false, partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned }, metadata),
            })
        })
    }

    fn project_prepared_source_read(
        &self,
        _: &ConnectorSession,
        request: &ConnectorReadSourcePreparationRequest,
        source: &Self::PreparedSourceRead,
        projection: ConnectorReadSourceProjection,
    ) -> Result<Self::Table, ConnectorError> {
        if source.0 != request.selection() || &source.1 != request.object_id() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "tiny source projection changed its original selection",
            ));
        }
        self.open_table(request.name(), ReadOpenCall::Prepared(source.0, projection))?
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "tiny source fixture table was already consumed",
                )
            })
    }

    type PreparedRelation = (
        Self::Table,
        Option<novarocks_spi::connector::read_stack::SystemTableUnitHint>,
    );
    fn prepare_relation<'a>(
        &'a self,
        session: &'a novarocks_spi::connector::read_stack::ConnectorSession,
        request: &'a novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationRequest,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Self::PreparedRelation>, novarocks_spi::connector::ConnectorError>,
    > {
        Box::pin(async move {
            use novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationKind as K;
            let name = request.name();
            match request.kind() {
                K::Table { version, reference } => self
                    .get_table_handle(session, name, *version, reference.as_deref())
                    .map(|v| v.map(|v| (v, None))),
                K::Metadata { request } => match request {
                    Some(request) => self.get_system_table_plan_for_request(session, name, request),
                    None => self.get_system_table_plan(session, name),
                }
                .map(|v| {
                    v.map(|v| {
                        let hint = v.unit_hint();
                        (v.into_handle(), Some(hint))
                    })
                }),
                K::Delta { window } => self
                    .get_change_window_plan(session, name, *window)
                    .map(|v| v.map(|v| (v, None))),
                K::MvTarget {
                    object_id,
                    snapshot_id,
                } => self
                    .get_mv_target_handle(session, name, object_id, *snapshot_id)
                    .map(|v| v.map(|v| (v, None))),
                K::MvTargetPartition { selection } => self
                    .get_mv_target_partition_handle(session, name, selection)
                    .map(|v| v.map(|v| (v, None))),
                K::PinnedFileSet { pinned } => self
                    .get_pinned_file_set_handle(session, name, pinned)
                    .map(|v| v.map(|v| (v, None))),
                K::TableExecute { procedure } => self
                    .get_table_execute_plan(session, name, procedure.clone())
                    .map(|v| v.map(|v| (v, None))),
            }
        })
    }
    fn create_prepared_relation(
        &self,
        prepared: Self::PreparedRelation,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation<Self::Table>,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(Some(
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation::new(
                prepared.0, prepared.1,
            ),
        ))
    }

    type FrozenSource = novarocks_spi::connector::read_stack::adapter::UnsupportedFrozenReadSource;

    fn get_table_handle(
        &self,
        _: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        // The fixture owns exactly one table; no fallback/private payload reconstruction.
        self.open_table(
            name,
            ReadOpenCall::Generic(name.clone(), version, reference.map(str::to_owned)),
        )
    }
    fn get_mv_target_handle(
        &self,
        _: &ConnectorSession,
        name: &SchemaTableName,
        object_id: &ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        self.open_table(
            name,
            ReadOpenCall::WholeMv(name.clone(), object_id.clone(), snapshot_id),
        )
    }
    fn get_mv_target_partition_handle(
        &self,
        _: &ConnectorSession,
        name: &SchemaTableName,
        selection: &ConnectorMvTargetPartitionSelection,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        self.open_table(
            name,
            ReadOpenCall::PartitionMv(name.clone(), selection.clone()),
        )
    }
    fn get_pinned_file_set_handle(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
    fn get_column_bindings(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError> {
        Ok(vec![ProviderReadColumnBinding::new("k", LateColumn, false)])
    }
    fn read_public_schema(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        columns: &[&Self::Column],
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        use novarocks_connector_contract::{
            ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape,
            PureProviderCompileError,
        };
        // The fixture provider independently declares k as required Int64.
        // Neither SQL's requested type nor source annotations supply semantics.
        let mut fields = creation.reserve(columns.len())?;
        let mut bindings = creation.reserve(columns.len())?;
        for _ in columns {
            creation.work(1)?;
            let name = creation.text("k")?;
            fields.push(creation.arc(arrow::datatypes::Field::new(
                name,
                arrow::datatypes::DataType::Int64,
                false,
            ))?);
            bindings.push(self.declared_type.clone());
        }
        let fields = creation.arrow_fields(fields)?;
        let schema = creation.arc(arrow::datatypes::Schema::new(fields))?;
        let storage = creation.storage();
        let (request, work) = creation.observers();
        let execution = ConnectorReadExecutionSchema::try_new_observed::<
            PureProviderCompileError<ConnectorError>,
        >(
            schema.clone(),
            bindings.into_boxed_slice(),
            storage.clone(),
            |layout| request(layout).map_err(PureProviderCompileError::Control),
            |units| work(units).map_err(PureProviderCompileError::Control),
        )?;
        let shape = ConnectorSourceSchemaShape::try_new_observed::<
            PureProviderCompileError<ConnectorError>,
        >(schema, storage.clone(), |units| {
            work(units).map_err(PureProviderCompileError::Control)
        })?;
        let source = ConnectorSourceSchemaFacts::try_new_observed::<
            PureProviderCompileError<ConnectorError>,
        >(
            shape,
            std::collections::HashMap::new(),
            Box::new([]),
            storage.clone(),
            |layout| request(layout).map_err(PureProviderCompileError::Control),
            |units| work(units).map_err(PureProviderCompileError::Control),
        )?;
        let published = ConnectorReadPublicSchema::try_new_typed_observed::<
            PureProviderCompileError<ConnectorError>,
        >(
            &execution,
            &source,
            storage,
            |layout| request(layout).map_err(PureProviderCompileError::Control),
            |units| work(units).map_err(PureProviderCompileError::Control),
        )?;
        self.public_schemas.fetch_add(1, Ordering::AcqRel);
        Ok(published)
    }
    fn final_static_facts(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
    ) -> Result<ConnectorReadStaticFacts<Self::Column>, ConnectorError> {
        self.freezes.fetch_add(1, Ordering::AcqRel);
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new(Arc::<[u8]>::from(&b"input-v1"[..]))?,
            [9; 32],
            ConnectorReadProperties::try_new(
                ConnectorReadDistribution::Unconstrained,
                Vec::<ConnectorReadOrderingKey<LateColumn>>::new(),
            )?,
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            Arc::<[u8]>::from(&b"coverage"[..]),
        )
    }
    fn apply_filter(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: &Constraint<Self::Column>,
    ) -> Result<Option<ProviderReadFilterApplication<Self::Table, Self::Column>>, ConnectorError>
    {
        Ok(None)
    }
    fn apply_projection(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: &[Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
    fn apply_limit(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
        Ok(None)
    }
    fn get_system_table_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        Ok(None)
    }
    fn get_change_window_plan(
        &self,
        _: &ConnectorSession,
        name: &SchemaTableName,
        window: ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        self.read_opens
            .lock()
            .unwrap()
            .push(ReadOpenCall::Delta(name.clone(), window));
        Ok(None)
    }
    fn get_table_execute_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
}
impl ProviderReadSplitManager for LateProvider {
    fn get_splits<'a>(
        &'a self,
        _: &'a ConnectorSession,
        _: &'a Self::Table,
        _: &'a [Assignment<Self::Column>],
        _: &'a BTreeSet<Self::Column>,
        _: &'a Constraint<Self::Column>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>,
    > {
        Box::pin(async move {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "fixture does not enumerate splits",
            ))
        })
    }
}

#[derive(Debug)]
struct OriginalProviderCreatorPanic(Arc<AtomicUsize>);
impl Drop for OriginalProviderCreatorPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

struct LateAccessSealer {
    panic_after_release: Option<Arc<AtomicUsize>>,
    adapter: Arc<ReadRuntimeAdapter<LateProvider>>,
    seals: Arc<AtomicUsize>,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl ConnectorReadAttemptAccessSealer for LateAccessSealer {
    fn seal(
        &self,
        _: &ConnectorReadTableHandle,
        mint: ConnectorReadAttemptAccessMint,
    ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
        self.seals.fetch_add(1, Ordering::AcqRel);
        // Schema publication has completed; hold the actual frozen mint until
        // the original plan sink closes, then exercise the late deposit.
        let _ = self.entered.send(());
        let _ = self.release.lock().unwrap().recv();
        if let Some(drops) = &self.panic_after_release {
            std::panic::panic_any(OriginalProviderCreatorPanic(Arc::clone(drops)));
        }
        Ok(mint.seal(Arc::new(LateReacquirer(self.adapter.clone()))))
    }
}
struct LateReacquirer(Arc<ReadRuntimeAdapter<LateProvider>>);
impl ConnectorReadAttemptAccessReacquirer for LateReacquirer {
    fn for_attempt<'a>(
        &'a self,
        _: &'a ConnectorAttemptContext,
    ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>> {
        Box::pin(async move { Ok(ConnectorReadAttemptRuntime::new(self.0.clone())) })
    }
}
struct LateRequestFactory {
    adapter: Arc<ReadRuntimeAdapter<LateProvider>>,
    sealer: Arc<LateAccessSealer>,
}
impl ConnectorReadRequestControlFactory for LateRequestFactory {
    fn for_planning(
        &self,
        _: &ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        Ok(
            ConnectorReadRequestControl::provider_reacquire_attempt_access(
                self.adapter.clone(),
                self.adapter.clone(),
                self.sealer.clone(),
            ),
        )
    }
}
struct LateEncoder {
    relation_allowed: bool,
    catalog: CatalogHandle,
    relation_calls: Arc<AtomicUsize>,
}
impl LateEncoder {
    fn unused() -> ConnectorCodecError {
        ConnectorCodecError::new(
            ConnectorFieldPath::root("fixture"),
            ConnectorCodecErrorKind::Unsupported,
            "closed plan must not encode a relation",
        )
    }
}
impl ConnectorReadWireEncoder for LateEncoder {
    fn owner(&self) -> &str {
        "iceberg"
    }
    fn encode_relation_payload(
        &self,
        _: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError> {
        self.relation_calls.fetch_add(1, Ordering::AcqRel);
        if !self.relation_allowed {
            return Err(Self::unused());
        }
        let payload = |category, body| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    self.catalog.clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                Bytes::from_static(body),
            )
        };
        Ok(ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(ConnectorCodecCategory::ReadTable, b"fixture-table-orders"),
            payload(ConnectorCodecCategory::ReadView, b"fixture-view-orders"),
        ))
    }
    fn encode_column_payload(
        &self,
        _: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        Ok(ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                self.catalog.clone(),
                ConnectorCodecCategory::ReadColumn,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"fixture-column-k"),
        ))
    }
    fn encode_transaction_payload(
        &self,
        _: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        Err(Self::unused())
    }
    fn encode_split_payload(
        &self,
        _: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError> {
        Err(Self::unused())
    }
}

#[test]
fn actual_provider_freeze_keeps_local_backing_and_rejects_a_closed_plan_deposit() {
    original_provider_completion_fixture(false, None);
}

#[test]
fn actual_provider_panic_reaches_completion_health_and_only_fixture_disposition_releases_it() {
    original_provider_completion_fixture(true, None);
}

#[test]
fn statistics_plan_failure_returns_real_access_before_cpu_input_retirement() {
    original_provider_completion_fixture(false, Some(false));
}

#[test]
fn statistics_plan_cancel_returns_real_access_before_provider_close() {
    original_provider_completion_fixture(false, Some(true));
}

fn original_provider_completion_fixture(
    panic_after_release: bool,
    statistics_cancel: Option<bool>,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let blocking = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) =
        crate::task_execution::blocking_io::tests::admitted_class(ResultWindowClass::Local);
    let capacity =
        QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
    let (seal_started, seal_entered) = mpsc::channel();
    let (release_seal, seal_held) = mpsc::channel();
    let (drop_started, drop_entered) = mpsc::channel();
    let (release_drop, drop_held) = mpsc::channel();
    let destroyed = Arc::new(AtomicBool::new(false));
    let (disarm, watch) = mpsc::channel();
    let rescue_seal = release_seal.clone();
    let rescue_drop = release_drop.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(6)).is_err() {
            let _ = rescue_seal.send(());
            let _ = rescue_drop.send(());
        }
    });
    let instance = ConnectorInstanceId::parse("catalog.analytics").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = Arc::new(LateProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        catalog: catalog.clone(),
        table: Mutex::new(Some(Arc::new(ActualFrozenTable {
            entered: drop_started,
            release: Mutex::new(drop_held),
            destroyed: destroyed.clone(),
        }))),
        declared_type: novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Int64,
            false,
        ),
        opens: AtomicUsize::new(0),
        freezes: AtomicUsize::new(0),
        public_schemas: AtomicUsize::new(0),
        read_opens: Mutex::new(Vec::new()),
        tiny_source: Mutex::new(None),
        source_prepares: AtomicUsize::new(0),
    });
    let adapter = Arc::new(ReadRuntimeAdapter::new(provider.clone()));
    let seals = Arc::new(AtomicUsize::new(0));
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let sealer = Arc::new(LateAccessSealer {
        panic_after_release: panic_after_release.then(|| Arc::clone(&cause_drops)),
        adapter: adapter.clone(),
        seals: seals.clone(),
        entered: seal_started,
        release: Mutex::new(seal_held),
    });
    let relation_calls = Arc::new(AtomicUsize::new(0));
    let props = CatalogProperties::new(
        catalog.clone(),
        provider.descriptor.provider_id.clone(),
        1,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let generic = Arc::new(
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 7)
            .with_catalog_properties(props.clone())
            .unwrap(),
    );
    let host = Arc::new(ConnectorControlHost::new());
    host.register_role_binding(
        ConnectorControlRoleBinding::try_new(
            NormalizedCatalogProperties::try_new(props).unwrap(),
            generic,
            Some(ConnectorControlReadBinding::new(
                adapter.clone(),
                adapter.clone(),
                Some(Arc::new(LateRequestFactory {
                    adapter: adapter.clone(),
                    sealer,
                })),
                Arc::new(LateEncoder {
                    relation_allowed: statistics_cancel.is_some(),
                    catalog: catalog.clone(),
                    relation_calls: relation_calls.clone(),
                }),
            )),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let lease = host.acquire_current(&instance).unwrap();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
    ]));
    let binding = bindings
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(instance.as_str(), "db", "orders"),
            |id| {
                let resolved = novarocks_sql::planning::catalog::materialize_connector_read_table(
                    novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                        catalog: instance.as_str().into(),
                        namespace: "db".into(),
                        table: "orders".into(),
                        columns: vec![novarocks_types::schema::ColumnDef {
                            name: "k".into(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        schema: schema.clone(),
                        binding: id,
                        selector: ConnectorReadSelector::Current,
                        planning_facts: ConnectorTablePlanningFacts::empty(),
                    },
                )
                .unwrap()
                .into_resolved_table();
                let mut value = QueryTableBinding::local(resolved, id);
                value.admission = QueryTableBindingAdmission::Exact(lease.clone());
                value.scan_materialization = Some(QueryScanMaterialization {
                    table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                        instance.clone(),
                        Bytes::from_static(b"orders"),
                    )
                    .unwrap(),
                    selector: ConnectorReadSelector::Current,
                    mv_partition_selection: None,
                    planning_lease: lease.clone(),
                });
                Ok(value)
            },
        )
        .unwrap();
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let mut provider_cpu =
        crate::query_execution::internal_result_cpu::InternalResultCpuOwner::try_new_for_test()
            .unwrap();
    let port = Arc::new(
        FrontendProviderReadFacts::binding_only(
            host.clone(),
            bindings.clone(),
            ConnectorSession::try_new("actual-port", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
                .unwrap(),
            context,
            blocking.clone(),
            capacity.clone(),
        )
        .for_cpu_fixture(provider_cpu.runtime()),
    );
    let completion_source =
        crate::query_execution::completion_facts::FrontendCompletionCustody::new();
    completion_source.install_provider_for_test(Arc::clone(&port));
    let column = novarocks_sql::compiler::fixtures::provider_read_column_need(
        0,
        "k",
        novarocks_physical_plan::ValueType::new(arrow::datatypes::DataType::Int64, false),
        ConnectorValueType::BigInt,
    )
    .unwrap();
    let need = novarocks_sql::compiler::fixtures::provider_read_need(
        1,
        novarocks_physical_plan::ProviderReadOccurrenceId::new(1),
        binding,
        ProviderReadRelationNeed::Data {
            relation: TableIdentity::new(instance.as_str(), "db", "orders"),
            version: ProviderReadVersionNeed::Current,
        },
        vec![column],
        Vec::new(),
        None,
    )
    .unwrap();
    let delivered = Arc::new(AtomicBool::new(false));
    let published = delivered.clone();
    // Only this future owns the real plan sink; its destruction closes deposits.
    let waiter_port = Arc::clone(&port);
    let mut waiter = runtime.spawn(async move {
        let sink = ReadAccessSink::new();
        let answer = waiter_port.resolve_provider_reads(&[need], &sink).await;
        published.store(true, Ordering::Release);
        (answer, sink)
    });
    let entered = seal_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let schema_was_published = provider.public_schemas.load(Ordering::Acquire) == 1;
    if let Some(cancel_plan) = statistics_cancel {
        use crate::query_execution::statistics::preparation::{
            StatisticsPlanCpuInput, StatisticsPreparationCustody, StatisticsPreparationFailure,
        };
        // Resolve through the same real Provider/sealer/deposit path first.
        // Neither a fabricated access nor an empty capability set can pass.
        release_seal.send(()).unwrap();
        let (facts, sink) = runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await })
            .expect("actual Provider resolution deadline")
            .expect("actual Provider waiter");
        let mut facts = facts.expect("actual Provider facts");
        assert_eq!(facts.len(), 1);
        let access = sink
            .try_into_access()
            .unwrap_or_else(|(error, _)| panic!("actual exact read access: {error}"));
        assert_eq!(access.len(), 1);
        let input =
            StatisticsPlanCpuInput::fixture_with_access(binding, facts.pop().unwrap(), access);
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        let wake = Arc::new(ProviderCreatorWake(AtomicWaker::new()));
        let mut plan = provider_cpu.runtime().submit_statistics_plan_fixture(
            input,
            window.retain_alias(),
            cancellation.view(),
            runtime.handle(),
            wake,
        );
        let mut custody = StatisticsPreparationCustody::default();
        let mut outcome = None;
        if cancel_plan {
            cancellation.request(novarocks_query_application::cancellation::QueryCancellationReason::ExecutionCancellationRequested);
            runtime
                .block_on(plan.close(&mut custody))
                .expect("same CPU actual exit");
            assert!(outcome.is_none());
        } else {
            runtime
                .block_on(std::future::poll_fn(|cx| {
                    plan.poll_finish_in(cx, &mut outcome, &mut custody)
                }))
                .expect("same CPU actual exit");
            assert!(matches!(
                outcome,
                Some(Err(StatisticsPreparationFailure::Domain(_)))
            ));
            runtime
                .block_on(plan.close(&mut custody))
                .expect("idempotent actual close");
        }
        // Drop the original CPU source after actual exit. The returned linear
        // access must remain in the Action's original custody through close.
        drop(plan);
        assert!(
            drop_entered.try_recv().is_err(),
            "CPU retirement dropped Provider access"
        );
        runtime
            .block_on(custody.close(&completion_source))
            .expect("actual Provider close");
        assert!(
            drop_entered.try_recv().is_err(),
            "Provider close dropped returned access"
        );
        assert!(!destroyed.load(Ordering::Acquire));
        let release_original = std::thread::spawn(move || drop(custody));
        let original_drop_started = drop_entered.recv_timeout(Duration::from_secs(2)).is_ok();
        let held = !destroyed.load(Ordering::Acquire);
        release_drop.send(()).unwrap();
        release_original
            .join()
            .expect("original capability destructor");
        disarm.send(()).unwrap();
        watchdog.join().unwrap();
        assert!(entered && schema_was_published && original_drop_started && held);
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        assert_eq!(provider.freezes.load(Ordering::Acquire), 1);
        assert_eq!(seals.load(Ordering::Acquire), 1);
        assert_eq!(relation_calls.load(Ordering::Acquire), 1);
        assert!(delivered.load(Ordering::Acquire));
        assert!(completion_source.local_health_failure().is_none());
        runtime
            .block_on(provider_cpu.shutdown_until(Instant::now() + Duration::from_secs(2)))
            .unwrap();
        drop(outcome);
        drop(port);
        drop(completion_source);
        drop(capacity);
        drop(bindings);
        drop(lease);
        drop(window);
        root.owner.complete();
        root.business.release();
        crate::task_execution::blocking_io::tests::settle_control(&control);
        control.close_admission();
        assert!(control.shutdown().is_ok());
        drop(adapter);
        drop(host);
        drop(blocking);
        drop(runtime);
        return;
    }
    stop.request_stop();
    root.owner.cancel(CancellationReason::Requested);
    control.expire_deadlines();
    // Retiring the original generation forbids new acquisition; it must not
    // substitute a new generation for the lease the in-flight read already owns.
    let retired = host.retire_current(&instance).is_ok();
    let no_new_generation = host.acquire_current(&instance).is_err();
    waiter.abort();
    let joined =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await });
    let cancelled = matches!(joined, Ok(Err(ref e)) if e.is_cancelled());
    drop(joined);
    drop(waiter);
    drop(capacity);
    drop(bindings);
    drop(lease);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);
    let held_call = control.snapshot();
    // Really poll and then cancel a borrowed close while the original worker
    // is paused in sealing. No task, receipt or input is replaced by this Drop.
    let mut borrowed_close = Box::pin(completion_source.close());
    let borrowed_close_pending = runtime.block_on(std::future::poll_fn(|cx| {
        std::task::Poll::Ready(borrowed_close.as_mut().poll(cx).is_pending())
    }));
    drop(borrowed_close);
    let close_source = Arc::clone(&completion_source);
    let mut closed = runtime.spawn(async move { close_source.close().await });
    let _ = release_seal.send(());
    let table_drop_entered = drop_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let held_output = control.snapshot();
    let held = !destroyed.load(Ordering::Acquire);
    let _ = release_drop.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let actual_closed =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut closed).await });
    let closed_ok = if panic_after_release {
        matches!(&actual_closed, Ok(Ok(Err(_))))
    } else {
        matches!(&actual_closed, Ok(Ok(Ok(()))))
    };
    // Do not detach a timed-out original close task. Its same JoinHandle is
    // retained in this concrete failure packet through test-harness disposition.
    struct CloseFailure {
        _join: tokio::task::JoinHandle<
            Result<(), novarocks_query_application::api::StatementNativeHealthFailure>,
        >,
        _outcome: Result<
            Result<
                Result<(), novarocks_query_application::api::StatementNativeHealthFailure>,
                tokio::task::JoinError,
            >,
            tokio::time::error::Elapsed,
        >,
        _source: std::mem::ManuallyDrop<
            Arc<crate::query_execution::completion_facts::FrontendCompletionCustody>,
        >,
        _disposition_refusal: Option<&'static str>,
        _cpu_shutdown: Option<String>,
        _cpu: std::mem::ManuallyDrop<
            crate::query_execution::internal_result_cpu::InternalResultCpuOwner,
        >,
        _runtime: std::mem::ManuallyDrop<tokio::runtime::Runtime>,
    }
    if !closed_ok {
        std::panic::panic_any(CloseFailure {
            _join: closed,
            _outcome: actual_closed,
            _source: std::mem::ManuallyDrop::new(completion_source),
            _disposition_refusal: None,
            _cpu_shutdown: None,
            _cpu: std::mem::ManuallyDrop::new(provider_cpu),
            _runtime: std::mem::ManuallyDrop::new(runtime),
        });
    }
    let mut health_observed = completion_source.local_health_failure();
    let held_health = panic_after_release.then(|| control.snapshot());
    let mut same_health = !panic_after_release;
    let mut original_payload = !panic_after_release;
    let mut cached_failure = !panic_after_release;
    let mut cause_held_after_disposition = !panic_after_release;
    if let Some(observed) = health_observed.as_ref() {
        same_health = match &actual_closed {
            Ok(Ok(Err(returned))) => std::ptr::addr_eq(returned.cause(), observed.cause()),
            _ => false,
        };
        original_payload =
            runtime.block_on(port.fixture_worker_failure_is::<OriginalProviderCreatorPanic>());
        let repeated = runtime.block_on(completion_source.close());
        cached_failure = matches!(&repeated, Err(failure) if std::ptr::addr_eq(failure.cause(), observed.cause()));
        drop(repeated);
        // The health getter retained exactly this packet; move the already
        // joined original job to the fixture, never manufacture normal exit.
        let original_job =
            match runtime.block_on(port.take_joined_for_fixture_disposition(observed)) {
                Ok(original) => original,
                Err(refusal) => std::panic::panic_any(CloseFailure {
                    _join: closed,
                    _outcome: actual_closed,
                    _source: std::mem::ManuallyDrop::new(completion_source),
                    _disposition_refusal: Some(refusal),
                    _cpu_shutdown: None,
                    _cpu: std::mem::ManuallyDrop::new(provider_cpu),
                    _runtime: std::mem::ManuallyDrop::new(runtime),
                }),
            };
        drop(original_job);
        let after_disposition = runtime.block_on(completion_source.close());
        cached_failure &= matches!(&after_disposition, Err(failure) if std::ptr::addr_eq(failure.cause(), observed.cause()));
        drop(after_disposition);
        // The canonical source never turns this into a healthy/unused branch.
        cause_held_after_disposition = cause_drops.load(Ordering::Acquire) == 0;
    }
    let source_healthy = completion_source.local_health_failure().is_none();
    let held_after_disposition = panic_after_release.then(|| control.snapshot());
    if let Err(detail) =
        runtime.block_on(provider_cpu.shutdown_until(Instant::now() + Duration::from_secs(2)))
    {
        std::panic::panic_any(CloseFailure {
            _join: closed,
            _outcome: actual_closed,
            _source: std::mem::ManuallyDrop::new(completion_source),
            _disposition_refusal: None,
            _cpu_shutdown: Some(detail),
            _cpu: std::mem::ManuallyDrop::new(provider_cpu),
            _runtime: std::mem::ManuallyDrop::new(runtime),
        });
    }
    drop(port);
    drop(completion_source);
    // close()'s returned health and the observed stable health are actual
    // aliases. Safe projection above did not consume either original payload.
    drop(actual_closed);
    let retained_by_observer = cause_drops.load(Ordering::Acquire) == 0;
    let held_by_observer = panic_after_release.then(|| control.snapshot());
    drop(health_observed.take());
    let cause_released = cause_drops.load(Ordering::Acquire) == usize::from(panic_after_release);
    let deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let facts = control.snapshot();
        if facts.scopes.is_empty() || Instant::now() >= deadline {
            break facts;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    control.close_admission();
    let shutdown = control.shutdown();
    drop(adapter);
    drop(host);
    drop(blocking);
    drop(runtime);
    assert!(
        entered
            && schema_was_published
            && cancelled
            && retired
            && no_new_generation
            && table_drop_entered
            && held
            && watchdog_joined
            && closed_ok
            && borrowed_close_pending
            && (source_healthy != panic_after_release)
            && same_health
            && original_payload
            && cached_failure
            && cause_held_after_disposition
            && retained_by_observer
            && cause_released
    );
    for facts in [&held_call, &held_output]
        .into_iter()
        .chain(held_health.iter())
        .chain(held_after_disposition.iter())
        .chain(held_by_observer.iter())
    {
        assert_eq!(facts.root_responsibilities, 1);
        assert_eq!(facts.result_windows.held_positions, [0, 1, 0, 0]);
        assert!(
            facts
                .scopes
                .iter()
                .any(|s| s.parent.is_some() && !s.own_completed)
        );
    }
    assert_eq!(provider.opens.load(Ordering::Acquire), 1);
    assert_eq!(provider.freezes.load(Ordering::Acquire), 1);
    assert_eq!(provider.public_schemas.load(Ordering::Acquire), 1);
    assert_eq!(
        seals.load(Ordering::Acquire),
        1,
        "actual freeze never reached capability sealing"
    );
    assert_eq!(
        relation_calls.load(Ordering::Acquire),
        0,
        "closed plan accepted a late capability and reached post-deposit relation encoding"
    );
    assert!(!delivered.load(Ordering::Acquire));
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(after.result_windows.held_positions, [0; 4]);
    assert!(after.scopes.is_empty() && shutdown.is_ok());
}

#[test]
fn frozen_mv_target_routing_preserves_exact_provider_facts_and_generic_history() {
    let instance = ConnectorInstanceId::parse("catalog.analytics").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    // This routing test needs no delayed teardown; the original lifecycle test
    // above retains its independently held/released destruction witness.
    drop(entered_rx);
    drop(release);
    let table = Arc::new(ActualFrozenTable {
        entered,
        release: Mutex::new(release_rx),
        destroyed: Arc::new(AtomicBool::new(false)),
    });
    let provider = Arc::new(LateProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        catalog: catalog.clone(),
        table: Mutex::new(None),
        declared_type: novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Int64,
            false,
        ),
        opens: AtomicUsize::new(0),
        freezes: AtomicUsize::new(0),
        public_schemas: AtomicUsize::new(0),
        read_opens: Mutex::new(Vec::new()),
        tiny_source: Mutex::new(None),
        source_prepares: AtomicUsize::new(0),
    });
    let adapter = Arc::new(ReadRuntimeAdapter::new(provider.clone()));
    let props = CatalogProperties::new(
        catalog.clone(),
        provider.descriptor.provider_id.clone(),
        1,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let generic = Arc::new(
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 7)
            .with_catalog_properties(props.clone())
            .unwrap(),
    );
    let host = ConnectorControlHost::new();
    host.register_role_binding(
        ConnectorControlRoleBinding::try_new(
            NormalizedCatalogProperties::try_new(props).unwrap(),
            generic,
            Some(ConnectorControlReadBinding::new(
                adapter.clone(),
                adapter.clone(),
                None,
                Arc::new(LateEncoder {
                    relation_allowed: false,
                    catalog: catalog.clone(),
                    relation_calls: Arc::new(AtomicUsize::new(0)),
                }),
            )),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let materialization = QueryScanMaterialization {
        table: novarocks_spi::connector::ConnectorTableHandle::try_new(
            instance.clone(),
            Bytes::from_static(b"orders"),
        )
        .unwrap(),
        selector: ConnectorReadSelector::SnapshotId(41),
        mv_partition_selection: None,
        planning_lease: host.acquire_current(&instance).unwrap(),
    };
    let session =
        ConnectorSession::try_new("mv-routing", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .unwrap();
    let name = SchemaTableName::try_new("db", "orders").unwrap();
    // FE must forward provider identity bytes, rather than interpret them as a UUID.
    let object =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"\0provider-object\xff")).unwrap();
    let mv_relation = |snapshot| ProviderReadRelationNeed::MvTarget {
        relation: TableIdentity::new(instance.as_str(), "db", "orders"),
        target_table_uuid: "00000000-0000-0000-0000-000000000041".into(),
        target_snapshot_id: snapshot,
        use_affected_partitions: false,
    };
    let open = |relation: &ProviderReadRelationNeed, admitted: &AdmittedRead| {
        *provider.table.lock().unwrap() = Some(table.clone());
        let (opened, work, metadata_kind) = open_relation(
            adapter.as_ref(),
            &session,
            &name,
            relation,
            admitted,
            "db.orders",
        )
        .expect("the admitted route opens its provider table");
        assert_eq!(opened.binding(), adapter.binding());
        assert!(Arc::ptr_eq(adapter.table(&opened).unwrap(), &table));
        assert!(work.is_none());
        assert!(metadata_kind.is_none());
        assert!(provider.table.lock().unwrap().is_none());
        std::mem::take(&mut *provider.read_opens.lock().unwrap())
    };
    for snapshot in [Some(41), None] {
        let mut frozen = materialization.clone();
        frozen.selector = snapshot.map_or(
            ConnectorReadSelector::Current,
            ConnectorReadSelector::SnapshotId,
        );
        assert_eq!(
            open(
                &mv_relation(snapshot),
                &AdmittedRead::MvTarget {
                    materialization: frozen,
                    object_id: object.clone(),
                }
            ),
            [ReadOpenCall::WholeMv(
                name.clone(),
                object.clone(),
                snapshot
            )],
        );
    }
    let selection = |object_id| {
        ConnectorMvTargetPartitionSelection::try_new(
            object_id,
            Bytes::from_static(b"opaque-spec"),
            vec![
                MvExactPartitionField::try_new(
                    Bytes::from_static(b"partition-field"),
                    Bytes::from_static(b"source-field"),
                    MvExactPartitionTransform::Identity,
                )
                .unwrap(),
            ],
            vec![vec![ConnectorMvPartitionValue::String(Arc::from("emea"))]],
            41,
        )
        .unwrap()
    };
    let mut partition_relation = mv_relation(Some(41));
    if let ProviderReadRelationNeed::MvTarget {
        use_affected_partitions,
        ..
    } = &mut partition_relation
    {
        *use_affected_partitions = true;
    }
    let mut partitioned = materialization.clone();
    partitioned.mv_partition_selection = Some(selection(object.clone()));
    assert_eq!(
        open(
            &partition_relation,
            &AdmittedRead::MvTarget {
                materialization: partitioned.clone(),
                object_id: object.clone(),
            }
        ),
        [ReadOpenCall::PartitionMv(
            name.clone(),
            partitioned.mv_partition_selection.clone().unwrap(),
        )],
    );
    for relation in [
        ProviderReadRelationNeed::Data {
            relation: TableIdentity::new(instance.as_str(), "db", "orders"),
            version: ProviderReadVersionNeed::Snapshot(41),
        },
        ProviderReadRelationNeed::FrozenInputSet {
            relation: TableIdentity::new(instance.as_str(), "db", "orders"),
            version: ProviderReadVersionNeed::Snapshot(41),
        },
    ] {
        assert_eq!(
            open(&relation, &AdmittedRead::Relation(materialization.clone())),
            [ReadOpenCall::Generic(
                name.clone(),
                ConnectorReadRelationVersion::SnapshotId(41),
                None
            )],
        );
    }
    *provider.table.lock().unwrap() = Some(table.clone());
    let error = open_relation(
        adapter.as_ref(),
        &session,
        &name,
        &partition_relation,
        &AdmittedRead::Relation(materialization),
        "db.orders",
    )
    .expect_err("ordinary admission cannot open an MV target");
    assert!(
        error
            .to_string()
            .contains("lacks its exact MV target admission")
    );
    partitioned.mv_partition_selection = Some(selection(
        ConnectorTableObjectId::try_new(Bytes::from_static(b"foreign-object")).unwrap(),
    ));
    assert!(
        open_relation(
            adapter.as_ref(),
            &session,
            &name,
            &partition_relation,
            &AdmittedRead::MvTarget {
                materialization: partitioned,
                object_id: object
            },
            "db.orders",
        )
        .is_err(),
        "a foreign selection must fail before provider dispatch"
    );
    assert!(provider.read_opens.lock().unwrap().is_empty());
    assert!(Arc::ptr_eq(
        provider.table.lock().unwrap().as_ref().unwrap(),
        &table
    ));
}

// This component fixture clones tiny immutable facts only. Any attempted CPU
// dispatch or original-scope observation would exceed its routing-only scope.
struct RoutingMustNotRunPreparation;
impl ProviderMetadataCpuPort for RoutingMustNotRunPreparation {
    fn submit_metadata_in(
        &self,
        _: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        panic!("routing fixture must not dispatch preparation CPU")
    }
    fn poll_finished(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("routing fixture has no preparation CPU result")
    }
    fn poll_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("routing fixture cannot certify CPU close")
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        panic!("routing fixture has no CPU health observation")
    }
}
impl ConnectorOriginalResultScopeCheck for RoutingMustNotRunPreparation {
    fn original_deadline(&self) -> Instant {
        panic!("routing fixture must not inspect an original preparation scope")
    }
    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        panic!("routing fixture must not inspect original preparation work")
    }
}

#[test]
fn original_prepared_source_routing_uses_to_and_refuses_missing_or_foreign_facts() {
    use crate::catalog_application::query_bindings::QueryPreparedSourceSelector;
    use novarocks_spi::connector::read_stack::ConnectorReadSourceProjection as P;
    let instance = ConnectorInstanceId::parse("catalog.analytics").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let owner = ConnectorProviderBindingKey {
        instance_id: instance.clone(),
        incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
    };
    let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"source-A")).unwrap();
    let metadata_for = |object: &ConnectorTableObjectId,
                        owner: &ConnectorProviderBindingKey,
                        handle: &'static [u8]| {
        crate::connector::schema_creation::declared_test_metadata(
            ConnectorTableMetadataHeader {
                identity: ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: "db".into(),
                    table: "orders".into(),
                },
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: Some(Bytes::from_static(b"schema-To41")),
                statistics_data_version: None,
                table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                    instance.clone(),
                    Bytes::from_static(handle),
                )
                .unwrap(),
            },
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            )],
        )
        .with_source_read_facts(ConnectorProcessLocalPayload::new(
            owner.clone(),
            Arc::new(object.clone()),
        ))
    };
    let metadata = Arc::new(metadata_for(&object, &owner, b"frozen-source-A-To41"));
    let new_provider = |metadata: Arc<ConnectorTableMetadata>,
                        object: ConnectorTableObjectId,
                        owner: ConnectorProviderBindingKey| {
        Arc::new(LateProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance.clone(),
            },
            catalog: catalog.clone(),
            table: Mutex::new(None),
            declared_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
            opens: AtomicUsize::new(0),
            freezes: AtomicUsize::new(0),
            public_schemas: AtomicUsize::new(0),
            read_opens: Mutex::new(Vec::new()),
            tiny_source: Mutex::new(Some(TinySourceObservation {
                metadata,
                object,
                owner,
            })),
            source_prepares: AtomicUsize::new(0),
        })
    };
    let provider = new_provider(metadata.clone(), object.clone(), owner.clone());
    let adapter = Arc::new(ReadRuntimeAdapter::new(provider.clone()));
    let props = CatalogProperties::new(
        catalog.clone(),
        provider.descriptor.provider_id.clone(),
        1,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let generic = Arc::new(
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 7)
            .with_catalog_properties(props.clone())
            .unwrap(),
    );
    let host = ConnectorControlHost::new();
    host.register_role_binding(
        ConnectorControlRoleBinding::try_new(
            NormalizedCatalogProperties::try_new(props).unwrap(),
            generic,
            Some(ConnectorControlReadBinding::new(
                adapter.clone(),
                adapter.clone(),
                None,
                Arc::new(LateEncoder {
                    relation_allowed: false,
                    catalog: catalog.clone(),
                    relation_calls: Arc::new(AtomicUsize::new(0)),
                }),
            )),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let lease = host.acquire_current(&instance).unwrap();
    let session = ConnectorSession::try_new(
        "prepared-routing",
        "u",
        "UTC",
        "en_US",
        SystemTime::UNIX_EPOCH,
    )
    .unwrap();
    let context = crate::connector::test_request_context();
    let cpu: Arc<dyn ProviderMetadataCpuPort> = Arc::new(RoutingMustNotRunPreparation);
    let original =
        ConnectorOriginalResultScope::from_original(Arc::new(RoutingMustNotRunPreparation));
    let prepare = |adapter: &ReadRuntimeAdapter<LateProvider>,
                   metadata: &ConnectorTableMetadata,
                   object: &ConnectorTableObjectId| {
        let revision = |id| {
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.descriptor.provider_id.clone(),
                object,
                Some(id),
            )
            .unwrap()
        };
        let request = ConnectorReadSourcePreparationRequest::change_window(
            SchemaTableName::try_new("db", "orders").unwrap(),
            metadata.table.clone(),
            metadata.source_read_facts().unwrap().clone(),
            object.clone(),
            ConnectorReadChangeWindow::new(40, 41),
            revision(40),
            revision(41),
        );
        let mut receiving = None;
        let value = futures::executor::block_on(adapter.prepare_source_read(
            &session,
            &request,
            &cpu,
            &original,
            &mut receiving,
        ))
        .unwrap();
        assert!(
            receiving.is_none(),
            "tiny immutable routing facts started a live Source"
        );
        value
    };
    let receipt = prepare(adapter.as_ref(), &metadata, &object);
    assert_eq!(provider.source_prepares.load(Ordering::Acquire), 1);
    let bindings = QueryTableBindingStore::try_new().unwrap();
    let make_binding = |id| {
        let resolved = novarocks_sql::planning::catalog::materialize_connector_read_table(
            novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                catalog: instance.as_str().into(),
                namespace: "db".into(),
                table: "orders".into(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "k".into(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                schema: Arc::new(metadata.schema().clone()),
                binding: id,
                selector: ConnectorReadSelector::SnapshotId(41),
                planning_facts: ConnectorTablePlanningFacts::empty(),
            },
        )
        .unwrap()
        .into_resolved_table();
        let mut value = QueryTableBinding::local(resolved, id);
        value.admission = QueryTableBindingAdmission::Exact(lease.clone());
        value.source_metadata = Some((*metadata).clone());
        value.scan_materialization = Some(QueryScanMaterialization {
            table: metadata.table.clone(),
            selector: ConnectorReadSelector::SnapshotId(41),
            mv_partition_selection: None,
            planning_lease: lease.clone(),
        });
        value
    };
    let id = bindings
        .resolve_or_insert_with_id(
            QueryTableBindingKey::snapshot(instance.as_str(), "db", "orders", 41),
            |id| {
                let mut value = make_binding(id);
                for projection in [P::To, P::From, P::Delta] {
                    value.install_prepared_source_read(
                        receipt.clone(),
                        projection,
                        lease.clone(),
                    )?;
                }
                Ok(value)
            },
        )
        .unwrap();
    let column = novarocks_sql::compiler::fixtures::provider_read_column_need(
        0,
        "k",
        novarocks_physical_plan::ValueType::new(arrow::datatypes::DataType::Int64, false),
        ConnectorValueType::BigInt,
    )
    .unwrap();
    let need = |relation| {
        novarocks_sql::compiler::fixtures::provider_read_need(
            1,
            novarocks_physical_plan::ProviderReadOccurrenceId::new(1),
            id,
            relation,
            vec![column.clone()],
            Vec::new(),
            None,
        )
        .unwrap()
    };
    let relation = TableIdentity::new(instance.as_str(), "db", "orders");
    let data = need(ProviderReadRelationNeed::Data {
        relation: relation.clone(),
        version: ProviderReadVersionNeed::Current,
    });
    let mut prepared = begin_one_read(data, &host, &bindings, &context).unwrap();
    futures::executor::block_on(prepare_observation(&mut prepared, &session)).unwrap();
    assert!(matches!(
        prepared.observation.as_ref(),
        Some(ReadObservation::OriginalPreparedSource)
    ));
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    drop(entered_rx);
    drop(release);
    let table = Arc::new(ActualFrozenTable {
        entered,
        release: Mutex::new(release_rx),
        destroyed: Arc::new(AtomicBool::new(false)),
    });
    *provider.table.lock().unwrap() = Some(table.clone());
    let handle = open_relation(
        adapter.as_ref(),
        &session,
        &prepared.opening.table,
        prepared.need.relation(),
        &prepared.opening.admitted,
        "db.orders",
    )
    .unwrap()
    .0;
    assert!(Arc::ptr_eq(adapter.table(&handle).unwrap(), &table));
    assert_eq!(
        *provider.read_opens.lock().unwrap(),
        [ReadOpenCall::Prepared(
            ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(40, 41)),
            P::To
        )]
    );
    assert_eq!(
        provider.source_prepares.load(Ordering::Acquire),
        1,
        "read routing re-prepared original facts"
    );
    let delta_relation = ProviderReadRelationNeed::Delta {
        relation: relation.clone(),
        from_snapshot_id: 40,
        to_snapshot_id: 41,
    };
    let exact = need(delta_relation.clone());
    let exact = prepare_read_opening(&exact, &host, &bindings, &context).unwrap();
    assert!(
        matches!(exact.admitted,AdmittedRead::PreparedSource(ref value) if value.projection==P::Delta)
    );
    *provider.table.lock().unwrap() = Some(table.clone());
    let delta_handle = open_relation(
        adapter.as_ref(),
        &session,
        &exact.table,
        &delta_relation,
        &exact.admitted,
        "db.orders",
    )
    .unwrap()
    .0;
    assert!(Arc::ptr_eq(adapter.table(&delta_handle).unwrap(), &table));
    assert!(
        matches!(provider.read_opens.lock().unwrap().last(), Some(ReadOpenCall::Prepared(ConnectorReadSourceSelection::ChangeWindow(window), P::Delta)) if *window == ConnectorReadChangeWindow::new(40, 41))
    );
    let missing = need(ProviderReadRelationNeed::Delta {
        relation: relation.clone(),
        from_snapshot_id: 39,
        to_snapshot_id: 41,
    });
    assert!(prepare_read_opening(&missing, &host, &bindings, &context).is_err());
    assert_eq!(provider.source_prepares.load(Ordering::Acquire), 1);
    let admitted = bindings.binding(id).unwrap();
    assert!(
        admitted
            .prepared_source_reads
            .contains_key(&QueryPreparedSourceSelector::Snapshot(41))
    );
    let other_owner = ConnectorProviderBindingKey {
        incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
        ..owner.clone()
    };
    let other_metadata = Arc::new(metadata_for(&object, &other_owner, b"frozen-source-A-To41"));
    let other_adapter = ReadRuntimeAdapter::new(new_provider(
        other_metadata.clone(),
        object.clone(),
        other_owner,
    ));
    let old = prepare(&other_adapter, &other_metadata, &object);
    assert!(
        make_binding(id)
            .install_prepared_source_read(old, P::To, lease.clone())
            .is_err(),
        "same catalog content from a foreign generation was installed"
    );
    let other_object = ConnectorTableObjectId::try_new(Bytes::from_static(b"source-B")).unwrap();
    let other_metadata = Arc::new(metadata_for(&other_object, &owner, b"frozen-source-B-To41"));
    let other_adapter = ReadRuntimeAdapter::new(new_provider(
        other_metadata.clone(),
        other_object.clone(),
        owner,
    ));
    let other = prepare(&other_adapter, &other_metadata, &other_object);
    assert!(
        make_binding(id)
            .install_prepared_source_read(other, P::To, lease.clone())
            .is_err()
    );
    let ordinary = QueryTableBindingStore::try_new().unwrap();
    let ordinary_id = ordinary
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(instance.as_str(), "db", "orders"),
            |id| Ok(make_binding(id)),
        )
        .unwrap();
    let delta = novarocks_sql::compiler::fixtures::provider_read_need(
        1,
        novarocks_physical_plan::ProviderReadOccurrenceId::new(1),
        ordinary_id,
        ProviderReadRelationNeed::Delta {
            relation,
            from_snapshot_id: 40,
            to_snapshot_id: 41,
        },
        vec![column],
        Vec::new(),
        None,
    )
    .unwrap();
    let mut ordinary = begin_one_read(delta, &host, &ordinary, &context).unwrap();
    assert!(matches!(
        ordinary.opening.admitted,
        AdmittedRead::Relation(_)
    ));
    assert!(
        futures::executor::block_on(prepare_observation(&mut ordinary, &session)).is_err(),
        "original fixture exposes no generic window"
    );
    assert!(
        matches!(provider.read_opens.lock().unwrap().last(),Some(ReadOpenCall::Delta(_,window)) if *window==ConnectorReadChangeWindow::new(40,41)),
        "ordinary explicit Delta stopped using its original typed author"
    );
    assert_eq!(provider.source_prepares.load(Ordering::Acquire), 1);
}
