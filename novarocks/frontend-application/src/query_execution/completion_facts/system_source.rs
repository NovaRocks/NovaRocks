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

//! Statement-local snapshot custody. Borrowed waits only drive its original
//! finite I/O operation; they never own an SDK future or inventory cursor.
use super::*;
use crate::mv::domain::{
    freshness::{
        CurrentSourceCapture, CurrentSourceObservationError, observe_current_source_with_pin,
        observe_current_sources,
    },
    staged_create::{CurrentMetadataCapture, CurrentProjectionMetadataCpu},
};
use crate::system_tables::{
    FrontendSystemTableProvider, ListableMaterializedViews, MaterializedViewFacts,
    MaterializedViewInventory, PinnedCatalogListing, RowStage, StatementSystemTableSource,
    repository_error,
};
use crate::wake::NotifyWake;
use novarocks_mv_application::{
    freshness::{MvSourceObservation, MvSourceState},
    persistence::codec::RelationOccurrence,
};
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    preparation::SystemTableSnapshotFactPort,
    system_tables::{
        SystemTableError, SystemTableProvider, SystemTableRequest, SystemTableSnapshot,
    },
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ProviderMetadataCpuPort};
use novarocks_sql::compiler::{SqlCompileError, SystemTableSnapshotFact, SystemTableSnapshotNeed};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

type Inventory = Box<dyn MaterializedViewInventory>;
#[derive(Default)]
struct OwnedStore {
    owner: Option<crate::state_store::execution::StateStoreOriginalOwner>,
    requests: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
}
struct StoreLoan<'a> {
    receiver: &'a FrontendSystemTableFacts,
    body: Option<OwnedStore>,
}
impl Drop for StoreLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self
            .receiver
            .store
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "one original system StateStore loan");
        *slot = self.body.take();
    }
}

type Operation = Pin<Box<dyn Future<Output = Response> + Send>>;
enum Response {
    Names(Result<Vec<String>, SystemTableError>),
    Exists(Result<bool, SystemTableError>),
    Open(Result<Inventory, SystemTableError>),
    Next(
        Inventory,
        Result<Option<MaterializedViewFacts>, SystemTableError>,
    ),
    Current(Result<Vec<MvSourceObservation>, SystemTableError>),
}
#[derive(Default)]
struct IoState {
    future: Option<Operation>,
    response: Option<Response>,
    inventory: Option<Inventory>,
    in_foreign: bool,
    exited: bool,
    closing: bool,
    joined: bool,
    pins: BTreeMap<String, Arc<PinnedCatalogListing>>,
    ports: Vec<Arc<dyn ProviderMetadataCpuPort>>,
    health: Option<StatementNativeHealthFailure>,
}
#[derive(Debug)]
struct IoExitUnproven;
impl std::fmt::Display for IoExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("system table original I/O exit is unproven")
    }
}
impl std::error::Error for IoExitUnproven {}
#[derive(Debug)]
struct HealthCause(StatementNativeHealthFailure);
impl std::fmt::Display for HealthCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.detail())
    }
}
impl novarocks_failure_contract::FailureProjection for HealthCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&novarocks_query_application::execution_error::QueryFailureReason::BlockingSourceContract)
    }
}
impl std::error::Error for HealthCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.cause())
    }
}
fn health_error(health: StatementNativeHealthFailure) -> SystemTableError {
    ConnectorError::from_domain_static(
        ConnectorErrorKind::Internal,
        novarocks_failure_contract::SharedFailureCause::new(HealthCause(health)),
        "system table original exit is unproven",
    )
    .into()
}
fn work_error(error: novarocks_workload_control::WorkError) -> SystemTableError {
    crate::system_tables::control_error(novarocks_type_contract::CompileControlError::Source(
        novarocks_failure_contract::SharedFailureCause::new(error),
    ))
}
fn sql_error(error: SystemTableError) -> SqlCompileError {
    match error {
        SystemTableError::Provider(error) => {
            use std::error::Error;
            if let Some(cause) = error
                .source()
                .and_then(|e| e.downcast_ref::<novarocks_failure_contract::SharedFailureCause>())
            {
                if let Some(control) =
                    cause.downcast_ref::<novarocks_type_contract::CompileControlError>()
                {
                    SqlCompileError::from(control.clone())
                } else {
                    SqlCompileError::CompilationSource(cause.clone())
                }
            } else {
                SqlCompileError::CompilationSource(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                )
            }
        }
        SystemTableError::InvalidRequest(message) | SystemTableError::Internal(message) => {
            SqlCompileError::Compilation(message)
        }
    }
}
/// The receiving header is data owned by FrontendCompletionCustody. It has no
/// runtime, task identity, alternate resource window or background driver.
pub(super) struct FrontendSystemTableFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    local_views: novarocks_query_application::view::LocalViewSnapshot,
    local_schemas: Vec<String>,
    notify: Arc<tokio::sync::Notify>,
    // Strong custody of the original admitted I/O owner; context itself is weak.
    _io: Option<Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>>,
    io: Mutex<IoState>,
    store: Mutex<Option<OwnedStore>>,
    store_foreign: std::sync::atomic::AtomicBool,
    capture: Arc<CurrentMetadataCapture>,
    self_ref: std::sync::Weak<FrontendSystemTableFacts>,
    resolving: tokio::sync::Mutex<()>,
    unproven: StatementNativeHealthFailure,
}
struct OperationLoan<'a> {
    original: &'a FrontendSystemTableFacts,
    future: Option<Operation>,
}
impl Drop for OperationLoan<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let mut state = self.original.io.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.future.is_none(), "one original system I/O future");
            state.future = Some(future);
        }
    }
}
impl FrontendSystemTableFacts {
    pub(super) fn new(
        owners: CompletionFactOwners,
        scope: StatementFactScope,
    ) -> Result<Arc<Self>, SqlCompileError> {
        let local_views = scope
            .bindings
            .capture_local_views(|| owners.view_service.catalog_snapshot())?;
        let local_schemas = owners
            .catalog_service
            .local()
            .read()
            .expect("local catalog snapshot lock")
            .database_names()
            .map(str::to_owned)
            .collect();
        let io = scope
            .connector_context
            .io_admission()
            .and_then(|p| p.upgrade());
        Ok(Arc::new_cyclic(|this| Self {
            owners,
            scope,
            local_views,
            local_schemas,
            notify: Arc::new(tokio::sync::Notify::new()),
            _io: io,
            io: Mutex::new(IoState::default()),
            store: Mutex::new(Some(OwnedStore::default())),
            store_foreign: std::sync::atomic::AtomicBool::new(false),
            capture: CurrentMetadataCapture::new(),
            self_ref: this.clone(),
            resolving: tokio::sync::Mutex::const_new(()),
            unproven: StatementNativeHealthFailure::actual_exit_unproven(
                "system table original I/O exit is unproven",
                Arc::new(IoExitUnproven),
            ),
        }))
    }
    fn planning(&self) -> Result<&StatementPlanningInputs, SystemTableError> {
        self.scope.planning.as_ref().ok_or_else(|| {
            SystemTableError::InvalidRequest(
                "system snapshot has no original planning source".into(),
            )
        })
    }
    fn cpu(&self) -> Result<&Arc<FrontendPlanningCpuSource>, SystemTableError> {
        self.planning()?.cpu.as_ref().ok_or_else(|| {
            SystemTableError::Internal(
                "system snapshot original CPU source is not installed".into(),
            )
        })
    }
    fn pin(&self, catalog: &str) -> Result<Arc<PinnedCatalogListing>, SystemTableError> {
        {
            let state = self.io.lock().unwrap_or_else(|p| p.into_inner());
            if state.closing {
                return Err(SystemTableError::InvalidRequest(
                    "system snapshot is closing".into(),
                ));
            }
            if let Some(pin) = state.pins.get(catalog) {
                return Ok(Arc::clone(pin));
            }
        }
        self.scope
            .bindings
            .require_catalog_observation_allowed()
            .map_err(SystemTableError::Internal)?;
        if self._io.is_none() {
            return Err(work_error(novarocks_workload_control::WorkError::NotReady));
        }
        let pin = crate::connector::metadata_binding_typed(
            self.owners.connector_control.as_ref(),
            catalog,
        )?;
        let pin = Arc::new(PinnedCatalogListing::new(
            pin,
            self.scope.connector_context.clone(),
        )?);
        let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
        if state.closing {
            return Err(SystemTableError::InvalidRequest(
                "system snapshot is closing".into(),
            ));
        }
        Ok(Arc::clone(
            state.pins.entry(catalog.to_owned()).or_insert(pin),
        ))
    }
    fn factory(&self) -> Result<CurrentProjectionMetadataCpu, SystemTableError> {
        let planning = self.planning()?;
        Ok(CurrentProjectionMetadataCpu::business(
            self.owners
                .blocking
                .provider_creator_cpu()
                .map_err(work_error)?,
            self.scope.capacity.scope().clone(),
            self.scope.capacity.try_window_alias().map_err(work_error)?,
            planning.cancellation.clone(),
            self.owners.blocking.runtime().clone(),
            Arc::new(NotifyWake::new(Arc::clone(&self.notify))),
        ))
    }
    fn install(&self, future: Operation) -> Result<(), SystemTableError> {
        let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
        if state.closing || state.in_foreign || state.future.is_some() || state.response.is_some() {
            return Err(SystemTableError::Internal(
                "system snapshot original I/O receiving is occupied".into(),
            ));
        }
        state.future = Some(future);
        state.exited = false;
        Ok(())
    }
    fn store_requests(
        &self,
    ) -> Result<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>, SystemTableError>
    {
        match &self.scope.state_store {
            StatementStateStore::Governed(Some(requests)) => return Ok(Arc::clone(requests)),
            StatementStateStore::Governed(None) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "system materialized-view listing has no original StateStore execution",
                )
                .into());
            }
            StatementStateStore::Legacy => {}
        }
        let mut slot = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let body = slot
            .as_mut()
            .expect("original system StateStore is not loaned during admission");
        if let Some(requests) = &body.requests {
            return Ok(Arc::clone(requests));
        }
        let binding = self.owners.state_store.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "system materialized-view listing requires original StateStore execution",
            )
        })?;
        let owner = binding
            .for_root(self.scope.capacity.root_scope().clone())
            .map_err(|error| ConnectorError::from_domain(ConnectorErrorKind::Internal, error))?;
        let requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests> = Arc::new(
            owner
                .execution_requests(
                    binding.transaction_deadline,
                    Some(self.scope.connector_context.deadline()),
                )
                .map_err(|error| {
                    ConnectorError::from_domain(ConnectorErrorKind::Internal, error)
                })?,
        );
        body.owner = Some(owner);
        body.requests = Some(Arc::clone(&requests));
        Ok(requests)
    }
    fn poll_store_progress(&self, cx: &mut Context<'_>) {
        let progress = self
            .store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|body| body.owner.as_ref())
            .map(|owner| owner.progress_handle());
        if let Some(progress) = progress
            && let Err(error) = progress.poll_progress(cx)
        {
            self.io
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .health
                .get_or_insert_with(|| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "system StateStore original exit is unproven",
                        Arc::new(error),
                    )
                });
        }
    }
    fn poll_store_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        use std::sync::atomic::Ordering;
        if self.store_foreign.swap(true, Ordering::AcqRel) {
            return Poll::Pending;
        }
        let body = self.store.lock().unwrap_or_else(|p| p.into_inner()).take();
        let mut loan = StoreLoan {
            receiver: self,
            body,
        };
        let result = match loan
            .body
            .as_mut()
            .expect("original StateStore body")
            .owner
            .as_mut()
        {
            Some(owner) => owner.poll_closed(cx),
            None => Poll::Ready(Ok(())),
        };
        if let Poll::Ready(Err(error)) = &result {
            self.io
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .health
                .get_or_insert_with(|| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "system StateStore original close failed",
                        Arc::new(error.clone()),
                    )
                });
        }
        drop(loan);
        self.store_foreign.store(false, Ordering::Release);
        result.map(|_| ())
    }
    fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_store_progress(cx);
        let future = {
            let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
            if state.in_foreign {
                return Poll::Pending;
            }
            let Some(future) = state.future.take() else {
                return Poll::Ready(());
            };
            state.in_foreign = true;
            future
        };
        let mut loan = OperationLoan {
            original: self,
            future: Some(future),
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
                state.future = loan.future.take();
                state.in_foreign = false;
                Poll::Pending
            }
            Poll::Ready(response) => {
                self.io.lock().unwrap_or_else(|p| p.into_inner()).response = Some(response);
                // Stabilize the result before the actual foreign future destructor.
                drop(loan.future.take());
                let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
                state.exited = true;
                state.in_foreign = false;
                Poll::Ready(())
            }
        }
    }
    async fn wait_progress(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            tokio::select! {
                () = std::future::poll_fn(|cx| self.poll_progress(cx)) => return,
                () = &mut notified => {}
            }
        }
    }
    async fn receive(&self) -> Response {
        self.wait_progress().await;
        let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            state.exited && !state.in_foreign,
            "actual system I/O exit before handoff"
        );
        state
            .response
            .take()
            .expect("same original system I/O response")
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        let (foreign, retained, ports) = {
            let state = self.io.lock().unwrap_or_else(|p| p.into_inner());
            (state.in_foreign, state.health.clone(), state.ports.clone())
        };
        retained
            .or_else(|| {
                (foreign
                    || self
                        .store_foreign
                        .load(std::sync::atomic::Ordering::Acquire))
                .then(|| self.unproven.clone())
            })
            .or_else(|| self.capture.local_health_failure())
            .or_else(|| {
                ports.iter().find_map(|p| p.local_failure()).map(|cause| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "system metadata CPU exit is unproven",
                        cause,
                    )
                })
            })
    }
    pub(super) async fn close(&self) -> Result<(), StatementNativeHealthFailure> {
        self.io.lock().unwrap_or_else(|p| p.into_inner()).closing = true;
        self.wait_progress().await;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            tokio::select! {
                () = std::future::poll_fn(|cx| self.capture.poll_closed(cx)) => break,
                () = &mut notified => {}
            }
        }
        std::future::poll_fn(|cx| self.poll_store_closed(cx)).await;
        let ports = self
            .io
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .ports
            .clone();
        for port in ports {
            if let Err(error) = std::future::poll_fn(|cx| port.poll_closed(cx)).await {
                let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
                state.health.get_or_insert_with(|| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "system metadata CPU close failed",
                        Arc::new(error),
                    )
                });
            }
        }
        if let Some(health) = self.local_health_failure() {
            return Err(health);
        }
        let retired = {
            let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined {
                return Ok(());
            }
            state.in_foreign = true;
            (
                state.response.take(),
                state.inventory.take(),
                std::mem::take(&mut state.ports),
                std::mem::take(&mut state.pins),
            )
        };
        drop(retired);
        let mut state = self.io.lock().unwrap_or_else(|p| p.into_inner());
        state.in_foreign = false;
        state.joined = true;
        Ok(())
    }
    async fn next_inventory(&self) -> Result<Option<MaterializedViewFacts>, SystemTableError> {
        let inventory = self
            .io
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .inventory
            .take()
            .ok_or_else(|| {
                SystemTableError::Internal("original MV inventory cursor is absent".into())
            })?;
        self.install(Box::pin(async move {
            let mut inventory = inventory;
            let result = inventory.next().await;
            Response::Next(inventory, result)
        }))?;
        let Response::Next(inventory, result) = self.receive().await else {
            panic!("original MV inventory response");
        };
        self.io.lock().unwrap_or_else(|p| p.into_inner()).inventory = Some(inventory);
        result
    }
}
struct InventoryBorrow {
    source: Arc<FrontendSystemTableFacts>,
}
#[async_trait]
impl MaterializedViewInventory for InventoryBorrow {
    async fn next(&mut self) -> Result<Option<MaterializedViewFacts>, SystemTableError> {
        self.source.next_inventory().await
    }
}
struct CurrentSources {
    pins: BTreeMap<String, Arc<PinnedCatalogListing>>,
    factory: CurrentProjectionMetadataCpu,
    capture: Arc<CurrentMetadataCapture>,
    observer: Arc<dyn MvStorageObservationPort>,
}
#[async_trait]
impl CurrentSourceCapture for CurrentSources {
    async fn observe(
        &self,
        table: novarocks_spi::connector::ConnectorTableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSourceState, CurrentSourceObservationError> {
        let pin = self
            .pins
            .get(table.instance_id.as_str())
            .expect("selected original source catalog");
        observe_current_source_with_pin(
            pin.pin(),
            self.observer.as_ref(),
            &self.factory,
            &self.capture,
            table,
            context,
        )
        .await
    }
}
#[async_trait]
impl StatementSystemTableSource for FrontendSystemTableFacts {
    async fn schema_names(
        &self,
        catalog: &str,
        _: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        if catalog == novarocks_types::naming::DEFAULT_CATALOG {
            return Ok(self.local_schemas.clone());
        }
        let pin = self.pin(catalog)?;
        self.install(Box::pin(async move {
            Response::Names(pin.schema_names().await.map_err(Into::into))
        }))?;
        let Response::Names(result) = self.receive().await else {
            panic!("original namespace listing response");
        };
        result
    }
    async fn schema_exists(
        &self,
        catalog: &str,
        schema: &str,
        _: &ConnectorRequestContext,
    ) -> Result<bool, SystemTableError> {
        if catalog == novarocks_types::naming::DEFAULT_CATALOG {
            return Ok(self.local_schemas.iter().any(|name| name == schema));
        }
        let pin = self.pin(catalog)?;
        let schema = schema.to_owned();
        self.install(Box::pin(async move {
            Response::Exists(pin.schema_exists(&schema).await.map_err(Into::into))
        }))?;
        let Response::Exists(result) = self.receive().await else {
            panic!("original namespace existence response");
        };
        result
    }
    async fn table_names(
        &self,
        catalog: &str,
        schema: &str,
        _: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        let pin = self.pin(catalog)?;
        let schema = schema.to_owned();
        self.install(Box::pin(async move {
            Response::Names(pin.table_names(&schema).await.map_err(Into::into))
        }))?;
        let Response::Names(result) = self.receive().await else {
            panic!("original table listing response");
        };
        result
    }
    async fn view_names(
        &self,
        catalog: &str,
        schema: &str,
        _: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError> {
        let pin = self.pin(catalog)?;
        let schema = schema.to_owned();
        let cpu = self.factory()?.port_for(pin.pin()).map_err(work_error)?;
        self.io
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .ports
            .push(Arc::clone(&cpu));
        self.install(Box::pin(async move {
            Response::Names(pin.view_names(&schema, cpu).await.map_err(Into::into))
        }))?;
        let Response::Names(result) = self.receive().await else {
            panic!("original view listing response");
        };
        result
    }
    fn local_view_snapshot(&self) -> &novarocks_query_application::view::LocalViewSnapshot {
        &self.local_views
    }
    async fn materialized_views(
        &self,
        include_freshness: bool,
        include_definition: bool,
        _: &ConnectorRequestContext,
    ) -> Result<Inventory, SystemTableError> {
        let requests = self.store_requests()?;
        let readiness = Arc::clone(&self.owners.mv_readiness);
        self.install(Box::pin(async move {
            Response::Open(
                readiness
                    .in_requests(requests)
                    .local_projection_inventory()
                    .await
                    .map(|inventory| {
                        Box::new(ListableMaterializedViews::new(
                            inventory,
                            include_freshness,
                            include_definition,
                        )) as Inventory
                    })
                    .map_err(repository_error),
            )
        }))?;
        let Response::Open(result) = self.receive().await else {
            panic!("original MV inventory open response");
        };
        let inventory = result?;
        // The exhausted previous cursor is retired outside the original mutex.
        let retired = self
            .io
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .inventory
            .replace(inventory);
        drop(retired);
        Ok(Box::new(InventoryBorrow {
            source: self
                .self_ref
                .upgrade()
                .expect("same original snapshot source"),
        }))
    }
    async fn current_sources(
        &self,
        occurrences: &[RelationOccurrence],
        _: &ConnectorRequestContext,
    ) -> Result<Vec<MvSourceObservation>, SystemTableError> {
        let mut pins = BTreeMap::new();
        for occurrence in occurrences {
            let catalog = &occurrence.catalog_at_binding;
            if !pins.contains_key(catalog) {
                pins.insert(catalog.clone(), self.pin(catalog)?);
            }
        }
        let source = CurrentSources {
            pins,
            factory: self.factory()?,
            capture: Arc::clone(&self.capture),
            observer: Arc::clone(&self.owners.mv_storage_observation),
        };
        let occurrences = occurrences.to_vec();
        let context = self.scope.connector_context.clone();
        self.install(Box::pin(async move {
            Response::Current(
                observe_current_sources(&source, &occurrences, &context)
                    .await
                    .map_err(|e| e.into_connector().into()),
            )
        }))?;
        let Response::Current(result) = self.receive().await else {
            panic!("original Current source response");
        };
        result
    }
    fn begin_snapshot(
        &self,
        declaration: Arc<novarocks_sql::system_table::SystemTableDeclaration>,
        required_columns: &[usize],
    ) -> Result<(), SystemTableError> {
        self.cpu()?
            .begin_system_snapshot(
                declaration,
                required_columns,
                &self.scope.capacity,
                self.planning()?.control.clone(),
            )
            .map_err(health_error)?
    }
    fn stage_row(&self, values: &[Option<&str>]) -> Result<RowStage, SystemTableError> {
        self.cpu()?.stage_system_row(values).map_err(health_error)?
    }
    async fn materialize_batch(&self) -> Result<(), SystemTableError> {
        self.cpu()?
            .materialize_system_batch()
            .await
            .map_err(health_error)?
    }
    async fn finish_snapshot(&self) -> Result<SystemTableSnapshot, SystemTableError> {
        self.cpu()?
            .finish_system_snapshot()
            .await
            .map_err(health_error)?
    }
}
#[async_trait]
// Design: ADR-0226 (docs/adr/ADR-0226-engine-metadata-through-typed-system-relations.md)
impl SystemTableSnapshotFactPort for FrontendSystemTableFacts {
    async fn resolve_system_tables(
        &self,
        needs: &[SystemTableSnapshotNeed],
    ) -> Result<Vec<SystemTableSnapshotFact>, SqlCompileError> {
        let _round = self.resolving.lock().await;
        let source = self
            .self_ref
            .upgrade()
            .expect("same original snapshot source");
        let provider =
            FrontendSystemTableProvider::new(Arc::clone(&self.owners.system_relations), source);
        let mut facts = Vec::with_capacity(needs.len());
        for need in needs {
            let request = SystemTableRequest::try_new(
                Arc::clone(need.declaration()),
                need.required_columns().to_vec(),
                need.key_constraint().clone(),
                &self.scope.connector_context,
                self.planning().map_err(sql_error)?.batch_target_rows,
            )
            .map_err(sql_error)?;
            let snapshot = provider.snapshot(&request).await.map_err(sql_error)?;
            facts.push(
                SystemTableSnapshotFact::try_new(need, snapshot.batches, snapshot.storage)
                    .map_err(|e| SqlCompileError::Compilation(e.to_string()))?,
            );
        }
        Ok(facts)
    }
}

#[cfg(test)]
mod tests;
