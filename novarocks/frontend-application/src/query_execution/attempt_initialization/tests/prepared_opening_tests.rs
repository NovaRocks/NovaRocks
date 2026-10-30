// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual Internal Source opening/retirement. Only Provider facts are a fixture.
use super::*;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use crate::query_execution::native_execution_adapter::source_cohort::tests as original_source;
use novarocks_spi::connector as spi;
use read::adapter::*;
use spi::read_stack as read;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::SystemTime;

// The same original authority-event driver used by governed View/MV owner
// tests. A forwarder can enqueue joint admission AFTER its caller's last poll;
// caller-result wakes alone cannot drive that request to its original grant.
async fn with_original_driver<F: Future>(
    control: &WorkloadControl,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    future: F,
) -> F::Output {
    use novarocks_workload_control::joint_admission::JointProgress;
    tokio::pin!(future);
    loop {
        match driver.step() {
            JointProgress::More => tokio::task::yield_now().await,
            JointProgress::Idle(revision) => tokio::select! {
                biased;
                result = &mut future => return result,
                _ = control.wait_progress(revision) => tokio::task::yield_now().await,
            },
        }
    }
}

fn unused() -> spi::ConnectorError {
    spi::ConnectorError::new(
        spi::ConnectorErrorKind::Unsupported,
        "unused original opening fixture capability",
    )
}
#[derive(Clone, Debug)]
struct Table;
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Column;
impl read::ColumnHandle for Column {}
#[derive(Debug)]
struct Split;
impl read::ConnectorSplit for Split {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

struct Probe {
    entered: AtomicBool,
    cpu_calls: AtomicUsize,
    child_drops: AtomicUsize,
    raw_received: AtomicBool,
    close_calls: AtomicUsize,
    source_drops: AtomicUsize,
    drop_entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
    child: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    actual_context: Mutex<Option<spi::ConnectorRequestContext>>,
    close_error: spi::ConnectorError,
}
impl Probe {
    fn release_drop(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Rescue(Arc<Probe>);
impl Drop for Rescue {
    fn drop(&mut self) {
        self.0.release_drop();
    }
}
struct OriginalChild {
    receive: tokio::sync::oneshot::Receiver<()>,
    probe: Arc<Probe>,
}
impl Future for OriginalChild {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        std::pin::Pin::new(&mut self.receive)
            .poll(cx)
            .map(|value| value.unwrap())
    }
}
impl Drop for OriginalChild {
    fn drop(&mut self) {
        assert!(
            self.probe.raw_received.load(Ordering::SeqCst) || std::thread::panicking(),
            "raw receipt precedes this child Drop"
        );
        self.probe.child_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct OriginalOpen {
    descriptor: spi::ConnectorInstanceDescriptor,
    epoch: spi::ProviderBindingEpoch,
    probe: Arc<Probe>,
    child: Mutex<Option<OriginalChild>>,
}
impl spi::ProviderMetadataCpuSource for OriginalOpen {
    fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> spi::ProviderBindingEpoch {
        self.epoch
    }
    fn run_metadata(&self) {
        self.probe.cpu_calls.fetch_add(1, Ordering::SeqCst);
    }
}
struct Source(Arc<Probe>);
impl ProviderReadSplitSource<Provider> for Source {
    fn next_batch(
        &mut self,
        _: usize,
        _: &read::DynamicFilterSnapshot<Column>,
    ) -> Result<read::ConnectorSplitBatch<Split>, spi::ConnectorError> {
        panic!("cancelled initializer must never enumerate")
    }
    fn is_finished(&self) -> bool {
        false
    }
    fn close(&mut self) -> Result<(), spi::ConnectorError> {
        assert_eq!(self.0.close_calls.fetch_add(1, Ordering::SeqCst), 0);
        Err(self.0.close_error.clone())
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.0.drop_entered.notify_one();
        let mut released = self.0.released.lock().unwrap();
        while !*released {
            released = self.0.changed.wait(released).unwrap();
        }
        self.0.source_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Provider {
    descriptor: spi::ConnectorInstanceDescriptor,
    epoch: spi::ProviderBindingEpoch,
    catalog: spi::CatalogHandle,
    probe: Arc<Probe>,
}
impl ProviderReadRuntime for Provider {
    type Table = Table;
    type Column = Column;
    type Transaction = ();
    type Split = Split;
    fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &spi::CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}
impl ProviderReadSplitManager for Provider {
    fn begin_get_splits(
        self: Arc<Self>,
        mut request: ProviderReadSplitOpenRequest<Self>,
        owner: &mut Option<spi::ConnectorProcessLocalPayload>,
    ) -> Result<ProviderReadSplitOpenFuture<Self>, spi::ConnectorError> {
        assert!(owner.is_none());
        let packet = request
            .preparation
            .take()
            .expect("ProductionSourceOpening supplied original Internal metadata preparation");
        packet.original().check_active().unwrap();
        let context = self.probe.actual_context.lock().unwrap().clone().unwrap();
        let child = self.probe.child.lock().unwrap().take().unwrap();
        let source = Arc::new(OriginalOpen {
            descriptor: self.descriptor.clone(),
            epoch: self.epoch,
            probe: self.probe.clone(),
            child: Mutex::new(Some(OriginalChild {
                receive: child,
                probe: self.probe.clone(),
            })),
        });
        *owner = Some(spi::ConnectorProcessLocalPayload::new(
            spi::ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.epoch,
            },
            source.clone(),
        ));
        Ok(Box::pin(async move {
            let mut input = Some(spi::ProviderMetadataCpuInput(source.clone()));
            packet.cpu().submit_metadata_in(&mut input)?;
            assert!(input.is_none());
            std::future::poll_fn(|cx| packet.cpu().poll_finished(cx)).await?;
            // An actual original MIO request cell remains held through the
            // already-started child, even after the borrowing initializer stops.
            let permit = context
                .admit_selected_io(spi::SharedDependency::LocalHost)
                .await?
                .expect("same positive MIO author");
            source.probe.entered.store(true, Ordering::SeqCst);
            std::future::poll_fn(|cx| {
                let mut child = source.child.lock().unwrap();
                match std::pin::Pin::new(child.as_mut().unwrap()).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => {
                        source.probe.raw_received.store(true, Ordering::SeqCst);
                        drop(child.take());
                        Poll::Ready(())
                    }
                }
            })
            .await;
            drop(permit);
            Ok(Box::new(Source(source.probe.clone())) as Box<dyn ProviderReadSplitSource<Self>>)
        }))
    }
    fn get_splits<'a>(
        &'a self,
        _: &'a read::ConnectorSession,
        _: &'a Table,
        _: &'a [read::Assignment<Column>],
        _: &'a BTreeSet<Column>,
        _: &'a read::Constraint<Column>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Box<dyn ProviderReadSplitSource<Self>>, spi::ConnectorError>,
    > {
        panic!("prepared opening cannot use ordinary split fallback")
    }
}
impl ProviderReadMetadata for Provider {
    type PreparedSourceRead = UnsupportedPreparedSourceRead;
    type PreparedRelation = ();
    type FrozenSource = ();
    fn prepare_relation<'a>(
        &'a self,
        _: &'a read::ConnectorSession,
        _: &'a read::ConnectorReadRelationPreparationRequest,
    ) -> futures::future::BoxFuture<'a, Result<Option<()>, spi::ConnectorError>> {
        Box::pin(async { Err(unused()) })
    }
    fn create_prepared_relation(
        &self,
        _: (),
    ) -> Result<Option<ProviderReadOpenedRelation<Table>>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_table_handle(
        &self,
        _: &read::ConnectorSession,
        _: &read::SchemaTableName,
        _: read::ConnectorReadRelationVersion,
        _: Option<&str>,
    ) -> Result<Option<Table>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_pinned_file_set_handle(
        &self,
        _: &read::ConnectorSession,
        _: &read::SchemaTableName,
        _: &spi::ConnectorPinnedFileSet,
    ) -> Result<Option<Table>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_column_bindings(
        &self,
        _: &read::ConnectorSession,
        _: &Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Column>>, spi::ConnectorError> {
        Err(unused())
    }
    fn apply_filter(
        &self,
        _: &read::ConnectorSession,
        _: &Table,
        _: &read::Constraint<Column>,
    ) -> ProviderReadFilterResult<Table, Column> {
        Err(unused())
    }
    fn apply_projection(
        &self,
        _: &read::ConnectorSession,
        _: &Table,
        _: &[read::Assignment<Column>],
    ) -> Result<Option<Table>, spi::ConnectorError> {
        Err(unused())
    }
    fn apply_limit(
        &self,
        _: &read::ConnectorSession,
        _: &Table,
        _: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Table>>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_system_table_plan(
        &self,
        _: &read::ConnectorSession,
        _: &read::SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Table>>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_change_window_plan(
        &self,
        _: &read::ConnectorSession,
        _: &read::SchemaTableName,
        _: read::ConnectorReadChangeWindow,
    ) -> Result<Option<Table>, spi::ConnectorError> {
        Err(unused())
    }
    fn get_table_execute_plan(
        &self,
        _: &read::ConnectorSession,
        _: &read::SchemaTableName,
        _: read::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Table>, spi::ConnectorError> {
        Err(unused())
    }
}
impl spi::ConnectorMetadata for Provider {
    fn provider_metadata_cpu_author(
        &self,
    ) -> Option<(spi::ConnectorProviderBindingKey, std::any::TypeId)> {
        Some((
            spi::ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.epoch,
            },
            std::any::TypeId::of::<OriginalOpen>(),
        ))
    }
    fn instance_id(&self) -> &spi::ConnectorInstanceId {
        &self.descriptor.instance_id
    }
    fn namespace_exists(
        &self,
        _: spi::ConnectorNamespaceRequest,
    ) -> Result<bool, spi::ConnectorError> {
        Err(unused())
    }
    fn table_exists(&self, _: spi::ConnectorTableRequest) -> Result<bool, spi::ConnectorError> {
        Err(unused())
    }
    fn list_tables(
        &self,
        _: spi::ConnectorListTablesRequest,
    ) -> Result<Vec<spi::ConnectorTableIdentity>, spi::ConnectorError> {
        Err(unused())
    }
    fn load_table(
        &self,
        _: spi::ConnectorTableRequest,
        _: &mut read::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        spi::ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<spi::ConnectorError>,
    > {
        Err(unused().into())
    }
}
struct Sealer(
    Arc<read::adapter::ReadRuntimeAdapter<Provider>>,
    Arc<Provider>,
);
impl read::ConnectorReadAttemptAccessSealer for Sealer {
    fn seal(
        &self,
        _: &read::ConnectorReadTableHandle,
        mint: read::ConnectorReadAttemptAccessMint,
    ) -> Result<read::ConnectorReadAttemptAccessSource, spi::ConnectorError> {
        Ok(mint.seal(Arc::new(Reacquirer(self.0.clone(), self.1.clone()))))
    }
}
struct Reacquirer(
    Arc<read::adapter::ReadRuntimeAdapter<Provider>>,
    Arc<Provider>,
);
impl read::ConnectorReadAttemptAccessReacquirer for Reacquirer {
    fn requires_metadata_split_preparation(&self) -> bool {
        true
    }
    fn for_attempt<'a>(
        &'a self,
        request: &'a spi::ConnectorAttemptContext,
    ) -> futures::future::BoxFuture<
        'a,
        Result<read::ConnectorReadAttemptRuntime, spi::ConnectorError>,
    > {
        Box::pin(async move {
            *self.1.probe.actual_context.lock().unwrap() = Some(request.request().clone());
            Ok(read::ConnectorReadAttemptRuntime::new(self.0.clone()))
        })
    }
}
struct Encoder;
impl spi::ConnectorReadWireEncoder for Encoder {
    fn owner(&self) -> &str {
        "opening-test"
    }
    fn encode_relation_payload(
        &self,
        _: &read::ConnectorReadRelation,
    ) -> Result<spi::ConnectorReadRelationPayload, spi::ConnectorCodecError> {
        panic!("no dispatch before initialization")
    }
    fn encode_column_payload(
        &self,
        _: &read::ConnectorReadColumnHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        panic!("no dispatch")
    }
    fn encode_transaction_payload(
        &self,
        _: &read::ConnectorReadTransactionHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        panic!("no dispatch")
    }
    fn encode_split_payload(
        &self,
        _: &read::ConnectorReadSplit,
    ) -> Result<spi::ConnectorReadSplitPayload, spi::ConnectorCodecError> {
        panic!("cancelled attempt must not dispatch")
    }
}

struct State {
    execution: QueryExecutionId,
    recipe: Option<AttemptSourceRecipe<ProductionSourceRecipe>>,
    sources: OpenRoundSplitSources,
    feedback: Arc<RuntimeFilterFeedbackState>,
}
impl SerialAttemptInitialization for State {
    type Ready = ();
    type Recipe = ProductionSourceRecipe;
    type Opened = OpenedRoundSplitSource;
    type Opening = ProductionSourceOpening;
    fn execution_id(&self) -> QueryExecutionId {
        self.execution
    }
    fn next_source_recipe(
        &mut self,
    ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError> {
        Ok(self.recipe.take())
    }
    fn begin_open(
        recipe: AttemptSourceRecipe<Self::Recipe>,
        supervisor: &ConnectorBlockingIoSupervisor,
    ) -> Self::Opening {
        ProductionInitializationState::begin_open(recipe, supervisor)
    }
    fn poll_open_in(
        opening: &mut Self::Opening,
        cx: &mut Context<'_>,
        outcome: &mut Option<AttemptSourceOpenOutcome<Self::Opened>>,
    ) -> Poll<()> {
        ProductionInitializationState::poll_open_in(opening, cx, outcome)
    }
    fn opening_health(opening: &Self::Opening) -> Option<StatementNativeHealthFailure> {
        ProductionInitializationState::opening_health(opening)
    }
    fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError> {
        self.sources
            .push(opened.into_round_source(self.feedback.clone()));
        Ok(())
    }
    fn finish_in(&mut self, ready: &mut Option<()>) -> Result<(), DistributedQueryError> {
        *ready = Some(());
        Ok(())
    }
    fn close_in(&mut self, job: &mut Option<ConnectorBlockingIoJob<()>>) {
        self.sources.close_in(job);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn internal_production_opening_cancellation_preserves_original_mio_child_and_close_cause() {
    use novarocks_execution_contract::failure::FailureProjection;
    use novarocks_workload_control::ResultWindowClass;
    use std::error::Error;
    use std::num::NonZeroUsize;
    let (mut control, mut driver, capacity) = original_source::fixture_before_ready();
    let mut creator = crate::query_execution::internal_result_cpu::InternalResultCpuOwner::try_new(
        novarocks_query_application::cpu::QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ),
        &mut control,
    )
    .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    original_source::bind(&scope, &mut driver);
    let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut positive = false;
    for _ in 0..256 {
        original_source::drive(&mut driver);
        if io.poll_progress(&root.owner, &mut cx).unwrap() {
            positive = true;
            break;
        }
    }
    assert!(
        positive,
        "the original MIO must actually install its paid Root payload"
    );
    let window = io.root_capacity().unwrap().try_window_alias().unwrap();
    assert_eq!(window.class(), ResultWindowClass::Internal);
    assert!(window.is_root_backed_internal());
    let cohort = original_source::cohort(&scope, 1, &mut driver);
    let (mut physical, supervisor) = supervisor();
    supervisor
        .install_provider_creator_cpu(creator.runtime())
        .unwrap();
    let original_cause = SharedFailureCause::new(spi::ConnectorError::new(
        spi::ConnectorErrorKind::CorruptData,
        "original prepared split close refusal",
    ));
    let close_error = spi::ConnectorError::from_domain(
        spi::ConnectorErrorKind::CorruptData,
        original_cause.clone(),
    );
    let fact = close_error.failure_fact();
    let (release_child, child) = tokio::sync::oneshot::channel();
    let probe = Arc::new(Probe {
        entered: AtomicBool::new(false),
        cpu_calls: AtomicUsize::new(0),
        child_drops: AtomicUsize::new(0),
        raw_received: AtomicBool::new(false),
        close_calls: AtomicUsize::new(0),
        source_drops: AtomicUsize::new(0),
        drop_entered: tokio::sync::Notify::new(),
        released: Mutex::new(false),
        changed: Condvar::new(),
        child: Mutex::new(Some(child)),
        actual_context: Mutex::new(None),
        close_error,
    });
    let _rescue = Rescue(probe.clone());
    let template = novarocks_catalog_application::test_support::test_control_binding_for(
        spi::ConnectorInstanceId::parse("opening-test").unwrap(),
        3,
    );
    let descriptor = template.descriptor().clone();
    let catalog = spi::CatalogHandle::new(
        descriptor.instance_id.clone(),
        spi::CatalogVersion::from_bytes([3; 32]),
    );
    let properties = spi::CatalogProperties::new(
        catalog.clone(),
        descriptor.provider_id.clone(),
        1,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let provider = Arc::new(Provider {
        descriptor: descriptor.clone(),
        epoch: template.incarnation(),
        catalog,
        probe: probe.clone(),
    });
    let binding = Arc::new(
        spi::ConnectorControlBinding::try_new(
            descriptor,
            provider.epoch,
            provider.clone(),
            template.planning().clone(),
            template.execution_distribution().clone(),
            None,
        )
        .unwrap()
        .with_catalog_properties(properties.clone())
        .unwrap(),
    );
    let planning = spi::ConnectorControlPlanningLease::new(binding, || {});
    let adapter = Arc::new(ReadRuntimeAdapter::new(provider.clone()));
    let metadata: Arc<dyn read::ConnectorReadMetadata> = adapter.clone();
    let splits: Arc<dyn read::ConnectorReadSplitManager> = adapter.clone();
    let request = read::ConnectorReadRequestControl::provider_reacquire_attempt_access(
        metadata.clone(),
        splits.clone(),
        Arc::new(Sealer(adapter.clone(), provider.clone())),
    );
    let read_group =
        spi::ConnectorControlReadBinding::new(metadata, splits, None, Arc::new(Encoder));
    let frozen = adapter.wrap_table(Table);
    let access = read_group.seal_attempt_access(&request, &frozen).unwrap();
    assert!(access.requires_metadata_split_preparation());
    let entry =
        crate::query_execution::preparation::ConnectorAttemptAccessEntry::for_initializer_test(
            properties, planning, access,
        );
    let source = RoundSplitSourceRecipe::for_initializer_test(1, 7, entry);
    let execution = execution_id(1);
    let identity = AttemptSourceIdentity {
        execution_id: execution,
        fragment_id: 1,
        plan_node_id: 7,
        generation: source.generation().clone(),
    };
    let cancellation = scope.cancellation().unwrap();
    let root_cancellation = cancellation.clone();
    assert!(root_cancellation.is_for_scope(&scope));
    let connector = connector_context().with_io_admission(io.clone());
    let lifecycle =
        AttemptInitializationLifecycle::governed(connector.deadline(), cancellation.clone());
    let recipe = ProductionSourceRecipe {
        admission: ConnectorBlockingIoResponsibility::request_from_cohort(
            &scope,
            &window,
            cohort.clone(),
        )
        .unwrap(),
        source,
        session: read::ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .unwrap(),
        connector_context: connector,
        metadata_preparation: Some(Arc::downgrade(&io)),
        cancellation,
        runtime: tokio::runtime::Handle::current(),
        blocking_io: supervisor.clone(),
    };
    let state = State {
        execution,
        recipe: Some(AttemptSourceRecipe { identity, recipe }),
        sources: OpenRoundSplitSources::with_capacity(1, supervisor.clone()),
        feedback: Arc::new(RuntimeFilterFeedbackState::new(execution, Default::default()).unwrap()),
    };
    let mut initializing = InitializationProgress::new(supervisor.clone(), lifecycle, state);
    {
        let mut borrowed = Box::pin(initializing.initialize());
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &control,
                &mut driver,
                std::future::poll_fn(|cx| {
                    io.poll_progress(&root.owner, cx).unwrap();
                    let observed = borrowed.as_mut().poll(cx);
                    assert!(
                        observed.is_pending(),
                        "original initializer finished before its held child: {observed:?}"
                    );
                    if probe.entered.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            probe.cpu_calls.load(Ordering::SeqCst),
            1,
            "the actual qualified closed metadata CPU phase ran"
        );
        root.owner
            .cancellation_requester()
            .request(CancellationReason::ClientDisconnected)
            .unwrap();
        assert_eq!(
            root_cancellation.reason(),
            Some(CancellationReason::ClientDisconnected)
        );
        let error = tokio::time::timeout(Duration::from_secs(5), borrowed)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.message().contains("cancelled"));
    }
    assert!(initializing.ready.is_none());
    let opening = initializing.opening.as_ref().unwrap();
    assert!(
        opening.preparation.is_some()
            && opening.open_owner.is_some()
            && opening.open_future.is_some()
    );
    let child_id = opening
        .responsibility
        .as_ref()
        .unwrap()
        .scope()
        .unwrap()
        .id();
    assert_eq!(cohort.remaining(), 0);
    assert_eq!(probe.child_drops.load(Ordering::SeqCst), 0);
    assert!(
        io.poll_actual_quiescent_continuing(&mut cx).is_pending(),
        "cancel is not actual exit of the held admitted request"
    );
    {
        let close = initializing.close();
        tokio::pin!(close);
        std::future::poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(probe.child_drops.load(Ordering::SeqCst), 0);
    assert!(
        control
            .snapshot()
            .scopes
            .iter()
            .any(|value| value.id == child_id)
    );
    release_child.send(()).unwrap();
    {
        let close = initializing.close();
        tokio::pin!(close);
        let entered = probe.drop_entered.notified();
        tokio::pin!(entered);
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &control,
                &mut driver,
                std::future::poll_fn(|cx| {
                    assert!(close.as_mut().poll(cx).is_pending());
                    entered.as_mut().poll(cx)
                }),
            ),
        )
        .await
        .unwrap();
    }
    assert_eq!(probe.child_drops.load(Ordering::SeqCst), 1);
    assert!(probe.raw_received.load(Ordering::SeqCst));
    assert_eq!(probe.source_drops.load(Ordering::SeqCst), 0);
    let job = initializing
        .close_job
        .as_ref()
        .expect("the original source retirement job is stable");
    assert!(!job.actual_exit_observed());
    let raw = job
        .source_close_failure()
        .expect("typed close Err precedes held Source Drop");
    assert_eq!(raw.failure_fact(), fact);
    let carried = raw
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    assert_eq!(
        carried.downcast_ref::<SharedFailureCause>(),
        Some(&original_cause)
    );
    assert!(
        control
            .snapshot()
            .scopes
            .iter()
            .any(|value| value.id == child_id)
    );
    probe.release_drop();
    let health = tokio::time::timeout(
        Duration::from_secs(5),
        with_original_driver(&control, &mut driver, initializing.close()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(probe.source_drops.load(Ordering::SeqCst), 1);
    assert!(
        initializing
            .close_job
            .as_ref()
            .unwrap()
            .actual_exit_observed()
    );
    assert_eq!(
        health
            .cause()
            .downcast_ref::<spi::ConnectorError>()
            .unwrap()
            .failure_fact(),
        fact
    );
    let after_drop = health
        .cause()
        .downcast_ref::<spi::ConnectorError>()
        .unwrap();
    let domain = after_drop
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    assert_eq!(
        domain.downcast_ref::<SharedFailureCause>(),
        Some(&original_cause)
    );
    assert_eq!(probe.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.cpu_calls.load(Ordering::SeqCst), 1);
    assert!(initializing.ready.is_none());
    drop(raw);
    drop((initializing, request, read_group, adapter, provider));
    assert!(
        !control
            .snapshot()
            .scopes
            .iter()
            .any(|value| value.id == child_id),
        "the actual close worker/Drop and final original owner release retire this exact Source child"
    );
    let exited = tokio::time::timeout(
        Duration::from_secs(5),
        with_original_driver(
            &control,
            &mut driver,
            std::future::poll_fn(|cx| io.poll_actual_quiescent_continuing(cx)),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    // Root cancellation is the initializer verdict, not an invented MIO
    // admission refusal. This request was already admitted and actually exited.
    let ordinary = exited.into_ordinary_failure();
    assert!(
        ordinary.is_none(),
        "fully admitted MIO child had no ordinary refusal: {ordinary:?}"
    );
    assert!(io.failure().is_none());
    assert!(io.health_failure().is_none());
    assert!(root_cancellation.is_for_scope(&scope));
    assert_eq!(
        root_cancellation.reason(),
        Some(CancellationReason::ClientDisconnected)
    );
    drop((
        io,
        window,
        cohort,
        capacity,
        scope,
        supervisor,
        root_cancellation,
    ));
    creator
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    physical.shutdown().await;
    original_source::finish(control, root, driver);
}
