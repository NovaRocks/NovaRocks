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

//! Wakeup-driven, complete-reread controller for local catalog projections.
//!
//! # Why there is no change feed here any more
//!
//! This controller used to poll a StateStore change feed. It never used what
//! the feed carried: the poll extracted exactly one boolean from a page — "did
//! any key under the attachment prefix move" — and answered it with a complete
//! authoritative reread, the same reread it performed for a retention gap and
//! for a store-identity change. Keeping a feed to transport one bit cost a
//! retained cursor, per-provider change history, and a full enumeration every
//! 250 ms whether or not anything had changed.
//!
//! Two deliberately unequal triggers replace it:
//!
//! * **A wakeup**, published by this process's own attachment repository when
//!   one of its writes commits. This is the latency path, and it is lossy by
//!   construction — a write on another frontend, or through a second
//!   repository over the same store, produces no wakeup here. Nothing may rest
//!   on receiving one.
//! * **A periodic sweep**, which is the correctness floor: it bounds how long
//!   *any* write can stay unobserved, including every write no wakeup could
//!   reach.
//!
//! Every round is a complete reread, so a missed wakeup costs latency and
//! never consistency. The sweep is timed from the end of the previous scan, so
//! a slow store stretches the cadence instead of queueing rounds behind
//! itself, and one controller never runs two scans at once.
//!
//! # Freshness does not run on the scan's clock
//!
//! Retiring projections that can no longer be confirmed has to happen on time
//! *especially* when a scan is stuck — which is exactly when a timer driven by
//! scan completions never fires. The freshness deadline is therefore armed
//! from the last scan that actually completed under the current control
//! generation, and is awaited *alongside* the in-flight scan rather than after
//! it.
//!
//! When that deadline wins, the round's generation is superseded: local
//! admission is withdrawn and the in-flight scan's result — whenever it
//! arrives — neither republishes nor counts as freshness. A scan that outlived
//! its generation enumerated a store state from before the expiry, so treating
//! its completion as current would reset the very clock that just fired.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_state_store_api::{StateStore, StoreIdentity};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::catalog_application::FrontendCatalogApplicationPort;
use crate::catalog_attachment::CatalogAttachmentWakeupSignal;

#[derive(Default)]
struct CatalogProjectionMetrics {
    successful_rounds: AtomicU64,
    failed_rounds: AtomicU64,
    resyncs: AtomicU64,
    freshness_expiries: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct CatalogProjectionConfig {
    pub page_size: usize,
    /// Floor on how long a write this process did not make can stay
    /// unobserved.
    ///
    /// This is a complete authoritative enumeration, not a cheap liveness
    /// poll, which is why it is seconds rather than the change feed's 250 ms:
    /// at that cadence the enumeration was resident CPU cost for a deployment
    /// where nothing had changed for hours. Local writes do not wait for it —
    /// they arrive through the wakeup.
    pub reconcile_interval: Duration,
    /// How long local admission may keep serving projections that no completed
    /// reconcile has been able to confirm.
    pub freshness_budget: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub worker_count: usize,
    pub shutdown_deadline: Duration,
}

impl Default for CatalogProjectionConfig {
    fn default() -> Self {
        Self {
            page_size: 256,
            reconcile_interval: Duration::from_secs(5),
            freshness_budget: Duration::from_secs(30),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            worker_count: 8,
            shutdown_deadline: Duration::from_secs(5),
        }
    }
}

/// One reconcile round, stamped with the control generation it began under.
struct ScanRound {
    generation: u64,
    identity: Result<StoreIdentity, String>,
    outcome: Result<(), String>,
}

pub struct FrontendCatalogController {
    store: Arc<dyn StateStore>,
    projection: Arc<FrontendCatalogApplicationPort>,
    config: CatalogProjectionConfig,
    stopping: AtomicBool,
    /// Interrupts the worker's waits.
    ///
    /// A flag alone is not enough: it is only read between rounds, so a worker
    /// parked on the sweep would take a whole interval to notice shutdown, and
    /// the sweep is now seconds rather than milliseconds. This is a `watch`
    /// rather than a notification because it holds state — a stop published
    /// before the worker starts waiting is still seen.
    stop: watch::Sender<bool>,
    /// The identity `bootstrap` observed, handed once to the worker.
    bootstrap_identity: Mutex<Option<StoreIdentity>>,
    /// Monotonic control generation. A freshness expiry supersedes it, which
    /// is what makes an abandoned scan's late completion inert rather than
    /// merely unlikely.
    generation: AtomicU64,
    metrics: CatalogProjectionMetrics,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl FrontendCatalogController {
    pub fn new(
        store: Arc<dyn StateStore>,
        projection: Arc<FrontendCatalogApplicationPort>,
        config: CatalogProjectionConfig,
    ) -> Result<Arc<Self>, String> {
        if config.page_size == 0 || config.page_size > store.limits().max_page_size {
            return Err("catalog controller page size is outside StateStore limits".to_string());
        }
        if config.reconcile_interval.is_zero()
            || config.freshness_budget.is_zero()
            || config.retry_initial.is_zero()
            || config.retry_max < config.retry_initial
            || config.worker_count == 0
            || config.shutdown_deadline.is_zero()
        {
            return Err(
                "catalog controller config contains an invalid zero or retry bound".to_string(),
            );
        }
        Ok(Arc::new(Self {
            store,
            projection,
            config,
            stopping: AtomicBool::new(false),
            stop: watch::channel(false).0,
            bootstrap_identity: Mutex::new(None),
            generation: AtomicU64::new(0),
            metrics: CatalogProjectionMetrics::default(),
            worker: Mutex::new(None),
        }))
    }

    /// Performs the first complete authoritative reread.
    ///
    /// There is no watermark to capture. A reconcile reads the attachment
    /// family itself, so the only thing startup has to establish is that one
    /// complete snapshot was applied; failing here fails frontend bootstrap,
    /// which is the point — a frontend that never enumerated desired state
    /// must not start serving a guess at it.
    pub async fn bootstrap(&self) -> Result<(), String> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string())?;
        self.reconcile().await?;
        *self
            .bootstrap_identity
            .lock()
            .map_err(|_| "catalog controller bootstrap state lock is poisoned".to_string())? =
            Some(identity);
        Ok(())
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?;
        if worker.is_some() {
            return Err("catalog controller is already running".to_string());
        }
        self.stopping.store(false, Ordering::Release);
        self.stop.send_replace(false);
        let controller = Arc::clone(self);
        *worker = Some(tokio::spawn(async move {
            controller.run().await;
        }));
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        self.stopping.store(true, Ordering::Release);
        self.stop.send_replace(true);
        let handle = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?
            .take();
        if let Some(mut handle) = handle
            && tokio::time::timeout(self.config.shutdown_deadline, &mut handle)
                .await
                .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        self.projection.unpublish_all();
        self.publish_metrics();
        Ok(())
    }

    pub fn metrics_snapshot(&self) -> crate::catalog_application::CatalogProjectionMetricsSnapshot {
        crate::catalog_application::CatalogProjectionMetricsSnapshot {
            projected_catalogs: self.projection.projection_count(),
            successful_rounds: self.metrics.successful_rounds.load(Ordering::Relaxed),
            failed_rounds: self.metrics.failed_rounds.load(Ordering::Relaxed),
            resyncs: self.metrics.resyncs.load(Ordering::Relaxed),
            freshness_expiries: self.metrics.freshness_expiries.load(Ordering::Relaxed),
        }
    }

    fn publish_metrics(&self) {
        crate::catalog_application::publish_catalog_projection_metrics(self.metrics_snapshot());
    }

    async fn run(&self) {
        let mut known_identity = match self.bootstrap_identity.lock() {
            Ok(mut state) => state.take(),
            Err(_) => {
                tracing::warn!("catalog controller bootstrap state lock is poisoned");
                None
            }
        };
        // A controller that never bootstrapped has published nothing, so its
        // clock starts here and its first round is its bootstrap.
        let mut last_fresh = Instant::now();
        let mut retry = self.config.retry_initial;
        let mut wakeup = self.projection.attachment_wakeup_signal();
        let mut stop = self.stop.subscribe();
        // True once this outage has already withdrawn admission. It also
        // disarms the deadline, so an expiry cannot re-fire every round and
        // supersede every scan forever.
        let mut fail_closed = false;

        while !self.stopping.load(Ordering::Acquire) {
            let expiry = (!fail_closed).then(|| last_fresh + self.config.freshness_budget);
            let round = self.run_round(expiry).await;
            let superseded = round.generation != self.generation.load(Ordering::Acquire);

            match &round.identity {
                Ok(identity) if known_identity.as_ref() != Some(identity) => {
                    // A different store is a different world, and the complete
                    // reread this round already performed is the whole
                    // recovery: nothing is carried over from the old store, so
                    // there is no cursor or watermark left to invalidate.
                    tracing::info!(
                        store_id = %identity.store_id,
                        "catalog controller observed a new StateStore identity"
                    );
                    known_identity = Some(identity.clone());
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "catalog controller could not read store identity");
                }
            }

            match round.outcome {
                Ok(()) if !superseded => {
                    last_fresh = Instant::now();
                    retry = self.config.retry_initial;
                    fail_closed = false;
                    self.metrics
                        .successful_rounds
                        .fetch_add(1, Ordering::Relaxed);
                    self.publish_metrics();
                    self.await_next_round(
                        wakeup.as_mut(),
                        &mut stop,
                        self.config.reconcile_interval,
                    )
                    .await;
                    continue;
                }
                Ok(()) => {
                    tracing::warn!(
                        "catalog attachment reconcile outlived its control generation; \
                         its snapshot predates the freshness expiry and was discarded"
                    );
                    // The expiry that superseded it already withdrew admission.
                    fail_closed = true;
                }
                Err(error) => {
                    tracing::warn!(%error, "catalog attachment reconcile failed");
                    self.metrics.failed_rounds.fetch_add(1, Ordering::Relaxed);
                    if superseded {
                        fail_closed = true;
                    } else if !fail_closed && last_fresh.elapsed() >= self.config.freshness_budget {
                        self.expire_freshness();
                        fail_closed = true;
                    }
                }
            }
            self.publish_metrics();
            // A wakeup during an outage is not worth waking for: the store is
            // what is broken, so the backoff is honoured in full rather than
            // raced against a signal. Shutdown still cuts it short.
            self.sleep_unless_stopping(&mut stop, retry).await;
            retry = retry.saturating_mul(2).min(self.config.retry_max);
        }
    }

    /// Sleeps, unless the controller is asked to stop first.
    ///
    /// The stop flag on its own is read only between rounds, so a worker
    /// parked on a wait would take the whole wait to notice shutdown. That was
    /// invisible at the change feed's 250 ms poll and is not at a multi-second
    /// sweep: it turns every frontend shutdown into a stall of one sweep.
    async fn sleep_unless_stopping(&self, stop: &mut watch::Receiver<bool>, wait: Duration) {
        tokio::select! {
            _ = stop.wait_for(|stopping| *stopping) => {}
            () = tokio::time::sleep(wait) => {}
        }
    }

    /// Waits for the next reason to reconcile.
    ///
    /// A wakeup and the sweep race, and the sweep always exists: a controller
    /// with no wakeup channel simply parks on the interval. Waiting on the
    /// wakeup collapses a burst of DDL into one round rather than one round
    /// per statement — the channel holds a single slot, so several commits
    /// arriving during a scan produce exactly one wakeup afterwards.
    async fn await_next_round(
        &self,
        wakeup: Option<&mut CatalogAttachmentWakeupSignal>,
        stop: &mut watch::Receiver<bool>,
        interval: Duration,
    ) {
        let Some(signal) = wakeup else {
            self.sleep_unless_stopping(stop, interval).await;
            return;
        };
        let woken = tokio::select! {
            _ = stop.wait_for(|stopping| *stopping) => return,
            woken = signal.changed() => woken,
            () = tokio::time::sleep(interval) => true,
        };
        if !woken {
            // A closed channel is a permanent answer, not a wakeup. Parking on
            // the interval is what stops the loop from spinning once the
            // publishing repository is gone.
            self.sleep_unless_stopping(stop, interval).await;
        }
    }

    /// Runs one reconcile round against the freshness deadline.
    ///
    /// The scan is awaited even after the deadline wins. Dropping it would
    /// abandon the port's in-flight submissions mid-round, and it would not be
    /// what makes the result safe anyway: the generation stamp is. Awaiting it
    /// is also what keeps "one scan per controller" true — a second round
    /// never starts beside a first.
    async fn run_round(&self, expiry: Option<Instant>) -> ScanRound {
        let generation = self.generation.load(Ordering::Acquire);
        let mut scan = std::pin::pin!(self.scan_once());
        let Some(expiry) = expiry else {
            let (identity, outcome) = scan.await;
            return ScanRound {
                generation,
                identity,
                outcome,
            };
        };
        let mut expired = false;
        loop {
            tokio::select! {
                biased;
                (identity, outcome) = &mut scan => {
                    return ScanRound { generation, identity, outcome };
                }
                () = tokio::time::sleep_until(expiry), if !expired => {
                    // Independent of the scan by construction: this fires on
                    // the clock the last completed reconcile set, so a scan
                    // that never returns cannot hold expired projections open.
                    self.expire_freshness();
                    expired = true;
                }
            }
        }
    }

    /// One complete authoritative reread, plus the store identity it ran
    /// against.
    ///
    /// The identity read is reported rather than propagated: a reconcile that
    /// succeeded is a reconcile that succeeded, and losing the identity costs
    /// only the log line that says which store it came from.
    async fn scan_once(&self) -> (Result<StoreIdentity, String>, Result<(), String>) {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string());
        let outcome = self.reconcile().await;
        (identity, outcome)
    }

    /// Enumerates the complete desired state and applies it.
    ///
    /// Partial results cannot reach here: the source reports an incomplete
    /// enumeration as a typed failure rather than as a smaller snapshot, so a
    /// half-read store fails the round instead of retiring the catalogs it did
    /// not manage to read.
    async fn reconcile(&self) -> Result<(), String> {
        self.projection
            .reconcile_with_page_size(self.config.page_size, self.config.worker_count)
            .await
            .map_err(|error| error.to_string())?;
        self.metrics.resyncs.fetch_add(1, Ordering::Relaxed);
        self.publish_metrics();
        Ok(())
    }

    /// Withdraws local admission for everything no completed reconcile can
    /// still vouch for, and supersedes the control generation.
    ///
    /// The decision is recorded before it is acted on. Retiring first would
    /// leave a window in which an operator — or a test — sees every projection
    /// gone and no expiry counted, which reads as an unexplained outage rather
    /// than as the deliberate fail-closed it is.
    fn expire_freshness(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.metrics
            .freshness_expiries
            .fetch_add(1, Ordering::Relaxed);
        self.projection.unpublish_all();
        self.publish_metrics();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::time::Instant as StdInstant;

    use crate::catalog_application::desired_state::CatalogDesiredStateSource;
    use crate::catalog_application::{
        CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind,
        CatalogApplicationPort, CatalogCreateCommand, CatalogDropCommand,
        CatalogRuntimeObservation, CatalogRuntimePublisherSink,
    };
    use crate::state_store::testing::{
        StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
        StateStoreLimitOverrides, StateStoreProviderConfig, TEST_STATE_STORE_PROVIDER_ID,
        builtin_state_store_provider_registry,
    };
    use novarocks_spi::connector::{ConnectorControlResolver, ConnectorProviderId};
    use novarocks_state_store_api::{
        AttemptSupervisor, ReadTransaction, StateStore, StateStoreError, StateStoreErrorKind,
        StateStoreLimits, StoreIdentity, WriteAttempt, WriteTransaction,
    };
    use tokio::sync::Semaphore;

    use super::*;
    use crate::catalog_attachment::{CatalogAttachment, CatalogAttachmentRepository};
    use crate::connector::ConnectorControlHost;

    /// Mints a distinct control generation per materialization, like a real
    /// provider role factory: reusing an incarnation would trip the retired-generation guard
    /// on a same-name recreate. It also counts materializations, so a test can
    /// tell "reconciled again" apart from "materialized again".
    #[derive(Default)]
    struct ReadyFactory {
        incarnations: AtomicU8,
        materializations: Arc<AtomicUsize>,
    }

    /// Fails local materialization without touching durable truth, so a test
    /// can observe the durable-first ordering of CREATE.
    struct UnavailableFactory;

    struct RejectingPublisher;

    impl CatalogRuntimePublisherSink for RejectingPublisher {
        fn publish_catalog_runtime(
            &self,
            _observation: CatalogRuntimeObservation,
        ) -> Result<(), CatalogApplicationError> {
            Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "injected query runtime publication failure",
            ))
        }

        fn unpublish_catalog_runtime(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
            _generation: u64,
        ) -> Result<(), CatalogApplicationError> {
            Ok(())
        }
    }

    struct ToggleReadStore {
        inner: Arc<dyn StateStore>,
        reads_available: AtomicBool,
    }

    /// Holds every read transaction open until a permit is released, so a test
    /// can keep one reconcile scan genuinely in flight across a freshness
    /// budget instead of approximating it with a sleep.
    struct StallableReadStore {
        inner: Arc<dyn StateStore>,
        gate: Arc<Semaphore>,
        stalled: AtomicBool,
    }

    struct IdentityChangedStore {
        inner: Arc<dyn StateStore>,
        identity: StoreIdentity,
    }

    #[async_trait::async_trait]
    impl StateStore for ToggleReadStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn attempts(&self) -> &AttemptSupervisor {
            self.inner.attempts()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            if !self.reads_available.load(Ordering::Acquire) {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected catalog admission read failure",
                ));
            }
            self.inner.begin_read().await
        }

        async fn begin_write(
            &self,
            attempt: WriteAttempt,
            purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.inner.begin_write(attempt, purpose).await
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            self.inner.identity().await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for StallableReadStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn attempts(&self) -> &AttemptSupervisor {
            self.inner.attempts()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            if self.stalled.load(Ordering::Acquire) {
                // Parks here for as long as the test holds the permit. The scan
                // is not failing and not cancelled; it simply has not returned.
                let _permit = self.gate.acquire().await.expect("stall gate");
            }
            self.inner.begin_read().await
        }

        async fn begin_write(
            &self,
            attempt: WriteAttempt,
            purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.inner.begin_write(attempt, purpose).await
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            self.inner.identity().await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for IdentityChangedStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn attempts(&self) -> &AttemptSupervisor {
            self.inner.attempts()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            self.inner.begin_read().await
        }

        async fn begin_write(
            &self,
            attempt: WriteAttempt,
            purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.inner.begin_write(attempt, purpose).await
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            Ok(self.identity.clone())
        }
    }

    impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for ReadyFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            ConnectorProviderId::parse("iceberg").expect("static provider ID")
        }

        fn normalize_and_validate(
            &self,
            properties: novarocks_spi::connector::CatalogProperties,
        ) -> Result<
            novarocks_spi::connector::NormalizedCatalogProperties,
            novarocks_spi::connector::ConnectorMaterializationError,
        > {
            novarocks_spi::connector::NormalizedCatalogProperties::try_new(properties).map_err(
                |detail| novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                ),
            )
        }

        fn materialize(
            &self,
            properties: novarocks_spi::connector::NormalizedCatalogProperties,
            _context: novarocks_spi::connector::MaterializationContext,
        ) -> futures::future::BoxFuture<
            'static,
            Result<
                novarocks_spi::connector::ConnectorControlRoleBinding,
                novarocks_spi::connector::ConnectorMaterializationError,
            >,
        > {
            use futures::FutureExt;

            self.materializations.fetch_add(1, Ordering::Relaxed);
            let incarnation = self.incarnations.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                let control = crate::connector::control_host::tests::test_control_binding_for(
                    properties.handle().catalog_name().clone(),
                    incarnation,
                )
                .with_catalog_properties(properties.as_catalog_properties().clone())
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)?;
                novarocks_spi::connector::ConnectorControlRoleBinding::try_new(
                    properties,
                    Arc::new(control),
                    None,
                    None,
                )
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)
            }
            .boxed()
        }
    }

    impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for UnavailableFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            ConnectorProviderId::parse("iceberg").expect("static provider ID")
        }

        fn normalize_and_validate(
            &self,
            properties: novarocks_spi::connector::CatalogProperties,
        ) -> Result<
            novarocks_spi::connector::NormalizedCatalogProperties,
            novarocks_spi::connector::ConnectorMaterializationError,
        > {
            novarocks_spi::connector::NormalizedCatalogProperties::try_new(properties).map_err(
                |detail| novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                ),
            )
        }

        fn materialize(
            &self,
            _properties: novarocks_spi::connector::NormalizedCatalogProperties,
            _context: novarocks_spi::connector::MaterializationContext,
        ) -> futures::future::BoxFuture<
            'static,
            Result<
                novarocks_spi::connector::ConnectorControlRoleBinding,
                novarocks_spi::connector::ConnectorMaterializationError,
            >,
        > {
            use futures::FutureExt;

            async move {
                Err(novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::Unavailable,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    "injected provider materialization failure",
                ))
            }
            .boxed()
        }
    }

    /// A store whose range scan silently omits every attachment, modelling a
    /// scan that read before a concurrent CREATE committed. Targeted `get`
    /// reads are untouched, exactly as a real snapshot boundary behaves.
    struct ScanMissesAttachmentsStore {
        inner: Arc<dyn StateStore>,
        hide_from_scan: AtomicBool,
    }

    struct ScanMissesAttachmentsRead {
        inner: Box<dyn ReadTransaction>,
        hide_from_scan: bool,
    }

    #[async_trait::async_trait]
    impl ReadTransaction for ScanMissesAttachmentsRead {
        async fn get(
            &mut self,
            key: &novarocks_state_store_api::Key,
        ) -> Result<Option<novarocks_state_store_api::StateRecord>, StateStoreError> {
            self.inner.get(key).await
        }

        async fn range(
            &mut self,
            request: &novarocks_state_store_api::RangeRequest,
        ) -> Result<novarocks_state_store_api::RangePage, StateStoreError> {
            let mut page = self.inner.range(request).await?;
            if self.hide_from_scan {
                page.records.clear();
            }
            Ok(page)
        }

        async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
            self.inner.abort().await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for ScanMissesAttachmentsStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn attempts(&self) -> &AttemptSupervisor {
            self.inner.attempts()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            Ok(Box::new(ScanMissesAttachmentsRead {
                inner: self.inner.begin_read().await?,
                hide_from_scan: self.hide_from_scan.load(Ordering::Acquire),
            }))
        }

        async fn begin_write(
            &self,
            attempt: WriteAttempt,
            purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.inner.begin_write(attempt, purpose).await
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            self.inner.identity().await
        }
    }

    /// A reconcile whose scan began before a CREATE committed must not retire
    /// that catalog's projection.
    ///
    /// `create_catalog` commits the attachment and only then installs the
    /// projection, so a projection can exist while the in-flight scan has not
    /// observed its attachment. Treating "absent from the scan" as proof the
    /// attachment is gone made the statement right after
    /// CREATE EXTERNAL CATALOG fail with "unknown catalog" whenever a reconcile
    /// cycle straddled it — a create that reported success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reconcile_scan_that_missed_a_fresh_create_keeps_its_projection() {
        let (_directory, mut host, inner) = open_store().await;
        let scanning = Arc::new(ScanMissesAttachmentsStore {
            inner: Arc::clone(&inner),
            hide_from_scan: AtomicBool::new(false),
        });
        let store = Arc::clone(&scanning) as Arc<dyn StateStore>;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open attachment repository");
        let (_control, port, _materializations) = projection(repository);

        let command = create_command(false);
        let instance_id = command.instance_id.clone();
        port.create_catalog(command).expect("create catalog");
        assert!(
            matches!(port.admit_catalog(&instance_id), CatalogAdmission::Ready(_)),
            "CREATE installs the projection synchronously"
        );

        // From here the scan behaves as though it read before the commit.
        scanning.hide_from_scan.store(true, Ordering::Release);
        port.reconcile_with_page_size(256, 1)
            .await
            .expect("reconcile");

        match port.admit_catalog(&instance_id) {
            CatalogAdmission::Ready(_) => {}
            other => panic!(
                "a scan that did not observe the attachment is not proof it is gone;                  retiring on that alone is what surfaces as `unknown catalog`: {other:?}"
            ),
        }

        // A genuinely dropped attachment is still retired: the targeted reread
        // is what distinguishes the two, so it must not blanket-preserve.
        scanning.hide_from_scan.store(false, Ordering::Release);
        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: false,
        })
        .expect("drop catalog");
        port.reconcile_with_page_size(256, 1)
            .await
            .expect("reconcile after drop");
        assert!(matches!(
            port.admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));

        // The provider cannot close while this test still holds store handles.
        drop(port);
        drop(_control);
        drop(store);
        drop(scanning);
        drop(inner);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("state store shutdown");
    }

    async fn open_store() -> (tempfile::TempDir, StateStoreHost, Arc<dyn StateStore>) {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-controller-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            StdInstant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        assert_eq!(host.provider_id(), TEST_STATE_STORE_PROVIDER_ID);
        let store = host.state_store().expect("ready StateStore");
        (directory, host, store)
    }

    fn attachment() -> CatalogAttachment {
        CatalogAttachment {
            attachment_id: uuid::Uuid::now_v7(),
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("catalog.analytics")
                .expect("instance ID"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            display_name: "catalog.analytics".to_string(),
            durable_properties: Vec::new(),
            credential_bindings: Vec::new(),
            created_at_ms: 1,
        }
    }

    fn projection(
        repository: CatalogAttachmentRepository,
    ) -> (
        Arc<ConnectorControlHost>,
        Arc<FrontendCatalogApplicationPort>,
        Arc<AtomicUsize>,
    ) {
        let materializations = Arc::new(AtomicUsize::new(0));
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(ReadyFactory {
                incarnations: AtomicU8::new(0),
                materializations: Arc::clone(&materializations),
            })])
            .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            CatalogDesiredStateSource::dynamic_state_store(repository),
            Arc::clone(&control),
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
        ));
        (control, port, materializations)
    }

    fn create_command(if_not_exists: bool) -> CatalogCreateCommand {
        CatalogCreateCommand {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("catalog.analytics")
                .expect("instance ID"),
            display_name: "catalog.analytics".to_string(),
            properties: vec![("type".to_string(), "iceberg".to_string())],
            if_not_exists,
        }
    }

    async fn wait_for_ready(
        port: &FrontendCatalogApplicationPort,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> CatalogRuntimeObservation {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let CatalogAdmission::Ready(observation) = port.admit_catalog(instance_id) {
                    return observation;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "catalog `{}` did not become ready after scheduler materialization",
                instance_id.as_str()
            )
        })
    }

    async fn wait_for<F>(deadline: Duration, description: &str, mut ready: F)
    where
        F: FnMut() -> bool,
    {
        tokio::time::timeout(deadline, async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{description}"));
    }

    /// Name uniqueness is arbitrated by the absent-precondition commit, not by a
    /// local lock: two independent frontend hosts racing the same SQL name
    /// produce exactly one durable attachment identity, and both converge on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_create_of_one_catalog_name_yields_a_single_attachment_identity() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let (_first_control, first_port, _first_calls) = projection(repository.clone());
        let (_second_control, second_port, _second_calls) = projection(repository.clone());

        // Plain OS threads, so each CREATE drives the port's blocking StateStore
        // path from outside the runtime and the two commits genuinely race.
        let (first_result, second_result) = std::thread::scope(|scope| {
            let first = scope.spawn(|| first_port.create_catalog(create_command(false)));
            let second = scope.spawn(|| second_port.create_catalog(create_command(false)));
            (
                first.join().expect("first CREATE thread"),
                second.join().expect("second CREATE thread"),
            )
        });

        let attachments = repository
            .list_with_page_size(256)
            .await
            .expect("list attachments");
        assert_eq!(
            attachments.len(),
            1,
            "one SQL name must own exactly one durable attachment"
        );
        let winner = attachments[0].attachment.attachment_id;
        let (winner_result, loser_result) = match (&first_result, &second_result) {
            (Ok(_), Err(_)) => (&first_result, &second_result),
            (Err(_), Ok(_)) => (&second_result, &first_result),
            other => panic!("exactly one concurrent CREATE must win: {other:?}"),
        };
        assert_eq!(
            winner_result
                .as_ref()
                .expect("winning CREATE observation")
                .attachment_id,
            winner
        );
        assert_eq!(
            loser_result
                .as_ref()
                .expect_err("the losing CREATE reports the existing attachment")
                .kind(),
            CatalogApplicationErrorKind::AlreadyExists
        );

        // The loser converges on the winner's identity through an authoritative
        // reread, and each host owns its own live control generation.
        first_port
            .reconcile_with_page_size(256, 1)
            .await
            .expect("first host reconcile");
        second_port
            .reconcile_with_page_size(256, 1)
            .await
            .expect("second host reconcile");
        let instance_id = create_command(false).instance_id;
        for port in [&first_port, &second_port] {
            assert_eq!(
                wait_for_ready(port, &instance_id).await.attachment_id,
                winner,
                "both hosts must admit the surviving attachment"
            );
        }
        assert!(
            _first_control.observe_current_binding(&instance_id).is_ok()
                && _second_control
                    .observe_current_binding(&instance_id)
                    .is_ok(),
            "each host materializes its own local control generation"
        );

        drop(first_port);
        drop(second_port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// CREATE validates the pure definition before commit, but a provider
    /// materialization failure leaves its durable desired state for recovery
    /// after a process-local dependency becomes available again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn materialization_failure_keeps_durable_attachment() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(UnavailableFactory)])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
            control,
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
        ));

        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("provider materialization failure must reject CREATE")
                .kind(),
            CatalogApplicationErrorKind::Unavailable
        );
        assert_eq!(
            repository
                .list_with_page_size(256)
                .await
                .expect("list attachments")
                .len(),
            1,
            "a post-CAS materialization failure must preserve durable desired state"
        );

        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// Durable commit is the linearization point. Once it succeeds, a local
    /// publication failure is reported as an unavailable runtime and must not
    /// roll back the cluster-wide fact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_keeps_the_committed_attachment_when_local_publication_fails() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(ReadyFactory::default())])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
            Arc::clone(&control),
            Arc::new(RejectingPublisher),
            tokio::runtime::Handle::current(),
        ));

        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("a failed local publication is reported to the client")
                .kind(),
            CatalogApplicationErrorKind::Unavailable
        );
        let attachments = repository
            .list_with_page_size(256)
            .await
            .expect("list attachments");
        assert_eq!(
            attachments.len(),
            1,
            "the committed attachment survives a local publication failure"
        );
        let instance_id = create_command(false).instance_id;
        assert!(
            matches!(
                port.admit_catalog(&instance_id),
                CatalogAdmission::Unavailable { .. }
            ),
            "the durable attachment exists but this host cannot serve it"
        );
        assert!(
            control.observe_current_binding(&instance_id).is_err(),
            "an unpublished generation must not stay locally live"
        );

        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// User-visible existence semantics, and the identity rule that stops a
    /// same-name recreate from resurrecting the dropped attachment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existence_semantics_and_recreate_mint_a_fresh_attachment_identity() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let (control, port, _materializations) = projection(repository.clone());
        let instance_id = create_command(false).instance_id;

        assert_eq!(
            port.drop_catalog(CatalogDropCommand {
                instance_id: instance_id.clone(),
                if_exists: false,
            })
            .expect_err("dropping an absent catalog is an error")
            .kind(),
            CatalogApplicationErrorKind::NotFound
        );
        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: true,
        })
        .expect("DROP IF EXISTS on an absent catalog is a no-op");

        let first = port
            .create_catalog(create_command(false))
            .expect("first CREATE");
        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("a duplicate CREATE is rejected")
                .kind(),
            CatalogApplicationErrorKind::AlreadyExists
        );
        // IF NOT EXISTS returns the current runtime instead of minting a second
        // lifecycle identity for the same SQL name.
        assert_eq!(
            port.create_catalog(create_command(true))
                .expect("CREATE IF NOT EXISTS resolves the existing attachment")
                .attachment_id,
            first.attachment_id
        );

        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: false,
        })
        .expect("DROP removes the durable attachment");
        assert!(matches!(
            port.admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));
        assert!(
            control.observe_current_binding(&instance_id).is_err(),
            "DROP stops local admission before the generation is gone"
        );

        let recreated = port
            .create_catalog(create_command(false))
            .expect("recreate the same SQL name");
        assert_ne!(
            recreated.attachment_id, first.attachment_id,
            "a recreated catalog must not reuse the dropped lifecycle identity"
        );

        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// The sweep is a complete enumeration, not a liveness ping, so its default
    /// cadence is a deliberate cost decision rather than a leftover: at the
    /// change feed's 250 ms it was resident CPU for a deployment where nothing
    /// changed. Local writes do not pay that latency; they arrive by wakeup.
    #[test]
    fn defaults_state_the_sweep_and_freshness_contract() {
        let config = CatalogProjectionConfig::default();
        assert_eq!(config.page_size, 256);
        assert_eq!(config.reconcile_interval, Duration::from_secs(5));
        assert_eq!(config.freshness_budget, Duration::from_secs(30));
        assert_eq!(config.retry_initial, Duration::from_millis(100));
        assert_eq!(config.retry_max, Duration::from_secs(5));
        assert_eq!(config.worker_count, 8);
        assert_eq!(config.shutdown_deadline, Duration::from_secs(5));
        assert!(
            config.reconcile_interval < config.freshness_budget,
            "a sweep that is slower than the freshness budget would expire \
             projections it was still on course to confirm"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_failure_keeps_durable_attachment_unavailable_and_retires_control() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(ReadyFactory::default())])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
            Arc::clone(&control),
            Arc::new(RejectingPublisher),
            tokio::runtime::Handle::current(),
        ));
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");

        controller
            .bootstrap()
            .await
            .expect("one provider failure does not fail the full bootstrap");
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Unavailable { .. }
        ));
        assert_eq!(port.projection_count(), 0);
        wait_for(
            Duration::from_secs(2),
            "failed publication must retire its registered control generation",
            || {
                control
                    .observe_current_binding(&created.attachment.instance_id)
                    .is_err()
            },
        )
        .await;

        drop(controller);
        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_admission_uses_only_the_local_projection_when_store_reads_fail() {
        let (_directory, mut host, store) = open_store().await;
        let durable_repository =
            CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
                .await
                .expect("open durable repository");
        let created = durable_repository
            .create(attachment())
            .await
            .expect("create attachment");
        let toggle_store = Arc::new(ToggleReadStore {
            inner: Arc::clone(&store),
            reads_available: AtomicBool::new(true),
        });
        let repository = CatalogAttachmentRepository::open(toggle_store.clone(), host.run_policy())
            .await
            .expect("open toggle repository");
        let (_control, port, _materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            toggle_store.clone(),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        toggle_store.reads_available.store(false, Ordering::Release);
        assert!(
            repository
                .get(&created.attachment.instance_id)
                .await
                .is_err(),
            "the injected StateStore read failure must be active"
        );
        wait_for_ready(&port, &created.attachment.instance_id).await;

        drop(controller);
        drop(port);
        drop(repository);
        drop(durable_repository);
        drop(toggle_store);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// A write no wakeup can reach must still converge, on the sweep.
    ///
    /// This replaces the old "converge after a change-feed gap" case, and it is
    /// the same property with the mechanism the gap stood in for made explicit:
    /// the removal here happens through a *second* repository instance, so
    /// neither controller's wakeup channel ever hears about it. That is exactly
    /// what a write on another frontend looks like from here, and it is why the
    /// sweep is the correctness floor rather than a redundancy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_removal_no_wakeup_can_reach_still_converges_within_one_sweep() {
        let (_directory, mut host, store) = open_store().await;
        let writer = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open writing repository");
        let created = writer
            .create(attachment())
            .await
            .expect("create attachment");

        // Each controller reads through its own repository instance, so a write
        // made through `writer` reaches none of their wakeup channels.
        let config = CatalogProjectionConfig {
            reconcile_interval: Duration::from_millis(20),
            ..CatalogProjectionConfig::default()
        };
        let mut controllers = Vec::new();
        let mut ports = Vec::new();
        let mut controls = Vec::new();
        for _ in 0..2 {
            let repository =
                CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
                    .await
                    .expect("open reading repository");
            let (control, port, _materializations) = projection(repository);
            let controller = FrontendCatalogController::new(
                Arc::clone(&store),
                Arc::clone(&port),
                config.clone(),
            )
            .expect("controller");
            controller.bootstrap().await.expect("bootstrap");
            controller.start().expect("start controller");
            assert!(matches!(
                port.admit_catalog(&created.attachment.instance_id),
                CatalogAdmission::Ready(_)
            ));
            controllers.push(controller);
            ports.push(port);
            controls.push(control);
        }

        assert_eq!(
            writer.published_wakeups(),
            1,
            "the writing repository is the only one that published anything"
        );
        writer
            .drop_exact(created.clone())
            .await
            .expect("remove durable attachment");

        for (index, port) in ports.iter().enumerate() {
            let port = Arc::clone(port);
            let instance_id = created.attachment.instance_id.clone();
            wait_for(
                // Several sweeps of headroom: the assertion is that convergence
                // happens on the sweep at all, not that it lands on the first.
                Duration::from_secs(2),
                &format!(
                    "controller {index} must converge on the sweep even though no \
                     wakeup could reach it"
                ),
                move || matches!(port.admit_catalog(&instance_id), CatalogAdmission::Absent),
            )
            .await;
        }

        for controller in &controllers {
            controller.shutdown().await.expect("shutdown controller");
        }
        drop(controllers);
        drop(ports);
        drop(controls);
        drop(writer);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// A sweep that finds nothing new must not re-materialize anything.
    ///
    /// Without the installed-projection short circuit in `materialize_entry`,
    /// every sweep would resubmit every catalog to the provider — which at the
    /// old 250 ms cadence was invisible against the enumeration cost and at any
    /// cadence is a provider call per catalog per round, for no change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sweep_with_no_change_does_not_materialize_again() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let (_control, port, materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig {
                reconcile_interval: Duration::from_millis(10),
                ..CatalogProjectionConfig::default()
            },
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        wait_for_ready(&port, &created.attachment.instance_id).await;
        assert_eq!(materializations.load(Ordering::Relaxed), 1);

        controller.start().expect("start controller");
        let before = controller.metrics_snapshot().resyncs;
        wait_for(
            Duration::from_secs(2),
            "the sweep must keep running with nothing to do",
            || controller.metrics_snapshot().resyncs >= before + 5,
        )
        .await;
        assert_eq!(
            materializations.load(Ordering::Relaxed),
            1,
            "an unchanged catalog must be materialized once, not once per sweep"
        );
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));

        controller.shutdown().await.expect("shutdown controller");
        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// Freshness must expire on its own clock, not on the scan's.
    ///
    /// The store here neither fails nor returns: one reconcile scan is held
    /// inside `begin_read` for longer than the whole freshness budget. A
    /// controller whose expiry ran after the scan — or that only checked
    /// freshness when a round *failed* — would keep serving those projections
    /// indefinitely, because no round ever ends. Local admission must be
    /// withdrawn on time anyway.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stalled_scan_does_not_hold_expired_projections_open() {
        let (_directory, mut host, inner) = open_store().await;
        let gate = Arc::new(Semaphore::new(0));
        let stalling = Arc::new(StallableReadStore {
            inner: Arc::clone(&inner),
            gate: Arc::clone(&gate),
            stalled: AtomicBool::new(false),
        });
        let store = Arc::clone(&stalling) as Arc<dyn StateStore>;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let (_control, port, _materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig {
                reconcile_interval: Duration::from_millis(10),
                freshness_budget: Duration::from_millis(80),
                ..CatalogProjectionConfig::default()
            },
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        wait_for_ready(&port, &created.attachment.instance_id).await;

        stalling.stalled.store(true, Ordering::Release);
        controller.start().expect("start controller");
        // The counter is the primary signal because the expiry records its
        // decision before acting on it; waiting on admission instead would
        // race the retirement it triggers.
        wait_for(
            Duration::from_secs(2),
            "a scan that never returns must not keep unconfirmable projections admitted",
            || controller.metrics_snapshot().freshness_expiries >= 1,
        )
        .await;
        assert!(
            matches!(
                port.admit_catalog(&created.attachment.instance_id),
                CatalogAdmission::Unavailable { .. } | CatalogAdmission::Absent
            ),
            "an expiry that counted itself must actually withdraw admission"
        );
        let metrics = controller.metrics_snapshot();
        assert_eq!(metrics.projected_catalogs, 0);
        assert_eq!(
            metrics.failed_rounds, 0,
            "the store never failed; a stall is not an error, and reporting it \
             as one would hide that the scan is still running: {metrics:?}"
        );

        // Release the stall so the abandoned scan can finish and the worker can
        // observe the shutdown flag rather than being aborted mid-read.
        stalling.stalled.store(false, Ordering::Release);
        gate.add_permits(64);
        controller.shutdown().await.expect("shutdown controller");

        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        drop(stalling);
        drop(inner);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// Freshness expiry is about persistent unreadability, not about a single
    /// unlucky read.
    ///
    /// There is no change feed to fail any more, so the only thing that can
    /// keep a controller from confirming desired state is the authoritative
    /// read itself. Once it fails for longer than the budget, everything this
    /// host was serving has to stop being admitted — the durable attachments
    /// are untouched, so a later successful round republishes them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_persistently_unreadable_store_expires_freshness_before_retrying() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let toggle_store = Arc::new(ToggleReadStore {
            inner: Arc::clone(&store),
            reads_available: AtomicBool::new(true),
        });
        // The reconcile reads through the port's own source, so the outage has
        // to be injected there: a controller whose store handle failed while
        // its source still read cleanly would not be an outage at all.
        let projected = CatalogAttachmentRepository::open(
            Arc::clone(&toggle_store) as Arc<dyn StateStore>,
            host.run_policy(),
        )
        .await
        .expect("open the projecting repository");
        let (_control, port, _materializations) = projection(projected);
        let controller = FrontendCatalogController::new(
            Arc::clone(&toggle_store) as Arc<dyn StateStore>,
            Arc::clone(&port),
            CatalogProjectionConfig {
                reconcile_interval: Duration::from_millis(1),
                freshness_budget: Duration::from_millis(10),
                retry_initial: Duration::from_millis(1),
                retry_max: Duration::from_millis(2),
                ..CatalogProjectionConfig::default()
            },
        )
        .expect("outage controller");
        controller.bootstrap().await.expect("bootstrap projection");
        wait_for_ready(&port, &created.attachment.instance_id).await;

        toggle_store.reads_available.store(false, Ordering::Release);
        controller.start().expect("start outage controller");
        wait_for(
            Duration::from_secs(2),
            "a store that cannot be read for longer than the budget must stop being served",
            || controller.metrics_snapshot().freshness_expiries >= 1,
        )
        .await;
        assert!(
            matches!(
                port.admit_catalog(&created.attachment.instance_id),
                CatalogAdmission::Unavailable { .. } | CatalogAdmission::Absent
            ),
            "an expiry that counted itself must actually withdraw admission"
        );
        let metrics = controller.metrics_snapshot();
        assert_eq!(metrics.projected_catalogs, 0);
        assert!(metrics.failed_rounds > 0);
        assert_eq!(
            metrics.freshness_expiries, 1,
            "one outage withdraws admission once, not once per failed round: {metrics:?}"
        );

        // Recovery needs nothing but a readable store: the durable attachment
        // was never touched.
        toggle_store.reads_available.store(true, Ordering::Release);
        wait_for_ready(&port, &created.attachment.instance_id).await;
        assert_eq!(
            controller.metrics_snapshot().freshness_expiries,
            1,
            "recovery must not be recorded as another expiry"
        );

        controller.shutdown().await.expect("shutdown controller");
        drop(controller);
        drop(port);
        drop(repository);
        drop(toggle_store);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// A different store identity is a different world, and the controller
    /// answers it the only way it can: with a complete authoritative reread.
    ///
    /// There is no cursor left to discard — that was the old change feed's
    /// answer, and it was the reason identity had to be tracked at all. What
    /// has to survive the removal is the behaviour: after the identity changes,
    /// the very next round reads the whole attachment family again and the
    /// projection matches what that store actually holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_changed_store_identity_is_answered_by_a_complete_reread() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let (_control, port, _materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        wait_for_ready(&port, &created.attachment.instance_id).await;

        // The attachment goes away, and the store reports a new identity. The
        // controller has no state that could tell it what changed.
        repository
            .drop_exact(created.clone())
            .await
            .expect("remove durable attachment");
        let original_identity = store.identity().await.expect("original identity");
        let changed_identity = StoreIdentity {
            store_id: uuid::Uuid::now_v7(),
            cluster_id: original_identity.cluster_id.clone(),
        };
        assert_ne!(changed_identity.store_id, original_identity.store_id);
        let changed_store: Arc<dyn StateStore> = Arc::new(IdentityChangedStore {
            inner: Arc::clone(&store),
            identity: changed_identity.clone(),
        });
        let changed_controller = FrontendCatalogController::new(
            changed_store,
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("changed-identity controller");

        let round = changed_controller.run_round(None).await;
        assert_eq!(
            round.identity.expect("identity is observed"),
            changed_identity,
            "the round must report the identity it actually ran against"
        );
        round.outcome.expect("the round completes");
        assert_eq!(
            changed_controller.metrics_snapshot().resyncs,
            1,
            "the round is a complete authoritative reread, not a diff"
        );
        assert!(
            matches!(
                port.admit_catalog(&created.attachment.instance_id),
                CatalogAdmission::Absent
            ),
            "the projection must match what the new store actually holds"
        );

        drop(changed_controller);
        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// Shutdown must not cost a sweep.
    ///
    /// The worker parks between rounds, and that park is now seconds rather
    /// than the change feed's 250 ms. A stop flag read only *between* rounds
    /// would therefore make every frontend shutdown wait out a whole sweep —
    /// which is exactly how this surfaced: as a frontend cleanup deadline
    /// elapsing on the catalog controller, not as anything about catalogs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_interrupts_the_sweep_instead_of_waiting_it_out() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let (_control, port, _materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig {
                // Far longer than any deadline a caller would allow, so a
                // shutdown that waits for the park cannot pass this.
                reconcile_interval: Duration::from_secs(3600),
                shutdown_deadline: Duration::from_secs(30),
                ..CatalogProjectionConfig::default()
            },
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        controller.start().expect("start controller");
        wait_for(
            Duration::from_secs(2),
            "the worker reaches its park between rounds",
            || controller.metrics_snapshot().resyncs >= 2,
        )
        .await;

        let started = StdInstant::now();
        controller.shutdown().await.expect("shutdown controller");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown must interrupt the park, not wait it out: took {elapsed:?}"
        );

        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// A wakeup is a hint, and a lost one costs latency rather than truth.
    /// This pins the low-latency half: a write made through the repository the
    /// port itself writes through wakes the controller well inside a sweep.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_local_write_wakes_the_controller_without_waiting_for_the_sweep() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let (_control, port, _materializations) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig {
                // Long enough that a sweep cannot explain the convergence
                // below; only the wakeup can.
                reconcile_interval: Duration::from_secs(3600),
                ..CatalogProjectionConfig::default()
            },
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        controller.start().expect("start controller");
        // The worker always runs one round of its own before it parks, so the
        // baseline is taken after that round; otherwise this would pass on the
        // worker's own startup round rather than on the wakeup.
        wait_for(
            Duration::from_secs(2),
            "the worker performs its own first round before parking",
            || controller.metrics_snapshot().resyncs >= 2,
        )
        .await;
        let parked = controller.metrics_snapshot().resyncs;

        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        assert_eq!(repository.published_wakeups(), 1);
        wait_for(
            Duration::from_secs(2),
            "a committed local write must wake the reconciler, not wait for the sweep",
            || controller.metrics_snapshot().resyncs > parked,
        )
        .await;
        wait_for_ready(&port, &created.attachment.instance_id).await;

        controller.shutdown().await.expect("shutdown controller");
        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(StdInstant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }
}
