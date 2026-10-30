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

//! Frontend-local catalog runtime projection and query bindings.
//!
//! The Catalog application owns catalog commands, desired-state truth, and
//! admission contracts. Frontend only projects an admitted runtime into its
//! local query catalog.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use novarocks_catalog_application::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogReferenceReader, CatalogRuntimeObservation,
    CatalogRuntimePublisherSink,
};
use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_state_store_api::StateStore;
#[cfg(test)]
use uuid::Uuid;

#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub mod admission_completion;

pub mod command;
pub mod create_table_ddl;
pub mod iceberg_ref_command;
pub mod information_schema;
pub mod model;
pub mod query_bindings;
pub mod query_catalog;
pub mod query_materializer;
pub mod resolver;
pub mod statement;
pub(crate) mod statement_action;
pub mod system_catalog_facts;

/// Frontend's narrow best-effort observation of its MV accelerator state.
pub struct MvCatalogReferenceReader;

impl CatalogReferenceReader for MvCatalogReferenceReader {
    fn observe_references_in<'a>(
        &'a self,
        store: &'a dyn StateStore,
        instance_id: &'a ConnectorInstanceId,
        page_size: usize,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
    ) -> BoxFuture<
        'a,
        Result<Option<&'static str>, novarocks_state_store_api::StateStoreExecutionFailure>,
    > {
        Box::pin(async move {
            novarocks_mv_application::state_store_repository::observe_catalog_references_in(
                store,
                instance_id.as_str(),
                page_size,
                execution,
            )
            .await
            .map(|reference| reference.map(|reference| reference.describe()))
            .map_err(novarocks_failure_contract::SharedFailureCause::new)
        })
    }

    fn observe_references<'a>(
        &'a self,
        store: &'a dyn StateStore,
        instance_id: &'a ConnectorInstanceId,
        page_size: usize,
    ) -> BoxFuture<'a, Result<Option<&'static str>, String>> {
        Box::pin(async move {
            novarocks_mv_application::state_store_repository::observe_catalog_references(
                store,
                instance_id.as_str(),
                page_size,
            )
            .await
            .map(|reference| reference.map(|reference| reference.describe()))
            .map_err(|error| error.to_string())
        })
    }
}

/// The query catalog registry this projection publishes admitted runtimes into.
///
/// Frontend opens its catalog controller before the engine exists, so the
/// registry arrives later and any observation published in the meantime is
/// replayed under the same lock that guards the publication set.
struct QueryCatalogBinding {
    service: Arc<crate::catalog_application::query_catalog::QueryCatalogService>,
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
}

impl QueryCatalogBinding {
    fn register(&self, observation: &CatalogRuntimeObservation) {
        self.service.register_catalog(
            crate::catalog_application::query_catalog::build_connector_catalog(
                observation.instance_id.as_str(),
                Arc::clone(&self.controls),
            ),
        );
    }

    fn unregister(&self, instance_id: &ConnectorInstanceId) {
        self.service.unregister_catalog(instance_id.as_str());
    }
}

/// A consumer of the moment a catalog runtime becomes admitted.
///
/// Notification is best-effort and deliberately returns nothing: admission has
/// already happened, and an observer that failed must not be able to unadmit
/// the catalog or fail the controller's convergence.
pub trait CatalogAdmissionObserver: Send + Sync {
    fn catalog_admitted(&self, instance_id: &ConnectorInstanceId);

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    fn catalog_admitted_observed(
        &self,
        observation: &CatalogRuntimeObservation,
        _sweep: Option<admission_completion::Sweep>,
    ) {
        self.catalog_admitted(&observation.instance_id);
    }
}

/// Frontend-owned exact runtime publication set.
///
/// Frontend publishes only after a local Connector control generation is
/// installed and unpublishes before retiring it. Core wraps the Frontend
/// application port with this set so a stale or partially installed local
/// projection can never be admitted into query materialization.
// Design: ADR-0115 (docs/adr/ADR-0115-catalog-desired-state-source-modes.md)
pub struct CatalogRuntimeProjection {
    published: Mutex<BTreeMap<ConnectorInstanceId, CatalogRuntimeObservation>>,
    query_catalog: Mutex<Option<QueryCatalogBinding>>,
    admission_observers: Mutex<Vec<std::sync::Weak<dyn CatalogAdmissionObserver>>>,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    admission_completion: Mutex<Option<Arc<admission_completion::AdmissionCompletion>>>,
}

impl CatalogRuntimeProjection {
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub fn bind_hms_admission_selector(
        &self,
        selector: admission_completion::OwnerSelector,
    ) -> Result<(), &'static str> {
        let mut installed = self
            .admission_completion
            .lock()
            .map_err(|_| "HMS admission slot lock failed")?;
        if installed.is_some() {
            return Err("HMS admission selector already bound");
        }
        let owner = admission_completion::AdmissionCompletion::new();
        owner.bind_selector(selector)?;
        *installed = Some(owner);
        Ok(())
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub fn hms_admission_completion(
        &self,
    ) -> Option<Arc<admission_completion::AdmissionCompletion>> {
        self.admission_completion.lock().ok()?.clone()
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn close_hms_admission_observation(&self) {
        if let Ok(mut slot) = self.admission_completion.lock() {
            if let Some(owner) = slot.take() {
                owner.close();
            }
        }
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            published: Mutex::new(BTreeMap::new()),
            query_catalog: Mutex::new(None),
            admission_observers: Mutex::new(Vec::new()),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            admission_completion: Mutex::new(None),
        })
    }

    /// Register a consumer of catalog admission.
    ///
    /// A catalog is admitted whenever the controller converges it, which is
    /// not only at startup: this fixture's catalogs are created by SQL long
    /// after the process opened. A consumer that swept once at startup would
    /// therefore sweep an empty set and never look again, so admission is
    /// delivered as it happens instead.
    ///
    /// The registration is weak, and that is a lifetime statement rather than
    /// an optimisation: this publication set belongs to the host and outlives
    /// the role graph, so an observer it owned would keep that graph -- and
    /// every provider handle inside it -- alive past shutdown. The registrant
    /// keeps its own reference for exactly as long as it wants to be called.
    pub fn bind_admission_observer(
        &self,
        observer: &Arc<dyn CatalogAdmissionObserver>,
    ) -> Result<(), CatalogApplicationError> {
        self.admission_observers
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog runtime admission observer lock is poisoned",
                )
            })?
            .push(Arc::downgrade(observer));
        Ok(())
    }

    /// Notify observers outside every publication lock. The work an observer
    /// does is provider I/O, and holding the publication set across it would
    /// stall every other catalog in the process.
    fn notify_admitted(&self, instance_id: &ConnectorInstanceId) {
        let observers = match self.admission_observers.lock() {
            Ok(mut observers) => {
                observers.retain(|observer| observer.strong_count() > 0);
                observers.clone()
            }
            Err(_) => return,
        };
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        let observed = self
            .published
            .lock()
            .ok()
            .and_then(|published| published.get(instance_id).cloned());
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        let sweep = observed.as_ref().and_then(|observation| {
            self.hms_admission_completion()
                .and_then(|owner| owner.queue(observation))
        });
        for observer in observers.iter().filter_map(std::sync::Weak::upgrade) {
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            if let Some(observation) = observed.as_ref() {
                observer.catalog_admitted_observed(observation, sweep.clone());
                continue;
            }
            observer.catalog_admitted(instance_id);
        }
    }

    /// Binds the engine's query catalog registry and replays every runtime the
    /// Frontend controller already published. Engine open calls this once; a
    /// second bind is rejected so two engines cannot share one publication set.
    pub fn bind_query_catalog(
        &self,
        service: Arc<crate::catalog_application::query_catalog::QueryCatalogService>,
        controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
    ) -> Result<(), CatalogApplicationError> {
        let published = self.published.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog runtime publication lock is poisoned",
            )
        })?;
        let mut binding = self.query_catalog.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog runtime query catalog lock is poisoned",
            )
        })?;
        if binding.is_some() {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Conflict,
                "catalog runtime projection is already bound to a query catalog",
            ));
        }
        let bound = QueryCatalogBinding { service, controls };
        for observation in published.values() {
            bound.register(observation);
        }
        *binding = Some(bound);
        Ok(())
    }

    pub fn publisher(self: &Arc<Self>) -> Arc<dyn CatalogRuntimePublisherSink> {
        Arc::clone(self) as Arc<dyn CatalogRuntimePublisherSink>
    }

    pub fn bind_application(
        self: &Arc<Self>,
        application: Arc<dyn CatalogApplicationPort>,
    ) -> Arc<dyn CatalogApplicationPort> {
        Arc::new(PublishedCatalogApplicationPort {
            application,
            projection: Arc::clone(self),
        })
    }

    /// Every catalog runtime this process currently admits.
    ///
    /// Startup rediscovery consumes this instead of a durable scan: the
    /// attachment record lives in StateStore and only the Frontend controller
    /// may read it.
    pub(crate) fn published_observations(
        &self,
    ) -> Result<Vec<CatalogRuntimeObservation>, CatalogApplicationError> {
        let published = self.published.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog runtime publication lock is poisoned",
            )
        })?;
        Ok(published.values().cloned().collect())
    }

    fn require_exact(
        &self,
        observation: &CatalogRuntimeObservation,
    ) -> Result<(), CatalogApplicationError> {
        let published = self.published.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "catalog runtime publication lock is poisoned",
            )
        })?;
        match published.get(&observation.instance_id) {
            Some(current) if current == observation => Ok(()),
            Some(_) => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "catalog runtime publication does not match the admitted attachment generation",
            )),
            None => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "catalog runtime is not published into Frontend",
            )),
        }
    }
}

impl CatalogRuntimePublisherSink for CatalogRuntimeProjection {
    fn catalog_runtime_admitted(&self, instance_id: &ConnectorInstanceId) {
        self.notify_admitted(instance_id);
    }

    fn publish_catalog_runtime(
        &self,
        observation: CatalogRuntimeObservation,
    ) -> Result<(), CatalogApplicationError> {
        let mut published = self.published.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog runtime publication lock is poisoned",
            )
        })?;
        match published.get(&observation.instance_id) {
            Some(current) if current == &observation => Ok(()),
            Some(_) => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Conflict,
                "catalog runtime must be unpublished before publishing another generation",
            )),
            None => {
                // The SQL name only becomes resolvable after the local control
                // generation is registered, so query admission can never see a
                // catalog without a binding.
                if let Some(binding) = self
                    .query_catalog
                    .lock()
                    .map_err(|_| {
                        CatalogApplicationError::new(
                            CatalogApplicationErrorKind::Internal,
                            "catalog runtime query catalog lock is poisoned",
                        )
                    })?
                    .as_ref()
                {
                    binding.register(&observation);
                }
                published.insert(observation.instance_id.clone(), observation);
                Ok(())
            }
        }
    }

    fn unpublish_catalog_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
        generation: u64,
    ) -> Result<(), CatalogApplicationError> {
        let mut published = self.published.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog runtime publication lock is poisoned",
            )
        })?;
        if published
            .get(instance_id)
            .is_some_and(|current| current.generation == generation)
        {
            published.remove(instance_id);
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            if let Some(owner) = self.hms_admission_completion() {
                owner.retired(instance_id, generation);
            }
            // Revoking the SQL name before the caller retires its local
            // generation is what stops new admission for a dropped catalog.
            if let Some(binding) = self
                .query_catalog
                .lock()
                .map_err(|_| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        "catalog runtime query catalog lock is poisoned",
                    )
                })?
                .as_ref()
            {
                binding.unregister(instance_id);
            }
        }
        Ok(())
    }
}

struct PublishedCatalogApplicationPort {
    application: Arc<dyn CatalogApplicationPort>,
    projection: Arc<CatalogRuntimeProjection>,
}

impl CatalogApplicationPort for PublishedCatalogApplicationPort {
    fn require_catalog_mutation_admission(&self) -> Result<(), CatalogApplicationError> {
        self.application.require_catalog_mutation_admission()
    }

    fn create_catalog_in<'a>(
        &'a self,
        command: CatalogCreateCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a novarocks_catalog_application::CatalogMutationReceipt,
    ) -> BoxFuture<'a, Result<CatalogRuntimeObservation, CatalogApplicationError>> {
        Box::pin(async move {
            let observation = self
                .application
                .create_catalog_in(command, execution, receipt)
                .await?;
            self.projection.require_exact(&observation)?;
            Ok(observation)
        })
    }
    fn drop_catalog_in<'a>(
        &'a self,
        command: CatalogDropCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a novarocks_catalog_application::CatalogMutationReceipt,
    ) -> BoxFuture<'a, Result<(), CatalogApplicationError>> {
        self.application
            .drop_catalog_in(command, execution, receipt)
    }

    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let observation = self.application.create_catalog(command)?;
        self.projection.require_exact(&observation)?;
        Ok(observation)
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        self.application.drop_catalog(command)
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        match self.application.admit_catalog(instance_id) {
            CatalogAdmission::Ready(observation) => {
                match self.projection.require_exact(&observation) {
                    Ok(()) => CatalogAdmission::Ready(observation),
                    Err(error) => CatalogAdmission::Unavailable {
                        reason: error.to_string(),
                    },
                }
            }
            admission => admission,
        }
    }
}

/// Process-local health facts for the Frontend-owned catalog projection.
///
/// The durable attachment remains in StateStore; these fields only describe
/// the local controller that projects it into a runtime generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogProjectionMetricsSnapshot {
    pub projected_catalogs: usize,
    /// Reconcile rounds that completed against the current generation.
    ///
    /// A round, not a poll: there is no cursor and no change feed to poll. Each
    /// round is a complete authoritative reread, whether a local wakeup or the
    /// periodic sweep started it.
    pub successful_rounds: u64,
    pub failed_rounds: u64,
    pub resyncs: u64,
    pub freshness_expiries: u64,
}

/// Publishes Frontend-owned projection health to the process metrics endpoint.
pub fn publish_catalog_projection_metrics(snapshot: CatalogProjectionMetricsSnapshot) {
    crate::catalog_projection_metrics::publish(snapshot);
}

#[cfg(test)]
#[path = "catalog_drop_reference_tests.rs"]
mod drop_reference_tests;

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use novarocks_spi::connector::ConnectorProviderId;

    use super::*;

    fn observation() -> CatalogRuntimeObservation {
        CatalogRuntimeObservation {
            attachment_id: Uuid::now_v7(),
            instance_id: ConnectorInstanceId::parse("warehouse").expect("instance"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            generation: 7,
        }
    }

    #[test]
    fn admission_preserves_not_found_and_unavailable_as_distinct_outcomes() {
        let instance_id = ConnectorInstanceId::parse("warehouse").expect("instance");
        let absent = CatalogAdmission::Absent
            .require_ready(&instance_id)
            .expect_err("absent catalog");
        assert_eq!(absent.kind(), CatalogApplicationErrorKind::NotFound);
        assert_eq!(absent.to_string(), "unknown catalog `warehouse`");
        let unavailable = CatalogAdmission::Unavailable {
            reason: "projection is stale".to_string(),
        }
        .require_ready(&instance_id)
        .expect_err("unavailable catalog");
        assert_eq!(unavailable.kind(), CatalogApplicationErrorKind::Unavailable);
        assert!(
            unavailable.to_string().contains("warehouse")
                && unavailable.to_string().contains("projection is stale"),
            "an unavailable catalog must name itself and keep the reason: {unavailable}"
        );
        assert_eq!(
            CatalogAdmission::Ready(observation())
                .require_ready(&instance_id)
                .expect("ready catalog")
                .generation,
            7
        );
    }

    struct FixedApplication {
        admission: Mutex<CatalogAdmission>,
    }

    impl CatalogApplicationPort for FixedApplication {
        fn create_catalog(
            &self,
            _command: CatalogCreateCommand,
        ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
            self.admission
                .lock()
                .expect("admission lock")
                .clone()
                .require_ready(&_command.instance_id)
        }

        fn drop_catalog(
            &self,
            _command: CatalogDropCommand,
        ) -> Result<(), CatalogApplicationError> {
            Ok(())
        }

        fn admit_catalog(&self, _instance_id: &ConnectorInstanceId) -> CatalogAdmission {
            self.admission.lock().expect("admission lock").clone()
        }
    }

    #[test]
    fn runtime_projection_requires_exact_publish_and_unpublishes_exact_generation() {
        let projection = CatalogRuntimeProjection::new();
        let current = observation();
        let application: Arc<dyn CatalogApplicationPort> = Arc::new(FixedApplication {
            admission: Mutex::new(CatalogAdmission::Ready(current.clone())),
        });
        let bound = projection.bind_application(application);

        assert!(matches!(
            bound.admit_catalog(&current.instance_id),
            CatalogAdmission::Unavailable { .. }
        ));
        projection
            .publish_catalog_runtime(current.clone())
            .expect("publish current runtime");
        assert_eq!(
            bound.admit_catalog(&current.instance_id),
            CatalogAdmission::Ready(current.clone())
        );

        projection
            .unpublish_catalog_runtime(&current.instance_id, current.generation + 1)
            .expect("ignore stale unpublish");
        assert_eq!(
            bound.admit_catalog(&current.instance_id),
            CatalogAdmission::Ready(current.clone())
        );
        projection
            .unpublish_catalog_runtime(&current.instance_id, current.generation)
            .expect("unpublish current runtime");
        assert!(matches!(
            bound.admit_catalog(&current.instance_id),
            CatalogAdmission::Unavailable { .. }
        ));
    }
}
