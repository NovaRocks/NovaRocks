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

//! Consumer-owned catalog control and admission contracts.
//!
//! Core consumes these facts but never owns the durable attachment record,
//! provider factory, or a provider-concrete catalog handle. Frontend owns
//! those control-plane concerns and projects Ready observations into this
//! boundary.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use uuid::Uuid;

pub mod command;
pub mod create_table_ddl;
pub mod iceberg_ref_command;
pub mod information_schema;
pub mod query_bindings;
pub mod query_catalog;
pub mod query_materializer;
pub mod resolver;
pub mod statement;
pub mod system_catalog;
pub mod virtual_table;

pub mod frontend_port;
pub use frontend_port::FrontendCatalogApplicationPort;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCreateCommand {
    pub instance_id: ConnectorInstanceId,
    pub display_name: String,
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogDropCommand {
    pub instance_id: ConnectorInstanceId,
    pub if_exists: bool,
}

/// The exact identity that Core may admit into a query/runtime path.
///
/// `attachment_id` distinguishes a catalog recreated under the same SQL
/// name; `generation` distinguishes locally retired and republished runtime
/// projections of that durable attachment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRuntimeObservation {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogAdmission {
    Absent,
    Unavailable { reason: String },
    Ready(CatalogRuntimeObservation),
}

impl CatalogAdmission {
    /// Resolves the admission, naming the catalog in both failure messages.
    ///
    /// Operators and tests match on the catalog name, so an absent attachment
    /// must not surface as an anonymous "not found".
    pub fn require_ready(
        self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        match self {
            Self::Ready(observation) => Ok(observation),
            Self::Absent => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::NotFound,
                format!("unknown catalog `{}`", instance_id.as_str()),
            )),
            Self::Unavailable { reason } => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                format!(
                    "catalog `{}` is unavailable on this frontend: {reason}",
                    instance_id.as_str()
                ),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogApplicationErrorKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogApplicationError {
    kind: CatalogApplicationErrorKind,
    message: String,
}

impl CatalogApplicationError {
    pub fn new(kind: CatalogApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> CatalogApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for CatalogApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CatalogApplicationError {}

/// Frontend's catalog command and admission dependency.
///
/// Implemented by Frontend. Core must not downcast this port to access an
/// attachment repository, control host, registry, or provider handle.
// Design: ADR-0066 (docs/adr/ADR-0066-state-store-catalog-attachment-authority.md)
pub trait CatalogApplicationPort: Send + Sync {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError>;

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError>;

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission;
}

/// The provider-neutral sink Frontend uses to project a Ready catalog runtime
/// into Core. It deliberately exposes only exact observations and retirement,
/// never a concrete registry or provider handle.
pub trait CatalogRuntimePublisherSink: Send + Sync {
    fn publish_catalog_runtime(
        &self,
        observation: CatalogRuntimeObservation,
    ) -> Result<(), CatalogApplicationError>;

    fn unpublish_catalog_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
        generation: u64,
    ) -> Result<(), CatalogApplicationError>;
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

/// Frontend-owned exact runtime publication set.
///
/// Frontend publishes only after a local Connector control generation is
/// installed and unpublishes before retiring it. Core wraps the Frontend
/// application port with this set so a stale or partially installed local
/// projection can never be admitted into query materialization.
// Design: ADR-0066 (docs/adr/ADR-0066-state-store-catalog-attachment-authority.md)
pub struct CatalogRuntimeProjection {
    published: Mutex<BTreeMap<ConnectorInstanceId, CatalogRuntimeObservation>>,
    query_catalog: Mutex<Option<QueryCatalogBinding>>,
}

impl CatalogRuntimeProjection {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            published: Mutex::new(BTreeMap::new()),
            query_catalog: Mutex::new(None),
        })
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
    pub successful_polls: u64,
    pub failed_polls: u64,
    pub resyncs: u64,
    pub freshness_expiries: u64,
}

/// Publishes Frontend-owned projection health to the process metrics endpoint.
pub fn publish_catalog_projection_metrics(snapshot: CatalogProjectionMetricsSnapshot) {
    crate::catalog_projection_metrics::publish(snapshot);
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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
