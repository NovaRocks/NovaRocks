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

//! Cohesive Core execution kernels.
//!
//! These values are deliberately separate.  They are the replacement seams
//! for implementations that previously borrowed the Core application facade; they must not
//! be gathered into another application context or service locator.

use std::sync::Arc;

use crate::common::backend_topology::BackendTopologyService;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::query_execution::service::QueryExecutionService;
use crate::view::ViewService;
use novarocks::catalog_application::CatalogApplicationPort;
use novarocks::catalog_application::query_catalog::QueryCatalogService;
use novarocks::catalog_application::system_catalog::SystemCatalog;
use novarocks::connector::MvBackend;
use novarocks::connector::unified_statistics::UnifiedStatisticsResolver;
use novarocks::mv::application::MvApplicationService;
use novarocks::mv::repository::MvRepository;
use novarocks::mv::storage_observation::MvStorageObservationPort;
use novarocks_spi::connector::ConnectorControlRegistry;

/// Query compilation and distributed-query preparation dependencies.
///
/// Catalog state stays query-specific here.  DML/MV command execution receives
/// only the leaf ports it uses rather than a reference back to this kernel.
#[derive(Clone)]
pub struct QueryPreparationKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    query_execution: QueryExecutionService,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
}

/// Read-only system-table query dependencies.
///
/// `information_schema` materialization is query preparation, not a command
/// service.  It receives exactly the local catalog snapshot source, the
/// connector control resolver needed for namespace facts, the injected system
/// catalog, and the durable MV metadata reader.  In particular, it has no DML
/// or MV mutation capability.
#[derive(Clone)]
pub struct SystemTableQueryKernel {
    catalog_service: Arc<QueryCatalogService>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    system_catalog: Arc<dyn SystemCatalog>,
    mv_repository: Arc<dyn MvRepository>,
}

impl SystemTableQueryKernel {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        system_catalog: Arc<dyn SystemCatalog>,
        mv_repository: Arc<dyn MvRepository>,
    ) -> Self {
        Self {
            catalog_service,
            connector_control,
            system_catalog,
            mv_repository,
        }
    }

    // System-table materialization takes these ports one by one, so the caller
    // that owns the kernel reads them and hands Core exactly what it needs.
    pub fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub fn system_catalog(&self) -> &Arc<dyn SystemCatalog> {
        &self.system_catalog
    }

    pub fn mv_repository(&self) -> &Arc<dyn MvRepository> {
        &self.mv_repository
    }
}

impl QueryPreparationKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        query_execution: QueryExecutionService,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            query_execution,
            backend_topology,
            exchange_port,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }

    pub(crate) fn backend_topology(&self) -> &BackendTopologyService {
        &self.backend_topology
    }

    pub(crate) const fn exchange_port(&self) -> u16 {
        self.exchange_port
    }
}

/// Foreground and historical DML execution dependencies, including CTAS.
///
/// The CTAS recovery adapter must use this same connector-control generation;
/// it is not a separate recovery context or scheduler.
#[derive(Clone)]
pub struct DmlExecutionKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

impl DmlExecutionKernel {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            mv_storage_observation,
            query_execution,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }
}

/// Catalog DDL dependencies.
///
/// This is intentionally a catalog-only kernel: it can mutate catalog facts
/// and enforce catalog-adjacent MV/view guards, but has no query execution,
/// statistics, DML writer or MV refresh capability.
#[derive(Clone)]
pub struct CatalogCommandKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    view_service: Arc<dyn ViewService>,
}

impl CatalogCommandKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_repository,
            mv_storage_observation,
            view_service,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn mv_repository(&self) -> &Arc<dyn MvRepository> {
        &self.mv_repository
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub fn view_service(&self) -> &Arc<dyn ViewService> {
        &self.view_service
    }
}

/// MV metadata and refresh execution dependencies.
///
/// The backend is injected directly; the obsolete string-keyed
/// `ConnectorRegistry` is intentionally not represented here.
#[derive(Clone)]
pub struct MvExecutionKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_backend: Arc<dyn MvBackend>,
    repository: Arc<dyn MvRepository>,
    application: Arc<dyn MvApplicationService>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

impl MvExecutionKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_backend: Arc<dyn MvBackend>,
        repository: Arc<dyn MvRepository>,
        application: Arc<dyn MvApplicationService>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            mv_backend,
            repository,
            application,
            storage_observation,
            query_execution,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn mv_backend(&self) -> &Arc<dyn MvBackend> {
        &self.mv_backend
    }

    pub(crate) fn repository(&self) -> &Arc<dyn MvRepository> {
        &self.repository
    }

    pub(crate) fn application(&self) -> &Arc<dyn MvApplicationService> {
        &self.application
    }

    pub(crate) fn storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }
}

/// View command dependencies.
#[derive(Clone)]
pub struct ViewExecutionKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    view_service: Arc<dyn ViewService>,
}

impl ViewExecutionKernel {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            view_service,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub fn view_service(&self) -> &Arc<dyn ViewService> {
        &self.view_service
    }
}

/// Table-maintenance command dependencies.
#[derive(Clone)]
pub struct MaintenanceExecutionKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceExecutionKernel {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
        service: Arc<dyn TableMaintenanceService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_storage_observation,
            query_execution,
            service,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }

    pub(crate) fn service(&self) -> &Arc<dyn TableMaintenanceService> {
        &self.service
    }
}

// Ownership: `CatalogAdmission` is Core's contract for target resolution, but
// each `impl` belongs to the value that holds the port. These blocks therefore
// live beside the kernels and move to Frontend together with them.
macro_rules! impl_kernel_catalog_admission {
    ($kernel:ty) => {
        impl novarocks::catalog_application::resolver::CatalogAdmission for $kernel {
            fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
                self.catalog_application().map(Arc::as_ref)
            }
        }
    };
}

impl_kernel_catalog_admission!(QueryPreparationKernel);
impl_kernel_catalog_admission!(CatalogCommandKernel);
impl_kernel_catalog_admission!(DmlExecutionKernel);
impl_kernel_catalog_admission!(MvExecutionKernel);
impl_kernel_catalog_admission!(ViewExecutionKernel);
impl_kernel_catalog_admission!(MaintenanceExecutionKernel);

/// FE-owned backend membership is intentionally a separate command capability.
#[derive(Clone)]
pub struct BackendManagementKernel {
    topology: BackendTopologyService,
}

impl BackendManagementKernel {
    pub fn new(topology: BackendTopologyService) -> Self {
        Self { topology }
    }

    pub(crate) fn topology(&self) -> &BackendTopologyService {
        &self.topology
    }
}

/// Session catalog admission and namespace lookup.
///
/// This is deliberately not part of generic command dispatch: `USE` and
/// `SET CATALOG` are session admission operations.
#[derive(Clone)]
pub struct SessionCatalogResolver {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
}

impl SessionCatalogResolver {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
        }
    }

    pub fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        self.catalog_service
            .local()
            .read()
            .map_err(|_| "query catalog read lock poisoned".to_string())?
            .database_exists(database_name)
    }

    pub fn require_external_catalog_ready(
        &self,
        catalog_name: &str,
    ) -> Result<(), novarocks::catalog_application::CatalogApplicationError> {
        let application = self.catalog_application.as_ref().ok_or_else(|| {
            novarocks::catalog_application::CatalogApplicationError::new(
                novarocks::catalog_application::CatalogApplicationErrorKind::Unavailable,
                "external catalogs require a configured frontend catalog application",
            )
        })?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog_name)
            .map_err(|error| {
                novarocks::catalog_application::CatalogApplicationError::new(
                    novarocks::catalog_application::CatalogApplicationErrorKind::InvalidRequest,
                    format!("invalid catalog connector instance ID: {error}"),
                )
            })?;
        application
            .admit_catalog(&instance_id)
            .require_ready(&instance_id)
            .map(|_| ())
    }

    pub fn iceberg_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, String> {
        let context = novarocks::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?;
        novarocks::connector::metadata_namespace_exists(
            self.connector_control.as_ref(),
            context,
            catalog_name,
            namespace_name,
        )
    }
}

/// Parser-private command family selected by Core syntax classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandDomain {
    Query,
    Dml,
    Catalog,
    Mv,
    Statistics,
    View,
    Maintenance,
    BackendManagement,
    Unsupported,
}

/// An opaque, normalized SQL token.  Frontend may route only on `domain`; it
/// must hand this token back to the matching typed Core capability rather than
/// reparsing SQL or using an arbitrary-SQL executor.
#[derive(Clone, Debug)]
pub(crate) struct PreparedCommand {
    domain: CommandDomain,
    normalized_sql: String,
}

impl PreparedCommand {
    pub(crate) const fn domain(&self) -> CommandDomain {
        self.domain
    }

    pub(crate) fn normalized_sql(&self) -> &str {
        &self.normalized_sql
    }
}

/// Pure statement-family classifier.  It performs no catalog lookup, config
/// read, topology snapshot, or recovery action.
pub(crate) fn classify_command(sql: &str) -> Result<PreparedCommand, String> {
    let normalized_sql = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
    let upper = normalized_sql.trim_start().to_ascii_uppercase();
    let domain = if is_query_prefix(&upper) {
        CommandDomain::Query
    } else if starts_with_any(
        &upper,
        &[
            "INSERT",
            "DELETE",
            "UPDATE",
            "MERGE",
            "TRUNCATE",
            "ADD FILES",
        ],
    ) || (upper.starts_with("CREATE TABLE") && upper.contains(" AS "))
    {
        CommandDomain::Dml
    } else if starts_with_any(
        &upper,
        &[
            "ANALYZE",
            "CANCEL ANALYZE",
            "SHOW ANALYZE",
            "SHOW TABLE STATS",
        ],
    ) {
        CommandDomain::Statistics
    } else if starts_with_any(&upper, &["OPTIMIZE", "ALTER TABLE"]) {
        CommandDomain::Maintenance
    } else if starts_with_any(
        &upper,
        &[
            "CREATE MATERIALIZED VIEW",
            "DROP MATERIALIZED VIEW",
            "REFRESH MATERIALIZED VIEW",
            "SHOW MATERIALIZED VIEW",
        ],
    ) {
        CommandDomain::Mv
    } else if starts_with_any(&upper, &["CREATE VIEW", "DROP VIEW", "ALTER VIEW"]) {
        CommandDomain::View
    } else if starts_with_any(&upper, &["ADD BACKEND", "DROP BACKEND", "SHOW BACKENDS"]) {
        CommandDomain::BackendManagement
    } else if starts_with_any(
        &upper,
        &[
            "CREATE CATALOG",
            "DROP CATALOG",
            "CREATE DATABASE",
            "DROP DATABASE",
            "CREATE TABLE",
            "DROP TABLE",
        ],
    ) {
        CommandDomain::Catalog
    } else {
        CommandDomain::Unsupported
    };
    Ok(PreparedCommand {
        domain,
        normalized_sql,
    })
}

fn starts_with_any(sql: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|prefix| sql.starts_with(prefix))
}

fn is_query_prefix(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    match words.next() {
        Some("SELECT") | Some("WITH") => true,
        Some("EXPLAIN") => {
            let mut target = words.next();
            while matches!(target, Some("ANALYZE" | "VERBOSE" | "COSTS" | "LOGICAL")) {
                target = words.next();
            }
            matches!(target, Some("SELECT") | Some("WITH"))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandDomain, classify_command};

    #[test]
    fn classifier_is_pure_and_routes_ctas_to_dml() {
        let prepared = classify_command("CREATE TABLE dst AS SELECT 1").expect("classification");
        assert_eq!(prepared.domain(), CommandDomain::Dml);
        assert_eq!(prepared.normalized_sql(), "CREATE TABLE dst AS SELECT 1");
    }

    #[test]
    fn classifier_keeps_session_catalog_out_of_generic_commands() {
        let prepared = classify_command("USE warehouse.db").expect("classification");
        assert_eq!(prepared.domain(), CommandDomain::Unsupported);
    }
}
