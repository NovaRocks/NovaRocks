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

pub mod application;
pub mod capabilities;
pub mod catalog_application;
pub mod catalog_attachment;
pub mod catalog_controller;
mod catalog_projection_metrics;
pub mod client_connection;
pub mod common;
pub mod connector;
pub mod coordinator;
pub mod dml;
pub(crate) mod durable;
pub mod maintenance;
pub mod metrics;
pub mod mv;
mod mysql;
mod native;
pub mod query;
pub mod query_control;
pub mod query_execution;
mod query_lifecycle_error;
pub mod runtime;
pub mod runtime_filter;
mod server;
pub(crate) mod session_error;
pub mod state_family;
pub mod state_store;
pub mod statistics;
pub mod statistics_jobs;
pub mod system_catalog;
pub mod table_maintenance;
mod topology;
mod user_variable;
pub mod view;
pub mod workload_lifecycle;

pub use application::{
    FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig, FrontendQueryControlTimeouts,
};
pub use client_connection::{
    ClientConnectionControlPort, ClientConnectionTerminateOutcome,
    ClientConnectionTerminationReason, ClientConnectionToken, ClientConnectionTokenError,
};
pub use common::admitted_query_context::LakePublicationRuntimePolicy;
pub use connector::typed_control_registry::ConnectorReadControlRegistry;
pub use dml::error::ERROR_CODE_DESCRIPTORS as DML_ERROR_CODE_DESCRIPTORS;
pub use mv::FrontendMvService;
pub use mv::maintenance::MaintenanceCoordinatorConfig;
pub use mv::scheduler::FrontendMvSchedulerConfig;
pub use mysql::session::{
    QueryServiceError, QueryServiceErrorKind, QuerySession, QuerySessionFactory,
    QuerySessionOpenRequest, SessionExecutionSettings,
};
pub use mysql::{
    MysqlClientConnectionRegistry, ResolvedMysqlListenerSettings, resolve_mysql_listener_settings,
    run_mysql_server_until_shutdown,
};
pub use native::report_server::FrontendReportServerHandle;
pub use native::transport::FrontendNativeTransport;
pub use query::FrontendQueryService;
pub use query_lifecycle_error::{QueryLifecycleError, QueryLifecycleErrorCode};
pub use server::{
    FrontendServerConfig, build_frontend_query_session_factory,
    open_frontend_application_for_server, run_frontend_server, run_frontend_server_until_shutdown,
};
pub use session_error::SESSION_ERROR_CODE_DESCRIPTORS;
pub use state_store::{
    OperationId, RunFailure, RunSuccess, StateStoreHost, StateStoreHostInput,
    StateStoreProviderRegistry, derive_transaction_id, run_side_effect_free,
};
pub use statistics::FrontendStatisticsService;
pub use system_catalog::SystemCatalogService;
pub use topology::ClusterBackendOpenConfig;
pub use view::FrontendViewService;
pub use workload_lifecycle::{
    FrontendAdmissionError, FrontendCatalogCounts, FrontendCatalogSnapshotIdentity,
    FrontendCatalogSourceMode, FrontendServingLifecycle, FrontendServingSnapshot,
    FrontendServingSnapshotReader, FrontendServingState, FrontendWorkloadKind,
    FrontendWorkloadLease, LateBoundFrontendServingSnapshotReader,
};
