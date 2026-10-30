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

#[cfg(all(feature = "mem-1-m07-hms-listing-observe", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-hms-listing-observe requires test-controls");
#[cfg(all(feature = "mem-1-m07-closing-pressure", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-closing-pressure requires test-controls");
#[cfg(all(feature = "mem-1-m07-root-observation", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-root-observation requires test-controls");
#[cfg(all(feature = "mem-1-m07-exact-mysql-write", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-exact-mysql-write requires test-controls");

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            "mem-1-m07-hms-listing-observe",
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            "mem-1-m07-closing-pressure",
            #[cfg(feature = "mem-1-m07-root-observation")]
            "mem-1-m07-root-observation",
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            "mem-1-m07-exact-mysql-write",
        ],
    )
}

/// Native Task protocol bounds frozen by frontend process composition.
pub mod task_protocol_config;

mod native_workload;
pub use native_workload::{
    FrontendNativeCapacityInputs, FrontendStatementIngress, FrontendWorkloadCapacityConfig,
    FrontendWorkloadCapacityProfile,
};

pub(crate) mod application;
pub use application::{
    FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig, FrontendLogicalExecutionRuntimeConfig, FrontendQueryControlTimeouts,
    FrontendResultCapacityProfile,
};
pub(crate) mod capabilities;
mod catalog_application;
mod catalog_controller;
mod catalog_projection_metrics;
mod catalog_prune;
pub(crate) mod connector;
mod dml;
pub(crate) mod metrics;
mod mv;
pub use mv::management_audit::FileManagementAuditSink;
pub use mv::startup_isolation_file::StartupIsolationSource;
mod native;
pub use query_execution::package_freeze::CompiledPackageCarrier;

/// Known metadata facts of the original closed Internal CPU job factory.
pub use query_execution::internal_result_cpu::{
    InternalCpuActionQuote, InternalCpuMetadataGeometry,
};
#[cfg(novarocks_dev_controls)]
mod preparation_diagnostics;
pub(crate) mod query;
mod query_execution;
pub(crate) mod runtime_filter;
pub(crate) mod server;
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub use catalog_application::admission_completion::{
    InstalledOwner as HmsAdmissionInstalledOwner, OwnerSelector as HmsAdmissionOwnerSelector,
};
pub use metrics::FrontendProcessMemoryObservation;
pub use server::{
    FrontendApplicationOpenConfig, FrontendManagementConfig, FrontendManagementServer,
    FrontendServingConfig, open_frontend_application_for_server,
    prepare_frontend_application_for_server, serve_ready_frontend_session_factory,
    shutdown_frontend_application_to_convergence, start_frontend_management_server,
};
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub use server::{HmsListingObservationHandler, HmsListingObservationSetup};
#[cfg(test)]
mod state_family;
pub(crate) mod state_store;
pub(crate) mod statistics;
pub(crate) mod statistics_jobs;
pub(crate) mod table_maintenance;
pub(crate) mod task_execution;
pub(crate) mod topology;
mod topology_metrics;
pub use topology::ClusterBackendOpenConfig;
mod view;
pub(crate) mod workload_lifecycle;

#[cfg(novarocks_dev_controls)]
pub mod test_session;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
