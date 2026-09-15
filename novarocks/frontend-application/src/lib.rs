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

pub(crate) mod application;
pub use application::{
    FrontendApplicationError, FrontendApplicationErrorKind, FrontendExecutionConfig,
    FrontendLogicalExecutionRuntimeConfig, FrontendQueryControlTimeouts,
};
pub(crate) mod capabilities;
mod catalog_application;
mod catalog_controller;
mod catalog_projection_metrics;
mod catalog_prune;
pub(crate) mod connector;
pub(crate) mod coordinator;
mod dml;
pub(crate) mod metrics;
mod mv;
mod native;
mod preparation_diagnostics;
pub(crate) mod query;
mod query_execution;
pub(crate) mod runtime_filter;
pub(crate) mod server;
pub use server::{
    FrontendApplicationOpenConfig, FrontendManagementConfig, FrontendServingConfig,
    open_frontend_application_for_server, serve_ready_frontend_session_factory,
    shutdown_frontend_application_to_convergence, start_frontend_management_server,
};
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
