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
pub mod cluster_heartbeat;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) mod connector_binding;
pub mod fragment_control;
pub mod grpc_client;
pub(crate) mod grpc_fragment_dispatcher;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) mod grpc_query_lifecycle_adapter;
pub mod grpc_query_lifecycle_client;
pub mod grpc_server;
pub mod internal_rpc;
pub(crate) mod metrics_http;
pub mod native_data_plane;
pub use metrics_http::{
    MetricsHttpServer, publish_backend_query_execution_resource,
    publish_backend_query_lifecycle_metrics, publish_backend_query_lifecycle_terminal_limits,
    publish_frontend_query_lifecycle_metrics, render_metrics, render_metrics_json,
};
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub mod native_fragment_ingress;
#[cfg(test)]
pub(crate) mod native_fragment_service_test_fixture;
pub(crate) mod result_batch_wire;
