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

mod http;

pub(crate) use http::{
    BackendMetricsRegistry, MetricsHttpServer, publish_backend_query_execution_resource,
    publish_connector_write_root_prepared_set_peak, record_backend_native_authentication_failure,
    record_backend_native_tls_handshake_failure, record_connector_write_writer_abort,
    record_connector_write_writer_finished, record_connector_write_writer_open,
    record_fragment_result_terminal, record_task_execution_task_created,
};

#[cfg(debug_assertions)]
pub(crate) use http::record_connector_write_debug_fault;
