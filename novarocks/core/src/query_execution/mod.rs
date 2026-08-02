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

pub mod artifact;
pub(crate) mod assembly;
pub mod backend;
pub mod cancellation;
mod connector_binding;
pub(crate) mod connector_write_transaction;
pub mod contract;
pub mod control;
pub mod fragment_transport;
#[cfg(test)]
pub(crate) mod in_process_test;
pub mod lifecycle;
pub(crate) mod outcome;
pub use outcome::{ConnectorWriteCompletion, ConnectorWriteStagingSummary, WriteExecutionOutcome};
pub(crate) mod preparation;
pub mod prepared_write;
pub(crate) mod profile;
pub mod read_session;
pub mod request_context;
mod runtime_filter;
#[cfg(feature = "runtime-filter-test-support")]
pub mod schedule;
#[cfg(not(feature = "runtime-filter-test-support"))]
pub(crate) mod schedule;
pub mod service;
pub mod session;
pub mod statistics;
pub mod write;
pub mod write_operation;
pub mod write_plan;

#[cfg(test)]
mod tests;
