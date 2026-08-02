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
include!(concat!(env!("OUT_DIR"), "/proto_root_mod.rs"));

#[cfg(test)]
mod tests;

pub mod cache;
pub(crate) mod catalog_attachment;
pub mod common;
pub mod connector;
pub mod engine;
pub mod exec;
pub mod formats;
pub mod fs;
pub mod meta;
pub mod mv;
pub mod protocol;
pub mod query_execution;
pub mod runtime;
pub(crate) mod runtime_filter;
#[doc(hidden)]
pub mod runtime_filter_transition;
pub mod server;
pub mod service;
pub mod sql;
pub use novarocks_version as version;
// StarRocks-BE-like folder layout, with `novarocks_*` convenience aliases.
pub use common::app_config as novarocks_config;
pub use common::logging as novarocks_logging;
pub use connector as novarocks_connectors;

pub use common::types::FetchResult;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub use service::grpc_server::start_grpc_exchange_server;
