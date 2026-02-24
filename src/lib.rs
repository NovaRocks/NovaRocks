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
include!(concat!(env!("OUT_DIR"), "/thrift_root_mod.rs"));

pub mod cache;
pub mod common;
pub mod connector;
pub mod exec;
pub mod formats;
pub mod fs;
pub mod lower;
pub mod runtime;
pub mod service;

// StarRocks-BE-like folder layout, with `novarocks_*` convenience aliases.
pub use common::app_config as novarocks_config;
pub use common::logging as novarocks_logging;
pub use connector as novarocks_connectors;
pub use connector::hdfs as novarocks_connector_iceberg;
pub use connector::jdbc as novarocks_connector_jdbc;
pub use connector::starrocks as novarocks_connector_starrocks;
pub use formats::parquet as novarocks_format_parquet;
pub use fs::local as novarocks_fs_local;
pub use fs::opendal as novarocks_fs_opendal;
pub use fs::oss as novarocks_fs_oss;
pub use fs::path as novarocks_fs_path;

pub use common::types::{FetchResult, UniqueId};
pub use service::grpc_server::start_grpc_server;
pub use service::internal_service::{
    cancel, submit_exec_batch_plan_fragments, submit_exec_plan_fragment,
};
