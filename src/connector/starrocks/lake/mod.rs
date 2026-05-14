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

// Lake transaction modules are scaffolded for publish/abort/compaction/vacuum
// operations that will be wired when FE lake RPCs are connected. Suppress
// dead-code warnings at the module level rather than per-item.
#[allow(dead_code)]
pub(crate) mod abort_executor;
#[allow(dead_code)]
pub(crate) mod abort_policy;
#[allow(dead_code)]
pub(crate) mod applier;
#[allow(dead_code)]
pub(crate) mod compaction;
pub(crate) mod context;
pub(crate) mod create_tablet;
pub(crate) mod delete_payload_codec;
pub(crate) mod delete_predicate_proto;
#[allow(dead_code)]
pub(crate) mod pk_applier;
#[allow(dead_code)]
pub(crate) mod replay_policy;
pub(crate) mod schema;
pub(crate) mod schema_change;
#[allow(dead_code)]
pub(crate) mod transactions;
#[allow(dead_code)]
pub(crate) mod txn_loader;
pub(crate) mod txn_log;

pub(crate) use context::TabletWriteContext;
pub(crate) use create_tablet::create_lake_tablet_from_req;
pub(crate) use schema::build_sink_tablet_schema;
pub(crate) use schema_change::{execute_alter_tablet_task, execute_update_tablet_meta_info_task};
pub(crate) use txn_log::append_lake_txn_log_with_chunk_rowset;
#[cfg(test)]
pub(crate) use txn_log::append_lake_txn_log_with_rowset;
