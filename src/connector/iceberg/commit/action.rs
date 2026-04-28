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

//! Trait abstraction over the three commit-action implementations
//! (FastAppend / Overwrite / RowDelta).
//!
//! `CommitCtx` carries everything an action needs to write manifests and
//! call `Catalog::update_table`. `FileIO` is supplied explicitly rather than
//! lifted from `table.file_io()` so that engine-side staging credentials and
//! catalog-default credentials can differ when needed.

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::table::Table;
use uuid::Uuid;

use super::abort::AbortLog;
use super::collector::IcebergCommitCollector;
use super::types::CommitOutcome;

pub struct CommitCtx<'a> {
    pub collector: &'a IcebergCommitCollector,
    pub table: &'a Table,
    pub catalog: &'a dyn Catalog,
    pub file_io: &'a FileIO,
    pub commit_uuid: Uuid,
    pub abort_handle: Arc<AbortLog>,
}

#[async_trait]
pub trait IcebergCommitAction: Send + Sync {
    /// Stage any manifests required, build a `TableCommit`, and submit it via
    /// `Catalog::update_table`. Implementations must record every staged
    /// manifest path on `ctx.abort_handle` so that a later failure can clean
    /// them up. On the success path the orchestrator does not call
    /// `AbortLog::cleanup`, so the records are harmless.
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String>;
}
