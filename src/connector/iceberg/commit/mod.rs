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

//! Foundational types for Iceberg commit operations.
//!
//! Exports [`CommitOpKind`] (which commit action to run), [`WrittenFile`]
//! (metadata for a single Parquet file produced by the pipeline), and
//! [`CommitOutcome`] (result of a successful commit).

mod abort;
mod action;
mod collector;
mod data_file;
mod fast_append;
mod helpers;
mod overwrite;
mod position_delete_writer;
mod row_delta;
mod run;
mod types;
mod validation;

pub use abort::{AbortLog, CleanupError};
pub use action::{CommitCtx, IcebergCommitAction};
pub use collector::IcebergCommitCollector;
pub use fast_append::FastAppendCommit;
pub use overwrite::OverwriteCommit;
pub use position_delete_writer::{PositionDeleteGroup, write_position_delete_files};
pub use row_delta::RowDeltaCommit;
pub use run::{RunInput, run_iceberg_commit};
pub use types::{CommitOpKind, CommitOutcome, WrittenFile};
pub use validation::{
    ensure_no_equality_deletes, ensure_single_partition_spec, ensure_v3_writable,
    match_select_schema_to_table,
};
