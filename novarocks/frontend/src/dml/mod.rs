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

//! Frontend DML application owner.
//!
//! Establishes the statement-agnostic Iceberg write-transaction foundation:
//! a typed operation model + state machine, an operation-journal repository
//! port, a runner lifecycle with an executor seam, and a commit/abort/reconcile
//! contract. DML-1 ships the runner + fake-backed tests; the real executor and
//! SQL routing land in DML-2.

mod delete;
pub mod error;
pub mod insert;
pub mod journal;
pub mod model;
pub mod reconcile;
pub mod runner;
pub mod service;
pub mod state_store_journal;

pub use error::{DmlError, DmlErrorKind};
pub use insert::{InsertCommand, InsertCommandSource, convert_insert_command, reorder_insert_rows};
pub use journal::OperationJournal;
pub use model::{
    CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, CreatePreparingRequest,
    DmlOperationId, IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord,
    IcebergOperationFailureKind, IcebergOperationFailureRecord, IcebergOperationNextAction,
    IcebergRecoveryEvidenceRecord, OperationFact, OperationKind, OperationState, OperationTarget,
    RecoveryEvidence, StoredOperation, WriteTransactionOutcome, WriteTransactionSpec,
};
pub use runner::{
    AlwaysAdmit, CoordinatedWriteReport, WriteAdmission, WriteExecutor, WriteTransactionRunner,
};
pub use service::DmlService;
pub use state_store_journal::StateStoreOperationJournal;

/// Current wall-clock time in Unix milliseconds, used for operation timestamps.
pub(crate) fn now_unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}
