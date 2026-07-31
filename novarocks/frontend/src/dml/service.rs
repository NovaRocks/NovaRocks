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

use std::sync::Arc;

use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    DmlOperationId, StoredOperation, WriteTransactionOutcome, WriteTransactionSpec,
};
use crate::dml::runner::{AlwaysAdmit, WriteAdmission, WriteExecutor, WriteTransactionRunner};

/// The frontend DML application owner. Composes the narrow ports (journal +
/// admission) and drives write transactions. Constructed from narrow handles —
/// never from the host or a service locator.
pub struct DmlService {
    journal: Arc<dyn OperationJournal>,
    admission: Arc<dyn WriteAdmission>,
}

impl DmlService {
    /// Build a service with single-FE (always-admit) admission.
    pub fn new(journal: Arc<dyn OperationJournal>) -> Self {
        Self {
            journal,
            admission: Arc::new(AlwaysAdmit),
        }
    }

    /// Build a service with a custom admission gate (CP-3 fencing).
    pub fn with_admission(
        journal: Arc<dyn OperationJournal>,
        admission: Arc<dyn WriteAdmission>,
    ) -> Self {
        Self { journal, admission }
    }

    /// Run one Iceberg write transaction with the given executor.
    pub fn run_write<E: WriteExecutor>(
        &self,
        spec: WriteTransactionSpec,
        executor: &E,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let runner =
            WriteTransactionRunner::new(self.journal.as_ref(), executor, self.admission.as_ref());
        runner.run(spec)
    }

    /// Load a stored operation by id.
    pub fn load_operation(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<StoredOperation>, DmlError> {
        self.journal.load(operation_id)
    }

    /// List operations that have not reached a terminal state (recovery input).
    pub fn list_unfinished_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.journal.list_unfinished()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::DmlService;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::{
        CommitOpKind, CommitOutcome, CommitServiceError, OperationKind, OperationState,
        OperationTarget, WriteTransactionSpec,
    };
    use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};

    struct OkExecutor;

    impl WriteExecutor for OkExecutor {
        type CommitHandle = ();

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<()>, String> {
            Ok(CoordinatedWriteReport::Committable(()))
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &(),
        ) -> Result<CommitOutcome, CommitServiceError> {
            Ok(CommitOutcome {
                new_snapshot_id: 7,
                written_manifest_paths: vec![],
            })
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            Ok(())
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            target: OperationTarget {
                catalog: "c".to_string(),
                namespace: "n".to_string(),
                table: "t".to_string(),
                ref_name: None,
            },
            operation_kind: OperationKind::InsertAppend,
            commit_op_kind: CommitOpKind::FastAppend,
            attempt_id: "a".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
        }
    }

    #[test]
    fn service_runs_write_and_exposes_operation() {
        let service = DmlService::new(Arc::new(InMemoryOperationJournal::default()));
        let outcome = service.run_write(spec(), &OkExecutor).unwrap();
        let id = outcome.operation_id.unwrap();
        assert_eq!(
            service.load_operation(id).unwrap().unwrap().state,
            OperationState::Finalized
        );
        assert!(service.list_unfinished_operations().unwrap().is_empty());
    }
}
