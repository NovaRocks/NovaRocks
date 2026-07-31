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

use crate::dml::error::DmlError;
use crate::dml::model::{
    CreatePreparingRequest, DmlOperationId, OperationFact, OperationState, StoredOperation,
};

pub trait OperationJournal: Send + Sync {
    fn create_preparing(&self, request: CreatePreparingRequest)
    -> Result<DmlOperationId, DmlError>;
    fn transition(&self, operation_id: DmlOperationId, to: OperationState) -> Result<(), DmlError>;
    fn record_fact(
        &self,
        operation_id: DmlOperationId,
        fact: OperationFact,
    ) -> Result<(), DmlError>;
    fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError>;
    fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError>;
    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError>;
}

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;
    use crate::dml::model::{DML_OPERATION_SCHEMA_VERSION, validate_operation_transition};
    use crate::dml::now_unix_millis;

    #[derive(Default)]
    pub(crate) struct InMemoryOperationJournal {
        inner: Mutex<BTreeMap<Uuid, StoredOperation>>,
    }

    impl InMemoryOperationJournal {
        pub(crate) fn only_operation(&self) -> StoredOperation {
            let guard = self.inner.lock().unwrap();
            assert_eq!(guard.len(), 1);
            guard.values().next().unwrap().clone()
        }
    }

    impl OperationJournal for InMemoryOperationJournal {
        fn create_preparing(
            &self,
            request: CreatePreparingRequest,
        ) -> Result<DmlOperationId, DmlError> {
            let operation_id = DmlOperationId::new_v7();
            let mutation_id = Uuid::now_v7();
            let stored = StoredOperation {
                schema_version: DML_OPERATION_SCHEMA_VERSION,
                operation_id,
                revision: 1,
                last_mutation_id: mutation_id,
                operation_kind: request.operation_kind,
                operation_subkind: request.operation_subkind,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: request.base_snapshot_id,
                base_snapshot_map: request.base_snapshot_map,
                staged_artifacts: request.staged_artifacts,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            };
            self.inner
                .lock()
                .unwrap()
                .insert(*operation_id.as_uuid(), stored);
            Ok(operation_id)
        }

        fn transition(
            &self,
            operation_id: DmlOperationId,
            to: OperationState,
        ) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let operation = guard
                .get_mut(operation_id.as_uuid())
                .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
            validate_operation_transition(operation.state, to)
                .map_err(DmlError::journal_unavailable)?;
            operation.state = to;
            operation.revision += 1;
            operation.last_mutation_id = Uuid::now_v7();
            operation.updated_at_ms = now_unix_millis();
            if to.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
            }
            Ok(())
        }

        fn record_fact(
            &self,
            operation_id: DmlOperationId,
            fact: OperationFact,
        ) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let operation = guard
                .get_mut(operation_id.as_uuid())
                .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
            validate_operation_transition(operation.state, fact.state)
                .map_err(DmlError::journal_unavailable)?;
            if operation.state == fact.state {
                let identical = operation.commit_outcome == fact.commit_outcome
                    && operation.cleanup_outcome == fact.cleanup_outcome
                    && operation.recovery_evidence == fact.recovery_evidence
                    && operation.failure == fact.failure;
                if !identical {
                    return Err(DmlError::journal_unavailable(format!(
                        "conflicting DML operation fact replay for operation {operation_id}"
                    )));
                }
            }
            operation.state = fact.state;
            operation.commit_outcome = fact
                .commit_outcome
                .or_else(|| operation.commit_outcome.clone());
            operation.cleanup_outcome = fact
                .cleanup_outcome
                .or_else(|| operation.cleanup_outcome.clone());
            operation.recovery_evidence = fact
                .recovery_evidence
                .or_else(|| operation.recovery_evidence.clone());
            operation.failure = fact.failure.or_else(|| operation.failure.clone());
            operation.revision += 1;
            operation.last_mutation_id = Uuid::now_v7();
            operation.updated_at_ms = now_unix_millis();
            if fact.state.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
            }
            Ok(())
        }

        fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .get(operation_id.as_uuid())
                .cloned())
        }

        fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self.inner.lock().unwrap().values().cloned().collect())
        }

        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .values()
                .filter(|operation| !operation.state.is_finished())
                .cloned()
                .collect())
        }
    }
}
