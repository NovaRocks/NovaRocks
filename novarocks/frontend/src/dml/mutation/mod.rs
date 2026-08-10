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

//! Frontend-owned UPDATE and MERGE application use cases.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks::engine::mutation_engine::{
    MutationAbort, MutationCommit, MutationEngine, MutationStageOutcome, MutationStatementKind,
    PrepareMutationRequest, PreparedMutation, parse_merge_statement, parse_update_statement,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_execution::runtime::query_options::QueryOptions;

use crate::dml::error::DmlError;
use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};
use crate::dml::service::DmlService;

struct MutationWriteExecutor<'a> {
    engine: &'a dyn MutationEngine,
    prepared: &'a PreparedMutation,
}

impl WriteExecutor for MutationWriteExecutor<'_> {
    type CommitHandle = Arc<dyn MutationCommit>;
    type AbortHandle = Arc<dyn MutationAbort>;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, String> {
        match self.engine.stage_mutation(self.prepared.handle.as_ref())? {
            MutationStageOutcome::NoOp => Ok(CoordinatedWriteReport::NoOp),
            MutationStageOutcome::AbortRequired { reason, handle } => {
                Ok(CoordinatedWriteReport::AbortRequired { reason, handle })
            }
            MutationStageOutcome::CommitRequired(handle) => {
                Ok(CoordinatedWriteReport::CommitRequired(handle))
            }
        }
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.engine
            .abort_mutation_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine
            .commit_mutation_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_mutation(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedMutation, subkind: &str) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        target: OperationTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            ref_name: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
        operation_kind: OperationKind::RowDelta,
        operation_subkind: Some(subkind.to_string()),
        attempt_id: operation.attempt_id.clone(),
        base_snapshot_id: operation.base_snapshot_id,
        base_snapshot_map: BTreeMap::new(),
    }
}

impl DmlService {
    pub fn try_execute_update(
        &self,
        engine: &dyn MutationEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        self.try_execute_mutation(
            engine,
            sql,
            context,
            query_options,
            MutationStatementKind::Update,
            "UPDATE",
        )
    }

    pub fn try_execute_merge(
        &self,
        engine: &dyn MutationEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        self.try_execute_mutation(
            engine,
            sql,
            context,
            query_options,
            MutationStatementKind::Merge,
            "MERGE",
        )
    }

    fn try_execute_mutation(
        &self,
        engine: &dyn MutationEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
        kind: MutationStatementKind,
        subkind: &str,
    ) -> Result<Option<()>, DmlError> {
        let recognized = match kind {
            MutationStatementKind::Update => parse_update_statement(sql),
            MutationStatementKind::Merge => parse_merge_statement(sql),
        }
        .map_err(DmlError::executor)?;
        if recognized.is_none() {
            return Ok(None);
        }

        let session = context.session();
        let prepared = engine
            .prepare_mutation(PrepareMutationRequest {
                sql,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
                kind,
            })
            .map_err(DmlError::executor)?;
        // Preparation is deliberately inert.  Requiring the journal after it
        // but before `run_write` guarantees durable intent precedes matching,
        // cohort registration and all staging side effects.
        self.require_journal()?;
        let executor = MutationWriteExecutor {
            engine,
            prepared: &prepared,
        };
        self.run_write(write_transaction_spec(&prepared, subkind), &executor)?;
        Ok(Some(()))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use novarocks::common::app_config::ClusterRole;
    use novarocks::engine::mutation_engine::{
        MutationAbort, MutationCommit, MutationEngine, MutationPrepared, MutationStageOutcome,
        MutationStatementKind, PrepareMutationRequest, PreparedMutation,
    };
    use novarocks::query_execution::backend::BackendTopologySnapshot;
    use novarocks::query_execution::cancellation::QueryCancellationSource;
    use novarocks::query_execution::request_context::{
        RequestAdmission, RequestContext, SessionOptimizerSettings,
    };

    use super::*;
    use crate::dml::OperationJournal;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::OperationState;

    struct TestPrepared;

    impl MutationPrepared for TestPrepared {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct RecordingMutationEngine {
        journal: Arc<InMemoryOperationJournal>,
        events: Mutex<Vec<&'static str>>,
    }

    impl MutationEngine for RecordingMutationEngine {
        fn prepare_mutation(
            &self,
            request: PrepareMutationRequest<'_>,
        ) -> Result<PreparedMutation, String> {
            self.events.lock().expect("events").push("prepare");
            Ok(PreparedMutation {
                operation: novarocks::engine::mutation_engine::MutationOperation {
                    kind: request.kind,
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    target_ref: "main".to_string(),
                    attempt_id: "mutation-test".to_string(),
                    base_snapshot_id: Some(7),
                },
                handle: Arc::new(TestPrepared),
            })
        }

        fn stage_mutation(
            &self,
            _prepared: &dyn MutationPrepared,
        ) -> Result<MutationStageOutcome, String> {
            assert_eq!(self.journal.list_operations().unwrap().len(), 1);
            self.events.lock().expect("events").push("stage");
            Ok(MutationStageOutcome::NoOp)
        }

        fn finalize_mutation(&self, _prepared: &dyn MutationPrepared) -> Result<(), String> {
            unreachable!("no-op mutation must not finalize")
        }
    }

    fn context() -> RequestContext {
        let cancellation = QueryCancellationSource::new();
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
            "db".to_string(),
            ClusterRole::AllInOne,
            BackendTopologySnapshot::empty(11),
            Some(Instant::now() + Duration::from_secs(30)),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ))
    }

    #[test]
    fn update_intent_is_durable_before_stage_and_carries_subkind() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        assert_eq!(
            service
                .try_execute_update(&engine, "UPDATE t SET k = 1", &context(), None)
                .unwrap(),
            Some(())
        );
        assert_eq!(*engine.events.lock().unwrap(), ["prepare", "stage"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("UPDATE"));
        assert_eq!(record.state, OperationState::Finalized);
    }

    #[test]
    fn merge_intent_is_durable_before_stage_and_carries_subkind() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        assert_eq!(
            service
                .try_execute_merge(
                    &engine,
                    "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = s.k",
                    &context(),
                    None,
                )
                .unwrap(),
            Some(())
        );
        assert_eq!(*engine.events.lock().unwrap(), ["prepare", "stage"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("MERGE"));
        assert_eq!(record.state, OperationState::Finalized);
    }
}
