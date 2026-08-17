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

use novarocks::query_execution::dml::mutation::{
    MutationAbort, MutationCommit, MutationEngine, MutationNativeFragmentEncoder,
    MutationStageOutcome, MutationStatementKind, PrepareMutationRequest, PreparedMutation,
    parse_merge_statement, parse_update_statement,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_protocol::lifecycle::QueryOptions;

use crate::dml::coordination::DmlExternalFenceProposal;
use crate::dml::error::DmlError;
use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{
    ActiveWriteTransactionRunner, CoordinatedWriteReport, WriteExecutor, preparing_request,
};
use crate::dml::service::DmlService;

struct MutationWriteExecutor<'a> {
    engine: &'a dyn MutationEngine,
    prepared: &'a PreparedMutation,
}

/// The Frontend application is the native FE-to-BE encoder caller for durable
/// row-mutation staging. Core supplies only the exact sealed plan/preparation
/// input and receives the resulting bundle for neutral request construction.
struct FrontendMutationNativeFragmentEncoder;

impl MutationNativeFragmentEncoder for FrontendMutationNativeFragmentEncoder {
    fn encode(
        &self,
        input: &novarocks::query_execution::compiler::NativeFragmentEncodingInput,
    ) -> Result<novarocks::query_execution::native_fragment::NativeFragmentAttachment, String> {
        crate::native::fragment_encoder::encode_native_fragment_bundle(input.encoding_view())
    }
}

impl WriteExecutor for MutationWriteExecutor<'_> {
    type CommitHandle = Arc<dyn MutationCommit>;
    type AbortHandle = Arc<dyn MutationAbort>;

    /// UPDATE and MERGE both fence through the exact write authority the
    /// mutation preparation retained, and the same fence must cover the
    /// terminal abort of an already activated authority.
    ///
    /// The reverse port does not expose that authority yet, so this route fails
    /// closed: no writer and no commit may run without a fence the provider can
    /// compare at its external linearization point.
    /// UPDATE and MERGE derive their write lease at preparation precisely so the
    /// fence can be established here, before staging dispatches anything.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so a lease
    /// derived later inside staging would carry no fence at all — which is why
    /// the derivation was hoisted rather than the fence pushed later.
    fn establish_external_fence(
        &self,
        _spec: &WriteTransactionSpec,
        proposal: &DmlExternalFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        self.engine.establish_mutation_external_fence(
            self.prepared.handle.as_ref(),
            &|operation_id, table, target_ref| proposal.seal(operation_id, table, target_ref),
        )
    }

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, String> {
        let native_encoder = FrontendMutationNativeFragmentEncoder;
        match self
            .engine
            .stage_mutation_with_native_encoder(self.prepared.handle.as_ref(), &native_encoder)?
        {
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
        // Preparation is deliberately inert. The admitted and claimed intent
        // below precedes matching, cohort registration and all staging side
        // effects.
        self.require_journal()?;
        let executor = MutationWriteExecutor {
            engine,
            prepared: &prepared,
        };
        let spec = write_transaction_spec(&prepared, subkind);
        let operation = self.begin_write_operation(preparing_request(&spec))?;
        ActiveWriteTransactionRunner::new(operation, &executor).run(spec)?;
        Ok(Some(()))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use novarocks::query_execution::backend::BackendTopologySnapshot;
    use novarocks::query_execution::cancellation::QueryCancellationSource;
    use novarocks::query_execution::dml::mutation::{
        MutationEngine, MutationPrepared, MutationStageOutcome, PrepareMutationRequest,
        PreparedMutation,
    };
    use novarocks::query_execution::request_context::{
        RequestAdmission, RequestContext, SessionOptimizerSettings,
    };
    use novarocks_types::ClusterRole;

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
                operation: novarocks::query_execution::dml::mutation::MutationOperation {
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
            let operations = self.journal.list_operations().unwrap();
            assert_eq!(operations.len(), 1);
            assert_eq!(operations[0].state, OperationState::Writing);
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

    /// The durable intent is still published before anything reaches the
    /// mutation engine, and the statement subkind is still recorded. What
    /// changed with CP-3B is that staging no longer follows: a route whose
    /// write authority cannot establish an external operation fence must not
    /// dispatch a writer at all.
    #[test]
    fn update_intent_is_durable_and_no_stage_runs_without_an_external_fence() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        let error = service
            .try_execute_update(&engine, "UPDATE t SET k = 1", &context(), None)
            .expect_err("an unfenced UPDATE must not stage");

        assert_eq!(*engine.events.lock().unwrap(), ["prepare"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("UPDATE"));
        assert_eq!(record.state, OperationState::Writing);
        assert_eq!(error.operation_id(), Some(record.operation_id));
    }

    #[test]
    fn merge_intent_is_durable_and_no_stage_runs_without_an_external_fence() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        let error = service
            .try_execute_merge(
                &engine,
                "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = s.k",
                &context(),
                None,
            )
            .expect_err("an unfenced MERGE must not stage");

        assert_eq!(*engine.events.lock().unwrap(), ["prepare"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("MERGE"));
        assert_eq!(record.state, OperationState::Writing);
        assert_eq!(error.operation_id(), Some(record.operation_id));
    }
}
