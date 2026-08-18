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

//! Frontend-owned DELETE statement recognition and application routing.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use crate::common::admitted_query_context::RequestContext;
use crate::query_execution::dml::delete::{
    DeleteCommit, DeleteEngine, DeleteStatementKind, DeleteWriteReport, PrepareDeleteRequest,
    PreparedDelete, parse_delete_statement, parse_equality_delete_statement,
};
use novarocks_protocol::lifecycle::QueryOptions;

use crate::dml::coordination::DmlExternalFenceProposal;
use crate::dml::error::DmlError;
use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{
    ActiveWriteTransactionRunner, CoordinatedWriteReport, WriteExecutor, preparing_request,
};
use crate::dml::service::DmlService;

struct DeleteWriteExecutor<'a> {
    engine: &'a dyn DeleteEngine,
    prepared: &'a PreparedDelete,
}

impl WriteExecutor for DeleteWriteExecutor<'_> {
    type CommitHandle = Arc<dyn DeleteCommit>;
    type AbortHandle = Infallible;

    /// Predicate and equality DELETE both fence through the exact write
    /// authority the DELETE preparation retained.
    ///
    /// The reverse port does not expose that authority yet, so this route fails
    /// closed: no writer and no commit may run without a fence the provider can
    /// compare at its external linearization point.
    /// Predicate and equality DELETE both activate their write generation during
    /// preparation, so the authority already exists here — before anything is
    /// dispatched. The route only supplies the sealing closure; the resource
    /// identity comes from the activated template.
    fn establish_external_fence(
        &self,
        _spec: &WriteTransactionSpec,
        proposal: &DmlExternalFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        self.engine.establish_delete_external_fence(
            self.prepared.handle.as_ref(),
            &|operation_id, table, target_ref| proposal.seal(operation_id, table, target_ref),
        )
    }

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, String> {
        let encoding = self
            .engine
            .delete_native_encoding(self.prepared.handle.as_ref())?;
        let input = encoding.input()?;
        let native_bundle =
            crate::native::fragment_encoder::encode_native_fragment_bundle(input.encoding_view())?;
        drop(encoding);
        Ok(
            match self
                .engine
                .run_delete_with_native_bundle(self.prepared.handle.as_ref(), native_bundle)?
            {
                DeleteWriteReport::Aborted { reason, .. } => {
                    CoordinatedWriteReport::Aborted { reason }
                }
                DeleteWriteReport::NoOp => CoordinatedWriteReport::NoOp,
                DeleteWriteReport::CommitRequired(handle) => {
                    CoordinatedWriteReport::CommitRequired(handle)
                }
            },
        )
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        match *handle {}
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
            .commit_delete_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_delete(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedDelete) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        target: OperationTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            ref_name: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
        operation_kind: OperationKind::RowDelta,
        operation_subkind: None,
        attempt_id: operation.attempt_id.clone(),
        base_snapshot_id: operation.base_snapshot_id,
        base_snapshot_map: BTreeMap::new(),
    }
}

impl DmlService {
    pub fn try_execute_delete(
        &self,
        engine: &dyn DeleteEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        let kind = if parse_delete_statement(sql)
            .map_err(DmlError::executor)?
            .is_some()
        {
            DeleteStatementKind::Predicate
        } else if parse_equality_delete_statement(sql)
            .map_err(DmlError::executor)?
            .is_some()
        {
            DeleteStatementKind::Equality
        } else {
            return Ok(None);
        };

        self.require_journal()?;
        let session = context.session();
        let prepared = engine
            .prepare_delete(PrepareDeleteRequest {
                sql,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
                kind,
            })
            .map_err(DmlError::executor)?;
        let executor = DeleteWriteExecutor {
            engine,
            prepared: &prepared,
        };
        let spec = write_transaction_spec(&prepared);
        let operation = self.begin_write_operation(preparing_request(&spec))?;
        ActiveWriteTransactionRunner::new(operation, &executor).run(spec)?;
        Ok(Some(()))
    }
}
