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

use novarocks::engine::delete_engine::{
    DeleteCommit, DeleteEngine, DeleteStatementKind, DeleteWriteReport, PrepareDeleteRequest,
    PreparedDelete, parse_delete_statement, parse_equality_delete_statement,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_execution::runtime::query_options::QueryOptions;

use crate::dml::error::DmlError;
use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};
use crate::dml::service::DmlService;

struct DeleteWriteExecutor<'a> {
    engine: &'a dyn DeleteEngine,
    prepared: &'a PreparedDelete,
}

impl WriteExecutor for DeleteWriteExecutor<'_> {
    type CommitHandle = Arc<dyn DeleteCommit>;
    type AbortHandle = Infallible;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, String> {
        Ok(
            match self.engine.run_delete(self.prepared.handle.as_ref())? {
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
        self.run_write(write_transaction_spec(&prepared), &executor)?;
        Ok(Some(()))
    }
}
