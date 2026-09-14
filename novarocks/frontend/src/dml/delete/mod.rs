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

use std::convert::Infallible;
use std::sync::Arc;

use crate::query_execution::dml::delete::{
    DeleteCommit, DeleteEngine, DeleteStatement, DeleteWriteReport, PrepareDeleteRequest,
    PreparedDelete,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_spi::connector::LakePublicationId;

use crate::dml::error::DmlError;
use crate::dml::runner::{
    CoordinatedWriteReport, StatementWriteTransactionRunner, WriteExecutor, WriteTarget,
    WriteTransactionSpec,
};
use crate::dml::service::DmlService;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;
use novarocks_spi::connector::LakePublicationFamily;

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
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, DmlError> {
        let encoding = self
            .engine
            .delete_native_encoding(self.prepared.handle.as_ref())
            .map_err(|error| error.into_dml_error(Some(&self.prepared.sql_source)))?;
        let input = encoding.input().map_err(DmlError::executor)?;
        let native_bundle =
            crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(input)
                .map_err(DmlError::executor)?;
        drop(encoding);
        Ok(
            match self
                .engine
                .run_delete_with_native_bundle(self.prepared.handle.as_ref(), native_bundle)
                .map_err(DmlError::executor)?
            {
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

    fn adjudicate_publication(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine.adjudicate_delete_publication(
            self.prepared.handle.as_ref(),
            handle.as_ref(),
            evidence,
        )
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_delete(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedDelete) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        publication_id: operation.publication_id,
        target: WriteTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            reference: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
    }
}

impl DmlService {
    /// Executes a parser-owned DELETE variant. `source` is carried only for
    /// AST-span slices during preparation; it is never reparsed here.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn execute_delete(
        &self,
        engine: &dyn DeleteEngine,
        statement: DeleteStatement<'_>,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        if let DeleteStatement::Predicate(delete) = statement
            && delete.selection.is_none()
        {
            return Err(DmlError::admit(DmlAdmissionError::DeleteRequiresWhere.to_user_error(
                source,
                delete.span,
                "DELETE requires a WHERE clause; for full table replacement use INSERT OVERWRITE t SELECT * FROM t WHERE FALSE",
            )));
        }

        let publication_id = LakePublicationId::new_v7();
        let session = context.session();
        let prepared = engine
            .prepare_delete(PrepareDeleteRequest {
                publication_id,
                statement,
                source,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        let executor = DeleteWriteExecutor {
            engine,
            prepared: &prepared,
        };
        let spec = write_transaction_spec(&prepared);
        StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::DataMutation)
            .run(spec)?;
        Ok(())
    }
}
