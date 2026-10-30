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

//! INSERT retains its original statement preparation and compilation authors.

use super::write_action::{self, WriteStatementFamily, WriteStatementStart};
use super::*;
use crate::dml::attempt::DmlPublicationAttempt;
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    dml::{
        iceberg_writer::{self, PreparedIcebergWrite},
        insert::InsertStatementInput,
    },
    logical_execution::InitialComputationReservation,
    maintenance::io_admission::MetadataIoAdmission,
    write_session::WriteBeginReceiving,
};
use novarocks_query_application::session_control::GovernedStatementAction;
use novarocks_spi::connector::ConnectorRequestContext;

struct InsertFamily;

impl WriteStatementFamily for InsertFamily {
    type Input = PreparationInput;
    type Prepared = PreparedIcebergWrite;

    async fn prepare(
        input: Arc<PreparationInput>,
    ) -> Result<Arc<PreparedIcebergWrite>, crate::dml::error::DmlExecutionError> {
        use novarocks_spi::connector::{
            ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        };
        let context =
            crate::connector::context_for_planning_lease(&input.lease, input.context.clone())?;
        let table = crate::connector::schema_creation::load_frontend_table_async(
            input.lease.binding().metadata().as_ref(),
            ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: input.lease.binding().descriptor().instance_id.clone(),
                    namespace: Arc::from(input.target.namespace.as_str()),
                    table: Arc::from(input.target.table.as_str()),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_lease_and_context_async(
            input.kernel.mv_storage_observation().as_ref(), &input.lease,
            input.kernel.mv_readiness().as_ref(), &input.target,
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert, context.clone(),
        ).await?;
        let resolved = crate::connector::backend::ResolvedTable {
            columns: crate::query_execution::dml::insert::insert_columns_from_connector_metadata(
                &table,
            ),
        };
        let mode = match input.command.overwrite_mode {
            crate::query_execution::dml::insert::InsertOverwriteMode::Append => {
                iceberg_writer::IcebergWriteMode::Append
            }
            crate::query_execution::dml::insert::InsertOverwriteMode::FullTable => {
                iceberg_writer::IcebergWriteMode::FullTableOverwrite
            }
            crate::query_execution::dml::insert::InsertOverwriteMode::DynamicPartitions => {
                iceberg_writer::IcebergWriteMode::DynamicPartitionOverwrite
            }
        };
        iceberg_writer::prepare_iceberg_write_async(
            &input.kernel,
            &input.target,
            &resolved,
            &input.command.columns,
            &input.command.source,
            mode,
            &input.reference,
            input.execution.clone(),
            &context,
            input.lease.clone(),
            Arc::clone(&input.begin_receiving),
        )
        .await
        .map(Arc::new)
        .map_err(Into::into)
    }
    async fn compile(
        prepared: Arc<PreparedIcebergWrite>,
        source: Arc<FrontendCompletionCustody>,
        options: QueryOptions,
    ) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
        prepared
            .prepare_native_assembly_for_execution_async(&source, Some(options))
            .await
    }
    fn finalize(prepared: &Self::Prepared) -> Result<(), String> {
        prepared.finalize()
    }
}

struct PreparationInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    command: crate::dml::insert::command::InsertCommand,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    context: ConnectorRequestContext,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    begin_receiving: Arc<WriteBeginReceiving>,
}
pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: InsertStatementInput,
) -> Result<Box<dyn GovernedStatementAction>, String> {
    let root_capacity = input
        .context
        .execution()
        .result_capacity()
        .ok_or_else(|| "INSERT has no original Root result capacity".to_owned())?;
    let root = root_capacity.root_scope().clone();
    let command = input.command;
    let name = command.target.clone();
    let reference = match input.reference {
        crate::dml::insert::InsertTargetRef::Main => "main".to_owned(),
        crate::dml::insert::InsertTargetRef::Branch(name) => name,
        crate::dml::insert::InsertTargetRef::Tag(_) => {
            unreachable!("tag rejected by original author")
        }
    };
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        &kernel,
        &novarocks_sql::semantic::ObjectName { parts: name.parts },
        input.context.session().current_catalog(),
        input.context.session().current_database(),
    )?;
    let lease = crate::connector::acquire_metadata_planning_lease(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )?;
    let stack = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(lease.control_runtime_id())
        .map_err(|e| e.to_string())?;
    let profile = stack.session().io_profile().map_err(|e| e.to_string())?;
    drop(stack);
    // Use the existing Provider profile for this source metadata backing.
    // Retained payload and Root admission keep their original owners.
    let source_bytes = profile.geometry().metadata_bytes().get();
    let io =
        MetadataIoAdmission::new(root.clone(), profile, source_bytes).map_err(|e| e.to_string())?;
    let initial = InitialComputationReservation::new(
        input
            .launcher
            .reserve_identity()
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let credentials = initial.credentials();
    let context = initial
        .connector_request_context(
            kernel.connector_request_context_for_execution(
                Some(&input.query_options),
                input.context.execution(),
            )?,
            #[cfg(novarocks_dev_controls)]
            input.context.execution().test_statement_nomination(),
        )
        .map_err(|e| e.to_string())?
        .with_io_admission(io.clone());
    let session_receiving = Arc::new(Mutex::new(None));
    let begin_receiving = WriteBeginReceiving::new(Arc::clone(&session_receiving));
    let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
    let attempt = DmlPublicationAttempt::new(
        publication_id,
        novarocks_spi::connector::LakePublicationFamily::Write,
        novarocks_spi::connector::LakePublicationTarget::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            Some(target.table.clone()),
            (reference != "main").then(|| reference.clone()),
        )
        .map_err(|e| e.to_string())?,
        None,
    );
    let original = Arc::new(PreparationInput {
        kernel: kernel.clone(),
        command,
        target,
        reference,
        context: context.clone(),
        execution: input.context.execution().clone(),
        lease,
        begin_receiving: Arc::clone(&begin_receiving),
    });
    Ok(write_action::prepare::<InsertFamily>(WriteStatementStart {
        kernel,
        root,
        io,
        initial,
        credentials,
        input: original,
        context,
        session_receiving,
        begin_receiving,
        sql: input.sql,
        query_options: input.query_options,
        attempt,
        launcher: input.launcher,
        #[cfg(novarocks_dev_controls)]
        observation: input.observation,
    }))
}
