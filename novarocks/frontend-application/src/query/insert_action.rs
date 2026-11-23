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

use super::write_action::{self, WriteCompletion, WriteStatementFamily, WriteStatementStart};
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

    fn completion(_input: &PreparationInput) -> WriteCompletion {
        WriteCompletion::Counted
    }

    async fn prepare(
        input: Arc<PreparationInput>,
    ) -> Result<Arc<PreparedIcebergWrite>, crate::dml::error::DmlExecutionError> {
        use novarocks_spi::connector::{
            ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        };
        let context =
            crate::connector::context_for_planning_pin(&input.planning_pin, input.context.clone())?;
        let table = crate::connector::schema_creation::load_frontend_table_async(
            input.planning_pin.binding().metadata().as_ref(),
            ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: input
                        .planning_pin
                        .binding()
                        .descriptor()
                        .instance_id
                        .clone(),
                    namespace: Arc::from(input.target.namespace.as_str()),
                    table: Arc::from(input.target.table.as_str()),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context_async(
            input.kernel.mv_storage_observation().as_ref(), &input.planning_pin,
            input.kernel.mv_readiness().as_ref(), &input.target,
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert, context.clone(),
        ).await?;
        let resolved = crate::connector::backend::ResolvedTable {
            columns: crate::query_execution::dml::insert::insert_columns_from_connector_metadata(
                &table,
            )?,
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
            input.planning_pin.clone(),
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

/// Validate the ordinary connector write declaration without beginning a write session.
pub(crate) async fn describe(
    kernel: &crate::query_execution::kernels::DmlExecutionKernel,
    input: crate::query_execution::dml::insert::InsertDescriptionInput,
) -> Result<
    crate::query_execution::dml::insert::PreparedInsertDescription,
    novarocks_query_application::session_error::QueryServiceError,
> {
    use crate::query_execution::dml::insert::{InsertOverwriteMode, PreparedInsertDescription};
    use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        ConnectorWriteInputRequest, ConnectorWriteIntent,
    };
    use novarocks_sql::analyze_error::AnalyzeError;
    use novarocks_sql::compiler::{SqlWriteAssignmentSite, SqlWriteAssignments};

    let session = input.request.session();
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        kernel,
        &novarocks_sql::semantic::ObjectName {
            parts: input.command.target.parts,
        },
        session.current_catalog(),
        session.current_database(),
    )
    .map_err(crate::catalog_application::resolver::TargetResolutionError::into_service)?;
    let internal = |message| QueryServiceError::new(QueryServiceErrorKind::Internal, message);
    let pin = crate::connector::acquire_metadata_planning_pin(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )
    .map_err(|cause| internal(cause.to_string()))?;
    let context =
        crate::connector::context_for_planning_pin(&pin, input.connector).map_err(internal)?;
    let table = crate::connector::schema_creation::load_frontend_table_async(
        pin.binding().metadata().as_ref(),
        ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: pin.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        },
    )
    .await
    .map_err(|cause| {
        let missing = cause.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound;
        let original = novarocks_failure_contract::SharedFailureCause::new(cause);
        if missing {
            QueryServiceError::from_user_error_with_cause(
                AnalyzeError::unknown_table(
                    format!(
                        "Table '{}.{}.{}' does not exist",
                        target.catalog, target.namespace, target.table
                    ),
                    input.target_span,
                )
                .to_user_error(Some(&input.source)),
                original,
            )
        } else {
            QueryServiceError::from_failure_cause(original)
        }
    })?;
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context_async(
        kernel.mv_storage_observation().as_ref(), &pin, kernel.mv_readiness().as_ref(), &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert, context.clone(),
    )
    .await
    .map_err(|cause| internal(cause.to_string()))?;
    let resolved = crate::connector::backend::ResolvedTable {
        columns: crate::query_execution::dml::insert::insert_columns_from_connector_metadata(
            &table,
        )
        .map_err(internal)?,
    };
    if let Some(name) = input.command.columns.iter().find(|name| {
        !resolved
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
    }) {
        return Err(QueryServiceError::from_user_error(
            AnalyzeError::unknown_column(
                format!("INSERT target has no column '{name}'"),
                input.command.source.span,
            )
            .to_user_error(Some(&input.source)),
        ));
    }
    let write_target = crate::connector::write_target::ConnectorWriteTargetBinding::new(table, pin);
    let write_lease = write_target.derive_write_lease().map_err(internal)?;
    let target_ref = match &input.reference {
        crate::dml::insert::InsertTargetRef::Main => "main",
        crate::dml::insert::InsertTargetRef::Branch(name) => name,
    };
    let intent = match input.command.overwrite_mode {
        InsertOverwriteMode::Append => ConnectorWriteIntent::Append,
        InsertOverwriteMode::FullTable => ConnectorWriteIntent::Overwrite,
        InsertOverwriteMode::DynamicPartitions => ConnectorWriteIntent::PartitionOverwrite,
    };
    let (query, constants, fields) = iceberg_writer::build_iceberg_write_plan(
        &write_lease,
        target_ref,
        intent,
        &context,
        &resolved,
        &input.command.columns,
        &input.command.source,
        write_target.metadata(),
        kernel.constant_policy(),
        &input.control,
    )
    .map_err(internal)?;
    let ConnectorWriteInputRequest::Data { fields } = fields else {
        return Err(internal(
            "INSERT description expected its ordinary data field declaration".to_owned(),
        ));
    };
    let sites = fields
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let binding = field.execution_type().ok_or_else(|| {
                internal("INSERT description has no exact target declaration".to_owned())
            })?;
            SqlWriteAssignmentSite::try_new(
                ordinal,
                binding.clone(),
                field.storage_type().cloned(),
                &input.control,
            )
            .map_err(|cause| internal(cause.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let assignments = SqlWriteAssignments::try_new(sites.into_boxed_slice(), &input.control)
        .map_err(|cause| internal(cause.to_string()))?;
    Ok(PreparedInsertDescription {
        query,
        assignments,
        constants,
    })
}

struct PreparationInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    command: crate::dml::insert::command::InsertCommand,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    context: ConnectorRequestContext,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    begin_receiving: Arc<WriteBeginReceiving>,
}
pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: InsertStatementInput,
) -> Result<
    Box<dyn GovernedStatementAction>,
    crate::catalog_application::resolver::TargetResolutionError,
> {
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
    };
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        &kernel,
        &novarocks_sql::semantic::ObjectName { parts: name.parts },
        input.context.session().current_catalog(),
        input.context.session().current_database(),
    )?;
    let planning_pin = crate::connector::acquire_metadata_planning_pin(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )?;
    let stack = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(planning_pin.control_runtime_id())
        .map_err(|e| e.to_string())?;
    let profile = kernel
        .snapshot_io_profile(stack.session().io_profile().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
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
    let begin_receiving = WriteBeginReceiving::for_statement(
        Arc::clone(&session_receiving),
        Arc::clone(input.context.execution().session_property_inputs()),
    );
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
        planning_pin,
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
