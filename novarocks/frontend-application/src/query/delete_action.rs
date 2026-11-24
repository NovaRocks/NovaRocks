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

//! DELETE retains the original typed statement and signed Provider facts.

use super::write_action::{self, WriteCompletion, WriteStatementFamily, WriteStatementStart};
use super::*;
mod choice;
mod metadata_commit;
use crate::connector::write_target::ConnectorWriteTargetBinding;
use crate::dml::attempt::DmlPublicationAttempt;
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    dml::delete::{self, DeleteStatementInput, OwnedDeleteStatement, PreparedDeleteWrite},
    logical_execution::InitialComputationReservation,
    maintenance::io_admission::MetadataIoAdmission,
    write_session::WriteBeginReceiving,
};
use novarocks_query_application::session_control::GovernedStatementAction;
use novarocks_spi::connector::ConnectorRequestContext;

struct DeleteFamily;

impl WriteStatementFamily for DeleteFamily {
    type Input = PreparationInput;
    type Prepared = PreparedDeleteWrite;

    fn completion(input: &PreparationInput) -> WriteCompletion {
        completion_for_statement(&input.statement)
    }

    async fn prepare(
        input: Arc<PreparationInput>,
    ) -> Result<Arc<PreparedDeleteWrite>, crate::dml::error::DmlExecutionError> {
        crate::query_execution::dml::delete::prepare_delete_write_async(
            &input.kernel,
            &input.statement,
            &input.target,
            &input.reference,
            input.planning_pin.clone(),
            input.retained_target.clone(),
            input.execution.clone(),
            &input.context,
            input.operation_id,
            Arc::clone(&input.begin_receiving),
        )
        .await
        .map(Arc::new)
    }
    async fn compile(
        prepared: Arc<PreparedDeleteWrite>,
        source: Arc<FrontendCompletionCustody>,
        options: QueryOptions,
    ) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
        prepared
            .prepare_native_assembly_for_execution_async(&source, Some(options))
            .await
    }
}

fn completion_for_statement(statement: &OwnedDeleteStatement) -> WriteCompletion {
    match statement {
        OwnedDeleteStatement::Predicate(_) => WriteCompletion::Counted,
        #[cfg(novarocks_dev_controls)]
        OwnedDeleteStatement::TrinoEquality { .. } => WriteCompletion::Empty,
    }
}

struct PreparationInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    statement: OwnedDeleteStatement,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    context: ConnectorRequestContext,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    retained_target: Option<ConnectorWriteTargetBinding>,
    begin_receiving: Arc<WriteBeginReceiving>,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
}
pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: DeleteStatementInput,
) -> Result<
    Box<dyn GovernedStatementAction>,
    crate::catalog_application::resolver::TargetResolutionError,
> {
    let root_capacity = input
        .context
        .execution()
        .result_capacity()
        .ok_or_else(|| "DELETE has no original Root result capacity".to_owned())?;
    let root = root_capacity.root_scope().clone();
    let (target, reference, planning_pin) = match &input.statement {
        #[cfg(novarocks_dev_controls)]
        OwnedDeleteStatement::TrinoEquality { resolved, .. } => (
            resolved.target.clone(),
            "main".to_owned(),
            resolved.planning.clone(),
        ),
        original => {
            let (target, reference) = delete::resolve_target(
                &kernel,
                original,
                input.context.session().current_catalog(),
                input.context.session().current_database(),
            )?;
            let planning = crate::connector::acquire_metadata_planning_pin(
                kernel.connector_control().as_ref(),
                &target.catalog,
            )?;
            (target, reference, planning)
        }
    };
    let stack = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(planning_pin.control_runtime_id())
        .map_err(|e| e.to_string())?;
    let profile = stack.session().io_profile().map_err(|e| e.to_string())?;
    drop(stack);
    // Use the existing Provider profile for this source metadata backing.
    // Retained payload and Root admission keep their original owners.
    let source_bytes = profile.geometry().metadata_bytes().get();
    let io =
        MetadataIoAdmission::new(root.clone(), profile, source_bytes).map_err(|e| e.to_string())?;
    #[cfg(novarocks_dev_controls)]
    if matches!(&input.statement, OwnedDeleteStatement::TrinoEquality { .. }) {
        return prepare_row(kernel, input, target, reference, planning_pin, io, None);
    }
    let context = kernel
        .connector_request_context_for_execution(
            Some(&input.query_options),
            input.context.execution(),
        )?
        .with_io_admission(io.clone());
    #[cfg(novarocks_dev_controls)]
    if let Some(nomination) = input.context.execution().test_statement_nomination() {
        let installed = context.request_scope_extension_or_insert_with(|| (**nomination).clone());
        if installed.as_ref() != nomination.as_ref() {
            return Err("DELETE could not retain the original statement nomination"
                .to_owned()
                .into());
        }
    }
    Ok(choice::prepare(choice::ChoiceStart {
        kernel,
        input,
        target,
        reference,
        planning: planning_pin,
        io,
        context,
    }))
}

fn prepare_row(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: DeleteStatementInput,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    io: Arc<MetadataIoAdmission>,
    retained_target: Option<ConnectorWriteTargetBinding>,
) -> Result<
    Box<dyn GovernedStatementAction>,
    crate::catalog_application::resolver::TargetResolutionError,
> {
    let root = input
        .context
        .execution()
        .result_capacity()
        .ok_or_else(|| "DELETE has no original Root result capacity".to_owned())?
        .root_scope()
        .clone();
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
        novarocks_spi::connector::LakePublicationFamily::DataMutation,
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
        statement: input.statement,
        target,
        reference,
        context: context.clone(),
        execution: input.context.execution().clone(),
        planning_pin,
        retained_target,
        begin_receiving: Arc::clone(&begin_receiving),
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId::from(publication_id),
    });
    Ok(write_action::prepare::<DeleteFamily>(WriteStatementStart {
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

#[cfg(test)]
mod completion_tests {
    use super::*;
    #[test]
    fn predicate_delete_selects_counted_completion() {
        use novarocks_parser::{ast::TrinoStatement, parser};
        let sql = "DELETE FROM t WHERE id = 2";
        let TrinoStatement::Delete(statement) = parser::parse_statement(sql).unwrap() else {
            panic!("expected DELETE");
        };
        let owned = crate::dml::prepare_trino_delete_shape(statement, sql).unwrap();
        assert_eq!(completion_for_statement(&owned), WriteCompletion::Counted);
    }
}
