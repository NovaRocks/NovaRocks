// SPDX-License-Identifier: Apache-2.0

//! Schema-only projection of the original row mutation and its signed target.

use super::{
    DmlExecutionKernel, build_update_mor_producer, merge_effect_set, validate_update_assignments,
};
use crate::catalog_application::resolver::TargetBackend;
use crate::connector::write_target::ConnectorWriteTargetBinding;
use crate::dml::error::DmlExecutionError;
use crate::query_execution::dml::{
    mutation::{MutationDescriptionInput, OwnedMutationStatement, PreparedMutationDescription},
    mutation_match_read::{describe_mutation_match_read, mutation_match_select},
};
use novarocks_query_application::session_error::QueryServiceError;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparation, ConnectorRowMutationStrategy, ConnectorWriteOperationId,
};
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use novarocks_types::schema::ColumnDef;

pub(super) struct DescribedMutationTarget {
    pub(super) target: TargetBackend,
    pub(super) reference: String,
    pub(super) binding: ConnectorWriteTargetBinding,
    pub(super) signed: ConnectorRowMutationPreparation,
    pub(super) columns: Vec<ColumnDef>,
}

pub(super) async fn admit_described_mutation_target(
    state: &DmlExecutionKernel,
    input: &MutationDescriptionInput,
) -> Result<DescribedMutationTarget, DmlExecutionError> {
    input
        .control
        .checkpoint(CompilePhase::Validate, 0)
        .map_err(DmlExecutionError::Control)?;
    let session = input.request.session();
    let (target, reference) = super::super::mutation::resolve_target(
        state,
        &input.statement,
        session.current_catalog(),
        session.current_database(),
    )?;
    let pin = crate::connector::acquire_metadata_planning_pin(
        state.connector_control().as_ref(),
        &target.catalog,
    )?;
    let binding = super::async_preparation::load_authorized_mutation_target(
        state,
        &input.statement,
        &target,
        pin,
        &input.connector,
    )
    .await
    .map_err(|error| {
        if let DmlExecutionError::Application(service) = &error
            && let Some(cause) = service.failure_cause()
            && cause
                .downcast_ref::<ConnectorError>()
                .is_some_and(|cause| cause.kind() == ConnectorErrorKind::NotFound)
        {
            return QueryServiceError::from_user_error_with_cause(
                AnalyzeError::unknown_table(
                    format!(
                        "Table '{}.{}.{}' does not exist",
                        target.catalog, target.namespace, target.table
                    ),
                    input.target_span,
                )
                .to_user_error(Some(&input.source)),
                cause.clone(),
            )
            .into();
        }
        error
    })?;
    input
        .control
        .checkpoint(CompilePhase::Validate, 0)
        .map_err(DmlExecutionError::Control)?;
    let (intent, operation) = match &input.statement {
        OwnedMutationStatement::Update(_) => (ConnectorRowMutationIntent::Update, "UPDATE"),
        OwnedMutationStatement::Merge(mutation) => (
            ConnectorRowMutationIntent::Merge {
                effects: merge_effect_set(mutation).effects(),
            },
            "MERGE",
        ),
    };
    let (_, signed) = binding.prepare_row_mutation(
        &reference,
        ConnectorWriteOperationId::new(),
        intent,
        input.connector.clone(),
    )?;
    if signed.strategy() != ConnectorRowMutationStrategy::MergeOnRead {
        return Err(format!(
            "{operation} cannot be served by row-mutation strategy {:?}",
            signed.strategy()
        )
        .into());
    }
    let columns = binding.dml_target_columns()?;
    input
        .control
        .checkpoint(CompilePhase::Validate, 0)
        .map_err(DmlExecutionError::Control)?;
    Ok(DescribedMutationTarget {
        target,
        reference,
        binding,
        signed,
        columns,
    })
}

pub(crate) async fn describe_update(
    state: &DmlExecutionKernel,
    input: MutationDescriptionInput,
) -> Result<PreparedMutationDescription, DmlExecutionError> {
    let OwnedMutationStatement::Update(update) = &input.statement else {
        return Err("UPDATE description requires its original UPDATE syntax".into());
    };
    let DescribedMutationTarget {
        target,
        reference,
        binding,
        signed,
        columns,
    } = admit_described_mutation_target(state, &input).await?;
    validate_update_assignments(&update.assignments, &columns)
        .map_err(DmlExecutionError::Analyze)?;
    let (mut query, kind, _) = build_update_mor_producer(
        &target,
        update,
        input.request.session().current_catalog(),
        &columns,
        &reference,
    )?;
    let from = mutation_match_select(&mut query)?;
    let factor = match from {
        novarocks_parser::ast::Relation::Join(join) => join.left.as_mut(),
        relation => relation,
    };
    let overlay = describe_mutation_match_read(
        &target,
        factor,
        true,
        &signed,
        binding.metadata(),
        binding.planning_pin(),
    )?;
    let assignments = super::async_preparation::describe_mutation_write_assignments(
        &query,
        &kind,
        signed.match_contract(),
        &input.control,
    )?;
    Ok(PreparedMutationDescription {
        query,
        overlay,
        assignments,
    })
}
