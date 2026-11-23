// SPDX-License-Identifier: Apache-2.0

use super::{
    DmlExecutionKernel, PreparedMergeMatchedAction,
    async_preparation::describe_mutation_write_assignments, build_merge_mor_producer,
    mutation_description::admit_described_mutation_target, resolve_merge_insert_columns,
    validate_update_assignments,
};
use crate::dml::error::DmlExecutionError;
use crate::query_execution::dml::{
    mutation::{MutationDescriptionInput, OwnedMutationStatement, PreparedMutationDescription},
    mutation_match_read::{describe_mutation_match_read, mutation_match_select},
};
use novarocks_parser::ast::Relation;

pub(crate) async fn describe_merge(
    state: &DmlExecutionKernel,
    input: MutationDescriptionInput,
) -> Result<PreparedMutationDescription, DmlExecutionError> {
    let OwnedMutationStatement::Merge(statement) = &input.statement else {
        return Err("MERGE description requires the original MERGE statement".into());
    };
    let target = admit_described_mutation_target(state, &input).await?;
    if let Some(clause) = &statement.matched
        && let PreparedMergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target.columns)
            .map_err(DmlExecutionError::Analyze)?;
    }
    let insert_columns = statement
        .not_matched
        .as_ref()
        .map(|clause| resolve_merge_insert_columns(&clause.action, &target.columns))
        .transpose()?;
    let (mut query, kind, _) = build_merge_mor_producer(
        &target.target,
        statement,
        input.request.session().current_catalog(),
        &target.columns,
        insert_columns.as_deref(),
        &target.reference,
    )?;
    let Relation::Join(join) = mutation_match_select(&mut query)? else {
        return Err("generated MERGE lost its match target join".into());
    };
    let overlay = describe_mutation_match_read(
        &target.target,
        join.right.as_mut(),
        true,
        &target.signed,
        target.binding.metadata(),
        target.binding.planning_pin(),
    )?;
    let assignments = describe_mutation_write_assignments(
        &query,
        &kind,
        target.signed.match_contract(),
        &input.control,
    )
    .map_err(DmlExecutionError::from_compile)?;
    Ok(PreparedMutationDescription {
        query,
        assignments,
        overlay,
    })
}
