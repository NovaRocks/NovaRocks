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

//! Metadata DELETE admission on the original table observation.

use std::sync::Arc;

use novarocks_parser::{ast, build};
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_spi::connector::read_stack::ConnectorDeleteConditionOutcome;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorDeleteCondition, ConnectorProcessLocalPayload,
    ConnectorRequestContext, ConnectorRowMutationStrategy,
};
use novarocks_sql::compiler::{
    DeleteAbsorption, RootDistributionRequirement, SqlCompileIntent, SqlDmlCatalogPreparation,
    SqlSessionContext,
};

use crate::catalog_application::query_bindings::{
    QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::catalog_application::query_catalog::connector_table_materialization_from_metadata;
use crate::catalog_application::query_materializer::{
    QueryLocalTableOverlay, connector_query_binding_from_materialization,
};
use crate::catalog_application::resolver::TargetBackend;
use crate::connector::write_target::ConnectorWriteTargetBinding;
use crate::dml::error::DmlExecutionError;
use crate::query_execution::compiler::{self, DmlQueryExecutionKernel};
use crate::query_execution::completion_facts::{
    FrontendCompletionCustody, StatementFactScope, install_frontend_fact_source,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::provider_read_facts;

pub(crate) enum DeletePath {
    Empty,
    Metadata {
        condition: ConnectorDeleteCondition,
        payload: Option<ConnectorProcessLocalPayload>,
    },
    RowLevel,
}

pub(crate) struct DeleteDecision {
    pub(crate) path: DeletePath,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) row_delete_strategy: Option<ConnectorRowMutationStrategy>,
    pub(crate) constrained_columns: Vec<String>,
}

impl DeletePath {
    fn all_rows() -> Self {
        Self::Metadata {
            condition: ConnectorDeleteCondition::AllRows,
            payload: None,
        }
    }
}

pub(crate) async fn select_delete_path(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    planning: ConnectorControlPlanningPin,
    statement: &super::PreparedPredicateDelete,
    context: &RequestContext,
    connector: &ConnectorRequestContext,
    facts: &FrontendCompletionCustody,
) -> Result<(ConnectorWriteTargetBinding, DeleteDecision), DmlExecutionError> {
    let original = super::standard::load_delete_target(state, target, planning, connector).await?;
    let decision = prove_delete_path(
        state, target, &original, statement, context, connector, facts,
    )
    .await?;
    Ok((original, decision))
}

async fn prove_delete_path(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    original: &ConnectorWriteTargetBinding,
    statement: &super::PreparedPredicateDelete,
    context: &RequestContext,
    connector: &ConnectorRequestContext,
    facts: &FrontendCompletionCustody,
) -> Result<DeleteDecision, DmlExecutionError> {
    let observation = provider_read_facts::observe_delete_target(
        state.typed_connector_control(),
        original,
        connector,
    )
    .map_err(project_provider_error)?
    .ok_or_else(|| "Iceberg DELETE target has no original observation".to_owned())?;
    let mut decision = DeleteDecision {
        path: DeletePath::RowLevel,
        snapshot_id: observation.snapshot_id,
        row_delete_strategy: observation.row_delete_strategy,
        constrained_columns: Vec::new(),
    };
    if statement.target.branch_is_explicit() {
        return Ok(decision);
    }
    let Some(predicate) = statement.selection.as_ref() else {
        decision.path = DeletePath::all_rows();
        return Ok(decision);
    };

    let execution = context.execution();
    let capacity = execution
        .result_capacity()
        .cloned()
        .ok_or_else(|| "DELETE predicate probe has no original result capacity".to_owned())?;
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let overlay = original_target_overlay(target, original)?;
    let control = crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let scope = StatementFactScope::binding_only(
        Arc::clone(&bindings),
        connector.clone(),
        context.session().current_catalog(),
        capacity.scope(),
        capacity.clone(),
    )
    .map_err(|error| error.to_string())?
    .with_query_local_overlays(vec![overlay])
    .with_snapshot_planning_owned(control.clone(), execution.cancellation().clone());
    let session = compiler::typed_connector_session()?;
    install_frontend_fact_source(
        facts,
        state.completion_fact_owners()?,
        scope,
        session.clone(),
        Arc::clone(execution.session_property_inputs()),
    )?;

    let query = predicate_query(target, Some(predicate.clone()))?;
    let qualifications = compiler::observe_dml_catalog_qualifications(
        &query,
        context.session().current_catalog(),
        context.session().current_database(),
        &control,
        &bindings,
        state.catalog_application().map(Arc::as_ref),
    )
    .map_err(DmlExecutionError::from_compile)?;
    let mut preparation = SqlDmlCatalogPreparation::try_new_for_intent_with_qualifications(
        execution.sql_statement_input(Box::new(query), false),
        SqlSessionContext {
            sql_semantics: execution.sql_semantics().clone(),
            current_catalog: context.session().current_catalog().map(str::to_owned),
            current_database: context.session().current_database().to_owned(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        control.clone(),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        qualifications,
    )
    .map_err(DmlExecutionError::from_compile)?;
    while !preparation.needs().is_empty() {
        let response = facts
            .resolve_catalog_relations(preparation.needs())
            .await
            .map_err(DmlExecutionError::from_compile)?;
        preparation = preparation
            .continue_catalog(response.into_boxed_slice())
            .map_err(DmlExecutionError::from_compile)?;
    }
    bindings.seal_catalog_observations().await;
    let analyzed = facts
        .analyze_dml_source(
            preparation,
            DmlQueryExecutionKernel::function_catalog(state),
            Arc::clone(DmlQueryExecutionKernel::sealed_functions(state)),
            DmlQueryExecutionKernel::constant_policy(state),
        )
        .await
        .map_err(DmlExecutionError::from_compile)?;
    let probe = analyzed
        .probe_delete_absorption(&control)
        .map_err(DmlExecutionError::from_compile)?;
    if decision.snapshot_id.is_none() {
        decision.path = DeletePath::Empty;
        return Ok(decision);
    }
    decision.path = match probe {
        DeleteAbsorption::Empty => DeletePath::Empty,
        DeleteAbsorption::AllRows => DeletePath::all_rows(),
        DeleteAbsorption::RowLevel => DeletePath::RowLevel,
        DeleteAbsorption::Filter(need) => {
            let answer = provider_read_facts::probe_delete_filter(
                &need,
                state.typed_connector_control(),
                &bindings,
                connector,
                original.metadata(),
                &session,
            )
            .await
            .map_err(project_provider_error)?;
            match answer {
                ConnectorDeleteConditionOutcome::Empty => DeletePath::Empty,
                ConnectorDeleteConditionOutcome::Condition { payload, digest } => {
                    for ordinal in need.filter().summary().columns() {
                        let column = need.columns().get(*ordinal as usize).ok_or_else(|| {
                            "DELETE proof names a column outside the original projection".to_owned()
                        })?;
                        decision.constrained_columns.push(column.name().to_owned());
                    }
                    DeletePath::Metadata {
                        condition: ConnectorDeleteCondition::SealedPredicate { digest },
                        payload: Some(payload),
                    }
                }
                ConnectorDeleteConditionOutcome::Refused { .. } => DeletePath::RowLevel,
            }
        }
    };
    Ok(decision)
}

pub(crate) fn original_target_overlay(
    target: &TargetBackend,
    original: &ConnectorWriteTargetBinding,
) -> Result<QueryLocalTableOverlay, String> {
    original_target_overlay_with_read(target, original, None)
}

pub(crate) fn original_target_explain_overlay(
    target: &TargetBackend,
    original: &ConnectorWriteTargetBinding,
    reference: Option<String>,
) -> Result<QueryLocalTableOverlay, String> {
    original_target_overlay_with_read(target, original, Some(reference))
}

fn original_target_overlay_with_read(
    target: &TargetBackend,
    original: &ConnectorWriteTargetBinding,
    explain_reference: Option<Option<String>>,
) -> Result<QueryLocalTableOverlay, String> {
    let planning = original.planning_pin().clone();
    let materialization = connector_table_materialization_from_metadata(
        original.metadata().clone(),
        planning.clone(),
    )?;
    let identity = target.clone();
    Ok(QueryLocalTableOverlay::for_catalog(
        &target.catalog,
        &target.namespace,
        &target.table,
        QueryTableBindingKey::strict_base(&target.catalog, &target.namespace, &target.table),
        move |id| {
            let mut binding = connector_query_binding_from_materialization(
                materialization.clone(),
                &identity.catalog,
                &identity.namespace,
                &identity.table,
                id,
            )?;
            if let Some(reference) = &explain_reference {
                binding.admission = QueryTableBindingAdmission::OriginalTableObservation(
                    planning.clone(),
                    reference.clone(),
                );
                // The original Current pin cannot describe the named-branch read.
                binding.statistics_pin = None;
            }
            Ok(binding)
        },
    ))
}

pub(crate) fn predicate_query(
    target: &TargetBackend,
    predicate: Option<ast::Expr>,
) -> Result<ast::Query, String> {
    let name = build::object_name(
        [&target.catalog, &target.namespace, &target.table]
            .into_iter()
            .map(|part| build::ident(part, true))
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    let select = build::select(
        vec![build::item(build::integer(1), None)],
        Some(build::table(name, None, None)),
        predicate,
        None,
    )
    .map_err(|error| error.to_string())?;
    Ok(build::query(ast::SetExpr::Select(ast::AstBox::new(select))))
}

fn project_provider_error(error: provider_read_facts::ProviderReadError) -> DmlExecutionError {
    match error.into_original_cause() {
        Ok(cause) => DmlExecutionError::Compilation(cause),
        Err(error) => error.to_string().into(),
    }
}
