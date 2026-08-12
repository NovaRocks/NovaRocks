// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Iceberg-private adapter from one frozen rewrite cohort to the generic C1
//! distributed writer.  The only generic inputs are an opaque frozen source,
//! an exact rewrite lease, and a sealed writer registration.  File ownership,
//! Iceberg table metadata, and sink construction stay in this module.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorRequestContext, ConnectorWriteCohortId, ConnectorWriteInputShape,
};

use crate::engine::StandaloneState;
use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::engine::query_planning::write_sink::{
    admit_prepared_connector_write_target, sql_write_plan_input_for_admitted_target,
};
use crate::query_execution::distributed_rewrite::{
    ConnectorDistributedRewriteSession, frozen_rewrite_scan_physical_plan,
    plan_frozen_rewrite_connector_read,
};
use crate::query_execution::outcome::{ConnectorWriteCompletion, ConnectorWriteStagingSummary};
use crate::query_execution::request_context::QueryExecutionContext;

/// Stage one sealed frozen cohort.  The caller is responsible for recording
/// the returned completion as accepted or superseded before any aggregate
/// commit is attempted.
pub(crate) fn stage_frozen_rewrite_cohort(
    state: &Arc<StandaloneState>,
    session: &ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: &QueryExecutionContext,
    context: &ConnectorRequestContext,
) -> Result<(ConnectorWriteCompletion, ConnectorWriteStagingSummary), String> {
    let cohort = session
        .plan()
        .cohorts()
        .iter()
        .find(|candidate| candidate.cohort_id() == cohort_id)
        .ok_or_else(|| "distributed rewrite execution names an unknown cohort".to_string())?;
    let read = plan_frozen_rewrite_connector_read(
        session.lease(),
        execution.topology(),
        cohort.source(),
        cohort.scan_schema(),
        (0..cohort.scan_schema().fields().len()).collect(),
        context.clone(),
    )
    .map_err(|error| format!("plan frozen rewrite source: {error}"))?;
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let source_binding =
        crate::query_execution::distributed_rewrite::admit_frozen_rewrite_scan_binding(
            table_bindings.as_ref(),
            cohort.scan_schema(),
        )?;
    let resolver = crate::query_execution::distributed_rewrite::frozen_rewrite_read_resolver(
        source_binding,
        read,
    );
    let physical_plan = frozen_rewrite_scan_physical_plan(cohort.scan_schema(), source_binding);
    let target_binding = admit_prepared_connector_write_target(
        table_bindings.as_ref(),
        rewrite_target_identity(session, cohort_id),
        cohort.preparation().clone(),
        session.lease().planning_lease(),
    )?;
    let sink = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        rewrite_sink_mode(cohort.preparation().input())?,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;
    let registration = session
        .execution_registration(cohort_id)
        .map_err(|error| format!("register frozen rewrite cohort: {error}"))?;
    crate::engine::execute_frozen_rewrite_physical_plan_as_iceberg_staging(
        state,
        physical_plan,
        sink,
        Some(execution),
        context,
        table_bindings.as_ref(),
        &resolver,
        registration,
    )
}

fn rewrite_target_identity(
    session: &ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> crate::sql::planner::table::SqlTableIdentity {
    crate::sql::planner::table::SqlTableIdentity {
        catalog: session
            .lease()
            .binding_key()
            .instance_id
            .as_str()
            .to_string(),
        namespace: "__connector_rewrite".to_string(),
        table: format!("cohort_{}", hex::encode(cohort_id.to_bytes())),
    }
}

fn rewrite_sink_mode(
    input: &ConnectorWriteInputShape,
) -> Result<crate::sql::planner::distributed::write::contract::SqlWriteSinkMode, String> {
    use crate::sql::planner::distributed::write::contract::SqlWriteSinkMode;

    match input {
        ConnectorWriteInputShape::Data { .. } => Ok(SqlWriteSinkMode::Data),
        ConnectorWriteInputShape::RowLineage { .. } => Ok(SqlWriteSinkMode::RowLineageData),
        ConnectorWriteInputShape::PositionDelete { .. } => Ok(SqlWriteSinkMode::PositionDeletes),
        ConnectorWriteInputShape::DeletionVector { .. } => Ok(SqlWriteSinkMode::DeletionVectors),
        ConnectorWriteInputShape::EqualityDelete { .. } => Ok(SqlWriteSinkMode::EqualityDeletes),
    }
}
