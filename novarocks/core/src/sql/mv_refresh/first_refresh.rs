// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Result-free SQL physicalization for MV first refresh.
//!
//! A first refresh writes a fresh, empty staging target. This module makes the
//! physical rows needed by that append cohort explicit, so the caller can put a
//! connector writer at the native distributed root without materializing data
//! in the frontend.

use crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::mv::aggregate_state::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, AGG_STATE_PREFIX, ROW_ID_COLUMN, sanitize_state_column_name,
};
use crate::mv::model::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME;
use crate::mv::refresh::aggregate_first_refresh::{
    prepare_aggregate_first_refresh_state_sql,
    prepare_branch_union_aggregate_first_refresh_state_sqls,
};
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::mv::refresh::projection_first_refresh::{
    prepare_projection_full_read_sql, prepare_union_projection_full_read_sql,
};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::planner::logical::LogicalPlanNode;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorWriteCohortId, ConnectorWriteOperationId,
};
use std::collections::BTreeSet;

/// Value-only facts needed to reconstruct the provider-private scan binding
/// for a typed join append after the frontend has admitted its exact lease.
/// This deliberately excludes catalog entries, table handles and execution
/// services: the Core activation adapter reloads those and validates them
/// against these facts.
pub(crate) struct MvFirstRefreshLogicalContext {
    pub(crate) mv_definition: crate::mv::persistence::definition::StoredMvDefinition,
    pub(crate) canonical_select_query: sqlparser::ast::Query,
    pub(crate) base_refs: Vec<novarocks_catalog::identifier::TableIdentity>,
    pub(crate) pin: RefreshSnapshotPin,
    pub(crate) previous_snapshot_ids: std::collections::BTreeMap<String, i64>,
    pub(crate) previous_table_uuids: std::collections::BTreeMap<String, String>,
    pub(crate) target_table_uuid: String,
    pub(crate) affected_partitions: crate::mv::model::AffectedTargetPartitions,
}

/// Immutable SQL artifact for a distributed first-refresh write.
///
/// `root_hash_column` is the target contract's hidden apply key. The native
/// planner must derive its actual writer fanout from the admitted topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvFirstRefreshPhysicalSql {
    sql: String,
    root_hash_column: String,
}

/// Canonical typed append projection for a join first refresh.  It is a
/// planning value, not a backend-local executable: code generation, fragment
/// preparation and connector handle attachment remain deferred until the
/// admitted native execution boundary.
pub(crate) struct MvFirstRefreshLogicalArtifact {
    plan: LogicalPlanNode,
    factory: ColumnRefFactory,
    root_hash_column: String,
    context: MvFirstRefreshLogicalContext,
}

impl MvFirstRefreshLogicalArtifact {
    pub(crate) fn from_join_append(
        append: crate::mv::refresh::join_first_refresh::JoinFirstRefreshAppendLogicalPlan,
        context: MvFirstRefreshLogicalContext,
    ) -> Self {
        Self {
            plan: append.plan,
            factory: append.factory,
            root_hash_column: crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME
                .to_string(),
            context,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        LogicalPlanNode,
        ColumnRefFactory,
        MvFirstRefreshLogicalContext,
    ) {
        (self.plan, self.factory, self.context)
    }
}

pub(crate) enum MvFirstRefreshExecutionArtifact {
    Sql(MvFirstRefreshPhysicalSql),
    Logical(MvFirstRefreshLogicalArtifact),
}

impl MvFirstRefreshExecutionArtifact {
    fn root_hash_column(&self) -> &str {
        match self {
            Self::Sql(sql) => sql.root_hash_column(),
            Self::Logical(logical) => &logical.root_hash_column,
        }
    }
}

impl MvFirstRefreshPhysicalSql {
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        &self.root_hash_column
    }
}

/// Validated logical shape of a first-refresh append.  All variants have one
/// empty target and therefore one sealed primary append cohort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvFirstRefreshShape {
    Projection,
    UnionProjection,
    Aggregate,
    FanInAggregate,
    BranchUnionAggregate,
    Join,
    JoinAggregate,
    ComposedAggregate,
}

/// The provider commit semantics for a fresh MV staging artifact. A policy
/// rebuild writes the same SQL-shaped rows as a first refresh, but its staging
/// branch starts from the published target and therefore must overwrite it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvStagedRefreshWriteMode {
    Append,
    FullOverwrite,
}

/// Target facts frozen before a first-refresh writer is admitted.  It carries
/// Arrow schema and field identities, never an Iceberg table/client or a
/// provider decoder.
#[derive(Clone)]
pub(crate) struct MvFirstRefreshTargetContract {
    schema: SchemaRef,
    field_ids: Vec<i32>,
    partition_spec_id: i32,
    hidden_hash_key: String,
}

impl MvFirstRefreshTargetContract {
    pub(crate) fn try_new(
        schema: SchemaRef,
        field_ids: Vec<i32>,
        partition_spec_id: i32,
        hidden_hash_key: String,
    ) -> Result<Self, String> {
        if schema.fields().is_empty()
            || schema.fields().len() != field_ids.len()
            || field_ids.iter().any(|field_id| *field_id <= 0)
            || field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len()
            || partition_spec_id < 0
            || hidden_hash_key.is_empty()
        {
            return Err("invalid MV first-refresh target physical contract".to_string());
        }
        Ok(Self {
            schema,
            field_ids,
            partition_spec_id,
            hidden_hash_key,
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub(crate) const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub(crate) fn hidden_hash_key(&self) -> &str {
        &self.hidden_hash_key
    }

    /// Verify provider-observed target facts before a deferred writer is
    /// activated. This is value-only so the SQL contract retains neither a
    /// catalog handle nor a provider codec.
    pub(crate) fn validate_observed(
        &self,
        schema: &Schema,
        field_ids: &[i32],
        partition_spec_id: i32,
    ) -> Result<(), String> {
        if schema != self.schema.as_ref()
            || field_ids != self.field_ids
            || partition_spec_id != self.partition_spec_id
        {
            return Err(
                "MV first-refresh target physical contract drifted after preparation".to_string(),
            );
        }
        if !self
            .schema
            .fields()
            .iter()
            .any(|field| field.name() == &self.hidden_hash_key)
        {
            return Err(
                "MV first-refresh target contract has no hidden hash key field".to_string(),
            );
        }
        Ok(())
    }
}

/// SQL/application handoff before fragment preparation.  The source SQL and
/// target contract are frozen, but topology, writer handles, provider service
/// construction and native fragment preparation are intentionally deferred.
#[derive(Clone)]
pub(crate) struct MvFirstRefreshWriteRequest {
    canonical_select_sql: String,
    shape: MvFirstRefreshShape,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    staging_branch: String,
    current_catalog: Option<String>,
    current_database: String,
    expected_target_snapshot_id: Option<i64>,
    target_table: ConnectorTableHandle,
    target_contract: MvFirstRefreshTargetContract,
    observed_binding: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    connector_context: ConnectorRequestContext,
}

impl MvFirstRefreshWriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        canonical_select_sql: String,
        shape: MvFirstRefreshShape,
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
        current_catalog: Option<String>,
        current_database: String,
        expected_target_snapshot_id: Option<i64>,
        target_table: ConnectorTableHandle,
        target_contract: MvFirstRefreshTargetContract,
        observed_binding: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        connector_context: ConnectorRequestContext,
    ) -> Result<Self, String> {
        if canonical_select_sql.trim().is_empty()
            || target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
            || current_database.is_empty()
            || target_table.owner() != &observed_binding.instance_id
        {
            return Err("invalid MV first-refresh write request identity".to_string());
        }
        Ok(Self {
            canonical_select_sql,
            shape,
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            current_catalog,
            current_database,
            expected_target_snapshot_id,
            target_table,
            target_contract,
            observed_binding,
            operation_id,
            connector_context,
        })
    }

    pub(crate) fn canonical_select_sql(&self) -> &str {
        &self.canonical_select_sql
    }

    pub(crate) const fn shape(&self) -> MvFirstRefreshShape {
        self.shape
    }

    pub(crate) fn target_catalog(&self) -> &str {
        &self.target_catalog
    }

    pub(crate) fn target_namespace(&self) -> &str {
        &self.target_namespace
    }

    pub(crate) fn target_name(&self) -> &str {
        &self.target_name
    }

    pub(crate) fn staging_branch(&self) -> &str {
        &self.staging_branch
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub(crate) fn current_database(&self) -> &str {
        &self.current_database
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.expected_target_snapshot_id
    }

    pub(crate) fn target_table(&self) -> &ConnectorTableHandle {
        &self.target_table
    }

    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        &self.target_contract
    }

    pub(crate) fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        &self.observed_binding
    }

    pub(crate) const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub(crate) fn connector_context(&self) -> &ConnectorRequestContext {
        &self.connector_context
    }
}

/// Side-effect-free SQL preparation for a first-refresh write.  Its fields
/// remain private so an application owner can inspect facts but cannot obtain
/// a local program, catalog object, record batch or provider payload.
pub struct PreparedMvFirstRefreshWrite {
    request: MvFirstRefreshWriteRequest,
    artifact: MvFirstRefreshExecutionArtifact,
    primary_cohort: ConnectorWriteCohortId,
    write_mode: MvStagedRefreshWriteMode,
    provenance_properties: std::collections::BTreeMap<String, String>,
}

impl PreparedMvFirstRefreshWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id()
    }

    pub const fn primary_cohort(&self) -> ConnectorWriteCohortId {
        self.primary_cohort
    }

    pub fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        self.request.observed_binding()
    }

    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        self.request.target_contract()
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        self.artifact.root_hash_column()
    }

    pub(crate) fn connector_context(&self) -> &ConnectorRequestContext {
        self.request.connector_context()
    }

    pub(crate) fn target_catalog(&self) -> &str {
        self.request.target_catalog()
    }

    pub(crate) fn target_namespace(&self) -> &str {
        self.request.target_namespace()
    }

    pub(crate) fn target_name(&self) -> &str {
        self.request.target_name()
    }

    pub(crate) fn staging_branch(&self) -> &str {
        self.request.staging_branch()
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.request.current_catalog()
    }

    pub(crate) fn current_database(&self) -> &str {
        self.request.current_database()
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.request.expected_target_snapshot_id()
    }

    pub(crate) const fn write_mode(&self) -> MvStagedRefreshWriteMode {
        self.write_mode
    }

    /// Reclassify a validated full-read artifact as a staging overwrite. This
    /// is only valid for SQL preparation before the artifact is activated or
    /// bound to an execution attempt.
    pub(crate) fn into_full_overwrite(mut self) -> Self {
        self.write_mode = MvStagedRefreshWriteMode::FullOverwrite;
        self
    }

    pub(crate) fn with_provenance_properties(
        mut self,
        provenance_properties: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.provenance_properties = provenance_properties;
        self
    }

    pub(crate) fn provenance_properties(&self) -> &std::collections::BTreeMap<String, String> {
        &self.provenance_properties
    }

    pub(crate) fn into_execution_artifact(self) -> MvFirstRefreshExecutionArtifact {
        self.artifact
    }

    /// Returns the frozen SQL physicalization for the generic distributed
    /// writer binder. Typed join artifacts deliberately return `None`: their
    /// dedicated native binder has not yet been extracted, so callers must
    /// fail closed rather than materialize rows in the frontend.
    pub(crate) fn physical_sql(&self) -> Option<&str> {
        match &self.artifact {
            MvFirstRefreshExecutionArtifact::Sql(sql) => Some(sql.sql()),
            MvFirstRefreshExecutionArtifact::Logical(_) => None,
        }
    }

    /// Consuming bind boundary: fragment preparation may only happen after
    /// admission and exact-lease activation.  The resulting artifact cannot
    /// be rebound to another operation/cohort.
    pub(crate) fn bind_distributed(
        self,
        distributed: PreparedDistributedWriteRequest,
    ) -> Result<BoundMvFirstRefreshWrite, String> {
        if distributed.write_operation_id() != self.operation_id()
            || distributed.write_cohort_id() != self.primary_cohort
        {
            return Err("MV first-refresh distributed artifact identity mismatch".to_string());
        }
        Ok(BoundMvFirstRefreshWrite { distributed })
    }
}

/// Opaque post-admission artifact. It is intentionally consumable only by the
/// application route that owns the exact write session.
pub(crate) struct BoundMvFirstRefreshWrite {
    distributed: PreparedDistributedWriteRequest,
}

impl BoundMvFirstRefreshWrite {
    pub(crate) fn into_distributed(self) -> PreparedDistributedWriteRequest {
        self.distributed
    }
}

pub(crate) struct MvFirstRefreshWritePreparer;

impl MvFirstRefreshWritePreparer {
    pub(crate) fn prepare(
        request: MvFirstRefreshWriteRequest,
        physical_sql: MvFirstRefreshPhysicalSql,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(physical_sql),
            MvStagedRefreshWriteMode::Append,
        )
    }

    /// A policy-driven rebuild uses the pinned full-read physicalization but
    /// replaces the staging ref contents instead of appending to its main-ref
    /// base snapshot.
    pub(crate) fn prepare_full_overwrite(
        request: MvFirstRefreshWriteRequest,
        physical_sql: MvFirstRefreshPhysicalSql,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(physical_sql),
            MvStagedRefreshWriteMode::FullOverwrite,
        )
    }

    /// Freeze a typed join append projection behind the same prepared artifact
    /// boundary used by SQL-shaped first refreshes.
    pub(crate) fn prepare_join_logical(
        request: MvFirstRefreshWriteRequest,
        append: crate::mv::refresh::join_first_refresh::JoinFirstRefreshAppendLogicalPlan,
        context: MvFirstRefreshLogicalContext,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Logical(
                MvFirstRefreshLogicalArtifact::from_join_append(append, context),
            ),
            MvStagedRefreshWriteMode::Append,
        )
    }

    fn prepare_artifact(
        request: MvFirstRefreshWriteRequest,
        artifact: MvFirstRefreshExecutionArtifact,
        write_mode: MvStagedRefreshWriteMode,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        if artifact.root_hash_column() != request.target_contract().hidden_hash_key() {
            return Err(
                "MV first-refresh root distribution does not match the target hidden hash key"
                    .to_string(),
            );
        }
        if matches!(&artifact, MvFirstRefreshExecutionArtifact::Sql(physical_sql)
            if physical_sql.sql().contains("QueryResult")
                || physical_sql.sql().contains("RecordBatch")
                || physical_sql.sql().contains("Chunk"))
        {
            return Err(
                "MV first-refresh SQL artifact contains a frontend row carrier".to_string(),
            );
        }
        let operation_id = request.operation_id();
        Ok(PreparedMvFirstRefreshWrite {
            request,
            artifact,
            primary_cohort: ConnectorWriteCohortId::primary(operation_id),
            write_mode,
            provenance_properties: std::collections::BTreeMap::new(),
        })
    }
}

pub(crate) fn prepare_projection_first_refresh_write_sql(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let sql = prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_union_projection_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let sql = prepare_union_projection_full_read_sql(
        select_sql,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let state_sql = prepare_aggregate_first_refresh_state_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(MvFirstRefreshPhysicalSql {
        sql: aggregate_physical_sql(
            &state_sql,
            calls,
            None,
            target_schema,
            aggregate_input_types,
        )?,
        root_hash_column: ROW_ID_COLUMN.to_string(),
    })
}

/// Fan-in aggregate first refresh uses the same state-shaped physical project
/// as a single aggregate.  The canonical SELECT already contains the pinned
/// UNION ALL input, so keeping this as a separate entry point makes the shape
/// contract explicit without reintroducing a frontend materialization phase.
pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        aggregate_input_types,
    )
}

/// A composed aggregate (for example aggregate-over-join) is still one
/// state-shaped SELECT.  Its join/fan-in relationship lives below the common
/// aggregate project and therefore remains BE-owned all the way to the
/// connector writer.
pub(crate) fn prepare_composed_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_aggregate_first_refresh_write_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let branches = prepare_branch_union_aggregate_first_refresh_state_sqls(
        select_sql,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
    )?;
    let sql = branches
        .into_iter()
        .enumerate()
        .map(|(branch_index, (calls, state_sql))| {
            validate_branch_aggregate_contract(branch_index, &calls, first_branch_calls)?;
            let branch_id = i32::try_from(branch_index).map_err(|_| {
                format!("MV first-refresh branch index {branch_index} exceeds Int32")
            })?;
            aggregate_physical_sql(&state_sql, &calls, Some(branch_id), target_schema, None)
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(" UNION ALL ");
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: ROW_ID_COLUMN.to_string(),
    })
}

fn aggregate_physical_sql(
    state_sql: &str,
    calls: &AggregateSqlCalls,
    branch_id: Option<i32>,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<String, String> {
    let mut projection = Vec::with_capacity(
        1 + calls.visible_outputs.len() + calls.aggregates.len() + usize::from(branch_id.is_some()),
    );
    let group_key_refs = calls
        .group_keys
        .iter()
        .map(|key| qualified_column("state", &key.output_name))
        .collect::<Vec<_>>();
    projection.push(format!(
        "mv_group_row_id({}) AS {}",
        group_key_refs.join(", "),
        quote_sql_identifier(ROW_ID_COLUMN),
    ));

    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!("MV first-refresh group key index {group_key_index} out of range")
                })?;
                projection.push(format!(
                    "{} AS {}",
                    qualified_column("state", &key.output_name),
                    quote_sql_identifier(&key.output_name),
                ));
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!("MV first-refresh aggregate index {aggregate_index} out of range")
                })?;
                let state_name = state_column_name(&aggregate.output_name);
                let witness = if matches!(
                    aggregate.function,
                    AggregateFunctionKind::Sum
                        | AggregateFunctionKind::Min
                        | AggregateFunctionKind::Max
                ) {
                    target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?
                } else {
                    None
                };
                let args = if aggregate.function == AggregateFunctionKind::Avg {
                    let input_type = aggregate_input_types
                        .and_then(|types| types.get(*aggregate_index))
                        .and_then(Option::as_ref);
                    let output_witness = target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?;
                    match output_witness {
                        Some(witness) => {
                            let input_scale = match input_type {
                                Some(DataType::Decimal128(_, scale)) => i64::from(*scale),
                                _ => -1,
                            };
                            format!(
                                "{}, CAST({input_scale} AS BIGINT), {witness}",
                                qualified_column("state", &state_name)
                            )
                        }
                        None => qualified_column("state", &state_name),
                    }
                } else {
                    match witness {
                        Some(witness) => {
                            format!("{}, {witness}", qualified_column("state", &state_name))
                        }
                        None => qualified_column("state", &state_name),
                    }
                };
                projection.push(format!(
                    "{}({args}) AS {}",
                    aggregate_visible_function(aggregate.function),
                    quote_sql_identifier(&aggregate.output_name),
                ));
            }
        }
    }

    for aggregate in &calls.aggregates {
        let state_name = state_column_name(&aggregate.output_name);
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", &state_name),
            quote_sql_identifier(&state_name),
        ));
    }
    if crate::mv::aggregate_state::mv_agg_state::aggregate_shape_needs_retraction_count_state(calls)
    {
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", AGG_RETRACTION_COUNT_STATE_COLUMN),
            quote_sql_identifier(AGG_RETRACTION_COUNT_STATE_COLUMN),
        ));
    }
    if let Some(branch_id) = branch_id {
        projection.push(format!(
            "CAST({branch_id} AS INT) AS {}",
            quote_sql_identifier(BRANCH_ID_COLUMN_NAME),
        ));
    }

    Ok(format!(
        "SELECT {} FROM ({state_sql}) AS state",
        projection.join(", "),
    ))
}

fn aggregate_visible_type_witness(data_type: &DataType) -> Result<String, String> {
    let sql_type = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => {
            return Err(format!(
                "unsupported MV aggregate visible target type {other:?}"
            ));
        }
    };
    Ok(format!("CAST(NULL AS {sql_type})"))
}

fn validate_branch_aggregate_contract(
    branch_index: usize,
    calls: &AggregateSqlCalls,
    expected: &AggregateSqlCalls,
) -> Result<(), String> {
    if calls.visible_outputs != expected.visible_outputs {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} visible output order differs from branch 0"
        ));
    }
    if calls.group_keys.len() != expected.group_keys.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} group-key count differs from branch 0"
        ));
    }
    if calls.aggregates.len() != expected.aggregates.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} aggregate count differs from branch 0"
        ));
    }
    for (aggregate_index, (actual, expected)) in calls
        .aggregates
        .iter()
        .zip(expected.aggregates.iter())
        .enumerate()
    {
        if actual.function != expected.function {
            return Err(format!(
                "MV first-refresh aggregate branch {branch_index} aggregate {aggregate_index} function differs from branch 0"
            ));
        }
    }
    Ok(())
}

fn aggregate_visible_function(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state_visible",
        AggregateFunctionKind::Sum => "sum_state_visible",
        AggregateFunctionKind::Avg => "avg_state_visible",
        AggregateFunctionKind::Min => "min_state_visible",
        AggregateFunctionKind::Max => "max_state_visible",
        AggregateFunctionKind::BoolOr => "bool_or_state_visible",
        AggregateFunctionKind::BoolAnd => "bool_and_state_visible",
        AggregateFunctionKind::CountDistinct => "count_distinct_state_visible",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state_visible",
    }
}

fn state_column_name(output_name: &str) -> String {
    format!(
        "{AGG_STATE_PREFIX}{}",
        sanitize_state_column_name(output_name)
    )
}

fn qualified_column(qualifier: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_sql_identifier(qualifier),
        quote_sql_identifier(column)
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use super::*;

    fn pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
    }

    #[test]
    fn projection_keeps_pinned_hidden_apply_key_for_writer_distribution() {
        let prepared = prepare_projection_first_refresh_write_sql(
            "SELECT v FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(
            prepared.root_hash_column(),
            crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME
        );
        assert!(prepared.sql().contains("__nova_base_row_id"));
        assert!(
            prepared.sql().contains("VERSION AS OF 42"),
            "expected pinned physical SQL, got: {}",
            prepared.sql()
        );
    }

    #[test]
    fn aggregate_uses_be_visible_and_state_projection() {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
        )
        .unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .unwrap();
        let prepared = prepare_aggregate_first_refresh_write_sql(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
            &calls,
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), ROW_ID_COLUMN);
        assert!(prepared.sql().contains("mv_group_row_id"));
        assert!(prepared.sql().contains("sum_state_visible"));
        assert!(prepared.sql().contains("__agg_state_total"));
        assert!(!prepared.sql().contains("RecordBatch"));
    }

    #[test]
    fn fan_in_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT k, sum(v) AS total FROM (SELECT k, v FROM ice.db.a UNION ALL SELECT k, v FROM ice.db.b) AS input GROUP BY k";
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .unwrap();
        let pin = RefreshSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared =
            prepare_fan_in_aggregate_first_refresh_write_sql(sql, &calls, &pin, Some("ice"), "db")
                .unwrap();
        assert_eq!(prepared.root_hash_column(), ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("sum_state_visible"));
    }

    #[test]
    fn fan_in_decimal_avg_freezes_input_scale_and_visible_type_in_be_sql() {
        let sql = "SELECT k, avg(d) AS a_d FROM (SELECT k, d FROM ice.db.a UNION ALL SELECT k, d FROM ice.db.b) AS input GROUP BY k";
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .unwrap();
        let target = Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("a_d", DataType::Decimal128(38, 12), true),
        ]);
        let prepared =
            prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                sql,
                &calls,
                &RefreshSnapshotPin::from_entries_for_tests(&[
                    ("ice.db.a", 11, "a"),
                    ("ice.db.b", 22, "b"),
                ]),
                Some("ice"),
                "db",
                Some(&target),
                Some(&[Some(DataType::Decimal128(20, 4))]),
            )
            .unwrap();
        assert!(prepared.sql().contains("avg_state_visible(`state`.`__agg_state_a_d`, CAST(4 AS BIGINT), CAST(NULL AS DECIMAL(38,12)))"), "{}", prepared.sql());
    }

    #[test]
    fn composed_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT a.k, count(*) AS total FROM ice.db.a AS a JOIN ice.db.b AS b ON a.k = b.k GROUP BY a.k";
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .unwrap();
        let pin = RefreshSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared = prepare_composed_aggregate_first_refresh_write_sql(
            sql,
            &calls,
            &pin,
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("count_state_visible"));
    }

    #[test]
    fn target_contract_rejects_schema_identity_and_partition_drift() {
        let expected = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        let contract = MvFirstRefreshTargetContract::try_new(
            Arc::clone(&expected),
            vec![1, 2],
            7,
            "__apply_key__".to_string(),
        )
        .expect("valid target contract");
        contract
            .validate_observed(expected.as_ref(), &[1, 2], 7)
            .expect("exact observed contract");
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 3], 7)
                .is_err()
        );
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 2], 8)
                .is_err()
        );
        let drifted_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        assert!(
            contract
                .validate_observed(drifted_schema.as_ref(), &[1, 2], 7)
                .is_err()
        );
    }
}
