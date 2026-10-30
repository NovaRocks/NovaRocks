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

//! Frontend-owned MV refresh-preparation coordinator.

//! Projection/filter materialized views backed by Iceberg target tables in the
//! current Iceberg catalog. Aggregate shapes are accepted at CREATE time for
//! target schema and contract persistence; refresh execution is gated later.

pub(crate) mod async_original;
pub(crate) mod driver;
pub(crate) mod io_gate;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::mv::domain::analysis::refresh_property::derive_fragment_property;
use crate::mv::domain::analysis::{
    canonicalize_iceberg_mv_select_query, validate_mv_partition_columns,
};
use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::iceberg_refresh::{IcebergMvCorePorts, join_base_refs_for_definition};
use crate::mv::domain::refresh::capabilities::RefreshCapabilities;
use crate::mv::domain::refresh::definition::{mv_definition_fingerprint, parse_mv_select_query};
use crate::mv::domain::refresh::execution_policy::{
    non_join_incremental_write_mode, select_join_incremental_execution_mode,
};
use crate::mv::domain::refresh::non_join_incremental::{
    NonJoinBaseChange, NonJoinIncrementalChangePlan, plan_non_join_incremental_changes,
};
use crate::mv::domain::refresh::pin::{RefreshSnapshotPin, RefreshSnapshotPinOccurrence};
use crate::mv::domain::refresh::planning::{
    RefreshPlanContract, RefreshStateBaseline, RefreshStateBaselineSource,
};
use crate::mv::domain::refresh::repartition::select_repartition_shape;
use crate::mv::domain::refresh::rewrite_context::{RefreshRewriteInputs, admitted_change_facts};
use crate::mv::domain::refresh::schema_contract::validate_projection_target;
use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::staged_create::AdmittedMvDataPublication;
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvFirstRefreshWritePreparer, MvFirstRefreshWriteRequest, MvIncrementalExecutionArtifact,
    MvIncrementalWritePreparer, MvIncrementalWriteRequest, PreparedMvFirstRefreshWrite,
    PreparedMvIncrementalWrite, PreparedMvMetadataOnlyWrite,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshPreparationRequest, PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::product::MvRefreshAttemptIdentity;
use novarocks_mv_application::product::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};
use novarocks_mv_application::publication::{
    MvRefreshPublicationBase, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorManagedPartitionSpecPreviewRequest, ConnectorProviderBindingKey,
    ConnectorTableIdentity, ConnectorTableObjectId,
};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::MvRefreshFinalizeFacts;
use novarocks_sql::planning::mv::{SqlMvAggregateLayoutScope, extract_aggregate_sql_calls};
use novarocks_sql::semantic::IcebergPartitionFieldExpr;
/// One repartition target observation retained across transition and write
/// preparation. Both payloads were produced from the same exact lease and
/// metadata value; no downstream repartition step may resolve `latest` again.
struct RetainedRepartitionTarget {
    binding: crate::mv::domain::refresh::target_binding::MvTargetBinding,
    schema_validation: MvSchemaValidationObservation,
}

struct PreparedManagedRepartitionTransition {
    replacement: novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement,
    preview: novarocks_spi::connector::ConnectorManagedPartitionSpecPreview,
}

pub(crate) fn validate_retained_target_identity(
    target: &IcebergMvTarget,
    identity: &ConnectorTableIdentity,
) -> Result<(), String> {
    let expected_instance =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    if identity.instance_id != expected_instance
        || identity.namespace.as_ref() != target.namespace
        || identity.table.as_ref() != target.table
    {
        return Err(format!(
            "retained MV repartition target identity does not match {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(())
}

fn execution_binding_key_for_target(
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
) -> ConnectorProviderBindingKey {
    ConnectorProviderBindingKey {
        instance_id: lease.binding().descriptor().instance_id.clone(),
        incarnation: lease.binding().incarnation(),
    }
}

fn managed_repartition_field(
    field: &IcebergPartitionFieldExpr,
) -> (
    &str,
    novarocks_spi::connector::ConnectorManagedPartitionTransform,
) {
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Transform;
    match field {
        IcebergPartitionFieldExpr::Identity { column } => (column, Transform::Identity),
        IcebergPartitionFieldExpr::Year { column } => (column, Transform::Year),
        IcebergPartitionFieldExpr::Month { column } => (column, Transform::Month),
        IcebergPartitionFieldExpr::Day { column } => (column, Transform::Day),
        IcebergPartitionFieldExpr::Hour { column } => (column, Transform::Hour),
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => (
            column,
            Transform::Bucket {
                buckets: *num_buckets,
            },
        ),
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            (column, Transform::Truncate { width: *width })
        }
        IcebergPartitionFieldExpr::Void { column } => (column, Transform::Void),
    }
}

fn managed_partition_transform(
    transform: &novarocks_mv_application::persistence::schema::MvPartitionTransformContract,
) -> novarocks_spi::connector::ConnectorManagedPartitionTransform {
    use novarocks_mv_application::persistence::schema::MvPartitionTransformContract as Observed;
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Managed;
    match transform {
        Observed::Identity => Managed::Identity,
        Observed::Year => Managed::Year,
        Observed::Month => Managed::Month,
        Observed::Day => Managed::Day,
        Observed::Hour => Managed::Hour,
        Observed::Bucket { num_buckets } => Managed::Bucket {
            buckets: *num_buckets,
        },
        Observed::Truncate { width } => Managed::Truncate { width: *width },
        Observed::Void => Managed::Void,
    }
}

/// The exact target's physical field IDs are positionally aligned with the
/// same retained observation's Arrow schema.
fn retained_target_field_id_by_name(
    retained_target: &RetainedRepartitionTarget,
) -> Result<Vec<(String, i32)>, String> {
    let schema = retained_target.binding.physical_write_schema()?;
    let field_ids = retained_target.binding.observation().field_ids();
    if schema.fields().len() != field_ids.len() {
        return Err(
            "MV repartition target observation does not align field IDs with its schema"
                .to_string(),
        );
    }
    Ok(schema
        .fields()
        .iter()
        .zip(field_ids)
        .map(|(field, field_id)| (field.name().clone(), *field_id))
        .collect())
}

/// Reparse D's immutable effective SQL under its own frozen resolution context.
/// The session that happens to issue the statement never resolves these names.
pub(crate) fn canonical_mv_select_query(
    projection: &StoredKeyedMvProjection,
) -> Result<novarocks_parser::ast::Query, String> {
    let effective_sql = projection.facts.definition().query.effective_sql.clone();
    canonical_mv_select_query_from_source(projection, &effective_sql)
}

/// Same canonical resolution, applied to the SQL an occurrence-aware rebind
/// produced for this attempt. The rebind never changes name resolution.
fn canonical_mv_select_query_from_source(
    projection: &StoredKeyedMvProjection,
    effective_sql: &str,
) -> Result<novarocks_parser::ast::Query, String> {
    let resolution = &projection.facts.definition().query.resolution;
    Ok(canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(effective_sql)?,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
    ))
}

/// D's relation shape decides whether one empty source may still refresh.
/// A two-relation join pair may; a fan-in of independent relations may not.
fn definition_has_join(query: &novarocks_parser::ast::Query) -> bool {
    novarocks_sql::planning::mv::extract_join_aliases(query).is_ok()
}

fn expected_target_snapshot(contract: &RefreshPlanContract) -> Option<i64> {
    match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
    }
}

/// Ordered exact source revisions the published baseline pinned, if any.
/// What each occurrence was pinned at, for the provenance a publication
/// records. An unpinned base is a planning mistake, not an absent fact.
fn pinned_snapshots_by_occurrence(
    contract: &RefreshPlanContract,
    context: &str,
) -> Result<
    BTreeMap<SqlMvRelationOccurrenceId, (novarocks_types::naming::TableIdentity, i64)>,
    String,
> {
    contract
        .base_refs
        .iter()
        .map(|base| {
            contract
                .snapshot_pins
                .get(&base.occurrence_id)
                .copied()
                .flatten()
                .map(|snapshot| (base.occurrence_id, (base.table.clone(), snapshot)))
                .ok_or_else(|| format!("{context} has no pinned snapshot for {}", base.display()))
        })
        .collect()
}

/// The contract's base relations as a positional table list.
///
/// Some consumers still match each base against D's occurrence at the same
/// index rather than by occurrence id. The order is the contract's own, so
/// projecting away the ids here loses nothing those consumers use.
fn contract_base_tables(
    contract: &RefreshPlanContract,
) -> Vec<novarocks_types::naming::TableIdentity> {
    contract
        .base_refs
        .iter()
        .map(|base| base.table.clone())
        .collect()
}

fn baseline_previous_sources(baseline: &RefreshStateBaseline) -> &[RefreshStateBaselineSource] {
    match baseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_sources, ..
        } => previous_sources,
    }
}

/// Prepare the value-only non-join incremental handoff.  This is deliberately
/// limited to change-stream shapes that already have one generic native
/// writer contract.  Join branches and policy-driven full rebuilds retain
/// their explicit preparation boundary until their distinct physical artifacts
/// are extracted; treating either as an append would be incorrect.
enum PreparedIncrementalRefreshWork {
    MetadataOnly,
    ChangeStream(PreparedMvIncrementalWrite, AdmittedMvDataPublication),
    FullRebuild(PreparedMvFirstRefreshWrite, AdmittedMvDataPublication),
}

pub(crate) mod async_facts;
pub(crate) mod captured_sources;

pub(crate) mod pure_reader;
