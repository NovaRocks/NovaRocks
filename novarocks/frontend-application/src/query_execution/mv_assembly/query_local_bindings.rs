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

//! Request-local IMV table bindings assembled from already captured facts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorReadSelector, ConnectorRequestContext,
    MvExactPartitionField, MvExactPartitionTransform,
    read_stack::{
        ConnectorMvPartitionValue, ConnectorMvTargetPartitionSelection,
        MAX_MV_TARGET_PARTITION_KEYS, MAX_MV_TARGET_PARTITION_VALUE_BYTES,
    },
};
use novarocks_types::naming::TableIdentity;

use crate::catalog_application::query_bindings::{
    MvTargetReadAdmission, QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::{
    QueryLocalTableOverlay, connector_query_binding_from_materialization,
};
use crate::mv::domain::model::{AffectedTargetPartitions, MvPartitionValue};
use crate::mv::domain::rewrite::context::{IcebergMvRewriteContext, MvRewriteSourceSnapshot};
use novarocks_mv_application::persistence::codec::TargetPartitionTransform;

struct SourceGroup {
    base: TableIdentity,
    current: Vec<MvRewriteSourceSnapshot>,
    previous: BTreeMap<novarocks_sql::compiler::SqlMvRelationOccurrenceId, MvRewriteSourceSnapshot>,
}

fn group_sources_by_locator(
    rewrite: &IcebergMvRewriteContext,
) -> Result<BTreeMap<String, SourceGroup>, String> {
    let definition = rewrite.mv_definition.facts.definition();
    if definition.relation_occurrences.len() != rewrite.pin.len() {
        return Err("MV query bindings do not cover every D occurrence".to_string());
    }
    let previous = rewrite
        .previous
        .iter()
        .map(|source| (source.occurrence_id, source))
        .collect::<BTreeMap<_, _>>();
    let mut matched_previous = BTreeSet::new();
    let mut groups = BTreeMap::new();
    for occurrence in &definition.relation_occurrences {
        let occurrence_id =
            novarocks_sql::compiler::SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
        let source = rewrite.pin.get(&occurrence_id).ok_or_else(|| {
            format!(
                "MV query binding is missing D occurrence {}",
                occurrence.occurrence_id
            )
        })?;
        let base = TableIdentity {
            catalog: occurrence.catalog_at_binding.clone(),
            namespace: occurrence.namespace_at_binding.clone(),
            table: occurrence.relation_at_binding.clone(),
        };
        let key = base.fqn().to_ascii_lowercase();
        let group = groups.entry(key).or_insert_with(|| SourceGroup {
            base: base.clone(),
            current: Vec::new(),
            previous: BTreeMap::new(),
        });
        if group.base != base {
            return Err("MV query binding locator normalization is ambiguous".to_string());
        }
        group.current.push(source.clone());
        if let Some(previous) = previous.get(&occurrence_id) {
            matched_previous.insert(occurrence_id);
            group.previous.insert(occurrence_id, (*previous).clone());
        }
    }
    if matched_previous.len() != previous.len() {
        return Err("MV query bindings contain history for an unknown D occurrence".to_string());
    }
    Ok(groups)
}

/// Binding projection is pure over the original exact metadata value.
pub(crate) fn bind_imv_target_query_table_from_metadata(
    rewrite: &IcebergMvRewriteContext,
    store: &Arc<QueryTableBindingStore>,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    metadata: novarocks_spi::connector::ConnectorTableMetadata,
    affected_partitions: Option<&AffectedTargetPartitions>,
) -> Result<novarocks_sql::binding::SqlTableBindingId, String> {
    let target = &rewrite.target;
    let target_table_uuid = rewrite.target_table_uuid.clone();
    let frozen_snapshot_id = rewrite.target_snapshot_id;
    let selector = frozen_snapshot_id
        .map(novarocks_spi::connector::ConnectorReadSelector::SnapshotId)
        .unwrap_or(novarocks_spi::connector::ConnectorReadSelector::Current);
    let target_read = QueryScanMaterialization {
        table: metadata.table.clone(),
        selector,
        mv_partition_selection: None,
        planning_lease: planning_lease.clone(),
    };
    let selection = affected_partitions
        .and_then(|affected| match affected {
            AffectedTargetPartitions::Known { partitions } => Some(partitions),
            AffectedTargetPartitions::Unpartitioned | AffectedTargetPartitions::NotDerived { .. } => None,
        })
        // A bounded carrier is an optimization. When the exact Known set is
        // too large to carry, read the whole pinned snapshot instead.
        .filter(|partitions| {
            partitions.len() <= MAX_MV_TARGET_PARTITION_KEYS
                && partitions.iter().flat_map(|key| &key.fields).all(|field| {
                    !matches!(&field.value, MvPartitionValue::String(value) if value.len() > MAX_MV_TARGET_PARTITION_VALUE_BYTES)
                })
        })
        .zip(frozen_snapshot_id)
        .map(|(partitions, snapshot_id)| {
            let target = &rewrite.mv_definition.facts.interpretation().target;
            let fields = target.partition_fields.iter().map(|field| {
                let transform = match field.transform {
                    TargetPartitionTransform::Identity => MvExactPartitionTransform::Identity,
                    TargetPartitionTransform::Year => MvExactPartitionTransform::Year,
                    TargetPartitionTransform::Month => MvExactPartitionTransform::Month,
                    TargetPartitionTransform::Day => MvExactPartitionTransform::Day,
                    TargetPartitionTransform::Hour => MvExactPartitionTransform::Hour,
                    TargetPartitionTransform::Bucket { num_buckets } => MvExactPartitionTransform::Bucket { num_buckets },
                    TargetPartitionTransform::Truncate { width } => MvExactPartitionTransform::Truncate { width },
                    TargetPartitionTransform::Void => MvExactPartitionTransform::Void,
                };
                MvExactPartitionField::try_new(
                    bytes::Bytes::copy_from_slice(field.partition_field_id.as_bytes()),
                    bytes::Bytes::copy_from_slice(field.source_target_field_id.as_bytes()),
                    transform,
                ).map_err(|error| error.to_string())
            }).collect::<Result<Vec<_>, String>>()?;
            let keys = partitions.iter().map(|partition| {
                if partition.spec_version != target.partition_spec_version
                    || partition.fields.len() != target.partition_fields.len() {
                    return Err("MV affected partition key disagrees with canonical target spec".to_string());
                }
                partition.fields.iter().zip(&target.partition_fields).map(|(value, field)| {
                    if value.partition_field_id != field.partition_field_id {
                        return Err("MV affected partition field order disagrees with canonical target".to_string());
                    }
                    Ok(match &value.value {
                        MvPartitionValue::Null => ConnectorMvPartitionValue::Null,
                        MvPartitionValue::String(value) => ConnectorMvPartitionValue::String(value.as_str().into()),
                    })
                }).collect::<Result<Vec<_>, String>>()
            }).collect::<Result<Vec<_>, String>>()?;
            ConnectorMvTargetPartitionSelection::try_new(
                rewrite.mv_definition.facts.source_revision().target_object_id.clone(),
                bytes::Bytes::copy_from_slice(target.partition_spec_version.as_bytes()),
                fields,
                keys,
                snapshot_id,
            ).map_err(|error| error.to_string())
        }).transpose()?;
    let mut affected_read = target_read.clone();
    affected_read.mv_partition_selection = selection;
    let mv_target_read = MvTargetReadAdmission {
        full: target_read.clone(),
        affected_partitions: affected_read,
        target_table_uuid: target_table_uuid.clone(),
        target_object_id: rewrite
            .mv_definition
            .facts
            .source_revision()
            .target_object_id
            .clone(),
        frozen_snapshot_id,
    };
    let key = QueryTableBindingKey::mv_target(
        &target.catalog,
        &target.namespace,
        &target.table,
        &target_table_uuid,
        frozen_snapshot_id,
    );
    let [apply_key] = rewrite.runtime_bindings.apply_key.as_slice() else {
        return Err("MV target binding requires one exact apply-key field".to_string());
    };
    let branch_column = match rewrite.runtime_bindings.branches.as_slice() {
        [] => None,
        [(_, first), rest @ ..] => {
            if rest
                .iter()
                .any(|(_, field)| field.field_id != first.field_id || field.name != first.name)
            {
                return Err(
                    "MV target binding has conflicting exact branch discriminator fields"
                        .to_string(),
                );
            }
            Some(first.name.clone())
        }
    };
    let apply_key_column = apply_key.name.clone();
    store.resolve_or_insert_with_id(key, |binding| {
        let resolved = novarocks_sql::planning::catalog::materialize_mv_target_locator_table(
            novarocks_sql::planning::catalog::SqlMvTargetLocatorTableFacts::try_new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
                target_table_uuid.clone(),
                frozen_snapshot_id,
                apply_key_column.clone(),
                branch_column.clone(),
                binding,
            )?,
        )
        .into_resolved_table();
        Ok(QueryTableBinding {
            resolved,
            statistics_pin: None,
            admission:
                crate::catalog_application::query_bindings::QueryTableBindingAdmission::Exact(
                    planning_lease,
                ),
            source_metadata: None,
            scan_materialization: Some(mv_target_read.full.clone()),
            mv_target_read: Some(mv_target_read),
            write_target_admission: None,
            frozen_cohort_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            prepared_source_reads: BTreeMap::new(),
        })
    })
}

pub(crate) async fn bind_imv_target_query_table_in_store_from_rewrite_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    rewrite: &IcebergMvRewriteContext,
    store: &Arc<QueryTableBindingStore>,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &ConnectorRequestContext,
    affected_partitions: Option<&AffectedTargetPartitions>,
) -> Result<
    novarocks_sql::binding::SqlTableBindingId,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    let metadata = io
        .metadata(
            planning_lease,
            connector_context.clone(),
            &rewrite.target.namespace,
            &rewrite.target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .await?;
    bind_imv_target_query_table_from_metadata(
        rewrite,
        store,
        planning_lease.clone(),
        metadata,
        affected_partitions,
    )
    .map_err(Into::into)
}

pub(crate) async fn freeze_imv_base_query_local_overlays_from_captured_inputs_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    _connector_control: &dyn ConnectorControlRegistry,
    _connector_context: &ConnectorRequestContext,
    rewrite: &IcebergMvRewriteContext,
) -> Result<
    Vec<QueryLocalTableOverlay>,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    let sources = io.captured_sources();
    for SourceGroup {
        base,
        current,
        previous,
    } in group_sources_by_locator(rewrite)?.into_values()
    {
        let source = sources.require(&base)?;
        let metadata = source.metadata()?;
        let materialization = crate::catalog_application::query_catalog::connector_table_materialization_from_metadata(
            metadata, source.lease.clone())?;
        validate_source_revisions_original(&materialization, current.iter())?;
        // Historical From is an execution dependency only for a Delta-capable receipt.
        // FullRebuild/MetadataOnly cannot manufacture an executable From relation.
        for old in previous.values() {
            if let Some(receipt) = source.receipt(Some(&old.semantic_revision))
                && matches!(
                    receipt.admission(),
                    Some(
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental { .. }
                    )
                )
            {
                validate_source_revisions_original(&materialization, std::iter::once(old))?;
            }
        }
    }
    freeze_captured_source_overlays(&sources)
}

/// Used by early schema analysis, first SQL and final activation alike.
/// The materialization and read recipes come from the same original To author.
pub(crate) fn freeze_captured_source_overlays(
    sources: &super::refresh_preparation::captured_sources::CapturedRefreshSources,
) -> Result<
    Vec<QueryLocalTableOverlay>,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    use novarocks_spi::connector::read_stack::ConnectorReadSourceProjection as Projection;
    let mut overlays = Vec::new();
    for (base, captured) in sources.bindings() {
        let metadata = if captured.snapshot.is_some() {
            captured.metadata()?
        } else {
            captured.observed.clone()
        };
        let mut materialization = crate::catalog_application::query_catalog::connector_table_materialization_from_metadata(
            metadata, captured.lease.clone())?;
        if let Some(snapshot) = captured.snapshot {
            materialization.read_selector = ConnectorReadSelector::SnapshotId(snapshot);
        }
        let receipts = captured.receipts();
        let to = if captured.snapshot.is_some() {
            Some(captured.to_receipt()?)
        } else {
            None
        };
        let catalog = base.catalog.clone();
        let namespace = base.namespace.clone();
        let table = base.table.clone();
        let key = match captured.snapshot {
            Some(id) => QueryTableBindingKey::snapshot(&catalog, &namespace, &table, id),
            None => QueryTableBindingKey::strict_base(&catalog, &namespace, &table),
        };
        overlays.push(QueryLocalTableOverlay::for_catalog(
            catalog.clone(),
            namespace.clone(),
            table.clone(),
            key,
            move |binding| {
                let mut result = connector_query_binding_from_materialization(
                    materialization.clone(),
                    &catalog,
                    &namespace,
                    &table,
                    binding,
                )?;
                if let Some(to) = &to {
                    result.install_prepared_source_read(
                        to.clone(),
                        Projection::To,
                        materialization.planning_lease.clone(),
                    )?;
                }
                for receipt in &receipts {
                    match receipt.admission() {
                        Some(
                            novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental {
                                ..
                            },
                        ) => {
                            result.install_prepared_source_read(
                                receipt.clone(),
                                Projection::From,
                                materialization.planning_lease.clone(),
                            )?;
                            result.install_prepared_source_read(
                                receipt.clone(),
                                Projection::Delta,
                                materialization.planning_lease.clone(),
                            )?;
                        }
                        Some(
                            novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                        ) => {
                            result.install_prepared_source_read(
                                receipt.clone(),
                                Projection::Delta,
                                materialization.planning_lease.clone(),
                            )?;
                        }
                        _ => {}
                    }
                }
                Ok(result)
            },
        ));
    }
    Ok(overlays)
}

fn validate_source_revisions_original<'a>(
    materialization: &crate::catalog_application::query_catalog::ConnectorQueryTableMaterialization,
    sources: impl Iterator<Item = &'a MvRewriteSourceSnapshot>,
) -> Result<
    (),
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    for source in sources {
        let observed = materialization
            .planning_lease
            .binding()
            .metadata()
            .exact_semantic_revision(
                &materialization.read_table,
                ConnectorReadSelector::SnapshotId(source.snapshot_id),
            )?;
        if observed != source.semantic_revision {
            return Err(format!(
                "MV source revision drifted for D occurrence {}",
                source.occurrence_id.get()
            )
            .into());
        }
    }
    Ok(())
}
