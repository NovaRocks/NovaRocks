// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Complete replacement scope and its provider-verified source predicates.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use novarocks_mv_application::persistence::schema::MvPartitionContract;
use novarocks_spi::connector::read_stack::{
    ConnectorExactPartitionSliceVolume, ConnectorExactSourcePartitionSlice,
    ConnectorMvTargetPartitionSelection, ConnectorPreparedSourceRead,
    ConnectorReadSourcePreparationRequest,
};
use novarocks_spi::connector::{
    ConnectorChangePartitionValue, ConnectorChangeWindowFacts, ConnectorRequestContext,
};
use novarocks_sql::planning::mv::{
    SqlMvRelationOccurrenceId, first_refresh::SqlMvPartitionSourceBinding,
};
use novarocks_types::naming::TableIdentity;

use super::query_local_bindings::target_partition_replacement_selection;
use super::refresh_preparation::async_facts::PreparationError;
use super::refresh_preparation::captured_sources::CapturedRefreshSource;
use super::refresh_preparation::io_gate::MvRefreshPreparationIo;
use crate::catalog_application::{
    query_bindings::{QueryFrozenReadInput, QueryTableBindingKey, QueryTableBindingStore},
    query_catalog::connector_table_materialization_from_metadata,
    query_materializer::connector_query_binding_from_materialization,
};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::model::{AffectedTargetPartitions, MvPartitionValue};
use crate::mv::domain::partition::mapping::{
    exact_source_partition_fields, plan_prepared_source_partitions,
};
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;
use crate::mv::domain::storage_observation::observe_source_schema;

pub(crate) struct PreparedPartitionReplacement {
    pub(crate) affected: AffectedTargetPartitions,
    pub(crate) target: ConnectorMvTargetPartitionSelection,
    pub(crate) reads: PreparedPartitionReplacementReads,
    pub(crate) volumes: BTreeMap<SqlMvRelationOccurrenceId, ConnectorExactPartitionSliceVolume>,
}

#[derive(Clone)]
pub(crate) struct PreparedPartitionReplacementReads(Vec<PreparedPartitionReplacementRead>);

#[derive(Clone)]
struct PreparedPartitionReplacementRead {
    occurrence: SqlMvRelationOccurrenceId,
    table: TableIdentity,
    qualifier: String,
    source: Arc<CapturedRefreshSource>,
    receipt: ConnectorPreparedSourceRead,
}

impl PreparedPartitionReplacementReads {
    pub(crate) fn register(
        &self,
        store: &QueryTableBindingStore,
    ) -> Result<Vec<SqlMvPartitionSourceBinding>, PreparationError> {
        self.0
            .iter()
            .map(|read| {
                let table = &read.table;
                let snapshot = read.receipt.request().selection().to_snapshot_id();
                let pin = &read.source.planning_pin;
                let key = QueryTableBindingKey::mv_partition_replacement_source(
                    &table.catalog,
                    &table.namespace,
                    &table.table,
                    snapshot,
                    read.occurrence.get(),
                );
                let binding = store.resolve_or_insert_with_id(key, |id| {
                    let mut materialization = connector_table_materialization_from_metadata(
                        read.receipt.to_metadata().clone(),
                        pin.clone(),
                    )?;
                    materialization.read_selector =
                        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot);
                    let mut binding = connector_query_binding_from_materialization(
                        materialization,
                        &table.catalog,
                        &table.namespace,
                        &table.table,
                        id,
                    )?;
                    binding.install_prepared_source_read(
                        read.receipt.clone(),
                        novarocks_spi::connector::read_stack::ConnectorReadSourceProjection::To,
                        pin.clone(),
                    )?;
                    Ok(binding)
                })?;
                let installed = store
                    .binding(binding)?
                    .prepared_source_read(QueryFrozenReadInput::Snapshot(snapshot))?
                    .ok_or("MV replacement binding has no exact prepared source")?;
                let original = read.receipt.request();
                let installed = installed.receipt.request();
                if installed.object_id() != original.object_id()
                    || installed.to_revision() != original.to_revision()
                    || installed.exact_partition_slice() != original.exact_partition_slice()
                {
                    return Err(
                        "MV replacement source binding was reused for another frozen slice".into(),
                    );
                }
                Ok(SqlMvPartitionSourceBinding::new(
                    read.occurrence,
                    table.clone(),
                    read.qualifier.clone(),
                    snapshot,
                    binding,
                ))
            })
            .collect()
    }
}

/// Recompute the inverse of the global affected S keys on every source To.
/// Original To and change-window reads stay untouched until strategy selection.
pub(crate) async fn prepare_partition_replacement(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target_partition: &MvPartitionContract,
    context: &ConnectorRequestContext,
    windows: &[ConnectorChangeWindowFacts],
) -> Result<Option<PreparedPartitionReplacement>, PreparationError> {
    if target_partition.fields.is_empty() || windows.len() != rewrite.base_refs.len() {
        return Ok(None);
    }
    let captured = io.captured_sources();
    let Some(direct_sources) = captured.replacement_output_sources() else {
        return Ok(None);
    };
    if direct_sources.len() != rewrite.base_refs.len() {
        return Ok(None);
    }
    let mut affected = BTreeSet::new();
    let mut reads = Vec::with_capacity(windows.len());
    for (base, expected_window) in rewrite.base_refs.iter().zip(windows) {
        let occurrence = base.occurrence_id.get();
        let Some(direct) = direct_sources.get(&occurrence) else {
            return Ok(None);
        };
        let pinned = captured.require(&base.table)?;
        let old = rewrite
            .previous
            .iter()
            .find(|value| value.occurrence_id == base.occurrence_id)
            .ok_or("MV replacement lost its original From occurrence")?;
        let receipt = pinned
            .receipt(Some(&old.semantic_revision))
            .ok_or("MV replacement has no exact prepared window")?;
        if receipt.window_facts() != Some(expected_window)
            || receipt.request().selection().to_snapshot_id() != rewrite.pinned_snapshot_id(base)?
        {
            return Err("MV replacement window differs from the original occurrence".into());
        }
        let schema = observe_source_schema(
            source.storage_observation(),
            &pinned.planning_pin,
            receipt.to_metadata(),
            context.clone(),
        )?;
        let planned = plan_prepared_source_partitions(
            &rewrite.mv_definition,
            occurrence,
            &schema,
            &pinned.planning_pin,
            &receipt,
            target_partition,
            Some(direct),
        )?;
        let AffectedTargetPartitions::Known { partitions } = planned else {
            return Ok(None);
        };
        affected.extend(partitions);
        let Some(fields) = exact_source_partition_fields(
            &rewrite.mv_definition,
            occurrence,
            &schema,
            target_partition,
            direct,
        )?
        else {
            return Ok(None);
        };
        let definition_occurrence = rewrite
            .mv_definition
            .facts
            .binding()
            .occurrences
            .iter()
            .find(|value| value.occurrence_id == occurrence)
            .ok_or("MV replacement lost its original definition occurrence")?;
        reads.push((
            base,
            &definition_occurrence.qualifier_at_binding,
            pinned,
            receipt,
            fields,
        ));
    }
    if affected.is_empty() {
        return Ok(None);
    }
    let keys: Arc<[Vec<ConnectorChangePartitionValue>]> = affected
        .iter()
        .map(|partition| {
            partition
                .fields
                .iter()
                .map(|field| match &field.value {
                    MvPartitionValue::Null => ConnectorChangePartitionValue::Null,
                    MvPartitionValue::String(value) => {
                        ConnectorChangePartitionValue::String(Arc::from(value.as_str()))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .into();
    let affected = AffectedTargetPartitions::Known {
        partitions: affected,
    };
    let Some(target) = target_partition_replacement_selection(rewrite, &affected)? else {
        return Ok(None);
    };
    let mut sources = HashMap::<TableIdentity, BTreeMap<_, ConnectorPreparedSourceRead>>::new();
    let mut selected = Vec::with_capacity(reads.len());
    let mut volumes = BTreeMap::new();
    for (base, qualifier, pinned, receipt, fields) in reads {
        let by_fields = sources.entry(pinned.table.clone()).or_default();
        let read = if let Some(existing) = by_fields.get(&fields) {
            existing.clone()
        } else {
            let predicate =
                ConnectorExactSourcePartitionSlice::try_new(fields.clone(), Arc::clone(&keys))?;
            let request = receipt.request();
            let request = ConnectorReadSourcePreparationRequest::snapshot(
                request.name().clone(),
                request.observed_table().clone(),
                request.observed_source_facts().clone(),
                request.object_id().clone(),
                request.selection().to_snapshot_id(),
                request.to_revision().clone(),
            )
            .with_exact_partition_slice(predicate)?;
            let read = io
                .source_read(source, &pinned.planning_pin, request, context.clone())
                .await?;
            if read.exact_partition_slice_volume().is_none() {
                return Ok(None);
            }
            by_fields.insert(fields, read.clone());
            read
        };
        let volume = read
            .exact_partition_slice_volume()
            .ok_or("provider did not report the exact MV replacement source volume")?;
        volumes.insert(base.occurrence_id, volume);
        selected.push(PreparedPartitionReplacementRead {
            occurrence: base.occurrence_id,
            table: base.table.clone(),
            qualifier: qualifier.clone(),
            source: pinned,
            receipt: read,
        });
    }
    Ok(Some(PreparedPartitionReplacement {
        affected,
        target,
        reads: PreparedPartitionReplacementReads(selected),
        volumes,
    }))
}
