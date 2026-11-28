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

//! Compare qualified refresh candidates using the original frozen scan inputs.

use std::{collections::HashMap, sync::Arc};

use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorRequestContext, ConnectorTableMetadata,
    StatisticsEvidence, StatisticsMetric, StatisticsMetricRequest, StatisticsMetricSource,
    StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature,
    read_stack::ConnectorReadSourcePreparationRequest,
};
use novarocks_sql::{
    binding::SqlTableBindingId,
    compiler::{
        SessionOptimizerSettings, SqlImvPlanningInput, SqlImvRewriteValidation, SqlSessionContext,
    },
    planning::{
        dml::DmlStatisticsSnapshot,
        mv::{
            SqlMvCostScanVersion as Version, SqlMvCostScanVolume as Volume, SqlMvRefreshCostInput,
            cost_mv_refresh_candidate,
        },
    },
};

use super::{
    partition_replace::PreparedPartitionReplacement,
    query_local_bindings::{
        bind_imv_target_query_table_from_metadata, freeze_captured_source_overlays,
    },
    refresh_preparation::{
        async_facts::PreparationError,
        async_original::{MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog},
        io_gate::MvRefreshPreparationIo,
    },
};
use crate::{
    catalog_application::query_bindings::{QueryPreparedSourceSelector, QueryTableBindingStore},
    mv::domain::{
        iceberg_refresh::IcebergMvCorePorts,
        refresh::{strategy::RefreshStrategy, target_binding::MvTargetBinding},
        rewrite::context::IcebergMvRewriteContext,
    },
};

pub(crate) async fn cost_candidate(
    io: &MvRefreshPreparationIo,
    ports: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target: &MvTargetBinding,
    context: &ConnectorRequestContext,
    settings: &SessionOptimizerSettings,
    strategy: &RefreshStrategy,
    partition: Option<&PreparedPartitionReplacement>,
) -> Result<f64, PreparationError> {
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let sources = io.captured_sources();
    if let Some(views) = sources.views() {
        bindings.retain_statement_views(views).await;
    }
    let partition_bindings = partition
        .map(|proof| proof.reads.register(&bindings))
        .transpose()?;
    let overlays = freeze_captured_source_overlays(&sources)?;
    let query = Box::new((*rewrite.canonical_select_query).clone());
    let control =
        crate::query_execution::planning::sql_compile_control_from_connector_request(context);
    let imv = if matches!(
        strategy,
        RefreshStrategy::AppendOnly | RefreshStrategy::ContentLocate(_)
    ) {
        let content = matches!(strategy, RefreshStrategy::ContentLocate(_));
        let target_binding = bind_imv_target_query_table_from_metadata(
            rewrite,
            &bindings,
            target.planning_pin().clone(),
            target.metadata().clone(),
            None,
            content,
        )?;
        let snapshot = if content {
            rewrite.to_sql_content_rewrite_snapshot(target_binding, target.metadata())?
        } else {
            rewrite.to_sql_rewrite_snapshot(target_binding)?
        };
        let input = SqlImvPlanningInput::new(snapshot, SqlImvRewriteValidation::None);
        Some((
            target_binding,
            if content {
                input.for_content_location()
            } else {
                input.for_append_only()
            },
        ))
    } else {
        None
    };
    let capture = capture_catalog(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(query.clone()),
            ports: ports.clone(),
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            context: context.clone(),
            control: control.clone(),
            bindings: bindings.clone(),
            overlays,
        },
        io,
    )
    .await?;

    let mut volumes = HashMap::new();
    if let (Some(proof), Some(bound)) = (partition, partition_bindings.as_deref()) {
        for source in bound {
            let slice = proof
                .volumes
                .get(&source.occurrence())
                .ok_or("MV scoped cost lost its exact source slice volume")?;
            let volume = ScanVolume::from_physical(slice.record_count, slice.file_size_in_bytes)?;
            register_volume(
                &mut volumes,
                source.binding(),
                Version::Snapshot(source.snapshot_id()),
                volume,
            )?;
        }
    } else {
        for (binding, entry) in bindings.captured_bindings() {
            if imv.as_ref().is_some_and(|(id, _)| *id == binding) {
                if matches!(strategy, RefreshStrategy::ContentLocate(_)) {
                    let snapshot = rewrite
                        .target_snapshot_id
                        .ok_or("MV cost lost its target snapshot")?;
                    let volume =
                        manifest_volume(io, target.planning_pin(), target.metadata(), context)
                            .await?;
                    register_volume(&mut volumes, binding, Version::Snapshot(snapshot), volume)?;
                }
                continue;
            }
            let Some(materialization) = entry.scan_materialization.as_ref() else {
                continue;
            };
            let Some(metadata) = entry.source_metadata.as_ref() else {
                continue;
            };
            let to = manifest_volume(io, &materialization.planning_pin, metadata, context).await?;
            let version = match materialization.selector {
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(id) => {
                    Version::Snapshot(id)
                }
                novarocks_spi::connector::ConnectorReadSelector::Current => Version::Current,
                _ => return Err("MV optimizer cost received an unsupported source selector".into()),
            };
            register_volume(&mut volumes, binding, version, to)?;
            // The original query can resolve a request-local Current alias of the
            // same snapshot. Its prepared binding still carries the pinned To.
            register_volume(&mut volumes, binding, Version::Current, to)?;
            if imv.is_some() {
                for (&selector, read) in &entry.prepared_source_reads {
                    let QueryPreparedSourceSelector::Delta {
                        from_exclusive,
                        to_inclusive,
                    } = selector
                    else {
                        continue;
                    };
                    let volume = delta_volume(&read.receipt)?;
                    register_volume(
                        &mut volumes,
                        binding,
                        Version::Delta {
                            from: from_exclusive,
                            to: to_inclusive,
                        },
                        volume,
                    )?;
                    if read.receipt.window_facts().is_some_and(|facts| {
                        facts.permits(
                    novarocks_spi::connector::read_stack::ConnectorReadSourceProjection::From,
                )
                    }) {
                        let original = read.receipt.request();
                        let revision = original
                            .from_revision()
                            .ok_or("MV cost lost its original From revision")?;
                        let request = ConnectorReadSourcePreparationRequest::snapshot(
                            original.name().clone(),
                            original.observed_table().clone(),
                            original.observed_source_facts().clone(),
                            original.object_id().clone(),
                            from_exclusive,
                            revision.clone(),
                        );
                        let retained = io
                            .source_read(ports, &read.planning, request, context.clone())
                            .await?;
                        if retained.request().selection().to_snapshot_id() != from_exclusive
                            || retained.request().to_revision() != revision
                            || retained.request().object_id() != original.object_id()
                        {
                            return Err("MV cost observed a different historical source".into());
                        }
                        let volume =
                            manifest_volume(io, &read.planning, retained.to_metadata(), context)
                                .await?;
                        register_volume(
                            &mut volumes,
                            binding,
                            Version::Snapshot(from_exclusive),
                            volume,
                        )?;
                    }
                }
            }
        }
    }
    let statistics =
        DmlStatisticsSnapshot::empty().with_mv_scan_volumes(volumes.into_values().collect())?;
    cost_mv_refresh_candidate(SqlMvRefreshCostInput {
        query,
        rewrite: imv.as_ref().map(|(_, plan)| plan),
        partition_sources: partition_bindings.as_deref(),
        session: SqlSessionContext {
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            optimizer_settings: settings.clone(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        },
        catalog: &capture.catalog,
        functions: ports.function_catalog().as_ref(),
        sealed_functions: Arc::clone(ports.sealed_functions()),
        constant_policy: ports.constant_policy(),
        statistics,
        control,
    })
    .map_err(|error| {
        PreparationError(crate::mv::domain::application::MvApplicationError::from_compile(error))
    })
}

pub(crate) fn prepared_delta_volumes(
    store: &QueryTableBindingStore,
) -> Result<Vec<Volume>, String> {
    let mut volumes = HashMap::new();
    for (binding, entry) in store.captured_bindings() {
        for (&selector, read) in &entry.prepared_source_reads {
            if let QueryPreparedSourceSelector::Delta {
                from_exclusive,
                to_inclusive,
            } = selector
            {
                register_volume(
                    &mut volumes,
                    binding,
                    Version::Delta {
                        from: from_exclusive,
                        to: to_inclusive,
                    },
                    delta_volume(&read.receipt)?,
                )?;
            }
        }
    }
    Ok(volumes.into_values().collect())
}

fn delta_volume(
    receipt: &novarocks_spi::connector::read_stack::ConnectorPreparedSourceRead,
) -> Result<ScanVolume, String> {
    use novarocks_spi::connector::ConnectorChangeWindowFacts;
    match receipt.window_facts() {
        Some(ConnectorChangeWindowFacts::NetZero(_)) => Ok(ScanVolume {
            rows: 0,
            bytes: 0,
            exact_rows: true,
        }),
        Some(ConnectorChangeWindowFacts::Difference { volume, .. }) => ScanVolume::from_physical(
            volume.source_event_rows_upper_bound,
            volume.source_data_file_bytes,
        ),
        _ => Err("MV Delta cost has no original window volume".into()),
    }
}

#[derive(Clone, Copy)]
struct ScanVolume {
    rows: u64,
    bytes: u128,
    exact_rows: bool,
}

impl ScanVolume {
    fn from_physical(rows: u128, bytes: u128) -> Result<Self, String> {
        Ok(Self {
            rows: u64::try_from(rows)
                .map_err(|_| "MV scan row estimate exceeds the optimizer row domain")?,
            bytes,
            exact_rows: rows == 0,
        })
    }
}

fn register_volume(
    volumes: &mut HashMap<(SqlTableBindingId, Version), Volume>,
    binding: SqlTableBindingId,
    version: Version,
    volume: ScanVolume,
) -> Result<(), String> {
    let volume = Volume {
        binding,
        version,
        rows: volume.rows,
        bytes: volume.bytes,
        exact_rows: volume.exact_rows,
    };
    match volumes.entry((binding, version)) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(volume);
        }
        std::collections::hash_map::Entry::Occupied(entry) if entry.get() != &volume => {
            return Err(
                "MV optimizer observed conflicting volume for an exact scan selector".into(),
            );
        }
        _ => {}
    }
    Ok(())
}

async fn manifest_volume(
    io: &MvRefreshPreparationIo,
    planning: &ConnectorControlPlanningPin,
    metadata: &ConnectorTableMetadata,
    context: &ConnectorRequestContext,
) -> Result<ScanVolume, PreparationError> {
    let evidence = io
        .content_statistics(
            planning,
            metadata,
            StatisticsMetricRequest::try_new(vec![
                StatisticsMetric::RowCount,
                StatisticsMetric::DataFileBytes,
            ])?,
            context,
        )
        .await?
        .ok_or("MV cost has no signed Iceberg manifest scan volume")?;
    if metadata.statistics_data_version.as_ref() != Some(evidence.data_version()) {
        return Err("MV manifest scan volume describes a different physical version".into());
    }
    let (rows, exact_rows) = manifest_count(&evidence, &StatisticsMetric::RowCount, true)?;
    let (bytes, _) = manifest_count(&evidence, &StatisticsMetric::DataFileBytes, false)?;
    Ok(ScanVolume {
        rows,
        bytes: u128::from(bytes),
        exact_rows,
    })
}

fn manifest_count(
    evidence: &StatisticsEvidence,
    metric: &StatisticsMetric,
    upper_bound: bool,
) -> Result<(u64, bool), String> {
    if let Some(StatisticsMetricState::Available(observed)) = evidence.metrics().get(metric)
        && observed.describes_queried_rows()
        && observed.basis_version() == evidence.data_version()
        && observed.source() == &StatisticsMetricSource::CurrentManifest
        && let StatisticsMetricValue::U64(count) = observed.value()
        && (observed.numeric_nature() == StatisticsNumericNature::Exact
            || (upper_bound && observed.numeric_nature() == StatisticsNumericNature::UpperBound))
    {
        return Ok((
            *count,
            observed.numeric_nature() == StatisticsNumericNature::Exact,
        ));
    }
    Err(format!(
        "MV scan requires the signed manifest metric {metric:?}"
    ))
}
