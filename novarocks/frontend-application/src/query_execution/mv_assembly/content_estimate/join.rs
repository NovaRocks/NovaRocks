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

//! Obtain the two join endpoints through the original refresh preparation.

use std::{collections::BTreeSet, sync::Arc};

use novarocks_spi::connector::read_stack::{
    ConnectorReadSourcePreparationRequest, ConnectorReadSourceSelection,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorRequestContext, ConnectorTableMetadata, StatisticsMetric,
    StatisticsMetricRequest,
};
use novarocks_sql::planning::mv::{
    SqlMvContentJoinKey, SqlMvContentJoinSource, estimate_content_join_amplification,
};

use super::ContentJoinAmplification;
use crate::mv::domain::{
    iceberg_refresh::{IcebergMvCorePorts, join_base_refs_for_definition},
    refresh::{
        observation::validate_observed_schema_locator, planning::RefreshBaseRelationOccurrence,
    },
    rewrite::context::IcebergMvRewriteContext,
    storage_observation::{MvSourceSchemaObservation, observe_source_schema},
};
use crate::query_execution::mv_assembly::refresh_preparation::{
    async_facts::PreparationError, io_gate::MvRefreshPreparationIo,
};

pub(crate) async fn estimate_join_amplification(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    context: &ConnectorRequestContext,
    windows: &[ConnectorChangeWindowFacts],
) -> Result<ContentJoinAmplification, PreparationError> {
    use ContentJoinAmplification::{OptimizerEstimate, Unavailable};
    let Some(join) = rewrite.content_join_contract() else {
        return Ok(Unavailable);
    };
    let Ok((left, right)) = join_base_refs_for_definition(
        &rewrite.mv_definition,
        &rewrite.canonical_select_query,
        &rewrite.base_refs,
    ) else {
        return Ok(Unavailable);
    };
    let Some(left_events) = occurrence_rows(rewrite, windows, left) else {
        return Ok(Unavailable);
    };
    let Some(right_events) = occurrence_rows(rewrite, windows, right) else {
        return Ok(Unavailable);
    };
    let Some(key_ids) = join.content_key_field_pairs(left.occurrence_id, right.occurrence_id)
    else {
        return Ok(Unavailable);
    };

    let captured = io.captured_sources();
    let left_source = captured.require(&left.table)?;
    let left_metadata = left_source.metadata()?;
    let left_fields = observe_source_schema(
        source.storage_observation(),
        &left_source.planning_pin,
        &left_metadata,
        context.clone(),
    )?;
    validate_observed_schema_locator(left_fields.table(), &left_metadata.identity)?;
    if left_fields.selected_snapshot_id() != Some(rewrite.pinned_snapshot_id(left)?)
        || left_fields.table_object_id() != &rewrite.pinned_table_object_id(left)?
    {
        return Ok(Unavailable);
    }

    let right_source = captured.require(&right.table)?;
    let Some(right_metadata) = right_from_metadata(io, source, rewrite, context, right).await?
    else {
        return Ok(Unavailable);
    };
    let right_fields = observe_source_schema(
        source.storage_observation(),
        &right_source.planning_pin,
        &right_metadata,
        context.clone(),
    )?;
    validate_observed_schema_locator(right_fields.table(), &right_metadata.identity)?;
    if right_fields.selected_snapshot_id() != Some(rewrite.previous_snapshot_id(right)?)
        || right_fields.table_object_id() != &rewrite.pinned_table_object_id(right)?
    {
        return Ok(Unavailable);
    }

    let distinct = key_ids.into_iter().collect::<BTreeSet<_>>();
    let Some(keys) = distinct
        .into_iter()
        .map(|(left_id, right_id)| {
            Some(SqlMvContentJoinKey {
                left_to: field_name(&left_fields, left_id)?,
                right_from: field_name(&right_fields, right_id)?,
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(Unavailable);
    };

    let Some(left_evidence) = io
        .content_statistics(
            &left_source.planning_pin,
            &left_metadata,
            metrics(keys.iter().map(|key| key.left_to))?,
            context,
        )
        .await?
    else {
        return Ok(Unavailable);
    };
    let Some(right_evidence) = io
        .content_statistics(
            &right_source.planning_pin,
            &right_metadata,
            metrics(keys.iter().map(|key| key.right_from))?,
            context,
        )
        .await?
    else {
        return Ok(Unavailable);
    };

    Ok(estimate_content_join_amplification(
        SqlMvContentJoinSource {
            schema: left_metadata.schema(),
            evidence: &left_evidence,
        },
        SqlMvContentJoinSource {
            schema: right_metadata.schema(),
            evidence: &right_evidence,
        },
        &keys,
        left_events,
        right_events,
    )
    .map_or(Unavailable, OptimizerEstimate))
}

fn occurrence_rows(
    rewrite: &IcebergMvRewriteContext,
    windows: &[ConnectorChangeWindowFacts],
    base: &RefreshBaseRelationOccurrence,
) -> Option<u128> {
    if windows.len() != rewrite.base_refs.len() {
        return None;
    }
    let ordinal = rewrite
        .base_refs
        .iter()
        .position(|item| item.occurrence_id == base.occurrence_id)?;
    match windows.get(ordinal)? {
        ConnectorChangeWindowFacts::Difference { volume, .. } => {
            Some(volume.source_event_rows_upper_bound)
        }
        ConnectorChangeWindowFacts::NetZero(_) => Some(0),
        ConnectorChangeWindowFacts::NotIncremental(_) => None,
    }
}

fn field_name<'a>(schema: &'a MvSourceSchemaObservation, id: &[u8]) -> Option<&'a str> {
    schema
        .fields()
        .iter()
        .find(|field| field.field_id.as_bytes() == id)
        .map(|field| field.name.as_str())
}

fn metrics<'a>(
    columns: impl IntoIterator<Item = &'a str>,
) -> Result<StatisticsMetricRequest, novarocks_spi::connector::ConnectorError> {
    let metrics = std::iter::once(StatisticsMetric::RowCount)
        .chain(
            columns
                .into_iter()
                .map(|column| StatisticsMetric::ThetaNdv {
                    column: Arc::from(column),
                }),
        )
        .collect::<BTreeSet<_>>();
    StatisticsMetricRequest::try_new(metrics.into_iter().collect())
}

async fn right_from_metadata(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    context: &ConnectorRequestContext,
    right: &RefreshBaseRelationOccurrence,
) -> Result<Option<ConnectorTableMetadata>, PreparationError> {
    let captured = io.captured_sources().require(&right.table)?;
    let from = rewrite.previous_snapshot_id(right)?;
    let to = rewrite.pinned_snapshot_id(right)?;
    let revision = rewrite.previous_revision(right)?;
    let Some(window) = captured.receipt(Some(revision)) else {
        return Ok(None);
    };
    let original = window.request();
    let ConnectorReadSourceSelection::ChangeWindow(selection) = original.selection() else {
        return Ok(None);
    };
    if selection.from_snapshot_id() != from || selection.to_snapshot_id() != to {
        return Ok(None);
    }
    if from == to && original.from_revision() == Some(original.to_revision()) {
        return Ok(Some(window.to_metadata().clone()));
    }
    // The provider authors this snapshot's statistics version under the same
    // original table observation and operation scope.
    let request = ConnectorReadSourcePreparationRequest::snapshot(
        original.name().clone(),
        original.observed_table().clone(),
        original.observed_source_facts().clone(),
        original.object_id().clone(),
        from,
        revision.clone(),
    );
    let historical = io
        .source_read(source, &captured.planning_pin, request, context.clone())
        .await?;
    let retained = historical.request();
    if retained.selection() != ConnectorReadSourceSelection::Snapshot(from)
        || retained.to_revision() != revision
        || retained.object_id() != original.object_id()
    {
        return Ok(None);
    }
    Ok(Some(historical.to_metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_the_original_bound_column_names_and_row_count_once_per_pin() {
        let request = metrics(["left_id", "n", "left_id"]).unwrap();
        assert_eq!(
            request.metrics(),
            &[
                StatisticsMetric::RowCount,
                StatisticsMetric::ThetaNdv {
                    column: Arc::from("left_id")
                },
                StatisticsMetric::ThetaNdv {
                    column: Arc::from("n")
                },
            ],
        );
    }
}
