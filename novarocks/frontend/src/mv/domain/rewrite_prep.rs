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

//! Application-side freezing for optional materialized-view rewrite.
//!
//! Repository enumeration and connector metadata reads belong here. The SQL
//! compiler receives the resulting immutable definition index and owns all
//! candidate parse/analyze/statistics/selection work.

use std::sync::Arc;

use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::refresh::definition::parse_mv_select_query;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::compiler::{
    MvRewriteDefinitionIndex, SqlMvRewriteBaseTableFacts, SqlMvRewriteDefinitionFacts,
    SqlMvRewritePublicationRelation, SqlMvRewriteSelectionFacts,
};

/// Freeze rewrite candidates from the caller's leaf ports.  The frozen index
/// remains request-local.
pub fn freeze_mv_rewrite_definition_index_with_ports(
    readiness: &MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
) -> Result<MvRewriteDefinitionIndex, String> {
    let definitions = readiness
        .list_ready_projections()
        .map_err(|error| format!("list mv definitions: {error}"))?;

    MvRewriteDefinitionIndex::try_new(
        definitions
            .into_iter()
            .map(|projection| {
                let definition = projection.definition;
                freeze_mv_rewrite_definition(connector_control, storage_observation, definition)
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
}

fn freeze_mv_rewrite_definition(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    definition: crate::mv::domain::persistence::definition::StoredMvDefinition,
) -> Result<SqlMvRewriteDefinitionFacts, String> {
    let selection =
        freeze_mv_rewrite_selection(connector_control, storage_observation, &definition);
    let mut base_table_states = std::collections::BTreeMap::new();
    if definition.storage_engine == "iceberg" {
        for fqn in &definition.base_table_refs {
            let state = freeze_base_table_state(connector_control, storage_observation, fqn)
                .unwrap_or_else(SqlMvRewriteBaseTableFacts::unavailable);
            base_table_states.insert(fqn.clone(), state);
        }
    }

    let facts = SqlMvRewriteDefinitionFacts::try_new(
        definition.mv_id,
        parse_mv_select_query(&definition.query_definition.raw_query_source)?,
        definition.base_table_refs,
        definition.storage_engine,
        definition.target_catalog,
        definition.target_namespace,
        definition.target_table,
        definition.last_refresh_snapshots,
        definition.last_refresh_table_object_ids,
        base_table_states,
    )?;
    match selection {
        Ok(selection) => Ok(facts.with_selection_facts(selection)),
        Err(error) => facts.with_selection_unavailable(error),
    }
}

fn freeze_mv_rewrite_selection(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    definition: &crate::mv::domain::persistence::definition::StoredMvDefinition,
) -> Result<SqlMvRewriteSelectionFacts, String> {
    let (Some(catalog), Some(namespace), Some(table)) = (
        definition.target_catalog.as_deref(),
        definition.target_namespace.as_deref(),
        definition.target_table.as_deref(),
    ) else {
        return Err("MV rewrite target identity is incomplete".to_string());
    };
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let lease = crate::connector::acquire_metadata_planning_lease(connector_control, catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &lease,
        context.clone(),
        namespace,
        table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let package = storage_observation
        .observe_lake_package(&lease, &metadata, context)
        .map_err(|error| format!("observe MV rewrite publication: {error}"))?
        .ok_or_else(|| "MV rewrite target has no lake publication package".to_string())?;
    let novarocks_spi::connector::MvLakePublicationObservation::Published(publication) =
        package.publication()
    else {
        return Err("MV rewrite target has no published version".to_string());
    };
    if package
        .current_target_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
        != Some(publication.target_snapshot_id)
    {
        return Err("MV rewrite publication is not the current target snapshot".to_string());
    }
    let expected_fingerprint = crate::mv::domain::refresh::definition::mv_definition_fingerprint(
        &definition.query_definition.raw_query_source,
    );
    if publication.definition_fingerprint != expected_fingerprint {
        return Err("MV rewrite publication definition fingerprint is stale".to_string());
    }
    let fingerprint = hex::decode(&publication.definition_fingerprint)
        .map_err(|error| format!("decode MV rewrite definition fingerprint: {error}"))?;
    let fingerprint: [u8; 32] = fingerprint
        .try_into()
        .map_err(|_| "MV rewrite definition fingerprint must be 32 bytes".to_string())?;
    let provider = lease.binding().descriptor().provider_id.clone();
    let publication_inputs = publication
        .bases
        .iter()
        .map(|base| {
            Ok(SqlMvRewritePublicationRelation::new(
                base.table_fqn.clone(),
                novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                    provider.clone(),
                    &base.object_id,
                    Some(base.to_snapshot),
                )
                .map_err(|error| format!("freeze MV rewrite base revision: {error}"))?,
            )?)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let publication_target = SqlMvRewritePublicationRelation::new(
        format!("{catalog}.{namespace}.{table}"),
        novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider,
            package.target_object_id(),
            Some(publication.target_snapshot_id),
        )
        .map_err(|error| format!("freeze MV rewrite target revision: {error}"))?,
    )?;
    SqlMvRewriteSelectionFacts::try_new_with_publication(
        *publication.publication_id.as_uuid().as_bytes(),
        fingerprint,
        publication_inputs,
        publication_target,
    )
}

fn freeze_base_table_state(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    fqn: &str,
) -> Result<SqlMvRewriteBaseTableFacts, String> {
    let table_ref =
        crate::mv::domain::refresh::definition::parse_iceberg_table_refs(&[fqn.to_string()])?
            .into_iter()
            .next()
            .expect("one table reference produces one parsed identity");
    let connector_context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table_ref.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table_ref.namespace,
        &table_ref.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let _schema_observation = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &exact_lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(|error| format!("observe MV rewrite storage facts for {fqn}: {error}"))?;
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&table_ref.catalog)
        .map_err(|error| format!("parse rewrite base connector instance for {fqn}: {error}"))?;
    let captured = exact_lease
        .binding()
        .metadata()
        .capture_table_object_binding(
            novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from(table_ref.namespace.as_str()),
                    table: Arc::from(table_ref.table.as_str()),
                },
                resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                context: connector_context.clone(),
            },
        )
        .map_err(|error| format!("capture MV rewrite object ID for {fqn}: {error}"))?;
    let reference_facts = crate::connector::metadata_read_reference_facts_with_planning_lease(
        exact_lease,
        connector_context,
        &table_ref.namespace,
        &table_ref.table,
    )?;
    Ok(SqlMvRewriteBaseTableFacts::resolved(
        reference_facts.current_snapshot_id(),
        Some(captured.object_id),
    ))
}
