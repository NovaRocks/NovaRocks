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

use crate::mv::repository::MvRepository;
use crate::mv::storage_observation::MvStorageObservationPort;
use crate::sql::compiler::mv_rewrite::{
    MvRewriteBaseTableState, MvRewriteDefinition, MvRewriteDefinitionIndex,
};

use super::StandaloneState;

/// Freeze repository order and every freshness observation once per request.
/// The compiler never observes repository or connector changes while it is
/// deciding optional rewrite candidates.
pub(crate) fn freeze_mv_rewrite_definition_index(
    state: &Arc<StandaloneState>,
) -> Result<MvRewriteDefinitionIndex, String> {
    freeze_mv_rewrite_definition_index_with_ports(
        state.mv_repository.as_ref(),
        state.connector_control.as_ref(),
        state.mv_storage_observation.as_ref(),
    )
}

/// Freeze rewrite candidates through the explicit MV kernel.  The frozen
/// index remains request-local; the kernel only supplies its leaf ports.
pub(crate) fn freeze_mv_rewrite_definition_index_with_kernel(
    kernel: &crate::engine::domain::MvExecutionKernel,
) -> Result<MvRewriteDefinitionIndex, String> {
    freeze_mv_rewrite_definition_index_with_ports(
        kernel.repository().as_ref(),
        kernel.connector_control().as_ref(),
        kernel.storage_observation().as_ref(),
    )
}

pub(crate) fn freeze_mv_rewrite_definition_index_with_ports(
    repository: &dyn MvRepository,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
) -> Result<MvRewriteDefinitionIndex, String> {
    let definitions = repository
        .list_definitions()
        .map_err(|error| format!("list mv definitions: {error}"))?;

    Ok(MvRewriteDefinitionIndex::new(
        definitions
            .into_iter()
            .map(|definition| {
                freeze_mv_rewrite_definition(connector_control, storage_observation, definition)
            })
            .collect(),
    ))
}

fn freeze_mv_rewrite_definition(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    definition: crate::mv::persistence::definition::StoredMvDefinition,
) -> MvRewriteDefinition {
    let mut base_table_states = std::collections::BTreeMap::new();
    if definition.storage_engine == "iceberg" {
        for fqn in &definition.base_table_refs {
            let state = freeze_base_table_state(connector_control, storage_observation, fqn)
                .unwrap_or_else(MvRewriteBaseTableState::Unavailable);
            base_table_states.insert(fqn.clone(), state);
        }
    }

    MvRewriteDefinition {
        mv_id: definition.mv_id,
        select_sql: definition.select_sql,
        base_table_refs: definition.base_table_refs,
        storage_engine: definition.storage_engine,
        target_catalog: definition.target_catalog,
        target_namespace: definition.target_namespace,
        target_table: definition.target_table,
        last_refresh_snapshots: definition.last_refresh_snapshots,
        last_refresh_table_uuids: definition.last_refresh_table_uuids,
        base_table_states,
    }
}

fn freeze_base_table_state(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    fqn: &str,
) -> Result<MvRewriteBaseTableState, String> {
    let table_ref = crate::engine::mv::refresh_io::parse_iceberg_table_refs(&[fqn.to_string()])?
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
    let schema_observation = storage_observation
        .observe_schema_validation(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| format!("observe MV rewrite storage facts for {fqn}: {error}"))?;
    let reference_facts = crate::connector::metadata_read_reference_facts_with_planning_lease(
        exact_lease,
        connector_context,
        &table_ref.namespace,
        &table_ref.table,
    )?;
    Ok(MvRewriteBaseTableState::Resolved {
        snapshot_id: reference_facts.current_snapshot_id(),
        table_uuid: Some(schema_observation.table_uuid().to_string()),
    })
}
