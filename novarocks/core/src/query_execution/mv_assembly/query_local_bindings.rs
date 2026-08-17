// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Request-local IMV table bindings assembled from already captured facts.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};

use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::query_execution::planning::bindings::QueryScanMaterialization;
use crate::query_execution::planning::bindings::QueryTableBindingKey;
use crate::query_execution::planning::catalog_materializer::{
    QueryLocalTableOverlay, admit_connector_change_window,
    connector_query_binding_from_materialization,
};

/// Materialize every pinned IMV base immediately after capture. The returned
/// overlays retain the exact connector lease, table handle, selected files and
/// delta facts; callers must carry them through later compilation instead of
/// asking a provider for its current generation again.
pub(crate) fn freeze_imv_base_query_local_overlays_from_captured_inputs(
    connector_control: &dyn ConnectorControlRegistry,
    connector_context: &ConnectorRequestContext,
    base_refs: &[TableIdentity],
    pin: &RefreshSnapshotPin,
    previous_snapshot_ids: &BTreeMap<String, i64>,
) -> Result<Vec<QueryLocalTableOverlay>, String> {
    let mut seen = BTreeSet::new();
    let mut overlays = Vec::with_capacity(base_refs.len());
    for base in base_refs {
        let snapshot_id = pin.get(base).ok_or_else(|| {
            format!(
                "IMV query binding is missing snapshot pin for {}",
                base.fqn()
            )
        })?;
        let identity = format!(
            "{}.{}.{}@{}",
            base.catalog.to_ascii_lowercase(),
            base.namespace.to_ascii_lowercase(),
            base.table.to_ascii_lowercase(),
            snapshot_id
        );
        if !seen.insert(identity) {
            continue;
        }
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&base.catalog)
            .map_err(|error| error.to_string())?;
        let planning_lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            connector_control,
            &instance_id,
        )
        .map_err(|error| error.to_string())?;
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
            &planning_lease,
            connector_context.clone(),
            &base.namespace,
            &base.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?;
        let mut materialization = crate::catalog_application::query_catalog::connector_table_materialization_from_metadata(
            metadata,
            planning_lease,
        )?;
        materialization.read_selector =
            novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        let mut frozen_snapshot_ids = BTreeSet::from([snapshot_id]);
        let mut admitted_change_scans = BTreeMap::new();
        if let Some(previous_snapshot_id) = previous_snapshot_ids.get(&base.fqn()) {
            frozen_snapshot_ids.insert(*previous_snapshot_id);
            let window = novarocks_spi::connector::ConnectorChangeWindow::new(
                *previous_snapshot_id,
                snapshot_id,
            );
            let admitted_scan = admit_connector_change_window(
                &materialization.read_table,
                &materialization.read_schema,
                &materialization.planning_lease,
                connector_context.clone(),
                window,
            )?;
            admitted_change_scans.insert((*previous_snapshot_id, snapshot_id), admitted_scan);
        }

        let catalog = base.catalog.clone();
        let namespace = base.namespace.clone();
        let table = base.table.clone();
        let key = QueryTableBindingKey::snapshot(&catalog, &namespace, &table, snapshot_id);
        overlays.push(QueryLocalTableOverlay::new(
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
                result.admitted_change_scans = admitted_change_scans.clone();
                for frozen_snapshot_id in frozen_snapshot_ids.iter().copied() {
                    result.frozen_snapshot_materializations.insert(
                        frozen_snapshot_id,
                        QueryScanMaterialization {
                            table: materialization.read_table.clone(),
                            schema: materialization.read_schema.clone(),
                            selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(
                                frozen_snapshot_id,
                            ),
                            statistics_pin: materialization.statistics_pin.clone(),
                            planning_lease: materialization.planning_lease.clone(),
                        },
                    );
                }
                Ok(result)
            },
        ));
    }
    Ok(overlays)
}
