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

//! Refresh-time schema and base-state observations over explicit Core ports.

use crate::mv::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation, MvStorageObservationPort,
};
use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorRequestContext, ConnectorTableResolution,
};

/// Loads the current schema facts used to validate a persisted MV contract.
pub(crate) fn observe_schema_validation_for_table(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    storage_observation
        .observe_schema_validation(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV schema validation facts for {}: {error}",
                table.fqn()
            )
        })
}

/// Loads the current base-table refresh facts without admitting query assembly
/// dependencies into refresh-domain planning.
pub(crate) fn observe_current_refresh_base(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvRefreshBaseObservation, String> {
    crate::mv::refresh_io::observe_current_refresh_base_with_ports(
        connector_control,
        storage_observation,
        table,
        connector_context,
    )
}
