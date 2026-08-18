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

use crate::mv::analysis::rebind::rewrite_select_sql_for_rebind;
use crate::mv::persistence::definition::StoredMvDefinition;
use crate::mv::refresh::capabilities::RefreshCapabilities;
use crate::mv::refresh::snapshot::BaseSnapshotPolicy;
use crate::mv::refresh::target::IcebergMvTarget;
use crate::mv::schema_validation::{
    ContractDecision, JoinContractDecision, validate_join_schema_contract, validate_schema_contract,
};
use crate::mv::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation, MvStorageObservationPort,
};
use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorRequestContext, ConnectorTableResolution,
};

/// Loads the current schema facts used to validate a persisted MV contract.
pub fn observe_schema_validation_for_table(
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
pub fn observe_current_refresh_base(
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

/// Revalidates a persisted definition against the exact leaf observations
/// needed by refresh planning. This is MV-domain schema work; it deliberately
/// receives the individual observation ports rather than a query-assembly
/// source object.
pub fn rebind_mv_definition_before_refresh_derivation(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<StoredMvDefinition, String> {
    let Some(contract) = mv_definition.schema_contract.as_ref() else {
        return Ok(mv_definition.clone());
    };
    let caps = RefreshCapabilities::from_schema_contract(contract)?;
    let target_ref = TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    match caps.snapshot_policy {
        BaseSnapshotPolicy::SingleBase => {
            let [base_ref] = base_refs else {
                return Err("single-base MV refresh has an invalid base reference set".to_string());
            };
            let base_observation = observe_schema_validation_for_table(
                connector_control,
                storage_observation,
                base_ref,
                connector_context,
            )?;
            let loaded_target_observation;
            let target_observation = match retained_target_observation {
                Some(observation) => observation,
                None => {
                    loaded_target_observation = observe_schema_validation_for_table(
                        connector_control,
                        storage_observation,
                        &target_ref,
                        connector_context,
                    )?;
                    &loaded_target_observation
                }
            };
            match validate_schema_contract(contract, &base_observation, target_observation) {
                ContractDecision::Incompatible(error) => Err(error.to_string()),
                ContractDecision::CompatibleSafe => Ok(mv_definition.clone()),
                ContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
                    let mut definition = mv_definition.clone();
                    definition.select_sql =
                        rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
                    Ok(definition)
                }
            }
        }
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            let [left_ref, right_ref] = base_refs else {
                return Err("join MV refresh has an invalid base reference set".to_string());
            };
            let left_observation = observe_schema_validation_for_table(
                connector_control,
                storage_observation,
                left_ref,
                connector_context,
            )?;
            let right_observation = observe_schema_validation_for_table(
                connector_control,
                storage_observation,
                right_ref,
                connector_context,
            )?;
            let loaded_target_observation;
            let target_observation = match retained_target_observation {
                Some(observation) => observation,
                None => {
                    loaded_target_observation = observe_schema_validation_for_table(
                        connector_control,
                        storage_observation,
                        &target_ref,
                        connector_context,
                    )?;
                    &loaded_target_observation
                }
            };
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            match validate_join_schema_contract(
                contract,
                &[
                    (left_fqn.as_str(), left_observation),
                    (right_fqn.as_str(), right_observation),
                ],
                target_observation,
            )
            .map_err(|error| error.to_string())?
            {
                JoinContractDecision::CompatibleSafe => Ok(mv_definition.clone()),
                JoinContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
                    let mut definition = mv_definition.clone();
                    definition.select_sql =
                        rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
                    Ok(definition)
                }
            }
        }
        BaseSnapshotPolicy::AllBasesRequired => Ok(mv_definition.clone()),
    }
}
