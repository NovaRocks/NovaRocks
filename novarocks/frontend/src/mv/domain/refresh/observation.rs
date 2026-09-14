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

//! Refresh-time schema and base-state observations over explicit Core ports.

use crate::mv::domain::analysis::{
    canonicalize_iceberg_mv_select_query, rebind::rewrite_select_sql_for_rebind,
};
use crate::mv::domain::refresh::capabilities::RefreshCapabilities;
use crate::mv::domain::refresh::definition::parse_mv_select_query;
use crate::mv::domain::refresh::snapshot::BaseSnapshotPolicy;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::schema_validation::{
    ContractDecision, JoinContractDecision, validate_join_schema_contract, validate_schema_contract,
};
use crate::mv::domain::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation,
};
use novarocks_mv_application::persistence::definition::StoredMvDefinition;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
};
use novarocks_types::naming::TableIdentity;
use std::sync::Arc;

fn derive_rebind_query_source(
    query_definition: &novarocks_query_application::persisted_query_definition::PersistedQueryDefinition,
) -> Result<String, String> {
    let query = parse_mv_select_query(&query_definition.raw_query_source)?;
    Ok(novarocks_parser::printer::print_query(
        &canonicalize_iceberg_mv_select_query(
            &query,
            Some(query_definition.resolution.default_catalog.as_str()),
            &query_definition.resolution.default_database,
        ),
    ))
}

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
    let observation = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &exact_lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(|error| {
        format!(
            "observe MV schema validation facts for {}: {error}",
            table.fqn()
        )
    })?;
    let instance_id = ConnectorInstanceId::parse(&table.catalog)
        .map_err(|error| format!("parse connector instance for {}: {error}", table.fqn()))?;
    let captured = exact_lease
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(table.namespace.as_str()),
                table: Arc::from(table.table.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: connector_context.clone(),
        })
        .map_err(|error| {
            format!(
                "capture MV table object binding for {}: {error}",
                table.fqn()
            )
        })?;
    Ok(observation.with_table_object_id(captured.object_id))
}

/// Loads the current base-table refresh facts without admitting query assembly
/// dependencies into refresh-domain planning.
pub(crate) fn observe_current_refresh_base(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvRefreshBaseObservation, String> {
    crate::mv::domain::refresh_io::observe_current_refresh_base_with_ports(
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
pub(crate) fn rebind_mv_definition_before_refresh_derivation(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<(StoredMvDefinition, String), String> {
    let Some(contract) = mv_definition.schema_contract.as_ref() else {
        return Ok((
            mv_definition.clone(),
            mv_definition.query_definition.raw_query_source.clone(),
        ));
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
                ContractDecision::CompatibleSafe => Ok((
                    mv_definition.clone(),
                    mv_definition.query_definition.raw_query_source.clone(),
                )),
                ContractDecision::CompatibleSafeWithRebind { rebound_columns } => Ok((
                    mv_definition.clone(),
                    rewrite_select_sql_for_rebind(
                        &derive_rebind_query_source(&mv_definition.query_definition)?,
                        &rebound_columns,
                    )?,
                )),
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
                JoinContractDecision::CompatibleSafe => Ok((
                    mv_definition.clone(),
                    mv_definition.query_definition.raw_query_source.clone(),
                )),
                JoinContractDecision::CompatibleSafeWithRebind { rebound_columns } => Ok((
                    mv_definition.clone(),
                    rewrite_select_sql_for_rebind(
                        &derive_rebind_query_source(&mv_definition.query_definition)?,
                        &rebound_columns,
                    )?,
                )),
            }
        }
        BaseSnapshotPolicy::AllBasesRequired => Ok((
            mv_definition.clone(),
            mv_definition.query_definition.raw_query_source.clone(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::analysis::rebind::RebindColumn;
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };

    #[test]
    fn rebind_source_uses_frozen_context_without_rewriting_raw_definition() {
        let raw = "SELECT d.region, SUM(f.amount) AS total FROM fact AS f JOIN dim AS d ON f.dim_id = d.id GROUP BY d.region";
        let definition =
            PersistedQueryDefinition::new(raw, PersistedQueryDialect::StarRocks, "ice", "sales")
                .expect("definition should be valid");

        let derived = derive_rebind_query_source(&definition).expect("derive rebind source");
        assert!(derived.contains("ice.sales.fact AS f"), "{derived}");
        assert!(derived.contains("ice.sales.dim AS d"), "{derived}");
        assert_eq!(definition.raw_query_source, raw);

        let rewritten = rewrite_select_sql_for_rebind(
            &derived,
            &[
                RebindColumn {
                    base_table_fqn: "ice.sales.fact".to_string(),
                    field_id: 2,
                    name_at_create: "dim_id".to_string(),
                    current_name: "new_dim_id".to_string(),
                },
                RebindColumn {
                    base_table_fqn: "ice.sales.dim".to_string(),
                    field_id: 3,
                    name_at_create: "region".to_string(),
                    current_name: "area".to_string(),
                },
            ],
        )
        .expect("qualified derived source should rebind");
        assert!(rewritten.contains("f.new_dim_id"), "{rewritten}");
        assert!(rewritten.contains("d.area AS region"), "{rewritten}");
    }
}
