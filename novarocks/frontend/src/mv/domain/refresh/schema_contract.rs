// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Refresh-time schema-contract validation over explicit observation leaves.

use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::persistence::schema::MvSchemaContract;
use crate::mv::domain::refresh::observation::observe_schema_validation_for_table;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::schema_validation::{
    ContractDecision, JoinContractDecision, validate_join_schema_contract, validate_schema_contract,
};
use crate::mv::domain::storage_observation::{
    MvSchemaValidationObservation, MvStorageObservationPort,
};
use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorRequestContext};

/// Validates the persisted aggregate metadata before any refresh planning or
/// query assembly derives an aggregate refresh path.
pub fn validate_aggregate_schema_contract_metadata<'a>(
    target: &IcebergMvTarget,
    mv_definition: &'a StoredMvDefinition,
) -> Result<&'a MvSchemaContract, String> {
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.contract_version != 3 {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} requires schema contract version 3, got {}",
            target.catalog, target.namespace, target.table, schema_contract.contract_version
        ));
    }
    if schema_contract.aggregate.is_none() {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} is missing aggregate schema contract; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(schema_contract)
}

pub(crate) fn validate_aggregate_schema_contract_for_base(
    schema_contract: &MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let mut base_contract = schema_contract.clone();
    if !schema_contract.bases.is_empty() {
        base_contract.base = schema_contract
            .bases
            .iter()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "iceberg aggregate-over-UNION-ALL MV schema contract missing base {}; recreate the MV",
                    base_ref.fqn()
                )
            })?;
    } else if !schema_contract
        .base
        .table_fqn
        .eq_ignore_ascii_case(&base_ref.fqn())
    {
        return Err(format!(
            "iceberg aggregate-over-UNION-ALL MV schema contract missing base {}; recreate the MV",
            base_ref.fqn()
        ));
    }
    match validate_schema_contract(&base_contract, base_observation, target_observation) {
        ContractDecision::Incompatible(err) => Err(err.to_string()),
        ContractDecision::CompatibleSafe => Ok(()),
        ContractDecision::CompatibleSafeWithRebind { .. } => Err(format!(
            "iceberg aggregate-over-UNION-ALL MV requires schema rebind for base {}, which is not supported for fan-in aggregate refresh; rebuild or recreate the MV",
            base_ref.fqn()
        )),
    }
}

pub fn validate_repartition_schema_contract(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    schema_contract: &MvSchemaContract,
    base_refs: &[TableIdentity],
    target_observation: &MvSchemaValidationObservation,
    connector_context: &ConnectorRequestContext,
) -> Result<(), String> {
    if schema_contract.join.is_some() {
        let [left_ref, right_ref] = base_refs else {
            return Err(format!(
                "Iceberg join MV repartition schema contract requires exactly two base tables, got {}",
                base_refs.len()
            ));
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
        let left_fqn = left_ref.fqn();
        let right_fqn = right_ref.fqn();
        match validate_join_schema_contract(
            schema_contract,
            &[
                (left_fqn.as_str(), left_observation),
                (right_fqn.as_str(), right_observation),
            ],
            target_observation,
        )
        .map_err(|error| error.to_string())?
        {
            JoinContractDecision::CompatibleSafe => {}
            JoinContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
                if schema_contract.aggregate.is_some() {
                    return Err(format!(
                        "iceberg join aggregate MV repartition requires schema rebind for {rebound_columns:?}, which is not supported during repartition; recreate the MV"
                    ));
                }
            }
        }
        return Ok(());
    }

    if !schema_contract.bases.is_empty() {
        for base_ref in base_refs {
            let base_observation = observe_schema_validation_for_table(
                connector_control,
                storage_observation,
                base_ref,
                connector_context,
            )?;
            if schema_contract.aggregate.is_some() {
                validate_aggregate_repartition_schema_contract_for_base(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    target_observation,
                )?;
            } else {
                validate_repartition_base_schema_contract(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    target_observation,
                )?;
            }
        }
        return Ok(());
    }

    let [base_ref] = base_refs else {
        return Err(format!(
            "ALTER MATERIALIZED VIEW ... REPARTITION single-base schema contract requires exactly one base table, got {}",
            base_refs.len()
        ));
    };
    let base_observation = observe_schema_validation_for_table(
        connector_control,
        storage_observation,
        base_ref,
        connector_context,
    )?;
    if schema_contract.aggregate.is_some() {
        validate_aggregate_repartition_schema_contract_for_base(
            schema_contract,
            base_ref,
            &base_observation,
            target_observation,
        )
    } else {
        match validate_schema_contract(schema_contract, &base_observation, target_observation) {
            ContractDecision::Incompatible(error) => Err(error.to_string()),
            ContractDecision::CompatibleSafe
            | ContractDecision::CompatibleSafeWithRebind { .. } => Ok(()),
        }
    }
}

fn validate_aggregate_repartition_schema_contract_for_base(
    schema_contract: &MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    validate_aggregate_schema_contract_for_base(
        schema_contract,
        base_ref,
        base_observation,
        target_observation,
    )
    .map_err(|err| {
        if err.contains("requires schema rebind") {
            format!(
                "iceberg aggregate MV repartition requires schema rebind for base {}, which is not supported during repartition; recreate the MV",
                base_ref.fqn()
            )
        } else {
            err
        }
    })
}

fn validate_repartition_base_schema_contract(
    schema_contract: &MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let mut base_contract = schema_contract.clone();
    base_contract.base = schema_contract
        .bases
        .iter()
        .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
        .cloned()
        .ok_or_else(|| {
            format!(
                "Iceberg MV repartition schema contract missing base {}; recreate the MV",
                base_ref.fqn()
            )
        })?;
    match validate_schema_contract(&base_contract, base_observation, target_observation) {
        ContractDecision::Incompatible(err) => Err(err.to_string()),
        ContractDecision::CompatibleSafe | ContractDecision::CompatibleSafeWithRebind { .. } => {
            Ok(())
        }
    }
}
