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

//! Refresh-time Iceberg MV schema contract validator.
//!
//! Canonical entry points:
//!   - `validate_schema_contract` validates single-base identity, partition, and schema contracts.
//!   - `validate_join_schema_contract` validates two-base identity and rebind contracts before
//!     applying the generic target and partition checks.
//!   - `validate_branch_id_field` validates the live branch-id field contract.
//!
//! Decisions are explicit. There is NO fallback path: incompatible
//! contracts result in fail-fast errors that propagate to the user.

use novarocks_sql::planning::mv::{
    ApplyKeySource, MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
    MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME,
    MV_JOIN_APPLY_KEY_COLUMN_NAME as JOIN_APPLY_KEY_COLUMN_NAME,
};

use super::model::{
    BranchFieldValidationError, ContractDecision, JoinContractDecision, JoinSchemaValidationError,
    SchemaEvolutionError,
};
use crate::mv::domain::analysis::rebind::RebindColumn;
use crate::mv::domain::persistence::schema::{
    BaseContract, BranchIdColumnContract, MvPartitionTransformContract, MvSchemaContract,
};
use crate::mv::domain::storage_observation::{
    MvObservedTargetField, MvSchemaValidationObservation, MvSchemaValidationPartitionContract,
    MvSchemaValidationPartitionTransform,
};

pub(crate) fn validate_schema_contract(
    contract: &MvSchemaContract,
    current_base: &MvSchemaValidationObservation,
    current_target: &MvSchemaValidationObservation,
) -> ContractDecision {
    // Stage 1: identity guard.
    if let Some(err) = validate_identity_guards(contract, current_base, current_target) {
        return ContractDecision::Incompatible(err);
    }
    if let Some(err) = check_target_partition_spec(contract, current_target.partition()) {
        return ContractDecision::Incompatible(err);
    }
    validate_observations_after_identity(contract, current_base, current_target)
}

pub(crate) fn validate_join_schema_contract(
    contract: &MvSchemaContract,
    bases: &[(&str, MvSchemaValidationObservation); 2],
    current_target: &MvSchemaValidationObservation,
) -> Result<JoinContractDecision, JoinSchemaValidationError> {
    contract.ensure_self_consistent().map_err(|error| {
        JoinSchemaValidationError::SelfInconsistent {
            reason: error.to_string(),
        }
    })?;
    if contract.bases.len() != 2 {
        return Err(JoinSchemaValidationError::BaseCount {
            actual: contract.bases.len(),
        });
    }
    if contract.target.table_uuid != current_target.table_uuid() {
        return Err(JoinSchemaValidationError::TargetIdentityChanged);
    }

    let mut rebound_columns = Vec::new();
    for (base_fqn, current_base) in bases {
        if !current_base.is_format_v3() || !current_base.stored_row_lineage_enabled() {
            return Err(JoinSchemaValidationError::BaseRowLineageContractBroken {
                base_fqn: (*base_fqn).to_string(),
            });
        }
        let base_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(base_fqn))
            .ok_or_else(|| JoinSchemaValidationError::MissingBaseContract {
                base_fqn: (*base_fqn).to_string(),
            })?;
        if base_contract.table_uuid != current_base.table_uuid() {
            return Err(JoinSchemaValidationError::BaseIdentityChanged {
                base_fqn: (*base_fqn).to_string(),
            });
        }
        rebound_columns.extend(validate_join_base_schema_contract_for_rebind(
            base_fqn,
            base_contract,
            current_base.fields(),
        )?);
    }

    match validate_schema_contract(contract, &bases[0].1, current_target) {
        ContractDecision::Incompatible(error) => {
            return Err(JoinSchemaValidationError::TargetCompatibility(error));
        }
        ContractDecision::CompatibleSafe | ContractDecision::CompatibleSafeWithRebind { .. } => {}
    }
    if rebound_columns.is_empty() {
        Ok(JoinContractDecision::CompatibleSafe)
    } else {
        Ok(JoinContractDecision::CompatibleSafeWithRebind { rebound_columns })
    }
}

fn validate_join_base_schema_contract_for_rebind(
    base_fqn: &str,
    base_contract: &BaseContract,
    current_fields: &[MvObservedTargetField],
) -> Result<Vec<RebindColumn>, JoinSchemaValidationError> {
    let mut rebound = Vec::new();
    for record in &base_contract.schema_at_create.fields {
        let Some(field) = current_fields
            .iter()
            .find(|field| field.field_id() == record.field_id)
        else {
            return Err(JoinSchemaValidationError::BaseFieldDropped {
                base_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_type = field.type_signature().to_string();
        if current_type != record.type_signature {
            return Err(JoinSchemaValidationError::BaseFieldTypeChanged {
                base_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_type,
            });
        }
        let current_required = !field.nullable();
        if current_required != record.required {
            return Err(JoinSchemaValidationError::BaseFieldNullabilityChanged {
                base_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from_required: record.required,
                to_required: current_required,
            });
        }
        if !field.name().eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(RebindColumn {
                base_table_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name().to_string(),
            });
        }
    }
    Ok(rebound)
}

pub(crate) fn validate_branch_id_field(
    contract: &BranchIdColumnContract,
    target: &MvSchemaValidationObservation,
) -> Result<(), BranchFieldValidationError> {
    let Some(field) = target
        .fields()
        .iter()
        .find(|field| field.field_id() == contract.target_field_id)
    else {
        return Err(BranchFieldValidationError::Missing {
            field_id: contract.target_field_id,
        });
    };
    if !field.name().eq_ignore_ascii_case(&contract.column_name) {
        return Err(BranchFieldValidationError::Renamed {
            expected: contract.column_name.clone(),
            actual: field.name().to_string(),
        });
    }
    if field.nullable() {
        return Err(BranchFieldValidationError::NotRequired);
    }
    if field.type_signature() == "int" {
        Ok(())
    } else {
        Err(BranchFieldValidationError::WrongType {
            expected: "Int".to_string(),
            actual: field.type_signature().to_string(),
        })
    }
}

fn validate_observations_after_identity(
    contract: &MvSchemaContract,
    base: &MvSchemaValidationObservation,
    target: &MvSchemaValidationObservation,
) -> ContractDecision {
    // Stage 2 fast path.
    if base.schema_id() == contract.base.schema_id_at_create
        && target.schema_id() == contract.target.schema_id_at_create
    {
        if contract.aggregate.is_some() {
            if let Some(err) = check_target_schema(contract, target.fields()) {
                return ContractDecision::Incompatible(err);
            }
        }
        return ContractDecision::CompatibleSafe;
    }
    // Stage 2 precise base check.
    let rebound = match check_base_referenced_fields(contract, base.fields()) {
        Err(err) => return ContractDecision::Incompatible(err),
        Ok(r) => r,
    };
    // Stage 3 target check.
    if let Some(err) = check_target_schema(contract, target.fields()) {
        return ContractDecision::Incompatible(err);
    }
    if rebound.is_empty() {
        ContractDecision::CompatibleSafe
    } else {
        ContractDecision::CompatibleSafeWithRebind {
            rebound_columns: rebound,
        }
    }
}

fn validate_identity_guards(
    contract: &MvSchemaContract,
    base: &MvSchemaValidationObservation,
    target: &MvSchemaValidationObservation,
) -> Option<SchemaEvolutionError> {
    if base.table_uuid() != contract.base.table_uuid {
        return Some(SchemaEvolutionError::BaseTableIdentityChanged {
            expected: contract.base.table_uuid.clone(),
            actual: base.table_uuid().to_string(),
        });
    }
    if !base.is_format_v3() {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: "base table must be Iceberg format v3, found non-v3".to_string(),
        });
    }
    if !base.stored_row_lineage_enabled() {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: "base table property write.row-lineage must be true".to_string(),
        });
    }

    if target.table_uuid() != contract.target.table_uuid {
        return Some(SchemaEvolutionError::TargetTableIdentityChanged {
            expected: contract.target.table_uuid.clone(),
            actual: target.table_uuid().to_string(),
        });
    }
    if !target.is_format_v3() {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: "target table must be Iceberg format v3, found non-v3".to_string(),
        });
    }
    if !target.stored_row_lineage_enabled() {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: "target table property write.row-lineage must be true".to_string(),
        });
    }
    None
}

fn check_base_referenced_fields(
    contract: &MvSchemaContract,
    current: &[MvObservedTargetField],
) -> Result<Vec<RebindColumn>, SchemaEvolutionError> {
    let mut rebound = Vec::new();
    for record in &contract.base.schema_at_create.fields {
        let Some(field) = current
            .iter()
            .find(|field| field.field_id() == record.field_id)
        else {
            return Err(SchemaEvolutionError::BaseFieldDropped {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_signature = field.type_signature().to_string();
        if current_signature != record.type_signature {
            return Err(SchemaEvolutionError::BaseFieldTypeChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_signature,
            });
        }
        let current_required = !field.nullable();
        if current_required != record.required {
            return Err(SchemaEvolutionError::BaseFieldNullabilityChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from_required: record.required,
                to_required: current_required,
            });
        }
        if !field.name().eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(RebindColumn {
                base_table_fqn: contract.base.table_fqn.clone(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name().to_string(),
            });
        }
    }
    Ok(rebound)
}

fn check_target_partition_spec(
    contract: &MvSchemaContract,
    current_spec: &MvSchemaValidationPartitionContract,
) -> Option<SchemaEvolutionError> {
    let Some(expected) = &contract.target.partition else {
        return None;
    };
    if current_spec.spec_id() != expected.target_spec_id {
        return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
            reason: format!(
                "expected default spec id {}, got {}",
                expected.target_spec_id,
                current_spec.spec_id()
            ),
        });
    }
    let fields = current_spec.fields();
    if fields.len() != expected.fields.len() {
        return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
            reason: format!(
                "expected {} partition fields, got {}",
                expected.fields.len(),
                fields.len()
            ),
        });
    }
    for (idx, (actual, expected)) in fields.iter().zip(expected.fields.iter()).enumerate() {
        if actual.partition_field_id() != expected.partition_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} id expected {}, got {}",
                    expected.partition_field_id,
                    actual.partition_field_id()
                ),
            });
        }
        if actual.source_target_field_id() != expected.source_target_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} source id expected {}, got {}",
                    expected.partition_field_name,
                    expected.source_target_field_id,
                    actual.source_target_field_id()
                ),
            });
        }
        if actual.partition_field_name() != expected.partition_field_name {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} name expected {}, got {}",
                    expected.partition_field_name,
                    actual.partition_field_name()
                ),
            });
        }
        let actual_transform = match actual.transform() {
            MvSchemaValidationPartitionTransform::Identity => {
                MvPartitionTransformContract::Identity
            }
            MvSchemaValidationPartitionTransform::Year => MvPartitionTransformContract::Year,
            MvSchemaValidationPartitionTransform::Month => MvPartitionTransformContract::Month,
            MvSchemaValidationPartitionTransform::Day => MvPartitionTransformContract::Day,
            MvSchemaValidationPartitionTransform::Hour => MvPartitionTransformContract::Hour,
            MvSchemaValidationPartitionTransform::Bucket { num_buckets } => {
                MvPartitionTransformContract::Bucket {
                    num_buckets: *num_buckets,
                }
            }
            MvSchemaValidationPartitionTransform::Truncate { width } => {
                MvPartitionTransformContract::Truncate { width: *width }
            }
            MvSchemaValidationPartitionTransform::Void => MvPartitionTransformContract::Void,
            MvSchemaValidationPartitionTransform::Unsupported(name) => {
                return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                    reason: format!(
                        "partition field {} has unsupported transform {name}",
                        actual.partition_field_name()
                    ),
                });
            }
        };
        if actual_transform != expected.transform {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} transform expected {:?}, got {:?}",
                    expected.partition_field_name, expected.transform, actual_transform
                ),
            });
        }
    }
    None
}

fn check_target_schema(
    contract: &MvSchemaContract,
    current: &[MvObservedTargetField],
) -> Option<SchemaEvolutionError> {
    for tv in &contract.target.visible_columns {
        let Some(field) = current
            .iter()
            .find(|field| field.field_id() == tv.target_field_id)
        else {
            return Some(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: tv.output_name.clone(),
                target_field_id: tv.target_field_id,
            });
        };
        let sig = field.type_signature().to_string();
        if sig != tv.type_signature {
            return Some(SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                target_field_id: tv.target_field_id,
                from: tv.type_signature.clone(),
                to: sig,
            });
        }
        if !field.name().eq_ignore_ascii_case(&tv.output_name) {
            return Some(SchemaEvolutionError::TargetVisibleFieldRenamed {
                target_field_id: tv.target_field_id,
                expected: tv.output_name.clone(),
                actual: field.name().to_string(),
            });
        }
    }

    let expected = &contract.target.hidden_apply_key;
    let Some(field) = current
        .iter()
        .find(|field| field.field_id() == expected.target_field_id)
    else {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!(
                "hidden apply-key field id {} not found",
                expected.target_field_id
            ),
        });
    };
    let expected_hidden_apply_key_column = match expected.source {
        ApplyKeySource::BaseRowId => HIDDEN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::JoinRowKey => JOIN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::GroupRowId => GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
    };
    if !field
        .name()
        .eq_ignore_ascii_case(expected_hidden_apply_key_column)
    {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key column renamed to {}", field.name()),
        });
    }
    if let Some(err) = check_aggregate_state_schema(contract, current) {
        return Some(err);
    }
    if field.nullable() {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: "hidden apply-key column must be required".to_string(),
        });
    }
    let (expected_apply_key_type, expected_apply_key_display) = match expected.source {
        ApplyKeySource::BaseRowId => ("long", "Long"),
        ApplyKeySource::JoinRowKey | ApplyKeySource::GroupRowId => ("string", "String"),
    };
    if field.type_signature() != expected_apply_key_type {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!(
                "hidden apply-key column must be {expected_apply_key_display}, got {}",
                field.type_signature()
            ),
        });
    }
    None
}

fn check_aggregate_state_schema(
    contract: &MvSchemaContract,
    current: &[MvObservedTargetField],
) -> Option<SchemaEvolutionError> {
    let aggregate = contract.aggregate.as_ref()?;
    if aggregate.state_layout_version != 1 {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate state layout version {} is unsupported; expected 1",
                aggregate.state_layout_version
            ),
        });
    }
    if aggregate.state_columns.is_empty() {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: "aggregate state columns must not be empty".to_string(),
        });
    }
    if aggregate.row_id_column_name != GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column name expected {}, got {}",
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, aggregate.row_id_column_name
            ),
        });
    }
    let mut row_id_matches = current.iter().filter(|field| {
        field
            .name()
            .eq_ignore_ascii_case(&aggregate.row_id_column_name)
    });
    let Some(row_id_field) = row_id_matches.next() else {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} not found",
                aggregate.row_id_column_name
            ),
        });
    };
    if row_id_matches.next().is_some() {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} is duplicated",
                aggregate.row_id_column_name
            ),
        });
    }
    if row_id_field.field_id() != contract.target.hidden_apply_key.target_field_id {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id field id {} must match hidden apply-key field id {}",
                row_id_field.field_id(),
                contract.target.hidden_apply_key.target_field_id
            ),
        });
    }
    if row_id_field.nullable() {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} must be required",
                aggregate.row_id_column_name
            ),
        });
    }
    if row_id_field.type_signature() != "string" {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} must be String, got {}",
                aggregate.row_id_column_name,
                row_id_field.type_signature()
            ),
        });
    }

    for state_col in &aggregate.state_columns {
        let Some(field) = current
            .iter()
            .find(|field| field.field_id() == state_col.target_field_id)
        else {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} not found",
                    state_col.column_name, state_col.target_field_id
                ),
            });
        };
        if !field.name().eq_ignore_ascii_case(&state_col.column_name) {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} renamed to {}",
                    state_col.column_name,
                    state_col.target_field_id,
                    field.name()
                ),
            });
        }
        let sig = field.type_signature();
        if sig != state_col.type_signature {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} changed type from {} to {}",
                    state_col.column_name, state_col.target_field_id, state_col.type_signature, sig
                ),
            });
        }
        let actual_nullable = field.nullable();
        if actual_nullable != state_col.nullable {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} nullable changed from {} to {}",
                    state_col.column_name,
                    state_col.target_field_id,
                    state_col.nullable,
                    actual_nullable
                ),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::persistence::schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, BranchIdColumnContract, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, JoinContract, JoinContractKind,
        JoinPredicateLineage, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, OutputColumnLineage, OutputContract, QualifiedFieldLineage,
        TargetContract, TargetVisibleColumn,
    };
    use crate::mv::domain::storage_observation::MvSchemaValidationPartitionField;
    use novarocks_sql::planning::mv::{
        MV_BRANCH_ID_COLUMN_NAME as BRANCH_ID_COLUMN_NAME,
        MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
        MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME,
        MV_JOIN_APPLY_KEY_COLUMN_NAME as JOIN_APPLY_KEY_COLUMN_NAME,
    };

    /// Test-local, provider-neutral stand-in for the target schema facts an
    /// observation is built from.
    ///
    /// These tests exercise Core's MV contract validator, which in production
    /// only ever sees the neutral observation -- never a provider schema.
    /// Building the fixtures out of Iceberg `spec::Schema` values made the
    /// tests assert a coupling the production code does not have, and kept the
    /// provider crate reachable from Core purely through test code.
    #[derive(Clone)]
    struct TestSchema {
        schema_id: i32,
        fields: Vec<MvObservedTargetField>,
    }

    fn req(field_id: i32, name: &str, type_signature: &str) -> MvObservedTargetField {
        MvObservedTargetField::new(
            field_id,
            name.to_string(),
            type_signature.to_string(),
            false,
        )
    }

    fn opt(field_id: i32, name: &str, type_signature: &str) -> MvObservedTargetField {
        MvObservedTargetField::new(field_id, name.to_string(), type_signature.to_string(), true)
    }

    fn test_schema(schema_id: i32, fields: Vec<MvObservedTargetField>) -> TestSchema {
        TestSchema { schema_id, fields }
    }

    fn unpartitioned() -> MvSchemaValidationPartitionContract {
        MvSchemaValidationPartitionContract::new(0, Vec::new())
    }

    /// Neutral stand-in for the live target facts a refresh observes.
    #[derive(Clone)]
    struct TestCurrentTarget {
        table_uuid: String,
        format_v3: bool,
        row_lineage_enabled: bool,
        schema: TestSchema,
        partition: MvSchemaValidationPartitionContract,
    }

    impl TestCurrentTarget {
        fn view(&self) -> MvSchemaValidationObservation {
            test_observation(
                &self.table_uuid,
                self.format_v3,
                self.row_lineage_enabled,
                &self.schema,
                &self.partition,
            )
        }
    }

    fn test_observation(
        table_uuid: &str,
        format_v3: bool,
        stored_row_lineage_enabled: bool,
        schema: &TestSchema,
        partition: &MvSchemaValidationPartitionContract,
    ) -> MvSchemaValidationObservation {
        MvSchemaValidationObservation::try_new_with_maximum_payload(
            table_uuid.to_string(),
            schema.schema_id,
            format_v3,
            stored_row_lineage_enabled,
            schema.fields.clone(),
            partition.clone(),
        )
        .expect("test schema observation")
    }

    fn validate_schema_contract_after_identity(
        contract: &MvSchemaContract,
        base_schema: &TestSchema,
        target_schema: &TestSchema,
    ) -> ContractDecision {
        let unpartitioned = unpartitioned();
        let base = test_observation("base-test", true, true, base_schema, &unpartitioned);
        let target = test_observation("target-test", true, true, target_schema, &unpartitioned);
        validate_observations_after_identity(contract, &base, &target)
    }

    fn validate_branch_id_field(
        contract: &BranchIdColumnContract,
        schema: &TestSchema,
    ) -> Result<(), BranchFieldValidationError> {
        let observation = test_observation("branch-test", true, true, schema, &unpartitioned());
        super::validate_branch_id_field(contract, &observation)
    }

    fn observed_fields(schema: &TestSchema) -> Vec<MvObservedTargetField> {
        schema.fields.clone()
    }

    fn check_base_referenced_fields(
        contract: &MvSchemaContract,
        schema: &TestSchema,
    ) -> Result<Vec<RebindColumn>, SchemaEvolutionError> {
        super::check_base_referenced_fields(contract, &observed_fields(schema))
    }

    fn check_target_schema(
        contract: &MvSchemaContract,
        schema: &TestSchema,
    ) -> Option<SchemaEvolutionError> {
        super::check_target_schema(contract, &observed_fields(schema))
    }

    /// Partition-contract fixtures are built neutrally: in production Core
    /// receives `MvSchemaValidationPartitionContract` from the observation
    /// port and never sees an Iceberg `PartitionSpec`.
    fn pf(
        partition_field_id: i32,
        partition_field_name: &str,
        source_target_field_id: i32,
        source_column_name: &str,
        transform: MvSchemaValidationPartitionTransform,
    ) -> MvSchemaValidationPartitionField {
        MvSchemaValidationPartitionField::new(
            partition_field_id,
            partition_field_name.to_string(),
            source_target_field_id,
            source_column_name.to_string(),
            transform,
        )
    }

    fn check_target_partition_spec(
        contract: &MvSchemaContract,
        observed: &MvSchemaValidationPartitionContract,
    ) -> Option<SchemaEvolutionError> {
        if contract.target.partition.is_none() {
            return None;
        }
        super::check_target_partition_spec(contract, observed)
    }

    fn identity_table(
        table_uuid: &str,
        format_v3: bool,
        row_lineage_enabled: bool,
    ) -> TestCurrentTarget {
        TestCurrentTarget {
            table_uuid: table_uuid.to_string(),
            format_v3,
            row_lineage_enabled,
            schema: test_schema(
                1,
                vec![
                    req(1, "id", "int"),
                    req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long"),
                ],
            ),
            partition: unpartitioned(),
        }
    }

    fn identity_contract(base: &TestCurrentTarget, target: &TestCurrentTarget) -> MvSchemaContract {
        let mut contract = minimal_base_row_id_contract();
        contract.base.table_uuid = base.table_uuid.clone();
        contract.target.table_uuid = target.table_uuid.clone();
        contract
    }

    #[test]
    fn schema_evolution_error_messages_are_action_oriented() {
        let err = SchemaEvolutionError::BaseFieldDropped {
            field_id: 5,
            name_at_create: "amount".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("field id 5"));
        assert!(msg.contains("amount"));
        assert!(msg.contains("REFRESH FULL"));
    }

    #[test]
    fn schema_evolution_error_messages_are_exact() {
        let cases = vec![
            (
                SchemaEvolutionError::BaseTableIdentityChanged {
                    expected: "base-a".to_string(),
                    actual: "base-b".to_string(),
                },
                "iceberg MV refresh blocked: base table identity changed (uuid expected=base-a, actual=base-b); run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseRowLineageContractBroken {
                    reason: "base reason".to_string(),
                },
                "iceberg MV refresh blocked: base table row-lineage contract broken (base reason); run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldDropped {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) was dropped from base table; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldTypeChanged {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                    from: "int".to_string(),
                    to: "long".to_string(),
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) changed type from int to long; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldNullabilityChanged {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                    from_required: true,
                    to_required: false,
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) changed nullability from required=true to required=false; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetTableIdentityChanged {
                    expected: "target-a".to_string(),
                    actual: "target-b".to_string(),
                },
                "iceberg MV refresh blocked: target table identity changed (uuid expected=target-a, actual=target-b); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetRowLineageContractBroken {
                    reason: "target reason".to_string(),
                },
                "iceberg MV refresh blocked: target table row-lineage contract broken (target reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldDropped {
                    output_name: "amount".to_string(),
                    target_field_id: 8,
                },
                "iceberg MV refresh blocked: target visible column \"amount\" (field id 8) was dropped; recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldRenamed {
                    target_field_id: 8,
                    expected: "amount".to_string(),
                    actual: "renamed_amount".to_string(),
                },
                "iceberg MV refresh blocked: target visible column (field id 8) renamed externally: expected \"amount\", actual \"renamed_amount\"; recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                    target_field_id: 8,
                    from: "int".to_string(),
                    to: "long".to_string(),
                },
                "iceberg MV refresh blocked: target visible column (field id 8) changed type from int to long; recreate the MV",
            ),
            (
                SchemaEvolutionError::HiddenApplyKeyContractBroken {
                    reason: "hidden reason".to_string(),
                },
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetPartitionSpecChanged {
                    reason: "partition reason".to_string(),
                },
                "iceberg MV refresh blocked: target partition spec changed externally (partition reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::AggregateStateContractBroken {
                    reason: "aggregate reason".to_string(),
                },
                "iceberg MV refresh blocked: target aggregate state contract broken (aggregate reason); recreate the MV",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }

        let good_base = identity_table("base-uuid", true, true);
        let good_target = identity_table("target-uuid", true, true);
        let base_v2 = identity_table("base-uuid", false, true);
        let base_missing = identity_table("base-uuid", true, false);
        let base_false = identity_table("base-uuid", true, false);
        let target_v2 = identity_table("target-uuid", false, true);
        let target_missing = identity_table("target-uuid", true, false);
        let target_false = identity_table("target-uuid", true, false);
        let identity_cases = [
            (
                &base_v2,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table must be Iceberg format v3, found non-v3); run REFRESH FULL or recreate the MV",
            ),
            (
                &base_missing,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table property write.row-lineage must be true); run REFRESH FULL or recreate the MV",
            ),
            (
                &base_false,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table property write.row-lineage must be true); run REFRESH FULL or recreate the MV",
            ),
            (
                &good_base,
                &target_v2,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table must be Iceberg format v3, found non-v3); recreate the MV",
            ),
            (
                &good_base,
                &target_missing,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table property write.row-lineage must be true); recreate the MV",
            ),
            (
                &good_base,
                &target_false,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table property write.row-lineage must be true); recreate the MV",
            ),
        ];
        for (base, target, expected) in identity_cases {
            let contract = identity_contract(base, target);
            let error = validate_identity_guards(&contract, &base.view(), &target.view())
                .expect("identity case must be incompatible");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn identity_validation_preserves_first_error_order() {
        let good_base = identity_table("base-uuid", true, true);
        let good_target = identity_table("target-uuid", true, true);
        let contract = identity_contract(&good_base, &good_target);

        let mut base = good_base.clone();
        let mut target = good_target.clone();
        base.table_uuid = "BASE-UUID".to_string();
        base.format_v3 = false;
        base.row_lineage_enabled = false;
        target.table_uuid = "other-target".to_string();
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseTableIdentityChanged {
                expected: "base-uuid".to_string(),
                actual: "BASE-UUID".to_string(),
            })
        );

        base.table_uuid = contract.base.table_uuid.clone();
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseRowLineageContractBroken {
                reason: "base table must be Iceberg format v3, found non-v3".to_string(),
            })
        );

        base.format_v3 = true;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseRowLineageContractBroken {
                reason: "base table property write.row-lineage must be true".to_string(),
            })
        );

        base.row_lineage_enabled = true;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetTableIdentityChanged {
                expected: "target-uuid".to_string(),
                actual: "other-target".to_string(),
            })
        );

        target.table_uuid = contract.target.table_uuid.clone();
        target.format_v3 = false;
        target.row_lineage_enabled = false;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetRowLineageContractBroken {
                reason: "target table must be Iceberg format v3, found non-v3".to_string(),
            })
        );

        target.format_v3 = true;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetRowLineageContractBroken {
                reason: "target table property write.row-lineage must be true".to_string(),
            })
        );

        target.row_lineage_enabled = true;
        target.schema = test_schema(12, Vec::new());
        let mut partition_contract = contract.clone();
        partition_contract.target.partition = Some(MvPartitionContract {
            target_spec_id: 1,
            fields: Vec::new(),
        });
        assert_eq!(
            validate_schema_contract(&partition_contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: "expected default spec id 1, got 0".to_string(),
            })
        );

        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: "id".to_string(),
                target_field_id: 1,
            })
        );
    }

    #[test]
    fn schema_evolution_error_target_messages_recommend_recreate() {
        let err = SchemaEvolutionError::TargetTableIdentityChanged {
            expected: "A".into(),
            actual: "B".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("recreate the MV"));
    }

    #[test]
    fn schema_evolution_error_implements_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(SchemaEvolutionError::BaseFieldDropped {
            field_id: 5,
            name_at_create: "amount".into(),
        });
        let _ = err; // just ensure it compiles
    }

    #[test]
    fn target_partition_spec_guard_detects_external_transform_change() {
        let matching = MvSchemaValidationPartitionContract::new(
            0,
            vec![pf(
                1000,
                "id_bucket_16",
                1,
                "id",
                MvSchemaValidationPartitionTransform::Bucket { num_buckets: 16 },
            )],
        );
        let changed = MvSchemaValidationPartitionContract::new(
            0,
            vec![pf(
                1000,
                "id_bucket_8",
                1,
                "id",
                MvSchemaValidationPartitionTransform::Bucket { num_buckets: 8 },
            )],
        );
        let mut contract = minimal_base_row_id_contract();
        contract.target.partition = Some(MvPartitionContract {
            target_spec_id: 0,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1000,
                partition_field_name: "id_bucket_16".to_string(),
                source_target_field_id: 1,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Bucket { num_buckets: 16 },
            }],
        });

        assert_eq!(check_target_partition_spec(&contract, &matching), None);
        assert!(matches!(
            check_target_partition_spec(&contract, &changed),
            Some(SchemaEvolutionError::TargetPartitionSpecChanged { .. })
        ));
    }

    #[test]
    fn partition_compatibility_preserves_strict_field_order() {
        // (partition_field_id, name, source_field_id, source_column, observed, contracted)
        let supported: Vec<(
            i32,
            &str,
            i32,
            &str,
            MvSchemaValidationPartitionTransform,
            MvPartitionTransformContract,
        )> = vec![
            (
                1000,
                "p_identity",
                1,
                "source_1",
                MvSchemaValidationPartitionTransform::Identity,
                MvPartitionTransformContract::Identity,
            ),
            (
                1001,
                "p_year",
                2,
                "source_2",
                MvSchemaValidationPartitionTransform::Year,
                MvPartitionTransformContract::Year,
            ),
            (
                1002,
                "p_month",
                3,
                "source_3",
                MvSchemaValidationPartitionTransform::Month,
                MvPartitionTransformContract::Month,
            ),
            (
                1003,
                "p_day",
                4,
                "source_4",
                MvSchemaValidationPartitionTransform::Day,
                MvPartitionTransformContract::Day,
            ),
            (
                1004,
                "p_hour",
                5,
                "source_5",
                MvSchemaValidationPartitionTransform::Hour,
                MvPartitionTransformContract::Hour,
            ),
            (
                1005,
                "p_bucket",
                6,
                "source_6",
                MvSchemaValidationPartitionTransform::Bucket { num_buckets: 16 },
                MvPartitionTransformContract::Bucket { num_buckets: 16 },
            ),
            (
                1006,
                "p_truncate",
                7,
                "source_7",
                MvSchemaValidationPartitionTransform::Truncate { width: 4 },
                MvPartitionTransformContract::Truncate { width: 4 },
            ),
            (
                1007,
                "p_void",
                8,
                "source_8",
                MvSchemaValidationPartitionTransform::Void,
                MvPartitionTransformContract::Void,
            ),
        ];
        let supported_spec = MvSchemaValidationPartitionContract::new(
            7,
            supported
                .iter()
                .map(|(id, name, src, col, observed, _)| pf(*id, name, *src, col, observed.clone()))
                .collect(),
        );
        let expected_partition = MvPartitionContract {
            target_spec_id: 7,
            fields: supported
                .iter()
                .map(
                    |(id, name, src, col, _, contracted)| MvPartitionFieldContract {
                        partition_field_id: *id,
                        partition_field_name: (*name).to_string(),
                        source_target_field_id: *src,
                        source_column_name: (*col).to_string(),
                        transform: contracted.clone(),
                    },
                )
                .collect(),
        };
        let mut contract = minimal_base_row_id_contract();
        contract.target.partition = Some(expected_partition.clone());
        assert_eq!(
            check_target_partition_spec(&contract, &supported_spec),
            None,
            "all supported transforms must preserve their exact contracts"
        );

        let unknown_spec = MvSchemaValidationPartitionContract::new(
            7,
            vec![pf(
                1000,
                "p_unknown",
                9,
                "unknown_src",
                MvSchemaValidationPartitionTransform::Unsupported("Unknown".to_string()),
            )],
        );
        let mut no_partition_contract = minimal_base_row_id_contract();
        no_partition_contract.target.partition = None;
        assert_eq!(
            check_target_partition_spec(&no_partition_contract, &unknown_spec),
            None,
            "live partition state must be ignored when no partition contract was persisted"
        );

        let exact_error =
            |partition: MvPartitionContract, current: &MvSchemaValidationPartitionContract| {
                let mut current_contract = minimal_base_row_id_contract();
                current_contract.target.partition = Some(partition);
                check_target_partition_spec(&current_contract, current)
                    .expect("partition mismatch")
                    .to_string()
            };
        let wrap = |reason: &str| {
            format!(
                "iceberg MV refresh blocked: target partition spec changed externally ({reason}); recreate the MV"
            )
        };

        let mut spec_id_changed = expected_partition.clone();
        spec_id_changed.target_spec_id = 8;
        assert_eq!(
            exact_error(spec_id_changed, &supported_spec),
            wrap("expected default spec id 8, got 7")
        );

        let mut count_changed = expected_partition.clone();
        count_changed.fields.pop();
        assert_eq!(
            exact_error(count_changed, &supported_spec),
            wrap("expected 7 partition fields, got 8")
        );

        let mut id_changed = expected_partition.clone();
        id_changed.fields[0].partition_field_id += 100;
        assert_eq!(
            exact_error(id_changed, &supported_spec),
            wrap("partition field #0 id expected 1100, got 1000")
        );

        let mut source_changed = expected_partition.clone();
        source_changed.fields[0].source_target_field_id = 99;
        assert_eq!(
            exact_error(source_changed, &supported_spec),
            wrap("partition field p_identity source id expected 99, got 1")
        );

        let mut name_changed = expected_partition.clone();
        name_changed.fields[0].partition_field_name = "P_IDENTITY".to_string();
        assert_eq!(
            exact_error(name_changed, &supported_spec),
            wrap("partition field #0 name expected P_IDENTITY, got p_identity")
        );

        let mut reordered = expected_partition.clone();
        reordered.fields.swap(0, 1);
        assert_eq!(
            exact_error(reordered, &supported_spec),
            wrap("partition field #0 id expected 1001, got 1000")
        );

        let mut transform_changed = expected_partition;
        transform_changed.fields[0].transform = MvPartitionTransformContract::Year;
        assert_eq!(
            exact_error(transform_changed, &supported_spec),
            wrap("partition field p_identity transform expected Year, got Identity")
        );

        let unknown_contract = MvPartitionContract {
            target_spec_id: 7,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1000,
                partition_field_name: "p_unknown".to_string(),
                source_target_field_id: 9,
                source_column_name: "unknown_src".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        };
        assert_eq!(
            exact_error(unknown_contract, &unknown_spec),
            wrap("partition field p_unknown has unsupported transform Unknown")
        );
    }

    #[test]
    fn supplied_base_schema_drives_base_rebind_decision() {
        let base_type = "int";
        let target_type = "int";
        let base_schema = test_schema(7, vec![req(1, "renamed_id", base_type)]);
        let target_schema = test_schema(
            11,
            vec![
                req(1, "id", target_type),
                req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long"),
            ],
        );
        let contract = MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: base_type.to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: target_type.to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        };

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(
            decision,
            ContractDecision::CompatibleSafeWithRebind {
                rebound_columns: vec![RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "renamed_id".to_string(),
                }],
            }
        );
    }

    #[test]
    fn supplied_base_schema_rejects_referenced_nullability_drift() {
        let base_type = "int";
        let target_type = "int";
        let base_schema = test_schema(7, vec![opt(1, "id", base_type)]);
        let target_schema = test_schema(
            11,
            vec![
                req(1, "id", target_type),
                req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long"),
            ],
        );
        let contract = minimal_base_row_id_contract();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(SchemaEvolutionError::BaseFieldNullabilityChanged {
                field_id,
                name_at_create,
                from_required,
                to_required,
            }) => {
                assert_eq!(field_id, 1);
                assert_eq!(name_at_create, "id");
                assert!(from_required);
                assert!(!to_required);
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn supplied_base_schema_rebind_payload_includes_base_fqn() {
        let base_type = "int";
        let target_type = "int";
        let base_schema = test_schema(7, vec![req(1, "renamed_id", base_type)]);
        let target_schema = test_schema(
            11,
            vec![
                req(1, "id", target_type),
                req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long"),
            ],
        );
        let contract = minimal_base_row_id_contract();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(
            decision,
            ContractDecision::CompatibleSafeWithRebind {
                rebound_columns: vec![RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "renamed_id".to_string(),
                }],
            }
        );
    }

    #[test]
    fn base_field_compatibility_preserves_tolerance_and_rebind_order() {
        let int_type = "int";
        let long_type = "long";
        let mut contract = minimal_base_row_id_contract();
        contract.base.schema_at_create.fields.push(BaseFieldRecord {
            field_id: 2,
            name_at_create: "amount".to_string(),
            type_signature: int_type.to_string(),
            required: false,
        });

        let dropped = test_schema(2, vec![opt(2, "amount", int_type)]);
        assert_eq!(
            check_base_referenced_fields(&contract, &dropped)
                .expect_err("referenced field drop must fail")
                .to_string(),
            "iceberg MV refresh blocked: base column \"id\" (field id 1) was dropped from base table; run REFRESH FULL or recreate the MV"
        );

        let type_changed =
            test_schema(2, vec![req(1, "id", long_type), opt(2, "amount", int_type)]);
        assert_eq!(
            check_base_referenced_fields(&contract, &type_changed)
                .expect_err("referenced field type change must fail")
                .to_string(),
            "iceberg MV refresh blocked: base column \"id\" (field id 1) changed type from int to long; run REFRESH FULL or recreate the MV"
        );

        let unrelated_reordered = test_schema(
            2,
            vec![
                opt(99, "unrelated", "string"),
                opt(2, "amount", int_type),
                req(1, "id", int_type),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &unrelated_reordered),
            Ok(Vec::new())
        );

        let case_only = test_schema(2, vec![req(1, "ID", int_type), opt(2, "AMOUNT", int_type)]);
        assert_eq!(
            check_base_referenced_fields(&contract, &case_only),
            Ok(Vec::new())
        );

        let renamed_in_physical_reverse_order = test_schema(
            2,
            vec![
                opt(2, "current_amount", int_type),
                req(1, "current_id", int_type),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &renamed_in_physical_reverse_order),
            Ok(vec![
                RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "current_id".to_string(),
                },
                RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 2,
                    name_at_create: "amount".to_string(),
                    current_name: "current_amount".to_string(),
                },
            ])
        );
    }

    #[test]
    fn target_field_compatibility_preserves_nullable_tolerance_and_failures() {
        let int_type = "int";
        let long_type = "long";
        let contract = minimal_base_row_id_contract();
        let schema = |visible: Option<MvObservedTargetField>,
                      hidden: Option<MvObservedTargetField>| {
            test_schema(12, visible.into_iter().chain(hidden).collect())
        };
        let hidden = || req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long");

        let visible_nullable = schema(Some(opt(1, "ID", int_type)), Some(hidden()));
        assert_eq!(check_target_schema(&contract, &visible_nullable), None);

        let cases = vec![
            (
                schema(None, Some(hidden())),
                "iceberg MV refresh blocked: target visible column \"id\" (field id 1) was dropped; recreate the MV",
            ),
            (
                schema(Some(req(1, "renamed_id", int_type)), Some(hidden())),
                "iceberg MV refresh blocked: target visible column (field id 1) renamed externally: expected \"id\", actual \"renamed_id\"; recreate the MV",
            ),
            (
                schema(Some(req(1, "id", long_type)), Some(hidden())),
                "iceberg MV refresh blocked: target visible column (field id 1) changed type from int to long; recreate the MV",
            ),
            (
                schema(Some(req(1, "id", int_type)), None),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key field id 2 not found); recreate the MV",
            ),
            (
                schema(
                    Some(req(1, "id", int_type)),
                    Some(req(2, "renamed_key", "long")),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column renamed to renamed_key); recreate the MV",
            ),
            (
                schema(
                    Some(req(1, "id", int_type)),
                    Some(opt(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "long")),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column must be required); recreate the MV",
            ),
            (
                schema(
                    Some(req(1, "id", int_type)),
                    Some(req(2, HIDDEN_APPLY_KEY_COLUMN_NAME, "string")),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column must be Long, got string); recreate the MV",
            ),
        ];
        for (current_schema, expected) in cases {
            let error = check_target_schema(&contract, &current_schema)
                .expect("target compatibility case must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn ordinary_schema_id_fast_path_preserves_target_tolerance() {
        let ordinary_contract = minimal_base_row_id_contract();
        let ordinary_base = test_schema(ordinary_contract.base.schema_id_at_create, Vec::new());
        let ordinary_target = test_schema(ordinary_contract.target.schema_id_at_create, Vec::new());
        assert_eq!(
            validate_schema_contract_after_identity(
                &ordinary_contract,
                &ordinary_base,
                &ordinary_target,
            ),
            ContractDecision::CompatibleSafe
        );

        let aggregate_type = "long";
        let mut aggregate_contract = aggregate_schema_contract(aggregate_type.to_string());
        aggregate_contract.target.schema_id_at_create = 11;
        let aggregate_base = test_schema(aggregate_contract.base.schema_id_at_create, Vec::new());
        let aggregate_target = aggregate_target_schema("__agg_state_c", "string", false);
        assert_eq!(
            validate_schema_contract_after_identity(
                &aggregate_contract,
                &aggregate_base,
                &aggregate_target,
            ),
            ContractDecision::Incompatible(SchemaEvolutionError::AggregateStateContractBroken {
                reason: "aggregate state column __agg_state_c field id 3 changed type from long to string"
                    .to_string(),
            })
        );
    }

    fn minimal_base_row_id_contract() -> MvSchemaContract {
        let target_type = "int";
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: target_type.to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: target_type.to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        }
    }

    fn join_schema_contract() -> MvSchemaContract {
        let int_type = "int";
        let left = BaseContract {
            table_fqn: "ice.db.left".to_string(),
            table_uuid: "left-uuid".to_string(),
            alias_at_create: Some("l".to_string()),
            schema_id_at_create: 1,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![BaseFieldRecord {
                    field_id: 1,
                    name_at_create: "left_id".to_string(),
                    type_signature: int_type.to_string(),
                    required: true,
                }],
            },
        };
        let right = BaseContract {
            table_fqn: "ice.db.right".to_string(),
            table_uuid: "right-uuid".to_string(),
            alias_at_create: Some("r".to_string()),
            schema_id_at_create: 1,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![BaseFieldRecord {
                    field_id: 2,
                    name_at_create: "right_id".to_string(),
                    type_signature: int_type.to_string(),
                    required: true,
                }],
            },
        };
        MvSchemaContract {
            contract_version: 2,
            base: left.clone(),
            bases: vec![left, right],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: Vec::new(),
                        referenced_base_fields: vec![QualifiedFieldLineage {
                            table_fqn: "ice.db.left".to_string(),
                            qualifier_at_create: "l".to_string(),
                            field_id: 1,
                        }],
                    },
                }],
                filter: None,
            },
            join: Some(JoinContract {
                kind: JoinContractKind::InnerEquiJoin,
                predicates: vec![JoinPredicateLineage {
                    left: QualifiedFieldLineage {
                        table_fqn: "ice.db.left".to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: "ice.db.right".to_string(),
                        qualifier_at_create: "r".to_string(),
                        field_id: 2,
                    },
                }],
            }),
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_join".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 1,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "left_id".to_string(),
                    target_field_id: 1,
                    type_signature: int_type.to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::JoinRowKey,
                },
                partition: None,
            },
        }
    }

    fn join_base_table(
        table_uuid: &str,
        field_id: i32,
        field_name: &str,
        field_type: &str,
        required: bool,
    ) -> TestCurrentTarget {
        let field = if required {
            req(field_id, field_name, field_type)
        } else {
            opt(field_id, field_name, field_type)
        };
        TestCurrentTarget {
            table_uuid: table_uuid.to_string(),
            format_v3: true,
            row_lineage_enabled: true,
            schema: test_schema(2, vec![field]),
            partition: unpartitioned(),
        }
    }

    fn join_target_table() -> TestCurrentTarget {
        TestCurrentTarget {
            table_uuid: "target-uuid".to_string(),
            format_v3: true,
            row_lineage_enabled: true,
            schema: test_schema(
                2,
                vec![
                    req(1, "left_id", "int"),
                    req(2, JOIN_APPLY_KEY_COLUMN_NAME, "string"),
                ],
            ),
            partition: unpartitioned(),
        }
    }

    fn validate_test_join(
        contract: &MvSchemaContract,
        left_fqn: &str,
        left: &TestCurrentTarget,
        right_fqn: &str,
        right: &TestCurrentTarget,
        target: &TestCurrentTarget,
    ) -> Result<JoinContractDecision, JoinSchemaValidationError> {
        let bases = [(left_fqn, left.view()), (right_fqn, right.view())];
        validate_join_schema_contract(contract, &bases, &target.view())
    }

    #[test]
    fn join_base_schema_contract_returns_rebind_for_rename() {
        let contract = join_schema_contract();
        let left = join_base_table("left-uuid", 1, "renamed_left_id", "int", true);
        let right = join_base_table("right-uuid", 2, "right_id", "int", true);
        let target = join_target_table();

        assert_eq!(
            validate_test_join(
                &contract,
                "ice.db.left",
                &left,
                "ice.db.right",
                &right,
                &target,
            ),
            Ok(JoinContractDecision::CompatibleSafeWithRebind {
                rebound_columns: vec![RebindColumn {
                    base_table_fqn: "ice.db.left".to_string(),
                    field_id: 1,
                    name_at_create: "left_id".to_string(),
                    current_name: "renamed_left_id".to_string(),
                }],
            })
        );
    }

    #[test]
    fn join_schema_validation_preserves_first_error_and_exact_messages() {
        let int_type = "int";
        let long_type = "long";
        let mut contract = join_schema_contract();
        let mut left = join_base_table("left-uuid", 1, "left_id", int_type.clone(), true);
        let mut right = join_base_table("right-uuid", 2, "right_id", int_type.clone(), true);
        let mut target = join_target_table();

        contract.target.hidden_apply_key.column_name = "wrong".to_string();
        contract.bases.push(contract.bases[0].clone());
        target.table_uuid = "wrong-target".to_string();
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("self-consistency must win");
        assert_eq!(
            error.to_string(),
            "Iceberg join MV schema contract is self-inconsistent: MV contract hidden apply-key column name expected __nova_join_row_key, got wrong"
        );

        contract = join_schema_contract();
        contract.bases.push(contract.bases[0].clone());
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("base count must precede target identity");
        assert_eq!(
            error.to_string(),
            "Iceberg join MV schema contract requires two base contracts, got 3"
        );

        contract = join_schema_contract();
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("target identity must precede base validation");
        assert_eq!(
            error.to_string(),
            "iceberg join MV refresh blocked: target table identity changed; recreate the MV"
        );
        assert!(!error.to_string().contains("target-uuid"));
        assert!(!error.to_string().contains("wrong-target"));

        target.table_uuid = "target-uuid".to_string();
        left.format_v3 = false;
        let error = validate_test_join(
            &contract,
            "ice.db.missing",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("row-lineage must precede contract lookup");
        assert_eq!(
            error.to_string(),
            "iceberg-backed materialized views require base table ice.db.missing to be Iceberg format-version=3 with write.row-lineage=true; upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        );

        left.format_v3 = true;
        left.row_lineage_enabled = false;
        let error = validate_test_join(
            &contract,
            "ice.db.missing",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("row-lineage property must share the combined error");
        assert_eq!(
            error.to_string(),
            "iceberg-backed materialized views require base table ice.db.missing to be Iceberg format-version=3 with write.row-lineage=true; upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        );

        left.row_lineage_enabled = true;
        let error = validate_test_join(
            &contract,
            "ice.db.missing",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("missing contract must precede identity");
        assert_eq!(
            error.to_string(),
            "Iceberg join MV schema contract missing base ice.db.missing"
        );

        left.table_uuid = "wrong-left".to_string();
        right.table_uuid = "wrong-right".to_string();
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("caller-provided base order must be preserved");
        assert_eq!(
            error.to_string(),
            "iceberg join MV refresh blocked: base table identity changed for ice.db.left; recreate the MV"
        );

        left.table_uuid = "left-uuid".to_string();
        right.table_uuid = "right-uuid".to_string();
        left.schema = test_schema(2, Vec::new());
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("field existence must precede later base validation");
        assert_eq!(
            error.to_string(),
            "iceberg join MV refresh blocked: base column \"left_id\" (field id 1) was dropped from ice.db.left; recreate the MV"
        );

        left.schema = test_schema(2, vec![req(1, "left_id", long_type)]);
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("type drift must fail");
        assert_eq!(
            error,
            JoinSchemaValidationError::BaseFieldTypeChanged {
                base_fqn: "ice.db.left".to_string(),
                field_id: 1,
                name_at_create: "left_id".to_string(),
                from: "int".to_string(),
                to: "long".to_string(),
            }
        );
        assert_eq!(
            error.to_string(),
            "iceberg join MV refresh blocked: base column \"left_id\" (field id 1) changed type from int to long; recreate the MV"
        );

        left.schema = test_schema(2, vec![opt(1, "left_id", int_type)]);
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("nullability drift must fail");
        assert_eq!(
            error,
            JoinSchemaValidationError::BaseFieldNullabilityChanged {
                base_fqn: "ice.db.left".to_string(),
                field_id: 1,
                name_at_create: "left_id".to_string(),
                from_required: true,
                to_required: false,
            }
        );
        assert_eq!(
            error.to_string(),
            "iceberg join MV refresh blocked: base column \"left_id\" (field id 1) changed nullability; recreate the MV"
        );

        left.schema = test_schema(2, vec![req(1, "left_id", int_type)]);
        target.format_v3 = false;
        contract.target.partition = Some(MvPartitionContract {
            target_spec_id: 1,
            fields: Vec::new(),
        });
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("generic target identity guard must precede partition");
        assert_eq!(
            error.to_string(),
            "iceberg MV refresh blocked: target table row-lineage contract broken (target table must be Iceberg format v3, found non-v3); recreate the MV"
        );

        target.format_v3 = true;
        target.schema = test_schema(2, Vec::new());
        let error = validate_test_join(
            &contract,
            "ice.db.left",
            &left,
            "ice.db.right",
            &right,
            &target,
        )
        .expect_err("partition must precede target schema");
        assert_eq!(
            error.to_string(),
            "iceberg MV refresh blocked: target partition spec changed externally (expected default spec id 1, got 0); recreate the MV"
        );
    }

    #[test]
    fn branch_id_field_validation_preserves_exact_failures() {
        let contract = BranchIdColumnContract {
            column_name: BRANCH_ID_COLUMN_NAME.to_string(),
            target_field_id: 2,
        };
        let schema =
            |field: Option<MvObservedTargetField>| test_schema(2, field.into_iter().collect());
        let cases = vec![
            (
                schema(None),
                BranchFieldValidationError::Missing { field_id: 2 },
            ),
            (
                schema(Some(req(2, "renamed_branch", "int"))),
                BranchFieldValidationError::Renamed {
                    expected: BRANCH_ID_COLUMN_NAME.to_string(),
                    actual: "renamed_branch".to_string(),
                },
            ),
            (
                schema(Some(opt(2, BRANCH_ID_COLUMN_NAME, "int"))),
                BranchFieldValidationError::NotRequired,
            ),
            (
                schema(Some(req(2, BRANCH_ID_COLUMN_NAME, "long"))),
                BranchFieldValidationError::WrongType {
                    expected: "Int".to_string(),
                    actual: "long".to_string(),
                },
            ),
        ];

        for (schema, expected) in cases {
            assert_eq!(validate_branch_id_field(&contract, &schema), Err(expected));
        }
    }

    #[test]
    fn join_row_key_target_hidden_column_is_accepted() {
        let target_type = "int";
        let base_schema = test_schema(7, vec![]);
        let target_schema = test_schema(
            11,
            vec![
                req(1, "id", target_type),
                req(2, JOIN_APPLY_KEY_COLUMN_NAME, "string"),
            ],
        );
        let contract = MvSchemaContract {
            contract_version: 2,
            base: BaseContract {
                table_fqn: "ice.db.left".to_string(),
                table_uuid: "left-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![
                BaseContract {
                    table_fqn: "ice.db.left".to_string(),
                    table_uuid: "left-uuid".to_string(),
                    alias_at_create: Some("l".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "id".to_string(),
                            type_signature: target_type.to_string(),
                            required: true,
                        }],
                    },
                },
                BaseContract {
                    table_fqn: "ice.db.right".to_string(),
                    table_uuid: "right-uuid".to_string(),
                    alias_at_create: Some("r".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 2,
                            name_at_create: "id".to_string(),
                            type_signature: target_type.to_string(),
                            required: true,
                        }],
                    },
                },
            ],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![],
                        referenced_base_fields: vec![QualifiedFieldLineage {
                            table_fqn: "ice.db.left".to_string(),
                            qualifier_at_create: "l".to_string(),
                            field_id: 1,
                        }],
                    },
                }],
                filter: None,
            },
            join: Some(JoinContract {
                kind: JoinContractKind::InnerEquiJoin,
                predicates: vec![JoinPredicateLineage {
                    left: QualifiedFieldLineage {
                        table_fqn: "ice.db.left".to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: "ice.db.right".to_string(),
                        qualifier_at_create: "r".to_string(),
                        field_id: 2,
                    },
                }],
            }),
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_join".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: target_type.to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::JoinRowKey,
                },
                partition: None,
            },
        };

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }

    #[test]
    fn aggregate_state_target_layout_is_accepted() {
        let target_type = "long";
        let base_schema = test_schema(7, vec![]);
        let target_schema = aggregate_target_schema("__agg_state_c", "long", false);
        let contract = aggregate_schema_contract(target_type.to_string());

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }

    #[test]
    fn aggregate_state_target_layout_rejects_renamed_state_column() {
        let target_type = "long";
        let base_schema = test_schema(7, vec![]);
        let target_schema = aggregate_target_schema("renamed_state", "long", false);
        let contract = aggregate_schema_contract(target_type.to_string());

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("renamed"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_type_changed_state_column() {
        let target_type = "long";
        let base_schema = test_schema(7, vec![]);
        let target_schema = aggregate_target_schema("__agg_state_c", "string", false);
        let contract = aggregate_schema_contract(target_type.to_string());

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("changed type"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_validation_runs_on_schema_id_fast_path() {
        let target_type = "long";
        let base_schema = test_schema(0, vec![]);
        let target_schema = aggregate_target_schema("__agg_state_c", "string", false);
        let mut contract = aggregate_schema_contract(target_type.to_string());
        contract.target.schema_id_at_create = 11;

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("changed type"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_nullable_row_id_column() {
        let target_type = "long";
        let base_schema = test_schema(7, vec![]);
        let target_schema =
            aggregate_target_schema_with_row_id("__agg_state_c", "long", false, 2, true);
        let contract = aggregate_schema_contract(target_type.to_string());

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("row-id"), "reason={reason}");
                assert!(reason.contains("required"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_row_id_that_is_not_hidden_apply_key() {
        let target_type = "long";
        let base_schema = test_schema(7, vec![]);
        let target_schema = aggregate_target_schema_with_extra_string_column(
            "__agg_state_c",
            "long",
            "other_key",
            false,
        );
        let mut contract = aggregate_schema_contract(target_type.to_string());
        contract
            .aggregate
            .as_mut()
            .expect("aggregate")
            .row_id_column_name = "other_key".to_string();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("row-id"), "reason={reason}");
                assert!(reason.contains("__row_id__"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    fn aggregate_target_schema_with_extra_string_column(
        state_column_name: &str,
        state_column_type: &str,
        extra_column_name: &str,
        extra_nullable: bool,
    ) -> TestSchema {
        let extra_field = if extra_nullable {
            opt(4, extra_column_name, "string")
        } else {
            req(4, extra_column_name, "string")
        };
        let mut fields =
            aggregate_target_schema(state_column_name, state_column_type, false).fields;
        fields.push(extra_field);
        test_schema(11, fields)
    }

    fn aggregate_target_schema(
        state_column_name: &str,
        state_column_type: &str,
        state_column_nullable: bool,
    ) -> TestSchema {
        aggregate_target_schema_with_row_id(
            state_column_name,
            state_column_type,
            state_column_nullable,
            2,
            false,
        )
    }

    fn aggregate_target_schema_with_row_id(
        state_column_name: &str,
        state_column_type: &str,
        state_column_nullable: bool,
        row_id_field_id: i32,
        row_id_nullable: bool,
    ) -> TestSchema {
        let state_field = if state_column_nullable {
            opt(3, state_column_name, state_column_type)
        } else {
            req(3, state_column_name, state_column_type)
        };
        let row_id_field = if row_id_nullable {
            opt(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                "string",
            )
        } else {
            req(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                "string",
            )
        };
        test_schema(11, vec![req(1, "id", "long"), row_id_field, state_field])
    }

    fn aggregate_schema_contract(state_type_signature: String) -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 3,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: Some(AggregateStateContract {
                state_layout_version: 1,
                row_id_column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                state_columns: vec![AggregateStateColumnContract {
                    column_name: "__agg_state_c".to_string(),
                    target_field_id: 3,
                    type_signature: state_type_signature,
                    nullable: false,
                    role: AggregateStateRoleContract::Single,
                }],
            }),
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_agg".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::GroupRowId,
                },
                partition: None,
            },
        }
    }
}
