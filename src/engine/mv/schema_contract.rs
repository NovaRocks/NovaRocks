//! IVM-A11 refresh-time schema contract validator.
//!
//! Single entry point: `validate_schema_contract`. Three-stage check:
//!   1. identity guard (uuid + format-version + row-lineage)
//!   2. schema-id fast path + base referenced-field exact match
//!   3. target visible columns + hidden apply-key exact match
//!
//! Decisions are explicit. There is NO fallback path: incompatible
//! contracts result in fail-fast errors that propagate to the user.

use crate::meta::repository::mv_contract::{
    ApplyKeySource, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME,
    JOIN_APPLY_KEY_COLUMN_NAME, MvPartitionTransformContract, MvSchemaContract,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        /// (base field id, name_at_create, current_name)
        rebound_columns: Vec<(i32, String, String)>,
    },
    Incompatible(SchemaEvolutionError),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SchemaEvolutionError {
    BaseTableIdentityChanged {
        expected: String,
        actual: String,
    },
    BaseRowLineageContractBroken {
        reason: String,
    },
    BaseFieldDropped {
        field_id: i32,
        name_at_create: String,
    },
    BaseFieldTypeChanged {
        field_id: i32,
        name_at_create: String,
        from: String,
        to: String,
    },
    TargetTableIdentityChanged {
        expected: String,
        actual: String,
    },
    TargetRowLineageContractBroken {
        reason: String,
    },
    TargetVisibleFieldDropped {
        output_name: String,
        target_field_id: i32,
    },
    TargetVisibleFieldRenamed {
        target_field_id: i32,
        expected: String,
        actual: String,
    },
    TargetVisibleFieldTypeChanged {
        target_field_id: i32,
        from: String,
        to: String,
    },
    HiddenApplyKeyContractBroken {
        reason: String,
    },
    TargetPartitionSpecChanged {
        reason: String,
    },
    AggregateStateContractBroken {
        reason: String,
    },
}

impl std::fmt::Display for SchemaEvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaseTableIdentityChanged { expected, actual } => write!(
                f,
                "iceberg MV refresh blocked: base table identity changed (uuid expected={expected}, actual={actual}); run REFRESH FULL or recreate the MV"
            ),
            Self::BaseRowLineageContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: base table row-lineage contract broken ({reason}); run REFRESH FULL or recreate the MV"
            ),
            Self::BaseFieldDropped {
                field_id,
                name_at_create,
            } => write!(
                f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) was dropped from base table; run REFRESH FULL or recreate the MV"
            ),
            Self::BaseFieldTypeChanged {
                field_id,
                name_at_create,
                from,
                to,
            } => write!(
                f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) changed type from {from} to {to}; run REFRESH FULL or recreate the MV"
            ),
            Self::TargetTableIdentityChanged { expected, actual } => write!(
                f,
                "iceberg MV refresh blocked: target table identity changed (uuid expected={expected}, actual={actual}); recreate the MV"
            ),
            Self::TargetRowLineageContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target table row-lineage contract broken ({reason}); recreate the MV"
            ),
            Self::TargetVisibleFieldDropped {
                output_name,
                target_field_id,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column \"{output_name}\" (field id {target_field_id}) was dropped; recreate the MV"
            ),
            Self::TargetVisibleFieldRenamed {
                target_field_id,
                expected,
                actual,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) renamed externally: expected \"{expected}\", actual \"{actual}\"; recreate the MV"
            ),
            Self::TargetVisibleFieldTypeChanged {
                target_field_id,
                from,
                to,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) changed type from {from} to {to}; recreate the MV"
            ),
            Self::HiddenApplyKeyContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target hidden apply-key column contract broken ({reason}); recreate the MV"
            ),
            Self::TargetPartitionSpecChanged { reason } => write!(
                f,
                "iceberg MV refresh blocked: target partition spec changed externally ({reason}); recreate the MV"
            ),
            Self::AggregateStateContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target aggregate state contract broken ({reason}); recreate the MV"
            ),
        }
    }
}

impl std::error::Error for SchemaEvolutionError {}

const ICEBERG_ROW_LINEAGE_PROP: &str = "write.row-lineage";

pub(crate) fn validate_schema_contract(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::table::Table,
    current_target_table: &iceberg::table::Table,
) -> ContractDecision {
    validate_schema_contract_with_base_schema(
        contract,
        current_base_table,
        current_base_table.metadata().current_schema(),
        current_target_table,
    )
}

pub(crate) fn validate_schema_contract_with_base_schema(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::table::Table,
    base_schema: &iceberg::spec::Schema,
    current_target_table: &iceberg::table::Table,
) -> ContractDecision {
    // Stage 1: identity guard.
    if let Some(err) = validate_identity_guards(contract, current_base_table, current_target_table)
    {
        return ContractDecision::Incompatible(err);
    }
    if let Some(err) = check_target_partition_spec(
        contract,
        current_target_table.metadata().default_partition_spec(),
    ) {
        return ContractDecision::Incompatible(err);
    }
    validate_schema_contract_after_identity(
        contract,
        base_schema,
        current_target_table.metadata().current_schema(),
    )
}

fn validate_schema_contract_after_identity(
    contract: &MvSchemaContract,
    base_schema: &iceberg::spec::Schema,
    target_schema: &iceberg::spec::Schema,
) -> ContractDecision {
    // Stage 2 fast path.
    if base_schema.schema_id() == contract.base.schema_id_at_create
        && target_schema.schema_id() == contract.target.schema_id_at_create
    {
        if contract.aggregate.is_some() {
            if let Some(err) = check_target_schema(contract, target_schema) {
                return ContractDecision::Incompatible(err);
            }
        }
        return ContractDecision::CompatibleSafe;
    }
    // Stage 2 precise base check.
    let rebound = match check_base_referenced_fields(contract, base_schema) {
        Err(err) => return ContractDecision::Incompatible(err),
        Ok(r) => r,
    };
    // Stage 3 target check.
    if let Some(err) = check_target_schema(contract, target_schema) {
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
    base: &iceberg::table::Table,
    target: &iceberg::table::Table,
) -> Option<SchemaEvolutionError> {
    let actual_base_uuid = base.metadata().uuid().to_string();
    if actual_base_uuid != contract.base.table_uuid {
        return Some(SchemaEvolutionError::BaseTableIdentityChanged {
            expected: contract.base.table_uuid.clone(),
            actual: actual_base_uuid,
        });
    }
    if base.metadata().format_version() != iceberg::spec::FormatVersion::V3 {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: format!(
                "base table must be Iceberg format v3, found {:?}",
                base.metadata().format_version()
            ),
        });
    }
    if !row_lineage_enabled(base.metadata().properties()) {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: "base table property write.row-lineage must be true".to_string(),
        });
    }

    let actual_target_uuid = target.metadata().uuid().to_string();
    if actual_target_uuid != contract.target.table_uuid {
        return Some(SchemaEvolutionError::TargetTableIdentityChanged {
            expected: contract.target.table_uuid.clone(),
            actual: actual_target_uuid,
        });
    }
    if target.metadata().format_version() != iceberg::spec::FormatVersion::V3 {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: format!(
                "target table must be Iceberg format v3, found {:?}",
                target.metadata().format_version()
            ),
        });
    }
    if !row_lineage_enabled(target.metadata().properties()) {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: "target table property write.row-lineage must be true".to_string(),
        });
    }
    None
}

fn check_base_referenced_fields(
    contract: &MvSchemaContract,
    base_schema: &iceberg::spec::Schema,
) -> Result<Vec<(i32, String, String)>, SchemaEvolutionError> {
    let current = base_schema.as_struct();
    let mut rebound = Vec::new();
    for record in &contract.base.schema_at_create.fields {
        let Some(field) = current.fields().iter().find(|f| f.id == record.field_id) else {
            return Err(SchemaEvolutionError::BaseFieldDropped {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_signature = format!("{}", field.field_type);
        if current_signature != record.type_signature {
            return Err(SchemaEvolutionError::BaseFieldTypeChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_signature,
            });
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push((
                record.field_id,
                record.name_at_create.clone(),
                field.name.clone(),
            ));
        }
    }
    Ok(rebound)
}

fn check_target_partition_spec(
    contract: &MvSchemaContract,
    current_spec: &iceberg::spec::PartitionSpec,
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
        if actual.field_id != expected.partition_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} id expected {}, got {}",
                    expected.partition_field_id, actual.field_id
                ),
            });
        }
        if actual.source_id != expected.source_target_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} source id expected {}, got {}",
                    expected.partition_field_name,
                    expected.source_target_field_id,
                    actual.source_id
                ),
            });
        }
        if actual.name != expected.partition_field_name {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} name expected {}, got {}",
                    expected.partition_field_name, actual.name
                ),
            });
        }
        let Some(actual_transform) = partition_transform_contract(&actual.transform) else {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} has unsupported transform {:?}",
                    actual.name, actual.transform
                ),
            });
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

fn partition_transform_contract(
    transform: &iceberg::spec::Transform,
) -> Option<MvPartitionTransformContract> {
    match transform {
        iceberg::spec::Transform::Identity => Some(MvPartitionTransformContract::Identity),
        iceberg::spec::Transform::Year => Some(MvPartitionTransformContract::Year),
        iceberg::spec::Transform::Month => Some(MvPartitionTransformContract::Month),
        iceberg::spec::Transform::Day => Some(MvPartitionTransformContract::Day),
        iceberg::spec::Transform::Hour => Some(MvPartitionTransformContract::Hour),
        iceberg::spec::Transform::Bucket(num_buckets) => {
            Some(MvPartitionTransformContract::Bucket {
                num_buckets: *num_buckets,
            })
        }
        iceberg::spec::Transform::Truncate(width) => {
            Some(MvPartitionTransformContract::Truncate { width: *width })
        }
        iceberg::spec::Transform::Void => Some(MvPartitionTransformContract::Void),
        iceberg::spec::Transform::Unknown => None,
    }
}

fn check_target_schema(
    contract: &MvSchemaContract,
    target_schema: &iceberg::spec::Schema,
) -> Option<SchemaEvolutionError> {
    let current = target_schema.as_struct();
    for tv in &contract.target.visible_columns {
        let Some(field) = current.fields().iter().find(|f| f.id == tv.target_field_id) else {
            return Some(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: tv.output_name.clone(),
                target_field_id: tv.target_field_id,
            });
        };
        let sig = format!("{}", field.field_type);
        if sig != tv.type_signature {
            return Some(SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                target_field_id: tv.target_field_id,
                from: tv.type_signature.clone(),
                to: sig,
            });
        }
        if !field.name.eq_ignore_ascii_case(&tv.output_name) {
            return Some(SchemaEvolutionError::TargetVisibleFieldRenamed {
                target_field_id: tv.target_field_id,
                expected: tv.output_name.clone(),
                actual: field.name.clone(),
            });
        }
    }

    let expected = &contract.target.hidden_apply_key;
    let Some(field) = current
        .fields()
        .iter()
        .find(|f| f.id == expected.target_field_id)
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
        .name
        .eq_ignore_ascii_case(expected_hidden_apply_key_column)
    {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key column renamed to {}", field.name),
        });
    }
    if let Some(err) = check_aggregate_state_schema(contract, current) {
        return Some(err);
    }
    if !field.required {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: "hidden apply-key column must be required".to_string(),
        });
    }
    let expected_apply_key_type = match expected.source {
        ApplyKeySource::BaseRowId => iceberg::spec::PrimitiveType::Long,
        ApplyKeySource::JoinRowKey | ApplyKeySource::GroupRowId => {
            iceberg::spec::PrimitiveType::String
        }
    };
    match field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(actual) if actual == &expected_apply_key_type => {}
        other => {
            return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
                reason: format!(
                    "hidden apply-key column must be {expected_apply_key_type:?}, got {other}"
                ),
            });
        }
    }
    None
}

fn check_aggregate_state_schema(
    contract: &MvSchemaContract,
    current: &iceberg::spec::StructType,
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
    let mut row_id_matches = current.fields().iter().filter(|field| {
        field
            .name
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
    if row_id_field.id != contract.target.hidden_apply_key.target_field_id {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id field id {} must match hidden apply-key field id {}",
                row_id_field.id, contract.target.hidden_apply_key.target_field_id
            ),
        });
    }
    if !row_id_field.required {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} must be required",
                aggregate.row_id_column_name
            ),
        });
    }
    match row_id_field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String) => {}
        other => {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate row-id column {} must be String, got {other}",
                    aggregate.row_id_column_name
                ),
            });
        }
    }

    for state_col in &aggregate.state_columns {
        let Some(field) = current
            .fields()
            .iter()
            .find(|field| field.id == state_col.target_field_id)
        else {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} not found",
                    state_col.column_name, state_col.target_field_id
                ),
            });
        };
        if !field.name.eq_ignore_ascii_case(&state_col.column_name) {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} renamed to {}",
                    state_col.column_name, state_col.target_field_id, field.name
                ),
            });
        }
        let sig = format!("{}", field.field_type);
        if sig != state_col.type_signature {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} changed type from {} to {}",
                    state_col.column_name, state_col.target_field_id, state_col.type_signature, sig
                ),
            });
        }
        let actual_nullable = !field.required;
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

fn row_lineage_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get(ICEBERG_ROW_LINEAGE_PROP)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HiddenApplyKeyContract,
        JOIN_APPLY_KEY_COLUMN_NAME, JoinContract, JoinContractKind, JoinPredicateLineage,
        MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
        OutputColumnLineage, OutputContract, QualifiedFieldLineage, TargetContract,
        TargetVisibleColumn,
    };
    use std::sync::Arc;

    // NOTE: building real `iceberg::table::Table` instances is heavy.
    // These tests cover the SchemaEvolutionError Display + the
    // sanity-test pure-function checks would need iceberg fixtures.
    // End-to-end tests run via the SQL integration suite in Task 13.

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
    fn schema_evolution_error_target_messages_recommend_recreate() {
        let err = SchemaEvolutionError::TargetTableIdentityChanged {
            expected: "A".into(),
            actual: "B".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("recreate the MV"));
    }

    #[test]
    fn row_lineage_enabled_recognizes_case_insensitive_true() {
        let mut p = std::collections::HashMap::new();
        p.insert("write.row-lineage".to_string(), "TRUE".to_string());
        assert!(row_lineage_enabled(&p));
        p.insert("write.row-lineage".to_string(), "false".to_string());
        assert!(!row_lineage_enabled(&p));
        p.clear();
        assert!(!row_lineage_enabled(&p));
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
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
        };

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("schema"),
        );
        let matching_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id_bucket_16", Transform::Bucket(16))
            .expect("partition field")
            .build()
            .bind(Arc::clone(&schema))
            .expect("bind spec");
        let changed_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id_bucket_8", Transform::Bucket(8))
            .expect("partition field")
            .build()
            .bind(schema)
            .expect("bind spec");
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

        assert_eq!(check_target_partition_spec(&contract, &matching_spec), None);
        assert!(matches!(
            check_target_partition_spec(&contract, &changed_spec),
            Some(SchemaEvolutionError::TargetPartitionSpecChanged { .. })
        ));
    }

    #[test]
    fn supplied_base_schema_drives_base_rebind_decision() {
        let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "renamed_id",
                base_type.clone(),
            ))])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("target schema");
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
                        type_signature: format!("{base_type}"),
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
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
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
                rebound_columns: vec![(1, "id".to_string(), "renamed_id".to_string())],
            }
        );
    }

    fn minimal_base_row_id_contract() -> MvSchemaContract {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
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
                        type_signature: format!("{target_type}"),
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
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
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

    #[test]
    fn join_row_key_target_hidden_column_is_accepted() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    JOIN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("target schema");
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
                            type_signature: format!("{target_type}"),
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
                            type_signature: format!("{target_type}"),
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
            target: TargetContract {
                table_fqn: "ice.db.mv_join".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
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
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::Long, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }

    #[test]
    fn aggregate_state_target_layout_rejects_renamed_state_column() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("renamed_state", iceberg::spec::PrimitiveType::Long, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

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
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::String, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

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
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::String, false);
        let mut contract = aggregate_schema_contract(format!("{target_type}"));
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
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = aggregate_target_schema_with_row_id(
            "__agg_state_c",
            iceberg::spec::PrimitiveType::Long,
            false,
            2,
            true,
        );
        let contract = aggregate_schema_contract(format!("{target_type}"));

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
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = aggregate_target_schema_with_extra_string_column(
            "__agg_state_c",
            iceberg::spec::PrimitiveType::Long,
            "other_key",
            false,
        );
        let mut contract = aggregate_schema_contract(format!("{target_type}"));
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
        state_column_type: iceberg::spec::PrimitiveType,
        extra_column_name: &str,
        extra_nullable: bool,
    ) -> iceberg::spec::Schema {
        let extra_field = if extra_nullable {
            iceberg::spec::NestedField::optional(
                4,
                extra_column_name,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        } else {
            iceberg::spec::NestedField::required(
                4,
                extra_column_name,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        };
        let mut fields = aggregate_target_schema(state_column_name, state_column_type, false)
            .as_struct()
            .fields()
            .to_vec();
        fields.push(Arc::new(extra_field));
        iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(fields)
            .build()
            .expect("target schema")
    }

    fn aggregate_target_schema(
        state_column_name: &str,
        state_column_type: iceberg::spec::PrimitiveType,
        state_column_nullable: bool,
    ) -> iceberg::spec::Schema {
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
        state_column_type: iceberg::spec::PrimitiveType,
        state_column_nullable: bool,
        row_id_field_id: i32,
        row_id_nullable: bool,
    ) -> iceberg::spec::Schema {
        let state_type = iceberg::spec::Type::Primitive(state_column_type);
        let state_field = if state_column_nullable {
            iceberg::spec::NestedField::optional(3, state_column_name, state_type)
        } else {
            iceberg::spec::NestedField::required(3, state_column_name, state_type)
        };
        let row_id_field = if row_id_nullable {
            iceberg::spec::NestedField::optional(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        } else {
            iceberg::spec::NestedField::required(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        };
        iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
                Arc::new(row_id_field),
                Arc::new(state_field),
            ])
            .build()
            .expect("target schema")
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
