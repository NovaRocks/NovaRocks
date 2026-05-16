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
    ApplyKeySource, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME, MvSchemaContract,
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
    };
    if !field
        .name
        .eq_ignore_ascii_case(expected_hidden_apply_key_column)
    {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key column renamed to {}", field.name),
        });
    }
    if !field.required {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: "hidden apply-key column must be required".to_string(),
        });
    }
    match field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long) => {}
        other => {
            return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
                reason: format!("hidden apply-key column must be long, got {other}"),
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
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, JOIN_APPLY_KEY_COLUMN_NAME, JoinContract,
        JoinContractKind, JoinPredicateLineage, OutputColumnLineage, OutputContract,
        QualifiedFieldLineage, TargetContract, TargetVisibleColumn,
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
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
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
            },
        };

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }
}
