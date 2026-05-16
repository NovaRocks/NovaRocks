//! IVM-A11 MV schema / field-id contract.
//!
//! Persisted inside `StoredMvDefinition.schema_contract`. Captures base
//! referenced fields + output lineage + target schema mapping at CREATE
//! MV time. Validated on every REFRESH.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvSchemaContract {
    pub contract_version: u16,
    pub base: BaseContract,
    #[serde(default)]
    pub bases: Vec<BaseContract>,
    pub output: OutputContract,
    #[serde(default)]
    pub join: Option<JoinContract>,
    pub target: TargetContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseContract {
    pub table_fqn: String,
    pub table_uuid: String,
    #[serde(default)]
    pub alias_at_create: Option<String>,
    pub schema_id_at_create: i32,
    pub schema_at_create: BaseSchemaSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSchemaSnapshot {
    pub fields: Vec<BaseFieldRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseFieldRecord {
    pub field_id: i32,
    pub name_at_create: String,
    pub type_signature: String,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    pub columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputColumnLineage {
    pub expression: ExpressionLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualifiedFieldLineage {
    pub table_fqn: String,
    pub qualifier_at_create: String,
    pub field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinContract {
    pub kind: JoinContractKind,
    pub predicates: Vec<JoinPredicateLineage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JoinContractKind {
    InnerEquiJoin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPredicateLineage {
    pub left: QualifiedFieldLineage,
    pub right: QualifiedFieldLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpressionLineage {
    pub kind: ExpressionKind,
    pub referenced_base_field_ids: Vec<i32>,
    #[serde(default)]
    pub referenced_base_fields: Vec<QualifiedFieldLineage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterLineage {
    pub referenced_base_field_ids: Vec<i32>,
    #[serde(default)]
    pub referenced_base_fields: Vec<QualifiedFieldLineage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetContract {
    pub table_fqn: String,
    pub table_uuid: String,
    pub schema_id_at_create: i32,
    pub visible_columns: Vec<TargetVisibleColumn>,
    pub hidden_apply_key: HiddenApplyKeyContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetVisibleColumn {
    pub output_name: String,
    pub target_field_id: i32,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiddenApplyKeyContract {
    pub column_name: String,
    pub target_field_id: i32,
    pub source: ApplyKeySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
    JoinRowKey,
}

/// Errors returned by `MvSchemaContract::ensure_self_consistent`.
/// These indicate the contract was constructed incorrectly at CREATE
/// time — they should never surface to end users in practice.
#[derive(Debug, PartialEq, Eq)]
pub enum ContractSelfCheckError {
    OutputTargetLenMismatch {
        output_len: usize,
        target_len: usize,
    },
    HiddenApplyKeyColumnNameWrong {
        expected: String,
        actual: String,
    },
    OutputReferencesUnknownBaseFieldId {
        output_index: usize,
        field_id: i32,
    },
    OutputReferencesUnknownQualifiedBaseField {
        output_index: usize,
        table_fqn: String,
        field_id: i32,
    },
    FilterReferencesUnknownBaseFieldId {
        field_id: i32,
    },
    FilterReferencesUnknownQualifiedBaseField {
        table_fqn: String,
        field_id: i32,
    },
    JoinReferencesUnknownQualifiedBaseField {
        table_fqn: String,
        field_id: i32,
    },
    EmptyBaseTableUuid,
    NegativeBaseSchemaId(i32),
    DuplicateBaseFieldIdWithDifferentType {
        field_id: i32,
        first: String,
        second: String,
    },
}

impl std::fmt::Display for ContractSelfCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputTargetLenMismatch {
                output_len,
                target_len,
            } => {
                write!(
                    f,
                    "MV contract output columns ({output_len}) and target visible columns ({target_len}) must have the same length"
                )
            }
            Self::HiddenApplyKeyColumnNameWrong { expected, actual } => {
                write!(
                    f,
                    "MV contract hidden apply-key column name expected {expected}, got {actual}"
                )
            }
            Self::OutputReferencesUnknownBaseFieldId {
                output_index,
                field_id,
            } => {
                write!(
                    f,
                    "MV contract output column #{output_index} references base field id {field_id} that is not in base.schema_at_create"
                )
            }
            Self::OutputReferencesUnknownQualifiedBaseField {
                output_index,
                table_fqn,
                field_id,
            } => {
                write!(
                    f,
                    "MV contract output column #{output_index} references unknown base field {table_fqn}#{field_id}"
                )
            }
            Self::FilterReferencesUnknownBaseFieldId { field_id } => {
                write!(
                    f,
                    "MV contract WHERE filter references base field id {field_id} that is not in base.schema_at_create"
                )
            }
            Self::FilterReferencesUnknownQualifiedBaseField {
                table_fqn,
                field_id,
            } => {
                write!(
                    f,
                    "MV contract WHERE filter references unknown base field {table_fqn}#{field_id}"
                )
            }
            Self::JoinReferencesUnknownQualifiedBaseField {
                table_fqn,
                field_id,
            } => {
                write!(
                    f,
                    "MV contract JOIN predicate references unknown base field {table_fqn}#{field_id}"
                )
            }
            Self::EmptyBaseTableUuid => write!(f, "MV contract base.table_uuid is empty"),
            Self::NegativeBaseSchemaId(id) => {
                write!(f, "MV contract base.schema_id_at_create is negative: {id}")
            }
            Self::DuplicateBaseFieldIdWithDifferentType {
                field_id,
                first,
                second,
            } => {
                write!(
                    f,
                    "MV contract base.schema_at_create contains field id {field_id} twice with different type signatures: {first} vs {second}"
                )
            }
        }
    }
}

impl std::error::Error for ContractSelfCheckError {}

pub const HIDDEN_APPLY_KEY_COLUMN_NAME: &str = "__nova_base_row_id";
pub const JOIN_APPLY_KEY_COLUMN_NAME: &str = "__nova_join_row_key";

impl MvSchemaContract {
    fn effective_bases(&self) -> Vec<&BaseContract> {
        if self.bases.is_empty() {
            vec![&self.base]
        } else {
            self.bases.iter().collect()
        }
    }

    /// Cheap structural self-check run at CREATE time. Does NOT consult
    /// the live Iceberg tables — that part lives in
    /// `validate_schema_contract` and runs at REFRESH time.
    pub fn ensure_self_consistent(&self) -> Result<(), ContractSelfCheckError> {
        if self.output.columns.len() != self.target.visible_columns.len() {
            return Err(ContractSelfCheckError::OutputTargetLenMismatch {
                output_len: self.output.columns.len(),
                target_len: self.target.visible_columns.len(),
            });
        }
        let expected_hidden_apply_key_column = match self.target.hidden_apply_key.source {
            ApplyKeySource::BaseRowId => HIDDEN_APPLY_KEY_COLUMN_NAME,
            ApplyKeySource::JoinRowKey => JOIN_APPLY_KEY_COLUMN_NAME,
        };
        if self.target.hidden_apply_key.column_name != expected_hidden_apply_key_column {
            return Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong {
                expected: expected_hidden_apply_key_column.to_string(),
                actual: self.target.hidden_apply_key.column_name.clone(),
            });
        }
        if self.base.table_uuid.is_empty() {
            return Err(ContractSelfCheckError::EmptyBaseTableUuid);
        }
        if self.base.schema_id_at_create < 0 {
            return Err(ContractSelfCheckError::NegativeBaseSchemaId(
                self.base.schema_id_at_create,
            ));
        }
        let known_field_ids: std::collections::BTreeSet<i32> = self
            .base
            .schema_at_create
            .fields
            .iter()
            .map(|f| f.field_id)
            .collect();
        for (i, col) in self.output.columns.iter().enumerate() {
            for fid in &col.expression.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId {
                        output_index: i,
                        field_id: *fid,
                    });
                }
            }
        }
        if let Some(filter) = &self.output.filter {
            for fid in &filter.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::FilterReferencesUnknownBaseFieldId {
                        field_id: *fid,
                    });
                }
            }
        }
        let bases = self.effective_bases();
        for (i, col) in self.output.columns.iter().enumerate() {
            for field in &col.expression.referenced_base_fields {
                if !qualified_field_known(&bases, field) {
                    return Err(
                        ContractSelfCheckError::OutputReferencesUnknownQualifiedBaseField {
                            output_index: i,
                            table_fqn: field.table_fqn.clone(),
                            field_id: field.field_id,
                        },
                    );
                }
            }
        }
        if let Some(filter) = &self.output.filter {
            for field in &filter.referenced_base_fields {
                if !qualified_field_known(&bases, field) {
                    return Err(
                        ContractSelfCheckError::FilterReferencesUnknownQualifiedBaseField {
                            table_fqn: field.table_fqn.clone(),
                            field_id: field.field_id,
                        },
                    );
                }
            }
        }
        if let Some(join) = &self.join {
            for pred in &join.predicates {
                for field in [&pred.left, &pred.right] {
                    if !qualified_field_known(&bases, field) {
                        return Err(
                            ContractSelfCheckError::JoinReferencesUnknownQualifiedBaseField {
                                table_fqn: field.table_fqn.clone(),
                                field_id: field.field_id,
                            },
                        );
                    }
                }
            }
        }
        let mut seen: std::collections::BTreeMap<i32, &str> = std::collections::BTreeMap::new();
        for f in &self.base.schema_at_create.fields {
            if let Some(prev) = seen.get(&f.field_id) {
                if *prev != f.type_signature.as_str() {
                    return Err(
                        ContractSelfCheckError::DuplicateBaseFieldIdWithDifferentType {
                            field_id: f.field_id,
                            first: prev.to_string(),
                            second: f.type_signature.clone(),
                        },
                    );
                }
            } else {
                seen.insert(f.field_id, &f.type_signature);
            }
        }
        Ok(())
    }
}

fn qualified_field_known(bases: &[&BaseContract], field: &QualifiedFieldLineage) -> bool {
    bases.iter().any(|base| {
        base.table_fqn == field.table_fqn
            && matches!(
                base.alias_at_create.as_deref(),
                Some(alias) if alias.eq_ignore_ascii_case(&field.qualifier_at_create)
            )
            && base
                .schema_at_create
                .fields
                .iter()
                .any(|record| record.field_id == field.field_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
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
                table_fqn: "ice.mv.orders_mv".to_string(),
                table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
            },
        }
    }

    #[test]
    fn contract_round_trips_through_serde_json() {
        let c = sample_contract();
        let json = serde_json::to_string(&c).expect("serialize");
        let decoded: MvSchemaContract = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, c);
    }

    #[test]
    fn contract_v1_json_defaults_multi_base_fields() {
        let json = r#"{
            "contract_version": 1,
            "base": {
                "table_fqn": "ice.ns.orders",
                "table_uuid": "11111111-1111-1111-1111-111111111111",
                "schema_id_at_create": 0,
                "schema_at_create": {
                    "fields": [
                        {
                            "field_id": 1,
                            "name_at_create": "id",
                            "type_signature": "long",
                            "required": true
                        }
                    ]
                }
            },
            "output": {
                "columns": [
                    {
                        "expression": {
                            "kind": "COLUMN",
                            "referenced_base_field_ids": [1]
                        }
                    }
                ],
                "filter": null
            },
            "target": {
                "table_fqn": "ice.mv.orders_mv",
                "table_uuid": "22222222-2222-2222-2222-222222222222",
                "schema_id_at_create": 0,
                "visible_columns": [
                    {
                        "output_name": "id",
                        "target_field_id": 1,
                        "type_signature": "long",
                        "nullable": false
                    }
                ],
                "hidden_apply_key": {
                    "column_name": "__nova_base_row_id",
                    "target_field_id": 2,
                    "source": "BASE_ROW_ID"
                }
            }
        }"#;
        let decoded: MvSchemaContract = serde_json::from_str(json).expect("deserialize v1");
        assert!(decoded.bases.is_empty());
        assert!(decoded.join.is_none());
        assert_eq!(decoded.base.alias_at_create, None);
        assert!(decoded.output.columns[0]
            .expression
            .referenced_base_fields
            .is_empty());
        decoded.ensure_self_consistent().expect("self check");
    }

    #[test]
    fn self_check_accepts_well_formed_contract() {
        assert!(sample_contract().ensure_self_consistent().is_ok());
    }

    #[test]
    fn self_check_rejects_mismatched_output_and_target_lengths() {
        let mut c = sample_contract();
        c.target.visible_columns.push(TargetVisibleColumn {
            output_name: "extra".to_string(),
            target_field_id: 99,
            type_signature: "long".to_string(),
            nullable: true,
        });
        match c.ensure_self_consistent() {
            Err(ContractSelfCheckError::OutputTargetLenMismatch {
                output_len: 1,
                target_len: 2,
            }) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn self_check_rejects_wrong_hidden_column_name() {
        let mut c = sample_contract();
        c.target.hidden_apply_key.column_name = "wrong".to_string();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong { .. })
        ));
    }

    #[test]
    fn self_check_rejects_unknown_referenced_field_id() {
        let mut c = sample_contract();
        c.output.columns[0].expression.referenced_base_field_ids = vec![999];
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId { field_id: 999, .. })
        ));
    }

    #[test]
    fn self_check_rejects_empty_base_uuid() {
        let mut c = sample_contract();
        c.base.table_uuid = String::new();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::EmptyBaseTableUuid)
        ));
    }

    #[test]
    fn self_check_rejects_filter_referencing_unknown_field_id() {
        let mut c = sample_contract();
        c.output.filter = Some(FilterLineage {
            referenced_base_field_ids: vec![999],
            referenced_base_fields: vec![],
        });
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::FilterReferencesUnknownBaseFieldId { field_id: 999 })
        ));
    }

    #[test]
    fn contract_v2_accepts_two_base_join_contract() {
        let contract = sample_join_contract();
        contract.ensure_self_consistent().expect("self check");
        assert_eq!(contract.contract_version, 2);
        assert_eq!(contract.bases.len(), 2);
        assert_eq!(
            contract.target.hidden_apply_key.source,
            ApplyKeySource::JoinRowKey
        );
    }

    #[test]
    fn contract_v2_rejects_output_reference_to_unknown_base() {
        let mut contract = sample_join_contract();
        contract.output.columns[0]
            .expression
            .referenced_base_fields
            .push(QualifiedFieldLineage {
                table_fqn: "ice.ns.missing".to_string(),
                qualifier_at_create: "m".to_string(),
                field_id: 99,
            });
        let err = contract.ensure_self_consistent().expect_err("unknown base");
        assert!(err.to_string().contains("unknown base field"), "err={err}");
    }

    #[test]
    fn contract_v2_rejects_output_reference_with_wrong_alias() {
        let mut contract = sample_join_contract();
        contract.output.columns[0].expression.referenced_base_fields[0].qualifier_at_create =
            "wrong".to_string();
        let err = contract.ensure_self_consistent().expect_err("wrong alias");
        assert!(err.to_string().contains("unknown base field"), "err={err}");
    }

    #[test]
    fn contract_v2_rejects_filter_reference_to_unknown_base() {
        let mut contract = sample_join_contract();
        contract.output.filter = Some(FilterLineage {
            referenced_base_field_ids: vec![],
            referenced_base_fields: vec![QualifiedFieldLineage {
                table_fqn: "ice.ns.missing".to_string(),
                qualifier_at_create: "m".to_string(),
                field_id: 99,
            }],
        });
        let err = contract.ensure_self_consistent().expect_err("unknown base");
        assert!(err.to_string().contains("unknown base field"), "err={err}");
    }

    #[test]
    fn contract_v2_rejects_join_reference_to_unknown_base() {
        let mut contract = sample_join_contract();
        contract.join.as_mut().expect("join").predicates[0]
            .right
            .table_fqn = "ice.ns.missing".to_string();
        let err = contract.ensure_self_consistent().expect_err("unknown base");
        assert!(err.to_string().contains("unknown base field"), "err={err}");
    }

    #[test]
    fn contract_v2_rejects_join_row_key_with_base_hidden_column() {
        let mut contract = sample_join_contract();
        contract.target.hidden_apply_key.column_name = HIDDEN_APPLY_KEY_COLUMN_NAME.to_string();
        assert!(matches!(
            contract.ensure_self_consistent(),
            Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong { .. })
        ));
    }

    fn sample_join_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 2,
            base: BaseContract {
                table_fqn: "ice.ns.left".to_string(),
                table_uuid: "left-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![
                BaseContract {
                    table_fqn: "ice.ns.left".to_string(),
                    table_uuid: "left-uuid".to_string(),
                    alias_at_create: Some("l".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "id".to_string(),
                            type_signature: "long".to_string(),
                            required: true,
                        }],
                    },
                },
                BaseContract {
                    table_fqn: "ice.ns.right".to_string(),
                    table_uuid: "right-uuid".to_string(),
                    alias_at_create: Some("r".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 2,
                            name_at_create: "id".to_string(),
                            type_signature: "long".to_string(),
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
                            table_fqn: "ice.ns.left".to_string(),
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
                        table_fqn: "ice.ns.left".to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: "ice.ns.right".to_string(),
                        qualifier_at_create: "r".to_string(),
                        field_id: 2,
                    },
                }],
            }),
            target: TargetContract {
                table_fqn: "ice.ns.mv".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::JoinRowKey,
                },
            },
        }
    }
}
