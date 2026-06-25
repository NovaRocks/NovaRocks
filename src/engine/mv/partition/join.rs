use crate::engine::mv::partition::{
    AffectedPartitionError, AffectedTargetPartitions, BoundPartitionField,
};
use crate::exec::chunk::Chunk;
use crate::meta::repository::mv_contract::MvSchemaContract;

#[derive(Clone, Debug)]
pub(crate) struct BoundJoinTargetPartitionDerivation {
    pub(crate) target_spec_id: i32,
    pub(crate) bound_fields: Vec<BoundPartitionField>,
}

pub(crate) fn target_visible_partition_derivation(
    contract: &MvSchemaContract,
) -> Result<Option<BoundJoinTargetPartitionDerivation>, String> {
    let Some(join) = contract.join.as_ref() else {
        return Ok(None);
    };
    if !matches!(
        join.kind,
        crate::meta::repository::mv_contract::JoinContractKind::InnerEquiJoin
    ) {
        return Err(
            "join MV affected partition planning requires inner equi-join contract".to_string(),
        );
    }

    let Some(spec) = crate::engine::mv::partition::resolve_partition_derivation_spec(contract)
        .map_err(join_partition_error)?
    else {
        return Ok(None);
    };
    let bound = crate::engine::mv::partition::bind_spec_to_target_visible_columns(&spec, contract)
        .map_err(join_partition_error)?;
    Ok(Some(BoundJoinTargetPartitionDerivation {
        target_spec_id: spec.target_spec_id,
        bound_fields: bound,
    }))
}

pub(crate) fn derive_join_target_partitions_from_delta_chunks(
    contract: &MvSchemaContract,
    delta_chunks: &[Chunk],
) -> AffectedTargetPartitions {
    let derivation = match target_visible_partition_derivation(contract) {
        Ok(Some(derivation)) => derivation,
        Ok(None) => return AffectedTargetPartitions::Unpartitioned,
        Err(reason) => return AffectedTargetPartitions::not_derived(reason),
    };

    match crate::engine::mv::partition::evaluate_partition_spec(
        derivation.target_spec_id,
        &derivation.bound_fields,
        delta_chunks,
    ) {
        Ok(partitions) => AffectedTargetPartitions::known_row_derived(partitions),
        Err(err) => AffectedTargetPartitions::not_derived(join_partition_error(err)),
    }
}

fn join_partition_error(err: AffectedPartitionError) -> String {
    match err {
        AffectedPartitionError::OutputLineageNotPureColumn { field } => {
            format!("join MV target partition field {field} is not a pure column lineage")
        }
        AffectedPartitionError::TransformUnsupported { field, transform } => {
            format!("join MV target partition field {field} uses unsupported transform {transform}")
        }
        AffectedPartitionError::GroupKeyColumnMissing { field, reason } => {
            format!("join MV target partition field {field}: {reason}")
        }
        AffectedPartitionError::GroupKeyTypeMismatch { field, want, got } => {
            format!(
                "join MV target partition field {field} delta column type mismatch: want {want}, got {got}"
            )
        }
        AffectedPartitionError::TransformFailed { field, source } => {
            format!("join MV target partition field {field} transform failed: {source}")
        }
        AffectedPartitionError::ContractMissing(reason) => {
            format!("join MV target partition contract missing or inconsistent: {reason}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, JoinContract, JoinContractKind,
        JoinPredicateLineage, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, OutputColumnLineage, OutputContract, QualifiedFieldLineage,
        TargetContract, TargetVisibleColumn,
    };

    fn join_contract_with_partition(kind: ExpressionKind) -> MvSchemaContract {
        let left = "ice.sales.fact".to_string();
        let right = "ice.sales.dim".to_string();
        MvSchemaContract {
            contract_version: 2,
            base: BaseContract {
                table_fqn: left.clone(),
                table_uuid: "left-uuid".to_string(),
                alias_at_create: Some("f".to_string()),
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
            bases: vec![
                BaseContract {
                    table_fqn: left.clone(),
                    table_uuid: "left-uuid".to_string(),
                    alias_at_create: Some("f".to_string()),
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
                    table_fqn: right.clone(),
                    table_uuid: "right-uuid".to_string(),
                    alias_at_create: Some("d".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![
                            BaseFieldRecord {
                                field_id: 2,
                                name_at_create: "id".to_string(),
                                type_signature: "long".to_string(),
                                required: true,
                            },
                            BaseFieldRecord {
                                field_id: 3,
                                name_at_create: "region".to_string(),
                                type_signature: "string".to_string(),
                                required: false,
                            },
                        ],
                    },
                },
            ],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind,
                        referenced_base_field_ids: vec![],
                        referenced_base_fields: vec![QualifiedFieldLineage {
                            table_fqn: right.clone(),
                            qualifier_at_create: "d".to_string(),
                            field_id: 3,
                        }],
                    },
                }],
                filter: None,
            },
            join: Some(JoinContract {
                kind: JoinContractKind::InnerEquiJoin,
                predicates: vec![JoinPredicateLineage {
                    left: QualifiedFieldLineage {
                        table_fqn: left,
                        qualifier_at_create: "f".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: right,
                        qualifier_at_create: "d".to_string(),
                        field_id: 2,
                    },
                }],
            }),
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_fact_dim".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "region".to_string(),
                    target_field_id: 10,
                    type_signature: "string".to_string(),
                    nullable: true,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_join_row_key".to_string(),
                    target_field_id: 11,
                    source: ApplyKeySource::JoinRowKey,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: "region".to_string(),
                        source_target_field_id: 10,
                        source_column_name: "region".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    fn delta_chunk(regions: &[&str]) -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(regions.to_vec()))],
        )
        .expect("batch");
        crate::engine::record_batch_to_chunk(batch).expect("chunk")
    }

    fn key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn join_partition_derivation_pure_column_inner_join_is_row_derived() {
        let contract = join_contract_with_partition(ExpressionKind::Column);
        let result = derive_join_target_partitions_from_delta_chunks(
            &contract,
            &[delta_chunk(&["west", "east", "west"])],
        );

        assert_eq!(
            result,
            AffectedTargetPartitions::known_row_derived([key("east"), key("west")])
        );
    }

    #[test]
    fn join_partition_derivation_allows_non_changed_side_output_rows() {
        let contract = join_contract_with_partition(ExpressionKind::Column);
        let result =
            derive_join_target_partitions_from_delta_chunks(&contract, &[delta_chunk(&["north"])]);

        assert_eq!(
            result,
            AffectedTargetPartitions::known_row_derived([key("north")])
        );
    }

    #[test]
    fn join_partition_derivation_reports_non_pure_lineage_field() {
        let contract = join_contract_with_partition(ExpressionKind::Func);
        let result = derive_join_target_partitions_from_delta_chunks(&contract, &[]);

        assert_eq!(
            result.not_derived_reason(),
            Some("join MV target partition field region is not a pure column lineage")
        );
    }
}
