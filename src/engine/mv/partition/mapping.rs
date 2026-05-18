use crate::connector::iceberg::changes::{ChangePartitionFieldValue, ChangePartitionValue};
use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::meta::repository::mv_contract::{
    ExpressionKind, MvPartitionTransformContract, MvSchemaContract,
};

pub(crate) fn map_file_partition_to_mv_key(
    contract: &MvSchemaContract,
    file_spec_id: i32,
    file_partition_values: &[ChangePartitionFieldValue],
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };

    let mut mapped_fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        if !matches!(
            partition_field.transform,
            MvPartitionTransformContract::Identity
        ) {
            return Err(format!(
                "MV partition field {} uses unsupported transform {}",
                partition_field.partition_field_name,
                partition_transform_name(&partition_field.transform)
            ));
        }

        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing target field {}",
                    partition_field.partition_field_name, partition_field.source_target_field_id
                )
            })?;
        let output_lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            )
        })?;

        if output_lineage.expression.kind != ExpressionKind::Column
            || output_lineage.expression.referenced_base_field_ids.len() != 1
        {
            return Err(format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            ));
        }

        let base_field_id = output_lineage.expression.referenced_base_field_ids[0];
        let file_partition_value = file_partition_values
            .iter()
            .find(|value| value.source_field_id == base_field_id && value.transform == "identity")
            .ok_or_else(|| {
                format!(
                    "MV partition field {} cannot be proven from Iceberg file partition metadata for file spec {}",
                    partition_field.partition_field_name, file_spec_id
                )
            })?;

        let value = match &file_partition_value.value {
            ChangePartitionValue::Primitive(value) => MvPartitionValue::String(value.clone()),
            ChangePartitionValue::Null => MvPartitionValue::Null,
            ChangePartitionValue::Unsupported(reason) => {
                return Err(format!(
                    "MV partition field {} has unsupported partition value: {}",
                    partition_field.partition_field_name, reason
                ));
            }
        };
        mapped_fields.push(MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            value,
        ));
    }

    Ok(Some(MvPartitionKey::new(
        partition.target_spec_id,
        mapped_fields,
    )))
}

fn partition_transform_name(transform: &MvPartitionTransformContract) -> String {
    match transform {
        MvPartitionTransformContract::Identity => "identity".to_string(),
        MvPartitionTransformContract::Year => "year".to_string(),
        MvPartitionTransformContract::Month => "month".to_string(),
        MvPartitionTransformContract::Day => "day".to_string(),
        MvPartitionTransformContract::Hour => "hour".to_string(),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            format!("bucket({num_buckets})")
        }
        MvPartitionTransformContract::Truncate { width } => format!("truncate({width})"),
        MvPartitionTransformContract::Void => "void".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

    fn contract_with_identity_partition() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: Vec::new(),
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 10,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 11,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: "id".to_string(),
                        source_target_field_id: 10,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    #[test]
    fn maps_identity_partition_value_to_mv_key() {
        let contract = contract_with_identity_partition();
        let file_partition_values = vec![ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: None,
            field_name: "renamed_id".to_string(),
            transform: "identity".to_string(),
            value: ChangePartitionValue::Primitive("42".to_string()),
        }];

        let mapped = map_file_partition_to_mv_key(&contract, 5, &file_partition_values).unwrap();

        assert_eq!(
            mapped,
            Some(MvPartitionKey::new(
                7,
                vec![MvPartitionKeyField::new(
                    "id".to_string(),
                    MvPartitionValue::String("42".to_string()),
                )],
            ))
        );
    }

    #[test]
    fn returns_none_for_unpartitioned_contract() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;

        let mapped = map_file_partition_to_mv_key(&contract, 5, &[]).unwrap();

        assert_eq!(mapped, None);
    }

    #[test]
    fn unsupported_partition_value_requires_unknown_mapping() {
        let contract = contract_with_identity_partition();
        let file_partition_values = vec![ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: ChangePartitionValue::Unsupported("binary partition value".to_string()),
        }];

        let err = map_file_partition_to_mv_key(&contract, 5, &file_partition_values).unwrap_err();

        assert!(err.contains("unsupported partition value"));
        assert!(err.contains("binary partition value"));
    }
}
