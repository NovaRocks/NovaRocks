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
        let expected_transform_text = contract_transform_manifest_text(&partition_field.transform)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported transform {}",
                    partition_field.partition_field_name,
                    partition_transform_name(&partition_field.transform)
                )
            })?;

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

        let mut matched_by_id_count = 0;
        let mut transform_mismatch: Option<&str> = None;
        let file_partition_value = file_partition_values
            .iter()
            .find(|value| {
                if value.source_field_id != base_field_id {
                    return false;
                }
                matched_by_id_count += 1;
                if value.transform.eq_ignore_ascii_case(&expected_transform_text) {
                    true
                } else {
                    transform_mismatch = Some(value.transform.as_str());
                    false
                }
            })
            .ok_or_else(|| {
                if matched_by_id_count == 0 {
                    format!(
                        "MV partition field {} cannot be proven from Iceberg file partition metadata for file spec {}",
                        partition_field.partition_field_name, file_spec_id
                    )
                } else {
                    format!(
                        "MV partition field {} file metadata transform {} mismatches contract transform {}",
                        partition_field.partition_field_name,
                        transform_mismatch.unwrap_or("<unknown>"),
                        expected_transform_text
                    )
                }
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

fn contract_transform_manifest_text(transform: &MvPartitionTransformContract) -> Option<String> {
    match transform {
        MvPartitionTransformContract::Identity => Some("identity".to_string()),
        MvPartitionTransformContract::Year => Some("year".to_string()),
        MvPartitionTransformContract::Month => Some("month".to_string()),
        MvPartitionTransformContract::Day => Some("day".to_string()),
        MvPartitionTransformContract::Hour => Some("hour".to_string()),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        MvPartitionTransformContract::Truncate { width } => Some(format!("truncate({width})")),
        MvPartitionTransformContract::Void => None,
    }
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
        MvPartitionTransformContract::Void => "Void".to_string(),
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

    fn contract_with_partition(transform: MvPartitionTransformContract) -> MvSchemaContract {
        let mut contract = contract_with_identity_partition();
        let partition = contract
            .target
            .partition
            .as_mut()
            .expect("identity helper always builds a partition");
        partition.fields[0].transform = transform;
        contract
    }

    fn partition_value(
        transform_text: &str,
        value: ChangePartitionValue,
    ) -> ChangePartitionFieldValue {
        ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: transform_text.to_string(),
            value,
        }
    }

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

    #[test]
    fn maps_year_transform_to_mv_key() {
        let contract = contract_with_partition(MvPartitionTransformContract::Year);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "year",
                ChangePartitionValue::Primitive("55".to_string()),
            )],
        )
        .unwrap();

        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("55".to_string())
        );
    }

    #[test]
    fn maps_month_day_hour_transforms() {
        for (contract_transform, manifest_text, value) in [
            (MvPartitionTransformContract::Month, "month", "660"),
            (MvPartitionTransformContract::Day, "day", "20000"),
            (MvPartitionTransformContract::Hour, "hour", "480000"),
        ] {
            let contract = contract_with_partition(contract_transform.clone());
            let mapped = map_file_partition_to_mv_key(
                &contract,
                7,
                &[partition_value(
                    manifest_text,
                    ChangePartitionValue::Primitive(value.to_string()),
                )],
            )
            .unwrap();
            assert_eq!(
                mapped.unwrap().fields[0].value,
                MvPartitionValue::String(value.to_string()),
                "transform {contract_transform:?} did not round-trip"
            );
        }
    }

    #[test]
    fn maps_bucket_transform_with_matching_arity() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(8)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("3".to_string())
        );
    }

    #[test]
    fn rejects_bucket_transform_arity_mismatch() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(16)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap_err();
        assert!(err.contains("file metadata transform"), "{err}");
        assert!(err.contains("bucket(16)"), "{err}");
        assert!(err.contains("bucket(8)"), "{err}");
    }

    #[test]
    fn maps_truncate_transform_with_matching_width() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Truncate { width: 16 });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "truncate(16)",
                ChangePartitionValue::Primitive("ho".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("ho".to_string())
        );
    }

    #[test]
    fn rejects_void_transform() {
        let contract = contract_with_partition(MvPartitionTransformContract::Void);
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("void", ChangePartitionValue::Null)],
        )
        .unwrap_err();
        assert!(err.contains("Void"), "{err}");
    }

    #[test]
    fn null_partition_value_renders_as_mv_null() {
        let contract = contract_with_partition(MvPartitionTransformContract::Day);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("day", ChangePartitionValue::Null)],
        )
        .unwrap();
        assert_eq!(mapped.unwrap().fields[0].value, MvPartitionValue::Null);
    }

    #[test]
    fn change_partition_field_values_is_reachable_for_mv_partition_module() {
        use crate::connector::iceberg::changes::change_partition_field_values;
        // We do not need to drive Iceberg metadata in a unit test — just make
        // sure the symbol is visible at the call site. If this fn ever becomes
        // private again, this test will fail to compile.
        let _fn_ptr: fn(
            &iceberg::spec::TableMetadata,
            i32,
            &iceberg::spec::Struct,
        ) -> Result<
            Vec<crate::connector::iceberg::changes::ChangePartitionFieldValue>,
            crate::connector::iceberg::changes::ChangeError,
        > = change_partition_field_values;
    }
}
