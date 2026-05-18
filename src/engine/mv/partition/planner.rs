use crate::connector::iceberg::changes::IcebergChangeBatch;
use crate::engine::mv::partition::mapping::map_file_partition_to_mv_key;
use crate::engine::mv::partition::{AffectedMvPartitions, MvPartitionKey};
use crate::meta::repository::mv_contract::MvSchemaContract;

pub(crate) struct AffectedPartitionPlanInput<'a> {
    pub schema_contract: &'a MvSchemaContract,
    pub change_batch: Option<&'a IcebergChangeBatch>,
}

pub(crate) fn plan_affected_partitions(
    input: &AffectedPartitionPlanInput<'_>,
) -> AffectedMvPartitions {
    if input.schema_contract.target.partition.is_none() {
        return AffectedMvPartitions::Unpartitioned;
    }
    let Some(batch) = input.change_batch else {
        return AffectedMvPartitions::unknown(
            "full refresh affected partition planning is not implemented",
        );
    };
    if !batch.deletes.is_empty() || !batch.equality_deletes.is_empty() {
        return AffectedMvPartitions::unknown(
            "row-level delete affected partitions require row-evaluation fallback",
        );
    }

    let mut new_partitions = Vec::<MvPartitionKey>::new();
    for file in &batch.inserts {
        let Some(spec_id) = file.partition_spec_id else {
            return AffectedMvPartitions::unknown(format!(
                "inserted data file {} is missing partition spec id",
                file.path
            ));
        };
        match map_file_partition_to_mv_key(input.schema_contract, spec_id, &file.partition_values) {
            Ok(Some(key)) => new_partitions.push(key),
            Ok(None) => return AffectedMvPartitions::Unpartitioned,
            Err(reason) => return AffectedMvPartitions::unknown(reason),
        }
    }

    let mut old_partitions = Vec::<MvPartitionKey>::new();
    for file in &batch.deleted_data_files {
        let Some(spec_id) = file.partition_spec_id else {
            return AffectedMvPartitions::unknown(format!(
                "deleted data file {} is missing partition spec id",
                file.path
            ));
        };
        match map_file_partition_to_mv_key(input.schema_contract, spec_id, &file.partition_values) {
            Ok(Some(key)) => old_partitions.push(key),
            Ok(None) => return AffectedMvPartitions::Unpartitioned,
            Err(reason) => return AffectedMvPartitions::unknown(reason),
        }
    }

    AffectedMvPartitions::known(new_partitions, old_partitions)
}

#[cfg(test)]
mod tests {
    use super::{AffectedPartitionPlanInput, plan_affected_partitions};
    use crate::connector::iceberg::changes::{
        ChangePartitionFieldValue, ChangePartitionValue, DataFileRef, DeletedDataFileRef,
        IcebergChangeBatch, PositionDeleteRef,
    };
    use crate::engine::mv::partition::{
        AffectedMvPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    };
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

    fn change_batch() -> IcebergChangeBatch {
        IcebergChangeBatch {
            previous_snapshot_id: 1,
            current_snapshot_id: 2,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        }
    }

    fn partition_value(value: &str) -> ChangePartitionFieldValue {
        ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: ChangePartitionValue::Primitive(value.to_string()),
        }
    }

    fn data_file(path: &str, partition_spec_id: Option<i32>, value: &str) -> DataFileRef {
        DataFileRef {
            path: path.to_string(),
            size: 128,
            record_count: Some(1),
            partition_spec_id,
            partition_key: Some(format!("id={value}")),
            partition_values: vec![partition_value(value)],
            first_row_id: None,
            data_sequence_number: None,
        }
    }

    fn deleted_data_file(
        path: &str,
        partition_spec_id: Option<i32>,
        value: &str,
    ) -> DeletedDataFileRef {
        DeletedDataFileRef {
            path: path.to_string(),
            size: 128,
            record_count: Some(1),
            partition_spec_id,
            partition_key: Some(format!("id={value}")),
            partition_values: vec![partition_value(value)],
            first_row_id: None,
            data_sequence_number: None,
        }
    }

    fn mv_key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "id".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn append_only_insert_returns_new_partitions() {
        let contract = contract_with_identity_partition();
        let mut batch = change_batch();
        batch
            .inserts
            .push(data_file("s3://bucket/new.parquet", Some(7), "42"));

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            change_batch: Some(&batch),
        });

        let AffectedMvPartitions::Known {
            new_partitions,
            old_partitions,
        } = result
        else {
            panic!("expected known affected partitions");
        };
        assert_eq!(
            new_partitions.into_iter().collect::<Vec<_>>(),
            vec![mv_key("42")]
        );
        assert!(old_partitions.is_empty());
    }

    #[test]
    fn overwrite_diff_returns_new_and_old_partitions() {
        let contract = contract_with_identity_partition();
        let mut batch = change_batch();
        batch
            .inserts
            .push(data_file("s3://bucket/new.parquet", Some(7), "42"));
        batch
            .deleted_data_files
            .push(deleted_data_file("s3://bucket/old.parquet", Some(7), "24"));

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            change_batch: Some(&batch),
        });

        let AffectedMvPartitions::Known {
            new_partitions,
            old_partitions,
        } = result
        else {
            panic!("expected known affected partitions");
        };
        assert_eq!(
            new_partitions.into_iter().collect::<Vec<_>>(),
            vec![mv_key("42")]
        );
        assert_eq!(
            old_partitions.into_iter().collect::<Vec<_>>(),
            vec![mv_key("24")]
        );
    }

    #[test]
    fn position_delete_returns_unknown() {
        let contract = contract_with_identity_partition();
        let mut batch = change_batch();
        batch.deletes.push(PositionDeleteRef {
            delete_file_path: "s3://bucket/delete.parquet".to_string(),
            delete_file_size: 128,
            record_count: Some(1),
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
            partition_values: Vec::new(),
        });

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            change_batch: Some(&batch),
        });

        assert_eq!(
            result.unknown_reason(),
            Some("row-level delete affected partitions require row-evaluation fallback")
        );
    }

    #[test]
    fn missing_insert_partition_spec_id_returns_unknown() {
        let contract = contract_with_identity_partition();
        let mut batch = change_batch();
        batch
            .inserts
            .push(data_file("s3://bucket/new.parquet", None, "42"));

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            change_batch: Some(&batch),
        });

        assert!(
            result
                .unknown_reason()
                .is_some_and(|reason| reason.contains("missing partition spec id"))
        );
    }

    #[test]
    fn unpartitioned_contract_returns_unpartitioned() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;
        let batch = change_batch();

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            change_batch: Some(&batch),
        });

        assert_eq!(result, AffectedMvPartitions::Unpartitioned);
    }
}
