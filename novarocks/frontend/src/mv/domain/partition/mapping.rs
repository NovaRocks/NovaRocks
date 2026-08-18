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

#[cfg(test)]
use novarocks_sql::planning::mv::ApplyKeySource;

use crate::mv::domain::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::mv::domain::persistence::schema::{
    ExpressionKind, MvPartitionTransformContract, MvSchemaContract,
};

pub(crate) fn map_connector_partition_to_mv_key(
    contract: &MvSchemaContract,
    observation: &crate::mv::domain::storage_observation::MvSchemaValidationObservation,
    connector_partition: &novarocks_spi::connector::ConnectorChangePartition,
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };
    let base_contract = std::iter::once(&contract.base)
        .chain(contract.bases.iter())
        .find(|base| base.table_uuid == observation.table_uuid())
        .ok_or_else(|| {
            format!(
                "MV partition mapping has no stable base contract for observed table UUID {}",
                observation.table_uuid()
            )
        })?;

    let mut mapped_fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
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
        let stable_field_id = output_lineage.expression.referenced_base_field_ids[0];
        if !base_contract
            .schema_at_create
            .fields
            .iter()
            .any(|field| field.field_id == stable_field_id)
        {
            return Err(format!(
                "MV partition field {} references unknown stable base field {}",
                partition_field.partition_field_name, stable_field_id
            ));
        }
        let observed_field = observation
            .fields()
            .iter()
            .find(|field| field.field_id() == stable_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} cannot resolve stable base field {} in the exact schema observation",
                    partition_field.partition_field_name, stable_field_id
                )
            })?;
        let connector_field = connector_partition
            .fields()
            .iter()
            .find(|field| field.source_column().eq_ignore_ascii_case(observed_field.name()))
            .ok_or_else(|| {
                format!(
                    "MV partition field {} has no connector partition fact for exact source column {}",
                    partition_field.partition_field_name,
                    observed_field.name()
                )
            })?;
        if !connector_transform_matches_contract(
            connector_field.transform(),
            &partition_field.transform,
        ) {
            return Err(format!(
                "MV partition field {} connector transform does not match its persisted contract",
                partition_field.partition_field_name
            ));
        }
        let value = match connector_field.value() {
            novarocks_spi::connector::ConnectorChangePartitionValue::Null => MvPartitionValue::Null,
            novarocks_spi::connector::ConnectorChangePartitionValue::String(value) => {
                MvPartitionValue::String(value.to_string())
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

fn connector_transform_matches_contract(
    connector: novarocks_spi::connector::ConnectorChangePartitionTransform,
    contract: &MvPartitionTransformContract,
) -> bool {
    use novarocks_spi::connector::ConnectorChangePartitionTransform as Connector;

    match (connector, contract) {
        (Connector::Identity, MvPartitionTransformContract::Identity)
        | (Connector::Year, MvPartitionTransformContract::Year)
        | (Connector::Month, MvPartitionTransformContract::Month)
        | (Connector::Day, MvPartitionTransformContract::Day)
        | (Connector::Hour, MvPartitionTransformContract::Hour) => true,
        (Connector::Bucket { buckets }, MvPartitionTransformContract::Bucket { num_buckets }) => {
            buckets.get() == *num_buckets
        }
        (
            Connector::Truncate { width },
            MvPartitionTransformContract::Truncate { width: expected },
        ) => width.get() == *expected,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
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

    /// Neutral observation fixture: the mapper matches the contract's base by
    /// observed table UUID, and reads the source column names from its fields.
    fn observation() -> crate::mv::domain::storage_observation::MvSchemaValidationObservation {
        crate::mv::domain::storage_observation::MvSchemaValidationObservation::try_new_with_maximum_payload(
            "base-uuid".to_string(),
            0,
            true,
            true,
            vec![crate::mv::domain::storage_observation::MvObservedTargetField {
                field_id: 1,
                name: "id".to_string(),
                type_signature: "int".to_string(),
                nullable: false,
            }],
            crate::mv::domain::storage_observation::MvSchemaValidationPartitionContract::new(7, Vec::new()),
        )
        .expect("observation fixture")
    }

    fn connector_partition(
        transform: novarocks_spi::connector::ConnectorChangePartitionTransform,
        value: novarocks_spi::connector::ConnectorChangePartitionValue,
    ) -> novarocks_spi::connector::ConnectorChangePartition {
        novarocks_spi::connector::ConnectorChangePartition::try_new(vec![
            novarocks_spi::connector::ConnectorChangePartitionField::try_new(
                "id", transform, value,
            )
            .expect("partition field fixture"),
        ])
        .expect("partition fixture")
    }

    fn map(
        contract: &MvSchemaContract,
        partition: &novarocks_spi::connector::ConnectorChangePartition,
    ) -> Result<Option<MvPartitionKey>, String> {
        map_connector_partition_to_mv_key(contract, &observation(), partition)
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
            branch: None,
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

    use novarocks_spi::connector::{
        ConnectorChangePartitionTransform as CT, ConnectorChangePartitionValue as CV,
    };
    use std::num::NonZeroU32;

    fn key(value: &str) -> Option<MvPartitionKey> {
        Some(MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "id".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        ))
    }

    #[test]
    fn maps_identity_partition_value_to_mv_key() {
        let contract = contract_with_identity_partition();
        let partition = connector_partition(CT::Identity, CV::String("42".into()));

        assert_eq!(map(&contract, &partition).unwrap(), key("42"));
    }

    #[test]
    fn maps_year_transform_to_mv_key() {
        let contract = contract_with_partition(MvPartitionTransformContract::Year);
        let partition = connector_partition(CT::Year, CV::String("2026".into()));

        assert_eq!(map(&contract, &partition).unwrap(), key("2026"));
    }

    #[test]
    fn maps_month_day_hour_transforms() {
        for (contracted, connector, value) in [
            (MvPartitionTransformContract::Month, CT::Month, "2026-08"),
            (MvPartitionTransformContract::Day, CT::Day, "2026-08-12"),
            (
                MvPartitionTransformContract::Hour,
                CT::Hour,
                "2026-08-12-07",
            ),
        ] {
            let contract = contract_with_partition(contracted);
            let partition = connector_partition(connector, CV::String(value.into()));

            assert_eq!(map(&contract, &partition).unwrap(), key(value));
        }
    }

    #[test]
    fn maps_bucket_transform_with_matching_arity() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let partition = connector_partition(
            CT::Bucket {
                buckets: NonZeroU32::new(8).expect("nonzero"),
            },
            CV::String("3".into()),
        );

        assert_eq!(map(&contract, &partition).unwrap(), key("3"));
    }

    #[test]
    fn rejects_bucket_transform_arity_mismatch() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let partition = connector_partition(
            CT::Bucket {
                buckets: NonZeroU32::new(16).expect("nonzero"),
            },
            CV::String("3".into()),
        );

        let err = map(&contract, &partition).unwrap_err();
        assert!(err.contains("transform"), "err={err}");
    }

    #[test]
    fn maps_truncate_transform_with_matching_width() {
        let contract = contract_with_partition(MvPartitionTransformContract::Truncate { width: 4 });
        let partition = connector_partition(
            CT::Truncate {
                width: NonZeroU32::new(4).expect("nonzero"),
            },
            CV::String("abcd".into()),
        );

        assert_eq!(map(&contract, &partition).unwrap(), key("abcd"));
    }

    #[test]
    fn rejects_truncate_transform_width_mismatch() {
        let contract = contract_with_partition(MvPartitionTransformContract::Truncate { width: 4 });
        let partition = connector_partition(
            CT::Truncate {
                width: NonZeroU32::new(16).expect("nonzero"),
            },
            CV::String("abcd".into()),
        );

        let err = map(&contract, &partition).unwrap_err();
        assert!(err.contains("transform"), "err={err}");
    }

    #[test]
    fn null_partition_value_renders_as_mv_null() {
        let contract = contract_with_identity_partition();
        let partition = connector_partition(CT::Identity, CV::Null);

        assert_eq!(
            map(&contract, &partition).unwrap(),
            Some(MvPartitionKey::new(
                7,
                vec![MvPartitionKeyField::new(
                    "id".to_string(),
                    MvPartitionValue::Null,
                )],
            ))
        );
    }

    #[test]
    fn returns_none_for_unpartitioned_contract() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;
        let partition = connector_partition(CT::Identity, CV::String("42".into()));

        assert_eq!(map(&contract, &partition).unwrap(), None);
    }

    // `rejects_void_transform` and `unsupported_partition_value_requires_unknown_mapping`
    // are not ported: ConnectorChangePartitionTransform has no Void variant and
    // ConnectorChangePartitionValue has no Unsupported variant, so both states
    // are unrepresentable rather than rejected at runtime.
}
