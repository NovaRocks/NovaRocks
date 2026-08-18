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

use crate::mv::domain::model::{AffectedTargetPartitions, MvPartitionKey};
use crate::mv::domain::partition::mapping::map_connector_partition_to_mv_key;
use crate::mv::domain::persistence::schema::MvSchemaContract;

pub(crate) struct AffectedPartitionPlanInput<'a> {
    pub schema_contract: &'a MvSchemaContract,
    pub partition_impact:
        Option<&'a novarocks_spi::connector::ConnectorChangeWindowPartitionImpact>,
    pub schema_observation:
        Option<&'a crate::mv::domain::storage_observation::MvSchemaValidationObservation>,
}

pub(crate) fn plan_affected_partitions(
    input: &AffectedPartitionPlanInput<'_>,
) -> AffectedTargetPartitions {
    if input.schema_contract.target.partition.is_none() {
        return AffectedTargetPartitions::Unpartitioned;
    }
    let Some(impact) = input.partition_impact else {
        return AffectedTargetPartitions::not_derived(
            "full refresh affected partition planning is not implemented",
        );
    };
    match impact {
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Unavailable => {
            AffectedTargetPartitions::not_derived(
                "connector change-window partition impact is unavailable",
            )
        }
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Unpartitioned => {
            AffectedTargetPartitions::Unpartitioned
        }
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Exact {
            has_row_deletes,
            added,
            removed,
        } => {
            if *has_row_deletes {
                return AffectedTargetPartitions::not_derived(
                    "row-level delete affected partitions require row-evaluation fallback",
                );
            }
            let Some(observation) = input.schema_observation else {
                return AffectedTargetPartitions::not_derived(
                    "connector partition impact is missing its exact schema observation",
                );
            };
            let mut partitions = Vec::<MvPartitionKey>::with_capacity(added.len() + removed.len());
            for partition in added.iter().chain(removed) {
                match map_connector_partition_to_mv_key(
                    input.schema_contract,
                    observation,
                    partition,
                ) {
                    Ok(Some(key)) => partitions.push(key),
                    Ok(None) => return AffectedTargetPartitions::Unpartitioned,
                    Err(reason) => return AffectedTargetPartitions::not_derived(reason),
                }
            }
            AffectedTargetPartitions::known(partitions)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AffectedPartitionPlanInput, plan_affected_partitions};
    use crate::mv::domain::model::{
        AffectedTargetPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    };
    use crate::mv::domain::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };
    use crate::mv::domain::storage_observation::{
        MvObservedTargetField, MvSchemaValidationObservation, MvSchemaValidationPartitionContract,
        MvSchemaValidationPartitionField, MvSchemaValidationPartitionTransform,
    };
    use novarocks_spi::connector::{
        ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
        ConnectorChangePartitionValue, ConnectorChangeWindowPartitionImpact,
    };
    use novarocks_sql::planning::mv::ApplyKeySource;

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

    fn observation() -> MvSchemaValidationObservation {
        MvSchemaValidationObservation::try_new_with_maximum_payload(
            "base-uuid".to_string(),
            0,
            true,
            true,
            vec![MvObservedTargetField::new(
                1,
                "id".to_string(),
                "int".to_string(),
                false,
            )],
            MvSchemaValidationPartitionContract::new(
                7,
                vec![MvSchemaValidationPartitionField::new(
                    100,
                    "id".to_string(),
                    1,
                    "id".to_string(),
                    MvSchemaValidationPartitionTransform::Identity,
                )],
            ),
        )
        .expect("schema observation")
    }

    fn partition(value: &str) -> ConnectorChangePartition {
        ConnectorChangePartition::try_new(vec![
            ConnectorChangePartitionField::try_new(
                "id",
                ConnectorChangePartitionTransform::Identity,
                ConnectorChangePartitionValue::String(value.into()),
            )
            .expect("partition field"),
        ])
        .expect("partition")
    }

    fn exact_impact(
        has_row_deletes: bool,
        added: Vec<ConnectorChangePartition>,
        removed: Vec<ConnectorChangePartition>,
    ) -> ConnectorChangeWindowPartitionImpact {
        ConnectorChangeWindowPartitionImpact::try_exact(
            has_row_deletes,
            added,
            removed,
            &novarocks::connector::test_request_context(),
        )
        .expect("partition impact")
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
        let impact = exact_impact(false, vec![partition("42")], Vec::new());
        let observation = observation();

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            partition_impact: Some(&impact),
            schema_observation: Some(&observation),
        });

        let AffectedTargetPartitions::Known { partitions } = result else {
            panic!("expected known affected partitions");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![mv_key("42")]
        );
    }

    #[test]
    fn overwrite_diff_returns_merged_partitions() {
        let contract = contract_with_identity_partition();
        let impact = exact_impact(false, vec![partition("42")], vec![partition("24")]);
        let observation = observation();

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            partition_impact: Some(&impact),
            schema_observation: Some(&observation),
        });

        let AffectedTargetPartitions::Known { partitions } = result else {
            panic!("expected known affected partitions");
        };
        let partitions: Vec<_> = partitions.into_iter().collect();
        assert!(partitions.contains(&mv_key("42")));
        assert!(partitions.contains(&mv_key("24")));
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn position_delete_returns_unknown() {
        let contract = contract_with_identity_partition();
        let impact = exact_impact(true, Vec::new(), Vec::new());
        let observation = observation();

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            partition_impact: Some(&impact),
            schema_observation: Some(&observation),
        });

        assert_eq!(
            result.not_derived_reason(),
            Some("row-level delete affected partitions require row-evaluation fallback")
        );
    }

    #[test]
    fn unavailable_connector_evidence_returns_unknown() {
        let contract = contract_with_identity_partition();
        let impact = ConnectorChangeWindowPartitionImpact::Unavailable;

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            partition_impact: Some(&impact),
            schema_observation: None,
        });

        assert_eq!(
            result.not_derived_reason(),
            Some("connector change-window partition impact is unavailable")
        );
    }

    #[test]
    fn unpartitioned_contract_returns_unpartitioned() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;
        let impact = ConnectorChangeWindowPartitionImpact::Unpartitioned;

        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            schema_contract: &contract,
            partition_impact: Some(&impact),
            schema_observation: None,
        });

        assert_eq!(result, AffectedTargetPartitions::Unpartitioned);
    }
}
