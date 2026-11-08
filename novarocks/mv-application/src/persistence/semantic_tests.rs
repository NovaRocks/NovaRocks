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

use super::*;
use novarocks_query_application::persisted_query_definition::PersistedQueryDialect;
use novarocks_sql::planning::mv::ApplyKeySource;

fn sample_schema_contract() -> MvSchemaContract {
    use crate::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, OutputColumnLineage, OutputContract, TargetContract,
        TargetVisibleColumn,
    };
    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorTableObjectId;

    MvSchemaContract {
        contract_version: 1,
        base: BaseContract {
            table_fqn: "ice.sales.orders".to_string(),
            table_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[0, 0xff, b'u']))
                .expect("valid opaque table object ID"),
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
        aggregate: None,
        branch: None,
        target: TargetContract {
            table_fqn: "ice.sales.mv_orders".to_string(),
            table_uuid: "target-uuid".to_string(),
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
            partition: None,
        },
    }
}

fn sample_semantics() -> MvDesiredSemantics {
    MvDesiredSemantics::new(
        "analytics.mv_orders".into(),
        PersistedQueryDefinition::new(
            "SELECT id FROM ice.sales.orders",
            PersistedQueryDialect::StarRocks,
            "ice",
            "sales",
        )
        .unwrap(),
        vec!["id".into()],
        vec!["__nova_base_row_id".into()],
        vec![DescriptorDependency {
            catalog: "ice".into(),
            namespace: "sales".into(),
            name: "orders".into(),
            object_type: "table".into(),
            storage_engine: "iceberg".into(),
        }],
        vec!["id".into()],
        sample_schema_contract(),
        MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            true,
            Some(60_000),
            Some(300_000),
        )
        .unwrap(),
        123,
    )
    .unwrap()
}

fn rebuild(value: MvDesiredSemantics) -> Result<MvDesiredSemantics, String> {
    MvDesiredSemantics::new(
        value.package_id,
        value.query_definition,
        value.visible_columns,
        value.hidden_columns,
        value.base_dependencies,
        value.primary_key_columns,
        value.schema_contract,
        value.refresh,
        value.created_at_ms,
    )
}

#[test]
fn desired_semantics_retains_the_explicit_query_schema_and_refresh_facts() {
    let expected = sample_semantics();
    let actual = rebuild(expected.clone()).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual.query_definition.raw_query_source,
        "SELECT id FROM ice.sales.orders"
    );
    assert_eq!(actual.visible_columns, vec!["id"]);
    assert_eq!(actual.hidden_columns, vec!["__nova_base_row_id"]);
    assert_eq!(actual.primary_key_columns, vec!["id"]);
    assert_eq!(actual.base_dependencies[0].name, "orders");
    assert!(actual.refresh.paused);
    assert_eq!(actual.refresh.interval_ms, Some(60_000));
    assert_eq!(actual.refresh.max_staleness_ms, Some(300_000));
    assert_eq!(actual.schema_contract, sample_schema_contract());
    assert_eq!(actual.created_at_ms, 123);
}

#[test]
fn desired_semantics_rejects_missing_package_query_and_negative_creation_time() {
    let mut value = sample_semantics();
    value.package_id.clear();
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("package_id must not be empty")
    );
    let mut value = sample_semantics();
    value.query_definition.raw_query_source.clear();
    assert!(rebuild(value).unwrap_err().contains("raw source is empty"));
    let mut value = sample_semantics();
    value.created_at_ms = -1;
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("created_at_ms must not be negative")
    );
}

#[test]
fn desired_semantics_rejects_noncanonical_or_duplicate_output_names() {
    let mut value = sample_semantics();
    value.visible_columns.clear();
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("at least one visible column")
    );
    let mut value = sample_semantics();
    value.visible_columns = vec!["ID".into()];
    assert!(rebuild(value).unwrap_err().contains("is not canonical"));
    let mut value = sample_semantics();
    value.hidden_columns = vec!["hidden".into(), "hidden".into()];
    assert!(rebuild(value).unwrap_err().contains("duplicate identifier"));
    let mut value = sample_semantics();
    value.primary_key_columns = vec!["`ID`".into(), "id".into()];
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("primary_key_columns has duplicate identifier")
    );
}

#[test]
fn desired_semantics_requires_complete_unique_dependency_facts() {
    let mut value = sample_semantics();
    value.base_dependencies.clear();
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("at least one base dependency")
    );
    let mut value = sample_semantics();
    value.base_dependencies[0].storage_engine.clear();
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("dependency storage_engine must not be empty")
    );
    let mut value = sample_semantics();
    value
        .base_dependencies
        .push(value.base_dependencies[0].clone());
    assert!(
        rebuild(value)
            .unwrap_err()
            .contains("duplicate base dependency")
    );
}
