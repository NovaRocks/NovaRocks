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

//! Provider-owned projection from an Iceberg schema into bounded connector
//! table-definition facts. Core renders SQL from these facts and never reads
//! an Iceberg table or type.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorError, ConnectorRequestContext, ConnectorTableColumnVisibility,
    ConnectorTableDefinitionColumn, ConnectorTableDefinitionFacts,
    ConnectorTableDefinitionStructField, ConnectorTableDefinitionType, ConnectorTablePlanningFacts,
};

use crate::iceberg::spec::{PrimitiveType, Schema, Type};

pub fn table_definition_facts(
    iceberg_schema: &Schema,
    arrow_schema: &SchemaRef,
    planning_facts: &ConnectorTablePlanningFacts,
    table_comment: Option<&str>,
    context: &ConnectorRequestContext,
) -> Result<ConnectorTableDefinitionFacts, ConnectorError> {
    let sql_ordinals = (!planning_facts.column_facts().is_empty()).then(|| {
        planning_facts
            .column_facts()
            .iter()
            .filter(|fact| fact.visibility() == ConnectorTableColumnVisibility::Sql)
            .map(|fact| fact.field_ordinal())
            .collect::<BTreeSet<_>>()
    });
    let columns = iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| {
            sql_ordinals.as_ref().is_none_or(|ordinals| {
                u32::try_from(*ordinal).is_ok_and(|ordinal| ordinals.contains(&ordinal))
            })
        })
        .map(|(ordinal, field)| {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                    "Iceberg table definition ordinal does not fit u32",
                )
            })?;
            Ok(ConnectorTableDefinitionColumn::new(
                ordinal,
                definition_type(&field.field_type),
                !field.required,
                field.doc.as_deref().map(Arc::from),
            ))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;

    ConnectorTableDefinitionFacts::try_new(
        arrow_schema,
        planning_facts,
        columns,
        table_comment.map(Arc::from),
        context,
    )
}

fn definition_type(data_type: &Type) -> ConnectorTableDefinitionType {
    match data_type {
        Type::Primitive(PrimitiveType::Boolean) => ConnectorTableDefinitionType::Boolean,
        Type::Primitive(PrimitiveType::Int) => ConnectorTableDefinitionType::Int,
        Type::Primitive(PrimitiveType::Long) => ConnectorTableDefinitionType::BigInt,
        Type::Primitive(PrimitiveType::Float) => ConnectorTableDefinitionType::Float,
        Type::Primitive(PrimitiveType::Double) => ConnectorTableDefinitionType::Double,
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            ConnectorTableDefinitionType::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        Type::Primitive(PrimitiveType::Date) => ConnectorTableDefinitionType::Date,
        Type::Primitive(PrimitiveType::Time) => ConnectorTableDefinitionType::Time,
        Type::Primitive(
            value @ (PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs),
        ) => ConnectorTableDefinitionType::Timestamp {
            precision: if matches!(
                value,
                PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs
            ) {
                9
            } else {
                6
            },
            with_time_zone: matches!(
                value,
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs
            ),
        },
        Type::Primitive(PrimitiveType::String) => ConnectorTableDefinitionType::String,
        Type::Primitive(PrimitiveType::Uuid) => ConnectorTableDefinitionType::Uuid,
        Type::Primitive(PrimitiveType::Fixed(length)) => ConnectorTableDefinitionType::Binary {
            fixed_length: Some(*length),
        },
        Type::Primitive(PrimitiveType::Binary) => {
            ConnectorTableDefinitionType::Binary { fixed_length: None }
        }
        Type::Variant(_) => ConnectorTableDefinitionType::Variant,
        Type::List(list) => ConnectorTableDefinitionType::Array(Box::new(definition_type(
            &list.element_field.field_type,
        ))),
        Type::Map(map) => ConnectorTableDefinitionType::Map(
            Box::new(definition_type(&map.key_field.field_type)),
            Box::new(definition_type(&map.value_field.field_type)),
        ),
        Type::Struct(struct_type) => ConnectorTableDefinitionType::Struct(
            struct_type
                .fields()
                .iter()
                .map(|field| {
                    ConnectorTableDefinitionStructField::new(
                        Arc::<str>::from(field.name.as_str()),
                        definition_type(&field.field_type),
                    )
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use novarocks_spi::connector::{
        ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
        ConnectorTableColumnSemanticKind, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use crate::iceberg::spec::{NestedField, StructType};

    use super::*;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("context")
    }

    #[test]
    fn projects_fixed_nested_uuid_and_timestamp_types() {
        let nested = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                1,
                "fixed_value",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                2,
                "uuid_value",
                Type::Primitive(PrimitiveType::Uuid),
            )),
            Arc::new(NestedField::optional(
                3,
                "timestamp_value",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )),
            Arc::new(NestedField::optional(
                4,
                "decimal_value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 18,
                    scale: 4,
                }),
            )),
        ]));

        assert_eq!(
            definition_type(&nested),
            ConnectorTableDefinitionType::Struct(vec![
                ConnectorTableDefinitionStructField::new(
                    "fixed_value",
                    ConnectorTableDefinitionType::Binary {
                        fixed_length: Some(16),
                    },
                ),
                ConnectorTableDefinitionStructField::new(
                    "uuid_value",
                    ConnectorTableDefinitionType::Uuid,
                ),
                ConnectorTableDefinitionStructField::new(
                    "timestamp_value",
                    ConnectorTableDefinitionType::Timestamp {
                        precision: 9,
                        with_time_zone: true,
                    },
                ),
                ConnectorTableDefinitionStructField::new(
                    "decimal_value",
                    ConnectorTableDefinitionType::Decimal {
                        precision: 18,
                        scale: 4,
                    },
                ),
            ])
        );
    }

    #[test]
    fn definition_facts_exclude_hidden_system_columns() {
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "_row_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("schema");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, true),
            ArrowField::new("_row_id", DataType::Int64, false),
        ]));
        let planning_facts = ConnectorTablePlanningFacts::try_new(
            &arrow_schema,
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    1,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(),
        )
        .expect("planning facts");

        let facts = table_definition_facts(
            &iceberg_schema,
            &arrow_schema,
            &planning_facts,
            None,
            &context(),
        )
        .expect("definition facts");

        assert_eq!(facts.columns().len(), 2);
        assert_eq!(facts.columns()[0].field_ordinal(), 0);
        assert_eq!(facts.columns()[1].field_ordinal(), 1);
    }
}
