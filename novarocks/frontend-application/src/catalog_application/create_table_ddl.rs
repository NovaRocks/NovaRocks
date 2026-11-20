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

//! Render catalog definition facts through the Trino syntax tree.

use novarocks_parser::{
    SYNTHETIC,
    ast::{
        AstBox, DateTimeDataType, DateTimeType, RowDataTypeField, StatementProperty,
        StatementSaveMode, TrinoColumnDefinition, TrinoCreateTable, TrinoTableElement, TypeKind,
        TypeName,
    },
    build,
    printer::print_create_table_definition,
};
use novarocks_spi::connector::{
    ConnectorTableDefinitionType, ConnectorTableMetadata, DdlPropertyValue,
};

/// Generate `CREATE TABLE` from one exact table observation.
pub(crate) fn build_iceberg_create_table_ddl(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &ConnectorTableMetadata,
    properties: Vec<(String, DdlPropertyValue)>,
) -> Result<String, String> {
    if loaded.definition_facts.is_empty() {
        return Err(
            "SHOW CREATE TABLE is unsupported because the connector returned no table definition facts"
                .to_string(),
        );
    }

    let name = build::object_name(
        [catalog, namespace, table]
            .into_iter()
            .map(build::ident_from_catalog)
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    let elements = loaded
        .definition_facts
        .columns()
        .iter()
        .map(|column| {
            let field = loaded.schema().field(column.field_ordinal() as usize);
            let name = build::object_name(vec![build::ident_from_catalog(field.name().as_str())])
                .map_err(|error| error.to_string())?;
            Ok(TrinoTableElement::Column(AstBox::new(
                TrinoColumnDefinition {
                    name,
                    data_type: definition_type_to_trino(column.data_type()),
                    default_value: None,
                    nullable: column.nullable(),
                    properties: Default::default(),
                    comment: column.comment().map(str::to_owned),
                    span: SYNTHETIC,
                },
            )))
        })
        .collect::<Result<_, String>>()?;
    let ddl = TrinoCreateTable {
        name,
        elements,
        save_mode: StatementSaveMode::Fail,
        properties: definition_properties(properties)?.into(),
        comment: loaded
            .definition_facts
            .table_comment()
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        span: SYNTHETIC,
    };
    print_create_table_definition(&ddl).map_err(|error| error.to_string())
}

pub(crate) fn definition_properties(
    properties: Vec<(String, DdlPropertyValue)>,
) -> Result<Vec<StatementProperty>, String> {
    properties
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                DdlPropertyValue::Null => build::null(),
                DdlPropertyValue::Boolean(value) => build::boolean(value),
                DdlPropertyValue::Integer(value) => build::integer(i64::from(value)),
                DdlPropertyValue::Bigint(value) => build::integer(value),
                DdlPropertyValue::Varchar(value) => build::string(value),
                DdlPropertyValue::ArrayVarchar(values) => build::array(
                    values
                        .into_iter()
                        .map(|value| value.map_or_else(build::null, build::string))
                        .collect(),
                ),
                _ => return Err(format!("Unsupported displayed DDL property '{name}'")),
            };
            Ok(StatementProperty {
                name: build::ident_from_catalog(name),
                value: Some(value),
                span: SYNTHETIC,
            })
        })
        .collect()
}

fn definition_type_to_trino(ty: &ConnectorTableDefinitionType) -> TypeName {
    use ConnectorTableDefinitionType as Type;

    let name = match ty {
        Type::Boolean => "boolean",
        Type::Int => "integer",
        Type::BigInt => "bigint",
        Type::Float => "real",
        Type::Double => "double",
        Type::Date => "date",
        Type::String => "varchar",
        Type::Uuid => "uuid",
        Type::Binary { .. } => "varbinary",
        Type::Variant => "variant",
        Type::Decimal { precision, scale } => {
            return build::type_name(
                build::ident("decimal", false),
                vec![
                    build::numeric_parameter(i64::from(*precision)),
                    build::numeric_parameter(i64::from(*scale)),
                ],
            );
        }
        Type::Time => return datetime(DateTimeType::Time, 6, false),
        Type::Timestamp {
            precision,
            with_time_zone,
        } => {
            return datetime(DateTimeType::Timestamp, *precision, *with_time_zone);
        }
        Type::Array(element) => {
            return build::type_name(
                build::ident("array", false),
                vec![build::type_parameter(definition_type_to_trino(element))],
            );
        }
        Type::Map(key, value) => {
            return build::type_name(
                build::ident("map", false),
                vec![
                    build::type_parameter(definition_type_to_trino(key)),
                    build::type_parameter(definition_type_to_trino(value)),
                ],
            );
        }
        Type::Struct(fields) => {
            return build::row_type(
                fields
                    .iter()
                    .map(|field| RowDataTypeField {
                        name: Some(build::ident_from_catalog(field.name())),
                        data_type: definition_type_to_trino(field.data_type()),
                        span: SYNTHETIC,
                    })
                    .collect(),
            );
        }
    };
    build::simple_type(name)
}

fn datetime(kind: DateTimeType, precision: u32, with_time_zone: bool) -> TypeName {
    TypeName {
        kind: TypeKind::DateTime(DateTimeDataType {
            kind,
            with_time_zone,
            precision: Some(AstBox::new(build::numeric_parameter(i64::from(precision)))),
        }),
        span: SYNTHETIC,
    }
}

#[cfg(test)]
mod tests {
    use super::build_iceberg_create_table_ddl;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorRequestContext, ConnectorTableDefinitionColumn,
        ConnectorTableDefinitionFacts, ConnectorTableDefinitionStructField,
        ConnectorTableDefinitionType, ConnectorTableHandle, ConnectorTableIdentity,
        ConnectorTableMetadata, ConnectorTablePlanningFacts,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1_024,
            64 * 1_024,
        )
        .expect("request context")
    }

    fn loaded_table(
        table_comment: Option<&str>,
        column_comment: Option<&str>,
        data_type: ConnectorTableDefinitionType,
        nullable: bool,
    ) -> ConnectorTableMetadata {
        let instance_id = ConnectorInstanceId::parse("cat").expect("instance ID");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            nullable,
        )]));
        let planning_facts = ConnectorTablePlanningFacts::empty();
        let definition_facts = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &planning_facts,
            vec![ConnectorTableDefinitionColumn::new(
                0,
                data_type,
                nullable,
                column_comment.map(Arc::from),
            )],
            table_comment.map(Arc::from),
            &request_context(),
        )
        .expect("definition facts");
        crate::connector::schema_creation::declared_test_metadata(
            novarocks_spi::connector::ConnectorTableMetadataHeader {
                identity: ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from("ns"),
                    table: Arc::from("tbl"),
                },
                planning_facts,
                definition_facts,
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                    .expect("table handle"),
            },
            schema,
            vec![novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                nullable,
            )],
        )
    }

    #[test]
    fn emits_table_and_column_comments_with_escaping() {
        let loaded = loaded_table(
            Some("it's great"),
            Some("owner's id"),
            ConnectorTableDefinitionType::Int,
            false,
        );
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded, Vec::new())
            .expect("build ddl");
        assert!(ddl.contains("id integer NOT NULL COMMENT 'owner''s id'"));
        assert!(ddl.contains("COMMENT 'it''s great'"));
    }

    #[test]
    fn renders_declared_properties_and_quoted_names_as_trino_ddl() {
        use novarocks_spi::connector::DdlPropertyValue as Value;

        let loaded = loaded_table(None, None, ConnectorTableDefinitionType::Int, true);
        let ddl = build_iceberg_create_table_ddl(
            "cat",
            "Mixed Case",
            "select",
            &loaded,
            vec![
                ("format".into(), Value::Varchar("PARQUET".into())),
                ("format_version".into(), Value::Integer(2)),
                (
                    "partitioning".into(),
                    Value::ArrayVarchar(vec![Some("bucket(id, 4)".into())]),
                ),
            ],
        )
        .expect("build ddl");
        assert_eq!(
            ddl,
            "CREATE TABLE cat.\"Mixed Case\".\"select\" (\n   id integer\n)\nWITH (\n   format = 'PARQUET',\n   format_version = 2,\n   partitioning = ARRAY['bucket(id, 4)']\n)"
        );
        assert!(novarocks_query_application::sql::parse_single_statement(&ddl).is_ok());
    }

    #[test]
    fn renders_fixed_and_nested_definition_types() {
        let loaded = loaded_table(
            None,
            None,
            ConnectorTableDefinitionType::Array(Box::new(ConnectorTableDefinitionType::Struct(
                vec![ConnectorTableDefinitionStructField::new(
                    "payload",
                    ConnectorTableDefinitionType::Map(
                        Box::new(ConnectorTableDefinitionType::String),
                        Box::new(ConnectorTableDefinitionType::Binary {
                            fixed_length: Some(16),
                        }),
                    ),
                )],
            ))),
            true,
        );
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded, Vec::new())
            .expect("build ddl");
        assert!(ddl.contains("array(ROW(payload map(varchar, varbinary)))"));
    }

    #[test]
    fn no_comment_clause_when_comment_is_empty() {
        let loaded = loaded_table(Some(""), None, ConnectorTableDefinitionType::Int, true);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded, Vec::new())
            .expect("build ddl");
        assert!(!ddl.contains("COMMENT"));
    }

    #[test]
    fn empty_definition_facts_fail_closed() {
        let mut loaded = loaded_table(None, None, ConnectorTableDefinitionType::Int, true);
        loaded.definition_facts = ConnectorTableDefinitionFacts::empty();
        let error = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded, Vec::new())
            .expect_err("empty definition facts must fail");
        assert!(error.contains("unsupported"));
    }
}
