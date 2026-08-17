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

//! `CREATE TABLE` DDL text handling for catalog DDL execution.
//!
//! Ownership: both directions of catalog `CREATE TABLE` text live here —
//! recognizing `CREATE TABLE ... LIKE ...` on the way in, and rendering
//! `SHOW CREATE TABLE` output from connector facts on the way out. Neither is
//! query assembly: they carry no plan, fragment, or execution state, and their
//! only consumer is the catalog DDL command executor.
//!
//! The purest owner for `parse_create_table_like` would be `novarocks-sql`
//! next to the other statement recognizers, but that crate has no `regex`
//! dependency today; adding one would create a new dependency edge, so this
//! keeps the nearest already-legal owner.

use novarocks_sql::syntax::ObjectName;

/// Recognize `CREATE TABLE <target> LIKE <source>` and return both names.
///
/// Returns `Ok(None)` when the statement is not a `CREATE TABLE ... LIKE`.
pub(crate) fn parse_create_table_like(
    sql: &str,
) -> Result<Option<(ObjectName, ObjectName)>, String> {
    let re = regex::Regex::new(
        r#"(?is)^\s*create\s+table\s+(?P<target>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+like\s+(?P<source>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s*$"#,
    )
    .map_err(|e| format!("compile CREATE TABLE LIKE regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let target = parse_simple_object_name(captures.name("target").expect("target").as_str())?;
    let source = parse_simple_object_name(captures.name("source").expect("source").as_str())?;
    Ok(Some((target, source)))
}

fn parse_simple_object_name(token: &str) -> Result<ObjectName, String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_backtick = false;
    for ch in token.chars() {
        match ch {
            '`' => in_backtick = !in_backtick,
            '.' if !in_backtick => {
                if cur.is_empty() {
                    return Err(format!("empty object name segment in `{token}`"));
                }
                parts.push(cur.clone());
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    if parts.is_empty() {
        return Err(format!("empty object name `{token}`"));
    }
    Ok(ObjectName { parts })
}

/// Generate a `CREATE TABLE` DDL string from exact-generation connector facts.
pub(crate) fn build_iceberg_create_table_ddl(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Result<String, String> {
    use novarocks_spi::connector::ConnectorTableDefinitionType;

    fn definition_type_to_sql(ty: &ConnectorTableDefinitionType) -> String {
        match ty {
            ConnectorTableDefinitionType::Boolean => "BOOLEAN".to_string(),
            ConnectorTableDefinitionType::Int => "INT".to_string(),
            ConnectorTableDefinitionType::BigInt => "BIGINT".to_string(),
            ConnectorTableDefinitionType::Float => "FLOAT".to_string(),
            ConnectorTableDefinitionType::Double => "DOUBLE".to_string(),
            ConnectorTableDefinitionType::Decimal { precision, scale } => {
                format!("DECIMAL({precision},{scale})")
            }
            ConnectorTableDefinitionType::Date => "DATE".to_string(),
            ConnectorTableDefinitionType::Time => "TIME".to_string(),
            ConnectorTableDefinitionType::DateTime => "DATETIME".to_string(),
            ConnectorTableDefinitionType::DateTimeNs => "TIMESTAMP_NS".to_string(),
            ConnectorTableDefinitionType::String => "STRING".to_string(),
            ConnectorTableDefinitionType::Binary {
                fixed_length: Some(length),
            } => format!("BINARY({length})"),
            ConnectorTableDefinitionType::Binary { fixed_length: None } => "BINARY".to_string(),
            ConnectorTableDefinitionType::Variant => "VARIANT".to_string(),
            ConnectorTableDefinitionType::Array(element) => {
                format!("ARRAY<{}>", definition_type_to_sql(element))
            }
            ConnectorTableDefinitionType::Map(key, value) => format!(
                "MAP<{},{}>",
                definition_type_to_sql(key),
                definition_type_to_sql(value)
            ),
            ConnectorTableDefinitionType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        format!(
                            "{} {}",
                            field.name(),
                            definition_type_to_sql(field.data_type())
                        )
                    })
                    .collect::<Vec<_>>();
                format!("STRUCT<{}>", fields.join(", "))
            }
        }
    }

    if loaded.definition_facts.is_empty() {
        return Err(
            "SHOW CREATE TABLE is unsupported because the connector returned no table definition facts"
                .to_string(),
        );
    }
    let mut col_defs = Vec::with_capacity(loaded.definition_facts.columns().len());
    for column in loaded.definition_facts.columns() {
        let field = loaded.schema.field(column.field_ordinal() as usize);
        let nullable = if column.nullable() { "" } else { " NOT NULL" };
        let comment = if let Some(doc) = column.comment() {
            let escaped = doc.replace('\'', "\\'");
            format!(" COMMENT '{escaped}'")
        } else {
            String::new()
        };
        col_defs.push(format!(
            "  `{}` {}{}{}",
            field.name(),
            definition_type_to_sql(column.data_type()),
            nullable,
            comment
        ));
    }

    let table_comment = loaded
        .definition_facts
        .table_comment()
        .filter(|v| !v.is_empty())
        .map(|v| {
            let escaped = v.replace('\'', "\\'");
            format!("\nCOMMENT '{escaped}'")
        })
        .unwrap_or_default();

    Ok(format!(
        "CREATE TABLE `{catalog}`.`{namespace}`.`{table}` (\n{}\n){table_comment}",
        col_defs.join(",\n")
    ))
}

#[cfg(test)]
mod tests {
    use super::build_iceberg_create_table_ddl;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableDefinitionColumn, ConnectorTableDefinitionFacts,
        ConnectorTableDefinitionStructField, ConnectorTableDefinitionType, ConnectorTableHandle,
        ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTablePlanningFacts,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
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
        ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from("ns"),
                table: Arc::from("tbl"),
            },
            schema,
            planning_facts,
            definition_facts,
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                .expect("table handle"),
        }
    }

    #[test]
    fn emits_table_and_column_comments_with_escaping() {
        let loaded = loaded_table(
            Some("it's great"),
            Some("owner's id"),
            ConnectorTableDefinitionType::Int,
            false,
        );
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(ddl.contains("`id` INT NOT NULL COMMENT 'owner\\'s id'"));
        assert!(ddl.contains("COMMENT 'it\\'s great'"));
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
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(ddl.contains("ARRAY<STRUCT<payload MAP<STRING,BINARY(16)>>>"));
    }

    #[test]
    fn no_comment_clause_when_comment_is_empty() {
        let loaded = loaded_table(Some(""), None, ConnectorTableDefinitionType::Int, true);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(!ddl.contains("COMMENT"));
    }

    #[test]
    fn empty_definition_facts_fail_closed() {
        let mut loaded = loaded_table(None, None, ConnectorTableDefinitionType::Int, true);
        loaded.definition_facts = ConnectorTableDefinitionFacts::empty();
        let error = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded)
            .expect_err("empty definition facts must fail");
        assert!(error.contains("unsupported"));
    }
}
