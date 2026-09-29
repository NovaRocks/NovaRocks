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

//! Provider-owned projection of frozen Iceberg metadata into bounded SPI facts.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorColumnDefault, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceId, ConnectorRequestContext, ConnectorTableColumnPlanningFact,
    ConnectorTableColumnRole, ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
    ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTablePlanningFacts,
    ConnectorTableUniqueConstraint,
};

use crate::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergTableInfo};

/// Validate the Iceberg-owned delete facts sealed into planned data files.
///
/// This is deliberately a provider-side validation step: generic planning
/// carries the opaque file facts but must neither infer Iceberg equality-delete
/// identity nor reinterpret table field IDs.  Callers invoke it before a
/// split is frozen for the execution host.
pub fn validate_planned_files(
    table: Option<&IcebergTableInfo>,
    files: &[IcebergDataFileInfo],
) -> Result<(), ConnectorError> {
    for file in files {
        crate::delete_file::validate_delete_apply_cost(file)?;
    }
    let Some(table) = table else {
        return Ok(());
    };

    let mut schema_by_id = BTreeMap::new();
    let mut schema_by_name = BTreeMap::new();
    for field in &table.schema.fields {
        if schema_by_id
            .insert(field.field_id, field.name.clone())
            .is_some()
        {
            return corrupt(format!(
                "Iceberg table schema has duplicate field id {} for table {}",
                field.field_id, table.table
            ));
        }
        if schema_by_name
            .insert(field.name.to_ascii_lowercase(), field.name.clone())
            .is_some()
        {
            return corrupt(format!(
                "Iceberg table schema has duplicate field name {} for table {}",
                field.name, table.table
            ));
        }
    }

    for file in files {
        for delete in &file.delete_files {
            if delete.file_content != IcebergDeleteFileContent::Equality {
                continue;
            }

            let mut ids_seen = BTreeSet::new();
            let mut resolved_ids = Vec::new();
            for field_id in &delete.equality_field_ids {
                if !ids_seen.insert(*field_id) {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has duplicate equality field id {}",
                        delete.path, field_id
                    ));
                }
                let name = schema_by_id.get(field_id).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg equality-delete file {} references unknown field id {} in table {}",
                            delete.path, field_id, table.table
                        ),
                    )
                })?;
                resolved_ids.push(name.to_ascii_lowercase());
            }

            let mut names_seen = BTreeSet::new();
            let mut resolved_names = Vec::new();
            for name in &delete.equality_column_names {
                let normalized = name.to_ascii_lowercase();
                if !names_seen.insert(normalized.clone()) {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has duplicate equality column name {}",
                        delete.path, name
                    ));
                }
                let canonical = schema_by_name.get(&normalized).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg equality-delete file {} references unknown equality column {} in table {}",
                            delete.path, name, table.table
                        ),
                    )
                })?;
                resolved_names.push(canonical.to_ascii_lowercase());
            }

            match (resolved_ids.is_empty(), resolved_names.is_empty()) {
                (true, true) => {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has no equality field identity",
                        delete.path
                    ));
                }
                (false, false)
                    if resolved_ids.iter().collect::<BTreeSet<_>>()
                        != resolved_names.iter().collect::<BTreeSet<_>>() =>
                {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} field id/name mismatch: ids={resolved_ids:?} names={resolved_names:?}",
                        delete.path
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn corrupt<T>(message: String) -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        message,
    ))
}

/// Derives the planning facts exposed by `ConnectorMetadata::load_table`.
///
/// The serialized metadata is parsed only inside the Iceberg provider.  The
/// returned facts intentionally contain no table UUID, snapshot, file, or
/// provider payload detail.
pub struct IcebergTablePlanningFactsInput<'a> {
    pub schema: &'a SchemaRef,
    /// Authoritative Iceberg schema behind `schema`, used only to read each
    /// column's write default.
    ///
    /// Metadata tables have a synthetic Arrow schema with no Iceberg column
    /// behind it, so they pass `None` and expose no write defaults.
    pub iceberg_schema: Option<&'a crate::iceberg::spec::Schema>,
    pub metadata_columns: &'a [String],
    pub hidden_columns: &'a [String],
    pub logical_type_columns: &'a BTreeMap<String, String>,
    pub serialized_metadata: Option<&'a str>,
    pub namespace: &'a Arc<str>,
    pub instance_id: &'a ConnectorInstanceId,
    pub context: &'a ConnectorRequestContext,
}

pub fn table_planning_facts(
    input: IcebergTablePlanningFactsInput<'_>,
) -> Result<ConnectorTablePlanningFacts, ConnectorError> {
    let column_facts = input
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let name = field.name().to_ascii_lowercase();
            let visibility = if field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                || input
                    .hidden_columns
                    .iter()
                    .any(|hidden| hidden.eq_ignore_ascii_case(field.name()))
            {
                ConnectorTableColumnVisibility::Hidden
            } else {
                ConnectorTableColumnVisibility::Sql
            };
            let semantic_kind = match input.logical_type_columns.get(&name).map(String::as_str) {
                Some("bitmap") => ConnectorTableColumnSemanticKind::Bitmap,
                Some("hll") => ConnectorTableColumnSemanticKind::Hll,
                _ => ConnectorTableColumnSemanticKind::None,
            };
            let role = if input
                .metadata_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(field.name()))
            {
                ConnectorTableColumnRole::RowLineageSystem
            } else {
                ConnectorTableColumnRole::Ordinary
            };
            let write_default = iceberg_write_default(input.iceberg_schema, field.name())?;
            let write_target_type =
                iceberg_write_target_type(input.iceberg_schema, field.name(), field.data_type());
            Ok(ConnectorTableColumnPlanningFact::new(
                u32::try_from(ordinal).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg schema ordinal does not fit connector planning facts",
                    )
                })?,
                visibility,
                semantic_kind,
                role,
            )
            .with_write_default(write_default)
            .with_write_target_type(write_target_type))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let metadata = input.serialized_metadata.and_then(|serialized| {
        serde_json::from_str::<crate::iceberg::spec::TableMetadata>(serialized).ok()
    });
    let (unique_constraints, foreign_key_constraints) = metadata
        .as_ref()
        .map(|metadata| {
            iceberg_constraint_facts(
                input.schema,
                metadata.properties(),
                input.namespace,
                input.instance_id,
            )
        })
        .unwrap_or_default();
    let partition_source_column_ordinals = match (metadata.as_ref(), input.iceberg_schema) {
        (Some(metadata), Some(iceberg_schema)) => {
            iceberg_partition_source_ordinals(metadata, iceberg_schema, input.schema)?
        }
        // A metadata table has no Iceberg schema behind its synthetic Arrow
        // schema, and therefore no partitioning to report.
        _ => Vec::new(),
    };
    ConnectorTablePlanningFacts::try_new(
        input.schema,
        column_facts,
        unique_constraints,
        foreign_key_constraints,
        partition_source_column_ordinals,
        input.context,
    )
}

/// The Arrow type this column takes as a row-DML write target, when that is not
/// the same as its read type.
///
/// Iceberg `variant` and `binary` are the two types whose write-target encoding
/// deliberately differs from the read encoding NovaRocks presents to SQL. This
/// projects that existing divergence as an explicit signed fact instead of
/// having each write caller re-derive it from an Iceberg schema.
fn iceberg_write_target_type(
    iceberg_schema: Option<&crate::iceberg::spec::Schema>,
    field_name: &str,
    read_type: &ArrowDataType,
) -> Option<ArrowDataType> {
    use crate::iceberg::spec::{PrimitiveType, Type};

    let nested = iceberg_schema?.field_by_name(field_name)?;
    let write_type = match nested.field_type.as_ref() {
        Type::Primitive(PrimitiveType::Variant) => ArrowDataType::LargeBinary,
        Type::Primitive(PrimitiveType::Binary) => ArrowDataType::Binary,
        _ => return None,
    };
    (write_type != *read_type).then_some(write_type)
}

/// Arrow ordinals of the columns the table's current default partition spec
/// derives from.
///
/// A partition field whose source is absent from either schema is a corrupt
/// table, not an empty partition set, so it fails the metadata request.
fn iceberg_partition_source_ordinals(
    metadata: &crate::iceberg::spec::TableMetadata,
    iceberg_schema: &crate::iceberg::spec::Schema,
    arrow_schema: &SchemaRef,
) -> Result<Vec<u32>, ConnectorError> {
    let mut ordinals = Vec::new();
    for partition_field in metadata.default_partition_spec().fields() {
        let source = iceberg_schema
            .field_by_id(partition_field.source_id)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!(
                        "Iceberg partition field '{}' references missing source id {}",
                        partition_field.name, partition_field.source_id
                    ),
                )
            })?;
        let ordinal = arrow_schema
            .fields()
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case(&source.name))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!(
                        "Iceberg partition source column '{}' is absent from the frozen schema",
                        source.name
                    ),
                )
            })?;
        let ordinal = u32::try_from(ordinal).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg partition source ordinal does not fit connector planning facts",
            )
        })?;
        if !ordinals.contains(&ordinal) {
            ordinals.push(ordinal);
        }
    }
    Ok(ordinals)
}

/// Read one column's Iceberg write default and project it onto the sealed SPI
/// value.
///
/// A column with no Iceberg write default, and every column of a metadata
/// table, yields `None`. A default that cannot be decoded is a deterministic
/// metadata failure: silently dropping it would let generic write admission
/// fill an omitted column with NULL instead of the value the table declares.
fn iceberg_write_default(
    iceberg_schema: Option<&crate::iceberg::spec::Schema>,
    column_name: &str,
) -> Result<Option<ConnectorColumnDefault>, ConnectorError> {
    let Some(iceberg_schema) = iceberg_schema else {
        return Ok(None);
    };
    let Some(field) = iceberg_schema.field_by_name(column_name) else {
        return Ok(None);
    };
    let Some(literal) = field.write_default.as_ref() else {
        return Ok(None);
    };
    let value = crate::default_value::iceberg_literal_to_column_default(
        literal,
        field.field_type.as_ref(),
    )
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("Iceberg write default for column `{column_name}` cannot be decoded: {error}"),
        )
    })?;
    Ok(Some(
        crate::default_value::column_default_to_connector_default(&value),
    ))
}

fn iceberg_constraint_facts(
    schema: &SchemaRef,
    properties: &HashMap<String, String>,
    namespace: &Arc<str>,
    instance_id: &ConnectorInstanceId,
) -> (
    Vec<ConnectorTableUniqueConstraint>,
    Vec<ConnectorTableForeignKeyConstraint>,
) {
    let ordinals = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| (field.name().to_ascii_lowercase(), ordinal as u32))
        .collect::<HashMap<_, _>>();
    let unique_constraints = properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("unique_constraints"))
        .into_iter()
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(parse_constraint_columns)
        .filter_map(|columns| {
            columns
                .iter()
                .map(|column| ordinals.get(column).copied())
                .collect::<Option<Vec<_>>>()
                .map(ConnectorTableUniqueConstraint::new)
        })
        .collect();
    let foreign_key_constraints = properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("foreign_key_constraints"))
        .into_iter()
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(parse_foreign_key_constraint)
        .filter_map(|(local_columns, referenced_table, referenced_columns)| {
            let local_column_ordinals = local_columns
                .iter()
                .map(|column| ordinals.get(column).copied())
                .collect::<Option<Vec<_>>>()?;
            let referenced_table =
                connector_table_identity(&referenced_table, namespace, instance_id)?;
            Some(ConnectorTableForeignKeyConstraint::new(
                local_column_ordinals,
                referenced_table,
                referenced_columns.into_iter().map(Arc::from).collect(),
            ))
        })
        .collect();
    (unique_constraints, foreign_key_constraints)
}

fn parse_constraint_columns(raw: &str) -> Option<Vec<String>> {
    let segment = if let Some(open) = raw.find('(') {
        let close = raw[open + 1..].find(')')? + open + 1;
        &raw[open + 1..close]
    } else {
        raw
    };
    let columns = segment
        .split(',')
        .map(normalize_identifier)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(columns)
}

fn parse_foreign_key_constraint(raw: &str) -> Option<(Vec<String>, String, Vec<String>)> {
    let raw = raw.trim().trim_end_matches(';').trim();
    let references_idx = raw.to_ascii_lowercase().find("references")?;
    let local_columns = parse_constraint_columns(raw[..references_idx].trim())?;
    let right = raw[references_idx + "references".len()..].trim();
    let open = right.find('(')?;
    let referenced_columns = parse_constraint_columns(right)?;
    let referenced_table = right[..open].trim().to_string();
    (!referenced_table.is_empty()).then_some((local_columns, referenced_table, referenced_columns))
}

fn connector_table_identity(
    raw: &str,
    namespace: &Arc<str>,
    instance_id: &ConnectorInstanceId,
) -> Option<ConnectorTableIdentity> {
    let parts = raw
        .split('.')
        .map(normalize_identifier)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (instance_id, namespace, table) = match parts.as_slice() {
        [table] => (
            instance_id.clone(),
            namespace.clone(),
            Arc::from(table.as_str()),
        ),
        [namespace, table] => (
            instance_id.clone(),
            Arc::from(namespace.as_str()),
            Arc::from(table.as_str()),
        ),
        [catalog, namespace, table] => (
            ConnectorInstanceId::parse(catalog).ok()?,
            Arc::from(namespace.as_str()),
            Arc::from(table.as_str()),
        ),
        _ => return None,
    };
    Some(ConnectorTableIdentity {
        instance_id,
        namespace,
        table,
    })
}

fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorTableColumnRole, ConnectorTableColumnSemanticKind,
        ConnectorTableColumnVisibility, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;
    use crate::scan_model::{
        IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
        IcebergDeleteFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid request context")
    }

    #[test]
    fn maps_frozen_iceberg_columns_without_provider_identity() {
        let mut hidden_metadata = std::collections::HashMap::new();
        hidden_metadata.insert(
            CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("row_id", DataType::Int64, false),
            Field::new("internal", DataType::Utf8, true).with_metadata(hidden_metadata),
        ]));
        let metadata_columns = vec!["row_id".to_string()];
        let logical_type_columns =
            BTreeMap::from([(String::from("payload"), String::from("bitmap"))]);
        let namespace = Arc::from("db");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let context = context();
        let facts = table_planning_facts(IcebergTablePlanningFactsInput {
            schema: &schema,
            iceberg_schema: None,
            metadata_columns: &metadata_columns,
            hidden_columns: &[],
            logical_type_columns: &logical_type_columns,
            serialized_metadata: None,
            namespace: &namespace,
            instance_id: &instance_id,
            context: &context,
        })
        .expect("planning facts");

        assert_eq!(
            facts.column_facts()[0].semantic_kind(),
            ConnectorTableColumnSemanticKind::Bitmap
        );
        assert_eq!(
            facts.column_facts()[1].role(),
            ConnectorTableColumnRole::RowLineageSystem
        );
        assert_eq!(
            facts.column_facts()[2].visibility(),
            ConnectorTableColumnVisibility::Hidden
        );
        assert!(facts.unique_constraints().is_empty());
        assert!(facts.foreign_key_constraints().is_empty());
    }

    #[test]
    fn rejects_duplicate_equality_delete_field_ids_before_split_freeze() {
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: "s3://warehouse/db/t".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![IcebergSchemaFieldDef {
                    field_id: 7,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let mut file = IcebergDataFileInfo::for_test("data.parquet", 10, 1);
        file.delete_files.push(IcebergDeleteFileInfo {
            record_count: None,
            partition_data_json: None,
            path: "eq-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: None,
            partition_spec_id: None,
            partition_key: None,
            referenced_data_file: None,
            equality_column_names: Vec::new(),
            equality_field_ids: vec![7, 7],
        });

        let error = validate_planned_files(Some(&table), &[file])
            .expect_err("duplicate equality field identity must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("duplicate equality field id 7"));
    }

    /// Build a two-column Iceberg schema where `with_default` carries a write
    /// default and `plain` does not.
    fn write_default_schema(
        default_type: crate::iceberg::spec::Type,
        default_literal: crate::iceberg::spec::Literal,
    ) -> crate::iceberg::spec::Schema {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        Schema::builder()
            .with_fields(vec![
                Arc::new(
                    NestedField::optional(1, "with_default", default_type)
                        .with_write_default(default_literal),
                ),
                Arc::new(NestedField::optional(
                    2,
                    "plain",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("valid Iceberg schema")
    }

    fn write_default_facts(
        iceberg_schema: Option<&crate::iceberg::spec::Schema>,
        arrow_schema: &SchemaRef,
    ) -> Result<ConnectorTablePlanningFacts, ConnectorError> {
        let namespace = Arc::from("db");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let context = context();
        table_planning_facts(IcebergTablePlanningFactsInput {
            schema: arrow_schema,
            iceberg_schema,
            metadata_columns: &[],
            hidden_columns: &[],
            logical_type_columns: &BTreeMap::new(),
            serialized_metadata: None,
            namespace: &namespace,
            instance_id: &instance_id,
            context: &context,
        })
    }

    #[test]
    fn spi5g_write_default_is_projected_onto_planning_facts() {
        use crate::iceberg::spec::{Literal, PrimitiveType, Type};

        let iceberg_schema = write_default_schema(
            Type::Primitive(PrimitiveType::String),
            Literal::string("fallback"),
        );
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("with_default", DataType::Utf8, true),
            Field::new("plain", DataType::Int64, true),
        ]));

        let facts =
            write_default_facts(Some(&iceberg_schema), &arrow_schema).expect("planning facts");

        assert_eq!(
            facts.column_facts()[0].write_default(),
            Some(&ConnectorColumnDefault::String(Arc::from("fallback")))
        );
        assert_eq!(facts.column_facts()[1].write_default(), None);
    }

    #[test]
    fn spi5g_metadata_tables_expose_no_write_default() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("with_default", DataType::Utf8, true),
            Field::new("plain", DataType::Int64, true),
        ]));

        let facts = write_default_facts(None, &arrow_schema).expect("planning facts");

        assert!(
            facts
                .column_facts()
                .iter()
                .all(|fact| fact.write_default().is_none())
        );
    }

    #[test]
    fn spi5g_undecodable_write_default_fails_closed() {
        use crate::iceberg::spec::{Literal, PrimitiveType, Type};

        // Variant column defaults have no neutral projection. Reporting the
        // failure keeps generic write admission from silently substituting NULL
        // for the value the table declares.
        let iceberg_schema = write_default_schema(
            Type::Primitive(PrimitiveType::Variant),
            Literal::string("not-a-variant"),
        );
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("with_default", DataType::LargeBinary, true),
            Field::new("plain", DataType::Int64, true),
        ]));

        let error = write_default_facts(Some(&iceberg_schema), &arrow_schema)
            .expect_err("an undecodable write default must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("with_default"));
    }

    #[test]
    fn spi5g_columns_absent_from_the_iceberg_schema_have_no_write_default() {
        use crate::iceberg::spec::{Literal, PrimitiveType, Type};

        // Synthesized Arrow columns (row lineage, virtual columns) have no
        // Iceberg field behind them and must not inherit another column's
        // default.
        let iceberg_schema = write_default_schema(
            Type::Primitive(PrimitiveType::String),
            Literal::string("fallback"),
        );
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("with_default", DataType::Utf8, true),
            Field::new("plain", DataType::Int64, true),
            Field::new("_synthesized", DataType::Int64, true),
        ]));

        let facts =
            write_default_facts(Some(&iceberg_schema), &arrow_schema).expect("planning facts");

        assert_eq!(facts.column_facts()[2].write_default(), None);
    }

    /// Iceberg schema holding one variant, one binary and one plain column.
    fn write_target_type_schema() -> crate::iceberg::spec::Schema {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "v",
                    Type::Primitive(PrimitiveType::Variant),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "n",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("valid Iceberg schema")
    }

    #[test]
    fn spi5h_write_target_type_is_declared_only_where_it_differs_from_the_read_type() {
        // The read schema presents the variant column as Utf8, which is exactly
        // the divergence the write-target fact has to carry. The binary column
        // is already read as Binary, so it needs no override.
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("n", DataType::Int64, true),
        ]));

        let facts = write_default_facts(Some(&write_target_type_schema()), &arrow_schema)
            .expect("planning facts");

        assert_eq!(
            facts.column_facts()[0].write_target_type(),
            Some(&DataType::LargeBinary)
        );
        assert_eq!(facts.column_facts()[1].write_target_type(), None);
        assert_eq!(facts.column_facts()[2].write_target_type(), None);
    }

    #[test]
    fn spi5h_columns_without_an_iceberg_field_declare_no_write_target_type() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("n", DataType::Int64, true),
            Field::new("_row_id", DataType::Int64, true),
        ]));

        let facts = write_default_facts(Some(&write_target_type_schema()), &arrow_schema)
            .expect("planning facts");

        assert_eq!(facts.column_facts()[3].write_target_type(), None);
        // A metadata table has no Iceberg schema at all.
        let metadata_table = write_default_facts(None, &arrow_schema).expect("planning facts");
        assert!(
            metadata_table
                .column_facts()
                .iter()
                .all(|fact| fact.write_target_type().is_none())
        );
        assert!(metadata_table.partition_source_column_ordinals().is_empty());
    }

    #[test]
    fn spi5h_partition_source_ordinals_map_the_default_spec_onto_arrow_ordinals() {
        use crate::iceberg::spec::Schema as IcebergSchema;
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder,
            TableMetadataBuilder, Transform, Type,
        };

        let iceberg_schema = Arc::new(
            IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "region",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .expect("schema"),
        );
        let partition_spec = PartitionSpec::builder(iceberg_schema.clone())
            .with_spec_id(0)
            .add_partition_field("region", "region_part", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("spec");
        let metadata = TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            partition_spec,
            SortOrder::builder().build_unbound().expect("sort"),
            "file:///tmp/x".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let serialized = serde_json::to_string(&metadata).expect("serialize metadata");

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let namespace = Arc::from("db");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let context = context();
        let facts = table_planning_facts(IcebergTablePlanningFactsInput {
            schema: &arrow_schema,
            iceberg_schema: Some(iceberg_schema.as_ref()),
            metadata_columns: &[],
            hidden_columns: &[],
            logical_type_columns: &BTreeMap::new(),
            serialized_metadata: Some(&serialized),
            namespace: &namespace,
            instance_id: &instance_id,
            context: &context,
        })
        .expect("planning facts");

        assert_eq!(facts.partition_source_column_ordinals(), &[1]);
    }

    #[test]
    fn spi5h_partition_source_absent_from_the_frozen_schema_fails_closed() {
        use crate::iceberg::spec::Schema as IcebergSchema;
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder,
            TableMetadataBuilder, Transform, Type,
        };

        let iceberg_schema = Arc::new(
            IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .expect("schema"),
        );
        let partition_spec = PartitionSpec::builder(iceberg_schema.clone())
            .with_spec_id(0)
            .add_partition_field("region", "region_part", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("spec");
        let metadata = TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            partition_spec,
            SortOrder::builder().build_unbound().expect("sort"),
            "file:///tmp/x".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let serialized = serde_json::to_string(&metadata).expect("serialize metadata");

        // The frozen Arrow schema is missing the partition source column, which
        // is an inconsistent table rather than an unpartitioned one.
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        let namespace = Arc::from("db");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let context = context();
        let error = table_planning_facts(IcebergTablePlanningFactsInput {
            schema: &arrow_schema,
            iceberg_schema: Some(iceberg_schema.as_ref()),
            metadata_columns: &[],
            hidden_columns: &[],
            logical_type_columns: &BTreeMap::new(),
            serialized_metadata: Some(&serialized),
            namespace: &namespace,
            instance_id: &instance_id,
            context: &context,
        })
        .expect_err("missing partition source column must fail closed");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
